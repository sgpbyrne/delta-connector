package com.deltaconnect.connect

import com.deltaconnect.connect.cdc.DebeziumRecordConverter
import com.deltaconnect.connect.storage.StorageProviderRegistry
import com.deltaconnect.protocol.storage.CommitConflictException
import com.deltaconnect.protocol.storage.DeltaLogStore
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.errors.RetriableException
import org.apache.kafka.connect.sink.ErrantRecordReporter
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import org.slf4j.LoggerFactory

/**
 * Kafka Connect SinkTask that processes records and writes them
 * to Delta Lake tables.
 *
 * Lifecycle: start - open - put - preCommit - close - stop.
 *
 * Records are buffered per topic and flushed when either the batch size
 * threshold or the time interval is reached. Committed Kafka offsets are
 * tracked in the Delta transaction log via [SetTransaction][com.deltaconnect.protocol.actions.SetTransaction]
 * for exactly-once recovery on restart.
 *
 * The storage backend is resolved from the `delta.storage.path` URI scheme
 * via [StorageProviderRegistry], making the task cloud-agnostic.
 *
 * Error handling follows Kafka Connect conventions:
 * - Data errors (record conversion failures): reported to
 *   [ErrantRecordReporter] (dead-letter queue) if configured, otherwise
 *   propagated as [ConnectException].
 * - Transient errors (storage I/O, commit conflicts after retries):
 *   thrown as [RetriableException] so Connect retries the task.
 * - Fatal errors (incompatible schema, configuration): thrown as
 *   [ConnectException] to fail the task.
 */
class DeltaSinkTask : SinkTask() {

    private val log = LoggerFactory.getLogger(DeltaSinkTask::class.java)

    private lateinit var config: DeltaSinkConfig
    private lateinit var logStore: DeltaLogStore
    private lateinit var converter: RecordConverter
    private lateinit var basePath: String
    private var reporter: ErrantRecordReporter? = null

    private val tableWriters = mutableMapOf<String, TableWriter>()
    private val committedOffsets = mutableMapOf<TopicPartition, Long>()
    private var lastFlushTimeMs: Long = 0L

    // Visible for testing: allows injecting custom factories
    internal var logStoreFactory: (DeltaSinkConfig) -> DeltaLogStore = ::createLogStoreFromProvider
    internal var converterFactory: (DeltaSinkConfig) -> RecordConverter = { cfg ->
        when (cfg.writeMode) {
            DeltaSinkConfig.WriteMode.APPEND -> AppendRecordConverter()
            DeltaSinkConfig.WriteMode.CDC -> DebeziumRecordConverter(cfg.cdcEnvelopeFormat)
            DeltaSinkConfig.WriteMode.UPSERT -> throw UnsupportedOperationException(
                "UpsertRecordConverter not yet implemented"
            )
        }
    }
    internal var clock: () -> Long = System::currentTimeMillis

    override fun version(): String = DeltaSinkConnector.VERSION

    override fun start(props: Map<String, String>) {
        log.info("Starting DeltaSinkTask")
        config = DeltaSinkConfig(props)
        logStore = logStoreFactory(config)
        converter = converterFactory(config)
        basePath = resolveBasePath(config)
        lastFlushTimeMs = clock()

        // ErrantRecordReporter may not be available (older Connect runtimes
        // or errors.tolerance not configured)
        reporter = try {
            context.errantRecordReporter()
        } catch (_: NoSuchMethodError) {
            null
        } catch (_: NoClassDefFoundError) {
            null
        }

        log.info(
            "DeltaSinkTask started: writeMode={}, batchSize={}, intervalMs={}, " +
                "schemaEvolution={}, dlqEnabled={}",
            config.writeMode, config.mergeBatchSize, config.mergeIntervalMs,
            config.schemaEvolutionEnabled, reporter != null
        )
    }

    override fun open(partitions: Collection<TopicPartition>) {
        log.info("Partitions assigned: {}", partitions)
        val seekOffsets = mutableMapOf<TopicPartition, Long>()

        for (tp in partitions) {
            val writer = getOrCreateWriter(tp.topic())
            val committedOffset = writer.getCommittedOffset(tp)
            if (committedOffset != null) {
                // Seek past the last committed offset
                seekOffsets[tp] = committedOffset + 1
                this.committedOffsets[tp] = committedOffset
                log.info(
                    "Recovered offset: partition={}, committedOffset={}, seekTo={}",
                    tp, committedOffset, committedOffset + 1
                )
            }
        }

        if (seekOffsets.isNotEmpty()) {
            context.offset(seekOffsets)
        }
    }

    override fun put(records: Collection<SinkRecord>) {
        if (records.isEmpty()) return

        val effectiveDeleteEnabled = config.mergeDeleteEnabled &&
            config.writeMode != DeltaSinkConfig.WriteMode.UPSERT

        for (record in records) {
            val sourceRecord = convertRecord(record, effectiveDeleteEnabled) ?: continue
            val topic = record.topic()
            val tp = TopicPartition(topic, record.kafkaPartition())
            getOrCreateWriter(topic).buffer(sourceRecord, tp, record.kafkaOffset())
        }

        // Check batch size trigger per topic
        for ((_, writer) in tableWriters) {
            if (writer.bufferSize() >= config.mergeBatchSize) {
                flushWriter(writer)
            }
        }

        // Check time-based trigger
        if (clock() - lastFlushTimeMs >= config.mergeIntervalMs) {
            flushAll()
        }
    }

    override fun preCommit(
        currentOffsets: Map<TopicPartition, OffsetAndMetadata>
    ): Map<TopicPartition, OffsetAndMetadata> {
        if (clock() - lastFlushTimeMs >= config.mergeIntervalMs) {
            flushAll()
        }

        return committedOffsets.mapValues { (_, offset) ->
            OffsetAndMetadata(offset + 1)
        }
    }

    override fun close(partitions: Collection<TopicPartition>) {
        log.info("Partitions revoked: {}", partitions)

        val topicsToFlush = partitions.map { it.topic() }.toSet()
        for (topic in topicsToFlush) {
            val writer = tableWriters[topic]
            if (writer != null && writer.hasBufferedRecords()) {
                flushWriter(writer)
            }
        }

        for (tp in partitions) {
            committedOffsets.remove(tp)
        }
    }

    override fun stop() {
        log.info("Stopping DeltaSinkTask")
        flushAll()
        tableWriters.clear()
        committedOffsets.clear()
    }

    /**
     * Convert a [SinkRecord] to a [com.deltaconnect.protocol.merge.SourceRecord],
     * reporting data errors to the DLQ if configured.
     */
    private fun convertRecord(
        record: SinkRecord,
        deleteEnabled: Boolean
    ): com.deltaconnect.protocol.merge.SourceRecord? {
        return try {
            converter.convert(record, deleteEnabled)
        } catch (e: Exception) {
            if (reporter != null) {
                log.warn(
                    "Record conversion failed, sending to DLQ: topic={}, partition={}, offset={}",
                    record.topic(), record.kafkaPartition(), record.kafkaOffset(), e
                )
                reporter!!.report(record, e)
                null
            } else {
                throw ConnectException(
                    "Record conversion failed for topic=${record.topic()}, " +
                        "partition=${record.kafkaPartition()}, offset=${record.kafkaOffset()}. " +
                        "Configure errors.tolerance=all and a DLQ topic to skip bad records.",
                    e
                )
            }
        }
    }

    private fun getOrCreateWriter(topic: String): TableWriter {
        return tableWriters.getOrPut(topic) {
            val tableName = DeltaSinkConfig.resolveTableName(config.tableName, topic)
            val tablePath = if (basePath.isEmpty()) tableName else "$basePath/$tableName"
            log.info("Creating TableWriter: topic={}, tablePath={}", topic, tablePath)
            TableWriter(
                logStore = logStore,
                tablePath = tablePath,
                mergeKeys = config.mergeKeys,
                writeMode = config.writeMode,
                schemaEvolutionEnabled = config.schemaEvolutionEnabled
            )
        }
    }

    private fun flushWriter(writer: TableWriter) {
        try {
            val flushedOffsets = writer.flush()
            committedOffsets.putAll(flushedOffsets)
            lastFlushTimeMs = clock()
        } catch (e: CommitConflictException) {
            // Retries exhausted in DeltaTransaction - ask Connect to retry the task
            throw RetriableException("Delta commit conflict after retries: ${e.message}", e)
        } catch (e: ConnectException) {
            // Schema incompatibility or other fatal errors - let Connect fail the task
            throw e
        } catch (e: java.io.IOException) {
            // Storage I/O error - transient, ask Connect to retry
            throw RetriableException("Storage I/O error during flush: ${e.message}", e)
        }
    }

    private fun flushAll() {
        for ((_, writer) in tableWriters) {
            if (writer.hasBufferedRecords()) {
                flushWriter(writer)
            }
        }
        lastFlushTimeMs = clock()
    }

    companion object {

        /**
         * Resolve the base path from the storage path using the appropriate
         * storage provider.
         */
        internal fun resolveBasePath(config: DeltaSinkConfig): String {
            return try {
                val provider = StorageProviderRegistry.resolve(config.storagePath)
                provider.parseBasePath(config.storagePath)
            } catch (_: Exception) {
                // Fallback for plain paths
                config.storagePath.trimEnd('/')
            }
        }

        /**
         * Create a [DeltaLogStore] via the storage provider resolved from the
         * storage path URI scheme.
         */
        private fun createLogStoreFromProvider(config: DeltaSinkConfig): DeltaLogStore {
            val provider = StorageProviderRegistry.resolve(config.storagePath)
            return provider.createLogStore(config.originalsStrings())
        }
    }
}

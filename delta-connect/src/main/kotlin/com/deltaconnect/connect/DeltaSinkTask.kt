package com.deltaconnect.connect

import com.deltaconnect.catalog.DatabricksSqlClient
import com.deltaconnect.catalog.UnityCatalogSync
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
import org.slf4j.MDC

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
    private var catalogSync: UnityCatalogSync? = null
    private var metricsRegistry: CloseableMeterRegistry? = null
    var metrics: ConnectorMetrics = ConnectorMetrics()

    private val tableWriters = mutableMapOf<String, TableWriter>()
    private val tablePaths = mutableMapOf<String, String>() // topic → tablePath
    private val committedOffsets = mutableMapOf<TopicPartition, Long>()
    private var lastFlushTimeMs: Long = 0L

    // Visible for testing: allows injecting custom factories
    var logStoreFactory: (DeltaSinkConfig) -> DeltaLogStore = ::createLogStoreFromProvider
    var converterFactory: (DeltaSinkConfig) -> RecordConverter = { cfg ->
        when (cfg.writeMode) {
            DeltaSinkConfig.WriteMode.APPEND -> AppendRecordConverter()
            DeltaSinkConfig.WriteMode.CDC -> DebeziumRecordConverter(cfg.cdcEnvelopeFormat)
            DeltaSinkConfig.WriteMode.UPSERT -> throw UnsupportedOperationException(
                "UpsertRecordConverter not yet implemented",
            )
        }
    }
    var clock: () -> Long = System::currentTimeMillis

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
        reporter =
            try {
                context.errantRecordReporter()
            } catch (_: NoSuchMethodError) {
                null
            } catch (_: NoClassDefFoundError) {
                null
            }

        // Metrics: JMX always on, OTLP opt-in
        metricsRegistry =
            MetricsRegistryFactory.create(
                otlpEndpoint = config.metricsOtlpEndpoint.ifBlank { null },
            )
        metrics = ConnectorMetrics(metricsRegistry!!.registry)

        // Unity Catalog sync (optional)
        if (config.unityCatalogEnabled) {
            val client =
                DatabricksSqlClient(
                    workspaceUrl = config.unityCatalogWorkspaceUrl,
                    warehouseId = config.unityCatalogWarehouseId,
                    tokenSupplier = { config.unityCatalogToken },
                )
            catalogSync =
                UnityCatalogSync(
                    client = client,
                    catalog = config.unityCatalogName,
                    schema = config.unityCatalogSchema,
                    syncIntervalMs = config.unityCatalogSyncIntervalMs,
                    clock = clock,
                )
            log.info(
                "Unity Catalog sync enabled: catalog={}, schema={}",
                config.unityCatalogName,
                config.unityCatalogSchema,
            )
        }

        log.info(
            "DeltaSinkTask started: writeMode={}, batchSize={}, intervalMs={}, " +
                "schemaEvolution={}, dlqEnabled={}, unityCatalog={}",
            config.writeMode,
            config.mergeBatchSize,
            config.mergeIntervalMs,
            config.schemaEvolutionEnabled,
            reporter != null,
            config.unityCatalogEnabled,
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
                    tp,
                    committedOffset,
                    committedOffset + 1,
                )
            }
        }

        if (seekOffsets.isNotEmpty()) {
            context.offset(seekOffsets)
        }
    }

    override fun put(records: Collection<SinkRecord>) {
        if (records.isEmpty()) return

        val effectiveDeleteEnabled =
            config.mergeDeleteEnabled &&
                config.writeMode != DeltaSinkConfig.WriteMode.UPSERT

        var convertedCount = 0L
        for (record in records) {
            MDC.put("topic", record.topic())
            MDC.put("partition", record.kafkaPartition().toString())
            try {
                val sourceRecord = convertRecord(record, effectiveDeleteEnabled) ?: continue
                val topic = record.topic()
                val tp = TopicPartition(topic, record.kafkaPartition())
                getOrCreateWriter(topic).buffer(sourceRecord, tp, record.kafkaOffset())
                convertedCount++
            } finally {
                MDC.remove("topic")
                MDC.remove("partition")
            }
        }

        // Record-level metrics by topic
        records.groupBy { it.topic() }.forEach { (topic, topicRecords) ->
            metrics.recordsReceived(topic, topicRecords.size.toLong())
        }
        if (convertedCount > 0) {
            metrics.recordsConverted(records.first().topic(), convertedCount)
        }

        // Check batch size trigger per topic
        for ((topic, writer) in tableWriters) {
            if (writer.bufferSize() >= config.mergeBatchSize) {
                flushWriter(topic, writer)
            }
        }

        // Check time-based trigger
        if (clock() - lastFlushTimeMs >= config.mergeIntervalMs) {
            flushAll()
        }
    }

    override fun preCommit(
        currentOffsets: Map<TopicPartition, OffsetAndMetadata>,
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
                flushWriter(topic, writer)
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
        tablePaths.clear()
        committedOffsets.clear()
        metricsRegistry?.close()
        metricsRegistry = null
    }

    /**
     * Convert a [SinkRecord] to a [com.deltaconnect.protocol.merge.SourceRecord],
     * reporting data errors to the DLQ if configured.
     */
    private fun convertRecord(
        record: SinkRecord,
        deleteEnabled: Boolean,
    ): com.deltaconnect.protocol.merge.SourceRecord? =
        try {
            converter.convert(record, deleteEnabled)
        } catch (
            @Suppress("TooGenericExceptionCaught") e: Exception,
        ) {
            // Catch broadly to route any conversion failure to DLQ rather than failing the task
            if (reporter != null) {
                metrics.recordsDlq(record.topic())
                log.warn(
                    "Record conversion failed, sending to DLQ: topic={}, partition={}, offset={}",
                    record.topic(),
                    record.kafkaPartition(),
                    record.kafkaOffset(),
                    e,
                )
                reporter!!.report(record, e)
                null
            } else {
                throw ConnectException(
                    "Record conversion failed for topic=${record.topic()}, " +
                        "partition=${record.kafkaPartition()}, offset=${record.kafkaOffset()}. " +
                        "Configure errors.tolerance=all and a DLQ topic to skip bad records.",
                    e,
                )
            }
        }

    private fun getOrCreateWriter(topic: String): TableWriter =
        tableWriters.getOrPut(topic) {
            val tableName = DeltaSinkConfig.resolveTableName(config.tableName, topic)
            val tablePath = if (basePath.isEmpty()) tableName else "$basePath/$tableName"
            tablePaths[topic] = tablePath
            log.info("Creating TableWriter: topic={}, tablePath={}", topic, tablePath)
            TableWriter(
                logStore = logStore,
                tablePath = tablePath,
                mergeKeys = config.mergeKeys,
                writeMode = config.writeMode,
                schemaEvolutionEnabled = config.schemaEvolutionEnabled,
                topic = topic,
                metrics = metrics,
            )
        }

    private fun flushWriter(
        topic: String,
        writer: TableWriter,
    ) {
        MDC.put("topic", topic)
        MDC.put("tablePath", tablePaths[topic] ?: "")
        try {
            val flushedOffsets = writer.flush()
            committedOffsets.putAll(flushedOffsets)
            lastFlushTimeMs = clock()

            val sync = catalogSync
            if (sync != null) {
                val path = tablePaths[topic]
                if (path != null) {
                    val tableName = DeltaSinkConfig.resolveTableName(config.tableName, topic)
                    val location = "${config.storagePath.trimEnd('/')}/$tableName"
                    sync.onTableFlush(tableName, location)
                }
            }
        } catch (e: CommitConflictException) {
            throw RetriableException("Delta commit conflict after retries: ${e.message}", e)
        } catch (e: ConnectException) {
            throw e
        } catch (e: java.io.IOException) {
            throw RetriableException("Storage I/O error during flush: ${e.message}", e)
        } finally {
            MDC.remove("topic")
            MDC.remove("tablePath")
        }
    }

    private fun flushAll() {
        for ((topic, writer) in tableWriters) {
            if (writer.hasBufferedRecords()) {
                flushWriter(topic, writer)
            }
        }
        lastFlushTimeMs = clock()
    }

    companion object {
        /**
         * Resolve the base path from the storage path using the appropriate
         * storage provider.
         */
        internal fun resolveBasePath(config: DeltaSinkConfig): String =
            try {
                val provider = StorageProviderRegistry.resolve(config.storagePath)
                provider.parseBasePath(config.storagePath)
            } catch (_: Exception) {
                // Fallback for plain paths
                config.storagePath.trimEnd('/')
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

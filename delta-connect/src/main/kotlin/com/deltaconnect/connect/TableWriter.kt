package com.deltaconnect.connect

import com.deltaconnect.protocol.DeltaTable
import com.deltaconnect.protocol.actions.AddFile
import com.deltaconnect.protocol.actions.Format
import com.deltaconnect.protocol.actions.MetaData
import com.deltaconnect.protocol.actions.SetTransaction
import com.deltaconnect.protocol.merge.DeltaMergeEngine
import com.deltaconnect.protocol.merge.SourceRecord
import com.deltaconnect.protocol.parquet.ParquetFileWriter
import com.deltaconnect.protocol.schema.AvroToDeltaConverter
import com.deltaconnect.protocol.schema.DeltaSchema
import com.deltaconnect.protocol.schema.DeltaType
import com.deltaconnect.protocol.schema.SchemaEvolution
import com.deltaconnect.protocol.storage.DeltaLogStore
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.errors.ConnectException
import org.slf4j.LoggerFactory
import java.util.UUID

/**
 * Manages a single Delta table for one topic.
 *
 * Buffers [SourceRecord]s and flushes them to the Delta table via
 * the merge engine (for upsert/cdc modes) or direct append.
 *
 * The Delta table is lazily created on the first flush when the schema
 * is known from the buffered records.
 *
 * @property logStore Storage backend for the Delta table.
 * @property tablePath Path of the Delta table relative to the storage root.
 * @property mergeKeys Column names used as merge keys.
 * @property writeMode How records are written (append, upsert, cdc).
 * @property schemaEvolutionEnabled Whether to automatically evolve the table
 *   schema when incoming records have new nullable columns or widened types.
 */
class TableWriter(
    private val logStore: DeltaLogStore,
    private val tablePath: String,
    private val mergeKeys: List<String>,
    private val writeMode: DeltaSinkConfig.WriteMode,
    private val schemaEvolutionEnabled: Boolean = true
) {

    private var table: DeltaTable? = null
    private var mergeEngine: DeltaMergeEngine? = null
    private var schema: DeltaType.StructType? = null

    private val buffer = mutableListOf<SourceRecord>()
    private val partitionOffsets = mutableMapOf<TopicPartition, Long>()

    /**
     * Buffer a source record along with its Kafka partition and offset.
     *
     * @param record The converted source record.
     * @param tp The topic partition this record came from.
     * @param offset The Kafka offset of this record.
     */
    fun buffer(record: SourceRecord, tp: TopicPartition, offset: Long) {
        buffer.add(record)
        partitionOffsets.merge(tp, offset) { old, new -> maxOf(old, new) }
    }

    /** Number of buffered records waiting to be flushed. */
    fun bufferSize(): Int = buffer.size

    /** Whether this writer has any buffered records. */
    fun hasBufferedRecords(): Boolean = buffer.isNotEmpty()

    /**
     * Flush all buffered records to the Delta table.
     *
     * Creates the table on first flush using the schema from the first record.
     * If schema evolution is enabled, the table schema is updated when incoming
     * records have new nullable columns or widened types.
     *
     * Returns the committed partition offsets (max offset per partition in this batch).
     *
     * @return Map of topic-partition to the highest committed Kafka offset,
     *   or empty map if the buffer was empty.
     * @throws ConnectException if schema is incompatible and cannot be evolved.
     */
    fun flush(): Map<TopicPartition, Long> {
        if (buffer.isEmpty()) return emptyMap()

        logger.info(
            "Flushing: table={}, records={}, partitions={}",
            tablePath, buffer.size, partitionOffsets.size
        )

        ensureTable()

        val txn = table!!.startTransaction()

        val schemaEvolved = evolveSchemaIfNeeded(txn)

        val writeRecords = if (schemaEvolved) {
            val evolvedAvro = AvroToDeltaConverter.toAvroSchema(schema!!)
            buffer.map { SourceRecord(projectRecord(it.record, evolvedAvro), it.operation) }
        } else {
            buffer
        }

        when (writeMode) {
            DeltaSinkConfig.WriteMode.APPEND -> {
                val records = writeRecords.map { it.record }
                val filePath = "$tablePath/part-${UUID.randomUUID()}.parquet"
                val writer = ParquetFileWriter(logStore, schema!!)
                val writeResult = writer.write(filePath, records)
                txn.addAction(
                    AddFile(
                        path = writeResult.filePath,
                        partitionValues = emptyMap(),
                        size = writeResult.fileSize,
                        modificationTime = System.currentTimeMillis(),
                        dataChange = true,
                        stats = writeResult.statsJson
                    )
                )
            }
            DeltaSinkConfig.WriteMode.CDC,
            DeltaSinkConfig.WriteMode.UPSERT -> {
                val snapshot = table!!.snapshot()
                val mergeResult = mergeEngine!!.merge(snapshot, writeRecords)
                txn.addActions(mergeResult.removeFiles)
                txn.addActions(mergeResult.addFiles)
            }
        }

        for ((tp, offset) in partitionOffsets) {
            txn.addAction(
                SetTransaction(
                    appId = makeAppId(tp),
                    version = offset,
                    lastUpdated = System.currentTimeMillis()
                )
            )
        }

        val operation = when (writeMode) {
            DeltaSinkConfig.WriteMode.APPEND -> "WRITE"
            DeltaSinkConfig.WriteMode.CDC -> "MERGE"
            DeltaSinkConfig.WriteMode.UPSERT -> "MERGE"
        }

        val committedVersion = txn.commit(
            operation = operation,
            engineInfo = ENGINE_INFO
        )

        logger.info(
            "Flush committed: table={}, version={}, records={}, operation={}",
            tablePath, committedVersion, buffer.size, operation
        )

        val flushedOffsets = partitionOffsets.toMap()
        buffer.clear()
        partitionOffsets.clear()
        return flushedOffsets
    }

    /**
     * Look up the last committed offset for a partition from the Delta transaction log.
     *
     * @param tp The topic-partition to look up.
     * @return The last committed Kafka offset, or null if no offset was recorded.
     */
    fun getCommittedOffset(tp: TopicPartition): Long? {
        val t = table ?: DeltaTable.forPath(logStore, tablePath)
        val snapshot = t.snapshot()
        if (snapshot.version < 0) return null
        val txn = snapshot.transactions[makeAppId(tp)]
        return txn?.version
    }

    /**
     * Check if incoming records have a different schema than the table and evolve
     * the table schema if needed. Returns true if the schema was evolved.
     */
    private fun evolveSchemaIfNeeded(
        txn: com.deltaconnect.protocol.DeltaTransaction
    ): Boolean {
        val firstRecord = buffer.first()
        val incomingDelta = AvroToDeltaConverter.toDeltaType(firstRecord.record.schema)

        if (incomingDelta == schema) return false

        val result = SchemaEvolution.merge(schema!!, incomingDelta)

        return when (result) {
            is SchemaEvolution.MergeResult.NoChange -> false
            is SchemaEvolution.MergeResult.Evolved -> {
                if (!schemaEvolutionEnabled) {
                    throw ConnectException(
                        "Schema changed for table $tablePath but schema evolution is disabled. " +
                            "Enable delta.schema.evolution=true or align source schema."
                    )
                }
                logger.info(
                    "Schema evolved: table={}, fields={} -> {}",
                    tablePath, schema!!.fields.size, result.schema.fields.size
                )
                schema = result.schema
                txn.addAction(
                    MetaData(
                        id = UUID.randomUUID().toString(),
                        format = Format(),
                        schemaString = DeltaSchema.toJson(schema!!),
                        partitionColumns = emptyList(),
                        configuration = emptyMap(),
                        createdTime = System.currentTimeMillis()
                    )
                )
                if (writeMode != DeltaSinkConfig.WriteMode.APPEND) {
                    mergeEngine = DeltaMergeEngine(logStore, schema!!, mergeKeys, tablePath)
                }
                true
            }
            is SchemaEvolution.MergeResult.Incompatible -> {
                throw ConnectException(
                    "Incompatible schema change for table $tablePath: ${result.reason}"
                )
            }
        }
    }

    private fun ensureTable() {
        if (table != null) return

        val firstRecord = buffer.first()
        val avroSchema = firstRecord.record.schema

        val t = DeltaTable.forPath(logStore, tablePath)
        val snapshot = t.snapshot()

        if (snapshot.version < 0) {
            schema = AvroToDeltaConverter.toDeltaType(avroSchema)
            table = DeltaTable.createOrReplace(
                logStore, tablePath, schema!!,
                engineInfo = ENGINE_INFO
            )
            logger.info("Created Delta table: path={}, schema={}", tablePath, schema)
        } else {
            table = t
            schema = DeltaSchema.fromJson(snapshot.metaData!!.schemaString)
            logger.info(
                "Opened existing Delta table: path={}, version={}",
                tablePath, snapshot.version
            )
        }

        if (writeMode != DeltaSinkConfig.WriteMode.APPEND) {
            mergeEngine = DeltaMergeEngine(logStore, schema!!, mergeKeys, tablePath)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(TableWriter::class.java)
        internal const val ENGINE_INFO = "delta-sink-connector/${DeltaSinkConnector.VERSION}"

        /**
         * Construct a stable app ID for SetTransaction offset tracking.
         *
         * Format: `delta-sink:{topic}:{partition}`
         */
        fun makeAppId(tp: TopicPartition): String =
            "delta-sink:${tp.topic()}:${tp.partition()}"

        /**
         * Project a [GenericRecord] to a target Avro schema. Fields present in the
         * target but missing from the source are set to null. Fields in the source
         * but absent from the target are dropped.
         */
        internal fun projectRecord(
            record: GenericRecord,
            targetSchema: org.apache.avro.Schema
        ): GenericRecord {
            if (record.schema == targetSchema) return record
            val projected = GenericData.Record(targetSchema)
            for (field in targetSchema.fields) {
                val sourceField = record.schema.getField(field.name())
                projected.put(field.name(), if (sourceField != null) record.get(field.name()) else null)
            }
            return projected
        }
    }
}

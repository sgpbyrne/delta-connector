package com.deltaconnect.protocol.log

import com.deltaconnect.protocol.DeltaSnapshot
import com.deltaconnect.protocol.actions.Action
import com.deltaconnect.protocol.actions.ActionSerializer
import com.deltaconnect.protocol.actions.AddFile
import com.deltaconnect.protocol.actions.MetaData
import com.deltaconnect.protocol.actions.Protocol
import com.deltaconnect.protocol.actions.SetTransaction
import com.deltaconnect.protocol.parquet.OutputStreamBackedOutputFile
import com.deltaconnect.protocol.storage.DeltaLogStore
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.slf4j.LoggerFactory

/**
 * Writes and reads Parquet checkpoints.
 *
 * A checkpoint captures the fully reconciled table state at a given version
 * as a Parquet file, enabling [TransactionLogReader] to skip replaying
 * earlier JSON commits. Contains: Protocol, MetaData, active AddFiles,
 * and SetTransactions. Does not contain RemoveFile or CommitInfo.
 *
 * Checkpoint file naming: `{tablePath}/_delta_log/%020d.checkpoint.parquet`
 *
 * @property logStore Storage backend for reading/writing checkpoint files.
 */
class CheckpointWriter(
    private val logStore: DeltaLogStore,
) {
    /**
     * Write a V1 checkpoint for the given snapshot.
     *
     * Writes a single Parquet file containing all reconciled actions,
     * then updates `_last_checkpoint` with the checkpoint metadata.
     *
     * @param tablePath Root path of the Delta table.
     * @param snapshot The snapshot to checkpoint. Must have version >= 0, Protocol, and MetaData.
     * @throws IllegalArgumentException if the snapshot is invalid for checkpointing.
     */
    fun writeCheckpoint(
        tablePath: String,
        snapshot: DeltaSnapshot,
    ) {
        require(snapshot.version >= 0) { "Cannot checkpoint empty table (version=${snapshot.version})" }
        require(snapshot.protocol != null) { "Snapshot must have Protocol for checkpointing" }
        require(snapshot.metaData != null) { "Snapshot must have MetaData for checkpointing" }

        val filePath = checkpointFilePath(tablePath, snapshot.version)
        val records = buildCheckpointRecords(snapshot)

        writeParquetFile(filePath, records)

        val checkpointInfo =
            LastCheckpointInfo(
                version = snapshot.version,
                size = records.size.toLong(),
            )
        logStore.writeLastCheckpoint(tablePath, checkpointInfo.toJson())

        logger.info(
            "Checkpoint written: table={}, version={}, actions={}",
            tablePath,
            snapshot.version,
            records.size,
        )
    }

    /**
     * Read a checkpoint Parquet file and return the actions it contains.
     *
     * @param tablePath Root path of the Delta table.
     * @param version The checkpoint version to read.
     * @return List of actions from the checkpoint.
     */
    fun readCheckpoint(
        tablePath: String,
        version: Long,
    ): List<Action> {
        val filePath = checkpointFilePath(tablePath, version)
        val records = readParquetFile(filePath)
        return records.mapNotNull { convertToAction(it) }
    }

    private fun buildCheckpointRecords(snapshot: DeltaSnapshot): List<GenericRecord> {
        val records = mutableListOf<GenericRecord>()

        records.add(protocolToRow(snapshot.protocol!!))
        records.add(metaDataToRow(snapshot.metaData!!))

        for (addFile in snapshot.activeFiles) {
            records.add(addFileToRow(addFile))
        }

        for ((_, txn) in snapshot.transactions) {
            records.add(txnToRow(txn))
        }

        return records
    }

    private fun protocolToRow(protocol: Protocol): GenericRecord {
        val row = GenericData.Record(checkpointSchema)
        val rec = GenericData.Record(protocolRecordSchema)
        rec.put("minReaderVersion", protocol.minReaderVersion)
        rec.put("minWriterVersion", protocol.minWriterVersion)
        rec.put("readerFeatures", protocol.readerFeatures?.toList())
        rec.put("writerFeatures", protocol.writerFeatures?.toList())
        row.put("protocol", rec)
        return row
    }

    private fun metaDataToRow(metaData: MetaData): GenericRecord {
        val row = GenericData.Record(checkpointSchema)
        val rec = GenericData.Record(metaDataRecordSchema)
        rec.put("id", metaData.id)
        rec.put("name", metaData.name)
        rec.put("description", metaData.description)

        val formatRec = GenericData.Record(formatRecordSchema)
        formatRec.put("provider", metaData.format.provider)
        formatRec.put("options", metaData.format.options)
        rec.put("format", formatRec)

        rec.put("schemaString", metaData.schemaString)
        rec.put("partitionColumns", metaData.partitionColumns)
        rec.put("configuration", metaData.configuration)
        rec.put("createdTime", metaData.createdTime)
        row.put("metaData", rec)
        return row
    }

    private fun addFileToRow(addFile: AddFile): GenericRecord {
        val row = GenericData.Record(checkpointSchema)
        val rec = GenericData.Record(addRecordSchema)
        rec.put("path", addFile.path)
        rec.put("partitionValues", addFile.partitionValues)
        rec.put("size", addFile.size)
        rec.put("modificationTime", addFile.modificationTime)
        rec.put("dataChange", addFile.dataChange)
        rec.put("stats", addFile.stats)
        rec.put("tags", addFile.tags)
        row.put("add", rec)
        return row
    }

    private fun txnToRow(txn: SetTransaction): GenericRecord {
        val row = GenericData.Record(checkpointSchema)
        val rec = GenericData.Record(txnRecordSchema)
        rec.put("appId", txn.appId)
        rec.put("version", txn.version)
        rec.put("lastUpdated", txn.lastUpdated)
        row.put("txn", rec)
        return row
    }

    private fun convertToAction(record: GenericRecord): Action? {
        val txn = record.get("txn") as? GenericRecord
        if (txn != null) {
            return SetTransaction(
                appId = txn.get("appId").toString(),
                version = txn.get("version") as Long,
                lastUpdated = txn.get("lastUpdated") as? Long,
            )
        }

        val add = record.get("add") as? GenericRecord
        if (add != null) {
            return AddFile(
                path = add.get("path").toString(),
                partitionValues = add.toStringMap("partitionValues"),
                size = add.get("size") as Long,
                modificationTime = add.get("modificationTime") as Long,
                dataChange = add.get("dataChange") as Boolean,
                stats = add.get("stats")?.toString(),
                tags = add.toStringMapOrNull("tags"),
            )
        }

        val meta = record.get("metaData") as? GenericRecord
        if (meta != null) {
            val format = meta.get("format") as GenericRecord
            return MetaData(
                id = meta.get("id").toString(),
                name = meta.get("name")?.toString(),
                description = meta.get("description")?.toString(),
                format =
                    com.deltaconnect.protocol.actions.Format(
                        provider = format.get("provider").toString(),
                        options = format.toStringMap("options"),
                    ),
                schemaString = meta.get("schemaString").toString(),
                partitionColumns = meta.toStringList("partitionColumns"),
                configuration = meta.toStringMap("configuration"),
                createdTime = meta.get("createdTime") as? Long,
            )
        }

        val proto = record.get("protocol") as? GenericRecord
        if (proto != null) {
            return Protocol(
                minReaderVersion = proto.get("minReaderVersion") as Int,
                minWriterVersion = proto.get("minWriterVersion") as Int,
                readerFeatures = proto.toStringSetOrNull("readerFeatures"),
                writerFeatures = proto.toStringSetOrNull("writerFeatures"),
            )
        }

        return null
    }

    @Suppress("UNCHECKED_CAST")
    private fun GenericRecord.toStringMap(field: String): Map<String, String> =
        (get(field) as Map<CharSequence, CharSequence>)
            .map { (k, v) -> k.toString() to v.toString() }
            .toMap()

    @Suppress("UNCHECKED_CAST")
    private fun GenericRecord.toStringMapOrNull(field: String): Map<String, String>? =
        (get(field) as? Map<CharSequence, CharSequence>)
            ?.map { (k, v) -> k.toString() to v.toString() }
            ?.toMap()

    @Suppress("UNCHECKED_CAST")
    private fun GenericRecord.toStringList(
        field: String,
    ): List<String> = (get(field) as List<CharSequence>).map { it.toString() }

    @Suppress("UNCHECKED_CAST")
    private fun GenericRecord.toStringSetOrNull(field: String): Set<String>? =
        (get(field) as? List<CharSequence>)?.map { it.toString() }?.toSet()

    private fun writeParquetFile(
        filePath: String,
        records: List<GenericRecord>,
    ) {
        val outputStream = logStore.createDataFile(filePath)
        val outputFile = OutputStreamBackedOutputFile(outputStream)

        val writer: ParquetWriter<GenericRecord> =
            AvroParquetWriter
                .builder<GenericRecord>(outputFile)
                .withSchema(checkpointSchema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()

        writer.use { w ->
            for (record in records) {
                w.write(record)
            }
        }
    }

    private fun readParquetFile(filePath: String): List<GenericRecord> {
        val inputFile = logStore.readDataFileAsInputFile(filePath)
        val reader = AvroParquetReader.builder<GenericRecord>(inputFile).build()
        val records = mutableListOf<GenericRecord>()

        reader.use { r ->
            while (true) {
                val record = r.read() ?: break
                records.add(record)
            }
        }

        return records
    }

    companion object {
        private val logger = LoggerFactory.getLogger(CheckpointWriter::class.java)

        const val DEFAULT_CHECKPOINT_INTERVAL: Long = 10

        fun checkpointFilePath(
            tablePath: String,
            version: Long,
        ): String = "$tablePath/_delta_log/${ActionSerializer.checkpointFileName(version)}"

        private fun extractRecordSchema(
            parentSchema: Schema,
            fieldName: String,
        ): Schema {
            val field =
                requireNotNull(parentSchema.getField(fieldName)) {
                    "Checkpoint schema missing expected field '$fieldName'"
                }
            val unionTypes = field.schema().types
            require(unionTypes.size >= 2) {
                "Expected union with at least 2 types for field '$fieldName', " +
                    "got ${unionTypes.size}"
            }
            return unionTypes[1]
        }

        private val CHECKPOINT_SCHEMA_JSON =
            """
            {
              "type": "record",
              "name": "CheckpointRow",
              "namespace": "com.deltaconnect.protocol.checkpoint",
              "fields": [
                {
                  "name": "txn",
                  "type": ["null", {
                    "type": "record",
                    "name": "SetTransaction",
                    "fields": [
                      {"name": "appId", "type": "string"},
                      {"name": "version", "type": "long"},
                      {"name": "lastUpdated", "type": ["null", "long"], "default": null}
                    ]
                  }],
                  "default": null
                },
                {
                  "name": "add",
                  "type": ["null", {
                    "type": "record",
                    "name": "AddFile",
                    "fields": [
                      {"name": "path", "type": "string"},
                      {"name": "partitionValues", "type": {"type": "map", "values": "string"}},
                      {"name": "size", "type": "long"},
                      {"name": "modificationTime", "type": "long"},
                      {"name": "dataChange", "type": "boolean"},
                      {"name": "stats", "type": ["null", "string"], "default": null},
                      {"name": "tags", "type": ["null", {"type": "map", "values": "string"}], "default": null}
                    ]
                  }],
                  "default": null
                },
                {
                  "name": "metaData",
                  "type": ["null", {
                    "type": "record",
                    "name": "MetaData",
                    "fields": [
                      {"name": "id", "type": "string"},
                      {"name": "name", "type": ["null", "string"], "default": null},
                      {"name": "description", "type": ["null", "string"], "default": null},
                      {"name": "format", "type": {
                        "type": "record",
                        "name": "Format",
                        "fields": [
                          {"name": "provider", "type": "string"},
                          {"name": "options", "type": {"type": "map", "values": "string"}}
                        ]
                      }},
                      {"name": "schemaString", "type": "string"},
                      {"name": "partitionColumns", "type": {"type": "array", "items": "string"}},
                      {"name": "configuration", "type": {"type": "map", "values": "string"}},
                      {"name": "createdTime", "type": ["null", "long"], "default": null}
                    ]
                  }],
                  "default": null
                },
                {
                  "name": "protocol",
                  "type": ["null", {
                    "type": "record",
                    "name": "Protocol",
                    "fields": [
                      {"name": "minReaderVersion", "type": "int"},
                      {"name": "minWriterVersion", "type": "int"},
                      {"name": "readerFeatures", "type": ["null", {"type": "array", "items": "string"}], "default": null},
                      {"name": "writerFeatures", "type": ["null", {"type": "array", "items": "string"}], "default": null}
                    ]
                  }],
                  "default": null
                }
              ]
            }
            """.trimIndent()

        val checkpointSchema: Schema = Schema.Parser().parse(CHECKPOINT_SCHEMA_JSON)
        val txnRecordSchema: Schema = extractRecordSchema(checkpointSchema, "txn")
        val addRecordSchema: Schema = extractRecordSchema(checkpointSchema, "add")
        val metaDataRecordSchema: Schema = extractRecordSchema(checkpointSchema, "metaData")
        val formatRecordSchema: Schema = metaDataRecordSchema.getField("format").schema()
        val protocolRecordSchema: Schema = extractRecordSchema(checkpointSchema, "protocol")
    }
}

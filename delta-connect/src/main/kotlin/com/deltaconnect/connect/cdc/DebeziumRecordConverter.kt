package com.deltaconnect.connect.cdc

import com.deltaconnect.connect.ConnectToAvroConverter
import com.deltaconnect.connect.DeltaSinkConfig.CdcEnvelopeFormat
import com.deltaconnect.connect.RecordConverter
import com.deltaconnect.protocol.merge.MergeOperation
import com.deltaconnect.protocol.merge.SourceRecord
import com.deltaconnect.protocol.schema.AvroToDeltaConverter
import com.deltaconnect.protocol.schema.DeltaType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.LoggerFactory
import org.apache.avro.Schema as AvroSchema

/**
 * Converts Debezium CDC records to Delta Lake [SourceRecord]s.
 *
 * Supports two envelope formats:
 * - [CdcEnvelopeFormat.DEBEZIUM_FULL]: Standard Debezium envelope with
 *   `before`, `after`, `op`, `source`, `ts_ms` fields.
 * - [CdcEnvelopeFormat.DEBEZIUM_FLATTENED]: Flattened records produced by
 *   Debezium's ExtractNewRecordState SMT.
 *
 * Debezium operation mapping:
 * - `r` (read/snapshot) → [MergeOperation.INSERT]
 * - `c` (create) → [MergeOperation.INSERT]
 * - `u` (update) → [MergeOperation.UPDATE]
 * - `d` (delete) → [MergeOperation.DELETE]
 */
class DebeziumRecordConverter(
    private val envelopeFormat: CdcEnvelopeFormat,
) : RecordConverter {
    private val log = LoggerFactory.getLogger(DebeziumRecordConverter::class.java)

    @Volatile private var cachedAvroSchema: AvroSchema? = null

    @Volatile private var cachedConnectSchema: Schema? = null

    override fun convert(
        record: SinkRecord,
        deleteEnabled: Boolean,
    ): SourceRecord? =
        when (envelopeFormat) {
            CdcEnvelopeFormat.DEBEZIUM_FULL ->
                convertFullEnvelope(record, deleteEnabled)
            CdcEnvelopeFormat.DEBEZIUM_FLATTENED ->
                convertFlattened(record, deleteEnabled)
        }

    override fun extractSchema(record: SinkRecord): DeltaType.StructType {
        val avroSchema =
            when (envelopeFormat) {
                CdcEnvelopeFormat.DEBEZIUM_FULL -> {
                    val afterSchema =
                        record.valueSchema().field("after")?.schema()
                            ?: record.valueSchema().field("before")?.schema()
                            ?: throw IllegalArgumentException(
                                "Debezium envelope must have 'after' or 'before' field",
                            )
                    getOrCreateAvroSchema(afterSchema)
                }
                CdcEnvelopeFormat.DEBEZIUM_FLATTENED -> {
                    val valueSchema =
                        record.valueSchema()
                            ?: throw IllegalArgumentException("Record must have a value schema")
                    getOrCreateAvroSchema(filterMetadataFields(valueSchema))
                }
            }
        return AvroToDeltaConverter.toDeltaType(avroSchema)
    }

    private fun convertFullEnvelope(
        record: SinkRecord,
        deleteEnabled: Boolean,
    ): SourceRecord? {
        // Skip tombsone values - these follow real delete events for log compaction
        val envelope = record.value() as? Struct ?: return null

        val op = envelope.getString("op")
        val operation = mapOperation(op)
        if (operation == null) {
            log.warn(
                "Skipping record with unknown Debezium op={}, topic={}, offset={}",
                op,
                record.topic(),
                record.kafkaOffset(),
            )
            return null
        }

        if (operation == MergeOperation.DELETE && !deleteEnabled) {
            return null
        }

        val dataStruct: Struct
        val dataSchema: Schema
        if (operation == MergeOperation.DELETE) {
            dataStruct = envelope.getStruct("before")
                ?: throw IllegalArgumentException(
                    "DELETE record (offset=${record.kafkaOffset()}) missing 'before' field",
                )
            dataSchema = record.valueSchema().field("before").schema()
        } else {
            dataStruct = envelope.getStruct("after")
                ?: throw IllegalArgumentException(
                    "op='$op' record (offset=${record.kafkaOffset()}) missing 'after' field",
                )
            dataSchema = record.valueSchema().field("after").schema()
        }

        val avroSchema = getOrCreateAvroSchema(dataSchema)
        val genericRecord = ConnectToAvroConverter.toGenericRecord(dataStruct, avroSchema)

        return SourceRecord(genericRecord, operation)
    }

    private fun convertFlattened(
        record: SinkRecord,
        deleteEnabled: Boolean,
    ): SourceRecord? {
        val value = record.value() as? Struct ?: return null

        val operation = detectFlattenedOperation(record, value)

        if (operation == MergeOperation.DELETE && !deleteEnabled) {
            return null
        }

        val filteredSchema = filterMetadataFields(record.valueSchema())
        val avroSchema = getOrCreateAvroSchema(filteredSchema)
        val genericRecord = ConnectToAvroConverter.toGenericRecord(value, avroSchema)

        return SourceRecord(genericRecord, operation)
    }

    private fun detectFlattenedOperation(
        record: SinkRecord,
        value: Struct,
    ): MergeOperation {
        val opHeader = record.headers()?.lastWithName("__op")
        if (opHeader != null) {
            val op = opHeader.value()?.toString()
            return mapOperation(op) ?: MergeOperation.INSERT
        }

        if (value.schema().field("__deleted") != null) {
            val deleted = value.getString("__deleted")
            if (deleted == "true") return MergeOperation.DELETE
        }

        if (value.schema().field("__op") != null) {
            val op = value.getString("__op")
            return mapOperation(op) ?: MergeOperation.INSERT
        }

        return MergeOperation.INSERT
    }

    private fun getOrCreateAvroSchema(connectSchema: Schema): AvroSchema {
        if (connectSchema === cachedConnectSchema) {
            return cachedAvroSchema!!
        }
        val avro = ConnectToAvroConverter.toAvroSchema(connectSchema)
        cachedConnectSchema = connectSchema
        cachedAvroSchema = avro
        return avro
    }

    companion object {
        /**
         * Map a Debezium operation code to a [MergeOperation].
         *
         * @return The merge operation, or null for unknown ops.
         */
        fun mapOperation(op: String?): MergeOperation? =
            when (op) {
                "r" -> MergeOperation.INSERT // snapshot read
                "c" -> MergeOperation.INSERT // create
                "u" -> MergeOperation.UPDATE // update
                "d" -> MergeOperation.DELETE // delete
                else -> null
            }

        /**
         * Filter out Debezium metadata fields (prefixed with `__`) from a
         * Connect schema, returning a schema with only data columns.
         */
        fun filterMetadataFields(schema: Schema): Schema {
            val dataFields = schema.fields().filter { !it.name().startsWith("__") }
            if (dataFields.size == schema.fields().size) return schema
            val builder = SchemaBuilder.struct()
            if (schema.isOptional) builder.optional()
            for (field in dataFields) {
                builder.field(field.name(), field.schema())
            }
            return builder.build()
        }
    }
}

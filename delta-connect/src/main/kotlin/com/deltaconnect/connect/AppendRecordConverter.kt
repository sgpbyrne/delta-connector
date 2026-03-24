package com.deltaconnect.connect

import com.deltaconnect.protocol.merge.MergeOperation
import com.deltaconnect.protocol.merge.SourceRecord
import com.deltaconnect.protocol.schema.AvroToDeltaConverter
import com.deltaconnect.protocol.schema.DeltaType
import org.apache.avro.Schema as AvroSchema
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord

/**
 * Record converter for append-only write mode.
 *
 * Every record is converted to a [SourceRecord] with [MergeOperation.INSERT].
 * No merge key handling or operation detection - all records are inserts.
 */
class AppendRecordConverter : RecordConverter {

    @Volatile private var cachedAvroSchema: AvroSchema? = null
    @Volatile private var cachedConnectSchema: Schema? = null

    override fun convert(record: SinkRecord, deleteEnabled: Boolean): SourceRecord? {
        val value = record.value() as? Struct ?: return null
        val avroSchema = getOrCreateAvroSchema(record.valueSchema())
        val genericRecord = ConnectToAvroConverter.toGenericRecord(value, avroSchema)
        return SourceRecord(genericRecord, MergeOperation.INSERT)
    }

    override fun extractSchema(record: SinkRecord): DeltaType.StructType {
        val avroSchema = getOrCreateAvroSchema(record.valueSchema())
        return AvroToDeltaConverter.toDeltaType(avroSchema)
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
}

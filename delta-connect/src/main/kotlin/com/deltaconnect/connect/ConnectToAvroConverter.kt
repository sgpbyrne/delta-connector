package com.deltaconnect.connect

import org.apache.avro.LogicalTypes
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import java.math.BigDecimal
import java.nio.ByteBuffer
import org.apache.avro.Schema as AvroSchema
import org.apache.kafka.connect.data.Date as ConnectDate
import org.apache.kafka.connect.data.Decimal as ConnectDecimal
import org.apache.kafka.connect.data.Time as ConnectTime
import org.apache.kafka.connect.data.Timestamp as ConnectTimestamp

/**
 * Converts Kafka Connect schemas and data to Apache Avro equivalents.
 *
 * Handles standard Connect types, standard Connect logical types,
 * and Debezium specific logical types (for CDC from SQL Server and
 * other databases).
 */
object ConnectToAvroConverter {
    private const val DEFAULT_DECIMAL_PRECISION = 38

    /**
     * Convert a Connect STRUCT schema to an Avro record schema.
     */
    fun toAvroSchema(
        connectSchema: Schema,
        recordName: String = "record",
    ): AvroSchema {
        require(connectSchema.type() == Schema.Type.STRUCT) {
            "Expected STRUCT schema, got ${connectSchema.type()}"
        }
        val fields =
            connectSchema.fields().map { field ->
                val avroType = connectFieldToAvro(field.schema())
                val fieldSchema =
                    if (field.schema().isOptional) {
                        AvroSchema.createUnion(
                            AvroSchema.create(AvroSchema.Type.NULL),
                            avroType,
                        )
                    } else {
                        avroType
                    }
                val defaultValue =
                    if (field.schema().isOptional) {
                        AvroSchema.Field.NULL_DEFAULT_VALUE
                    } else {
                        null
                    }
                AvroSchema.Field(field.name(), fieldSchema, null, defaultValue)
            }
        return AvroSchema.createRecord(recordName, null, "com.deltaconnect", false, fields)
    }

    /**
     * Convert a Connect [Struct] to an Avro [GenericRecord].
     *
     * Fields present in the struct's schema but absent from the Avro schema
     * are silently skipped (useful for filtering metadata fields).
     *
     * @param struct The Connect Struct to convert.
     * @param avroSchema The target Avro record schema.
     */
    fun toGenericRecord(
        struct: Struct,
        avroSchema: AvroSchema,
    ): GenericRecord {
        val record = GenericData.Record(avroSchema)
        for (field in struct.schema().fields()) {
            val avroField = avroSchema.getField(field.name()) ?: continue
            val value = struct.get(field)
            record.put(
                field.name(),
                convertValue(value, field.schema(), avroField.schema()),
            )
        }
        return record
    }

    private fun connectFieldToAvro(schema: Schema): AvroSchema {
        val logicalName = schema.name()
        if (logicalName != null) {
            logicalTypeToAvro(logicalName, schema)?.let { return it }
        }
        return connectPrimitiveToAvro(schema)
    }

    private fun logicalTypeToAvro(
        logicalName: String,
        schema: Schema,
    ): AvroSchema? =
        when (logicalName) {
            // Standard Connect decimal
            ConnectDecimal.LOGICAL_NAME -> {
                val scale = schema.parameters()?.get("scale")?.toInt() ?: 0
                val precision =
                    schema
                        .parameters()
                        ?.get("connect.decimal.precision")
                        ?.toInt()
                        ?: DEFAULT_DECIMAL_PRECISION
                LogicalTypes
                    .decimal(precision, scale)
                    .addToSchema(AvroSchema.create(AvroSchema.Type.BYTES))
            }
            // Standard Connect date (days since epoch)
            ConnectDate.LOGICAL_NAME,
            DEBEZIUM_DATE,
            ->
                LogicalTypes
                    .date()
                    .addToSchema(AvroSchema.create(AvroSchema.Type.INT))
            // Standard Connect timestamp (ms)
            ConnectTimestamp.LOGICAL_NAME,
            DEBEZIUM_TIMESTAMP,
            ->
                LogicalTypes
                    .timestampMicros()
                    .addToSchema(AvroSchema.create(AvroSchema.Type.LONG))
            // Debezium microsecond timestamp (datetime2)
            DEBEZIUM_MICRO_TIMESTAMP ->
                LogicalTypes
                    .timestampMicros()
                    .addToSchema(AvroSchema.create(AvroSchema.Type.LONG))
            // Debezium nanosecond timestamp - truncate to micros
            DEBEZIUM_NANO_TIMESTAMP ->
                LogicalTypes
                    .timestampMicros()
                    .addToSchema(AvroSchema.create(AvroSchema.Type.LONG))
            // Debezium zoned timestamp (datetimeoffset) - ISO string
            DEBEZIUM_ZONED_TIMESTAMP ->
                AvroSchema.create(AvroSchema.Type.STRING)
            // Time types - store as long
            ConnectTime.LOGICAL_NAME,
            DEBEZIUM_TIME,
            DEBEZIUM_MICRO_TIME,
            DEBEZIUM_NANO_TIME,
            ->
                AvroSchema.create(AvroSchema.Type.LONG)
            // Debezium variable scale decimal - string representation
            DEBEZIUM_VARIABLE_SCALE_DECIMAL ->
                AvroSchema.create(AvroSchema.Type.STRING)
            else -> null
        }

    private fun connectPrimitiveToAvro(schema: Schema): AvroSchema =
        when (schema.type()) {
            Schema.Type.INT8,
            Schema.Type.INT16,
            Schema.Type.INT32,
            -> AvroSchema.create(AvroSchema.Type.INT)
            Schema.Type.INT64 -> AvroSchema.create(AvroSchema.Type.LONG)
            Schema.Type.FLOAT32 -> AvroSchema.create(AvroSchema.Type.FLOAT)
            Schema.Type.FLOAT64 -> AvroSchema.create(AvroSchema.Type.DOUBLE)
            Schema.Type.BOOLEAN -> AvroSchema.create(AvroSchema.Type.BOOLEAN)
            Schema.Type.STRING -> AvroSchema.create(AvroSchema.Type.STRING)
            Schema.Type.BYTES -> AvroSchema.create(AvroSchema.Type.BYTES)
            Schema.Type.STRUCT -> toAvroSchema(schema)
            Schema.Type.ARRAY -> {
                val itemAvro = connectFieldToAvro(schema.valueSchema())
                val itemSchema =
                    if (schema.valueSchema().isOptional) {
                        AvroSchema.createUnion(
                            AvroSchema.create(AvroSchema.Type.NULL),
                            itemAvro,
                        )
                    } else {
                        itemAvro
                    }
                AvroSchema.createArray(itemSchema)
            }
            Schema.Type.MAP -> {
                val valueAvro = connectFieldToAvro(schema.valueSchema())
                val valueSchema =
                    if (schema.valueSchema().isOptional) {
                        AvroSchema.createUnion(
                            AvroSchema.create(AvroSchema.Type.NULL),
                            valueAvro,
                        )
                    } else {
                        valueAvro
                    }
                AvroSchema.createMap(valueSchema)
            }
            else -> throw IllegalArgumentException(
                "Unsupported Connect schema type: ${schema.type()}",
            )
        }

    private fun convertValue(
        value: Any?,
        connectSchema: Schema,
        avroSchema: AvroSchema,
    ): Any? {
        if (value == null) return null

        val logicalName = connectSchema.name()
        if (logicalName != null) {
            convertLogicalValue(logicalName, value)?.let { return it }
        }

        return convertPrimitiveValue(value, connectSchema, resolveUnion(avroSchema))
    }

    private fun convertLogicalValue(
        logicalName: String,
        value: Any,
    ): Any? =
        when (logicalName) {
            ConnectDecimal.LOGICAL_NAME -> {
                val decimal = value as BigDecimal
                ByteBuffer.wrap(decimal.unscaledValue().toByteArray())
            }
            ConnectDate.LOGICAL_NAME -> {
                (value as java.util.Date).time / 86_400_000L
            }
            ConnectTimestamp.LOGICAL_NAME -> {
                (value as java.util.Date).time * 1000L
            }
            ConnectTime.LOGICAL_NAME -> {
                (value as java.util.Date).time
            }
            DEBEZIUM_DATE -> (value as Number).toInt()
            DEBEZIUM_MICRO_TIMESTAMP -> (value as Number).toLong()
            DEBEZIUM_TIMESTAMP -> (value as Number).toLong() * 1000L // ms → µs
            DEBEZIUM_NANO_TIMESTAMP -> (value as Number).toLong() / 1000L // ns → µs
            DEBEZIUM_ZONED_TIMESTAMP -> value.toString()
            DEBEZIUM_MICRO_TIME -> (value as Number).toLong()
            DEBEZIUM_TIME -> (value as Number).toLong()
            DEBEZIUM_NANO_TIME -> (value as Number).toLong()
            DEBEZIUM_VARIABLE_SCALE_DECIMAL -> {
                if (value is Struct) {
                    val scale = value.getInt32("scale")
                    val unscaledBytes = value.getBytes("value")
                    val unscaled = java.math.BigInteger(unscaledBytes)
                    BigDecimal(unscaled, scale).toPlainString()
                } else {
                    value.toString()
                }
            }
            else -> null
        }

    private fun convertPrimitiveValue(
        value: Any,
        connectSchema: Schema,
        avroSchema: AvroSchema,
    ): Any =
        when (connectSchema.type()) {
            Schema.Type.INT8 -> (value as Number).toInt()
            Schema.Type.INT16 -> (value as Number).toInt()
            Schema.Type.INT32 -> (value as Number).toInt()
            Schema.Type.INT64 -> (value as Number).toLong()
            Schema.Type.FLOAT32 -> (value as Number).toFloat()
            Schema.Type.FLOAT64 -> (value as Number).toDouble()
            Schema.Type.BOOLEAN -> value as Boolean
            Schema.Type.STRING -> value.toString()
            Schema.Type.BYTES ->
                when (value) {
                    is ByteBuffer -> value
                    is ByteArray -> ByteBuffer.wrap(value)
                    else -> throw IllegalArgumentException(
                        "Cannot convert ${value::class} to bytes",
                    )
                }
            Schema.Type.STRUCT -> toGenericRecord(value as Struct, avroSchema)
            Schema.Type.ARRAY -> {
                @Suppress("UNCHECKED_CAST")
                val list = value as List<Any?>
                val elemSchema = connectSchema.valueSchema()
                val avroElemSchema = avroSchema.elementType
                list.map { convertValue(it, elemSchema, avroElemSchema) }
            }
            Schema.Type.MAP -> {
                @Suppress("UNCHECKED_CAST")
                val map = value as Map<Any?, Any?>
                val valSchema = connectSchema.valueSchema()
                val avroValSchema = avroSchema.valueType
                map.entries.associate { (k, v) ->
                    k.toString() to convertValue(v, valSchema, avroValSchema)
                }
            }
            else -> value
        }

    private fun resolveUnion(schema: AvroSchema): AvroSchema =
        if (schema.type == AvroSchema.Type.UNION) {
            schema.types.first { it.type != AvroSchema.Type.NULL }
        } else {
            schema
        }

    private const val DEBEZIUM_DATE = "io.debezium.time.Date"
    private const val DEBEZIUM_TIMESTAMP = "io.debezium.time.Timestamp"
    private const val DEBEZIUM_MICRO_TIMESTAMP = "io.debezium.time.MicroTimestamp"
    private const val DEBEZIUM_NANO_TIMESTAMP = "io.debezium.time.NanoTimestamp"
    private const val DEBEZIUM_ZONED_TIMESTAMP = "io.debezium.time.ZonedTimestamp"
    private const val DEBEZIUM_TIME = "io.debezium.time.Time"
    private const val DEBEZIUM_MICRO_TIME = "io.debezium.time.MicroTime"
    private const val DEBEZIUM_NANO_TIME = "io.debezium.time.NanoTime"
    private const val DEBEZIUM_VARIABLE_SCALE_DECIMAL = "io.debezium.data.VariableScaleDecimal"
}

package com.deltaconnect.protocol.schema

import org.apache.avro.LogicalTypes
import org.apache.avro.Schema as AvroSchema
import org.apache.avro.SchemaBuilder

/**
 * Bidirectional conversion between Delta Lake schemas and Apache Avro schemas.
 *
 * Used by the Parquet writer/reader which works with Avro's GenericRecord.
 */
object AvroToDeltaConverter {

    /** Convert a Delta StructType to an Avro Schema (record type). */
    fun toAvroSchema(structType: DeltaType.StructType, recordName: String = "record"): AvroSchema {
        val fields = structType.fields.map { field ->
            val avroType = deltaTypeToAvro(field.type)
            val fieldSchema = if (field.nullable) {
                AvroSchema.createUnion(AvroSchema.create(AvroSchema.Type.NULL), avroType)
            } else {
                avroType
            }
            AvroSchema.Field(field.name, fieldSchema, null, if (field.nullable) AvroSchema.Field.NULL_DEFAULT_VALUE else null)
        }
        return AvroSchema.createRecord(recordName, null, "com.deltaconnect", false, fields)
    }

    /** Convert an Avro record Schema to a Delta StructType. */
    fun toDeltaType(avroSchema: AvroSchema): DeltaType.StructType {
        require(avroSchema.type == AvroSchema.Type.RECORD) { "Expected RECORD schema, got ${avroSchema.type}" }
        val fields = avroSchema.fields.map { field ->
            val (deltaType, nullable) = avroFieldToDelta(field.schema())
            StructField(
                name = field.name(),
                type = deltaType,
                nullable = nullable
            )
        }
        return DeltaType.StructType(fields)
    }

    private fun deltaTypeToAvro(type: DeltaType): AvroSchema {
        return when (type) {
            is DeltaType.StringType -> AvroSchema.create(AvroSchema.Type.STRING)
            is DeltaType.LongType -> AvroSchema.create(AvroSchema.Type.LONG)
            is DeltaType.IntegerType -> AvroSchema.create(AvroSchema.Type.INT)
            is DeltaType.ShortType -> AvroSchema.create(AvroSchema.Type.INT)
            is DeltaType.ByteType -> AvroSchema.create(AvroSchema.Type.INT)
            is DeltaType.FloatType -> AvroSchema.create(AvroSchema.Type.FLOAT)
            is DeltaType.DoubleType -> AvroSchema.create(AvroSchema.Type.DOUBLE)
            is DeltaType.BooleanType -> AvroSchema.create(AvroSchema.Type.BOOLEAN)
            is DeltaType.BinaryType -> AvroSchema.create(AvroSchema.Type.BYTES)
            is DeltaType.DateType -> LogicalTypes.date().addToSchema(AvroSchema.create(AvroSchema.Type.INT))
            is DeltaType.TimestampType -> LogicalTypes.timestampMicros().addToSchema(AvroSchema.create(AvroSchema.Type.LONG))
            is DeltaType.DecimalType -> {
                val bytesSchema = AvroSchema.create(AvroSchema.Type.BYTES)
                LogicalTypes.decimal(type.precision, type.scale).addToSchema(bytesSchema)
            }
            is DeltaType.ArrayType -> {
                val elementSchema = deltaTypeToAvro(type.elementType)
                val itemSchema = if (type.containsNull) {
                    AvroSchema.createUnion(AvroSchema.create(AvroSchema.Type.NULL), elementSchema)
                } else {
                    elementSchema
                }
                AvroSchema.createArray(itemSchema)
            }
            is DeltaType.MapType -> {
                // Avro maps always have string keys
                val valueSchema = deltaTypeToAvro(type.valueType)
                val valueItemSchema = if (type.valueContainsNull) {
                    AvroSchema.createUnion(AvroSchema.create(AvroSchema.Type.NULL), valueSchema)
                } else {
                    valueSchema
                }
                AvroSchema.createMap(valueItemSchema)
            }
            is DeltaType.StructType -> toAvroSchema(type)
        }
    }

    /**
     * Returns the Delta type and nullability from an Avro field schema.
     * Handles union types: `["null", T]` means nullable T.
     */
    private fun avroFieldToDelta(schema: AvroSchema): Pair<DeltaType, Boolean> {
        if (schema.type == AvroSchema.Type.UNION) {
            val nonNullTypes = schema.types.filter { it.type != AvroSchema.Type.NULL }
            val nullable = schema.types.any { it.type == AvroSchema.Type.NULL }
            require(nonNullTypes.size == 1) { "Unsupported union type: $schema" }
            return avroToDeltaType(nonNullTypes[0]) to nullable
        }
        return avroToDeltaType(schema) to false
    }

    private fun avroToDeltaType(schema: AvroSchema): DeltaType {
        // Check logical types first
        val logicalType = schema.logicalType
        if (logicalType != null) {
            return when (logicalType.name) {
                "date" -> DeltaType.DateType
                "timestamp-micros" -> DeltaType.TimestampType
                "timestamp-millis" -> DeltaType.TimestampType
                "decimal" -> {
                    val decimal = logicalType as LogicalTypes.Decimal
                    DeltaType.DecimalType(decimal.precision, decimal.scale)
                }
                else -> throw IllegalArgumentException("Unsupported Avro logical type: ${logicalType.name}")
            }
        }

        return when (schema.type) {
            AvroSchema.Type.STRING -> DeltaType.StringType
            AvroSchema.Type.LONG -> DeltaType.LongType
            AvroSchema.Type.INT -> DeltaType.IntegerType
            AvroSchema.Type.FLOAT -> DeltaType.FloatType
            AvroSchema.Type.DOUBLE -> DeltaType.DoubleType
            AvroSchema.Type.BOOLEAN -> DeltaType.BooleanType
            AvroSchema.Type.BYTES -> DeltaType.BinaryType
            AvroSchema.Type.RECORD -> toDeltaType(schema)
            AvroSchema.Type.ARRAY -> {
                val (elementType, containsNull) = avroFieldToDelta(schema.elementType)
                DeltaType.ArrayType(elementType, containsNull)
            }
            AvroSchema.Type.MAP -> {
                val (valueType, valueContainsNull) = avroFieldToDelta(schema.valueType)
                DeltaType.MapType(DeltaType.StringType, valueType, valueContainsNull)
            }
            else -> throw IllegalArgumentException("Unsupported Avro type: ${schema.type}")
        }
    }
}

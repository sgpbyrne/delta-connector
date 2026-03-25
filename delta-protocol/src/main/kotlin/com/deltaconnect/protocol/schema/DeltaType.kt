package com.deltaconnect.protocol.schema

/**
 * Sealed hierarchy representing all Delta Lake data types.
 *
 * Maps to the "type" field in Delta's JSON schema format.
 * See: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#schema-serialization-format
 */
sealed interface DeltaType {
    data object StringType : DeltaType

    data object LongType : DeltaType

    data object IntegerType : DeltaType

    data object ShortType : DeltaType

    data object ByteType : DeltaType

    data object FloatType : DeltaType

    data object DoubleType : DeltaType

    data object BooleanType : DeltaType

    data object BinaryType : DeltaType

    data object DateType : DeltaType

    data object TimestampType : DeltaType

    data class DecimalType(
        val precision: Int,
        val scale: Int,
    ) : DeltaType

    data class ArrayType(
        val elementType: DeltaType,
        val containsNull: Boolean,
    ) : DeltaType

    data class MapType(
        val keyType: DeltaType,
        val valueType: DeltaType,
        val valueContainsNull: Boolean,
    ) : DeltaType

    data class StructType(
        val fields: List<StructField>,
    ) : DeltaType
}

data class StructField(
    val name: String,
    val type: DeltaType,
    val nullable: Boolean = true,
    val metadata: Map<String, Any> = emptyMap(),
)

/**
 * The canonical Delta Lake type name string (e.g. "string", "integer", "decimal(10,2)").
 *
 * Matches the names used in Delta's JSON schema serialization format.
 */
val DeltaType.typeName: String
    get() =
        when (this) {
            is DeltaType.StringType -> "string"
            is DeltaType.LongType -> "long"
            is DeltaType.IntegerType -> "integer"
            is DeltaType.ShortType -> "short"
            is DeltaType.ByteType -> "byte"
            is DeltaType.FloatType -> "float"
            is DeltaType.DoubleType -> "double"
            is DeltaType.BooleanType -> "boolean"
            is DeltaType.BinaryType -> "binary"
            is DeltaType.DateType -> "date"
            is DeltaType.TimestampType -> "timestamp"
            is DeltaType.DecimalType -> "decimal($precision,$scale)"
            is DeltaType.ArrayType -> "array"
            is DeltaType.MapType -> "map"
            is DeltaType.StructType -> "struct"
        }

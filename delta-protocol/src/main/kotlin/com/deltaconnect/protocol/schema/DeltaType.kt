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
    data class DecimalType(val precision: Int, val scale: Int) : DeltaType
    data class ArrayType(val elementType: DeltaType, val containsNull: Boolean) : DeltaType
    data class MapType(
        val keyType: DeltaType,
        val valueType: DeltaType,
        val valueContainsNull: Boolean
    ) : DeltaType
    data class StructType(val fields: List<StructField>) : DeltaType
}

data class StructField(
    val name: String,
    val type: DeltaType,
    val nullable: Boolean = true,
    val metadata: Map<String, Any> = emptyMap()
)

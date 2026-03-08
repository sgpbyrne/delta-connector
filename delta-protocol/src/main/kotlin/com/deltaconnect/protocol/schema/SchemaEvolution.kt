package com.deltaconnect.protocol.schema

/**
 * Handles Delta table schema evolution: adding nullable columns and widening types.
 */
object SchemaEvolution {

    /** Result of merging a new schema into an existing one. */
    sealed interface MergeResult {
        data class Evolved(val schema: DeltaType.StructType) : MergeResult
        data class NoChange(val schema: DeltaType.StructType) : MergeResult
        data class Incompatible(val reason: String) : MergeResult
    }

    /**
     * Merge [incoming] schema into [existing] schema.
     *
     * Rules:
     * - New nullable columns in [incoming] are added to the result.
     * - Type widening (e.g., INT → LONG) is allowed.
     * - Incompatible type changes (e.g., STRING → INT) are rejected.
     * - Columns in [existing] but not in [incoming] are preserved.
     */
    fun merge(existing: DeltaType.StructType, incoming: DeltaType.StructType): MergeResult {
        val existingByName = existing.fields.associateBy { it.name }
        val incomingByName = incoming.fields.associateBy { it.name }
        val mergedFields = mutableListOf<StructField>()
        var changed = false

        // Process existing fields, checking for type changes
        for (field in existing.fields) {
            val incomingField = incomingByName[field.name]
            if (incomingField == null) {
                mergedFields.add(field)
                continue
            }

            val widened = widenType(field.type, incomingField.type)
                ?: return MergeResult.Incompatible(
                    "Incompatible type change for column '${field.name}': " +
                        "${typeName(field.type)} → ${typeName(incomingField.type)}"
                )

            val mergedNullable = field.nullable || incomingField.nullable
            val mergedField = field.copy(type = widened, nullable = mergedNullable)
            if (mergedField != field) changed = true
            mergedFields.add(mergedField)
        }

        // Add new columns from incoming (must be nullable)
        for (field in incoming.fields) {
            if (field.name !in existingByName) {
                if (!field.nullable) {
                    return MergeResult.Incompatible(
                        "New column '${field.name}' must be nullable for schema evolution"
                    )
                }
                mergedFields.add(field)
                changed = true
            }
        }

        val result = DeltaType.StructType(mergedFields)
        return if (changed) MergeResult.Evolved(result) else MergeResult.NoChange(result)
    }

    /**
     * Try to widen [from] type to [to] type. Returns the widened type,
     * or null if the types are incompatible.
     */
    private fun widenType(from: DeltaType, to: DeltaType): DeltaType? {
        if (from == to) return from

        return when {
            // Integer widening: Byte → Short → Integer → Long
            from is DeltaType.ByteType && to in INT_WIDENABLE -> to
            from is DeltaType.ShortType && to in SHORT_WIDENABLE -> to
            from is DeltaType.IntegerType && to is DeltaType.LongType -> to
            // Float → Double
            from is DeltaType.FloatType && to is DeltaType.DoubleType -> to
            // Decimal precision/scale widening
            from is DeltaType.DecimalType && to is DeltaType.DecimalType -> {
                if (to.precision >= from.precision && to.scale >= from.scale) to else null
            }
            else -> null
        }
    }

    private val INT_WIDENABLE = setOf(
        DeltaType.ShortType, DeltaType.IntegerType, DeltaType.LongType
    )
    private val SHORT_WIDENABLE = setOf(
        DeltaType.IntegerType, DeltaType.LongType
    )

    private fun typeName(type: DeltaType): String = when (type) {
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
        is DeltaType.DecimalType -> "decimal(${type.precision},${type.scale})"
        is DeltaType.ArrayType -> "array"
        is DeltaType.MapType -> "map"
        is DeltaType.StructType -> "struct"
    }
}

package com.deltaconnect.protocol.actions

/**
 * Defines the table schema, format, partition columns, and configuration.
 *
 * Only the latest MetaData action in the log is active. Each new MetaData
 * replaces the previous one entirely.
 *
 * @property id Unique table identifier (UUID string).
 * @property name Optional human-readable table name.
 * @property description Optional table description.
 * @property format Data file format (always Parquet for this connector).
 * @property schemaString Delta schema as a JSON string.
 * @property partitionColumns Columns used for Hive-style partitioning.
 * @property configuration Table properties (e.g., delta.appendOnly).
 * @property createdTime Epoch millis when the table was created.
 */
data class MetaData(
    val id: String,
    val name: String? = null,
    val description: String? = null,
    val format: Format = Format(),
    val schemaString: String,
    val partitionColumns: List<String> = emptyList(),
    val configuration: Map<String, String> = emptyMap(),
    val createdTime: Long? = null
) : Action

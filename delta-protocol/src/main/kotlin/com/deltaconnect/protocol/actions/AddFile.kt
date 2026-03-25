package com.deltaconnect.protocol.actions

/**
 * Records a data file as part of the table at a given version.
 *
 * @property path Relative path to the data file from the table root.
 * @property partitionValues Map of partition column name to value (empty string for null).
 * @property size File size in bytes.
 * @property modificationTime Last modification time in epoch millis.
 * @property dataChange Whether this file was added as part of a data-changing operation.
 * @property stats Optional JSON string with file-level column statistics
 *   (numRecords, minValues, maxValues, nullCount).
 * @property tags Optional metadata tags.
 */
data class AddFile(
    val path: String,
    val partitionValues: Map<String, String> = emptyMap(),
    val size: Long,
    val modificationTime: Long,
    val dataChange: Boolean,
    val stats: String? = null,
    val tags: Map<String, String>? = null,
) : Action

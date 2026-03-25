package com.deltaconnect.protocol.actions

/**
 * Marks a data file as logically removed from the table.
 *
 * A RemoveFile cancels the AddFile with the same path. The physical file
 * remains on storage until cleaned up by VACUUM.
 *
 * @property path Relative path to the data file from the table root.
 * @property deletionTimestamp Epoch millis when the file was logically removed.
 * @property dataChange Whether this removal is part of a data-changing operation.
 * @property extendedFileMetadata Whether extended metadata fields are populated.
 * @property partitionValues Partition values of the removed file (if extended metadata).
 * @property size File size in bytes (if extended metadata).
 */
data class RemoveFile(
    val path: String,
    val deletionTimestamp: Long? = null,
    val dataChange: Boolean,
    val extendedFileMetadata: Boolean? = null,
    val partitionValues: Map<String, String>? = null,
    val size: Long? = null,
) : Action

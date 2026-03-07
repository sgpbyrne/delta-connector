package com.deltaconnect.protocol.actions

/**
 * Records metadata about a commit operation.
 *
 * CommitInfo is informational — it does not affect table state reconstruction.
 * Only the CommitInfo from the snapshot version is retained in snapshots.
 *
 * @property timestamp Epoch millis when the commit was made.
 * @property operation Operation name (e.g., "WRITE", "MERGE", "CREATE TABLE").
 * @property operationParameters Additional operation context.
 * @property engineInfo Identifier for the engine that made the commit.
 * @property isBlindAppend True if the commit only adds files without reading existing data.
 */
data class CommitInfo(
    val timestamp: Long,
    val operation: String,
    val operationParameters: Map<String, String> = emptyMap(),
    val engineInfo: String? = null,
    val isBlindAppend: Boolean? = null
) : Action

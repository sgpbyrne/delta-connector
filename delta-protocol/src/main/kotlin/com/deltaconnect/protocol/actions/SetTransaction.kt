package com.deltaconnect.protocol.actions

/**
 * Records an application-specific transaction identifier for exactly-once semantics.
 *
 * The latest SetTransaction per [appId] in the log indicates the last
 * successfully committed application version. Applications use this to
 * resume processing after a restart without reprocessing.
 *
 * @property appId Application identifier (e.g., "my-app-partition-0").
 * @property version Application-defined version (e.g., stream offset).
 * @property lastUpdated Epoch millis of the last update.
 */
data class SetTransaction(
    val appId: String,
    val version: Long,
    val lastUpdated: Long? = null,
) : Action

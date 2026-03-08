package com.deltaconnect.protocol.actions

/**
 * Records an application specific transaction identifier for exactly-once semantics.
 *
 * Used by this connector to store Kafka offsets atomically in the Delta commit.
 * On restart, the latest SetTransaction for a given appId indicates the last
 * successfully committed offset.
 *
 * @property appId Application identifier (e.g., "delta-cdc-sink-{connector}-{topic}-{partition}").
 * @property version Application-defined version (e.g., Kafka offset).
 * @property lastUpdated Epoch millis of the last update.
 */
data class SetTransaction(
    val appId: String,
    val version: Long,
    val lastUpdated: Long? = null
) : Action

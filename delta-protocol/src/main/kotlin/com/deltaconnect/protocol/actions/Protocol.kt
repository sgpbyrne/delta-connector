package com.deltaconnect.protocol.actions

/**
 * Sets the minimum reader and writer protocol versions required for the table.
 *
 * This connector targets minWriterVersion=2 (required for MERGE / column invariants).
 * Versions 3+ (readerFeatures/writerFeatures) are not used but fields are preserved
 * for forward compatibility when reading existing tables.
 */
data class Protocol(
    val minReaderVersion: Int = 1,
    val minWriterVersion: Int = 2,
    val readerFeatures: Set<String>? = null,
    val writerFeatures: Set<String>? = null
) : Action

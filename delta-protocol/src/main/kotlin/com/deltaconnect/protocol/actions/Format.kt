package com.deltaconnect.protocol.actions

/**
 * Describes the data format of files in a Delta table.
 */
data class Format(
    val provider: String = "parquet",
    val options: Map<String, String> = emptyMap(),
)

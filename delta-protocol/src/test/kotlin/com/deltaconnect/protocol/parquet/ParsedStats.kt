package com.deltaconnect.protocol.parquet

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue

data class ParsedStats(
    val numRecords: Long,
    val minValues: Map<String, Any>,
    val maxValues: Map<String, Any>,
    val nullCount: Map<String, Long>,
) {
    companion object {
        private val objectMapper = jacksonObjectMapper()

        fun fromJson(json: String): ParsedStats {
            val map: Map<String, Any> = objectMapper.readValue(json)
            @Suppress("UNCHECKED_CAST")
            return ParsedStats(
                numRecords = (map["numRecords"] as Number).toLong(),
                minValues = map["minValues"] as Map<String, Any>,
                maxValues = map["maxValues"] as Map<String, Any>,
                nullCount = (map["nullCount"] as Map<String, Any>).mapValues { (_, v) -> (v as Number).toLong() },
            )
        }
    }
}

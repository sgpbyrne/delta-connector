package com.deltaconnect.protocol.log

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue

/** Thrown when the `_last_checkpoint` file cannot be parsed. */
class LastCheckpointParseException(
    message: String,
    cause: Throwable,
) : RuntimeException(message, cause)

/**
 * Represents the content of the `_last_checkpoint` file in `_delta_log/`.
 *
 * This file records the version of the latest checkpoint, enabling the
 * [TransactionLogReader] to skip replaying commits before the checkpoint.
 *
 * @property version The version of the latest checkpoint.
 * @property size Number of actions in the checkpoint (optional).
 * @property parts Number of parts for multi-part checkpoints (optional, null = single file).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class LastCheckpointInfo(
    val version: Long,
    val size: Long? = null,
    val parts: Int? = null,
) {
    /** Serialize this checkpoint info to JSON for writing to `_last_checkpoint`. */
    fun toJson(): String = mapper.writeValueAsString(this)

    companion object {
        private val mapper = jacksonObjectMapper()

        /**
         * Parse the JSON content of a `_last_checkpoint` file.
         *
         * @param json Raw JSON string from `_last_checkpoint`.
         * @return Parsed checkpoint info.
         * @throws LastCheckpointParseException if the JSON is malformed or missing required fields.
         */
        fun fromJson(json: String): LastCheckpointInfo =
            try {
                mapper.readValue<LastCheckpointInfo>(json)
            } catch (e: com.fasterxml.jackson.core.JacksonException) {
                throw LastCheckpointParseException("Failed to parse _last_checkpoint JSON", e)
            }
    }
}

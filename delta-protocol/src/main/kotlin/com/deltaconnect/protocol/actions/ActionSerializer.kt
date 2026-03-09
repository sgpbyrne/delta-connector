package com.deltaconnect.protocol.actions

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue

/**
 * Serializes and deserializes Delta Lake transaction log actions.
 *
 * Each action is serialized as a single JSON line with a type-discriminator key:
 * ```
 * {"add": {"path": "...", "size": 1024, ...}}
 * {"remove": {"path": "...", ...}}
 * {"metaData": {"id": "...", ...}}
 * ```
 *
 * Multiple actions form a commit file.
 */
object ActionSerializer {

    private val mapper: ObjectMapper = jacksonObjectMapper()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    /** Serialize a single action to a JSON line (no trailing newline). */
    fun serialize(action: Action): String {
        val (key, value) = when (action) {
            is AddFile -> "add" to mapper.valueToTree<JsonNode>(action)
            is RemoveFile -> "remove" to mapper.valueToTree<JsonNode>(action)
            is MetaData -> "metaData" to mapper.valueToTree<JsonNode>(action)
            is Protocol -> "protocol" to mapper.valueToTree<JsonNode>(action)
            is SetTransaction -> "txn" to mapper.valueToTree<JsonNode>(action)
            is CommitInfo -> "commitInfo" to mapper.valueToTree<JsonNode>(action)
        }
        return mapper.writeValueAsString(mapOf(key to value))
    }

    /** Serialize a list of actions to a commit file (newline-delimited JSON). */
    fun serializeActions(actions: List<Action>): String {
        return actions.joinToString("\n") { serialize(it) }
    }

    /** Deserialize a single JSON line to an action. */
    fun deserialize(json: String): Action {
        val tree = mapper.readTree(json)
        return when {
            tree.has("add") -> mapper.readValue<AddFile>(tree["add"].traverse(mapper))
            tree.has("remove") -> mapper.readValue<RemoveFile>(tree["remove"].traverse(mapper))
            tree.has("metaData") -> mapper.readValue<MetaData>(tree["metaData"].traverse(mapper))
            tree.has("protocol") -> mapper.readValue<Protocol>(tree["protocol"].traverse(mapper))
            tree.has("txn") -> mapper.readValue<SetTransaction>(tree["txn"].traverse(mapper))
            tree.has("commitInfo") -> mapper.readValue<CommitInfo>(tree["commitInfo"].traverse(mapper))
            else -> throw IllegalArgumentException("Unknown action type in: $json")
        }
    }

    /** Deserialize a commit file (newline-delimited JSON) to a list of actions. */
    fun deserializeActions(content: String): List<Action> {
        return content.lines()
            .filter { it.isNotBlank() }
            .map { deserialize(it) }
    }

    /** Format a commit version as a zero-padded 20-digit filename. */
    fun commitFileName(version: Long): String = "%020d.json".format(version)

    /** Format a checkpoint version as a zero-padded 20-digit Parquet filename. */
    fun checkpointFileName(version: Long): String = "%020d.checkpoint.parquet".format(version)
}

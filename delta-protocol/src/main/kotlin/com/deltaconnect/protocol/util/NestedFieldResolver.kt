package com.deltaconnect.protocol.util

import com.fasterxml.jackson.databind.JsonNode
import org.apache.avro.generic.GenericRecord

/**
 * Resolves dot-notation field paths in nested structures.
 *
 * Used by stats collection and file pruning to traverse nested
 * Avro records and JSON nodes using paths like `"address.city"`.
 */
object NestedFieldResolver {
    /**
     * Resolve a dot-notation path in a nested [GenericRecord].
     *
     * @param record The root Avro record.
     * @param path Dot-separated field path (e.g. `"address.city"`).
     * @return The resolved value, or null if any segment is missing.
     */
    fun resolveGenericRecord(
        record: GenericRecord,
        path: String,
    ): Any? {
        val parts = path.split('.')
        var current: Any? = record
        for (part in parts) {
            if (current == null) return null
            current = (current as? GenericRecord)?.get(part) ?: return null
        }
        return current
    }

    /**
     * Resolve a dot-notation path in a nested [JsonNode].
     *
     * @param node The root JSON node.
     * @param path Dot-separated field path (e.g. `"address.city"`).
     * @return The resolved node, or null if any segment is missing.
     */
    fun resolveJsonNode(
        node: JsonNode,
        path: String,
    ): JsonNode? {
        val parts = path.split('.')
        var current: JsonNode? = node
        for (part in parts) {
            current = current?.get(part) ?: return null
        }
        return current
    }
}

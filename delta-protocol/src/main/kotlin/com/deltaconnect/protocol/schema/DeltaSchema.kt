package com.deltaconnect.protocol.schema

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

/**
 * Parses and serializes Delta Lake schema JSON strings.
 *
 * Delta schemas use a JSON format where struct types have `"type": "struct"`
 * and a `"fields"` array. Primitive types are represented as simple strings
 * like `"string"`, `"integer"`, `"long"`, etc.
 *
 * Example:
 * ```json
 * {"type":"struct","fields":[
 *   {"name":"id","type":"integer","nullable":false,"metadata":{}},
 *   {"name":"name","type":"string","nullable":true,"metadata":{}}
 * ]}
 * ```
 */
object DeltaSchema {

    private val mapper: ObjectMapper = jacksonObjectMapper()

    /** Parse a Delta JSON schema string into a [DeltaType.StructType]. */
    fun fromJson(schemaString: String): DeltaType.StructType {
        val node = mapper.readTree(schemaString)
        val parsed = parseType(node)
        require(parsed is DeltaType.StructType) { "Top-level schema must be a struct type" }
        return parsed
    }

    /** Serialize a [DeltaType.StructType] to a Delta JSON schema string. */
    fun toJson(structType: DeltaType.StructType): String {
        return mapper.writeValueAsString(typeToNode(structType))
    }

    private fun parseType(node: JsonNode): DeltaType {
        if (node.isTextual) {
            return parsePrimitiveType(node.asText())
        }
        if (node.isObject) {
            return when (node["type"].asText()) {
                "struct" -> parseStructType(node)
                "array" -> parseArrayType(node)
                "map" -> parseMapType(node)
                else -> parsePrimitiveType(node["type"].asText())
            }
        }
        throw IllegalArgumentException("Unexpected schema node: $node")
    }

    private fun parsePrimitiveType(typeName: String): DeltaType {
        val decimalMatch = DECIMAL_PATTERN.matchEntire(typeName)
        if (decimalMatch != null) {
            val (precision, scale) = decimalMatch.destructured
            return DeltaType.DecimalType(precision.toInt(), scale.toInt())
        }
        return when (typeName) {
            "string" -> DeltaType.StringType
            "long" -> DeltaType.LongType
            "integer" -> DeltaType.IntegerType
            "short" -> DeltaType.ShortType
            "byte" -> DeltaType.ByteType
            "float" -> DeltaType.FloatType
            "double" -> DeltaType.DoubleType
            "boolean" -> DeltaType.BooleanType
            "binary" -> DeltaType.BinaryType
            "date" -> DeltaType.DateType
            "timestamp" -> DeltaType.TimestampType
            else -> throw IllegalArgumentException("Unknown Delta type: $typeName")
        }
    }

    private fun parseStructType(node: JsonNode): DeltaType.StructType {
        val fields = node["fields"].map { fieldNode ->
            StructField(
                name = fieldNode["name"].asText(),
                type = parseType(fieldNode["type"]),
                nullable = fieldNode["nullable"].asBoolean(true),
                metadata = parseMetadata(fieldNode["metadata"])
            )
        }
        return DeltaType.StructType(fields)
    }

    private fun parseArrayType(node: JsonNode): DeltaType.ArrayType {
        return DeltaType.ArrayType(
            elementType = parseType(node["elementType"]),
            containsNull = node["containsNull"].asBoolean(true)
        )
    }

    private fun parseMapType(node: JsonNode): DeltaType.MapType {
        return DeltaType.MapType(
            keyType = parseType(node["keyType"]),
            valueType = parseType(node["valueType"]),
            valueContainsNull = node["valueContainsNull"].asBoolean(true)
        )
    }

    @Suppress("UNCHECKED_CAST")
    private fun parseMetadata(node: JsonNode?): Map<String, Any> {
        if (node == null || node.isNull || node.isEmpty) return emptyMap()
        return mapper.convertValue(node, Map::class.java) as Map<String, Any>
    }

    private fun typeToNode(type: DeltaType): JsonNode {
        return when (type) {
            is DeltaType.StructType -> structTypeToNode(type)
            is DeltaType.ArrayType -> arrayTypeToNode(type)
            is DeltaType.MapType -> mapTypeToNode(type)
            else -> mapper.nodeFactory.textNode(type.typeName)
        }
    }

    private fun structTypeToNode(type: DeltaType.StructType): ObjectNode {
        val node = mapper.createObjectNode()
        node.put("type", "struct")
        val fieldsArray: ArrayNode = node.putArray("fields")
        for (field in type.fields) {
            val fieldNode = mapper.createObjectNode()
            fieldNode.put("name", field.name)
            fieldNode.set<JsonNode>("type", typeToNode(field.type))
            fieldNode.put("nullable", field.nullable)
            fieldNode.set<JsonNode>("metadata", mapper.valueToTree(field.metadata))
            fieldsArray.add(fieldNode)
        }
        return node
    }

    private fun arrayTypeToNode(type: DeltaType.ArrayType): ObjectNode {
        val node = mapper.createObjectNode()
        node.put("type", "array")
        node.set<JsonNode>("elementType", typeToNode(type.elementType))
        node.put("containsNull", type.containsNull)
        return node
    }

    private fun mapTypeToNode(type: DeltaType.MapType): ObjectNode {
        val node = mapper.createObjectNode()
        node.put("type", "map")
        node.set<JsonNode>("keyType", typeToNode(type.keyType))
        node.set<JsonNode>("valueType", typeToNode(type.valueType))
        node.put("valueContainsNull", type.valueContainsNull)
        return node
    }

    private val DECIMAL_PATTERN = Regex("""decimal\((\d+),\s*(\d+)\)""")
}

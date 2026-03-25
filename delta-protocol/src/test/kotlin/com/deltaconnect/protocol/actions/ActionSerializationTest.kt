package com.deltaconnect.protocol.actions

import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldNotContain
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class ActionSerializationTest {
    @Nested
    inner class RoundTrip {
        @Test
        fun `AddFile round-trips through serialization`() {
            val action =
                AddFile(
                    path = "part-00000-abc.parquet",
                    partitionValues = mapOf("date" to "2024-01-15"),
                    size = 1024L,
                    modificationTime = 1705300000000L,
                    dataChange = true,
                    stats = """{"numRecords":100,"minValues":{"id":1},"maxValues":{"id":100},"nullCount":{"id":0}}""",
                )
            val json = ActionSerializer.serialize(action)
            val deserialized = ActionSerializer.deserialize(json)
            deserialized shouldBe action
        }

        @Test
        fun `AddFile with minimal fields round-trips`() {
            val action =
                AddFile(
                    path = "data.parquet",
                    size = 0L,
                    modificationTime = 0L,
                    dataChange = false,
                )
            val json = ActionSerializer.serialize(action)
            val deserialized = ActionSerializer.deserialize(json)
            deserialized shouldBe action
        }

        @Test
        fun `RemoveFile round-trips through serialization`() {
            val action =
                RemoveFile(
                    path = "part-00000-abc.parquet",
                    deletionTimestamp = 1705300000000L,
                    dataChange = true,
                    extendedFileMetadata = true,
                    partitionValues = mapOf("date" to "2024-01-15"),
                    size = 1024L,
                )
            val json = ActionSerializer.serialize(action)
            val deserialized = ActionSerializer.deserialize(json)
            deserialized shouldBe action
        }

        @Test
        fun `RemoveFile with minimal fields round-trips`() {
            val action =
                RemoveFile(
                    path = "data.parquet",
                    dataChange = true,
                )
            val json = ActionSerializer.serialize(action)
            val deserialized = ActionSerializer.deserialize(json)
            deserialized shouldBe action
        }

        @Test
        fun `Protocol round-trips through serialization`() {
            val action = Protocol(minReaderVersion = 1, minWriterVersion = 2)
            val json = ActionSerializer.serialize(action)
            val deserialized = ActionSerializer.deserialize(json)
            deserialized shouldBe action
        }

        @Test
        fun `Protocol with features round-trips`() {
            val action =
                Protocol(
                    minReaderVersion = 3,
                    minWriterVersion = 7,
                    readerFeatures = setOf("columnMapping"),
                    writerFeatures = setOf("columnMapping", "deletionVectors"),
                )
            val json = ActionSerializer.serialize(action)
            val deserialized = ActionSerializer.deserialize(json)
            deserialized shouldBe action
        }

        @Test
        fun `MetaData round-trips through serialization`() {
            val action =
                MetaData(
                    id = "af23c9d7-fff1-4a5a-a2c8-55c59bd16b8f",
                    name = "test_table",
                    description = "A test table",
                    format = Format("parquet", mapOf("compression" to "snappy")),
                    schemaString = """{"type":"struct","fields":[{"name":"id","type":"integer","nullable":false}]}""",
                    partitionColumns = listOf("date"),
                    configuration = mapOf("delta.appendOnly" to "false"),
                    createdTime = 1705300000000L,
                )
            val json = ActionSerializer.serialize(action)
            val deserialized = ActionSerializer.deserialize(json)
            deserialized shouldBe action
        }

        @Test
        fun `MetaData with minimal fields round-trips`() {
            val action =
                MetaData(
                    id = "test-id",
                    schemaString = """{"type":"struct","fields":[]}""",
                )
            val json = ActionSerializer.serialize(action)
            val deserialized = ActionSerializer.deserialize(json)
            deserialized shouldBe action
        }

        @Test
        fun `SetTransaction round-trips through serialization`() {
            val action =
                SetTransaction(
                    appId = "delta-cdc-sink-connector1-topic1-0",
                    version = 42L,
                    lastUpdated = 1705300000000L,
                )
            val json = ActionSerializer.serialize(action)
            val deserialized = ActionSerializer.deserialize(json)
            deserialized shouldBe action
        }

        @Test
        fun `SetTransaction with minimal fields round-trips`() {
            val action = SetTransaction(appId = "app1", version = 0L)
            val json = ActionSerializer.serialize(action)
            val deserialized = ActionSerializer.deserialize(json)
            deserialized shouldBe action
        }

        @Test
        fun `CommitInfo round-trips through serialization`() {
            val action =
                CommitInfo(
                    timestamp = 1705300000000L,
                    operation = "MERGE",
                    operationParameters = mapOf("predicate" to "id = source.id"),
                    engineInfo = "delta-cdc-connector/0.1.0",
                    isBlindAppend = false,
                )
            val json = ActionSerializer.serialize(action)
            val deserialized = ActionSerializer.deserialize(json)
            deserialized shouldBe action
        }

        @Test
        fun `CommitInfo with minimal fields round-trips`() {
            val action = CommitInfo(timestamp = 0L, operation = "WRITE")
            val json = ActionSerializer.serialize(action)
            val deserialized = ActionSerializer.deserialize(json)
            deserialized shouldBe action
        }
    }

    @Nested
    inner class JsonFormat {
        @Test
        fun `AddFile serializes with 'add' wrapper key`() {
            val action =
                AddFile(
                    path = "data.parquet",
                    size = 100L,
                    modificationTime = 0L,
                    dataChange = true,
                )
            val json = ActionSerializer.serialize(action)
            json shouldContain """"add":"""
            json shouldNotContain """"AddFile":"""
        }

        @Test
        fun `RemoveFile serializes with 'remove' wrapper key`() {
            val action = RemoveFile(path = "data.parquet", dataChange = true)
            val json = ActionSerializer.serialize(action)
            json shouldContain """"remove":"""
        }

        @Test
        fun `Protocol serializes with 'protocol' wrapper key`() {
            val action = Protocol()
            val json = ActionSerializer.serialize(action)
            json shouldContain """"protocol":"""
        }

        @Test
        fun `MetaData serializes with 'metaData' wrapper key`() {
            val action = MetaData(id = "id", schemaString = "{}")
            val json = ActionSerializer.serialize(action)
            json shouldContain """"metaData":"""
        }

        @Test
        fun `SetTransaction serializes with 'txn' wrapper key`() {
            val action = SetTransaction(appId = "app", version = 1L)
            val json = ActionSerializer.serialize(action)
            json shouldContain """"txn":"""
        }

        @Test
        fun `CommitInfo serializes with 'commitInfo' wrapper key`() {
            val action = CommitInfo(timestamp = 0L, operation = "WRITE")
            val json = ActionSerializer.serialize(action)
            json shouldContain """"commitInfo":"""
        }

        @Test
        fun `null fields are excluded from JSON`() {
            val action =
                AddFile(
                    path = "data.parquet",
                    size = 100L,
                    modificationTime = 0L,
                    dataChange = true,
                )
            val json = ActionSerializer.serialize(action)
            json shouldNotContain "stats"
            json shouldNotContain "tags"
        }

        @Test
        fun `stats field is serialized as raw string`() {
            val statsJson = """{"numRecords":50}"""
            val action =
                AddFile(
                    path = "data.parquet",
                    size = 100L,
                    modificationTime = 0L,
                    dataChange = true,
                    stats = statsJson,
                )
            val json = ActionSerializer.serialize(action)
            // stats should be a string value in JSON, not a parsed object
            json shouldContain """"stats":"""
        }
    }

    @Nested
    inner class MultiAction {
        @Test
        fun `serializeActions produces newline-delimited JSON`() {
            val actions =
                listOf(
                    Protocol(minReaderVersion = 1, minWriterVersion = 2),
                    MetaData(id = "table-id", schemaString = "{}"),
                    CommitInfo(timestamp = 1000L, operation = "CREATE TABLE", isBlindAppend = true),
                )
            val content = ActionSerializer.serializeActions(actions)
            val lines = content.lines()
            lines.size shouldBe 3
            lines[0] shouldContain "protocol"
            lines[1] shouldContain "metaData"
            lines[2] shouldContain "commitInfo"
        }

        @Test
        fun `deserializeActions parses newline-delimited JSON`() {
            val actions =
                listOf(
                    Protocol(minReaderVersion = 1, minWriterVersion = 2),
                    AddFile(path = "f1.parquet", size = 100L, modificationTime = 0L, dataChange = true),
                    RemoveFile(path = "f0.parquet", deletionTimestamp = 1000L, dataChange = true),
                    SetTransaction(appId = "app1", version = 5L),
                    CommitInfo(timestamp = 1000L, operation = "MERGE"),
                )
            val content = ActionSerializer.serializeActions(actions)
            val deserialized = ActionSerializer.deserializeActions(content)
            deserialized shouldBe actions
        }

        @Test
        fun `deserializeActions ignores blank lines`() {
            val json =
                """
                {"protocol":{"minReaderVersion":1,"minWriterVersion":2}}

                {"commitInfo":{"timestamp":0,"operation":"WRITE"}}
                """.trimIndent()
            val actions = ActionSerializer.deserializeActions(json)
            actions.size shouldBe 2
        }
    }

    @Nested
    inner class ForwardCompatibility {
        @Test
        fun `unknown fields in JSON are ignored during deserialization`() {
            val json =
                """{"add":{"path":"data.parquet","size":100,""" +
                    """"modificationTime":0,"dataChange":true,""" +
                    """"unknownField":"value","anotherUnknown":42}}"""
            val action = ActionSerializer.deserialize(json) as AddFile
            action.path shouldBe "data.parquet"
            action.size shouldBe 100L
        }

        @Test
        fun `unknown action type throws IllegalArgumentException`() {
            val json = """{"unknownAction":{"field":"value"}}"""
            org.junit.jupiter.api.assertThrows<IllegalArgumentException> {
                ActionSerializer.deserialize(json)
            }
        }
    }

    @Nested
    inner class EdgeCases {
        @Test
        fun `version 0 commit file name`() {
            ActionSerializer.commitFileName(0) shouldBe "00000000000000000000.json"
        }

        @Test
        fun `large version commit file name`() {
            ActionSerializer.commitFileName(99999999999999999L) shouldBe "00099999999999999999.json"
        }

        @Test
        fun `SetTransaction with version Long MAX_VALUE`() {
            val action = SetTransaction(appId = "app", version = Long.MAX_VALUE)
            val json = ActionSerializer.serialize(action)
            val deserialized = ActionSerializer.deserialize(json)
            deserialized shouldBe action
        }

        @Test
        fun `AddFile with empty partition values map`() {
            val action =
                AddFile(
                    path = "data.parquet",
                    partitionValues = emptyMap(),
                    size = 0L,
                    modificationTime = 0L,
                    dataChange = false,
                )
            val json = ActionSerializer.serialize(action)
            val deserialized = ActionSerializer.deserialize(json) as AddFile
            deserialized.partitionValues shouldBe emptyMap()
        }

        @Test
        fun `CommitInfo with empty operation parameters`() {
            val action =
                CommitInfo(
                    timestamp = 0L,
                    operation = "WRITE",
                    operationParameters = emptyMap(),
                )
            val json = ActionSerializer.serialize(action)
            val deserialized = ActionSerializer.deserialize(json) as CommitInfo
            deserialized.operationParameters shouldBe emptyMap()
        }
    }
}

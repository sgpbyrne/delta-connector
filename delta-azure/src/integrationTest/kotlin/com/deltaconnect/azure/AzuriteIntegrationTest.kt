package com.deltaconnect.azure

import com.deltaconnect.protocol.DeltaTable
import com.deltaconnect.protocol.merge.DeltaMergeEngine
import com.deltaconnect.protocol.merge.MergeOperation
import com.deltaconnect.protocol.merge.SourceRecord
import com.deltaconnect.protocol.parquet.ParquetFileReader
import com.deltaconnect.protocol.parquet.ParquetFileWriter
import com.deltaconnect.protocol.schema.AvroToDeltaConverter
import com.deltaconnect.protocol.schema.DeltaType
import com.deltaconnect.protocol.schema.StructField
import com.deltaconnect.protocol.storage.CommitConflictException
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import org.apache.avro.generic.GenericData
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName

/**
 * Integration tests for [AzureBlobLogStore] against Azurite (Azure Storage emulator).
 *
 * These tests verify real HTTP calls to the Azure Blob Storage SDK,
 * including atomic commit semantics, file I/O, and
 * Parquet read/write through the Azure backend.
 */
@Testcontainers
class AzuriteIntegrationTest {
    companion object {
        private const val ACCOUNT_NAME = "devstoreaccount1"
        private const val ACCOUNT_KEY =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
        private const val CONTAINER_NAME = "testfs"

        @Container
        @JvmStatic
        val azurite: GenericContainer<*> =
            GenericContainer(
                DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite:3.34.0"),
            ).withExposedPorts(10000) // Blob service
                .withCommand("azurite-blob", "--blobHost", "0.0.0.0", "--blobPort", "10000")

        private lateinit var logStore: AzureBlobLogStore

        @BeforeAll
        @JvmStatic
        fun setUpContainer() {
            val blobEndpoint = "http://127.0.0.1:${azurite.getMappedPort(10000)}/$ACCOUNT_NAME"

            val config =
                AzureBlobConfig(
                    authType = AzureAuthType.STORAGE_KEY,
                    accountName = ACCOUNT_NAME,
                    containerName = CONTAINER_NAME,
                    accountKey = ACCOUNT_KEY,
                    endpoint = blobEndpoint,
                )

            val client = AzureBlobConfigurer.createContainerClient(config)
            try {
                client.create()
            } catch (_: Exception) {
                // Already exists
            }
            logStore = AzureBlobLogStore(client)
        }
    }

    private var testTable: String = ""

    @BeforeEach
    fun setUpTest() {
        testTable = "test_table_${System.nanoTime()}"
    }

    @Nested
    inner class CommitOperations {
        @Test
        fun `write and read commit roundtrip`() {
            val content = """{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}""".toByteArray()
            logStore.writeCommit(testTable, 0, content)

            val read = logStore.readCommit(testTable, 0)
            read.shouldNotBeNull()
            String(read) shouldBe String(content)
        }

        @Test
        fun `read nonexistent commit returns null`() {
            logStore.readCommit(testTable, 999).shouldBeNull()
        }

        @Test
        fun `atomic commit conflict detection`() {
            val content = """{"protocol":{"minReaderVersion":1}}""".toByteArray()
            logStore.writeCommit(testTable, 0, content)

            assertThrows<CommitConflictException> {
                logStore.writeCommit(testTable, 0, content)
            }
        }

        @Test
        fun `list commit versions in order`() {
            logStore.writeCommit(testTable, 0, "v0".toByteArray())
            logStore.writeCommit(testTable, 1, "v1".toByteArray())
            logStore.writeCommit(testTable, 2, "v2".toByteArray())

            val versions = logStore.listCommitVersions(testTable)
            versions shouldBe listOf(0L, 1L, 2L)
        }

        @Test
        fun `list commit versions with start version`() {
            logStore.writeCommit(testTable, 0, "v0".toByteArray())
            logStore.writeCommit(testTable, 1, "v1".toByteArray())
            logStore.writeCommit(testTable, 2, "v2".toByteArray())

            val versions = logStore.listCommitVersions(testTable, startVersion = 2)
            versions shouldBe listOf(2L)
        }
    }

    @Nested
    inner class DataFileOperations {
        @Test
        fun `write and read data file`() {
            val filePath = "$testTable/data/test.bin"
            val data = "hello world".toByteArray()

            logStore.createDataFile(filePath).use { out -> out.write(data) }

            val inputFile = logStore.readDataFileAsInputFile(filePath)
            inputFile.length shouldBe data.size.toLong()
        }

        @Test
        fun `write and read Parquet file through Azure Blob`() {
            val schema =
                DeltaType.StructType(
                    listOf(
                        StructField("id", DeltaType.IntegerType, nullable = false),
                        StructField("name", DeltaType.StringType, nullable = true),
                    ),
                )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)

            val records =
                (1..10).map { i ->
                    val rec = GenericData.Record(avroSchema)
                    rec.put("id", i)
                    rec.put("name", "user_$i")
                    rec
                }

            val filePath = "$testTable/data/part-001.parquet"
            val writer = ParquetFileWriter(logStore, schema)
            val writeResult = writer.write(filePath, records)
            writeResult.recordCount shouldBe 10

            val reader = ParquetFileReader(logStore)
            val readRecords = reader.read(filePath)
            readRecords shouldHaveSize 10
            readRecords[0].get("id") shouldBe 1
            readRecords[0].get("name").toString() shouldBe "user_1"
            readRecords[9].get("id") shouldBe 10
        }
    }

    @Nested
    inner class CheckpointOperations {
        @Test
        fun `write and read last checkpoint`() {
            logStore.writeLastCheckpoint(testTable, """{"version":5}""")

            val checkpoint = logStore.readLastCheckpoint(testTable)
            checkpoint shouldBe """{"version":5}"""
        }

        @Test
        fun `read nonexistent checkpoint returns null`() {
            logStore.readLastCheckpoint(testTable).shouldBeNull()
        }

        @Test
        fun `overwrite checkpoint`() {
            logStore.writeLastCheckpoint(testTable, """{"version":5}""")
            logStore.writeLastCheckpoint(testTable, """{"version":10}""")

            logStore.readLastCheckpoint(testTable) shouldBe """{"version":10}"""
        }
    }

    @Nested
    inner class EndToEnd {
        @Test
        fun `create table, write, merge, read back through Azure Blob`() {
            val schema =
                DeltaType.StructType(
                    listOf(
                        StructField("id", DeltaType.IntegerType, nullable = false),
                        StructField("name", DeltaType.StringType, nullable = true),
                        StructField("value", DeltaType.IntegerType, nullable = true),
                    ),
                )
            val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)

            val table = DeltaTable.createOrReplace(logStore, testTable, schema)
            table.snapshot().version shouldBe 0L

            val mergeEngine = DeltaMergeEngine(logStore, schema, listOf("id"), testTable)
            val insertRecords =
                (1..5).map { i ->
                    val rec = GenericData.Record(avroSchema)
                    rec.put("id", i)
                    rec.put("name", "user_$i")
                    rec.put("value", i * 100)
                    SourceRecord(rec, MergeOperation.INSERT)
                }

            val txn1 = table.startTransaction()
            val mergeResult1 = mergeEngine.merge(table.snapshot(), insertRecords)
            txn1.addActions(mergeResult1.addFiles)
            txn1.commit("MERGE")

            val snapshot1 = table.snapshot()
            snapshot1.version shouldBe 1L
            snapshot1.activeFiles shouldHaveSize 1

            val reader = ParquetFileReader(logStore)
            val rows1 = snapshot1.activeFiles.flatMap { reader.read(it.path) }
            rows1 shouldHaveSize 5

            val updateRecords =
                listOf(
                    SourceRecord(
                        GenericData.Record(avroSchema).apply {
                            put("id", 3)
                            put("name", "updated_3")
                            put("value", 999)
                        },
                        MergeOperation.UPDATE,
                    ),
                )

            val txn2 = table.startTransaction()
            val mergeResult2 = mergeEngine.merge(table.snapshot(), updateRecords)
            txn2.addActions(mergeResult2.removeFiles)
            txn2.addActions(mergeResult2.addFiles)
            txn2.commit("MERGE")

            val snapshot2 = table.snapshot()
            snapshot2.version shouldBe 2L

            val rows2 = snapshot2.activeFiles.flatMap { reader.read(it.path) }
            rows2 shouldHaveSize 5
            val row3 = rows2.find { it.get("id") == 3 }!!
            row3.get("name").toString() shouldBe "updated_3"
            row3.get("value") shouldBe 999
        }
    }
}

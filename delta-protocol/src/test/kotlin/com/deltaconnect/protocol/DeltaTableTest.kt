package com.deltaconnect.protocol

import com.deltaconnect.protocol.actions.AddFile
import com.deltaconnect.protocol.actions.MetaData
import com.deltaconnect.protocol.actions.RemoveFile
import com.deltaconnect.protocol.actions.SetTransaction
import com.deltaconnect.protocol.log.LogTestFixtures
import com.deltaconnect.protocol.schema.DeltaSchema
import com.deltaconnect.protocol.schema.DeltaType
import com.deltaconnect.protocol.schema.StructField
import com.deltaconnect.protocol.storage.LocalFileSystemLogStore
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.longs.shouldBeGreaterThanOrEqual
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.string.shouldContain
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

class DeltaTableTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var logStore: LocalFileSystemLogStore

    private val tablePath = "test-table"

    private val simpleSchema = DeltaType.StructType(
        listOf(
            StructField("id", DeltaType.IntegerType, nullable = false),
            StructField("value", DeltaType.StringType, nullable = true)
        )
    )

    @BeforeEach
    fun setUp() {
        logStore = LocalFileSystemLogStore(tempDir)
    }

    @Nested
    inner class ForPath {

        @Test
        fun `returns table handle for non-existent path`() {
            val table = DeltaTable.forPath(logStore, tablePath)

            table.tablePath shouldBe tablePath
            table.logStore shouldBe logStore
        }

        @Test
        fun `snapshot of non-existent table is empty`() {
            val table = DeltaTable.forPath(logStore, tablePath)
            val snapshot = table.snapshot()

            snapshot shouldBe DeltaSnapshot.empty()
        }

        @Test
        fun `returns table handle for existing table`() {
            DeltaTable.createOrReplace(logStore, tablePath, simpleSchema)

            val table = DeltaTable.forPath(logStore, tablePath)
            val snapshot = table.snapshot()

            snapshot.version shouldBe 0L
            snapshot.metaData.shouldNotBeNull()
        }
    }

    @Nested
    inner class CreateOrReplace {

        @Test
        fun `creates new table at version 0`() {
            val table = DeltaTable.createOrReplace(logStore, tablePath, simpleSchema)
            val snapshot = table.snapshot()

            snapshot.version shouldBe 0L
            snapshot.activeFiles.shouldBeEmpty()
            snapshot.protocol.shouldNotBeNull()
            snapshot.protocol!!.minReaderVersion shouldBe 1
            snapshot.protocol!!.minWriterVersion shouldBe 2
            snapshot.metaData.shouldNotBeNull()
            snapshot.commitInfo.shouldNotBeNull()
            snapshot.commitInfo!!.operation shouldBe "CREATE TABLE"
            snapshot.commitInfo!!.isBlindAppend shouldBe true
            snapshot.commitInfo!!.engineInfo shouldBe null
        }

        @Test
        fun `stores schema in metadata`() {
            val table = DeltaTable.createOrReplace(logStore, tablePath, simpleSchema)
            val snapshot = table.snapshot()

            val storedSchema = DeltaSchema.fromJson(snapshot.metaData!!.schemaString)
            storedSchema.fields shouldHaveSize 2
            storedSchema.fields[0].name shouldBe "id"
            storedSchema.fields[0].type shouldBe DeltaType.IntegerType
            storedSchema.fields[0].nullable shouldBe false
            storedSchema.fields[1].name shouldBe "value"
            storedSchema.fields[1].type shouldBe DeltaType.StringType
        }

        @Test
        fun `stores partition columns`() {
            val table = DeltaTable.createOrReplace(
                logStore, tablePath, simpleSchema,
                partitionColumns = listOf("id")
            )
            val snapshot = table.snapshot()

            snapshot.metaData!!.partitionColumns shouldBe listOf("id")
        }

        @Test
        fun `stores configuration`() {
            val config = mapOf("delta.appendOnly" to "false")
            val table = DeltaTable.createOrReplace(
                logStore, tablePath, simpleSchema,
                configuration = config
            )
            val snapshot = table.snapshot()

            snapshot.metaData!!.configuration shouldBe config
        }

        @Test
        fun `sets createdTime`() {
            val before = System.currentTimeMillis()
            val table = DeltaTable.createOrReplace(logStore, tablePath, simpleSchema)
            val snapshot = table.snapshot()

            snapshot.metaData!!.createdTime.shouldNotBeNull()
            snapshot.metaData!!.createdTime!! shouldBeGreaterThanOrEqual before
        }

        @Test
        fun `stores engine info in commit info`() {
            val table = DeltaTable.createOrReplace(
                logStore, tablePath, simpleSchema,
                engineInfo = "delta-cdc-connector/1.0"
            )
            val snapshot = table.snapshot()

            snapshot.commitInfo.shouldNotBeNull()
            snapshot.commitInfo!!.engineInfo shouldBe "delta-cdc-connector/1.0"
        }

        @Test
        fun `generates unique table id`() {
            val table1 = DeltaTable.createOrReplace(logStore, "table-1", simpleSchema)
            val table2 = DeltaTable.createOrReplace(logStore, "table-2", simpleSchema)

            val id1 = table1.snapshot().metaData!!.id
            val id2 = table2.snapshot().metaData!!.id
            id1 shouldNotBe id2
        }

        @Test
        fun `replaces existing table with new metadata`() {
            DeltaTable.createOrReplace(logStore, tablePath, simpleSchema)

            val table = DeltaTable.forPath(logStore, tablePath)
            val txn = table.startTransaction()
            txn.addAction(LogTestFixtures.addFile("data-001.parquet"))
            txn.commit("WRITE")

            val newSchema = DeltaType.StructType(
                listOf(
                    StructField("id", DeltaType.LongType, nullable = false),
                    StructField("name", DeltaType.StringType, nullable = true)
                )
            )
            val replaced = DeltaTable.createOrReplace(logStore, tablePath, newSchema)
            val snapshot = replaced.snapshot()

            snapshot.version shouldBe 2L
            snapshot.activeFiles.shouldBeEmpty()
            snapshot.commitInfo!!.operation shouldBe "CREATE OR REPLACE TABLE"
            snapshot.commitInfo!!.isBlindAppend shouldBe false
            val storedSchema = DeltaSchema.fromJson(snapshot.metaData!!.schemaString)
            storedSchema.fields[0].type shouldBe DeltaType.LongType
        }
    }

    @Nested
    inner class StartTransaction {

        @Test
        fun `returns transaction with current snapshot`() {
            DeltaTable.createOrReplace(logStore, tablePath, simpleSchema)
            val table = DeltaTable.forPath(logStore, tablePath)

            val txn = table.startTransaction()

            txn.readSnapshot.version shouldBe 0L
        }

        @Test
        fun `transaction on empty table has version -1`() {
            val table = DeltaTable.forPath(logStore, tablePath)
            val txn = table.startTransaction()

            txn.readSnapshot.version shouldBe -1L
        }
    }

    @Nested
    inner class AppendWorkflow {

        @Test
        fun `append files to existing table`() {
            DeltaTable.createOrReplace(logStore, tablePath, simpleSchema)
            val table = DeltaTable.forPath(logStore, tablePath)

            val txn = table.startTransaction()
            txn.addAction(LogTestFixtures.addFile("data-001.parquet", size = 2048L))
            txn.addAction(LogTestFixtures.addFile("data-002.parquet", size = 4096L))
            val version = txn.commit("WRITE")

            version shouldBe 1L
            val snapshot = table.snapshot()
            snapshot.version shouldBe 1L
            snapshot.activeFiles shouldHaveSize 2
            snapshot.commitInfo!!.operation shouldBe "WRITE"
            snapshot.commitInfo!!.isBlindAppend shouldBe true
        }

        @Test
        fun `multiple sequential appends`() {
            DeltaTable.createOrReplace(logStore, tablePath, simpleSchema)
            val table = DeltaTable.forPath(logStore, tablePath)

            for (i in 1..5) {
                val txn = table.startTransaction()
                txn.addAction(LogTestFixtures.addFile("data-%03d.parquet".format(i)))
                txn.commit("WRITE")
            }

            val snapshot = table.snapshot()
            snapshot.version shouldBe 5L
            snapshot.activeFiles shouldHaveSize 5
        }
    }

    @Nested
    inner class MergeWorkflow {

        @Test
        fun `merge replaces files atomically`() {
            DeltaTable.createOrReplace(logStore, tablePath, simpleSchema)
            val table = DeltaTable.forPath(logStore, tablePath)

            val txn1 = table.startTransaction()
            txn1.addAction(LogTestFixtures.addFile("data-001.parquet", size = 1024L))
            txn1.addAction(LogTestFixtures.addFile("data-002.parquet", size = 2048L))
            txn1.commit("WRITE")

            val txn2 = table.startTransaction()
            txn2.addAction(LogTestFixtures.removeFile("data-001.parquet"))
            txn2.addAction(LogTestFixtures.addFile("data-003.parquet", size = 3072L))
            val version = txn2.commit("MERGE")

            version shouldBe 2L
            val snapshot = table.snapshot()
            snapshot.activeFiles shouldHaveSize 2
            val paths = snapshot.activeFiles.map { it.path }.toSet()
            paths shouldBe setOf("data-002.parquet", "data-003.parquet")
            snapshot.commitInfo!!.operation shouldBe "MERGE"
            snapshot.commitInfo!!.isBlindAppend shouldBe false
        }
    }

    @Nested
    inner class SetTransactionWorkflow {

        @Test
        fun `SetTransaction stored and retrievable via snapshot`() {
            DeltaTable.createOrReplace(logStore, tablePath, simpleSchema)
            val table = DeltaTable.forPath(logStore, tablePath)

            val txn = table.startTransaction()
            txn.addAction(LogTestFixtures.addFile("data-001.parquet"))
            txn.addAction(
                SetTransaction(
                    appId = "delta-cdc-sink-connector-topic0-0",
                    version = 42L,
                    lastUpdated = System.currentTimeMillis()
                )
            )
            txn.commit("MERGE")

            val snapshot = table.snapshot()
            snapshot.transactions shouldBe mapOf(
                "delta-cdc-sink-connector-topic0-0" to snapshot.transactions["delta-cdc-sink-connector-topic0-0"]!!
            )
            snapshot.transactions["delta-cdc-sink-connector-topic0-0"]!!.version shouldBe 42L
        }

        @Test
        fun `multiple SetTransactions across commits`() {
            DeltaTable.createOrReplace(logStore, tablePath, simpleSchema)
            val table = DeltaTable.forPath(logStore, tablePath)

            val txn1 = table.startTransaction()
            txn1.addAction(LogTestFixtures.addFile("data-001.parquet"))
            txn1.addAction(SetTransaction("app-p0", version = 100L))
            txn1.commit("MERGE")

            val txn2 = table.startTransaction()
            txn2.addAction(LogTestFixtures.addFile("data-002.parquet"))
            txn2.addAction(SetTransaction("app-p1", version = 200L))
            txn2.commit("MERGE")

            val snapshot = table.snapshot()
            snapshot.transactions["app-p0"]!!.version shouldBe 100L
            snapshot.transactions["app-p1"]!!.version shouldBe 200L
        }

        @Test
        fun `SetTransaction version updates on subsequent commit`() {
            DeltaTable.createOrReplace(logStore, tablePath, simpleSchema)
            val table = DeltaTable.forPath(logStore, tablePath)

            val txn1 = table.startTransaction()
            txn1.addAction(LogTestFixtures.addFile("data-001.parquet"))
            txn1.addAction(SetTransaction("app-p0", version = 100L))
            txn1.commit("MERGE")

            val txn2 = table.startTransaction()
            txn2.addAction(LogTestFixtures.addFile("data-002.parquet"))
            txn2.addAction(SetTransaction("app-p0", version = 150L))
            txn2.commit("MERGE")

            val snapshot = table.snapshot()
            snapshot.transactions["app-p0"]!!.version shouldBe 150L
        }
    }

    @Nested
    inner class EndToEnd {

        @Test
        fun `create, insert, update, verify`() {
            DeltaTable.createOrReplace(logStore, tablePath, simpleSchema)
            val table = DeltaTable.forPath(logStore, tablePath)

            val txn1 = table.startTransaction()
            txn1.addAction(LogTestFixtures.addFile("batch-1.parquet", size = 1024L))
            txn1.addAction(SetTransaction("app-p0", version = 10L))
            val v1 = txn1.commit("WRITE")
            v1 shouldBe 1L

            val txn2 = table.startTransaction()
            txn2.addAction(LogTestFixtures.removeFile("batch-1.parquet"))
            txn2.addAction(LogTestFixtures.addFile("batch-1-merged.parquet", size = 2048L))
            txn2.addAction(LogTestFixtures.addFile("batch-2-inserts.parquet", size = 512L))
            txn2.addAction(SetTransaction("app-p0", version = 20L))
            val v2 = txn2.commit("MERGE")
            v2 shouldBe 2L

            val snapshot = table.snapshot()
            snapshot.version shouldBe 2L
            snapshot.activeFiles shouldHaveSize 2
            val paths = snapshot.activeFiles.map { it.path }.toSet()
            paths shouldBe setOf("batch-1-merged.parquet", "batch-2-inserts.parquet")
            snapshot.transactions["app-p0"]!!.version shouldBe 20L
            snapshot.metaData.shouldNotBeNull()
            snapshot.protocol.shouldNotBeNull()
        }
    }
}

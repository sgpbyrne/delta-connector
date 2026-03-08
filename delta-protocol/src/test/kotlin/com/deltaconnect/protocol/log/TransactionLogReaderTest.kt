package com.deltaconnect.protocol.log

import com.deltaconnect.protocol.DeltaSnapshot
import com.deltaconnect.protocol.storage.LocalFileSystemLogStore
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.maps.shouldBeEmpty as shouldBeEmptyMap
import io.kotest.matchers.maps.shouldHaveSize as shouldHaveMapSize
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

class TransactionLogReaderTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var logStore: LocalFileSystemLogStore
    private lateinit var reader: TransactionLogReader

    private val tablePath = "test-table"

    @BeforeEach
    fun setUp() {
        logStore = LocalFileSystemLogStore(tempDir)
        reader = TransactionLogReader(logStore)
    }

    @Nested
    inner class EmptyTable {

        @Test
        fun `getSnapshot returns empty snapshot for non-existent table`() {
            val snapshot = reader.getSnapshot(tablePath)

            snapshot shouldBe DeltaSnapshot.empty()
            snapshot.version shouldBe -1L
            snapshot.activeFiles.shouldBeEmpty()
            snapshot.metaData.shouldBeNull()
            snapshot.protocol.shouldBeNull()
            snapshot.transactions.shouldBeEmptyMap()
            snapshot.commitInfo.shouldBeNull()
        }

        @Test
        fun `getLatestVersion returns -1 for empty table`() {
            reader.getLatestVersion(tablePath) shouldBe -1L
        }
    }

    @Nested
    inner class SingleCommit {

        @Test
        fun `captures protocol, metaData, addFile, and commitInfo from initial commit`() {
            val file = LogTestFixtures.addFile("data-001.parquet", size = 2048L)
            LogTestFixtures.writeInitialCommit(logStore, tablePath, listOf(file))

            val snapshot = reader.getSnapshot(tablePath)

            snapshot.version shouldBe 0L
            snapshot.activeFiles shouldHaveSize 1
            snapshot.activeFiles.first().path shouldBe "data-001.parquet"
            snapshot.activeFiles.first().size shouldBe 2048L
            snapshot.metaData.shouldNotBeNull()
            snapshot.metaData!!.id shouldBe "test-table-id"
            snapshot.protocol.shouldNotBeNull()
            snapshot.protocol!!.minReaderVersion shouldBe 1
            snapshot.protocol!!.minWriterVersion shouldBe 2
            snapshot.commitInfo.shouldNotBeNull()
            snapshot.commitInfo!!.operation shouldBe "CREATE TABLE"
        }
    }

    @Nested
    inner class MultipleCommits {

        @Test
        fun `accumulates files across versions`() {
            LogTestFixtures.writeInitialCommit(logStore, tablePath, listOf(
                LogTestFixtures.addFile("data-001.parquet")
            ))
            LogTestFixtures.writeCommit(logStore, tablePath, 1L, listOf(
                LogTestFixtures.addFile("data-002.parquet"),
                LogTestFixtures.commitInfo(operation = "WRITE")
            ))

            val snapshot = reader.getSnapshot(tablePath)

            snapshot.version shouldBe 1L
            snapshot.activeFiles shouldHaveSize 2
            val paths = snapshot.activeFiles.map { it.path }.toSet()
            paths shouldBe setOf("data-001.parquet", "data-002.parquet")
        }

        @Test
        fun `getLatestVersion returns highest version`() {
            LogTestFixtures.writeInitialCommit(logStore, tablePath)
            LogTestFixtures.writeCommit(logStore, tablePath, 1L, listOf(
                LogTestFixtures.addFile("data-001.parquet")
            ))
            LogTestFixtures.writeCommit(logStore, tablePath, 2L, listOf(
                LogTestFixtures.addFile("data-002.parquet")
            ))

            reader.getLatestVersion(tablePath) shouldBe 2L
        }
    }

    @Nested
    inner class Reconciliation {

        @Test
        fun `remove cancels add across versions`() {
            LogTestFixtures.writeInitialCommit(logStore, tablePath, listOf(
                LogTestFixtures.addFile("data-001.parquet")
            ))
            LogTestFixtures.writeCommit(logStore, tablePath, 1L, listOf(
                LogTestFixtures.removeFile("data-001.parquet"),
                LogTestFixtures.addFile("data-002.parquet")
            ))

            val snapshot = reader.getSnapshot(tablePath)

            snapshot.activeFiles shouldHaveSize 1
            snapshot.activeFiles.first().path shouldBe "data-002.parquet"
        }

        @Test
        fun `add and remove in same commit results in file absent`() {
            LogTestFixtures.writeInitialCommit(logStore, tablePath)
            LogTestFixtures.writeCommit(logStore, tablePath, 1L, listOf(
                LogTestFixtures.addFile("ephemeral.parquet"),
                LogTestFixtures.removeFile("ephemeral.parquet")
            ))

            val snapshot = reader.getSnapshot(tablePath)

            snapshot.activeFiles.none { it.path == "ephemeral.parquet" } shouldBe true
        }

        @Test
        fun `remove of non-existent path is a no-op`() {
            LogTestFixtures.writeInitialCommit(logStore, tablePath, listOf(
                LogTestFixtures.addFile("data-001.parquet")
            ))
            LogTestFixtures.writeCommit(logStore, tablePath, 1L, listOf(
                LogTestFixtures.removeFile("does-not-exist.parquet")
            ))

            val snapshot = reader.getSnapshot(tablePath)

            snapshot.activeFiles shouldHaveSize 1
            snapshot.activeFiles.first().path shouldBe "data-001.parquet"
        }

        @Test
        fun `re-add same path replaces with latest AddFile`() {
            LogTestFixtures.writeInitialCommit(logStore, tablePath, listOf(
                LogTestFixtures.addFile("data-001.parquet", size = 100L)
            ))
            LogTestFixtures.writeCommit(logStore, tablePath, 1L, listOf(
                LogTestFixtures.addFile("data-001.parquet", size = 200L)
            ))

            val snapshot = reader.getSnapshot(tablePath)

            snapshot.activeFiles shouldHaveSize 1
            snapshot.activeFiles.first().size shouldBe 200L
        }
    }

    @Nested
    inner class MetaDataHandling {

        @Test
        fun `latest metaData replaces earlier`() {
            val schema2 = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}"""
            LogTestFixtures.writeInitialCommit(logStore, tablePath)
            LogTestFixtures.writeCommit(logStore, tablePath, 1L, listOf(
                LogTestFixtures.metaData(id = "updated-id", schemaString = schema2)
            ))

            val snapshot = reader.getSnapshot(tablePath)

            snapshot.metaData.shouldNotBeNull()
            snapshot.metaData!!.id shouldBe "updated-id"
            snapshot.metaData!!.schemaString shouldBe schema2
        }
    }

    @Nested
    inner class ProtocolHandling {

        @Test
        fun `latest protocol replaces earlier`() {
            LogTestFixtures.writeInitialCommit(logStore, tablePath)
            LogTestFixtures.writeCommit(logStore, tablePath, 1L, listOf(
                LogTestFixtures.protocol(minReader = 1, minWriter = 3)
            ))

            val snapshot = reader.getSnapshot(tablePath)

            snapshot.protocol.shouldNotBeNull()
            snapshot.protocol!!.minWriterVersion shouldBe 3
        }
    }

    @Nested
    inner class TransactionHandling {

        @Test
        fun `accumulates transactions from multiple appIds`() {
            LogTestFixtures.writeInitialCommit(logStore, tablePath)
            LogTestFixtures.writeCommit(logStore, tablePath, 1L, listOf(
                LogTestFixtures.setTransaction("app1", version = 10L)
            ))
            LogTestFixtures.writeCommit(logStore, tablePath, 2L, listOf(
                LogTestFixtures.setTransaction("app2", version = 20L)
            ))

            val snapshot = reader.getSnapshot(tablePath)

            snapshot.transactions shouldHaveMapSize 2
            snapshot.transactions["app1"]!!.version shouldBe 10L
            snapshot.transactions["app2"]!!.version shouldBe 20L
        }

        @Test
        fun `latest transaction for same appId wins`() {
            LogTestFixtures.writeInitialCommit(logStore, tablePath)
            LogTestFixtures.writeCommit(logStore, tablePath, 1L, listOf(
                LogTestFixtures.setTransaction("app1", version = 10L)
            ))
            LogTestFixtures.writeCommit(logStore, tablePath, 2L, listOf(
                LogTestFixtures.setTransaction("app1", version = 30L)
            ))

            val snapshot = reader.getSnapshot(tablePath)

            snapshot.transactions shouldHaveMapSize 1
            snapshot.transactions["app1"]!!.version shouldBe 30L
        }
    }

    @Nested
    inner class CommitInfoHandling {

        @Test
        fun `only last commit info is retained`() {
            LogTestFixtures.writeInitialCommit(logStore, tablePath)
            LogTestFixtures.writeCommit(logStore, tablePath, 1L, listOf(
                LogTestFixtures.addFile("data-001.parquet"),
                LogTestFixtures.commitInfo(operation = "WRITE")
            ))
            LogTestFixtures.writeCommit(logStore, tablePath, 2L, listOf(
                LogTestFixtures.addFile("data-002.parquet"),
                LogTestFixtures.commitInfo(operation = "MERGE")
            ))

            val snapshot = reader.getSnapshot(tablePath)

            snapshot.commitInfo.shouldNotBeNull()
            snapshot.commitInfo!!.operation shouldBe "MERGE"
        }
    }

    @Nested
    inner class CheckpointHandling {

        @Test
        fun `still replays from version 0 when last_checkpoint exists`() {
            LogTestFixtures.writeInitialCommit(logStore, tablePath, listOf(
                LogTestFixtures.addFile("data-001.parquet")
            ))
            LogTestFixtures.writeCommit(logStore, tablePath, 1L, listOf(
                LogTestFixtures.addFile("data-002.parquet")
            ))
            logStore.writeLastCheckpoint(tablePath, """{"version":0,"size":3}""")

            val snapshot = reader.getSnapshot(tablePath)

            snapshot.version shouldBe 1L
            snapshot.activeFiles shouldHaveSize 2
        }

        @Test
        fun `handles corrupt last_checkpoint gracefully`() {
            LogTestFixtures.writeInitialCommit(logStore, tablePath, listOf(
                LogTestFixtures.addFile("data-001.parquet")
            ))
            logStore.writeLastCheckpoint(tablePath, "not valid json {{{")

            val snapshot = reader.getSnapshot(tablePath)

            snapshot.version shouldBe 0L
            snapshot.activeFiles shouldHaveSize 1
        }

        @Test
        fun `readCheckpointInfo parses valid checkpoint`() {
            logStore.writeLastCheckpoint(tablePath, """{"version":5,"size":100,"parts":1}""")

            val info = reader.readCheckpointInfo(tablePath)

            info.shouldNotBeNull()
            info.version shouldBe 5L
            info.size shouldBe 100L
            info.parts shouldBe 1
        }

        @Test
        fun `readCheckpointInfo returns null when no checkpoint exists`() {
            reader.readCheckpointInfo(tablePath).shouldBeNull()
        }
    }

    @Nested
    inner class Scale {

        @Test
        fun `handles 100 commits correctly`() {
            LogTestFixtures.writeInitialCommit(logStore, tablePath)
            for (v in 1L..99L) {
                LogTestFixtures.writeCommit(logStore, tablePath, v, listOf(
                    LogTestFixtures.addFile("data-%03d.parquet".format(v))
                ))
            }

            val snapshot = reader.getSnapshot(tablePath)

            snapshot.version shouldBe 99L
            snapshot.activeFiles shouldHaveSize 99
        }
    }
}

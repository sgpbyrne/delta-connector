package com.deltaconnect.protocol

import com.deltaconnect.protocol.actions.ActionSerializer
import com.deltaconnect.protocol.actions.AddFile
import com.deltaconnect.protocol.actions.RemoveFile
import com.deltaconnect.protocol.actions.SetTransaction
import com.deltaconnect.protocol.log.LogTestFixtures
import com.deltaconnect.protocol.schema.DeltaType
import com.deltaconnect.protocol.schema.StructField
import com.deltaconnect.protocol.storage.CommitConflictException
import com.deltaconnect.protocol.storage.DeltaLogStore
import com.deltaconnect.protocol.storage.LocalFileSystemLogStore
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.io.OutputStream
import java.nio.file.Path

class DeltaTransactionTest {

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
        DeltaTable.createOrReplace(logStore, tablePath, simpleSchema)
    }

    @Nested
    inner class BasicCommit {

        @Test
        fun `commits at next version after snapshot`() {
            val table = DeltaTable.forPath(logStore, tablePath)
            val txn = table.startTransaction()
            txn.addAction(LogTestFixtures.addFile("data-001.parquet"))

            val version = txn.commit("WRITE")

            version shouldBe 1L
        }

        @Test
        fun `returns committed version`() {
            val table = DeltaTable.forPath(logStore, tablePath)

            val txn1 = table.startTransaction()
            txn1.addAction(LogTestFixtures.addFile("data-001.parquet"))
            txn1.commit("WRITE") shouldBe 1L

            val txn2 = table.startTransaction()
            txn2.addAction(LogTestFixtures.addFile("data-002.parquet"))
            txn2.commit("WRITE") shouldBe 2L
        }

        @Test
        fun `includes CommitInfo with operation`() {
            val table = DeltaTable.forPath(logStore, tablePath)
            val txn = table.startTransaction()
            txn.addAction(LogTestFixtures.addFile("data-001.parquet"))
            txn.commit("WRITE", mapOf("mode" to "Append"))

            val snapshot = table.snapshot()
            snapshot.commitInfo.shouldNotBeNull()
            snapshot.commitInfo!!.operation shouldBe "WRITE"
            snapshot.commitInfo!!.operationParameters shouldBe mapOf("mode" to "Append")
            snapshot.commitInfo!!.engineInfo shouldBe null
        }

        @Test
        fun `blind append has isBlindAppend true`() {
            val table = DeltaTable.forPath(logStore, tablePath)
            val txn = table.startTransaction()
            txn.addAction(LogTestFixtures.addFile("data-001.parquet"))
            txn.commit("WRITE")

            val snapshot = table.snapshot()
            snapshot.commitInfo!!.isBlindAppend shouldBe true
        }

        @Test
        fun `commit with RemoveFile has isBlindAppend false`() {
            val table = DeltaTable.forPath(logStore, tablePath)

            val txn1 = table.startTransaction()
            txn1.addAction(LogTestFixtures.addFile("data-001.parquet"))
            txn1.commit("WRITE")

            val txn2 = table.startTransaction()
            txn2.addAction(LogTestFixtures.removeFile("data-001.parquet"))
            txn2.addAction(LogTestFixtures.addFile("data-002.parquet"))
            txn2.commit("MERGE")

            val snapshot = table.snapshot()
            snapshot.commitInfo!!.isBlindAppend shouldBe false
        }
    }

    @Nested
    inner class Validation {

        @Test
        fun `throws on empty transaction`() {
            val table = DeltaTable.forPath(logStore, tablePath)
            val txn = table.startTransaction()

            val ex = assertThrows<IllegalStateException> {
                txn.commit("WRITE")
            }
            ex.message shouldContain "empty"
        }
    }

    @Nested
    inner class BlindAppendConflictRetry {

        @Test
        fun `retries blind append on conflict`() {
            val table = DeltaTable.forPath(logStore, tablePath)

            val txn = table.startTransaction()
            txn.addAction(LogTestFixtures.addFile("txn-file.parquet"))

            LogTestFixtures.writeCommit(logStore, tablePath, 1L, listOf(
                LogTestFixtures.addFile("concurrent-file.parquet"),
                LogTestFixtures.commitInfo(operation = "WRITE")
            ))

            val version = txn.commit("WRITE")
            version shouldBe 2L

            val snapshot = table.snapshot()
            snapshot.activeFiles shouldHaveSize 2
            val paths = snapshot.activeFiles.map { it.path }.toSet()
            paths shouldBe setOf("concurrent-file.parquet", "txn-file.parquet")
        }

        @Test
        fun `retries blind append through multiple conflicts`() {
            val table = DeltaTable.forPath(logStore, tablePath)
            val txn = table.startTransaction()
            txn.addAction(LogTestFixtures.addFile("txn-file.parquet"))

            for (v in 1L..3L) {
                LogTestFixtures.writeCommit(logStore, tablePath, v, listOf(
                    LogTestFixtures.addFile("concurrent-$v.parquet")
                ))
            }

            val version = txn.commit("WRITE")
            version shouldBe 4L
        }
    }

    @Nested
    inner class DisjointFileConflictRetry {

        @Test
        fun `retries merge when concurrent commit touches different files`() {
            val table = DeltaTable.forPath(logStore, tablePath)

            val txn1 = table.startTransaction()
            txn1.addAction(LogTestFixtures.addFile("file-a.parquet"))
            txn1.addAction(LogTestFixtures.addFile("file-b.parquet"))
            txn1.commit("WRITE")

            val txn2 = table.startTransaction()
            txn2.addAction(LogTestFixtures.removeFile("file-a.parquet"))
            txn2.addAction(LogTestFixtures.addFile("file-a-v2.parquet"))

            LogTestFixtures.writeCommit(logStore, tablePath, 2L, listOf(
                LogTestFixtures.removeFile("file-b.parquet"),
                LogTestFixtures.addFile("file-b-v2.parquet")
            ))

            val version = txn2.commit("MERGE")
            version shouldBe 3L

            val snapshot = table.snapshot()
            val paths = snapshot.activeFiles.map { it.path }.toSet()
            paths shouldBe setOf("file-a-v2.parquet", "file-b-v2.parquet")
        }

        @Test
        fun `retries merge when concurrent commit only adds new files`() {
            val table = DeltaTable.forPath(logStore, tablePath)

            val txn1 = table.startTransaction()
            txn1.addAction(LogTestFixtures.addFile("file-a.parquet"))
            txn1.commit("WRITE")

            val txn2 = table.startTransaction()
            txn2.addAction(LogTestFixtures.removeFile("file-a.parquet"))
            txn2.addAction(LogTestFixtures.addFile("file-a-v2.parquet"))

            LogTestFixtures.writeCommit(logStore, tablePath, 2L, listOf(
                LogTestFixtures.addFile("new-file.parquet")
            ))

            val version = txn2.commit("MERGE")
            version shouldBe 3L
        }
    }

    @Nested
    inner class OverlappingFileConflict {

        @Test
        fun `fails when concurrent commit removes same file`() {
            val table = DeltaTable.forPath(logStore, tablePath)

            val txn1 = table.startTransaction()
            txn1.addAction(LogTestFixtures.addFile("file-a.parquet"))
            txn1.commit("WRITE")

            val txn2 = table.startTransaction()
            txn2.addAction(LogTestFixtures.removeFile("file-a.parquet"))
            txn2.addAction(LogTestFixtures.addFile("file-a-v2.parquet"))

            LogTestFixtures.writeCommit(logStore, tablePath, 2L, listOf(
                LogTestFixtures.removeFile("file-a.parquet"),
                LogTestFixtures.addFile("file-a-other.parquet")
            ))

            assertThrows<CommitConflictException> {
                txn2.commit("MERGE")
            }
        }

        @Test
        fun `fails when concurrent commit adds to same path we remove`() {
            val table = DeltaTable.forPath(logStore, tablePath)

            val txn1 = table.startTransaction()
            txn1.addAction(LogTestFixtures.addFile("file-a.parquet"))
            txn1.commit("WRITE")

            val txn2 = table.startTransaction()
            txn2.addAction(LogTestFixtures.removeFile("file-a.parquet"))
            txn2.addAction(LogTestFixtures.addFile("file-a-replaced.parquet"))

            LogTestFixtures.writeCommit(logStore, tablePath, 2L, listOf(
                LogTestFixtures.addFile("file-a.parquet", size = 9999L)
            ))

            assertThrows<CommitConflictException> {
                txn2.commit("MERGE")
            }
        }
    }

    @Nested
    inner class MaxRetries {

        @Test
        fun `fails after max retries exceeded`() {
            val conflictStore = ConflictInjectingLogStore(logStore, conflictsToInject = 3)
            val table = DeltaTable.forPath(conflictStore, tablePath)
            val txn = table.startTransaction()
            txn.addAction(LogTestFixtures.addFile("txn-file.parquet"))

            assertThrows<CommitConflictException> {
                txn.commit("WRITE", maxRetries = 2)
            }
            conflictStore.writeAttempts shouldBe 3
        }

        @Test
        fun `succeeds when retries within limit`() {
            val conflictStore = ConflictInjectingLogStore(logStore, conflictsToInject = 2)
            val table = DeltaTable.forPath(conflictStore, tablePath)
            val txn = table.startTransaction()
            txn.addAction(LogTestFixtures.addFile("txn-file.parquet"))

            val version = txn.commit("WRITE", maxRetries = 3)
            version shouldBe 3L
            conflictStore.writeAttempts shouldBe 3
        }

        @Test
        fun `zero retries fails on first conflict`() {
            val table = DeltaTable.forPath(logStore, tablePath)
            val txn = table.startTransaction()
            txn.addAction(LogTestFixtures.addFile("txn-file.parquet"))

            LogTestFixtures.writeCommit(logStore, tablePath, 1L, listOf(
                LogTestFixtures.addFile("concurrent.parquet")
            ))

            assertThrows<CommitConflictException> {
                txn.commit("WRITE", maxRetries = 0)
            }
        }
    }

    @Nested
    inner class TransactionWithSetTransaction {

        @Test
        fun `SetTransaction committed alongside data actions`() {
            val table = DeltaTable.forPath(logStore, tablePath)
            val txn = table.startTransaction()
            txn.addAction(LogTestFixtures.addFile("data-001.parquet"))
            txn.addAction(SetTransaction("my-app-p0", version = 500L))
            txn.commit("MERGE")

            val snapshot = table.snapshot()
            snapshot.transactions["my-app-p0"]!!.version shouldBe 500L
        }

        @Test
        fun `SetTransaction survives conflict retry`() {
            val table = DeltaTable.forPath(logStore, tablePath)
            val txn = table.startTransaction()
            txn.addAction(LogTestFixtures.addFile("data-001.parquet"))
            txn.addAction(SetTransaction("my-app-p0", version = 500L))

            LogTestFixtures.writeCommit(logStore, tablePath, 1L, listOf(
                LogTestFixtures.addFile("concurrent.parquet")
            ))

            val version = txn.commit("WRITE")
            version shouldBe 2L

            val snapshot = table.snapshot()
            snapshot.transactions["my-app-p0"]!!.version shouldBe 500L
        }
    }

    @Nested
    inner class MultipleActions {

        @Test
        fun `addActions adds multiple actions at once`() {
            val table = DeltaTable.forPath(logStore, tablePath)
            val txn = table.startTransaction()
            txn.addActions(
                listOf(
                    LogTestFixtures.addFile("data-001.parquet"),
                    LogTestFixtures.addFile("data-002.parquet"),
                    LogTestFixtures.addFile("data-003.parquet")
                )
            )
            txn.commit("WRITE")

            val snapshot = table.snapshot()
            snapshot.activeFiles shouldHaveSize 3
        }

        @Test
        fun `mixed addAction and addActions`() {
            val table = DeltaTable.forPath(logStore, tablePath)
            val txn = table.startTransaction()
            txn.addAction(LogTestFixtures.addFile("data-001.parquet"))
            txn.addActions(
                listOf(
                    LogTestFixtures.removeFile("data-001.parquet"),
                    LogTestFixtures.addFile("data-002.parquet")
                )
            )
            txn.commit("MERGE")

            val snapshot = table.snapshot()
            snapshot.activeFiles shouldHaveSize 1
            snapshot.activeFiles.first().path shouldBe "data-002.parquet"
        }
    }

    @Nested
    inner class EngineInfo {

        @Test
        fun `default engine info is null`() {
            val table = DeltaTable.forPath(logStore, tablePath)
            val txn = table.startTransaction()
            txn.addAction(LogTestFixtures.addFile("data-001.parquet"))
            txn.commit("WRITE")

            table.snapshot().commitInfo!!.engineInfo shouldBe null
        }

        @Test
        fun `custom engine info`() {
            val table = DeltaTable.forPath(logStore, tablePath)
            val txn = table.startTransaction()
            txn.addAction(LogTestFixtures.addFile("data-001.parquet"))
            txn.commit("WRITE", engineInfo = "custom-engine/1.0")

            table.snapshot().commitInfo!!.engineInfo shouldBe "custom-engine/1.0"
        }
    }

    @Nested
    inner class CheckpointResilience {

        @Test
        fun `commit succeeds even when checkpoint write fails`() {
            val checkpointTable = "checkpoint-test-table"
            val failingStore = CheckpointFailingLogStore(logStore)
            DeltaTable.createOrReplace(failingStore, checkpointTable, simpleSchema)
            val table = DeltaTable.forPath(failingStore, checkpointTable)

            for (i in 1..10) {
                val txn = table.startTransaction()
                txn.addAction(LogTestFixtures.addFile("data-%03d.parquet".format(i)))
                val version = txn.commit("WRITE")
                version shouldBe i.toLong()
            }

            val snapshot = table.snapshot()
            snapshot.version shouldBe 10L
            snapshot.activeFiles shouldHaveSize 10
        }
    }

    private class ConflictInjectingLogStore(
        private val delegate: LocalFileSystemLogStore,
        private val conflictsToInject: Int
    ) : DeltaLogStore {

        var writeAttempts: Int = 0
            private set

        override fun writeCommit(tablePath: String, version: Long, content: ByteArray) {
            writeAttempts++
            if (writeAttempts <= conflictsToInject) {
                val competingContent = ActionSerializer.serializeActions(
                    listOf(LogTestFixtures.addFile("conflict-$writeAttempts.parquet"))
                ).toByteArray(Charsets.UTF_8)
                delegate.writeCommit(tablePath, version, competingContent)
                throw CommitConflictException(tablePath, version)
            }
            delegate.writeCommit(tablePath, version, content)
        }

        override fun readCommit(tablePath: String, version: Long): ByteArray? =
            delegate.readCommit(tablePath, version)

        override fun listCommitVersions(tablePath: String, startVersion: Long): List<Long> =
            delegate.listCommitVersions(tablePath, startVersion)

        override fun createDataFile(filePath: String): OutputStream =
            delegate.createDataFile(filePath)

        override fun readDataFileAsInputFile(filePath: String): org.apache.parquet.io.InputFile =
            delegate.readDataFileAsInputFile(filePath)

        override fun readLastCheckpoint(tablePath: String): String? =
            delegate.readLastCheckpoint(tablePath)

        override fun writeLastCheckpoint(tablePath: String, content: String) =
            delegate.writeLastCheckpoint(tablePath, content)
    }

    private class CheckpointFailingLogStore(
        private val delegate: LocalFileSystemLogStore
    ) : DeltaLogStore {

        override fun writeCommit(tablePath: String, version: Long, content: ByteArray) =
            delegate.writeCommit(tablePath, version, content)

        override fun readCommit(tablePath: String, version: Long): ByteArray? =
            delegate.readCommit(tablePath, version)

        override fun listCommitVersions(tablePath: String, startVersion: Long): List<Long> =
            delegate.listCommitVersions(tablePath, startVersion)

        override fun createDataFile(filePath: String): OutputStream {
            if (filePath.endsWith(".checkpoint.parquet")) {
                throw RuntimeException("Simulated checkpoint write failure")
            }
            return delegate.createDataFile(filePath)
        }

        override fun readDataFileAsInputFile(filePath: String): org.apache.parquet.io.InputFile =
            delegate.readDataFileAsInputFile(filePath)

        override fun readLastCheckpoint(tablePath: String): String? =
            delegate.readLastCheckpoint(tablePath)

        override fun writeLastCheckpoint(tablePath: String, content: String) =
            delegate.writeLastCheckpoint(tablePath, content)
    }
}

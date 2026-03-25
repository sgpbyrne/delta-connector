package com.deltaconnect.protocol.storage

import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

class LocalFileSystemLogStoreTest {
    @TempDir
    lateinit var tempDir: Path

    private lateinit var store: LocalFileSystemLogStore

    private val tablePath = "test_table"

    @BeforeEach
    fun setUp() {
        store = LocalFileSystemLogStore(tempDir)
    }

    @Nested
    inner class WriteCommit {
        @Test
        fun `writes commit file with correct name`() {
            val content = """{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}""".toByteArray()
            store.writeCommit(tablePath, 0, content)

            val read = store.readCommit(tablePath, 0)
            read.shouldNotBeNull()
            String(read) shouldBe String(content)
        }

        @Test
        fun `creates _delta_log directory if missing`() {
            store.writeCommit(tablePath, 0, "{}".toByteArray())

            val logDir = tempDir.resolve(tablePath).resolve("_delta_log")
            logDir.toFile().exists() shouldBe true
        }

        @Test
        fun `throws CommitConflictException when version already exists`() {
            store.writeCommit(tablePath, 0, "first".toByteArray())

            val ex =
                assertThrows<CommitConflictException> {
                    store.writeCommit(tablePath, 0, "second".toByteArray())
                }
            ex.tablePath shouldBe tablePath
            ex.version shouldBe 0L
            ex.message shouldContain "version 0"
        }

        @Test
        fun `allows writing different versions`() {
            store.writeCommit(tablePath, 0, "v0".toByteArray())
            store.writeCommit(tablePath, 1, "v1".toByteArray())
            store.writeCommit(tablePath, 2, "v2".toByteArray())

            String(store.readCommit(tablePath, 0)!!) shouldBe "v0"
            String(store.readCommit(tablePath, 1)!!) shouldBe "v1"
            String(store.readCommit(tablePath, 2)!!) shouldBe "v2"
        }
    }

    @Nested
    inner class ReadCommit {
        @Test
        fun `returns null for non-existent version`() {
            store.readCommit(tablePath, 0).shouldBeNull()
        }

        @Test
        fun `returns null for non-existent table`() {
            store.readCommit("no_such_table", 0).shouldBeNull()
        }
    }

    @Nested
    inner class ListCommitVersions {
        @Test
        fun `returns empty list for non-existent table`() {
            store.listCommitVersions(tablePath).shouldBeEmpty()
        }

        @Test
        fun `returns sorted versions`() {
            store.writeCommit(tablePath, 2, "v2".toByteArray())
            store.writeCommit(tablePath, 0, "v0".toByteArray())
            store.writeCommit(tablePath, 1, "v1".toByteArray())

            store.listCommitVersions(tablePath) shouldContainExactly listOf(0L, 1L, 2L)
        }

        @Test
        fun `filters by startVersion`() {
            store.writeCommit(tablePath, 0, "v0".toByteArray())
            store.writeCommit(tablePath, 1, "v1".toByteArray())
            store.writeCommit(tablePath, 2, "v2".toByteArray())

            store.listCommitVersions(tablePath, startVersion = 1) shouldContainExactly listOf(1L, 2L)
        }

        @Test
        fun `ignores checkpoint files`() {
            store.writeCommit(tablePath, 0, "v0".toByteArray())
            val logDir = tempDir.resolve(tablePath).resolve("_delta_log")
            logDir.resolve("00000000000000000000.checkpoint.parquet").toFile().createNewFile()

            store.listCommitVersions(tablePath) shouldContainExactly listOf(0L)
        }
    }

    @Nested
    inner class DataFiles {
        @Test
        fun `createDataFile and readDataFileAsInputFile round-trip`() {
            val filePath = "$tablePath/data/part-00000.parquet"
            val data = "parquet-content".toByteArray()

            store.createDataFile(filePath).use { out ->
                out.write(data)
            }

            val inputFile = store.readDataFileAsInputFile(filePath)
            inputFile.length shouldBe data.size.toLong()
        }

        @Test
        fun `createDataFile creates parent directories`() {
            val filePath = "$tablePath/deep/nested/dir/file.parquet"
            store.createDataFile(filePath).use { it.write("data".toByteArray()) }

            val inputFile = store.readDataFileAsInputFile(filePath)
            inputFile.length shouldBe 4L
        }
    }

    @Nested
    inner class LastCheckpoint {
        @Test
        fun `returns null when no checkpoint exists`() {
            store.readLastCheckpoint(tablePath).shouldBeNull()
        }

        @Test
        fun `write and read round-trip`() {
            val content = """{"version":10,"size":1234}"""
            store.writeLastCheckpoint(tablePath, content)
            store.readLastCheckpoint(tablePath) shouldBe content
        }

        @Test
        fun `overwrites existing checkpoint`() {
            store.writeLastCheckpoint(tablePath, """{"version":5}""")
            store.writeLastCheckpoint(tablePath, """{"version":10}""")
            store.readLastCheckpoint(tablePath) shouldBe """{"version":10}"""
        }
    }
}

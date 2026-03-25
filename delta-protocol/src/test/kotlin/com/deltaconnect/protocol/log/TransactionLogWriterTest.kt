package com.deltaconnect.protocol.log

import com.deltaconnect.protocol.actions.ActionSerializer
import com.deltaconnect.protocol.storage.CommitConflictException
import com.deltaconnect.protocol.storage.LocalFileSystemLogStore
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

class TransactionLogWriterTest {
    @TempDir
    lateinit var tempDir: Path

    private lateinit var logStore: LocalFileSystemLogStore
    private lateinit var writer: TransactionLogWriter

    private val tablePath = "test-table"

    @BeforeEach
    fun setUp() {
        logStore = LocalFileSystemLogStore(tempDir)
        writer = TransactionLogWriter(logStore)
    }

    @Nested
    inner class BasicCommit {
        @Test
        fun `writes commit and reads it back`() {
            val actions =
                listOf(
                    LogTestFixtures.protocol(),
                    LogTestFixtures.metaData(),
                    LogTestFixtures.commitInfo(operation = "CREATE TABLE"),
                )

            writer.commit(tablePath, 0L, actions)

            val content = logStore.readCommit(tablePath, 0L)
            content shouldBe ActionSerializer.serializeActions(actions).toByteArray(Charsets.UTF_8)
        }

        @Test
        fun `writes sequential versions`() {
            writer.commit(
                tablePath,
                0L,
                listOf(
                    LogTestFixtures.protocol(),
                    LogTestFixtures.metaData(),
                ),
            )
            writer.commit(
                tablePath,
                1L,
                listOf(
                    LogTestFixtures.addFile("data-001.parquet"),
                ),
            )
            writer.commit(
                tablePath,
                2L,
                listOf(
                    LogTestFixtures.addFile("data-002.parquet"),
                ),
            )

            logStore.listCommitVersions(tablePath) shouldBe listOf(0L, 1L, 2L)
        }

        @Test
        fun `serializes actions as newline-delimited JSON`() {
            val actions =
                listOf(
                    LogTestFixtures.protocol(),
                    LogTestFixtures.metaData(),
                    LogTestFixtures.addFile("data-001.parquet"),
                )

            writer.commit(tablePath, 0L, actions)

            val raw = String(logStore.readCommit(tablePath, 0L)!!, Charsets.UTF_8)
            val lines = raw.lines().filter { it.isNotBlank() }
            lines.size shouldBe 3
            lines[0] shouldContain "\"protocol\""
            lines[1] shouldContain "\"metaData\""
            lines[2] shouldContain "\"add\""
        }
    }

    @Nested
    inner class ConflictDetection {
        @Test
        fun `throws CommitConflictException on duplicate version`() {
            writer.commit(tablePath, 0L, listOf(LogTestFixtures.protocol()))

            assertThrows<CommitConflictException> {
                writer.commit(tablePath, 0L, listOf(LogTestFixtures.metaData()))
            }
        }
    }

    @Nested
    inner class Validation {
        @Test
        fun `rejects empty actions list`() {
            val ex =
                assertThrows<IllegalArgumentException> {
                    writer.commit(tablePath, 0L, emptyList())
                }
            ex.message shouldContain "empty"
        }
    }
}

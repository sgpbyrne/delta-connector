package com.deltaconnect.azure

import com.azure.core.http.rest.PagedIterable
import com.azure.storage.file.datalake.DataLakeFileClient
import com.azure.storage.file.datalake.DataLakeFileSystemClient
import com.azure.storage.file.datalake.models.DataLakeStorageException
import com.azure.storage.file.datalake.models.ListPathsOptions
import com.azure.storage.file.datalake.models.PathItem
import com.azure.storage.file.datalake.models.PathProperties
import com.azure.storage.file.datalake.options.FileParallelUploadOptions
import com.deltaconnect.protocol.storage.CommitConflictException
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.io.OutputStream
import java.time.Duration

class AdlsGen2LogStoreTest {

    private val fileSystemClient = mockk<DataLakeFileSystemClient>()
    private lateinit var logStore: AdlsGen2LogStore

    @BeforeEach
    fun setUp() {
        logStore = AdlsGen2LogStore(fileSystemClient)
    }

    @Nested
    inner class WriteCommit {

        @Test
        fun `writes commit with If-None-Match condition`() {
            val fileClient = mockk<DataLakeFileClient>()
            every { fileSystemClient.getFileClient(any()) } returns fileClient
            val optionsSlot = slot<FileParallelUploadOptions>()
            every { fileClient.uploadWithResponse(capture(optionsSlot), any(), any()) } returns mockk()

            val content = """{"add":{"path":"data.parquet"}}""".toByteArray()
            logStore.writeCommit("my-table", 0, content)

            verify { fileSystemClient.getFileClient("my-table/_delta_log/00000000000000000000.json") }
            optionsSlot.captured.requestConditions.ifNoneMatch shouldBe "*"
        }

        @Test
        fun `writes commit at version 5 with correct file path`() {
            val fileClient = mockk<DataLakeFileClient>()
            every { fileSystemClient.getFileClient(any()) } returns fileClient
            every { fileClient.uploadWithResponse(any<FileParallelUploadOptions>(), any(), any()) } returns mockk()

            logStore.writeCommit("my-table", 5, "data".toByteArray())

            verify { fileSystemClient.getFileClient("my-table/_delta_log/00000000000000000005.json") }
        }

        @Test
        fun `throws CommitConflictException on 409 status`() {
            val fileClient = mockk<DataLakeFileClient>()
            every { fileSystemClient.getFileClient(any()) } returns fileClient
            every { fileClient.uploadWithResponse(any<FileParallelUploadOptions>(), any(), any()) } throws
                DataLakeStorageException("Conflict", mockResponse(409), null)

            shouldThrow<CommitConflictException> {
                logStore.writeCommit("my-table", 0, "data".toByteArray())
            }
        }

        @Test
        fun `throws CommitConflictException on 412 precondition failed`() {
            val fileClient = mockk<DataLakeFileClient>()
            every { fileSystemClient.getFileClient(any()) } returns fileClient
            every { fileClient.uploadWithResponse(any<FileParallelUploadOptions>(), any(), any()) } throws
                DataLakeStorageException("Precondition Failed", mockResponse(412), null)

            shouldThrow<CommitConflictException> {
                logStore.writeCommit("my-table", 0, "data".toByteArray())
            }
        }

        @Test
        fun `rethrows non-conflict exceptions`() {
            val fileClient = mockk<DataLakeFileClient>()
            every { fileSystemClient.getFileClient(any()) } returns fileClient
            every { fileClient.uploadWithResponse(any<FileParallelUploadOptions>(), any(), any()) } throws
                DataLakeStorageException("Internal Error", mockResponse(500), null)

            shouldThrow<DataLakeStorageException> {
                logStore.writeCommit("my-table", 0, "data".toByteArray())
            }
        }
    }

    @Nested
    inner class ReadCommit {

        @Test
        fun `reads commit content`() {
            val fileClient = mockk<DataLakeFileClient>()
            every { fileSystemClient.getFileClient("my-table/_delta_log/00000000000000000005.json") } returns fileClient
            val content = """{"add":{"path":"data.parquet"}}""".toByteArray()
            every { fileClient.read(any<OutputStream>()) } answers {
                (firstArg<OutputStream>()).write(content)
            }

            val result = logStore.readCommit("my-table", 5)
            result shouldBe content
        }

        @Test
        fun `returns null for non-existent version`() {
            val fileClient = mockk<DataLakeFileClient>()
            every { fileSystemClient.getFileClient(any()) } returns fileClient
            every { fileClient.read(any<OutputStream>()) } throws
                DataLakeStorageException("Not Found", mockResponse(404), null)

            logStore.readCommit("my-table", 99).shouldBeNull()
        }
    }

    @Nested
    inner class ListCommitVersions {

        @Test
        fun `lists versions sorted ascending`() {
            val pathItems = listOf(
                createPathItem("my-table/_delta_log/00000000000000000002.json"),
                createPathItem("my-table/_delta_log/00000000000000000000.json"),
                createPathItem("my-table/_delta_log/00000000000000000001.json")
            )
            mockListPaths(pathItems)

            logStore.listCommitVersions("my-table") shouldContainExactly listOf(0L, 1L, 2L)
        }

        @Test
        fun `filters by startVersion`() {
            val pathItems = listOf(
                createPathItem("my-table/_delta_log/00000000000000000000.json"),
                createPathItem("my-table/_delta_log/00000000000000000001.json"),
                createPathItem("my-table/_delta_log/00000000000000000002.json")
            )
            mockListPaths(pathItems)

            logStore.listCommitVersions("my-table", 2) shouldContainExactly listOf(2L)
        }

        @Test
        fun `ignores checkpoint files`() {
            val pathItems = listOf(
                createPathItem("my-table/_delta_log/00000000000000000000.json"),
                createPathItem("my-table/_delta_log/00000000000000000010.checkpoint.parquet"),
                createPathItem("my-table/_delta_log/00000000000000000001.json")
            )
            mockListPaths(pathItems)

            logStore.listCommitVersions("my-table") shouldContainExactly listOf(0L, 1L)
        }

        @Test
        fun `returns empty list for non-existent directory`() {
            every { fileSystemClient.listPaths(any<ListPathsOptions>(), any<Duration>()) } throws
                DataLakeStorageException("Not Found", mockResponse(404), null)

            logStore.listCommitVersions("my-table").shouldBeEmpty()
        }

        @Test
        fun `passes correct path option`() {
            val optionsSlot = slot<ListPathsOptions>()
            mockListPaths(emptyList(), optionsSlot)

            logStore.listCommitVersions("my-table")

            optionsSlot.captured.path shouldBe "my-table/_delta_log/"
        }
    }

    @Nested
    inner class CreateDataFile {

        @Test
        fun `returns buffered output stream that uploads on close`() {
            val fileClient = mockk<DataLakeFileClient>()
            every { fileSystemClient.getFileClient("data/part-00000.parquet") } returns fileClient
            every { fileClient.uploadWithResponse(any<FileParallelUploadOptions>(), any(), any()) } returns mockk()

            val outputStream = logStore.createDataFile("data/part-00000.parquet")
            val content = "parquet-data".toByteArray()
            outputStream.write(content)
            outputStream.close()

            verify { fileClient.uploadWithResponse(any<FileParallelUploadOptions>(), any(), any()) }
        }

        @Test
        fun `close is idempotent`() {
            val fileClient = mockk<DataLakeFileClient>()
            every { fileSystemClient.getFileClient("data/part-00000.parquet") } returns fileClient
            every { fileClient.uploadWithResponse(any<FileParallelUploadOptions>(), any(), any()) } returns mockk()

            val outputStream = logStore.createDataFile("data/part-00000.parquet")
            outputStream.write("data".toByteArray())
            outputStream.close()
            outputStream.close() // second close should be no-op

            verify(exactly = 1) { fileClient.uploadWithResponse(any<FileParallelUploadOptions>(), any(), any()) }
        }
    }

    @Nested
    inner class ReadDataFileAsInputFile {

        @Test
        fun `returns InputFile with correct file size`() {
            val fileClient = mockk<DataLakeFileClient>()
            every { fileSystemClient.getFileClient("data/part-00000.parquet") } returns fileClient
            val properties = mockk<PathProperties>()
            every { properties.fileSize } returns 1024L
            every { fileClient.properties } returns properties

            val inputFile = logStore.readDataFileAsInputFile("data/part-00000.parquet")
            inputFile.length shouldBe 1024L
        }
    }

    @Nested
    inner class LastCheckpoint {

        @Test
        fun `reads last checkpoint content`() {
            val fileClient = mockk<DataLakeFileClient>()
            every { fileSystemClient.getFileClient("my-table/_delta_log/_last_checkpoint") } returns fileClient
            val content = """{"version":10,"size":42}"""
            every { fileClient.read(any<OutputStream>()) } answers {
                (firstArg<OutputStream>()).write(content.toByteArray())
            }

            logStore.readLastCheckpoint("my-table") shouldBe content
        }

        @Test
        fun `returns null when checkpoint does not exist`() {
            val fileClient = mockk<DataLakeFileClient>()
            every { fileSystemClient.getFileClient("my-table/_delta_log/_last_checkpoint") } returns fileClient
            every { fileClient.read(any<OutputStream>()) } throws
                DataLakeStorageException("Not Found", mockResponse(404), null)

            logStore.readLastCheckpoint("my-table").shouldBeNull()
        }

        @Test
        fun `writes last checkpoint content`() {
            val fileClient = mockk<DataLakeFileClient>()
            every { fileSystemClient.getFileClient("my-table/_delta_log/_last_checkpoint") } returns fileClient
            every { fileClient.uploadWithResponse(any<FileParallelUploadOptions>(), any(), any()) } returns mockk()

            logStore.writeLastCheckpoint("my-table", """{"version":10,"size":42}""")

            verify { fileClient.uploadWithResponse(any<FileParallelUploadOptions>(), any(), any()) }
        }
    }

    private fun mockResponse(statusCode: Int): com.azure.core.http.HttpResponse {
        val response = mockk<com.azure.core.http.HttpResponse>(relaxed = true)
        every { response.statusCode } returns statusCode
        return response
    }

    private fun createPathItem(name: String): PathItem {
        val item = mockk<PathItem>()
        every { item.name } returns name
        every { item.isDirectory } returns false
        return item
    }

    private fun mockListPaths(
        items: List<PathItem>,
        optionsSlot: io.mockk.CapturingSlot<ListPathsOptions>? = null
    ) {
        val pagedIterable = mockk<PagedIterable<PathItem>>()
        every { pagedIterable.iterator() } returns items.toMutableList().iterator()

        if (optionsSlot != null) {
            every {
                fileSystemClient.listPaths(capture(optionsSlot), any<Duration>())
            } returns pagedIterable
        } else {
            every {
                fileSystemClient.listPaths(any<ListPathsOptions>(), any<Duration>())
            } returns pagedIterable
        }
    }
}

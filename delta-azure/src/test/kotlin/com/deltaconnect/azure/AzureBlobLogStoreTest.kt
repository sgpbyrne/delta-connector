package com.deltaconnect.azure

import com.azure.core.http.rest.PagedIterable
import com.azure.storage.blob.BlobClient
import com.azure.storage.blob.BlobContainerClient
import com.azure.storage.blob.models.BlobItem
import com.azure.storage.blob.models.BlobProperties
import com.azure.storage.blob.models.BlobStorageException
import com.azure.storage.blob.models.ListBlobsOptions
import com.azure.storage.blob.options.BlobParallelUploadOptions
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

class AzureBlobLogStoreTest {

    private val containerClient = mockk<BlobContainerClient>()
    private lateinit var logStore: AzureBlobLogStore

    @BeforeEach
    fun setUp() {
        logStore = AzureBlobLogStore(containerClient)
    }

    @Nested
    inner class WriteCommit {

        @Test
        fun `writes commit with If-None-Match condition`() {
            val blobClient = mockk<BlobClient>()
            every { containerClient.getBlobClient(any()) } returns blobClient
            val optionsSlot = slot<BlobParallelUploadOptions>()
            every { blobClient.uploadWithResponse(capture(optionsSlot), any(), any()) } returns mockk()

            val content = """{"add":{"path":"data.parquet"}}""".toByteArray()
            logStore.writeCommit("my-table", 0, content)

            verify { containerClient.getBlobClient("my-table/_delta_log/00000000000000000000.json") }
            optionsSlot.captured.requestConditions.ifNoneMatch shouldBe "*"
        }

        @Test
        fun `writes commit at version 5 with correct file path`() {
            val blobClient = mockk<BlobClient>()
            every { containerClient.getBlobClient(any()) } returns blobClient
            every { blobClient.uploadWithResponse(any<BlobParallelUploadOptions>(), any(), any()) } returns mockk()

            logStore.writeCommit("my-table", 5, "data".toByteArray())

            verify { containerClient.getBlobClient("my-table/_delta_log/00000000000000000005.json") }
        }

        @Test
        fun `throws CommitConflictException on 409 status`() {
            val blobClient = mockk<BlobClient>()
            every { containerClient.getBlobClient(any()) } returns blobClient
            every { blobClient.uploadWithResponse(any<BlobParallelUploadOptions>(), any(), any()) } throws
                BlobStorageException("Conflict", mockResponse(409), null)

            shouldThrow<CommitConflictException> {
                logStore.writeCommit("my-table", 0, "data".toByteArray())
            }
        }

        @Test
        fun `throws CommitConflictException on 412 precondition failed`() {
            val blobClient = mockk<BlobClient>()
            every { containerClient.getBlobClient(any()) } returns blobClient
            every { blobClient.uploadWithResponse(any<BlobParallelUploadOptions>(), any(), any()) } throws
                BlobStorageException("Precondition Failed", mockResponse(412), null)

            shouldThrow<CommitConflictException> {
                logStore.writeCommit("my-table", 0, "data".toByteArray())
            }
        }

        @Test
        fun `rethrows non-conflict exceptions`() {
            val blobClient = mockk<BlobClient>()
            every { containerClient.getBlobClient(any()) } returns blobClient
            every { blobClient.uploadWithResponse(any<BlobParallelUploadOptions>(), any(), any()) } throws
                BlobStorageException("Internal Error", mockResponse(500), null)

            shouldThrow<BlobStorageException> {
                logStore.writeCommit("my-table", 0, "data".toByteArray())
            }
        }
    }

    @Nested
    inner class ReadCommit {

        @Test
        fun `reads commit content`() {
            val blobClient = mockk<BlobClient>()
            every { containerClient.getBlobClient("my-table/_delta_log/00000000000000000005.json") } returns blobClient
            val content = """{"add":{"path":"data.parquet"}}""".toByteArray()
            every { blobClient.downloadStream(any<OutputStream>()) } answers {
                (firstArg<OutputStream>()).write(content)
            }

            val result = logStore.readCommit("my-table", 5)
            result shouldBe content
        }

        @Test
        fun `returns null for non-existent version`() {
            val blobClient = mockk<BlobClient>()
            every { containerClient.getBlobClient(any()) } returns blobClient
            every { blobClient.downloadStream(any<OutputStream>()) } throws
                BlobStorageException("Not Found", mockResponse(404), null)

            logStore.readCommit("my-table", 99).shouldBeNull()
        }
    }

    @Nested
    inner class ListCommitVersions {

        @Test
        fun `lists versions sorted ascending`() {
            val blobItems = listOf(
                createBlobItem("my-table/_delta_log/00000000000000000002.json"),
                createBlobItem("my-table/_delta_log/00000000000000000000.json"),
                createBlobItem("my-table/_delta_log/00000000000000000001.json")
            )
            mockListBlobs(blobItems)

            logStore.listCommitVersions("my-table") shouldContainExactly listOf(0L, 1L, 2L)
        }

        @Test
        fun `filters by startVersion`() {
            val blobItems = listOf(
                createBlobItem("my-table/_delta_log/00000000000000000000.json"),
                createBlobItem("my-table/_delta_log/00000000000000000001.json"),
                createBlobItem("my-table/_delta_log/00000000000000000002.json")
            )
            mockListBlobs(blobItems)

            logStore.listCommitVersions("my-table", 2) shouldContainExactly listOf(2L)
        }

        @Test
        fun `ignores checkpoint files`() {
            val blobItems = listOf(
                createBlobItem("my-table/_delta_log/00000000000000000000.json"),
                createBlobItem("my-table/_delta_log/00000000000000000010.checkpoint.parquet"),
                createBlobItem("my-table/_delta_log/00000000000000000001.json")
            )
            mockListBlobs(blobItems)

            logStore.listCommitVersions("my-table") shouldContainExactly listOf(0L, 1L)
        }

        @Test
        fun `returns empty list for non-existent directory`() {
            every { containerClient.listBlobs(any<ListBlobsOptions>(), any<Duration>()) } throws
                BlobStorageException("Not Found", mockResponse(404), null)

            logStore.listCommitVersions("my-table").shouldBeEmpty()
        }

        @Test
        fun `passes correct prefix option`() {
            val optionsSlot = slot<ListBlobsOptions>()
            mockListBlobs(emptyList(), optionsSlot)

            logStore.listCommitVersions("my-table")

            optionsSlot.captured.prefix shouldBe "my-table/_delta_log/"
        }
    }

    @Nested
    inner class CreateDataFile {

        @Test
        fun `returns buffered output stream that uploads on close`() {
            val blobClient = mockk<BlobClient>()
            every { containerClient.getBlobClient("data/part-00000.parquet") } returns blobClient
            every { blobClient.uploadWithResponse(any<BlobParallelUploadOptions>(), any(), any()) } returns mockk()

            val outputStream = logStore.createDataFile("data/part-00000.parquet")
            val content = "parquet-data".toByteArray()
            outputStream.write(content)
            outputStream.close()

            verify { blobClient.uploadWithResponse(any<BlobParallelUploadOptions>(), any(), any()) }
        }

        @Test
        fun `close is idempotent`() {
            val blobClient = mockk<BlobClient>()
            every { containerClient.getBlobClient("data/part-00000.parquet") } returns blobClient
            every { blobClient.uploadWithResponse(any<BlobParallelUploadOptions>(), any(), any()) } returns mockk()

            val outputStream = logStore.createDataFile("data/part-00000.parquet")
            outputStream.write("data".toByteArray())
            outputStream.close()
            outputStream.close()

            verify(exactly = 1) { blobClient.uploadWithResponse(any<BlobParallelUploadOptions>(), any(), any()) }
        }
    }

    @Nested
    inner class ReadDataFileAsInputFile {

        @Test
        fun `returns InputFile with correct file size`() {
            val blobClient = mockk<BlobClient>()
            every { containerClient.getBlobClient("data/part-00000.parquet") } returns blobClient
            val properties = mockk<BlobProperties>()
            every { properties.blobSize } returns 1024L
            every { blobClient.properties } returns properties

            val inputFile = logStore.readDataFileAsInputFile("data/part-00000.parquet")
            inputFile.length shouldBe 1024L
        }
    }

    @Nested
    inner class LastCheckpoint {

        @Test
        fun `reads last checkpoint content`() {
            val blobClient = mockk<BlobClient>()
            every { containerClient.getBlobClient("my-table/_delta_log/_last_checkpoint") } returns blobClient
            val content = """{"version":10,"size":42}"""
            every { blobClient.downloadStream(any<OutputStream>()) } answers {
                (firstArg<OutputStream>()).write(content.toByteArray())
            }

            logStore.readLastCheckpoint("my-table") shouldBe content
        }

        @Test
        fun `returns null when checkpoint does not exist`() {
            val blobClient = mockk<BlobClient>()
            every { containerClient.getBlobClient("my-table/_delta_log/_last_checkpoint") } returns blobClient
            every { blobClient.downloadStream(any<OutputStream>()) } throws
                BlobStorageException("Not Found", mockResponse(404), null)

            logStore.readLastCheckpoint("my-table").shouldBeNull()
        }

        @Test
        fun `writes last checkpoint content`() {
            val blobClient = mockk<BlobClient>()
            every { containerClient.getBlobClient("my-table/_delta_log/_last_checkpoint") } returns blobClient
            every { blobClient.uploadWithResponse(any<BlobParallelUploadOptions>(), any(), any()) } returns mockk()

            logStore.writeLastCheckpoint("my-table", """{"version":10,"size":42}""")

            verify { blobClient.uploadWithResponse(any<BlobParallelUploadOptions>(), any(), any()) }
        }
    }

    private fun mockResponse(statusCode: Int): com.azure.core.http.HttpResponse {
        val response = mockk<com.azure.core.http.HttpResponse>(relaxed = true)
        every { response.statusCode } returns statusCode
        return response
    }

    private fun createBlobItem(name: String): BlobItem {
        val item = mockk<BlobItem>()
        every { item.name } returns name
        return item
    }

    private fun mockListBlobs(
        items: List<BlobItem>,
        optionsSlot: io.mockk.CapturingSlot<ListBlobsOptions>? = null
    ) {
        val pagedIterable = mockk<PagedIterable<BlobItem>>()
        every { pagedIterable.iterator() } returns items.toMutableList().iterator()

        if (optionsSlot != null) {
            every {
                containerClient.listBlobs(capture(optionsSlot), any<Duration>())
            } returns pagedIterable
        } else {
            every {
                containerClient.listBlobs(any<ListBlobsOptions>(), any<Duration>())
            } returns pagedIterable
        }
    }
}

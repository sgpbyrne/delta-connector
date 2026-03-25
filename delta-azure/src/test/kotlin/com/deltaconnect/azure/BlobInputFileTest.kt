package com.deltaconnect.azure

import com.azure.storage.blob.BlobClient
import com.azure.storage.blob.models.BlobRange
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.io.EOFException
import java.io.OutputStream
import java.nio.ByteBuffer

class BlobInputFileTest {
    @Nested
    inner class Length {
        @Test
        fun `returns file size`() {
            val blobClient = mockk<BlobClient>()
            val inputFile = BlobInputFile(blobClient, 1024L)
            inputFile.length shouldBe 1024L
        }
    }

    @Nested
    inner class SeekableStream {
        @Test
        fun `reads single bytes sequentially`() {
            val data = byteArrayOf(0x41, 0x42, 0x43) // A, B, C
            val blobClient = mockBlobClient(data)
            val inputFile = BlobInputFile(blobClient, data.size.toLong(), chunkSize = 1024)

            inputFile.newStream().use { stream ->
                stream.read() shouldBe 0x41
                stream.read() shouldBe 0x42
                stream.read() shouldBe 0x43
                stream.read() shouldBe -1
            }
        }

        @Test
        fun `seeks to position and reads`() {
            val data = "ABCDEFGH".toByteArray()
            val blobClient = mockBlobClient(data)
            val inputFile = BlobInputFile(blobClient, data.size.toLong(), chunkSize = 1024)

            inputFile.newStream().use { stream ->
                stream.seek(4)
                stream.getPos() shouldBe 4L
                stream.read() shouldBe 'E'.code
                stream.read() shouldBe 'F'.code
            }
        }

        @Test
        fun `reads bulk bytes`() {
            val data = "Hello, World!".toByteArray()
            val blobClient = mockBlobClient(data)
            val inputFile = BlobInputFile(blobClient, data.size.toLong(), chunkSize = 1024)

            inputFile.newStream().use { stream ->
                val buf = ByteArray(5)
                val n = stream.read(buf, 0, 5)
                n shouldBe 5
                String(buf) shouldBe "Hello"
            }
        }

        @Test
        fun `readFully reads exact number of bytes`() {
            val data = "ABCDEFGH".toByteArray()
            val blobClient = mockBlobClient(data)
            val inputFile = BlobInputFile(blobClient, data.size.toLong(), chunkSize = 1024)

            inputFile.newStream().use { stream ->
                val buf = ByteArray(4)
                stream.readFully(buf)
                String(buf) shouldBe "ABCD"
            }
        }

        @Test
        fun `readFully throws EOFException when not enough data`() {
            val data = "AB".toByteArray()
            val blobClient = mockBlobClient(data)
            val inputFile = BlobInputFile(blobClient, data.size.toLong(), chunkSize = 1024)

            inputFile.newStream().use { stream ->
                val buf = ByteArray(10)
                shouldThrow<EOFException> {
                    stream.readFully(buf)
                }
            }
        }

        @Test
        fun `reads across chunk boundaries`() {
            val data = "ABCDEFGHIJKLMNOP".toByteArray() // 16 bytes
            val blobClient = mockBlobClient(data)
            val inputFile = BlobInputFile(blobClient, data.size.toLong(), chunkSize = 4)

            inputFile.newStream().use { stream ->
                val buf = ByteArray(8)
                stream.readFully(buf)
                String(buf) shouldBe "ABCDEFGH" // crosses first 4-byte chunk
            }
        }

        @Test
        fun `seek invalidates buffer and fetches new chunk`() {
            val data = "0123456789ABCDEF".toByteArray() // 16 bytes
            val blobClient = mockBlobClient(data)
            val inputFile = BlobInputFile(blobClient, data.size.toLong(), chunkSize = 4)

            inputFile.newStream().use { stream ->
                stream.read() shouldBe '0'.code
                stream.seek(10)
                stream.read() shouldBe 'A'.code
            }
        }

        @Test
        fun `reads into ByteBuffer`() {
            val data = "Hello".toByteArray()
            val blobClient = mockBlobClient(data)
            val inputFile = BlobInputFile(blobClient, data.size.toLong(), chunkSize = 1024)

            inputFile.newStream().use { stream ->
                val buf = ByteBuffer.allocate(5)
                stream.read(buf)
                buf.flip()
                val bytes = ByteArray(5)
                buf.get(bytes)
                String(bytes) shouldBe "Hello"
            }
        }

        @Test
        fun `readFully into ByteBuffer`() {
            val data = "World".toByteArray()
            val blobClient = mockBlobClient(data)
            val inputFile = BlobInputFile(blobClient, data.size.toLong(), chunkSize = 1024)

            inputFile.newStream().use { stream ->
                val buf = ByteBuffer.allocate(5)
                stream.readFully(buf)
                buf.flip()
                val bytes = ByteArray(5)
                buf.get(bytes)
                String(bytes) shouldBe "World"
            }
        }

        @Test
        fun `returns -1 when reading past end`() {
            val data = "A".toByteArray()
            val blobClient = mockBlobClient(data)
            val inputFile = BlobInputFile(blobClient, data.size.toLong(), chunkSize = 1024)

            inputFile.newStream().use { stream ->
                stream.read() shouldBe 'A'.code
                stream.read() shouldBe -1
            }
        }

        @Test
        fun `seek to end position is valid`() {
            val data = "AB".toByteArray()
            val blobClient = mockBlobClient(data)
            val inputFile = BlobInputFile(blobClient, data.size.toLong(), chunkSize = 1024)

            inputFile.newStream().use { stream ->
                stream.seek(2) // seek to end
                stream.getPos() shouldBe 2L
                stream.read() shouldBe -1
            }
        }

        @Test
        fun `seek to invalid position throws`() {
            val data = "AB".toByteArray()
            val blobClient = mockBlobClient(data)
            val inputFile = BlobInputFile(blobClient, data.size.toLong(), chunkSize = 1024)

            inputFile.newStream().use { stream ->
                shouldThrow<IllegalArgumentException> {
                    stream.seek(-1)
                }
                shouldThrow<IllegalArgumentException> {
                    stream.seek(3) // past end
                }
            }
        }
    }

    private fun mockBlobClient(data: ByteArray): BlobClient {
        val blobClient = mockk<BlobClient>()
        val rangeSlot = slot<BlobRange>()

        every {
            blobClient.downloadStreamWithResponse(
                any<OutputStream>(),
                capture(rangeSlot),
                any(),
                any(),
                any(),
                any(),
                any(),
            )
        } answers {
            val outputStream = firstArg<OutputStream>()
            val range = rangeSlot.captured
            val offset = range.offset.toInt()
            val count = range.count.toInt()
            outputStream.write(data, offset, count)
            mockk()
        }

        return blobClient
    }
}

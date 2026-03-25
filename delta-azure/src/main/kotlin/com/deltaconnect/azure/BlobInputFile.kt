package com.deltaconnect.azure

import com.azure.storage.blob.BlobClient
import com.azure.storage.blob.models.BlobRange
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream
import java.io.ByteArrayOutputStream
import java.io.EOFException
import java.nio.ByteBuffer

/**
 * Parquet [InputFile] backed by Azure Blob Storage HTTP range reads.
 *
 * Each call to [newStream] creates a new [BlobSeekableInputStream] that fetches
 * data in configurable chunks (default 4 MB) to reduce HTTP round trips while
 * keeping memory usage bounded.
 *
 * @param blobClient Azure Blob client for the target blob.
 * @param fileSize Total blob size in bytes.
 * @param chunkSize Read-ahead buffer size in bytes (default 4 MB).
 */
internal class BlobInputFile(
    private val blobClient: BlobClient,
    private val fileSize: Long,
    private val chunkSize: Int = DEFAULT_CHUNK_SIZE,
) : InputFile {
    override fun getLength(): Long = fileSize

    override fun newStream(): SeekableInputStream = BlobSeekableInputStream(blobClient, fileSize, chunkSize)

    companion object {
        /** Default read-ahead chunk size: 4 MB. */
        const val DEFAULT_CHUNK_SIZE: Int = 4 * 1024 * 1024
    }
}

/**
 * Seekable input stream that fetches data from Azure Blob Storage via HTTP range reads.
 *
 * Maintains a read-ahead buffer to reduce HTTP round trips. When the current
 * position falls outside the buffered range, a new chunk is fetched.
 */
internal class BlobSeekableInputStream(
    private val blobClient: BlobClient,
    private val fileSize: Long,
    private val chunkSize: Int,
) : SeekableInputStream() {
    private var pos: Long = 0
    private var buffer: ByteArray = EMPTY_BUFFER
    private var bufferStart: Long = -1
    private var bufferLength: Int = 0

    override fun getPos(): Long = pos

    override fun seek(newPos: Long) {
        require(newPos in 0..fileSize) { "Seek position $newPos out of range [0, $fileSize]" }
        pos = newPos
    }

    override fun read(): Int {
        if (pos >= fileSize) return -1
        ensureBuffered(pos, 1)
        val idx = (pos - bufferStart).toInt()
        pos++
        return buffer[idx].toInt() and 0xFF
    }

    override fun read(
        b: ByteArray,
        off: Int,
        len: Int,
    ): Int {
        if (pos >= fileSize) return -1
        val toRead = minOf(len.toLong(), fileSize - pos).toInt()
        var bytesRead = 0
        while (bytesRead < toRead) {
            ensureBuffered(pos, 1)
            val bufferOffset = (pos - bufferStart).toInt()
            val available = bufferLength - bufferOffset
            val chunk = minOf(available, toRead - bytesRead)
            System.arraycopy(buffer, bufferOffset, b, off + bytesRead, chunk)
            pos += chunk
            bytesRead += chunk
        }
        return bytesRead
    }

    override fun readFully(bytes: ByteArray) {
        readFully(bytes, 0, bytes.size)
    }

    override fun readFully(
        bytes: ByteArray,
        start: Int,
        len: Int,
    ) {
        if (pos + len > fileSize) {
            throw EOFException(
                "Reached end of stream at position $pos (needed $len bytes, file size $fileSize)",
            )
        }
        var bytesRead = 0
        while (bytesRead < len) {
            ensureBuffered(pos, 1)
            val bufferOffset = (pos - bufferStart).toInt()
            val available = bufferLength - bufferOffset
            val chunk = minOf(available, len - bytesRead)
            System.arraycopy(buffer, bufferOffset, bytes, start + bytesRead, chunk)
            pos += chunk
            bytesRead += chunk
        }
    }

    override fun read(buf: ByteBuffer): Int {
        if (pos >= fileSize) return -1
        val toRead = minOf(buf.remaining().toLong(), fileSize - pos).toInt()
        var bytesRead = 0
        while (bytesRead < toRead) {
            ensureBuffered(pos, 1)
            val bufferOffset = (pos - bufferStart).toInt()
            val available = bufferLength - bufferOffset
            val chunk = minOf(available, toRead - bytesRead)
            buf.put(buffer, bufferOffset, chunk)
            pos += chunk
            bytesRead += chunk
        }
        return bytesRead
    }

    override fun readFully(buf: ByteBuffer) {
        val len = buf.remaining()
        if (pos + len > fileSize) {
            throw EOFException(
                "Reached end of stream at position $pos (needed $len bytes, file size $fileSize)",
            )
        }
        var bytesRead = 0
        while (bytesRead < len) {
            ensureBuffered(pos, 1)
            val bufferOffset = (pos - bufferStart).toInt()
            val available = bufferLength - bufferOffset
            val chunk = minOf(available, len - bytesRead)
            buf.put(buffer, bufferOffset, chunk)
            pos += chunk
            bytesRead += chunk
        }
    }

    private fun ensureBuffered(
        position: Long,
        minBytes: Int,
    ) {
        if (position >= bufferStart && position + minBytes <= bufferStart + bufferLength) {
            return
        }
        fetchChunk(position)
    }

    private fun fetchChunk(startPosition: Long) {
        val rangeLength = minOf(chunkSize.toLong(), fileSize - startPosition)
        val range = BlobRange(startPosition, rangeLength)
        val output = ByteArrayOutputStream(rangeLength.toInt())
        blobClient.downloadStreamWithResponse(output, range, null, null, false, null, null)
        val fetched = output.toByteArray()
        buffer = fetched
        bufferStart = startPosition
        bufferLength = fetched.size
    }

    companion object {
        private val EMPTY_BUFFER = ByteArray(0)
    }
}

package com.deltaconnect.protocol.storage

import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.file.Path

/**
 * Parquet [InputFile] backed by a local file using [RandomAccessFile].
 *
 * Provides true seekable access without buffering the entire file in JVM heap memory.
 * Each call to [newStream] opens a new [RandomAccessFile] handle; callers must close
 * the returned stream when done.
 *
 * @param path Path to the local file.
 */
class LocalRandomAccessInputFile(private val path: Path) : InputFile {

    private val fileLength: Long = path.toFile().length()

    override fun getLength(): Long = fileLength

    override fun newStream(): SeekableInputStream = LocalRandomAccessSeekableInputStream(path)
}

/**
 * Seekable input stream backed by a [RandomAccessFile].
 */
internal class LocalRandomAccessSeekableInputStream(
    path: Path
) : SeekableInputStream() {

    private val raf: RandomAccessFile = RandomAccessFile(path.toFile(), "r")

    override fun getPos(): Long = raf.filePointer

    override fun seek(newPos: Long) {
        raf.seek(newPos)
    }

    override fun read(): Int = raf.read()

    override fun read(b: ByteArray, off: Int, len: Int): Int = raf.read(b, off, len)

    override fun readFully(bytes: ByteArray) {
        raf.readFully(bytes)
    }

    override fun readFully(bytes: ByteArray, start: Int, len: Int) {
        raf.readFully(bytes, start, len)
    }

    override fun read(buf: ByteBuffer): Int {
        val bytes = ByteArray(buf.remaining())
        val n = raf.read(bytes)
        if (n > 0) {
            buf.put(bytes, 0, n)
        }
        return n
    }

    override fun readFully(buf: ByteBuffer) {
        val bytes = ByteArray(buf.remaining())
        raf.readFully(bytes)
        buf.put(bytes)
    }

    override fun close() {
        raf.close()
    }
}

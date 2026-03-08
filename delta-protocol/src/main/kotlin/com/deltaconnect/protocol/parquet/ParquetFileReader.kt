package com.deltaconnect.protocol.parquet

import com.deltaconnect.protocol.schema.AvroToDeltaConverter
import com.deltaconnect.protocol.schema.DeltaType
import com.deltaconnect.protocol.storage.DeltaLogStore
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream
import org.slf4j.LoggerFactory
import java.io.EOFException
import java.nio.ByteBuffer

/**
 * Reads Avro GenericRecords from a Parquet file via [DeltaLogStore].
 *
 * The reader buffers the entire file into memory (via [DeltaLogStore.readDataFile])
 * to provide the seekable access that Parquet format requires.
 *
 * Supports:
 * - **Column projection**: read only a subset of columns (reduces I/O and memory).
 * - **Schema evolution**: if the file was written with a narrower schema, missing
 *   columns are filled with null in the returned records.
 * - **Streaming iteration**: records are returned via [Iterator]
 *
 * @property logStore Storage backend for reading data files.
 */
class ParquetFileReader(private val logStore: DeltaLogStore) {

    /**
     * Read all records from a Parquet file.
     *
     * @param filePath Path to the Parquet file (relative to store base).
     * @return List of all records in the file.
     */
    fun read(filePath: String): List<GenericRecord> {
        return readIterator(filePath).asSequence().toList()
    }

    /**
     * Read records from a Parquet file with column projection.
     *
     * Only the columns present in [projectionSchema] are materialized.
     *
     * @param filePath Path to the Parquet file.
     * @param projectionSchema Delta schema containing only the columns to read.
     * @return List of records containing only the projected columns.
     */
    fun read(filePath: String, projectionSchema: DeltaType.StructType): List<GenericRecord> {
        return readIterator(filePath, projectionSchema).asSequence().toList()
    }

    /**
     * Return a lazy [CloseableRecordIterator] over the records in a Parquet file.
     *
     * Caller is responsible for closing the iterator when done to release resources.
     *
     * @param filePath Path to the Parquet file.
     * @param projectionSchema Optional projection schema. If null, all columns are read.
     * @return Streaming iterator over records.
     */
    fun readIterator(
        filePath: String,
        projectionSchema: DeltaType.StructType? = null
    ): CloseableRecordIterator {
        val inputFile = loadInputFile(filePath)

        val conf = Configuration(false)
        if (projectionSchema != null) {
            val avroProjection = AvroToDeltaConverter.toAvroSchema(projectionSchema)
            AvroReadSupport.setRequestedProjection(conf, avroProjection)
            AvroReadSupport.setAvroReadSchema(conf, avroProjection)
        }

        val reader = AvroParquetReader.builder<GenericRecord>(inputFile)
            .withConf(conf)
            .build()
        return CloseableRecordIterator(reader)
    }

    private fun loadInputFile(filePath: String): InputFile {
        val inputStream = logStore.readDataFile(filePath)
        val bytes = inputStream.use { it.readAllBytes() }
        logger.debug("Loaded Parquet file into memory: path={}, size={}", filePath, bytes.size)
        return ByteArrayBackedInputFile(bytes)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ParquetFileReader::class.java)
    }
}

/**
 * Iterator over GenericRecords that must be closed to release the underlying Parquet reader.
 *
 * Implements both [Iterator] and [AutoCloseable] for use in `use {}` blocks.
 */
class CloseableRecordIterator(
    private val reader: org.apache.parquet.hadoop.ParquetReader<GenericRecord>
) : Iterator<GenericRecord>, AutoCloseable {

    private var next: GenericRecord? = null
    private var exhausted: Boolean = false

    override fun hasNext(): Boolean {
        if (exhausted) return false
        if (next != null) return true
        next = reader.read()
        if (next == null) {
            exhausted = true
            reader.close()
            return false
        }
        return true
    }

    override fun next(): GenericRecord {
        if (!hasNext()) throw NoSuchElementException("No more records")
        val record = next!!
        next = null
        return record
    }

    override fun close() {
        if (!exhausted) {
            exhausted = true
            reader.close()
        }
    }
}

/**
 * Parquet [InputFile] backed by an in-memory byte array.
 *
 * Provides seekable access required by the Parquet format
 * by buffering the entire file content in memory.
 */
internal class ByteArrayBackedInputFile(private val data: ByteArray) : InputFile {

    override fun getLength(): Long = data.size.toLong()

    override fun newStream(): SeekableInputStream {
        return ByteArraySeekableInputStream(data)
    }
}

/**
 * Seekable input stream backed by an in-memory byte array.
 */
internal class ByteArraySeekableInputStream(
    private val data: ByteArray
) : SeekableInputStream() {

    private var position: Int = 0

    override fun getPos(): Long = position.toLong()

    override fun seek(newPos: Long) {
        if (newPos < 0 || newPos > data.size) {
            throw IllegalArgumentException("Seek position $newPos out of range [0, ${data.size}]")
        }
        position = newPos.toInt()
    }

    override fun read(): Int {
        if (position >= data.size) return -1
        return data[position++].toInt() and 0xFF
    }

    override fun read(b: ByteArray, off: Int, len: Int): Int {
        if (position >= data.size) return -1
        val bytesToRead = minOf(len, data.size - position)
        System.arraycopy(data, position, b, off, bytesToRead)
        position += bytesToRead
        return bytesToRead
    }

    override fun readFully(bytes: ByteArray) {
        readFully(bytes, 0, bytes.size)
    }

    override fun readFully(bytes: ByteArray, start: Int, len: Int) {
        if (position + len > data.size) {
            throw EOFException(
                "Reached end of stream after ${data.size - position} bytes (expected $len)"
            )
        }
        System.arraycopy(data, position, bytes, start, len)
        position += len
    }

    override fun read(buf: ByteBuffer): Int {
        if (position >= data.size) return -1
        val bytesToRead = minOf(buf.remaining(), data.size - position)
        buf.put(data, position, bytesToRead)
        position += bytesToRead
        return bytesToRead
    }

    override fun readFully(buf: ByteBuffer) {
        val len = buf.remaining()
        if (position + len > data.size) {
            throw EOFException(
                "Reached end of stream after ${data.size - position} bytes (expected $len)"
            )
        }
        buf.put(data, position, len)
        position += len
    }
}

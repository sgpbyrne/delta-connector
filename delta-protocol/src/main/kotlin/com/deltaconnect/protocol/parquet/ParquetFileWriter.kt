package com.deltaconnect.protocol.parquet

import com.deltaconnect.protocol.schema.AvroToDeltaConverter
import com.deltaconnect.protocol.schema.DeltaType
import com.deltaconnect.protocol.storage.DeltaLogStore
import org.apache.avro.Schema as AvroSchema
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.OutputFile
import org.apache.parquet.io.PositionOutputStream
import org.slf4j.LoggerFactory
import java.io.OutputStream

/**
 * Writes Avro GenericRecords to a Parquet file via [DeltaLogStore].
 *
 * Uses parquet-avro under the hood. The file is written through [DeltaLogStore.createDataFile]
 *
 * @property logStore Storage backend for creating data files.
 * @property schema Delta struct type describing the record schema.
 * @property compression Parquet compression codec (default: SNAPPY).
 * @property rowGroupSize Target row group size in bytes (default: 128 MB).
 */
class ParquetFileWriter(
    private val logStore: DeltaLogStore,
    private val schema: DeltaType.StructType,
    private val compression: CompressionCodecName = CompressionCodecName.SNAPPY,
    private val rowGroupSize: Long = DEFAULT_ROW_GROUP_SIZE
) {
    private val avroSchema: AvroSchema = AvroToDeltaConverter.toAvroSchema(schema)

    /**
     * Result of writing a Parquet file.
     *
     * @property filePath Path where the file was written.
     * @property fileSize Size of the written file in bytes.
     * @property recordCount Number of records written.
     * @property statsJson Column statistics as a JSON string for [AddFile.stats].
     */
    data class WriteResult(
        val filePath: String,
        val fileSize: Long,
        val recordCount: Long,
        val statsJson: String
    )

    /**
     * Write records to a new Parquet file and return the result with stats.
     *
     * @param filePath Relative path for the data file (e.g. "data/part-00000.parquet").
     * @param records Records to write. Must conform to [schema].
     * @return Write result including file size and column stats.
     */
    fun write(filePath: String, records: List<GenericRecord>): WriteResult {
        val outputStream = logStore.createDataFile(filePath)
        val outputFile = OutputStreamBackedOutputFile(outputStream)

        val writer: ParquetWriter<GenericRecord> = AvroParquetWriter.builder<GenericRecord>(outputFile)
            .withSchema(avroSchema)
            .withCompressionCodec(compression)
            .withRowGroupSize(rowGroupSize)
            .build()

        val statsCollector = StatsCollector(schema)
        var count = 0L

        writer.use { w ->
            for (record in records) {
                w.write(record)
                statsCollector.update(record)
                count++
            }
        }

        val fileSize = outputFile.bytesWritten()
        val statsJson = statsCollector.toJson()

        logger.info(
            "Parquet file written: path={}, records={}, size={}, compression={}",
            filePath, count, fileSize, compression
        )

        return WriteResult(
            filePath = filePath,
            fileSize = fileSize,
            recordCount = count,
            statsJson = statsJson
        )
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ParquetFileWriter::class.java)

        /** Default row group size: 128 MB. */
        const val DEFAULT_ROW_GROUP_SIZE: Long = 128L * 1024 * 1024
    }
}

/**
 * Adapter that allows parquet-avro to write to an arbitrary [OutputStream].
 *
 * Tracks the number of bytes written for use in [AddFile.size].
 */
internal class OutputStreamBackedOutputFile(private val outputStream: OutputStream) : OutputFile {

    private var positionStream: CountingPositionOutputStream? = null

    override fun create(blockSizeHint: Long): PositionOutputStream {
        val stream = CountingPositionOutputStream(outputStream)
        positionStream = stream
        return stream
    }

    override fun createOrOverwrite(blockSizeHint: Long): PositionOutputStream {
        return create(blockSizeHint)
    }

    override fun supportsBlockSize(): Boolean = false

    override fun defaultBlockSize(): Long = 0

    fun bytesWritten(): Long {
        return positionStream?.getPos()
            ?: throw IllegalStateException("File not yet written")
    }
}

/**
 * A [PositionOutputStream] that wraps a regular [OutputStream] and tracks position.
 */
internal class CountingPositionOutputStream(
    private val delegate: OutputStream
) : PositionOutputStream() {

    private var position: Long = 0L

    override fun getPos(): Long = position

    override fun write(b: Int) {
        delegate.write(b)
        position++
    }

    override fun write(b: ByteArray, off: Int, len: Int) {
        delegate.write(b, off, len)
        position += len
    }

    override fun flush() {
        delegate.flush()
    }

    override fun close() {
        delegate.close()
    }
}

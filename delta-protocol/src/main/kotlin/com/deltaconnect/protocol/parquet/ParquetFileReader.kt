package com.deltaconnect.protocol.parquet

import com.deltaconnect.protocol.schema.AvroToDeltaConverter
import com.deltaconnect.protocol.schema.DeltaType
import com.deltaconnect.protocol.storage.DeltaLogStore
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.avro.AvroReadSupport

/**
 * Reads Avro GenericRecords from a Parquet file via [DeltaLogStore].
 *
 * Uses [DeltaLogStore.readDataFileAsInputFile] for seekable access, avoiding
 * full-file buffering on implementations that support it (e.g. local FS uses
 * [java.io.RandomAccessFile], ADLS uses HTTP range reads).
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
        return readIterator(filePath).use { it.asSequence().toList() }
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
        return readIterator(filePath, projectionSchema).use { it.asSequence().toList() }
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
        val inputFile = logStore.readDataFileAsInputFile(filePath)

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

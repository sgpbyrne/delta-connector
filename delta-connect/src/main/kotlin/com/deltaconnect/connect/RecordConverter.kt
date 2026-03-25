package com.deltaconnect.connect

import com.deltaconnect.protocol.merge.MergeOperation
import com.deltaconnect.protocol.merge.SourceRecord
import com.deltaconnect.protocol.schema.DeltaType
import org.apache.kafka.connect.sink.SinkRecord

/**
 * Converts Kafka Connect [SinkRecord]s to Delta Lake [SourceRecord]s.
 *
 * Implementations handle envelope specific parsing (e.g. Debezium full
 * envelope, flattened SMT) and type mapping from the source database.
 *
 */
interface RecordConverter {
    /**
     * Convert a [SinkRecord] to a [SourceRecord] for the Delta merge engine.
     *
     * @param record The Kafka Connect record to convert.
     * @param deleteEnabled Whether DELETE operations should be processed.
     *   When false, records with [MergeOperation.DELETE] should return null.
     * @return The converted record, or null if the record should be skipped
     */
    fun convert(
        record: SinkRecord,
        deleteEnabled: Boolean,
    ): SourceRecord?

    /**
     * Extract the Delta table schema from a [SinkRecord].
     *
     * Used to create the Delta table on first write. The schema is derived
     * from the record's value schema (Connect Schema or Avro).
     *
     * @param record A representative record to extract the schema from.
     * @return The Delta struct type representing the table schema.
     */
    fun extractSchema(record: SinkRecord): DeltaType.StructType
}

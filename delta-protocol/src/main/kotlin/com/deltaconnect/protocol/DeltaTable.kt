package com.deltaconnect.protocol

import com.deltaconnect.protocol.actions.Action
import com.deltaconnect.protocol.actions.CommitInfo
import com.deltaconnect.protocol.actions.Format
import com.deltaconnect.protocol.actions.MetaData
import com.deltaconnect.protocol.actions.Protocol
import com.deltaconnect.protocol.actions.RemoveFile
import com.deltaconnect.protocol.log.TransactionLogReader
import com.deltaconnect.protocol.log.TransactionLogWriter
import com.deltaconnect.protocol.schema.DeltaSchema
import com.deltaconnect.protocol.schema.DeltaType
import com.deltaconnect.protocol.storage.DeltaLogStore
import org.slf4j.LoggerFactory
import java.util.UUID

/**
 * Entry point for interacting with a Delta table.
 *
 * Provides factory methods to open or create tables, read snapshots,
 * and start transactions for writing data.
 *
 * @property logStore Storage backend for transaction log and data files.
 * @property tablePath Root path of the Delta table.
 */
class DeltaTable private constructor(
    val logStore: DeltaLogStore,
    val tablePath: String
) {

    internal val logReader: TransactionLogReader = TransactionLogReader(logStore)
    internal val logWriter: TransactionLogWriter = TransactionLogWriter(logStore)

    /**
     * Read the current table state by replaying the transaction log.
     *
     * @return The current [DeltaSnapshot] or [DeltaSnapshot.empty] if no commits exist.
     */
    fun snapshot(): DeltaSnapshot = logReader.getSnapshot(tablePath)

    /**
     * Start a new transaction against the current table state.
     *
     * The transaction captures the current snapshot and allows accumulating
     * actions before committing with optimistic concurrency control.
     *
     * @return A new [DeltaTransaction] based on the current snapshot.
     */
    fun startTransaction(): DeltaTransaction {
        val currentSnapshot = snapshot()
        logger.debug("Transaction started: table={}, readVersion={}", tablePath, currentSnapshot.version)
        return DeltaTransaction(
            logStore = logStore,
            tablePath = tablePath,
            logReader = logReader,
            logWriter = logWriter,
            readSnapshot = currentSnapshot
        )
    }

    companion object {

        private val logger = LoggerFactory.getLogger(DeltaTable::class.java)

        /**
         * Open a Delta table at the given path.
         *
         * The table may or may not have existing commits. Use [snapshot] to
         * read the current state or [startTransaction] to begin writing.
         *
         * @param logStore Storage backend.
         * @param tablePath Root path of the Delta table.
         * @return A [DeltaTable] handle for the given path.
         */
        fun forPath(logStore: DeltaLogStore, tablePath: String): DeltaTable {
            logger.debug("Opening table: path={}", tablePath)
            return DeltaTable(logStore, tablePath)
        }

        /**
         * Create a new Delta table or replace an existing one's metadata.
         *
         * For a new table (no commits): writes version 0 with [Protocol],
         * [MetaData] and [CommitInfo] ("CREATE TABLE").
         *
         * For an existing table: writes the next version with updated [Protocol]
         * and [MetaData], removes all existing data files and records
         * [CommitInfo] ("CREATE OR REPLACE TABLE").
         *
         * @param logStore Storage backend.
         * @param tablePath Root path of the Delta table.
         * @param schema Table schema.
         * @param partitionColumns Columns used for Hive-style partitioning.
         * @param configuration Table properties.
         * @return A [DeltaTable] handle for the created/replaced table.
         */
        fun createOrReplace(
            logStore: DeltaLogStore,
            tablePath: String,
            schema: DeltaType.StructType,
            partitionColumns: List<String> = emptyList(),
            configuration: Map<String, String> = emptyMap(),
            engineInfo: String? = null
        ): DeltaTable {
            val table = DeltaTable(logStore, tablePath)
            val currentSnapshot = table.snapshot()

            val isNew = currentSnapshot.version < 0
            val now = System.currentTimeMillis()

            val protocol = Protocol(minReaderVersion = 1, minWriterVersion = 2)
            val metaData = MetaData(
                id = UUID.randomUUID().toString(),
                format = Format(),
                schemaString = DeltaSchema.toJson(schema),
                partitionColumns = partitionColumns,
                configuration = configuration,
                createdTime = now
            )

            val actions = mutableListOf<Action>(protocol, metaData)

            if (!isNew) {
                currentSnapshot.activeFiles.forEach { addFile ->
                    actions.add(
                        RemoveFile(
                            path = addFile.path,
                            deletionTimestamp = now,
                            dataChange = true
                        )
                    )
                }
            }

            actions.add(
                CommitInfo(
                    timestamp = now,
                    operation = if (isNew) "CREATE TABLE" else "CREATE OR REPLACE TABLE",
                    engineInfo = engineInfo,
                    isBlindAppend = isNew
                )
            )

            val targetVersion = currentSnapshot.version + 1
            table.logWriter.commit(tablePath, targetVersion, actions)

            logger.info(
                "Table {}: path={}, version={}",
                if (isNew) "created" else "replaced",
                tablePath,
                targetVersion
            )

            return table
        }
    }
}

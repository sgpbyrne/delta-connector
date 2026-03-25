package com.deltaconnect.protocol.log

import com.deltaconnect.protocol.actions.Action
import com.deltaconnect.protocol.actions.ActionSerializer
import com.deltaconnect.protocol.actions.AddFile
import com.deltaconnect.protocol.actions.CommitInfo
import com.deltaconnect.protocol.actions.Format
import com.deltaconnect.protocol.actions.MetaData
import com.deltaconnect.protocol.actions.Protocol
import com.deltaconnect.protocol.actions.RemoveFile
import com.deltaconnect.protocol.actions.SetTransaction
import com.deltaconnect.protocol.storage.DeltaLogStore

/**
 * Test helper factories for writing Delta transaction log commits.
 *
 * Provides factory methods for common action configurations and
 * a convenience method for writing commits to a [DeltaLogStore].
 */
object LogTestFixtures {
    /** Write a commit containing the given actions at the specified version. */
    fun writeCommit(
        logStore: DeltaLogStore,
        tablePath: String,
        version: Long,
        actions: List<Action>,
    ) {
        val content = ActionSerializer.serializeActions(actions)
        logStore.writeCommit(tablePath, version, content.toByteArray(Charsets.UTF_8))
    }

    /** Create a standard Protocol action. */
    fun protocol(
        minReader: Int = 1,
        minWriter: Int = 2,
    ): Protocol = Protocol(minReaderVersion = minReader, minWriterVersion = minWriter)

    /** Create a MetaData action with a simple two-column schema (id: integer, value: string). */
    fun metaData(
        id: String = "test-table-id",
        schemaString: String = SIMPLE_SCHEMA,
        partitionColumns: List<String> = emptyList(),
        configuration: Map<String, String> = emptyMap(),
    ): MetaData =
        MetaData(
            id = id,
            format = Format(),
            schemaString = schemaString,
            partitionColumns = partitionColumns,
            configuration = configuration,
            createdTime = System.currentTimeMillis(),
        )

    /** Create an AddFile action. */
    fun addFile(
        path: String,
        size: Long = 1024L,
        modificationTime: Long = System.currentTimeMillis(),
        dataChange: Boolean = true,
        partitionValues: Map<String, String> = emptyMap(),
        stats: String? = null,
    ): AddFile =
        AddFile(
            path = path,
            partitionValues = partitionValues,
            size = size,
            modificationTime = modificationTime,
            dataChange = dataChange,
            stats = stats,
        )

    /** Create a RemoveFile action. */
    fun removeFile(
        path: String,
        deletionTimestamp: Long = System.currentTimeMillis(),
        dataChange: Boolean = true,
    ): RemoveFile =
        RemoveFile(
            path = path,
            deletionTimestamp = deletionTimestamp,
            dataChange = dataChange,
        )

    /** Create a CommitInfo action. */
    fun commitInfo(
        operation: String = "WRITE",
        timestamp: Long = System.currentTimeMillis(),
        engineInfo: String = "delta-cdc-connector/test",
        isBlindAppend: Boolean? = null,
    ): CommitInfo =
        CommitInfo(
            timestamp = timestamp,
            operation = operation,
            engineInfo = engineInfo,
            isBlindAppend = isBlindAppend,
        )

    /** Create a SetTransaction action. */
    fun setTransaction(
        appId: String,
        version: Long,
        lastUpdated: Long = System.currentTimeMillis(),
    ): SetTransaction =
        SetTransaction(
            appId = appId,
            version = version,
            lastUpdated = lastUpdated,
        )

    /**
     * Write a typical initial commit (version 0) with protocol, metadata,
     * and optionally some data files.
     */
    fun writeInitialCommit(
        logStore: DeltaLogStore,
        tablePath: String,
        dataFiles: List<AddFile> = emptyList(),
    ) {
        val actions =
            mutableListOf<Action>(
                protocol(),
                metaData(),
            )
        actions.addAll(dataFiles)
        actions.add(commitInfo(operation = "CREATE TABLE", isBlindAppend = true))
        writeCommit(logStore, tablePath, 0L, actions)
    }

    /** A simple two-column Delta schema: {id: integer (non-null), value: string (nullable)}. */
    const val SIMPLE_SCHEMA: String =
        """{"type":"struct","fields":[""" +
            """{"name":"id","type":"integer","nullable":false,"metadata":{}},""" +
            """{"name":"value","type":"string","nullable":true,"metadata":{}}]}"""
}

package com.deltaconnect.protocol

import com.deltaconnect.protocol.actions.AddFile
import com.deltaconnect.protocol.actions.CommitInfo
import com.deltaconnect.protocol.actions.MetaData
import com.deltaconnect.protocol.actions.Protocol
import com.deltaconnect.protocol.actions.SetTransaction

/**
 * Immutable point-in-time view of a Delta table's state at a given version.
 *
 * Constructed by replaying the transaction log from version 0 (or from a
 * checkpoint) to the latest commit. [AddFile] and [RemoveFile][com.deltaconnect.protocol.actions.RemoveFile]
 * actions are reconciled by path to produce the set of [activeFiles].
 *
 * @property version The commit version this snapshot represents. -1 for an empty table.
 * @property activeFiles All data files currently part of the table (after add/remove reconciliation).
 * @property metaData The latest table metadata (schema, partitioning, configuration).
 * @property protocol The latest protocol version requirements.
 * @property transactions Latest [SetTransaction] per appId, keyed by appId.
 * @property commitInfo [CommitInfo] from the snapshot version's commit, if present.
 */
data class DeltaSnapshot(
    val version: Long,
    val activeFiles: Set<AddFile>,
    val metaData: MetaData?,
    val protocol: Protocol?,
    val transactions: Map<String, SetTransaction>,
    val commitInfo: CommitInfo?,
) {
    companion object {
        /** Snapshot representing an empty table with no commits. */
        fun empty(): DeltaSnapshot =
            DeltaSnapshot(
                version = -1L,
                activeFiles = emptySet(),
                metaData = null,
                protocol = null,
                transactions = emptyMap(),
                commitInfo = null,
            )
    }
}

package com.deltaconnect.protocol.log

import com.deltaconnect.protocol.actions.Action
import com.deltaconnect.protocol.actions.ActionSerializer
import com.deltaconnect.protocol.storage.DeltaLogStore
import org.slf4j.LoggerFactory

/**
 * Writes commit files to the Delta transaction log.
 *
 * This is the low-level atomic write. Each call to [commit] writes
 * a single commit file containing the serialized actions. Conflict detection
 * is delegated to [DeltaLogStore.writeCommit], which throws
 * [com.deltaconnect.protocol.storage.CommitConflictException] if the version
 * already exists.
 *
 */
class TransactionLogWriter(
    private val logStore: DeltaLogStore,
) {
    private val logger = LoggerFactory.getLogger(TransactionLogWriter::class.java)

    /**
     * Write a commit file containing the given actions at the specified version.
     *
     * @param tablePath Root path of the Delta table.
     * @param version Commit version number.
     * @param actions List of actions to include in the commit.
     * @throws com.deltaconnect.protocol.storage.CommitConflictException if the version already exists.
     * @throws IllegalArgumentException if [actions] is empty.
     */
    fun commit(
        tablePath: String,
        version: Long,
        actions: List<Action>,
    ) {
        require(actions.isNotEmpty()) { "Cannot write an empty commit" }

        val content = ActionSerializer.serializeActions(actions)
        logStore.writeCommit(tablePath, version, content.toByteArray(Charsets.UTF_8))

        logger.debug(
            "Commit written: table={}, version={}, actionCount={}",
            tablePath,
            version,
            actions.size,
        )
    }
}

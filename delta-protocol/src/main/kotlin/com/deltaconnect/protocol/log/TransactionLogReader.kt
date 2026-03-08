package com.deltaconnect.protocol.log

import com.deltaconnect.protocol.DeltaSnapshot
import com.deltaconnect.protocol.actions.ActionSerializer
import com.deltaconnect.protocol.actions.AddFile
import com.deltaconnect.protocol.actions.CommitInfo
import com.deltaconnect.protocol.actions.MetaData
import com.deltaconnect.protocol.actions.Protocol
import com.deltaconnect.protocol.actions.RemoveFile
import com.deltaconnect.protocol.actions.SetTransaction
import com.deltaconnect.protocol.storage.DeltaLogStore
import org.slf4j.LoggerFactory

/**
 * Reads the Delta transaction log and reconstructs a [DeltaSnapshot].
 *
 * The snapshot reconstruction algorithm:
 * 1. Read `_last_checkpoint` to find the latest checkpoint version (if any).
 * 2. Read JSON commit files from start version to latest.
 * 3. Reconcile: [RemoveFile] cancels the matching [AddFile] by path.
 * 4. Produce a [DeltaSnapshot] with the reconciled active files and latest metadata.
 *
 */
class TransactionLogReader(private val logStore: DeltaLogStore) {

    private val logger = LoggerFactory.getLogger(TransactionLogReader::class.java)

    /**
     * Reconstruct the current table state by replaying the transaction log.
     *
     * @param tablePath Root path of the Delta table.
     * @return The current [DeltaSnapshot], or [DeltaSnapshot.empty] if no commits exist.
     */
    fun getSnapshot(tablePath: String): DeltaSnapshot {
        val checkpointInfo = readCheckpointInfo(tablePath)
        if (checkpointInfo != null) {
            logger.debug(
                "Checkpoint found but loading deferred: table={}, checkpointVersion={}",
                tablePath, checkpointInfo.version
            )
        }

        val startVersion = 0L
        val versions = logStore.listCommitVersions(tablePath, startVersion)

        if (versions.isEmpty()) {
            logger.debug("No commits found for table={}", tablePath)
            return DeltaSnapshot.empty()
        }

        val activeFiles = mutableMapOf<String, AddFile>()
        var metaData: MetaData? = null
        var protocol: Protocol? = null
        val transactions = mutableMapOf<String, SetTransaction>()
        var commitInfo: CommitInfo? = null
        var latestVersion = -1L

        for (version in versions) {
            val content = logStore.readCommit(tablePath, version)
                ?: continue

            val actions = ActionSerializer.deserializeActions(String(content, Charsets.UTF_8))
            for (action in actions) {
                when (action) {
                    is AddFile -> activeFiles[action.path] = action
                    is RemoveFile -> activeFiles.remove(action.path)
                    is MetaData -> metaData = action
                    is Protocol -> protocol = action
                    is SetTransaction -> transactions[action.appId] = action
                    is CommitInfo -> commitInfo = action
                }
            }
            latestVersion = version
        }

        logger.debug(
            "Snapshot reconstructed: table={}, version={}, activeFiles={}, hasMetaData={}, hasProtocol={}",
            tablePath, latestVersion, activeFiles.size, metaData != null, protocol != null
        )

        return DeltaSnapshot(
            version = latestVersion,
            activeFiles = activeFiles.values.toSet(),
            metaData = metaData,
            protocol = protocol,
            transactions = transactions.toMap(),
            commitInfo = commitInfo
        )
    }

    /**
     * Get the latest commit version for the table without reading commit content.
     *
     * @param tablePath Root path of the Delta table.
     * @return The latest version number, or -1 if no commits exist.
     */
    fun getLatestVersion(tablePath: String): Long {
        val versions = logStore.listCommitVersions(tablePath)
        return if (versions.isEmpty()) -1L else versions.last()
    }

    /**
     * Read and parse the `_last_checkpoint` file, if it exists.
     *
     * @param tablePath Root path of the Delta table.
     * @return Parsed checkpoint info, or null if no checkpoint file exists or parsing fails.
     */
    internal fun readCheckpointInfo(tablePath: String): LastCheckpointInfo? {
        val content = logStore.readLastCheckpoint(tablePath) ?: return null
        return try {
            val info = LastCheckpointInfo.fromJson(content)
            logger.debug("Found checkpoint: table={}, version={}", tablePath, info.version)
            info
        } catch (e: LastCheckpointParseException) {
            logger.warn("Corrupt _last_checkpoint for table={}, falling back to full replay", tablePath, e)
            null
        }
    }
}

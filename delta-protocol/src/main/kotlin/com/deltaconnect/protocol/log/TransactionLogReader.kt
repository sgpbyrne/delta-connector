package com.deltaconnect.protocol.log

import com.deltaconnect.protocol.DeltaSnapshot
import com.deltaconnect.protocol.actions.Action
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
 * 2. If a checkpoint exists, load its Parquet file as the base state.
 * 3. Replay JSON commit files from the checkpoint version + 1 (or 0 if no checkpoint).
 * 4. Reconcile: [RemoveFile] cancels the matching [AddFile] by path.
 * 5. Produce a [DeltaSnapshot] with the reconciled active files and latest metadata.
 */
class TransactionLogReader(
    private val logStore: DeltaLogStore,
) {
    private val logger = LoggerFactory.getLogger(TransactionLogReader::class.java)
    private val checkpointWriter = CheckpointWriter(logStore)

    /**
     * Reconstruct the current table state by replaying the transaction log.
     *
     * If a checkpoint exists, the reader loads it as the base state and only
     * replays JSON commits after the checkpoint version. Falls back to full
     * replay from version 0 if the checkpoint cannot be read.
     *
     * @param tablePath Root path of the Delta table.
     * @return The current [DeltaSnapshot], or [DeltaSnapshot.empty] if no commits exist.
     */
    fun getSnapshot(tablePath: String): DeltaSnapshot {
        val state = SnapshotState()

        val checkpointInfo = readCheckpointInfo(tablePath)
        val startVersion =
            if (checkpointInfo != null) {
                try {
                    val checkpointActions = checkpointWriter.readCheckpoint(tablePath, checkpointInfo.version)
                    for (action in checkpointActions) {
                        state.applyAction(action)
                    }
                    state.latestVersion = checkpointInfo.version
                    logger.debug(
                        "Checkpoint loaded: table={}, version={}, actions={}",
                        tablePath,
                        checkpointInfo.version,
                        checkpointActions.size,
                    )
                    checkpointInfo.version + 1
                } catch (
                    @Suppress("TooGenericExceptionCaught") e: Exception,
                ) {
                    // Best-effort checkpoint read — fall back to full log replay on any failure
                    logger.warn(
                        "Failed to read checkpoint at version {}: table={}, falling back to full replay",
                        checkpointInfo.version,
                        tablePath,
                        e,
                    )
                    0L
                }
            } else {
                0L
            }

        val versions = logStore.listCommitVersions(tablePath, startVersion)

        if (versions.isEmpty() && state.latestVersion < 0) {
            logger.debug("No commits found for table={}", tablePath)
            return DeltaSnapshot.empty()
        }

        for (version in versions) {
            val content =
                logStore.readCommit(tablePath, version)
                    ?: continue

            val actions = ActionSerializer.deserializeActions(String(content, Charsets.UTF_8))
            for (action in actions) {
                state.applyAction(action)
            }
            state.latestVersion = version
        }

        logger.debug(
            "Snapshot reconstructed: table={}, version={}, activeFiles={}, hasMetaData={}, hasProtocol={}",
            tablePath,
            state.latestVersion,
            state.activeFiles.size,
            state.metaData != null,
            state.protocol != null,
        )

        return state.toSnapshot()
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

    private class SnapshotState {
        val activeFiles = mutableMapOf<String, AddFile>()
        var metaData: MetaData? = null
        var protocol: Protocol? = null
        val transactions = mutableMapOf<String, SetTransaction>()
        var commitInfo: CommitInfo? = null
        var latestVersion = -1L

        fun applyAction(action: Action) {
            when (action) {
                is AddFile -> activeFiles[action.path] = action
                is RemoveFile -> activeFiles.remove(action.path)
                is MetaData -> metaData = action
                is Protocol -> protocol = action
                is SetTransaction -> transactions[action.appId] = action
                is CommitInfo -> commitInfo = action
            }
        }

        fun toSnapshot(): DeltaSnapshot =
            DeltaSnapshot(
                version = latestVersion,
                activeFiles = activeFiles.values.toSet(),
                metaData = metaData,
                protocol = protocol,
                transactions = transactions.toMap(),
                commitInfo = commitInfo,
            )
    }
}

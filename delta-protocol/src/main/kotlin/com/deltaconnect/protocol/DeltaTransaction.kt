package com.deltaconnect.protocol

import com.deltaconnect.protocol.actions.Action
import com.deltaconnect.protocol.actions.ActionSerializer
import com.deltaconnect.protocol.actions.AddFile
import com.deltaconnect.protocol.actions.CommitInfo
import com.deltaconnect.protocol.actions.RemoveFile
import com.deltaconnect.protocol.actions.SetTransaction
import com.deltaconnect.protocol.log.CheckpointWriter
import com.deltaconnect.protocol.log.TransactionLogReader
import com.deltaconnect.protocol.log.TransactionLogWriter
import com.deltaconnect.protocol.storage.CommitConflictException
import com.deltaconnect.protocol.storage.DeltaLogStore
import org.slf4j.LoggerFactory

/**
 * Accumulates actions and commits them atomically with optimistic concurrency.
 *
 * On commit conflict the transaction re-reads the log, checks if the conflict
 * is resolvable and retries up to [DEFAULT_MAX_RETRIES] times.
 *
 * Blind appends (commits with only [AddFile] actions and no [RemoveFile]) are always
 * safe to retry because they don't depend on existing file state.
 *
 * Non-blind commits (containing [RemoveFile]) are only safe to retry if the
 * intermediate commits did not touch the same file paths.
 */
class DeltaTransaction internal constructor(
    private val logStore: DeltaLogStore,
    private val tablePath: String,
    private val logReader: TransactionLogReader,
    private val logWriter: TransactionLogWriter,
    val readSnapshot: DeltaSnapshot
) {

    private val actions = mutableListOf<Action>()

    /**
     * Add a single action to this transaction.
     */
    fun addAction(action: Action) {
        actions.add(action)
    }

    /**
     * Add multiple actions to this transaction.
     */
    fun addActions(actions: List<Action>) {
        this.actions.addAll(actions)
    }

    /**
     * Commit the accumulated actions to the transaction log.
     *
     * A [CommitInfo] action is automatically appended with the given operation
     * metadata. The commit uses optimistic concurrency: on conflict, it re-reads
     * the log and retries if the conflicting commits touched disjoint files.
     *
     * @param operation Operation name for [CommitInfo] (e.g., "WRITE", "MERGE").
     * @param operationParameters Additional operation context.
     * @param engineInfo Engine identifier for [CommitInfo].
     * @param maxRetries Maximum number of conflict retries.
     * @return The committed version number.
     * @throws CommitConflictException if the conflict cannot be resolved after [maxRetries].
     * @throws IllegalStateException if no actions have been added.
     */
    fun commit(
        operation: String,
        operationParameters: Map<String, String> = emptyMap(),
        engineInfo: String? = null,
        maxRetries: Int = DEFAULT_MAX_RETRIES
    ): Long {
        check(actions.isNotEmpty()) { "Cannot commit an empty transaction" }

        val isBlindAppend = actions.none { it is RemoveFile } &&
            actions.filterIsInstance<AddFile>().all { it.dataChange }

        val commitInfo = CommitInfo(
            timestamp = System.currentTimeMillis(),
            operation = operation,
            operationParameters = operationParameters,
            engineInfo = engineInfo,
            isBlindAppend = isBlindAppend
        )

        val allActions = actions.toList() + commitInfo

        var baseVersion = readSnapshot.version
        var attempt = 0

        while (true) {
            val targetVersion = baseVersion + 1
            try {
                logWriter.commit(tablePath, targetVersion, allActions)
                logger.info(
                    "Transaction committed: table={}, version={}, operation={}, actions={}",
                    tablePath, targetVersion, operation, allActions.size
                )
                tryWriteCheckpoint(targetVersion)
                return targetVersion
            } catch (e: CommitConflictException) {
                attempt++
                if (attempt > maxRetries) {
                    logger.warn(
                        "Transaction failed after {} retries: table={}, operation={}",
                        maxRetries, tablePath, operation
                    )
                    throw e
                }

                logger.debug(
                    "Commit conflict at version {}, attempt {}/{}: table={}",
                    targetVersion, attempt, maxRetries, tablePath
                )

                val latestVersion = logReader.getLatestVersion(tablePath)

                if (!isBlindAppend) {
                    val ourRemovePaths = actions.filterIsInstance<RemoveFile>()
                        .map { it.path }
                        .toSet()
                    if (hasConflictingPaths(baseVersion, latestVersion, ourRemovePaths)) {
                        logger.warn(
                            "Unresolvable conflict — overlapping file paths: table={}, version={}",
                            tablePath, targetVersion
                        )
                        throw e
                    }
                }

                baseVersion = latestVersion
                logger.debug(
                    "Conflict resolved, rebasing to version {}: table={}",
                    baseVersion, tablePath
                )
            }
        }
    }

    private fun hasConflictingPaths(
        fromVersion: Long,
        toVersion: Long,
        ourRemovePaths: Set<String>
    ): Boolean {
        if (ourRemovePaths.isEmpty()) return false

        for (version in (fromVersion + 1)..toVersion) {
            val content = logStore.readCommit(tablePath, version) ?: continue
            val intermediateActions = ActionSerializer.deserializeActions(String(content, Charsets.UTF_8))

            for (action in intermediateActions) {
                val conflictingPath = when (action) {
                    is AddFile -> action.path
                    is RemoveFile -> action.path
                    else -> null
                }
                if (conflictingPath != null && conflictingPath in ourRemovePaths) {
                    logger.debug(
                        "Conflicting path found: path={}, intermediateVersion={}",
                        conflictingPath, version
                    )
                    return true
                }
            }
        }
        return false
    }

    private fun tryWriteCheckpoint(committedVersion: Long) {
        if (committedVersion <= 0 ||
            committedVersion % CheckpointWriter.DEFAULT_CHECKPOINT_INTERVAL != 0L
        ) {
            return
        }
        try {
            val checkpointWriter = CheckpointWriter(logStore)
            val snapshot = logReader.getSnapshot(tablePath)
            checkpointWriter.writeCheckpoint(tablePath, snapshot)
        } catch (e: Exception) {
            logger.warn(
                "Failed to write checkpoint at version {}: table={}",
                committedVersion, tablePath, e
            )
        }
    }

    companion object {
        const val DEFAULT_MAX_RETRIES: Int = 5
        private val logger = LoggerFactory.getLogger(DeltaTransaction::class.java)
    }
}

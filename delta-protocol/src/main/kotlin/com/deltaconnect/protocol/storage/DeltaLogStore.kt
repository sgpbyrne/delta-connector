package com.deltaconnect.protocol.storage

import java.io.InputStream
import java.io.OutputStream

/**
 * Abstraction over the storage backend for Delta Lake transaction log and data files.
 *
 * All file I/O in the protocol module goes through this interface.
 * Implementations must provide atomic commit semantics for [writeCommit].
 * If the version file already exists, [CommitConflictException] must be thrown.
 *
 * Implementations:
 * - [LocalFileSystemLogStore] for testing (uses local filesystem)
 * - AdlsGen2LogStore (in delta-azure module) for production
 */
interface DeltaLogStore {

    /**
     * Atomically write a commit file for the given version.
     *
     * @param tablePath Root path of the Delta table.
     * @param version Commit version number.
     * @param content Serialized commit actions (newline-delimited JSON).
     * @throws CommitConflictException if the version file already exists.
     */
    fun writeCommit(tablePath: String, version: Long, content: ByteArray)

    /**
     * Read a commit file for the given version.
     *
     * @return The commit file content, or null if the version does not exist.
     */
    fun readCommit(tablePath: String, version: Long): ByteArray?

    /**
     * List all commit versions at or after [startVersion], sorted ascending.
     */
    fun listCommitVersions(tablePath: String, startVersion: Long = 0): List<Long>

    /**
     * Create a data file and return an output stream for writing.
     *
     * @param filePath Full path to the data file (relative to table root or absolute).
     */
    fun createDataFile(filePath: String): OutputStream

    /**
     * Read a data file and return an input stream.
     *
     * @param filePath Full path to the data file.
     */
    fun readDataFile(filePath: String): InputStream

    /**
     * Read the `_last_checkpoint` file for the given table.
     *
     * @return The file content as a string, or null if it does not exist.
     */
    fun readLastCheckpoint(tablePath: String): String?

    /**
     * Write (overwrite) the `_last_checkpoint` file for the given table.
     */
    fun writeLastCheckpoint(tablePath: String, content: String)
}

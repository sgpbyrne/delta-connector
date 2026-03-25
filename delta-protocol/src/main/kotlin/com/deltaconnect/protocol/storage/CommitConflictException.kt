package com.deltaconnect.protocol.storage

/**
 * Thrown when a commit file for the given version already exists,
 * indicating a concurrent writer has committed the same version.
 */
class CommitConflictException(
    val tablePath: String,
    val version: Long,
) : RuntimeException("Commit conflict: version $version already exists at $tablePath")

package com.deltaconnect.protocol.storage

import com.deltaconnect.protocol.actions.ActionSerializer
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.FileAlreadyExistsException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

/**
 * Local filesystem implementation of [DeltaLogStore] for testing.
 *
 * Uses [Files.createFile] with CREATE_NEW to provide atomic conflict detection
 * on commit writes (the file must not already exist).
 *
 * @param baseDir Root directory for all Delta tables.
 */
class LocalFileSystemLogStore(private val baseDir: Path) : DeltaLogStore {

    override fun writeCommit(tablePath: String, version: Long, content: ByteArray) {
        val logDir = resolveLogDir(tablePath)
        Files.createDirectories(logDir)
        val commitFile = logDir.resolve(ActionSerializer.commitFileName(version))
        try {
            Files.write(commitFile, content, StandardOpenOption.CREATE_NEW)
        } catch (e: FileAlreadyExistsException) {
            throw CommitConflictException(tablePath, version)
        }
    }

    override fun readCommit(tablePath: String, version: Long): ByteArray? {
        val commitFile = resolveLogDir(tablePath).resolve(ActionSerializer.commitFileName(version))
        return if (Files.exists(commitFile)) Files.readAllBytes(commitFile) else null
    }

    override fun listCommitVersions(tablePath: String, startVersion: Long): List<Long> {
        val logDir = resolveLogDir(tablePath)
        if (!Files.exists(logDir)) return emptyList()

        return Files.list(logDir).use { stream ->
            stream
                .map { it.fileName.toString() }
                .filter { it.endsWith(".json") && !it.contains("checkpoint") }
                .map { it.removeSuffix(".json").toLongOrNull() }
                .filter { it != null && it >= startVersion }
                .map { it!! }
                .sorted()
                .toList()
        }
    }

    override fun createDataFile(filePath: String): OutputStream {
        val path = baseDir.resolve(filePath)
        Files.createDirectories(path.parent)
        return Files.newOutputStream(path, StandardOpenOption.CREATE_NEW)
    }

    override fun readDataFile(filePath: String): InputStream {
        val path = baseDir.resolve(filePath)
        return Files.newInputStream(path)
    }

    override fun readLastCheckpoint(tablePath: String): String? {
        val file = resolveLogDir(tablePath).resolve("_last_checkpoint")
        return if (Files.exists(file)) Files.readString(file) else null
    }

    override fun writeLastCheckpoint(tablePath: String, content: String) {
        val logDir = resolveLogDir(tablePath)
        Files.createDirectories(logDir)
        Files.writeString(
            logDir.resolve("_last_checkpoint"),
            content,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING
        )
    }

    private fun resolveLogDir(tablePath: String): Path =
        baseDir.resolve(tablePath).resolve("_delta_log")
}

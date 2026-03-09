package com.deltaconnect.azure

import com.azure.storage.file.datalake.DataLakeFileSystemClient
import com.azure.storage.file.datalake.models.DataLakeRequestConditions
import com.azure.storage.file.datalake.models.DataLakeStorageException
import com.azure.storage.file.datalake.models.ListPathsOptions
import com.azure.storage.file.datalake.options.FileParallelUploadOptions
import com.deltaconnect.protocol.actions.ActionSerializer
import com.deltaconnect.protocol.storage.CommitConflictException
import com.deltaconnect.protocol.storage.DeltaLogStore
import org.apache.parquet.io.InputFile
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.OutputStream

/**
 * Azure ADLS Gen2 implementation of [DeltaLogStore].
 *
 * Uses the Azure Data Lake Storage SDK for all I/O.
 * Atomic commit semantics are provided via conditional creates,
 * leveraging ADLS Gen2's Hierarchical Namespace for atomic file creation.
 *
 * @param fileSystemClient Azure Data Lake filesystem client (container-level).
 */
class AdlsGen2LogStore(
    private val fileSystemClient: DataLakeFileSystemClient
) : DeltaLogStore {

    override fun writeCommit(tablePath: String, version: Long, content: ByteArray) {
        val filePath = commitPath(tablePath, version)
        val fileClient = fileSystemClient.getFileClient(filePath)

        val conditions = DataLakeRequestConditions().setIfNoneMatch("*")
        val options = FileParallelUploadOptions(ByteArrayInputStream(content))
            .setRequestConditions(conditions)

        try {
            fileClient.uploadWithResponse(options, null, null)
            logger.debug("Commit written: table={}, version={}, size={}", tablePath, version, content.size)
        } catch (e: DataLakeStorageException) {
            if (e.statusCode == 409 || e.statusCode == 412) {
                throw CommitConflictException(tablePath, version)
            }
            throw e
        }
    }

    override fun readCommit(tablePath: String, version: Long): ByteArray? {
        val filePath = commitPath(tablePath, version)
        return readFileBytes(filePath)
    }

    override fun listCommitVersions(tablePath: String, startVersion: Long): List<Long> {
        val logDir = "$tablePath/_delta_log/"

        val options = ListPathsOptions().setPath(logDir).setRecursive(false)

        return try {
            fileSystemClient.listPaths(options, null)
                .asSequence()
                .map { pathItem -> pathItem.name.substringAfterLast('/') }
                .filter { name -> name.endsWith(".json") && !name.contains("checkpoint") }
                .mapNotNull { name -> name.removeSuffix(".json").toLongOrNull() }
                .filter { version -> version >= startVersion }
                .sorted()
                .toList()
        } catch (e: DataLakeStorageException) {
            if (e.statusCode == 404) emptyList() else throw e
        }
    }

    override fun createDataFile(filePath: String): OutputStream {
        return AdlsBufferedOutputStream(fileSystemClient, filePath)
    }

    override fun readDataFileAsInputFile(filePath: String): InputFile {
        val fileClient = fileSystemClient.getFileClient(filePath)
        val properties = fileClient.properties
        val fileSize = properties.fileSize
        return AdlsInputFile(fileClient, fileSize)
    }

    override fun readLastCheckpoint(tablePath: String): String? {
        val filePath = "$tablePath/_delta_log/_last_checkpoint"
        val bytes = readFileBytes(filePath) ?: return null
        return String(bytes, Charsets.UTF_8)
    }

    override fun writeLastCheckpoint(tablePath: String, content: String) {
        val filePath = "$tablePath/_delta_log/_last_checkpoint"
        val fileClient = fileSystemClient.getFileClient(filePath)
        val bytes = content.toByteArray(Charsets.UTF_8)
        val options = FileParallelUploadOptions(ByteArrayInputStream(bytes))
        fileClient.uploadWithResponse(options, null, null)
    }

    private fun readFileBytes(filePath: String): ByteArray? {
        val fileClient = fileSystemClient.getFileClient(filePath)
        return try {
            val outputStream = ByteArrayOutputStream()
            fileClient.read(outputStream)
            outputStream.toByteArray()
        } catch (e: DataLakeStorageException) {
            if (e.statusCode == 404) null else throw e
        }
    }

    private fun commitPath(tablePath: String, version: Long): String =
        "$tablePath/_delta_log/${ActionSerializer.commitFileName(version)}"

    companion object {
        private val logger = LoggerFactory.getLogger(AdlsGen2LogStore::class.java)
    }
}

/**
 * Buffered output stream that accumulates bytes in memory and uploads
 * to ADLS Gen2 on [close].
 *
 * This ensures that data files are written atomically (the file is only
 * visible after the full content is uploaded).
 */
internal class AdlsBufferedOutputStream(
    private val fileSystemClient: DataLakeFileSystemClient,
    private val filePath: String
) : OutputStream() {

    private val buffer = ByteArrayOutputStream()
    private var closed = false

    override fun write(b: Int) {
        buffer.write(b)
    }

    override fun write(b: ByteArray, off: Int, len: Int) {
        buffer.write(b, off, len)
    }

    override fun flush() {
        buffer.flush()
    }

    override fun close() {
        if (closed) return
        closed = true

        val bytes = buffer.toByteArray()
        val fileClient = fileSystemClient.getFileClient(filePath)
        val options = FileParallelUploadOptions(ByteArrayInputStream(bytes))
        fileClient.uploadWithResponse(options, null, null)
    }
}

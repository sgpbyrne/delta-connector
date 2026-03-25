package com.deltaconnect.azure

import com.azure.storage.blob.BlobContainerClient
import com.azure.storage.blob.models.BlobRequestConditions
import com.azure.storage.blob.models.BlobStorageException
import com.azure.storage.blob.models.ListBlobsOptions
import com.azure.storage.blob.options.BlobParallelUploadOptions
import com.deltaconnect.protocol.actions.ActionSerializer
import com.deltaconnect.protocol.storage.CommitConflictException
import com.deltaconnect.protocol.storage.DeltaLogStore
import org.apache.parquet.io.InputFile
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.OutputStream

/**
 * Azure Blob Storage implementation of [DeltaLogStore].
 *
 * Atomic commit semantics are provided via conditional creates using
 * `If-None-Match: *`, which returns 409/412 if the blob already exists.
 *
 * @param containerClient Azure Blob container client.
 */
class AzureBlobLogStore(
    private val containerClient: BlobContainerClient,
) : DeltaLogStore {
    override fun writeCommit(
        tablePath: String,
        version: Long,
        content: ByteArray,
    ) {
        val filePath = commitPath(tablePath, version)
        val blobClient = containerClient.getBlobClient(filePath)

        val conditions = BlobRequestConditions().setIfNoneMatch("*")
        val options =
            BlobParallelUploadOptions(ByteArrayInputStream(content))
                .setRequestConditions(conditions)

        try {
            blobClient.uploadWithResponse(options, null, null)
            logger.debug("Commit written: table={}, version={}, size={}", tablePath, version, content.size)
        } catch (e: BlobStorageException) {
            if (e.statusCode == 409 || e.statusCode == 412) {
                throw CommitConflictException(tablePath, version)
            }
            throw e
        }
    }

    override fun readCommit(
        tablePath: String,
        version: Long,
    ): ByteArray? {
        val filePath = commitPath(tablePath, version)
        return readFileBytes(filePath)
    }

    override fun listCommitVersions(
        tablePath: String,
        startVersion: Long,
    ): List<Long> {
        val logDir = "$tablePath/_delta_log/"

        val options = ListBlobsOptions().setPrefix(logDir)

        return try {
            containerClient
                .listBlobs(options, null)
                .asSequence()
                .map { blobItem -> blobItem.name.substringAfterLast('/') }
                .filter { name -> name.endsWith(".json") && !name.contains("checkpoint") }
                .mapNotNull { name -> name.removeSuffix(".json").toLongOrNull() }
                .filter { version -> version >= startVersion }
                .sorted()
                .toList()
        } catch (e: BlobStorageException) {
            if (e.statusCode == 404) emptyList() else throw e
        }
    }

    override fun createDataFile(filePath: String): OutputStream = AzureBlobBufferedOutputStream(
        containerClient,
        filePath,
    )

    override fun readDataFileAsInputFile(filePath: String): InputFile {
        val blobClient = containerClient.getBlobClient(filePath)
        val properties = blobClient.properties
        val fileSize = properties.blobSize
        return BlobInputFile(blobClient, fileSize)
    }

    override fun readLastCheckpoint(tablePath: String): String? {
        val filePath = "$tablePath/_delta_log/_last_checkpoint"
        val bytes = readFileBytes(filePath) ?: return null
        return String(bytes, Charsets.UTF_8)
    }

    override fun writeLastCheckpoint(
        tablePath: String,
        content: String,
    ) {
        val filePath = "$tablePath/_delta_log/_last_checkpoint"
        val blobClient = containerClient.getBlobClient(filePath)
        val bytes = content.toByteArray(Charsets.UTF_8)
        val options = BlobParallelUploadOptions(ByteArrayInputStream(bytes))
        blobClient.uploadWithResponse(options, null, null)
    }

    private fun readFileBytes(filePath: String): ByteArray? {
        val blobClient = containerClient.getBlobClient(filePath)
        return try {
            val outputStream = ByteArrayOutputStream()
            blobClient.downloadStream(outputStream)
            outputStream.toByteArray()
        } catch (e: BlobStorageException) {
            if (e.statusCode == 404) null else throw e
        }
    }

    private fun commitPath(
        tablePath: String,
        version: Long,
    ): String = "$tablePath/_delta_log/${ActionSerializer.commitFileName(version)}"

    companion object {
        private val logger = LoggerFactory.getLogger(AzureBlobLogStore::class.java)
    }
}

/**
 * Buffered output stream that accumulates bytes in memory and uploads
 * to Azure Blob Storage on [close].
 *
 * This ensures that data files are written atomically.
 */
internal class AzureBlobBufferedOutputStream(
    private val containerClient: BlobContainerClient,
    private val filePath: String,
) : OutputStream() {
    private val buffer = ByteArrayOutputStream()
    private var closed = false

    override fun write(b: Int) {
        buffer.write(b)
    }

    override fun write(
        b: ByteArray,
        off: Int,
        len: Int,
    ) {
        buffer.write(b, off, len)
    }

    override fun flush() {
        buffer.flush()
    }

    override fun close() {
        if (closed) return
        closed = true

        val bytes = buffer.toByteArray()
        val blobClient = containerClient.getBlobClient(filePath)
        val options = BlobParallelUploadOptions(ByteArrayInputStream(bytes))
        blobClient.uploadWithResponse(options, null, null)
    }
}

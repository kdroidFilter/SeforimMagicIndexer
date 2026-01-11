package io.github.kdroidfilter.seforim.magicindexer

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.io.File
import java.net.HttpURLConnection
import java.net.URI

/**
 * Handles downloading the lexical database from the latest GitHub release.
 */
object GitHubReleaseDownloader {

    private const val GITHUB_REPO = "kdroidFilter/SeforimMagicIndexer"
    private const val GITHUB_API_URL = "https://api.github.com/repos/$GITHUB_REPO/releases/latest"
    private const val DATABASE_FILENAME = "lexical.db"

    @Serializable
    private data class GitHubRelease(
        @SerialName("tag_name") val tagName: String,
        val assets: List<GitHubAsset>
    )

    @Serializable
    private data class GitHubAsset(
        val name: String,
        @SerialName("browser_download_url") val downloadUrl: String,
        val size: Long
    )

    /**
     * Downloads the lexical.db from the latest GitHub release if it doesn't exist locally.
     * @param outputDbPath The path where the database should be saved
     * @return true if the database exists (either downloaded or already present), false on error
     */
    fun ensureDatabaseFromRelease(outputDbPath: String): Boolean {
        val dbFile = File(outputDbPath)

        if (dbFile.exists()) {
            println("Local database found: $outputDbPath")
            println("Will continue processing from existing database.\n")
            return true
        }

        println("No local database found. Attempting to download from latest GitHub release...")

        return try {
            // Fetch release info from GitHub API
            val json = Json { ignoreUnknownKeys = true }
            val releaseJson = fetchUrl(GITHUB_API_URL)
            val release = json.decodeFromString<GitHubRelease>(releaseJson)

            // Find lexical.db asset
            val dbAsset = release.assets.find { it.name == DATABASE_FILENAME }
            if (dbAsset == null) {
                println("Warning: No $DATABASE_FILENAME found in latest release (${release.tagName})")
                println("Starting with fresh database.\n")
                return true
            }

            println("Found $DATABASE_FILENAME in release ${release.tagName} (${formatSize(dbAsset.size)})")
            println("Downloading from: ${dbAsset.downloadUrl}")

            // Ensure parent directory exists
            dbFile.parentFile?.mkdirs()

            // Download the file
            downloadFile(dbAsset.downloadUrl, dbFile)

            println("Download complete: $outputDbPath")
            println("Will continue processing from downloaded database.\n")
            true
        } catch (e: Exception) {
            System.err.println("Warning: Failed to download database from GitHub: ${e.message}")
            println("Starting with fresh database.\n")
            true // Continue anyway with fresh database
        }
    }

    /**
     * Fetches content from a URL as a string.
     */
    private fun fetchUrl(url: String): String {
        val connection = URI(url).toURL().openConnection() as HttpURLConnection
        connection.setRequestProperty("Accept", "application/vnd.github.v3+json")
        connection.setRequestProperty("User-Agent", "SeforimMagicIndexer")
        connection.connectTimeout = 10000
        connection.readTimeout = 30000

        return connection.inputStream.bufferedReader().use { it.readText() }
    }

    /**
     * Downloads a file from a URL with progress indication.
     */
    private fun downloadFile(url: String, destination: File) {
        val connection = URI(url).toURL().openConnection() as HttpURLConnection
        connection.setRequestProperty("User-Agent", "SeforimMagicIndexer")
        connection.instanceFollowRedirects = true
        connection.connectTimeout = 10000
        connection.readTimeout = 300000 // 5 minutes for large files

        val totalSize = connection.contentLengthLong
        var downloaded = 0L
        var lastProgress = -1

        connection.inputStream.use { input ->
            destination.outputStream().use { output ->
                val buffer = ByteArray(8192)
                var bytesRead: Int

                while (input.read(buffer).also { bytesRead = it } != -1) {
                    output.write(buffer, 0, bytesRead)
                    downloaded += bytesRead

                    if (totalSize > 0) {
                        val progress = ((downloaded * 100) / totalSize).toInt()
                        if (progress != lastProgress && progress % 10 == 0) {
                            println("  Progress: $progress% (${formatSize(downloaded)} / ${formatSize(totalSize)})")
                            lastProgress = progress
                        }
                    }
                }
            }
        }
    }

    /**
     * Formats a file size in human-readable format.
     */
    private fun formatSize(bytes: Long): String {
        return when {
            bytes >= 1_000_000_000 -> String.format("%.2f GB", bytes / 1_000_000_000.0)
            bytes >= 1_000_000 -> String.format("%.2f MB", bytes / 1_000_000.0)
            bytes >= 1_000 -> String.format("%.2f KB", bytes / 1_000.0)
            else -> "$bytes B"
        }
    }
}

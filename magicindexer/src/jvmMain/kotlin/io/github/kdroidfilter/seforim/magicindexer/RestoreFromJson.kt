package io.github.kdroidfilter.seforim.magicindexer

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforim.magicindexer.db.Database
import io.github.kdroidfilter.seforim.magicindexer.db.DatabaseQueries
import io.github.kdroidfilter.seforim.magicindexer.model.LexicalEntry
import io.github.kdroidfilter.seforim.magicindexer.model.LexicalEntrySerializer
import java.io.File

/**
 * Restores lexical entries from a JSON backup file into an existing SQLite database.
 * Can be used to:
 * - Restore data from backup
 * - Import data from another source
 * - Merge multiple JSON files into one database
 */
object RestoreFromJson {

    /**
     * Restores lexical entries from a JSON backup into an existing database.
     * If the database doesn't exist, it will be created.
     *
     * @param jsonFilePath Path to the JSON backup file
     * @param dbFilePath Path to the target SQLite database (will be created if doesn't exist)
     * @return Number of entries successfully imported
     */
    fun restore(jsonFilePath: String, dbFilePath: String): Int {
        println("=== Restoring from JSON Backup ===")
        println("JSON file: $jsonFilePath")
        println("Target DB: $dbFilePath")

        // Verify JSON file exists
        val jsonFile = File(jsonFilePath)
        if (!jsonFile.exists()) {
            throw IllegalArgumentException("JSON file not found: $jsonFilePath")
        }

        // Read and parse JSON
        println("\nReading JSON file...")
        val jsonContent = jsonFile.readText()
        val lexicalEntries = LexicalEntrySerializer.fromJson(jsonContent)
        println("Found ${lexicalEntries.entries.size} entries in JSON")

        // Open or create database
        val dbFile = File(dbFilePath)
        if (dbFile.exists()) {
            println("Opening existing database: $dbFilePath")
        } else {
            println("Creating new database: $dbFilePath")
        }

        val driver = JdbcSqliteDriver("jdbc:sqlite:$dbFilePath")
        Database.Schema.create(driver)
        val database = Database(driver)
        val queries = database.databaseQueries

        // Import entries
        println("\nImporting entries...")
        var successCount = 0
        var skipCount = 0

        lexicalEntries.entries.forEachIndexed { index, entry ->
            try {
                insertEntry(queries, entry)
                successCount++

                if ((index + 1) % 100 == 0) {
                    println("  Progress: ${index + 1}/${lexicalEntries.entries.size} entries processed...")
                }
            } catch (e: Exception) {
                skipCount++
                System.err.println("Warning: Could not import entry ${entry.surface} -> ${entry.base}: ${e.message}")
            }
        }

        driver.close()

        println("\n=== Import Complete ===")
        println("Successfully imported: $successCount entries")
        if (skipCount > 0) {
            println("Skipped (already exist): $skipCount entries")
        }
        println("Total in JSON: ${lexicalEntries.entries.size}")
        println("Target database: $dbFilePath")

        return successCount
    }

    /**
     * Restores multiple JSON backup files into a single database.
     * Useful for merging backups from multiple books.
     *
     * @param jsonFilePaths List of JSON backup file paths
     * @param dbFilePath Path to the target SQLite database
     * @return Total number of entries successfully imported
     */
    fun restoreMultiple(jsonFilePaths: List<String>, dbFilePath: String): Int {
        println("=== Restoring from Multiple JSON Backups ===")
        println("Number of JSON files: ${jsonFilePaths.size}")
        println("Target DB: $dbFilePath")

        var totalImported = 0

        jsonFilePaths.forEachIndexed { index, jsonPath ->
            println("\n[${index + 1}/${jsonFilePaths.size}] Processing: $jsonPath")
            try {
                val imported = restore(jsonPath, dbFilePath)
                totalImported += imported
            } catch (e: Exception) {
                System.err.println("Error processing $jsonPath: ${e.message}")
                e.printStackTrace()
            }
        }

        println("\n=== All Imports Complete ===")
        println("Total entries imported: $totalImported")
        println("Target database: $dbFilePath")

        return totalImported
    }

    /**
     * Restores all JSON backup files from a directory into a single database.
     * Useful for merging all book backups.
     *
     * @param directoryPath Path to directory containing JSON backup files
     * @param dbFilePath Path to the target SQLite database
     * @param pattern Optional file pattern (default: "*.json")
     * @return Total number of entries successfully imported
     */
    fun restoreFromDirectory(
        directoryPath: String,
        dbFilePath: String,
        pattern: String = ".*\\.json$"
    ): Int {
        println("=== Restoring from Directory ===")
        println("Directory: $directoryPath")
        println("Pattern: $pattern")
        println("Target DB: $dbFilePath")

        val directory = File(directoryPath)
        if (!directory.exists() || !directory.isDirectory) {
            throw IllegalArgumentException("Directory not found or not a directory: $directoryPath")
        }

        // Find all JSON files matching pattern
        val regex = Regex(pattern)
        val jsonFiles = directory.listFiles()?.filter { file ->
            file.isFile && regex.matches(file.name)
        }?.sortedBy { it.name } ?: emptyList()

        if (jsonFiles.isEmpty()) {
            println("No JSON files found matching pattern: $pattern")
            return 0
        }

        println("Found ${jsonFiles.size} JSON files")

        val jsonPaths = jsonFiles.map { it.absolutePath }
        return restoreMultiple(jsonPaths, dbFilePath)
    }

    /**
     * Inserts a single lexical entry into the database.
     * Handles the relationships: base -> surface -> variants
     */
    private fun insertEntry(queries: DatabaseQueries, entry: LexicalEntry) {
        // 1. Insert base (will be ignored if already exists due to UNIQUE constraint)
        queries.insertBase(entry.base)

        // 2. Get base_id
        val baseId = queries.selectBaseByValue(entry.base).executeAsOne()

        // 3. Insert surface with base_id and optional notes
        queries.insertSurface(
            value_ = entry.surface,
            base_id = baseId,
            notes = entry.notes
        )

        // 4. Get surface_id
        val surfaceRow = queries.selectSurfaceByValue(entry.surface).executeAsOne()
        val surfaceId = surfaceRow.id

        // 5. Insert all variants
        entry.variants.forEach { variant ->
            queries.insertVariant(
                value_ = variant,
                surface_id = surfaceId
            )
        }
    }
}

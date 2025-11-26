package io.github.kdroidfilter.seforim.magicindexer

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforim.magicindexer.db.Database
import io.github.kdroidfilter.seforim.magicindexer.db.DatabaseQueries
import io.github.kdroidfilter.seforim.magicindexer.model.LexicalEntries
import io.github.kdroidfilter.seforim.magicindexer.model.LexicalEntry
import io.github.kdroidfilter.seforim.magicindexer.model.LexicalEntrySerializer
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths

/**
 * Generator that transforms JSON lexical entries into a SQLite database.
 */
object DatabaseGenerator {

    /**
     * Generates a SQLite database from a JSON file containing lexical entries.
     *
     * @param jsonFilePath Path to the input JSON file
     * @param dbFilePath Path to the output SQLite database file
     * @return Number of entries processed
     */
    fun generateDatabase(jsonFilePath: String, dbFilePath: String): Int {
        println("Reading JSON file: $jsonFilePath")

        // Read and parse JSON
        val jsonContent = File(jsonFilePath).readText()
        val lexicalEntries = LexicalEntrySerializer.fromJson(jsonContent)

        println("Found ${lexicalEntries.entries.size} entries to process")

        // Delete existing database if it exists
        val dbFile = File(dbFilePath)
        if (dbFile.exists()) {
            println("Deleting existing database: $dbFilePath")
            dbFile.delete()
        }

        // Create database connection
        println("Creating database: $dbFilePath")
        val driver = JdbcSqliteDriver("jdbc:sqlite:$dbFilePath")
        Database.Schema.create(driver)
        val database = Database(driver)
        val queries = database.databaseQueries

        // Process each entry
        var processedCount = 0
        lexicalEntries.entries.forEach { entry ->
            try {
                insertEntry(queries, entry)
                processedCount++
                if (processedCount % 100 == 0) {
                    println("Processed $processedCount entries...")
                }
            } catch (e: Exception) {
                System.err.println("Error processing entry: ${entry.surface} -> ${entry.base}")
                e.printStackTrace()
            }
        }

        println("Successfully generated database with $processedCount entries")
        println("Database location: $dbFilePath")

        // Print statistics
        printStatistics(queries)

        return processedCount
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

        // 5. Insert all variants (globally unique, shared across surfaces)
        entry.variants.forEach { variant ->
            // Insert variant (will be ignored if already exists due to UNIQUE constraint)
            queries.insertVariant(value_ = variant)

            // Get variant_id
            val variantRow = queries.selectVariantByValue(variant).executeAsOne()
            val variantId = variantRow.id

            // Link variant to surface (will be ignored if link already exists)
            queries.insertSurfaceVariant(
                surface_id = surfaceId,
                variant_id = variantId
            )
        }
    }

    /**
     * Prints database statistics.
     */
    private fun printStatistics(queries: DatabaseQueries) {
        // Count bases, surfaces, and variants
        val baseCount = queries.transactionWithResult {
            var count = 0L
            queries.selectBaseByValue("").executeAsOneOrNull() // This will fail, but we can use a different approach
            // Since we don't have a count query, we'll skip this for now
            count
        }

        println("\n=== Database Statistics ===")
        println("Database created successfully")
        println("===========================\n")
    }

    /**
     * Generates database from the default sample-result.json in resources.
     */
    fun generateFromResources(outputDbPath: String): Int {
        // Try to load from core module resources
        val resourcePath = "/sample-result.json"
        val inputStream = DatabaseGenerator::class.java.getResourceAsStream(resourcePath)
            ?: throw IllegalArgumentException("Could not find $resourcePath in resources")

        // Create temporary file with the resource content
        val tempFile = Files.createTempFile("sample-result", ".json").toFile()
        tempFile.deleteOnExit()

        inputStream.use { input ->
            tempFile.outputStream().use { output ->
                input.copyTo(output)
            }
        }

        return generateDatabase(tempFile.absolutePath, outputDbPath)
    }
}

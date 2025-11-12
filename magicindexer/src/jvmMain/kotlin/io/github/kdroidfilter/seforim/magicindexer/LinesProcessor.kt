package io.github.kdroidfilter.seforim.magicindexer

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforim.magicindexer.db.Database
import io.github.kdroidfilter.seforim.magicindexer.model.LexicalEntry
import io.github.kdroidfilter.seforim.magicindexer.model.LexicalEntrySerializer
import kotlinx.coroutines.runBlocking
import java.io.File

/**
 * Processes lines from the SeforimLibrary database through an LLM to generate lexical entries.
 * Writes entries directly to the database incrementally to avoid memory issues.
 */
object LinesProcessor {

    /**
     * Processes lines from specified books in the SeforimLibrary database.
     * Writes entries directly to the lexical database incrementally.
     *
     * @param sourceDbPath Path to the source SeforimLibrary database
     * @param outputDbPath Path to save the generated lexical database
     * @param bookIds List of book IDs to process (defaults to 1-10)
     * @param saveJsonBackup If true, saves a JSON backup for debugging
     * @return Number of entries processed
     */
    fun processLines(
        sourceDbPath: String,
        outputDbPath: String,
        bookIds: List<Long> = (1L..10L).toList(),
        saveJsonBackup: Boolean = false
    ): Int = runBlocking {
        println("=== Starting Incremental Processing ===")
        println("Source DB: $sourceDbPath")
        println("Output DB: $outputDbPath")
        println("Books to process: ${bookIds.joinToString(", ")}")

        // Initialize the source repository
        println("\nInitializing SeforimRepository...")
        val sourceDriver = JdbcSqliteDriver("jdbc:sqlite:$sourceDbPath")
        val repository = SeforimRepository(sourceDbPath, sourceDriver)

        // Initialize the output database
        println("Initializing output lexical database...")
        val dbFile = File(outputDbPath)
        if (dbFile.exists()) {
            println("Deleting existing database: $outputDbPath")
            dbFile.delete()
        }

        val outputDriver = JdbcSqliteDriver("jdbc:sqlite:$outputDbPath")
        Database.Schema.create(outputDriver)
        val database = Database(outputDriver)
        val queries = database.databaseQueries

        var totalLinesProcessed = 0
        var totalEntriesInserted = 0
        val jsonBackup = if (saveJsonBackup) mutableListOf<LexicalEntry>() else null

        try {
            // Process specified books
            for (bookId in bookIds) {
                println("\n=== Processing Book ID: $bookId ===")

                val book = repository.getBook(bookId)
                if (book == null) {
                    println("Book $bookId not found, skipping...")
                    continue
                }

                println("Book: ${book.title}")
                println("Total lines: ${book.totalLines}")

                // Get all lines for this book
                val lines = repository.getLines(bookId, 0, book.totalLines)
                println("Retrieved ${lines.size} lines")

                // Process each line
                lines.forEachIndexed { index, line ->
                    if (line.content.isBlank()) {
                        return@forEachIndexed
                    }

                    try {
                        // Call LLM with line content
                        if ((index + 1) % 10 == 0 || index == 0) {
                            println("Processing line ${index + 1}/${lines.size} from book $bookId...")
                        }

                        val response = LLMProvider.generateResponse(line.content)

                        if (response.isBlank()) {
                            if ((index + 1) % 10 == 0) {
                                println("  Empty response from LLM, skipping...")
                            }
                            return@forEachIndexed
                        }

                        // Parse JSON response
                        val entries = LexicalEntrySerializer.fromJson(response).entries

                        // Insert each entry directly into the database
                        entries.forEach { entry ->
                            try {
                                insertEntry(queries, entry)
                                totalEntriesInserted++

                                // Add to backup if enabled
                                jsonBackup?.add(entry)

                            } catch (e: Exception) {
                                System.err.println("Error inserting entry: ${entry.surface} -> ${entry.base}: ${e.message}")
                            }
                        }

                        totalLinesProcessed++

                        // Progress update every 10 lines
                        if ((index + 1) % 10 == 0) {
                            println("  Progress: ${index + 1}/${lines.size} lines | $totalEntriesInserted entries inserted")
                        }

                    } catch (e: Exception) {
                        System.err.println("Error processing line ${line.id}: ${e.message}")
                        e.printStackTrace()
                    }
                }

                println("Completed book $bookId: $totalLinesProcessed lines processed, $totalEntriesInserted entries inserted")
            }

            // Save JSON backup if requested
            if (saveJsonBackup && jsonBackup != null) {
                val jsonPath = outputDbPath.replace(".db", "-backup.json")
                println("\nSaving JSON backup to: $jsonPath")
                val json = LexicalEntrySerializer.toJson(
                    io.github.kdroidfilter.seforim.magicindexer.model.LexicalEntries(jsonBackup)
                )
                File(jsonPath).writeText(json)
                println("JSON backup saved successfully")
            }

            println("\n=== Processing Complete ===")
            println("Total lines processed: $totalLinesProcessed")
            println("Total entries inserted: $totalEntriesInserted")
            println("Output DB: $outputDbPath")

            // Print statistics
            printStatistics(queries)

        } finally {
            // Close connections
            repository.close()
            outputDriver.close()
        }

        return@runBlocking totalEntriesInserted
    }

    /**
     * Inserts a single lexical entry into the database.
     * Handles the relationships: base -> surface -> variants
     */
    private fun insertEntry(queries: io.github.kdroidfilter.seforim.magicindexer.db.DatabaseQueries, entry: LexicalEntry) {
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

    /**
     * Prints database statistics.
     */
    private fun printStatistics(queries: io.github.kdroidfilter.seforim.magicindexer.db.DatabaseQueries) {
        println("\n=== Database Statistics ===")
        println("Database created successfully with incremental writes")
        println("===========================\n")
    }
}

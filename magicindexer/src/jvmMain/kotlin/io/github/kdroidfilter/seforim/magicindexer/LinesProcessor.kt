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

        // Initialize the output database (create if doesn't exist, or open existing)
        val dbFile = File(outputDbPath)
        if (dbFile.exists()) {
            println("Opening existing database: $outputDbPath")
        } else {
            println("Creating new database: $outputDbPath")
        }

        val outputDriver = JdbcSqliteDriver("jdbc:sqlite:$outputDbPath")
        Database.Schema.create(outputDriver)
        val database = Database(outputDriver)
        val queries = database.databaseQueries

        var totalLinesProcessed = 0
        var totalLinesSkipped = 0
        var totalEntriesInserted = 0

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

                // JSON backup per book
                val bookJsonBackup = if (saveJsonBackup) mutableListOf<LexicalEntry>() else null

                // Check how many lines already processed for this book
                val alreadyProcessedCount = queries.countProcessedLinesForBook(bookId).executeAsOne()
                if (alreadyProcessedCount > 0) {
                    println("Already processed: $alreadyProcessedCount lines (will skip)")
                }

                // Get all lines for this book
                val lines = repository.getLines(bookId, 0, book.totalLines)
                println("Retrieved ${lines.size} lines")

                // Process each line
                lines.forEachIndexed { index, line ->
                    if (line.content.isBlank()) {
                        return@forEachIndexed
                    }

                    try {
                        // Check if line already processed
                        val isProcessed = queries.isLineProcessed(line.id).executeAsOne()
                        if (isProcessed) {
                            totalLinesSkipped++
                            if ((index + 1) % 100 == 0) {
                                println("  Skipped ${totalLinesSkipped} already processed lines...")
                            }
                            return@forEachIndexed
                        }

                        // Call LLM with line content
                        if ((index + 1) % 10 == 0 || index == 0) {
                            println("Processing line ${index + 1}/${lines.size} (line_id: ${line.id}) from book $bookId...")
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

                                // Add to book backup if enabled
                                bookJsonBackup?.add(entry)

                            } catch (e: Exception) {
                                System.err.println("Error inserting entry: ${entry.surface} -> ${entry.base}: ${e.message}")
                            }
                        }

                        // Mark line as processed
                        val currentTime = System.currentTimeMillis() / 1000
                        queries.insertProcessedLine(line.id, bookId, currentTime)

                        totalLinesProcessed++

                        // Progress update every 10 lines
                        if ((index + 1) % 10 == 0) {
                            println("  Progress: ${index + 1}/${lines.size} lines | $totalLinesProcessed new, $totalLinesSkipped skipped | $totalEntriesInserted entries")
                        }

                    } catch (e: Exception) {
                        System.err.println("Error processing line ${line.id}: ${e.message}")
                        e.printStackTrace()
                    }
                }

                val bookProcessedCount = queries.countProcessedLinesForBook(bookId).executeAsOne()
                println("Completed book $bookId: $totalLinesProcessed new lines processed, $totalLinesSkipped skipped, $bookProcessedCount total processed for this book")

                // Save JSON backup for this book
                if (saveJsonBackup && bookJsonBackup != null && bookJsonBackup.isNotEmpty()) {
                    // Sanitize book title for filename
                    val sanitizedTitle = book.title
                        .replace(Regex("[^a-zA-Z0-9א-ת\\s-]"), "")
                        .replace(Regex("\\s+"), "-")
                        .take(50) // Limit length

                    val jsonPath = File(File(outputDbPath).parent, "book-${bookId}-${sanitizedTitle}.json").absolutePath
                    println("Saving JSON backup for book $bookId to: $jsonPath")

                    val json = LexicalEntrySerializer.toJson(
                        io.github.kdroidfilter.seforim.magicindexer.model.LexicalEntries(bookJsonBackup)
                    )
                    File(jsonPath).writeText(json)
                    println("JSON backup saved: ${bookJsonBackup.size} entries")
                }
            }

            println("\n=== Processing Complete ===")
            println("Total new lines processed: $totalLinesProcessed")
            println("Total lines skipped (already processed): $totalLinesSkipped")
            println("Total entries inserted: $totalEntriesInserted")
            println("Output DB: $outputDbPath")

            // Print statistics
            printStatistics(queries, bookIds)

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
    private fun printStatistics(queries: io.github.kdroidfilter.seforim.magicindexer.db.DatabaseQueries, bookIds: List<Long>) {
        println("\n=== Database Statistics ===")
        println("Database created successfully with incremental writes")

        // Statistics per book
        println("\nProcessed lines per book:")
        bookIds.forEach { bookId ->
            val count = queries.countProcessedLinesForBook(bookId).executeAsOne()
            println("  Book $bookId: $count lines processed")
        }

        println("===========================\n")
    }
}

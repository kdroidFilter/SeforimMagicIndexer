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

    private const val BATCH_SIZE = 10 // Number of lines to process together
    private const val TIMEOUT_SECONDS = 40 // Timeout per batch in seconds

    /**
     * Processes lines from specified books in the SeforimLibrary database.
     * Writes entries directly to the lexical database incrementally.
     * Lines are processed in batches of 10 to optimize LLM calls.
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
                println("Processing in batches of $BATCH_SIZE lines")

                // Process lines in batches
                var batchIndex = 0
                while (batchIndex < lines.size) {
                    val batchContents = mutableListOf<String>()
                    val batchLineIds = mutableListOf<Long>()

                    // Build batch of unprocessed lines
                    var currentIndex = batchIndex
                    while (batchContents.size < BATCH_SIZE && currentIndex < lines.size) {
                        val line = lines[currentIndex]
                        currentIndex++

                        // Skip blank lines
                        if (line.content.isBlank()) {
                            continue
                        }

                        // Check if line already processed
                        val isProcessed = queries.isLineProcessed(line.id).executeAsOne()
                        if (isProcessed) {
                            totalLinesSkipped++
                            continue
                        }

                        batchContents.add(line.content)
                        batchLineIds.add(line.id)
                    }

                    batchIndex = currentIndex

                    // Skip if batch is empty
                    if (batchContents.isEmpty()) {
                        if (batchIndex % 100 == 0 && totalLinesSkipped > 0) {
                            println("  Skipped $totalLinesSkipped already processed lines...")
                        }
                        continue
                    }

                    try {
                        // Combine batch lines content
                        val combinedContent = batchContents.joinToString("\n")

                        // Progress update
                        println("Processing batch at lines ${batchIndex - batchContents.size + 1}-$batchIndex/${lines.size} (${batchContents.size} lines in batch, ${TIMEOUT_SECONDS}s timeout)")

                        // Call LLM with combined content and timeout
                        val response = LLMProvider.generateResponse(combinedContent, TIMEOUT_SECONDS)

                        if (response.isBlank()) {
                            println("  ⚠ Empty response from LLM (possibly timeout or error), skipping batch...")
                            continue
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

                        // Mark all lines in batch as processed
                        val currentTime = System.currentTimeMillis() / 1000
                        batchLineIds.forEach { lineId ->
                            queries.insertProcessedLine(lineId, bookId, currentTime)
                        }

                        totalLinesProcessed += batchContents.size

                        // Progress update
                        println("  → Processed ${batchContents.size} lines | Generated ${entries.size} entries | Total: $totalLinesProcessed new, $totalLinesSkipped skipped")

                    } catch (e: Exception) {
                        System.err.println("Error processing batch: ${e.message}")
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

package io.github.kdroidfilter.seforim.magicindexer

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforim.magicindexer.db.Database
import io.github.kdroidfilter.seforim.magicindexer.model.LexicalEntry
import io.github.kdroidfilter.seforim.magicindexer.model.LexicalEntrySerializer
import kotlinx.coroutines.runBlocking
import org.jsoup.Jsoup
import org.jsoup.safety.Safelist
import java.io.File

/**
 * Processes lines from the SeforimLibrary database through an LLM to generate lexical entries.
 * Writes entries directly to the database incrementally to avoid memory issues.
 */
object LinesProcessor {

    private const val BATCH_SIZE = 10 // Number of lines to process together
    private const val TIMEOUT_SECONDS = 80 // Timeout per batch in seconds
    private const val INVALID_JSON_DIR = "build/invalid-llm-json"

    /**
     * Cleans HTML tags from text using JSoup and returns plain text.
     * Removes all HTML tags, decodes HTML entities, and trims whitespace.
     */
    private fun cleanHtml(html: String): String {
        return Jsoup.clean(html, Safelist.none())
            .replace("&nbsp;", " ")
            .replace("\u00A0", " ")
            .trim()
    }

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

                        // Clean HTML before adding to batch
                        val cleanedContent = cleanHtml(line.content)
                        if (cleanedContent.isNotBlank()) {
                            batchContents.add(cleanedContent)
                            batchLineIds.add(line.id)
                        }
                    }

                    batchIndex = currentIndex

                    // Skip if batch is empty
                    if (batchContents.isEmpty()) {
                        if (batchIndex % 100 == 0 && totalLinesSkipped > 0) {
                            println("  Skipped $totalLinesSkipped already processed lines...")
                        }
                        continue
                    }

                    var llmResponse: String? = null
                    try {
                        // Combine batch lines content
                        val combinedContent = batchContents.joinToString("\n")

                        // Progress update
                        println("Processing batch at lines ${batchIndex - batchContents.size + 1}-$batchIndex/${lines.size} (${batchContents.size} lines in batch, ${TIMEOUT_SECONDS}s timeout)")

                        // Call LLM with combined content and timeout
                        llmResponse = LLMProvider.generateResponse(combinedContent, TIMEOUT_SECONDS)

                        if (llmResponse.isNullOrBlank()) {
                            println("  ⚠ Empty response from LLM (possibly timeout or error), skipping batch...")
                            continue
                        }

                        // Parse JSON response (with robust error handling)
                        val entries = parseLexicalEntriesOrNull(
                            response = llmResponse,
                            bookId = bookId,
                            batchIndex = batchIndex
                        ) ?: run {
                            println("  ⚠ Invalid JSON from LLM, skipping this batch (bookId=$bookId, batchIndex=$batchIndex)")
                            continue
                        }

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
                        // If we have the raw LLM response, save it for later inspection
                        // (only if it was not already saved by parseLexicalEntriesOrNull)
                        // This helps diagnose issues like truncated JSON.
                        // We intentionally do not rethrow to allow processing to continue.
                        // The problematic lines will be retried on the next run.
                        // Note: suppress any secondary I/O errors to avoid masking the root cause.
                        try {
                            // Heuristic: only save if this does not look like a parse error already handled
                            val message = e.message.orEmpty()
                            if (llmResponse != null &&
                                !message.contains("Expected end of the array") &&
                                !message.contains("Unexpected JSON token")
                            ) {
                                saveInvalidJsonResponse(
                                    response = llmResponse,
                                    bookId = bookId,
                                    batchIndex = batchIndex
                                )
                            }
                        } catch (_: Exception) {
                            // Ignore secondary failures when saving debug JSON
                        }
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

    /**
     * Tries to parse the LLM JSON response into a list of lexical entries.
     * On failure, logs a concise error message and saves the raw response
     * into build/invalid-llm-json for offline inspection.
     */
    private fun parseLexicalEntriesOrNull(
        response: String,
        bookId: Long,
        batchIndex: Int
    ): List<LexicalEntry>? {
        return try {
            LexicalEntrySerializer.fromJson(response).entries
        } catch (e: Exception) {
            System.err.println(
                "  ⚠ Failed to parse LLM JSON for book $bookId, batch $batchIndex: ${e.message}"
            )
            try {
                saveInvalidJsonResponse(response, bookId, batchIndex)
            } catch (_: Exception) {
                // Ignore secondary failures when saving debug JSON
            }
            null
        }
    }

    /**
     * Saves an invalid LLM JSON response to disk for debugging.
     * Files are written under build/invalid-llm-json with a descriptive name.
     */
    private fun saveInvalidJsonResponse(
        response: String,
        bookId: Long,
        batchIndex: Int
    ) {
        val dir = File(INVALID_JSON_DIR)
        if (!dir.exists()) {
            dir.mkdirs()
        }
        val fileName = "book-${bookId}-batch-${batchIndex}-${System.currentTimeMillis()}.json"
        val file = File(dir, fileName)
        file.writeText(response)
        println("  ⚠ Saved invalid JSON response to: ${file.absolutePath}")
    }
}

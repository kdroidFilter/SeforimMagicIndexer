package io.github.kdroidfilter.seforim.magicindexer

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforim.magicindexer.db.Database
import io.github.kdroidfilter.seforim.magicindexer.db.DatabaseQueries
import io.github.kdroidfilter.seforim.magicindexer.model.LexicalEntry
import io.github.kdroidfilter.seforim.magicindexer.model.LexicalEntrySerializer
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import org.jsoup.Jsoup
import org.jsoup.safety.Safelist
import java.io.File
import java.util.concurrent.atomic.AtomicInteger

/**
 * Processes lines from the SeforimLibrary database through an LLM to generate lexical entries.
 * Writes entries directly to the database incrementally to avoid memory issues.
 */
object LinesProcessor {

    private const val BATCH_SIZE = 2 // Number of lines to process together
    private const val TIMEOUT_SECONDS = 160 // Timeout per batch in seconds
    private const val INVALID_JSON_DIR = "build/invalid-llm-json"
    private const val DEFAULT_CONCURRENT_REQUESTS = 1 // Default to sequential processing

    /**
     * Data class representing a batch of lines to process.
     */
    private data class BatchData(
        val contents: List<String>,
        val lineIds: List<Long>,
        val startIndex: Int,
        val endIndex: Int
    )

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
     * Lines are processed in batches to optimize LLM calls.
     *
     * @param sourceDbPath Path to the source SeforimLibrary database
     * @param outputDbPath Path to save the generated lexical database
     * @param bookIds List of book IDs to process (defaults to 1-10)
     * @param saveJsonBackup If true, saves a JSON backup for debugging
     * @param concurrentRequests Number of concurrent LLM requests (default: 1 for sequential)
     * @return Number of entries processed
     */
    fun processLines(
        sourceDbPath: String,
        outputDbPath: String,
        bookIds: List<Long> = (1L..10L).toList(),
        saveJsonBackup: Boolean = false,
        concurrentRequests: Int = DEFAULT_CONCURRENT_REQUESTS
    ): Int = runBlocking {
        val effectiveConcurrency = concurrentRequests.coerceIn(1, 250) // Limit between 1 and 20

        println("=== Starting Incremental Processing ===")
        println("Source DB: $sourceDbPath")
        println("Output DB: $outputDbPath")
        println("Books to process: ${bookIds.joinToString(", ")}")
        println("Concurrent requests: $effectiveConcurrency")

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

        // Thread-safe counters
        val totalLinesProcessed = AtomicInteger(0)
        val totalLinesSkipped = AtomicInteger(0)
        val totalEntriesInserted = AtomicInteger(0)

        // Mutex for database operations (SQLite is not thread-safe)
        val dbMutex = Mutex()

        // Semaphore to limit concurrent LLM requests
        val llmSemaphore = Semaphore(effectiveConcurrency)

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

                // JSON backup per book (thread-safe list)
                val bookJsonBackup = if (saveJsonBackup) mutableListOf<LexicalEntry>() else null
                val backupMutex = Mutex()

                // Check how many lines already processed for this book
                val alreadyProcessedCount = dbMutex.withLock {
                    queries.countProcessedLinesForBook(bookId).executeAsOne()
                }
                if (alreadyProcessedCount > 0) {
                    println("Already processed: $alreadyProcessedCount lines (will skip)")
                }

                // Get all lines for this book
                val lines = repository.getLines(bookId, 0, book.totalLines)
                println("Retrieved ${lines.size} lines")
                println("Processing in batches of $BATCH_SIZE lines with $effectiveConcurrency concurrent requests")

                // Prepare all batches first
                val batches = mutableListOf<BatchData>()
                var batchIndex = 0

                while (batchIndex < lines.size) {
                    val batchContents = mutableListOf<String>()
                    val batchLineIds = mutableListOf<Long>()
                    val startIdx = batchIndex

                    var currentIndex = batchIndex
                    while (batchContents.size < BATCH_SIZE && currentIndex < lines.size) {
                        val line = lines[currentIndex]
                        currentIndex++

                        // Skip blank lines
                        if (line.content.isBlank()) {
                            continue
                        }

                        // Check if line already processed
                        val isProcessed = dbMutex.withLock {
                            queries.isLineProcessed(line.id).executeAsOne()
                        }
                        if (isProcessed) {
                            totalLinesSkipped.incrementAndGet()
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

                    if (batchContents.isNotEmpty()) {
                        batches.add(BatchData(
                            contents = batchContents.toList(),
                            lineIds = batchLineIds.toList(),
                            startIndex = startIdx,
                            endIndex = batchIndex
                        ))
                    }
                }

                println("Prepared ${batches.size} batches for parallel processing")

                // Process batches in parallel with limited concurrency
                val jobs = batches.map { batch ->
                    async(Dispatchers.IO) {
                        llmSemaphore.withPermit {
                            processBatch(
                                batch = batch,
                                bookId = bookId,
                                totalLines = lines.size,
                                queries = queries,
                                dbMutex = dbMutex,
                                bookJsonBackup = bookJsonBackup,
                                backupMutex = backupMutex,
                                totalLinesProcessed = totalLinesProcessed,
                                totalEntriesInserted = totalEntriesInserted
                            )
                        }
                    }
                }

                // Wait for all batches to complete
                jobs.awaitAll()

                val bookProcessedCount = dbMutex.withLock {
                    queries.countProcessedLinesForBook(bookId).executeAsOne()
                }
                println("Completed book $bookId: ${totalLinesProcessed.get()} new lines processed, ${totalLinesSkipped.get()} skipped, $bookProcessedCount total processed for this book")

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
            println("Total new lines processed: ${totalLinesProcessed.get()}")
            println("Total lines skipped (already processed): ${totalLinesSkipped.get()}")
            println("Total entries inserted: ${totalEntriesInserted.get()}")
            println("Output DB: $outputDbPath")

            // Print statistics
            dbMutex.withLock {
                printStatistics(queries, bookIds)
            }

        } finally {
            // Close connections
            repository.close()
            outputDriver.close()
        }

        return@runBlocking totalEntriesInserted.get()
    }

    /**
     * Processes a single batch of lines.
     */
    private suspend fun processBatch(
        batch: BatchData,
        bookId: Long,
        totalLines: Int,
        queries: DatabaseQueries,
        dbMutex: Mutex,
        bookJsonBackup: MutableList<LexicalEntry>?,
        backupMutex: Mutex?,
        totalLinesProcessed: AtomicInteger,
        totalEntriesInserted: AtomicInteger
    ) {
        var llmResponse: String? = null
        try {
            // Combine batch lines content
            val combinedContent = batch.contents.joinToString("\n")

            // Progress update
            println("Processing batch at lines ${batch.startIndex + 1}-${batch.endIndex}/$totalLines (${batch.contents.size} lines in batch, ${TIMEOUT_SECONDS}s timeout)")

            // Call LLM with combined content and timeout
            llmResponse = LLMProvider.generateResponse(combinedContent, TIMEOUT_SECONDS)

            if (llmResponse.isNullOrBlank()) {
                println("  ⚠ Empty response from LLM (possibly timeout or error), skipping batch...")
                return
            }

            // Parse JSON response (with robust error handling)
            val entries = parseLexicalEntriesOrNull(
                response = llmResponse,
                bookId = bookId,
                batchIndex = batch.endIndex
            ) ?: run {
                println("  ⚠ Invalid JSON from LLM, skipping this batch (bookId=$bookId, batchIndex=${batch.endIndex})")
                return
            }

            // Insert each entry directly into the database (synchronized)
            dbMutex.withLock {
                entries.forEach { entry ->
                    try {
                        insertEntry(queries, entry)
                        totalEntriesInserted.incrementAndGet()

                        // Add to book backup if enabled
                        bookJsonBackup?.add(entry)

                    } catch (e: Exception) {
                        System.err.println("Error inserting entry: ${entry.surface} -> ${entry.base}: ${e.message}")
                    }
                }

                // Mark all lines in batch as processed
                val currentTime = System.currentTimeMillis() / 1000
                batch.lineIds.forEach { lineId ->
                    queries.insertProcessedLine(lineId, bookId, currentTime)
                }
            }

            totalLinesProcessed.addAndGet(batch.contents.size)

            // Progress update
            println("  → Processed ${batch.contents.size} lines | Generated ${entries.size} entries | Total: ${totalLinesProcessed.get()} new")

        } catch (e: Exception) {
            System.err.println("Error processing batch: ${e.message}")
            try {
                val message = e.message.orEmpty()
                if (llmResponse != null &&
                    !message.contains("Expected end of the array") &&
                    !message.contains("Unexpected JSON token")
                ) {
                    saveInvalidJsonResponse(
                        response = llmResponse,
                        bookId = bookId,
                        batchIndex = batch.endIndex
                    )
                }
            } catch (_: Exception) {
                // Ignore secondary failures when saving debug JSON
            }
            e.printStackTrace()
        }
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

    /**
     * Prints database statistics.
     */
    private fun printStatistics(queries: DatabaseQueries, bookIds: List<Long>) {
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
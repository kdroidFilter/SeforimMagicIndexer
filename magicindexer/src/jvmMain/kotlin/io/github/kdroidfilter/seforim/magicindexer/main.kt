package io.github.kdroidfilter.seforim.magicindexer

import java.io.File

fun main(args: Array<String>) {
    println("=== Lexical Index Database Generator ===\n")

    // Check for special commands that don't require env vars
    if (args.isNotEmpty()) {
        when (args[0]) {
            "restore" -> {
                handleRestoreCommand(args)
                return
            }
            "help", "--help", "-h" -> {
                printUsage()
                return
            }
        }
    }

    // Get source DB from environment variable
    val sourceDbPath = System.getenv("SEFORIM_DB")
    if (sourceDbPath.isNullOrBlank()) {
        println("Error: SEFORIM_DB environment variable is not set")
        println("Please set it to the path of your SeforimLibrary database")
        println("Example: export SEFORIM_DB=/path/to/seforim.db")
        return
    }

    // Check for GEMINI_API_KEY
    val apiKey = System.getenv("GEMINI_API_KEY")
    if (apiKey.isNullOrBlank()) {
        println("Error: GEMINI_API_KEY environment variable is not set")
        println("Please set it to your Gemini API key")
        println("Example: export GEMINI_API_KEY=your-api-key")
        return
    }

    // Get concurrent requests from environment variable (default: 1)
    val concurrentRequests = System.getenv("CONCURRENT_REQUESTS")?.toIntOrNull() ?: 1

    // Output DB path in build/db directory
    val buildDir = File("build/db")
    if (!buildDir.exists()) {
        buildDir.mkdirs()
    }
    val outputDbPath = File(buildDir, "lexical.db").absolutePath

    // JSON backup is enabled by default (one JSON per book)
    val saveJsonBackup = true

    // Load book IDs from resources
    println("Loading book IDs from resources/books.txt...")
    val bookIds = try {
        loadBookIdsFromResources()
    } catch (e: Exception) {
        System.err.println("Error loading book IDs: ${e.message}")
        e.printStackTrace()
        return
    }

    if (bookIds.isEmpty()) {
        println("Error: No book IDs found in resources/books.txt")
        return
    }

    println("Source DB: $sourceDbPath")
    println("Output DB: $outputDbPath")
    println("JSON backup: One file per book in build/db/")
    println("Books to process: ${bookIds.joinToString(", ")}")
    println("Concurrent requests: $concurrentRequests")
    println()

    try {
        LinesProcessor.processLines(
            sourceDbPath = sourceDbPath,
            outputDbPath = outputDbPath,
            bookIds = bookIds,
            saveJsonBackup = saveJsonBackup,
            concurrentRequests = concurrentRequests
        )
        println("\n✓ Database successfully created at: $outputDbPath")
        println("✓ JSON backups saved in: ${File(outputDbPath).parent}/")
    } catch (e: Exception) {
        System.err.println("\n✗ Error processing lines: ${e.message}")
        e.printStackTrace()
    }
}

/**
 * Loads book IDs from the books.txt file in resources.
 * Supports:
 * - Individual IDs: 1 2 3
 * - Ranges: 1-10
 * - Mixed: 1 2 5-8 12
 * - Comments with #
 */
private fun loadBookIdsFromResources(): List<Long> {
    val resourcePath = "/books.txt"
    val inputStream = object {}.javaClass.getResourceAsStream(resourcePath)
        ?: throw IllegalArgumentException("Could not find $resourcePath in resources")

    return inputStream.bufferedReader().useLines { lines ->
        val bookIdArgs = mutableListOf<String>()

        lines.forEach { line ->
            val trimmed = line.trim()
            // Skip empty lines and comments
            if (trimmed.isNotEmpty() && !trimmed.startsWith("#")) {
                // Split by whitespace and add all tokens
                bookIdArgs.addAll(trimmed.split(Regex("\\s+")))
            }
        }

        parseBookIds(bookIdArgs)
    }
}

/**
 * Parses book IDs from command line arguments.
 * Supports formats: "1 2 3 5" or "1-10" or mixed "1 2 5-8 10"
 */
private fun parseBookIds(args: List<String>): List<Long> {
    val bookIds = mutableListOf<Long>()

    for (arg in args) {
        when {
            arg.contains("-") -> {
                // Range format: "1-10"
                val parts = arg.split("-")
                if (parts.size == 2) {
                    try {
                        val start = parts[0].toLong()
                        val end = parts[1].toLong()
                        bookIds.addAll(start..end)
                    } catch (e: NumberFormatException) {
                        System.err.println("Invalid range format: $arg")
                    }
                }
            }
            else -> {
                // Single ID
                try {
                    bookIds.add(arg.toLong())
                } catch (e: NumberFormatException) {
                    System.err.println("Invalid book ID: $arg")
                }
            }
        }
    }

    return bookIds.distinct().sorted()
}

/**
 * Handles the restore command for importing JSON backups.
 */
private fun handleRestoreCommand(args: Array<String>) {
    if (args.size < 3) {
        println("Error: 'restore' requires JSON file/directory path and target DB path")
        println("\nUsage:")
        println("  restore <json-file> <target-db>        # Restore single JSON")
        println("  restore --dir <directory> <target-db>  # Restore all JSON files from directory")
        return
    }

    try {
        if (args[1] == "--dir") {
            // Restore from directory
            if (args.size < 4) {
                println("Error: '--dir' requires directory path and target DB path")
                return
            }
            val directoryPath = args[2]
            val dbPath = args[3]
            RestoreFromJson.restoreFromDirectory(directoryPath, dbPath)
        } else {
            // Restore single file or multiple files
            val dbPath = args.last()
            val jsonPaths = args.slice(1 until args.lastIndex)

            if (jsonPaths.size == 1) {
                RestoreFromJson.restore(jsonPaths[0], dbPath)
            } else {
                RestoreFromJson.restoreMultiple(jsonPaths, dbPath)
            }
        }
    } catch (e: Exception) {
        System.err.println("Error during restore: ${e.message}")
        e.printStackTrace()
    }
}

private fun printUsage() {
    println("""
        Usage:
          java -jar magicindexer.jar [command] [options]

        Commands:

          (default) - Process books from SeforimLibrary database
            Environment Variables (required):
              SEFORIM_DB         Path to the SeforimLibrary database
              GEMINI_API_KEY     API key for Gemini LLM

            Environment Variables (optional):
              CONCURRENT_REQUESTS  Number of concurrent LLM requests (default: 1)
                                   Higher values speed up processing but may hit rate limits.
                                   Recommended: 2-5 for most APIs.

            Configuration:
              Book IDs are read from resources/books.txt
              Output database: build/db/lexical.db (automatically created)
              JSON backups: build/db/book-{id}-{title}.json (one per book)

            Example:
              export SEFORIM_DB=/path/to/seforim.db
              export GEMINI_API_KEY=your-api-key
              export CONCURRENT_REQUESTS=3
              java -jar magicindexer.jar

          restore <json-file> <target-db>
            Restore/import a single JSON backup into a database

            Example:
              java -jar magicindexer.jar restore backup.json lexical.db

          restore <json-file1> <json-file2> ... <target-db>
            Merge multiple JSON backups into a single database

            Example:
              java -jar magicindexer.jar restore book-1.json book-2.json lexical.db

          restore --dir <directory> <target-db>
            Restore all JSON files from a directory into a database

            Example:
              java -jar magicindexer.jar restore --dir build/db/ merged.db

          help, --help, -h
            Show this help message
    """.trimIndent())
    println()
}
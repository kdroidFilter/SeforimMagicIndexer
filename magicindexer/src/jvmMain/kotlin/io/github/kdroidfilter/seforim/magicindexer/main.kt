package io.github.kdroidfilter.seforim.magicindexer

import java.io.File

fun main(args: Array<String>) {
    println("=== Lexical Index Database Generator ===\n")

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

    // Output DB path in build/db directory
    val buildDir = File("build/db")
    if (!buildDir.exists()) {
        buildDir.mkdirs()
    }
    val outputDbPath = File(buildDir, "lexical.db").absolutePath
    val jsonBackupPath = outputDbPath.replace(".db", "-backup.json")

    // JSON backup is enabled by default
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
    println("JSON backup: $jsonBackupPath")
    println("Books to process: ${bookIds.joinToString(", ")}")
    println()

    try {
        LinesProcessor.processLines(sourceDbPath, outputDbPath, bookIds, saveJsonBackup)
        println("\n✓ Database successfully created at: $outputDbPath")
        println("✓ JSON backup saved at: $jsonBackupPath")
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

private fun printUsage() {
    println("""
        Usage:
          java -jar magicindexer.jar

        Environment Variables (required):
          SEFORIM_DB     Path to the SeforimLibrary database
          GEMINI_API_KEY API key for Gemini LLM

        Configuration:
          Book IDs are read from resources/books.txt
          Output database: build/db/lexical.db (automatically created)
          JSON backup: build/db/lexical-backup.json (automatically created)

          Format in books.txt:
            - Individual IDs: 1 2 3
            - Ranges: 1-10
            - Mixed: 1 2 5-8 12
            - Comments with #

        Example:
          export SEFORIM_DB=/path/to/seforim.db
          export GEMINI_API_KEY=your-api-key

          # Process books defined in resources/books.txt
          java -jar magicindexer.jar
    """.trimIndent())
    println()
}
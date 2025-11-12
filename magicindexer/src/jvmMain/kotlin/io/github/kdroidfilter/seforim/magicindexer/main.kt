package io.github.kdroidfilter.seforim.magicindexer

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

    if (args.isEmpty()) {
        printUsage()
        return
    }

    val outputDbPath = args[0]

    // Parse book IDs from remaining arguments
    val bookIdArgs = args.drop(1).filter { it != "--save-json" }
    if (bookIdArgs.isEmpty()) {
        println("Error: No book IDs specified")
        printUsage()
        return
    }

    val bookIds = parseBookIds(bookIdArgs)
    val saveJsonBackup = args.contains("--save-json")

    println("Source DB: $sourceDbPath")
    println("Output DB: $outputDbPath")
    println("Books to process: ${bookIds.joinToString(", ")}")
    if (saveJsonBackup) {
        println("JSON backup will be saved")
    }
    println()

    try {
        LinesProcessor.processLines(sourceDbPath, outputDbPath, bookIds, saveJsonBackup)
    } catch (e: Exception) {
        System.err.println("\nError processing lines: ${e.message}")
        e.printStackTrace()
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
          java -jar magicindexer.jar <output-db-path> <book-ids...> [--save-json]

        Environment Variables (required):
          SEFORIM_DB     Path to the SeforimLibrary database
          GEMINI_API_KEY API key for Gemini LLM

        Arguments:
          <output-db-path>  Path where the lexical database will be created
          <book-ids...>     Book IDs to process (individual: 1 2 3, range: 1-10, mixed: 1 2 5-8)
          --save-json       Optional: save JSON backup for debugging

        Examples:
          export SEFORIM_DB=/path/to/seforim.db
          export GEMINI_API_KEY=your-api-key

          # Process books 1, 2, and 3
          java -jar magicindexer.jar lexical.db 1 2 3

          # Process books 1 to 10
          java -jar magicindexer.jar lexical.db 1-10

          # Process books 1, 2, and 5 to 8 with JSON backup
          java -jar magicindexer.jar lexical.db 1 2 5-8 --save-json
    """.trimIndent())
    println()
}
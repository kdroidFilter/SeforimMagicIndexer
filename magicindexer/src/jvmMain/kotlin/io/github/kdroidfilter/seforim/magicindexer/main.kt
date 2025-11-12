package io.github.kdroidfilter.seforim.magicindexer

import java.nio.file.Files
import java.nio.file.Paths

fun main(args: Array<String>) {
    println("=== Lexical Index Database Generator ===\n")

    // Parse command line arguments
    if (args.isEmpty()) {
        printUsage()
        generateFromDefaultLocation()
    } else {
        when (args[0]) {
            "generate" -> {
                if (args.size < 3) {
                    println("Error: 'generate' requires input JSON path and output DB path")
                    printUsage()
                    return
                }
                val jsonPath = args[1]
                val dbPath = args[2]
                DatabaseGenerator.generateDatabase(jsonPath, dbPath)
            }
            "generate-from-resources" -> {
                val dbPath = if (args.size >= 2) args[1] else "lexical-index.db"
                DatabaseGenerator.generateFromResources(dbPath)
            }
            "help" -> {
                printUsage()
            }
            else -> {
                println("Unknown command: ${args[0]}\n")
                printUsage()
            }
        }
    }
}

private fun generateFromDefaultLocation() {
    println("No arguments provided. Generating from resources with default output location.\n")
    val defaultDbPath = "lexical-index.db"
    try {
        DatabaseGenerator.generateFromResources(defaultDbPath)
    } catch (e: Exception) {
        System.err.println("Error generating database: ${e.message}")
        e.printStackTrace()
    }
}

private fun printUsage() {
    println("""
        Usage:
          java -jar magicindexer.jar [command] [arguments]

        Commands:
          generate <json-path> <db-path>
              Generate database from a JSON file
              Example: java -jar magicindexer.jar generate data.json output.db

          generate-from-resources [db-path]
              Generate database from embedded sample-result.json
              Example: java -jar magicindexer.jar generate-from-resources mydb.db
              Default db-path: lexical-index.db

          help
              Show this help message

        If no command is provided, generates from resources with default output location.
    """.trimIndent())
    println()
}
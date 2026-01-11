package io.github.kdroidfilter.seforim.magicindexer

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforim.magicindexer.db.Database
import java.io.File

fun main(args: Array<String>) {
    println("=== Hebrew Diacritics Post-Processor ===\n")

    val dbPath = if (args.isNotEmpty()) {
        args[0]
    } else {
        // Default to build/db/lexical.db
        val defaultPath = File("build/db/lexical.db").absolutePath
        println("No database path provided, using default: $defaultPath")
        defaultPath
    }

    // Parse options
    var cleanSurfaces = true
    var cleanVariants = true
    var cleanBases = true

    for (i in 1 until args.size) {
        when (args[i]) {
            "--no-surfaces" -> cleanSurfaces = false
            "--no-variants" -> cleanVariants = false
            "--no-bases" -> cleanBases = false
            "--help", "-h" -> {
                printUsage()
                return
            }
        }
    }

    println("Database: $dbPath")
    println("Clean surfaces: $cleanSurfaces")
    println("Clean variants: $cleanVariants")
    println("Clean bases: $cleanBases")

    try {
        val stats = HebrewDiacriticsPostProcessor.postProcess(
            dbPath = dbPath,
            cleanSurfaces = cleanSurfaces,
            cleanVariants = cleanVariants,
            cleanBases = cleanBases
        )

        println("\n=== Post-Processing Complete ===")
        println("Total entries modified: ${stats.totalModified}")
        if (cleanSurfaces) println("  Surfaces: ${stats.surfacesModified} / ${stats.surfacesProcessed}")
        if (cleanVariants) println("  Variants: ${stats.variantsModified} / ${stats.variantsProcessed}")
        if (cleanBases) println("  Bases: ${stats.basesModified} / ${stats.basesProcessed}")
    } catch (e: Exception) {
        System.err.println("Error during post-processing: ${e.message}")
        e.printStackTrace()
    }
}

private fun printUsage() {
    println("""
        Usage: postprocessor [database-path] [options]

        Remove Hebrew diacritics (nikud and taamim) from database values.
        If no path is provided, defaults to build/db/lexical.db

        Options:
          --no-surfaces     Skip cleaning surface values
          --no-variants     Skip cleaning variant values
          --no-bases        Skip cleaning base values
          --help, -h        Show this help message

        Examples:
          postprocessor
          postprocessor build/db/lexical.db
          postprocessor lexical.db --no-bases
    """.trimIndent())
}

/**
 * Post-processor that removes Hebrew diacritics (nikud and taamim) from database values.
 *
 * Hebrew diacritics Unicode ranges:
 * - Cantillation marks (taamim): U+0591 to U+05AF
 * - Vowel points (nikud): U+05B0 to U+05BD
 * - Other marks: U+05BF (RAFE), U+05C1-U+05C2 (shin/sin dots),
 *                U+05C4-U+05C5 (upper/lower dots), U+05C7 (QAMATS QATAN)
 */
object HebrewDiacriticsPostProcessor {

    // Regex pattern to match all Hebrew diacritics
    private val DIACRITICS_PATTERN = Regex("[\u0591-\u05BD\u05BF\u05C1\u05C2\u05C4\u05C5\u05C7]")

    /**
     * Removes all Hebrew diacritics (nikud and taamim) from a string.
     */
    fun removeHebrewDiacritics(text: String): String {
        return text.replace(DIACRITICS_PATTERN, "")
    }

    /**
     * Post-processes the database to remove Hebrew diacritics from surface, variant, and base values.
     *
     * @param dbPath Path to the lexical database
     * @param cleanSurfaces Whether to clean surface values (default: true)
     * @param cleanVariants Whether to clean variant values (default: true)
     * @param cleanBases Whether to clean base values (default: true)
     * @return Statistics about the cleaning operation
     */
    fun postProcess(
        dbPath: String,
        cleanSurfaces: Boolean = true,
        cleanVariants: Boolean = true,
        cleanBases: Boolean = true
    ): PostProcessStats {
        val dbFile = File(dbPath)
        if (!dbFile.exists()) {
            throw IllegalArgumentException("Database file not found: $dbPath")
        }

        println("Opening database: $dbPath")
        val driver = JdbcSqliteDriver("jdbc:sqlite:$dbPath")
        Database.Schema.create(driver)
        val database = Database(driver)
        val queries = database.databaseQueries

        var surfacesProcessed = 0
        var surfacesModified = 0
        var variantsProcessed = 0
        var variantsModified = 0
        var basesProcessed = 0
        var basesModified = 0

        // Clean surfaces
        if (cleanSurfaces) {
            println("\nCleaning surface values...")
            val surfaces = queries.selectAllSurfaces().executeAsList()
            surfacesProcessed = surfaces.size

            // Group surfaces by their cleaned value to detect duplicates
            val cleanedGroups = surfaces.groupBy { removeHebrewDiacritics(it.value_) }
            var duplicatesMerged = 0

            cleanedGroups.forEach { (cleanedValue, group) ->
                if (group.size > 1) {
                    // Multiple surfaces map to the same cleaned value - merge them
                    val keeper = group.first()
                    val duplicates = group.drop(1)

                    // Move all variant associations from duplicates to the keeper
                    duplicates.forEach { duplicate ->
                        queries.mergeSurfaceVariants(keeper.id, duplicate.id)
                        queries.deleteSurfaceVariants(duplicate.id)
                        queries.deleteSurface(duplicate.id)
                        duplicatesMerged++
                    }

                    // Update the keeper's value if needed
                    if (keeper.value_ != cleanedValue) {
                        queries.updateSurfaceValue(cleanedValue, keeper.id)
                        surfacesModified++
                        if (surfacesModified <= 5) {
                            println("  Example: '${keeper.value_}' → '$cleanedValue' (merged ${group.size} entries)")
                        }
                    }
                } else {
                    // Single surface - just update if needed
                    val surface = group.first()
                    if (surface.value_ != cleanedValue) {
                        queries.updateSurfaceValue(cleanedValue, surface.id)
                        surfacesModified++
                        if (surfacesModified <= 5) {
                            println("  Example: '${surface.value_}' → '$cleanedValue'")
                        }
                    }
                }
            }
            println("  Surfaces: $surfacesModified modified, $duplicatesMerged duplicates merged")
        }

        // Clean variants
        if (cleanVariants) {
            println("\nCleaning variant values...")
            val variants = queries.selectAllVariants().executeAsList()
            variantsProcessed = variants.size

            // Group variants by their cleaned value to detect duplicates
            val cleanedVariantGroups = variants.groupBy { removeHebrewDiacritics(it.value_) }
            var variantDuplicatesMerged = 0

            cleanedVariantGroups.forEach { (cleanedValue, group) ->
                if (group.size > 1) {
                    // Multiple variants map to the same cleaned value - merge them
                    val keeper = group.first()
                    val duplicates = group.drop(1)

                    duplicates.forEach { duplicate ->
                        queries.mergeVariantSurfaces(keeper.id, duplicate.id)
                        queries.deleteVariantSurfaces(duplicate.id)
                        queries.deleteVariant(duplicate.id)
                        variantDuplicatesMerged++
                    }

                    if (keeper.value_ != cleanedValue) {
                        queries.updateVariantValue(cleanedValue, keeper.id)
                        variantsModified++
                        if (variantsModified <= 5) {
                            println("  Example: '${keeper.value_}' → '$cleanedValue' (merged ${group.size} entries)")
                        }
                    }
                } else {
                    val variant = group.first()
                    if (variant.value_ != cleanedValue) {
                        queries.updateVariantValue(cleanedValue, variant.id)
                        variantsModified++
                        if (variantsModified <= 5) {
                            println("  Example: '${variant.value_}' → '$cleanedValue'")
                        }
                    }
                }
            }
            println("  Variants: $variantsModified modified, $variantDuplicatesMerged duplicates merged")
        }

        // Clean bases (optional, usually not needed)
        if (cleanBases) {
            println("\nCleaning base values...")
            val bases = queries.selectAllBases().executeAsList()
            basesProcessed = bases.size

            // Group bases by their cleaned value to detect duplicates
            val cleanedBaseGroups = bases.groupBy { removeHebrewDiacritics(it.value_) }
            var baseDuplicatesMerged = 0

            cleanedBaseGroups.forEach { (cleanedValue, group) ->
                if (group.size > 1) {
                    // Multiple bases map to the same cleaned value - merge them
                    val keeper = group.first()
                    val duplicates = group.drop(1)

                    duplicates.forEach { duplicate ->
                        queries.mergeBaseSurfaces(keeper.id, duplicate.id)
                        queries.deleteBase(duplicate.id)
                        baseDuplicatesMerged++
                    }

                    if (keeper.value_ != cleanedValue) {
                        queries.updateBaseValue(cleanedValue, keeper.id)
                        basesModified++
                        if (basesModified <= 5) {
                            println("  Example: '${keeper.value_}' → '$cleanedValue' (merged ${group.size} entries)")
                        }
                    }
                } else {
                    val base = group.first()
                    if (base.value_ != cleanedValue) {
                        queries.updateBaseValue(cleanedValue, base.id)
                        basesModified++
                        if (basesModified <= 5) {
                            println("  Example: '${base.value_}' → '$cleanedValue'")
                        }
                    }
                }
            }
            println("  Bases: $basesModified modified, $baseDuplicatesMerged duplicates merged")
        }

        driver.close()

        return PostProcessStats(
            surfacesProcessed = surfacesProcessed,
            surfacesModified = surfacesModified,
            variantsProcessed = variantsProcessed,
            variantsModified = variantsModified,
            basesProcessed = basesProcessed,
            basesModified = basesModified
        )
    }

    data class PostProcessStats(
        val surfacesProcessed: Int,
        val surfacesModified: Int,
        val variantsProcessed: Int,
        val variantsModified: Int,
        val basesProcessed: Int,
        val basesModified: Int
    ) {
        val totalModified: Int
            get() = surfacesModified + variantsModified + basesModified
    }
}

package io.github.kdroidfilter.seforim.magicindexer

import java.io.File
import java.sql.Connection
import java.sql.DriverManager

fun main(args: Array<String>) {
    println("=== Cross-Family Acronym Post-Processor ===\n")

    val dbPath = args.firstOrNull { !it.startsWith("--") } ?: File("build/db/lexical.db").absolutePath
    val dryRun = "--dry-run" in args
    if ("--help" in args || "-h" in args) {
        println(
            """
            Usage: crossfamily [database-path] [--dry-run]

            Removes forms that spell a sibling acronym (same letters except the last one,
            e.g. רמב"ן inside the רמב"ם family) together with that acronym's expansions.
            If no path is provided, defaults to build/db/lexical.db
            """.trimIndent()
        )
        return
    }

    println("Database: $dbPath")
    println("Dry run: $dryRun")
    val stats = CrossFamilyPostProcessor.postProcess(dbPath, dryRun)
    println("\n=== Post-Processing Complete ===")
    println("Variant links removed: ${stats.linksRemoved}")
    println("Orphan variants deleted: ${stats.variantsDeleted}")
    println("Surfaces moved to their own family: ${stats.surfacesMoved}")
}

/**
 * Removes forms filed under an acronym family that spell a sibling acronym (רמב"ן in רמב"ם),
 * as the LLM lists every completion of a truncated acronym such as "ורמב" under one base.
 */
object CrossFamilyPostProcessor {

    data class Variant(val id: Long, val value: String)
    data class Surface(val id: Long, val value: String, val variants: List<Variant>)
    data class Family(val id: Long, val base: String, val surfaces: List<Surface>)

    /** A form of [familyId] spelling [foreignKey]; [variantId] is null when the surface itself leaks. */
    data class Leak(
        val familyId: Long,
        val surfaceId: Long,
        val variantId: Long?,
        val foreignKey: String,
        val targetFamilyId: Long
    )

    data class Stats(val linksRemoved: Int, val variantsDeleted: Int, val surfacesMoved: Int)

    private val NIKUD = Regex("[֑-ׇ]")
    private val QUOTES = Regex("[\"'`׳״]")
    private val ACRONYM = Regex("^[א-ת]+(?:\"|״|''|׳׳)[א-ת]$")
    private const val EDGE = "()[]{}.,:;!?-־ <>"
    private const val PREFIXES = "והבכלמשד"
    private val FINALS = mapOf('ך' to 'כ', 'ם' to 'מ', 'ן' to 'נ', 'ף' to 'פ', 'ץ' to 'צ')
    private val GEMATRIA = "אבגדהוזחטיכלמנסעפצקרשת".toList()
        .zip(listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400))
        .toMap()

    private fun clean(value: String): String = NIKUD.replace(value, "").trim { it in EDGE }

    /** Comparison key: no nikud, punctuation or gershayim, final letters as regular ones. */
    fun fold(value: String): String =
        QUOTES.replace(clean(value), "")
            .map { FINALS[it] ?: it }
            .joinToString("")
            .split(Regex("\\s+"))
            .filter { it.isNotEmpty() }
            .joinToString(" ")

    fun isAcronym(value: String): Boolean = ACRONYM.matches(clean(value))

    /** [key] and the keys left after dropping up to three leading prefix letters. */
    fun strips(key: String): List<String> {
        val out = mutableListOf(key)
        while (out.size <= 3 && out.last().length > 3 && out.last()[0] in PREFIXES) {
            out += out.last().drop(1)
        }
        return out
    }

    fun initials(value: String): String? {
        val words = fold(value).split(' ').filter { it.isNotEmpty() }
        return if (words.size >= 2) words.joinToString("") { it.take(1) } else null
    }

    // Gematria numbers (קנ"א, קנ"ו) differ in the last letter by nature, not by mistake.
    private fun isNumeral(key: String): Boolean {
        val values = key.map { GEMATRIA[it] ?: return false }
        return values.zipWithNext().all { (a, b) -> a > b }
    }

    private fun isSibling(a: String, b: String): Boolean =
        a.length == b.length && a != b && a.dropLast(1) == b.dropLast(1)

    /** Finds forms that spell a sibling acronym of their family's key. */
    fun findLeaks(families: Collection<Family>): List<Leak> {
        val attested = HashSet<String>()
        families.forEach { family ->
            if (isAcronym(family.base)) attested += fold(family.base)
            family.surfaces.forEach { if (isAcronym(it.value)) attested += fold(it.value) }
        }

        val keysByFamily = HashMap<Long, Set<String>>()
        families.forEach { family ->
            val keys = acronymKeys(family, attested)
            if (keys.isNotEmpty()) keysByFamily[family.id] = keys
        }
        // A key that is also an ordinary word (אמה, ראי) is not evidence of another acronym.
        val words = families
            .filter { it.id !in keysByFamily }
            .map { fold(it.base) }
            .filter { ' ' !in it && it !in attested }
            .toHashSet()
        val familiesByKey = HashMap<String, MutableList<Family>>()
        families.forEach { family ->
            keysByFamily[family.id]?.forEach { familiesByKey.getOrPut(it) { mutableListOf() } += family }
        }
        val strong = familiesByKey.keys.filter { it in attested && it !in words }.toHashSet()
        val strongByStem = strong.groupBy { it.dropLast(1) }

        val leaks = mutableListOf<Leak>()
        families.forEach { family ->
            val keys = keysByFamily[family.id] ?: return@forEach
            val siblings = keys.flatMap { key -> strongByStem[key.dropLast(1)].orEmpty().filter { isSibling(key, it) } }
                .toSet() - keys
            if (siblings.isEmpty()) return@forEach
            val own = keys.flatMap { strips(it) }.toSet()

            fun foreignKey(value: String): String? {
                val folded = fold(value)
                if (folded.isEmpty()) return null
                if (' ' in folded) return initials(value)?.takeIf { it in siblings }
                val candidates = strips(folded)
                if (candidates.any { it in own }) return null
                return candidates.firstOrNull { it in siblings }
            }

            fun target(key: String): Long =
                familiesByKey.getValue(key).sortedWith(compareBy({ !isAcronym(it.base) }, { it.id })).first().id

            family.surfaces.forEach { surface ->
                val surfaceKey = foreignKey(surface.value)
                if (surfaceKey != null) {
                    leaks += Leak(family.id, surface.id, null, surfaceKey, target(surfaceKey))
                    return@forEach
                }
                surface.variants.forEach { variant ->
                    foreignKey(variant.value)?.let {
                        leaks += Leak(family.id, surface.id, variant.id, it, target(it))
                    }
                }
            }
        }
        return leaks
    }

    private fun acronymKeys(family: Family, attested: Set<String>): Set<String> {
        val base = fold(family.base)
        val key = if (' ' in base) {
            val ini = initials(family.base)
            ini.takeIf { family.surfaces.any { s -> ' ' !in fold(s.value) && ini in strips(fold(s.value)) } }
        } else {
            val shaped = family.surfaces.count { isAcronym(it.value) }
            base.takeIf {
                isAcronym(family.base) || (base in attested && family.surfaces.isNotEmpty() && shaped * 2 >= family.surfaces.size)
            }
        }
        return setOfNotNull(key?.takeIf { it.length >= 3 && !isNumeral(it) })
    }

    fun loadFamilies(connection: Connection): List<Family> {
        val variantsBySurface = HashMap<Long, MutableList<Variant>>()
        connection.createStatement().use { st ->
            st.executeQuery(
                "SELECT sv.surface_id, v.id, v.value FROM surface_variant sv JOIN variant v ON v.id = sv.variant_id"
            ).use { rs ->
                while (rs.next()) {
                    variantsBySurface.getOrPut(rs.getLong(1)) { mutableListOf() } += Variant(rs.getLong(2), rs.getString(3))
                }
            }
        }
        val surfacesByBase = HashMap<Long, MutableList<Surface>>()
        connection.createStatement().use { st ->
            st.executeQuery("SELECT id, value, base_id FROM surface ORDER BY id").use { rs ->
                while (rs.next()) {
                    val id = rs.getLong(1)
                    surfacesByBase.getOrPut(rs.getLong(3)) { mutableListOf() } +=
                        Surface(id, rs.getString(2), variantsBySurface[id].orEmpty())
                }
            }
        }
        val families = mutableListOf<Family>()
        connection.createStatement().use { st ->
            st.executeQuery("SELECT id, value FROM base ORDER BY id").use { rs ->
                while (rs.next()) {
                    val id = rs.getLong(1)
                    families += Family(id, rs.getString(2), surfacesByBase[id].orEmpty())
                }
            }
        }
        return families
    }

    fun postProcess(dbPath: String, dryRun: Boolean = false): Stats {
        require(File(dbPath).exists()) { "Database file not found: $dbPath" }
        DriverManager.getConnection("jdbc:sqlite:$dbPath").use { connection ->
            val families = loadFamilies(connection)
            val byId = families.associateBy { it.id }
            val leaks = findLeaks(families)

            leaks.forEach { leak ->
                val family = byId.getValue(leak.familyId)
                val surface = family.surfaces.first { it.id == leak.surfaceId }
                val form = leak.variantId?.let { id -> surface.variants.first { it.id == id }.value }
                println(
                    "  [${family.base}] ${surface.value}${form?.let { " | $it" } ?: ""}" +
                        " -> ${byId.getValue(leak.targetFamilyId).base}"
                )
            }
            if (dryRun) return Stats(leaks.count { it.variantId != null }, 0, leaks.count { it.variantId == null })

            connection.autoCommit = false
            var links = 0
            var moved = 0
            val touched = HashSet<Long>()
            connection.prepareStatement("DELETE FROM surface_variant WHERE surface_id = ? AND variant_id = ?").use { del ->
                connection.prepareStatement("UPDATE surface SET base_id = ? WHERE id = ?").use { move ->
                    leaks.forEach { leak ->
                        if (leak.variantId != null) {
                            del.setLong(1, leak.surfaceId)
                            del.setLong(2, leak.variantId)
                            links += del.executeUpdate()
                            touched += leak.variantId
                        } else {
                            move.setLong(1, leak.targetFamilyId)
                            move.setLong(2, leak.surfaceId)
                            moved += move.executeUpdate()
                        }
                    }
                }
            }
            var orphans = 0
            connection.prepareStatement(
                "DELETE FROM variant WHERE id = ? AND NOT EXISTS (SELECT 1 FROM surface_variant WHERE variant_id = ?)"
            ).use { del ->
                touched.forEach { id ->
                    del.setLong(1, id)
                    del.setLong(2, id)
                    orphans += del.executeUpdate()
                }
            }
            connection.commit()
            return Stats(links, orphans, moved)
        }
    }
}

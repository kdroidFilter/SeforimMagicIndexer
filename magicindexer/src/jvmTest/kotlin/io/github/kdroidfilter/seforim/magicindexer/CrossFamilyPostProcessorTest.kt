package io.github.kdroidfilter.seforim.magicindexer

import io.github.kdroidfilter.seforim.magicindexer.CrossFamilyPostProcessor.Family
import io.github.kdroidfilter.seforim.magicindexer.CrossFamilyPostProcessor.Surface
import io.github.kdroidfilter.seforim.magicindexer.CrossFamilyPostProcessor.Variant
import java.io.File
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CrossFamilyPostProcessorTest {

    private var nextId = 1000L

    private fun surface(value: String, vararg variants: String) =
        Surface(nextId++, value, variants.map { Variant(nextId++, it) })

    private fun family(id: Long, base: String, vararg surfaces: Surface) = Family(id, base, surfaces.toList())

    private fun leakedForms(families: List<Family>): Set<String> {
        val surfaces = families.flatMap { it.surfaces }.associateBy { it.id }
        return CrossFamilyPostProcessor.findLeaks(families).map { leak ->
            val surface = surfaces.getValue(leak.surfaceId)
            leak.variantId?.let { id -> surface.variants.first { it.id == id }.value } ?: surface.value
        }.toSet()
    }

    private val rambam = family(
        1, "רמב\"ם",
        surface("ורמב", "ורמב\"ם", "רמבם", "רמבן", "ורמבן", "רבי משה בן נחמן", "רבי משה בן מימון", "ורמב"),
        surface("ברמב\"ם", "ברמב\"ם", "רמב\"ם", "רבי משה בן מימון"),
        surface("הרמב\"ם", "הרמב\"ם", "רמבם"),
        surface("להרמב\"ם", "להרמב\"ם", "לרמב\"ם", "לרמבם"),
    )
    private val ramban = family(2, "רמב\"ן", surface("רמב\"ן", "רמבן"), surface("הרמב\"ן", "הרמבן"))
    private val rambanExpansion = family(3, "רבי משה בן נחמן", surface("והרמב\"ן", "רמבן"), surface("רמבן"))

    @Test
    fun siblingAcronymAndItsExpansionAreRemoved() {
        assertEquals(
            setOf("רמבן", "ורמבן", "רבי משה בן נחמן"),
            leakedForms(listOf(rambam, ramban, rambanExpansion))
        )
    }

    @Test
    fun surfaceSpellingSiblingMovesToItsFamily() {
        val rashba = family(10, "רשב\"א", surface("רשב\"א", "רשבא"), surface("הרשב\"ם", "רשבם"))
        val rashbam = family(11, "רשב\"ם", surface("רשב\"ם", "רשבם"), surface("ורשב\"ם"))
        val leaks = CrossFamilyPostProcessor.findLeaks(listOf(rashba, rashbam))
        assertEquals(1, leaks.size)
        assertEquals(null, leaks[0].variantId)
        assertEquals(11L, leaks[0].targetFamilyId)
    }

    @Test
    fun wordFamiliesNumeralsAndAlternativeNamesAreKept() {
        val see = family(20, "ראה", surface("ראי", "ראי", "ראם"), surface("וראם", "ראם"))
        val raam = family(21, "רא\"ם", surface("רא\"ם", "ראם"), surface("הרא\"ם"))
        val n151 = family(22, "קנ\"א", surface("קנ\"א", "קנ\"ו", "קנו"))
        val n156 = family(23, "קנ\"ו", surface("קנ\"ו"), surface("בקנ\"ו"))
        val rosh = family(24, "רא\"ש", surface("רא\"ש", "אשיר\"י", "אשירי"), surface("הרא\"ש"))
        val asheri = family(25, "אשיר\"י", surface("אשיר\"י"), surface("האשיר\"י"))
        assertTrue(leakedForms(listOf(see, raam, n151, n156, rosh, asheri)).isEmpty())
    }

    @Test
    fun postProcessRemovesLinksAndOrphanVariantsOnly() {
        val file = File.createTempFile("lexical", ".db").apply { deleteOnExit() }
        DriverManager.getConnection("jdbc:sqlite:${file.path}").use { c ->
            c.createStatement().use { st ->
                st.executeUpdate("CREATE TABLE base (id INTEGER PRIMARY KEY, value TEXT NOT NULL UNIQUE)")
                st.executeUpdate("CREATE TABLE surface (id INTEGER PRIMARY KEY, value TEXT NOT NULL UNIQUE, base_id INTEGER NOT NULL, notes TEXT)")
                st.executeUpdate("CREATE TABLE variant (id INTEGER PRIMARY KEY, value TEXT NOT NULL UNIQUE)")
                st.executeUpdate("CREATE TABLE surface_variant (surface_id INTEGER NOT NULL, variant_id INTEGER NOT NULL, PRIMARY KEY (surface_id, variant_id))")
                st.executeUpdate("INSERT INTO base VALUES (1, 'רמב\"ם'), (2, 'רמב\"ן')")
                st.executeUpdate("INSERT INTO surface VALUES (1, 'ורמב', 1, NULL), (2, 'ברמב\"ם', 1, NULL), (3, 'רמב\"ן', 2, NULL), (4, 'הרמב\"ן', 2, NULL)")
                st.executeUpdate("INSERT INTO variant VALUES (1, 'רמבם'), (2, 'רמבן'), (3, 'ורמבן'), (4, 'ברמב\"ם')")
                st.executeUpdate("INSERT INTO surface_variant VALUES (1, 1), (1, 2), (1, 3), (2, 4), (3, 2)")
            }
        }

        val stats = CrossFamilyPostProcessor.postProcess(file.path)

        assertEquals(CrossFamilyPostProcessor.Stats(linksRemoved = 2, variantsDeleted = 1, surfacesMoved = 0), stats)
        DriverManager.getConnection("jdbc:sqlite:${file.path}").use { c ->
            c.createStatement().use { st ->
                val forms = mutableSetOf<String>()
                st.executeQuery(
                    "SELECT v.value FROM surface s JOIN surface_variant sv ON sv.surface_id = s.id " +
                        "JOIN variant v ON v.id = sv.variant_id WHERE s.base_id = 1"
                ).use { rs -> while (rs.next()) forms += rs.getString(1) }
                assertEquals(setOf("רמבם", "ברמב\"ם"), forms)
            }
        }
        assertEquals(CrossFamilyPostProcessor.Stats(0, 0, 0), CrossFamilyPostProcessor.postProcess(file.path))
    }
}

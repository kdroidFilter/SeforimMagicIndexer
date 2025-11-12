package io.github.kdroidfilter.seforim.magicindexer.repository

import io.github.kdroidfilter.seforim.magicindexer.db.DatabaseQueries
import io.github.kdroidfilter.seforim.magicindexer.db.Surface
import io.github.kdroidfilter.seforim.magicindexer.db.Base

/**
 * Extension functions for working directly with SQLDelight generated types.
 */

/**
 * Gets the base value for this surface.
 */
fun Surface.getBase(queries: DatabaseQueries): String? {
    return queries.selectBaseById(this.base_id)
        .executeAsOneOrNull()
        ?.value_
}

/**
 * Gets all variants for this surface.
 */
fun Surface.getVariants(queries: DatabaseQueries): List<String> {
    return queries.selectVariantsForSurface(this.id)
        .executeAsList()
        .map { it.value_ }
}

/**
 * Gets all related surfaces (surfaces with the same base).
 */
fun Surface.getRelatedSurfaces(queries: DatabaseQueries): List<Surface> {
    return queries.selectSurfacesForBase(this.base_id)
        .executeAsList()
        .filter { it.id != this.id }
}



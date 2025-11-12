package io.github.kdroidfilter.seforim.magicindexer.model

import kotlinx.serialization.Serializable

/**
 * Represents a lexical normalization entry with surface form, base form, and variants.
 *
 * @property surface The surface form as it appears in the text
 * @property base The normalized base form
 * @property variants List of all possible variant forms
 * @property notes Optional notes about this entry (e.g., explanations, etymologies)
 */
@Serializable
data class LexicalEntry(
    val surface: String,
    val base: String,
    val variants: List<String>,
    val notes: String? = null
)

/**
 * Container for a collection of lexical entries.
 *
 * @property entries List of lexical normalization entries
 */
@Serializable
data class LexicalEntries(
    val entries: List<LexicalEntry>
)

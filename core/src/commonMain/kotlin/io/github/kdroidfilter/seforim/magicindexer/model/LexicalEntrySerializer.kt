package io.github.kdroidfilter.seforim.magicindexer.model

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.json.Json
import kotlinx.serialization.encodeToString
import kotlinx.serialization.decodeFromString

/**
 * Utility object for serializing and deserializing lexical entries to/from JSON.
 */
object LexicalEntrySerializer {

    /**
     * JSON configuration with pretty printing and lenient parsing.
     */
    @OptIn(ExperimentalSerializationApi::class)
    private val json = Json {
        prettyPrint = true
        ignoreUnknownKeys = true
        encodeDefaults = true
        allowTrailingComma = true
    }

    /**
     * Deserialize JSON string to LexicalEntries object.
     *
     * @param jsonString The JSON string to deserialize
     * @return LexicalEntries object
     */
    fun fromJson(jsonString: String): LexicalEntries {
        return json.decodeFromString<LexicalEntries>(jsonString)
    }

    /**
     * Serialize LexicalEntries object to JSON string.
     *
     * @param entries The LexicalEntries object to serialize
     * @return JSON string
     */
    fun toJson(entries: LexicalEntries): String {
        return json.encodeToString(entries)
    }

    /**
     * Deserialize JSON string to a single LexicalEntry.
     *
     * @param jsonString The JSON string to deserialize
     * @return LexicalEntry object
     */
    fun entryFromJson(jsonString: String): LexicalEntry {
        return json.decodeFromString<LexicalEntry>(jsonString)
    }

    /**
     * Serialize a single LexicalEntry to JSON string.
     *
     * @param entry The LexicalEntry to serialize
     * @return JSON string
     */
    fun entryToJson(entry: LexicalEntry): String {
        return json.encodeToString(entry)
    }
}

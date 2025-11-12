package io.github.kdroidfilter.seforim.magicindexer

import java.nio.file.Files
import java.nio.file.Paths

fun main() {
    val uri = object {}.javaClass.getResource("/system-prompt.txt")?.toURI()
        ?: error("Fichier non trouvé !")
    val text = Files.readString(Paths.get(uri))
    println(text)
}
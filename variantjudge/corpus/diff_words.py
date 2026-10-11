#!/usr/bin/env python3
"""Diff corpus words against the dictionary: emit words present in the corpus but
absent from lexical.db (under the same canonical normalization).

Usage:
    diff_words.py <lexical_db> <corpus_words.tsv.db> <out_new.tsv> [min_len=2] [min_freq=1]

Existing dictionary words = normalized( surface.value ∪ base.value ∪ variant.value ).
A corpus word is "new" if its normalized form is not in that set.

Read-only on both databases. Output TSV "<word>\t<freq>" sorted by descending freq.
"""
import re
import sqlite3
import sys

COMBINING_RE = re.compile("[֑-ׇֽֿׁׂׅׄ]")
NON_LETTER_RE = re.compile("[^א-ת]+")


def norm_tokens(value):
    if not value:
        return []
    return NON_LETTER_RE.sub(" ", COMBINING_RE.sub("", value)).split()


def main():
    if len(sys.argv) < 4:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    lex_db, corpus_db, out_tsv = sys.argv[1], sys.argv[2], sys.argv[3]
    min_len = int(sys.argv[4]) if len(sys.argv) > 4 else 2
    min_freq = int(sys.argv[5]) if len(sys.argv) > 5 else 1

    lex = sqlite3.connect(lex_db)
    lex.execute("PRAGMA query_only = ON")
    existing = set()
    for table, col in (("surface", "value"), ("base", "value"), ("variant", "value")):
        for (val,) in lex.execute(f"SELECT {col} FROM {table}"):
            existing.update(norm_tokens(val))
    lex.close()
    print(f"dictionary: {len(existing)} unique normalized words", file=sys.stderr)

    cor = sqlite3.connect(corpus_db)
    cor.execute("PRAGMA query_only = ON")
    new = []
    scanned = 0
    for word, freq in cor.execute("SELECT value, freq FROM word ORDER BY freq DESC"):
        scanned += 1
        if len(word) < min_len or freq < min_freq:
            continue
        if word not in existing:
            new.append((word, freq))
    cor.close()

    with open(out_tsv, "w", encoding="utf-8") as f:
        for word, freq in new:
            f.write(f"{word}\t{freq}\n")
    print(
        f"corpus scanned: {scanned} | new words (len>={min_len}, freq>={min_freq}): "
        f"{len(new)} -> {out_tsv}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()

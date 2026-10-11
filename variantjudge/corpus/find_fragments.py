#!/usr/bin/env python3
"""Detect corpus "words" that are really abbreviation fragments.

extract_corpus_words.py treated geresh/gershayim as separators, so an abbreviation
like לפמ"ש or הלכ' was counted as the words לפמ + ש and הלכ. This rescans the corpus
keeping gershayim inside tokens and, for each target word, counts how often it
appears as a standalone word vs as a piece of an abbreviation.

Usage:
    find_fragments.py <seforim_db> <words.tsv> <out_tsv>

Output TSV: word, standalone, in_abbreviation, standalone_ratio (sorted by ratio).
"""
import html
import re
import sqlite3
import sys
from collections import Counter

TAG_RE = re.compile(r"<[^>]+>")
COMBINING_RE = re.compile("[֑-ׇֽֿׁׂׅׄ]")
# Tokens keep geresh/gershayim (ASCII and Hebrew) so abbreviations stay whole.
TOKEN_RE = re.compile("[א-ת\"'׳״]+")
QUOTES = "\"'׳״"
LETTERS_RE = re.compile("[א-ת]+")


def main() -> None:
    if len(sys.argv) != 4:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    db_path, words_tsv, out_tsv = sys.argv[1:]
    targets = {line.split("\t", 1)[0].strip() for line in open(words_tsv, encoding="utf-8")}
    standalone, in_abbr = Counter(), Counter()

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA query_only = ON")
    cur = conn.cursor()
    cur.execute("SELECT content FROM line")
    processed = 0
    while rows := cur.fetchmany(20000):
        for (content,) in rows:
            if not content:
                continue
            text = COMBINING_RE.sub("", html.unescape(TAG_RE.sub(" ", content)))
            for tok in TOKEN_RE.findall(text):
                # Opening quotation marks and closing double quotes are punctuation, not gershayim
                tok = tok.lstrip(QUOTES).rstrip("\"״")
                pieces = LETTERS_RE.findall(tok)
                counter = in_abbr if any(q in tok for q in QUOTES) else standalone
                for p in pieces:
                    if p in targets:
                        counter[p] += 1
        processed += len(rows)
        if processed % 1000000 < 20000:
            print(f"  {processed} lines", file=sys.stderr)

    with open(out_tsv, "w", encoding="utf-8") as f:
        rows = []
        for w in targets:
            s, a = standalone[w], in_abbr[w]
            rows.append((w, s, a, s / (s + a) if s + a else 0.0))
        for w, s, a, r in sorted(rows, key=lambda x: x[3]):
            f.write(f"{w}\t{s}\t{a}\t{r:.3f}\n")
    print(f"wrote {len(targets)} rows to {out_tsv}", file=sys.stderr)


if __name__ == "__main__":
    main()

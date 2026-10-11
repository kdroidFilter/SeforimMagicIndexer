#!/usr/bin/env python3
"""Extract unique normalized Hebrew words from the Seforim text DB (line.content).

Usage:
    extract_corpus_words.py <seforim_db> <out_tsv>

Normalization (same canonical form used for diffing against the dictionary):
- Strip HTML tags + unescape entities.
- DELETE non-spacing combining marks (taamim, nikud, rafe, shin/sin dots, qamats
  qatan) so a word's letters rejoin instead of splitting on every vowel point.
- Treat maqaf / paseq / sof-pasuq / nun-hafukha and any non Hebrew-letter char
  (Latin, digits, punctuation) as separators.
- Skip abbreviations (a geresh/gershayim inside or after the letters: לפמ"ש, הלכ'):
  splitting them on the quote counted their pieces (לפמ, ש) as words.
- Keep only base Hebrew letters U+05D0..U+05EA.

Output: TSV "<word>\t<frequency>" sorted by descending frequency, plus a companion
SQLite file <out_tsv>.db with table word(value TEXT PRIMARY KEY, freq INTEGER) for fast diff.
"""
import html
import re
import sqlite3
import sys
from collections import Counter

TAG_RE = re.compile(r"<[^>]+>")
# Non-spacing combining marks to DELETE: taamim+nikud U+0591-U+05BD, rafe U+05BF,
# shin/sin dots U+05C1-U+05C2, marks U+05C4-U+05C5, qamats qatan U+05C7.
# NOT included (kept as separators): U+05BE maqaf, U+05C0 paseq, U+05C3 sof-pasuq,
# U+05C6 nun-hafukha.
COMBINING_RE = re.compile("[֑-ׇֽֿׁׂׅׄ]")
# Tokens keep geresh/gershayim (ASCII and Hebrew) so abbreviations can be told apart.
TOKEN_RE = re.compile("[א-ת\"'׳״]+")
QUOTES = "\"'׳״"


def normalize_line(content: str) -> list:
    text = TAG_RE.sub(" ", content)
    text = html.unescape(text)
    text = COMBINING_RE.sub("", text)
    words = []
    for tok in TOKEN_RE.findall(text):
        # Opening quotation marks and closing double quotes are punctuation, not gershayim
        tok = tok.lstrip(QUOTES).rstrip("\"״")
        if tok and not any(q in tok for q in QUOTES):
            words.append(tok)
    return words


def main() -> None:
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        sys.exit(2)

    db_path, out_tsv = sys.argv[1], sys.argv[2]
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA query_only = ON")
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM line")
    total = cur.fetchone()[0]

    counts = Counter()
    cur.execute("SELECT content FROM line")
    processed = 0
    while True:
        rows = cur.fetchmany(20000)
        if not rows:
            break
        for (content,) in rows:
            if content:
                counts.update(normalize_line(content))
        processed += len(rows)
        if processed % 500000 < 20000:
            print(f"  {processed}/{total} lines, {len(counts)} unique words", file=sys.stderr)

    conn.close()
    print(f"done scan: {processed} lines, {len(counts)} unique normalized words", file=sys.stderr)

    ordered = counts.most_common()
    with open(out_tsv, "w", encoding="utf-8") as f:
        for word, freq in ordered:
            f.write(f"{word}\t{freq}\n")

    side = sqlite3.connect(out_tsv + ".db")
    side.execute("CREATE TABLE IF NOT EXISTS word (value TEXT PRIMARY KEY, freq INTEGER)")
    side.execute("DELETE FROM word")
    side.executemany("INSERT OR REPLACE INTO word(value, freq) VALUES (?, ?)", ordered)
    side.commit()
    side.close()
    print(f"wrote {len(ordered)} words to {out_tsv} and {out_tsv}.db", file=sys.stderr)


if __name__ == "__main__":
    main()

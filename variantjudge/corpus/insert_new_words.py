#!/usr/bin/env python3
"""Insert new corpus words into lexical.db as fresh dictionary entries so the
existing variant-judge/enrich loop can generate their variants.

Usage:
    insert_new_words.py <lexical_db> <new_words.tsv> [limit]

For each word W (TSV "<word>\t<freq>", takes the first column):
- INSERT OR IGNORE base(value=W)
- INSERT OR IGNORE surface(value=W, base_id=<W's base>)
- INSERT OR IGNORE variant(value=W)  + surface_variant link  (identity seed so the
  enrich loop, which skips zero-variant surfaces, picks the surface up)
- Leaves the surface UNJUDGED (no variant_judgment row) so judge_loop enriches it.

WRITES to the DB. Back up first. Idempotent: re-running skips words already present.
"""
import sqlite3
import sys


def main():
    if len(sys.argv) < 3:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    db_path, tsv = sys.argv[1], sys.argv[2]
    limit = int(sys.argv[3]) if len(sys.argv) > 3 else None

    words = []
    with open(tsv, encoding="utf-8") as f:
        for line in f:
            w = line.split("\t", 1)[0].strip()
            if w:
                words.append(w)
            if limit and len(words) >= limit:
                break

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    inserted_surface = 0
    seeded = 0
    skipped_existing = 0
    for w in words:
        cur.execute("SELECT id FROM surface WHERE value = ?", (w,))
        if cur.fetchone():
            skipped_existing += 1
            continue
        cur.execute("INSERT OR IGNORE INTO base(value) VALUES (?)", (w,))
        cur.execute("SELECT id FROM base WHERE value = ?", (w,))
        base_id = cur.fetchone()[0]
        cur.execute("INSERT INTO surface(value, base_id) VALUES (?, ?)", (w, base_id))
        surface_id = cur.lastrowid
        inserted_surface += 1
        cur.execute("INSERT OR IGNORE INTO variant(value) VALUES (?)", (w,))
        cur.execute("SELECT id FROM variant WHERE value = ?", (w,))
        variant_id = cur.fetchone()[0]
        cur.execute(
            "INSERT OR IGNORE INTO surface_variant(surface_id, variant_id) VALUES (?, ?)",
            (surface_id, variant_id),
        )
        if cur.rowcount > 0:
            seeded += 1

    conn.commit()
    conn.close()
    print(
        f"inserted {inserted_surface} new surfaces ({seeded} identity-seeded), "
        f"{skipped_existing} already existed",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()

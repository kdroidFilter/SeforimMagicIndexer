#!/usr/bin/env python3
"""Delete abbreviation fragments (from find_fragments.py) inserted as words in lexical.db.

Usage:
    delete_fragments.py <lexical_db> <fragments.tsv> [max_ratio=0.1]

A word whose standalone ratio is below max_ratio is a fragment: its surface, its
variant links and judgment are deleted, then the bases and variants it alone used.
WRITES to the DB. Back up first.
"""
import sqlite3
import sys


def main() -> None:
    if len(sys.argv) < 3:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    db, tsv = sys.argv[1], sys.argv[2]
    max_ratio = float(sys.argv[3]) if len(sys.argv) > 3 else 0.1
    words = [
        cols[0]
        for cols in (line.rstrip("\n").split("\t") for line in open(tsv, encoding="utf-8"))
        if float(cols[3]) < max_ratio
    ]

    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute("CREATE TEMP TABLE frag(id INTEGER PRIMARY KEY)")
    cur.executemany("INSERT OR IGNORE INTO frag SELECT id FROM surface WHERE value = ?", [(w,) for w in words])
    cur.execute("CREATE TEMP TABLE frag_variant AS SELECT DISTINCT variant_id AS id FROM surface_variant WHERE surface_id IN (SELECT id FROM frag)")
    cur.execute("CREATE TEMP TABLE frag_base AS SELECT DISTINCT base_id AS id FROM surface WHERE id IN (SELECT id FROM frag)")

    links = cur.execute("DELETE FROM surface_variant WHERE surface_id IN (SELECT id FROM frag)").rowcount
    cur.execute("DELETE FROM variant_judgment WHERE surface_id IN (SELECT id FROM frag)")
    surfaces = cur.execute("DELETE FROM surface WHERE id IN (SELECT id FROM frag)").rowcount
    variants = cur.execute(
        "DELETE FROM variant WHERE id IN (SELECT id FROM frag_variant) "
        "AND id NOT IN (SELECT variant_id FROM surface_variant)"
    ).rowcount
    bases = cur.execute(
        "DELETE FROM base WHERE id IN (SELECT id FROM frag_base) AND id NOT IN (SELECT base_id FROM surface)"
    ).rowcount
    conn.commit()
    conn.close()
    print(f"deleted {surfaces} surfaces, {links} links, {variants} orphan variants, {bases} orphan bases", file=sys.stderr)


if __name__ == "__main__":
    main()

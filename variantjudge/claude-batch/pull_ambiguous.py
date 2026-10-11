#!/usr/bin/env python3
"""Pull ambiguous-surface candidates (issue #3) from lexical.db and emit as JSON.

A candidate is a surface S (primary lemma B1 = surface.base_id) whose value is also
linked as a variant to surfaces of another lemma B2. The judge decides, per B2,
whether S is a genuine form of B2 too (פנינו: base פנה, also "our face" of פנים).

Usage:
    pull_ambiguous.py <db_path> [forms_per_lemma=12]

Output (stdout):
    {"items": [
        {"surface_id": 1, "surface": "פנינו",
         "own_base": "פנה", "own_forms": ["פנה", "פנו", ...],
         "candidates": [{"base_id": 7, "base": "פנים", "forms": ["פנים", "פניו", ...]}]},
        ...
    ]}

Surfaces already present in surface_extra_base are skipped (resumable).
"""
import json
import sqlite3
import sys


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    db = sys.argv[1]
    per_lemma = int(sys.argv[2]) if len(sys.argv) > 2 else 12

    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS surface_extra_base ("
        "surface_id INTEGER NOT NULL REFERENCES surface(id) ON UPDATE CASCADE ON DELETE CASCADE, "
        "base_id INTEGER NOT NULL REFERENCES base(id) ON UPDATE CASCADE ON DELETE CASCADE, "
        "PRIMARY KEY (surface_id, base_id))"
    )
    conn.execute("CREATE TABLE IF NOT EXISTS ambiguity_judgment (surface_id INTEGER PRIMARY KEY, judged_at INTEGER NOT NULL)")
    cur = conn.cursor()

    pairs = cur.execute(
        """
        SELECT s.id, s.value, s.base_id, s2.base_id
        FROM surface s
        JOIN variant v ON v.value = s.value
        JOIN surface_variant sv ON sv.variant_id = v.id
        JOIN surface s2 ON s2.id = sv.surface_id
        LEFT JOIN ambiguity_judgment aj ON aj.surface_id = s.id
        WHERE s2.base_id <> s.base_id AND aj.surface_id IS NULL
        GROUP BY s.id, s2.base_id
        ORDER BY s.id, s2.base_id
        """
    ).fetchall()

    base_value = dict(cur.execute("SELECT id, value FROM base"))
    forms_cache = {}

    def forms(base_id):
        if base_id not in forms_cache:
            forms_cache[base_id] = [
                r[0]
                for r in cur.execute(
                    "SELECT value FROM surface WHERE base_id = ? ORDER BY length(value), id LIMIT ?",
                    (base_id, per_lemma),
                )
            ]
        return forms_cache[base_id]

    items, by_surface = [], {}
    for sid, sval, own, other in pairs:
        item = by_surface.get(sid)
        if item is None:
            item = {
                "surface_id": sid,
                "surface": sval,
                "own_base": base_value[own],
                "own_forms": forms(own),
                "candidates": [],
            }
            by_surface[sid] = item
            items.append(item)
        item["candidates"].append({"base_id": other, "base": base_value[other], "forms": forms(other)})

    json.dump({"items": items}, sys.stdout, ensure_ascii=False)
    print(f"{len(items)} surfaces, {len(pairs)} candidate pairs", file=sys.stderr)


if __name__ == "__main__":
    main()

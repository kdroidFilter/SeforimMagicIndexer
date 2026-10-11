#!/usr/bin/env python3
"""Pull a batch of unjudged surfaces from lexical.db and emit as JSON.

Usage:
    pull_batch.py <db_path> [limit=50] [offset=0]

Output (stdout): a JSON object with key "items":
    {"items": [
        {"surface_id": 42, "surface": "...", "base": "...", "variants": ["...", ...]},
        ...
    ]}

Surfaces with zero variants are skipped (nothing to judge).
"""
import json
import sqlite3
import sys


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        sys.exit(2)

    db = sys.argv[1]
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 50

    conn = sqlite3.connect(db)
    cur = conn.cursor()

    cur.execute(
        """
        SELECT s.id, s.value, b.value
        FROM surface AS s
        JOIN base AS b ON b.id = s.base_id
        LEFT JOIN variant_judgment AS vj ON vj.surface_id = s.id
        WHERE vj.surface_id IS NULL
        ORDER BY s.id
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()

    items = []
    for surface_id, surface, base in rows:
        cur.execute(
            """
            SELECT v.value
            FROM surface_variant AS sv
            JOIN variant AS v ON v.id = sv.variant_id
            WHERE sv.surface_id = ?
            ORDER BY v.id
            """,
            (surface_id,),
        )
        variants = [r[0] for r in cur.fetchall()]
        if not variants:
            # Mark as judged with 0 removals — no work to do.
            cur.execute(
                """
                INSERT OR REPLACE INTO variant_judgment(surface_id, judged_at, removed_count)
                VALUES (?, strftime('%s', 'now') * 1000, 0)
                """,
                (surface_id,),
            )
            continue
        items.append({
            "surface_id": surface_id,
            "surface": surface,
            "base": base,
            "variants": variants,
        })

    conn.commit()
    conn.close()

    json.dump({"items": items}, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()

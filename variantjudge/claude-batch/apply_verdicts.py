#!/usr/bin/env python3
"""Apply a verdict file to lexical.db: hard-delete flagged variants, INSERT
mode-C additions, log everything, and mark each surface as judged.

Usage:
    apply_verdicts.py <db_path> <verdict_file>

Verdict file schema (mode C):
    {"verdicts": [
        {
            "surface_id": 42,
            "surface": "...",
            "base": "...",
            "false_variants": ["badvariant1", "badvariant2"],
            "reasons": {"badvariant1": "F1 different root", ...},
            "additions": ["newvariant1", "newvariant2"],
            "add_reasons": {"newvariant1": "A1 prefix ב + base", ...}
        },
        ...
    ]}

`additions` and `add_reasons` are optional (mode A still works).

Behaviour:
- For each verdict:
  - DELETE matching surface_variant rows by (surface_id, variant value).
  - INSERT one row into variant_removal_log per actual deletion.
  - For each addition: INSERT OR IGNORE INTO variant + INSERT OR IGNORE INTO
    surface_variant. Log to variant_addition_log.
  - INSERT OR REPLACE into variant_judgment.
- Refuses to flag/add a variant equal to surface or base (HR1/HR2 enforcement).
- Refuses to add a variant that already exists in this surface's variants.
"""
import json
import sqlite3
import sys
import time


def main() -> None:
    if len(sys.argv) < 3:
        print(__doc__, file=sys.stderr)
        sys.exit(2)

    db_path, verdict_path = sys.argv[1], sys.argv[2]
    with open(verdict_path, encoding="utf-8") as f:
        data = json.load(f)

    verdicts = data.get("verdicts", [])
    if not verdicts:
        print("apply_verdicts: no verdicts in file", file=sys.stderr)
        return

    conn = sqlite3.connect(db_path)
    # Make sure the addition log table exists (mode C may run before
    # SQLDelight schema migration on legacy DBs).
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS variant_addition_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          surface_value TEXT NOT NULL,
          base_value TEXT,
          added_variant TEXT NOT NULL,
          reason TEXT,
          added_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS variant_addition_log_time_idx ON variant_addition_log(added_at);
        """
    )
    cur = conn.cursor()
    now = int(time.time() * 1000)

    total_judged = 0
    total_removed = 0
    total_added = 0
    skipped_self_flag = 0
    skipped_self_add = 0
    skipped_dup_add = 0

    for v in verdicts:
        surface_id = v["surface_id"]
        surface_value = v["surface"]
        base_value = v.get("base")
        falses = v.get("false_variants", [])
        reasons = v.get("reasons", {}) or {}
        additions = v.get("additions", [])
        add_reasons = v.get("add_reasons", {}) or {}

        # --- Purge phase ---
        applied_removals = 0
        for variant in falses:
            if variant == surface_value or variant == base_value:
                skipped_self_flag += 1
                continue
            cur.execute(
                """
                DELETE FROM surface_variant
                WHERE surface_id = ?
                  AND variant_id = (SELECT id FROM variant WHERE value = ?)
                """,
                (surface_id, variant),
            )
            if cur.rowcount > 0:
                cur.execute(
                    """
                    INSERT INTO variant_removal_log
                        (surface_value, base_value, removed_variant, reason, removed_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (surface_value, base_value, variant, reasons.get(variant), now),
                )
                applied_removals += 1

        # --- Enrich phase ---
        applied_additions = 0
        # Pre-fetch existing variants to avoid duplicate inserts within the
        # same surface (post-purge; an addition that was just-deleted in this
        # batch would simply be re-added — that's a contradiction we refuse).
        cur.execute(
            """
            SELECT v.value FROM surface_variant sv
            JOIN variant v ON v.id = sv.variant_id
            WHERE sv.surface_id = ?
            """,
            (surface_id,),
        )
        existing = {row[0] for row in cur.fetchall()}
        # Don't allow re-adding what we just removed in this verdict.
        existing.update(falses)

        for variant in additions:
            if variant == surface_value or variant == base_value:
                skipped_self_add += 1
                continue
            if variant in existing:
                skipped_dup_add += 1
                continue
            # Ensure the variant row exists; UNIQUE on value handles dedup.
            cur.execute("INSERT OR IGNORE INTO variant(value) VALUES (?)", (variant,))
            cur.execute("SELECT id FROM variant WHERE value = ?", (variant,))
            row = cur.fetchone()
            if row is None:
                continue
            variant_id = row[0]
            # Link to the surface; UNIQUE on (surface_id, variant_id) handles dedup.
            cur.execute(
                "INSERT OR IGNORE INTO surface_variant(surface_id, variant_id) VALUES (?, ?)",
                (surface_id, variant_id),
            )
            if cur.rowcount > 0:
                cur.execute(
                    """
                    INSERT INTO variant_addition_log
                        (surface_value, base_value, added_variant, reason, added_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (surface_value, base_value, variant, add_reasons.get(variant), now),
                )
                applied_additions += 1
                existing.add(variant)

        cur.execute(
            """
            INSERT OR REPLACE INTO variant_judgment(surface_id, judged_at, removed_count)
            VALUES (?, ?, ?)
            """,
            (surface_id, now, applied_removals),
        )
        total_judged += 1
        total_removed += applied_removals
        total_added += applied_additions

    conn.commit()
    conn.close()

    extras = []
    if skipped_self_flag:
        extras.append(f"{skipped_self_flag} self-flags refused")
    if skipped_self_add:
        extras.append(f"{skipped_self_add} self-adds refused")
    if skipped_dup_add:
        extras.append(f"{skipped_dup_add} dup-adds skipped")
    extras_str = (", " + ", ".join(extras)) if extras else ""
    print(
        f"applied: {total_judged} judged, {total_removed} removed, {total_added} added{extras_str}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Apply ambiguous-surface verdicts (issue #3) to lexical.db.

Usage:
    apply_ambiguous.py <db_path> <candidates.json> <verdicts.json>

verdicts.json: {"verdicts": [{"surface_id": 1, "extra_base_ids": [7]}, ...]}

Inserts (surface_id, base_id) into surface_extra_base and marks every surface of
the candidates file that has a verdict as judged in ambiguity_judgment. Only
base_ids offered as candidates for that surface are accepted, and never the
surface's own base.
"""
import json
import sqlite3
import sys
import time


def main() -> None:
    if len(sys.argv) != 4:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    db, cand_path, verdict_path = sys.argv[1:]
    allowed = {
        it["surface_id"]: {c["base_id"] for c in it["candidates"]}
        for it in json.load(open(cand_path, encoding="utf-8"))["items"]
    }
    verdicts = json.load(open(verdict_path, encoding="utf-8"))["verdicts"]

    conn = sqlite3.connect(db)
    cur = conn.cursor()
    now = int(time.time() * 1000)
    added = rejected = judged = 0
    for v in verdicts:
        sid = v["surface_id"]
        if sid not in allowed:
            rejected += len(v.get("extra_base_ids", []))
            continue
        own = cur.execute("SELECT base_id FROM surface WHERE id = ?", (sid,)).fetchone()
        if own is None:
            continue
        for bid in v.get("extra_base_ids", []):
            if bid not in allowed[sid] or bid == own[0]:
                rejected += 1
                continue
            cur.execute("INSERT OR IGNORE INTO surface_extra_base(surface_id, base_id) VALUES (?, ?)", (sid, bid))
            added += cur.rowcount
        cur.execute("INSERT OR REPLACE INTO ambiguity_judgment(surface_id, judged_at) VALUES (?, ?)", (sid, now))
        judged += 1
    conn.execute("CREATE INDEX IF NOT EXISTS surface_extra_base_base_id_index ON surface_extra_base(base_id)")
    conn.commit()
    conn.close()
    print(f"applied: {judged} judged, {added} extra lemmas added, {rejected} rejected", file=sys.stderr)


if __name__ == "__main__":
    main()

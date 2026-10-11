#!/usr/bin/env python3
"""Split a single batch JSON into N smaller batch files of approximately equal size.

Usage:
    split_batch.py <input.json> <out_prefix> <chunk_size>

Produces files <out_prefix>001.json, <out_prefix>002.json, ... each containing
{"items": [...]} with up to <chunk_size> items.
"""
import json
import sys


def main() -> None:
    if len(sys.argv) != 4:
        print(__doc__, file=sys.stderr)
        sys.exit(2)

    src, prefix, chunk = sys.argv[1], sys.argv[2], int(sys.argv[3])

    with open(src, encoding="utf-8") as f:
        data = json.load(f)

    items = data.get("items", [])
    paths = []
    for i in range(0, len(items), chunk):
        n = i // chunk + 1
        out_path = f"{prefix}{n:03d}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump({"items": items[i:i + chunk]}, f, ensure_ascii=False, indent=2)
        paths.append(out_path)

    for p in paths:
        print(p)


if __name__ == "__main__":
    main()

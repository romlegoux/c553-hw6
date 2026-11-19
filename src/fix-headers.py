#!/usr/bin/env python3
"""Move the provenance header (written by the program during the run) to line 1.

  python3 src/fix_headers.py [RESULTS_DIR]     (default: results)

The Cassandra driver's warnings were printed before the header in the client
summaries and worker logs. Only the position of that existing line changes.
"""

import glob
import gzip
import os
import sys


def fix(path):
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt") as f:
        lines = f.readlines()
    if not lines or lines[0].startswith("# vm="):
        return False
    idx = next((i for i, l in enumerate(lines[:20]) if l.startswith("# vm=")), None)
    if idx is None:
        return False
    lines.insert(0, lines.pop(idx))
    with opener(path, "wt") as f:
        f.writelines(lines)
    return True


def main():
    res = sys.argv[1] if len(sys.argv) > 1 else "results"
    files = [p for p in glob.glob(os.path.join(res, "**", "*"), recursive=True) if os.path.isfile(p)]
    moved = [p for p in files if not p.endswith(".png") and fix(p)]
    print(f"header moved to line 1 in {len(moved)} files")
    no_header = []
    for p in files:
        if p.endswith(".png") or "/analysis/" in p or os.path.basename(p).startswith("setup-"):
            continue
        opener = gzip.open if p.endswith(".gz") else open
        with opener(p, "rt") as f:
            if not f.readline().startswith("# vm="):
                no_header.append(p)
    print(f"files still without a header on line 1: {len(no_header)}")
    for p in no_header[:20]:
        print("  ", p)


if __name__ == "__main__":
    main()

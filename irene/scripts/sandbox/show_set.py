"""Render a set as markdown — same per-row format used elsewhere.

    python -m irene.scripts.sandbox.show_set --set highconf
    python -m irene.scripts.sandbox.show_set --set highconf --out review.md
"""

from __future__ import annotations

import argparse
import sys

from irene.sandbox import loader, sets
from irene.sandbox.report import render_row_block, render_summary, summary_counts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--set", required=True, dest="set_name")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    s = sets.load(args.set_name)
    con = loader.connect_readonly()
    rows = loader.load_rows(con, s.exploration_ids)

    parts = [render_summary(f"Set: {s.name}", summary_counts(rows))]
    if s.description:
        parts.append(f"_{s.description}_\n\n")
    for i, row in enumerate(rows, 1):
        parts.append(render_row_block(i, row))
        parts.append("\n")
    out = "".join(parts)

    if args.out:
        with open(args.out, "w") as f:
            f.write(out)
        print(f"wrote {args.out} ({len(rows)} rows)")
    else:
        sys.stdout.write(out)


if __name__ == "__main__":
    main()

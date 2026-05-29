"""Bucket explorer decisions by (confidence × type) and sample N from each
pile for manual review. Read-only.

Confidence buckets (fixed): <0.5, 0.5-0.7, 0.7-0.9, >=0.9, unknown
Type dimension is configurable via --type (default `freshness`); current
useful columns: `freshness`, `source` (article_source), `capture_status`.
`article_tier` is monolithic in current data and not useful here.

    python -m irene.scripts.explorer.sample_for_review
    python -m irene.scripts.explorer.sample_for_review --type source --n 30
    python -m irene.scripts.explorer.sample_for_review --seed 7
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import random
from collections import defaultdict

from irene.scripts.explorer._common import connect_readonly
from irene.scripts.explorer.dump_for_review import (
    COLS, QUERY, _render_row_md, DEFAULT_DB, DEFAULT_OUT_DIR,
)


CONF_EDGES = [0.5, 0.7, 0.9]   # produces 4 buckets + "unknown" for NULLs
CONF_LABELS = ["<0.5", "0.5-0.7", "0.7-0.9", ">=0.9", "unknown"]

TYPE_COLUMNS = {
    "freshness":      "freshness",
    "source":         "article_source",
    "capture_status": "capture_status",
    "tier":           "article_tier",
}


def conf_bucket(c):
    if c is None:
        return "unknown"
    if c < CONF_EDGES[0]:
        return CONF_LABELS[0]
    if c < CONF_EDGES[1]:
        return CONF_LABELS[1]
    if c < CONF_EDGES[2]:
        return CONF_LABELS[2]
    return CONF_LABELS[3]


def type_value(row, col):
    v = row.get(col)
    return v if v not in (None, "") else "unknown"


def _matrix_md(counts, type_keys):
    # counts[(conf_label, type_val)] -> int
    type_keys = sorted(type_keys)
    header = "| confidence \\ type | " + " | ".join(type_keys) + " | **total** |"
    sep = "|---|" + "|".join(["---"] * (len(type_keys) + 1)) + "|"
    lines = [header, sep]
    col_totals = defaultdict(int)
    grand = 0
    for cl in CONF_LABELS:
        row_total = 0
        cells = []
        for tk in type_keys:
            n = counts.get((cl, tk), 0)
            cells.append(str(n) if n else "·")
            row_total += n
            col_totals[tk] += n
        if row_total == 0:
            continue
        grand += row_total
        lines.append(f"| **{cl}** | " + " | ".join(cells) + f" | **{row_total}** |")
    lines.append(
        "| **total** | "
        + " | ".join(f"**{col_totals[tk]}**" for tk in type_keys)
        + f" | **{grand}** |"
    )
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--cycle", default="explorer-0")
    ap.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    ap.add_argument("--only-complete", action="store_true")
    ap.add_argument("--type", default="freshness", choices=list(TYPE_COLUMNS))
    ap.add_argument("--n", type=int, default=30, help="rows to sample per bucket")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    extra_where = "AND e.capture_status = 'complete'" if args.only_complete else ""
    con = connect_readonly(args.db)
    rows = [dict(r) for r in con.execute(
        QUERY.format(extra_where=extra_where), (args.cycle,)
    )]

    type_col = TYPE_COLUMNS[args.type]
    buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in rows:
        key = (conf_bucket(r["llm_confidence"]), type_value(r, type_col))
        buckets[key].append(r)

    counts = {k: len(v) for k, v in buckets.items()}
    type_keys = sorted({k[1] for k in buckets})

    rng = random.Random(args.seed)
    samples: dict[tuple[str, str], list[dict]] = {}
    for key, pile in buckets.items():
        if len(pile) <= args.n:
            samples[key] = list(pile)
        else:
            samples[key] = rng.sample(pile, args.n)

    matrix = _matrix_md(counts, type_keys)
    print(f"Confidence × {args.type} bucket counts (cycle={args.cycle}, "
          f"only_complete={args.only_complete}):\n")
    print(matrix)
    print()

    os.makedirs(args.out_dir, exist_ok=True)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base = f"sample_{args.cycle}_{args.type}_n{args.n}_seed{args.seed}_{stamp}"
    md_path = os.path.join(args.out_dir, base + ".md")
    csv_path = os.path.join(args.out_dir, base + ".csv")

    with open(md_path, "w") as f:
        f.write(f"# Explorer sample — cycle `{args.cycle}`, type=`{args.type}`, "
                f"n={args.n}, seed={args.seed}\n")
        f.write(f"_generated {stamp}; only_complete={args.only_complete}_\n\n")
        f.write("## Bucket counts (full population)\n\n")
        f.write(matrix + "\n\n")
        f.write(f"## Samples (≤{args.n} per non-empty bucket)\n\n")
        for cl in CONF_LABELS:
            for tk in type_keys:
                key = (cl, tk)
                pile = buckets.get(key, [])
                if not pile:
                    continue
                taken = samples[key]
                f.write(f"---\n\n### confidence **{cl}** · {args.type} **{tk}** "
                        f"— sampled {len(taken)}/{len(pile)}\n\n")
                for i, r in enumerate(taken, 1):
                    f.write(_render_row_md(i, r))
                    f.write("\n")

    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(
            f, fieldnames=["conf_bucket", args.type] + COLS, extrasaction="ignore"
        )
        w.writeheader()
        for key, taken in samples.items():
            cl, tk = key
            for r in taken:
                row = dict(r)
                row["conf_bucket"] = cl
                row[args.type] = tk
                w.writerow(row)

    print(f"wrote {md_path}")
    print(f"wrote {csv_path}")


if __name__ == "__main__":
    main()

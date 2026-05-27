"""Dump `explorations` + joined `odds_observations` to a flat CSV.

Each output row corresponds to one (exploration, time bucket). Time
buckets are derived from `offset_seconds` and rounded to a configurable
grid so cross-exploration analysis can align ticks. Default: 60s grid
in `[-600, +1200]` (the explorer's full capture window).

Read-only — opens the DB with `file:?mode=ro` so a buggy script can
never corrupt the production data.

Usage:
  python -m scripts.explorer.export_dataset \\
      --db /data/irene.db --cycle explorer-0 --out /tmp/explorer.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Optional

# Allow running as a script (irene/scripts/explorer/export_dataset.py)
# without configuring PYTHONPATH.
if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.explorer._common import connect_readonly, iter_explorations


def _bucket_offset(offset: int, grid: int) -> int:
    """Round an offset_seconds value down to the nearest `grid`."""
    if offset >= 0:
        return (offset // grid) * grid
    # For negative offsets we round AWAY from zero so a -300s sample
    # buckets to -300, not -240, on a 60s grid.
    return -(((-offset) + grid - 1) // grid) * grid


HEADER = [
    "exploration_id",
    "article_id",
    "market_condition_id",
    "t0_iso",
    "cycle_id",
    "dkg_final_score",
    "cosine_similarity",
    "freshness",
    "breaking_score",
    "article_tier",
    "market_volume_24h",
    "market_spread_t0",
    "market_mid_t0",
    "hours_to_expiry",
    "llm_confidence",
    "capture_status",
    "obs_source",
    "obs_offset_seconds",
    "obs_offset_bucket",
    "obs_mid",
    "obs_best_bid",
    "obs_best_ask",
    "obs_spread",
]


def export(
    db_path: str,
    out_path: str,
    *,
    cycle: Optional[str] = None,
    only_complete: bool = False,
    grid_seconds: int = 60,
) -> int:
    con = connect_readonly(db_path)
    rows_written = 0
    with open(out_path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(HEADER)
        for exp in iter_explorations(
            con, cycle=cycle, only_complete=only_complete,
        ):
            cur = con.execute(
                "SELECT source, offset_seconds, mid, best_bid, best_ask, "
                "       spread "
                "FROM odds_observations "
                "WHERE exploration_id = ? "
                "ORDER BY offset_seconds ASC",
                (exp.id,),
            )
            for o in cur:
                offset = int(o["offset_seconds"])
                writer.writerow([
                    exp.id, exp.article_id, exp.market_condition_id,
                    exp.t0_iso, exp.cycle_id,
                    exp.dkg_final_score, exp.cosine_similarity,
                    exp.freshness, exp.breaking_score, exp.article_tier,
                    exp.market_volume_24h, exp.market_spread_t0,
                    exp.market_mid_t0, exp.hours_to_expiry,
                    exp.llm_confidence, exp.capture_status,
                    o["source"], offset,
                    _bucket_offset(offset, grid_seconds),
                    o["mid"], o["best_bid"], o["best_ask"], o["spread"],
                ])
                rows_written += 1
    return rows_written


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Dump explorer dataset to CSV."
    )
    p.add_argument("--db", default="/data/irene.db",
                   help="path to irene.db")
    p.add_argument("--cycle", default=None,
                   help="filter by cycle name (e.g. explorer-0)")
    p.add_argument("--only-complete", action="store_true",
                   help="only include capture_status='complete' rows")
    p.add_argument("--grid", type=int, default=60,
                   help="bucket grid for obs_offset_bucket (seconds)")
    p.add_argument("--out", required=True,
                   help="output CSV path")
    args = p.parse_args(argv)

    n = export(
        args.db, args.out,
        cycle=args.cycle,
        only_complete=args.only_complete,
        grid_seconds=args.grid,
    )
    print(f"wrote {n} observation rows to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

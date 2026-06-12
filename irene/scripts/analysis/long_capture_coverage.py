"""Cycle 29 long-horizon checkpoint coverage (read-only).

Reports how much of the exploration corpus has its sparse +4h/+1d/+2d/+7d
checkpoints, by `long_capture_status` and by per-offset fill rate — the
verification number for the cycle-29 report (target: the backlog drains
to ~100% of reachable rows within days of deploy).

Usage:
    python3 scripts/analysis/long_capture_coverage.py [--db data/irene.db]
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from capture_schedule import CAPTURE_LONG_OFFSETS_SECS  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default="data/irene.db")
    args = ap.parse_args()
    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)

    cols = [r[1] for r in conn.execute("PRAGMA table_info(explorations)")]
    if "long_capture_status" not in cols:
        print("pre-cycle-29 DB: no long_capture_status column yet "
              "(migration lands at container boot).")
        return

    print("== long_capture_status ==")
    total = 0
    for status, n in conn.execute(
        "SELECT COALESCE(long_capture_status, '(pending)'), COUNT(*) "
        "FROM explorations GROUP BY 1 ORDER BY 2 DESC"
    ):
        print(f"  {status:<22} {n:>8}")
        total += n
    print(f"  {'total':<22} {total:>8}")

    print("\n== per-offset checkpoint fill (rows with status='done') ==")
    n_done = conn.execute(
        "SELECT COUNT(*) FROM explorations WHERE long_capture_status='done'"
    ).fetchone()[0]
    for off in CAPTURE_LONG_OFFSETS_SECS:
        grace = 1800 if off < 86_400 else 7200
        n = conn.execute(
            """
            SELECT COUNT(DISTINCT o.exploration_id)
            FROM odds_observations o
            JOIN explorations e ON e.id = o.exploration_id
            WHERE o.source='checkpoint'
              AND e.long_capture_status='done'
              AND ABS(o.offset_seconds - ?) <= ?
            """,
            (off, grace),
        ).fetchone()[0]
        pct = 100.0 * n / n_done if n_done else 0.0
        print(f"  +{off // 3600}h checkpoint: {n}/{n_done} ({pct:.1f}%)")

    n_ckpt = conn.execute(
        "SELECT COUNT(*) FROM odds_observations WHERE source='checkpoint'"
    ).fetchone()[0]
    print(f"\ncheckpoint rows total: {n_ckpt}")
    conn.close()


if __name__ == "__main__":
    main()

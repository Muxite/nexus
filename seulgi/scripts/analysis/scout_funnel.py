"""
SCOUT funnel analyzer.

Reads `trader_trace` JSON from every scout trade in the DB and tabulates
how many decisions die at each step of the trader pipeline. Lets you see —
after the fact — which filters were correct kills vs over-restrictive.

Usage:
    python3 seulgi/scripts/analysis/scout_funnel.py [--since-hours N]
                                                    [--engine scout]
                                                    [--out path]

The script also reports counterfactual volume — "if min_edge=-1.0 instead
of 0.03, how many more trades would have admitted?" — used to calibrate
real-engine gates against honest scout data.

Computation lives in `seulgi/app/scout_funnel_core.py` so the live
dashboard tile and this CLI share one implementation.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

# Pull the pure-function core from seulgi/app/ — the runtime container
# only ships app/, not scripts/, so the shared module must live there.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(_REPO_ROOT, "app"))
from scout_funnel_core import (  # noqa: E402
    SCOUT_FUNNEL_COLUMNS,
    compute_funnel,
    render_funnel_markdown,
)


def _connect_ro(path: str) -> sqlite3.Connection:
    """Read-only handle so analysis never blocks the live writer."""
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=20)
    conn.row_factory = sqlite3.Row
    return conn


def funnel_report(
    db_path: str,
    engine_prefix: str = "scout",
    since_hours: float = 24.0,
    out: str | None = None,
) -> str:
    """Emit a Markdown report of the decision funnel + counterfactuals."""
    conn = _connect_ro(db_path)
    cutoff = (
        datetime.now(timezone.utc) - timedelta(hours=since_hours)
    ).isoformat()
    rows = conn.execute(
        f"SELECT {SCOUT_FUNNEL_COLUMNS} "
        "FROM trades "
        "WHERE engine_name LIKE ? AND decided_at >= ? "
        "ORDER BY decided_at",
        (f"{engine_prefix}%", cutoff),
    ).fetchall()

    stats = compute_funnel(rows)
    text = render_funnel_markdown(
        stats,
        engine_prefix=engine_prefix,
        since_hours=since_hours,
        cutoff=cutoff,
    )
    if out:
        with open(out, "w") as f:
            f.write(text)
    return text


def main() -> int:
    p = argparse.ArgumentParser(description="SCOUT decision funnel analyzer")
    p.add_argument("--db", default="seulgi/data/seulgi.db", help="path to seulgi.db")
    p.add_argument("--engine", default="scout", help="engine name prefix")
    p.add_argument("--since-hours", type=float, default=24.0)
    p.add_argument("--out", default=None, help="write Markdown to this path")
    args = p.parse_args()

    if not os.path.exists(args.db):
        print(f"ERROR: db not found at {args.db}", file=sys.stderr)
        return 2

    report = funnel_report(
        db_path=args.db,
        engine_prefix=args.engine,
        since_hours=args.since_hours,
        out=args.out,
    )
    sys.stdout.write(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

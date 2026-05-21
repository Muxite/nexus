"""
Silent-rollback forensic report.

Walks the trades + engine_results tables and identifies "gap bursts" —
contiguous ranges of missing row IDs that indicate inserts which got an
autoincrement ID then ROLLED BACK without ever committing.

For each burst, correlates with the `cycles` table (was a cycle reset
running at the time?) and the most recent `decided_at` before the gap.

Pure read-only. Run from the host:

    python3 seulgi/scripts/analysis/reports/silent_rollback_forensic.py
    python3 seulgi/scripts/analysis/reports/silent_rollback_forensic.py \\
        --table trades --since-hours 24

The report makes the B-WRITES failure pattern visible and dated.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from datetime import datetime, timezone


def _connect_ro(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def find_gaps(conn: sqlite3.Connection, table: str, id_col: str = "id",
              min_burst: int = 20) -> list[dict]:
    """Find contiguous ID-ranges that are missing from the table.

    A burst of >= `min_burst` consecutive missing IDs is reported. The
    surrounding committed rows (one before, one after) anchor the burst
    in time so it can be correlated with cycle resets.
    """
    rows = conn.execute(
        f"SELECT {id_col} AS id FROM {table} ORDER BY {id_col}"
    ).fetchall()
    if not rows:
        return []
    bursts: list[dict] = []
    prev_id = rows[0]["id"]
    for r in rows[1:]:
        cur_id = r["id"]
        gap = cur_id - prev_id - 1
        if gap >= min_burst:
            bursts.append({
                "after_id": prev_id,
                "before_id": cur_id,
                "missing": gap,
                "start_missing": prev_id + 1,
                "end_missing": cur_id - 1,
            })
        prev_id = cur_id
    return bursts


def annotate_with_time(conn: sqlite3.Connection, table: str, bursts: list[dict],
                       ts_col: str = "decided_at") -> None:
    """Attach the timestamp of the row immediately before each burst."""
    for b in bursts:
        row = conn.execute(
            f"SELECT {ts_col} AS ts FROM {table} WHERE id = ?",
            (b["after_id"],),
        ).fetchone()
        b["after_ts"] = row["ts"] if row else None
        row = conn.execute(
            f"SELECT {ts_col} AS ts FROM {table} WHERE id = ?",
            (b["before_id"],),
        ).fetchone()
        b["before_ts"] = row["ts"] if row else None


def load_cycles(conn: sqlite3.Connection) -> list[dict]:
    return [dict(r) for r in conn.execute(
        "SELECT id, name, started_at, ended_at FROM cycles ORDER BY id"
    ).fetchall()]


def correlate_with_cycle(b: dict, cycles: list[dict]) -> dict | None:
    """If the burst occurred during a cycle, return that cycle dict."""
    if not b.get("after_ts"):
        return None
    after = b["after_ts"]
    for c in cycles:
        start = c.get("started_at") or "9999"
        end = c.get("ended_at") or "9999"
        if start <= after <= end:
            return c
    return None


def render_burst(b: dict, cycle: dict | None) -> list[str]:
    lines: list[str] = []
    lines.append(
        f"- IDs {b['start_missing']:,}-{b['end_missing']:,}  "
        f"({b['missing']:,} missing)"
    )
    if b.get("after_ts") and b.get("before_ts"):
        lines.append(
            f"  between {b['after_ts'][:19]}  →  {b['before_ts'][:19]}"
        )
    if cycle is not None:
        lines.append(
            f"  during cycle id={cycle['id']} name={cycle.get('name')} "
            f"(started {(cycle.get('started_at') or '?')[:19]})"
        )
    return lines


def main() -> int:
    p = argparse.ArgumentParser(description="Silent-rollback forensic report")
    p.add_argument("--db", default="seulgi/data/seulgi.db", help="path to seulgi.db")
    p.add_argument("--table", default=None,
                   help="restrict to one table (default: trades + engine_results + signals)")
    p.add_argument("--min-burst", type=int, default=20,
                   help="minimum consecutive missing IDs to report a burst")
    p.add_argument("--out", default=None, help="write Markdown to this path")
    args = p.parse_args()

    if not os.path.exists(args.db):
        print(f"ERROR: db not found at {args.db}", file=sys.stderr)
        return 2

    targets: list[tuple[str, str]] = [
        ("trades", "decided_at"),
        ("engine_results", "decided_at"),
        ("signals", "detected_at"),
        ("usage_events", "ts"),
    ]
    if args.table:
        targets = [t for t in targets if t[0] == args.table]
        if not targets:
            print(f"unknown table {args.table}", file=sys.stderr)
            return 2

    conn = _connect_ro(args.db)
    cycles = load_cycles(conn)
    lines: list[str] = [
        f"# Silent-rollback forensic — {datetime.now(timezone.utc).isoformat()}",
        f"_db: {args.db}_",
        "",
        f"Found {len(cycles)} historical cycles.",
        "",
    ]

    grand_missing = 0
    for table, ts_col in targets:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            max_id = conn.execute(f"SELECT MAX(id) FROM {table}").fetchone()[0]
        except sqlite3.OperationalError as exc:
            lines.append(f"## {table}: error — {exc}")
            continue
        gap_total = (max_id or 0) - (count or 0)
        grand_missing += gap_total
        lines.append(f"## {table}")
        lines.append("")
        lines.append(f"- count={count:,} max_id={max_id:,} total_gap={gap_total:,}")
        if gap_total == 0:
            lines.append("- ✓ no missing IDs")
            lines.append("")
            continue
        bursts = find_gaps(conn, table, "id", min_burst=args.min_burst)
        annotate_with_time(conn, table, bursts, ts_col=ts_col)
        lines.append(f"- {len(bursts)} bursts of ≥{args.min_burst} missing IDs:")
        lines.append("")
        # Show top 10 by size, then list all dated.
        top = sorted(bursts, key=lambda b: -b["missing"])[:10]
        for b in top:
            cycle = correlate_with_cycle(b, cycles)
            lines.extend(render_burst(b, cycle))
        lines.append("")

    lines.append(f"## Grand total missing IDs across all tables: {grand_missing:,}")
    lines.append("")

    text = "\n".join(lines) + "\n"
    if args.out:
        with open(args.out, "w") as f:
            f.write(text)
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

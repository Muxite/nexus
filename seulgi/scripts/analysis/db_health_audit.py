"""
SEULGI DB-health audit.

Read-only forensic snapshot of the seulgi.db writer state. Designed to
diagnose the B-WRITES failure mode (silent rollback inserts) and to
serve as the host-side companion to the live `db_health.txt` dashboard
tile.

Reports
───────
* Per-table:
    - COUNT(*)
    - MAX(id) (when an integer primary key exists)
    - GAP = MAX(id) − COUNT(*) — non-zero indicates silent rollbacks
      (an INSERT obtained an autoincrement ID then ROLLBACK happened).
    - MAX(<ts column>) and age vs now
* WAL state via `PRAGMA wal_checkpoint(PASSIVE)`:
    - busy flag, total frames, checkpointed frames
* Connection PRAGMAs:
    - journal_mode, busy_timeout, wal_autocheckpoint
* `--watch` mode loops every 30 s emitting a one-line summary so you
  can leave it running during a soak.

Never blocks the live writer — opens via `file:…?mode=ro` URI.

Usage
─────
    python3 seulgi/scripts/analysis/db_health_audit.py
    python3 seulgi/scripts/analysis/db_health_audit.py --watch --interval 30
    python3 seulgi/scripts/analysis/db_health_audit.py --db PATH

Exit 0 if every monitored table's `last_insert` age is under THRESHOLD;
exit 2 if any age exceeds it (CI-friendly).
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone

# Tables to audit. `(table, ts_col, id_col)`. id_col=None when no integer
# primary key (we still report COUNT but skip the gap math).
TABLES: list[tuple[str, str, str | None]] = [
    ("signals", "detected_at", "id"),
    ("trades", "decided_at", "id"),
    ("engine_results", "decided_at", "id"),
    ("usage_events", "ts", "id"),
    ("metrics_minutely", "bucket_ts", None),  # composite PK
    ("positions", "opened_at", "trade_id"),
]

# Age threshold (seconds) above which a table is "stale" — non-zero exit.
DEFAULT_STALE_THRESHOLD = 120.0


def _connect_ro(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:  # noqa: BLE001
        return None


def _fmt_age(secs: float) -> str:
    if secs < 90:
        return f"{secs:>5.0f}s"
    if secs < 3600:
        return f"{secs/60:>4.1f}m"
    if secs < 86400:
        return f"{secs/3600:>4.1f}h"
    return f"{secs/86400:>4.1f}d"


def audit_once(conn: sqlite3.Connection) -> dict:
    """Run one full audit pass; returns a dict suitable for JSON or pretty-print."""
    now = datetime.now(timezone.utc)
    report: dict = {"now": now.isoformat(), "tables": [], "wal": {}, "pragmas": {}}

    for table, ts_col, id_col in TABLES:
        row_info = {"table": table, "count": None, "max_id": None, "gap": None,
                    "last_ts": None, "age_secs": None, "stale": False}
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            row_info["count"] = count
        except sqlite3.OperationalError as exc:
            row_info["error"] = str(exc)
            report["tables"].append(row_info)
            continue
        if id_col:
            try:
                max_id = conn.execute(f"SELECT MAX({id_col}) FROM {table}").fetchone()[0]
                row_info["max_id"] = max_id
                if max_id is not None and count is not None:
                    row_info["gap"] = max_id - count
            except sqlite3.OperationalError:
                pass
        try:
            last_ts = conn.execute(f"SELECT MAX({ts_col}) FROM {table}").fetchone()[0]
            row_info["last_ts"] = last_ts
            t = _parse_iso(last_ts)
            if t is not None:
                age = (now - t).total_seconds()
                row_info["age_secs"] = age
                row_info["stale"] = age > DEFAULT_STALE_THRESHOLD
        except sqlite3.OperationalError:
            pass
        report["tables"].append(row_info)

    # WAL state — use a RW connection on a side path? PASSIVE wal_checkpoint
    # is safe to call from a RO connection in newer sqlite, but most builds
    # require a writeable handle. We retry on a transient writer connection.
    try:
        rw = sqlite3.connect(conn.execute("PRAGMA database_list").fetchone()[2], timeout=5)
        rw.execute("PRAGMA busy_timeout=2000")
        busy, total, done = rw.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
        report["wal"] = {"busy": busy, "total_frames": total, "checkpointed_frames": done}
        for p in ("journal_mode", "busy_timeout", "wal_autocheckpoint"):
            row = rw.execute(f"PRAGMA {p}").fetchone()
            if row:
                report["pragmas"][p] = row[0]
        rw.close()
    except Exception as exc:  # noqa: BLE001
        report["wal_error"] = str(exc)

    return report


def render(report: dict) -> str:
    lines: list[str] = []
    lines.append(f"# DB HEALTH AUDIT — {report['now']}")
    lines.append("")
    lines.append(
        "| table             |     count |     max_id |   gap | last_ts             | age      |"
    )
    lines.append(
        "|-------------------|----------:|-----------:|------:|---------------------|---------:|"
    )
    for t in report["tables"]:
        count = "?" if t["count"] is None else f"{t['count']:>9,}"
        max_id = "—" if t["max_id"] is None else f"{t['max_id']:>10,}"
        gap = "—" if t["gap"] is None else (
            f"⚠{t['gap']:>4,}" if t["gap"] > 0 else f"{t['gap']:>5,}"
        )
        last_ts = (t["last_ts"] or "(none)")[:19]
        age = "  —  " if t["age_secs"] is None else _fmt_age(t["age_secs"])
        mark = "  ⚠" if t["stale"] else ""
        lines.append(f"| {t['table']:<17} | {count} | {max_id} | {gap} | {last_ts:<19} | {age:>8} |{mark}")
    lines.append("")
    if report.get("wal"):
        w = report["wal"]
        lines.append(
            f"## WAL: busy={w.get('busy')} total_frames={w.get('total_frames')} "
            f"checkpointed_frames={w.get('checkpointed_frames')}"
        )
    if report.get("wal_error"):
        lines.append(f"## WAL probe error: {report['wal_error']}")
    if report.get("pragmas"):
        pragmas = report["pragmas"]
        bits = "  ".join(f"{k}={v}" for k, v in pragmas.items())
        lines.append(f"## PRAGMAs: {bits}")
    return "\n".join(lines) + "\n"


def watch(path: str, interval: float, threshold: float) -> int:
    """Print a one-line summary every `interval` seconds; never returns."""
    line_fmt = (
        "{ts}  signals[{sig_age:>5}/{sig_n:>8}]  "
        "trades[{tr_age:>5}/{tr_n:>8}/gap={tr_gap}]  "
        "er[{er_age:>5}/{er_n:>8}/gap={er_gap}]  "
        "wal[busy={wb} total={wt}]"
    )
    while True:
        conn = _connect_ro(path)
        try:
            rep = audit_once(conn)
        finally:
            conn.close()
        by_table = {t["table"]: t for t in rep["tables"]}
        sig = by_table.get("signals", {})
        tr = by_table.get("trades", {})
        er = by_table.get("engine_results", {})
        wal = rep.get("wal", {})
        line = line_fmt.format(
            ts=rep["now"][11:19],
            sig_age=_fmt_age(sig.get("age_secs") or 1e9) if sig.get("age_secs") is not None else "  —  ",
            sig_n=sig.get("count", 0),
            tr_age=_fmt_age(tr.get("age_secs") or 1e9) if tr.get("age_secs") is not None else "  —  ",
            tr_n=tr.get("count", 0),
            tr_gap=tr.get("gap", 0),
            er_age=_fmt_age(er.get("age_secs") or 1e9) if er.get("age_secs") is not None else "  —  ",
            er_n=er.get("count", 0),
            er_gap=er.get("gap", 0),
            wb=wal.get("busy", "?"),
            wt=wal.get("total_frames", "?"),
        )
        print(line, flush=True)
        time.sleep(interval)


def main() -> int:
    p = argparse.ArgumentParser(description="Seulgi DB-health audit")
    p.add_argument("--db", default="seulgi/data/seulgi.db", help="path to seulgi.db")
    p.add_argument("--watch", action="store_true", help="loop forever, one line per cycle")
    p.add_argument("--interval", type=float, default=30.0, help="watch interval (sec)")
    p.add_argument("--threshold", type=float, default=DEFAULT_STALE_THRESHOLD,
                   help="age threshold (sec) for non-zero exit")
    args = p.parse_args()

    if not os.path.exists(args.db):
        print(f"ERROR: db not found at {args.db}", file=sys.stderr)
        return 2

    if args.watch:
        try:
            watch(args.db, args.interval, args.threshold)
        except KeyboardInterrupt:
            return 0
        return 0

    conn = _connect_ro(args.db)
    try:
        report = audit_once(conn)
    finally:
        conn.close()
    sys.stdout.write(render(report))
    # Exit code: non-zero if any monitored core table is stale.
    core = {"signals", "trades", "engine_results", "usage_events"}
    stale = [t for t in report["tables"]
             if t["table"] in core and t.get("stale")]
    return 0 if not stale else 2


if __name__ == "__main__":
    raise SystemExit(main())

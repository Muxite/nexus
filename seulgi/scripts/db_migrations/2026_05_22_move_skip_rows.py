#!/usr/bin/env python3
"""
A1.4 (2026-05-22): skip-row backfill move from `trades` → archive + delete.

Why
---
Per cycle-2 contract, `engine_results` carries every per-engine
decision (executed + skipped), and `trades` carries only executed
attempts + closures. The trader.evaluate path has historically been
writing per-market skip rows to `trades` as well, producing
~6.6M skip rows in the live DB against ~498 executed rows
(see `seulgi/reports/2026-05-22-db-audit.md` §6 for the numbers).

Bug 8 / A1.4 ship two halves:

1. **Stop the bleed** — `trader._record` now drops skip writes
   to `trades` (DEV raises ValueError; PROD silently routes to a
   metric counter, since `record_engine_decisions` already wrote
   the per-engine row to `engine_results`).
2. **Drop the historical tail** — this one-shot migration archives
   the existing skip rows out of `trades` and deletes them so the
   table shrinks from ~6.6M rows to the ~498 executed attempts.

Skip-row signature (canonical per `app/db.py:_SCHEMA`)
------------------------------------------------------
A row in `trades` is a skip iff
  `skip_reason IS NOT NULL AND realized_pnl IS NULL`
We DELETE rows matching that predicate. We additionally exclude rows
that have a matching `positions` row by `trade_id` — defensive only;
positions and skips never coexist by construction, but the cost is
negligible and the safety is real.

NOTE: `opened_at` does NOT exist on `trades` (only on `positions`),
so the master-plan note "WHERE … AND opened_at IS NULL" is a
description of the conceptual contract — the actual query uses the
columns that exist.

Schema compatibility check
--------------------------
Every column needed by `engine_results` (engine_name, condition_id,
question, direction, confidence, similarity, p_hat, mid_price, edge,
size_usdc, executed, skip_reason, decided_at, headline_id,
signal_latency_ms) is present on `trades` skip rows. confidence is
back-filled from `confidence_raw` (the AI's raw scalar before the
edge formula); edge is back-filled from `signed_edge` (the
direction-signed magnitude). Both fall back to 0.0 on legacy rows
that pre-date those columns. `signal_latency_ms` does not exist on
trades; rows are imported with NULL.

What happens
------------
For each batch of skip rows (50,000 at a time, single transaction):
  1) INSERT INTO engine_results (...) SELECT ... FROM trades
     WHERE skip_reason IS NOT NULL AND realized_pnl IS NULL AND id IN (...)
       AND NOT EXISTS in engine_results by (engine_name, condition_id, decided_at, headline_id)
  2) DELETE FROM trades WHERE id IN (...) AND skip-row contract holds.

Idempotency
-----------
Re-running on a clean `trades` table is a no-op (no rows match the
contract). The INSERT side guards against duplicates via a NOT EXISTS
subquery on the (engine_name, condition_id, decided_at, headline_id)
tuple. Worst case re-run produces zero net writes.

Output
------
A log file is written to
`seulgi/data/archive/skip_row_move_<ts>.log` with the per-batch
counts: before, after, moved, deleted, errors, elapsed.

Performance
-----------
Each batch is one txn, scoped to 50k rows. WAL keeps readers
unblocked. Expect a few minutes per batch on a 9 GB DB. Vacuum
afterwards (manual; see end of script docstring) to reclaim the
~4-5 GB the rows occupied.

Usage
-----
From `seulgi/`:

    python3 scripts/db_migrations/2026_05_22_move_skip_rows.py
    # or with explicit path and batch size:
    python3 scripts/db_migrations/2026_05_22_move_skip_rows.py \
        --db data/seulgi.db --batch 50000

Add `--dry-run` to print counts without writing.

Manual VACUUM after this script finishes (do NOT run inside this
script — VACUUM holds an exclusive lock for minutes and blocks all
writers):

    sqlite3 data/seulgi.db 'VACUUM;'
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


# Skip-row predicate. Per the canonical contract documented in db.py,
# a skip row is any trades row with a non-NULL skip_reason and a NULL
# realized_pnl (i.e. the trade was never executed → no close → no
# realized PnL).
SKIP_PREDICATE = "skip_reason IS NOT NULL AND realized_pnl IS NULL"

# Backfill INSERT. We map confidence ← confidence_raw and edge ← signed_edge
# with COALESCE so pre-cycle-7 rows that don't have those columns populated
# fall back to 0.0 (matching the not-null-with-default schema of
# engine_results). `executed=0` is hard-coded because every skip is by
# definition non-executed.
INSERT_SQL = """
INSERT INTO engine_results
    (engine_name, condition_id, question, direction, confidence, similarity,
     p_hat, mid_price, edge, size_usdc, executed, skip_reason, decided_at,
     headline_id, signal_latency_ms)
SELECT
    COALESCE(t.engine_name, 'unknown'),
    t.condition_id,
    COALESCE(t.question, ''),
    t.direction,
    COALESCE(t.confidence_raw, 0.0),
    t.similarity,
    t.p_hat,
    t.mid_price,
    COALESCE(t.signed_edge, 0.0),
    t.size_usdc,
    0,
    t.skip_reason,
    t.decided_at,
    t.headline_id,
    NULL
FROM trades t
WHERE t.id IN ({id_placeholders})
  AND t.skip_reason IS NOT NULL
  AND t.realized_pnl IS NULL
  AND NOT EXISTS (
        SELECT 1 FROM engine_results er
         WHERE er.engine_name  IS t.engine_name
           AND er.condition_id =  t.condition_id
           AND er.decided_at   =  t.decided_at
           AND er.headline_id  IS t.headline_id
  )
"""


def _count_skip(cur: sqlite3.Cursor) -> int:
    cur.execute(f"SELECT COUNT(*) FROM trades WHERE {SKIP_PREDICATE}")
    return int(cur.fetchone()[0])


def _count_trades(cur: sqlite3.Cursor) -> int:
    cur.execute("SELECT COUNT(*) FROM trades")
    return int(cur.fetchone()[0])


def _count_engine_results(cur: sqlite3.Cursor) -> int:
    cur.execute("SELECT COUNT(*) FROM engine_results")
    return int(cur.fetchone()[0])


def _fetch_batch_ids(cur: sqlite3.Cursor, batch: int) -> list[int]:
    cur.execute(
        f"SELECT id FROM trades WHERE {SKIP_PREDICATE} ORDER BY id LIMIT ?",
        (batch,),
    )
    return [row[0] for row in cur.fetchall()]


def _process_batch(
    cur: sqlite3.Cursor, ids: list[int]
) -> tuple[int, int]:
    """Insert + delete one batch. Returns (inserted, deleted)."""
    placeholders = ",".join("?" * len(ids))
    sql_ins = INSERT_SQL.format(id_placeholders=placeholders)
    cur.execute(sql_ins, ids)
    inserted = cur.rowcount or 0
    cur.execute(
        f"DELETE FROM trades WHERE id IN ({placeholders}) AND {SKIP_PREDICATE}",
        ids,
    )
    deleted = cur.rowcount or 0
    return inserted, deleted


def migrate(
    db_path: Path,
    batch: int,
    dry_run: bool,
    log_path: Path,
) -> dict:
    """Run the migration. Returns a summary dict."""
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    # WAL + busy-timeout so we don't deadlock with the live writer.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=60000")
    cur = conn.cursor()

    summary: dict = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path),
        "batch": batch,
        "dry_run": dry_run,
    }

    trades_before = _count_trades(cur)
    skips_before = _count_skip(cur)
    er_before = _count_engine_results(cur)
    summary["trades_before"] = trades_before
    summary["skip_rows_before"] = skips_before
    summary["engine_results_before"] = er_before

    moved_total = 0
    deleted_total = 0
    error_total = 0
    batch_n = 0
    elapsed_s = 0.0

    log_lines: list[str] = [
        f"# skip-row move log — {summary['started_at']}",
        f"db={db_path}  batch={batch}  dry_run={dry_run}",
        f"BEFORE  trades={trades_before:>10}  skip_rows={skips_before:>10}  "
        f"engine_results={er_before:>10}",
        "",
    ]

    if dry_run:
        log_lines.append("dry-run: no writes performed.")
    else:
        # Loop while skip rows remain. Each batch is a single transaction.
        while True:
            ids = _fetch_batch_ids(cur, batch)
            if not ids:
                break
            t0 = time.monotonic()
            try:
                cur.execute("BEGIN")
                ins, dele = _process_batch(cur, ids)
                conn.commit()
            except Exception as exc:  # noqa: BLE001
                try:
                    conn.rollback()
                except Exception:  # noqa: BLE001
                    pass
                error_total += 1
                log_lines.append(
                    f"  batch={batch_n:>4} ids[{ids[0]}..{ids[-1]}] "
                    f"ERROR: {exc!r}"
                )
                # Stop on errors to avoid runaway corruption.
                break
            dt = time.monotonic() - t0
            elapsed_s += dt
            moved_total += ins
            deleted_total += dele
            batch_n += 1
            log_lines.append(
                f"  batch={batch_n:>4} ids[{ids[0]}..{ids[-1]}] "
                f"moved={ins:>6} deleted={dele:>6} t={dt:>5.1f}s"
            )
            # Tight loop is fine — WAL keeps readers unblocked, and
            # busy_timeout absorbs any contention with the live writer.

    trades_after = _count_trades(cur)
    skips_after = _count_skip(cur)
    er_after = _count_engine_results(cur)
    summary["trades_after"] = trades_after
    summary["skip_rows_after"] = skips_after
    summary["engine_results_after"] = er_after
    summary["batches"] = batch_n
    summary["moved"] = moved_total
    summary["deleted"] = deleted_total
    summary["errors"] = error_total
    summary["elapsed_s"] = round(elapsed_s, 2)
    summary["finished_at"] = datetime.now(timezone.utc).isoformat()

    log_lines.append("")
    log_lines.append(
        f"AFTER   trades={trades_after:>10}  skip_rows={skips_after:>10}  "
        f"engine_results={er_after:>10}"
    )
    log_lines.append(
        f"TOTAL   batches={batch_n}  moved={moved_total}  "
        f"deleted={deleted_total}  errors={error_total}  "
        f"elapsed={elapsed_s:.1f}s"
    )

    log_path.write_text("\n".join(log_lines) + "\n")
    conn.close()
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db", default=os.environ.get("SEULGI_DB", "data/seulgi.db"),
        help="Path to seulgi.db (default: $SEULGI_DB or data/seulgi.db)",
    )
    parser.add_argument(
        "--batch", type=int, default=50_000,
        help="Rows per transaction (default 50000)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print counts only; do not write.",
    )
    parser.add_argument(
        "--log", default=None,
        help="Path to write the run log (default: data/archive/"
             "skip_row_move_<ts>.log relative to DB parent).",
    )
    args = parser.parse_args(argv)
    db_path = Path(args.db).resolve()
    if args.log:
        log_path = Path(args.log).resolve()
    else:
        ts = datetime.now(timezone.utc).strftime("%Y_%m_%d_%H%M%S")
        log_path = db_path.parent / "archive" / f"skip_row_move_{ts}.log"
    summary = migrate(db_path, args.batch, args.dry_run, log_path)
    for k, v in summary.items():
        print(f"  {k}: {v}")
    print(f"  log: {log_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
FIX-2B (2026-05-22): persist exit_price + entry_price_at_close on trades.

Why
---
`db.close_trade` writes `realized_pnl`, `closed_at`, `close_reason`, and
`exit_fee_usdc`, but **not** the effective exit fill price or the
entry_price the close was computed against. Since `close_position`
deletes the `positions` row the instant the trade closes, the actual
exit price is unreconstructible from the DB — blocking Phase 1
paper-accuracy audits and Phase 3 backtest validation (see
seulgi/reports/2026-05-22-trader-exit-bugs.md §5.1).

This migration adds two REAL columns to `trades`:

  - `exit_price`           — the effective fill price at close (slipped
                             mid for early exits; $1/$0 for settlement
                             rails). NULL on pre-migration rows.
  - `entry_price_at_close` — sanity-check anchor (the position's
                             entry_price at the moment of close).
                             NULL on pre-migration rows. **Not**
                             back-derived from realized_pnl — only
                             populated going forward.

Idempotency
-----------
Both ALTERs are no-ops if the column already exists. Uses
`PRAGMA table_info(trades)` to gate the ALTER calls, since SQLite
does not support `IF NOT EXISTS` on `ALTER TABLE ADD COLUMN`.

Usage
-----
From the `seulgi/` directory:

    python3 scripts/db_migrations/2026_05_22_add_exit_price.py

Picks up DB path from `$SEULGI_DB` if set, otherwise defaults to
`data/seulgi.db` (relative to CWD). The same script also runs as a
schema-level migration inside `db._MIGRATIONS` on every connect; this
file exists so the migration can be applied to the live DB without
waiting for the next writer restart and so it can be re-run safely.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

NEW_COLUMNS = (
    ("exit_price", "REAL"),
    ("entry_price_at_close", "REAL"),
)


def _existing_columns(cur: sqlite3.Cursor, table: str) -> set[str]:
    cur.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}


def migrate(db_path: Path) -> dict[str, str]:
    """Apply the migration. Returns {col: action} for each new column."""
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")
    result: dict[str, str] = {}
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        existing = _existing_columns(cur, "trades")
        for col, sql_type in NEW_COLUMNS:
            if col in existing:
                result[col] = "already-present"
                continue
            cur.execute(f"ALTER TABLE trades ADD COLUMN {col} {sql_type}")
            result[col] = "added"
        conn.commit()
    finally:
        conn.close()
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db", default=os.environ.get("SEULGI_DB", "data/seulgi.db"),
        help="Path to seulgi.db (default: $SEULGI_DB or data/seulgi.db)",
    )
    args = parser.parse_args(argv)
    db_path = Path(args.db)
    result = migrate(db_path)
    for col, action in result.items():
        print(f"[migration] trades.{col}: {action}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

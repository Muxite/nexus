#!/usr/bin/env python3
"""
A1.4 (2026-05-22): drop the dead `lessons` table.

Why
---
The `lessons` table carried continuous-reinforcement notes injected into
the AI prompt. Its writer (`app/lessons.py`) was deleted in cycle-4 and
the table has been empty since. The only stale reader,
`scripts/analysis/sample_classify.py`, fetches `SELECT note FROM lessons
WHERE active=1` and gracefully no-ops on an empty result set; it
continues to work after the drop because the table simply returns
"no such table" the first time, which the script tolerates (see
`fetch_lesson_notes` — it returns `[]` on any exception path).

`lesson_notes` was never a real table — the string refers to a Python
local in the same script. This migration still issues
`DROP TABLE IF EXISTS lesson_notes` defensively in case some old DB has
a stray table by that name.

What changes
------------
- `DROP TABLE IF EXISTS lessons;`
- `DROP TABLE IF EXISTS lesson_notes;`
- Both are idempotent — re-running this script is a no-op.

The matching change in `seulgi/app/db.py` removes the CREATE statements
from `_MIGRATIONS` and replaces them with the two DROPs, so any DB
opened via `db.connect()` reaches the same end state automatically.

The schema (CREATE TABLE statements only — table was empty at drop
time, no row export needed) is archived to
`seulgi/data/archive/lessons_2026_05_22.sql`.

Usage
-----
From the `seulgi/` directory:

    python3 scripts/db_migrations/2026_05_22_drop_lessons.py

Picks up DB path from `$SEULGI_DB`; otherwise defaults to
`data/seulgi.db`.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

TABLES_TO_DROP = ("lessons", "lesson_notes")


def _existing_tables(cur: sqlite3.Cursor) -> set[str]:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return {row[0] for row in cur.fetchall()}


def _count(cur: sqlite3.Cursor, table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    return int(cur.fetchone()[0])


def migrate(db_path: Path) -> dict[str, str]:
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")
    result: dict[str, str] = {}
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        existing = _existing_tables(cur)
        for table in TABLES_TO_DROP:
            if table not in existing:
                result[table] = "already-absent"
                continue
            n = _count(cur, table)
            if n != 0:
                # Defensive: the audit recorded 0 rows; if a live DB has
                # non-empty data, refuse to drop silently. Operator can
                # rerun with --force after archiving.
                result[table] = f"REFUSED ({n} rows present — re-run with --force to drop anyway)"
                continue
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            result[table] = "dropped"
        conn.commit()
    finally:
        conn.close()
    return result


def migrate_force(db_path: Path) -> dict[str, str]:
    """Force-drop variant that does not refuse on non-empty tables.

    Use only after archiving rows externally. Honour the safety rail by
    requiring the explicit --force flag.
    """
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")
    result: dict[str, str] = {}
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        existing = _existing_tables(cur)
        for table in TABLES_TO_DROP:
            if table not in existing:
                result[table] = "already-absent"
                continue
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            result[table] = "dropped (force)"
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
    parser.add_argument(
        "--force", action="store_true",
        help="Drop tables even if they contain rows (export first).",
    )
    args = parser.parse_args(argv)
    db_path = Path(args.db)
    result = migrate_force(db_path) if args.force else migrate(db_path)
    for table, action in result.items():
        print(f"[migration] {table}: {action}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

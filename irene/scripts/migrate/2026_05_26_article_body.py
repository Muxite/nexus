"""
## Specification

**Purpose.** Cycle-24 handoff H — add the `body` column to `articles` so
the trade-stage `compose_direction_prompt` can prefer the full
trafilatura-cleaned article body over the (often 1-sentence) RSS
`summary` field. Backfill is NOT performed — new articles only.

The ingest stage (`app/stages/ingest.py`) writes the `body` field when
the upstream scraper-v2 combined / article response carries it; until
then the column is NULL and the prompt builder transparently falls back
to `summary`.

The DDL is also registered in `app/stages/ingest.py::_INGEST_DDL`
(idempotent ADD COLUMN, sha1-tracked by `app.db.register_schema`) so
fresh boots get the column without running this script. This script is
for already-deployed DBs that want the column applied ahead of the next
container restart.

**Public interface.**
- CLI: ``python -m scripts.migrate.2026_05_26_article_body
        --irene-db <path> [--dry-run]``
- ``run(db_path: str, *, dry_run: bool = False) -> bool`` — returns
  True when the column was added, False when it already existed.

**Invariants.**
- Idempotent: re-runs return False without raising.
- Pure ADD COLUMN — no data writes, no other schema change.
- Refuses to run against a seulgi-shaped path (`app.db._assert_not_seulgi`).
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import sqlite3
import sys
from datetime import datetime, timezone

from app.db import _assert_not_seulgi

logger = logging.getLogger("scripts.migrate.article_body")


# Must match the literal statement in `app/stages/ingest.py::_INGEST_DDL`
# so `schema_migrations` sees the same sha. If you edit the DDL text,
# update this constant too — otherwise the next irene boot will re-issue
# the ALTER and SQLite will raise "duplicate column name body".
_ALTER_STMT = "ALTER TABLE articles ADD COLUMN body TEXT"
_OWNER = "app/stages/ingest.py"


def _has_body_column(conn: sqlite3.Connection) -> bool:
    cur = conn.execute("PRAGMA table_info(articles)")
    cols = {row[1] for row in cur.fetchall()}
    cur.close()
    return "body" in cols


def _record_in_schema_migrations(conn: sqlite3.Connection) -> None:
    """Insert (owner, sha) into schema_migrations so the next irene boot's
    `register_schema` lookup short-circuits the same ALTER. The
    schema_migrations table is created by `app/db.py` on first connect;
    if absent (truly empty DB) we no-op and the regular boot will create
    it and apply the ALTER itself.
    """
    cur = conn.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='table' AND name='schema_migrations'"
    )
    exists = cur.fetchone() is not None
    cur.close()
    if not exists:
        return
    sha = hashlib.sha1(_ALTER_STMT.encode("utf-8")).hexdigest()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT OR IGNORE INTO schema_migrations "
        "(owner, statement_sha, applied_at) VALUES (?, ?, ?)",
        (_OWNER, sha, now),
    )


def run(db_path: str, *, dry_run: bool = False) -> bool:
    """Add `articles.body TEXT` if it doesn't exist. Returns True on add."""
    _assert_not_seulgi(db_path)
    conn = sqlite3.connect(db_path)
    try:
        if _has_body_column(conn):
            logger.info("article_body: column already present — no-op.")
            # Still ensure schema_migrations records the sha so the next
            # boot doesn't re-issue a duplicate ALTER (handles the case
            # where the column was added out-of-band, e.g. manual ALTER).
            if not dry_run:
                _record_in_schema_migrations(conn)
                conn.commit()
            return False
        if dry_run:
            logger.info(
                "article_body: --dry-run; would ALTER TABLE articles "
                "ADD COLUMN body TEXT"
            )
            return False
        conn.execute(_ALTER_STMT)
        _record_in_schema_migrations(conn)
        conn.commit()
        logger.info("article_body: ALTER TABLE complete — column added.")
        return True
    finally:
        conn.close()


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Cycle-24 handoff H: ADD COLUMN articles.body TEXT. "
            "Idempotent; safe to run live."
        ),
    )
    p.add_argument(
        "--irene-db",
        required=True,
        help="Path to the irene SQLite DB (e.g. /data/irene.db).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would happen without issuing the ALTER.",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = _build_parser().parse_args(argv)
    run(args.irene_db, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())

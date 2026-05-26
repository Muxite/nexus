"""Migration: add `tier` and `breaking_score` columns to `articles`.

Cycle 22-23 added these columns to the CREATE TABLE in ingest.py and added
a CREATE INDEX on `breaking_score`, but never wrote the ALTER for existing
DBs. The cycle-24 rebuild surfaced the gap — boot raised `no such column:
breaking_score` on the CREATE INDEX statement against a live DB that
predates the column.

Idempotent: skips columns that already exist (e.g. on a fresh schema where
the CREATE TABLE supplied them). Safe to re-run.

Usage (from irene/):
    python3 -m scripts.migrate.2026_05_26_breaking_score --irene-db data/irene.db

Or inside the container:
    docker exec irene python -m scripts.migrate.2026_05_26_breaking_score \
        --irene-db /data/irene.db
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

logger = logging.getLogger("scripts.migrate.breaking_score")


def apply(db_path: str) -> dict:
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        cur = conn.execute("PRAGMA table_info(articles)")
        cols = {row[1] for row in cur.fetchall()}
        added: list[str] = []
        if "tier" not in cols:
            conn.execute(
                "ALTER TABLE articles ADD COLUMN tier TEXT NOT NULL DEFAULT 'mainstream'"
            )
            added.append("tier")
        if "breaking_score" not in cols:
            conn.execute(
                "ALTER TABLE articles ADD COLUMN breaking_score REAL NOT NULL DEFAULT 0.0"
            )
            added.append("breaking_score")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_articles_breaking_score "
            "ON articles(breaking_score)"
        )
        conn.commit()
        return {"added": added, "skipped": sorted(cols & {"tier", "breaking_score"})}
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--irene-db", required=True, help="Path to irene.db")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if not Path(args.irene_db).exists():
        logger.error("DB not found: %s", args.irene_db)
        return 2
    result = apply(args.irene_db)
    if result["added"]:
        logger.info("breaking_score: added column(s) %s", result["added"])
    else:
        logger.info("breaking_score: no-op — columns already present")
    if result["skipped"]:
        logger.info("breaking_score: pre-existing %s", result["skipped"])
    return 0


if __name__ == "__main__":
    sys.exit(main())

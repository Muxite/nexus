"""Migration — cycle 24 (handoff B): add `latest_article_at` to article_clusters.

**Purpose.** Cycle 24's dedupe/freshness fix replaces cluster-age with
**latest-article-age** in the freshness label cascade. The new column
`article_clusters.latest_article_at TEXT` is the rolling-recency stamp;
on a fresh DB the registered DDL creates it implicitly, but every
existing irene.db needs an `ALTER TABLE` plus a backfill so the first
recency read on an old cluster doesn't return NULL (which would force
the fallback to representative-article ingested_at — correct but slower
than just having the right value).

**Backfill rule.** `latest_article_at = MAX(member.added_at)` per
cluster, falling back to `created_at` when a cluster somehow has no
members. This is the most truthful value we can reconstruct: every
member-row has a TEXT ISO `added_at` stamped at attach time.

**Idempotent.** The ALTER is wrapped in a "PRAGMA table_info" check;
the backfill UPDATE only touches rows where `latest_article_at IS NULL`.
Safe to re-run.

**Usage.**
    python -m scripts.migrate.2026_05_26_cluster_latest --db /data/irene.db
    # or, with the default container path:
    docker compose run --rm irene python -m \\
        scripts.migrate.2026_05_26_cluster_latest --db /data/irene.db

Should run BEFORE restarting the irene container with the new dedupe
code; on a fresh DB it's a no-op. After the migration, `_apply_schema`
on next boot will re-register the DDL but `IF NOT EXISTS` + the
schema_migrations dedupe both make that a no-op.
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from datetime import datetime, timezone

logger = logging.getLogger("migrate.cluster_latest")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


DEFAULT_DB = "/data/irene.db"


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cur.fetchall())


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return cur.fetchone() is not None


def migrate(db_path: str, *, dry_run: bool = False) -> dict:
    """Add the column + backfill. Returns counters for caller logging."""
    started_at = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        if not _table_exists(conn, "article_clusters"):
            logger.warning(
                "article_clusters table missing at %s — irene DB not bootstrapped. "
                "Nothing to migrate.", db_path,
            )
            return {"added_column": False, "backfilled": 0, "skipped": True}

        added = False
        if not _column_exists(conn, "article_clusters", "latest_article_at"):
            logger.info("Adding column article_clusters.latest_article_at …")
            if not dry_run:
                conn.execute(
                    "ALTER TABLE article_clusters "
                    "ADD COLUMN latest_article_at TEXT"
                )
                conn.commit()
            added = True
        else:
            logger.info("Column already exists — skipping ALTER.")

        # Backfill from MAX(member.added_at), fall back to created_at.
        # Two-step so NULL rows after backfill can be reported.
        backfill_sql = """
            UPDATE article_clusters
            SET latest_article_at = COALESCE(
                (SELECT MAX(m.added_at)
                   FROM article_cluster_members m
                   WHERE m.cluster_id = article_clusters.id),
                article_clusters.created_at
            )
            WHERE latest_article_at IS NULL
        """
        if dry_run:
            # Defensive: in a dry-run that "would have added" the column,
            # the SELECT below would error — so count rows unconditionally
            # when the column doesn't exist, and only filter NULLs when it does.
            if _column_exists(conn, "article_clusters", "latest_article_at"):
                cur = conn.execute(
                    "SELECT COUNT(*) FROM article_clusters "
                    "WHERE latest_article_at IS NULL"
                )
            else:
                cur = conn.execute("SELECT COUNT(*) FROM article_clusters")
            would_update = cur.fetchone()[0]
            logger.info("[dry-run] Would backfill %d rows.", would_update)
            return {
                "added_column": added, "backfilled": 0,
                "would_backfill": would_update, "skipped": False,
            }

        logger.info("Backfilling NULL latest_article_at rows …")
        cur = conn.execute(backfill_sql)
        backfilled = cur.rowcount
        conn.commit()
        # Sanity: any remaining NULLs after backfill?
        cur = conn.execute(
            "SELECT COUNT(*) FROM article_clusters WHERE latest_article_at IS NULL"
        )
        remaining = cur.fetchone()[0]
        logger.info(
            "Backfilled %d rows; %d remain NULL (these should be 0).",
            backfilled, remaining,
        )
        finished_at = datetime.now(timezone.utc).isoformat()
        logger.info(
            "Migration complete: started=%s finished=%s added_column=%s",
            started_at, finished_at, added,
        )
        return {
            "added_column": added, "backfilled": backfilled,
            "remaining_null": remaining, "skipped": False,
        }
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=DEFAULT_DB,
                        help=f"Path to irene.db (default {DEFAULT_DB})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would change, don't write.")
    args = parser.parse_args(argv)
    result = migrate(args.db, dry_run=args.dry_run)
    logger.info("Result: %s", result)
    return 0


if __name__ == "__main__":
    sys.exit(main())

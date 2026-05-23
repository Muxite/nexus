"""
## Specification

**Purpose.** One-time importer of seulgi's data-layer tables into
`irene.db`. Reads seulgi's `signals` + `headline_articles` + `markets`
read-only and writes equivalent rows into irene's `articles` and
`markets` tables under a synthetic `seulgi-import-<date>` cycle.

Scope (locked by the user during planning):
- **In:** `articles` (from `signals` LEFT JOIN `headline_articles`)
  and `markets` (with embedding pass-through). Both get stamped to
  the import cycle.
- **Out:** trades, positions, engine_results, price_impacts, and
  every observability table. price_impacts is excluded because
  irene's schema requires a `trade_id NOT NULL UNIQUE REFERENCES trades(id)`
  FK and we're not migrating trades — relaxing the FK or
  backfilling 564 trade rows is a follow-up.
- **DKG layer (entities/facts/etc.) is NOT touched here** — it's
  populated by `scripts/migrate/dkg_bulk_sample.py` + the replay
  step against `irene.articles` once those rows land.

**Public interface.**
- CLI: `python -m scripts.migrate.from_seulgi --seulgi-db <path>
        --irene-db <path> [--limit N] [--dry-run]`
- `run_inproc(seulgi_db, irene_db, *, limit=None, dry_run=False) -> dict`
  for programmatic use (tests).

**Invariants.**
- Read-only handle to seulgi (`file:<path>?mode=ro`); we never write
  to seulgi.db under any code path.
- The irene-side path goes through `app.db.DB.connect()` once to
  bootstrap the full registered schema (and to fire the seulgi-path
  assertion). After bootstrap, all import writes happen on a sync
  sqlite3 connection — simpler than threading aiosqlite through a
  long-running offline job.
- Resumability comes from `articles.UNIQUE(url)` and
  `markets.PRIMARY KEY(condition_id)`. Re-running with the same
  `--irene-db` is a no-op on already-migrated rows.
- Articles get `source='seulgi_migration'`, source_detail=publisher,
  cycle_id=<import cycle>, is_tracer=0.
- Markets carry the seulgi embedding (1536-byte float32 blob =
  384-d) verbatim, stamped with
  `embedder_id='sentence-transformers/all-MiniLM-L6-v2@384'`
  (confirmed identical to irene's default).
- Writes commit in chunks of `_COMMIT_CHUNK_ROWS` rows so a mid-run
  abort leaves the next run with at most one chunk's worth of
  dropped progress.

**Side effects.** Bootstraps the irene schema in `--irene-db`
(creates the file + all registered tables). Writes to
`articles`, `markets`, `cycles`, and the importer's own
`seulgi_migration_batches` log table.

**Failure modes.**
- `IreneDBPathError` if `--irene-db` matches a seulgi-shaped path.
- `RuntimeError` on dim mismatch between seulgi's stored embedding
  and the declared 384-d shape.
- `sqlite3.IntegrityError` propagates if a required column is missing
  on either side (schema drift).

**Non-goals.**
- Does NOT touch trades / positions / engine_results / price_impacts /
  decision_inputs / market_price_snaps / cycles in seulgi / any
  observability table.
- Does NOT compute embeddings (seulgi's are reused as-is).
- Does NOT run the DKG extraction or resolver. That happens via the
  bulk-extraction thread, separately.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sqlite3
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np

# Register all owner DDLs by importing the modules. Each module calls
# db.register_schema() at import time; the bootstrap step below picks
# them up via DB.connect().
import app.main  # noqa: F401 — registers `cycles`
import app.stages.dedupe  # noqa: F401
import app.stages.extract  # noqa: F401
import app.stages.ingest  # noqa: F401 — registers `articles`
import app.stages.markets  # noqa: F401 — registers `markets`
import app.stages.match  # noqa: F401
import app.stages.resolve  # noqa: F401
import app.stages.trade  # noqa: F401
import app.ai as _ai_module  # noqa: F401 — registers `predictions`, `llm_calls`
from app import db as db_module
from app.db import DB

logger = logging.getLogger(__name__)

# Stable across both seulgi (confirmed in seulgi/app/markets.py) and
# irene's default. Both use 384-d. If this changes on either side, the
# stamp here is wrong — bump together.
_SEULGI_EMBEDDER_ID = "sentence-transformers/all-MiniLM-L6-v2@384"
_SEULGI_EMBEDDING_DIM = 384

_CYCLE_NAME = f"seulgi-import-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
_COMMIT_CHUNK_ROWS = 1000


# ---------------------------------------------------------------------------
# Schema for the importer's own batch log
# ---------------------------------------------------------------------------


_SEULGI_MIGRATION_DDL = [
    """
    CREATE TABLE IF NOT EXISTS seulgi_migration_batches (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id        TEXT NOT NULL,
        table_name      TEXT NOT NULL,
        started_at      TEXT NOT NULL,
        finished_at     TEXT,
        rows_seen       INTEGER NOT NULL DEFAULT 0,
        rows_inserted   INTEGER NOT NULL DEFAULT 0,
        rows_skipped    INTEGER NOT NULL DEFAULT 0,
        aborted_reason  TEXT,
        UNIQUE(batch_id, table_name)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_seulgi_migration_batch ON seulgi_migration_batches(batch_id)",
]
db_module.register_schema("scripts/migrate/from_seulgi.py", _SEULGI_MIGRATION_DDL)


# ---------------------------------------------------------------------------
# Per-table counters
# ---------------------------------------------------------------------------


@dataclass
class TableStats:
    table_name: str
    rows_seen: int = 0
    rows_inserted: int = 0
    rows_skipped: int = 0
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    finished_at: Optional[str] = None
    aborted_reason: Optional[str] = None

    def finalize(self, reason: Optional[str] = None) -> None:
        self.finished_at = datetime.now(timezone.utc).isoformat()
        self.aborted_reason = reason


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------


async def _bootstrap_irene_schema(irene_db_path: str) -> None:
    """Open irene's DB once via the production async path so every
    registered DDL (including our own batches table) lands. Closes
    immediately; the rest of the importer is sync."""
    db = DB(path=irene_db_path)
    await db.connect()
    await db.close()


def _open_seulgi_ro(path: str) -> sqlite3.Connection:
    if not Path(path).exists():
        raise FileNotFoundError(f"seulgi db not found: {path}")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _open_irene_rw(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, isolation_level=None)  # autocommit; we control txns
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


# ---------------------------------------------------------------------------
# Cycle
# ---------------------------------------------------------------------------


def _ensure_cycle(irene_conn: sqlite3.Connection) -> int:
    """Insert the import cycle row if absent; return its id."""
    irene_conn.execute(
        "INSERT OR IGNORE INTO cycles (name, opened_at) VALUES (?, ?)",
        (_CYCLE_NAME, _now_iso()),
    )
    row = irene_conn.execute(
        "SELECT id FROM cycles WHERE name = ?", (_CYCLE_NAME,)
    ).fetchone()
    if row is None:
        raise RuntimeError(f"failed to upsert cycle {_CYCLE_NAME!r}")
    return int(row[0])


# ---------------------------------------------------------------------------
# Articles
# ---------------------------------------------------------------------------


_ARTICLES_SELECT = """
SELECT s.headline_id, s.headline, s.detected_at, s.site_url AS signal_site_url,
       ha.article_url, ha.body, ha.site_url AS ha_site_url, ha.fetched_at
FROM signals s
LEFT JOIN headline_articles ha ON s.headline_id = ha.headline_id
WHERE s.headline IS NOT NULL AND length(s.headline) > 0
"""


def _import_articles(
    *,
    seulgi: sqlite3.Connection,
    irene: sqlite3.Connection,
    cycle_id: int,
    limit: Optional[int],
    dry_run: bool,
) -> TableStats:
    stats = TableStats(table_name="articles")
    sql = _ARTICLES_SELECT
    if limit is not None:
        sql += f" LIMIT {int(limit)}"

    cur = seulgi.execute(sql)
    now = _now_iso()
    pending: list[tuple] = []
    no_url_skipped = 0

    def _flush() -> None:
        nonlocal pending
        if not pending or dry_run:
            pending = []
            return
        irene.execute("BEGIN")
        try:
            irene.executemany(
                """
                INSERT OR IGNORE INTO articles
                    (source, source_detail, url, title, summary, published_at,
                     ingested_at, cycle_id, is_tracer)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                pending,
            )
            irene.execute("COMMIT")
        except Exception:
            irene.execute("ROLLBACK")
            raise
        pending = []

    total_changes_before = irene.total_changes
    while True:
        row = cur.fetchone()
        if row is None:
            break
        stats.rows_seen += 1
        # url: prefer the headline_articles.article_url. If absent, synthesize
        # a unique URL from the headline_id. Falling back to signal.site_url
        # (the publisher homepage) collapses ~16k distinct headlines to ~46
        # publisher rows — not what we want.
        headline_id = row["headline_id"]
        if row["article_url"]:
            url = row["article_url"]
        elif headline_id:
            url = f"seulgi-headline://{headline_id}"
        else:
            no_url_skipped += 1
            continue
        publisher = row["ha_site_url"] or row["signal_site_url"]
        pending.append((
            "seulgi_migration",
            publisher,
            url,
            row["headline"],
            row["body"],
            row["detected_at"],
            now,
            cycle_id,
        ))
        if len(pending) >= _COMMIT_CHUNK_ROWS:
            _flush()

    _flush()
    cur.close()

    # INSERT OR IGNORE: rows that hit the UNIQUE(url) clause count as 0
    # changes, so the cumulative delta equals successful inserts.
    inserted = irene.total_changes - total_changes_before
    stats.rows_inserted = inserted if not dry_run else 0
    if dry_run:
        # No writes occurred; treat all eligible rows as "would insert".
        eligible = stats.rows_seen - no_url_skipped
        stats.rows_skipped = no_url_skipped
        # We don't know the dedup rate without writing; report eligible
        # as a separate field via the caller's summary.
        stats.rows_inserted = 0  # explicit
        # Stash eligible count under skipped's complement is misleading;
        # callers should treat dry_run as informational only.
    else:
        unique_dedup_skipped = (stats.rows_seen - no_url_skipped) - stats.rows_inserted
        if unique_dedup_skipped < 0:
            unique_dedup_skipped = 0
        stats.rows_skipped = no_url_skipped + unique_dedup_skipped
    stats.finalize()
    return stats


# ---------------------------------------------------------------------------
# Markets
# ---------------------------------------------------------------------------


_MARKETS_SELECT = """
SELECT condition_id, question, token_yes, token_no, end_date, volume_usdc,
       embedding, market_type, closed_at, fetched_at
FROM markets
WHERE question IS NOT NULL
"""


def _import_markets(
    *,
    seulgi: sqlite3.Connection,
    irene: sqlite3.Connection,
    limit: Optional[int],
    dry_run: bool,
) -> TableStats:
    stats = TableStats(table_name="markets")
    sql = _MARKETS_SELECT
    if limit is not None:
        sql += f" LIMIT {int(limit)}"

    cur = seulgi.execute(sql)
    now = _now_iso()
    pending: list[tuple] = []

    def _flush() -> None:
        nonlocal pending
        if not pending or dry_run:
            pending = []
            return
        irene.execute("BEGIN")
        try:
            irene.executemany(
                """
                INSERT OR IGNORE INTO markets
                    (condition_id, question, token_yes, token_no, end_date,
                     volume_usdc, embedding, embedder_id, embedding_dim,
                     embedding_updated_at, active, closed_at,
                     first_seen_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                pending,
            )
            irene.execute("COMMIT")
        except Exception:
            irene.execute("ROLLBACK")
            raise
        pending = []

    total_changes_before = irene.total_changes
    for row in cur:
        stats.rows_seen += 1
        blob = row["embedding"]
        embedder_id: Optional[str] = None
        embedding_dim: Optional[int] = None
        if blob is not None:
            # Seulgi stores raw float32 bytes (confirmed: 1536 bytes = 384*4).
            vec = np.frombuffer(blob, dtype=np.float32)
            if vec.shape[0] != _SEULGI_EMBEDDING_DIM:
                raise RuntimeError(
                    f"market {row['condition_id']!r}: embedding dim "
                    f"{vec.shape[0]} != expected {_SEULGI_EMBEDDING_DIM}"
                )
            embedder_id = _SEULGI_EMBEDDER_ID
            embedding_dim = _SEULGI_EMBEDDING_DIM
        active = 0 if row["closed_at"] else 1
        first_seen = row["fetched_at"] or now
        pending.append((
            row["condition_id"],
            row["question"],
            row["token_yes"],
            row["token_no"],
            row["end_date"],
            row["volume_usdc"],
            blob,
            embedder_id,
            embedding_dim,
            row["fetched_at"],  # embedding_updated_at — best proxy seulgi has
            active,
            row["closed_at"],
            first_seen,
            now,
        ))
        if len(pending) >= _COMMIT_CHUNK_ROWS:
            _flush()

    _flush()
    cur.close()

    inserted = irene.total_changes - total_changes_before
    stats.rows_inserted = inserted if not dry_run else 0
    stats.rows_skipped = stats.rows_seen - stats.rows_inserted
    if stats.rows_skipped < 0:
        stats.rows_skipped = 0
    stats.finalize()
    return stats


# ---------------------------------------------------------------------------
# Batch log
# ---------------------------------------------------------------------------


def _log_batch(
    irene: sqlite3.Connection, *, batch_id: str, stats: TableStats
) -> None:
    irene.execute(
        """
        INSERT INTO seulgi_migration_batches
            (batch_id, table_name, started_at, finished_at,
             rows_seen, rows_inserted, rows_skipped, aborted_reason)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(batch_id, table_name) DO UPDATE SET
            finished_at = excluded.finished_at,
            rows_seen = excluded.rows_seen,
            rows_inserted = excluded.rows_inserted,
            rows_skipped = excluded.rows_skipped,
            aborted_reason = excluded.aborted_reason
        """,
        (
            batch_id,
            stats.table_name,
            stats.started_at,
            stats.finished_at,
            stats.rows_seen,
            stats.rows_inserted,
            stats.rows_skipped,
            stats.aborted_reason,
        ),
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run_inproc(
    *,
    seulgi_db: str,
    irene_db: str,
    limit: Optional[int] = None,
    dry_run: bool = False,
    batch_id: Optional[str] = None,
) -> dict:
    """Programmatic entry. Returns a summary dict."""
    asyncio.run(_bootstrap_irene_schema(irene_db))

    seulgi = _open_seulgi_ro(seulgi_db)
    irene = _open_irene_rw(irene_db)
    try:
        cycle_id = _ensure_cycle(irene)
        batch_id = batch_id or (
            f"seulgi-import-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-"
            f"{uuid.uuid4().hex[:6]}"
        )
        t_start = time.monotonic()

        articles_stats = _import_articles(
            seulgi=seulgi, irene=irene, cycle_id=cycle_id,
            limit=limit, dry_run=dry_run,
        )
        _log_batch(irene, batch_id=batch_id, stats=articles_stats)

        markets_stats = _import_markets(
            seulgi=seulgi, irene=irene,
            limit=limit, dry_run=dry_run,
        )
        _log_batch(irene, batch_id=batch_id, stats=markets_stats)

        elapsed = time.monotonic() - t_start
        return {
            "batch_id": batch_id,
            "cycle_id": cycle_id,
            "cycle_name": _CYCLE_NAME,
            "dry_run": dry_run,
            "elapsed_seconds": round(elapsed, 2),
            "tables": {
                "articles": {
                    "rows_seen": articles_stats.rows_seen,
                    "rows_inserted": articles_stats.rows_inserted,
                    "rows_skipped": articles_stats.rows_skipped,
                },
                "markets": {
                    "rows_seen": markets_stats.rows_seen,
                    "rows_inserted": markets_stats.rows_inserted,
                    "rows_skipped": markets_stats.rows_skipped,
                },
            },
        }
    finally:
        irene.close()
        seulgi.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="One-time import of seulgi data-layer rows into irene.db.",
    )
    parser.add_argument(
        "--seulgi-db",
        default="/home/muk/projects/jennie/seulgi/data/seulgi.db",
    )
    parser.add_argument(
        "--irene-db",
        default="/home/muk/projects/jennie/irene/data/irene.db",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="If set, cap each importer at N rows seen (for dry runs).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Read seulgi + count rows but don't write to irene.",
    )
    parser.add_argument("--batch-id", default=None)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    try:
        summary = run_inproc(
            seulgi_db=args.seulgi_db,
            irene_db=args.irene_db,
            limit=args.limit,
            dry_run=args.dry_run,
            batch_id=args.batch_id,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("migration failed: %s", exc)
        return 2

    import json
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

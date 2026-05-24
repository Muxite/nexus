"""
## Specification

**Purpose.** One-shot resolver backfill. Walks every `extraction_runs`
row with `status='ok'` that has at least one entity lacking an
`entity_resolutions` row, and invokes the live `ResolveStage.handle()`
against a synthesised payload so the deterministic rule chain
(`acronym` / `alias` / `suffix` / `self`) fires for the unresolved
entities.

Needed because `scripts/dkg/extract_to_irene.py` writes
`extraction_runs` + `entities` + `facts` directly to the DB without
publishing on the bus, so the live resolver (a
`TOPIC_EXTRACTION_COMPLETED` subscriber) never fires for those
articles. This script closes the loop without re-routing the live
extract path through the bus.

**Public interface.**
- CLI: `python -m scripts.dkg.run_resolver_backfill --irene-db <path>
        [--batch-size N] [--max-articles N]`
- `run_inproc(irene_db: str, *, batch_size: int = 200,
              max_articles: Optional[int] = None) -> dict`

**Invariants.**
- Idempotent: `entity_resolutions.UNIQUE(entity_id)` makes re-runs
  safe (already-resolved entities are skipped by the resolver's own
  `_fetch_unresolved_entities` SELECT).
- Doesn't mutate `entities`, `facts`, or `extraction_runs`. Only
  `entity_resolutions` is written.
- Uses the same `ResolveStage` instance that runs live in the bus
  pipeline — single code path for the rule chain, no drift risk.

**Side effects.** Bootstraps the irene schema (DB.connect once); reads
`extraction_runs`, `entities`, `facts`, `entity_aliases`,
`acronyms.yaml`; writes `entity_resolutions`.

**Non-goals.**
- Doesn't republish `TOPIC_RESOLUTION_COMPLETED` to downstream
  subscribers. The script uses a stub bus that swallows publishes —
  no one is listening offline.
- Doesn't change the resolver rule chain. Bump
  `RESOLVER_VERSION` in `app/stages/resolve.py` and re-run if you
  want a new resolution pass.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Optional

# Register schemas + import stage modules.
import app.main  # noqa: F401
import app.stages.dedupe  # noqa: F401
import app.stages.extract  # noqa: F401
import app.stages.ingest  # noqa: F401
import app.stages.markets  # noqa: F401
import app.stages.match  # noqa: F401
import app.stages.resolve  # noqa: F401
import app.stages.trade  # noqa: F401
import app.ai  # noqa: F401
from app.db import DB
from app.deps import StageDeps
from app.stages.resolve import ResolveStage

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stub bus — resolve.handle() publishes on completion; nobody's listening
# during a backfill, so we swallow the call.
# ---------------------------------------------------------------------------


class _StubBus:
    """Minimal bus implementation that no-ops every operation. Just
    enough surface that `ResolveStage._publish_completion` doesn't
    crash when it calls `bus.publish()`."""

    async def publish(self, topic: str, payload: dict) -> None:
        # JSON-roundtrip the payload so we'd catch any non-JSON content
        # the same way the live bus would.
        json.dumps(payload)

    def subscribe(self, topic: str, handler) -> None:  # noqa: ARG002
        pass

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


@dataclass
class _StubConfig:
    """ResolveStage only reads `deps.db` + `deps.bus`; an empty config
    shim satisfies the StageDeps frozen dataclass."""


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------


@dataclass
class BackfillStats:
    articles_seen: int = 0
    articles_with_unresolved: int = 0
    articles_skipped_no_facts: int = 0
    entities_resolved: int = 0
    merges: int = 0
    started_at: float = field(default_factory=time.monotonic)

    def to_dict(self) -> dict:
        elapsed = time.monotonic() - self.started_at
        return {
            "articles_seen": self.articles_seen,
            "articles_with_unresolved": self.articles_with_unresolved,
            "articles_skipped_no_facts": self.articles_skipped_no_facts,
            "entities_resolved": self.entities_resolved,
            "merges": self.merges,
            "elapsed_seconds": round(elapsed, 2),
            "rate_per_sec": round(
                self.articles_with_unresolved / max(elapsed, 0.001), 2
            ),
        }


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------


async def _select_target_articles(
    db: DB, *, max_articles: Optional[int]
) -> list[dict]:
    """Article ids that have at least one fact-referenced entity without
    an `entity_resolutions` row. This mirrors the LEFT JOIN the resolver
    itself uses in `_fetch_unresolved_entities` but at article granularity
    so we only fire one handle() per article."""
    sql = """
        SELECT DISTINCT er.article_id, er.id AS extraction_run_id,
               er.is_tracer, er.tracer_id
        FROM extraction_runs er
        WHERE er.status = 'ok'
          AND EXISTS (
              SELECT 1 FROM facts f
              JOIN entities e ON f.subj_entity_id = e.id
                              OR f.obj_entity_id = e.id
              LEFT JOIN entity_resolutions r ON r.entity_id = e.id
              WHERE f.article_id = er.article_id
                AND r.id IS NULL
          )
        ORDER BY er.article_id
    """
    if max_articles is not None:
        sql += f" LIMIT {int(max_articles)}"
    cur = await db.conn.execute(sql)
    rows = await cur.fetchall()
    await cur.close()
    return [
        {
            "article_id": int(r["article_id"]),
            "extraction_run_id": int(r["extraction_run_id"]),
            "is_tracer": bool(r["is_tracer"]),
            "tracer_id": r["tracer_id"],
        }
        for r in rows
    ]


async def run_async(
    *,
    irene_db: str,
    batch_size: int = 200,
    max_articles: Optional[int] = None,
) -> dict:
    db = DB(path=irene_db)
    await db.connect()
    bus = _StubBus()
    deps = StageDeps(db=db, bus=bus, config=_StubConfig())
    stage = ResolveStage(deps)
    await stage.start()

    stats = BackfillStats()
    try:
        # Snapshot the target list once; the resolver's own SELECT inside
        # handle() also gates on unresolved-ness, so re-processing the
        # same article twice is a no-op even if the snapshot is stale.
        targets = await _select_target_articles(db, max_articles=max_articles)
        stats.articles_seen = len(targets)
        if not targets:
            logger.info("backfill: nothing to resolve")
            return {"stats": stats.to_dict(), "by_kind": _kind_counts(db)}

        logger.info("backfill: %d articles have unresolved entities", len(targets))
        for i, t in enumerate(targets, 1):
            payload = {
                "status": "ok",
                "extraction_run_id": t["extraction_run_id"],
                "article_id": t["article_id"],
                "is_tracer": t["is_tracer"],
                "tracer_id": t["tracer_id"],
            }
            before = await _count_resolutions(db)
            await stage.handle(payload)
            after = await _count_resolutions(db)
            new_rows = after - before
            if new_rows > 0:
                stats.articles_with_unresolved += 1
                stats.entities_resolved += new_rows
            else:
                stats.articles_skipped_no_facts += 1
            if i % batch_size == 0:
                elapsed = time.monotonic() - stats.started_at
                logger.info(
                    "backfill: %d/%d articles @ %.1f/s (rows so far: %d)",
                    i, len(targets), i / max(elapsed, 0.001),
                    stats.entities_resolved,
                )

        stats.merges = await _count_merges(db)
        return {
            "stats": stats.to_dict(),
            "by_kind": await _kind_counts_async(db),
        }
    finally:
        await stage.stop()
        await db.close()


async def _count_resolutions(db: DB) -> int:
    cur = await db.conn.execute("SELECT COUNT(*) FROM entity_resolutions")
    row = await cur.fetchone()
    await cur.close()
    return int(row[0])


async def _count_merges(db: DB) -> int:
    cur = await db.conn.execute(
        "SELECT COUNT(*) FROM entity_resolutions "
        "WHERE resolution_kind != 'self'"
    )
    row = await cur.fetchone()
    await cur.close()
    return int(row[0])


async def _kind_counts_async(db: DB) -> dict[str, int]:
    cur = await db.conn.execute(
        "SELECT resolution_kind, COUNT(*) FROM entity_resolutions "
        "GROUP BY resolution_kind"
    )
    rows = await cur.fetchall()
    await cur.close()
    return {r[0]: int(r[1]) for r in rows}


def _kind_counts(db: DB) -> dict[str, int]:
    # Sync wrapper for the empty-case return path before the loop starts.
    return {}


def run_inproc(
    *,
    irene_db: str,
    batch_size: int = 200,
    max_articles: Optional[int] = None,
) -> dict:
    return asyncio.run(run_async(
        irene_db=irene_db,
        batch_size=batch_size,
        max_articles=max_articles,
    ))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Backfill entity_resolutions for offline-extracted articles.",
    )
    parser.add_argument(
        "--irene-db",
        default="/home/muk/projects/jennie/irene/data/irene.db",
    )
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--max-articles", type=int, default=None)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    try:
        summary = run_inproc(
            irene_db=args.irene_db,
            batch_size=args.batch_size,
            max_articles=args.max_articles,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("backfill failed: %s", exc)
        return 2
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""
## Specification

**Purpose.** One-shot eventize backfill. Walks every `extraction_runs`
row with `status='ok'` that has at least one fact lacking an
`event_id`, constructs the resolution-completed-shaped payload the
EventizeStage expects, and invokes `stage.handle()` against the live
ResolveStage's output. Closes the loop after the offline
`extract_to_irene.py` → `run_resolver_backfill.py` chain that bypasses
the bus.

**Public interface.**
- CLI: `python -m scripts.dkg.run_eventize_backfill --irene-db <path>
        [--max-articles N] [--progress-every N]`
- `run_inproc(irene_db, *, max_articles=None, progress_every=100) -> dict`

**Invariants.**
- Idempotent — facts already carrying `event_id` are skipped by
  `EventizeStage.handle()` directly; re-runs are safe.
- Uses the SAME `EventizeStage` instance as the live pipeline — single
  algorithm, no drift risk.
- Reads `facts` + `entities` + `entity_resolutions` + `articles`.
  Writes `events`, `event_edges`, `facts.event_id`.

**Side effects.** Bootstraps the irene schema (DB.connect once),
loads `event_types.yaml`, fires `EventizeStage.handle()` per article
that needs eventizing.

**Non-goals.**
- Doesn't republish `TOPIC_EVENT_UPDATED` to downstream subscribers
  (no one is listening offline).
- Doesn't recompute event_id for facts that already have one. Bump
  `EVENTIZE_VERSION` in `app/stages/eventize.py` and re-run if you
  want a new clustering pass.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

# Register all DDLs.
import app.main  # noqa: F401
import app.stages.dedupe  # noqa: F401
import app.stages.eventize  # noqa: F401
import app.stages.extract  # noqa: F401
import app.stages.ingest  # noqa: F401
import app.stages.markets  # noqa: F401
import app.stages.match  # noqa: F401
import app.stages.resolve  # noqa: F401
import app.stages.trade  # noqa: F401
import app.ai  # noqa: F401
from app.db import DB
from app.deps import StageDeps
from app.embed import Embedder
from app.stages.eventize import EventizeStage

logger = logging.getLogger(__name__)


class _StubBus:
    async def publish(self, topic: str, payload: dict) -> None:
        json.dumps(payload)
    def subscribe(self, topic: str, handler) -> None: pass
    async def start(self) -> None: pass
    async def stop(self) -> None: pass


@dataclass
class _StubConfig:
    pass


@dataclass
class BackfillStats:
    articles_seen: int = 0
    articles_eventized: int = 0
    events_created: int = 0
    events_confirmed: int = 0
    started_at: float = field(default_factory=time.monotonic)

    def to_dict(self) -> dict:
        elapsed = time.monotonic() - self.started_at
        return {
            "articles_seen": self.articles_seen,
            "articles_eventized": self.articles_eventized,
            "events_created": self.events_created,
            "events_confirmed": self.events_confirmed,
            "elapsed_seconds": round(elapsed, 2),
            "rate_per_sec": round(
                self.articles_eventized / max(elapsed, 0.001), 2,
            ),
        }


async def _select_targets(
    db: DB, *, max_articles: Optional[int],
) -> list[dict]:
    """Articles whose ok-status extraction_runs have at least one fact
    with NULL event_id. Sorted by published_at so events accumulate in
    temporal order (matters for the time-window logic)."""
    sql = """
        SELECT DISTINCT a.id AS article_id, er.id AS extraction_run_id,
               er.is_tracer, er.tracer_id, a.published_at
        FROM articles a
        JOIN extraction_runs er ON er.article_id = a.id AND er.status = 'ok'
        WHERE EXISTS (
            SELECT 1 FROM facts f
            WHERE f.article_id = a.id AND f.event_id IS NULL
        )
        ORDER BY a.published_at, a.id
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
    max_articles: Optional[int] = None,
    progress_every: int = 100,
) -> dict:
    db = DB(path=irene_db)
    await db.connect()
    embedder = Embedder("sentence-transformers/all-MiniLM-L6-v2", 384)
    # Warm the embedder.
    _ = embedder.embed("warmup")
    bus = _StubBus()
    deps = StageDeps(db=db, bus=bus, config=_StubConfig(), embedder=embedder)
    stage = EventizeStage(deps)
    await stage.start()

    stats = BackfillStats()
    try:
        targets = await _select_targets(db, max_articles=max_articles)
        stats.articles_seen = len(targets)
        if not targets:
            logger.info("eventize-backfill: nothing to do")
            return {"stats": stats.to_dict(), "summary": _summarize(db)}

        logger.info("eventize-backfill: %d articles to eventize", len(targets))

        # Recording bus captures per-handle counters.
        class _RecordingBus(_StubBus):
            def __init__(self):
                self.records: list[dict] = []
            async def publish(self, topic, payload):
                self.records.append(payload)
        rec = _RecordingBus()
        # Swap in the recording bus.
        deps_recording = StageDeps(db=db, bus=rec, config=_StubConfig(), embedder=embedder)
        stage_recording = EventizeStage(deps_recording)
        await stage_recording.start()

        for i, t in enumerate(targets, 1):
            await stage_recording.handle({
                "article_id": t["article_id"],
                "extraction_run_id": t["extraction_run_id"],
                "is_tracer": t["is_tracer"],
                "tracer_id": t["tracer_id"],
            })
            stats.articles_eventized += 1
            if i % progress_every == 0:
                published_so_far = sum(r["events_created"] + r["events_confirmed"]
                                       for r in rec.records)
                elapsed = time.monotonic() - stats.started_at
                logger.info(
                    "eventize-backfill: %d/%d @ %.1f/s (fact-events processed: %d)",
                    i, len(targets), i / max(elapsed, 0.001), published_so_far,
                )

        # Final counters from the recording bus.
        stats.events_created = sum(r["events_created"] for r in rec.records)
        stats.events_confirmed = sum(r["events_confirmed"] for r in rec.records)

        await stage_recording.stop()
        return {"stats": stats.to_dict(), "summary": await _summarize_async(db)}
    finally:
        await stage.stop()
        await db.close()


def _summarize(db: DB) -> dict:
    return {}


async def _summarize_async(db: DB) -> dict:
    """End-state shape: counts + event_type distribution."""
    cur = await db.conn.execute("SELECT COUNT(*) FROM events")
    n_events = (await cur.fetchone())[0]
    await cur.close()
    cur = await db.conn.execute("SELECT COUNT(*) FROM event_edges")
    n_edges = (await cur.fetchone())[0]
    await cur.close()
    cur = await db.conn.execute(
        "SELECT COUNT(*) FROM facts WHERE event_id IS NOT NULL"
    )
    n_facts_with_eid = (await cur.fetchone())[0]
    await cur.close()
    cur = await db.conn.execute(
        "SELECT event_type, COUNT(*) FROM events GROUP BY event_type ORDER BY 2 DESC"
    )
    types = await cur.fetchall()
    await cur.close()
    cur = await db.conn.execute(
        "SELECT support_count, COUNT(*) FROM events GROUP BY support_count "
        "ORDER BY support_count"
    )
    supports = await cur.fetchall()
    await cur.close()
    return {
        "events": n_events,
        "event_edges": n_edges,
        "facts_with_event_id": n_facts_with_eid,
        "event_type_counts": [(r[0], r[1]) for r in types],
        "support_histogram": [(r[0], r[1]) for r in supports],
    }


def run_inproc(
    *,
    irene_db: str,
    max_articles: Optional[int] = None,
    progress_every: int = 100,
) -> dict:
    return asyncio.run(run_async(
        irene_db=irene_db,
        max_articles=max_articles,
        progress_every=progress_every,
    ))


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Backfill events for offline-extracted facts.",
    )
    parser.add_argument(
        "--irene-db",
        default="/home/muk/projects/nexus/irene/data/irene.db",
    )
    parser.add_argument("--max-articles", type=int, default=None)
    parser.add_argument("--progress-every", type=int, default=100)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    try:
        out = run_inproc(
            irene_db=args.irene_db,
            max_articles=args.max_articles,
            progress_every=args.progress_every,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("eventize-backfill failed: %s", exc)
        return 2
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

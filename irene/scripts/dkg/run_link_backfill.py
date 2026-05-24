"""
## Specification

**Purpose.** One-shot linker backfill. Walks every event (or just the
events lacking `market_links` rows under the current `LINKER_VERSION`)
and scores it against all active markets via `LinkerStage`. Closes the
loop after offline eventize: the live stage is bus-driven, but our
backfill chain skips the bus.

**Public interface.**
- CLI: `python -m scripts.dkg.run_link_backfill --irene-db <path>
        [--max-events N]`
- `run_inproc(irene_db, *, max_events=None) -> dict`

**Invariants.**
- Idempotent — `market_links` UNIQUE on (event_id, market_condition_id,
  link_class, linker_version) makes re-runs no-ops over existing links.
- Same `LinkerStage` instance the live pipeline will use; no parallel
  codepath.

**Side effects.** Reads `events` + `markets`. Writes `market_links`.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

import app.main  # noqa: F401
import app.stages.dedupe  # noqa: F401
import app.stages.eventize  # noqa: F401
import app.stages.extract  # noqa: F401
import app.stages.ingest  # noqa: F401
import app.stages.link  # noqa: F401
import app.stages.markets  # noqa: F401
import app.stages.match  # noqa: F401
import app.stages.resolve  # noqa: F401
import app.stages.trade  # noqa: F401
import app.ai  # noqa: F401
from app.db import DB
from app.deps import StageDeps
from app.embed import Embedder
from app.stages.link import LinkerStage

logger = logging.getLogger(__name__)


class _StubBus:
    def __init__(self): self.records = []
    async def publish(self, topic, payload): self.records.append(payload)
    def subscribe(self, topic, handler): pass
    async def start(self): pass
    async def stop(self): pass


@dataclass
class _StubConfig:
    pass


@dataclass
class BackfillStats:
    events_seen: int = 0
    events_linked: int = 0
    links_written: int = 0
    started_at: float = field(default_factory=time.monotonic)

    def to_dict(self) -> dict:
        elapsed = time.monotonic() - self.started_at
        return {
            "events_seen": self.events_seen,
            "events_linked": self.events_linked,
            "links_written": self.links_written,
            "elapsed_seconds": round(elapsed, 2),
            "rate_per_sec": round(
                self.events_linked / max(elapsed, 0.001), 2,
            ),
        }


async def _select_targets(db: DB, *, max_events: Optional[int]) -> list[int]:
    """Event ids that don't have any market_links rows yet under
    LINKER_VERSION = current. Top-up only — already-scored events are
    skipped."""
    sql = """
        SELECT e.id FROM events e
        WHERE NOT EXISTS (
            SELECT 1 FROM market_links ml
            WHERE ml.event_id = e.id AND ml.linker_version = 'v1'
        )
        ORDER BY e.support_count DESC, e.id
    """
    if max_events is not None:
        sql += f" LIMIT {int(max_events)}"
    cur = await db.conn.execute(sql)
    rows = await cur.fetchall()
    await cur.close()
    return [int(r["id"]) for r in rows]


async def _fetch_one_article_id_for_event(db: DB, event_id: int) -> Optional[int]:
    """LinkerStage.handle() takes an article payload; we need an
    article that hosts at least one of the event's facts."""
    cur = await db.conn.execute(
        "SELECT article_id FROM facts WHERE event_id = ? LIMIT 1",
        (event_id,),
    )
    row = await cur.fetchone()
    await cur.close()
    return int(row[0]) if row else None


async def run_async(*, irene_db: str, max_events: Optional[int]) -> dict:
    db = DB(path=irene_db)
    await db.connect()
    embedder = Embedder("sentence-transformers/all-MiniLM-L6-v2", 384)
    _ = embedder.embed("warmup")
    bus = _StubBus()
    deps = StageDeps(db=db, bus=bus, config=_StubConfig(), embedder=embedder)
    stage = LinkerStage(deps)
    await stage.start()

    stats = BackfillStats()
    try:
        target_event_ids = await _select_targets(db, max_events=max_events)
        stats.events_seen = len(target_event_ids)
        if not target_event_ids:
            logger.info("link-backfill: nothing to score")
            return {"stats": stats.to_dict(), "summary": await _summarize(db)}

        logger.info("link-backfill: %d events to score", len(target_event_ids))
        for i, eid in enumerate(target_event_ids, 1):
            aid = await _fetch_one_article_id_for_event(db, eid)
            if aid is None:
                continue
            await stage.handle({"article_id": aid, "event_id": eid})
            stats.events_linked += 1
            if i % 200 == 0:
                elapsed = time.monotonic() - stats.started_at
                logger.info(
                    "link-backfill: %d/%d @ %.1f/s",
                    i, len(target_event_ids), i / max(elapsed, 0.001),
                )
        stats.links_written = sum(
            r.get("links_written", 0) for r in bus.records
        )
        return {"stats": stats.to_dict(), "summary": await _summarize(db)}
    finally:
        await stage.stop()
        await db.close()


async def _summarize(db: DB) -> dict:
    cur = await db.conn.execute("SELECT COUNT(*) FROM market_links")
    n_links = (await cur.fetchone())[0]
    await cur.close()
    cur = await db.conn.execute(
        "SELECT MIN(final_score), MAX(final_score), AVG(final_score) FROM market_links"
    )
    mn, mx, av = await cur.fetchone()
    await cur.close()
    cur = await db.conn.execute(
        "SELECT link_class, COUNT(*) FROM market_links GROUP BY link_class"
    )
    classes = await cur.fetchall()
    await cur.close()
    cur = await db.conn.execute(
        "SELECT directness_score, novelty_score, deadline_fit_score, "
        "resolution_fit_score, final_score FROM market_links"
    )
    all_rows = await cur.fetchall()
    await cur.close()
    components = ["directness", "novelty", "deadline_fit", "resolution_fit"]
    pct = {}
    if all_rows:
        import numpy as np
        arr = np.array([list(r) for r in all_rows], dtype=np.float32)
        for i, name in enumerate(components):
            col = arr[:, i]
            pct[name] = {
                "min": float(col.min()), "max": float(col.max()),
                "mean": float(col.mean()), "std": float(col.std()),
            }
        pct["final"] = {
            "min": float(arr[:, 4].min()), "max": float(arr[:, 4].max()),
            "mean": float(arr[:, 4].mean()), "std": float(arr[:, 4].std()),
            "p95": float(np.percentile(arr[:, 4], 95)),
            "p99": float(np.percentile(arr[:, 4], 99)),
        }
    return {
        "market_links_total": n_links,
        "final_score_min": float(mn or 0), "final_score_max": float(mx or 0),
        "final_score_mean": float(av or 0),
        "by_link_class": {r[0]: int(r[1]) for r in classes},
        "component_stats": pct,
    }


def run_inproc(*, irene_db: str, max_events: Optional[int] = None) -> dict:
    return asyncio.run(run_async(irene_db=irene_db, max_events=max_events))


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Backfill market_links over the populated DKG.",
    )
    parser.add_argument(
        "--irene-db",
        default="/home/muk/projects/jennie/irene/data/irene.db",
    )
    parser.add_argument("--max-events", type=int, default=None)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    try:
        out = run_inproc(
            irene_db=args.irene_db, max_events=args.max_events,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("link-backfill failed: %s", exc)
        return 2
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

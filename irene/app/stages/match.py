"""
## Specification

**Purpose.** Subscriber on `article.deduped`. For each new article, embed the
title, query the shared `MarketIndex` for top-k candidate markets above the
similarity floor, write one immutable `signals` row per `(article, market)`
pair, and publish `signal.created` per row so `trade.py` can take it from
there.

**Public interface.**
- `MatchStage`:
  - `MatchStage(deps: StageDeps)` — requires `deps.embedder` AND
    `deps.market_index`.
  - `await start()` — no-op (subscriber stage; the bus drives it).
  - `await stop()` — idempotent.
  - `await handle(payload: dict)` — bus subscriber.

**Owned tables** (modularization rule 4): `signals`. Schema registered with
`db.register_schema()` at import time.

**Invariants.**
- `signals` carries no `direction` column — that lives in `predictions`,
  written by `ai.py`. Splitting structural match (signals) from the LLM
  call (predictions) is the seulgi lesson that motivated this redesign.
- `UNIQUE(article_id, market_id)` keeps re-ingest idempotent — `INSERT OR
  IGNORE` and only publish when `cur.rowcount == 1`.
- `cycle_id` stamped from `cycle.current_cycle_id()` on every row.
- `is_tracer` / `tracer_id` threaded from the inbound payload onto every
  row.
- `embedder_id` stamped on every row from `deps.embedder.embedder_id` so
  a future dim/space rotation is detectable (no silent space mixing).

**Side effects.** Reads `articles` (the title column). Calls
`deps.embedder.embed()`. Calls `deps.market_index.topk()`. Writes `signals`
**and** a `decision_inputs` capture row inside the same `db.batch()` (per
`docs/p1_contracts.md` §7 — capture cannot lag the live write). Publishes
`TOPIC_SIGNAL_CREATED` per inserted row.

**Failure modes.**
- Article missing (race with ingest): log WARNING, no signals.
- `market_index.size == 0` (index not yet loaded): log DEBUG, no signals.
- Top-k returns empty (nothing above floor): no signals (this is the funnel
  drop point — every article eventually lands here when no candidates
  match).

**Non-goals.**
- LLM direction calls (those live in `ai.py`, invoked by `trade.py`).
- The freshness/STALE gate — match passes `freshness` along on the payload
  and writes it on the row, but the *filter* lives in `trade.py`.
- Filtering by article category / source — that's a future quality knob.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from app import db as db_module
from app.cycle import current_cycle_id
from app.deps import StageDeps
from app.topics import TOPIC_SIGNAL_CREATED

logger = logging.getLogger(__name__)


_MATCH_DDL = [
    """
    CREATE TABLE IF NOT EXISTS signals (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        article_id      INTEGER NOT NULL,
        market_id       TEXT NOT NULL,
        similarity      REAL NOT NULL,
        cluster_id      INTEGER,
        freshness       TEXT,
        embedder_id     TEXT NOT NULL,
        cycle_id        INTEGER NOT NULL,
        is_tracer       INTEGER NOT NULL DEFAULT 0,
        tracer_id       TEXT,
        created_at      TEXT NOT NULL,
        UNIQUE(article_id, market_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_signals_article_id ON signals(article_id)",
    "CREATE INDEX IF NOT EXISTS idx_signals_market_id ON signals(market_id)",
    "CREATE INDEX IF NOT EXISTS idx_signals_cycle_id ON signals(cycle_id)",
    "CREATE INDEX IF NOT EXISTS idx_signals_created_at ON signals(created_at)",
]
db_module.register_schema("app/stages/match.py", _MATCH_DDL)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class MatchStage:
    def __init__(self, deps: StageDeps) -> None:
        if deps.embedder is None:
            raise RuntimeError("MatchStage requires deps.embedder")
        if deps.market_index is None:
            raise RuntimeError("MatchStage requires deps.market_index")
        self._db = deps.db
        self._bus = deps.bus
        self._embedder = deps.embedder
        self._index = deps.market_index
        self._floor = deps.config.match.similarity_floor
        self._top_k = deps.config.match.top_k

    async def start(self) -> None:
        # No background task — the bus drives handle().
        return

    async def stop(self) -> None:
        return

    async def handle(self, payload: dict) -> None:
        article_id = payload.get("article_id")
        if article_id is None:
            logger.warning("match: payload missing article_id: %r", payload)
            return
        cluster_id = payload.get("cluster_id")
        freshness = payload.get("freshness")
        is_tracer = bool(payload.get("is_tracer", False))
        tracer_id = payload.get("tracer_id")

        article = await self._fetch_title(int(article_id))
        if article is None:
            logger.warning("match: article %s not found in articles table", article_id)
            return
        title = article["title"]

        if self._index.size == 0:
            logger.debug("match: market_index empty, skipping article %s", article_id)
            return

        # Embed title + a slice of the summary. The headline alone often
        # misses the real claim ("Pakistan PM hails Trump" doesn't say
        # what about); the first ~300 chars of summary disambiguates.
        query_text = title
        summary = (article.get("summary") or "").strip()
        if summary:
            query_text = f"{title}. {summary[:300]}"
        query_vec = self._embedder.embed(query_text)
        candidates = self._index.topk(query_vec, k=self._top_k, floor=self._floor)
        if not candidates:
            return

        cycle_id = current_cycle_id()
        embedder_id = self._embedder.embedder_id

        for market_id, similarity in candidates:
            signal_id = await self._insert_signal_with_capture(
                payload=payload,
                article_id=int(article_id),
                title=title,
                market_id=market_id,
                similarity=float(similarity),
                cluster_id=cluster_id,
                freshness=freshness,
                embedder_id=embedder_id,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            if signal_id is None:
                # UNIQUE conflict — already matched on a previous pass. Skip
                # the publish so the trade stage doesn't double-process.
                continue
            await self._bus.publish(
                TOPIC_SIGNAL_CREATED,
                {
                    "signal_id": signal_id,
                    "article_id": int(article_id),
                    "market_id": market_id,
                    "similarity": float(similarity),
                    "freshness": freshness,
                    "is_tracer": is_tracer,
                    "tracer_id": tracer_id,
                },
            )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _fetch_title(self, article_id: int) -> Optional[dict]:
        cur = await self._db.conn.execute(
            "SELECT title, summary FROM articles WHERE id = ?", (article_id,)
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return {"title": row["title"], "summary": row["summary"]}

    async def _insert_signal_with_capture(
        self,
        *,
        payload: dict,
        article_id: int,
        title: str,
        market_id: str,
        similarity: float,
        cluster_id: Optional[int],
        freshness: Optional[str],
        embedder_id: str,
        cycle_id: int,
        is_tracer: bool,
        tracer_id: Optional[str],
    ) -> Optional[int]:
        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT OR IGNORE INTO signals
                    (article_id, market_id, similarity, cluster_id, freshness,
                     embedder_id, cycle_id, is_tracer, tracer_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    article_id,
                    market_id,
                    similarity,
                    cluster_id,
                    freshness,
                    embedder_id,
                    cycle_id,
                    1 if is_tracer else 0,
                    tracer_id,
                    _now_iso(),
                ),
            )
            if cur.rowcount != 1:
                return None
            signal_id = cur.lastrowid
            # Capture lives inside the same batch — if either insert fails,
            # both roll back. Snapshot shape is fixed per p1_contracts §7.
            await self._db.capture(
                cur,
                kind="signal",
                ref_id=signal_id,
                payload=payload,
                snapshot={
                    "article_id": article_id,
                    "market_id": market_id,
                    "similarity": similarity,
                    "cluster_id": cluster_id,
                    "freshness": freshness,
                    "embedder_id": embedder_id,
                    "title": title,
                },
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return signal_id

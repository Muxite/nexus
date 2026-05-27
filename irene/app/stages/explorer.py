"""
## Specification

**Purpose.** Noisy-exploration sister of `app/stages/dkg_trade.py`.
Subscribes to `TOPIC_MARKET_LINKS_UPDATED`; for every link with
`final_score >= cfg.explorer.score_floor` (permissive, default 0.5
vs production 0.8), runs a cheap LLM relatedness check and — when
`related=True` — writes an `explorations` row and publishes
`TOPIC_EXPLORATION_CREATED` so `odds_capture.py` can start the
[-10m, +20m] odds time-series.

This stage does NOT trade. It exists to build a labeled dataset for
offline filter/hypothesis backtests. Treat the `explorations` log as
a research dataset, not a verdict on the strategy.

**Public interface.**
- `ExplorerStage(deps: StageDeps)` — requires `deps.ai`, `deps.clob`.
- `await start()` / `await stop()` — no-ops; the bus drives it.
- `await handle(payload: dict)` — bus subscriber.

**Owned tables.** `explorations`. DDL registered with
`db_module.register_schema()` at import time.

**Idempotency.** `UNIQUE(article_id, market_condition_id)` makes
re-fires safe — `INSERT OR IGNORE` returns 0 on a duplicate and the
stage short-circuits without re-calling the LLM.

**Side effects.**
- Reads `market_links`, `events`, `facts`, `articles`, `markets`.
- Calls `deps.ai.relatedness()` (charged to the OpenRouter budget).
- Calls `deps.clob.get_top_of_book()` for the t0 mid/spread snapshot.
- Writes `explorations`. Publishes `TOPIC_EXPLORATION_CREATED`.

**Failure modes.**
- Score below floor / event missing / article missing / market missing
  → silent skip. No row written. The explorer is deliberately quiet
  about rejections — we only persist the cases we *captured*.
- Budget paused → relatedness call returns `related=False`; we skip.
- LLM parse error → relatedness call returns `related=False`; we skip.
- CLOB unavailable at t0 snapshot → `market_mid_t0` / `market_spread_t0`
  are NULL on the row but the row is still written.

**Non-goals.**
- Trading, dashboard, PnL — see `trade.py` / `dkg_trade.py`.
- Strict gates (similarity floor, freshness, expiry, edge, size) —
  the explorer is permissive on admission by design.
- Pre-news / post-news odds capture — that's `odds_capture.py`.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from app import db as db_module
from app.cycle import current_cycle_id
from app.deps import StageDeps
from app.topics import TOPIC_EXPLORATION_CREATED, TOPIC_MARKET_LINKS_UPDATED

logger = logging.getLogger(__name__)


_EXPLORER_DDL = [
    """
    CREATE TABLE IF NOT EXISTS explorations (
        id                   INTEGER PRIMARY KEY AUTOINCREMENT,
        article_id           INTEGER NOT NULL,
        event_id             INTEGER,
        market_condition_id  TEXT NOT NULL,
        token_yes            TEXT,
        token_no             TEXT,
        t0_iso               TEXT NOT NULL,
        dkg_final_score      REAL,
        cosine_similarity    REAL,
        freshness            TEXT,
        breaking_score       REAL,
        article_tier         TEXT,
        market_volume_24h    REAL,
        market_spread_t0     REAL,
        market_mid_t0        REAL,
        hours_to_expiry      REAL,
        llm_related          INTEGER NOT NULL CHECK(llm_related IN (0, 1)),
        llm_confidence       REAL,
        llm_reason           TEXT,
        llm_prompt_hash      TEXT,
        capture_status       TEXT NOT NULL DEFAULT 'pending'
            CHECK(capture_status IN ('pending','active','complete','aborted')),
        capture_end_at       TEXT,
        cycle_id             INTEGER NOT NULL,
        is_tracer            INTEGER NOT NULL DEFAULT 0,
        tracer_id            TEXT,
        created_at           TEXT NOT NULL,
        UNIQUE(article_id, market_condition_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_explorations_cycle "
    "ON explorations(cycle_id)",
    "CREATE INDEX IF NOT EXISTS idx_explorations_capture_status "
    "ON explorations(capture_status)",
    "CREATE INDEX IF NOT EXISTS idx_explorations_t0 "
    "ON explorations(t0_iso)",
    "CREATE INDEX IF NOT EXISTS idx_explorations_market "
    "ON explorations(market_condition_id)",
]
db_module.register_schema("app/stages/explorer.py", _EXPLORER_DDL)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def _hours_until(end_date: Optional[str]) -> Optional[float]:
    dt = _parse_iso(end_date)
    if dt is None:
        return None
    delta = dt - _now()
    return delta.total_seconds() / 3600.0


class ExplorerStage:
    """Permissive DKG + LLM-relatedness gate → exploration row + odds capture."""

    def __init__(self, deps: StageDeps) -> None:
        if deps.ai is None:
            raise RuntimeError("ExplorerStage requires deps.ai")
        self._db = deps.db
        self._bus = deps.bus
        self._ai = deps.ai
        self._clob = deps.clob
        self._cfg = deps.config.explorer

    async def start(self) -> None:
        return

    async def stop(self) -> None:
        return

    async def handle(self, payload: dict) -> None:
        event_id = payload.get("event_id")
        if event_id is None:
            logger.warning("explorer: payload missing event_id: %r", payload)
            return
        links = await self._fetch_high_score_links(int(event_id))
        if not links:
            return
        cycle_id = current_cycle_id()
        is_tracer = bool(payload.get("is_tracer", False))
        tracer_id = payload.get("tracer_id")
        for link in links:
            await self._handle_link(
                event_id=int(event_id),
                link=link,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )

    # ------------------------------------------------------------------
    # Per-link pipeline
    # ------------------------------------------------------------------

    async def _handle_link(
        self,
        *,
        event_id: int,
        link: dict,
        cycle_id: int,
        is_tracer: bool,
        tracer_id: Optional[str],
    ) -> None:
        market_condition_id = link["market_condition_id"]
        final_score = float(link["final_score"])

        # Lookup — silent skip on any missing row. The explorer keeps no
        # rejection log; only `related=True` rows are persisted.
        article = await self._fetch_article_for_event(event_id)
        market = await self._fetch_market(market_condition_id)
        if article is None or market is None:
            return

        # Pre-check idempotency before paying for an LLM call.
        if await self._exists(article["id"], market_condition_id):
            return

        # Best-effort signals lookup for the article ↔ market cosine +
        # freshness label. Absent on DKG-only paths (signal_id=NULL).
        signal = await self._fetch_signal(article["id"], market_condition_id)
        cosine_similarity = signal.get("similarity") if signal else None
        freshness = signal.get("freshness") if signal else None

        # LLM relatedness — cheap binary check. Budget pause / parse
        # error / network failure all return related=False, so the
        # row is dropped without odds capture.
        relatedness = await self._ai.relatedness(
            article={
                "id": article["id"],
                "title": article.get("title") or "",
                "body": article.get("body") or article.get("summary") or "",
            },
            market={
                "condition_id": market_condition_id,
                "question": market.get("question") or "",
                "description": market.get("description") or "",
                "end_date": market.get("end_date"),
            },
            max_article_chars=self._cfg.relatedness_max_article_chars,
            max_output_tokens=self._cfg.relatedness_max_output_tokens,
            model=self._cfg.relatedness_model,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
        )
        if not relatedness.related:
            return

        # t0 snapshot of the YES side mid/spread (best-effort; NULL on
        # any failure). We snapshot YES by convention so downstream
        # analysis has a stable reference token.
        mid_t0, spread_t0 = await self._snapshot_book(market.get("token_yes"))

        t0_iso = _now_iso()
        capture_end_at_iso = self._compute_capture_end_at(t0_iso)
        hours_to_expiry = _hours_until(market.get("end_date"))

        exploration_id = await self._insert_exploration(
            article_id=article["id"],
            event_id=event_id,
            market_condition_id=market_condition_id,
            token_yes=market.get("token_yes"),
            token_no=market.get("token_no"),
            t0_iso=t0_iso,
            dkg_final_score=final_score,
            cosine_similarity=cosine_similarity,
            freshness=freshness,
            breaking_score=article.get("breaking_score"),
            article_tier=article.get("tier"),
            market_volume_24h=market.get("volume_24h"),
            market_spread_t0=spread_t0,
            market_mid_t0=mid_t0,
            hours_to_expiry=hours_to_expiry,
            llm_related=True,
            llm_confidence=relatedness.confidence,
            llm_reason=relatedness.reason,
            llm_prompt_hash=relatedness.prompt_hash,
            capture_end_at=capture_end_at_iso,
            cycle_id=cycle_id,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
        )
        if exploration_id is None:
            # Lost the idempotency race — another payload for the same
            # (article, market) inserted first. Silent skip.
            return

        await self._bus.publish(
            TOPIC_EXPLORATION_CREATED,
            {
                "exploration_id": exploration_id,
                "article_id": article["id"],
                "market_condition_id": market_condition_id,
                "token_yes": market.get("token_yes"),
                "token_no": market.get("token_no"),
                "t0_iso": t0_iso,
                "capture_end_at": capture_end_at_iso,
                "is_tracer": is_tracer,
                "tracer_id": tracer_id,
            },
        )

    def _compute_capture_end_at(self, t0_iso: str) -> str:
        dt = _parse_iso(t0_iso) or _now()
        end = datetime.fromtimestamp(
            dt.timestamp() + self._cfg.capture_post_secs, tz=timezone.utc
        )
        return end.isoformat()

    async def _snapshot_book(
        self, token_id: Optional[str]
    ) -> tuple[Optional[float], Optional[float]]:
        """Return `(mid, spread)` at t0 — best effort. NULL on any failure."""
        if self._clob is None or not token_id:
            return None, None
        try:
            book = await self._clob.get_top_of_book(str(token_id))
        except Exception:  # noqa: BLE001 — never raise out of handle()
            logger.warning("explorer: get_top_of_book(%s) raised", token_id)
            return None, None
        if book is None:
            return None, None
        bid, ask = book
        mid = (bid + ask) / 2.0
        spread = ask - bid
        return mid, spread

    # ------------------------------------------------------------------
    # Lookups
    # ------------------------------------------------------------------

    async def _fetch_high_score_links(self, event_id: int) -> list[dict]:
        # lint: allow-extraction-table-ref — explorer mirrors the DKG path's
        # `market_links` read by design.
        cur = await self._db.conn.execute(
            "SELECT market_condition_id, final_score, directness_score "
            "FROM market_links "
            "WHERE event_id = ? AND final_score >= ? "
            "ORDER BY final_score DESC",
            (event_id, float(self._cfg.score_floor)),
        )
        rows = await cur.fetchall()
        await cur.close()
        return [
            {
                "market_condition_id": str(r["market_condition_id"]),
                "final_score": float(r["final_score"]),
                # `directness_score` IS the event-summary ↔ market-question
                # cosine on the DKG side; we log it alongside the
                # article ↔ market signal cosine below for downstream
                # slicing.
                "directness_score": (
                    float(r["directness_score"])
                    if r["directness_score"] is not None else None
                ),
            }
            for r in rows
        ]

    async def _fetch_article_for_event(
        self, event_id: int
    ) -> Optional[dict]:
        """Walk facts → articles to the most-recent article behind this
        event. Returns title + body (preferring full body over summary)
        + tier + breaking_score. All ingest-added columns (`body`,
        `tier`, `breaking_score`) are guaranteed present by ingest.py
        DDL/migrate.
        """
        # lint: allow-extraction-table-ref — explorer walks DKG path tables.
        cur = await self._db.conn.execute(
            """
            SELECT a.id, a.title, a.summary, a.body,
                   a.tier, a.breaking_score
            FROM articles a
            JOIN facts f ON f.article_id = a.id
            WHERE f.event_id = ?
            ORDER BY a.published_at DESC, f.id DESC
            LIMIT 1
            """,
            (event_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return {
            "id": int(row["id"]),
            "title": row["title"],
            "summary": row["summary"],
            "body": row["body"],
            "tier": row["tier"],
            "breaking_score": (
                float(row["breaking_score"])
                if row["breaking_score"] is not None else None
            ),
        }

    async def _fetch_signal(
        self, article_id: int, market_condition_id: str
    ) -> Optional[dict]:
        """Return the signals row for this (article, market) pair if
        present. Returns the most-recent by id when there are multiple
        (UNIQUE constraint normally prevents this, but defensive). NULL
        on DKG-only paths where no signal was created."""
        cur = await self._db.conn.execute(
            "SELECT similarity, freshness FROM signals "
            "WHERE article_id=? AND market_id=? "
            "ORDER BY id DESC LIMIT 1",
            (article_id, market_condition_id),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return {
            "similarity": (
                float(row["similarity"]) if row["similarity"] is not None
                else None
            ),
            "freshness": row["freshness"],
        }

    async def _fetch_market(
        self, market_condition_id: str
    ) -> Optional[dict]:
        cur = await self._db.conn.execute(
            "SELECT question, description, token_yes, token_no, "
            "       active, closed_at, end_date, volume_usdc "
            "FROM markets WHERE condition_id=?",
            (market_condition_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return {
            "question": row["question"],
            "description": row["description"],
            "token_yes": row["token_yes"],
            "token_no": row["token_no"],
            "active": row["active"],
            "closed_at": row["closed_at"],
            "end_date": row["end_date"],
            "volume_24h": (
                float(row["volume_usdc"]) if row["volume_usdc"] is not None
                else None
            ),
        }

    async def _exists(
        self, article_id: int, market_condition_id: str
    ) -> bool:
        cur = await self._db.conn.execute(
            "SELECT 1 FROM explorations "
            "WHERE article_id=? AND market_condition_id=? LIMIT 1",
            (article_id, market_condition_id),
        )
        row = await cur.fetchone()
        await cur.close()
        return row is not None

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    async def _insert_exploration(
        self,
        *,
        article_id: int,
        event_id: Optional[int],
        market_condition_id: str,
        token_yes: Optional[str],
        token_no: Optional[str],
        t0_iso: str,
        dkg_final_score: Optional[float],
        cosine_similarity: Optional[float],
        freshness: Optional[str],
        breaking_score: Optional[float],
        article_tier: Optional[str],
        market_volume_24h: Optional[float],
        market_spread_t0: Optional[float],
        market_mid_t0: Optional[float],
        hours_to_expiry: Optional[float],
        llm_related: bool,
        llm_confidence: Optional[float],
        llm_reason: Optional[str],
        llm_prompt_hash: Optional[str],
        capture_end_at: str,
        cycle_id: int,
        is_tracer: bool,
        tracer_id: Optional[str],
    ) -> Optional[int]:
        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT OR IGNORE INTO explorations
                    (article_id, event_id, market_condition_id,
                     token_yes, token_no, t0_iso,
                     dkg_final_score, cosine_similarity, freshness,
                     breaking_score, article_tier,
                     market_volume_24h, market_spread_t0, market_mid_t0,
                     hours_to_expiry,
                     llm_related, llm_confidence, llm_reason,
                     llm_prompt_hash,
                     capture_status, capture_end_at,
                     cycle_id, is_tracer, tracer_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?)
                """,
                (
                    article_id, event_id, market_condition_id,
                    token_yes, token_no, t0_iso,
                    dkg_final_score, cosine_similarity, freshness,
                    breaking_score, article_tier,
                    market_volume_24h, market_spread_t0, market_mid_t0,
                    hours_to_expiry,
                    1 if llm_related else 0, llm_confidence, llm_reason,
                    llm_prompt_hash,
                    capture_end_at,
                    cycle_id, 1 if is_tracer else 0, tracer_id, _now_iso(),
                ),
            )
            if cur.rowcount == 0:
                return None
            return cur.lastrowid

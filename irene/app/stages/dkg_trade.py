"""
## Specification

**Purpose.** P3 chunk 3 dual-path trader — the DKG-side of irene.
Subscribes to `TOPIC_MARKET_LINKS_UPDATED`; for each link with
`final_score >= cfg.dkg_trade.score_floor` re-runs the same LLM
direction call the MVP path uses (against the article behind the
event) and writes a `trades` row stamped `decision_path='dkg'`.

This stage is the OTHER half of `app/stages/trade.py` — the MVP
path fires off `signal.created`; this one fires off
`market_links.updated`. Both write into the same `decisions` /
`trades` / `trade_features` tables and both publish
`TOPIC_TRADE_DECIDED` so the impact tracker doesn't care which
path produced the bet.

**Public interface.**
- `DKGTradeStage(deps: StageDeps)` — requires `deps.ai`. Reads
  `cfg.dkg_trade` for its own knobs and `cfg.trade` for the gate
  thresholds (sizing, edge, fee_buffer) — `inherit_trade_gates`
  keeps the two paths in lockstep.
- `await start()` / `await stop()` — no-ops; the bus drives it.
- `await handle(payload)` — bus subscriber.

**Owned tables.** `dkg_trade_links`. The `trades` /
`trade_features` / `decisions` writes go through the shared schema
owned by `app/stages/trade.py`.

**Trade-path exemption.** This file legitimately reads
`events`, `facts`, and `market_links` — the DKG path needs to walk
from an event-id back to the article that generated it before it
can compose the prompt. The
`scripts/lint/no_facts_in_trade_path.py` lint is therefore
intentionally NOT applied to this module; the docstring there
explains the carve-out. The lint's purpose is to keep the MVP
trade path pure (`match.py` / `trade.py` / `closer.py` / `ai.py`);
DKG is the bridge between the DKG layer and that path.

**Idempotency.** `dkg_trade_links(event_id, market_condition_id,
decision_path)` is the idempotency key. `INSERT OR IGNORE` on
entry — if `rowcount == 0` the link was already processed and we
skip. On a successful trade write we `UPDATE` the row with the
`trade_id`. This shape lets us:

  1. handle a re-publish of the same `market_links.updated` payload
     without doubling up trades;
  2. record the per-(event, market) outcome even when the gate
     cascade rejects (the row stays with `trade_id=NULL`).

**signal_id design choice.** MVP rows carry `signals.id` in
`trades.signal_id`. DKG rows leave `trades.signal_id = NULL` —
the column was broadened from `INTEGER NOT NULL UNIQUE` to
`INTEGER` with a partial unique index `WHERE signal_id IS NOT
NULL`. Synthesising a signal row per DKG hit was the alternative;
we chose null because (a) a synthetic signal would conflict with
real ones for the same `(article_id, market_id)` pair, breaking
MVP idempotency, and (b) "this trade has no signal" is the honest
data model. See `app/stages/trade.py` DDL note.

**Gate cascade (same order as MVP, abridged because there's no
similarity / freshness on a link payload).**

  1. `score_floor` — `market_links.final_score >= cfg.dkg_trade.score_floor`.
  2. `lookup` — fetch the event, the most-recent article behind it,
                 and the market; reject if any are missing.
  3. `direction` — `ai.direction()` against the composed prompt;
                   NEUTRAL or below `cfg.trade.min_confidence`
                   rejects.
  4. `edge` — `p_hat` vs mid +/- `cfg.trade.fee_buffer`.
  5. `size` — fractional-Kelly capped by `cfg.trade.max_position_fraction`.

**Side effects.** Reads `events`, `facts`, `articles`, `markets`,
`market_links` (the bridge). Writes `decisions`, `trades`,
`trade_features`, `dkg_trade_links`. Publishes
`TOPIC_TRADE_DECIDED`.

**Failure modes.**
- Event / article / market missing → reject with a typed reason,
  the `dkg_trade_links` row stays with `trade_id=NULL`.
- `ai.direction()` budget-paused / parse-error → returns NEUTRAL
  and the direction gate rejects normally.

**Non-goals.**
- Reading `entities` / `entity_aliases` / `entity_resolutions` —
  the DKG path uses event_id + market_condition_id only.
- Refreshing market_links — that's link.py's job.
- Cross-path coordination — the MVP and DKG paths can both fire
  for the same article. The impact tracker sees both rows and
  can compare.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from app import db as db_module
from app.cycle import current_cycle_id
from app.deps import StageDeps
from app.stages._gates import (
    _gate_binary_dedup,
    _gate_entry,
    _gate_market_state,
    _gate_market_vanished,
    _gate_mid_unavailable,
)
from app.stages.trade import compose_direction_prompt
from app.topics import TOPIC_MARKET_LINKS_UPDATED, TOPIC_TRADE_DECIDED

logger = logging.getLogger(__name__)


_DKG_TRADE_DDL = [
    """
    CREATE TABLE IF NOT EXISTS dkg_trade_links (
        id                   INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id             INTEGER NOT NULL,
        market_condition_id  TEXT NOT NULL,
        decision_path        TEXT NOT NULL DEFAULT 'dkg',
        final_score          REAL NOT NULL,
        trade_id             INTEGER,
        decision_id          INTEGER,
        reject_reason        TEXT,
        cycle_id             INTEGER NOT NULL,
        is_tracer            INTEGER NOT NULL DEFAULT 0,
        tracer_id            TEXT,
        created_at           TEXT NOT NULL,
        UNIQUE(event_id, market_condition_id, decision_path)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_dkg_trade_links_event "
    "ON dkg_trade_links(event_id)",
    "CREATE INDEX IF NOT EXISTS idx_dkg_trade_links_market "
    "ON dkg_trade_links(market_condition_id)",
    "CREATE INDEX IF NOT EXISTS idx_dkg_trade_links_trade "
    "ON dkg_trade_links(trade_id)",
    "CREATE INDEX IF NOT EXISTS idx_dkg_trade_links_cycle "
    "ON dkg_trade_links(cycle_id)",
]
db_module.register_schema("app/stages/dkg_trade.py", _DKG_TRADE_DDL)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class DKGTradeStage:
    DECISION_PATH: str = "dkg"

    def __init__(self, deps: StageDeps) -> None:
        if deps.ai is None:
            raise RuntimeError("DKGTradeStage requires deps.ai")
        self._db = deps.db
        self._bus = deps.bus
        self._ai = deps.ai
        # Read both knobs at construction time — the config is frozen.
        self._cfg = deps.config.dkg_trade
        self._trade_cfg = deps.config.trade
        self._clob = deps.clob

    async def start(self) -> None:
        return

    async def stop(self) -> None:
        return

    async def handle(self, payload: dict) -> None:
        if not self._cfg.enabled:
            return
        event_id = payload.get("event_id")
        if event_id is None:
            logger.warning("dkg_trade: payload missing event_id: %r", payload)
            return
        # Fan-out: payload describes ONE event but may have many high-score
        # links; fetch them inline rather than trusting the payload's
        # `top_final_score` summary.
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

        # Idempotency: claim the (event, market, 'dkg') slot. If the
        # claim is rejected we've already processed this link.
        if not await self._claim_link(
            event_id=event_id,
            market_condition_id=market_condition_id,
            final_score=final_score,
            cycle_id=cycle_id,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
        ):
            return

        gate_history: list[dict] = [
            _gate_entry("score_floor", "ok", final_score, self._cfg.score_floor)
        ]

        # 0. execution kill switch (cycle 24, mirrors cycle-23 trade.py) ----
        # Lands BEFORE any LLM/CLOB-cost work so a halted bot doesn't burn
        # tokens or hit Polymarket. The claim_link row above gives us
        # idempotency; we then write a decisions row + reject_reason on the
        # link so the soak funnel sees the rejection — operationally
        # distinguishable from a silent stall.
        if not self._trade_cfg.execution_enabled:
            gate_history.append(
                _gate_entry("execution_enabled", "reject", False, True)
            )
            await self._record_reject(
                event_id=event_id,
                market_condition_id=market_condition_id,
                reason="execution_disabled",
                gate_history=gate_history,
                prediction_id=None,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return

        # 2. lookup ----------------------------------------------------
        event = await self._fetch_event(event_id)
        article = await self._fetch_article_for_event(event_id) if event else None
        market = await self._fetch_market(market_condition_id)
        if event is None:
            await self._record_reject(
                event_id=event_id,
                market_condition_id=market_condition_id,
                reason="event_missing",
                gate_history=gate_history + [
                    _gate_entry("lookup", "reject", "event_missing", None)
                ],
                prediction_id=None,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return
        if article is None:
            await self._record_reject(
                event_id=event_id,
                market_condition_id=market_condition_id,
                reason="article_missing",
                gate_history=gate_history + [
                    _gate_entry("lookup", "reject", "article_missing", None)
                ],
                prediction_id=None,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return
        if market is None:
            await self._record_reject(
                event_id=event_id,
                market_condition_id=market_condition_id,
                reason="market_missing",
                gate_history=gate_history + [
                    _gate_entry("lookup", "reject", "market_missing", None)
                ],
                prediction_id=None,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return

        # 2.25 market_state (cycle 24, mirrors cycle-23 trade.py) -----------
        # Refuses entries on closed / inactive / near-expiry markets. Lands
        # before the LLM direction call so a bad market never burns tokens.
        # Permissive when the relevant columns are NULL (don't reject on
        # missing data). Trade 40 (cycle 23 audit) would have been rejected
        # here under `market_closed` + `market_near_expiry`.
        status, entry, reason = _gate_market_state(market, self._trade_cfg)
        if status == "reject":
            gate_history.append(entry)
            await self._record_reject(
                event_id=event_id,
                market_condition_id=market_condition_id,
                reason=reason,
                gate_history=gate_history,
                prediction_id=None,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return

        prompt = compose_direction_prompt(article, market)

        # 3. direction -------------------------------------------------
        # DKG path passes signal_id=None; the predictions table allows
        # it under the partial unique index. The LLM cache (prompt_sha)
        # still suppresses the HTTP round-trip if MVP already called
        # with this prompt.
        result = await self._ai.direction(
            signal_id=None,
            prompt=prompt,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
        )
        if result.direction == "NEUTRAL":
            gate_history.append(
                _gate_entry("direction", "reject", "NEUTRAL", "YES|NO")
            )
            await self._record_reject(
                event_id=event_id,
                market_condition_id=market_condition_id,
                reason="direction_neutral",
                gate_history=gate_history,
                prediction_id=result.prediction_id,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return
        if result.confidence < self._trade_cfg.min_confidence:
            gate_history.append(
                _gate_entry(
                    "direction", "reject",
                    result.confidence, self._trade_cfg.min_confidence,
                )
            )
            await self._record_reject(
                event_id=event_id,
                market_condition_id=market_condition_id,
                reason="direction_lowconf",
                gate_history=gate_history,
                prediction_id=result.prediction_id,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return
        gate_history.append(
            _gate_entry("direction", "ok", result.direction, result.confidence)
        )

        # 3.5 market_vanished re-check (cycle 25, mirrors trade.py) -----
        # Re-fetch the markets row after the LLM call. If the row was
        # deleted or transitioned to inactive/closed between the initial
        # lookup and the decision, refuse the trade and log the decision
        # with `reason='market_vanished'` so the audit layer can quantify
        # how often discovery is unreliable.
        fresh_market = await self._fetch_market(market_condition_id)
        status, entry, reason = _gate_market_vanished(market, fresh_market)
        if status == "reject":
            gate_history.append(entry)
            await self._record_reject(
                event_id=event_id,
                market_condition_id=market_condition_id,
                reason=reason,
                gate_history=gate_history,
                prediction_id=result.prediction_id,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return

        # 3.6 binary_dedup (cycle 25-B) --------------------------------
        # Trades 38+39 (NO on opposite legs of the Bank-of-Israel binary)
        # were the smoking-gun proof that DKG enters per market without
        # awareness of sibling outcomes. Refuse when another open trade
        # already sits on a market sharing the same canonical event.
        from app.stages._gates import query_open_sibling_markets
        siblings = await query_open_sibling_markets(
            self._db.conn, market_condition_id
        )
        status, entry, reason = _gate_binary_dedup(siblings)
        if status == "reject":
            gate_history.append(entry)
            await self._record_reject(
                event_id=event_id,
                market_condition_id=market_condition_id,
                reason=reason,
                gate_history=gate_history,
                prediction_id=result.prediction_id,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return

        # 4. edge ------------------------------------------------------
        direction = result.direction
        confidence = result.confidence
        p_hat = confidence if direction == "YES" else (1.0 - confidence)
        mid = await self._resolve_mid(direction=direction, market=market)
        # 4-pre. mid_unavailable (cycle 24, mirrors cycle-23 trade.py) ------
        # `_resolve_mid` now returns None on any non-real-mid path (no clob,
        # missing token, clob returned None). Pre-cycle-24 the DKG path
        # silently fell back to `cfg.trade.default_mid_price` (0.5), the
        # same blind-bot behaviour cycle 23 fixed on the MVP path.
        status, entry, reason = _gate_mid_unavailable(mid)
        if status == "reject":
            gate_history.append(entry)
            await self._record_reject(
                event_id=event_id,
                market_condition_id=market_condition_id,
                reason=reason,
                gate_history=gate_history,
                prediction_id=result.prediction_id,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return
        # 4a. degenerate-mid guard — mirrors trade.py:405 (cycle 4 fix).
        # cycle 5.1: applied to the DKG path too. The dashboard caught a
        # dkg trade opened at mid=0.998 on a "Will X drop out?" market
        # that the MVP path's same guard would have rejected.
        if mid <= self._trade_cfg.min_mid or mid >= self._trade_cfg.max_mid:
            gate_history.append(_gate_entry("edge", "reject", mid, "mid_degenerate"))
            await self._record_reject(
                event_id=event_id,
                market_condition_id=market_condition_id,
                reason="mid_degenerate",
                gate_history=gate_history,
                prediction_id=result.prediction_id,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return
        # Cycle 27: `mid` is position space (the traded token's own mid —
        # `_resolve_mid` reads the traded side), so the edge is P(our
        # token pays out) minus its price for BOTH directions. The old NO
        # branch (`mid - p_hat`) assumed a YES-space mid that was never
        # supplied. Mirrors trade.py:_compute_edge's fallback path.
        p_win = p_hat if direction == "YES" else (1.0 - p_hat)
        edge = p_win - mid - self._trade_cfg.fee_buffer
        if edge < self._trade_cfg.min_edge:
            gate_history.append(
                _gate_entry("edge", "reject", edge, self._trade_cfg.min_edge)
            )
            await self._record_reject(
                event_id=event_id,
                market_condition_id=market_condition_id,
                reason="edge_below_min",
                gate_history=gate_history,
                prediction_id=result.prediction_id,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return
        gate_history.append(_gate_entry("edge", "ok", edge, self._trade_cfg.min_edge))

        # 5. size ------------------------------------------------------
        # Cycle 27: position-space Kelly — (P(our side wins), our token's
        # price) for both directions. Mirrors trade.py:_compute_kelly_and_size.
        b = (1.0 - mid) / mid
        kelly_full = (p_win * b - (1.0 - p_win)) / b
        kelly_capped = max(0.0, kelly_full) * self._trade_cfg.kelly_fraction
        size = min(
            kelly_capped * self._trade_cfg.balance_usdc,
            self._trade_cfg.balance_usdc * self._trade_cfg.max_position_fraction,
        )
        if size < self._trade_cfg.min_position_usdc:
            gate_history.append(
                _gate_entry("size", "reject", size, self._trade_cfg.min_position_usdc)
            )
            await self._record_reject(
                event_id=event_id,
                market_condition_id=market_condition_id,
                reason="size_below_min",
                gate_history=gate_history,
                prediction_id=result.prediction_id,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return
        gate_history.append(
            _gate_entry("size", "ok", size, self._trade_cfg.min_position_usdc)
        )

        # Execute -----------------------------------------------------
        features_snapshot = {
            "decision_path": "dkg",
            "event_id": event_id,
            "final_score": final_score,
            "gate_history": gate_history,
            "prompt": prompt,
            "llm_response_excerpt": (result.raw_response or "")[:512],
            "direction": direction,
            "confidence": confidence,
            "p_hat": p_hat,
            "mid": mid,
            "edge": edge,
            "kelly_full": kelly_full,
            "kelly_capped": kelly_capped,
            "size_usdc": size,
            "balance_usdc": self._trade_cfg.balance_usdc,
        }
        await self._record_execute(
            event_id=event_id,
            market_condition_id=market_condition_id,
            final_score=final_score,
            prediction_id=result.prediction_id,
            direction=direction,
            confidence=confidence,
            p_hat=p_hat,
            mid=mid,
            edge=edge,
            kelly_fraction=kelly_capped,
            size_usdc=size,
            features_snapshot=features_snapshot,
            gate_history=gate_history,
            cycle_id=cycle_id,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
        )

    # ------------------------------------------------------------------
    # Lookups
    # ------------------------------------------------------------------

    async def _fetch_high_score_links(self, event_id: int) -> list[dict]:
        # lint: allow-extraction-table-ref — `market_links` is the
        # documented DKG ↔ trade-path bridge (see link.py docstring).
        cur = await self._db.conn.execute(
            "SELECT market_condition_id, final_score, link_class "
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
                "link_class": r["link_class"],
            }
            for r in rows
        ]

    async def _fetch_event(self, event_id: int) -> Optional[dict]:
        # lint: allow-extraction-table-ref — DKG path reads events by design.
        cur = await self._db.conn.execute(
            "SELECT id, event_type, canonical_summary, support_count "
            "FROM events WHERE id = ?",
            (event_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return {
            "id": int(row["id"]),
            "event_type": row["event_type"],
            "canonical_summary": row["canonical_summary"] or "",
            "support_count": int(row["support_count"]),
        }

    async def _fetch_article_for_event(self, event_id: int) -> Optional[dict]:
        """Return the most-recent article that contributed a fact to
        this event. Articles are joined through `facts.event_id`; ties
        broken by `articles.published_at DESC` then `facts.id DESC`."""
        # lint: allow-extraction-table-ref — DKG path walks facts → article.
        cur = await self._db.conn.execute(
            """
            SELECT a.id, a.title, a.summary
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
        }

    async def _fetch_market(self, market_condition_id: str) -> Optional[dict]:
        # Cycle 24: widen the SELECT to include `active`, `closed_at`, and
        # `end_date` so `_gate_market_state` can read them. Mirrors the
        # cycle-23 trade.py change.
        cur = await self._db.conn.execute(
            "SELECT question, description, token_yes, token_no, "
            "       active, closed_at, end_date "
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
            # Cycle 24: read by `_gate_market_state` so the DKG path can
            # refuse entries on closed / inactive / near-expiry markets.
            "active": row["active"],
            "closed_at": row["closed_at"],
            "end_date": row["end_date"],
        }

    async def _resolve_mid(
        self, *, direction: str, market: Optional[dict]
    ) -> Optional[float]:
        """Return the mid-price for the side we're betting on, or `None`
        when no real CLOB price is available.

        Cycle 24: removed the silent fallback to `cfg.default_mid_price`.
        Pre-cycle-24 this returned 0.5 on (a) `self._clob is None`,
        (b) missing token, (c) `clob.get_mid()` returning None — the bot
        then placed real BUYs at the stub price (the cycle-22 audit caught
        21/39 MVP trades doing this, which cycle 23 patched on the MVP
        path; this is the DKG-path mirror). `_gate_mid_unavailable` now
        rejects on `None`. The clob WARN-once defense is in
        `_warn_once_clob_missing` below.
        """
        if self._clob is None:
            self._warn_once_clob_missing("_resolve_mid")
            return None
        if market is None:
            return None
        token_id: Optional[str] = (
            market.get("token_yes") if direction == "YES" else market.get("token_no")
        )
        if not token_id:
            return None
        mid = await self._clob.get_mid(str(token_id))
        if mid is None:
            return None
        return float(mid)

    # Cycle 24: per-process one-shot WARN log mirroring trade.py so the
    # DKG-side silent-fallback regression is impossible to overlook. Keyed
    # by call-site so each surface logs exactly once per process lifetime.
    _CLOB_WARN_SEEN: set[str] = set()

    def _warn_once_clob_missing(self, site: str) -> None:
        if site in self._CLOB_WARN_SEEN:
            return
        self._CLOB_WARN_SEEN.add(site)
        logger.warning(
            "dkg_trade: %s observed self._clob is None — pre-trade gate "
            "will reject with mid_unavailable. This is a SILENT-FALLBACK "
            "regression: investigate build_deps() / build_clob() wiring.",
            site,
        )

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    async def _claim_link(
        self,
        *,
        event_id: int,
        market_condition_id: str,
        final_score: float,
        cycle_id: int,
        is_tracer: bool,
        tracer_id: Optional[str],
    ) -> bool:
        """Insert the per-(event, market, path) idempotency row. Returns
        True when we won the race, False when an earlier handle()
        already wrote it."""
        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT OR IGNORE INTO dkg_trade_links
                    (event_id, market_condition_id, decision_path,
                     final_score, cycle_id, is_tracer, tracer_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    market_condition_id,
                    self.DECISION_PATH,
                    final_score,
                    cycle_id,
                    1 if is_tracer else 0,
                    tracer_id,
                    _now_iso(),
                ),
            )
            return cur.rowcount == 1

    async def _record_reject(
        self,
        *,
        event_id: int,
        market_condition_id: str,
        reason: str,
        gate_history: list[dict],
        prediction_id: Optional[int],
        cycle_id: int,
        is_tracer: bool,
        tracer_id: Optional[str],
    ) -> None:
        decision_id = await self._insert_decision(
            outcome="reject",
            reason=reason,
            gate_history=gate_history,
            prediction_id=prediction_id,
            cycle_id=cycle_id,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
        )
        await self._update_link(
            event_id=event_id,
            market_condition_id=market_condition_id,
            decision_id=decision_id,
            trade_id=None,
            reject_reason=reason,
        )
        await self._bus.publish(
            TOPIC_TRADE_DECIDED,
            {
                "trade_id": None,
                "decision_id": decision_id,
                "outcome": "reject",
                "reason": reason,
                "decision_path": self.DECISION_PATH,
                "is_tracer": is_tracer,
                "tracer_id": tracer_id,
            },
        )

    async def _record_execute(
        self,
        *,
        event_id: int,
        market_condition_id: str,
        final_score: float,
        prediction_id: int,
        direction: str,
        confidence: float,
        p_hat: float,
        mid: float,
        edge: float,
        kelly_fraction: float,
        size_usdc: float,
        features_snapshot: dict,
        gate_history: list[dict],
        cycle_id: int,
        is_tracer: bool,
        tracer_id: Optional[str],
    ) -> None:
        decision_id = await self._insert_decision(
            outcome="execute",
            reason="ok",
            gate_history=gate_history,
            prediction_id=prediction_id,
            cycle_id=cycle_id,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
        )
        trade_id = await self._insert_trade(
            decision_id=decision_id,
            prediction_id=prediction_id,
            market_id=market_condition_id,
            direction=direction,
            confidence=confidence,
            # similarity is an MVP-path concept; for DKG we surface
            # `final_score` as the analogous scalar so downstream
            # readers can compare across paths.
            similarity=final_score,
            p_hat=p_hat,
            mid=mid,
            edge=edge,
            kelly_fraction=kelly_fraction,
            size_usdc=size_usdc,
            features_snapshot=features_snapshot,
            cycle_id=cycle_id,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
        )
        await self._insert_trade_features(
            trade_id=trade_id,
            features={
                "final_score": ("numeric", final_score, None),
                "confidence": ("numeric", confidence, None),
                "p_hat": ("numeric", p_hat, None),
                "mid": ("numeric", mid, None),
                "edge": ("numeric", edge, None),
                "kelly_fraction": ("numeric", kelly_fraction, None),
                "size_usdc": ("numeric", size_usdc, None),
                "decision_path": ("category", None, "dkg"),
            },
        )
        await self._update_link(
            event_id=event_id,
            market_condition_id=market_condition_id,
            decision_id=decision_id,
            trade_id=trade_id,
            reject_reason=None,
        )
        await self._bus.publish(
            TOPIC_TRADE_DECIDED,
            {
                "trade_id": trade_id,
                "decision_id": decision_id,
                "outcome": "execute",
                "reason": "ok",
                "decision_path": self.DECISION_PATH,
                "is_tracer": is_tracer,
                "tracer_id": tracer_id,
            },
        )

    async def _insert_decision(
        self,
        *,
        outcome: str,
        reason: str,
        gate_history: list[dict],
        prediction_id: Optional[int],
        cycle_id: int,
        is_tracer: bool,
        tracer_id: Optional[str],
    ) -> int:
        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT INTO decisions
                    (signal_id, outcome, reason, gate_history, prediction_id,
                     cycle_id, is_tracer, tracer_id, created_at)
                VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    outcome,
                    reason,
                    json.dumps(gate_history),
                    prediction_id,
                    cycle_id,
                    1 if is_tracer else 0,
                    tracer_id,
                    _now_iso(),
                ),
            )
            rid = cur.lastrowid
            if rid is None:
                raise RuntimeError("dkg_trade: decisions insert returned no rowid")
            return int(rid)

    async def _insert_trade(
        self,
        *,
        decision_id: int,
        prediction_id: int,
        market_id: str,
        direction: str,
        confidence: float,
        similarity: float,
        p_hat: float,
        mid: float,
        edge: float,
        kelly_fraction: float,
        size_usdc: float,
        features_snapshot: dict,
        cycle_id: int,
        is_tracer: bool,
        tracer_id: Optional[str],
    ) -> int:
        # Cycle 24: post-condition (mirrors trade.py:_insert_trade). After
        # the mid_unavailable + mid_degenerate gates a degenerate mid
        # should be unreachable here; assert so any regression is loud,
        # never a silent bad row.
        assert mid is not None and 0.0 < float(mid) < 1.0, (
            f"_insert_trade refusing degenerate mid: market={market_id} "
            f"direction={direction} mid={mid!r}"
        )
        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT INTO trades
                    (signal_id, decision_id, prediction_id, market_id, direction,
                     confidence, similarity, freshness, p_hat, mid_price, edge,
                     kelly_fraction, size_usdc, decision_path, features_snapshot,
                     cycle_id, is_tracer, tracer_id, decided_at)
                VALUES (NULL, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, 'dkg', ?, ?, ?, ?, ?)
                """,
                (
                    decision_id,
                    prediction_id,
                    market_id,
                    direction,
                    confidence,
                    similarity,
                    p_hat,
                    mid,
                    edge,
                    kelly_fraction,
                    size_usdc,
                    json.dumps(features_snapshot),
                    cycle_id,
                    1 if is_tracer else 0,
                    tracer_id,
                    _now_iso(),
                ),
            )
            rid = cur.lastrowid
            if rid is None:
                raise RuntimeError("dkg_trade: trades insert returned no rowid")
            return int(rid)

    async def _insert_trade_features(
        self,
        *,
        trade_id: int,
        features: dict[str, tuple[str, Optional[float], Optional[str]]],
    ) -> None:
        async with self._db.batch() as cur:
            for name, (kind, value, label) in features.items():
                await cur.execute(
                    """
                    INSERT OR IGNORE INTO trade_features
                        (trade_id, feature_name, feature_value, feature_kind, feature_label)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (trade_id, name, value, kind, label),
                )

    async def _update_link(
        self,
        *,
        event_id: int,
        market_condition_id: str,
        decision_id: int,
        trade_id: Optional[int],
        reject_reason: Optional[str],
    ) -> None:
        async with self._db.batch() as cur:
            await cur.execute(
                """
                UPDATE dkg_trade_links
                   SET trade_id = ?, decision_id = ?, reject_reason = ?
                 WHERE event_id = ?
                   AND market_condition_id = ?
                   AND decision_path = ?
                """,
                (
                    trade_id,
                    decision_id,
                    reject_reason,
                    event_id,
                    market_condition_id,
                    self.DECISION_PATH,
                ),
            )


# Silence import-unused (topic constant pulled in for grep-discoverability).
_ = TOPIC_MARKET_LINKS_UPDATED

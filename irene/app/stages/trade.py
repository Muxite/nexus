"""
## Specification

**Purpose.** Subscriber on `signal.created`. For each new signal, run a fixed
gate cascade and either write an execute decision + a paper-trade row + a
fan-out of trade_features rows, or write a reject decision pinned to the
failing gate. Publishes `trade.decided` per signal so monitor/diagnostic
consumers can follow the funnel.

**Public interface.**
- `TradeStage`:
  - `TradeStage(deps: StageDeps)` — requires `deps.ai`.
  - `await start()` — no-op (subscriber stage; the bus drives it).
  - `await stop()` — idempotent.
  - `await handle(payload: dict)` — bus subscriber.

**Owned tables** (`docs/modularization.md` §4): `decisions`, `trades`,
`trade_features`. DDL is registered with `db.register_schema()` at import
time.

**Gate cascade (order is contractual — master-plan §"Phase 1 — Detailed scope").**
For every signal, in order, until one rejects or all pass:

  1. `similarity_floor` — `payload.similarity` vs `cfg.trade.min_similarity`.
  2. `freshness`       — `payload.freshness` ∈ `cfg.trade.accept_freshness`.
  3. `direction`       — call `ai.direction()`, reject if NEUTRAL
                        (`direction_neutral`) or confidence below
                        `cfg.trade.min_confidence` (`direction_lowconf`).
  4. `edge`            — `p_hat` (= confidence on YES, 1-confidence on NO) vs
                        mid-price ± `cfg.trade.fee_buffer`. Mid defaults to
                        `cfg.trade.default_mid_price` (P1 has no live CLOB).
  5. `size`            — fractional-Kelly sizing capped by
                        `cfg.trade.max_position_fraction * balance`; reject
                        if the size falls below `cfg.trade.min_position_usdc`.

The first gate to reject writes a `decisions(outcome='reject', reason=<gate>)`
row and stops. If every gate passes, a single `decisions(outcome='execute',
reason='ok')` row, a `trades` row, and one `trade_features` row per surfaced
feature are written in the same `db.batch()`.

**Invariants.**
- Every signal yields exactly one `decisions` row — reject or execute.
  `UNIQUE(signal_id)` makes a re-handle of the same payload idempotent: the
  second pass sees the existing decision and short-circuits without writing.
- `trades.signal_id` is `UNIQUE` so an execute path is also idempotent.
- `cycle_id` is read once per `handle()` from `cycle.current_cycle_id()` and
  stamped on every owned-table row.
- `is_tracer` / `tracer_id` are read off the inbound payload and threaded
  onto `decisions`, `trades`, and every `trade_features` row.
- `features_snapshot` on `trades` is the full diagnostic JSON — every gate's
  value + threshold + outcome, the prompt, the LLM response excerpt, p_hat,
  mid, edge, kelly_full, kelly_capped, size, balance. This is what
  back-testing/replay reads.

**Side effects.** Reads `articles` (title + summary) and `markets` (question +
description) to compose the LLM prompt. Calls `deps.ai.direction()`. Writes
`decisions` / `trades` / `trade_features` inside `db.batch()`. Publishes
`TOPIC_TRADE_DECIDED` per signal.

**Failure modes.**
- `articles` row missing (race with ingest): logged WARNING; the signal is
  recorded as a reject with reason `'article_missing'` so the funnel stays
  consistent.
- `markets` row missing (race with markets refresh): same shape; reason
  `'market_missing'`.
- `ai.direction()` returning NEUTRAL or low-confidence: handled by the
  direction gate; not a failure mode here.

**Non-goals.**
- Trade closing / mark-to-market / realized PnL — `closed_at` and
  `realized_pnl` columns exist on the `trades` row but stay NULL in P1; a
  follow-up closer stage fills them.
- Live CLOB pricing — mid is `cfg.trade.default_mid_price` in P1.
- Risk management beyond the listed gates (no exposure caps across signals,
  no portfolio constraints, no slippage modeling).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from app import db as db_module
from app.cycle import current_cycle_id
from app.deps import StageDeps
from app.stages._entity_overlap import article_entities_overlap_market
from app.topics import TOPIC_TRADE_DECIDED

logger = logging.getLogger(__name__)


_TRADE_DDL = [
    # NOTE (P3 chunk 3): `signal_id` is nullable on both `decisions` and
    # `trades` so DKG-path rows (which have no signal backing them) can
    # land. The MVP path's idempotency is enforced by the partial unique
    # indexes below — `UNIQUE(signal_id) WHERE signal_id IS NOT NULL`.
    # DKG-path idempotency lives in `dkg_trade_links` (owned by
    # `app/stages/dkg_trade.py`).
    """
    CREATE TABLE IF NOT EXISTS decisions (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id       INTEGER,
        outcome         TEXT NOT NULL CHECK(outcome IN ('execute','reject')),
        reason          TEXT NOT NULL,
        gate_history    TEXT NOT NULL,
        prediction_id   INTEGER,
        cycle_id        INTEGER NOT NULL,
        is_tracer       INTEGER NOT NULL DEFAULT 0,
        tracer_id       TEXT,
        created_at      TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_decisions_outcome ON decisions(outcome)",
    "CREATE INDEX IF NOT EXISTS idx_decisions_reason ON decisions(reason)",
    "CREATE INDEX IF NOT EXISTS idx_decisions_cycle_id ON decisions(cycle_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_decisions_signal_id "
    "ON decisions(signal_id) WHERE signal_id IS NOT NULL",
    # NOTE (P3 chunk 3): `signal_id` was MVP-only — DKG trades have no
    # signal row backing them. We broadened the column to nullable +
    # dropped the table-level UNIQUE; the MVP path's per-signal
    # idempotency is now enforced by a partial unique index
    # (`signal_id IS NOT NULL`). The DKG path's idempotency lives in
    # `dkg_trade_links` (owned by `app/stages/dkg_trade.py`).
    """
    CREATE TABLE IF NOT EXISTS trades (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id       INTEGER,
        decision_id     INTEGER NOT NULL REFERENCES decisions(id),
        prediction_id   INTEGER NOT NULL,
        market_id       TEXT NOT NULL,
        direction       TEXT NOT NULL CHECK(direction IN ('YES','NO')),
        confidence      REAL NOT NULL,
        similarity      REAL NOT NULL,
        freshness       TEXT,
        p_hat           REAL NOT NULL,
        mid_price       REAL NOT NULL,
        edge            REAL NOT NULL,
        kelly_fraction  REAL NOT NULL,
        size_usdc       REAL NOT NULL,
        decision_path   TEXT NOT NULL DEFAULT 'mvp',
        features_snapshot TEXT NOT NULL,
        cycle_id        INTEGER NOT NULL,
        is_tracer       INTEGER NOT NULL DEFAULT 0,
        tracer_id       TEXT,
        decided_at      TEXT NOT NULL,
        closed_at       TEXT,
        realized_pnl    REAL,
        -- v6 (cycle 3): the model-implied "edge collapses to here" price.
        -- Closer's edge_decay rule fires when current_mid crosses it.
        -- For YES: target_mid = p_hat - fee_buffer.
        -- For NO:  target_mid = (1 - p_hat) + fee_buffer.
        -- NULL on legacy rows; rule skips when NULL.
        target_mid      REAL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_trades_market_id ON trades(market_id)",
    "CREATE INDEX IF NOT EXISTS idx_trades_cycle_id ON trades(cycle_id)",
    # Partial unique index replaces the old `signal_id UNIQUE` constraint.
    # MVP rows have a non-NULL signal_id; DKG rows leave it NULL, so the
    # index simply doesn't apply to them.
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_trades_signal_id "
    "ON trades(signal_id) WHERE signal_id IS NOT NULL",
    """
    CREATE TABLE IF NOT EXISTS trade_features (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_id        INTEGER NOT NULL REFERENCES trades(id),
        feature_name    TEXT NOT NULL,
        feature_value   REAL,
        feature_kind    TEXT NOT NULL CHECK(feature_kind IN ('numeric','flag','category')),
        feature_label   TEXT,
        UNIQUE(trade_id, feature_name)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_trade_features_trade_id ON trade_features(trade_id)",
    "CREATE INDEX IF NOT EXISTS idx_trade_features_feature_name ON trade_features(feature_name)",
]
db_module.register_schema("app/stages/trade.py", _TRADE_DDL)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _gate_entry(name: str, outcome: str, value, threshold) -> dict:
    return {"gate": name, "outcome": outcome, "value": value, "threshold": threshold}


def compose_direction_prompt(article: dict, market: dict) -> str:
    """Shared LLM-direction prompt builder for the MVP + DKG trade paths.

    Both paths read the news as plain language and ask whether it
    decisively resolves the market — see `docs/philosophy.md`. The
    function is module-level so `app/stages/dkg_trade.py` can call it
    without inheriting any state from `TradeStage`.
    """
    title = article.get("title") or ""
    summary = article.get("summary") or "(none)"
    question = market.get("question") or ""
    description = market.get("description") or "(none)"
    return (
        f"NEWS HEADLINE: {title}\n"
        f"NEWS SUMMARY:  {summary}\n"
        f"\n"
        f"MARKET QUESTION: {question}\n"
        f"MARKET DETAILS:  {description}\n"
        f"\n"
        f"Read the news as plain language. Does the news DECISIVELY\n"
        f"resolve this market?\n"
        f"\n"
        f"  YES     — the news plainly says the market's YES outcome\n"
        f"            will happen or has just happened.\n"
        f"  NO      — the news plainly says the market's NO outcome\n"
        f"            will happen or has just happened.\n"
        f"  NEUTRAL — the news is unrelated, ambiguous, or doesn't\n"
        f"            clearly push the market either way. Default to\n"
        f"            this whenever you're unsure.\n"
        f"\n"
        f"`confidence` is how CLEARLY the news resolves the market\n"
        f"(not a probability of YES). 0.9 = the article spells out\n"
        f"the resolution; 0.5 = related but ambiguous; below the gate\n"
        f"falls through to NEUTRAL anyway.\n"
        f"\n"
        f"Output JSON ONLY."
    )


class TradeStage:
    def __init__(self, deps: StageDeps) -> None:
        if deps.ai is None:
            raise RuntimeError("TradeStage requires deps.ai")
        self._db = deps.db
        self._bus = deps.bus
        self._ai = deps.ai
        self._cfg = deps.config.trade
        # Optional — when set, edge gate uses real CLOB mid for the bet
        # side; when None or the call returns None, we fall back to the
        # default_mid_price stub. This keeps tests + tracer harnesses
        # working without a live CLOB.
        self._clob = deps.clob

    async def start(self) -> None:
        return

    async def stop(self) -> None:
        return

    async def handle(self, payload: dict) -> None:
        signal_id = payload.get("signal_id")
        article_id = payload.get("article_id")
        market_id = payload.get("market_id")
        similarity = payload.get("similarity")
        freshness = payload.get("freshness")
        if signal_id is None or article_id is None or market_id is None or similarity is None:
            logger.warning("trade: payload missing required field: %r", payload)
            return
        is_tracer = bool(payload.get("is_tracer", False))
        tracer_id = payload.get("tracer_id")

        if await self._decision_exists(int(signal_id)):
            return

        cycle_id = current_cycle_id()
        gate_history: list[dict] = []

        # 1. similarity_floor -------------------------------------------------
        if float(similarity) < self._cfg.min_similarity:
            gate_history.append(
                _gate_entry("similarity_floor", "reject", float(similarity), self._cfg.min_similarity)
            )
            await self._record_reject(
                signal_id=int(signal_id),
                reason="similarity_floor",
                gate_history=gate_history,
                prediction_id=None,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return
        gate_history.append(
            _gate_entry("similarity_floor", "ok", float(similarity), self._cfg.min_similarity)
        )

        # 2. freshness --------------------------------------------------------
        if freshness not in self._cfg.accept_freshness:
            gate_history.append(
                _gate_entry("freshness", "reject", freshness, list(self._cfg.accept_freshness))
            )
            await self._record_reject(
                signal_id=int(signal_id),
                reason="freshness_stale",
                gate_history=gate_history,
                prediction_id=None,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return
        gate_history.append(
            _gate_entry("freshness", "ok", freshness, list(self._cfg.accept_freshness))
        )

        # Compose prompt from article + market. Done after the cheap gates so
        # we don't pay a DB round-trip for signals that fail upstream.
        article = await self._fetch_article(int(article_id))
        market = await self._fetch_market(str(market_id))
        if article is None:
            await self._record_reject(
                signal_id=int(signal_id),
                reason="article_missing",
                gate_history=gate_history + [_gate_entry("lookup", "reject", "article_missing", None)],
                prediction_id=None,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return
        if market is None:
            await self._record_reject(
                signal_id=int(signal_id),
                reason="market_missing",
                gate_history=gate_history + [_gate_entry("lookup", "reject", "market_missing", None)],
                prediction_id=None,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return

        # 2.5 entity_overlap (stage-0 hard filter) ---------------------------
        # Reject (article, market) pairs where the article's resolved
        # entities don't appear in the market question. Saves an LLM
        # call on obvious noise matches. Permissive when the extractor
        # hasn't completed for this article yet — `n_entities == 0`
        # means "no signal", not "no match".
        overlaps, n_entities = await article_entities_overlap_market(
            self._db, int(article_id), market["question"] or ""
        )
        if n_entities > 0 and not overlaps:
            gate_history.append(
                _gate_entry("entity_overlap", "reject", 0, n_entities)
            )
            await self._record_reject(
                signal_id=int(signal_id),
                reason="entity_mismatch",
                gate_history=gate_history,
                prediction_id=None,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return
        gate_history.append(
            _gate_entry(
                "entity_overlap",
                "ok",
                "match" if overlaps else "no_entities_yet",
                n_entities,
            )
        )

        prompt = self._compose_prompt(article, market)

        # 3. direction --------------------------------------------------------
        result = await self._ai.direction(
            signal_id=int(signal_id),
            prompt=prompt,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
        )
        if result.direction == "NEUTRAL":
            gate_history.append(
                _gate_entry("direction", "reject", "NEUTRAL", "YES|NO")
            )
            await self._record_reject(
                signal_id=int(signal_id),
                reason="direction_neutral",
                gate_history=gate_history,
                prediction_id=result.prediction_id,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return
        if result.confidence < self._cfg.min_confidence:
            gate_history.append(
                _gate_entry("direction", "reject", result.confidence, self._cfg.min_confidence)
            )
            await self._record_reject(
                signal_id=int(signal_id),
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

        # 4. edge -------------------------------------------------------------
        direction = result.direction
        confidence = result.confidence
        p_hat = confidence if direction == "YES" else (1.0 - confidence)
        mid = await self._resolve_mid(direction=direction, market=market)
        # 4a. degenerate-mid guard — markets at mid <= min_mid or >= max_mid
        # are essentially resolved. Cycle 3 surfaced PnL=0 trades on these
        # because the closer would force-close at entry_mid. Block at entry.
        if mid <= self._cfg.min_mid or mid >= self._cfg.max_mid:
            gate_history.append(_gate_entry("edge", "reject", mid, "mid_degenerate"))
            await self._record_reject(
                signal_id=int(signal_id),
                reason="mid_degenerate",
                gate_history=gate_history,
                prediction_id=result.prediction_id,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return
        # 4a-bis. wide-spread guard — Polymarket's /midpoint returns the
        # last traded price (not the true mid) when spread > $0.10. Any
        # edge estimate built on that scalar is unreliable. Read the book
        # and reject if spread exceeds the threshold. Graceful degrade:
        # if the book read fails (None), skip this check rather than
        # blocking the trade. See app/clob.py:get_top_of_book.
        spread = await self._resolve_spread(direction=direction, market=market)
        if spread is not None and spread > self._cfg.wide_spread_threshold:
            gate_history.append(_gate_entry("edge", "reject", spread, "wide_spread"))
            await self._record_reject(
                signal_id=int(signal_id),
                reason="wide_spread",
                gate_history=gate_history,
                prediction_id=result.prediction_id,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return
        # 4b. market-news weight imbalance — reject when the market is
        # too big / too thin / too far / too soon for the news to
        # plausibly move it. Cycle 7. See _check_market_news_imbalance.
        imbalance = await self._check_market_news_imbalance(
            market=market,
            mid=mid,
            freshness=freshness,
            signal_id=int(signal_id),
        )
        if imbalance is not None:
            sub_rule, detail = imbalance
            gate_history.append(
                _gate_entry("market_news_imbalance", "reject", sub_rule, detail)
            )
            await self._record_reject(
                signal_id=int(signal_id),
                reason="market_news_imbalance",
                gate_history=gate_history,
                prediction_id=result.prediction_id,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return
        if direction == "YES":
            edge = p_hat - mid - self._cfg.fee_buffer
        else:
            edge = mid - p_hat - self._cfg.fee_buffer
        if edge < self._cfg.min_edge:
            gate_history.append(_gate_entry("edge", "reject", edge, self._cfg.min_edge))
            await self._record_reject(
                signal_id=int(signal_id),
                reason="edge_below_min",
                gate_history=gate_history,
                prediction_id=result.prediction_id,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return
        gate_history.append(_gate_entry("edge", "ok", edge, self._cfg.min_edge))

        # 5. size -------------------------------------------------------------
        if direction == "YES":
            b = (1.0 - mid) / mid
            kelly_full = (p_hat * b - (1.0 - p_hat)) / b
        else:
            p_no = 1.0 - p_hat
            mid_no = 1.0 - mid
            b = (1.0 - mid_no) / mid_no
            kelly_full = (p_no * b - (1.0 - p_no)) / b
        kelly_capped = max(0.0, kelly_full) * self._cfg.kelly_fraction
        size = min(
            kelly_capped * self._cfg.balance_usdc,
            self._cfg.balance_usdc * self._cfg.max_position_fraction,
        )
        if size < self._cfg.min_position_usdc:
            gate_history.append(_gate_entry("size", "reject", size, self._cfg.min_position_usdc))
            await self._record_reject(
                signal_id=int(signal_id),
                reason="size_below_min",
                gate_history=gate_history,
                prediction_id=result.prediction_id,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return
        gate_history.append(_gate_entry("size", "ok", size, self._cfg.min_position_usdc))

        # 6. cash (paper wallet) ---------------------------------------------
        # Cycle 3: CLIP size to fit available cash instead of rejecting
        # outright. Multiple concurrent signals on the same market can
        # add to exposure up to whatever cash allows. Only when even the
        # clipped size falls below min_position_usdc do we reject.
        # Cash = initial - sum(open trade sizes), never negative.
        cur = await self._db.conn.execute(
            "SELECT COALESCE(SUM(t.size_usdc), 0) "
            "FROM trades t LEFT JOIN trade_closures c ON c.trade_id=t.id "
            "WHERE c.id IS NULL AND t.is_tracer=0"
        )
        row = await cur.fetchone()
        await cur.close()
        open_invested = float(row[0] or 0.0)
        cash_available = self._cfg.balance_usdc - open_invested
        if cash_available <= 0.0:
            gate_history.append(
                _gate_entry("cash", "reject", size, cash_available)
            )
            await self._record_reject(
                signal_id=int(signal_id),
                reason="insufficient_cash",
                gate_history=gate_history,
                prediction_id=result.prediction_id,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return
        if size > cash_available:
            # Clip — note the original requested size in gate_history.
            gate_history.append(
                _gate_entry("cash", "clip", size, cash_available)
            )
            size = cash_available
            if size < self._cfg.min_position_usdc:
                gate_history.append(
                    _gate_entry("size_after_clip", "reject", size, self._cfg.min_position_usdc)
                )
                await self._record_reject(
                    signal_id=int(signal_id),
                    reason="size_below_min",
                    gate_history=gate_history,
                    prediction_id=result.prediction_id,
                    cycle_id=cycle_id,
                    is_tracer=is_tracer,
                    tracer_id=tracer_id,
                )
                return
        else:
            gate_history.append(_gate_entry("cash", "ok", size, cash_available))

        # Execute path --------------------------------------------------------
        features_snapshot = {
            "gate_history": gate_history,
            "prompt": prompt,
            "llm_response_excerpt": (result.raw_response or "")[:512],
            "similarity": float(similarity),
            "freshness": freshness,
            "direction": direction,
            "confidence": confidence,
            "p_hat": p_hat,
            "mid": mid,
            "edge": edge,
            "kelly_full": kelly_full,
            "kelly_capped": kelly_capped,
            "size_usdc": size,
            "balance_usdc": self._cfg.balance_usdc,
        }
        await self._record_execute(
            signal_id=int(signal_id),
            market_id=str(market_id),
            prediction_id=result.prediction_id,
            direction=direction,
            confidence=confidence,
            similarity=float(similarity),
            freshness=freshness,
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
    # Internals
    # ------------------------------------------------------------------

    async def _decision_exists(self, signal_id: int) -> bool:
        cur = await self._db.conn.execute(
            "SELECT 1 FROM decisions WHERE signal_id=?", (signal_id,)
        )
        row = await cur.fetchone()
        await cur.close()
        return row is not None

    async def _fetch_article(self, article_id: int) -> Optional[dict]:
        cur = await self._db.conn.execute(
            "SELECT title, summary FROM articles WHERE id=?", (article_id,)
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return {"title": row["title"], "summary": row["summary"]}

    async def _fetch_market(self, market_id: str) -> Optional[dict]:
        cur = await self._db.conn.execute(
            "SELECT question, description, token_yes, token_no, "
            "       category, end_date, volume_usdc "
            "FROM markets WHERE condition_id=?",
            (market_id,),
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
            "category": row["category"],
            "end_date": row["end_date"],
            "volume_usdc": row["volume_usdc"],
        }

    async def _cluster_size_for_signal(self, signal_id: int) -> int:
        """COUNT(*) on article_cluster_members for the signal's cluster.
        Returns 1 (singleton) when the join produces no match (defensive
        for tests that skip dedupe). Used by the market_news_imbalance
        gate's singleton-on-extreme-mid sub-rule."""
        cur = await self._db.conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM article_cluster_members
            WHERE cluster_id = (SELECT cluster_id FROM signals WHERE id = ?)
            """,
            (signal_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None or row["n"] is None:
            return 1
        return max(1, int(row["n"]))

    @staticmethod
    def _days_to(end_date_iso: Optional[str]) -> Optional[int]:
        """Days from now (UTC) to a Polymarket end_date string. Returns
        None if the timestamp can't be parsed. Negative for past dates."""
        if not end_date_iso:
            return None
        from datetime import datetime, timezone
        s = end_date_iso.replace("Z", "+00:00")
        try:
            end = datetime.fromisoformat(s)
        except (TypeError, ValueError):
            return None
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return int((end - now).total_seconds() // 86400)

    async def _check_market_news_imbalance(
        self,
        *,
        market: dict,
        mid: float,
        freshness: Optional[str],
        signal_id: int,
    ) -> Optional[tuple[str, object]]:
        """Cycle 7: reject when news weight is too small for the market's
        narrative weight. Returns None (ok) or (sub_rule, detail). Five
        sub-rules — see plan §"Market-weight vs news-weight gate".

        Defensive: when a signal is missing (volume=None/0, end_date=None)
        we skip that sub-rule rather than fail-open the whole gate. This
        keeps the system trading while upstream parsers are improving."""
        cfg = self._cfg
        is_breaking = (freshness == "BREAKING")

        # Sub-rule 1: thin market (skip when volume unknown / 0 — parser bug).
        volume = market.get("volume_usdc")
        if isinstance(volume, (int, float)) and volume > 0:
            if volume < cfg.min_market_volume_usdc:
                return ("thin_market", float(volume))

        # Sub-rule 2: extreme mid without BREAKING news.
        if cfg.require_breaking_for_extreme_mid and not is_breaking:
            if mid <= cfg.extreme_mid_low or mid >= cfg.extreme_mid_high:
                return ("extreme_mid_no_breaking", mid)

        # Sub-rules 3 & 4: time-to-expiry.
        days = self._days_to(market.get("end_date"))
        if days is not None:
            if days < cfg.min_days_to_expiry:
                return ("near_expiry", days)
            if cfg.require_breaking_for_far_future and not is_breaking:
                if days > cfg.far_future_days:
                    return ("far_future_no_breaking", days)

        # Sub-rule 5: singleton cluster on extreme mid (slightly tighter
        # threshold than the breaking check). Costs one COUNT(*) query.
        if cfg.reject_singleton_on_extreme_mid:
            if mid <= 0.15 or mid >= 0.85:
                cluster_size = await self._cluster_size_for_signal(signal_id)
                if cluster_size == 1:
                    return ("singleton_on_extreme_mid", mid)
        return None

    def _compose_prompt(self, article: dict, market: dict) -> str:
        # Per `docs/philosophy.md`: irene is not a forecaster. The LLM is
        # asked to read the news as natural language and tell us whether
        # the article DECISIVELY resolves the market, not to estimate the
        # probability of YES. `confidence` here is "how clearly the news
        # resolves it", not P(YES).
        # Implementation lives at module scope (`compose_direction_prompt`)
        # so the DKG path (`app/stages/dkg_trade.py`) can call exactly the
        # same prompt without inheriting state from this class — the LLM
        # cache then stays warm across both paths for the same
        # (article, market) pair.
        return compose_direction_prompt(article, market)

    async def _resolve_mid(self, *, direction: str, market: Optional[dict]) -> float:
        """Return the mid-price for the side we're betting on.

        Order of preference:
          1. CLOB lookup for the side's token, when `deps.clob` is set
             and the market row has a non-NULL token_id for that side.
          2. `cfg.trade.default_mid_price` fallback (P1 stub).

        Errors from CLOB are absorbed (the client itself returns None on
        any recoverable failure); we just hit the fallback in that case.
        """
        if self._clob is None or market is None:
            return self._cfg.default_mid_price
        token_id: Optional[str] = market.get("token_yes") if direction == "YES" else market.get("token_no")
        if not token_id:
            return self._cfg.default_mid_price
        mid = await self._clob.get_mid(str(token_id))
        if mid is None:
            return self._cfg.default_mid_price
        return float(mid)

    async def _resolve_spread(
        self, *, direction: str, market: Optional[dict]
    ) -> Optional[float]:
        """Return `best_ask - best_bid` for the side we're betting on, or
        `None` if the book read fails (no client, no token, network /
        parse error, empty book). Callers use `None` to mean "can't
        tell" and should NOT reject on that signal alone — the
        degenerate-mid and market-news gates are still in force.
        """
        if self._clob is None or market is None:
            return None
        if not hasattr(self._clob, "get_top_of_book"):
            return None
        token_id: Optional[str] = (
            market.get("token_yes") if direction == "YES" else market.get("token_no")
        )
        if not token_id:
            return None
        book = await self._clob.get_top_of_book(str(token_id))
        if book is None:
            return None
        best_bid, best_ask = book
        return float(best_ask) - float(best_bid)

    async def _record_reject(
        self,
        *,
        signal_id: int,
        reason: str,
        gate_history: list[dict],
        prediction_id: Optional[int],
        cycle_id: int,
        is_tracer: bool,
        tracer_id: Optional[str],
    ) -> None:
        decision_id = await self._insert_decision(
            signal_id=signal_id,
            outcome="reject",
            reason=reason,
            gate_history=gate_history,
            prediction_id=prediction_id,
            cycle_id=cycle_id,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
        )
        if decision_id is None:
            # A prior pass already wrote the decision; don't republish.
            return
        await self._bus.publish(
            TOPIC_TRADE_DECIDED,
            {
                "trade_id": None,
                "decision_id": decision_id,
                "outcome": "reject",
                "reason": reason,
                "is_tracer": is_tracer,
                "tracer_id": tracer_id,
            },
        )

    async def _record_execute(
        self,
        *,
        signal_id: int,
        market_id: str,
        prediction_id: int,
        direction: str,
        confidence: float,
        similarity: float,
        freshness: Optional[str],
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
            signal_id=signal_id,
            outcome="execute",
            reason="ok",
            gate_history=gate_history,
            prediction_id=prediction_id,
            cycle_id=cycle_id,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
        )
        if decision_id is None:
            return
        trade_id = await self._insert_trade(
            signal_id=signal_id,
            decision_id=decision_id,
            prediction_id=prediction_id,
            market_id=market_id,
            direction=direction,
            confidence=confidence,
            similarity=similarity,
            freshness=freshness,
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
        if trade_id is None:
            return
        await self._insert_trade_features(
            trade_id=trade_id,
            features={
                "similarity": ("numeric", similarity, None),
                "freshness": ("category", None, freshness),
                "confidence": ("numeric", confidence, None),
                "p_hat": ("numeric", p_hat, None),
                "mid": ("numeric", mid, None),
                "edge": ("numeric", edge, None),
                "kelly_fraction": ("numeric", kelly_fraction, None),
                "size_usdc": ("numeric", size_usdc, None),
            },
        )
        await self._bus.publish(
            TOPIC_TRADE_DECIDED,
            {
                "trade_id": trade_id,
                "decision_id": decision_id,
                "outcome": "execute",
                "reason": "ok",
                "is_tracer": is_tracer,
                "tracer_id": tracer_id,
            },
        )

    async def _insert_decision(
        self,
        *,
        signal_id: int,
        outcome: str,
        reason: str,
        gate_history: list[dict],
        prediction_id: Optional[int],
        cycle_id: int,
        is_tracer: bool,
        tracer_id: Optional[str],
    ) -> Optional[int]:
        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT OR IGNORE INTO decisions
                    (signal_id, outcome, reason, gate_history, prediction_id,
                     cycle_id, is_tracer, tracer_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_id,
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
            if cur.rowcount != 1:
                return None
            return cur.lastrowid

    async def _insert_trade(
        self,
        *,
        signal_id: int,
        decision_id: int,
        prediction_id: int,
        market_id: str,
        direction: str,
        confidence: float,
        similarity: float,
        freshness: Optional[str],
        p_hat: float,
        mid: float,
        edge: float,
        kelly_fraction: float,
        size_usdc: float,
        features_snapshot: dict,
        cycle_id: int,
        is_tracer: bool,
        tracer_id: Optional[str],
    ) -> Optional[int]:
        # v6: derive the target_mid where the model's edge collapses.
        # `p_hat` is the model's implied P(YES). The target is always
        # expressed in YES-mid terms (Polymarket's primary price).
        # For YES we bought low expecting YES to rise toward p_hat —
        # sell just BELOW p_hat (-fee_buffer) to lock profit before fees.
        # For NO we bought expecting YES to fall toward p_hat — sell just
        # ABOVE p_hat (+fee_buffer) by the same logic. (Cycle 3 had a
        # sign bug here: target was 1-p_hat+fee, double-flipping and
        # firing edge_decay on every NO trade's first poll.)
        fee_buffer = float(self._cfg.fee_buffer)
        if direction == "YES":
            target_mid = max(0.0, min(1.0, float(p_hat) - fee_buffer))
        else:  # NO
            target_mid = max(0.0, min(1.0, float(p_hat) + fee_buffer))
        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT OR IGNORE INTO trades
                    (signal_id, decision_id, prediction_id, market_id, direction,
                     confidence, similarity, freshness, p_hat, mid_price, edge,
                     kelly_fraction, size_usdc, decision_path, features_snapshot,
                     cycle_id, is_tracer, tracer_id, decided_at, target_mid)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'mvp', ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_id,
                    decision_id,
                    prediction_id,
                    market_id,
                    direction,
                    confidence,
                    similarity,
                    freshness,
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
                    target_mid,
                ),
            )
            if cur.rowcount != 1:
                return None
            return cur.lastrowid

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

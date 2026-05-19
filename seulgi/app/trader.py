"""
Seulgi paper trader.

Responsibilities:
  1. Receive a headline + list of market matches.
  2. Classify each match in parallel via the AI classifier (semaphore-limited).
  3. Decide whether to trade based on edge, balance, and position limits.
  4. Record the trade decision to the DB.
  5. Open a paper position (mock fill at current mid-price).
  6. Periodically close positions that hit a stop condition.

Edge model
──────────
For every actionable signal (YES or NO with enough confidence), we estimate
how far the true probability is above the token's current mid-price:

    edge = edge_increment * (2 * confidence - 1)

  • confidence = 0.5  →  edge = 0       (no signal, skip)
  • confidence = 0.67 →  edge = 0.083   (borderline)
  • confidence = 1.0  →  edge = edge_increment  (maximum certainty)

p_hat = token_mid_price + edge

A trade is executed only when edge ≥ min_edge.  This makes confidence directly
responsible for filtering — there's no magic constant for "how far we think the
market is mispriced."

Guards (all checked before deducting balance)
─────────────────────────────────────────────
  • NEUTRAL direction or confidence < min_confidence  → skip
  • No token available for the direction              → skip
  • Cannot fetch mid-price                            → skip
  • Edge < min_edge                                   → skip (logged as near-miss)
  • Balance < min_trade_usdc                          → skip
  • Open positions ≥ max_open_positions               → skip
  • Condition ID already has an open position         → skip (no doubling-up)

All guards use a single asyncio.Lock so concurrent headline processing
cannot double-spend balance or exceed position limits.

Position close conditions (checked every 60 s by monitor_positions)
────────────────────────────────────────────────────────────────────
  • token price ≥ 0.95  → WIN  (market resolving YES)
  • token price ≤ 0.05  → LOSS (market resolving against us)
  • age ≥ timeout_hours → TIMEOUT (sell at whatever price we can get)

PnL accounting
──────────────
  shares = size_usdc / entry_price
  realized_pnl = (exit_price - entry_price) * shares
  balance_returned = size_usdc + realized_pnl

Balance is restored from DB on init_from_db() so restarts are safe.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from ai import Direction, ClassifyResult, DirectionClassifier
from db import SeulgiDB
from markets import MarketMatch
from watcher import RawHeadline
import simulate as sim  # plan §7 — fees/slippage/Kelly helpers

logger = logging.getLogger(__name__)

_CLOB_PRICE_URL = "https://clob.polymarket.com/midpoint?token_id={token_id}"
_WIN_PRICE  = 0.95
_LOSS_PRICE = 0.05


# ---------------------------------------------------------------------------
# Public result type
# ---------------------------------------------------------------------------


@dataclass
class TradeDecision:
    """Result of evaluating one headline against one market."""
    condition_id: str
    question: str
    direction: Direction
    confidence: float
    similarity: float
    p_hat: float
    mid_price: Optional[float]
    edge: float               # p_hat - mid_price (0 if mid unavailable)
    size_usdc: float
    executed: bool
    skip_reason: Optional[str]   # None when executed
    # Cycle-4 analytics fields (logged only — not used by execution).
    # Let the next postmortem build a real calibration curve between the
    # classifier's raw confidence and realized outcome, and quantify how
    # often the alternative "p_hat = confidence" model would flip sign vs
    # the production "p_hat = mid + bump" model.
    confidence_raw: float = 0.0          # AI's claimed P(direction)
    p_hat_if_raw: float = 0.0            # what p_hat would be under conf-as-prob model
    signed_edge: float = 0.0             # p_hat - mid (matches production edge; renamed for clarity)
    signed_edge_if_raw: float = 0.0      # confidence_raw - mid
    agreement: float = 0.0               # ensemble agreement fraction (1.0 = unanimous, 1/N = noise)
    # Cycle-7: AI's two-axis judgement on the trade itself.
    # p_alpha   = AI's estimated P(this trade is +EV in the 5–30m window)
    # risk_score = expected |Δ5m| magnitude on a [0,1] scale
    p_alpha: float = 0.0
    risk_score: float = 0.0
    # Both rails captured at decision time. Lets post-hoc analysis re-derive
    # the edge against the *opposite* side and detect skew where the model
    # picked the side with worse rail headroom.
    mid_yes: Optional[float] = None
    mid_no: Optional[float] = None
    # True when invert_high_conf flipped this trade's direction off the AI's
    # original call. The trade's `direction` reflects the post-flip side.
    inverted: bool = False


# ---------------------------------------------------------------------------
# Price oracle
# ---------------------------------------------------------------------------


async def _fetch_mid_price(
    session: aiohttp.ClientSession, token_id: str
) -> Optional[float]:
    """Fetch the current mid-price for a Polymarket CLOB token."""
    url = _CLOB_PRICE_URL.format(token_id=token_id)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            mid = data.get("mid")
            return float(mid) if mid is not None else None
    except Exception as exc:
        logger.debug("[trader] price fetch failed %s: %s", token_id[:12], exc)
        return None


# ---------------------------------------------------------------------------
# Trader
# ---------------------------------------------------------------------------


class PaperTrader:
    """
    Evaluates headlines against market matches and records paper trades.

    Usage:
        trader = PaperTrader(db, classifier, cfg, ai_cfg)
        await trader.init_from_db()          # restore state after restart
        decisions = await trader.process(headline, matches)
        await trader.monitor_positions()     # call periodically
    """

    def __init__(
        self,
        db: SeulgiDB,
        classifier: DirectionClassifier,
        cfg,    # TradingConfig
        ai_cfg, # AIConfig
        engine_name: str = "default",
        rule_engine=None,   # Optional[RuleEngine] — None disables rule gating
        variant_name: Optional[str] = None,
        parent_engine: Optional[str] = None,
    ) -> None:
        self._db = db
        self._classifier = classifier
        self._cfg = cfg
        self._ai_cfg = ai_cfg
        self._engine_name = engine_name
        # Variants share parent_engine groupings and AI calls but write their
        # own trade rows tagged with variant_name. When no variants are
        # defined, variant_name == engine_name (existing behaviour).
        self._variant_name = variant_name or engine_name
        self._parent_engine = parent_engine or engine_name
        # Manual-planning rules engine — shared across engines, evaluated
        # pre-AI (to save tokens) and post-AI (to skip on AI confidence).
        self._rules = rule_engine
        # In infinite-balance mode, `_balance` represents cumulative *realized*
        # PnL (the "gain"), starting at 0. There's no capital constraint —
        # position-side caps (max_open / max_unique / cold-cull) gate trades.
        # In normal mode it's the live USDC bankroll.
        self._balance: float = 0.0 if cfg.infinite_balance else cfg.balance_usdc
        self._total_staked: float = 0.0     # lifetime USDC put at risk
        self._last_tokens_used: int = 0     # tokens consumed by most recent process()
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None
        # Lessons cache: refreshed at most once per LESSON_TTL seconds, since
        # the aggregator only runs every ~10 min.
        self._lesson_notes: list[str] = []
        self._lesson_cache_until: float = 0.0

    async def _get_lesson_notes_cached(self) -> list[str]:
        from time import monotonic
        if monotonic() < self._lesson_cache_until:
            return self._lesson_notes
        try:
            import lessons as _lessons
            notes = await _lessons.load_active_notes(self._db, limit=8)
        except Exception:  # noqa: BLE001
            notes = []
        self._lesson_notes = notes
        self._lesson_cache_until = monotonic() + 60.0
        return notes

    # ------------------------------------------------------------------
    # Init
    # ------------------------------------------------------------------

    async def init_from_db(self) -> None:
        """
        Restore balance/gain from DB.

        Infinite mode: balance is "cumulative realized PnL". Restore as the
            sum of realized_pnl for this engine's closed trades.
        Normal mode: balance = starting balance minus currently committed.
        """
        if self._cfg.infinite_balance:
            # Cumulative realized gain so far + total staked across all trades
            # within the *current* cycle window (cycle 8+). The cycles table
            # has a single row with ended_at IS NULL; its started_at narrows
            # the restore. Without this filter the dashboard would carry the
            # PnL of every prior cycle (since we no longer delete trades).
            cycle = await self._db.get_current_cycle()
            since_ts = cycle["started_at"] if cycle else None
            gain, staked = await self._db.get_engine_gain_and_staked(
                self._engine_name, since_ts=since_ts,
            )
            self._balance = gain
            self._total_staked = staked
            cycle_label = cycle["name"] if cycle else "<no cycle>"
            logger.info(
                "[trader] [%s] infinite mode restored (cycle=%s): gain=%+.2f  staked=%.2f",
                self._engine_name, cycle_label, self._balance, self._total_staked,
            )
            return

        committed = await self._db.get_committed_usdc(engine_name=self._engine_name)
        if committed > 0:
            self._balance = self._cfg.balance_usdc - committed
            logger.info(
                "[trader] [%s] restored: balance=%.2f (%.2f committed in %d positions)",
                self._engine_name,
                self._balance,
                committed,
                await self._db.count_open_positions(engine_name=self._engine_name),
            )
        else:
            self._balance = self._cfg.balance_usdc

    # ------------------------------------------------------------------
    # Session
    # ------------------------------------------------------------------

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    async def process(
        self, headline: RawHeadline, matches: list[MarketMatch]
    ) -> list[TradeDecision]:
        """
        Backwards-compatible single-trader path: insert signal, run pre-AI
        rules, call the classifier directly, then delegate to evaluate().

        Used by the existing test suite and any single-engine deployment. In
        the multi-engine/variant runner path, EngineRunner does the signal
        insert + classify_batch once and calls evaluate() per variant
        directly — keeping AI tokens flat across variants.
        """
        if not matches:
            self._last_tokens_used = 0
            return []

        signal_id = await self._db.insert_signal(
            site_url=headline.site_url,
            headline=headline.headline,
            headline_id=getattr(headline, "headline_id", None),
        )

        # Pre-AI rule gate
        survivors: list[MarketMatch] = []
        pre_skips: list[tuple[MarketMatch, str]] = []
        if self._rules is not None:
            fresh_lbl = getattr(headline, "_freshness_label", None)
            for m in matches:
                reason = self._rules.gate_pre_ai(
                    market_type=m.market_type, mid_price=m.mid_yes,
                    freshness_label=fresh_lbl, question=m.question,
                )
                if reason is None:
                    survivors.append(m)
                else:
                    pre_skips.append((m, reason))
        else:
            survivors = list(matches)

        # AI call
        classify_results: list[ClassifyResult] = []
        prompt_meta: dict = {}
        if survivors:
            is_republish = getattr(headline, "_is_republish", False)
            similar_to = getattr(headline, "_similar_to", None)
            source_host: Optional[str] = None
            site_url = getattr(headline, "site_url", None)
            if site_url:
                try:
                    from urllib.parse import urlparse
                    source_host = urlparse(site_url).netloc or None
                except Exception:  # noqa: BLE001
                    source_host = None
            try:
                lesson_notes = await self._get_lesson_notes_cached()
            except Exception:  # noqa: BLE001
                lesson_notes = []

            raw = await self._classifier.classify_batch(
                headline=headline.headline,
                matches=survivors,
                is_republish=is_republish,
                similar_to=similar_to,
                source=source_host,
                lesson_notes=lesson_notes,
            )
            # classify_batch returns (results, prompt_meta) post-refactor. Older
            # test fixtures still return a bare list — accept both so the
            # existing test suite keeps working without per-test edits.
            if isinstance(raw, tuple) and len(raw) == 2:
                classify_results, prompt_meta = raw
            else:
                classify_results, prompt_meta = raw, {}

        self._last_tokens_used = sum(r.tokens_used for r in classify_results)
        prompt_meta_json = (
            json.dumps(prompt_meta, separators=(",", ":")) if prompt_meta else None
        )
        return await self.evaluate(
            signal_id, headline, survivors, classify_results,
            prompt_meta_json, pre_ai_skips=pre_skips,
        )

    async def evaluate(
        self,
        signal_id: int,
        headline: RawHeadline,
        survivors: list[MarketMatch],
        classify_results: list[ClassifyResult],
        prompt_meta_json: Optional[str],
        pre_ai_skips: Optional[list[tuple[MarketMatch, str]]] = None,
        relevance_skips: Optional[list] = None,  # list[(MarketMatch, RelevanceVerdict)]
    ) -> list[TradeDecision]:
        """
        Evaluate pre-classified survivors against this variant's rule set.

        The runner does the AI call once per headline (shared across all
        variants) and feeds the results in here. Pre-AI rule failures arrive
        as `pre_ai_skips` so this variant still records them as skip rows
        tagged with its variant_name.

        Returns one TradeDecision per market (skips + survivors).
        """
        decisions: list[TradeDecision] = []

        # Record pre-AI skips with a minimal trace. Same skip rows used to be
        # written N times (once per engine.trader.process); the runner now
        # passes pre_ai_skips identical for every variant.
        for match, reason in pre_ai_skips or []:
            decision = TradeDecision(
                condition_id=match.condition_id,
                question=match.question,
                direction=Direction.NEUTRAL,
                confidence=0.0,
                similarity=match.similarity,
                p_hat=0.0,
                mid_price=match.mid_yes,
                edge=0.0,
                size_usdc=0.0,
                executed=False,
                skip_reason=reason,
                mid_yes=match.mid_yes,
                mid_no=match.mid_no,
            )
            trace = [{"step": "pre_ai_rule", "pass": False, "reason": reason}]
            await self._record(
                signal_id, decision, headline=headline, token_id=None,
                trace=trace, result=None, prompt_meta_json=prompt_meta_json,
                match=match,
            )
            decisions.append(decision)

        # Record relevance gate drops — Stage-2 said this market is unrelated.
        # One trade row per variant per dropped match, so the dashboard funnel
        # still accounts for every (variant × match) decision.
        for match, verdict in relevance_skips or []:
            score = getattr(verdict, "score", "NO")
            reason_text = getattr(verdict, "reason", "")
            skip_reason = f"not_relevant:{score}"
            decision = TradeDecision(
                condition_id=match.condition_id,
                question=match.question,
                direction=Direction.NEUTRAL,
                confidence=0.0,
                similarity=match.similarity,
                p_hat=0.0,
                mid_price=match.mid_yes,
                edge=0.0,
                size_usdc=0.0,
                executed=False,
                skip_reason=skip_reason,
                mid_yes=match.mid_yes,
                mid_no=match.mid_no,
            )
            trace = [{
                "step": "relevance", "pass": False,
                "score": score, "reason": reason_text[:200],
            }]
            await self._record(
                signal_id, decision, headline=headline, token_id=None,
                trace=trace, result=None, prompt_meta_json=prompt_meta_json,
                match=match,
            )
            decisions.append(decision)

        if not survivors:
            return decisions

        session = self._get_session()
        for match, result in zip(survivors, classify_results):
            decision = await self._evaluate_market(
                session, signal_id, headline, match, result, prompt_meta_json,
            )
            decisions.append(decision)

        return decisions

    async def evaluate_low_sim_skips(
        self,
        signal_id: int,
        headline: RawHeadline,
        matches: list[MarketMatch],
        reason: str,
        max_sim: float,
        prompt_meta_json: Optional[str] = None,
    ) -> list[TradeDecision]:
        """Write per-variant skip rows for the cycle-13 low-similarity
        bypass. Behaves like `evaluate_freshness_skips` but with a
        `low_similarity` trace step and no AI artefacts attached.
        """
        decisions: list[TradeDecision] = []
        for match in matches:
            decision = TradeDecision(
                condition_id=match.condition_id,
                question=match.question,
                direction=Direction.NEUTRAL,
                confidence=0.0,
                similarity=match.similarity,
                p_hat=0.0,
                mid_price=match.mid_yes,
                edge=0.0,
                size_usdc=0.0,
                executed=False,
                skip_reason=reason,
                mid_yes=match.mid_yes,
                mid_no=match.mid_no,
            )
            trace = [{
                "step": "low_similarity", "pass": False,
                "max_sim": round(max_sim, 4),
            }]
            await self._record(
                signal_id, decision, headline=headline, token_id=None,
                trace=trace, result=None, prompt_meta_json=prompt_meta_json,
                match=match,
            )
            decisions.append(decision)
        return decisions

    async def evaluate_ai_paused_skips(
        self,
        signal_id: int,
        headline: RawHeadline,
        matches: list[MarketMatch],
    ) -> list[TradeDecision]:
        """plan §3 — simulate mode AI pause. Persists per-engine×match skip
        rows with reason='ai_paused_no_capital' so the decision funnel
        accounts for the headline even though no LLM call fired.
        """
        decisions: list[TradeDecision] = []
        for match in matches:
            decision = TradeDecision(
                condition_id=match.condition_id,
                question=match.question,
                direction=Direction.NEUTRAL,
                confidence=0.0,
                similarity=match.similarity,
                p_hat=0.0,
                mid_price=match.mid_yes,
                edge=0.0,
                size_usdc=0.0,
                executed=False,
                skip_reason="ai_paused_no_capital",
                mid_yes=match.mid_yes,
                mid_no=match.mid_no,
            )
            trace = [{
                "step": "ai_paused", "pass": False,
                "balance": round(self._balance, 4),
                "min_trade_usdc": round(self._cfg.min_trade_usdc, 4),
            }]
            await self._record(
                signal_id, decision, headline=headline, token_id=None,
                trace=trace, result=None, prompt_meta_json=None,
                match=match,
            )
            decisions.append(decision)
        return decisions

    async def evaluate_freshness_skips(
        self,
        signal_id: int,
        headline: RawHeadline,
        matches: list[MarketMatch],
        freshness_label: str,
        nearest_age_secs: Optional[float],
        prompt_meta_json: Optional[str] = None,
    ) -> list[TradeDecision]:
        """Write per-variant skip rows for a freshness-driven short-circuit.

        Used when the runner classifies a headline as REHASH (or any other
        label we choose to gate on): no AI call is made, but the funnel
        still needs a row per (variant × match) tagged with `freshness:rehash`
        so the dashboard accounts for every decision.
        """
        decisions: list[TradeDecision] = []
        age = int(nearest_age_secs) if nearest_age_secs is not None else 0
        skip_reason = f"freshness:{freshness_label.lower()}:age={age}"
        for match in matches:
            decision = TradeDecision(
                condition_id=match.condition_id,
                question=match.question,
                direction=Direction.NEUTRAL,
                confidence=0.0,
                similarity=match.similarity,
                p_hat=0.0,
                mid_price=match.mid_yes,
                edge=0.0,
                size_usdc=0.0,
                executed=False,
                skip_reason=skip_reason,
                mid_yes=match.mid_yes,
                mid_no=match.mid_no,
            )
            trace = [{
                "step": "freshness", "label": freshness_label,
                "nearest_age_secs": age,
            }]
            await self._record(
                signal_id, decision, headline=headline, token_id=None,
                trace=trace, result=None, prompt_meta_json=prompt_meta_json,
                match=match,
            )
            decisions.append(decision)
        return decisions

    # ------------------------------------------------------------------
    # Per-market evaluation
    # ------------------------------------------------------------------

    async def _evaluate_market(
        self,
        session: aiohttp.ClientSession,
        signal_id: int,
        headline: RawHeadline,
        match: MarketMatch,
        result: ClassifyResult,
        prompt_meta_json: Optional[str],
    ) -> TradeDecision:
        """Evaluate one match and record the trade decision."""
        # Step-by-step audit trail of guard outcomes. Persisted as JSON on
        # the trade row so post-hoc filtering ("which trades survived
        # min_confidence but died on min_edge?") becomes a json_extract query.
        trace: list[dict] = []
        inverted_flag = False

        def _skip(reason: str, p_hat: float = 0.0, mid: Optional[float] = None) -> TradeDecision:
            conf_raw = result.confidence
            edge_val = (p_hat - mid) if mid is not None else 0.0
            edge_if_raw = (conf_raw - mid) if mid is not None else 0.0
            return TradeDecision(
                condition_id=match.condition_id,
                question=match.question,
                direction=result.direction,
                confidence=result.confidence,
                similarity=match.similarity,
                p_hat=p_hat,
                mid_price=mid,
                edge=edge_val,
                size_usdc=0.0,
                executed=False,
                skip_reason=reason,
                confidence_raw=conf_raw,
                p_hat_if_raw=conf_raw,
                signed_edge=edge_val,
                signed_edge_if_raw=edge_if_raw,
                agreement=getattr(result, "agreement", 0.0),
                p_alpha=getattr(result, "p_alpha", 0.0),
                risk_score=getattr(result, "risk_score", 0.0),
                mid_yes=match.mid_yes,
                mid_no=match.mid_no,
                inverted=inverted_flag,
            )

        async def _finalize_skip(reason, token_id, p_hat=0.0, mid=None):
            trace.append({"step": "skip", "reason": reason})
            decision = _skip(reason, p_hat=p_hat, mid=mid)
            await self._record(
                signal_id, decision, headline=headline, token_id=token_id,
                trace=trace, result=result, prompt_meta_json=prompt_meta_json,
                inverted=inverted_flag, match=match,
            )
            return decision

        # ── AI filters ────────────────────────────────────────────────
        trace.append({
            "step": "ai",
            "direction": result.direction.value,
            "confidence": round(result.confidence, 4),
            "p_alpha": round(getattr(result, "p_alpha", 0.0), 4),
            "agreement": round(getattr(result, "agreement", 0.0), 4),
            "votes": getattr(result, "votes", {}) or {},
        })

        if result.direction == Direction.NEUTRAL:
            return await _finalize_skip("neutral", token_id=None)

        if result.confidence < self._ai_cfg.min_confidence:
            trace.append({
                "step": "min_confidence", "pass": False,
                "value": round(result.confidence, 4),
                "threshold": self._ai_cfg.min_confidence,
            })
            return await _finalize_skip(f"low_confidence:{result.confidence:.2f}", token_id=None)
        trace.append({"step": "min_confidence", "pass": True,
                      "value": round(result.confidence, 4),
                      "threshold": self._ai_cfg.min_confidence})

        # Ensemble agreement gate: drop trades where the parallel samples
        # didn't reach majority consensus. agreement=0 means the result came
        # in pre-AI (no ensemble run) — accept those without checking.
        min_agree = getattr(self._ai_cfg, "min_agreement", 0.66)
        agree = getattr(result, "agreement", 0.0) or 0.0
        if agree > 0.0 and agree < min_agree:
            trace.append({"step": "agreement", "pass": False,
                          "value": round(agree, 4), "threshold": min_agree})
            return await _finalize_skip(f"low_agreement:{agree:.2f}", token_id=None)
        trace.append({"step": "agreement", "pass": True,
                      "value": round(agree, 4), "threshold": min_agree})

        # ── Min-entry-YES-mid gate (cycle 10) ─────────────────────────
        # Asymmetric entry gate: refuse entries when the market's YES mid
        # is below `min_entry_yes_mid`, regardless of AI direction. The
        # observation: profitable slice was direction=NO × entry∈[0.75,0.90]
        # on high-priced markets where consensus is wrong-side-up. Variants
        # opt in to focus there; 0 disables.
        min_entry = self._cfg.min_entry_yes_mid
        if min_entry > 0 and match.mid_yes is not None and match.mid_yes < min_entry:
            trace.append({"step": "min_entry", "pass": False,
                          "mid_yes": round(match.mid_yes, 4),
                          "threshold": min_entry})
            return await _finalize_skip(
                f"min_entry_yes_mid:{match.mid_yes:.3f}", token_id=None,
            )

        # ── Market volatility gate (cycle 11) ─────────────────────────
        # Refuse entries when 30-min pre-headline stddev exceeds the
        # variant's tolerance — wide-bouncing markets trip SL on noise
        # alone. Reads the MarketContext stashed on the match by the
        # runner; absent ctx → guard is skipped (fail-open). 0 disables.
        max_vol = self._cfg.max_market_vol_30m
        mkt_ctx = getattr(match, "_mkt_ctx", None)
        if max_vol > 0 and mkt_ctx is not None and mkt_ctx.vol > max_vol:
            trace.append({"step": "mkt_vol", "pass": False,
                          "vol": round(mkt_ctx.vol, 4),
                          "threshold": max_vol})
            return await _finalize_skip(
                f"market_too_noisy:{mkt_ctx.vol:.3f}", token_id=None,
            )

        # ── Post-AI rule gate ─────────────────────────────────────────
        # Use the YES-side mid as the canonical reference price (matches the
        # pre-AI gate). Direction here is the AI's call BEFORE the optional
        # inversion below — intentional: we want to catch high-conf calls
        # regardless of whether the shadow engine later flips them.
        if self._rules is not None:
            reason = self._rules.gate_post_ai(
                market_type=match.market_type,
                mid_price=match.mid_yes,
                direction=result.direction.value,
                confidence=result.confidence,
                freshness_label=getattr(headline, "_freshness_label", None),
                question=match.question,
            )
            if reason is not None:
                trace.append({"step": "post_ai_rule", "pass": False, "reason": reason})
                return await _finalize_skip(reason, token_id=None)
            trace.append({"step": "post_ai_rule", "pass": True})

        # ── Optional: invert direction on high-confidence calls ───────
        # Shadow strategy: empirically conf ≥ invert_conf_threshold has IC<0
        # against the next 5m. Flip only when the *original* side's mid is
        # not at a rail (so the flip lands on a tradeable price too).
        if (
            self._cfg.invert_high_conf
            and result.confidence >= self._cfg.invert_conf_threshold
        ):
            original_mid = (
                match.mid_yes if result.direction == Direction.YES else match.mid_no
            )
            if original_mid is not None and 0.05 < original_mid < 0.95:
                from dataclasses import replace as _dc_replace
                flipped = Direction.NO if result.direction == Direction.YES else Direction.YES
                trace.append({"step": "invert", "fired": True,
                              "from": result.direction.value, "to": flipped.value})
                result = _dc_replace(result, direction=flipped)
                inverted_flag = True
            else:
                trace.append({"step": "invert", "fired": False, "reason": "rail_or_no_mid"})
        elif self._cfg.invert_high_conf:
            trace.append({"step": "invert", "fired": False, "reason": "below_threshold"})

        # ── Token + price selection ───────────────────────────────────
        # Prefer the price that was prefetched in EngineRunner.process()
        # (one parallel gather instead of N serial HTTP calls per engine).
        if result.direction == Direction.YES:
            token_id = match.token_yes
            mid_price = match.mid_yes
        else:
            token_id = match.token_no
            mid_price = match.mid_no

        if not token_id:
            return await _finalize_skip("no_token", token_id=None)

        # Fallback: refetch only if prefetch missed this side
        if mid_price is None:
            mid_price = await _fetch_mid_price(session, token_id)
        if mid_price is None:
            return await _finalize_skip("no_price", token_id=token_id)

        # ── Hard rail floor (applies to every engine; aggressive too) ─
        # At extreme prices the bid-ask spread alone routinely exceeds a
        # tight stop-loss, so SL fires on spread before any signal arrives.
        # Reject entries inside [0, hard_thr] ∪ [1-hard_thr, 1] regardless
        # of whether the engine opted out of the softer rail filter below.
        hard_thr = self._cfg.entry_rail_skip_threshold
        if hard_thr > 0 and (mid_price <= hard_thr or mid_price >= 1.0 - hard_thr):
            trace.append({"step": "hard_rail", "pass": False,
                          "mid": round(mid_price, 4), "threshold": hard_thr})
            return await _finalize_skip(f"hard_rail:{mid_price:.3f}", token_id=token_id, mid=mid_price)
        trace.append({"step": "hard_rail", "pass": True, "mid": round(mid_price, 4)})

        # ── Soft rail-skip: refuse trades on saturated markets ────────
        # In the 6h backtest, p_t0 ≥ 0.95 was 99% no_signal — the headline
        # cannot move a market already pinned at the rail. Aggressive opts
        # out (rail_skip_threshold=0) to keep its TP harvesters running.
        rail_thr = self._cfg.rail_skip_threshold
        if rail_thr > 0 and (mid_price <= rail_thr or mid_price >= 1.0 - rail_thr):
            trace.append({"step": "rail_skip", "pass": False,
                          "mid": round(mid_price, 4), "threshold": rail_thr})
            return await _finalize_skip(f"rail_price:{mid_price:.3f}", token_id=token_id, mid=mid_price)
        if rail_thr > 0:
            trace.append({"step": "rail_skip", "pass": True, "mid": round(mid_price, 4)})

        # ── Edge model ────────────────────────────────────────────────
        # edge = edge_increment * (2 * confidence - 1)
        # This makes confidence directly control edge size:
        #   conf=0.5 → edge=0 (no signal)
        #   conf=1.0 → edge=edge_increment (maximum certainty)
        confidence_scale = max(0.0, 2.0 * result.confidence - 1.0)
        edge = self._cfg.edge_increment * confidence_scale
        p_hat = mid_price + edge

        # Cycle-4 analytics shadow values (logged only; execution unchanged):
        #   p_hat_if_raw = confidence (the "trust the AI's probability" model)
        #   signed_edge_if_raw = confidence - mid_price (negative when the AI's
        #     raw confidence sits below the directional token's mid — useful
        #     for spotting trades the alternative model would have killed)
        confidence_raw = result.confidence
        p_hat_if_raw = confidence_raw
        signed_edge = edge                              # production edge is already mid-relative
        signed_edge_if_raw = confidence_raw - mid_price

        if edge < self._cfg.min_edge:
            trace.append({"step": "edge", "pass": False,
                          "p_hat": round(p_hat, 4), "mid": round(mid_price, 4),
                          "edge": round(edge, 4), "min_edge": self._cfg.min_edge})
            return await _finalize_skip(
                f"edge_too_small:{edge:.4f}", token_id=token_id, p_hat=p_hat, mid=mid_price,
            )
        trace.append({"step": "edge", "pass": True,
                      "p_hat": round(p_hat, 4), "mid": round(mid_price, 4),
                      "edge": round(edge, 4), "min_edge": self._cfg.min_edge})

        # ── Balance / position guards (under lock) ────────────────────
        async with self._lock:
            # Per-engine open position cap
            open_count = await self._db.count_open_positions(engine_name=self._engine_name)
            if open_count >= self._cfg.max_open_positions:
                trace.append({"step": "max_positions", "pass": False,
                              "value": open_count, "threshold": self._cfg.max_open_positions})
                return await _finalize_skip(
                    f"max_positions:{open_count}", token_id=token_id, p_hat=p_hat, mid=mid_price,
                )

            # Per-engine duplicate position check (one engine can hold a market
            # even if another engine is in the same market)
            already_open = await self._db.has_open_position(
                match.condition_id, engine_name=self._engine_name
            )
            if already_open:
                trace.append({"step": "duplicate_position", "pass": False})
                return await _finalize_skip(
                    "duplicate_position", token_id=token_id, p_hat=p_hat, mid=mid_price,
                )

            unique = await self._db.count_unique_open_markets(engine_name=self._engine_name)
            if unique >= self._cfg.max_unique_markets:
                trace.append({"step": "max_unique_markets", "pass": False,
                              "value": unique, "threshold": self._cfg.max_unique_markets})
                return await _finalize_skip(
                    f"max_unique_markets:{unique}", token_id=token_id, p_hat=p_hat, mid=mid_price,
                )

            # Compute actual trade size.
            # Simulate mode (plan §7.4): fractional Kelly sized off `p_hat`
            #   vs current market price. Falls through to "fixed" branch if
            #   the operator set simulate_sizing.method = "fixed".
            # Infinite mode: always trade max_position_usdc (no balance gating).
            # Normal/simulate-fixed: capped by remaining balance, must clear
            #   min_trade_usdc.
            simulate_mode = getattr(self._cfg, "mode", "paper") == "simulate"
            if simulate_mode and self._cfg.simulate_sizing.method == "fractional_kelly":
                # p_market = price we'd pay for the YES of the side we're
                # buying. Direction is already resolved above; for a YES
                # entry use mid_yes, for NO use mid_no (which equals
                # 1 - mid_yes in a well-formed binary market).
                if result.direction == Direction.YES:
                    p_market = match.mid_yes if match.mid_yes is not None else mid_price
                else:
                    p_market = match.mid_no if match.mid_no is not None else (1.0 - mid_price)
                kelly = sim.fractional_kelly(
                    bankroll_usdc=self._balance,
                    p_true=p_hat,
                    p_market=p_market,
                    max_position_usdc=self._cfg.max_position_usdc,
                    cfg=self._cfg.simulate_sizing,
                )
                trace.append({"step": "kelly",
                              "f_star": round(kelly.f_star, 4),
                              "fractional": round(kelly.fractional, 4),
                              "capped_pct": round(kelly.capped_pct, 4),
                              "stake_usdc": round(kelly.stake_usdc, 4),
                              "skip_reason": kelly.skip_reason,
                              "bankroll": round(self._balance, 4),
                              "p_market": round(p_market, 4),
                              "p_true": round(p_hat, 4)})
                if kelly.skip_reason is not None:
                    return await _finalize_skip(
                        kelly.skip_reason, token_id=token_id, p_hat=p_hat, mid=mid_price,
                    )
                size = kelly.stake_usdc
            elif self._cfg.infinite_balance:
                size = self._cfg.max_position_usdc
            else:
                size = min(self._cfg.max_position_usdc, self._balance)
                if size < self._cfg.min_trade_usdc:
                    reason = "no_balance" if self._balance <= 0 else f"size_too_small:{size:.2f}"
                    trace.append({"step": "size", "pass": False,
                                  "size": round(size, 2), "balance": round(self._balance, 2)})
                    return await _finalize_skip(
                        reason, token_id=token_id, p_hat=p_hat, mid=mid_price,
                    )

            # plan §7.1 / §7.2 / §7.3 — slippage, fee, share quantization.
            entry_fee = 0.0
            shares_held: Optional[float] = None
            effective_entry_price = mid_price
            if simulate_mode:
                # Market type drives the fee rate AND slippage opt-out.
                market_type = getattr(match, "market_type", None) or getattr(match, "_market_type", None)
                slip = sim.apply_slippage(
                    mid=mid_price,
                    notional_usdc=size,
                    side=result.direction.value,
                    market_type=market_type,
                    cfg=self._cfg.simulate_slippage,
                )
                effective_entry_price = slip.effective_price
                # Reject zero-liquidity rails (price after slippage is at the wall).
                if sim.is_at_rail(effective_entry_price):
                    trace.append({"step": "rail_zero_liquidity", "pass": False,
                                  "effective_price": round(effective_entry_price, 4)})
                    return await _finalize_skip(
                        "rail_zero_liquidity", token_id=token_id, p_hat=p_hat, mid=mid_price,
                    )
                # Estimate fee first using a naive share count, then quantize.
                naive_shares = size / effective_entry_price
                fee_est = sim.compute_fee(naive_shares, effective_entry_price,
                                          market_type, self._cfg.simulate_fees)
                shares_held = sim.quantize_shares(size, effective_entry_price, fee_est.fee)
                # Recompute fee using the actual share count for precision.
                fee_res = sim.compute_fee(shares_held, effective_entry_price,
                                          market_type, self._cfg.simulate_fees)
                entry_fee = fee_res.fee
                trace.append({"step": "fidelity",
                              "mid": round(mid_price, 4),
                              "slip_walk": round(slip.walk, 5),
                              "entry_price": round(effective_entry_price, 4),
                              "shares": round(shares_held, 4),
                              "entry_fee_usdc": round(entry_fee, 6),
                              "fee_rate": fee_res.rate,
                              "market_type": market_type or "(none)"})
                # Reject if the resulting stake leaves us under min_stake.
                if shares_held <= 0:
                    return await _finalize_skip(
                        "shares_zero_after_quantize", token_id=token_id, p_hat=p_hat, mid=mid_price,
                    )

            trace.append({"step": "size", "pass": True, "size_usdc": round(size, 2)})
            trace.append({"step": "executed"})

            # Execute — debit balance only in normal mode; either way track stake.
            if not self._cfg.infinite_balance:
                self._balance -= size
            self._total_staked += size
            trade_id = await self._db.insert_trade(
                signal_id=signal_id,
                condition_id=match.condition_id,
                question=match.question,
                token_id=token_id,
                direction=result.direction.value,
                similarity=match.similarity,
                p_hat=p_hat,
                mid_price=mid_price,
                size_usdc=size,
                skip_reason=None,
                headline_id=getattr(headline, "headline_id", None),
                confidence_raw=confidence_raw,
                p_hat_if_raw=p_hat_if_raw,
                signed_edge=signed_edge,
                signed_edge_if_raw=signed_edge_if_raw,
                engine_name=self._engine_name,
                agreement=getattr(result, "agreement", 0.0),
                p_alpha=getattr(result, "p_alpha", 0.0),
                risk_score=getattr(result, "risk_score", 0.0),
                ai_reason=getattr(result, "reason", None),
                ai_raw_response=getattr(result, "raw_response", None) or None,
                ai_prompt_meta=prompt_meta_json,
                trader_trace=json.dumps(trace, separators=(",", ":")),
                mid_yes=match.mid_yes,
                mid_no=match.mid_no,
                inverted=1 if inverted_flag else 0,
                variant_name=self._variant_name,
                freshness_label=getattr(headline, "_freshness_label", None),
                relevance_score=getattr(match, "_relevance_score", None),
                relevance_reason=getattr(match, "_relevance_reason", None),
                mkt_vol_30m=getattr(getattr(match, "_mkt_ctx", None), "vol", None),
                mkt_mean_30m=getattr(getattr(match, "_mkt_ctx", None), "mean", None),
                mkt_min_30m=getattr(getattr(match, "_mkt_ctx", None), "lo", None),
                mkt_max_30m=getattr(getattr(match, "_mkt_ctx", None), "hi", None),
                mkt_drift_30m=getattr(getattr(match, "_mkt_ctx", None), "drift", None),
                mkt_n_samples=getattr(getattr(match, "_mkt_ctx", None), "n_samples", None),
            )
            await self._db.open_position(
                trade_id=trade_id,
                condition_id=match.condition_id,
                question=match.question,
                token_id=token_id,
                direction=result.direction.value,
                size_usdc=size,
                # Simulate mode stamps the slipped fill price; paper/infinite
                # keep the historical mid_price (no behaviour change there).
                entry_price=effective_entry_price if simulate_mode else mid_price,
                p_hat=p_hat,
                engine_name=self._engine_name,
                shares=shares_held,
                entry_fee_usdc=(entry_fee if simulate_mode else None),
            )

        logger.info(
            "[trader] PAPER %s %s  $%.2f @ %.3f  edge=%.4f  sim=%.2f  conf=%.0f%%  %s",
            result.direction.value,
            match.condition_id[:12],
            size,
            mid_price,
            edge,
            match.similarity,
            result.confidence * 100,
            match.question[:70],
        )

        return TradeDecision(
            condition_id=match.condition_id,
            question=match.question,
            direction=result.direction,
            confidence=result.confidence,
            similarity=match.similarity,
            p_hat=p_hat,
            mid_price=mid_price,
            edge=edge,
            size_usdc=size,
            executed=True,
            skip_reason=None,
            confidence_raw=confidence_raw,
            p_hat_if_raw=p_hat_if_raw,
            signed_edge=signed_edge,
            signed_edge_if_raw=signed_edge_if_raw,
            agreement=getattr(result, "agreement", 0.0),
            p_alpha=getattr(result, "p_alpha", 0.0),
            risk_score=getattr(result, "risk_score", 0.0),
            mid_yes=match.mid_yes,
            mid_no=match.mid_no,
            inverted=inverted_flag,
        )

    async def _record(
        self,
        signal_id: int,
        decision: TradeDecision,
        token_id: Optional[str],
        *,
        headline: Optional[RawHeadline] = None,
        trace: Optional[list] = None,
        result: Optional[ClassifyResult] = None,
        prompt_meta_json: Optional[str] = None,
        inverted: bool = False,
        match=None,
    ) -> None:
        """Persist a skipped trade decision to the DB.

        Reads freshness label off `headline._freshness_label` and per-match
        relevance verdict off `match._relevance_score` / `_relevance_reason`
        when present (runner-level handoff; absent on legacy single-trader
        paths and that's fine).
        """
        ai_reason = getattr(result, "reason", None) if result is not None else None
        ai_raw = getattr(result, "raw_response", None) if result is not None else None
        freshness_label = (
            getattr(headline, "_freshness_label", None) if headline is not None else None
        )
        relevance_score = getattr(match, "_relevance_score", None) if match is not None else None
        relevance_reason = getattr(match, "_relevance_reason", None) if match is not None else None
        # Market context fields off the match (cycle 11). May be None
        # when the runner skipped the fetch or it failed; insert_trade
        # handles None for all six.
        mkt_ctx = getattr(match, "_mkt_ctx", None) if match is not None else None
        await self._db.insert_trade(
            signal_id=signal_id,
            condition_id=decision.condition_id,
            question=decision.question,
            token_id=token_id,
            direction=decision.direction.value,
            similarity=decision.similarity,
            p_hat=decision.p_hat,
            mid_price=decision.mid_price,
            size_usdc=0.0,
            skip_reason=decision.skip_reason,
            headline_id=getattr(headline, "headline_id", None) if headline is not None else None,
            confidence_raw=decision.confidence_raw,
            p_hat_if_raw=decision.p_hat_if_raw,
            signed_edge=decision.signed_edge,
            signed_edge_if_raw=decision.signed_edge_if_raw,
            engine_name=self._engine_name,
            agreement=decision.agreement,
            p_alpha=decision.p_alpha,
            risk_score=decision.risk_score,
            ai_reason=ai_reason,
            ai_raw_response=ai_raw or None,
            ai_prompt_meta=prompt_meta_json,
            trader_trace=json.dumps(trace, separators=(",", ":")) if trace else None,
            mid_yes=decision.mid_yes,
            mid_no=decision.mid_no,
            inverted=1 if inverted else 0,
            variant_name=self._variant_name,
            freshness_label=freshness_label,
            relevance_score=relevance_score,
            relevance_reason=relevance_reason,
            mkt_vol_30m=getattr(mkt_ctx, "vol", None),
            mkt_mean_30m=getattr(mkt_ctx, "mean", None),
            mkt_min_30m=getattr(mkt_ctx, "lo", None),
            mkt_max_30m=getattr(mkt_ctx, "hi", None),
            mkt_drift_30m=getattr(mkt_ctx, "drift", None),
            mkt_n_samples=getattr(mkt_ctx, "n_samples", None),
        )

    # ------------------------------------------------------------------
    # Position monitor
    # ------------------------------------------------------------------

    async def monitor_positions(self) -> None:
        """
        Check all open positions and close those that hit a stop condition.

        Price fetches run concurrently for all positions.
        Balance updates are applied under the lock.
        """
        # Proactively cull cold losing positions (losing + no recent headline match)
        session = self._get_session()
        cold = await self._db.get_cold_losing_positions(
            self._cfg.cold_position_hours, engine_name=self._engine_name
        )
        for pos in cold:
            price = await _fetch_mid_price(session, pos["token_id"])
            if price is None:
                continue
            persisted_shares = pos.get("shares")
            shares = persisted_shares if persisted_shares else (pos["size_usdc"] / pos["entry_price"])
            gross = (price - pos["entry_price"]) * shares
            simulate_mode = getattr(self._cfg, "mode", "paper") == "simulate"
            exit_fee = 0.0
            if simulate_mode:
                slip = sim.apply_slippage(
                    mid=price, notional_usdc=shares * price,
                    side="NO" if pos["direction"] == "YES" else "YES",
                    market_type=None, cfg=self._cfg.simulate_slippage,
                )
                exit_price = slip.effective_price
                fee_res = sim.compute_fee(shares, exit_price, None, self._cfg.simulate_fees)
                exit_fee = fee_res.fee
                proceeds = shares * exit_price - exit_fee
                pnl = proceeds - pos["size_usdc"]
            else:
                cost = pos["size_usdc"] * (self._cfg.spread_cost_bps / 10000.0)
                pnl = gross - cost
                proceeds = pos["size_usdc"] + pnl
            async with self._lock:
                if self._cfg.infinite_balance:
                    self._balance += pnl
                elif simulate_mode:
                    self._balance += proceeds
                else:
                    self._balance += pos["size_usdc"] + pnl
                await self._db.close_position(pos["trade_id"])
                await self._db.close_trade(
                    pos["trade_id"], pnl, close_reason="COLD_CULL",
                    exit_fee_usdc=(exit_fee if simulate_mode else None),
                )
            logger.info(
                "[trader] COLD_CULL %s %s  exit=%.3f  pnl=%.2f",
                pos["direction"], pos["condition_id"][:12], price, pnl,
            )

        positions = await self._db.get_open_positions(engine_name=self._engine_name)
        if not positions:
            return

        session = self._get_session()
        timeout_secs = self._cfg.position_timeout_hours * 3600
        now = datetime.now(timezone.utc)

        # Fetch all prices concurrently
        prices: list[Optional[float]] = await asyncio.gather(
            *[_fetch_mid_price(session, pos["token_id"]) for pos in positions]
        )

        for pos, current_price in zip(positions, prices):
            if current_price is None:
                continue

            trade_id    = pos["trade_id"]
            size_usdc   = pos["size_usdc"]
            entry_price = pos["entry_price"]
            direction   = pos["direction"]
            condition_id = pos["condition_id"]
            prev_hwm    = pos.get("high_water_price")

            opened_at = datetime.fromisoformat(pos["opened_at"])
            # Ensure timezone-aware for comparison
            if opened_at.tzinfo is None:
                opened_at = opened_at.replace(tzinfo=timezone.utc)
            age_secs = (now - opened_at).total_seconds()

            # Prefer the persisted share count (simulate mode); fall back to
            # the legacy implicit count for paper/infinite rows that pre-date
            # the shares column.
            persisted_shares = pos.get("shares")
            shares = persisted_shares if persisted_shares else (size_usdc / entry_price)
            unrealized_pnl = (current_price - entry_price) * shares

            # Update price + high-water mark in DB. The DB MAX() guarantees
            # monotonically-rising HWM even if multiple ticks race.
            await self._db.update_position_price(
                trade_id, current_price, unrealized_pnl,
                high_water_price=current_price,
            )
            # Local copy of the updated HWM for the trailing-stop check.
            local_hwm = max(current_price, prev_hwm if prev_hwm is not None else current_price)

            # MTM in probability points. current_price is the mid of the side
            # we entered (token_id is per-side), so (current - entry) is positive
            # when the position is in profit regardless of YES/NO direction.
            mtm     = current_price - entry_price
            hwm_mtm = local_hwm     - entry_price

            tp = self._cfg.take_profit
            # Price-relative SL floor (cycle-10): mid-price entries get a
            # wider absolute SL so spread alone can't trip them; low-price
            # entries keep the existing fixed floor. 0 disables (legacy).
            sl_pct_floor = self._cfg.stop_loss_pct * entry_price
            sl = max(self._cfg.stop_loss, sl_pct_floor)
            ts = self._cfg.trailing_stop
            min_hold = self._cfg.min_hold_seconds

            # Determine close trigger. Order: hard WIN/LOSS first, then
            # technical exits once min_hold has elapsed (trailing > TP > SL),
            # then absolute timeout as backstop.
            close_reason: Optional[str] = None
            min_hold_met = (min_hold <= 0) or (age_secs >= min_hold)
            if current_price >= _WIN_PRICE:
                close_reason = "WIN"
            elif current_price <= _LOSS_PRICE:
                close_reason = "LOSS"
            elif min_hold_met and ts > 0 and hwm_mtm >= ts and (hwm_mtm - mtm) >= ts and mtm >= 0:
                # E10: gate TRAIL on current price being at-or-above entry.
                # Phase A.6 saw TRAIL fire underwater (n=57, avg -$0.032) when
                # HWM had been high enough to arm the trail but price then
                # retreated below entry — locked in losses that often recovered.
                # Falls through to SL/TIMEOUT instead.
                close_reason = "TRAIL"
            elif min_hold_met and tp > 0 and mtm >= tp:
                close_reason = "TP"
            elif min_hold_met and sl > 0 and mtm <= -sl:
                # Note: SL is gated by min_hold_met (same as TP/TRAIL) to
                # prevent the first post-entry spread tick from instantly
                # triggering a loss before any headline has had a chance to
                # move the market.
                close_reason = "SL"
            elif age_secs >= timeout_secs:
                close_reason = "TIMEOUT"
            if close_reason is None:
                continue

            # plan §7 — simulate mode: model real Polymarket settlement +
            # exit fees + slippage. Paper/infinite use the legacy
            # spread_cost_bps round-trip drag.
            simulate_mode = getattr(self._cfg, "mode", "paper") == "simulate"
            exit_fee = 0.0
            if simulate_mode:
                # WIN/LOSS price-rails in simulate mode mean the market has
                # effectively resolved — settle at $1/$0 per share with no
                # exit fee (settlement is on-chain and free).
                if close_reason == "WIN":
                    close_reason = "RESOLVE_WIN"
                    exit_price = 1.00
                    proceeds = sim.settlement_payout(shares, win=True)
                elif close_reason == "LOSS":
                    close_reason = "RESOLVE_LOSS"
                    exit_price = 0.00
                    proceeds = sim.settlement_payout(shares, win=False)
                else:
                    # Early exit (TP/SL/TRAIL/TIMEOUT): walk the book + fee.
                    market_type = None  # no need to refetch; rate captured at entry
                    slip = sim.apply_slippage(
                        mid=current_price, notional_usdc=shares * current_price,
                        # selling our held side → the opposite slippage direction
                        side="NO" if direction == "YES" else "YES",
                        market_type=market_type, cfg=self._cfg.simulate_slippage,
                    )
                    exit_price = slip.effective_price
                    fee_res = sim.compute_fee(shares, exit_price, market_type,
                                              self._cfg.simulate_fees)
                    exit_fee = fee_res.fee
                    proceeds = shares * exit_price - exit_fee
                gross_pnl = (exit_price - entry_price) * shares
                # realized_pnl = (what we got back) - (what we put in)
                # what_we_put_in == size_usdc (already includes entry fee)
                realized_pnl = proceeds - size_usdc
            else:
                gross_pnl = (current_price - entry_price) * shares
                cost = size_usdc * (self._cfg.spread_cost_bps / 10000.0)
                realized_pnl = gross_pnl - cost
                proceeds = size_usdc + realized_pnl

            async with self._lock:
                if self._cfg.infinite_balance:
                    self._balance += realized_pnl
                elif simulate_mode:
                    # In simulate mode, credit the actual proceeds back to
                    # the wallet — entry fee was already debited at open.
                    self._balance += proceeds
                else:
                    self._balance += size_usdc + realized_pnl
                await self._db.close_position(trade_id)
                await self._db.close_trade(
                    trade_id, realized_pnl, close_reason=close_reason,
                    exit_fee_usdc=(exit_fee if simulate_mode else None),
                )

            sign = "+" if realized_pnl >= 0 else ""
            logger.info(
                "[trader] CLOSE %s %s @ %.3f  %s  PnL=%s%.2f  balance=%.2f",
                direction,
                condition_id[:12],
                current_price,
                close_reason,
                sign,
                realized_pnl,
                self._balance,
            )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def balance(self) -> float:
        """In infinite mode this is cumulative realized PnL; else live USDC."""
        return self._balance

    @property
    def total_staked(self) -> float:
        """Lifetime USDC put at risk by this engine."""
        return self._total_staked

    @property
    def infinite_balance(self) -> bool:
        return self._cfg.infinite_balance

    @property
    def cfg(self):
        """Expose the per-engine trading config (read-only)."""
        return self._cfg

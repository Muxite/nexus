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
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from ai import Direction, ClassifyResult, DirectionClassifier
from db import SeulgiDB
from markets import MarketMatch
from watcher import RawHeadline

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
    ) -> None:
        self._db = db
        self._classifier = classifier
        self._cfg = cfg
        self._ai_cfg = ai_cfg
        self._balance: float = cfg.balance_usdc
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None

    # ------------------------------------------------------------------
    # Init
    # ------------------------------------------------------------------

    async def init_from_db(self) -> None:
        """
        Restore balance from DB by subtracting committed capital.

        Call once after connect(). Prevents double-spending on restart.
        Safe to skip in tests where DB state is irrelevant.
        """
        committed = await self._db.get_committed_usdc()
        if committed > 0:
            self._balance = self._cfg.balance_usdc - committed
            logger.info(
                "[trader] restored: balance=%.2f (%.2f committed in %d positions)",
                self._balance,
                committed,
                await self._db.count_open_positions(),
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
        Evaluate a headline against all matching markets.

        AI classification runs concurrently for all matches (bounded by
        the classifier's internal semaphore).  Price fetches and balance
        accounting run sequentially under the asyncio lock.

        Returns one TradeDecision per market evaluated.
        """
        if not matches:
            return []

        signal_id = await self._db.insert_signal(
            site_url=headline.site_url,
            headline=headline.headline,
        )

        # Classify all matches concurrently
        classify_results: list[ClassifyResult] = await asyncio.gather(
            *[
                self._classifier.classify(
                    headline=headline.headline,
                    question=match.question,
                )
                for match in matches
            ]
        )

        session = self._get_session()
        decisions: list[TradeDecision] = []

        for match, result in zip(matches, classify_results):
            decision = await self._evaluate_market(
                session, signal_id, headline, match, result
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
    ) -> TradeDecision:
        """Evaluate one match and record the trade decision."""

        def _skip(reason: str, p_hat: float = 0.0, mid: Optional[float] = None) -> TradeDecision:
            return TradeDecision(
                condition_id=match.condition_id,
                question=match.question,
                direction=result.direction,
                confidence=result.confidence,
                similarity=match.similarity,
                p_hat=p_hat,
                mid_price=mid,
                edge=(p_hat - mid) if mid is not None else 0.0,
                size_usdc=0.0,
                executed=False,
                skip_reason=reason,
            )

        # ── AI filters ────────────────────────────────────────────────
        if result.direction == Direction.NEUTRAL:
            decision = _skip("neutral")
            await self._record(signal_id, decision, token_id=None)
            return decision

        if result.confidence < self._ai_cfg.min_confidence:
            decision = _skip(f"low_confidence:{result.confidence:.2f}")
            await self._record(signal_id, decision, token_id=None)
            return decision

        # ── Token selection ───────────────────────────────────────────
        if result.direction == Direction.YES:
            token_id = match.token_yes
        else:
            token_id = match.token_no

        if not token_id:
            decision = _skip("no_token")
            await self._record(signal_id, decision, token_id=None)
            return decision

        # ── Market price ──────────────────────────────────────────────
        mid_price = await _fetch_mid_price(session, token_id)
        if mid_price is None:
            decision = _skip("no_price")
            await self._record(signal_id, decision, token_id=token_id)
            return decision

        # ── Edge model ────────────────────────────────────────────────
        # edge = edge_increment * (2 * confidence - 1)
        # This makes confidence directly control edge size:
        #   conf=0.5 → edge=0 (no signal)
        #   conf=1.0 → edge=edge_increment (maximum certainty)
        confidence_scale = max(0.0, 2.0 * result.confidence - 1.0)
        edge = self._cfg.edge_increment * confidence_scale
        p_hat = mid_price + edge

        if edge < self._cfg.min_edge:
            decision = _skip(f"edge_too_small:{edge:.4f}", p_hat=p_hat, mid=mid_price)
            await self._record(signal_id, decision, token_id=token_id)
            return decision

        # ── Balance / position guards (under lock) ────────────────────
        async with self._lock:
            # Check max open positions
            open_count = await self._db.count_open_positions()
            if open_count >= self._cfg.max_open_positions:
                decision = _skip(f"max_positions:{open_count}", p_hat=p_hat, mid=mid_price)
                await self._record(signal_id, decision, token_id=token_id)
                return decision

            # No doubling-up on the same market
            if await self._db.has_open_position(match.condition_id):
                decision = _skip("duplicate_position", p_hat=p_hat, mid=mid_price)
                await self._record(signal_id, decision, token_id=token_id)
                return decision

            # Compute actual trade size
            size = min(self._cfg.max_position_usdc, self._balance)
            if size < self._cfg.min_trade_usdc:
                reason = "no_balance" if self._balance <= 0 else f"size_too_small:{size:.2f}"
                decision = _skip(reason, p_hat=p_hat, mid=mid_price)
                await self._record(signal_id, decision, token_id=token_id)
                return decision

            # Execute — deduct balance before any await
            self._balance -= size
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
            )
            await self._db.open_position(
                trade_id=trade_id,
                condition_id=match.condition_id,
                question=match.question,
                token_id=token_id,
                direction=result.direction.value,
                size_usdc=size,
                entry_price=mid_price,
                p_hat=p_hat,
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
        )

    async def _record(
        self,
        signal_id: int,
        decision: TradeDecision,
        token_id: Optional[str],
    ) -> None:
        """Persist a skipped trade decision to the DB."""
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
        positions = await self._db.get_open_positions()
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

            opened_at = datetime.fromisoformat(pos["opened_at"])
            # Ensure timezone-aware for comparison
            if opened_at.tzinfo is None:
                opened_at = opened_at.replace(tzinfo=timezone.utc)
            age_secs = (now - opened_at).total_seconds()

            shares = size_usdc / entry_price
            unrealized_pnl = (current_price - entry_price) * shares
            await self._db.update_position_price(trade_id, current_price, unrealized_pnl)

            # Determine close trigger
            if current_price >= _WIN_PRICE:
                close_reason = "WIN"
            elif current_price <= _LOSS_PRICE:
                close_reason = "LOSS"
            elif age_secs >= timeout_secs:
                close_reason = "TIMEOUT"
            else:
                continue

            realized_pnl = (current_price - entry_price) * shares

            async with self._lock:
                self._balance += size_usdc + realized_pnl
                await self._db.close_position(trade_id)
                await self._db.close_trade(trade_id, realized_pnl)

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
        return self._balance

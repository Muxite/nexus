"""CoreStrategy — orchestrates guard cascade → trader.place_order, and
monitor_positions → exit policy → trader.close_order.

Duck-types the legacy PaperTrader's surface (`evaluate`,
`monitor_positions`, `balance` property) so the legacy EngineRunner can
treat it as just another Engine.trader. Internally it is built on the
Trader ABC, so a future LiveTrader swap requires no changes here.

Public attribute `balance` is delegated to `trader.cash` in paper mode;
external dashboards reading it stay backward-compatible.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import aiohttp

try:
    from db import SeulgiDB
except ImportError:
    from app.db import SeulgiDB  # type: ignore[no-redef]

from .config import CoreConfig
from .exits import ExitPolicy
from .guards import (
    DEFAULT_GUARDS,
    GuardContext,
    run_cascade,
)
from .observability import EventBus
from .reconcile import Reconciler
from .trader_base import OrderRequest, Trader

logger = logging.getLogger(__name__)


async def _fetch_mid(session: aiohttp.ClientSession, token_id: str) -> Optional[float]:
    """Lightweight mid-price fetcher (matches legacy semantics)."""
    if not token_id:
        return None
    url = f"https://clob.polymarket.com/midpoint?token_id={token_id}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status != 200:
                return None
            j = await r.json()
            mid = j.get("mid")
            return float(mid) if mid is not None else None
    except Exception:
        return None


class CoreStrategy:
    """Engine.trader-shaped façade backed by a Trader ABC implementation."""

    def __init__(
        self,
        *,
        cfg: CoreConfig,
        db: SeulgiDB,
        trader: Trader,
        bus: EventBus,
        guards: Optional[list] = None,
    ) -> None:
        self._cfg = cfg
        self._db = db
        self._trader = trader
        self._bus = bus
        self._guards = guards or DEFAULT_GUARDS
        self._exits = ExitPolicy(cfg)
        self._reconciler = Reconciler(trader=trader, bus=bus)
        self._recent_entry_ts: list[float] = []
        self._session: Optional[aiohttp.ClientSession] = None
        # Hooks updated externally by the runner (or left as zero defaults)
        self.daily_ai_spend_usd: float = 0.0
        self.watcher_degraded_fraction: float = 0.0
        # Legacy compatibility — EngineRunner reads these
        self._engine_name = cfg.engine_name
        self._total_staked: float = 0.0

    # ------------------------------------------------------------------
    # Compatibility surface for the legacy EngineRunner
    # ------------------------------------------------------------------

    async def init_from_db(self) -> None:
        """Legacy EngineRunner hook — restore paper cash from DB so a restart
        doesn't double-spend on open positions."""
        restore = getattr(self._trader, "restore_balance_from_db", None)
        if restore is not None:
            await restore()
        # Also refresh total_staked from open commitments
        try:
            self._total_staked = await self._db.get_committed_usdc(
                engine_name=self._cfg.engine_name,
            )
        except Exception:
            self._total_staked = 0.0

    @property
    def balance(self) -> float:
        return getattr(self._trader, "cash", 0.0)

    @property
    def total_staked(self) -> float:
        return self._total_staked

    # ------------------------------------------------------------------
    # Entry
    # ------------------------------------------------------------------

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def evaluate(
        self,
        signal_id: int,
        headline,
        survivors: list,
        classify_results: list,
        prompt_meta_json: Optional[str] = None,
        pre_ai_skips: Optional[list] = None,
        relevance_skips: Optional[list] = None,
    ):
        """Mirrors PaperTrader.evaluate signature. We ignore pre_ai_skips /
        relevance_skips for entry purposes (the legacy trader writes skip
        rows there for analytics; the carved module's observability stream
        captures the same information without DB writes per-variant)."""
        if not survivors:
            return []
        bal = await self._trader.verify_balance()
        for match, result in zip(survivors, classify_results):
            await self._consider(signal_id, headline, match, result, bal)
        return []

    async def _consider(self, signal_id, headline, match, result, bal):
        cfg = self._cfg
        # Direction & price selection — uses YES side by default, flips token if NO.
        direction = getattr(result, "direction", None)
        direction_str = direction.value if hasattr(direction, "value") else str(direction)
        confidence = float(getattr(result, "confidence", 0.0))
        # invert_high_conf: shadow rule that flips direction when AI confidence
        # exceeds threshold (originated as the inverted_highconf engine — the
        # cycle-6 finding that high-conf calls were anti-informative).
        if cfg.invert_high_conf and confidence >= cfg.invert_conf_threshold:
            if direction_str == "YES":
                direction_str = "NO"
            elif direction_str == "NO":
                direction_str = "YES"
        token_id = match.token_yes if direction_str == "YES" else match.token_no
        quoted_price = match.mid_yes if direction_str == "YES" else match.mid_no
        if not token_id or quoted_price is None:
            return
        # _hi65 / _hi75 variants: only enter when YES-side mid clears a floor
        # (biases toward the 0.65–0.90 entry bucket which was the only
        # consistently-profitable slice in the cycle-9 win/loss report).
        if cfg.min_entry_yes_mid > 0 and (match.mid_yes or 0.0) < cfg.min_entry_yes_mid:
            return

        signed_edge = float(getattr(result, "p_alpha", 0.0)) - float(quoted_price)
        # Aggressive parity: use signed_edge magnitude
        signed_edge = abs(signed_edge)

        # Check market_closed once (cheap query)
        async with self._db._conn.execute(   # type: ignore[attr-defined]
            "SELECT closed_at FROM markets WHERE condition_id = ?",
            (match.condition_id,),
        ) as cur:
            row = await cur.fetchone()
        market_closed = bool(row and row[0])

        ctx = GuardContext(
            cfg=cfg, db=self._db, bus=self._bus,
            cash=bal.cash_usdc,
            open_positions=bal.open_positions,
            unique_markets=bal.open_positions,   # 1:1 with positions in paper
            committed_usdc=bal.committed_usdc,
            recent_entry_ts=self._recent_entry_ts,
            condition_id=match.condition_id,
            direction=direction_str,
            confidence=float(getattr(result, "confidence", 0.0)),
            agreement=float(getattr(result, "agreement", 1.0)),
            quoted_price=float(quoted_price),
            quoted_spread_bps=0.0,                 # unknown in paper without book read
            signed_edge=signed_edge,
            mkt_vol_30m=getattr(getattr(match, "_mkt_ctx", None), "vol", None),
            market_closed=market_closed,
            daily_ai_spend_usd=self.daily_ai_spend_usd,
            watcher_degraded_fraction=self.watcher_degraded_fraction,
        )
        guard_result = await run_cascade(self._guards, ctx)
        if not guard_result.passed:
            return

        # Size: full max_position_usdc unless cash limits us
        size = min(cfg.max_position_usdc, bal.cash_usdc)
        if size < cfg.min_trade_usdc:
            return

        req = OrderRequest(
            condition_id=match.condition_id,
            question=match.question,
            token_id=token_id,
            direction=direction_str,
            size_usdc=size,
            quoted_price=float(quoted_price),
            signal_id=signal_id,
            headline_id=getattr(headline, "headline_id", None),
            p_hat=float(getattr(result, "p_alpha", 0.0)),
            confidence_raw=float(getattr(result, "confidence", 0.0)),
            similarity=float(getattr(match, "similarity", 0.0)),
            signed_edge=signed_edge,
            ai_reason=getattr(result, "reason", "")[:500],
            ai_raw_response=getattr(result, "raw_response", "")[:2000],
            agreement=float(getattr(result, "agreement", 1.0)),
            p_alpha=float(getattr(result, "p_alpha", 0.0)),
            risk_score=float(getattr(result, "risk_score", 0.0)),
            mid_yes=match.mid_yes,
            mid_no=match.mid_no,
            freshness_label=getattr(headline, "_freshness_label", None),
            trader_trace=list(ctx.trace),
        )
        res = await self._trader.place_order(req)
        if res.filled:
            self._recent_entry_ts.append(time.monotonic())
            self._total_staked += res.filled_size_usdc or 0.0

    # ------------------------------------------------------------------
    # Monitor
    # ------------------------------------------------------------------

    async def monitor_positions(self) -> None:
        positions = await self._db.get_open_positions(engine_name=self._cfg.engine_name)
        if not positions:
            return
        session = self._get_session()
        prices = await asyncio.gather(
            *[_fetch_mid(session, p["token_id"]) for p in positions]
        )
        for pos, price in zip(positions, prices):
            if price is None:
                continue
            shares = pos["size_usdc"] / max(pos["entry_price"], 1e-6)
            unrealized = (price - pos["entry_price"]) * shares
            await self._db.update_position_price(
                pos["trade_id"], price, unrealized, high_water_price=price,
            )
            decision = self._exits.decide(
                entry_price=pos["entry_price"], current_price=price,
                high_water_price=pos.get("high_water_price"),
                opened_at_iso=pos["opened_at"],
            )
            if not decision.close:
                continue
            await self._trader.close_order(
                trade_id=pos["trade_id"],
                condition_id=pos["condition_id"],
                token_id=pos["token_id"],
                direction=pos["direction"],
                size_usdc=pos["size_usdc"],
                entry_price=pos["entry_price"],
                current_price=price,
                close_reason=decision.close_reason,
            )

        # Periodic reconcile (cheap in paper mode)
        await self._reconciler.check(engine_name=self._cfg.engine_name)

    # ------------------------------------------------------------------
    # No-op shims for methods EngineRunner may call (legacy parity)
    # ------------------------------------------------------------------

    async def evaluate_low_sim_skips(self, *args, **kwargs):
        return []

    async def evaluate_freshness_skips(self, *args, **kwargs):
        return []

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        await self._trader.close()

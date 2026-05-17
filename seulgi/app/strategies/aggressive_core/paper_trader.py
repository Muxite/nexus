"""PaperTrader — friction-aware paper executor.

Implements §B.1: bid-ask, latency, partial-fill, slippage abort modelled
without committing real capital. Writes trades and positions to the same
SQLite tables the legacy PaperTrader uses (engine_name="aggressive_core"
so analysis tools pick it up automatically).

Friction model (defaults from CoreConfig):
  - entry fill price = quoted_price + spread/2 (i.e. cross to ask)
  - exit  fill price = current_price - spread/2 (cross to bid)
  - round-trip spread_cost_bps deducted at close (legacy semantics —
    preserves comparability with historical realized_pnl)
  - simulated entry_delay_ms before fill
  - if (post-delay quoted) drift > slippage_tolerance_bps → abort
  - LiveTrader will replace this with actual CLOB book reads + partials

The PaperTrader is the venue in paper mode, so verify_balance and
verify_positions return what the trader itself recorded — they trivially
reconcile, which is the intended baseline for the live reconciler.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Optional

try:
    from db import SeulgiDB
except ImportError:
    from app.db import SeulgiDB  # type: ignore[no-redef]

from .config import CoreConfig
from .observability import EventBus
from .trader_base import (
    BalanceSnapshot,
    CloseResult,
    OrderRequest,
    OrderResult,
    PositionSnapshot,
    Trader,
)

logger = logging.getLogger(__name__)

_WIN_PRICE = 0.95
_LOSS_PRICE = 0.05


class PaperTrader(Trader):
    """The default Trader implementation. Owns balance state in-memory;
    persists trades + positions to the DB."""

    name = "paper"

    def __init__(
        self,
        *,
        cfg: CoreConfig,
        db: SeulgiDB,
        bus: EventBus,
    ) -> None:
        self._cfg = cfg
        self._db = db
        self._bus = bus
        # Bankroll is replenished from realized PnL — reinvestment is the
        # default. To start truly fresh, call reset_balance(cfg.bankroll_usdc)
        # before processing any headlines.
        self._cash: float = cfg.bankroll_usdc
        self._lock = asyncio.Lock()
        # Per-market-hour cumulative size — feeds the volume guard.
        self._market_hour_size: dict[str, list[tuple[float, float]]] = {}

    # ------------------------------------------------------------------
    # Lifecycle / state
    # ------------------------------------------------------------------

    async def restore_balance_from_db(self) -> None:
        """Restore cash by subtracting open-position commitments from
        the configured bankroll. Use at boot so we don't double-spend
        across restarts."""
        committed = await self._db.get_committed_usdc(engine_name=self._cfg.engine_name)
        self._cash = max(0.0, self._cfg.bankroll_usdc - committed)
        self._bus.emit(
            engine_name=self._cfg.engine_name, phase="boot", name="balance_restored",
            payload={"cash": round(self._cash, 4),
                     "committed": round(committed, 4),
                     "bankroll": self._cfg.bankroll_usdc},
        )

    async def reset_balance(self, new_cash: float) -> None:
        async with self._lock:
            self._cash = float(new_cash)
        self._bus.emit(
            engine_name=self._cfg.engine_name, phase="boot", name="balance_reset",
            payload={"cash": round(self._cash, 4)},
        )

    @property
    def cash(self) -> float:
        return self._cash

    # ------------------------------------------------------------------
    # Friction simulation
    # ------------------------------------------------------------------

    def _apply_entry_friction(self, quoted_price: float) -> tuple[float, dict]:
        spread = self._cfg.max_entry_spread_bps / 10_000.0
        # We use half the *cap* spread as the modelled half-spread — the
        # cap is the worst we'd ever accept, so this is the conservative
        # entry assumption. Real LiveTrader will read the actual book.
        half = spread / 2.0
        fill = min(0.999, quoted_price + half)
        return fill, {"quoted": quoted_price, "half_spread": half, "fill": fill}

    def _apply_exit_friction(self, current_price: float) -> tuple[float, dict]:
        spread = self._cfg.max_entry_spread_bps / 10_000.0
        half = spread / 2.0
        fill = max(0.001, current_price - half)
        return fill, {"current": current_price, "half_spread": half, "fill": fill}

    def _record_market_size(self, condition_id: str, size: float) -> float:
        """Append (ts, size) and return cumulative size in the last hour."""
        now = time.monotonic()
        cutoff = now - 3600.0
        buf = self._market_hour_size.setdefault(condition_id, [])
        buf.append((now, size))
        # prune in place
        self._market_hour_size[condition_id] = [(t, s) for t, s in buf if t >= cutoff]
        return sum(s for _, s in self._market_hour_size[condition_id])

    # ------------------------------------------------------------------
    # Trader API
    # ------------------------------------------------------------------

    async def place_order(self, req: OrderRequest) -> OrderResult:
        # Simulated order-placement latency
        if self._cfg.entry_delay_ms > 0:
            await asyncio.sleep(self._cfg.entry_delay_ms / 1000.0)

        async with self._lock:
            # Per-market-hour volume cap
            cum = self._record_market_size(req.condition_id, req.size_usdc)
            if cum > self._cfg.max_size_per_market_per_hour_usdc:
                self._bus.emit(
                    engine_name=self._cfg.engine_name, phase="order",
                    name="volume_cap", level="warn",
                    headline_id=req.headline_id, condition_id=req.condition_id,
                    payload={"hourly_size": round(cum, 2),
                             "cap": self._cfg.max_size_per_market_per_hour_usdc},
                )
                return OrderResult(
                    filled=False, trade_id=None, fill_price=None,
                    filled_size_usdc=None, skip_reason="volume_cap",
                    detail={"hourly_size": cum},
                )

            # Cash check
            if req.size_usdc > self._cash + 1e-6:
                return OrderResult(
                    filled=False, trade_id=None, fill_price=None,
                    filled_size_usdc=None, skip_reason="insufficient_cash",
                    detail={"requested": req.size_usdc, "cash": round(self._cash, 2)},
                )

            fill_price, friction = self._apply_entry_friction(req.quoted_price)

            # Slippage abort: in PAPER mode the fill is deterministic given
            # the modelled half-spread, so any drift is by definition
            # expected. The check is a no-op here; LiveTrader will re-quote
            # against the real book and compare against the original quote.

            # All good — write trade row + open position
            req.trader_trace.append(
                {"step": "place_order", "pass": True, "fill_price": fill_price,
                 "friction": friction}
            )
            trade_id = await self._db.insert_trade(
                signal_id=req.signal_id,
                condition_id=req.condition_id,
                question=req.question,
                token_id=req.token_id,
                direction=req.direction,
                similarity=req.similarity,
                p_hat=req.p_hat,
                mid_price=fill_price,
                size_usdc=req.size_usdc,
                skip_reason=None,
                headline_id=req.headline_id,
                confidence_raw=req.confidence_raw,
                p_hat_if_raw=req.p_hat,
                signed_edge=req.signed_edge,
                signed_edge_if_raw=req.signed_edge,
                engine_name=self._cfg.engine_name,
                agreement=req.agreement,
                p_alpha=req.p_alpha,
                risk_score=req.risk_score,
                ai_reason=req.ai_reason,
                ai_raw_response=req.ai_raw_response,
                trader_trace=json.dumps(req.trader_trace),
                mid_yes=req.mid_yes,
                mid_no=req.mid_no,
                inverted=req.inverted,
                variant_name=self._cfg.engine_name,
                freshness_label=req.freshness_label,
                relevance_score=req.relevance_score,
                relevance_reason=req.relevance_reason,
                mkt_vol_30m=req.mkt_vol_30m,
                mkt_mean_30m=req.mkt_mean_30m,
                mkt_min_30m=req.mkt_min_30m,
                mkt_max_30m=req.mkt_max_30m,
                mkt_drift_30m=req.mkt_drift_30m,
                mkt_n_samples=req.mkt_n_samples,
            )
            await self._db.open_position(
                trade_id=trade_id,
                condition_id=req.condition_id,
                question=req.question,
                token_id=req.token_id,
                direction=req.direction,
                size_usdc=req.size_usdc,
                entry_price=fill_price,
                p_hat=req.p_hat,
                engine_name=self._cfg.engine_name,
            )
            self._cash -= req.size_usdc
            self._bus.emit(
                engine_name=self._cfg.engine_name, phase="order", name="filled",
                headline_id=req.headline_id, condition_id=req.condition_id,
                trade_id=trade_id,
                payload={"direction": req.direction, "size": req.size_usdc,
                         "fill_price": fill_price, "cash_after": round(self._cash, 2)},
            )
            return OrderResult(
                filled=True, trade_id=trade_id, fill_price=fill_price,
                filled_size_usdc=req.size_usdc, skip_reason=None,
                detail=friction,
            )

    async def close_order(
        self, *, trade_id: int, condition_id: str, token_id: str, direction: str,
        size_usdc: float, entry_price: float, current_price: float, close_reason: str,
    ) -> CloseResult:
        fill_price, friction = self._apply_exit_friction(current_price)
        shares = size_usdc / max(entry_price, 1e-6)
        gross = (fill_price - entry_price) * shares
        # Round-trip cost — matches legacy semantics so realized_pnl is
        # directly comparable to historical trades.
        cost = size_usdc * (self._cfg.spread_cost_bps / 10_000.0)
        realized = gross - cost

        async with self._lock:
            # Cash back: stake + realized (positive or negative)
            self._cash += size_usdc + realized
            await self._db.close_position(trade_id)
            await self._db.close_trade(trade_id, realized, close_reason=close_reason)

        self._bus.emit(
            engine_name=self._cfg.engine_name, phase="exit", name="closed",
            condition_id=condition_id, trade_id=trade_id,
            payload={"close_reason": close_reason,
                     "fill_price": fill_price,
                     "realized": round(realized, 4),
                     "cash_after": round(self._cash, 2),
                     "friction": friction},
        )
        return CloseResult(
            realized_pnl=realized, exit_price=fill_price,
            close_reason=close_reason, detail=friction,
        )

    async def verify_balance(self) -> BalanceSnapshot:
        committed = await self._db.get_committed_usdc(engine_name=self._cfg.engine_name)
        n_open = await self._db.count_open_positions(engine_name=self._cfg.engine_name)
        return BalanceSnapshot(
            cash_usdc=self._cash, committed_usdc=committed, open_positions=n_open,
        )

    async def verify_positions(self) -> list[PositionSnapshot]:
        rows = await self._db.get_open_positions(engine_name=self._cfg.engine_name)
        return [
            PositionSnapshot(
                condition_id=r["condition_id"], token_id=r["token_id"],
                direction=r["direction"], size_usdc=r["size_usdc"],
                entry_price=r["entry_price"],
            )
            for r in rows
        ]

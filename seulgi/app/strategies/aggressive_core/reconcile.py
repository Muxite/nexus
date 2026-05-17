"""Reconciliation hooks.

In paper mode the venue *is* the local DB, so verify_balance and
verify_positions trivially agree with their own writes — reconcile()
returns OK. The hook is wired in regardless so the LiveTrader has a
defined integration point.

When LiveTrader lands, the implementation will:
  - compare on-chain USDC balance to PaperTrader._cash
  - compare on-chain positions to db.get_open_positions
  - on divergence beyond `tolerance_usdc`, halt entries + alert
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from .observability import EventBus
from .trader_base import Trader

logger = logging.getLogger(__name__)


@dataclass
class ReconcileResult:
    ok: bool
    detail: dict


class Reconciler:
    def __init__(self, *, trader: Trader, bus: EventBus, tolerance_usdc: float = 0.50) -> None:
        self._trader = trader
        self._bus = bus
        self._tol = tolerance_usdc

    async def check(self, *, engine_name: str) -> ReconcileResult:
        try:
            bal = await self._trader.verify_balance()
            pos = await self._trader.verify_positions()
        except NotImplementedError as exc:
            # LiveTrader stub path: treat as failure so a live boot can't
            # silently proceed with unverifiable state.
            self._bus.emit(
                engine_name=engine_name, phase="alert", name="reconcile_unavailable",
                level="error", payload={"error": str(exc)},
            )
            return ReconcileResult(ok=False, detail={"error": str(exc)})

        detail = {
            "cash": round(bal.cash_usdc, 2),
            "committed": round(bal.committed_usdc, 2),
            "open_positions_balance": bal.open_positions,
            "open_positions_venue": len(pos),
        }
        # Paper mode invariant: counts must match (we write both sides).
        if bal.open_positions != len(pos):
            self._bus.emit(
                engine_name=engine_name, phase="alert", name="reconcile_divergence",
                level="error", payload=detail,
            )
            return ReconcileResult(ok=False, detail=detail)
        return ReconcileResult(ok=True, detail=detail)

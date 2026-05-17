"""LiveTrader — real-money CLOB executor (NOT YET IMPLEMENTED).

This file is the contract for the future live implementation. When
SEULGI_LIVE=1 the bootstrap picks LiveTrader instead of PaperTrader and
passes it to CoreStrategy. Until the methods below are implemented,
every call raises NotImplementedError on purpose so a misconfigured
boot fails loudly rather than silently paper-trading.

What still needs to be built (separate PR):
  1. Polymarket CLOB REST + WebSocket client (signing, EIP-712 orders)
  2. Real book reader replacing _apply_*_friction in PaperTrader
  3. Order-status polling + cancel/replace
  4. Balance fetcher (USDC on-chain + open-position notionals)
  5. Position fetcher (on-chain positions vs local DB diff)
  6. Pre-trade gas estimate guard (refuse if gas > size_usdc * X%)
"""
from __future__ import annotations

import logging

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


class LiveTrader(Trader):
    name = "live"

    def __init__(self, *, bus: EventBus, cfg) -> None:
        self._bus = bus
        self._cfg = cfg
        # Audible bootstrap warning — if anything constructs this trader by
        # accident, it will show up in dashboards/alerts immediately.
        logger.warning(
            "LiveTrader constructed but not implemented — "
            "any place_order call will raise NotImplementedError."
        )
        self._bus.emit(
            engine_name=cfg.engine_name, phase="boot", name="live_trader_stub",
            level="warn",
            payload={"note": "LiveTrader is a stub — order placement will raise."},
        )

    async def place_order(self, req: OrderRequest) -> OrderResult:
        raise NotImplementedError(
            "LiveTrader.place_order is not implemented. Set SEULGI_LIVE=0 or "
            "use PaperTrader until the live executor lands."
        )

    async def close_order(self, **_kwargs) -> CloseResult:
        raise NotImplementedError("LiveTrader.close_order is not implemented.")

    async def verify_balance(self) -> BalanceSnapshot:
        raise NotImplementedError("LiveTrader.verify_balance is not implemented.")

    async def verify_positions(self) -> list[PositionSnapshot]:
        raise NotImplementedError("LiveTrader.verify_positions is not implemented.")

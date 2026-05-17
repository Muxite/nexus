"""Trader ABC — the OOP boundary between strategy and execution.

CoreStrategy talks ONLY to this interface. PaperTrader and (future)
LiveTrader implement it. Swapping one for the other is the entire
paper-vs-live switch — no `if mode == "live":` branches anywhere else.

Design rules:
 - Every method is async (matches the rest of the codebase).
 - place_order returns OrderResult, which includes whether the fill
   actually happened. Strategy must respect `OrderResult.filled` —
   rejected orders are normal in live trading (thin book, slippage).
 - close_order returns CloseResult with realized_pnl already net of
   fees + spread.
 - verify_balance / verify_positions return what the venue thinks,
   not what the local DB thinks. In paper mode the venue *is* the
   local DB, so they trivially agree. In live mode they're the diff
   detection point.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Order-side dataclasses
# ---------------------------------------------------------------------------

@dataclass
class OrderRequest:
    """What the strategy hands to the trader. Prices are at decision time;
    the trader is expected to re-fetch the book and either fill at the
    then-current ask (capped by `slippage_tolerance_bps`) or refuse."""
    condition_id: str
    question: str
    token_id: str
    direction: str                    # "YES" | "NO"
    size_usdc: float
    quoted_price: float               # mid at decision time (for slippage check)
    # Provenance — copied straight through to insert_trade so DB rows stay
    # comparable to legacy variants.
    signal_id: int
    headline_id: Optional[str] = None
    p_hat: float = 0.0
    confidence_raw: float = 0.0
    similarity: float = 0.0
    signed_edge: float = 0.0
    ai_reason: str = ""
    ai_raw_response: str = ""
    agreement: float = 0.0
    p_alpha: float = 0.0
    risk_score: float = 0.0
    mid_yes: Optional[float] = None
    mid_no: Optional[float] = None
    freshness_label: Optional[str] = None
    relevance_score: Optional[str] = None
    relevance_reason: Optional[str] = None
    mkt_vol_30m: Optional[float] = None
    mkt_mean_30m: Optional[float] = None
    mkt_min_30m: Optional[float] = None
    mkt_max_30m: Optional[float] = None
    mkt_drift_30m: Optional[float] = None
    mkt_n_samples: Optional[int] = None
    trader_trace: list = field(default_factory=list)
    inverted: int = 0


@dataclass
class OrderResult:
    """What the trader hands back. `filled=False` means the order was
    rejected at the venue — strategy should log and move on, not retry."""
    filled: bool
    trade_id: Optional[int]           # DB row id when filled, else None
    fill_price: Optional[float]       # actual fill (paper: post-friction; live: real)
    filled_size_usdc: Optional[float] # may be < requested if partial
    skip_reason: Optional[str]        # populated when filled=False
    detail: dict = field(default_factory=dict)


@dataclass
class CloseResult:
    realized_pnl: float
    exit_price: float
    close_reason: str
    detail: dict = field(default_factory=dict)


@dataclass
class BalanceSnapshot:
    """What `verify_balance` returns. `cash` is free; `committed` is in
    open positions. In live mode these come from the venue; in paper mode
    from the in-memory state of the PaperTrader."""
    cash_usdc: float
    committed_usdc: float
    open_positions: int

    @property
    def total_usdc(self) -> float:
        return self.cash_usdc + self.committed_usdc


@dataclass
class PositionSnapshot:
    """Per-position view as the venue sees it. Compared against DB rows
    in reconcile.py."""
    condition_id: str
    token_id: str
    direction: str
    size_usdc: float
    entry_price: float


# ---------------------------------------------------------------------------
# Trader ABC
# ---------------------------------------------------------------------------

class Trader(ABC):
    """Strategy-facing executor. Implementations: PaperTrader, LiveTrader."""

    name: str = "trader"

    @abstractmethod
    async def place_order(self, req: OrderRequest) -> OrderResult:
        """Quote → fill → record. Returns OrderResult.filled=False on any
        legitimate refusal (thin book, spread too wide, slippage abort)."""

    @abstractmethod
    async def close_order(
        self,
        *,
        trade_id: int,
        condition_id: str,
        token_id: str,
        direction: str,
        size_usdc: float,
        entry_price: float,
        current_price: float,
        close_reason: str,
    ) -> CloseResult:
        """Close an open position. `current_price` is the strategy's view
        of mid at decision; trader may diverge based on book/bid."""

    @abstractmethod
    async def verify_balance(self) -> BalanceSnapshot:
        """Read what the venue thinks our balance is. Used by reconcile."""

    @abstractmethod
    async def verify_positions(self) -> list[PositionSnapshot]:
        """Read what the venue thinks our open positions are."""

    async def close(self) -> None:
        """Optional cleanup hook. Default no-op."""
        return None

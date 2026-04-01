"""Shared Pydantic models used across all Irene services."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class TradeMode(str, Enum):
    PAPER = "paper"
    LIVE = "live"


class Direction(str, Enum):
    YES = "YES"
    NO = "NO"
    NEUTRAL = "NEUTRAL"


class SignalSource(str, Enum):
    NEWSAPI = "newsapi"
    RSS = "rss"


class OrderType(str, Enum):
    FOK = "FOK"
    FAK = "FAK"


class ExitReason(str, Enum):
    RESOLVED = "resolved"
    TIMEOUT = "timeout"
    MANUAL = "manual"


class Outcome(str, Enum):
    WIN = "WIN"
    LOSS = "LOSS"
    PUSH = "PUSH"


class MarketEventType(str, Enum):
    SIGNAL_MATCH = "signal_match"
    TRADE_DECIDED = "trade_decided"
    ORDER_FILLED = "order_filled"
    POSITION_CLOSED = "position_closed"
    MARKET_RESOLVED = "market_resolved"
    SCANNER_REFRESH = "scanner_refresh"


# ---------------------------------------------------------------------------
# Market Index
# ---------------------------------------------------------------------------


class MarketRecord(BaseModel):
    """A Polymarket market as stored in the market index."""

    condition_id: str
    slug: Optional[str] = None
    question: str
    description: Optional[str] = None
    category: Optional[str] = None
    end_date: datetime
    volume_usdc: Optional[float] = None
    token_yes: Optional[str] = None
    token_no: Optional[str] = None
    active: bool = True
    first_seen_at: datetime
    updated_at: datetime

    # embedding is not included here — it's stored as raw bytes in the DB
    # and managed directly by the scanner/feeder via numpy


# ---------------------------------------------------------------------------
# Signals
# ---------------------------------------------------------------------------


class RawSignal(BaseModel):
    """A news article or RSS item before market mapping."""

    source: SignalSource
    source_detail: str  # e.g. "newsapi" or RSS feed URL
    url: str
    title: str
    description: Optional[str] = None
    published_at: Optional[datetime] = None
    ingested_at: datetime


class MappedSignal(BaseModel):
    """A signal matched to a specific market — published on Redis irene:signals."""

    signal_id: int
    condition_id: str
    token_yes: str
    token_no: str
    question: str
    similarity_score: float = Field(ge=0.0, le=1.0)
    direction: Direction
    direction_confidence: float = Field(ge=0.0, le=1.0)
    signal_title: str
    signal_source: str
    signal_published_at: Optional[datetime] = None
    mapped_at: datetime


# ---------------------------------------------------------------------------
# Trading
# ---------------------------------------------------------------------------


class Orderbook(BaseModel):
    """Minimal CLOB orderbook snapshot."""

    token_id: str
    best_bid: float = Field(ge=0.0, le=1.0)
    best_ask: float = Field(ge=0.0, le=1.0)
    mid_price: float = Field(ge=0.0, le=1.0)
    bid_depth_usdc: float  # available liquidity at best bid
    ask_depth_usdc: float  # available liquidity at best ask

    @field_validator("best_ask")
    @classmethod
    def ask_above_bid(cls, v: float, info) -> float:
        bid = info.data.get("best_bid", 0.0)
        if v < bid:
            raise ValueError(f"best_ask ({v}) must be >= best_bid ({bid})")
        return v

    @property
    def spread(self) -> float:
        return self.best_ask - self.best_bid


class TradeDecision(BaseModel):
    """The trader's decision for a mapped signal."""

    mapped_signal_id: int
    condition_id: str
    token_id: str  # the token being bought (YES or NO token)
    direction: Direction
    p_hat: float = Field(ge=0.0, le=1.0)
    mid_price: float = Field(ge=0.0, le=1.0)
    edge: float
    kelly_size_usdc: float
    executed: bool
    order_type: Optional[OrderType] = None
    skip_reason: Optional[str] = None  # why we didn't trade (edge too small, etc.)
    decided_at: datetime


class FillResult(BaseModel):
    """Result of a live order submission."""

    order_id: str
    token_id: str
    direction: Direction
    fill_price: float
    fill_size_usdc: float
    fill_at: datetime


class OpenPosition(BaseModel):
    """A currently open position."""

    trade_id: int
    condition_id: str
    token_id: str
    direction: Direction
    size_usdc: float
    entry_price: float
    opened_at: datetime
    last_price: Optional[float] = None
    unrealized_pnl: Optional[float] = None

    @property
    def unrealized_pnl_pct(self) -> Optional[float]:
        if self.last_price is None or self.entry_price == 0:
            return None
        return (self.last_price - self.entry_price) / self.entry_price


# ---------------------------------------------------------------------------
# Observability
# ---------------------------------------------------------------------------


class MarketEvent(BaseModel):
    """A single event in a market's lifecycle."""

    condition_id: str
    event_type: MarketEventType
    payload: dict
    created_at: datetime


class BrierScore(BaseModel):
    """Calibration record for one resolved trade."""

    trade_id: int
    signal_source: str
    p_hat: float = Field(ge=0.0, le=1.0)
    actual_outcome: float = Field(ge=0.0, le=1.0)  # 1.0 = YES won, 0.0 = NO won
    brier_component: float  # (p_hat - actual_outcome) ** 2
    recorded_at: datetime

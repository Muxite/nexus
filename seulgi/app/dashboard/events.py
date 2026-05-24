"""Event-typed payloads pushed into the Dashboard ring buffers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class NewsEvent:
    site_url: str
    headline: str
    article_url: str
    detected_at: datetime
    headline_id: str = ""


@dataclass
class TradeEvent:
    condition_id: str
    question: str
    headline: str
    site_url: str
    direction: str
    confidence: float
    similarity: float
    p_hat: float
    mid_price: Optional[float]
    size_usdc: float
    skip_reason: Optional[str]
    edge: float
    decided_at: datetime
    headline_id: str = ""


@dataclass
class PositionEvent:
    condition_id: str
    question: str
    direction: str
    size_usdc: float
    entry_price: float
    current_price: float
    unrealized_pnl: float
    p_hat: float
    opened_at: datetime
    age_hours: float
    engine_name: str = "default"


@dataclass
class ClosedEvent:
    condition_id: str
    direction: str
    size_usdc: float
    entry_price: float
    exit_price: float
    realized_pnl: float
    reason: str
    closed_at: datetime


@dataclass
class WatcherStatus:
    url: str
    reachable: bool
    failures: int
    last_headline_at: Optional[datetime]
    last_headline: str
    total_headlines: int
    is_unreachable: bool
    budget_used: Optional[int] = None
    budget_limit: Optional[int] = None
    last_error: str = ""


@dataclass
class SystemHealth:
    """Aggregate health snapshot rendered in the SYSTEM HEALTH dashboard block."""
    scraper_reachable: bool = False
    scraper_url: str = ""
    scraper_last_ok_at: Optional[datetime] = None
    scraper_last_error: str = "never probed"
    scraper_latency_ms: Optional[float] = None
    ai_key_present: bool = False
    ai_neutral_rate_1h: Optional[float] = None
    ai_total_decisions_1h: int = 0
    db_last_insert_at: Optional[datetime] = None

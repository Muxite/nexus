"""Guard cascade — every entry-side safety check, as an OOP list.

A Guard is a callable that takes a GuardContext and returns GuardResult.
The cascade is just `for g in guards: r = await g.check(ctx); if not r.passed: skip`.
Each guard is independent and individually unit-testable.

Concrete guards implemented here:
  - DirectionGuard          — NEUTRAL or below min_confidence/agreement
  - HardRailGuard           — entry price too close to 0 or 1
  - SoftRailGuard           — per-strategy rail override
  - VolGuard                — pre-headline vol exceeds cap (_lowvol)
  - MarketClosedGuard       — markets.closed_at IS NOT NULL          (§D.5)
  - SpreadGuard             — quoted half-spread too wide             (§D.4)
  - EdgeGuard               — signed_edge < min_edge
  - DuplicateGuard          — already have a position on this market
  - PositionCountGuard      — max_open_positions / max_unique_markets
  - CashGuard               — not enough cash for min_trade_usdc
  - FrequencyGuard          — min_seconds_between_entries / hourly cap
  - DrawdownGuard           — §D.2 24h and 4h thresholds
  - TokenBudgetGuard        — §D.3b daily AI spend
  - WatcherHealthGuard      — §D.3 scraper health degraded
  - ExposureGuard           — total committed > max_exposure_pct of bankroll
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional, Protocol

from .config import CoreConfig
from .observability import EventBus


# ---------------------------------------------------------------------------
# Context & result
# ---------------------------------------------------------------------------

@dataclass
class GuardContext:
    """Everything a guard needs to make a pass/skip decision."""
    cfg: CoreConfig
    db: object                         # SeulgiDB
    bus: EventBus
    # Strategy state shared across guards
    cash: float                        # current cash
    open_positions: int
    unique_markets: int
    committed_usdc: float
    recent_entry_ts: list[float] = field(default_factory=list)   # monotonic ts of recent entries
    # Per-evaluation inputs (set fresh for each candidate)
    condition_id: Optional[str] = None
    direction: Optional[str] = None
    confidence: float = 0.0
    agreement: float = 0.0
    quoted_price: float = 0.5
    quoted_spread_bps: float = 0.0     # set by upstream book reader; 0 = unknown
    signed_edge: float = 0.0
    mkt_vol_30m: Optional[float] = None
    market_closed: bool = False
    daily_ai_spend_usd: float = 0.0
    watcher_degraded_fraction: float = 0.0
    trace: list = field(default_factory=list)

    def record(self, step: str, *, passed: bool, **payload) -> None:
        self.trace.append({"step": step, "pass": passed, **payload})


@dataclass
class GuardResult:
    passed: bool
    skip_reason: Optional[str] = None
    detail: dict = field(default_factory=dict)


class Guard(Protocol):
    name: str
    async def check(self, ctx: GuardContext) -> GuardResult: ...


# ---------------------------------------------------------------------------
# Concrete guards
# ---------------------------------------------------------------------------

class DirectionGuard:
    name = "direction"
    async def check(self, ctx: GuardContext) -> GuardResult:
        if ctx.direction in (None, "NEUTRAL"):
            return GuardResult(False, "neutral")
        if ctx.confidence < ctx.cfg.min_confidence:
            return GuardResult(False, f"low_confidence:{ctx.confidence:.2f}")
        if ctx.agreement and ctx.agreement < ctx.cfg.min_agreement:
            return GuardResult(False, f"low_agreement:{ctx.agreement:.2f}")
        return GuardResult(True)


class HardRailGuard:
    name = "hard_rail"
    async def check(self, ctx: GuardContext) -> GuardResult:
        thr = ctx.cfg.entry_rail_skip_threshold
        p = ctx.quoted_price
        if thr > 0 and (p < thr or p > 1.0 - thr):
            return GuardResult(False, f"hard_rail:{p:.3f}")
        return GuardResult(True)


class SoftRailGuard:
    name = "soft_rail"
    async def check(self, ctx: GuardContext) -> GuardResult:
        thr = ctx.cfg.rail_skip_threshold
        p = ctx.quoted_price
        if thr > 0 and (p < thr or p > 1.0 - thr):
            return GuardResult(False, f"soft_rail:{p:.3f}")
        return GuardResult(True)


class VolGuard:
    name = "vol"
    async def check(self, ctx: GuardContext) -> GuardResult:
        cap = ctx.cfg.max_market_vol_30m
        if cap > 0 and ctx.mkt_vol_30m is not None and ctx.mkt_vol_30m > cap:
            return GuardResult(False, f"high_vol:{ctx.mkt_vol_30m:.4f}")
        return GuardResult(True)


class MarketClosedGuard:
    """§D.5 — refuse trades on closed markets."""
    name = "market_closed"
    async def check(self, ctx: GuardContext) -> GuardResult:
        if ctx.market_closed:
            return GuardResult(False, "market_closed")
        return GuardResult(True)


class SpreadGuard:
    """§D.4 — refuse if the venue's bid-ask spread exceeds max_entry_spread_bps."""
    name = "spread"
    async def check(self, ctx: GuardContext) -> GuardResult:
        if ctx.quoted_spread_bps and ctx.quoted_spread_bps > ctx.cfg.max_entry_spread_bps:
            return GuardResult(
                False, f"spread_too_wide:{ctx.quoted_spread_bps:.0f}bps",
                {"spread_bps": ctx.quoted_spread_bps,
                 "cap_bps": ctx.cfg.max_entry_spread_bps},
            )
        return GuardResult(True)


class EdgeGuard:
    name = "edge"
    async def check(self, ctx: GuardContext) -> GuardResult:
        if ctx.signed_edge < ctx.cfg.min_edge:
            return GuardResult(False, f"low_edge:{ctx.signed_edge:.4f}")
        return GuardResult(True)


class DuplicateGuard:
    name = "duplicate"
    async def check(self, ctx: GuardContext) -> GuardResult:
        if ctx.condition_id and await ctx.db.has_open_position(
            ctx.condition_id, engine_name=ctx.cfg.engine_name,
        ):
            return GuardResult(False, "duplicate_position")
        return GuardResult(True)


class PositionCountGuard:
    name = "position_count"
    async def check(self, ctx: GuardContext) -> GuardResult:
        if ctx.open_positions >= ctx.cfg.max_open_positions:
            return GuardResult(False, f"max_open_positions:{ctx.open_positions}")
        if ctx.unique_markets >= ctx.cfg.max_unique_markets:
            return GuardResult(False, f"max_unique_markets:{ctx.unique_markets}")
        return GuardResult(True)


class CashGuard:
    name = "cash"
    async def check(self, ctx: GuardContext) -> GuardResult:
        if ctx.cash < ctx.cfg.min_trade_usdc:
            return GuardResult(False, f"insufficient_cash:{ctx.cash:.2f}")
        return GuardResult(True)


class ExposureGuard:
    name = "exposure"
    async def check(self, ctx: GuardContext) -> GuardResult:
        cap = ctx.cfg.bankroll_usdc * ctx.cfg.max_exposure_pct
        if ctx.committed_usdc >= cap:
            return GuardResult(
                False, f"exposure_cap:{ctx.committed_usdc:.2f}",
                {"committed": ctx.committed_usdc, "cap": cap},
            )
        return GuardResult(True)


class FrequencyGuard:
    """min_seconds_between_entries + per-hour cap."""
    name = "frequency"
    async def check(self, ctx: GuardContext) -> GuardResult:
        now = time.monotonic()
        # Prune to last hour
        ctx.recent_entry_ts[:] = [t for t in ctx.recent_entry_ts if now - t <= 3600.0]
        if ctx.recent_entry_ts:
            since_last = now - ctx.recent_entry_ts[-1]
            if since_last < ctx.cfg.min_seconds_between_entries:
                return GuardResult(False, f"too_fast:{since_last:.0f}s")
        if len(ctx.recent_entry_ts) >= ctx.cfg.max_entries_per_hour:
            return GuardResult(
                False, f"hourly_cap:{len(ctx.recent_entry_ts)}",
            )
        return GuardResult(True)


class DrawdownGuard:
    """§D.2 — halt on rolling drawdown breach (24h and 4h tiers)."""
    name = "drawdown"
    async def check(self, ctx: GuardContext) -> GuardResult:
        for window_hours, pct, label in (
            (ctx.cfg.drawdown_window_hours, ctx.cfg.max_drawdown_pct, "drawdown_24h"),
            (ctx.cfg.session_drawdown_window_hours, ctx.cfg.session_drawdown_pct, "drawdown_4h"),
        ):
            if pct <= 0 or window_hours <= 0:
                continue
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=window_hours)).isoformat()
            row = await ctx.db._conn.execute(   # type: ignore[attr-defined]
                """SELECT COALESCE(SUM(realized_pnl), 0.0) FROM trades
                   WHERE engine_name = ? AND realized_pnl IS NOT NULL
                     AND closed_at >= ?""",
                (ctx.cfg.engine_name, cutoff),
            )
            row = await row.fetchone()
            window_pnl = float(row[0] if row else 0.0)
            limit = -pct * ctx.cfg.bankroll_usdc
            if window_pnl <= limit:
                ctx.bus.emit(
                    engine_name=ctx.cfg.engine_name, phase="alert", name=label,
                    level="error",
                    payload={"pnl": round(window_pnl, 2),
                             "limit": round(limit, 2),
                             "window_hours": window_hours},
                )
                return GuardResult(
                    False, f"{label}:{window_pnl:.2f}",
                    {"pnl": window_pnl, "limit": limit},
                )
        return GuardResult(True)


class TokenBudgetGuard:
    """§D.3b — halt entries once daily AI spend exceeds budget."""
    name = "token_budget"
    async def check(self, ctx: GuardContext) -> GuardResult:
        if ctx.cfg.daily_ai_token_budget_usd <= 0:
            return GuardResult(True)
        if ctx.daily_ai_spend_usd > ctx.cfg.daily_ai_token_budget_usd:
            return GuardResult(
                False, "token_budget_exceeded",
                {"spent": round(ctx.daily_ai_spend_usd, 4),
                 "budget": ctx.cfg.daily_ai_token_budget_usd},
            )
        # Soft warn at 75%
        if ctx.daily_ai_spend_usd > 0.75 * ctx.cfg.daily_ai_token_budget_usd:
            ctx.bus.emit(
                engine_name=ctx.cfg.engine_name, phase="alert", name="token_budget_warn",
                level="warn",
                payload={"spent": round(ctx.daily_ai_spend_usd, 4),
                         "budget": ctx.cfg.daily_ai_token_budget_usd},
            )
        return GuardResult(True)


class WatcherHealthGuard:
    """§D.3 — halt entries if scrapers / watchers are degraded."""
    name = "watcher_health"
    async def check(self, ctx: GuardContext) -> GuardResult:
        if ctx.watcher_degraded_fraction > 0.5:
            return GuardResult(
                False, f"watchers_degraded:{ctx.watcher_degraded_fraction:.0%}",
                {"degraded_fraction": ctx.watcher_degraded_fraction},
            )
        return GuardResult(True)


# ---------------------------------------------------------------------------
# Default cascade (order matters — cheap checks first)
# ---------------------------------------------------------------------------

DEFAULT_GUARDS: list[Guard] = [
    DirectionGuard(),
    MarketClosedGuard(),
    HardRailGuard(),
    SoftRailGuard(),
    VolGuard(),
    EdgeGuard(),
    SpreadGuard(),
    DuplicateGuard(),
    PositionCountGuard(),
    ExposureGuard(),
    CashGuard(),
    FrequencyGuard(),
    DrawdownGuard(),
    TokenBudgetGuard(),
    WatcherHealthGuard(),
]


async def run_cascade(guards: list[Guard], ctx: GuardContext) -> GuardResult:
    for g in guards:
        r = await g.check(ctx)
        ctx.record(g.name, passed=r.passed, **r.detail)
        if not r.passed:
            ctx.bus.emit(
                engine_name=ctx.cfg.engine_name, phase="guard", name=g.name,
                level="warn",
                condition_id=ctx.condition_id,
                payload={"skip_reason": r.skip_reason, **r.detail},
            )
            return r
    return GuardResult(True)

"""
Simulate-mode fidelity helpers.

Pure functions modelling Polymarket mechanics — fees, slippage, share
quantization, fractional Kelly, and EV math (4B.2). Dependency-free so
audit scripts + tests import without the trader.

## Specification

Purpose: pure microstructure + sizing layer the simulate-mode trader
composes on every entry / exit / resolution leg. No DB / network / clock.

Public interface:
    clamp_price(p) -> float
    is_at_rail(p) -> bool
    fee_rate_for(market_type, cfg) -> float
    apply_slippage(mid, notional_usdc, side, market_type, cfg) -> SlippageResult
    compute_fee(shares, price, market_type, cfg) -> FeeResult
    quantize_shares(size_usdc, fill_price, fee_estimate=0.0) -> float
    fractional_kelly(bankroll, p_true, p_market, max_position, cfg) -> KellyResult
    compute_ev_yes(p_true, p_ask_yes) -> float            # 4B.2
    compute_ev_no(p_true,  p_ask_no)  -> float
    settlement_payout(shares, win) -> float

Caller contract (binding):
    - trader.py MUST pass `market_type` at every leg (entry AND exit),
      sourced from db.get_open_positions() or the live match. None falls
      back to cfg.default_rate, never 0.0.
    - The Direction enum guarantees side ∈ {"YES","NO"}; any other string
      is treated as NO (no raise).

Invariants (hardened in this rewrite — 2026-05-22, A1.1):
    - clamp_price(NaN/inf) → PRICE_MIN (bug #6).
    - apply_slippage: when the post-clamp eff_price == mid (rail snap),
      walk is RESET to 0 and applied=False so `slip_walk` telemetry never
      logs fictional displacement (bug #2).
    - compute_fee / compute_ev_* / quantize_shares / fractional_kelly all
      reject non-finite inputs safely (bugs #4, #8, #13).
    - fractional_kelly: stake bounded by min(bankroll × max_bankroll_pct,
      bankroll × max_fractional_kelly_cap, max_position_usdc, bankroll).
      Skip codes: kelly_nan_input | kelly_at_rail | kelly_negative_edge |
      kelly_below_min | None.

Slippage curve (refit 2026-05-22):
    walk = min(max_price_move, bps_per_dollar * notional_usdc / 10000)
    Historical fit on 90 matched price_impacts rows yielded
    |delta_t5m| = 0.00466·size + 0.11988·vol_30m, but that signal is
    contaminated by directional headline drift — interpreting it as pure
    walk overstates slippage ~10×. We keep the 2 bps/$ baseline (matches
    Polymarket maker docs for sub-$100 notional) and LOWER
    disable_below_usdc 5.0 → 0.50 in config.yaml so super_scout's $0.05
    micro-stakes still skip but ≥$1 stakes finally see drag.
    Before: disable_below_usdc=5.0   → 100/100 sampled trades walked 0.
    After:  disable_below_usdc=0.50  → stakes ≥ $0.50 see linear walk.

No side effects. Gas drag, maker rebates, settlement tax, spread_cost_bps
live in trader.py; this module exposes config but does not apply them.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

from config import (
    SimulateFeesConfig,
    SimulateSizingConfig,
    SimulateSlippageConfig,
)


def _is_finite(*values: float) -> bool:
    """True only if every value is a finite float (rejects NaN/inf)."""
    for v in values:
        if not isinstance(v, (int, float)) or not math.isfinite(v):
            return False
    return True


# Price clamp — Polymarket prices live in [0.01, 0.99]
PRICE_MIN = 0.01
PRICE_MAX = 0.99


def clamp_price(p: float) -> float:
    """Restrict to [PRICE_MIN, PRICE_MAX]; NaN/inf → PRICE_MIN (bug #6 fix)."""
    if not _is_finite(p):
        return PRICE_MIN
    return max(PRICE_MIN, min(PRICE_MAX, p))


def is_at_rail(p: float) -> bool:
    """True when price saturates either rail. NaN → False."""
    if not _is_finite(p):
        return False
    return p <= PRICE_MIN or p >= PRICE_MAX


# Category lookup
def fee_rate_for(market_type: Optional[str], cfg: SimulateFeesConfig) -> float:
    """Taker-fee rate for a category; None / unknown → cfg.default_rate."""
    if not market_type:
        return cfg.default_rate
    return cfg.category_rates.get(market_type, cfg.default_rate)


# Slippage
@dataclass(frozen=True)
class SlippageResult:
    mid: float
    walk: float          # absolute price units, signed against the trader
    effective_price: float
    applied: bool        # False if disabled OR rail-snapped


def apply_slippage(
    mid: float,
    notional_usdc: float,
    side: str,
    market_type: Optional[str],
    cfg: SimulateSlippageConfig,
) -> SlippageResult:
    """Walk the effective fill price AGAINST the trader.

    YES buy → price up; NO buy → price down. Capped at cfg.max_price_move.
    Disabled for tiny notional, whitelisted categories, or non-finite input.
    Rail-snap fix: if the clamp neutralised the walk, walk=0 and applied=False
    so telemetry never logs fictional displacement (bug #2).
    """
    if not _is_finite(mid, notional_usdc):
        m = clamp_price(mid)
        return SlippageResult(mid=m, walk=0.0, effective_price=m, applied=False)
    mid = clamp_price(mid)
    if (
        not cfg.enabled
        or notional_usdc < cfg.disable_below_usdc
        or (market_type and market_type in cfg.disable_for_categories)
    ):
        return SlippageResult(mid=mid, walk=0.0, effective_price=mid, applied=False)
    walk = min(cfg.bps_per_dollar * notional_usdc / 10000.0, cfg.max_price_move)
    if isinstance(side, str) and side.upper() == "YES":
        eff = clamp_price(mid + walk)
    else:
        eff = clamp_price(mid - walk)
    if eff == mid:
        return SlippageResult(mid=mid, walk=0.0, effective_price=mid, applied=False)
    return SlippageResult(mid=mid, walk=walk, effective_price=eff, applied=True)


# Dynamic taker fee — Fee = max(min_fee, shares × rate × p × (1-p))
@dataclass(frozen=True)
class FeeResult:
    shares: float
    price: float
    rate: float
    raw_fee: float
    fee: float


def compute_fee(
    shares: float,
    price: float,
    market_type: Optional[str],
    cfg: SimulateFeesConfig,
) -> FeeResult:
    """Polymarket dynamic taker fee. Non-finite inputs → zero fee."""
    if not cfg.enabled or shares <= 0 or not _is_finite(shares, price):
        return FeeResult(shares=shares, price=price, rate=0.0, raw_fee=0.0, fee=0.0)
    rate = fee_rate_for(market_type, cfg)
    p = clamp_price(price)
    raw = shares * rate * p * (1.0 - p)
    fee = max(cfg.min_fee_usdc, raw) if rate > 0 else 0.0
    return FeeResult(shares=shares, price=p, rate=rate, raw_fee=raw, fee=fee)


# Share quantization — Polymarket settles in 0.01-share increments
SHARE_INCREMENT = 0.01


def quantize_shares(
    size_usdc: float,
    fill_price: float,
    fee_estimate: float = 0.0,
) -> float:
    """Convert dollar stake → integer-cent shares at fill_price.

    Reserves fee_estimate out of the stake before computing share count.
    Non-finite inputs return 0 shares.
    """
    if not _is_finite(size_usdc, fill_price, fee_estimate):
        return 0.0
    fill_price = clamp_price(fill_price)
    available = max(0.0, size_usdc - fee_estimate)
    return math.floor((available / fill_price) / SHARE_INCREMENT) * SHARE_INCREMENT


# EV math (master plan 4B.2)
def compute_ev_yes(p_true: float, p_ask_yes: float) -> float:
    """EV per $1 of YES exposure: p · (1-P_ask) - (1-p) · P_ask."""
    if not _is_finite(p_true, p_ask_yes):
        return 0.0
    p_ask = clamp_price(p_ask_yes)
    p = max(0.0, min(1.0, p_true))
    return p * (1.0 - p_ask) - (1.0 - p) * p_ask


def compute_ev_no(p_true: float, p_ask_no: float) -> float:
    """EV per $1 of NO exposure. P(NO wins) = 1 - p_true."""
    if not _is_finite(p_true, p_ask_no):
        return 0.0
    p_ask = clamp_price(p_ask_no)
    p = max(0.0, min(1.0, p_true))
    return (1.0 - p) * (1.0 - p_ask) - p * p_ask


# Fractional Kelly sizing (with hard cap — 4B.3)
@dataclass(frozen=True)
class KellyResult:
    f_star: float
    fractional: float
    capped_pct: float
    stake_usdc: float
    skip_reason: Optional[str]


def fractional_kelly(
    bankroll_usdc: float,
    p_true: float,
    p_market: float,
    max_position_usdc: float,
    cfg: SimulateSizingConfig,
) -> KellyResult:
    """Compute the stake for one bet under fractional Kelly.

    p_true   — model's probability of the outcome we're betting ON
    p_market — current market-implied probability for that same outcome
               (price we'd pay per share for the YES of our side).

    Effective kelly = min(cfg.kelly_fraction, cfg.max_fractional_kelly_cap).
    This caps an unsafe config knob at call time (4B.3). The final stake is
    additionally bounded by max_position_usdc and `hard_cap × bankroll`.
    """
    if not _is_finite(p_true, p_market, bankroll_usdc, max_position_usdc):
        return KellyResult(0.0, 0.0, 0.0, 0.0, "kelly_nan_input")
    p_market = clamp_price(p_market)
    p_true = max(0.0, min(1.0, p_true))
    b = (1.0 / p_market) - 1.0
    if b <= 0:
        return KellyResult(0.0, 0.0, 0.0, 0.0, "kelly_at_rail")
    f_star = (p_true * (b + 1.0) - 1.0) / b
    if f_star <= 0:
        return KellyResult(f_star, 0.0, 0.0, 0.0, "kelly_negative_edge")
    hard_cap = getattr(cfg, "max_fractional_kelly_cap", 0.10)
    effective_kelly = min(cfg.kelly_fraction, hard_cap)
    fractional = f_star * effective_kelly
    capped_pct = min(fractional, cfg.max_bankroll_pct, hard_cap)
    stake = min(
        bankroll_usdc * capped_pct,
        max_position_usdc,
        bankroll_usdc,
        bankroll_usdc * hard_cap,
    )
    if stake < cfg.min_stake_usdc:
        return KellyResult(f_star, fractional, capped_pct, 0.0, "kelly_below_min")
    return KellyResult(f_star, fractional, capped_pct, stake, None)


# Settlement payout — RESOLVE_WIN pays $1.00/share, RESOLVE_LOSS pays $0
def settlement_payout(shares: float, win: bool) -> float:
    """At final resolution every winning share pays exactly $1.00."""
    if not _is_finite(shares) or shares <= 0:
        return 0.0
    return shares if win else 0.0

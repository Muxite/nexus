"""
Simulate-mode fidelity helpers.

Pure functions that model Polymarket mechanics — fees, slippage, share
quantization, fractional Kelly sizing. Kept dependency-free so the audit
script and tests can import them without spinning up a trader.

See plan §7 for the design and §7.5 for the integration notes.
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

# ---------------------------------------------------------------------------
# Price clamp — Polymarket prices live in [0.01, 0.99]
# ---------------------------------------------------------------------------

PRICE_MIN = 0.01
PRICE_MAX = 0.99


def clamp_price(p: float) -> float:
    return max(PRICE_MIN, min(PRICE_MAX, p))


def is_at_rail(p: float) -> bool:
    """True when price is so saturated there's effectively no liquidity."""
    return p <= PRICE_MIN or p >= PRICE_MAX


# ---------------------------------------------------------------------------
# Category lookup
# ---------------------------------------------------------------------------


def fee_rate_for(market_type: Optional[str], cfg: SimulateFeesConfig) -> float:
    """Look up the taker-fee rate for a market category. Falls back to default."""
    if not market_type:
        return cfg.default_rate
    return cfg.category_rates.get(market_type, cfg.default_rate)


# ---------------------------------------------------------------------------
# Slippage — deterministic "your order walks the book" model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SlippageResult:
    mid: float
    walk: float          # absolute price units, signed against the trader
    effective_price: float
    applied: bool        # False if disabled for this notional/category


def apply_slippage(
    mid: float,
    notional_usdc: float,
    side: str,            # "YES" or "NO" — direction we're buying
    market_type: Optional[str],
    cfg: SimulateSlippageConfig,
) -> SlippageResult:
    """Walk the effective fill price *against* the trader.

    For a YES buy, the price moves up; for a NO buy, the price moves down
    (we're paying more for the contract we want). Capped at max_price_move.
    Disabled for tiny notional or whitelisted categories.
    """
    mid = clamp_price(mid)
    if not cfg.enabled:
        return SlippageResult(mid=mid, walk=0.0, effective_price=mid, applied=False)
    if notional_usdc < cfg.disable_below_usdc:
        return SlippageResult(mid=mid, walk=0.0, effective_price=mid, applied=False)
    if market_type and market_type in cfg.disable_for_categories:
        return SlippageResult(mid=mid, walk=0.0, effective_price=mid, applied=False)

    walk_raw = cfg.bps_per_dollar * notional_usdc / 10000.0
    walk = min(walk_raw, cfg.max_price_move)
    if side.upper() == "YES":
        eff = clamp_price(mid + walk)
    else:
        eff = clamp_price(mid - walk)
    return SlippageResult(mid=mid, walk=walk, effective_price=eff, applied=True)


# ---------------------------------------------------------------------------
# Dynamic taker fee — Fee = max(min_fee, shares × rate × p × (1-p))
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FeeResult:
    shares: float
    price: float
    rate: float
    raw_fee: float
    fee: float           # post min_fee floor


def compute_fee(
    shares: float,
    price: float,
    market_type: Optional[str],
    cfg: SimulateFeesConfig,
) -> FeeResult:
    """Polymarket dynamic taker fee."""
    if not cfg.enabled or shares <= 0:
        return FeeResult(shares=shares, price=price, rate=0.0, raw_fee=0.0, fee=0.0)
    rate = fee_rate_for(market_type, cfg)
    p = clamp_price(price)
    raw = shares * rate * p * (1.0 - p)
    fee = max(cfg.min_fee_usdc, raw) if rate > 0 else 0.0
    return FeeResult(shares=shares, price=p, rate=rate, raw_fee=raw, fee=fee)


# ---------------------------------------------------------------------------
# Share quantization — Polymarket settles in 0.01-share increments
# ---------------------------------------------------------------------------

SHARE_INCREMENT = 0.01


def quantize_shares(
    size_usdc: float,
    fill_price: float,
    fee_estimate: float = 0.0,
) -> float:
    """Convert dollar stake → integer-cent shares at the fill price.

    Reserves fee_estimate out of the stake before computing share count, so
    the trader's bankroll debit (size_usdc) covers shares × price + fee.
    """
    fill_price = clamp_price(fill_price)
    available = max(0.0, size_usdc - fee_estimate)
    raw_shares = available / fill_price
    return math.floor(raw_shares / SHARE_INCREMENT) * SHARE_INCREMENT


# ---------------------------------------------------------------------------
# Fractional Kelly sizing
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class KellyResult:
    f_star: float           # raw Kelly fraction (may be ≤ 0)
    fractional: float       # f_star × kelly_fraction (still pre-cap)
    capped_pct: float       # min(fractional, max_bankroll_pct)
    stake_usdc: float       # final stake in dollars (0 if skipped)
    skip_reason: Optional[str]  # "kelly_negative_edge" | "kelly_below_min" | None


def fractional_kelly(
    bankroll_usdc: float,
    p_true: float,
    p_market: float,
    max_position_usdc: float,
    cfg: SimulateSizingConfig,
) -> KellyResult:
    """Compute the stake for one bet under fractional Kelly.

    p_true   — model's true probability of the outcome we're betting ON
    p_market — current market-implied probability for that same outcome
               (i.e. the price we'd pay per share for the YES of our side)
    """
    p_market = clamp_price(p_market)
    p_true = max(0.0, min(1.0, p_true))
    # b = payout/stake - 1 = (1 / p_market) - 1
    b = (1.0 / p_market) - 1.0
    if b <= 0:
        return KellyResult(0.0, 0.0, 0.0, 0.0, "kelly_at_rail")
    f_star = (p_true * (b + 1.0) - 1.0) / b
    if f_star <= 0:
        return KellyResult(f_star, 0.0, 0.0, 0.0, "kelly_negative_edge")
    fractional = f_star * cfg.kelly_fraction
    capped_pct = min(fractional, cfg.max_bankroll_pct)
    stake = min(bankroll_usdc * capped_pct, max_position_usdc, bankroll_usdc)
    if stake < cfg.min_stake_usdc:
        return KellyResult(f_star, fractional, capped_pct, 0.0, "kelly_below_min")
    return KellyResult(f_star, fractional, capped_pct, stake, None)


# ---------------------------------------------------------------------------
# Settlement payout — RESOLVE_WIN pays $1.00/share, RESOLVE_LOSS pays $0
# ---------------------------------------------------------------------------


def settlement_payout(shares: float, win: bool) -> float:
    """At final resolution every winning share pays exactly $1.00."""
    return shares * 1.0 if win else 0.0

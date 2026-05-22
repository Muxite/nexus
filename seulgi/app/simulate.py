"""
Simulate-mode fidelity helpers.

Pure functions that model Polymarket mechanics — fees, slippage, share
quantization, fractional Kelly sizing. Kept dependency-free so the audit
script and tests can import them without spinning up a trader.

See plan §7 for the design and §7.5 for the integration notes.

## Specification

Purpose:
    Pure microstructure layer the simulate-mode trader composes on every
    entry, exit, and resolution leg. Plain-float inputs + dataclass configs;
    no DB, network, or clock access.

Public interface:
    clamp_price(p) -> float: restrict to [PRICE_MIN=0.01, PRICE_MAX=0.99].
    is_at_rail(p) -> bool: True iff p saturates either rail.
    fee_rate_for(market_type, cfg) -> float: category lookup, falls back
        to cfg.default_rate for None / unknown keys.
    apply_slippage(mid, notional_usdc, side, market_type, cfg)
        -> SlippageResult(mid, walk, effective_price, applied):
        walk price AGAINST trader; YES buys up, NO buys down; disabled
        below disable_below_usdc or for whitelisted categories.
    compute_fee(shares, price, market_type, cfg)
        -> FeeResult: fee = max(min_fee_usdc, shares*rate*p*(1-p));
        zero when disabled, shares<=0, or rate==0.
    quantize_shares(size_usdc, fill_price, fee_estimate=0.0) -> float:
        floor to SHARE_INCREMENT=0.01 after reserving fee_estimate.
    fractional_kelly(bankroll, p_true, p_market, max_position, cfg)
        -> KellyResult: stake under fractional Kelly with bankroll-pct
        + min-stake guards; skip_reason ∈ {kelly_at_rail,
        kelly_negative_edge, kelly_below_min, None}.
    settlement_payout(shares, win) -> float: $1/share if win else $0.

Invariants:
    - clamp_price(p) ∈ [PRICE_MIN, PRICE_MAX] for any finite real p.
    - fee_rate_for never raises (KeyError-free dict lookup).
    - apply_slippage: effective_price ∈ [PRICE_MIN, PRICE_MAX]; walk >= 0
      (magnitude — sign applied per side); applied=False ⇒ walk == 0.0.
    - compute_fee: fee >= 0 for non-negative inputs; fee == 0 when
      rate == 0 or shares <= 0 or cfg.enabled is False.
    - quantize_shares: non-negative multiple of SHARE_INCREMENT; never
      exceeds (size_usdc - fee_estimate) / fill_price.
    - fractional_kelly: stake_usdc <= min(bankroll * max_bankroll_pct,
      max_position_usdc, bankroll); stake==0 iff skip_reason set.

Side effects:
    None. No I/O, no global mutation.

Failure modes (silent unless noted):
    - clamp_price(NaN) -> PRICE_MAX (NaN comparison quirk).
    - is_at_rail(NaN) -> False.
    - apply_slippage(side=None) raises AttributeError.
    - apply_slippage(side != "YES") behaves as NO.
    - apply_slippage at rail: walk>0, applied=True, but effective_price
      clamps back to mid — the walk is fictional.
    - compute_fee(price=NaN) routes through clamp to PRICE_MAX.
    - compute_fee(rate==0) bypasses the min_fee_usdc floor.
    - quantize_shares(fill_price<=0) clamps to 0.01 and inflates shares.
    - quantize_shares(fee_estimate>size_usdc) returns 0.0.
    - fractional_kelly(p_true=NaN) propagates NaN, may return stake>0.
    - settlement_payout(shares<0, win=True) returns shares (negative).

Non-goals:
    - No exchange / DB / clock access; wire via trader.py.
    - No input validation (NaN/None/negative) — callers must guard.
    - Gas drag, maker rebates, settlement tax, and spread_cost_bps all
      live in trader.py; this module exposes config but does not apply
      them.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


def _is_finite(*values: float) -> bool:
    """Return True only if every value is a finite float (rejects NaN/inf)."""
    return all(isinstance(v, (int, float)) and math.isfinite(v) for v in values)

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
    if not _is_finite(p_true, p_market, bankroll_usdc, max_position_usdc):
        return KellyResult(0.0, 0.0, 0.0, 0.0, "kelly_nan_input")
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

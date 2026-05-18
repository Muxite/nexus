"""Unit tests for the pure-function simulate fidelity layer (plan §7)."""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import simulate as sim  # noqa: E402
from config import (  # noqa: E402
    SimulateFeesConfig,
    SimulateSizingConfig,
    SimulateSlippageConfig,
)


# ---------------------------------------------------------------------------
# Price clamp
# ---------------------------------------------------------------------------


def test_clamp_price_low_high():
    assert sim.clamp_price(0.0) == 0.01
    assert sim.clamp_price(1.5) == 0.99
    assert sim.clamp_price(0.5) == 0.5


def test_is_at_rail():
    assert sim.is_at_rail(0.005)
    assert sim.is_at_rail(0.995)
    assert not sim.is_at_rail(0.5)


# ---------------------------------------------------------------------------
# Fee
# ---------------------------------------------------------------------------


def test_fee_peak_at_p_half():
    cfg = SimulateFeesConfig()
    f_mid = sim.compute_fee(shares=100, price=0.5, market_type=None, cfg=cfg).fee
    f_edge = sim.compute_fee(shares=100, price=0.05, market_type=None, cfg=cfg).fee
    assert f_mid > f_edge
    # 100 × 0.04 × 0.5 × 0.5 = 1.00
    assert pytest.approx(f_mid, rel=1e-6) == 1.0


def test_fee_geopolitics_zero():
    cfg = SimulateFeesConfig()
    f = sim.compute_fee(shares=100, price=0.5, market_type="geopolitics", cfg=cfg)
    assert f.rate == 0.0
    assert f.fee == 0.0


def test_fee_crypto_higher():
    cfg = SimulateFeesConfig()
    f_crypto = sim.compute_fee(shares=100, price=0.5, market_type="crypto", cfg=cfg).fee
    f_politics = sim.compute_fee(shares=100, price=0.5, market_type="politics", cfg=cfg).fee
    assert f_crypto > f_politics


def test_fee_disabled():
    cfg = SimulateFeesConfig(enabled=False)
    assert sim.compute_fee(100, 0.5, None, cfg).fee == 0.0


# ---------------------------------------------------------------------------
# Slippage
# ---------------------------------------------------------------------------


def test_slippage_walks_against_yes_buyer():
    cfg = SimulateSlippageConfig()
    r = sim.apply_slippage(mid=0.5, notional_usdc=100, side="YES",
                           market_type=None, cfg=cfg)
    # 2 bps × $100 / 10000 = 0.02; capped at max_price_move=0.02
    assert r.applied
    assert r.effective_price > r.mid
    assert pytest.approx(r.walk) == 0.02


def test_slippage_walks_against_no_buyer():
    cfg = SimulateSlippageConfig()
    r = sim.apply_slippage(mid=0.5, notional_usdc=10, side="NO",
                           market_type=None, cfg=cfg)
    assert r.applied
    assert r.effective_price < r.mid


def test_slippage_disabled_below_threshold():
    cfg = SimulateSlippageConfig()
    r = sim.apply_slippage(mid=0.5, notional_usdc=1.0, side="YES",
                           market_type=None, cfg=cfg)
    assert not r.applied
    assert r.walk == 0.0


def test_slippage_geopolitics_excluded():
    cfg = SimulateSlippageConfig()
    r = sim.apply_slippage(mid=0.5, notional_usdc=100, side="YES",
                           market_type="geopolitics", cfg=cfg)
    assert not r.applied


# ---------------------------------------------------------------------------
# Share quantization
# ---------------------------------------------------------------------------


def test_quantize_shares_floors_to_cent():
    # $10 at $0.40 → 25 shares; fee $0.10 → ($10-$0.10)/0.40 = 24.75 → 24.75
    s = sim.quantize_shares(size_usdc=10.0, fill_price=0.40, fee_estimate=0.10)
    assert s == pytest.approx(24.75)


def test_quantize_zero_size():
    assert sim.quantize_shares(0.0, 0.5, 0.0) == 0.0


# ---------------------------------------------------------------------------
# Fractional Kelly
# ---------------------------------------------------------------------------


def test_kelly_positive_edge():
    cfg = SimulateSizingConfig(kelly_fraction=0.25, max_bankroll_pct=0.05,
                               min_stake_usdc=1.0)
    r = sim.fractional_kelly(bankroll_usdc=50.0, p_true=0.50, p_market=0.40,
                             max_position_usdc=100.0, cfg=cfg)
    # b = 1/0.4 - 1 = 1.5  →  f* = (0.5*2.5 - 1)/1.5 = 0.25/1.5 ≈ 0.1667
    assert r.skip_reason is None
    assert pytest.approx(r.f_star, rel=1e-3) == 0.25 / 1.5
    # 0.25 × 0.1667 = 0.0417 → capped at 0.05? No, fractional < cap → stake = 50 × 0.0417 ≈ 2.08
    assert r.stake_usdc > 1.0


def test_kelly_negative_edge_skipped():
    cfg = SimulateSizingConfig()
    r = sim.fractional_kelly(bankroll_usdc=50.0, p_true=0.30, p_market=0.40,
                             max_position_usdc=100.0, cfg=cfg)
    assert r.skip_reason == "kelly_negative_edge"
    assert r.stake_usdc == 0.0


def test_kelly_capped_at_bankroll_pct():
    # Big edge — uncapped Kelly wants more than the cap.
    cfg = SimulateSizingConfig(kelly_fraction=1.0, max_bankroll_pct=0.05,
                               min_stake_usdc=1.0)
    r = sim.fractional_kelly(bankroll_usdc=50.0, p_true=0.95, p_market=0.40,
                             max_position_usdc=100.0, cfg=cfg)
    assert r.stake_usdc <= 50.0 * 0.05 + 1e-6


def test_kelly_below_min_stake():
    cfg = SimulateSizingConfig(kelly_fraction=0.01, max_bankroll_pct=0.05,
                               min_stake_usdc=5.0)
    r = sim.fractional_kelly(bankroll_usdc=10.0, p_true=0.52, p_market=0.50,
                             max_position_usdc=100.0, cfg=cfg)
    assert r.skip_reason == "kelly_below_min"


# ---------------------------------------------------------------------------
# Settlement
# ---------------------------------------------------------------------------


def test_settlement_win_pays_dollar_per_share():
    assert sim.settlement_payout(50, win=True) == 50.0


def test_settlement_loss_pays_zero():
    assert sim.settlement_payout(50, win=False) == 0.0

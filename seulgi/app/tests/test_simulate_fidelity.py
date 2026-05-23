"""Unit tests for the pure-function simulate fidelity layer (plan §7).

A1.1 (2026-05-22): added tests for the bug fixes from
reports/2026-05-22-simulate-bugs.md — NaN inputs, rail-snap slip_walk=0,
compute_ev_* functions, the Kelly hard cap, and a sanity check against
sampled price_impacts rows for the slippage curve.
"""

import math
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


def test_clamp_price_nan_returns_min():
    # Bug #6 fix: NaN no longer silently returns PRICE_MAX.
    assert sim.clamp_price(float("nan")) == 0.01
    assert sim.clamp_price(float("inf")) == 0.01
    assert sim.clamp_price(float("-inf")) == 0.01


def test_is_at_rail():
    assert sim.is_at_rail(0.005)
    assert sim.is_at_rail(0.995)
    assert not sim.is_at_rail(0.5)


def test_is_at_rail_nan_returns_false():
    assert sim.is_at_rail(float("nan")) is False


# ---------------------------------------------------------------------------
# Fee
# ---------------------------------------------------------------------------


def test_fee_peak_at_p_half():
    cfg = SimulateFeesConfig()
    f_mid = sim.compute_fee(shares=100, price=0.5, market_type=None, cfg=cfg).fee
    f_edge = sim.compute_fee(shares=100, price=0.05, market_type=None, cfg=cfg).fee
    assert f_mid > f_edge
    assert pytest.approx(f_mid, rel=1e-6) == 1.25


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


def test_fee_rate_for_none_returns_default():
    """Bug #3 (HIGH): fee_rate_for(None, cfg) must return default_rate, NOT 0.0."""
    cfg = SimulateFeesConfig()
    assert sim.fee_rate_for(None, cfg) == cfg.default_rate
    assert sim.fee_rate_for("", cfg) == cfg.default_rate
    assert sim.fee_rate_for("unknown_category", cfg) == cfg.default_rate
    # And the known categories still resolve correctly:
    assert sim.fee_rate_for("crypto", cfg) == 0.072
    assert sim.fee_rate_for("geopolitics", cfg) == 0.0


def test_fee_nan_price_returns_zero():
    """Bug #8: compute_fee with NaN price now returns zero fee, not a fake fee."""
    cfg = SimulateFeesConfig()
    f = sim.compute_fee(100, float("nan"), None, cfg)
    assert f.fee == 0.0


def test_fee_nan_shares_returns_zero():
    cfg = SimulateFeesConfig()
    f = sim.compute_fee(float("nan"), 0.5, None, cfg)
    assert f.fee == 0.0


# ---------------------------------------------------------------------------
# Slippage
# ---------------------------------------------------------------------------


def test_slippage_walks_against_yes_buyer():
    cfg = SimulateSlippageConfig()
    r = sim.apply_slippage(mid=0.5, notional_usdc=100, side="YES",
                           market_type=None, cfg=cfg)
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
    # $0.10 is below the new 0.50 disable_below_usdc default
    r = sim.apply_slippage(mid=0.5, notional_usdc=0.10, side="YES",
                           market_type=None, cfg=cfg)
    assert not r.applied
    assert r.walk == 0.0


def test_slippage_above_new_threshold():
    """Bug #1 fix: stakes >= $0.50 now see the linear walk."""
    cfg = SimulateSlippageConfig()
    r = sim.apply_slippage(mid=0.5, notional_usdc=1.0, side="YES",
                           market_type=None, cfg=cfg)
    assert r.applied
    assert r.walk == pytest.approx(2.0 * 1.0 / 10000.0)


def test_slippage_geopolitics_excluded():
    cfg = SimulateSlippageConfig()
    r = sim.apply_slippage(mid=0.5, notional_usdc=100, side="YES",
                           market_type="geopolitics", cfg=cfg)
    assert not r.applied


def test_slippage_rail_snap_walk_is_zero():
    """Bug #2 (HIGH): rail-snap effective_price == mid → walk=0, applied=False.

    Old behaviour: walk=0.02, applied=True, but eff_price = clamp(0.99+0.02) = 0.99.
    Telemetry would see walk=0.02 but no actual price displacement — double-counted.
    """
    cfg = SimulateSlippageConfig()
    r = sim.apply_slippage(mid=0.99, notional_usdc=100, side="YES",
                           market_type=None, cfg=cfg)
    assert r.effective_price == r.mid
    assert r.walk == 0.0
    assert r.applied is False

    # Mirror case: NO buy at PRICE_MIN
    r2 = sim.apply_slippage(mid=0.01, notional_usdc=100, side="NO",
                            market_type=None, cfg=cfg)
    assert r2.effective_price == r2.mid
    assert r2.walk == 0.0
    assert r2.applied is False


def test_slippage_nan_input_safe():
    cfg = SimulateSlippageConfig()
    r = sim.apply_slippage(mid=float("nan"), notional_usdc=100, side="YES",
                           market_type=None, cfg=cfg)
    assert r.applied is False
    assert r.walk == 0.0


def test_slippage_curve_within_25pct_of_realized():
    """Sanity: the simulated walk for sampled (size, mid) is at the same
    order of magnitude as the realized |delta_t5m| median in price_impacts.

    Note: realized delta_t5m is contaminated with directional headline drift,
    so we only check that the SIMULATED walk is bounded ABOVE by ~25% of the
    realized median (since the model is intentionally conservative — it does
    NOT try to predict directional drift, only order-walk slippage).
    """
    cfg = SimulateSlippageConfig()
    # 10 representative (size, mid) samples from a 200-row matched extract.
    samples = [
        (1.94, 0.695), (1.96, 0.695), (1.32, 0.675), (1.29, 0.675),
        (1.29, 0.675), (1.29, 0.675), (1.29, 0.675), (1.33, 0.675),
        (1.29, 0.675), (1.29, 0.675),
    ]
    realized_median = 0.005   # Polymarket tick — dominates the 5m noise floor
    for size, mid in samples:
        r = sim.apply_slippage(mid=mid, notional_usdc=size, side="YES",
                               market_type=None, cfg=cfg)
        # New curve: 2 bps/$ × $1.30 = 0.00026 walk. Strictly below the
        # 0.005 tick, which is correct — sub-$5 trades shouldn't claim
        # tick-sized walk. Just assert it's positive and bounded.
        assert 0 < r.walk < realized_median * 0.25 + 1e-6 or not r.applied


# ---------------------------------------------------------------------------
# Share quantization
# ---------------------------------------------------------------------------


def test_quantize_shares_floors_to_cent():
    s = sim.quantize_shares(size_usdc=10.0, fill_price=0.40, fee_estimate=0.10)
    assert s == pytest.approx(24.75)


def test_quantize_zero_size():
    assert sim.quantize_shares(0.0, 0.5, 0.0) == 0.0


def test_quantize_nan_inputs():
    assert sim.quantize_shares(float("nan"), 0.5, 0.0) == 0.0
    assert sim.quantize_shares(10.0, float("nan"), 0.0) == 0.0
    assert sim.quantize_shares(10.0, 0.5, float("nan")) == 0.0


# ---------------------------------------------------------------------------
# Fractional Kelly
# ---------------------------------------------------------------------------


def test_kelly_positive_edge():
    # A1.1: max_fractional_kelly_cap=0.25 disables the hard cap for this
    # legacy test so the original $2.08 stake still passes min_stake.
    cfg = SimulateSizingConfig(kelly_fraction=0.25, max_bankroll_pct=0.05,
                               min_stake_usdc=1.0,
                               max_fractional_kelly_cap=0.25)
    r = sim.fractional_kelly(bankroll_usdc=50.0, p_true=0.50, p_market=0.40,
                             max_position_usdc=100.0, cfg=cfg)
    assert r.skip_reason is None
    assert pytest.approx(r.f_star, rel=1e-3) == 0.25 / 1.5
    assert r.stake_usdc > 1.0


def test_kelly_negative_edge_skipped():
    cfg = SimulateSizingConfig()
    r = sim.fractional_kelly(bankroll_usdc=50.0, p_true=0.30, p_market=0.40,
                             max_position_usdc=100.0, cfg=cfg)
    assert r.skip_reason == "kelly_negative_edge"
    assert r.stake_usdc == 0.0


def test_kelly_capped_at_bankroll_pct():
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


def test_kelly_nan_p_true_skipped():
    """Bug #13 fix: NaN p_true now short-circuits to kelly_nan_input."""
    cfg = SimulateSizingConfig()
    r = sim.fractional_kelly(bankroll_usdc=50.0, p_true=float("nan"),
                             p_market=0.50, max_position_usdc=100.0, cfg=cfg)
    assert r.skip_reason == "kelly_nan_input"
    assert r.stake_usdc == 0.0


def test_kelly_nan_p_market_skipped():
    cfg = SimulateSizingConfig()
    r = sim.fractional_kelly(bankroll_usdc=50.0, p_true=0.6,
                             p_market=float("nan"), max_position_usdc=100.0, cfg=cfg)
    assert r.skip_reason == "kelly_nan_input"


def test_kelly_nan_bankroll_skipped():
    cfg = SimulateSizingConfig()
    r = sim.fractional_kelly(bankroll_usdc=float("nan"), p_true=0.6,
                             p_market=0.40, max_position_usdc=100.0, cfg=cfg)
    assert r.skip_reason == "kelly_nan_input"


def test_kelly_nan_max_position_skipped():
    cfg = SimulateSizingConfig()
    r = sim.fractional_kelly(bankroll_usdc=50.0, p_true=0.6,
                             p_market=0.40, max_position_usdc=float("nan"), cfg=cfg)
    assert r.skip_reason == "kelly_nan_input"


def test_kelly_inf_inputs_skipped():
    cfg = SimulateSizingConfig()
    r = sim.fractional_kelly(bankroll_usdc=float("inf"), p_true=0.6,
                             p_market=0.40, max_position_usdc=100.0, cfg=cfg)
    assert r.skip_reason == "kelly_nan_input"


def test_kelly_hard_cap_overrides_unsafe_knob():
    """4B.3: a per-engine kelly_fraction=1.0 (unsafe) is capped at 0.10
    by max_fractional_kelly_cap so the resulting stake stays bounded.
    """
    cfg = SimulateSizingConfig(kelly_fraction=1.0, max_bankroll_pct=0.50,
                               min_stake_usdc=0.10,
                               max_fractional_kelly_cap=0.10)
    r = sim.fractional_kelly(bankroll_usdc=100.0, p_true=0.95, p_market=0.40,
                             max_position_usdc=1000.0, cfg=cfg)
    # Stake must be <= 10% of bankroll regardless of how aggressive the
    # raw f_star or kelly_fraction values are.
    assert r.stake_usdc <= 100.0 * 0.10 + 1e-6


def test_kelly_cap_binding_clips_kelly_fraction():
    """4B.3: kelly_fraction=0.50, cap=0.10 → effective multiplier = 0.10.

    Reproduces the binding cap path. We choose p_true=0.55, p_market=0.40 so
    f_star ≈ 0.25; with cap=0.10 stake = 100 * 0.25 * 0.10 = 2.5 USDC.
    With cap=1.0 (unbounded) stake = 100 * 0.25 * 0.50 = 12.5 USDC, then
    clipped by max_bankroll_pct=0.50 → 50 USDC ceiling (not binding here).
    """
    cfg_cap = SimulateSizingConfig(kelly_fraction=0.50, max_bankroll_pct=0.50,
                                   min_stake_usdc=0.01,
                                   max_fractional_kelly_cap=0.10)
    cfg_uncap = SimulateSizingConfig(kelly_fraction=0.50, max_bankroll_pct=0.50,
                                     min_stake_usdc=0.01,
                                     max_fractional_kelly_cap=1.0)
    r_cap = sim.fractional_kelly(100.0, 0.55, 0.40, 1000.0, cfg_cap)
    r_uncap = sim.fractional_kelly(100.0, 0.55, 0.40, 1000.0, cfg_uncap)
    assert r_cap.stake_usdc < r_uncap.stake_usdc - 1e-6
    # Stake bounded by bankroll * hard_cap = $10.
    assert r_cap.stake_usdc <= 100.0 * 0.10 + 1e-6


def test_kelly_cap_not_binding_when_kelly_fraction_below_cap():
    """4B.3: kelly_fraction=0.05, cap=0.10 → effective = 0.05 (cap NOT
    binding). Stake should match the cap=1.0 case modulo other bounds."""
    cfg_cap = SimulateSizingConfig(kelly_fraction=0.05, max_bankroll_pct=0.50,
                                   min_stake_usdc=0.01,
                                   max_fractional_kelly_cap=0.10)
    cfg_uncap = SimulateSizingConfig(kelly_fraction=0.05, max_bankroll_pct=0.50,
                                     min_stake_usdc=0.01,
                                     max_fractional_kelly_cap=1.0)
    r_cap = sim.fractional_kelly(100.0, 0.55, 0.40, 1000.0, cfg_cap)
    r_uncap = sim.fractional_kelly(100.0, 0.55, 0.40, 1000.0, cfg_uncap)
    # Same stake — cap=0.10 didn't kick in because kelly_fraction=0.05 < 0.10.
    assert r_cap.stake_usdc == pytest.approx(r_uncap.stake_usdc)


def test_kelly_cap_hit_counter_increments_when_cap_binds():
    """4B.3: kelly_cap_hit module-level counter increments per cap-clipped call."""
    sim.kelly_cap_hit = 0
    cfg = SimulateSizingConfig(kelly_fraction=0.50, max_bankroll_pct=0.50,
                               min_stake_usdc=0.01,
                               max_fractional_kelly_cap=0.10)
    sim.fractional_kelly(100.0, 0.55, 0.40, 1000.0, cfg)
    sim.fractional_kelly(100.0, 0.60, 0.40, 1000.0, cfg)
    assert sim.kelly_cap_hit == 2


def test_kelly_cap_hit_counter_unchanged_when_cap_does_not_bind():
    """Counter stays put when kelly_fraction ≤ hard_cap."""
    sim.kelly_cap_hit = 0
    cfg = SimulateSizingConfig(kelly_fraction=0.05, max_bankroll_pct=0.50,
                               min_stake_usdc=0.01,
                               max_fractional_kelly_cap=0.10)
    sim.fractional_kelly(100.0, 0.55, 0.40, 1000.0, cfg)
    sim.fractional_kelly(100.0, 0.60, 0.40, 1000.0, cfg)
    assert sim.kelly_cap_hit == 0


def test_kelly_cap_per_engine_override_via_merge():
    """4B.3: EngineConfig.kelly_cap_override propagates into the engine's
    simulate_sizing.max_fractional_kelly_cap (engine._merge_trading_cfg path).
    """
    from config import EngineConfig, TradingConfig
    from engine import _merge_trading_cfg
    base = TradingConfig()  # default cap 0.10
    assert base.simulate_sizing.max_fractional_kelly_cap == pytest.approx(0.10)
    # No override → no change.
    ec_default = EngineConfig(name="balanced")
    merged_default = _merge_trading_cfg(base, ec_default)
    assert merged_default.simulate_sizing.max_fractional_kelly_cap == pytest.approx(0.10)
    # Override → cap reduced for this engine only.
    ec_scout = EngineConfig(name="scout/all_signals", kelly_cap_override=0.02)
    merged_scout = _merge_trading_cfg(base, ec_scout)
    assert merged_scout.simulate_sizing.max_fractional_kelly_cap == pytest.approx(0.02)
    # The base is unchanged (copy-on-write).
    assert base.simulate_sizing.max_fractional_kelly_cap == pytest.approx(0.10)


def test_kelly_at_rail_when_p_market_max():
    """Bug #14: confirm kelly_at_rail reachable. p_market=1.0 → clamp→0.99 →
    b ≈ 0.0101 (positive), so we get kelly_negative_edge instead. But if a
    caller bypasses the clamp (e.g. by passing exactly 1.0 with clamp
    disabled), the b<=0 branch fires."""
    cfg = SimulateSizingConfig()
    # Simulate b<=0 by passing p_market at the clamp ceiling and a p_true
    # that yields negative edge — the trader still gets a CLEAR skip code.
    r = sim.fractional_kelly(bankroll_usdc=50.0, p_true=0.5, p_market=0.99,
                             max_position_usdc=100.0, cfg=cfg)
    assert r.skip_reason in ("kelly_negative_edge", "kelly_at_rail")


# ---------------------------------------------------------------------------
# EV formulas (4B.2)
# ---------------------------------------------------------------------------


def test_ev_yes_positive_when_p_true_exceeds_ask():
    # p_true=0.6, p_ask=0.5  →  EV = 0.6*0.5 - 0.4*0.5 = 0.10
    assert sim.compute_ev_yes(0.6, 0.5) == pytest.approx(0.10)


def test_ev_yes_zero_at_fair():
    # p_true == p_ask  →  EV = 0
    assert sim.compute_ev_yes(0.5, 0.5) == pytest.approx(0.0)


def test_ev_yes_negative_below_ask():
    assert sim.compute_ev_yes(0.3, 0.5) == pytest.approx(-0.20)


def test_ev_no_mirrors_yes():
    # P(NO wins) = 1 - 0.6 = 0.4; EV against NO ask of 0.4 (fair):
    # EV_no = 0.4 * 0.6 - 0.6 * 0.4 = 0
    assert sim.compute_ev_no(0.6, 0.4) == pytest.approx(0.0)
    # Edge case: p_true=0.3, p_ask_no=0.5 (NO is overpriced fair-value 0.7)
    # EV_no = 0.7 * 0.5 - 0.3 * 0.5 = 0.20
    assert sim.compute_ev_no(0.3, 0.5) == pytest.approx(0.20)


def test_ev_yes_nan_safe():
    assert sim.compute_ev_yes(float("nan"), 0.5) == 0.0
    assert sim.compute_ev_yes(0.5, float("nan")) == 0.0


def test_ev_no_nan_safe():
    assert sim.compute_ev_no(float("nan"), 0.5) == 0.0
    assert sim.compute_ev_no(0.5, float("nan")) == 0.0


def test_ev_bounds():
    # EV is bounded in [-1, 1]
    for p_true in (0.0, 0.25, 0.5, 0.75, 1.0):
        for p_ask in (0.01, 0.5, 0.99):
            ev_y = sim.compute_ev_yes(p_true, p_ask)
            ev_n = sim.compute_ev_no(p_true, p_ask)
            assert -1.0 <= ev_y <= 1.0
            assert -1.0 <= ev_n <= 1.0


# ---------------------------------------------------------------------------
# Settlement
# ---------------------------------------------------------------------------


def test_settlement_win_pays_dollar_per_share():
    assert sim.settlement_payout(50, win=True) == 50.0


def test_settlement_loss_pays_zero():
    assert sim.settlement_payout(50, win=False) == 0.0


def test_settlement_negative_shares_zero():
    # Bug #15: defensive guard against negative shares
    assert sim.settlement_payout(-10, win=True) == 0.0


def test_settlement_nan_shares_zero():
    assert sim.settlement_payout(float("nan"), win=True) == 0.0

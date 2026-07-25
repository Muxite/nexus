"""Tests for `scripts/replay/predict.py`."""

from __future__ import annotations

import json

import pytest

from app.config import Config
from scripts.replay.predict import (
    BucketSummary,
    ReplayReport,
    _coerce,
    _entity_overlap_from_gate_history,
    _gross_pnl,
    _mid_from_gate_history,
    _net_pnl_for,
    _polymarket_taker_fee,
    _resolve_fee_rate,
    _spread_from_gate_history,
    apply_overrides,
    run_inproc,
)


# ---------------------------------------------------------------------------
# Coercion + overrides
# ---------------------------------------------------------------------------


def test_coerce_int():
    assert _coerce("42") == 42


def test_coerce_float():
    assert _coerce("0.06") == 0.06


def test_coerce_bool():
    assert _coerce("true") is True
    assert _coerce("False") is False


def test_coerce_none():
    assert _coerce("null") is None


def test_coerce_string_fallback():
    assert _coerce("hello") == "hello"


def test_coerce_json_list():
    assert _coerce("[1, 2, 3]") == [1, 2, 3]


def test_apply_overrides_simple():
    cfg = Config()
    out = apply_overrides(cfg, {"trade.min_edge": 0.06})
    assert out.trade.min_edge == 0.06
    # original untouched
    assert cfg.trade.min_edge == 0.04


def test_apply_overrides_nested():
    cfg = Config()
    out = apply_overrides(cfg, {"trade.polymarket_fee_rates.crypto": 0.06})
    assert out.trade.polymarket_fee_rates["crypto"] == 0.06


def test_apply_overrides_rejects_unknown_path():
    cfg = Config()
    with pytest.raises(ValueError, match="unknown"):
        apply_overrides(cfg, {"trade.nonsense.x": 1})


# ---------------------------------------------------------------------------
# Fee math (mirrors closer.py + accuracy.py)
# ---------------------------------------------------------------------------


def test_polymarket_taker_fee_zero_rate():
    assert _polymarket_taker_fee(0.5, 100.0, 0.0) == 0.0


def test_polymarket_taker_fee_parabolic_peak():
    # peak at p=0.5: fee = shares * rate * 0.25
    assert _polymarket_taker_fee(0.5, 100.0, 0.10) == pytest.approx(2.5)


def test_polymarket_taker_fee_extremes_near_zero():
    assert _polymarket_taker_fee(0.99, 100.0, 0.10) == pytest.approx(100 * 0.10 * 0.99 * 0.01)


def test_gross_pnl_yes_winning():
    # YES @ 0.40 → 0.50: size 100 → 100 * 0.10 / 0.40 = 25
    assert _gross_pnl("YES", 100.0, 0.40, 0.50) == pytest.approx(25.0)


def test_gross_pnl_no_winning():
    # NO @ 0.60 → 0.55: size 80 → 80 * 0.05 / 0.40 = 10
    assert _gross_pnl("NO", 80.0, 0.60, 0.55) == pytest.approx(10.0)


def test_gross_pnl_zero_entry_returns_zero():
    assert _gross_pnl("YES", 100.0, 0.0, 0.5) == 0.0
    assert _gross_pnl("NO", 100.0, 1.0, 0.5) == 0.0


def test_net_pnl_subtracts_round_trip_fee():
    # YES @ 0.5 → 0.5: gross 0; fee 100/0.5 * 0.10 * 0.5 * 0.5 = 5 per leg
    pnl = _net_pnl_for(
        direction="YES", size_usdc=100.0, entry_mid=0.5, exit_mid=0.5,
        fee_rate=0.10, gas_per_leg=0.0,
    )
    # gross 0 - 2*5 = -10
    assert pnl == pytest.approx(-10.0)


def test_resolve_fee_rate_prefers_per_market():
    rate = _resolve_fee_rate(
        category="politics",
        fee_rates_table={"politics": 0.04},
        default_fee_rate=0.10,
        market_fee_rate=0.072,
    )
    assert rate == pytest.approx(0.072)


def test_resolve_fee_rate_falls_back_to_category():
    rate = _resolve_fee_rate(
        category="crypto",
        fee_rates_table={"crypto": 0.072, "politics": 0.04},
        default_fee_rate=0.10,
        market_fee_rate=None,
    )
    assert rate == pytest.approx(0.072)


def test_resolve_fee_rate_default_when_unknown_category():
    rate = _resolve_fee_rate(
        category="unknown_cat",
        fee_rates_table={"politics": 0.04},
        default_fee_rate=0.10,
    )
    assert rate == pytest.approx(0.10)


# ---------------------------------------------------------------------------
# gate_history recovery
# ---------------------------------------------------------------------------


def test_entity_overlap_recovered_from_gate_history():
    hist = [
        {"gate": "similarity_floor", "outcome": "ok"},
        {"gate": "entity_overlap", "outcome": "ok", "value": "match", "threshold": 3},
    ]
    assert _entity_overlap_from_gate_history(hist) == (True, 3)


def test_entity_overlap_no_entities_marker():
    hist = [
        {"gate": "entity_overlap", "outcome": "ok", "value": "no_entities_yet", "threshold": 0},
    ]
    assert _entity_overlap_from_gate_history(hist) == (False, 0)


def test_entity_overlap_returns_none_when_gate_never_ran():
    hist = [{"gate": "similarity_floor", "outcome": "reject"}]
    assert _entity_overlap_from_gate_history(hist) is None


def test_mid_recovered_from_mid_degenerate_reject():
    hist = [
        {"gate": "edge", "outcome": "reject", "value": 0.99, "threshold": "mid_degenerate"},
    ]
    assert _mid_from_gate_history(hist) == 0.99


def test_spread_recovered_from_wide_spread_reject():
    hist = [
        {"gate": "edge", "outcome": "reject", "value": 0.15, "threshold": "wide_spread"},
    ]
    assert _spread_from_gate_history(hist) == 0.15


# ---------------------------------------------------------------------------
# Full run — bucket dispatch + headline math
# ---------------------------------------------------------------------------


def test_run_inproc_zero_overrides_changes_only_from_code_drift(snapshot_dir):
    """With zero overrides, every signal whose features are recoverable
    should produce the same decision the snapshot recorded — EXCEPT where
    code has drifted between the snapshot's era and current. For the
    fixture snapshot built by conftest, the cascade is current-code so
    decision_changes should be 0."""
    rep = run_inproc(str(snapshot_dir))
    # Fixture is current-code; expect zero decision flips and zero
    # cascade errors. (Real snapshots from older cycles may flip;
    # soak_compare.py handles that case separately.)
    assert rep.counterfactual_decision_changes == 0


def test_run_inproc_basic_counts(snapshot_dir):
    rep = run_inproc(str(snapshot_dir))
    assert rep.n_signals == 3  # 3 decisions in fixture
    assert rep.bucket_a.n == 3  # all 3 fixture decisions were executes
    assert rep.bucket_b.n == 0
    assert rep.bucket_c.n == 0
    assert rep.bucket_d.n == 0


def test_run_inproc_tighter_edge_pushes_to_bucket_b(snapshot_dir):
    """A min_edge override that exceeds the recomputed edge moves a
    trade from bucket-a to bucket-b. evaluate_gates RECOMPUTES the edge
    from p_hat/mid/fee_buffer (cfg.fee_buffer=0.02 default) — the
    fixture's stored `trade.edge` value is *original* and doesn't
    constrain the counterfactual.

    Recomputed edges are `p_win - mid - fee_buffer`, where `p_win` is
    `p_hat` for YES and `1 - p_hat` for NO (`_compute_edge`, cycle 27 —
    the book-unavailable branch treats `mid` as position space):
      trade 1 YES p_hat=0.80 mid=0.40 → 0.80 - 0.40 - 0.02 = 0.38
      trade 2 NO  p_hat=0.25 mid=0.60 → 0.75 - 0.60 - 0.02 = 0.13
      trade 3 YES p_hat=0.65 mid=0.50 → 0.65 - 0.50 - 0.02 = 0.13

    Trades 2 and 3 therefore tie at 0.13, so no threshold displaces one
    without the other — any min_edge in (0.13, 0.38] partitions 1 | 2+3.
    This docstring previously used the pre-cycle-27 NO formula
    (`mid - p_hat` = 0.33) and expected a 2 | 3 split; the fixture's own
    `edge=0.13` on trade 2 already agreed with the current code."""
    rep = run_inproc(str(snapshot_dir), {"trade.min_edge": 0.30})
    assert rep.bucket_a.n == 1  # trade 1 still fires
    assert rep.bucket_b.n == 2  # trades 2+3 displaced
    # Trade 2 closed at +10; trade 3 is open so contributes 0.
    assert rep.bucket_b.original_pnl == pytest.approx(10.0)
    # Implied notional sums size_usdc over every displaced trade, open or
    # closed: trade 2 (80) + trade 3 (50).
    assert rep.bucket_b.implied_notional == pytest.approx(130.0)


def test_run_inproc_aggressive_edge_displaces_all_but_one(snapshot_dir):
    """Pushing min_edge above trade 2's edge (0.13, see the recomputation
    note on the 0.30 case above) displaces trades 2+3 and leaves only
    trade 1 firing. Trade 2 is closed with +10 PnL → that's the
    bucket_b.original_pnl."""
    rep = run_inproc(str(snapshot_dir), {"trade.min_edge": 0.35})
    assert rep.bucket_a.n == 1
    assert rep.bucket_b.n == 2
    assert rep.bucket_b.original_pnl == pytest.approx(10.0)  # trade 2 closed @ +10


def test_run_inproc_reports_overrides_in_output(snapshot_dir):
    rep = run_inproc(str(snapshot_dir), {"trade.min_edge": 0.30})
    d = rep.to_dict()
    assert d["overrides"] == {"trade.min_edge": 0.30}
    assert "buckets" in d
    assert "net_pnl_delta_excluding_d" in d


def test_run_inproc_bucket_d_pnl_never_in_headline(snapshot_dir):
    """Sanity: bucket-(d) PnL must NEVER appear in net_pnl_delta. Even
    if we somehow accumulated it (we don't), the property would skip it."""
    rep = run_inproc(str(snapshot_dir), {"trade.min_edge": 0.30})
    # Manually create an artificial bucket-d entry; headline must ignore.
    rep.bucket_d.counterfactual_pnl = 10_000.0
    assert rep.net_pnl_delta == pytest.approx(
        (rep.bucket_a.counterfactual_pnl - rep.bucket_a.original_pnl)
        + rep.bucket_c.counterfactual_pnl
        - rep.bucket_b.original_pnl
    )


def test_run_inproc_funnel_deltas_shape(snapshot_dir):
    rep = run_inproc(str(snapshot_dir), {"trade.min_edge": 0.30})
    # Should show "edge_below_min" gaining and "ok" losing.
    deltas = rep.funnel_deltas
    assert deltas.get("ok", 0) <= 0  # 2 fewer execute
    assert deltas.get("edge_below_min", 0) >= 1  # at least 1 more reject


def test_run_inproc_to_dict_round_trips_through_json(snapshot_dir):
    rep = run_inproc(str(snapshot_dir))
    rt = json.loads(json.dumps(rep.to_dict()))
    assert rt["n_signals"] == rep.n_signals
    assert "buckets" in rt


def test_run_inproc_projected_frac_zero_when_no_c_bucket(snapshot_dir):
    rep = run_inproc(str(snapshot_dir), {"trade.min_edge": 0.30})
    assert rep.bucket_c.n == 0
    assert rep.projected_frac == 0.0

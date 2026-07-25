"""Pure-cascade tests for `evaluate_gates` + its per-gate helpers.

These tests bypass the bus/DB/LLM and exercise the gate cascade as a pure
function. They are the regression net for the PR-A extraction — if a
gate's predicate, threshold, or gate_history entry shape drifts, these
fail before the integration tests would.

The replay harness (`scripts/replay/predict.py`) calls `evaluate_gates`
directly. These tests are also the replay's correctness contract.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import pytest

from app.stages.trade import (
    GateFeatures,
    GateResult,
    evaluate_gates,
    _compute_edge,
    _compute_kelly_and_size,
    _eval_market_news_imbalance_subrules,
    _gate_cash,
    _gate_direction,
    _gate_edge,
    _gate_entity_overlap,
    _gate_freshness,
    _gate_market_news_imbalance,
    _gate_mid_degenerate,
    _gate_similarity_floor,
    _gate_size,
    _gate_wide_spread,
)
from app.tests._dates import open_end_date


# ---------------------------------------------------------------------------
# Minimal cfg shape mirroring app/config.py TradeSection. Duck-typed —
# evaluate_gates only reads attributes by name.
# ---------------------------------------------------------------------------


@dataclass
class _Cfg:
    balance_usdc: float = 1000.0
    min_similarity: float = 0.35
    min_confidence: float = 0.55
    min_edge: float = 0.04
    fee_buffer: float = 0.02
    taker_fee_buffer: float = 0.01
    kelly_fraction: float = 0.25
    max_position_fraction: float = 0.20
    min_position_usdc: float = 1.0
    min_mid: float = 0.02
    max_mid: float = 0.98
    wide_spread_threshold: float = 0.10
    min_market_volume_usdc: float = 10_000.0
    far_future_days: int = 180
    min_days_to_expiry: int = 1
    extreme_mid_low: float = 0.15
    extreme_mid_high: float = 0.85
    require_breaking_for_extreme_mid: bool = True
    require_breaking_for_far_future: bool = True
    reject_singleton_on_extreme_mid: bool = True
    accept_freshness: list = field(default_factory=lambda: ["BREAKING", "EVOLVING"])
    # Cycle 23: market-state buffer + kill switch. Defaults mirror config.py.
    execution_enabled: bool = True
    min_hours_to_expiry: float = 6.0


def _market(
    *,
    volume_usdc: Optional[float] = 50_000.0,
    days_to_expiry: Optional[int] = 30,
    min_order_size: Optional[float] = None,
) -> dict:
    """Build a market dict matching what TradeStage._fetch_market returns,
    pre-populated with `_days_to_expiry` (which the live wrapper stuffs
    on)."""
    return {
        "question": "Will X happen by end of year?",
        "description": "Details.",
        "token_yes": "ty",
        "token_no": "tn",
        "category": "politics",
        "end_date": open_end_date(),
        "volume_usdc": volume_usdc,
        "min_order_size": min_order_size,
        "_days_to_expiry": days_to_expiry,
    }


def _execute_features(**overrides) -> GateFeatures:
    """A GateFeatures that passes every gate — overridable per-test."""
    defaults = dict(
        similarity=0.80,
        freshness="BREAKING",
        article_present=True,
        market_present=True,
        entity_overlap_result=(True, 2),
        direction="YES",
        confidence=0.70,
        prediction_id=42,
        mid=0.50,
        executable_price=None,
        spread=None,
        market=_market(),
        cluster_size=2,
        cash_available=1000.0,
    )
    defaults.update(overrides)
    return GateFeatures(**defaults)


# ---------------------------------------------------------------------------
# Per-gate helpers — isolated reject paths.
# ---------------------------------------------------------------------------


def test_gate_similarity_floor_rejects_below_threshold():
    cfg = _Cfg(min_similarity=0.35)
    status, entry, reason = _gate_similarity_floor(0.20, cfg)
    assert status == "reject"
    assert reason == "similarity_floor"
    assert entry["gate"] == "similarity_floor"
    assert entry["outcome"] == "reject"
    assert entry["value"] == 0.20
    assert entry["threshold"] == 0.35


def test_gate_similarity_floor_passes_at_or_above():
    cfg = _Cfg(min_similarity=0.35)
    status, entry, reason = _gate_similarity_floor(0.35, cfg)
    assert status == "ok"
    assert reason is None
    assert entry["outcome"] == "ok"


def test_gate_freshness_rejects_stale():
    cfg = _Cfg()
    status, entry, reason = _gate_freshness("STALE_RETURN", cfg)
    assert status == "reject"
    assert reason == "freshness_stale"
    assert entry["gate"] == "freshness"


def test_gate_freshness_accepts_breaking_and_evolving():
    cfg = _Cfg()
    for label in ("BREAKING", "EVOLVING"):
        status, _entry, _reason = _gate_freshness(label, cfg)
        assert status == "ok"


def test_gate_entity_overlap_rejects_when_entities_present_but_no_match():
    status, entry, reason = _gate_entity_overlap(overlaps=False, n_entities=3)
    assert status == "reject"
    assert reason == "entity_mismatch"
    assert entry["value"] == 0
    assert entry["threshold"] == 3


def test_gate_entity_overlap_permissive_when_no_entities():
    """`n_entities == 0` means the extractor hasn't run yet — pass through."""
    status, entry, reason = _gate_entity_overlap(overlaps=False, n_entities=0)
    assert status == "ok"
    assert entry["value"] == "no_entities_yet"


def test_gate_entity_overlap_passes_on_match():
    status, entry, _reason = _gate_entity_overlap(overlaps=True, n_entities=2)
    assert status == "ok"
    assert entry["value"] == "match"


def test_gate_direction_rejects_neutral():
    cfg = _Cfg()
    status, _entry, reason = _gate_direction("NEUTRAL", 0.9, cfg)
    assert status == "reject"
    assert reason == "direction_neutral"


def test_gate_direction_rejects_low_confidence():
    cfg = _Cfg(min_confidence=0.55)
    status, _entry, reason = _gate_direction("YES", 0.40, cfg)
    assert status == "reject"
    assert reason == "direction_lowconf"


def test_gate_direction_passes_on_decisive_label():
    cfg = _Cfg(min_confidence=0.55)
    for d in ("YES", "NO"):
        status, _entry, _reason = _gate_direction(d, 0.70, cfg)
        assert status == "ok"


def test_gate_mid_degenerate_rejects_at_or_below_min_mid():
    cfg = _Cfg(min_mid=0.02, max_mid=0.98)
    for mid in (0.0, 0.01, 0.02):
        status, entry, reason = _gate_mid_degenerate(mid, cfg)
        assert status == "reject", f"mid={mid} should reject"
        assert reason == "mid_degenerate"
        assert entry["threshold"] == "mid_degenerate"


def test_gate_mid_degenerate_rejects_at_or_above_max_mid():
    cfg = _Cfg(min_mid=0.02, max_mid=0.98)
    for mid in (0.98, 0.99, 1.0):
        status, _entry, reason = _gate_mid_degenerate(mid, cfg)
        assert status == "reject", f"mid={mid} should reject"
        assert reason == "mid_degenerate"


def test_gate_mid_degenerate_passes_in_middle_without_appending_entry():
    cfg = _Cfg(min_mid=0.02, max_mid=0.98)
    status, entry, reason = _gate_mid_degenerate(0.50, cfg)
    assert status == "ok"
    assert reason is None
    # Pre-refactor cascade does NOT append a pass-entry for this gate.
    assert entry is None


def test_gate_wide_spread_rejects_above_threshold():
    cfg = _Cfg(wide_spread_threshold=0.10)
    status, _entry, reason = _gate_wide_spread(0.15, cfg)
    assert status == "reject"
    assert reason == "wide_spread"


def test_gate_wide_spread_skipped_when_book_unavailable():
    cfg = _Cfg(wide_spread_threshold=0.10)
    status, entry, reason = _gate_wide_spread(None, cfg)
    assert status == "ok"
    assert entry is None  # no append on skip


def test_gate_wide_spread_passes_at_threshold():
    cfg = _Cfg(wide_spread_threshold=0.10)
    status, entry, _reason = _gate_wide_spread(0.10, cfg)
    assert status == "ok"
    assert entry is None  # no append on pass


# ---------------------------------------------------------------------------
# Market-news imbalance sub-rules (cycle 7).
# ---------------------------------------------------------------------------


def test_imbalance_thin_market_rejects_below_volume_floor():
    cfg = _Cfg(min_market_volume_usdc=10_000.0)
    out = _eval_market_news_imbalance_subrules(
        market=_market(volume_usdc=5_000.0), mid=0.5, freshness="BREAKING",
        cluster_size=5, cfg=cfg,
    )
    assert out == ("thin_market", 5_000.0)


def test_imbalance_thin_market_skipped_when_volume_zero():
    """Defensive: a 0/NULL volume means the parser bug — don't reject."""
    cfg = _Cfg(min_market_volume_usdc=10_000.0)
    for v in (0.0, None):
        out = _eval_market_news_imbalance_subrules(
            market=_market(volume_usdc=v), mid=0.5, freshness="BREAKING",
            cluster_size=5, cfg=cfg,
        )
        assert out is None


def test_imbalance_extreme_mid_rejects_when_not_breaking():
    cfg = _Cfg()
    out = _eval_market_news_imbalance_subrules(
        market=_market(), mid=0.92, freshness="EVOLVING",
        cluster_size=5, cfg=cfg,
    )
    assert out == ("extreme_mid_no_breaking", 0.92)


def test_imbalance_extreme_mid_bypassed_when_breaking():
    cfg = _Cfg()
    out = _eval_market_news_imbalance_subrules(
        market=_market(), mid=0.92, freshness="BREAKING",
        cluster_size=5, cfg=cfg,
    )
    assert out is None


def test_imbalance_near_expiry_rejects_regardless_of_freshness():
    cfg = _Cfg(min_days_to_expiry=1)
    out = _eval_market_news_imbalance_subrules(
        market=_market(days_to_expiry=0), mid=0.5, freshness="BREAKING",
        cluster_size=5, cfg=cfg,
    )
    assert out == ("near_expiry", 0)


def test_imbalance_far_future_rejects_when_not_breaking():
    cfg = _Cfg(far_future_days=180)
    out = _eval_market_news_imbalance_subrules(
        market=_market(days_to_expiry=400), mid=0.5, freshness="EVOLVING",
        cluster_size=5, cfg=cfg,
    )
    assert out == ("far_future_no_breaking", 400)


def test_imbalance_far_future_bypassed_when_breaking():
    cfg = _Cfg(far_future_days=180)
    out = _eval_market_news_imbalance_subrules(
        market=_market(days_to_expiry=400), mid=0.5, freshness="BREAKING",
        cluster_size=5, cfg=cfg,
    )
    assert out is None


def test_imbalance_singleton_extreme_rejects():
    cfg = _Cfg()
    out = _eval_market_news_imbalance_subrules(
        market=_market(), mid=0.10, freshness="BREAKING",
        cluster_size=1, cfg=cfg,
    )
    assert out == ("singleton_on_extreme_mid", 0.10)


def test_imbalance_singleton_in_middle_passes():
    cfg = _Cfg()
    out = _eval_market_news_imbalance_subrules(
        market=_market(), mid=0.50, freshness="BREAKING",
        cluster_size=1, cfg=cfg,
    )
    assert out is None


# ---------------------------------------------------------------------------
# Edge + Kelly arithmetic.
# ---------------------------------------------------------------------------


def test_compute_edge_uses_executable_path_when_book_available():
    cfg = _Cfg(taker_fee_buffer=0.01)
    edge, comp = _compute_edge(
        direction="YES", p_hat=0.70, mid=0.50,
        executable_price=0.51, spread=0.02, cfg=cfg,
    )
    # p_hat - executable_price - taker_fee_buffer - half_spread
    assert edge == pytest.approx(0.70 - 0.51 - 0.01 - 0.01)
    assert comp["fallback"] is False
    assert comp["half_spread"] == pytest.approx(0.01)


def test_compute_edge_falls_back_when_no_book():
    cfg = _Cfg(fee_buffer=0.02)
    edge, comp = _compute_edge(
        direction="YES", p_hat=0.70, mid=0.50,
        executable_price=None, spread=None, cfg=cfg,
    )
    assert edge == pytest.approx(0.70 - 0.50 - 0.02)
    assert comp["fallback"] is True


def test_compute_edge_no_side_with_book():
    """NO buy edge in YES-price units: edge = executable - p_hat - costs."""
    cfg = _Cfg(taker_fee_buffer=0.01)
    edge, _comp = _compute_edge(
        direction="NO", p_hat=0.30, mid=0.50,
        executable_price=0.48, spread=0.02, cfg=cfg,
    )
    assert edge == pytest.approx(0.48 - 0.30 - 0.01 - 0.01)


def test_compute_edge_no_side_fallback():
    cfg = _Cfg(fee_buffer=0.02)
    edge, _comp = _compute_edge(
        direction="NO", p_hat=0.30, mid=0.50,
        executable_price=None, spread=None, cfg=cfg,
    )
    # YES-mid convention: edge_NO_fallback = mid - p_hat - fee_buffer
    assert edge == pytest.approx(0.50 - 0.30 - 0.02)


def test_compute_kelly_caps_at_max_position_fraction():
    cfg = _Cfg(
        balance_usdc=1000.0, kelly_fraction=1.0, max_position_fraction=0.10,
    )
    # Huge edge — Kelly says "bet everything" — cap kicks in at 10% × balance.
    _full, _capped, size = _compute_kelly_and_size(
        direction="YES", p_hat=0.95, mid=0.10, cfg=cfg,
    )
    assert size == pytest.approx(100.0)  # 10% × 1000


def test_gate_size_uses_per_market_floor_when_higher():
    cfg = _Cfg(min_position_usdc=1.0)
    status, entry, reason, floor = _gate_size(
        size=15.0, market_floor_usdc=20.0, cfg=cfg,
    )
    assert status == "reject"
    assert reason == "below_market_min"
    assert floor == 20.0
    assert entry["threshold"] == 20.0


def test_gate_size_uses_config_floor_when_no_per_market():
    cfg = _Cfg(min_position_usdc=25.0)
    status, _entry, reason, floor = _gate_size(
        size=10.0, market_floor_usdc=None, cfg=cfg,
    )
    assert status == "reject"
    assert reason == "size_below_min"
    assert floor == 25.0


def test_gate_size_passes():
    cfg = _Cfg(min_position_usdc=1.0)
    status, _entry, reason, floor = _gate_size(
        size=50.0, market_floor_usdc=10.0, cfg=cfg,
    )
    assert status == "ok"
    assert reason is None
    assert floor == 10.0


def test_gate_cash_rejects_when_empty():
    cfg = _Cfg(min_position_usdc=1.0)
    status, entries, reason, _final = _gate_cash(
        size=50.0, cash_available=0.0, effective_floor=1.0,
        market_floor_usdc=None, cfg=cfg,
    )
    assert status == "reject"
    assert reason == "insufficient_cash"
    assert len(entries) == 1
    assert entries[0]["outcome"] == "reject"


def test_gate_cash_clips_and_passes_when_above_floor():
    """Cycle 3 clip-to-cash. Post-clip size must clear the effective floor
    for the trade to proceed."""
    cfg = _Cfg(min_position_usdc=10.0)
    status, entries, reason, final_size = _gate_cash(
        size=200.0, cash_available=50.0, effective_floor=10.0,
        market_floor_usdc=None, cfg=cfg,
    )
    assert status == "ok"
    assert reason is None
    assert final_size == 50.0
    # ONE entry — the clip — and no separate "ok" entry on this path.
    assert len(entries) == 1
    assert entries[0]["outcome"] == "clip"


def test_gate_cash_clips_then_rejects_when_below_floor():
    cfg = _Cfg(min_position_usdc=10.0)
    status, entries, reason, final_size = _gate_cash(
        size=200.0, cash_available=5.0, effective_floor=10.0,
        market_floor_usdc=None, cfg=cfg,
    )
    assert status == "reject"
    assert reason == "size_below_min"
    assert final_size == 5.0
    assert len(entries) == 2
    assert entries[0]["outcome"] == "clip"
    assert entries[1]["outcome"] == "reject"


def test_gate_cash_no_clip_path_appends_ok_entry():
    cfg = _Cfg(min_position_usdc=10.0)
    status, entries, reason, final_size = _gate_cash(
        size=20.0, cash_available=500.0, effective_floor=10.0,
        market_floor_usdc=None, cfg=cfg,
    )
    assert status == "ok"
    assert reason is None
    assert final_size == 20.0
    assert len(entries) == 1
    assert entries[0]["outcome"] == "ok"


# ---------------------------------------------------------------------------
# evaluate_gates — end-to-end pure cascade.
# ---------------------------------------------------------------------------


def test_evaluate_gates_executes_on_clean_path():
    cfg = _Cfg()
    result = evaluate_gates(_execute_features(), cfg)
    assert result.outcome == "execute"
    assert result.reason == "ok"
    assert result.direction == "YES"
    assert result.confidence == 0.70
    assert result.p_hat == pytest.approx(0.70)
    assert result.mid == 0.50
    assert result.size_usdc > 0
    assert result.prediction_id == 42
    # Same 7-gate count the integration test asserts (similarity_floor,
    # freshness, entity_overlap, direction, edge, size, cash).
    assert len(result.gate_history) == 7
    names = [g["gate"] for g in result.gate_history]
    assert names == [
        "similarity_floor", "freshness", "entity_overlap",
        "direction", "edge", "size", "cash",
    ]


def test_evaluate_gates_short_circuits_at_first_reject():
    cfg = _Cfg()
    result = evaluate_gates(_execute_features(similarity=0.10), cfg)
    assert result.outcome == "reject"
    assert result.reason == "similarity_floor"
    # No further gates evaluated after the first reject.
    assert len(result.gate_history) == 1


def test_evaluate_gates_propagates_freshness_reject():
    cfg = _Cfg()
    result = evaluate_gates(_execute_features(freshness="STALE_RETURN"), cfg)
    assert result.reason == "freshness_stale"
    assert len(result.gate_history) == 2


def test_evaluate_gates_handles_article_missing():
    cfg = _Cfg()
    result = evaluate_gates(_execute_features(article_present=False), cfg)
    assert result.reason == "article_missing"


def test_evaluate_gates_handles_market_missing():
    cfg = _Cfg()
    result = evaluate_gates(_execute_features(market_present=False), cfg)
    assert result.reason == "market_missing"


def test_evaluate_gates_propagates_entity_mismatch():
    cfg = _Cfg()
    result = evaluate_gates(
        _execute_features(entity_overlap_result=(False, 3)), cfg,
    )
    assert result.reason == "entity_mismatch"


def test_evaluate_gates_skips_entity_gate_when_unset():
    cfg = _Cfg()
    result = evaluate_gates(
        _execute_features(entity_overlap_result=None), cfg,
    )
    assert result.outcome == "execute"
    # entity_overlap absent from gate_history when caller didn't compute it.
    names = [g["gate"] for g in result.gate_history]
    assert "entity_overlap" not in names
    assert len(result.gate_history) == 6  # 7 - entity_overlap


def test_evaluate_gates_propagates_neutral_direction():
    cfg = _Cfg()
    result = evaluate_gates(_execute_features(direction="NEUTRAL"), cfg)
    assert result.reason == "direction_neutral"
    assert result.prediction_id == 42  # prediction_id still carried through


def test_evaluate_gates_propagates_low_confidence():
    cfg = _Cfg(min_confidence=0.55)
    result = evaluate_gates(_execute_features(confidence=0.40), cfg)
    assert result.reason == "direction_lowconf"


def test_evaluate_gates_propagates_mid_degenerate():
    cfg = _Cfg()
    result = evaluate_gates(_execute_features(mid=0.99), cfg)
    assert result.reason == "mid_degenerate"


def test_evaluate_gates_propagates_wide_spread():
    cfg = _Cfg(wide_spread_threshold=0.05)
    result = evaluate_gates(
        _execute_features(executable_price=0.51, spread=0.10), cfg,
    )
    assert result.reason == "wide_spread"


def test_evaluate_gates_propagates_imbalance():
    cfg = _Cfg()
    result = evaluate_gates(
        _execute_features(mid=0.92, freshness="EVOLVING"), cfg,
    )
    assert result.reason == "market_news_imbalance"


def test_evaluate_gates_propagates_edge_below_min():
    cfg = _Cfg(min_edge=0.20, fee_buffer=0.02)
    result = evaluate_gates(_execute_features(), cfg)
    # p_hat=0.70, mid=0.50, fee=0.02 → edge=0.18 < min_edge 0.20
    assert result.reason == "edge_below_min"


def test_evaluate_gates_propagates_size_below_min():
    cfg = _Cfg(min_position_usdc=200.0, max_position_fraction=0.05)
    # max_position_fraction × balance = 50, < min_position_usdc 200 → reject.
    result = evaluate_gates(_execute_features(), cfg)
    assert result.reason == "size_below_min"


def test_evaluate_gates_propagates_insufficient_cash():
    cfg = _Cfg()
    result = evaluate_gates(_execute_features(cash_available=0.0), cfg)
    assert result.reason == "insufficient_cash"


def test_evaluate_gates_clips_to_cash_when_above_floor():
    cfg = _Cfg(min_position_usdc=1.0)
    # size from Kelly will be larger than the cash available; we should
    # CLIP (not reject) and execute at the clipped size.
    result = evaluate_gates(_execute_features(cash_available=10.0), cfg)
    assert result.outcome == "execute"
    assert result.size_usdc == 10.0
    # Cash gate appended one "clip" entry, no "ok" entry.
    cash_entries = [g for g in result.gate_history if g["gate"] == "cash"]
    assert len(cash_entries) == 1
    assert cash_entries[0]["outcome"] == "clip"


def test_evaluate_gates_raises_when_direction_missing():
    """Wiring contract: caller must populate LLM result before gate 3."""
    cfg = _Cfg()
    with pytest.raises(ValueError, match="direction"):
        evaluate_gates(
            _execute_features(direction=None, confidence=None), cfg,
        )


def test_evaluate_gates_rejects_when_mid_missing():
    """Cycle 23: pre-cycle-23 this raised ValueError (caller wiring
    contract). Now mid=None is a first-class reject mirroring the live
    `_gate_mid_unavailable` path so replay reproduces production behaviour."""
    cfg = _Cfg()
    result = evaluate_gates(_execute_features(mid=None), cfg)
    assert result.outcome == "reject"
    assert result.reason == "mid_unavailable"


def test_evaluate_gates_raises_when_market_missing_from_features():
    """Distinct from `market_present=False` — that's a presence reject;
    this is a wiring bug (features.market is None when imbalance gate is reached)."""
    cfg = _Cfg()
    with pytest.raises(ValueError, match="market"):
        evaluate_gates(_execute_features(market=None), cfg)


def test_evaluate_gates_raises_when_cash_missing():
    cfg = _Cfg()
    with pytest.raises(ValueError, match="cash"):
        evaluate_gates(_execute_features(cash_available=None), cfg)


def test_evaluate_gates_no_direction_executes_correctly():
    cfg = _Cfg()
    # NO direction: p_hat = 1 - confidence = 0.3. mid=0.50, fallback fee=0.02.
    # edge = mid - p_hat - fee = 0.50 - 0.30 - 0.02 = 0.18 > min_edge 0.04.
    result = evaluate_gates(_execute_features(direction="NO"), cfg)
    assert result.outcome == "execute"
    assert result.direction == "NO"
    assert result.p_hat == pytest.approx(0.30)
    assert result.edge == pytest.approx(0.18)

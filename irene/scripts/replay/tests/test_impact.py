"""Tests for `scripts/replay/impact.py`."""

from __future__ import annotations

import pytest

from scripts.replay.impact import (
    BUCKET_LABELS,
    BUCKET_SECONDS,
    _first_half_seconds,
    _median,
    _pearson,
    _percentile,
    run_inproc,
)


def test_median_empty():
    assert _median([]) is None
    assert _median([None, None]) is None


def test_median_ignores_none():
    assert _median([1.0, None, 3.0]) == 2.0


def test_percentile_basic():
    assert _percentile([0.0, 1.0, 2.0, 3.0, 4.0], 50.0) == 2.0
    assert _percentile([0.0, 1.0, 2.0, 3.0, 4.0], 25.0) == 1.0
    assert _percentile([0.0, 1.0, 2.0, 3.0, 4.0], 75.0) == 3.0


def test_pearson_perfect_correlation():
    r = _pearson([1.0, 2.0, 3.0, 4.0], [2.0, 4.0, 6.0, 8.0])
    assert r == pytest.approx(1.0)


def test_pearson_negative():
    r = _pearson([1.0, 2.0, 3.0, 4.0], [4.0, 3.0, 2.0, 1.0])
    assert r == pytest.approx(-1.0)


def test_pearson_insufficient_data():
    assert _pearson([1.0, 2.0], [1.0, 2.0]) is None
    assert _pearson([1.0, 2.0, 3.0], [None, None, None]) is None


def test_first_half_seconds_basic():
    # entry_mid=0.40, p_hat=0.80 → full target=0.40, threshold=0.20
    pi = {
        "entry_mid": 0.40,
        "edge_realised_t60s": 0.05,
        "edge_realised_t300s": 0.15,
        "edge_realised_t900s": 0.25,  # first to cross 0.20
        "edge_realised_t1800s": 0.30,
        "edge_realised_t3600s": 0.40,
        "edge_realised_t7200s": None,
        "edge_realised_t14400s": None,
        "edge_realised_t28800s": None,
        "edge_realised_t86400s": None,
    }
    assert _first_half_seconds(pi, p_hat=0.80) == 900


def test_first_half_seconds_never_reaches():
    pi = {
        "entry_mid": 0.40,
        **{f"edge_realised_t{sec}s": 0.01 for sec in BUCKET_SECONDS},
    }
    assert _first_half_seconds(pi, p_hat=0.80) is None


def test_first_half_seconds_returns_none_if_p_hat_missing():
    pi = {"entry_mid": 0.40, **{f"edge_realised_t{sec}s": 1.0 for sec in BUCKET_SECONDS}}
    assert _first_half_seconds(pi, p_hat=None) is None


def test_run_inproc_counts(snapshot_dir):
    rep = run_inproc(str(snapshot_dir))
    assert rep.n_trades_total == 3
    assert rep.n_trades_with_impact == 2


def test_run_inproc_overall_buckets(snapshot_dir):
    rep = run_inproc(str(snapshot_dir))
    # 9 buckets present whether populated or not.
    assert len(rep.overall.buckets) == 9
    labels = [b.label for b in rep.overall.buckets]
    assert labels == BUCKET_LABELS

    # 1-hour bucket: trade1 edge=0.05, trade2 edge=0.05 → median=0.05.
    one_h = next(b for b in rep.overall.buckets if b.label == "1h")
    assert one_h.n == 2
    assert one_h.median_edge_realised == pytest.approx(0.05)


def test_run_inproc_by_freshness_split(snapshot_dir):
    rep = run_inproc(str(snapshot_dir))
    names = {s.name for s in rep.by_freshness}
    assert names == {"BREAKING", "EVOLVING"}
    breaking = next(s for s in rep.by_freshness if s.name == "BREAKING")
    assert breaking.n_trades == 1  # only trade 1 (BREAKING + has impact)


def test_run_inproc_by_category_uses_pinned_market(snapshot_dir):
    rep = run_inproc(str(snapshot_dir))
    names = {s.name for s in rep.by_category}
    # trade1 (politics), trade2 (politics) — trade3 (crypto) has no impact.
    assert names == {"politics"}
    politics = next(s for s in rep.by_category if s.name == "politics")
    assert politics.n_trades == 2


def test_run_inproc_pre_news_drift_correlation(snapshot_dir):
    """With only n=2 trades carrying drift data, correlation must return
    None — the small-n threshold is n>=3."""
    rep = run_inproc(str(snapshot_dir))
    assert rep.pre_news_drift_correlation_t3600s_r is None
    assert rep.pre_news_drift_correlation_t3600s_n == 2


def test_run_inproc_to_dict_is_serializable(snapshot_dir):
    import json
    rep = run_inproc(str(snapshot_dir))
    blob = json.dumps(rep.to_dict())
    rt = json.loads(blob)
    assert rt["n_trades_total"] == 3

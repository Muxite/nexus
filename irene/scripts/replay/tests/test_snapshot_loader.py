"""Tests for `scripts/replay/_snapshot_loader.py`."""

from __future__ import annotations

import pytest

from scripts.replay._snapshot_loader import SnapshotLoader


def test_loader_rejects_missing_dir(tmp_path):
    with pytest.raises(FileNotFoundError):
        SnapshotLoader(tmp_path / "does_not_exist")


def test_loader_rejects_dir_without_manifest(tmp_path):
    (tmp_path / "empty_snap").mkdir()
    with pytest.raises(FileNotFoundError, match="manifest"):
        SnapshotLoader(tmp_path / "empty_snap")


def test_manifest_loads(snapshot_dir):
    loader = SnapshotLoader(snapshot_dir)
    m = loader.manifest
    assert m["cycle_id"] == 99
    assert m["cycle_name"] == "test"
    assert m["wallet"]["n_trades"] == 3


def test_trades_loaded(snapshot_dir):
    loader = SnapshotLoader(snapshot_dir)
    assert len(loader.trades) == 3
    assert {t["id"] for t in loader.trades} == {1, 2, 3}


def test_indexes_built(snapshot_dir):
    loader = SnapshotLoader(snapshot_dir)
    assert set(loader.trades_by_id) == {1, 2, 3}
    assert set(loader.closures_by_trade_id) == {1, 2}  # trade 3 not closed
    assert set(loader.impacts_by_trade_id) == {1, 2}   # trade 3 has no impact
    assert set(loader.signals_by_id) == {10, 20, 30}
    assert set(loader.predictions_by_id) == {101, 102, 103}
    assert set(loader.decisions_by_signal_id) == {10, 20, 30}
    assert set(loader.decision_inputs_by_signal_ref) == {10, 20, 30}


def test_predictions_by_prompt_sha(snapshot_dir):
    loader = SnapshotLoader(snapshot_dir)
    assert set(loader.predictions_by_prompt_sha) == {"s1", "s2", "s3"}
    assert loader.predictions_by_prompt_sha["s1"]["direction"] == "YES"


def test_fired_trades_excludes_tracers(snapshot_dir):
    loader = SnapshotLoader(snapshot_dir)
    assert len(loader.fired_trades()) == 3  # all non-tracer in fixture


def test_closed_trades(snapshot_dir):
    loader = SnapshotLoader(snapshot_dir)
    pairs = loader.closed_trades()
    assert len(pairs) == 2  # trades 1 + 2 closed; trade 3 open
    ids = sorted(t["id"] for t, _ in pairs)
    assert ids == [1, 2]


def test_trade_with_impact(snapshot_dir):
    loader = SnapshotLoader(snapshot_dir)
    pairs = loader.trade_with_impact()
    assert len(pairs) == 2  # only trades 1 + 2 have impact rows
    ids = sorted(t["id"] for t, _ in pairs)
    assert ids == [1, 2]


def test_features_snapshot_decodes_json(snapshot_dir):
    loader = SnapshotLoader(snapshot_dir)
    trade = loader.trades_by_id[1]
    fs = loader.features_snapshot(trade)
    assert fs is not None
    assert fs["direction"] == "YES"
    assert fs["confidence"] == 0.80
    assert "market_at_trade" in fs
    assert fs["market_at_trade"]["category"] == "politics"


def test_features_snapshot_handles_missing(snapshot_dir):
    loader = SnapshotLoader(snapshot_dir)
    assert loader.features_snapshot({}) is None
    assert loader.features_snapshot({"features_snapshot": "{not valid json"}) is None


def test_caching_is_idempotent(snapshot_dir):
    """`cached_property` should ensure repeated access returns the same
    object (no re-parse)."""
    loader = SnapshotLoader(snapshot_dir)
    a = loader.trades
    b = loader.trades
    assert a is b

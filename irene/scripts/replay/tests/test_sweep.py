"""Tests for `scripts/replay/sweep.py`."""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from scripts.replay.sweep import (
    _cartesian,
    _parse_sweep_flag,
    _write_csv,
    run_inproc,
)


def test_parse_sweep_basic():
    key, values = _parse_sweep_flag("trade.min_edge=0.02,0.04,0.06")
    assert key == "trade.min_edge"
    assert values == [0.02, 0.04, 0.06]


def test_parse_sweep_missing_equals():
    with pytest.raises(ValueError, match="missing '='"):
        _parse_sweep_flag("trade.min_edge")


def test_parse_sweep_empty_values():
    with pytest.raises(ValueError, match="no values"):
        _parse_sweep_flag("trade.min_edge=")


def test_cartesian_empty():
    assert list(_cartesian({})) == [{}]


def test_cartesian_single_dim():
    combos = list(_cartesian({"k": [1, 2, 3]}))
    assert len(combos) == 3
    assert combos == [{"k": 1}, {"k": 2}, {"k": 3}]


def test_cartesian_two_dims():
    combos = list(_cartesian({"a": [1, 2], "b": [10, 20]}))
    assert len(combos) == 4
    assert {"a": 1, "b": 10} in combos
    assert {"a": 2, "b": 20} in combos


def test_run_inproc_single_snapshot_single_sweep(snapshot_dir):
    rows = run_inproc([str(snapshot_dir)], {"trade.min_edge": [0.04, 0.35]})
    assert len(rows) == 2
    # Sorted by sweep value: 0.04 → all 3 fire; 0.35 → only 1 fires.
    by_edge = {r["trade.min_edge"]: r for r in rows}
    assert by_edge[0.04]["bucket_a_n"] == 3
    assert by_edge[0.35]["bucket_a_n"] == 1
    assert by_edge[0.35]["bucket_b_n"] == 2


def test_run_inproc_multi_snapshot_cartesian(snapshot_dir, tmp_path):
    # Duplicate the fixture dir for a second snapshot to exercise multi-snap.
    import shutil
    dup = tmp_path / "dup_snap"
    shutil.copytree(snapshot_dir, dup)
    rows = run_inproc(
        [str(snapshot_dir), str(dup)],
        {"trade.min_edge": [0.04, 0.35]},
    )
    # 2 snapshots × 2 sweep values = 4 rows
    assert len(rows) == 4
    snaps = {r["snapshot"] for r in rows}
    assert snaps == {str(snapshot_dir), str(dup)}


def test_run_inproc_no_sweeps_acts_as_single_run(snapshot_dir):
    rows = run_inproc([str(snapshot_dir)], {})
    assert len(rows) == 1
    assert rows[0]["bucket_a_n"] == 3


def test_csv_writer_round_trip(tmp_path, snapshot_dir):
    rows = run_inproc([str(snapshot_dir)], {"trade.min_edge": [0.04, 0.35]})
    out = tmp_path / "sweep.csv"
    _write_csv(rows, out)
    assert out.exists()
    read = list(csv.DictReader(out.open()))
    assert len(read) == 2
    # Schema sanity
    assert "trade.min_edge" in read[0]
    assert "net_pnl_delta" in read[0]


def test_csv_writer_handles_empty(tmp_path):
    out = tmp_path / "empty.csv"
    _write_csv([], out)
    # Empty → no file written / nothing crashes. Either behavior is fine.


def test_sweep_smoke_default_equals_zero_override(snapshot_dir):
    """A sweep with a single value at the default reproduces the
    zero-override predict.py run row-for-row."""
    from scripts.replay.predict import run_inproc as predict_run
    zero = predict_run(str(snapshot_dir))
    swept = run_inproc(
        [str(snapshot_dir)], {"trade.min_edge": [0.04]},
    )[0]
    assert swept["bucket_a_n"] == zero.bucket_a.n
    assert swept["bucket_b_n"] == zero.bucket_b.n
    assert swept["decision_changes"] == zero.counterfactual_decision_changes

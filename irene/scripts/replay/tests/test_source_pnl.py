"""Tests for `scripts/replay/source_pnl.py`."""

from __future__ import annotations

import json

import pytest

from scripts.replay.source_pnl import (
    SourcePnlReport,
    SourceStats,
    _gate_history_reached,
    _median,
    run_inproc,
)


def test_median_helpers():
    assert _median([]) is None
    assert _median([None]) is None
    assert _median([1.0, 3.0]) == 2.0


def test_gate_history_reached_finds_gate():
    hist = json.dumps([{"gate": "direction", "outcome": "ok"}])
    assert _gate_history_reached(hist, "direction") is True
    assert _gate_history_reached(hist, "edge") is False


def test_gate_history_reached_handles_bad_json():
    assert _gate_history_reached("{not valid", "direction") is False
    assert _gate_history_reached("", "direction") is False


def test_run_inproc_groups_signals_by_source(snapshot_dir):
    """Fixture has 3 signals from articles 1/2/3; sources are
    reuters, axios, reuters → 2 reuters, 1 axios."""
    rep = run_inproc(str(snapshot_dir))
    assert rep.n_signals_total == 3
    assert rep.n_sources == 2  # reuters + axios
    by_name = {s.source: s for s in rep.by_source}
    assert by_name["reuters"].n_signals == 2
    assert by_name["axios"].n_signals == 1


def test_run_inproc_attributes_pnl_to_correct_source(snapshot_dir):
    """Trade 1 (reuters) closed +25, trade 2 (axios) closed +10,
    trade 3 (reuters) open. reuters total = +25 closed; axios = +10."""
    rep = run_inproc(str(snapshot_dir))
    by_name = {s.source: s for s in rep.by_source}
    assert by_name["reuters"].net_pnl_total == pytest.approx(25.0)
    assert by_name["axios"].net_pnl_total == pytest.approx(10.0)
    assert by_name["reuters"].n_trades_open == 1
    assert by_name["reuters"].n_trades_closed == 1
    assert by_name["axios"].n_trades_closed == 1


def test_run_inproc_records_notional(snapshot_dir):
    rep = run_inproc(str(snapshot_dir))
    by_name = {s.source: s for s in rep.by_source}
    # reuters: trades 1 (100) + 3 (50) = 150 notional
    # axios:   trade 2 (80) = 80 notional
    assert by_name["reuters"].notional_total == pytest.approx(150.0)
    assert by_name["axios"].notional_total == pytest.approx(80.0)


def test_run_inproc_includes_impact_edge_realised(snapshot_dir):
    rep = run_inproc(str(snapshot_dir))
    by_name = {s.source: s for s in rep.by_source}
    # reuters: trade 1 has impact_t3600s=0.05; trade 3 has no impact.
    # axios: trade 2 has impact_t3600s=0.05.
    assert by_name["reuters"].median_edge_realised_t3600s == pytest.approx(0.05)
    assert by_name["axios"].median_edge_realised_t3600s == pytest.approx(0.05)


def test_run_inproc_sorts_by_signal_count(snapshot_dir):
    rep = run_inproc(str(snapshot_dir))
    # reuters (2) before axios (1)
    assert rep.by_source[0].source == "reuters"
    assert rep.by_source[1].source == "axios"


def test_run_inproc_to_dict_serialisable(snapshot_dir):
    rep = run_inproc(str(snapshot_dir))
    blob = json.dumps(rep.to_dict())
    rt = json.loads(blob)
    assert rt["n_signals_total"] == 3


def test_unknown_source_when_articles_missing(snapshot_dir, tmp_path):
    """If articles.jsonl is empty / missing, every signal falls into
    the UNKNOWN source bucket. PR-F is backward-compat with snapshots
    that predate the articles dump."""
    import shutil
    dup = tmp_path / "no_articles"
    shutil.copytree(snapshot_dir, dup)
    (dup / "articles.jsonl").write_text("")  # truncate
    rep = run_inproc(str(dup))
    assert rep.n_sources == 1
    assert rep.by_source[0].source == "UNKNOWN"

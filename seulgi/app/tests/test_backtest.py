"""Tests for the offline PnL backtest replay (Phase 2 A2.3+A2.4).

Builds a small fixture DB with `markets`, `signals`, `engine_results`, and
`market_price_snaps` rows, then drives `scripts.replay.backtest.main()`
end-to-end. Validates:

1. Smoke run produces a non-empty summary.md + manifest.json + trades file.
2. Variant overlay (tighter `min_confidence`) drops the simulated-trade
   count, proving the new config actually overrides what `_evaluate_pure`
   reads.
3. Oracle fallback fires when `decision_inputs` is empty.
4. `--max-headlines` correctly caps the run.
5. Manifest carries the right metadata.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path

import pytest
import yaml

_HERE = os.path.dirname(__file__)
_SEULGI = os.path.normpath(os.path.join(_HERE, "..", ".."))
sys.path.insert(0, os.path.join(_SEULGI, "scripts", "replay"))
sys.path.insert(0, os.path.join(_SEULGI, "app"))

import backtest as bt  # noqa: E402


# ---------------------------------------------------------------------------
# Fixture builder
# ---------------------------------------------------------------------------


_SCHEMA = """
CREATE TABLE markets (
    condition_id TEXT PRIMARY KEY,
    question     TEXT NOT NULL,
    token_yes    TEXT,
    token_no     TEXT,
    end_date     TEXT,
    volume_usdc  REAL,
    embedding    BLOB,
    fetched_at   TEXT NOT NULL,
    market_type  TEXT
);
CREATE TABLE signals (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    site_url     TEXT NOT NULL,
    headline     TEXT NOT NULL,
    detected_at  TEXT NOT NULL,
    headline_id  TEXT,
    freshness_label    TEXT,
    nearest_age_secs   REAL
);
CREATE TABLE engine_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    engine_name     TEXT NOT NULL,
    condition_id    TEXT NOT NULL,
    question        TEXT NOT NULL DEFAULT '',
    direction       TEXT NOT NULL,
    confidence      REAL NOT NULL,
    similarity      REAL NOT NULL,
    p_hat           REAL NOT NULL,
    mid_price       REAL,
    edge            REAL NOT NULL DEFAULT 0.0,
    size_usdc       REAL NOT NULL,
    executed        INTEGER NOT NULL DEFAULT 0,
    skip_reason     TEXT,
    decided_at      TEXT NOT NULL,
    headline_id     TEXT,
    signal_latency_ms REAL
);
CREATE TABLE decision_inputs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    headline_id TEXT NOT NULL,
    decided_at TEXT NOT NULL,
    source_host TEXT,
    embedding_bytes BLOB,
    matches_json TEXT,
    freshness_label TEXT,
    freshness_dist REAL,
    freshness_age_secs REAL,
    pre_ai_skips_json TEXT,
    max_similarity REAL
);
CREATE TABLE market_price_snaps (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    condition_id TEXT NOT NULL,
    token_id     TEXT,
    side         TEXT NOT NULL,
    price        REAL NOT NULL,
    t_unix       INTEGER NOT NULL,
    source       TEXT NOT NULL DEFAULT 'prefetch',
    headline_id  TEXT
);
CREATE INDEX idx_mps_condition_t ON market_price_snaps(condition_id, t_unix);
"""


def _build_fixture_db(path: str, n_headlines: int = 6,
                     trade_confidence: float = 0.9) -> dict:
    """Build a small DB. Returns the (condition_id, headline_ids) used so
    tests can assert against them."""
    if os.path.exists(path):
        os.unlink(path)
    con = sqlite3.connect(path)
    con.executescript(_SCHEMA)
    con.execute(
        "INSERT INTO markets (condition_id, question, token_yes, token_no, "
        "fetched_at) VALUES (?, ?, ?, ?, ?)",
        ("0xABC", "Will it rain?", "tok_yes", "tok_no", "2026-05-22T00:00:00+00:00"),
    )
    base_unix = 1779400000  # ~2026-05-22 evening UTC
    headline_ids: list = []
    for i in range(n_headlines):
        decided_at = f"2026-05-22T20:0{i}:00+00:00"
        hid = f"hid_{i:03d}"
        headline_ids.append(hid)
        con.execute(
            "INSERT INTO signals (site_url, headline, detected_at, headline_id, "
            "freshness_label, nearest_age_secs) VALUES (?, ?, ?, ?, ?, ?)",
            ("https://x", f"H{i}", decided_at, hid, "BREAKING", 100.0),
        )
        # engine_results row: direction=YES, conf=0.9, mid=0.50 → edge=0.12 > 0.04.
        con.execute(
            "INSERT INTO engine_results (engine_name, condition_id, question, "
            "direction, confidence, similarity, p_hat, mid_price, edge, "
            "size_usdc, executed, skip_reason, decided_at, headline_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("test_engine/default", "0xABC", "Will it rain?", "YES",
             trade_confidence, 0.6, 0.62, 0.50, 0.12, 50.0, 1, None,
             decided_at, hid),
        )
        # Price snaps: entry mid=0.50, +5m at 0.55 (up 5¢ — winning trade).
        snap_unix = base_unix + i * 60
        for dt, p in [(0, 0.50), (300, 0.55), (900, 0.58), (1800, 0.60),
                      (3600, 0.62), (14400, 0.65)]:
            con.execute(
                "INSERT INTO market_price_snaps (condition_id, side, price, "
                "t_unix, source) VALUES (?, ?, ?, ?, ?)",
                ("0xABC", "yes", p, snap_unix + dt, "test"),
            )
        # decided_unix must match what `bt._parse_decided` produces.
        # Override the snap base to actual decided unix for accuracy.
    con.commit()
    # Re-anchor snaps to the actual decided_at unix from signals.
    con.execute("DELETE FROM market_price_snaps")
    for i, hid in enumerate(headline_ids):
        decided_at = f"2026-05-22T20:0{i}:00+00:00"
        decided_unix = bt._parse_decided(decided_at)
        for dt, p in [(0, 0.50), (300, 0.55), (900, 0.58), (1800, 0.60),
                      (3600, 0.62), (14400, 0.65)]:
            con.execute(
                "INSERT INTO market_price_snaps (condition_id, side, price, "
                "t_unix, source) VALUES (?, ?, ?, ?, ?)",
                ("0xABC", "yes", p, decided_unix + dt, "test"),
            )
    con.commit()
    con.close()
    return {"condition_id": "0xABC", "headline_ids": headline_ids}


def _write_config(path: str, *, min_confidence: float = 0.45) -> str:
    cfg = {
        "trading": {
            "balance_usdc": 1000.0,
            "max_position_usdc": 100.0,
            "min_trade_usdc": 1.0,
            "min_edge": 0.04,
            "edge_increment": 0.12,
            "position_timeout_hours": 24.0,
            "max_open_positions": 20,
            "entry_rail_skip_threshold": 0.0,
            "rail_skip_threshold": 0.0,
            "take_profit": 0.05,
            "stop_loss": 0.05,
            "spread_cost_bps": 0.0,
            "infinite_balance": True,
            "mode": "paper",
        },
        "ai": {"min_confidence": min_confidence, "min_agreement": 0.0,
               "ensemble_samples": 1},
        "engines": [{"name": "test_engine", "variants": [{"name": "default"}]}],
    }
    with open(path, "w") as f:
        yaml.safe_dump(cfg, f)
    return path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.fixture
def fixture_db(tmp_path):
    db_path = str(tmp_path / "src.db")
    info = _build_fixture_db(db_path)
    return db_path, info


@pytest.fixture
def variant_config(tmp_path):
    return _write_config(str(tmp_path / "variant.yaml"))


def _run(args):
    rc = bt.main(args)
    assert rc == 0


def test_end_to_end_backtest_produces_outputs(tmp_path, fixture_db,
                                              variant_config):
    """Smoke: end-to-end run on a fixture DB → manifest + summary + trades."""
    src, _ = fixture_db
    out = tmp_path / "out"
    _run([
        "--since", "2026-05-22T00:00:00", "--until", "2026-05-23T00:00:00",
        "--variant-config", variant_config,
        "--source-db", src,
        "--out-dir", str(out),
    ])
    runs = list((out / "runs").iterdir())
    assert len(runs) == 1
    run_dir = runs[0]
    manifest = json.loads((run_dir / "manifest.json").read_text())
    assert manifest["n_trades_simulated"] > 0, manifest
    assert (run_dir / "summary.md").exists()
    assert (run_dir / "trades.csv").exists()
    assert (run_dir / "trades.jsonl").exists()
    summary_text = (run_dir / "summary.md").read_text()
    assert "test_engine/default" in summary_text
    assert "trades simulated:" in summary_text


def test_variant_overlay_changes_trade_count(tmp_path, fixture_db):
    """A tighter min_confidence in the variant config must drop trades."""
    src, _ = fixture_db
    loose_cfg = _write_config(str(tmp_path / "loose.yaml"), min_confidence=0.45)
    tight_cfg = _write_config(str(tmp_path / "tight.yaml"), min_confidence=0.95)
    out_loose = tmp_path / "loose_out"
    out_tight = tmp_path / "tight_out"
    _run(["--since", "2026-05-22T00:00:00", "--until", "2026-05-23T00:00:00",
          "--variant-config", loose_cfg, "--source-db", src,
          "--out-dir", str(out_loose)])
    _run(["--since", "2026-05-22T00:00:00", "--until", "2026-05-23T00:00:00",
          "--variant-config", tight_cfg, "--source-db", src,
          "--out-dir", str(out_tight)])

    def _trades(out: Path) -> int:
        run = next((out / "runs").iterdir())
        return json.loads((run / "manifest.json").read_text())["n_trades_simulated"]

    n_loose = _trades(out_loose)
    n_tight = _trades(out_tight)
    assert n_loose > 0, "loose should produce trades"
    assert n_tight == 0, f"tight (min_conf=0.95) should skip everything (got {n_tight})"
    assert n_loose > n_tight, (n_loose, n_tight)


def test_oracle_fallback_when_decision_inputs_empty(tmp_path, fixture_db,
                                                    variant_config):
    """`decision_inputs` has 0 rows in the fixture → fallback to engine_results.
    The run must succeed AND the manifest must declare the oracle used."""
    src, _ = fixture_db
    # Sanity: decision_inputs empty in our fixture.
    con = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
    assert con.execute("SELECT COUNT(*) FROM decision_inputs").fetchone()[0] == 0
    con.close()
    out = tmp_path / "out"
    _run([
        "--since", "2026-05-22T00:00:00", "--until", "2026-05-23T00:00:00",
        "--variant-config", variant_config,
        "--source-db", src,
        "--out-dir", str(out),
    ])
    manifest_path = next((out / "runs").iterdir()) / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    assert manifest["oracle_strategy"] == "engine_results"
    assert manifest["n_decisions_walked"] > 0


def test_max_headlines_caps_run(tmp_path, fixture_db, variant_config):
    """--max-headlines 2 must walk only 2 headlines and trade ≤ 2 times."""
    src, _ = fixture_db
    out = tmp_path / "out"
    _run([
        "--since", "2026-05-22T00:00:00", "--until", "2026-05-23T00:00:00",
        "--variant-config", variant_config, "--source-db", src,
        "--out-dir", str(out), "--max-headlines", "2",
    ])
    manifest = json.loads(
        (next((out / "runs").iterdir()) / "manifest.json").read_text()
    )
    assert manifest["n_headlines_walked"] == 2
    assert manifest["n_trades_simulated"] <= 2


def test_manifest_carries_required_metadata(tmp_path, fixture_db,
                                            variant_config):
    """Manifest must carry the full Phase-2 spec metadata: window, oracle,
    coverage, source db hash, git sha, run_seconds."""
    src, _ = fixture_db
    out = tmp_path / "out"
    _run([
        "--since", "2026-05-22T00:00:00", "--until", "2026-05-23T00:00:00",
        "--variant-config", variant_config, "--source-db", src,
        "--out-dir", str(out),
    ])
    manifest = json.loads(
        (next((out / "runs").iterdir()) / "manifest.json").read_text()
    )
    for k in (
        "run_id", "git_sha", "started_at", "source_db_path",
        "source_db_hash", "window", "variant_config_path",
        "oracle_strategy", "oracle_coverage_pct",
        "n_headlines_walked", "n_decisions_walked",
        "n_trades_simulated", "n_skipped", "total_pnl_per_engine",
        "run_seconds",
    ):
        assert k in manifest, f"missing manifest key: {k}"
    assert manifest["window"]["since"] == "2026-05-22T00:00:00"
    assert manifest["window"]["until"] == "2026-05-23T00:00:00"
    assert manifest["variant_config_path"] == variant_config


def test_no_calibration_yet_fails_fast(tmp_path, fixture_db, variant_config):
    """Phase 4B.1 calibration not shipped; --calibration must fail fast."""
    src, _ = fixture_db
    out = tmp_path / "out"
    with pytest.raises(SystemExit):
        bt.main([
            "--since", "2026-05-22T00:00:00", "--until", "2026-05-23T00:00:00",
            "--variant-config", variant_config, "--source-db", src,
            "--out-dir", str(out), "--calibration",
        ])


def test_empty_window_hard_fails(tmp_path, fixture_db, variant_config):
    """Window with zero rows must hard-fail (no silent zero-trade exit)."""
    src, _ = fixture_db
    out = tmp_path / "out"
    with pytest.raises(SystemExit):
        bt.main([
            "--since", "1999-01-01T00:00:00", "--until", "1999-01-02T00:00:00",
            "--variant-config", variant_config, "--source-db", src,
            "--out-dir", str(out),
        ])

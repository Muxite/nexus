"""
Tests for `python -m app.debug` CLI subcommands.

Invokes the CLI as a subprocess so any aiosqlite / ML-framework worker
threads die with the child process. Asserts exit codes + key strings.
"""
from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]


def _make_test_db(path: Path) -> None:
    """Minimal schema the debug CLI's read-only queries need."""
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_url TEXT NOT NULL,
            headline TEXT NOT NULL,
            detected_at TEXT NOT NULL,
            headline_id TEXT
        );
        CREATE TABLE engine_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            engine_name TEXT NOT NULL,
            condition_id TEXT NOT NULL,
            question TEXT NOT NULL DEFAULT '',
            direction TEXT NOT NULL,
            confidence REAL NOT NULL,
            similarity REAL NOT NULL,
            p_hat REAL NOT NULL,
            mid_price REAL,
            edge REAL NOT NULL DEFAULT 0.0,
            size_usdc REAL NOT NULL,
            executed INTEGER NOT NULL DEFAULT 0,
            skip_reason TEXT,
            decided_at TEXT NOT NULL,
            headline_id TEXT,
            signal_latency_ms REAL
        );
        CREATE TABLE positions (
            id INTEGER PRIMARY KEY,
            trade_id INTEGER,
            condition_id TEXT,
            question TEXT,
            token_id TEXT,
            direction TEXT,
            size_usdc REAL,
            entry_price REAL,
            p_hat REAL,
            engine_name TEXT,
            opened_at TEXT,
            last_price REAL,
            unrealized_pnl REAL,
            high_water_price REAL,
            closed_at TEXT
        );
        CREATE TABLE trades (
            id INTEGER PRIMARY KEY,
            engine_name TEXT,
            headline_id TEXT,
            condition_id TEXT,
            size_usdc REAL,
            realized_pnl REAL,
            closed_at TEXT
        );
        """
    )
    conn.executemany(
        """INSERT INTO engine_results
           (engine_name, condition_id, question, direction, confidence,
            similarity, p_hat, mid_price, edge, size_usdc, executed,
            skip_reason, decided_at, headline_id)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        [
            ("aggressive", "0xc1", "Q1", "NEUTRAL", 0.5, 0.6, 0.0, 0.5,
             0.0, 0.0, 0, "neutral", "2026-05-13T12:00:00+00:00", "hid_test"),
            ("aggressive", "0xc2", "Q2", "YES", 0.7, 0.7, 0.6, 0.5,
             0.1, 100.0, 1, None, "2026-05-13T12:00:00+00:00", "hid_test"),
            ("balanced", "0xc3", "Q3", "NO", 0.6, 0.55, 0.6, 0.7,
             -0.1, 0.0, 0, "edge_too_small:0.03", "2026-05-13T12:00:00+00:00", "hid_test"),
        ],
    )
    conn.commit()
    conn.close()


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    p = tmp_path / "seulgi.db"
    _make_test_db(p)
    return p


@pytest.fixture
def config_path(tmp_path: Path, db_path: Path) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(
        f"""
db:
  path: "{db_path}"
ai:
  api_key: ""
  model: "test/model"
trading:
  entry_rail_skip_threshold: 0.02
engines:
  - name: "aggressive"
    min_edge: 0.005
"""
    )
    return p


def _run_cli(*argv: str, timeout: float = 25.0) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["PYTHONPATH"] = f"{REPO}/app" + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.run(
        [sys.executable, "-u", "-m", "app.debug", *argv],
        cwd=REPO,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )


def test_probe_scraper_returns_nonzero_when_unreachable():
    r = _run_cli("probe-scraper", "--scraper-url", "http://127.0.0.1:1")
    assert r.returncode != 0
    assert "127.0.0.1:1" in r.stdout


def test_system_health_runs(db_path: Path, config_path: Path):
    r = _run_cli(
        "system-health",
        "--db", str(db_path),
        "--config", str(config_path),
        "--scraper-url", "http://127.0.0.1:1",
    )
    # Unreachable scraper + no AI key → handler returns 2 (degraded), not 0.
    assert r.returncode in (0, 2), r.stderr
    assert "scraper" in r.stdout


def test_inspect_engine_runs(db_path: Path, config_path: Path):
    r = _run_cli(
        "inspect-engine", "aggressive",
        "--db", str(db_path),
        "--config", str(config_path),
    )
    assert r.returncode == 0, r.stderr
    assert "aggressive" in r.stdout
    assert "entry_rail_skip_threshold" in r.stdout


def test_inspect_engine_unknown_returns_nonzero(db_path: Path, config_path: Path):
    r = _run_cli(
        "inspect-engine", "no_such_engine",
        "--db", str(db_path),
        "--config", str(config_path),
    )
    assert r.returncode != 0


def test_recent_skips_runs(db_path: Path, config_path: Path):
    r = _run_cli(
        "recent-skips", "-n", "10",
        "--db", str(db_path),
        "--config", str(config_path),
    )
    assert r.returncode == 0, r.stderr
    assert "neutral" in r.stdout
    assert "edge_too_small" in r.stdout


def test_recent_skips_filtered_by_engine(db_path: Path, config_path: Path):
    r = _run_cli(
        "recent-skips", "--engine", "balanced", "-n", "10",
        "--db", str(db_path),
        "--config", str(config_path),
    )
    assert r.returncode == 0, r.stderr
    assert "edge_too_small" in r.stdout

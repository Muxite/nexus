"""Tests for `scripts.replay.preflight`."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from scripts.replay.preflight import main


def _seed_db(path: Path, *, stored_pnl: float) -> None:
    """Create a minimal sqlite DB with one closed YES trade.

    YES PnL formula: size * (exit - entry) / entry.
    With size=100, entry=0.40, exit=0.50 → reconstructed = $25.00.
    Caller controls `stored_pnl` so the test can dial drift in.
    """
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE trades (
                id INTEGER PRIMARY KEY,
                direction TEXT NOT NULL,
                size_usdc REAL NOT NULL,
                mid_price REAL NOT NULL
            );
            CREATE TABLE trade_closures (
                id INTEGER PRIMARY KEY,
                trade_id INTEGER NOT NULL,
                exit_mid_price REAL NOT NULL,
                realized_pnl_usdc REAL NOT NULL
            );
            """
        )
        conn.execute(
            "INSERT INTO trades (id, direction, size_usdc, mid_price) VALUES (1, 'YES', 100.0, 0.40)"
        )
        conn.execute(
            "INSERT INTO trade_closures (id, trade_id, exit_mid_price, realized_pnl_usdc) "
            "VALUES (1, 1, 0.50, ?)",
            (stored_pnl,),
        )
        conn.commit()
    finally:
        conn.close()


def test_preflight_passes_when_pnl_matches(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db = tmp_path / "soak.db"
    _seed_db(db, stored_pnl=25.0)

    rc = main(["--db", str(db)])

    assert rc == 0
    captured = capsys.readouterr()
    assert "PREFLIGHT OK: 1/1" in captured.out


def test_preflight_fails_when_pnl_drifts(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db = tmp_path / "soak.db"
    _seed_db(db, stored_pnl=26.5)  # drift = $1.50, well past $0.01 tolerance

    rc = main(["--db", str(db)])

    assert rc == 1
    captured = capsys.readouterr()
    assert "PREFLIGHT DRIFT: 0/1" in captured.out
    assert "trade 1:" in captured.out
    assert "stored=$26.5000" in captured.out
    assert "recon=$25.0000" in captured.out


def test_preflight_handles_missing_db(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc = main(["--db", str(tmp_path / "does-not-exist.db")])

    assert rc == 2
    captured = capsys.readouterr()
    assert "PREFLIGHT ERROR" in captured.out


def test_preflight_passes_on_empty_db(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db = tmp_path / "soak.db"
    conn = sqlite3.connect(db)
    try:
        conn.executescript(
            """
            CREATE TABLE trades (
                id INTEGER PRIMARY KEY, direction TEXT, size_usdc REAL, mid_price REAL
            );
            CREATE TABLE trade_closures (
                id INTEGER PRIMARY KEY, trade_id INTEGER,
                exit_mid_price REAL, realized_pnl_usdc REAL
            );
            """
        )
        conn.commit()
    finally:
        conn.close()

    rc = main(["--db", str(db)])

    assert rc == 0
    captured = capsys.readouterr()
    assert "PREFLIGHT OK: 0/0" in captured.out

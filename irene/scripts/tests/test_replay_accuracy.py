"""Tests for scripts.replay.accuracy."""

from __future__ import annotations

import asyncio
import sqlite3

import pytest

import app.main  # noqa: F401 — registers cycles DDL
import app.stages.closer  # noqa: F401 — registers trade_closures DDL
import app.stages.trade   # noqa: F401 — registers trades DDL
from app.db import DB

from scripts.replay import accuracy


# ---------------------------------------------------------------------------
# Pure formula
# ---------------------------------------------------------------------------


def test_pnl_yes_winning():
    assert accuracy.reconstruct_pnl(
        direction="YES", size_usdc=100.0, entry_mid=0.4, exit_mid=0.6
    ) == pytest.approx(50.0)


def test_pnl_yes_losing():
    assert accuracy.reconstruct_pnl(
        direction="YES", size_usdc=100.0, entry_mid=0.6, exit_mid=0.4
    ) == pytest.approx(-33.3333, rel=1e-3)


def test_pnl_no_winning():
    # NO direction: bought at NO_price=1-0.6=0.4, exit NO_price=1-0.4=0.6
    # → shares = 100 / 0.4 = 250; pnl/share = 0.6 - 0.4 = 0.2 → 50.
    # Formula: size * (entry_YES - exit_YES) / (1 - entry_YES)
    #        = 100 * (0.6 - 0.4) / 0.4 = 50.
    assert accuracy.reconstruct_pnl(
        direction="NO", size_usdc=100.0, entry_mid=0.6, exit_mid=0.4
    ) == pytest.approx(50.0)


def test_pnl_no_losing():
    # Direction NO with YES going up: entry YES=0.4 (NO=0.6), exit YES=0.6
    # (NO=0.4). NO lost: per-share = 0.4 - 0.6 = -0.2. Shares = 100/0.6.
    # PnL = -100 * 0.2 / 0.6 ≈ -33.33.
    # Formula: 100 * (0.4 - 0.6) / (1 - 0.4) = -33.33 ✓
    assert accuracy.reconstruct_pnl(
        direction="NO", size_usdc=100.0, entry_mid=0.4, exit_mid=0.6
    ) == pytest.approx(-33.3333, rel=1e-3)


def test_pnl_unknown_direction_raises():
    with pytest.raises(ValueError):
        accuracy.reconstruct_pnl(
            direction="MAYBE", size_usdc=100.0, entry_mid=0.5, exit_mid=0.5
        )


def test_pnl_edge_zero_entry_yes_returns_zero():
    assert accuracy.reconstruct_pnl(
        direction="YES", size_usdc=100.0, entry_mid=0.0, exit_mid=0.5
    ) == 0.0


def test_pnl_edge_one_entry_no_returns_zero():
    assert accuracy.reconstruct_pnl(
        direction="NO", size_usdc=100.0, entry_mid=1.0, exit_mid=0.5
    ) == 0.0


# ---------------------------------------------------------------------------
# run_inproc — needs an on-disk DB
# ---------------------------------------------------------------------------


def _seed_db(db_path: str, rows: list[dict]) -> None:
    conn = sqlite3.connect(db_path)
    try:
        for r in rows:
            conn.execute(
                """
                INSERT INTO decisions (id, signal_id, outcome, reason, gate_history,
                                       prediction_id, cycle_id, is_tracer, tracer_id, created_at)
                VALUES (?, ?, 'execute', 'ok', '[]', ?, 1, 0, NULL, '2026-05-23T00:00:00+00:00')
                """,
                (r["trade_id"], r["trade_id"], r["trade_id"]),
            )
            conn.execute(
                """
                INSERT INTO trades
                  (id, signal_id, decision_id, prediction_id, market_id, direction,
                   confidence, similarity, freshness, p_hat, mid_price, edge,
                   kelly_fraction, size_usdc, decision_path, features_snapshot,
                   cycle_id, is_tracer, tracer_id, decided_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    r["trade_id"], r["trade_id"], r["trade_id"], r["trade_id"],
                    r.get("market_id", "m1"), r["direction"], 0.7, 0.7, "BREAKING",
                    0.7, r["entry_mid"], 0.1, 0.1, r["size_usdc"], "mvp", "{}",
                    1, 0, None, "2026-05-23T00:00:00+00:00",
                ),
            )
            conn.execute(
                """
                INSERT INTO trade_closures
                  (trade_id, closed_at, close_reason, exit_mid_price,
                   realized_pnl_usdc, cycle_id, is_tracer, tracer_id)
                VALUES (?, '2026-05-23T01:00:00+00:00', 'timeout', ?, ?, 1, 0, NULL)
                """,
                (r["trade_id"], r["exit_mid"], r["stored_pnl"]),
            )
        conn.commit()
    finally:
        conn.close()


async def _bootstrap(path: str) -> None:
    db = DB(path=path)
    await db.connect()
    await db.close()


@pytest.fixture
async def populated_db(tmp_path):
    path = str(tmp_path / "replay.db")
    await _bootstrap(path)
    return path


async def test_run_inproc_all_accurate(populated_db):
    rows = [
        {"trade_id": 1, "direction": "YES", "size_usdc": 100.0,
         "entry_mid": 0.4, "exit_mid": 0.6, "stored_pnl": 50.0},
        {"trade_id": 2, "direction": "NO", "size_usdc": 50.0,
         "entry_mid": 0.6, "exit_mid": 0.4, "stored_pnl": 25.0},
    ]
    _seed_db(populated_db, rows)
    result = accuracy.run_inproc(populated_db, tolerance=0.01)
    assert result.passed is True
    assert result.total_closed == 2
    assert result.within_tolerance == 2
    assert result.mismatches == []


async def test_run_inproc_flags_drift(populated_db):
    rows = [
        {"trade_id": 1, "direction": "YES", "size_usdc": 100.0,
         "entry_mid": 0.4, "exit_mid": 0.6, "stored_pnl": 99.0},
        {"trade_id": 2, "direction": "YES", "size_usdc": 100.0,
         "entry_mid": 0.5, "exit_mid": 0.5, "stored_pnl": 0.0},
    ]
    _seed_db(populated_db, rows)
    result = accuracy.run_inproc(populated_db, tolerance=0.01)
    assert result.passed is False
    assert result.total_closed == 2
    assert result.within_tolerance == 1
    assert len(result.mismatches) == 1
    assert result.mismatches[0].trade_id == 1
    assert result.mismatches[0].diff == pytest.approx(49.0)


def test_run_inproc_rejects_memory_db():
    with pytest.raises(ValueError, match="memory"):
        accuracy.run_inproc(":memory:")


def test_run_inproc_missing_db(tmp_path):
    with pytest.raises(FileNotFoundError):
        accuracy.run_inproc(str(tmp_path / "no_such.db"))


async def test_run_inproc_empty_db_passes(populated_db):
    result = accuracy.run_inproc(populated_db)
    assert result.passed is True
    assert result.total_closed == 0


def test_cli_main_zero_on_accurate(tmp_path):
    path = str(tmp_path / "cli.db")
    asyncio.run(_bootstrap(path))
    _seed_db(path, [
        {"trade_id": 1, "direction": "YES", "size_usdc": 100.0,
         "entry_mid": 0.4, "exit_mid": 0.6, "stored_pnl": 50.0},
    ])
    rc = accuracy.main(["--db", path])
    assert rc == 0


def test_cli_main_one_on_drift(tmp_path):
    path = str(tmp_path / "cli2.db")
    asyncio.run(_bootstrap(path))
    _seed_db(path, [
        {"trade_id": 1, "direction": "YES", "size_usdc": 100.0,
         "entry_mid": 0.4, "exit_mid": 0.6, "stored_pnl": 999.0},
    ])
    rc = accuracy.main(["--db", path])
    assert rc == 1


def test_cli_main_two_on_missing_db(tmp_path):
    rc = accuracy.main(["--db", str(tmp_path / "absent.db")])
    assert rc == 2

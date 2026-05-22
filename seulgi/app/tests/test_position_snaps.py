"""
Tests for A2.2 trade-anchored continuous price tape.

Covers three contracts:
1. `db.insert_position_price_snap` + `db.get_position_price_snaps`
   round-trip, including the idempotency guard on
   (trade_id, t_minutes_since_entry) UNIQUE.
2. The migration is idempotent — running `_migrate()` twice does not
   raise, and the schema reports the expected columns on
   `position_price_snaps` and the new bid/ask/book_depth_usdc columns
   on `market_price_snaps`.
3. `PriceImpactTracker._run_position_snap_sweep` with two fake open
   positions at different ages: only the offsets ≤ age get written,
   per-token CLOB fetches are coalesced, and a second sweep does not
   double-write the same checkpoints.
"""

import asyncio
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from db import SeulgiDB
import impact_tracker
from impact_tracker import PriceImpactTracker


# ---------------------------------------------------------------------------
# 1. DB round-trip + idempotency
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_position_price_snap_roundtrip(tmp_path):
    """Insert one row, read it back via get_position_price_snaps."""
    db = SeulgiDB(str(tmp_path / "snap.db"))
    await db.connect()
    try:
        await db.insert_position_price_snap(
            trade_id=42,
            token_id="tok_a",
            side="YES",
            t_unix=1_700_000_000,
            t_minutes_since_entry=5,
            price=0.512,
            bid=0.510,
            ask=0.514,
            book_depth_usdc=125.0,
            source="monitor",
        )
        rows = await db.get_position_price_snaps(trade_id=42)
        assert len(rows) == 1
        row = rows[0]
        assert row["trade_id"] == 42
        assert row["token_id"] == "tok_a"
        assert row["side"] == "YES"
        assert row["t_minutes_since_entry"] == 5
        assert abs(row["price"] - 0.512) < 1e-9
        assert abs(row["bid"] - 0.510) < 1e-9
        assert abs(row["ask"] - 0.514) < 1e-9
        assert abs(row["book_depth_usdc"] - 125.0) < 1e-9
        assert row["source"] == "monitor"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_position_price_snap_idempotent_on_unique(tmp_path):
    """Writing the same (trade_id, t_minutes_since_entry) twice → one row."""
    db = SeulgiDB(str(tmp_path / "snap.db"))
    await db.connect()
    try:
        for _ in range(2):
            await db.insert_position_price_snap(
                trade_id=7,
                token_id="tok_b",
                side="NO",
                t_unix=1_700_000_100,
                t_minutes_since_entry=15,
                price=0.43,
                source="monitor",
            )
        rows = await db.get_position_price_snaps(trade_id=7)
        assert len(rows) == 1, (
            "INSERT OR IGNORE on UNIQUE(trade_id, t_minutes_since_entry) "
            "must collapse duplicate writes to a single row"
        )
        # A different offset for the same trade is a fresh row.
        await db.insert_position_price_snap(
            trade_id=7, token_id="tok_b", side="NO",
            t_unix=1_700_000_700, t_minutes_since_entry=30,
            price=0.44, source="monitor",
        )
        rows = await db.get_position_price_snaps(trade_id=7)
        assert len(rows) == 2
        assert {r["t_minutes_since_entry"] for r in rows} == {15, 30}
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_migration_is_idempotent(tmp_path):
    """connect() + a second _migrate() must not raise; expected columns
    must exist on both tables."""
    db = SeulgiDB(str(tmp_path / "snap.db"))
    await db.connect()
    try:
        # Run migrations a second time — every ALTER/CREATE inside
        # _MIGRATIONS is wrapped in a try/except, so this must be a
        # silent no-op.
        await db._migrate()

        async def _cols(table: str) -> set[str]:
            async with db._conn.execute(f"PRAGMA table_info({table})") as cur:
                rows = await cur.fetchall()
            return {r[1] for r in rows}

        pps = await _cols("position_price_snaps")
        expected_pps = {
            "id", "trade_id", "token_id", "side", "t_unix",
            "t_minutes_since_entry", "price", "bid", "ask",
            "book_depth_usdc", "source",
        }
        assert expected_pps.issubset(pps), (
            f"position_price_snaps missing columns: {expected_pps - pps}"
        )

        mps = await _cols("market_price_snaps")
        for col in ("bid", "ask", "book_depth_usdc"):
            assert col in mps, (
                f"market_price_snaps must have new column {col!r} after "
                f"A2.2 migration; got: {sorted(mps)}"
            )

        # Bumping write_counters must record the new method on success.
        await db.insert_position_price_snap(
            trade_id=1, token_id="t", side="YES",
            t_unix=1, t_minutes_since_entry=5, price=0.5,
        )
        counters = db.write_health_snapshot()["counters"]
        assert counters.get("insert_position_price_snap", {}).get("ok", 0) >= 1
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_market_price_snaps_accepts_legacy_and_bbo_shape(tmp_path):
    """insert_market_price_snaps must accept the legacy 7-tuple AND the
    A2.2 10-tuple (bid/ask/book_depth_usdc), populating NULL where
    the legacy caller omitted them."""
    db = SeulgiDB(str(tmp_path / "snap.db"))
    await db.connect()
    try:
        legacy_row = ("c1", "tok_x", "yes", 0.5, 1, "prefetch", "hid1")
        bbo_row = ("c2", "tok_y", "no", 0.6, 2, "prefetch", "hid2",
                   0.59, 0.61, 200.0)
        await db.insert_market_price_snaps([legacy_row, bbo_row])
        async with db._conn.execute(
            "SELECT condition_id, bid, ask, book_depth_usdc "
            "FROM market_price_snaps ORDER BY t_unix"
        ) as cur:
            rows = await cur.fetchall()
        assert len(rows) == 2
        assert rows[0]["condition_id"] == "c1"
        assert rows[0]["bid"] is None
        assert rows[0]["ask"] is None
        assert rows[0]["book_depth_usdc"] is None
        assert rows[1]["condition_id"] == "c2"
        assert abs(rows[1]["bid"] - 0.59) < 1e-9
        assert abs(rows[1]["ask"] - 0.61) < 1e-9
        assert abs(rows[1]["book_depth_usdc"] - 200.0) < 1e-9
    finally:
        await db.close()


# ---------------------------------------------------------------------------
# 2. impact_tracker._run_position_snap_sweep
# ---------------------------------------------------------------------------


def _open_position_dict(
    trade_id: int,
    token_id: str,
    direction: str,
    opened_at_iso: str,
    condition_id: str = "cond1",
) -> dict:
    """Mimic the shape of one row from `db.get_open_positions()`."""
    return {
        "trade_id": trade_id,
        "condition_id": condition_id,
        "question": "Will X happen?",
        "token_id": token_id,
        "direction": direction,
        "size_usdc": 50.0,
        "entry_price": 0.45,
        "opened_at": opened_at_iso,
        "last_price": 0.45,
        "unrealized_pnl": 0.0,
        "p_hat": 0.55,
        "engine_name": "default",
        "shares": 100.0,
        "market_type": None,
    }


@pytest.mark.asyncio
async def test_position_snap_loop_writes_due_offsets(tmp_path, monkeypatch):
    """Two open positions at different ages → only the offsets that
    have elapsed get written, per-token fetches are coalesced, and
    a second sweep is a no-op (idempotent INSERT OR IGNORE)."""
    db = SeulgiDB(str(tmp_path / "snap.db"))
    await db.connect()

    now = datetime.now(timezone.utc)
    # Position A: opened 20 minutes ago → 5m, 15m due; 30m / 60m / 240m
    # not yet.
    pos_a = _open_position_dict(
        trade_id=101,
        token_id="tok_old",
        direction="YES",
        opened_at_iso=(now - timedelta(minutes=20)).isoformat(),
        condition_id="condA",
    )
    # Position B: opened 4 minutes ago → no offset due (the 5m offset
    # is still ~60s away). Used to assert "not-yet-due rows are
    # skipped" by the sweep.
    pos_b = _open_position_dict(
        trade_id=202,
        token_id="tok_new",
        direction="NO",
        opened_at_iso=(now - timedelta(minutes=4)).isoformat(),
        condition_id="condB",
    )
    # Position C: shares the token with A → must NOT cause a second
    # CLOB fetch; per-token coalescing in the sweep is part of the
    # contract.
    pos_c = _open_position_dict(
        trade_id=303,
        token_id="tok_old",
        direction="YES",
        opened_at_iso=(now - timedelta(minutes=20)).isoformat(),
        condition_id="condA",
    )

    db.get_open_positions = AsyncMock(return_value=[pos_a, pos_b, pos_c])

    fetch_calls: list[str] = []

    async def _fake_fetch_bbo(session, token_id):
        fetch_calls.append(token_id)
        # 0.55 mid; no BBO available (matches the current /midpoint-only
        # CLOB endpoint behaviour).
        return (0.55, None, None, None)

    monkeypatch.setattr(impact_tracker, "_fetch_bbo", _fake_fetch_bbo)

    tracker = PriceImpactTracker(db=db, dashboard_dir=str(tmp_path / "dash"))

    # Run one sweep deterministically (no `while self._running` loop).
    await tracker._run_position_snap_sweep()

    # Only tok_old should have been fetched (Position B has no due
    # offset, so no fetch is needed). Coalescing means tok_old is
    # fetched exactly ONCE despite two positions sharing it.
    assert fetch_calls == ["tok_old"], (
        f"expected exactly 1 fetch for the only due token; got {fetch_calls}"
    )

    # Position A: 20 min old → 5m and 15m offsets are due.
    rows_a = await db.get_position_price_snaps(trade_id=101)
    offsets_a = {r["t_minutes_since_entry"] for r in rows_a}
    assert offsets_a == {5, 15}, (
        f"Position A (20m old) should have 5m and 15m snaps; got {offsets_a}"
    )
    for r in rows_a:
        assert r["side"] == "YES"
        assert r["token_id"] == "tok_old"
        assert r["source"] == "monitor"
        assert abs(r["price"] - 0.55) < 1e-9

    # Position B: 4 min old → nothing should have been written.
    rows_b = await db.get_position_price_snaps(trade_id=202)
    assert rows_b == [], (
        f"Position B (4m old) should have no snaps yet; got {rows_b}"
    )

    # Position C: same age + token as A → independently written rows
    # under its own trade_id.
    rows_c = await db.get_position_price_snaps(trade_id=303)
    offsets_c = {r["t_minutes_since_entry"] for r in rows_c}
    assert offsets_c == {5, 15}

    # A second sweep must NOT add duplicate rows.
    fetch_calls.clear()
    await tracker._run_position_snap_sweep()
    rows_a_after = await db.get_position_price_snaps(trade_id=101)
    assert len(rows_a_after) == 2, (
        "INSERT OR IGNORE must prevent duplicate writes on re-sweep; "
        f"got {len(rows_a_after)} rows"
    )

    await db.close()


@pytest.mark.asyncio
async def test_position_snap_loop_no_open_positions_is_noop(tmp_path, monkeypatch):
    """No open positions → no fetches, no writes."""
    db = SeulgiDB(str(tmp_path / "snap.db"))
    await db.connect()
    db.get_open_positions = AsyncMock(return_value=[])

    fetch_calls: list[str] = []

    async def _fake_fetch_bbo(session, token_id):
        fetch_calls.append(token_id)
        return (0.5, None, None, None)

    monkeypatch.setattr(impact_tracker, "_fetch_bbo", _fake_fetch_bbo)

    tracker = PriceImpactTracker(db=db, dashboard_dir=str(tmp_path / "dash"))
    await tracker._run_position_snap_sweep()
    assert fetch_calls == []

    await db.close()


@pytest.mark.asyncio
async def test_position_snap_loop_skips_when_mid_unavailable(tmp_path, monkeypatch):
    """If `_fetch_bbo` returns mid=None (CLOB failure), the loop must
    NOT write a row — next sweep retries."""
    db = SeulgiDB(str(tmp_path / "snap.db"))
    await db.connect()

    now = datetime.now(timezone.utc)
    pos = _open_position_dict(
        trade_id=999,
        token_id="tok_dead",
        direction="YES",
        opened_at_iso=(now - timedelta(minutes=10)).isoformat(),
    )
    db.get_open_positions = AsyncMock(return_value=[pos])

    async def _fake_fetch_bbo(session, token_id):
        return (None, None, None, None)

    monkeypatch.setattr(impact_tracker, "_fetch_bbo", _fake_fetch_bbo)

    tracker = PriceImpactTracker(db=db, dashboard_dir=str(tmp_path / "dash"))
    await tracker._run_position_snap_sweep()

    rows = await db.get_position_price_snaps(trade_id=999)
    assert rows == [], (
        "No row should be written when mid is unavailable; got "
        f"{rows}"
    )

    await db.close()

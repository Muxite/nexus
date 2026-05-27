"""Cycle 25-B — binary-event dedup gate tests.

Unit tests for `_gate_binary_dedup` (pure sync predicate) and integration
tests for `query_open_sibling_markets` against a seeded in-memory DB.
"""

from __future__ import annotations

import sqlite3

import pytest

from app.stages._gates import _gate_binary_dedup, query_open_sibling_markets


# ---------------------------------------------------------------------------
# Pure predicate
# ---------------------------------------------------------------------------


def test_passes_when_no_open_siblings():
    status, entry, reason = _gate_binary_dedup([])
    assert status == "ok"
    assert entry is None
    assert reason is None


def test_rejects_when_a_sibling_market_is_open():
    status, entry, reason = _gate_binary_dedup(["0x_other_leg_market"])
    assert status == "reject"
    assert reason == "binary_dedup"
    assert entry is not None and entry["gate"] == "binary_dedup"
    assert "0x_other_leg_market" in entry["value"]


def test_rejects_when_multiple_open_siblings():
    status, entry, reason = _gate_binary_dedup(["m_a", "m_b", "m_c"])
    assert status == "reject"
    assert reason == "binary_dedup"
    # Entry value lists up to 3 siblings.
    val = entry["value"]
    assert "m_a" in val and "m_b" in val and "m_c" in val


# ---------------------------------------------------------------------------
# Integration: query_open_sibling_markets against a seeded DB
# ---------------------------------------------------------------------------


def _seed_market_links_and_trades(con: sqlite3.Connection) -> None:
    """Two markets share canonical event 800 (top-scoring link for both);
    one of them already has an open trade. Plus a third unrelated market
    with its own canonical event so we test that the join filters.
    """
    con.executescript(
        """
        CREATE TABLE market_links (
            id INTEGER PRIMARY KEY,
            event_id INTEGER NOT NULL,
            market_condition_id TEXT NOT NULL,
            final_score REAL NOT NULL
        );
        CREATE TABLE trades (
            id INTEGER PRIMARY KEY,
            market_id TEXT,
            direction TEXT,
            size_usdc REAL,
            is_tracer INTEGER DEFAULT 0
        );
        CREATE TABLE trade_closures (
            id INTEGER PRIMARY KEY,
            trade_id INTEGER,
            closed_at TEXT
        );

        -- Market A: canonical event = 800 (score 0.9), also linked to 700.
        INSERT INTO market_links (event_id, market_condition_id, final_score)
            VALUES (800, 'mA', 0.9), (700, 'mA', 0.3);
        -- Market B: canonical event = 800 (score 0.8), also linked to 900.
        INSERT INTO market_links (event_id, market_condition_id, final_score)
            VALUES (800, 'mB', 0.8), (900, 'mB', 0.4);
        -- Market C: canonical event = 950 (independent).
        INSERT INTO market_links (event_id, market_condition_id, final_score)
            VALUES (950, 'mC', 0.7);

        -- Open paper trade on Market A (no closure row).
        INSERT INTO trades (id, market_id, direction, size_usdc, is_tracer)
            VALUES (1, 'mA', 'NO', 100.0, 0);
        -- Closed trade on Market A (already has closure row).
        INSERT INTO trades (id, market_id, direction, size_usdc, is_tracer)
            VALUES (2, 'mA', 'YES', 50.0, 0);
        INSERT INTO trade_closures (trade_id, closed_at)
            VALUES (2, '2026-05-01T00:00:00');
        -- Open tracer trade on Market A — should NOT count (is_tracer=1).
        INSERT INTO trades (id, market_id, direction, size_usdc, is_tracer)
            VALUES (3, 'mA', 'NO', 25.0, 1);
        -- Open trade on Market C — different canonical event, no conflict.
        INSERT INTO trades (id, market_id, direction, size_usdc, is_tracer)
            VALUES (4, 'mC', 'YES', 30.0, 0);
        """
    )


class _AsyncConnWrapper:
    """Wrap sqlite3.Connection so it has the async surface
    `query_open_sibling_markets` expects (`execute` returns an awaitable
    that yields a cursor with `fetchall()` + `close()` coroutines)."""

    def __init__(self, raw: sqlite3.Connection) -> None:
        raw.row_factory = sqlite3.Row
        self._raw = raw

    async def execute(self, sql, params=()):
        cur = self._raw.execute(sql, params)
        return _AsyncCursor(cur)


class _AsyncCursor:
    def __init__(self, raw_cur):
        self._raw = raw_cur

    async def fetchall(self):
        return self._raw.fetchall()

    async def close(self):
        self._raw.close()


@pytest.mark.asyncio
async def test_query_finds_open_sibling_for_market_B(tmp_path):
    db = sqlite3.connect(":memory:")
    _seed_market_links_and_trades(db)
    conn = _AsyncConnWrapper(db)
    siblings = await query_open_sibling_markets(conn, "mB")
    # mA has an open paper trade and shares canonical event 800 with mB.
    assert siblings == ["mA"]


@pytest.mark.asyncio
async def test_query_excludes_tracer_trades(tmp_path):
    db = sqlite3.connect(":memory:")
    _seed_market_links_and_trades(db)
    # Delete the non-tracer open trade so only the tracer trade remains.
    db.execute("DELETE FROM trades WHERE id = 1")
    conn = _AsyncConnWrapper(db)
    siblings = await query_open_sibling_markets(conn, "mB")
    assert siblings == [], "tracer trades should not block paper trades"


@pytest.mark.asyncio
async def test_query_excludes_closed_trades(tmp_path):
    db = sqlite3.connect(":memory:")
    _seed_market_links_and_trades(db)
    # Delete the OPEN paper trade — only the closed one remains on mA.
    db.execute("DELETE FROM trades WHERE id = 1")
    db.execute("DELETE FROM trades WHERE id = 3")  # tracer
    conn = _AsyncConnWrapper(db)
    siblings = await query_open_sibling_markets(conn, "mB")
    assert siblings == [], "closed trades should not block new entries"


@pytest.mark.asyncio
async def test_query_filters_to_same_canonical_event_only(tmp_path):
    db = sqlite3.connect(":memory:")
    _seed_market_links_and_trades(db)
    conn = _AsyncConnWrapper(db)
    # mC's canonical event is 950 — unrelated to mA's 800.
    siblings = await query_open_sibling_markets(conn, "mC")
    assert siblings == [], "different canonical event should not conflict"


@pytest.mark.asyncio
async def test_query_does_not_match_self(tmp_path):
    db = sqlite3.connect(":memory:")
    _seed_market_links_and_trades(db)
    conn = _AsyncConnWrapper(db)
    # mA has its own open trade; querying mA shouldn't return itself.
    siblings = await query_open_sibling_markets(conn, "mA")
    assert siblings == [], "same market_id must be excluded from siblings"


@pytest.mark.asyncio
async def test_query_picks_top_score_as_canonical(tmp_path):
    """Markets linked to multiple events should only sibling on the
    HIGHEST-scoring event, not on any shared low-score link."""
    db = sqlite3.connect(":memory:")
    db.executescript(
        """
        CREATE TABLE market_links (
            id INTEGER PRIMARY KEY,
            event_id INTEGER NOT NULL,
            market_condition_id TEXT NOT NULL,
            final_score REAL NOT NULL
        );
        CREATE TABLE trades (
            id INTEGER PRIMARY KEY,
            market_id TEXT,
            direction TEXT,
            size_usdc REAL,
            is_tracer INTEGER DEFAULT 0
        );
        CREATE TABLE trade_closures (
            id INTEGER PRIMARY KEY,
            trade_id INTEGER,
            closed_at TEXT
        );

        -- mX canonical = 1 (0.9), also weakly linked to 2 (0.2).
        INSERT INTO market_links VALUES (1, 1, 'mX', 0.9);
        INSERT INTO market_links VALUES (2, 2, 'mX', 0.2);
        -- mY canonical = 2 (0.8) — only shares the WEAK link with mX.
        INSERT INTO market_links VALUES (3, 2, 'mY', 0.8);
        INSERT INTO market_links VALUES (4, 3, 'mY', 0.5);

        -- Open trade on mY.
        INSERT INTO trades (id, market_id, direction, size_usdc, is_tracer)
            VALUES (10, 'mY', 'YES', 100.0, 0);
        """
    )
    conn = _AsyncConnWrapper(db)
    # mX's canonical is 1; mY's is 2. They share the weak event 2 link only,
    # which is NOT canonical for mX → no sibling.
    siblings = await query_open_sibling_markets(conn, "mX")
    assert siblings == [], "weak-link shared event should not trigger dedup"

"""Tests for Phase 4A.3 BBO + book-depth capture.

Covers:
1. CLOB /book parsing: best bid/ask, sizes, top-5 ask notional.
2. 5xx fallback: degrade to /midpoint with source="clob_mid_fallback".
3. Cache TTL: two calls inside the window share one HTTP request.
4. bid <= mid <= ask clamping (so spread_bps is never negative).
5. One-sided book: missing-side fields stay None, present side fills.
6. `_fetch_bbo` 4-tuple shim mirrors the new BBOResult.
7. Concurrent first-fetchers coalesce via the in-flight future map.

All tests stub aiohttp at the session level — no real network.
"""

from __future__ import annotations

import asyncio
import os
import sys
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import impact_tracker
from impact_tracker import BBOResult, fetch_bbo, _fetch_bbo


# ---------------------------------------------------------------------------
# Fake aiohttp session
# ---------------------------------------------------------------------------


class _FakeResp:
    """Minimal aiohttp response stand-in covering `.status` + `.json()`."""

    def __init__(self, status: int, payload: dict | None = None):
        self.status = status
        self._payload = payload or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def json(self):
        return self._payload


class _FakeSession:
    """Stand-in for aiohttp.ClientSession.

    `routes` maps URL substring → list of responses (popped in order).
    Counts calls per URL substring so tests can assert cache + coalescing
    behaviour.
    """

    def __init__(self, routes: dict[str, list]):
        self._routes = {k: list(v) for k, v in routes.items()}
        self.calls: dict[str, int] = {k: 0 for k in routes}

    def get(self, url: str, **_kwargs):  # noqa: D401
        for key, queue in self._routes.items():
            if key in url:
                self.calls[key] += 1
                if not queue:
                    raise AssertionError(
                        f"FakeSession: no more responses queued for {key!r}"
                    )
                item = queue.pop(0)
                if isinstance(item, Exception):
                    @asynccontextmanager
                    async def _raise():
                        raise item
                        yield  # pragma: no cover
                    return _raise()
                return item
        raise AssertionError(f"FakeSession: unrouted URL {url!r}")


@pytest.fixture(autouse=True)
def _reset_cache():
    """Each test starts with an empty module-level BBO cache."""
    impact_tracker._bbo_cache.clear()
    impact_tracker._bbo_inflight.clear()
    yield
    impact_tracker._bbo_cache.clear()
    impact_tracker._bbo_inflight.clear()


# ---------------------------------------------------------------------------
# 1. Happy path: /book returns full book
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_bbo_parses_full_book():
    book = {
        # bids ascending: best (highest) is last
        "bids": [
            {"price": "0.50", "size": "10"},
            {"price": "0.52", "size": "20"},
            {"price": "0.57", "size": "75.9"},
        ],
        # asks descending: best (lowest) is last
        "asks": [
            {"price": "0.70", "size": "30"},
            {"price": "0.66", "size": "38"},
            {"price": "0.65", "size": "40"},
            {"price": "0.61", "size": "23.07"},
            {"price": "0.60", "size": "53.4"},
            {"price": "0.59", "size": "8"},
        ],
    }
    session = _FakeSession({"/book": [_FakeResp(200, book)]})
    r = await fetch_bbo(session, "tok_a")
    assert r.source == "clob_book"
    assert r.bid == pytest.approx(0.57)
    assert r.ask == pytest.approx(0.59)
    assert r.bid_size == pytest.approx(75.9)
    assert r.ask_size == pytest.approx(8)
    # mid = (0.57 + 0.59) / 2 = 0.58
    assert r.mid == pytest.approx(0.58)
    # top-5 ask levels (the 5 nearest to top): 0.59, 0.60, 0.61, 0.65, 0.66.
    # depth_usdc = 0.59*8 + 0.60*53.4 + 0.61*23.07 + 0.65*40 + 0.66*38
    expected = 0.59 * 8 + 0.60 * 53.4 + 0.61 * 23.07 + 0.65 * 40 + 0.66 * 38
    assert r.book_depth_usdc_top5 == pytest.approx(expected)
    assert session.calls["/book"] == 1


# ---------------------------------------------------------------------------
# 2. 5xx fallback to /midpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_bbo_5xx_falls_back_to_midpoint():
    session = _FakeSession({
        "/book": [_FakeResp(503)],
        "/midpoint": [_FakeResp(200, {"mid": "0.42"})],
    })
    r = await fetch_bbo(session, "tok_5xx")
    assert r.source == "clob_mid_fallback"
    assert r.mid == pytest.approx(0.42)
    assert r.bid is None
    assert r.ask is None
    assert r.book_depth_usdc_top5 is None
    assert session.calls["/book"] == 1
    assert session.calls["/midpoint"] == 1


@pytest.mark.asyncio
async def test_fetch_bbo_4xx_no_orderbook_falls_back():
    """The CLOB returns 404 with `{"error": "No orderbook ..."}` for
    inactive tokens. We must NOT treat that as a hard failure — fall
    back to /midpoint just like the 5xx path."""
    session = _FakeSession({
        "/book": [_FakeResp(404, {"error": "No orderbook exists"})],
        "/midpoint": [_FakeResp(200, {"mid": "0.5"})],
    })
    r = await fetch_bbo(session, "tok_dead")
    assert r.source == "clob_mid_fallback"
    assert r.mid == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_fetch_bbo_both_endpoints_fail_returns_unavailable():
    session = _FakeSession({
        "/book": [_FakeResp(500)],
        "/midpoint": [_FakeResp(500)],
    })
    r = await fetch_bbo(session, "tok_dead2")
    assert r.source == "unavailable"
    assert r.mid is None
    assert r.bid is None


# ---------------------------------------------------------------------------
# 3. Cache TTL
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_bbo_cache_collapses_calls_within_ttl():
    book = {"bids": [{"price": "0.4", "size": "5"}],
            "asks": [{"price": "0.5", "size": "5"}]}
    # Only one /book response queued — a second call MUST hit the cache.
    session = _FakeSession({"/book": [_FakeResp(200, book)]})
    r1 = await fetch_bbo(session, "tok_cache", cache_ttl=60.0)
    r2 = await fetch_bbo(session, "tok_cache", cache_ttl=60.0)
    assert r1.source == "clob_book"
    assert r2.source == "cache"
    assert r2.bid == r1.bid
    assert r2.ask == r1.ask
    assert r2.latency_ms == 0
    assert session.calls["/book"] == 1


@pytest.mark.asyncio
async def test_fetch_bbo_cache_expires():
    book = {"bids": [{"price": "0.4", "size": "5"}],
            "asks": [{"price": "0.5", "size": "5"}]}
    session = _FakeSession({"/book": [_FakeResp(200, book), _FakeResp(200, book)]})
    # cache_ttl=0 → second call must refetch.
    await fetch_bbo(session, "tok_exp", cache_ttl=0.0)
    await fetch_bbo(session, "tok_exp", cache_ttl=0.0)
    assert session.calls["/book"] == 2


# ---------------------------------------------------------------------------
# 4. bid <= mid <= ask clamping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_bbo_clamps_mid_within_bracket():
    """Book-derived mid is always (bid+ask)/2 → automatically inside
    the bracket. This test asserts that property for a one-tick book."""
    book = {"bids": [{"price": "0.10", "size": "1"}],
            "asks": [{"price": "0.90", "size": "1"}]}
    session = _FakeSession({"/book": [_FakeResp(200, book)]})
    r = await fetch_bbo(session, "tok_wide")
    assert r.bid <= r.mid <= r.ask


# ---------------------------------------------------------------------------
# 5. One-sided book
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_bbo_one_sided_book_uses_midpoint_for_mid():
    """When only one side has resting orders, the BBO row should
    keep the existing side, leave the empty side None, and fall back
    to /midpoint for `mid` so the snap is still useful."""
    book = {
        "bids": [{"price": "0.30", "size": "10"}],
        "asks": [],
    }
    session = _FakeSession({
        "/book": [_FakeResp(200, book)],
        "/midpoint": [_FakeResp(200, {"mid": "0.31"})],
    })
    r = await fetch_bbo(session, "tok_one_sided")
    assert r.source == "clob_book"
    assert r.bid == pytest.approx(0.30)
    assert r.ask is None
    assert r.ask_size is None
    assert r.mid == pytest.approx(0.31)
    assert r.book_depth_usdc_top5 is None


# ---------------------------------------------------------------------------
# 6. _fetch_bbo 4-tuple shim
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_legacy_fetch_bbo_shim_matches_bboresult():
    book = {"bids": [{"price": "0.20", "size": "10"}],
            "asks": [{"price": "0.30", "size": "20"}]}
    session = _FakeSession({"/book": [_FakeResp(200, book)]})
    mid, bid, ask, depth = await _fetch_bbo(session, "tok_shim")
    assert mid == pytest.approx(0.25)
    assert bid == pytest.approx(0.20)
    assert ask == pytest.approx(0.30)
    # depth is single-level top-5: 0.30 * 20 = 6.0
    assert depth == pytest.approx(6.0)


# ---------------------------------------------------------------------------
# 7. Concurrent coalescing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_bbo_coalesces_concurrent_first_fetchers():
    """Two callers racing for the same token must share one HTTP call."""
    book = {"bids": [{"price": "0.5", "size": "5"}],
            "asks": [{"price": "0.6", "size": "5"}]}

    # Slow /book response so both callers enter the in-flight path.
    call_count = {"n": 0}

    @asynccontextmanager
    async def slow_resp():
        call_count["n"] += 1
        await asyncio.sleep(0.05)
        yield _FakeResp(200, book)

    session = MagicMock()
    session.get = lambda url, **kw: slow_resp()

    results = await asyncio.gather(
        fetch_bbo(session, "tok_race"),
        fetch_bbo(session, "tok_race"),
    )
    assert call_count["n"] == 1, (
        f"Concurrent fetches must share one /book call; got {call_count['n']}"
    )
    # One result is the fresh fetch, the other is from the shared future
    # (either way both must report the same BBO). Source is "clob_book"
    # because the second caller awaits the same `_do_fetch_bbo` result.
    assert results[0].bid == results[1].bid == pytest.approx(0.5)
    assert results[0].ask == results[1].ask == pytest.approx(0.6)

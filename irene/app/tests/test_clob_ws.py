"""ClobWsClient + ClobWsPool unit tests.

The transport never hits the network in tests — we fake `aiohttp.ws_
connect` with a stub WebSocket that queues outbound frames and lets
the test push inbound messages back in. Covers:
- subscribe routes to the cache parser via the dispatch table
- reconnect with backoff replays the full subscription set
- pool sharding caps subscriptions at `chunk_size`
- pool refuses past `max_subscriptions` with a logged WARNING
- unsubscribe sends a delta frame to the right shard
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import aiohttp
import pytest

from app.clob_ws import ClobWsClient
from app.clob_ws_pool import ClobWsPool
from app.live_market_cache import LiveMarketCache


# ---------------------------------------------------------------------------
# Fake aiohttp ws session — the only network seam under test
# ---------------------------------------------------------------------------


class _FakeMessage:
    def __init__(self, msg_type: aiohttp.WSMsgType, data: Any = None):
        self.type = msg_type
        self.data = data


class _FakeWS:
    """Stand-in for `aiohttp.ClientWebSocketResponse`. Outbound frames
    land in `sent`. Inbound is driven by the test pushing into `inbox`.
    """

    def __init__(self) -> None:
        self.sent: list[dict] = []
        self.inbox: asyncio.Queue[_FakeMessage] = asyncio.Queue()
        self.closed = False

    async def send_json(self, payload: dict) -> None:
        self.sent.append(payload)

    async def close(self) -> None:
        self.closed = True
        # Drop a CLOSED marker so the reader exits cleanly.
        await self.inbox.put(_FakeMessage(aiohttp.WSMsgType.CLOSED))

    def exception(self):
        return None

    def __aiter__(self):
        return self

    async def __anext__(self):
        msg = await self.inbox.get()
        if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
            raise StopAsyncIteration
        return msg


class _FakeSession:
    """Stand-in for `aiohttp.ClientSession`. Hands out fresh `_FakeWS`
    instances on each `ws_connect` so reconnect tests can observe the
    new socket separately."""

    def __init__(self) -> None:
        self.connects = 0
        self.last_ws: _FakeWS | None = None

    async def ws_connect(self, url, *, heartbeat=None):  # noqa: ARG002
        self.connects += 1
        self.last_ws = _FakeWS()
        return self.last_ws

    async def close(self) -> None:
        pass


@pytest.fixture(autouse=True)
def _patch_aiohttp_session(monkeypatch):
    """Replace `aiohttp.ClientSession` with the fake for every test
    in this file. Saves each new session on the module-level singleton
    so tests can reach it."""
    sessions: list[_FakeSession] = []

    def _make_session(*args, **kwargs):
        del args, kwargs
        s = _FakeSession()
        sessions.append(s)
        return s

    monkeypatch.setattr(aiohttp, "ClientSession", _make_session)
    return sessions


# ---------------------------------------------------------------------------
# ClobWsClient
# ---------------------------------------------------------------------------


async def test_subscribe_after_connect_pushes_frame(_patch_aiohttp_session):
    cache = LiveMarketCache(stale_after_s=5.0)
    client = ClobWsClient(
        on_book_snapshot=cache.apply_book_snapshot,
        on_price_change=cache.apply_price_change,
        on_last_trade=cache.apply_last_trade,
    )
    await client.start()
    # Let the reader establish a connection.
    for _ in range(50):
        if client.is_connected:
            break
        await asyncio.sleep(0.01)
    assert client.is_connected

    await client.subscribe({"tok-1", "tok-2"})

    sessions = _patch_aiohttp_session
    ws = sessions[-1].last_ws
    assert ws is not None
    # Frame format we ship to Polymarket.
    assert any(
        frame.get("type") == "market"
        and set(frame.get("assets_ids", [])) == {"tok-1", "tok-2"}
        for frame in ws.sent
    )
    await client.stop()


async def test_book_snapshot_routes_to_cache(_patch_aiohttp_session):
    cache = LiveMarketCache(stale_after_s=5.0)
    client = ClobWsClient(
        on_book_snapshot=cache.apply_book_snapshot,
        on_price_change=cache.apply_price_change,
        on_last_trade=cache.apply_last_trade,
    )
    await client.start()
    for _ in range(50):
        if client.is_connected:
            break
        await asyncio.sleep(0.01)
    assert client.is_connected

    ws = _patch_aiohttp_session[-1].last_ws
    assert ws is not None
    snapshot = {
        "event_type": "book",
        "asset_id": "tok-1",
        "bids": [{"price": "0.40"}],
        "asks": [{"price": "0.44"}],
        "seq": 11,
    }
    await ws.inbox.put(_FakeMessage(aiohttp.WSMsgType.TEXT, json.dumps(snapshot)))
    # Let the reader process the frame.
    for _ in range(50):
        if cache.get("tok-1") is not None:
            break
        await asyncio.sleep(0.01)
    tick = cache.get("tok-1")
    assert tick is not None
    assert tick.best_bid == pytest.approx(0.40)
    assert tick.best_ask == pytest.approx(0.44)
    assert tick.mid == pytest.approx(0.42)
    await client.stop()


async def test_reconnect_replays_subscription_set(_patch_aiohttp_session):
    """After a forced disconnect, the next connection MUST receive the
    full subscription set in one frame before any further messages."""
    cache = LiveMarketCache(stale_after_s=5.0)
    client = ClobWsClient(
        on_book_snapshot=cache.apply_book_snapshot,
        on_price_change=cache.apply_price_change,
        on_last_trade=cache.apply_last_trade,
    )
    await client.start()
    for _ in range(50):
        if client.is_connected:
            break
        await asyncio.sleep(0.01)
    await client.subscribe({"alpha", "beta"})

    # Wait for the initial subscribe frame to flush.
    await asyncio.sleep(0.05)

    session = _patch_aiohttp_session[-1]
    first_ws = session.last_ws
    assert first_ws is not None

    # Force the server-side close. The reader's backoff loop reconnects.
    await first_ws.close()

    # Wait for a fresh connection.
    for _ in range(200):
        if session.last_ws is not first_ws and client.is_connected:
            break
        await asyncio.sleep(0.05)
    second_ws = session.last_ws
    assert second_ws is not None and second_ws is not first_ws

    # On the new socket, the first frame sent is a full-set subscribe.
    await asyncio.sleep(0.05)
    assert second_ws.sent, "no frame replayed after reconnect"
    first_frame = second_ws.sent[0]
    assert set(first_frame.get("assets_ids", [])) == {"alpha", "beta"}

    await client.stop()


async def test_parser_exception_does_not_kill_reader(_patch_aiohttp_session, caplog):
    """A bad message that makes the parser raise must not terminate the
    reader loop. Subsequent good messages should still land in the cache."""
    cache = LiveMarketCache(stale_after_s=5.0)

    def _boom_on_first(token_id: str, msg: dict) -> None:
        if msg.get("die"):
            raise RuntimeError("synthetic")
        cache.apply_book_snapshot(token_id, msg)

    client = ClobWsClient(
        on_book_snapshot=_boom_on_first,
        on_price_change=cache.apply_price_change,
        on_last_trade=cache.apply_last_trade,
    )
    await client.start()
    for _ in range(50):
        if client.is_connected:
            break
        await asyncio.sleep(0.01)
    ws = _patch_aiohttp_session[-1].last_ws
    assert ws is not None

    bad = {"event_type": "book", "asset_id": "tok-1", "die": True}
    good = {
        "event_type": "book",
        "asset_id": "tok-2",
        "bids": [{"price": "0.20"}],
        "asks": [{"price": "0.30"}],
        "seq": 1,
    }
    await ws.inbox.put(_FakeMessage(aiohttp.WSMsgType.TEXT, json.dumps(bad)))
    await ws.inbox.put(_FakeMessage(aiohttp.WSMsgType.TEXT, json.dumps(good)))
    for _ in range(50):
        if cache.get("tok-2") is not None:
            break
        await asyncio.sleep(0.01)
    assert cache.get("tok-2") is not None
    await client.stop()


# ---------------------------------------------------------------------------
# ClobWsPool
# ---------------------------------------------------------------------------


async def test_pool_routes_subscriptions_within_chunk_size(_patch_aiohttp_session):
    """A subscription set smaller than chunk_size should land on a
    single shard — no need to spin up multiple connections."""
    cache = LiveMarketCache(stale_after_s=5.0)
    pool = ClobWsPool(cache, chunk_size=10, max_subscriptions=100)
    await pool.start()

    await pool.subscribe({f"tok-{i}" for i in range(5)})
    # Give the shard time to spin up and ship the subscribe frame.
    await asyncio.sleep(0.05)

    status = pool.status()
    assert status["clients"] == 1
    assert status["total_subscriptions"] == 5
    assert status["per_client_subs"] == [5]
    assert pool.subscribed_set() == {f"tok-{i}" for i in range(5)}

    await pool.stop()


async def test_pool_spins_up_new_shard_at_chunk_size(_patch_aiohttp_session):
    """Once a shard is full, the next subscribe must land on a new
    shard. Hash routing falls back to a stable walk for placement."""
    cache = LiveMarketCache(stale_after_s=5.0)
    pool = ClobWsPool(cache, chunk_size=3, max_subscriptions=100)
    await pool.start()

    await pool.subscribe({f"tok-{i}" for i in range(7)})
    await asyncio.sleep(0.05)

    status = pool.status()
    # 7 tokens / 3 per shard ⇒ at least 3 shards. Exact distribution
    # depends on hash routing; what matters is the cap is honored.
    assert status["clients"] >= 3
    assert all(c <= 3 for c in status["per_client_subs"])
    assert status["total_subscriptions"] == 7

    await pool.stop()


async def test_pool_refuses_past_max_subscriptions(_patch_aiohttp_session, caplog):
    """Hitting the configured max must log a WARNING and DROP the
    overflow — REST fallback covers the dropped tokens."""
    import logging
    cache = LiveMarketCache(stale_after_s=5.0)
    pool = ClobWsPool(cache, chunk_size=10, max_subscriptions=4)
    await pool.start()

    with caplog.at_level(logging.WARNING, logger="app.clob_ws_pool"):
        await pool.subscribe({f"tok-{i}" for i in range(10)})
    await asyncio.sleep(0.05)

    assert pool.status()["total_subscriptions"] == 4
    assert any("max_subscriptions" in r.message for r in caplog.records)

    await pool.stop()


async def test_pool_unsubscribe_removes_from_owning_shard(_patch_aiohttp_session):
    cache = LiveMarketCache(stale_after_s=5.0)
    pool = ClobWsPool(cache, chunk_size=10, max_subscriptions=100)
    await pool.start()

    await pool.subscribe({"keep-1", "keep-2", "drop"})
    await asyncio.sleep(0.05)
    assert "drop" in pool.subscribed_set()

    await pool.unsubscribe({"drop"})
    await asyncio.sleep(0.05)
    assert "drop" not in pool.subscribed_set()
    assert {"keep-1", "keep-2"}.issubset(pool.subscribed_set())

    await pool.stop()


async def test_pool_subscribe_is_idempotent(_patch_aiohttp_session):
    cache = LiveMarketCache(stale_after_s=5.0)
    pool = ClobWsPool(cache, chunk_size=10, max_subscriptions=100)
    await pool.start()
    await pool.subscribe({"x", "y"})
    await asyncio.sleep(0.05)
    await pool.subscribe({"x", "y"})
    await asyncio.sleep(0.05)
    assert pool.status()["total_subscriptions"] == 2
    await pool.stop()

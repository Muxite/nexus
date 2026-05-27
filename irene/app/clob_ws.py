"""
## Specification

**Purpose.** Single-shard WebSocket transport for Polymarket's CLOB
`/ws/market` channel. Owns one long-lived `aiohttp` connection, an
async reader task, reconnect-with-backoff, and a mutable subscription
set. Inbound messages are routed by `event_type` to caller-provided
parsers (`LiveMarketCache.apply_*`); we do not interpret the payload
here.

**Public interface.**
- `ClobWsClient(on_book_snapshot, on_price_change, on_last_trade, *,
  ws_url, chunk_size, heartbeat_seconds)`
  - `start()` / `stop()` — lifecycle.
  - `subscribe(token_ids)` / `unsubscribe(token_ids)` — mutate the
    set; resends a frame on the open socket (modify-without-reconnect).
  - `subscribed_set()` — current view of the subscription set.
  - `is_connected` — whether the underlying ws is up.

**Invariants.**
- One asyncio task per client (the reader loop). `stop()` cancels it
  cleanly.
- On reconnect, the full subscription set is re-sent in one frame so
  the server's view matches ours from the first message onward.
- The reader catches every exception out of the parser callbacks
  (`apply_*`) and logs at WARNING; one bad message can't kill the loop.

**Side effects.** WebSocket connection to `wss://ws-subscriptions-clob
.polymarket.com/ws/market`.

**Failure modes.**
- Connection failure → exponential backoff with jitter (mirror of the
  Gamma 429 backoff in `markets.py`). Cap at 60s.
- Parser exception → logged, message dropped, loop continues.
- Server-initiated close → backoff + reconnect.

**Non-goals.**
- Subscription sharding across many connections (`ClobWsPool` owns that).
- Parsing message bodies (`LiveMarketCache.apply_*` owns that).
- Authentication (the market channel is public).
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from typing import Awaitable, Callable, Optional

import aiohttp

logger = logging.getLogger(__name__)

CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Mirror the Gamma backoff constants in `app/stages/markets.py:922-963`
# so a future tuner has ONE place to think about Polymarket backoff.
_INITIAL_BACKOFF_S = 1.0
_MAX_BACKOFF_S = 60.0
_HEARTBEAT_S = 15.0

# Polymarket WS messages tag their kind with `event_type`. We pin the
# names but defer envelope-shape decisions to the parser; if a future
# probe reveals a renamed field, only the dispatch table here changes.
EVENT_BOOK = "book"
EVENT_PRICE_CHANGE = "price_change"
EVENT_LAST_TRADE = "last_trade_price"

ParserCb = Callable[[str, dict], None]


class ClobWsClient:
    """One WS connection. Manages a subscription set (mutable without
    reconnect) and routes inbound messages to caller-supplied parsers.

    Designed for use behind `ClobWsPool` — each pool client handles a
    bounded slice of the universe (default 500 tokens) so subscription-
    cap surprises and message-rate spikes stay isolated.
    """

    def __init__(
        self,
        on_book_snapshot: ParserCb,
        on_price_change: ParserCb,
        on_last_trade: ParserCb,
        *,
        ws_url: str = CLOB_WS_URL,
        heartbeat_seconds: float = _HEARTBEAT_S,
    ) -> None:
        self._on_book = on_book_snapshot
        self._on_price = on_price_change
        self._on_trade = on_last_trade
        self._ws_url = ws_url
        self._heartbeat = heartbeat_seconds

        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._reader_task: Optional[asyncio.Task] = None
        # Authoritative copy of "what should be subscribed when the
        # connection is up". Mutating this is cheap; pushing changes
        # to the wire is `_push_subscribe`.
        self._subscriptions: set[str] = set()
        self._lock = asyncio.Lock()
        self._stopped = False

    @property
    def is_connected(self) -> bool:
        return self._ws is not None and not self._ws.closed

    def subscribed_set(self) -> set[str]:
        return set(self._subscriptions)

    async def start(self) -> None:
        if self._reader_task is not None:
            return
        self._stopped = False
        self._session = aiohttp.ClientSession()
        self._reader_task = asyncio.create_task(self._run_forever())

    async def stop(self) -> None:
        self._stopped = True
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._reader_task = None
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
            self._ws = None
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def subscribe(self, token_ids: set[str]) -> None:
        """Add `token_ids` to the subscription set and push to the wire
        if connected. Idempotent — already-subscribed tokens are no-ops."""
        async with self._lock:
            new = token_ids - self._subscriptions
            if not new:
                return
            self._subscriptions.update(new)
            if self.is_connected:
                await self._push_subscribe(new)

    async def unsubscribe(self, token_ids: set[str]) -> None:
        async with self._lock:
            gone = token_ids & self._subscriptions
            if not gone:
                return
            self._subscriptions.difference_update(gone)
            if self.is_connected:
                await self._push_unsubscribe(gone)

    # -- internal: connection lifecycle ----------------------------------

    async def _run_forever(self) -> None:
        """Reader loop with exponential backoff. Survives every error
        short of an explicit `stop()` cancellation."""
        delay = _INITIAL_BACKOFF_S
        while not self._stopped:
            try:
                await self._connect_and_read()
                delay = _INITIAL_BACKOFF_S  # successful run resets the clock
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "clob_ws: connection cycle failed (%s); backing off %.2fs",
                    exc.__class__.__name__, delay,
                )
            if self._stopped:
                return
            # Jitter so a fleet of pool clients doesn't synchronise their
            # reconnect bursts after a server-side incident.
            sleep_s = delay + random.uniform(0.0, 0.5)
            await asyncio.sleep(sleep_s)
            delay = min(delay * 2, _MAX_BACKOFF_S)

    async def _connect_and_read(self) -> None:
        assert self._session is not None
        self._ws = await self._session.ws_connect(
            self._ws_url, heartbeat=self._heartbeat,
        )
        try:
            # First action after every (re)connect: replay the full
            # subscription set so the server's view matches ours.
            if self._subscriptions:
                await self._push_subscribe(self._subscriptions)
            await self._reader_loop()
        finally:
            if self._ws is not None and not self._ws.closed:
                await self._ws.close()
            self._ws = None

    async def _reader_loop(self) -> None:
        assert self._ws is not None
        async for msg in self._ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                self._dispatch(msg.data)
            elif msg.type == aiohttp.WSMsgType.ERROR:
                logger.warning("clob_ws: error frame; reconnecting")
                return
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
                return
            # Ignore PING/PONG/BINARY at this layer; aiohttp handles
            # heartbeat ping/pong via the `heartbeat=` argument.

    def _dispatch(self, raw: str) -> None:
        """Parse one TEXT frame and route to the appropriate `apply_*`
        callback. A single bad message must not kill the loop."""
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError):
            logger.debug("clob_ws: unparseable frame: %r", raw[:120])
            return
        # Polymarket sometimes sends a list-of-events per frame; normalise.
        items = payload if isinstance(payload, list) else [payload]
        for item in items:
            if not isinstance(item, dict):
                continue
            event_type = (
                item.get("event_type")
                or item.get("type")
                or item.get("eventType")
            )
            token_id = (
                item.get("asset_id")
                or item.get("assetId")
                or item.get("token_id")
                or item.get("market")
            )
            if not isinstance(token_id, str) or not token_id:
                continue
            try:
                if event_type == EVENT_BOOK:
                    self._on_book(token_id, item)
                elif event_type == EVENT_PRICE_CHANGE:
                    self._on_price(token_id, item)
                elif event_type == EVENT_LAST_TRADE:
                    self._on_trade(token_id, item)
                else:
                    logger.debug(
                        "clob_ws: unrouted event_type=%r token=%s",
                        event_type, token_id[:16],
                    )
            except Exception:  # noqa: BLE001 — parser must not kill the loop
                logger.exception(
                    "clob_ws: parser raised on token=%s event_type=%r",
                    token_id[:16], event_type,
                )

    # -- internal: wire frames -------------------------------------------

    async def _push_subscribe(self, token_ids: set[str]) -> None:
        """Send a subscribe frame. We use the simpler `{"type":"market",
        "assets_ids":[...]}` shape verified in `scripts/probe_clob_ws.py`.
        If the probe reveals the richer `{"action":"subscribe",...}` form
        is required, only this method changes."""
        if self._ws is None or self._ws.closed:
            return
        frame = {"type": "market", "assets_ids": sorted(token_ids)}
        await self._ws.send_json(frame)

    async def _push_unsubscribe(self, token_ids: set[str]) -> None:
        if self._ws is None or self._ws.closed:
            return
        # Polymarket has historically accepted an `action: "unsubscribe"`
        # variant; if subscription-modify-without-reconnect turns out
        # to be subscribe-only, the pool falls back to dropping the
        # entire chunk's connection (chunk is bounded so churn is small).
        frame = {
            "type": "market",
            "action": "unsubscribe",
            "assets_ids": sorted(token_ids),
        }
        await self._ws.send_json(frame)

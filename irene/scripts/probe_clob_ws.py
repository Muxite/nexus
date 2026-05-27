#!/usr/bin/env python3
"""CLOB WebSocket envelope probe — one-shot.

Why this exists. Phase 2 of the cycle-26 plan (events-discovery + live
WebSocket) introduces `app/clob_ws.py` + `app/clob_ws_pool.py`. Before
we write the production parser in `app/live_market_cache.py`, we need
to VERIFY:

1. The Polymarket CLOB WS URL accepts a subscription with `assets_ids`.
2. The event envelope shape — `event_type`, top-of-book field names,
   sequence/timestamp positioning. The plan assumes `book` / `price_change`
   / `last_trade_price`; this script confirms.
3. Subscription-modify-without-reconnect actually works (community
   guidance, not contract). If it doesn't, the production pool falls
   back to reconnect-per-chunk — sharded design means churn is bounded.

Output: stderr breadcrumbs + JSON-Lines dump of the first MAX_MESSAGES
messages to stdout. Read both before writing the parser body.

Run (no compose needed — just needs network from the host):
    TOKEN_IDS=tok1,tok2 python3 irene/scripts/probe_clob_ws.py > /tmp/clob_ws.jsonl 2>&1

Defaults to two known-liquid markets (BTC $150k YES + NO).
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone

import aiohttp

CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# BTC $150k EOY YES / NO tokens — high-volume, always present.
DEFAULT_TOKEN_IDS = [
    "13915689317269078219168496739008737517740566192006337297676041270492637394586",
    "94403740540569672658671953316574739135411739569013879671293737094049823090841",
]

MAX_MESSAGES = 100
PROBE_DURATION_S = 60.0
# Mid-run subscription swap to exercise modify-without-reconnect. Set
# SKIP_MODIFY=1 to disable.
MODIFY_AFTER_S = 15.0


def _log(msg: str) -> None:
    print(f"[probe] {datetime.now(timezone.utc).isoformat(timespec='seconds')} {msg}",
          file=sys.stderr, flush=True)


async def _modify_subscription(
    ws: aiohttp.ClientWebSocketResponse, token_ids: list[str], action: str,
) -> None:
    """Send a subscribe/unsubscribe frame. The two payload shapes
    Polymarket has shipped historically: top-level `{"type":"market",
    "assets_ids": [...]}` and a richer `{"action": "subscribe",
    "channel": "market", "assets_ids": [...]}`. We try the simpler one
    first; the probe captures whichever the server accepts."""
    frame = {"type": "market", "assets_ids": token_ids}
    await ws.send_json(frame)
    _log(f"sent {action} frame for {len(token_ids)} tokens: {frame}")


async def main() -> None:
    raw = os.environ.get("TOKEN_IDS")
    if raw:
        token_ids = [t.strip() for t in raw.split(",") if t.strip()]
    else:
        token_ids = list(DEFAULT_TOKEN_IDS)
    if not token_ids:
        _log("no token ids configured; exiting")
        sys.exit(2)

    skip_modify = bool(os.environ.get("SKIP_MODIFY"))

    _log(f"connecting to {CLOB_WS_URL}")
    _log(f"initial subscription set: {len(token_ids)} tokens")
    start_mono = time.monotonic()
    msg_count = 0
    second_subscription_sent = False

    timeout = aiohttp.ClientTimeout(total=PROBE_DURATION_S + 30.0)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            ws = await session.ws_connect(CLOB_WS_URL, heartbeat=15.0)
        except Exception as exc:  # noqa: BLE001
            _log(f"ws_connect failed: {exc!r}")
            raise

        try:
            # Initial subscription — pass the first half so we can also
            # exercise an in-flight modify with the second half.
            half = max(1, len(token_ids) // 2)
            first_half = token_ids[:half]
            second_half = token_ids[half:]
            await _modify_subscription(ws, first_half, "initial-subscribe")

            while time.monotonic() - start_mono < PROBE_DURATION_S:
                # Periodically attempt the second-half subscribe to test
                # modify-without-reconnect. Logs the wall-clock at swap
                # time so the consumer can see if message types changed.
                if (
                    not second_subscription_sent
                    and not skip_modify
                    and second_half
                    and (time.monotonic() - start_mono) >= MODIFY_AFTER_S
                ):
                    await _modify_subscription(ws, second_half, "modify-subscribe")
                    second_subscription_sent = True

                if msg_count >= MAX_MESSAGES:
                    _log(f"hit MAX_MESSAGES={MAX_MESSAGES}; exiting")
                    break

                try:
                    msg = await asyncio.wait_for(ws.receive(), timeout=5.0)
                except asyncio.TimeoutError:
                    _log("5s receive timeout — no message; continuing")
                    continue

                if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
                    _log(f"ws closed by server: {msg!r}")
                    break
                if msg.type == aiohttp.WSMsgType.ERROR:
                    _log(f"ws error frame: {ws.exception()!r}")
                    break
                if msg.type != aiohttp.WSMsgType.TEXT:
                    _log(f"unhandled msg type {msg.type}: {msg.data!r}")
                    continue

                try:
                    payload = json.loads(msg.data)
                except (TypeError, ValueError):
                    _log(f"unparseable JSON ({msg.data!r:.80}); continuing")
                    continue

                msg_count += 1
                envelope = {
                    "i": msg_count,
                    "received_at": time.time(),
                    "payload": payload,
                }
                # JSON-Lines so the consumer can grep / jq freely.
                print(json.dumps(envelope), flush=True)
        finally:
            await ws.close()

    _log(f"done — captured {msg_count} messages over "
         f"{time.monotonic() - start_mono:.1f}s")


if __name__ == "__main__":
    asyncio.run(main())

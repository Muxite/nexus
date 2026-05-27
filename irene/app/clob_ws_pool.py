"""
## Specification

**Purpose.** Sharded WebSocket fanout — many small `ClobWsClient`
instances behind a single subscription surface. Designed for the
~18k tokens we'd subscribe to if we cover every tradable market on
Polymarket; one connection per shard caps blast-radius on disconnect,
parallelises read+parse, and gives a single knob to dial up if
Polymarket ever publishes a hard per-connection cap.

**Public interface.**
- `ClobWsPool(cache, *, chunk_size, max_subscriptions, ws_url)`
- `await pool.start()` / `await pool.stop()`
- `await pool.subscribe(token_ids: set[str])`
- `await pool.unsubscribe(token_ids: set[str])`
- `pool.subscribed_set() -> set[str]`
- `pool.status() -> dict` — small metrics dict for the dashboard /
  soak script (per-client connection state, sub count, total subs).

**Invariants.**
- Each token_id is routed to exactly one client via stable hashing —
  `hash(token_id) % len(clients)` — so a given token's updates always
  land on the same connection.
- Client count grows monotonically: when a shard hits `chunk_size`, a
  new client is spun up. Reducing the count would force a rebalance
  (and a temporary subscription gap); we don't bother.
- `subscribe(set)` is idempotent. The pool resolves to per-shard
  add-only deltas; no `ClobWsClient` ever sees a token it's already
  carrying.
- `unsubscribe(set)` is best-effort: when a shard sends an unsub
  frame, the live cache will stop receiving updates for those tokens,
  but the reads stay safe because `LiveMarketCache.is_fresh` catches
  the staleness.

**Side effects.** Owns N aiohttp WebSocket connections to
`wss://ws-subscriptions-clob.polymarket.com/ws/market`.

**Failure modes.**
- Hitting `max_subscriptions` returns the un-subscribed remainder
  via a logged WARNING; callers should not silently assume every
  token is subscribed. Closer / trade stages already fall back to
  REST on cache miss, so the overflow degrades gracefully.

**Non-goals.**
- Dynamic rebalancing on remove. Simpler to let shards run light.
- Cross-connection deduplication. Hash-routing guarantees each token
  lives on exactly one client.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import asyncio

from app.clob_ws import CLOB_WS_URL, ClobWsClient

if TYPE_CHECKING:
    from app.live_market_cache import LiveMarketCache

logger = logging.getLogger(__name__)


def _shard_index(token_id: str, n_shards: int) -> int:
    """Stable shard assignment. Uses Python's builtin `hash` modulo
    n_shards. Built-in `hash` is randomised across processes (PYTHONHASH
    SEED), which is fine — we only need stability within one process
    lifetime. A reconnect doesn't re-route because the shard count is
    monotonic."""
    if n_shards <= 0:
        return 0
    return hash(token_id) % n_shards


class ClobWsPool:
    def __init__(
        self,
        cache: "LiveMarketCache",
        *,
        chunk_size: int = 500,
        max_subscriptions: int = 20_000,
        ws_url: str = CLOB_WS_URL,
    ) -> None:
        self._cache = cache
        self._chunk_size = int(chunk_size)
        self._max_subscriptions = int(max_subscriptions)
        self._ws_url = ws_url
        self._clients: list[ClobWsClient] = []
        # `_subscribed[i]` is the set of tokens currently carried by
        # `_clients[i]`. The live ground truth for "what's subscribed
        # right now" — `ClobWsClient.subscribed_set()` lags by one
        # frame in flight, so we mirror locally.
        self._subscribed: list[set[str]] = []
        # Tasks spawned by `_spawn_shard` to `client.start()`. Tracked
        # so `stop()` can await any still-pending starts before
        # closing the sockets — avoids "no running event loop" tracebacks
        # from a `start()` that resolved between `subscribe` and `stop`.
        self._start_tasks: list[asyncio.Task] = []
        self._started = False

    # -- lifecycle --------------------------------------------------------

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        # Don't preallocate clients — they're created on demand the first
        # time `subscribe` routes a token to a shard index that doesn't
        # exist yet. Keeps the boot path quiet when `subscribe` hasn't
        # been called yet (e.g. between MarketsStage.start and the first
        # poll cycle).

    async def stop(self) -> None:
        if not self._started:
            return
        self._started = False
        # Wait for any in-flight `client.start()` coroutines to finish so
        # we can safely `await client.stop()` against an initialised state.
        if self._start_tasks:
            await asyncio.gather(*self._start_tasks, return_exceptions=True)
        for client in self._clients:
            await client.stop()

    # -- subscription surface --------------------------------------------

    def subscribed_set(self) -> set[str]:
        out: set[str] = set()
        for s in self._subscribed:
            out.update(s)
        return out

    async def subscribe(self, token_ids: set[str]) -> None:
        """Add tokens to the pool. Routes to existing shards (or spins
        up new ones up to `_max_subscriptions / chunk_size`). Tokens that
        would push past the configured `max_subscriptions` are dropped
        with a WARNING — callers are expected to fall back to REST."""
        if not self._started:
            raise RuntimeError("ClobWsPool.start() must be called before subscribe()")

        new = token_ids - self.subscribed_set()
        if not new:
            return

        total_after = len(self.subscribed_set()) + len(new)
        if total_after > self._max_subscriptions:
            allowed = max(0, self._max_subscriptions - len(self.subscribed_set()))
            dropped = len(new) - allowed
            logger.warning(
                "clob_ws_pool: refusing to subscribe %d tokens past the "
                "max_subscriptions cap (%d); kept %d, dropped %d",
                len(new), self._max_subscriptions, allowed, dropped,
            )
            # Truncate deterministically (sorted) so a re-run with the
            # same desired set produces the same kept/dropped partition.
            new = set(sorted(new)[:allowed])
            if not new:
                return

        # Route each token to a shard, growing as we go. Routing
        # consults BOTH `_subscribed` (already-confirmed) and `per_shard`
        # (in-batch additions) so a single subscribe call doesn't pile
        # the whole batch onto one fresh shard.
        per_shard: dict[int, set[str]] = {}
        for token in sorted(new):  # sorted for deterministic placement
            idx = self._route_for_subscribe(token, per_shard)
            per_shard.setdefault(idx, set()).add(token)

        for idx, tokens in per_shard.items():
            client = self._clients[idx]
            self._subscribed[idx].update(tokens)
            await client.subscribe(tokens)

    async def unsubscribe(self, token_ids: set[str]) -> None:
        if not self._started or not self._clients:
            return
        per_shard: dict[int, set[str]] = {}
        for token in token_ids:
            for i, s in enumerate(self._subscribed):
                if token in s:
                    per_shard.setdefault(i, set()).add(token)
                    break
        for idx, tokens in per_shard.items():
            self._subscribed[idx].difference_update(tokens)
            await self._clients[idx].unsubscribe(tokens)

    def status(self) -> dict:
        return {
            "clients": len(self._clients),
            "total_subscriptions": sum(len(s) for s in self._subscribed),
            "per_client_subs": [len(s) for s in self._subscribed],
            "per_client_connected": [c.is_connected for c in self._clients],
            "max_subscriptions": self._max_subscriptions,
            "chunk_size": self._chunk_size,
        }

    # -- internal --------------------------------------------------------

    def _route_for_subscribe(
        self, token_id: str, in_batch: dict[int, set[str]] | None = None,
    ) -> int:
        """Pick a shard index for a new token. Prefers a not-yet-full
        existing shard via stable hashing; if every existing shard is
        full, spins up a new one.

        `in_batch` carries the routing decisions made earlier in the
        same `subscribe` call so we don't pile a fresh batch onto one
        empty shard. None means "no batch state to consider".
        """
        batch = in_batch or {}

        def _projected_size(i: int) -> int:
            return len(self._subscribed[i]) + len(batch.get(i, set()))

        # Try the hash-routed shard first.
        if self._clients:
            primary = _shard_index(token_id, len(self._clients))
            if _projected_size(primary) < self._chunk_size:
                return primary
            # Walk the shards in stable order looking for a slot. Avoids
            # a hot shard locking us out when the hash distribution is
            # uneven for small N.
            for i in range(len(self._clients)):
                if _projected_size(i) < self._chunk_size:
                    return i
        # No room — add a new shard.
        return self._spawn_shard()

    def _spawn_shard(self) -> int:
        client = ClobWsClient(
            on_book_snapshot=self._cache.apply_book_snapshot,
            on_price_change=self._cache.apply_price_change,
            on_last_trade=self._cache.apply_last_trade,
            ws_url=self._ws_url,
        )
        # Fire-and-forget start: the pool's `start()` already ran, so
        # we just kick the reader. The task is tracked on `_start_tasks`
        # so `stop()` can drain it before tearing down sockets.
        self._start_tasks.append(asyncio.create_task(client.start()))
        self._clients.append(client)
        self._subscribed.append(set())
        idx = len(self._clients) - 1
        logger.info(
            "clob_ws_pool: spawned shard #%d (chunk_size=%d, total_clients=%d)",
            idx, self._chunk_size, len(self._clients),
        )
        return idx

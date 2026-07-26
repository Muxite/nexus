"""
## Specification

**Purpose.** In-process pub/sub bus with the contract surface that a future
Redis backend will satisfy unchanged. The bus is the **only** inter-stage
channel (`docs/modularization.md` §2).

**Public interface.**
- `Bus` — protocol every backend implements.
- `InProcBus(BackendQueueConfig)` — v1 implementation backed by
  `asyncio.Queue`. Drop-in replaceable in `main.py` when v2 swaps to Redis.
- `BusDropCounters` — read-only counters surfaced for tests + telemetry.

**Invariants.**
- **Strict-mode JSON roundtrip.** When `IRENE_BUS_STRICT=1` (default in
  tests), every `publish()` runs `json.dumps(payload)` and `json.loads`
  before delivery. A non-JSON value raises `BusPayloadError` — this is the
  enforcement point for `docs/modularization.md` §3 "JSON-dict payloads
  only".
- **Trade-path back-pressure.** `publish()` to a non-telemetry topic blocks
  the caller when the per-topic queue is full. Publishers feel the back-
  pressure and slow down rather than silently dropping a signal.
- **Telemetry drop-first.** A `telemetry.*` topic is non-blocking; full
  queue → silently drop and increment `BusDropCounters.telemetry`. Never
  blocks a publisher.
- **Per-subscriber isolation.** Each subscriber runs in its own task; one
  subscriber's exception is logged and counted but does not propagate to
  siblings or to the publisher.
- `subscribe()` must be called before `start()`; subscribing after the bus
  is running raises `RuntimeError` (catches the wiring bug where a stage
  registers too late and silently misses messages).

**Side effects.** Spawns one asyncio Task per (topic, subscriber). Logs at
WARNING when a telemetry drop happens or a subscriber raises.

**Failure modes.**
- `BusPayloadError` on non-JSON payload (strict mode).
- `RuntimeError` on out-of-order lifecycle (`subscribe` after `start`,
  `publish` before `start`).
- Subscriber exceptions are caught, logged, counted; the bus stays up.

**Non-goals.**
- Persistence (a Redis backend can add it; in-process never persists).
- Topic wildcards (deferred to v2 if needed).
- Capture / decision_inputs writes — that's bus middleware, lands when
  the first stage that needs it ships (rule: bus middleware writes
  `decision_inputs` inside the subscriber's `db.batch()`; see
  `docs/p1_contracts.md` §7). Not implemented in this PR.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Protocol

from app.aioutil import cancel_and_join
from app.topics import is_telemetry

logger = logging.getLogger(__name__)

Handler = Callable[[dict], Awaitable[None]]


class BusPayloadError(ValueError):
    """Raised in strict mode when a payload is not JSON-serialisable."""


@dataclass
class BusDropCounters:
    telemetry: int = 0
    handler_exceptions: int = 0


# ---------------------------------------------------------------------------
# Protocol (the contract a Redis backend will also satisfy)
# ---------------------------------------------------------------------------


class Bus(Protocol):
    async def publish(self, topic: str, payload: dict) -> None: ...
    def subscribe(self, topic: str, handler: Handler) -> None: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...


# ---------------------------------------------------------------------------
# InProcBus — v1 implementation
# ---------------------------------------------------------------------------


@dataclass
class InProcBus:
    """asyncio.Queue-backed bus. One bounded queue per (topic, subscriber)."""

    maxsize: int = 1024
    strict: bool = field(default_factory=lambda: os.environ.get("IRENE_BUS_STRICT", "1") == "1")
    counters: BusDropCounters = field(default_factory=BusDropCounters)
    _subscribers: dict[str, list[tuple[Handler, asyncio.Queue]]] = field(default_factory=dict, init=False, repr=False)
    _tasks: list[asyncio.Task] = field(default_factory=list, init=False, repr=False)
    _started: bool = field(default=False, init=False, repr=False)
    _stopped: bool = field(default=False, init=False, repr=False)

    # ---- lifecycle ------------------------------------------------------

    def subscribe(self, topic: str, handler: Handler) -> None:
        if self._started:
            raise RuntimeError(
                f"cannot subscribe to {topic!r} after bus.start() — "
                "late subscribers silently miss messages. Wire all subscribers in main.py first."
            )
        queue: asyncio.Queue = asyncio.Queue(maxsize=self.maxsize)
        self._subscribers.setdefault(topic, []).append((handler, queue))

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        for topic, subs in self._subscribers.items():
            for handler, queue in subs:
                self._tasks.append(asyncio.create_task(self._run_subscriber(topic, handler, queue)))

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        # Cancel everything first so the workers wind down concurrently,
        # then join them one by one.
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            await cancel_and_join(task, log=logger)
        self._tasks.clear()

    # ---- publish --------------------------------------------------------

    async def publish(self, topic: str, payload: dict) -> None:
        if not self._started:
            raise RuntimeError(
                f"bus.publish({topic!r}) called before bus.start() — "
                "publishers may only fire after wiring is complete."
            )
        if self.strict:
            try:
                # Round-trip enforces JSON-only payloads (modularization rule 3).
                payload = json.loads(json.dumps(payload))
            except (TypeError, ValueError) as exc:
                raise BusPayloadError(
                    f"non-JSON payload on topic {topic!r}: {exc}"
                ) from exc
        subs = self._subscribers.get(topic, ())
        if not subs:
            return  # no subscribers → no-op (a stage may publish before peers wire)
        if is_telemetry(topic):
            self._publish_telemetry(topic, payload, subs)
        else:
            await self._publish_trade_path(topic, payload, subs)

    def _publish_telemetry(self, topic: str, payload: dict, subs) -> None:
        for _, queue in subs:
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                self.counters.telemetry += 1
                logger.warning(
                    "bus telemetry drop on %r (queue full, total drops=%d)",
                    topic,
                    self.counters.telemetry,
                )

    async def _publish_trade_path(self, topic: str, payload: dict, subs) -> None:
        # Trade-path: block until every subscriber accepts. Back-pressure
        # propagates into the publisher (modularization §"Back-pressure").
        for _, queue in subs:
            await queue.put(payload)

    # ---- subscriber loop -----------------------------------------------

    async def _run_subscriber(self, topic: str, handler: Handler, queue: asyncio.Queue) -> None:
        while True:
            payload = await queue.get()
            try:
                await handler(payload)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                self.counters.handler_exceptions += 1
                logger.exception(
                    "bus subscriber for %r raised (count=%d)",
                    topic,
                    self.counters.handler_exceptions,
                )
            finally:
                queue.task_done()

    # ---- introspection (tests) -----------------------------------------

    def queue_depth(self, topic: str) -> int:
        """Sum of queue sizes for all subscribers on `topic`."""
        return sum(q.qsize() for _, q in self._subscribers.get(topic, ()))

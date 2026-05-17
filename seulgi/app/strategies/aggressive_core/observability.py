"""Structured event bus + decision tracer for aggressive_core.

Every guard pass/fail, AI call, order, exit decision is one Event written
to two sinks:
  - in-memory ring buffer (TUI tail / health screen consume this)
  - append-only NDJSON file at data/dashboard/events.ndjson

A single writer task drains the asyncio.Queue so concurrent emitters never
contend on the file handle. If the file sink fails we keep going — the
TUI ring buffer is the canonical source for live observability.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class Event:
    ts: float                                # epoch seconds
    engine_name: str
    phase: str                               # "guard" | "ai" | "order" | "exit" | "alert" | "boot"
    name: str                                # specific event within phase
    headline_id: Optional[str] = None
    condition_id: Optional[str] = None
    trade_id: Optional[int] = None
    level: str = "info"                      # "info" | "warn" | "error"
    payload: dict = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


class EventBus:
    """Async event bus with one durable writer task.

    Use one instance per process; share across guards/exits/strategy.
    Call `start()` once after the event loop is running, and `stop()` on
    shutdown to flush the queue.
    """

    def __init__(
        self,
        *,
        ndjson_path: Optional[Path] = None,
        ring_size: int = 500,
    ) -> None:
        self._queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=10_000)
        self._ring: deque[Event] = deque(maxlen=ring_size)
        self._ndjson_path = ndjson_path
        self._writer_task: Optional[asyncio.Task] = None
        self._stopping = asyncio.Event()
        if ndjson_path is not None:
            try:
                ndjson_path.parent.mkdir(parents=True, exist_ok=True)
            except (PermissionError, OSError) as exc:
                # Best-effort. Ring buffer still works; file sink degrades gracefully.
                logger.warning("event sink path unwritable (%s); ring-only mode", exc)
                self._ndjson_path = None
                return

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if self._writer_task is None:
            self._writer_task = asyncio.create_task(
                self._drain(), name="aggressive_core.events"
            )

    async def stop(self) -> None:
        self._stopping.set()
        if self._writer_task is not None:
            try:
                await asyncio.wait_for(self._writer_task, timeout=5.0)
            except asyncio.TimeoutError:
                self._writer_task.cancel()
            self._writer_task = None

    # ------------------------------------------------------------------
    # Emit
    # ------------------------------------------------------------------

    def emit(
        self,
        *,
        engine_name: str,
        phase: str,
        name: str,
        level: str = "info",
        headline_id: Optional[str] = None,
        condition_id: Optional[str] = None,
        trade_id: Optional[int] = None,
        payload: Optional[dict] = None,
    ) -> Event:
        ev = Event(
            ts=time.time(),
            engine_name=engine_name,
            phase=phase,
            name=name,
            headline_id=headline_id,
            condition_id=condition_id,
            trade_id=trade_id,
            level=level,
            payload=payload or {},
        )
        # Ring is always updated synchronously — TUI must see the event
        # even if the queue is overflowing.
        self._ring.append(ev)
        try:
            self._queue.put_nowait(ev)
        except asyncio.QueueFull:
            # Drop on the file sink; ring buffer is the durable copy in this
            # degraded case. Logged once per 1000 drops to surface backpressure.
            self._dropped = getattr(self, "_dropped", 0) + 1
            if self._dropped % 1000 == 0:
                logger.warning("event queue saturated; dropped %d events", self._dropped)
        return ev

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def recent(self, n: int = 50, phase: Optional[str] = None) -> list[Event]:
        """Return up to `n` most-recent events, optionally filtered by phase."""
        if phase is None:
            buf = list(self._ring)
        else:
            buf = [e for e in self._ring if e.phase == phase]
        return buf[-n:]

    def skip_histogram(self, since_secs: float = 300.0) -> dict[str, int]:
        """Count guard skip reasons in the last `since_secs`. Used by the
        TUI top pane. Phase="guard", level∈{warn,error} or payload.skip
        set means a skip; we key on payload.skip_reason or event.name."""
        cutoff = time.time() - since_secs
        hist: dict[str, int] = {}
        for ev in self._ring:
            if ev.ts < cutoff or ev.phase != "guard":
                continue
            reason = ev.payload.get("skip_reason") or (
                ev.name if ev.level != "info" else None
            )
            if reason:
                hist[reason] = hist.get(reason, 0) + 1
        return hist

    # ------------------------------------------------------------------
    # Writer task
    # ------------------------------------------------------------------

    async def _drain(self) -> None:
        path = self._ndjson_path
        while not self._stopping.is_set() or not self._queue.empty():
            try:
                ev = await asyncio.wait_for(self._queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            if path is None:
                continue
            try:
                # Append one line per event. Best-effort: log on failure but
                # never raise — the writer task must keep running.
                with path.open("a") as f:
                    f.write(ev.to_json() + "\n")
            except Exception as exc:  # noqa: BLE001
                logger.warning("event sink write failed: %s", exc)

"""Async telemetry queue (Phase 4C.2).

## Specification
**Purpose.** Decouple telemetry writes from the trade-critical fast
path. Rows go on a bounded deque; a single background worker drains
and batch-inserts via `db.batch()` (cycle-2 FIX-3 shielded rollback).
**Public interface.** `TelemetryQueue` (ABC), `AsyncTelemetryQueue`,
`TradeWriteRow`, `EngineDecisionsRow`, `DecisionInputRow`, `LLMCallRow`,
`PRIORITY_TRADE`, `PRIORITY_DECISION`, `PRIORITY_SKIP`.
**Invariants.** Priority-0 rows never silently drop. Drops are counted
+ surfaced in `db_health.txt`. A single worker owns the batch.
**Side effects.** Writes `engine_results`, `decision_inputs`, `trades`,
`llm_calls`. On graceful-shutdown timeout writes
`data/telemetry_lost_<ts>.jsonl`.
**Failure modes.** Worker crash mid-batch → shielded ROLLBACK + log +
counter; queue keeps draining.
**Non-goals.** Cross-process queueing; ordering across priority classes.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Priority classes. Lower number = higher priority = harder to drop.
PRIORITY_TRADE = 0      # trades, executed-trade engine_results — never drop.
PRIORITY_DECISION = 1   # decision_inputs, llm_calls — drop under pressure.
PRIORITY_SKIP = 2       # engine_results for skip rows — drop first.


@dataclass
class _RowBase:
    priority: int = PRIORITY_DECISION


@dataclass
class TradeWriteRow(_RowBase):
    """Routes to `db.insert_trade`. Priority-0 (never drop)."""
    kwargs: dict[str, Any] = field(default_factory=dict)
    priority: int = PRIORITY_TRADE


@dataclass
class EngineDecisionsRow(_RowBase):
    """Routes to `db.record_engine_decisions`. Caller sets priority:
    TRADE if any decision executed, else SKIP."""
    engine_name: str = ""
    decisions: list = field(default_factory=list)
    headline_id: Optional[str] = None
    signal_latency_ms: Optional[float] = None


@dataclass
class DecisionInputRow(_RowBase):
    """Routes to `db.insert_decision_input`. Priority-1."""
    kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass
class LLMCallRow(_RowBase):
    """Routes to `db.log_llm_call`. Priority-1."""
    kwargs: dict[str, Any] = field(default_factory=dict)


class TelemetryQueue(ABC):
    """Interface FastPath (A4C.1) targets."""

    @abstractmethod
    async def put(self, row: _RowBase) -> None: ...
    @abstractmethod
    def start(self) -> None: ...
    @abstractmethod
    async def stop(self) -> None: ...
    @property
    @abstractmethod
    def metrics(self) -> dict: ...


class AsyncTelemetryQueue(TelemetryQueue):
    """In-process bounded queue + single background batch-flush worker.

    Drop-oldest backpressure protects priority-0 rows by evicting the
    oldest priority-2 (then priority-1) row when full. If only
    priority-0 rows occupy the queue, `put` raises `QueueFull` so the
    fast path sees the alarm rather than silently losing trade data.
    """

    def __init__(
        self, db, max_size: int = 10_000, batch_size: int = 100,
        batch_interval_ms: int = 1000, lost_dir: str = "data",
        flush_timeout_s: float = 5.0,
    ) -> None:
        self._db = db
        self._max_size = max_size
        self._batch_size = batch_size
        self._batch_interval_s = batch_interval_ms / 1000.0
        self._lost_dir = Path(lost_dir)
        self._flush_timeout_s = flush_timeout_s
        self._buf: deque[_RowBase] = deque()
        self._buf_lock = asyncio.Lock()
        self._not_empty = asyncio.Event()
        self._stop = asyncio.Event()
        self._worker: Optional[asyncio.Task] = None
        self._dropped_skip = 0
        self._dropped_decision = 0
        self._flush_batches = 0
        self._flush_errors = 0
        self._warned_80 = False

    async def put(self, row: _RowBase) -> None:
        async with self._buf_lock:
            if len(self._buf) >= self._max_size:
                if self._evict_oldest_non_trade_locked() is None:
                    logger.error(
                        "[telemetry] queue full of PRIORITY_TRADE rows — "
                        "refusing %s", type(row).__name__)
                    raise asyncio.QueueFull("telemetry queue full of priority-0 rows")
            self._buf.append(row)
            self._not_empty.set()
            depth = len(self._buf)
        if depth > self._max_size * 0.8 and not self._warned_80:
            self._warned_80 = True
            logger.warning("[telemetry] queue depth %d > 80%% of max=%d",
                           depth, self._max_size)
        elif depth < self._max_size * 0.5:
            self._warned_80 = False

    def start(self) -> None:
        if self._worker is None or self._worker.done():
            self._stop.clear()
            self._worker = asyncio.create_task(self._run(), name="telemetry-worker")

    async def stop(self) -> None:
        self._stop.set()
        self._not_empty.set()
        if self._worker is None:
            return
        try:
            await asyncio.wait_for(self._worker, timeout=self._flush_timeout_s)
        except asyncio.TimeoutError:
            logger.warning("[telemetry] worker did not drain within %.1fs — "
                           "spilling %d rows", self._flush_timeout_s, len(self._buf))
            self._worker.cancel()
            try:
                await self._worker
            except asyncio.CancelledError:
                # Only our own cancel of the worker is expected here. A
                # pending cancellation request on *this* task means the
                # caller is being cancelled and the error is just passing
                # through — re-raise rather than discard it.
                current = asyncio.current_task()
                if current is not None and current.cancelling() > 0:
                    raise
            except Exception:  # noqa: BLE001
                logger.exception("[telemetry] worker raised during shutdown")
            await self._spill_remaining()

    @property
    def metrics(self) -> dict:
        return {
            "depth": len(self._buf), "max": self._max_size,
            "dropped_skip": self._dropped_skip,
            "dropped_decision": self._dropped_decision,
            "flush_batches": self._flush_batches,
            "flush_errors": self._flush_errors,
        }

    async def _run(self) -> None:
        while not self._stop.is_set() or self._buf:
            try:
                await asyncio.wait_for(self._not_empty.wait(),
                                       timeout=self._batch_interval_s)
            except asyncio.TimeoutError:
                pass
            batch = await self._drain_batch()
            if not batch:
                continue
            try:
                await self._flush(batch)
                self._flush_batches += 1
            except Exception as exc:  # noqa: BLE001
                self._flush_errors += 1
                logger.error("[telemetry] flush failed (n=%d) — dropping batch: %r",
                             len(batch), exc, exc_info=True)

    async def _drain_batch(self) -> list[_RowBase]:
        async with self._buf_lock:
            n = min(self._batch_size, len(self._buf))
            batch = [self._buf.popleft() for _ in range(n)]
            if not self._buf:
                self._not_empty.clear()
        return batch

    async def _flush(self, batch: list[_RowBase]) -> None:
        async with self._db.batch():
            for row in batch:
                await self._dispatch(row)

    async def _dispatch(self, row: _RowBase) -> None:
        if isinstance(row, TradeWriteRow):
            await self._db.insert_trade(**row.kwargs)
        elif isinstance(row, EngineDecisionsRow):
            await self._db.record_engine_decisions(
                engine_name=row.engine_name, decisions=row.decisions,
                headline_id=row.headline_id,
                signal_latency_ms=row.signal_latency_ms,
            )
        elif isinstance(row, DecisionInputRow):
            await self._db.insert_decision_input(**row.kwargs)
        elif isinstance(row, LLMCallRow):
            await self._db.log_llm_call(**row.kwargs)
        else:
            logger.warning("[telemetry] unknown row type: %s", type(row).__name__)

    def _evict_oldest_non_trade_locked(self) -> Optional[_RowBase]:
        """Caller holds `_buf_lock`. Drops oldest priority-2, then -1."""
        for target in (PRIORITY_SKIP, PRIORITY_DECISION):
            for idx, r in enumerate(self._buf):
                if r.priority == target:
                    del self._buf[idx]
                    if target == PRIORITY_SKIP:
                        self._dropped_skip += 1
                    else:
                        self._dropped_decision += 1
                    return r
        return None

    async def _spill_remaining(self) -> None:
        if not self._buf:
            return
        try:
            self._lost_dir.mkdir(parents=True, exist_ok=True)
            path = self._lost_dir / f"telemetry_lost_{int(time.time())}.jsonl"
            with path.open("w") as f:
                while self._buf:
                    r = self._buf.popleft()
                    try:
                        data = asdict(r)
                    except Exception:  # noqa: BLE001
                        data = {"repr": repr(r)}
                    f.write(json.dumps(
                        {"type": type(r).__name__, "data": data}, default=str,
                    ) + "\n")
            logger.warning("[telemetry] spilled remaining rows to %s", path)
        except Exception as exc:  # noqa: BLE001
            logger.error("[telemetry] spill failed: %r", exc)

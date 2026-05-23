"""
## Specification

**Purpose.** Provide an in-memory profiling primitive: a `Timer` context
manager that records the elapsed milliseconds of a code block, and a
`ProfileStore` that holds bounded rolling windows of samples per
named op. Percentile queries serve the telemetry flush that publishes
`telemetry.timing` payloads to the bus (the flush itself lives elsewhere
— this module only stores samples).

**Public interface.**
- `Timer(op: str, store: ProfileStore)` — synchronous context manager.
  - `with Timer("op_name", store): ...` records one sample under `op_name`
    on exit. Records the sample even if the inner block raises, then
    re-raises the original exception.
- `ProfileStore`
  - `ProfileStore(*, window: int = 200)` — per-op rolling window size.
  - `store.sample(op: str, ms: float) -> None` — record one timing.
  - `store.percentiles(op: str) -> dict[str, float]` — returns
    `{"p50": float, "p95": float, "p99": float, "count": int}`.
    Unknown / empty op returns `{"p50": 0.0, "p95": 0.0, "p99": 0.0,
    "count": 0}`.
  - `store.ops() -> list[str]` — list of op names with at least one
    sample.

**Invariants.**
- Each op keeps at most `window` most-recent samples; older samples are
  evicted in FIFO order (backed by `collections.deque(maxlen=window)`).
- All elapsed times are measured with `time.monotonic_ns()` so a wall-
  clock jump cannot produce a negative or nonsense sample.
- `Timer` records exactly one sample per `with` block — including when
  the block raises.
- `ProfileStore` instances hold their own state; this module has no
  module-level mutable state (modularization rule 5).

**Side effects.** None. Pure in-memory storage.

**Failure modes.** None expected at the API boundary: `sample()` accepts
any float; `percentiles()` on an unknown op returns zeros rather than
raising. `Timer` propagates any exception the inner block raises after
recording the sample.

**Non-goals.** Async variant, decorator form, JSON export, bus flush,
file output. The bus integration that ships `percentiles()` snapshots as
`telemetry.timing` payloads lives in the wiring code, not here.
"""

from __future__ import annotations

import time
from collections import deque
from typing import Deque


class ProfileStore:
    def __init__(self, *, window: int = 200) -> None:
        if window <= 0:
            raise ValueError(f"window must be positive, got {window!r}")
        self._window = window
        self._samples: dict[str, Deque[float]] = {}

    def sample(self, op: str, ms: float) -> None:
        dq = self._samples.get(op)
        if dq is None:
            dq = deque(maxlen=self._window)
            self._samples[op] = dq
        dq.append(float(ms))

    def percentiles(self, op: str) -> dict[str, float]:
        dq = self._samples.get(op)
        if not dq:
            return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "count": 0}
        ordered = sorted(dq)
        n = len(ordered)
        return {
            "p50": ordered[_rank(n, 0.50)],
            "p95": ordered[_rank(n, 0.95)],
            "p99": ordered[_rank(n, 0.99)],
            "count": n,
        }

    def ops(self) -> list[str]:
        return [op for op, dq in self._samples.items() if len(dq) > 0]


def _rank(n: int, q: float) -> int:
    idx = int(q * n)
    if idx >= n:
        idx = n - 1
    return idx


class Timer:
    """Context manager. `with Timer("op", store): ...` records the
    elapsed time of the block under `op` in `store`."""

    __slots__ = ("_op", "_store", "_t0")

    def __init__(self, op: str, store: ProfileStore) -> None:
        self._op = op
        self._store = store
        self._t0: int = 0

    def __enter__(self) -> "Timer":
        self._t0 = time.monotonic_ns()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        ms = (time.monotonic_ns() - self._t0) / 1_000_000.0
        self._store.sample(self._op, ms)
        return None

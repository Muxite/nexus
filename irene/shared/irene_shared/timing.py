"""
Timing profiler and jittered sleep for Irene services.

Two things live here:

  1. Timer — context manager that measures wall time per operation and
     accumulates stats in a per-process ProfileStore. Results are logged
     as structured JSON and can be published to Redis for the monitor.

  2. jittered_sleep — async sleep with ±jitter to prevent thundering-herd
     against the Gamma API, NewsAPI, RSS feeds, and the DB.

Usage:
    store = ProfileStore()
    with Timer("scanner", "gamma_fetch", store) as t:
        results = await client.fetch_open_markets()
    # t.duration_ms is now set; store has one sample for "gamma_fetch"

    await jittered_sleep(120, jitter_fraction=0.20)
    # sleeps between 96s and 144s
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ProfileStore — in-memory ring buffer of timing samples
# ---------------------------------------------------------------------------


@dataclass
class _Sample:
    duration_ms: float
    ok: bool


@dataclass
class OperationStats:
    """Aggregate stats for one named operation."""

    operation: str
    count: int
    mean_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    error_rate: float   # 0.0 – 1.0
    last_ms: float

    def as_dict(self) -> dict:
        return {
            "operation": self.operation,
            "count": self.count,
            "mean_ms": round(self.mean_ms, 2),
            "p50_ms": round(self.p50_ms, 2),
            "p95_ms": round(self.p95_ms, 2),
            "p99_ms": round(self.p99_ms, 2),
            "error_rate": round(self.error_rate, 4),
            "last_ms": round(self.last_ms, 2),
        }


class ProfileStore:
    """
    Per-process ring buffer of timing samples keyed by operation name.

    Thread-safe: uses no shared mutable state beyond the deques, which are
    GIL-protected for append/popleft in CPython.
    """

    def __init__(self, window: int = 200) -> None:
        self._window = window
        self._samples: dict[str, deque[_Sample]] = {}

    def record(self, operation: str, duration_ms: float, ok: bool = True) -> None:
        """Add one sample. Oldest sample is evicted when window is full."""
        if operation not in self._samples:
            self._samples[operation] = deque(maxlen=self._window)
        self._samples[operation].append(_Sample(duration_ms=duration_ms, ok=ok))

    def summary(self) -> dict[str, OperationStats]:
        """Return aggregate stats for every recorded operation."""
        result = {}
        for op, samples in self._samples.items():
            if not samples:
                continue
            durations = sorted(s.duration_ms for s in samples)
            errors = sum(1 for s in samples if not s.ok)
            n = len(durations)
            result[op] = OperationStats(
                operation=op,
                count=n,
                mean_ms=sum(durations) / n,
                p50_ms=_percentile(durations, 50),
                p95_ms=_percentile(durations, 95),
                p99_ms=_percentile(durations, 99),
                error_rate=errors / n,
                last_ms=samples[-1].duration_ms,
            )
        return result

    def summary_as_dict(self) -> dict[str, dict]:
        return {op: stats.as_dict() for op, stats in self.summary().items()}

    def clear(self) -> None:
        self._samples.clear()


def _percentile(sorted_values: list[float], pct: float) -> float:
    """Nearest-rank percentile of a pre-sorted list."""
    if not sorted_values:
        return 0.0
    idx = max(0, math.ceil(pct / 100 * len(sorted_values)) - 1)
    return sorted_values[idx]


# ---------------------------------------------------------------------------
# Timer — context manager
# ---------------------------------------------------------------------------


class Timer:
    """
    Measures wall time for a block and records the sample in a ProfileStore.

    Usage:
        with Timer("scanner", "gamma_fetch", store) as t:
            ...
        print(t.duration_ms)

    Logs a structured JSON line at DEBUG level on exit. Marks the sample as
    failed (ok=False) if an exception propagates out of the block.
    """

    def __init__(
        self,
        service: str,
        operation: str,
        store: Optional[ProfileStore] = None,
    ) -> None:
        self._service = service
        self._operation = operation
        self._store = store
        self._start: float = 0.0
        self._ok: bool = True
        self.duration_ms: float = 0.0

    def __enter__(self) -> "Timer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        elapsed = (time.perf_counter() - self._start) * 1000.0
        self.duration_ms = elapsed
        self._ok = exc_type is None

        if self._store is not None:
            self._store.record(self._operation, elapsed, ok=self._ok)

        logger.debug(
            "%s",
            json.dumps({
                "service": self._service,
                "operation": self._operation,
                "duration_ms": round(elapsed, 2),
                "ok": self._ok,
            }),
        )
        return False  # never suppress exceptions


# ---------------------------------------------------------------------------
# Jittered sleep
# ---------------------------------------------------------------------------


def _jitter_seconds(
    base_seconds: float,
    jitter_fraction: float,
    rng: random.Random,
) -> float:
    """
    Compute a jittered sleep duration.

    Result is base_seconds * (1 + uniform(-jitter_fraction, +jitter_fraction)),
    clamped to [0, 2 * base_seconds].
    """
    offset = rng.uniform(-jitter_fraction, jitter_fraction) * base_seconds
    return max(0.0, min(base_seconds + offset, 2.0 * base_seconds))


async def jittered_sleep(
    base_seconds: float,
    jitter_fraction: float = 0.20,
    rng: Optional[random.Random] = None,
) -> float:
    """
    Async sleep for base_seconds ± (jitter_fraction * base_seconds).

    Prevents thundering-herd when multiple services or loops share the same
    poll interval. Each call draws a fresh random offset.

    Args:
        base_seconds:   Nominal sleep duration.
        jitter_fraction: Fraction of base_seconds to add/subtract randomly.
                         0.20 means ±20% (sleeps 80–120% of base).
        rng:            Optional seeded random.Random instance. If None, a
                        module-level Random() is used (not the global state).

    Returns:
        The actual sleep duration in seconds.
    """
    _rng = rng or _MODULE_RNG
    actual = _jitter_seconds(base_seconds, jitter_fraction, _rng)
    logger.debug(
        "jittered_sleep: base=%.1fs jitter=±%.0f%% actual=%.2fs",
        base_seconds, jitter_fraction * 100, actual,
    )
    await asyncio.sleep(actual)
    return actual


# Module-level RNG — not the global `random` state, so user code's random
# calls aren't affected.
_MODULE_RNG = random.Random()

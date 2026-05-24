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

**Sampled hot-path variant.** `SampledTimer` extends `Timer` with two
properties needed when measurement itself is hot enough to skew the
thing being measured:
- **Random sampling.** A per-instance `sample_rate` in `[0,1]`. When a
  `with` block fires, the probe rolls once: if the roll wins, the
  block is timed and a sample is recorded; otherwise the block is
  passed through with only the cost of one `random()` call.
- **Self-overhead subtraction.** A `overhead_ns` parameter encodes the
  measured cost of the probe's own enter/record/exit code. The probe
  subtracts that from every recorded elapsed time so the stored
  sample approximates the work being timed, not work-plus-probe.
  `calibrate_timer_overhead()` measures this overhead at startup;
  callers pass its return value to every `SampledTimer` they construct.

**Non-goals.** Async variant, decorator form, JSON export, bus flush,
file output. The bus integration that ships `percentiles()` snapshots as
`telemetry.timing` payloads lives in the wiring code, not here.
"""

from __future__ import annotations

import random as _random_module
import time
from collections import deque
from typing import Deque, Optional


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


class SampledTimer:
    """Sampled, self-overhead-subtracting context manager.

    For hot-path code where unconditional `Timer` would measurably skew
    the thing being measured. The probe samples `sample_rate` of `with`
    invocations and subtracts a calibrated `overhead_ns` from each
    recorded measurement.

    The `recorded` attribute (after exit) tells the caller whether THIS
    invocation was sampled — useful for guarding follow-on log writes
    so non-sampled invocations stay cheap.

    Usage:
        overhead_ns = calibrate_timer_overhead()
        timer = SampledTimer("extract_llm_call", store,
                             sample_rate=0.1, overhead_ns=overhead_ns)
        with timer:
            do_work()
        if timer.recorded:
            structured_log("extract", ...)
    """

    __slots__ = (
        "_op", "_store", "_sample_rate", "_overhead_ns",
        "_rng", "_t0", "recorded",
    )

    def __init__(
        self,
        op: str,
        store: ProfileStore,
        *,
        sample_rate: float = 0.1,
        overhead_ns: int = 0,
        rng: Optional[_random_module.Random] = None,
    ) -> None:
        if not 0.0 <= sample_rate <= 1.0:
            raise ValueError(
                f"sample_rate must be in [0.0, 1.0], got {sample_rate!r}"
            )
        if overhead_ns < 0:
            raise ValueError(
                f"overhead_ns must be >= 0, got {overhead_ns!r}"
            )
        self._op = op
        self._store = store
        self._sample_rate = float(sample_rate)
        self._overhead_ns = int(overhead_ns)
        self._rng = rng if rng is not None else _random_module
        self._t0: int = 0
        self.recorded: bool = False

    def __enter__(self) -> "SampledTimer":
        # Cheap path when not sampling: one random() + compare, no time
        # call, no store write. The point of the probe is exactly that
        # this path is fast.
        if self._rng.random() < self._sample_rate:
            self._t0 = time.monotonic_ns()
            self.recorded = True
        else:
            self.recorded = False
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if not self.recorded:
            return None
        elapsed_ns = time.monotonic_ns() - self._t0
        corrected_ns = elapsed_ns - self._overhead_ns
        if corrected_ns < 0:
            corrected_ns = 0
        self._store.sample(self._op, corrected_ns / 1_000_000.0)
        return None


def calibrate_timer_overhead(*, iters: int = 5000) -> int:
    """Measure the median nanoseconds of `SampledTimer` enter+exit
    overhead.

    Runs an empty `with` block `iters` times at `sample_rate=1.0` so
    every iteration takes the recording path. Returns the median of the
    measured nanoseconds. Pass the return value to every `SampledTimer`
    constructed in production so the recorded samples reflect the work
    being measured, not the probe.

    The median (not the mean) is reported so a single GC pause or
    scheduler hiccup doesn't inflate the subtraction floor — most calls
    should see ≈ the median overhead.
    """
    store = ProfileStore(window=iters)
    # Use a dedicated RNG so we don't shift the global random state.
    rng = _random_module.Random(0xC0FFEE)
    samples_ns: list[int] = []
    for _ in range(iters):
        timer = SampledTimer(
            "__calibration__",
            store,
            sample_rate=1.0,
            overhead_ns=0,
            rng=rng,
        )
        t0 = time.monotonic_ns()
        with timer:
            pass
        samples_ns.append(time.monotonic_ns() - t0)
    samples_ns.sort()
    return samples_ns[len(samples_ns) // 2]

from __future__ import annotations

import random

import pytest

from app.timing import ProfileStore, SampledTimer, Timer, calibrate_timer_overhead


def test_timer_records_sample_under_named_op() -> None:
    store = ProfileStore()
    with Timer("work", store):
        pass
    stats = store.percentiles("work")
    assert stats["count"] == 1
    assert stats["p50"] >= 0.0
    assert store.ops() == ["work"]


def test_timer_records_then_reraises_on_exception() -> None:
    store = ProfileStore()
    with pytest.raises(RuntimeError, match="boom"):
        with Timer("flaky", store):
            raise RuntimeError("boom")
    stats = store.percentiles("flaky")
    assert stats["count"] == 1


def test_rolling_window_evicts_old_samples() -> None:
    store = ProfileStore(window=3)
    for ms in (1.0, 2.0, 3.0, 4.0, 5.0):
        store.sample("op", ms)
    stats = store.percentiles("op")
    assert stats["count"] == 3
    # Only the last three (3, 4, 5) should remain — p50 == 4.0.
    assert stats["p50"] == 4.0
    assert stats["p99"] == 5.0


def test_percentiles_known_input() -> None:
    store = ProfileStore()
    for ms in range(10, 101, 10):  # 10, 20, ..., 100
        store.sample("calc", float(ms))
    stats = store.percentiles("calc")
    assert stats["count"] == 10
    # rank: int(0.50 * 10) == 5 -> sorted[5] == 60.0
    assert stats["p50"] == 60.0
    # rank: int(0.95 * 10) == 9 -> sorted[9] == 100.0
    assert stats["p95"] == 100.0
    assert stats["p99"] == 100.0


def test_empty_op_returns_zeros_with_count_zero() -> None:
    store = ProfileStore()
    stats = store.percentiles("missing")
    assert stats == {"p50": 0.0, "p95": 0.0, "p99": 0.0, "count": 0}


def test_ops_lists_distinct_sampled_ops() -> None:
    store = ProfileStore()
    store.sample("a", 1.0)
    store.sample("b", 2.0)
    store.sample("a", 3.0)
    assert sorted(store.ops()) == ["a", "b"]


def test_invalid_window_rejected() -> None:
    with pytest.raises(ValueError):
        ProfileStore(window=0)


def test_timer_measures_nonzero_elapsed_for_sleep() -> None:
    import time as _time

    store = ProfileStore()
    with Timer("sleep", store):
        _time.sleep(0.005)
    stats = store.percentiles("sleep")
    assert stats["count"] == 1
    assert stats["p50"] >= 4.0  # at least ~4ms; slack for scheduler jitter


# ---------------------------------------------------------------------------
# SampledTimer
# ---------------------------------------------------------------------------


class _ScriptedRng:
    """Deterministic RNG that returns a pre-set sequence from random()."""

    def __init__(self, values: list[float]) -> None:
        self._values = list(values)
        self._i = 0

    def random(self) -> float:
        v = self._values[self._i % len(self._values)]
        self._i += 1
        return v


def test_sampled_timer_records_when_sampling_fires() -> None:
    # sample_rate=1.0 → every call records.
    store = ProfileStore()
    timer = SampledTimer("op", store, sample_rate=1.0)
    with timer:
        pass
    assert timer.recorded
    assert store.percentiles("op")["count"] == 1


def test_sampled_timer_skips_when_sampling_misses() -> None:
    # sample_rate=0.0 → nothing recorded ever.
    store = ProfileStore()
    timer = SampledTimer("op", store, sample_rate=0.0)
    with timer:
        pass
    assert not timer.recorded
    assert store.percentiles("op")["count"] == 0


def test_sampled_timer_with_scripted_rng_records_only_winners() -> None:
    # Three rolls: 0.05 (win), 0.50 (lose), 0.05 (win) — at rate 0.1.
    store = ProfileStore()
    rng = _ScriptedRng([0.05, 0.50, 0.05])
    timer = SampledTimer("op", store, sample_rate=0.1, rng=rng)
    seen: list[bool] = []
    for _ in range(3):
        with timer:
            pass
        seen.append(timer.recorded)
    assert seen == [True, False, True]
    assert store.percentiles("op")["count"] == 2


def test_sampled_timer_subtracts_overhead() -> None:
    # Force a huge overhead claim: every recorded sample is clamped at 0.
    store = ProfileStore()
    timer = SampledTimer(
        "op", store, sample_rate=1.0, overhead_ns=10_000_000_000,  # 10s
    )
    with timer:
        pass
    assert store.percentiles("op")["p50"] == 0.0


def test_sampled_timer_overhead_subtraction_never_negative() -> None:
    """Even if overhead_ns is wildly over-estimated, recorded samples
    floor at 0 rather than going negative."""
    store = ProfileStore()
    timer = SampledTimer(
        "op", store, sample_rate=1.0, overhead_ns=int(1e15),
    )
    with timer:
        pass
    stats = store.percentiles("op")
    assert stats["count"] == 1
    assert stats["p50"] >= 0.0


def test_sampled_timer_invalid_sample_rate_rejected() -> None:
    store = ProfileStore()
    with pytest.raises(ValueError):
        SampledTimer("op", store, sample_rate=1.5)
    with pytest.raises(ValueError):
        SampledTimer("op", store, sample_rate=-0.1)


def test_sampled_timer_invalid_overhead_rejected() -> None:
    store = ProfileStore()
    with pytest.raises(ValueError):
        SampledTimer("op", store, overhead_ns=-1)


def test_calibrate_returns_nonnegative_int() -> None:
    overhead = calibrate_timer_overhead(iters=200)
    assert isinstance(overhead, int)
    assert overhead >= 0


def test_calibrate_overhead_subtraction_reduces_recorded_idle_time() -> None:
    """Calibrating then passing the overhead back to a probe pulls
    the recorded median for an empty `with` block close to zero."""
    overhead_ns = calibrate_timer_overhead(iters=500)
    store = ProfileStore()
    timer = SampledTimer(
        "idle", store, sample_rate=1.0, overhead_ns=overhead_ns,
    )
    for _ in range(50):
        with timer:
            pass
    p50_ms = store.percentiles("idle")["p50"]
    # An empty block, corrected by its own overhead, should sit very
    # near zero. Slack for scheduler jitter is generous.
    assert p50_ms < 0.05  # < 50µs

from __future__ import annotations

import pytest

from app.timing import ProfileStore, Timer


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

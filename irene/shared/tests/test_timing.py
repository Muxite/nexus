"""Unit tests for irene_shared.timing — Timer, ProfileStore, jittered_sleep."""

from __future__ import annotations

import asyncio
import random
import time

import pytest

from irene_shared.timing import (
    ProfileStore,
    Timer,
    OperationStats,
    _jitter_seconds,
    _percentile,
    jittered_sleep,
)


# ---------------------------------------------------------------------------
# _percentile
# ---------------------------------------------------------------------------


class TestPercentile:
    def test_median_odd(self):
        assert _percentile([1, 2, 3, 4, 5], 50) == 3

    def test_median_even(self):
        # Nearest-rank: ceil(50/100 * 4) - 1 = 1 → index 1 → 2
        assert _percentile([1, 2, 3, 4], 50) == 2

    def test_p100_is_max(self):
        assert _percentile([10, 20, 30], 100) == 30

    def test_p0_ish_is_min(self):
        # ceil(1/100 * 3) - 1 = 0 → index 0
        assert _percentile([5, 10, 15], 1) == 5

    def test_empty_returns_zero(self):
        assert _percentile([], 50) == 0.0

    def test_single_element(self):
        assert _percentile([42.0], 95) == 42.0


# ---------------------------------------------------------------------------
# _jitter_seconds
# ---------------------------------------------------------------------------


class TestJitterSeconds:
    def setup_method(self):
        self.rng = random.Random(42)

    def test_result_within_bounds(self):
        base = 100.0
        for _ in range(200):
            result = _jitter_seconds(base, 0.20, self.rng)
            assert 0.0 <= result <= 2.0 * base, f"Out of bounds: {result}"

    def test_result_near_base(self):
        base = 60.0
        results = [_jitter_seconds(base, 0.20, self.rng) for _ in range(1000)]
        mean = sum(results) / len(results)
        assert abs(mean - base) < 5.0, f"Mean too far from base: {mean}"

    def test_zero_jitter_returns_base(self):
        rng = random.Random(0)
        result = _jitter_seconds(100.0, 0.0, rng)
        assert result == pytest.approx(100.0)

    def test_never_negative(self):
        rng = random.Random(0)
        # Even with 100% jitter it shouldn't go below 0
        for _ in range(200):
            result = _jitter_seconds(5.0, 1.0, rng)
            assert result >= 0.0

    def test_spread_is_random(self):
        rng = random.Random(99)
        results = {_jitter_seconds(10.0, 0.50, rng) for _ in range(20)}
        # With 50% jitter and 20 samples, very unlikely to get all identical
        assert len(results) > 1


# ---------------------------------------------------------------------------
# ProfileStore
# ---------------------------------------------------------------------------


class TestProfileStore:
    def test_empty_store_has_no_summary(self):
        store = ProfileStore()
        assert store.summary() == {}

    def test_record_and_retrieve(self):
        store = ProfileStore(window=10)
        store.record("op_a", 100.0, ok=True)
        summary = store.summary()
        assert "op_a" in summary
        assert summary["op_a"].count == 1
        assert summary["op_a"].mean_ms == pytest.approx(100.0)
        assert summary["op_a"].last_ms == pytest.approx(100.0)

    def test_window_evicts_old_samples(self):
        store = ProfileStore(window=3)
        for i in range(5):
            store.record("op", float(i * 10), ok=True)
        assert store.summary()["op"].count == 3

    def test_multiple_operations(self):
        store = ProfileStore()
        store.record("fetch", 50.0)
        store.record("embed", 200.0)
        store.record("fetch", 60.0)
        summary = store.summary()
        assert "fetch" in summary and "embed" in summary
        assert summary["fetch"].count == 2
        assert summary["embed"].count == 1

    def test_error_rate(self):
        store = ProfileStore()
        store.record("op", 10.0, ok=True)
        store.record("op", 10.0, ok=True)
        store.record("op", 10.0, ok=False)
        assert store.summary()["op"].error_rate == pytest.approx(1 / 3)

    def test_zero_error_rate(self):
        store = ProfileStore()
        store.record("op", 5.0, ok=True)
        assert store.summary()["op"].error_rate == pytest.approx(0.0)

    def test_percentiles(self):
        store = ProfileStore(window=100)
        # Known distribution: 1..100ms
        for ms in range(1, 101):
            store.record("op", float(ms))
        stats = store.summary()["op"]
        assert stats.p50_ms == pytest.approx(50.0)
        assert stats.p95_ms == pytest.approx(95.0)
        assert stats.p99_ms == pytest.approx(99.0)

    def test_summary_as_dict(self):
        store = ProfileStore()
        store.record("op", 42.0)
        d = store.summary_as_dict()
        assert isinstance(d, dict)
        assert "op" in d
        assert d["op"]["mean_ms"] == pytest.approx(42.0)
        assert isinstance(d["op"]["count"], int)

    def test_clear(self):
        store = ProfileStore()
        store.record("op", 1.0)
        store.clear()
        assert store.summary() == {}


# ---------------------------------------------------------------------------
# Timer
# ---------------------------------------------------------------------------


class TestTimer:
    def test_measures_duration(self):
        store = ProfileStore()
        with Timer("svc", "op", store) as t:
            time.sleep(0.01)
        assert t.duration_ms >= 5.0

    def test_records_to_store(self):
        store = ProfileStore()
        with Timer("svc", "op", store):
            pass
        assert "op" in store.summary()

    def test_ok_true_on_success(self):
        store = ProfileStore()
        with Timer("svc", "op", store):
            pass
        assert store.summary()["op"].error_rate == 0.0

    def test_ok_false_on_exception(self):
        store = ProfileStore()
        try:
            with Timer("svc", "op", store):
                raise ValueError("boom")
        except ValueError:
            pass
        assert store.summary()["op"].error_rate == pytest.approx(1.0)

    def test_exception_propagates(self):
        with pytest.raises(RuntimeError, match="test error"):
            with Timer("svc", "op"):
                raise RuntimeError("test error")

    def test_works_without_store(self):
        with Timer("svc", "op") as t:
            pass
        assert t.duration_ms >= 0.0

    def test_duration_accessible_after_exit(self):
        with Timer("svc", "op") as t:
            time.sleep(0.005)
        assert t.duration_ms > 0.0

    def test_multiple_timers_same_store(self):
        store = ProfileStore()
        for _ in range(5):
            with Timer("svc", "op", store):
                pass
        assert store.summary()["op"].count == 5


# ---------------------------------------------------------------------------
# jittered_sleep
# ---------------------------------------------------------------------------


class TestJitteredSleep:
    @pytest.mark.asyncio
    async def test_returns_actual_duration(self):
        rng = random.Random(0)
        actual = await jittered_sleep(0.05, jitter_fraction=0.10, rng=rng)
        assert 0.0 < actual <= 0.1

    @pytest.mark.asyncio
    async def test_sleep_duration_is_near_base(self):
        rng = random.Random(1)
        start = time.perf_counter()
        await jittered_sleep(0.05, jitter_fraction=0.20, rng=rng)
        elapsed = time.perf_counter() - start
        # Should sleep between 0.04s and 0.06s
        assert 0.02 < elapsed < 0.12

    @pytest.mark.asyncio
    async def test_zero_jitter_sleeps_base(self):
        rng = random.Random(0)
        start = time.perf_counter()
        await jittered_sleep(0.02, jitter_fraction=0.0, rng=rng)
        elapsed = time.perf_counter() - start
        assert elapsed >= 0.015

    @pytest.mark.asyncio
    async def test_uses_module_rng_when_none_given(self):
        # Should not raise
        result = await jittered_sleep(0.01, jitter_fraction=0.10)
        assert result >= 0.0

    @pytest.mark.asyncio
    async def test_different_calls_produce_different_sleeps(self):
        rng = random.Random(7)
        results = set()
        for _ in range(10):
            r = await jittered_sleep(0.01, jitter_fraction=0.50, rng=rng)
            results.add(round(r, 4))
        # With 50% jitter across 10 samples, expect variety
        assert len(results) > 1

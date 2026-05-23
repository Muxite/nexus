"""
Tests for the async telemetry queue (Phase 4C.2).

Verifies:
- Round-trip: put(TradeWriteRow) → worker flushes → DB row exists.
- Backpressure: warning at 80% depth, drop-oldest at 100%.
- PRIORITY_TRADE never silently drops (QueueFull raised instead).
- Crash mid-batch: flush error is logged + counted, worker continues.
- Graceful shutdown: stop() drains the queue.
- Spill on timeout: stuck worker → rows land in JSONL.
- Metrics: counters reflect real activity.
"""

import asyncio
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from db import SeulgiDB  # noqa: E402
from telemetry import (  # noqa: E402
    AsyncTelemetryQueue,
    DecisionInputRow,
    EngineDecisionsRow,
    LLMCallRow,
    PRIORITY_DECISION,
    PRIORITY_SKIP,
    PRIORITY_TRADE,
    TradeWriteRow,
)


async def _wait_for_flush(q: AsyncTelemetryQueue, n: int, deadline_s: float = 5.0):
    """Spin until at least `n` batches have flushed (or timeout)."""
    t0 = asyncio.get_event_loop().time()
    while q.metrics["flush_batches"] < n:
        if asyncio.get_event_loop().time() - t0 > deadline_s:
            raise AssertionError(
                f"only {q.metrics['flush_batches']} batches flushed, expected ≥ {n}"
            )
        await asyncio.sleep(0.02)


@pytest.mark.asyncio
async def test_round_trip_trade_row_lands_in_db():
    db = SeulgiDB(":memory:")
    await db.connect()
    q = AsyncTelemetryQueue(db, batch_interval_ms=50, batch_size=10)
    q.start()
    try:
        sig_id = await db.insert_signal(
            site_url="https://t.test", headline="h", headline_id="h1",
        )
        await q.put(TradeWriteRow(kwargs=dict(
            signal_id=sig_id, condition_id="cond1", question="Q?",
            token_id=None, direction="YES", similarity=0.7, p_hat=0.6,
            mid_price=0.55, size_usdc=10.0, skip_reason=None,
        )))
        await _wait_for_flush(q, 1)
        rows = await db.recent_trades(limit=5)
        assert len(rows) == 1
        assert rows[0]["condition_id"] == "cond1"
    finally:
        await q.stop()
        await db.close()


@pytest.mark.asyncio
async def test_backpressure_drops_skip_first():
    db = SeulgiDB(":memory:")
    await db.connect()
    # Worker not started — manually inspect drop behaviour.
    q = AsyncTelemetryQueue(db, max_size=3, batch_size=10, batch_interval_ms=10)
    try:
        await q.put(EngineDecisionsRow(
            engine_name="e1", decisions=[], priority=PRIORITY_SKIP,
        ))
        await q.put(DecisionInputRow(kwargs={"headline_id": "h1"}))
        await q.put(LLMCallRow(kwargs={"prompt_sha": "abc"}))
        # Queue full; the next put must evict the priority-2 skip row.
        await q.put(TradeWriteRow(kwargs={"signal_id": 1}))
        m = q.metrics
        assert m["dropped_skip"] == 1
        assert m["dropped_decision"] == 0
        # Now fill again with no skip rows present; eviction must hit a -1 row.
        await q.put(LLMCallRow(kwargs={"prompt_sha": "xyz"}))
        assert q.metrics["dropped_decision"] == 1
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_priority_trade_never_drops():
    db = SeulgiDB(":memory:")
    await db.connect()
    q = AsyncTelemetryQueue(db, max_size=2, batch_size=10, batch_interval_ms=10)
    try:
        await q.put(TradeWriteRow(kwargs={"signal_id": 1}))
        await q.put(TradeWriteRow(kwargs={"signal_id": 2}))
        # Both slots are priority-0. A third TRADE put must raise, not drop.
        with pytest.raises(asyncio.QueueFull):
            await q.put(TradeWriteRow(kwargs={"signal_id": 3}))
        assert q.metrics["dropped_skip"] == 0
        assert q.metrics["dropped_decision"] == 0
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_backpressure_warning_at_80_pct(caplog):
    db = SeulgiDB(":memory:")
    await db.connect()
    q = AsyncTelemetryQueue(db, max_size=10, batch_size=100, batch_interval_ms=10)
    try:
        import logging
        caplog.set_level(logging.WARNING)
        # Push past 80% (9 rows ≥ 8) without exceeding max.
        for i in range(9):
            await q.put(LLMCallRow(kwargs={"prompt_sha": f"s{i}"}))
        assert any("> 80%" in rec.message for rec in caplog.records)
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_crash_mid_batch_continues():
    db = SeulgiDB(":memory:")
    await db.connect()
    q = AsyncTelemetryQueue(db, batch_size=5, batch_interval_ms=20)
    # Make the first flush fail by mocking db.batch to raise once.
    orig_batch = db.batch
    calls = {"n": 0}

    class _Boom:
        async def __aenter__(self):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("simulated mid-batch failure")
            self._cm = orig_batch()
            return await self._cm.__aenter__()
        async def __aexit__(self, et, ev, tb):
            return await self._cm.__aexit__(et, ev, tb)

    db.batch = lambda: _Boom()
    q.start()
    try:
        sig_id = await db.insert_signal(
            site_url="https://t.test", headline="h", headline_id="h2",
        )
        await q.put(TradeWriteRow(kwargs=dict(
            signal_id=sig_id, condition_id="cb", question="?",
            token_id=None, direction="YES", similarity=0.5, p_hat=0.5,
            mid_price=0.5, size_usdc=1.0, skip_reason=None,
        )))
        # Wait for the failed flush attempt to register.
        deadline = asyncio.get_event_loop().time() + 3.0
        while q.metrics["flush_errors"] == 0:
            if asyncio.get_event_loop().time() > deadline:
                raise AssertionError("expected a flush_errors bump")
            await asyncio.sleep(0.02)
        assert q.metrics["flush_errors"] >= 1
        # Worker must still be alive — next put + flush must succeed.
        await q.put(TradeWriteRow(kwargs=dict(
            signal_id=sig_id, condition_id="cc", question="?",
            token_id=None, direction="NO", similarity=0.6, p_hat=0.4,
            mid_price=0.5, size_usdc=1.0, skip_reason=None,
        )))
        deadline = asyncio.get_event_loop().time() + 3.0
        while q.metrics["flush_batches"] == 0:
            if asyncio.get_event_loop().time() > deadline:
                raise AssertionError("worker did not recover after crash")
            await asyncio.sleep(0.02)
    finally:
        await q.stop()
        await db.close()


@pytest.mark.asyncio
async def test_graceful_shutdown_drains_queue():
    db = SeulgiDB(":memory:")
    await db.connect()
    q = AsyncTelemetryQueue(db, batch_size=50, batch_interval_ms=2000)
    q.start()
    try:
        sig_id = await db.insert_signal(
            site_url="https://t.test", headline="h", headline_id="h3",
        )
        for i in range(5):
            await q.put(TradeWriteRow(kwargs=dict(
                signal_id=sig_id, condition_id=f"c{i}", question="?",
                token_id=None, direction="YES", similarity=0.5, p_hat=0.5,
                mid_price=0.5, size_usdc=1.0, skip_reason=None,
            )))
        # stop() must drain — even though batch_interval is 2s, the stop event
        # wakes the worker and one flush covers the queue.
        await q.stop()
        rows = await db.recent_trades(limit=10)
        assert len(rows) == 5
        assert q.metrics["depth"] == 0
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_spill_on_shutdown_timeout(tmp_path):
    db = SeulgiDB(":memory:")
    await db.connect()
    q = AsyncTelemetryQueue(
        db, batch_size=5, batch_interval_ms=20,
        lost_dir=str(tmp_path), flush_timeout_s=0.1,
    )
    # Stick the worker by replacing _flush with one that never returns.
    async def _stuck(batch):
        await asyncio.sleep(60)
    q._flush = _stuck
    q.start()
    try:
        await q.put(LLMCallRow(kwargs={"prompt_sha": "spill1"}))
        await q.put(LLMCallRow(kwargs={"prompt_sha": "spill2"}))
        # Let the worker pick up the first batch (and get stuck).
        await asyncio.sleep(0.1)
        # Now ensure there's still a row left in the deque before stop().
        await q.put(LLMCallRow(kwargs={"prompt_sha": "spill3"}))
        await q.stop()
        spill_files = list(Path(tmp_path).glob("telemetry_lost_*.jsonl"))
        assert spill_files, "expected a spill file after timeout"
        contents = spill_files[0].read_text().strip().splitlines()
        assert contents
        for line in contents:
            rec = json.loads(line)
            assert rec["type"] == "LLMCallRow"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_metrics_reflect_activity():
    db = SeulgiDB(":memory:")
    await db.connect()
    q = AsyncTelemetryQueue(db, batch_size=10, batch_interval_ms=20)
    q.start()
    try:
        sig_id = await db.insert_signal(
            site_url="https://t.test", headline="h", headline_id="h4",
        )
        await q.put(TradeWriteRow(kwargs=dict(
            signal_id=sig_id, condition_id="cm", question="?",
            token_id=None, direction="YES", similarity=0.5, p_hat=0.5,
            mid_price=0.5, size_usdc=1.0, skip_reason=None,
        )))
        await _wait_for_flush(q, 1)
        m = q.metrics
        assert m["flush_batches"] >= 1
        assert m["flush_errors"] == 0
        assert m["max"] == 10_000
        assert m["depth"] == 0
        assert set(m.keys()) == {
            "depth", "max", "dropped_skip", "dropped_decision",
            "flush_batches", "flush_errors",
        }
    finally:
        await q.stop()
        await db.close()

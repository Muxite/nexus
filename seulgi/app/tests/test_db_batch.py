"""
Tests for db.SeulgiDB.batch() — Phase B (2026-05-21) write coalescing.

The batch() context manager must:
- Group all inserts inside it into a single COMMIT (one fsync per batch).
- Serialise concurrent outer batches via an asyncio.Lock so two
  process() coroutines can't race on BEGIN/COMMIT and roll back each
  other's writes (the bug recorded in engine.py:706-711).
"""

import asyncio
import sys
import os

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from db import SeulgiDB


@pytest.mark.asyncio
async def test_batch_coalesces_writes_into_single_commit():
    """N inserts inside one batch must produce exactly 1 COMMIT.

    Post FIX-3B (2026-05-22, cycle-2 Layer C re-ship): the connection
    is opened with isolation_level=None, so `aiosqlite.commit()` is a
    no-op. batch() bracket now uses explicit `execute("BEGIN")` /
    `execute("COMMIT")` (shielded from cancellation). We instrument
    `_conn.execute` to count COMMIT statements instead of `.commit()`
    calls.
    """
    db = SeulgiDB(":memory:")
    await db.connect()
    try:
        commit_count = 0
        real_execute = db._conn.execute

        async def _counting_execute(sql, *args, **kwargs):
            nonlocal commit_count
            if isinstance(sql, str) and sql.strip().upper() == "COMMIT":
                commit_count += 1
            return await real_execute(sql, *args, **kwargs)

        db._conn.execute = _counting_execute

        async with db.batch():
            await db.insert_signal(
                site_url="https://a.test", headline="h1",
                headline_id="hid1",
            )
            await db.insert_signal(
                site_url="https://a.test", headline="h2",
                headline_id="hid2",
            )
            await db.insert_signal(
                site_url="https://a.test", headline="h3",
                headline_id="hid3",
            )

        assert commit_count == 1, (
            f"expected 1 COMMIT for 3 inserts inside batch(), got {commit_count}"
        )
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_concurrent_outer_batches_serialise():
    """Two concurrent batch() entries must serialise — second waits for
    first to commit before its own BEGIN. Without the lock both would
    race on BEGIN and aiosqlite's second BEGIN would raise, OR one's
    writes would leak into the other's transaction."""
    db = SeulgiDB(":memory:")
    await db.connect()
    try:
        sequence: list[str] = []

        async def _task(name: str) -> None:
            async with db.batch():
                sequence.append(f"{name}-in")
                # Force a yield so the other coroutine has a chance to
                # try entering its own outer batch.
                await asyncio.sleep(0.01)
                await db.insert_signal(
                    site_url="https://x.test", headline=f"h-{name}",
                    headline_id=f"hid-{name}",
                )
                sequence.append(f"{name}-out")

        await asyncio.gather(_task("A"), _task("B"))

        # With the lock, the two batches do not interleave — one
        # completes (in→out) before the other starts. Without the lock
        # we would see ["A-in", "B-in", ...] (or B-in, A-in).
        valid = [
            ["A-in", "A-out", "B-in", "B-out"],
            ["B-in", "B-out", "A-in", "A-out"],
        ]
        assert sequence in valid, (
            f"outer batches interleaved (race): {sequence}"
        )
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_concurrent_batch_and_individual_write_serialise():
    """Reentrant write-lock fix (2026-05-21): an individual write from
    one task must not interleave with another task's batch BEGIN. Before
    the fix, the individual write's implicit-txn window left a gap into
    which the batch BEGIN would crash with `cannot start a transaction
    within a transaction`. After the fix, individual writes acquire the
    same `_batch_lock` briefly and the BEGIN waits its turn."""
    db = SeulgiDB(":memory:")
    await db.connect()
    try:
        async def _batcher() -> None:
            # Open + hold a batch for a short period, exercising the
            # BEGIN/COMMIT bracket while the inserter races.
            async with db.batch():
                await db.insert_signal(
                    site_url="b", headline="batch_inner", headline_id="b1",
                )
                await asyncio.sleep(0.005)
                await db.insert_signal(
                    site_url="b", headline="batch_inner2", headline_id="b2",
                )

        async def _inserter(name: str) -> None:
            for i in range(5):
                await db.insert_signal(
                    site_url="i", headline=f"ind_{name}_{i}",
                    headline_id=f"i_{name}_{i}",
                )
                await asyncio.sleep(0.001)

        # Run one batch concurrently with two streams of individual writes.
        # Before the fix, this would raise OperationalError("cannot start a
        # transaction within a transaction") roughly every other run.
        await asyncio.gather(
            _batcher(), _inserter("A"), _inserter("B"), _batcher(),
        )

        # All writes must land. 2 batches × 2 + 2 inserters × 5 = 14 signals.
        snap = db.write_health_snapshot()
        assert snap["counters"]["insert_signal"]["err"] == 0, (
            f"insert_signal errors: {snap['counters']}"
        )
        assert snap["counters"]["insert_signal"]["ok"] == 14, (
            f"expected 14 ok inserts, got {snap['counters']}"
        )
        async with db._conn.execute("SELECT COUNT(*) FROM signals") as cur:
            n = (await cur.fetchone())[0]
        assert n == 14, f"expected 14 rows landed, got {n} (writes lost!)"
    finally:
        await db.close()

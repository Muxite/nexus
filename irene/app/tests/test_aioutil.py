"""aioutil tests — exercise every invariant in app/aioutil.py's spec block."""

from __future__ import annotations

import asyncio
import logging

import pytest

from app.aioutil import cancel_and_join


async def test_none_task_is_a_noop():
    await cancel_and_join(None)


async def test_cancels_and_joins_a_running_task():
    started = asyncio.Event()

    async def forever() -> None:
        started.set()
        await asyncio.sleep(3600)

    task = asyncio.create_task(forever())
    await started.wait()

    await cancel_and_join(task)

    assert task.cancelled()


async def test_swallows_our_own_cancellation():
    async def forever() -> None:
        await asyncio.sleep(3600)

    task = asyncio.create_task(forever())
    await asyncio.sleep(0)

    # No CancelledError escapes to the caller.
    await cancel_and_join(task)


async def test_reraises_when_the_caller_is_cancelled():
    """The bug this module exists to prevent: a caller cancelled while it
    awaits the child must not have that cancellation swallowed."""
    child_blocked = asyncio.Event()

    async def uncancellable() -> None:
        # Absorbs the first cancel and keeps running, so the outer
        # cancellation is what lands on `await task`.
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            child_blocked.set()
            await asyncio.sleep(3600)

    child = asyncio.create_task(uncancellable())
    await asyncio.sleep(0)

    caller_entered = asyncio.Event()

    async def caller() -> None:
        caller_entered.set()
        await cancel_and_join(child)

    outer = asyncio.create_task(caller())
    await caller_entered.wait()
    await child_blocked.wait()

    outer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await outer

    assert outer.cancelled(), "outer cancellation must not be swallowed"

    child.cancel()
    await asyncio.gather(child, return_exceptions=True)


async def test_logs_and_suppresses_a_real_task_failure(caplog):
    async def boom() -> None:
        raise RuntimeError("poller died")

    task = asyncio.create_task(boom())
    await asyncio.sleep(0)

    with caplog.at_level(logging.ERROR):
        await cancel_and_join(task)

    assert "poller died" in caplog.text
    assert "error while shutting down task" in caplog.text


async def test_joins_an_already_finished_task():
    async def quick() -> None:
        return None

    task = asyncio.create_task(quick())
    await task

    await cancel_and_join(task)

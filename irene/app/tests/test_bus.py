"""Bus tests — exercise every invariant in app/bus.py's spec block."""

from __future__ import annotations

import asyncio

import pytest

from app.bus import BusPayloadError, InProcBus
from app.topics import (
    TOPIC_ARTICLE_INGESTED,
    TOPIC_TELEMETRY_TIMING,
    is_telemetry,
)


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


async def test_publish_round_trips_to_subscriber():
    bus = InProcBus()
    received: list[dict] = []

    async def handler(payload: dict) -> None:
        received.append(payload)

    bus.subscribe(TOPIC_ARTICLE_INGESTED, handler)
    await bus.start()

    await bus.publish(TOPIC_ARTICLE_INGESTED, {"article_id": 1, "url": "https://x"})
    await asyncio.sleep(0.01)
    assert received == [{"article_id": 1, "url": "https://x"}]
    await bus.stop()


async def test_subscribe_after_start_raises():
    bus = InProcBus()
    await bus.start()
    with pytest.raises(RuntimeError, match="after bus.start"):
        bus.subscribe(TOPIC_ARTICLE_INGESTED, lambda p: asyncio.sleep(0))
    await bus.stop()


async def test_publish_before_start_raises():
    bus = InProcBus()
    with pytest.raises(RuntimeError, match="before bus.start"):
        await bus.publish(TOPIC_ARTICLE_INGESTED, {"x": 1})


async def test_publish_with_no_subscribers_is_noop():
    bus = InProcBus()
    await bus.start()
    await bus.publish(TOPIC_ARTICLE_INGESTED, {"x": 1})  # should not raise
    await bus.stop()


# ---------------------------------------------------------------------------
# JSON-roundtrip strict mode
# ---------------------------------------------------------------------------


async def test_strict_mode_rejects_non_json_payload():
    bus = InProcBus(strict=True)
    bus.subscribe(TOPIC_ARTICLE_INGESTED, lambda p: asyncio.sleep(0))
    await bus.start()

    class NotJsonable:
        pass

    with pytest.raises(BusPayloadError):
        await bus.publish(TOPIC_ARTICLE_INGESTED, {"obj": NotJsonable()})
    await bus.stop()


async def test_strict_mode_off_allows_through():
    bus = InProcBus(strict=False)
    received: list[dict] = []

    async def handler(payload: dict) -> None:
        received.append(payload)

    bus.subscribe(TOPIC_ARTICLE_INGESTED, handler)
    await bus.start()

    payload = {"obj": object()}
    await bus.publish(TOPIC_ARTICLE_INGESTED, payload)
    await asyncio.sleep(0.01)
    assert len(received) == 1
    await bus.stop()


# ---------------------------------------------------------------------------
# Back-pressure (trade-path blocks) + telemetry drop-first
# ---------------------------------------------------------------------------


async def test_trade_path_blocks_publisher_when_full():
    bus = InProcBus(maxsize=2)
    # Block the handler forever so the queue fills.
    block = asyncio.Event()

    async def slow_handler(payload: dict) -> None:
        await block.wait()

    bus.subscribe(TOPIC_ARTICLE_INGESTED, slow_handler)
    await bus.start()

    # First message is delivered into the handler's running coroutine (await
    # block.wait()); the next 2 fit in the queue (maxsize=2). The 4th
    # publish() must block.
    await bus.publish(TOPIC_ARTICLE_INGESTED, {"i": 1})
    await bus.publish(TOPIC_ARTICLE_INGESTED, {"i": 2})
    await bus.publish(TOPIC_ARTICLE_INGESTED, {"i": 3})

    publish_task = asyncio.create_task(bus.publish(TOPIC_ARTICLE_INGESTED, {"i": 4}))
    await asyncio.sleep(0.05)
    assert not publish_task.done(), "trade-path publish must block when queue is full"

    # Unblock and let everything drain.
    block.set()
    await publish_task
    await bus.stop()


async def test_telemetry_drops_when_full_increments_counter():
    bus = InProcBus(maxsize=1)
    block = asyncio.Event()

    async def slow_handler(payload: dict) -> None:
        await block.wait()

    bus.subscribe(TOPIC_TELEMETRY_TIMING, slow_handler)
    await bus.start()

    # The first publish is consumed and the handler blocks. Subsequent
    # publishes either fit the queue (maxsize=1, so 1 fits) or drop.
    for i in range(10):
        await bus.publish(TOPIC_TELEMETRY_TIMING, {"i": i})

    assert bus.counters.telemetry >= 1, "telemetry overflow must increment the drop counter"
    block.set()
    await bus.stop()


async def test_telemetry_never_blocks():
    bus = InProcBus(maxsize=1)
    block = asyncio.Event()

    async def slow_handler(payload: dict) -> None:
        await block.wait()

    bus.subscribe(TOPIC_TELEMETRY_TIMING, slow_handler)
    await bus.start()

    # If telemetry blocked, this would hang. We bound the wait.
    publish_task = asyncio.create_task(
        asyncio.wait_for(
            asyncio.gather(*[bus.publish(TOPIC_TELEMETRY_TIMING, {"i": i}) for i in range(20)]),
            timeout=0.5,
        )
    )
    await publish_task  # should complete well within the timeout
    block.set()
    await bus.stop()


# ---------------------------------------------------------------------------
# Subscriber isolation
# ---------------------------------------------------------------------------


async def test_subscriber_exception_does_not_kill_bus():
    bus = InProcBus()
    received: list[dict] = []

    async def boom(payload: dict) -> None:
        raise RuntimeError("subscriber broken")

    async def good(payload: dict) -> None:
        received.append(payload)

    bus.subscribe(TOPIC_ARTICLE_INGESTED, boom)
    bus.subscribe(TOPIC_ARTICLE_INGESTED, good)
    await bus.start()

    await bus.publish(TOPIC_ARTICLE_INGESTED, {"i": 1})
    await bus.publish(TOPIC_ARTICLE_INGESTED, {"i": 2})
    await asyncio.sleep(0.05)

    assert received == [{"i": 1}, {"i": 2}]  # the good subscriber still got both
    assert bus.counters.handler_exceptions == 2  # the bad one raised twice
    await bus.stop()


# ---------------------------------------------------------------------------
# topics helper
# ---------------------------------------------------------------------------


def test_is_telemetry_classification():
    assert is_telemetry(TOPIC_TELEMETRY_TIMING)
    assert not is_telemetry(TOPIC_ARTICLE_INGESTED)
    assert is_telemetry("telemetry.anything")
    assert not is_telemetry("article.ingested")

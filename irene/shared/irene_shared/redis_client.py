"""
Async Redis wrapper for Irene using redis.asyncio.

Provides publish/subscribe helpers for the irene:signals channel
and heartbeat support for service health tracking.
"""

from __future__ import annotations

import json
import logging
from typing import AsyncIterator, Optional

import redis.asyncio as aioredis
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class IreneRedis:
    """Thin wrapper around redis.asyncio with pub/sub helpers."""

    def __init__(
        self,
        url: str,
        signal_channel: str = "irene:signals",
        heartbeat_channel: str = "irene:heartbeat",
    ) -> None:
        self._url = url
        self.signal_channel = signal_channel
        self.heartbeat_channel = heartbeat_channel
        self._client: Optional[aioredis.Redis] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        self._client = aioredis.from_url(
            self._url,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        # Verify connectivity
        await self._client.ping()
        logger.info("IreneRedis connected to %s", self._url)

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _client_required(self) -> aioredis.Redis:
        if self._client is None:
            raise RuntimeError("IreneRedis.connect() has not been called")
        return self._client

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    async def publish_signal(self, model: BaseModel) -> int:
        """Publish a Pydantic model as JSON to the signal channel. Returns receiver count."""
        client = self._client_required()
        payload = model.model_dump_json()
        count = await client.publish(self.signal_channel, payload)
        logger.debug("Published to %s (%d receivers)", self.signal_channel, count)
        return count

    async def publish_heartbeat(self, service: str) -> None:
        client = self._client_required()
        payload = json.dumps({"service": service})
        await client.publish(self.heartbeat_channel, payload)

    # ------------------------------------------------------------------
    # Subscribing
    # ------------------------------------------------------------------

    async def subscribe_signals(self) -> AsyncIterator[str]:
        """
        Async generator yielding raw JSON strings from the signal channel.
        Runs until cancelled.

        Usage:
            async for raw_json in redis.subscribe_signals():
                signal = MappedSignal.model_validate_json(raw_json)
        """
        client = self._client_required()
        pubsub = client.pubsub()
        await pubsub.subscribe(self.signal_channel)
        logger.info("Subscribed to %s", self.signal_channel)
        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    yield message["data"]
        finally:
            await pubsub.unsubscribe(self.signal_channel)
            await pubsub.aclose()

    async def subscribe_heartbeats(self) -> AsyncIterator[dict]:
        """Async generator yielding heartbeat dicts."""
        client = self._client_required()
        pubsub = client.pubsub()
        await pubsub.subscribe(self.heartbeat_channel)
        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    yield json.loads(message["data"])
        finally:
            await pubsub.unsubscribe(self.heartbeat_channel)
            await pubsub.aclose()

    # ------------------------------------------------------------------
    # Simple key-value helpers (for last-seen timestamps etc.)
    # ------------------------------------------------------------------

    async def set_last_seen(self, service: str, timestamp: str) -> None:
        client = self._client_required()
        await client.set(f"irene:last_seen:{service}", timestamp, ex=300)

    async def get_last_seen(self, service: str) -> Optional[str]:
        client = self._client_required()
        return await client.get(f"irene:last_seen:{service}")

    async def set_profile(self, service: str, data: dict) -> None:
        """Publish a ProfileStore summary to Redis (TTL 5 min)."""
        client = self._client_required()
        await client.set(
            f"irene:profile:{service}", json.dumps(data), ex=300
        )

    async def get_profile(self, service: str) -> Optional[dict]:
        """Fetch the last published ProfileStore summary for a service."""
        client = self._client_required()
        raw = await client.get(f"irene:profile:{service}")
        return json.loads(raw) if raw else None

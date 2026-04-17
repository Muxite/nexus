"""
Pluggable daily (UTC) rate limiting with Redis and optional API-key rotation.

``DailyBudgetLimiterBase`` defines the interface. ``RedisDailyBudgetLimiter``
implements per-bucket counters. ``RotatingApiKeyPool`` picks the first key in a
round-robin order that still has budget for the period.
"""

from __future__ import annotations

import hashlib
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from irene_shared.redis_client import IreneRedis

logger = logging.getLogger(__name__)


def _seconds_until_utc_midnight() -> int:
    """Seconds from now until 00:00:00 UTC tomorrow (minimum 60)."""
    now = datetime.now(timezone.utc)
    tomorrow = (now + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    sec = int((tomorrow - now).total_seconds())
    return max(sec, 60)


def _key_fingerprint(secret: str) -> str:
    """
    Stable short id for Redis bucket names without storing raw secrets.

    :param secret: API key material.
    :returns: First 16 hex chars of SHA-256.
    """
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()[:16]


class DailyBudgetLimiterBase(ABC):
    """
    Abstract daily budget gate. Implementations track consumption per logical
    bucket name (caller supplies full bucket id, e.g. ``newsapi`` or
    ``newsapi:deadbeef...``).
    """

    @abstractmethod
    async def try_consume(
        self,
        redis: "IreneRedis",
        bucket: str,
        max_per_day: int,
        cost: int = 1,
    ) -> bool:
        """
        Reserve ``cost`` units for ``bucket`` for the current UTC day.

        :param redis: Connected Redis wrapper.
        :param bucket: Logical bucket (no date suffix; implementation adds it).
        :param max_per_day: Cap for the period; ``0`` means unlimited.
        :param cost: Units to consume.
        :returns: True if reserved, False if over cap or invalid.
        """


class RedisDailyBudgetLimiter(DailyBudgetLimiterBase):
    """
    UTC-day counters in Redis: ``irene:ratelimit:{bucket}:{YYYY-MM-DD}``.
    """

    async def try_consume(
        self,
        redis: "IreneRedis",
        bucket: str,
        max_per_day: int,
        cost: int = 1,
    ) -> bool:
        """
        Reserve ``cost`` units from a UTC-day counter for ``bucket``.

        :param redis: Connected ``IreneRedis`` instance.
        :param bucket: Sub-key after ``irene:ratelimit:`` (may include key fingerprint).
        :param max_per_day: Inclusive cap; ``0`` disables limiting.
        :param cost: HTTP calls or units to consume at once.
        :returns: Whether the reservation succeeded.
        """
        if max_per_day <= 0:
            return True
        if cost <= 0:
            return True
        if cost > max_per_day:
            logger.warning(
                "rate limit: cost %d > max_per_day %d for %s — deny",
                cost,
                max_per_day,
                bucket,
            )
            return False
        client = redis._client_required()
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        key = f"irene:ratelimit:{bucket}:{day}"
        ttl = _seconds_until_utc_midnight()
        n = await client.incrby(key, cost)
        if n == cost:
            await client.expire(key, ttl)
        if n > max_per_day:
            await client.incrby(key, -cost)
            logger.warning(
                "rate limit: %s daily budget exhausted (%d/%d) — skipping",
                bucket,
                n - cost,
                max_per_day,
            )
            return False
        return True


class RotatingApiKeyPool:
    """
    Selects an API key with available per-key daily quota using round-robin
    starting points so load spreads across keys over time.

    Each key uses its own Redis bucket: ``{service_prefix}:{fingerprint}``.
    """

    def __init__(
        self,
        service_prefix: str,
        limiter: DailyBudgetLimiterBase | None = None,
    ) -> None:
        self._prefix = service_prefix
        self._limiter = limiter or RedisDailyBudgetLimiter()
        self._rr = 0

    async def reserve_key(
        self,
        redis: "IreneRedis",
        keys: list[str],
        max_per_key_per_day: int,
        cost: int = 1,
    ) -> str | None:
        """
        Return the first key that can admit ``cost`` units, or None if all are over cap.

        When ``max_per_key_per_day`` is 0 (unlimited), rotates keys without Redis.

        :param redis: Redis client.
        :param keys: Non-empty list of secret strings.
        :param max_per_key_per_day: Per-key daily cap (same limit as single-key mode).
        :param cost: Units (usually 1 HTTP call).
        :returns: Selected key material, or None if no key is available.
        """
        if not keys:
            return None
        if max_per_key_per_day <= 0:
            k = keys[self._rr % len(keys)]
            self._rr = (self._rr + 1) % len(keys)
            return k
        n = len(keys)
        for offset in range(n):
            idx = (self._rr + offset) % n
            key = keys[idx]
            bucket = f"{self._prefix}:{_key_fingerprint(key)}"
            ok = await self._limiter.try_consume(
                redis, bucket, max_per_key_per_day, cost
            )
            if ok:
                self._rr = (idx + 1) % n
                return key
        logger.warning(
            "All %s API keys exhausted for today (%d keys)",
            self._prefix,
            len(keys),
        )
        return None

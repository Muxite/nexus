"""Tests for RedisDailyBudgetLimiter and RotatingApiKeyPool."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from irene_shared.ratelimit import RedisDailyBudgetLimiter, RotatingApiKeyPool
from irene_shared.redis_client import IreneRedis


@pytest.mark.asyncio
async def test_unlimited_when_max_zero():
    r = IreneRedis(url="redis://localhost:6379/0")
    r._client = MagicMock()
    assert await r.try_consume_daily_budget("x", 0, cost=5) is True
    r._client.incrby.assert_not_called()


@pytest.mark.asyncio
async def test_deny_when_cost_exceeds_max():
    r = IreneRedis(url="redis://localhost:6379/0")
    r._client = MagicMock()
    assert await r.try_consume_daily_budget("x", 10, cost=11) is False


@pytest.mark.asyncio
async def test_consume_rolls_back_when_over_budget():
    r = IreneRedis(url="redis://localhost:6379/0")
    client = MagicMock()
    client.incrby = AsyncMock(side_effect=[95, 85])
    r._client = client
    assert await r.try_consume_daily_budget("newsapi", 90, cost=10) is False
    assert client.incrby.await_count == 2
    second_call = client.incrby.await_args_list[1]
    assert second_call[0][1] == -10


@pytest.mark.asyncio
async def test_first_increment_sets_ttl():
    r = IreneRedis(url="redis://localhost:6379/0")
    client = MagicMock()
    client.incrby = AsyncMock(return_value=1)
    client.expire = AsyncMock()
    r._client = client
    assert await r.try_consume_daily_budget("brave", 100, cost=1) is True
    client.expire.assert_awaited_once()


@pytest.mark.asyncio
async def test_rotating_pool_skips_exhausted_key():
    limiter = MagicMock()
    limiter.try_consume = AsyncMock(side_effect=[False, True])
    pool = RotatingApiKeyPool("newsapi", limiter=limiter)
    redis = MagicMock()
    k = await pool.reserve_key(redis, ["a", "b"], max_per_key_per_day=10, cost=1)
    assert k == "b"
    assert limiter.try_consume.await_count == 2


@pytest.mark.asyncio
async def test_rotating_pool_unlimited_rotates():
    pool = RotatingApiKeyPool("newsapi")
    redis = MagicMock()
    a = await pool.reserve_key(redis, ["x", "y"], max_per_key_per_day=0, cost=1)
    b = await pool.reserve_key(redis, ["x", "y"], max_per_key_per_day=0, cost=1)
    c = await pool.reserve_key(redis, ["x", "y"], max_per_key_per_day=0, cost=1)
    assert a == "x" and b == "y" and c == "x"


@pytest.mark.asyncio
async def test_redis_limiter_direct():
    lim = RedisDailyBudgetLimiter()
    r = IreneRedis(url="redis://localhost:6379/0")
    client = MagicMock()
    client.incrby = AsyncMock(return_value=2)
    client.expire = AsyncMock()
    r._client = client
    assert await lim.try_consume(r, "clob", 100, cost=2) is True

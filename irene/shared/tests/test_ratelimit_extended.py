"""
Extended tests for RedisDailyBudgetLimiter and RotatingApiKeyPool.

Covers: all-keys-exhausted, cost > 1 multi-unit consume, round-robin
fairness across N keys, key fingerprint stability, cost=0 / max=0 edge cases.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, call

import pytest

from irene_shared.ratelimit import (
    RedisDailyBudgetLimiter,
    RotatingApiKeyPool,
    _key_fingerprint,
)
from irene_shared.redis_client import IreneRedis


# ---------------------------------------------------------------------------
# _key_fingerprint
# ---------------------------------------------------------------------------


class TestKeyFingerprint:
    def test_same_key_gives_same_fingerprint(self):
        assert _key_fingerprint("abc123") == _key_fingerprint("abc123")

    def test_different_keys_give_different_fingerprints(self):
        assert _key_fingerprint("key_a") != _key_fingerprint("key_b")

    def test_fingerprint_is_16_hex_chars(self):
        fp = _key_fingerprint("test")
        assert len(fp) == 16
        assert all(c in "0123456789abcdef" for c in fp)

    def test_empty_string_gives_stable_fingerprint(self):
        fp1 = _key_fingerprint("")
        fp2 = _key_fingerprint("")
        assert fp1 == fp2


# ---------------------------------------------------------------------------
# RedisDailyBudgetLimiter — multi-unit consume
# ---------------------------------------------------------------------------


def _make_redis(incrby_return: int) -> IreneRedis:
    r = IreneRedis(url="redis://localhost:6379/0")
    client = MagicMock()
    client.incrby = AsyncMock(return_value=incrby_return)
    client.expire = AsyncMock()
    r._client = client
    return r


class TestRedisDailyBudgetLimiterExtended:
    @pytest.mark.asyncio
    async def test_cost_greater_than_1_consumed_atomically(self):
        r = _make_redis(incrby_return=5)
        lim = RedisDailyBudgetLimiter()
        result = await lim.try_consume(r, "bucket", max_per_day=100, cost=5)
        assert result is True
        r._client.incrby.assert_awaited_once()
        # The increment call should use cost=5
        args = r._client.incrby.await_args[0]
        assert args[1] == 5

    @pytest.mark.asyncio
    async def test_cost_greater_than_max_denied_immediately(self):
        r = _make_redis(incrby_return=0)
        lim = RedisDailyBudgetLimiter()
        result = await lim.try_consume(r, "bucket", max_per_day=10, cost=11)
        assert result is False
        r._client.incrby.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_rollback_uses_negative_cost(self):
        """On over-budget, the rollback should decrement by the same cost."""
        r = IreneRedis(url="redis://localhost:6379/0")
        client = MagicMock()
        # First incrby: returns 105 (over 100), second: rollback
        client.incrby = AsyncMock(side_effect=[105, 95])
        client.expire = AsyncMock()
        r._client = client

        lim = RedisDailyBudgetLimiter()
        result = await lim.try_consume(r, "bucket", max_per_day=100, cost=5)
        assert result is False
        # Rollback call should be -5
        rollback_call = client.incrby.await_args_list[1]
        assert rollback_call[0][1] == -5

    @pytest.mark.asyncio
    async def test_max_zero_skips_redis(self):
        r = _make_redis(incrby_return=0)
        lim = RedisDailyBudgetLimiter()
        result = await lim.try_consume(r, "bucket", max_per_day=0, cost=9999)
        assert result is True
        r._client.incrby.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cost_zero_returns_true_without_redis(self):
        r = _make_redis(incrby_return=0)
        lim = RedisDailyBudgetLimiter()
        result = await lim.try_consume(r, "bucket", max_per_day=10, cost=0)
        assert result is True
        r._client.incrby.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_ttl_set_on_first_increment_only(self):
        """expire() should be called only when counter == cost (first call of the day)."""
        r = _make_redis(incrby_return=1)  # first increment
        lim = RedisDailyBudgetLimiter()
        await lim.try_consume(r, "bucket", max_per_day=100, cost=1)
        r._client.expire.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_ttl_not_set_on_subsequent_increments(self):
        """expire() should NOT be called when counter > cost (not first call)."""
        r = _make_redis(incrby_return=5)  # counter already at 5 (not == cost=1)
        lim = RedisDailyBudgetLimiter()
        await lim.try_consume(r, "bucket", max_per_day=100, cost=1)
        r._client.expire.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_exactly_at_budget_is_accepted(self):
        """Counter hitting exactly max_per_day should succeed."""
        r = _make_redis(incrby_return=100)
        lim = RedisDailyBudgetLimiter()
        result = await lim.try_consume(r, "bucket", max_per_day=100, cost=1)
        assert result is True


# ---------------------------------------------------------------------------
# RotatingApiKeyPool — all-exhausted, N-key round-robin
# ---------------------------------------------------------------------------


class TestRotatingApiKeyPoolExtended:
    @pytest.mark.asyncio
    async def test_all_keys_exhausted_returns_none(self):
        lim = MagicMock()
        lim.try_consume = AsyncMock(return_value=False)  # all fail
        pool = RotatingApiKeyPool("newsapi", limiter=lim)
        redis = MagicMock()
        result = await pool.reserve_key(redis, ["a", "b", "c"], max_per_key_per_day=10)
        assert result is None
        assert lim.try_consume.await_count == 3  # tried all 3

    @pytest.mark.asyncio
    async def test_empty_keys_returns_none(self):
        pool = RotatingApiKeyPool("newsapi")
        redis = MagicMock()
        result = await pool.reserve_key(redis, [], max_per_key_per_day=10)
        assert result is None

    @pytest.mark.asyncio
    async def test_round_robin_advances_across_calls(self):
        """Over 4 calls with 2 unlimited keys, usage should alternate: a,b,a,b."""
        pool = RotatingApiKeyPool("newsapi")
        redis = MagicMock()
        results = [
            await pool.reserve_key(redis, ["a", "b"], max_per_key_per_day=0)
            for _ in range(4)
        ]
        assert results == ["a", "b", "a", "b"]

    @pytest.mark.asyncio
    async def test_skips_first_exhausted_key_in_round_robin(self):
        """First key exhausted → should return second key and advance pointer."""
        lim = MagicMock()
        lim.try_consume = AsyncMock(side_effect=[False, True])
        pool = RotatingApiKeyPool("newsapi", limiter=lim)
        redis = MagicMock()

        result = await pool.reserve_key(redis, ["key_a", "key_b"], max_per_key_per_day=10)
        assert result == "key_b"

    @pytest.mark.asyncio
    async def test_rr_pointer_advances_past_successful_key(self):
        """After using key_b, next call should start from key_c (not key_a)."""
        call_count = 0

        async def _limiter(redis, bucket, max_per_day, cost):
            nonlocal call_count
            call_count += 1
            # First call (key_a) fails, second (key_b) succeeds
            return call_count % 2 == 0

        lim = MagicMock()
        lim.try_consume = AsyncMock(side_effect=_limiter)

        pool = RotatingApiKeyPool("newsapi", limiter=lim)
        redis = MagicMock()

        first = await pool.reserve_key(redis, ["key_a", "key_b", "key_c"], max_per_key_per_day=10)
        assert first == "key_b"
        # After selecting key_b (idx=1), _rr advances to 2 → next starts at key_c
        assert pool._rr == 2

    @pytest.mark.asyncio
    async def test_single_key_pool_always_returns_same_key_when_unlimited(self):
        pool = RotatingApiKeyPool("brave")
        redis = MagicMock()
        for _ in range(5):
            result = await pool.reserve_key(redis, ["only_key"], max_per_key_per_day=0)
            assert result == "only_key"

    @pytest.mark.asyncio
    async def test_fingerprint_used_in_bucket_name(self):
        """The Redis bucket for a key should contain its fingerprint, not the raw key."""
        lim = MagicMock()
        lim.try_consume = AsyncMock(return_value=True)

        pool = RotatingApiKeyPool("newsapi", limiter=lim)
        redis = MagicMock()
        api_key = "super_secret_key"

        await pool.reserve_key(redis, [api_key], max_per_key_per_day=10)

        called_bucket = lim.try_consume.await_args[0][1]
        expected_fp = _key_fingerprint(api_key)
        assert expected_fp in called_bucket
        assert api_key not in called_bucket  # raw key never stored in Redis key name

    @pytest.mark.asyncio
    async def test_n_keys_3_exhausted_tries_all_in_order(self):
        """With 3 keys all exhausted, try_consume called exactly 3 times."""
        lim = MagicMock()
        lim.try_consume = AsyncMock(return_value=False)
        pool = RotatingApiKeyPool("brave", limiter=lim)
        redis = MagicMock()

        result = await pool.reserve_key(
            redis, ["k1", "k2", "k3"], max_per_key_per_day=500, cost=1
        )
        assert result is None
        assert lim.try_consume.await_count == 3

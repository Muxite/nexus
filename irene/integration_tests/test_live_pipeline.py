"""
Live integration tests — require real PostgreSQL + Redis + Gamma API.

Run with:
    docker compose --profile integration run --rm integration-test

Or locally (with services up):
    IRENE_INTEGRATION_TESTS=true pytest integration_tests/ -v -m integration

These tests use unique IDs to avoid conflicts between runs and clean up after
themselves even if assertions fail.

What is tested:
  1. DB schema initialises idempotently
  2. Market upsert + retrieval round-trip with embeddings
  3. Signal insert + mapped_signal insert, event logging
  4. Rate limiter Redis counters persist and roll back correctly
  5. Full feeder→trader pipeline with real DB/Redis (mocked CLOB + Embedder)
  6. Gamma API returns live markets matching our filter criteria
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

pytestmark = pytest.mark.integration

_SKIP = os.getenv("IRENE_INTEGRATION_TESTS", "false").lower() != "true"
_DB_URL = os.getenv("IRENE_DATABASE_URL", "postgresql://irene:irene@postgres:5432/irene")
_REDIS_URL = os.getenv("IRENE_REDIS_URL", "redis://redis:6379/0")
_GAMMA_URL = os.getenv("IRENE_GAMMA_URL", "https://gamma-api.polymarket.com")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _uid(prefix: str = "test") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def _unit_vec() -> np.ndarray:
    v = np.ones(384, dtype=np.float32)
    return v / np.linalg.norm(v)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
async def db():
    """Module-scoped DB connection."""
    from irene_shared.db import IreneDB

    d = IreneDB(_DB_URL, pool_size=2)
    await d.connect()
    yield d
    await d.close()


@pytest.fixture(scope="module")
async def redis():
    """Module-scoped Redis connection."""
    from irene_shared.redis_client import IreneRedis

    r = IreneRedis(url=_REDIS_URL)
    await r.connect()
    yield r
    await r.close()


# ---------------------------------------------------------------------------
# 1. DB schema — idempotent init
# ---------------------------------------------------------------------------


@pytest.mark.skipif(_SKIP, reason="IRENE_INTEGRATION_TESTS not set")
@pytest.mark.asyncio
async def test_db_schema_idempotent(db):
    """Running connect() twice should not raise (migrations are IF NOT EXISTS)."""
    from irene_shared.db import IreneDB

    db2 = IreneDB(_DB_URL, pool_size=1)
    await db2.connect()
    await db2.close()


# ---------------------------------------------------------------------------
# 2. Market upsert + retrieval round-trip
# ---------------------------------------------------------------------------


@pytest.mark.skipif(_SKIP, reason="IRENE_INTEGRATION_TESTS not set")
@pytest.mark.asyncio
async def test_market_upsert_and_retrieval(db):
    condition_id = _uid("mkt")
    token_yes = _uid("tok_yes")
    token_no = _uid("tok_no")
    emb = _unit_vec()

    try:
        await db.upsert_market(
            condition_id=condition_id,
            slug=f"slug_{condition_id}",
            question="Will inflation drop below 3%?",
            description="CPI test market",
            category="economics",
            end_date=_now() + timedelta(days=30),
            volume_usdc=50000.0,
            token_yes=token_yes,
            token_no=token_no,
            embedding=emb,
        )

        markets = await db.get_active_markets_with_embeddings()
        found = [m for m in markets if m["condition_id"] == condition_id]
        assert len(found) == 1
        m = found[0]
        assert m["token_yes"] == token_yes
        assert m["token_no"] == token_no
        assert m["embedding"] is not None
        assert len(m["embedding"]) == 384

    finally:
        pool = db._pool
        if pool:
            async with pool.acquire() as conn:
                await conn.execute("DELETE FROM markets WHERE condition_id=$1", condition_id)


@pytest.mark.skipif(_SKIP, reason="IRENE_INTEGRATION_TESTS not set")
@pytest.mark.asyncio
async def test_market_upsert_is_idempotent(db):
    """Upserting the same market twice should not raise or duplicate."""
    condition_id = _uid("idem")
    emb = _unit_vec()

    try:
        for _ in range(2):
            await db.upsert_market(
                condition_id=condition_id,
                slug=None,
                question="Idempotency test market?",
                description=None,
                category="test",
                end_date=_now() + timedelta(days=7),
                volume_usdc=1000.0,
                token_yes=_uid("ty"),
                token_no=_uid("tn"),
                embedding=emb,
            )

        markets = await db.get_active_markets_with_embeddings()
        found = [m for m in markets if m["condition_id"] == condition_id]
        assert len(found) == 1  # not duplicated

    finally:
        pool = db._pool
        if pool:
            async with pool.acquire() as conn:
                await conn.execute("DELETE FROM markets WHERE condition_id=$1", condition_id)


# ---------------------------------------------------------------------------
# 3. Signal pipeline: insert signal → mapped_signal → market_events
# ---------------------------------------------------------------------------


@pytest.mark.skipif(_SKIP, reason="IRENE_INTEGRATION_TESTS not set")
@pytest.mark.asyncio
async def test_signal_and_event_round_trip(db):
    condition_id = _uid("sig")
    token_yes = _uid("ty")
    token_no = _uid("tn")
    signal_url = f"https://integration-test.example.com/{uuid.uuid4().hex}"

    try:
        # Seed a market
        await db.upsert_market(
            condition_id=condition_id,
            slug=None,
            question="Integration test: will signal round-trip work?",
            description=None,
            category="test",
            end_date=_now() + timedelta(days=5),
            volume_usdc=5000.0,
            token_yes=token_yes,
            token_no=token_no,
            embedding=_unit_vec(),
        )

        # Insert a signal
        signal_id = await db.insert_signal(
            source="newsapi",
            source_detail="newsapi",
            url=signal_url,
            title="Integration test headline: Fed raises rates",
            description="Test description",
            published_at=_now() - timedelta(minutes=5),
        )
        assert signal_id is not None
        assert isinstance(signal_id, int)

        # Insert a mapped signal
        mapped_id = await db.insert_mapped_signal(
            signal_id=signal_id,
            condition_id=condition_id,
            token_yes=token_yes,
            token_no=token_no,
            question="Integration test: will signal round-trip work?",
            similarity_score=0.85,
            direction="YES",
            direction_confidence=0.75,
        )
        assert mapped_id is not None

        # Log a signal_match event
        from irene_shared.models import MarketEventType
        await db.log_event(
            condition_id=condition_id,
            event_type=MarketEventType.SIGNAL_MATCH,
            payload={"signal_id": signal_id, "score": 0.85},
        )

        # Retrieve and verify events
        events = await db.get_market_events(condition_id, limit=50)
        event_types = [e["event_type"] for e in events]
        assert MarketEventType.SIGNAL_MATCH in event_types

    finally:
        pool = db._pool
        if pool:
            async with pool.acquire() as conn:
                await conn.execute("DELETE FROM market_events WHERE condition_id=$1", condition_id)
                await conn.execute("DELETE FROM mapped_signals WHERE condition_id=$1", condition_id)
                await conn.execute("DELETE FROM markets WHERE condition_id=$1", condition_id)
                await conn.execute("DELETE FROM signals WHERE url=$1", signal_url)


# ---------------------------------------------------------------------------
# 4. Redis rate limiter — counters persist and roll back
# ---------------------------------------------------------------------------


@pytest.mark.skipif(_SKIP, reason="IRENE_INTEGRATION_TESTS not set")
@pytest.mark.asyncio
async def test_redis_rate_limiter_increments(redis):
    """Counter should increment across calls within the same day."""
    bucket = _uid("ratelimit_test")

    # Consume 3 times
    for _ in range(3):
        ok = await redis.try_consume_daily_budget(bucket, max_per_day=100, cost=1)
        assert ok is True

    # Should still have budget
    ok = await redis.try_consume_daily_budget(bucket, max_per_day=100, cost=1)
    assert ok is True

    # Clean up
    day = _now().strftime("%Y-%m-%d")
    key = f"irene:ratelimit:{bucket}:{day}"
    await redis._client.delete(key)


@pytest.mark.skipif(_SKIP, reason="IRENE_INTEGRATION_TESTS not set")
@pytest.mark.asyncio
async def test_redis_rate_limiter_blocks_when_exhausted(redis):
    """After exhausting the budget, further calls should return False."""
    bucket = _uid("exhaust_test")
    day = _now().strftime("%Y-%m-%d")
    key = f"irene:ratelimit:{bucket}:{day}"

    try:
        # Consume exactly at the limit
        for i in range(5):
            ok = await redis.try_consume_daily_budget(bucket, max_per_day=5, cost=1)
            assert ok is True, f"Call {i+1} should succeed"

        # Next call should fail
        ok = await redis.try_consume_daily_budget(bucket, max_per_day=5, cost=1)
        assert ok is False

    finally:
        await redis._client.delete(key)


@pytest.mark.skipif(_SKIP, reason="IRENE_INTEGRATION_TESTS not set")
@pytest.mark.asyncio
async def test_redis_rate_limiter_rollback_on_over_budget(redis):
    """When a call would exceed the limit, counter should be rolled back."""
    bucket = _uid("rollback_test")
    day = _now().strftime("%Y-%m-%d")
    key = f"irene:ratelimit:{bucket}:{day}"

    try:
        # Use up 4 of 5 budget
        for _ in range(4):
            await redis.try_consume_daily_budget(bucket, max_per_day=5, cost=1)

        # This should fail (would take us to 5+1=6 > 5... wait, 5 is at limit)
        # Let's try cost=2 which would take us to 6 > 5
        ok = await redis.try_consume_daily_budget(bucket, max_per_day=5, cost=2)
        assert ok is False

        # Counter should still be at 4 (rolled back from 6 to 4)
        count = int(await redis._client.get(key) or 0)
        assert count == 4

    finally:
        await redis._client.delete(key)


# ---------------------------------------------------------------------------
# 5. Full feeder→trader pipeline (real DB/Redis, mocked CLOB + Embedder)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(_SKIP, reason="IRENE_INTEGRATION_TESTS not set")
@pytest.mark.asyncio
async def test_full_pipeline_signal_to_trade_decided(db, redis):
    """
    Seed a market with a known embedding. Feed a signal whose embedding
    matches it. Run the trader handler. Verify both signal_match and
    trade_decided events are recorded.
    """
    from irene_shared.models import Direction, MarketEventType, MappedSignal, Orderbook, RawSignal, SignalSource

    condition_id = _uid("pipeline")
    token_yes = _uid("ty")
    token_no = _uid("tn")
    signal_url = f"https://pipeline-test.example.com/{uuid.uuid4().hex}"

    emb = _unit_vec()

    cfg = MagicMock()
    cfg.signals.similarity_threshold = 0.01
    cfg.signals.top_k_markets = 5
    cfg.signals.direction_min_confidence = 0.0  # accept all directions
    cfg.signals.yes_keywords = []
    cfg.signals.no_keywords = []
    cfg.trading.balance_usdc = 1000.0
    cfg.trading.max_open_positions = 10
    cfg.trading.min_edge = 0.01
    cfg.trading.fee_buffer = 0.01
    cfg.trading.kelly_fraction = 0.25
    cfg.trading.max_position_fraction = 0.20
    cfg.trading.max_liquidity_fraction = 0.50
    cfg.trading.edge_increment_yes = 0.12
    cfg.trading.edge_increment_no = 0.12
    cfg.trading.order_type = "FOK"
    cfg.trading.position_timeout_hours = 48.0
    cfg.polymarket.clob_url = "https://clob.polymarket.com"
    cfg.irene.mode = "paper"
    cfg.rate_limits.clob_max_per_day = 20000
    cfg.timing.jitter_fraction = 0.0

    cfg_manager = MagicMock()
    cfg_manager.get.return_value = cfg

    try:
        # 1. Seed market
        await db.upsert_market(
            condition_id=condition_id,
            slug=None,
            question="Will the Fed raise rates above 5.5% this year?",
            description="Federal Reserve rate policy",
            category="economics",
            end_date=_now() + timedelta(days=60),
            volume_usdc=25000.0,
            token_yes=token_yes,
            token_no=token_no,
            embedding=emb,
        )

        # 2. Run feeder pipeline (mocked embedder → known vector)
        with patch("mapper.SignalMapper._db", db), \
             patch("main.Embedder") as MockEmb:
            MockEmb.return_value.embed.return_value = emb

            from main import Feeder
            feeder = Feeder(cfg_manager, db, redis)
            feeder._embedder = MagicMock()
            feeder._embedder.embed.return_value = emb

            raw = RawSignal(
                source=SignalSource.NEWSAPI,
                source_detail="newsapi",
                url=signal_url,
                title="Federal Reserve signals rate hike above 5.5%",
                description="The Fed chair hinted at raising rates further",
                published_at=_now() - timedelta(minutes=2),
                ingested_at=_now(),
            )
            await feeder._process_signals([raw])

        events_after_feeder = await db.get_market_events(condition_id, limit=50)
        types_after_feeder = [e["event_type"] for e in events_after_feeder]
        assert MarketEventType.SIGNAL_MATCH in types_after_feeder, (
            f"No SIGNAL_MATCH event. Got: {types_after_feeder}"
        )

        # 3. Run trader handler (mocked CLOB, real DB)
        ob = Orderbook(
            token_id=token_yes,
            best_bid=0.48,
            best_ask=0.52,
            mid_price=0.50,
            bid_depth_usdc=1000.0,
            ask_depth_usdc=1000.0,
        )
        clob_mock = MagicMock()
        clob_mock.get_orderbook = AsyncMock(return_value=ob)
        clob_cm = MagicMock()
        clob_cm.__aenter__ = AsyncMock(return_value=clob_mock)
        clob_cm.__aexit__ = AsyncMock(return_value=False)
        ClobClass = MagicMock(return_value=clob_cm)

        ms = MappedSignal(
            signal_id=1,
            condition_id=condition_id,
            token_yes=token_yes,
            token_no=token_no,
            question="Will the Fed raise rates above 5.5% this year?",
            similarity_score=0.95,
            direction=Direction.YES,
            direction_confidence=0.80,
            signal_title="Federal Reserve signals rate hike above 5.5%",
            signal_source="newsapi",
            mapped_at=_now(),
        )

        with patch("main.ClobClient", ClobClass):
            from main import Trader
            trader = Trader(cfg_manager, db, redis)
            await trader._handle_signal(ms)

        events_final = await db.get_market_events(condition_id, limit=50)
        types_final = [e["event_type"] for e in events_final]
        assert MarketEventType.TRADE_DECIDED in types_final, (
            f"No TRADE_DECIDED event. Got: {types_final}"
        )

    finally:
        pool = db._pool
        if pool:
            async with pool.acquire() as conn:
                await conn.execute("DELETE FROM trades WHERE condition_id=$1", condition_id)
                await conn.execute("DELETE FROM positions WHERE condition_id=$1", condition_id)
                await conn.execute("DELETE FROM market_events WHERE condition_id=$1", condition_id)
                await conn.execute("DELETE FROM mapped_signals WHERE condition_id=$1", condition_id)
                await conn.execute("DELETE FROM markets WHERE condition_id=$1", condition_id)
                await conn.execute("DELETE FROM signals WHERE url=$1", signal_url)


# ---------------------------------------------------------------------------
# 6. Gamma API live markets
# ---------------------------------------------------------------------------


@pytest.mark.skipif(_SKIP, reason="IRENE_INTEGRATION_TESTS not set")
@pytest.mark.asyncio
async def test_gamma_api_returns_open_markets():
    """Fetch the first page from the live Gamma API and verify basic structure."""
    import aiohttp

    async with aiohttp.ClientSession() as session:
        url = f"{_GAMMA_URL}/markets"
        params = {
            "closed": "false",
            "limit": 10,
            "offset": 0,
        }
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            assert resp.status == 200, f"Gamma API returned {resp.status}"
            data = await resp.json()

    assert isinstance(data, list), "Expected list of markets"
    assert len(data) > 0, "Expected at least one market"

    market = data[0]
    # Verify required fields present
    assert "conditionId" in market or "condition_id" in market
    assert "question" in market


@pytest.mark.skipif(_SKIP, reason="IRENE_INTEGRATION_TESTS not set")
@pytest.mark.asyncio
async def test_gamma_api_markets_have_volume_and_end_date():
    """Markets from Gamma should have volume and end date for our filter."""
    import aiohttp

    async with aiohttp.ClientSession() as session:
        url = f"{_GAMMA_URL}/markets"
        params = {"closed": "false", "limit": 20, "offset": 0}
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            data = await resp.json()

    # At least some markets should have volume > 0 and an endDate
    markets_with_volume = [m for m in data if float(m.get("volume", 0) or 0) > 0]
    assert len(markets_with_volume) > 0, "Expected markets with volume > 0"

    markets_with_end_date = [m for m in data if m.get("endDate") or m.get("end_date")]
    assert len(markets_with_end_date) > 0, "Expected markets with endDate"


# ---------------------------------------------------------------------------
# 7. DB get_dashboard_stats smoke test
# ---------------------------------------------------------------------------


@pytest.mark.skipif(_SKIP, reason="IRENE_INTEGRATION_TESTS not set")
@pytest.mark.asyncio
async def test_dashboard_stats_returns_all_keys(db):
    """get_dashboard_stats() should return a dict with the expected keys."""
    stats = await db.get_dashboard_stats()
    required_keys = {
        "active_markets",
        "total_signals",
        "total_mapped_signals",
        "total_trades",
        "open_positions",
        "realized_pnl",
        "unrealized_pnl",
    }
    missing = required_keys - set(stats.keys())
    assert not missing, f"Missing dashboard stats keys: {missing}"

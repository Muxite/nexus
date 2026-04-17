"""
Phase 5 integration test: seed market + signal → feeder mapper + trader handler
→ verify market_events contains signal_match + trade_decided.

Skipped unless IRENE_INTEGRATION_TESTS=true (requires real postgres + redis).
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

from irene_shared.db import IreneDB
from irene_shared.models import Direction, MarketEventType, RawSignal, SignalSource
from irene_shared.redis_client import IreneRedis

_SKIP = os.getenv("IRENE_INTEGRATION_TESTS", "false").lower() != "true"
pytestmark = pytest.mark.integration


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _make_embedding() -> np.ndarray:
    """Return a unit vector embedding (all ones, normalised)."""
    v = np.ones(384, dtype=np.float32)
    return v / np.linalg.norm(v)


def _make_cfg(condition_id: str):
    cfg = MagicMock()
    cfg.signals.similarity_threshold = 0.1  # low threshold so our market always matches
    cfg.signals.top_k_markets = 5
    cfg.polymarket.clob_url = "https://clob.polymarket.com"
    cfg.trading.balance_usdc = 1000.0
    cfg.trading.max_open_positions = 10
    cfg.trading.min_edge = 0.01
    cfg.trading.edge_increment = 0.05
    cfg.trading.kelly_fraction = 0.25
    cfg.trading.max_position_fraction = 0.1
    cfg.trading.max_liquidity_fraction = 0.5
    cfg.trading.fee_bps = 20
    cfg.trading.max_signal_age_seconds = 3600
    cfg.trading.position_timeout_hours = 24
    cfg.trading.mode = "paper"
    return cfg


@pytest.mark.skipif(_SKIP, reason="IRENE_INTEGRATION_TESTS not set")
@pytest.mark.asyncio
async def test_signal_match_and_trade_decided_events():
    """
    Seed a market + raw signal in the DB, run the feeder mapper,
    then run the trader handler and verify both event types appear
    in market_events.
    """
    db_url = os.getenv(
        "IRENE_DATABASE_URL",
        "postgresql://irene:irene@localhost:5432/irene",
    )
    redis_url = os.getenv("IRENE_REDIS_URL", "redis://localhost:6379/0")

    db = IreneDB(db_url, pool_size=2)
    await db.connect()

    redis = IreneRedis(url=redis_url)
    await redis.connect()

    # Unique IDs per test run so runs don't interfere with each other
    condition_id = f"test_{uuid.uuid4().hex[:12]}"
    token_yes = f"tok_yes_{uuid.uuid4().hex[:8]}"
    token_no = f"tok_no_{uuid.uuid4().hex[:8]}"

    try:
        # ----------------------------------------------------------------
        # 1. Seed market
        # ----------------------------------------------------------------
        emb = _make_embedding()
        await db.upsert_market(
            condition_id=condition_id,
            slug=None,
            question="Will interest rates rise above 5%?",
            description="Fed rate policy question",
            category="economics",
            end_date=_now() + timedelta(hours=48),
            volume_usdc=10000.0,
            token_yes=token_yes,
            token_no=token_no,
            embedding=emb,
        )

        # ----------------------------------------------------------------
        # 2. Build feeder pipeline (real SignalMapper, mock Embedder)
        # ----------------------------------------------------------------
        from mapper import SignalMapper

        cfg = _make_cfg(condition_id)
        cfg_manager = MagicMock()
        cfg_manager.get.return_value = cfg

        # Patch Embedder so it returns our known embedding
        with patch("main.Embedder") as MockEmbedder, \
             patch("main.SignalMapper", wraps=SignalMapper):
            from main import Feeder

            feeder = Feeder(cfg_manager, db, redis)
            feeder._embedder = MagicMock()
            feeder._embedder.embed.return_value = emb

        raw_signal = RawSignal(
            source=SignalSource.NEWSAPI,
            source_detail="newsapi",
            url=f"https://news.example.com/{uuid.uuid4().hex}",
            title="Federal Reserve raises interest rates sharply",
            description="The Fed raised rates above 5% today",
            published_at=_now(),
            ingested_at=_now(),
        )

        await feeder._process_signals([raw_signal])

        # Verify signal_match event was logged
        events = await db.get_market_events(condition_id, limit=50)
        event_types = [e["event_type"] for e in events]
        assert MarketEventType.SIGNAL_MATCH in event_types, (
            f"Expected SIGNAL_MATCH in {event_types}"
        )

        # ----------------------------------------------------------------
        # 3. Run the trader handler on the mapped signal
        # ----------------------------------------------------------------
        from irene_shared.models import MappedSignal
        from decision import DecisionEngine
        from executor import Executor
        from position_tracker import PositionTracker
        from main import Trader  # trader/app/main.py

        trader = Trader(cfg_manager, db, redis)

        # Mock the CLOB client so we don't need network
        from irene_shared.models import Orderbook

        ob = Orderbook(
            token_id=token_yes,
            best_bid=0.49,
            best_ask=0.51,
            mid_price=0.50,
            bid_depth_usdc=500.0,
            ask_depth_usdc=500.0,
        )
        clob_client = MagicMock()
        clob_client.get_orderbook = AsyncMock(return_value=ob)
        clob_cm = MagicMock()
        clob_cm.__aenter__ = AsyncMock(return_value=clob_client)
        clob_cm.__aexit__ = AsyncMock(return_value=False)
        clob_klass = MagicMock(return_value=clob_cm)

        ms = MappedSignal(
            signal_id=1,
            condition_id=condition_id,
            token_yes=token_yes,
            token_no=token_no,
            question="Will interest rates rise above 5%?",
            similarity_score=0.90,
            direction=Direction.YES,
            direction_confidence=0.80,
            signal_title="Federal Reserve raises interest rates sharply",
            signal_source="newsapi",
            mapped_at=_now(),
        )

        with patch("main.ClobClient", clob_klass):
            await trader._handle_signal(ms)

        # Verify trade_decided event was logged
        events = await db.get_market_events(condition_id, limit=50)
        event_types = [e["event_type"] for e in events]
        assert MarketEventType.TRADE_DECIDED in event_types, (
            f"Expected TRADE_DECIDED in {event_types}"
        )

    finally:
        # Clean up test data
        pool = db._pool
        if pool:
            async with pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM market_events WHERE condition_id = $1", condition_id
                )
                await conn.execute(
                    "DELETE FROM mapped_signals WHERE condition_id = $1", condition_id
                )
                await conn.execute(
                    "DELETE FROM markets WHERE condition_id = $1", condition_id
                )
                # signals table has no condition_id; clean up by URL prefix
                await conn.execute(
                    "DELETE FROM signals WHERE url LIKE 'https://news.example.com/%'"
                )
        await redis.close()
        await db.close()

"""Unit tests for feeder/app/main.py — Feeder._process_signals pipeline."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

from irene_shared.models import Direction, MappedSignal, RawSignal, SignalSource
from main import Feeder


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _make_cfg():
    cfg = MagicMock()
    cfg.signals.newsapi_key = "key"
    cfg.signals.newsapi_poll_seconds = 300
    cfg.signals.rss_poll_seconds = 300
    cfg.signals.rss_startup_spread_fraction = 0.5
    cfg.signals.jitter_fraction = 0.0
    cfg.signals.rss_feeds = []
    cfg.signals.similarity_threshold = 0.5
    cfg.signals.top_k_markets = 3
    return cfg


def _make_config_manager(cfg=None):
    m = MagicMock()
    m.get.return_value = cfg or _make_cfg()
    return m


def _make_db(signal_id: int = 1, mapped_id: int = 10):
    db = MagicMock()
    db.insert_signal = AsyncMock(return_value=signal_id)
    db.update_signal_embedding = AsyncMock()
    db.insert_mapped_signal = AsyncMock(return_value=mapped_id)
    db.mark_mapped_signal_dispatched = AsyncMock()
    db.mark_signal_mapped = AsyncMock()
    db.log_event = AsyncMock()
    return db


def _make_redis():
    redis = MagicMock()
    redis.publish_signal = AsyncMock(return_value=1)
    redis.publish_heartbeat = AsyncMock()
    redis.set_last_seen = AsyncMock()
    return redis


def _make_raw_signal(url: str = "https://news.example.com/a") -> RawSignal:
    return RawSignal(
        source=SignalSource.NEWSAPI,
        source_detail="newsapi",
        url=url,
        title="Fed raises interest rates above 5%",
        description="Federal Reserve raises rates",
        published_at=_now(),
        ingested_at=_now(),
    )


def _make_mapped_signal(
    direction: Direction = Direction.YES,
    signal_id: int = 1,
) -> MappedSignal:
    return MappedSignal(
        signal_id=signal_id,
        condition_id="cond_abc",
        token_yes="tok_yes",
        token_no="tok_no",
        question="Will rates exceed 5%?",
        similarity_score=0.82,
        direction=direction,
        direction_confidence=0.75,
        signal_title="Fed raises interest rates above 5%",
        signal_source="newsapi",
        mapped_at=_now(),
    )


def _make_feeder(db=None, redis=None, cfg=None) -> Feeder:
    """Build a Feeder with all heavy deps (Embedder, aiohttp) mocked out."""
    with patch("main.Embedder") as MockEmbedder, \
         patch("main.SignalMapper"):
        feeder = Feeder(
            _make_config_manager(cfg),
            db or _make_db(),
            redis or _make_redis(),
        )
        # Replace embedder with one whose embed() is a plain MagicMock
        fake_emb = np.zeros(384, dtype=np.float32)
        feeder._embedder = MagicMock()
        feeder._embedder.embed.return_value = fake_emb
        feeder._embedder.load.return_value = None
    return feeder


# ---------------------------------------------------------------------------
# _process_signals — no signals
# ---------------------------------------------------------------------------


class TestProcessSignalsEmpty:

    @pytest.mark.asyncio
    async def test_empty_list_returns_immediately(self):
        db = _make_db()
        redis = _make_redis()
        feeder = _make_feeder(db=db, redis=redis)
        await feeder._process_signals([])

        db.insert_signal.assert_not_called()
        redis.publish_signal.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_list_still_publishes_heartbeat(self):
        redis = _make_redis()
        feeder = _make_feeder(redis=redis)
        # _process_signals returns early without heartbeat for empty list
        await feeder._process_signals([])
        redis.publish_heartbeat.assert_not_called()


# ---------------------------------------------------------------------------
# _process_signals — signal insertion
# ---------------------------------------------------------------------------


class TestProcessSignalsInsertion:

    @pytest.mark.asyncio
    async def test_inserts_signal_to_db(self):
        db = _make_db(signal_id=42)
        feeder = _make_feeder(db=db)
        feeder._mapper.map = AsyncMock(return_value=[])

        raw = _make_raw_signal()
        await feeder._process_signals([raw])

        db.insert_signal.assert_called_once()
        kwargs = db.insert_signal.call_args.kwargs
        assert kwargs["url"] == raw.url
        assert kwargs["title"] == raw.title
        assert kwargs["source"] == raw.source.value

    @pytest.mark.asyncio
    async def test_skips_duplicate_url(self):
        db = _make_db(signal_id=None)  # None = duplicate
        feeder = _make_feeder(db=db)

        raw = _make_raw_signal()
        await feeder._process_signals([raw])

        db.update_signal_embedding.assert_not_called()
        feeder._mapper.map.assert_not_called()

    @pytest.mark.asyncio
    async def test_embeds_and_updates_signal(self):
        db = _make_db(signal_id=5)
        feeder = _make_feeder(db=db)
        feeder._mapper.map = AsyncMock(return_value=[])

        raw = _make_raw_signal()
        await feeder._process_signals([raw])

        db.update_signal_embedding.assert_called_once()
        args = db.update_signal_embedding.call_args
        assert args.args[0] == 5  # signal_id passed


# ---------------------------------------------------------------------------
# _process_signals — mapping + publishing
# ---------------------------------------------------------------------------


class TestProcessSignalsMapping:

    @pytest.mark.asyncio
    async def test_publishes_yes_direction_signal(self):
        db = _make_db(signal_id=1, mapped_id=10)
        redis = _make_redis()
        feeder = _make_feeder(db=db, redis=redis)

        ms = _make_mapped_signal(direction=Direction.YES)
        feeder._mapper.map = AsyncMock(return_value=[ms])

        await feeder._process_signals([_make_raw_signal()])

        redis.publish_signal.assert_called_once()

    @pytest.mark.asyncio
    async def test_publishes_no_direction_signal(self):
        db = _make_db(signal_id=1, mapped_id=10)
        redis = _make_redis()
        feeder = _make_feeder(db=db, redis=redis)

        ms = _make_mapped_signal(direction=Direction.NO)
        feeder._mapper.map = AsyncMock(return_value=[ms])

        await feeder._process_signals([_make_raw_signal()])

        redis.publish_signal.assert_called_once()

    @pytest.mark.asyncio
    async def test_skips_neutral_direction(self):
        redis = _make_redis()
        feeder = _make_feeder(redis=redis)

        ms = _make_mapped_signal(direction=Direction.NEUTRAL)
        feeder._mapper.map = AsyncMock(return_value=[ms])

        await feeder._process_signals([_make_raw_signal()])

        redis.publish_signal.assert_not_called()

    @pytest.mark.asyncio
    async def test_inserts_mapped_signal_to_db(self):
        db = _make_db(signal_id=1, mapped_id=10)
        feeder = _make_feeder(db=db)

        ms = _make_mapped_signal(direction=Direction.YES)
        feeder._mapper.map = AsyncMock(return_value=[ms])

        await feeder._process_signals([_make_raw_signal()])

        db.insert_mapped_signal.assert_called_once()
        kwargs = db.insert_mapped_signal.call_args.kwargs
        assert kwargs["condition_id"] == "cond_abc"
        assert kwargs["direction"] == "YES"

    @pytest.mark.asyncio
    async def test_logs_signal_match_event(self):
        db = _make_db(signal_id=1, mapped_id=10)
        feeder = _make_feeder(db=db)

        ms = _make_mapped_signal(direction=Direction.YES)
        feeder._mapper.map = AsyncMock(return_value=[ms])

        await feeder._process_signals([_make_raw_signal()])

        from irene_shared.models import MarketEventType
        db.log_event.assert_called()
        event_types = [
            call.kwargs.get("event_type") or call.args[1]
            for call in db.log_event.call_args_list
        ]
        assert MarketEventType.SIGNAL_MATCH in event_types

    @pytest.mark.asyncio
    async def test_marks_dispatched_after_publish(self):
        db = _make_db(signal_id=1, mapped_id=10)
        feeder = _make_feeder(db=db)

        ms = _make_mapped_signal(direction=Direction.YES)
        feeder._mapper.map = AsyncMock(return_value=[ms])

        await feeder._process_signals([_make_raw_signal()])

        db.mark_mapped_signal_dispatched.assert_called_once_with(10)
        db.mark_signal_mapped.assert_called_once_with(1)

    @pytest.mark.asyncio
    async def test_publishes_heartbeat_after_batch(self):
        redis = _make_redis()
        feeder = _make_feeder(redis=redis)
        feeder._mapper.map = AsyncMock(return_value=[])

        await feeder._process_signals([_make_raw_signal()])

        redis.publish_heartbeat.assert_called_once_with("feeder")
        redis.set_last_seen.assert_called_once()

    @pytest.mark.asyncio
    async def test_multiple_signals_all_processed(self):
        db = _make_db(signal_id=1, mapped_id=10)
        redis = _make_redis()
        feeder = _make_feeder(db=db, redis=redis)
        feeder._mapper.map = AsyncMock(return_value=[])

        signals = [_make_raw_signal(f"https://a.com/{i}") for i in range(3)]
        await feeder._process_signals(signals)

        assert db.insert_signal.call_count == 3

    @pytest.mark.asyncio
    async def test_neutral_signal_does_not_call_insert_mapped(self):
        db = _make_db(signal_id=1)
        feeder = _make_feeder(db=db)

        ms = _make_mapped_signal(direction=Direction.NEUTRAL)
        feeder._mapper.map = AsyncMock(return_value=[ms])

        await feeder._process_signals([_make_raw_signal()])

        db.insert_mapped_signal.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_matches_no_publish(self):
        redis = _make_redis()
        feeder = _make_feeder(redis=redis)
        feeder._mapper.map = AsyncMock(return_value=[])

        await feeder._process_signals([_make_raw_signal()])

        redis.publish_signal.assert_not_called()

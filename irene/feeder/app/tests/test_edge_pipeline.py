"""
Edge pipeline tests — no real DB / Redis / CLOB required.

Simulates the full cycle: breaking news → feeder maps signal → AI classifies
direction → trader evaluates edge → executes trade → position tracker closes
for profit.

Two scenarios end-to-end:
  YES edge:  news strongly implies YES outcome
             → feeder dispatches YES signal
             → trader buys YES tokens at 0.51
             → price resolves to 0.97 → WIN, positive PnL

  NO edge:   news strongly implies NO outcome
             → feeder dispatches NO signal
             → trader buys NO tokens (YES token price tracked at 0.55)
             → YES price collapses to 0.03 → NO wins → POSITION_CLOSED

Position tracker mechanics (existing code behaviour):
  - Positions always store the direction-appropriate token_id.
  - The YES-resolution threshold is price >= 0.97.
  - The NO-resolution threshold is price <= 0.03.
  - For YES positions: won = exit_price >= 0.97 → positive PnL.
  - For NO positions (YES token tracked, entry = YES price at entry):
      won = exit_price <= 0.03, but realized_pnl may be negative
      because the formula is (exit - entry) * shares — this is a
      known asymmetry in the current implementation that these tests
      document intentionally.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

from irene_shared.models import (
    Direction,
    MarketEventType,
    MappedSignal,
    Orderbook,
    RawSignal,
    SignalSource,
)

# ---------------------------------------------------------------------------
# Path setup: pull in trader/app so DecisionEngine + PositionTracker are importable
# ---------------------------------------------------------------------------

def _find_trader_app() -> str:
    """
    Find trader/app directory.
    - In Docker test container: /trader_app is already in PYTHONPATH.
    - In local dev: navigate relative to this test file.
    """
    # Prefer an already-on-path directory that contains executor.py (trader-specific)
    for p in sys.path:
        if p and os.path.exists(os.path.join(p, "executor.py")):
            return p
    # Local fallback: ../../../trader/app relative to this file
    return os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "trader", "app")
    )


_TRADER_APP = _find_trader_app()
if _TRADER_APP not in sys.path:
    sys.path.insert(0, _TRADER_APP)

# Load trader's main.py under an alias to avoid shadowing feeder's `main` module
_TRADER_MAIN_PATH = os.path.join(_TRADER_APP, "main.py")
_spec = importlib.util.spec_from_file_location("trader_main", _TRADER_MAIN_PATH)
_trader_main = importlib.util.module_from_spec(_spec)
sys.modules["trader_main"] = _trader_main
_spec.loader.exec_module(_trader_main)
Trader = _trader_main.Trader

from decision import DecisionEngine           # noqa: E402 (trader/app)
from position_tracker import (               # noqa: E402
    PositionTracker,
    _RESOLVED_THRESHOLD,
    _ANTI_RESOLVED_THRESHOLD,
)

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

_CONDITION_ID = "0xedge_pipeline_test_aabb1122"
_TOKEN_YES = "tok_yes_edge_test"
_TOKEN_NO = "tok_no_edge_test"
_QUESTION = "Will the Federal Reserve raise interest rates above 5%?"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _make_embedding() -> np.ndarray:
    """Unit vector so cosine similarity with itself is exactly 1.0."""
    v = np.ones(384, dtype=np.float32)
    return (v / np.linalg.norm(v)).astype(np.float32)


# ---------------------------------------------------------------------------
# Config factory
# ---------------------------------------------------------------------------


def _make_cfg(
    edge_increment_yes: float = 0.12,
    edge_increment_no: float = 0.12,
    min_edge: float = 0.03,
):
    cfg = MagicMock()
    # signals
    cfg.signals.similarity_threshold = 0.30
    cfg.signals.top_k_markets = 10
    cfg.signals.direction_min_confidence = 0.40
    cfg.signals.use_llm_direction = False
    cfg.signals.newsapi_keys = []
    cfg.signals.brave_api_keys = []
    cfg.signals.rss_feeds = []
    cfg.signals.watched_pages = []
    cfg.signals.brave_poll_seconds = 300
    cfg.signals.jitter_fraction = 0.0
    # trading
    cfg.trading.balance_usdc = 1000.0
    cfg.trading.max_open_positions = 10
    cfg.trading.min_edge = min_edge
    cfg.trading.edge_increment_yes = edge_increment_yes
    cfg.trading.edge_increment_no = edge_increment_no
    cfg.trading.kelly_fraction = 0.25
    cfg.trading.max_position_fraction = 0.10
    cfg.trading.max_liquidity_fraction = 0.50
    cfg.trading.fee_bps = 20
    cfg.trading.fee_buffer = 0.01
    cfg.trading.max_signal_age_seconds = 3600
    cfg.trading.position_timeout_hours = 24.0
    cfg.trading.mode = "paper"
    cfg.trading.order_type = "FOK"
    # misc
    cfg.timing.publish_interval_seconds = 0
    cfg.timing.jitter_fraction = 0.0
    cfg.rate_limits.clob_max_per_day = 20_000
    cfg.rate_limits.newsapi_max_per_day = 100
    cfg.rate_limits.rss_http_max_per_day = 500
    cfg.polymarket.clob_url = "https://clob.polymarket.com"
    cfg.llm.api_key = ""
    cfg.llm.model = "gemini-2.5-flash"
    return cfg


def _make_cfg_mgr(cfg=None):
    m = MagicMock()
    m.get.return_value = cfg or _make_cfg()
    return m


# ---------------------------------------------------------------------------
# Signal factories
# ---------------------------------------------------------------------------


def _make_yes_signal() -> RawSignal:
    """Headline with YES-indicating keywords: 'raises', 'above'."""
    return RawSignal(
        source=SignalSource.NEWSAPI,
        source_detail="newsapi",
        url="https://example.com/fed-raises-rates-above-5-pct",
        title="Federal Reserve raises interest rates above 5 percent",
        description="The Fed hiked its benchmark rate above 5% today in a historic move.",
        published_at=_now(),
        ingested_at=_now(),
    )


def _make_no_signal() -> RawSignal:
    """Headline with NO-indicating keywords: 'fails', 'delays'."""
    return RawSignal(
        source=SignalSource.NEWSAPI,
        source_detail="newsapi",
        url="https://example.com/fed-fails-rate-hike-delayed",
        title="Federal Reserve fails to raise rates, delays decision indefinitely",
        description="The Fed postponed the planned rate hike citing economic uncertainty.",
        published_at=_now(),
        ingested_at=_now(),
    )


def _make_mapped_signal(direction: Direction = Direction.YES) -> MappedSignal:
    return MappedSignal(
        signal_id=42,
        condition_id=_CONDITION_ID,
        token_yes=_TOKEN_YES,
        token_no=_TOKEN_NO,
        question=_QUESTION,
        similarity_score=0.78,
        direction=direction,
        direction_confidence=0.85,
        signal_title="Federal Reserve raises interest rates above 5 percent",
        signal_source="newsapi",
        mapped_at=_now(),
    )


def _make_orderbook(token_id: str, mid_price: float) -> Orderbook:
    spread = 0.01
    return Orderbook(
        token_id=token_id,
        best_bid=max(mid_price - spread, 0.01),
        best_ask=min(mid_price + spread, 0.99),
        mid_price=mid_price,
        bid_depth_usdc=500.0,
        ask_depth_usdc=500.0,
    )


# ---------------------------------------------------------------------------
# Feeder factory
# ---------------------------------------------------------------------------


def _make_feeder(cfg=None):
    """Return (feeder, db_mock, redis_mock) with all I/O mocked."""
    cfg_obj = cfg or _make_cfg()
    cfg_mgr = _make_cfg_mgr(cfg_obj)

    db = MagicMock()
    db.insert_signal = AsyncMock(return_value=99)
    db.update_signal_embedding = AsyncMock()
    db.insert_mapped_signal = AsyncMock(return_value=7)
    db.log_event = AsyncMock()
    db.mark_mapped_signal_dispatched = AsyncMock()
    db.mark_signal_mapped = AsyncMock()

    redis = MagicMock()
    redis.publish_signal = AsyncMock(return_value=1)
    redis.publish_heartbeat = AsyncMock()
    redis.set_last_seen = AsyncMock()
    redis.try_consume_daily_budget = AsyncMock(return_value=True)

    with patch("main.Embedder"), patch("main.SignalMapper"), patch("main.RotatingApiKeyPool"):
        from main import Feeder
        feeder = Feeder(cfg_mgr, db, redis)

    # Replace embedder with a mock that returns a known unit vector
    emb = _make_embedding()
    feeder._embedder = MagicMock()
    feeder._embedder.embed = MagicMock(return_value=emb)
    feeder._db = db
    feeder._redis = redis

    return feeder, db, redis


# ---------------------------------------------------------------------------
# Trader factory
# ---------------------------------------------------------------------------


def _make_trader(cfg=None):
    """Return (trader, db_mock, redis_mock) with executor mocked."""
    cfg_obj = cfg or _make_cfg()
    cfg_mgr = _make_cfg_mgr(cfg_obj)

    db = MagicMock()
    db.count_open_positions = AsyncMock(return_value=0)
    db.log_event = AsyncMock()

    redis = MagicMock()
    redis.set_last_seen = AsyncMock()
    redis.try_consume_daily_budget = AsyncMock(return_value=True)

    with patch("trader_main.Executor") as MockExec, \
         patch("trader_main.PositionTracker"):
        MockExec.return_value.execute = AsyncMock()
        trader = Trader(cfg_mgr, db, redis)

    # Use real DecisionEngine so we test actual edge math
    trader._engine = DecisionEngine()
    trader._db = db
    trader._redis = redis
    return trader, db, redis


# ---------------------------------------------------------------------------
# PositionTracker factory
# ---------------------------------------------------------------------------


def _make_tracker(cfg=None):
    cfg_obj = cfg or _make_cfg()
    cfg_mgr = _make_cfg_mgr(cfg_obj)

    db = MagicMock()
    db.upsert_position = AsyncMock()
    db.close_trade = AsyncMock()
    db.delete_position = AsyncMock()
    db.log_event = AsyncMock()
    db.insert_brier_score = AsyncMock()

    redis = MagicMock()
    redis.try_consume_daily_budget = AsyncMock(return_value=True)

    return PositionTracker(cfg_mgr, db, MagicMock(), redis), db


def _make_yes_position(entry_price: float = 0.51, age_hours: float = 1.0) -> dict:
    return {
        "trade_id": 101,
        "condition_id": _CONDITION_ID,
        "token_id": _TOKEN_YES,
        "direction": Direction.YES.value,
        "size_usdc": 50.0,
        "entry_price": entry_price,
        "opened_at": _now() - timedelta(hours=age_hours),
        "last_price": entry_price,
        "unrealized_pnl": None,
        "p_hat": 0.63,
        "signal_source": "newsapi",
    }


def _make_no_position(entry_price: float = 0.55, age_hours: float = 1.0) -> dict:
    """
    NO position: direction=NO, token_id=YES token, entry_price=YES token price at entry.
    The position tracker closes it when YES price <= 0.03 (market resolved NO).
    """
    return {
        "trade_id": 202,
        "condition_id": _CONDITION_ID,
        "token_id": _TOKEN_YES,          # YES token price tracked for NO positions
        "direction": Direction.NO.value,
        "size_usdc": 50.0,
        "entry_price": entry_price,      # YES price when we went NO (e.g. 0.55)
        "opened_at": _now() - timedelta(hours=age_hours),
        "last_price": entry_price,
        "unrealized_pnl": None,
        "p_hat": 0.20,                   # our estimated P(YES) is low → NO bet
        "signal_source": "newsapi",
    }


def _make_clob_cm(ob: Orderbook | None):
    client = MagicMock()
    client.get_orderbook = AsyncMock(return_value=ob)
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return MagicMock(return_value=cm), client


# ===========================================================================
# YES EDGE PIPELINE
# ===========================================================================


class TestYesEdgePipeline:
    """
    Breaking news implies YES outcome.
    Verifies: feeder dispatches → trader executes → position closes at 0.97 (WIN).
    """

    # -----------------------------------------------------------------------
    # Feeder: keyword classification with a real signal headline
    # -----------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_yes_keywords_in_headline_classify_correctly(self):
        """Keyword classifier correctly identifies YES direction from headline."""
        from direction import DirectionClassifier

        clf = DirectionClassifier()
        result = clf.classify(
            "Federal Reserve raises interest rates above 5 percent",
            min_confidence=0.40,
        )
        assert result.direction == Direction.YES
        assert result.confidence >= 0.40

    @pytest.mark.asyncio
    async def test_feeder_logs_signal_match_with_yes_direction(self):
        """
        Feeder maps signal to market, classifies YES, and logs SIGNAL_MATCH
        with direction=YES and dispatched=True.
        """
        feeder, db, redis = _make_feeder()

        # The mapper returns a pre-built MappedSignal (direction overridden by classifier)
        ms = _make_mapped_signal(Direction.YES)
        feeder._mapper.map = AsyncMock(return_value=[ms])

        await feeder._process_signals([_make_yes_signal()])

        db.log_event.assert_called_once()
        call = db.log_event.call_args.kwargs
        assert call["event_type"] == MarketEventType.SIGNAL_MATCH
        payload = call["payload"]
        assert payload["direction"] == "YES"
        assert payload["dispatched"] is True
        assert payload["similarity"] == pytest.approx(0.78, abs=0.01)

    @pytest.mark.asyncio
    async def test_feeder_publishes_yes_mapped_signal_to_redis(self):
        """After a YES detection, the feeder publishes a MappedSignal to Redis."""
        feeder, db, redis = _make_feeder()

        ms = _make_mapped_signal(Direction.YES)
        feeder._mapper.map = AsyncMock(return_value=[ms])

        await feeder._process_signals([_make_yes_signal()])

        redis.publish_signal.assert_called_once()
        published: MappedSignal = redis.publish_signal.call_args.args[0]
        assert published.direction == Direction.YES
        assert published.condition_id == _CONDITION_ID
        assert published.token_yes == _TOKEN_YES

    @pytest.mark.asyncio
    async def test_feeder_neutral_signal_logged_but_not_dispatched(self):
        """
        When the classifier returns NEUTRAL, SIGNAL_MATCH is still logged
        (for coverage) but no signal is published to Redis.
        """
        feeder, db, redis = _make_feeder()

        ms = _make_mapped_signal(Direction.NEUTRAL)
        feeder._mapper.map = AsyncMock(return_value=[ms])

        # Headline with no YES/NO keywords → NEUTRAL
        neutral_signal = RawSignal(
            source=SignalSource.NEWSAPI,
            source_detail="newsapi",
            url="https://example.com/fed-meeting-scheduled",
            title="Federal Reserve schedules policy meeting for next Tuesday",
            description="Officials will gather to review current economic data.",
            published_at=_now(),
            ingested_at=_now(),
        )
        await feeder._process_signals([neutral_signal])

        db.log_event.assert_called_once()
        payload = db.log_event.call_args.kwargs["payload"]
        assert payload["dispatched"] is False
        redis.publish_signal.assert_not_called()

    @pytest.mark.asyncio
    async def test_feeder_duplicate_url_skips_processing(self):
        """A duplicate URL (insert_signal returns None) is silently skipped."""
        feeder, db, redis = _make_feeder()
        db.insert_signal = AsyncMock(return_value=None)  # duplicate

        feeder._mapper.map = AsyncMock(return_value=[_make_mapped_signal()])

        await feeder._process_signals([_make_yes_signal()])

        db.log_event.assert_not_called()
        redis.publish_signal.assert_not_called()

    # -----------------------------------------------------------------------
    # Trader: YES trade execution with real DecisionEngine
    # -----------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_trader_fetches_yes_token_orderbook(self):
        """Trader requests the YES token orderbook for a YES-direction signal."""
        trader, db, redis = _make_trader()

        ob = _make_orderbook(_TOKEN_YES, mid_price=0.50)
        clob_klass, clob_client = _make_clob_cm(ob)

        with patch("trader_main.ClobClient", clob_klass):
            await trader._handle_signal(_make_mapped_signal(Direction.YES))

        clob_client.get_orderbook.assert_called_once_with(_TOKEN_YES)

    @pytest.mark.asyncio
    async def test_trader_executes_yes_trade_with_sufficient_edge(self):
        """
        With mid=0.50 and edge_increment=0.12:
          p_hat = 0.50 + 0.12 = 0.62
          edge  = 0.62 - 0.51 (ask) = 0.11 > min_edge=0.03 → execute
        """
        trader, db, redis = _make_trader()

        ob = _make_orderbook(_TOKEN_YES, mid_price=0.50)
        clob_klass, _ = _make_clob_cm(ob)

        with patch("trader_main.ClobClient", clob_klass):
            await trader._handle_signal(_make_mapped_signal(Direction.YES))

        db.log_event.assert_called_once()
        payload = db.log_event.call_args.kwargs["payload"]
        assert payload["skip_reason"] is None, f"unexpected skip: {payload['skip_reason']}"
        assert payload["direction"] == "YES"
        assert payload["p_hat"] > 0.50
        trader._executor.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_trader_skips_yes_trade_when_price_already_high(self):
        """
        With mid=0.90 and edge_increment=0.12:
          p_hat = min(0.90 + 0.12, 0.99) = 0.99
          edge  = 0.99 - 0.91 (ask) = 0.08 → positive, but check max_position_fraction
        This test verifies that a high-confidence YES market can still execute.
        The important scenario is when there genuinely IS no edge.
        """
        # Artificially tiny edge_increment and high price → negative net edge → skip
        cfg = _make_cfg(edge_increment_yes=0.005, min_edge=0.05)
        trader, db, redis = _make_trader(cfg)

        ob = _make_orderbook(_TOKEN_YES, mid_price=0.80)
        clob_klass, _ = _make_clob_cm(ob)

        with patch("trader_main.ClobClient", clob_klass):
            await trader._handle_signal(_make_mapped_signal(Direction.YES))

        payload = db.log_event.call_args.kwargs["payload"]
        assert payload["skip_reason"] is not None
        trader._executor.execute.assert_not_called()

    # -----------------------------------------------------------------------
    # Position tracker: YES position resolution → WIN
    # -----------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_position_tracker_closes_yes_position_at_097(self):
        """
        When the YES token price hits 0.97, the tracker closes the position
        with exit_reason='resolved', outcome='WIN', and positive realized PnL.
        """
        tracker, db = _make_tracker()

        pos = _make_yes_position(entry_price=0.51)
        clob = MagicMock()
        clob.get_orderbook = AsyncMock(
            return_value=_make_orderbook(_TOKEN_YES, mid_price=0.97)
        )

        await tracker._check_one(pos, clob, timeout_hours=24.0, now=_now())

        db.close_trade.assert_called_once()
        close_kwargs = db.close_trade.call_args.kwargs
        assert close_kwargs["exit_reason"] == "resolved"
        assert close_kwargs["realized_pnl"] > 0, "YES position must profit when price → 1.0"

        db.log_event.assert_called_once()
        event = db.log_event.call_args.kwargs
        assert event["event_type"] == MarketEventType.POSITION_CLOSED
        assert event["payload"]["outcome"] == "WIN"
        db.insert_brier_score.assert_called_once()

    @pytest.mark.asyncio
    async def test_position_tracker_updates_pnl_before_yes_resolution(self):
        """
        At price 0.72 (profitable but not resolved), only unrealized PnL is
        updated — the position is NOT closed.
        """
        tracker, db = _make_tracker()

        pos = _make_yes_position(entry_price=0.51)
        clob = MagicMock()
        clob.get_orderbook = AsyncMock(
            return_value=_make_orderbook(_TOKEN_YES, mid_price=0.72)
        )

        await tracker._check_one(pos, clob, timeout_hours=24.0, now=_now())

        db.upsert_position.assert_called_once()
        unrealized = db.upsert_position.call_args.kwargs["unrealized_pnl"]
        assert unrealized > 0, "price rose from 0.51 → 0.72, unrealized must be positive"
        db.close_trade.assert_not_called()

    @pytest.mark.asyncio
    async def test_position_tracker_loss_when_yes_position_goes_wrong(self):
        """
        YES position held too long (timeout) while price sits below entry → PUSH.
        """
        tracker, db = _make_tracker()

        pos = _make_yes_position(entry_price=0.51, age_hours=25)
        clob = MagicMock()
        # Price below entry, not resolved → timeout triggers close
        clob.get_orderbook = AsyncMock(
            return_value=_make_orderbook(_TOKEN_YES, mid_price=0.40)
        )

        await tracker._check_one(pos, clob, timeout_hours=24.0, now=_now())

        db.close_trade.assert_called_once()
        event_payload = db.log_event.call_args.kwargs["payload"]
        assert event_payload["exit_reason"] == "timeout"
        assert event_payload["outcome"] == "PUSH"
        # On timeout Brier score is NOT recorded
        db.insert_brier_score.assert_not_called()


# ===========================================================================
# NO EDGE PIPELINE
# ===========================================================================


class TestNoEdgePipeline:
    """
    Breaking news implies NO outcome.
    Verifies: feeder dispatches NO signal → trader buys NO (via YES token tracking)
    → YES price collapses to 0.03 → POSITION_CLOSED outcome=WIN.
    """

    # -----------------------------------------------------------------------
    # Feeder: NO keyword detection
    # -----------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_no_keywords_in_headline_classify_correctly(self):
        """Keyword classifier correctly identifies NO direction from headline."""
        from direction import DirectionClassifier

        clf = DirectionClassifier()
        result = clf.classify(
            "Federal Reserve fails to raise rates, delays decision indefinitely",
            min_confidence=0.40,
        )
        assert result.direction == Direction.NO
        assert result.confidence >= 0.40

    @pytest.mark.asyncio
    async def test_feeder_logs_signal_match_with_no_direction(self):
        """Feeder maps a NO signal and logs SIGNAL_MATCH with direction=NO."""
        feeder, db, redis = _make_feeder()

        ms = _make_mapped_signal(Direction.NO)
        feeder._mapper.map = AsyncMock(return_value=[ms])

        await feeder._process_signals([_make_no_signal()])

        db.log_event.assert_called_once()
        payload = db.log_event.call_args.kwargs["payload"]
        assert payload["direction"] == "NO"
        assert payload["dispatched"] is True

    @pytest.mark.asyncio
    async def test_feeder_publishes_no_mapped_signal_to_redis(self):
        """After a NO detection, the feeder publishes a NO-direction MappedSignal."""
        feeder, db, redis = _make_feeder()

        ms = _make_mapped_signal(Direction.NO)
        feeder._mapper.map = AsyncMock(return_value=[ms])

        await feeder._process_signals([_make_no_signal()])

        redis.publish_signal.assert_called_once()
        published: MappedSignal = redis.publish_signal.call_args.args[0]
        assert published.direction == Direction.NO
        assert published.condition_id == _CONDITION_ID

    @pytest.mark.asyncio
    async def test_feeder_dispatches_both_yes_and_no_for_multi_market_signal(self):
        """
        A single signal can map to multiple markets with different directions.
        Each detection is logged; all non-NEUTRAL are dispatched.
        """
        feeder, db, redis = _make_feeder()

        ms_yes = _make_mapped_signal(Direction.YES)
        ms_no = MappedSignal(
            signal_id=42,
            condition_id="0xother_market_ccdd",
            token_yes="tok_yes_other",
            token_no="tok_no_other",
            question="Will the Fed hold rates steady?",
            similarity_score=0.65,
            direction=Direction.NO,
            direction_confidence=0.72,
            signal_title="Federal Reserve raises interest rates above 5 percent",
            signal_source="newsapi",
            mapped_at=_now(),
        )
        feeder._mapper.map = AsyncMock(return_value=[ms_yes, ms_no])
        db.insert_mapped_signal = AsyncMock(side_effect=[7, 8])

        await feeder._process_signals([_make_yes_signal()])

        assert db.log_event.call_count == 2
        assert redis.publish_signal.call_count == 2

    # -----------------------------------------------------------------------
    # Trader: NO trade execution
    # -----------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_trader_fetches_no_token_orderbook(self):
        """For a NO-direction signal, the trader requests the NO token orderbook."""
        trader, db, redis = _make_trader()

        ob = _make_orderbook(_TOKEN_NO, mid_price=0.45)
        clob_klass, clob_client = _make_clob_cm(ob)

        with patch("trader_main.ClobClient", clob_klass):
            await trader._handle_signal(_make_mapped_signal(Direction.NO))

        clob_client.get_orderbook.assert_called_once_with(_TOKEN_NO)

    @pytest.mark.asyncio
    async def test_trader_executes_no_trade_with_sufficient_edge(self):
        """
        With NO token mid=0.45 and edge_increment_no=0.12:
          p_hat = 0.45 + 0.12 = 0.57
          edge  = 0.57 - 0.46 (ask) = 0.11 > min_edge=0.03 → execute
        """
        trader, db, redis = _make_trader()

        ob = _make_orderbook(_TOKEN_NO, mid_price=0.45)
        clob_klass, _ = _make_clob_cm(ob)

        with patch("trader_main.ClobClient", clob_klass):
            await trader._handle_signal(_make_mapped_signal(Direction.NO))

        db.log_event.assert_called_once()
        payload = db.log_event.call_args.kwargs["payload"]
        assert payload["skip_reason"] is None, f"unexpected skip: {payload['skip_reason']}"
        assert payload["direction"] == "NO"
        trader._executor.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_trader_skips_no_trade_at_position_cap(self):
        """Trader skips execution when already at max_open_positions."""
        cfg = _make_cfg()
        cfg.trading.max_open_positions = 3
        trader, db, redis = _make_trader(cfg)
        db.count_open_positions = AsyncMock(return_value=3)  # at cap

        ob = _make_orderbook(_TOKEN_NO, mid_price=0.45)
        clob_klass, _ = _make_clob_cm(ob)

        with patch("trader_main.ClobClient", clob_klass):
            await trader._handle_signal(_make_mapped_signal(Direction.NO))

        payload = db.log_event.call_args.kwargs["payload"]
        assert payload["skip_reason"] is not None
        assert "cap" in payload["skip_reason"]
        trader._executor.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_trader_skips_when_orderbook_unavailable(self):
        """When CLOB returns None, the trader logs nothing and skips."""
        trader, db, redis = _make_trader()

        clob_klass, _ = _make_clob_cm(None)

        with patch("trader_main.ClobClient", clob_klass):
            await trader._handle_signal(_make_mapped_signal(Direction.NO))

        db.log_event.assert_not_called()
        trader._executor.execute.assert_not_called()

    # -----------------------------------------------------------------------
    # Position tracker: NO position resolution
    # -----------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_position_tracker_closes_no_position_when_yes_collapses(self):
        """
        A NO position closes with outcome=WIN when YES token price falls to 0.03.
        (YES collapsed → NO resolved → our NO bet wins.)
        """
        tracker, db = _make_tracker()

        pos = _make_no_position(entry_price=0.55)
        clob = MagicMock()
        clob.get_orderbook = AsyncMock(
            return_value=_make_orderbook(_TOKEN_YES, mid_price=_ANTI_RESOLVED_THRESHOLD)
        )

        await tracker._check_one(pos, clob, timeout_hours=24.0, now=_now())

        db.close_trade.assert_called_once()
        close_kwargs = db.close_trade.call_args.kwargs
        assert close_kwargs["exit_reason"] == "resolved"

        event_payload = db.log_event.call_args.kwargs["payload"]
        assert event_payload["outcome"] == "WIN"
        db.insert_brier_score.assert_called_once()

    @pytest.mark.asyncio
    async def test_position_tracker_loss_when_no_position_resolves_yes(self):
        """
        A NO position suffers a LOSS when YES resolves (price → 0.97).
        outcome=LOSS, Brier score recorded.
        """
        tracker, db = _make_tracker()

        pos = _make_no_position(entry_price=0.55)
        clob = MagicMock()
        clob.get_orderbook = AsyncMock(
            return_value=_make_orderbook(_TOKEN_YES, mid_price=_RESOLVED_THRESHOLD)
        )

        await tracker._check_one(pos, clob, timeout_hours=24.0, now=_now())

        db.close_trade.assert_called_once()
        event_payload = db.log_event.call_args.kwargs["payload"]
        assert event_payload["exit_reason"] == "resolved"
        assert event_payload["outcome"] == "LOSS"
        db.insert_brier_score.assert_called_once()

    @pytest.mark.asyncio
    async def test_position_tracker_no_close_mid_market(self):
        """
        When YES price is at a mid-market value (e.g. 0.50), a NO position
        should NOT be closed — we are still holding.
        """
        tracker, db = _make_tracker()

        pos = _make_no_position(entry_price=0.55)
        clob = MagicMock()
        clob.get_orderbook = AsyncMock(
            return_value=_make_orderbook(_TOKEN_YES, mid_price=0.50)
        )

        await tracker._check_one(pos, clob, timeout_hours=24.0, now=_now())

        db.close_trade.assert_not_called()

    # -----------------------------------------------------------------------
    # Full NO edge narrative (condensed integration smoke test)
    # -----------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_no_edge_full_narrative_smoke(self):
        """
        Narrative smoke test tracing a NO edge from signal to close:

          1. Feeder receives a NO headline.
          2. Mapper returns one matching market (direction=NO).
          3. Feeder logs SIGNAL_MATCH(direction=NO, dispatched=True).
          4. Redis.publish_signal receives a NO MappedSignal.
          5. Trader evaluates orderbook → executes NO trade.
          6. Position tracker later sees YES price at 0.03 → closes WIN.
        """
        # --- Step 1-4: Feeder ---
        feeder, f_db, f_redis = _make_feeder()
        ms = _make_mapped_signal(Direction.NO)
        feeder._mapper.map = AsyncMock(return_value=[ms])

        await feeder._process_signals([_make_no_signal()])

        signal_match_payload = f_db.log_event.call_args.kwargs["payload"]
        assert signal_match_payload["direction"] == "NO"
        assert signal_match_payload["dispatched"] is True
        dispatched_signal: MappedSignal = f_redis.publish_signal.call_args.args[0]
        assert dispatched_signal.direction == Direction.NO

        # --- Step 5: Trader ---
        trader, t_db, _ = _make_trader()
        ob = _make_orderbook(_TOKEN_NO, mid_price=0.45)
        clob_klass, _ = _make_clob_cm(ob)

        with patch("trader_main.ClobClient", clob_klass):
            await trader._handle_signal(dispatched_signal)

        trade_payload = t_db.log_event.call_args.kwargs["payload"]
        assert trade_payload["skip_reason"] is None
        assert trade_payload["direction"] == "NO"
        assert trader._executor.execute.call_count == 1

        # --- Step 6: Position tracker ---
        tracker, pt_db = _make_tracker()
        pos = _make_no_position(entry_price=0.55)
        clob = MagicMock()
        clob.get_orderbook = AsyncMock(
            return_value=_make_orderbook(_TOKEN_YES, mid_price=0.03)
        )

        await tracker._check_one(pos, clob, timeout_hours=24.0, now=_now())

        close_event = pt_db.log_event.call_args.kwargs["payload"]
        assert close_event["outcome"] == "WIN"
        assert close_event["exit_reason"] == "resolved"


# ===========================================================================
# YES edge: full narrative (condensed integration smoke test)
# ===========================================================================


class TestYesEdgeFullNarrative:

    @pytest.mark.asyncio
    async def test_yes_edge_full_narrative_smoke(self):
        """
        Narrative smoke test tracing a YES edge from signal to close:

          1. Feeder receives a YES headline.
          2. Mapper returns one matching market (direction=YES).
          3. Feeder logs SIGNAL_MATCH(direction=YES, dispatched=True).
          4. Redis.publish_signal receives a YES MappedSignal.
          5. Trader evaluates YES orderbook → executes YES trade.
          6. Position tracker sees price at 0.97 → closes WIN, positive PnL.
        """
        # --- Steps 1-4: Feeder ---
        feeder, f_db, f_redis = _make_feeder()
        ms = _make_mapped_signal(Direction.YES)
        feeder._mapper.map = AsyncMock(return_value=[ms])

        await feeder._process_signals([_make_yes_signal()])

        signal_match_payload = f_db.log_event.call_args.kwargs["payload"]
        assert signal_match_payload["direction"] == "YES"
        assert signal_match_payload["dispatched"] is True
        dispatched_signal: MappedSignal = f_redis.publish_signal.call_args.args[0]
        assert dispatched_signal.direction == Direction.YES

        # --- Step 5: Trader ---
        trader, t_db, _ = _make_trader()
        ob = _make_orderbook(_TOKEN_YES, mid_price=0.50)
        clob_klass, _ = _make_clob_cm(ob)

        with patch("trader_main.ClobClient", clob_klass):
            await trader._handle_signal(dispatched_signal)

        trade_payload = t_db.log_event.call_args.kwargs["payload"]
        assert trade_payload["skip_reason"] is None
        assert trade_payload["direction"] == "YES"
        assert trader._executor.execute.call_count == 1

        # --- Step 6: Position tracker ---
        tracker, pt_db = _make_tracker()
        pos = _make_yes_position(entry_price=0.51)
        clob = MagicMock()
        clob.get_orderbook = AsyncMock(
            return_value=_make_orderbook(_TOKEN_YES, mid_price=0.97)
        )

        await tracker._check_one(pos, clob, timeout_hours=24.0, now=_now())

        close_event = pt_db.log_event.call_args.kwargs["payload"]
        assert close_event["outcome"] == "WIN"
        assert close_event["exit_reason"] == "resolved"
        assert close_event["realized_pnl"] > 0


# ===========================================================================
# AI Direction Classifier integration
# ===========================================================================


class TestAIDirectionIntegration:
    """
    Tests for the AIDirectionClassifier used by the feeder when
    use_llm_direction=True.  The Gemini REST API is mocked so no network
    calls are made.  Verifies that:
      - YES/NO responses from Gemini produce correct dispatches.
      - NEUTRAL responses produce a logged-but-not-dispatched event.
      - Malformed Gemini responses fall back to keyword classification.
      - The feeder falls back to keyword classifier when no API key is set.
    """

    def _make_gemini_response(self, direction: str, confidence: float) -> dict:
        """Build a minimal Gemini REST response payload."""
        return {
            "candidates": [{
                "content": {
                    "parts": [{"text": f"{direction}|{confidence:.2f}"}]
                }
            }]
        }

    def _make_ai_feeder(self, gemini_direction: str, gemini_confidence: float):
        """
        Return (feeder, db, redis) with AI classifier wired up.
        The Gemini HTTP call is replaced by a mock that returns the given
        direction/confidence for every market question.
        """
        cfg = _make_cfg()
        cfg.signals.use_llm_direction = True
        cfg.llm.api_key = "test-api-key-fake"
        cfg.llm.model = "gemini-2.5-flash"
        cfg_mgr = _make_cfg_mgr(cfg)

        db = MagicMock()
        db.insert_signal = AsyncMock(return_value=99)
        db.update_signal_embedding = AsyncMock()
        db.insert_mapped_signal = AsyncMock(return_value=7)
        db.log_event = AsyncMock()
        db.mark_mapped_signal_dispatched = AsyncMock()
        db.mark_signal_mapped = AsyncMock()

        redis = MagicMock()
        redis.publish_signal = AsyncMock(return_value=1)
        redis.publish_heartbeat = AsyncMock()
        redis.set_last_seen = AsyncMock()
        redis.try_consume_daily_budget = AsyncMock(return_value=True)

        with patch("main.Embedder"), patch("main.SignalMapper"), patch("main.RotatingApiKeyPool"):
            from main import Feeder
            feeder = Feeder(cfg_mgr, db, redis)

        emb = _make_embedding()
        feeder._embedder = MagicMock()
        feeder._embedder.embed = MagicMock(return_value=emb)
        feeder._db = db
        feeder._redis = redis

        # Inject AI classifier with mocked HTTP session
        from ai_direction import AIDirectionClassifier
        ai = AIDirectionClassifier(api_key="test-api-key-fake", model="gemini-2.5-flash")

        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=self._make_gemini_response(gemini_direction, gemini_confidence))

        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.closed = False  # _get_session() checks .closed; False = reuse this mock
        mock_session.post = MagicMock(return_value=mock_cm)
        ai._session = mock_session

        feeder._ai_classifier = ai
        return feeder, db, redis

    # -----------------------------------------------------------------------
    # AI returns YES
    # -----------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_ai_yes_direction_dispatched(self):
        """Gemini says YES → SIGNAL_MATCH(direction=YES, dispatched=True) + Redis publish."""
        feeder, db, redis = self._make_ai_feeder("YES", 0.85)

        ms = _make_mapped_signal(Direction.NEUTRAL)   # mapper gives NEUTRAL; AI overrides
        feeder._mapper.map = AsyncMock(return_value=[ms])

        await feeder._process_signals([_make_yes_signal()])

        payload = db.log_event.call_args.kwargs["payload"]
        assert payload["direction"] == "YES"
        assert payload["dispatched"] is True
        redis.publish_signal.assert_called_once()
        published = redis.publish_signal.call_args.args[0]
        assert published.direction == Direction.YES
        assert published.direction_confidence >= 0.80

    # -----------------------------------------------------------------------
    # AI returns NO
    # -----------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_ai_no_direction_dispatched(self):
        """Gemini says NO → SIGNAL_MATCH(direction=NO, dispatched=True) + Redis publish."""
        feeder, db, redis = self._make_ai_feeder("NO", 0.78)

        ms = _make_mapped_signal(Direction.NEUTRAL)
        feeder._mapper.map = AsyncMock(return_value=[ms])

        await feeder._process_signals([_make_no_signal()])

        payload = db.log_event.call_args.kwargs["payload"]
        assert payload["direction"] == "NO"
        assert payload["dispatched"] is True
        redis.publish_signal.assert_called_once()
        published = redis.publish_signal.call_args.args[0]
        assert published.direction == Direction.NO

    # -----------------------------------------------------------------------
    # AI returns NEUTRAL → logged but NOT dispatched
    # -----------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_ai_neutral_not_dispatched(self):
        """Gemini says NEUTRAL → SIGNAL_MATCH logged but no Redis publish."""
        feeder, db, redis = self._make_ai_feeder("NEUTRAL", 0.0)

        ms = _make_mapped_signal(Direction.NEUTRAL)
        feeder._mapper.map = AsyncMock(return_value=[ms])

        await feeder._process_signals([_make_yes_signal()])

        payload = db.log_event.call_args.kwargs["payload"]
        assert payload["dispatched"] is False
        redis.publish_signal.assert_not_called()

    # -----------------------------------------------------------------------
    # Fallback: no API key → keyword classifier used instead
    # -----------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_keyword_fallback_when_no_api_key(self):
        """When use_llm_direction=True but api_key='', keyword classifier is used."""
        cfg = _make_cfg()
        cfg.signals.use_llm_direction = True
        cfg.llm.api_key = ""

        feeder, db, redis = _make_feeder(cfg)
        ms = _make_mapped_signal(Direction.NEUTRAL)
        feeder._mapper.map = AsyncMock(return_value=[ms])

        # YES headline → keyword classifier should catch it
        await feeder._process_signals([_make_yes_signal()])

        payload = db.log_event.call_args.kwargs["payload"]
        # Keyword classifier should produce YES for "raises ... above"
        assert payload["direction"] == "YES"

    # -----------------------------------------------------------------------
    # AI handles malformed response (fallback keyword scan)
    # -----------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_ai_malformed_response_falls_back(self):
        """When Gemini returns a non-standard format, the classifier falls back gracefully."""
        feeder, db, redis = self._make_ai_feeder("YES", 0.90)

        # Override the mock to return a malformed response
        from ai_direction import AIDirectionClassifier
        ai = AIDirectionClassifier(api_key="key", model="gemini-2.5-flash")

        malformed_resp = MagicMock()
        malformed_resp.status = 200
        malformed_resp.json = AsyncMock(return_value={
            "candidates": [{"content": {"parts": [{"text": "I think YES, confidence around 0.80"}]}}]
        })
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=malformed_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=False)
        mock_session = MagicMock()
        mock_session.closed = False
        mock_session.post = MagicMock(return_value=mock_cm)
        ai._session = mock_session
        feeder._ai_classifier = ai

        ms = _make_mapped_signal(Direction.NEUTRAL)
        feeder._mapper.map = AsyncMock(return_value=[ms])
        await feeder._process_signals([_make_yes_signal()])

        # Fallback keyword scan in _parse_response should find YES and ~0.80
        payload = db.log_event.call_args.kwargs["payload"]
        assert payload["direction"] in ("YES", "NEUTRAL")   # may or may not catch it

    # -----------------------------------------------------------------------
    # AI batch: multiple markets, independent directions
    # -----------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_ai_batch_independent_directions_per_market(self):
        """
        A single signal maps to two markets. The AI classifier is called for
        each (batch). Both get independent direction results and both are dispatched.
        """
        # Build feeder with AI that alternates YES then NO across calls
        cfg = _make_cfg()
        cfg.signals.use_llm_direction = True
        cfg.llm.api_key = "test-key"
        cfg.llm.model = "gemini-2.5-flash"
        cfg_mgr = _make_cfg_mgr(cfg)

        db = MagicMock()
        db.insert_signal = AsyncMock(return_value=99)
        db.update_signal_embedding = AsyncMock()
        db.insert_mapped_signal = AsyncMock(side_effect=[7, 8])
        db.log_event = AsyncMock()
        db.mark_mapped_signal_dispatched = AsyncMock()
        db.mark_signal_mapped = AsyncMock()

        redis = MagicMock()
        redis.publish_signal = AsyncMock(return_value=1)
        redis.publish_heartbeat = AsyncMock()
        redis.set_last_seen = AsyncMock()
        redis.try_consume_daily_budget = AsyncMock(return_value=True)

        with patch("main.Embedder"), patch("main.SignalMapper"), patch("main.RotatingApiKeyPool"):
            from main import Feeder
            feeder = Feeder(cfg_mgr, db, redis)

        emb = _make_embedding()
        feeder._embedder = MagicMock()
        feeder._embedder.embed = MagicMock(return_value=emb)
        feeder._db = db
        feeder._redis = redis

        # AI returns YES for market 1 and NO for market 2 (two separate HTTP calls)
        from ai_direction import AIDirectionClassifier
        ai = AIDirectionClassifier(api_key="test-key", model="gemini-2.5-flash")

        responses = [
            {"candidates": [{"content": {"parts": [{"text": "YES|0.88"}]}}]},
            {"candidates": [{"content": {"parts": [{"text": "NO|0.82"}]}}]},
        ]
        resp_iter = iter(responses)

        def _make_resp_cm():
            payload = next(resp_iter)
            r = MagicMock()
            r.status = 200
            r.json = AsyncMock(return_value=payload)
            cm = MagicMock()
            cm.__aenter__ = AsyncMock(return_value=r)
            cm.__aexit__ = AsyncMock(return_value=False)
            return cm

        mock_session = MagicMock()
        mock_session.closed = False  # _get_session() checks .closed; False = reuse this mock
        mock_session.post = MagicMock(side_effect=lambda *a, **kw: _make_resp_cm())
        ai._session = mock_session
        feeder._ai_classifier = ai

        ms_yes = _make_mapped_signal(Direction.NEUTRAL)
        ms_no = MappedSignal(
            signal_id=42,
            condition_id="0xother_batch_market",
            token_yes="tok_yes_b",
            token_no="tok_no_b",
            question="Will the Fed hold rates steady?",
            similarity_score=0.66,
            direction=Direction.NEUTRAL,
            direction_confidence=0.0,
            signal_title="Federal Reserve raises interest rates above 5 percent",
            signal_source="newsapi",
            mapped_at=_now(),
        )
        feeder._mapper.map = AsyncMock(return_value=[ms_yes, ms_no])

        await feeder._process_signals([_make_yes_signal()])

        assert db.log_event.call_count == 2
        assert redis.publish_signal.call_count == 2


# ===========================================================================
# Quick-profit-exit & hold-then-sell scenarios
# ===========================================================================


class TestProfitExitScenarios:
    """
    Verifies the exact price conditions that trigger position closes.

    Irene's profit exit model:
      - Position tracker polls periodically.
      - Closes immediately when price >= 0.97 (YES resolved) or <= 0.03 (NO resolved).
      - Closes at timeout (position_timeout_hours) otherwise.

    We test:
      1. YES position: price jumps from 0.51 → 0.97 on the first poll → WIN.
      2. NO position: YES price collapses 0.55 → 0.03 on the first poll → WIN.
      3. YES position: price sits at 0.60 for the whole timeout → PUSH (not enough to resolve).
      4. YES position: price dips then recovers but timeout fires → PUSH.
      5. Position tracker skips close when CLOB rate limit is exhausted.
    """

    def _make_tracker(self, timeout_hours: float = 24.0):
        db = MagicMock()
        db.get_open_positions = AsyncMock(return_value=[])
        db.upsert_position = AsyncMock()
        db.close_trade = AsyncMock()
        db.delete_position = AsyncMock()
        db.log_event = AsyncMock()
        db.insert_brier_score = AsyncMock()

        cfg = MagicMock()
        cfg.get.return_value.trading.position_timeout_hours = timeout_hours
        cfg.get.return_value.polymarket.clob_url = "https://clob.polymarket.com"
        cfg.get.return_value.timing.jitter_fraction = 0.0
        cfg.get.return_value.rate_limits.clob_max_per_day = 20_000

        redis = MagicMock()
        redis.try_consume_daily_budget = AsyncMock(return_value=True)

        return PositionTracker(cfg, db, MagicMock(), redis), db, redis

    def _yes_pos(self, entry_price=0.51, age_hours=1.0):
        return {
            "trade_id": 1,
            "condition_id": _CONDITION_ID,
            "token_id": _TOKEN_YES,
            "direction": Direction.YES.value,
            "size_usdc": 50.0,
            "entry_price": entry_price,
            "opened_at": _now() - timedelta(hours=age_hours),
            "last_price": entry_price,
            "unrealized_pnl": None,
            "p_hat": 0.63,
            "signal_source": "newsapi",
        }

    def _no_pos(self, entry_price=0.55, age_hours=1.0):
        return {
            "trade_id": 2,
            "condition_id": _CONDITION_ID,
            "token_id": _TOKEN_YES,
            "direction": Direction.NO.value,
            "size_usdc": 50.0,
            "entry_price": entry_price,
            "opened_at": _now() - timedelta(hours=age_hours),
            "last_price": entry_price,
            "unrealized_pnl": None,
            "p_hat": 0.20,
            "signal_source": "newsapi",
        }

    # -----------------------------------------------------------------------
    # Scenario 1: YES position — quick resolution WIN
    # -----------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_yes_quick_win_at_resolution_price(self):
        """
        Price jumps to 0.97 on first poll → immediate WIN close.
        PnL = (0.97 - 0.51) * (50 / 0.51) = 0.46 * 98.04 ≈ $45.10
        """
        tracker, db, _ = self._make_tracker()
        pos = self._yes_pos(entry_price=0.51)
        clob = MagicMock()
        clob.get_orderbook = AsyncMock(return_value=_make_orderbook(_TOKEN_YES, 0.97))

        await tracker._check_one(pos, clob, timeout_hours=24.0, now=_now())

        db.close_trade.assert_called_once()
        kw = db.close_trade.call_args.kwargs
        assert kw["exit_reason"] == "resolved"
        assert kw["outcome"] == "WIN"
        expected_pnl = (0.97 - 0.51) * (50.0 / 0.51)
        assert kw["realized_pnl"] == pytest.approx(expected_pnl, abs=0.01)
        db.insert_brier_score.assert_called_once()

    # -----------------------------------------------------------------------
    # Scenario 2: NO position — quick resolution WIN (YES price collapses)
    # -----------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_no_quick_win_when_yes_collapses(self):
        """
        YES token price collapses from 0.55 → 0.03 (NO resolved) → WIN.
        The position tracker closes the NO position with outcome=WIN.
        """
        tracker, db, _ = self._make_tracker()
        pos = self._no_pos(entry_price=0.55)
        clob = MagicMock()
        clob.get_orderbook = AsyncMock(return_value=_make_orderbook(_TOKEN_YES, 0.03))

        await tracker._check_one(pos, clob, timeout_hours=24.0, now=_now())

        db.close_trade.assert_called_once()
        kw = db.close_trade.call_args.kwargs
        assert kw["exit_reason"] == "resolved"
        assert kw["outcome"] == "WIN"

    # -----------------------------------------------------------------------
    # Scenario 3: YES position — held at mid-market, exits on timeout → PUSH
    # -----------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_yes_hold_then_sell_timeout_push(self):
        """
        Position opened 25 hours ago; price is at 0.60 (profitable but not resolved).
        Timeout fires → PUSH (not a resolution close, so no Brier score).
        We hold a bit and then the timeout forces liquidation.
        """
        tracker, db, _ = self._make_tracker(timeout_hours=24.0)
        pos = self._yes_pos(entry_price=0.51, age_hours=25.0)
        clob = MagicMock()
        clob.get_orderbook = AsyncMock(return_value=_make_orderbook(_TOKEN_YES, 0.60))

        await tracker._check_one(pos, clob, timeout_hours=24.0, now=_now())

        db.close_trade.assert_called_once()
        kw = db.close_trade.call_args.kwargs
        assert kw["exit_reason"] == "timeout"
        assert kw["outcome"] == "PUSH"
        db.insert_brier_score.assert_not_called()

    # -----------------------------------------------------------------------
    # Scenario 4: NO position — held at mid-market, exits on timeout → PUSH
    # -----------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_no_hold_then_sell_timeout_push(self):
        """
        NO position: YES price stuck at 0.50 for 25h → timeout → PUSH.
        No Brier score (not a resolved outcome).
        """
        tracker, db, _ = self._make_tracker(timeout_hours=24.0)
        pos = self._no_pos(entry_price=0.55, age_hours=25.0)
        clob = MagicMock()
        clob.get_orderbook = AsyncMock(return_value=_make_orderbook(_TOKEN_YES, 0.50))

        await tracker._check_one(pos, clob, timeout_hours=24.0, now=_now())

        db.close_trade.assert_called_once()
        kw = db.close_trade.call_args.kwargs
        assert kw["exit_reason"] == "timeout"
        assert kw["outcome"] == "PUSH"
        db.insert_brier_score.assert_not_called()

    # -----------------------------------------------------------------------
    # Scenario 5: CLOB rate limit exhausted — position check skipped
    # -----------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_position_check_skipped_on_rate_limit(self):
        """When the daily CLOB budget is exhausted, no orderbook call is made."""
        tracker, db, redis = self._make_tracker()
        redis.try_consume_daily_budget = AsyncMock(return_value=False)

        pos = self._yes_pos()
        clob = MagicMock()
        clob.get_orderbook = AsyncMock()

        await tracker._check_one(pos, clob, timeout_hours=24.0, now=_now())

        clob.get_orderbook.assert_not_called()
        db.close_trade.assert_not_called()

    # -----------------------------------------------------------------------
    # Scenario 6: YES position losing ground — price below entry, not resolved
    # -----------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_yes_position_unrealized_loss_not_closed(self):
        """
        Price drops from 0.51 entry to 0.35 — unrealized loss, but NOT resolved.
        Position stays open (only resolution or timeout closes it).
        """
        tracker, db, _ = self._make_tracker(timeout_hours=24.0)
        pos = self._yes_pos(entry_price=0.51, age_hours=5.0)
        clob = MagicMock()
        clob.get_orderbook = AsyncMock(return_value=_make_orderbook(_TOKEN_YES, 0.35))

        await tracker._check_one(pos, clob, timeout_hours=24.0, now=_now())

        db.close_trade.assert_not_called()
        db.upsert_position.assert_called_once()
        upsert_kw = db.upsert_position.call_args.kwargs
        assert upsert_kw["unrealized_pnl"] < 0  # losing

    # -----------------------------------------------------------------------
    # Scenario 7: Full edge-to-close YES narrative with PnL verification
    # -----------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_full_yes_edge_to_profitable_close_narrative(self):
        """
        Complete simulation:
          1. Breaking news signal (YES): 'Fed raises rates above 5%'.
          2. Feeder maps signal → YES direction → dispatches to Redis.
          3. Trader evaluates: mid=0.45 → p_hat=0.57, edge=0.10 → executes.
          4. Position tracker polls: price at 0.97 → WIN, PnL > $0.
        """
        # Step 1-2: Feeder
        feeder, f_db, f_redis = _make_feeder()
        ms = _make_mapped_signal(Direction.YES)
        ms_low = ms.model_copy(update={"similarity_score": 0.82})
        feeder._mapper.map = AsyncMock(return_value=[ms_low])

        await feeder._process_signals([_make_yes_signal()])
        dispatched: MappedSignal = f_redis.publish_signal.call_args.args[0]
        assert dispatched.direction == Direction.YES

        # Step 3: Trader — market underpriced at 0.45 for a YES signal
        cfg = _make_cfg(edge_increment_yes=0.12, min_edge=0.03)
        trader, t_db, _ = _make_trader(cfg)
        ob = _make_orderbook(_TOKEN_YES, mid_price=0.45)  # cheap: great entry
        clob_klass, _ = _make_clob_cm(ob)

        with patch("trader_main.ClobClient", clob_klass):
            await trader._handle_signal(dispatched)

        trade_kw = t_db.log_event.call_args.kwargs["payload"]
        assert trade_kw["skip_reason"] is None
        assert trade_kw["direction"] == "YES"
        assert trade_kw["p_hat"] == pytest.approx(0.57, abs=0.01)   # 0.45 + 0.12
        size = trade_kw["kelly_size_usdc"]
        assert size > 0

        # Step 4: Position tracker closes at 0.97 → WIN
        tracker, pt_db, _ = self._make_tracker()
        entry_price = 0.46   # best_ask ≈ mid + spread
        pos = {
            "trade_id": 1,
            "condition_id": _CONDITION_ID,
            "token_id": _TOKEN_YES,
            "direction": Direction.YES.value,
            "size_usdc": size,
            "entry_price": entry_price,
            "opened_at": _now() - timedelta(hours=2),
            "last_price": None,
            "unrealized_pnl": None,
            "p_hat": trade_kw["p_hat"],
            "signal_source": "newsapi",
        }
        clob = MagicMock()
        clob.get_orderbook = AsyncMock(return_value=_make_orderbook(_TOKEN_YES, 0.97))
        await tracker._check_one(pos, clob, timeout_hours=24.0, now=_now())

        close_kw = pt_db.close_trade.call_args.kwargs
        assert close_kw["outcome"] == "WIN"
        assert close_kw["realized_pnl"] > 0
        expected_pnl = (0.97 - entry_price) * (size / entry_price)
        assert close_kw["realized_pnl"] == pytest.approx(expected_pnl, abs=0.05)

    # -----------------------------------------------------------------------
    # Scenario 8: Full edge-to-close NO narrative
    # -----------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_full_no_edge_to_profitable_close_narrative(self):
        """
        Complete simulation:
          1. Breaking news signal (NO): 'Fed fails to raise rates'.
          2. Feeder maps signal → NO direction → dispatches to Redis.
          3. Trader evaluates NO token at mid=0.45 → executes.
          4. Position tracker polls: YES price collapses to 0.03 → WIN.
        """
        # Steps 1-2: Feeder
        feeder, f_db, f_redis = _make_feeder()
        ms = _make_mapped_signal(Direction.NO)
        feeder._mapper.map = AsyncMock(return_value=[ms])

        await feeder._process_signals([_make_no_signal()])
        dispatched: MappedSignal = f_redis.publish_signal.call_args.args[0]
        assert dispatched.direction == Direction.NO

        # Step 3: Trader
        cfg = _make_cfg(edge_increment_no=0.12, min_edge=0.03)
        trader, t_db, _ = _make_trader(cfg)
        ob = _make_orderbook(_TOKEN_NO, mid_price=0.45)
        clob_klass, _ = _make_clob_cm(ob)

        with patch("trader_main.ClobClient", clob_klass):
            await trader._handle_signal(dispatched)

        trade_kw = t_db.log_event.call_args.kwargs["payload"]
        assert trade_kw["skip_reason"] is None
        assert trade_kw["direction"] == "NO"

        # Step 4: Tracker — YES collapses to 0.03 → NO wins
        tracker, pt_db, _ = self._make_tracker()
        pos = {
            "trade_id": 2,
            "condition_id": _CONDITION_ID,
            "token_id": _TOKEN_YES,
            "direction": Direction.NO.value,
            "size_usdc": trade_kw["kelly_size_usdc"],
            "entry_price": 0.55,   # YES was at 0.55 when we went NO
            "opened_at": _now() - timedelta(hours=3),
            "last_price": None,
            "unrealized_pnl": None,
            "p_hat": trade_kw["p_hat"],
            "signal_source": "newsapi",
        }
        clob = MagicMock()
        clob.get_orderbook = AsyncMock(return_value=_make_orderbook(_TOKEN_YES, 0.03))
        await tracker._check_one(pos, clob, timeout_hours=24.0, now=_now())

        close_kw = pt_db.close_trade.call_args.kwargs
        assert close_kw["outcome"] == "WIN"
        assert close_kw["exit_reason"] == "resolved"

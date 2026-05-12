"""
End-to-end pipeline smoke tests for Seulgi.

Simulates the full flow:
  headline detected → embed → match market → AI classify → paper trade

All external I/O is mocked (OpenRouter API, CLOB price, Gamma API, Playwright).

Edge model used in these tests (edge_increment=0.12):
    conf=0.88 → edge = 0.12 × (2×0.88−1) = 0.12×0.76 = 0.0912
    conf=0.90 → edge = 0.12 × 0.80 = 0.096
    conf=0.85 → edge = 0.12 × 0.70 = 0.084
"""

import asyncio
import sys
import os
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ai import Direction, ClassifyResult
from markets import Market, MarketIndex, Embedder, MarketMatch
from trader import PaperTrader
from watcher import RawHeadline


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_headline(text="Fed slashes rates by 50bps in emergency meeting today"):
    return RawHeadline(
        site_url="https://apnews.com",
        headline=text,
        article_url="https://apnews.com/article/fed-cuts",
        detected_at=datetime.now(timezone.utc),
    )


def _make_market_index(questions: list[str]):
    db = MagicMock()
    embedder = MagicMock()
    cfg = MagicMock()
    cfg.markets.similarity_threshold = 0.30
    cfg.markets.top_k = 5
    cfg.markets.refresh_hours = 4.0

    idx = MarketIndex(db=db, embedder=embedder, cfg=cfg)
    idx._markets = [
        Market(f"cid{i}", q, f"ty{i}", f"tn{i}")
        for i, q in enumerate(questions)
    ]
    vec = np.ones(384, dtype=np.float32)
    vec /= np.linalg.norm(vec)
    idx._matrix = np.tile(vec, (len(questions), 1))
    return idx


def _make_trader(balance: float = 1000.0):
    db = MagicMock()
    db.insert_signal = AsyncMock(return_value=1)
    db.insert_trade = AsyncMock(return_value=10)
    db.open_position = AsyncMock()
    db.close_position = AsyncMock()
    db.close_trade = AsyncMock()
    db.count_open_positions = AsyncMock(return_value=0)
    db.has_open_position = AsyncMock(return_value=False)
    db.count_unique_open_markets = AsyncMock(return_value=0)
    db.get_cold_losing_positions = AsyncMock(return_value=[])
    db.update_position_price = AsyncMock()

    cfg = MagicMock()
    cfg.balance_usdc = balance
    cfg.max_position_usdc = 50.0
    cfg.min_trade_usdc = 5.0
    cfg.min_edge = 0.04
    cfg.edge_increment = 0.12
    cfg.position_timeout_hours = 24.0
    cfg.max_open_positions = 20
    cfg.max_unique_markets = 15
    cfg.cold_position_hours = 12.0
    cfg.infinite_balance = False

    ai_cfg = MagicMock()
    ai_cfg.min_confidence = 0.45

    trader = PaperTrader(db=db, classifier=MagicMock(), cfg=cfg, ai_cfg=ai_cfg)
    trader._balance = balance

    mock_session = MagicMock()
    mock_session.closed = False
    trader._session = mock_session

    return trader, db


def _query_vec():
    v = np.ones(384, dtype=np.float32)
    v /= np.linalg.norm(v)
    return v


# ---------------------------------------------------------------------------
# YES edge: bullish headline → YES trade executed → WIN on close
# ---------------------------------------------------------------------------


class TestYesEdgePipeline:
    @pytest.mark.asyncio
    async def test_yes_trade_full_flow(self):
        """Fed cuts rates → YES on 'Will Fed cut?' → trade executed → balance decreases."""
        idx = _make_market_index(["Will the Fed cut rates before end of 2025?"])
        matches = idx.find_matches(_query_vec())
        assert len(matches) == 1

        trader, db = _make_trader(balance=500.0)
        trader._classifier.classify_batch = AsyncMock(
            return_value=[ClassifyResult(Direction.YES, 0.88, "bullish")]
        )

        # edge = 0.12 * (2*0.88-1) = 0.0912 > min_edge(0.04) → execute
        with patch("trader._fetch_mid_price", AsyncMock(return_value=0.45)):
            decisions = await trader.process(_make_headline(), matches)

        assert len(decisions) == 1
        assert decisions[0].executed
        assert decisions[0].direction == Direction.YES
        assert decisions[0].size_usdc == 50.0
        assert trader.balance == 450.0
        db.open_position.assert_called_once()

    @pytest.mark.asyncio
    async def test_yes_quick_win(self):
        """After YES trade, price rises to 0.97 → WIN, balance restored with profit."""
        idx = _make_market_index(["Will inflation fall below 2% in Q1 2025?"])
        matches = idx.find_matches(_query_vec())

        trader, db = _make_trader(balance=1000.0)
        trader._classifier.classify_batch = AsyncMock(
            return_value=[ClassifyResult(Direction.YES, 0.90, "very bullish")]
        )

        entry_price = 0.45
        with patch("trader._fetch_mid_price", AsyncMock(return_value=entry_price)):
            decisions = await trader.process(_make_headline("Inflation crashes to 1.5%"), matches)

        assert decisions[0].executed
        balance_post_trade = trader.balance  # 950.0

        # Simulate monitor: price hits 0.97 → WIN
        pos = {
            "trade_id": 10,
            "condition_id": "cid0",
            "token_id": "ty0",
            "direction": "YES",
            "size_usdc": 50.0,
            "entry_price": entry_price,
            "opened_at": datetime.now(timezone.utc).isoformat(),
            "last_price": 0.97,
            "p_hat": 0.50,
        }
        db.get_open_positions = AsyncMock(return_value=[pos])

        with patch("trader._fetch_mid_price", AsyncMock(return_value=0.97)):
            await trader.monitor_positions()

        db.close_position.assert_called_once_with(10)
        db.close_trade.assert_called_once()

        shares = 50.0 / entry_price
        expected_pnl = (0.97 - entry_price) * shares
        assert trader.balance == pytest.approx(balance_post_trade + 50.0 + expected_pnl, abs=0.01)


# ---------------------------------------------------------------------------
# NO edge: bearish headline → NO trade → WIN on close
# ---------------------------------------------------------------------------


class TestNoEdgePipeline:
    @pytest.mark.asyncio
    async def test_no_trade_full_flow(self):
        """Inflation surge → NO on 'Will inflation fall?' → NO token bought."""
        idx = _make_market_index(["Will US inflation fall below 3% by end of 2025?"])
        matches = idx.find_matches(_query_vec())

        trader, db = _make_trader(balance=500.0)
        trader._classifier.classify_batch = AsyncMock(
            return_value=[ClassifyResult(Direction.NO, 0.80, "bearish for YES")]
        )

        # NO token: edge = 0.12*(2*0.80-1) = 0.072 > 0.04 → execute
        with patch("trader._fetch_mid_price", AsyncMock(return_value=0.40)):
            decisions = await trader.process(
                _make_headline("Inflation surges to 8% — worst reading in 40 years"), matches
            )

        assert decisions[0].executed
        assert decisions[0].direction == Direction.NO

    @pytest.mark.asyncio
    async def test_no_quick_win(self):
        """NO position: NO token price rises to 0.96 → WIN."""
        idx = _make_market_index(["Will US-China trade deal be signed in 2025?"])
        matches = idx.find_matches(_query_vec())

        trader, db = _make_trader(balance=1000.0)
        trader._classifier.classify_batch = AsyncMock(
            return_value=[ClassifyResult(Direction.NO, 0.85, "trade deal unlikely")]
        )

        entry_price = 0.38
        with patch("trader._fetch_mid_price", AsyncMock(return_value=entry_price)):
            decisions = await trader.process(
                _make_headline("Trump imposes 200% tariffs — trade war escalates"), matches
            )

        assert decisions[0].executed
        balance_post = trader.balance

        pos = {
            "trade_id": 10,
            "condition_id": "cid0",
            "token_id": "tn0",
            "direction": "NO",
            "size_usdc": 50.0,
            "entry_price": entry_price,
            "opened_at": datetime.now(timezone.utc).isoformat(),
            "last_price": 0.96,
            "p_hat": 0.50,
        }
        db.get_open_positions = AsyncMock(return_value=[pos])

        with patch("trader._fetch_mid_price", AsyncMock(return_value=0.96)):
            await trader.monitor_positions()

        db.close_position.assert_called_once()
        assert trader.balance > balance_post


# ---------------------------------------------------------------------------
# Hold-then-sell (timeout)
# ---------------------------------------------------------------------------


class TestTimeoutPipeline:
    @pytest.mark.asyncio
    async def test_timeout_exit(self):
        """Position held beyond timeout → closed at current price, PnL recorded."""
        trader, db = _make_trader(balance=950.0)  # 50 already committed
        trader._cfg.position_timeout_hours = 1.0

        opened_at = datetime.now(timezone.utc) - timedelta(hours=2)
        pos = {
            "trade_id": 7,
            "condition_id": "cid_timeout",
            "token_id": "tok_yes",
            "direction": "YES",
            "size_usdc": 50.0,
            "entry_price": 0.50,
            "opened_at": opened_at.isoformat(),
            "last_price": 0.52,
            "p_hat": 0.584,
        }
        db.get_open_positions = AsyncMock(return_value=[pos])

        exit_price = 0.52
        with patch("trader._fetch_mid_price", AsyncMock(return_value=exit_price)):
            await trader.monitor_positions()

        db.close_position.assert_called_once_with(7)
        db.close_trade.assert_called_once()

        shares = 50.0 / 0.50
        expected_pnl = (exit_price - 0.50) * shares
        assert trader.balance == pytest.approx(950.0 + 50.0 + expected_pnl, abs=0.01)


# ---------------------------------------------------------------------------
# Guard: duplicate position on same market
# ---------------------------------------------------------------------------


class TestDuplicatePositionGuard:
    @pytest.mark.asyncio
    async def test_second_trade_same_market_blocked(self):
        """Opening a second position on the same condition_id should be skipped."""
        idx = _make_market_index(["Will the Fed cut rates?"])
        matches = idx.find_matches(_query_vec())

        trader, db = _make_trader(balance=1000.0)
        trader._classifier.classify_batch = AsyncMock(
            return_value=[ClassifyResult(Direction.YES, 0.88, "bullish")]
        )

        with patch("trader._fetch_mid_price", AsyncMock(return_value=0.45)):
            # First trade executes
            await trader.process(_make_headline(), matches)

        # Simulate the position is now open
        db.has_open_position = AsyncMock(return_value=True)

        with patch("trader._fetch_mid_price", AsyncMock(return_value=0.45)):
            decisions = await trader.process(_make_headline(), matches)

        assert not decisions[0].executed
        assert decisions[0].skip_reason == "duplicate_position"


# ---------------------------------------------------------------------------
# Guard: max open positions
# ---------------------------------------------------------------------------


class TestMaxPositionsGuard:
    @pytest.mark.asyncio
    async def test_trade_blocked_at_max(self):
        trader, db = _make_trader()
        trader._cfg.max_open_positions = 3
        db.count_open_positions = AsyncMock(return_value=3)

        idx = _make_market_index(["Will X happen?"])
        matches = idx.find_matches(_query_vec())
        trader._classifier.classify_batch = AsyncMock(
            return_value=[ClassifyResult(Direction.YES, 0.88, "bullish")]
        )

        with patch("trader._fetch_mid_price", AsyncMock(return_value=0.45)):
            decisions = await trader.process(_make_headline(), matches)

        assert not decisions[0].executed
        assert decisions[0].skip_reason.startswith("max_positions")


# ---------------------------------------------------------------------------
# No match → no trades
# ---------------------------------------------------------------------------


class TestNoMatchPipeline:
    @pytest.mark.asyncio
    async def test_no_matching_markets(self):
        """If no markets pass threshold, process() returns empty list."""
        idx = _make_market_index(["Will the Fed cut rates?"])
        idx._cfg.markets.similarity_threshold = 0.99  # nothing passes

        # Orthogonal query so cosine sim ≈ 0
        q = np.zeros(384, dtype=np.float32)
        q[0] = 1.0
        idx._matrix = np.zeros((1, 384), dtype=np.float32)
        idx._matrix[0, 1] = 1.0  # orthogonal to q

        matches = idx.find_matches(q)
        assert len(matches) == 0

        trader, db = _make_trader()
        decisions = await trader.process(_make_headline("Local dog show held in Tulsa"), matches)
        assert decisions == []
        db.insert_signal.assert_not_called()

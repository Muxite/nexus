"""Unit tests for clob_client.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from clob_client import ClobClient, _best_price, _depth_at_price, _parse_orderbook


# ---------------------------------------------------------------------------
# _best_price
# ---------------------------------------------------------------------------


class TestBestPrice:
    def test_returns_none_for_empty(self):
        assert _best_price([], highest=True) is None
        assert _best_price([], highest=False) is None

    def test_highest_returns_max(self):
        levels = [{"price": "0.45"}, {"price": "0.50"}, {"price": "0.40"}]
        assert _best_price(levels, highest=True) == pytest.approx(0.50)

    def test_lowest_returns_min(self):
        levels = [{"price": "0.45"}, {"price": "0.50"}, {"price": "0.40"}]
        assert _best_price(levels, highest=False) == pytest.approx(0.40)

    def test_skips_malformed_price(self):
        levels = [{"price": "bad"}, {"price": "0.55"}, {}]
        assert _best_price(levels, highest=True) == pytest.approx(0.55)

    def test_skips_missing_price_key(self):
        levels = [{"size": "100"}, {"price": "0.30"}]
        assert _best_price(levels, highest=False) == pytest.approx(0.30)

    def test_single_level(self):
        levels = [{"price": "0.72"}]
        assert _best_price(levels, highest=True) == pytest.approx(0.72)
        assert _best_price(levels, highest=False) == pytest.approx(0.72)


# ---------------------------------------------------------------------------
# _depth_at_price
# ---------------------------------------------------------------------------


class TestDepthAtPrice:
    def test_sums_matching_levels(self):
        levels = [
            {"price": "0.50", "size": "100"},
            {"price": "0.50", "size": "50"},
            {"price": "0.60", "size": "200"},
        ]
        # 0.50 * 100 + 0.50 * 50 = 75
        assert _depth_at_price(levels, 0.50) == pytest.approx(75.0)

    def test_returns_zero_for_no_match(self):
        levels = [{"price": "0.60", "size": "100"}]
        assert _depth_at_price(levels, 0.50) == pytest.approx(0.0)

    def test_empty_levels_returns_zero(self):
        assert _depth_at_price([], 0.50) == pytest.approx(0.0)

    def test_skips_malformed_entries(self):
        levels = [
            {"price": "bad", "size": "100"},
            {"price": "0.50", "size": "bad"},
        ]
        assert _depth_at_price(levels, 0.50) == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# _parse_orderbook
# ---------------------------------------------------------------------------


class TestParseOrderbook:
    def test_parses_valid_book(self):
        data = {
            "bids": [{"price": "0.45", "size": "200"}],
            "asks": [{"price": "0.55", "size": "150"}],
        }
        ob = _parse_orderbook("tok1", data)
        assert ob is not None
        assert ob.best_bid == pytest.approx(0.45)
        assert ob.best_ask == pytest.approx(0.55)
        assert ob.mid_price == pytest.approx(0.50)
        assert ob.token_id == "tok1"

    def test_returns_none_for_empty_book(self):
        assert _parse_orderbook("tok1", {"bids": [], "asks": []}) is None

    def test_returns_none_for_missing_data(self):
        assert _parse_orderbook("tok1", {}) is None

    def test_missing_bids_uses_ask_for_both_sides(self):
        data = {
            "bids": [],
            "asks": [{"price": "0.60", "size": "100"}],
        }
        ob = _parse_orderbook("tok1", data)
        assert ob is not None
        assert ob.best_bid == pytest.approx(0.60)
        assert ob.best_ask == pytest.approx(0.60)
        assert ob.mid_price == pytest.approx(0.60)

    def test_missing_asks_uses_bid_for_both_sides(self):
        data = {
            "bids": [{"price": "0.40", "size": "100"}],
            "asks": [],
        }
        ob = _parse_orderbook("tok1", data)
        assert ob is not None
        assert ob.best_bid == pytest.approx(0.40)
        assert ob.best_ask == pytest.approx(0.40)

    def test_depth_computed_at_best_level(self):
        data = {
            "bids": [{"price": "0.45", "size": "200"}],
            "asks": [{"price": "0.55", "size": "100"}],
        }
        ob = _parse_orderbook("tok1", data)
        assert ob.bid_depth_usdc == pytest.approx(0.45 * 200)
        assert ob.ask_depth_usdc == pytest.approx(0.55 * 100)

    def test_spread_property(self):
        data = {
            "bids": [{"price": "0.48", "size": "100"}],
            "asks": [{"price": "0.52", "size": "100"}],
        }
        ob = _parse_orderbook("tok1", data)
        assert ob.spread == pytest.approx(0.04, abs=1e-6)


# ---------------------------------------------------------------------------
# ClobClient HTTP integration (mocked session)
# ---------------------------------------------------------------------------


def _make_mock_session(json_data: dict) -> MagicMock:
    mock_resp = AsyncMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = AsyncMock(return_value=json_data)
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=False)

    session = MagicMock()
    session.get = MagicMock(return_value=mock_resp)
    return session


class TestClobClientGetOrderbook:
    @pytest.mark.asyncio
    async def test_returns_orderbook_on_success(self):
        data = {
            "bids": [{"price": "0.45", "size": "200"}],
            "asks": [{"price": "0.55", "size": "150"}],
        }
        client = ClobClient(session=_make_mock_session(data))
        ob = await client.get_orderbook("tok1")

        assert ob is not None
        assert ob.mid_price == pytest.approx(0.50)

    @pytest.mark.asyncio
    async def test_returns_none_on_http_error(self):
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock(
            side_effect=aiohttp.ClientResponseError(
                request_info=MagicMock(), history=(), status=429
            )
        )
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        session = MagicMock()
        session.get = MagicMock(return_value=mock_resp)

        client = ClobClient(session=session)
        assert await client.get_orderbook("tok1") is None

    @pytest.mark.asyncio
    async def test_returns_none_on_connection_error(self):
        mock_resp = AsyncMock()
        mock_resp.__aenter__ = AsyncMock(
            side_effect=aiohttp.ClientConnectionError("timeout")
        )
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        session = MagicMock()
        session.get = MagicMock(return_value=mock_resp)

        client = ClobClient(session=session)
        assert await client.get_orderbook("tok1") is None

    @pytest.mark.asyncio
    async def test_get_mid_price_returns_float(self):
        data = {
            "bids": [{"price": "0.45", "size": "200"}],
            "asks": [{"price": "0.55", "size": "150"}],
        }
        client = ClobClient(session=_make_mock_session(data))
        mid = await client.get_mid_price("tok1")
        assert mid == pytest.approx(0.50)

    @pytest.mark.asyncio
    async def test_get_mid_price_returns_none_on_failure(self):
        client = ClobClient(session=_make_mock_session({"bids": [], "asks": []}))
        assert await client.get_mid_price("tok1") is None

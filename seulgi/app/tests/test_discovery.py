"""Tests for seulgi/app/discovery.py — Kalshi + Predictit adapters,
quality scoring, active discovery loop, schema migration."""

from __future__ import annotations

import os
import sys
import asyncio
import tempfile
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from discovery import (
    DiscoveredMarket,
    KalshiAdapter,
    PredictitAdapter,
    quality_score,
    ActiveDiscoveryLoop,
    _days_until,
)
import db as db_module


# ---------------------------------------------------------------------------
# quality_score()
# ---------------------------------------------------------------------------


class TestQualityScore:
    def test_null_fields_score_zero(self):
        m = DiscoveredMarket(source="kalshi", cond_id="X", question="Q?")
        assert quality_score(m) == 0.0

    def test_partial_score_with_volume_only(self):
        m = DiscoveredMarket(
            source="kalshi", cond_id="X", question="Q?",
            volume_24h_usdc=50_000.0,
        )
        # 0.3 weight × volume (capped at 1.0) = 0.3
        assert quality_score(m) == pytest.approx(0.3, abs=0.01)

    def test_full_score_with_all_fields(self):
        m = DiscoveredMarket(
            source="kalshi", cond_id="X", question="Q?",
            liquidity_usdc=20_000.0,    # capped → 1.0  × 0.4 = 0.40
            volume_24h_usdc=100_000.0,  # capped → 1.0  × 0.3 = 0.30
            days_to_resolution=5,       # within 1..7 → 1.0  × 0.2 = 0.20
            yes_price=0.55, no_price=0.45,  # spread=0 → 1.0  × 0.1 = 0.10
        )
        # Sum = 1.0
        assert quality_score(m) == pytest.approx(1.0, abs=0.001)

    def test_score_in_unit_interval(self):
        # All clamped to [0, 1]
        m = DiscoveredMarket(
            source="kalshi", cond_id="X", question="Q?",
            liquidity_usdc=1e9, volume_24h_usdc=1e9,
            days_to_resolution=10, yes_price=0.5, no_price=0.5,
        )
        s = quality_score(m)
        assert 0.0 <= s <= 1.0

    def test_far_dated_market_has_lower_score(self):
        near = DiscoveredMarket(
            source="kalshi", cond_id="N", question="Q?",
            liquidity_usdc=1000.0, days_to_resolution=3,
        )
        far = DiscoveredMarket(
            source="kalshi", cond_id="F", question="Q?",
            liquidity_usdc=1000.0, days_to_resolution=365,
        )
        assert quality_score(near) > quality_score(far)


# ---------------------------------------------------------------------------
# _days_until
# ---------------------------------------------------------------------------


class TestDaysUntil:
    def test_none_input(self):
        assert _days_until(None) is None
        assert _days_until("") is None

    def test_garbage_input(self):
        assert _days_until("not-a-date") is None

    def test_future_date(self):
        target = datetime.now(timezone.utc) + timedelta(days=10)
        result = _days_until(target.isoformat())
        assert 9 <= result <= 10

    def test_zulu_suffix(self):
        target = datetime.now(timezone.utc) + timedelta(days=3)
        zulu = target.isoformat().replace("+00:00", "Z")
        result = _days_until(zulu)
        assert 2 <= result <= 3


# ---------------------------------------------------------------------------
# KalshiAdapter
# ---------------------------------------------------------------------------


def _make_session_returning(payload):
    """Build a mock aiohttp session whose .get(...).__aenter__ returns a
    response whose .json() resolves to `payload` (or each entry of the list
    in sequence)."""
    payloads = payload if isinstance(payload, list) else [payload]
    call = {"i": 0}

    class _Resp:
        def raise_for_status(self):
            return None

        async def json(self):
            i = call["i"]
            call["i"] += 1
            return payloads[min(i, len(payloads) - 1)]

    class _Ctx:
        async def __aenter__(self):
            return _Resp()

        async def __aexit__(self, *args):
            return None

    session = MagicMock()
    session.get = MagicMock(return_value=_Ctx())
    return session


class TestKalshiAdapter:
    def test_returns_normalized_markets(self):
        payload = {
            "markets": [
                {
                    "ticker": "INXD-24DEC31-T100",
                    "title": "Will the S&P 500 close above 6000 by Dec 31, 2024?",
                    "yes_bid": 55,    # cents
                    "no_bid": 43,
                    "liquidity": 12500.0,
                    "volume_24h": 80000.0,
                    "close_time": (
                        datetime.now(timezone.utc) + timedelta(days=10)
                    ).isoformat(),
                    "event_ticker": "INXD",
                    "category": "Financial",
                },
            ],
            "cursor": None,
        }
        session = _make_session_returning(payload)
        ad = KalshiAdapter()
        results = asyncio.get_event_loop().run_until_complete(
            ad.fetch_open_markets(session, limit=5)
        )
        assert len(results) == 1
        m = results[0]
        assert m.source == "kalshi"
        assert m.cond_id == "INXD-24DEC31-T100"
        assert "S&P" in m.question
        assert m.yes_price == pytest.approx(0.55)
        assert m.no_price == pytest.approx(0.43)
        assert m.liquidity_usdc == 12500.0
        assert m.volume_24h_usdc == 80000.0
        assert m.days_to_resolution is not None
        assert m.quality_score > 0.5

    def test_skips_markets_missing_ticker(self):
        payload = {
            "markets": [
                {"title": "no ticker"},
                {"ticker": "OK", "title": "good"},
            ],
            "cursor": None,
        }
        session = _make_session_returning(payload)
        ad = KalshiAdapter()
        out = asyncio.get_event_loop().run_until_complete(
            ad.fetch_open_markets(session)
        )
        assert len(out) == 1 and out[0].cond_id == "OK"

    def test_http_error_returns_empty(self):
        class _Resp:
            def raise_for_status(self):
                raise RuntimeError("boom")

            async def json(self):
                return {}

        class _Ctx:
            async def __aenter__(self):
                return _Resp()

            async def __aexit__(self, *a):
                return None

        session = MagicMock()
        session.get = MagicMock(return_value=_Ctx())
        out = asyncio.get_event_loop().run_until_complete(
            KalshiAdapter().fetch_open_markets(session)
        )
        assert out == []


# ---------------------------------------------------------------------------
# PredictitAdapter
# ---------------------------------------------------------------------------


class TestPredictitAdapter:
    def test_returns_normalized_contracts(self):
        end_iso = (datetime.now(timezone.utc) + timedelta(days=5)).isoformat()
        payload = {
            "markets": [
                {
                    "id": 7000,
                    "name": "2024 Presidential Election Winner",
                    "dateEnd": end_iso,
                    "contracts": [
                        {
                            "id": 24001,
                            "name": "Candidate A",
                            "bestBuyYesCost": 0.43,
                            "bestBuyNoCost": 0.59,
                        },
                        {
                            "id": 24002,
                            "name": "Candidate B",
                            "bestBuyYesCost": 0.21,
                            "bestBuyNoCost": 0.81,
                        },
                    ],
                },
            ],
        }
        session = _make_session_returning(payload)
        out = asyncio.get_event_loop().run_until_complete(
            PredictitAdapter().fetch_open_markets(session)
        )
        assert len(out) == 2
        assert all(m.source == "predictit" for m in out)
        assert out[0].cond_id == "24001"
        assert "Candidate A" in out[0].question
        assert out[0].yes_price == pytest.approx(0.43)
        # days_to_resolution should be computed
        assert out[0].days_to_resolution is not None


# ---------------------------------------------------------------------------
# ActiveDiscoveryLoop
# ---------------------------------------------------------------------------


class _FakeDB:
    def __init__(self):
        self.upserted: list[DiscoveredMarket] = []

    async def upsert_discovered_market(self, m: DiscoveredMarket) -> None:
        self.upserted.append(m)


class _FakeIndex:
    def __init__(self):
        self.refresh_calls = 0

    async def load_from_db(self) -> int:
        self.refresh_calls += 1
        return 0


class _FakeAdapter:
    def __init__(self, markets):
        self._markets = markets

    async def fetch_open_markets(self, session, **kwargs):
        return list(self._markets)


class TestActiveDiscoveryLoop:
    def test_dedup_across_sources(self):
        m_kalshi = DiscoveredMarket(
            source="kalshi", cond_id="A", question="Q?",
            liquidity_usdc=10_000, volume_24h_usdc=50_000,
            days_to_resolution=5, yes_price=0.5, no_price=0.5,
        )
        m_kalshi.quality_score = quality_score(m_kalshi)
        # Duplicate of m_kalshi (same source+cond_id) must be dropped.
        m_dup = DiscoveredMarket(
            source="kalshi", cond_id="A", question="Q? v2",
            liquidity_usdc=8_000, volume_24h_usdc=40_000,
            days_to_resolution=4, yes_price=0.5, no_price=0.5,
        )
        m_dup.quality_score = quality_score(m_dup)
        m_predictit = DiscoveredMarket(
            source="predictit", cond_id="B", question="Q2?",
            liquidity_usdc=5_000, volume_24h_usdc=20_000,
            days_to_resolution=3, yes_price=0.5, no_price=0.5,
        )
        m_predictit.quality_score = quality_score(m_predictit)

        db = _FakeDB()
        idx = _FakeIndex()
        loop = ActiveDiscoveryLoop(
            db=db, market_index=idx, min_quality=0.0,
            adapters=[_FakeAdapter([m_kalshi, m_dup]), _FakeAdapter([m_predictit])],
        )
        upserted = asyncio.get_event_loop().run_until_complete(
            loop.tick(session=None)
        )
        assert upserted == 2
        keys = sorted((m.source, m.cond_id) for m in db.upserted)
        assert keys == [("kalshi", "A"), ("predictit", "B")]
        # MarketIndex refresh triggered exactly once after a non-empty batch.
        assert idx.refresh_calls == 1

    def test_quality_floor_filters_low_score(self):
        good = DiscoveredMarket(
            source="kalshi", cond_id="G", question="good",
            liquidity_usdc=10_000, volume_24h_usdc=50_000,
            days_to_resolution=5, yes_price=0.5, no_price=0.5,
        )
        good.quality_score = quality_score(good)
        bad = DiscoveredMarket(source="kalshi", cond_id="B", question="bad")
        bad.quality_score = quality_score(bad)
        assert good.quality_score > 0.3 > bad.quality_score

        db = _FakeDB()
        loop = ActiveDiscoveryLoop(
            db=db, market_index=None, min_quality=0.3,
            adapters=[_FakeAdapter([good, bad])],
        )
        upserted = asyncio.get_event_loop().run_until_complete(
            loop.tick(session=None)
        )
        assert upserted == 1
        assert db.upserted[0].cond_id == "G"


# ---------------------------------------------------------------------------
# Schema migration + upsert
# ---------------------------------------------------------------------------


class TestSchemaMigration:
    def test_source_column_idempotent_and_present(self, tmp_path):
        path = str(tmp_path / "schema.db")

        async def go():
            d = db_module.SeulgiDB(path)
            await d.connect()
            # Second connect must be a no-op (idempotent migration).
            await d.close()
            d2 = db_module.SeulgiDB(path)
            await d2.connect()
            async with d2._conn.execute(
                "PRAGMA table_info(markets)"
            ) as cur:
                cols = [r[1] for r in await cur.fetchall()]
            await d2.close()
            return cols

        cols = asyncio.get_event_loop().run_until_complete(go())
        assert "source" in cols

    def test_upsert_discovered_market_writes_source(self, tmp_path):
        path = str(tmp_path / "upsert.db")

        m = DiscoveredMarket(
            source="kalshi", cond_id="TICK-1", question="Will X?",
            volume_24h_usdc=12_345.0,
        )
        m.quality_score = quality_score(m)

        async def go():
            d = db_module.SeulgiDB(path)
            await d.connect()
            await d.upsert_discovered_market(m)
            async with d._conn.execute(
                "SELECT condition_id, question, source, volume_usdc "
                "FROM markets WHERE source='kalshi'"
            ) as cur:
                row = await cur.fetchone()
            await d.close()
            return row

        row = asyncio.get_event_loop().run_until_complete(go())
        assert row is not None
        assert row[0] == "kalshi:TICK-1"
        assert row[1] == "Will X?"
        assert row[2] == "kalshi"
        assert row[3] == pytest.approx(12_345.0)

"""Unit tests for scanner/app/main.py — Scanner main loop."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

from irene_shared.models import MarketEventType
from main import Scanner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _make_cfg(poll_interval: int = 120, jitter: float = 0.0):
    cfg = MagicMock()
    cfg.polymarket.gamma_url = "https://gamma.example.com"
    cfg.scanner.poll_interval_seconds = poll_interval
    cfg.scanner.jitter_fraction = jitter
    cfg.scanner.gamma_page_size = 100
    cfg.markets = MagicMock()
    return cfg


def _make_config_manager(cfg=None):
    m = MagicMock()
    m.get.return_value = cfg or _make_cfg()
    return m


def _make_db(expired_count: int = 0):
    db = MagicMock()
    db.upsert_market = AsyncMock()
    db.deactivate_expired_markets = AsyncMock(return_value=expired_count)
    db.log_event = AsyncMock()
    return db


def _make_raw_market(suffix: str = "abc") -> dict:
    return {"conditionId": f"cond_{suffix}", "question": f"Q{suffix}"}


def _make_parsed_market(suffix: str = "abc") -> dict:
    return {
        "condition_id": f"cond_{suffix}",
        "question": f"Q{suffix}",
        "end_date": _now(),
        "token_yes": f"tok_yes_{suffix}",
        "token_no": f"tok_no_{suffix}",
    }


def _make_gamma_cm(raw_markets: list):
    """Return a (class_mock, client_mock) pair for GammaClient as async ctx manager."""
    client = MagicMock()
    client.fetch_open_markets = AsyncMock(return_value=raw_markets)

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=client)
    cm.__aexit__ = AsyncMock(return_value=False)

    klass = MagicMock(return_value=cm)
    return klass, client


def _fake_embedding() -> np.ndarray:
    return np.zeros(384, dtype=np.float32)


# ---------------------------------------------------------------------------
# _run_once — happy path
# ---------------------------------------------------------------------------


class TestScannerRunOnce:

    @pytest.mark.asyncio
    async def test_upserts_each_filtered_market(self):
        raw = [_make_raw_market("a"), _make_raw_market("b")]
        parsed = [_make_parsed_market("a"), _make_parsed_market("b")]
        emb = _fake_embedding()

        db = _make_db()
        gamma_class, _ = _make_gamma_cm(raw)

        with patch("main.GammaClient", gamma_class), \
             patch("main.parse_market", side_effect=parsed), \
             patch("main.MarketFilter") as MockFilter, \
             patch("main.Embedder") as MockEmbedder:

            MockFilter.return_value.filter.return_value = parsed
            MockEmbedder.return_value.load.return_value = None
            MockEmbedder.return_value.embed_batch.return_value = [emb, emb]

            scanner = Scanner(_make_config_manager(), db)
            await scanner._run_once()

        assert db.upsert_market.call_count == 2
        calls = db.upsert_market.call_args_list
        assert calls[0].kwargs["condition_id"] == "cond_a"
        assert calls[1].kwargs["condition_id"] == "cond_b"

    @pytest.mark.asyncio
    async def test_deactivates_expired_markets(self):
        db = _make_db(expired_count=3)
        gamma_class, _ = _make_gamma_cm([])

        with patch("main.GammaClient", gamma_class), \
             patch("main.parse_market", return_value=None), \
             patch("main.MarketFilter") as MockFilter, \
             patch("main.Embedder") as MockEmbedder:

            MockFilter.return_value.filter.return_value = []
            MockEmbedder.return_value.load.return_value = None

            scanner = Scanner(_make_config_manager(), db)
            await scanner._run_once()

        db.deactivate_expired_markets.assert_called_once()

    @pytest.mark.asyncio
    async def test_logs_scanner_refresh_event(self):
        db = _make_db()
        gamma_class, _ = _make_gamma_cm([])

        with patch("main.GammaClient", gamma_class), \
             patch("main.parse_market", return_value=None), \
             patch("main.MarketFilter") as MockFilter, \
             patch("main.Embedder") as MockEmbedder:

            MockFilter.return_value.filter.return_value = []
            MockEmbedder.return_value.load.return_value = None

            scanner = Scanner(_make_config_manager(), db)
            await scanner._run_once()

        db.log_event.assert_called_once()
        call_kwargs = db.log_event.call_args.kwargs
        assert call_kwargs["event_type"] == MarketEventType.SCANNER_REFRESH

    @pytest.mark.asyncio
    async def test_empty_market_list_no_upserts(self):
        db = _make_db()
        gamma_class, _ = _make_gamma_cm([])

        with patch("main.GammaClient", gamma_class), \
             patch("main.parse_market", return_value=None), \
             patch("main.MarketFilter") as MockFilter, \
             patch("main.Embedder") as MockEmbedder:

            MockFilter.return_value.filter.return_value = []
            MockEmbedder.return_value.load.return_value = None

            scanner = Scanner(_make_config_manager(), db)
            await scanner._run_once()

        db.upsert_market.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_unparseable_markets(self):
        """parse_market returning None for a raw entry means it is excluded from embed+upsert."""
        raw = [_make_raw_market("a"), _make_raw_market("b")]
        parsed_a = _make_parsed_market("a")
        emb = _fake_embedding()
        db = _make_db()
        gamma_class, _ = _make_gamma_cm(raw)

        # First call returns a valid market, second returns None
        with patch("main.GammaClient", gamma_class), \
             patch("main.parse_market", side_effect=[parsed_a, None]), \
             patch("main.MarketFilter") as MockFilter, \
             patch("main.Embedder") as MockEmbedder:

            MockFilter.return_value.filter.return_value = [parsed_a]
            MockEmbedder.return_value.load.return_value = None
            MockEmbedder.return_value.embed_batch.return_value = [emb]

            scanner = Scanner(_make_config_manager(), db)
            await scanner._run_once()

        assert db.upsert_market.call_count == 1
        assert db.upsert_market.call_args.kwargs["condition_id"] == "cond_a"

    @pytest.mark.asyncio
    async def test_refresh_event_payload_contains_counts(self):
        raw = [_make_raw_market("a")]
        parsed = [_make_parsed_market("a")]
        emb = _fake_embedding()
        db = _make_db(expired_count=2)
        gamma_class, _ = _make_gamma_cm(raw)

        with patch("main.GammaClient", gamma_class), \
             patch("main.parse_market", side_effect=parsed), \
             patch("main.MarketFilter") as MockFilter, \
             patch("main.Embedder") as MockEmbedder:

            MockFilter.return_value.filter.return_value = parsed
            MockEmbedder.return_value.load.return_value = None
            MockEmbedder.return_value.embed_batch.return_value = [emb]

            scanner = Scanner(_make_config_manager(), db)
            await scanner._run_once()

        payload = db.log_event.call_args.kwargs["payload"]
        assert payload["fetched"] == 1
        assert payload["filtered"] == 1
        assert payload["expired"] == 2

    @pytest.mark.asyncio
    async def test_embedding_passed_to_upsert(self):
        raw = [_make_raw_market("a")]
        parsed = [_make_parsed_market("a")]
        emb = np.ones(384, dtype=np.float32) * 0.5
        db = _make_db()
        gamma_class, _ = _make_gamma_cm(raw)

        with patch("main.GammaClient", gamma_class), \
             patch("main.parse_market", side_effect=parsed), \
             patch("main.MarketFilter") as MockFilter, \
             patch("main.Embedder") as MockEmbedder:

            MockFilter.return_value.filter.return_value = parsed
            MockEmbedder.return_value.load.return_value = None
            MockEmbedder.return_value.embed_batch.return_value = [emb]

            scanner = Scanner(_make_config_manager(), db)
            await scanner._run_once()

        upsert_emb = db.upsert_market.call_args.kwargs["embedding"]
        assert np.allclose(upsert_emb, emb)

    @pytest.mark.asyncio
    async def test_filter_called_with_config_markets(self):
        cfg = _make_cfg()
        raw = [_make_raw_market("a")]
        parsed = [_make_parsed_market("a")]
        emb = _fake_embedding()
        db = _make_db()
        gamma_class, _ = _make_gamma_cm(raw)

        with patch("main.GammaClient", gamma_class), \
             patch("main.parse_market", side_effect=parsed), \
             patch("main.MarketFilter") as MockFilter, \
             patch("main.Embedder") as MockEmbedder:

            filter_instance = MockFilter.return_value
            filter_instance.filter.return_value = parsed
            MockEmbedder.return_value.load.return_value = None
            MockEmbedder.return_value.embed_batch.return_value = [emb]

            scanner = Scanner(_make_config_manager(cfg), db)
            await scanner._run_once()

        call_args = filter_instance.filter.call_args
        assert call_args.args[1] is cfg.markets


# ---------------------------------------------------------------------------
# _loop — control flow
# ---------------------------------------------------------------------------


class TestScannerLoop:

    @pytest.mark.asyncio
    async def test_loop_stops_immediately_when_not_running(self):
        db = _make_db()
        run_once_calls = []

        async def _fake_run_once():
            run_once_calls.append(1)

        with patch("main.Embedder"), patch("main.MarketFilter"):
            scanner = Scanner(_make_config_manager(), db)
            scanner._running = False
            scanner._run_once = _fake_run_once

            with patch("main.jittered_sleep", new_callable=AsyncMock, return_value=0.0):
                await scanner._loop()

        assert run_once_calls == []

    @pytest.mark.asyncio
    async def test_loop_calls_run_once_when_running(self):
        db = _make_db()
        call_count = 0

        async def _fake_run_once():
            nonlocal call_count
            call_count += 1

        sleep_calls = []

        async def _stop_after_first_sleep(base, *, jitter_fraction, rng):
            sleep_calls.append(base)
            # Stop the loop after the first iteration
            return 0.0

        with patch("main.Embedder"), patch("main.MarketFilter"):
            scanner = Scanner(_make_config_manager(), db)
            scanner._running = True
            scanner._run_once = _fake_run_once

            # Patch jittered_sleep to stop the loop after one iteration
            original_sleep = None

            async def _patched_sleep(base, *, jitter_fraction, rng):
                scanner._running = False
                return 0.0

            with patch("main.jittered_sleep", side_effect=_patched_sleep):
                await scanner._loop()

        assert call_count == 1

    @pytest.mark.asyncio
    async def test_loop_continues_after_run_once_exception(self):
        db = _make_db()
        call_count = 0

        async def _raising_run_once():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("boom")

        async def _patched_sleep(base, *, jitter_fraction, rng):
            if call_count >= 2:
                # Stop after 2 calls
                scanner._running = False
            return 0.0

        with patch("main.Embedder"), patch("main.MarketFilter"):
            scanner = Scanner(_make_config_manager(), db)
            scanner._running = True
            scanner._run_once = _raising_run_once

            with patch("main.jittered_sleep", side_effect=_patched_sleep):
                await scanner._loop()

        assert call_count == 2

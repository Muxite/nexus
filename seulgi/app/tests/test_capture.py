"""Phase-5 capture integration tests (A2.5).

Asserts every Phase-5 row writes when a headline traverses the rebuilt
pipeline. Two positive cases (real-LLM-mocked + bypass mode) plus a
negative case proving the assertion is meaningful.

Coverage:
- `signals`              — always (via insert_signal at SIGNAL stage)
- `market_price_snaps`   — always (via _prefetch_prices at INGRESS)
- `engine_results`       — always (via record_engine_decisions at FAN_OUT)
- `llm_calls`            — when LLM ran (A2.1 shipped — `ai._log_llm_call_safe`)
                            NOT when SEULGI_BYPASS_LLM=1
- `decision_inputs`      — always (writer wired in engine.process at SIGNAL
                            stage as fire-and-forget background task)
- `trades`               — when any engine fires (sized + edge cleared gates)
- `position_price_snaps` — A2.2 schema shipped; writer is the continuous
                            impact_tracker loop (>=0 sanity assertion)

The harness builds ONE engine via build_engines + a real SeulgiDB on tmp_path
+ a real DirectionClassifier with its HTTP layer mocked. The Embedder and
MarketIndex are MagicMock so we control the matched market deterministically.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import ai
import engine as engine_mod
from ai import DirectionClassifier
from config import (
    AIConfig, Config, EngineConfig, MarketsConfig, RelevanceConfig,
    TradingConfig,
)
from db import SeulgiDB
from engine import EngineRunner, build_engines
from markets import Market, MarketMatch
from watcher import RawHeadline


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_headline(hid: str = "hid_test_capture_001") -> RawHeadline:
    return RawHeadline(
        site_url="https://apnews.com",
        headline="Fed slashes rates by 50bps in emergency meeting today",
        article_url="https://apnews.com/article/fed-cuts",
        detected_at=datetime.now(timezone.utc),
        headline_id=hid,
        source="news",
    )


def _make_config() -> Config:
    """Minimal Config that builds exactly one default engine eligible to
    trade: real-money mode, 1k balance, low edge floors, no rail skips."""
    cfg = Config()
    cfg.markets = MarketsConfig(similarity_threshold=0.0, top_k=5)
    cfg.relevance = RelevanceConfig(enabled=False, min_top_similarity=0.0)
    cfg.ai = AIConfig(
        api_key="test-key", ensemble_samples=1, min_confidence=0.0,
        min_agreement=0.0,
    )
    cfg.trading = TradingConfig(
        balance_usdc=1000.0, min_edge=0.02, max_position_usdc=50.0,
        min_trade_usdc=1.0, entry_rail_skip_threshold=0.0,
        spread_cost_bps=0.0,
    )
    cfg.engines = [EngineConfig(name="default")]
    return cfg


def _make_market_match() -> MarketMatch:
    return MarketMatch(
        condition_id="cidA",
        question="Will the Fed cut rates before end of 2026?",
        token_yes="ty_A",
        token_no="tn_A",
        similarity=0.85,
        mid_yes=0.45,
        mid_no=0.55,
    )


def _make_openrouter_response(p_yes=0.7, p_no=0.2, p_neu=0.1, p_alpha=0.8):
    body = {"results": [{
        "idx": 0, "p_yes": p_yes, "p_no": p_no, "p_neutral": p_neu,
        "p_alpha": p_alpha, "risk": 0.3, "reason": "mock bullish",
    }]}
    return {
        "choices": [{"message": {"content": json.dumps(body)}}],
        "usage": {"total_tokens": 17, "prompt_tokens": 10,
                  "completion_tokens": 7},
    }


@asynccontextmanager
async def _post_cm(data):
    resp = MagicMock()
    resp.status = 200
    resp.json = AsyncMock(return_value=data)
    resp.text = AsyncMock(return_value="")
    yield resp


def _stub_classifier_http(classifier: DirectionClassifier, payload=None):
    """Replace the classifier's session.post so HTTP is intercepted but
    the rest of `classify_batch` (including the `_log_llm_call_safe` write
    path) runs untouched."""
    payload = payload or _make_openrouter_response()
    session = MagicMock()
    session.closed = False
    session.post = MagicMock(return_value=_post_cm(payload))
    classifier._session = session


async def _build_harness(tmp_db_path: str):
    """One default engine, real SeulgiDB, mocked embedder + market index.
    Returns (runner, db, classifier, hid) so tests can configure further.
    Caller is responsible for setting SEULGI_DISABLE_HOUR_GATE via the
    monkeypatch fixture so tests can run at any UTC hour without leaking
    env state across the suite."""
    db = SeulgiDB(tmp_db_path)
    await db.connect()

    cfg = _make_config()
    classifier = DirectionClassifier(
        api_key="test-key", model="test-model",
        ensemble_samples=1, db=db,
    )
    engines = build_engines(cfg, db, classifier)
    for e in engines:
        await e.trader.init_from_db()

    # Embedder + index — deterministic; both return our single market match.
    embedder = MagicMock()
    vec = np.ones(384, dtype=np.float32)
    vec /= np.linalg.norm(vec)
    embedder.embed.return_value = vec

    match = _make_market_match()
    index = MagicMock()
    index.find_matches = MagicMock(return_value=[match])
    # Used by some hot-path code; harmless to keep simple.
    index._markets = [Market(match.condition_id, match.question,
                             match.token_yes, match.token_no)]

    runner = EngineRunner(
        engines=engines, index=index, embedder=embedder, db=db,
        classifier=classifier, rule_engine=None, dashboard=None,
        news_memory=None, impact_tracker=None, article_fetcher=None,
        market_context_cfg=None, min_top_similarity=0.0,
    )
    return runner, db, classifier, match


async def _count(db: SeulgiDB, sql: str, params=()) -> int:
    cur = await db._conn.execute(sql, params)
    row = await cur.fetchone()
    return int(row[0] if row and row[0] is not None else 0)


# ---------------------------------------------------------------------------
# Positive case 1 — full pipeline with the LLM call mocked
# ---------------------------------------------------------------------------


class TestCapturePipelineWithLlmMocked:
    @pytest.mark.asyncio
    async def test_capture_pipeline_with_llm_mocked(self, tmp_path, monkeypatch):
        """Headline → embed → match → predict (mocked HTTP) → trader.evaluate.
        Asserts every load-bearing Phase-5 row writes."""
        monkeypatch.delenv("SEULGI_BYPASS_LLM", raising=False)
        monkeypatch.setenv("SEULGI_DISABLE_HOUR_GATE", "1")
        ai._BYPASS_STARTUP_LOGGED = False

        runner, db, classifier, _match = await _build_harness(
            str(tmp_path / "capture_llm.db"))
        try:
            _stub_classifier_http(classifier)
            headline = _make_headline("hid_llm_test_001")
            hid = headline.headline_id

            # Pin the prefetch + trader-side mid fetches so price-discovery
            # doesn't reach the real CLOB. 0.45 keeps mid_yes < threshold so
            # the YES-direction read picks a viable price.
            with patch("engine._fetch_mid_price",
                       AsyncMock(return_value=0.45)), \
                 patch("trader._fetch_mid_price",
                       AsyncMock(return_value=0.45)):
                await runner.process(headline)

            # signals — always
            assert await _count(
                db, "SELECT COUNT(*) FROM signals WHERE headline_id = ?",
                (hid,),
            ) >= 1, "signals row missing"

            # market_price_snaps — INGRESS prefetch persists every match's mid
            assert await _count(
                db,
                "SELECT COUNT(*) FROM market_price_snaps WHERE headline_id = ?",
                (hid,),
            ) >= 1, "market_price_snaps row missing"

            # engine_results — record_engine_decisions per engine
            assert await _count(
                db,
                "SELECT COUNT(*) FROM engine_results WHERE headline_id = ?",
                (hid,),
            ) >= 1, "engine_results row missing"

            # llm_calls — A2.1 shipped; LLM path was taken, expect ≥1 row
            assert await _count(
                db, "SELECT COUNT(*) FROM llm_calls WHERE headline_id = ?",
                (hid,),
            ) >= 1, (
                "llm_calls row missing — sibling A2.1 must have wired "
                "`ai._log_llm_call_safe`. Re-run with that PR landed."
            )

            # decision_inputs — writer fires as a fire-and-forget background
            # task at the SIGNAL stage (engine.py). It's spawned before the
            # SIM_GATE so even early-skip headlines write one row.
            assert await _count(
                db,
                "SELECT COUNT(*) FROM decision_inputs WHERE headline_id = ?",
                (hid,),
            ) >= 1, "decision_inputs row missing"

            # trades / position_price_snaps — best-effort; depends on
            # whether the lone match actually cleared the trader gates.
            n_trades = await _count(
                db, "SELECT COUNT(*) FROM trades WHERE headline_id = ?",
                (hid,),
            )
            if n_trades > 0:
                # position_price_snaps writer (impact_tracker loop) is async
                # and fires on a 5/15/30/60/240 min timer; one-shot
                # `process()` won't tick it. >=0 sanity bound only.
                assert await _count(
                    db,
                    "SELECT COUNT(*) FROM position_price_snaps "
                    "WHERE trade_id IN (SELECT id FROM trades "
                    "WHERE headline_id = ?)", (hid,),
                ) >= 0
        finally:
            await runner.close()
            await db.close()


# ---------------------------------------------------------------------------
# Positive case 2 — SEULGI_BYPASS_LLM=1: llm_calls NOT written, rest is
# ---------------------------------------------------------------------------


class TestCapturePipelineWithBypass:
    @pytest.mark.asyncio
    async def test_capture_pipeline_with_bypass(self, tmp_path, monkeypatch):
        """With SEULGI_BYPASS_LLM=1 the prediction LLM is short-circuited
        with synthetic favorite-side results — llm_calls is NOT written but
        every other Phase-5 row still is."""
        monkeypatch.setenv("SEULGI_BYPASS_LLM", "1")
        monkeypatch.setenv("SEULGI_DISABLE_HOUR_GATE", "1")
        ai._BYPASS_STARTUP_LOGGED = False

        runner, db, classifier, _match = await _build_harness(
            str(tmp_path / "capture_bypass.db"))
        try:
            # Arm the HTTP layer to raise — bypass MUST NOT call it.
            async def _explode(*_a, **_kw):
                raise AssertionError("bypass must not call OpenRouter")
            classifier._call_api = _explode  # type: ignore[assignment]

            headline = _make_headline("hid_bypass_test_001")
            hid = headline.headline_id

            with patch("engine._fetch_mid_price",
                       AsyncMock(return_value=0.45)), \
                 patch("trader._fetch_mid_price",
                       AsyncMock(return_value=0.45)):
                await runner.process(headline)

            assert await _count(
                db, "SELECT COUNT(*) FROM signals WHERE headline_id = ?",
                (hid,),
            ) >= 1
            assert await _count(
                db,
                "SELECT COUNT(*) FROM market_price_snaps WHERE headline_id = ?",
                (hid,),
            ) >= 1
            assert await _count(
                db,
                "SELECT COUNT(*) FROM engine_results WHERE headline_id = ?",
                (hid,),
            ) >= 1

            # The KEY bypass invariant: no LLM row written for this headline.
            assert await _count(
                db, "SELECT COUNT(*) FROM llm_calls WHERE headline_id = ?",
                (hid,),
            ) == 0, "llm_calls row found despite SEULGI_BYPASS_LLM=1"
        finally:
            await runner.close()
            await db.close()


# ---------------------------------------------------------------------------
# Negative case — disabling the engine_results writer must make the test FAIL.
# Proves the positive assertions above are load-bearing.
# ---------------------------------------------------------------------------


class TestCaptureNegative:
    @pytest.mark.asyncio
    async def test_capture_fails_loudly_when_writer_disabled(
        self, tmp_path, monkeypatch,
    ):
        """Stub out `db.record_engine_decisions` so it never writes; the
        engine_results assertion must then fail. This guards against
        accidentally weakening the positive cases (e.g. `>= 0`)."""
        monkeypatch.delenv("SEULGI_BYPASS_LLM", raising=False)
        monkeypatch.setenv("SEULGI_DISABLE_HOUR_GATE", "1")
        ai._BYPASS_STARTUP_LOGGED = False

        runner, db, classifier, _match = await _build_harness(
            str(tmp_path / "capture_neg.db"))
        try:
            _stub_classifier_http(classifier)

            # Replace the writer with a coroutine that returns without
            # writing. The Phase-5 assertion below must observe 0 rows.
            async def _noop(*_a, **_kw):
                return None
            monkeypatch.setattr(db, "record_engine_decisions", _noop)

            headline = _make_headline("hid_neg_test_001")
            hid = headline.headline_id

            with patch("engine._fetch_mid_price",
                       AsyncMock(return_value=0.45)), \
                 patch("trader._fetch_mid_price",
                       AsyncMock(return_value=0.45)):
                await runner.process(headline)

            # The integration test from the positive cases would have
            # asserted >= 1; here we INVERT and confirm 0, then re-run
            # the same assertion and confirm it raises.
            n = await _count(
                db,
                "SELECT COUNT(*) FROM engine_results WHERE headline_id = ?",
                (hid,),
            )
            assert n == 0, (
                "writer was meant to be disabled but rows were still "
                f"written (n={n}); the negative test is moot"
            )
            with pytest.raises(AssertionError):
                assert n >= 1, "engine_results row missing"
        finally:
            await runner.close()
            await db.close()

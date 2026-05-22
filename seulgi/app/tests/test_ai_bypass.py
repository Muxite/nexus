"""
Tests for the SEULGI_BYPASS_LLM operator toggle (A1.6, 2026-05-22).

The bypass flag short-circuits DirectionClassifier.classify_batch BEFORE
any OpenRouter HTTP call, returning deterministic favorite-side synthetic
results so the trade-filter pipeline can be exercised at full volume with
$0 LLM spend while Phase 1 work is in flight.

Pinned behaviour:
  - SEULGI_BYPASS_LLM=1 → no HTTP, N synthetic results for N matches.
  - Direction follows market favorite (mid_yes >= 0.5 → YES else NO).
  - Confidence/p_alpha clear any plausible trader gate (0.9 / 1.0).
  - prompt_meta carries model='llm_bypass' + bypass=True so engine.py
    writes a $0 usage_events row for cost-dashboard visibility.
  - Default off — without the env var, the legacy LLM path runs.
"""

import asyncio
import json
import sys
import os
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import ai
from ai import Direction, ClassifyResult, DirectionClassifier
from markets import MarketMatch


def _mk_match(mid_yes, condition_id="c1", question="q?", similarity=0.8):
    return MarketMatch(
        condition_id=condition_id,
        question=question,
        token_yes="ty",
        token_no="tn",
        similarity=similarity,
        mid_yes=mid_yes,
        mid_no=(None if mid_yes is None else 1.0 - mid_yes),
    )


def _make_openrouter_dist_response(p_yes=0.6, p_no=0.3, p_neu=0.1, p_alpha=0.7):
    """Build a parseable batched OpenRouter response (one market)."""
    body = {"results": [{
        "idx": 0,
        "p_yes": p_yes, "p_no": p_no, "p_neutral": p_neu,
        "p_alpha": p_alpha, "risk": 0.3,
        "reason": "mock",
    }]}
    return {
        "choices": [{"message": {"content": json.dumps(body)}}],
        "usage": {"total_tokens": 17},
    }


def _make_mock_response_cm(status, data):
    mock_resp = MagicMock()
    mock_resp.status = status
    mock_resp.json = AsyncMock(return_value=data)
    mock_resp.text = AsyncMock(return_value="")

    @asynccontextmanager
    async def _cm(*args, **kwargs):
        yield mock_resp

    return _cm


@pytest.fixture(autouse=True)
def _reset_bypass_state(monkeypatch):
    """Each test starts with the env var cleared and the startup banner
    re-armed so log-once behaviour can be observed deterministically."""
    monkeypatch.delenv("SEULGI_BYPASS_LLM", raising=False)
    ai._BYPASS_STARTUP_LOGGED = False
    yield
    ai._BYPASS_STARTUP_LOGGED = False


# ---------------------------------------------------------------------------
# Short-circuit: no HTTP touched
# ---------------------------------------------------------------------------


class TestBypassShortCircuitsNoHttp:
    @pytest.mark.asyncio
    async def test_bypass_short_circuits_no_http(self, monkeypatch):
        """SEULGI_BYPASS_LLM=1 must return N results for N matches without
        invoking the HTTP layer. We arm _call_api to raise if hit."""
        monkeypatch.setenv("SEULGI_BYPASS_LLM", "1")
        clf = DirectionClassifier(api_key="key", model="m", ensemble_samples=3)

        async def _explode(*_a, **_kw):
            raise AssertionError("bypass must not call OpenRouter")

        clf._call_api = _explode  # type: ignore[assignment]

        matches = [_mk_match(0.7), _mk_match(0.3), _mk_match(0.55)]
        results, meta = await clf.classify_batch(
            headline="something happened",
            matches=matches,
        )

        assert len(results) == 3
        assert meta["model"] == "llm_bypass"
        assert meta["bypass"] is True
        for r in results:
            assert r.bypass is True
            assert r.tokens_used == 0


# ---------------------------------------------------------------------------
# Direction follows the favorite
# ---------------------------------------------------------------------------


class TestBypassDirectionFollowsMid:
    @pytest.mark.asyncio
    async def test_bypass_direction_follows_mid(self, monkeypatch):
        """mid_yes ∈ {0.3, 0.6, 0.8} → directions NO, YES, YES."""
        monkeypatch.setenv("SEULGI_BYPASS_LLM", "1")
        clf = DirectionClassifier(api_key="key", model="m", ensemble_samples=1)

        matches = [_mk_match(0.3), _mk_match(0.6), _mk_match(0.8)]
        results, _meta = await clf.classify_batch(
            headline="anything", matches=matches,
        )

        directions = [r.direction for r in results]
        assert directions == [Direction.NO, Direction.YES, Direction.YES]

        # Synthetic invariants the trader depends on
        for r in results:
            assert r.confidence == pytest.approx(0.9)
            assert r.p_alpha == pytest.approx(1.0)
            assert r.agreement == pytest.approx(1.0)
            assert r.p_neutral == pytest.approx(0.0)
            assert r.bypass is True
            assert r.reason == "LLM bypass: favorite-side synthetic"

        # Deterministic: same input → byte-identical output
        results2, _ = await clf.classify_batch(headline="anything", matches=matches)
        for a, b in zip(results, results2):
            assert (a.direction, a.confidence, a.p_yes, a.p_no, a.p_alpha) == (
                b.direction, b.confidence, b.p_yes, b.p_no, b.p_alpha
            )

    @pytest.mark.asyncio
    async def test_bypass_exact_half_is_yes(self, monkeypatch):
        """Boundary check: mid_yes == 0.5 lands on YES (>= comparison)."""
        monkeypatch.setenv("SEULGI_BYPASS_LLM", "1")
        clf = DirectionClassifier(api_key="key", model="m", ensemble_samples=1)
        results, _ = await clf.classify_batch(
            headline="x", matches=[_mk_match(0.5)],
        )
        assert results[0].direction == Direction.YES

    @pytest.mark.asyncio
    async def test_bypass_missing_mid_defaults_to_no(self, monkeypatch):
        """mid_yes=None → NO (deterministic stable fallback)."""
        monkeypatch.setenv("SEULGI_BYPASS_LLM", "1")
        clf = DirectionClassifier(api_key="key", model="m", ensemble_samples=1)
        results, _ = await clf.classify_batch(
            headline="x", matches=[_mk_match(None)],
        )
        assert results[0].direction == Direction.NO


# ---------------------------------------------------------------------------
# Telemetry: usage_events row carries model='llm_bypass' with 0 tokens
# ---------------------------------------------------------------------------


class TestBypassRecordsTelemetry:
    @pytest.mark.asyncio
    async def test_bypass_records_telemetry(self, monkeypatch):
        """Engine.py logs a usage_events row for the bypass batch with
        model='llm_bypass' and count=0 — cost dashboards must see $0
        burn instead of a silent gap in the stream."""
        monkeypatch.setenv("SEULGI_BYPASS_LLM", "1")
        clf = DirectionClassifier(api_key="key", model="m", ensemble_samples=1)

        results, meta = await clf.classify_batch(
            headline="x", matches=[_mk_match(0.7), _mk_match(0.2)],
        )

        # Simulate the engine.py write path inline so this test stays
        # local to the bypass contract.
        tokens_used = sum(r.tokens_used for r in results)
        is_bypass = bool(meta.get("bypass"))
        assert is_bypass is True
        assert tokens_used == 0

        logged: list[dict] = []

        async def _fake_log(kind, count, provider, meta):
            logged.append({"kind": kind, "count": count,
                           "provider": provider, "meta": meta})

        if tokens_used or is_bypass:
            await _fake_log(
                kind="ai_tokens", count=float(tokens_used),
                provider=meta.get("model"),
                meta=json.dumps({"hid": "hid_x", "n_markets": len(results),
                                 "stage": "prediction",
                                 **({"bypass": True} if is_bypass else {})}),
            )

        assert len(logged) == 1
        row = logged[0]
        assert row["kind"] == "ai_tokens"
        assert row["provider"] == "llm_bypass"
        assert row["count"] == 0.0
        assert json.loads(row["meta"])["bypass"] is True


# ---------------------------------------------------------------------------
# Default off: env var unset → normal LLM path runs
# ---------------------------------------------------------------------------


class TestBypassDefaultOff:
    @pytest.mark.asyncio
    async def test_bypass_default_off(self, monkeypatch):
        """Without SEULGI_BYPASS_LLM, classify_batch hits OpenRouter as
        normal and parses a real response."""
        # Env var explicitly absent (fixture clears it; assert defensively).
        assert os.environ.get("SEULGI_BYPASS_LLM") is None

        clf = DirectionClassifier(api_key="key", model="m", ensemble_samples=1)
        clf._inject_session = lambda *a, **kw: None  # noqa: ARG005
        mock_resp = _make_openrouter_dist_response()

        mock_session = MagicMock()
        mock_session.closed = False
        mock_session.post = _make_mock_response_cm(200, mock_resp)
        clf._session = mock_session

        results, meta = await clf.classify_batch(
            headline="x", matches=[_mk_match(0.5)],
        )

        assert len(results) == 1
        # Real LLM path → bypass flag stays False on every result
        assert results[0].bypass is False
        # Real path emits the actual model name, not the bypass tag
        assert meta.get("model") == "m"
        assert meta.get("bypass") is None or meta.get("bypass") is False

    @pytest.mark.asyncio
    async def test_bypass_zero_string_is_off(self, monkeypatch):
        """SEULGI_BYPASS_LLM=0 (explicit) is still OFF — only '1' enables."""
        monkeypatch.setenv("SEULGI_BYPASS_LLM", "0")

        clf = DirectionClassifier(api_key="key", model="m", ensemble_samples=1)
        mock_session = MagicMock()
        mock_session.closed = False
        mock_session.post = _make_mock_response_cm(
            200, _make_openrouter_dist_response(),
        )
        clf._session = mock_session

        results, meta = await clf.classify_batch(
            headline="x", matches=[_mk_match(0.5)],
        )
        assert results[0].bypass is False
        assert meta.get("model") == "m"

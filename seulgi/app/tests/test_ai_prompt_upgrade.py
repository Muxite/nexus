"""
Tests for A4E.4 (2026-05-23) — Stage-3 prompt + parse upgrade.

Pinned behaviour:
  - `response_format=json_schema` is the default; the request body carries
    the strict schema dict.
  - A 400 mentioning "json_schema unsupported" flips the classifier to
    `response_format=json_object` for the remainder of its lifetime AND
    retries the failing call once so it still succeeds.
  - `<scratch>...</scratch>` content (and any prose preamble) is stripped
    from the response by `_extract_text` before JSON parsing.
  - `_aggregate_ensemble` enforces a hard 2/3 agreement gate: when
    agreement < 2/3 the result is demoted to NEUTRAL with confidence=0.0.
  - `batch_size=10` chunks `matches` into ⌈len/batch_size⌉ separate LLM
    calls and concatenates per-market results in input order.
  - `_regex_extract_partial` recovers per-market tuples from malformed
    JSON when JSON parse fails; accepted at ≥ 50% recovery rate.
  - Bypass mode (SEULGI_BYPASS_LLM=1) remains unchanged — short-circuits
    before any HTTP, regardless of schema / batch_size / agreement gate.
"""

from __future__ import annotations

import asyncio
import json
import sys
import os
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import ai
from ai import (
    Direction,
    ClassifyResult,
    DirectionClassifier,
    _RESPONSE_SCHEMA,
    _AGREEMENT_DEMOTE_THRESHOLD,
    _aggregate_ensemble,
    _extract_text,
    _regex_extract_partial,
)
from markets import MarketMatch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mk_match(mid_yes=0.5, condition_id="c1", question="q?", similarity=0.8):
    return MarketMatch(
        condition_id=condition_id,
        question=question,
        token_yes="ty",
        token_no="tn",
        similarity=similarity,
        mid_yes=mid_yes,
        mid_no=(None if mid_yes is None else 1.0 - mid_yes),
    )


def _make_decisions_response(n: int, *, p_yes=0.6, p_no=0.3, p_neu=0.1,
                             p_alpha=0.7, scratch: str = "") -> dict:
    """Build a parseable response in the A4E.4 `{"decisions":[...]}` shape."""
    body = {"decisions": [
        {"idx": i, "p_yes": p_yes, "p_no": p_no, "p_neutral": p_neu,
         "p_alpha": p_alpha, "reason": "mock"}
        for i in range(n)
    ]}
    content = (scratch + json.dumps(body)) if scratch else json.dumps(body)
    return {
        "choices": [{"message": {"content": content}}],
        "usage": {"total_tokens": 50, "prompt_tokens": 30, "completion_tokens": 20},
    }


def _make_mock_response_cm(status, data, *, body_text: str = ""):
    mock_resp = MagicMock()
    mock_resp.status = status
    mock_resp.json = AsyncMock(return_value=data)
    mock_resp.text = AsyncMock(return_value=body_text)

    @asynccontextmanager
    async def _cm(*args, **kwargs):
        yield mock_resp

    return _cm


@pytest.fixture(autouse=True)
def _reset_bypass_state(monkeypatch):
    monkeypatch.delenv("SEULGI_BYPASS_LLM", raising=False)
    ai._BYPASS_STARTUP_LOGGED = False
    yield
    ai._BYPASS_STARTUP_LOGGED = False


# ---------------------------------------------------------------------------
# (1) json_schema as default response_format
# ---------------------------------------------------------------------------


class TestJsonSchemaResponseFormat:
    @pytest.mark.asyncio
    async def test_request_body_uses_json_schema_by_default(self):
        """When schema_supported is True (default), the body sent to
        OpenRouter must carry the strict `_RESPONSE_SCHEMA` shape."""
        clf = DirectionClassifier(
            api_key="k", model="m", ensemble_samples=1, batch_size=10,
        )
        assert clf._schema_supported is True

        captured: list[dict] = []

        @asynccontextmanager
        async def _cm(*args, **kwargs):
            captured.append(kwargs.get("json", {}))
            resp = MagicMock()
            resp.status = 200
            resp.json = AsyncMock(return_value=_make_decisions_response(1))
            resp.text = AsyncMock(return_value="")
            yield resp

        session = MagicMock()
        session.closed = False
        session.post = _cm
        clf._session = session

        await clf.classify_batch(headline="x", matches=[_mk_match(0.5)])

        assert captured, "no request was made"
        body = captured[0]
        assert body["response_format"] == _RESPONSE_SCHEMA
        # Strict schema must request `decisions` as the outer key.
        assert body["response_format"]["json_schema"]["schema"]["required"] == ["decisions"]


# ---------------------------------------------------------------------------
# (2) Fallback to json_object when provider rejects json_schema
# ---------------------------------------------------------------------------


class TestJsonSchemaFallback:
    @pytest.mark.asyncio
    async def test_schema_unsupported_400_flips_to_json_object(self):
        """A 400 mentioning json_schema must flip the instance to
        json_object for the remainder of its life AND retry the failing
        call so it succeeds anyway."""
        clf = DirectionClassifier(
            api_key="k", model="m", ensemble_samples=1, batch_size=10,
        )

        responses_by_format: list[str] = []

        @asynccontextmanager
        async def _cm(*args, **kwargs):
            body = kwargs.get("json") or {}
            fmt_type = (body.get("response_format") or {}).get("type")
            responses_by_format.append(fmt_type)
            resp = MagicMock()
            if fmt_type == "json_schema":
                resp.status = 400
                resp.text = AsyncMock(
                    return_value='{"error":"response_format json_schema is not supported"}'
                )
                resp.json = AsyncMock(return_value={})
            else:
                resp.status = 200
                resp.text = AsyncMock(return_value="")
                resp.json = AsyncMock(return_value=_make_decisions_response(1))
            yield resp

        session = MagicMock()
        session.closed = False
        session.post = _cm
        clf._session = session

        results, _meta = await clf.classify_batch(
            headline="x", matches=[_mk_match(0.5)],
        )

        # Schema was tried, rejected, then json_object succeeded on retry.
        assert responses_by_format[0] == "json_schema"
        assert "json_object" in responses_by_format
        assert clf._schema_supported is False
        assert len(results) == 1

    def test_schema_unsupported_detector_matches_common_messages(self):
        assert DirectionClassifier._is_schema_unsupported_error(
            "Unknown parameter: response_format.json_schema"
        )
        assert DirectionClassifier._is_schema_unsupported_error(
            "Unsupported response_format type"
        )
        assert DirectionClassifier._is_schema_unsupported_error(
            "json schema is not supported by this model"
        )
        # Doesn't false-positive on unrelated 400s
        assert not DirectionClassifier._is_schema_unsupported_error(
            "Invalid model id"
        )


# ---------------------------------------------------------------------------
# (3) <scratch> block stripped from response
# ---------------------------------------------------------------------------


class TestScratchBlockStripped:
    def test_extract_text_drops_scratch_preamble(self):
        """`<scratch>...</scratch>` (and any prose before the first `{`)
        is stripped so json.loads sees only the structured body."""
        scratch = (
            "<scratch>\nReasoning about market 0: headline likely YES.\n"
            "Reasoning about market 1: too noisy.\n</scratch>\n\n"
        )
        data = _make_decisions_response(2, scratch=scratch)
        text = _extract_text(data)
        # The first character of the stripped output must be `{`.
        assert text.startswith("{"), f"got: {text[:40]!r}"
        # And it must parse cleanly with no leading prose.
        parsed = json.loads(text)
        assert "decisions" in parsed
        assert len(parsed["decisions"]) == 2

    def test_extract_text_drops_arbitrary_preamble(self):
        """Any prose before the first `{` is stripped, not just scratch."""
        data = {
            "choices": [{"message": {"content": (
                "I will now think out loud: this looks like a YES signal.\n"
                "Here is my JSON:\n"
                '{"decisions":[{"idx":0,"p_yes":0.9,"p_no":0.05,'
                '"p_neutral":0.05,"p_alpha":0.8,"reason":"hi"}]}'
            )}}],
            "usage": {},
        }
        text = _extract_text(data)
        assert text.startswith("{")
        assert json.loads(text)["decisions"][0]["idx"] == 0


# ---------------------------------------------------------------------------
# (4) Hard 2/3 agreement gate
# ---------------------------------------------------------------------------


class TestAgreementGate:
    def test_threshold_value(self):
        # Pin the constant — if anyone bumps it from 2/3 the trade volume
        # behaviour changes silently otherwise.
        assert _AGREEMENT_DEMOTE_THRESHOLD == pytest.approx(2.0 / 3.0)

    def test_one_third_vote_demotes_to_neutral(self):
        """3 samples voting [YES, NO, NEUTRAL] = 1/3 agreement → NEUTRAL,
        conf=0.0, regardless of the model's stated p_alpha."""
        yes_r = ClassifyResult(Direction.YES, 0.8, "y", p_yes=0.7, p_no=0.2,
                               p_neutral=0.1, p_alpha=0.8)
        no_r = ClassifyResult(Direction.NO, 0.8, "n", p_yes=0.2, p_no=0.7,
                              p_neutral=0.1, p_alpha=0.8)
        neu_r = ClassifyResult(Direction.NEUTRAL, 0.5, "z", p_yes=0.3, p_no=0.3,
                               p_neutral=0.4, p_alpha=0.0)

        out = _aggregate_ensemble([[yes_r], [no_r], [neu_r]], n_markets=1)
        assert out[0].direction == Direction.NEUTRAL
        assert out[0].confidence == 0.0
        assert out[0].agreement == pytest.approx(1 / 3)
        assert "agreement_below_threshold" in out[0].reason

    def test_two_thirds_vote_keeps_direction(self):
        """2/3 agreement → direction kept, confidence = p_alpha."""
        yes_r = ClassifyResult(Direction.YES, 0.7, "y", p_yes=0.7, p_no=0.2,
                               p_neutral=0.1, p_alpha=0.7)
        out = _aggregate_ensemble(
            [[yes_r], [yes_r], [ClassifyResult(
                Direction.NO, 0.6, "n", p_yes=0.2, p_no=0.7, p_neutral=0.1,
                p_alpha=0.6,
            )]],
            n_markets=1,
        )
        assert out[0].direction == Direction.YES
        assert out[0].agreement == pytest.approx(2 / 3)
        assert out[0].confidence > 0.0

    def test_unanimous_vote_preserves_strong_confidence(self):
        yes_r = ClassifyResult(Direction.YES, 0.9, "y", p_yes=0.9, p_no=0.05,
                               p_neutral=0.05, p_alpha=0.85)
        out = _aggregate_ensemble([[yes_r], [yes_r], [yes_r]], n_markets=1)
        assert out[0].direction == Direction.YES
        assert out[0].agreement == 1.0
        assert out[0].confidence == pytest.approx(0.85)


# ---------------------------------------------------------------------------
# (5) Smaller batch size (10) produces multiple chunked calls
# ---------------------------------------------------------------------------


class TestBatchSizeChunking:
    @pytest.mark.asyncio
    async def test_batch_size_10_chunks_25_markets_into_3_calls(self):
        """25 markets at batch_size=10 → 3 chunks of (10, 10, 5)."""
        import re as _re
        clf = DirectionClassifier(
            api_key="k", model="m", ensemble_samples=1, batch_size=10,
        )

        call_sizes: list[int] = []

        async def _fake_call_api(body):
            # Count lines that look like the prompt's market list entries:
            # they start with a digit (0..24), a `.`, a space, then `[YES`.
            content = body["messages"][1]["content"]
            n = len(_re.findall(r"^\d+\.\s+\[YES", content, _re.MULTILINE))
            call_sizes.append(n)
            return _make_decisions_response(n)

        clf._call_api = _fake_call_api  # type: ignore[assignment]

        matches = [_mk_match(0.5, condition_id=f"c{i}") for i in range(25)]
        results, meta = await clf.classify_batch(
            headline="x", matches=matches,
        )

        assert len(results) == 25
        assert len(call_sizes) == 3, f"chunked into {len(call_sizes)} calls: {call_sizes}"
        assert sorted(call_sizes, reverse=True) == [10, 10, 5]
        assert meta["n_chunks"] == 3
        assert meta["batch_size"] == 10

    @pytest.mark.asyncio
    async def test_batch_size_larger_than_matches_one_call(self):
        """5 markets at batch_size=10 → exactly one call."""
        clf = DirectionClassifier(
            api_key="k", model="m", ensemble_samples=1, batch_size=10,
        )

        calls = 0

        async def _fake_call_api(body):
            nonlocal calls
            calls += 1
            return _make_decisions_response(5)

        clf._call_api = _fake_call_api  # type: ignore[assignment]

        matches = [_mk_match(0.5, condition_id=f"c{i}") for i in range(5)]
        await clf.classify_batch(headline="x", matches=matches)
        assert calls == 1


# ---------------------------------------------------------------------------
# (6) Regex-extract fallback recovers partial JSON
# ---------------------------------------------------------------------------


class TestRegexExtractFallback:
    def test_recovers_when_json_truncated(self):
        """Truncated response (no closing `]}`) — regex still finds the
        per-market tuples and returns them."""
        text = (
            '{"decisions":['
            '{"idx":0,"p_yes":0.6,"p_no":0.3,"p_neutral":0.1,"p_alpha":0.7,"reason":"a"},'
            '{"idx":1,"p_yes":0.2,"p_no":0.7,"p_neutral":0.1,"p_alpha":0.65,"reason":"b"},'
            '{"idx":2,"p_yes":0.5,"p_no":0.4,"p_neutral":0.1,"p_alpha":0.5,"reason":"c"}'
            # ← deliberately truncated, no trailing `]}`
        )
        out = _regex_extract_partial(text, n=3)
        assert out is not None
        results, raws = out
        assert len(results) == 3
        assert results[0].direction == Direction.YES
        assert results[1].direction == Direction.NO

    def test_returns_none_below_50_percent_recovery(self):
        """Only 1 of 10 markets recovered — refuse the partial."""
        text = (
            '{"decisions":[{"idx":0,"p_yes":0.6,"p_no":0.3,'
            '"p_neutral":0.1,"p_alpha":0.7,"reason":"a"}]}'
        )
        # Asking for 10 markets but only 1 recovered → 10% < 50%, refused.
        assert _regex_extract_partial(text, n=10) is None

    def test_handles_empty_text(self):
        assert _regex_extract_partial("", n=3) is None
        assert _regex_extract_partial("not json at all", n=3) is None

    def test_pads_missing_indices_with_neutral(self):
        """When some idxs in [0,n) are missing from the recovered set
        but ≥ 50% are present, the gap is filled with NEUTRAL."""
        text = (
            '{"decisions":['
            '{"idx":0,"p_yes":0.6,"p_no":0.3,"p_neutral":0.1,"p_alpha":0.7,"reason":"a"},'
            # idx 1 missing
            '{"idx":2,"p_yes":0.2,"p_no":0.7,"p_neutral":0.1,"p_alpha":0.6,"reason":"c"}]}'
        )
        out = _regex_extract_partial(text, n=3)
        assert out is not None
        results, _raws = out
        assert results[0].direction == Direction.YES
        assert results[1].direction == Direction.NEUTRAL  # padded
        assert results[1].reason == "missing_regex"
        assert results[2].direction == Direction.NO


# ---------------------------------------------------------------------------
# (7) Bypass mode unchanged by A4E.4 changes
# ---------------------------------------------------------------------------


class TestBypassModeUnchanged:
    @pytest.mark.asyncio
    async def test_bypass_still_short_circuits_with_new_batch_size(
        self, monkeypatch,
    ):
        """SEULGI_BYPASS_LLM=1 short-circuits before chunking + schema
        machinery. Even at batch_size=10 and 25 matches, zero HTTP."""
        monkeypatch.setenv("SEULGI_BYPASS_LLM", "1")
        clf = DirectionClassifier(
            api_key="k", model="m", ensemble_samples=3, batch_size=10,
        )

        async def _explode(*_a, **_kw):
            raise AssertionError("bypass must not call OpenRouter")

        clf._call_api = _explode  # type: ignore[assignment]

        matches = [_mk_match(0.6 if i % 2 == 0 else 0.4,
                              condition_id=f"c{i}") for i in range(25)]
        results, meta = await clf.classify_batch(
            headline="x", matches=matches,
        )

        assert len(results) == 25
        assert meta["bypass"] is True
        assert meta["model"] == "llm_bypass"
        # Bypass produces unanimous-by-construction (agreement = 1.0).
        for r in results:
            assert r.bypass is True
            assert r.agreement == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# (8) End-to-end smoke: scratch + decisions schema + per-market output
# ---------------------------------------------------------------------------


class TestEndToEndDecisionsSchema:
    @pytest.mark.asyncio
    async def test_full_round_trip_with_scratch_and_decisions(self):
        """A response with a scratch preamble and the new `{"decisions":[...]}`
        shape parses into per-market ClassifyResult correctly."""
        clf = DirectionClassifier(
            api_key="k", model="m", ensemble_samples=1, batch_size=10,
        )

        scratch = "<scratch>thinking...</scratch>\n\n"
        ok = _make_decisions_response(2, p_yes=0.7, p_no=0.2, p_neu=0.1,
                                      p_alpha=0.75, scratch=scratch)

        session = MagicMock()
        session.closed = False
        session.post = _make_mock_response_cm(200, ok)
        clf._session = session

        results, meta = await clf.classify_batch(
            headline="x",
            matches=[_mk_match(0.5, condition_id="c0"),
                     _mk_match(0.5, condition_id="c1")],
        )

        assert len(results) == 2
        for r in results:
            assert r.direction == Direction.YES
            assert r.confidence == pytest.approx(0.75)
        # Meta surfaces the new chunking knobs.
        assert meta["batch_size"] == 10
        assert meta["n_chunks"] == 1

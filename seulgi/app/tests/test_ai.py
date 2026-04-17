"""
Tests for seulgi/app/ai.py

Covers:
- _parse_response: valid JSON, markdown fences, missing fields, parse errors
- DirectionClassifier.classify: YES/NO/NEUTRAL responses, 429 retry, API errors
"""

import asyncio
import json
import sys
import os
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ai import Direction, ClassifyResult, DirectionClassifier, _parse_response


# ---------------------------------------------------------------------------
# _parse_response
# ---------------------------------------------------------------------------


def _make_openrouter_response(direction: str, confidence: float, reason: str) -> dict:
    """Build a mock OpenRouter chat completions response."""
    return {
        "choices": [{
            "message": {"content": json.dumps({
                "direction": direction,
                "confidence": confidence,
                "reason": reason,
            })}
        }],
        "usage": {"total_tokens": 42},
    }

# Keep old name as alias so the rest of the tests still work
_make_gemini_response = _make_openrouter_response


class TestParseResponse:
    def test_yes_direction(self):
        data = _make_gemini_response("YES", 0.8, "clearly bullish")
        result = _parse_response(data)
        assert result.direction == Direction.YES
        assert result.confidence == pytest.approx(0.8)
        assert result.reason == "clearly bullish"
        assert result.tokens_used == 42

    def test_no_direction(self):
        data = _make_gemini_response("NO", 0.75, "clearly bearish")
        result = _parse_response(data)
        assert result.direction == Direction.NO

    def test_neutral_direction(self):
        data = _make_gemini_response("NEUTRAL", 0.5, "unclear")
        result = _parse_response(data)
        assert result.direction == Direction.NEUTRAL

    def test_unknown_direction_becomes_neutral(self):
        data = _make_gemini_response("MAYBE", 0.6, "unsure")
        result = _parse_response(data)
        assert result.direction == Direction.NEUTRAL

    def test_confidence_clamped(self):
        data = _make_gemini_response("YES", 1.5, "very sure")
        result = _parse_response(data)
        assert result.confidence == 1.0

    def test_strips_markdown_fences(self):
        text = "```json\n{\"direction\":\"YES\",\"confidence\":0.9,\"reason\":\"yes\"}\n```"
        data = {
            "choices": [{"message": {"content": text}}],
            "usage": {},
        }
        result = _parse_response(data)
        assert result.direction == Direction.YES

    def test_empty_candidates(self):
        result = _parse_response({"choices": []})
        assert result.direction == Direction.NEUTRAL
        assert result.reason == "no_candidates"

    def test_invalid_json(self):
        data = {
            "choices": [{"message": {"content": "not json at all"}}],
        }
        result = _parse_response(data)
        assert result.direction == Direction.NEUTRAL
        assert result.reason == "parse_error"


# ---------------------------------------------------------------------------
# DirectionClassifier.classify
# ---------------------------------------------------------------------------


def _make_mock_response(status: int, json_data: dict):
    """Build a mock aiohttp response context manager."""
    mock_resp = MagicMock()
    mock_resp.status = status
    mock_resp.json = AsyncMock(return_value=json_data)
    mock_resp.text = AsyncMock(return_value="error text")

    @asynccontextmanager
    async def _cm(*args, **kwargs):
        yield mock_resp

    return _cm


class TestDirectionClassifier:
    def _make_classifier(self) -> DirectionClassifier:
        return DirectionClassifier(api_key="test_key", model="gemini-2.5-flash")

    def _inject_session(self, classifier: DirectionClassifier, response_cm):
        mock_session = MagicMock()
        mock_session.closed = False
        mock_session.post = response_cm
        classifier._session = mock_session
        return mock_session

    @pytest.mark.asyncio
    async def test_classify_yes(self):
        clf = self._make_classifier()
        gemini_data = _make_gemini_response("YES", 0.85, "bullish signal")
        self._inject_session(clf, _make_mock_response(200, gemini_data))

        result = await clf.classify("Fed cuts rates by 50bps", "Will inflation decrease in 2025?")
        assert result.direction == Direction.YES
        assert result.confidence == pytest.approx(0.85)

    @pytest.mark.asyncio
    async def test_classify_no(self):
        clf = self._make_classifier()
        gemini_data = _make_gemini_response("NO", 0.78, "bearish for YES")
        self._inject_session(clf, _make_mock_response(200, gemini_data))

        result = await clf.classify("Inflation surges to 8%", "Will inflation decrease in 2025?")
        assert result.direction == Direction.NO

    @pytest.mark.asyncio
    async def test_classify_neutral(self):
        clf = self._make_classifier()
        gemini_data = _make_gemini_response("NEUTRAL", 0.5, "unrelated")
        self._inject_session(clf, _make_mock_response(200, gemini_data))

        result = await clf.classify("Dog show held in London", "Will inflation decrease in 2025?")
        assert result.direction == Direction.NEUTRAL

    @pytest.mark.asyncio
    async def test_non_200_returns_neutral(self):
        clf = self._make_classifier()
        self._inject_session(clf, _make_mock_response(500, {}))

        result = await clf.classify("some headline", "some question?")
        assert result.direction == Direction.NEUTRAL
        assert result.reason == "api_error"

    @pytest.mark.asyncio
    async def test_exception_returns_neutral(self):
        clf = self._make_classifier()
        mock_session = MagicMock()
        mock_session.closed = False

        @asynccontextmanager
        async def _raise(*args, **kwargs):
            raise ConnectionError("network down")
            yield  # unreachable but makes it a generator

        mock_session.post = _raise
        clf._session = mock_session

        result = await clf.classify("some headline", "some question?")
        assert result.direction == Direction.NEUTRAL

    @pytest.mark.asyncio
    async def test_429_retries_and_succeeds(self):
        clf = self._make_classifier()
        gemini_data = _make_gemini_response("YES", 0.9, "bullish")

        call_count = 0

        mock_resp_429 = MagicMock()
        mock_resp_429.status = 429
        mock_resp_200 = MagicMock()
        mock_resp_200.status = 200
        mock_resp_200.json = AsyncMock(return_value=gemini_data)

        @asynccontextmanager
        async def _cm(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                yield mock_resp_429
            else:
                yield mock_resp_200

        mock_session = MagicMock()
        mock_session.closed = False
        mock_session.post = _cm
        clf._session = mock_session

        with patch("asyncio.sleep", AsyncMock()):
            result = await clf.classify("headline", "question?")

        assert result.direction == Direction.YES
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_429_three_times_returns_neutral(self):
        clf = self._make_classifier()
        mock_resp = MagicMock()
        mock_resp.status = 429

        @asynccontextmanager
        async def _cm(*args, **kwargs):
            yield mock_resp

        mock_session = MagicMock()
        mock_session.closed = False
        mock_session.post = _cm
        clf._session = mock_session

        with patch("asyncio.sleep", AsyncMock()):
            result = await clf.classify("headline", "question?")

        assert result.direction == Direction.NEUTRAL
        assert result.reason == "rate_limited"

    @pytest.mark.asyncio
    async def test_semaphore_limits_concurrency(self):
        """Max concurrent requests is enforced by the semaphore."""
        clf = DirectionClassifier(api_key="key", model="gemini-2.5-flash", max_concurrent=2)
        assert clf._semaphore._value == 2

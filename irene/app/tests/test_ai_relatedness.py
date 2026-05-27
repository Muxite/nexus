"""AIClient.relatedness() tests — every branch exercised against an
in-memory DB. The OpenRouter HTTP layer is mocked at the
`_call_openrouter_relatedness` boundary; budget pause-flag is mocked
at the budget instance.

The relatedness call:
- Returns RelatednessResult; never raises.
- Caches via `llm_calls.prompt_sha` so repeats skip the HTTP call.
- Returns `related=False` (conservative) on budget pause, 402, non-2xx,
  network error, or parse error.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional

import pytest

import app.ai  # noqa: F401  — DDL registration side effect
import app.main  # noqa: F401  — cycles DDL
from app.ai import (
    AIClient,
    RelatednessResult,
    RELATEDNESS_PROMPT_VERSION,
    _parse_relatedness_response,
)
from app.db import DB


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


@dataclass
class _FakeBudget:
    paused: bool = False
    ok_count: int = 0
    f402_count: int = 0

    def is_paused(self) -> bool:
        return self.paused

    def note_ok(self) -> None:
        self.ok_count += 1

    def note_402(self) -> None:
        self.f402_count += 1


def _fake_config() -> object:
    class _OR:
        api_key = "test-key"
        direction_model = "google/gemini-2.5-flash"
        timeout_seconds = 5.0
        max_input_tokens = 4000

    class _Cfg:
        openrouter = _OR()

    return _Cfg()


def _ok_response(related: bool, confidence: float, reason: str) -> dict:
    body = {
        "choices": [{"message": {"content": json.dumps({
            "related": related,
            "confidence": confidence,
            "reason": reason,
        })}}],
        "usage": {"prompt_tokens": 200, "completion_tokens": 20},
    }
    return {
        "status_code": 200,
        "body": json.dumps(body),
        "latency_ms": 10.0,
        "tokens_in": 200,
        "tokens_out": 20,
        "error": None,
    }


def _article() -> dict:
    return {
        "id": 42,
        "title": "Fed cuts rates by 50 bps",
        "body": "The Federal Reserve cut its benchmark interest rate by "
                "half a percentage point at today's policy meeting, "
                "citing softening labor markets.",
    }


def _market() -> dict:
    return {
        "condition_id": "0xmarket",
        "question": "Will the Fed cut rates at the September meeting?",
        "description": "Resolves YES if the Federal Reserve announces a "
                       "rate cut at its September FOMC meeting.",
        "end_date": "2026-09-30T00:00:00+00:00",
    }


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_parse_relatedness_happy():
    assert _parse_relatedness_response(
        '{"related": true, "confidence": 0.9, "reason": "direct"}'
    ) == (True, 0.9, "direct")
    assert _parse_relatedness_response(
        '{"related": false, "confidence": 0.1, "reason": "off-topic"}'
    ) == (False, 0.1, "off-topic")


def test_parse_relatedness_tolerates_fences_and_aliases():
    # Markdown fences
    assert _parse_relatedness_response(
        '```json\n{"related": true, "confidence": 0.7, "reason": ""}\n```'
    ) == (True, 0.7, "")
    # String boolean
    assert _parse_relatedness_response(
        '{"related": "yes", "confidence": 0.6, "reason": "x"}'
    ) == (True, 0.6, "x")
    assert _parse_relatedness_response(
        '{"related": "no", "confidence": 0.0, "reason": ""}'
    ) == (False, 0.0, "")
    # Numeric boolean
    assert _parse_relatedness_response(
        '{"related": 1, "confidence": 0.8}'
    ) == (True, 0.8, "")
    # Missing confidence → default 0.5 on True, 0.0 on False
    rel, conf, _ = _parse_relatedness_response(
        '{"related": true, "reason": "ok"}'
    )
    assert rel is True and conf == 0.5
    rel, conf, _ = _parse_relatedness_response('{"related": false}')
    assert rel is False and conf == 0.0
    # Out-of-range confidence clamps to [0, 1]
    assert _parse_relatedness_response(
        '{"related": true, "confidence": 5.0}'
    )[1] == 1.0
    assert _parse_relatedness_response(
        '{"related": true, "confidence": -0.5}'
    )[1] == 0.0


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "   ",
        "not json at all",
        '{"confidence": 0.5}',  # missing related
        '{"related": "maybe", "confidence": 0.5}',  # unknown string
        '{"related": [1, 2]}',  # wrong type
        "[]",  # not a dict
    ],
)
def test_parse_relatedness_rejects_malformed(raw):
    assert _parse_relatedness_response(raw) is None


# ---------------------------------------------------------------------------
# relatedness() — full path
# ---------------------------------------------------------------------------


@pytest.fixture
async def db_ready():
    db = DB.ephemeral()
    await db.connect()
    try:
        yield db
    finally:
        await db.close()


@pytest.fixture
async def ai(db_ready, monkeypatch):
    budget = _FakeBudget()
    client = AIClient(db=db_ready, budget=budget, config=_fake_config())
    client._mock_response = None

    async def stub_call(*, full_prompt, model, max_tokens):
        if client._mock_response is None:
            raise AssertionError("_mock_response not set for this test")
        client._last_call = {
            "full_prompt": full_prompt, "model": model, "max_tokens": max_tokens,
        }
        return client._mock_response

    monkeypatch.setattr(
        client, "_call_openrouter_relatedness", stub_call
    )
    try:
        yield client, budget
    finally:
        await client.close()


async def test_relatedness_happy_path_writes_llm_call(ai, db_ready):
    client, budget = ai
    client._mock_response = _ok_response(True, 0.85, "Fed cut directly resolves")

    result = await client.relatedness(
        article=_article(), market=_market(),
    )

    assert isinstance(result, RelatednessResult)
    assert result.related is True
    assert result.confidence == pytest.approx(0.85)
    assert result.reason == "Fed cut directly resolves"
    assert result.cached is False
    assert result.prompt_hash  # non-empty sha1
    assert budget.ok_count == 1

    cur = await db_ready.conn.execute(
        "SELECT model, prompt_version, prompt_sha FROM llm_calls"
    )
    row = await cur.fetchone()
    assert row["model"] == "google/gemini-2.5-flash"
    assert row["prompt_version"] == RELATEDNESS_PROMPT_VERSION
    assert row["prompt_sha"] == result.prompt_hash


async def test_relatedness_returns_false_on_budget_pause(ai, db_ready):
    client, budget = ai
    budget.paused = True
    # _mock_response intentionally unset — must not be called.

    result = await client.relatedness(
        article=_article(), market=_market(),
    )

    assert result.related is False
    assert result.reason == "budget_paused"
    assert result.cached is False
    # No HTTP call → no llm_calls row.
    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM llm_calls")
    assert (await cur.fetchone())[0] == 0


async def test_relatedness_returns_false_on_402(ai, db_ready):
    client, budget = ai
    client._mock_response = {
        "status_code": 402, "body": "credits", "latency_ms": 1.0,
        "tokens_in": None, "tokens_out": None, "error": None,
    }

    result = await client.relatedness(
        article=_article(), market=_market(),
    )

    assert result.related is False
    assert result.reason == "402_credits"
    assert budget.f402_count == 1
    # 402 still writes the llm_calls row (for diagnostics).
    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM llm_calls")
    assert (await cur.fetchone())[0] == 1


async def test_relatedness_returns_false_on_parse_error(ai, db_ready):
    client, budget = ai
    client._mock_response = {
        "status_code": 200,
        "body": json.dumps({
            "choices": [{"message": {"content": "not json"}}],
            "usage": {"prompt_tokens": 100, "completion_tokens": 5},
        }),
        "latency_ms": 1.0, "tokens_in": 100, "tokens_out": 5, "error": None,
    }

    result = await client.relatedness(
        article=_article(), market=_market(),
    )

    assert result.related is False
    assert result.reason == "parse_error"
    # Budget NOT noted ok on parse error.
    assert budget.ok_count == 0


async def test_relatedness_cache_hit_skips_http_call(ai, db_ready):
    client, budget = ai
    client._mock_response = _ok_response(True, 0.7, "ok")

    result1 = await client.relatedness(
        article=_article(), market=_market(),
    )
    assert result1.cached is False

    # Second call: blow up if the stub is invoked.
    client._mock_response = None
    result2 = await client.relatedness(
        article=_article(), market=_market(),
    )

    assert result2.cached is True
    assert result2.related is True
    assert result2.confidence == pytest.approx(0.7)
    assert result2.reason == "ok"
    assert result2.prompt_hash == result1.prompt_hash
    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM llm_calls")
    assert (await cur.fetchone())[0] == 1


async def test_relatedness_uses_explorer_model_param(ai):
    client, _ = ai
    client._mock_response = _ok_response(False, 0.0, "n/a")

    await client.relatedness(
        article=_article(), market=_market(),
        model="anthropic/claude-haiku-4-5",
        max_output_tokens=120,
    )

    assert client._last_call["model"] == "anthropic/claude-haiku-4-5"
    assert client._last_call["max_tokens"] == 120


async def test_relatedness_returns_false_on_non_2xx(ai, db_ready):
    client, _ = ai
    client._mock_response = {
        "status_code": 503, "body": "down", "latency_ms": 1.0,
        "tokens_in": None, "tokens_out": None, "error": None,
    }

    result = await client.relatedness(
        article=_article(), market=_market(),
    )

    assert result.related is False
    assert result.reason == "http_status_503"

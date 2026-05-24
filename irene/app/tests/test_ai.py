"""AIClient tests — every branch of `direction()` exercised against an
in-memory DB. The OpenRouter HTTP layer is mocked; budget pause-flag I/O
is mocked at the budget instance."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Optional

import httpx
import pytest

# Importing app.ai also registers its DDL with db_module.
import app.ai  # noqa: F401 — DDL registration side effect
from app.ai import AIClient, DirectionResult, _parse_response
from app.cycle import cycle_scope
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


def _fake_config(model: str = "google/gemini-2.5-flash") -> object:
    """Minimal Config-shaped object; AIClient only reads config.openrouter.*."""

    class _OR:
        api_key = "test-key"
        direction_model = model
        timeout_seconds = 5.0

    class _Cfg:
        openrouter = _OR()

    return _Cfg()


def _ok_response(direction: str = "YES", confidence: float = 1.0) -> dict:
    """OpenRouter-shaped 2xx body. v2-fast: content is a single char.

    The `confidence` arg is kept for test-call ergonomics but is ignored
    in the new regime — the parser fixes 1.0 for Y/N and 0.0 for U.
    Defaults to YES @ 1.0 to match the most common test shape.
    """
    inner_map = {"YES": "Y", "NO": "N", "NEUTRAL": "U"}
    content = inner_map.get(direction, direction[:1].upper())
    return {
        "status_code": 200,
        "body": json.dumps(
            {
                "choices": [{"message": {"content": content}}],
                "usage": {"prompt_tokens": 50, "completion_tokens": 1},
            }
        ),
        "latency_ms": 12.3,
        "tokens_in": 50,
        "tokens_out": 1,
        "error": None,
    }


# Importing main.py also registers the cycles DDL.
import app.main  # noqa: E402, F401


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
    # Inject a controllable mock for _call_openrouter; default raises to
    # catch any test that forgets to set its own response.
    client._mock_response = None  # set per-test

    async def stub_call(full_prompt: str) -> dict:
        if client._mock_response is None:
            raise AssertionError("_mock_response not set for this test")
        return client._mock_response

    monkeypatch.setattr(client, "_call_openrouter", stub_call)
    try:
        yield client, budget
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_parse_response_happy():
    # v3-fences-tolerant single-char outputs.
    assert _parse_response("Y") == ("YES", 1.0)
    assert _parse_response("N") == ("NO", 1.0)
    assert _parse_response("U") == ("NEUTRAL", 0.0)
    # Case-insensitive + tolerates whitespace.
    assert _parse_response("y") == ("YES", 1.0)
    assert _parse_response(" Y ") == ("YES", 1.0)
    assert _parse_response("u\n") == ("NEUTRAL", 0.0)
    # First character wins (provider sometimes emits trailing text).
    assert _parse_response("Yes, definitely") == ("YES", 1.0)
    # v3: tolerates markdown code fences (Gemini-2.5-flash prefixes ```).
    assert _parse_response("```Y") == ("YES", 1.0)
    assert _parse_response("```N\n```") == ("NO", 1.0)
    assert _parse_response("```\nU\n```") == ("NEUTRAL", 0.0)
    assert _parse_response("```u") == ("NEUTRAL", 0.0)
    # v4: JSON answer envelope per the new prompt contract.
    assert _parse_response('{"a":"Y"}') == ("YES", 1.0)
    assert _parse_response('{"a":"N"}') == ("NO", 1.0)
    assert _parse_response('{"a":"U"}') == ("NEUTRAL", 0.0)
    assert _parse_response('```json\n{"a":"Y"}\n```') == ("YES", 1.0)
    assert _parse_response('```json\n{"answer":"Y"}\n```') == ("YES", 1.0)
    assert _parse_response('{"answer":"NO"}') == ("NO", 1.0)


@pytest.mark.parametrize(
    "raw",
    [
        "",                                                   # empty
        "  ",                                                 # whitespace-only
        "X",                                                  # neither Y/N/U
        "Maybe",                                              # M
        "{",                                                  # punctuation
        "\n\n",                                               # only whitespace
    ],
)
def test_parse_response_rejects_malformed(raw):
    assert _parse_response(raw) is None


# ---------------------------------------------------------------------------
# direction() — branches
# ---------------------------------------------------------------------------


async def test_direction_happy_path_writes_call_and_prediction(ai, db_ready):
    client, budget = ai
    client._mock_response = _ok_response("YES", 0.7)
    with cycle_scope(1):
        result = await client.direction(signal_id=10, prompt="news X market Y")
    assert isinstance(result, DirectionResult)
    assert result.direction == "YES"
    # v2-fast: confidence is fixed (Y/N → 1.0, U → 0.0). The arg to
    # _ok_response is preserved for ergonomics but no longer round-trips.
    assert result.confidence == 1.0
    assert result.cached is False
    assert budget.ok_count == 1
    assert budget.f402_count == 0

    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM llm_calls")
    assert (await cur.fetchone())[0] == 1
    cur = await db_ready.conn.execute("SELECT direction, confidence, cached FROM predictions WHERE signal_id=10")
    row = await cur.fetchone()
    assert row["direction"] == "YES"
    assert row["confidence"] == 1.0
    assert row["cached"] == 0
    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM llm_parse_failures")
    assert (await cur.fetchone())[0] == 0


async def test_direction_second_call_same_prompt_is_cache_hit(ai, db_ready):
    client, budget = ai
    client._mock_response = _ok_response("NO", 0.55)
    with cycle_scope(1):
        first = await client.direction(signal_id=20, prompt="P")
        # Set the mock to something *different* so we can verify it wasn't called.
        client._mock_response = _ok_response("YES", 0.99)
        second = await client.direction(signal_id=21, prompt="P")
    assert first.direction == "NO"
    assert first.cached is False
    assert second.direction == "NO"           # cache served the original
    assert second.confidence == 1.0           # v2-fast: NO → 1.0 fixed
    assert second.cached is True

    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM llm_calls")
    assert (await cur.fetchone())[0] == 1     # exactly one llm_calls row
    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM predictions")
    assert (await cur.fetchone())[0] == 2     # two predictions, one cached


async def test_direction_budget_paused_short_circuits(ai, db_ready):
    client, budget = ai
    budget.paused = True
    # Set a mock that would explode the assertion if called.
    client._mock_response = _ok_response("YES", 0.99)
    with cycle_scope(1):
        result = await client.direction(signal_id=30, prompt="P")
    assert result.direction == "NEUTRAL"
    assert result.confidence == 0.0
    assert result.cached is False
    cur = await db_ready.conn.execute("SELECT reason FROM llm_parse_failures WHERE signal_id=30")
    row = await cur.fetchone()
    assert row["reason"] == "budget_paused"
    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM llm_calls")
    assert (await cur.fetchone())[0] == 0


async def test_direction_402_triggers_note_402_and_neutral(ai, db_ready):
    client, budget = ai
    client._mock_response = {
        "status_code": 402,
        "body": '{"error":"Insufficient credits"}',
        "latency_ms": 1.0,
        "tokens_in": None,
        "tokens_out": None,
        "error": None,
    }
    with cycle_scope(1):
        result = await client.direction(signal_id=40, prompt="P")
    assert result.direction == "NEUTRAL"
    assert budget.f402_count == 1
    assert budget.ok_count == 0
    cur = await db_ready.conn.execute("SELECT reason FROM llm_parse_failures WHERE signal_id=40")
    row = await cur.fetchone()
    assert row["reason"] == "402_credits"


async def test_direction_parse_error_yields_neutral(ai, db_ready):
    client, budget = ai
    # 200 status but the content is not the JSON we asked for.
    client._mock_response = {
        "status_code": 200,
        "body": json.dumps({"choices": [{"message": {"content": "absolutely not json"}}]}),
        "latency_ms": 1.0,
        "tokens_in": 5,
        "tokens_out": 3,
        "error": None,
    }
    with cycle_scope(1):
        result = await client.direction(signal_id=50, prompt="P")
    assert result.direction == "NEUTRAL"
    cur = await db_ready.conn.execute("SELECT reason FROM llm_parse_failures WHERE signal_id=50")
    row = await cur.fetchone()
    assert row["reason"] == "parse_error"
    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM llm_calls")
    assert (await cur.fetchone())[0] == 1     # still recorded the call


async def test_direction_http_error_yields_neutral(ai, db_ready):
    client, budget = ai
    client._mock_response = {
        "status_code": 0,
        "body": "",
        "latency_ms": 1.0,
        "tokens_in": None,
        "tokens_out": None,
        "error": "http_error: ConnectError",
    }
    with cycle_scope(1):
        result = await client.direction(signal_id=60, prompt="P")
    assert result.direction == "NEUTRAL"
    cur = await db_ready.conn.execute("SELECT reason FROM llm_parse_failures WHERE signal_id=60")
    row = await cur.fetchone()
    assert row["reason"] == "http_error: ConnectError"


async def test_direction_tracer_flag_threaded_to_owned_tables(ai, db_ready):
    client, budget = ai
    client._mock_response = _ok_response("YES", 0.8)
    with cycle_scope(1):
        await client.direction(
            signal_id=70, prompt="P", is_tracer=True, tracer_id="t-abc"
        )
    cur = await db_ready.conn.execute(
        "SELECT is_tracer, tracer_id FROM llm_calls WHERE id=(SELECT MAX(id) FROM llm_calls)"
    )
    row = await cur.fetchone()
    assert row["is_tracer"] == 1
    assert row["tracer_id"] == "t-abc"
    cur = await db_ready.conn.execute(
        "SELECT is_tracer, tracer_id FROM predictions WHERE signal_id=70"
    )
    row = await cur.fetchone()
    assert row["is_tracer"] == 1
    assert row["tracer_id"] == "t-abc"


async def test_direction_unset_cycle_raises(ai, db_ready):
    client, budget = ai
    client._mock_response = _ok_response("YES", 0.5)
    # No cycle_scope — should LookupError out.
    with pytest.raises(LookupError):
        await client.direction(signal_id=80, prompt="P")

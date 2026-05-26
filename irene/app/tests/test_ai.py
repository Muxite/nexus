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
    # v6-strict: returns (direction, confidence, label). Default conf 0.80 for Y/N
    # without a digit; P/U/R always 0.0.
    assert _parse_response("Y") == ("YES", 0.80, "Y")
    assert _parse_response("N") == ("NO", 0.80, "N")
    assert _parse_response("U") == ("NEUTRAL", 0.0, "U")
    assert _parse_response("P") == ("NEUTRAL", 0.0, "P")
    assert _parse_response("R") == ("NEUTRAL", 0.0, "R")
    # Case-insensitive + tolerates whitespace.
    assert _parse_response("y") == ("YES", 0.80, "Y")
    assert _parse_response(" Y ") == ("YES", 0.80, "Y")
    assert _parse_response("u\n") == ("NEUTRAL", 0.0, "U")
    # v3: markdown code fences stripped first.
    assert _parse_response("```Y") == ("YES", 0.80, "Y")
    assert _parse_response("```N\n```") == ("NO", 0.80, "N")
    assert _parse_response("```\nU\n```") == ("NEUTRAL", 0.0, "U")
    # v4: JSON answer envelope.
    assert _parse_response('{"a":"Y"}') == ("YES", 0.80, "Y")
    assert _parse_response('{"a":"N"}') == ("NO", 0.80, "N")
    assert _parse_response('{"a":"P"}') == ("NEUTRAL", 0.0, "P")
    assert _parse_response('{"a":"U"}') == ("NEUTRAL", 0.0, "U")
    assert _parse_response('{"a":"R"}') == ("NEUTRAL", 0.0, "R")
    # JSON with full-word values (the v5-bug regression test):
    # {"decision":"NEUTRAL"} MUST map to U/NEUTRAL, NOT to N/NO.
    assert _parse_response('{"decision":"NEUTRAL"}') == ("NEUTRAL", 0.0, "U")
    assert _parse_response('{"resolution":"NEUTRAL"}') == ("NEUTRAL", 0.0, "U")
    assert _parse_response('{"answer":"YES"}') == ("YES", 0.80, "Y")
    assert _parse_response('{"answer":"NO"}') == ("NO", 0.80, "N")
    assert _parse_response('{"a":"PASS"}') == ("NEUTRAL", 0.0, "P")
    # Fenced wrap still works.
    assert _parse_response('```json\n{"a":"Y"}\n```') == ("YES", 0.80, "Y")
    # Multi-char garbage values reject (NOT classified by first char).
    assert _parse_response('{"a":"Maybe"}') is None
    assert _parse_response('{"a":"YESsir"}') is None
    # Bare strings: exact match or word required (no first-char scan).
    assert _parse_response("YES") == ("YES", 0.80, "Y")
    assert _parse_response("Maybe") is None
    assert _parse_response("Yes, definitely") is None


def test_parse_response_confidence_digit():
    # c=9 → conf=0.95 (max), c=0 → conf=0.55 (min after clamp).
    assert _parse_response('{"a":"Y","c":9}') == ("YES", 0.95, "Y")
    assert _parse_response('{"a":"Y","c":0}') == ("NEUTRAL", 0.0, "Y") or \
           _parse_response('{"a":"Y","c":0}')[1] == pytest.approx(0.55)
    # c=5 → conf=0.75
    d, conf, label = _parse_response('{"a":"N","c":5}')
    assert d == "NO" and label == "N" and conf == pytest.approx(0.75)
    # c missing on Y/N → default 0.80
    assert _parse_response('{"a":"Y"}')[1] == 0.80
    # P/U/R ignore the digit entirely (always conf=0.0)
    assert _parse_response('{"a":"P","c":9}') == ("NEUTRAL", 0.0, "P")
    # Out-of-range digit → default 0.80
    assert _parse_response('{"a":"Y","c":99}')[1] == 0.80
    assert _parse_response('{"a":"Y","c":-1}')[1] == 0.80
    # Garbage digit type → default 0.80
    assert _parse_response('{"a":"Y","c":"high"}')[1] == 0.80


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
    # v6-strict: Y/N default confidence is 0.80 (no 'c' digit in response).
    # P/U/R fixed at 0.0. The arg to _ok_response is ignored at parse time.
    assert result.confidence == 0.80
    assert result.cached is False
    assert budget.ok_count == 1
    assert budget.f402_count == 0

    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM llm_calls")
    assert (await cur.fetchone())[0] == 1
    cur = await db_ready.conn.execute("SELECT direction, confidence, cached FROM predictions WHERE signal_id=10")
    row = await cur.fetchone()
    assert row["direction"] == "YES"
    assert row["confidence"] == 0.80
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
    assert second.confidence == 0.80          # v6-strict: NO default conf 0.80
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


# ---------------------------------------------------------------------------
# Cycle 14: A/B probe lane
# ---------------------------------------------------------------------------


def _fake_config_with_probe(
    *,
    probe_enabled: bool,
    probe_sample_rate: float = 1.0,
    probe_providers: list[str] | None = None,
    probe_timeout_seconds: float = 10.0,
    model: str = "google/gemini-2.5-flash",
) -> object:
    """Variant of _fake_config that includes the cycle-14 ProbeSection
    fields. AIClient reads these via getattr on `config.probe.*`."""

    class _OR:
        api_key = "test-key"
        direction_model = model
        timeout_seconds = 5.0

    class _Probe:
        pass

    pr = _Probe()
    pr.probe_enabled = probe_enabled
    pr.probe_sample_rate = probe_sample_rate
    pr.probe_providers = list(probe_providers or [])
    pr.probe_timeout_seconds = probe_timeout_seconds

    class _Cfg:
        openrouter = _OR()
        probe = pr

    return _Cfg()


async def _drain_probe_tasks(client: AIClient) -> None:
    """Await all outstanding fire-and-forget probe tasks so assertions
    on the `llm_probe_calls` table observe the writes."""
    if client._probe_tasks:
        await asyncio.gather(*list(client._probe_tasks), return_exceptions=True)


async def test_probe_disabled_by_default(db_ready, monkeypatch):
    """Default config has probe_enabled=False → no probe rows are written
    even when probe_providers is non-empty."""
    budget = _FakeBudget()
    config = _fake_config_with_probe(
        probe_enabled=False,
        probe_sample_rate=1.0,
        probe_providers=["anthropic/claude-haiku-4.5"],
    )
    client = AIClient(db=db_ready, budget=budget, config=config)
    client._mock_response = _ok_response("YES")

    async def stub_call(full_prompt: str) -> dict:
        return client._mock_response

    monkeypatch.setattr(client, "_call_openrouter", stub_call)

    # Probe path should never execute; assert by patching to raise.
    async def boom(*_a, **_kw):
        raise AssertionError("probe should not run when probe_enabled=False")

    monkeypatch.setattr(client, "_call_probe_provider", boom)

    try:
        with cycle_scope(1):
            await client.direction(signal_id=100, prompt="news A market B")
        await _drain_probe_tasks(client)
    finally:
        await client.close()

    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM llm_probe_calls")
    assert (await cur.fetchone())[0] == 0


async def test_probe_writes_one_row_per_provider(db_ready, monkeypatch):
    """probe_enabled=True, sample_rate=1.0, 2 providers → 2 probe rows
    per production call. Mock the probe HTTP layer so no real provider
    is hit."""
    budget = _FakeBudget()
    providers = [
        "anthropic/claude-haiku-4.5",
        "deepseek/deepseek-chat-v3",
    ]
    config = _fake_config_with_probe(
        probe_enabled=True,
        probe_sample_rate=1.0,
        probe_providers=providers,
    )
    client = AIClient(db=db_ready, budget=budget, config=config)
    client._mock_response = _ok_response("YES")

    async def stub_call(full_prompt: str) -> dict:
        return client._mock_response

    monkeypatch.setattr(client, "_call_openrouter", stub_call)

    calls: list[str] = []

    async def fake_probe(provider: str, full_prompt: str) -> dict:
        calls.append(provider)
        return {
            "ttft_ms": 42.0,
            "total_ms": 100.0,
            "direction": "YES",
            "confidence": 1.0,
            "raw_response": "Y",
            "error": None,
        }

    monkeypatch.setattr(client, "_call_probe_provider", fake_probe)

    try:
        with cycle_scope(1):
            await client.direction(signal_id=110, prompt="news X market Y")
        await _drain_probe_tasks(client)
    finally:
        await client.close()

    assert sorted(calls) == sorted(providers)
    cur = await db_ready.conn.execute(
        "SELECT provider, ttft_ms, total_ms, direction, confidence, error "
        "FROM llm_probe_calls ORDER BY provider"
    )
    rows = await cur.fetchall()
    assert len(rows) == 2
    assert {r["provider"] for r in rows} == set(providers)
    for r in rows:
        assert r["ttft_ms"] == 42.0
        assert r["total_ms"] == 100.0
        assert r["direction"] == "YES"
        assert r["confidence"] == 1.0
        assert r["error"] is None


async def test_probe_failure_records_error_row(db_ready, monkeypatch):
    """One probe provider times out → row written with error='timeout'
    and metric fields NULL. The other provider succeeds normally."""
    budget = _FakeBudget()
    providers = [
        "anthropic/claude-haiku-4.5",  # will time out
        "deepseek/deepseek-chat-v3",   # will succeed
    ]
    config = _fake_config_with_probe(
        probe_enabled=True,
        probe_sample_rate=1.0,
        probe_providers=providers,
    )
    client = AIClient(db=db_ready, budget=budget, config=config)
    client._mock_response = _ok_response("YES")

    async def stub_call(full_prompt: str) -> dict:
        return client._mock_response

    monkeypatch.setattr(client, "_call_openrouter", stub_call)

    async def selective_probe(provider: str, full_prompt: str) -> dict:
        if provider == "anthropic/claude-haiku-4.5":
            return {
                "ttft_ms": None,
                "total_ms": 10_000.0,
                "direction": None,
                "confidence": None,
                "raw_response": None,
                "error": "timeout",
            }
        return {
            "ttft_ms": 55.0,
            "total_ms": 120.0,
            "direction": "YES",
            "confidence": 1.0,
            "raw_response": "Y",
            "error": None,
        }

    monkeypatch.setattr(client, "_call_probe_provider", selective_probe)

    try:
        with cycle_scope(1):
            await client.direction(signal_id=120, prompt="news P market Q")
        await _drain_probe_tasks(client)
    finally:
        await client.close()

    cur = await db_ready.conn.execute(
        "SELECT provider, ttft_ms, total_ms, direction, confidence, error "
        "FROM llm_probe_calls ORDER BY provider"
    )
    rows = await cur.fetchall()
    assert len(rows) == 2
    by_provider = {r["provider"]: r for r in rows}
    haiku = by_provider["anthropic/claude-haiku-4.5"]
    assert haiku["error"] == "timeout"
    assert haiku["ttft_ms"] is None
    assert haiku["direction"] is None
    assert haiku["confidence"] is None
    deepseek = by_provider["deepseek/deepseek-chat-v3"]
    assert deepseek["error"] is None
    assert deepseek["direction"] == "YES"
    assert deepseek["confidence"] == 1.0


async def test_probe_does_not_change_production_call(db_ready, monkeypatch):
    """The DirectionResult returned by direction() is identical whether
    probe_enabled is True or False (for the same inputs and same mocked
    production response). The probe is a pure side-effect lane."""
    budget_off = _FakeBudget()
    budget_on = _FakeBudget()
    mock_resp = _ok_response("YES")

    async def stub(full_prompt: str) -> dict:
        return mock_resp

    cfg_off = _fake_config_with_probe(probe_enabled=False)
    cfg_on = _fake_config_with_probe(
        probe_enabled=True,
        probe_sample_rate=1.0,
        probe_providers=["anthropic/claude-haiku-4.5"],
    )

    client_off = AIClient(db=db_ready, budget=budget_off, config=cfg_off)
    client_on = AIClient(db=db_ready, budget=budget_on, config=cfg_on)
    monkeypatch.setattr(client_off, "_call_openrouter", stub)
    monkeypatch.setattr(client_on, "_call_openrouter", stub)

    async def fake_probe(provider, full_prompt):
        return {
            "ttft_ms": 1.0,
            "total_ms": 2.0,
            "direction": "NO",  # deliberately disagree — must NOT leak
            "confidence": 1.0,
            "raw_response": "N",
            "error": None,
        }

    monkeypatch.setattr(client_on, "_call_probe_provider", fake_probe)

    try:
        with cycle_scope(1):
            # Different signal_ids so the per-signal predictions UNIQUE
            # doesn't dedupe; same prompt so the llm_calls cache fires
            # on the second call (which is fine — the comparison is on
            # the DirectionResult shape, not on cached vs fresh).
            off_result = await client_off.direction(signal_id=130, prompt="P")
            on_result = await client_on.direction(signal_id=131, prompt="P")
        await _drain_probe_tasks(client_on)
    finally:
        await client_off.close()
        await client_on.close()

    # Identical user-visible direction + confidence + raw_response + label.
    assert off_result.direction == on_result.direction == "YES"
    assert off_result.confidence == on_result.confidence
    assert off_result.label == on_result.label
    assert off_result.raw_response == on_result.raw_response
    # Probe row exists for the on-client only.
    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM llm_probe_calls")
    assert (await cur.fetchone())[0] == 1
    cur = await db_ready.conn.execute(
        "SELECT direction FROM llm_probe_calls LIMIT 1"
    )
    row = await cur.fetchone()
    # Probe's disagreeing label sits in the probe row, not in any
    # production-visible field.
    assert row["direction"] == "NO"


# ---------------------------------------------------------------------------
# Cycle 24 (handoff H): max_input_tokens knob + provider-level prompt
# truncation. The trade-stage prompt builder caps the article slice at
# ~3000 chars; this layer is the safety net for any future caller that
# builds an unbounded prompt. `max_tokens=1` + logit_bias (cycle-7 design)
# stays untouched — only INPUT context expands.
# ---------------------------------------------------------------------------


def _fake_config_with_input_cap(max_input_tokens: int) -> object:
    """Variant of `_fake_config` that exposes the cycle-24
    `max_input_tokens` field on `config.openrouter`."""

    class _OR:
        api_key = "test-key"
        direction_model = "google/gemini-2.5-flash"
        timeout_seconds = 5.0

    _OR.max_input_tokens = max_input_tokens

    class _Cfg:
        openrouter = _OR()

    return _Cfg()


async def test_h_default_max_input_tokens_when_unset(db_ready):
    """Tests using the legacy `_fake_config` (no `max_input_tokens`
    attribute) get the 4000-token default — preserves test fakes
    written before cycle 24."""
    budget = _FakeBudget()
    client = AIClient(db=db_ready, budget=budget, config=_fake_config())
    try:
        assert client._max_input_tokens == 4000
        assert client._max_input_chars == 16_000
    finally:
        await client.close()


async def test_h_max_input_tokens_read_from_config(db_ready):
    """A config-supplied `max_input_tokens` overrides the default and
    drives the char budget (chars = tokens × 4)."""
    budget = _FakeBudget()
    client = AIClient(
        db=db_ready,
        budget=budget,
        config=_fake_config_with_input_cap(max_input_tokens=2_000),
    )
    try:
        assert client._max_input_tokens == 2_000
        assert client._max_input_chars == 8_000
    finally:
        await client.close()


async def test_h_long_prompt_accepted_without_truncation(db_ready, monkeypatch):
    """A 3500-char prompt (well under the 16k default char cap) must
    pass through to the provider verbatim. Asserts the truncation
    branch does NOT fire on the normal cycle-24 enriched prompt size."""
    budget = _FakeBudget()
    client = AIClient(db=db_ready, budget=budget, config=_fake_config())
    captured: dict[str, str] = {}

    async def capture_call(full_prompt: str) -> dict:
        captured["prompt"] = full_prompt
        return _ok_response("YES")

    monkeypatch.setattr(client, "_call_openrouter", capture_call)
    try:
        long_prompt = "x" * 3_500
        with cycle_scope(1):
            await client.direction(signal_id=200, prompt=long_prompt)
    finally:
        await client.close()

    assert captured["prompt"] == long_prompt
    assert "truncated" not in captured["prompt"]
    # The single-token-output (cycle-7 / cycle-4) design must be preserved:
    # the stub we just used short-circuits the HTTP call, so the
    # observable here is that the prompt reached _call_openrouter intact.


async def test_h_oversized_prompt_truncated_at_char_cap(db_ready, monkeypatch):
    """A 100k-char prompt exceeds the 16k default char budget and must be
    truncated at the cap (with a marker). The stub still observes a
    well-formed call, so the cache-key/hashing pipeline downstream stays
    consistent."""
    budget = _FakeBudget()
    client = AIClient(db=db_ready, budget=budget, config=_fake_config())
    captured: dict[str, str] = {}

    async def capture_call(full_prompt: str) -> dict:
        captured["prompt"] = full_prompt
        return _ok_response("YES")

    monkeypatch.setattr(client, "_call_openrouter", capture_call)
    try:
        runaway = "y" * 100_000
        with cycle_scope(1):
            await client.direction(signal_id=201, prompt=runaway)
    finally:
        await client.close()

    sent = captured["prompt"]
    # Cap is max_input_chars (16_000) + the truncation marker tail.
    assert len(sent) <= 16_000 + 50, (
        f"sent prompt is {len(sent)} chars; expected <= ~16_050"
    )
    assert "truncated" in sent


async def test_h_output_max_tokens_unchanged_by_input_knob(db_ready, monkeypatch):
    """The cycle-7 single-token output cap (`max_tokens=32` + logit_bias
    on Y/N/P/U/R) must NOT change when handoff H raises the input budget.
    This locks the contract: handoff H expands INPUT only. We assert by
    inspecting the JSON payload the real `_call_openrouter` builds."""
    budget = _FakeBudget()
    client = AIClient(
        db=db_ready,
        budget=budget,
        config=_fake_config_with_input_cap(max_input_tokens=8_000),
    )
    seen: dict = {}

    class _StubResp:
        status_code = 200

        def __init__(self, body: str) -> None:
            self.text = body

        def json(self) -> dict:
            return json.loads(self.text)

    class _StubClient:
        def __init__(self) -> None:
            self.posted: list[dict] = []

        async def post(self, _url, *, headers, json):  # noqa: A002 — match httpx sig
            self.posted.append(json)
            seen["payload"] = json
            seen["headers"] = headers
            body = {
                "choices": [{"message": {"content": "Y"}}],
                "usage": {"prompt_tokens": 100, "completion_tokens": 1},
            }
            return _StubResp(json_lib.dumps(body))

        async def aclose(self) -> None:
            pass

    import json as json_lib  # noqa: F401 — used inside _StubClient
    client._client = _StubClient()
    try:
        with cycle_scope(1):
            result = await client.direction(
                signal_id=210, prompt="news A market B"
            )
    finally:
        await client.close()

    assert result.direction == "YES"
    # The protected contract: max_tokens and logit_bias are unchanged.
    # (Cycle 4 set max_tokens=32 to absorb gemini's ```json fence wrap.)
    assert seen["payload"]["max_tokens"] == 32
    assert seen["payload"]["temperature"] == 0
    lb = seen["payload"]["logit_bias"]
    assert set(lb.keys()) == {"Y", "N", "P", "U", "R"}
    assert all(v == 100 for v in lb.values())

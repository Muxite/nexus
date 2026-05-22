"""
Tests for A2.1 (2026-05-22) — `llm_calls` writer wired into
DirectionClassifier.classify_batch.

Contract pinned by these tests:
  - Every completed OpenRouter round-trip writes ONE `llm_calls` row.
  - Parse-failure samples write a row with `parse_ok=0` AND a paired
    `llm_parse_failures` row.
  - SEULGI_BYPASS_LLM=1 short-circuits BEFORE any HTTP — zero llm_calls rows.
  - response_text gets capped at 8 KB with the `[...truncated]` suffix.
  - `prompt_sha` is deterministic across identical prompts (replay key).
  - A logging failure is non-fatal — classify_batch returns results anyway.
  - `db=None` is a supported configuration (legacy / unit tests).
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
    DirectionClassifier,
    _LLM_CALL_RESPONSE_CAP,
    _LLM_CALL_TRUNC_SUFFIX,
    _truncate_response,
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


def _make_ok_response(p_yes=0.6, p_no=0.3, p_neu=0.1, p_alpha=0.7,
                     prompt_tokens=100, completion_tokens=50):
    body = {"results": [{
        "idx": 0,
        "p_yes": p_yes, "p_no": p_no, "p_neutral": p_neu,
        "p_alpha": p_alpha, "risk": 0.3, "reason": "mock",
    }]}
    return {
        "choices": [{"message": {"content": json.dumps(body)}}],
        "usage": {
            "total_tokens": prompt_tokens + completion_tokens,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
        },
    }


def _make_unparseable_response():
    """OpenRouter returns 200 but the content isn't valid JSON for our schema."""
    return {
        "choices": [{"message": {"content": "this is not json at all"}}],
        "usage": {
            "total_tokens": 42, "prompt_tokens": 30, "completion_tokens": 12,
        },
    }


class FakeDB:
    """Minimal in-memory stand-in for `Database` capturing the two writers
    classify_batch is expected to call. Synchronous in spirit but async on
    the surface to match the real DB contract."""

    def __init__(self) -> None:
        self.llm_calls: list[dict] = []
        self.llm_parse_failures: list[dict] = []
        # When set, raise on next log_llm_call to exercise the safe-wrap path.
        self.raise_on_next_call: bool = False

    async def log_llm_call(self, **kwargs):
        if self.raise_on_next_call:
            self.raise_on_next_call = False
            raise RuntimeError("simulated DB failure")
        self.llm_calls.append(kwargs)
        return len(self.llm_calls)

    async def log_llm_parse_failure(self, **kwargs):
        self.llm_parse_failures.append(kwargs)
        return len(self.llm_parse_failures)


def _mk_session(response_data, *, fail_n: int = 0):
    """Build an aiohttp.ClientSession mock that yields the same response on
    every POST. `fail_n` lets the first N calls return a non-200 instead, so
    we can simulate partial-batch failure on ensembles."""
    state = {"calls": 0}

    @asynccontextmanager
    async def _post(*args, **kwargs):
        state["calls"] += 1
        mock_resp = MagicMock()
        if state["calls"] <= fail_n:
            mock_resp.status = 500
            mock_resp.text = AsyncMock(return_value="oops")
            mock_resp.json = AsyncMock(return_value={})
        else:
            mock_resp.status = 200
            mock_resp.json = AsyncMock(return_value=response_data)
            mock_resp.text = AsyncMock(return_value="")
        yield mock_resp

    session = MagicMock()
    session.closed = False
    session.post = _post
    return session


@pytest.fixture(autouse=True)
def _clear_bypass(monkeypatch):
    monkeypatch.delenv("SEULGI_BYPASS_LLM", raising=False)
    ai._BYPASS_STARTUP_LOGGED = False
    yield
    ai._BYPASS_STARTUP_LOGGED = False


# ---------------------------------------------------------------------------
# Successful path: N rows, parse_ok=1, correct field plumbing
# ---------------------------------------------------------------------------


class TestSuccessfulBatchLogsRows:
    @pytest.mark.asyncio
    async def test_three_samples_three_rows_parse_ok_1(self):
        db = FakeDB()
        clf = DirectionClassifier(
            api_key="key", model="google/gemini-2.5-flash",
            ensemble_samples=3, db=db,
        )
        clf._session = _mk_session(_make_ok_response())

        results, meta = await clf.classify_batch(
            headline="big news", matches=[_mk_match(0.7)],
            headline_id="hid_abc",
        )

        assert len(results) == 1
        # Exactly one row per ensemble sample.
        assert len(db.llm_calls) == 3
        for row in db.llm_calls:
            assert row["parse_ok"] is True
            assert row["model"] == "google/gemini-2.5-flash"
            assert row["headline_id"] == "hid_abc"
            assert row["batch_size"] == 1
            assert row["prompt_tokens"] == 100
            assert row["completion_tokens"] == 50
            assert row["response_text"]  # non-empty
            # SHA-256 hex digest is 64 chars
            assert len(row["prompt_sha"]) == 64
        # No parse-failure side-table writes when everything parsed.
        assert db.llm_parse_failures == []

    @pytest.mark.asyncio
    async def test_n_equals_one_writes_one_row(self):
        db = FakeDB()
        clf = DirectionClassifier(
            api_key="key", model="m", ensemble_samples=1, db=db,
        )
        clf._session = _mk_session(_make_ok_response())
        await clf.classify_batch(headline="h", matches=[_mk_match(0.5)])
        assert len(db.llm_calls) == 1
        assert db.llm_calls[0]["parse_ok"] is True

    @pytest.mark.asyncio
    async def test_batch_size_reflects_n_matches(self):
        db = FakeDB()
        clf = DirectionClassifier(
            api_key="key", model="m", ensemble_samples=2, db=db,
        )
        # Build a response with 3 result objects so parser accepts it
        body = {"results": [
            {"idx": i, "p_yes": 0.5, "p_no": 0.3, "p_neutral": 0.2,
             "p_alpha": 0.6, "risk": 0.3, "reason": "r"}
            for i in range(3)
        ]}
        data = {"choices": [{"message": {"content": json.dumps(body)}}],
                "usage": {"total_tokens": 1, "prompt_tokens": 1,
                          "completion_tokens": 0}}
        clf._session = _mk_session(data)
        await clf.classify_batch(
            headline="h",
            matches=[_mk_match(0.6, condition_id=f"c{i}") for i in range(3)],
        )
        assert len(db.llm_calls) == 2  # ensemble_samples
        for row in db.llm_calls:
            assert row["batch_size"] == 3


# ---------------------------------------------------------------------------
# Parse-failure path: parse_ok=0 + paired llm_parse_failures row
# ---------------------------------------------------------------------------


class TestParseFailureWritesBothTables:
    @pytest.mark.asyncio
    async def test_parse_failure_writes_llm_calls_and_failures(self):
        db = FakeDB()
        clf = DirectionClassifier(
            api_key="key", model="m", ensemble_samples=3, db=db,
        )
        clf._session = _mk_session(_make_unparseable_response())

        results, _meta = await clf.classify_batch(
            headline="h", matches=[_mk_match(0.5)],
            headline_id="hid_fail",
        )

        # All N samples failed to parse → fallback path runs; main writer
        # still captured every attempt.
        assert len(db.llm_calls) == 3
        for row in db.llm_calls:
            assert row["parse_ok"] is False
            assert row["headline_id"] == "hid_fail"
        # Paired failure-mode rows: one per parse failure.
        assert len(db.llm_parse_failures) == 3
        for row in db.llm_parse_failures:
            assert row["error_msg"] == "batch_parse_failed"
            assert row["headline_id"] == "hid_fail"


# ---------------------------------------------------------------------------
# Bypass mode: zero rows
# ---------------------------------------------------------------------------


class TestBypassWritesZeroRows:
    @pytest.mark.asyncio
    async def test_bypass_writes_no_llm_calls_rows(self, monkeypatch):
        monkeypatch.setenv("SEULGI_BYPASS_LLM", "1")
        db = FakeDB()
        clf = DirectionClassifier(
            api_key="key", model="m", ensemble_samples=3, db=db,
        )

        async def _explode(*_a, **_kw):
            raise AssertionError("bypass must not call OpenRouter")

        clf._call_api = _explode  # type: ignore[assignment]

        results, meta = await clf.classify_batch(
            headline="x", matches=[_mk_match(0.7), _mk_match(0.3)],
            headline_id="hid_bypass",
        )

        assert len(results) == 2
        assert meta["bypass"] is True
        # No DB writes — bypass means no real LLM call was made.
        assert db.llm_calls == []
        assert db.llm_parse_failures == []


# ---------------------------------------------------------------------------
# Response truncation at 8 KB
# ---------------------------------------------------------------------------


class TestResponseTruncation:
    def test_short_text_passes_through_unchanged(self):
        text = "x" * 100
        assert _truncate_response(text, _LLM_CALL_RESPONSE_CAP) == text

    def test_long_text_capped_with_suffix(self):
        text = "y" * (_LLM_CALL_RESPONSE_CAP + 500)
        out = _truncate_response(text, _LLM_CALL_RESPONSE_CAP)
        assert len(out) == _LLM_CALL_RESPONSE_CAP
        assert out.endswith(_LLM_CALL_TRUNC_SUFFIX)
        # Body before the suffix is original prefix
        body = out[: -len(_LLM_CALL_TRUNC_SUFFIX)]
        assert body == "y" * len(body)

    def test_none_returns_empty(self):
        assert _truncate_response(None, _LLM_CALL_RESPONSE_CAP) == ""

    @pytest.mark.asyncio
    async def test_classify_batch_truncates_oversized_response(self):
        db = FakeDB()
        clf = DirectionClassifier(
            api_key="key", model="m", ensemble_samples=1, db=db,
        )
        # Build a valid JSON envelope whose content blows past 8 KB by
        # padding the `reason` field. The parser keeps `reason` short
        # internally but `_extract_text` returns the raw content string,
        # which is what we persist.
        long_reason = "z" * (_LLM_CALL_RESPONSE_CAP + 1000)
        body = {"results": [{
            "idx": 0, "p_yes": 0.5, "p_no": 0.3, "p_neutral": 0.2,
            "p_alpha": 0.6, "risk": 0.3, "reason": long_reason,
        }]}
        data = {"choices": [{"message": {"content": json.dumps(body)}}],
                "usage": {"total_tokens": 1, "prompt_tokens": 1,
                          "completion_tokens": 0}}
        clf._session = _mk_session(data)

        await clf.classify_batch(headline="h", matches=[_mk_match(0.5)])

        assert len(db.llm_calls) == 1
        row = db.llm_calls[0]
        assert len(row["response_text"]) == _LLM_CALL_RESPONSE_CAP
        assert row["response_text"].endswith(_LLM_CALL_TRUNC_SUFFIX)


# ---------------------------------------------------------------------------
# Prompt SHA determinism (replay key contract)
# ---------------------------------------------------------------------------


class TestPromptShaDeterministic:
    @pytest.mark.asyncio
    async def test_identical_inputs_yield_same_sha(self):
        db = FakeDB()
        clf = DirectionClassifier(
            api_key="key", model="m", ensemble_samples=2, db=db,
        )
        clf._session = _mk_session(_make_ok_response())

        m = _mk_match(0.5, condition_id="c1")
        await clf.classify_batch(headline="same", matches=[m])
        await clf.classify_batch(headline="same", matches=[m])

        # 2 batches × 2 ensemble samples = 4 rows.
        assert len(db.llm_calls) == 4
        shas = {row["prompt_sha"] for row in db.llm_calls}
        # All four prompts are byte-identical → single shared sha.
        assert len(shas) == 1

    @pytest.mark.asyncio
    async def test_different_headlines_yield_different_shas(self):
        db = FakeDB()
        clf = DirectionClassifier(
            api_key="key", model="m", ensemble_samples=1, db=db,
        )
        clf._session = _mk_session(_make_ok_response())

        await clf.classify_batch(headline="alpha", matches=[_mk_match(0.5)])
        await clf.classify_batch(headline="beta", matches=[_mk_match(0.5)])

        assert len(db.llm_calls) == 2
        assert db.llm_calls[0]["prompt_sha"] != db.llm_calls[1]["prompt_sha"]


# ---------------------------------------------------------------------------
# Robustness: db=None and logging failures don't break classify_batch
# ---------------------------------------------------------------------------


class TestRobustness:
    @pytest.mark.asyncio
    async def test_db_none_is_supported(self):
        """No DB → no logging, classify_batch still returns results."""
        clf = DirectionClassifier(
            api_key="key", model="m", ensemble_samples=2, db=None,
        )
        clf._session = _mk_session(_make_ok_response())
        results, _meta = await clf.classify_batch(
            headline="h", matches=[_mk_match(0.5)],
        )
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_logger_failure_does_not_block_trade_path(self):
        """A raise inside `db.log_llm_call` must be swallowed — the trade
        path returns the parsed results normally."""
        db = FakeDB()
        db.raise_on_next_call = True
        clf = DirectionClassifier(
            api_key="key", model="m", ensemble_samples=2, db=db,
        )
        clf._session = _mk_session(_make_ok_response())

        results, _meta = await clf.classify_batch(
            headline="h", matches=[_mk_match(0.5)],
        )

        assert len(results) == 1
        # First call raised (and was swallowed); second succeeded.
        assert len(db.llm_calls) == 1

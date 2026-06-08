"""Tests for app/budget.py — OpenRouter cap watcher.

Covers:
- Fresh instance: `is_paused()` is False, `remaining_usd` is None.
- 402-streak: 5 `note_402()` calls flip pause on; 5 `note_ok()` calls flip
  it back off; streak counter is capped.
- Poll loop: a mocked `/key` response with remaining below threshold flips
  pause on after one tick.
- Poll loop: a mocked `/key` recovery flips pause off and unlinks the flag.
- Pause-flag file: written on the false→true edge, deleted on the
  true→false edge.
- HTTP errors (httpx.ConnectError) during poll do not crash the loop, and
  do not clear an existing pause state.
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any

import httpx
import pytest

from app.budget import Budget


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_budget(tmp_path, **overrides) -> Budget:
    kwargs: dict[str, Any] = dict(
        api_key="sk-test",
        monthly_cap_usd=10.0,
        pause_at_remaining_usd=2.0,
        poll_seconds=60,
        pause_flag_path=str(tmp_path / "budget_paused.flag"),
    )
    kwargs.update(overrides)
    return Budget(**kwargs)


class _FakeResponse:
    """Mimics the bits of `httpx.Response` that `Budget._poll_once` touches."""

    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                "boom", request=None, response=None  # type: ignore[arg-type]
            )

    def json(self) -> dict:
        return self._payload


class _FakeClient:
    """Stands in for `httpx.AsyncClient`. The poll loop only calls `.get()`
    and `.aclose()`; we intercept both. URL-aware so one fake serves both the
    `/key` (monthly) and `/credits` (account self-heal) polls; `/credits`
    requests draw from `credits` if given, else fall back to `behaviours`."""

    def __init__(self, behaviours: list, credits: list | None = None) -> None:
        # Entries are either a dict (payload) or an Exception (raised on get()).
        self._behaviours = behaviours
        self._credits = credits
        self.calls = 0  # total gets (back-compat)
        self.key_calls = 0
        self.credits_calls = 0

    async def get(self, url: str, headers: dict | None = None) -> _FakeResponse:
        self.calls += 1
        if "/credits" in url:
            self.credits_calls += 1
            seq = self._credits if self._credits is not None else self._behaviours
            b = seq[min(self.credits_calls - 1, len(seq) - 1)]
        else:
            self.key_calls += 1
            b = self._behaviours[min(self.key_calls - 1, len(self._behaviours) - 1)]
        if isinstance(b, Exception):
            raise b
        return _FakeResponse(b)

    async def aclose(self) -> None:
        return None


# ---------------------------------------------------------------------------
# Fresh-instance defaults
# ---------------------------------------------------------------------------


def test_fresh_instance_is_not_paused(tmp_path):
    b = _make_budget(tmp_path)
    assert b.is_paused() is False
    assert b.remaining_usd is None
    assert not os.path.exists(b._pause_flag_path)


# ---------------------------------------------------------------------------
# 402-streak watcher
# ---------------------------------------------------------------------------


def test_five_consecutive_402s_pauses(tmp_path):
    b = _make_budget(tmp_path)
    for _ in range(4):
        b.note_402()
        assert b.is_paused() is False
    b.note_402()
    assert b.is_paused() is True
    assert os.path.exists(b._pause_flag_path)


def test_streak_clears_back_to_zero_unpauses(tmp_path):
    b = _make_budget(tmp_path)
    for _ in range(5):
        b.note_402()
    assert b.is_paused() is True
    for _ in range(5):
        b.note_ok()
    assert b.is_paused() is False
    assert not os.path.exists(b._pause_flag_path)


def test_streak_is_capped(tmp_path):
    b = _make_budget(tmp_path)
    for _ in range(500):
        b.note_402()
    # After capping, a bounded number of note_ok() calls must restore.
    for _ in range(100):
        b.note_ok()
    assert b.is_paused() is False


def test_note_ok_below_threshold_does_not_unpause_until_zero(tmp_path):
    # Hysteresis: streak in (0, 5) keeps prior state.
    b = _make_budget(tmp_path)
    for _ in range(5):
        b.note_402()
    assert b.is_paused() is True
    b.note_ok()  # streak now 4
    assert b.is_paused() is True
    for _ in range(3):
        b.note_ok()
    assert b.is_paused() is True  # streak now 1, still paused
    b.note_ok()  # streak now 0
    assert b.is_paused() is False


# ---------------------------------------------------------------------------
# Pause-flag file contents
# ---------------------------------------------------------------------------


def test_pause_flag_file_contents_on_pause(tmp_path):
    b = _make_budget(tmp_path)
    for _ in range(5):
        b.note_402()
    with open(b._pause_flag_path) as fh:
        payload = json.load(fh)
    assert payload["reason"] == "account_credits_zero"
    assert "paused_at" in payload
    assert "remaining_usd" in payload


# ---------------------------------------------------------------------------
# Poll loop
# ---------------------------------------------------------------------------


async def test_poll_pauses_when_remaining_below_threshold(tmp_path, monkeypatch):
    b = _make_budget(tmp_path, poll_seconds=3600)  # only ever poll once in test
    fake = _FakeClient([{"data": {"limit": 10.0, "usage": 9.5}}])
    # remaining = 0.5 < pause_at_remaining_usd = 2.0
    monkeypatch.setattr(
        "app.budget.httpx.AsyncClient", lambda *a, **kw: fake
    )
    await b.start()
    # Wait for first poll to complete.
    for _ in range(50):
        if fake.calls >= 1 and b.remaining_usd is not None:
            break
        await asyncio.sleep(0.02)
    await b.stop()
    assert b.remaining_usd == pytest.approx(0.5)
    assert b.is_paused() is True
    assert os.path.exists(b._pause_flag_path)
    with open(b._pause_flag_path) as fh:
        assert json.load(fh)["reason"] == "monthly_cap_near_zero"


async def test_poll_prefers_limit_remaining_over_lifetime_usage(tmp_path, monkeypatch):
    # Real `/key` payload: lifetime `usage` (169.92) is near the cap, but the
    # monthly allowance is full — `limit_remaining` must win so we don't
    # phantom-pause. (limit - usage would give 30.08; limit_remaining = 200.)
    b = _make_budget(tmp_path, poll_seconds=3600)
    fake = _FakeClient(
        [{"data": {"limit": 200.0, "usage": 169.92, "limit_remaining": 200.0}}]
    )
    monkeypatch.setattr("app.budget.httpx.AsyncClient", lambda *a, **kw: fake)
    await b.start()
    for _ in range(50):
        if fake.calls >= 1 and b.remaining_usd is not None:
            break
        await asyncio.sleep(0.02)
    await b.stop()
    assert b.remaining_usd == pytest.approx(200.0)
    assert b.is_paused() is False


async def test_poll_resumes_when_remaining_recovers(tmp_path, monkeypatch):
    b = _make_budget(tmp_path, poll_seconds=3600)
    fake = _FakeClient([{"data": {"limit": 10.0, "usage": 8.0}}])
    monkeypatch.setattr(
        "app.budget.httpx.AsyncClient", lambda *a, **kw: fake
    )
    # Pre-state: already paused via monthly source so the recovery is an edge.
    b._paused_monthly = True
    b._remaining_usd = 0.5
    b._write_flag()
    assert os.path.exists(b._pause_flag_path)
    await b.start()
    for _ in range(50):
        if fake.calls >= 1 and b.is_paused() is False:
            break
        await asyncio.sleep(0.02)
    await b.stop()
    assert b.is_paused() is False
    assert not os.path.exists(b._pause_flag_path)


async def test_poll_survives_http_error(tmp_path, monkeypatch):
    b = _make_budget(tmp_path, poll_seconds=3600)
    fake = _FakeClient([httpx.ConnectError("offline")])
    monkeypatch.setattr(
        "app.budget.httpx.AsyncClient", lambda *a, **kw: fake
    )
    # Pre-state: paused via credits streak. A network error must NOT clear
    # the pause (real cap-out + transient /key 5xx would silently unpause).
    for _ in range(5):
        b.note_402()
    assert b.is_paused() is True
    await b.start()
    for _ in range(50):
        if fake.calls >= 1:
            break
        await asyncio.sleep(0.02)
    await b.stop()
    assert fake.calls >= 1
    assert b.is_paused() is True
    assert b.remaining_usd is None  # never set on a failed poll


# ---------------------------------------------------------------------------
# Credits self-heal (account balance recovery while credits-paused)
# ---------------------------------------------------------------------------


async def test_credits_pause_self_heals_when_account_balance_recovers(
    tmp_path, monkeypatch
):
    b = _make_budget(tmp_path, poll_seconds=3600)
    # /key keeps the monthly source healthy; /credits shows a topped-up account.
    fake = _FakeClient(
        [{"data": {"limit": 200.0, "usage": 100.0}}],
        credits=[{"data": {"total_credits": 100.0, "total_usage": 50.0}}],
    )
    monkeypatch.setattr("app.budget.httpx.AsyncClient", lambda *a, **kw: fake)
    for _ in range(5):
        b.note_402()
    assert b.is_paused() is True
    assert os.path.exists(b._pause_flag_path)
    await b.start()
    for _ in range(50):
        if fake.credits_calls >= 1 and b.is_paused() is False:
            break
        await asyncio.sleep(0.02)
    await b.stop()
    assert b.is_paused() is False
    assert not os.path.exists(b._pause_flag_path)
    assert b._account_remaining_usd == pytest.approx(50.0)


async def test_credits_poll_does_not_unpause_while_account_still_exhausted(
    tmp_path, monkeypatch
):
    b = _make_budget(tmp_path, poll_seconds=3600)
    fake = _FakeClient(
        [{"data": {"limit": 200.0, "usage": 100.0}}],
        # total_usage > total_credits → balance still negative.
        credits=[{"data": {"total_credits": 100.0, "total_usage": 100.5}}],
    )
    monkeypatch.setattr("app.budget.httpx.AsyncClient", lambda *a, **kw: fake)
    for _ in range(5):
        b.note_402()
    assert b.is_paused() is True
    await b.start()
    for _ in range(50):
        if fake.credits_calls >= 1:
            break
        await asyncio.sleep(0.02)
    await b.stop()
    assert fake.credits_calls >= 1
    assert b.is_paused() is True
    assert os.path.exists(b._pause_flag_path)


async def test_credits_poll_error_keeps_pause(tmp_path, monkeypatch):
    b = _make_budget(tmp_path, poll_seconds=3600)
    # /key healthy, /credits errors — a failed probe must NOT unpause.
    fake = _FakeClient(
        [{"data": {"limit": 200.0, "usage": 100.0}}],
        credits=[httpx.ConnectError("offline")],
    )
    monkeypatch.setattr("app.budget.httpx.AsyncClient", lambda *a, **kw: fake)
    for _ in range(5):
        b.note_402()
    assert b.is_paused() is True
    await b.start()
    for _ in range(50):
        if fake.credits_calls >= 1:
            break
        await asyncio.sleep(0.02)
    await b.stop()
    assert fake.credits_calls >= 1
    assert b.is_paused() is True
    assert b._account_remaining_usd is None  # never set on a failed probe


async def test_credits_poll_skipped_when_not_paused(tmp_path, monkeypatch):
    b = _make_budget(tmp_path, poll_seconds=3600)
    fake = _FakeClient(
        [{"data": {"limit": 200.0, "usage": 100.0}}],
        credits=[{"data": {"total_credits": 100.0, "total_usage": 50.0}}],
    )
    monkeypatch.setattr("app.budget.httpx.AsyncClient", lambda *a, **kw: fake)
    await b.start()  # never paused
    for _ in range(50):
        if fake.key_calls >= 1:
            break
        await asyncio.sleep(0.02)
    await b.stop()
    assert fake.key_calls >= 1
    assert fake.credits_calls == 0  # gated to paused-only


async def test_stop_is_idempotent_and_cancels_task(tmp_path, monkeypatch):
    b = _make_budget(tmp_path, poll_seconds=3600)
    fake = _FakeClient([{"data": {"limit": 10.0, "usage": 0.0}}])
    monkeypatch.setattr(
        "app.budget.httpx.AsyncClient", lambda *a, **kw: fake
    )
    await b.start()
    await b.stop()
    await b.stop()  # second call must not raise
    assert b._task is None

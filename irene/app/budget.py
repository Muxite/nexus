"""
## Specification

**Purpose.** Watch the OpenRouter spend cap and pause the LLM pipeline before
it crashes into a wall. Two independent pause sources: the monthly cap
(polled from `/api/v1/key`) and a 402-streak counter (driven by `ai.py` on
"Insufficient credits" responses, since the per-account credit balance is
**not** exposed by `/key`). Both sources can pause; either resuming alone is
sufficient to unpause.

**Public interface.**
- `Budget` — instance carries all state (no module globals; modularization
  rule 5).
  - `Budget(*, api_key, monthly_cap_usd, pause_at_remaining_usd,
            poll_seconds, pause_flag_path)` — construct.
  - `await budget.start()` — launches the background poll task.
  - `await budget.stop()` — cancels the poll task; idempotent.
  - `budget.is_paused() -> bool` — O(1) attribute read. `ai.py` calls this
    before every LLM call.
  - `budget.note_402()` — `ai.py` calls this on a 402 response.
  - `budget.note_ok()` — `ai.py` calls this on a 200 response.
  - `budget.remaining_usd -> float | None` — last value from `/key`; `None`
    until the first successful poll.

**Pause-flag file.** When pause state flips on, a JSON file is written at
`pause_flag_path` with `{reason, remaining_usd, paused_at}`. When pause flips
off, the file is deleted. Ad-hoc scripts and the dashboard read the file
without IPC (seulgi lesson — the file IS the protocol).

**Pause sources.**
- `monthly_cap_near_zero` — `remaining = limit - usage < pause_at_remaining_usd`.
  Cleared when `remaining >= pause_at_remaining_usd`.
- `account_credits_zero` — 402-streak reaches 5. Cleared either when the
  streak drops to 0 (one `note_ok()` per `note_402()` to climb back) OR, while
  paused, when the `/api/v1/credits` poll shows the account balance recovered
  to `>= pause_at_remaining_usd` (self-heal after a top-up — without it the
  pause deadlocks, since `ai.py` makes no calls while paused so the streak can
  never decrement on its own).

The streak counter is capped at 100 to bound the climb-back time after a
prolonged outage.

**Invariants.**
- All state on the instance. Two `Budget` objects in the same process do not
  interfere.
- The poll loop never crashes on network errors — it logs and retries on the
  next tick. `remaining_usd` stays at its last successful value during an
  outage (a network blip during a real cap-out must NOT silently unpause).
- The pause-flag file is written exactly on the false→true edge and unlinked
  exactly on the true→false edge.

**Side effects.** Holds an `httpx.AsyncClient`. Spawns one asyncio task on
`start()`. Reads/writes a single file path.

**Failure modes.**
- `httpx` network errors: caught, logged, poll continues.
- Malformed `/key` response: caught, logged, poll continues.
- Pause-flag file write/unlink errors: logged; pause state is still correct
  in-process (the in-memory `is_paused()` is the source of truth for `ai.py`;
  the file is a side channel).

**Non-goals.** Metrics export, Slack alerts, retry budgets, multi-key
rotation. None of that lives here.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


_OPENROUTER_KEY_URL = "https://openrouter.ai/api/v1/key"
# Account-level prepaid balance. `/key` only exposes the per-key monthly cap;
# the account credit ceiling (what 402s when exhausted) lives here. Polled
# only while credits-paused, to detect a top-up and self-heal.
_OPENROUTER_CREDITS_URL = "https://openrouter.ai/api/v1/credits"

# Streak thresholds. Hard-coded because they're protocol-level constants
# (OpenRouter's behaviour, not user config); tunability would invite
# operator footguns ("I set it to 100 and the bot kept burning credits").
_PAUSE_AT_STREAK = 5
_STREAK_CAP = 100  # prevent unbounded growth across long outages

_REASON_MONTHLY = "monthly_cap_near_zero"
_REASON_CREDITS = "account_credits_zero"


class Budget:
    def __init__(
        self,
        *,
        api_key: str,
        monthly_cap_usd: float,
        pause_at_remaining_usd: float,
        poll_seconds: int,
        pause_flag_path: str,
    ) -> None:
        self._api_key = api_key
        self._monthly_cap_usd = monthly_cap_usd
        self._pause_at_remaining_usd = pause_at_remaining_usd
        self._poll_seconds = poll_seconds
        self._pause_flag_path = pause_flag_path

        # Per-source pause state. Either flag → overall paused.
        self._paused_monthly = False
        self._paused_credits = False
        self._streak_402 = 0
        self._remaining_usd: Optional[float] = None
        # Account-level prepaid balance from `/credits`; only refreshed while
        # credits-paused (the self-heal probe). None until first such poll.
        self._account_remaining_usd: Optional[float] = None

        self._task: Optional[asyncio.Task[None]] = None
        self._stopping = asyncio.Event()
        self._client: Optional[httpx.AsyncClient] = None

    # ---- lifecycle -------------------------------------------------------

    async def start(self) -> None:
        if self._task is not None:
            return
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(8.0))
        self._stopping.clear()
        # A fresh process starts optimistically unpaused (__init__ zeroes the
        # streak). A pause-flag left on disk by a prior process is therefore
        # stale — remove it so the dashboard doesn't show a false pause. The
        # first real 402 streak re-writes it.
        if not self.is_paused() and os.path.exists(self._pause_flag_path):
            self._remove_flag()
        self._task = asyncio.create_task(self._run(), name="budget-poll")

    async def stop(self) -> None:
        self._stopping.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # ---- public surface --------------------------------------------------

    def is_paused(self) -> bool:
        return self._paused_monthly or self._paused_credits

    def note_402(self) -> None:
        self._streak_402 = min(self._streak_402 + 1, _STREAK_CAP)
        self._recompute_credits_pause()

    def note_ok(self) -> None:
        if self._streak_402 > 0:
            self._streak_402 -= 1
        self._recompute_credits_pause()

    @property
    def remaining_usd(self) -> Optional[float]:
        return self._remaining_usd

    # ---- internals -------------------------------------------------------

    def _recompute_credits_pause(self) -> None:
        was_paused = self.is_paused()
        if self._streak_402 >= _PAUSE_AT_STREAK:
            self._paused_credits = True
        elif self._streak_402 == 0:
            self._paused_credits = False
        # streak in (0, _PAUSE_AT_STREAK): keep current value — hysteresis
        # so a single OK after a streak doesn't immediately unpause.
        self._sync_pause_flag(was_paused)

    def _note_credits_recovered(self) -> None:
        # Account balance is healthy again (top-up detected). The 402 streak is
        # now stale — zero it so `_recompute_credits_pause` clears the pause and
        # fires the true→false flag edge. Breaks the deadlock where no LLM call
        # goes out while paused, so `note_ok()` could never decrement the streak.
        self._streak_402 = 0
        self._recompute_credits_pause()

    def _recompute_monthly_pause(self) -> None:
        was_paused = self.is_paused()
        if self._remaining_usd is None:
            return
        if self._remaining_usd < self._pause_at_remaining_usd:
            self._paused_monthly = True
        else:
            self._paused_monthly = False
        self._sync_pause_flag(was_paused)

    def _sync_pause_flag(self, was_paused: bool) -> None:
        now_paused = self.is_paused()
        if now_paused and not was_paused:
            self._write_flag()
        elif was_paused and not now_paused:
            self._remove_flag()

    def _current_reason(self) -> str:
        # Monthly takes precedence in the message: a near-zero cap is the
        # more actionable signal (top up, raise the cap).
        if self._paused_monthly:
            return _REASON_MONTHLY
        return _REASON_CREDITS

    def _write_flag(self) -> None:
        payload = {
            "reason": self._current_reason(),
            "remaining_usd": self._remaining_usd,
            "paused_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            parent = os.path.dirname(self._pause_flag_path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            with open(self._pause_flag_path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh)
            logger.warning(
                "budget paused reason=%s remaining_usd=%s",
                payload["reason"],
                payload["remaining_usd"],
            )
        except OSError as exc:
            logger.warning("budget pause-flag write failed: %r", exc)

    def _remove_flag(self) -> None:
        try:
            os.unlink(self._pause_flag_path)
            logger.warning("budget resumed remaining_usd=%s", self._remaining_usd)
        except FileNotFoundError:
            pass
        except OSError as exc:
            logger.warning("budget pause-flag unlink failed: %r", exc)

    async def _run(self) -> None:
        while not self._stopping.is_set():
            await self._poll_once()
            try:
                await asyncio.wait_for(
                    self._stopping.wait(), timeout=self._poll_seconds
                )
            except asyncio.TimeoutError:
                pass

    async def _poll_once(self) -> None:
        assert self._client is not None
        try:
            resp = await self._client.get(
                _OPENROUTER_KEY_URL,
                headers={"Authorization": f"Bearer {self._api_key}"},
            )
            resp.raise_for_status()
            data = resp.json().get("data") or {}
            limit = float(data.get("limit") or self._monthly_cap_usd)
            usage = float(data.get("usage") or 0.0)
            self._remaining_usd = max(limit - usage, 0.0)
            self._recompute_monthly_pause()
        except (httpx.HTTPError, ValueError, KeyError) as exc:
            # Network blip or malformed payload — keep last known value so a
            # real cap-out is not silently unpaused by a transient /key 5xx.
            logger.warning("budget /key poll failed: %r", exc)

        # Self-heal the credits pause: while paused on a 402 streak, watch the
        # account balance and unpause once a top-up lands. Gated to paused-only
        # so normal operation makes no extra HTTP call.
        if self._paused_credits:
            await self._poll_credits_once()

    async def _poll_credits_once(self) -> None:
        assert self._client is not None
        try:
            resp = await self._client.get(
                _OPENROUTER_CREDITS_URL,
                headers={"Authorization": f"Bearer {self._api_key}"},
            )
            resp.raise_for_status()
            data = resp.json().get("data") or {}
            total = float(data.get("total_credits") or 0.0)
            used = float(data.get("total_usage") or 0.0)
            self._account_remaining_usd = total - used
            if self._account_remaining_usd >= self._pause_at_remaining_usd:
                logger.warning(
                    "budget credits self-heal: account_remaining=%.2f — clearing pause",
                    self._account_remaining_usd,
                )
                self._note_credits_recovered()
        except (httpx.HTTPError, ValueError, KeyError) as exc:
            # Transient error — stay paused. Never unpause on a failed probe.
            logger.warning("budget /credits poll failed: %r", exc)

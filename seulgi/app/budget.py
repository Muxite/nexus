"""OpenRouter budget alarm + auto-pause.

Polls the OpenRouter /key endpoint every BUDGET_POLL_SECS. When the
remaining cap drops below BUDGET_PAUSE_USD, writes
`data/budget_paused.flag` containing the current state. When the cap
recovers (e.g., user tops up), the file is removed.

2026-05-22 cycle 3.1 fix: ALSO watches for `402 Insufficient credits`
streaks. OpenRouter has a SEPARATE per-account credit balance that
/key does NOT expose — when it hits zero, every call 402s even though
the monthly cap looks healthy. Caught by the streak counter
(`note_402` / `note_ok`) which any caller (currently ai.py) bumps;
when the streak crosses BUDGET_PAUSE_402_STREAK, we pause with
reason='account_credits_zero'.

The runner checks `is_paused()` at the top of `process()` and
short-circuits with skip_reason=`budget_paused` so the funnel still
records every (engine × match) decision the bot would have made.

Why a file (not a process attr) — the dashboard subprocess and any
ad-hoc analysis script can read the same flag without IPC. Also
survives runner restarts: if we resume while still over-budget, the
short-circuit stays active until the flag is removed.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# 2026-05-22 cycle 3.1: 402-streak watcher. ai.py increments this on
# every "Insufficient credits" response and resets it on the next
# successful call. The poll loop reads this to decide pause state.
_402_streak: int = 0
BUDGET_PAUSE_402_STREAK = 5      # pause after 5 consecutive 402s
BUDGET_RESUME_OK_STREAK = 3      # resume after 3 consecutive 200s


def note_402() -> None:
    """Call from ai.py (and any other LLM caller) on 402 response."""
    global _402_streak
    _402_streak = max(_402_streak + 1, 1)


def note_ok() -> None:
    """Call from ai.py on a successful (200) response."""
    global _402_streak
    if _402_streak > 0:
        _402_streak = max(_402_streak - 1, 0)


def get_402_streak() -> int:
    return _402_streak


def _flag_path() -> Path:
    return Path("/data") / "budget_paused.flag"


def is_paused() -> bool:
    """O(1) check used at the top of process()."""
    try:
        return _flag_path().exists()
    except OSError:
        return False


def read_state() -> Optional[dict]:
    """Read the current pause-flag content (None if not paused)."""
    p = _flag_path()
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except (OSError, json.JSONDecodeError):
        return {"raw": "(unparseable)"}


def _fetch_key_state(api_key: str, timeout: float = 8.0) -> Optional[dict]:
    req = urllib.request.Request(
        "https://openrouter.ai/api/v1/key",
        headers={"Authorization": f"Bearer {api_key}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))["data"]
    except (urllib.error.URLError, KeyError, ValueError) as exc:
        logger.warning("[budget] /key fetch failed: %r", exc)
        return None


async def poll_loop(
    api_key: str,
    pause_threshold_usd: float = 5.0,
    poll_secs: float = 300.0,
) -> None:
    """Background task: every poll_secs, check remaining cap. Toggle
    `data/budget_paused.flag` on / off. Logs a single line on each
    transition so we can grep for it in incident review.
    """
    flag = _flag_path()
    if not api_key:
        logger.warning("[budget] OPENROUTER_API_KEY not set — poller idle")
        # Wait forever (don't burn CPU) so the parent task doesn't crash
        # when it adds the done_callback.
        while True:
            await asyncio.sleep(3600)
    # 2026-05-22 cycle-3 F1 fix: seed paused_prev from the on-disk flag,
    # not None. Without this, a restart that comes up with the flag
    # already on disk AND a recovered cap stays stuck — the unlink
    # branch requires `paused_prev is True`, which `None` never is.
    # We also make the body fully idempotent below: every tick we
    # write the flag when paused and unlink when not, regardless of
    # the previous tick. The previous-state tracking exists only to
    # decide whether to log the PAUSED / RESUMED edge transitions.
    paused_prev: bool = flag.exists()
    pause_reason_prev: Optional[str] = None
    while True:
        try:
            state = _fetch_key_state(api_key)
            streak = get_402_streak()
            # Network blip with no independent pause signal: don't touch
            # the flag. Otherwise a /key 5xx during a real cap-out would
            # silently unpause the bot.
            if state is None and streak < BUDGET_PAUSE_402_STREAK:
                await asyncio.sleep(poll_secs)
                continue
            limit = used = remaining = 0.0
            cap_paused = False
            if state is not None:
                limit = float(state.get("limit") or 0.0)
                used = float(state.get("usage_monthly") or 0.0)
                remaining = float(
                    state.get("limit_remaining") or max(limit - used, 0.0)
                )
                cap_paused = (limit > 0) and (remaining < pause_threshold_usd)
            credits_paused = streak >= BUDGET_PAUSE_402_STREAK
            paused = cap_paused or credits_paused
            reason = (
                "monthly_cap_low" if cap_paused else
                "account_credits_zero" if credits_paused else
                None
            )
            if paused:
                flag.parent.mkdir(parents=True, exist_ok=True)
                flag.write_text(json.dumps({
                    "paused_at": datetime.now(timezone.utc).isoformat(),
                    "reason": reason,
                    "limit_usd": limit,
                    "used_usd": used,
                    "remaining_usd": remaining,
                    "pause_threshold_usd": pause_threshold_usd,
                    "_402_streak": streak,
                }))
                if not paused_prev:
                    logger.warning(
                        "[budget] PAUSED reason=%s — remaining=$%.2f "
                        "402_streak=%d (limit=$%.2f used=$%.2f)",
                        reason, remaining, streak, limit, used,
                    )
            else:
                try:
                    flag.unlink()
                except FileNotFoundError:
                    pass
                if paused_prev:
                    logger.warning(
                        "[budget] RESUMED — remaining=$%.2f >= threshold=$%.2f "
                        "(402_streak=%d)",
                        remaining, pause_threshold_usd, streak,
                    )
            paused_prev = paused
            pause_reason_prev = reason
        except Exception as exc:  # noqa: BLE001
            logger.warning("[budget] poller error: %r", exc)
        await asyncio.sleep(poll_secs)

"""
## Specification

**Purpose.** Provide the one correct way to shut a background `asyncio.Task`
down. Every stage / subsystem in irene owns one or more long-lived tasks and
tears them down in `stop()`; doing that by hand is easy to get subtly wrong,
so the idiom lives here once.

**Public interface.**
- `cancel_and_join(task, *, log=None) -> None` — cancel `task`, await its
  settlement, and return. `None` is accepted and is a no-op, so callers can
  pass an `Optional[Task]` field directly.

**Invariants.**
- The `CancelledError` raised by *our own* `task.cancel()` is swallowed —
  that is the expected outcome of a clean shutdown, not an error.
- A `CancelledError` that does *not* belong to our cancellation is
  re-raised. If the caller is itself being cancelled while it awaits the
  child, swallowing that would discard the caller's cancellation and let a
  shutdown path run on as though nothing happened.
- The two cases are told apart with `asyncio.current_task().cancelling()`,
  which counts cancellation requests delivered to *the caller*. Inspecting
  the child instead (`task.cancelled()`) does not work: cancelling a task
  that is awaiting another task propagates the cancellation into the awaited
  task, so the child reports `cancelled()` either way.
- A non-`CancelledError` exception means the background task died of a real
  fault. It is logged with its traceback rather than dropped, then
  suppressed so one failing task cannot abort the rest of the teardown.
- `BaseException` (`KeyboardInterrupt`, `SystemExit`) is never suppressed.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


async def cancel_and_join(
    task: Optional["asyncio.Task[Any]"],
    *,
    log: Optional[logging.Logger] = None,
) -> None:
    """Cancel `task` and wait for it to settle. See module spec."""
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        # Ours only. A pending cancellation request on the *caller* means
        # this CancelledError is the caller's, passing through our await —
        # re-raise so it is not silently discarded. (Checking the child's
        # `cancelled()` would not work: cancelling a task that awaits
        # another propagates into the awaited task, so the child looks
        # cancelled in both cases.)
        current = asyncio.current_task()
        if current is not None and current.cancelling() > 0:
            raise
    except Exception:
        (log or logger).exception(
            "error while shutting down task %r", task.get_name()
        )

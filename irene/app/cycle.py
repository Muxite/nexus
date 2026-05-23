"""
## Specification

**Purpose.** Hold the active cycle_id in a context variable so every
trade-path row can stamp it without threading the value through every
function signature.

**Public interface.**
- `current_cycle_id() -> int` — read; raises if unset.
- `set_cycle_id(cycle_id: int) -> None` — called once from `main.py` after
  `db.ensure_current_cycle()`.
- `cycle_scope(cycle_id: int)` — context manager for tests / scripts that
  need a temporary scope.

**Invariants.**
- `current_cycle_id()` on an unset context **raises** rather than returning
  a default. Forgetting to wire `set_cycle_id()` is a programming error
  worth crashing on; silently writing `cycle_id=0` would corrupt the
  partition.
- Uses `contextvars` so the value propagates across asyncio tasks spawned
  from the setter's task.

**Side effects.** Mutates a module-level `ContextVar`. This is the documented
exception to modularization rule 5 "no shared mutable state" — the rule
applies to per-message state; cycle is a process-lifetime singleton.

**Failure modes.** `LookupError` from `current_cycle_id()` when called before
`set_cycle_id()`. Treated as fatal — main.py sets it before any stage runs.

**Non-goals.** Cycle CRUD (that's `db.ensure_current_cycle()` + the `cycles`
table owned by `main.py`).
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator

_CYCLE_CTX: ContextVar[int] = ContextVar("irene_cycle_id")


def current_cycle_id() -> int:
    """Return the cycle_id set by main.py. Raises LookupError if unset."""
    return _CYCLE_CTX.get()


def set_cycle_id(cycle_id: int) -> None:
    """Set the process-wide cycle_id. Called once from main.py."""
    _CYCLE_CTX.set(cycle_id)


@contextmanager
def cycle_scope(cycle_id: int) -> Iterator[None]:
    """Temporary scope for tests: `with cycle_scope(7): ...`."""
    token = _CYCLE_CTX.set(cycle_id)
    try:
        yield
    finally:
        _CYCLE_CTX.reset(token)

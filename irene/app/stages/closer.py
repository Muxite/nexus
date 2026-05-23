"""
## Specification

**Purpose.** Pure-poller stage that scans `trades` rows that have not yet
been closed, evaluates the P1 close-rule list against each, and — for any
trade that hits a rule — writes a `trade_closures` row stamping the exit
mid-price, the close reason, and the realised PnL. Replay reconstructs
PnL from the join `(trades JOIN trade_closures)`; `trades` itself stays
append-only (entry-time fact) per `docs/p1_contracts.md` §9.

**Public interface.**
- `CloserStage` — standard stage surface per `docs/p1_contracts.md` §9:
  - `CloserStage(deps: StageDeps)` — requires `deps.clob`.
  - `await start()` — launches the close-poll background task.
  - `await stop()` — cancels the task; idempotent.
  - `await handle(payload)` — raises `NotImplementedError`; this stage
    is a poller and subscribes to no topics.

**Owned tables** (`docs/modularization.md` §4): `trade_closures`. DDL is
registered with `db.register_schema()` at import time.

**Close-rule list (in order).** Only the first rule to fire closes the
trade; later rules are not evaluated for that trade.

  1. `timeout` — `now - trades.decided_at >= cfg.closer.timeout_hours`.
                 The only rule wired in P1.

Future rules (P2+) live in this same list, in order: `target` (price
crosses the take-profit threshold), `stop` (price crosses the stop-loss
threshold), `market_resolved` (Polymarket has resolved the underlying).
`manual` is reserved for an out-of-band operator close. The CHECK
constraint on `close_reason` already enumerates every future value so a
later phase only adds the rule, not the schema.

**PnL formulas** (validated to ±1¢ by `scripts/replay/accuracy.py`):

- `YES`: `pnl = size_usdc * (exit_mid - entry_mid) / entry_mid`
- `NO`:  `pnl = size_usdc * (entry_mid - exit_mid) / (1 - entry_mid)`

where `entry_mid = trades.mid_price` and `exit_mid` is the fresh CLOB
mid fetched at close time (or, when the CLOB returns `None` and
`cfg.closer.skip_on_missing_mid` is False, the stored entry mid — a
degenerate but deterministic fallback that produces `pnl == 0`).

**Invariants.**
- `trade_closures.trade_id` is `UNIQUE`. A second poll cycle that sees
  the same expired trade is a no-op (`INSERT OR IGNORE`). The whole
  close-loop body is therefore safe to re-run.
- `cycle_id` is read from `cycle.current_cycle_id()` at close time.
- `is_tracer` / `tracer_id` are carried from the trade row — the closer
  never receives a payload that could supply them.
- `trades` is read-only here. Closer NEVER updates the trade row.

**Side effects.** Reads `trades` and `markets` (token lookup). Calls
`deps.clob.get_mid(token_id)`. Writes `trade_closures` inside
`db.batch()`. Spawns a background asyncio task in `start()`; cancels it
in `stop()`.

**Failure modes.**
- CLOB returns `None` for a trade's token and `cfg.closer.skip_on_missing_mid`
  is True (default): the trade is logged at INFO and left open this
  cycle. The next poll retries.
- CLOB returns `None` and `skip_on_missing_mid` is False: the trade
  closes at its stored entry mid (pnl ≈ 0).
- `markets` row for a trade's `market_id` is missing (a race that
  shouldn't happen in P1 since markets only deactivate, never delete):
  log WARNING and skip the trade.
- Any unhandled exception inside the poll body is logged and the loop
  continues — a single bad trade must not kill the closer.

**Non-goals.**
- Mark-to-market for open positions — closer writes only at close-time.
- Target / stop / market-resolved rules — P1 is timeout-only.
- Cross-trade portfolio accounting (drawdown, exposure caps).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from app import db as db_module
from app.cycle import current_cycle_id
from app.deps import StageDeps

logger = logging.getLogger(__name__)


_CLOSER_DDL = [
    """
    CREATE TABLE IF NOT EXISTS trade_closures (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_id        INTEGER NOT NULL UNIQUE REFERENCES trades(id),
        closed_at       TEXT NOT NULL,
        close_reason    TEXT NOT NULL CHECK(close_reason IN ('timeout','target','stop','market_resolved','manual')),
        exit_mid_price  REAL NOT NULL CHECK(exit_mid_price >= 0.0 AND exit_mid_price <= 1.0),
        realized_pnl_usdc REAL NOT NULL,
        cycle_id        INTEGER NOT NULL,
        is_tracer       INTEGER NOT NULL DEFAULT 0,
        tracer_id       TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_trade_closures_close_reason ON trade_closures(close_reason)",
    "CREATE INDEX IF NOT EXISTS idx_trade_closures_closed_at ON trade_closures(closed_at)",
    "CREATE INDEX IF NOT EXISTS idx_trade_closures_cycle_id ON trade_closures(cycle_id)",
]
db_module.register_schema("app/stages/closer.py", _CLOSER_DDL)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def _parse_iso(value: str) -> Optional[datetime]:
    # ISO-8601 round-trip used by every other stage. `fromisoformat` handles
    # the offset-bearing strings we write; a malformed value returns None so
    # the caller can skip rather than crash the poll loop.
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def _compute_pnl(direction: str, entry_mid: float, exit_mid: float, size_usdc: float) -> float:
    """The two PnL formulas the spec block documents. Kept as a free
    function so `scripts/replay/accuracy.py` can call it without
    instantiating the stage."""
    if direction == "YES":
        if entry_mid <= 0.0:
            return 0.0
        return size_usdc * (exit_mid - entry_mid) / entry_mid
    # NO
    denom = 1.0 - entry_mid
    if denom <= 0.0:
        return 0.0
    return size_usdc * (entry_mid - exit_mid) / denom


class CloserStage:
    def __init__(self, deps: StageDeps) -> None:
        if deps.clob is None:
            raise RuntimeError("CloserStage requires deps.clob")
        self._db = deps.db
        self._clob = deps.clob
        self._cfg = deps.config.closer
        self._task: Optional[asyncio.Task] = None
        self._stopped = False

    # ---- lifecycle -----------------------------------------------------

    async def start(self) -> None:
        self._task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._task = None

    async def handle(self, payload: dict) -> None:  # noqa: ARG002
        raise NotImplementedError("CloserStage is a poller, not a subscriber")

    # ---- background loop ----------------------------------------------

    async def _poll_loop(self) -> None:
        while True:
            try:
                await self.run_close_cycle()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — never let the loop die
                logger.exception("closer poll cycle failed; continuing")
            await asyncio.sleep(self._cfg.poll_seconds)

    # ---- single-cycle entry point (also called directly in tests) ----

    async def run_close_cycle(self) -> int:
        """Run one close-poll pass. Returns the number of closures written.

        Exposed as a method (not nested inside `_poll_loop`) so tests can
        drive the loop body deterministically without sleeping.
        """
        open_trades = await self._fetch_open_trades()
        if not open_trades:
            return 0

        now = _now()
        timeout = timedelta(hours=float(self._cfg.timeout_hours))
        written = 0
        for trade in open_trades:
            rule = self._evaluate_rules(trade, now=now, timeout=timeout)
            if rule is None:
                continue
            if await self._close_trade(trade, close_reason=rule, now_iso=now.isoformat()):
                written += 1
        return written

    # ---- internals -----------------------------------------------------

    def _evaluate_rules(
        self,
        trade: dict,
        *,
        now: datetime,
        timeout: timedelta,
    ) -> Optional[str]:
        """Walk the close-rule list in order and return the first rule that
        fires, or None if the trade should stay open."""
        decided_at = _parse_iso(trade["decided_at"])
        if decided_at is not None and now - decided_at >= timeout:
            return "timeout"
        # P2+ rules slot in here, in order.
        return None

    async def _fetch_open_trades(self) -> list[dict]:
        cur = await self._db.conn.execute(
            """
            SELECT t.id, t.signal_id, t.market_id, t.direction, t.mid_price,
                   t.size_usdc, t.decided_at, t.is_tracer, t.tracer_id
            FROM trades t
            LEFT JOIN trade_closures c ON c.trade_id = t.id
            WHERE c.id IS NULL
            """
        )
        rows = await cur.fetchall()
        await cur.close()
        return [
            {
                "id": r["id"],
                "signal_id": r["signal_id"],
                "market_id": r["market_id"],
                "direction": r["direction"],
                "mid_price": float(r["mid_price"]),
                "size_usdc": float(r["size_usdc"]),
                "decided_at": r["decided_at"],
                "is_tracer": int(r["is_tracer"] or 0),
                "tracer_id": r["tracer_id"],
            }
            for r in rows
        ]

    async def _fetch_tokens(self, market_id: str) -> Optional[dict]:
        cur = await self._db.conn.execute(
            "SELECT token_yes, token_no FROM markets WHERE condition_id = ?",
            (market_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return {"token_yes": row["token_yes"], "token_no": row["token_no"]}

    async def _close_trade(
        self,
        trade: dict,
        *,
        close_reason: str,
        now_iso: str,
    ) -> bool:
        """Resolve exit mid + write the closure row. Returns True iff a
        closure row was actually inserted (i.e. UNIQUE(trade_id) did not
        kick in)."""
        tokens = await self._fetch_tokens(trade["market_id"])
        if tokens is None:
            logger.warning(
                "closer: trade_id=%d market %s missing from markets — skipping",
                trade["id"], trade["market_id"],
            )
            return False

        direction = trade["direction"]
        token_id = tokens["token_yes"] if direction == "YES" else tokens["token_no"]
        exit_mid: Optional[float] = None
        if token_id:
            exit_mid = await self._clob.get_mid(token_id)

        if exit_mid is None:
            if self._cfg.skip_on_missing_mid:
                logger.info(
                    "closer: trade_id=%d no CLOB mid for %s; holding (skip_on_missing_mid=true)",
                    trade["id"], token_id,
                )
                return False
            # Degenerate fallback: close at the entry mid. PnL is ~0 but the
            # trade exits the open set deterministically.
            exit_mid = trade["mid_price"]
            logger.info(
                "closer: trade_id=%d no CLOB mid; closing at entry mid (skip_on_missing_mid=false)",
                trade["id"],
            )

        # Clamp into the CHECK range — a misbehaving CLOB returning a value
        # just outside [0,1] would otherwise trip the constraint and crash
        # the whole poll body.
        exit_mid = max(0.0, min(1.0, float(exit_mid)))

        pnl = _compute_pnl(
            direction=direction,
            entry_mid=trade["mid_price"],
            exit_mid=exit_mid,
            size_usdc=trade["size_usdc"],
        )

        cycle_id = current_cycle_id()
        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT OR IGNORE INTO trade_closures
                    (trade_id, closed_at, close_reason, exit_mid_price,
                     realized_pnl_usdc, cycle_id, is_tracer, tracer_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade["id"],
                    now_iso,
                    close_reason,
                    exit_mid,
                    pnl,
                    cycle_id,
                    1 if trade["is_tracer"] else 0,
                    trade["tracer_id"],
                ),
            )
            return cur.rowcount == 1


if __name__ == "__main__":
    # Per modularization rule 8: every stage runs standalone for dev poking.
    # No real CLOB available here; the stage simply raises on instantiation
    # without one, which is the contract.
    import sys

    print("CloserStage requires a CLOBClient; instantiate via app.main wiring.",
          file=sys.stderr)

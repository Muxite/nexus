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
        close_reason    TEXT NOT NULL CHECK(close_reason IN ('timeout','target','stop','market_resolved','manual','edge_decay','stagnation')),
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
    # Cycle 7: fee accounting columns (NULL = legacy pre-cycle-7 row).
    # entry_fee_usdc + exit_fee_usdc are already baked into realized_pnl_usdc;
    # they exist so audits can attribute total fees paid per cycle.
    "ALTER TABLE trade_closures ADD COLUMN entry_fee_usdc REAL DEFAULT 0",
    "ALTER TABLE trade_closures ADD COLUMN exit_fee_usdc  REAL DEFAULT 0",
    # Cycle 25-A: expand the close_reason CHECK constraint to allow
    # `market_dead_unresolved` (force-close path for State 1 markets that
    # never finalize via UMA). SQLite can't ALTER a CHECK in place, so
    # we rebuild the table. The new table OMITS the `REFERENCES trades(id)`
    # FK clause that the original had — the FK was decorative (the closer
    # already validates trade existence before insert) and including it
    # forces SQLite to resolve `trades` during the migration, which fails
    # in test fixtures that boot only the closer's DDL. Guarded against
    # re-execution by the SHA-based migrations table.
    """
    CREATE TABLE IF NOT EXISTS trade_closures_cycle25 (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_id        INTEGER NOT NULL UNIQUE,
        closed_at       TEXT NOT NULL,
        close_reason    TEXT NOT NULL CHECK(close_reason IN (
            'timeout','target','stop','market_resolved','manual',
            'edge_decay','stagnation','market_dead_unresolved'
        )),
        exit_mid_price  REAL NOT NULL CHECK(exit_mid_price >= 0.0 AND exit_mid_price <= 1.0),
        realized_pnl_usdc REAL NOT NULL,
        cycle_id        INTEGER NOT NULL,
        is_tracer       INTEGER NOT NULL DEFAULT 0,
        tracer_id       TEXT,
        entry_fee_usdc  REAL DEFAULT 0,
        exit_fee_usdc   REAL DEFAULT 0
    )
    """,
    """
    INSERT OR IGNORE INTO trade_closures_cycle25
        (id, trade_id, closed_at, close_reason, exit_mid_price,
         realized_pnl_usdc, cycle_id, is_tracer, tracer_id,
         entry_fee_usdc, exit_fee_usdc)
    SELECT id, trade_id, closed_at, close_reason, exit_mid_price,
           realized_pnl_usdc, cycle_id, is_tracer, tracer_id,
           COALESCE(entry_fee_usdc, 0), COALESCE(exit_fee_usdc, 0)
    FROM trade_closures
    """,
    "DROP TABLE IF EXISTS trade_closures",
    "ALTER TABLE trade_closures_cycle25 RENAME TO trade_closures",
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


def _compute_gross_pnl(direction: str, entry_mid: float, exit_mid: float, size_usdc: float) -> float:
    """Pure mid-to-mid PnL. The two formulas the spec block documents.
    Kept as a free function so `scripts/replay/accuracy.py` can call it
    without instantiating the stage."""
    if direction == "YES":
        if entry_mid <= 0.0:
            return 0.0
        return size_usdc * (exit_mid - entry_mid) / entry_mid
    # NO
    denom = 1.0 - entry_mid
    if denom <= 0.0:
        return 0.0
    return size_usdc * (entry_mid - exit_mid) / denom


def _resolve_fee_rate(
    category: Optional[str],
    rates: dict,
    default: float,
    *,
    market_fee_rate: Optional[float] = None,
) -> float:
    """Resolve the Polymarket taker fee rate for a trade.

    **Cycle 10 lookup priority** (markets-gamma.md + closer-fees.md):
      1. `market_fee_rate` — the per-market rate fetched from
         `GET /markets/<conditionId>:taker_base_fee` and stored in
         `markets.fee_rate` (authoritative; varies per market — a
         2026-05-25 live survey found {0.00, 0.02, 0.10} across the
         universe). Used as-is when present and in `[0.0, 1.0]`.
      2. `category` → `rates` lookup (case-insensitive, substring-
         tolerant fallback for categories like "us-politics" /
         "world-politics" → "politics" base tier).
      3. `default` when both are missing / unrecognized.

    The category table stays as the fallback — markets without a
    fetched fee_rate (network miss at discovery, legacy rows from
    before cycle 10, NULL after migration) still get a sensible
    fee deduction.
    """
    if market_fee_rate is not None:
        try:
            mf = float(market_fee_rate)
        except (TypeError, ValueError):
            mf = -1.0
        if 0.0 <= mf <= 1.0:
            return mf
    if not category:
        return default
    key = category.strip().lower()
    if key in rates:
        return float(rates[key])
    # Light substring match — Polymarket categories sometimes include
    # qualifiers like "us-politics" or "world-politics"; collapse to the
    # base tier.
    for token, rate in rates.items():
        if token in key:
            return float(rate)
    return default


def _polymarket_taker_fee(price: float, shares: float, fee_rate: float) -> float:
    """Per-leg Polymarket taker fee.
      fee = shares * fee_rate * p * (1 - p)
    The parabola peaks at p=0.5 and is symmetric — buying NO at YES_mid=0.45
    (i.e. NO_price = 0.55) and buying YES at 0.55 pay the same per-share fee.
    Returns 0 when fee_rate=0 (Polymarket's geopolitics tier).
    """
    if fee_rate <= 0.0 or shares <= 0.0:
        return 0.0
    p = max(0.0, min(1.0, float(price)))
    return shares * fee_rate * p * (1.0 - p)


def _compute_round_trip_fees(
    direction: str,
    entry_mid: float,
    exit_mid: float,
    size_usdc: float,
    fee_rate: float,
    gas_cost_usdc: float = 0.0,
) -> tuple[float, float]:
    """Cycle 7: faithful Polymarket fee model.

    Returns (entry_fee_usdc, exit_fee_usdc). Each leg uses
        fee = shares * fee_rate * p * (1 - p)
    with `p` being the share-side price (=entry/exit mid for YES,
    =(1-mid) for NO). `shares = size_usdc / position_price`.

    Gas (if configured) is added per leg.
    """
    if size_usdc <= 0.0:
        return 0.0, 0.0
    if direction == "YES":
        entry_price = entry_mid
        exit_price = exit_mid
    else:
        entry_price = 1.0 - entry_mid
        exit_price = 1.0 - exit_mid

    if entry_price <= 0.0:
        # Degenerate entry — no shares purchased. Zero fee model-wise.
        return 0.0, 0.0
    shares = size_usdc / entry_price
    entry_fee = _polymarket_taker_fee(entry_price, shares, fee_rate) + gas_cost_usdc
    exit_fee = _polymarket_taker_fee(exit_price, shares, fee_rate) + gas_cost_usdc
    return entry_fee, exit_fee


# Backwards-compat shim: scripts/replay/accuracy.py imports _compute_pnl.
# Keep the old name as the gross-only path so existing callers see no
# behaviour change. CloserStage._close_trade uses the gross helper plus
# the fee deduction directly.
_compute_pnl = _compute_gross_pnl


async def write_trade_closure(
    db,
    *,
    trade: dict,
    exit_mid: float,
    close_reason: str,
    trade_cfg,
    category: Optional[str] = None,
    market_fee_rate: Optional[float] = None,
    now_iso: Optional[str] = None,
) -> bool:
    """Cycle 7: free function that writes a trade_closures row with
    Polymarket-fee-deducted PnL. Used by CloserStage._close_trade and
    by MarketsStage when a market deactivates with an open trade.
    Returns True iff a closure row was inserted (UNIQUE(trade_id)
    prevents double-fires).

    `trade_cfg` must expose `polymarket_fee_rates`, `default_fee_rate`,
    and `gas_cost_usdc`. `category` comes from the market row when the
    caller has it (CloserStage._close_trade fetches it; tests pass it
    inline). Missing/unknown category falls back to default_fee_rate.

    Fee model — per-leg parabolic:
        fee = shares * fee_rate * p * (1 - p)
    Both legs are taker fees. Maker fees and settlement fees are 0.
    Exception: `close_reason='market_resolved'` zeros the exit-leg fee
    (no taker fill at resolution — shares redeem at $1/$0). See
    app/config.py:TradeSection.polymarket_fee_rates.

    Paper-mode caveat: `p` in the formula is the *taker fill price*, but
    we use `entry_mid` / `exit_mid` as the best proxy available in paper
    mode. The error is bounded by half the spread per leg and is roughly
    symmetric. The real-money executor (cycle 9+) must swap `mid` for
    the executed `fill_price` on the trade row.
    """
    exit_mid = max(0.0, min(1.0, float(exit_mid)))
    gross_pnl = _compute_gross_pnl(
        direction=trade["direction"],
        entry_mid=float(trade["mid_price"]),
        exit_mid=exit_mid,
        size_usdc=float(trade["size_usdc"]),
    )
    fee_rate = _resolve_fee_rate(
        category,
        getattr(trade_cfg, "polymarket_fee_rates", {}),
        # Cycle 15 audit: top-500 markets by volume24hr are 82% at 10%
        # taker; raised this defensive fallback from 0.05 to 0.10 to
        # match the volume-tail modal value. See cfg.trade.default_fee_rate
        # for the canonical config knob (per-market fetch is preferred).
        float(getattr(trade_cfg, "default_fee_rate", 0.10)),
        market_fee_rate=market_fee_rate,
    )
    gas_usdc = float(getattr(trade_cfg, "gas_cost_usdc", 0.0))
    entry_fee, exit_fee = _compute_round_trip_fees(
        direction=trade["direction"],
        entry_mid=float(trade["mid_price"]),
        exit_mid=exit_mid,
        size_usdc=float(trade["size_usdc"]),
        fee_rate=fee_rate,
        gas_cost_usdc=gas_usdc,
    )
    if close_reason in ("market_resolved", "market_dead_unresolved"):
        # No taker fill on the exit leg in either case:
        # - `market_resolved`: shares redeem at $1/$0 (Polymarket charges
        #   no fee at resolution).
        # - `market_dead_unresolved` (cycle 25-A): force-close at entry_mid
        #   because the market closed but UMA never finalized in the
        #   force-close window. No actual exit transaction occurred, so no
        #   exit fee applies. The entry-leg fee still stands.
        exit_fee = 0.0
    net_pnl = gross_pnl - entry_fee - exit_fee

    cycle_id = current_cycle_id()
    timestamp = now_iso or _now_iso()
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT OR IGNORE INTO trade_closures
                (trade_id, closed_at, close_reason, exit_mid_price,
                 realized_pnl_usdc, cycle_id, is_tracer, tracer_id,
                 entry_fee_usdc, exit_fee_usdc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trade["id"],
                timestamp,
                close_reason,
                exit_mid,
                net_pnl,
                cycle_id,
                1 if trade.get("is_tracer") else 0,
                trade.get("tracer_id"),
                entry_fee,
                exit_fee,
            ),
        )
        return cur.rowcount == 1


class CloserStage:
    def __init__(self, deps: StageDeps) -> None:
        if deps.clob is None:
            raise RuntimeError("CloserStage requires deps.clob")
        self._db = deps.db
        self._clob = deps.clob
        self._cfg = deps.config.closer
        # Cycle 7: stash cfg.trade so closure writes can read the fee
        # knobs (spread_cost_bps, gas_cost_usdc).
        self._trade_cfg = deps.config.trade
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

        Cycle 2: evaluation order is take_profit → stop_loss → timeout.
        take_profit + stop_loss need a fresh CLOB mid to compare against
        entry; we fetch it once per open trade and reuse it as the exit
        mid if the rule fires (no double round-trip).
        """
        open_trades = await self._fetch_open_trades()
        if not open_trades:
            return 0

        now = _now()
        timeout = timedelta(hours=float(self._cfg.timeout_hours))
        written = 0
        for trade in open_trades:
            # Try the price-aware rules first — they require a CLOB call.
            tokens = await self._fetch_tokens(trade["market_id"])
            current_mid: Optional[float] = None
            if tokens is not None:
                token_id = tokens["token_yes"] if trade["direction"] == "YES" else tokens["token_no"]
                if token_id:
                    current_mid = await self._clob.get_mid(token_id)
            rule = self._evaluate_rules(
                trade, now=now, timeout=timeout, current_mid=current_mid
            )
            if rule is None:
                continue
            # Re-use the current_mid we just fetched (saves a 2nd CLOB
            # round-trip in `_close_trade`).
            if await self._close_trade(
                trade,
                close_reason=rule,
                now_iso=now.isoformat(),
                exit_mid_hint=current_mid,
            ):
                written += 1
        return written

    # ---- internals -----------------------------------------------------

    def _evaluate_rules(
        self,
        trade: dict,
        *,
        now: datetime,
        timeout: timedelta,
        current_mid: Optional[float],
    ) -> Optional[str]:
        """Walk the close-rule list in order, return the first match.

        Order (cycle 3): edge_decay → take_profit → stop_loss →
        stagnation → timeout. edge_decay is the primary exit — once the
        live mid reaches the model's target_mid, the original Kelly edge
        is gone and capital should rotate. The rest are safety nets.
        """
        size = float(trade["size_usdc"])
        entry = float(trade["mid_price"])
        direction = trade["direction"]
        target = trade.get("target_mid")

        if current_mid is not None and size > 0.0:
            cur = float(current_mid)
            # 1. edge_decay — Kelly target reached. For YES the price
            # rose to >= target; for NO it fell to <= target. Both
            # directions: edge = 0, sell.
            if target is not None:
                if direction == "YES" and cur >= float(target):
                    return "edge_decay"
                if direction == "NO" and cur <= float(target):
                    return "edge_decay"
            # 2/3. take_profit + stop_loss — absolute %age thresholds.
            pnl_frac = _compute_pnl(direction, entry, cur, 1.0)
            if self._cfg.take_profit_pct > 0.0 and pnl_frac >= self._cfg.take_profit_pct:
                return "target"
            if self._cfg.stop_loss_pct > 0.0 and pnl_frac <= -self._cfg.stop_loss_pct:
                return "stop"

        # 4. stagnation — aged past stagnation_minutes AND mid hasn't
        # moved by more than stagnation_drift. Catalyst was priced in
        # or wasn't real; free the cash.
        decided_at = _parse_iso(trade["decided_at"])
        if decided_at is not None:
            age = now - decided_at
            if current_mid is not None and self._cfg.stagnation_minutes > 0:
                stag_age = timedelta(minutes=int(self._cfg.stagnation_minutes))
                if age >= stag_age and abs(float(current_mid) - entry) < float(self._cfg.stagnation_drift):
                    return "stagnation"
            # 5. timeout — catchall.
            if age >= timeout:
                return "timeout"
        return None

    async def _fetch_open_trades(self) -> list[dict]:
        cur = await self._db.conn.execute(
            """
            SELECT t.id, t.signal_id, t.market_id, t.direction, t.mid_price,
                   t.size_usdc, t.decided_at, t.is_tracer, t.tracer_id,
                   t.target_mid
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
                "target_mid": (float(r["target_mid"]) if r["target_mid"] is not None else None),
            }
            for r in rows
        ]

    async def _fetch_tokens(self, market_id: str) -> Optional[dict]:
        # Cycle 10: also pull `fee_rate` so the closure write can prefer
        # the per-market Polymarket taker rate over the category lookup.
        # NULL fee_rate (legacy rows / network miss at first-seen) falls
        # back to `category` → `config.polymarket_fee_rates` in
        # `_resolve_fee_rate`.
        cur = await self._db.conn.execute(
            "SELECT token_yes, token_no, category, fee_rate FROM markets WHERE condition_id = ?",
            (market_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return {
            "token_yes": row["token_yes"],
            "token_no": row["token_no"],
            "category": row["category"],
            "fee_rate": row["fee_rate"],
        }

    async def _close_trade(
        self,
        trade: dict,
        *,
        close_reason: str,
        now_iso: str,
        exit_mid_hint: Optional[float] = None,
    ) -> bool:
        """Resolve exit mid + write the closure row. Returns True iff a
        closure row was actually inserted (i.e. UNIQUE(trade_id) did not
        kick in).

        Cycle 2: callers that already fetched a fresh CLOB mid (the
        take_profit / stop_loss rules) pass it as `exit_mid_hint` so we
        don't pay for a second round-trip. Timeout rule passes None and
        we fetch fresh — its exit mid wasn't needed to evaluate the rule.
        """
        direction = trade["direction"]
        exit_mid: Optional[float] = exit_mid_hint
        # Cycle 7: fetch category alongside tokens so the closure write
        # can resolve the Polymarket fee_rate. _fetch_tokens always runs
        # — even when we have exit_mid_hint — because the hint comes
        # from rule evaluation, not the markets row.
        market_meta = await self._fetch_tokens(trade["market_id"])
        category = market_meta.get("category") if market_meta else None
        market_fee_rate = market_meta.get("fee_rate") if market_meta else None
        if exit_mid is None:
            if market_meta is None:
                logger.warning(
                    "closer: trade_id=%d market %s missing from markets — skipping",
                    trade["id"], trade["market_id"],
                )
                return False
            token_id = market_meta["token_yes"] if direction == "YES" else market_meta["token_no"]
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

        return await write_trade_closure(
            self._db,
            trade=trade,
            exit_mid=exit_mid,
            close_reason=close_reason,
            trade_cfg=self._trade_cfg,
            category=category,
            market_fee_rate=market_fee_rate,
            now_iso=now_iso,
        )


if __name__ == "__main__":
    # Per modularization rule 8: every stage runs standalone for dev poking.
    # No real CLOB available here; the stage simply raises on instantiation
    # without one, which is the contract.
    import sys

    print("CloserStage requires a CLOBClient; instantiate via app.main wiring.",
          file=sys.stderr)

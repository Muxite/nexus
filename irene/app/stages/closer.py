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

**Price-space convention (cycle 27 — read this before touching PnL).**
Every price this stage handles is in **position space**: the traded
token's own price. `trades.mid_price` is the mid of the token we bought
(YES token for YES trades, NO token for NO trades — `_resolve_mid` and
this stage's poll both read the traded side), `entry_fill_price` /
`exit_fill_price` are that token's ask/bid, and resolution payouts are
the traded token's redemption value. The pre-cycle-27 NO formula
(`(entry - exit) / (1 - entry)`) assumed YES-space mids the data
pipeline never supplied — NO-trade PnL was sign-flipped and rescaled
(verified against `features_snapshot.edge_components.executable_price`
on live rows, 2026-06-12).

**PnL formula** (position space, direction-independent):

    shares = size_usdc / entry_price
    pnl    = shares * (exit_price - entry_price)

- Cycle-27+ rows fill honestly: `entry_price = trades.entry_fill_price`
  (the ask at entry), `exit_price = exit_fill_price` (the bid at close).
  The bid/ask pair is persisted on both legs for forensics.
- Legacy rows (`entry_fill_price IS NULL`) close mid-to-mid:
  `entry_price = trades.mid_price`, `exit_price` = fresh book mid at
  close (both position space, so the formula is exact for them too).
- When the book is unavailable at close and
  `cfg.closer.skip_on_missing_mid` is True (default) the trade is held;
  when False it closes at its entry price (deterministic `pnl == 0`).

**Invariants.**
- `trade_closures.trade_id` is `UNIQUE`. A second poll cycle that sees
  the same expired trade is a no-op (`INSERT OR IGNORE`). The whole
  close-loop body is therefore safe to re-run.
- `cycle_id` is read from `cycle.current_cycle_id()` at close time.
- `is_tracer` / `tracer_id` are carried from the trade row — the closer
  never receives a payload that could supply them.
- `trades` is read-only here. Closer NEVER updates the trade row.

**Side effects.** Reads `trades` and `markets` (token lookup). Calls
`deps.clob.get_top_of_book(token_id)`. Writes `trade_closures` inside
`db.batch()`. Spawns a background asyncio task in `start()`; cancels it
in `stop()`.

**Failure modes.**
- CLOB book is `None` for a trade's token and `cfg.closer.skip_on_missing_mid`
  is True (default): the trade is logged at INFO and left open this
  cycle. The next poll retries.
- CLOB book is `None` and `skip_on_missing_mid` is False: the trade
  closes at its entry price (pnl == 0).
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
from app.aioutil import cancel_and_join
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
    # Cycle 27 (wallet honesty): spread-aware exits. The traded token's
    # top-of-book at close time, plus the fill price the exit actually
    # uses (= exit_best_bid — you sell at the bid). All three NULL on
    # legacy rows, on resolution/dead-market closes (redemption, no book
    # fill), and on the skip_on_missing_mid=false degenerate path.
    # `exit_mid_price` stays NOT NULL and keeps the position-space mid.
    "ALTER TABLE trade_closures ADD COLUMN exit_best_bid   REAL",
    "ALTER TABLE trade_closures ADD COLUMN exit_best_ask   REAL",
    "ALTER TABLE trade_closures ADD COLUMN exit_fill_price REAL",
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


def _compute_gross_pnl(direction: str, entry_price: float, exit_price: float, size_usdc: float) -> float:
    """Pure gross PnL in **position space** (both prices are the traded
    token's own — see the spec block). Direction-independent:

        shares = size_usdc / entry_price
        pnl    = shares * (exit_price - entry_price)

    Cycle 27: the old NO branch (`(entry - exit) / (1 - entry)`) assumed
    YES-space mids, but `_resolve_mid` and the closer's poll have always
    fed the traded token's price — NO-trade PnL came out sign-flipped
    and rescaled by `entry/(1-entry)`. `direction` stays in the
    signature so the historical-recompute scripts and call sites don't
    churn; it is intentionally unused.
    """
    del direction  # position-space prices make the formula symmetric
    if entry_price <= 0.0:
        return 0.0
    return size_usdc * (exit_price - entry_price) / entry_price


def _resolve_fee_rate(
    market_fee_rate: Optional[float],
    default: float,
) -> float:
    """Resolve the Polymarket taker fee rate for a trade.

    Cycle 27: per-market `markets.fee_rate` (CLOB
    `GET /markets/<conditionId>:taker_base_fee`, refreshed weekly) or
    `default` — nothing else. The cycle-7 category→rate fallback table
    is gone: it invented 3–7.2% fees for markets whose real taker fee
    is 0, and was the main source of the wallet's 5.7%-of-size mean fee
    (2026-06-12 retrospective §3). `default` should be 0.0 (see
    `cfg.trade.default_fee_rate`): an unknown fee is far more likely to
    be Polymarket's modal 0% than any invented positive rate, and a
    paper wallet must not manufacture losses.
    """
    if market_fee_rate is not None:
        try:
            mf = float(market_fee_rate)
        except (TypeError, ValueError):
            mf = -1.0
        if 0.0 <= mf <= 1.0:
            return mf
    return default


def _polymarket_taker_fee(price: float, shares: float, fee_rate: float) -> float:
    """Per-leg Polymarket taker fee, per the published formula:
      fee = shares * fee_rate * min(p, 1 - p)
    `p` is the leg's fill price in position space; `min(p, 1-p)` is
    symmetric across the binary, so YES at 0.55 and NO at 0.45 pay the
    same per-share fee. Cycle 27 replaced the cycle-7 parabola
    `p * (1 - p)` — close near the middle but up to 2x off at the tails,
    and not what Polymarket documents. Returns 0 when fee_rate=0 (the
    modal market).
    """
    if fee_rate <= 0.0 or shares <= 0.0:
        return 0.0
    p = max(0.0, min(1.0, float(price)))
    return shares * fee_rate * min(p, 1.0 - p)


def _compute_round_trip_fees(
    entry_price: float,
    exit_price: float,
    size_usdc: float,
    fee_rate: float,
    gas_cost_usdc: float = 0.0,
) -> tuple[float, float]:
    """Returns (entry_fee_usdc, exit_fee_usdc). Each leg uses
        fee = shares * fee_rate * min(p, 1 - p)
    with `p` the leg's fill price in **position space** (the traded
    token's own price — cycle 27 dropped the direction-based flip along
    with the YES-space convention; `min(p, 1-p)` is symmetric anyway).
    `shares = size_usdc / entry_price`. Gas (if configured) is added
    per leg.
    """
    if size_usdc <= 0.0:
        return 0.0, 0.0
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
    market_fee_rate: Optional[float] = None,
    now_iso: Optional[str] = None,
    exit_book: Optional[tuple[float, float]] = None,
) -> bool:
    """Free function that writes a trade_closures row with
    Polymarket-fee-deducted PnL. Used by CloserStage._close_trade and
    by MarketsStage when a market resolves/dies with an open trade.
    Returns True iff a closure row was inserted (UNIQUE(trade_id)
    prevents double-fires).

    All prices are **position space** (traded token's own — spec block).

    Pricing (cycle 27, spread-aware):
      - `exit_book` is the traded token's `(best_bid, best_ask)` at
        close time, when the caller has one (CloserStage does;
        resolution/dead-market paths don't — redemption has no book
        fill). Persisted for forensics.
      - entry price for PnL/fees = `trade['entry_fill_price']` (the ask
        paid at entry) when present, else the legacy `trade['mid_price']`.
      - exit price for PnL/fees = `best_bid` from `exit_book` (you sell
        at the bid) when present, else `exit_mid` (resolution payout /
        legacy mid / degenerate entry-price fallback).
      The residual between mid-to-mid drift and realized PnL is exactly
      the spread paid — the quantity the pre-cycle-27 wallet modeled
      as $0.00.

    Fee model — per-leg taker fee on the fill price:
        fee = shares * fee_rate * min(p, 1 - p)
    `fee_rate` comes from `markets.fee_rate` (per-market fetch) or
    `cfg.trade.default_fee_rate` (0.0 — see `_resolve_fee_rate`).
    Exception: `close_reason='market_resolved'` /
    `'market_dead_unresolved'` zero the exit-leg fee — shares redeem at
    $1/$0 (no taker fill) or no exit transaction occurred at all. The
    entry-leg fee still stands.
    """
    exit_mid = max(0.0, min(1.0, float(exit_mid)))
    size_usdc = float(trade["size_usdc"])
    entry_fill = trade.get("entry_fill_price")
    entry_price = float(entry_fill) if entry_fill is not None else float(trade["mid_price"])
    exit_best_bid: Optional[float] = None
    exit_best_ask: Optional[float] = None
    exit_fill: Optional[float] = None
    if exit_book is not None and close_reason not in (
        "market_resolved", "market_dead_unresolved",
    ):
        exit_best_bid, exit_best_ask = float(exit_book[0]), float(exit_book[1])
        exit_fill = exit_best_bid
    exit_price = exit_fill if exit_fill is not None else exit_mid

    gross_pnl = _compute_gross_pnl(
        direction=trade["direction"],
        entry_price=entry_price,
        exit_price=exit_price,
        size_usdc=size_usdc,
    )
    fee_rate = _resolve_fee_rate(
        market_fee_rate,
        float(getattr(trade_cfg, "default_fee_rate", 0.0)),
    )
    gas_usdc = float(getattr(trade_cfg, "gas_cost_usdc", 0.0))
    entry_fee, exit_fee = _compute_round_trip_fees(
        entry_price=entry_price,
        exit_price=exit_price,
        size_usdc=size_usdc,
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
                 entry_fee_usdc, exit_fee_usdc,
                 exit_best_bid, exit_best_ask, exit_fill_price)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                exit_best_bid,
                exit_best_ask,
                exit_fill,
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
            await cancel_and_join(self._task, log=logger)
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

        Cycle 27: the per-trade CLOB read is `get_top_of_book` on the
        traded token — never `/midpoint`, whose scalar silently degrades
        to the last traded price on wide-spread books. The rule mid is
        derived as `(bid + ask) / 2`; the book itself rides along to
        `_close_trade` so the exit fills at the bid. A missing/empty
        book leaves `current_mid=None`: price rules are skipped and the
        trade is held (timeout/stagnation still need a book to actually
        close under `skip_on_missing_mid=true`) — no more fictional
        exits on empty books.
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
            book: Optional[tuple[float, float]] = None
            current_mid: Optional[float] = None
            if tokens is not None:
                token_id = tokens["token_yes"] if trade["direction"] == "YES" else tokens["token_no"]
                if token_id:
                    book = await self._clob.get_top_of_book(token_id)
                    if book is not None:
                        current_mid = (float(book[0]) + float(book[1])) / 2.0
            rule = self._evaluate_rules(
                trade, now=now, timeout=timeout, current_mid=current_mid
            )
            if rule is None:
                continue
            # Re-use the book we just fetched (saves a 2nd CLOB
            # round-trip in `_close_trade`).
            if await self._close_trade(
                trade,
                close_reason=rule,
                now_iso=now.isoformat(),
                exit_book_hint=book,
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

        Cycle 27: `current_mid`, `mid_price` and (on new rows)
        `target_mid` are all position space, so every rule is "price
        rose to/past X" regardless of direction. Legacy NO rows
        (entry_fill_price IS NULL) stored target_mid in YES space —
        converted via `1 - target` on the fly. take_profit/stop_loss
        evaluate against the real cost basis (entry fill) when present.
        """
        size = float(trade["size_usdc"])
        entry_fill = trade.get("entry_fill_price")
        entry = float(entry_fill) if entry_fill is not None else float(trade["mid_price"])
        direction = trade["direction"]
        target = trade.get("target_mid")

        if current_mid is not None and size > 0.0:
            cur = float(current_mid)
            # 1. edge_decay — Kelly target reached: the traded token's
            # price rose to the model's implied value. Position space →
            # one comparison for both directions. Legacy NO rows stored
            # the target in YES space (p_hat + fee); mirror it.
            if target is not None:
                target_pos = float(target)
                if direction == "NO" and entry_fill is None:
                    target_pos = 1.0 - target_pos
                if cur >= target_pos:
                    return "edge_decay"
            # 2/3. take_profit + stop_loss — absolute %age thresholds.
            pnl_frac = _compute_pnl(direction, entry, cur, 1.0)
            if self._cfg.take_profit_pct > 0.0 and pnl_frac >= self._cfg.take_profit_pct:
                return "target"
            if self._cfg.stop_loss_pct > 0.0 and pnl_frac <= -self._cfg.stop_loss_pct:
                return "stop"

        # 4. stagnation — aged past stagnation_minutes AND mid hasn't
        # moved by more than stagnation_drift. Catalyst was priced in
        # or wasn't real; free the cash. Drift is mid-vs-entry-MID
        # (not entry fill — the half-spread paid at entry isn't price
        # movement).
        decided_at = _parse_iso(trade["decided_at"])
        if decided_at is not None:
            age = now - decided_at
            if current_mid is not None and self._cfg.stagnation_minutes > 0:
                stag_age = timedelta(minutes=int(self._cfg.stagnation_minutes))
                entry_mid = float(trade["mid_price"])
                if age >= stag_age and abs(float(current_mid) - entry_mid) < float(self._cfg.stagnation_drift):
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
                   t.target_mid, t.entry_fill_price
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
                "entry_fill_price": (
                    float(r["entry_fill_price"]) if r["entry_fill_price"] is not None else None
                ),
            }
            for r in rows
        ]

    async def _fetch_tokens(self, market_id: str) -> Optional[dict]:
        # Cycle 10: also pull `fee_rate` so the closure write can use
        # the per-market Polymarket taker rate. NULL fee_rate (legacy
        # rows / network miss at first-seen) falls back to
        # `cfg.trade.default_fee_rate` in `_resolve_fee_rate` (cycle 27
        # removed the category table from the fee path).
        cur = await self._db.conn.execute(
            "SELECT token_yes, token_no, fee_rate FROM markets WHERE condition_id = ?",
            (market_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return {
            "token_yes": row["token_yes"],
            "token_no": row["token_no"],
            "fee_rate": row["fee_rate"],
        }

    async def _close_trade(
        self,
        trade: dict,
        *,
        close_reason: str,
        now_iso: str,
        exit_book_hint: Optional[tuple[float, float]] = None,
    ) -> bool:
        """Resolve the exit book + write the closure row. Returns True
        iff a closure row was actually inserted (i.e. UNIQUE(trade_id)
        did not kick in).

        Cycle 27: the exit is priced off the traded token's
        `(best_bid, best_ask)` — fill at the bid, mid recorded as
        `(bid + ask) / 2`. Callers that already fetched the book during
        rule evaluation pass it as `exit_book_hint` so we don't pay for
        a second round-trip (timeout passes None and we fetch fresh).
        No book → hold under `skip_on_missing_mid=true` (default); the
        resolution / dead-market paths in markets.py are what eventually
        close positions whose books never come back.
        """
        direction = trade["direction"]
        book: Optional[tuple[float, float]] = exit_book_hint
        token_id: Optional[str] = None
        # Fetch fee metadata always — the hint comes from rule
        # evaluation, not the markets row.
        market_meta = await self._fetch_tokens(trade["market_id"])
        market_fee_rate = market_meta.get("fee_rate") if market_meta else None
        if book is None:
            if market_meta is None:
                logger.warning(
                    "closer: trade_id=%d market %s missing from markets — skipping",
                    trade["id"], trade["market_id"],
                )
                return False
            token_id = market_meta["token_yes"] if direction == "YES" else market_meta["token_no"]
            if token_id:
                book = await self._clob.get_top_of_book(token_id)

        if book is None:
            if self._cfg.skip_on_missing_mid:
                logger.info(
                    "closer: trade_id=%d no CLOB book for %s; holding (skip_on_missing_mid=true)",
                    trade["id"], token_id,
                )
                return False
            # Degenerate fallback: close at the entry price. PnL is 0 but
            # the trade exits the open set deterministically.
            entry_fill = trade.get("entry_fill_price")
            exit_mid = float(entry_fill) if entry_fill is not None else float(trade["mid_price"])
            logger.info(
                "closer: trade_id=%d no CLOB book; closing at entry price (skip_on_missing_mid=false)",
                trade["id"],
            )
            return await write_trade_closure(
                self._db,
                trade=trade,
                exit_mid=max(0.0, min(1.0, exit_mid)),
                close_reason=close_reason,
                trade_cfg=self._trade_cfg,
                market_fee_rate=market_fee_rate,
                now_iso=now_iso,
            )

        # Clamp into the CHECK range — a misbehaving CLOB returning a value
        # just outside [0,1] would otherwise trip the constraint and crash
        # the whole poll body.
        exit_mid = max(0.0, min(1.0, (float(book[0]) + float(book[1])) / 2.0))

        return await write_trade_closure(
            self._db,
            trade=trade,
            exit_mid=exit_mid,
            close_reason=close_reason,
            trade_cfg=self._trade_cfg,
            market_fee_rate=market_fee_rate,
            now_iso=now_iso,
            exit_book=book,
        )


if __name__ == "__main__":
    # Per modularization rule 8: every stage runs standalone for dev poking.
    # No real CLOB available here; the stage simply raises on instantiation
    # without one, which is the contract.
    import sys

    print("CloserStage requires a CLOBClient; instantiate via app.main wiring.",
          file=sys.stderr)

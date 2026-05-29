"""Backtest PnL against stored odds_observations.

Entry: first live tick at offset >= 0, crossing the book (best_ask for
YES, best_bid for NO). Exit: realistic — we cross the book again to get
out (sell YES at best_bid, sell NO at best_ask), at whichever comes
first of an optional take-profit / stop-loss / trailing trigger or the
fixed exit horizon. PnL is entry-quote to exit-quote with a flat fee_pct
on notional. Skip reasons are surfaced rather than silently zeroing PnL.

The mid-to-mid "free exit" of earlier versions is gone: both legs now
pay the half-spread, so a market that never moves loses spread + fee.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Optional


@dataclass
class TradeResult:
    exploration_id: int
    direction: str             # "YES" | "NO" | ""
    notional: float
    entry_offset_s: Optional[int]
    entry_price: Optional[float]
    exit_offset_s: Optional[int]
    exit_price: Optional[float]
    fee: float
    pnl: Optional[float]
    skip_reason: str           # "" if executed
    raw_kelly_f: Optional[float] = None
    edge: Optional[float] = None      # p_yes vs entry quote (signed by direction)
    p_yes: Optional[float] = None
    exit_reason: str = ""             # "horizon" | "take_profit" | "stop_loss" | "trailing"
    exit_spread_cost: Optional[float] = None  # half-spread paid on exit (>=0)


@dataclass
class _Tick:
    off: int
    mid: Optional[float]
    bid: Optional[float]
    ask: Optional[float]


def simulate(
    con: sqlite3.Connection,
    *,
    exploration_id: int,
    direction: str,
    notional: float,
    fee_pct: float,
    exit_horizon_seconds: int,
    fallback_entry_mid_t0: Optional[float] = None,
    skip_when_no_book: bool = True,
    exit_config: Optional[dict] = None,
) -> TradeResult:
    if direction not in ("YES", "NO"):
        return TradeResult(
            exploration_id=exploration_id,
            direction=direction or "",
            notional=notional,
            entry_offset_s=None, entry_price=None,
            exit_offset_s=None, exit_price=None,
            fee=0.0, pnl=None,
            skip_reason="not_tradeable_direction",
        )

    entry = _entry_quote(con, exploration_id, direction)
    if entry is None:
        if fallback_entry_mid_t0 is None or fallback_entry_mid_t0 != fallback_entry_mid_t0:
            if skip_when_no_book:
                return _skip(exploration_id, direction, notional, "no_entry_quote")
            entry_price, entry_offset = None, None
        else:
            entry_price = (
                fallback_entry_mid_t0
                if direction == "YES" else 1.0 - fallback_entry_mid_t0
            )
            entry_offset = 0
    else:
        entry_price, entry_offset = entry

    if entry_price is None:
        return _skip(exploration_id, direction, notional, "no_entry_quote")

    ticks = _live_ticks(con, exploration_id)
    exit_res = _resolve_exit(
        ticks, direction,
        entry_offset=entry_offset if entry_offset is not None else 0,
        horizon_s=exit_horizon_seconds,
        exit_config=exit_config,
    )
    if exit_res is None:
        return _skip(exploration_id, direction, notional, "no_exit_quote")
    exit_price, exit_offset, exit_mid_d, exit_reason = exit_res

    cost = entry_price * notional
    payoff = exit_price * notional
    fee = fee_pct * notional
    pnl = payoff - cost - fee
    # The half-spread surrendered crossing out: mark (mid) minus the
    # quote we actually received. >=0 by construction (mid >= bid we sell into).
    exit_spread_cost = (exit_mid_d - exit_price) * notional

    return TradeResult(
        exploration_id=exploration_id,
        direction=direction,
        notional=notional,
        entry_offset_s=entry_offset,
        entry_price=entry_price,
        exit_offset_s=exit_offset,
        exit_price=exit_price,
        fee=fee,
        pnl=pnl,
        skip_reason="",
        exit_reason=exit_reason,
        exit_spread_cost=exit_spread_cost,
    )


def _entry_quote(
    con: sqlite3.Connection, exploration_id: int, direction: str,
) -> Optional[tuple[float, int]]:
    col = "best_ask" if direction == "YES" else "best_bid"
    row = con.execute(
        f"SELECT {col} AS px, offset_seconds AS off FROM odds_observations "
        "WHERE exploration_id = ? AND source = 'live' "
        f"AND offset_seconds >= 0 AND {col} IS NOT NULL "
        "ORDER BY offset_seconds ASC LIMIT 1",
        (exploration_id,),
    ).fetchone()
    if row is None:
        return None
    px = float(row["px"])
    if direction == "NO":
        px = 1.0 - px
    return px, int(row["off"])


def _live_ticks(con: sqlite3.Connection, exploration_id: int) -> list[_Tick]:
    rows = con.execute(
        "SELECT offset_seconds AS off, mid, best_bid, best_ask "
        "FROM odds_observations WHERE exploration_id = ? AND source = 'live' "
        "ORDER BY offset_seconds ASC",
        (exploration_id,),
    ).fetchall()
    return [
        _Tick(
            off=int(r["off"]),
            mid=float(r["mid"]) if r["mid"] is not None else None,
            bid=float(r["best_bid"]) if r["best_bid"] is not None else None,
            ask=float(r["best_ask"]) if r["best_ask"] is not None else None,
        )
        for r in rows
    ]


def _exit_quote_at(tick: _Tick, direction: str) -> Optional[tuple[float, float]]:
    """Realistic exit quote in the bet's price space at one tick, plus the
    mark (mid) in that space. We *sell* our position: YES at best_bid,
    NO at (1 - best_ask). Fall back to the mid when that book side is NULL
    so a mid-only history tick still prices an exit."""
    if direction == "YES":
        px = tick.bid if tick.bid is not None else tick.mid
        mark = tick.mid if tick.mid is not None else px
    else:
        ask = tick.ask if tick.ask is not None else tick.mid
        px = (1.0 - ask) if ask is not None else None
        mark = (1.0 - tick.mid) if tick.mid is not None else px
    if px is None or mark is None:
        return None
    return px, mark


def _resolve_exit(
    ticks: list[_Tick],
    direction: str,
    *,
    entry_offset: int,
    horizon_s: int,
    exit_config: Optional[dict],
) -> Optional[tuple[float, int, float, str]]:
    """Return (exit_price_in_dir_space, exit_offset, exit_mark_in_dir_space,
    reason). Walks live ticks after entry; honors take_profit / stop_loss /
    trailing from exit_config, else exits at the first tick >= horizon (or
    the last available tick)."""
    post = [t for t in ticks if t.off >= entry_offset and t.mid is not None]
    if not post:
        # No marks after entry — fall back to the last live tick with any quote.
        for t in reversed(ticks):
            q = _exit_quote_at(t, direction)
            if q is not None:
                return q[0], t.off, q[1], "horizon"
        return None

    cfg = exit_config or {}
    tp = _f(cfg.get("take_profit"))
    sl = _f(cfg.get("stop_loss"))
    trail = _f(cfg.get("trailing"))

    entry_mark = _exit_quote_at(post[0], direction)
    entry_mark = entry_mark[1] if entry_mark is not None else None

    if entry_mark is not None and (tp or sl or trail):
        peak = 0.0  # best favorable move seen
        for t in post:
            q = _exit_quote_at(t, direction)
            if q is None:
                continue
            fav = q[1] - entry_mark            # favorable move (mark vs entry mark)
            peak = max(peak, fav)
            if tp is not None and fav >= tp:
                return q[0], t.off, q[1], "take_profit"
            if sl is not None and fav <= -sl:
                return q[0], t.off, q[1], "stop_loss"
            if trail is not None and peak > 0 and (peak - fav) >= trail:
                return q[0], t.off, q[1], "trailing"

    # No trigger fired — exit at the first tick at/after the horizon.
    for t in post:
        if t.off >= horizon_s:
            q = _exit_quote_at(t, direction)
            if q is not None:
                return q[0], t.off, q[1], "horizon"
    # Horizon beyond captured window: use the last tick with a usable quote.
    for t in reversed(post):
        q = _exit_quote_at(t, direction)
        if q is not None:
            return q[0], t.off, q[1], "horizon"
    return None


def _skip(exploration_id: int, direction: str, notional: float, reason: str) -> TradeResult:
    return TradeResult(
        exploration_id=exploration_id,
        direction=direction,
        notional=notional,
        entry_offset_s=None, entry_price=None,
        exit_offset_s=None, exit_price=None,
        fee=0.0, pnl=None,
        skip_reason=reason,
    )


def _f(v) -> Optional[float]:
    if v in (None, "", "None"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def entry_quote(con, exploration_id: int, direction: str):
    """Public alias of _entry_quote for callers (e.g. Kelly sizing in run_strategy)."""
    return _entry_quote(con, exploration_id, direction)


def kelly_size(
    *,
    p_yes: float,
    entry_price: float,
    direction: str,
    bankroll: float,
    kelly_fraction: float,
    max_position_pct: float,
    min_size_usdc: float,
    min_edge: float,
) -> tuple[float, float, str]:
    """Return (notional, raw_kelly_f, skip_reason).

    For YES at price P (you pay P for $1 if YES wins):
        b = (1-P)/P,  Kelly f* = (p_yes - P) / (1 - P)
    For NO at price (1 - mid_yes) — entry_price here is already 1 - best_bid:
        with q = 1 - p_yes, b = (1 - entry_price)/entry_price,
        Kelly f* = (q - entry_price) / (1 - entry_price)
    """
    if direction not in ("YES", "NO"):
        return 0.0, 0.0, "not_tradeable_direction"
    if entry_price is None or entry_price <= 0 or entry_price >= 1:
        return 0.0, 0.0, "bad_quote"
    if p_yes is None or p_yes < 0:
        return 0.0, 0.0, "missing_p_yes"

    if direction == "YES":
        edge = p_yes - entry_price
        f_star = edge / (1.0 - entry_price)
    else:
        q = 1.0 - p_yes
        edge = q - entry_price
        f_star = edge / (1.0 - entry_price)

    if edge <= min_edge:
        return 0.0, f_star, "negative_edge" if edge <= 0 else "below_min_edge"
    if f_star <= 0:
        return 0.0, f_star, "negative_kelly"

    raw_notional = bankroll * kelly_fraction * f_star
    cap = bankroll * max_position_pct
    notional = min(raw_notional, cap)
    if notional < min_size_usdc:
        return 0.0, f_star, "below_min_size"
    return notional, f_star, ""

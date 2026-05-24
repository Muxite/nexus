"""Missed-edge diagnostic plot — one PNG per sampled closed trade.

For every 4th closed trade (`trade.id % 4 == 0`), draw the market price
on a log scale across `decided_at - 1h` to `closed_at + 1h`. Annotates
entry, exit, and target_mid so the operator can eyeball whether the
trade was:

- TOO LATE: price already moved before our entry
- TOO EARLY: price kept moving in our favour after our exit
- GHOST EDGE: price flat throughout — no real signal
- TIMED WELL: price moved during the hold and reverted/plateaued after

Saves PNGs to `data/dashboard/plots/missed_edge/trade_<id>.png`. Skips
trades that already have a plot file. Runs every 5 min via the standard
plot-loop cadence. Sample modulo keeps PnL/cost ratios accurate when
not every trade is plotted.
"""

from __future__ import annotations

import asyncio
import io
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import matplotlib.dates as mdates
import numpy as np

from ._base import plt, figure_to_png

FILENAME = "missed_edge_index.png"
SAMPLE_MODULO = 4

logger = logging.getLogger(__name__)


async def render(dash, *, loop: Optional[asyncio.AbstractEventLoop] = None) -> Optional[bytes]:
    """Generate any missing missed-edge PNGs for sampled closed trades.

    Returns a small index PNG body for the standard plot registry. The
    real per-trade artifacts are written by side-effect to
    `data/dashboard/plots/missed_edge/trade_<id>.png`.
    """
    cycle_id = getattr(dash, "current_cycle_id", None)
    cycle_sql = f"AND t.cycle_id = {int(cycle_id)}" if cycle_id is not None else ""
    cur = await dash.db.conn.execute(
        f"""
        SELECT t.id, t.market_id, t.direction, t.mid_price AS entry_mid,
               t.target_mid, t.decided_at, c.closed_at, c.exit_mid_price,
               c.close_reason, c.realized_pnl_usdc,
               m.token_yes, m.token_no, m.question
        FROM trades t
        JOIN trade_closures c ON c.trade_id = t.id
        LEFT JOIN markets m ON m.condition_id = t.market_id
        WHERE t.is_tracer = 0 AND (t.id % ?) = 0 {cycle_sql}
        ORDER BY c.closed_at DESC
        LIMIT 25
        """,
        (SAMPLE_MODULO,),
    )
    rows = await cur.fetchall()
    await cur.close()

    plots_dir = Path(dash.data_dir) / "plots" / "missed_edge"
    plots_dir.mkdir(parents=True, exist_ok=True)

    clob = getattr(dash, "_clob_for_dashboard", None)
    if clob is None and dash.cfg is not None:
        from app.clob import CLOBClient
        clob = CLOBClient(
            base_url=dash.cfg.clob.base_url,
            timeout_seconds=dash.cfg.clob.timeout_seconds,
        )
        dash._clob_for_dashboard = clob

    generated = 0
    for r in rows:
        out_path = plots_dir / f"trade_{r['id']:06d}.png"
        if out_path.exists():
            continue
        if clob is None:
            continue
        token_id = r["token_yes"] if r["direction"] == "YES" else r["token_no"]
        if not token_id:
            continue
        history = await clob.get_price_history(token_id, interval="1h", fidelity=10)
        if not history or len(history) < 2:
            continue
        try:
            png_bytes = _render_single(history=history, trade=dict(r))
        except Exception:  # noqa: BLE001
            logger.exception("missed_edge: render failed for trade %d", r["id"])
            continue
        if png_bytes:
            out_path.write_bytes(png_bytes)
            generated += 1

    return _placeholder_index(generated, len(rows))


def _parse_iso(s: Any) -> Optional[datetime]:
    if not isinstance(s, str):
        return None
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _render_single(*, history: list[tuple[int, float]], trade: dict) -> bytes:
    times = [datetime.fromtimestamp(t, tz=timezone.utc) for t, _ in history]
    prices = np.array([p for _, p in history], dtype=float)
    # Clamp to avoid log(0) when a token hit a wall.
    prices = np.clip(prices, 1e-3, 1.0 - 1e-3)

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.set_yscale("log")
    ax.plot(times, prices, color="#5fa8ff", linewidth=1.5)

    entry_dt = _parse_iso(trade.get("decided_at"))
    exit_dt = _parse_iso(trade.get("closed_at"))
    entry_mid = float(trade["entry_mid"])
    exit_mid = float(trade["exit_mid_price"])
    target_mid = trade.get("target_mid")

    if entry_dt is not None:
        ax.axvline(entry_dt, color="#73c873", linestyle="--", linewidth=1, alpha=0.8, label="entry")
    if exit_dt is not None:
        ax.axvline(exit_dt, color="#e36767", linestyle="--", linewidth=1, alpha=0.8, label="exit")
    ax.axhline(entry_mid, color="#73c873", linestyle=":", linewidth=0.7, alpha=0.6)
    ax.axhline(exit_mid, color="#e36767", linestyle=":", linewidth=0.7, alpha=0.6)
    if target_mid is not None:
        ax.axhline(float(target_mid), color="#ffa940", linestyle=":", linewidth=0.8,
                   alpha=0.85, label=f"target {float(target_mid):.3f}")

    title = (
        f"trade #{trade['id']}  {trade['direction']}  "
        f"entry {entry_mid:.3f} → exit {exit_mid:.3f}  "
        f"pnl {float(trade['realized_pnl_usdc']):+.2f}  "
        f"reason {trade['close_reason']}"
    )
    sub = (trade.get("question") or "")[:80]
    ax.set_title(f"{title}\n{sub}", fontsize=9)
    ax.set_xlabel("time (UTC)")
    ax.set_ylabel("mid price (log)")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    fig.autofmt_xdate()
    ax.legend(loc="best", fontsize=8)
    return figure_to_png(fig)


def _placeholder_index(generated: int, total: int) -> bytes:
    fig, ax = plt.subplots(figsize=(4, 2))
    now_s = datetime.now(timezone.utc).isoformat(timespec="seconds")
    ax.text(
        0.5, 0.5,
        f"missed_edge plots\n{generated} new / {total} sampled\n{now_s}",
        ha="center", va="center", fontsize=10,
    )
    ax.axis("off")
    return figure_to_png(fig)

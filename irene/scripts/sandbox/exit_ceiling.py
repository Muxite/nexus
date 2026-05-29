"""Oracle exit diagnostic: how much PnL did the fixed-horizon exit leave behind?

For every executed trade in a run, re-price the exit at the *best* quote we
could have sold into anywhere in the post-news window (perfect timing, still
crossing the book realistically via pnl._exit_quote_at). The gap between this
ceiling and realized PnL is the upper bound on what a smarter take-profit /
trailing exit could recover — if the gap is small, smarter exits aren't worth
chasing; if it's large, volatility harvesting is on the table.

    python -m irene.scripts.sandbox.exit_ceiling --run <run_id>
    python -m irene.scripts.sandbox.exit_ceiling --all

Read-only. Does not write run artifacts.
"""

from __future__ import annotations

import argparse
import os
from typing import Optional

from irene.sandbox import loader, pnl, runio
from irene.sandbox.paths import RUNS_DIR


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", nargs="+", default=[])
    ap.add_argument("--all", action="store_true")
    args = ap.parse_args()

    run_ids = _resolve(args)
    if not run_ids:
        ap.error("pass --run <id> or --all")
    con = loader.connect_readonly()

    print("| run | N | realized PnL | oracle PnL | harvestable gap | mean gap/t |")
    print("|---|---:|---:|---:|---:|---:|")
    tot_real = tot_ceil = tot_n = 0
    for run_id in run_ids:
        res = _ceiling_for_run(con, run_id)
        if res is None:
            continue
        n, real, ceil = res
        tot_real += real
        tot_ceil += ceil
        tot_n += n
        gap = ceil - real
        print(f"| {run_id[:46]} | {n} | {_money(real)} | {_money(ceil)} | "
              f"{_money(gap)} | {_money(gap / n) if n else '—'} |")
    if len(run_ids) > 1 and tot_n:
        gap = tot_ceil - tot_real
        print(f"| **TOTAL** | {tot_n} | {_money(tot_real)} | {_money(tot_ceil)} | "
              f"{_money(gap)} | {_money(gap / tot_n)} |")


def _ceiling_for_run(con, run_id: str) -> Optional[tuple[int, float, float]]:
    run_dir = os.path.join(RUNS_DIR, run_id)
    try:
        manifest = runio.load_manifest(run_dir)
    except FileNotFoundError:
        return None
    fee_pct = float(manifest.get("backtest", {}).get("fee_pct", 0.02))
    trades = runio.executed(runio.load_trades(run_dir))
    if not trades:
        return None
    n = 0
    real = ceil = 0.0
    for t in trades:
        eid = t["exploration_id"]
        direction = t.get("direction") or ""
        notional = t.get("notional") or 0.0
        entry_price = t.get("entry_price")
        pnl_real = t.get("pnl")
        entry_off = t.get("entry_offset_s")
        if entry_price is None or pnl_real is None or direction not in ("YES", "NO"):
            continue
        ticks = pnl._live_ticks(con, eid)
        best_exit = None
        for tk in ticks:
            if entry_off is not None and tk.off < entry_off:
                continue
            q = pnl._exit_quote_at(tk, direction)
            if q is None:
                continue
            if best_exit is None or q[0] > best_exit:
                best_exit = q[0]
        if best_exit is None:
            continue
        ceil_pnl = best_exit * notional - entry_price * notional - fee_pct * notional
        # The ceiling can never be worse than what actually happened.
        ceil_pnl = max(ceil_pnl, pnl_real)
        n += 1
        real += pnl_real
        ceil += ceil_pnl
    if n == 0:
        return None
    return n, real, ceil


def _resolve(args) -> list[str]:
    if args.all:
        return sorted(
            name for name in os.listdir(RUNS_DIR)
            if os.path.isdir(os.path.join(RUNS_DIR, name))
            and os.path.exists(os.path.join(RUNS_DIR, name, "manifest.json"))
        )
    return list(args.run)


def _money(v) -> str:
    if not isinstance(v, (int, float)):
        return "?"
    sign = "+" if v >= 0 else "-"
    return f"${sign}{abs(v):,.2f}"


if __name__ == "__main__":
    main()

"""Rank every sandbox run by realized PnL / ROI / win-rate.

    # Top runs across the board (default: hide runs with <10 executed trades)
    python -m irene.scripts.sandbox.leaderboard

    # Loosen the trade-count floor for tiny filter sets
    python -m irene.scripts.sandbox.leaderboard --min-trades 5

    # Aggregate per-strategy (mean ROI across every set it ran on)
    python -m irene.scripts.sandbox.leaderboard --by strategy

    # Aggregate per-set
    python -m irene.scripts.sandbox.leaderboard --by set

Reads manifest.json + trades.csv from every directory under
irene/data/sandbox/runs/. Ignores any directory missing those files
(e.g. an aborted run)."""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

from irene.sandbox.paths import RUNS_DIR


@dataclass
class RunStats:
    run_id: str
    strategy: str
    set_name: str
    sizing: str
    bankroll: Optional[float]
    n_rows: int
    n_executed: int
    n_skipped: int
    total_pnl: float
    mean_pnl: Optional[float]
    win_rate: Optional[float]
    capital_deployed: float
    roi_on_deployed_pct: Optional[float]
    roi_on_bankroll_pct: Optional[float]
    sharpe_like: Optional[float]
    avg_edge: Optional[float]
    avg_notional: Optional[float]
    exit_horizon_s: Optional[int]
    min_edge: Optional[float]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-trades", type=int, default=10,
                    help="Hide runs with fewer than N executed trades.")
    ap.add_argument("--by", choices=("run", "strategy", "set"), default="run",
                    help="Aggregation axis. 'run' shows every run.")
    ap.add_argument("--sort", default="roi_on_bankroll_pct",
                    choices=("roi_on_bankroll_pct", "roi_on_deployed_pct",
                             "total_pnl", "win_rate", "sharpe_like",
                             "mean_pnl"),
                    help="Sort key (descending).")
    args = ap.parse_args()

    stats = []
    for name in sorted(os.listdir(RUNS_DIR)):
        run_dir = os.path.join(RUNS_DIR, name)
        if not os.path.isdir(run_dir):
            continue
        try:
            stats.append(_load(run_dir))
        except (FileNotFoundError, json.JSONDecodeError, KeyError):
            continue

    stats = [s for s in stats if s.n_executed >= args.min_trades]

    if args.by == "run":
        _print_run_table(stats, args.sort)
    elif args.by == "strategy":
        _print_grouped_table(stats, "strategy", args.sort)
    else:
        _print_grouped_table(stats, "set_name", args.sort)


def _load(run_dir: str) -> RunStats:
    with open(os.path.join(run_dir, "manifest.json")) as f:
        manifest = json.load(f)
    with open(os.path.join(run_dir, "trades.csv")) as f:
        trades = list(csv.DictReader(f))

    sizing = manifest["backtest"].get("sizing", "flat")
    bankroll = manifest["backtest"].get("bankroll_usdc")
    exit_horizon = manifest["backtest"].get("exit_horizon_seconds")
    min_edge = manifest["backtest"].get("min_edge")

    executed = []
    skipped = 0
    for t in trades:
        skip = (t.get("skip_reason") or "").strip()
        if skip:
            skipped += 1
            continue
        pnl = _f(t.get("pnl"))
        if pnl is None:
            skipped += 1
            continue
        executed.append({
            "pnl": pnl,
            "notional": _f(t.get("notional")) or 0.0,
            "entry_price": _f(t.get("entry_price")) or 0.0,
            "edge": _f(t.get("edge")),
        })

    n_exec = len(executed)
    total_pnl = sum(x["pnl"] for x in executed)
    mean_pnl = (total_pnl / n_exec) if n_exec else None
    wins = sum(1 for x in executed if x["pnl"] > 0)
    win_rate = (wins / n_exec * 100) if n_exec else None
    deployed = sum(x["entry_price"] * x["notional"] for x in executed)
    roi_dep = (total_pnl / deployed * 100) if deployed > 0 else None
    roi_bk = (total_pnl / bankroll * 100) if (bankroll and n_exec) else None

    # Sharpe-like: mean / stdev * sqrt(N). Not annualized, just a
    # within-run signal-to-noise floor.
    sharpe = None
    if n_exec >= 2:
        pnls = [x["pnl"] for x in executed]
        mu = sum(pnls) / n_exec
        var = sum((p - mu) ** 2 for p in pnls) / (n_exec - 1)
        sd = math.sqrt(var)
        if sd > 1e-9:
            sharpe = (mu / sd) * math.sqrt(n_exec)

    edges = [x["edge"] for x in executed if x["edge"] is not None]
    avg_edge = (sum(edges) / len(edges)) if edges else None
    avg_notional = (sum(x["notional"] for x in executed) / n_exec) if n_exec else None

    return RunStats(
        run_id=os.path.basename(run_dir),
        strategy=manifest.get("strategy", "?"),
        set_name=manifest.get("set", "?"),
        sizing=sizing,
        bankroll=bankroll if isinstance(bankroll, (int, float)) else None,
        n_rows=manifest.get("n_rows", len(trades)),
        n_executed=n_exec,
        n_skipped=skipped,
        total_pnl=total_pnl,
        mean_pnl=mean_pnl,
        win_rate=win_rate,
        capital_deployed=deployed,
        roi_on_deployed_pct=roi_dep,
        roi_on_bankroll_pct=roi_bk,
        sharpe_like=sharpe,
        avg_edge=avg_edge,
        avg_notional=avg_notional,
        exit_horizon_s=exit_horizon,
        min_edge=min_edge,
    )


def _print_run_table(stats: list[RunStats], sort_key: str) -> None:
    stats.sort(key=lambda s: _sort_val(s, sort_key), reverse=True)
    cols = [
        ("strategy", lambda s: s.strategy, 26),
        ("set", lambda s: s.set_name, 16),
        ("exit_s", lambda s: _i(s.exit_horizon_s), 7),
        ("min_e", lambda s: _f3(s.min_edge), 6),
        ("N", lambda s: str(s.n_executed), 4),
        ("skip", lambda s: str(s.n_skipped), 5),
        ("PnL", lambda s: _money(s.total_pnl, signed=True), 11),
        ("mean/t", lambda s: _money(s.mean_pnl, signed=True) if s.mean_pnl is not None else "?", 9),
        ("win%", lambda s: _pct(s.win_rate), 6),
        ("ROI(dep)", lambda s: _pct(s.roi_on_deployed_pct), 9),
        ("ROI(bk)", lambda s: _pct(s.roi_on_bankroll_pct), 9),
        ("sharpe", lambda s: _f2(s.sharpe_like), 7),
        ("avg_edge", lambda s: _f3(s.avg_edge), 9),
    ]
    _render(cols, stats)


def _print_grouped_table(stats: list[RunStats], key: str, sort_key: str) -> None:
    groups: dict = defaultdict(list)
    for s in stats:
        groups[getattr(s, key)].append(s)

    @dataclass
    class GroupRow:
        label: str
        n_runs: int
        n_executed: int
        total_pnl: float
        roi_on_bankroll_pct: Optional[float]
        roi_on_deployed_pct: Optional[float]
        win_rate: Optional[float]
        mean_pnl: Optional[float]
        sharpe_like: Optional[float]

    rows = []
    for label, runs in groups.items():
        n_exec = sum(r.n_executed for r in runs)
        tot = sum(r.total_pnl for r in runs)
        # ROI on bankroll: mean across runs (each run already normalised)
        bks = [r.roi_on_bankroll_pct for r in runs if r.roi_on_bankroll_pct is not None]
        roi_bk = (sum(bks) / len(bks)) if bks else None
        deps = [r.roi_on_deployed_pct for r in runs if r.roi_on_deployed_pct is not None]
        roi_dep = (sum(deps) / len(deps)) if deps else None
        wrs = [r.win_rate for r in runs if r.win_rate is not None]
        wr = (sum(wrs) / len(wrs)) if wrs else None
        mn = (tot / n_exec) if n_exec else None
        sh = [r.sharpe_like for r in runs if r.sharpe_like is not None]
        sharpe = (sum(sh) / len(sh)) if sh else None
        rows.append(GroupRow(label, len(runs), n_exec, tot, roi_bk, roi_dep, wr, mn, sharpe))

    key_map = {
        "roi_on_bankroll_pct": lambda g: (g.roi_on_bankroll_pct if g.roi_on_bankroll_pct is not None else -1e9),
        "roi_on_deployed_pct": lambda g: (g.roi_on_deployed_pct if g.roi_on_deployed_pct is not None else -1e9),
        "total_pnl": lambda g: g.total_pnl,
        "win_rate": lambda g: (g.win_rate if g.win_rate is not None else -1e9),
        "sharpe_like": lambda g: (g.sharpe_like if g.sharpe_like is not None else -1e9),
        "mean_pnl": lambda g: (g.mean_pnl if g.mean_pnl is not None else -1e9),
    }
    rows.sort(key=key_map.get(sort_key, key_map["roi_on_bankroll_pct"]), reverse=True)

    cols = [
        ("group", lambda r: r.label, 28),
        ("runs", lambda r: str(r.n_runs), 5),
        ("N", lambda r: str(r.n_executed), 5),
        ("PnL", lambda r: _money(r.total_pnl, signed=True), 11),
        ("mean/t", lambda r: _money(r.mean_pnl, signed=True) if r.mean_pnl is not None else "?", 9),
        ("win%", lambda r: _pct(r.win_rate), 6),
        ("ROI(dep)", lambda r: _pct(r.roi_on_deployed_pct), 9),
        ("ROI(bk)", lambda r: _pct(r.roi_on_bankroll_pct), 9),
        ("sharpe", lambda r: _f2(r.sharpe_like), 7),
    ]
    _render(cols, rows)


def _render(cols, items) -> None:
    header = "  ".join(name.ljust(w) for name, _, w in cols)
    print(header)
    print("  ".join("-" * w for _, _, w in cols))
    for it in items:
        print("  ".join(str(fn(it))[:w].ljust(w) for _, fn, w in cols))


def _sort_val(s: RunStats, key: str) -> float:
    v = getattr(s, key, None)
    return v if isinstance(v, (int, float)) else -1e9


def _f(v) -> Optional[float]:
    if v in (None, "", "None"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _money(v, signed=False) -> str:
    if v is None:
        return "?"
    sign = "+" if signed and v >= 0 else ""
    return f"${sign}{v:,.2f}"


def _pct(v) -> str:
    return f"{v:+.2f}%" if isinstance(v, (int, float)) else "?"


def _f2(v) -> str:
    return f"{v:.2f}" if isinstance(v, (int, float)) else "?"


def _f3(v) -> str:
    return f"{v:.3f}" if isinstance(v, (int, float)) else "?"


def _i(v) -> str:
    return str(int(v)) if isinstance(v, (int, float)) else "?"


if __name__ == "__main__":
    main()

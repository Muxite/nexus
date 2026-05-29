"""Decompose where a sandbox run's PnL actually went.

Every executed trade's realized PnL splits exactly into four additive
components (validated against pnl.simulate):

    pnl = directional_drift - entry_spread_cost - exit_spread_cost - fee

  - directional_drift   = (exit_mid - entry_mid) signed by bet direction × notional
                          — the move we'd have captured with zero friction.
  - entry_spread_cost   = (entry_price - entry_mid) × notional  (≥0)
                          — half-spread paid crossing the book on entry.
  - exit_spread_cost    = (exit_mid - exit_price) × notional     (≥0)
                          — half-spread paid on exit. Zero for legacy runs whose
                            exit took the mid for free (pre bid/ask exit change).
  - fee                 = fee_pct × notional.

It then tags each trade — flat (market never moved), extreme (mid near 0/1),
mismatch (production llm_related=0, e.g. Oura-Ring↔Las-Vegas-Aces), wide
(fat entry spread) — and prints counterfactuals: how much PnL we'd recover by
skipping each junk class. This is the "fees vs bad trades" report.

    python -m irene.scripts.sandbox.loss_attribution --run <run_id>
    python -m irene.scripts.sandbox.loss_attribution --run <id_a> <id_b>
    python -m irene.scripts.sandbox.loss_attribution --all
    python -m irene.scripts.sandbox.loss_attribution --all --top 30
"""

from __future__ import annotations

import argparse
import os
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

from irene.sandbox import features, loader, runio
from irene.sandbox.paths import RUNS_DIR

# A market that never deviates more than this (in YES-price terms) from its
# anchor during the post-news window is "flat" — no move to trade.
FLAT_EPS = 0.01
EXTREME_LO, EXTREME_HI = 0.05, 0.95
WIDE_SPREAD = 0.03   # per-unit entry half-spread above this is "wide"


@dataclass
class TradeBreakdown:
    exploration_id: int
    run_id: str
    direction: str
    notional: float
    pnl: float
    directional_drift: float
    entry_spread_cost: float
    exit_spread_cost: float
    fee: float
    tags: frozenset
    article: str
    market: str


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", nargs="+", default=[],
                    help="One or more run_ids. Use --all for every run.")
    ap.add_argument("--all", action="store_true",
                    help="Attribute across every run under runs/.")
    ap.add_argument("--top", type=int, default=20,
                    help="How many worst trades (by |PnL|) to list.")
    ap.add_argument("--flat-eps", type=float, default=FLAT_EPS)
    args = ap.parse_args()

    run_ids = _resolve_runs(args)
    if not run_ids:
        ap.error("no runs found; pass --run <id> or --all")

    con = loader.connect_readonly()
    feat_cache: dict[int, features.TradeFeatures] = {}
    breakdowns: list[TradeBreakdown] = []
    skipped_no_mid = 0

    for run_id in run_ids:
        run_dir = os.path.join(RUNS_DIR, run_id)
        try:
            manifest = runio.load_manifest(run_dir)
        except FileNotFoundError:
            continue
        fee_pct = float(manifest.get("backtest", {}).get("fee_pct", 0.02))
        trades = runio.executed(runio.load_trades(run_dir))
        if not trades:
            continue
        rows = {r.exploration_id: r
                for r in loader.load_rows(con, [t["exploration_id"] for t in trades])}
        for t in trades:
            bd = _breakdown(con, run_id, t, fee_pct, rows, feat_cache, args.flat_eps)
            if bd is None:
                skipped_no_mid += 1
                continue
            breakdowns.append(bd)

    if not breakdowns:
        print("no executed trades with recoverable mids")
        return

    label = run_ids[0] if len(run_ids) == 1 else f"{len(run_ids)} runs"
    _report(label, breakdowns, args.top, skipped_no_mid)


def _breakdown(con, run_id, t, fee_pct, rows, feat_cache, flat_eps) -> Optional[TradeBreakdown]:
    eid = t["exploration_id"]
    direction = t.get("direction") or ""
    notional = t.get("notional") or 0.0
    pnl = t.get("pnl")
    entry_price = t.get("entry_price")
    exit_price = t.get("exit_price")
    if pnl is None or entry_price is None or exit_price is None:
        return None

    entry_mid = _live_mid_at(con, eid, t.get("entry_offset_s"))
    exit_mid = _live_mid_at(con, eid, t.get("exit_offset_s"))
    if entry_mid is None or exit_mid is None:
        return None
    # Project mids into the bet's price space (pnl.py stores entry/exit_price there).
    entry_mid_d = entry_mid if direction == "YES" else 1.0 - entry_mid
    exit_mid_d = exit_mid if direction == "YES" else 1.0 - exit_mid

    directional_drift = (exit_mid_d - entry_mid_d) * notional
    entry_spread_cost = (entry_price - entry_mid_d) * notional
    exit_spread_cost = (exit_mid_d - exit_price) * notional
    fee = fee_pct * notional

    if eid not in feat_cache:
        feat_cache[eid] = features.compute(con, eid, direction=direction)
    f = feat_cache[eid]
    row = rows.get(eid)

    tags = set()
    exc = f.max_excursion_signed
    if exc is None or abs(exc) < flat_eps:
        tags.add("flat")
    mid0 = f.mid_t0 if f.mid_t0 is not None else (row.market_mid_t0 if row else None)
    if mid0 is not None and (mid0 <= EXTREME_LO or mid0 >= EXTREME_HI):
        tags.add("extreme")
    if row is not None and not row.orig_llm_related:
        tags.add("mismatch")
    if notional > 0 and (entry_spread_cost / notional) > WIDE_SPREAD:
        tags.add("wide")

    return TradeBreakdown(
        exploration_id=eid, run_id=run_id, direction=direction,
        notional=notional, pnl=pnl,
        directional_drift=directional_drift,
        entry_spread_cost=entry_spread_cost,
        exit_spread_cost=exit_spread_cost,
        fee=fee, tags=frozenset(tags),
        article=(row.article_title if row else "") or "",
        market=(row.market_question if row else "") or "",
    )


def _report(label: str, bds: list[TradeBreakdown], top: int, skipped: int) -> None:
    n = len(bds)
    tot_pnl = sum(b.pnl for b in bds)
    drift = sum(b.directional_drift for b in bds)
    espread = sum(b.entry_spread_cost for b in bds)
    xspread = sum(b.exit_spread_cost for b in bds)
    fees = sum(b.fee for b in bds)

    print(f"# Loss attribution — {label}")
    print(f"_executed trades: {n}"
          + (f"  ·  skipped (no recoverable mid): {skipped}" if skipped else "") + "_")
    print()
    print(f"**Total PnL: {_money(tot_pnl)}**  ·  "
          f"mean/trade {_money(tot_pnl / n)}  ·  "
          f"win-rate {100 * sum(1 for b in bds if b.pnl > 0) / n:.1f}%")
    print()
    print("## PnL decomposition")
    print()
    print("| component | total | mean/trade | note |")
    print("|---|---:|---:|---|")
    print(f"| directional drift (move) | {_money(drift)} | {_money(drift / n)} | "
          f"what we'd net with zero friction |")
    print(f"| − entry spread cost | {_money(-espread)} | {_money(-espread / n)} | "
          f"half-spread crossed on entry |")
    print(f"| − exit spread cost | {_money(-xspread)} | {_money(-xspread / n)} | "
          f"half-spread crossed on exit (0 = legacy mid-exit) |")
    print(f"| − fees | {_money(-fees)} | {_money(-fees / n)} | flat fee_pct × notional |")
    print(f"| **= total PnL** | **{_money(tot_pnl)}** | **{_money(tot_pnl / n)}** | |")
    print()
    friction = espread + xspread + fees
    if abs(drift) > 1e-9 or abs(friction) > 1e-9:
        print(f"_Friction (spread+fees) = {_money(-friction)}.  "
              f"Directional drift = {_money(drift)}.  "
              f"Friction is {(friction / abs(tot_pnl) * 100 if tot_pnl else 0):.0f}% "
              f"the size of total PnL._")
        print()

    # ----- counterfactuals by tag -----
    print("## Counterfactual: skip the junk")
    print()
    print("| skip class | N | their PnL | PnL of the rest | mean/t (rest) |")
    print("|---|---:|---:|---:|---:|")
    for tag in ("flat", "extreme", "mismatch", "wide"):
        tagged = [b for b in bds if tag in b.tags]
        rest = [b for b in bds if tag not in b.tags]
        _cf_row(tag, tagged, rest)
    any_junk = [b for b in bds if b.tags & {"flat", "extreme", "mismatch"}]
    clean = [b for b in bds if not (b.tags & {"flat", "extreme", "mismatch"})]
    _cf_row("any (flat∪extreme∪mismatch)", any_junk, clean)
    print()
    print(f"_Tag rules: flat=|max excursion|<{FLAT_EPS} (never moved), "
          f"extreme=mid≤{EXTREME_LO} or ≥{EXTREME_HI}, "
          f"mismatch=production llm_related=0, wide=entry half-spread>{WIDE_SPREAD}._")
    print()

    # ----- worst trades -----
    worst = sorted(bds, key=lambda b: b.pnl)[:top]
    print(f"## {len(worst)} worst trades by PnL")
    print()
    print("| eid | dir | PnL | tags | drift | e.spread | fee | news → market |")
    print("|---:|:--:|---:|---|---:|---:|---:|---|")
    for b in worst:
        tagstr = ",".join(sorted(b.tags)) or "—"
        pair = f"{b.article[:42]} → {b.market[:34]}"
        print(f"| {b.exploration_id} | {b.direction or '-'} | {_money(b.pnl)} | "
              f"{tagstr} | {_money(b.directional_drift)} | {_money(-b.entry_spread_cost)} | "
              f"{_money(-b.fee)} | {pair} |")


def _cf_row(label: str, tagged: list[TradeBreakdown], rest: list[TradeBreakdown]) -> None:
    n_t = len(tagged)
    pnl_t = sum(b.pnl for b in tagged)
    pnl_r = sum(b.pnl for b in rest)
    mean_r = (pnl_r / len(rest)) if rest else None
    print(f"| {label} | {n_t} | {_money(pnl_t)} | {_money(pnl_r)} | "
          f"{_money(mean_r) if mean_r is not None else '—'} |")


def _live_mid_at(con, eid: int, off) -> Optional[float]:
    if off is None:
        return None
    r = con.execute(
        "SELECT mid FROM odds_observations WHERE exploration_id = ? "
        "AND source = 'live' AND offset_seconds = ? AND mid IS NOT NULL",
        (eid, off),
    ).fetchone()
    if r is not None:
        return float(r["mid"])
    r = con.execute(
        "SELECT mid FROM odds_observations WHERE exploration_id = ? "
        "AND source = 'live' AND offset_seconds >= ? AND mid IS NOT NULL "
        "ORDER BY offset_seconds ASC LIMIT 1",
        (eid, off),
    ).fetchone()
    return float(r["mid"]) if r is not None else None


def _resolve_runs(args) -> list[str]:
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

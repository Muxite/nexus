"""Per-trade feature table for a sandbox run.

    python -m irene.scripts.sandbox.feature_table --run <run_id>

Writes two files into the run directory:
  features.md   — markdown table sorted wins-first then by |PnL|.
  features.json — same data, structured (one object per executed trade).

Each row carries the price-action features needed to judge whether the
trade caught a real news edge: pre-news slope, post-news slope/drift,
the 60-second post-news jump, volatility, max excursion, plus an ASCII
sparkline of the full mid trajectory.
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Optional

from irene.sandbox import features, loader, runio
from irene.sandbox.paths import RUNS_DIR


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    args = ap.parse_args()

    run_dir = os.path.join(RUNS_DIR, args.run)
    manifest = runio.load_manifest(run_dir)
    trades = runio.load_trades(run_dir)
    decisions_by_eid = {d["exploration_id"]: d for d in runio.load_decisions(run_dir)}
    executed = runio.executed(trades)
    executed.sort(key=lambda t: (0 if t["pnl"] > 0 else 1, -abs(t["pnl"])))

    con = loader.connect_readonly()

    rows = []
    for t in executed:
        eid = t["exploration_id"]
        f = features.compute(con, eid, direction=t.get("direction", "") or "")
        d = decisions_by_eid.get(eid, {})
        p_yes = d.get("p_yes")
        rows.append({
            "exploration_id": eid,
            "direction": t.get("direction", ""),
            "pnl": t.get("pnl"),
            "win": (t["pnl"] or 0) > 0,
            "notional": t.get("notional"),
            "entry_offset_s": t.get("entry_offset_s"),
            "exit_offset_s": t.get("exit_offset_s"),
            "p_yes": p_yes if isinstance(p_yes, (int, float)) and p_yes >= 0 else None,
            "features": f.to_dict(),
        })

    _write_markdown(os.path.join(run_dir, "features.md"), manifest, rows)
    _write_json(os.path.join(run_dir, "features.json"), manifest, rows)
    print(f"wrote {run_dir}/features.md and features.json  ({len(rows)} trades)")


def _write_markdown(path: str, manifest: dict, rows: list[dict]) -> None:
    lines = []
    lines.append(f"# Trade features — {manifest.get('strategy')} × {manifest.get('set')}")
    lines.append(f"_run_id: {manifest.get('run_id')}_  ·  {len(rows)} executed trades")
    lines.append("")
    lines.append("Legend: `pre_slope` / `post_slope` are mid per minute. "
                 "`jump60s` is the mid 60s post-news minus the last pre-news mid. "
                 "`drift` is last live mid minus mid_t0 anchor. "
                 "`exc` is signed maximum excursion in the bet direction "
                 "(positive = price moved in our favor at some point). "
                 "`spark` plots the full mid trajectory pre+post in 16 chars.")
    lines.append("")
    lines.append(
        "| eid | dir | PnL | win | p_yes | mid_t0 | pre_slp | post_slp | "
        "jump60s | drift | exc | post_vol | spark |"
    )
    lines.append(
        "|---:|:--:|---:|:--:|---:|---:|---:|---:|---:|---:|---:|---:|---|"
    )
    for r in rows:
        f = r["features"]
        lines.append(
            f"| {r['exploration_id']} "
            f"| {r['direction'] or '-'} "
            f"| {_money(r['pnl'])} "
            f"| {'✓' if r['win'] else '✗'} "
            f"| {_f3(r['p_yes'])} "
            f"| {_f3(f.get('mid_t0'))} "
            f"| {_f4(f.get('pre_slope_per_min'))} "
            f"| {_f4(f.get('post_slope_per_min'))} "
            f"| {_f3(f.get('jump_60s'))} "
            f"| {_f3(f.get('post_drift_total'))} "
            f"| {_f3(f.get('max_excursion_signed'))} "
            f"| {_f3(f.get('post_stdev'))} "
            f"| `{f.get('sparkline','')}` |"
        )
    lines.append("")
    lines.append("## Aggregate")
    agg = _aggregate(rows)
    for label, value in agg.items():
        lines.append(f"- **{label}**: {value}")
    with open(path, "w") as fh:
        fh.write("\n".join(lines) + "\n")


def _write_json(path: str, manifest: dict, rows: list[dict]) -> None:
    out = {
        "run_id": manifest.get("run_id"),
        "strategy": manifest.get("strategy"),
        "set": manifest.get("set"),
        "n_executed": len(rows),
        "trades": rows,
    }
    with open(path, "w") as fh:
        json.dump(out, fh, indent=2)


def _aggregate(rows: list[dict]) -> dict:
    if not rows:
        return {"trades": "0"}
    wins = [r for r in rows if r["win"]]
    losses = [r for r in rows if not r["win"]]
    drift_dir_agreement = sum(
        1 for r in rows
        if r["direction"] in ("YES", "NO") and isinstance(r["features"].get("post_drift_total"), (int, float))
        and ((r["features"]["post_drift_total"] > 0 and r["direction"] == "YES") or
             (r["features"]["post_drift_total"] < 0 and r["direction"] == "NO"))
    )
    n_with_drift = sum(
        1 for r in rows if isinstance(r["features"].get("post_drift_total"), (int, float))
    )

    def _mean(rs, key):
        vals = [r["features"].get(key) for r in rs]
        vals = [v for v in vals if isinstance(v, (int, float))]
        return sum(vals) / len(vals) if vals else None

    return {
        "wins": f"{len(wins)} / {len(rows)} ({100*len(wins)/len(rows):.1f}%)",
        "post-news drift agrees with bet direction":
            f"{drift_dir_agreement} / {n_with_drift} "
            f"({(100*drift_dir_agreement/n_with_drift if n_with_drift else 0):.1f}%)",
        "mean post-news drift (winners)": _f4(_mean(wins, 'post_drift_total')),
        "mean post-news drift (losers)":  _f4(_mean(losses, 'post_drift_total')),
        "mean post-news vol (winners)":   _f4(_mean(wins, 'post_stdev')),
        "mean post-news vol (losers)":    _f4(_mean(losses, 'post_stdev')),
        "mean signed excursion (winners)": _f4(_mean(wins, 'max_excursion_signed')),
        "mean signed excursion (losers)":  _f4(_mean(losses, 'max_excursion_signed')),
    }


def _money(v) -> str:
    if not isinstance(v, (int, float)):
        return "?"
    sign = "+" if v >= 0 else ""
    return f"${sign}{v:,.2f}"


def _f3(v) -> str:
    return f"{v:.3f}" if isinstance(v, (int, float)) else "?"


def _f4(v) -> str:
    return f"{v:+.4f}" if isinstance(v, (int, float)) else "?"


if __name__ == "__main__":
    main()

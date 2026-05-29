"""Diff two strategy runs on the same set.

    python -m irene.scripts.sandbox.compare_runs --a <run_id_a> --b <run_id_b>
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from collections import Counter

from irene.sandbox.paths import RUNS_DIR
from irene.sandbox.report import fmt_float


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--a", required=True)
    ap.add_argument("--b", required=True)
    args = ap.parse_args()

    a = _load(args.a)
    b = _load(args.b)

    if a["manifest"]["set"] != b["manifest"]["set"]:
        print(f"WARN: runs use different sets ({a['manifest']['set']} vs "
              f"{b['manifest']['set']}) — comparing by exploration_id intersection")

    a_ids = {t["exploration_id"] for t in a["trades"]}
    b_ids = {t["exploration_id"] for t in b["trades"]}
    common = a_ids.intersection(b_ids)

    a_trade = {t["exploration_id"]: t for t in a["trades"]}
    b_trade = {t["exploration_id"]: t for t in b["trades"]}
    a_dec = {d["exploration_id"]: d for d in a["decisions"]}
    b_dec = {d["exploration_id"]: d for d in b["decisions"]}

    a_pnl = _total_pnl(a["trades"])
    b_pnl = _total_pnl(b["trades"])

    direction_flips = []
    skip_changes = []
    pnl_deltas = []
    for eid in common:
        ad = a_dec.get(eid, {})
        bd = b_dec.get(eid, {})
        if (ad.get("direction") or "") != (bd.get("direction") or ""):
            direction_flips.append((eid, ad.get("direction") or "-", bd.get("direction") or "-"))
        at = a_trade.get(eid, {})
        bt = b_trade.get(eid, {})
        if bool(at.get("skip_reason")) != bool(bt.get("skip_reason")):
            skip_changes.append((eid, at.get("skip_reason") or "-", bt.get("skip_reason") or "-"))
        ap_val = at.get("pnl")
        bp_val = bt.get("pnl")
        if ap_val is not None and bp_val is not None and abs(ap_val - bp_val) > 0.001:
            pnl_deltas.append((eid, ap_val, bp_val, bp_val - ap_val))

    print(f"# Compare {args.a} vs {args.b}")
    print(f"- set A: {a['manifest']['set']}  strategy A: {a['manifest']['strategy']} ({a['manifest']['strategy_version']})")
    print(f"- set B: {b['manifest']['set']}  strategy B: {b['manifest']['strategy']} ({b['manifest']['strategy_version']})")
    print(f"- common exploration_ids: {len(common)}")
    print()
    print(f"## PnL")
    print(f"- A total: {fmt_float(a_pnl, '${:+.2f}')}  (n trades exec: {sum(1 for t in a['trades'] if not t.get('skip_reason'))})")
    print(f"- B total: {fmt_float(b_pnl, '${:+.2f}')}  (n trades exec: {sum(1 for t in b['trades'] if not t.get('skip_reason'))})")
    print(f"- ΔPnL (B-A): {fmt_float(b_pnl - a_pnl, '${:+.2f}')}")
    print()
    print(f"## Direction flips: {len(direction_flips)}")
    for eid, a_dir, b_dir in direction_flips[:30]:
        print(f"- id={eid}: A={a_dir} → B={b_dir}")
    if len(direction_flips) > 30:
        print(f"  ... and {len(direction_flips) - 30} more")

    print(f"\n## Skip changes: {len(skip_changes)}")
    for eid, a_skip, b_skip in skip_changes[:30]:
        print(f"- id={eid}: A=`{a_skip}` → B=`{b_skip}`")

    pnl_deltas.sort(key=lambda x: abs(x[3]), reverse=True)
    print(f"\n## Largest PnL deltas (top 15)")
    for eid, ap_val, bp_val, d in pnl_deltas[:15]:
        print(f"- id={eid}: A={fmt_float(ap_val, '${:+.2f}')}  B={fmt_float(bp_val, '${:+.2f}')}  Δ={fmt_float(d, '${:+.2f}')}")


def _load(run_id: str) -> dict:
    base = os.path.join(RUNS_DIR, run_id)
    if not os.path.isdir(base):
        raise SystemExit(f"run not found: {base}")
    with open(os.path.join(base, "manifest.json")) as f:
        manifest = json.load(f)
    with open(os.path.join(base, "decisions.jsonl")) as f:
        decisions = [json.loads(line) for line in f if line.strip()]
    trades = []
    with open(os.path.join(base, "trades.csv")) as f:
        for row in csv.DictReader(f):
            for k in ("entry_offset_s", "exit_offset_s"):
                row[k] = int(row[k]) if row[k] not in ("", None) else None
            for k in ("notional", "entry_price", "exit_price", "fee", "pnl"):
                row[k] = float(row[k]) if row[k] not in ("", None) else None
            trades.append(row)
    return {"manifest": manifest, "decisions": decisions, "trades": trades}


def _total_pnl(trades: list[dict]) -> float:
    return sum(t["pnl"] for t in trades if t.get("pnl") is not None)


if __name__ == "__main__":
    main()

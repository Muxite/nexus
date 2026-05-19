"""backtest-min-edge — replay historical AI signals through the current
edge model and report whether the Phase E.9 cut (min_edge 0.03 → 0.015)
would change outcomes.

Methodology
-----------
We don't re-call the LLM. Each historical row already carries the
ClassifyResult signal (`direction`, `confidence_raw`) and market state
(`mid_yes`/`mid_no`/`mid_price`). Per-row we re-apply the trader edge
formula `edge = edge_increment × (2·conf − 1)`, then compare against
OLD and NEW min_edge floors.

What we MUST acknowledge
------------------------
* No re-AI. The LLM signal is frozen as logged. We're measuring the
  trader gate's response to that signal under different config.
* Post-AI rule kills (e.g. RULE_high_confidence_anti_informative_SKIP)
  are honored exactly as they fired live — we don't try to re-evaluate
  them. A row killed by that rule stays killed in both columns.
* Sizing/Kelly/cluster/duplicate gates are skipped — they need
  per-engine bankroll state we don't have. So the "would_execute" count
  is an UPPER BOUND on what would actually fire live; treat it as
  "signals admitted to the sizing stage", not realized trades.
* Anti-informative-rule kills are reported as a separate diagnostic so
  we can see how much extra unblocking dropping THAT rule would add.

Usage
-----
    python3 seulgi/scripts/analysis/backtest_min_edge.py [--start ISO] [--end ISO]

Defaults: pre-drought window (2026-05-18T21:00 → 2026-05-19T01:30 UTC).
"""
from __future__ import annotations

import argparse
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

DEFAULT_START = "2026-05-18T21:00"
DEFAULT_END = "2026-05-19T01:30"

# Per-variant config snapshot — taken from seulgi/config.yaml. Kept inline
# (rather than parsed) so this script is reproducible from a single file.
VARIANT_CFG = {
    # Phase E.9 live config — same shape pre/post; only min_edge changed.
    "balanced":          {"min_confidence_default": 0.40, "edge_increment": 0.12},
    "inverted_highconf": {"min_confidence_default": 0.40, "edge_increment": 0.12},
    # Retired aggressive engine (cycle 9 cut, 2026-05-19T02:15). Included
    # so historical rows still tally — but they wouldn't fire today
    # regardless of min_edge because the variant is gone.
    "aggressive":        {"min_confidence_default": 0.40, "edge_increment": 0.20},
}

# Per-variant min_confidence overrides (conf45_* variants raise it to 0.45).
PER_VARIANT_MIN_CONF = {
    "balanced/conf45_tp030_sl050": 0.45,
    "inverted_highconf/conf45_tp030_sl050_inv75": 0.45,
}

OLD_MIN_EDGE = 0.03
NEW_MIN_EDGE = 0.015


def edge_for(conf: float, edge_increment: float) -> float:
    return edge_increment * max(0.0, 2.0 * conf - 1.0)


def variant_root(variant_name: str) -> str:
    return variant_name.split("/", 1)[0] if variant_name else ""


def replay(db_path: str, start: str, end: str) -> dict:
    conn = sqlite3.connect(db_path, timeout=15)
    rows = conn.execute(
        """
        SELECT decided_at, variant_name, direction, confidence_raw,
               mid_yes, mid_no, mid_price, skip_reason
          FROM trades
         WHERE direction IS NOT NULL
           AND direction != 'NEUTRAL'
           AND confidence_raw IS NOT NULL
           AND decided_at >= ?
           AND decided_at <= ?
        """,
        (start, end),
    ).fetchall()
    conn.close()

    by_variant = defaultdict(lambda: dict(
        rows=0,
        live_executed=0,           # skip_reason was NULL
        live_skipped=Counter(),    # bucket by reason
        old_admit=0,               # would pass min_edge=0.03 + min_confidence
        new_admit=0,               # would pass min_edge=0.015 + min_confidence
        anti_informative_kills=0,  # rows blocked by the conf>=0.75 rule
        below_min_conf=0,          # rows that fail min_confidence
        zero_edge=0,               # conf <= 0.5 → edge=0
        edge_in_old=0,             # edge ≥ 0.03
        edge_in_e9=0,              # 0.015 ≤ edge < 0.03 (the NEW-only band)
    ))

    for decided_at, variant_name, direction, conf, mid_yes, mid_no, mid_price, skip_reason in rows:
        v = by_variant[variant_name or "<unknown>"]
        v["rows"] += 1
        if skip_reason is None:
            v["live_executed"] += 1
        else:
            v["live_skipped"][skip_reason.split(":", 1)[0]] += 1
            if skip_reason.startswith("RULE_high_confidence_anti_informative"):
                v["anti_informative_kills"] += 1

        root = variant_root(variant_name or "")
        cfg = VARIANT_CFG.get(root)
        if cfg is None:
            continue
        edge_inc = cfg["edge_increment"]
        min_conf = PER_VARIANT_MIN_CONF.get(variant_name, cfg["min_confidence_default"])

        e = edge_for(conf, edge_inc)
        if e <= 0:
            v["zero_edge"] += 1
        if e >= OLD_MIN_EDGE:
            v["edge_in_old"] += 1
        if NEW_MIN_EDGE <= e < OLD_MIN_EDGE:
            v["edge_in_e9"] += 1

        if conf < min_conf:
            v["below_min_conf"] += 1
            continue

        # Honor live post-AI rule kills (we can't reproduce them faithfully
        # but we know they fired — exclude from admit counts).
        if skip_reason is not None and skip_reason.startswith("RULE_"):
            continue

        if e >= OLD_MIN_EDGE:
            v["old_admit"] += 1
        if e >= NEW_MIN_EDGE:
            v["new_admit"] += 1

    return dict(by_variant)


def render(report: dict, start: str, end: str) -> str:
    lines = [
        "# Backtest — Phase E.9 (min_edge 0.03 → 0.015)",
        "",
        f"Window: `{start}` → `{end}` UTC",
        "",
        "Replays AI signals already logged in `trades`. For each non-NEUTRAL row,",
        "re-applies `edge = edge_increment × (2·conf − 1)` and the variant's",
        "`min_confidence`, then counts how many would be admitted under the",
        "old (`min_edge=0.03`) vs new (`min_edge=0.015`) gate.",
        "",
        "Rows killed by post-AI rules (e.g. RULE_high_confidence_anti_informative)",
        "are excluded from both admit columns — we trust the logged kill.",
        "",
        "| variant | rows | live_exec | edge≥0.03 | new admits (0.015≤e<0.03) | new total | anti-inf kills | <min_conf |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    totals = Counter()
    for variant, v in sorted(report.items()):
        new_only = v["new_admit"] - v["old_admit"]
        lines.append(
            f"| `{variant}` | {v['rows']} | {v['live_executed']} | "
            f"{v['old_admit']} | **+{new_only}** | {v['new_admit']} | "
            f"{v['anti_informative_kills']} | {v['below_min_conf']} |"
        )
        for k in ("rows", "live_executed", "old_admit", "new_admit",
                  "anti_informative_kills", "below_min_conf", "edge_in_e9"):
            totals[k] += v[k]
    lines += [
        f"| **TOTAL** | {totals['rows']} | {totals['live_executed']} | "
        f"{totals['old_admit']} | **+{totals['new_admit']-totals['old_admit']}** | "
        f"{totals['new_admit']} | {totals['anti_informative_kills']} | "
        f"{totals['below_min_conf']} |",
        "",
        "## Reading the table",
        "",
        "* `live_exec` is what actually executed on the historical pass (with",
        "  whatever min_edge was live at the time, plus all real-time gates).",
        "* `edge≥0.03` is the dry-run replay under the old min_edge. It can",
        "  be higher than `live_exec` because we don't re-apply sizing/Kelly/",
        "  duplicate/max-positions — treat it as the AI-stage admit ceiling.",
        "* `new admits` is purely the rows that move from skipped to admitted",
        "  by the E.9 cut. If this is near zero, E.9 doesn't help this slice.",
        "* `anti-inf kills` shows how many rows the post-AI rule blocks; that",
        "  population is the ceiling for any future rule-relaxation experiment.",
    ]
    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="seulgi/data/seulgi.db")
    p.add_argument("--start", default=DEFAULT_START)
    p.add_argument("--end", default=DEFAULT_END)
    p.add_argument("--out", default="seulgi/REPORT_BACKTEST_E9.md")
    args = p.parse_args()

    report = replay(args.db, args.start, args.end)
    md = render(report, args.start, args.end)
    Path(args.out).write_text(md)
    print(md)
    print(f"\nWrote {args.out}")


if __name__ == "__main__":
    main()

"""historical-postmortem — full forensic pass over closed trades.

Slices the lifetime trade log along axes the H8 overnight report and the
DECISION_FUNNEL doc didn't cover individually: per-market PnL, per-source
ROI, hour-of-day heatmap, conf-vs-realized calibration. Goal is to surface
moves that the cycle-by-cycle commit history would otherwise hide.

Excludes `close_reason IN ('cycle_reset', 'engine_retired')` everywhere —
those are administrative closes, not strategy outcomes. Includes cycle 9
+ cycle 2 in one report because the strategy regime is essentially the
same engine family across both.

Usage:
    python3 seulgi/scripts/analysis/historical_postmortem.py [--db PATH] [--out PATH]
"""
from __future__ import annotations

import argparse
import math
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import urlparse


def open_db(path: str) -> sqlite3.Connection:
    c = sqlite3.connect(path, timeout=15)
    c.row_factory = sqlite3.Row
    return c


def closed_rows(c: sqlite3.Connection) -> list[sqlite3.Row]:
    """Strategy-relevant closed trades only."""
    return c.execute(
        """
        SELECT t.realized_pnl, t.direction, t.confidence_raw, t.size_usdc,
               t.mid_yes, t.mid_price, t.freshness_label, t.close_reason,
               t.decided_at, t.closed_at, t.variant_name, t.condition_id,
               t.question, t.engine_name,
               s.site_url
          FROM trades t
          LEFT JOIN signals s ON t.signal_id = s.id
         WHERE t.realized_pnl IS NOT NULL
           AND t.close_reason NOT IN ('cycle_reset', 'engine_retired')
        """
    ).fetchall()


def fmt_dollar(x: float) -> str:
    return f"${x:+.2f}"


def section_overview(rows: list[sqlite3.Row]) -> list[str]:
    n = len(rows)
    wins = sum(1 for r in rows if r["realized_pnl"] > 0)
    losses = sum(1 for r in rows if r["realized_pnl"] < 0)
    zero = n - wins - losses
    sum_pnl = sum(r["realized_pnl"] for r in rows)
    avg = sum_pnl / n if n else 0.0
    sum_size = sum(r["size_usdc"] or 0 for r in rows)
    roi_on_stake = sum_pnl / sum_size if sum_size else 0.0
    return [
        "## Overview",
        "",
        f"* Closed trades scored: **{n}** (excludes cycle_reset, engine_retired).",
        f"* Wins / losses / zero: {wins} / {losses} / {zero} (hit% {wins / n:.1%})" if n else "* No data.",
        f"* Net realized PnL: **{fmt_dollar(sum_pnl)}** (avg per trade {fmt_dollar(avg)})",
        f"* ROI on staked capital: **{roi_on_stake:.2%}** (lifetime, all engines)",
    ]


def bucket(rows, key, buckets):
    """Group rows into named buckets via predicate(row) -> bucket_name."""
    g = defaultdict(list)
    for r in rows:
        try:
            v = key(r)
        except Exception:
            continue
        if v is None:
            continue
        for name, lo, hi in buckets:
            if (lo is None or v >= lo) and (hi is None or v < hi):
                g[name].append(r)
                break
    return g


def render_bucket_table(title, groups, order):
    lines = [f"### {title}", "", "| bucket | n | hit% | sum_pnl | avg_pnl |", "|---|---:|---:|---:|---:|"]
    for name in order:
        rs = groups.get(name, [])
        n = len(rs)
        if n == 0:
            lines.append(f"| `{name}` | 0 | — | — | — |")
            continue
        wins = sum(1 for r in rs if r["realized_pnl"] > 0)
        s = sum(r["realized_pnl"] for r in rs)
        lines.append(f"| `{name}` | {n} | {wins / n:.1%} | {fmt_dollar(s)} | {fmt_dollar(s / n)} |")
    lines.append("")
    return lines


def section_calibration(rows):
    """Conf vs realized hit-rate. Used to spot mis-calibration that would
    motivate an isotonic-regression calibration layer."""
    buckets = [
        ("<0.40", 0, 0.40), ("0.40-0.50", 0.40, 0.50),
        ("0.50-0.60", 0.50, 0.60), ("0.60-0.70", 0.60, 0.70),
        ("0.70-0.75", 0.70, 0.75), ("0.75-0.85", 0.75, 0.85),
        (">=0.85", 0.85, 1.01),
    ]
    g = bucket(rows, lambda r: r["confidence_raw"], buckets)
    out = ["## Conf calibration",
           "",
           "If a row says `hit% < confidence midpoint`, the LLM is",
           "over-confident in that bucket; an isotonic-regression",
           "layer could re-shape the output.",
           ""]
    out += render_bucket_table("Conf vs realized hit-rate", g,
                                [b[0] for b in buckets])
    return out


def section_direction_freshness(rows):
    out = ["## Direction + freshness", ""]
    by_dir = defaultdict(list)
    for r in rows:
        by_dir[r["direction"]].append(r)
    out.append("### By direction")
    out.append("")
    out.append("| direction | n | hit% | sum_pnl |")
    out.append("|---|---:|---:|---:|")
    for d, rs in sorted(by_dir.items()):
        if not rs:
            continue
        wins = sum(1 for r in rs if r["realized_pnl"] > 0)
        s = sum(r["realized_pnl"] for r in rs)
        out.append(f"| {d} | {len(rs)} | {wins / len(rs):.1%} | {fmt_dollar(s)} |")
    out.append("")
    by_fl = defaultdict(list)
    for r in rows:
        by_fl[r["freshness_label"] or "<none>"].append(r)
    out.append("### By freshness label")
    out.append("")
    out.append("| freshness | n | hit% | sum_pnl | avg_pnl |")
    out.append("|---|---:|---:|---:|---:|")
    for fl, rs in sorted(by_fl.items()):
        if not rs:
            continue
        wins = sum(1 for r in rs if r["realized_pnl"] > 0)
        s = sum(r["realized_pnl"] for r in rs)
        out.append(f"| {fl} | {len(rs)} | {wins / len(rs):.1%} | {fmt_dollar(s)} | {fmt_dollar(s / len(rs))} |")
    out.append("")
    return out


def section_close_reasons(rows):
    g = defaultdict(list)
    for r in rows:
        g[r["close_reason"]].append(r)
    out = ["## Close-reason analysis", "",
           "TP/SL/TRAIL asymmetry tells you whether the win/loss",
           "magnitudes match the strategy assumptions (e.g. TP 2% vs SL 3%).",
           "",
           "| close_reason | n | sum_pnl | avg_pnl | avg_hold_min |",
           "|---|---:|---:|---:|---:|"]
    for cr, rs in sorted(g.items(), key=lambda kv: -len(kv[1])):
        n = len(rs)
        s = sum(r["realized_pnl"] for r in rs)
        holds = []
        for r in rs:
            try:
                a = r["decided_at"]
                b = r["closed_at"]
                if not a or not b:
                    continue
                # crude: parse via fromisoformat (drop tz)
                from datetime import datetime
                da = datetime.fromisoformat(a.replace("Z", "+00:00"))
                db = datetime.fromisoformat(b.replace("Z", "+00:00"))
                holds.append((db - da).total_seconds())
            except Exception:
                pass
        avg_hold = (sum(holds) / len(holds) / 60.0) if holds else 0.0
        out.append(f"| `{cr}` | {n} | {fmt_dollar(s)} | {fmt_dollar(s / n)} | {avg_hold:.1f} |")
    out.append("")
    return out


def section_top_markets(rows, n_top=15):
    """Same condition_id appearing repeatedly is a strong signal."""
    g = defaultdict(list)
    for r in rows:
        g[r["condition_id"]].append(r)

    items = []
    for cid, rs in g.items():
        if len(rs) < 2:
            continue
        s = sum(r["realized_pnl"] for r in rs)
        items.append((s, cid, len(rs), rs[0]["question"][:80] if rs[0]["question"] else ""))
    items.sort(key=lambda x: x[0])

    out = ["## Top losing markets (≥2 trades)", "",
           "Markets that we kept re-entering and consistently lost on.",
           "If the same question_id dominates losses, the right fix is at",
           "the market-index level (block it) or the LLM prompt level (it's",
           "miscalling this market type).",
           "",
           "| pnl | n_trades | question |",
           "|---:|---:|---|"]
    for s, cid, n, q in items[:n_top]:
        out.append(f"| {fmt_dollar(s)} | {n} | {q} |")
    out.append("")
    out += ["### Top winning markets (≥2 trades)", "",
            "| pnl | n_trades | question |",
            "|---:|---:|---|"]
    items.sort(key=lambda x: -x[0])
    for s, cid, n, q in items[:n_top]:
        out.append(f"| {fmt_dollar(s)} | {n} | {q} |")
    out.append("")
    return out


def section_source_roi(rows):
    g = defaultdict(list)
    for r in rows:
        url = r["site_url"] or ""
        try:
            host = urlparse(url).netloc or "<unknown>"
        except Exception:
            host = "<unknown>"
        g[host].append(r)
    items = []
    for host, rs in g.items():
        if len(rs) < 3:
            continue
        s = sum(r["realized_pnl"] for r in rs)
        sz = sum(r["size_usdc"] or 0 for r in rs)
        roi = s / sz if sz else 0.0
        wins = sum(1 for r in rs if r["realized_pnl"] > 0)
        items.append((roi, host, len(rs), s, wins / len(rs)))
    items.sort(key=lambda x: x[0])

    out = ["## Per-source ROI (≥3 trades)", "",
           "Source-host aggregation. The watcher feeds 15-ish sites; if any of",
           "them has consistently negative ROI we can deprioritize it at the",
           "watcher level (low-pri queue) or kill it.",
           "",
           "Sorted worst first.",
           "",
           "| host | n | ROI | sum_pnl | hit% |",
           "|---|---:|---:|---:|---:|"]
    for roi, host, n, s, hr in items:
        out.append(f"| `{host}` | {n} | {roi:.2%} | {fmt_dollar(s)} | {hr:.1%} |")
    out.append("")
    return out


def section_hour_of_day(rows):
    g = defaultdict(list)
    for r in rows:
        try:
            from datetime import datetime
            t = datetime.fromisoformat(r["decided_at"].replace("Z", "+00:00"))
            g[t.hour].append(r)
        except Exception:
            pass
    out = ["## Hour-of-day heatmap (UTC)", "",
           "Phase E.11 currently pauses trading at hours 23/00/01 UTC.",
           "Verify whether the actual data still supports that window.",
           "",
           "| hour (UTC) | n | hit% | sum_pnl | avg_pnl |",
           "|---:|---:|---:|---:|---:|"]
    for h in range(24):
        rs = g.get(h, [])
        if not rs:
            out.append(f"| {h:02d} | 0 | — | — | — |")
            continue
        wins = sum(1 for r in rs if r["realized_pnl"] > 0)
        s = sum(r["realized_pnl"] for r in rs)
        out.append(f"| {h:02d} | {len(rs)} | {wins / len(rs):.1%} | {fmt_dollar(s)} | {fmt_dollar(s / len(rs))} |")
    out.append("")
    return out


def section_variant_roi(rows):
    """Per-variant lifetime ROI assuming the $100-per-engine pot."""
    g = defaultdict(list)
    for r in rows:
        g[r["variant_name"] or r["engine_name"]].append(r)
    items = []
    for v, rs in g.items():
        s = sum(r["realized_pnl"] for r in rs)
        sz = sum(r["size_usdc"] or 0 for r in rs)
        wins = sum(1 for r in rs if r["realized_pnl"] > 0)
        roi_on_pot = s / 100.0   # per-engine pot
        roi_on_stake = s / sz if sz else 0.0
        items.append((roi_on_pot, v, len(rs), s, wins / len(rs) if rs else 0, roi_on_stake))
    items.sort(key=lambda x: -x[0])
    out = ["## Per-variant lifetime ROI", "",
           "ROI-on-pot uses the $100/engine bankroll (cycle 9 + cycle 2",
           "combined). ROI-on-stake is sum_pnl / sum_size_usdc.",
           "",
           "| variant | n | hit% | sum_pnl | ROI on $100 pot | ROI on staked |",
           "|---|---:|---:|---:|---:|---:|"]
    for roi_p, v, n, s, hr, roi_s in items:
        out.append(f"| `{v}` | {n} | {hr:.1%} | {fmt_dollar(s)} | {roi_p:.2%} | {roi_s:.2%} |")
    out.append("")
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="seulgi/data/seulgi.db")
    p.add_argument("--out", default="seulgi/REPORT_HISTORICAL_POSTMORTEM_2026-05-19.md")
    args = p.parse_args()

    c = open_db(args.db)
    rows = closed_rows(c)
    c.close()

    parts: list[list[str]] = [
        [f"# SEULGI historical postmortem — {args.out.split('_')[-1].replace('.md', '')}",
         "",
         "Forensic pass over every strategy-relevant closed trade in the DB.",
         "Use this to inform the next round of strategy/variant changes.",
         ""],
        section_overview(rows),
        section_calibration(rows),
        section_direction_freshness(rows),
        section_close_reasons(rows),
        section_variant_roi(rows),
        section_top_markets(rows),
        section_source_roi(rows),
        section_hour_of_day(rows),
    ]

    md = "\n".join("\n".join(p) for p in parts)
    Path(args.out).write_text(md)
    print(f"Wrote {args.out} ({len(rows)} closed rows analyzed)")


if __name__ == "__main__":
    main()

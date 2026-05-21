"""
Pure-function core of the SCOUT decision-funnel report.

Shared by the live dashboard tile (`dashboard.py::_write_scout_funnel`) and
the post-hoc CLI (`seulgi/scripts/analysis/scout_funnel.py`) so both views
stay in lock-step. No I/O — callers pass in iterable trade rows and get a
stats dict back, then optionally render Markdown.

Row shape expected by `compute_funnel`: dict-like with the columns selected
by `SCOUT_FUNNEL_COLUMNS` below.
"""
from __future__ import annotations

import json
from collections import Counter
from typing import Iterable


SCOUT_FUNNEL_COLUMNS = (
    "decided_at, engine_name, direction, confidence_raw, signed_edge, "
    "mid_yes, size_usdc, skip_reason, trader_trace, realized_pnl, closed_at"
)


def _parse_trace(raw):
    if not raw:
        return []
    try:
        return json.loads(raw)
    except Exception:  # noqa: BLE001
        return []


def compute_funnel(rows: Iterable) -> dict:
    """Tabulate scout's decision funnel from raw trade rows.

    `rows`: any iterable of dict-like objects matching SCOUT_FUNNEL_COLUMNS.
    Returns {"total": 0} if no rows.
    """
    rows = list(rows)
    total = len(rows)
    if total == 0:
        return {"total": 0}

    skip_top: Counter = Counter(r["skip_reason"] or "EXECUTED" for r in rows)
    step_fail: Counter = Counter()
    step_pass: Counter = Counter()
    ai_directions: Counter = Counter()
    pa_buckets: Counter = Counter()
    edge_buckets: Counter = Counter()
    mid_buckets: Counter = Counter()
    executed = 0
    closed_wins = 0
    closed_loss = 0
    realized_pnl = 0.0
    cf_relax_edge = 0
    cf_relax_conf = 0
    cf_drop_pre_ai = 0

    for r in rows:
        trace = _parse_trace(r["trader_trace"])
        first_fail_step = None
        first_fail_data = None
        for step in trace:
            name = step.get("step", "?")
            if step.get("pass") is False:
                step_fail[name] += 1
                first_fail_step = name
                first_fail_data = step
                break
            if step.get("pass") is True:
                step_pass[name] += 1
            if name == "ai":
                ai_directions[step.get("direction", "?")] += 1
                pa = step.get("p_alpha")
                if pa is not None:
                    pa_buckets[round(pa, 1)] += 1
        if first_fail_step is None and (r["skip_reason"] is None or r["skip_reason"] == ""):
            executed += 1
            if r["closed_at"]:
                pnl = r["realized_pnl"] or 0.0
                realized_pnl += pnl
                if pnl > 0:
                    closed_wins += 1
                else:
                    closed_loss += 1
        if first_fail_step == "edge":
            edge_val = (first_fail_data or {}).get("edge", 0.0)
            if edge_val > -1.0:
                cf_relax_edge += 1
        elif first_fail_step == "min_confidence":
            cf_relax_conf += 1
        elif first_fail_step == "pre_ai_rule":
            cf_drop_pre_ai += 1
        se = r["signed_edge"]
        if se is not None:
            edge_buckets[round(se, 2)] += 1
        my = r["mid_yes"]
        if my is not None:
            mid_buckets[round(my, 1)] += 1

    return {
        "total": total,
        "executed": executed,
        "closed_wins": closed_wins,
        "closed_loss": closed_loss,
        "realized_pnl": realized_pnl,
        "skip_top": skip_top,
        "step_fail": step_fail,
        "step_pass": step_pass,
        "ai_directions": ai_directions,
        "pa_buckets": pa_buckets,
        "edge_buckets": edge_buckets,
        "mid_buckets": mid_buckets,
        "cf_relax_edge": cf_relax_edge,
        "cf_relax_conf": cf_relax_conf,
        "cf_drop_pre_ai": cf_drop_pre_ai,
    }


def render_funnel_markdown(
    stats: dict,
    *,
    engine_prefix: str = "scout",
    since_hours: float = 24.0,
    cutoff: str = "",
) -> str:
    """Render a Markdown funnel report from `compute_funnel`'s output."""
    if not stats or stats.get("total", 0) == 0:
        return (
            f"# scout_funnel: no rows for engine_prefix={engine_prefix!r} "
            f"since {cutoff}\n"
        )

    total = stats["total"]
    lines: list[str] = []
    lines.append(f"# SCOUT funnel report — {engine_prefix} engine")
    if cutoff:
        lines.append(f"_cutoff: since {since_hours:.1f}h ago ({cutoff})_")
    lines.append("")
    lines.append(f"**Total decisions**: {total:,}")
    lines.append(
        f"**Executed**: {stats['executed']:,} "
        f"({stats['executed'] / max(total, 1) * 100:.2f}%)"
    )
    n_closed = stats["closed_wins"] + stats["closed_loss"]
    if n_closed > 0:
        hit = stats["closed_wins"] / n_closed * 100
        lines.append(
            f"**Closed**: {n_closed} (wins={stats['closed_wins']}, "
            f"losses={stats['closed_loss']}, hit={hit:.1f}%, "
            f"realized_pnl=${stats['realized_pnl']:+.2f})"
        )
    lines.append("")
    lines.append("## Where decisions die (first failing step)")
    lines.append("")
    lines.append("| step | count | pct |")
    lines.append("|---|---:|---:|")
    for step, n in stats["step_fail"].most_common():
        lines.append(f"| {step} | {n:,} | {n / total * 100:.1f}% |")
    lines.append("")
    lines.append("## Top skip_reason values")
    lines.append("")
    lines.append("| skip_reason | count |")
    lines.append("|---|---:|")
    for reason, n in stats["skip_top"].most_common(15):
        lines.append(f"| `{reason}` | {n:,} |")
    lines.append("")
    lines.append("## AI Stage direction mix (when reached)")
    lines.append("")
    for d, n in stats["ai_directions"].most_common():
        lines.append(f"- {d}: {n:,}")
    lines.append("")
    lines.append("## p_alpha distribution (when AI reached)")
    lines.append("")
    lines.append("| p_alpha bucket | count |")
    lines.append("|---:|---:|")
    for pa in sorted(stats["pa_buckets"].keys(), reverse=True):
        lines.append(f"| {pa} | {stats['pa_buckets'][pa]:,} |")
    lines.append("")
    lines.append("## signed_edge distribution")
    lines.append("")
    lines.append("| edge bucket | count |")
    lines.append("|---:|---:|")
    for eb in sorted(stats["edge_buckets"].keys()):
        lines.append(f"| {eb:+.2f} | {stats['edge_buckets'][eb]:,} |")
    lines.append("")
    lines.append("## mid_yes distribution")
    lines.append("")
    lines.append("| mid_yes bucket | count |")
    lines.append("|---:|---:|")
    for mb in sorted(stats["mid_buckets"].keys()):
        lines.append(f"| {mb:.1f} | {stats['mid_buckets'][mb]:,} |")
    lines.append("")
    lines.append("## Counterfactuals (rows that died on each gate)")
    lines.append("")
    lines.append(
        f"- If `min_edge` were lowered to -1.0: would admit "
        f"~{stats['cf_relax_edge']:,} more rows."
    )
    lines.append(
        f"- If `min_confidence` gate were removed: would admit "
        f"~{stats['cf_relax_conf']:,} more rows."
    )
    lines.append(
        f"- If `pre_ai_rule` filters were dropped: would admit "
        f"~{stats['cf_drop_pre_ai']:,} more rows."
    )
    lines.append("")
    lines.append(
        "> NB: these are upper-bound counts — each row could still fail on "
        "a downstream gate after the earlier fix."
    )
    return "\n".join(lines) + "\n"

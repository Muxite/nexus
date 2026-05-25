"""Per-stage decision-latency breakdown — `latency_breakdown.txt`.

Cycle 15: every execute-path `trades` row carries `decision_latency_ms`
(total wall-clock from `handle()` entry to the trade-row INSERT) plus
`decision_latency_breakdown` (JSON `{step_name: ms}`). Cycle 14 wrote
those columns; this page is the operator-facing surface.

The headline question the page answers: *which step in the decision
chain dominates the latency budget?* `philosophy.md` allocates ~200ms
end-to-end; the per-step p50/p95/p99 here is what an operator uses to
replan that budget once chains widen.

Stages instrumented in cycle 14 (canonical order):
    fetch_article, fetch_market, ai_direction,
    resolve_mid, resolve_book, imbalance_check, db_insert

Cycle 16: extend the panel to ALSO read the same two columns on
`decisions` (reject rows). Execute-path rows are still the primary
source — the cycle-15 table renders first as before. Underneath, a
new "Reject-path latencies" section buckets the reject rows by
`reason` and shows the per-step p50 for the steps that ran before
each gate fired. ~95%+ of signals fail the cascade, so this is where
most of the latency-budget signal actually lives once we look beyond
the execute path.

The page reads ONLY (no writes back to the DB) and uses the same
atomic `.tmp → rename` pattern as the rest of the dashboard via
`core.tick`'s `_atomic_write`.
"""

from __future__ import annotations

import json

from ..io import _now, _percentiles

FILENAME = "latency_breakdown.txt"

# Canonical render order. Any step that appears in JSON but isn't here
# gets appended in alphabetic order so a future instrumentation point
# still shows up without a code change.
_CANONICAL_STAGES: tuple[str, ...] = (
    "fetch_article",
    "fetch_market",
    "ai_direction",
    "resolve_mid",
    "resolve_book",
    "imbalance_check",
    "db_insert",
)

# Last N execute-path trades to sample. Matches the task spec: enough
# for stable p99 without scanning the full history each tick.
_SAMPLE_LIMIT = 500


def _empty_panel(cycle_id) -> str:
    lines = [
        f"# irene latency breakdown ({_now().isoformat()})",
        f"  per-stage decision-latency p50/p95/p99 (ms), "
        f"last {_SAMPLE_LIMIT} executed trades (cycle_id={cycle_id})",
        "",
        "  no instrumented trades yet — wait for the first executed "
        "trade post-cycle-14.",
    ]
    return "\n".join(lines) + "\n"


def _max(samples: list[float]) -> float:
    return max(samples) if samples else 0.0


async def render(dash) -> str:
    cycle_id = dash.current_cycle_id
    cycle_sql = f"AND cycle_id = {int(cycle_id)}" if cycle_id is not None else ""
    # Pull the last N execute-path rows with both fields populated.
    # decision_latency_ms is NULL on legacy rows + reject rows; the
    # NOT NULL filter naturally drops both. We also require the JSON
    # blob to be non-NULL so the per-stage scan has something to do.
    cur = await dash.db.conn.execute(
        f"""
        SELECT decision_latency_ms, decision_latency_breakdown
        FROM trades
        WHERE decision_latency_ms IS NOT NULL
          AND decision_latency_breakdown IS NOT NULL
          {cycle_sql}
        ORDER BY id DESC
        LIMIT {_SAMPLE_LIMIT}
        """
    )
    rows = await cur.fetchall()
    await cur.close()

    if not rows:
        # No executes yet — show the empty-execute message, then
        # render the cycle-16 reject section underneath if any reject
        # rows carry latency data. (Production hits this state during
        # the first cycle after a deploy: rejects accumulate fast,
        # executes trickle in.)
        empty = _empty_panel(cycle_id)
        reject_section = await _render_reject_section(dash, cycle_sql)
        if reject_section:
            return empty + "\n" + "\n".join(reject_section) + "\n"
        return empty

    # Accumulate per-step samples. Rows with malformed JSON are skipped
    # (logged via the dashboard's per-page exception handler in core.tick
    # only if the whole render raises — we keep going on bad rows so a
    # single corrupt blob doesn't blank the page).
    totals: list[float] = []
    by_stage: dict[str, list[float]] = {}
    for r in rows:
        total = r["decision_latency_ms"]
        if total is not None and total >= 0:
            totals.append(float(total))
        raw = r["decision_latency_breakdown"]
        if not raw:
            continue
        try:
            blob = json.loads(raw)
        except (TypeError, ValueError):
            continue
        if not isinstance(blob, dict):
            continue
        for step, ms in blob.items():
            if ms is None:
                continue
            try:
                ms_f = float(ms)
            except (TypeError, ValueError):
                continue
            if ms_f < 0:
                continue
            by_stage.setdefault(str(step), []).append(ms_f)

    # If the breakdown column existed but every blob was malformed,
    # treat the panel as empty rather than rendering a header with
    # nothing under it.
    if not by_stage and not totals:
        return _empty_panel(cycle_id)

    # Stage order: canonical first (those present in the data), then
    # any extras alphabetically.
    seen = set(by_stage.keys())
    ordered_stages = [s for s in _CANONICAL_STAGES if s in seen]
    extras = sorted(seen - set(_CANONICAL_STAGES))
    ordered_stages.extend(extras)

    lines = [
        f"# irene latency breakdown ({_now().isoformat()})",
        f"  per-stage decision-latency p50/p95/p99 (ms), "
        f"last {len(rows)} executed trades (cycle_id={cycle_id})",
        "",
        f"  {'stage':<16} {'n':>5} {'p50':>7} {'p95':>7} {'p99':>7} {'max':>7}",
        "  " + "-" * 54,
    ]
    for stage in ordered_stages:
        samples = by_stage[stage]
        p = _percentiles(samples)
        lines.append(
            f"  {stage:<16} {p['count']:>5d} "
            f"{p['p50']:>7.1f} {p['p95']:>7.1f} {p['p99']:>7.1f} "
            f"{_max(samples):>7.1f}"
        )
    # Total row: same window, computed straight from
    # decision_latency_ms (NOT a sum of per-step samples — caller may
    # have skipped instrumenting wait time, retries, or other gaps).
    if totals:
        p = _percentiles(totals)
        lines.append("  " + "-" * 54)
        lines.append(
            f"  {'total':<16} {p['count']:>5d} "
            f"{p['p50']:>7.1f} {p['p95']:>7.1f} {p['p99']:>7.1f} "
            f"{_max(totals):>7.1f}"
        )

    # ── Reject-path latencies (cycle 16) ─────────────────────────────
    # ~95%+ of signals fail the cascade. Bucket those reject rows by
    # `reason` (the column on decisions that names which gate fired)
    # so an operator can see, per gate, how much time it consumed
    # before tripping. The per-step samples are partial — a
    # `similarity_floor` reject's breakdown is empty (gate 1, no
    # fetches yet); an `edge_below_min` reject's breakdown carries
    # everything up to (but not including) `db_insert`.
    reject_section = await _render_reject_section(dash, cycle_sql)
    if reject_section:
        lines.append("")
        lines.extend(reject_section)

    return "\n".join(lines) + "\n"


async def _render_reject_section(dash, cycle_sql: str) -> list[str]:
    """Render the per-reason reject-path sub-tables. Returns the lines
    to append (empty list when no reject rows carry latency data).

    Bucketing strategy: GROUP BY `reason` in Python (cheaper than two
    round-trips + simpler to keep ordering deterministic). For each
    bucket we render a one-line header (`reason (n=N)`) plus a
    fixed-width per-step table showing only the steps that actually
    ran inside that bucket. Steps follow the same canonical order as
    the execute-path table.
    """
    cur = await dash.db.conn.execute(
        f"""
        SELECT reason, decision_latency_ms, decision_latency_breakdown
        FROM decisions
        WHERE outcome='reject'
          AND decision_latency_ms IS NOT NULL
          {cycle_sql}
        ORDER BY id DESC
        LIMIT {_SAMPLE_LIMIT}
        """
    )
    rows = await cur.fetchall()
    await cur.close()
    if not rows:
        return []

    # Per-reason aggregation: { reason: { "total": [ms,...],
    #                                     "by_stage": { step: [ms,...] } } }
    buckets: dict[str, dict] = {}
    for r in rows:
        reason = str(r["reason"])
        b = buckets.setdefault(reason, {"total": [], "by_stage": {}})
        total = r["decision_latency_ms"]
        if total is not None and total >= 0:
            b["total"].append(float(total))
        raw = r["decision_latency_breakdown"]
        if not raw:
            continue
        try:
            blob = json.loads(raw)
        except (TypeError, ValueError):
            continue
        if not isinstance(blob, dict):
            continue
        for step, ms in blob.items():
            if ms is None:
                continue
            try:
                ms_f = float(ms)
            except (TypeError, ValueError):
                continue
            if ms_f < 0:
                continue
            b["by_stage"].setdefault(str(step), []).append(ms_f)

    if not buckets:
        return []

    n_total = sum(len(b["total"]) for b in buckets.values())
    out: list[str] = [
        f"  Reject-path latencies (last {n_total} rejects, by reason)",
        "  " + "=" * 54,
    ]
    # Stable ordering: most-frequent reason first. Ties broken alphabetically.
    ordered_reasons = sorted(
        buckets.keys(),
        key=lambda k: (-len(buckets[k]["total"]), k),
    )
    for reason in ordered_reasons:
        b = buckets[reason]
        n = len(b["total"])
        total_p = _percentiles(b["total"])
        out.append("")
        out.append(
            f"  {reason} (n={n}): total p50={total_p['p50']:.1f}ms "
            f"p95={total_p['p95']:.1f}ms"
        )
        # Per-step lines — only steps that actually ran before this gate.
        seen = set(b["by_stage"].keys())
        if not seen:
            # Gate-1 rejects (similarity_floor / freshness_stale) ran
            # NO instrumented step. Make that visible rather than
            # silently dropping the section.
            out.append("    (no per-step samples — reject fired before any instrumented step)")
            continue
        ordered_stages = [s for s in _CANONICAL_STAGES if s in seen]
        extras = sorted(seen - set(_CANONICAL_STAGES))
        ordered_stages.extend(extras)
        for stage in ordered_stages:
            samples = b["by_stage"][stage]
            p = _percentiles(samples)
            out.append(
                f"    {stage:<16} n={p['count']:<4d} "
                f"p50={p['p50']:>6.1f}ms p95={p['p95']:>6.1f}ms "
                f"p99={p['p99']:>6.1f}ms"
            )
    return out

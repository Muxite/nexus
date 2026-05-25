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
        return _empty_panel(cycle_id)

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
    return "\n".join(lines) + "\n"

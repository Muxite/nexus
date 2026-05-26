"""Near-miss audit — `near_miss_audit.txt`.

Two sections, both surfacing decisions where the LLM was decisive (or
close to it) but the trade didn't fire. The point is to give the
operator a focused review surface for the "leaning yes/no but cut from
final trading" set, so they can manually grade the gate cascade and
decide whether to (a) lower `cfg.trade.min_confidence`, (b) tweak the
direction prompt, or (c) relax a downstream gate.

Section A — `direction_lowconf` rejects with confidence in the
near-miss band [`audit_confidence_floor`, `min_confidence`). These are
the signals the trade-execute gate killed at the direction step
itself; they're the most direct evidence for "should we drop
min_confidence?".

Section B — decisive YES/NO predictions (confidence >= min_confidence)
killed by a DOWNSTREAM gate (mid_degenerate, wide_spread, edge_below,
insufficient_cash, etc.). These are the "LLM nailed the call but
microstructure refused" rows from handoff A's funnel.

Cycle 24 (handoff F). Pattern stolen from `pages/decisions.py`.
"""

from __future__ import annotations

from ..io import _now

FILENAME = "near_miss_audit.txt"

# Per-section row cap. Keeps the file scannable in a terminal.
LIMIT = 20

# Section B excludes these reject reasons. The exclusion set covers
# both upstream gates (similarity_floor, freshness_stale, etc.) where
# the LLM never even produced a YES/NO -- those rows already have
# direction=NULL and confidence=NULL on the joined prediction, but we
# filter explicitly anyway for clarity -- and direction-stage rejects
# (handled by section A) so we don't double-count.
_SECTION_B_EXCLUDED_REASONS = (
    "similarity_floor",
    "freshness_stale",
    "entity_overlap",
    "entity_mismatch",
    "direction_neutral",
    "direction_lowconf",
    "article_missing",
    "market_missing",
    "market_closed",
    "market_inactive",
    "market_near_expiry",
    "execution_disabled",
)


def _truncate(s: str | None, n: int) -> str:
    if not s:
        return ""
    s = s.replace("\n", " ").strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def _excluded_reasons_sql() -> str:
    quoted = ",".join(f"'{r}'" for r in _SECTION_B_EXCLUDED_REASONS)
    return f"({quoted})"


def _fmt_row(r, *, include_reason: bool) -> list[str]:
    """Render one near-miss row as a multi-line block."""
    ts = (r["created_at"] or "")[11:19]
    path = r["path"] or "mvp"
    direction = r["direction"] or "?"
    confidence = r["confidence"]
    conf_str = f"{confidence:.2f}" if confidence is not None else "?"
    label = r["label"] or "-"
    reason = (r["reason"] or "?") if include_reason else None
    header_bits = [
        f"[{ts}]",
        f"{path:<3}",
        f"{direction:<3}",
        f"conf={conf_str}",
        f"label={label}",
    ]
    if reason is not None:
        header_bits.append(f"reason={reason}")
    lines = ["  " + "  ".join(header_bits)]
    title = _truncate(r["article_title"], 100)
    question = _truncate(r["market_question"], 100)
    lines.append(f'    art: "{title}"' if title else "    art: (missing)")
    lines.append(f'    mkt: "{question}"' if question else "    mkt: (missing)")
    return lines


async def _fetch_section_a(dash, *, audit_floor: float, cycle_sql: str):
    cur = await dash.db.conn.execute(
        f"""
        SELECT
            d.created_at,
            d.reason,
            COALESCE(t.decision_path, 'mvp') AS path,
            p.direction,
            p.confidence,
            p.label,
            a.title    AS article_title,
            m.question AS market_question,
            m.condition_id
        FROM decisions d
        JOIN predictions p ON p.id = d.prediction_id
        LEFT JOIN signals  s ON s.id = d.signal_id
        LEFT JOIN articles a ON a.id = s.article_id
        LEFT JOIN markets  m ON m.condition_id = s.market_id
        LEFT JOIN trades   t ON t.decision_id = d.id
        WHERE d.outcome   = 'reject'
          AND d.reason    = 'direction_lowconf'
          AND d.is_tracer = 0
          AND p.direction IN ('YES','NO')
          AND p.confidence >= ?
          AND datetime(d.created_at) >= datetime('now','-6 hours')
          {cycle_sql}
        ORDER BY p.confidence DESC
        LIMIT ?
        """,
        (audit_floor, LIMIT),
    )
    rows = await cur.fetchall()
    await cur.close()
    return rows


async def _fetch_section_b(dash, *, trade_floor: float, cycle_sql: str):
    cur = await dash.db.conn.execute(
        f"""
        SELECT
            d.created_at,
            d.reason,
            COALESCE(t.decision_path, 'mvp') AS path,
            p.direction,
            p.confidence,
            p.label,
            a.title    AS article_title,
            m.question AS market_question
        FROM decisions d
        JOIN predictions p ON p.id = d.prediction_id
        LEFT JOIN signals  s ON s.id = d.signal_id
        LEFT JOIN articles a ON a.id = s.article_id
        LEFT JOIN markets  m ON m.condition_id = s.market_id
        LEFT JOIN trades   t ON t.decision_id = d.id
        WHERE d.outcome    = 'reject'
          AND d.reason NOT IN {_excluded_reasons_sql()}
          AND d.is_tracer  = 0
          AND p.direction IN ('YES','NO')
          AND p.confidence >= ?
          AND datetime(d.created_at) >= datetime('now','-6 hours')
          {cycle_sql}
        ORDER BY p.confidence DESC
        LIMIT ?
        """,
        (trade_floor, LIMIT),
    )
    rows = await cur.fetchall()
    await cur.close()
    return rows


def _audit_floor(dash) -> float:
    """Read `cfg.trade.audit_confidence_floor` with a sane fallback for
    tests that build a `Dashboard` without a cfg handle."""
    if dash.cfg is not None:
        try:
            return float(dash.cfg.trade.audit_confidence_floor)
        except AttributeError:
            pass
    return 0.35


def _trade_floor(dash) -> float:
    if dash.cfg is not None:
        try:
            return float(dash.cfg.trade.min_confidence)
        except AttributeError:
            pass
    return 0.55


_EMPTY_STATE = (
    "  (no near-miss rows in window — pipeline may be starved upstream;"
    " see handoff B)"
)


async def render(dash) -> str:
    cycle_id = dash.current_cycle_id
    cycle_sql = (
        f"AND d.cycle_id = {int(cycle_id)}" if cycle_id is not None else ""
    )
    audit_floor = _audit_floor(dash)
    trade_floor = _trade_floor(dash)

    lines = [
        f"# irene near-miss audit ({_now().isoformat()})",
        (
            f"  audit_floor={audit_floor:.2f}  trade_floor={trade_floor:.2f}"
            f"  window=last 6h  cycle_id={cycle_id}"
        ),
        "",
        "=== Section A: leaning YES/NO rejected at the direction gate ===",
        (
            f"  predictions with direction in (YES,NO) and"
            f" confidence in [{audit_floor:.2f}, {trade_floor:.2f})"
        ),
        "",
    ]

    rows_a = await _fetch_section_a(
        dash, audit_floor=audit_floor, cycle_sql=cycle_sql,
    )
    if not rows_a:
        lines.append(_EMPTY_STATE)
    else:
        for r in rows_a:
            lines.extend(_fmt_row(r, include_reason=False))
            lines.append("")

    lines.append("")
    lines.append("=== Section B: decisive YES/NO killed by a downstream gate ===")
    lines.append(
        f"  predictions with confidence >= {trade_floor:.2f} rejected after"
        " the direction step (mid/spread/edge/size/cash/imbalance/etc.)"
    )
    lines.append("")

    rows_b = await _fetch_section_b(
        dash, trade_floor=trade_floor, cycle_sql=cycle_sql,
    )
    if not rows_b:
        lines.append(_EMPTY_STATE)
    else:
        for r in rows_b:
            lines.extend(_fmt_row(r, include_reason=True))
            lines.append("")

    return "\n".join(lines) + "\n"

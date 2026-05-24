"""Per-decision audit trail — `decisions.txt`.

Multi-line per decision so the operator can read the article + market
pair, the full gate-cascade thought process, and the LLM's raw
response. This is the file you tail when tuning the LLM prompt or
diagnosing why nothing is trading.

Per `app/config.py:124-129` the direction model is configured for
`max_tokens=1, single-char Y/N/U`, so `raw_response` is literally one
char (when it parses) — that's why this page shows the prompt's *inputs*
plus the deterministic gate cascade rather than any AI reasoning text.
"""

from __future__ import annotations

import json

from ..io import _now

FILENAME = "decisions.txt"

# By design this file grows unbounded — the operator wants "loads of
# data to work with" for prompt tuning. The 30s rewrite cycle is fine
# at thousands of decisions; revisit if we cross tens of thousands and
# the atomic-rename starts hurting IO. Newest-first so `less decisions.txt`
# opens to the freshest decisions.
LIMIT = 10000


def _truncate(s: str | None, n: int) -> str:
    if not s:
        return ""
    s = s.replace("\n", " ").strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def _fmt_value(v) -> str:
    """Compact, human-readable representation of a gate's value."""
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:.3f}"
    if isinstance(v, list):
        return "{" + ", ".join(str(x) for x in v) + "}"
    return str(v)


def _render_gate_history(gate_history_json: str | None) -> list[str]:
    """Return the gate cascade as a numbered thought-process block."""
    if not gate_history_json:
        return ["  process: (no gate history)"]
    try:
        gates = json.loads(gate_history_json)
    except (TypeError, ValueError):
        return ["  process: (gate_history unparseable)"]
    if not gates:
        return ["  process: (no gates evaluated)"]

    out = ["  process:"]
    rejected_at = None
    for i, g in enumerate(gates, start=1):
        name = g.get("gate", "?")
        outcome = g.get("outcome", "?")
        value = _fmt_value(g.get("value"))
        threshold = _fmt_value(g.get("threshold"))
        marker = "✓" if outcome == "ok" else "✗"
        op = "≥" if (outcome == "ok" and "_floor" in name) else "vs"
        # Special-case nicer phrasing for the freshness and direction gates.
        if name == "freshness":
            line = f"    {i}. {name:<16} {marker} {outcome:<7}  {value} ∈ {threshold}"
        elif name == "direction":
            line = f"    {i}. {name:<16} {marker} {outcome:<7}  {value} {op} {threshold}"
        else:
            line = f"    {i}. {name:<16} {marker} {outcome:<7}  {value} {op} {threshold}"
        out.append(line)
        if outcome == "reject":
            rejected_at = name
            break
    if rejected_at:
        out.append(f"       └─ stopped at `{rejected_at}`")
    else:
        out.append("       └─ EXECUTED")
    return out


async def render(dash) -> str:
    lines = [
        f"# irene decisions ({_now().isoformat()})",
        f"  all non-tracer decisions (cap {LIMIT}), newest first — full audit trail",
        "",
    ]
    cur = await dash.db.conn.execute(
        """
        SELECT
            d.created_at,
            d.outcome,
            d.reason,
            d.gate_history,
            COALESCE(t.decision_path, 'mvp') AS path,
            s.similarity,
            p.direction,
            p.confidence,
            p.cached,
            p.prompt_version,
            p.raw_response,
            p.label,
            t.edge,
            t.size_usdc,
            a.title    AS article_title,
            a.summary  AS article_summary,
            m.question AS market_question
        FROM decisions d
        LEFT JOIN signals     s ON s.id = d.signal_id
        LEFT JOIN predictions p ON p.id = d.prediction_id
        LEFT JOIN trades      t ON t.decision_id = d.id
        LEFT JOIN articles    a ON a.id = s.article_id
        LEFT JOIN markets     m ON m.condition_id = s.market_id
        WHERE d.is_tracer = 0
        ORDER BY d.created_at DESC
        LIMIT ?
        """,
        (LIMIT,),
    )
    rows = await cur.fetchall()
    await cur.close()

    if not rows:
        lines.append("  (no decisions yet)")
        return "\n".join(lines) + "\n"

    for r in rows:
        ts = (r["created_at"] or "")[11:19]
        path = r["path"] or "?"
        outcome = r["outcome"] or "?"

        # Header line — when, path, outcome, edge/size for executes
        if outcome == "execute":
            outcome_label = "execute              "
            extras = ""
            if r["edge"] is not None and r["size_usdc"] is not None:
                extras = f"  edge={r['edge']:+.3f}  size=${r['size_usdc']:.2f}"
        else:
            reason = (r["reason"] or "?")[:20]
            outcome_label = f"reject:{reason:<13}"
            extras = ""
        lines.append(f"[{ts}]  {path:<3}  {outcome_label}{extras}")

        # Article + market — the news that triggered this decision.
        title = _truncate(r["article_title"], 100)
        if title and len(title) < 50 and r["article_summary"]:
            title = f'{title}  · {_truncate(r["article_summary"], 60)}'
        lines.append(f'  art: "{title}"' if title else "  art: (missing)")
        mkt = _truncate(r["market_question"], 100)
        lines.append(f'  mkt: "{mkt}"' if mkt else "  mkt: (missing)")

        # AI line — raw_response, cached, prompt_version (LLM output, if reached)
        raw = (r["raw_response"] or "").replace("\n", " ").strip()
        if len(raw) > 40:
            raw = raw[:39] + "…"
        if raw or r["direction"]:
            cached_str = "yes" if r["cached"] else "no"
            prompt_v = r["prompt_version"] or "?"
            label = r["label"] or "-"
            lines.append(
                f"  ai:  label={label}  raw={raw!r}  cached={cached_str}  prompt_v={prompt_v}"
            )

        # Gate cascade — the deterministic thought process the trade
        # stage walked through. This is the answer to "WHY did this end
        # up rejected / executed?"
        lines.extend(_render_gate_history(r["gate_history"]))
        lines.append("")

    return "\n".join(lines) + "\n"

"""Per-decision audit trail — `decisions.txt`.

Multi-line per decision so the operator can read the article + market
pair and instantly judge whether the AI's call (YES / NO / NEUTRAL)
was warranted. This is the file you tail when tuning the LLM prompt.

Per `app/config.py:124-129` the direction model is configured for
`max_tokens=1, single-char Y/N/U`, so `raw_response` is literally one
char — that's why this page shows the prompt's *inputs* (article + market)
rather than any AI reasoning text.
"""

from __future__ import annotations

from ..io import _now

FILENAME = "decisions.txt"

LIMIT = 15


def _truncate(s: str | None, n: int) -> str:
    if not s:
        return ""
    s = s.replace("\n", " ").strip()
    return s if len(s) <= n else s[: n - 1] + "…"


async def render(dash) -> str:
    lines = [
        f"# irene decisions ({_now().isoformat()})",
        f"  last {LIMIT}, newest first — multi-line so each decision is auditable",
        "",
    ]
    cur = await dash.db.conn.execute(
        """
        SELECT
            d.created_at,
            d.outcome,
            d.reason,
            COALESCE(t.decision_path, 'mvp') AS path,
            s.similarity,
            p.direction,
            p.confidence,
            p.cached,
            p.prompt_version,
            p.raw_response,
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

        # Header line — when, path, outcome, key numbers
        sim = f"sim={r['similarity']:.2f}" if r["similarity"] is not None else "sim=-"
        conf = f"conf={r['confidence']:.2f}" if r["confidence"] is not None else "conf=-"
        direction = f"dir={(r['direction'] or '-')[:3]}"
        # Execute-only extras
        extras = ""
        if outcome == "execute":
            outcome_label = "execute              "
            if r["edge"] is not None and r["size_usdc"] is not None:
                extras = f"  edge={r['edge']:+.3f}  size=${r['size_usdc']:.2f}"
        else:
            reason = (r["reason"] or "?")[:20]
            outcome_label = f"reject:{reason:<13}"

        lines.append(
            f"[{ts}]  {path:<3}  {outcome_label}  {sim:<9}  {conf:<10} {direction}{extras}"
        )
        # Article line (title — append a short summary excerpt if title is terse)
        title = _truncate(r["article_title"], 100)
        if title and len(title) < 50 and r["article_summary"]:
            title = f'{title}  · {_truncate(r["article_summary"], 60)}'
        lines.append(f'  art: "{title}"' if title else "  art: (missing)")
        # Market line
        mkt = _truncate(r["market_question"], 100)
        lines.append(f'  mkt: "{mkt}"' if mkt else "  mkt: (missing)")
        # AI line — raw_response, cached, prompt_version
        raw = (r["raw_response"] or "").replace("\n", " ").strip()
        if len(raw) > 40:
            raw = raw[:39] + "…"
        cached_str = "yes" if r["cached"] else "no"
        prompt_v = r["prompt_version"] or "?"
        lines.append(f"  ai:  raw={raw!r}  cached={cached_str}  prompt_v={prompt_v}")
        lines.append("")

    return "\n".join(lines) + "\n"

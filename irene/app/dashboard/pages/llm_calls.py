"""LLM call diagnostics: count, p50/p95 latency, error rate, token totals."""

from __future__ import annotations

from ..io import _now, _percentiles

FILENAME = "llm_calls.txt"


async def render(dash) -> str:
    lines = [
        f"# irene llm calls ({_now().isoformat()})",
        "  last 1h, all models",
        "",
    ]
    cur = await dash.db.conn.execute(
        """
        SELECT model,
               COUNT(*)                                        AS n,
               SUM(CASE WHEN status_code BETWEEN 200 AND 299
                        THEN 0 ELSE 1 END)                     AS n_err,
               SUM(COALESCE(tokens_in, 0))                     AS toks_in,
               SUM(COALESCE(tokens_out, 0))                    AS toks_out
        FROM llm_calls
        WHERE created_at >= datetime('now','-1 hour')
        GROUP BY model
        ORDER BY n DESC
        """
    )
    rows = await cur.fetchall()
    await cur.close()
    if not rows:
        lines.append("  (no LLM calls in last hour)")
        return "\n".join(lines) + "\n"

    # Latency percentiles, all models pooled
    cur = await dash.db.conn.execute(
        "SELECT latency_ms FROM llm_calls "
        "WHERE created_at >= datetime('now','-1 hour') "
        "  AND latency_ms IS NOT NULL"
    )
    lat_rows = await cur.fetchall()
    await cur.close()
    p = _percentiles([float(r["latency_ms"]) for r in lat_rows])

    lines.append(
        f"  latency (ms, pooled): p50={p['p50']:>6.0f}  "
        f"p95={p['p95']:>6.0f}  p99={p['p99']:>6.0f}  n={p['count']}"
    )
    lines.append("")
    lines.append(
        f"  {'MODEL':<40} {'N':>5} {'ERR%':>6} {'TOK_IN':>9} {'TOK_OUT':>9}"
    )
    lines.append("  " + "─" * 72)
    for r in rows:
        n = int(r["n"])
        err_pct = 100.0 * int(r["n_err"]) / max(n, 1)
        lines.append(
            f"  {(r['model'] or '?')[:40]:<40} "
            f"{n:>5d} {err_pct:>5.1f}% "
            f"{int(r['toks_in']):>9d} {int(r['toks_out']):>9d}"
        )
    return "\n".join(lines) + "\n"

"""Distribution of (traded_at - article.published_at).

THE diagnostic for the speed thesis: median 2 s = winning the race,
60 s = losing it.
"""

from __future__ import annotations

from ..io import _now, _percentiles

FILENAME = "news_staleness.txt"


async def render(dash) -> str:
    cycle_id = dash.current_cycle_id
    cs = f"AND t.cycle_id = {int(cycle_id)}" if cycle_id is not None else ""
    cur = await dash.db.conn.execute(
        f"""
        SELECT (julianday(t.decided_at) - julianday(a.published_at)) * 86400.0 AS sec
        FROM trades t
        JOIN signals s ON s.id = t.signal_id
        JOIN articles a ON a.id = s.article_id
        WHERE datetime(t.decided_at) >= datetime('now','-1 hour')
          AND a.published_at IS NOT NULL {cs}
        """
    )
    rows = await cur.fetchall()
    await cur.close()
    samples = [float(r["sec"]) for r in rows if r["sec"] is not None]
    p = _percentiles(samples)
    lines = [
        f"# irene news staleness ({_now().isoformat()})",
        "  (traded_at - article.published_at), in seconds, last 1h",
        "  The speed thesis says this should be small. Watch the p95.",
        "",
        f"  p50: {p['p50']:>10.2f} s",
        f"  p95: {p['p95']:>10.2f} s",
        f"  p99: {p['p99']:>10.2f} s",
        f"  n  : {p['count']:>10d}",
    ]
    return "\n".join(lines) + "\n"

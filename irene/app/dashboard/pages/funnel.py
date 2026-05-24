"""Decision outcomes in the last hour — where signals get dropped."""

from __future__ import annotations

from ..io import _now

FILENAME = "funnel.txt"


async def render(dash) -> str:
    lines = [
        f"# irene funnel ({_now().isoformat()})",
        "  decision outcomes in the last 1h",
        "",
    ]
    cur = await dash.db.conn.execute(
        """
        SELECT reason, COUNT(*) AS n
        FROM decisions
        WHERE created_at >= datetime('now','-1 hour')
        GROUP BY reason
        ORDER BY n DESC
        """
    )
    rows = await cur.fetchall()
    await cur.close()
    if not rows:
        lines.append("  (no decisions in the last hour)")
    else:
        total = sum(r["n"] for r in rows)
        for r in rows:
            pct = 100.0 * r["n"] / max(total, 1)
            lines.append(f"  {r['reason']:<28} n={r['n']:>5d}  ({pct:>5.1f}%)")
        lines.append("")
        lines.append(f"  total decisions:           n={total}")
    return "\n".join(lines) + "\n"

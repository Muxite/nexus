"""Decision outcomes in the last hour — where signals get dropped."""

from __future__ import annotations

from ..io import _now

FILENAME = "funnel.txt"


async def render(dash) -> str:
    # Cycle 4: scope by current_cycle_id so the funnel reflects THIS
    # cycle's behaviour, not lifetime decisions.
    cycle_id = dash.current_cycle_id
    cycle_sql = f"AND cycle_id = {int(cycle_id)}" if cycle_id is not None else ""
    lines = [
        f"# irene funnel ({_now().isoformat()})",
        f"  decision outcomes in the last 1h (cycle_id={cycle_id})",
        "",
    ]
    cur = await dash.db.conn.execute(
        f"""
        SELECT reason, COUNT(*) AS n
        FROM decisions
        WHERE datetime(created_at) >= datetime('now','-1 hour') {cycle_sql}
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

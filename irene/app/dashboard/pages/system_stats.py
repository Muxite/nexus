"""Per-container CPU/RAM snapshot — `system_stats.txt`.

Sourced from the `container_stats` table populated by
`app/system_stats.py`. Cycle-independent (system load is not a
cycle-scoped metric) so no `current_cycle_id` filter is applied — see
the same rationale on the system_stats collector.
"""

from __future__ import annotations

from datetime import datetime, timezone

from ..io import _now

FILENAME = "system_stats.txt"


def _fmt_bytes(n: int) -> str:
    if n >= 1 << 30:
        return f"{n / (1 << 30):>5.2f} GB"
    if n >= 1 << 20:
        return f"{n / (1 << 20):>5.0f} MB"
    if n >= 1 << 10:
        return f"{n / (1 << 10):>5.0f} KB"
    return f"{n:>5d}  B"


def _age_seconds(ts: str) -> float:
    try:
        sampled = datetime.fromisoformat(ts)
    except ValueError:
        return -1.0
    if sampled.tzinfo is None:
        sampled = sampled.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(timezone.utc) - sampled).total_seconds())


async def render(dash) -> str:
    cur = await dash.db.conn.execute(
        """
        SELECT cs.container, cs.ts, cs.cpu_pct, cs.mem_bytes, cs.mem_limit_bytes
        FROM container_stats cs
        JOIN (
            SELECT container, MAX(ts) AS max_ts
            FROM container_stats
            GROUP BY container
        ) latest
          ON latest.container = cs.container
         AND latest.max_ts    = cs.ts
        ORDER BY cs.container
        """
    )
    rows = [dict(r) for r in await cur.fetchall()]
    await cur.close()

    if not rows:
        return ""

    lines = [
        f"# irene system stats ({_now().isoformat()})",
        "  per-container CPU% and RAM; sampled every 30s, retained ~2h",
        "",
        f"  {'CONTAINER':<22} {'CPU%':>6}  {'MEM (used / limit)':<22} {'MEM%':>6}  {'AGE':>5}",
        "  " + "─" * 70,
    ]
    for r in rows:
        used = int(r["mem_bytes"])
        limit = int(r["mem_limit_bytes"])
        mem_pct = (100.0 * used / limit) if limit > 0 else 0.0
        age = _age_seconds(r["ts"])
        age_str = f"{age:>4.0f}s" if age >= 0 else "   ?"
        mem_str = f"{_fmt_bytes(used)} / {_fmt_bytes(limit)}"
        lines.append(
            f"  {r['container']:<22} "
            f"{float(r['cpu_pct']):>6.1f}  "
            f"{mem_str:<22} "
            f"{mem_pct:>5.1f}%  "
            f"{age_str:>5}"
        )
    return "\n".join(lines) + "\n"

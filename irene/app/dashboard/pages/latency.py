"""Per-stage latency table — the headline diagnostic for the speed thesis."""

from __future__ import annotations

from ..db_helpers import fetch_ms, latency_panel_sqls
from ..io import _now, _percentiles

FILENAME = "latency.txt"


async def render(dash) -> str:
    lines = [
        f"# irene latency ({_now().isoformat()})",
        f"  per-stage diff p50 / p95 / p99 (ms), last 1h (cycle_id={dash.current_cycle_id})",
        "",
    ]
    for label, sql in latency_panel_sqls(cycle_id=dash.current_cycle_id):
        samples = await fetch_ms(dash.db, sql)
        p = _percentiles(samples)
        lines.append(
            f"  {label:<28} "
            f"p50={p['p50']:>7.0f} "
            f"p95={p['p95']:>7.0f} "
            f"p99={p['p99']:>7.0f} "
            f"n={p['count']:>5d}"
        )
    return "\n".join(lines) + "\n"

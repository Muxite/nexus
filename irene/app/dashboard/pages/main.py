"""Operator landing page — `dashboard.txt`."""

from __future__ import annotations

import time

from ..db_helpers import end_to_end_latency_ms, scalar
from ..io import _now, _percentiles

FILENAME = "dashboard.txt"


async def render(dash) -> str:
    uptime = time.monotonic() - dash._t_boot
    lines = [
        f"# irene dashboard ({_now().isoformat()})",
        f"uptime: {uptime:.0f}s",
        "",
    ]
    for sql, label in (
        ("SELECT COUNT(*) FROM articles WHERE ingested_at >= datetime('now','-1 hour')",
         "articles ingested (1h)"),
        ("SELECT COUNT(*) FROM signals  WHERE created_at  >= datetime('now','-1 hour')",
         "signals matched   (1h)"),
        ("SELECT COUNT(*) FROM trades   WHERE decided_at  >= datetime('now','-1 hour')",
         "trades decided    (1h)"),
        ("SELECT COUNT(*) FROM decisions WHERE outcome='execute' AND "
         "created_at >= datetime('now','-1 hour')",
         "trades executed   (1h)"),
        ("SELECT COUNT(*) FROM decisions WHERE outcome='reject'  AND "
         "created_at >= datetime('now','-1 hour')",
         "trades rejected   (1h)"),
    ):
        n = await scalar(dash.db, sql)
        lines.append(f"  {label}: {n}")
    lines.append("")
    samples = await end_to_end_latency_ms(dash.db)
    p = _percentiles(samples)
    if p["count"]:
        lines += [
            "end-to-end latency (article.ingested → trade.decided), last 1h:",
            f"  p50={p['p50']:>8.0f} ms",
            f"  p95={p['p95']:>8.0f} ms",
            f"  p99={p['p99']:>8.0f} ms",
            f"  n  ={p['count']:>8d}",
        ]
    else:
        lines.append("end-to-end latency: no completed trades in the last hour.")
    return "\n".join(lines) + "\n"

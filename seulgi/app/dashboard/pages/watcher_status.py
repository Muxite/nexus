from __future__ import annotations

from ..io import _header, _ts

FILENAME = "watcher_status.txt"
TITLE = "WATCHER STATUS"


async def render(dash) -> str:
    lines = [_header("WATCHER STATUS")]
    if not dash._watcher_statuses:
        lines.append("  (watchers not yet started)")
    else:
        for url, ws in dash._watcher_statuses.items():
            if ws.is_unreachable:
                status = "UNREACHABLE"
            elif ws.failures == 0:
                status = "OK"
            else:
                status = f"DEGRADED ({ws.failures} failures)"
            since = _ts(ws.last_headline_at) if ws.last_headline_at else "never"
            lines.append(f"  {'✗' if ws.is_unreachable else '✓'} {url}")
            lines.append(f"    Status         : {status}")
            lines.append(f"    Last headline  : {since}")
            lines.append(f"    Total headlines: {ws.total_headlines}")
            if ws.last_headline:
                lines.append(f"    Latest         : {ws.last_headline[:90]}")
            if ws.budget_limit:
                pct = int((ws.budget_used or 0) / ws.budget_limit * 100)
                lines.append(
                    f"    Brave budget   : {ws.budget_used}/{ws.budget_limit} "
                    f"calls/hr  ({pct}%)"
                )
            lines.append("")
    return "\n".join(lines) + "\n"

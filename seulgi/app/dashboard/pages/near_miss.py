from __future__ import annotations

from ..io import _header, _trade_block

FILENAME = "near_miss.txt"
TITLE = "NEAR MISSES"


async def render(dash) -> str:
    lines = [
        _header("NEAR MISSES  (positive edge — just below threshold)"),
        "  These had a real edge but didn't quite clear the min_edge filter.",
        "  Useful for tuning: lower min_edge to start catching these.",
        "",
    ]
    if not dash._near_miss:
        lines.append("  (none yet — a good sign, threshold may be well-calibrated)")
    else:
        for ev in dash._near_miss:
            _trade_block(lines, ev)
    return "\n".join(lines) + "\n"

from __future__ import annotations

from ..io import _header

FILENAME = "activity_log.txt"
TITLE = "ACTIVITY LOG"


async def render(dash) -> str:
    lines = [_header("ACTIVITY LOG  (newest first)")]
    lines.extend(dash._activity)
    return "\n".join(lines) + "\n"

from __future__ import annotations

from ..io import _header, _trade_block

FILENAME = "trades_declined.txt"
TITLE = "DECLINED TRADES"


async def render(dash) -> str:
    lines = [_header("DECLINED TRADE DECISIONS  (most recent first)")]
    if not dash._declined:
        lines.append("  (none yet)")
    else:
        for ev in dash._declined:
            _trade_block(lines, ev)
    return "\n".join(lines) + "\n"

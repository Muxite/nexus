from __future__ import annotations

from ..io import _header, _trade_block

FILENAME = "trades_executed.txt"
TITLE = "EXECUTED TRADES"


async def render(dash) -> str:
    lines = [_header("EXECUTED TRADES  (most recent first)")]
    if not dash._executed:
        lines.append("  (none yet)")
    else:
        for ev in dash._executed:
            _trade_block(lines, ev)
    return "\n".join(lines) + "\n"

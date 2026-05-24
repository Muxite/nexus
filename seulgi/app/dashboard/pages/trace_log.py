from __future__ import annotations

from ..io import _header

FILENAME = "trace_log.txt"
TITLE = "TRACE LOG"


async def render(dash) -> str:
    lines = [_header("TRACE LOG  (newest first)")]
    lines.append(
        f"  {'TIMESTAMP':<19}  {'HID':<10}  {'SITE':<20}  "
        f"{'HEADLINE':<62}  {'MARKET':<52}  {'AI':<12}  OUTCOME"
    )
    lines.append("  " + "-" * 200)
    lines.extend(f"  {t}" for t in dash._traces)
    if not dash._traces:
        lines.append("  (no traces yet)")
    return "\n".join(lines) + "\n"

from __future__ import annotations

from ..io import _header, _ts

FILENAME = "signal_journal.txt"
TITLE = "SIGNAL JOURNAL"

_DIR_SYMBOL = {"YES": "▲", "NO": "▼"}


async def render(dash) -> str:
    lines = [
        _header("SIGNAL JOURNAL  (non-NEUTRAL AI decisions, newest first)"),
        "  Every non-NEUTRAL signal from any engine — whether traded or skipped.",
        "  Use this to see if the AI is reasoning about the right markets.",
        "",
    ]
    if not dash._journal:
        lines.append("  (no non-NEUTRAL signals yet — AI may be returning mostly NEUTRAL)")
    else:
        for e in dash._journal:
            sym = _DIR_SYMBOL.get(e["direction"], "?")
            lines.append(
                f"  [{_ts(e['ts'])}] [{e['engine']:<12}] "
                f"{sym} {e['direction']:<4}  conf={e['confidence']:.0%}  "
                f"sim={e['similarity']:.2f}"
            )
            lines.append(f"    Headline : {e['headline'][:95]}")
            lines.append(f"    Market   : {e['market'][:95]}")
            lines.append(f"    Outcome  : {e['outcome']}")
            if e["headline_id"]:
                lines.append(f"    hid={e['headline_id'][:8]}")
            lines.append("")
    return "\n".join(lines) + "\n"

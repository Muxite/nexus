from __future__ import annotations

from ..io import _header, _short, _ts

FILENAME = "news_seen.txt"
TITLE = "NEWS SEEN"


async def render(dash) -> str:
    lines = [_header("NEWS SEEN  (most recent first)")]
    if not dash._news:
        lines.append("  (no news detected yet — watchers starting up)")
    else:
        for ev in dash._news:
            lines.append(f"  [{_ts(ev.detected_at)}] {_short(ev.site_url)}")
            lines.append(f"    {ev.headline}")
            lines.append(f"    {ev.article_url}")
            lines.append("")
    return "\n".join(lines) + "\n"

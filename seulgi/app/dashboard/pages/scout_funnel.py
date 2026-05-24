"""Render the SCOUT decision funnel tile from the 5-min heavy cache."""

from __future__ import annotations

from datetime import datetime, timezone

FILENAME = "scout_funnel.txt"
TITLE = "SCOUT FUNNEL"


async def render(dash) -> str:
    cached = dash._heavy_cache.get("scout_funnel") if dash._db is not None else None
    last_refresh = dash._heavy_cache.get("_last_refresh_at")
    if cached is None:
        header = "# SCOUT funnel — warming up\n\n"
        if last_refresh is None:
            body = (
                "Cache is empty. The 5-min background refresh runs on "
                "first dashboard tick; check back shortly.\n"
            )
        else:
            age = int((datetime.now(timezone.utc) - last_refresh).total_seconds())
            body = (
                f"Last refresh attempt {age}s ago returned no data "
                "(query may have failed — see logs).\n"
            )
        return header + body

    from scout_funnel_core import render_funnel_markdown
    text = render_funnel_markdown(
        cached["stats"],
        engine_prefix="scout",
        since_hours=cached.get("since_hours", 0.0),
        cutoff=cached.get("cutoff", ""),
    )
    if last_refresh is not None:
        age = int((datetime.now(timezone.utc) - last_refresh).total_seconds())
        text = (
            text.rstrip("\n")
            + f"\n\n_5-min cache, refreshed {age}s ago "
              f"(cycle-scoped from {cached.get('cutoff', '?')})_\n"
        )
    return text

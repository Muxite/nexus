"""Stacked bar of decision reasons by hour, last 24h."""

from __future__ import annotations

from ._base import figure_to_png, plt

FILENAME = "funnel_dropoff.png"

# Stable colour map so the same reason gets the same colour each tick.
_REASON_COLOURS = {
    "ok":                 "#5cb085",
    "execute":            "#5cb085",
    "no_market":          "#888888",
    "low_similarity":     "#7788aa",
    "stale_news":         "#998877",
    "low_confidence":     "#d09844",
    "low_edge":           "#d09844",
    "min_size":           "#aa7755",
    "duplicate":          "#665577",
    "no_credit":          "#d04444",
    "budget_paused":      "#d04444",
}


async def render(dash, *, loop) -> bytes:
    cs = f"AND cycle_id = {int(dash.current_cycle_id)}" if dash.current_cycle_id is not None else ""
    cur = await dash.db.conn.execute(
        f"""
        SELECT strftime('%Y-%m-%dT%H:00', created_at) AS hour,
               reason, COUNT(*) AS n
        FROM decisions
        WHERE datetime(created_at) >= datetime('now','-24 hours') {cs}
        GROUP BY hour, reason
        ORDER BY hour ASC
        """
    )
    rows = await cur.fetchall()
    await cur.close()
    if not rows:
        return b""
    by_hour: dict[str, dict[str, int]] = {}
    reasons: set[str] = set()
    for r in rows:
        by_hour.setdefault(r["hour"], {})[r["reason"]] = int(r["n"])
        reasons.add(r["reason"])
    return await loop.run_in_executor(None, _draw, by_hour, sorted(reasons))


def _draw(by_hour: dict[str, dict[str, int]], reasons: list[str]) -> bytes:
    hours = sorted(by_hour.keys())
    xs = list(range(len(hours)))

    fig, ax = plt.subplots(figsize=(8, 3.5))
    bottoms = [0.0] * len(hours)
    for reason in reasons:
        ys = [by_hour[h].get(reason, 0) for h in hours]
        ax.bar(
            xs, ys, bottom=bottoms,
            color=_REASON_COLOURS.get(reason, "#445566"),
            label=reason, edgecolor="#222222",
        )
        bottoms = [b + y for b, y in zip(bottoms, ys)]

    ax.set_xticks(xs)
    ax.set_xticklabels([h[-5:] for h in hours], rotation=45, ha="right")
    ax.set_ylabel("decisions / hour")
    ax.set_title("Funnel drop-off by hour (last 24h)")
    ax.legend(
        loc="upper left", bbox_to_anchor=(1.02, 1.0),
        facecolor="#222222", edgecolor="#444444", fontsize=8,
    )
    return figure_to_png(fig)

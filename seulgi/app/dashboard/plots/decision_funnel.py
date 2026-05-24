"""Step bar of the decision-funnel cascade (last 1h)."""

from __future__ import annotations

from ._base import figure_to_png, plt

FILENAME = "decision_funnel.png"


async def render(dash, *, loop) -> bytes:
    funnel = dash._heavy_cache.get("decision_funnel")
    if not funnel or funnel.get("total", 0) == 0:
        return b""
    rows = [
        ("decisions seen",         funnel["total"]),
        ("after freshness gate",   funnel["total"] - funnel.get("failed_freshness", 0)),
        ("after relevance gate",   funnel["total"] - funnel.get("failed_freshness", 0)
                                   - funnel.get("failed_low_sim", 0)
                                   - funnel.get("failed_relevance", 0)),
        ("after pre-AI rules",     funnel["total"] - funnel.get("failed_freshness", 0)
                                   - funnel.get("failed_low_sim", 0)
                                   - funnel.get("failed_relevance", 0)
                                   - funnel.get("pre_ai_dropped", 0)),
        ("AI non-NEUTRAL",         funnel["total"] - funnel.get("failed_freshness", 0)
                                   - funnel.get("failed_low_sim", 0)
                                   - funnel.get("failed_relevance", 0)
                                   - funnel.get("pre_ai_dropped", 0)
                                   - funnel.get("ai_neutral", 0)),
        ("executed",               funnel["executed"]),
    ]
    return await loop.run_in_executor(None, _draw, rows)


def _draw(rows: list[tuple[str, int]]) -> bytes:
    labels = [r[0] for r in rows]
    values = [max(r[1], 0) for r in rows]

    fig, ax = plt.subplots(figsize=(8, 3.2))
    xs = list(range(len(labels)))
    bars = ax.bar(xs, values, color="#4488cc", edgecolor="#222222")
    ax.set_xticks(xs)
    ax.set_xticklabels(labels, rotation=22, ha="right")
    ax.set_ylabel("count (last 1h)")
    ax.set_title("Decision funnel — survivors at each gate")
    for bar, v in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2.0, bar.get_height(),
            str(v), ha="center", va="bottom", fontsize=8, color="#ffffff",
        )
    return figure_to_png(fig)

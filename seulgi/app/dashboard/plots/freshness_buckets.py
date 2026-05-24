"""Hit rate + PnL by news-age bucket (BREAKING / EVOLVING / REHASH / …)."""

from __future__ import annotations

from ._base import figure_to_png, plt

FILENAME = "freshness_buckets.png"


async def render(dash, *, loop) -> bytes:
    buckets = dash._heavy_cache.get("freshness_buckets") or []
    rows = [b for b in buckets
            if (b.get("n_wins") or 0) + (b.get("n_losses") or 0) > 0]
    if not rows:
        return b""
    return await loop.run_in_executor(None, _draw, rows)


def _draw(rows: list[dict]) -> bytes:
    labels = [b["label"] for b in rows]
    hits = []
    pnls = []
    for b in rows:
        closed = (b.get("n_wins") or 0) + (b.get("n_losses") or 0)
        hits.append((b["n_wins"] / closed * 100.0) if closed else 0.0)
        pnls.append(float(b.get("sum_pnl") or 0))

    fig, ax1 = plt.subplots(figsize=(8, 3.2))
    ax1.bar(labels, hits, color="#5cb085", alpha=0.75, edgecolor="#222222")
    ax1.set_ylabel("hit rate (%)", color="#7fcc9a")
    ax1.set_ylim(0, 100)
    ax1.axhline(50, color="#666666", linewidth=0.6, linestyle="--")
    ax2 = ax1.twinx()
    ax2.plot(labels, pnls, "o-", color="#ddaa44", linewidth=2)
    ax2.set_ylabel("sum PnL ($)", color="#ddaa44")
    ax2.axhline(0, color="#666666", linewidth=0.6)
    ax2.grid(False)
    ax1.set_title("Freshness buckets (24h): hit rate vs cumulative PnL")
    return figure_to_png(fig)

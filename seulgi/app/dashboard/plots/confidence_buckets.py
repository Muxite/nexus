"""Hit rate + cumulative PnL by confidence bucket (twin-axis bar+line)."""

from __future__ import annotations

from ._base import figure_to_png, plt

FILENAME = "confidence_buckets.png"


async def render(dash, *, loop) -> bytes:
    buckets = dash._heavy_cache.get("confidence_buckets") or []
    rows = [b for b in buckets
            if (b.get("n_wins") or 0) + (b.get("n_losses") or 0) > 0]
    if not rows:
        return b""
    return await loop.run_in_executor(None, _draw, rows)


def _draw(rows: list[dict]) -> bytes:
    labels = [b["bucket"] for b in rows]
    hits = []
    pnls = []
    for b in rows:
        closed = (b.get("n_wins") or 0) + (b.get("n_losses") or 0)
        hits.append((b["n_wins"] / closed * 100.0) if closed else 0.0)
        pnls.append(float(b.get("sum_pnl") or 0))

    fig, ax1 = plt.subplots(figsize=(8, 3.2))
    ax1.bar(labels, hits, color="#4488cc", alpha=0.75, edgecolor="#222222",
            label="hit rate (%)")
    ax1.set_ylabel("hit rate (%)", color="#88aadd")
    ax1.set_ylim(0, 100)
    ax1.axhline(50, color="#666666", linewidth=0.6, linestyle="--")
    ax2 = ax1.twinx()
    ax2.plot(labels, pnls, "o-", color="#ddaa44", linewidth=2,
             label="sum PnL ($)")
    ax2.set_ylabel("sum PnL ($)", color="#ddaa44")
    ax2.axhline(0, color="#666666", linewidth=0.6)
    ax2.grid(False)
    ax1.set_title("Confidence buckets (24h): hit rate vs cumulative PnL")
    return figure_to_png(fig)

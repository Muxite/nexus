"""Signal-source audit — quantify where signal comes from and what the
funnel waste looks like.

Generates four plots under `irene/data/dashboard/plots/`:

  1. `signal_volume_timeseries.png` — hourly articles + signals + decisions
     over the last 24h. Shows volume and any gaps in the pipeline.
  2. `signals_by_source.png` — top 20 sources by signal count (stacked
     RSS vs combined). Tells you where the bulk of signal is coming from.
  3. `signal_yield_by_source.png` — signals-per-article ratio per source,
     sorted ascending. The bottom of the chart is the noise floor (sources
     producing articles with no matching market); the top is the
     market-rich sources.
  4. `signal_yield_gap.png` — actual total signal volume vs the
     "theoretical healthy" yield if every source produced at the median
     of the top-quartile rate. The gap is how much noise the bottom
     sources are dragging in for free.

Output also includes a per-source CSV at
`data/dashboard/plots/signal_source_breakdown.csv` for any follow-up
analysis (per_host.yaml pruning, etc).

Run: `python3 -m scripts.analysis.signal_source_audit`
"""
from __future__ import annotations

import csv
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Optional

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from datetime import datetime, timedelta, timezone

DB_PATH = "/home/muk/projects/jennie/irene/data/irene.db"
PLOTS_DIR = Path("/home/muk/projects/jennie/irene/data/dashboard/plots")
CSV_PATH = PLOTS_DIR / "signal_source_breakdown.csv"

WINDOW_HOURS = 24


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _label_for(source: str, source_detail: Optional[str]) -> str:
    """Short human label for plot axes. Strips the URL down to host+path-hint."""
    detail = source_detail or ""
    # Strip the scheme + any trailing path
    short = detail.replace("https://", "").replace("http://", "")
    # Keep up to the first 3 segments
    short = "/".join(short.split("/")[:2])
    return f"{source}: {short}" if short else source


# ---------------------------------------------------------------------------
# Plot 1 — signal volume timeseries
# ---------------------------------------------------------------------------


def plot_timeseries(conn: sqlite3.Connection) -> None:
    # SQLite's strftime to get the floor-hour bucket as a string.
    sql = """
      SELECT strftime('%Y-%m-%d %H:00:00', {col}) AS hour, COUNT(*) AS n
      FROM {tbl} WHERE {col} >= datetime('now', '-{w} hours')
      GROUP BY hour ORDER BY hour
    """
    arts = {r["hour"]: r["n"] for r in conn.execute(sql.format(
        col="ingested_at", tbl="articles", w=WINDOW_HOURS,
    ))}
    sigs = {r["hour"]: r["n"] for r in conn.execute(sql.format(
        col="created_at", tbl="signals", w=WINDOW_HOURS,
    ))}
    decs = {r["hour"]: r["n"] for r in conn.execute(sql.format(
        col="created_at", tbl="decisions", w=WINDOW_HOURS,
    ))}

    # Union the hour keys so we plot consistent x-axis even on empty hours.
    hours = sorted(set(arts) | set(sigs) | set(decs))
    if not hours:
        print("plot_timeseries: no data in window; skipping")
        return
    xs = [datetime.strptime(h, "%Y-%m-%d %H:%M:%S") for h in hours]
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.plot(xs, [arts.get(h, 0) for h in hours], "-o", label="articles ingested", linewidth=2)
    ax.plot(xs, [sigs.get(h, 0) for h in hours], "-s", label="signals matched", linewidth=2)
    ax.plot(xs, [decs.get(h, 0) for h in hours], "-^", label="decisions made", linewidth=2)
    ax.set_title(f"Signal volume — last {WINDOW_HOURS}h hourly")
    ax.set_xlabel("hour (UTC)")
    ax.set_ylabel("count")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper left")
    fig.autofmt_xdate()
    fig.tight_layout()
    out = PLOTS_DIR / "signal_volume_timeseries.png"
    fig.savefig(out, dpi=110)
    plt.close(fig)
    print(f"wrote {out}")


# ---------------------------------------------------------------------------
# Plots 2 + 3 + 4 — per-source breakdowns
# ---------------------------------------------------------------------------


def _per_source(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        """
        SELECT a.source, a.source_detail,
               COUNT(DISTINCT a.id) AS articles,
               COUNT(s.id) AS signals
          FROM articles a LEFT JOIN signals s ON s.article_id = a.id
         WHERE a.ingested_at >= datetime('now', '-{w} hours')
         GROUP BY a.source, a.source_detail
         HAVING articles >= 5
        """.format(w=WINDOW_HOURS)
    ).fetchall()
    return [
        {
            "source": r["source"],
            "source_detail": r["source_detail"] or "",
            "label": _label_for(r["source"], r["source_detail"]),
            "articles": r["articles"],
            "signals": r["signals"],
            "yield": r["signals"] / r["articles"] if r["articles"] else 0.0,
        }
        for r in rows
    ]


def plot_signals_by_source(rows: list[dict]) -> None:
    top = sorted(rows, key=lambda r: -r["signals"])[:20]
    if not top:
        print("plot_signals_by_source: no rows; skipping")
        return
    labels = [r["label"] for r in top]
    ys = [r["signals"] for r in top]
    colors = ["#1f77b4" if r["source"] == "rss" else "#ff7f0e" for r in top]
    fig, ax = plt.subplots(figsize=(11, 7))
    ax.barh(labels, ys, color=colors)
    ax.invert_yaxis()
    ax.set_title(f"Signals by source — last {WINDOW_HOURS}h (top 20)")
    ax.set_xlabel("signals matched")
    legend_rss = matplotlib.patches.Patch(color="#1f77b4", label="rss")
    legend_combined = matplotlib.patches.Patch(color="#ff7f0e", label="combined")
    ax.legend(handles=[legend_rss, legend_combined])
    ax.grid(True, axis="x", alpha=0.3)
    fig.tight_layout()
    out = PLOTS_DIR / "signals_by_source.png"
    fig.savefig(out, dpi=110)
    plt.close(fig)
    print(f"wrote {out}")


def plot_yield_by_source(rows: list[dict]) -> None:
    # Show all sources sorted by yield ascending — the bottom = noise sources,
    # the top = market-rich sources.
    sorted_rows = sorted(rows, key=lambda r: r["yield"])
    if not sorted_rows:
        return
    labels = [r["label"] for r in sorted_rows]
    yields = [r["yield"] for r in sorted_rows]
    # Color: red for <0.10 (noise), yellow for 0.10-0.50 (mediocre), green for >0.50.
    colors = [
        "#d62728" if y < 0.10 else "#ffbf00" if y < 0.50 else "#2ca02c"
        for y in yields
    ]
    fig, ax = plt.subplots(figsize=(11, max(6, len(sorted_rows) * 0.22)))
    ax.barh(labels, yields, color=colors)
    ax.axvline(0.10, color="#d62728", linestyle=":", linewidth=1, label="0.10 noise floor")
    ax.axvline(0.50, color="#2ca02c", linestyle=":", linewidth=1, label="0.50 healthy")
    ax.set_title(f"Signal yield by source (signals/article) — last {WINDOW_HOURS}h")
    ax.set_xlabel("signals per article")
    ax.legend(loc="lower right")
    ax.grid(True, axis="x", alpha=0.3)
    fig.tight_layout()
    out = PLOTS_DIR / "signal_yield_by_source.png"
    fig.savefig(out, dpi=110)
    plt.close(fig)
    print(f"wrote {out}")


def plot_yield_gap(rows: list[dict]) -> dict:
    """Actual signal volume vs 'healthy' baseline.

    Healthy baseline = median yield of the top quartile, applied across
    all articles. Tells us "if every source were as productive as a
    top-quartile source, we'd see X signals — but we see Y. The gap is
    noise from low-yield sources."
    """
    if not rows:
        return {}
    sorted_rows = sorted(rows, key=lambda r: r["yield"])
    n = len(sorted_rows)
    q3_start = (3 * n) // 4
    top_q = sorted_rows[q3_start:]
    median_top = sorted(top_q, key=lambda r: r["yield"])[len(top_q) // 2]["yield"]

    actual_articles = sum(r["articles"] for r in rows)
    actual_signals = sum(r["signals"] for r in rows)
    healthy_signals = sum(min(r["articles"] * median_top, r["articles"] * 2.5) for r in rows)
    # `min` cap at 2.5/article guards against degenerate extrapolation
    # — a single market matching the same article 20x isn't "more signal".

    # Per-source: how many noise articles below the noise floor?
    noise_articles = sum(r["articles"] for r in rows if r["yield"] < 0.10)
    healthy_articles = sum(r["articles"] for r in rows if r["yield"] >= 0.50)

    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.bar(
        ["actual\n(all sources)", "if all sources\n== top-Q median", "current\nnoise-source articles", "current\nhealthy-source articles"],
        [actual_signals, healthy_signals, noise_articles, healthy_articles],
        color=["#1f77b4", "#2ca02c", "#d62728", "#2ca02c"],
    )
    for bar, val in zip(bars, [actual_signals, healthy_signals, noise_articles, healthy_articles]):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(actual_signals, healthy_signals) * 0.01,
                f"{int(val):,}", ha="center", va="bottom", fontsize=10)
    ax.set_title(
        f"Signal yield gap — last {WINDOW_HOURS}h\n"
        f"top-quartile median yield = {median_top:.2f} signals/article"
    )
    ax.set_ylabel("count")
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    out = PLOTS_DIR / "signal_yield_gap.png"
    fig.savefig(out, dpi=110)
    plt.close(fig)
    print(f"wrote {out}")
    return {
        "actual_articles": actual_articles,
        "actual_signals": actual_signals,
        "healthy_signals": int(healthy_signals),
        "top_quartile_median_yield": median_top,
        "noise_source_articles": noise_articles,
        "healthy_source_articles": healthy_articles,
    }


def write_csv(rows: list[dict]) -> None:
    fields = ["source", "source_detail", "articles", "signals", "yield"]
    with CSV_PATH.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for r in sorted(rows, key=lambda r: -r["signals"]):
            writer.writerow({k: r[k] for k in fields})
    print(f"wrote {CSV_PATH}")


def main() -> int:
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    conn = _connect()
    try:
        plot_timeseries(conn)
        rows = _per_source(conn)
        plot_signals_by_source(rows)
        plot_yield_by_source(rows)
        gap = plot_yield_gap(rows)
        write_csv(rows)
        print()
        print("=== summary ===")
        print(f"  sources analyzed       : {len(rows)}")
        if gap:
            print(f"  articles (24h)         : {gap['actual_articles']:,}")
            print(f"  signals (24h)          : {gap['actual_signals']:,}")
            print(f"  top-Q median yield     : {gap['top_quartile_median_yield']:.2f} sig/art")
            print(f"  healthy-baseline yield : {gap['healthy_signals']:,} (if all == top-Q median)")
            print(f"  noise-source articles  : {gap['noise_source_articles']:,} "
                  f"(yield < 0.10)")
            print(f"  healthy-source articles: {gap['healthy_source_articles']:,} "
                  f"(yield >= 0.50)")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""
Seulgi quant plotter — periodic matplotlib renders of all DB-backed stats.

Every `interval_secs` (default 600 = 10 min) this writes a fresh PNG for each
chart into <dashboard_dir>/plots/. Plots use ALL available data; queries are
unbounded so the longer Seulgi runs, the richer the chart.

Charts produced:
  calibration.png      — hit-rate vs confidence bucket + diagonal reference
  alpha_decay.png      — precision at t1m/t5m/t15m/t30m/t60m per direction
  source_breakdown.png — bar chart of hit-rate by source (news/twitter/...)
  scatter_ic.png       — scatter of signed_confidence vs Δ5m with IC value
  latency_hist.png     — histogram of signal_latency_ms
  precision_over_time.png — rolling hit-rate vs detection time

All renders run in a thread executor — matplotlib is synchronous and CPU-bound,
so off-loading keeps the asyncio loop responsive. Failure to plot is logged
and swallowed; the rest of seulgi continues regardless.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from db import SeulgiDB

logger = logging.getLogger(__name__)


class QuantPlotter:
    """Periodically renders matplotlib charts of seulgi's stats."""

    def __init__(
        self,
        db: SeulgiDB,
        dashboard_dir: str,
        interval_secs: float = 600.0,
    ) -> None:
        self._db = db
        self._plots_dir = Path(dashboard_dir) / "plots"
        self._interval = interval_secs
        self._running = False

    async def run(self) -> None:
        """Background loop — render every interval_secs. Cancelled on shutdown."""
        self._running = True
        self._plots_dir.mkdir(parents=True, exist_ok=True)
        logger.info("[plotter] plots dir: %s  interval=%.0fs", self._plots_dir, self._interval)

        # First render right away so the user sees something during the first hour
        await self._render_once()
        while self._running:
            try:
                await asyncio.sleep(self._interval)
                await self._render_once()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("[plotter] render error: %s", exc)
        self._running = False

    async def _render_once(self) -> None:
        """Fetch ALL data and hand off to the sync renderer in an executor."""
        try:
            data = await self._collect_data()
        except Exception as exc:
            logger.warning("[plotter] data collection error: %s", exc)
            return

        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, _render_all, data, str(self._plots_dir))
            logger.info("[plotter] rendered %d charts to %s", len(_CHARTS), self._plots_dir)
        except Exception as exc:
            logger.warning("[plotter] matplotlib render failed: %s", exc)

    async def _collect_data(self) -> dict:
        """Pull every DB query needed by the renderer."""
        return {
            "calibration":    await self._db.get_calibration_curve(),
            "alpha_decay":    await self._db.get_alpha_decay(),
            "source_stats":   await self._db.get_source_stats(),
            "ic_pairs":       await self._db.get_ic_pairs(),
            "latencies":      await _fetch_all_latencies(self._db),
            "precision_ts":   await _fetch_precision_timeseries(self._db),
            "generated_at":   datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }


# ---------------------------------------------------------------------------
# Extra DB pulls used only by plots — no need to clutter db.py for these
# ---------------------------------------------------------------------------


async def _fetch_all_latencies(db: SeulgiDB) -> list[float]:
    async with db._conn.execute(
        "SELECT signal_latency_ms FROM engine_results "
        "WHERE signal_latency_ms IS NOT NULL"
    ) as cur:
        return [row[0] for row in await cur.fetchall()]


async def _fetch_precision_timeseries(db: SeulgiDB) -> list[dict]:
    """
    One row per price_impact, ordered by detection time.
    Used to compute a rolling-window hit rate in the renderer.
    """
    async with db._conn.execute(
        """
        SELECT detected_at, direction, delta_t5m
        FROM price_impacts
        WHERE delta_t5m IS NOT NULL AND direction IN ('YES','NO')
        ORDER BY detected_at ASC
        """
    ) as cur:
        return [dict(r) for r in await cur.fetchall()]


# ---------------------------------------------------------------------------
# Synchronous matplotlib renderer (runs in a thread executor)
# ---------------------------------------------------------------------------

_CHARTS = (
    "calibration.png",
    "alpha_decay.png",
    "source_breakdown.png",
    "scatter_ic.png",
    "latency_hist.png",
    "precision_over_time.png",
)


def _render_all(data: dict, out_dir: str) -> None:
    """Render every chart. Each is wrapped in its own try so one failure
    doesn't kill the rest."""
    # Configure matplotlib for headless rendering BEFORE pyplot import
    os.environ.setdefault("MPLBACKEND", "Agg")
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt  # noqa: E402

    plt.rcParams["figure.dpi"] = 110
    plt.rcParams["savefig.bbox"] = "tight"
    plt.rcParams["axes.grid"] = True
    plt.rcParams["grid.alpha"] = 0.3

    ts = data.get("generated_at", "")

    for name, fn in (
        ("calibration",         _plot_calibration),
        ("alpha_decay",         _plot_alpha_decay),
        ("source_breakdown",    _plot_source_breakdown),
        ("scatter_ic",          _plot_scatter_ic),
        ("latency_hist",        _plot_latency_hist),
        ("precision_over_time", _plot_precision_timeseries),
    ):
        path = os.path.join(out_dir, f"{name}.png")
        try:
            fn(plt, data, path, ts)
        except Exception as exc:
            logger.warning("[plotter] %s render failed: %s", name, exc)


# ── Individual plots ────────────────────────────────────────────────────────


def _plot_calibration(plt, data: dict, path: str, ts: str) -> None:
    rows = data["calibration"]
    fig, ax = plt.subplots(figsize=(6.5, 5))
    if not rows:
        ax.text(0.5, 0.5, "no data yet", ha="center", va="center", transform=ax.transAxes)
    else:
        confs = [r["conf_bucket"] for r in rows]
        rates = [(r["hits"] / r["n"]) if r["n"] else 0.0 for r in rows]
        sizes = [max(20, min(400, (r["n"] or 0) * 4)) for r in rows]
        ax.plot([0, 1], [0, 1], "k--", linewidth=1, alpha=0.4, label="perfect calibration")
        ax.scatter(confs, rates, s=sizes, alpha=0.7, edgecolors="black")
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axhline(0.5, color="red", linestyle=":", linewidth=0.8, alpha=0.5)
        for r, c, rate in zip(rows, confs, rates):
            ax.annotate(f"n={r['n']}", (c, rate),
                        textcoords="offset points", xytext=(7, 4), fontsize=8)
    ax.set_xlabel("AI confidence")
    ax.set_ylabel("Actual hit rate (Δ5m in predicted direction)")
    ax.set_title(f"Calibration  ({ts})")
    ax.legend(loc="upper left")
    fig.savefig(path)
    plt.close(fig)


def _plot_alpha_decay(plt, data: dict, path: str, ts: str) -> None:
    rows = data["alpha_decay"]
    fig, ax = plt.subplots(figsize=(7, 5))
    buckets = [1, 5, 15, 30, 60]
    if not rows:
        ax.text(0.5, 0.5, "no t60m-completed rows yet", ha="center", va="center",
                transform=ax.transAxes)
    else:
        for r in rows:
            ys = [r.get(f"hit_t{b}m") or 0.0 for b in buckets]
            ax.plot(buckets, ys, marker="o", linewidth=2,
                    label=f"{r['direction']} (n={r['n']})")
        ax.axhline(0.5, color="red", linestyle=":", linewidth=0.8,
                   alpha=0.5, label="random baseline")
    ax.set_xlabel("Time bucket (minutes after headline)")
    ax.set_ylabel("Hit rate")
    ax.set_ylim(0, 1)
    ax.set_xticks(buckets)
    ax.set_title(f"Alpha decay  ({ts})")
    ax.legend()
    fig.savefig(path)
    plt.close(fig)


def _plot_source_breakdown(plt, data: dict, path: str, ts: str) -> None:
    rows = data["source_stats"]
    fig, ax = plt.subplots(figsize=(7, 5))
    if not rows:
        ax.text(0.5, 0.5, "no data yet", ha="center", va="center", transform=ax.transAxes)
    else:
        sources = [r["source"] or "news" for r in rows]
        rates = [(r["hits"] / r["n"]) if r["n"] else 0.0 for r in rows]
        ns = [r["n"] or 0 for r in rows]
        bars = ax.bar(sources, rates, color=["steelblue", "darkorange", "seagreen", "crimson"][:len(rows)])
        ax.axhline(0.5, color="red", linestyle=":", linewidth=0.8, alpha=0.6,
                   label="random baseline")
        for bar, n, rate in zip(bars, ns, rates):
            ax.text(bar.get_x() + bar.get_width() / 2, rate + 0.01,
                    f"n={n}\n{rate:.0%}", ha="center", va="bottom", fontsize=9)
    ax.set_ylabel("Hit rate (Δ5m in predicted direction)")
    ax.set_ylim(0, 1)
    ax.set_title(f"Precision by source  ({ts})")
    ax.legend()
    fig.savefig(path)
    plt.close(fig)


def _plot_scatter_ic(plt, data: dict, path: str, ts: str) -> None:
    pairs = data["ic_pairs"]
    fig, ax = plt.subplots(figsize=(7, 6))
    if not pairs:
        ax.text(0.5, 0.5, "no data yet", ha="center", va="center", transform=ax.transAxes)
    else:
        xs = [p[0] for p in pairs]
        ys = [p[1] for p in pairs]
        ic = _pearson(xs, ys)
        # Colour: green if Δ5m moved in predicted direction, red if not
        colours = ["green" if (x > 0) == (y > 0) else "red" for x, y in pairs]
        ax.scatter(xs, ys, c=colours, alpha=0.5, s=22, edgecolors="none")
        ax.axhline(0, color="black", linewidth=0.5)
        ax.axvline(0, color="black", linewidth=0.5)
        ax.set_title(f"Signed confidence vs Δ5m   IC = {ic:+.4f}  (n={len(pairs)})  ({ts})")
    ax.set_xlabel("Signed AI confidence  (+conf=YES, -conf=NO)")
    ax.set_ylabel("Δ5m (price change 5 minutes after headline)")
    fig.savefig(path)
    plt.close(fig)


def _plot_latency_hist(plt, data: dict, path: str, ts: str) -> None:
    latencies = data["latencies"]
    fig, ax = plt.subplots(figsize=(7, 5))
    if not latencies:
        ax.text(0.5, 0.5, "no data yet", ha="center", va="center", transform=ax.transAxes)
    else:
        # Clip extreme tail at 99th percentile so the histogram remains readable
        latencies_sorted = sorted(latencies)
        clip = latencies_sorted[int(0.99 * len(latencies_sorted))]
        clipped = [min(x, clip) for x in latencies]
        ax.hist(clipped, bins=40, color="steelblue", edgecolor="black", alpha=0.85)
        avg = sum(latencies) / len(latencies)
        ax.axvline(avg, color="red", linestyle="--", linewidth=1.2,
                   label=f"mean = {avg:.0f} ms")
        ax.axvline(5000, color="darkgreen", linestyle=":", linewidth=1.2,
                   label="5s target")
    ax.set_xlabel("Signal latency (ms, clipped at p99)")
    ax.set_ylabel("Count")
    ax.set_title(f"Signal latency distribution  ({ts})")
    ax.legend()
    fig.savefig(path)
    plt.close(fig)


def _plot_precision_timeseries(plt, data: dict, path: str, ts: str) -> None:
    rows = data["precision_ts"]
    fig, ax = plt.subplots(figsize=(9, 5))
    if len(rows) < 5:
        ax.text(0.5, 0.5, "need ≥5 samples for time-series",
                ha="center", va="center", transform=ax.transAxes)
    else:
        # Compute rolling hit rate with window = max(20, len/10)
        window = max(20, len(rows) // 10)
        times = []
        rates = []
        hits_buf: list[int] = []
        for r in rows:
            d = r["direction"]
            d5 = r["delta_t5m"]
            hit = 1 if ((d == "YES" and d5 > 0) or (d == "NO" and d5 < 0)) else 0
            hits_buf.append(hit)
            if len(hits_buf) > window:
                hits_buf.pop(0)
            if len(hits_buf) >= max(5, window // 2):
                try:
                    t = datetime.fromisoformat(r["detected_at"])
                except (ValueError, TypeError):
                    continue
                times.append(t)
                rates.append(sum(hits_buf) / len(hits_buf))
        if times:
            ax.plot(times, rates, color="steelblue", linewidth=1.5)
            ax.axhline(0.5, color="red", linestyle=":", linewidth=0.8,
                       alpha=0.6, label="random baseline")
            ax.set_ylim(0, 1)
            ax.set_title(
                f"Rolling hit rate  (window={window}, n_total={len(rows)})  ({ts})"
            )
            fig.autofmt_xdate()
            ax.legend()
    ax.set_xlabel("Detection time (UTC)")
    ax.set_ylabel("Hit rate (rolling window)")
    fig.savefig(path)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Stats helper (duplicated from dashboard.py to keep this module self-contained)
# ---------------------------------------------------------------------------


def _pearson(xs: list[float], ys: list[float]) -> float:
    n = len(xs)
    if n < 2:
        return 0.0
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sx2 = sum((x - mx) ** 2 for x in xs)
    sy2 = sum((y - my) ** 2 for y in ys)
    den = (sx2 * sy2) ** 0.5
    return num / den if den else 0.0

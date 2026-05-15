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
import math
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

        # Trade-example charts (price history + event lines + headline text).
        # Variable file set, written into plots/trade_examples/. Independent
        # of the 13 fixed dashboards.
        try:
            # Cycle-8: clean stale PNGs each render so we don't accumulate
            # cross-cycle bloat. The cap is 8 fresh examples per render now,
            # so the dir size stays bounded.
            out_dir = self._plots_dir / "trade_examples"
            out_dir.mkdir(parents=True, exist_ok=True)
            for p in out_dir.glob("*.png"):
                try:
                    p.unlink()
                except Exception:  # noqa: BLE001
                    pass

            examples = await self._collect_trade_examples()
            if examples:
                await loop.run_in_executor(
                    None, _render_trade_examples, examples, str(out_dir),
                )
                logger.info(
                    "[plotter] rendered %d trade-example charts to %s",
                    len(examples), out_dir,
                )
        except Exception as exc:
            logger.warning("[plotter] trade-example render failed: %s", exc)

    async def _collect_trade_examples(self) -> list[dict]:
        """
        Sample plot-eligible price_impacts (full timeseries backfill) across
        the four missed-alpha buckets so the user sees a mix of too_slow /
        captured / no_signal / wrong_dir.

        Cycle-8: scope to the current cycle only — historical examples stay
        in DB for SQL trace but shouldn't clutter the live plots.
        """
        from impact_tracker import classify_missed_alpha  # local import to avoid cycle

        cycle = await self._db.get_current_cycle()
        cycle_start = cycle["started_at"] if cycle else None

        candidates = await self._db.get_plot_candidate_impacts(limit=400, min_samples=30)
        if cycle_start:
            candidates = [r for r in candidates
                          if (r.get("detected_at") or "") >= cycle_start]
        # Group by bucket so we sample evenly. Avoids one big bucket dominating.
        by_bucket: dict[str, list[dict]] = {
            "too_slow": [], "captured": [], "no_signal": [], "wrong_dir": [],
        }
        for row in candidates:
            bucket = classify_missed_alpha(
                row.get("direction"),
                row.get("delta_pre_60s"),
                row.get("delta_t5m"),
            )
            if bucket is None:
                continue
            by_bucket[bucket].append(row)

        # Deterministic 1/4 sample per bucket: take every 4th row (already
        # ordered newest-first by the query). Stable across plotter ticks
        # within a single trial so the user can spot trends.
        sampled: list[dict] = []
        for bucket_rows in by_bucket.values():
            sampled.extend(bucket_rows[::4])

        # Cycle-8: lowered cap from 24 → 8 plus the plotter now clears the
        # trade_examples/ dir each render to avoid the 64 MB / 892-file
        # accumulation we saw in cycle 7. Snapshots therefore archive ~8
        # current-cycle examples instead of every render's worth.
        sampled.sort(key=lambda r: r.get("detected_at") or "", reverse=True)
        sampled = sampled[:8]
        result = []
        for row in sampled:
            series = await self._db.get_timeseries_for_impact(row["id"])
            if len(series) < 30:
                continue
            row["_series"] = series
            row["_bucket"] = classify_missed_alpha(
                row.get("direction"),
                row.get("delta_pre_60s"),
                row.get("delta_t5m"),
            )
            # Fetch article body for context if available
            try:
                async with self._db._conn.execute(
                    "SELECT body FROM headline_articles WHERE headline_id = ?",
                    (row["headline_id"],),
                ) as cur:
                    art = await cur.fetchone()
                    row["_article_body"] = art[0] if art else None
            except Exception:
                row["_article_body"] = None
            result.append(row)
        return result

    async def _collect_data(self) -> dict:
        """Pull every DB query needed by the renderer."""
        return {
            "calibration":         await self._db.get_calibration_curve(),
            "alpha_decay":         await self._db.get_alpha_decay(),
            "source_stats":        await self._db.get_source_stats(),
            "source_stats_sites":  await _fetch_source_stats_per_site(self._db),
            "ic_pairs":            await self._db.get_ic_pairs(),
            "latencies":           await _fetch_all_latencies(self._db),
            "precision_ts":        await _fetch_precision_timeseries(self._db),
            "similarity_buckets":  await _fetch_similarity_buckets(self._db),
            "delta_5m_dist":       await _fetch_delta_5m_distribution(self._db),
            "edge_vs_pnl":         await _fetch_edge_vs_pnl(self._db),
            "close_reasons":       await _fetch_close_reasons(self._db),
            "confidence_dist":     await _fetch_confidence_distribution(self._db),
            "pnl_over_time":       await _fetch_pnl_over_time(self._db),
            "tokens_over_time":    await _fetch_tokens_over_time(self._db),
            "trade_pnl_stats":     await _fetch_trade_pnl_stats(self._db),
            "generated_at":        datetime.now(timezone.utc).isoformat(timespec="seconds"),
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


async def _fetch_similarity_buckets(db: SeulgiDB) -> list[dict]:
    """Hit-rate + count grouped by similarity decile."""
    async with db._conn.execute(
        """
        SELECT ROUND(similarity * 10) / 10.0 AS sim_bucket,
               COUNT(*) AS n,
               SUM(CASE WHEN (direction='YES' AND delta_t5m > 0)
                             OR (direction='NO' AND delta_t5m < 0)
                        THEN 1 ELSE 0 END) AS hits,
               AVG(ABS(delta_t5m)) AS avg_abs_delta
        FROM price_impacts
        WHERE delta_t5m IS NOT NULL AND direction IN ('YES','NO')
        GROUP BY sim_bucket
        ORDER BY sim_bucket
        """
    ) as cur:
        return [dict(r) for r in await cur.fetchall()]


async def _fetch_delta_5m_distribution(db: SeulgiDB) -> list[float]:
    async with db._conn.execute(
        "SELECT delta_t5m FROM price_impacts WHERE delta_t5m IS NOT NULL"
    ) as cur:
        return [row[0] for row in await cur.fetchall()]


async def _fetch_edge_vs_pnl(db: SeulgiDB) -> list[dict]:
    """
    For each closed trade (realized_pnl IS NOT NULL), pair the engine's
    predicted edge with the realized PnL normalized to 1.0 stake.
    """
    async with db._conn.execute(
        """
        SELECT er.edge AS edge,
               t.realized_pnl / t.size_usdc AS norm_pnl,
               t.size_usdc AS size,
               er.engine_name
        FROM trades t
        JOIN engine_results er
          ON er.headline_id = t.headline_id
         AND er.condition_id = t.condition_id
         AND er.executed = 1
        WHERE t.realized_pnl IS NOT NULL
          AND t.size_usdc > 0
        """
    ) as cur:
        return [dict(r) for r in await cur.fetchall()]


async def _fetch_source_stats_per_site(db: SeulgiDB, top_n: int = 30) -> list[dict]:
    """
    Per-site (site_url, not just the 'news'/'twitter' source bucket) precision.
    The original get_source_stats() collapses to 3-4 categories which throws
    away the per-outlet variation we actually want to act on. This query
    surfaces it.
    """
    async with db._conn.execute(
        """
        SELECT site_url,
               COUNT(*) AS n,
               SUM(CASE WHEN (direction='YES' AND delta_t5m > 0)
                             OR (direction='NO' AND delta_t5m < 0)
                        THEN 1 ELSE 0 END) AS hits,
               AVG(ABS(delta_t5m)) AS avg_abs_delta,
               AVG(ai_confidence) AS avg_confidence
        FROM price_impacts
        WHERE delta_t5m IS NOT NULL AND direction IN ('YES','NO')
              AND site_url IS NOT NULL
        GROUP BY site_url
        ORDER BY n DESC
        LIMIT ?
        """,
        (top_n,),
    ) as cur:
        return [dict(r) for r in await cur.fetchall()]


async def _fetch_trade_pnl_stats(db: SeulgiDB) -> dict:
    """
    Returns the sample mean / std of per-trade *normalized* PnL (realized_pnl
    / size_usdc) and the trade rate over the most recent activity window.
    Used by the sample-size plot to drive a Central-Limit-Theorem-based "how
    many more trades do we need?" calculation.
    """
    async with db._conn.execute(
        """
        SELECT realized_pnl, size_usdc, closed_at
        FROM trades
        WHERE realized_pnl IS NOT NULL AND size_usdc > 0 AND closed_at IS NOT NULL
        ORDER BY closed_at ASC
        """
    ) as cur:
        rows = await cur.fetchall()
    if not rows:
        return {"n": 0, "mean": 0.0, "std": 0.0, "trades_per_hour": 0.0, "ts": []}

    norm = [float(r[0]) / float(r[1]) for r in rows]
    n = len(norm)
    mean = sum(norm) / n
    var = sum((x - mean) ** 2 for x in norm) / max(1, n - 1)  # unbiased
    std = var ** 0.5

    # Trade rate over the realized window
    try:
        from datetime import datetime as _dt
        first = _dt.fromisoformat(rows[0][2])
        last = _dt.fromisoformat(rows[-1][2])
        hours = max(1e-6, (last - first).total_seconds() / 3600.0)
        rate = n / hours
    except Exception:  # noqa: BLE001
        rate = 0.0

    return {
        "n": n, "mean": mean, "std": std,
        "trades_per_hour": rate,
        "norm_pnl_series": norm,
    }


async def _fetch_close_reasons(db: SeulgiDB) -> dict[str, int]:
    """
    Bucket closed trades by why they closed, using the close_reason persisted
    by the trader. Legacy trades that closed before the column existed are
    grouped under "UNKNOWN" so the pie chart still renders.
    """
    async with db._conn.execute(
        """
        SELECT COALESCE(close_reason, 'UNKNOWN') AS reason, COUNT(*) AS n
        FROM trades
        WHERE closed_at IS NOT NULL
        GROUP BY reason
        """
    ) as cur:
        rows = await cur.fetchall()
    return {row["reason"]: int(row["n"]) for row in rows}


async def _fetch_confidence_distribution(db: SeulgiDB) -> list[float]:
    """All AI confidence scores recorded across engine_results (non-NEUTRAL)."""
    async with db._conn.execute(
        """
        SELECT confidence FROM engine_results
        WHERE direction IN ('YES','NO')
        """
    ) as cur:
        return [row[0] for row in await cur.fetchall()]


async def _fetch_pnl_over_time(db: SeulgiDB) -> list[dict]:
    """Closed trades in chronological order with engine name — for cumulative curves."""
    async with db._conn.execute(
        """
        SELECT t.closed_at, t.realized_pnl, er.engine_name
        FROM trades t
        JOIN engine_results er
          ON er.headline_id = t.headline_id
         AND er.condition_id = t.condition_id
         AND er.executed = 1
        WHERE t.realized_pnl IS NOT NULL
          AND t.closed_at IS NOT NULL
        ORDER BY t.closed_at ASC
        """
    ) as cur:
        return [dict(r) for r in await cur.fetchall()]


async def _fetch_tokens_over_time(db: SeulgiDB, bucket_minutes: int = 15) -> list[dict]:
    """Token spend bucketed every `bucket_minutes`."""
    async with db._conn.execute(
        f"""
        SELECT strftime('%Y-%m-%dT%H:', ts) ||
               printf('%02d', (CAST(strftime('%M', ts) AS INTEGER) / {bucket_minutes}) * {bucket_minutes})
                 AS bucket_start,
               SUM(count) AS tokens
        FROM usage_events
        WHERE kind = 'ai_tokens'
        GROUP BY bucket_start
        ORDER BY bucket_start ASC
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
    "source_breakdown_sites.png",
    "scatter_ic.png",
    "latency_hist.png",
    "precision_over_time.png",
    "similarity_buckets.png",
    "delta_5m_dist.png",
    "edge_vs_pnl.png",
    "close_reasons.png",
    "confidence_dist.png",
    "pnl_over_time.png",
    "tokens_over_time.png",
    "sample_size.png",
)


"""
Palette: Okabe-Ito-inspired colourblind-safe set, plus a few semantic accents
the trader-style charts need (WIN/LOSS/TP/SL). Use these everywhere instead
of ad-hoc colour names so every chart in the dashboard reads as one set.
"""
PALETTE = {
    # Primary categorical colours (Okabe-Ito) — for engines, sources, buckets
    "primary":     "#0072B2",  # strong blue
    "secondary":   "#E69F00",  # orange
    "tertiary":    "#009E73",  # green
    "quaternary":  "#CC79A7",  # rose
    "quinary":     "#56B4E9",  # sky
    "senary":      "#D55E00",  # vermillion
    "septenary":   "#F0E442",  # yellow
    "octonary":    "#000000",  # black
    # Semantic
    "gain":        "#1B7F4D",  # deep emerald — realized profit
    "loss":        "#B5142E",  # deep crimson — realized loss
    "neutral":     "#5C6770",  # slate — neutral / unknown
    "baseline":    "#A0A4AB",  # light grey — reference lines
    "ci_band":     "#C7D8EC",  # pale blue — confidence-interval fill
    "highlight":   "#FFB000",  # amber — call-out
}
CAT_ORDER = ["primary", "secondary", "tertiary", "quaternary",
             "quinary", "senary", "septenary"]


def _cat_colors(n: int) -> list[str]:
    """Return n categorical colours in canonical order (wraps if n > len)."""
    return [PALETTE[CAT_ORDER[i % len(CAT_ORDER)]] for i in range(n)]


def _render_all(data: dict, out_dir: str) -> None:
    """Render every chart. Each is wrapped in its own try so one failure
    doesn't kill the rest."""
    # Configure matplotlib for headless rendering BEFORE pyplot import
    os.environ.setdefault("MPLBACKEND", "Agg")
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt  # noqa: E402

    plt.rcParams["figure.dpi"] = 120
    plt.rcParams["savefig.bbox"] = "tight"
    plt.rcParams["axes.grid"] = True
    plt.rcParams["grid.alpha"] = 0.22
    plt.rcParams["grid.linestyle"] = "--"
    plt.rcParams["axes.spines.top"] = False
    plt.rcParams["axes.spines.right"] = False
    plt.rcParams["axes.edgecolor"] = "#333740"
    plt.rcParams["axes.labelcolor"] = "#22262C"
    plt.rcParams["axes.titleweight"] = "semibold"
    plt.rcParams["axes.titlesize"] = 12
    plt.rcParams["axes.labelsize"] = 10
    plt.rcParams["xtick.color"] = "#444851"
    plt.rcParams["ytick.color"] = "#444851"
    plt.rcParams["font.family"] = ["DejaVu Sans"]
    plt.rcParams["axes.prop_cycle"] = matplotlib.cycler(
        color=[PALETTE[k] for k in CAT_ORDER]
    )

    ts = data.get("generated_at", "")

    for name, fn in (
        ("calibration",            _plot_calibration),
        ("alpha_decay",            _plot_alpha_decay),
        ("source_breakdown",       _plot_source_breakdown),
        ("source_breakdown_sites", _plot_source_breakdown_per_site),
        ("scatter_ic",             _plot_scatter_ic),
        ("latency_hist",           _plot_latency_hist),
        ("precision_over_time",    _plot_precision_timeseries),
        ("similarity_buckets",     _plot_similarity_buckets),
        ("delta_5m_dist",          _plot_delta_5m_dist),
        ("edge_vs_pnl",            _plot_edge_vs_pnl),
        ("close_reasons",          _plot_close_reasons),
        ("confidence_dist",        _plot_confidence_dist),
        ("pnl_over_time",          _plot_pnl_over_time),
        ("tokens_over_time",       _plot_tokens_over_time),
        ("sample_size",            _plot_sample_size),
    ):
        path = os.path.join(out_dir, f"{name}.png")
        try:
            fn(plt, data, path, ts)
        except Exception as exc:
            logger.warning("[plotter] %s render failed: %s", name, exc)


# ── Individual plots ────────────────────────────────────────────────────────


def _plot_calibration(plt, data: dict, path: str, ts: str) -> None:
    rows = data["calibration"]
    fig, ax = plt.subplots(figsize=(7, 5.5))
    if not rows:
        ax.text(0.5, 0.5, "no data yet", ha="center", va="center", transform=ax.transAxes)
    else:
        confs = [r["conf_bucket"] for r in rows]
        rates = [(r["hits"] / r["n"]) if r["n"] else 0.0 for r in rows]
        # 95% Wilson CI error bars
        los, his = [], []
        for r, rate in zip(rows, rates):
            lo, hi = _wilson_ci(int(r["hits"] or 0), int(r["n"] or 0))
            los.append(rate - lo)
            his.append(hi - rate)
        sizes = [max(20, min(400, (r["n"] or 0) * 4)) for r in rows]
        ax.plot([0, 1], [0, 1], "k--", linewidth=1, alpha=0.4, label="perfect calibration")
        ax.axhline(0.5, color=PALETTE["loss"], linestyle=":", linewidth=0.8, alpha=0.6,
                   label="random (0.5)")
        ax.errorbar(confs, rates, yerr=[los, his], fmt="none",
                    ecolor=PALETTE["neutral"], elinewidth=1, capsize=4, alpha=0.75,
                    label="95% Wilson CI")
        ax.scatter(confs, rates, s=sizes, alpha=0.85, c=PALETTE["primary"],
                   edgecolors=PALETTE["octonary"], linewidths=0.5)
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        for r, c, rate in zip(rows, confs, rates):
            ax.annotate(f"n={r['n']}", (c, rate),
                        textcoords="offset points", xytext=(7, 4), fontsize=8)
    ax.set_xlabel("AI confidence")
    ax.set_ylabel("Actual hit rate (Δ5m in predicted direction)")
    ax.set_title(f"Calibration  ({ts})")
    ax.legend(loc="upper left")
    fig.savefig(path)
    plt.close(fig)


def _plot_similarity_buckets(plt, data: dict, path: str, ts: str) -> None:
    """Hit rate by similarity bucket with Wilson CI error bars."""
    rows = data["similarity_buckets"]
    fig, ax = plt.subplots(figsize=(7, 5))
    if not rows:
        ax.text(0.5, 0.5, "no data yet", ha="center", va="center", transform=ax.transAxes)
    else:
        sims = [r["sim_bucket"] for r in rows]
        rates = [(r["hits"] / r["n"]) if r["n"] else 0.0 for r in rows]
        los, his = [], []
        for r, rate in zip(rows, rates):
            lo, hi = _wilson_ci(int(r["hits"] or 0), int(r["n"] or 0))
            los.append(rate - lo)
            his.append(hi - rate)
        sizes = [max(20, min(400, (r["n"] or 0) * 4)) for r in rows]
        ax.axhline(0.5, color=PALETTE["loss"], linestyle=":", linewidth=0.8,
                   alpha=0.6, label="random (0.5)")
        ax.errorbar(sims, rates, yerr=[los, his], fmt="none",
                    ecolor=PALETTE["neutral"], elinewidth=1, capsize=4, alpha=0.7)
        ax.scatter(sims, rates, s=sizes, alpha=0.8, c="seagreen", edgecolors="black")
        for r, s, rate in zip(rows, sims, rates):
            ax.annotate(f"n={r['n']}", (s, rate),
                        textcoords="offset points", xytext=(6, 4), fontsize=8)
    ax.set_xlabel("Cosine similarity bucket")
    ax.set_ylabel("Hit rate (Δ5m direction)")
    ax.set_ylim(0, 1)
    ax.set_title(f"Hit rate vs similarity (with Wilson CI)  ({ts})")
    ax.legend(loc="lower right")
    fig.savefig(path)
    plt.close(fig)


def _plot_delta_5m_dist(plt, data: dict, path: str, ts: str) -> None:
    """Histogram of Δ5m showing how reactive markets are to news."""
    deltas = data["delta_5m_dist"]
    fig, ax = plt.subplots(figsize=(7, 5))
    if not deltas:
        ax.text(0.5, 0.5, "no data yet", ha="center", va="center", transform=ax.transAxes)
    else:
        # Clip outliers at ±0.20 for a readable histogram
        clipped = [max(-0.20, min(0.20, d)) for d in deltas]
        ax.hist(clipped, bins=40, color=PALETTE["primary"], edgecolor=PALETTE["octonary"], alpha=0.85)
        mean_abs = sum(abs(d) for d in deltas) / len(deltas)
        ax.axvline(0, color=PALETTE["octonary"], linewidth=0.7)
        ax.axvline(0.01, color=PALETTE["gain"], linestyle=":", linewidth=1,
                   label="|Δ| = 0.01 (meaningful move)")
        ax.axvline(-0.01, color=PALETTE["gain"], linestyle=":", linewidth=1)
        ax.set_title(f"Δ5m distribution  N={len(deltas)}  mean|Δ|={mean_abs:.4f}  ({ts})")
    ax.set_xlabel("Δ5m (price change 5 minutes after headline, clipped at ±0.20)")
    ax.set_ylabel("Count")
    ax.legend()
    fig.savefig(path)
    plt.close(fig)


def _plot_edge_vs_pnl(plt, data: dict, path: str, ts: str) -> None:
    """
    Predicted edge vs realized PnL — but the raw scatter from cycle 4 was
    illegible because executed edges cluster at edge_increment × small
    multipliers (every dot landed on the same vertical strip). Re-render as:
      * quantile-binned mean PnL ± 95% CI per bin (heavy line)
      * raw scatter underneath (very faint, for context)
      * OLS fit + Pearson r (since IC is the value we actually want)
      * mean line and zero baselines
    Reads vastly better and surfaces the real question: does *predicted* edge
    monotonically buy us *realized* PnL?
    """
    rows = data["edge_vs_pnl"]
    fig, ax = plt.subplots(figsize=(8.5, 5.5))
    if len(rows) < 3:
        ax.text(0.5, 0.5, "need ≥3 closed trades", ha="center", va="center",
                transform=ax.transAxes, color=PALETTE["neutral"])
        ax.set_title(f"Predicted edge vs realized PnL  ({ts})")
        fig.savefig(path)
        plt.close(fig)
        return

    xs = [float(r["edge"]) for r in rows]
    ys = [float(r["norm_pnl"]) for r in rows]
    n = len(xs)
    ic = _pearson(xs, ys)

    # Raw scatter — faint, for context only.
    ax.scatter(xs, ys, c=PALETTE["primary"], alpha=0.18,
               edgecolors="none", s=20, label="trades")

    # Quantile bins (5 if enough data, 3 if very sparse). Empty bins are
    # silently dropped so a 6-trade chart still renders something useful.
    nbins = 5 if n >= 25 else (4 if n >= 12 else 3)
    sorted_pairs = sorted(zip(xs, ys), key=lambda p: p[0])
    bin_size = max(1, n // nbins)
    bins = [sorted_pairs[i:i + bin_size] for i in range(0, n, bin_size)]
    # Merge a tiny trailing bin into the previous one
    if len(bins) > nbins and len(bins[-1]) < bin_size * 0.6:
        bins[-2].extend(bins[-1]); bins.pop()

    bin_x_mid = []
    bin_y_mean = []
    bin_y_ci_half = []
    bin_n = []
    for b in bins:
        if not b:
            continue
        bxs = [p[0] for p in b]; bys = [p[1] for p in b]
        bn = len(b)
        bmean = sum(bys) / bn
        if bn > 1:
            bvar = sum((y - bmean) ** 2 for y in bys) / (bn - 1)
            bstd = bvar ** 0.5
            ci_half = 1.96 * bstd / (bn ** 0.5)
        else:
            ci_half = 0.0
        bin_x_mid.append(sum(bxs) / bn)
        bin_y_mean.append(bmean)
        bin_y_ci_half.append(ci_half)
        bin_n.append(bn)

    if bin_x_mid:
        ax.errorbar(
            bin_x_mid, bin_y_mean, yerr=bin_y_ci_half,
            fmt="o-", color=PALETTE["secondary"], ecolor=PALETTE["secondary"],
            elinewidth=1.6, capsize=4, markersize=8, linewidth=2.0,
            label=f"binned mean ± 95% CI ({len(bin_x_mid)} bins)", zorder=3,
        )
        for x, y, bn_ in zip(bin_x_mid, bin_y_mean, bin_n):
            ax.annotate(f"n={bn_}", (x, y), textcoords="offset points",
                        xytext=(0, 10), ha="center", fontsize=8,
                        color=PALETTE["neutral"])

    # OLS line over the full range
    mx, my = sum(xs) / n, sum(ys) / n
    denom = sum((x - mx) ** 2 for x in xs)
    if denom > 0:
        slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / denom
        intercept = my - slope * mx
        xfit = [min(xs), max(xs)]
        yfit = [slope * x + intercept for x in xfit]
        ax.plot(xfit, yfit, "-", linewidth=1.4, alpha=0.7,
                color=PALETTE["loss"],
                label=f"OLS: y = {slope:+.2f}x + {intercept:+.3f}")

    ax.axhline(0, color=PALETTE["baseline"], linewidth=0.7)
    ax.axvline(0, color=PALETTE["baseline"], linewidth=0.7)
    mean_y = sum(ys) / n
    ax.axhline(mean_y, color=PALETTE["tertiary"], linewidth=0.8,
               linestyle=":", label=f"mean PnL/stake = {mean_y:+.3f}")

    ax.set_title(f"Predicted edge vs realized PnL  ·  IC={ic:+.3f}  N={n}  ({ts})")
    ax.set_xlabel("Predicted edge  (p_hat − mid_price)")
    ax.set_ylabel("Realized PnL / stake")
    ax.legend(loc="best", framealpha=0.9, fontsize=9)
    fig.savefig(path)
    plt.close(fig)


def _plot_source_breakdown_per_site(plt, data: dict, path: str, ts: str) -> None:
    """
    Per-site (site_url) hit rate with Wilson 95% CIs. Horizontal bar chart so
    long lists of outlets fit on one image without rotated x-labels. Bars are
    coloured by sample size (darker = more samples = trust this rate more).
    """
    rows = data["source_stats_sites"]
    fig, ax = plt.subplots(figsize=(9, max(4, 0.32 * len(rows) + 1.6)))
    if not rows:
        ax.text(0.5, 0.5, "no per-site data yet",
                ha="center", va="center", transform=ax.transAxes,
                color=PALETTE["neutral"])
        ax.set_title(f"Hit rate by site  ({ts})")
        ax.set_xticks([]); ax.set_yticks([])
        fig.savefig(path)
        plt.close(fig)
        return

    # Sort ascending by rate so the worst is at the bottom and best at top —
    # which matches reading order on a horizontal bar chart.
    rows = sorted(rows, key=lambda r: (r["hits"] / r["n"]) if r["n"] else 0)
    sites = [(_short_site(r["site_url"]) or "?") for r in rows]
    rates = [(r["hits"] / r["n"]) if r["n"] else 0.0 for r in rows]
    ns = [r["n"] or 0 for r in rows]
    cis = [_wilson_ci(int(r["hits"] or 0), int(r["n"] or 0)) for r in rows]
    lower_err = [max(0.0, rate - lo) for rate, (lo, _) in zip(rates, cis)]
    upper_err = [max(0.0, hi - rate) for rate, (_, hi) in zip(rates, cis)]

    # Colour-grade by sample size (light → dark)
    max_n = max(ns) or 1
    bar_colors = [_grade_color(n / max_n) for n in ns]

    bars = ax.barh(sites, rates, color=bar_colors, edgecolor="#202428", linewidth=0.5)
    ax.errorbar(
        rates, sites, xerr=[lower_err, upper_err], fmt="none",
        ecolor=PALETTE["neutral"], elinewidth=1.0, capsize=3, alpha=0.8,
    )

    ax.axvline(0.5, color=PALETTE["loss"], linestyle=":", linewidth=0.9,
               alpha=0.7, label="random baseline (0.5)")
    for bar, n_, rate in zip(bars, ns, rates):
        ax.text(min(rate + 0.015, 0.98), bar.get_y() + bar.get_height() / 2,
                f"{rate:.0%}  n={n_}", va="center", fontsize=8,
                color=PALETTE["octonary"])

    ax.set_xlim(0, 1.05)
    ax.set_xlabel("Hit rate (Δ5m in predicted direction)  ±  95% Wilson CI")
    ax.set_title(f"Precision by site — top {len(rows)} by sample size  ({ts})")
    ax.legend(loc="lower right", framealpha=0.9, fontsize=9)
    fig.savefig(path)
    plt.close(fig)


def _short_site(url) -> str:
    if not url:
        return ""
    try:
        from urllib.parse import urlparse
        p = urlparse(url)
        host = (p.netloc or url).replace("www.", "")
        path = (p.path or "").rstrip("/")
        return f"{host}{path}" if path and len(path) < 20 else host
    except Exception:  # noqa: BLE001
        return str(url)[:32]


def _grade_color(intensity: float) -> str:
    """Map intensity [0..1] to a blue→primary gradient hex."""
    intensity = max(0.0, min(1.0, intensity))
    # Linear interp from pale blue to primary blue
    c0 = (0.78, 0.85, 0.92)  # pale
    c1 = (0.00, 0.45, 0.70)  # primary
    rgb = tuple(c0[i] + (c1[i] - c0[i]) * intensity for i in range(3))
    return "#%02x%02x%02x" % tuple(int(255 * x) for x in rgb)


def _plot_sample_size(plt, data: dict, path: str, ts: str) -> None:
    """
    How many closed trades do we need before the sample-mean ROI is "reasonably
    accurate"? Apply the CLT: for an iid sample with std σ, the sample mean
    has std σ/√n, and a normal-approximation 95% CI half-width is 1.96·σ/√n.
    Solving for n at a target margin-of-error gives:

        n_required = (1.96 · σ / MoE)²

    We compute σ from the realized per-trade normalized-PnL series and report:
      * current n, mean ROI ± 95% CI, σ
      * n required for MoE of {0.5%, 1.0%, 2.0%} of stake
      * estimated wall-clock time at the current trade rate

    Left panel: text panel with the numbers (easier than reading a number off
    a chart). Right panel: CI half-width as a function of n on log-x, with
    the current n highlighted.
    """
    stats = data.get("trade_pnl_stats", {})
    n = int(stats.get("n", 0) or 0)
    mean = float(stats.get("mean", 0.0) or 0.0)
    std = float(stats.get("std", 0.0) or 0.0)
    rate = float(stats.get("trades_per_hour", 0.0) or 0.0)

    fig, (axL, axR) = plt.subplots(1, 2, figsize=(12, 5.2),
                                   gridspec_kw={"width_ratios": [1.0, 1.4]})

    # ── Left: numbers panel ────────────────────────────────────────────
    axL.axis("off")
    if n < 2:
        lines = [
            "Sample size analysis",
            "",
            f"Closed trades observed: n = {n}",
            "",
            "Need n ≥ 2 before we can estimate σ.",
            "Come back once 5–10 trades have closed.",
        ]
    else:
        z = 1.96
        ci_half = z * std / (n ** 0.5)

        def n_req(moe: float) -> int:
            if moe <= 0 or std == 0:
                return 0
            return int(math.ceil((z * std / moe) ** 2))

        def hours_for(target_n: int) -> float:
            return target_n / rate if rate > 0 else float("inf")

        targets = [0.005, 0.01, 0.02]
        rows_lines = ["Required n for 95% CI half-width =", ""]
        for t in targets:
            nr = n_req(t)
            extra = max(0, nr - n)
            h = hours_for(extra)
            if h == float("inf"):
                eta = "—"
            elif h < 1:
                eta = f"{h*60:.0f} min"
            elif h < 48:
                eta = f"{h:.1f} h"
            else:
                eta = f"{h/24:.1f} d"
            rows_lines.append(
                f"   ±{t*100:.1f}% of stake → n ≥ {nr:>5}   (need +{extra}, ETA {eta})"
            )

        lines = [
            "Sample size analysis (CLT, 95% CI)",
            "",
            f"Closed trades observed:  n = {n}",
            f"Sample mean ROI:         μ̂ = {mean*100:+.2f}%",
            f"Sample std dev:          σ = {std*100:.2f}%",
            f"Current 95% CI half-width: ±{ci_half*100:.2f}%",
            f"Trade rate:              {rate:.2f} / hour",
            "",
            *rows_lines,
        ]

    axL.text(0.0, 1.0, "\n".join(lines), family="DejaVu Sans Mono",
             fontsize=10, va="top", ha="left", color=PALETTE["octonary"])

    # ── Right: CI half-width vs n curve ────────────────────────────────
    if n >= 2 and std > 0:
        z = 1.96
        ns = [int(round(x)) for x in _logspace(1, max(5000, n * 5), 80)]
        moes = [z * std / (k ** 0.5) for k in ns]
        axR.plot(ns, [m * 100 for m in moes], color=PALETTE["primary"],
                 linewidth=2.0, label="95% CI half-width")
        axR.fill_between(ns, 0, [m * 100 for m in moes], alpha=0.15,
                         color=PALETTE["primary"])
        # Mark current n
        axR.axvline(n, color=PALETTE["secondary"], linestyle="--",
                    linewidth=1.2, label=f"current n = {n}")
        # Target lines
        for t, col in zip([0.005, 0.01, 0.02],
                          [PALETTE["tertiary"], PALETTE["highlight"], PALETTE["loss"]]):
            axR.axhline(t * 100, color=col, linestyle=":",
                        linewidth=1.0, alpha=0.75,
                        label=f"target ±{t*100:.1f}%")
        axR.set_xscale("log")
        axR.set_xlabel("Sample size n (log scale)")
        axR.set_ylabel("95% CI half-width  (% of stake)")
        axR.set_title("Estimator precision vs sample size")
        axR.legend(loc="upper right", framealpha=0.9, fontsize=9)
    else:
        axR.text(0.5, 0.5, "need ≥2 closed trades for σ",
                 ha="center", va="center", transform=axR.transAxes,
                 color=PALETTE["neutral"])
        axR.set_xticks([]); axR.set_yticks([])

    fig.suptitle(f"How many trades until we trust the mean ROI?  ({ts})",
                 fontsize=12, fontweight="semibold")
    fig.savefig(path)
    plt.close(fig)


def _logspace(lo: float, hi: float, n: int) -> list[float]:
    import math
    if lo <= 0:
        lo = 1.0
    if hi <= lo:
        hi = lo * 10
    log_lo = math.log10(lo)
    log_hi = math.log10(hi)
    step = (log_hi - log_lo) / max(1, n - 1)
    return [10 ** (log_lo + i * step) for i in range(n)]


def _plot_close_reasons(plt, data: dict, path: str, ts: str) -> None:
    """
    Pie of close reasons (WIN/LOSS/TP/SL/TRAIL/TIMEOUT/COLD_CULL/UNKNOWN).
    Sourced from `trades.close_reason` populated by the trader on close.
    """
    counts: dict[str, int] = data["close_reasons"]
    fig, ax = plt.subplots(figsize=(6, 6))
    total = sum(counts.values())
    if total == 0:
        ax.text(0.5, 0.5, "no closed trades yet",
                ha="center", va="center", transform=ax.transAxes)
        ax.set_xticks([]); ax.set_yticks([])
    else:
        # Deterministic colour mapping; anything unrecognized falls back to grey.
        color_map = {
            "WIN":       "seagreen",
            "LOSS":      "crimson",
            "TP":        "mediumseagreen",
            "SL":        "indianred",
            "TRAIL":     "goldenrod",
            "TIMEOUT":   "steelblue",
            "COLD_CULL": "slategrey",
            "UNKNOWN":   PALETTE["baseline"],
        }
        # Render in descending-count order so the biggest wedge starts at 0deg.
        ordered = sorted(counts.items(), key=lambda kv: -kv[1])
        labels = [f"{k}  ({v}, {100*v/total:.0f}%)" for k, v in ordered]
        values = [v for _, v in ordered]
        colors = [color_map.get(k, PALETTE["baseline"]) for k, _ in ordered]
        ax.pie(values, labels=labels, colors=colors,
               autopct="", startangle=90, wedgeprops={"edgecolor": "white"})
    ax.set_title(f"Close-reason breakdown  N={total}  ({ts})")
    fig.savefig(path)
    plt.close(fig)


def _plot_confidence_dist(plt, data: dict, path: str, ts: str) -> None:
    """Histogram of AI confidence on non-NEUTRAL calls."""
    confs = data["confidence_dist"]
    fig, ax = plt.subplots(figsize=(7, 5))
    if not confs:
        ax.text(0.5, 0.5, "no data yet", ha="center", va="center", transform=ax.transAxes)
    else:
        ax.hist(confs, bins=20, range=(0.0, 1.0), color=PALETTE["quaternary"],
                edgecolor=PALETTE["octonary"], alpha=0.8)
        avg = sum(confs) / len(confs)
        ax.axvline(avg, color=PALETTE["loss"], linestyle="--", linewidth=1.2,
                   label=f"mean = {avg:.2f}")
        ax.axvline(0.5, color=PALETTE["neutral"], linestyle=":", linewidth=0.8,
                   label="0.5 floor")
    ax.set_xlabel("AI confidence")
    ax.set_ylabel("Count")
    ax.set_title(f"AI confidence distribution  N={len(confs)}  ({ts})")
    ax.legend()
    fig.savefig(path)
    plt.close(fig)


def _plot_pnl_over_time(plt, data: dict, path: str, ts: str) -> None:
    """Cumulative realized PnL per engine over time."""
    rows = data["pnl_over_time"]
    fig, ax = plt.subplots(figsize=(9, 5.5))
    if not rows:
        ax.text(0.5, 0.5, "no closed trades yet", ha="center", va="center",
                transform=ax.transAxes)
    else:
        by_engine: dict[str, dict] = {}
        for r in rows:
            eng = r["engine_name"] or "?"
            try:
                t = datetime.fromisoformat(r["closed_at"])
            except (ValueError, TypeError):
                continue
            by_engine.setdefault(eng, {"ts": [], "cum": []})
            buf = by_engine[eng]
            last = buf["cum"][-1] if buf["cum"] else 0.0
            buf["ts"].append(t)
            buf["cum"].append(last + (r["realized_pnl"] or 0.0))
        ax.axhline(0, color=PALETTE["octonary"], linewidth=0.5)
        for eng, buf in sorted(by_engine.items()):
            ax.plot(buf["ts"], buf["cum"], linewidth=1.5,
                    label=f"{eng}  final={buf['cum'][-1]:+.2f}")
        fig.autofmt_xdate()
    ax.set_xlabel("Close time (UTC)")
    ax.set_ylabel("Cumulative realized PnL ($)")
    ax.set_title(f"Realized PnL over time per engine  ({ts})")
    ax.legend(loc="best")
    fig.savefig(path)
    plt.close(fig)


def _plot_tokens_over_time(plt, data: dict, path: str, ts: str) -> None:
    """Tokens consumed per 15-minute bucket."""
    rows = data["tokens_over_time"]
    fig, ax = plt.subplots(figsize=(9, 4.5))
    if not rows:
        ax.text(0.5, 0.5, "no usage events yet", ha="center", va="center",
                transform=ax.transAxes)
    else:
        ts_vals = []
        ys = []
        for r in rows:
            try:
                ts_vals.append(datetime.fromisoformat(r["bucket_start"]))
                ys.append(int(r["tokens"] or 0))
            except (ValueError, TypeError):
                continue
        if ts_vals:
            ax.bar(ts_vals, ys, width=0.01, color=PALETTE["secondary"], edgecolor=PALETTE["octonary"],
                   linewidth=0.5)
            total = sum(ys)
            ax.set_title(f"AI token usage per 15-min bucket  total={total:,}  ({ts})")
            fig.autofmt_xdate()
        else:
            ax.text(0.5, 0.5, "no data yet", ha="center", va="center",
                    transform=ax.transAxes)
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Tokens (15-min bucket)")
    fig.savefig(path)
    plt.close(fig)


def _wilson_ci(hits: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """95% Wilson CI on a binomial proportion. Local copy to keep plotter self-contained."""
    if n == 0:
        return 0.0, 1.0
    p = hits / n
    denom = 1.0 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    margin = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5) / denom
    return max(0.0, centre - margin), min(1.0, centre + margin)


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
        ax.axhline(0.5, color=PALETTE["loss"], linestyle=":", linewidth=0.8,
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
        ax.axhline(0.5, color=PALETTE["loss"], linestyle=":", linewidth=0.8, alpha=0.6,
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
        ax.axhline(0, color=PALETTE["octonary"], linewidth=0.5)
        ax.axvline(0, color=PALETTE["octonary"], linewidth=0.5)
        ax.set_title(f"Signed confidence vs Δ5m   IC = {ic:+.4f}  (n={len(pairs)})  ({ts})")
    ax.set_xlabel("Signed AI confidence  (+conf=YES, -conf=NO)")
    ax.set_ylabel("Δ5m (price change 5 minutes after headline)")
    fig.savefig(path)
    plt.close(fig)


def _plot_latency_hist(plt, data: dict, path: str, ts: str) -> None:
    """
    Histogram of signal_latency_ms. When the distribution spans several
    orders of magnitude (it routinely does — typical run has p50 ~5s and
    p99 ~300s+) a linear-x histogram collapses everything into one bin, so
    we use log-spaced bins on a log x-axis. The y-axis stays linear; counts
    in the modal bin are still informative.
    """
    latencies = data["latencies"]
    fig, ax = plt.subplots(figsize=(7, 5))
    if not latencies:
        ax.text(0.5, 0.5, "no data yet", ha="center", va="center", transform=ax.transAxes)
    else:
        # Filter zeros/negatives so log binning is safe; clip at p99 to keep
        # the tail readable.
        positive = [max(1.0, x) for x in latencies if x is not None and x > 0]
        positive_sorted = sorted(positive)
        clip = positive_sorted[int(0.99 * len(positive_sorted))]
        # log10 bin edges from the smallest sample to the p99 clip.
        import math
        lo = max(1.0, positive_sorted[0])
        bins = [10 ** x for x in
                _linspace(math.log10(lo), math.log10(max(clip, lo * 10)), 41)]
        clipped = [min(x, clip) for x in positive]
        ax.hist(clipped, bins=bins, color=PALETTE["primary"], edgecolor=PALETTE["octonary"], alpha=0.85)
        avg = sum(positive) / len(positive)
        p50 = positive_sorted[len(positive_sorted) // 2]
        p99 = positive_sorted[int(0.99 * len(positive_sorted))]
        ax.axvline(avg,  color=PALETTE["loss"],       linestyle="--", linewidth=1.2,
                   label=f"mean = {avg:.0f} ms")
        ax.axvline(p50,  color=PALETTE["secondary"],    linestyle="-",  linewidth=1.0,
                   label=f"p50 = {p50:.0f} ms")
        ax.axvline(p99,  color=PALETTE["loss"],   linestyle=":",  linewidth=1.0,
                   label=f"p99 = {p99:.0f} ms")
        ax.axvline(5000, color=PALETTE["gain"], linestyle=":",  linewidth=1.2,
                   label="5s target")
        ax.set_xscale("log")
    ax.set_xlabel("Signal latency (ms, log scale, clipped at p99)")
    ax.set_ylabel("Count")
    ax.set_title(f"Signal latency distribution  ({ts})")
    ax.legend()
    fig.savefig(path)
    plt.close(fig)


def _linspace(start: float, stop: float, n: int) -> list[float]:
    """Tiny linspace stand-in to avoid pulling numpy here."""
    if n <= 1:
        return [start]
    step = (stop - start) / (n - 1)
    return [start + step * i for i in range(n)]


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
            ax.plot(times, rates, color=PALETTE["primary"], linewidth=1.5)
            ax.axhline(0.5, color=PALETTE["loss"], linestyle=":", linewidth=0.8,
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


# ---------------------------------------------------------------------------
# Trade-example charts (price history with event lines + headline text)
# ---------------------------------------------------------------------------

_BUCKET_COLORS = {
    "too_slow":  "#d97706",  # amber — we were late to a real signal
    "captured":  "#16a34a",  # green — caught alpha
    "no_signal": "#6b7280",  # gray — flat
    "wrong_dir": "#dc2626",  # red — picked the wrong side
}


def _iso_to_unix(iso: str | None) -> int | None:
    if not iso:
        return None
    try:
        # SQLite mixes "T" and " " separators; isoformat handles both via fromisoformat
        from datetime import datetime as _dt, timezone as _tz
        dt = _dt.fromisoformat(iso.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_tz.utc)
        return int(dt.timestamp())
    except Exception:
        return None


def _wrap_text(text: str, width: int = 90, max_lines: int = 4) -> str:
    import textwrap
    if not text:
        return ""
    wrapped = textwrap.wrap(text, width=width)
    if len(wrapped) > max_lines:
        wrapped = wrapped[:max_lines]
        wrapped[-1] = wrapped[-1][: max(0, width - 1)] + "…"
    return "\n".join(wrapped)


def _render_trade_examples(examples: list[dict], out_dir: str) -> None:
    """Render one PNG per example. Writes to out_dir/<bucket>_<id>.png."""
    os.environ.setdefault("MPLBACKEND", "Agg")
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt  # noqa: E402

    Path(out_dir).mkdir(parents=True, exist_ok=True)

    plt.rcParams["figure.dpi"] = 110
    plt.rcParams["savefig.bbox"] = "tight"

    for row in examples:
        try:
            _plot_one_trade_example(plt, row, out_dir)
        except Exception as exc:
            logger.warning(
                "[plotter] trade-example id=%s failed: %s", row.get("id"), exc,
            )


def _plot_one_trade_example(plt, row: dict, out_dir: str) -> None:
    series = row.get("_series") or []
    if len(series) < 2:
        return

    bucket = row.get("_bucket") or "no_signal"
    color  = _BUCKET_COLORS.get(bucket, "#374151")

    # Anchor: impact detection time. price_timeseries.relative_s is already
    # signed seconds from this anchor, so we plot directly in those units
    # (minutes for readability).
    detected_unix = _iso_to_unix(row.get("detected_at")) or 0
    xs = [s["relative_s"] / 60.0 for s in series]   # minutes from impact-track time
    ys = [s["price"] for s in series]

    fig, ax = plt.subplots(figsize=(11, 6.2))
    ax.plot(xs, ys, color=color, linewidth=1.8, marker="o", markersize=2.2,
            alpha=0.9, label=f"market price ({bucket})")

    # Event lines: we_saw (signals.detected_at), we_tracked (relative_s=0),
    # we_acted (trades.decided_at), we_exited (trades.closed_at).
    def _add_vline(t_unix: int | None, label: str, style: str, c: str):
        if t_unix is None or detected_unix == 0:
            return
        x_min = (t_unix - detected_unix) / 60.0
        if x_min < min(xs) - 5 or x_min > max(xs) + 5:
            return  # off-axis — don't clutter
        ax.axvline(x_min, color=c, linestyle=style, linewidth=1.4, alpha=0.85)
        ax.text(
            x_min, ax.get_ylim()[1] if False else max(ys),
            f"  {label}", color=c, fontsize=8, rotation=90,
            verticalalignment="top", alpha=0.95,
        )

    seen_unix  = _iso_to_unix(row.get("seen_at"))
    acted_unix = _iso_to_unix(row.get("trade_decided_at"))
    exit_unix  = _iso_to_unix(row.get("trade_closed_at"))

    _add_vline(seen_unix,    "saw headline",                    "--", "#2563eb")
    # impact-tracker reference (relative_s == 0)
    ax.axvline(0, color="#000000", linestyle=":", linewidth=1.2, alpha=0.55)
    ax.text(0, max(ys), "  tracked", color="#000000", fontsize=8,
            rotation=90, verticalalignment="top", alpha=0.7)

    direction = row.get("trade_direction") or row.get("direction") or "?"
    size = row.get("trade_size_usdc")
    acted_label = f"bought {direction}"
    if size:
        acted_label += f" ${size:.0f}"
    _add_vline(acted_unix, acted_label, "--", "#16a34a")

    pnl = row.get("trade_realized_pnl")
    exit_label = "closed"
    if pnl is not None:
        exit_label += f" pnl={pnl:+.2f}"
    _add_vline(exit_unix, exit_label, "--", "#dc2626")

    # Pre/post delta annotations in a small text box
    dpre = row.get("delta_pre_60s")
    d5   = row.get("delta_t5m")
    info = [
        f"bucket: {bucket}",
        f"direction (AI): {row.get('direction','?')}  conf={row.get('ai_confidence', 0):.2f}  sim={row.get('similarity', 0):.2f}",
        f"price_t0={row.get('price_t0', 0):.3f}"
        + (f"   Δpre60s={dpre:+.4f}" if dpre is not None else "")
        + (f"   Δt5m={d5:+.4f}"   if d5   is not None else ""),
    ]
    ax.text(
        0.01, 0.98, "\n".join(info),
        transform=ax.transAxes, fontsize=8.5,
        verticalalignment="top", family="monospace",
        bbox=dict(boxstyle="round,pad=0.4", facecolor="white",
                  edgecolor="#cccccc", alpha=0.92),
    )

    # Title: market question (one line)
    question = (row.get("question") or "").strip()
    ax.set_title(f"{question}", fontsize=11, loc="left")

    ax.set_xlabel("Minutes from impact tracking (t=0)")
    ax.set_ylabel("Token price (probability)")
    ax.set_ylim(max(0.0, min(ys) - 0.02), min(1.0, max(ys) + 0.04))
    ax.grid(True, alpha=0.3)
    ax.legend(loc="lower right", fontsize=8)

    # Headline + source + (optional) first-paragraph body below the chart.
    headline = (row.get("headline") or "").strip()
    source   = (row.get("site_url") or "").strip()
    body     = (row.get("_article_body") or "").strip()
    footer   = f"HEADLINE: {headline}  —  {source}" if source else f"HEADLINE: {headline}"
    footer   = _wrap_text(footer, width=120, max_lines=2)
    if body:
        footer += "\n" + _wrap_text(body, width=120, max_lines=4)

    fig.text(0.02, -0.02, footer, fontsize=8.5, family="sans-serif",
             color="#1f2937", verticalalignment="top")

    fname = f"{bucket}_{row['id']:06d}.png"
    fig.savefig(os.path.join(out_dir, fname))
    plt.close(fig)

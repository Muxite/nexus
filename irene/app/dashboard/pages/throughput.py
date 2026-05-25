"""Per-stage throughput in the last hour: rows/min for each pipeline
stage AND per-stage latency percentiles. Two sections:

  COUNTS  — rows/h and rows/min per stage table (existing).
  TIMINGS — per-stage latency p1/p50/mean/p95/p99 in milliseconds,
            computed over the same 1h window using
            db_helpers.latency_panel_sqls.

The TIMINGS panel mirrors what `latency.txt` exports but with extra
columns (p1, mean) so an operator can see top-1% and bottom-1% tails
alongside the median.
"""

from __future__ import annotations

from ..db_helpers import fetch_ms, latency_panel_sqls, scalar
from ..io import _now, _percentiles

FILENAME = "throughput.txt"


def _extra_stats(samples: list[float]) -> dict[str, float]:
    """Add p1 + mean on top of the standard p50/p95/p99 set. Pure
    function so it doesn't need a DB. Empty input → all zeros."""
    if not samples:
        return {"p1": 0.0, "mean": 0.0}
    s = sorted(samples)
    idx = max(0, min(len(s) - 1, int(round((len(s) - 1) * 0.01))))
    p1 = s[idx]
    return {"p1": p1, "mean": sum(s) / len(s)}


def _clt_stats(samples: list[float]) -> dict[str, float]:
    """CLT-based summary for the sample mean.

    Premise: per-event processing time at each pipeline stage is
    approximately IID. The events themselves are NOT iid (news clusters,
    bursts), but the *time* a stage takes to process each one is driven
    by CPU/I-O work whose distribution is stable.

    Under LLN, the sample mean converges to the true mean E[X] as n→∞.
    Under CLT, for large n, the sample mean is approximately Normal
    with mean μ and standard deviation σ/√n (the standard error of
    the mean — `sem`). A 95% confidence interval is `mean ± 1.96·sem`.

    Required-n for accuracy ε (in ms) of the mean:
        n ≥ (1.96 · σ / ε)²
    We report n for ε = 10% of the observed mean and ε = 1% of mean —
    the first answers "how many samples until the mean is reliable",
    the second answers "how many until it's tight".
    """
    n = len(samples)
    if n < 2:
        return {
            "n": n, "mean": samples[0] if samples else 0.0,
            "sigma": 0.0, "sem": 0.0, "ci95": 0.0,
            "n_10pct": 0, "n_1pct": 0,
        }
    mean = sum(samples) / n
    var = sum((x - mean) ** 2 for x in samples) / (n - 1)  # Bessel-corrected
    sigma = var ** 0.5
    sem = sigma / (n ** 0.5)
    ci95 = 1.96 * sem
    # Required-n grows quadratically as the target tightens. Cap the
    # display at 10M so a degenerate (sigma=0 or mean=0) doesn't show
    # an "infinity" value.
    def _required(eps_frac: float) -> int:
        if mean <= 0.0 or sigma <= 0.0:
            return 0
        eps = eps_frac * mean
        return min(int((1.96 * sigma / eps) ** 2) + 1, 10_000_000)

    return {
        "n": n, "mean": mean, "sigma": sigma, "sem": sem, "ci95": ci95,
        "n_10pct": _required(0.10),
        "n_1pct": _required(0.01),
    }


async def render(dash) -> str:
    # Cycle 4: wrap timestamp cols with datetime() so the comparison
    # parses both sides instead of doing a lex match (see main.py).
    # Add cycle filter for tables that have cycle_id (per-cycle rows);
    # leave clusters/entity_resolutions unscoped (shared across cycles).
    cycle_id = dash.current_cycle_id
    cs = f" AND cycle_id = {int(cycle_id)}" if cycle_id is not None else ""
    stages = [
        ("ingest  (articles)        ",
         f"SELECT COUNT(*) FROM articles WHERE datetime(ingested_at) >= datetime('now','-1 hour'){cs}"),
        ("dedupe  (clusters)        ",
         "SELECT COUNT(*) FROM article_clusters WHERE datetime(created_at) >= datetime('now','-1 hour')"),
        ("match   (signals)         ",
         f"SELECT COUNT(*) FROM signals WHERE datetime(created_at) >= datetime('now','-1 hour'){cs}"),
        ("extract (runs)            ",
         f"SELECT COUNT(*) FROM extraction_runs WHERE datetime(created_at) >= datetime('now','-1 hour'){cs}"),
        ("resolve (entity_resolutions)",
         "SELECT COUNT(*) FROM entity_resolutions WHERE datetime(created_at) >= datetime('now','-1 hour')"),
        ("decide  (decisions)       ",
         f"SELECT COUNT(*) FROM decisions WHERE datetime(created_at) >= datetime('now','-1 hour'){cs}"),
        ("trade   (trades)          ",
         f"SELECT COUNT(*) FROM trades WHERE datetime(decided_at) >= datetime('now','-1 hour'){cs}"),
        ("close   (trade_closures)  ",
         f"SELECT COUNT(*) FROM trade_closures WHERE datetime(closed_at) >= datetime('now','-1 hour'){cs}"),
    ]
    lines = [
        f"# irene throughput ({_now().isoformat()})",
        f"  cycle_id={cycle_id}; rows/min per stage and per-stage latency (last 1h)",
        "",
        "── COUNTS ──",
        f"  {'STAGE':<32} {'/HR':>6} {'/MIN':>6}",
        "  " + "─" * 50,
    ]
    for label, sql in stages:
        n = await scalar(dash.db, sql)
        per_min = n / 60.0
        lines.append(f"  {label:<32} {n:>6d} {per_min:>6.2f}")

    # ── TIMINGS ────────────────────────────────────────────────────────
    # Reuses the latency_panel_sqls helper. Same five adjacent-stage
    # diffs as latency.txt: ingest→dedupe, ingest→match, ingest→trade,
    # match→trade (gate cascade), news-age-at-trade.
    #
    # Cache the per-stage samples here so the TIMINGS table and the
    # CLT panel below share one query per stage (avoids 2× DB load).
    timings: list[tuple[str, list[float]]] = []
    for label, sql in latency_panel_sqls(cycle_id=cycle_id):
        timings.append((label, await fetch_ms(dash.db, sql)))

    lines += [
        "",
        "── TIMINGS (ms; lower is faster) ──",
        f"  {'STAGE':<28} {'p1':>7} {'p50':>7} {'mean':>7} {'p95':>7} {'p99':>7} {'n':>5}",
        "  " + "─" * 70,
    ]
    for label, samples in timings:
        p = _percentiles(samples)
        extra = _extra_stats(samples)
        lines.append(
            f"  {label:<28} "
            f"{extra['p1']:>7.0f} "
            f"{p['p50']:>7.0f} "
            f"{extra['mean']:>7.0f} "
            f"{p['p95']:>7.0f} "
            f"{p['p99']:>7.0f} "
            f"{p['count']:>5d}"
        )

    # ── CLT-BASED CONFIDENCE / REQUIRED-N (ms) ────────────────────────
    # Per-event processing time at each stage is approximately IID
    # even though the events themselves arrive in bursts. Under LLN
    # the sample mean → true mean as n grows; under CLT it's
    # ~Normal(μ, σ/√n) for large n.
    #
    # Columns:
    #   n        — sample size in the 1h window
    #   σ        — sample std dev (individual-observation spread)
    #   SE       — σ/√n (uncertainty about the mean)
    #   95% CI±  — half-width of the 95% confidence interval for the mean
    #   n→±10%   — samples needed so 95% CI half-width is 10% of mean
    #   n→±1%    — same but 1%
    #
    # Caveat: news-age-at-trade is NOT IID (depends on news age, which
    # is itself non-stationary). Treat its CLT row as diagnostic only;
    # the others (ingest→dedupe, ingest→match, ingest→trade,
    # match→trade) reflect bounded internal work and should converge.
    lines += [
        "",
        "── CLT STATS (per-stage processing time, ms) ──",
        f"  {'STAGE':<28} {'n':>5} {'σ':>7} {'SE':>7} {'95% CI±':>9} {'n→±10%':>8} {'n→±1%':>9}",
        "  " + "─" * 78,
    ]
    for label, samples in timings:
        clt = _clt_stats(samples)
        lines.append(
            f"  {label:<28} "
            f"{int(clt['n']):>5d} "
            f"{clt['sigma']:>7.0f} "
            f"{clt['sem']:>7.1f} "
            f"{clt['ci95']:>9.1f} "
            f"{int(clt['n_10pct']):>8d} "
            f"{int(clt['n_1pct']):>9d}"
        )
    return "\n".join(lines) + "\n"

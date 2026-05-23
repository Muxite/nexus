"""counterfactual-report — per-filter marginal EV from cf-sampled trades.

A4E.5 readout: for each named filter that the trader bypassed (1% of
would-have-been-skipped signals), compare the cf trades' closed PnL
distribution against normal trades over the same window.

  marginal_ev = mean(pnl_cf) - mean(pnl_normal)

Positive marginal EV: the filter is HURTING — it was skipping
profitable trades. Negative marginal EV: the filter is HELPING — cf
trades are losers, confirming the skip was correct. The 95% CI
(via the per-trade pnl std-error) tells you whether the signal is
above noise.

Recommend column:
  - tighten  : cf trades lose hard (marginal EV ≪ 0, CI excludes 0)
  - keep     : noise-level marginal EV (CI straddles 0)
  - loosen   : cf trades win meaningfully (marginal EV > 0, CI > 0)
"""
from __future__ import annotations

import math

from scripts.analysis._common import add_common_args, parse_since
from scripts.analysis._format import Section


def add_parser(p) -> None:
    add_common_args(p)
    p.set_defaults(since="30d")


def _stats(rows: list[float]) -> tuple[int, float, float]:
    """Return (n, mean, stderr) for a list of pnl values."""
    n = len(rows)
    if n == 0:
        return 0, 0.0, 0.0
    mean = sum(rows) / n
    if n < 2:
        return n, mean, 0.0
    var = sum((x - mean) ** 2 for x in rows) / (n - 1)
    return n, mean, math.sqrt(var / n)


def _recommend(marginal: float, ci_lo: float, ci_hi: float, n_cf: int) -> str:
    if n_cf < 5:
        return "wait"  # too few cf trades to call
    if ci_lo > 0:
        return "loosen"
    if ci_hi < 0:
        return "tighten"
    return "keep"


def run(args, conn) -> Section:
    since = parse_since(getattr(args, "since", None) or "30d")

    # 1) Closed cf trades, per-filter pnl rolls.
    cf_sql = """
        SELECT cf_bypassed_filter AS f, realized_pnl AS pnl
          FROM trades
         WHERE cf_bypassed_filter IS NOT NULL
           AND size_usdc > 0
           AND skip_reason IS NULL
           AND realized_pnl IS NOT NULL
    """
    cf_params: list = []
    if since:
        cf_sql += " AND decided_at >= ?"
        cf_params.append(since)
    cf_by_filter: dict[str, list[float]] = {}
    for row in conn.execute(cf_sql, cf_params).fetchall():
        cf_by_filter.setdefault(row["f"], []).append(float(row["pnl"]))

    # 2) Normal closed trades (cf_bypassed_filter IS NULL) — single pool
    #    used as the "would have traded under all filters" baseline.
    normal_sql = """
        SELECT realized_pnl AS pnl
          FROM trades
         WHERE cf_bypassed_filter IS NULL
           AND size_usdc > 0
           AND skip_reason IS NULL
           AND realized_pnl IS NOT NULL
    """
    normal_params: list = []
    if since:
        normal_sql += " AND decided_at >= ?"
        normal_params.append(since)
    normal_pnls = [
        float(r["pnl"])
        for r in conn.execute(normal_sql, normal_params).fetchall()
    ]
    n_norm, mean_norm, se_norm = _stats(normal_pnls)

    # 3) Per-filter row.
    rows: list[list] = []
    all_filters = sorted(cf_by_filter.keys())
    for f in all_filters:
        pnls = cf_by_filter[f]
        n_cf, mean_cf, se_cf = _stats(pnls)
        marginal = mean_cf - mean_norm
        # SE of the difference assumes the two pools are independent;
        # fine because cf trades are by construction the would-have-been-
        # skipped sample, disjoint from the normal-pool.
        se_diff = math.sqrt(se_cf * se_cf + se_norm * se_norm)
        ci_lo = marginal - 1.96 * se_diff
        ci_hi = marginal + 1.96 * se_diff
        rec = _recommend(marginal, ci_lo, ci_hi, n_cf)
        rows.append([
            f, n_norm, n_cf,
            round(sum(normal_pnls), 2), round(sum(pnls), 2),
            round(mean_norm, 4), round(mean_cf, 4),
            round(marginal, 4),
            f"[{ci_lo:+.3f}, {ci_hi:+.3f}]",
            rec,
        ])

    summary = (
        f"per-filter marginal EV — {len(all_filters)} filters with cf trades "
        f"vs {n_norm} normal trades since {since or 'beginning'}"
    )
    notes = [
        "marginal_ev = mean(pnl_cf) − mean(pnl_normal). "
        "> 0 ⇒ filter is HURTING (loosen); < 0 with CI excluding 0 ⇒ helping (tighten).",
        "wait = n_cf < 5 (insufficient cf sample); keep = CI straddles 0.",
    ]
    return Section(
        title="counterfactual marginal-EV per filter (A4E.5)",
        headers=[
            "filter", "n_normal", "n_cf",
            "sum_pnl_normal", "sum_pnl_cf",
            "avg_pnl_normal", "avg_pnl_cf",
            "marginal_ev", "ci95", "recommend",
        ],
        rows=rows,
        summary=summary,
        notes=notes,
    )


if __name__ == "__main__":
    import argparse
    from scripts.analysis._common import open_db
    from scripts.analysis._format import render
    p = argparse.ArgumentParser(prog="counterfactual_report")
    add_parser(p)
    args = p.parse_args()
    conn = open_db(getattr(args, "db", None))
    try:
        sec = run(args, conn)
    finally:
        conn.close()
    render(sec, fmt=getattr(args, "format", "text"))

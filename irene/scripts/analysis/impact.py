"""impact — realised-edge summary from the price_impacts table.

Per `docs/philosophy.md`, the impact tracker is how we verify the speed
thesis: does the price actually move our way after we trade? This
subcommand summarises that in a single table.
"""

from __future__ import annotations

from scripts.analysis._common import add_common_args, parse_since
from scripts.analysis._format import Section


def add_parser(p) -> None:
    add_common_args(p)


def _avg(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _hit_rate(values: list[float]) -> float | None:
    """Fraction of values > 0 (the price moved our way)."""
    if not values:
        return None
    return sum(1 for v in values if v > 0) / len(values)


def run(args, conn) -> Section:
    cutoff = parse_since(args.since) or "1970-01-01T00:00:00"
    rows_db = conn.execute(
        """
        SELECT direction,
               edge_realised_t60s, edge_realised_t300s, edge_realised_t900s,
               edge_realised_t1800s, edge_realised_t3600s
        FROM price_impacts
        WHERE created_at >= ?
        """,
        (cutoff,),
    ).fetchall()
    n_total = len(rows_db)

    out_rows = []
    for label, key in (
        ("+1m  (t60s)", "edge_realised_t60s"),
        ("+5m  (t300s)", "edge_realised_t300s"),
        ("+15m (t900s)", "edge_realised_t900s"),
        ("+30m (t1800s)", "edge_realised_t1800s"),
        ("+1h  (t3600s)", "edge_realised_t3600s"),
    ):
        values = [r[key] for r in rows_db if r[key] is not None]
        out_rows.append([
            label,
            len(values),
            round(_avg(values) or 0.0, 4),
            round((_hit_rate(values) or 0.0) * 100, 1),
        ])
    return Section(
        title=f"realised-edge by horizon (since {cutoff})",
        headers=["bucket", "n_filled", "avg edge_realised", "hit-rate %"],
        rows=out_rows,
        notes=[
            f"total impact rows in window: {n_total}",
            "edge_realised > 0 means the price moved the way we bet. "
            "Sign convention: YES → price-rise positive; NO → price-fall positive.",
            "This IS the speed-thesis check. If hit-rate at +5m is ~50%, "
            "we have no edge — we're just noise-trading.",
        ],
    )

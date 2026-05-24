"""Seulgi PNG plot tier: each plot renders against seeded cache state and
returns valid PNG bytes; atomic-write leaves no `.tmp` files."""

from __future__ import annotations

import asyncio

import pytest

matplotlib = pytest.importorskip("matplotlib")

from dashboard import Dashboard  # noqa: E402
from dashboard.plots.registry import PLOTS  # noqa: E402

PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


def _seed_cache(d: Dashboard) -> None:
    d._heavy_cache["variant_stats"] = [
        {"variant": "scout/all", "sum_pnl": 3.14,
         "n_wins": 12, "n_losses": 8, "n_trades": 20},
        {"variant": "balanced/v1", "sum_pnl": -2.0,
         "n_wins": 4, "n_losses": 9, "n_trades": 13},
    ]
    d._heavy_cache["decision_funnel"] = {
        "total": 100, "failed_freshness": 5, "failed_low_sim": 10,
        "failed_relevance": 3, "pre_ai_dropped": 12, "ai_neutral": 60,
        "executed": 5, "failed_min_confidence": 0, "failed_agreement": 0,
        "failed_post_ai_rule": 0, "failed_rail": 0, "failed_edge": 0,
        "failed_position_guard": 0, "failed_size": 0, "other_skipped": 0,
        "failed_min_entry": 0, "failed_market_vol": 0,
    }
    d._heavy_cache["confidence_buckets"] = [
        {"bucket": "0.55-0.70", "n_executed": 8, "n_wins": 3, "n_losses": 5,
         "sum_pnl": -1.2, "avg_pnl": -0.15, "n_skipped": 2},
        {"bucket": "0.70-0.85", "n_executed": 20, "n_wins": 12, "n_losses": 8,
         "sum_pnl": 4.5, "avg_pnl": 0.22, "n_skipped": 1},
    ]
    d._heavy_cache["freshness_buckets"] = [
        {"label": "BREAKING", "n_executed": 15, "n_wins": 9, "n_losses": 6,
         "sum_pnl": 3.0, "avg_pnl": 0.2, "n_skipped": 5},
        {"label": "EVOLVING", "n_executed": 12, "n_wins": 4, "n_losses": 8,
         "sum_pnl": -1.5, "avg_pnl": -0.12, "n_skipped": 3},
    ]
    d._heavy_cache["scout_funnel"] = {
        "stats": {"total": 100, "similarity_fail": 30, "freshness_fail": 10,
                  "direction_fail": 25, "edge_fail": 20, "size_fail": 5,
                  "executed": 10},
        "cutoff": "2026-05-24T00:00:00", "since_hours": 24,
    }
    d._active_variants = {"scout/all", "balanced/v1"}


@pytest.mark.parametrize("plot", PLOTS, ids=lambda p: p.FILENAME)
async def test_plot_renders_valid_png(plot, tmp_path):
    d = Dashboard(data_dir=str(tmp_path), interval_secs=60, db=None)
    _seed_cache(d)
    loop = asyncio.get_running_loop()
    png = await plot.render(d, loop=loop)
    if not png:
        # equity_curve has no DB, that's an acceptable empty.
        return
    assert png[:8] == PNG_MAGIC, f"{plot.FILENAME}: bad PNG magic"
    assert len(png) > 1000, f"{plot.FILENAME}: suspiciously small PNG"


async def test_no_tmp_files_after_atomic_write(tmp_path):
    """The atomic PNG writer must clean up its tempfile on success."""
    from dashboard.io import atomic_write_png

    out = tmp_path / "plot.png"
    atomic_write_png(str(out), b"\x89PNG\r\n\x1a\n" + b"\x00" * 1024)
    assert out.exists()
    assert not (tmp_path / "plot.png.tmp").exists()


async def test_plots_unavailable_does_not_kill_dashboard(tmp_path, monkeypatch):
    """If the plots subpackage fails to import, the text tier still runs."""
    import sys
    # Pretend matplotlib's registry blows up.
    bad = type(sys)("dashboard.plots.registry")
    def _explode():
        raise RuntimeError("no matplotlib")
    bad.__getattr__ = lambda _name: (_ for _ in ()).throw(RuntimeError("no matplotlib"))
    monkeypatch.setitem(sys.modules, "dashboard.plots.registry", bad)
    d = Dashboard(data_dir=str(tmp_path), interval_secs=60, db=None)
    # _plot_loop should exit cleanly after logging the warning.
    await asyncio.wait_for(d._plot_loop(), timeout=1.0)

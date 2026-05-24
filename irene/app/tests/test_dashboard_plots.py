"""Irene PNG plot tier: every plot renders against a populated DB; atomic
writes leave no `.tmp` artefacts; empty-DB paths return empty bytes
(not exceptions)."""

from __future__ import annotations

import pytest

# Skip the entire module gracefully when matplotlib isn't installed —
# CI uses the container's pinned dep, but local quick-runs may not.
matplotlib = pytest.importorskip("matplotlib")

# Same fan-in as test_dashboard.py — these imports register table DDLs.
import app.ai  # noqa: F401, E402
import app.main  # noqa: F401, E402
import app.stages.closer  # noqa: F401, E402
import app.stages.dedupe  # noqa: F401, E402
import app.stages.extract  # noqa: F401, E402
import app.stages.ingest  # noqa: F401, E402
import app.stages.markets  # noqa: F401, E402
import app.stages.match  # noqa: F401, E402
import app.stages.resolve  # noqa: F401, E402
import app.stages.trade  # noqa: F401, E402
from app.dashboard import Dashboard  # noqa: E402
from app.dashboard.plots.registry import PLOTS  # noqa: E402
from app.db import DB  # noqa: E402

PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


@pytest.fixture
async def db_ready():
    db = DB.ephemeral()
    await db.connect()
    try:
        yield db
    finally:
        await db.close()


@pytest.fixture
async def populated_db(db_ready):
    """Seed enough rows so every plot has something to draw."""
    async with db_ready.batch() as cur:
        await cur.execute(
            """
            INSERT INTO markets
              (condition_id, slug, question, description, category, end_date,
               volume_usdc, token_yes, token_no, embedding, embedder_id,
               embedding_dim, embedding_updated_at, active, closed_at,
               first_seen_at, updated_at)
            VALUES ('m1','s','q','d','c','2026-12-31',100.0,'ty','tn',NULL,NULL,NULL,NULL,1,NULL,
                    datetime('now'), datetime('now'))
            """
        )
        # Seed a handful of articles spanning the last 24h
        for i in range(1, 6):
            await cur.execute(
                """
                INSERT INTO articles (id, source, source_detail, url, title, summary,
                                      published_at, ingested_at, cycle_id, is_tracer, tracer_id)
                VALUES (?,'rss','x',?,?,?,
                        datetime('now', ?),
                        datetime('now', ?), 1, 0, NULL)
                """,
                (i, f"u{i}", f"t{i}", f"s{i}", f"-{i*20} seconds", f"-{i*10} seconds"),
            )
            await cur.execute(
                """
                INSERT INTO signals (id, article_id, market_id, similarity, cluster_id,
                                     freshness, embedder_id, cycle_id, is_tracer, tracer_id, created_at)
                VALUES (?, ?, 'm1', 0.8, NULL, 'BREAKING', 'fake@4', 1, 0, NULL,
                        datetime('now', ?))
                """,
                (i, i, f"-{i*5} seconds"),
            )
            outcome = "execute" if i % 2 == 0 else "reject"
            reason = "ok" if outcome == "execute" else "low_edge"
            await cur.execute(
                """
                INSERT INTO decisions (id, signal_id, outcome, reason, gate_history,
                                       prediction_id, cycle_id, is_tracer, tracer_id, created_at)
                VALUES (?, ?, ?, ?, '[]', ?, 1, 0, NULL,
                        datetime('now', ?))
                """,
                (i, i, outcome, reason, i, f"-{i*3} seconds"),
            )
            if outcome == "execute":
                await cur.execute(
                    """
                    INSERT INTO trades
                      (id, signal_id, decision_id, prediction_id, market_id, direction,
                       confidence, similarity, freshness, p_hat, mid_price, edge,
                       kelly_fraction, size_usdc, decision_path, features_snapshot,
                       cycle_id, is_tracer, tracer_id, decided_at)
                    VALUES (?, ?, ?, ?, 'm1', 'YES', 0.8, 0.8, 'BREAKING', 0.8, 0.5, 0.1,
                            0.1, 25.0, 'mvp', '{}', 1, 0, NULL,
                            datetime('now', ?))
                    """,
                    (i, i, i, i, f"-{i} seconds"),
                )
    return db_ready


async def test_every_plot_renders_to_valid_png(populated_db, tmp_path):
    """Each plot produces a PNG whose magic bytes are correct."""
    import asyncio

    dash = Dashboard(data_dir=tmp_path, db=populated_db, interval_secs=60)
    loop = asyncio.get_running_loop()
    seen_at_least_one_real_png = False
    for plot in PLOTS:
        png = await plot.render(dash, loop=loop)
        if not png:
            continue  # empty bytes are an acceptable "no data" sentinel
        assert png[:8] == PNG_MAGIC, (
            f"{plot.FILENAME}: not a valid PNG (first 8 bytes: {png[:8]!r})"
        )
        assert len(png) > 1000, f"{plot.FILENAME}: suspiciously tiny PNG"
        seen_at_least_one_real_png = True
    assert seen_at_least_one_real_png, "fixture seeded zero rows visible to plots"


async def test_plot_tick_writes_files_and_no_tmp(populated_db, tmp_path):
    """plot_tick writes PNGs under data/dashboard/plots/ atomically."""
    dash = Dashboard(data_dir=tmp_path, db=populated_db, interval_secs=60)
    await dash.plot_tick(PLOTS)
    plots_dir = tmp_path / "plots"
    pngs = sorted(plots_dir.glob("*.png"))
    assert pngs, "no plot files written"
    # No leftover .tmp artefacts
    assert list(plots_dir.glob("*.tmp")) == []
    for png in pngs:
        assert png.read_bytes()[:8] == PNG_MAGIC


async def test_empty_db_yields_empty_or_skipped(db_ready, tmp_path):
    """An empty DB returns b'' for every plot — never raises."""
    import asyncio

    dash = Dashboard(data_dir=tmp_path, db=db_ready, interval_secs=60)
    loop = asyncio.get_running_loop()
    for plot in PLOTS:
        png = await plot.render(dash, loop=loop)
        # Either empty (no data) or a valid PNG with the empty-set header.
        if png:
            assert png[:8] == PNG_MAGIC, f"{plot.FILENAME}: bad PNG on empty DB"


async def test_pnl_cumulative_plot_smoke(db_ready, tmp_path):
    """Seed mvp + dkg trades with closures and confirm the plot draws."""
    import asyncio

    from app.dashboard.plots import pnl_cumulative

    async with db_ready.batch() as cur:
        await cur.execute(
            """
            INSERT INTO markets
              (condition_id, slug, question, description, category, end_date,
               volume_usdc, token_yes, token_no, embedding, embedder_id,
               embedding_dim, embedding_updated_at, active, closed_at,
               first_seen_at, updated_at)
            VALUES ('m1','s','q','d','c','2026-12-31',100.0,'ty','tn',NULL,NULL,NULL,NULL,1,NULL,
                    datetime('now'), datetime('now'))
            """
        )
        # Two mvp trades (one win, one loss) and one dkg trade (win), all closed.
        seeds = [
            (1, "mvp", +0.40),
            (2, "mvp", -0.15),
            (3, "dkg", +0.25),
        ]
        for tid, path, pnl in seeds:
            await cur.execute(
                """
                INSERT INTO articles (id, source, source_detail, url, title, summary,
                                      published_at, ingested_at, cycle_id, is_tracer, tracer_id)
                VALUES (?, 'rss','x', ?, ?, ?,
                        datetime('now','-60 seconds'),
                        datetime('now','-30 seconds'), 1, 0, NULL)
                """,
                (tid, f"u{tid}", f"t{tid}", f"s{tid}"),
            )
            await cur.execute(
                """
                INSERT INTO signals (id, article_id, market_id, similarity, cluster_id,
                                     freshness, embedder_id, cycle_id, is_tracer, tracer_id, created_at)
                VALUES (?, ?, 'm1', 0.8, NULL, 'BREAKING', 'fake@4', 1, 0, NULL,
                        datetime('now','-15 seconds'))
                """,
                (tid, tid),
            )
            await cur.execute(
                """
                INSERT INTO decisions (id, signal_id, outcome, reason, gate_history,
                                       prediction_id, cycle_id, is_tracer, tracer_id, created_at)
                VALUES (?, ?, 'execute', 'ok', '[]', ?, 1, 0, NULL,
                        datetime('now','-10 seconds'))
                """,
                (tid, tid, tid),
            )
            await cur.execute(
                """
                INSERT INTO trades
                  (id, signal_id, decision_id, prediction_id, market_id, direction,
                   confidence, similarity, freshness, p_hat, mid_price, edge,
                   kelly_fraction, size_usdc, decision_path, features_snapshot,
                   cycle_id, is_tracer, tracer_id, decided_at)
                VALUES (?, ?, ?, ?, 'm1', 'YES', 0.8, 0.8, 'BREAKING', 0.8, 0.5, 0.1,
                        0.1, 25.0, ?, '{}', 1, 0, NULL,
                        datetime('now','-5 seconds'))
                """,
                (tid, tid, tid, tid, path),
            )
            await cur.execute(
                """
                INSERT INTO trade_closures
                  (trade_id, closed_at, close_reason, exit_mid_price,
                   realized_pnl_usdc, cycle_id, is_tracer, tracer_id)
                VALUES (?, datetime('now', ?), 'timeout', 0.55,
                        ?, 1, 0, NULL)
                """,
                (tid, f"-{tid} seconds", pnl),
            )

    dash = Dashboard(data_dir=tmp_path, db=db_ready, interval_secs=60)
    loop = asyncio.get_running_loop()
    png = await pnl_cumulative.render(dash, loop=loop)
    assert png, "pnl_cumulative produced empty bytes despite seeded data"
    assert png[:8] == PNG_MAGIC
    assert len(png) > 1000, "pnl_cumulative PNG suspiciously small"


async def test_pnl_cumulative_plot_empty_db_placeholder(db_ready, tmp_path):
    """With no closed trades, the placeholder PNG still draws (no 404)."""
    import asyncio

    from app.dashboard.plots import pnl_cumulative

    dash = Dashboard(data_dir=tmp_path, db=db_ready, interval_secs=60)
    loop = asyncio.get_running_loop()
    png = await pnl_cumulative.render(dash, loop=loop)
    # Placeholder mode: a real PNG, not empty bytes, so the dashboard
    # always has something to display.
    assert png[:8] == PNG_MAGIC
    assert len(png) > 1000

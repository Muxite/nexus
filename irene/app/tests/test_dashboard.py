"""Dashboard tests — every panel renders against a populated DB, atomic
writes are verified, and empty-DB paths don't crash."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

import app.ai  # noqa: F401
import app.main  # noqa: F401
import app.stages.closer  # noqa: F401
import app.stages.dedupe  # noqa: F401
import app.stages.extract  # noqa: F401
import app.stages.ingest  # noqa: F401
import app.stages.markets  # noqa: F401
import app.stages.match  # noqa: F401
import app.stages.resolve  # noqa: F401
import app.stages.trade  # noqa: F401
from app.dashboard import Dashboard, _percentiles
from app.db import DB


def test_percentiles_empty():
    p = _percentiles([])
    assert p == {"p50": 0.0, "p95": 0.0, "p99": 0.0, "count": 0}


def test_percentiles_single_value():
    p = _percentiles([42.0])
    assert p["count"] == 1
    assert p["p50"] == 42.0


def test_percentiles_known_distribution():
    p = _percentiles(list(range(1, 101)))  # 1..100
    assert p["count"] == 100
    assert 49 <= p["p50"] <= 51
    assert p["p95"] >= 95
    assert p["p99"] >= 99


@pytest.fixture
async def db_ready():
    db = DB.ephemeral()
    await db.connect()
    try:
        yield db
    finally:
        await db.close()


async def test_tick_writes_all_files_empty_db(db_ready, tmp_path):
    dash = Dashboard(data_dir=tmp_path, db=db_ready, interval_secs=60)
    await dash.tick()
    for name in (
        "dashboard.txt", "latency.txt", "funnel.txt",
        "recent_trades.txt", "news_staleness.txt",
    ):
        path = tmp_path / name
        assert path.exists(), f"{name} not written"
        # Atomic write left no .tmp behind
        assert not (tmp_path / (name + ".tmp")).exists()
        body = path.read_text()
        assert body.startswith("# irene"), f"{name} missing header"


async def test_dashboard_with_data(db_ready, tmp_path):
    """Seed one article + signal + trade and confirm the panels render
    non-empty rows for them."""
    async with db_ready.batch() as cur:
        await cur.execute(
            """
            INSERT INTO articles (id, source, source_detail, url, title, summary,
                                  published_at, ingested_at, cycle_id, is_tracer, tracer_id)
            VALUES (1,'rss','x','u','t','s',
                    datetime('now','-30 seconds'),
                    datetime('now','-20 seconds'), 1, 0, NULL)
            """,
        )
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
        await cur.execute(
            """
            INSERT INTO signals (id, article_id, market_id, similarity, cluster_id,
                                 freshness, embedder_id, cycle_id, is_tracer, tracer_id, created_at)
            VALUES (1, 1, 'm1', 0.8, NULL, 'BREAKING', 'fake@4', 1, 0, NULL,
                    datetime('now','-10 seconds'))
            """,
        )
        await cur.execute(
            """
            INSERT INTO decisions (id, signal_id, outcome, reason, gate_history,
                                   prediction_id, cycle_id, is_tracer, tracer_id, created_at)
            VALUES (1, 1, 'execute', 'ok', '[]', 1, 1, 0, NULL,
                    datetime('now','-5 seconds'))
            """,
        )
        await cur.execute(
            """
            INSERT INTO trades
              (id, signal_id, decision_id, prediction_id, market_id, direction,
               confidence, similarity, freshness, p_hat, mid_price, edge,
               kelly_fraction, size_usdc, decision_path, features_snapshot,
               cycle_id, is_tracer, tracer_id, decided_at)
            VALUES (1, 1, 1, 1, 'm1', 'YES', 0.8, 0.8, 'BREAKING', 0.8, 0.5, 0.1,
                    0.1, 25.0, 'mvp', '{}', 1, 0, NULL,
                    datetime('now','-2 seconds'))
            """,
        )

    dash = Dashboard(data_dir=tmp_path, db=db_ready, interval_secs=60)
    await dash.tick()

    main = (tmp_path / "dashboard.txt").read_text()
    assert "articles ingested (1h): 1" in main
    assert "signals matched   (1h): 1" in main
    assert "trades decided    (1h): 1" in main

    latency = (tmp_path / "latency.txt").read_text()
    assert "ingest → trade" in latency
    assert "news age at trade-time" in latency

    funnel = (tmp_path / "funnel.txt").read_text()
    assert "ok" in funnel

    trades = (tmp_path / "recent_trades.txt").read_text()
    assert "m1" in trades
    assert "YES" in trades

    staleness = (tmp_path / "news_staleness.txt").read_text()
    # The seeded article was published 30s ago, trade 2s ago → ~28s
    assert "p50:" in staleness


async def test_one_bad_panel_does_not_break_others(db_ready, tmp_path, monkeypatch):
    """If the funnel query raises, every other file still writes."""
    dash = Dashboard(data_dir=tmp_path, db=db_ready, interval_secs=60)

    async def boom() -> str:
        raise RuntimeError("query exploded")

    monkeypatch.setattr(dash, "_render_funnel", boom)
    await dash.tick()
    assert (tmp_path / "dashboard.txt").exists()
    assert (tmp_path / "latency.txt").exists()
    # funnel.txt either doesn't exist or wasn't updated this tick — either is fine.
    assert not (tmp_path / "funnel.txt.tmp").exists()


async def test_stop_is_idempotent(db_ready, tmp_path):
    dash = Dashboard(data_dir=tmp_path, db=db_ready, interval_secs=60)
    await dash.stop()
    await dash.stop()  # second call must no-op

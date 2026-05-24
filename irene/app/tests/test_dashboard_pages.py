"""Smoke tests for individual irene dashboard page modules.

These complement test_dashboard.py — that file exercises the full
`Dashboard.tick()` loop; this one targets specific pages in isolation
so a regression on one page produces a focused failure.
"""

from __future__ import annotations

import pytest

# These imports register table DDLs — same fan-in as test_dashboard.py.
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
from app.dashboard.pages import pnl as pnl_page  # noqa: E402
from app.dashboard.pages import recent_trades as recent_trades_page  # noqa: E402
from app.db import DB  # noqa: E402


@pytest.fixture
async def db_ready():
    db = DB.ephemeral()
    await db.connect()
    try:
        yield db
    finally:
        await db.close()


async def _seed_market(cur, condition_id: str = "m1") -> None:
    await cur.execute(
        """
        INSERT INTO markets
          (condition_id, slug, question, description, category, end_date,
           volume_usdc, token_yes, token_no, embedding, embedder_id,
           embedding_dim, embedding_updated_at, active, closed_at,
           first_seen_at, updated_at)
        VALUES (?, 's','q','d','c','2026-12-31',100.0,'ty','tn',NULL,NULL,NULL,NULL,1,NULL,
                datetime('now'), datetime('now'))
        """,
        (condition_id,),
    )


async def _seed_trade(
    cur,
    *,
    trade_id: int,
    decision_path: str,
    closed: bool,
    realized_pnl: float = 0.0,
) -> None:
    await cur.execute(
        """
        INSERT INTO articles (id, source, source_detail, url, title, summary,
                              published_at, ingested_at, cycle_id, is_tracer, tracer_id)
        VALUES (?, 'rss','x', ?, ?, ?,
                datetime('now','-60 seconds'),
                datetime('now','-30 seconds'), 1, 0, NULL)
        """,
        (trade_id, f"u{trade_id}", f"t{trade_id}", f"s{trade_id}"),
    )
    await cur.execute(
        """
        INSERT INTO signals (id, article_id, market_id, similarity, cluster_id,
                             freshness, embedder_id, cycle_id, is_tracer, tracer_id, created_at)
        VALUES (?, ?, 'm1', 0.8, NULL, 'BREAKING', 'fake@4', 1, 0, NULL,
                datetime('now','-15 seconds'))
        """,
        (trade_id, trade_id),
    )
    await cur.execute(
        """
        INSERT INTO decisions (id, signal_id, outcome, reason, gate_history,
                               prediction_id, cycle_id, is_tracer, tracer_id, created_at)
        VALUES (?, ?, 'execute', 'ok', '[]', ?, 1, 0, NULL,
                datetime('now','-10 seconds'))
        """,
        (trade_id, trade_id, trade_id),
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
        (trade_id, trade_id, trade_id, trade_id, decision_path),
    )
    if closed:
        await cur.execute(
            """
            INSERT INTO trade_closures
              (trade_id, closed_at, close_reason, exit_mid_price,
               realized_pnl_usdc, cycle_id, is_tracer, tracer_id)
            VALUES (?, datetime('now','-1 seconds'), 'timeout', 0.55,
                    ?, 1, 0, NULL)
            """,
            (trade_id, realized_pnl),
        )


async def test_pnl_page_smoke(db_ready, tmp_path):
    """pnl.render handles both decision paths and surfaces them in output."""
    async with db_ready.batch() as cur:
        await _seed_market(cur)
        # Two MVP trades, one closed (winner) one open.
        await _seed_trade(
            cur, trade_id=1, decision_path="mvp", closed=True, realized_pnl=0.42,
        )
        await _seed_trade(cur, trade_id=2, decision_path="mvp", closed=False)
        # Two DKG trades, one closed (loser) one open.
        await _seed_trade(
            cur, trade_id=3, decision_path="dkg", closed=True, realized_pnl=-0.10,
        )
        await _seed_trade(cur, trade_id=4, decision_path="dkg", closed=False)

    dash = Dashboard(data_dir=tmp_path, db=db_ready, interval_secs=60)
    body = await pnl_page.render(dash)

    # Both paths appear in the summary block.
    assert "mvp" in body
    assert "dkg" in body
    # Header is correct.
    assert body.startswith("# irene pnl")
    # Closed-trade counts present (we have one closed per path).
    assert "+0.420" in body or "+0.42" in body  # winner mean/sum
    # Open positions block present.
    assert "open positions:" in body
    # The DKG tail block fires when there's any DKG trade.
    assert "recent dkg trades" in body


async def test_pnl_page_empty_db(db_ready, tmp_path):
    """Empty DB renders cleanly without raising."""
    dash = Dashboard(data_dir=tmp_path, db=db_ready, interval_secs=60)
    body = await pnl_page.render(dash)
    assert body.startswith("# irene pnl")
    assert "(no trades yet)" in body


async def test_recent_trades_with_closures(db_ready, tmp_path):
    """recent_trades.render shows 'open' for unclosed trades and a signed PnL
    for closed ones — exercises the LEFT JOIN against trade_closures."""
    async with db_ready.batch() as cur:
        await _seed_market(cur)
        # One closed winner (DKG path), one still open (MVP path).
        await _seed_trade(
            cur, trade_id=1, decision_path="dkg", closed=True, realized_pnl=0.50,
        )
        await _seed_trade(
            cur, trade_id=2, decision_path="mvp", closed=False,
        )

    dash = Dashboard(data_dir=tmp_path, db=db_ready, interval_secs=60)
    body = await recent_trades_page.render(dash)

    assert body.startswith("# irene recent trades")
    # Header advertises the new columns.
    assert "path" in body
    assert "pnl" in body
    # Open trade shows 'open' status with no signed PnL.
    assert "open" in body
    # Closed trade shows a positive signed PnL.
    assert "+0.500" in body
    # Both decision_path values are visible.
    assert "mvp" in body
    assert "dkg" in body

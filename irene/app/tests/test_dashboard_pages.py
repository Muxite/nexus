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
from app.dashboard.pages import latency_breakdown as latency_breakdown_page  # noqa: E402
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


async def test_pnl_page_filters_by_current_cycle(db_ready, tmp_path):
    """Cycle 4: pnl.render must restrict to dash.current_cycle_id so
    historical closures from prior cycles don't bleed into the live
    `sum`. Seed two cycles, set current to cycle 2, expect only
    cycle-2 trades to appear in the summary."""
    async with db_ready.batch() as cur:
        await _seed_market(cur)
        # Cycle 1 (old): one closed loser
        await _seed_trade(
            cur, trade_id=10, decision_path="mvp", closed=True, realized_pnl=-99.0,
        )
        await cur.execute("UPDATE trades SET cycle_id=1 WHERE id=10")
        await cur.execute("UPDATE trade_closures SET cycle_id=1 WHERE trade_id=10")
        # Cycle 2 (current): one closed winner
        await _seed_trade(
            cur, trade_id=11, decision_path="mvp", closed=True, realized_pnl=+0.42,
        )
        await cur.execute("UPDATE trades SET cycle_id=2 WHERE id=11")
        await cur.execute("UPDATE trade_closures SET cycle_id=2 WHERE trade_id=11")

    dash = Dashboard(data_dir=tmp_path, db=db_ready, interval_secs=60)
    dash.current_cycle_id = 2
    body = await pnl_page.render(dash)

    # cycle-2 winner shown, cycle-1 loser absent
    assert "+0.420" in body or "+0.42" in body
    assert "-99.000" not in body and "-99.00" not in body
    # Header advertises the cycle scope
    assert "cycle_id=2" in body


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


# ─── latency_breakdown (cycle 15) ─────────────────────────────────────
#
# The latency_breakdown page reads decision_latency_ms +
# decision_latency_breakdown (JSON) from the trades table, computes
# per-stage p50/p95/p99, and surfaces them in a fixed-width table.
# These three tests cover: empty DB, populated DB (percentile values
# match the seed), and NULL-tolerant filtering.

import json  # noqa: E402  (test-helper-only import; keep colocated)


async def _seed_trade_with_latency(
    cur,
    *,
    trade_id: int,
    decision_latency_ms,
    decision_latency_breakdown,
) -> None:
    """Insert one execute-path trade with arbitrary latency fields.

    Mirrors `_seed_trade` (closed=False) but parameterises the two
    cycle-14 latency columns so each test can drive specific values
    through `_percentiles`.
    """
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
    breakdown_json = (
        json.dumps(decision_latency_breakdown)
        if decision_latency_breakdown is not None
        else None
    )
    await cur.execute(
        """
        INSERT INTO trades
          (id, signal_id, decision_id, prediction_id, market_id, direction,
           confidence, similarity, freshness, p_hat, mid_price, edge,
           kelly_fraction, size_usdc, decision_path, features_snapshot,
           cycle_id, is_tracer, tracer_id, decided_at,
           decision_latency_ms, decision_latency_breakdown)
        VALUES (?, ?, ?, ?, 'm1', 'YES', 0.8, 0.8, 'BREAKING', 0.8, 0.5, 0.1,
                0.1, 25.0, 'mvp', '{}', 1, 0, NULL,
                datetime('now','-5 seconds'), ?, ?)
        """,
        (trade_id, trade_id, trade_id, trade_id,
         decision_latency_ms, breakdown_json),
    )


async def test_latency_panel_renders_with_zero_trades(db_ready, tmp_path):
    """Empty DB → graceful 'no instrumented trades yet' message, no crash."""
    dash = Dashboard(data_dir=tmp_path, db=db_ready, interval_secs=60)
    body = await latency_breakdown_page.render(dash)
    assert body.startswith("# irene latency breakdown")
    assert "no instrumented trades yet" in body
    # No stage rows leaked through.
    assert "fetch_article" not in body
    assert "total" not in body


async def test_latency_panel_with_seeded_trades(db_ready, tmp_path):
    """10 seeded trades with deterministic latency → percentiles match.

    `fetch_article` samples are 1..10ms; p50 should be ~5/6 and p95
    ~10 (small-N sort percentile via _percentiles). Total is twice
    each per-step sample (so p50 ~10/12, p95 ~20).
    """
    async with db_ready.batch() as cur:
        await _seed_market(cur)
        for i in range(1, 11):
            await _seed_trade_with_latency(
                cur,
                trade_id=i,
                decision_latency_ms=float(i * 2),
                decision_latency_breakdown={
                    "fetch_article": float(i),
                    "fetch_market": float(i) + 0.5,
                    "ai_direction": float(i * 10),
                },
            )

    dash = Dashboard(data_dir=tmp_path, db=db_ready, interval_secs=60)
    body = await latency_breakdown_page.render(dash)

    # Header advertises the right window size.
    assert body.startswith("# irene latency breakdown")
    assert "last 10 executed trades" in body
    # All three seeded stages and the total row are rendered.
    assert "fetch_article" in body
    assert "fetch_market" in body
    assert "ai_direction" in body
    assert "total" in body
    # Canonical column header.
    assert "stage" in body and "p50" in body and "p95" in body and "p99" in body
    # Spot-check one cell: fetch_article max is 10.0 (samples 1..10).
    # The page formats as "10.0" — survives the right-justified column.
    assert "10.0" in body
    # ai_direction max is 100.0 — confirms the largest step dominates.
    assert "100.0" in body
    # n column for each stage = 10 (all rows contributed).
    # We assert the existence of the 'n' header and that '10' appears
    # alongside fetch_article on its row.
    fetch_line = next(
        line for line in body.splitlines() if line.strip().startswith("fetch_article")
    )
    cells = fetch_line.split()
    # Row shape: stage n p50 p95 p99 max → 6 cells.
    assert len(cells) == 6
    assert cells[0] == "fetch_article"
    assert cells[1] == "10"  # n
    # p50 for 1..10 (sorted): index round((10-1)*0.5)=4 → value 5.0
    assert cells[2] == "5.0"
    # max == 10.0
    assert cells[5] == "10.0"


async def test_latency_panel_skips_trades_with_null_breakdown(db_ready, tmp_path):
    """Mixed populated + NULL rows: only the populated ones contribute.

    Seed 5 trades with breakdown and 5 with decision_latency_ms=NULL +
    decision_latency_breakdown=NULL (legacy / reject-path shape). The
    page's WHERE clause drops the NULL rows so the rendered n is 5,
    not 10, and the percentiles come only from the populated set.
    """
    async with db_ready.batch() as cur:
        await _seed_market(cur)
        # 5 populated rows with fetch_article in {1, 2, 3, 4, 5}
        for i in range(1, 6):
            await _seed_trade_with_latency(
                cur,
                trade_id=i,
                decision_latency_ms=float(i),
                decision_latency_breakdown={"fetch_article": float(i)},
            )
        # 5 NULL rows that should be filtered out by the WHERE clause.
        for i in range(6, 11):
            await _seed_trade_with_latency(
                cur,
                trade_id=i,
                decision_latency_ms=None,
                decision_latency_breakdown=None,
            )

    dash = Dashboard(data_dir=tmp_path, db=db_ready, interval_secs=60)
    body = await latency_breakdown_page.render(dash)

    # Window header reports 5, not 10 — proving the NULL rows are dropped.
    assert "last 5 executed trades" in body
    # fetch_article row: n=5, max=5.0 (NOT 10 — those rows are out).
    fetch_line = next(
        line for line in body.splitlines() if line.strip().startswith("fetch_article")
    )
    cells = fetch_line.split()
    assert cells[0] == "fetch_article"
    assert cells[1] == "5"
    assert cells[5] == "5.0"

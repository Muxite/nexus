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
from app.dashboard.pages import main as main_page  # noqa: E402
from app.dashboard.pages import near_miss_audit as near_miss_audit_page  # noqa: E402
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


# ─── reject-path section (cycle 16) ──────────────────────────────────
# The cycle-16 dashboard extension reads the same two latency columns
# off `decisions` (reject rows) and appends a per-`reason` section
# underneath the existing execute-path table. Seed a mix of rejects
# with partial breakdowns and verify the section renders.


async def _seed_reject_decision(
    cur,
    *,
    decision_id: int,
    reason: str,
    decision_latency_ms,
    decision_latency_breakdown,
) -> None:
    """Insert a reject decision with cycle-16 latency columns populated.

    No `signal_id` is set on the decision (and no signal/article rows
    are seeded) — the dashboard reader only joins on `decisions`
    columns, so the reject-section render needs nothing else.
    """
    breakdown_json = (
        json.dumps(decision_latency_breakdown)
        if decision_latency_breakdown is not None
        else None
    )
    await cur.execute(
        """
        INSERT INTO decisions (id, signal_id, outcome, reason, gate_history,
                               prediction_id, cycle_id, is_tracer, tracer_id, created_at,
                               decision_latency_ms, decision_latency_breakdown)
        VALUES (?, NULL, 'reject', ?, '[]', NULL, 1, 0, NULL,
                datetime('now','-5 seconds'), ?, ?)
        """,
        (decision_id, reason, decision_latency_ms, breakdown_json),
    )


async def test_latency_panel_renders_reject_section(db_ready, tmp_path):
    """Mix of reject decisions with partial breakdowns:
      - 3 `similarity_floor` rejects (gate 1 → empty breakdown).
      - 2 `edge_below_min` rejects (gate 4 → fetch_article +
        fetch_market + ai_direction + resolve_mid + resolve_book).
    The rendered page must contain the reject-section header, both
    per-reason headers with the right counts, and a per-step line for
    ai_direction under edge_below_min.
    """
    async with db_ready.batch() as cur:
        # similarity_floor — gate 1, no instrumented step ran.
        for i in range(1, 4):
            await _seed_reject_decision(
                cur,
                decision_id=i,
                reason="similarity_floor",
                decision_latency_ms=float(i),
                decision_latency_breakdown={},
            )
        # edge_below_min — gate 4, fetched + ai_direction + resolves.
        for i in range(4, 6):
            await _seed_reject_decision(
                cur,
                decision_id=i,
                reason="edge_below_min",
                decision_latency_ms=float(i * 50),
                decision_latency_breakdown={
                    "fetch_article": 2.0,
                    "fetch_market": 1.5,
                    "ai_direction": 140.0 + i,
                    "resolve_mid": 5.0,
                    "resolve_book": 6.0,
                },
            )

    dash = Dashboard(data_dir=tmp_path, db=db_ready, interval_secs=60)
    body = await latency_breakdown_page.render(dash)

    # Section header — the n is the count of reject rows we seeded.
    assert "Reject-path latencies" in body
    assert "last 5 rejects" in body
    # Both per-reason headers appear with the right n.
    assert "similarity_floor (n=3)" in body
    assert "edge_below_min (n=2)" in body
    # similarity_floor had no per-step samples — render explains why.
    assert "no per-step samples" in body
    # edge_below_min carries ai_direction.
    edge_lines = [
        line for line in body.splitlines()
        if line.strip().startswith("ai_direction")
    ]
    assert edge_lines, "ai_direction per-step line not found in reject section"
    # n=2 for the two edge_below_min rejects.
    assert "n=2" in edge_lines[0]


# ─── wallet honesty on dead-market positions (cycle 24, handoff E) ───
#
# When a market closes while we still hold a position, Polymarket's
# /midpoint returns the frozen last-traded price and the wallet panel
# used to render "invested $X → current $X" as if that were live PnL.
# main._current_mid_for_trade now short-circuits to None on closed/
# inactive markets, and the per-position display annotates the row as
# "unpriceable (frozen, market closed YYYY-MM-DD)". The accounting
# identity must still hold (current_mid=None ⇒ value=entry_mid ⇒
# unrealized=0), so the EQUITY check is still "OK".


async def test_wallet_marks_closed_market_position_as_unpriceable(db_ready, tmp_path):
    """Position on a closed market renders as unpriceable, not as a
    healthy mark-to-market. EQUITY identity still holds because the
    wallet helpers fall back to entry_mid (unrealized PnL = 0)."""
    async with db_ready.batch() as cur:
        # Seed the market as CLOSED 3 days ago (mirrors trade 40 in the
        # cycle-19 retro). active=0, closed_at='2026-05-23'.
        await cur.execute(
            """
            INSERT INTO markets
              (condition_id, slug, question, description, category, end_date,
               volume_usdc, token_yes, token_no, embedding, embedder_id,
               embedding_dim, embedding_updated_at, active, closed_at,
               first_seen_at, updated_at)
            VALUES ('m1','s','Will the dead market resolve?','d','c','2026-05-23',
                    100.0,'ty','tn',NULL,NULL,NULL,NULL,0,'2026-05-23T00:00:00+00:00',
                    datetime('now'), datetime('now'))
            """
        )
        # One open trade on the dead market.
        await _seed_trade(cur, trade_id=1, decision_path="mvp", closed=False)

    dash = Dashboard(data_dir=tmp_path, db=db_ready, interval_secs=60)
    # cfg=None is the safe default — _current_mid_for_trade returns None
    # before reaching the closed-market check, so we explicitly want a
    # cfg so the closed-market short-circuit is exercised.
    from app.config import Config
    dash.cfg = Config()
    body = await main_page.render(dash)

    # Per-position annotation — date is the truncated closed_at.
    assert "unpriceable (frozen, market closed 2026-05-23)" in body
    # Summary line surfaces the unpriceable count: 1 open, 0 priced, 1 unpriceable.
    assert "open positions (1, 0 priced (1 unpriceable))" in body
    # current == invested (no fake PnL): both are $25.00 from the seed.
    assert "invested $   25.00 → current $   25.00" in body
    # And the accounting identity still passes (current=entry_mid → unrealized=0).
    assert "identity OK" in body


# ─── near-miss audit (cycle 24, handoff F) ────────────────────────────
#
# Two sections, both populated from `decisions ← predictions`:
#   A. direction_lowconf rejects with confidence in
#      [audit_confidence_floor, min_confidence).
#   B. decisive YES/NO rejects (confidence >= min_confidence) killed by
#      a downstream gate (cash, mid, spread, etc.).
# The page must render an empty-state block when either section is
# empty so the operator knows the pipeline is starved upstream.


async def _seed_near_miss_decision(
    cur,
    *,
    decision_id: int,
    reason: str,
    direction: str,
    confidence: float,
) -> None:
    """Seed an article + signal + prediction + reject decision pair.

    Mirrors `_seed_trade` (with closed=False) up to the trades insert,
    but writes a `reject` decision instead of `execute` and writes a
    prediction row with the supplied direction/confidence so the
    near-miss page's JOINs land.
    """
    await cur.execute(
        """
        INSERT INTO articles (id, source, source_detail, url, title, summary,
                              published_at, ingested_at, cycle_id, is_tracer, tracer_id)
        VALUES (?, 'rss','x', ?, ?, ?,
                datetime('now','-60 seconds'),
                datetime('now','-30 seconds'), 1, 0, NULL)
        """,
        (decision_id, f"u{decision_id}", f"t{decision_id}", f"s{decision_id}"),
    )
    await cur.execute(
        """
        INSERT INTO signals (id, article_id, market_id, similarity, cluster_id,
                             freshness, embedder_id, cycle_id, is_tracer, tracer_id, created_at)
        VALUES (?, ?, 'm1', 0.8, NULL, 'BREAKING', 'fake@4', 1, 0, NULL,
                datetime('now','-15 seconds'))
        """,
        (decision_id, decision_id),
    )
    await cur.execute(
        """
        INSERT INTO predictions
            (id, signal_id, llm_call_id, model, prompt_version,
             direction, confidence, cached, raw_response, label,
             cycle_id, is_tracer, tracer_id, created_at)
        VALUES (?, ?, NULL, 'fake-model', 'v1', ?, ?, 0, ?, ?, 1, 0, NULL,
                datetime('now','-12 seconds'))
        """,
        (
            decision_id,
            decision_id,
            direction,
            confidence,
            direction[0],
            direction[0],
        ),
    )
    # gate_history contains the rejecting gate so the CLI's snippet
    # extractor has something to chew on, but the dashboard page does
    # not use it.
    gate_history = json.dumps([
        {"gate": "freshness", "outcome": "ok",
         "value": "BREAKING", "threshold": ["BREAKING", "EVOLVING"]},
        {"gate": reason, "outcome": "reject",
         "value": confidence, "threshold": 0.55},
    ])
    await cur.execute(
        """
        INSERT INTO decisions (id, signal_id, outcome, reason, gate_history,
                               prediction_id, cycle_id, is_tracer, tracer_id, created_at)
        VALUES (?, ?, 'reject', ?, ?, ?, 1, 0, NULL,
                datetime('now','-5 seconds'))
        """,
        (decision_id, decision_id, reason, gate_history, decision_id),
    )


async def test_near_miss_audit_empty_db(db_ready, tmp_path):
    """Empty DB → both sections render the empty-state message and the
    page does not crash. Header advertises the configured floors."""
    dash = Dashboard(data_dir=tmp_path, db=db_ready, interval_secs=60)
    body = await near_miss_audit_page.render(dash)

    # Header carries the default floors (cfg=None on the test Dashboard
    # → page falls back to 0.35 / 0.55).
    assert body.startswith("# irene near-miss audit")
    assert "audit_floor=0.35" in body
    assert "trade_floor=0.55" in body
    # Both section headers are present.
    assert "Section A: leaning YES/NO rejected at the direction gate" in body
    assert "Section B: decisive YES/NO killed by a downstream gate" in body
    # Empty-state message renders TWICE (one per section).
    empty_marker = "no near-miss rows in window"
    assert body.count(empty_marker) == 2


async def test_near_miss_audit_populated_sections(db_ready, tmp_path):
    """Seed one near-miss per section and verify each lands in the
    right block with the right metadata.

    - Section A: a `direction_lowconf` reject with confidence 0.45
      (above the 0.35 audit floor, below the 0.55 trade floor).
    - Section B: an `insufficient_cash` reject with confidence 0.70
      (above the 0.55 trade floor — the LLM was decisive but a
      downstream gate killed the trade).
    """
    async with db_ready.batch() as cur:
        await _seed_market(cur)
        # Section A row — leaning YES, killed at direction gate.
        await _seed_near_miss_decision(
            cur,
            decision_id=1,
            reason="direction_lowconf",
            direction="YES",
            confidence=0.45,
        )
        # Section B row — decisive NO, killed by insufficient cash.
        await _seed_near_miss_decision(
            cur,
            decision_id=2,
            reason="insufficient_cash",
            direction="NO",
            confidence=0.70,
        )

    dash = Dashboard(data_dir=tmp_path, db=db_ready, interval_secs=60)
    body = await near_miss_audit_page.render(dash)

    # Carve into the two sections so we can assert per-section content.
    section_a, section_b = body.split("=== Section B:", 1)

    # Section A — direction_lowconf row visible with its confidence,
    # direction, and the article/market titles from the seed.
    assert "conf=0.45" in section_a
    assert "YES" in section_a
    assert "t1" in section_a  # article title from the seed helper
    assert "q" in section_a   # market question from _seed_market
    # The empty-state marker must NOT fire on Section A.
    assert "no near-miss rows in window" not in section_a

    # Section B — insufficient_cash row visible with its confidence,
    # direction, and an explicit `reason=` annotation (only Section B
    # surfaces the reject reason on the header).
    assert "conf=0.70" in section_b
    assert "NO" in section_b
    assert "reason=insufficient_cash" in section_b
    assert "no near-miss rows in window" not in section_b

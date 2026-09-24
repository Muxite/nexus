"""Cycle 36 — category concentration cap gate.

`cluster_dedup` (cycle 28) stops the bot stacking bets on one article
cluster; `binary_dedup` (cycle 25-B) stops opposite legs of one binary.
Neither sees concentration across many *unrelated* trades that all happen
to share a `markets.category` (e.g. a dozen independent "politics" bets
that all move on the same macro news day). These tests lock:

1. a second, different-cluster trade in the same category is rejected
   once cumulative exposure would exceed `max_category_exposure_fraction`
   of balance;
2. a trade in a DIFFERENT category is unaffected by the first category's
   exposure;
3. once the first trade closes, its category's exposure frees up and the
   next trade in that category passes.

Reuses test_trade.py fixtures (same pattern as test_trade_cluster_dedup).
"""
from __future__ import annotations

import pytest

from app.cycle import cycle_scope
from app.tests._dates import open_end_date

from app.tests.stages.test_trade import (
    DirectionResult,
    _Config,
    _FakeAI,
    _FakeBus,
    _FakeCLOB,
    _TradeCfg,
    _make_stage,
    _payload,
    db_seeded,  # fixture, re-exported by import
)

pytestmark = pytest.mark.asyncio


async def _seed_market_and_signal(
    db, *, signal_id: int, market_id: str, category: str,
) -> None:
    """Second (article, market, signal) triple — db_seeded's article id=1
    is reused (signals are UNIQUE on (article_id, market_id), so one
    article can back several markets)."""
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO markets
                (condition_id, slug, question, description, category, end_date,
                 volume_usdc, token_yes, token_no, embedding, embedder_id,
                 embedding_dim, embedding_updated_at, active, closed_at,
                 first_seen_at, updated_at)
            VALUES (?, ?, 'Will Y happen?', 'd2', ?, ?,
                    50000.0, 'tY2', 'tN2', NULL, NULL, NULL, NULL, 1, NULL,
                    '2026-05-23T00:00:00+00:00', '2026-05-23T00:00:00+00:00')
            """,
            (market_id, f"slug-{market_id}", category, open_end_date()),
        )
        await cur.execute(
            """
            INSERT INTO signals
                (id, article_id, market_id, similarity, cluster_id, freshness,
                 embedder_id, cycle_id, is_tracer, tracer_id, created_at)
            VALUES (?, 1, ?, 0.80, NULL, 'BREAKING', 'fake@4', 1, 0, NULL,
                    '2026-05-23T00:00:00+00:00')
            """,
            (signal_id, market_id),
        )


async def _decision_for(db, signal_id: int):
    cur = await db.conn.execute(
        "SELECT outcome, reason FROM decisions WHERE signal_id=?", (signal_id,)
    )
    row = await cur.fetchone()
    await cur.close()
    return row


async def _close_trade(db, trade_id: int) -> None:
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO trade_closures
                (trade_id, closed_at, close_reason, exit_mid_price,
                 realized_pnl_usdc, cycle_id, is_tracer, tracer_id)
            VALUES (?, '2026-05-23T01:00:00+00:00', 'timeout', 0.5, 0.0, 1, 0, NULL)
            """,
            (trade_id,),
        )


def _tight_cap_cfg() -> _Config:
    # First trade sizes ~100 USDC (YES @ 0.70, mid 0.50, kelly_fraction
    # 0.25, balance 1000 — see test_trade_cluster_dedup / test_evaluate_gates
    # for the same math). Cap 0.15 * 1000 = 150 so a second same-category
    # trade (100 + 100 = 200 > 150) rejects without needing many signals.
    return _Config(trade=_TradeCfg(max_category_exposure_fraction=0.15))


async def test_second_trade_same_category_rejected_over_cap(db_seeded):
    # db_seeded's market m1 is category='politics'.
    cfg = _tight_cap_cfg()
    stage = _make_stage(db_seeded, _FakeBus(), _FakeAI(), cfg)
    with cycle_scope(1):
        await stage.handle(_payload(signal_id=1))
    d1 = await _decision_for(db_seeded, 1)
    assert d1["outcome"] == "execute"

    await _seed_market_and_signal(
        db_seeded, signal_id=2, market_id="m2", category="politics",
    )
    ai2 = _FakeAI()
    stage2 = _make_stage(db_seeded, _FakeBus(), ai2, cfg)
    with cycle_scope(1):
        await stage2.handle(_payload(signal_id=2, market_id="m2"))
    d2 = await _decision_for(db_seeded, 2)
    assert d2["outcome"] == "reject"
    assert d2["reason"] == "category_exposure_exceeded"

    cur = await db_seeded.conn.execute("SELECT COUNT(*) AS n FROM trades")
    assert (await cur.fetchone())["n"] == 1
    await cur.close()


async def test_different_category_unaffected_by_first(db_seeded):
    cfg = _tight_cap_cfg()
    stage = _make_stage(db_seeded, _FakeBus(), _FakeAI(), cfg)
    with cycle_scope(1):
        await stage.handle(_payload(signal_id=1))
    d1 = await _decision_for(db_seeded, 1)
    assert d1["outcome"] == "execute"

    await _seed_market_and_signal(
        db_seeded, signal_id=2, market_id="m2", category="sports",
    )
    stage2 = _make_stage(db_seeded, _FakeBus(), _FakeAI(), cfg)
    with cycle_scope(1):
        await stage2.handle(_payload(signal_id=2, market_id="m2"))
    d2 = await _decision_for(db_seeded, 2)
    assert d2["outcome"] == "execute"


async def test_category_unblocks_after_close(db_seeded):
    cfg = _tight_cap_cfg()
    stage = _make_stage(db_seeded, _FakeBus(), _FakeAI(), cfg)
    with cycle_scope(1):
        await stage.handle(_payload(signal_id=1))
    cur = await db_seeded.conn.execute("SELECT id FROM trades WHERE signal_id=1")
    trade_id = (await cur.fetchone())["id"]
    await cur.close()
    await _close_trade(db_seeded, trade_id)

    await _seed_market_and_signal(
        db_seeded, signal_id=2, market_id="m2", category="politics",
    )
    stage2 = _make_stage(db_seeded, _FakeBus(), _FakeAI(), cfg)
    with cycle_scope(1):
        await stage2.handle(_payload(signal_id=2, market_id="m2"))
    d2 = await _decision_for(db_seeded, 2)
    assert d2["outcome"] == "execute"

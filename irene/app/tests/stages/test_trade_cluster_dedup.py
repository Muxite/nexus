"""Cycle 28 — one-position-per-cluster gate.

The retrospective's re-scoring showed 845 trades collapse to ~52 article
clusters; the cluster, not the trade, is the unit of risk. These tests
lock:

1. gate: a second signal from a cluster with an OPEN trade rejects with
   `cluster_position_open` (pre-LLM — no direction call is made);
2. gate: once the cluster's trade closes, the next signal passes;
3. legacy rows: an open trade with NULL `trades.cluster_id` still blocks
   via the signals-join fallback;
4. race re-check: `_insert_trade` refuses inside the insert transaction
   when an open cluster trade landed after the gate ran;
5. concurrency invariant: two concurrent handles on one cluster yield
   exactly one trade and one `cluster_position_open` reject.

Reuses test_trade.py fixtures (same pattern as test_trade_safety_gates).
"""
from __future__ import annotations

import asyncio

import pytest

from app.cycle import cycle_scope

from app.tests.stages.test_trade import (
    DirectionResult,
    _FakeAI,
    _FakeBus,
    _FakeCLOB,
    _make_stage,
    _payload,
    db_seeded,  # fixture, re-exported by import
)

pytestmark = pytest.mark.asyncio


async def _seed_signal(db, *, signal_id: int, article_id: int | None = None,
                       market_id: str = "m1", cluster_id=None) -> None:
    """Seed a signal (plus a backing article — signals are UNIQUE on
    (article_id, market_id), so each extra signal gets its own article,
    defaulting to article_id=signal_id)."""
    if article_id is None:
        article_id = signal_id
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT OR IGNORE INTO articles
                (id, source, source_detail, url, title, summary, published_at,
                 ingested_at, cycle_id, is_tracer, tracer_id)
            VALUES (?, 'rss','test',?, 'Another headline',
                    'More context.', '2026-05-23T00:00:00+00:00',
                    '2026-05-23T00:00:00+00:00', 1, 0, NULL)
            """,
            (article_id, f"https://x/{article_id}"),
        )
        await cur.execute(
            """
            INSERT INTO signals
                (id, article_id, market_id, similarity, cluster_id, freshness,
                 embedder_id, cycle_id, is_tracer, tracer_id, created_at)
            VALUES (?, ?, ?, 0.80, ?, 'BREAKING', 'fake@4', 1, 0, NULL,
                    '2026-05-23T00:00:00+00:00')
            """,
            (signal_id, article_id, market_id, cluster_id),
        )


async def _set_signal_cluster(db, signal_id: int, cluster_id: int) -> None:
    async with db.batch() as cur:
        await cur.execute(
            "UPDATE signals SET cluster_id=? WHERE id=?", (cluster_id, signal_id)
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


async def test_second_signal_same_cluster_rejected_pre_llm(db_seeded):
    await _set_signal_cluster(db_seeded, 1, 77)
    bus = _FakeBus()
    ai = _FakeAI()  # YES @ 0.7
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload(signal_id=1))
    d = await _decision_for(db_seeded, 1)
    assert d["outcome"] == "execute"

    # Second signal, same cluster, fresh article+market not needed — the
    # gate fires before any fetch. The LLM must NOT be called.
    await _seed_signal(db_seeded, signal_id=2, cluster_id=77)
    ai2 = _FakeAI()
    calls_before = len(getattr(ai2, "calls", []) or [])
    stage2 = _make_stage(db_seeded, _FakeBus(), ai2)
    with cycle_scope(1):
        await stage2.handle(_payload(signal_id=2))
    d2 = await _decision_for(db_seeded, 2)
    assert d2["outcome"] == "reject"
    assert d2["reason"] == "cluster_position_open"
    assert len(getattr(ai2, "calls", []) or []) == calls_before, (
        "cluster_dedup must fire before the direction LLM call"
    )
    cur = await db_seeded.conn.execute("SELECT COUNT(*) AS n FROM trades")
    assert (await cur.fetchone())["n"] == 1
    await cur.close()


async def test_cluster_unblocks_after_close(db_seeded):
    await _set_signal_cluster(db_seeded, 1, 77)
    stage = _make_stage(db_seeded, _FakeBus(), _FakeAI())
    with cycle_scope(1):
        await stage.handle(_payload(signal_id=1))
    cur = await db_seeded.conn.execute("SELECT id FROM trades WHERE signal_id=1")
    trade_id = (await cur.fetchone())["id"]
    await cur.close()
    await _close_trade(db_seeded, trade_id)

    await _seed_signal(db_seeded, signal_id=2, cluster_id=77)
    stage2 = _make_stage(db_seeded, _FakeBus(), _FakeAI())
    with cycle_scope(1):
        await stage2.handle(_payload(signal_id=2))
    d2 = await _decision_for(db_seeded, 2)
    assert d2["outcome"] == "execute"


async def test_legacy_null_cluster_trade_blocks_via_signals_join(db_seeded):
    # Open a trade whose trades.cluster_id is NULL (legacy shape) but
    # whose SIGNAL belongs to cluster 77.
    await _set_signal_cluster(db_seeded, 1, 77)
    stage = _make_stage(db_seeded, _FakeBus(), _FakeAI())
    with cycle_scope(1):
        await stage.handle(_payload(signal_id=1))
    async with db_seeded.batch() as cur:
        await cur.execute("UPDATE trades SET cluster_id=NULL WHERE signal_id=1")

    await _seed_signal(db_seeded, signal_id=2, cluster_id=77)
    stage2 = _make_stage(db_seeded, _FakeBus(), _FakeAI())
    with cycle_scope(1):
        await stage2.handle(_payload(signal_id=2))
    d2 = await _decision_for(db_seeded, 2)
    assert d2["outcome"] == "reject"
    assert d2["reason"] == "cluster_position_open"


async def test_insert_trade_race_recheck_refuses(db_seeded):
    # Simulate the race window deterministically: an open cluster-77
    # trade lands BETWEEN handle()'s gate check and _insert_trade. Call
    # _insert_trade directly — the in-transaction re-check must refuse.
    await _set_signal_cluster(db_seeded, 1, 77)
    stage = _make_stage(db_seeded, _FakeBus(), _FakeAI())
    with cycle_scope(1):
        await stage.handle(_payload(signal_id=1))  # opens the cluster trade

    await _seed_signal(db_seeded, signal_id=2, cluster_id=77)
    with cycle_scope(1):
        trade_id, raced = await stage._insert_trade(
            signal_id=2, decision_id=999, prediction_id=2, market_id="m1",
            direction="YES", confidence=0.7, similarity=0.8, freshness="BREAKING",
            p_hat=0.7, mid=0.5, edge=0.18, kelly_fraction=0.1, size_usdc=100.0,
            features_snapshot={}, cycle_id=1, is_tracer=False, tracer_id=None,
            cluster_id=77,
        )
    assert trade_id is None
    assert raced is True
    cur = await db_seeded.conn.execute("SELECT COUNT(*) AS n FROM trades")
    assert (await cur.fetchone())["n"] == 1
    await cur.close()


async def test_concurrent_same_cluster_yields_one_trade_one_reject(db_seeded):
    await _set_signal_cluster(db_seeded, 1, 77)
    await _seed_signal(db_seeded, signal_id=2, cluster_id=77)
    stage1 = _make_stage(db_seeded, _FakeBus(), _FakeAI())
    stage2 = _make_stage(db_seeded, _FakeBus(), _FakeAI(
        next_result=DirectionResult(
            direction="YES", confidence=0.8, prediction_id=2,
            cached=False, raw_response="{}",
        )
    ))
    with cycle_scope(1):
        await asyncio.gather(
            stage1.handle(_payload(signal_id=1)),
            stage2.handle(_payload(signal_id=2)),
        )
    cur = await db_seeded.conn.execute("SELECT COUNT(*) AS n FROM trades")
    n_trades = (await cur.fetchone())["n"]
    await cur.close()
    cur = await db_seeded.conn.execute(
        "SELECT COUNT(*) AS n FROM decisions "
        "WHERE outcome='reject' AND reason='cluster_position_open'"
    )
    n_rejects = (await cur.fetchone())["n"]
    await cur.close()
    # Invariant regardless of interleaving (gate-level reject or the
    # in-transaction race re-check): one position per cluster, the loser
    # audited — never two trades, never a silent drop.
    assert n_trades == 1
    assert n_rejects == 1


async def test_clusterless_signal_passes_with_audited_entry(db_seeded):
    # db_seeded's signal 1 has cluster_id NULL — must trade normally with
    # an explicit "no_cluster" gate entry (audit-everything).
    import json as _json
    stage = _make_stage(db_seeded, _FakeBus(), _FakeAI())
    with cycle_scope(1):
        await stage.handle(_payload(signal_id=1))
    cur = await db_seeded.conn.execute(
        "SELECT features_snapshot FROM trades WHERE signal_id=1"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row is not None
    snap = _json.loads(row["features_snapshot"])
    entries = [g for g in snap["gate_history"] if g["gate"] == "cluster_dedup"]
    assert entries and entries[0]["outcome"] == "ok"
    assert entries[0]["threshold"] == "no_cluster"
    assert snap["cluster_id"] is None

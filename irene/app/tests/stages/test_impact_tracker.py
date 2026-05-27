"""ImpactTrackerStage tests.

Strategy mirrors `test_closer.py`:
- Real in-memory DB (`DB.ephemeral()`); seed `markets`, `trades` (and
  parent `decisions`) with raw SQL.
- Fake CLOB: `get_mid(token)` returns whatever the test stuffs into `mids`.
- Drive a single iteration of the poll loop via the public
  `run_one_poll_cycle()` method — no asyncio.sleep, no real background
  task. "Now" is controlled by patching `app.stages.impact_tracker._now`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

import pytest

# DDL registration side effects -------------------------------------------
import app.main  # noqa: F401  — cycles DDL
import app.stages.impact_tracker  # noqa: F401
import app.stages.markets  # noqa: F401
import app.stages.trade  # noqa: F401
from app.db import DB
from app.deps import StageDeps
from app.stages import impact_tracker as it_module
from app.stages.impact_tracker import ImpactTrackerStage


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeBus:
    """ImpactTracker neither publishes nor subscribes (the bus dispatches
    `handle` directly), but StageDeps requires a bus reference."""

    async def publish(self, topic: str, payload: dict) -> None:
        pass

    def subscribe(self, topic: str, handler) -> None:  # noqa: ARG002
        pass

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


@dataclass
class _FakeCLOB:
    mids: dict = field(default_factory=dict)
    calls: list = field(default_factory=list)
    # If set, every call returns this regardless of token (test convenience).
    fixed: Optional[float] = None
    # Per-token price history for the W6 pre-news drift path. Maps
    # token_id → list[(unix_ts, mid)]. Empty / missing token ⇒ None.
    histories: dict = field(default_factory=dict)
    history_calls: list = field(default_factory=list)

    async def get_mid(
        self, token_id: str, *, prefer_live: bool = True,
    ) -> Optional[float]:
        # Cycle 26: real CLOBClient.get_mid accepts a `prefer_live` kwarg;
        # the impact-tracker passes False to bypass the WS-fed cache and
        # read REST. Accept-and-ignore here — the fake has no cache.
        del prefer_live
        self.calls.append(token_id)
        if self.fixed is not None:
            return self.fixed
        return self.mids.get(token_id)

    async def get_price_history(
        self,
        token_id: str,
        *,
        interval: str = "1h",
        fidelity: int = 10,
    ) -> Optional[list]:
        self.history_calls.append(token_id)
        return self.histories.get(token_id)


@dataclass
class _ImpactCfg:
    poll_seconds: int = 30
    # Mirror the production default (`config.ImpactSection`). Tests that
    # need a tighter cutoff pass their own value.
    max_age_hours: float = 24.0


@dataclass
class _Config:
    impact: _ImpactCfg = field(default_factory=_ImpactCfg)


# ---------------------------------------------------------------------------
# Fixtures + helpers
# ---------------------------------------------------------------------------


@pytest.fixture
async def db():
    d = DB.ephemeral()
    await d.connect()
    try:
        yield d
    finally:
        await d.close()


async def _seed_market(
    db: DB,
    *,
    condition_id: str = "m1",
    token_yes: str = "tY",
    token_no: str = "tN",
) -> None:
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO markets
                (condition_id, slug, question, description, category, end_date,
                 volume_usdc, token_yes, token_no, embedding, embedder_id,
                 embedding_dim, embedding_updated_at, active, closed_at,
                 first_seen_at, updated_at)
            VALUES (?, 'slug', 'Q?', 'desc', 'cat', '2026-12-31', 1000.0,
                    ?, ?, NULL, NULL, NULL, NULL, 1, NULL,
                    '2026-05-23T00:00:00+00:00', '2026-05-23T00:00:00+00:00')
            """,
            (condition_id, token_yes, token_no),
        )


async def _seed_article_and_signal(
    db: DB,
    *,
    article_id: int = 1,
    signal_id: int = 1,
    market_id: str = "m1",
    published_at: Optional[datetime] = None,
) -> None:
    """Insert an `articles` row and a matching `signals` row so the
    impact-tracker pre-news-drift join (trades → signals → articles)
    can resolve. Tests that don't exercise pre-news drift can skip
    this — `_compute_pre_news_drift` is robust to a missing join."""
    if published_at is None:
        published_at = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO articles
                (id, source, source_detail, url, title, summary, published_at,
                 ingested_at, cycle_id, is_tracer, tracer_id)
            VALUES (?, 'rss', 'test', ?, 'headline', 'summary', ?,
                    ?, 1, 0, NULL)
            """,
            (
                article_id,
                f"https://x/{article_id}",
                published_at.isoformat(),
                published_at.isoformat(),
            ),
        )
        await cur.execute(
            """
            INSERT INTO signals
                (id, article_id, market_id, similarity, cluster_id, freshness,
                 embedder_id, cycle_id, is_tracer, tracer_id, created_at)
            VALUES (?, ?, ?, 0.8, NULL, 'BREAKING', 'fake@4', 1, 0, NULL, ?)
            """,
            (signal_id, article_id, market_id, published_at.isoformat()),
        )


async def _seed_trade(
    db: DB,
    *,
    trade_id: int,
    signal_id: int,
    market_id: str = "m1",
    direction: str = "YES",
    mid_price: float = 0.4,
    size_usdc: float = 100.0,
    decided_at: Optional[datetime] = None,
    is_tracer: int = 0,
    tracer_id: Optional[str] = None,
) -> None:
    if decided_at is None:
        decided_at = datetime.now(timezone.utc)
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO decisions
                (signal_id, outcome, reason, gate_history, prediction_id,
                 cycle_id, is_tracer, tracer_id, created_at)
            VALUES (?, 'execute', 'ok', '[]', 1, 1, ?, ?,
                    '2026-05-23T00:00:00+00:00')
            """,
            (signal_id, is_tracer, tracer_id),
        )
        decision_id = cur.lastrowid
        await cur.execute(
            """
            INSERT INTO trades
                (id, signal_id, decision_id, prediction_id, market_id, direction,
                 confidence, similarity, freshness, p_hat, mid_price, edge,
                 kelly_fraction, size_usdc, decision_path, features_snapshot,
                 cycle_id, is_tracer, tracer_id, decided_at)
            VALUES (?, ?, ?, 1, ?, ?, 0.7, 0.8, 'BREAKING', 0.7, ?, 0.1, 0.1,
                    ?, 'mvp', '{}', 1, ?, ?, ?)
            """,
            (
                trade_id, signal_id, decision_id, market_id, direction,
                mid_price, size_usdc, is_tracer, tracer_id,
                decided_at.isoformat(),
            ),
        )


def _make_stage(db: DB, clob: _FakeCLOB, cfg: Optional[_Config] = None) -> ImpactTrackerStage:
    deps = StageDeps(db=db, bus=_FakeBus(), config=cfg or _Config(), clob=clob)
    return ImpactTrackerStage(deps)


async def _impacts(db: DB) -> list[dict]:
    cur = await db.conn.execute(
        "SELECT * FROM price_impacts ORDER BY id"
    )
    rows = await cur.fetchall()
    await cur.close()
    return [dict(r) for r in rows]


@pytest.fixture
def freeze_now(monkeypatch):
    """Patch impact_tracker._now to a controllable clock so poll-cycle
    tests are deterministic."""
    state = {"now": datetime.now(timezone.utc)}

    def _set(dt: datetime) -> None:
        state["now"] = dt

    monkeypatch.setattr(it_module, "_now", lambda: state["now"])
    return _set


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_owned_table_and_indexes_exist_after_connect():
    d = DB.ephemeral()
    await d.connect()
    try:
        cur = await d.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='price_impacts'"
        )
        assert await cur.fetchone() is not None
        await cur.close()
        for idx in (
            "idx_price_impacts_trade_id",
            "idx_price_impacts_market_id",
            "idx_price_impacts_last_polled",
        ):
            cur = await d.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name=?",
                (idx,),
            )
            assert await cur.fetchone() is not None, f"missing index {idx}"
            await cur.close()
    finally:
        await d.close()


async def test_init_requires_clob(db):
    with pytest.raises(RuntimeError, match="clob"):
        ImpactTrackerStage(
            StageDeps(db=db, bus=_FakeBus(), config=_Config(), clob=None)
        )


async def test_handle_reject_writes_no_row(db):
    await _seed_market(db)
    await _seed_trade(db, trade_id=1, signal_id=1)
    stage = _make_stage(db, _FakeCLOB())
    await stage.handle({
        "trade_id": None,
        "decision_id": 1,
        "outcome": "reject",
        "reason": "similarity_floor",
        "is_tracer": False,
        "tracer_id": None,
    })
    assert await _impacts(db) == []


async def test_handle_execute_inserts_anchor_row(db):
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(seconds=10)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      direction="YES", mid_price=0.42, size_usdc=100.0)
    stage = _make_stage(db, _FakeCLOB())
    await stage.handle({
        "trade_id": 1,
        "decision_id": 1,
        "outcome": "execute",
        "reason": "ok",
        "is_tracer": False,
        "tracer_id": None,
    })
    rows = await _impacts(db)
    assert len(rows) == 1
    r = rows[0]
    assert r["trade_id"] == 1
    assert r["market_id"] == "m1"
    assert r["token_id"] == "tY"
    assert r["direction"] == "YES"
    assert r["t0_iso"] == decided.isoformat()
    assert r["entry_mid"] == pytest.approx(0.42)
    for col in (
        "price_t60s", "price_t300s", "price_t900s", "price_t1800s", "price_t3600s",
        "delta_t60s", "edge_realised_t60s", "last_polled_at",
    ):
        assert r[col] is None
    assert r["is_tracer"] == 0
    assert r["created_at"] is not None


async def test_handle_idempotent_on_repeat(db):
    await _seed_market(db)
    await _seed_trade(db, trade_id=1, signal_id=1)
    stage = _make_stage(db, _FakeCLOB())
    payload = {
        "trade_id": 1, "decision_id": 1, "outcome": "execute",
        "reason": "ok", "is_tracer": False, "tracer_id": None,
    }
    await stage.handle(payload)
    await stage.handle(payload)
    rows = await _impacts(db)
    assert len(rows) == 1


async def test_poll_fills_t60s_when_elapsed(db, freeze_now):
    await _seed_market(db)
    t0 = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=t0,
                      direction="YES", mid_price=0.30)
    clob = _FakeCLOB(mids={"tY": 0.55})
    stage = _make_stage(db, clob)

    # Insert the anchor row at t0
    freeze_now(t0 + timedelta(seconds=5))
    await stage.handle({
        "trade_id": 1, "decision_id": 1, "outcome": "execute",
        "reason": "ok", "is_tracer": False, "tracer_id": None,
    })

    # Now jump forward 61s — the +60s bucket is due, the +300s one is not.
    freeze_now(t0 + timedelta(seconds=61))
    filled = await stage.run_one_poll_cycle()
    assert filled == 1
    r = (await _impacts(db))[0]
    assert r["price_t60s"] == pytest.approx(0.55)
    assert r["delta_t60s"] == pytest.approx(0.55 - 0.30)
    assert r["edge_realised_t60s"] == pytest.approx(0.55 - 0.30)
    assert r["price_t300s"] is None
    assert r["last_polled_at"] is not None


async def test_poll_skips_buckets_not_yet_due(db, freeze_now):
    await _seed_market(db)
    t0 = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=t0,
                      direction="YES", mid_price=0.30)
    clob = _FakeCLOB(mids={"tY": 0.55})
    stage = _make_stage(db, clob)

    freeze_now(t0 + timedelta(seconds=5))
    await stage.handle({
        "trade_id": 1, "decision_id": 1, "outcome": "execute",
        "reason": "ok", "is_tracer": False, "tracer_id": None,
    })
    # Only 30s elapsed: nothing due yet.
    freeze_now(t0 + timedelta(seconds=30))
    filled = await stage.run_one_poll_cycle()
    assert filled == 0
    assert clob.calls == []  # CLOB not even queried
    r = (await _impacts(db))[0]
    assert r["price_t60s"] is None


async def test_poll_skips_buckets_already_populated(db, freeze_now):
    await _seed_market(db)
    t0 = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=t0,
                      direction="YES", mid_price=0.30)
    clob = _FakeCLOB(mids={"tY": 0.55})
    stage = _make_stage(db, clob)

    freeze_now(t0 + timedelta(seconds=5))
    await stage.handle({
        "trade_id": 1, "decision_id": 1, "outcome": "execute",
        "reason": "ok", "is_tracer": False, "tracer_id": None,
    })
    # First sweep: fill t60s.
    freeze_now(t0 + timedelta(seconds=61))
    await stage.run_one_poll_cycle()
    assert len(clob.calls) == 1
    # Second sweep at same time: t60s already filled, nothing else due.
    await stage.run_one_poll_cycle()
    assert len(clob.calls) == 1  # no additional CLOB call


async def test_clob_returning_none_leaves_bucket_null(db, freeze_now):
    await _seed_market(db)
    t0 = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=t0,
                      direction="YES", mid_price=0.30)
    clob = _FakeCLOB(mids={})  # token has no mid
    stage = _make_stage(db, clob)

    freeze_now(t0 + timedelta(seconds=5))
    await stage.handle({
        "trade_id": 1, "decision_id": 1, "outcome": "execute",
        "reason": "ok", "is_tracer": False, "tracer_id": None,
    })
    freeze_now(t0 + timedelta(seconds=61))
    filled = await stage.run_one_poll_cycle()
    assert filled == 0  # no fills, but no exception either
    r = (await _impacts(db))[0]
    assert r["price_t60s"] is None


async def test_edge_realised_sign_for_yes_direction(db, freeze_now):
    # YES direction + price rises (0.30 → 0.55) ⇒ edge_realised positive.
    await _seed_market(db)
    t0 = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=t0,
                      direction="YES", mid_price=0.30)
    clob = _FakeCLOB(mids={"tY": 0.55})
    stage = _make_stage(db, clob)

    freeze_now(t0 + timedelta(seconds=5))
    await stage.handle({
        "trade_id": 1, "decision_id": 1, "outcome": "execute",
        "reason": "ok", "is_tracer": False, "tracer_id": None,
    })
    freeze_now(t0 + timedelta(seconds=61))
    await stage.run_one_poll_cycle()
    r = (await _impacts(db))[0]
    assert r["delta_t60s"] == pytest.approx(0.25)
    assert r["edge_realised_t60s"] == pytest.approx(0.25)


async def test_edge_realised_sign_for_no_direction(db, freeze_now):
    # NO direction + NO-token price rises (0.30 → 0.55) ⇒ edge_realised
    # positive (the NO bettor wants the NO leg to climb).
    await _seed_market(db)
    t0 = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=t0,
                      direction="NO", mid_price=0.30)
    clob = _FakeCLOB(mids={"tN": 0.15})  # NO token fell
    stage = _make_stage(db, clob)

    freeze_now(t0 + timedelta(seconds=5))
    await stage.handle({
        "trade_id": 1, "decision_id": 1, "outcome": "execute",
        "reason": "ok", "is_tracer": False, "tracer_id": None,
    })
    freeze_now(t0 + timedelta(seconds=61))
    await stage.run_one_poll_cycle()
    r = (await _impacts(db))[0]
    # delta is unsigned: -0.15.
    assert r["delta_t60s"] == pytest.approx(-0.15)
    # NO direction + price fell ⇒ edge_realised = -(delta) = +0.15.
    assert r["edge_realised_t60s"] == pytest.approx(0.15)


async def test_delta_computed_correctly(db, freeze_now):
    await _seed_market(db)
    t0 = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=t0,
                      direction="YES", mid_price=0.42)
    clob = _FakeCLOB(mids={"tY": 0.61})
    stage = _make_stage(db, clob)
    freeze_now(t0 + timedelta(seconds=5))
    await stage.handle({
        "trade_id": 1, "decision_id": 1, "outcome": "execute",
        "reason": "ok", "is_tracer": False, "tracer_id": None,
    })
    # Jump past the +5m bucket so two buckets fire at once.
    freeze_now(t0 + timedelta(seconds=305))
    filled = await stage.run_one_poll_cycle()
    assert filled == 2
    r = (await _impacts(db))[0]
    assert r["delta_t60s"] == pytest.approx(0.61 - 0.42)
    assert r["delta_t300s"] == pytest.approx(0.61 - 0.42)


async def test_max_age_hours_cutoff_stops_polling(db, freeze_now):
    await _seed_market(db)
    t0 = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=t0,
                      direction="YES", mid_price=0.30)
    clob = _FakeCLOB(mids={"tY": 0.55})
    # Tight max_age so the row falls off quickly.
    stage = _make_stage(db, clob, cfg=_Config(impact=_ImpactCfg(max_age_hours=1.0)))

    freeze_now(t0 + timedelta(seconds=5))
    await stage.handle({
        "trade_id": 1, "decision_id": 1, "outcome": "execute",
        "reason": "ok", "is_tracer": False, "tracer_id": None,
    })
    # 90 minutes later — past max_age_hours=1 — no polling should happen
    # even though the +60s..+3600s buckets are due.
    freeze_now(t0 + timedelta(minutes=90))
    filled = await stage.run_one_poll_cycle()
    assert filled == 0
    assert clob.calls == []
    r = (await _impacts(db))[0]
    assert r["price_t60s"] is None


async def test_tracer_flag_carried_from_trade(db, freeze_now):
    await _seed_market(db)
    t0 = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=t0,
                      is_tracer=1, tracer_id="tracer-zzz")
    stage = _make_stage(db, _FakeCLOB())
    freeze_now(t0 + timedelta(seconds=5))
    # Payload's is_tracer is intentionally False — the stage reads the
    # trade row, not the payload, for the flag (denormalised invariant).
    await stage.handle({
        "trade_id": 1, "decision_id": 1, "outcome": "execute",
        "reason": "ok", "is_tracer": False, "tracer_id": None,
    })
    r = (await _impacts(db))[0]
    assert r["is_tracer"] == 1
    assert r["tracer_id"] == "tracer-zzz"


async def test_no_direction_reads_no_token(db, freeze_now):
    # Belt-and-braces: ensure the stage picks token_no when direction=NO,
    # not token_yes — a one-character bug here would silently invert sign.
    await _seed_market(db)
    t0 = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=t0,
                      direction="NO", mid_price=0.30)
    clob = _FakeCLOB(mids={"tY": 0.99, "tN": 0.21})
    stage = _make_stage(db, clob)
    freeze_now(t0 + timedelta(seconds=5))
    await stage.handle({
        "trade_id": 1, "decision_id": 1, "outcome": "execute",
        "reason": "ok", "is_tracer": False, "tracer_id": None,
    })
    freeze_now(t0 + timedelta(seconds=61))
    await stage.run_one_poll_cycle()
    assert clob.calls == ["tN"]  # not "tY"
    r = (await _impacts(db))[0]
    assert r["price_t60s"] == pytest.approx(0.21)


# ---------------------------------------------------------------------------
# Cycle 11 — W6 pre-news drift + long-tail buckets
# ---------------------------------------------------------------------------


async def test_pre_news_drift_positive_when_market_drifted_our_way(db):
    # YES trade. Article published at pub_ts. In the 15 min before pub,
    # the YES token's mid drifted UP (0.40 → 0.55) — i.e. someone
    # already knew the news. pre_news_drift_15m should therefore be
    # positive (= +0.15).
    pub = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    pub_ts = int(pub.timestamp())
    await _seed_market(db)
    await _seed_article_and_signal(db, article_id=1, signal_id=1, published_at=pub)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=pub,
                      direction="YES", mid_price=0.55)
    clob = _FakeCLOB(histories={
        "tY": [
            (pub_ts - 3600, 0.30),  # 60m before pub
            (pub_ts - 900,  0.40),  # 15m before pub
            (pub_ts,         0.55),  # at pub
        ],
    })
    stage = _make_stage(db, clob)
    await stage.handle({
        "trade_id": 1, "decision_id": 1, "outcome": "execute",
        "reason": "ok", "is_tracer": False, "tracer_id": None,
    })
    assert clob.history_calls == ["tY"]
    r = (await _impacts(db))[0]
    assert r["pre_news_drift_15m"] == pytest.approx(0.15)  # 0.55 - 0.40
    # 60m drift: 0.55 - 0.30 = 0.25, sign +1.
    assert r["pre_news_drift_60m"] == pytest.approx(0.25)


async def test_pre_news_drift_negative_when_market_drifted_against(db):
    # NO trade. In the 15 min before pub the NO token's mid ROSE
    # (0.40 → 0.55). Since we poll the NO token and the sign rule is
    # `signed = sign * (mid_pub - mid_lookback)` with sign = -1 for NO,
    # the result for a rising NO token is NEGATIVE: -1 * 0.15 = -0.15.
    # That negative correctly signals "movement against a NO bettor"
    # — same convention as the post-trade `edge_realised` columns
    # (NO direction wins when NO token FALLS).
    pub = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    pub_ts = int(pub.timestamp())
    await _seed_market(db)
    await _seed_article_and_signal(db, article_id=1, signal_id=1, published_at=pub)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=pub,
                      direction="NO", mid_price=0.55)
    clob = _FakeCLOB(histories={
        "tN": [
            (pub_ts - 3600, 0.30),  # 60m before pub
            (pub_ts - 900,  0.40),  # 15m before pub
            (pub_ts,         0.55),  # at pub — NO token rose
        ],
    })
    stage = _make_stage(db, clob)
    await stage.handle({
        "trade_id": 1, "decision_id": 1, "outcome": "execute",
        "reason": "ok", "is_tracer": False, "tracer_id": None,
    })
    assert clob.history_calls == ["tN"]
    r = (await _impacts(db))[0]
    # raw_delta_15m = 0.55 - 0.40 = +0.15; sign = -1 ⇒ signed = -0.15.
    assert r["pre_news_drift_15m"] == pytest.approx(-0.15)
    # raw_delta_60m = 0.55 - 0.30 = +0.25; sign = -1 ⇒ signed = -0.25.
    assert r["pre_news_drift_60m"] == pytest.approx(-0.25)


async def test_pre_news_drift_null_when_history_short(db):
    # History only covers the +/- 5 min around publication — neither
    # the 15m nor the 60m lookback can reach a sample, so both pre-
    # news drift columns must be NULL.
    pub = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    pub_ts = int(pub.timestamp())
    await _seed_market(db)
    await _seed_article_and_signal(db, article_id=1, signal_id=1, published_at=pub)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=pub,
                      direction="YES", mid_price=0.55)
    clob = _FakeCLOB(histories={
        "tY": [
            (pub_ts - 60, 0.54),  # only 1 min before pub
            (pub_ts,       0.55),
        ],
    })
    stage = _make_stage(db, clob)
    await stage.handle({
        "trade_id": 1, "decision_id": 1, "outcome": "execute",
        "reason": "ok", "is_tracer": False, "tracer_id": None,
    })
    r = (await _impacts(db))[0]
    assert r["pre_news_drift_15m"] is None
    assert r["pre_news_drift_60m"] is None


async def test_pre_news_drift_null_when_no_history(db):
    # CLOB returns no history at all (e.g. brand-new token). Both
    # columns must be NULL — and the anchor row must still be written.
    pub = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    await _seed_market(db)
    await _seed_article_and_signal(db, article_id=1, signal_id=1, published_at=pub)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=pub,
                      direction="YES", mid_price=0.55)
    clob = _FakeCLOB(histories={})  # no entry for "tY"
    stage = _make_stage(db, clob)
    await stage.handle({
        "trade_id": 1, "decision_id": 1, "outcome": "execute",
        "reason": "ok", "is_tracer": False, "tracer_id": None,
    })
    rows = await _impacts(db)
    assert len(rows) == 1
    r = rows[0]
    assert r["pre_news_drift_15m"] is None
    assert r["pre_news_drift_60m"] is None


async def test_pre_news_drift_null_when_no_signal_link(db):
    # DKG trades have signal_id = NULL — no article join is possible.
    # The stage MUST still write the anchor row, just with both pre-
    # news drift columns NULL and zero history fetches attempted.
    pub = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    await _seed_market(db)
    # Create a trade whose signal_id refers to a non-existent signal —
    # mirrors the DKG path where the FK isn't populated.
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO decisions
                (signal_id, outcome, reason, gate_history, prediction_id,
                 cycle_id, is_tracer, tracer_id, created_at)
            VALUES (NULL, 'execute', 'ok', '[]', 1, 1, 0, NULL,
                    '2026-05-23T00:00:00+00:00')
            """,
        )
        decision_id = cur.lastrowid
        await cur.execute(
            """
            INSERT INTO trades
                (id, signal_id, decision_id, prediction_id, market_id, direction,
                 confidence, similarity, freshness, p_hat, mid_price, edge,
                 kelly_fraction, size_usdc, decision_path, features_snapshot,
                 cycle_id, is_tracer, tracer_id, decided_at)
            VALUES (1, NULL, ?, 1, 'm1', 'YES', 0.7, 0.8, 'BREAKING', 0.7,
                    0.40, 0.1, 0.1, 100.0, 'dkg', '{}', 1, 0, NULL, ?)
            """,
            (decision_id, pub.isoformat()),
        )
    clob = _FakeCLOB()
    stage = _make_stage(db, clob)
    await stage.handle({
        "trade_id": 1, "decision_id": 1, "outcome": "execute",
        "reason": "ok", "is_tracer": False, "tracer_id": None,
    })
    rows = await _impacts(db)
    assert len(rows) == 1
    r = rows[0]
    assert r["pre_news_drift_15m"] is None
    assert r["pre_news_drift_60m"] is None
    # No history fetch attempted when published_at is unavailable.
    assert clob.history_calls == []


async def test_long_tail_buckets_populated_at_their_offsets(db, freeze_now):
    # Trade fires; advance clock past +24h with mid set; expect every
    # bucket (60s through 86400s) to fill in a single sweep.
    await _seed_market(db)
    t0 = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=t0,
                      direction="YES", mid_price=0.30)
    clob = _FakeCLOB(mids={"tY": 0.65})
    stage = _make_stage(db, clob)

    freeze_now(t0 + timedelta(seconds=5))
    await stage.handle({
        "trade_id": 1, "decision_id": 1, "outcome": "execute",
        "reason": "ok", "is_tracer": False, "tracer_id": None,
    })
    # Jump past the +24h bucket. Default max_age_hours is 24.0, so we
    # land RIGHT on the boundary — bump by a few minutes to make sure
    # we're inside the cutoff window (now - t0 = 24h + 1m < cutoff).
    freeze_now(t0 + timedelta(hours=24, seconds=60))
    # Bump max_age_hours so the row survives the cutoff check at this
    # advanced clock (cutoff is strict-greater-than).
    stage._cfg = _ImpactCfg(max_age_hours=48.0)
    filled = await stage.run_one_poll_cycle()
    # All 9 buckets fill in one sweep.
    assert filled == 9
    r = (await _impacts(db))[0]
    for bucket_s in (60, 300, 900, 1800, 3600, 7200, 14400, 28800, 86400):
        col = f"price_t{bucket_s}s"
        assert r[col] == pytest.approx(0.65), f"bucket {col} not filled"
        assert r[f"delta_t{bucket_s}s"] == pytest.approx(0.35)
        assert r[f"edge_realised_t{bucket_s}s"] == pytest.approx(0.35)


async def test_max_age_hours_default_24():
    # The production config default lifted from 2.0 → 24.0 in cycle 11
    # so the +24h long-tail bucket has time to fire.
    from app.config import ImpactSection
    section = ImpactSection()
    assert section.max_age_hours == 24.0


async def test_long_tail_columns_exist_in_schema():
    # Belt-and-braces: every new bucket column landed via the
    # ADD-COLUMN migrations and is queryable.
    d = DB.ephemeral()
    await d.connect()
    try:
        cur = await d.conn.execute("PRAGMA table_info(price_impacts)")
        cols = {row["name"] for row in await cur.fetchall()}
        await cur.close()
        for bucket_s in (7200, 14400, 28800, 86400):
            for prefix in ("price_t", "delta_t", "edge_realised_t"):
                col = f"{prefix}{bucket_s}s"
                assert col in cols, f"missing column {col}"
        for col in ("pre_news_drift_15m", "pre_news_drift_60m"):
            assert col in cols, f"missing column {col}"
    finally:
        await d.close()

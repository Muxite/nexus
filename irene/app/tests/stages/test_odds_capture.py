"""OddsCaptureStage tests.

Strategy mirrors `test_impact_tracker.py`:
- Real in-memory DB (`DB.ephemeral()`); seed an `explorations` row with
  raw SQL.
- Fake CLOB: `get_price_history` + `get_top_of_book` return whatever the
  test stuffs in.
- Drive a single iteration of the poll loop via the public
  `run_one_poll_cycle()` — no asyncio.sleep, no real background task.
- "Now" is controlled by patching `app.stages.odds_capture._now`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

import pytest

import app.main  # noqa: F401  — cycles DDL
import app.stages.explorer  # noqa: F401  — owns explorations
import app.stages.odds_capture  # noqa: F401  — owns odds_observations
from app.db import DB
from app.deps import StageDeps
from app.stages import odds_capture as oc_module
from app.stages.odds_capture import OddsCaptureStage


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeBus:
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
    histories: dict = field(default_factory=dict)
    books: dict = field(default_factory=dict)
    # If set, get_top_of_book returns this regardless of token.
    fixed_book: Optional[tuple[float, float]] = None
    history_calls: list = field(default_factory=list)
    book_calls: list = field(default_factory=list)

    async def get_price_history(
        self,
        token_id: str,
        *,
        interval: str = "1h",
        fidelity: int = 10,
    ):
        self.history_calls.append(token_id)
        return self.histories.get(token_id)

    async def get_top_of_book(
        self, token_id: str
    ) -> Optional[tuple[float, float]]:
        self.book_calls.append(token_id)
        if self.fixed_book is not None:
            return self.fixed_book
        return self.books.get(token_id)


@dataclass
class _ExplorerCfg:
    score_floor: float = 0.5
    relatedness_model: str = "google/gemini-2.5-flash"
    relatedness_max_article_chars: int = 800
    relatedness_max_output_tokens: int = 80
    odds_poll_secs: int = 15
    capture_post_secs: int = 1200  # 20 min
    capture_post_offsets_secs: list = field(
        default_factory=lambda: [300, 600, 1200]
    )
    capture_pre_secs: int = 600    # 10 min
    capture_pre_interval: str = "1d"
    capture_pre_fidelity: int = 5


@dataclass
class _Config:
    explorer: _ExplorerCfg = field(default_factory=_ExplorerCfg)


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


T0 = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)


async def _seed_exploration(
    db: DB,
    *,
    exploration_id: int = 1,
    article_id: int = 1,
    market_condition_id: str = "cond-1",
    token_yes: str = "ty",
    token_no: str = "tn",
    t0: datetime = T0,
    capture_end_at: Optional[datetime] = None,
    capture_status: str = "pending",
    cycle_id: int = 1,
) -> None:
    if capture_end_at is None:
        capture_end_at = t0 + timedelta(seconds=1200)
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO explorations
                (id, article_id, event_id, market_condition_id,
                 token_yes, token_no, t0_iso,
                 dkg_final_score, cosine_similarity, freshness,
                 breaking_score, article_tier,
                 market_volume_24h, market_spread_t0, market_mid_t0,
                 hours_to_expiry,
                 llm_related, llm_confidence, llm_reason, llm_prompt_hash,
                 capture_status, capture_end_at,
                 cycle_id, is_tracer, tracer_id, created_at)
            VALUES (?, ?, NULL, ?, ?, ?, ?,
                    0.85, 0.6, 'BREAKING',
                    0.8, 'mainstream',
                    1000.0, 0.02, 0.5,
                    24.0,
                    1, 0.9, 'ok', 'sha',
                    ?, ?,
                    ?, 0, NULL, ?)
            """,
            (
                exploration_id, article_id, market_condition_id,
                token_yes, token_no, t0.isoformat(),
                capture_status, capture_end_at.isoformat(),
                cycle_id, t0.isoformat(),
            ),
        )


def _make_stage(db, cfg: Optional[_Config] = None, *, clob=None) -> OddsCaptureStage:
    deps = StageDeps(
        db=db,
        bus=_FakeBus(),
        config=cfg or _Config(),
        clob=clob if clob is not None else _FakeCLOB(),
    )
    return OddsCaptureStage(deps)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_owned_table_exists_after_connect(db):
    cur = await db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name='odds_observations'"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row is not None


async def test_init_requires_clob(db):
    with pytest.raises(RuntimeError, match="clob"):
        OddsCaptureStage(StageDeps(
            db=db, bus=_FakeBus(), config=_Config(), clob=None,
        ))


async def test_handle_writes_pre_news_history(db, monkeypatch):
    # History returns 3 points: one BEFORE the 10-min window (should be
    # filtered out), one INSIDE the window, one EXACTLY at t0.
    t0_ts = int(T0.timestamp())
    clob = _FakeCLOB(histories={
        "ty": [
            (t0_ts - 1200, 0.40),  # 20 min before — outside 10m window
            (t0_ts - 300, 0.45),   # 5 min before
            (t0_ts, 0.47),         # at t0 (offset 0)
        ],
    })
    await _seed_exploration(db)
    stage = _make_stage(db, clob=clob)
    # Freeze "now" to t0 so the active flag is meaningful.
    monkeypatch.setattr(oc_module, "_now", lambda: T0)

    await stage.handle({
        "exploration_id": 1,
        "article_id": 1,
        "market_condition_id": "cond-1",
        "token_yes": "ty",
        "token_no": "tn",
        "t0_iso": T0.isoformat(),
        "capture_end_at": (T0 + timedelta(seconds=1200)).isoformat(),
    })

    cur = await db.conn.execute(
        "SELECT source, offset_seconds, mid FROM odds_observations "
        "ORDER BY offset_seconds"
    )
    rows = await cur.fetchall()
    await cur.close()
    # Only the in-window samples persist.
    assert len(rows) == 2
    assert all(r["source"] == "history" for r in rows)
    assert [int(r["offset_seconds"]) for r in rows] == [-300, 0]
    assert [float(r["mid"]) for r in rows] == [0.45, 0.47]

    # Exploration moved to active and is on the watchlist.
    cur = await db.conn.execute(
        "SELECT capture_status FROM explorations WHERE id=1"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row["capture_status"] == "active"
    assert 1 in stage._watch


async def test_handle_with_no_history_still_activates(db, monkeypatch):
    clob = _FakeCLOB(histories={})  # missing → None
    await _seed_exploration(db)
    stage = _make_stage(db, clob=clob)
    monkeypatch.setattr(oc_module, "_now", lambda: T0)

    await stage.handle({
        "exploration_id": 1,
        "article_id": 1,
        "market_condition_id": "cond-1",
        "token_yes": "ty",
        "token_no": "tn",
        "t0_iso": T0.isoformat(),
        "capture_end_at": (T0 + timedelta(seconds=1200)).isoformat(),
    })

    cur = await db.conn.execute("SELECT COUNT(*) FROM odds_observations")
    assert (await cur.fetchone())[0] == 0  # no pre-news rows
    cur = await db.conn.execute(
        "SELECT capture_status FROM explorations WHERE id=1"
    )
    assert (await cur.fetchone())["capture_status"] == "active"
    assert 1 in stage._watch


async def test_run_one_poll_cycle_writes_live_tick(db, monkeypatch):
    clob = _FakeCLOB(books={"ty": (0.46, 0.48)})  # mid 0.47, spread 0.02
    await _seed_exploration(db, capture_status="active")
    stage = _make_stage(db, clob=clob)
    # Manually add to watchlist (simulating an active capture from handle).
    stage._watch[1] = {
        "token_id": "ty",
        "t0_iso": T0.isoformat(),
        "capture_end_at": (T0 + timedelta(seconds=1200)).isoformat(),
        "cycle_id": 1,
    }
    # Advance "now" to t0 + 60s.
    now = T0 + timedelta(seconds=60)
    monkeypatch.setattr(oc_module, "_now", lambda: now)

    await stage.run_one_poll_cycle()

    cur = await db.conn.execute(
        "SELECT source, offset_seconds, mid, best_bid, best_ask, spread "
        "FROM odds_observations"
    )
    rows = await cur.fetchall()
    await cur.close()
    assert len(rows) == 1
    r = rows[0]
    assert r["source"] == "live"
    assert int(r["offset_seconds"]) == 60
    assert float(r["mid"]) == pytest.approx(0.47)
    assert float(r["best_bid"]) == pytest.approx(0.46)
    assert float(r["best_ask"]) == pytest.approx(0.48)
    assert float(r["spread"]) == pytest.approx(0.02)
    # Still active (window has 1140s left).
    assert 1 in stage._watch


async def test_book_unavailable_writes_null_row(db, monkeypatch):
    clob = _FakeCLOB(books={})  # token missing → None
    await _seed_exploration(db, capture_status="active")
    stage = _make_stage(db, clob=clob)
    stage._watch[1] = {
        "token_id": "ty",
        "t0_iso": T0.isoformat(),
        "capture_end_at": (T0 + timedelta(seconds=1200)).isoformat(),
        "cycle_id": 1,
    }
    monkeypatch.setattr(oc_module, "_now", lambda: T0 + timedelta(seconds=30))

    await stage.run_one_poll_cycle()

    cur = await db.conn.execute(
        "SELECT mid, best_bid, best_ask, spread FROM odds_observations"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row is not None
    assert row["mid"] is None
    assert row["best_bid"] is None
    assert row["best_ask"] is None
    assert row["spread"] is None


async def test_poll_after_window_marks_complete(db, monkeypatch):
    clob = _FakeCLOB(books={"ty": (0.5, 0.52)})
    end_at = T0 + timedelta(seconds=1200)
    await _seed_exploration(db, capture_status="active", capture_end_at=end_at)
    stage = _make_stage(db, clob=clob)
    stage._watch[1] = {
        "token_id": "ty",
        "t0_iso": T0.isoformat(),
        "capture_end_at": end_at.isoformat(),
        "cycle_id": 1,
    }
    # Advance past the window.
    monkeypatch.setattr(
        oc_module, "_now", lambda: end_at + timedelta(seconds=5)
    )

    await stage.run_one_poll_cycle()

    # One final snapshot is forced at the window boundary so the terminal
    # checkpoint always has a usable book (offset >= capture_post_secs).
    cur = await db.conn.execute(
        "SELECT offset_seconds, mid, best_bid, best_ask FROM odds_observations"
    )
    rows = await cur.fetchall()
    await cur.close()
    assert len(rows) == 1
    final = rows[0]
    assert final["offset_seconds"] >= 1200
    assert final["mid"] == pytest.approx(0.51)  # (0.5 + 0.52) / 2
    assert final["best_bid"] == pytest.approx(0.5)
    assert final["best_ask"] == pytest.approx(0.52)
    # Exploration graduated.
    cur = await db.conn.execute(
        "SELECT capture_status FROM explorations WHERE id=1"
    )
    assert (await cur.fetchone())["capture_status"] == "complete"
    # Removed from watchlist.
    assert 1 not in stage._watch


async def test_start_rehydrates_in_flight_captures(db, monkeypatch):
    """Restart simulation: explorer wrote two `active` rows before crash;
    one is still in-window, one already past `capture_end_at`. `start()`
    rehydrates the in-flight one and graduates the elapsed one."""
    end_in_future = T0 + timedelta(seconds=1200)
    end_in_past = T0 - timedelta(seconds=60)
    await _seed_exploration(
        db, exploration_id=1, capture_status="active",
        capture_end_at=end_in_future,
    )
    await _seed_exploration(
        db, exploration_id=2, article_id=2,
        market_condition_id="cond-2",
        capture_status="active", capture_end_at=end_in_past,
    )
    stage = _make_stage(db, clob=_FakeCLOB())
    # Freeze "now" so we know which is in-flight vs elapsed.
    monkeypatch.setattr(oc_module, "_now", lambda: T0)

    await stage.start()
    try:
        assert 1 in stage._watch
        assert 2 not in stage._watch  # elapsed → graduated
        # Verify the elapsed row is now `complete`.
        cur = await db.conn.execute(
            "SELECT capture_status FROM explorations WHERE id=2"
        )
        assert (await cur.fetchone())["capture_status"] == "complete"
    finally:
        await stage.stop()


async def test_duplicate_live_tick_at_same_timestamp_is_idempotent(
    db, monkeypatch,
):
    """The UNIQUE(exploration_id, sampled_at, source) index makes the
    poller's INSERT OR IGNORE safe — a replay on the same wall-clock
    second writes one row, not two."""
    clob = _FakeCLOB(books={"ty": (0.46, 0.48)})
    await _seed_exploration(db, capture_status="active")
    stage = _make_stage(db, clob=clob)
    stage._watch[1] = {
        "token_id": "ty",
        "t0_iso": T0.isoformat(),
        "capture_end_at": (T0 + timedelta(seconds=1200)).isoformat(),
        "cycle_id": 1,
    }
    now = T0 + timedelta(seconds=30)
    monkeypatch.setattr(oc_module, "_now", lambda: now)

    await stage.run_one_poll_cycle()
    await stage.run_one_poll_cycle()  # same "now" — duplicate sampled_at

    cur = await db.conn.execute("SELECT COUNT(*) FROM odds_observations")
    assert (await cur.fetchone())[0] == 1

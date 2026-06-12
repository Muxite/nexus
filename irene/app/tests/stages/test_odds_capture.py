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
    # Cycle 29: long-horizon checkpoint loop. Disabled here so existing
    # tests' start() doesn't spawn the second task; the long-horizon
    # tests drive `run_one_long_horizon_cycle()` directly.
    capture_long_enabled: bool = False
    capture_long_offsets_secs: list = field(
        default_factory=lambda: [14_400, 86_400, 172_800, 604_800]
    )
    capture_long_poll_secs: int = 600
    capture_long_max_calls_per_poll: int = 50


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


# ---------------------------------------------------------------------------
# Cycle 29 — long-horizon checkpoints (+4h/+1d/+2d/+7d)
# ---------------------------------------------------------------------------


def test_grace_for_offset_scales_with_horizon():
    from app.stages.odds_capture import _grace_for_offset
    assert _grace_for_offset(14_400) == 1800       # ±30min at +4h
    assert _grace_for_offset(86_400) == 7200       # ±2h at day scale
    assert _grace_for_offset(604_800) == 7200


def test_missing_due_checkpoints_pure():
    from app.stages.odds_capture import _missing_due_checkpoints
    offsets = [14_400, 86_400, 172_800, 604_800]
    t0 = 1_000_000
    # Only +4h and +1d are due; +1d already covered (within grace).
    missing = _missing_due_checkpoints(
        offsets, t0_ts=t0, now_ts=t0 + 100_000,
        existing_offsets=[86_400 + 3600],
    )
    assert missing == [14_400]
    # Nothing due yet.
    assert _missing_due_checkpoints(
        offsets, t0_ts=t0, now_ts=t0 + 100, existing_offsets=[]
    ) == []
    # All due, none covered.
    assert _missing_due_checkpoints(
        offsets, t0_ts=t0, now_ts=t0 + 700_000, existing_offsets=[]
    ) == offsets


def test_nearest_sample_respects_grace():
    from app.stages.odds_capture import _nearest_sample
    history = [(1000, 0.40), (4000, 0.45), (8000, 0.50)]
    assert _nearest_sample(history, target_ts=4200, grace_secs=1000) == (4000, 0.45)
    # Out of grace → None.
    assert _nearest_sample(history, target_ts=20_000, grace_secs=1000) is None
    # Garbled points skipped.
    assert _nearest_sample(
        [("x", "y"), (4000, 0.45)], target_ts=4100, grace_secs=500
    ) == (4000, 0.45)


async def test_long_horizon_cycle_fills_all_checkpoints_once_final_due(
    db, monkeypatch,
):
    """An exploration whose +7d checkpoint is due gets ONE history call
    that fills every checkpoint, then flips long_capture_status='done'."""
    now = T0 + timedelta(seconds=604_800 + 3600)  # final due an hour ago
    monkeypatch.setattr(oc_module, "_now", lambda: now)
    t0_ts = int(T0.timestamp())
    history = [
        (t0_ts + 14_400 + 60, 0.41),
        (t0_ts + 86_400 - 120, 0.47),
        (t0_ts + 172_800 + 600, 0.52),
        (t0_ts + 604_800 - 900, 0.61),
    ]
    clob = _FakeCLOB(histories={"ty": history})
    await _seed_exploration(db, exploration_id=1, capture_status="complete")
    stage = _make_stage(db, clob=clob)

    n = await stage.run_one_long_horizon_cycle()
    assert n == 1
    assert clob.history_calls == ["ty"]

    cur = await db.conn.execute(
        "SELECT offset_seconds, mid, best_bid FROM odds_observations "
        "WHERE exploration_id=1 AND source='checkpoint' ORDER BY offset_seconds"
    )
    rows = [dict(r) for r in await cur.fetchall()]
    await cur.close()
    assert len(rows) == 4
    assert rows[0]["offset_seconds"] == 14_460
    assert rows[-1]["mid"] == pytest.approx(0.61)
    assert all(r["best_bid"] is None for r in rows)  # mid-only, no book

    cur = await db.conn.execute(
        "SELECT long_capture_status, long_capture_done_at "
        "FROM explorations WHERE id=1"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row["long_capture_status"] == "done"
    assert row["long_capture_done_at"] is not None

    # Second pass: nothing to do, no extra history calls.
    n2 = await stage.run_one_long_horizon_cycle()
    assert n2 == 0
    assert clob.history_calls == ["ty"]


async def test_long_horizon_not_picked_before_final_due(db, monkeypatch):
    """Rows whose final checkpoint isn't due yet are not processed —
    prices-history is retroactive, so earlier checkpoints lose nothing
    by waiting (design note in odds_capture.py)."""
    now = T0 + timedelta(seconds=86_400 * 2)  # +2d: final (+7d) not due
    monkeypatch.setattr(oc_module, "_now", lambda: now)
    clob = _FakeCLOB(histories={"ty": [(int(T0.timestamp()) + 14_400, 0.4)]})
    await _seed_exploration(db, exploration_id=1, capture_status="complete")
    stage = _make_stage(db, clob=clob)
    n = await stage.run_one_long_horizon_cycle()
    assert n == 0
    assert clob.history_calls == []
    cur = await db.conn.execute(
        "SELECT long_capture_status FROM explorations WHERE id=1"
    )
    assert (await cur.fetchone())["long_capture_status"] is None
    await cur.close()


async def test_long_horizon_unreachable_history_marks_unavailable(
    db, monkeypatch,
):
    """history=None twice → 'history_unavailable' (audited, not silent)."""
    now = T0 + timedelta(seconds=604_800 + 3600)
    monkeypatch.setattr(oc_module, "_now", lambda: now)
    clob = _FakeCLOB(histories={})  # get_price_history → None
    await _seed_exploration(db, exploration_id=1, capture_status="complete")
    stage = _make_stage(db, clob=clob)
    assert await stage.run_one_long_horizon_cycle() == 1
    cur = await db.conn.execute(
        "SELECT long_capture_status FROM explorations WHERE id=1"
    )
    assert (await cur.fetchone())["long_capture_status"] is None
    await cur.close()
    assert await stage.run_one_long_horizon_cycle() == 1
    cur = await db.conn.execute(
        "SELECT long_capture_status FROM explorations WHERE id=1"
    )
    assert (await cur.fetchone())["long_capture_status"] == "history_unavailable"
    await cur.close()


async def test_long_horizon_backlog_older_than_history_span_expired(
    db, monkeypatch,
):
    """First-deploy backlog rows whose final checkpoint already fell out
    of the ~7d history span are bulk-marked, zero history calls."""
    now = T0 + timedelta(days=20)
    monkeypatch.setattr(oc_module, "_now", lambda: now)
    clob = _FakeCLOB()
    await _seed_exploration(db, exploration_id=1, capture_status="complete")
    stage = _make_stage(db, clob=clob)
    n = await stage.run_one_long_horizon_cycle()
    assert n == 0
    assert clob.history_calls == []
    cur = await db.conn.execute(
        "SELECT long_capture_status FROM explorations WHERE id=1"
    )
    assert (await cur.fetchone())["long_capture_status"] == "history_unavailable"
    await cur.close()


async def test_checkpoint_source_admitted_and_legacy_rows_survive(db):
    """The cycle-29 CHECK rebuild admits source='checkpoint' and keeps
    legacy 'history'/'live' rows byte-identical."""
    await _seed_exploration(db, exploration_id=1)
    async with db.batch() as cur:
        await cur.execute(
            "INSERT INTO odds_observations "
            "(exploration_id, token_id, source, sampled_at, offset_seconds, mid) "
            "VALUES (1, 'ty', 'live', '2026-05-23T12:00:15+00:00', 15, 0.5)"
        )
        await cur.execute(
            "INSERT INTO odds_observations "
            "(exploration_id, token_id, source, sampled_at, offset_seconds, mid) "
            "VALUES (1, 'ty', 'checkpoint', '2026-05-23T16:00:00+00:00', 14400, 0.52)"
        )
    cur = await db.conn.execute(
        "SELECT source, COUNT(*) AS n FROM odds_observations GROUP BY 1 ORDER BY 1"
    )
    rows = {r["source"]: r["n"] for r in await cur.fetchall()}
    await cur.close()
    assert rows == {"checkpoint": 1, "live": 1}
    # 'bogus' still rejected by the rebuilt CHECK.
    import sqlite3 as _sq
    with pytest.raises(Exception) as exc_info:
        async with db.batch() as cur:
            await cur.execute(
                "INSERT INTO odds_observations "
                "(exploration_id, token_id, source, sampled_at, offset_seconds) "
                "VALUES (1, 'ty', 'bogus', '2026-05-23T17:00:00+00:00', 1)"
            )
    assert "CHECK" in str(exc_info.value) or isinstance(
        exc_info.value, _sq.IntegrityError
    )


async def test_cycle29_migration_upgrades_populated_legacy_db(tmp_path):
    """Production upgrade path: a file DB that already ran the PRE-cycle-29
    statements and holds data. Re-connecting with the cycle-29 DDL must
    rebuild the table, keep every legacy row identical, admit
    'checkpoint', and preserve the duplicate-tick uniqueness guard."""
    import sqlite3 as _sq

    from app.db import DB as _DB, _REGISTRY, _sha1

    db_path = str(tmp_path / "legacy.db")
    owner = "app/stages/odds_capture.py"
    stmts = next(m.statements for m in _REGISTRY if m.owner == owner)
    # The first 4 statements are the pre-cycle-29 schema (table + 3
    # indexes); everything after is the cycle-29 rebuild.
    legacy, _rebuild = stmts[:4], stmts[4:]
    assert "CREATE TABLE IF NOT EXISTS odds_observations\n" in legacy[0] or \
        "CREATE TABLE IF NOT EXISTS odds_observations" in legacy[0]

    conn = _sq.connect(db_path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, owner TEXT NOT NULL, "
        "statement_sha TEXT NOT NULL, applied_at TEXT NOT NULL, "
        "UNIQUE(owner, statement_sha))"
    )
    for stmt in legacy:
        conn.execute(stmt)
        conn.execute(
            "INSERT INTO schema_migrations (owner, statement_sha, applied_at) "
            "VALUES (?, ?, '2026-06-01T00:00:00+00:00')",
            (owner, _sha1(stmt)),
        )
    conn.execute(
        "INSERT INTO odds_observations "
        "(id, exploration_id, token_id, source, sampled_at, offset_seconds, "
        " mid, best_bid, best_ask, spread, cycle_id) "
        "VALUES (42, 7, 'ty', 'live', '2026-06-01T01:00:00+00:00', 15, "
        "        0.5, 0.49, 0.51, 0.02, 3)"
    )
    conn.commit()
    conn.close()

    d = _DB(path=db_path, journal_mode="WAL", synchronous="NORMAL")
    await d.connect()  # applies the remaining (cycle-29) statements
    try:
        cur = await d.conn.execute(
            "SELECT id, exploration_id, source, mid, best_bid, cycle_id "
            "FROM odds_observations"
        )
        rows = [dict(r) for r in await cur.fetchall()]
        await cur.close()
        assert rows == [{
            "id": 42, "exploration_id": 7, "source": "live",
            "mid": 0.5, "best_bid": 0.49, "cycle_id": 3,
        }]
        # 'checkpoint' admitted; duplicate-tick guard still enforced.
        async with d.batch() as cur:
            await cur.execute(
                "INSERT INTO odds_observations "
                "(exploration_id, token_id, source, sampled_at, offset_seconds, mid) "
                "VALUES (7, 'ty', 'checkpoint', '2026-06-01T05:00:00+00:00', 14400, 0.52)"
            )
        async with d.batch() as cur:
            await cur.execute(
                "INSERT OR IGNORE INTO odds_observations "
                "(exploration_id, token_id, source, sampled_at, offset_seconds, mid) "
                "VALUES (7, 'ty', 'checkpoint', '2026-06-01T05:00:00+00:00', 14400, 0.99)"
            )
        cur = await d.conn.execute(
            "SELECT COUNT(*) AS n, MIN(mid) AS mid FROM odds_observations "
            "WHERE source='checkpoint'"
        )
        row = await cur.fetchone()
        await cur.close()
        assert row["n"] == 1 and row["mid"] == pytest.approx(0.52)
    finally:
        await d.close()

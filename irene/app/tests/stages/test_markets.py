"""MarketsStage tests — DDL registration, the Gamma poll + upsert path,
disappearance → deactivate, embedding refresh + embedder_id stamping,
MarketIndex top-k, and the poller `handle()` contract.

Everything network-bound is mocked at the stage's helpers / the embedder.
No real httpx, no torch."""

from __future__ import annotations

import json
from typing import Awaitable, Callable, Optional

import numpy as np
import pytest

# Importing app.stages.markets registers the markets DDL with db.py.
import app.stages.markets  # noqa: F401
# Cycle 7: markets-stage auto-close calls write_trade_closure, which
# inserts into trade_closures. Import the closer (DDL owner) and trade
# (decisions/trades DDL) modules here so DB.ephemeral() has the full
# schema set when the auto-close test runs.
import app.stages.closer  # noqa: F401
import app.stages.trade  # noqa: F401
# main.py owns the cycles DDL; importing it keeps DB.connect() symmetrical with
# the rest of the suite even though MarketsStage itself never reads cycle_id.
import app.main  # noqa: F401
from app.db import DB
from app.deps import StageDeps
from app.stages.markets import (
    MarketIndex,
    MarketIndexEmbedderMismatch,
    MarketsStage,
    _parse_gamma_row,
)
from app.topics import TOPIC_MARKETS_REFRESHED


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeBus:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict]] = []

    async def publish(self, topic: str, payload: dict) -> None:
        self.published.append((topic, payload))

    def subscribe(self, topic: str, handler: Callable[[dict], Awaitable[None]]) -> None:
        pass

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


class _FakeEmbedder:
    """Deterministic mock embedder: encodes the first 4 chars of each text
    as a unique unit vector so order is preserved and rows are
    distinguishable in tests."""

    def __init__(self, dim: int = 8, embedder_id: str = "fake@8") -> None:
        self._dim = dim
        self._id = embedder_id
        self.encoded: list[str] = []

    @property
    def embedder_id(self) -> str:
        return self._id

    def embed(self, text: str) -> np.ndarray:
        return self._vec(text)

    def embed_batch(self, texts: list[str]) -> np.ndarray:
        self.encoded.extend(texts)
        if not texts:
            return np.zeros((0, self._dim), dtype=np.float32)
        return np.stack([self._vec(t) for t in texts]).astype(np.float32)

    def _vec(self, text: str) -> np.ndarray:
        v = np.zeros(self._dim, dtype=np.float32)
        # Hash the text into a slot; bump value so different texts → different vecs.
        slot = (sum(ord(c) for c in text) % self._dim)
        v[slot] = 1.0
        return v


def _config(dim: int = 8) -> object:
    class _Emb:
        # Mirrors the EmbedderSection surface MarketsStage reads.
        dim = 8

    class _TradeFeeCfg:
        # Cycle 7: market-resolved auto-close path delegates to the
        # shared `write_trade_closure` helper, which reads fee knobs off
        # cfg.trade. Mirror app/config.py defaults so the tests don't
        # blow up on missing attrs.
        polymarket_fee_rates = {
            "crypto": 0.072,
            "culture": 0.05, "economics": 0.05, "weather": 0.05, "general": 0.05,
            "finance": 0.04, "politics": 0.04, "tech": 0.04, "mentions": 0.04,
            "sports": 0.03,
            "geopolitics": 0.00,
        }
        default_fee_rate = 0.05
        gas_cost_usdc = 0.0

    class _Cfg:
        embedder = _Emb()
        trade = _TradeFeeCfg()

    _Emb.dim = dim
    return _Cfg()


def _gamma_row(condition_id: str, question: str, **overrides) -> dict:
    row = {
        "conditionId": condition_id,
        "question": question,
        "slug": condition_id.lower(),
        "description": f"desc for {condition_id}",
        "category": "Politics",
        "endDate": "2026-12-31T23:59:59Z",
        # Production Gamma returns both `volume24hr` (rolling 24h) and
        # `volume` / `volumeNum` (lifetime). The parser must read the 24h
        # field. We include both with deliberately different values so a
        # regression to reading lifetime would fail the assertion.
        "volume24hr": 1234.5,
        "volume": 999999.0,
        "volumeNum": 999999.0,
        # Production Gamma returns clobTokenIds as a JSON-encoded *string*.
        "clobTokenIds": json.dumps([f"{condition_id}-yes", f"{condition_id}-no"]),
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def db_ready():
    db = DB.ephemeral()
    await db.connect()
    try:
        yield db
    finally:
        await db.close()


@pytest.fixture
def bus() -> _FakeBus:
    return _FakeBus()


@pytest.fixture
def embedder() -> _FakeEmbedder:
    return _FakeEmbedder()


def _make_stage(
    db: DB,
    bus: _FakeBus,
    embedder: _FakeEmbedder,
    dim: int = 8,
    clob=None,
) -> MarketsStage:
    deps = StageDeps(
        db=db, bus=bus, config=_config(dim=dim), embedder=embedder, clob=clob,
    )
    return MarketsStage(deps)


class _FakeCLOB:
    """Minimal CLOB stub for cycle-10 per-market fee_rate fetch and
    cycle-13 per-market min_order_size fetch.

    Records every `get_market_fee_rate` / `get_market_info` call so
    tests can assert first-seen-only semantics. Cycle 13: prefers the
    new unified `get_market_info` when present on the stub; legacy
    tests pinned to `get_market_fee_rate` keep working via the shim."""

    def __init__(
        self,
        fee_by_id: Optional[dict] = None,
        default: Optional[float] = 0.02,
        min_order_size_by_id: Optional[dict] = None,
    ) -> None:
        self._fee_by_id = fee_by_id or {}
        self._default = default
        self._mos_by_id = min_order_size_by_id or {}
        self.fee_calls: list[str] = []
        self.info_calls: list[str] = []

    async def get_market_fee_rate(self, condition_id: str) -> Optional[float]:
        self.fee_calls.append(condition_id)
        if condition_id in self._fee_by_id:
            return self._fee_by_id[condition_id]
        return self._default

    async def get_market_info(self, condition_id: str) -> Optional[dict]:
        self.info_calls.append(condition_id)
        # Mirror clob.py contract: one HTTP, both fields. Each value is
        # independently None when the stub wasn't pre-seeded with it.
        if condition_id in self._fee_by_id:
            fee = self._fee_by_id[condition_id]
        else:
            fee = self._default
        mos = self._mos_by_id.get(condition_id)
        return {"fee_rate": fee, "min_order_size": mos}


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_parse_gamma_row_happy():
    row = _parse_gamma_row(_gamma_row("0xabc", "Will X happen?"))
    assert row is not None
    assert row["condition_id"] == "0xabc"
    assert row["question"] == "Will X happen?"
    assert row["token_yes"] == "0xabc-yes"
    assert row["token_no"] == "0xabc-no"
    assert row["volume_usdc"] == 1234.5
    assert row["category"] == "Politics"


def test_parse_gamma_row_rejects_missing_fields():
    assert _parse_gamma_row({"question": "X"}) is None       # no conditionId
    assert _parse_gamma_row({"conditionId": "0x1"}) is None  # no question


def test_parse_gamma_row_handles_native_clob_list():
    raw = _gamma_row("0xc", "Q")
    raw["clobTokenIds"] = ["a", "b"]  # not stringified
    row = _parse_gamma_row(raw)
    assert row["token_yes"] == "a"
    assert row["token_no"] == "b"


def test_parse_gamma_row_reads_volume24hr_not_lifetime():
    """Regression guard: thin_market gate compares against a 24h threshold,
    so the parser must read Gamma's `volume24hr`, never fall back to the
    `volume` / `volumeNum` lifetime totals."""
    raw = _gamma_row("0xd", "Q")
    # Lifetime is ~810x the 24h figure on this fixture — a fallback would
    # blow the assertion well outside any approx tolerance.
    row = _parse_gamma_row(raw)
    assert row["volume_usdc"] == 1234.5


def test_parse_gamma_row_missing_volume24hr_is_zero_not_lifetime():
    """When `volume24hr` is absent, the parser returns 0 rather than
    silently picking up the lifetime `volume` field."""
    raw = _gamma_row("0xe", "Q")
    del raw["volume24hr"]
    row = _parse_gamma_row(raw)
    assert row["volume_usdc"] == 0.0


def test_parse_gamma_row_surfaces_closed_and_closedtime():
    """Cycle 18: parser surfaces `closed` and `closedTime` so the
    upsert can prefer the platform timestamp over our first-observation
    `now()` stamp."""
    raw = _gamma_row("0xf", "Q")
    raw["closed"] = True
    raw["closedTime"] = "2026-05-25 09:09:00+00"
    row = _parse_gamma_row(raw)
    assert row["closed"] is True
    assert row["closed_time"] == "2026-05-25 09:09:00+00"


def test_parse_gamma_row_default_closed_is_false_when_missing():
    raw = _gamma_row("0xg", "Q")
    row = _parse_gamma_row(raw)
    assert row["closed"] is False
    assert row["closed_time"] is None


async def test_upsert_closed_market_uses_closedtime_and_sets_inactive(
    db_ready, bus, embedder,
):
    """Cycle 18: when Gamma returns closed=true with a closedTime, the
    upsert MUST land active=0 and prefer the platform timestamp over
    our first-observation now(). Fixes the cycle-12 active-flicker bug
    where the resolution re-poll path used to re-set active=1 on already-
    closed markets."""
    stage = _make_stage(db_ready, bus, embedder)
    raw = _gamma_row("0xclosed1", "Will X be closed?")
    raw["closed"] = True
    raw["closedTime"] = "2026-05-25T09:09:00+00:00"
    parsed = _parse_gamma_row(raw)
    await stage._upsert_market(parsed)

    cur = await db_ready.conn.execute(
        "SELECT active, closed_at FROM markets WHERE condition_id='0xclosed1'"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row["active"] == 0
    assert row["closed_at"] == "2026-05-25T09:09:00+00:00"


async def test_upsert_closed_market_without_closedtime_falls_back_to_now(
    db_ready, bus, embedder,
):
    """closed=true with no closedTime → fall back to _now_iso() rather
    than leaving closed_at NULL."""
    stage = _make_stage(db_ready, bus, embedder)
    raw = _gamma_row("0xclosed2", "Q")
    raw["closed"] = True
    # no closedTime
    parsed = _parse_gamma_row(raw)
    await stage._upsert_market(parsed)

    cur = await db_ready.conn.execute(
        "SELECT active, closed_at FROM markets WHERE condition_id='0xclosed2'"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row["active"] == 0
    assert row["closed_at"] is not None
    # Should look like an ISO timestamp, not the literal "now".
    assert "T" in row["closed_at"] or " " in row["closed_at"]


async def test_upsert_preserves_earlier_closedtime_via_coalesce(
    db_ready, bus, embedder,
):
    """If we observed closedTime once and Gamma later transiently drops
    it, the COALESCE in the UPDATE clause must preserve the earlier
    authoritative timestamp."""
    stage = _make_stage(db_ready, bus, embedder)

    # First observation: closed with platform timestamp.
    raw1 = _gamma_row("0xclosed3", "Q")
    raw1["closed"] = True
    raw1["closedTime"] = "2026-05-25T08:00:00+00:00"
    await stage._upsert_market(_parse_gamma_row(raw1))

    # Second observation: still closed but Gamma drops closedTime.
    raw2 = _gamma_row("0xclosed3", "Q")
    raw2["closed"] = True
    # no closedTime — would otherwise stamp now()
    await stage._upsert_market(_parse_gamma_row(raw2))

    cur = await db_ready.conn.execute(
        "SELECT closed_at FROM markets WHERE condition_id='0xclosed3'"
    )
    row = await cur.fetchone()
    await cur.close()
    # COALESCE preserves the original timestamp, not the second-obs now().
    assert row["closed_at"] == "2026-05-25T08:00:00+00:00"


async def test_upsert_does_not_reactivate_closed_market(
    db_ready, bus, embedder,
):
    """Cycle 24 (handoff D): once a market has a `closed_at` stamp,
    a subsequent Gamma observation that reports `closed=false` (the
    cycle-12 active-flicker shape) MUST NOT flip `active` back to 1.
    The SQL CASE in `_upsert_market`'s ON CONFLICT clause forces
    `active=0` whenever `markets.closed_at IS NOT NULL`, completing
    the fix that cycle-18 left half-done (COALESCE preserved
    closed_at but `active = excluded.active` still let it flicker).

    Worst-case witness: trade 40 in handoff A — DKG bought NO on a
    market with `active=1, closed_at='2026-05-23'` because the upsert
    let the two columns drift out of sync."""
    stage = _make_stage(db_ready, bus, embedder)

    # First upsert: market closed with a platform `closedTime`.
    raw1 = _gamma_row("0xFLK1", "Will the flicker test pass?")
    raw1["closed"] = True
    raw1["closedTime"] = "2026-05-23T05:51:21+00:00"
    await stage._upsert_market(_parse_gamma_row(raw1))

    cur = await db_ready.conn.execute(
        "SELECT active, closed_at FROM markets WHERE condition_id='0xFLK1'"
    )
    r = await cur.fetchone()
    await cur.close()
    assert r["active"] == 0
    assert r["closed_at"] == "2026-05-23T05:51:21+00:00"

    # Second upsert: Gamma re-emits the same market with closed=false
    # (the active-flicker shape). Pre-cycle-24 this would have set
    # active=1 while COALESCE kept closed_at populated, leaving the
    # row in the contradictory "open + closed" state.
    raw2 = _gamma_row("0xFLK1", "Will the flicker test pass?")
    raw2["closed"] = False
    await stage._upsert_market(_parse_gamma_row(raw2))

    cur = await db_ready.conn.execute(
        "SELECT active, closed_at FROM markets WHERE condition_id='0xFLK1'"
    )
    r = await cur.fetchone()
    await cur.close()
    # Cycle-24 fix: active stays 0 because closed_at is preserved.
    assert r["active"] == 0, "active should NOT flip back to 1"
    assert r["closed_at"] == "2026-05-23T05:51:21+00:00"


# ---------------------------------------------------------------------------
# DDL registration
# ---------------------------------------------------------------------------


async def test_markets_table_exists_after_connect(db_ready):
    cur = await db_ready.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='markets'"
    )
    assert (await cur.fetchone()) is not None

    cur = await db_ready.conn.execute("PRAGMA table_info(markets)")
    cols = {row["name"] for row in await cur.fetchall()}
    expected = {
        "condition_id", "slug", "question", "description", "category",
        "end_date", "volume_usdc", "token_yes", "token_no",
        "embedding", "embedder_id", "embedding_dim", "embedding_updated_at",
        "active", "closed_at", "first_seen_at", "updated_at",
    }
    assert expected.issubset(cols), f"missing cols: {expected - cols}"


async def test_markets_indexes_exist_after_connect(db_ready):
    cur = await db_ready.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='markets'"
    )
    names = {r["name"] for r in await cur.fetchall()}
    assert "idx_markets_active" in names
    assert "idx_markets_closed_at" in names


# ---------------------------------------------------------------------------
# Stage lifecycle
# ---------------------------------------------------------------------------


async def test_init_requires_embedder(db_ready, bus):
    deps = StageDeps(db=db_ready, bus=bus, config=_config(), embedder=None)
    with pytest.raises(RuntimeError, match="embedder"):
        MarketsStage(deps)


async def test_handle_raises_not_implemented(db_ready, bus, embedder):
    stage = _make_stage(db_ready, bus, embedder)
    with pytest.raises(NotImplementedError):
        await stage.handle({"any": "payload"})


# ---------------------------------------------------------------------------
# Gamma poll happy path + deactivate-on-disappear
# ---------------------------------------------------------------------------


async def test_poll_once_inserts_rows_and_publishes_topic(db_ready, bus, embedder, monkeypatch):
    stage = _make_stage(db_ready, bus, embedder)

    async def fake_fetch(self):
        return [_gamma_row("0x1", "Q1"), _gamma_row("0x2", "Q2")]

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)

    n = await stage.poll_once()
    assert n == 2

    cur = await db_ready.conn.execute(
        "SELECT condition_id, question, active, token_yes FROM markets ORDER BY condition_id"
    )
    rows = await cur.fetchall()
    assert [r["condition_id"] for r in rows] == ["0x1", "0x2"]
    assert all(r["active"] == 1 for r in rows)
    assert rows[0]["token_yes"] == "0x1-yes"

    assert len(bus.published) == 1
    topic, payload = bus.published[0]
    assert topic == TOPIC_MARKETS_REFRESHED
    assert payload["count"] == 2
    assert "refreshed_at" in payload


async def test_disappearing_market_is_deactivated(db_ready, bus, embedder, monkeypatch):
    stage = _make_stage(db_ready, bus, embedder)

    fetched = [_gamma_row("0xA", "QA"), _gamma_row("0xB", "QB")]

    async def fake_fetch(self):
        return list(fetched)

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)
    await stage.poll_once()

    # Now 0xB disappears from the open list.
    fetched.pop()  # drop 0xB
    await stage.poll_once()

    cur = await db_ready.conn.execute(
        "SELECT condition_id, active, closed_at FROM markets ORDER BY condition_id"
    )
    rows = await cur.fetchall()
    by_id = {r["condition_id"]: r for r in rows}
    assert by_id["0xA"]["active"] == 1
    assert by_id["0xA"]["closed_at"] is None
    assert by_id["0xB"]["active"] == 0
    assert by_id["0xB"]["closed_at"] is not None


async def test_disappearing_market_without_uma_status_does_not_auto_close(
    db_ready, bus, embedder, monkeypatch,
):
    """Cycle 11 (replaces cycle-7 conservative-close behavior): when a
    market drops from Gamma's open list with NO UMA resolution status on
    record, the three-state machine is in State 1 — closed but oracle has
    not spoken. Do NOT auto-close at entry_mid. Wait for `umaResolutionStatus`
    to advance to `'resolved'` (which surfaces via `outcomePrices`) before
    realising PnL. Plans/resolution-oracle.md §"State 1"."""
    stage = _make_stage(db_ready, bus, embedder)

    # Seed via Gamma: two markets so we can drop one without triggering
    # the empty-fetch safeguard in _deactivate_missing.
    fetched = [_gamma_row("0xZ", "Will Z happen?"), _gamma_row("0xY", "Will Y happen?")]

    async def fake_fetch(self):
        return list(fetched)

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)
    await stage.poll_once()
    await _seed_open_trade_async(
        db_ready, trade_id=100, market_id="0xZ", direction="YES",
        decided_at="2026-05-25T00:02:00+00:00", cycle_id=1,
    )

    # Now 0xZ disappears from Gamma's open list (but 0xY stays so the
    # empty-fetch safeguard doesn't short-circuit). No UMA status was ever
    # observed for 0xZ — State 1, must NOT auto-close.
    from app.cycle import cycle_scope
    fetched.pop(0)  # drop the first entry = 0xZ
    with cycle_scope(1):
        await stage.poll_once()

    # Trade is still open: no closure written.
    cur = await db_ready.conn.execute(
        "SELECT COUNT(*) AS n FROM trade_closures WHERE trade_id=100"
    )
    assert (await cur.fetchone())["n"] == 0


async def _seed_open_trade_async(
    db,
    *,
    trade_id: int,
    market_id: str,
    direction: str,
    decided_at: str,
    cycle_id: int,
):
    async with db.batch() as cur:
        # Cycle row may already exist across multiple seed calls in one test.
        await cur.execute(
            "INSERT OR IGNORE INTO cycles (id, name, opened_at) "
            "VALUES (?, 'test', '2026-05-25T00:00:00+00:00')",
            (cycle_id,),
        )
        await cur.execute(
            """
            INSERT INTO decisions
                (signal_id, outcome, reason, gate_history, prediction_id,
                 cycle_id, is_tracer, tracer_id, created_at)
            VALUES (NULL, 'execute', 'ok', '[]', 1, ?, 0, NULL,
                    '2026-05-25T00:01:00+00:00')
            """,
            (cycle_id,),
        )
        decision_id = cur.lastrowid
        await cur.execute(
            """
            INSERT INTO trades
                (id, signal_id, decision_id, prediction_id, market_id,
                 direction, confidence, similarity, freshness, p_hat,
                 mid_price, edge, kelly_fraction, size_usdc, decision_path,
                 features_snapshot, cycle_id, is_tracer, tracer_id,
                 decided_at, target_mid)
            VALUES (?, NULL, ?, 1, ?, ?, 0.7, 0.8, 'BREAKING',
                    0.7, 0.5, 0.1, 0.1, 100.0, 'mvp', '{}', ?, 0, NULL,
                    ?, 0.7)
            """,
            (trade_id, decision_id, market_id, direction, cycle_id, decided_at),
        )


async def test_first_seen_market_populates_fee_rate_from_clob(
    db_ready, bus, embedder, monkeypatch,
):
    """Cycle 10/13: on first-seen markets the stage calls the CLOB
    (cycle 13: `get_market_info`; falls back to cycle-10
    `get_market_fee_rate` for legacy stubs) and persists `fee_rate`
    into `markets.fee_rate`. Already-populated rows are NOT re-fetched
    (refresh cadence is a separate decision per plans/markets-gamma.md).
    """
    clob = _FakeCLOB(fee_by_id={"0xfee1": 0.02, "0xfee2": 0.10})
    stage = _make_stage(db_ready, bus, embedder, clob=clob)

    async def fake_fetch(self):
        return [_gamma_row("0xfee1", "Q1"), _gamma_row("0xfee2", "Q2")]

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)

    # First poll — both markets are first-seen so both get fetched.
    await stage.poll_once()

    cur = await db_ready.conn.execute(
        "SELECT condition_id, fee_rate FROM markets ORDER BY condition_id"
    )
    rows = await cur.fetchall()
    by_id = {r["condition_id"]: r["fee_rate"] for r in rows}
    assert by_id["0xfee1"] == pytest.approx(0.02)
    assert by_id["0xfee2"] == pytest.approx(0.10)
    # Cycle 13: stage prefers `get_market_info` when the CLOB exposes
    # it (our `_FakeCLOB` does). `fee_calls` from the legacy path
    # stays empty; `info_calls` tracks the unified fetch.
    assert sorted(clob.info_calls) == ["0xfee1", "0xfee2"]
    assert clob.fee_calls == []

    # Second poll — both rows now have non-NULL fee_rate, so no extra
    # fetches fire (first-seen-only semantics, keyed off fee_rate).
    clob.info_calls.clear()
    await stage.poll_once()
    assert clob.info_calls == []


async def test_first_seen_market_populates_min_order_size_from_clob(
    db_ready, bus, embedder, monkeypatch,
):
    """Cycle 13: on first-seen markets the stage calls
    `clob.get_market_info(conditionId)` and persists BOTH `fee_rate`
    AND `min_order_size` into the markets row in one HTTP. The new
    column is in SHARES (not USDC) per the platform contract — the
    trade-stage size gate handles the shares→USDC conversion.

    Backwards-compat with cycle 10: a market whose response omits
    `minimum_order_size` (older markets in the live 2026-05-25 survey)
    leaves the column NULL; fee_rate still lands."""
    clob = _FakeCLOB(
        fee_by_id={"0xmos1": 0.02, "0xmos2": 0.10},
        min_order_size_by_id={"0xmos1": 5.0, "0xmos2": 15.0},
    )
    stage = _make_stage(db_ready, bus, embedder, clob=clob)

    async def fake_fetch(self):
        return [_gamma_row("0xmos1", "Q1"), _gamma_row("0xmos2", "Q2")]

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)
    await stage.poll_once()

    cur = await db_ready.conn.execute(
        "SELECT condition_id, fee_rate, min_order_size "
        "FROM markets ORDER BY condition_id"
    )
    rows = await cur.fetchall()
    by_id = {r["condition_id"]: r for r in rows}
    assert by_id["0xmos1"]["fee_rate"] == pytest.approx(0.02)
    assert by_id["0xmos1"]["min_order_size"] == pytest.approx(5.0)
    assert by_id["0xmos2"]["fee_rate"] == pytest.approx(0.10)
    assert by_id["0xmos2"]["min_order_size"] == pytest.approx(15.0)
    # One HTTP per market populated both columns.
    assert sorted(clob.info_calls) == ["0xmos1", "0xmos2"]
    # Second poll — first-seen-only contract holds; no refetch.
    clob.info_calls.clear()
    await stage.poll_once()
    assert clob.info_calls == []


# ---------------------------------------------------------------------------
# Cycle 15: per-market info refresh cadence
# ---------------------------------------------------------------------------


async def _seed_market_with_fee(
    db: DB,
    condition_id: str,
    *,
    fee_rate: Optional[float],
    fetched_at: Optional[str],
) -> None:
    """Insert a minimal markets row with an explicit fee_rate /
    fee_rate_fetched_at pair. Cycle-15 tests use this to fast-forward
    a row past first-seen without going through `poll_once`."""
    now = "2026-05-25T00:00:00+00:00"
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO markets (
                condition_id, question, slug, description, category,
                end_date, volume_usdc, token_yes, token_no, active,
                first_seen_at, updated_at, fee_rate, fee_rate_fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
            """,
            (
                condition_id, f"Q for {condition_id}", condition_id.lower(),
                "desc", "Politics", "2026-12-31T23:59:59Z",
                1000.0, f"{condition_id}-yes", f"{condition_id}-no",
                now, now, fee_rate, fetched_at,
            ),
        )


async def test_market_info_refreshes_when_stale(
    db_ready, bus, embedder, monkeypatch,
):
    """Cycle 15: a market whose `fee_rate_fetched_at` is older than
    `cfg.markets.market_info_refresh_hours` (default 168h) MUST get
    re-fetched on the next poll, even though `fee_rate IS NOT NULL`.
    On success, `fee_rate_fetched_at` advances to the new `now`."""
    from datetime import datetime, timedelta, timezone

    stale_ts = (
        datetime.now(timezone.utc) - timedelta(hours=192)
    ).isoformat()  # 8 days ago, well past the 168h default
    await _seed_market_with_fee(
        db_ready, "0xstale", fee_rate=0.01, fetched_at=stale_ts,
    )
    # The fetch returns a *different* fee so the refresh is observable.
    clob = _FakeCLOB(
        fee_by_id={"0xstale": 0.05},
        min_order_size_by_id={"0xstale": 7.0},
    )
    stage = _make_stage(db_ready, bus, embedder, clob=clob)

    async def fake_fetch(self):
        return [_gamma_row("0xstale", "Q")]

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)
    await stage.poll_once()

    assert clob.info_calls == ["0xstale"]
    cur = await db_ready.conn.execute(
        "SELECT fee_rate, min_order_size, fee_rate_fetched_at "
        "FROM markets WHERE condition_id='0xstale'"
    )
    row = await cur.fetchone()
    assert row["fee_rate"] == pytest.approx(0.05)
    assert row["min_order_size"] == pytest.approx(7.0)
    # Timestamp advanced — was 8d ago, must now be within the last minute.
    assert row["fee_rate_fetched_at"] is not None
    new_ts = datetime.fromisoformat(
        row["fee_rate_fetched_at"].replace("Z", "+00:00")
    )
    age_seconds = (datetime.now(timezone.utc) - new_ts).total_seconds()
    assert 0 <= age_seconds < 60


async def test_market_info_skips_when_fresh(
    db_ready, bus, embedder, monkeypatch,
):
    """Cycle 15: a market whose `fee_rate_fetched_at` is well within
    the refresh window (1h ago vs 168h default) MUST NOT trigger a
    CLOB fetch — the cadence avoids hammering the CLOB on every poll."""
    from datetime import datetime, timedelta, timezone

    fresh_ts = (
        datetime.now(timezone.utc) - timedelta(hours=1)
    ).isoformat()
    await _seed_market_with_fee(
        db_ready, "0xfresh", fee_rate=0.01, fetched_at=fresh_ts,
    )
    clob = _FakeCLOB(fee_by_id={"0xfresh": 0.99})  # would be obvious if called
    stage = _make_stage(db_ready, bus, embedder, clob=clob)

    async def fake_fetch(self):
        return [_gamma_row("0xfresh", "Q")]

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)
    await stage.poll_once()

    assert clob.info_calls == []
    assert clob.fee_calls == []
    cur = await db_ready.conn.execute(
        "SELECT fee_rate, fee_rate_fetched_at FROM markets "
        "WHERE condition_id='0xfresh'"
    )
    row = await cur.fetchone()
    # Untouched.
    assert row["fee_rate"] == pytest.approx(0.01)
    assert row["fee_rate_fetched_at"] == fresh_ts


async def test_market_info_first_seen_still_works(
    db_ready, bus, embedder, monkeypatch,
):
    """Cycle 15 preserves the cycle-10/13 first-seen contract: a row
    with `fee_rate IS NULL` triggers a fetch regardless of timestamp.
    Here we seed the row with `fee_rate=NULL` but a *fresh* (would-be
    skip) `fetched_at` to prove the NULL-fee path still fires."""
    from datetime import datetime, timedelta, timezone

    fresh_ts = (
        datetime.now(timezone.utc) - timedelta(hours=1)
    ).isoformat()
    await _seed_market_with_fee(
        db_ready, "0xfirst", fee_rate=None, fetched_at=fresh_ts,
    )
    clob = _FakeCLOB(fee_by_id={"0xfirst": 0.03})
    stage = _make_stage(db_ready, bus, embedder, clob=clob)

    async def fake_fetch(self):
        return [_gamma_row("0xfirst", "Q")]

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)
    await stage.poll_once()

    assert clob.info_calls == ["0xfirst"]
    cur = await db_ready.conn.execute(
        "SELECT fee_rate FROM markets WHERE condition_id='0xfirst'"
    )
    row = await cur.fetchone()
    assert row["fee_rate"] == pytest.approx(0.03)


class _NoneReturningCLOB:
    """CLOB stub that always returns None from `get_market_info`,
    simulating any failure mode that lands the helper at None
    (network error, non-2xx, unparseable JSON). Records calls so
    tests can assert the fetch was attempted."""

    def __init__(self) -> None:
        self.info_calls: list[str] = []
        self.fee_calls: list[str] = []

    async def get_market_info(self, condition_id: str) -> Optional[dict]:
        self.info_calls.append(condition_id)
        return None

    async def get_market_fee_rate(self, condition_id: str) -> Optional[float]:
        self.fee_calls.append(condition_id)
        return None


async def test_market_info_failure_does_not_advance_timestamp(
    db_ready, bus, embedder, monkeypatch,
):
    """Cycle 15 fetch-failure discipline: when the CLOB returns None
    (network blip, 5xx, unparseable JSON, etc.) we MUST leave
    `fee_rate_fetched_at` unchanged so the next poll retries instead
    of waiting another full refresh window. For a legacy row whose
    prior value was NULL, that means it stays NULL."""
    await _seed_market_with_fee(
        db_ready, "0xfail", fee_rate=0.02, fetched_at=None,
    )
    clob = _NoneReturningCLOB()
    stage = _make_stage(db_ready, bus, embedder, clob=clob)

    async def fake_fetch(self):
        return [_gamma_row("0xfail", "Q")]

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)
    await stage.poll_once()

    # NULL fetched_at qualified as stale → fetch was attempted.
    assert clob.info_calls == ["0xfail"]
    cur = await db_ready.conn.execute(
        "SELECT fee_rate, fee_rate_fetched_at FROM markets "
        "WHERE condition_id='0xfail'"
    )
    row = await cur.fetchone()
    # fee_rate untouched, fetched_at NOT advanced — next cycle retries.
    assert row["fee_rate"] == pytest.approx(0.02)
    assert row["fee_rate_fetched_at"] is None


async def test_fee_rate_column_stays_null_when_clob_absent(
    db_ready, bus, embedder, monkeypatch,
):
    """When the CLOB client isn't wired (e.g. minimal test deps), the
    stage MUST NOT crash — fee_rate just stays NULL and closer.py
    falls back to the category lookup."""
    stage = _make_stage(db_ready, bus, embedder, clob=None)

    async def fake_fetch(self):
        return [_gamma_row("0xno-clob", "Q")]

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)
    await stage.poll_once()

    cur = await db_ready.conn.execute(
        "SELECT fee_rate FROM markets WHERE condition_id='0xno-clob'"
    )
    row = await cur.fetchone()
    assert row["fee_rate"] is None


async def test_network_error_keeps_loop_alive(db_ready, bus, embedder, monkeypatch, caplog):
    import httpx
    stage = _make_stage(db_ready, bus, embedder)

    async def fake_fetch(self):
        raise httpx.ConnectError("boom")

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)

    n = await stage.poll_once()
    assert n == 0
    # No bus publish on a failed cycle.
    assert bus.published == []
    # No rows inserted.
    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM markets")
    assert (await cur.fetchone())[0] == 0


# ---------------------------------------------------------------------------
# Cycle 13: Gamma Retry-After backoff
# ---------------------------------------------------------------------------


class _ScriptedHttpx:
    """Returns scripted responses in order, recording every call. Each
    scripted entry is a tuple of (status_code, body_json, headers).
    Raises IndexError if the test scripts fewer responses than the helper
    actually makes — which is what we want for "expected exactly N retries".
    """

    def __init__(self, responses: list[tuple[int, object, dict]]) -> None:
        self._responses = list(responses)
        self.calls: list[tuple[str, dict]] = []

    async def get(self, url: str, params=None):
        import httpx
        self.calls.append((url, dict(params or {})))
        status, body, headers = self._responses.pop(0)
        return httpx.Response(
            status, json=body, headers=headers,
            request=httpx.Request("GET", url),
        )

    async def aclose(self):
        pass


async def test_backoff_passes_200_through(db_ready, bus, embedder, monkeypatch):
    stage = _make_stage(db_ready, bus, embedder)
    stage._client = _ScriptedHttpx([(200, [{"ok": True}], {})])  # type: ignore[assignment]

    resp = await stage._gamma_get_with_backoff({"x": "y"})
    assert resp.status_code == 200
    assert len(stage._client.calls) == 1  # type: ignore[attr-defined]


async def test_backoff_retries_on_429_then_succeeds(
    db_ready, bus, embedder, monkeypatch,
):
    """One 429 with Retry-After=0.01 then a 200. Helper should sleep,
    retry, return the 200 — caller never sees the 429."""
    stage = _make_stage(db_ready, bus, embedder)
    stage._client = _ScriptedHttpx([  # type: ignore[assignment]
        (429, {"error": "rate"}, {"Retry-After": "0.01"}),
        (200, [{"ok": True}], {}),
    ])

    slept: list[float] = []
    async def fake_sleep(s):
        slept.append(s)
    monkeypatch.setattr("app.stages.markets.asyncio.sleep", fake_sleep)

    resp = await stage._gamma_get_with_backoff({"x": "y"})
    assert resp.status_code == 200
    assert len(stage._client.calls) == 2  # type: ignore[attr-defined]
    assert len(slept) == 1
    # Sleep should be at LEAST the Retry-After value (jitter adds 0-0.5s).
    assert 0.01 <= slept[0] <= 0.51


async def test_backoff_uses_fallback_when_no_retry_after_header(
    db_ready, bus, embedder, monkeypatch,
):
    """429 without Retry-After header → exponential-backoff fallback fires."""
    stage = _make_stage(db_ready, bus, embedder)
    stage._client = _ScriptedHttpx([  # type: ignore[assignment]
        (429, {"error": "rate"}, {}),  # no Retry-After
        (200, [{"ok": True}], {}),
    ])

    slept: list[float] = []
    async def fake_sleep(s):
        slept.append(s)
    monkeypatch.setattr("app.stages.markets.asyncio.sleep", fake_sleep)

    resp = await stage._gamma_get_with_backoff({"x": "y"})
    assert resp.status_code == 200
    assert len(slept) == 1
    # Fallback initial backoff is 1.0s; jitter brings it to [1.0, 1.5].
    assert 1.0 <= slept[0] <= 1.5


async def test_backoff_gives_up_after_max_retries(
    db_ready, bus, embedder, monkeypatch,
):
    """Persistent 429s: after max_retries we return the 429 so the caller's
    raise_for_status() still fires. Don't loop forever."""
    stage = _make_stage(db_ready, bus, embedder)
    stage._client = _ScriptedHttpx([  # type: ignore[assignment]
        (429, {"error": "rate"}, {"Retry-After": "0.01"}),
        (429, {"error": "rate"}, {"Retry-After": "0.01"}),
        (429, {"error": "rate"}, {"Retry-After": "0.01"}),
        (429, {"error": "rate"}, {"Retry-After": "0.01"}),
    ])

    async def fake_sleep(s):
        pass
    monkeypatch.setattr("app.stages.markets.asyncio.sleep", fake_sleep)

    resp = await stage._gamma_get_with_backoff({"x": "y"}, max_retries=3)
    assert resp.status_code == 429
    # max_retries=3 → up to 4 attempts (initial + 3 retries).
    assert len(stage._client.calls) == 4  # type: ignore[attr-defined]


async def test_backoff_handles_non_numeric_retry_after_header(
    db_ready, bus, embedder, monkeypatch,
):
    """HTTP-date Retry-After (rare from Polymarket) → fall back to delay."""
    stage = _make_stage(db_ready, bus, embedder)
    stage._client = _ScriptedHttpx([  # type: ignore[assignment]
        (429, {}, {"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}),
        (200, [{"ok": True}], {}),
    ])

    slept: list[float] = []
    async def fake_sleep(s):
        slept.append(s)
    monkeypatch.setattr("app.stages.markets.asyncio.sleep", fake_sleep)

    resp = await stage._gamma_get_with_backoff({"x": "y"})
    assert resp.status_code == 200
    # Fell back to 1.0s + jitter.
    assert 1.0 <= slept[0] <= 1.5


# ---------------------------------------------------------------------------
# Embedding refresh
# ---------------------------------------------------------------------------


async def test_refresh_embeddings_writes_embedder_id_and_blob(db_ready, bus, embedder, monkeypatch):
    stage = _make_stage(db_ready, bus, embedder)

    async def fake_fetch(self):
        return [_gamma_row("0x1", "Question one"), _gamma_row("0x2", "Question two")]

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)
    await stage.poll_once()

    n = await stage.refresh_embeddings_once()
    assert n == 2
    assert sorted(embedder.encoded) == ["Question one", "Question two"]

    cur = await db_ready.conn.execute(
        "SELECT condition_id, embedder_id, embedding_dim, embedding "
        "FROM markets ORDER BY condition_id"
    )
    rows = await cur.fetchall()
    assert all(r["embedder_id"] == "fake@8" for r in rows)
    assert all(r["embedding_dim"] == 8 for r in rows)
    assert all(len(r["embedding"]) == 8 * 4 for r in rows)  # 8 floats × 4 bytes


async def test_refresh_skips_rows_already_at_current_embedder_id(db_ready, bus, embedder, monkeypatch):
    stage = _make_stage(db_ready, bus, embedder)

    async def fake_fetch(self):
        return [_gamma_row("0x1", "Q1")]

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)
    await stage.poll_once()

    n_first = await stage.refresh_embeddings_once()
    assert n_first == 1
    embedder.encoded.clear()

    # Second pass: the row is already stamped with the current embedder_id,
    # so the refresh must short-circuit without re-embedding.
    n_second = await stage.refresh_embeddings_once()
    assert n_second == 0
    assert embedder.encoded == []


async def test_refresh_reembeds_rows_with_stale_embedder_id(db_ready, bus, embedder, monkeypatch):
    stage = _make_stage(db_ready, bus, embedder)

    async def fake_fetch(self):
        return [_gamma_row("0x1", "Q1")]

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)
    await stage.poll_once()
    await stage.refresh_embeddings_once()

    # Simulate a model rotation: swap in a fresh embedder with a new id.
    new_emb = _FakeEmbedder(dim=8, embedder_id="rotated@8")
    stage._embedder = new_emb

    n = await stage.refresh_embeddings_once()
    assert n == 1
    assert new_emb.encoded == ["Q1"]

    cur = await db_ready.conn.execute(
        "SELECT embedder_id FROM markets WHERE condition_id='0x1'"
    )
    row = await cur.fetchone()
    assert row["embedder_id"] == "rotated@8"


# ---------------------------------------------------------------------------
# MarketIndex
# ---------------------------------------------------------------------------


async def _populate(db: DB, items: list[tuple[str, np.ndarray, str]], active: int = 1) -> None:
    """Insert rows (condition_id, embedding, embedder_id) into the markets table
    bypassing the stage. Lets MarketIndex tests run without the full poll path."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    async with db.batch() as cur:
        for cid, vec, eid in items:
            await cur.execute(
                """
                INSERT INTO markets (condition_id, question, embedding, embedder_id,
                                     embedding_dim, embedding_updated_at,
                                     active, first_seen_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (cid, f"q-{cid}", vec.astype(np.float32).tobytes(), eid,
                 int(vec.shape[0]), now, active, now, now),
            )


async def test_market_index_topk_ordering(db_ready, embedder):
    # Three unit vectors aligned to distinct axes; query closest to axis 0.
    v0 = np.zeros(8, dtype=np.float32); v0[0] = 1.0
    v1 = np.zeros(8, dtype=np.float32); v1[1] = 1.0
    v2 = np.zeros(8, dtype=np.float32); v2[0] = 0.7; v2[1] = 0.3  # not unit; mid-similarity to v0
    await _populate(
        db_ready,
        [("c0", v0, "fake@8"), ("c1", v1, "fake@8"), ("c2", v2, "fake@8")],
    )
    idx = MarketIndex(embedder, dim=8)
    n = await idx.load(db_ready)
    assert n == 3

    results = idx.topk(v0, k=10, floor=0.0)
    assert [cid for cid, _ in results] == ["c0", "c2", "c1"]
    assert results[0][1] == pytest.approx(1.0)
    assert results[2][1] == pytest.approx(0.0)


async def test_market_index_floor_filters(db_ready, embedder):
    v0 = np.zeros(8, dtype=np.float32); v0[0] = 1.0
    v1 = np.zeros(8, dtype=np.float32); v1[1] = 1.0
    await _populate(db_ready, [("c0", v0, "fake@8"), ("c1", v1, "fake@8")])
    idx = MarketIndex(embedder, dim=8)
    await idx.load(db_ready)

    results = idx.topk(v0, k=10, floor=0.5)
    assert results == [("c0", pytest.approx(1.0))]


async def test_market_index_k_limit(db_ready, embedder):
    vecs = []
    for i in range(5):
        v = np.zeros(8, dtype=np.float32); v[0] = 1.0 - i * 0.1
        vecs.append((f"c{i}", v, "fake@8"))
    await _populate(db_ready, vecs)
    idx = MarketIndex(embedder, dim=8)
    await idx.load(db_ready)

    q = np.zeros(8, dtype=np.float32); q[0] = 1.0
    results = idx.topk(q, k=3, floor=0.0)
    assert len(results) == 3
    assert [cid for cid, _ in results] == ["c0", "c1", "c2"]


async def test_market_index_empty_returns_empty(db_ready, embedder):
    idx = MarketIndex(embedder, dim=8)
    n = await idx.load(db_ready)
    assert n == 0
    q = np.zeros(8, dtype=np.float32); q[0] = 1.0
    assert idx.topk(q, k=5, floor=0.0) == []


async def test_market_index_mixed_embedder_id_raises(db_ready, embedder):
    v0 = np.zeros(8, dtype=np.float32); v0[0] = 1.0
    v1 = np.zeros(8, dtype=np.float32); v1[1] = 1.0
    # Two rows tagged with different embedder_id values — the in-flight
    # rotation state. load() must refuse to mix them.
    await _populate(db_ready, [("c0", v0, "modelA@8"), ("c1", v1, "modelB@8")])
    idx = MarketIndex(embedder, dim=8)
    with pytest.raises(MarketIndexEmbedderMismatch):
        await idx.load(db_ready)


async def test_market_index_skips_inactive_rows(db_ready, embedder):
    v0 = np.zeros(8, dtype=np.float32); v0[0] = 1.0
    v1 = np.zeros(8, dtype=np.float32); v1[1] = 1.0
    await _populate(db_ready, [("c0", v0, "fake@8")], active=1)
    await _populate(db_ready, [("c1", v1, "fake@8")], active=0)
    idx = MarketIndex(embedder, dim=8)
    n = await idx.load(db_ready)
    assert n == 1
    results = idx.topk(v1, k=10, floor=0.0)
    assert [cid for cid, _ in results] == ["c0"]


# ---------------------------------------------------------------------------
# Cycle 11: three-state UMA close machine
# (plans/resolution-oracle.md D1)
# ---------------------------------------------------------------------------


async def _seed_market_row(
    db,
    *,
    condition_id: str,
    category: str = "Politics",
    fee_rate: float = 0.04,
    last_resolution_status=None,
    last_outcome_prices=None,
    uma_proposed_at=None,
    uma_finalized_at=None,
    closed_at=None,
    active: int = 0,
):
    """Seed a markets row directly with the UMA fields the close machine
    keys off. Bypasses the Gamma poll so each test controls exactly one
    market's resolution state. `last_outcome_prices` may be a Python list
    (auto-JSON'd) or a pre-formatted string."""
    now = "2026-05-25T00:00:00+00:00"
    if isinstance(last_outcome_prices, list):
        outcome_json = json.dumps(last_outcome_prices)
    else:
        outcome_json = last_outcome_prices  # None or already a string
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO markets (
                condition_id, question, active, closed_at,
                first_seen_at, updated_at, category, fee_rate,
                last_resolution_status, last_outcome_prices,
                uma_proposed_at, uma_finalized_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                condition_id, f"Q-{condition_id}", active, closed_at,
                now, now, category, fee_rate,
                last_resolution_status, outcome_json,
                uma_proposed_at, uma_finalized_at,
            ),
        )


async def test_close_machine_state1_does_not_close(db_ready, bus, embedder):
    """State 1: market is closed (active=0) but no UMA status recorded yet.
    The 2h UMA challenge window has not even started — auto-closing now
    would book PnL at a stale mid instead of the actual $1/$0 redemption.
    Must NOT write a closure."""
    from app.cycle import cycle_scope
    stage = _make_stage(db_ready, bus, embedder)
    await _seed_market_row(
        db_ready, condition_id="0xS1",
        last_resolution_status=None,
        last_outcome_prices=None,
        closed_at="2026-05-25T00:00:00+00:00",
    )
    await _seed_open_trade_async(
        db_ready, trade_id=201, market_id="0xS1", direction="YES",
        decided_at="2026-05-25T00:02:00+00:00", cycle_id=1,
    )
    with cycle_scope(1):
        n = await stage._close_orphaned_trades_for(["0xS1"])
    assert n == 0
    cur = await db_ready.conn.execute(
        "SELECT COUNT(*) AS n FROM trade_closures WHERE trade_id=201"
    )
    assert (await cur.fetchone())["n"] == 0


async def test_close_machine_state2_records_proposed_at_no_close(
    db_ready, bus, embedder, monkeypatch,
):
    """State 2: `umaResolutionStatus='proposed'`. The Gamma poll has
    already stamped `uma_proposed_at` via the upsert path; the close
    machine sees the row in State 2 and waits (we're inside the 2h
    challenge window). Closure must NOT be written."""
    from app.cycle import cycle_scope
    stage = _make_stage(db_ready, bus, embedder)

    async def fake_fetch(self):
        # Gamma returns the market still in the open list, but with
        # umaResolutionStatus already 'proposed'.
        row = _gamma_row("0xS2", "Q proposed")
        row["umaResolutionStatus"] = "proposed"
        row["resolvedBy"] = "0xORACLE"
        row["acceptingOrders"] = False
        return [row]

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)
    await stage.poll_once()

    # Confirm the upsert path stamped uma_proposed_at on the transition.
    cur = await db_ready.conn.execute(
        "SELECT uma_proposed_at, last_resolution_status "
        "FROM markets WHERE condition_id='0xS2'"
    )
    row = await cur.fetchone()
    assert row["last_resolution_status"] == "proposed"
    assert row["uma_proposed_at"] is not None

    # Seed an open trade and run the close machine — must NOT close.
    await _seed_open_trade_async(
        db_ready, trade_id=202, market_id="0xS2", direction="YES",
        decided_at="2026-05-25T00:02:00+00:00", cycle_id=1,
    )
    with cycle_scope(1):
        n = await stage._close_orphaned_trades_for(["0xS2"])
    assert n == 0
    cur = await db_ready.conn.execute(
        "SELECT COUNT(*) AS n FROM trade_closures WHERE trade_id=202"
    )
    assert (await cur.fetchone())["n"] == 0


async def test_close_machine_state3_resolved_yes_closes_at_one(
    db_ready, bus, embedder,
):
    """State 3 + YES winner: outcomePrices=["1","0"] (Polymarket convention
    — index 0 = YES payout). A YES position must close at exit_mid=1.0,
    realizing full $1 redemption. exit_fee zeroed per market_resolved
    contract (closer.write_trade_closure)."""
    from app.cycle import cycle_scope
    stage = _make_stage(db_ready, bus, embedder)
    await _seed_market_row(
        db_ready, condition_id="0xS3Y",
        last_resolution_status="resolved",
        last_outcome_prices=["1", "0"],
        uma_finalized_at="2026-05-25T01:00:00+00:00",
        closed_at="2026-05-25T00:00:00+00:00",
    )
    await _seed_open_trade_async(
        db_ready, trade_id=203, market_id="0xS3Y", direction="YES",
        decided_at="2026-05-25T00:02:00+00:00", cycle_id=1,
    )
    with cycle_scope(1):
        n = await stage._close_orphaned_trades_for(["0xS3Y"])
    assert n == 1
    cur = await db_ready.conn.execute(
        "SELECT close_reason, exit_mid_price, realized_pnl_usdc, "
        "       entry_fee_usdc, exit_fee_usdc "
        "FROM trade_closures WHERE trade_id=203"
    )
    row = await cur.fetchone()
    assert row["close_reason"] == "market_resolved"
    assert row["exit_mid_price"] == pytest.approx(1.0)
    assert row["exit_fee_usdc"] == pytest.approx(0.0)
    # entry_mid=0.5, size=100 → shares=200. YES wins at 1.0:
    #   gross = 200 * (1.0 - 0.5) = 100
    #   entry_fee = 200 * 0.04 * 0.5 * 0.5 = 2.0
    #   net = 100 - 2.0 - 0 = 98.0
    assert row["entry_fee_usdc"] == pytest.approx(2.0, rel=1e-3)
    assert row["realized_pnl_usdc"] == pytest.approx(98.0, rel=1e-3)


async def test_close_machine_state3_resolved_no_closes_at_zero(
    db_ready, bus, embedder,
):
    """State 3 + NO winner: outcomePrices=["0","1"] (YES payout=0, NO
    payout=1). `exit_mid_price` stored in the closure is always the YES
    side (closer.write_trade_closure convention — the NO sign flip is
    applied inside _compute_gross_pnl, not by the caller). On this
    market: YES position loses, NO position wins."""
    from app.cycle import cycle_scope
    stage = _make_stage(db_ready, bus, embedder)
    await _seed_market_row(
        db_ready, condition_id="0xS3N",
        last_resolution_status="resolved",
        last_outcome_prices=["0", "1"],
        uma_finalized_at="2026-05-25T01:00:00+00:00",
        closed_at="2026-05-25T00:00:00+00:00",
    )
    await _seed_open_trade_async(
        db_ready, trade_id=204, market_id="0xS3N", direction="YES",
        decided_at="2026-05-25T00:02:00+00:00", cycle_id=1,
    )
    # Also seed a NO position on the same market — should also close.
    await _seed_open_trade_async(
        db_ready, trade_id=205, market_id="0xS3N", direction="NO",
        decided_at="2026-05-25T00:03:00+00:00", cycle_id=1,
    )
    with cycle_scope(1):
        n = await stage._close_orphaned_trades_for(["0xS3N"])
    assert n == 2

    cur = await db_ready.conn.execute(
        "SELECT trade_id, close_reason, exit_mid_price, realized_pnl_usdc, "
        "       entry_fee_usdc, exit_fee_usdc "
        "FROM trade_closures WHERE trade_id IN (204, 205) ORDER BY trade_id"
    )
    rows = await cur.fetchall()
    by_id = {r["trade_id"]: r for r in rows}

    # YES position on a NO-winning market — exit_mid (YES side) = 0.
    # _compute_gross_pnl(YES) = size * (exit - entry) / entry
    #   = 100 * (0.0 - 0.5) / 0.5 = -100
    # entry_fee = 200 * 0.04 * 0.5 * 0.5 = 2.0; exit_fee = 0 (market_resolved)
    # net = -100 - 2 = -102
    assert by_id[204]["close_reason"] == "market_resolved"
    assert by_id[204]["exit_mid_price"] == pytest.approx(0.0)
    assert by_id[204]["exit_fee_usdc"] == pytest.approx(0.0)
    assert by_id[204]["realized_pnl_usdc"] == pytest.approx(-102.0, rel=1e-3)

    # NO position on a NO-winning market — exit_mid (YES side) = 0 too,
    # but _compute_gross_pnl flips the sign for NO:
    #   gross = size * (entry - exit) / (1 - entry)
    #         = 100 * (0.5 - 0.0) / 0.5 = +100   (full $1 NO redemption)
    # entry_fee = 200 * 0.04 * 0.5 * 0.5 = 2.0; exit_fee = 0
    # net = 100 - 2 = 98
    assert by_id[205]["close_reason"] == "market_resolved"
    assert by_id[205]["exit_mid_price"] == pytest.approx(0.0)
    assert by_id[205]["exit_fee_usdc"] == pytest.approx(0.0)
    assert by_id[205]["realized_pnl_usdc"] == pytest.approx(98.0, rel=1e-3)


async def test_close_machine_human_review_timeout_logs_warning(
    db_ready, bus, embedder, caplog,
):
    """State 1 with age > resolution_timeout_hours: emit RESOLUTION_TIMEOUT
    WARNING and STILL do NOT auto-close (the trade is left open for human
    review — better than guessing at a $1/$0 outcome we can't verify)."""
    import logging
    from app.cycle import cycle_scope
    stage = _make_stage(db_ready, bus, embedder)
    # Patch the timeout to a tiny value so we don't have to fake "now".
    stage._config.markets = type("MarketsCfg", (), {"resolution_timeout_hours": 0.001})()
    # closed_at well in the past so we breach the timeout.
    await _seed_market_row(
        db_ready, condition_id="0xST",
        last_resolution_status=None,
        closed_at="2020-01-01T00:00:00+00:00",
    )
    await _seed_open_trade_async(
        db_ready, trade_id=206, market_id="0xST", direction="YES",
        decided_at="2020-01-01T00:02:00+00:00", cycle_id=1,
    )
    with caplog.at_level(logging.WARNING, logger="app.stages.markets"):
        with cycle_scope(1):
            n = await stage._close_orphaned_trades_for(["0xST"])
    assert n == 0
    # Trade still open.
    cur = await db_ready.conn.execute(
        "SELECT COUNT(*) AS n FROM trade_closures WHERE trade_id=206"
    )
    assert (await cur.fetchone())["n"] == 0
    # And a tagged warning was logged.
    assert any(
        "RESOLUTION_TIMEOUT" in rec.getMessage() and "trade_id=206" in rec.getMessage()
        for rec in caplog.records
    )


async def test_close_machine_idempotent_on_finalized_row(db_ready, bus, embedder):
    """Re-running the close machine on an already-finalized market must
    NOT double-write. trade_closures.trade_id UNIQUE enforces this at the
    DB level; the second call should report 0 new closures."""
    from app.cycle import cycle_scope
    stage = _make_stage(db_ready, bus, embedder)
    await _seed_market_row(
        db_ready, condition_id="0xSI",
        last_resolution_status="resolved",
        last_outcome_prices=["1", "0"],
        uma_finalized_at="2026-05-25T01:00:00+00:00",
        closed_at="2026-05-25T00:00:00+00:00",
    )
    await _seed_open_trade_async(
        db_ready, trade_id=207, market_id="0xSI", direction="YES",
        decided_at="2026-05-25T00:02:00+00:00", cycle_id=1,
    )
    with cycle_scope(1):
        n_first = await stage._close_orphaned_trades_for(["0xSI"])
        n_second = await stage._close_orphaned_trades_for(["0xSI"])
    assert n_first == 1
    assert n_second == 0
    cur = await db_ready.conn.execute(
        "SELECT COUNT(*) AS n FROM trade_closures WHERE trade_id=207"
    )
    assert (await cur.fetchone())["n"] == 1


# ---------------------------------------------------------------------------
# Parser additions for cycle 11
# ---------------------------------------------------------------------------


def test_parse_gamma_row_extracts_uma_fields_native_list():
    """outcomePrices as a native list (some Gamma responses)."""
    raw = _gamma_row("0xUMA1", "Q")
    raw["umaResolutionStatus"] = "resolved"
    raw["resolvedBy"] = "0xORACLE"
    raw["acceptingOrders"] = False
    raw["outcomePrices"] = ["1", "0"]
    row = _parse_gamma_row(raw)
    assert row["uma_resolution_status"] == "resolved"
    assert row["resolved_by"] == "0xORACLE"
    assert row["accepting_orders"] is False
    assert row["outcome_prices"] == ["1", "0"]


def test_parse_gamma_row_extracts_uma_fields_json_string():
    """outcomePrices as a JSON-encoded string (matches the clobTokenIds
    quirk — Polymarket stringifies some array fields)."""
    raw = _gamma_row("0xUMA2", "Q")
    raw["umaResolutionStatus"] = "proposed"
    raw["outcomePrices"] = json.dumps(["0", "1"])
    row = _parse_gamma_row(raw)
    assert row["uma_resolution_status"] == "proposed"
    assert row["outcome_prices"] == ["0", "1"]


def test_parse_gamma_row_handles_missing_uma_fields():
    """Open markets typically have no UMA fields at all — parser must
    return Nones / empty list, not blow up."""
    row = _parse_gamma_row(_gamma_row("0xUMA3", "Q"))
    assert row["uma_resolution_status"] is None
    assert row["resolved_by"] is None
    assert row["accepting_orders"] is None
    assert row["outcome_prices"] == []


# ---------------------------------------------------------------------------
# Cycle 12: resolution re-poll orchestration
# (plans/resolution-oracle.md — closes the "orchestration gap" flagged in
# cycle 11; without this task the three-state machine only sees State 1.)
# ---------------------------------------------------------------------------


def _recent_closed_at() -> str:
    """ISO timestamp ~1 minute ago, so the closed_at filter in the
    poll query treats the market as 'still inside the human-review
    window' for the default timeout (168h)."""
    from datetime import datetime, timedelta, timezone
    return (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()


def _ancient_closed_at() -> str:
    """ISO timestamp years in the past — used to assert the poll
    walks PAST markets that breached the human-review window."""
    return "2020-01-01T00:00:00+00:00"


async def test_resolved_poll_state2_records_proposed_at_but_does_not_close(
    db_ready, bus, embedder, monkeypatch,
):
    """Cycle 12: closed market with no UMA status initially. The
    re-poll fetches the Gamma row, which now carries
    `umaResolutionStatus='proposed'`. The upsert stamps
    `uma_proposed_at`; the close-machine call sees State 2 and waits
    — no trade_closures row written."""
    from app.cycle import cycle_scope
    stage = _make_stage(db_ready, bus, embedder)
    await _seed_market_row(
        db_ready, condition_id="0xRP2",
        last_resolution_status=None,
        closed_at=_recent_closed_at(),
        active=0,
    )
    await _seed_open_trade_async(
        db_ready, trade_id=301, market_id="0xRP2", direction="YES",
        decided_at="2026-05-25T00:02:00+00:00", cycle_id=1,
    )

    async def fake_fetch_single(self, condition_id):
        assert condition_id == "0xRP2"
        row = _gamma_row("0xRP2", "Q proposed via re-poll")
        row["umaResolutionStatus"] = "proposed"
        row["resolvedBy"] = "0xORACLE"
        row["acceptingOrders"] = False
        return row

    monkeypatch.setattr(MarketsStage, "_fetch_single_market", fake_fetch_single)

    with cycle_scope(1):
        n = await stage.poll_resolved_markets_once()
    assert n == 1

    # uma_proposed_at stamped by the upsert path (cycle-11 transition stamp).
    cur = await db_ready.conn.execute(
        "SELECT uma_proposed_at, last_resolution_status "
        "FROM markets WHERE condition_id='0xRP2'"
    )
    row = await cur.fetchone()
    assert row["last_resolution_status"] == "proposed"
    assert row["uma_proposed_at"] is not None

    # No closure: State 2 waits.
    cur = await db_ready.conn.execute(
        "SELECT COUNT(*) AS n FROM trade_closures WHERE trade_id=301"
    )
    assert (await cur.fetchone())["n"] == 0


async def test_resolved_poll_state3_resolved_yes_closes(
    db_ready, bus, embedder, monkeypatch,
):
    """Cycle 12: closed market with no UMA status initially. The
    re-poll fetches Gamma and sees `umaResolutionStatus='resolved'`
    with `outcomePrices=["1","0"]`. The state machine closes the
    trade at the YES payout (exit_mid=1.0). Verifies the orchestration
    end-to-end."""
    from app.cycle import cycle_scope
    stage = _make_stage(db_ready, bus, embedder)
    await _seed_market_row(
        db_ready, condition_id="0xRP3",
        last_resolution_status=None,
        closed_at=_recent_closed_at(),
        active=0,
    )
    await _seed_open_trade_async(
        db_ready, trade_id=302, market_id="0xRP3", direction="YES",
        decided_at="2026-05-25T00:02:00+00:00", cycle_id=1,
    )

    async def fake_fetch_single(self, condition_id):
        assert condition_id == "0xRP3"
        row = _gamma_row("0xRP3", "Q resolved via re-poll")
        row["umaResolutionStatus"] = "resolved"
        row["outcomePrices"] = ["1", "0"]
        row["acceptingOrders"] = False
        return row

    monkeypatch.setattr(MarketsStage, "_fetch_single_market", fake_fetch_single)

    with cycle_scope(1):
        n = await stage.poll_resolved_markets_once()
    assert n == 1

    # Closure recorded at exit_mid=1.0 (YES payout from outcomePrices[0]).
    cur = await db_ready.conn.execute(
        "SELECT close_reason, exit_mid_price "
        "FROM trade_closures WHERE trade_id=302"
    )
    row = await cur.fetchone()
    assert row is not None, "expected a trade_closures row for trade 302"
    assert row["close_reason"] == "market_resolved"
    assert row["exit_mid_price"] == pytest.approx(1.0)

    # uma_finalized_at stamped by the upsert path.
    cur = await db_ready.conn.execute(
        "SELECT uma_finalized_at FROM markets WHERE condition_id='0xRP3'"
    )
    row = await cur.fetchone()
    assert row["uma_finalized_at"] is not None


async def test_resolved_poll_skips_markets_past_timeout(
    db_ready, bus, embedder, monkeypatch,
):
    """Cycle 12: a closed market whose `closed_at` is older than
    `cfg.markets.resolution_timeout_hours` is OUT of the human-review
    window — leave it for a human, do NOT refetch (saves Gamma quota,
    and matches the cycle-11 contract that we never auto-close past
    the timeout). The poll query must filter it out so
    `_fetch_single_market` is never called."""
    from app.cycle import cycle_scope
    stage = _make_stage(db_ready, bus, embedder)
    # Default timeout is 168h; closed_at in 2020 is well past that.
    await _seed_market_row(
        db_ready, condition_id="0xRPT",
        last_resolution_status=None,
        closed_at=_ancient_closed_at(),
        active=0,
    )
    await _seed_open_trade_async(
        db_ready, trade_id=303, market_id="0xRPT", direction="YES",
        decided_at="2020-01-01T00:02:00+00:00", cycle_id=1,
    )

    calls: list[str] = []

    async def fake_fetch_single(self, condition_id):
        calls.append(condition_id)
        return None  # would no-op anyway; the assertion is on `calls`

    monkeypatch.setattr(MarketsStage, "_fetch_single_market", fake_fetch_single)

    with cycle_scope(1):
        n = await stage.poll_resolved_markets_once()
    assert n == 0
    # Critically: the timed-out market is NOT refetched.
    assert calls == []


async def test_resolved_poll_only_walks_open_trades(
    db_ready, bus, embedder, monkeypatch,
):
    """Cycle 12: a closed-but-not-finalized market with NO open
    trade on it does NOT get refetched. This bounds Gamma quota
    usage to markets we actually care about (don't waste budget on
    the long-tail universe). Two markets are seeded: one with an
    open trade, one without; only the former should be fetched."""
    from app.cycle import cycle_scope
    stage = _make_stage(db_ready, bus, embedder)
    await _seed_market_row(
        db_ready, condition_id="0xRPA",  # has an open trade
        last_resolution_status=None,
        closed_at=_recent_closed_at(),
        active=0,
    )
    await _seed_market_row(
        db_ready, condition_id="0xRPB",  # no open trade — should NOT be fetched
        last_resolution_status=None,
        closed_at=_recent_closed_at(),
        active=0,
    )
    await _seed_open_trade_async(
        db_ready, trade_id=304, market_id="0xRPA", direction="YES",
        decided_at="2026-05-25T00:02:00+00:00", cycle_id=1,
    )

    calls: list[str] = []

    async def fake_fetch_single(self, condition_id):
        calls.append(condition_id)
        return None

    monkeypatch.setattr(MarketsStage, "_fetch_single_market", fake_fetch_single)

    with cycle_scope(1):
        n = await stage.poll_resolved_markets_once()
    assert n == 1
    assert calls == ["0xRPA"]


async def test_resolved_poll_handles_gamma_failure_gracefully(
    db_ready, bus, embedder, monkeypatch, caplog,
):
    """Cycle 12: a single-market Gamma fetch returning None (network
    error, unknown id, malformed JSON) does NOT crash the poll loop.
    The close machine still runs against the existing DB state, and
    a subsequent market in the same cycle is processed normally."""
    import logging
    from app.cycle import cycle_scope
    stage = _make_stage(db_ready, bus, embedder)
    # Seed two markets so we can verify the cycle continues past the
    # failing one. Both have open trades and recent closed_at.
    await _seed_market_row(
        db_ready, condition_id="0xRPF1",  # fetch will return None
        last_resolution_status=None,
        closed_at=_recent_closed_at(),
        active=0,
    )
    await _seed_market_row(
        db_ready, condition_id="0xRPF2",  # fetch will succeed (still proposed)
        last_resolution_status=None,
        closed_at=_recent_closed_at(),
        active=0,
    )
    await _seed_open_trade_async(
        db_ready, trade_id=305, market_id="0xRPF1", direction="YES",
        decided_at="2026-05-25T00:02:00+00:00", cycle_id=1,
    )
    await _seed_open_trade_async(
        db_ready, trade_id=306, market_id="0xRPF2", direction="YES",
        decided_at="2026-05-25T00:03:00+00:00", cycle_id=1,
    )

    async def fake_fetch_single(self, condition_id):
        if condition_id == "0xRPF1":
            return None  # simulate fetch failure
        row = _gamma_row(condition_id, "Q recoverable")
        row["umaResolutionStatus"] = "proposed"
        return row

    monkeypatch.setattr(MarketsStage, "_fetch_single_market", fake_fetch_single)

    with caplog.at_level(logging.WARNING, logger="app.stages.markets"):
        with cycle_scope(1):
            n = await stage.poll_resolved_markets_once()

    # Both markets evaluated (the failing fetch is non-fatal — the
    # close machine still runs against the existing DB row).
    assert n == 2
    # Neither trade closed: 0xRPF1 stayed in State 1 (no Gamma update);
    # 0xRPF2 transitioned to State 2 (proposed) but that's a wait state.
    cur = await db_ready.conn.execute(
        "SELECT COUNT(*) AS n FROM trade_closures WHERE trade_id IN (305, 306)"
    )
    assert (await cur.fetchone())["n"] == 0
    # 0xRPF2 still got its uma_proposed_at stamped from the successful
    # refetch — proves the loop did not bail out after the first failure.
    cur = await db_ready.conn.execute(
        "SELECT uma_proposed_at FROM markets WHERE condition_id='0xRPF2'"
    )
    assert (await cur.fetchone())["uma_proposed_at"] is not None

"""MarketsStage cycle-26 events-endpoint switchover tests.

Covers the three modes of `_fetch_universe`:
- `use_events_endpoint=False` → legacy `/markets` path, with the new
  `events_seen_in_last_poll=0` stamp applied to every row.
- `use_events_endpoint=True, dual_read=False` → single-source events,
  rows carry `polymarket_event_id` / `polymarket_event_slug`.
- `use_events_endpoint=True, dual_read=True` → both endpoints fetched;
  union by condition_id, events wins on conflict, `/markets`-only rows
  get NULL event_id and `events_seen_in_last_poll=0`.

Plus the 422-as-end-of-pagination guard on `_fetch_all_gamma_events`,
mirroring the existing markets-endpoint regression test.
"""

from __future__ import annotations

import json
from typing import Awaitable, Callable

import httpx
import pytest

# Importing the markets module registers the markets DDL with db.py.
import app.stages.markets  # noqa: F401
import app.stages.closer  # noqa: F401
import app.stages.trade  # noqa: F401
import app.main  # noqa: F401
from app.db import DB
from app.deps import StageDeps
from app.stages.markets import MarketsStage


# ---------------------------------------------------------------------------
# Fakes (mirror test_markets.py shapes; kept local to avoid cross-file imports)
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
    def __init__(self) -> None:
        self._id = "fake@8"

    @property
    def embedder_id(self) -> str:
        return self._id

    def embed(self, text):  # noqa: ARG002
        import numpy as np
        return np.zeros(8, dtype=np.float32)

    def embed_batch(self, texts):
        import numpy as np
        if not texts:
            return np.zeros((0, 8), dtype=np.float32)
        return np.zeros((len(texts), 8), dtype=np.float32)


def _config_with_markets(*, use_events: bool, dual_read: bool) -> object:
    """Minimal config carrying a `markets` section that mirrors the real
    `MarketsSection`. Other sections kept inline (mirroring test_markets.py's
    minimal `_Cfg`) so the stage's wiring assumptions hold."""

    class _Emb:
        dim = 8

    class _Markets:
        use_events_endpoint = use_events
        events_endpoint_dual_read = dual_read

    class _TradeFeeCfg:
        polymarket_fee_rates = {
            "crypto": 0.072, "culture": 0.05, "economics": 0.05, "weather": 0.05,
            "general": 0.05, "finance": 0.04, "politics": 0.04, "tech": 0.04,
            "mentions": 0.04, "sports": 0.03, "geopolitics": 0.00,
        }
        default_fee_rate = 0.05
        gas_cost_usdc = 0.0

    class _Cfg:
        embedder = _Emb()
        markets = _Markets()
        trade = _TradeFeeCfg()

    return _Cfg()


def _market_row(condition_id: str, question: str) -> dict:
    """Raw Gamma `/markets`-shape row — identical to what the nested
    `markets: [...]` array under `/events` returns per element."""
    return {
        "conditionId": condition_id,
        "question": question,
        "slug": condition_id.lower(),
        "description": f"desc for {condition_id}",
        "category": "Politics",
        "endDate": "2026-12-31T23:59:59Z",
        "volume24hr": 1234.5,
        "volume": 999999.0,
        "clobTokenIds": json.dumps([f"{condition_id}-yes", f"{condition_id}-no"]),
    }


def _event_wrapper(event_id: str, slug: str, markets: list[dict]) -> dict:
    return {"id": event_id, "slug": slug, "markets": markets}


@pytest.fixture
async def db_ready():
    db = DB.ephemeral()
    await db.connect()
    try:
        yield db
    finally:
        await db.close()


def _make_stage(db: DB, bus, embedder, cfg) -> MarketsStage:
    deps = StageDeps(db=db, bus=bus, config=cfg, embedder=embedder, clob=None)
    return MarketsStage(deps)


# ---------------------------------------------------------------------------
# Mode 1: use_events_endpoint=False — legacy /markets path, stamped events_seen=0
# ---------------------------------------------------------------------------


async def test_fetch_universe_markets_only_stamps_events_seen_zero(
    db_ready, monkeypatch,
):
    bus = _FakeBus()
    cfg = _config_with_markets(use_events=False, dual_read=True)
    stage = _make_stage(db_ready, bus, _FakeEmbedder(), cfg)

    async def fake_fetch(self):
        return [_market_row("0xA", "QA"), _market_row("0xB", "QB")]

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)

    n = await stage.poll_once()
    assert n == 2

    cur = await db_ready.conn.execute(
        """
        SELECT condition_id, polymarket_event_id, polymarket_event_slug,
               events_seen_in_last_poll
        FROM markets ORDER BY condition_id
        """
    )
    rows = await cur.fetchall()
    assert len(rows) == 2
    for r in rows:
        assert r["polymarket_event_id"] is None
        assert r["polymarket_event_slug"] is None
        assert r["events_seen_in_last_poll"] == 0


# ---------------------------------------------------------------------------
# Mode 2: events only — single-source via /events
# ---------------------------------------------------------------------------


async def test_fetch_universe_events_only_populates_event_metadata(
    db_ready, monkeypatch,
):
    bus = _FakeBus()
    cfg = _config_with_markets(use_events=True, dual_read=False)
    stage = _make_stage(db_ready, bus, _FakeEmbedder(), cfg)

    async def fake_events_fetch(self):
        return [
            ({"id": "evt-1", "slug": "election-2026"}, _market_row("0xA", "QA")),
            ({"id": "evt-1", "slug": "election-2026"}, _market_row("0xB", "QB")),
            ({"id": "evt-2", "slug": "btc-eoy"}, _market_row("0xC", "QC")),
        ]

    async def boom_markets(self):
        # Single-source mode should not hit `/markets` at all.
        raise AssertionError("markets endpoint must not be called in single-source events mode")

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma_events", fake_events_fetch)
    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", boom_markets)

    n = await stage.poll_once()
    assert n == 3

    cur = await db_ready.conn.execute(
        """
        SELECT condition_id, polymarket_event_id, polymarket_event_slug,
               events_seen_in_last_poll
        FROM markets ORDER BY condition_id
        """
    )
    rows = await cur.fetchall()
    by_cid = {r["condition_id"]: r for r in rows}
    assert by_cid["0xA"]["polymarket_event_id"] == "evt-1"
    assert by_cid["0xA"]["polymarket_event_slug"] == "election-2026"
    assert by_cid["0xA"]["events_seen_in_last_poll"] == 1
    assert by_cid["0xC"]["polymarket_event_id"] == "evt-2"


# ---------------------------------------------------------------------------
# Mode 3: dual-read — both endpoints, events wins, markets-only stamped 0
# ---------------------------------------------------------------------------


async def test_fetch_universe_dual_read_unions_events_winning(
    db_ready, monkeypatch,
):
    """events has A, B, C; markets has B, C, D. After merge:
    - A: events-only → polymarket_event_id populated, events_seen=1
    - B, C: in both → events_id wins, events_seen=1
    - D: markets-only → polymarket_event_id NULL, events_seen=0
    """
    bus = _FakeBus()
    cfg = _config_with_markets(use_events=True, dual_read=True)
    stage = _make_stage(db_ready, bus, _FakeEmbedder(), cfg)

    async def fake_events_fetch(self):
        return [
            ({"id": "evt-1", "slug": "s-1"}, _market_row("0xA", "QA")),
            ({"id": "evt-1", "slug": "s-1"}, _market_row("0xB", "QB")),
            ({"id": "evt-2", "slug": "s-2"}, _market_row("0xC", "QC")),
        ]

    async def fake_markets_fetch(self):
        return [
            _market_row("0xB", "QB"),
            _market_row("0xC", "QC"),
            _market_row("0xD", "QD"),
        ]

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma_events", fake_events_fetch)
    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_markets_fetch)

    n = await stage.poll_once()
    assert n == 4  # union {A, B, C, D}

    cur = await db_ready.conn.execute(
        """
        SELECT condition_id, polymarket_event_id, polymarket_event_slug,
               events_seen_in_last_poll
        FROM markets ORDER BY condition_id
        """
    )
    rows = await cur.fetchall()
    by_cid = {r["condition_id"]: r for r in rows}

    assert by_cid["0xA"]["polymarket_event_id"] == "evt-1"
    assert by_cid["0xA"]["events_seen_in_last_poll"] == 1

    assert by_cid["0xB"]["polymarket_event_id"] == "evt-1"
    assert by_cid["0xB"]["events_seen_in_last_poll"] == 1

    assert by_cid["0xC"]["polymarket_event_id"] == "evt-2"
    assert by_cid["0xC"]["events_seen_in_last_poll"] == 1

    assert by_cid["0xD"]["polymarket_event_id"] is None
    assert by_cid["0xD"]["polymarket_event_slug"] is None
    assert by_cid["0xD"]["events_seen_in_last_poll"] == 0


# ---------------------------------------------------------------------------
# `_fetch_all_gamma_events` 422 guard (mirrors the markets-endpoint test)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Phase 2d: _sync_ws_subscriptions integration with a fake pool
# ---------------------------------------------------------------------------


class _FakePool:
    """Minimal ClobWsPool stand-in. Captures subscribe/unsubscribe
    deltas so the test can assert against the desired-vs-current diff."""

    def __init__(self) -> None:
        self.current: set[str] = set()
        self.subscribe_calls: list[set[str]] = []
        self.unsubscribe_calls: list[set[str]] = []

    def subscribed_set(self) -> set[str]:
        return set(self.current)

    async def subscribe(self, token_ids: set[str]) -> None:
        self.subscribe_calls.append(set(token_ids))
        self.current.update(token_ids)

    async def unsubscribe(self, token_ids: set[str]) -> None:
        self.unsubscribe_calls.append(set(token_ids))
        self.current.difference_update(token_ids)

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    def status(self) -> dict:
        return {"clients": 1, "total_subscriptions": len(self.current)}


async def test_sync_ws_subscriptions_initial_subscribe(db_ready, monkeypatch):
    bus = _FakeBus()
    cfg = _config_with_markets(use_events=False, dual_read=False)
    stage = _make_stage(db_ready, bus, _FakeEmbedder(), cfg)
    pool = _FakePool()
    stage._ws_pool = pool  # inject (would normally come via StageDeps)

    async def fake_fetch(self):
        return [_market_row("0xA", "QA"), _market_row("0xB", "QB")]

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)

    await stage.poll_once()

    assert pool.subscribe_calls, "no subscribe frame issued"
    subbed = pool.subscribed_set()
    # Each market has 2 tokens (YES + NO). _market_row produces
    # `<cid>-yes` and `<cid>-no`.
    assert subbed == {"0xA-yes", "0xA-no", "0xB-yes", "0xB-no"}


async def test_sync_ws_subscriptions_unsubscribes_disappeared(db_ready, monkeypatch):
    """A market that drops out of the open list must be unsubscribed
    from the pool — otherwise the cache accumulates stale tokens."""
    bus = _FakeBus()
    cfg = _config_with_markets(use_events=False, dual_read=False)
    stage = _make_stage(db_ready, bus, _FakeEmbedder(), cfg)
    pool = _FakePool()
    stage._ws_pool = pool

    fetched = [_market_row("0xA", "QA"), _market_row("0xB", "QB")]

    async def fake_fetch(self):
        return list(fetched)

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)

    await stage.poll_once()
    assert pool.subscribed_set() == {
        "0xA-yes", "0xA-no", "0xB-yes", "0xB-no",
    }

    # 0xB drops from the active list — _deactivate_missing flips
    # active=0 and the next sync should drop its tokens from the pool.
    fetched.pop()  # drop 0xB
    await stage.poll_once()
    assert pool.subscribed_set() == {"0xA-yes", "0xA-no"}
    # And we issued an unsubscribe frame for the dropped pair.
    assert any(
        {"0xB-yes", "0xB-no"}.issubset(call) for call in pool.unsubscribe_calls
    )


async def test_sync_ws_subscriptions_noop_when_pool_is_none(db_ready, monkeypatch):
    """Feature-flag-off path: deps.ws_pool=None means poll_once must
    silently skip the subscription sync."""
    bus = _FakeBus()
    cfg = _config_with_markets(use_events=False, dual_read=False)
    stage = _make_stage(db_ready, bus, _FakeEmbedder(), cfg)
    assert stage._ws_pool is None  # default

    async def fake_fetch(self):
        return [_market_row("0xA", "QA")]

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)
    # Just shouldn't raise.
    await stage.poll_once()


async def test_fetch_all_gamma_events_preserves_partial_on_422(
    db_ready, monkeypatch,
):
    bus = _FakeBus()
    cfg = _config_with_markets(use_events=True, dual_read=False)
    stage = _make_stage(db_ready, bus, _FakeEmbedder(), cfg)

    class _FakeResp:
        def __init__(self, status: int, body):
            self.status_code = status
            self._body = body

        def raise_for_status(self):
            if 400 <= self.status_code < 600:
                req = httpx.Request("GET", "https://gamma-api.polymarket.com/events")
                raise httpx.HTTPStatusError(
                    f"status {self.status_code}", request=req,
                    response=httpx.Response(self.status_code, request=req),
                )

        def json(self):
            return self._body

    # 100 events per page, each with 1 nested market → 100 markets per page.
    page = [
        _event_wrapper(f"evt-{i:04d}", f"slug-{i:04d}", [_market_row(f"0xE{i:04d}", f"Q{i}")])
        for i in range(100)
    ]

    async def fake_get(self, params, *, max_retries=3, url=None):  # noqa: ARG002
        off = int(params["offset"])
        if off <= 9900:
            return _FakeResp(200, page)
        if off == 10000:
            return _FakeResp(200, page)
        return _FakeResp(422, {"error": "offset out of bounds"})

    monkeypatch.setattr(MarketsStage, "_gamma_get_with_backoff", fake_get)

    result = await stage._fetch_all_gamma_events()

    # 101 successful pages × 100 markets each — preserved across the 422.
    assert len(result) == 10100
    assert all(isinstance(item, tuple) and len(item) == 2 for item in result)
    event_meta, market_row = result[0]
    assert event_meta["id"].startswith("evt-")
    assert market_row["conditionId"].startswith("0xE")


# ---------------------------------------------------------------------------
# Cycle 27 — category from event tags + operative fee_rate from gamma
# ---------------------------------------------------------------------------


def test_category_from_tags_first_match_wins():
    from app.stages.markets import _category_from_tags
    assert _category_from_tags(["exchange", "crypto"]) == "tech"
    assert _category_from_tags(["crypto", "exchange"]) == "crypto"
    assert _category_from_tags(["nba"]) == "sports"


def test_category_from_tags_unmatched_tags_fall_back_to_general():
    from app.stages.markets import _category_from_tags
    assert _category_from_tags(["2025-predictions", "featured"]) == "general"


def test_category_from_tags_none_when_no_tags():
    from app.stages.markets import _category_from_tags
    assert _category_from_tags(None) is None
    assert _category_from_tags([]) is None


def test_parse_gamma_row_derives_category_from_event_tags():
    from app.stages.markets import _parse_gamma_row
    raw = {
        "conditionId": "0xc27", "question": "Q?",
        "clobTokenIds": json.dumps(["t1", "t2"]),
    }
    row = _parse_gamma_row(
        raw, event_meta={"id": "9", "slug": "ev", "tags": ["geopolitics"]}
    )
    assert row is not None
    assert row["category"] == "geopolitics"
    # No event meta (legacy /markets row) → category stays NULL.
    row2 = _parse_gamma_row(raw)
    assert row2["category"] is None


def test_parse_gamma_row_fee_rate_from_fees_enabled():
    from app.stages.markets import _parse_gamma_row
    base = {
        "conditionId": "0xfee", "question": "Q?",
        "clobTokenIds": json.dumps(["t1", "t2"]),
    }
    # Fee-disabled market: operative rate is 0 — an explicit value, not
    # a missing one (it must not fall back to default_fee_rate later).
    row = _parse_gamma_row({**base, "feesEnabled": False, "takerBaseFee": None})
    assert row["fee_rate"] == 0.0
    # Fee-enabled sports market: 1000 bps → 0.10.
    row = _parse_gamma_row({**base, "feesEnabled": True, "takerBaseFee": 1000})
    assert row["fee_rate"] == pytest.approx(0.10)
    # Legacy payload without the fields → unknown (NULL; the upsert
    # COALESCEs so it never clobbers a known rate).
    row = _parse_gamma_row(base)
    assert row["fee_rate"] is None
    # feesEnabled=True but bps missing/garbled → unknown, not 0.
    row = _parse_gamma_row({**base, "feesEnabled": True, "takerBaseFee": "x"})
    assert row["fee_rate"] is None

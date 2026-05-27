"""CLOBClient tests — every recoverable failure mode returns None; happy
path returns the mid-point."""

from __future__ import annotations

import httpx
import pytest

from app.clob import CLOBClient


def _ok_midpoint(mid: float | str) -> dict:
    """Response shape from `GET /midpoint?token_id=...` is `{"mid":"0.545"}`."""
    return {"mid": str(mid)}


class _Mock:
    def __init__(self, *, status: int = 200, body=None, raise_exc=None):
        self.status = status
        self.body = body if body is not None else {}
        self.raise_exc = raise_exc
        self.calls: list[tuple[str, dict | None]] = []

    async def get(self, url, params=None):
        self.calls.append((url, params))
        if self.raise_exc:
            raise self.raise_exc
        r = httpx.Response(self.status, json=self.body, request=httpx.Request("GET", url))
        return r

    async def aclose(self):
        pass


def _install(client: CLOBClient, mock: _Mock) -> None:
    client._client = mock  # type: ignore[assignment]


async def test_get_mid_happy_path():
    c = CLOBClient(base_url="http://x")
    m = _Mock(body=_ok_midpoint(0.545))
    _install(c, m)
    assert await c.get_mid("tk1") == pytest.approx(0.545)
    # Sanity-check we hit /midpoint with token_id in the query, not /book/<id>.
    assert m.calls == [("http://x/midpoint", {"token_id": "tk1"})]


async def test_get_mid_empty_token_returns_none():
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(body=_ok_midpoint(0.5)))
    assert await c.get_mid("") is None


async def test_get_mid_http_error_returns_none():
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(raise_exc=httpx.ConnectError("nope")))
    assert await c.get_mid("tk1") is None


async def test_get_mid_non_2xx_returns_none():
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(status=404, body={}))
    assert await c.get_mid("tk1") is None


async def test_get_mid_missing_field_returns_none():
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(body={"not_mid": "0.5"}))
    assert await c.get_mid("tk1") is None


async def test_get_mid_unparseable_value_returns_none():
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(body={"mid": "n/a"}))
    assert await c.get_mid("tk1") is None


async def test_get_mid_out_of_range_returns_none():
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(body=_ok_midpoint(1.5)))
    assert await c.get_mid("tk1") is None


async def test_close_is_idempotent():
    c = CLOBClient(base_url="http://x")
    await c.close()
    await c.close()


# -------------------------------------------------------------------------
# get_top_of_book — added 2026-05-25 for the wide-spread gate
# -------------------------------------------------------------------------


def _ok_book(bids: list, asks: list) -> dict:
    """Response shape from `GET /book?token_id=...`:
        {"bids":[{"price":"0.40","size":"100"},...],
         "asks":[{"price":"0.60","size":"50"},...]}
    """
    return {
        "bids": [{"price": str(p), "size": str(s)} for p, s in bids],
        "asks": [{"price": str(p), "size": str(s)} for p, s in asks],
    }


async def test_get_top_of_book_happy_path():
    c = CLOBClient(base_url="http://x")
    # Real Polymarket book has bids ascending; best bid = max, best ask = min.
    m = _Mock(body=_ok_book(
        bids=[(0.001, 1000), (0.002, 500), (0.40, 100)],
        asks=[(0.60, 50), (0.61, 80), (0.999, 1)],
    ))
    _install(c, m)
    assert await c.get_top_of_book("tk1") == (0.40, 0.60)
    assert m.calls == [("http://x/book", {"token_id": "tk1"})]


async def test_get_top_of_book_empty_token_returns_none():
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(body=_ok_book([(0.4, 1)], [(0.6, 1)])))
    assert await c.get_top_of_book("") is None


async def test_get_top_of_book_empty_side_returns_none():
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(body=_ok_book([(0.4, 1)], [])))  # no asks
    assert await c.get_top_of_book("tk1") is None


async def test_get_top_of_book_crossed_book_returns_none():
    c = CLOBClient(base_url="http://x")
    # Best bid > best ask shouldn't happen on a sane book; if it does, refuse.
    _install(c, _Mock(body=_ok_book([(0.7, 1)], [(0.5, 1)])))
    assert await c.get_top_of_book("tk1") is None


async def test_get_top_of_book_out_of_range_price_returns_none():
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(body=_ok_book([(0.4, 1)], [(1.5, 1)])))
    assert await c.get_top_of_book("tk1") is None


async def test_get_top_of_book_non_2xx_returns_none():
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(status=404, body={}))
    assert await c.get_top_of_book("tk1") is None


async def test_get_top_of_book_network_error_returns_none():
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(raise_exc=httpx.ConnectError("nope")))
    assert await c.get_top_of_book("tk1") is None


async def test_get_top_of_book_unparseable_price_returns_none():
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(body={"bids": [{"price": "n/a", "size": "1"}],
                             "asks": [{"price": "0.6", "size": "1"}]}))
    assert await c.get_top_of_book("tk1") is None


# -------------------------------------------------------------------------
# get_market_fee_rate — added 2026-05-25 (cycle 10) for per-market fees
# -------------------------------------------------------------------------


def _ok_market(taker_base_fee: int | str | None = 200, **extra) -> dict:
    """Minimal `GET /markets/<conditionId>` response shape. Live 2026-05-25
    probe of 1000 markets returned `taker_base_fee` in {0, 200, 1000} —
    fractional rates {0.00, 0.02, 0.10}."""
    body = {
        "condition_id": "0xabc",
        "question": "Will X happen?",
        "active": True,
        "maker_base_fee": 0,
    }
    if taker_base_fee is not None:
        body["taker_base_fee"] = taker_base_fee
    body.update(extra)
    return body


async def test_get_market_fee_rate_happy_path():
    """200 bps → 0.02 fractional. Hits the path-param /markets/<id> form."""
    c = CLOBClient(base_url="http://x")
    m = _Mock(body=_ok_market(taker_base_fee=200))
    _install(c, m)
    assert await c.get_market_fee_rate("0xabc") == pytest.approx(0.02)
    assert m.calls == [("http://x/markets/0xabc", None)]


async def test_get_market_fee_rate_zero_fee_market():
    """The dominant case (999/1000 markets in the live survey): no fee."""
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(body=_ok_market(taker_base_fee=0)))
    assert await c.get_market_fee_rate("0xabc") == 0.0


async def test_get_market_fee_rate_empty_condition_id_returns_none():
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(body=_ok_market()))
    assert await c.get_market_fee_rate("") is None


async def test_get_market_fee_rate_missing_field_returns_none():
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(body=_ok_market(taker_base_fee=None)))
    assert await c.get_market_fee_rate("0xabc") is None


async def test_get_market_fee_rate_non_2xx_returns_none():
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(status=404, body={}))
    assert await c.get_market_fee_rate("0xabc") is None


async def test_get_market_fee_rate_network_error_returns_none():
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(raise_exc=httpx.ConnectError("nope")))
    assert await c.get_market_fee_rate("0xabc") is None


async def test_get_market_fee_rate_unparseable_value_returns_none():
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(body=_ok_market(taker_base_fee="n/a")))
    assert await c.get_market_fee_rate("0xabc") is None


async def test_get_market_fee_rate_out_of_range_returns_none():
    """A value > 10000 bps would map to > 1.0 fractional — refuse."""
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(body=_ok_market(taker_base_fee=20000)))
    assert await c.get_market_fee_rate("0xabc") is None


# -------------------------------------------------------------------------
# get_market_info — added 2026-05-25 (cycle 13). Returns both `fee_rate`
# (cycle-10 backwards-compat) AND `min_order_size` (cycle-13, SHARES NOT
# USDC) from one HTTP. `get_market_fee_rate` is now a thin shim over this.
# -------------------------------------------------------------------------


async def test_get_market_info_happy_path_returns_both_fields():
    """Live 2026-05-25 cycle-13 verification: a single /markets call
    yields BOTH `taker_base_fee` (bps) and `minimum_order_size` (shares).
    `get_market_info` parses both and returns a dict — one HTTP, two
    columns populated downstream."""
    c = CLOBClient(base_url="http://x")
    m = _Mock(body=_ok_market(taker_base_fee=200, minimum_order_size=5))
    _install(c, m)
    info = await c.get_market_info("0xabc")
    assert info == {"fee_rate": pytest.approx(0.02), "min_order_size": 5.0}
    assert m.calls == [("http://x/markets/0xabc", None)]


async def test_get_market_info_missing_min_order_size_keeps_fee_rate():
    """Older markets may not expose `minimum_order_size`. The fee_rate
    path must keep working — only the missing field returns None inside
    the dict. The shim `get_market_fee_rate` stays useful in that
    regime (regression guard for cycle-10 callers)."""
    c = CLOBClient(base_url="http://x")
    body = _ok_market(taker_base_fee=200)  # no minimum_order_size field
    _install(c, _Mock(body=body))
    info = await c.get_market_info("0xabc")
    assert info == {"fee_rate": pytest.approx(0.02), "min_order_size": None}


async def test_get_market_info_negative_min_order_size_returns_none_for_that_field():
    """Defensive: a negative minimum_order_size is nonsensical — parse
    to None so the gate falls back to the global config floor instead
    of inverting the comparison."""
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(body=_ok_market(taker_base_fee=200, minimum_order_size=-1)))
    info = await c.get_market_info("0xabc")
    assert info is not None
    assert info["min_order_size"] is None
    assert info["fee_rate"] == pytest.approx(0.02)


async def test_get_market_info_empty_condition_id_returns_none():
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(body=_ok_market()))
    assert await c.get_market_info("") is None


async def test_get_market_info_non_2xx_returns_none():
    """Whole-call failure (no usable data) → return None, NOT a dict
    of two Nones. Distinguishes "Polymarket dropped the field" from
    "we never got a response"."""
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(status=404, body={}))
    assert await c.get_market_info("0xabc") is None


async def test_get_market_fee_rate_still_works_after_cycle13_refactor():
    """Backwards-compat guard: `get_market_fee_rate` is now a thin shim
    over `get_market_info`. Cycle-10 callers (markets.py legacy path,
    closer.py fallback) must keep getting the same float."""
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(body=_ok_market(taker_base_fee=1000, minimum_order_size=5)))
    assert await c.get_market_fee_rate("0xabc") == pytest.approx(0.10)


# -------------------------------------------------------------------------
# Cycle 26: LiveMarketCache read-through behavior
# -------------------------------------------------------------------------

import time as _time  # noqa: E402

from app.live_market_cache import LiveMarketCache, LiveTick  # noqa: E402


def _fresh_tick(
    token_id: str = "tk1",
    *,
    mid: float | None = 0.42,
    bid: float | None = 0.40,
    ask: float | None = 0.44,
    age_s: float = 0.0,
) -> LiveTick:
    return LiveTick(
        token_id=token_id,
        mid=mid,
        best_bid=bid,
        best_ask=ask,
        last_trade=None,
        updated_at=_time.monotonic() - age_s,
        sequence=1,
    )


async def test_get_mid_returns_cached_value_without_http_when_fresh():
    cache = LiveMarketCache(stale_after_s=5.0)
    cache.set_tick(_fresh_tick(mid=0.55))
    c = CLOBClient(base_url="http://x", live_cache=cache)
    mock = _Mock(body=_ok_midpoint(0.99))  # would dominate if REST runs
    _install(c, mock)
    assert await c.get_mid("tk1") == pytest.approx(0.55)
    # Cache hit: no HTTP made.
    assert mock.calls == []


async def test_get_mid_falls_through_to_rest_when_cache_stale():
    cache = LiveMarketCache(stale_after_s=0.01)
    cache.set_tick(_fresh_tick(mid=0.55, age_s=5.0))  # well past stale
    c = CLOBClient(base_url="http://x", live_cache=cache)
    mock = _Mock(body=_ok_midpoint(0.30))
    _install(c, mock)
    assert await c.get_mid("tk1") == pytest.approx(0.30)
    assert mock.calls == [("http://x/midpoint", {"token_id": "tk1"})]


async def test_get_mid_falls_through_to_rest_when_cache_missing_token():
    cache = LiveMarketCache(stale_after_s=5.0)
    # No tick for tk1.
    c = CLOBClient(base_url="http://x", live_cache=cache)
    mock = _Mock(body=_ok_midpoint(0.30))
    _install(c, mock)
    assert await c.get_mid("tk1") == pytest.approx(0.30)
    assert len(mock.calls) == 1


async def test_get_mid_prefer_live_false_skips_cache():
    """impact_tracker passes prefer_live=False to ensure REST is the
    source. The cache must be ignored even when it has a fresh tick."""
    cache = LiveMarketCache(stale_after_s=5.0)
    cache.set_tick(_fresh_tick(mid=0.55))
    c = CLOBClient(base_url="http://x", live_cache=cache)
    mock = _Mock(body=_ok_midpoint(0.30))
    _install(c, mock)
    assert await c.get_mid("tk1", prefer_live=False) == pytest.approx(0.30)
    assert mock.calls == [("http://x/midpoint", {"token_id": "tk1"})]


async def test_get_mid_no_cache_attached_is_pure_rest_path():
    """Default constructor (no live_cache=) preserves cycle-10 behavior
    so every existing test wiring keeps working."""
    c = CLOBClient(base_url="http://x")  # no live_cache
    mock = _Mock(body=_ok_midpoint(0.42))
    _install(c, mock)
    assert await c.get_mid("tk1") == pytest.approx(0.42)


async def test_get_mid_cache_tick_with_no_mid_falls_through():
    """A tick that has bid/ask but no mid (e.g. crossed book) must
    not be returned — REST is the safety net."""
    cache = LiveMarketCache(stale_after_s=5.0)
    cache.set_tick(_fresh_tick(mid=None))
    c = CLOBClient(base_url="http://x", live_cache=cache)
    mock = _Mock(body=_ok_midpoint(0.42))
    _install(c, mock)
    assert await c.get_mid("tk1") == pytest.approx(0.42)
    assert len(mock.calls) == 1


# get_top_of_book read-through


async def test_get_top_of_book_returns_cached_when_fresh():
    cache = LiveMarketCache(stale_after_s=5.0)
    cache.set_tick(_fresh_tick(bid=0.40, ask=0.44))
    c = CLOBClient(base_url="http://x", live_cache=cache)
    mock = _Mock(body={"bids": [{"price": "0.10"}], "asks": [{"price": "0.90"}]})
    _install(c, mock)
    assert await c.get_top_of_book("tk1") == (0.40, 0.44)
    assert mock.calls == []


async def test_get_top_of_book_falls_through_when_cache_incomplete():
    cache = LiveMarketCache(stale_after_s=5.0)
    cache.set_tick(_fresh_tick(bid=0.40, ask=None))  # half-populated
    c = CLOBClient(base_url="http://x", live_cache=cache)
    mock = _Mock(body={"bids": [{"price": "0.30"}], "asks": [{"price": "0.50"}]})
    _install(c, mock)
    assert await c.get_top_of_book("tk1") == (0.30, 0.50)


async def test_get_top_of_book_prefer_live_false_skips_cache():
    cache = LiveMarketCache(stale_after_s=5.0)
    cache.set_tick(_fresh_tick(bid=0.40, ask=0.44))
    c = CLOBClient(base_url="http://x", live_cache=cache)
    mock = _Mock(body={"bids": [{"price": "0.30"}], "asks": [{"price": "0.50"}]})
    _install(c, mock)
    assert await c.get_top_of_book("tk1", prefer_live=False) == (0.30, 0.50)
    assert len(mock.calls) == 1

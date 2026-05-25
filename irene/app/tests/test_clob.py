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

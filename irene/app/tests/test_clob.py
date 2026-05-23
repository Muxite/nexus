"""CLOBClient tests — every recoverable failure mode returns None; happy
path returns the mid-point."""

from __future__ import annotations

import httpx
import pytest

from app.clob import CLOBClient


def _ok_book(bid: float, ask: float) -> dict:
    return {"bids": [{"price": str(bid), "size": "100"}],
            "asks": [{"price": str(ask), "size": "100"}]}


class _Mock:
    def __init__(self, *, status: int = 200, body=None, raise_exc=None):
        self.status = status
        self.body = body if body is not None else {}
        self.raise_exc = raise_exc
        self.calls: list[str] = []

    async def get(self, url):
        self.calls.append(url)
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
    _install(c, _Mock(body=_ok_book(0.48, 0.52)))
    assert await c.get_mid("tk1") == pytest.approx(0.50)


async def test_get_mid_empty_token_returns_none():
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(body=_ok_book(0.5, 0.5)))
    assert await c.get_mid("") is None


async def test_get_mid_http_error_returns_none():
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(raise_exc=httpx.ConnectError("nope")))
    assert await c.get_mid("tk1") is None


async def test_get_mid_non_2xx_returns_none():
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(status=503, body={}))
    assert await c.get_mid("tk1") is None


async def test_get_mid_empty_book_returns_none():
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(body={"bids": [], "asks": []}))
    assert await c.get_mid("tk1") is None


async def test_get_mid_malformed_book_returns_none():
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(body={"bids": [{"oops": 1}], "asks": [{"oops": 2}]}))
    assert await c.get_mid("tk1") is None


async def test_get_mid_out_of_range_returns_none():
    c = CLOBClient(base_url="http://x")
    _install(c, _Mock(body=_ok_book(1.5, 2.0)))
    assert await c.get_mid("tk1") is None


async def test_close_is_idempotent():
    c = CLOBClient(base_url="http://x")
    await c.close()
    await c.close()

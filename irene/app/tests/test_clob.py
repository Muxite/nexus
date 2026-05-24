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

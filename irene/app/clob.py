"""
## Specification

**Purpose.** Thin async client for Polymarket's CLOB price endpoints.
Returns the current mid-point price for a given outcome token. Consumed by
`stages/trade.py` (entry-mid for the edge gate) and `stages/closer.py`
(exit-mid for PnL).

**Public interface.**
- `CLOBClient(base_url: str, timeout_seconds: float = 5.0)`
- `await client.get_mid(token_id: str) -> float | None` — `(bid+ask)/2`
  from `/book/<token>`, or `None` on any failure mode the caller can
  reasonably recover from (HTTP error, malformed body, empty book).
- `await client.close()` — closes the underlying `httpx.AsyncClient`.

**Invariants.**
- Returns a number in `[0.0, 1.0]` or `None`. Never raises.
- Lazy connection: the `httpx.AsyncClient` is created on first call so
  importing the module is free.

**Side effects.** HTTP GET to `<base_url>/book/<token_id>`.

**Failure modes.** Network error / non-2xx / unparseable body /
empty book → `None` (caller falls back to whatever default it uses).
A WARNING is logged once per consecutive error class; recovery is
silent.

**Non-goals.** Order submission, signing, websocket streaming, full
orderbook depth. P1 is paper-trade-only; we read mid-point and nothing
more.
"""

from __future__ import annotations

import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://clob.polymarket.com"


class CLOBClient:
    def __init__(self, base_url: str = DEFAULT_BASE_URL, timeout_seconds: float = 5.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_seconds
        self._client: Optional[httpx.AsyncClient] = None

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def get_mid(self, token_id: str) -> Optional[float]:
        if not token_id:
            return None
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._timeout)
        url = f"{self._base_url}/book/{token_id}"
        try:
            resp = await self._client.get(url)
        except httpx.HTTPError as exc:
            logger.warning("clob: get_mid(%s) network error: %s", token_id, exc.__class__.__name__)
            return None
        if resp.status_code != 200:
            logger.warning("clob: get_mid(%s) status %d", token_id, resp.status_code)
            return None
        try:
            book = resp.json()
        except ValueError:
            logger.warning("clob: get_mid(%s) unparseable JSON", token_id)
            return None
        bids = book.get("bids") or []
        asks = book.get("asks") or []
        if not bids or not asks:
            return None
        try:
            best_bid = float(bids[0]["price"])
            best_ask = float(asks[0]["price"])
        except (KeyError, TypeError, ValueError):
            return None
        mid = (best_bid + best_ask) / 2.0
        if 0.0 <= mid <= 1.0:
            return mid
        return None

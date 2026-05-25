"""
## Specification

**Purpose.** Thin async client for Polymarket's CLOB price endpoints.
Returns the current mid-point price for a given outcome token. Consumed by
`stages/trade.py` (entry-mid for the edge gate), `stages/closer.py`
(exit-mid for PnL), and `dashboard/pages/main.py` (mark-to-market on
open positions).

**Public interface.**
- `CLOBClient(base_url: str, timeout_seconds: float = 5.0)`
- `await client.get_mid(token_id: str) -> float | None` — reads the
  midpoint from `GET /midpoint?token_id=<id>` which returns
  `{"mid":"0.545"}`. NOTE: per Polymarket's own docs, this scalar is
  the bid-ask midpoint only when spread ≤ $0.10; on wider spreads it
  falls back to the **last traded price** (which can be hours stale).
  Use `get_top_of_book()` to detect this case before trusting `mid`.
- `await client.get_top_of_book(token_id: str) -> (best_bid, best_ask) | None`
  — reads `GET /book?token_id=<id>` and returns the best executable
  prices. Spread = `best_ask - best_bid`. Returns `None` on any
  failure (network, non-2xx, missing fields, crossed book, out-of-
  range prices). The path-param form `/book/<id>` 404s; the
  query-param form is the correct API (2026-05-25 live probe).
- `await client.close()` — closes the underlying `httpx.AsyncClient`.

**Invariants.**
- Returns a number in `[0.0, 1.0]` or `None`. Never raises.
- Lazy connection: the `httpx.AsyncClient` is created on first call so
  importing the module is free.

**Side effects.** HTTP GET to `<base_url>/midpoint?token_id=<id>`.

**Failure modes.** Network error / non-2xx / unparseable body / missing
`mid` field → `None`. A WARNING is logged per failure; recovery is silent.

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

    async def get_price_history(
        self,
        token_id: str,
        *,
        interval: str = "1h",
        fidelity: int = 10,
    ) -> Optional[list[tuple[int, float]]]:
        """Fetch the price-history time series for a token from
        `GET /prices-history?market=<id>&interval=<i>&fidelity=<f>`.
        Returns a list of `(unix_ts, mid_price)` tuples, newest last.
        `None` on any failure (network, non-2xx, malformed body).

        Polymarket rejects `interval=1m` with the error
        `"minimum 'fidelity' for '1m' range is 10"` — use `1h` or `6h`
        for the missed-edge plot window.
        """
        if not token_id:
            return None
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._timeout)
        url = f"{self._base_url}/prices-history"
        try:
            resp = await self._client.get(
                url, params={"market": token_id, "interval": interval, "fidelity": fidelity}
            )
        except httpx.HTTPError as exc:
            logger.warning(
                "clob: get_price_history(%s) network error: %s",
                token_id, exc.__class__.__name__,
            )
            return None
        if resp.status_code != 200:
            logger.warning(
                "clob: get_price_history(%s) status %d", token_id, resp.status_code,
            )
            return None
        try:
            body = resp.json()
        except ValueError:
            logger.warning("clob: get_price_history(%s) unparseable JSON", token_id)
            return None
        history = body.get("history")
        if not isinstance(history, list):
            return None
        out: list[tuple[int, float]] = []
        for pt in history:
            t = pt.get("t")
            p = pt.get("p")
            try:
                out.append((int(t), float(p)))
            except (TypeError, ValueError):
                continue
        return out or None

    async def get_mid(self, token_id: str) -> Optional[float]:
        if not token_id:
            return None
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._timeout)
        url = f"{self._base_url}/midpoint"
        try:
            resp = await self._client.get(url, params={"token_id": token_id})
        except httpx.HTTPError as exc:
            logger.warning("clob: get_mid(%s) network error: %s", token_id, exc.__class__.__name__)
            return None
        if resp.status_code != 200:
            logger.warning("clob: get_mid(%s) status %d", token_id, resp.status_code)
            return None
        try:
            body = resp.json()
        except ValueError:
            logger.warning("clob: get_mid(%s) unparseable JSON", token_id)
            return None
        raw = body.get("mid")
        if raw is None:
            return None
        try:
            mid = float(raw)
        except (TypeError, ValueError):
            return None
        if 0.0 <= mid <= 1.0:
            return mid
        return None

    async def get_top_of_book(
        self, token_id: str
    ) -> Optional[tuple[float, float]]:
        """Return `(best_bid, best_ask)` from `GET /book?token_id=<id>`.

        `/book` returns the full depth as `{"bids":[{"price","size"},...],
        "asks":[...]}`. We pick `max(bids[].price)` and `min(asks[].price)`
        so order-of-entries doesn't matter. `None` on any failure or
        degenerate response (empty side, crossed book, out-of-range
        prices).

        Consumers can derive `mid = (bid + ask) / 2` and
        `spread = ask - bid` from the returned pair, which is the only
        way to reliably detect Polymarket's "spread > $0.10 → display
        last trade instead of mid" fallback documented for `/midpoint`.
        """
        if not token_id:
            return None
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._timeout)
        url = f"{self._base_url}/book"
        try:
            resp = await self._client.get(url, params={"token_id": token_id})
        except httpx.HTTPError as exc:
            logger.warning(
                "clob: get_top_of_book(%s) network error: %s",
                token_id, exc.__class__.__name__,
            )
            return None
        if resp.status_code != 200:
            logger.warning(
                "clob: get_top_of_book(%s) status %d", token_id, resp.status_code,
            )
            return None
        try:
            body = resp.json()
        except ValueError:
            logger.warning("clob: get_top_of_book(%s) unparseable JSON", token_id)
            return None
        bids = body.get("bids")
        asks = body.get("asks")
        if not isinstance(bids, list) or not isinstance(asks, list):
            return None
        if not bids or not asks:
            return None
        try:
            best_bid = max(float(b["price"]) for b in bids if "price" in b)
            best_ask = min(float(a["price"]) for a in asks if "price" in a)
        except (TypeError, ValueError, KeyError):
            return None
        if not (0.0 <= best_bid <= 1.0 and 0.0 <= best_ask <= 1.0):
            return None
        if best_bid > best_ask:
            return None
        return (best_bid, best_ask)

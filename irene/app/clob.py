"""
## Specification

**Purpose.** Thin async client for Polymarket's CLOB price endpoints.
Returns the current mid-point price for a given outcome token. Consumed by
`stages/trade.py` (entry-mid for the edge gate), `stages/closer.py`
(exit-mid for PnL), and `dashboard/pages/main.py` (mark-to-market on
open positions).

**Public interface.**
- `CLOBClient(base_url: str, timeout_seconds: float = 5.0, *,
  live_cache: LiveMarketCache | None = None)`
- `await client.get_mid(token_id: str, *, prefer_live: bool = True) ->
  float | None` — reads the midpoint. When `live_cache` is wired AND
  `prefer_live=True` AND the cached tick is non-stale, returns it
  without any HTTP. Otherwise falls back to `GET /midpoint?token_id=
  <id>` which returns `{"mid":"0.545"}`. NOTE: per Polymarket's own
  docs, the REST scalar is the bid-ask midpoint only when spread ≤
  $0.10; on wider spreads it falls back to the **last traded price**
  (which can be hours stale). Use `get_top_of_book()` to detect this
  case before trusting `mid`. Callers that need an authoritative REST
  reading (e.g. impact_tracker measuring its own bot's trade drift)
  pass `prefer_live=False` to skip the cache.
- `await client.get_top_of_book(token_id: str, *, prefer_live: bool =
  True) -> (best_bid, best_ask) | None` — reads `GET /book?token_id=
  <id>` and returns the best executable prices. Spread = `best_ask -
  best_bid`. Same cache opt-out shape as `get_mid`. Returns `None` on
  any failure (network, non-2xx, missing fields, crossed book, out-
  of-range prices). The path-param form `/book/<id>` 404s; the
  query-param form is the correct API (2026-05-25 live probe).
- `await client.get_market_fee_rate(condition_id: str) -> float | None`
  — reads `GET /markets/<condition_id>` and returns the **taker** fee
  rate as a fractional number in `[0.0, 1.0]`. The CLOB payload
  exposes `maker_base_fee` and `taker_base_fee` in **basis points**
  (1 bp = 0.01% = 1e-4); we read `taker_base_fee` (the relevant
  rate for paper-trade taker fills) and divide by 10000. A 2026-05-25
  live survey of 1000 markets returned `taker_base_fee` in
  {0, 200, 1000} → fractional {0.00, 0.02, 0.10}. Returns `None` on
  any failure (network, non-2xx, missing field, parse error,
  out-of-range value). Authoritative source for `markets.fee_rate`
  (see plans/markets-gamma.md + plans/closer-fees.md). Cycle 13:
  this is now a thin shim over `get_market_info` so callers that
  need BOTH fee_rate and min_order_size pay one HTTP, while existing
  callers keep the single-purpose interface.
- `await client.get_market_info(condition_id: str) -> dict | None`
  — reads `GET /markets/<condition_id>` once and returns a small dict
  `{"fee_rate": float | None, "min_order_size": float | None}` so the
  markets stage can populate both columns in a single round-trip on
  first-seen markets. Each value is independently `None` when the
  source field is missing / unparseable / out-of-range; the whole
  call returns `None` only on network / non-2xx / unparseable-JSON
  failure (i.e. when we got NO usable data). `min_order_size` is
  read from the `minimum_order_size` field of the CLOB payload —
  units are SHARES, NOT USDC (cycle-13 live verification:
  `minimum_order_size=5` for a market priced ~$0.006 ⇒ ~$0.03 USDC
  floor). The trade-stage size gate handles the shares→USDC
  conversion using the executable price for the bet's side.
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
from typing import Optional, TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from app.live_market_cache import LiveMarketCache

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://clob.polymarket.com"


class CLOBClient:
    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        timeout_seconds: float = 5.0,
        *,
        live_cache: Optional["LiveMarketCache"] = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_seconds
        self._client: Optional[httpx.AsyncClient] = None
        # Cycle 26: optional read-through to the WebSocket-fed cache.
        # When `live_cache` is None (e.g. feature flag off, minimal test
        # wirings), every call hits REST — original behavior preserved.
        self._live_cache: Optional["LiveMarketCache"] = live_cache

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

    async def get_mid(
        self, token_id: str, *, prefer_live: bool = True,
    ) -> Optional[float]:
        if not token_id:
            return None
        # Cycle 26: read-through cache. Skip when (a) feature-flag-off
        # (cache is None), (b) caller pinned `prefer_live=False` because
        # it needs an authoritative REST mid (e.g. impact_tracker post-
        # trade drift measurement), or (c) the cached tick is stale or
        # has no mid yet — fall through to REST in all cases.
        if prefer_live and self._live_cache is not None:
            tick = self._live_cache.get(token_id)
            if (
                tick is not None
                and tick.mid is not None
                and self._live_cache.is_fresh(tick)
            ):
                return tick.mid
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
        self, token_id: str, *, prefer_live: bool = True,
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
        # Cycle 26: read-through cache. Same opt-out shape as `get_mid`.
        if prefer_live and self._live_cache is not None:
            tick = self._live_cache.get(token_id)
            if (
                tick is not None
                and tick.best_bid is not None
                and tick.best_ask is not None
                and self._live_cache.is_fresh(tick)
            ):
                return (tick.best_bid, tick.best_ask)
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

    async def get_market_info(self, condition_id: str) -> Optional[dict]:
        """Read `GET /markets/<condition_id>` once and return a dict
        `{"fee_rate": <float|None>, "min_order_size": <float|None>}`.

        - `fee_rate` is the **taker** fee fraction in `[0.0, 1.0]`,
          parsed from `taker_base_fee` (basis points → /10000). `None`
          when the field is missing / unparseable / out-of-range.
        - `min_order_size` is the **per-market platform floor in
          SHARES** (cycle 13), parsed from `minimum_order_size`. `None`
          when the field is missing / unparseable / negative. Units
          are NOT USDC: callers must convert via
          `min_shares × executable_price = min_usdc_on_this_market`.
          Live verification 2026-05-25: a market priced ~$0.006 with
          `minimum_order_size=5` ⇒ ~$0.03 USDC platform floor.

        Returns `None` only when the HTTP call itself fails (empty
        condition_id, network error, non-2xx, unparseable body) — i.e.
        when we got NO usable data. A 200 response with one field
        missing yields a dict with that field set to `None` and the
        other populated; this is what lets cycle-10 callers keep
        getting a fee_rate even if Polymarket later removes
        `minimum_order_size` from the payload.
        """
        if not condition_id:
            return None
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._timeout)
        url = f"{self._base_url}/markets/{condition_id}"
        try:
            resp = await self._client.get(url)
        except httpx.HTTPError as exc:
            logger.warning(
                "clob: get_market_info(%s) network error: %s",
                condition_id, exc.__class__.__name__,
            )
            return None
        if resp.status_code != 200:
            logger.warning(
                "clob: get_market_info(%s) status %d",
                condition_id, resp.status_code,
            )
            return None
        try:
            body = resp.json()
        except ValueError:
            logger.warning(
                "clob: get_market_info(%s) unparseable JSON", condition_id,
            )
            return None
        return {
            "fee_rate": _parse_taker_fee_rate(body, condition_id),
            "min_order_size": _parse_min_order_size(body, condition_id),
        }

    async def get_market_fee_rate(self, condition_id: str) -> Optional[float]:
        """Return the **taker** fee rate for a market as a fractional
        number in `[0.0, 1.0]`. Cycle-13 thin shim over `get_market_info`
        so callers that only need the fee continue to work.

        Reads `GET /markets/<condition_id>`. The CLOB payload exposes
        `taker_base_fee` in basis points (1 bp = 1e-4); we divide by
        10000 to return the fractional rate that the rest of the codebase
        (`closer.py:_polymarket_taker_fee`) consumes directly. We use
        `taker_base_fee` rather than `maker_base_fee` because the paper-
        trade model in `closer.py:_compute_round_trip_fees` assumes
        we always cross the spread (taker fills both legs).

        Returns `None` on any failure: empty condition_id, network error,
        non-2xx status, unparseable JSON, missing/invalid field, or a
        fractional value outside [0.0, 1.0]. Same failure discipline as
        `get_top_of_book` — a WARNING is logged per failure and recovery
        is silent so the caller can fall back to the category lookup
        (`config.polymarket_fee_rates`).
        """
        info = await self.get_market_info(condition_id)
        if info is None:
            return None
        return info["fee_rate"]


def _parse_taker_fee_rate(body: dict, condition_id: str) -> Optional[float]:
    """Pull `taker_base_fee` (basis points) out of a /markets response
    body and return the fractional rate in `[0.0, 1.0]` or `None`.
    Extracted as a free helper so `get_market_info` can compose with
    `_parse_min_order_size` without duplicating the HTTP plumbing."""
    raw = body.get("taker_base_fee")
    if raw is None:
        logger.warning(
            "clob: get_market_info(%s) missing taker_base_fee", condition_id,
        )
        return None
    try:
        fee_bps = float(raw)
    except (TypeError, ValueError):
        return None
    fee_rate = fee_bps / 10000.0
    if 0.0 <= fee_rate <= 1.0:
        return fee_rate
    return None


def _parse_min_order_size(body: dict, condition_id: str) -> Optional[float]:
    """Pull `minimum_order_size` out of a /markets response body. Units
    are SHARES, not USDC — the trade-stage size gate is responsible
    for the conversion using the executable price. Returns `None`
    when the field is missing / unparseable / negative. Zero is a
    legitimate value (the platform default for many markets) and is
    preserved; the gate then becomes a no-op for that market."""
    raw = body.get("minimum_order_size")
    if raw is None:
        # Missing field is common (older markets); don't WARN — log at
        # DEBUG so the discovery loop stays quiet. Caller will leave the
        # column NULL and the gate falls back to the global config floor.
        return None
    try:
        mos = float(raw)
    except (TypeError, ValueError):
        logger.warning(
            "clob: get_market_info(%s) unparseable minimum_order_size=%r",
            condition_id, raw,
        )
        return None
    if mos < 0.0:
        return None
    return mos

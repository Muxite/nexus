"""Polymarket CLOB BBO + book-depth fetcher (Phase 4A.3).

`fetch_bbo(session, token_id) -> BBOResult` is the single source of
truth for "what's on the order book?". Used by engine._prefetch_prices
(writes bid/ask/depth into market_price_snaps), the legacy
impact_tracker._fetch_bbo shim (position-snap loop), sibling A4A.2
(JIT pricing), and Phase 4D.1 (spread+depth filter).

CLOB /book returns bids ascending, asks descending — best=last on
each side. Per-token TTL cache + in-flight coalescing collapse bursts.
4xx/5xx/timeout → /midpoint fallback → both down → all-None BBOResult.
Never raises. `bid <= mid <= ask` preserved (book-derived mid).
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

_CLOB_BOOK_URL = "https://clob.polymarket.com/book?token_id={token_id}"
_CLOB_MID_URL = "https://clob.polymarket.com/midpoint?token_id={token_id}"

_BBO_FETCH_TIMEOUT_S = 0.5
_BBO_CACHE_TTL_S = 5.0
_BBO_DEPTH_LEVELS = 5

_bbo_cache: dict[str, tuple["BBOResult", float]] = {}
_bbo_inflight: dict[str, asyncio.Future] = {}


@dataclass
class BBOResult:
    """Single-token order-book snapshot. `source` is one of
    "clob_book" / "clob_mid_fallback" / "cache" / "unavailable"."""
    mid: Optional[float]
    bid: Optional[float]
    ask: Optional[float]
    bid_size: Optional[float]
    ask_size: Optional[float]
    book_depth_usdc_top5: Optional[float]
    latency_ms: int
    source: str


async def _fetch_mid_only(session: aiohttp.ClientSession,
                          token_id: str) -> Optional[float]:
    """Internal /midpoint call. No impact_tracker import → no cycle."""
    try:
        async with session.get(
            _CLOB_MID_URL.format(token_id=token_id),
            timeout=aiohttp.ClientTimeout(total=2.0),
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            mid = data.get("mid")
            return float(mid) if mid is not None else None
    except Exception as exc:  # noqa: BLE001
        logger.debug("[bbo] /midpoint failed %s: %s", token_id[:12], exc)
        return None


def _parse_book(payload: dict) -> tuple[
    Optional[float], Optional[float], Optional[float],
    Optional[float], Optional[float],
]:
    """Return (best_bid, best_ask, bid_size, ask_size, top5_ask_notional)
    from Polymarket /book (bids ascending, asks descending — best=last)."""
    def _best(side: list) -> tuple[Optional[float], Optional[float]]:
        if not side:
            return None, None
        try:
            return float(side[-1].get("price")), float(side[-1].get("size"))
        except (TypeError, ValueError):
            return None, None
    bids = payload.get("bids") or []
    asks = payload.get("asks") or []
    best_bid, bid_size = _best(bids)
    best_ask, ask_size = _best(asks)
    depth: Optional[float] = None
    if asks:
        running = 0.0
        for lvl in asks[-_BBO_DEPTH_LEVELS:]:
            try:
                running += float(lvl.get("price")) * float(lvl.get("size"))
            except (TypeError, ValueError):
                continue
        depth = running if running > 0 else None
    return best_bid, best_ask, bid_size, ask_size, depth


async def _do_fetch_bbo(session: aiohttp.ClientSession,
                        token_id: str, timeout: float) -> BBOResult:
    """Inner BBO fetch — never raises, never reads/writes the cache."""
    t0 = time.monotonic()
    try:
        async with session.get(
            _CLOB_BOOK_URL.format(token_id=token_id),
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as resp:
            if resp.status != 200:
                raise aiohttp.ClientError(f"clob /book {resp.status}")
            data = await resp.json()
        best_bid, best_ask, bid_size, ask_size, depth = _parse_book(data)
        if best_bid is not None and best_ask is not None:
            mid = (best_bid + best_ask) / 2.0
        else:
            mid = await _fetch_mid_only(session, token_id)
        return BBOResult(
            mid=mid, bid=best_bid, ask=best_ask,
            bid_size=bid_size, ask_size=ask_size,
            book_depth_usdc_top5=depth,
            latency_ms=int((time.monotonic() - t0) * 1000),
            source="clob_book",
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("[bbo] /book failed %s: %s", token_id[:12], exc)
    mid = await _fetch_mid_only(session, token_id)
    return BBOResult(
        mid=mid, bid=None, ask=None, bid_size=None, ask_size=None,
        book_depth_usdc_top5=None,
        latency_ms=int((time.monotonic() - t0) * 1000),
        source="clob_mid_fallback" if mid is not None else "unavailable",
    )


async def fetch_bbo(session: aiohttp.ClientSession, token_id: str, *,
                    timeout: float = _BBO_FETCH_TIMEOUT_S,
                    cache_ttl: float = _BBO_CACHE_TTL_S) -> BBOResult:
    """Cached, coalesced BBO fetch. Never raises."""
    now_mono = time.monotonic()
    cached = _bbo_cache.get(token_id)
    if cached is not None and cached[1] > now_mono:
        r = cached[0]
        return BBOResult(
            mid=r.mid, bid=r.bid, ask=r.ask,
            bid_size=r.bid_size, ask_size=r.ask_size,
            book_depth_usdc_top5=r.book_depth_usdc_top5,
            latency_ms=0, source="cache",
        )
    inflight = _bbo_inflight.get(token_id)
    if inflight is not None:
        try:
            return await inflight
        except Exception:  # noqa: BLE001
            pass
    fut: asyncio.Future = asyncio.get_event_loop().create_future()
    _bbo_inflight[token_id] = fut
    try:
        result = await _do_fetch_bbo(session, token_id, timeout)
        _bbo_cache[token_id] = (result, time.monotonic() + cache_ttl)
        if not fut.done():
            fut.set_result(result)
        return result
    finally:
        _bbo_inflight.pop(token_id, None)

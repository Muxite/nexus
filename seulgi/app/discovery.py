"""
Market discovery beyond Polymarket — Kalshi + Predictit adapters.

## Specification
**Purpose.** Discover open prediction-market contracts from sources other
than Polymarket's gamma-api (which caps at ~10,100 markets). Adapters
normalise each source's native shape into a `DiscoveredMarket` so the
MarketIndex can ingest them via the existing `markets` table (with a new
`source` column).

**Public interface.** `DiscoveredMarket`, `KalshiAdapter`,
`PredictitAdapter`, `quality_score`, `ActiveDiscoveryLoop`.

**Invariants.** `quality_score(m) ∈ [0, 1]`; NULL fields contribute 0.
Adapters never raise on HTTP/parse errors — they log + return [].
`(source, cond_id)` is the natural key.

**Side effects.** HTTPS GET on api.elections.kalshi.com and
www.predictit.org/api. Calls `db.upsert_discovered_market`.

**Failure modes.** 4xx/5xx/timeout → adapter returns []. Missing required
fields → row skipped. DB write failure surfaces from upsert; the loop
logs and continues.

**Non-goals.** find_matches changes (Phase 4E.2). Kalshi trading
endpoints. Live API-key management — operator supplies via env if needed.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal, Optional

import aiohttp

logger = logging.getLogger(__name__)

_KALSHI_MARKETS_URL = "https://api.elections.kalshi.com/trade-api/v2/markets"
_PREDICTIT_MARKETS_URL = "https://www.predictit.org/api/marketdata/all/"

Source = Literal["polymarket", "kalshi", "predictit"]


@dataclass
class DiscoveredMarket:
    """Source-agnostic market record. Persisted to `markets` table."""
    source: Source
    cond_id: str
    question: str
    yes_price: Optional[float] = None
    no_price: Optional[float] = None
    liquidity_usdc: Optional[float] = None
    volume_24h_usdc: Optional[float] = None
    days_to_resolution: Optional[int] = None
    quality_score: float = 0.0
    raw_metadata: dict = field(default_factory=dict)


def _days_until(end_iso: Optional[str]) -> Optional[int]:
    if not end_iso:
        return None
    try:
        end = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
        delta = end - datetime.now(timezone.utc)
        return max(0, int(delta.total_seconds() // 86400))
    except (ValueError, TypeError):
        return None


def quality_score(m: DiscoveredMarket) -> float:
    """Composite 0-1 quality. Weights: liquidity 0.4, volume 0.3,
    days-to-resolution 0.2 (sweet spot 1..30d), spread 0.1. NULL → 0."""
    liq = min(1.0, m.liquidity_usdc / 10_000.0) \
        if m.liquidity_usdc and m.liquidity_usdc > 0 else 0.0
    vol = min(1.0, m.volume_24h_usdc / 50_000.0) \
        if m.volume_24h_usdc and m.volume_24h_usdc > 0 else 0.0
    if m.days_to_resolution is not None:
        d = m.days_to_resolution
        if d < 1:
            dt = 0.2
        elif d <= 7:
            dt = 1.0
        elif d <= 30:
            dt = 1.0 - (d - 7) / (30 - 7) * 0.5
        elif d <= 180:
            dt = 0.3
        else:
            dt = 0.1
    else:
        dt = 0.0
    if m.yes_price is not None and m.no_price is not None:
        sp = max(0.0, 1.0 - abs(1.0 - (m.yes_price + m.no_price)) * 5.0)
    else:
        sp = 0.0
    return max(0.0, min(1.0, 0.4 * liq + 0.3 * vol + 0.2 * dt + 0.1 * sp))


class KalshiAdapter:
    """https://api.elections.kalshi.com — public read endpoints, no auth."""

    URL = _KALSHI_MARKETS_URL

    async def fetch_open_markets(
        self, session: aiohttp.ClientSession, limit: int = 1000,
    ) -> list[DiscoveredMarket]:
        markets: list[DiscoveredMarket] = []
        cursor: Optional[str] = None
        page_size = 200
        max_pages = max(1, limit // page_size + 1)
        for _ in range(max_pages):
            params: dict = {"limit": str(page_size), "status": "open"}
            if cursor:
                params["cursor"] = cursor
            try:
                async with session.get(
                    self.URL, params=params,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    resp.raise_for_status()
                    page = await resp.json()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Kalshi fetch error (cursor=%s): %s", cursor, exc)
                break
            for raw in page.get("markets") or []:
                m = self._parse(raw)
                if m is not None:
                    markets.append(m)
                if len(markets) >= limit:
                    break
            cursor = page.get("cursor")
            if not cursor or len(markets) >= limit:
                break
        logger.info("Kalshi: fetched %d markets", len(markets))
        return markets

    @staticmethod
    def _parse(raw: dict) -> Optional[DiscoveredMarket]:
        ticker = raw.get("ticker") or ""
        question = raw.get("title") or raw.get("subtitle") or ""
        if not ticker or not question:
            return None

        def _cents(v):
            if v is None:
                return None
            try:
                return float(v) / 100.0
            except (ValueError, TypeError):
                return None

        liq = float(raw.get("liquidity") or 0) or None
        vol = float(raw.get("volume_24h") or raw.get("volume") or 0) or None
        m = DiscoveredMarket(
            source="kalshi", cond_id=ticker, question=question,
            yes_price=_cents(raw.get("yes_bid") or raw.get("last_price")),
            no_price=_cents(raw.get("no_bid")),
            liquidity_usdc=liq, volume_24h_usdc=vol,
            days_to_resolution=_days_until(
                raw.get("close_time") or raw.get("expiration_time")),
            raw_metadata={k: raw.get(k) for k in ("event_ticker", "category")},
        )
        m.quality_score = quality_score(m)
        return m


class PredictitAdapter:
    """https://www.predictit.org/api/marketdata/all/ — public JSON, no auth."""

    URL = _PREDICTIT_MARKETS_URL

    async def fetch_open_markets(
        self, session: aiohttp.ClientSession,
    ) -> list[DiscoveredMarket]:
        try:
            async with session.get(
                self.URL, timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                resp.raise_for_status()
                payload = await resp.json()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Predictit fetch error: %s", exc)
            return []
        markets: list[DiscoveredMarket] = []
        for raw_market in payload.get("markets") or []:
            mid = raw_market.get("id")
            mname = raw_market.get("name") or ""
            end_iso = raw_market.get("dateEnd")
            for c in raw_market.get("contracts") or []:
                m = self._parse_contract(mid, mname, end_iso, c)
                if m is not None:
                    markets.append(m)
        logger.info("Predictit: fetched %d contracts", len(markets))
        return markets

    @staticmethod
    def _parse_contract(market_id, market_name, end_iso, c):
        cid = c.get("id")
        if cid is None:
            return None
        cname = c.get("name") or ""
        question = f"{market_name} — {cname}" if cname else market_name
        if not question:
            return None
        yes_p = c.get("bestBuyYesCost")
        no_p = c.get("bestBuyNoCost")
        m = DiscoveredMarket(
            source="predictit", cond_id=str(cid), question=question,
            yes_price=float(yes_p) if yes_p is not None else None,
            no_price=float(no_p) if no_p is not None else None,
            liquidity_usdc=None, volume_24h_usdc=None,
            days_to_resolution=_days_until(end_iso or c.get("dateEnd")),
            raw_metadata={"market_id": market_id},
        )
        m.quality_score = quality_score(m)
        return m


class ActiveDiscoveryLoop:
    """Background poller across non-polymarket sources.

    Each tick: fetch from every adapter → drop markets below
    min_quality → dedup by (source, cond_id) → upsert via
    `db.upsert_discovered_market` → trigger `market_index.load_from_db()`.

    TODO(active-search): when the prediction LLM emits a "relevant but
    no good match" signal (currently surfaced only as
    `low_similarity:<max_sim>` in engine.py:427 with no keyword+tag
    side-channel), enqueue a one-shot search for the headline keywords.
    Tracked on master plan 4E.3.
    """

    def __init__(
        self, db, market_index=None,
        interval_secs: float = 1800.0,
        min_quality: float = 0.3,
        adapters: Optional[list] = None,
    ) -> None:
        self._db = db
        self._index = market_index
        self._interval = float(interval_secs)
        self._min_quality = float(min_quality)
        self._adapters = adapters if adapters is not None else [
            KalshiAdapter(), PredictitAdapter(),
        ]
        self._stop = asyncio.Event()

    async def tick(self, session: aiohttp.ClientSession) -> int:
        all_markets: list[DiscoveredMarket] = []
        for ad in self._adapters:
            try:
                all_markets.extend(await ad.fetch_open_markets(session))
            except Exception as exc:  # noqa: BLE001
                logger.warning("Adapter %s raised: %s", type(ad).__name__, exc)
        seen: set[tuple[str, str]] = set()
        upserted = 0
        for m in all_markets:
            if m.quality_score < self._min_quality:
                continue
            key = (m.source, m.cond_id)
            if key in seen:
                continue
            seen.add(key)
            try:
                await self._db.upsert_discovered_market(m)
            except Exception as exc:  # noqa: BLE001
                logger.warning("upsert(%s) failed: %s", key, exc)
                continue
            upserted += 1
        if upserted and self._index is not None:
            try:
                await self._index.load_from_db()
            except Exception as exc:  # noqa: BLE001
                logger.warning("MarketIndex refresh failed: %s", exc)
        logger.info("ActiveDiscoveryLoop: upserted %d markets", upserted)
        return upserted

    async def run(self) -> None:
        async with aiohttp.ClientSession() as session:
            while not self._stop.is_set():
                try:
                    await self.tick(session)
                except Exception as exc:  # noqa: BLE001
                    logger.exception("tick crashed: %s", exc)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=self._interval)
                except asyncio.TimeoutError:
                    pass

    def stop(self) -> None:
        self._stop.set()

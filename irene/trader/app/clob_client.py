"""
Polymarket CLOB API client — read-only (orderbook, mid-price).

Order submission is handled by executor.py via py-clob-client.
This module only does price discovery: fetching the current best
bid/ask and available liquidity depth for a given token.

CLOB REST docs: https://docs.polymarket.com/#get-order-book
"""

from __future__ import annotations

import logging
from typing import Optional

import aiohttp

from irene_shared.models import Orderbook

logger = logging.getLogger(__name__)

_CLOB_URL = "https://clob.polymarket.com"


class ClobClient:
    """
    Async CLOB REST client for price discovery.

    Caller owns the aiohttp.ClientSession lifecycle (pass in via constructor
    or use as an async context manager to create/close one automatically).
    """

    def __init__(
        self,
        base_url: str = _CLOB_URL,
        session: Optional[aiohttp.ClientSession] = None,
        timeout_seconds: int = 8,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._session = session
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._owns_session = session is None

    async def __aenter__(self) -> "ClobClient":
        if self._owns_session:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self

    async def __aexit__(self, *_) -> None:
        if self._owns_session and self._session:
            await self._session.close()
            self._session = None

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    async def get_orderbook(self, token_id: str) -> Optional[Orderbook]:
        """
        Fetch the current orderbook for a token.

        Returns None if the request fails or the book is empty.
        """
        url = f"{self._base_url}/book"
        params = {"token_id": token_id}

        try:
            async with self._session.get(url, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientResponseError as exc:
            logger.error("CLOB orderbook HTTP error %d for %s: %s", exc.status, token_id, exc.message)
            return None
        except Exception as exc:
            logger.error("CLOB orderbook request failed for %s: %s", token_id, exc)
            return None

        return _parse_orderbook(token_id, data)

    async def get_mid_price(self, token_id: str) -> Optional[float]:
        """Return the current mid-price for a token, or None on failure."""
        ob = await self.get_orderbook(token_id)
        return ob.mid_price if ob else None


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _parse_orderbook(token_id: str, data: dict) -> Optional[Orderbook]:
    """
    Parse a raw CLOB /book response into an Orderbook.

    The CLOB returns bids/asks as lists of {"price": str, "size": str} dicts,
    sorted best-first (bids descending, asks ascending).
    """
    try:
        bids: list[dict] = data.get("bids") or []
        asks: list[dict] = data.get("asks") or []

        if not bids and not asks:
            logger.debug("Empty orderbook for token %s", token_id)
            return None

        best_bid = _best_price(bids, highest=True)
        best_ask = _best_price(asks, highest=False)

        if best_bid is None and best_ask is None:
            return None

        # If one side is missing, use the other as both bid and ask
        if best_bid is None:
            best_bid = best_ask
        if best_ask is None:
            best_ask = best_bid

        mid_price = (best_bid + best_ask) / 2.0

        bid_depth = _depth_at_price(bids, best_bid)
        ask_depth = _depth_at_price(asks, best_ask)

        return Orderbook(
            token_id=token_id,
            best_bid=round(best_bid, 6),
            best_ask=round(best_ask, 6),
            mid_price=round(mid_price, 6),
            bid_depth_usdc=round(bid_depth, 4),
            ask_depth_usdc=round(ask_depth, 4),
        )
    except Exception as exc:
        logger.warning("Failed to parse orderbook for %s: %s", token_id, exc)
        return None


def _best_price(levels: list[dict], highest: bool) -> Optional[float]:
    """Return the best price from a list of order levels."""
    prices = []
    for level in levels:
        try:
            prices.append(float(level["price"]))
        except (KeyError, ValueError, TypeError):
            continue
    if not prices:
        return None
    return max(prices) if highest else min(prices)


def _depth_at_price(levels: list[dict], price: float) -> float:
    """Return total size (in USDC) available at exactly `price`."""
    total = 0.0
    for level in levels:
        try:
            p = float(level["price"])
            s = float(level["size"])
            if abs(p - price) < 1e-9:
                total += p * s
        except (KeyError, ValueError, TypeError):
            continue
    return total

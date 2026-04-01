"""
Gamma API client for fetching Polymarket market metadata.

Polymarket's Gamma API is unauthenticated and returns market questions,
volumes, end dates, category labels, and token IDs.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp

logger = logging.getLogger(__name__)

_GAMMA_URL = "https://gamma-api.polymarket.com"


class GammaClient:
    """Async Gamma API client. Caller owns the aiohttp.ClientSession lifecycle."""

    def __init__(
        self,
        base_url: str = _GAMMA_URL,
        session: Optional[aiohttp.ClientSession] = None,
        timeout_seconds: int = 15,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._session = session
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._owns_session = session is None

    async def __aenter__(self) -> "GammaClient":
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

    async def fetch_open_markets(
        self,
        page_size: int = 200,
        max_pages: int = 20,
    ) -> list[dict[str, Any]]:
        """
        Fetch all open markets from the Gamma API.

        Paginates automatically up to `max_pages`. Returns raw dicts —
        filtering and normalization happen in market_filter.py.
        """
        all_markets: list[dict[str, Any]] = []
        offset = 0

        for _ in range(max_pages):
            batch = await self._fetch_page(
                active=True,
                closed=False,
                limit=page_size,
                offset=offset,
            )
            if not batch:
                break
            all_markets.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size

        logger.info("GammaClient: fetched %d open markets", len(all_markets))
        return all_markets

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _fetch_page(
        self,
        active: bool = True,
        closed: bool = False,
        limit: int = 200,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        if self._session is None:
            raise RuntimeError("GammaClient must be used as an async context manager")

        params = {
            "active": str(active).lower(),
            "closed": str(closed).lower(),
            "limit": str(limit),
            "offset": str(offset),
        }

        url = f"{self._base_url}/markets"
        try:
            async with self._session.get(url, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientResponseError as exc:
            logger.error("Gamma API HTTP error %d at %s: %s", exc.status, url, exc.message)
            return []
        except Exception as exc:
            logger.error("Gamma API request failed: %s", exc)
            return []

        if not isinstance(data, list):
            logger.warning("Gamma API returned unexpected type: %s", type(data))
            return []

        return data


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------


def parse_market(raw: dict[str, Any]) -> Optional[dict[str, Any]]:
    """
    Extract and normalise fields from a raw Gamma API market dict.

    Returns None if required fields are missing or unparseable.

    Returned dict keys:
        condition_id, slug, question, description, category,
        end_date, volume_usdc, token_yes, token_no
    """
    try:
        condition_id = raw.get("conditionId") or raw.get("condition_id") or ""
        if not condition_id:
            return None

        question = raw.get("question") or ""
        if not question:
            return None

        end_raw = raw.get("endDate") or raw.get("end_date") or ""
        if not end_raw:
            return None
        end_date = _parse_dt(end_raw)
        if end_date is None:
            return None

        # Volume: prefer USD volume field
        volume_usdc = _safe_float(
            raw.get("volumeNum")
            or raw.get("volume")
            or raw.get("usdcLiquidity")
        )

        # Tokens: YES is index 0, NO is index 1
        tokens = raw.get("tokens") or []
        token_yes = _extract_token_id(tokens, 0)
        token_no = _extract_token_id(tokens, 1)

        return {
            "condition_id": condition_id,
            "slug": raw.get("slug"),
            "question": question,
            "description": raw.get("description"),
            "category": raw.get("category") or raw.get("groupItemTitle"),
            "end_date": end_date,
            "volume_usdc": volume_usdc,
            "token_yes": token_yes,
            "token_no": token_no,
        }
    except Exception as exc:
        logger.debug("parse_market failed: %s", exc)
        return None


def _parse_dt(raw: str) -> Optional[datetime]:
    """Parse an ISO-8601 string to a timezone-aware datetime."""
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, AttributeError):
        return None


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _extract_token_id(tokens: list, index: int) -> Optional[str]:
    if not tokens or index >= len(tokens):
        return None
    token = tokens[index]
    if isinstance(token, dict):
        return token.get("token_id") or token.get("tokenId")
    return None

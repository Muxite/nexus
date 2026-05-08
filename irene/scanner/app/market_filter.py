"""
Market filter for the scanner service.

Takes parsed Gamma API market dicts and applies the configured filters:
  - end_date within [min_hours, max_hours] from now
  - volume >= min_volume_usdc
  - question does not contain exclude_keywords (case-insensitive)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from irene_shared.config import MarketsSection

logger = logging.getLogger(__name__)


class MarketFilter:
    """
    Stateless filter — reads config section on each call.

    Pass the current MarketsSection from ConfigManager.get().markets
    on every call so config changes take effect without restart.
    """

    def filter(
        self,
        markets: list[dict[str, Any]],
        cfg: MarketsSection,
        now: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """
        Return markets that pass all configured filters.

        Args:
            markets: List of dicts from gamma_client.parse_market().
            cfg:     Current MarketsSection config.
            now:     Reference time (UTC). Defaults to datetime.now(utc).
        """
        if now is None:
            now = datetime.now(timezone.utc)

        accepted = []
        for m in markets:
            reason = self._reject_reason(m, cfg, now)
            if reason:
                logger.debug(
                    "Rejected %s (%s): %s",
                    m.get("condition_id", "?"),
                    m.get("slug", "?"),
                    reason,
                )
            else:
                accepted.append(m)

        logger.info(
            "MarketFilter: %d/%d markets passed", len(accepted), len(markets)
        )
        return accepted

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _reject_reason(
        self,
        market: dict[str, Any],
        cfg: MarketsSection,
        now: datetime,
    ) -> str | None:
        """Return a rejection reason string, or None if the market passes."""
        end_date: datetime | None = market.get("end_date")
        if end_date is None:
            return "missing end_date"

        # Ensure timezone-aware
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)

        hours_to_close = (end_date - now).total_seconds() / 3600

        if hours_to_close < cfg.min_hours_to_close:
            return f"closes too soon ({hours_to_close:.1f}h < {cfg.min_hours_to_close}h)"

        if hours_to_close > cfg.max_hours_to_close:
            return f"closes too far out ({hours_to_close:.1f}h > {cfg.max_hours_to_close}h)"

        volume = market.get("volume_usdc")
        if volume is not None and volume < cfg.min_volume_usdc:
            return f"volume too low (${volume:.0f} < ${cfg.min_volume_usdc:.0f})"

        question_lower = (market.get("question") or "").lower()
        for kw in cfg.exclude_keywords:
            if kw.lower() in question_lower:
                return f"excluded keyword: {kw!r}"

        return None

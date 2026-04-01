"""
Position tracker.

Polls open positions periodically and closes them when:
  - The market has resolved (token price >= 0.97 or <= 0.03)
  - The position has exceeded position_timeout_hours

Writes realized P&L and Brier scores to the DB.
Uses jittered_sleep to avoid thundering-herd on the CLOB.
"""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timedelta, timezone
from typing import Optional

from irene_shared.config import ConfigManager
from irene_shared.db import IreneDB
from irene_shared.models import Direction, MarketEventType
from irene_shared.timing import ProfileStore, Timer, jittered_sleep

from clob_client import ClobClient

logger = logging.getLogger(__name__)

_POLL_BASE_SECONDS = 60
_RESOLVED_THRESHOLD = 0.97   # price above this → YES resolved
_ANTI_RESOLVED_THRESHOLD = 0.03  # price below this → NO resolved


class PositionTracker:
    """
    Background coroutine that closes stale or resolved positions.
    """

    def __init__(
        self,
        config_manager: ConfigManager,
        db: IreneDB,
        profile: ProfileStore,
    ) -> None:
        self._cfg = config_manager
        self._db = db
        self._profile = profile
        self._running = False
        self._rng = random.Random()

    async def start(self) -> None:
        self._running = True
        await self._loop()

    async def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------
    # Poll loop
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        # Stagger startup vs the trader main loop
        startup = self._rng.uniform(5.0, 30.0)
        await asyncio.sleep(startup)

        while self._running:
            try:
                with Timer("trader", "position_check", self._profile):
                    await self._check_all()
            except Exception as exc:
                logger.exception("PositionTracker error: %s", exc)

            cfg = self._cfg.get()
            jitter = cfg.timing.jitter_fraction
            await jittered_sleep(_POLL_BASE_SECONDS, jitter_fraction=jitter, rng=self._rng)

    # ------------------------------------------------------------------
    # Check and close positions
    # ------------------------------------------------------------------

    async def _check_all(self) -> None:
        positions = await self._db.get_open_positions()
        if not positions:
            return

        cfg = self._cfg.get()
        timeout_hours = cfg.trading.position_timeout_hours
        now = datetime.now(timezone.utc)

        async with ClobClient(base_url=cfg.polymarket.clob_url) as clob:
            for pos in positions:
                try:
                    await self._check_one(pos, clob, timeout_hours, now)
                except Exception as exc:
                    logger.warning(
                        "Failed to check position trade_id=%s: %s",
                        pos["trade_id"], exc,
                    )

    async def _check_one(
        self,
        pos: dict,
        clob: ClobClient,
        timeout_hours: float,
        now: datetime,
    ) -> None:
        trade_id = pos["trade_id"]
        token_id = pos["token_id"]
        direction = pos["direction"]
        size_usdc = pos["size_usdc"]
        entry_price = pos["entry_price"]
        opened_at: datetime = pos["opened_at"]
        if opened_at.tzinfo is None:
            opened_at = opened_at.replace(tzinfo=timezone.utc)

        ob = await clob.get_orderbook(token_id)
        current_price = ob.mid_price if ob else None

        # Update unrealized P&L in positions table
        if current_price is not None:
            shares = size_usdc / entry_price if entry_price > 0 else 0
            unrealized = (current_price - entry_price) * shares
            await self._db.upsert_position(
                trade_id=trade_id,
                condition_id=pos["condition_id"],
                token_id=token_id,
                direction=direction,
                size_usdc=size_usdc,
                entry_price=entry_price,
                opened_at=opened_at,
                last_price=current_price,
                unrealized_pnl=unrealized,
            )

        # --- Resolution check ---
        exit_reason: Optional[str] = None
        exit_price: Optional[float] = current_price

        if current_price is not None:
            if current_price >= _RESOLVED_THRESHOLD:
                exit_reason = "resolved"
                exit_price = current_price
            elif current_price <= _ANTI_RESOLVED_THRESHOLD:
                exit_reason = "resolved"
                exit_price = current_price

        # --- Timeout check ---
        age_hours = (now - opened_at).total_seconds() / 3600
        if exit_reason is None and age_hours >= timeout_hours:
            exit_reason = "timeout"

        if exit_reason is None:
            return

        # --- Close the position ---
        await self._close(
            trade_id=trade_id,
            pos=pos,
            exit_price=exit_price or entry_price,
            exit_reason=exit_reason,
            direction=direction,
            size_usdc=size_usdc,
            entry_price=entry_price,
        )

    async def _close(
        self,
        trade_id: int,
        pos: dict,
        exit_price: float,
        exit_reason: str,
        direction: str,
        size_usdc: float,
        entry_price: float,
    ) -> None:
        shares = size_usdc / entry_price if entry_price > 0 else 0
        realized_pnl = (exit_price - entry_price) * shares

        # Determine outcome
        if direction == Direction.YES.value:
            won = exit_price >= _RESOLVED_THRESHOLD
        else:
            won = exit_price <= _ANTI_RESOLVED_THRESHOLD
        outcome = "WIN" if won else ("LOSS" if exit_reason == "resolved" else "PUSH")

        await self._db.close_trade(
            trade_id=trade_id,
            exit_price=exit_price,
            exit_reason=exit_reason,
            realized_pnl=realized_pnl,
            outcome=outcome,
        )
        await self._db.delete_position(trade_id)

        await self._db.log_event(
            condition_id=pos["condition_id"],
            event_type=MarketEventType.POSITION_CLOSED,
            payload={
                "trade_id": trade_id,
                "exit_reason": exit_reason,
                "exit_price": round(exit_price, 6),
                "entry_price": round(entry_price, 6),
                "realized_pnl": round(realized_pnl, 4),
                "outcome": outcome,
            },
        )

        logger.info(
            "Closed position trade_id=%d reason=%s pnl=$%.4f outcome=%s",
            trade_id, exit_reason, realized_pnl, outcome,
        )

        # Record Brier score if we have an actual outcome
        if exit_reason == "resolved":
            actual = 1.0 if direction == Direction.YES.value and won else 0.0
            await self._db.insert_brier_score(
                trade_id=trade_id,
                signal_source=pos.get("signal_source", "unknown"),
                p_hat=pos.get("p_hat", 0.5),
                actual_outcome=actual,
            )

"""ExitPolicy — when to close an open position.

Mirrors the legacy logic in app/trader.py:1086-1103 minus the inversion
path (which the validated edge doesn't use). The decision order matches
the legacy code so realized_pnl distributions stay comparable.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from .config import CoreConfig

_WIN_PRICE = 0.95
_LOSS_PRICE = 0.05


@dataclass
class ExitDecision:
    close: bool
    close_reason: Optional[str] = None
    detail: dict = None


class ExitPolicy:
    def __init__(self, cfg: CoreConfig) -> None:
        self._cfg = cfg

    def decide(
        self,
        *,
        entry_price: float,
        current_price: float,
        high_water_price: Optional[float],
        opened_at_iso: str,
        now: Optional[datetime] = None,
    ) -> ExitDecision:
        c = self._cfg
        now = now or datetime.now(timezone.utc)
        opened_at = datetime.fromisoformat(opened_at_iso)
        if opened_at.tzinfo is None:
            opened_at = opened_at.replace(tzinfo=timezone.utc)
        age_secs = (now - opened_at).total_seconds()
        timeout_secs = c.position_timeout_hours * 3600.0
        min_hold_met = c.min_hold_seconds <= 0 or age_secs >= c.min_hold_seconds
        local_hwm = max(current_price, high_water_price if high_water_price is not None else current_price)
        mtm = current_price - entry_price
        hwm_mtm = local_hwm - entry_price

        if current_price >= _WIN_PRICE:
            return ExitDecision(True, "WIN", {"price": current_price})
        if current_price <= _LOSS_PRICE:
            return ExitDecision(True, "LOSS", {"price": current_price})
        if min_hold_met and c.trailing_stop > 0 and hwm_mtm >= c.trailing_stop and (hwm_mtm - mtm) >= c.trailing_stop:
            return ExitDecision(True, "TRAIL", {"hwm": local_hwm, "mtm": mtm})
        if min_hold_met and c.take_profit > 0 and mtm >= c.take_profit:
            return ExitDecision(True, "TP", {"mtm": mtm})
        # Price-relative SL floor (cycle-10 compatibility)
        sl = max(c.stop_loss, c.stop_loss_pct * entry_price)
        if min_hold_met and sl > 0 and mtm <= -sl:
            return ExitDecision(True, "SL", {"mtm": mtm, "sl": sl})
        if age_secs >= timeout_secs:
            return ExitDecision(True, "TIMEOUT", {"age_secs": age_secs})
        return ExitDecision(False)

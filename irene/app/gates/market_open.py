"""Market-liveness gate — pure-Python, no network.

Drops `(article, market)` candidates whose market is already closed or
past its end date at t0. A closed/resolved market has no resting order
book, so odds capture against it returns an empty book for the entire
post-news window — history-only mids, no askable price. Placed first in
the pipeline so dead markets never reach the paid LLM gate or odds
capture.

Pass criterion (mirrors `active = 1 AND (end_date IS NULL OR
end_date > t0)`):
  - `market_active` is 1 or None (unknown treated as open), AND
  - `market_end_date` is None or parses to a datetime in the future.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from app.gates.base import Candidate, Gate, GateDecision, GateOutcome


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


class MarketOpenGate(Gate):
    def __init__(self, *, name: str = "market_open") -> None:
        self.name = name

    async def evaluate(
        self, candidates: list[Candidate]
    ) -> list[GateDecision]:
        now = datetime.now(timezone.utc)
        out: list[GateDecision] = []
        for c in candidates:
            reason: Optional[str] = None
            if c.market_active == 0:
                reason = "market_inactive"
            else:
                end = _parse_iso(c.market_end_date)
                if end is not None and end <= now:
                    reason = f"market_ended ({c.market_end_date} < t0)"
            passed = reason is None
            out.append(GateDecision(
                candidate=c,
                gate_name=self.name,
                outcome=GateOutcome.PASSED if passed else GateOutcome.REJECTED,
                reason="" if passed else reason,
                metadata={
                    "active": c.market_active,
                    "end_date": c.market_end_date,
                },
            ))
        return out

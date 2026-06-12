"""Already-moving gate — H8 lane (cycle 30).

H8's surviving observation: signal lives in markets that were ALREADY
repricing before the article landed — news increments narratives in
motion; it rarely starts them. Pass iff
`|market_mid - mid_lookback| >= min_abs_move`, where `mid_lookback` is
the market's mid ~`lookback_secs` ago from `clob.get_price_history`.

Cost control: one history call per token with an in-process TTL memo
(default 1h) — bursty clusters hit the same markets repeatedly. The
gate never raises; any failure path is an audited reject (per the Gate
ABC contract).
"""

from __future__ import annotations

import logging
import time
from typing import Optional

from app.gates.base import Candidate, Gate, GateDecision, GateOutcome

logger = logging.getLogger(__name__)

#: Tolerance when matching the lookback sample (hourly fidelity → a
#: sample is always within ±2h of any target inside the window).
_LOOKBACK_GRACE_SECS = 7200


class AlreadyMovingGate(Gate):
    def __init__(
        self,
        *,
        name: str = "already_moving",
        clob,
        lookback_secs: int = 86_400,
        min_abs_move: float = 0.05,
        memo_ttl_secs: int = 3600,
    ) -> None:
        self.name = name
        self._clob = clob
        self._lookback = int(lookback_secs)
        self._min_move = float(min_abs_move)
        self._ttl = int(memo_ttl_secs)
        # token_id -> (fetched_at_monotonic, history | None)
        self._memo: dict[str, tuple[float, Optional[list]]] = {}

    async def _history(self, token_id: str) -> Optional[list]:
        cached = self._memo.get(token_id)
        now = time.monotonic()
        if cached is not None and now - cached[0] < self._ttl:
            return cached[1]
        history = None
        try:
            history = await self._clob.get_price_history(
                token_id, interval="1d", fidelity=60,
            )
        except Exception as exc:  # noqa: BLE001 — gates must not raise
            logger.warning(
                "already_moving: get_price_history(%s) raised %s",
                token_id, exc.__class__.__name__,
            )
        self._memo[token_id] = (now, history)
        # Bound the memo so a long-running explorer doesn't grow forever.
        if len(self._memo) > 2048:
            oldest = sorted(self._memo.items(), key=lambda kv: kv[1][0])
            for k, _ in oldest[:1024]:
                self._memo.pop(k, None)
        return history

    async def evaluate(
        self, candidates: list[Candidate]
    ) -> list[GateDecision]:
        out: list[GateDecision] = []
        now_ts = int(time.time())
        for c in candidates:
            if c.market_mid is None or not c.token_yes:
                out.append(GateDecision(
                    candidate=c, gate_name=self.name,
                    outcome=GateOutcome.REJECTED,
                    reason="inputs_unknown",
                ))
                continue
            history = await self._history(str(c.token_yes))
            if not history:
                out.append(GateDecision(
                    candidate=c, gate_name=self.name,
                    outcome=GateOutcome.REJECTED,
                    reason="history_unavailable",
                ))
                continue
            target = now_ts - self._lookback
            best: Optional[tuple[int, float]] = None
            for ts, mid in history:
                try:
                    ts_i, mid_f = int(ts), float(mid)
                except (TypeError, ValueError):
                    continue
                if abs(ts_i - target) > _LOOKBACK_GRACE_SECS:
                    continue
                if best is None or abs(ts_i - target) < abs(best[0] - target):
                    best = (ts_i, mid_f)
            if best is None:
                out.append(GateDecision(
                    candidate=c, gate_name=self.name,
                    outcome=GateOutcome.REJECTED,
                    reason="lookback_sample_missing",
                ))
                continue
            move = abs(float(c.market_mid) - best[1])
            meta = {
                "mid_now": float(c.market_mid),
                "mid_lookback": best[1],
                "abs_move": move,
                "lookback_secs": self._lookback,
            }
            if move >= self._min_move:
                out.append(GateDecision(
                    candidate=c, gate_name=self.name,
                    outcome=GateOutcome.PASSED, metadata=meta,
                ))
            else:
                out.append(GateDecision(
                    candidate=c, gate_name=self.name,
                    outcome=GateOutcome.REJECTED,
                    reason="not_moving", metadata=meta,
                ))
        return out

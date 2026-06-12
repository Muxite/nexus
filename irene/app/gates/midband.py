"""Midband gate — H8 lane (cycle 30).

The 05-29 synthesis (H8) found post-move signal concentrates 8x in
markets whose price sits AWAY from the poles: a mid near 0 or 1 is a
market that already made up its mind, and any "edge" there is spread +
fees. Pass iff `min_mid <= market_mid <= max_mid`.

`market_mid is None` (no t0 book snapshot) is an audited reject — the
H8 lane annotates rows for days-horizon study, and a row we can't price
at t0 can't anchor a price-move measurement. No silent passes.
"""

from __future__ import annotations

from app.gates.base import Candidate, Gate, GateDecision, GateOutcome


class MidbandGate(Gate):
    def __init__(
        self,
        *,
        name: str = "midband",
        min_mid: float = 0.10,
        max_mid: float = 0.90,
    ) -> None:
        self.name = name
        self._min = float(min_mid)
        self._max = float(max_mid)

    async def evaluate(
        self, candidates: list[Candidate]
    ) -> list[GateDecision]:
        out: list[GateDecision] = []
        for c in candidates:
            mid = c.market_mid
            if mid is None:
                out.append(GateDecision(
                    candidate=c, gate_name=self.name,
                    outcome=GateOutcome.REJECTED,
                    reason="mid_unknown",
                ))
                continue
            mid_f = float(mid)
            if self._min <= mid_f <= self._max:
                out.append(GateDecision(
                    candidate=c, gate_name=self.name,
                    outcome=GateOutcome.PASSED,
                    metadata={"mid": mid_f},
                ))
            else:
                out.append(GateDecision(
                    candidate=c, gate_name=self.name,
                    outcome=GateOutcome.REJECTED,
                    reason="mid_out_of_band",
                    metadata={"mid": mid_f,
                              "band": [self._min, self._max]},
                ))
        return out

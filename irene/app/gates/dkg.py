"""DKG threshold gate — pure-Python, no network. Replaces the inline
`final_score >= score_floor` check that used to live in
`explorer.py::_fetch_high_score_links`."""

from __future__ import annotations

from app.gates.base import Candidate, Gate, GateDecision, GateOutcome


class DKGThresholdGate(Gate):
    def __init__(self, *, name: str = "dkg", min_final_score: float) -> None:
        self.name = name
        self.min_final_score = float(min_final_score)

    async def evaluate(
        self, candidates: list[Candidate]
    ) -> list[GateDecision]:
        out: list[GateDecision] = []
        for c in candidates:
            passed = c.final_score >= self.min_final_score
            out.append(GateDecision(
                candidate=c,
                gate_name=self.name,
                outcome=GateOutcome.PASSED if passed else GateOutcome.REJECTED,
                reason="" if passed else (
                    f"dkg_score_below_threshold "
                    f"({c.final_score:.3f} < {self.min_final_score:.3f})"
                ),
                metadata={
                    "value": c.final_score,
                    "threshold": self.min_final_score,
                },
            ))
        return out

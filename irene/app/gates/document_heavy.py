"""Document-heavy gate — H8 lane (cycle 30).

H8's lane is "slow / narrative catalysts": filings, rulings, scheduled
decisions — markets where repricing requires READING, the only place
retail latency isn't already beaten (retro §8.1 / §9). Two cheap
proxies, OR'd:

  - the triggering DKG event's type is in the document-heavy set
    (TRIAL, REGULATORY_DECISION, LAWSUIT, MERGER, NOMINATION, ELECTION
    by default — names from app/stages/data/event_types.yaml);
  - the market's resolution description is long (rule-heavy resolution
    criteria run long; one-liner markets resolve on a glance).

Pure Python, no I/O.
"""

from __future__ import annotations

from app.gates.base import Candidate, Gate, GateDecision, GateOutcome

DEFAULT_DOCUMENT_EVENT_TYPES = (
    "TRIAL",
    "REGULATORY_DECISION",
    "LAWSUIT",
    "MERGER",
    "NOMINATION",
    "ELECTION",
)


class DocumentHeavyGate(Gate):
    def __init__(
        self,
        *,
        name: str = "document_heavy",
        event_types: tuple[str, ...] | list[str] = DEFAULT_DOCUMENT_EVENT_TYPES,
        min_description_chars: int = 800,
    ) -> None:
        self.name = name
        self._types = {str(t).strip().upper() for t in event_types}
        self._min_chars = int(min_description_chars)

    async def evaluate(
        self, candidates: list[Candidate]
    ) -> list[GateDecision]:
        out: list[GateDecision] = []
        for c in candidates:
            etype = (c.event_type or "").strip().upper()
            desc_len = len(c.market_description or "")
            type_hit = bool(etype) and etype in self._types
            desc_hit = desc_len >= self._min_chars
            meta = {
                "event_type": etype or None,
                "description_chars": desc_len,
                "matched": (
                    "event_type" if type_hit
                    else ("description" if desc_hit else None)
                ),
            }
            if type_hit or desc_hit:
                out.append(GateDecision(
                    candidate=c, gate_name=self.name,
                    outcome=GateOutcome.PASSED, metadata=meta,
                ))
            else:
                out.append(GateDecision(
                    candidate=c, gate_name=self.name,
                    outcome=GateOutcome.REJECTED,
                    reason="not_document_heavy", metadata=meta,
                ))
        return out

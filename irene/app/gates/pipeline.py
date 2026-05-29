"""GatePipeline — walks gates in order, threading survivors forward."""

from __future__ import annotations

import logging
from typing import Optional

from app.gates.base import Candidate, Gate, GateDecision, GateOutcome

logger = logging.getLogger(__name__)


class GatePipeline:
    """Stateless pipeline of `Gate` instances. Each candidate's full
    decision history is returned in the order gates ran. Survivors of
    the last gate are the candidates that should be promoted (in the
    explorer's case, get odds capture started)."""

    def __init__(self, gates: list[Gate]) -> None:
        self._gates = list(gates)

    @property
    def gate_names(self) -> list[str]:
        return [g.name for g in self._gates]

    async def run(self, candidates: list[Candidate]) -> dict[int, list[GateDecision]]:
        """Return {article_id: list[GateDecision]} keyed by the (article,
        market) pair the candidate represents — but since the explorer
        feeds many candidates per article (one per market), the key
        actually has to discriminate by market too. We use the tuple
        (article_id, market_condition_id) as the dict key."""
        history: dict[tuple[int, str], list[GateDecision]] = {
            (c.article_id, c.market_condition_id): [] for c in candidates
        }
        survivors = list(candidates)
        for gate in self._gates:
            if not survivors:
                break
            try:
                decisions = await gate.evaluate(survivors)
            except Exception as exc:  # noqa: BLE001 — gates must not raise
                logger.warning("gate %s raised %s; rejecting all survivors",
                               gate.name, exc.__class__.__name__)
                decisions = [
                    GateDecision(
                        candidate=c, gate_name=gate.name,
                        outcome=GateOutcome.REJECTED,
                        reason=f"gate_error: {exc.__class__.__name__}",
                    )
                    for c in survivors
                ]
            next_survivors: list[Candidate] = []
            for d in decisions:
                key = (d.candidate.article_id, d.candidate.market_condition_id)
                history[key].append(d)
                if d.outcome == GateOutcome.PASSED:
                    next_survivors.append(d.candidate)
            survivors = next_survivors
        return {f"{a}:{m}": history[(a, m)] for (a, m) in history}

    @staticmethod
    def passed(history: list[GateDecision]) -> bool:
        """Convenience: did the candidate survive the entire pipeline?"""
        return bool(history) and history[-1].outcome == GateOutcome.PASSED

    @staticmethod
    def rejected_by(history: list[GateDecision]) -> Optional[str]:
        for d in history:
            if d.outcome == GateOutcome.REJECTED:
                return d.gate_name
        return None


def build_pipeline(configs: list, *, llm_caller=None) -> GatePipeline:
    """Construct a GatePipeline from a list of `GateConfig` pydantic
    models (see irene/app/config.py). `llm_caller` is injected for
    LLM-backed gates so the pipeline stays decoupled from the AIClient.

    Dispatch is by `cfg.type`; unknown types raise so misconfiguration
    fails fast at startup.
    """
    from app.gates.dkg import DKGThresholdGate
    from app.gates.llm_batch import LLMBatchGate
    from app.gates.market_open import MarketOpenGate

    gates: list[Gate] = []
    for cfg in configs:
        t = cfg.type
        if t == "market_open":
            gates.append(MarketOpenGate(name=cfg.name))
        elif t == "dkg_threshold":
            gates.append(DKGThresholdGate(
                name=cfg.name, min_final_score=cfg.min_final_score,
            ))
        elif t == "llm_batch":
            if llm_caller is None:
                raise RuntimeError(
                    "llm_batch gate requires an llm_caller injection"
                )
            gates.append(LLMBatchGate(
                name=cfg.name,
                llm_caller=llm_caller,
                model=cfg.model,
                batch_size=cfg.batch_size,
                max_output_tokens=cfg.max_output_tokens,
                prompt_version=cfg.prompt_version,
            ))
        else:
            raise RuntimeError(f"unknown gate type: {t!r}")
    return GatePipeline(gates)

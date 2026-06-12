"""Cycle 30 — H8 lane gate units (midband, already_moving,
document_heavy, resolution_rules) + build_pipeline dispatch."""

from __future__ import annotations

import json
import time

import pytest

from app.gates.already_moving import AlreadyMovingGate
from app.gates.base import Candidate, GateOutcome
from app.gates.document_heavy import DocumentHeavyGate
from app.gates.midband import MidbandGate
from app.gates.resolution_rules import ResolutionRulesGate

pytestmark = pytest.mark.asyncio


def _cand(**kw) -> Candidate:
    base = dict(
        article_id=1,
        market_condition_id="cond-1",
        final_score=0.85,
        article_title="Trump strikes Iran",
        article_body="Targeted operation announced.",
        market_question="Will Trump strike Iran?",
        market_description="Resolution details.",
    )
    base.update(kw)
    return Candidate(**base)


# --- midband ----------------------------------------------------------------


async def test_midband_boundaries_and_unknown():
    gate = MidbandGate(min_mid=0.10, max_mid=0.90)
    cases = {
        0.10: GateOutcome.PASSED,   # inclusive lower bound
        0.50: GateOutcome.PASSED,
        0.90: GateOutcome.PASSED,   # inclusive upper bound
        0.05: GateOutcome.REJECTED,
        0.95: GateOutcome.REJECTED,
    }
    for mid, expected in cases.items():
        [d] = await gate.evaluate([_cand(market_mid=mid)])
        assert d.outcome == expected, f"mid={mid}"
    [d] = await gate.evaluate([_cand(market_mid=None)])
    assert d.outcome == GateOutcome.REJECTED
    assert d.reason == "mid_unknown"


# --- already_moving ----------------------------------------------------------


class _HistoryCLOB:
    def __init__(self, history):
        self.history = history
        self.calls = []

    async def get_price_history(self, token_id, *, interval, fidelity):
        self.calls.append(token_id)
        return self.history


async def test_already_moving_passes_on_move_and_memoizes():
    now = int(time.time())
    # Lookback sample 24h ago at 0.40; now at 0.50 → |move|=0.10 ≥ 0.05.
    clob = _HistoryCLOB([(now - 86_400 + 300, 0.40), (now - 600, 0.49)])
    gate = AlreadyMovingGate(clob=clob, lookback_secs=86_400, min_abs_move=0.05)
    c = _cand(market_mid=0.50, token_yes="ty")
    [d] = await gate.evaluate([c])
    assert d.outcome == GateOutcome.PASSED
    assert d.metadata["mid_lookback"] == pytest.approx(0.40)
    # Second evaluation hits the memo — no extra history call.
    await gate.evaluate([c])
    assert clob.calls == ["ty"]


async def test_already_moving_rejects_flat_and_failure_modes():
    now = int(time.time())
    clob = _HistoryCLOB([(now - 86_400, 0.49)])
    gate = AlreadyMovingGate(clob=clob, lookback_secs=86_400, min_abs_move=0.05)
    [d] = await gate.evaluate([_cand(market_mid=0.50, token_yes="ty")])
    assert d.outcome == GateOutcome.REJECTED
    assert d.reason == "not_moving"
    # Missing inputs.
    [d] = await gate.evaluate([_cand(market_mid=None, token_yes="ty")])
    assert d.reason == "inputs_unknown"
    [d] = await gate.evaluate([_cand(market_mid=0.5, token_yes=None)])
    assert d.reason == "inputs_unknown"
    # History unavailable (clob without the method must not raise).
    class _NoHistory:
        pass
    gate2 = AlreadyMovingGate(clob=_NoHistory())
    [d] = await gate2.evaluate([_cand(market_mid=0.5, token_yes="ty")])
    assert d.outcome == GateOutcome.REJECTED
    assert d.reason == "history_unavailable"
    # History exists but no sample near the lookback target.
    gate3 = AlreadyMovingGate(clob=_HistoryCLOB([(now - 100, 0.5)]))
    [d] = await gate3.evaluate([_cand(market_mid=0.5, token_yes="ty")])
    assert d.reason == "lookback_sample_missing"


# --- document_heavy ----------------------------------------------------------


async def test_document_heavy_event_type_or_long_description():
    gate = DocumentHeavyGate(min_description_chars=100)
    [d] = await gate.evaluate([_cand(event_type="REGULATORY_DECISION")])
    assert d.outcome == GateOutcome.PASSED
    assert d.metadata["matched"] == "event_type"
    [d] = await gate.evaluate([_cand(market_description="x" * 150)])
    assert d.outcome == GateOutcome.PASSED
    assert d.metadata["matched"] == "description"
    [d] = await gate.evaluate([_cand(event_type="EARNINGS")])
    assert d.outcome == GateOutcome.REJECTED
    assert d.reason == "not_document_heavy"
    [d] = await gate.evaluate([_cand()])  # no event, short description
    assert d.outcome == GateOutcome.REJECTED


# --- resolution_rules ---------------------------------------------------------


class _StubLLM:
    def __init__(self, response):
        self.response = response
        self.calls = []

    async def __call__(self, *, system, user, model, max_tokens, prompt_version):
        self.calls.append({"system": system, "user": user,
                           "prompt_version": prompt_version})
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


async def test_resolution_rules_opens_passes_and_features_recorded():
    llm = _StubLLM(json.dumps({"verdicts": [
        {"label": "A", "opens": True, "still_open": True,
         "rule_relevant": True, "short_reason": "filing names criterion"},
        {"label": "B", "opens": False, "still_open": False,
         "rule_relevant": True, "short_reason": "already determined"},
    ]}))
    gate = ResolutionRulesGate(llm_caller=llm)
    a = _cand(market_condition_id="m-a")
    b = _cand(market_condition_id="m-b")
    da, db_ = await gate.evaluate([a, b])
    assert da.outcome == GateOutcome.PASSED
    assert da.metadata["features"]["still_open"] is True
    assert db_.outcome == GateOutcome.REJECTED
    assert db_.reason == "not_rule_opening"
    assert db_.metadata["features"]["still_open"] is False
    # Prompt includes the resolution criteria slice.
    assert "Rules:" in llm.calls[0]["user"]
    assert llm.calls[0]["prompt_version"] == "rr-v1"


async def test_resolution_rules_malformed_and_error_paths_reject():
    gate = ResolutionRulesGate(llm_caller=_StubLLM("not json at all"))
    [d] = await gate.evaluate([_cand()])
    assert d.outcome == GateOutcome.REJECTED
    assert d.reason.startswith("parse_error")
    gate2 = ResolutionRulesGate(llm_caller=_StubLLM(""))
    [d] = await gate2.evaluate([_cand()])
    assert d.reason.startswith("parse_error")
    gate3 = ResolutionRulesGate(llm_caller=_StubLLM(RuntimeError("boom")))
    [d] = await gate3.evaluate([_cand()])
    assert d.reason.startswith("llm_error")


# --- build_pipeline dispatch ---------------------------------------------------


async def test_build_pipeline_constructs_h8_gates():
    from app.config import (
        AlreadyMovingGateConfig,
        DocumentHeavyGateConfig,
        MidbandGateConfig,
        ResolutionRulesGateConfig,
    )
    from app.gates import build_pipeline

    async def _llm(**kw):  # pragma: no cover — never called here
        return ""

    pipeline = build_pipeline(
        [
            MidbandGateConfig(),
            AlreadyMovingGateConfig(),
            DocumentHeavyGateConfig(),
            ResolutionRulesGateConfig(),
        ],
        llm_caller=_llm,
        clob=_HistoryCLOB([]),
    )
    assert pipeline.gate_names == [
        "midband", "already_moving", "document_heavy", "resolution_rules",
    ]
    # already_moving without a clob injection fails fast at startup.
    with pytest.raises(RuntimeError, match="clob"):
        build_pipeline([AlreadyMovingGateConfig()], llm_caller=_llm)
    # resolution_rules without an llm_caller fails fast too.
    with pytest.raises(RuntimeError, match="llm_caller"):
        build_pipeline([ResolutionRulesGateConfig()], clob=_HistoryCLOB([]))

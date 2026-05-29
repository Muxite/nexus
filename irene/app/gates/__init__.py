"""Pluggable gate pipeline for the explorer.

A `Gate` is one filtering step on a (article, market) candidate list.
The pipeline walks gates in order: each gate evaluates the survivors of
the previous one and returns per-candidate `GateDecision` records.
Rejected candidates accumulate into the full decision history and are
persisted to `explorations` with `gate_status='rejected'` for audit.

Add a new gate by:
1. Subclassing `Gate` (in `base.py`) and implementing `evaluate()`.
2. Adding a config sub-model and dispatch in `pipeline.build_pipeline()`.
"""

from app.gates.base import Candidate, Gate, GateDecision, GateOutcome
from app.gates.pipeline import GatePipeline, build_pipeline

__all__ = [
    "Candidate", "Gate", "GateDecision", "GateOutcome",
    "GatePipeline", "build_pipeline",
]

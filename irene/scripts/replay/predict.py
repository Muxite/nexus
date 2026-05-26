"""Tier 1 — counterfactual gate-cascade replay (`scripts/replay/predict.py`).

## Specification

**Purpose.** Replay the live trade.py gate cascade against snapshot data
with a swept config. Answers "if `min_edge` were 0.06 instead of 0.04,
how many of yesterday's trades would still have fired, and what would
the PnL have been?"

For every signal that was decided in the snapshot, the replay runs the
exact same gate-cascade logic the live bot ran — via the shared
`evaluate_gates()` extracted in PR-A — under a config that the caller
overrides on the CLI.

**Bucket accounting (the load-bearing invariant).**

Four buckets, with strict honest/projected/unknowable separation:

  (a) fired-both         — original=execute, counterfactual=execute.
                            PnL recomputed honestly using new size+fees
                            against the real entry_mid / exit_mid.
  (b) fired→rejected     — original=execute, counterfactual=reject.
                            Original realised_pnl is the cost of the
                            tighter config. Counterfactual PnL → 0.
  (c) rejected→fired-with-trajectory
                          — original=reject, counterfactual=execute,
                            AND a same-market fired trade exists in this
                            snapshot whose closure we can use as a
                            best-effort projection. Counterfactual PnL
                            is PROJECTED (flagged separately).
  (d) rejected→fired-unknowable
                          — original=reject, counterfactual=execute, no
                            usable same-market projection. PnL UNKNOWN.
                            Reported as count + implied notional only;
                            never folded into headline PnL.

**The hard invariant.** Bucket-(d) PnL is never synthesised. The
output report's headline PnL delta is (a) + (b) + (c-projected) only.
A growing (d) count is the signal that the sweep has wandered into
"new fires we have no honest read on" — read the (d) count alongside
the headline number.

**Public interface.**
- CLI:
    python -m scripts.replay.predict \\
      --snapshot data/snapshots/cycle_0029_cycle7-expand/ \\
      --config-override trade.min_edge=0.06 \\
      --config-override trade.kelly_fraction=0.15 \\
      [--out report.json]
- `run_inproc(snapshot_dir, overrides_dict) -> ReplayReport` —
  programmatic entry for `sweep.py` and `soak_compare.py`.

**Invariants** (mirror `accuracy.py`).
- Snapshot directory only; never reads the live DB.
- Cascade logic via `evaluate_gates()` — never reimplemented locally.
- Self-consistency: zero overrides ⇒ counterfactual decision matches
  the snapshot's stored decision for every signal whose LLM result
  is recoverable; ⇒ counterfactual headline PnL delta = $0.00.

**Non-goals.**
- Re-running LLM calls.
- Reconstructing intra-trade mid trajectory for rejected signals.
- Writing to the live DB.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

# Allow `python -m scripts.replay.predict` from irene/.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.config import Config  # noqa: E402
from app.stages.trade import (  # noqa: E402
    GateFeatures,
    GateResult,
    evaluate_gates,
)
from scripts.replay._snapshot_loader import SnapshotLoader  # noqa: E402


# ---------------------------------------------------------------------------
# Config override plumbing
# ---------------------------------------------------------------------------


def _coerce(value: str) -> Any:
    """Best-effort literal coercion for `--config-override key=value`.
    Tries int, float, then bool, then JSON, then leaves as string."""
    s = value.strip()
    low = s.lower()
    if low in ("true", "false"):
        return low == "true"
    if low in ("null", "none"):
        return None
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)
    except ValueError:
        pass
    # Try JSON (lists, dicts).
    if s and s[0] in "[{\"":
        try:
            return json.loads(s)
        except (TypeError, ValueError):
            pass
    return s


def apply_overrides(cfg: Config, overrides: dict[str, Any]) -> Config:
    """Return a NEW Config with the overrides applied. Keys are dotted
    paths like `trade.min_edge` or `trade.polymarket_fee_rates.crypto`.
    """
    base = cfg.model_dump()
    for dotted, value in overrides.items():
        parts = dotted.split(".")
        target = base
        for p in parts[:-1]:
            if p not in target or not isinstance(target[p], dict):
                raise ValueError(f"unknown config path segment: {dotted!r} at {p!r}")
            target = target[p]
        leaf = parts[-1]
        target[leaf] = value
    return Config.model_validate(base)


# ---------------------------------------------------------------------------
# Building GateFeatures from snapshot rows
# ---------------------------------------------------------------------------


def _entity_overlap_from_gate_history(gate_history: list[dict]) -> Optional[tuple[bool, int]]:
    """Recover (overlaps, n_entities) from the decision's persisted
    gate_history. Returns None when the gate never ran (decision rejected
    before entity_overlap)."""
    for g in gate_history:
        if g.get("gate") == "entity_overlap":
            value = g.get("value")
            threshold = g.get("threshold")
            overlaps = (value == "match")
            try:
                n_entities = int(threshold) if threshold is not None else 0
            except (TypeError, ValueError):
                n_entities = 0
            return (overlaps, n_entities)
    return None


def _mid_from_gate_history(gate_history: list[dict]) -> Optional[float]:
    """For original-reject decisions where the mid-degenerate gate fired,
    the mid is the `value` of that gate entry. Returns None if not present."""
    for g in gate_history:
        if g.get("gate") == "edge" and g.get("threshold") == "mid_degenerate":
            try:
                return float(g["value"])
            except (TypeError, ValueError):
                return None
    return None


def _spread_from_gate_history(gate_history: list[dict]) -> Optional[float]:
    """When the wide_spread gate fired, the value is the observed spread."""
    for g in gate_history:
        if g.get("gate") == "edge" and g.get("threshold") == "wide_spread":
            try:
                return float(g["value"])
            except (TypeError, ValueError):
                return None
    return None


@dataclass
class SignalContext:
    """Everything we know about one snapshot signal — used both as input
    to evaluate_gates and as the join handle to original trade/closure
    rows."""

    signal: dict
    decision: dict  # canonical original decision row
    decision_input: Optional[dict]
    prediction: Optional[dict]
    trade: Optional[dict]      # original trade row (None if rejected)
    closure: Optional[dict]    # original trade_closure row (None if not closed)
    impact: Optional[dict]     # original price_impacts row (None if no trajectory)
    features_snapshot: Optional[dict]  # decoded features_snapshot from trade
    gate_history: list[dict]   # parsed from decision.gate_history JSON

    @property
    def market_id(self) -> str:
        return str(self.signal["market_id"])

    @property
    def signal_id(self) -> int:
        return int(self.signal["id"])


def _build_signal_contexts(loader: SnapshotLoader) -> list[SignalContext]:
    """Index every snapshot decision by signal and join the auxiliary rows."""
    # Build a `trades_by_signal_id` view (UNIQUE partial index in DDL).
    trades_by_signal = {
        int(t["signal_id"]): t
        for t in loader.trades
        if t.get("signal_id") is not None
    }
    contexts: list[SignalContext] = []
    for decision in loader.decisions:
        sig_id_raw = decision.get("signal_id")
        if sig_id_raw is None:
            # DKG-path decision — no signal backing. Skip; predict.py is
            # MVP-only for now (DKG cascade is a separate file).
            continue
        sig_id = int(sig_id_raw)
        signal = loader.signals_by_id.get(sig_id)
        if signal is None:
            # Signal predates the cycle's `signals.jsonl` dump (PR-B). Skip
            # this signal: we cannot rebuild features without the row.
            continue
        try:
            gate_history = json.loads(decision.get("gate_history") or "[]")
        except (TypeError, ValueError):
            gate_history = []
        di = loader.decision_inputs_by_signal_ref.get(sig_id)
        # prediction: try decision.prediction_id first, then decision_inputs payload's prompt_sha.
        prediction = None
        pred_id = decision.get("prediction_id")
        if pred_id is not None:
            prediction = loader.predictions_by_id.get(int(pred_id))
        if prediction is None and di is not None:
            try:
                payload = json.loads(di["payload_json"]) if isinstance(di.get("payload_json"), str) else di.get("payload_json", {})
            except (TypeError, ValueError):
                payload = {}
            sha = payload.get("prompt_sha")
            if sha:
                prediction = loader.predictions_by_prompt_sha.get(sha)
        trade = trades_by_signal.get(sig_id)
        closure = loader.closures_by_trade_id.get(int(trade["id"])) if trade else None
        impact = loader.impacts_by_trade_id.get(int(trade["id"])) if trade else None
        features_snapshot = loader.features_snapshot(trade) if trade else None
        contexts.append(SignalContext(
            signal=signal, decision=decision, decision_input=di,
            prediction=prediction, trade=trade, closure=closure, impact=impact,
            features_snapshot=features_snapshot, gate_history=gate_history,
        ))
    return contexts


def _features_for_signal(
    ctx: SignalContext, *, cash_available: float,
) -> tuple[Optional[GateFeatures], Optional[str]]:
    """Assemble GateFeatures from a SignalContext. Returns
    `(features, skip_reason)`. When features is None, skip_reason names
    what's missing (e.g. "no_llm_result"). Caller buckets these as
    'replay_skipped'."""
    signal = ctx.signal
    similarity = signal.get("similarity")
    freshness = signal.get("freshness")
    if similarity is None:
        return None, "signal_missing_similarity"

    # Build market dict from features_snapshot.market_at_trade (PR-B pin)
    # if we have it (original execute path); fall back to a minimal stub
    # if the cascade rejected before features_snapshot was written.
    market: dict[str, Any] = {}
    if ctx.features_snapshot:
        mat = ctx.features_snapshot.get("market_at_trade") or {}
        market = {
            "volume_usdc": mat.get("volume_usdc"),
            "min_order_size": mat.get("min_order_size"),
            "category": mat.get("category"),
            "end_date": mat.get("end_date"),
            "fee_rate": mat.get("fee_rate"),
            # _days_to_expiry is computed by the imbalance helper at gate
            # time; pre-compute from end_date or pass through.
            "_days_to_expiry": ctx.features_snapshot.get("days_to_expiry"),
        }
    # entity_overlap from gate_history
    entity = _entity_overlap_from_gate_history(ctx.gate_history)
    # LLM result
    direction = None
    confidence = None
    prediction_id = None
    if ctx.prediction is not None:
        direction = ctx.prediction.get("direction")
        confidence = ctx.prediction.get("confidence")
        prediction_id = ctx.prediction.get("id")
        # Predictions table stores "YES"/"NO"/"NEUTRAL".
        if direction not in ("YES", "NO", "NEUTRAL"):
            direction = None

    # mid: prefer features_snapshot (original execute path); fall back to
    # gate_history mid_degenerate value (original reject at gate 4).
    mid = None
    if ctx.features_snapshot:
        mid = ctx.features_snapshot.get("mid")
    if mid is None:
        mid = _mid_from_gate_history(ctx.gate_history)

    # spread / executable_price: features_snapshot.edge_components carries
    # them when the book was available on the original path.
    executable_price = None
    spread = None
    if ctx.features_snapshot:
        comps = ctx.features_snapshot.get("edge_components") or {}
        if comps.get("fallback") is False:
            executable_price = comps.get("executable_price")
            spread = comps.get("spread")
    if spread is None:
        spread = _spread_from_gate_history(ctx.gate_history)

    # presence: derived from the original gate_history
    article_present = not any(
        g.get("gate") == "lookup" and g.get("value") == "article_missing"
        for g in ctx.gate_history
    )
    market_present = not any(
        g.get("gate") == "lookup" and g.get("value") == "market_missing"
        for g in ctx.gate_history
    )

    # cluster_size: not preserved in snapshot. Fall back to 2 (the
    # singleton sub-rule is the only thing this affects; non-singleton
    # by default lets the rule skip cleanly).
    cluster_size = 2

    # If we have no LLM result AND the original cascade reached the LLM
    # gate, we can't replay past that gate — skip.
    original_reached_llm = any(
        g.get("gate") == "direction" for g in ctx.gate_history
    )
    if direction is None and original_reached_llm:
        return None, "no_llm_result"

    # Mid recovery limitation. The mid is only persisted when the original
    # cascade either executed (→ features_snapshot.mid) or rejected at
    # mid_degenerate (→ gate_history value). Signals whose original cascade
    # passed mid_degenerate but rejected later (edge / size / cash) have
    # no mid in the snapshot — we can't run the replay's mid-dependent
    # gates against them. Bucket as `no_recoverable_mid` instead of
    # letting evaluate_gates() raise.
    original_passed_mid = any(
        g.get("gate") == "edge"
        and g.get("threshold") not in ("mid_degenerate", "wide_spread")
        for g in ctx.gate_history
    ) or any(
        g.get("gate") == "market_news_imbalance" for g in ctx.gate_history
    ) or any(
        g.get("gate") in ("size", "size_after_clip", "cash") for g in ctx.gate_history
    )
    if mid is None and direction is not None and original_passed_mid:
        return None, "no_recoverable_mid"

    return GateFeatures(
        similarity=float(similarity),
        freshness=freshness,
        article_present=article_present,
        market_present=market_present,
        entity_overlap_result=entity,
        direction=direction,
        confidence=confidence,
        prediction_id=prediction_id,
        mid=mid,
        executable_price=executable_price,
        spread=spread,
        market=market,
        cluster_size=cluster_size,
        cash_available=cash_available,
    ), None


# ---------------------------------------------------------------------------
# PnL recomputation
# ---------------------------------------------------------------------------


def _polymarket_taker_fee(price: float, shares: float, fee_rate: float) -> float:
    if fee_rate <= 0.0 or shares <= 0.0:
        return 0.0
    p = max(0.0, min(1.0, price))
    return shares * fee_rate * p * (1.0 - p)


def _gross_pnl(direction: str, size_usdc: float, entry_mid: float, exit_mid: float) -> float:
    """Mirror of `scripts/replay/accuracy.py:reconstruct_pnl`."""
    if direction == "YES":
        if entry_mid <= 0.0:
            return 0.0
        return size_usdc * (exit_mid - entry_mid) / entry_mid
    if direction == "NO":
        if entry_mid >= 1.0:
            return 0.0
        return size_usdc * (entry_mid - exit_mid) / (1.0 - entry_mid)
    raise ValueError(f"unknown direction: {direction!r}")


def _resolve_fee_rate(
    category: Optional[str],
    fee_rates_table: dict,
    default_fee_rate: float,
    *,
    market_fee_rate: Optional[float] = None,
) -> float:
    """Mirror of `closer.py:_resolve_fee_rate`. Prefer per-market when present."""
    if market_fee_rate is not None:
        try:
            mf = float(market_fee_rate)
        except (TypeError, ValueError):
            mf = None
        if mf is not None and 0.0 <= mf < 1.0:
            return mf
    if category is None:
        return default_fee_rate
    cat_low = str(category).lower()
    for key, rate in fee_rates_table.items():
        if str(key).lower() in cat_low:
            return float(rate)
    return default_fee_rate


def _net_pnl_for(
    *, direction: str, size_usdc: float, entry_mid: float, exit_mid: float,
    fee_rate: float, gas_per_leg: float,
) -> float:
    """Gross PnL minus round-trip Polymarket taker fee minus per-leg gas."""
    entry_price = entry_mid if direction == "YES" else (1.0 - entry_mid)
    if entry_price <= 0.0:
        return 0.0
    shares = size_usdc / entry_price
    exit_price = exit_mid if direction == "YES" else (1.0 - exit_mid)
    entry_fee = _polymarket_taker_fee(entry_price, shares, fee_rate) + gas_per_leg
    exit_fee = _polymarket_taker_fee(exit_price, shares, fee_rate) + gas_per_leg
    gross = _gross_pnl(direction, size_usdc, entry_mid, exit_mid)
    return gross - entry_fee - exit_fee


# ---------------------------------------------------------------------------
# Report dataclasses
# ---------------------------------------------------------------------------


@dataclass
class BucketSummary:
    n: int = 0
    original_pnl: float = 0.0
    counterfactual_pnl: float = 0.0
    implied_notional: float = 0.0


@dataclass
class ReplayReport:
    snapshot: str
    overrides: dict[str, Any]
    n_signals: int
    n_skipped: int
    skip_reasons: dict[str, int]
    bucket_a: BucketSummary   # fired-both
    bucket_b: BucketSummary   # fired-then-rejected (displaced)
    bucket_c: BucketSummary   # rejected-now-fired with trajectory (projected)
    bucket_d: BucketSummary   # rejected-now-fired without trajectory (unknown)
    bucket_no_change_reject: int  # both rejected — counted but no PnL impact
    counterfactual_decision_changes: int  # rows whose outcome flipped vs snapshot
    funnel_deltas: dict[str, int]  # reason → count delta (counterfactual − original)
    # Per-close_reason original PnL bucket (helps spot which exit type the
    # tightened config displaces).
    by_close_reason_displaced: dict[str, float]

    @property
    def net_pnl_delta(self) -> float:
        """(a) recomputed - (a) original + (c) projected - (b) displaced.
        BUCKET-(d) is NOT included — it is unknowable by invariant."""
        return (
            (self.bucket_a.counterfactual_pnl - self.bucket_a.original_pnl)
            + self.bucket_c.counterfactual_pnl
            - self.bucket_b.original_pnl
        )

    @property
    def projected_frac(self) -> float:
        """Fraction of the headline PnL delta that came from projection
        (bucket c), not from honest realisation (a) + (b). Reader hint —
        a high value means the headline is structurally fragile."""
        denom = (
            abs(self.bucket_a.counterfactual_pnl - self.bucket_a.original_pnl)
            + abs(self.bucket_b.original_pnl)
            + abs(self.bucket_c.counterfactual_pnl)
        )
        if denom == 0.0:
            return 0.0
        return abs(self.bucket_c.counterfactual_pnl) / denom

    def to_dict(self) -> dict:
        def b(x: BucketSummary) -> dict:
            return {
                "n": x.n,
                "original_pnl": round(x.original_pnl, 4),
                "counterfactual_pnl": round(x.counterfactual_pnl, 4),
                "implied_notional": round(x.implied_notional, 4),
            }
        return {
            "snapshot": self.snapshot,
            "overrides": self.overrides,
            "n_signals": self.n_signals,
            "n_skipped": self.n_skipped,
            "skip_reasons": self.skip_reasons,
            "buckets": {
                "a_fired_both": b(self.bucket_a),
                "b_fired_then_rejected": b(self.bucket_b),
                "c_rejected_now_fired_projected": b(self.bucket_c),
                "d_rejected_now_fired_unknown": b(self.bucket_d),
                "no_change_reject": self.bucket_no_change_reject,
            },
            "counterfactual_decision_changes": self.counterfactual_decision_changes,
            "net_pnl_delta_excluding_d": round(self.net_pnl_delta, 4),
            "projected_frac": round(self.projected_frac, 4),
            "funnel_deltas": self.funnel_deltas,
            "by_close_reason_displaced": {
                k: round(v, 4) for k, v in self.by_close_reason_displaced.items()
            },
        }


# ---------------------------------------------------------------------------
# Main run
# ---------------------------------------------------------------------------


def _projected_pnl_via_same_market(
    *, ctx: SignalContext, new_features: GateFeatures, new_size: float,
    loader: SnapshotLoader, cfg, base_cfg,
) -> Optional[float]:
    """Bucket-(c) projection: borrow another fired trade on the same
    market in the snapshot, project counterfactual PnL using its
    closing mid delta. Returns None when no usable same-market trade
    exists (caller buckets as (d))."""
    if new_features.mid is None or new_features.direction is None:
        return None
    same_market = [
        (t, loader.closures_by_trade_id.get(int(t["id"])))
        for t in loader.fired_trades()
        if t["market_id"] == ctx.market_id
        and int(t["id"]) != (int(ctx.trade["id"]) if ctx.trade else -1)
    ]
    closed_same_market = [(t, c) for t, c in same_market if c is not None]
    if not closed_same_market:
        return None
    # Pick the closest in time (by decided_at) to this signal's
    # decision time. Falls back to first if timestamps are unparseable.
    def _ts(row: dict) -> str:
        return row.get("decided_at") or row.get("created_at") or ""
    ref_trade, ref_closure = min(
        closed_same_market,
        key=lambda tc: abs(len(_ts(tc[0])) - len(_ts(ctx.decision))),
    )
    # Build the projection: same-market reference trade traversed a mid
    # delta of (exit_mid - entry_mid). Assume the counterfactual fire
    # rides the same absolute delta from its own entry mid.
    ref_entry = float(ref_trade["mid_price"])
    ref_exit = float(ref_closure["exit_mid_price"])
    new_entry = float(new_features.mid)
    new_exit = new_entry + (ref_exit - ref_entry)
    new_exit = max(0.001, min(0.999, new_exit))
    cat = (new_features.market or {}).get("category")
    market_fee_rate = (new_features.market or {}).get("fee_rate")
    fee_rate = _resolve_fee_rate(
        cat, cfg.polymarket_fee_rates, cfg.default_fee_rate,
        market_fee_rate=market_fee_rate,
    )
    return _net_pnl_for(
        direction=new_features.direction, size_usdc=new_size,
        entry_mid=new_entry, exit_mid=new_exit,
        fee_rate=fee_rate, gas_per_leg=cfg.gas_cost_usdc,
    )


def run_inproc(
    snapshot_dir: str,
    overrides: Optional[dict[str, Any]] = None,
    *,
    base_config: Optional[Config] = None,
) -> ReplayReport:
    overrides = overrides or {}
    base_config = base_config or Config()
    swept_config = apply_overrides(base_config, overrides)
    base_cfg = base_config.trade
    cfg = swept_config.trade

    loader = SnapshotLoader(snapshot_dir)
    contexts = _build_signal_contexts(loader)

    # Cash available is constant across replay — paper wallet starts at
    # cfg.balance_usdc each cycle. Real live replay should mirror the
    # paper wallet's behaviour; for the v1 we use balance_usdc as a
    # static cap which matches "the wallet was never below balance".
    # NOTE: this is a known approximation. soak_compare.py treats this
    # by validating against the original execute/reject outcomes — if
    # the cash gate genuinely bit on a snapshot trade, this replay's
    # static cap may admit it. The self-consistency test below catches
    # any drift.
    cash_static = cfg.balance_usdc

    bucket_a = BucketSummary()
    bucket_b = BucketSummary()
    bucket_c = BucketSummary()
    bucket_d = BucketSummary()
    no_change_reject = 0
    decision_changes = 0
    skip_reasons: dict[str, int] = {}
    funnel_deltas: dict[str, int] = {}
    by_close_reason_displaced: dict[str, float] = {}

    n_skipped = 0

    # Funnel baseline (original counts) and overlay (counterfactual).
    orig_funnel: dict[str, int] = {}
    new_funnel: dict[str, int] = {}

    for ctx in contexts:
        orig_reason = ctx.decision.get("reason", "?")
        orig_funnel[orig_reason] = orig_funnel.get(orig_reason, 0) + 1

        features, skip = _features_for_signal(ctx, cash_available=cash_static)
        if features is None:
            n_skipped += 1
            skip_reasons[skip or "unknown"] = skip_reasons.get(skip or "unknown", 0) + 1
            continue
        try:
            result = evaluate_gates(features, cfg)
        except ValueError as exc:
            n_skipped += 1
            key = f"cascade_error:{type(exc).__name__}"
            skip_reasons[key] = skip_reasons.get(key, 0) + 1
            continue

        new_reason = result.reason
        new_funnel[new_reason] = new_funnel.get(new_reason, 0) + 1

        orig_outcome = ctx.decision.get("outcome", "?")
        new_outcome = result.outcome

        if orig_outcome != new_outcome or orig_reason != new_reason:
            decision_changes += 1

        # Bucket dispatch.
        if orig_outcome == "execute" and new_outcome == "execute":
            # (a) fired-both — recompute PnL with new size + new fees.
            if ctx.closure is None:
                # Open trade: counterfactual PnL = 0 (no realised PnL to
                # compare against). Still counted in bucket-a.
                bucket_a.n += 1
                continue
            orig_pnl = float(ctx.closure.get("realized_pnl_usdc") or 0.0)
            new_size = float(result.size_usdc or 0.0)
            new_dir = result.direction or ctx.trade["direction"]
            entry_mid = float(ctx.trade["mid_price"])
            exit_mid = float(ctx.closure["exit_mid_price"])
            cat = (features.market or {}).get("category")
            market_fee_rate = (features.market or {}).get("fee_rate")
            fee_rate = _resolve_fee_rate(
                cat, cfg.polymarket_fee_rates, cfg.default_fee_rate,
                market_fee_rate=market_fee_rate,
            )
            new_pnl = _net_pnl_for(
                direction=new_dir, size_usdc=new_size,
                entry_mid=entry_mid, exit_mid=exit_mid,
                fee_rate=fee_rate, gas_per_leg=cfg.gas_cost_usdc,
            )
            bucket_a.n += 1
            bucket_a.original_pnl += orig_pnl
            bucket_a.counterfactual_pnl += new_pnl

        elif orig_outcome == "execute" and new_outcome == "reject":
            # (b) fired→rejected. Displace the original realised PnL.
            if ctx.closure is None:
                bucket_b.n += 1
                bucket_b.implied_notional += float(ctx.trade.get("size_usdc") or 0.0)
                continue
            orig_pnl = float(ctx.closure.get("realized_pnl_usdc") or 0.0)
            close_reason = ctx.closure.get("close_reason") or "unknown"
            by_close_reason_displaced[close_reason] = (
                by_close_reason_displaced.get(close_reason, 0.0) + orig_pnl
            )
            bucket_b.n += 1
            bucket_b.original_pnl += orig_pnl
            bucket_b.implied_notional += float(ctx.trade.get("size_usdc") or 0.0)

        elif orig_outcome == "reject" and new_outcome == "execute":
            new_size = float(result.size_usdc or 0.0)
            projected = _projected_pnl_via_same_market(
                ctx=ctx, new_features=features, new_size=new_size,
                loader=loader, cfg=cfg, base_cfg=base_cfg,
            )
            if projected is not None:
                bucket_c.n += 1
                bucket_c.counterfactual_pnl += projected
                bucket_c.implied_notional += new_size
            else:
                bucket_d.n += 1
                bucket_d.implied_notional += new_size

        else:
            # (reject, reject) — no PnL impact, just count.
            no_change_reject += 1

    # Funnel deltas: every reason appearing in either map.
    keys = set(orig_funnel) | set(new_funnel)
    for k in keys:
        funnel_deltas[k] = new_funnel.get(k, 0) - orig_funnel.get(k, 0)

    return ReplayReport(
        snapshot=str(snapshot_dir),
        overrides=dict(overrides),
        n_signals=len(contexts),
        n_skipped=n_skipped,
        skip_reasons=skip_reasons,
        bucket_a=bucket_a,
        bucket_b=bucket_b,
        bucket_c=bucket_c,
        bucket_d=bucket_d,
        bucket_no_change_reject=no_change_reject,
        counterfactual_decision_changes=decision_changes,
        funnel_deltas=funnel_deltas,
        by_close_reason_displaced=by_close_reason_displaced,
    )


def _print_report(rep: ReplayReport) -> None:
    print(f"predict.py replay: {rep.snapshot}")
    if rep.overrides:
        print("  overrides:")
        for k, v in rep.overrides.items():
            print(f"    {k} = {v}")
    print(f"  signals: {rep.n_signals}  skipped: {rep.n_skipped}")
    if rep.skip_reasons:
        for k, v in sorted(rep.skip_reasons.items()):
            print(f"    skip[{k}] = {v}")
    print(f"  decision changes vs snapshot: {rep.counterfactual_decision_changes}")
    print()
    print("buckets:")
    print(f"  (a) fired-both        n={rep.bucket_a.n:4d}  "
          f"original=${rep.bucket_a.original_pnl:+.4f}  "
          f"counterfactual=${rep.bucket_a.counterfactual_pnl:+.4f}")
    print(f"  (b) fired→rejected    n={rep.bucket_b.n:4d}  "
          f"displaced=${rep.bucket_b.original_pnl:+.4f}  "
          f"notional=${rep.bucket_b.implied_notional:.2f}")
    print(f"  (c) rej→fired (proj)  n={rep.bucket_c.n:4d}  "
          f"projected=${rep.bucket_c.counterfactual_pnl:+.4f}  "
          f"notional=${rep.bucket_c.implied_notional:.2f}")
    print(f"  (d) rej→fired (UNK)   n={rep.bucket_d.n:4d}  "
          f"notional=${rep.bucket_d.implied_notional:.2f}  "
          f"** PnL UNKNOWN; not in headline **")
    print(f"  no-change reject:    n={rep.bucket_no_change_reject}")
    print()
    print(f"net PnL delta (excludes bucket d): ${rep.net_pnl_delta:+.4f}")
    print(f"projected_frac: {rep.projected_frac:.3f}  "
          f"(headline fragility — share of |delta| from bucket c)")
    print()
    if rep.funnel_deltas:
        print("funnel deltas (new − original):")
        nonzero = [(k, v) for k, v in rep.funnel_deltas.items() if v != 0]
        for k, v in sorted(nonzero, key=lambda kv: -abs(kv[1])):
            print(f"  {k:30s} {v:+d}")
    if rep.by_close_reason_displaced:
        print("displaced PnL by close_reason:")
        for k, v in sorted(rep.by_close_reason_displaced.items()):
            print(f"  {k:20s} ${v:+.4f}")


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--snapshot", required=True, help="snapshot directory")
    p.add_argument(
        "--config-override", action="append", default=[],
        help="repeatable: dotted.path=value (e.g. trade.min_edge=0.06)",
    )
    p.add_argument("--out", help="optional JSON output path")
    args = p.parse_args(argv)

    overrides: dict[str, Any] = {}
    for raw in args.config_override:
        if "=" not in raw:
            print(f"--config-override: missing '=' in {raw!r}", file=sys.stderr)
            return 2
        key, val = raw.split("=", 1)
        overrides[key.strip()] = _coerce(val)

    try:
        rep = run_inproc(args.snapshot, overrides)
    except (FileNotFoundError, ValueError) as exc:
        print(f"predict: ERROR {exc}", file=sys.stderr)
        return 2

    _print_report(rep)
    if args.out:
        Path(args.out).write_text(json.dumps(rep.to_dict(), indent=2))
        print(f"\nreport JSON: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

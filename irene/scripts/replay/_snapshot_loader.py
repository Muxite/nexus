"""Snapshot loader — read + index the per-cycle JSONL dumps written by
`scripts/analysis/snapshot_cycle.py`.

## Specification

**Purpose.** Provide one programmatic entry point for the replay scripts
(`predict.py`, `impact.py`, `sweep.py`, `source_pnl.py`, `soak_compare.py`)
to load a snapshot directory into typed, indexed-by-id collections without
re-implementing JSONL parsing five times.

**Public interface.**
- `SnapshotLoader(snapshot_dir: str | Path)` — constructor takes a path
  to one `cycle_NNNN_<name>/` directory.
- `.manifest` — parsed `manifest.json` dict.
- `.trades`, `.decisions`, `.predictions`, `.trade_closures`,
  `.decision_inputs`, `.trade_features`, `.signals`, `.price_impacts`,
  `.llm_probe_calls`, `.llm_calls`, `.llm_parse_failures` — `list[dict]`
  rows from the matching `*.jsonl`. Files that don't exist resolve to an
  empty list (`[]`). Files that exist but are empty also resolve to `[]`.
- `.trades_by_id`, `.signals_by_id`, etc. — `dict[int, dict]` lazy
  indexes for the most-joined collections.
- `.closures_by_trade_id` — `dict[int, dict]` for the canonical 1:1 join.
- `.impacts_by_trade_id` — same shape for `price_impacts.trade_id` (UNIQUE).
- `.predictions_by_id` — `dict[int, dict]` keyed by `predictions.id`.
- `.decisions_by_signal_id` — `dict[int, dict]` (UNIQUE on signal_id, MVP).

**Invariants.**
- Snapshot directory must exist and contain `manifest.json`. A missing
  manifest raises `FileNotFoundError`.
- Loader is read-only; never writes to the snapshot.
- Indexes are computed lazily on first access and cached.

**Non-goals.**
- Querying the live DB. The replay path is snapshot-only by invariant
  (see `architecture.md`'s reproducibility argument). For live-DB reads
  use a separate helper.
- Cross-snapshot joins. Each `SnapshotLoader` owns ONE cycle's data.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from typing import Any, Optional


def _load_jsonl(path: Path) -> list[dict]:
    """Read a JSONL file; return [] if missing or empty. Each line must
    be a JSON object; blank lines are skipped."""
    if not path.exists():
        return []
    out: list[dict] = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


@dataclass
class SnapshotLoader:
    snapshot_dir: Path

    def __init__(self, snapshot_dir):
        self.snapshot_dir = Path(snapshot_dir)
        if not self.snapshot_dir.exists():
            raise FileNotFoundError(
                f"snapshot dir does not exist: {self.snapshot_dir}"
            )
        if not (self.snapshot_dir / "manifest.json").exists():
            raise FileNotFoundError(
                f"manifest.json missing in {self.snapshot_dir}"
            )

    # ---- raw row collections (lazy) --------------------------------------

    @cached_property
    def manifest(self) -> dict:
        return json.loads((self.snapshot_dir / "manifest.json").read_text())

    @cached_property
    def trades(self) -> list[dict]:
        return _load_jsonl(self.snapshot_dir / "trades.jsonl")

    @cached_property
    def decisions(self) -> list[dict]:
        return _load_jsonl(self.snapshot_dir / "decisions.jsonl")

    @cached_property
    def predictions(self) -> list[dict]:
        return _load_jsonl(self.snapshot_dir / "predictions.jsonl")

    @cached_property
    def trade_closures(self) -> list[dict]:
        return _load_jsonl(self.snapshot_dir / "trade_closures.jsonl")

    @cached_property
    def decision_inputs(self) -> list[dict]:
        return _load_jsonl(self.snapshot_dir / "decision_inputs.jsonl")

    @cached_property
    def trade_features(self) -> list[dict]:
        return _load_jsonl(self.snapshot_dir / "trade_features.jsonl")

    @cached_property
    def signals(self) -> list[dict]:
        """Signals that were referenced by a decision in this cycle.
        PR-B addition — older snapshots may be empty."""
        return _load_jsonl(self.snapshot_dir / "signals.jsonl")

    @cached_property
    def price_impacts(self) -> list[dict]:
        """Per-trade 9-bucket mid trajectory. PR-B addition."""
        return _load_jsonl(self.snapshot_dir / "price_impacts.jsonl")

    @cached_property
    def llm_probe_calls(self) -> list[dict]:
        """Shadow A/B challenger LLM calls. PR-B addition; default-off in
        production, so most snapshots will be empty."""
        return _load_jsonl(self.snapshot_dir / "llm_probe_calls.jsonl")

    @cached_property
    def llm_calls(self) -> list[dict]:
        return _load_jsonl(self.snapshot_dir / "llm_calls.jsonl")

    @cached_property
    def articles(self) -> list[dict]:
        """Articles referenced by any signal in this cycle. PR-B/F addition."""
        return _load_jsonl(self.snapshot_dir / "articles.jsonl")

    @cached_property
    def articles_by_id(self) -> dict[int, dict]:
        return {int(a["id"]): a for a in self.articles}

    @cached_property
    def llm_parse_failures(self) -> list[dict]:
        return _load_jsonl(self.snapshot_dir / "llm_parse_failures.jsonl")

    # ---- indexed views (lazy) --------------------------------------------

    @cached_property
    def trades_by_id(self) -> dict[int, dict]:
        return {int(t["id"]): t for t in self.trades}

    @cached_property
    def signals_by_id(self) -> dict[int, dict]:
        return {int(s["id"]): s for s in self.signals}

    @cached_property
    def predictions_by_id(self) -> dict[int, dict]:
        return {int(p["id"]): p for p in self.predictions}

    @cached_property
    def predictions_by_prompt_sha(self) -> dict[str, dict]:
        """Caching key the live `ai.py` cache uses. Predictions table has
        a `prompt_sha` column; falling back to `prompt_hash` if the older
        column name is in the data."""
        out: dict[str, dict] = {}
        for p in self.predictions:
            sha = p.get("prompt_sha") or p.get("prompt_hash")
            if sha:
                out[sha] = p
        return out

    @cached_property
    def closures_by_trade_id(self) -> dict[int, dict]:
        """1:1 by UNIQUE(trade_id) on trade_closures."""
        return {int(c["trade_id"]): c for c in self.trade_closures}

    @cached_property
    def impacts_by_trade_id(self) -> dict[int, dict]:
        """1:1 by UNIQUE(trade_id) on price_impacts."""
        return {int(pi["trade_id"]): pi for pi in self.price_impacts}

    @cached_property
    def decisions_by_signal_id(self) -> dict[int, dict]:
        """UNIQUE(signal_id WHERE signal_id IS NOT NULL) — MVP-path
        decisions only. DKG-path decisions have `signal_id=NULL` and are
        excluded here."""
        return {
            int(d["signal_id"]): d
            for d in self.decisions
            if d.get("signal_id") is not None
        }

    @cached_property
    def decision_inputs_by_signal_ref(self) -> dict[int, dict]:
        """`decision_inputs.kind='signal' AND ref_id=signal_id` — the
        capture row per signal. UNIQUE on (kind, ref_id)."""
        return {
            int(di["ref_id"]): di
            for di in self.decision_inputs
            if di.get("kind") == "signal"
        }

    # ---- joined views ----------------------------------------------------

    def fired_trades(self) -> list[dict]:
        """Non-tracer trades that produced a `trades` row (executed)."""
        return [t for t in self.trades if not t.get("is_tracer")]

    def closed_trades(self) -> list[tuple[dict, dict]]:
        """Pairs of (trade, closure) for every closed trade in the cycle."""
        out: list[tuple[dict, dict]] = []
        for t in self.fired_trades():
            c = self.closures_by_trade_id.get(int(t["id"]))
            if c is not None:
                out.append((t, c))
        return out

    def trade_with_impact(self) -> list[tuple[dict, dict]]:
        """Pairs of (trade, price_impacts) — the trade trajectory pairs."""
        out: list[tuple[dict, dict]] = []
        for t in self.fired_trades():
            pi = self.impacts_by_trade_id.get(int(t["id"]))
            if pi is not None:
                out.append((t, pi))
        return out

    # ---- features_snapshot decode ----------------------------------------

    def features_snapshot(self, trade: dict) -> Optional[dict]:
        """Decode the JSON-serialised `features_snapshot` blob on a trade
        row. Returns None on missing / parse error so callers can skip."""
        raw = trade.get("features_snapshot")
        if not raw:
            return None
        try:
            return json.loads(raw) if isinstance(raw, str) else raw
        except (TypeError, ValueError):
            return None

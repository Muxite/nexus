"""Tier 4 — per-source PnL attribution + funnel survival.

## Specification

**Purpose.** Read-only join over (`articles → signals → decisions →
trades → trade_closures → price_impacts`) to answer:

- **i-G1.** Top-N sources by trade count, by realised net PnL, and by
  median `edge_realised_t3600s` (using `price_impacts` because realised
  PnL alone is too small a sample at ~15 closed trades).
- **i-E2 partial.** Per-source funnel survival — what fraction of each
  source's signals reach each gate stage; what fraction reach execute.

Closed-loop answer to "are NYT scrapes worth the engineering vs cheap
RSS?".

**Public interface.**
- CLI: `python -m scripts.replay.source_pnl --snapshot <dir> [--out report.json]`
- `run_inproc(snapshot_dir) -> SourcePnlReport` — programmatic entry.

**Invariants.**
- Snapshot directory only.
- `source` comes from `articles.source` (PR-B added articles to the
  dump). When `articles.jsonl` is missing or empty, every row falls
  into the "UNKNOWN" source bucket.

**Non-goals.**
- Cross-snapshot aggregation (run once per snapshot; the operator
  combines reports in their own script).
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.replay._snapshot_loader import SnapshotLoader  # noqa: E402


def _median(values: list[float]) -> Optional[float]:
    vs = [v for v in values if v is not None]
    if not vs:
        return None
    return statistics.median(vs)


@dataclass
class SourceStats:
    source: str
    n_signals: int = 0
    n_signals_reached_llm: int = 0
    n_decisions_execute: int = 0
    n_decisions_reject: int = 0
    by_reject_reason: dict[str, int] = field(default_factory=dict)
    n_trades_open: int = 0
    n_trades_closed: int = 0
    net_pnl_total: float = 0.0
    notional_total: float = 0.0
    edge_realised_t3600s_values: list[float] = field(default_factory=list)

    @property
    def survival_rate_to_execute(self) -> float:
        if self.n_signals == 0:
            return 0.0
        return self.n_decisions_execute / self.n_signals

    @property
    def llm_reach_rate(self) -> float:
        if self.n_signals == 0:
            return 0.0
        return self.n_signals_reached_llm / self.n_signals

    @property
    def median_edge_realised_t3600s(self) -> Optional[float]:
        return _median(self.edge_realised_t3600s_values)


@dataclass
class SourcePnlReport:
    snapshot: str
    n_signals_total: int
    n_sources: int
    by_source: list[SourceStats]

    def to_dict(self) -> dict:
        def stats_to_dict(s: SourceStats) -> dict:
            return {
                "source": s.source,
                "n_signals": s.n_signals,
                "n_signals_reached_llm": s.n_signals_reached_llm,
                "n_decisions_execute": s.n_decisions_execute,
                "n_decisions_reject": s.n_decisions_reject,
                "survival_rate_to_execute": round(s.survival_rate_to_execute, 4),
                "llm_reach_rate": round(s.llm_reach_rate, 4),
                "by_reject_reason": dict(s.by_reject_reason),
                "n_trades_open": s.n_trades_open,
                "n_trades_closed": s.n_trades_closed,
                "net_pnl_total": round(s.net_pnl_total, 4),
                "notional_total": round(s.notional_total, 4),
                "median_edge_realised_t3600s": (
                    round(s.median_edge_realised_t3600s, 4)
                    if s.median_edge_realised_t3600s is not None else None
                ),
            }
        return {
            "snapshot": self.snapshot,
            "n_signals_total": self.n_signals_total,
            "n_sources": self.n_sources,
            "by_source": [stats_to_dict(s) for s in self.by_source],
        }


def _gate_history_reached(history_json: str, gate_name: str) -> bool:
    try:
        hist = json.loads(history_json or "[]")
    except (TypeError, ValueError):
        return False
    return any(g.get("gate") == gate_name for g in hist)


def run_inproc(snapshot_dir: str) -> SourcePnlReport:
    loader = SnapshotLoader(snapshot_dir)

    # signal_id → source. signals don't carry source; join via article_id.
    sig_id_to_source: dict[int, str] = {}
    for s in loader.signals:
        article = loader.articles_by_id.get(int(s["article_id"]))
        source = (article.get("source") if article else None) or "UNKNOWN"
        sig_id_to_source[int(s["id"])] = source

    # Build per-source stats by iterating decisions.
    by_source: dict[str, SourceStats] = {}

    # Trade index by signal_id for trade-level aggregations.
    trades_by_signal = {
        int(t["signal_id"]): t for t in loader.trades
        if t.get("signal_id") is not None
    }

    for decision in loader.decisions:
        sig_id_raw = decision.get("signal_id")
        if sig_id_raw is None:
            continue  # DKG-path: skip (no source on those today).
        sig_id = int(sig_id_raw)
        source = sig_id_to_source.get(sig_id, "UNKNOWN")
        stats = by_source.setdefault(source, SourceStats(source=source))
        stats.n_signals += 1

        if _gate_history_reached(decision.get("gate_history", ""), "direction"):
            stats.n_signals_reached_llm += 1

        outcome = decision.get("outcome")
        reason = decision.get("reason") or "?"
        if outcome == "execute":
            stats.n_decisions_execute += 1
            trade = trades_by_signal.get(sig_id)
            if trade is not None:
                stats.notional_total += float(trade.get("size_usdc") or 0.0)
                closure = loader.closures_by_trade_id.get(int(trade["id"]))
                if closure is not None:
                    stats.n_trades_closed += 1
                    stats.net_pnl_total += float(closure.get("realized_pnl_usdc") or 0.0)
                else:
                    stats.n_trades_open += 1
                # Pull post-trade edge_realised_t3600s if we have it.
                impact = loader.impacts_by_trade_id.get(int(trade["id"]))
                if impact is not None:
                    er = impact.get("edge_realised_t3600s")
                    if er is not None:
                        stats.edge_realised_t3600s_values.append(float(er))
        else:
            stats.n_decisions_reject += 1
            stats.by_reject_reason[reason] = stats.by_reject_reason.get(reason, 0) + 1

    # Sort sources by signal count, descending.
    sources_sorted = sorted(by_source.values(), key=lambda s: -s.n_signals)

    return SourcePnlReport(
        snapshot=str(snapshot_dir),
        n_signals_total=sum(s.n_signals for s in by_source.values()),
        n_sources=len(by_source),
        by_source=sources_sorted,
    )


def _print_report(rep: SourcePnlReport, top_n: int = 20) -> None:
    print(f"source_pnl: {rep.snapshot}")
    print(f"  signals: {rep.n_signals_total}  sources: {rep.n_sources}")
    print()
    cols = (
        f"  {'source':<28} {'signals':>8} {'llm%':>6} {'exec%':>6} "
        f"{'open':>5} {'closed':>6} {'pnl':>10} {'notional':>10} "
        f"{'med_er_1h':>10}"
    )
    print(cols)
    print("  " + "-" * (len(cols) - 2))
    for s in rep.by_source[:top_n]:
        med_er = s.median_edge_realised_t3600s
        med_er_str = f"{med_er:.4f}" if med_er is not None else "  -    "
        print(
            f"  {s.source[:28]:<28} "
            f"{s.n_signals:>8d} "
            f"{s.llm_reach_rate * 100:>5.1f}% "
            f"{s.survival_rate_to_execute * 100:>5.1f}% "
            f"{s.n_trades_open:>5d} "
            f"{s.n_trades_closed:>6d} "
            f"{s.net_pnl_total:>+10.4f} "
            f"{s.notional_total:>10.2f} "
            f"{med_er_str:>10}"
        )
    if len(rep.by_source) > top_n:
        print(f"  ... and {len(rep.by_source) - top_n} more sources")


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--snapshot", required=True, help="snapshot directory")
    p.add_argument("--top", type=int, default=20, help="rows to print (default 20)")
    p.add_argument("--out", help="optional JSON output path")
    args = p.parse_args(argv)

    try:
        rep = run_inproc(args.snapshot)
    except (FileNotFoundError, ValueError) as exc:
        print(f"source_pnl: ERROR {exc}", file=sys.stderr)
        return 2

    _print_report(rep, top_n=args.top)
    if args.out:
        Path(args.out).write_text(json.dumps(rep.to_dict(), indent=2))
        print(f"\nreport JSON: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

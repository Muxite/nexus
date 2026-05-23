"""
## Specification

**Purpose.** Run a short, repeatable soak of the irene pipeline and emit
the metrics the P1 ship-gate report depends on. Wraps the tracer harness
(which embeds the pipeline) with a synthetic short-timeout config so the
closer actually fires during the run, and then invokes the replay
accuracy gate against the resulting DB.

Designed for the report-generation step at the close of P1. A live
production soak would run for hours against real data; this is the
**reproducible** short version that proves the toolchain works.

**Public interface.**
- CLI: `python -m scripts.soak.run --count 200 --concurrency 20 --db /tmp/irene_soak.db`
- `run_inproc(...) -> SoakResult` — programmatic entry for tests.

**Invariants.**
- Uses an on-disk DB (not `:memory:`) so the replay gate can re-open it.
- Closer timeout is forced to 0.0 hours so every tracer trade closes
  within the run.
- Idempotent on re-run: removes the DB file at start.

**Side effects.** Writes DB file at `--db`. Prints a summary table to
stdout. No network I/O (uses fake CLOB).

**Non-goals.** A full multi-hour soak against live Polymarket. Real
counterfactual replay against a different model.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path
import os
from typing import Optional

from scripts.replay import accuracy
from scripts.tracer import fire as tracer_fire
from scripts.tracer.fire import _run_async as _tracer_run_async  # async-callable form


@dataclass
class SoakResult:
    elapsed_seconds: float
    articles: int
    article_clusters: int
    signals: int
    decisions: int
    trades: int
    trade_closures: int
    decision_inputs: int
    trade_path_drops: int
    handler_exceptions: int
    replay_total: int
    replay_within_tolerance: int
    replay_max_drift_usdc: float
    replay_passed: bool

    @property
    def passed(self) -> bool:
        return (
            self.trade_path_drops == 0
            and self.handler_exceptions == 0
            and self.replay_passed
        )


def _count_rows(db_path: str) -> dict[str, int]:
    conn = sqlite3.connect(db_path)
    try:
        out = {}
        for table in (
            "articles", "article_clusters", "signals", "decisions",
            "trades", "trade_closures", "decision_inputs",
        ):
            row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            out[table] = int(row[0])
        return out
    finally:
        conn.close()


async def run_inproc(
    *,
    count: int = 200,
    concurrency: int = 20,
    db_path: str,
    timeout_seconds: float = 30.0,
) -> SoakResult:
    if db_path == ":memory:":
        raise ValueError(":memory: cannot be replayed — soak must use an on-disk DB.")
    if Path(db_path).exists():
        os.unlink(db_path)

    # Force closer.timeout_hours=0 so every trade closes within the run.
    # Pull the override through the IRENE__SECTION__KEY env path that
    # `load_config()` honours.
    # `tracer_fire.run_inproc` wraps `_run_async` in `asyncio.run`, which
    # collides with our own running loop. Call the async form directly.
    # Force every trade to time out instantly + closer poll cadence to ~1s
    # so we exercise the close path within the soak window.
    tracer_result = await _tracer_run_async(
        count=count,
        concurrency=concurrency,
        db_path=db_path,
        template_name=None,
        timeout_seconds=timeout_seconds,
        start_closer=True,
        # Pick an exit mid that's NOT the trade.py default (0.5) so
        # realized_pnl_usdc is non-trivial — gives the replay gate
        # something to validate.
        closer_mid=0.55,
        closer_timeout_hours=0.0,
        closer_poll_seconds=1,
    )

    counts = _count_rows(db_path)
    replay = accuracy.run_inproc(db_path)
    return SoakResult(
        elapsed_seconds=tracer_result.elapsed_seconds,
        articles=counts["articles"],
        article_clusters=counts["article_clusters"],
        signals=counts["signals"],
        decisions=counts["decisions"],
        trades=counts["trades"],
        trade_closures=counts["trade_closures"],
        decision_inputs=counts["decision_inputs"],
        trade_path_drops=int(tracer_result.drops),
        handler_exceptions=int(tracer_result.handler_exceptions),
        replay_total=replay.total_closed,
        replay_within_tolerance=replay.within_tolerance,
        replay_max_drift_usdc=replay.max_drift,
        replay_passed=replay.passed,
    )


def _print_summary(result: SoakResult) -> None:
    print(f"soak: elapsed={result.elapsed_seconds:.2f}s  passed={result.passed}")
    print(f"  articles            {result.articles}")
    print(f"  article_clusters    {result.article_clusters}")
    print(f"  signals             {result.signals}")
    print(f"  decisions           {result.decisions}")
    print(f"  trades              {result.trades}")
    print(f"  trade_closures      {result.trade_closures}")
    print(f"  decision_inputs     {result.decision_inputs}")
    print(f"  trade_path_drops    {result.trade_path_drops}")
    print(f"  handler_exceptions  {result.handler_exceptions}")
    print(f"  replay              "
          f"{result.replay_within_tolerance}/{result.replay_total} within ±$0.01  "
          f"max_drift=${result.replay_max_drift_usdc:.4f}")


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run a short irene soak + replay gate.")
    parser.add_argument("--count", type=int, default=200)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--db", required=True)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of a text summary")
    args = parser.parse_args(argv)
    result = asyncio.run(
        run_inproc(
            count=args.count,
            concurrency=args.concurrency,
            db_path=args.db,
            timeout_seconds=args.timeout_seconds,
        )
    )
    if args.json:
        print(json.dumps(asdict(result), indent=2))
    else:
        _print_summary(result)
    return 0 if result.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())

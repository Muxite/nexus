"""
## Specification

**Purpose.** Standalone DB checker for the tracer harness. Connects to an
existing irene SQLite file, queries the tracer-row counts across every
owned table, and prints a one-line-per-table report. Exits non-zero if
any expected table is missing tracer rows when the article count is
positive — that's the soak-time tripwire that fires when the funnel
drops bullets between stages.

**Public surface.**
- `main(argv=None) -> int` — argparse CLI.
- `run_inproc(db_path) -> CheckResult` — function-level entry for tests.

**Why standalone?** The harness in `fire.py` embeds the pipeline in its
own process — useful for CI. The checker is for *operator-side* use:
attach to a long-running soak DB without booting the pipeline at all.
Same query set, different lifetime.

**Owned tables.** None — read-only.

**Side effects.** Reads the DB at the supplied path. No writes.

**Failure modes.**
- `:memory:` is rejected — there's nothing to inspect.
- A missing table is treated as `count=0` (the table-ownership lint
  guarantees the schema is present after `db.connect()`; the checker
  tolerates an unmigrated soak DB without crashing).

**Non-goals.** Booting the pipeline; per-row replay verification (that's
`scripts/replay/accuracy.py`).
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass

from scripts.tracer._queries import (
    TRACER_TABLES,
    TracerCounts,
    open_readonly,
    query_tracer_counts,
)


@dataclass
class CheckResult:
    counts: TracerCounts
    missing_tables: list[str]

    @property
    def ok(self) -> bool:
        """Tracer rows are visible end-to-end iff every gating table has
        at least one row whenever `articles` is positive.

        `trades` is excluded from the "must be > 0" check because a
        purely-rejecting cascade can produce 0 trade rows legitimately —
        the funnel still propagated, the gates just rejected. The
        production tripwire on `trades=0` lives in the soak report, not
        the in-pipe checker.
        """
        if self.counts.articles == 0:
            return True  # nothing tracer'd — nothing to assert
        return not self.missing_tables


def _diagnose(counts: TracerCounts) -> list[str]:
    """Return the list of tables that should hold tracer rows but don't.

    `trades` is intentionally NOT considered missing — see CheckResult.ok.
    """
    if counts.articles == 0:
        return []
    missing: list[str] = []
    for name in ("article_clusters", "signals", "decisions", "decision_inputs"):
        if getattr(counts, name) == 0:
            missing.append(name)
    return missing


def _format_report(counts: TracerCounts, missing: list[str]) -> str:
    lines: list[str] = []
    width = max(len(t) for t in TRACER_TABLES)
    for name in TRACER_TABLES:
        lines.append(f"  {name.ljust(width)}  count={getattr(counts, name)}")
    if missing and counts.articles > 0:
        lines.append("")
        lines.append(f"  MISSING: {', '.join(missing)}")
    return "\n".join(lines)


def run_inproc(db_path: str) -> CheckResult:
    """Function-level entry — used by tests to skip the CLI."""
    conn = open_readonly(db_path)
    try:
        counts = query_tracer_counts(conn)
    finally:
        conn.close()
    return CheckResult(counts=counts, missing_tables=_diagnose(counts))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.tracer.check",
        description="Report tracer row counts in an existing irene DB.",
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to the irene SQLite file (`:memory:` is rejected).",
    )
    parser.add_argument(
        "--tracer-id-pattern",
        default=None,
        help="Reserved — present for CLI stability. Ignored in P1.",
    )
    args = parser.parse_args(argv)

    try:
        result = run_inproc(args.db)
    except ValueError as exc:
        print(f"check FAILED: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 — surface
        print(
            f"check FAILED: {exc.__class__.__name__}: {exc}",
            file=sys.stderr,
        )
        return 1

    print(_format_report(result.counts, result.missing_tables))
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())

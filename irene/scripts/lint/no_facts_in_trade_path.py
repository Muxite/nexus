"""
## Specification

**Purpose.** Mechanically enforce the P2 hard invariant from
`docs/p2_contracts.md` §1: the **trade path** never reads any of the
extraction tables. Specifically, no file under
`app/stages/trade.py`, `app/stages/match.py`, `app/stages/closer.py`, or
`app/ai.py` may contain a SQL string mentioning `facts`, `entities`,
`entity_aliases`, or `extraction_runs`.

**Public interface.**
- CLI: `python -m scripts.lint.no_facts_in_trade_path`
- `scan(roots) -> list[Violation]` — programmatic entry for tests.

**Invariants.**
- Exit 0 on a clean scan, exit 1 on any violation. Prints offending
  file + line + matched substring.
- Defensive on its own scope: this lint scans **source files only**
  (not docs / tests / comments-only lines). String literals that
  contain table names are caught; SQL inside docstrings explaining the
  invariant is intentionally skipped via a one-line allow comment
  (`# lint: allow-extraction-table-ref`).

**Side effects.** Reads files under `irene/app/` and emits to stdout.

**Failure modes.** I/O errors on the target files propagate (the lint
should fail loudly if it can't read what it's supposed to scan).

**Non-goals.** General SQL parsing; AST analysis; lint for non-trade
modules. The check is a tight grep — explicit on what it scans, easy
to reason about, hard to silently bypass.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

# Files where reading any extraction table is forbidden.
TRADE_PATH_FILES = (
    "app/ai.py",
    "app/stages/trade.py",
    "app/stages/match.py",
    "app/stages/closer.py",
)

# The four extraction tables (P2) + the resolver's overlay (P3). Trade
# path may not read any of them.
FORBIDDEN_TABLES = (
    "facts", "entities", "entity_aliases", "extraction_runs",
    "entity_resolutions",
)

# A line containing this token is intentionally allowed (e.g. a comment
# referring to the table by name without reading from it).
ALLOW_MARKER = "lint: allow-extraction-table-ref"

# Match "FROM facts", "JOIN facts", "INTO facts", "UPDATE facts", etc.
# Case-insensitive. Bounded with word boundaries so "factsheet" doesn't
# match.
_TABLE_TOKENS = "|".join(re.escape(t) for t in FORBIDDEN_TABLES)
_PATTERN = re.compile(
    rf"\b(FROM|JOIN|INTO|UPDATE|DELETE\s+FROM)\s+({_TABLE_TOKENS})\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Violation:
    file: str
    line_no: int
    line: str
    matched: str


def scan(roots: Iterable[Path]) -> list[Violation]:
    out: list[Violation] = []
    for root in roots:
        if not root.exists():
            continue
        text = root.read_text(encoding="utf-8")
        for i, line in enumerate(text.splitlines(), start=1):
            if ALLOW_MARKER in line:
                continue
            m = _PATTERN.search(line)
            if m:
                out.append(
                    Violation(
                        file=str(root),
                        line_no=i,
                        line=line.rstrip(),
                        matched=m.group(0),
                    )
                )
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--root",
        default=str(Path(__file__).resolve().parents[2]),
        help="Path to the `irene/` source tree (default: auto-detected).",
    )
    args = parser.parse_args(argv)
    base = Path(args.root)
    targets = [base / rel for rel in TRADE_PATH_FILES]
    missing = [t for t in targets if not t.exists()]
    if missing:
        print(
            "lint: ERROR — trade-path files not found:\n  "
            + "\n  ".join(str(m) for m in missing),
            file=sys.stderr,
        )
        return 2
    violations = scan(targets)
    if not violations:
        print(
            f"no_facts_in_trade_path: clean ({len(targets)} files scanned, "
            f"forbidden tables {FORBIDDEN_TABLES})"
        )
        return 0
    print(
        f"no_facts_in_trade_path: {len(violations)} VIOLATION(S) — the trade "
        "path is not allowed to read extraction tables (docs/p2_contracts.md §1)."
    )
    for v in violations:
        print(f"  {v.file}:{v.line_no}: {v.matched!r}")
        print(f"    >>> {v.line.strip()}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

"""
Code-path audit — static analysis only, no DB.

Greps the seulgi/app codebase for suspect "silent exception" patterns
that hide write-path failures, and lists every `self._conn.execute*`
call site so we can quickly verify each is wrapped in the Phase-0
observability counters.

Patterns flagged
────────────────
- `except .*: pass`                           — bare swallows
- `except .*:[ \t]*$\n[ \t]+pass`             — multi-line variant
- `return_exceptions=True`                    — asyncio.gather silently
  swallows individual coro failures
- `self._conn.execute*` call sites            — every DB write/read

Output: Markdown.

Usage
─────
    python3 seulgi/scripts/analysis/reports/code_path_audit.py
    python3 seulgi/scripts/analysis/reports/code_path_audit.py --out seulgi/reports/code_path_audit.md
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from datetime import datetime, timezone


DEFAULT_ROOT = Path("seulgi/app")
EXCLUDE_DIRS = {"__pycache__", "tests", ".pytest_cache"}


# Patterns of interest. (label, regex, multiline) tuples.
PATTERNS = [
    ("bare-swallow (except: pass)",
     re.compile(r"except\s+[^:]*:[ \t]*$\s+pass\b", re.MULTILINE)),
    ("inline-swallow (except ...: pass)",
     re.compile(r"except\s+[^:]+:\s*pass\b")),
    ("gather(return_exceptions=True)",
     re.compile(r"return_exceptions\s*=\s*True")),
    ("noqa BLE001 (broad exception)",
     re.compile(r"#\s*noqa:\s*BLE001")),
    ("self._conn.execute*",
     re.compile(r"self\._conn\.execute\w*\(")),
    ("self._conn.commit",
     re.compile(r"self\._conn\.commit\b")),
    ("self._conn.rollback",
     re.compile(r"self\._conn\.rollback\b")),
]


def iter_python_files(root: Path):
    for path in root.rglob("*.py"):
        if any(part in EXCLUDE_DIRS for part in path.parts):
            continue
        yield path


def scan_file(path: Path) -> dict[str, list[tuple[int, str]]]:
    """Return {label: [(line_no, line_text)]} of matches."""
    out: dict[str, list[tuple[int, str]]] = {}
    try:
        text = path.read_text()
    except Exception:  # noqa: BLE001
        return out
    for label, rx in PATTERNS:
        hits: list[tuple[int, str]] = []
        for m in rx.finditer(text):
            line_no = text.count("\n", 0, m.start()) + 1
            # Pull the matching line
            line_text = text.splitlines()[line_no - 1].strip()
            hits.append((line_no, line_text))
        if hits:
            out[label] = hits
    return out


def render(root: Path, all_hits: dict[Path, dict[str, list[tuple[int, str]]]]) -> str:
    lines: list[str] = [
        f"# Code-path audit — {datetime.now(timezone.utc).isoformat()}",
        f"_root: `{root}`_",
        "",
    ]

    # Aggregate counts per label.
    totals: dict[str, int] = {}
    for hits in all_hits.values():
        for label, items in hits.items():
            totals[label] = totals.get(label, 0) + len(items)

    lines.append("## Summary")
    lines.append("")
    lines.append("| label | total occurrences |")
    lines.append("|---|---:|")
    for label, _ in PATTERNS:
        lines.append(f"| {label} | {totals.get(label, 0)} |")
    lines.append("")

    # Per-file detail, only files with any hit.
    files_with_hits = sorted(p for p, h in all_hits.items() if h)
    lines.append(f"## Per-file detail  ({len(files_with_hits)} files with hits)")
    lines.append("")
    for path in files_with_hits:
        hits = all_hits[path]
        lines.append(f"### {path}")
        for label, _ in PATTERNS:
            items = hits.get(label, [])
            if not items:
                continue
            lines.append(f"- **{label}** — {len(items)} occurrence(s)")
            for line_no, text in items[:8]:
                lines.append(f"    - `{path}:{line_no}` — `{text[:120]}`")
            if len(items) > 8:
                lines.append(f"    - … +{len(items) - 8} more")
        lines.append("")

    lines.append("## Notes")
    lines.append("")
    lines.append(
        "- Each `except ...: pass` block IS a candidate silent-failure "
        "site. Phase 0 wraps the critical write paths so failures land in "
        "logs + counters; if a new bug surfaces, audit these first."
    )
    lines.append(
        "- `return_exceptions=True` is correct for engine fan-out, but the "
        "callers MUST iterate the results and log/metric each exception "
        "(see `engine.py::process` lines 570-590)."
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    p = argparse.ArgumentParser(description="Code-path audit")
    p.add_argument("--root", default=str(DEFAULT_ROOT))
    p.add_argument("--out", default=None)
    args = p.parse_args()

    root = Path(args.root)
    if not root.exists():
        print(f"ERROR: root not found at {root}", file=sys.stderr)
        return 2

    all_hits: dict[Path, dict[str, list[tuple[int, str]]]] = {}
    for path in iter_python_files(root):
        hits = scan_file(path)
        if hits:
            all_hits[path] = hits

    text = render(root, all_hits)
    if args.out:
        with open(args.out, "w") as f:
            f.write(text)
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

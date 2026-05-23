"""Table renderers for `scripts.analysis` subcommands.

Each subcommand builds a `Section`; the dispatcher asks `render()` for
text / md / json. Pattern lifted from seulgi/scripts/analysis/_format.py.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from typing import Iterable, Optional


@dataclass
class Section:
    """One coherent table the analyst can pipe somewhere."""

    title: str
    headers: list[str]
    rows: list[list]
    aligns: Optional[list[str]] = None
    notes: list[str] = field(default_factory=list)


def render(section: Section, fmt: str = "text") -> str:
    if fmt == "json":
        return _render_json(section)
    if fmt == "md":
        return _render_md(section)
    return _render_text(section)


def _stringify(cell) -> str:
    if cell is None:
        return "-"
    if isinstance(cell, float):
        return f"{cell:.3f}"
    return str(cell)


def _is_numeric_col(rows: list[list], col: int) -> bool:
    """Column is numeric if every non-None cell is float/int."""
    for r in rows:
        v = r[col]
        if v is None:
            continue
        if not isinstance(v, (int, float)):
            return False
    return True


def _column_aligns(section: Section) -> list[str]:
    if section.aligns:
        return section.aligns
    n = len(section.headers)
    return ["r" if _is_numeric_col(section.rows, i) else "l" for i in range(n)]


def _column_widths(section: Section) -> list[int]:
    widths = [len(h) for h in section.headers]
    for r in section.rows:
        for i, c in enumerate(r):
            widths[i] = max(widths[i], len(_stringify(c)))
    return widths


def _fmt_cell(value: str, width: int, align: str) -> str:
    if align == "r":
        return value.rjust(width)
    if align == "c":
        return value.center(width)
    return value.ljust(width)


def _render_text(section: Section) -> str:
    widths = _column_widths(section)
    aligns = _column_aligns(section)
    lines = []
    lines.append(f"# {section.title}")
    lines.append("")
    # Header
    header = "  ".join(
        _fmt_cell(h, widths[i], aligns[i]) for i, h in enumerate(section.headers)
    )
    lines.append(header)
    lines.append("  ".join("-" * w for w in widths))
    for r in section.rows:
        line = "  ".join(
            _fmt_cell(_stringify(c), widths[i], aligns[i]) for i, c in enumerate(r)
        )
        lines.append(line)
    for note in section.notes:
        lines.append("")
        lines.append(f"# {note}")
    return "\n".join(lines) + "\n"


def _render_md(section: Section) -> str:
    aligns = _column_aligns(section)
    lines = [f"## {section.title}", ""]
    lines.append("| " + " | ".join(section.headers) + " |")
    sep = []
    for a in aligns:
        sep.append(":--" if a == "l" else ("--:" if a == "r" else ":-:"))
    lines.append("| " + " | ".join(sep) + " |")
    for r in section.rows:
        lines.append("| " + " | ".join(_stringify(c) for c in r) + " |")
    for note in section.notes:
        lines.append("")
        lines.append(f"_{note}_")
    return "\n".join(lines) + "\n"


def _render_json(section: Section) -> str:
    out = {
        "title": section.title,
        "rows": [
            {h: r[i] for i, h in enumerate(section.headers)}
            for r in section.rows
        ],
    }
    if section.notes:
        out["notes"] = section.notes
    return json.dumps(out, indent=2, default=str)

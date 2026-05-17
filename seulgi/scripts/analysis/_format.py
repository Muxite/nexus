"""Table renderers for analysis subcommands.

Every subcommand builds a `Section` and asks `render(section, fmt)` for
text / md / json output. Same data structure, multiple sinks — analyst
chooses with --format.

Design notes:
- Cells are pre-stringified. The subcommand decides formatting (e.g.
  "+24.30" vs "+24.30 USDC"); the renderer is purely about layout.
- Text alignment is inferred from the cell value: numbers right-justify,
  strings left-justify. Override per-column with the optional `aligns`
  param (list of "l"/"r"/"c" with one entry per column).
- JSON output uses the headers as keys so consumers don't have to know
  the column order.
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
    rows: list[list]                              # list of lists; cells may be any type
    aligns: Optional[list[str]] = None            # "l" / "r" / "c" per column
    notes: list[str] = field(default_factory=list)  # short lines printed under the table
    summary: str = ""                             # one-line lead-in printed above the table


def render(section: Section, fmt: str = "text", stream=sys.stdout) -> None:
    if fmt == "text":
        _render_text(section, stream)
    elif fmt == "md":
        _render_md(section, stream)
    elif fmt == "json":
        _render_json(section, stream)
    else:
        raise ValueError(f"unknown format: {fmt!r}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _stringify(cell) -> str:
    if cell is None:
        return "—"
    if isinstance(cell, float):
        return f"{cell:.4f}".rstrip("0").rstrip(".") if cell != int(cell) else str(int(cell))
    return str(cell)


def _infer_aligns(rows: list[list], n_cols: int) -> list[str]:
    """Right-align columns that are mostly numeric."""
    aligns = ["l"] * n_cols
    for c in range(n_cols):
        n_num = 0
        for r in rows:
            if c >= len(r):
                continue
            v = r[c]
            if isinstance(v, (int, float)):
                n_num += 1
                continue
            if isinstance(v, str):
                stripped = v.lstrip("+-$").rstrip("%").replace(",", "")
                try:
                    float(stripped)
                    n_num += 1
                except ValueError:
                    pass
        if rows and n_num >= len(rows) * 0.7:
            aligns[c] = "r"
    return aligns


# ---------------------------------------------------------------------------
# Text renderer
# ---------------------------------------------------------------------------


def _render_text(s: Section, out) -> None:
    if s.title:
        bar = "─" * max(8, 60 - len(s.title))
        print(f"── {s.title} {bar}", file=out)
    if s.summary:
        print(f"  {s.summary}", file=out)
        print(file=out)

    cells = [[_stringify(c) for c in row] for row in s.rows]
    n_cols = len(s.headers)
    aligns = s.aligns or _infer_aligns(s.rows, n_cols)

    widths = [len(h) for h in s.headers]
    for row in cells:
        for i, cell in enumerate(row[:n_cols]):
            widths[i] = max(widths[i], len(cell))

    def _fmt_cell(cell: str, w: int, align: str) -> str:
        if align == "r":
            return cell.rjust(w)
        if align == "c":
            return cell.center(w)
        return cell.ljust(w)

    header_line = "  " + "  ".join(
        _fmt_cell(h, widths[i], aligns[i]) for i, h in enumerate(s.headers)
    )
    sep_line = "  " + "  ".join("─" * widths[i] for i in range(n_cols))
    print(header_line, file=out)
    print(sep_line, file=out)
    for row in cells:
        print(
            "  " + "  ".join(
                _fmt_cell(row[i] if i < len(row) else "", widths[i], aligns[i])
                for i in range(n_cols)
            ),
            file=out,
        )
    for note in s.notes:
        print(f"  ! {note}", file=out)


# ---------------------------------------------------------------------------
# Markdown renderer
# ---------------------------------------------------------------------------


def _render_md(s: Section, out) -> None:
    if s.title:
        print(f"### {s.title}\n", file=out)
    if s.summary:
        print(f"_{s.summary}_\n", file=out)

    aligns = s.aligns or _infer_aligns(s.rows, len(s.headers))
    md_aligns = {"l": ":--", "r": "--:", "c": ":-:"}
    align_row = "| " + " | ".join(md_aligns.get(a, "---") for a in aligns) + " |"
    header_row = "| " + " | ".join(s.headers) + " |"
    print(header_row, file=out)
    print(align_row, file=out)
    for row in s.rows:
        cells = [_stringify(c) for c in row]
        # pad to n_cols
        while len(cells) < len(s.headers):
            cells.append("")
        print("| " + " | ".join(cells) + " |", file=out)
    if s.notes:
        print(file=out)
        for note in s.notes:
            print(f"> {note}", file=out)


# ---------------------------------------------------------------------------
# JSON renderer
# ---------------------------------------------------------------------------


def _render_json(s: Section, out) -> None:
    payload = {
        "title": s.title,
        "summary": s.summary,
        "notes": s.notes,
        "rows": [
            {h: (c if not isinstance(c, float) or c == c else None)  # NaN guard
             for h, c in zip(s.headers, row)}
            for row in s.rows
        ],
    }
    json.dump(payload, out, default=str, indent=2)
    out.write("\n")


__all__ = ["Section", "render"]

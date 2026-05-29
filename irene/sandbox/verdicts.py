"""Human-graded verdict files: emit a stub, parse a filled file.

Verdict file format (one block per exploration):

    ## [<n>] exploration_id=<id>
    verdict: GOOD   # one of GOOD, MARG, BAD, SKIP, (blank = unrated)
    notes: optional free text
    ---
    <row context, ignored on parse>

Parsing is line-by-line and tolerant: missing `verdict:` defaults to
"" (unrated) so a partially-filled file still aggregates cleanly.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

_BLOCK_HEADER = re.compile(r"^##\s*\[(\d+)\]\s*exploration_id\s*=\s*(\d+)")
_VERDICT_LINE = re.compile(r"^verdict\s*:\s*(\S+)")
_NOTES_LINE = re.compile(r"^notes\s*:\s*(.*)$")

VALID = {"GOOD", "MARG", "BAD", "SKIP"}


@dataclass
class Verdict:
    n: int
    exploration_id: int
    verdict: str          # "" if unrated
    notes: str


def parse(md_text: str) -> list[Verdict]:
    out: list[Verdict] = []
    cur: dict | None = None
    for line in md_text.splitlines():
        m = _BLOCK_HEADER.match(line)
        if m:
            if cur is not None:
                out.append(_finalise(cur))
            cur = {"n": int(m.group(1)), "exploration_id": int(m.group(2)),
                   "verdict": "", "notes": ""}
            continue
        if cur is None:
            continue
        m = _VERDICT_LINE.match(line)
        if m:
            v = m.group(1).strip().upper().rstrip(",")
            cur["verdict"] = v if v in VALID else ""
            continue
        m = _NOTES_LINE.match(line)
        if m:
            cur["notes"] = m.group(1).strip()
    if cur is not None:
        out.append(_finalise(cur))
    return out


def _finalise(d: dict) -> Verdict:
    return Verdict(
        n=d["n"],
        exploration_id=d["exploration_id"],
        verdict=d["verdict"],
        notes=d["notes"],
    )

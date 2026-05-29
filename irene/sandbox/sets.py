"""Set files: YAML on disk, list of exploration_ids + metadata.

Hand-editable. Compose via union/intersection/difference. Sample with a
seeded RNG. No DB access in this module — set membership is pure ID lists.
"""

from __future__ import annotations

import datetime as dt
import os
import random
import re
from dataclasses import dataclass, field
from typing import Iterable

import yaml

from irene.sandbox.paths import SETS_DIR, ensure_data_dirs


@dataclass
class SetFile:
    name: str
    description: str
    exploration_ids: list[int]
    filter_summary: str = ""
    created_at: str = ""
    parents: list[str] = field(default_factory=list)


def set_path(name: str) -> str:
    return os.path.join(SETS_DIR, f"{name}.yaml")


def load(name: str) -> SetFile:
    p = set_path(name)
    with open(p) as f:
        raw = yaml.safe_load(f)
    return SetFile(
        name=raw["name"],
        description=raw.get("description", ""),
        exploration_ids=[int(x) for x in raw["exploration_ids"]],
        filter_summary=raw.get("filter_summary", ""),
        created_at=raw.get("created_at", ""),
        parents=list(raw.get("parents", [])),
    )


def save(s: SetFile) -> str:
    ensure_data_dirs()
    p = set_path(s.name)
    payload = {
        "name": s.name,
        "description": s.description,
        "filter_summary": s.filter_summary,
        "created_at": s.created_at or _now(),
        "parents": s.parents,
        "exploration_ids": s.exploration_ids,
    }
    with open(p, "w") as f:
        yaml.safe_dump(payload, f, sort_keys=False, default_flow_style=False)
    return p


def list_sets() -> list[str]:
    ensure_data_dirs()
    return sorted(
        os.path.splitext(f)[0]
        for f in os.listdir(SETS_DIR)
        if f.endswith(".yaml")
    )


def compose(parents: list[SetFile], op: str) -> list[int]:
    if not parents:
        return []
    pools = [set(p.exploration_ids) for p in parents]
    if op == "union":
        out = set().union(*pools)
    elif op == "intersect":
        out = set(pools[0]).intersection(*pools[1:])
    elif op == "difference":
        out = set(pools[0]).difference(*pools[1:])
    else:
        raise ValueError(f"unknown op {op!r} (union|intersect|difference)")
    return sorted(out)


def sample(ids: Iterable[int], n: int, seed: int) -> list[int]:
    pool = list(ids)
    if n >= len(pool):
        return sorted(pool)
    rng = random.Random(seed)
    return sorted(rng.sample(pool, n))


# Filter-expression validator — only allows references to columns we
# know about, plus a small whitelist of SQL operators and literals. The
# operator runs this on her own DB so injection isn't a security
# concern, but we still reject obvious typos / unknown columns so the
# WHERE-clause failure happens in Python, not as a sqlite error.

_TOKEN = re.compile(r"[A-Za-z_][A-Za-z_0-9]*")
_ALLOWED_KEYWORDS = {
    "AND", "OR", "NOT", "IN", "IS", "NULL", "LIKE", "BETWEEN",
    "TRUE", "FALSE", "EXISTS",
}

# `1=1` is the no-op filter used by --all-explorations.
_TRIVIAL_FILTERS = {"1=1"}


_STRING_LITERAL = re.compile(r"'([^']|'')*'")
_DOTTED = re.compile(r"([A-Za-z_]\w*)\.([A-Za-z_]\w*)")


def validate_filter(expr: str, allowed_columns: dict[str, list[str]]) -> None:
    if expr.strip() in _TRIVIAL_FILTERS:
        return
    flat = set()
    valid_prefixes = set(allowed_columns)
    for prefix, cols in allowed_columns.items():
        for c in cols:
            flat.add(c)
            flat.add(f"{prefix}.{c}")
    stripped = _STRING_LITERAL.sub("''", expr)
    dotted_refs = list(_DOTTED.finditer(stripped))
    for m in dotted_refs:
        ref = m.group(0)
        if ref not in flat:
            raise ValueError(
                f"filter references unknown column {ref!r}. "
                f"Allowed prefixes: {sorted(valid_prefixes)}"
            )
    stripped_no_dotted = _DOTTED.sub("", stripped)
    bad = []
    for tok in _TOKEN.findall(stripped_no_dotted):
        if tok.upper() in _ALLOWED_KEYWORDS:
            continue
        if tok in flat:
            continue
        bad.append(tok)
    if bad:
        raise ValueError(
            f"filter references unknown identifiers: {sorted(set(bad))}. "
            f"Use 'e.col' / 'a.col' / 'm.col' / 'c.col' qualified names. "
            f"Allowed columns example: {sorted(flat)[:10]}..."
        )


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

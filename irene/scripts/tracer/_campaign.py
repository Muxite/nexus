"""Campaign spec + parsing helpers for the centralized tracer fire mode.

A "campaign" is one CLI invocation that fires N templates back-to-back in a
single process, sharing the embedder / AI client / CLOB / budget snapshot /
DB connection across all sub-bursts. The `tracer_id` for every row carries
the template name as a middle segment so `audit_burst.py --group-by template`
can break the funnel out per template after the fact.

Two ways to specify a campaign:

  1. Inline flags on the fire.py CLI: repeated `--template <name>:<count>`.
  2. A YAML file: `--campaign scripts/tracer/campaigns/<name>.yaml`.

Both lower into a `CampaignSpec`. The `_run_async` orchestrator in fire.py
loops over `spec.bursts` and writes `tracer_id = compose_tracer_id(root,
template, i)` for every published article.

Pure module — no IO besides reading the YAML in `load_campaign`. Easy to
test in isolation.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import yaml
from pydantic import BaseModel, Field, field_validator


# `tracer_id` middle-segment grammar. The middle segment is the template
# name; the same character set the templates/*.yaml filename stems use.
# Disallow '-' so we can split on it cleanly.
_TEMPLATE_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")
# `name:count` arg parser. Accepts whitespace either side of the colon.
_TEMPLATE_COUNT_RE = re.compile(r"^\s*([A-Za-z0-9_]+)\s*:\s*(\d+)\s*$")


class BurstSpec(BaseModel):
    """One template's slice of a campaign."""

    template: str = Field(..., description="Template name (templates/<template>.yaml stem)")
    count: int = Field(..., gt=0, description="Number of articles to fire from this template")

    @field_validator("template")
    @classmethod
    def _validate_template_name(cls, v: str) -> str:
        if not _TEMPLATE_NAME_RE.fullmatch(v):
            raise ValueError(
                f"template name {v!r} must match {_TEMPLATE_NAME_RE.pattern} "
                "(letters, digits, underscore — no hyphens so we can split tracer_ids)"
            )
        return v


class CampaignSpec(BaseModel):
    """A full campaign — sequential bursts, shared externals, one summary."""

    name: str = Field(..., description="Human-readable campaign name (for logs)")
    prefix_root: str = Field(
        ...,
        description=(
            "Root tracer_id prefix shared by every burst. Supports the {ts} "
            "placeholder which resolves at fire time to UTC YYYYMMDDTHHmm. "
            "Cleanup + grouped audit both query 'tracer_id LIKE <prefix_root>%'."
        ),
    )
    shared: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Default values for fire.py flags shared across every burst: "
            "concurrency, db, config, budget_cap_usd, log_level, exercise_wallet, "
            "live_externals. CLI flags override these."
        ),
    )
    bursts: list[BurstSpec] = Field(..., min_length=1)

    @field_validator("prefix_root")
    @classmethod
    def _no_trailing_dash(cls, v: str) -> str:
        if v.endswith("-"):
            raise ValueError(
                "prefix_root must not end with '-' — compose_tracer_id adds it"
            )
        return v

    def total_count(self) -> int:
        return sum(b.count for b in self.bursts)


def parse_template_count_arg(arg: str) -> BurstSpec:
    """Parse a `--template name:count` CLI arg into a BurstSpec.

    Backwards compat: a bare `--template name` (no colon) is NOT handled here.
    fire.py's argparse layer keeps that as the legacy single-template path.
    """
    m = _TEMPLATE_COUNT_RE.fullmatch(arg)
    if not m:
        raise ValueError(
            f"--template arg {arg!r} not in 'name:count' form "
            "(e.g. 'iran_strike:50')"
        )
    return BurstSpec(template=m.group(1), count=int(m.group(2)))


def resolve_prefix_root(prefix_root: str, *, now: Optional[datetime] = None) -> str:
    """Substitute `{ts}` placeholder in a prefix_root.

    `{ts}` → UTC YYYYMMDDTHHmm (matches the format used by the legacy
    `cycle24-verify-20260526T1908` prefix the user typed manually).
    """
    if "{ts}" not in prefix_root:
        return prefix_root
    ts_source = now or datetime.now(timezone.utc)
    return prefix_root.replace("{ts}", ts_source.strftime("%Y%m%dT%H%M"))


def compose_tracer_id(prefix_root: str, template: str, i: int) -> str:
    """Compose the per-article tracer_id used by campaign mode.

    `<root>-<template>-<i>`. The middle segment is the grouping key for
    `audit_burst.py --group-by template`. Template names are validated to
    contain no hyphens so the parse is unambiguous.
    """
    if not _TEMPLATE_NAME_RE.fullmatch(template):
        raise ValueError(
            f"template name {template!r} contains characters outside "
            f"{_TEMPLATE_NAME_RE.pattern}"
        )
    return f"{prefix_root}-{template}-{i}"


def parse_tracer_id_template(tracer_id: str, prefix_root: str) -> Optional[str]:
    """Extract the template middle segment from a tracer_id, if present.

    Returns None when the tracer_id is a legacy `<prefix>-<i>` (no template
    segment) — callers should bucket such rows under '(legacy)' instead of
    raising. Used by audit_burst.py's --group-by template aggregation.
    """
    if not tracer_id.startswith(prefix_root + "-"):
        return None
    remainder = tracer_id[len(prefix_root) + 1 :]
    # Two valid shapes:
    #   campaign mode: <template>-<i>   → split, take [0]
    #   legacy mode:   <i>              → all-digit, return None (legacy)
    if "-" not in remainder:
        return None
    head, tail = remainder.rsplit("-", 1)
    if not tail.isdigit():
        return None
    if not _TEMPLATE_NAME_RE.fullmatch(head):
        return None
    return head


def load_campaign(path: Path) -> CampaignSpec:
    """Read + validate a campaign YAML file."""
    raw = yaml.safe_load(Path(path).read_text())
    if not isinstance(raw, dict):
        raise ValueError(f"campaign file {path} did not parse as a mapping")
    return CampaignSpec.model_validate(raw)


def campaign_from_args(
    *,
    template_args: list[str],
    count_fallback: int,
    prefix_root: str,
) -> CampaignSpec:
    """Build a CampaignSpec from CLI flags (no YAML file).

    `template_args` is the list collected from repeated `--template` flags.
    Each entry is either:
      - `name:count` → BurstSpec(name, count)
      - `name`       → BurstSpec(name, count_fallback)  ← legacy compat

    If the list is empty the caller never reached here (no template means
    legacy round-robin-all path; that one doesn't go through CampaignSpec).
    """
    if not template_args:
        raise ValueError("template_args must be non-empty")
    bursts: list[BurstSpec] = []
    for raw in template_args:
        if ":" in raw:
            bursts.append(parse_template_count_arg(raw))
        else:
            # Bare `--template name` in multi-template context: full count for
            # each (matches the principle of least surprise — user can override
            # any with `name:N` syntax).
            if not _TEMPLATE_NAME_RE.fullmatch(raw):
                raise ValueError(
                    f"--template arg {raw!r} not a valid template name "
                    f"({_TEMPLATE_NAME_RE.pattern})"
                )
            bursts.append(BurstSpec(template=raw, count=count_fallback))
    return CampaignSpec(
        name=f"cli-{len(bursts)}-burst",
        prefix_root=prefix_root,
        bursts=bursts,
    )

"""Counterfactual sampling — per-filter marginal EV measurement.

Phase 4E.5 (was old plan C.2): for each named filter in the trader, let a
small fraction (default 1%) of would-have-been-skipped signals PASS
THROUGH anyway. Track via a `cf_bypassed_filter` column on the resulting
trade row. After enough closed trades land, compute per-filter marginal
EV: `mean(pnl_cf) - mean(pnl_normal)` per filter. Positive marginal EV
means the filter is HURTING (it was skipping profitable trades); very
negative means the filter is HELPING (the cf trades are net losers).

Determinism: bypass decision is a hash of
`(filter_name, headline_id, condition_id, engine)`. The same trade
replays identically — the bypass either fires every time for that
quadruple, or never. This is essential for the Phase-5 historical
replay harness: re-running a recorded run must reproduce the cf trades
byte-for-byte.

Default OFF. The operator opts in via `counterfactual.enabled: true`
in config.yaml.
"""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from typing import Optional, Set

logger = logging.getLogger(__name__)


# The TOP-5 EV-relevant filters we wire first. Other filters keep
# their normal hard-skip behaviour. Operator can override via
# `enabled_filters` in config.yaml.
TOP_5_FILTERS: Set[str] = {
    "freshness_filter",
    "mid_band",
    "min_confidence",
    "min_edge",
    "duplicate_position",
}


@dataclass
class CounterfactualConfig:
    """Operator-facing config block. Mapped 1:1 to `counterfactual:` in YAML."""

    enabled: bool = False
    sample_rate_per_filter: float = 0.01
    max_cf_per_headline: int = 3
    # `None` means "use TOP_5_FILTERS"; an explicit set narrows or widens.
    enabled_filters: Optional[Set[str]] = None


class CounterfactualSampler:
    """Deterministic per-filter bypass sampler.

    Stateless w.r.t. bypass decisions — the bypass is purely a hash of the
    inputs — but tracks per-headline cf-trade counts so a single noisy
    headline doesn't spawn 20 cf trades.

    Reset the per-headline counter between headlines via
    `reset_headline_counter(headline_id)`. The counter is keyed on
    headline_id so concurrent headlines don't share state.
    """

    def __init__(self, cfg: CounterfactualConfig) -> None:
        self._cfg = cfg
        # Per-headline bypass counter. {headline_id: n_bypasses_so_far}.
        # Pruned on `reset_headline_counter`; total memory is bounded by
        # max-in-flight headlines (small — process()s for one headline
        # are sequential per engine).
        self._counters: dict[str, int] = {}

    @property
    def cfg(self) -> CounterfactualConfig:
        return self._cfg

    @property
    def enabled_filters(self) -> Set[str]:
        """Resolved filter set — TOP_5 if cfg.enabled_filters is None."""
        if self._cfg.enabled_filters is None:
            return TOP_5_FILTERS
        return self._cfg.enabled_filters

    def should_bypass(
        self,
        filter_name: str,
        headline_id: Optional[str],
        condition_id: Optional[str],
        engine: Optional[str],
    ) -> bool:
        """Deterministic bypass decision.

        Returns True iff ALL of:
          - cfg.enabled is True
          - `filter_name` is in the enabled_filters set
          - the hash of (filter_name, headline_id, condition_id, engine)
            falls in the sampled bucket at `sample_rate_per_filter`
          - the per-headline cap `max_cf_per_headline` hasn't been hit

        Same inputs → same answer. No randomness.
        """
        if not self._cfg.enabled:
            return False
        if filter_name not in self.enabled_filters:
            return False
        rate = self._cfg.sample_rate_per_filter
        if rate <= 0.0:
            return False
        # NULL-safe: missing identifiers degrade to "no bypass" — we never
        # want to mass-bypass when a headline_id is missing.
        if not headline_id or not condition_id or not engine:
            return False

        # 10000-bucket hash → threshold = rate * 10000. With rate=0.01 we
        # bypass when the hash bucket falls in [0, 100). 10000 is fine-
        # grained enough for rates ≥ 0.0001 (1 in 10k); the hash is
        # SHA-256 truncated to 32 bits which is more entropy than we need.
        key = f"{filter_name}|{headline_id}|{condition_id}|{engine}".encode("utf-8")
        h = hashlib.sha256(key).digest()
        bucket = int.from_bytes(h[:4], "big") % 10000
        threshold = int(rate * 10000)
        if bucket >= threshold:
            return False

        # Per-headline cap. Read-then-bump intentionally: at the cap we
        # don't bump (so a 4th candidate at cap=3 still reports cap=3).
        n = self._counters.get(headline_id, 0)
        if n >= self._cfg.max_cf_per_headline:
            logger.debug(
                "[cf] headline=%s cap_hit n=%d filter=%s — bypass denied",
                headline_id[:12], n, filter_name,
            )
            return False
        self._counters[headline_id] = n + 1
        logger.info(
            "[cf] BYPASS filter=%s engine=%s cond=%s headline=%s (n=%d/%d)",
            filter_name, engine, condition_id[:12], headline_id[:12],
            n + 1, self._cfg.max_cf_per_headline,
        )
        return True

    def reset_headline_counter(self, headline_id: Optional[str]) -> None:
        """Drop the per-headline counter once the headline is fully processed.

        Safe to call with None / missing keys — pure best-effort cleanup.
        """
        if not headline_id:
            return
        self._counters.pop(headline_id, None)


def load_from_yaml(raw: Optional[dict]) -> CounterfactualConfig:
    """Build a `CounterfactualConfig` from a parsed YAML `counterfactual:` block.

    Missing / empty input → all defaults (i.e. disabled).
    """
    if not raw:
        return CounterfactualConfig()
    enabled_filters_raw = raw.get("enabled_filters")
    enabled_filters: Optional[Set[str]]
    if enabled_filters_raw is None:
        enabled_filters = None
    elif isinstance(enabled_filters_raw, (list, tuple, set)):
        enabled_filters = {str(x) for x in enabled_filters_raw}
    else:
        enabled_filters = None
    return CounterfactualConfig(
        enabled=bool(raw.get("enabled", False)),
        sample_rate_per_filter=float(raw.get("sample_rate_per_filter", 0.01)),
        max_cf_per_headline=int(raw.get("max_cf_per_headline", 3)),
        enabled_filters=enabled_filters,
    )


__all__ = [
    "TOP_5_FILTERS",
    "CounterfactualConfig",
    "CounterfactualSampler",
    "load_from_yaml",
]

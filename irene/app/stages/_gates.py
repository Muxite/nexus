"""Shared pre-trade gate predicates — extracted from `app/stages/trade.py`
so the MVP path (`trade.py`) and the DKG path (`dkg_trade.py`) share one
implementation. Modularization rule 1 forbids `app/stages/*.py` from
importing each other; underscore-prefixed `app/stages/_<helper>.py`
modules are the documented exception (see `app/stages/_entity_overlap.py`).

Each helper is pure (no I/O, no DB, no async). The return shape is
`(status, gate_entry_or_None, reject_reason_or_None)`:

  - status     : "ok" | "reject"
  - gate_entry : a dict suitable to append to gate_history, or None when
                 the live cascade traditionally omits a pass-entry (the
                 mid_degenerate / wide_spread / market_news_imbalance
                 family).
  - reason     : the canonical `decisions.reason` string when rejecting,
                 None on pass.

The same helpers are called by `TradeStage.handle()` and by the
`evaluate_gates()` orchestrator in `trade.py`; that is the contract that
keeps replay from drifting away from production. Cycle 24 added the DKG
path as the third caller — same contract.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional


def _gate_entry(name: str, outcome: str, value, threshold) -> dict:
    return {"gate": name, "outcome": outcome, "value": value, "threshold": threshold}


def _gate_similarity_floor(similarity: float, cfg) -> tuple[str, dict, Optional[str]]:
    threshold = cfg.min_similarity
    if float(similarity) < threshold:
        return (
            "reject",
            _gate_entry("similarity_floor", "reject", float(similarity), threshold),
            "similarity_floor",
        )
    return (
        "ok",
        _gate_entry("similarity_floor", "ok", float(similarity), threshold),
        None,
    )


def _gate_freshness(freshness: Optional[str], cfg) -> tuple[str, dict, Optional[str]]:
    accept = cfg.accept_freshness
    if freshness not in accept:
        return (
            "reject",
            _gate_entry("freshness", "reject", freshness, list(accept)),
            "freshness_stale",
        )
    return (
        "ok",
        _gate_entry("freshness", "ok", freshness, list(accept)),
        None,
    )


def _gate_entity_overlap(overlaps: bool, n_entities: int) -> tuple[str, dict, Optional[str]]:
    if n_entities > 0 and not overlaps:
        return (
            "reject",
            _gate_entry("entity_overlap", "reject", 0, n_entities),
            "entity_mismatch",
        )
    return (
        "ok",
        _gate_entry(
            "entity_overlap",
            "ok",
            "match" if overlaps else "no_entities_yet",
            n_entities,
        ),
        None,
    )


def _gate_direction(
    direction: str, confidence: float, cfg
) -> tuple[str, dict, Optional[str]]:
    if direction == "NEUTRAL":
        return (
            "reject",
            _gate_entry("direction", "reject", "NEUTRAL", "YES|NO"),
            "direction_neutral",
        )
    if confidence < cfg.min_confidence:
        return (
            "reject",
            _gate_entry("direction", "reject", confidence, cfg.min_confidence),
            "direction_lowconf",
        )
    return (
        "ok",
        _gate_entry("direction", "ok", direction, confidence),
        None,
    )


def _gate_mid_unavailable(
    mid: Optional[float],
) -> tuple[str, Optional[dict], Optional[str]]:
    """Cycle 23: reject when `_resolve_mid` could not return a real CLOB
    price. The pre-cycle-23 behaviour was a silent fallback to
    `cfg.trade.default_mid_price` (0.5), which produced 21/39 trades at
    `mid_price=0.5` over the audit window — the bot was buying blind.
    No gate_history append on pass — mirrors `_gate_mid_degenerate`.
    """
    if mid is None:
        return (
            "reject",
            _gate_entry("mid_unavailable", "reject", None, "non_null"),
            "mid_unavailable",
        )
    return ("ok", None, None)


def _gate_market_state(
    market: dict, cfg
) -> tuple[str, Optional[dict], Optional[str]]:
    """Cycle 23: refuse entries on closed/inactive/near-expiry markets.

    The cycle-22 audit found two $200 NO positions placed on markets with
    `end_date=2026-05-25T00:00:00Z` at 19:04Z the same day. TradeStage had
    no market-state gate; the only thing keeping us off closed markets was
    luck (cycle-12 resolution poll catches them post-entry, not pre-entry).

    Reasons (in priority order so the most-actionable label wins):
      - `market_inactive`     — `active != 1`
      - `market_closed`       — `closed_at IS NOT NULL`
      - `market_near_expiry`  — `end_date` parses to within
                                `cfg.min_hours_to_expiry` of now.

    Permissive when `end_date` is missing or unparseable — the upstream
    `market_news_imbalance` gate already handles far/near time logic;
    this gate only fires on KNOWN-bad timestamps.
    """
    active = market.get("active")
    if active is not None and int(active) != 1:
        return (
            "reject",
            _gate_entry("market_state", "reject", "inactive", "active=1"),
            "market_inactive",
        )
    if market.get("closed_at"):
        return (
            "reject",
            _gate_entry(
                "market_state", "reject", market.get("closed_at"), "closed_at IS NULL"
            ),
            "market_closed",
        )
    end_date_raw = market.get("end_date")
    if end_date_raw:
        try:
            # Polymarket emits ISO 8601 with `Z` suffix; fromisoformat
            # rejected `Z` until 3.11 — keep the explicit replace for
            # belt-and-braces across runtimes.
            iso = str(end_date_raw).replace("Z", "+00:00")
            end_dt = datetime.fromisoformat(iso)
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            end_dt = None
        if end_dt is not None:
            now = datetime.now(timezone.utc)
            hours_left = (end_dt - now).total_seconds() / 3600.0
            if hours_left < float(cfg.min_hours_to_expiry):
                return (
                    "reject",
                    _gate_entry(
                        "market_state",
                        "reject",
                        round(hours_left, 2),
                        float(cfg.min_hours_to_expiry),
                    ),
                    "market_near_expiry",
                )
    return ("ok", None, None)


def _gate_mid_degenerate(
    mid: float, cfg
) -> tuple[str, Optional[dict], Optional[str]]:
    """No gate_history append on pass — mirrors live behaviour."""
    if mid <= cfg.min_mid or mid >= cfg.max_mid:
        return (
            "reject",
            _gate_entry("edge", "reject", mid, "mid_degenerate"),
            "mid_degenerate",
        )
    return ("ok", None, None)


def _gate_wide_spread(
    spread: Optional[float], cfg
) -> tuple[str, Optional[dict], Optional[str]]:
    """Skipped (no append, no reject) when spread is None — book read failed."""
    if spread is not None and spread > cfg.wide_spread_threshold:
        return (
            "reject",
            _gate_entry("edge", "reject", spread, "wide_spread"),
            "wide_spread",
        )
    return ("ok", None, None)


def _eval_market_news_imbalance_subrules(
    *,
    market: dict,
    mid: float,
    freshness: Optional[str],
    cluster_size: int,
    cfg,
) -> Optional[tuple[str, object]]:
    """Pure sync evaluation of the five cycle-7 imbalance sub-rules. Returns
    (sub_rule, detail) on reject, None on pass. The async wrapper in
    `TradeStage._check_market_news_imbalance` queries cluster_size and
    delegates here so the pure logic stays testable without a DB."""
    is_breaking = (freshness == "BREAKING")

    # Sub-rule 1: thin market (skip when volume unknown / 0 — parser bug).
    volume = market.get("volume_usdc")
    if isinstance(volume, (int, float)) and volume > 0:
        if volume < cfg.min_market_volume_usdc:
            return ("thin_market", float(volume))

    # Sub-rule 2: extreme mid without BREAKING news.
    if cfg.require_breaking_for_extreme_mid and not is_breaking:
        if mid <= cfg.extreme_mid_low or mid >= cfg.extreme_mid_high:
            return ("extreme_mid_no_breaking", mid)

    # Sub-rules 3 & 4: time-to-expiry. Caller supplies the resolved
    # deadline-days via `market`; the parser logic lives on TradeStage
    # because it depends on ET/UTC handling. evaluate_gates() pre-computes
    # this and stuffs it as `market["_days_to_expiry"]` so this function
    # stays sync. Live handle() also stuffs the same key.
    days = market.get("_days_to_expiry")
    if days is not None:
        if days < cfg.min_days_to_expiry:
            return ("near_expiry", days)
        if cfg.require_breaking_for_far_future and not is_breaking:
            if days > cfg.far_future_days:
                return ("far_future_no_breaking", days)

    # Sub-rule 5: singleton cluster on extreme mid.
    if cfg.reject_singleton_on_extreme_mid:
        if mid <= 0.15 or mid >= 0.85:
            if cluster_size == 1:
                return ("singleton_on_extreme_mid", mid)
    return None


def _gate_market_news_imbalance(
    *,
    market: dict,
    mid: float,
    freshness: Optional[str],
    cluster_size: int,
    cfg,
) -> tuple[str, Optional[dict], Optional[str]]:
    sub = _eval_market_news_imbalance_subrules(
        market=market, mid=mid, freshness=freshness,
        cluster_size=cluster_size, cfg=cfg,
    )
    if sub is not None:
        sub_rule, detail = sub
        return (
            "reject",
            _gate_entry("market_news_imbalance", "reject", sub_rule, detail),
            "market_news_imbalance",
        )
    return ("ok", None, None)


def _gate_edge(edge: float, cfg) -> tuple[str, dict, Optional[str]]:
    if edge < cfg.min_edge:
        return (
            "reject",
            _gate_entry("edge", "reject", edge, cfg.min_edge),
            "edge_below_min",
        )
    return (
        "ok",
        _gate_entry("edge", "ok", edge, cfg.min_edge),
        None,
    )


def _gate_size(
    *, size: float, market_floor_usdc: Optional[float], cfg,
) -> tuple[str, dict, Optional[str], float]:
    """Returns `(status, gate_entry, reason, effective_floor)`."""
    effective_floor = max(cfg.min_position_usdc, market_floor_usdc or 0.0)
    if size < effective_floor:
        if (
            market_floor_usdc is not None
            and market_floor_usdc > cfg.min_position_usdc
        ):
            reason = "below_market_min"
            threshold = market_floor_usdc
        else:
            reason = "size_below_min"
            threshold = cfg.min_position_usdc
        return (
            "reject",
            _gate_entry("size", "reject", size, threshold),
            reason,
            effective_floor,
        )
    return (
        "ok",
        _gate_entry("size", "ok", size, effective_floor),
        None,
        effective_floor,
    )


def _gate_cash(
    *, size: float, cash_available: float,
    effective_floor: float, market_floor_usdc: Optional[float], cfg,
) -> tuple[str, list[dict], Optional[str], float]:
    """Cycle 3 clip-to-cash. Returns `(status, gate_entries, reason, final_size)`.
    The clip-success path produces ONE entry (the clip) and no further ok
    entry — mirrors live cascade behaviour the funnel queries depend on."""
    entries: list[dict] = []
    if cash_available <= 0.0:
        entries.append(_gate_entry("cash", "reject", size, cash_available))
        return ("reject", entries, "insufficient_cash", size)
    if size > cash_available:
        entries.append(_gate_entry("cash", "clip", size, cash_available))
        clipped = cash_available
        if clipped < effective_floor:
            if (
                market_floor_usdc is not None
                and market_floor_usdc > cfg.min_position_usdc
            ):
                reason_clip = "below_market_min"
                threshold_clip = market_floor_usdc
            else:
                reason_clip = "size_below_min"
                threshold_clip = cfg.min_position_usdc
            entries.append(
                _gate_entry("size_after_clip", "reject", clipped, threshold_clip)
            )
            return ("reject", entries, reason_clip, clipped)
        return ("ok", entries, None, clipped)
    entries.append(_gate_entry("cash", "ok", size, cash_available))
    return ("ok", entries, None, size)

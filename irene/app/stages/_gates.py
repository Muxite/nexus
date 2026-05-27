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


def _gate_market_vanished(
    cached: dict, fresh: Optional[dict]
) -> tuple[str, Optional[dict], Optional[str]]:
    """Cycle 25: post-decision re-check that the market still exists.

    `_gate_market_state` runs BEFORE the LLM call against the cached
    market dict captured at lookup time. The markets-stage poller can
    update `active` / `closed_at` mid-flight, so by the time the LLM
    decision lands the cached dict may be stale. This gate re-compares a
    fresh DB read against the cached dict and refuses the trade when the
    market has vanished (row deleted) or transitioned to inactive/closed
    since lookup.

    Why a distinct reason: `market_vanished` is a signal to the audit
    layer that **market discovery / tracking** is unreliable, not that
    the bot itself is misbehaving. Operators triage these differently
    from a pre-decision `market_inactive` (which says "we never should
    have considered this market in the first place").

    `fresh=None` means the row has been deleted between lookup and the
    re-check — flagged as `row_deleted`. Otherwise we report the
    transition direction (was active → now inactive, etc.).
    """
    if fresh is None:
        return (
            "reject",
            _gate_entry("market_vanished", "reject", "row_deleted", "row_exists"),
            "market_vanished",
        )
    cached_active = cached.get("active")
    fresh_active = fresh.get("active")
    if fresh_active is not None and int(fresh_active) != 1 and (
        cached_active is None or int(cached_active) == 1
    ):
        return (
            "reject",
            _gate_entry(
                "market_vanished",
                "reject",
                f"active {cached_active} -> {fresh_active}",
                "active=1",
            ),
            "market_vanished",
        )
    if fresh.get("closed_at") and not cached.get("closed_at"):
        return (
            "reject",
            _gate_entry(
                "market_vanished",
                "reject",
                f"closed_at appeared ({fresh.get('closed_at')})",
                "closed_at IS NULL",
            ),
            "market_vanished",
        )
    return ("ok", None, None)


async def query_open_sibling_markets(db_conn, market_id: str) -> list[str]:
    """Cycle 25-B helper: list open trades on OTHER markets that share the
    same canonical Polymarket event as `market_id`.

    "Canonical event" = the top-scoring row in `market_links` for the
    market (the linker ranks all candidate events; we trust the strongest).
    Two markets sharing the canonical event are mutually-exclusive legs of
    the same binary. The caller passes this list to `_gate_binary_dedup`.

    Lives in `_gates.py` so both `trade.py` (MVP) and `dkg_trade.py` (DKG)
    can use a single query without crossing the no-stage-import rule.
    """
    cur = await db_conn.execute(
        """
        WITH ranked AS (
            SELECT ml.market_condition_id, ml.event_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY ml.market_condition_id
                       ORDER BY ml.final_score DESC, ml.event_id ASC
                   ) AS rk
            FROM market_links ml
        ),
        canonical AS (
            SELECT market_condition_id, event_id
            FROM ranked WHERE rk = 1
        ),
        my_event AS (
            SELECT event_id FROM canonical WHERE market_condition_id = ?
        )
        SELECT DISTINCT t.market_id
        FROM trades t
        LEFT JOIN trade_closures c ON c.trade_id = t.id
        JOIN canonical other ON other.market_condition_id = t.market_id
        JOIN my_event me ON me.event_id = other.event_id
        WHERE c.id IS NULL
          AND t.is_tracer = 0
          AND t.market_id != ?
        """,
        (market_id, market_id),
    )
    rows = await cur.fetchall()
    await cur.close()
    return [r["market_id"] for r in rows]


def _gate_binary_dedup(
    open_sibling_market_ids: list[str],
) -> tuple[str, Optional[dict], Optional[str]]:
    """Cycle 25-B: refuse trades on the second leg of a binary event.

    Polymarket binary events have multiple `markets` rows (one per outcome
    leg). Two markets sharing the same canonical event are mutually
    exclusive — buying NO on both guarantees one leg loses. The pre-cycle-25
    bot did this (trades 38+39: NO on both legs of the Bank of Israel rate
    decision).

    The caller is responsible for fetching the canonical event for the
    candidate market (top-scoring `market_links.event_id` per market) and
    finding open trades on OTHER markets that share that event. This gate
    is then a sync decision: if `open_sibling_market_ids` is non-empty,
    refuse with `reason='binary_dedup'`.

    `open_sibling_market_ids` is the list of OTHER market_ids the bot has
    open positions on within the same canonical event. An empty list
    passes the gate.
    """
    if open_sibling_market_ids:
        return (
            "reject",
            _gate_entry(
                "binary_dedup",
                "reject",
                f"open sibling markets: {','.join(open_sibling_market_ids[:3])}",
                "0 open siblings",
            ),
            "binary_dedup",
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

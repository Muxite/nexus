"""
## Specification

**Purpose.** Subscriber on `signal.created`. For each new signal, run a fixed
gate cascade and either write an execute decision + a paper-trade row + a
fan-out of trade_features rows, or write a reject decision pinned to the
failing gate. Publishes `trade.decided` per signal so monitor/diagnostic
consumers can follow the funnel.

**Public interface.**
- `TradeStage`:
  - `TradeStage(deps: StageDeps)` — requires `deps.ai`.
  - `await start()` — no-op (subscriber stage; the bus drives it).
  - `await stop()` — idempotent.
  - `await handle(payload: dict)` — bus subscriber.

**Owned tables** (`docs/modularization.md` §4): `decisions`, `trades`,
`trade_features`. DDL is registered with `db.register_schema()` at import
time.

**Gate cascade (order is contractual — master-plan §"Phase 1 — Detailed scope").**
For every signal, in order, until one rejects or all pass:

  1. `similarity_floor` — `payload.similarity` vs `cfg.trade.min_similarity`.
  2. `freshness`       — `payload.freshness` ∈ `cfg.trade.accept_freshness`.
  3. `direction`       — call `ai.direction()`, reject if NEUTRAL
                        (`direction_neutral`) or confidence below
                        `cfg.trade.min_confidence` (`direction_lowconf`).
  4. `edge`            — `p_hat` (= confidence on YES, 1-confidence on NO) vs
                        mid-price ± `cfg.trade.fee_buffer`. Mid defaults to
                        `cfg.trade.default_mid_price` (P1 has no live CLOB).
  5. `size`            — fractional-Kelly sizing capped by
                        `cfg.trade.max_position_fraction * balance`; reject
                        if the size falls below `cfg.trade.min_position_usdc`.

The first gate to reject writes a `decisions(outcome='reject', reason=<gate>)`
row and stops. If every gate passes, a single `decisions(outcome='execute',
reason='ok')` row, a `trades` row, and one `trade_features` row per surfaced
feature are written in the same `db.batch()`.

**Invariants.**
- Every signal yields exactly one `decisions` row — reject or execute.
  `UNIQUE(signal_id)` makes a re-handle of the same payload idempotent: the
  second pass sees the existing decision and short-circuits without writing.
- `trades.signal_id` is `UNIQUE` so an execute path is also idempotent.
- `cycle_id` is read once per `handle()` from `cycle.current_cycle_id()` and
  stamped on every owned-table row.
- `is_tracer` / `tracer_id` are read off the inbound payload and threaded
  onto `decisions`, `trades`, and every `trade_features` row.
- `features_snapshot` on `trades` is the full diagnostic JSON — every gate's
  value + threshold + outcome, the prompt, the LLM response excerpt, p_hat,
  mid, edge, kelly_full, kelly_capped, size, balance. This is what
  back-testing/replay reads.

**Side effects.** Reads `articles` (title + summary) and `markets` (question +
description) to compose the LLM prompt. Calls `deps.ai.direction()`. Writes
`decisions` / `trades` / `trade_features` inside `db.batch()`. Publishes
`TOPIC_TRADE_DECIDED` per signal.

**Failure modes.**
- `articles` row missing (race with ingest): logged WARNING; the signal is
  recorded as a reject with reason `'article_missing'` so the funnel stays
  consistent.
- `markets` row missing (race with markets refresh): same shape; reason
  `'market_missing'`.
- `ai.direction()` returning NEUTRAL or low-confidence: handled by the
  direction gate; not a failure mode here.

**Non-goals.**
- Trade closing / mark-to-market / realized PnL — `closed_at` and
  `realized_pnl` columns exist on the `trades` row but stay NULL in P1; a
  follow-up closer stage fills them.
- Live CLOB pricing — mid is `cfg.trade.default_mid_price` in P1.
- Risk management beyond the listed gates (no exposure caps across signals,
  no portfolio constraints, no slippage modeling).
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, time as dt_time, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

from app import db as db_module
from app.cycle import current_cycle_id
from app.deps import StageDeps
from app.stages._entity_overlap import article_entities_overlap_market
from app.stages._gates import (
    _eval_market_news_imbalance_subrules,
    _gate_cash,
    _gate_direction,
    _gate_edge,
    _gate_entity_overlap,
    _gate_entry,
    _gate_freshness,
    _gate_market_news_imbalance,
    _gate_market_state,
    _gate_mid_degenerate,
    _gate_mid_unavailable,
    _gate_similarity_floor,
    _gate_size,
    _gate_wide_spread,
)
from app.topics import TOPIC_TRADE_DECIDED

logger = logging.getLogger(__name__)


_TRADE_DDL = [
    # NOTE (P3 chunk 3): `signal_id` is nullable on both `decisions` and
    # `trades` so DKG-path rows (which have no signal backing them) can
    # land. The MVP path's idempotency is enforced by the partial unique
    # indexes below — `UNIQUE(signal_id) WHERE signal_id IS NOT NULL`.
    # DKG-path idempotency lives in `dkg_trade_links` (owned by
    # `app/stages/dkg_trade.py`).
    """
    CREATE TABLE IF NOT EXISTS decisions (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id       INTEGER,
        outcome         TEXT NOT NULL CHECK(outcome IN ('execute','reject')),
        reason          TEXT NOT NULL,
        gate_history    TEXT NOT NULL,
        prediction_id   INTEGER,
        cycle_id        INTEGER NOT NULL,
        is_tracer       INTEGER NOT NULL DEFAULT 0,
        tracer_id       TEXT,
        created_at      TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_decisions_outcome ON decisions(outcome)",
    "CREATE INDEX IF NOT EXISTS idx_decisions_reason ON decisions(reason)",
    "CREATE INDEX IF NOT EXISTS idx_decisions_cycle_id ON decisions(cycle_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_decisions_signal_id "
    "ON decisions(signal_id) WHERE signal_id IS NOT NULL",
    # NOTE (P3 chunk 3): `signal_id` was MVP-only — DKG trades have no
    # signal row backing them. We broadened the column to nullable +
    # dropped the table-level UNIQUE; the MVP path's per-signal
    # idempotency is now enforced by a partial unique index
    # (`signal_id IS NOT NULL`). The DKG path's idempotency lives in
    # `dkg_trade_links` (owned by `app/stages/dkg_trade.py`).
    """
    CREATE TABLE IF NOT EXISTS trades (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id       INTEGER,
        decision_id     INTEGER NOT NULL REFERENCES decisions(id),
        prediction_id   INTEGER NOT NULL,
        market_id       TEXT NOT NULL,
        direction       TEXT NOT NULL CHECK(direction IN ('YES','NO')),
        confidence      REAL NOT NULL,
        similarity      REAL NOT NULL,
        freshness       TEXT,
        p_hat           REAL NOT NULL,
        mid_price       REAL NOT NULL,
        edge            REAL NOT NULL,
        kelly_fraction  REAL NOT NULL,
        size_usdc       REAL NOT NULL,
        decision_path   TEXT NOT NULL DEFAULT 'mvp',
        features_snapshot TEXT NOT NULL,
        cycle_id        INTEGER NOT NULL,
        is_tracer       INTEGER NOT NULL DEFAULT 0,
        tracer_id       TEXT,
        decided_at      TEXT NOT NULL,
        closed_at       TEXT,
        realized_pnl    REAL,
        -- v6 (cycle 3): the model-implied "edge collapses to here" price.
        -- Closer's edge_decay rule fires when current_mid crosses it.
        -- For YES: target_mid = p_hat - fee_buffer.
        -- For NO:  target_mid = (1 - p_hat) + fee_buffer.
        -- NULL on legacy rows; rule skips when NULL.
        target_mid      REAL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_trades_market_id ON trades(market_id)",
    "CREATE INDEX IF NOT EXISTS idx_trades_cycle_id ON trades(cycle_id)",
    # Partial unique index replaces the old `signal_id UNIQUE` constraint.
    # MVP rows have a non-NULL signal_id; DKG rows leave it NULL, so the
    # index simply doesn't apply to them.
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_trades_signal_id "
    "ON trades(signal_id) WHERE signal_id IS NOT NULL",
    """
    CREATE TABLE IF NOT EXISTS trade_features (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_id        INTEGER NOT NULL REFERENCES trades(id),
        feature_name    TEXT NOT NULL,
        feature_value   REAL,
        feature_kind    TEXT NOT NULL CHECK(feature_kind IN ('numeric','flag','category')),
        feature_label   TEXT,
        UNIQUE(trade_id, feature_name)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_trade_features_trade_id ON trade_features(trade_id)",
    "CREATE INDEX IF NOT EXISTS idx_trade_features_feature_name ON trade_features(feature_name)",
    # Cycle 14: per-signal latency-chain instrumentation. Total wall-clock
    # ms from `handle()` entry to the `trades` row insert (`decision_latency_ms`),
    # plus a per-step breakdown JSON keyed by step name (`fetch_article`,
    # `fetch_market`, `ai_direction`, `resolve_mid`, `resolve_book`,
    # `imbalance_check`, `db_insert`). NULL on legacy + reject rows;
    # populated only on execute paths. See calibration_seeds.md C4/E2 and
    # philosophy.md's 200ms budget — the breakdown is what lets us
    # actually replan that number once chains widen past a single
    # CLOB call. ALTER TABLE rows are sha1-tracked so re-importing
    # the module is idempotent.
    "ALTER TABLE trades ADD COLUMN decision_latency_ms        REAL",
    "ALTER TABLE trades ADD COLUMN decision_latency_breakdown TEXT",
    # Cycle 16: extend latency-chain instrumentation to REJECT paths.
    # ~95%+ of signals fail the gate cascade and never write a `trades`
    # row, so the cycle-14 columns above only reveal half the latency
    # story. Mirror the two columns onto `decisions`: total ms from
    # handle()-entry up to the reject point, plus the partial breakdown
    # dict of whichever steps ran before the gate fired. A `similarity_floor`
    # reject (gate 1, no fetch) will hold an empty breakdown; an
    # `edge_below_min` reject (gate 4) will have fetch_article +
    # fetch_market + ai_direction + resolve_mid + resolve_book. Execute
    # rows on `decisions` leave both NULL — the canonical chain numbers
    # for those live on the `trades` row instead. ALTER TABLE rows
    # are sha1-tracked so re-importing the module is idempotent.
    "ALTER TABLE decisions ADD COLUMN decision_latency_ms        REAL",
    "ALTER TABLE decisions ADD COLUMN decision_latency_breakdown TEXT",
]
db_module.register_schema("app/stages/trade.py", _TRADE_DDL)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _market_min_usdc(
    *,
    min_shares,
    executable_price: Optional[float],
    mid: float,
    direction: str,
) -> tuple[Optional[float], Optional[str]]:
    """Convert a per-market `minimum_order_size` (in SHARES) to a USDC
    floor for the size gate.

    The Polymarket platform `minimum_order_size` is a per-market floor
    measured in SHARES. To compare against `size` (USDC) we use the
    price-per-share we'd actually pay on this side:

      - YES buy: `executable_price` is `best_ask` (the YES we'd pay).
      - NO buy:  `executable_price` is `1 - best_ask_no` projected back
                 into YES-price terms. The NO share's cost is
                 `1 - executable_price` (i.e. `best_ask_no`).

    When the book is unavailable (`executable_price is None`) we fall
    back to `mid` for the YES side and `1 - mid` for the NO side — same
    side-aware logic as the legacy edge fallback. This is intentionally
    conservative: the platform fee is in USDC at fill time and we don't
    want a missing-book event to silently bypass the gate.

    Returns `(usdc_floor, source)` where `source` is one of:
      - `None` if `min_shares` was NULL / negative / unparseable, OR
        we couldn't derive a sane price scalar (mid degenerate).
      - `"executable"` when we used the live book.
      - `"mid_fallback"` when we used `mid` (book unavailable).
    """
    if min_shares is None:
        return None, None
    try:
        ms = float(min_shares)
    except (TypeError, ValueError):
        return None, None
    if ms <= 0.0:
        # Zero (or negative) shares is a no-op floor — return 0 with a
        # source so we get a sane `gate_history` audit but never reject.
        return 0.0, "executable"
    if executable_price is not None:
        # Convert the YES-price executable to the per-share USDC cost
        # for the bet's side. YES side pays `executable_price`; NO side
        # pays `1 - executable_price` per share.
        per_share = (
            float(executable_price)
            if direction == "YES"
            else 1.0 - float(executable_price)
        )
        if per_share <= 0.0:
            return None, None
        return ms * per_share, "executable"
    # Book-unavailable fallback: use mid (YES) or 1 - mid (NO). `mid` is
    # the YES mid by convention (see `_resolve_mid`); guard against the
    # degenerate edge so we don't multiply by 0.
    per_share = mid if direction == "YES" else (1.0 - mid)
    if per_share <= 0.0:
        return None, None
    return ms * per_share, "mid_fallback"


#: Cycle 24 (handoff H): hard cap on the article-body slice fed to the
#: LLM. ~3000 chars ≈ 750 tokens — leaves comfortable room for the
#: market block + instructions + system message inside the ~4000-token
#: `cfg.openrouter.max_input_tokens` budget without unbounded LLM cost
#: on a 12 KB long-form article.
MAX_ARTICLE_CHARS_IN_PROMPT = 3000


def compose_direction_prompt(article: dict, market: dict) -> str:
    """Shared LLM-direction prompt builder for the MVP + DKG trade paths.

    Both paths read the news as plain language and ask whether it
    decisively resolves the market — see `docs/philosophy.md`. The
    function is module-level so `app/stages/dkg_trade.py` can call it
    without inheriting any state from `TradeStage`.

    Cycle 24 (handoff H — LLM context richness):
    - Prefer `body` (full trafilatura-cleaned article text) over
      `summary` (often 1-sentence RSS lede) when the body is meaningfully
      longer — gives the LLM the supporting context the cycle-22 Iran
      "decisively resolve" audit flagged as missing.
    - Hard-cap the article slice at `MAX_ARTICLE_CHARS_IN_PROMPT` so a
      runaway long-form article cannot blow the input budget.
    - Surface `end_date`, `volume_usdc`, `category` so the LLM can reason
      about resolution timing and market depth, not just the question
      text.
    - Soften the NEUTRAL guidance (the bot was over-abstaining per the
      cycle-22 audit) — the gate cascade still filters on confidence, so
      letting the model express a low-confidence directional lean is
      preferable to a hard NEUTRAL whenever there is any visible cue.
    """
    title = article.get("title") or ""
    summary = article.get("summary") or ""
    body = article.get("body") or ""
    # Prefer body when it adds meaningful context vs the (often very
    # short) RSS summary. The 1.5× floor avoids swapping a clean
    # 1-sentence lede for a 1-sentence trafilatura excerpt that happens
    # to be a few characters longer.
    if body and len(body) > max(len(summary) * 1.5, 200):
        article_text = body
    elif summary:
        article_text = summary
    else:
        article_text = "(no body or summary available)"
    if len(article_text) > MAX_ARTICLE_CHARS_IN_PROMPT:
        article_text = (
            article_text[:MAX_ARTICLE_CHARS_IN_PROMPT] + " […truncated]"
        )

    question = market.get("question") or ""
    description = market.get("description") or "(none)"
    end_date = market.get("end_date") or "(unknown)"
    volume = market.get("volume_usdc")
    if isinstance(volume, (int, float)):
        volume_str = f"{float(volume):,.0f} USDC"
    else:
        volume_str = "?"
    category = market.get("category") or "(uncategorized)"

    return (
        f"=== NEWS ARTICLE ===\n"
        f"Headline: {title}\n"
        f"\n"
        f"{article_text}\n"
        f"\n"
        f"=== MARKET ===\n"
        f"Question: {question}\n"
        f"Resolution deadline: {end_date}\n"
        f"Volume (24h): {volume_str}\n"
        f"Category: {category}\n"
        f"\n"
        f"Details: {description}\n"
        f"\n"
        f"=== INSTRUCTIONS ===\n"
        f"Read the news as plain language. Does the news DECISIVELY\n"
        f"resolve this market?\n"
        f"\n"
        f"  YES     — the news plainly says the market's YES outcome\n"
        f"            will happen or has just happened.\n"
        f"  NO      — the news plainly says the market's NO outcome\n"
        f"            will happen or has just happened.\n"
        f"  NEUTRAL — the news is unrelated, ambiguous, or genuinely\n"
        f"            doesn't push the market either way. Prefer YES or\n"
        f"            NO with a low confidence (e.g. 0.4) when you can\n"
        f"            see a directional lean even if the resolution\n"
        f"            isn't certain — the gate handles confidence\n"
        f"            filtering downstream. Reserve NEUTRAL for the\n"
        f"            truly orthogonal cases.\n"
        f"\n"
        f"`confidence` is how CLEARLY the news resolves the market\n"
        f"(not a probability of YES). 0.9 = the article spells out\n"
        f"the resolution; 0.5 = related but ambiguous; below the gate\n"
        f"falls through to NEUTRAL anyway.\n"
        f"\n"
        f"Output JSON ONLY."
    )


# ---------------------------------------------------------------------------
# Pure gate predicates — extracted for the replay harness so the replay
# runs *exactly* the same cascade logic the live bot runs. Each helper is
# pure (no I/O, no DB, no async). Returns
# `(status, gate_entry_or_None, reject_reason_or_None)`:
#   - status     : "ok" | "reject"
#   - gate_entry : a dict suitable to append to gate_history, or None when
#                  the live cascade traditionally omits a pass-entry (the
#                  mid_degenerate / wide_spread / market_news_imbalance
#                  family).
#   - reason     : the canonical `decisions.reason` string when rejecting,
#                  None on pass.
#
# The same helpers are called by `TradeStage.handle()` and by the
# `evaluate_gates()` orchestrator below; that is the contract that keeps
# replay from drifting away from production.
# ---------------------------------------------------------------------------


def _compute_edge(
    *, direction: str, p_hat: float, mid: float,
    executable_price: Optional[float], spread: Optional[float], cfg,
) -> tuple[float, dict]:
    """Cycle 10 explicit-spread formula when book is available, else legacy
    lumped-fee fallback. Returns `(edge, edge_components_dict)`."""
    if executable_price is not None and spread is not None:
        half_spread = float(spread) / 2.0
        if direction == "YES":
            edge = p_hat - executable_price - cfg.taker_fee_buffer - half_spread
        else:
            edge = executable_price - p_hat - cfg.taker_fee_buffer - half_spread
        edge_components = {
            "mid": mid,
            "executable_price": float(executable_price),
            "spread": float(spread),
            "half_spread": half_spread,
            "taker_fee_buffer": float(cfg.taker_fee_buffer),
            "fallback": False,
        }
    else:
        if direction == "YES":
            edge = p_hat - mid - cfg.fee_buffer
        else:
            edge = mid - p_hat - cfg.fee_buffer
        edge_components = {
            "mid": mid,
            "fee_buffer": float(cfg.fee_buffer),
            "fallback": True,
        }
    return edge, edge_components


def _compute_kelly_and_size(
    *, direction: str, p_hat: float, mid: float, cfg,
) -> tuple[float, float, float]:
    """Cycle 3 Kelly math + balance × max_position_fraction cap. Returns
    `(kelly_full, kelly_capped, size_usdc)`."""
    if direction == "YES":
        b = (1.0 - mid) / mid
        kelly_full = (p_hat * b - (1.0 - p_hat)) / b
    else:
        p_no = 1.0 - p_hat
        mid_no = 1.0 - mid
        b = (1.0 - mid_no) / mid_no
        kelly_full = (p_no * b - (1.0 - p_no)) / b
    kelly_capped = max(0.0, kelly_full) * cfg.kelly_fraction
    size = min(
        kelly_capped * cfg.balance_usdc,
        cfg.balance_usdc * cfg.max_position_fraction,
    )
    return kelly_full, kelly_capped, size


# ---------------------------------------------------------------------------
# Replay surface: GateFeatures / GateResult / evaluate_gates.
#
# The replay harness (`scripts/replay/predict.py`) constructs a
# GateFeatures from snapshot data and calls evaluate_gates() to obtain a
# GateResult. The live `TradeStage.handle()` does the same gate decisions
# via the per-gate helpers above; both paths share one implementation.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GateFeatures:
    """Inputs to `evaluate_gates`. Fields without values default to a
    permissive sentinel (None for optional gates, True for presence checks).
    The replay harness should populate every field it has; gates whose
    required inputs are None will raise (a wiring bug) — except where the
    field is genuinely optional (executable_price/spread, entity_overlap)."""

    similarity: float
    freshness: Optional[str]
    article_present: bool = True
    market_present: bool = True
    # (overlaps, n_entities). None = skip the gate (older signals predate
    # the cycle-7 entity_overlap check).
    entity_overlap_result: Optional[tuple[bool, int]] = None
    # LLM result — must be populated before the direction gate runs.
    direction: Optional[str] = None  # 'YES' | 'NO' | 'NEUTRAL'
    confidence: Optional[float] = None
    prediction_id: Optional[int] = None
    # Price scalars resolved from the CLOB (or stub).
    mid: Optional[float] = None
    executable_price: Optional[float] = None
    spread: Optional[float] = None
    # Market dict — fed verbatim to imbalance sub-rules. Must carry
    # `volume_usdc`, `_days_to_expiry`, `min_order_size`.
    market: Optional[dict] = None
    cluster_size: int = 1
    # Paper-wallet cash at decision time.
    cash_available: Optional[float] = None


@dataclass(frozen=True)
class GateResult:
    """Output of `evaluate_gates`. Either `outcome='execute'` (every gate
    passed) or `outcome='reject'` with `reason` naming the failing gate."""

    outcome: str  # 'execute' | 'reject'
    reason: str
    gate_history: list[dict]
    # Execute-only fields (None on reject).
    direction: Optional[str] = None
    confidence: Optional[float] = None
    p_hat: Optional[float] = None
    mid: Optional[float] = None
    edge: Optional[float] = None
    edge_components: Optional[dict] = None
    kelly_full: Optional[float] = None
    kelly_capped: Optional[float] = None
    size_usdc: Optional[float] = None
    market_floor_usdc: Optional[float] = None
    effective_floor: Optional[float] = None
    prediction_id: Optional[int] = None


def evaluate_gates(features: GateFeatures, cfg) -> GateResult:
    """Pure (sync) cascade runner. Same gates, same order, same helpers as
    `TradeStage.handle()`. The function does no I/O — all features must be
    pre-populated. Replay use only; live trades go through `handle()` which
    interleaves I/O between gates for short-circuit savings.
    """
    history: list[dict] = []

    # Gate 1 — similarity_floor.
    status, entry, reason = _gate_similarity_floor(features.similarity, cfg)
    history.append(entry)
    if status == "reject":
        return GateResult("reject", reason, history)

    # Gate 2 — freshness.
    status, entry, reason = _gate_freshness(features.freshness, cfg)
    history.append(entry)
    if status == "reject":
        return GateResult("reject", reason, history)

    # Presence checks (not gates, but they reject pre-LLM).
    if not features.article_present:
        history.append(_gate_entry("lookup", "reject", "article_missing", None))
        return GateResult("reject", "article_missing", history)
    if not features.market_present:
        history.append(_gate_entry("lookup", "reject", "market_missing", None))
        return GateResult("reject", "market_missing", history)

    # Gate 2.25 — market_state (cycle 23). Permissive when the relevant
    # columns are missing from the snapshot (historical replays predate the
    # active/closed_at/end_date fields landing in features_snapshot).
    if features.market is not None:
        status, entry, reason = _gate_market_state(features.market, cfg)
        if status == "reject":
            history.append(entry)
            return GateResult("reject", reason, history)

    # Gate 2.5 — entity_overlap. Skipped if caller didn't compute it.
    if features.entity_overlap_result is not None:
        overlaps, n_entities = features.entity_overlap_result
        status, entry, reason = _gate_entity_overlap(overlaps, n_entities)
        history.append(entry)
        if status == "reject":
            return GateResult("reject", reason, history)

    # Gate 3 — direction.
    if features.direction is None or features.confidence is None:
        raise ValueError(
            "evaluate_gates: features.direction / features.confidence required "
            "before the direction gate can run"
        )
    status, entry, reason = _gate_direction(
        features.direction, features.confidence, cfg,
    )
    history.append(entry)
    if status == "reject":
        return GateResult(
            "reject", reason, history, prediction_id=features.prediction_id,
        )

    direction = features.direction
    confidence = features.confidence
    p_hat = confidence if direction == "YES" else (1.0 - confidence)

    # Gate 4-pre — mid_unavailable (cycle 23). Replaces the
    # pre-cycle-23 `raise ValueError` so replay sees the same rejection a
    # live trade would see when CLOB returns no price.
    status, entry, reason = _gate_mid_unavailable(features.mid)
    if status == "reject":
        history.append(entry)
        return GateResult(
            "reject", reason, history, prediction_id=features.prediction_id,
        )
    mid = features.mid

    # Gate 4 — mid_degenerate (no append on pass).
    status, entry, reason = _gate_mid_degenerate(mid, cfg)
    if status == "reject":
        history.append(entry)
        return GateResult(
            "reject", reason, history, prediction_id=features.prediction_id,
        )

    # Gate 4a-bis — wide_spread (skipped silently when spread is None).
    status, entry, reason = _gate_wide_spread(features.spread, cfg)
    if status == "reject":
        history.append(entry)
        return GateResult(
            "reject", reason, history, prediction_id=features.prediction_id,
        )

    # Gate 4b — market_news_imbalance.
    if features.market is None:
        raise ValueError("evaluate_gates: features.market required before imbalance gate")
    status, entry, reason = _gate_market_news_imbalance(
        market=features.market, mid=mid, freshness=features.freshness,
        cluster_size=features.cluster_size, cfg=cfg,
    )
    if status == "reject":
        history.append(entry)
        return GateResult(
            "reject", reason, history, prediction_id=features.prediction_id,
        )

    # Compute edge.
    edge, edge_components = _compute_edge(
        direction=direction, p_hat=p_hat, mid=mid,
        executable_price=features.executable_price, spread=features.spread, cfg=cfg,
    )

    # Gate 5 — edge.
    status, entry, reason = _gate_edge(edge, cfg)
    history.append(entry)
    if status == "reject":
        return GateResult(
            "reject", reason, history, prediction_id=features.prediction_id,
        )

    # Compute Kelly + size.
    kelly_full, kelly_capped, size = _compute_kelly_and_size(
        direction=direction, p_hat=p_hat, mid=mid, cfg=cfg,
    )

    # Gate 6 — size (with per-market floor conversion).
    min_shares = features.market.get("min_order_size") if features.market else None
    market_floor_usdc, _floor_source = _market_min_usdc(
        min_shares=min_shares,
        executable_price=features.executable_price,
        mid=mid,
        direction=direction,
    )
    status, entry, reason, effective_floor = _gate_size(
        size=size, market_floor_usdc=market_floor_usdc, cfg=cfg,
    )
    history.append(entry)
    if status == "reject":
        return GateResult(
            "reject", reason, history,
            market_floor_usdc=market_floor_usdc,
            effective_floor=effective_floor,
            prediction_id=features.prediction_id,
        )

    # Gate 7 — cash (clip + post-clip floor check).
    if features.cash_available is None:
        raise ValueError("evaluate_gates: features.cash_available required before cash gate")
    status, entries, reason, final_size = _gate_cash(
        size=size, cash_available=features.cash_available,
        effective_floor=effective_floor, market_floor_usdc=market_floor_usdc, cfg=cfg,
    )
    history.extend(entries)
    if status == "reject":
        return GateResult(
            "reject", reason, history,
            market_floor_usdc=market_floor_usdc,
            effective_floor=effective_floor,
            prediction_id=features.prediction_id,
        )

    return GateResult(
        outcome="execute",
        reason="ok",
        gate_history=history,
        direction=direction,
        confidence=confidence,
        p_hat=p_hat,
        mid=mid,
        edge=edge,
        edge_components=edge_components,
        kelly_full=kelly_full,
        kelly_capped=kelly_capped,
        size_usdc=final_size,
        market_floor_usdc=market_floor_usdc,
        effective_floor=effective_floor,
        prediction_id=features.prediction_id,
    )


class TradeStage:
    def __init__(self, deps: StageDeps) -> None:
        if deps.ai is None:
            raise RuntimeError("TradeStage requires deps.ai")
        self._db = deps.db
        self._bus = deps.bus
        self._ai = deps.ai
        self._cfg = deps.config.trade
        # Optional — when set, edge gate uses real CLOB mid for the bet
        # side; when None or the call returns None, we fall back to the
        # default_mid_price stub. This keeps tests + tracer harnesses
        # working without a live CLOB.
        self._clob = deps.clob

    async def start(self) -> None:
        return

    async def stop(self) -> None:
        return

    async def handle(self, payload: dict) -> None:
        # Cycle 14: per-signal latency-chain timer. `t_start` brackets the
        # whole gate cascade up to (and including) the trade-row insert on
        # execute paths. `breakdown` accumulates `{step_name: ms}` for the
        # major async steps so we can see which call dominates a chain.
        # See calibration_seeds.md C4/E2 + plans/instrumentation. Reject
        # paths leave both columns NULL on `decisions` rows in this cycle
        # (reject-path latency is a follow-up); we still record the timer
        # data inline but it just isn't persisted.
        t_start = time.perf_counter()
        breakdown: dict[str, float] = {}

        signal_id = payload.get("signal_id")
        article_id = payload.get("article_id")
        market_id = payload.get("market_id")
        similarity = payload.get("similarity")
        freshness = payload.get("freshness")
        if signal_id is None or article_id is None or market_id is None or similarity is None:
            logger.warning("trade: payload missing required field: %r", payload)
            return
        is_tracer = bool(payload.get("is_tracer", False))
        tracer_id = payload.get("tracer_id")

        if await self._decision_exists(int(signal_id)):
            return

        cycle_id = current_cycle_id()
        gate_history: list[dict] = []

        # 0. execution kill switch (cycle 23) --------------------------------
        # Lands BEFORE any LLM/CLOB-cost work so a halted bot doesn't burn
        # tokens or hit Polymarket. Still writes a decisions row so the
        # soak funnel shows the rejection — operationally distinguishable
        # from a silent stall.
        if not self._cfg.execution_enabled:
            gate_history.append(
                _gate_entry("execution_enabled", "reject", False, True)
            )
            await self._record_reject(
                signal_id=int(signal_id),
                reason="execution_disabled",
                gate_history=gate_history,
                prediction_id=None,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
                decision_latency_ms=(time.perf_counter() - t_start) * 1000.0,
                decision_latency_breakdown=dict(breakdown),
            )
            return

        # 1. similarity_floor -------------------------------------------------
        status, entry, reason = _gate_similarity_floor(similarity, self._cfg)
        gate_history.append(entry)
        if status == "reject":
            await self._record_reject(
                signal_id=int(signal_id),
                reason=reason,
                gate_history=gate_history,
                prediction_id=None,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
                decision_latency_ms=(time.perf_counter() - t_start) * 1000.0,
                decision_latency_breakdown=dict(breakdown),
            )
            return

        # 2. freshness --------------------------------------------------------
        status, entry, reason = _gate_freshness(freshness, self._cfg)
        gate_history.append(entry)
        if status == "reject":
            await self._record_reject(
                signal_id=int(signal_id),
                reason=reason,
                gate_history=gate_history,
                prediction_id=None,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
                decision_latency_ms=(time.perf_counter() - t_start) * 1000.0,
                decision_latency_breakdown=dict(breakdown),
            )
            return

        # Compose prompt from article + market. Done after the cheap gates so
        # we don't pay a DB round-trip for signals that fail upstream.
        _t = time.perf_counter()
        article = await self._fetch_article(int(article_id))
        breakdown["fetch_article"] = (time.perf_counter() - _t) * 1000.0
        _t = time.perf_counter()
        market = await self._fetch_market(str(market_id))
        breakdown["fetch_market"] = (time.perf_counter() - _t) * 1000.0
        if article is None:
            await self._record_reject(
                signal_id=int(signal_id),
                reason="article_missing",
                gate_history=gate_history + [_gate_entry("lookup", "reject", "article_missing", None)],
                prediction_id=None,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
                decision_latency_ms=(time.perf_counter() - t_start) * 1000.0,
                decision_latency_breakdown=dict(breakdown),
            )
            return
        if market is None:
            await self._record_reject(
                signal_id=int(signal_id),
                reason="market_missing",
                gate_history=gate_history + [_gate_entry("lookup", "reject", "market_missing", None)],
                prediction_id=None,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
                decision_latency_ms=(time.perf_counter() - t_start) * 1000.0,
                decision_latency_breakdown=dict(breakdown),
            )
            return

        # 2.25 market_state (cycle 23) ---------------------------------------
        # Refuses entries on closed / inactive / near-expiry markets. Lands
        # before the entity-overlap DB read and the LLM direction call so a
        # bad market never burns tokens. Permissive when the relevant
        # columns are NULL (don't reject on missing data — the upstream
        # market_news_imbalance gate covers expiry distance separately).
        status, entry, reason = _gate_market_state(market, self._cfg)
        if status == "reject":
            gate_history.append(entry)
            await self._record_reject(
                signal_id=int(signal_id),
                reason=reason,
                gate_history=gate_history,
                prediction_id=None,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
                decision_latency_ms=(time.perf_counter() - t_start) * 1000.0,
                decision_latency_breakdown=dict(breakdown),
            )
            return

        # 2.5 entity_overlap (stage-0 hard filter) ---------------------------
        # Reject (article, market) pairs where the article's resolved
        # entities don't appear in the market question. Saves an LLM
        # call on obvious noise matches. Permissive when the extractor
        # hasn't completed for this article yet — `n_entities == 0`
        # means "no signal", not "no match".
        overlaps, n_entities = await article_entities_overlap_market(
            self._db, int(article_id), market["question"] or ""
        )
        status, entry, reason = _gate_entity_overlap(overlaps, n_entities)
        gate_history.append(entry)
        if status == "reject":
            await self._record_reject(
                signal_id=int(signal_id),
                reason=reason,
                gate_history=gate_history,
                prediction_id=None,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
                decision_latency_ms=(time.perf_counter() - t_start) * 1000.0,
                decision_latency_breakdown=dict(breakdown),
            )
            return

        prompt = self._compose_prompt(article, market)

        # 3. direction --------------------------------------------------------
        _t = time.perf_counter()
        result = await self._ai.direction(
            signal_id=int(signal_id),
            prompt=prompt,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
        )
        breakdown["ai_direction"] = (time.perf_counter() - _t) * 1000.0
        status, entry, reason = _gate_direction(
            result.direction, result.confidence, self._cfg,
        )
        gate_history.append(entry)
        if status == "reject":
            await self._record_reject(
                signal_id=int(signal_id),
                reason=reason,
                gate_history=gate_history,
                prediction_id=result.prediction_id,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
                decision_latency_ms=(time.perf_counter() - t_start) * 1000.0,
                decision_latency_breakdown=dict(breakdown),
            )
            return

        # 4. edge -------------------------------------------------------------
        direction = result.direction
        confidence = result.confidence
        p_hat = confidence if direction == "YES" else (1.0 - confidence)
        _t = time.perf_counter()
        mid = await self._resolve_mid(direction=direction, market=market)
        breakdown["resolve_mid"] = (time.perf_counter() - _t) * 1000.0
        # 4-pre. mid_unavailable (cycle 23) ----------------------------------
        # Replaces the pre-cycle-23 silent fallback to default_mid_price=0.5
        # which produced 21/39 trades at stub mid. `_resolve_mid` now returns
        # None on any non-real-mid path; we reject loudly.
        status, entry, reason = _gate_mid_unavailable(mid)
        if status == "reject":
            gate_history.append(entry)
            await self._record_reject(
                signal_id=int(signal_id),
                reason=reason,
                gate_history=gate_history,
                prediction_id=result.prediction_id,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
                decision_latency_ms=(time.perf_counter() - t_start) * 1000.0,
                decision_latency_breakdown=dict(breakdown),
            )
            return
        # 4a. degenerate-mid guard — markets at mid <= min_mid or >= max_mid
        # are essentially resolved. Cycle 3 surfaced PnL=0 trades on these
        # because the closer would force-close at entry_mid. Block at entry.
        status, entry, reason = _gate_mid_degenerate(mid, self._cfg)
        if status == "reject":
            gate_history.append(entry)
            await self._record_reject(
                signal_id=int(signal_id),
                reason=reason,
                gate_history=gate_history,
                prediction_id=result.prediction_id,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
                decision_latency_ms=(time.perf_counter() - t_start) * 1000.0,
                decision_latency_breakdown=dict(breakdown),
            )
            return
        # 4a-bis. wide-spread guard — Polymarket's /midpoint returns the
        # last traded price (not the true mid) when spread > $0.10. Any
        # edge estimate built on that scalar is unreliable. Read the book
        # and reject if spread exceeds the threshold. Graceful degrade:
        # if the book read fails (None), skip this check rather than
        # blocking the trade. See app/clob.py:get_top_of_book.
        _t = time.perf_counter()
        book = await self._resolve_executable_side(direction=direction, market=market)
        breakdown["resolve_book"] = (time.perf_counter() - _t) * 1000.0
        if book is not None:
            executable_price, spread = book
        else:
            executable_price, spread = None, None
        status, entry, reason = _gate_wide_spread(spread, self._cfg)
        if status == "reject":
            gate_history.append(entry)
            await self._record_reject(
                signal_id=int(signal_id),
                reason=reason,
                gate_history=gate_history,
                prediction_id=result.prediction_id,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
                decision_latency_ms=(time.perf_counter() - t_start) * 1000.0,
                decision_latency_breakdown=dict(breakdown),
            )
            return
        # 4b. market-news weight imbalance — reject when the market is
        # too big / too thin / too far / too soon for the news to
        # plausibly move it. Cycle 7. See _check_market_news_imbalance.
        _t = time.perf_counter()
        imbalance = await self._check_market_news_imbalance(
            market=market,
            mid=mid,
            freshness=freshness,
            signal_id=int(signal_id),
        )
        breakdown["imbalance_check"] = (time.perf_counter() - _t) * 1000.0
        if imbalance is not None:
            sub_rule, detail = imbalance
            gate_history.append(
                _gate_entry("market_news_imbalance", "reject", sub_rule, detail)
            )
            await self._record_reject(
                signal_id=int(signal_id),
                reason="market_news_imbalance",
                gate_history=gate_history,
                prediction_id=result.prediction_id,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
                decision_latency_ms=(time.perf_counter() - t_start) * 1000.0,
                decision_latency_breakdown=dict(breakdown),
            )
            return
        # Cycle 10: explicit-spread-term refactor. When the live book is
        # available, use the executable side + half-spread + an explicit
        # taker-fee buffer instead of the single lumped `fee_buffer`
        # constant. The fallback path (book-unavailable) preserves the
        # legacy behaviour so paper/test environments without a book
        # source keep reproducing prior cycle-7 trade counts.
        # See irene/plans/order-execution-spreads.md §2.
        edge, edge_components = _compute_edge(
            direction=direction, p_hat=p_hat, mid=mid,
            executable_price=executable_price, spread=spread, cfg=self._cfg,
        )
        status, entry, reason = _gate_edge(edge, self._cfg)
        gate_history.append(entry)
        if status == "reject":
            await self._record_reject(
                signal_id=int(signal_id),
                reason=reason,
                gate_history=gate_history,
                prediction_id=result.prediction_id,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
                decision_latency_ms=(time.perf_counter() - t_start) * 1000.0,
                decision_latency_breakdown=dict(breakdown),
            )
            return

        # 5. size -------------------------------------------------------------
        kelly_full, kelly_capped, size = _compute_kelly_and_size(
            direction=direction, p_hat=p_hat, mid=mid, cfg=self._cfg,
        )
        # Cycle 13: per-market platform floor (plans/order-execution-spreads.md
        # proposal #4). Polymarket exposes `minimum_order_size` in SHARES on
        # `GET /markets/<cid>`. To compare against `size` (USDC) we must
        # convert via the price we'd actually pay on this side:
        #   min_usdc_on_this_market = min_shares × executable_price
        # `executable_price` (best_ask for YES, 1 - best_ask_no for NO,
        # both in YES-price units) is the right scalar — it's exactly what
        # the exchange will fill at. Fall back to `mid` when the book read
        # failed so we still get a sane USDC estimate. The gate floor is
        # the MAX of the global config and the market-derived floor — the
        # config is still in force as a baseline; the per-market value
        # only ever ratchets the floor UP.
        min_shares = market.get("min_order_size")
        market_floor_usdc, _floor_source = _market_min_usdc(
            min_shares=min_shares,
            executable_price=executable_price,
            mid=mid,
            direction=direction,
        )
        status, entry, reason, effective_floor = _gate_size(
            size=size, market_floor_usdc=market_floor_usdc, cfg=self._cfg,
        )
        gate_history.append(entry)
        if status == "reject":
            await self._record_reject(
                signal_id=int(signal_id),
                reason=reason,
                gate_history=gate_history,
                prediction_id=result.prediction_id,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
                decision_latency_ms=(time.perf_counter() - t_start) * 1000.0,
                decision_latency_breakdown=dict(breakdown),
            )
            return

        # 6. cash (paper wallet) ---------------------------------------------
        # Cycle 3: CLIP size to fit available cash instead of rejecting
        # outright. Multiple concurrent signals on the same market can
        # add to exposure up to whatever cash allows. Only when even the
        # clipped size falls below min_position_usdc do we reject.
        # Cash = initial - sum(open trade sizes), never negative.
        cur = await self._db.conn.execute(
            "SELECT COALESCE(SUM(t.size_usdc), 0) "
            "FROM trades t LEFT JOIN trade_closures c ON c.trade_id=t.id "
            "WHERE c.id IS NULL AND t.is_tracer=0"
        )
        row = await cur.fetchone()
        await cur.close()
        open_invested = float(row[0] or 0.0)
        cash_available = self._cfg.balance_usdc - open_invested
        status, entries, reason, final_size = _gate_cash(
            size=size, cash_available=cash_available,
            effective_floor=effective_floor, market_floor_usdc=market_floor_usdc,
            cfg=self._cfg,
        )
        gate_history.extend(entries)
        if status == "reject":
            await self._record_reject(
                signal_id=int(signal_id),
                reason=reason,
                gate_history=gate_history,
                prediction_id=result.prediction_id,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
                decision_latency_ms=(time.perf_counter() - t_start) * 1000.0,
                decision_latency_breakdown=dict(breakdown),
            )
            return
        # Adopt the (possibly clipped) size for downstream feature/PnL math.
        size = final_size

        # Execute path --------------------------------------------------------
        # PR-B: `market_at_trade` pins the subset of the market dict that
        # influenced this decision — `markets` is mutable across cycles, so
        # the replay can't trust the current row. Captures: per-market
        # fee_rate, volume_usdc, category, min_order_size, end_date. spread
        # is already preserved inside `edge_components` for the book-aware
        # path.
        features_snapshot = {
            "gate_history": gate_history,
            "prompt": prompt,
            "llm_response_excerpt": (result.raw_response or "")[:512],
            "similarity": float(similarity),
            "freshness": freshness,
            "direction": direction,
            "confidence": confidence,
            "p_hat": p_hat,
            "mid": mid,
            "edge": edge,
            "edge_components": edge_components,
            "kelly_full": kelly_full,
            "kelly_capped": kelly_capped,
            "size_usdc": size,
            "balance_usdc": self._cfg.balance_usdc,
            "market_at_trade": {
                "fee_rate": market.get("fee_rate"),
                "volume_usdc": market.get("volume_usdc"),
                "category": market.get("category"),
                "min_order_size": market.get("min_order_size"),
                "end_date": market.get("end_date"),
            },
        }
        # Cycle 14: time the DB write (decision + trade + features inserts
        # are all under `_record_execute`). Total chain latency snapshots
        # *after* the write so what lands on the trade row matches the
        # actual end-to-end time including the insert it's annotating.
        _t = time.perf_counter()
        await self._record_execute(
            signal_id=int(signal_id),
            market_id=str(market_id),
            prediction_id=result.prediction_id,
            direction=direction,
            confidence=confidence,
            similarity=float(similarity),
            freshness=freshness,
            p_hat=p_hat,
            mid=mid,
            edge=edge,
            kelly_fraction=kelly_capped,
            size_usdc=size,
            features_snapshot=features_snapshot,
            gate_history=gate_history,
            cycle_id=cycle_id,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
            breakdown=breakdown,
            t_start=t_start,
            db_insert_t_start=_t,
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _decision_exists(self, signal_id: int) -> bool:
        cur = await self._db.conn.execute(
            "SELECT 1 FROM decisions WHERE signal_id=?", (signal_id,)
        )
        row = await cur.fetchone()
        await cur.close()
        return row is not None

    async def _fetch_article(self, article_id: int) -> Optional[dict]:
        # Cycle 24 (handoff H): `body` joins the fetch so the prompt
        # builder can prefer the full trafilatura-cleaned article over
        # the (often 1-sentence) RSS summary. NULL on legacy rows + on
        # any path where the upstream scraper-v2 payload didn't supply
        # a body field — the prompt builder falls back to `summary` in
        # that case.
        cur = await self._db.conn.execute(
            "SELECT title, summary, body FROM articles WHERE id=?",
            (article_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return {
            "title": row["title"],
            "summary": row["summary"],
            "body": row["body"],
        }

    async def _fetch_market(self, market_id: str) -> Optional[dict]:
        # Cycle 21 / PR-B: `fee_rate` (cycle 10) is also fetched so the
        # snapshot the trade row pins under `features_snapshot.market_at_trade`
        # captures the per-market Polymarket taker rate as-of trade time.
        # `markets` is a SHARED mutable table — its rows get refreshed by the
        # markets-stage poller — so the replay harness cannot trust the
        # current row to represent the value at decision time. Pinning the
        # subset on the trade row is the durable record.
        cur = await self._db.conn.execute(
            "SELECT question, description, token_yes, token_no, "
            "       category, end_date, volume_usdc, min_order_size, "
            "       fee_rate, active, closed_at "
            "FROM markets WHERE condition_id=?",
            (market_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return {
            "question": row["question"],
            "description": row["description"],
            "token_yes": row["token_yes"],
            "token_no": row["token_no"],
            "category": row["category"],
            "end_date": row["end_date"],
            "volume_usdc": row["volume_usdc"],
            # Cycle 13: per-market platform floor in SHARES (NOT USDC).
            # The size gate converts to USDC via the executable price for
            # the bet's side. NULL when the row was inserted before the
            # column existed or the CLOB fetch failed — gate then falls
            # back to `cfg.trade.min_position_usdc` only. See
            # plans/order-execution-spreads.md proposal #4.
            "min_order_size": row["min_order_size"],
            # Cycle 10 / pinned by PR-B: per-market Polymarket taker
            # fee_rate. NULL = not-yet-fetched; closer.py + replay both
            # fall back to `cfg.trade.default_fee_rate` (cycle 16). See
            # `markets.py:_maybe_fetch_market_info`.
            "fee_rate": row["fee_rate"],
            # Cycle 23: read by `_gate_market_state` so the trade path can
            # refuse entries on closed / inactive / near-expiry markets.
            "active": row["active"],
            "closed_at": row["closed_at"],
        }

    async def _cluster_size_for_signal(self, signal_id: int) -> int:
        """COUNT(*) on article_cluster_members for the signal's cluster.
        Returns 1 (singleton) when the join produces no match (defensive
        for tests that skip dedupe). Used by the market_news_imbalance
        gate's singleton-on-extreme-mid sub-rule."""
        cur = await self._db.conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM article_cluster_members
            WHERE cluster_id = (SELECT cluster_id FROM signals WHERE id = ?)
            """,
            (signal_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None or row["n"] is None:
            return 1
        return max(1, int(row["n"]))

    # Eastern Time is the operative clock on Polymarket — the frontend and
    # API surface deadlines in ET, and a DST-related platform incident
    # confirmed ET as the canonical zone. So a naïve timestamp from Gamma
    # (no `Z`, no `+HH:MM` suffix) must be interpreted as ET, not UTC.
    # Source: `irene/questions/unstructured answers.md` lines 183-185.
    _ET_ZONE = ZoneInfo("America/New_York")

    # Match a title-string deadline like "by June 30" / "by June 30, 2026".
    # The year is optional — when missing we assume the current ET year.
    # Only fires as a true last resort when Gamma's `end_date` is absent.
    _TITLE_DEADLINE_RE = re.compile(
        r"by\s+"
        r"(?P<month>"
        r"january|february|march|april|may|june|july|"
        r"august|september|october|november|december"
        r")\s+"
        r"(?P<day>\d{1,2})"
        r"(?:,\s*(?P<year>\d{4}))?",
        re.IGNORECASE,
    )
    _MONTHS = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
    }

    @classmethod
    def _parse_market_deadline(cls, market: Optional[dict]) -> Optional[datetime]:
        """Parse a market's deadline into a UTC-aware ``datetime`` or None.

        Resolution order:
          1. Gamma's ``end_date`` field. Explicit-UTC suffixes (``Z``,
             ``+00:00``, ``+0000``) parse as UTC. Other explicit offsets
             are respected verbatim. **Naïve** timestamps are interpreted
             as Eastern Time, then converted to UTC.
          2. Title-string fallback (``by <Month> <Day>[, <Year>]``) →
             that day 23:59:59 ET. Only fires when ``end_date`` is
             missing or unparseable.

        Returns None when neither source yields a usable timestamp; the
        caller's existing ``days is None`` defensives skip the sub-rule.
        """
        if not isinstance(market, dict):
            return None

        end_iso = market.get("end_date")
        if isinstance(end_iso, str) and end_iso:
            parsed = cls._parse_end_date_string(end_iso)
            if parsed is not None:
                return parsed

        for key in ("question", "description"):
            text = market.get(key)
            if isinstance(text, str) and text:
                title_parsed = cls._parse_title_deadline(text)
                if title_parsed is not None:
                    return title_parsed
        return None

    @classmethod
    def _parse_end_date_string(cls, end_date_iso: str) -> Optional[datetime]:
        """Parse an ISO-ish end_date string into UTC.

        - ``...Z`` / ``...+00:00`` / ``...+0000`` → UTC (preserved).
        - Other explicit offsets (``...+02:00``, ``...-05:00``) → respected.
        - Naïve (no tz) → interpreted as America/New_York, converted to UTC.
        """
        s = end_date_iso.strip()
        if not s:
            return None
        # `fromisoformat` accepts `+HHMM` only on Python 3.11+; normalise
        # the bare-Z form which is by far the most common Gamma payload.
        normalised = s
        if normalised.endswith("Z"):
            normalised = normalised[:-1] + "+00:00"
        elif normalised.endswith("+0000"):
            normalised = normalised[:-5] + "+00:00"
        elif normalised.endswith("-0000"):
            normalised = normalised[:-5] + "+00:00"
        try:
            parsed = datetime.fromisoformat(normalised)
        except (TypeError, ValueError):
            return None
        if parsed.tzinfo is None:
            # Naïve → ET (NOT UTC). This is the key behaviour shift.
            parsed = parsed.replace(tzinfo=cls._ET_ZONE)
        return parsed.astimezone(timezone.utc)

    @classmethod
    def _parse_title_deadline(cls, text: str) -> Optional[datetime]:
        """Extract ``by <Month> <Day>[, <Year>]`` → that day 23:59:59 ET.

        End-of-day ET — NOT UTC midnight — because Polymarket's docs say
        the market rules page (not the English phrase alone) is the source
        of truth, and the platform's operative clock is ET. Source:
        ``irene/questions/unstructured answers.md`` lines 183-185.
        """
        match = cls._TITLE_DEADLINE_RE.search(text)
        if not match:
            return None
        month = cls._MONTHS.get(match.group("month").lower())
        if month is None:
            return None
        try:
            day = int(match.group("day"))
        except (TypeError, ValueError):
            return None
        year_str = match.group("year")
        if year_str:
            try:
                year = int(year_str)
            except (TypeError, ValueError):
                return None
        else:
            # No year in the phrase → assume current ET year. Reasonable
            # for live trading; will need a re-eval if we ever backfill
            # historical markets through this path.
            year = datetime.now(cls._ET_ZONE).year
        try:
            naive = datetime.combine(
                datetime(year, month, day).date(),
                dt_time(23, 59, 59),
            )
        except ValueError:
            return None
        et_local = naive.replace(tzinfo=cls._ET_ZONE)
        return et_local.astimezone(timezone.utc)

    @classmethod
    def _days_to(cls, end_date_or_market) -> Optional[int]:
        """Days from now (UTC) to a Polymarket deadline. Negative for past.

        Accepts either a market dict (preferred — enables title-fallback)
        or a raw ``end_date`` string (legacy callers / tests). Returns
        None if the deadline can't be resolved.
        """
        if end_date_or_market is None:
            return None
        if isinstance(end_date_or_market, dict):
            end = cls._parse_market_deadline(end_date_or_market)
        elif isinstance(end_date_or_market, str):
            if not end_date_or_market:
                return None
            end = cls._parse_end_date_string(end_date_or_market)
        else:
            return None
        if end is None:
            return None
        now = datetime.now(timezone.utc)
        return int((end - now).total_seconds() // 86400)

    async def _check_market_news_imbalance(
        self,
        *,
        market: dict,
        mid: float,
        freshness: Optional[str],
        signal_id: int,
    ) -> Optional[tuple[str, object]]:
        """Cycle 7: reject when news weight is too small for the market's
        narrative weight. Returns None (ok) or (sub_rule, detail). Five
        sub-rules — see plan §"Market-weight vs news-weight gate".

        Delegates to the pure helper `_eval_market_news_imbalance_subrules`
        so the replay harness can re-run the same rules without a DB. The
        two pieces of I/O the rules need — the parsed deadline-days and
        sub-rule-5's cluster_size — are resolved here and passed in via
        the market dict + `cluster_size` arg.

        Defensive: when a signal is missing (volume=None/0, end_date=None)
        the helper skips that sub-rule rather than fail-open the whole gate."""
        cfg = self._cfg
        # Pre-compute the deadline-days and stuff it onto a copy of the
        # market dict — the pure helper is sync and can't call the
        # `_parse_market_deadline` chain itself (it depends on ET/UTC
        # handling and lives as a TradeStage classmethod).
        market_with_days = dict(market)
        market_with_days["_days_to_expiry"] = self._days_to(market)
        # Sub-rule 5's cluster_size query only matters when the
        # singleton-on-extreme-mid rule could fire. Query lazily, mirroring
        # the pre-refactor behaviour (only one extra COUNT(*) at most).
        cluster_size = 1
        if cfg.reject_singleton_on_extreme_mid and (mid <= 0.15 or mid >= 0.85):
            cluster_size = await self._cluster_size_for_signal(signal_id)
        return _eval_market_news_imbalance_subrules(
            market=market_with_days, mid=mid, freshness=freshness,
            cluster_size=cluster_size, cfg=cfg,
        )

    def _compose_prompt(self, article: dict, market: dict) -> str:
        # Per `docs/philosophy.md`: irene is not a forecaster. The LLM is
        # asked to read the news as natural language and tell us whether
        # the article DECISIVELY resolves the market, not to estimate the
        # probability of YES. `confidence` here is "how clearly the news
        # resolves it", not P(YES).
        # Implementation lives at module scope (`compose_direction_prompt`)
        # so the DKG path (`app/stages/dkg_trade.py`) can call exactly the
        # same prompt without inheriting state from this class — the LLM
        # cache then stays warm across both paths for the same
        # (article, market) pair.
        return compose_direction_prompt(article, market)

    async def _resolve_mid(
        self, *, direction: str, market: Optional[dict]
    ) -> Optional[float]:
        """Return the mid-price for the side we're betting on, or `None`
        when no real CLOB price is available.

        Cycle 23: removed the silent fallback to `cfg.default_mid_price`.
        Pre-cycle-23 this returned 0.5 on (a) `self._clob is None`,
        (b) missing token, (c) `clob.get_mid()` returning None — the bot
        then placed real BUYs at the stub price (21/39 trades in the audit
        window). `_gate_mid_unavailable` now rejects on `None`. The clob
        WARN-once defense is in `_warn_once_*` below.
        """
        if self._clob is None:
            self._warn_once_clob_missing("_resolve_mid")
            return None
        if market is None:
            return None
        token_id: Optional[str] = (
            market.get("token_yes") if direction == "YES" else market.get("token_no")
        )
        if not token_id:
            return None
        mid = await self._clob.get_mid(str(token_id))
        if mid is None:
            return None
        return float(mid)

    # Cycle 23: per-process one-shot WARN log so silent-fallback regressions
    # are impossible to overlook. Keyed by call-site so each surface logs
    # exactly once per process lifetime.
    _CLOB_WARN_SEEN: set[str] = set()

    def _warn_once_clob_missing(self, site: str) -> None:
        if site in self._CLOB_WARN_SEEN:
            return
        self._CLOB_WARN_SEEN.add(site)
        logger.warning(
            "trade: %s observed self._clob is None — pre-trade gate "
            "will reject with mid_unavailable. This is a SILENT-FALLBACK "
            "regression: investigate build_deps() / build_clob() wiring.",
            site,
        )

    async def _resolve_spread(
        self, *, direction: str, market: Optional[dict]
    ) -> Optional[float]:
        """Return `best_ask - best_bid` for the side we're betting on, or
        `None` if the book read fails (no client, no token, network /
        parse error, empty book). Callers use `None` to mean "can't
        tell" and should NOT reject on that signal alone — the
        degenerate-mid and market-news gates are still in force.
        """
        if self._clob is None or market is None:
            return None
        if not hasattr(self._clob, "get_top_of_book"):
            return None
        token_id: Optional[str] = (
            market.get("token_yes") if direction == "YES" else market.get("token_no")
        )
        if not token_id:
            return None
        book = await self._clob.get_top_of_book(str(token_id))
        if book is None:
            return None
        best_bid, best_ask = book
        return float(best_ask) - float(best_bid)

    async def _resolve_executable_side(
        self, *, direction: str, market: Optional[dict]
    ) -> Optional[tuple[float, float]]:
        """Return ``(executable_price, spread)`` in YES-price units for the
        side we're betting, or ``None`` when the book read fails.

        Edge-math convention. The whole edge gate works in YES-price
        space (mid is the YES mid, p_hat is P(YES)). The executable
        price returned here is the YES-side scalar that slots into
        that formula:

          - YES buy: pay ``best_ask``. ``edge = p_hat - best_ask - costs``.
          - NO buy: NO-side cost is ``1 - best_bid_yes``. The NO buy's
            edge expressed in YES-price terms is
            ``(1 - p_hat) - (1 - best_bid) = best_bid - p_hat``, so the
            executable price to plug into the symmetric formula
            ``edge = executable_price - p_hat - costs`` is ``best_bid``
            (the YES bid). This matches the legacy fallback's "use the
            YES mid for both sides" convention — we're just swapping mid
            for the executable YES-side price.

        Both branches read the YES token's book (``market['token_yes']``)
        because the NO book is the mirror image; reading the YES side
        once keeps the math in one price space and avoids a second
        round-trip on every signal.
        """
        if self._clob is None or market is None:
            return None
        if not hasattr(self._clob, "get_top_of_book"):
            return None
        token_id: Optional[str] = (
            market.get("token_yes") if direction == "YES" else market.get("token_no")
        )
        if not token_id:
            return None
        book = await self._clob.get_top_of_book(str(token_id))
        if book is None:
            return None
        best_bid, best_ask = book
        best_bid_f = float(best_bid)
        best_ask_f = float(best_ask)
        spread = best_ask_f - best_bid_f
        if direction == "YES":
            # For YES we read the YES token's book; executable = best_ask
            # (price we pay to buy YES).
            executable = best_ask_f
        else:
            # For NO we read the NO token's book. Buying NO costs
            # `best_ask` on the NO book. We project that back into the
            # YES-price convention used by the edge formula: NO-cost is
            # `best_ask_no`, which corresponds to a YES-side scalar of
            # `1 - best_ask_no`. Edge of NO buy =
            #     (1 - p_hat) - best_ask_no
            #   = (1 - best_ask_no) - p_hat
            # so `executable = 1 - best_ask_no` fits the symmetric
            # formula `edge = executable - p_hat - costs`.
            executable = 1.0 - best_ask_f
        return executable, spread

    async def _record_reject(
        self,
        *,
        signal_id: int,
        reason: str,
        gate_history: list[dict],
        prediction_id: Optional[int],
        cycle_id: int,
        is_tracer: bool,
        tracer_id: Optional[str],
        decision_latency_ms: Optional[float] = None,
        decision_latency_breakdown: Optional[dict[str, float]] = None,
    ) -> None:
        decision_id = await self._insert_decision(
            signal_id=signal_id,
            outcome="reject",
            reason=reason,
            gate_history=gate_history,
            prediction_id=prediction_id,
            cycle_id=cycle_id,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
            decision_latency_ms=decision_latency_ms,
            decision_latency_breakdown=decision_latency_breakdown,
        )
        if decision_id is None:
            # A prior pass already wrote the decision; don't republish.
            return
        await self._bus.publish(
            TOPIC_TRADE_DECIDED,
            {
                "trade_id": None,
                "decision_id": decision_id,
                "outcome": "reject",
                "reason": reason,
                "is_tracer": is_tracer,
                "tracer_id": tracer_id,
            },
        )

    async def _record_execute(
        self,
        *,
        signal_id: int,
        market_id: str,
        prediction_id: int,
        direction: str,
        confidence: float,
        similarity: float,
        freshness: Optional[str],
        p_hat: float,
        mid: float,
        edge: float,
        kelly_fraction: float,
        size_usdc: float,
        features_snapshot: dict,
        gate_history: list[dict],
        cycle_id: int,
        is_tracer: bool,
        tracer_id: Optional[str],
        breakdown: Optional[dict[str, float]] = None,
        t_start: Optional[float] = None,
        db_insert_t_start: Optional[float] = None,
    ) -> None:
        decision_id = await self._insert_decision(
            signal_id=signal_id,
            outcome="execute",
            reason="ok",
            gate_history=gate_history,
            prediction_id=prediction_id,
            cycle_id=cycle_id,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
        )
        if decision_id is None:
            return
        # Cycle 14: finalise the per-signal latency-chain numbers right
        # before the trade-row INSERT so the values that land on the row
        # genuinely cover handle()-entry → trades-write. `db_insert` here
        # is the cost of `_insert_decision` (and is replaced by the full
        # _insert_trade cost on subsequent passes — see _insert_trade).
        if breakdown is not None and db_insert_t_start is not None:
            breakdown["db_insert"] = (time.perf_counter() - db_insert_t_start) * 1000.0
        decision_latency_ms = (
            (time.perf_counter() - t_start) * 1000.0 if t_start is not None else None
        )
        trade_id = await self._insert_trade(
            signal_id=signal_id,
            decision_id=decision_id,
            prediction_id=prediction_id,
            market_id=market_id,
            direction=direction,
            confidence=confidence,
            similarity=similarity,
            freshness=freshness,
            p_hat=p_hat,
            mid=mid,
            edge=edge,
            kelly_fraction=kelly_fraction,
            size_usdc=size_usdc,
            features_snapshot=features_snapshot,
            cycle_id=cycle_id,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
            decision_latency_ms=decision_latency_ms,
            decision_latency_breakdown=breakdown,
        )
        if trade_id is None:
            return
        await self._insert_trade_features(
            trade_id=trade_id,
            features={
                "similarity": ("numeric", similarity, None),
                "freshness": ("category", None, freshness),
                "confidence": ("numeric", confidence, None),
                "p_hat": ("numeric", p_hat, None),
                "mid": ("numeric", mid, None),
                "edge": ("numeric", edge, None),
                "kelly_fraction": ("numeric", kelly_fraction, None),
                "size_usdc": ("numeric", size_usdc, None),
            },
        )
        await self._bus.publish(
            TOPIC_TRADE_DECIDED,
            {
                "trade_id": trade_id,
                "decision_id": decision_id,
                "outcome": "execute",
                "reason": "ok",
                "is_tracer": is_tracer,
                "tracer_id": tracer_id,
            },
        )

    async def _insert_decision(
        self,
        *,
        signal_id: int,
        outcome: str,
        reason: str,
        gate_history: list[dict],
        prediction_id: Optional[int],
        cycle_id: int,
        is_tracer: bool,
        tracer_id: Optional[str],
        decision_latency_ms: Optional[float] = None,
        decision_latency_breakdown: Optional[dict[str, float]] = None,
    ) -> Optional[int]:
        # Cycle 16: serialise the partial breakdown for reject rows. The
        # dict only contains the steps that ACTUALLY RAN before the
        # rejecting gate fired (e.g. a `similarity_floor` reject yields
        # an empty dict; an `edge_below_min` reject yields fetch_article,
        # fetch_market, ai_direction, resolve_mid, resolve_book). Execute
        # decisions pass None on both kwargs — the canonical chain numbers
        # live on the `trades` row in that case.
        breakdown_json = (
            json.dumps(decision_latency_breakdown)
            if decision_latency_breakdown is not None
            else None
        )
        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT OR IGNORE INTO decisions
                    (signal_id, outcome, reason, gate_history, prediction_id,
                     cycle_id, is_tracer, tracer_id, created_at,
                     decision_latency_ms, decision_latency_breakdown)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_id,
                    outcome,
                    reason,
                    json.dumps(gate_history),
                    prediction_id,
                    cycle_id,
                    1 if is_tracer else 0,
                    tracer_id,
                    _now_iso(),
                    decision_latency_ms,
                    breakdown_json,
                ),
            )
            if cur.rowcount != 1:
                return None
            return cur.lastrowid

    async def _insert_trade(
        self,
        *,
        signal_id: int,
        decision_id: int,
        prediction_id: int,
        market_id: str,
        direction: str,
        confidence: float,
        similarity: float,
        freshness: Optional[str],
        p_hat: float,
        mid: float,
        edge: float,
        kelly_fraction: float,
        size_usdc: float,
        features_snapshot: dict,
        cycle_id: int,
        is_tracer: bool,
        tracer_id: Optional[str],
        decision_latency_ms: Optional[float] = None,
        decision_latency_breakdown: Optional[dict[str, float]] = None,
    ) -> Optional[int]:
        # v6: derive the target_mid where the model's edge collapses.
        # `p_hat` is the model's implied P(YES). The target is always
        # expressed in YES-mid terms (Polymarket's primary price).
        # For YES we bought low expecting YES to rise toward p_hat —
        # sell just BELOW p_hat (-fee_buffer) to lock profit before fees.
        # For NO we bought expecting YES to fall toward p_hat — sell just
        # ABOVE p_hat (+fee_buffer) by the same logic. (Cycle 3 had a
        # sign bug here: target was 1-p_hat+fee, double-flipping and
        # firing edge_decay on every NO trade's first poll.)
        fee_buffer = float(self._cfg.fee_buffer)
        if direction == "YES":
            target_mid = max(0.0, min(1.0, float(p_hat) - fee_buffer))
        else:  # NO
            target_mid = max(0.0, min(1.0, float(p_hat) + fee_buffer))
        # Cycle 14: serialise the per-step breakdown to JSON. NULL when
        # the caller didn't pass one (defensive — non-handle() callers
        # of _insert_trade simply omit the kwarg and legacy rows stay
        # NULL on both new columns).
        breakdown_json = (
            json.dumps(decision_latency_breakdown)
            if decision_latency_breakdown is not None
            else None
        )
        # Cycle 23: post-condition. After Phase 1 the mid_unavailable and
        # mid_degenerate gates should make a degenerate mid unreachable
        # here; assert so any regression is loud, never a silent bad row.
        assert mid is not None and 0.0 < float(mid) < 1.0, (
            f"_insert_trade refusing degenerate mid: market={market_id} "
            f"direction={direction} mid={mid!r}"
        )
        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT OR IGNORE INTO trades
                    (signal_id, decision_id, prediction_id, market_id, direction,
                     confidence, similarity, freshness, p_hat, mid_price, edge,
                     kelly_fraction, size_usdc, decision_path, features_snapshot,
                     cycle_id, is_tracer, tracer_id, decided_at, target_mid,
                     decision_latency_ms, decision_latency_breakdown)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'mvp', ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_id,
                    decision_id,
                    prediction_id,
                    market_id,
                    direction,
                    confidence,
                    similarity,
                    freshness,
                    p_hat,
                    mid,
                    edge,
                    kelly_fraction,
                    size_usdc,
                    json.dumps(features_snapshot),
                    cycle_id,
                    1 if is_tracer else 0,
                    tracer_id,
                    _now_iso(),
                    target_mid,
                    decision_latency_ms,
                    breakdown_json,
                ),
            )
            if cur.rowcount != 1:
                return None
            return cur.lastrowid

    async def _insert_trade_features(
        self,
        *,
        trade_id: int,
        features: dict[str, tuple[str, Optional[float], Optional[str]]],
    ) -> None:
        async with self._db.batch() as cur:
            for name, (kind, value, label) in features.items():
                await cur.execute(
                    """
                    INSERT OR IGNORE INTO trade_features
                        (trade_id, feature_name, feature_value, feature_kind, feature_label)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (trade_id, name, value, kind, label),
                )

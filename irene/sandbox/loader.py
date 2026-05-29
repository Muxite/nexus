"""Pull explorer rows + joined article/market context out of irene.db.

Read-only. One SQL per call; rows materialised into the SandboxRow
dataclass so callers don't depend on sqlite3.Row column names.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from typing import Iterable, Optional

from irene.sandbox.paths import DB_PATH


def _yes_payout(last_outcome_prices) -> Optional[float]:
    """Resolved YES-side payout from a `markets.last_outcome_prices`
    payload (~1.0 = YES won, ~0.0 = NO won), or None if unresolved /
    malformed / not a clean binary market. Mirrors the validation in
    `app/stages/markets.py::_state3_exit_mid` (kept local so the sandbox
    stays import-light); keep the two in sync if the Gamma convention
    changes. `outcomePrices` is a stringified 2-element [YES, NO] list.
    """
    if last_outcome_prices is None:
        return None
    if isinstance(last_outcome_prices, str):
        s = last_outcome_prices.strip()
        if not s.startswith("["):
            return None
        try:
            parsed = json.loads(s)
        except (json.JSONDecodeError, ValueError):
            return None
    else:
        parsed = last_outcome_prices
    if not isinstance(parsed, list) or len(parsed) < 2:
        return None
    try:
        yes_payout = float(parsed[0])
        no_payout = float(parsed[1])
    except (TypeError, ValueError):
        return None
    if not (0.0 <= yes_payout <= 1.0 and 0.0 <= no_payout <= 1.0):
        return None
    if abs((yes_payout + no_payout) - 1.0) > 1e-6:
        return None
    return yes_payout


def connect_readonly(db_path: str = DB_PATH) -> sqlite3.Connection:
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


@dataclass(frozen=True)
class SandboxRow:
    exploration_id: int
    t0_iso: str
    cycle_name: str
    capture_status: str
    article_id: int
    article_title: str
    article_body: Optional[str]
    article_summary: Optional[str]
    article_source: Optional[str]
    article_url: Optional[str]
    article_tier: Optional[str]
    article_breaking_score: Optional[float]
    market_condition_id: str
    market_question: str
    market_description: Optional[str]
    market_end_date: Optional[str]
    market_volume_24h: Optional[float]
    market_token_yes: Optional[str]
    market_token_no: Optional[str]
    market_mid_t0: Optional[float]
    market_spread_t0: Optional[float]
    hours_to_expiry: Optional[float]
    orig_llm_related: int
    orig_llm_confidence: Optional[float]
    orig_llm_reason: Optional[str]
    orig_freshness: Optional[str]
    orig_cosine_similarity: Optional[float]
    orig_dkg_final_score: Optional[float]
    # Explorer-v2 gate fields. `gate_status` is always populated; legacy
    # rows (before the migration) default to 'passed'. `gate_history` is
    # a JSON list[dict] when the v2 pipeline ran, NULL on legacy rows.
    gate_status: str
    rejected_by_gate: Optional[str]
    gate_history: Optional[str]
    # Explorer-v3 sandbox-feedback fields. All NULL on pre-migration
    # rows. `gate_history` of v2-prompt rows additionally contains
    # per-candidate `metadata.features` ({entities_overlap,
    # claim_horizon_hours, directional_lean, novelty, short_reason}) —
    # extract with `json_extract(gate_history, '$[1].metadata.features.*')`.
    capture_end_reason: Optional[str]
    cluster_id: Optional[str]
    capture_sample_reason: Optional[str]
    # Resolution outcome (joined from `markets.last_outcome_prices`).
    # `resolved_yes_payout` ~1.0 = YES won, ~0.0 = NO won, None = not yet
    # resolved / malformed. Lets a strategy be graded for correctness
    # ("did we get it right") against ground truth, including for
    # otherwise-untradeable rows that never had a live ask.
    market_last_outcome_prices: Optional[str]
    resolved_yes_payout: Optional[float]


_BASE_SELECT = """
SELECT
  e.id            AS exploration_id,
  e.t0_iso, e.capture_status,
  c.name          AS cycle_name,
  e.article_id,
  a.title         AS article_title,
  a.body          AS article_body,
  a.summary       AS article_summary,
  a.source        AS article_source,
  a.url           AS article_url,
  a.tier          AS article_tier,
  a.breaking_score AS article_breaking_score,
  e.market_condition_id,
  m.question      AS market_question,
  m.description   AS market_description,
  m.end_date      AS market_end_date,
  m.volume_usdc   AS market_volume_24h,
  e.token_yes     AS market_token_yes,
  e.token_no      AS market_token_no,
  e.market_mid_t0, e.market_spread_t0, e.hours_to_expiry,
  e.llm_related       AS orig_llm_related,
  e.llm_confidence    AS orig_llm_confidence,
  e.llm_reason        AS orig_llm_reason,
  e.freshness         AS orig_freshness,
  e.cosine_similarity AS orig_cosine_similarity,
  e.dkg_final_score   AS orig_dkg_final_score,
  e.gate_status,
  e.rejected_by_gate,
  e.gate_history,
  e.capture_end_reason,
  e.cluster_id,
  e.capture_sample_reason,
  m.last_outcome_prices AS market_last_outcome_prices
FROM explorations e
LEFT JOIN articles a ON a.id           = e.article_id
LEFT JOIN markets  m ON m.condition_id = e.market_condition_id
LEFT JOIN cycles   c ON c.id           = e.cycle_id
"""

# Same column list under e.* / a.* / m.* — used by make_set.py to
# whitelist filter expressions. Keep in sync with _BASE_SELECT.
FILTER_COLUMNS = {
    "e": [
        "id", "article_id", "event_id", "market_condition_id",
        "t0_iso", "capture_status", "cycle_id", "is_tracer", "tracer_id",
        "llm_related", "llm_confidence", "freshness", "cosine_similarity",
        "dkg_final_score", "breaking_score", "article_tier",
        "market_volume_24h", "market_spread_t0", "market_mid_t0",
        "hours_to_expiry",
        "gate_status", "rejected_by_gate", "llm_prompt_hash",
        "capture_end_reason", "cluster_id", "capture_sample_reason",
    ],
    "a": ["id", "title", "source", "url", "tier", "breaking_score",
          "published_at", "ingested_at"],
    "m": ["condition_id", "slug", "question", "category", "end_date",
          "volume_usdc", "active", "last_outcome_prices"],
    "c": ["id", "name", "opened_at", "closed_at"],
}


def load_rows(
    con: sqlite3.Connection,
    exploration_ids: Iterable[int],
) -> list[SandboxRow]:
    ids = list(exploration_ids)
    if not ids:
        return []
    placeholders = ",".join("?" * len(ids))
    sql = _BASE_SELECT + f" WHERE e.id IN ({placeholders}) ORDER BY e.t0_iso ASC"
    return [_row_from_db(r) for r in con.execute(sql, ids)]


def query_rows(
    con: sqlite3.Connection,
    where_sql: str,
    params: Iterable = (),
    *,
    tradeable_only: bool = False,
) -> list[SandboxRow]:
    """Run a free-form WHERE fragment against the joined view. Used by
    make_set.py to materialise an initial set from a filter. Caller is
    responsible for validating column names against FILTER_COLUMNS.

    `tradeable_only=True` AND-conjoins a TRADEABLE_EXISTS subquery so
    only rows with at least one non-NULL post-news best_ask survive —
    the rest can't be priced for entry and are noise in any backtest.
    """
    where = where_sql
    if tradeable_only:
        where = f"({where}) AND {TRADEABLE_EXISTS}"
    sql = _BASE_SELECT + f" WHERE {where} ORDER BY e.t0_iso ASC"
    return [_row_from_db(r) for r in con.execute(sql, tuple(params))]


# Subquery used by `tradeable_only`. Refers to columns from
# odds_observations, NOT in FILTER_COLUMNS — fixed string, not user input,
# so the make_set whitelist validator never sees it.
TRADEABLE_EXISTS = (
    "EXISTS ("
    "SELECT 1 FROM odds_observations o "
    "WHERE o.exploration_id = e.id "
    "AND o.source = 'live' "
    "AND o.best_ask IS NOT NULL "
    "LIMIT 1)"
)


def _row_from_db(r: sqlite3.Row) -> SandboxRow:
    return SandboxRow(
        exploration_id=int(r["exploration_id"]),
        t0_iso=str(r["t0_iso"]),
        cycle_name=str(r["cycle_name"] or ""),
        capture_status=str(r["capture_status"]),
        article_id=int(r["article_id"]),
        article_title=str(r["article_title"] or ""),
        article_body=r["article_body"],
        article_summary=r["article_summary"],
        article_source=r["article_source"],
        article_url=r["article_url"],
        article_tier=r["article_tier"],
        article_breaking_score=_f(r["article_breaking_score"]),
        market_condition_id=str(r["market_condition_id"]),
        market_question=str(r["market_question"] or ""),
        market_description=r["market_description"],
        market_end_date=r["market_end_date"],
        market_volume_24h=_f(r["market_volume_24h"]),
        market_token_yes=r["market_token_yes"],
        market_token_no=r["market_token_no"],
        market_mid_t0=_f(r["market_mid_t0"]),
        market_spread_t0=_f(r["market_spread_t0"]),
        hours_to_expiry=_f(r["hours_to_expiry"]),
        orig_llm_related=int(r["orig_llm_related"] or 0),
        orig_llm_confidence=_f(r["orig_llm_confidence"]),
        orig_llm_reason=r["orig_llm_reason"],
        orig_freshness=r["orig_freshness"],
        orig_cosine_similarity=_f(r["orig_cosine_similarity"]),
        orig_dkg_final_score=_f(r["orig_dkg_final_score"]),
        gate_status=str(r["gate_status"] or "passed"),
        rejected_by_gate=r["rejected_by_gate"],
        gate_history=r["gate_history"],
        capture_end_reason=r["capture_end_reason"],
        cluster_id=(
            str(r["cluster_id"]) if r["cluster_id"] is not None else None
        ),
        capture_sample_reason=r["capture_sample_reason"],
        market_last_outcome_prices=r["market_last_outcome_prices"],
        resolved_yes_payout=_yes_payout(r["market_last_outcome_prices"]),
    )


def _f(v) -> Optional[float]:
    return float(v) if v is not None else None


def row_to_dict(row: SandboxRow) -> dict:
    return asdict(row)

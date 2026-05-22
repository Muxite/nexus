"""
Seulgi database layer — thin wrapper around SQLite via aiosqlite.

Schema:
  markets   - Polymarket markets with embeddings stored as raw bytes
  signals   - Detected headline changes from watched sites
  trades    - Every trade decision (executed or skipped), including close results
  positions - Currently open paper positions (deleted on close)

Embeddings stored as raw float32 bytes (numpy tobytes/frombuffer).

Schema migrations are applied at connect() time. New columns are added
idempotently — each ALTER TABLE is attempted and the error silently ignored
if the column already exists (SQLite's only safe migration pattern).
"""

from __future__ import annotations

import asyncio
import contextvars
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiosqlite
import numpy as np

# Reentrancy flag for the write-lock: set True inside a batch() so that
# nested write calls (insert_signal, insert_trade, etc.) running in the
# SAME task skip the lock acquire (they're already serialized by the
# enclosing batch). Other tasks see False and must acquire the lock.
# contextvars is per-task — asyncio.create_task / gather copies the
# context so child coroutines inherit the flag.
_in_batch_var: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "seulgi_db_in_batch", default=False,
)

# Phase-0 observability: write-path counters bump into metrics + logs.
# Import lazily/defensively because db.py is imported by scripts that
# may not have the metrics module on their path (analysis CLIs).
try:
    import metrics as _metrics
except Exception:  # noqa: BLE001
    _metrics = None

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS engine_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    engine_name     TEXT NOT NULL,
    condition_id    TEXT NOT NULL,
    question        TEXT NOT NULL DEFAULT '',
    direction       TEXT NOT NULL,
    confidence      REAL NOT NULL,
    similarity      REAL NOT NULL,
    p_hat           REAL NOT NULL,
    mid_price       REAL,
    edge            REAL NOT NULL DEFAULT 0.0,
    size_usdc       REAL NOT NULL,
    executed        INTEGER NOT NULL DEFAULT 0,  -- 1=executed, 0=skipped
    skip_reason     TEXT,
    decided_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS markets (
    condition_id    TEXT PRIMARY KEY,
    question        TEXT NOT NULL,
    token_yes       TEXT,
    token_no        TEXT,
    end_date        TEXT,
    volume_usdc     REAL,
    embedding       BLOB,          -- float32 bytes, shape (384,)
    fetched_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS signals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    site_url        TEXT NOT NULL,
    headline        TEXT NOT NULL,
    detected_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS trades (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id       INTEGER NOT NULL REFERENCES signals(id),
    condition_id    TEXT NOT NULL,
    question        TEXT NOT NULL DEFAULT '',
    token_id        TEXT,
    direction       TEXT NOT NULL,
    similarity      REAL NOT NULL,
    p_hat           REAL NOT NULL,
    mid_price       REAL,
    size_usdc       REAL NOT NULL,
    skip_reason     TEXT,           -- NULL = executed
    decided_at      TEXT NOT NULL,
    realized_pnl    REAL,           -- filled on close; NULL if still open / skipped
    closed_at       TEXT            -- ISO timestamp when position closed
);

CREATE TABLE IF NOT EXISTS positions (
    trade_id        INTEGER PRIMARY KEY REFERENCES trades(id),
    condition_id    TEXT NOT NULL,
    question        TEXT NOT NULL DEFAULT '',
    token_id        TEXT NOT NULL,
    direction       TEXT NOT NULL,
    size_usdc       REAL NOT NULL,
    entry_price     REAL NOT NULL,
    opened_at       TEXT NOT NULL,
    last_price      REAL,
    unrealized_pnl  REAL DEFAULT 0.0,
    p_hat           REAL
);
"""

# Idempotent migrations for columns added after initial schema
_PRICE_IMPACT_SCHEMA = """
CREATE TABLE IF NOT EXISTS price_impacts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    headline_id     TEXT NOT NULL,
    headline        TEXT NOT NULL,
    site_url        TEXT NOT NULL,
    condition_id    TEXT NOT NULL,
    question        TEXT NOT NULL,
    token_id        TEXT NOT NULL,
    direction       TEXT NOT NULL,      -- YES or NO (which token we expect to move)
    ai_confidence   REAL NOT NULL,
    similarity      REAL NOT NULL,
    price_t0        REAL,               -- price at headline detection time
    price_t1m       REAL,               -- price 1 minute later
    price_t5m       REAL,               -- price 5 minutes later
    price_t15m      REAL,               -- 15 minutes later
    price_t30m      REAL,               -- 30 minutes later
    price_t60m      REAL,               -- 60 minutes later
    delta_t1m       REAL,               -- price_t1m  - price_t0
    delta_t5m       REAL,               -- price_t5m  - price_t0
    delta_t15m      REAL,
    delta_t30m      REAL,
    delta_t60m      REAL,
    detected_at     TEXT NOT NULL,      -- ISO timestamp of headline
    source          TEXT DEFAULT 'news' -- 'news' | 'twitter' | 'truth_social'
);

CREATE INDEX IF NOT EXISTS idx_pi_headline_id ON price_impacts(headline_id);
CREATE INDEX IF NOT EXISTS idx_pi_condition_id ON price_impacts(condition_id);
CREATE INDEX IF NOT EXISTS idx_pi_detected_at ON price_impacts(detected_at);
"""

# FIX-3C: Phase-5 historical-simulation data foundation. Three append-only
# capture tables that let a future replay harness re-run trader.evaluate
# with new configs without re-paying LLM tokens. Schema is shipped now so
# Phase-2 wiring (engine.py / ai.py) doesn't get blocked on migration
# coordination. Writers exist below as `insert_decision_input`,
# `log_llm_call`, `log_llm_parse_failure` — fire-and-forget per spec.
_HIST_SIM_SCHEMA = """
CREATE TABLE IF NOT EXISTS decision_inputs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    headline_id TEXT NOT NULL,
    decided_at TEXT NOT NULL,
    source_host TEXT,
    embedding_bytes BLOB,
    matches_json TEXT,          -- list of {cond_id, question, similarity, mid_yes, mid_no, relevance_score}
    freshness_label TEXT,
    freshness_dist REAL,
    freshness_age_secs REAL,
    pre_ai_skips_json TEXT,
    max_similarity REAL
);
CREATE INDEX IF NOT EXISTS idx_decision_inputs_headline ON decision_inputs(headline_id);
CREATE INDEX IF NOT EXISTS idx_decision_inputs_decided ON decision_inputs(decided_at);

CREATE TABLE IF NOT EXISTS llm_calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    model TEXT,
    prompt_sha TEXT NOT NULL,
    prompt_tokens INTEGER,
    completion_tokens INTEGER,
    response_text TEXT,         -- capped at 8KB by caller
    parse_ok INTEGER NOT NULL,  -- 0/1
    headline_id TEXT,
    batch_size INTEGER
);
CREATE INDEX IF NOT EXISTS idx_llm_calls_prompt_sha ON llm_calls(prompt_sha);
CREATE INDEX IF NOT EXISTS idx_llm_calls_ts ON llm_calls(ts);

CREATE TABLE IF NOT EXISTS llm_parse_failures (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    model TEXT,
    error_msg TEXT,
    response_text TEXT,         -- capped at 4KB by caller
    headline_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_llm_parse_failures_ts ON llm_parse_failures(ts);
"""

_MIGRATIONS = [
    "ALTER TABLE trades    ADD COLUMN question     TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE trades    ADD COLUMN realized_pnl REAL",
    "ALTER TABLE trades    ADD COLUMN closed_at    TEXT",
    "ALTER TABLE positions ADD COLUMN question     TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE positions ADD COLUMN unrealized_pnl REAL DEFAULT 0.0",
    "ALTER TABLE signals        ADD COLUMN headline_id TEXT",
    "ALTER TABLE trades         ADD COLUMN headline_id TEXT",
    "ALTER TABLE engine_results ADD COLUMN headline_id TEXT",
    # market heat tracking
    "ALTER TABLE markets ADD COLUMN match_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE markets ADD COLUMN last_matched_at TEXT",
    # signal latency
    "ALTER TABLE engine_results ADD COLUMN signal_latency_ms REAL",
    # per-engine position isolation
    "ALTER TABLE positions ADD COLUMN engine_name TEXT NOT NULL DEFAULT 'default'",
    # Why a trade closed (WIN/LOSS/TP/SL/TRAIL/TIMEOUT/COLD_CULL). The reason
    # is computed by the trader at close time; without persisting it the
    # close_reasons dashboard cannot distinguish micro-exits (TP/SL/TRAIL)
    # from rail closes (WIN/LOSS) — they all just look like "trade closed".
    # NULL on legacy rows; the plotter buckets them as UNKNOWN.
    "ALTER TABLE trades ADD COLUMN close_reason TEXT",
    # Coarse classification of the market question itself, written by the
    # offline tagger (seulgi/scripts/postmortem/tag_market_types.py).
    # Categories: price_threshold | crypto_price | sports | weather |
    # political | entertainment | event | other. Drives the market_type
    # slice in the post-mortem and (in a later cycle) the rules-engine gate.
    "ALTER TABLE markets ADD COLUMN market_type TEXT",
    "CREATE INDEX IF NOT EXISTS idx_markets_market_type ON markets(market_type)",
    # High-water mark of the entered-side price since the position opened.
    # Drives the trailing-stop exit: once price drops by trailing_stop from
    # this value, close. NULL on legacy rows; trader treats NULL as "use the
    # current price as the initial high-water mark."
    "ALTER TABLE positions ADD COLUMN high_water_price REAL",
    # closed-market tracking — set when Gamma drops the market from its open list,
    # or when end_date passes. Filtering by `closed_at IS NULL` excludes stale markets.
    "ALTER TABLE markets ADD COLUMN closed_at TEXT",
    "CREATE INDEX IF NOT EXISTS idx_markets_closed_at ON markets(closed_at)",
    # Pre-trade price lookback: how much did the market already move in the
    # window just BEFORE we detected the headline? Big |pre_move| in the
    # predicted direction = alpha was real but we were too slow (latency
    # problem). Zero pre_move + zero post_move = headline had no signal.
    # These are recorded for diagnostics; they MUST NOT be counted as profit.
    "ALTER TABLE price_impacts ADD COLUMN price_pre_60s  REAL",
    "ALTER TABLE price_impacts ADD COLUMN price_pre_300s REAL",
    "ALTER TABLE price_impacts ADD COLUMN delta_pre_60s  REAL",
    "ALTER TABLE price_impacts ADD COLUMN delta_pre_300s REAL",
    # Per-impact full time-series backfill flag. NULL = not yet backfilled.
    # A sweep loop picks rows where this is NULL and detected_at + 1h has passed,
    # fetches the full -1h..+1h CLOB history in a single call, and writes minute
    # samples to price_timeseries below.
    "ALTER TABLE price_impacts ADD COLUMN timeseries_backfilled_at TEXT",
    # Full minute-fidelity price samples around each headline. Lets future
    # scripts compute PnL at any horizon and infer required reaction speed
    # per market. NOT used for live trading — diagnostic only.
    """CREATE TABLE IF NOT EXISTS price_timeseries (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        impact_id    INTEGER NOT NULL,
        headline_id  TEXT NOT NULL,
        condition_id TEXT NOT NULL,
        token_id     TEXT NOT NULL,
        t_unix       INTEGER NOT NULL,
        price        REAL NOT NULL,
        relative_s   INTEGER NOT NULL,   -- t_unix - detected_unix (negative = before)
        UNIQUE(impact_id, t_unix)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_pts_impact_id  ON price_timeseries(impact_id)",
    "CREATE INDEX IF NOT EXISTS idx_pts_headline_id ON price_timeseries(headline_id)",
    "CREATE INDEX IF NOT EXISTS idx_pts_t_unix      ON price_timeseries(t_unix)",
    # Article bodies / first paragraphs. Keyed by headline_id so headlines
    # that never reach the trader still get recorded. Stored independently
    # of signals (which require a market match).
    """CREATE TABLE IF NOT EXISTS headline_articles (
        headline_id TEXT PRIMARY KEY,
        article_url TEXT NOT NULL,
        site_url    TEXT,
        body        TEXT,
        body_chars  INTEGER,
        fetched_at  TEXT NOT NULL,
        fetch_error TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_ha_fetched_at ON headline_articles(fetched_at)",
    # External usage logging (OpenRouter tokens + Brave Search calls).
    # Append-only event log; aggregate via SUM() at read time.
    """CREATE TABLE IF NOT EXISTS usage_events (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        ts          TEXT NOT NULL,
        kind        TEXT NOT NULL,   -- 'ai_tokens' | 'brave_search'
        provider    TEXT,            -- e.g. 'google/gemini-2.5-flash-lite' or 'brave'
        count       REAL NOT NULL,   -- token count or 1.0 per Brave call
        meta        TEXT             -- optional JSON (engine_name, hid, etc.)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_usage_ts   ON usage_events(ts)",
    "CREATE INDEX IF NOT EXISTS idx_usage_kind ON usage_events(kind)",
    # Continuous-reinforcement lessons table. Filled by the lessons aggregator
    # (app/lessons.py) every N minutes from classified price_impacts. Active
    # rows are injected into the AI batch prompt and (optionally) consulted by
    # the trader for soft gates. rule_key is the canonical (source × market_type
    # × conf_bucket) tuple as a string so we can UPSERT cheaply.
    """CREATE TABLE IF NOT EXISTS lessons (
        rule_key      TEXT PRIMARY KEY,
        source        TEXT,
        market_type   TEXT,
        conf_bucket   TEXT,
        support_n     INTEGER NOT NULL DEFAULT 0,
        captured_n    INTEGER NOT NULL DEFAULT 0,
        no_signal_n   INTEGER NOT NULL DEFAULT 0,
        wrong_dir_n   INTEGER NOT NULL DEFAULT 0,
        ic            REAL,
        capture_rate  REAL,
        action        TEXT,            -- 'note' | 'downweight' | 'skip' | 'invert'
        note          TEXT,            -- one-line lesson for the AI prompt
        seeded        INTEGER DEFAULT 0,  -- 1 if seeded from postmortem (no empirical support)
        active        INTEGER NOT NULL DEFAULT 0,
        updated_at    TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_lessons_active ON lessons(active)",
    "CREATE INDEX IF NOT EXISTS idx_lessons_action ON lessons(action)",
    # Per-watcher health: persist content hash + churn so the watcher can
    # short-circuit identical fetches and adapt its polling interval across
    # restarts. Filled by SiteWatcher; read by the dashboard.
    """CREATE TABLE IF NOT EXISTS watcher_health (
        url                  TEXT PRIMARY KEY,
        last_fetch_at        TEXT,
        last_success_at      TEXT,
        last_failure_at      TEXT,
        last_content_hash    TEXT,
        last_headline_count  INTEGER DEFAULT 0,
        total_fetches        INTEGER NOT NULL DEFAULT 0,
        total_successes      INTEGER NOT NULL DEFAULT 0,
        total_new_headlines  INTEGER NOT NULL DEFAULT 0,
        ema_latency_ms       REAL,
        current_interval_s   REAL,
        stability_score      REAL
    )""",
    # Cycle-4 analytics shadow fields: log the AI's raw confidence alongside
    # the production p_hat/edge so the next postmortem can build a real
    # calibration curve. NULL on legacy rows; new rows always populate.
    "ALTER TABLE trades ADD COLUMN confidence_raw REAL",
    "ALTER TABLE trades ADD COLUMN p_hat_if_raw REAL",
    "ALTER TABLE trades ADD COLUMN signed_edge REAL",
    "ALTER TABLE trades ADD COLUMN signed_edge_if_raw REAL",
    # Cycle-5 fix: tag each trade with the engine that made it. Without this
    # column the per-engine PnL restore on startup uses a broken JOIN
    # (engine_results.headline_id = trades.headline_id) and silently returns
    # zero for every engine, so the dashboard says ROI=0% after every
    # container restart even when the bot has been profitable. Backfilled on
    # startup via (condition_id, decided_at) proximity to engine_results.
    "ALTER TABLE trades ADD COLUMN engine_name TEXT",
    "CREATE INDEX IF NOT EXISTS idx_trades_engine_name ON trades(engine_name)",
    # Cycle-6 ensemble: fraction of N parallel classifier samples that voted
    # for the winning direction (1.0 = unanimous, 1/N = no signal). Lets the
    # postmortem isolate "high-agreement-but-still-wrong" pockets — exactly
    # the slice where a hard skip lesson is justified.
    "ALTER TABLE trades ADD COLUMN agreement REAL",
    # Cycle-7 (2D measurement): model emits a probability the chosen-direction
    # trade is +EV (`p_alpha`) and an expected variance magnitude (`risk_score`,
    # on a 0..1 scale). p_alpha replaces `confidence` as the input to the edge
    # formula. Both columns logged on every executed trade so the postmortem
    # can plot realized hit-rate vs predicted p_alpha (calibration check) and
    # realized |Δ| vs predicted risk (risk calibration).
    "ALTER TABLE trades ADD COLUMN p_alpha REAL",
    "ALTER TABLE trades ADD COLUMN risk_score REAL",
    # Cycle-8 (cycle scoping): persistent record of every cycle the bot has
    # ever run. `started_at` is the moment cycle.py cycle <name> was invoked;
    # `ended_at` is when the *next* cycle started (so the active cycle has
    # ended_at IS NULL). All "current cycle" reads filter by
    # `decided_at >= started_at`. NO data is deleted across cycles — only
    # the dashboard / plots / engine-PnL restore narrows its window.
    """CREATE TABLE IF NOT EXISTS cycles (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        name        TEXT NOT NULL,
        started_at  TEXT NOT NULL,
        ended_at    TEXT,
        notes       TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_cycles_started_at ON cycles(started_at)",
    # Per-trade reasoning capture. The AI's one-line `reason`, the truncated
    # raw JSON it emitted, and the runtime context that produced the call
    # (source, republish flag, lessons injected, model+samples+votes). The
    # trader's step-by-step guard trace is JSON in `trader_trace`. Both mids
    # at decision time so post-hoc analysis can re-derive edge against the
    # *other* side. `inverted` records whether invert_high_conf flipped the
    # direction. `variant_name` is per-variant strategy tag — falls back to
    # engine_name when no variants are defined.
    "ALTER TABLE trades ADD COLUMN ai_reason       TEXT",
    "ALTER TABLE trades ADD COLUMN ai_raw_response TEXT",
    "ALTER TABLE trades ADD COLUMN ai_prompt_meta  TEXT",
    "ALTER TABLE trades ADD COLUMN trader_trace    TEXT",
    "ALTER TABLE trades ADD COLUMN mid_yes         REAL",
    "ALTER TABLE trades ADD COLUMN mid_no          REAL",
    "ALTER TABLE trades ADD COLUMN inverted        INTEGER",
    "ALTER TABLE trades ADD COLUMN variant_name    TEXT",
    "CREATE INDEX IF NOT EXISTS idx_trades_variant_name ON trades(variant_name)",
    "CREATE INDEX IF NOT EXISTS idx_trades_closed_at    ON trades(closed_at)",
    "CREATE INDEX IF NOT EXISTS idx_trades_headline_id  ON trades(headline_id)",
    # Cycle 9 — 3-stage pipeline (freshness + relevance):
    # `freshness_label` = BREAKING / EVOLVING / REHASH / STALE_RETURN (set on
    # both signals and trades so post-hoc joins are cheap). `relevance_score`
    # = STRONG_YES / YES / WEAK / NO from Stage-2 gate; relevance_reason is
    # the one-line LLM explanation. `nearest_age_secs` (signals only) is
    # how long ago the closest prior headline landed in news_memory.
    "ALTER TABLE signals ADD COLUMN freshness_label   TEXT",
    "ALTER TABLE signals ADD COLUMN nearest_age_secs  REAL",
    "ALTER TABLE trades  ADD COLUMN freshness_label   TEXT",
    "ALTER TABLE trades  ADD COLUMN relevance_score   TEXT",
    "ALTER TABLE trades  ADD COLUMN relevance_reason  TEXT",
    "CREATE INDEX IF NOT EXISTS idx_signals_freshness ON signals(freshness_label)",
    "CREATE INDEX IF NOT EXISTS idx_trades_freshness  ON trades(freshness_label)",
    # Cycle 11 — pre-headline market behaviour (6 derived scalars).
    # `current` is omitted: it mirrors mid_yes at decision time, which
    # we already store. `range` and `dist_to_rail` derive in queries.
    "ALTER TABLE trades ADD COLUMN mkt_vol_30m   REAL",
    "ALTER TABLE trades ADD COLUMN mkt_mean_30m  REAL",
    "ALTER TABLE trades ADD COLUMN mkt_min_30m   REAL",
    "ALTER TABLE trades ADD COLUMN mkt_max_30m   REAL",
    "ALTER TABLE trades ADD COLUMN mkt_drift_30m REAL",
    "ALTER TABLE trades ADD COLUMN mkt_n_samples INTEGER",
    # ── Simulate-mode fidelity (plan §7) ──────────────────────────────
    # shares is the integer-cent share count actually held at the entry
    # fill price. NULL on legacy rows; trader treats NULL as
    # `size_usdc / entry_price` (the old implicit accounting).
    "ALTER TABLE positions ADD COLUMN shares REAL",
    # Polymarket dynamic taker fees applied to entry and exit fills under
    # simulate mode. NULL outside simulate mode. Persisted so the audit
    # script can re-derive realized_pnl independently.
    "ALTER TABLE trades ADD COLUMN entry_fee_usdc REAL",
    "ALTER TABLE trades ADD COLUMN exit_fee_usdc  REAL",
    # FIX-2B (2026-05-22): persist the actual exit fill price and the
    # entry_price the close was computed against. Once close_position
    # deletes the positions row, these are the only DB anchors that
    # let an audit/postmortem reconcile realized_pnl independently.
    # The explicit one-shot migration runnable against the live DB is
    # seulgi/scripts/db_migrations/2026_05_22_add_exit_price.py; these
    # two ALTERs keep fresh DBs (tests, replay) in sync.
    "ALTER TABLE trades ADD COLUMN exit_price           REAL",
    "ALTER TABLE trades ADD COLUMN entry_price_at_close REAL",
    # Tier 2 (2026-05-20 perf) — see plan-file: DB + program perf.
    # Dashboard queries (variant_stats, decision_funnel, freshness_buckets,
    # confidence_buckets) all WHERE decided_at >= ?. Without these indexes
    # SQLite does full scans over 4 M-row trades + 1.7 M-row engine_results
    # and the queries time out at 10 s. Composite (engine_name, decided_at)
    # serves the common per-engine GROUP BY / ORDER BY pattern.
    "CREATE INDEX IF NOT EXISTS idx_trades_decided_at ON trades(decided_at)",
    "CREATE INDEX IF NOT EXISTS idx_trades_engine_decided ON trades(engine_name, decided_at)",
    "CREATE INDEX IF NOT EXISTS idx_trades_variant_decided ON trades(variant_name, decided_at)",
    "CREATE INDEX IF NOT EXISTS idx_engine_results_decided ON engine_results(decided_at)",
    "CREATE INDEX IF NOT EXISTS idx_engine_results_engine_decided ON engine_results(engine_name, decided_at)",
    # 2026-05-20 observability: minute-bucketed metrics from app.metrics.
    # One row per (bucket_ts, name, tags). counters write count only;
    # timers write count + sum + percentiles from a reservoir sample.
    """CREATE TABLE IF NOT EXISTS metrics_minutely (
        bucket_ts TEXT NOT NULL,
        name      TEXT NOT NULL,
        tags      TEXT NOT NULL DEFAULT '',
        count     INTEGER NOT NULL,
        sum       REAL,
        min       REAL,
        max       REAL,
        p50       REAL,
        p95       REAL,
        p99       REAL,
        PRIMARY KEY (bucket_ts, name, tags)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_metrics_name_bucket ON metrics_minutely(name, bucket_ts)",
    "CREATE INDEX IF NOT EXISTS idx_metrics_bucket ON metrics_minutely(bucket_ts)",
    # Part 2.1 (2026-05-21): market_price_snaps — one row per matched
    # market per headline, captured at price-prefetch time. Complements
    # price_timeseries (which only covers executed-trade impacts) by
    # giving the backtest framework a full corpus of "what was the
    # market price at time T?" snapshots, including for markets the
    # bot looked at but skipped. Foundation for Part 2 (strategy
    # backtest framework).
    """CREATE TABLE IF NOT EXISTS market_price_snaps (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        condition_id TEXT NOT NULL,
        token_id     TEXT,                     -- nullable: harness / inferred prices
        side         TEXT NOT NULL,           -- 'yes' | 'no'
        price        REAL NOT NULL,
        t_unix       INTEGER NOT NULL,
        source       TEXT NOT NULL DEFAULT 'prefetch',
        headline_id  TEXT                      -- nullable: future non-headline sources
    )""",
    "CREATE INDEX IF NOT EXISTS idx_mps_condition_t ON market_price_snaps(condition_id, t_unix)",
    "CREATE INDEX IF NOT EXISTS idx_mps_t           ON market_price_snaps(t_unix)",
    "CREATE INDEX IF NOT EXISTS idx_mps_headline    ON market_price_snaps(headline_id)",
    # FIX-3D (per A0.6 db-audit, cycle-2 Layer A follow-up):
    # - (engine_name, closed_at) is the hot path for the rolling-hit-rate
    #   query that took 1.4 s/engine on 288k rows (28 s total stall for
    #   20 engines). Background refresher hides it today; this index
    #   restores correctness when the refresher is re-enabled.
    # - (condition_id, decided_at) for "did we ever trade X" lookups on
    #   the 6.6M-row trades table; today these full-scan.
    # - (t_unix) on market_price_snaps is redundant with idx_mps_t above
    #   but the A0.6 audit explicitly called for the named index; both
    #   are IF NOT EXISTS so applying both is safe.
    "CREATE INDEX IF NOT EXISTS idx_trades_engine_closed ON trades(engine_name, closed_at)",
    "CREATE INDEX IF NOT EXISTS idx_trades_condition_decided ON trades(condition_id, decided_at)",
    "CREATE INDEX IF NOT EXISTS idx_market_price_snaps_t_unix ON market_price_snaps(t_unix)",
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SeulgiDB:
    """
    Async SQLite wrapper for Seulgi.

    All operations go through a single aiosqlite connection; SQLite's
    serialised write model means no extra locking is needed here.
    """

    def __init__(self, path: str) -> None:
        self._path = path
        self._conn: Optional[aiosqlite.Connection] = None
        # Tier 3 (2026-05-20 perf): nesting depth for db.batch(). Insert
        # functions check this — if > 0, they skip the per-insert commit
        # so the outer batch's single commit covers everything (one fsync
        # per headline instead of 30-150).
        self._in_batch: int = 0
        # Phase B (2026-05-21): serialise outer batch() entries so
        # concurrent process() calls don't race on the BEGIN/COMMIT
        # bracket. Lazy-init in batch() because asyncio.Lock must be
        # created inside a running event loop.
        self._batch_lock: Optional[asyncio.Lock] = None
        # Phase-0 observability: track per-table last successful insert
        # and per-method ok/err counters. Surfaced by `write_health()`
        # and the `db_health.txt` dashboard tile. Previously the silent-
        # rollback bug ate inserts invisibly; these counters make it
        # impossible to lose visibility again.
        self._last_insert_at: dict[str, datetime] = {}
        self._write_counters: dict[str, dict[str, int]] = {}

    async def connect(self) -> None:
        """Open the DB connection, create schema, and apply migrations."""
        # FIX-3A (cycle-2 Layer C): open with isolation_level=None for true
        # autocommit. Python's sqlite3 default `DEFERRED` auto-issues implicit
        # BEGIN before write statements, which can interleave with the
        # explicit BEGIN in batch() → "cannot start a transaction within a
        # transaction". With autocommit, the explicit BEGIN/COMMIT in batch()
        # are the only txn boundaries; per-row writes outside a batch
        # auto-commit (cheap on WAL+synchronous=NORMAL).
        self._conn = await aiosqlite.connect(self._path, isolation_level=None)
        self._conn.row_factory = aiosqlite.Row
        # Tier 1 (2026-05-20 perf) — see plan-file: DB + program perf:
        #   WAL: readers don't block writers (was journal_mode=delete).
        #   synchronous=NORMAL: fsync at WAL checkpoint, not every commit.
        #     Still crash-safe; on power loss only last few txns may be lost.
        #   cache_size=-65536: 64 MB page cache (was 2 MB on a 3.7 GB DB).
        #   temp_store=MEMORY: GROUP BY / ORDER BY in RAM, not disk.
        #   mmap_size: cheap reads via memory map.
        #   wal_autocheckpoint: keep -wal file bounded.
        #   busy_timeout: writers wait up to 30 s instead of raising
        #     "database is locked" immediately on transient contention.
        await self._conn.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            PRAGMA cache_size=-65536;
            PRAGMA temp_store=MEMORY;
            PRAGMA mmap_size=268435456;
            PRAGMA wal_autocheckpoint=1000;
            PRAGMA busy_timeout=30000;
        """)
        await self._conn.executescript(_SCHEMA)
        await self._conn.executescript(_PRICE_IMPACT_SCHEMA)
        # FIX-3C: Phase-5 capture tables (idempotent via IF NOT EXISTS).
        await self._conn.executescript(_HIST_SIM_SCHEMA)
        await self._commit_if_unbatched()
        await self._migrate()
        # One-time backfill: tag any historical NULL-engine trades using the
        # closest engine_results match. Cheap (one UPDATE), idempotent, and
        # essential — without it `get_engine_gain_and_staked` returns 0 for
        # every engine after a container restart even when realized PnL is
        # non-zero in DB. See `backfill_trades_engine_name` for the why.
        try:
            n = await self.backfill_trades_engine_name()
            if n:
                logger.info("[db] backfilled engine_name on %d historical trades", n)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[db] engine_name backfill failed: %s", exc)
        logger.info("SeulgiDB connected: %s", self._path)

    @asynccontextmanager
    async def batch(self):
        """Group inserts into a single transaction = single fsync.

        Tier 3 (2026-05-20 perf): per-headline write fan-out used to call
        commit() 30-150 times. With WAL+synchronous=NORMAL each fsync is
        cheap, but commit() still serialises the write thread. Batching
        keeps the writer occupied with INSERTs instead of BEGIN/COMMIT
        churn and amortises the WAL frame writes.

        Insert functions check `self._in_batch`: if > 0 they skip their
        per-insert commit; the outer batch commits once at exit.

        Concurrency model (2026-05-21 v2): batches are **strictly
        serialised** across coroutines via `_batch_lock`. The earlier
        "outer / inner via counter" design let a second coroutine join
        an open batch (saw `_in_batch > 0`, took the no-op path) and
        kept `_in_batch` positive past the original holder's exit. The
        original holder's `_commit_if_unbatched` then saw `_in_batch >
        0` and SKIPPED the COMMIT. The transaction stayed open. The
        next coroutine entering batch saw `_in_batch == 0`, did BEGIN,
        and SQLite raised "cannot start a transaction within a
        transaction". Any inserts that landed in the stranded
        transaction got silently rolled back when the connection
        eventually recycled — the actual B-WRITES bug.

        The fix: always acquire the lock, always BEGIN, always COMMIT.
        `_in_batch` is now binary (0 or 1) used ONLY by
        `_commit_if_unbatched` so individual inserts skip their per-row
        commits while a batch is active. No code path nests batches
        today; if one ever does it now deadlocks loudly instead of
        silently corrupting the writer.

        Latency cost: concurrent process() calls serialise their write
        phases (~50 ms typical at production size). HTTP/AI work in
        process() still runs in parallel before the batch opens.
        """
        if self._batch_lock is None:
            self._batch_lock = asyncio.Lock()
        async with self._batch_lock:
            # FIX-3B (cycle-2 Layer B): Recover from any prior batch that
            # left the connection mid-transaction (cancelled at an await
            # point — e.g. the wait_for(90s) on gather timing out). Without
            # this probe, our subsequent `BEGIN` raises "cannot start a
            # transaction within a transaction" → every following batch
            # caller hits the same → permanent break until container
            # restart. ROLLBACK is a no-op when there's no active txn
            # (autocommit mode), so this is cheap and safe.
            try:
                await asyncio.shield(self._conn.execute("ROLLBACK"))
            except Exception:  # noqa: BLE001
                pass

            # Mark "inside batch" in THIS task's context. Child coroutines
            # awaited inside the batch inherit the flag via contextvars,
            # so their writes route through _write_lock's reentrant path
            # and don't deadlock or skip commit.
            token = _in_batch_var.set(True)
            await self._conn.execute("BEGIN")
            self._in_batch = 1
            try:
                yield
            except BaseException as exc:
                self._in_batch = 0
                logger.warning(
                    "[db.batch] rolling back due to %s: %r",
                    type(exc).__name__, exc, exc_info=True,
                )
                if _metrics is not None:
                    try:
                        _metrics.incr(
                            "db.batch.rollback",
                            tags={"exc": type(exc).__name__},
                        )
                    except Exception:  # noqa: BLE001
                        pass
                # FIX-3B: shield the cleanup rollback from cancellation
                # propagating UP the await chain. If we're being cancelled
                # (e.g. wait_for timeout), a plain `await rollback()` would
                # re-raise CancelledError BEFORE the rollback completes,
                # leaving the connection mid-txn. `asyncio.shield` keeps
                # the rollback running to completion even when the task
                # is cancelled. We catch BaseException because shield can
                # itself raise CancelledError when the outer task is
                # cancelled.
                try:
                    await asyncio.shield(self._conn.execute("ROLLBACK"))
                except BaseException as r_exc:
                    logger.error(
                        "[db.batch] rollback() raised: %r — connection state may be corrupt",
                        r_exc, exc_info=True,
                    )
                    if _metrics is not None:
                        try:
                            _metrics.incr("db.batch.rollback_failed")
                        except Exception:  # noqa: BLE001
                            pass
                raise
            else:
                self._in_batch = 0
                # FIX-3B: shield commit too — the symmetric cancellation
                # window exists on the success path.
                await asyncio.shield(self._conn.execute("COMMIT"))
            finally:
                _in_batch_var.reset(token)

    @asynccontextmanager
    async def _write_lock(self):
        """TEMPORARILY a no-op pass-through (2026-05-21 04:20 UTC).

        Originally added to serialize individual writes against
        concurrent `batch()` calls (closing the metrics-flush implicit-
        txn race). The 200/c=12 + 500/c=16 replay harness PASSED with
        the lock in place — but production deployment of Container
        6/7 with the same code deadlocked: `bump_market_heat` calls
        succeeded but every subsequent `insert_signal` blocked
        forever (provably: external Python script `insert_signal`
        on the production DB timed out at 10 s while the writer
        worker thread was idle in `select()` per `py-spy dump`).

        The harness doesn't reproduce the deadlock so the cause is
        production-specific (real LLM latency / metrics flush /
        position monitor / dashboard background tasks interacting
        with the lock somehow). Need to investigate separately.

        Falling back to pre-write-lock behavior: writes do NOT
        serialize against concurrent `batch()`. The metrics-flush
        race remains (every 5 min one tick may fail with `cannot
        start a transaction`) but doesn't lose data. The B-WRITES
        bug stays fixed because `batch()` still strictly serializes
        on `_batch_lock`.
        """
        yield

    async def _commit_if_unbatched(self) -> None:
        """Helper for insert/update functions: commit only outside a batch()."""
        if self._in_batch == 0:
            await self._conn.commit()

    @asynccontextmanager
    async def savepoint(self, name: str = "sp"):
        """Inner-scope SAVEPOINT — survives outer commits/rollbacks.

        Phase 1 R2: trader.evaluate wraps `insert_trade + open_position`
        in this so if open_position fails the trade audit row survives
        rather than being silently rolled back together with the position
        attempt. Compatible with autocommit and with `batch()`.
        """
        safe = "".join(c if c.isalnum() else "_" for c in name)[:32] or "sp"
        await self._conn.execute(f"SAVEPOINT {safe}")
        try:
            yield
        except BaseException as exc:
            logger.warning(
                "[db.savepoint:%s] rolling back due to %s: %r",
                safe, type(exc).__name__, exc, exc_info=True,
            )
            if _metrics is not None:
                try:
                    _metrics.incr(
                        "db.savepoint.rollback",
                        tags={"name": safe, "exc": type(exc).__name__},
                    )
                except Exception:  # noqa: BLE001
                    pass
            try:
                await self._conn.execute(f"ROLLBACK TO SAVEPOINT {safe}")
            except Exception as r_exc:  # noqa: BLE001
                logger.error(
                    "[db.savepoint:%s] ROLLBACK TO raised: %r",
                    safe, r_exc, exc_info=True,
                )
            try:
                await self._conn.execute(f"RELEASE SAVEPOINT {safe}")
            except Exception:  # noqa: BLE001
                pass
            raise
        else:
            await self._conn.execute(f"RELEASE SAVEPOINT {safe}")

    # ------------------------------------------------------------------
    # Phase-0 observability helpers
    # ------------------------------------------------------------------

    def _bump_ok(self, method: str, table: str) -> None:
        """Record a successful write — update last_insert_at + counter + metric."""
        self._last_insert_at[table] = datetime.now(timezone.utc)
        c = self._write_counters.setdefault(method, {"ok": 0, "err": 0})
        c["ok"] += 1
        if _metrics is not None:
            try:
                _metrics.incr(f"db.{method}.ok")
            except Exception:  # noqa: BLE001
                pass

    def _bump_err(self, method: str, exc: BaseException) -> None:
        """Record a failed write — log with traceback + counter + metric."""
        c = self._write_counters.setdefault(method, {"ok": 0, "err": 0})
        c["err"] += 1
        logger.warning("[db] %s failed: %r", method, exc, exc_info=True)
        if _metrics is not None:
            try:
                _metrics.incr(
                    f"db.{method}.err",
                    tags={"exc": type(exc).__name__},
                )
            except Exception:  # noqa: BLE001
                pass

    def write_health_snapshot(self) -> dict:
        """Synchronous snapshot of in-memory write counters.

        Pure dict construction — no DB call. Safe to invoke from the
        30 s dashboard render path even when the writer is contended.
        WAL frame count is reported by the host-side audit CLI, not
        from here.
        """
        return {
            "last_insert_at": {
                table: ts.isoformat()
                for table, ts in self._last_insert_at.items()
            },
            "counters": {m: dict(c) for m, c in self._write_counters.items()},
        }

    async def write_health(self) -> dict:
        """Async wrapper that also probes PRAGMAs.

        The PRAGMA calls share the writer connection — under load they
        queue behind real writes and can be slow. Callers that need a
        fast non-blocking snapshot should use `write_health_snapshot()`
        directly. The dashboard uses the sync version; analysis CLIs
        use this async one.
        """
        health = self.write_health_snapshot()
        try:
            async with self._conn.execute(
                "PRAGMA wal_checkpoint(PASSIVE)"
            ) as cur:
                row = await cur.fetchone()
                if row:
                    health["wal_busy"] = row[0]
                    health["wal_total_frames"] = row[1]
                    health["wal_checkpointed_frames"] = row[2]
        except Exception as exc:  # noqa: BLE001
            health["wal_error"] = repr(exc)
        try:
            async with self._conn.execute("PRAGMA busy_timeout") as cur:
                row = await cur.fetchone()
                if row:
                    health["busy_timeout_ms"] = row[0]
        except Exception:  # noqa: BLE001
            pass
        return health

    async def connect_ro(self) -> None:
        """
        Open the DB in read-only mode (no schema create, no migrations).

        Use from sidecar tools (debug CLI, analysis scripts) that must NOT
        block on the bot's writer lock. Migrations via ALTER TABLE require an
        exclusive lock which conflicts with a live writer; read-only opens via
        URI mode=ro never take a write lock.
        """
        uri = f"file:{self._path}?mode=ro"
        # FIX-3A: pass isolation_level=None for consistency with writer
        # connections; RO conns never write, but autocommit avoids the
        # implicit-BEGIN footgun if a sibling ever flips to RW.
        self._conn = await aiosqlite.connect(uri, uri=True, isolation_level=None)
        self._conn.row_factory = aiosqlite.Row
        logger.info("SeulgiDB connected RO: %s", self._path)

    @classmethod
    async def open_ro_clone(cls, path: str) -> "SeulgiDB":
        """Open a sibling SeulgiDB with its own RO aiosqlite connection.

        Phase 2 (2026-05-21): the dashboard's heavy-query cache used to
        run all 5+ SELECTs on the writer's `self._conn`, which serialised
        with the writer in aiosqlite's single-connection worker thread.
        With this clone the cache runs on its own thread; WAL guarantees
        zero blocking against the writer.
        """
        inst = cls(path)
        uri = f"file:{path}?mode=ro"
        # FIX-3A: autocommit also on RO clones for consistency.
        inst._conn = await aiosqlite.connect(uri, uri=True, isolation_level=None)
        inst._conn.row_factory = aiosqlite.Row
        await inst._conn.executescript("PRAGMA busy_timeout=10000;")
        logger.info("SeulgiDB RO clone connected: %s", path)
        return inst

    @classmethod
    async def open_writer_clone(cls, path: str) -> "SeulgiDB":
        """Open a sibling SeulgiDB with its OWN aiosqlite writer connection.

        Phase 3 (2026-05-21): the metrics flush task used to share the
        main writer's connection — if a flush queued up behind a slow
        write, both stalled together. With this clone the metrics writer
        runs on its own thread; WAL still serialises actual lock
        acquisition (one writer at a time on the file), but the main
        writer's worker thread is never blocked by flush work.
        """
        inst = cls(path)
        # FIX-3A: writer clones MUST use autocommit for the same reason
        # as the main writer — implicit BEGIN can race with batch()'s
        # explicit BEGIN.
        inst._conn = await aiosqlite.connect(path, isolation_level=None)
        inst._conn.row_factory = aiosqlite.Row
        await inst._conn.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            PRAGMA busy_timeout=10000;
        """)
        logger.info("SeulgiDB writer clone connected: %s", path)
        return inst

    async def _migrate(self) -> None:
        """Apply column additions idempotently — ignores 'duplicate column' errors."""
        for sql in _MIGRATIONS:
            try:
                await self._conn.execute(sql)
                await self._commit_if_unbatched()
            except Exception:
                pass  # column already exists

    async def close(self) -> None:
        if self._conn:
            # WAL checkpoint before close so the next start opens a clean
            # main DB file. Without TRUNCATE the -wal file can keep growing
            # across container restarts; with it, the wal is rolled into
            # the main DB and reset. Best-effort: if the checkpoint fails
            # (locked, busy, etc.) we still close.
            try:
                await self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                await self._commit_if_unbatched()
            except Exception as exc:  # noqa: BLE001
                logger.warning("[db] wal_checkpoint on close failed: %s", exc)
            await self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------
    # Markets
    # ------------------------------------------------------------------

    async def upsert_market(
        self,
        condition_id: str,
        question: str,
        token_yes: Optional[str],
        token_no: Optional[str],
        end_date: Optional[str],
        volume_usdc: Optional[float],
        embedding: Optional[np.ndarray],
    ) -> None:
        emb_bytes = embedding.tobytes() if embedding is not None else None
        # NOTE: do NOT use INSERT OR REPLACE — it would wipe match_count/last_matched_at
        # (REPLACE = DELETE + INSERT, defaulting columns not in VALUES).
        await self._conn.execute(
            """
            INSERT INTO markets
              (condition_id, question, token_yes, token_no, end_date, volume_usdc, embedding, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(condition_id) DO UPDATE SET
              question    = excluded.question,
              token_yes   = excluded.token_yes,
              token_no    = excluded.token_no,
              end_date    = excluded.end_date,
              volume_usdc = excluded.volume_usdc,
              embedding   = excluded.embedding,
              fetched_at  = excluded.fetched_at,
              -- Reopen any market that Gamma is showing again. Without this
              -- a market mistakenly closed by an earlier short-pagination
              -- refresh would stay closed forever (until 30-day prune).
              closed_at   = NULL
            """,
            (condition_id, question, token_yes, token_no, end_date,
             volume_usdc, emb_bytes, _now_iso()),
        )
        await self._commit_if_unbatched()

    async def get_markets_with_embeddings(self) -> list[dict]:
        async with self._conn.execute(
            """
            SELECT condition_id, question, token_yes, token_no, embedding, market_type
            FROM markets
            WHERE embedding IS NOT NULL
              AND closed_at IS NULL
            """
        ) as cur:
            rows = await cur.fetchall()
        result = []
        for row in rows:
            emb = np.frombuffer(row["embedding"], dtype=np.float32).copy()
            result.append({
                "condition_id": row["condition_id"],
                "question": row["question"],
                "token_yes": row["token_yes"],
                "token_no": row["token_no"],
                "embedding": emb,
                "market_type": row["market_type"],
            })
        return result

    async def count_markets(self, include_closed: bool = False) -> int:
        sql = "SELECT COUNT(*) FROM markets"
        if not include_closed:
            sql += " WHERE closed_at IS NULL"
        async with self._conn.execute(sql) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0

    async def mark_markets_closed(self, keep_condition_ids: list[str]) -> int:
        """
        Mark any market NOT in `keep_condition_ids` as closed (sets closed_at = now).
        Also marks markets whose end_date has passed.
        Returns the number of markets newly marked closed.

        Called once per refresh — keeps the in-memory index in sync with Gamma's
        open-markets list and prevents stale matches against resolved markets.
        """
        now = _now_iso()
        # 1) Mark by absence from the latest fetch (chunked to avoid SQLite's
        #    999-param limit; we typically pass ≤300 IDs but be defensive)
        newly_closed_by_fetch = 0
        if keep_condition_ids:
            chunk = 500
            for i in range(0, len(keep_condition_ids), chunk):
                ids = keep_condition_ids[i:i + chunk]
                placeholders = ",".join(["?"] * len(ids))
                # First batch resets nothing; subsequent batches restrict the
                # update to rows not in any chunk. Simplest path: collect all
                # "not in" deletions once with a temporary table.
            # Switch strategy: load existing open IDs, diff in Python (clearer).
            async with self._conn.execute(
                "SELECT condition_id FROM markets WHERE closed_at IS NULL"
            ) as cur:
                existing = {row[0] for row in await cur.fetchall()}
            keep_set = set(keep_condition_ids)
            to_close = list(existing - keep_set)
            if to_close:
                # Chunked UPDATE to dodge param limits
                for i in range(0, len(to_close), chunk):
                    ids = to_close[i:i + chunk]
                    placeholders = ",".join(["?"] * len(ids))
                    await self._conn.execute(
                        f"UPDATE markets SET closed_at = ? "
                        f"WHERE condition_id IN ({placeholders})",
                        (now, *ids),
                    )
                newly_closed_by_fetch = len(to_close)

        # 2) Also mark anything with end_date in the past
        await self._conn.execute(
            """
            UPDATE markets
               SET closed_at = ?
             WHERE closed_at IS NULL
               AND end_date IS NOT NULL
               AND end_date < ?
            """,
            (now, now),
        )
        await self._commit_if_unbatched()
        return newly_closed_by_fetch

    async def prune_closed_markets(self, older_than_days: int = 30) -> int:
        """
        Hard-delete markets that have been closed for more than `older_than_days`.
        Keeps recent closures around so heat history isn't wiped immediately.
        Returns the number of rows deleted.
        """
        cur = await self._conn.execute(
            """
            DELETE FROM markets
             WHERE closed_at IS NOT NULL
               AND closed_at < datetime('now', ? || ' days')
            """,
            (f"-{older_than_days}",),
        )
        await self._commit_if_unbatched()
        return cur.rowcount or 0

    # ------------------------------------------------------------------
    # Signals
    # ------------------------------------------------------------------

    async def insert_signal(
        self,
        site_url: str,
        headline: str,
        headline_id: Optional[str] = None,
        freshness_label: Optional[str] = None,
        nearest_age_secs: Optional[float] = None,
    ) -> int:
        async with self._write_lock():
            try:
                cur = await self._conn.execute(
                    """INSERT INTO signals
                         (site_url, headline, detected_at, headline_id,
                          freshness_label, nearest_age_secs)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (site_url, headline, _now_iso(), headline_id,
                     freshness_label, nearest_age_secs),
                )
                await self._commit_if_unbatched()
                self._bump_ok("insert_signal", "signals")
                return cur.lastrowid
            except Exception as exc:
                self._bump_err("insert_signal", exc)
                raise

    # ------------------------------------------------------------------
    # Trades
    # ------------------------------------------------------------------

    async def insert_trade(
        self,
        signal_id: int,
        condition_id: str,
        question: str,
        token_id: Optional[str],
        direction: str,
        similarity: float,
        p_hat: float,
        mid_price: Optional[float],
        size_usdc: float,
        skip_reason: Optional[str],
        headline_id: Optional[str] = None,
        confidence_raw: Optional[float] = None,
        p_hat_if_raw: Optional[float] = None,
        signed_edge: Optional[float] = None,
        signed_edge_if_raw: Optional[float] = None,
        engine_name: Optional[str] = None,
        agreement: Optional[float] = None,
        p_alpha: Optional[float] = None,
        risk_score: Optional[float] = None,
        ai_reason: Optional[str] = None,
        ai_raw_response: Optional[str] = None,
        ai_prompt_meta: Optional[str] = None,
        trader_trace: Optional[str] = None,
        mid_yes: Optional[float] = None,
        mid_no: Optional[float] = None,
        inverted: Optional[int] = None,
        variant_name: Optional[str] = None,
        freshness_label: Optional[str] = None,
        relevance_score: Optional[str] = None,
        relevance_reason: Optional[str] = None,
        mkt_vol_30m: Optional[float] = None,
        mkt_mean_30m: Optional[float] = None,
        mkt_min_30m: Optional[float] = None,
        mkt_max_30m: Optional[float] = None,
        mkt_drift_30m: Optional[float] = None,
        mkt_n_samples: Optional[int] = None,
    ) -> int:
        async with self._write_lock():
            try:
                cur = await self._conn.execute(
                    """
                    INSERT INTO trades
                      (signal_id, condition_id, question, token_id, direction, similarity,
                       p_hat, mid_price, size_usdc, skip_reason, decided_at, headline_id,
                       confidence_raw, p_hat_if_raw, signed_edge, signed_edge_if_raw,
                       engine_name, agreement, p_alpha, risk_score,
                       ai_reason, ai_raw_response, ai_prompt_meta, trader_trace,
                       mid_yes, mid_no, inverted, variant_name,
                       freshness_label, relevance_score, relevance_reason,
                       mkt_vol_30m, mkt_mean_30m, mkt_min_30m, mkt_max_30m,
                       mkt_drift_30m, mkt_n_samples)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (signal_id, condition_id, question, token_id, direction, similarity,
                     p_hat, mid_price, size_usdc, skip_reason, _now_iso(), headline_id,
                     confidence_raw, p_hat_if_raw, signed_edge, signed_edge_if_raw,
                     engine_name, agreement, p_alpha, risk_score,
                     ai_reason, ai_raw_response, ai_prompt_meta, trader_trace,
                     mid_yes, mid_no, inverted, variant_name,
                     freshness_label, relevance_score, relevance_reason,
                     mkt_vol_30m, mkt_mean_30m, mkt_min_30m, mkt_max_30m,
                     mkt_drift_30m, mkt_n_samples),
                )
                await self._commit_if_unbatched()
                self._bump_ok("insert_trade", "trades")
                return cur.lastrowid
            except Exception as exc:
                self._bump_err("insert_trade", exc)
                raise

    async def backfill_trades_engine_name(self) -> int:
        """
        One-time backfill: tag historical `trades` rows with the engine that
        made them. Matches each NULL-engine trade to the engine_results row
        with the same `condition_id` whose `decided_at` is closest in time
        (engines decide on the same headline within seconds of each other,
        so the nearest timestamp wins). Idempotent. Returns rows updated.

        Implemented in Python rather than a correlated SQL UPDATE because the
        SQLite shipped with our base image can't reference the outer table
        name from inside the SET subquery (the correlated form raised
        "no such column: trades.decided_at"). The Python loop is fine: this
        runs at most once per DB, on at most a few thousand legacy rows.
        """
        cur = await self._conn.execute(
            """SELECT id, condition_id, decided_at FROM trades
               WHERE engine_name IS NULL AND skip_reason IS NULL"""
        )
        nulls = await cur.fetchall()
        if not nulls:
            return 0

        # Pull every executed engine_results row keyed by condition_id.
        cur = await self._conn.execute(
            """SELECT condition_id, engine_name, decided_at
               FROM engine_results WHERE executed = 1"""
        )
        er_rows = await cur.fetchall()
        from collections import defaultdict
        from datetime import datetime as _dt
        er_by_cond: dict[str, list[tuple[str, float]]] = defaultdict(list)
        for er in er_rows:
            try:
                ts = _dt.fromisoformat(er[2]).timestamp()
            except Exception:  # noqa: BLE001
                continue
            er_by_cond[er[0]].append((er[1], ts))

        updated = 0
        for tid, cond_id, t_at in nulls:
            try:
                tts = _dt.fromisoformat(t_at).timestamp()
            except Exception:  # noqa: BLE001
                continue
            candidates = er_by_cond.get(cond_id)
            if not candidates:
                continue
            best = min(candidates, key=lambda p: abs(p[1] - tts))
            await self._conn.execute(
                "UPDATE trades SET engine_name = ? WHERE id = ?",
                (best[0], tid),
            )
            updated += 1
        await self._commit_if_unbatched()
        return updated

    async def close_trade(
        self,
        trade_id: int,
        realized_pnl: float,
        close_reason: Optional[str] = None,
        entry_fee_usdc: Optional[float] = None,
        exit_fee_usdc: Optional[float] = None,
        exit_price: Optional[float] = None,
        entry_price_at_close: Optional[float] = None,
    ) -> None:
        """Record the outcome of a closed position on the trade row.

        `entry_fee_usdc` / `exit_fee_usdc` are populated only under simulate
        mode's fidelity layer (plan §7.2). Left NULL otherwise.

        FIX-2B (2026-05-22): `exit_price` (the effective fill price the
        close was settled at — slipped mid for early exits; $1/$0 for
        settlement rails) and `entry_price_at_close` (the position's
        entry_price the close was computed against; sanity-check
        anchor) are now persisted on every close path. Backfilled to
        NULL on pre-migration rows — see scripts/db_migrations/
        2026_05_22_add_exit_price.py. NULL-tolerant via COALESCE so
        legacy callers (e.g. aggressive_core paper_trader) still work.
        """
        await self._conn.execute(
            """UPDATE trades SET realized_pnl = ?, closed_at = ?,
                                 close_reason = ?,
                                 entry_fee_usdc       = COALESCE(?, entry_fee_usdc),
                                 exit_fee_usdc        = COALESCE(?, exit_fee_usdc),
                                 exit_price           = COALESCE(?, exit_price),
                                 entry_price_at_close = COALESCE(?, entry_price_at_close)
               WHERE id = ?""",
            (realized_pnl, _now_iso(), close_reason,
             entry_fee_usdc, exit_fee_usdc,
             exit_price, entry_price_at_close, trade_id),
        )
        await self._commit_if_unbatched()

    async def recent_trades(self, limit: int = 20) -> list[dict]:
        async with self._conn.execute(
            """
            SELECT t.id, t.condition_id, t.question, t.direction, t.p_hat,
                   t.mid_price, t.size_usdc, t.skip_reason, t.decided_at,
                   t.realized_pnl, t.closed_at, s.headline, s.site_url
            FROM trades t JOIN signals s ON t.signal_id = s.id
            ORDER BY t.decided_at DESC LIMIT ?
            """,
            (limit,),
        ) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # Positions
    # ------------------------------------------------------------------

    async def open_position(
        self,
        trade_id: int,
        condition_id: str,
        question: str,
        token_id: str,
        direction: str,
        size_usdc: float,
        entry_price: float,
        p_hat: float,
        engine_name: str = "default",
        shares: Optional[float] = None,
        entry_fee_usdc: Optional[float] = None,
    ) -> None:
        async with self._write_lock():
            try:
                await self._conn.execute(
                    """
                    INSERT OR REPLACE INTO positions
                      (trade_id, condition_id, question, token_id, direction,
                       size_usdc, entry_price, opened_at, last_price, unrealized_pnl, p_hat,
                       engine_name, shares)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0.0, ?, ?, ?)
                    """,
                    (trade_id, condition_id, question, token_id, direction,
                     size_usdc, entry_price, _now_iso(), entry_price, p_hat,
                     engine_name, shares),
                )
                if entry_fee_usdc is not None:
                    await self._conn.execute(
                        "UPDATE trades SET entry_fee_usdc = ? WHERE id = ?",
                        (entry_fee_usdc, trade_id),
                    )
                await self._commit_if_unbatched()
                self._bump_ok("open_position", "positions")
            except Exception as exc:
                self._bump_err("open_position", exc)
                raise

    async def get_open_positions(self, engine_name: Optional[str] = None) -> list[dict]:
        """Return open positions, optionally filtered to one engine.

        Joins to `markets` so callers see the market_type alongside the
        position fields — the exit-leg fee/slippage compute needs it to
        look up the per-category rate (FIX-1, plan §exit-fee plumbing).
        """
        if engine_name is None:
            async with self._conn.execute(
                """
                SELECT p.*, m.market_type AS market_type
                  FROM positions p
                  LEFT JOIN markets m ON p.condition_id = m.condition_id
                """
            ) as cur:
                rows = await cur.fetchall()
        else:
            async with self._conn.execute(
                """
                SELECT p.*, m.market_type AS market_type
                  FROM positions p
                  LEFT JOIN markets m ON p.condition_id = m.condition_id
                 WHERE p.engine_name = ?
                """,
                (engine_name,),
            ) as cur:
                rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def has_open_position(
        self, condition_id: str, engine_name: Optional[str] = None
    ) -> bool:
        if engine_name is None:
            async with self._conn.execute(
                "SELECT 1 FROM positions WHERE condition_id = ? LIMIT 1",
                (condition_id,),
            ) as cur:
                return await cur.fetchone() is not None
        async with self._conn.execute(
            "SELECT 1 FROM positions WHERE condition_id = ? AND engine_name = ? LIMIT 1",
            (condition_id, engine_name),
        ) as cur:
            return await cur.fetchone() is not None

    async def count_open_positions(self, engine_name: Optional[str] = None) -> int:
        if engine_name is None:
            async with self._conn.execute("SELECT COUNT(*) FROM positions") as cur:
                row = await cur.fetchone()
                return row[0] if row else 0
        async with self._conn.execute(
            "SELECT COUNT(*) FROM positions WHERE engine_name = ?", (engine_name,)
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0

    async def get_committed_usdc(self, engine_name: Optional[str] = None) -> float:
        """Sum of size_usdc across open positions — used to restore balance per engine."""
        if engine_name is None:
            async with self._conn.execute(
                "SELECT COALESCE(SUM(size_usdc), 0.0) FROM positions"
            ) as cur:
                row = await cur.fetchone()
                return float(row[0]) if row else 0.0
        async with self._conn.execute(
            "SELECT COALESCE(SUM(size_usdc), 0.0) FROM positions WHERE engine_name = ?",
            (engine_name,),
        ) as cur:
            row = await cur.fetchone()
            return float(row[0]) if row else 0.0

    async def update_position_price(
        self, trade_id: int, last_price: float, unrealized_pnl: float,
        high_water_price: Optional[float] = None,
    ) -> None:
        """
        Update last_price and unrealized_pnl. If high_water_price is provided
        AND is higher than the existing value (or the existing value is NULL),
        also update high_water_price. The MAX(...) preserves the
        monotonically-rising semantics the trailing stop depends on, even
        across concurrent updates.
        """
        if high_water_price is None:
            await self._conn.execute(
                "UPDATE positions SET last_price = ?, unrealized_pnl = ? WHERE trade_id = ?",
                (last_price, unrealized_pnl, trade_id),
            )
        else:
            await self._conn.execute(
                """
                UPDATE positions
                   SET last_price       = ?,
                       unrealized_pnl   = ?,
                       high_water_price = MAX(COALESCE(high_water_price, ?), ?)
                 WHERE trade_id = ?
                """,
                (last_price, unrealized_pnl, high_water_price, high_water_price, trade_id),
            )
        await self._commit_if_unbatched()

    async def close_position(self, trade_id: int) -> None:
        async with self._write_lock():
            try:
                await self._conn.execute(
                    "DELETE FROM positions WHERE trade_id = ?", (trade_id,)
                )
                await self._commit_if_unbatched()
                self._bump_ok("close_position", "positions")
            except Exception as exc:
                self._bump_err("close_position", exc)
                raise

    # ------------------------------------------------------------------
    # Balance state restoration
    # ------------------------------------------------------------------

    async def get_realized_pnl_total(self) -> float:
        """Sum of all realized_pnl from closed trades."""
        async with self._conn.execute(
            "SELECT COALESCE(SUM(realized_pnl), 0.0) FROM trades WHERE realized_pnl IS NOT NULL"
        ) as cur:
            row = await cur.fetchone()
            return float(row[0]) if row else 0.0

    # ──────────────────────────────────────────────────────────────────
    # Cycle bookkeeping (cycle 8+)
    # ──────────────────────────────────────────────────────────────────

    async def get_current_cycle(self) -> Optional[dict]:
        """Return the open cycle (the row with ended_at IS NULL), or None."""
        cur = await self._conn.execute(
            "SELECT id, name, started_at FROM cycles "
            "WHERE ended_at IS NULL ORDER BY id DESC LIMIT 1"
        )
        row = await cur.fetchone()
        if not row:
            return None
        return {"id": row[0], "name": row[1], "started_at": row[2]}

    async def ensure_current_cycle(self, default_name: str = "cycle_auto") -> dict:
        """Return the open cycle, creating one if none exists yet."""
        existing = await self.get_current_cycle()
        if existing:
            return existing
        await self._conn.execute(
            "INSERT INTO cycles (name, started_at) VALUES (?, ?)",
            (default_name, _now_iso()),
        )
        await self._commit_if_unbatched()
        result = await self.get_current_cycle()
        assert result is not None
        return result

    # ──────────────────────────────────────────────────────────────────
    # Watcher health (cycle 8+) — persisted per-site stability
    # ──────────────────────────────────────────────────────────────────

    async def upsert_watcher_health(
        self,
        url: str,
        *,
        success: bool,
        ema_latency_ms: Optional[float],
        current_interval_s: Optional[float],
        new_headlines: int = 0,
        content_hash: Optional[str] = None,
    ) -> None:
        """Persist one watcher-tick worth of health stats.

        Idempotent UPSERT keyed on url. `stability_score` is a recency-weighted
        EWMA of success rate − idle penalty, computed downstream from the
        accumulated counters (we just keep the raw numbers honest here).
        """
        now = _now_iso()
        s_field = "last_success_at" if success else "last_failure_at"
        # The UPSERT is verbose because we need to increment specific counters
        # only on success — and SQLite's ON CONFLICT requires fully-formed SETs.
        await self._conn.execute(
            f"""
            INSERT INTO watcher_health (url, last_fetch_at, {s_field},
                                        last_content_hash, last_headline_count,
                                        total_fetches, total_successes,
                                        total_new_headlines,
                                        ema_latency_ms, current_interval_s)
            VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
              last_fetch_at        = excluded.last_fetch_at,
              {s_field}            = excluded.{s_field},
              last_content_hash    = COALESCE(excluded.last_content_hash, watcher_health.last_content_hash),
              last_headline_count  = excluded.last_headline_count,
              total_fetches        = watcher_health.total_fetches + 1,
              total_successes      = watcher_health.total_successes + ?,
              total_new_headlines  = watcher_health.total_new_headlines + excluded.total_new_headlines,
              ema_latency_ms       = excluded.ema_latency_ms,
              current_interval_s   = excluded.current_interval_s
            """,
            (url, now, now, content_hash, new_headlines,
             1 if success else 0, new_headlines,
             ema_latency_ms, current_interval_s,
             1 if success else 0),
        )
        await self._commit_if_unbatched()

    async def start_new_cycle(self, name: str, notes: Optional[str] = None) -> dict:
        """Close the current cycle and open a new one. No data deletion.

        After this returns, `get_engine_gain_and_staked(since_ts=...)` and the
        plotter / dashboard / trade-examples sampler all narrow to the new
        cycle's window. Historical trades stay in the DB for tracing.
        """
        now = _now_iso()
        await self._conn.execute(
            "UPDATE cycles SET ended_at = ? WHERE ended_at IS NULL",
            (now,),
        )
        await self._conn.execute(
            "INSERT INTO cycles (name, started_at, notes) VALUES (?, ?, ?)",
            (name, now, notes),
        )
        await self._commit_if_unbatched()
        result = await self.get_current_cycle()
        assert result is not None
        logger.info("[db] new cycle started: %s (id=%d, ts=%s)",
                    result["name"], result["id"], result["started_at"])
        return result

    async def get_engine_gain_and_staked(
        self, engine_name: str, since_ts: Optional[str] = None,
    ) -> tuple[float, float]:
        """
        Return (realized_gain, total_staked) for ONE engine, restoring from DB.

        realized_gain = sum of realized_pnl for closed trades (matching engine).
        total_staked  = sum of size_usdc across executed engine_results rows.

        Used by infinite-balance mode to restart with the cumulative gain
        already credited and the staked total visible.

        Cycle-8: optional `since_ts` (ISO8601) scopes the restore to only the
        current cycle's data so cycle boundaries reset the dashboard to $0
        without deleting any history. Pass `None` to get lifetime totals.
        """
        gain_sql = (
            "SELECT COALESCE(SUM(realized_pnl), 0.0) FROM trades "
            "WHERE engine_name = ? AND realized_pnl IS NOT NULL"
        )
        gain_args: list = [engine_name]
        if since_ts:
            gain_sql += " AND decided_at >= ?"
            gain_args.append(since_ts)
        async with self._conn.execute(gain_sql, tuple(gain_args)) as cur:
            row = await cur.fetchone()
            gain = float(row[0]) if row else 0.0

        staked_sql = (
            "SELECT COALESCE(SUM(size_usdc), 0.0) FROM engine_results "
            "WHERE engine_name = ? AND executed = 1"
        )
        staked_args: list = [engine_name]
        if since_ts:
            staked_sql += " AND decided_at >= ?"
            staked_args.append(since_ts)
        async with self._conn.execute(staked_sql, tuple(staked_args)) as cur:
            row = await cur.fetchone()
            staked = float(row[0]) if row else 0.0

        return gain, staked

    # ------------------------------------------------------------------
    # Engine results (multi-engine comparison)
    # ------------------------------------------------------------------

    async def record_engine_decisions(
        self,
        engine_name: str,
        decisions: list,
        headline_id: Optional[str] = None,
        signal_latency_ms: Optional[float] = None,
    ) -> None:
        """Bulk-insert all TradeDecision objects from one engine run."""
        if not decisions:
            return
        now = _now_iso()
        rows = [
            (
                engine_name,
                d.condition_id,
                d.question,
                d.direction.value,
                d.confidence,
                d.similarity,
                d.p_hat,
                d.mid_price,
                d.edge,
                d.size_usdc,
                1 if d.executed else 0,
                d.skip_reason,
                now,
                headline_id,
                signal_latency_ms,
            )
            for d in decisions
        ]
        sql = """
            INSERT INTO engine_results
              (engine_name, condition_id, question, direction, confidence,
               similarity, p_hat, mid_price, edge, size_usdc, executed,
               skip_reason, decided_at, headline_id, signal_latency_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        async with self._write_lock():
            try:
                await self._conn.executemany(sql, rows)
                await self._commit_if_unbatched()
                self._bump_ok("record_engine_decisions", "engine_results")
                if _metrics is not None:
                    try:
                        _metrics.incr(
                            "db.record_engine_decisions.rows",
                            n=len(rows),
                            tags={"engine": engine_name.split("/")[0]},
                        )
                    except Exception:  # noqa: BLE001
                        pass
            except Exception as exc:  # noqa: BLE001
                # R6 Phase 1: per-row fallback. executemany is atomic — if one
                # row violates a constraint, none commit. Retry as N individual
                # INSERTs so good rows still land. The first failure is logged
                # at warning; subsequent ones bump a counter only to avoid log
                # spam under pathological data.
                logger.warning(
                    "[db] record_engine_decisions batch failed for engine=%s "
                    "n_rows=%d: %r — retrying per-row",
                    engine_name, len(rows), exc,
                )
                self._bump_err("record_engine_decisions", exc)
                ok_n = 0
                err_n = 0
                first_err: Optional[BaseException] = None
                for row in rows:
                    try:
                        await self._conn.execute(sql, row)
                        ok_n += 1
                    except Exception as row_exc:  # noqa: BLE001
                        err_n += 1
                        if first_err is None:
                            first_err = row_exc
                            logger.warning(
                                "[db] record_engine_decisions per-row INSERT failed "
                                "engine=%s cond=%s: %r",
                                engine_name, row[1], row_exc,
                            )
                try:
                    await self._commit_if_unbatched()
                except Exception as c_exc:  # noqa: BLE001
                    logger.error(
                        "[db] record_engine_decisions per-row fallback commit failed: %r",
                        c_exc, exc_info=True,
                    )
                if _metrics is not None:
                    try:
                        _metrics.incr(
                            "db.record_engine_decisions.rows",
                            n=ok_n,
                            tags={"engine": engine_name.split("/")[0]},
                        )
                        if err_n:
                            _metrics.incr(
                                "db.record_engine_decisions.err_row",
                                n=err_n,
                                tags={"engine": engine_name.split("/")[0]},
                            )
                    except Exception:  # noqa: BLE001
                        pass
                if ok_n:
                    self._last_insert_at["engine_results"] = datetime.now(timezone.utc)

    async def get_engine_summary(self) -> list[dict]:
        """
        Per-engine summary: trades executed, total staked, skip breakdown.
        Used by the dashboard engine_comparison.txt file.
        """
        async with self._conn.execute(
            """
            SELECT
                engine_name,
                COUNT(*)                                        AS total_decisions,
                SUM(executed)                                   AS executed_count,
                COUNT(*) - SUM(executed)                        AS skipped_count,
                COALESCE(SUM(CASE WHEN executed=1 THEN size_usdc END), 0) AS total_staked,
                COALESCE(AVG(CASE WHEN executed=1 THEN edge END), 0)      AS avg_edge,
                COALESCE(AVG(CASE WHEN executed=1 THEN confidence END), 0) AS avg_confidence,
                COALESCE(AVG(signal_latency_ms), 0)             AS avg_latency_ms,
                MIN(decided_at)                                 AS first_trade,
                MAX(decided_at)                                 AS last_trade
            FROM engine_results
            GROUP BY engine_name
            ORDER BY executed_count DESC
            """
        ) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def get_scout_trader_traces(
        self,
        *,
        engine_prefix: str = "scout",
        since_ts: Optional[str] = None,
        limit: int = 200_000,
    ) -> list[dict]:
        """Fetch trader_trace rows for the scout/probe engine's decision funnel.

        Column set matches `scout_funnel_core.SCOUT_FUNNEL_COLUMNS` so both
        the live dashboard tile and the CLI analyzer share one parser.
        Cycle-scope `since_ts` (or use a window like 1h) to keep the scan
        cheap on the multi-million-row trades table.
        """
        where = "WHERE engine_name LIKE ?"
        params: tuple = (f"{engine_prefix}%",)
        if since_ts:
            where += " AND decided_at >= ?"
            params = (f"{engine_prefix}%", since_ts)
        async with self._conn.execute(
            f"""
            SELECT decided_at, engine_name, direction, confidence_raw,
                   signed_edge, mid_yes, size_usdc, skip_reason,
                   trader_trace, realized_pnl, closed_at
            FROM trades
            {where}
            ORDER BY decided_at
            LIMIT ?
            """,
            params + (limit,),
        ) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def get_variant_stats(self, since_ts: Optional[str] = None) -> list[dict]:
        """
        Per-variant rollup for variant_compare.txt.

        Rows include only executed trades; wins/losses come from realized_pnl
        on closed rows. `variant_name` falls back to `engine_name` for legacy
        rows written before the variant column existed.
        """
        where = "WHERE size_usdc > 0 AND skip_reason IS NULL"
        params: tuple = ()
        if since_ts:
            where += " AND decided_at >= ?"
            params = (since_ts,)
        # M5: extend with avg_hold_seconds and last_trade_at so the dashboard
        # can render time-since-last-trade. realized-pnl-only avg_hold to
        # avoid skewing on still-open positions.
        async with self._conn.execute(
            f"""
            SELECT
                COALESCE(variant_name, engine_name, 'default')  AS variant,
                COUNT(*)                                        AS n_trades,
                SUM(CASE WHEN realized_pnl IS NULL THEN 1 ELSE 0 END)   AS n_open,
                SUM(CASE WHEN realized_pnl > 0  THEN 1 ELSE 0 END)      AS n_wins,
                SUM(CASE WHEN realized_pnl <= 0 AND realized_pnl IS NOT NULL THEN 1 ELSE 0 END)
                                                                  AS n_losses,
                COALESCE(SUM(realized_pnl), 0)                  AS sum_pnl,
                COALESCE(AVG(realized_pnl), 0)                  AS avg_pnl,
                COALESCE(AVG(confidence_raw), 0)                AS avg_conf,
                COALESCE(AVG(signed_edge), 0)                   AS avg_edge,
                COALESCE(SUM(size_usdc), 0)                     AS total_staked,
                SUM(CASE WHEN inverted = 1 THEN 1 ELSE 0 END)   AS n_inverted,
                COALESCE(AVG(
                  CASE WHEN closed_at IS NOT NULL
                       THEN (julianday(closed_at) - julianday(decided_at)) * 86400.0
                  END
                ), 0)                                            AS avg_hold_seconds,
                MAX(decided_at)                                  AS last_trade_at,
                MAX(closed_at)                                   AS last_close_at
            FROM trades
            {where}
            GROUP BY variant
            ORDER BY sum_pnl DESC
            """,
            params,
        ) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def get_top_wins_losses(
        self, limit: int = 50, since_ts: Optional[str] = None,
    ) -> tuple[list[dict], list[dict]]:
        """Return (wins, losses) sorted by realized_pnl magnitude."""
        cols = (
            "id, signal_id, headline_id, "
            "COALESCE(variant_name, engine_name, 'default') AS variant, "
            "engine_name, question, direction, similarity, confidence_raw AS confidence, "
            "p_alpha, agreement, signed_edge, mid_yes, mid_no, mid_price AS entry_price, "
            "size_usdc, realized_pnl, close_reason, inverted, ai_reason, "
            "decided_at, closed_at"
        )
        where_extra = ""
        params: list = []
        if since_ts:
            where_extra = " AND decided_at >= ?"
            params.append(since_ts)
        async with self._conn.execute(
            f"""SELECT {cols} FROM trades
               WHERE realized_pnl IS NOT NULL AND realized_pnl > 0{where_extra}
               ORDER BY realized_pnl DESC LIMIT ?""",
            (*params, limit),
        ) as cur:
            wins = [dict(r) for r in await cur.fetchall()]
        async with self._conn.execute(
            f"""SELECT {cols} FROM trades
               WHERE realized_pnl IS NOT NULL AND realized_pnl <= 0{where_extra}
               ORDER BY realized_pnl ASC LIMIT ?""",
            (*params, limit),
        ) as cur:
            losses = [dict(r) for r in await cur.fetchall()]
        return wins, losses

    async def get_closed_trades_since(self, last_id: int) -> list[dict]:
        """
        Append-only feed for trade_log.jsonl. Returns all closed-trade rows
        with id > last_id, joined with the headline text from `signals`.
        """
        async with self._conn.execute(
            """
            SELECT
                t.id, t.signal_id, t.headline_id,
                COALESCE(t.variant_name, t.engine_name, 'default') AS variant_name,
                t.engine_name,
                s.site_url, s.headline,
                t.question, t.direction, t.similarity,
                t.confidence_raw, t.p_alpha, t.agreement, t.signed_edge,
                t.mid_yes, t.mid_no, t.mid_price AS entry_price,
                t.size_usdc, t.realized_pnl, t.close_reason, t.inverted,
                t.ai_reason, t.ai_prompt_meta, t.trader_trace,
                t.decided_at, t.closed_at
            FROM trades t
            LEFT JOIN signals s ON s.id = t.signal_id
            WHERE t.realized_pnl IS NOT NULL AND t.id > ?
            ORDER BY t.id ASC
            LIMIT 500
            """,
            (last_id,),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]

    async def get_headline_summary(
        self, limit: int = 100, since_ts: Optional[str] = None,
    ) -> list[dict]:
        """
        One row per headline that produced at least one trade. Joins price
        impact context (t0/t5m/t60m for the first impact row of that headline)
        so the journal shows the news + the market reaction in one line.
        """
        where_extra = ""
        params: list = []
        if since_ts:
            where_extra = "WHERE s.detected_at >= ?"
            params.append(since_ts)
        async with self._conn.execute(
            f"""
            SELECT
                s.id              AS signal_id,
                s.headline_id,
                s.headline,
                s.site_url,
                s.detected_at,
                COUNT(t.id)                                                  AS n_trades,
                SUM(CASE WHEN t.size_usdc > 0 AND t.skip_reason IS NULL THEN 1 ELSE 0 END)
                                                                              AS n_executed,
                COUNT(DISTINCT COALESCE(t.variant_name, t.engine_name))      AS n_variants,
                COALESCE(SUM(t.realized_pnl), 0)                             AS sum_pnl,
                (SELECT price_t0  FROM price_impacts pi WHERE pi.headline_id = s.headline_id LIMIT 1)
                                                                              AS price_t0,
                (SELECT price_t5m FROM price_impacts pi WHERE pi.headline_id = s.headline_id LIMIT 1)
                                                                              AS price_t5m,
                (SELECT price_t60m FROM price_impacts pi WHERE pi.headline_id = s.headline_id LIMIT 1)
                                                                              AS price_t60m
            FROM signals s
            LEFT JOIN trades t ON t.signal_id = s.id
            {where_extra}
            GROUP BY s.id
            HAVING n_trades > 0
            ORDER BY s.detected_at DESC
            LIMIT ?
            """,
            (*params, limit),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]

    async def get_headline_winners(
        self, limit: int = 30, min_variants: int = 2,
    ) -> list[dict]:
        """Headlines where ≥min_variants traded, ranked by sum_pnl."""
        async with self._conn.execute(
            """
            SELECT
                s.headline_id,
                s.headline,
                s.site_url,
                s.detected_at,
                COUNT(DISTINCT COALESCE(t.variant_name, t.engine_name)) AS n_variants,
                SUM(CASE WHEN t.size_usdc > 0 AND t.skip_reason IS NULL THEN 1 ELSE 0 END)
                                                                         AS n_executed,
                COALESCE(SUM(t.realized_pnl), 0)                        AS sum_pnl,
                SUM(CASE WHEN t.realized_pnl > 0 THEN 1 ELSE 0 END)     AS n_wins,
                SUM(CASE WHEN t.realized_pnl < 0 THEN 1 ELSE 0 END)     AS n_losses
            FROM signals s
            JOIN trades t ON t.signal_id = s.id
            GROUP BY s.id
            HAVING n_variants >= ? AND n_executed > 0
            ORDER BY sum_pnl DESC
            LIMIT ?
            """,
            (min_variants, limit),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]

    async def get_recent_neutral_rate(self, hours: float = 1.0) -> tuple[Optional[float], int]:
        """
        Return (neutral_fraction, total_decisions) over the last `hours`.

        Used by the SYSTEM HEALTH dashboard block to flag "AI signal starved"
        (e.g. 88% NEUTRAL) without needing a full analysis run.
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        async with self._conn.execute(
            """
            SELECT
                SUM(CASE WHEN direction='NEUTRAL' THEN 1 ELSE 0 END) AS neutral_n,
                COUNT(*) AS total_n
            FROM engine_results
            WHERE decided_at >= ?
            """,
            (cutoff,),
        ) as cur:
            row = await cur.fetchone()
        if not row or not row["total_n"]:
            return (None, 0)
        return (row["neutral_n"] / row["total_n"], int(row["total_n"]))

    async def get_decision_funnel(self, since_secs: int = 3600) -> dict:
        """
        Guard-by-guard decision funnel over the last `since_secs` window.

        Returns a dict keyed by funnel step (ordered as the trader evaluates):
          {
            "total":                int,
            "ai_neutral":           int,
            "pre_ai_dropped":       int,
            "failed_min_confidence":int,
            "failed_agreement":     int,
            "failed_post_ai_rule":  int,
            "failed_rail":          int,
            "failed_edge":          int,
            "failed_position_guard":int,
            "failed_size":          int,
            "other_skipped":        int,
            "executed":             int,
          }

        Drives the DECISION FUNNEL block in dashboard.txt — the operator can
        see at a glance which gate is killing trades (e.g. AI signal starved
        vs ensemble-disagreement bleed vs balance exhaustion).

        Implementation note: classifies each `trades` row by `skip_reason`
        prefix. The trader writes skip_reason values whose prefixes match the
        trace step names exactly (see `_evaluate_market` in app/trader.py),
        so no JSON parsing is needed.
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=since_secs)).isoformat()
        async with self._conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN skip_reason LIKE 'freshness:%' THEN 1 ELSE 0 END)
                                                                    AS failed_freshness,
                SUM(CASE WHEN skip_reason LIKE 'not_relevant%' THEN 1 ELSE 0 END)
                                                                    AS failed_relevance,
                SUM(CASE WHEN skip_reason LIKE 'low_similarity%' THEN 1 ELSE 0 END)
                                                                    AS failed_low_sim,
                SUM(CASE WHEN skip_reason = 'neutral' THEN 1 ELSE 0 END)
                                                                    AS ai_neutral,
                SUM(CASE WHEN skip_reason LIKE 'pre_ai_rule%'
                          OR skip_reason LIKE 'RULE_%' THEN 1 ELSE 0 END)
                                                                    AS pre_ai_dropped,
                SUM(CASE WHEN skip_reason LIKE 'low_confidence%' THEN 1 ELSE 0 END)
                                                                    AS failed_min_confidence,
                SUM(CASE WHEN skip_reason LIKE 'low_agreement%' THEN 1 ELSE 0 END)
                                                                    AS failed_agreement,
                SUM(CASE WHEN skip_reason LIKE 'post_ai_rule%' THEN 1 ELSE 0 END)
                                                                    AS failed_post_ai_rule,
                SUM(CASE WHEN skip_reason LIKE 'hard_rail%'
                          OR skip_reason LIKE 'rail_price%'
                          OR skip_reason IN ('no_token','no_price')
                          THEN 1 ELSE 0 END)                       AS failed_rail,
                SUM(CASE WHEN skip_reason LIKE 'min_entry_yes_mid%' THEN 1 ELSE 0 END)
                                                                    AS failed_min_entry,
                SUM(CASE WHEN skip_reason LIKE 'market_too_noisy%' THEN 1 ELSE 0 END)
                                                                    AS failed_market_vol,
                SUM(CASE WHEN skip_reason LIKE 'edge_too_small%' THEN 1 ELSE 0 END)
                                                                    AS failed_edge,
                SUM(CASE WHEN skip_reason LIKE 'max_positions%'
                          OR skip_reason = 'duplicate_position'
                          OR skip_reason LIKE 'max_unique_markets%'
                          THEN 1 ELSE 0 END)                       AS failed_position_guard,
                SUM(CASE WHEN skip_reason LIKE 'size_too_small%'
                          OR skip_reason = 'no_balance'
                          THEN 1 ELSE 0 END)                       AS failed_size,
                SUM(CASE WHEN size_usdc > 0 AND skip_reason IS NULL THEN 1 ELSE 0 END)
                                                                    AS executed
            FROM trades
            WHERE decided_at >= ?
            """,
            (cutoff,),
        ) as cur:
            row = await cur.fetchone()
        if row is None:
            return {"total": 0, "failed_freshness": 0, "failed_relevance": 0,
                    "failed_low_sim": 0,
                    "ai_neutral": 0, "pre_ai_dropped": 0,
                    "failed_min_confidence": 0, "failed_agreement": 0,
                    "failed_post_ai_rule": 0, "failed_rail": 0,
                    "failed_min_entry": 0, "failed_market_vol": 0,
                    "failed_edge": 0, "failed_position_guard": 0,
                    "failed_size": 0, "other_skipped": 0, "executed": 0}
        d = dict(row)
        # Coerce SUM(NULL) → 0 so callers can format safely.
        for k, v in list(d.items()):
            d[k] = int(v or 0)
        accounted = (
            d["failed_freshness"] + d["failed_relevance"] + d["failed_low_sim"]
            + d["ai_neutral"] + d["pre_ai_dropped"] + d["failed_min_confidence"]
            + d["failed_agreement"] + d["failed_post_ai_rule"] + d["failed_rail"]
            + d["failed_min_entry"] + d["failed_market_vol"]
            + d["failed_edge"] + d["failed_position_guard"] + d["failed_size"]
            + d["executed"]
        )
        d["other_skipped"] = max(0, d["total"] - accounted)
        return d

    async def get_freshness_counts(self, since_secs: int = 3600) -> list[dict]:
        """Per-label headline counts over the last window, from `signals`."""
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=since_secs)).isoformat()
        async with self._conn.execute(
            """
            SELECT
                COALESCE(freshness_label, 'UNLABELED') AS label,
                COUNT(*)                               AS n
            FROM signals
            WHERE detected_at >= ?
            GROUP BY label
            ORDER BY n DESC
            """,
            (cutoff,),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]

    async def get_freshness_buckets(self, since_secs: int = 86400) -> list[dict]:
        """Per-freshness trade outcome rollup. Mirrors get_confidence_buckets."""
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=since_secs)).isoformat()
        async with self._conn.execute(
            """
            SELECT
                COALESCE(freshness_label, 'UNLABELED') AS label,
                SUM(CASE WHEN size_usdc > 0 AND skip_reason IS NULL THEN 1 ELSE 0 END)
                                                                  AS n_executed,
                SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) AS n_wins,
                SUM(CASE WHEN realized_pnl <= 0 AND realized_pnl IS NOT NULL THEN 1 ELSE 0 END)
                                                                  AS n_losses,
                SUM(CASE WHEN skip_reason IS NOT NULL THEN 1 ELSE 0 END)
                                                                  AS n_skipped,
                COALESCE(AVG(CASE WHEN realized_pnl IS NOT NULL THEN realized_pnl END), 0)
                                                                  AS avg_pnl,
                COALESCE(SUM(realized_pnl), 0)                    AS sum_pnl
            FROM trades
            WHERE decided_at >= ?
            GROUP BY label
            """,
            (cutoff,),
        ) as cur:
            rows = await cur.fetchall()
        order = {"BREAKING": 0, "EVOLVING": 1, "STALE_RETURN": 2,
                 "REHASH": 3, "UNLABELED": 4}
        out = []
        for r in rows:
            d = dict(r)
            if (d.get("n_executed") or 0) == 0 and (d.get("n_skipped") or 0) == 0:
                continue
            out.append(d)
        out.sort(key=lambda r: order.get(r["label"], 99))
        return out

    async def get_confidence_buckets(self, since_secs: int = 86400) -> list[dict]:
        """
        Hit-rate and avg PnL by AI confidence bucket over the last window.

        Buckets: [0.0,0.4), [0.4,0.55), [0.55,0.7), [0.7,0.85), [0.85,1.0].
        Returns one dict per non-empty bucket, ordered high→low:
          { "bucket": "0.85-1.0", "lo": 0.85, "hi": 1.0,
            "n_executed": int, "n_wins": int, "n_losses": int,
            "n_skipped": int, "avg_pnl": float, "sum_pnl": float,
            "avg_edge": float }

        Powers CONFIDENCE BUCKETS block in dashboard.txt — answers "are
        high-conf calls actually profitable?" without an analysis run.
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=since_secs)).isoformat()
        async with self._conn.execute(
            """
            SELECT
                CASE
                    WHEN confidence_raw >= 0.85 THEN '0.85-1.0'
                    WHEN confidence_raw >= 0.70 THEN '0.70-0.85'
                    WHEN confidence_raw >= 0.55 THEN '0.55-0.70'
                    WHEN confidence_raw >= 0.40 THEN '0.40-0.55'
                    ELSE '0.00-0.40'
                END                                              AS bucket,
                SUM(CASE WHEN size_usdc > 0 AND skip_reason IS NULL THEN 1 ELSE 0 END)
                                                                  AS n_executed,
                SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) AS n_wins,
                SUM(CASE WHEN realized_pnl <= 0 AND realized_pnl IS NOT NULL THEN 1 ELSE 0 END)
                                                                  AS n_losses,
                SUM(CASE WHEN skip_reason IS NOT NULL THEN 1 ELSE 0 END)
                                                                  AS n_skipped,
                COALESCE(AVG(CASE WHEN realized_pnl IS NOT NULL THEN realized_pnl END), 0)
                                                                  AS avg_pnl,
                COALESCE(SUM(realized_pnl), 0)                    AS sum_pnl,
                COALESCE(AVG(CASE WHEN size_usdc > 0 AND skip_reason IS NULL
                                  THEN signed_edge END), 0)       AS avg_edge
            FROM trades
            WHERE decided_at >= ? AND confidence_raw IS NOT NULL
            GROUP BY bucket
            """,
            (cutoff,),
        ) as cur:
            rows = await cur.fetchall()
        # Order buckets high→low, drop empty ones for compact display
        order = {"0.85-1.0": 0, "0.70-0.85": 1, "0.55-0.70": 2,
                 "0.40-0.55": 3, "0.00-0.40": 4}
        edges = {"0.85-1.0": (0.85, 1.0), "0.70-0.85": (0.70, 0.85),
                 "0.55-0.70": (0.55, 0.70), "0.40-0.55": (0.40, 0.55),
                 "0.00-0.40": (0.00, 0.40)}
        out = []
        for r in rows:
            d = dict(r)
            if (d.get("n_executed") or 0) == 0 and (d.get("n_skipped") or 0) == 0:
                continue
            lo, hi = edges[d["bucket"]]
            d["lo"] = lo
            d["hi"] = hi
            out.append(d)
        out.sort(key=lambda r: order[r["bucket"]])
        return out

    async def get_last_signal_at(self) -> Optional[datetime]:
        """Wall-clock time of the most recent inserted signal (DB-write liveness)."""
        async with self._conn.execute(
            "SELECT detected_at FROM signals ORDER BY id DESC LIMIT 1"
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return None
        try:
            dt = datetime.fromisoformat(row["detected_at"])
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None

    async def get_engine_skip_reasons(self) -> list[dict]:
        """Per-engine skip reason breakdown."""
        async with self._conn.execute(
            """
            SELECT engine_name, skip_reason, COUNT(*) AS count
            FROM engine_results
            WHERE executed = 0
            GROUP BY engine_name, skip_reason
            ORDER BY engine_name, count DESC
            """
        ) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # Price impact tracking
    # ------------------------------------------------------------------

    async def insert_price_impact(
        self,
        headline_id: str,
        headline: str,
        site_url: str,
        condition_id: str,
        question: str,
        token_id: str,
        direction: str,
        ai_confidence: float,
        similarity: float,
        price_t0: Optional[float],
        detected_at: str,
        source: str = "news",
        price_pre_60s: Optional[float] = None,
        price_pre_300s: Optional[float] = None,
    ) -> int:
        """Create a new price impact row. Returns the row id."""
        d60 = (price_t0 - price_pre_60s) if (price_t0 is not None and price_pre_60s is not None) else None
        d300 = (price_t0 - price_pre_300s) if (price_t0 is not None and price_pre_300s is not None) else None
        async with self._conn.execute(
            """
            INSERT INTO price_impacts
              (headline_id, headline, site_url, condition_id, question,
               token_id, direction, ai_confidence, similarity,
               price_t0, detected_at, source,
               price_pre_60s, price_pre_300s, delta_pre_60s, delta_pre_300s)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (headline_id, headline, site_url, condition_id, question,
             token_id, direction, ai_confidence, similarity,
             price_t0, detected_at, source,
             price_pre_60s, price_pre_300s, d60, d300),
        ) as cur:
            row_id = cur.lastrowid
        await self._commit_if_unbatched()
        return row_id

    async def update_price_impact_snapshot(
        self, row_id: int, bucket: str, price: float
    ) -> None:
        """
        Update one time-bucket column (t1m, t5m, t15m, t30m, t60m) and its delta.

        bucket must be one of: "t1m", "t5m", "t15m", "t30m", "t60m"
        """
        assert bucket in ("t1m", "t5m", "t15m", "t30m", "t60m")
        await self._conn.execute(
            f"""
            UPDATE price_impacts
            SET price_{bucket} = ?,
                delta_{bucket} = ? - price_t0
            WHERE id = ?
            """,
            (price, price, row_id),
        )
        await self._commit_if_unbatched()

    # ------------------------------------------------------------------
    # Full price time-series (diagnostic, not used for live trading)
    # ------------------------------------------------------------------

    async def get_impacts_needing_timeseries(
        self, min_age_seconds: int = 3900, limit: int = 50
    ) -> list[dict]:
        """
        Rows where detected_at is at least `min_age_seconds` old AND
        timeseries_backfilled_at IS NULL. These are ready for full backfill.

        NOTE on the date comparison: detected_at is stored as the Python ISO
        string (e.g. 2026-05-13T04:28:50.342732+00:00) — note the T separator
        and explicit +00:00 offset. SQLite's `datetime('now', ...)` returns
        'YYYY-MM-DD HH:MM:SS' with a space, which sorts lexicographically
        AFTER the same instant written with 'T'. We build the threshold here
        in matching format so the string compare actually works.
        """
        threshold = (
            datetime.now(timezone.utc) - timedelta(seconds=int(min_age_seconds))
        ).isoformat()
        async with self._conn.execute(
            """
            SELECT id, headline_id, condition_id, token_id, detected_at
            FROM price_impacts
            WHERE timeseries_backfilled_at IS NULL
              AND detected_at <= ?
            ORDER BY detected_at ASC
            LIMIT ?
            """,
            (threshold, limit),
        ) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def insert_price_timeseries_batch(
        self, rows: list[tuple[int, str, str, str, int, float, int]]
    ) -> int:
        """
        Bulk insert minute samples. Each row:
          (impact_id, headline_id, condition_id, token_id, t_unix, price, relative_s)
        Returns count of newly inserted rows (duplicates ignored via UNIQUE).
        """
        if not rows:
            return 0
        await self._conn.executemany(
            """
            INSERT OR IGNORE INTO price_timeseries
              (impact_id, headline_id, condition_id, token_id, t_unix, price, relative_s)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        await self._commit_if_unbatched()
        return len(rows)

    async def get_plot_candidate_impacts(
        self, limit: int = 200, min_samples: int = 30,
    ) -> list[dict]:
        """
        Return price_impacts rows that have:
          - full timeseries backfilled, AND
          - at least `min_samples` samples in price_timeseries.

        Each row includes the impact fields, the earliest matching
        signal.detected_at for the same headline (when we *saw* it), and the
        earliest matching trade.decided_at / closed_at / realized_pnl /
        engine_name for that headline+token (when we *acted* and *exited*).

        Used by the plotter to draw example trade charts. Most recent first.
        """
        async with self._conn.execute(
            """
            SELECT
                pi.*,
                (SELECT MIN(s.detected_at)
                   FROM signals s
                   WHERE s.headline_id = pi.headline_id) AS seen_at,
                t.decided_at      AS trade_decided_at,
                t.closed_at       AS trade_closed_at,
                t.realized_pnl    AS trade_realized_pnl,
                t.direction       AS trade_direction,
                t.size_usdc       AS trade_size_usdc,
                t.mid_price       AS trade_entry_price,
                (SELECT COUNT(*) FROM price_timeseries pts WHERE pts.impact_id = pi.id) AS samples
            FROM price_impacts pi
            LEFT JOIN trades t
                  ON t.headline_id = pi.headline_id
                 AND t.token_id    = pi.token_id
                 AND t.skip_reason IS NULL
            WHERE pi.timeseries_backfilled_at IS NOT NULL
            ORDER BY pi.detected_at DESC
            LIMIT ?
            """,
            (limit,),
        ) as cur:
            rows = await cur.fetchall()
        out = []
        for row in rows:
            d = dict(row)
            if (d.get("samples") or 0) < min_samples:
                continue
            out.append(d)
        return out

    async def get_timeseries_for_impact(self, impact_id: int) -> list[dict]:
        """All minute samples for one impact row, ordered by t_unix ascending."""
        async with self._conn.execute(
            """
            SELECT t_unix, price, relative_s
            FROM price_timeseries
            WHERE impact_id = ?
            ORDER BY t_unix ASC
            """,
            (impact_id,),
        ) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def mark_timeseries_backfilled(self, impact_id: int) -> None:
        await self._conn.execute(
            "UPDATE price_impacts SET timeseries_backfilled_at = ? WHERE id = ?",
            (_now_iso(), impact_id),
        )
        await self._commit_if_unbatched()

    # ------------------------------------------------------------------
    # Article bodies (headline first-paragraph capture)
    # ------------------------------------------------------------------

    async def upsert_headline_article(
        self,
        headline_id: str,
        article_url: str,
        site_url: Optional[str],
        body: Optional[str],
        fetch_error: Optional[str] = None,
    ) -> None:
        await self._conn.execute(
            """
            INSERT INTO headline_articles
              (headline_id, article_url, site_url, body, body_chars, fetched_at, fetch_error)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(headline_id) DO UPDATE SET
              body        = excluded.body,
              body_chars  = excluded.body_chars,
              fetched_at  = excluded.fetched_at,
              fetch_error = excluded.fetch_error
            """,
            (
                headline_id, article_url, site_url, body,
                len(body) if body else 0, _now_iso(), fetch_error,
            ),
        )
        await self._commit_if_unbatched()

    async def get_pending_price_impacts(self) -> list[dict]:
        """Return rows that still have at least one bucket unfilled (up to 2h old)."""
        async with self._conn.execute(
            """
            SELECT * FROM price_impacts
            WHERE (price_t60m IS NULL)
              AND detected_at > datetime('now', '-2 hours')
            ORDER BY detected_at DESC
            LIMIT 500
            """
        ) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def get_price_impact_stats(self, limit: int = 500) -> list[dict]:
        """Return completed rows (price_t60m is not null) for analysis."""
        async with self._conn.execute(
            """
            SELECT * FROM price_impacts
            WHERE price_t60m IS NOT NULL
            ORDER BY detected_at DESC LIMIT ?
            """,
            (limit,),
        ) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def get_engine_recent_trades(
        self, engine_name: str, limit: int = 20
    ) -> list[dict]:
        async with self._conn.execute(
            """
            SELECT * FROM engine_results
            WHERE engine_name = ? AND executed = 1
            ORDER BY decided_at DESC LIMIT ?
            """,
            (engine_name, limit),
        ) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def get_engine_rolling_hit_rate(
        self, engine_name: str, limit: int = 50
    ) -> tuple[Optional[float], int]:
        """Return (hit_rate, n) over the last `limit` closed trades for an engine.

        A trade counts as a hit when realized_pnl > 0. Open trades (NULL
        realized_pnl) are excluded. Returns (None, 0) if no closed trades yet.
        Used by EngineRunner for the E.5b variant-pause guard.
        """
        async with self._conn.execute(
            """
            SELECT realized_pnl FROM trades
            WHERE engine_name = ? AND realized_pnl IS NOT NULL
            ORDER BY closed_at DESC LIMIT ?
            """,
            (engine_name, limit),
        ) as cur:
            rows = await cur.fetchall()
        n = len(rows)
        if n == 0:
            return None, 0
        hits = sum(1 for r in rows if (r[0] or 0.0) > 0.0)
        return hits / n, n

    # ------------------------------------------------------------------
    # Market price snapshots (Part 2.1 — backtest corpus)
    # ------------------------------------------------------------------

    async def insert_market_price_snaps(
        self,
        snaps: list[tuple[str, str, str, float, int, str, Optional[str]]],
    ) -> None:
        """Bulk-insert mid-price snapshots for matched markets.

        Each tuple: (condition_id, token_id, side, price, t_unix, source,
        headline_id). `side` is 'yes' or 'no' — the side fetched (the
        other side is inferred via 1 − price in binary markets).

        Called from `engine._prefetch_prices` after fetching mids. One
        row per (market, headline) so the backtest framework can ask
        "what was the price for condition X at time T?" for any market
        the bot has ever looked at — not just markets where a trade
        executed. Complements `price_timeseries` (executed-trade-only).
        """
        if not snaps:
            return
        async with self._write_lock():
            try:
                await self._conn.executemany(
                    "INSERT INTO market_price_snaps "
                    "(condition_id, token_id, side, price, t_unix, source, headline_id) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    snaps,
                )
                await self._commit_if_unbatched()
                self._bump_ok("insert_market_price_snaps", "market_price_snaps")
            except Exception as exc:
                self._bump_err("insert_market_price_snaps", exc)
                raise

    # ------------------------------------------------------------------
    # Market heat tracking
    # ------------------------------------------------------------------

    async def bump_market_heat(self, condition_ids: list[str]) -> None:
        """Increment match_count and update last_matched_at for matched markets."""
        if not condition_ids:
            return
        now = _now_iso()
        async with self._write_lock():
            try:
                await self._conn.executemany(
                    "UPDATE markets SET match_count = match_count + 1, last_matched_at = ? WHERE condition_id = ?",
                    [(now, cid) for cid in condition_ids],
                )
                await self._commit_if_unbatched()
                self._bump_ok("bump_market_heat", "markets")
            except Exception as exc:
                self._bump_err("bump_market_heat", exc)
                raise

    async def count_unique_open_markets(self, engine_name: Optional[str] = None) -> int:
        """Return number of distinct condition_ids with open positions (optionally per engine)."""
        if engine_name is None:
            async with self._conn.execute(
                "SELECT COUNT(DISTINCT condition_id) FROM positions"
            ) as cur:
                row = await cur.fetchone()
                return row[0] if row else 0
        async with self._conn.execute(
            "SELECT COUNT(DISTINCT condition_id) FROM positions WHERE engine_name = ?",
            (engine_name,),
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0

    async def get_cold_losing_positions(
        self,
        cold_threshold_hours: float = 12.0,
        engine_name: Optional[str] = None,
    ) -> list[dict]:
        """Return losing positions in markets that haven't matched any headline recently."""
        sql = """
            SELECT p.trade_id, p.condition_id, p.token_id, p.direction,
                   p.size_usdc, p.entry_price, p.unrealized_pnl, p.engine_name,
                   p.shares, m.market_type
            FROM positions p
            LEFT JOIN markets m ON p.condition_id = m.condition_id
            WHERE p.unrealized_pnl < 0
              AND (m.last_matched_at IS NULL
                   OR m.last_matched_at < datetime('now', ? || ' hours'))
        """
        params: tuple = (f"-{cold_threshold_hours:.0f}",)
        if engine_name is not None:
            sql += " AND p.engine_name = ?"
            params = params + (engine_name,)
        async with self._conn.execute(sql, params) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def get_quant_stats(self, since_hours: Optional[float] = None) -> dict:
        """
        Return prediction accuracy and signal latency stats for quant analysis.

        If `since_hours` is set, only consider rows newer than that window
        (used to produce a rolling 24h column alongside all-time).
        """
        time_clause = (
            f" AND detected_at > datetime('now', '-{since_hours:.0f} hours')"
            if since_hours is not None else ""
        )
        lat_time_clause = (
            f" AND decided_at > datetime('now', '-{since_hours:.0f} hours')"
            if since_hours is not None else ""
        )
        async with self._conn.execute(
            f"""
            SELECT direction,
                   COUNT(*) AS n,
                   SUM(CASE WHEN (direction='YES' AND delta_t5m > 0)
                                 OR (direction='NO' AND delta_t5m < 0) THEN 1 ELSE 0 END) AS hits,
                   AVG(delta_t5m) AS avg_delta_t5m,
                   AVG(ABS(delta_t5m)) AS avg_abs_delta_t5m
            FROM price_impacts
            WHERE delta_t5m IS NOT NULL AND direction IN ('YES','NO')
            {time_clause}
            GROUP BY direction
            """
        ) as cur:
            impact = [dict(r) for r in await cur.fetchall()]
        async with self._conn.execute(
            f"""
            SELECT AVG(signal_latency_ms) AS avg_ms,
                   MIN(signal_latency_ms) AS min_ms,
                   MAX(signal_latency_ms) AS max_ms,
                   COUNT(signal_latency_ms) AS n
            FROM engine_results
            WHERE signal_latency_ms IS NOT NULL
            {lat_time_clause}
            """
        ) as cur:
            lat = dict(await cur.fetchone() or {})
        return {"impact": impact, "latency": lat}

    # ------------------------------------------------------------------
    # Statistical analysis — calibration, IC, source split, alpha decay
    # ------------------------------------------------------------------

    async def get_calibration_curve(self) -> list[dict]:
        """
        Return hit rate bucketed by confidence decile.
        A well-calibrated model: conf=0.7 → ~70% hit rate.
        """
        async with self._conn.execute(
            """
            SELECT ROUND(ai_confidence * 10) / 10.0 AS conf_bucket,
                   COUNT(*) AS n,
                   SUM(CASE WHEN (direction='YES' AND delta_t5m > 0)
                                 OR (direction='NO' AND delta_t5m < 0)
                            THEN 1 ELSE 0 END) AS hits,
                   AVG(ABS(delta_t5m)) AS avg_abs_delta
            FROM price_impacts
            WHERE delta_t5m IS NOT NULL AND direction IN ('YES','NO')
            GROUP BY conf_bucket
            ORDER BY conf_bucket
            """
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]

    async def get_ic_pairs(self) -> list[tuple[float, float]]:
        """
        Return (signed_confidence, delta_t5m) pairs for IC computation.
        signed_confidence = +conf if direction=YES, -conf if direction=NO.
        Computed in Python (no scipy dep): see dashboard for pearson().
        """
        async with self._conn.execute(
            """
            SELECT direction, ai_confidence, delta_t5m
            FROM price_impacts
            WHERE delta_t5m IS NOT NULL AND direction IN ('YES','NO')
            """
        ) as cur:
            rows = await cur.fetchall()
        pairs = []
        for r in rows:
            signed = r["ai_confidence"] if r["direction"] == "YES" else -r["ai_confidence"]
            pairs.append((signed, r["delta_t5m"]))
        return pairs

    async def get_source_stats(self) -> list[dict]:
        """Per-source precision: news vs twitter vs truth_social."""
        async with self._conn.execute(
            """
            SELECT source,
                   COUNT(*) AS n,
                   SUM(CASE WHEN (direction='YES' AND delta_t5m > 0)
                                 OR (direction='NO' AND delta_t5m < 0)
                            THEN 1 ELSE 0 END) AS hits,
                   AVG(ABS(delta_t5m)) AS avg_abs_delta,
                   AVG(ai_confidence) AS avg_confidence
            FROM price_impacts
            WHERE delta_t5m IS NOT NULL AND direction IN ('YES','NO')
            GROUP BY source
            ORDER BY n DESC
            """
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]

    # ------------------------------------------------------------------
    # Usage tracking — AI tokens + Brave Search calls
    # ------------------------------------------------------------------

    async def log_usage(
        self,
        kind: str,
        count: float,
        provider: Optional[str] = None,
        meta: Optional[str] = None,
    ) -> None:
        """Append a usage event (AI tokens or Brave search). Cheap, batched OK."""
        async with self._write_lock():
            try:
                await self._conn.execute(
                    "INSERT INTO usage_events (ts, kind, provider, count, meta) VALUES (?, ?, ?, ?, ?)",
                    (_now_iso(), kind, provider, count, meta),
                )
                await self._commit_if_unbatched()
                self._bump_ok("log_usage", "usage_events")
            except Exception as exc:
                self._bump_err("log_usage", exc)
                raise

    # ------------------------------------------------------------------
    # FIX-3C: Phase-5 historical-simulation data foundation
    # ------------------------------------------------------------------
    #
    # Three append-only writers feed the replay-harness corpus. Per spec
    # these are fire-and-forget: callers should swallow exceptions at
    # the callsite (engine.py / ai.py) so a capture failure never breaks
    # the live decision path. Each writer still bumps its err counter so
    # silent breakage is observable in db_health.txt.
    #
    # NOT wired into engine.py/ai.py here — that's Phase 2 (A2.1/A2.2).
    # Shipping schema + writers now unblocks Phase 2 without forcing a
    # coordinated migration.

    async def insert_decision_input(
        self,
        *,
        headline_id: str,
        decided_at: Optional[str] = None,
        source_host: Optional[str] = None,
        embedding_bytes: Optional[bytes] = None,
        matches_json: Optional[str] = None,
        freshness_label: Optional[str] = None,
        freshness_dist: Optional[float] = None,
        freshness_age_secs: Optional[float] = None,
        pre_ai_skips_json: Optional[str] = None,
        max_similarity: Optional[float] = None,
    ) -> int:
        """Append one row per signal capturing every input that fed the
        decision. Returns the new row id. Fire-and-forget at callsite."""
        async with self._write_lock():
            try:
                cur = await self._conn.execute(
                    """
                    INSERT INTO decision_inputs
                        (headline_id, decided_at, source_host, embedding_bytes,
                         matches_json, freshness_label, freshness_dist,
                         freshness_age_secs, pre_ai_skips_json, max_similarity)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        headline_id,
                        decided_at or _now_iso(),
                        source_host,
                        embedding_bytes,
                        matches_json,
                        freshness_label,
                        freshness_dist,
                        freshness_age_secs,
                        pre_ai_skips_json,
                        max_similarity,
                    ),
                )
                row_id = cur.lastrowid
                await self._commit_if_unbatched()
                self._bump_ok("insert_decision_input", "decision_inputs")
                return int(row_id) if row_id is not None else 0
            except Exception as exc:
                self._bump_err("insert_decision_input", exc)
                raise

    async def log_llm_call(
        self,
        *,
        ts: Optional[str] = None,
        model: Optional[str] = None,
        prompt_sha: str,
        prompt_tokens: Optional[int] = None,
        completion_tokens: Optional[int] = None,
        response_text: Optional[str] = None,
        parse_ok: bool = True,
        headline_id: Optional[str] = None,
        batch_size: Optional[int] = None,
    ) -> int:
        """Append one row per LLM round-trip. `response_text` should be
        capped at 8KB by the caller. Returns the new row id."""
        async with self._write_lock():
            try:
                cur = await self._conn.execute(
                    """
                    INSERT INTO llm_calls
                        (ts, model, prompt_sha, prompt_tokens, completion_tokens,
                         response_text, parse_ok, headline_id, batch_size)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ts or _now_iso(),
                        model,
                        prompt_sha,
                        prompt_tokens,
                        completion_tokens,
                        response_text,
                        1 if parse_ok else 0,
                        headline_id,
                        batch_size,
                    ),
                )
                row_id = cur.lastrowid
                await self._commit_if_unbatched()
                self._bump_ok("log_llm_call", "llm_calls")
                return int(row_id) if row_id is not None else 0
            except Exception as exc:
                self._bump_err("log_llm_call", exc)
                raise

    async def log_llm_parse_failure(
        self,
        *,
        ts: Optional[str] = None,
        model: Optional[str] = None,
        error_msg: Optional[str] = None,
        response_text: Optional[str] = None,
        headline_id: Optional[str] = None,
    ) -> int:
        """Append one row per LLM parse failure. `response_text` should
        be capped at 4KB by the caller. Returns the new row id."""
        async with self._write_lock():
            try:
                cur = await self._conn.execute(
                    """
                    INSERT INTO llm_parse_failures
                        (ts, model, error_msg, response_text, headline_id)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        ts or _now_iso(),
                        model,
                        error_msg,
                        response_text,
                        headline_id,
                    ),
                )
                row_id = cur.lastrowid
                await self._commit_if_unbatched()
                self._bump_ok("log_llm_parse_failure", "llm_parse_failures")
                return int(row_id) if row_id is not None else 0
            except Exception as exc:
                self._bump_err("log_llm_parse_failure", exc)
                raise

    async def get_usage_totals(self) -> dict:
        """
        Return cumulative usage:
            { 'ai_tokens_total': int, 'brave_search_total': int,
              'ai_by_provider': [{provider,n,tokens}, ...],
              'ai_tokens_24h': int, 'brave_search_24h': int }
        """
        async with self._conn.execute(
            """
            SELECT
              COALESCE(SUM(CASE WHEN kind='ai_tokens'    THEN count END), 0) AS ai_total,
              COALESCE(SUM(CASE WHEN kind='brave_search' THEN count END), 0) AS brave_total,
              COALESCE(SUM(CASE WHEN kind='ai_tokens'    AND ts > datetime('now','-24 hours')
                                THEN count END), 0) AS ai_24h,
              COALESCE(SUM(CASE WHEN kind='brave_search' AND ts > datetime('now','-24 hours')
                                THEN count END), 0) AS brave_24h
            FROM usage_events
            """
        ) as cur:
            totals = dict(await cur.fetchone() or {})

        async with self._conn.execute(
            """
            SELECT provider, COUNT(*) AS n, COALESCE(SUM(count), 0) AS tokens
            FROM usage_events
            WHERE kind = 'ai_tokens'
            GROUP BY provider
            ORDER BY tokens DESC
            """
        ) as cur:
            totals["ai_by_provider"] = [dict(r) for r in await cur.fetchall()]
        return totals

    async def get_alpha_decay(self) -> list[dict]:
        """
        Hit rate at each time bucket. Reveals whether alpha decays fast
        (speed matters) or persists (wide entry window).

        Only rows with delta_t60m filled are included so every bucket
        has the same denominator.
        """
        async with self._conn.execute(
            """
            SELECT direction,
                   COUNT(*) AS n,
                   1.0 * SUM(CASE WHEN (direction='YES' AND delta_t1m > 0)
                                       OR (direction='NO' AND delta_t1m < 0)
                                  THEN 1 ELSE 0 END) / COUNT(*) AS hit_t1m,
                   1.0 * SUM(CASE WHEN (direction='YES' AND delta_t5m > 0)
                                       OR (direction='NO' AND delta_t5m < 0)
                                  THEN 1 ELSE 0 END) / COUNT(*) AS hit_t5m,
                   1.0 * SUM(CASE WHEN (direction='YES' AND delta_t15m > 0)
                                       OR (direction='NO' AND delta_t15m < 0)
                                  THEN 1 ELSE 0 END) / COUNT(*) AS hit_t15m,
                   1.0 * SUM(CASE WHEN (direction='YES' AND delta_t30m > 0)
                                       OR (direction='NO' AND delta_t30m < 0)
                                  THEN 1 ELSE 0 END) / COUNT(*) AS hit_t30m,
                   1.0 * SUM(CASE WHEN (direction='YES' AND delta_t60m > 0)
                                       OR (direction='NO' AND delta_t60m < 0)
                                  THEN 1 ELSE 0 END) / COUNT(*) AS hit_t60m,
                   AVG(ABS(delta_t1m))  AS avg_abs_d1m,
                   AVG(ABS(delta_t5m))  AS avg_abs_d5m,
                   AVG(ABS(delta_t15m)) AS avg_abs_d15m,
                   AVG(ABS(delta_t30m)) AS avg_abs_d30m,
                   AVG(ABS(delta_t60m)) AS avg_abs_d60m
            FROM price_impacts
            WHERE delta_t60m IS NOT NULL AND direction IN ('YES','NO')
            GROUP BY direction
            """
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]

"""
Seulgi dashboard — writes human-readable status files to data/.

Files written (in priority order):
  data/dashboard.txt       — MAIN: balance, open positions, last events, uptime
  data/positions_open.txt  — every live position with entry price and unrealized PnL
  data/trades_executed.txt — last 50 executed paper trades
  data/trades_declined.txt — last 50 declined trade decisions with reason
  data/near_miss.txt       — trades with positive edge that just missed our threshold
  data/news_seen.txt       — last 50 raw headlines detected across all sites
  data/ai_decisions.txt    — last 50 AI classify calls (YES/NO/NEUTRAL + confidence)
  data/watcher_status.txt  — per-site watcher health (last seen, failures, reachable)
  data/market_index.txt    — market index stats (count, last refresh, top questions)
  data/activity_log.txt    — append-only rolling event log (most recent events first)

All files are written atomically via write-to-.tmp-then-rename so readers always
see a complete file even while Seulgi is updating it.

Dashboard is event-driven:
  - Components call record_*() methods when something notable happens.
  - A background loop (run()) rewrites all files every `interval_secs`.
  - High-priority events (trade executed, near miss) trigger an immediate flush.

Usage in main.py:
    dash = Dashboard(data_dir="/data", interval_secs=30)
    asyncio.create_task(dash.run())
    # then pass `dash` through the pipeline and call record_*() everywhere
"""

from __future__ import annotations

import asyncio
import collections
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

_MAX_BUFFER = 50       # ring buffer size for most lists
_MAX_ACTIVITY = 500    # lines kept in activity_log.txt
_NEAR_MISS_EDGE = 0.0  # near-miss = positive edge that didn't cross threshold


# ---------------------------------------------------------------------------
# Event types stored in memory
# ---------------------------------------------------------------------------


@dataclass
class NewsEvent:
    site_url: str
    headline: str
    article_url: str
    detected_at: datetime
    headline_id: str = ""


@dataclass
class AIEvent:
    headline: str
    question: str
    direction: str          # YES / NO / NEUTRAL
    confidence: float
    reason: str
    decided_at: datetime


@dataclass
class TradeEvent:
    condition_id: str
    question: str
    headline: str
    site_url: str
    direction: str
    confidence: float
    similarity: float
    p_hat: float
    mid_price: Optional[float]
    size_usdc: float
    skip_reason: Optional[str]
    edge: float             # p_hat - mid_price (0 if no price)
    decided_at: datetime
    headline_id: str = ""


@dataclass
class PositionEvent:
    condition_id: str
    question: str
    direction: str
    size_usdc: float
    entry_price: float
    current_price: float
    unrealized_pnl: float
    p_hat: float
    opened_at: datetime
    age_hours: float
    engine_name: str = "default"


@dataclass
class ClosedEvent:
    condition_id: str
    direction: str
    size_usdc: float
    entry_price: float
    exit_price: float
    realized_pnl: float
    reason: str             # WIN / LOSS / TIMEOUT
    closed_at: datetime


@dataclass
class WatcherStatus:
    url: str
    reachable: bool
    failures: int
    last_headline_at: Optional[datetime]
    last_headline: str
    total_headlines: int
    is_unreachable: bool
    budget_used: Optional[int] = None    # Brave calls in last hour (Twitter watchers only)
    budget_limit: Optional[int] = None
    last_error: str = ""                  # last fetch exception (truncated)


@dataclass
class SystemHealth:
    """Aggregate health snapshot rendered in the SYSTEM HEALTH dashboard block."""
    scraper_reachable: bool = False
    scraper_url: str = ""
    scraper_last_ok_at: Optional[datetime] = None
    scraper_last_error: str = "never probed"
    scraper_latency_ms: Optional[float] = None
    ai_key_present: bool = False
    ai_neutral_rate_1h: Optional[float] = None   # 0..1; how flat the AI is
    ai_total_decisions_1h: int = 0
    db_last_insert_at: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------


class Dashboard:
    """
    Collects pipeline events and writes them to text files in data_dir.
    """

    def __init__(self, data_dir: str, interval_secs: float = 30.0, db=None) -> None:
        self._dir = data_dir
        self._interval = interval_secs
        self._db = db                         # Optional[SeulgiDB] for new views
        self._last_trade_log_id: int = 0      # cursor into closed trades for trade_log.jsonl
        self._started_at = datetime.now(timezone.utc)

        # Ring buffers
        self._news: collections.deque[NewsEvent] = collections.deque(maxlen=_MAX_BUFFER)
        self._ai: collections.deque[AIEvent] = collections.deque(maxlen=_MAX_BUFFER)
        self._executed: collections.deque[TradeEvent] = collections.deque(maxlen=_MAX_BUFFER)
        self._declined: collections.deque[TradeEvent] = collections.deque(maxlen=_MAX_BUFFER)
        self._near_miss: collections.deque[TradeEvent] = collections.deque(maxlen=_MAX_BUFFER)
        self._closed: collections.deque[ClosedEvent] = collections.deque(maxlen=_MAX_BUFFER)
        self._activity: collections.deque[str] = collections.deque(maxlen=_MAX_ACTIVITY)
        self._traces: collections.deque[str] = collections.deque(maxlen=200)

        # Live state
        self._open_positions: list[PositionEvent] = []
        self._watcher_statuses: dict[str, WatcherStatus] = {}
        self._system_health: SystemHealth = SystemHealth()
        self._market_count: int = 0
        self._market_last_refresh: Optional[datetime] = None
        # Per-engine balances: engine_name → balance
        self._engine_balances: dict[str, float] = {}
        self._total_realized_pnl: float = 0.0
        # E.7.1: active-variant filter for the lifetime leaderboard render.
        # Populated by register_engines(). Empty set → no filter (all-time view).
        self._active_variants: set[str] = set()
        # Engine comparison summary rows (from DB query)
        self._engine_summary: list[dict] = []
        self._engine_skip_reasons: list[dict] = []

        # Signal journal (non-NEUTRAL AI decisions with outcomes)
        self._journal: collections.deque[dict] = collections.deque(maxlen=200)

        # Quant stats (updated from DB periodically)
        self._quant_stats: dict = {}

        # External usage totals (AI tokens, Brave Search) — updated periodically
        self._usage: dict = {}

        # Per-engine totals: engine_name → total_staked
        self._engine_staked: dict[str, float] = {}
        self._infinite_balance: bool = False
        # plan §1: three modes — "paper" | "infinite" | "simulate"
        self._mode: str = "paper"
        # plan §3: True while LLM is paused because no engine has free capital
        self._ai_paused: bool = False
        self._ai_paused_since: Optional[str] = None
        # plan §2: starting bankroll for simulate mode (recorded so the
        # dashboard can show "free + staked + realized = total" reconciliation)
        self._simulate_total_usdc: float = 0.0
        # Cycle scope for the leaderboard queries. trades table can grow to
        # millions of rows; lifetime GROUP BY variant_name takes 10s+. The
        # current cycle's pots are what matters for the per-engine ROI block,
        # so scope queries to >= cycle started_at when known.
        self._cycle_started_at: Optional[str] = None

        # Heavy-query cache. Five aggregate queries (variant_stats,
        # decision_funnel, confidence_buckets, freshness_counts,
        # freshness_buckets) used to run inline in _write_main every 30s
        # with 10s timeouts each — under load they piled up and starved
        # aiosqlite's writer (B-WRITES). A background task refreshes them
        # every _heavy_cache_interval seconds; _write_main reads from the
        # cache and never blocks on DB I/O.
        self._heavy_cache: dict = {
            "variant_stats": [],
            "decision_funnel": None,
            "confidence_buckets": [],
            "freshness_counts": [],
            "freshness_buckets": [],
            "scout_funnel": None,
            "_last_refresh_at": None,
        }
        self._heavy_cache_interval: float = 300.0  # 5 min
        self._heavy_cache_task: Optional[asyncio.Task] = None
        # Phase 2: separate read-only SeulgiDB used by the heavy-query
        # cache so cache reads never serialise with the writer connection
        # (root cause of the 22:20 cache timeouts on the previous deploy).
        # Opened lazily in run().
        self._db_ro: Optional["SeulgiDB"] = None  # type: ignore[name-defined]
        # Consecutive cache failures per query — soft-degrade rather than
        # blank the tile if a query repeatedly fails.
        self._heavy_cache_fail_streak: dict[str, int] = {}

        self._flush_now = asyncio.Event()

        os.makedirs(data_dir, exist_ok=True)

    @property
    def _balance(self) -> float:
        """Legacy balance: sum across all engines (or 0 if none registered)."""
        return sum(self._engine_balances.values()) if self._engine_balances else 0.0

    # ------------------------------------------------------------------
    # Record methods — called by pipeline components
    # ------------------------------------------------------------------

    def record_news(
        self, site_url: str, headline: str, article_url: str,
        headline_id: str = "",
    ) -> None:
        now = datetime.now(timezone.utc)
        ev = NewsEvent(site_url, headline, article_url, now, headline_id=headline_id)
        self._news.appendleft(ev)
        hid = f"[hid={headline_id[:8]}] " if headline_id else ""
        self._activity.appendleft(
            f"[{_ts(now)}] NEWS  {hid}{_short(site_url)}  {headline[:100]}"
        )

    def record_ai(
        self, headline: str, question: str, direction: str, confidence: float, reason: str
    ) -> None:
        now = datetime.now(timezone.utc)
        self._ai.appendleft(AIEvent(headline, question, direction, confidence, reason, now))
        self._activity.appendleft(
            f"[{_ts(now)}] AI    {direction} {confidence:.0%}  {headline[:80]}"
        )

    def record_trace(
        self,
        headline_id: str,
        site: str,
        headline: str,
        market: str,
        direction: str,
        confidence: float,
        outcome: str,
    ) -> None:
        """Append a single end-to-end trace line to the trace log."""
        now = datetime.now(timezone.utc)
        line = (
            f"{_ts(now)} | hid={headline_id[:8]} | {_short(site)} | "
            f"{headline[:60]} | {market[:50]} | "
            f"{direction} {confidence:.0%} | {outcome}"
        )
        self._traces.appendleft(line)

    def record_trade(
        self,
        condition_id: str,
        question: str,
        headline: str,
        site_url: str,
        direction: str,
        confidence: float,
        similarity: float,
        p_hat: float,
        mid_price: Optional[float],
        size_usdc: float,
        skip_reason: Optional[str],
        engine_name: str = "default",
        headline_id: str = "",
    ) -> None:
        now = datetime.now(timezone.utc)
        edge = (p_hat - mid_price) if mid_price is not None else 0.0
        ev = TradeEvent(
            condition_id=condition_id,
            question=question,
            headline=headline,
            site_url=site_url,
            direction=direction,
            confidence=confidence,
            similarity=similarity,
            p_hat=p_hat,
            mid_price=mid_price,
            size_usdc=size_usdc,
            skip_reason=skip_reason,
            edge=edge,
            decided_at=now,
            headline_id=headline_id,
        )
        pfx = f"[{engine_name}]" if engine_name != "default" else ""
        hid = f"[hid={headline_id[:8]}] " if headline_id else ""
        if skip_reason is None:
            self._executed.appendleft(ev)
            self._activity.appendleft(
                f"[{_ts(now)}]{pfx} TRADE {hid}{direction} ${size_usdc:.0f} @ {mid_price:.3f}  "
                f"edge={edge:+.3f}  {question[:70]}"
            )
            self._flush_now.set()
        else:
            self._declined.appendleft(ev)
            self._activity.appendleft(
                f"[{_ts(now)}]{pfx} SKIP  {hid}{direction} {skip_reason}  {question[:70]}"
            )
            # Near miss: edge is positive but below threshold
            if skip_reason.startswith("edge_too_small") and edge > _NEAR_MISS_EDGE:
                self._near_miss.appendleft(ev)
                self._activity.appendleft(
                    f"[{_ts(now)}]{pfx} NEAR  {direction} edge={edge:+.3f} (missed!)  {question[:70]}"
                )
                self._flush_now.set()

    def record_position_closed(
        self,
        condition_id: str,
        direction: str,
        size_usdc: float,
        entry_price: float,
        exit_price: float,
        realized_pnl: float,
        reason: str,
    ) -> None:
        now = datetime.now(timezone.utc)
        self._closed.appendleft(ClosedEvent(
            condition_id, direction, size_usdc,
            entry_price, exit_price, realized_pnl, reason, now,
        ))
        self._total_realized_pnl += realized_pnl
        sign = "+" if realized_pnl >= 0 else ""
        self._activity.appendleft(
            f"[{_ts(now)}] CLOSE {direction} {condition_id[:10]}  "
            f"{reason}  PnL={sign}{realized_pnl:.2f}"
        )
        self._flush_now.set()

    def set_balance(self, engine_name: str, balance: float) -> None:
        self._engine_balances[engine_name] = balance

    def register_engines(self, engine_names: list[str]) -> None:
        """
        Pre-seed the per-engine balance/staked dicts to 0.0 so every engine
        appears in the dashboard from the first tick — even before init_from_db
        completes. Without this, the dashboard render loop can race the
        sequential init in main.py and intermittently drop engines from the
        per-engine block.

        Also records the active-variant set so the lifetime leaderboard can
        hide retired variants (E.7.1). Lifetime stats query the trades table
        across all-time, so without this filter the top of the table is
        dominated by variants no longer running (e.g. the aggressive core
        retired 2026-05-18, _lowvol retired 2026-05-19).
        """
        for name in engine_names:
            self._engine_balances.setdefault(name, 0.0)
            self._engine_staked.setdefault(name, 0.0)
        # Set the active-variant filter (variant_name == engine name in DB).
        self._active_variants: set[str] = set(engine_names)

    def set_engine_summary(self, summary: list[dict], skip_reasons: list[dict]) -> None:
        """Refresh engine comparison data (called after each position monitor cycle)."""
        self._engine_summary = summary
        self._engine_skip_reasons = skip_reasons

    def set_open_positions(self, positions: list[PositionEvent]) -> None:
        self._open_positions = positions

    def set_system_health(self, health: "SystemHealth") -> None:
        self._system_health = health
        self._flush_now.set()

    def set_watcher_status(self, status: WatcherStatus) -> None:
        self._watcher_statuses[status.url] = status

    def record_signal_journal(
        self,
        headline: str,
        market: str,
        direction: str,
        confidence: float,
        similarity: float,
        engine_name: str,
        outcome: str,
        headline_id: str = "",
    ) -> None:
        self._journal.appendleft({
            "ts": datetime.now(timezone.utc),
            "headline": headline,
            "market": market,
            "direction": direction,
            "confidence": confidence,
            "similarity": similarity,
            "engine": engine_name,
            "outcome": outcome,
            "headline_id": headline_id,
        })

    def set_quant_stats(self, stats: dict) -> None:
        self._quant_stats = stats

    def set_usage(self, usage: dict) -> None:
        self._usage = usage

    def set_engine_staked(self, engine_name: str, staked: float) -> None:
        self._engine_staked[engine_name] = staked

    def set_infinite_balance(self, infinite: bool) -> None:
        self._infinite_balance = infinite
        if infinite and self._mode == "paper":
            self._mode = "infinite"

    def set_mode(self, mode: str) -> None:
        """plan §1: 'paper' | 'infinite' | 'simulate' (live not rendered yet)."""
        if mode not in ("paper", "infinite", "simulate", "live"):
            return
        self._mode = mode
        self._infinite_balance = (mode == "infinite")

    def set_cycle_started_at(self, started_at: Optional[str]) -> None:
        """Scope the leaderboard / funnel queries to >= started_at."""
        self._cycle_started_at = started_at

    def set_simulate_total(self, total_usdc: float) -> None:
        self._simulate_total_usdc = float(total_usdc)

    def set_ai_paused(self, paused: bool) -> None:
        """plan §3 — runner toggles this when LLM is suspended for cost discipline."""
        if paused == self._ai_paused:
            return
        self._ai_paused = paused
        self._ai_paused_since = datetime.now(timezone.utc).isoformat(timespec="seconds")

    def set_market_index(self, count: int) -> None:
        self._market_count = count
        self._market_last_refresh = datetime.now(timezone.utc)
        self._activity.appendleft(
            f"[{_ts(self._market_last_refresh)}] MKTIDX refreshed — {count} markets"
        )

    # ------------------------------------------------------------------
    # Background loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Write all dashboard files every interval_secs, or sooner on flush."""
        # Start the heavy-query background refresher exactly once. The cache
        # task never blocks the render loop; if it dies the render loop
        # keeps serving stale data instead of crashing the dashboard.
        if self._db is not None and self._heavy_cache_task is None:
            # Phase 2: open a sibling RO connection for the cache loop so
            # SELECTs never serialise with the writer thread.
            if self._db_ro is None:
                try:
                    # Import here to avoid a circular dependency at module load.
                    from db import SeulgiDB  # type: ignore
                    self._db_ro = await SeulgiDB.open_ro_clone(
                        getattr(self._db, "_path", "/data/seulgi.db")
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "[dash-cache] RO clone failed (%s) — falling back to writer conn",
                        exc,
                    )
            self._heavy_cache_task = asyncio.create_task(
                self._heavy_cache_loop()
            )
        while True:
            try:
                try:
                    await asyncio.wait_for(
                        asyncio.shield(self._flush_now.wait()),
                        timeout=self._interval,
                    )
                    self._flush_now.clear()
                except asyncio.TimeoutError:
                    pass

                await self._write_all()

            except asyncio.CancelledError:
                if self._heavy_cache_task is not None:
                    self._heavy_cache_task.cancel()
                if self._db_ro is not None:
                    try:
                        await self._db_ro.close()
                    except Exception:  # noqa: BLE001
                        pass
                await self._write_all()  # final flush
                break
            except Exception as exc:
                logger.warning("[dash] write error: %s", exc)

    async def _heavy_cache_loop(self) -> None:
        """Refresh _heavy_cache every _heavy_cache_interval seconds.

        First refresh fires immediately so the dashboard can render real
        data within a few seconds of boot rather than waiting 5 minutes.
        Per-query failures are isolated: a stuck variant_stats won't blank
        the freshness counts that returned fine.
        """
        while True:
            try:
                await self._refresh_heavy_cache()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning("[dash-cache] refresh loop error: %s", exc)
            try:
                await asyncio.sleep(self._heavy_cache_interval)
            except asyncio.CancelledError:
                raise

    async def _refresh_heavy_cache(self) -> None:
        """Run each heavy query and update its cache slot in-place.

        Phase 2: queries run on `self._db_ro` (a separate RO aiosqlite
        connection) so they never serialise with the writer. Each query
        is wrapped in its own try/except + wait_for (30 s) so one slow
        query never starves the others, and a transient failure leaves
        the previously cached value intact rather than blanking the tile.
        After 3 consecutive failures of the same query we log at ERROR
        and tag the tile with "cache unavailable".
        """
        # Prefer the RO clone; fall back to the writer connection if the
        # clone failed to open at boot.
        db = self._db_ro or self._db
        if db is None:
            return
        one_hour_ago = (
            datetime.now(timezone.utc) - timedelta(hours=1)
        ).isoformat()
        refreshers = [
            ("variant_stats",
             lambda: db.get_variant_stats(since_ts=one_hour_ago)),
            ("decision_funnel",
             lambda: db.get_decision_funnel(since_secs=3600)),
            ("confidence_buckets",
             lambda: db.get_confidence_buckets(since_secs=86400)),
            ("freshness_counts",
             lambda: db.get_freshness_counts(since_secs=3600)),
            ("freshness_buckets",
             lambda: db.get_freshness_buckets(since_secs=86400)),
        ]
        for key, make_coro in refreshers:
            try:
                self._heavy_cache[key] = await asyncio.wait_for(
                    make_coro(), timeout=30,
                )
                self._heavy_cache_fail_streak[key] = 0
            except Exception as exc:  # noqa: BLE001
                streak = self._heavy_cache_fail_streak.get(key, 0) + 1
                self._heavy_cache_fail_streak[key] = streak
                level = logger.error if streak >= 3 else logger.warning
                level(
                    "[dash-cache] %s refresh failed (streak=%d): %s",
                    key, streak, exc,
                )
        # Scout funnel is computed from a single SELECT; reuse the same
        # cache slot so the dashboard tile and the variant block share a
        # consistent freshness stamp.
        try:
            self._heavy_cache["scout_funnel"] = await asyncio.wait_for(
                self._compute_scout_funnel(db_override=db), timeout=30,
            )
            self._heavy_cache_fail_streak["scout_funnel"] = 0
        except Exception as exc:  # noqa: BLE001
            streak = self._heavy_cache_fail_streak.get("scout_funnel", 0) + 1
            self._heavy_cache_fail_streak["scout_funnel"] = streak
            level = logger.error if streak >= 3 else logger.warning
            level(
                "[dash-cache] scout_funnel refresh failed (streak=%d): %s",
                streak, exc,
            )
        self._heavy_cache["_last_refresh_at"] = datetime.now(timezone.utc)

    async def _compute_scout_funnel(self, db_override=None) -> Optional[dict]:
        """Fetch scout's trader_trace rows and tabulate the decision funnel.

        Cycle-scoped query (falls back to a 1h window when the cycle start
        isn't known) keeps the SELECT cheap on the multi-million-row trades
        table. The pure `compute_funnel` core lives in
        `seulgi/app/scout_funnel_core.py` so the CLI analyzer and this tile
        share one implementation. Returns a dict with `stats`, `cutoff`,
        `since_hours` for the renderer; or None on DB failure.

        Phase 2: `db_override` lets the cache loop use the RO clone so the
        scout funnel query doesn't serialise with the writer thread.
        """
        db = db_override or self._db
        if db is None:
            return None
        from scout_funnel_core import compute_funnel
        now = datetime.now(timezone.utc)
        if self._cycle_started_at:
            cutoff = self._cycle_started_at
            try:
                cutoff_dt = datetime.fromisoformat(
                    cutoff.replace("Z", "+00:00")
                )
                since_hours = max((now - cutoff_dt).total_seconds() / 3600.0, 0.0)
            except Exception:  # noqa: BLE001
                since_hours = 0.0
        else:
            cutoff = (now - timedelta(hours=1)).isoformat()
            since_hours = 1.0
        try:
            rows = await db.get_scout_trader_traces(
                engine_prefix="scout", since_ts=cutoff,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("[dash-cache] scout_funnel query failed: %s", exc)
            return None
        stats = compute_funnel(rows)
        return {
            "stats": stats,
            "cutoff": cutoff,
            "since_hours": since_hours,
        }

    async def _write_all(self) -> None:
        """Rewrite all dashboard files.

        Uses return_exceptions=True so one writer's failure doesn't
        silently cancel its peers via asyncio.gather's default behaviour.
        Per-task exceptions are surfaced as WARN lines with the file name
        so future regressions don't masquerade as "stale dashboard.txt".
        """
        writers = [
            ("main", self._write_main()),
            ("scout_funnel", self._write_scout_funnel()),
            ("db_health", self._write_db_health()),
            ("positions", self._write_positions()),
            ("executed", self._write_executed()),
            ("declined", self._write_declined()),
            ("near_miss", self._write_near_miss()),
            ("news", self._write_news()),
            ("ai", self._write_ai()),
            ("watcher_status", self._write_watcher_status()),
            ("market_index", self._write_market_index()),
            ("engine_comparison", self._write_engine_comparison()),
            ("activity_log", self._write_activity_log()),
            ("trace_log", self._write_trace_log()),
            ("signal_journal", self._write_signal_journal()),
            ("quant_stats", self._write_quant_stats()),
            ("usage", self._write_usage()),
            ("trade_log", self._write_trade_log()),
            ("variant_compare", self._write_variant_compare()),
            ("wins_losses", self._write_wins_losses()),
            ("headline_journal", self._write_headline_journal()),
            ("headline_winners", self._write_headline_winners()),
        ]
        results = await asyncio.gather(
            *[coro for _, coro in writers], return_exceptions=True,
        )
        for (name, _), result in zip(writers, results):
            if isinstance(result, BaseException):
                logger.warning("[dash] %s writer failed: %r", name, result, exc_info=result)

    # ------------------------------------------------------------------
    # Individual file writers
    # ------------------------------------------------------------------

    async def _write_main(self) -> None:
        now = datetime.now(timezone.utc)
        uptime = now - self._started_at
        hours, rem = divmod(int(uptime.total_seconds()), 3600)
        mins = rem // 60

        n_open_total = len(self._open_positions)
        unrealized = sum(p.unrealized_pnl for p in self._open_positions)
        unreal_sign = "+" if unrealized >= 0 else "-"

        last_news = self._news[0] if self._news else None
        last_trade = self._executed[0] if self._executed else None
        last_miss = self._near_miss[0] if self._near_miss else None
        last_skip = self._declined[0] if self._declined else None

        total_balance = sum(self._engine_balances.values()) if self._engine_balances else 0.0
        total_staked = sum(self._engine_staked.values()) if self._engine_staked else 0.0
        if self._mode == "simulate":
            mode_label = "SIMULATE MODE — micro-bankroll, live capital tracking"
        elif self._mode == "infinite" or self._infinite_balance:
            mode_label = "INFINITE MODE — gain tracked vs staked"
        else:
            mode_label = "PAPER TRADING"
        lines = [
            "╔══════════════════════════════════════════════════════════════╗",
            "║                    SEULGI  DASHBOARD                        ║",
            "╚══════════════════════════════════════════════════════════════╝",
            "",
            f"  Updated : {_ts(now)}",
            f"  Uptime  : {hours}h {mins}m",
            "",
            f"── {mode_label} " + "─" * (66 - len(mode_label) - 5),
        ]
        # Per-engine aggregates derived strictly from currently-open
        # positions (cost basis + MTM). _engine_staked is cumulative-staked-
        # ever and not safe to use here — it never decrements on close, which
        # is what produced the phantom "+$177 with 0 closes" headline.
        per_engine_unreal: dict[str, float] = {}
        per_engine_open: dict[str, int] = {}
        per_engine_cost: dict[str, float] = {}
        per_engine_mtm: dict[str, float] = {}
        for p in self._open_positions:
            eng = p.engine_name or "default"
            per_engine_unreal[eng] = per_engine_unreal.get(eng, 0.0) + p.unrealized_pnl
            per_engine_open[eng] = per_engine_open.get(eng, 0) + 1
            per_engine_cost[eng] = per_engine_cost.get(eng, 0.0) + p.size_usdc
            per_engine_mtm[eng] = per_engine_mtm.get(eng, 0.0) + (p.size_usdc + p.unrealized_pnl)
        open_cost_total = sum(per_engine_cost.values())
        open_mtm_total = sum(per_engine_mtm.values())

        engine_names = sorted(set(self._engine_balances) | set(self._engine_staked))
        # Widen name column to fit the longest engine name (min 14).
        name_w = max(14, *(len(n) for n in engine_names)) if engine_names else 14

        if self._mode == "simulate":
            # Honest accounting (post-bankroll-drift fix):
            #   MTM total = free cash + Σ (open position cost + unrealized)
            #   Net PnL   = realized (closed trades) + unrealized (open)
            # Drift = MTM_total - start - Net PnL.  Non-zero drift = bug.
            start = self._simulate_total_usdc
            free_cash = total_balance                  # trader._cash sum (safe)
            mtm_total = free_cash + open_mtm_total
            unrealized = open_mtm_total - open_cost_total
            net_pnl = self._total_realized_pnl + unrealized
            n_open = len(self._open_positions)

            lines.append(f"  Starting bankroll       : ${start:>10.2f} USDC")
            lines.append(f"  Free cash               : ${free_cash:>10.2f} USDC")
            lines.append(
                f"  Open positions (cost)   : ${open_cost_total:>10.2f} USDC  (n={n_open})"
            )
            lines.append(f"  Open positions (MTM)    : ${open_mtm_total:>10.2f} USDC")
            mtm_delta = mtm_total - start
            md_sign = "+" if mtm_delta >= 0 else "-"
            lines.append(
                f"  Mark-to-market total    : ${mtm_total:>10.2f} USDC  "
                f"Δ vs start: {md_sign}${abs(mtm_delta):.2f}"
            )
            lines.append("")
            r_sign = "+" if self._total_realized_pnl >= 0 else "-"
            lines.append(
                f"  Realized PnL (closed)   : {r_sign}${abs(self._total_realized_pnl):>9.2f} USDC"
            )
            u_sign = "+" if unrealized >= 0 else "-"
            lines.append(
                f"  Unrealized PnL (open)   : {u_sign}${abs(unrealized):>9.2f} USDC"
            )
            n_sign = "+" if net_pnl >= 0 else "-"
            lines.append(
                f"  Net PnL (real+unreal)   : {n_sign}${abs(net_pnl):>9.2f} USDC  "
                f"← are we up?"
            )
            ai_status = "PAUSED (no free capital)" if self._ai_paused else "ACTIVE"
            since = f"  since {self._ai_paused_since}" if (self._ai_paused and self._ai_paused_since) else ""
            lines.append(f"  AI status               : {ai_status}{since}")

            # Drift = (money currently in system) - (start + net pnl).
            # Should be ≈ 0 (modulo fees already paid on open positions).
            # Anything bigger than a few cents indicates an accounting bug.
            drift = mtm_total - start - net_pnl
            if abs(drift) > 0.05 and start > 0:
                lines.append(
                    f"  ⚠ ACCOUNTING DRIFT      : MTM-start-NetPnL = {drift:+.4f} USDC"
                )

            lines.append("")
            lines.append(
                f"  {'ENGINE':<{name_w}}  {'FREE':>10}  {'OPEN_COST':>10}  "
                f"{'OPEN_MTM':>10}  {'OPEN':>5}  {'UNREAL':>9}"
            )
            lines.append("  " + "─" * (name_w + 2 + 10 + 2 + 10 + 2 + 10 + 2 + 5 + 2 + 9))
            for eng_name in engine_names:
                free = self._engine_balances.get(eng_name, 0.0)
                cost = per_engine_cost.get(eng_name, 0.0)
                mtm = per_engine_mtm.get(eng_name, 0.0)
                unreal = per_engine_unreal.get(eng_name, 0.0)
                eng_open = per_engine_open.get(eng_name, 0)
                lines.append(
                    f"  {eng_name:<{name_w}}  {free:>10.2f}  {cost:>10.2f}  "
                    f"{mtm:>10.2f}  {eng_open:>5d}  {unreal:>+9.2f}"
                )
        elif self._infinite_balance:
            tb_sign = "+" if total_balance >= 0 else "-"
            roi = (total_balance / total_staked * 100.0) if total_staked > 0 else 0.0
            lines.append(f"  Total realized gain : {tb_sign}${abs(total_balance):>10.2f} USDC")
            lines.append(f"  Total staked        : ${total_staked:>10.2f} USDC")
            lines.append(f"  ROI                 : {roi:>+6.2f}%")
            lines.append("")
            # Aligned per-engine PnL table
            lines.append(
                f"  {'ENGINE':<{name_w}}  {'GAIN':>10}  {'STAKED':>10}  {'ROI':>8}  "
                f"{'OPEN':>5}  {'UNREALIZED':>11}"
            )
            lines.append("  " + "─" * (name_w + 2 + 10 + 2 + 10 + 2 + 8 + 2 + 5 + 2 + 11))
            for eng_name in engine_names:
                gain = self._engine_balances.get(eng_name, 0.0)
                staked = self._engine_staked.get(eng_name, 0.0)
                eng_roi = (gain / staked * 100.0) if staked > 0 else 0.0
                unreal = per_engine_unreal.get(eng_name, 0.0)
                eng_open = per_engine_open.get(eng_name, 0)
                lines.append(
                    f"  {eng_name:<{name_w}}  {gain:>+10.2f}  {staked:>10.2f}  "
                    f"{eng_roi:>+7.2f}%  {eng_open:>5d}  {unreal:>+11.2f}"
                )
        else:
            lines.append(f"  Total balance   : ${total_balance:>10.2f} USDC")
            lines.append("")
            lines.append(f"  {'ENGINE':<{name_w}}  {'BALANCE':>10}  {'OPEN':>5}  {'UNREALIZED':>11}")
            lines.append("  " + "─" * (name_w + 2 + 10 + 2 + 5 + 2 + 11))
            for eng_name in engine_names:
                eng_bal = self._engine_balances.get(eng_name, 0.0)
                unreal = per_engine_unreal.get(eng_name, 0.0)
                eng_open = per_engine_open.get(eng_name, 0)
                lines.append(
                    f"  {eng_name:<{name_w}}  {eng_bal:>+10.2f}  {eng_open:>5d}  {unreal:>+11.2f}"
                )
        # Process-local counters since the container started. The per-engine
        # GAIN/ROI block above shows the *lifetime* numbers restored from
        # DB; the lines below show "what's happened since this run booted".
        # We label them explicitly so the operator stops being confused
        # when the top says +$61 lifetime and the bottom says +$0 session.
        n_closed_session = len(self._closed)
        session_realized = self._total_realized_pnl
        sess_sign = "+" if session_realized >= 0 else ""
        lines += [
            "",
            f"  Open positions      : {n_open_total}",
            f"  Unrealized PnL      : {unreal_sign}${abs(unrealized):.2f}    (mark-to-market on open positions)",
            f"  Closes this session : {n_closed_session} / {sess_sign}${session_realized:.2f}   (since container start; lifetime gain shown above)",
            f"  Recent trades buf   : {len(self._executed)} / {self._executed.maxlen}    (in-memory ring)",
            f"  Trades declined     : {len(self._declined)}",
            f"  Near misses         : {len(self._near_miss)}",
        ]

        # ── Cycle-8 per-decision insight blocks ──────────────────────────
        # Five aggregate queries against the trades / signals tables. Live
        # execution was a B-WRITES suspect — the 10s wait_for blocked
        # aiosqlite's writer. Reads come from _heavy_cache, refreshed by a
        # background task every 5 minutes (see _heavy_cache_loop).
        if self._db is not None:
            cache = self._heavy_cache
            v_stats = cache.get("variant_stats") or []
            funnel = cache.get("decision_funnel")
            buckets = cache.get("confidence_buckets") or []
            fresh_counts = cache.get("freshness_counts") or []
            fresh_buckets = cache.get("freshness_buckets") or []
            last_refresh = cache.get("_last_refresh_at")
            if last_refresh is not None:
                age = int((datetime.now(timezone.utc) - last_refresh).total_seconds())
                cache_hint = f"  (5-min cache, refreshed {age}s ago)"
            else:
                cache_hint = "  (5-min cache, warming up…)"

            # E.7.1: filter to active variants only. The lifetime leaderboard
            # is otherwise dominated by retired variants (aggressive/_lowvol
            # whose lifetime PnL still ranks ahead of newly-cut survivors).
            # variant_compare.txt retains the unfiltered full table.
            if self._active_variants:
                v_stats_view = [r for r in v_stats
                                if (r.get("variant") or "") in self._active_variants]
            else:
                v_stats_view = list(v_stats)

            lines += ["", "── VARIANT LEADERBOARD (active engines, by ROI% on $100 pot) ────"]
            lines.append(cache_hint)
            if not v_stats_view:
                if self._active_variants and v_stats:
                    lines.append("  (no closed trades yet for the active engines)")
                else:
                    lines.append("  (no executed trades yet)")
            else:
                # M5: ROI on per-engine $100 pot is the right scoring metric.
                # Sort active leaderboard by it. Each engine has its own pot;
                # sum_pnl / 100 IS its ROI (no cross-engine borrowing).
                # NOTE: datetime is already module-imported at top of file
                # (line 39). Re-importing here would shadow it and trigger
                # "local variable not associated" earlier in _write_main.
                now_for_last = datetime.now(timezone.utc)
                v_stats_view = sorted(
                    v_stats_view,
                    key=lambda r: (r.get("sum_pnl") or 0) / 100.0,
                    reverse=True,
                )
                lines.append(
                    f"  {'VARIANT':<34}  {'N':>4} {'HIT%':>5}  "
                    f"{'PnL':>7} {'ROI%':>6}  {'HOLDmn':>6}  {'LASTtr':>7}"
                )
                lines.append("  " + "─" * 84)
                for r in v_stats_view[:14]:
                    closed = (r.get("n_wins") or 0) + (r.get("n_losses") or 0)
                    hit = (r["n_wins"] / closed * 100.0) if closed else 0.0
                    pnl = r.get("sum_pnl") or 0.0
                    roi_pot = (pnl / 100.0) * 100.0   # % on $100 pot
                    avg_hold_min = (r.get("avg_hold_seconds") or 0) / 60.0
                    last_at = r.get("last_trade_at")
                    if last_at:
                        try:
                            t = datetime.fromisoformat(last_at.replace("Z", "+00:00"))
                            mins = (now_for_last - t).total_seconds() / 60.0
                            last_str = (f"{mins:.0f}m" if mins < 90
                                        else f"{mins / 60:.1f}h")
                        except Exception:  # noqa: BLE001
                            last_str = "?"
                    else:
                        last_str = "—"
                    lines.append(
                        f"  {(r['variant'] or '?')[:34]:<34}  {r['n_trades']:>4} "
                        f"{hit:>4.1f}%  {pnl:>+7.2f} {roi_pot:>+5.2f}%  "
                        f"{avg_hold_min:>5.1f}m  {last_str:>7}"
                    )
                lines.append("  Full table (incl. retired): variant_compare.txt")

            lines += ["", "── DECISION FUNNEL (last 1h) ────────────────────────────────────"]
            if funnel is None or funnel["total"] == 0:
                lines.append("  (no decisions in last hour)")
            else:
                total = funnel["total"]
                def _pct(n: int) -> str:
                    return f"({n / total * 100.0:>4.1f}%)"
                lines.append(f"  decisions seen          : {total}")
                rows = [
                    ("freshness skip       ", funnel.get("failed_freshness", 0)),
                    ("low_similarity bypass",funnel.get("failed_low_sim", 0)),
                    ("relevance gate dropped",funnel.get("failed_relevance", 0)),
                    ("pre-AI rule dropped",   funnel["pre_ai_dropped"]),
                    ("AI returned NEUTRAL",   funnel["ai_neutral"]),
                    ("conf below threshold",  funnel["failed_min_confidence"]),
                    ("ensemble disagreement", funnel["failed_agreement"]),
                    ("post-AI rule dropped",  funnel["failed_post_ai_rule"]),
                    ("rail / price unusable", funnel["failed_rail"]),
                    ("min-entry-mid blocked", funnel.get("failed_min_entry", 0)),
                    ("market too noisy",     funnel.get("failed_market_vol", 0)),
                    ("edge too small",        funnel["failed_edge"]),
                    ("position-cap blocked",  funnel["failed_position_guard"]),
                    ("size/balance blocked",  funnel["failed_size"]),
                    ("other skipped",         funnel["other_skipped"]),
                    ("executed",              funnel["executed"]),
                ]
                # Pin the two new cycle-9 buckets so the operator can see
                # the freshness + relevance gates are actively engaged even
                # when they fire 0 times in the window. The other buckets
                # only show when nonzero — keeps the section scannable.
                always_show = {"freshness skip       ", "low_similarity bypass",
                               "relevance gate dropped",
                               "executed", "AI returned NEUTRAL"}
                for label, n in rows:
                    if n == 0 and label not in always_show:
                        continue
                    lines.append(f"  → {label:<22}: {n:>4}  {_pct(n)}")

            lines += ["", "── FRESHNESS (last 1h headlines) ────────────────────────────────"]
            if not fresh_counts:
                lines.append("  (no labelled signals yet)")
            else:
                total_f = sum(r["n"] for r in fresh_counts)
                for r in fresh_counts:
                    pct = (r["n"] / total_f * 100.0) if total_f else 0.0
                    lines.append(f"  {r['label']:<14} : {r['n']:>4}  ({pct:>4.1f}%)")

            lines += ["", "── FRESHNESS BUCKETS (last 24h) ─────────────────────────────────"]
            if not fresh_buckets:
                lines.append("  (no decisions in last 24h)")
            else:
                lines.append(
                    f"  {'LABEL':<14} {'EXEC':>5} {'WINS':>5} {'LOSS':>5}  "
                    f"{'HIT%':>6}  {'AVG PnL':>8}  {'SUM PnL':>9}  {'SKIP':>5}"
                )
                lines.append("  " + "─" * 76)
                for b in fresh_buckets:
                    closed = (b.get("n_wins") or 0) + (b.get("n_losses") or 0)
                    hit_s = f"{(b['n_wins']/closed*100.0):>5.1f}%" if closed else "    —"
                    avg_s = f"{b['avg_pnl']:>+8.3f}" if closed else "       —"
                    sum_s = f"{b['sum_pnl']:>+9.2f}" if closed else "        —"
                    lines.append(
                        f"  {b['label']:<14} {b['n_executed']:>5} "
                        f"{b['n_wins']:>5} {b['n_losses']:>5}  "
                        f"{hit_s}  {avg_s}  {sum_s}  {b['n_skipped']:>5}"
                    )

            lines += ["", "── CONFIDENCE BUCKETS (last 24h) ────────────────────────────────"]
            if not buckets:
                lines.append("  (no AI decisions in last 24h)")
            else:
                lines.append(
                    f"  {'CONF':<10} {'EXEC':>5} {'WINS':>5} {'LOSS':>5}  "
                    f"{'HIT%':>6}  {'AVG PnL':>8}  {'SUM PnL':>9}  {'SKIP':>5}"
                )
                lines.append("  " + "─" * 76)
                for b in buckets:
                    closed = (b.get("n_wins") or 0) + (b.get("n_losses") or 0)
                    hit_s = f"{(b['n_wins']/closed*100.0):>5.1f}%" if closed else "    —"
                    avg_s = f"{b['avg_pnl']:>+8.3f}" if closed else "       —"
                    sum_s = f"{b['sum_pnl']:>+9.2f}" if closed else "        —"
                    lines.append(
                        f"  {b['bucket']:<10} {b['n_executed']:>5} "
                        f"{b['n_wins']:>5} {b['n_losses']:>5}  "
                        f"{hit_s}  {avg_s}  {sum_s}  {b['n_skipped']:>5}"
                    )

        lines += [
            "",
            "── MARKET INDEX ────────────────────────────────────────────────",
            f"  Markets tracked : {self._market_count}",
            f"  Last refresh    : {_ts(self._market_last_refresh) if self._market_last_refresh else 'never'}",
            "",
            "── SYSTEM HEALTH ───────────────────────────────────────────────",
        ]
        sh = self._system_health
        scraper_glyph = "✓" if sh.scraper_reachable else "✗"
        scraper_summary = (
            f"reachable  (last OK {_ts(sh.scraper_last_ok_at)}"
            + (f", latency {sh.scraper_latency_ms:.0f}ms)" if sh.scraper_latency_ms is not None else ")")
            if sh.scraper_reachable
            else f"UNREACHABLE — {sh.scraper_last_error or 'unknown'}"
        )
        lines.append(f"  {scraper_glyph} scraper      : {scraper_summary}")
        lines.append(
            f"    AI key      : {'✓ present' if sh.ai_key_present else '✗ MISSING'}"
        )
        if sh.ai_total_decisions_1h > 0 and sh.ai_neutral_rate_1h is not None:
            warn = "  ⚠ signal starved" if sh.ai_neutral_rate_1h >= 0.85 else ""
            lines.append(
                f"    AI 1h       : {sh.ai_neutral_rate_1h*100:>5.1f}% NEUTRAL "
                f"(n={sh.ai_total_decisions_1h}){warn}"
            )
        if sh.db_last_insert_at is not None:
            lines.append(f"    DB last write : {_ts(sh.db_last_insert_at)}")
        lines.append("")

        lines.append("── WATCHERS ────────────────────────────────────────────────────")
        if not sh.scraper_reachable and self._watcher_statuses:
            lines.append("  ⚠ scraper unreachable — every watcher below will report failures")
        for url, ws in self._watcher_statuses.items():
            status = "DEAD" if ws.is_unreachable else f"fail={ws.failures}"
            since = _ts(ws.last_headline_at) if ws.last_headline_at else "never"
            tail = f"  err={ws.last_error[:40]}" if ws.last_error else ""
            lines.append(
                f"  {'✓' if not ws.is_unreachable else '✗'} {_short(url):<30} "
                f"last={since}  {status}{tail}"
            )
        if not self._watcher_statuses:
            lines.append("  (no watchers registered yet)")
        lines.append("")

        lines.append("── LAST EVENTS ─────────────────────────────────────────────────")
        if last_news:
            lines.append(f"  Last news    : [{_ts(last_news.detected_at)}] {last_news.headline[:80]}")
            lines.append(f"               : {last_news.site_url}")
        if last_trade:
            lines.append(f"  Last trade   : [{_ts(last_trade.decided_at)}] {last_trade.direction} "
                         f"${last_trade.size_usdc:.0f} @ {last_trade.mid_price:.3f}  "
                         f"{last_trade.question[:60]}")
        if last_miss:
            lines.append(f"  Last near miss: [{_ts(last_miss.decided_at)}] {last_miss.direction} "
                         f"edge={last_miss.edge:+.3f}  {last_miss.question[:60]}")
        if last_skip:
            lines.append(f"  Last declined : [{_ts(last_skip.decided_at)}] {last_skip.skip_reason}  "
                         f"{last_skip.question[:60]}")
        # ── Perf metrics (in-memory snapshot, last 5 minutes) ────────
        try:
            import metrics as _m
            rows = _m.snapshot_recent()
            if rows:
                lines.append("")
                lines.append("── PERF METRICS (rolling, ~5m in-memory) ───────────────────────")
                lines.append(
                    f"  {'NAME':<28} {'TAGS':<22} {'N':>6}  "
                    f"{'p50':>6}  {'p95':>6}  {'p99':>6}  {'MAX':>6}"
                )
                lines.append("  " + "─" * 84)
                for r in rows[:15]:
                    tags_str = (r["tags"] or "").replace('"', "")[:22]
                    if r["sum"] > 0:  # timer
                        lines.append(
                            f"  {r['name'][:28]:<28} {tags_str:<22} {r['count']:>6}  "
                            f"{r['p50']:>5.0f}ms  {r['p95']:>5.0f}ms  "
                            f"{r['p99']:>5.0f}ms  {r['max']:>5.0f}ms"
                        )
                    else:  # counter (count only)
                        lines.append(
                            f"  {r['name'][:28]:<28} {tags_str:<22} {r['count']:>6}  "
                            f"{'—':>6}  {'—':>6}  {'—':>6}  {'—':>6}"
                        )
        except Exception:  # noqa: BLE001
            pass

        lines.append("")
        lines.append("  See positions_open.txt · trades_executed.txt · near_miss.txt")
        lines.append("      trades_declined.txt · news_seen.txt · activity_log.txt · profile.txt")

        _write(self._path("dashboard.txt"), "\n".join(lines) + "\n")

    async def _write_scout_funnel(self) -> None:
        """Render the SCOUT decision funnel tile from the 5-min cache.

        Reads `_heavy_cache["scout_funnel"]` (populated by
        `_compute_scout_funnel`) and renders the Markdown report via
        `scout_funnel_core.render_funnel_markdown`. Renders a placeholder
        until the first refresh completes.
        """
        out_path = self._path("scout_funnel.txt")
        cached = self._heavy_cache.get("scout_funnel") if self._db is not None else None
        last_refresh = self._heavy_cache.get("_last_refresh_at")
        if cached is None:
            header = "# SCOUT funnel — warming up\n\n"
            if last_refresh is None:
                body = (
                    "Cache is empty. The 5-min background refresh runs on "
                    "first dashboard tick; check back shortly.\n"
                )
            else:
                age = int((datetime.now(timezone.utc) - last_refresh).total_seconds())
                body = (
                    f"Last refresh attempt {age}s ago returned no data "
                    "(query may have failed — see logs).\n"
                )
            _write(out_path, header + body)
            return

        from scout_funnel_core import render_funnel_markdown
        text = render_funnel_markdown(
            cached["stats"],
            engine_prefix="scout",
            since_hours=cached.get("since_hours", 0.0),
            cutoff=cached.get("cutoff", ""),
        )
        if last_refresh is not None:
            age = int((datetime.now(timezone.utc) - last_refresh).total_seconds())
            text = (
                text.rstrip("\n")
                + f"\n\n_5-min cache, refreshed {age}s ago "
                  f"(cycle-scoped from {cached.get('cutoff', '?')})_\n"
            )
        _write(out_path, text)

    async def _write_db_health(self) -> None:
        """Render DB write health tile.

        Uses the synchronous in-memory snapshot so the render path never
        queues behind a writer-bound PRAGMA. WAL frame counts come from
        the host-side audit CLI (`db_health_audit.py`).
        """
        out_path = self._path("db_health.txt")
        now = datetime.now(timezone.utc)
        if self._db is None:
            _write(out_path, f"# DB HEALTH — {_ts(now)}\n\n  (no DB attached)\n")
            return
        health = self._db.write_health_snapshot()

        lines: list[str] = [f"# DB HEALTH — {_ts(now)}", ""]
        bt = health.get("busy_timeout_ms")
        lines.append(f"  busy_timeout_ms : {bt if bt is not None else '?'}")
        wal_total = health.get("wal_total_frames")
        wal_done = health.get("wal_checkpointed_frames")
        wal_busy = health.get("wal_busy")
        if wal_total is not None:
            lines.append(
                f"  WAL frames      : {wal_total} total, {wal_done} checkpointed, busy={wal_busy}"
            )
        if "wal_error" in health:
            lines.append(f"  WAL error       : {health['wal_error']}")
        lines.append("")

        lines.append("## Last successful insert per table")
        lines.append("")
        lines.append("| table             | last_insert (UTC)   | age      |")
        lines.append("|-------------------|---------------------|---------:|")
        last_insert_at = health.get("last_insert_at", {})
        for table in sorted(last_insert_at.keys()):
            ts_iso = last_insert_at[table]
            try:
                t = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
                age = (now - t).total_seconds()
                if age < 90:
                    age_s = f"{age:>5.0f}s"
                elif age < 3600:
                    age_s = f"{age/60:>4.1f}m"
                else:
                    age_s = f"{age/3600:>4.1f}h"
                ts_display = ts_iso[:19]
            except Exception:  # noqa: BLE001
                age_s = "  ?  "
                ts_display = ts_iso[:19]
            lines.append(f"| {table:<17} | {ts_display:<19} | {age_s:>8} |")
        if not last_insert_at:
            lines.append("| (no successful inserts since boot)                          |")
        lines.append("")

        lines.append("## Write counters (ok / err since boot)")
        lines.append("")
        lines.append("| method                       | ok        | err     |")
        lines.append("|------------------------------|----------:|--------:|")
        counters = health.get("counters", {})
        for method in sorted(counters.keys()):
            c = counters[method]
            ok_n = c.get("ok", 0)
            err_n = c.get("err", 0)
            mark = " ⚠" if err_n > 0 else ""
            lines.append(
                f"| {method:<28} | {ok_n:>9} | {err_n:>7} |{mark}"
            )
        if not counters:
            lines.append("| (no write attempts yet)                                |")
        lines.append("")

        # Cache section reads in-memory state; no extra DB query.
        cache = self._heavy_cache
        last_refresh = cache.get("_last_refresh_at")
        if last_refresh is not None:
            cache_age = int((now - last_refresh).total_seconds())
            lines.append(
                f"## Dashboard heavy-query cache  (last refresh {cache_age}s ago)"
            )
        else:
            lines.append("## Dashboard heavy-query cache  (warming up)")
        lines.append("")
        for k in (
            "variant_stats", "decision_funnel", "confidence_buckets",
            "freshness_counts", "freshness_buckets", "scout_funnel",
        ):
            v = cache.get(k)
            status = "OK" if v is not None else "(empty)"
            lines.append(f"  {k:<22}: {status}")
        _write(out_path, "\n".join(lines) + "\n")

    async def _write_positions(self) -> None:
        lines = [_header("OPEN POSITIONS")]
        if not self._open_positions:
            lines.append("  (none)")
        else:
            lines.append(
                f"  {'#':<3} {'DIRECTION':<10} {'SIZE':>8}  "
                f"{'ENTRY':>6}  {'CURRENT':>7}  {'PnL':>8}  "
                f"{'AGE':>6}  CONDITION"
            )
            lines.append("  " + "-" * 90)
            for i, p in enumerate(self._open_positions, 1):
                sign = "+" if p.unrealized_pnl >= 0 else ""
                lines.append(
                    f"  {i:<3} {p.direction:<10} ${p.size_usdc:>7.2f}  "
                    f"{p.entry_price:>6.3f}  {p.current_price:>7.3f}  "
                    f"{sign}${abs(p.unrealized_pnl):>7.2f}  "
                    f"{p.age_hours:>5.1f}h  {p.condition_id[:20]}"
                )
                lines.append(f"       {p.question[:95]}")
        _write(self._path("positions_open.txt"), "\n".join(lines) + "\n")

    async def _write_executed(self) -> None:
        lines = [_header("EXECUTED TRADES  (most recent first)")]
        if not self._executed:
            lines.append("  (none yet)")
        else:
            for ev in self._executed:
                _trade_block(lines, ev)
        _write(self._path("trades_executed.txt"), "\n".join(lines) + "\n")

    async def _write_declined(self) -> None:
        lines = [_header("DECLINED TRADE DECISIONS  (most recent first)")]
        if not self._declined:
            lines.append("  (none yet)")
        else:
            for ev in self._declined:
                _trade_block(lines, ev)
        _write(self._path("trades_declined.txt"), "\n".join(lines) + "\n")

    async def _write_near_miss(self) -> None:
        lines = [
            _header("NEAR MISSES  (positive edge — just below threshold)"),
            "  These had a real edge but didn't quite clear the min_edge filter.",
            "  Useful for tuning: lower min_edge to start catching these.",
            "",
        ]
        if not self._near_miss:
            lines.append("  (none yet — a good sign, threshold may be well-calibrated)")
        else:
            for ev in self._near_miss:
                _trade_block(lines, ev)
        _write(self._path("near_miss.txt"), "\n".join(lines) + "\n")

    async def _write_news(self) -> None:
        lines = [_header("NEWS SEEN  (most recent first)")]
        if not self._news:
            lines.append("  (no news detected yet — watchers starting up)")
        else:
            for ev in self._news:
                lines.append(f"  [{_ts(ev.detected_at)}] {_short(ev.site_url)}")
                lines.append(f"    {ev.headline}")
                lines.append(f"    {ev.article_url}")
                lines.append("")
        _write(self._path("news_seen.txt"), "\n".join(lines) + "\n")

    async def _write_ai(self) -> None:
        lines = [_header("AI DECISIONS  (most recent first)")]
        if not self._ai:
            lines.append("  (no AI calls yet)")
        else:
            _DIR_SYMBOL = {"YES": "▲", "NO": "▼", "NEUTRAL": "─"}
            for ev in self._ai:
                sym = _DIR_SYMBOL.get(ev.direction, "?")
                lines.append(
                    f"  [{_ts(ev.decided_at)}] {sym} {ev.direction:<8} {ev.confidence:.0%}  "
                    f"{ev.reason[:60]}"
                )
                lines.append(f"    Headline : {ev.headline[:95]}")
                lines.append(f"    Question : {ev.question[:95]}")
                lines.append("")
        _write(self._path("ai_decisions.txt"), "\n".join(lines) + "\n")

    async def _write_watcher_status(self) -> None:
        lines = [_header("WATCHER STATUS")]
        if not self._watcher_statuses:
            lines.append("  (watchers not yet started)")
        else:
            for url, ws in self._watcher_statuses.items():
                status = "UNREACHABLE" if ws.is_unreachable else ("OK" if ws.failures == 0 else f"DEGRADED ({ws.failures} failures)")
                since = _ts(ws.last_headline_at) if ws.last_headline_at else "never"
                lines.append(f"  {'✗' if ws.is_unreachable else '✓'} {url}")
                lines.append(f"    Status         : {status}")
                lines.append(f"    Last headline  : {since}")
                lines.append(f"    Total headlines: {ws.total_headlines}")
                if ws.last_headline:
                    lines.append(f"    Latest         : {ws.last_headline[:90]}")
                if ws.budget_limit:
                    pct = int((ws.budget_used or 0) / ws.budget_limit * 100)
                    lines.append(f"    Brave budget   : {ws.budget_used}/{ws.budget_limit} calls/hr  ({pct}%)")
                lines.append("")
        _write(self._path("watcher_status.txt"), "\n".join(lines) + "\n")

    async def _write_market_index(self) -> None:
        lines = [_header("MARKET INDEX")]
        lines.append(f"  Markets in index : {self._market_count}")
        lines.append(f"  Last refresh     : {_ts(self._market_last_refresh) if self._market_last_refresh else 'never'}")
        lines.append("")
        lines.append("  The market index is built from Polymarket's Gamma API.")
        lines.append("  Each market's question text is embedded with sentence-transformers.")
        lines.append("  Headlines are matched against the index by cosine similarity.")
        _write(self._path("market_index.txt"), "\n".join(lines) + "\n")

    async def _write_engine_comparison(self) -> None:
        lines = [
            _header("ENGINE COMPARISON"),
            "  Each engine runs the same headlines with different trading rules.",
            "  Balances and skip reasons show how each strategy would have performed.",
            "",
        ]

        if not self._engine_summary:
            lines.append("  (no engine data yet — waiting for first trades)")
        else:
            # Header row
            lines.append(
                f"  {'ENGINE':<20} {'EXEC':>6} {'SKIP':>6} {'STAKED':>9}  "
                f"{'AVG EDGE':>9}  {'AVG CONF':>9}  {'LAT ms':>7}  {'BALANCE':>10}  FIRST → LAST"
            )
            lines.append("  " + "─" * 110)
            for row in self._engine_summary:
                name = row["engine_name"]
                bal = self._engine_balances.get(name, 0.0)
                lat = row.get("avg_latency_ms") or 0.0
                lines.append(
                    f"  {name:<20} {row['executed_count']:>6} {row['skipped_count']:>6} "
                    f"${row['total_staked']:>8.2f}  "
                    f"{row['avg_edge']:>+9.4f}  "
                    f"{row['avg_confidence']:>8.1%}  "
                    f"{lat:>6.0f}ms  "
                    f"${bal:>9.2f}  "
                    f"{(row['first_trade'] or '')[:16]} → {(row['last_trade'] or '')[:16]}"
                )

            # Skip reason breakdown
            lines.append("")
            lines.append("  Skip reason breakdown:")
            current_engine = None
            for row in self._engine_skip_reasons:
                if row["engine_name"] != current_engine:
                    current_engine = row["engine_name"]
                    lines.append(f"    [{current_engine}]")
                lines.append(f"      {row['skip_reason']:<30} {row['count']:>4}x")

        lines.append("")
        lines.append("  See engine_results table in seulgi.db for full history.")
        _write(self._path("engine_comparison.txt"), "\n".join(lines) + "\n")

    async def _write_signal_journal(self) -> None:
        lines = [
            _header("SIGNAL JOURNAL  (non-NEUTRAL AI decisions, newest first)"),
            "  Every non-NEUTRAL signal from any engine — whether traded or skipped.",
            "  Use this to see if the AI is reasoning about the right markets.",
            "",
        ]
        if not self._journal:
            lines.append("  (no non-NEUTRAL signals yet — AI may be returning mostly NEUTRAL)")
        else:
            _DIR_SYMBOL = {"YES": "▲", "NO": "▼"}
            for e in self._journal:
                sym = _DIR_SYMBOL.get(e["direction"], "?")
                lines.append(
                    f"  [{_ts(e['ts'])}] [{e['engine']:<12}] "
                    f"{sym} {e['direction']:<4}  conf={e['confidence']:.0%}  sim={e['similarity']:.2f}"
                )
                lines.append(f"    Headline : {e['headline'][:95]}")
                lines.append(f"    Market   : {e['market'][:95]}")
                lines.append(f"    Outcome  : {e['outcome']}")
                if e["headline_id"]:
                    lines.append(f"    hid={e['headline_id'][:8]}")
                lines.append("")
        _write(self._path("signal_journal.txt"), "\n".join(lines) + "\n")

    async def _write_quant_stats(self) -> None:
        lines = [_header("QUANT STATS  — bring to quant meeting")]
        if not self._quant_stats:
            lines.append("  (no data yet — run for at least 1 hour to collect price impacts)")
            _write(self._path("quant_stats.txt"), "\n".join(lines) + "\n")
            return

        impact = self._quant_stats.get("impact", [])
        impact_24h = self._quant_stats.get("impact_24h", [])
        lat = self._quant_stats.get("latency", {})
        calibration = self._quant_stats.get("calibration", [])
        ic_pairs = self._quant_stats.get("ic_pairs", [])
        source_stats = self._quant_stats.get("source_stats", [])
        alpha_decay = self._quant_stats.get("alpha_decay", [])

        # ── 1. PREDICTION ACCURACY with Wilson CI + 24h rolling column ───────
        lines.append("")
        lines.append("  PREDICTION ACCURACY  (did price move in predicted direction within 5m?)")
        lines.append(
            f"  {'DIR':<4} {'N':>5} {'PREC':>7}  {'95% CI':>16}  "
            f"{'AVG Δ5m':>9} {'AVG|Δ5m|':>9}   {'24h N':>6} {'24h PREC':>9}"
        )
        lines.append("  " + "─" * 84)
        by_dir_24h = {r["direction"]: r for r in impact_24h}
        for row in impact:
            n = row["n"] or 0
            hits = row["hits"] or 0
            prec = hits / n if n else 0.0
            lo, hi = _wilson_ci(hits, n)
            avg_d = row["avg_delta_t5m"] or 0.0
            avg_abs = row["avg_abs_delta_t5m"] or 0.0
            r24 = by_dir_24h.get(row["direction"], {})
            n24 = r24.get("n") or 0
            prec24 = (r24.get("hits") or 0) / n24 if n24 else 0.0
            ci_str = f"{lo:.1%}–{hi:.1%}"
            sig = " ✓" if n >= 30 and lo > 0.50 else ""
            lines.append(
                f"  {row['direction']:<4} {n:>5} {prec:>6.1%}  {ci_str:>16}  "
                f"{avg_d:>+8.4f} {avg_abs:>8.4f}   {n24:>6} {prec24:>8.1%}{sig}"
            )
        lines.append("")
        lines.append("  ✓ = lower CI bound > 50% with N ≥ 30  (statistically significant alpha)")

        # ── 2. INFORMATION COEFFICIENT ──────────────────────────────────────
        if ic_pairs:
            xs = [p[0] for p in ic_pairs]
            ys = [p[1] for p in ic_pairs]
            ic = _pearson(xs, ys)
            lines.append("")
            lines.append("  INFORMATION COEFFICIENT  (Pearson correlation: signed_confidence vs Δ5m)")
            lines.append(f"    IC = {ic:+.4f}   N = {len(ic_pairs)}")
            if abs(ic) >= 0.10:
                lines.append("    Interpretation: STRONG signal (|IC| ≥ 0.10).")
            elif abs(ic) >= 0.05:
                lines.append("    Interpretation: useful alpha (0.05 ≤ |IC| < 0.10).")
            else:
                lines.append("    Interpretation: weak / noise (|IC| < 0.05).")

        # ── 3. CALIBRATION CURVE ────────────────────────────────────────────
        if calibration:
            lines.append("")
            lines.append("  CALIBRATION  (does conf=0.7 → 70% hit rate?)")
            lines.append(f"  {'CONF':<5} {'N':>5}  {'HIT RATE':>9}  {'AVG|Δ5m|':>10}")
            lines.append("  " + "─" * 38)
            for row in calibration:
                n = row["n"] or 0
                hits = row["hits"] or 0
                rate = hits / n if n else 0.0
                avg_abs = row["avg_abs_delta"] or 0.0
                lines.append(
                    f"  {row['conf_bucket']:<5.1f} {n:>5}  {rate:>8.1%}  {avg_abs:>9.4f}"
                )
            lines.append("  Monotonically rising = model is well-calibrated.")
            lines.append("  Flat line = overconfident; threshold tuning may help.")

        # ── 4. SOURCE STRATIFICATION ────────────────────────────────────────
        if source_stats:
            lines.append("")
            lines.append("  BY SOURCE  (news vs twitter vs truth_social)")
            lines.append(
                f"  {'SOURCE':<14} {'N':>5}  {'HIT RATE':>9}  {'AVG|Δ5m|':>10}  {'AVG CONF':>9}"
            )
            lines.append("  " + "─" * 60)
            for row in source_stats:
                n = row["n"] or 0
                hits = row["hits"] or 0
                rate = hits / n if n else 0.0
                avg_abs = row["avg_abs_delta"] or 0.0
                avg_conf = row["avg_confidence"] or 0.0
                lines.append(
                    f"  {row['source']:<14} {n:>5}  {rate:>8.1%}  "
                    f"{avg_abs:>9.4f}  {avg_conf:>8.1%}"
                )

        # ── 5. ALPHA DECAY ──────────────────────────────────────────────────
        if alpha_decay:
            lines.append("")
            lines.append("  ALPHA DECAY  (hit rate at each time bucket; rows where 60m is filled)")
            lines.append(
                f"  {'DIR':<4} {'N':>5}  {'1m':>6} {'5m':>6} {'15m':>6} {'30m':>6} {'60m':>6}"
            )
            lines.append("  " + "─" * 46)
            for row in alpha_decay:
                lines.append(
                    f"  {row['direction']:<4} {row['n']:>5}  "
                    f"{(row['hit_t1m']  or 0):>5.1%} "
                    f"{(row['hit_t5m']  or 0):>5.1%} "
                    f"{(row['hit_t15m'] or 0):>5.1%} "
                    f"{(row['hit_t30m'] or 0):>5.1%} "
                    f"{(row['hit_t60m'] or 0):>5.1%}"
                )
            lines.append("  hit(1m) > hit(60m) → alpha decays fast → speed matters.")
            lines.append("  hit(60m) > hit(1m) → market digests slowly → wide entry window.")

        # ── 6. SIGNAL LATENCY ───────────────────────────────────────────────
        lines.append("")
        lines.append("  SIGNAL LATENCY  (headline detection → trade decision, includes AI call)")
        n_lat = lat.get("n") or 0
        if n_lat:
            lines.append(f"    Samples : {n_lat}")
            lines.append(f"    Average : {lat.get('avg_ms') or 0:.0f} ms")
            lines.append(f"    Min     : {lat.get('min_ms') or 0:.0f} ms")
            lines.append(f"    Max     : {lat.get('max_ms') or 0:.0f} ms")
            lines.append("")
            lines.append("    Target: < 5000ms (5s) headline-to-trade.")
            lines.append("    Even at 30s we beat human traders — FPGAs are sub-1ms.")
        else:
            lines.append("    (no latency data yet)")

        _write(self._path("quant_stats.txt"), "\n".join(lines) + "\n")

    async def _write_usage(self) -> None:
        """
        External-spend log: cumulative AI tokens and Brave Search calls.
        Includes a rough cost estimate at $0.10/M tokens (gemini-flash-lite)
        and $3/M for premium. Brave free tier = 2k/month.
        """
        lines = [
            _header("EXTERNAL USAGE  (lifetime tokens + searches)"),
            "  Tracks every AI token consumed and every Brave Search API call",
            "  across all engines and watchers. Use it to spot runaway costs.",
            "",
        ]
        if not self._usage:
            lines.append("  (no usage data yet)")
            _write(self._path("usage.txt"), "\n".join(lines) + "\n")
            return

        ai_total = int(self._usage.get("ai_total") or 0)
        brave_total = int(self._usage.get("brave_total") or 0)
        ai_24h = int(self._usage.get("ai_24h") or 0)
        brave_24h = int(self._usage.get("brave_24h") or 0)
        ai_by_provider = self._usage.get("ai_by_provider", [])

        # Rough cost: assume flash-lite price $0.10/M input + $0.40/M output ≈ $0.25/M blended
        est_cost = ai_total / 1_000_000.0 * 0.25
        est_cost_24h = ai_24h / 1_000_000.0 * 0.25

        lines.append("  AI TOKENS (OpenRouter / Gemini)")
        lines.append(f"    Lifetime : {ai_total:>10,}  tokens  (~${est_cost:.4f} blended)")
        lines.append(f"    Last 24h : {ai_24h:>10,}  tokens  (~${est_cost_24h:.4f})")
        if ai_by_provider:
            lines.append("    By model:")
            for row in ai_by_provider:
                p = row.get("provider") or "?"
                n = int(row.get("n") or 0)
                t = int(row.get("tokens") or 0)
                lines.append(f"      {p:<40} calls={n:>6}  tokens={t:>10,}")
        lines.append("")

        lines.append("  BRAVE SEARCH  (Twitter watchers)")
        lines.append(f"    Lifetime : {brave_total:>6}  searches")
        lines.append(f"    Last 24h : {brave_24h:>6}  searches")
        lines.append("    Free tier cap : ~2,000/month  (~67/day)")
        if brave_24h > 67:
            lines.append("    ⚠  24h usage exceeds free-tier daily quota.")
        lines.append("")
        lines.append("  Real-time hourly rate visible in watcher_status.txt.")
        _write(self._path("usage.txt"), "\n".join(lines) + "\n")

    async def _write_activity_log(self) -> None:
        lines = [_header("ACTIVITY LOG  (newest first)")]
        lines.extend(self._activity)
        _write(self._path("activity_log.txt"), "\n".join(lines) + "\n")

    async def _write_trace_log(self) -> None:
        """Write end-to-end trace log: hid → site → headline → market → AI → outcome."""
        lines = [_header("TRACE LOG  (newest first)")]
        lines.append(
            f"  {'TIMESTAMP':<19}  {'HID':<10}  {'SITE':<20}  "
            f"{'HEADLINE':<62}  {'MARKET':<52}  {'AI':<12}  OUTCOME"
        )
        lines.append("  " + "-" * 200)
        lines.extend(f"  {t}" for t in self._traces)
        if not self._traces:
            lines.append("  (no traces yet)")
        _write(self._path("trace_log.txt"), "\n".join(lines) + "\n")

    # ------------------------------------------------------------------
    # Phase C: rich per-trade reasoning views
    # ------------------------------------------------------------------

    # trade_log.jsonl rotation cap: keep only the last N lines. Without
    # this, append-only growth pushed the file to 85 MB / 26k lines
    # (2026-05-21 cleanup). Closed-trade history of record lives in the
    # `trades` table; the .jsonl is just for `tail -f` and grep-friendly
    # recent inspection.
    _TRADE_LOG_MAX_LINES = 5000

    async def _write_trade_log(self) -> None:
        """
        Rolling JSONL of recent closed trades with full reasoning context.
        Append on each tick, truncate to the last _TRADE_LOG_MAX_LINES
        when it exceeds 1.5× the cap so `tail -f` keeps working without
        unbounded growth.
        """
        if self._db is None:
            return
        try:
            rows = await self._db.get_closed_trades_since(self._last_trade_log_id)
        except Exception as exc:
            logger.warning("[dash] trade_log query failed: %s", exc)
            return
        if not rows:
            return
        path = self._path("trade_log.jsonl")
        try:
            with open(path, "a", encoding="utf-8") as f:
                for r in rows:
                    f.write(json.dumps(r, default=str, separators=(",", ":")) + "\n")
            self._last_trade_log_id = max(r["id"] for r in rows)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[dash] trade_log append failed: %s", exc)
            return

        # Rotate when file grows past 1.5× the cap. Use 1.5× hysteresis
        # so we don't truncate-and-rewrite every single append once we
        # cross the cap.
        try:
            with open(path, "rb") as f:
                # Cheap line count via newline scan
                n = sum(1 for _ in f)
            if n > int(self._TRADE_LOG_MAX_LINES * 1.5):
                with open(path, "r", encoding="utf-8") as f:
                    tail = f.readlines()[-self._TRADE_LOG_MAX_LINES:]
                tmp = path + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    f.writelines(tail)
                os.replace(tmp, path)
                logger.info(
                    "[dash] trade_log rotated %d → %d lines",
                    n, self._TRADE_LOG_MAX_LINES,
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("[dash] trade_log rotation failed: %s", exc)

    async def _write_variant_compare(self) -> None:
        """Per-variant pivot: trades, wins, losses, hit-rate, sum PnL."""
        if self._db is None:
            return
        try:
            stats = await self._db.get_variant_stats()
        except Exception as exc:
            logger.warning("[dash] variant_stats query failed: %s", exc)
            return
        lines = [
            _header("VARIANT COMPARISON  (all engines × variants, sorted by sum PnL)"),
            "  Each row is one strategy variant. Variants of the same engine share the AI",
            "  call but differ in entry/exit rules — compare to find which rule set wins.",
            "",
        ]
        if not stats:
            lines.append("  (no executed trades yet)")
        else:
            lines.append(
                f"  {'VARIANT':<28}  {'TRADES':>6} {'OPEN':>5} {'WINS':>5} {'LOSS':>5}  "
                f"{'HIT%':>6}  {'SUM PnL':>9}  {'AVG PnL':>8}  {'AVG CONF':>8}  "
                f"{'AVG EDGE':>9}  {'STAKED':>9}  {'INV':>4}"
            )
            lines.append("  " + "─" * 130)
            for r in stats:
                closed = (r["n_wins"] or 0) + (r["n_losses"] or 0)
                hit = (r["n_wins"] / closed * 100.0) if closed else 0.0
                lines.append(
                    f"  {r['variant']:<28}  {r['n_trades']:>6} {r['n_open']:>5} "
                    f"{r['n_wins']:>5} {r['n_losses']:>5}  "
                    f"{hit:>5.1f}%  {r['sum_pnl']:>+9.2f}  {r['avg_pnl']:>+8.3f}  "
                    f"{r['avg_conf']:>7.1%}  {r['avg_edge']:>+9.4f}  "
                    f"{r['total_staked']:>9.2f}  {r['n_inverted']:>4}"
                )
        _write(self._path("variant_compare.txt"), "\n".join(lines) + "\n")

    async def _write_wins_losses(self) -> None:
        """Top winners and losers with full reasoning context."""
        if self._db is None:
            return
        try:
            wins, losses = await self._db.get_top_wins_losses(limit=50)
        except Exception as exc:
            logger.warning("[dash] wins/losses query failed: %s", exc)
            return
        for path, label, rows in (
            ("wins.txt",   "TOP WINS",   wins),
            ("losses.txt", "TOP LOSSES", losses),
        ):
            lines = [_header(f"{label}  (top 50 by realized PnL)"), ""]
            if not rows:
                lines.append("  (none yet)")
            else:
                for r in rows:
                    inv = " [INV]" if (r.get("inverted") or 0) else ""
                    mid_y = r.get("mid_yes")
                    mid_n = r.get("mid_no")
                    mid_y_s = f"{mid_y:.3f}" if mid_y is not None else "  —  "
                    mid_n_s = f"{mid_n:.3f}" if mid_n is not None else "  —  "
                    lines.append(
                        f"  PnL={r['realized_pnl']:>+8.3f}  [{r['variant']}]{inv}  "
                        f"{r['direction']} ${r['size_usdc']:.0f} "
                        f"@ {r['entry_price']:.3f}  edge={r['signed_edge']:+.4f}  "
                        f"conf={r['confidence']:.0%}  agree={(r.get('agreement') or 0):.2f}  "
                        f"close={r.get('close_reason') or '?'}"
                    )
                    lines.append(f"    yes/no rails  : {mid_y_s} / {mid_n_s}")
                    lines.append(f"    Market   : {(r.get('question') or '')[:95]}")
                    if r.get("ai_reason"):
                        lines.append(f"    AI says  : {r['ai_reason'][:120]}")
                    lines.append("")
            _write(self._path(path), "\n".join(lines) + "\n")

    async def _write_headline_journal(self) -> None:
        """One JSONL line per headline + linked trades + price context."""
        if self._db is None:
            return
        try:
            rows = await self._db.get_headline_summary(limit=500)
        except Exception as exc:
            logger.warning("[dash] headline_summary query failed: %s", exc)
            return
        path = self._path("headline_journal.jsonl")
        try:
            with open(path, "w", encoding="utf-8") as f:
                for r in rows:
                    f.write(json.dumps(r, default=str, separators=(",", ":")) + "\n")
        except Exception as exc:  # noqa: BLE001
            logger.warning("[dash] headline_journal write failed: %s", exc)

    async def _write_headline_winners(self) -> None:
        """Group by headline; show headlines where ≥2 variants traded, ranked by sum PnL."""
        if self._db is None:
            return
        try:
            rows = await self._db.get_headline_winners(limit=40, min_variants=2)
        except Exception as exc:
            logger.warning("[dash] headline_winners query failed: %s", exc)
            return
        lines = [
            _header("HEADLINE WINNERS  (≥2 variants traded; sum PnL desc)"),
            "  Headlines where the signal was robust across multiple strategy variants.",
            "  Single-variant flukes are filtered out.",
            "",
        ]
        if not rows:
            lines.append("  (no multi-variant trades yet — give it more time)")
        else:
            for r in rows:
                lines.append(
                    f"  Σ PnL={r['sum_pnl']:>+8.3f}  variants={r['n_variants']}  "
                    f"wins={r['n_wins']}  losses={r['n_losses']}  "
                    f"at {(r['detected_at'] or '')[:19]}"
                )
                lines.append(f"    {(r.get('headline') or '')[:110]}")
                lines.append(f"    {_short(r.get('site_url') or '')}  hid={(r.get('headline_id') or '')[:8]}")
                lines.append("")
        _write(self._path("headline_winners.txt"), "\n".join(lines) + "\n")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _path(self, filename: str) -> str:
        return os.path.join(self._dir, filename)


# ---------------------------------------------------------------------------
# File/format utilities
# ---------------------------------------------------------------------------


def _wilson_ci(hits: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """
    95% Wilson confidence interval for a binomial proportion.

    Returns (low, high). Stable for small N (unlike the normal-approximation CI).
    """
    if n == 0:
        return 0.0, 1.0
    p = hits / n
    denom = 1.0 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    margin = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5) / denom
    return max(0.0, centre - margin), min(1.0, centre + margin)


def _pearson(xs: list[float], ys: list[float]) -> float:
    """Pearson correlation. Pure stdlib — no numpy/scipy."""
    n = len(xs)
    if n < 2:
        return 0.0
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sx2 = sum((x - mx) ** 2 for x in xs)
    sy2 = sum((y - my) ** 2 for y in ys)
    den = (sx2 * sy2) ** 0.5
    return num / den if den else 0.0


def _ts(dt) -> str:
    if dt is None:
        return "—"
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except (ValueError, TypeError):
            return dt[:19]  # best effort: truncate ISO string
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _short(url: str) -> str:
    """Strip scheme and www from a URL for compact display."""
    return url.replace("https://", "").replace("http://", "").replace("www.", "")


def _header(title: str) -> str:
    bar = "─" * 66
    return f"\n{bar}\n  {title}\n{bar}\n"


def _trade_block(lines: list[str], ev: TradeEvent) -> None:
    edge_str = f"{ev.edge:+.4f}" if ev.mid_price is not None else "n/a"
    lines.append(
        f"  [{_ts(ev.decided_at)}]  {ev.direction:<4}  conf={ev.confidence:.0%}  "
        f"sim={ev.similarity:.2f}  edge={edge_str}"
    )
    if ev.skip_reason is None:
        lines.append(f"    EXECUTED  ${ev.size_usdc:.2f} @ {ev.mid_price:.3f}")
    else:
        lines.append(f"    SKIPPED   reason={ev.skip_reason}")
    lines.append(f"    Headline : {ev.headline[:95]}")
    lines.append(f"    Question : {ev.question[:95]}")
    lines.append(f"    Market   : {ev.condition_id[:40]}")
    lines.append("")


def _write(path: str, content: str) -> None:
    """Atomic write via rename."""
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmp, path)
    except Exception as exc:
        logger.warning("[dash] failed to write %s: %s", path, exc)
        try:
            os.unlink(tmp)
        except OSError:
            pass

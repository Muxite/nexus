"""Dashboard class: state, ring buffers, event recorders, run loop.

Per-page rendering lives under `app.dashboard.pages.*`; per-plot rendering
under `app.dashboard.plots.*`. The heavy-query cache loop lives in
`app.dashboard.cache`.
"""

from __future__ import annotations

import asyncio
import collections
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from . import cache as cache_mod
from .events import (
    ClosedEvent,
    NewsEvent,
    PositionEvent,
    SystemHealth,
    TradeEvent,
    WatcherStatus,
)
from .io import _short, _ts, _write
from .registry import PAGES

logger = logging.getLogger(__name__)

_MAX_BUFFER = 50       # ring buffer size for most lists
_MAX_ACTIVITY = 500    # lines kept in activity_log.txt
_NEAR_MISS_EDGE = 0.0  # near-miss = positive edge that didn't cross threshold


class Dashboard:
    """Collects pipeline events and writes them to text files in data_dir."""

    def __init__(
        self,
        data_dir: str,
        interval_secs: float = 30.0,
        db=None,
        *,
        plot_interval_secs: float = 300.0,
    ) -> None:
        self._dir = data_dir
        self._interval = interval_secs
        self._plot_interval = plot_interval_secs
        self._plot_task: Optional[asyncio.Task] = None
        self._db = db
        # A4C.2: optional telemetry queue ref; surfaced in db_health.txt.
        self._telemetry = None
        # Phase 4B.1: optional calibrator ref for db_health tile.
        self._calibrator = None
        # Phase 4E.1: optional composite scorer ref for db_health tile.
        self._scorer = None
        self._started_at = datetime.now(timezone.utc)

        # Ring buffers
        self._news: collections.deque[NewsEvent] = collections.deque(maxlen=_MAX_BUFFER)
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
        self._engine_balances: dict[str, float] = {}
        self._total_realized_pnl: float = 0.0
        # E.7.1: active-variant filter for the lifetime leaderboard render.
        self._active_variants: set[str] = set()
        self._engine_summary: list[dict] = []
        self._engine_skip_reasons: list[dict] = []

        # Signal journal (non-NEUTRAL AI decisions with outcomes)
        self._journal: collections.deque[dict] = collections.deque(maxlen=200)

        self._quant_stats: dict = {}
        self._usage: dict = {}

        # Per-engine totals: engine_name → total_staked
        self._engine_staked: dict[str, float] = {}
        self._infinite_balance: bool = False
        # plan §1: three modes — "paper" | "infinite" | "simulate"
        self._mode: str = "paper"
        # plan §3: True while LLM is paused because no engine has free capital
        self._ai_paused: bool = False
        self._ai_paused_since: Optional[str] = None
        # Phase 4A.1: boot-cumulative count of stage-0a capital-floor skips.
        self._capital_floor_skips: int = 0
        # plan §2: starting bankroll for simulate mode (reconciliation block).
        self._simulate_total_usdc: float = 0.0
        # Cycle scope for the leaderboard queries: trades table can grow to
        # millions of rows; the current cycle's pots are what matters here.
        self._cycle_started_at: Optional[str] = None

        # Heavy-query cache (see cache.py). Five aggregate queries used to run
        # inline in _write_main every 30s with 10s timeouts each — under load
        # they piled up and starved aiosqlite's writer (B-WRITES). Now refreshed
        # by a background task every _heavy_cache_interval seconds.
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
        # Phase 2: separate read-only SeulgiDB used by the heavy-query cache
        # so cache reads never serialise with the writer connection.
        self._db_ro = None
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
        self, headline: str, question: str, direction: str,
        confidence: float, reason: str,
    ) -> None:
        # NB: `question` and `reason` kept for API stability — only the
        # activity_log line is emitted since the dedicated ai_decisions.txt
        # writer was culled in A1.5.
        now = datetime.now(timezone.utc)
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
                f"[{_ts(now)}]{pfx} TRADE {hid}{direction} ${size_usdc:.0f} "
                f"@ {mid_price:.3f}  edge={edge:+.3f}  {question[:70]}"
            )
            self._flush_now.set()
        else:
            self._declined.appendleft(ev)
            self._activity.appendleft(
                f"[{_ts(now)}]{pfx} SKIP  {hid}{direction} {skip_reason}  "
                f"{question[:70]}"
            )
            # Near miss: edge is positive but below threshold
            if skip_reason.startswith("edge_too_small") and edge > _NEAR_MISS_EDGE:
                self._near_miss.appendleft(ev)
                self._activity.appendleft(
                    f"[{_ts(now)}]{pfx} NEAR  {direction} edge={edge:+.3f} "
                    f"(missed!)  {question[:70]}"
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

    def set_calibrator(self, calibrator) -> None:
        """Phase 4B.1: attach per-direction calibrator for db_health tile."""
        self._calibrator = calibrator

    def set_scorer(self, scorer) -> None:
        """Phase 4E.1: attach composite scorer for db_health tile."""
        self._scorer = scorer

    def set_telemetry(self, telemetry) -> None:
        """A4C.2: attach async telemetry queue for db_health tile."""
        self._telemetry = telemetry

    def set_balance(self, engine_name: str, balance: float) -> None:
        self._engine_balances[engine_name] = balance

    def register_engines(self, engine_names: list[str]) -> None:
        """Pre-seed per-engine balance/staked dicts to 0.0 so every engine
        appears in the dashboard from the first tick.

        Also records the active-variant set so the lifetime leaderboard can
        hide retired variants (E.7.1).
        """
        for name in engine_names:
            self._engine_balances.setdefault(name, 0.0)
            self._engine_staked.setdefault(name, 0.0)
        self._active_variants = set(engine_names)

    def set_engine_summary(self, summary: list[dict], skip_reasons: list[dict]) -> None:
        self._engine_summary = summary
        self._engine_skip_reasons = skip_reasons

    def set_open_positions(self, positions: list[PositionEvent]) -> None:
        self._open_positions = positions

    def set_system_health(self, health: SystemHealth) -> None:
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
        """plan §3 — runner toggles when LLM suspended for cost discipline."""
        if paused == self._ai_paused:
            return
        self._ai_paused = paused
        self._ai_paused_since = datetime.now(timezone.utc).isoformat(timespec="seconds")

    def set_capital_floor_skips(self, n: int) -> None:
        """Phase 4A.1: boot-cumulative count of stage-0a capital-floor skips."""
        self._capital_floor_skips = int(n)

    # ------------------------------------------------------------------
    # Background loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Write all dashboard files every interval_secs, or sooner on flush."""
        # Start the heavy-query background refresher exactly once. If it
        # dies the render loop keeps serving stale data rather than crashing.
        if self._db is not None and self._heavy_cache_task is None:
            if self._db_ro is None:
                try:
                    from db import SeulgiDB  # type: ignore  # circular at module load
                    self._db_ro = await SeulgiDB.open_ro_clone(
                        getattr(self._db, "_path", "/data/seulgi.db")
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "[dash-cache] RO clone failed (%s) — falling back to writer conn",
                        exc,
                    )
            self._heavy_cache_task = asyncio.create_task(
                cache_mod.heavy_cache_loop(self)
            )
        if self._plot_task is None:
            self._plot_task = asyncio.create_task(self._plot_loop())
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
                if self._plot_task is not None:
                    self._plot_task.cancel()
                if self._db_ro is not None:
                    try:
                        await self._db_ro.close()
                    except Exception:  # noqa: BLE001
                        pass
                await self._write_all()  # final flush
                break
            except Exception as exc:
                logger.warning("[dash] write error: %s", exc)

    async def _write_all(self) -> None:
        """Rewrite all dashboard files via the page registry.

        Uses return_exceptions=True so one writer's failure doesn't
        silently cancel its peers via asyncio.gather's default behaviour.
        Per-task exceptions are surfaced as WARN lines with the file name
        so future regressions don't masquerade as "stale dashboard.txt".
        """
        async def _wrap(page) -> None:
            body = await page.render(self)
            if body == "":
                return
            _write(self._path(page.FILENAME), body)

        results = await asyncio.gather(
            *[_wrap(p) for p in PAGES], return_exceptions=True,
        )
        for page, result in zip(PAGES, results):
            if isinstance(result, BaseException):
                logger.warning(
                    "[dash] %s writer failed: %r",
                    page.FILENAME, result, exc_info=result,
                )

    async def _plot_loop(self) -> None:
        """Regenerate PNG plots every _plot_interval seconds.

        Plots are best-effort: a missing matplotlib (or one bad plot)
        doesn't affect the text-file tier. First render fires immediately
        so operators see plots within a few seconds of boot.
        """
        try:
            from .plots.registry import PLOTS
        except Exception as exc:  # noqa: BLE001
            logger.warning("[dash] plots unavailable (%s); text tier still on", exc)
            return
        plots_dir = self._path("plots")
        os.makedirs(plots_dir, exist_ok=True)
        from .io import atomic_write_png
        loop = asyncio.get_running_loop()
        while True:
            for plot in PLOTS:
                try:
                    png = await plot.render(self, loop=loop)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "[dash] plot %s failed: %s", plot.FILENAME, exc,
                    )
                    continue
                if png:
                    atomic_write_png(os.path.join(plots_dir, plot.FILENAME), png)
            try:
                await asyncio.sleep(self._plot_interval)
            except asyncio.CancelledError:
                raise

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _path(self, filename: str) -> str:
        return os.path.join(self._dir, filename)

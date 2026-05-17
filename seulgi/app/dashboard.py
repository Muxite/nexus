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
from datetime import datetime, timezone
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
        """
        for name in engine_names:
            self._engine_balances.setdefault(name, 0.0)
            self._engine_staked.setdefault(name, 0.0)

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
                await self._write_all()  # final flush
                break
            except Exception as exc:
                logger.warning("[dash] write error: %s", exc)

    async def _write_all(self) -> None:
        """Rewrite all dashboard files."""
        await asyncio.gather(
            self._write_main(),
            self._write_positions(),
            self._write_executed(),
            self._write_declined(),
            self._write_near_miss(),
            self._write_news(),
            self._write_ai(),
            self._write_watcher_status(),
            self._write_market_index(),
            self._write_engine_comparison(),
            self._write_activity_log(),
            self._write_trace_log(),
            self._write_signal_journal(),
            self._write_quant_stats(),
            self._write_usage(),
            self._write_trade_log(),
            self._write_variant_compare(),
            self._write_wins_losses(),
            self._write_headline_journal(),
            self._write_headline_winners(),
        )

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
        mode_label = "INFINITE MODE — gain tracked vs staked" if self._infinite_balance else "PAPER TRADING"
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
        # Per-engine unrealized PnL & open-position count (computed once,
        # reused below)
        per_engine_unreal: dict[str, float] = {}
        per_engine_open: dict[str, int] = {}
        for p in self._open_positions:
            eng = p.engine_name or "default"
            per_engine_unreal[eng] = per_engine_unreal.get(eng, 0.0) + p.unrealized_pnl
            per_engine_open[eng] = per_engine_open.get(eng, 0) + 1

        engine_names = sorted(set(self._engine_balances) | set(self._engine_staked))
        # Widen name column to fit the longest engine name (min 14).
        name_w = max(14, *(len(n) for n in engine_names)) if engine_names else 14

        if self._infinite_balance:
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
        # Each block reads from the trades table (variant_name, skip_reason,
        # confidence_raw). All three queries are cheap (~ms on indexed cols)
        # and surface what classify_batch already paid for.
        if self._db is not None:
            try:
                v_stats = await self._db.get_variant_stats()
            except Exception as exc:  # noqa: BLE001
                logger.warning("[dash] variant_stats query failed: %s", exc)
                v_stats = []
            try:
                funnel = await self._db.get_decision_funnel(since_secs=3600)
            except Exception as exc:  # noqa: BLE001
                logger.warning("[dash] decision_funnel query failed: %s", exc)
                funnel = None
            try:
                buckets = await self._db.get_confidence_buckets(since_secs=86400)
            except Exception as exc:  # noqa: BLE001
                logger.warning("[dash] confidence_buckets query failed: %s", exc)
                buckets = []
            try:
                fresh_counts = await self._db.get_freshness_counts(since_secs=3600)
            except Exception as exc:  # noqa: BLE001
                logger.warning("[dash] freshness_counts query failed: %s", exc)
                fresh_counts = []
            try:
                fresh_buckets = await self._db.get_freshness_buckets(since_secs=86400)
            except Exception as exc:  # noqa: BLE001
                logger.warning("[dash] freshness_buckets query failed: %s", exc)
                fresh_buckets = []

            lines += ["", "── VARIANT LEADERBOARD (lifetime, top 5 by sum PnL) ─────────────"]
            if not v_stats:
                lines.append("  (no executed trades yet)")
            else:
                lines.append(
                    f"  {'VARIANT':<34}  {'N':>5} {'WINS':>5} {'LOSS':>5}  "
                    f"{'HIT%':>6}  {'SUM PnL':>9}"
                )
                lines.append("  " + "─" * 76)
                for r in v_stats[:5]:
                    closed = (r.get("n_wins") or 0) + (r.get("n_losses") or 0)
                    hit = (r["n_wins"] / closed * 100.0) if closed else 0.0
                    lines.append(
                        f"  {(r['variant'] or '?')[:34]:<34}  {r['n_trades']:>5} "
                        f"{r['n_wins']:>5} {r['n_losses']:>5}  "
                        f"{hit:>5.1f}%  {r['sum_pnl']:>+9.2f}"
                    )
                lines.append("  Full table: variant_compare.txt")

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
        lines.append("")
        lines.append("  See positions_open.txt · trades_executed.txt · near_miss.txt")
        lines.append("      trades_declined.txt · news_seen.txt · activity_log.txt · profile.txt")

        _write(self._path("dashboard.txt"), "\n".join(lines) + "\n")

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

    async def _write_trade_log(self) -> None:
        """
        Append-only JSONL of every closed trade with full reasoning context.
        One row per close, in id order so `tail -f` shows new closes live.
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
            # Append-only; never rewritten. Atomic line-by-line under POSIX
            # writes since each line is < PIPE_BUF.
            with open(path, "a", encoding="utf-8") as f:
                for r in rows:
                    f.write(json.dumps(r, default=str, separators=(",", ":")) + "\n")
            self._last_trade_log_id = max(r["id"] for r in rows)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[dash] trade_log append failed: %s", exc)

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

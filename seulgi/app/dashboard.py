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


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------


class Dashboard:
    """
    Collects pipeline events and writes them to text files in data_dir.
    """

    def __init__(self, data_dir: str, interval_secs: float = 30.0) -> None:
        self._dir = data_dir
        self._interval = interval_secs
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
        self._market_count: int = 0
        self._market_last_refresh: Optional[datetime] = None
        # Per-engine balances: engine_name → balance
        self._engine_balances: dict[str, float] = {}
        self._total_realized_pnl: float = 0.0
        # Engine comparison summary rows (from DB query)
        self._engine_summary: list[dict] = []
        self._engine_skip_reasons: list[dict] = []

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

    def set_engine_summary(self, summary: list[dict], skip_reasons: list[dict]) -> None:
        """Refresh engine comparison data (called after each position monitor cycle)."""
        self._engine_summary = summary
        self._engine_skip_reasons = skip_reasons

    def set_open_positions(self, positions: list[PositionEvent]) -> None:
        self._open_positions = positions

    def set_watcher_status(self, status: WatcherStatus) -> None:
        self._watcher_statuses[status.url] = status

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
        )

    # ------------------------------------------------------------------
    # Individual file writers
    # ------------------------------------------------------------------

    async def _write_main(self) -> None:
        now = datetime.now(timezone.utc)
        uptime = now - self._started_at
        hours, rem = divmod(int(uptime.total_seconds()), 3600)
        mins = rem // 60

        n_open = len(self._open_positions)
        unrealized = sum(p.unrealized_pnl for p in self._open_positions)
        sign = "+" if unrealized >= 0 else ""

        last_news = self._news[0] if self._news else None
        last_trade = self._executed[0] if self._executed else None
        last_miss = self._near_miss[0] if self._near_miss else None
        last_skip = self._declined[0] if self._declined else None

        total_balance = sum(self._engine_balances.values()) if self._engine_balances else 0.0
        lines = [
            "╔══════════════════════════════════════════════════════════════╗",
            "║                    SEULGI  DASHBOARD                        ║",
            "╚══════════════════════════════════════════════════════════════╝",
            "",
            f"  Updated : {_ts(now)}",
            f"  Uptime  : {hours}h {mins}m",
            "",
            "── PAPER TRADING ──────────────────────────────────────────────",
            f"  Total balance   : ${total_balance:>10.2f} USDC",
        ]
        for eng_name, eng_bal in sorted(self._engine_balances.items()):
            lines.append(f"    [{eng_name}]  ${eng_bal:.2f}")
        lines += [
            f"  Open positions  : {n_open}",
            f"  Unrealized PnL  : {sign}${abs(unrealized):.2f}",
            f"  Realized PnL    : {'+' if self._total_realized_pnl >= 0 else ''}${self._total_realized_pnl:.2f}",
            f"  Trades executed : {len(self._executed)}",
            f"  Trades declined : {len(self._declined)}",
            f"  Near misses     : {len(self._near_miss)}",
            "",
            "── MARKET INDEX ────────────────────────────────────────────────",
            f"  Markets tracked : {self._market_count}",
            f"  Last refresh    : {_ts(self._market_last_refresh) if self._market_last_refresh else 'never'}",
            "",
            "── WATCHERS ────────────────────────────────────────────────────",
        ]
        for url, ws in self._watcher_statuses.items():
            status = "DEAD" if ws.is_unreachable else f"fail={ws.failures}"
            since = _ts(ws.last_headline_at) if ws.last_headline_at else "never"
            lines.append(f"  {'✓' if not ws.is_unreachable else '✗'} {_short(url):<30} last={since}  {status}")
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
                f"{'AVG EDGE':>9}  {'AVG CONF':>9}  {'BALANCE':>10}  FIRST → LAST"
            )
            lines.append("  " + "─" * 100)
            for row in self._engine_summary:
                name = row["engine_name"]
                bal = self._engine_balances.get(name, 0.0)
                lines.append(
                    f"  {name:<20} {row['executed_count']:>6} {row['skipped_count']:>6} "
                    f"${row['total_staked']:>8.2f}  "
                    f"{row['avg_edge']:>+9.4f}  "
                    f"{row['avg_confidence']:>8.1%}  "
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
    # Helpers
    # ------------------------------------------------------------------

    def _path(self, filename: str) -> str:
        return os.path.join(self._dir, filename)


# ---------------------------------------------------------------------------
# File/format utilities
# ---------------------------------------------------------------------------


def _ts(dt: Optional[datetime]) -> str:
    if dt is None:
        return "—"
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

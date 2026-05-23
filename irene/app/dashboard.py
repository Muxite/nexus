"""
## Specification

**Purpose.** Periodic snapshot writer for `data/dashboard/*.txt`. The
operator (or `cat` / `less` / `tail -f`) reads these files; the analysis
scripts also consume them. **Latency is the headline metric** — per
`docs/philosophy.md`, irene's edge is time-to-trade and every dashboard
panel must surface that.

**Public interface.**
- `Dashboard(data_dir: str | Path, db: DB, *, interval_secs: int = 30)`
- `await dash.run()` — background loop entry point. Spawn from `main.py`
  with `asyncio.create_task(dash.run())`.
- `await dash.stop()` — idempotent shutdown.
- `dash.tick()` — synchronous one-shot flush (also used by tests).

**Files written** (all atomic via `.tmp`-then-rename):
- `dashboard.txt` — operator landing page: uptime, current cycle, last
  events, current funnel rates, end-to-end latency p50/p95/p99.
- `latency.txt` — per-stage latency table. The headline diagnostic.
- `funnel.txt` — where signals get dropped (similarity / freshness /
  direction / edge / size). Decision-counts in the last hour.
- `recent_trades.txt` — last 20 trade decisions with their full chain
  (article id → market id → direction → mid → edge → outcome).
- `news_staleness.txt` — distribution of `(traded_at - published_at)`
  in seconds. The most important diagnostic for the speed thesis.

**Invariants.**
- Atomic writes only — `.tmp` files are renamed in. Readers never see
  half-written files.
- The dashboard reads the DB but never writes to it. Self-contained.
- Single instance per process; the loop is idempotent on `stop()`.
- All queries are bounded windows (`last 1h`, `last 50 rows`) so file
  sizes stay small even on a long-running soak.

**Side effects.** Reads from every irene table (articles, signals,
predictions, decisions, trades, trade_closures, price_impacts when it
exists). Writes files atomically into `data_dir`.

**Failure modes.** A query failure on ONE panel logs WARNING and leaves
the previous version of the file in place — one bad panel can't take
down the whole dashboard.

**Non-goals.**
- HTML / web UI. Plain text only.
- Plots. Numeric tables only (matplotlib was removed from seulgi for the
  same reason; the analyst tier under `scripts/analysis/` covers any
  richer visualisation).
- Pushed notifications. The dashboard is pull-only.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from app.db import DB

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Atomic write helper
# ---------------------------------------------------------------------------


def _atomic_write(path: Path, body: str) -> None:
    """Write `body` to `path` via a `.tmp` file + rename so readers always
    see a complete file. Pattern lifted from seulgi/app/dashboard.py."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(body)
    os.replace(tmp, path)


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Percentile helper (small N; sorting is fine)
# ---------------------------------------------------------------------------


def _percentiles(samples: list[float]) -> dict[str, float]:
    if not samples:
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "count": 0}
    s = sorted(samples)

    def _q(p: float) -> float:
        if not s:
            return 0.0
        idx = max(0, min(len(s) - 1, int(round((len(s) - 1) * p))))
        return s[idx]

    return {
        "p50": _q(0.50),
        "p95": _q(0.95),
        "p99": _q(0.99),
        "count": len(s),
    }


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------


@dataclass
class Dashboard:
    data_dir: Path
    db: DB
    interval_secs: int = 30
    _task: Optional[asyncio.Task] = None
    _stopped: bool = False

    def __post_init__(self) -> None:
        self.data_dir = Path(self.data_dir)
        self._t_boot = time.monotonic()

    async def run(self) -> None:
        while True:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — one bad tick can't kill the loop
                logger.exception("dashboard tick failed; continuing")
            await asyncio.sleep(self.interval_secs)

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._task = None

    async def tick(self) -> None:
        """One snapshot pass. Each panel is independent — a failure on
        any one logs WARNING and leaves its file alone."""
        for name, fn in (
            ("dashboard.txt", self._render_main),
            ("latency.txt", self._render_latency),
            ("funnel.txt", self._render_funnel),
            ("recent_trades.txt", self._render_recent_trades),
            ("news_staleness.txt", self._render_news_staleness),
        ):
            try:
                body = await fn()
                _atomic_write(self.data_dir / name, body)
            except Exception:  # noqa: BLE001
                logger.exception("dashboard panel %r failed; leaving file alone", name)

    # ---- panels ---------------------------------------------------------

    async def _render_main(self) -> str:
        uptime = time.monotonic() - self._t_boot
        lines = [
            f"# irene dashboard ({_now().isoformat()})",
            f"uptime: {uptime:.0f}s",
            "",
        ]
        # Counts in the last hour
        for sql, label in (
            ("SELECT COUNT(*) FROM articles WHERE ingested_at >= datetime('now','-1 hour')", "articles ingested (1h)"),
            ("SELECT COUNT(*) FROM signals  WHERE created_at  >= datetime('now','-1 hour')", "signals matched   (1h)"),
            ("SELECT COUNT(*) FROM trades   WHERE decided_at  >= datetime('now','-1 hour')", "trades decided    (1h)"),
            ("SELECT COUNT(*) FROM decisions WHERE outcome='execute' AND created_at >= datetime('now','-1 hour')", "trades executed   (1h)"),
            ("SELECT COUNT(*) FROM decisions WHERE outcome='reject'  AND created_at >= datetime('now','-1 hour')", "trades rejected   (1h)"),
        ):
            n = await self._scalar(sql)
            lines.append(f"  {label}: {n}")
        lines.append("")
        # End-to-end latency (article ingest → trade)
        e2e = await self._end_to_end_latency_ms()
        if e2e["count"]:
            lines += [
                "end-to-end latency (article.ingested → trade.decided), last 1h:",
                f"  p50={e2e['p50']:>8.0f} ms",
                f"  p95={e2e['p95']:>8.0f} ms",
                f"  p99={e2e['p99']:>8.0f} ms",
                f"  n  ={e2e['count']:>8d}",
            ]
        else:
            lines.append("end-to-end latency: no completed trades in the last hour.")
        return "\n".join(lines) + "\n"

    async def _render_latency(self) -> str:
        """Per-stage latency table.

        For each adjacent (T_k → T_{k+1}) in the article-life timeline,
        report the millisecond p50/p95/p99 of (T_{k+1} - T_k) over the
        last hour. The five canonical stages:

            ingest → dedupe → match → trade → close
            (article.ingested_at → cluster.created_at → signal.created_at
             → decision.created_at → trade_closures.closed_at)
        """
        lines = [
            f"# irene latency ({_now().isoformat()})",
            "  per-stage diff p50 / p95 / p99 (ms), last 1h",
            "",
        ]
        for label, sql in self._latency_panels_sql():
            samples = await self._fetch_ms(sql)
            p = _percentiles(samples)
            lines.append(
                f"  {label:<28} "
                f"p50={p['p50']:>7.0f} "
                f"p95={p['p95']:>7.0f} "
                f"p99={p['p99']:>7.0f} "
                f"n={p['count']:>5d}"
            )
        return "\n".join(lines) + "\n"

    def _latency_panels_sql(self) -> list[tuple[str, str]]:
        """Each row: (label, SELECT that returns ms diffs).

        We measure the delta between adjacent stage timestamps. Some are
        denormalised (e.g. articles → signals via article_id join). All
        windowed at last 1h.
        """
        return [
            (
                "ingest → dedupe",
                """
                SELECT (julianday(c.created_at) - julianday(a.ingested_at)) * 86400000.0 AS ms
                FROM article_clusters c
                JOIN article_cluster_members m ON m.cluster_id = c.id
                JOIN articles a ON a.id = m.article_id
                WHERE c.created_at >= datetime('now','-1 hour')
                """,
            ),
            (
                "ingest → match",
                """
                SELECT (julianday(s.created_at) - julianday(a.ingested_at)) * 86400000.0 AS ms
                FROM signals s
                JOIN articles a ON a.id = s.article_id
                WHERE s.created_at >= datetime('now','-1 hour')
                """,
            ),
            (
                "ingest → trade",
                """
                SELECT (julianday(t.decided_at) - julianday(a.ingested_at)) * 86400000.0 AS ms
                FROM trades t
                JOIN signals s ON s.id = t.signal_id
                JOIN articles a ON a.id = s.article_id
                WHERE t.decided_at >= datetime('now','-1 hour')
                """,
            ),
            (
                "match → trade (gate cascade)",
                """
                SELECT (julianday(d.created_at) - julianday(s.created_at)) * 86400000.0 AS ms
                FROM decisions d
                JOIN signals s ON s.id = d.signal_id
                WHERE d.created_at >= datetime('now','-1 hour')
                """,
            ),
            (
                "news age at trade-time",
                """
                SELECT (julianday(t.decided_at) - julianday(a.published_at)) * 86400000.0 AS ms
                FROM trades t
                JOIN signals s ON s.id = t.signal_id
                JOIN articles a ON a.id = s.article_id
                WHERE t.decided_at >= datetime('now','-1 hour')
                  AND a.published_at IS NOT NULL
                """,
            ),
        ]

    async def _render_funnel(self) -> str:
        lines = [
            f"# irene funnel ({_now().isoformat()})",
            "  decision outcomes in the last 1h",
            "",
        ]
        cur = await self.db.conn.execute(
            """
            SELECT reason, COUNT(*) AS n
            FROM decisions
            WHERE created_at >= datetime('now','-1 hour')
            GROUP BY reason
            ORDER BY n DESC
            """
        )
        rows = await cur.fetchall()
        await cur.close()
        if not rows:
            lines.append("  (no decisions in the last hour)")
        else:
            total = sum(r["n"] for r in rows)
            for r in rows:
                pct = 100.0 * r["n"] / max(total, 1)
                lines.append(f"  {r['reason']:<28} n={r['n']:>5d}  ({pct:>5.1f}%)")
            lines.append("")
            lines.append(f"  total decisions:           n={total}")
        return "\n".join(lines) + "\n"

    async def _render_recent_trades(self) -> str:
        lines = [
            f"# irene recent trades ({_now().isoformat()})",
            "  last 20, newest first",
            "",
            f"  {'when':<20} {'mkt':<10} {'dir':<4} {'mid':<6} {'edge':<7} {'size$':<7} reason",
        ]
        cur = await self.db.conn.execute(
            """
            SELECT decided_at, market_id, direction, mid_price, edge, size_usdc
            FROM trades
            ORDER BY decided_at DESC
            LIMIT 20
            """
        )
        rows = await cur.fetchall()
        await cur.close()
        if not rows:
            lines.append("  (no trades yet)")
            return "\n".join(lines) + "\n"
        for r in rows:
            short_mkt = (r["market_id"] or "")[:8]
            lines.append(
                f"  {r['decided_at'][:19]:<20} "
                f"{short_mkt:<10} "
                f"{r['direction']:<4} "
                f"{r['mid_price']:<6.3f} "
                f"{r['edge']:<7.3f} "
                f"{r['size_usdc']:<7.2f} "
                "executed"
            )
        return "\n".join(lines) + "\n"

    async def _render_news_staleness(self) -> str:
        """For every trade in the last hour, how old was the news when we
        bought? This is THE diagnostic for the speed thesis: a median of
        2s means we're winning the race; 60s means we're losing it."""
        cur = await self.db.conn.execute(
            """
            SELECT (julianday(t.decided_at) - julianday(a.published_at)) * 86400.0 AS sec
            FROM trades t
            JOIN signals s ON s.id = t.signal_id
            JOIN articles a ON a.id = s.article_id
            WHERE t.decided_at >= datetime('now','-1 hour')
              AND a.published_at IS NOT NULL
            """
        )
        rows = await cur.fetchall()
        await cur.close()
        samples = [float(r["sec"]) for r in rows if r["sec"] is not None]
        p = _percentiles(samples)
        lines = [
            f"# irene news staleness ({_now().isoformat()})",
            "  (traded_at - article.published_at), in seconds, last 1h",
            "  The speed thesis says this should be small. Watch the p95.",
            "",
            f"  p50: {p['p50']:>10.2f} s",
            f"  p95: {p['p95']:>10.2f} s",
            f"  p99: {p['p99']:>10.2f} s",
            f"  n  : {p['count']:>10d}",
        ]
        return "\n".join(lines) + "\n"

    # ---- helpers --------------------------------------------------------

    async def _scalar(self, sql: str) -> int:
        cur = await self.db.conn.execute(sql)
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return 0
        return int(row[0] or 0)

    async def _fetch_ms(self, sql: str) -> list[float]:
        cur = await self.db.conn.execute(sql)
        rows = await cur.fetchall()
        await cur.close()
        return [float(r["ms"]) for r in rows if r["ms"] is not None and r["ms"] >= 0]

    async def _end_to_end_latency_ms(self) -> dict[str, float]:
        samples = await self._fetch_ms(
            """
            SELECT (julianday(t.decided_at) - julianday(a.ingested_at)) * 86400000.0 AS ms
            FROM trades t
            JOIN signals s ON s.id = t.signal_id
            JOIN articles a ON a.id = s.article_id
            WHERE t.decided_at >= datetime('now','-1 hour')
            """
        )
        return _percentiles(samples)

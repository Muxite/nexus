"""
Price-impact tracker.

For every significant headline that matches a Polymarket market, record
the market token price at the moment of detection, then poll the price at
+1m, +5m, +15m, +30m, +60m. The resulting dataset lets us test the core
hypothesis:

    "Does critical news move Polymarket odds in a predictable direction
     within minutes, and can we profit by acting before the market adjusts?"

Usage
─────
    tracker = PriceImpactTracker(db, session)
    await tracker.start()                 # background polling loop

    # When a significant match is found:
    await tracker.record(
        headline_id="abc123def456",
        headline="Fed cuts rates by 25bp",
        site_url="https://apnews.com",
        condition_id="0x...",
        question="Will the Fed cut rates in 2025?",
        token_id="71321...",
        direction="YES",
        ai_confidence=0.82,
        similarity=0.74,
        source="news",
    )

Dashboard output
────────────────
  data/dashboard/price_impact.txt — sorted by absolute 5m delta, last 100 rows

Significance filter
───────────────────
Only headlines meeting ALL of:
  - AI confidence ≥ MIN_CONFIDENCE  (default 0.50)
  - similarity    ≥ MIN_SIMILARITY  (default 0.55)
are tracked. This avoids polluting the dataset with noise.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp

from db import SeulgiDB

logger = logging.getLogger(__name__)

_CLOB_PRICE_URL = "https://clob.polymarket.com/midpoint?token_id={token_id}"

# Minimum thresholds to bother tracking a match
MIN_CONFIDENCE = 0.50
MIN_SIMILARITY  = 0.55

# Polling schedule: (bucket_name, seconds_after_detection)
_BUCKETS = [
    ("t1m",  60),
    ("t5m",  300),
    ("t15m", 900),
    ("t30m", 1800),
    ("t60m", 3600),
]

_POLL_INTERVAL = 30   # check pending rows every 30 s


async def _fetch_price(session: aiohttp.ClientSession, token_id: str) -> Optional[float]:
    url = _CLOB_PRICE_URL.format(token_id=token_id)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            mid = data.get("mid")
            return float(mid) if mid is not None else None
    except Exception as exc:
        logger.debug("[impact] price fetch failed %s: %s", token_id[:12], exc)
        return None


class PriceImpactTracker:
    """
    Background service that:
    1. Records price_t0 for every significant headline/market match.
    2. Polls price at each time bucket and fills in the delta columns.
    3. Writes a human-readable summary to dashboard/price_impact.txt.
    """

    def __init__(
        self,
        db: SeulgiDB,
        dashboard_dir: str = "/data/dashboard",
    ) -> None:
        self._db = db
        self._dashboard = Path(dashboard_dir)
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        # In-memory pending set: {row_id: detected_at_unix}
        self._pending: dict[int, float] = {}

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        self._running = False
        if self._session and not self._session.closed:
            await self._session.close()

    # ------------------------------------------------------------------
    # Public: record a new impact row
    # ------------------------------------------------------------------

    async def record(
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
        source: str = "news",
    ) -> None:
        """
        Fetch the current token price and create a new price_impacts row.

        Only records if confidence ≥ MIN_CONFIDENCE and similarity ≥ MIN_SIMILARITY.
        """
        if ai_confidence < MIN_CONFIDENCE or similarity < MIN_SIMILARITY:
            return

        session = self._get_session()
        price_t0 = await _fetch_price(session, token_id)
        detected_at = datetime.now(timezone.utc).isoformat()

        row_id = await self._db.insert_price_impact(
            headline_id=headline_id,
            headline=headline,
            site_url=site_url,
            condition_id=condition_id,
            question=question,
            token_id=token_id,
            direction=direction,
            ai_confidence=ai_confidence,
            similarity=similarity,
            price_t0=price_t0,
            detected_at=detected_at,
            source=source,
        )

        self._pending[row_id] = time.time()
        logger.info(
            "[impact] [hid=%s] tracking %s  dir=%s  conf=%.2f  sim=%.2f  "
            "p0=%.3f  q=%s",
            headline_id, condition_id[:12], direction,
            ai_confidence, similarity,
            price_t0 or -1, question[:60],
        )

    # ------------------------------------------------------------------
    # Background loop
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the background polling loop (call once, runs until close())."""
        self._running = True
        # Reload any pending rows from DB (survive restarts)
        pending_rows = await self._db.get_pending_price_impacts()
        for row in pending_rows:
            detected_dt = datetime.fromisoformat(row["detected_at"])
            if detected_dt.tzinfo is None:
                detected_dt = detected_dt.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - detected_dt).total_seconds()
            if age < 3600:
                self._pending[row["id"]] = time.time() - age
        if self._pending:
            logger.info("[impact] resumed %d pending impact rows", len(self._pending))

        asyncio.create_task(self._poll_loop())
        asyncio.create_task(self._dashboard_loop())

    async def _poll_loop(self) -> None:
        """Periodically fill in time-bucket price snapshots."""
        while self._running:
            await asyncio.sleep(_POLL_INTERVAL)
            if not self._pending:
                continue

            # Load fresh rows from DB
            pending_rows = await self._db.get_pending_price_impacts()
            if not pending_rows:
                self._pending.clear()
                continue

            session = self._get_session()
            now = time.time()

            for row in pending_rows:
                row_id = row["id"]
                detected_dt = datetime.fromisoformat(row["detected_at"])
                if detected_dt.tzinfo is None:
                    detected_dt = detected_dt.replace(tzinfo=timezone.utc)
                age_s = (datetime.now(timezone.utc) - detected_dt).total_seconds()
                token_id = row["token_id"]
                if not token_id:
                    continue

                for bucket, offset_s in _BUCKETS:
                    col = f"price_{bucket}"
                    if row.get(col) is not None:
                        continue           # already filled
                    if age_s >= offset_s:  # time to fill this bucket
                        price = await _fetch_price(session, token_id)
                        if price is not None:
                            await self._db.update_price_impact_snapshot(row_id, bucket, price)
                            p0 = row.get("price_t0") or 0.0
                            delta = price - p0
                            logger.info(
                                "[impact] [hid=%s] %s bucket=%s  price=%.3f  "
                                "delta=%+.3f  q=%s",
                                row["headline_id"], condition_id_short(row["condition_id"]),
                                bucket, price, delta, row["question"][:50],
                            )

            # Remove rows whose 60m snapshot is filled
            done_ids = {r["id"] for r in pending_rows if r.get("price_t60m") is not None}
            for rid in done_ids:
                self._pending.pop(rid, None)

    async def _dashboard_loop(self) -> None:
        """Write price_impact.txt every 60 s."""
        while self._running:
            await asyncio.sleep(60)
            await self._write_dashboard()

    async def _write_dashboard(self) -> None:
        try:
            rows = await self._db.get_price_impact_stats(limit=100)
            pending = await self._db.get_pending_price_impacts()
            self._dashboard.mkdir(parents=True, exist_ok=True)
            path = self._dashboard / "price_impact.txt"
            path.write_text(_format_dashboard(rows, pending))
        except Exception as exc:
            logger.debug("[impact] dashboard write error: %s", exc)


# ------------------------------------------------------------------
# Formatting helpers
# ------------------------------------------------------------------

def condition_id_short(cid: str) -> str:
    return cid[:12] if cid else "?"


def _format_dashboard(completed: list[dict], pending: list[dict]) -> str:
    lines = [
        "──────────────────────────────────────────────────────────────────",
        f"  PRICE IMPACT TRACKER  ({len(completed)} completed, {len(pending)} pending)",
        "──────────────────────────────────────────────────────────────────",
        "",
    ]

    # Hypothesis summary from completed rows
    if completed:
        lines += _hypothesis_summary(completed)
        lines += ["", "── TOP MOVES (sorted by |Δ5m|) ─────────────────────────────────────", ""]
        sorted_rows = sorted(
            completed,
            key=lambda r: abs(r.get("delta_t5m") or 0),
            reverse=True,
        )[:30]
        lines.append(
            f"  {'HEADLINE':<50} {'DIR':>4} {'CONF':>5} {'SIM':>5} "
            f"{'p0':>6} {'Δ1m':>6} {'Δ5m':>6} {'Δ15m':>7} {'Δ60m':>7}"
        )
        lines.append("  " + "─" * 105)
        for r in sorted_rows:
            hl = (r.get("headline") or "")[:49]
            d = r.get("direction", "?")
            conf = r.get("ai_confidence") or 0.0
            sim  = r.get("similarity") or 0.0
            p0   = r.get("price_t0")
            d1   = r.get("delta_t1m")
            d5   = r.get("delta_t5m")
            d15  = r.get("delta_t15m")
            d60  = r.get("delta_t60m")
            src  = r.get("source", "")[:6]

            def fmt(v):
                return f"{v:+.3f}" if v is not None else "  n/a"

            lines.append(
                f"  {hl:<50} {d:>4} {conf:>5.2f} {sim:>5.2f} "
                f"{(p0 or 0):>6.3f} {fmt(d1):>6} {fmt(d5):>6} "
                f"{fmt(d15):>7} {fmt(d60):>7}  [{src}]"
            )

    # Pending rows
    if pending:
        lines += ["", "── PENDING (awaiting price snapshots) ───────────────────────────────", ""]
        for r in pending[:20]:
            hl = (r.get("headline") or "")[:60]
            buckets_done = sum(
                1 for b in ("price_t1m", "price_t5m", "price_t15m", "price_t30m", "price_t60m")
                if r.get(b) is not None
            )
            lines.append(
                f"  [{r.get('source','news'):>10}] [{buckets_done}/5] {hl}"
            )

    lines.append("")
    return "\n".join(lines)


def _hypothesis_summary(rows: list[dict]) -> list[str]:
    """Aggregate stats: when direction=YES and delta>0, did we win?"""
    lines = ["── HYPOTHESIS TEST ──────────────────────────────────────────────────", ""]

    # For each direction, count hits (delta_t5m in predicted direction)
    hits = {"YES": 0, "NO": 0}
    total = {"YES": 0, "NO": 0}
    avg_delta = {"YES": [], "NO": []}

    for r in rows:
        d = r.get("direction", "")
        d5 = r.get("delta_t5m")
        if d5 is None or d not in ("YES", "NO"):
            continue
        total[d] += 1
        avg_delta[d].append(d5)
        # A "hit" = price moved in the predicted direction
        if (d == "YES" and d5 > 0) or (d == "NO" and d5 < 0):
            hits[d] += 1

    for d in ("YES", "NO"):
        n = total[d]
        if n == 0:
            lines.append(f"  {d}: no data")
            continue
        hit_rate = 100 * hits[d] / n
        mean_d = sum(avg_delta[d]) / n
        lines.append(
            f"  Direction={d}  n={n}  hit_rate={hit_rate:.0f}%  "
            f"mean_Δ5m={mean_d:+.4f}"
        )

    # Combined: avg |delta| at 5m for all significant matches
    all_d5 = [abs(r.get("delta_t5m") or 0) for r in rows if r.get("delta_t5m") is not None]
    if all_d5:
        lines.append(f"\n  Mean |Δ5m| = {sum(all_d5)/len(all_d5):.4f}  "
                     f"Max |Δ5m| = {max(all_d5):.4f}  n={len(all_d5)}")
        lines.append("  (>0.01 Δ5m suggests markets do react to news within 5 minutes)")

    # Per-source breakdown
    by_source: dict[str, dict] = {}
    for r in rows:
        d = r.get("direction", "")
        d5 = r.get("delta_t5m")
        src = r.get("source") or "news"
        if d5 is None or d not in ("YES", "NO"):
            continue
        s = by_source.setdefault(src, {"n": 0, "hits": 0, "abs_sum": 0.0})
        s["n"] += 1
        s["abs_sum"] += abs(d5)
        if (d == "YES" and d5 > 0) or (d == "NO" and d5 < 0):
            s["hits"] += 1
    if by_source:
        lines.append("")
        lines.append("  BY SOURCE:")
        for src, s in sorted(by_source.items(), key=lambda kv: -kv[1]["n"]):
            n = s["n"]
            rate = 100 * s["hits"] / n if n else 0
            avg_abs = s["abs_sum"] / n if n else 0.0
            lines.append(
                f"    {src:<14} n={n:>4}  hit_rate={rate:>3.0f}%  avg|Δ5m|={avg_abs:.4f}"
            )

    return lines

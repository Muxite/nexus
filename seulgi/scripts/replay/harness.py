"""
ReplayHarness — re-injects historical headlines through the production
EngineRunner against an isolated test DB.

Why: validates the database write path under controlled load WITHOUT
waiting for live news. Originally built to verify the B-WRITES fix
(Phase A/B/C, 2026-05-21).

Pipeline:
    src_db (read-only)   — historical signals + trades for replay payload
       ↓
    ReplayTriple list    — built once via load_headlines()
       ↓
    inject_one(triple)   — calls runner.process(RawHeadline(...))
       ↓
    test_db (writer)     — fresh schema, all writes land here
       ↓
    assert_no_gaps()     — count(*) vs max(id) per table; pass = zero gaps
"""
from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import sys
import time
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "app"))

import engine as engine_mod  # for monkey-patching _fetch_mid_price
from config import load_config
from db import SeulgiDB
from engine import EngineRunner, build_engines
from markets import MarketMatch
from watcher import RawHeadline

from mocks import (
    HistoricalTrade,
    MockArticleFetcher,
    MockDirectionClassifier,
    MockEmbedder,
    MockMarketIndex,
    MockNewsMemory,
    MockRelevanceFilter,
    ReplayTriple,
    make_mock_fetch_mid_price,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass
class TableState:
    table: str
    count: int
    max_id: Optional[int]
    gap: int

    @property
    def ok(self) -> bool:
        return self.gap == 0


@dataclass
class ReplayResult:
    n_headlines: int
    n_injected: int
    n_failed: int
    table_states: list[TableState] = field(default_factory=list)
    write_counters: dict = field(default_factory=dict)
    elapsed_secs: float = 0.0
    per_headline_latency_ms: list[float] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(t.ok for t in self.table_states) and self.n_failed == 0


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


class ReplayHarness:
    """One-shot harness. Build, setup, replay, report.

    Usage:
        h = ReplayHarness(src_db_path="seulgi/data/seulgi.db",
                          test_db_path="/tmp/replay.db")
        await h.setup()
        triples = await h.load_headlines(n=100)
        result = await h.run(triples, concurrency=4)
        await h.close()
        print(result.ok, result.table_states)
    """

    # Tables we measure for row-ID gaps. signals/trades/engine_results are
    # the B-WRITES victims; usage_events is a separate fan-out (log_usage).
    # positions has no autoincrement (keyed by trade_id) so excluded.
    GAP_TABLES = ("signals", "trades", "engine_results", "usage_events")

    def __init__(
        self,
        src_db_path: str,
        test_db_path: str,
        n_engines_cap: Optional[int] = None,
        log_level: int = logging.WARNING,
        config_path: Optional[str] = None,
    ) -> None:
        self._src_path = src_db_path
        self._test_path = test_db_path
        self._n_engines_cap = n_engines_cap
        self._config_path = config_path
        self._test_db: Optional[SeulgiDB] = None
        self._runner: Optional[EngineRunner] = None
        self._mock_index: Optional[MockMarketIndex] = None
        self._mock_classifier: Optional[MockDirectionClassifier] = None
        self._mock_relevance: Optional[MockRelevanceFilter] = None
        self._mock_news: Optional[MockNewsMemory] = None
        self._orig_fetch_mid: Optional[callable] = None
        logging.basicConfig(level=log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    # ------------------------------------------------------------------
    # Setup / teardown
    # ------------------------------------------------------------------

    async def setup(self) -> None:
        """Build a fresh test DB, mocks, and an EngineRunner wired to all of
        them. Hour_gate is disabled here so replay works at any UTC hour."""
        os.environ["SEULGI_DISABLE_HOUR_GATE"] = "1"

        # Make sure the test path is empty so connect() builds fresh schema.
        if os.path.exists(self._test_path):
            os.unlink(self._test_path)
        for suffix in ("-wal", "-shm"):
            p = self._test_path + suffix
            if os.path.exists(p):
                os.unlink(p)

        # Fresh writer DB with full schema + WAL setup.
        self._test_db = SeulgiDB(self._test_path)
        await self._test_db.connect()

        # Mocks are populated empty; load_headlines fills them.
        self._mock_news = MockNewsMemory()
        self._mock_index = MockMarketIndex(matches_by_headline={})
        self._mock_classifier = MockDirectionClassifier(verdicts={})
        self._mock_relevance = MockRelevanceFilter(scores={})

        # Monkey-patch engine._fetch_mid_price so the prefetch loop returns
        # historical mids instead of hitting CLOB. Patched on the engine
        # module so EngineRunner._prefetch_prices picks it up.
        self._prices: dict[str, float] = {}
        self._orig_fetch_mid = engine_mod._fetch_mid_price
        engine_mod._fetch_mid_price = make_mock_fetch_mid_price(self._prices)

        # Build production engines against the TEST db. build_engines reads
        # cfg.engines (15 variants today). Cap if requested for a faster test.
        cfg = load_config(self._config_path)
        # We pass the mock classifier as the per-trader classifier — every
        # PaperTrader.evaluate uses self._classifier.classify_batch which
        # IS NOT actually called in process() (the runner calls its OWN
        # classifier and feeds results down), but PaperTrader holds the
        # reference so we keep it consistent.
        engines = build_engines(cfg, self._test_db, self._mock_classifier)  # type: ignore[arg-type]
        if self._n_engines_cap is not None:
            engines = engines[: self._n_engines_cap]
        # init_from_db is needed so each trader's balance is initialised.
        for e in engines:
            await e.trader.init_from_db()

        self._runner = EngineRunner(
            engines=engines,
            index=self._mock_index,  # type: ignore[arg-type]
            embedder=MockEmbedder(),  # type: ignore[arg-type]
            db=self._test_db,
            classifier=self._mock_classifier,  # type: ignore[arg-type]
            rule_engine=None,
            dashboard=None,
            news_memory=self._mock_news,  # type: ignore[arg-type]
            impact_tracker=None,
            article_fetcher=MockArticleFetcher(),  # type: ignore[arg-type]
            relevance_filter=self._mock_relevance,  # type: ignore[arg-type]
            market_context_cfg=None,
        )
        logger.info(
            "[replay] harness setup: test_db=%s engines=%d",
            self._test_path, len(engines),
        )

    async def close(self) -> None:
        if self._orig_fetch_mid is not None:
            engine_mod._fetch_mid_price = self._orig_fetch_mid
        if self._runner is not None:
            with suppress(Exception):
                await self._runner.close()
        if self._test_db is not None:
            with suppress(Exception):
                await self._test_db.close()

    # ------------------------------------------------------------------
    # Load replay payload from source DB
    # ------------------------------------------------------------------

    def load_headlines(
        self,
        n: int,
        since: Optional[str] = None,
        require_trades: bool = True,
    ) -> list[ReplayTriple]:
        """Pull N headlines from the source DB joined with their historical
        trades + market state. Returns a list of ReplayTriple, and registers
        each one's matches + verdicts + prices into the mock objects."""
        con = sqlite3.connect(self._src_path)
        con.row_factory = sqlite3.Row

        # Pick signals that actually produced trades (otherwise replay has
        # nothing for the mock classifier to return).
        join_filter = "INNER JOIN trades t ON t.signal_id = s.id" if require_trades else "LEFT JOIN trades t ON t.signal_id = s.id"
        where = ["s.headline_id IS NOT NULL"]
        if since is not None:
            where.append(f"s.detected_at >= '{since}'")
        where_sql = " AND ".join(where)

        sig_sql = f"""
            SELECT DISTINCT s.id, s.site_url, s.headline, s.detected_at,
                            s.headline_id, s.freshness_label, s.nearest_age_secs
              FROM signals s
              {join_filter}
             WHERE {where_sql}
          ORDER BY s.id DESC
             LIMIT ?
        """
        rows = con.execute(sig_sql, (n,)).fetchall()

        triples: list[ReplayTriple] = []
        for s in rows:
            hid = s["headline_id"]
            triple = ReplayTriple(
                signal_id_orig=s["id"],
                site_url=s["site_url"],
                headline=s["headline"],
                article_url=s["site_url"],  # source DB doesn't store per-headline article_url
                headline_id=hid,
                detected_at_iso=s["detected_at"],
                freshness_label=s["freshness_label"],
                nearest_age_secs=s["nearest_age_secs"],
            )
            t_rows = con.execute(
                """
                SELECT engine_name, condition_id, question, token_id,
                       direction, confidence_raw, p_alpha, mid_yes, mid_no,
                       similarity, relevance_score, relevance_reason
                  FROM trades
                 WHERE signal_id = ?
                """,
                (s["id"],),
            ).fetchall()
            for t in t_rows:
                triple.trades.append(HistoricalTrade(
                    engine_name=t["engine_name"] or "default",
                    condition_id=t["condition_id"],
                    question=t["question"] or "",
                    token_id=t["token_id"],
                    direction=t["direction"] or "NEUTRAL",
                    confidence_raw=t["confidence_raw"] or 0.0,
                    p_alpha=t["p_alpha"] or 0.0,
                    mid_yes=t["mid_yes"],
                    mid_no=t["mid_no"],
                    similarity=t["similarity"] or 0.0,
                    relevance_score=t["relevance_score"],
                    relevance_reason=t["relevance_reason"],
                ))
            triples.append(triple)
        con.close()

        # Now register the payload into the mocks so the runner sees it
        # consistently regardless of which coroutine processes a headline.
        self._register_in_mocks(triples)
        return triples

    def _register_in_mocks(self, triples: list[ReplayTriple]) -> None:
        assert self._mock_index and self._mock_classifier and self._mock_relevance
        matches_by_hid: dict[str, list[MarketMatch]] = {}
        verdicts: dict[tuple[str, str], HistoricalTrade] = {}
        rel_scores: dict[tuple[str, str], tuple[str, str]] = {}
        for triple in triples:
            seen_cids: set[str] = set()
            ms: list[MarketMatch] = []
            for t in triple.trades:
                if t.condition_id in seen_cids:
                    continue
                seen_cids.add(t.condition_id)
                ms.append(MarketMatch(
                    condition_id=t.condition_id,
                    question=t.question,
                    token_yes=t.token_id if t.direction == "YES" else None,
                    token_no=t.token_id if t.direction == "NO" else None,
                    similarity=t.similarity,
                    mid_yes=t.mid_yes,
                    mid_no=t.mid_no,
                    market_type=None,
                ))
                verdicts[(triple.headline_id, t.condition_id)] = t
                if t.relevance_score:
                    rel_scores[(triple.headline_id, t.condition_id)] = (
                        t.relevance_score, t.relevance_reason or ""
                    )
                if t.token_id:
                    if t.mid_yes is not None and t.direction == "YES":
                        self._prices[t.token_id] = t.mid_yes
                    elif t.mid_no is not None and t.direction == "NO":
                        self._prices[t.token_id] = t.mid_no
            matches_by_hid[triple.headline_id] = ms
        self._mock_index._map = matches_by_hid
        self._mock_classifier._verdicts = verdicts
        self._mock_relevance._scores = rel_scores

    # ------------------------------------------------------------------
    # Inject + run
    # ------------------------------------------------------------------

    async def inject_one(self, triple: ReplayTriple) -> float:
        """Inject one historical headline. Returns the wall-clock ms."""
        assert self._runner and self._mock_index and self._mock_classifier
        assert self._mock_relevance and self._mock_news

        # Stage mocks for this headline. Done synchronously in the caller
        # context so concurrent inject calls don't race (these are
        # set-then-immediately-read scalar slots).
        self._mock_index.stage(triple.headline_id)
        self._mock_classifier.stage_hid(triple.headline_id)
        self._mock_relevance.stage_hid(triple.headline_id)
        self._mock_news.stage(triple.freshness_label, triple.nearest_age_secs)

        try:
            detected_at = datetime.fromisoformat(triple.detected_at_iso)
            if detected_at.tzinfo is None:
                detected_at = detected_at.replace(tzinfo=timezone.utc)
        except Exception:
            detected_at = datetime.now(timezone.utc)

        headline = RawHeadline(
            site_url=triple.site_url,
            headline=triple.headline,
            article_url=triple.article_url,
            detected_at=detected_at,
            headline_id=triple.headline_id,
            source="news",
        )
        t0 = time.monotonic()
        await self._runner.process(headline)
        return (time.monotonic() - t0) * 1000.0

    async def run(
        self,
        triples: list[ReplayTriple],
        concurrency: int = 1,
        rate_multiplier: float = 0.0,
    ) -> ReplayResult:
        """Replay all triples. concurrency=1 is sequential; higher values
        use a semaphore to cap parallelism. rate_multiplier=0 means
        as-fast-as-possible; non-zero spaces injects by the original
        cadence / rate_multiplier."""
        assert self._runner is not None

        result = ReplayResult(
            n_headlines=len(triples), n_injected=0, n_failed=0,
        )

        # Concurrency semaphore + ordered timing accumulator.
        sem = asyncio.Semaphore(concurrency)
        latencies: list[float] = []
        lock = asyncio.Lock()

        async def _do(triple: ReplayTriple, delay: float) -> None:
            if delay > 0:
                await asyncio.sleep(delay)
            async with sem:
                try:
                    ms = await self.inject_one(triple)
                    async with lock:
                        latencies.append(ms)
                        result.n_injected += 1
                except Exception as exc:  # noqa: BLE001
                    async with lock:
                        result.n_failed += 1
                    logger.warning(
                        "[replay] inject failed for hid=%s: %r",
                        triple.headline_id, exc,
                    )

        # Compute per-triple delays for rate-multiplier mode.
        delays = self._compute_delays(triples, rate_multiplier)

        t_start = time.monotonic()
        await asyncio.gather(*[
            _do(triple, delay) for triple, delay in zip(triples, delays)
        ])
        result.elapsed_secs = time.monotonic() - t_start
        result.per_headline_latency_ms = sorted(latencies)

        # Collect counters + per-table gaps.
        await self._collect_state(result)
        return result

    def _compute_delays(
        self,
        triples: list[ReplayTriple],
        rate_multiplier: float,
    ) -> list[float]:
        """Returns a list of seconds-from-start delays. With
        rate_multiplier <= 0 every delay is 0 (as fast as possible)."""
        if rate_multiplier <= 0 or not triples:
            return [0.0] * len(triples)
        # Anchor on first detected_at, scale gaps by 1/rate_multiplier.
        try:
            anchor = datetime.fromisoformat(triples[0].detected_at_iso)
            if anchor.tzinfo is None:
                anchor = anchor.replace(tzinfo=timezone.utc)
        except Exception:
            return [0.0] * len(triples)
        out: list[float] = []
        for t in triples:
            try:
                ts = datetime.fromisoformat(t.detected_at_iso)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                gap = max(0.0, (ts - anchor).total_seconds()) / rate_multiplier
            except Exception:
                gap = 0.0
            out.append(gap)
        return out

    async def _collect_state(self, result: ReplayResult) -> None:
        assert self._test_db is not None
        # Run table state checks via sqlite3 (sync) — same pattern the
        # production db_health_audit.py uses.
        con = sqlite3.connect(self._test_path)
        try:
            for table in self.GAP_TABLES:
                cnt = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                try:
                    mx = con.execute(f"SELECT MAX(id) FROM {table}").fetchone()[0]
                except Exception:
                    mx = None
                gap = (mx - cnt) if (mx is not None and cnt is not None) else 0
                result.table_states.append(TableState(
                    table=table, count=cnt, max_id=mx, gap=gap,
                ))
        finally:
            con.close()
        snap = self._test_db.write_health_snapshot()
        result.write_counters = snap.get("counters", {})

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    @staticmethod
    def format_report(result: ReplayResult) -> str:
        """Markdown summary of one replay run."""
        lines: list[str] = []
        lines.append(f"# Replay report — {datetime.now(timezone.utc).isoformat()}")
        lines.append("")
        lines.append(f"- headlines: {result.n_headlines}")
        lines.append(f"- injected:  {result.n_injected}")
        lines.append(f"- failed:    {result.n_failed}")
        lines.append(f"- elapsed:   {result.elapsed_secs:.2f}s")
        if result.per_headline_latency_ms:
            ls = result.per_headline_latency_ms
            p50 = ls[len(ls) // 2]
            p95 = ls[int(len(ls) * 0.95)]
            p99 = ls[int(len(ls) * 0.99)]
            lines.append(f"- per-headline latency: p50={p50:.1f}ms p95={p95:.1f}ms p99={p99:.1f}ms")
        lines.append("")
        lines.append("## Row-ID gaps (B-WRITES verdict)")
        lines.append("")
        lines.append("| table          |  count |  max_id |  gap | OK |")
        lines.append("|----------------|-------:|--------:|-----:|:--:|")
        for ts in result.table_states:
            ok = "✅" if ts.ok else "❌"
            mx = "-" if ts.max_id is None else str(ts.max_id)
            lines.append(f"| {ts.table:<14} | {ts.count:>6} | {mx:>7} | {ts.gap:>4} | {ok} |")
        lines.append("")
        lines.append("## Write counters")
        lines.append("")
        if not result.write_counters:
            lines.append("(no writes recorded)")
        else:
            lines.append("| method                       |       ok |     err |")
            lines.append("|------------------------------|---------:|--------:|")
            for method in sorted(result.write_counters):
                c = result.write_counters[method]
                ok = c.get("ok", 0)
                err = c.get("err", 0)
                lines.append(f"| {method:<28} | {ok:>8} | {err:>7} |")
        lines.append("")
        verdict = "✅ PASS — writer is healthy" if result.ok else "❌ FAIL — see gaps above"
        lines.append(f"## Verdict: {verdict}")
        return "\n".join(lines)

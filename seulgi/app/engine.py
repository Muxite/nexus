"""Seulgi engine registry — multi-engine paper trading orchestrator.

Each engine is a PaperTrader with a named config profile. `EngineRunner`
fans every headline out to all registered engines in parallel. Engines are
declared under `engines:` in config.yaml; empty → one "default".

## Specification
**Purpose.** Pipeline orchestrator: ingress → signal → sim-gate → market
context → predict (LLM) → fan-out → batched write.

**Public interface.**
- `Engine(name, trader, fixed_balance=None)` — dataclass adapter.
- `EngineRunner(engines, index, embedder, db, classifier, …)`:
  `process(headline)`, `monitor_all_positions()`, `close()`, `engines`.
- `build_engines(cfg, db, classifier, rule_engine=None) -> list[Engine]` —
  YAML engines + variants + optional `AGGRESSIVE_CORE_ENABLE=1` core.

**Pipeline stages.** Each log line starts with `[STAGE]`:
- `[INGRESS]` — **stage 0a capital floor** (Phase 4A.1) → embed → freshness →
  match → mid-prefetch → heat. Stage 0a is a zero-cost short-circuit:
  if NO engine has `balance >= min_trade_usdc`, return before any
  LLM / embed / chromadb / CLOB / DB-insert work.
  Phase 4D.1 (2026-05-23): the legacy hardcoded `hour_gate`
  (UTC 23/00/01 silent drop) is **retired**. Late-night breaking news
  is no longer blinded at INGRESS; liquidity is gated dynamically at
  `trader._evaluate_pure` via `max_spread_bps` + `min_book_depth_usdc`.
  `SEULGI_DISABLE_HOUR_GATE` is accepted as a no-op for backward
  compatibility.
- `[SIGNAL]` — `insert_signal`, freshness short-circuit, universal pre-AI.
- `[SIM_GATE]` — low-sim short-circuit + per-match SIM_PASS/SIM_FAIL.
- `[MARKET_CONTEXT]` — parallel CLOB pre-headline snapshot.
- `[PREDICT]` — `classify_batch` (LLM or `SEULGI_BYPASS_LLM`).
- `[FAN_OUT]` — `gather` under `db.batch()` with 90 s `wait_for`; scoped
  rules + `trader.evaluate` + `record_engine_decisions` live here.
- `[WRITE]` — implicit `db.batch()` `__aexit__` commit at FAN_OUT end.
- `[BG]` — fire-and-forget tasks bound to `_bg_tasks` so the GC can't reap.

**Invariants.**
1. Exactly one `signals` row per non-short-circuited headline.
2. Each active engine gets exactly one `record_engine_decisions` call per
   headline (decisions list may be empty on per-engine crash).
3. `classify_batch` is called at most once per headline; results shared.
4. `_batched_gather` MUST complete within 90 s; on timeout the `db.batch()`
   `__aexit__` rolls back via the shielded path.
5. Sim-gate is deterministic (similarity floor); no LLM since 2026-05-22.

**Side effects.** DB writes: `signals`, `engine_results`, `usage_events`,
`market_price_snaps`, `markets_heat`; indirect via `trader.evaluate` →
`trades`, `positions`, `balances`. DB reads: `get_engine_rolling_hit_rate`.
External I/O: CLOB mid + context, embedder, classifier, article body.

**Failure modes.** Embed error: drop. `news_memory.assess`: fail-open to
BREAKING. `find_matches` empty / all-no-price: drop. `_engine_should_pause`
DB fail: engine stays active. Per-engine `evaluate` crash: decisions=[].
90 s timeout: batch rollback, lose ≤ 1 headline. (Hour gate retired in
Phase 4D.1 — see INGRESS notes; the spread+depth filter at
`trader._evaluate_pure` now owns liquidity gating.)

**Non-goals.** Does NOT size trades / compute edge / write trades / close
positions (`trader.py`), call the LLM (`ai.py`), maintain market embeddings
(`markets.py`), or decide freshness labels (`news_memory.py`).

## Notes
- Phase 4A.1 (2026-05-23): capital check now lives at INGRESS stage 0a —
  `_engines_have_capital()` is checked immediately after the hour gate
  and BEFORE article_fetch / embed / freshness / match / mid-prefetch.
  When False, `process()` returns with zero downstream cost; the
  `[PREDICT] ai_paused_no_capital` gate still runs as a defence-in-depth
  belt-and-braces check until Phase 4A.1 has soaked for ≥ 1 week.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, TYPE_CHECKING
from urllib.parse import urlparse

import aiohttp

from ai import DirectionClassifier, ClassifyResult
from config import TradingConfig, AIConfig
from db import SeulgiDB
from markets import MarketIndex, Embedder, MarketMatch
from trader import PaperTrader, TradeDecision, _fetch_mid_price
from watcher import RawHeadline

if TYPE_CHECKING:
    # Imported lazily at the call site (see `_prefetch_prices`); this makes
    # the module name resolvable to type checkers without pulling it in at
    # import time.
    import impact_tracker

# Defensive — engine.py runs in tests that don't initialise app.metrics.
try:
    import metrics as _metrics
except Exception:  # noqa: BLE001
    _metrics = None

logger = logging.getLogger(__name__)


# Verdict carrier for the deterministic sim-gate. ai.py reads `.score` +
# `.reason` for SIM_PASS/SIM_FAIL provenance in the prediction prompt;
# trader._record reads them to write the `relevance_score` column.
@dataclass
class _SimVerdict:
    score: str   # "SIM_PASS" | "SIM_FAIL"
    reason: str


@dataclass
class Engine:
    """A named PaperTrader. Engines share classifier + market index but
    keep separate balances/positions via `engine_name`."""
    name: str
    trader: PaperTrader
    # Per-engine pot (None → engine pools in simulate-mode split). main.py
    # respects this; fixed-balance engines keep their pot.
    fixed_balance: Optional[float] = None


async def _gather_or_cancel(*coros, label: str):
    """`asyncio.gather(..., return_exceptions=True)` with CancelledError
    re-raised so parent `wait_for`/shutdown semantics survive. Real
    exceptions are logged (`label` for traceability) and returned in-place.
    """
    results = await asyncio.gather(*coros, return_exceptions=True)
    for r in results:
        if isinstance(r, asyncio.CancelledError):
            raise r
    for r in results:
        if isinstance(r, BaseException):
            logger.warning("[FAN_OUT] %s sub-task error: %r", label, r)
    return results


class EngineRunner:
    """Fans every headline out to all registered engines concurrently. See
    the module docstring for the per-stage contract."""

    def __init__(
        self, engines: list[Engine], index: MarketIndex, embedder: Embedder,
        db: SeulgiDB, classifier: DirectionClassifier,
        rule_engine=None, dashboard=None, news_memory=None,
        impact_tracker=None, article_fetcher=None, market_context_cfg=None,
        min_top_similarity: float = 0.45,
        telemetry=None,
        matchers: Optional[list] = None,
        matcher_weights: Optional[list[float]] = None,
    ) -> None:
        self._engines, self._index, self._embedder = engines, index, embedder
        self._db, self._classifier, self._rules = db, classifier, rule_engine
        self._dashboard = dashboard
        self._news_memory = news_memory
        self._impact_tracker = impact_tracker
        self._article_fetcher = article_fetcher
        self._min_top_similarity = float(min_top_similarity)
        self._mkt_ctx_cfg = market_context_cfg
        # A4E.2: optional pluggable matchers passed through to
        # `MarketIndex.find_matches`. Default None → cosine-only.
        self._matchers = list(matchers or [])
        self._matcher_weights = list(matcher_weights or [])
        # A4C.2: optional async telemetry queue. A4C.1's FastPath uses
        # this to offload engine_results / decision_inputs / llm_calls /
        # trades writes off the trade-critical path. None = legacy
        # direct-write path (current behaviour preserved).
        self._telemetry = telemetry
        self._mkt_ctx_cache: dict = {}  # 30s TTL inside market_context.fetch_*
        self._price_session: Optional[aiohttp.ClientSession] = None
        # token_id → (price|None, monotonic_expiry); 10 s burst-traffic shield.
        self._price_cache: dict[str, tuple[Optional[float], float]] = {}
        self._price_cache_ttl: float = 10.0
        # A4A.3: wall-clock cap for BBO fan-out inside _prefetch_prices.
        # Per-call timeout is set inside `fetch_bbo` itself (500 ms);
        # this budget caps the parallel gather across all matched
        # tokens. On overrun we fall through to mid-only snaps —
        # ingress is never blocked on CLOB book availability.
        self._bbo_budget_s: float = 0.5
        # Held refs for fire-and-forget tasks — without this set, the GC can
        # reap an in-flight asyncio.create_task() mid-run.
        self._bg_tasks: set[asyncio.Task] = set()
        # plan §3 — simulate mode pauses LLM when no engine has free capital.
        self._simulate_mode: bool = bool(
            engines and getattr(engines[0].trader.cfg, "mode", "paper") == "simulate")
        self._ai_paused: bool = False
        # Phase 4A.1: stage-0a capital floor cache. `_capital_cache` is
        # (have_capital, expiry_monotonic). TTL is short (5 s) so a stale
        # True window can never burn more than ~5 s of headlines on a fresh
        # drain; PaperTrader._notify_balance_change clears it immediately
        # whenever a real mutation happens.
        self._capital_cache: Optional[tuple[bool, float]] = None
        self._capital_cache_ttl_secs: float = 5.0
        self._capital_floor_skips: int = 0  # boot-cumulative; surfaced in db_health
        for _e in engines:
            try:
                _e.trader.set_balance_change_callback(self._invalidate_capital_cache)
            except Exception:  # noqa: BLE001
                # Test mocks may not implement the setter; the TTL still
                # bounds staleness so the cache is safe without the hook.
                pass
        # E.5b: engines with last-50 hit-rate below floor are skipped at
        # dispatch (n >= min_n so brand-new variants aren't paused on noise).
        self._hit_rate_cache: dict[str, tuple[float, int, float]] = {}
        self._hit_rate_ttl_secs, self._hit_rate_floor = 300.0, 0.12
        self._hit_rate_min_n = 25
        self._paused_engines: set[str] = set()

    def _spawn_bg(self, coro, *, name: str) -> None:
        """Create a background task and hold a strong reference until done.
        Prevents GC-collection of orphaned tasks; surfaces unhandled exceptions.
        """
        task = asyncio.create_task(coro, name=name)
        self._bg_tasks.add(task)

        def _on_done(t: asyncio.Task) -> None:
            self._bg_tasks.discard(t)
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                logger.warning("[BG] task %s failed: %r", name, exc)

        task.add_done_callback(_on_done)

    async def _engine_should_pause(self, name: str) -> bool:
        """E.5b: True if engine's last-50 hit-rate is below the floor."""
        from time import monotonic
        now = monotonic()
        cached = self._hit_rate_cache.get(name)
        if cached is None or cached[2] < now:
            try:
                rate, n = await self._db.get_engine_rolling_hit_rate(name, 50)
            except Exception:  # noqa: BLE001
                return False
            rate_val = rate if rate is not None else 1.0  # unknown → don't pause
            self._hit_rate_cache[name] = (rate_val, n, now + self._hit_rate_ttl_secs)
        rate_val, n, _ = self._hit_rate_cache[name]
        paused = (n >= self._hit_rate_min_n and rate_val < self._hit_rate_floor)
        if paused and name not in self._paused_engines:
            self._paused_engines.add(name)
            logger.info("[FAN_OUT] E.5b PAUSE '%s' — rolling-%d hit_rate=%.1f%% < %.0f%%",
                        name, n, rate_val * 100, self._hit_rate_floor * 100)
        elif (not paused) and name in self._paused_engines:
            self._paused_engines.discard(name)
            logger.info("[FAN_OUT] E.5b RESUME '%s' — rolling-%d hit_rate=%.1f%%",
                        name, n, rate_val * 100)
        return paused

    def _ai_should_pause(self) -> bool:
        """plan §3 — every active simulate engine fully staked? Skip the LLM."""
        if not self._simulate_mode:
            return False
        for e in self._engines:
            cfg = e.trader.cfg
            if e.trader.balance >= cfg.min_trade_usdc:
                return False
        return True

    def _invalidate_capital_cache(self) -> None:
        """Phase 4A.1: bust the stage-0a capital cache. Called by
        PaperTrader._notify_balance_change after any balance mutation
        AND on every cache-miss recompute path."""
        self._capital_cache = None

    def _engines_have_capital(self) -> bool:
        """Phase 4A.1 stage-0a gate. Returns True iff ANY engine has
        `balance >= min_trade_usdc`. The check is GLOBAL on purpose: if
        even one engine can trade, the embed+match cost is recouped by
        that engine's potential trade — only when ALL engines are drained
        does the headline genuinely waste downstream work.

        Caches the bool for `_capital_cache_ttl_secs` (5 s) so repeated
        calls inside one process() tick are O(1); the trader callback
        busts the cache immediately on real mutations so we never serve
        stale True.

        No-ops (always True):
        - `SEULGI_BYPASS_LLM=1` — bypass mode still wants the full
          pipeline to exercise (cost is synthetic anyway).
        - Any engine in `infinite_balance` mode — balance is gain-only,
          `min_trade_usdc` is meaningless there.
        """
        # Bypass + infinite_balance escape hatch (no DB calls).
        if os.environ.get("SEULGI_BYPASS_LLM", "0") == "1":
            return True
        for e in self._engines:
            if getattr(e.trader, "infinite_balance", False):
                return True
        # TTL cache fast-path.
        if self._capital_cache is not None:
            have, expiry = self._capital_cache
            if expiry > time.monotonic():
                return have
        # Recompute — in-memory attribute read per engine, no DB.
        have = False
        for e in self._engines:
            cfg = e.trader.cfg
            if e.trader.balance >= cfg.min_trade_usdc:
                have = True
                break
        self._capital_cache = (have, time.monotonic() + self._capital_cache_ttl_secs)
        return have

    def _update_ai_pause_state(self) -> bool:
        """Recompute the pause flag, log on edges, notify dashboard."""
        paused = self._ai_should_pause()
        if paused != self._ai_paused:
            self._ai_paused = paused
            logger.info(
                "[PREDICT] simulate AI %s — global free=%.2f across %d engines",
                "paused (no free capital)" if paused else "resumed",
                sum(e.trader.balance for e in self._engines), len(self._engines),
            )
            if self._dashboard is not None:
                try: self._dashboard.set_ai_paused(paused)
                except Exception: pass  # noqa: BLE001, E701
        return paused

    def _get_price_session(self) -> aiohttp.ClientSession:
        if self._price_session is None or self._price_session.closed:
            self._price_session = aiohttp.ClientSession()
        return self._price_session

    async def close(self) -> None:
        if self._price_session and not self._price_session.closed:
            await self._price_session.close()

    async def process(self, headline: RawHeadline) -> None:
        """Embed → freshness → match → sim-gate → predict → fan-out."""
        _t0 = time.monotonic()
        hid = getattr(headline, "headline_id", "")

        # ── INGRESS ──────────────────────────────────────────────────────
        # Phase 4D.1 (2026-05-23): hour_gate retired. The legacy hardcoded
        # UTC 23/00/01 silent drop has been replaced by the dynamic
        # spread+depth liquidity filter at trader._evaluate_pure. The
        # `SEULGI_DISABLE_HOUR_GATE` env var is accepted as a no-op for
        # operator scripts that still set it; remove after a 1-week soak.
        # Stage 0a (Phase 4A.1, 2026-05-23): capital floor — short-circuit
        # BEFORE embed/freshness/match/CLOB/DB writes when NO engine can
        # afford a trade. The [PREDICT] _update_ai_pause_state gate below
        # is kept as belt-and-braces and will be removed after a 1-week
        # soak confirms cache invalidation is correct.
        if not self._engines_have_capital():
            self._capital_floor_skips += 1
            logger.info(
                "[INGRESS] [hid=%s] capital_floor skip — no engine has "
                "free_cash >= min_trade", hid,
            )
            if _metrics is not None:
                try:
                    _metrics.incr("engine.capital_floor_skip")
                except Exception:  # noqa: BLE001
                    pass
            if self._dashboard is not None:
                try:
                    self._dashboard.set_capital_floor_skips(self._capital_floor_skips)
                except Exception:  # noqa: BLE001
                    pass
            return
        if self._article_fetcher is not None and getattr(headline, "article_url", None):
            self._spawn_bg(
                self._article_fetcher.fetch_and_store(
                    headline_id=hid, article_url=headline.article_url,
                    site_url=headline.site_url),
                name=f"article_fetch:{hid}",
            )
        try:
            embedding = await asyncio.get_event_loop().run_in_executor(
                None, self._embedder.embed, headline.headline)
        except Exception as exc:
            logger.warning("[INGRESS] [hid=%s] embed error: %s", hid, exc)
            return
        freshness = await self._assess_freshness(headline, embedding, hid)
        # Mirror freshness on the headline so downstream can read it.
        headline._is_republish = freshness.is_republish
        headline._similar_to = freshness.similar_to
        headline._freshness_label = freshness.label
        headline._nearest_age_secs = freshness.nearest_age_secs
        matches = self._index.find_matches(
            embedding, headline=headline.headline,
            matchers=self._matchers or None,
            matcher_weights=self._matcher_weights or None,
        )
        if not matches:
            logger.debug("[INGRESS] [hid=%s] no matches: %s", hid, headline.headline[:80])
            return
        # Phase 4D.2: surface tag-routed augment matches so the new path is
        # observable. Cosine-only matches keep `tag=None`; tag-routed ones
        # carry their triggering macro category (e.g. "interest_rates").
        tag_routed = [m for m in matches if getattr(m, "tag", None)]
        if tag_routed:
            tag_set = sorted({m.tag for m in tag_routed if m.tag})
            logger.info(
                "[INGRESS] [hid=%s] tag-routing added %d matches via tags=%s",
                hid, len(tag_routed), tag_set,
            )
        await self._db.bump_market_heat([m.condition_id for m in matches])
        matches = await self._prefetch_prices(matches, hid)
        if not matches:
            logger.info("[INGRESS] [hid=%s] all matches dropped (no_price)", hid)
            return
        logger.info(
            "[INGRESS] [hid=%s] '%s' matched %d markets (freshness=%s) — "
            "dispatching to %d engines", hid, headline.headline[:60],
            len(matches), freshness.label, len(self._engines),
        )

        # ── SIGNAL ───────────────────────────────────────────────────────
        signal_id = await self._db.insert_signal(
            site_url=headline.site_url, headline=headline.headline,
            headline_id=getattr(headline, "headline_id", None),
            freshness_label=freshness.label,
            nearest_age_secs=freshness.nearest_age_secs,
        )
        sc_labels = (getattr(self._news_memory, "_short_circuit_labels", []) or []
                     if self._news_memory is not None else [])
        if freshness.label in sc_labels:
            logger.info("[SIGNAL] [hid=%s] %s short-circuit — %d×%d skipped",
                        hid, freshness.label, len(self._engines), len(matches))
            await _gather_or_cancel(*[
                e.trader.evaluate_freshness_skips(
                    signal_id, headline, matches,
                    freshness_label=freshness.label,
                    nearest_age_secs=freshness.nearest_age_secs,
                ) for e in self._engines], label="freshness_skip")
            return
        # Universal pre-AI rules; scoped rules run per-engine in _run_engine.
        pre_skips: list[tuple[MarketMatch, str]] = []
        survivors: list[MarketMatch] = []
        if self._rules is None:
            survivors = list(matches)
        else:
            for m in matches:
                r = self._rules.gate_pre_ai_universal(
                    market_type=m.market_type, mid_price=m.mid_yes,
                    freshness_label=freshness.label, question=m.question)
                if r is None:
                    survivors.append(m)
                else:
                    pre_skips.append((m, r))

        # Phase 5 follow-up: capture every signal's inputs to `decision_inputs`
        # for offline replay. Fire-and-forget; failures never block the trade
        # path. Backtest harness reads this as the primary oracle.
        try:
            import json as _json
            import urllib.parse as _urlparse
            _matches_payload = [
                {
                    "cond_id": m.condition_id,
                    "question": getattr(m, "question", None),
                    "similarity": float(getattr(m, "similarity", 0.0)),
                    "mid_yes": getattr(m, "mid_yes", None),
                    "mid_no": getattr(m, "mid_no", None),
                    "market_type": getattr(m, "market_type", None),
                    "tag": getattr(m, "tag", None),
                    "extra_score": getattr(m, "extra_score", None),
                    "bid": getattr(m, "bid", None),
                    "ask": getattr(m, "ask", None),
                }
                for m in matches
            ]
            _max_sim = max(
                (float(getattr(m, "similarity", 0.0)) for m in matches),
                default=0.0,
            )
            _emb_bytes = embedding.tobytes() if hasattr(embedding, "tobytes") else None
            _pre_skips_payload = [
                {"cond_id": m.condition_id, "reason": r}
                for (m, r) in pre_skips
            ]
            self._spawn_bg(
                self._db.insert_decision_input(
                    headline_id=hid,
                    source_host=_urlparse.urlparse(headline.site_url).netloc or None,
                    embedding_bytes=_emb_bytes,
                    matches_json=_json.dumps(_matches_payload, separators=(",", ":")),
                    freshness_label=freshness.label,
                    freshness_dist=getattr(freshness, "nearest_dist", None),
                    freshness_age_secs=freshness.nearest_age_secs,
                    pre_ai_skips_json=_json.dumps(
                        _pre_skips_payload, separators=(",", ":"),
                    ),
                    max_similarity=_max_sim,
                ),
                name=f"decision_input:{hid}",
            )
        except Exception as _exc:
            logger.debug("[SIGNAL] [hid=%s] decision_input capture failed: %r", hid, _exc)

        # ── SIM_GATE ─────────────────────────────────────────────────────
        if survivors and self._min_top_similarity > 0:
            max_sim = max(getattr(m, "similarity", 0.0) for m in survivors)
            if max_sim < self._min_top_similarity:
                logger.info(
                    "[SIM_GATE] [hid=%s] low_similarity short-circuit "
                    "(max_sim=%.3f < %.2f) — %d×%d skipped", hid, max_sim,
                    self._min_top_similarity, len(self._engines), len(survivors),
                )
                reason = f"low_similarity:{max_sim:.3f}"
                await _gather_or_cancel(*[
                    e.trader.evaluate_low_sim_skips(
                        signal_id, headline, survivors, reason=reason,
                        max_sim=max_sim,
                    ) for e in self._engines], label="low_sim_skip")
                return

        source_host: Optional[str] = None
        if headline.site_url:
            try:
                source_host = urlparse(headline.site_url).netloc or None
            except Exception:  # noqa: BLE001
                pass
        survivors, relevance_skips, rv_kept = self._sim_gate_split(survivors, hid)

        # ── MARKET_CONTEXT ───────────────────────────────────────────────
        await self._fetch_market_context(survivors, hid)

        # ── PREDICT ──────────────────────────────────────────────────────
        # TODO(Phase 4A.1 post-soak): remove this fallback gate. Stage 0a
        # already short-circuits before any embed/CLOB/DB work; keeping
        # this here for ≥ 1-week soak as belt-and-braces against a buggy
        # cache invalidation. Track removal in master-plan §4A.1.
        if survivors and self._update_ai_pause_state():
            logger.info("[PREDICT] [hid=%s] ai_paused_no_capital — %d×%d skipped",
                        hid, len(self._engines), len(survivors))
            await _gather_or_cancel(*[
                e.trader.evaluate_ai_paused_skips(signal_id, headline, survivors)
                for e in self._engines], label="ai_paused_skip")
            return

        classify_results, prompt_meta = await self._predict(
            headline, survivors, freshness, source_host, rv_kept, hid,
        )
        prompt_meta_json = (
            json.dumps(prompt_meta, separators=(",", ":")) if prompt_meta else None
        )
        pre_ms = (time.monotonic() - _t0) * 1000.0
        active_engines = [e for e in self._engines  # E.5b rolling hit-rate guard
                          if not await self._engine_should_pause(e.name)]

        # ── FAN_OUT ──────────────────────────────────────────────────────
        # db.batch() coalesces fsyncs; the 90 s outer wait_for wraps the whole
        # async-with so a hang in __aenter__ (cycle-2 prod failure mode) is
        # still cancelable. See reports/2026-05-22-bugs-cycle2.md §1.
        async def _batched_gather():
            async with self._db.batch():
                return await _gather_or_cancel(*[
                    self._run_engine(
                        engine, headline, signal_id, survivors,
                        classify_results, prompt_meta_json,
                        pre_skips, pre_ms, relevance_skips=relevance_skips,
                        freshness_label=freshness.label,
                    ) for engine in active_engines], label="run_engine")

        results: list[list[TradeDecision]] = []
        try:
            results = await asyncio.wait_for(_batched_gather(), timeout=90.0)
        except asyncio.TimeoutError:
            logger.error(
                "[FAN_OUT] [hid=%s] gather TIMEOUT 90s — batch cancelled, lock "
                "released via __aexit__. %d engines were running.",
                hid, len(active_engines),
            )
            results = []

        for engine, eng_results in zip(active_engines, results):
            if isinstance(eng_results, BaseException):
                logger.warning("[FAN_OUT] engine '%s' error: %s",
                               engine.name, eng_results)
                continue
            executed = [d for d in eng_results if d.executed]
            if executed:
                logger.info(
                    "[FAN_OUT] engine '%s' executed %d/%d trades  balance=%.2f",
                    engine.name, len(executed), len(eng_results),
                    engine.trader.balance,
                )

    async def _assess_freshness(self, headline: RawHeadline, embedding, hid: str):
        from news_memory import FreshnessResult  # local — avoid import cycle
        if self._news_memory is None:
            return FreshnessResult(label="BREAKING")
        try:
            freshness = await asyncio.get_event_loop().run_in_executor(
                None, self._news_memory.assess, headline.headline,
                embedding, headline.site_url, headline.article_url,
            )
            logger.info(
                "[INGRESS] [hid=%s] freshness=%s dist=%.3f age=%ss",
                hid, freshness.label, freshness.nearest_dist or 0.0,
                int(freshness.nearest_age_secs or 0),
            )
            return freshness
        except Exception as exc:
            logger.debug("[INGRESS] news_memory.assess error: %s", exc)
            return FreshnessResult(label="BREAKING")

    def _sim_gate_split(
        self, survivors: list[MarketMatch], hid: str,
    ) -> tuple[list[MarketMatch], list, list[_SimVerdict]]:
        """Per-match deterministic SIM_PASS / SIM_FAIL bucketing."""
        if not survivors:
            return [], [], []
        sim_floor = float(self._min_top_similarity)
        kept: list[MarketMatch] = []
        kept_v: list[_SimVerdict] = []
        skipped: list = []
        for m in survivors:
            sim = m.similarity or 0.0
            if sim >= sim_floor:
                v = _SimVerdict("SIM_PASS", f"sim={sim:.3f}")
                kept.append(m); kept_v.append(v)
            else:
                v = _SimVerdict("SIM_FAIL", f"sim={sim:.3f}<{sim_floor:.2f}")
                skipped.append((m, v))
            m._relevance_score, m._relevance_reason = v.score, v.reason
        logger.info("[SIM_GATE] [hid=%s] kept %d/%d (floor=%.2f)",
                    hid, len(kept), len(kept) + len(skipped), sim_floor)
        return kept, skipped, kept_v

    async def _fetch_market_context(
        self, survivors: list[MarketMatch], hid: str,
    ) -> None:
        """Parallel CLOB pre-headline snapshot (only kept markets pay)."""
        cfg = self._mkt_ctx_cfg
        if not survivors or (cfg is not None and not cfg.enabled):
            return
        try:
            from market_context import fetch_context_for_matches
            meta = await fetch_context_for_matches(
                self._get_price_session(), survivors,
                window_secs=(cfg.window_secs if cfg else 1800),
                cache=self._mkt_ctx_cache,
            )
            logger.info("[MARKET_CONTEXT] [hid=%s] %d/%d fetched in %dms",
                        hid, meta["n_fetched"], meta["n_in"],
                        int(meta["elapsed_ms"]))
        except Exception as exc:  # noqa: BLE001
            logger.warning("[MARKET_CONTEXT] error: %s — proceeding without", exc)

    async def _predict(
        self, headline: RawHeadline, survivors: list[MarketMatch],
        freshness, source_host: Optional[str],
        relevance_verdicts_kept: list[_SimVerdict], hid: str,
    ) -> tuple[list[ClassifyResult], dict]:
        """LLM ensemble (or SEULGI_BYPASS_LLM synthetic) + token logging."""
        if not survivors:
            return [], {}
        results, meta = await self._classifier.classify_batch(
            headline=headline.headline, matches=survivors,
            is_republish=freshness.is_republish, similar_to=freshness.similar_to,
            source=source_host, freshness=freshness,
            relevance_verdicts=relevance_verdicts_kept or None,
            # A2.1 (2026-05-22): correlate `llm_calls` rows with the
            # headline that triggered them. Additive kwarg — older test
            # paths that don't set this still work (headline_id stays NULL).
            headline_id=hid,
        )
        # ONE usage_events row per headline; A1.6 bypass logs count=0 so cost
        # dashboards show $0 burn explicitly (not a silent gap).
        tokens = sum(r.tokens_used for r in results)
        bypass = bool(meta.get("bypass"))
        if tokens or bypass:
            await self._db.log_usage(
                kind="ai_tokens", count=float(tokens),
                provider=meta.get("model"),
                meta=json.dumps({"hid": hid, "n_markets": len(survivors),
                                 "stage": "prediction",
                                 **({"bypass": True} if bypass else {})}),
            )
        return results, meta

    @staticmethod
    def _assign_mid(m: MarketMatch, side: str, price: Optional[float]) -> None:
        """Set mid_yes/mid_no from one fetched side via binary symmetry."""
        other = (1.0 - price) if price is not None else None
        if side == "yes":
            m.mid_yes, m.mid_no = price, other
        else:
            m.mid_no, m.mid_yes = price, other

    async def _prefetch_prices(
        self, matches: list[MarketMatch], hid: str
    ) -> list[MarketMatch]:
        """Fetch ONE side per match in parallel (preferring YES). Derive the
        other via binary-market symmetry: mid_no ≈ 1 − mid_yes. Halves CLOB
        calls; stays within ~1¢ of true mid. 10s TTL cache cuts burst traffic.
        """
        session = self._get_price_session()
        now_mono = time.monotonic()
        # Evict expired cache entries
        for k in [k for k, (_, exp) in self._price_cache.items() if exp <= now_mono]:
            self._price_cache.pop(k, None)

        tasks: list = []
        index_map: list[tuple[int, str]] = []  # (match_idx, side_fetched)
        for i, m in enumerate(matches):
            side = "yes" if m.token_yes else ("no" if m.token_no else None)
            if side is None:
                continue
            token = m.token_yes if side == "yes" else m.token_no
            cached = self._price_cache.get(token)
            if cached is not None:
                self._assign_mid(m, side, cached[0])
                continue
            tasks.append(_fetch_mid_price(session, token))
            index_map.append((i, side))

        if tasks:
            raw = await asyncio.gather(*tasks, return_exceptions=True)
            for r in raw:
                if isinstance(r, asyncio.CancelledError):
                    raise r
            exp_at = time.monotonic() + self._price_cache_ttl
            for (i, side), value in zip(index_map, raw):
                price = value if isinstance(value, float) else None
                m = matches[i]
                token = m.token_yes if side == "yes" else m.token_no
                if token is not None:
                    self._price_cache[token] = (price, exp_at)
                self._assign_mid(m, side, price)

        kept = [m for m in matches if m.mid_yes is not None or m.mid_no is not None]
        if len(kept) != len(matches):
            logger.info("[INGRESS] [hid=%s] dropped %d/%d no-price matches",
                        hid, len(matches) - len(kept), len(matches))
        # A4A.3 (2026-05-22): BBO + depth capture. Fan out per-token
        # /book fetches IN PARALLEL with a strict overall budget
        # (`_BBO_BUDGET_S`). On timeout / failure we fall through to
        # mid-only snaps so the ingress path is never blocked on CLOB.
        # The per-token cache inside `fetch_bbo` collapses bursts
        # across `_prefetch_prices` and `_position_snap_loop`.
        bbo_by_token: dict[str, "impact_tracker.BBOResult"] = {}
        if kept:
            from impact_tracker import fetch_bbo as _fb
            unique_tokens: list[str] = []
            seen: set[str] = set()
            for m in kept:
                tok = m.token_yes if m.mid_yes is not None else m.token_no
                if tok and tok not in seen:
                    seen.add(tok)
                    unique_tokens.append(tok)
            if unique_tokens:
                try:
                    bbo_results = await asyncio.wait_for(
                        asyncio.gather(
                            *(_fb(session, t) for t in unique_tokens),
                            return_exceptions=True,
                        ),
                        timeout=self._bbo_budget_s,
                    )
                    for tok, br in zip(unique_tokens, bbo_results):
                        if isinstance(br, BaseException):
                            continue
                        bbo_by_token[tok] = br
                except asyncio.TimeoutError:
                    logger.debug(
                        "[INGRESS] [hid=%s] bbo budget %.0fms exceeded — "
                        "mid-only snaps", hid, self._bbo_budget_s * 1000,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.debug("[INGRESS] [hid=%s] bbo gather failed: %r",
                                 hid, exc)

        # Persist mids so backtest has a price corpus for every matched
        # market (not only ones that traded). Awaited so writes join any
        # enclosing batch. A4A.3 fills bid/ask/book_depth_usdc when the
        # CLOB /book endpoint returned data; otherwise NULL.
        #
        # Phase 4D.1 (2026-05-23): in addition to the DB snap, we now
        # *also* populate MarketMatch.bid/ask/book_depth_usdc/spread_bps
        # so trader._evaluate_pure can run its spread+depth liquidity
        # filter without an extra DB round-trip. NULL values mean the
        # CLOB /book endpoint failed; the filter degrades to a NO-OP.
        if kept:
            t_unix = int(time.time())
            snaps: list[tuple] = []
            for m in kept:
                if m.mid_yes is not None:
                    side, tok, price = "yes", m.token_yes, float(m.mid_yes)
                else:
                    side, tok, price = "no", m.token_no, float(m.mid_no)
                br = bbo_by_token.get(tok or "")
                bid = br.bid if br is not None else None
                ask = br.ask if br is not None else None
                depth = br.book_depth_usdc_top5 if br is not None else None
                # Phase 4D.1: stamp BBO state onto the MarketMatch so the
                # trader's liquidity gate sees it. spread_bps is derived
                # from bid/ask + price (mid); when either side is NULL we
                # leave spread_bps=None so the gate no-ops.
                m.bid = bid
                m.ask = ask
                m.book_depth_usdc = depth
                if bid is not None and ask is not None and price > 0:
                    try:
                        m.spread_bps = (ask - bid) / price * 10_000.0
                    except (TypeError, ZeroDivisionError):
                        m.spread_bps = None
                snaps.append((m.condition_id, tok, side, price,
                              t_unix, "prefetch", hid, bid, ask, depth))
            try:
                await self._db.insert_market_price_snaps(snaps)
            except Exception as exc:  # noqa: BLE001
                logger.warning("[INGRESS] [hid=%s] price_snap write failed: %r",
                               hid, exc)
        return kept

    async def _run_engine(
        self, engine: Engine, headline: RawHeadline, signal_id: int,
        survivors: list[MarketMatch], classify_results: list[ClassifyResult],
        prompt_meta_json: Optional[str],
        pre_skips: list[tuple[MarketMatch, str]],
        pre_ms: float = 0.0, relevance_skips: Optional[list] = None,
        freshness_label: Optional[str] = None,
    ) -> list[TradeDecision]:
        """Run one engine. Catches `evaluate` crashes so one bad variant
        doesn't contaminate the shared aiosqlite connection."""
        hid = getattr(headline, "headline_id", "")
        _t_engine = time.monotonic()
        # Per-engine scoped pre-AI gate (rules with non-empty engines_exclude).
        eng_survivors, eng_classify, eng_skips = self._apply_scoped_rules(
            engine, survivors, classify_results, pre_skips, freshness_label,
        )
        # E8 cluster-size sizing: N = pre-AI gate survivors for this engine.
        # 2026-05-24: floored at 0.50 — SBERT cosine typically returns N=20-25
        # markets per headline, which uncapped shrunk every stake 5× silently.
        # The cap preserves the diversification intent for small clusters
        # (N=2-4 still scale 0.71→0.50) while preventing the typical cluster
        # from making research-tier ($1) and micro ($0.05) stakes unviable.
        n_cluster = max(len(eng_survivors), 1)
        cluster_factor = max(0.50, 1.0 / math.sqrt(n_cluster)) if n_cluster > 1 else 1.0
        try:
            decisions = await engine.trader.evaluate(
                signal_id, headline, eng_survivors, eng_classify,
                prompt_meta_json, pre_ai_skips=eng_skips,
                relevance_skips=relevance_skips, cluster_factor=cluster_factor,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning("[FAN_OUT] [hid=%s] engine '%s' evaluate crashed: %r",
                           hid, engine.name, exc, exc_info=True)
            decisions = []
        total_latency_ms = pre_ms + (time.monotonic() - _t_engine) * 1000.0
        try:
            await self._db.record_engine_decisions(
                engine.name, decisions, headline_id=hid,
                signal_latency_ms=total_latency_ms,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning("[FAN_OUT] [hid=%s] engine '%s' record failed: %r",
                           hid, engine.name, exc, exc_info=True)

        if self._impact_tracker is not None and engine == self._engines[0]:
            self._track_impacts(headline, decisions, survivors, hid)
        if self._dashboard:
            self._publish_dashboard(engine, headline, decisions, hid)
        return decisions

    def _apply_scoped_rules(
        self, engine: Engine, survivors: list[MarketMatch],
        classify_results: list[ClassifyResult],
        pre_skips: list[tuple[MarketMatch, str]],
        freshness_label: Optional[str],
    ) -> tuple[list[MarketMatch], list[ClassifyResult],
               list[tuple[MarketMatch, str]]]:
        """Apply rules with `engines_exclude` set, scoped to engine.parent_root."""
        eng_skips: list[tuple[MarketMatch, str]] = list(pre_skips or [])
        if self._rules is None:
            return list(survivors), list(classify_results), eng_skips
        kept_m: list[MarketMatch] = []
        kept_c: list[ClassifyResult] = []
        parent = (getattr(engine.trader, "_parent_engine", engine.name)
                  or engine.name)
        for m, cr in zip(survivors, classify_results):
            reason = self._rules.gate_pre_ai_scoped(
                market_type=m.market_type, mid_price=m.mid_yes,
                engine_root=parent, freshness_label=freshness_label,
                question=m.question)
            if reason is None:
                kept_m.append(m); kept_c.append(cr)
            else:
                eng_skips.append((m, reason))
        return kept_m, kept_c, eng_skips

    def _track_impacts(
        self, headline: RawHeadline, decisions: list[TradeDecision],
        survivors: list[MarketMatch], hid: str,
    ) -> None:
        """Fire one impact-tracker task per directional decision (BG-tagged)."""
        if getattr(headline, "_is_republish", False):
            return
        by_cid = {m.condition_id: m for m in survivors}
        for d in decisions:
            if d.direction.value not in ("YES", "NO"):
                continue
            m = by_cid.get(d.condition_id)
            if m is None:
                continue
            token_id = m.token_yes if d.direction.value == "YES" else m.token_no
            if not token_id:
                continue
            self._spawn_bg(
                self._impact_tracker.record(
                    headline_id=hid, headline=headline.headline,
                    site_url=headline.site_url, condition_id=d.condition_id,
                    question=d.question, token_id=token_id,
                    direction=d.direction.value, ai_confidence=d.confidence,
                    similarity=d.similarity, source=headline.source,
                ),
                name=f"impact:{hid}:{d.condition_id}",
            )

    def _publish_dashboard(
        self, engine: Engine, headline: RawHeadline,
        decisions: list[TradeDecision], hid: str,
    ) -> None:
        """Push per-decision rows to the dashboard's in-memory buffers."""
        d_name = engine.name
        for d in decisions:
            if d.direction.value != "NEUTRAL":
                if d.executed:
                    j_outcome = (f"TRADED ${d.size_usdc:.0f} @ {d.mid_price:.3f}  "
                                 f"edge={d.edge:+.4f}")
                else:
                    j_outcome = f"SKIP  {d.skip_reason or 'unknown'}"
                self._dashboard.record_signal_journal(
                    headline=headline.headline, market=d.question,
                    direction=d.direction.value, confidence=d.confidence,
                    similarity=d.similarity, engine_name=d_name,
                    outcome=j_outcome, headline_id=hid,
                )
            t_outcome = (f"TRADE ${d.size_usdc:.0f}" if d.executed
                         else f"SKIP {d.skip_reason or ''}")
            self._dashboard.record_trade(
                condition_id=d.condition_id, question=d.question,
                headline=headline.headline, site_url=headline.site_url,
                direction=d.direction.value, confidence=d.confidence,
                similarity=d.similarity, p_hat=d.p_hat,
                mid_price=d.mid_price, size_usdc=d.size_usdc,
                skip_reason=d.skip_reason, engine_name=d_name, headline_id=hid,
            )
            self._dashboard.record_trace(
                headline_id=hid, site=headline.site_url,
                headline=headline.headline, market=d.question,
                direction=d.direction.value, confidence=d.confidence,
                outcome=f"[{d_name}] {t_outcome}",
            )
        self._dashboard.set_balance(d_name, engine.trader.balance)

    async def monitor_all_positions(self) -> None:
        """Run position monitor for all engines concurrently."""
        await _gather_or_cancel(
            *[engine.trader.monitor_positions() for engine in self._engines],
            label="monitor_positions",
        )

    @property
    def engines(self) -> list[Engine]:
        return self._engines


def _make_engine(name: str, *, db, classifier, trading, ai, rule_engine,
                 parent: str, balance: Optional[float] = None,
                 cf_sampler=None) -> Engine:
    return Engine(
        name=name,
        trader=PaperTrader(
            db=db, classifier=classifier, cfg=trading, ai_cfg=ai,
            engine_name=name, rule_engine=rule_engine,
            variant_name=name, parent_engine=parent,
            cf_sampler=cf_sampler,
        ),
        fixed_balance=balance,
    )


def build_engines(cfg, db: SeulgiDB, classifier: DirectionClassifier,
                  rule_engine=None) -> list[Engine]:
    """Build engines + variants from cfg. Empty cfg.engines → one "default".
    AGGRESSIVE_CORE_ENABLE=1 appends the carved-out aggressive_core strategy.
    """
    # A4E.5 — shared CounterfactualSampler across every engine/variant in
    # this process. The per-headline cap is enforced inside the sampler
    # so a single noisy headline can't spawn 20 cf trades across 20
    # variants. Default OFF via `counterfactual.enabled: false` in
    # config.yaml.
    cf_sampler = None
    try:
        from counterfactual import (
            CounterfactualSampler, load_from_yaml as _cf_load,
        )
        cf_cfg = _cf_load(getattr(cfg, "counterfactual", None))
        if cf_cfg.enabled:
            cf_sampler = CounterfactualSampler(cf_cfg)
            logger.info(
                "[FAN_OUT] counterfactual sampler enabled "
                "(rate=%.3f, cap=%d, filters=%s)",
                cf_cfg.sample_rate_per_filter, cf_cfg.max_cf_per_headline,
                sorted(cf_sampler.enabled_filters),
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "[FAN_OUT] counterfactual sampler init failed: %r — cf-sampling OFF",
            exc,
        )

    engine_cfgs = getattr(cfg, "engines", [])
    if not engine_cfgs:
        return [_make_engine("default", db=db, classifier=classifier,
                             trading=cfg.trading, ai=cfg.ai,
                             rule_engine=rule_engine, parent="default",
                             cf_sampler=cf_sampler)]

    engines: list[Engine] = []
    for ec in engine_cfgs:
        base_trading = _merge_trading_cfg(cfg.trading, ec)
        base_ai = _merge_ai_cfg(cfg.ai, ec)
        for variant in (getattr(ec, "variants", None) or [None]):
            if variant is None:
                v_trading, v_ai, v_name = base_trading, base_ai, ec.name
            else:
                v_trading = _merge_trading_cfg(base_trading, variant)
                v_ai = _merge_ai_cfg(base_ai, variant)
                v_name = f"{ec.name}/{variant.name}"
            engines.append(_make_engine(
                v_name, db=db, classifier=classifier, trading=v_trading,
                ai=v_ai, rule_engine=rule_engine, parent=ec.name,
                balance=ec.balance_usdc, cf_sampler=cf_sampler,
            ))

    # Carved-out strategy module — opt-in via AGGRESSIVE_CORE_ENABLE=1.
    try:
        try:
            from strategies.aggressive_core.factory import maybe_append    # container
        except ImportError:
            from app.strategies.aggressive_core.factory import maybe_append  # type: ignore[no-redef]
        if maybe_append(engines, db=db)[0] is not None:
            logger.info("[FAN_OUT] aggressive_core engine appended")
    except Exception as exc:  # noqa: BLE001
        logger.warning("[FAN_OUT] aggressive_core registration failed: %s", exc)

    logger.info("[FAN_OUT] built %d engines: %s",
                len(engines), [e.name for e in engines])
    return engines


# Fields whose `None` override means 'inherit base'. Everything else
# (max_unique_markets, cold_position_hours, infinite_balance, mode) is a
# global-only flag and propagates from base unchanged.
_OVERRIDABLE_TRADING_FIELDS = (
    "balance_usdc", "max_position_usdc", "min_trade_usdc", "min_edge",
    "edge_increment", "position_timeout_hours", "max_open_positions",
    "take_profit", "stop_loss", "trailing_stop", "min_hold_seconds",
    "rail_skip_threshold", "entry_rail_skip_threshold",
    "invert_high_conf", "invert_conf_threshold", "spread_cost_bps",
    "stop_loss_pct", "min_entry_yes_mid", "max_market_vol_30m",
    "direction_filter", "freshness_filter", "mid_yes_min", "mid_yes_max",
    # A4B.2 — true-EV formula toggle + per-engine EV floor.
    "use_true_ev_formula", "min_ev_per_dollar",
    # A4D.3 — position-scaling toggle + per-condition exposure cap.
    "position_scaling_enabled", "max_exposure_per_condition_usdc",
    # A4D.1 (2026-05-23) — dynamic liquidity filter replacing hour_gate.
    "max_spread_bps", "min_book_depth_usdc",
)


def _merge_trading_cfg(base: TradingConfig, override) -> TradingConfig:
    """Apply per-engine overrides on top of base. None ⇒ inherit base.

    Also handles A4B.3 `kelly_cap_override`: when set on the override, the
    base's `simulate_sizing` is replaced with a copy whose
    `max_fractional_kelly_cap` reflects the engine-local cap.
    """
    from dataclasses import replace
    updates = {a: v for a in _OVERRIDABLE_TRADING_FIELDS
               if (v := getattr(override, a, None)) is not None}
    kelly_cap = getattr(override, "kelly_cap_override", None)
    if kelly_cap is not None and base.simulate_sizing is not None:
        updates["simulate_sizing"] = replace(
            base.simulate_sizing, max_fractional_kelly_cap=float(kelly_cap),
        )
    return replace(base, **updates)


def _merge_ai_cfg(base: AIConfig, override) -> AIConfig:
    """Copy base AIConfig with per-engine min_confidence override applied."""
    from dataclasses import replace
    min_conf = getattr(override, "min_confidence", None)
    return replace(base, min_confidence=min_conf) if min_conf is not None else base

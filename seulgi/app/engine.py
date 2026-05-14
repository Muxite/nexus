"""
Seulgi engine registry — multi-engine paper trading framework.

Multiple engines can run concurrently on the same headline stream and market
data, each with a different rule set. Their results are recorded independently
so you can compare strategies side-by-side.

Each engine is just a PaperTrader with a named config profile. The Engine
dataclass bundles the name, config, and trader together. The EngineRunner
fans out each incoming headline to all registered engines in parallel.

Result comparison lives in the `engine_results` DB table (one row per
engine×trade decision) and is written to data/dashboard/engine_comparison.txt
by the dashboard.

Defining engines
─────────────────
Add entries under the `engines:` key in config.yaml:

    engines:
      - name: "aggressive"
        min_edge: 0.03
        edge_increment: 0.15
        max_position_usdc: 150.0
        min_confidence: 0.40

      - name: "conservative"
        min_edge: 0.08
        edge_increment: 0.10
        max_position_usdc: 50.0
        min_confidence: 0.60

Any unspecified field falls back to the global `trading:` defaults.

If no `engines:` section exists, a single default engine named "default" is
created from the global `trading:` config.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

import aiohttp

from ai import DirectionClassifier
from config import TradingConfig, AIConfig
from db import SeulgiDB
from markets import MarketIndex, Embedder, MarketMatch
from trader import PaperTrader, TradeDecision, _fetch_mid_price
from watcher import RawHeadline

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Engine dataclass
# ---------------------------------------------------------------------------


@dataclass
class Engine:
    """
    One trading engine — a named PaperTrader with its own rule set.

    Multiple engines share the same classifier, market index, and DB
    connection but maintain separate balances and position tables
    (via the `engine_name` column).
    """
    name: str
    trader: PaperTrader


# ---------------------------------------------------------------------------
# EngineRunner
# ---------------------------------------------------------------------------


class EngineRunner:
    """
    Fans out each headline to all registered engines concurrently.

    Usage:
        runner = EngineRunner(engines, index, embedder, db, dashboard)
        await runner.process(headline)
    """

    def __init__(
        self,
        engines: list[Engine],
        index: MarketIndex,
        embedder: Embedder,
        db: SeulgiDB,
        dashboard=None,       # Optional[Dashboard] — avoids circular import
        news_memory=None,     # Optional[NewsMemory] — avoids circular import
        impact_tracker=None,  # Optional[PriceImpactTracker] — avoids circular import
        article_fetcher=None, # Optional[ArticleFetcher] — fire-and-forget body capture
    ) -> None:
        self._engines = engines
        self._index = index
        self._embedder = embedder
        self._db = db
        self._dashboard = dashboard
        self._news_memory = news_memory
        self._impact_tracker = impact_tracker
        self._article_fetcher = article_fetcher
        # Single shared aiohttp session for parallel price prefetches
        self._price_session: Optional[aiohttp.ClientSession] = None
        # Mid-price TTL cache shared across headlines — cuts CLOB API traffic
        # dramatically when a burst of headlines hits the same markets.
        # token_id → (price_or_None, monotonic_expiry_secs)
        self._price_cache: dict[str, tuple[Optional[float], float]] = {}
        self._price_cache_ttl: float = 10.0   # seconds

    def _get_price_session(self) -> aiohttp.ClientSession:
        if self._price_session is None or self._price_session.closed:
            self._price_session = aiohttp.ClientSession()
        return self._price_session

    async def close(self) -> None:
        if self._price_session and not self._price_session.closed:
            await self._price_session.close()

    async def process(self, headline: RawHeadline) -> None:
        """
        Embed headline, check for republish, find matches, fan out to engines.
        """
        _t0 = time.monotonic()
        hid = getattr(headline, "headline_id", "")
        # Fire-and-forget article body capture — runs in parallel with the rest
        # of the pipeline; never blocks the trade decision. Data collection
        # only (lands in headline_articles for later analysis / re-classification).
        if self._article_fetcher is not None and getattr(headline, "article_url", None):
            asyncio.create_task(
                self._article_fetcher.fetch_and_store(
                    headline_id=hid,
                    article_url=headline.article_url,
                    site_url=headline.site_url,
                ),
                name=f"article_fetch:{hid}",
            )
        loop = asyncio.get_event_loop()
        try:
            embedding = await loop.run_in_executor(
                None, self._embedder.embed, headline.headline
            )
        except Exception as exc:
            logger.warning("[runner] [hid=%s] embed error: %s", hid, exc)
            return

        # Dedup check — runs in executor (ChromaDB is synchronous)
        is_republish = False
        similar_to: Optional[str] = None
        if self._news_memory is not None:
            try:
                is_republish, similar_to = await loop.run_in_executor(
                    None,
                    self._news_memory.check,
                    headline.headline,
                    embedding,
                    headline.site_url,
                    headline.article_url,
                )
                if is_republish:
                    logger.info(
                        "[runner] [hid=%s] republish detected: %s",
                        hid, headline.headline[:80],
                    )
            except Exception as exc:
                logger.debug("[runner] news_memory error: %s", exc)

        # Store republish context on headline for downstream use
        headline._is_republish = is_republish
        headline._similar_to = similar_to

        matches = self._index.find_matches(embedding)
        if not matches:
            logger.debug("[runner] [hid=%s] no matches: %s", hid, headline.headline[:80])
            return

        # Bump heat scores for matched markets (fire-and-forget)
        asyncio.create_task(self._db.bump_market_heat([m.condition_id for m in matches]))

        # ── Parallel mid-price prefetch ─────────────────────────────────────
        # Fetch every match's YES and NO mid price in ONE gather. Drops matches
        # where neither token has a price (almost always a closed market) BEFORE
        # the AI sees them, saving tokens and producing meaningful AI input.
        matches = await self._prefetch_prices(matches, hid)
        if not matches:
            logger.info("[runner] [hid=%s] all matches dropped (no_price) — likely stale tokens",
                        hid)
            return

        source = headline.source
        logger.info(
            "[runner] [hid=%s] '%s' matched %d markets (republish=%s) — dispatching to %d engines",
            hid, headline.headline[:60], len(matches), is_republish, len(self._engines),
        )

        # Embed + dedup + match time — passed in; AI call time is measured per-engine
        pre_ms = (time.monotonic() - _t0) * 1000.0

        # Fan out to all engines concurrently
        results: list[list[TradeDecision]] = await asyncio.gather(
            *[
                self._run_engine(engine, headline, matches, pre_ms)
                for engine in self._engines
            ],
            return_exceptions=True,
        )

        for engine, engine_results in zip(self._engines, results):
            if isinstance(engine_results, BaseException):
                logger.warning("[runner] engine '%s' error: %s", engine.name, engine_results)
                continue
            executed = [d for d in engine_results if d.executed]
            if executed:
                logger.info(
                    "[runner] engine '%s' executed %d/%d trades  balance=%.2f",
                    engine.name, len(executed), len(engine_results),
                    engine.trader.balance,
                )

    async def _prefetch_prices(
        self, matches: list[MarketMatch], hid: str
    ) -> list[MarketMatch]:
        """
        Fetch ONE side per match in parallel (preferring YES). Derive the other
        side via binary-market symmetry: mid_no ≈ 1 − mid_yes. This halves
        CLOB API calls vs fetching both sides while staying within ~1¢ of true
        mid (well below typical spread + our trading thresholds).

        Mutates each MarketMatch with the prices. Returns the subset where at
        least one side has a price (drops fully-stale markets).

        A 10-second TTL cache further reduces traffic during burst events
        (multiple headlines hitting the same hot market within seconds).
        """
        session = self._get_price_session()
        now_mono = time.monotonic()
        # Evict expired cache entries
        for k in [k for k, (_, exp) in self._price_cache.items() if exp <= now_mono]:
            self._price_cache.pop(k, None)

        tasks: list = []
        index_map: list[tuple[int, str]] = []  # (match_idx, side_fetched: "yes"|"no")
        for i, m in enumerate(matches):
            # Prefer YES (one canonical side per market). Fall back to NO if
            # this market has no YES token.
            side = "yes" if m.token_yes else ("no" if m.token_no else None)
            if side is None:
                continue
            token = m.token_yes if side == "yes" else m.token_no
            cached = self._price_cache.get(token)
            if cached is not None:
                price = cached[0]
                if side == "yes":
                    m.mid_yes = price
                    m.mid_no = (1.0 - price) if price is not None else None
                else:
                    m.mid_no = price
                    m.mid_yes = (1.0 - price) if price is not None else None
                continue
            tasks.append(_fetch_mid_price(session, token))
            index_map.append((i, side))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            exp_at = time.monotonic() + self._price_cache_ttl
            for (i, side), value in zip(index_map, results):
                price = value if isinstance(value, float) else None
                m = matches[i]
                token = m.token_yes if side == "yes" else m.token_no
                if token is not None:
                    self._price_cache[token] = (price, exp_at)
                if side == "yes":
                    m.mid_yes = price
                    m.mid_no = (1.0 - price) if price is not None else None
                else:
                    m.mid_no = price
                    m.mid_yes = (1.0 - price) if price is not None else None

        kept = [m for m in matches if m.mid_yes is not None or m.mid_no is not None]
        if len(kept) != len(matches):
            logger.info("[runner] [hid=%s] dropped %d/%d no-price matches",
                        hid, len(matches) - len(kept), len(matches))
        return kept

    async def _run_engine(
        self,
        engine: Engine,
        headline: RawHeadline,
        matches: list[MarketMatch],
        pre_ms: float = 0.0,
    ) -> list[TradeDecision]:
        """
        Run one engine on the matches. Total signal latency = pre_ms (embed +
        dedup + similarity) + the AI classification call inside trader.process().
        This captures end-to-end headline-to-decision time.
        """
        hid = getattr(headline, "headline_id", "")
        _t_engine = time.monotonic()
        decisions = await engine.trader.process(headline, matches)
        total_latency_ms = pre_ms + (time.monotonic() - _t_engine) * 1000.0
        await self._db.record_engine_decisions(
            engine.name, decisions, headline_id=hid, signal_latency_ms=total_latency_ms
        )

        # Log AI tokens consumed by this engine's batch call for this headline.
        # PaperTrader.process stashes the batch's total on _last_tokens_used.
        tokens_used = getattr(engine.trader, "_last_tokens_used", 0) or 0
        if tokens_used:
            asyncio.create_task(self._db.log_usage(
                kind="ai_tokens", count=float(tokens_used),
                provider=getattr(engine.trader._ai_cfg, "model", None),
                meta=f'{{"engine":"{engine.name}","hid":"{hid}"}}',
            ))

        # Price-impact tracking — only from first engine to avoid duplicate rows
        if self._impact_tracker is not None and engine == self._engines[0]:
            is_republish = getattr(headline, "_is_republish", False)
            source = headline.source
            if not is_republish:
                for d in decisions:
                    # Pick the token for the AI's predicted direction
                    match_for_d = next(
                        (m for m in matches if m.condition_id == d.condition_id), None
                    )
                    token_id = None
                    if match_for_d:
                        token_id = (
                            match_for_d.token_yes if d.direction.value == "YES"
                            else match_for_d.token_no
                        )
                    if token_id and d.direction.value in ("YES", "NO"):
                        asyncio.create_task(self._impact_tracker.record(
                            headline_id=hid,
                            headline=headline.headline,
                            site_url=headline.site_url,
                            condition_id=d.condition_id,
                            question=d.question,
                            token_id=token_id,
                            direction=d.direction.value,
                            ai_confidence=d.confidence,
                            similarity=d.similarity,
                            source=source,
                        ))

        if self._dashboard:
            for d in decisions:
                if d.direction.value != "NEUTRAL":
                    if d.executed:
                        outcome = f"TRADED ${d.size_usdc:.0f} @ {d.mid_price:.3f}  edge={d.edge:+.4f}"
                    else:
                        outcome = f"SKIP  {d.skip_reason or 'unknown'}"
                    self._dashboard.record_signal_journal(
                        headline=headline.headline,
                        market=d.question,
                        direction=d.direction.value,
                        confidence=d.confidence,
                        similarity=d.similarity,
                        engine_name=engine.name,
                        outcome=outcome,
                        headline_id=hid,
                    )

            for d in decisions:
                outcome = (
                    f"TRADE ${d.size_usdc:.0f}" if d.executed
                    else f"SKIP {d.skip_reason or ''}"
                )
                self._dashboard.record_trade(
                    condition_id=d.condition_id,
                    question=d.question,
                    headline=headline.headline,
                    site_url=headline.site_url,
                    direction=d.direction.value,
                    confidence=d.confidence,
                    similarity=d.similarity,
                    p_hat=d.p_hat,
                    mid_price=d.mid_price,
                    size_usdc=d.size_usdc,
                    skip_reason=d.skip_reason,
                    engine_name=engine.name,
                    headline_id=hid,
                )
                self._dashboard.record_trace(
                    headline_id=hid,
                    site=headline.site_url,
                    headline=headline.headline,
                    market=d.question,
                    direction=d.direction.value,
                    confidence=d.confidence,
                    outcome=f"[{engine.name}] {outcome}",
                )
            self._dashboard.set_balance(
                engine.name, engine.trader.balance
            )

        return decisions

    async def monitor_all_positions(self) -> None:
        """Run position monitor for all engines concurrently."""
        await asyncio.gather(
            *[engine.trader.monitor_positions() for engine in self._engines],
            return_exceptions=True,
        )

    @property
    def engines(self) -> list[Engine]:
        return self._engines


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def build_engines(
    cfg,                       # Config
    db: SeulgiDB,
    classifier: DirectionClassifier,
    rule_engine=None,          # Optional[RuleEngine] — shared across engines
) -> list[Engine]:
    """
    Build the list of engines from config.

    If cfg.engines is populated, one engine per entry is created, each
    overriding the global trading defaults. Otherwise a single "default"
    engine is created from cfg.trading.

    All engines share the same `rule_engine` — rules are user-curated trade
    gates that apply uniformly. Per-engine rule overrides can be added later
    if needed.
    """
    engine_cfgs = getattr(cfg, "engines", [])

    if not engine_cfgs:
        return [Engine(
            name="default",
            trader=PaperTrader(
                db=db, classifier=classifier, cfg=cfg.trading, ai_cfg=cfg.ai,
                engine_name="default", rule_engine=rule_engine,
            ),
        )]

    engines = []
    for ec in engine_cfgs:
        trading_cfg = _merge_trading_cfg(cfg.trading, ec)
        ai_cfg = _merge_ai_cfg(cfg.ai, ec)
        engines.append(Engine(
            name=ec.name,
            trader=PaperTrader(
                db=db, classifier=classifier, cfg=trading_cfg, ai_cfg=ai_cfg,
                engine_name=ec.name, rule_engine=rule_engine,
            ),
        ))

    logger.info("[engine] built %d engines: %s", len(engines), [e.name for e in engines])
    return engines


def _merge_trading_cfg(base: TradingConfig, override) -> TradingConfig:
    """Apply per-engine overrides on top of the global trading config.
    None values in override mean 'use the base value'.
    """
    def _pick(attr, default):
        v = getattr(override, attr, None)
        return v if v is not None else default

    return TradingConfig(
        balance_usdc=_pick("balance_usdc", base.balance_usdc),
        max_position_usdc=_pick("max_position_usdc", base.max_position_usdc),
        min_trade_usdc=_pick("min_trade_usdc", base.min_trade_usdc),
        min_edge=_pick("min_edge", base.min_edge),
        edge_increment=_pick("edge_increment", base.edge_increment),
        position_timeout_hours=_pick("position_timeout_hours", base.position_timeout_hours),
        max_open_positions=_pick("max_open_positions", base.max_open_positions),
        max_unique_markets=base.max_unique_markets,
        cold_position_hours=base.cold_position_hours,
        take_profit=_pick("take_profit", base.take_profit),
        stop_loss=_pick("stop_loss", base.stop_loss),
        trailing_stop=_pick("trailing_stop", base.trailing_stop),
        min_hold_seconds=_pick("min_hold_seconds", base.min_hold_seconds),
        infinite_balance=base.infinite_balance,   # propagate global flag to every engine
        mode=base.mode,
        rail_skip_threshold=_pick("rail_skip_threshold", base.rail_skip_threshold),
        entry_rail_skip_threshold=_pick("entry_rail_skip_threshold", base.entry_rail_skip_threshold),
        invert_high_conf=_pick("invert_high_conf", base.invert_high_conf),
        invert_conf_threshold=_pick("invert_conf_threshold", base.invert_conf_threshold),
    )


def _merge_ai_cfg(base: AIConfig, override) -> AIConfig:
    """Return a copy of base AIConfig with per-engine min_confidence override applied."""
    from dataclasses import replace
    min_conf = getattr(override, "min_confidence", None)
    if min_conf is not None:
        return replace(base, min_confidence=min_conf)
    return base

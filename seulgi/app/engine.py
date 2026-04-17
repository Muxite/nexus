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
from dataclasses import dataclass, field
from typing import Optional

from ai import DirectionClassifier
from config import TradingConfig
from db import SeulgiDB
from markets import MarketIndex, Embedder, MarketMatch
from trader import PaperTrader, TradeDecision
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
        dashboard=None,     # Optional[Dashboard] — avoids circular import
    ) -> None:
        self._engines = engines
        self._index = index
        self._embedder = embedder
        self._db = db
        self._dashboard = dashboard

    async def process(self, headline: RawHeadline) -> None:
        """
        Embed headline, find matches, then fan out to all engines in parallel.
        Each engine evaluates the same matches with its own rules.
        """
        loop = asyncio.get_event_loop()
        try:
            embedding = await loop.run_in_executor(
                None, self._embedder.embed, headline.headline
            )
        except Exception as exc:
            logger.warning("[runner] embed error: %s", exc)
            return

        matches = self._index.find_matches(embedding)
        if not matches:
            logger.debug("[runner] no matches: %s", headline.headline[:80])
            return

        logger.info(
            "[runner] '%s' matched %d markets — dispatching to %d engines",
            headline.headline[:60], len(matches), len(self._engines),
        )

        # Fan out to all engines concurrently
        results: list[list[TradeDecision]] = await asyncio.gather(
            *[
                self._run_engine(engine, headline, matches)
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

    async def _run_engine(
        self,
        engine: Engine,
        headline: RawHeadline,
        matches: list[MarketMatch],
    ) -> list[TradeDecision]:
        decisions = await engine.trader.process(headline, matches)
        await self._db.record_engine_decisions(engine.name, decisions)

        if self._dashboard:
            for d in decisions:
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
) -> list[Engine]:
    """
    Build the list of engines from config.

    If cfg.engines is populated, one engine per entry is created, each
    overriding the global trading defaults. Otherwise a single "default"
    engine is created from cfg.trading.
    """
    engine_cfgs = getattr(cfg, "engines", [])

    if not engine_cfgs:
        return [Engine(
            name="default",
            trader=PaperTrader(db=db, classifier=classifier, cfg=cfg.trading, ai_cfg=cfg.ai),
        )]

    engines = []
    for ec in engine_cfgs:
        trading_cfg = _merge_trading_cfg(cfg.trading, ec)
        engines.append(Engine(
            name=ec.name,
            trader=PaperTrader(db=db, classifier=classifier, cfg=trading_cfg, ai_cfg=cfg.ai),
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
        mode=base.mode,
    )

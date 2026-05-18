"""Construct a fully-wired aggressive_core Engine ready for EngineRunner.

The runner consumes objects with `.name` and `.trader` (where `.trader`
has `.evaluate`, `.monitor_positions`, `.balance`, `._engine_name`).
CoreStrategy fits that shape, so this factory just builds the chain:

    CoreConfig → EventBus → PaperTrader|LiveTrader → CoreStrategy → Engine
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

try:                       # container: /app on sys.path, flat imports
    from db import SeulgiDB
    from engine import Engine
except ImportError:        # local dev / pytest: app/ as package
    from app.db import SeulgiDB      # type: ignore[no-redef]
    from app.engine import Engine    # type: ignore[no-redef]

from .config import CoreConfig
from .live_trader import LiveTrader
from .observability import EventBus
from .paper_trader import PaperTrader
from .strategy import CoreStrategy
from .trader_base import Trader
from .variants import load_active_variants

logger = logging.getLogger(__name__)


def _build_trader(*, cfg: CoreConfig, db: SeulgiDB, bus: EventBus) -> Trader:
    if cfg.is_live:
        return LiveTrader(bus=bus, cfg=cfg)
    return PaperTrader(cfg=cfg, db=db, bus=bus)


def build_aggressive_core(
    *,
    db: SeulgiDB,
    cfg: Optional[CoreConfig] = None,
    events_dir: Optional[Path] = None,
) -> tuple[Engine, CoreStrategy, EventBus]:
    """Return (Engine adapter for EngineRunner, the strategy itself, the bus).

    The bus is exposed so the runner can keep a reference for TUI/health
    queries. The strategy is exposed so callers can do
    strategy.balance / strategy.monitor_positions(...) from menu CLIs.
    """
    cfg = cfg or CoreConfig(mode=("live" if os.getenv("SEULGI_LIVE") == "1" else "paper"))
    ndjson_path = (events_dir or Path("/data/dashboard")) / "events.ndjson"
    bus = EventBus(ndjson_path=ndjson_path)
    trader = _build_trader(cfg=cfg, db=db, bus=bus)
    strategy = CoreStrategy(cfg=cfg, db=db, trader=trader, bus=bus)
    bus.emit(
        engine_name=cfg.engine_name, phase="boot", name="built",
        payload={"mode": cfg.mode, "trader": trader.name, **cfg.derived()},
    )
    return Engine(name=cfg.engine_name, trader=strategy), strategy, bus


def maybe_append(engines: list, *, db: SeulgiDB,
                 events_dir: Optional[Path] = None) -> tuple[Optional[CoreStrategy], Optional[EventBus]]:
    """Helper for build_engines() in app/engine.py — appends the carved
    engine to `engines` if AGGRESSIVE_CORE_ENABLE=1 in the environment.

    Returns (strategy, bus) for the caller to retain; (None, None) if
    not enabled. Safe to call multiple times — duplicate names are
    filtered by EngineRunner."""
    if os.getenv("AGGRESSIVE_CORE_ENABLE") != "1":
        return None, None
    engine, strategy, bus = build_aggressive_core(db=db, events_dir=events_dir)
    if any(e.name == engine.name for e in engines):
        logger.info("[aggressive_core] already registered; skipping duplicate")
        return strategy, bus
    engines.append(engine)
    logger.info("[aggressive_core] registered engine (mode=%s)", strategy._cfg.mode)
    return strategy, bus


def build_all_variants(
    *,
    db: SeulgiDB,
    events_dir: Optional[Path] = None,
) -> tuple[list, list[CoreStrategy], EventBus]:
    """Build one (Engine, CoreStrategy) per VARIANT_CONFIGS entry — every
    surviving legacy variant runs through the new friction-aware pipeline.

    All variants share a single EventBus so the dashboard sees one
    unified event stream. Each variant gets its own PaperTrader instance
    (= its own $50 bankroll, its own cash bookkeeping).

    Returns (engines, strategies, bus). Strategies list lets the menu CLI
    inspect per-variant balance without going through EngineRunner.
    """
    ndjson_path = (events_dir or Path("/data/dashboard")) / "events.ndjson"
    bus = EventBus(ndjson_path=ndjson_path)
    engines: list = []
    strategies: list[CoreStrategy] = []
    selected = load_active_variants()
    for cfg in selected:
        # Honour SEULGI_LIVE at boot — but in practice multi-variant is
        # paper-only; flipping a 17-variant fan-out to live is not the
        # intended use of LiveTrader.
        cfg_live = replace_mode(cfg)
        trader = _build_trader(cfg=cfg_live, db=db, bus=bus)
        strategy = CoreStrategy(cfg=cfg_live, db=db, trader=trader, bus=bus)
        engines.append(Engine(name=cfg_live.engine_name, trader=strategy))
        strategies.append(strategy)
        bus.emit(
            engine_name=cfg_live.engine_name, phase="boot", name="built",
            payload={"mode": cfg_live.mode, "trader": trader.name,
                     **cfg_live.derived()},
        )
    logger.info("[aggressive_core] built %d variants via CoreStrategy pipeline",
                len(strategies))
    return engines, strategies, bus


def replace_mode(cfg: CoreConfig) -> CoreConfig:
    """Apply SEULGI_LIVE env at construction time without mutating the
    catalog instances (which would persist across boots in tests)."""
    from dataclasses import replace
    mode = "live" if os.getenv("SEULGI_LIVE") == "1" else "paper"
    return replace(cfg, mode=mode)

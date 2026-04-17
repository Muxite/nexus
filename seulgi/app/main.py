"""
Seulgi main entry point.

Starts the full pipeline:
  1. Load config + connect DB.
  2. Load/refresh market index (embeddings from Gamma API).
  3. Build engine registry (one or more trading strategies from config.yaml).
  4. Launch one SiteWatcher task per watched site (Playwright).
  5. Drain the shared headline queue → embed → match → fan out to all engines.
  6. Periodically refresh markets, monitor positions, update dashboard.

Multiple engines run concurrently on every headline, each with their own
balance and rule set. Results are recorded in `engine_results` DB table and
displayed in data/dashboard/engine_comparison.txt.

Dashboard files are written every 30 seconds (configurable).
Ctrl-C / SIGTERM shuts down cleanly.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from datetime import datetime, timezone

from ai import DirectionClassifier
from config import load_config
from dashboard import Dashboard, WatcherStatus, PositionEvent
from db import SeulgiDB
from engine import EngineRunner, build_engines
from markets import Embedder, MarketIndex
from watcher import RawHeadline, SiteWatcher


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )


# ---------------------------------------------------------------------------
# Background loops
# ---------------------------------------------------------------------------


async def _market_refresh_loop(
    index: MarketIndex, interval_secs: float, dash: Dashboard
) -> None:
    logger = logging.getLogger(__name__)
    while True:
        try:
            await asyncio.sleep(interval_secs)
            if index.needs_refresh:
                count = await index.refresh()
                dash.set_market_index(count)
                logger.info("[main] market refresh: %d markets", count)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("[main] market refresh error: %s", exc)


async def _position_monitor_loop(
    runner: EngineRunner,
    db: SeulgiDB,
    dash: Dashboard,
    interval_secs: float = 60.0,
) -> None:
    logger = logging.getLogger(__name__)
    while True:
        try:
            await asyncio.sleep(interval_secs)
            await runner.monitor_all_positions()

            # Update per-engine balances on dashboard
            for engine in runner.engines:
                dash.set_balance(engine.name, engine.trader.balance)

            # Update open positions display
            raw_positions = await db.get_open_positions()
            now = datetime.now(timezone.utc)
            pos_events = []
            for p in raw_positions:
                opened = datetime.fromisoformat(p["opened_at"])
                if opened.tzinfo is None:
                    opened = opened.replace(tzinfo=timezone.utc)
                age_h = (now - opened).total_seconds() / 3600
                pos_events.append(PositionEvent(
                    condition_id=p["condition_id"],
                    question=p.get("question", p["condition_id"]),
                    direction=p["direction"],
                    size_usdc=p["size_usdc"],
                    entry_price=p["entry_price"],
                    current_price=p.get("last_price") or p["entry_price"],
                    unrealized_pnl=p.get("unrealized_pnl") or 0.0,
                    p_hat=p.get("p_hat") or 0.0,
                    opened_at=opened,
                    age_hours=age_h,
                ))
            dash.set_open_positions(pos_events)

            # Engine comparison summary refresh
            summary = await db.get_engine_summary()
            skips = await db.get_engine_skip_reasons()
            dash.set_engine_summary(summary, skips)

        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("[main] position monitor error: %s", exc)


async def _watcher_status_loop(
    watchers: list[SiteWatcher], dash: Dashboard, interval_secs: float = 30.0
) -> None:
    while True:
        try:
            await asyncio.sleep(interval_secs)
            for w in watchers:
                s = w.status
                dash.set_watcher_status(WatcherStatus(
                    url=s["url"],
                    reachable=s["reachable"],
                    failures=s["failures"],
                    last_headline_at=s["last_headline_at"],
                    last_headline=s["last_headline"],
                    total_headlines=s["total_headlines"],
                    is_unreachable=s["is_unreachable"],
                ))
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logging.getLogger(__name__).warning("[main] watcher status error: %s", exc)


async def _headline_drain_loop(
    queue: asyncio.Queue,
    runner: EngineRunner,
    dash: Dashboard,
) -> None:
    logger = logging.getLogger(__name__)
    while True:
        try:
            headline: RawHeadline = await queue.get()
            dash.record_news(headline.site_url, headline.headline, headline.article_url)
            asyncio.create_task(runner.process(headline))
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("[main] drain loop error: %s", exc)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def main() -> None:
    cfg = load_config()
    _setup_logging(cfg.log_level)
    logger = logging.getLogger(__name__)
    logger.info("Seulgi starting up")

    # Dashboard
    dash = Dashboard(data_dir=cfg.dashboard.data_dir, interval_secs=cfg.dashboard.interval_secs)

    # DB
    db = SeulgiDB(cfg.db.path)
    await db.connect()

    # Embedder + Market index
    embedder = Embedder()
    index = MarketIndex(db=db, embedder=embedder, cfg=cfg)
    cached = await index.load_from_db()
    if cached == 0 or index.needs_refresh:
        try:
            count = await index.refresh()
            dash.set_market_index(count)
            logger.info("[main] market index ready: %d markets", count)
        except Exception as exc:
            logger.warning("[main] market refresh failed: %s — cached=%d", exc, cached)
            if cached:
                dash.set_market_index(cached)
    else:
        dash.set_market_index(cached)
        logger.info("[main] market index loaded from DB: %d markets", cached)

    # AI classifier (shared across all engines)
    api_key = cfg.ai.api_key or os.getenv("OPENROUTER_API_KEY") or os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        logger.warning("[main] No OPENROUTER_API_KEY — AI classification will fail")
    classifier = DirectionClassifier(api_key=api_key, model=cfg.ai.model, api_base=cfg.ai.api_base)

    # Build engines
    engines = build_engines(cfg, db, classifier)
    for engine in engines:
        await engine.trader.init_from_db()
        dash.set_balance(engine.name, engine.trader.balance)

    logger.info("[main] engines: %s", [e.name for e in engines])

    # Engine runner (fans out headlines to all engines)
    runner = EngineRunner(engines=engines, index=index, embedder=embedder, db=db, dashboard=dash)

    # Watchers
    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    watchers = [
        SiteWatcher(
            url=site.url,
            change_wait_seconds=site.change_wait_seconds,
            reload_seconds=site.reload_seconds,
            max_failures=site.max_failures,
        )
        for site in cfg.watched_sites
    ]
    for w in watchers:
        s = w.status
        dash.set_watcher_status(WatcherStatus(
            url=s["url"], reachable=True, failures=0,
            last_headline_at=None, last_headline="", total_headlines=0,
            is_unreachable=False,
        ))

    watcher_tasks = [
        asyncio.create_task(w.run(queue), name=f"watcher-{w._url}")
        for w in watchers
    ]

    refresh_secs = cfg.markets.refresh_hours * 3600
    bg_tasks = [
        asyncio.create_task(dash.run(), name="dashboard"),
        asyncio.create_task(_market_refresh_loop(index, refresh_secs, dash), name="market-refresh"),
        asyncio.create_task(_position_monitor_loop(runner, db, dash), name="position-monitor"),
        asyncio.create_task(_watcher_status_loop(watchers, dash), name="watcher-status"),
        asyncio.create_task(_headline_drain_loop(queue, runner, dash), name="drain"),
    ]

    all_tasks = watcher_tasks + bg_tasks

    loop = asyncio.get_event_loop()
    stop_event = asyncio.Event()

    def _shutdown():
        logger.info("[main] shutdown signal received")
        stop_event.set()

    loop.add_signal_handler(signal.SIGINT, _shutdown)
    loop.add_signal_handler(signal.SIGTERM, _shutdown)

    logger.info(
        "[main] %d watchers, %d engines, dashboard → %s",
        len(watcher_tasks), len(engines), cfg.dashboard.data_dir,
    )

    await stop_event.wait()

    logger.info("[main] cancelling tasks …")
    for task in all_tasks:
        task.cancel()
    await asyncio.gather(*all_tasks, return_exceptions=True)

    for w in watchers:
        await w.close()

    await classifier.close()
    for engine in engines:
        await engine.trader.close()
    await db.close()
    logger.info("[main] Seulgi stopped cleanly")


if __name__ == "__main__":
    asyncio.run(main())

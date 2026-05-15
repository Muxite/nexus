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
from dashboard import Dashboard, WatcherStatus, PositionEvent, SystemHealth
from db import SeulgiDB
from article_fetch import ArticleFetcher
from engine import EngineRunner, build_engines
from rules import RuleEngine
from markets import Embedder, MarketIndex
from impact_tracker import PriceImpactTracker
import lessons
from news_memory import NewsMemory
from plotter import QuantPlotter
from watcher import (
    RawHeadline, SiteWatcher, TwitterAccountWatcher, TruthSocialWatcher, FeedWatcher,
    SearchBudget, ScraperHealthPoller,
)


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
                await index.load_heat_from_db()
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

            # Update per-engine balances + staked on dashboard
            for engine in runner.engines:
                dash.set_balance(engine.name, engine.trader.balance)
                dash.set_engine_staked(engine.name, engine.trader.total_staked)

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
                    engine_name=p.get("engine_name") or "default",
                ))
            dash.set_open_positions(pos_events)

            # Engine comparison summary refresh
            summary = await db.get_engine_summary()
            skips = await db.get_engine_skip_reasons()
            dash.set_engine_summary(summary, skips)

            # Quant stats refresh — all-time + 24h + calibration + IC + source + decay
            quant = await db.get_quant_stats()
            quant["impact_24h"] = (await db.get_quant_stats(since_hours=24.0))["impact"]
            quant["calibration"] = await db.get_calibration_curve()
            quant["ic_pairs"] = await db.get_ic_pairs()
            quant["source_stats"] = await db.get_source_stats()
            quant["alpha_decay"] = await db.get_alpha_decay()
            dash.set_quant_stats(quant)

            # External usage (AI tokens + Brave searches)
            usage = await db.get_usage_totals()
            dash.set_usage(usage)

        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("[main] position monitor error: %s", exc)


async def _system_health_loop(
    poller: ScraperHealthPoller,
    db: SeulgiDB,
    dash: Dashboard,
    api_key_present: bool,
    interval_secs: float = 30.0,
) -> None:
    """
    Aggregate the cheap health signals (scraper reachability from the poller,
    AI 1h neutral rate from engine_results, DB last write) and push a single
    SystemHealth snapshot to the dashboard each tick.
    """
    log = logging.getLogger(__name__)
    while True:
        try:
            await asyncio.sleep(interval_secs)
            s = poller.status
            neutral_rate, n_decisions = await db.get_recent_neutral_rate(hours=1.0)
            last_insert = await db.get_last_signal_at()
            dash.set_system_health(SystemHealth(
                scraper_reachable=s["reachable"],
                scraper_url=s["scraper_url"],
                scraper_last_ok_at=s["last_ok_at"],
                scraper_last_error=s.get("last_error") or "",
                scraper_latency_ms=s.get("latency_ms"),
                ai_key_present=api_key_present,
                ai_neutral_rate_1h=neutral_rate,
                ai_total_decisions_1h=n_decisions,
                db_last_insert_at=last_insert,
            ))
        except asyncio.CancelledError:
            break
        except Exception as exc:
            log.debug("[main] system_health loop error: %s", exc)


async def _watcher_status_loop(
    watchers: list, dash: Dashboard, interval_secs: float = 30.0
) -> None:
    while True:
        try:
            await asyncio.sleep(interval_secs)
            for w in watchers:
                s = w.status
                # SiteWatcher status uses "url" key; social watchers use "username"
                url = s.get("url") or f"@{s.get('username', '?')} ({s.get('type', 'social')})"
                dash.set_watcher_status(WatcherStatus(
                    url=url,
                    reachable=s.get("reachable", True),
                    failures=s.get("failures", 0),
                    last_headline_at=s.get("last_headline_at") or s.get("last_tweet_at") or s.get("last_post_at"),
                    last_headline=s.get("last_headline", ""),
                    total_headlines=s.get("total_headlines") or s.get("total_tweets") or s.get("total_posts") or 0,
                    is_unreachable=s.get("is_unreachable", False),
                    budget_used=s.get("budget_used"),
                    budget_limit=s.get("budget_limit"),
                    last_error=s.get("last_error", ""),
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
    dash = Dashboard(
        data_dir=cfg.dashboard.data_dir,
        interval_secs=cfg.dashboard.interval_secs,
        db=db,
    )

    # DB
    db = SeulgiDB(cfg.db.path)
    await db.connect()

    # Cycle-8 scoping: ensure the cycles table has an open row before any
    # restore queries run. The SEULGI__CYCLE_NAME env var lets cycle.py name
    # the cycle explicitly; falls back to a timestamped name otherwise.
    _default_cycle_name = os.getenv(
        "SEULGI__CYCLE_NAME",
        f"cycle_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}",
    )
    _current_cycle = await db.ensure_current_cycle(default_name=_default_cycle_name)
    logger.info("[main] current cycle: %s (id=%d, started=%s)",
                _current_cycle["name"], _current_cycle["id"], _current_cycle["started_at"])

    # Embedder + Market index
    embedder = Embedder()
    index = MarketIndex(db=db, embedder=embedder, cfg=cfg)
    cached = await index.load_from_db()
    await index.load_heat_from_db()
    if cached == 0 or index.needs_refresh:
        try:
            count = await index.refresh()
            await index.load_heat_from_db()  # refresh may have added new markets
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
    classifier = DirectionClassifier(
        api_key=api_key,
        model=cfg.ai.model,
        api_base=cfg.ai.api_base,
        premium_model=cfg.ai.premium_model or None,
        premium_sim_threshold=cfg.ai.premium_sim_threshold,
        ensemble_samples=cfg.ai.ensemble_samples,
        ensemble_temperature=cfg.ai.ensemble_temperature,
    )

    # Manual-planning rules engine — shared across trading engines.
    # Rules live in /app/rules/ (baked into the image so reload-on-restart is
    # the source of truth). Loaded once at startup; restart to pick up edits.
    from pathlib import Path as _Path
    rules_dir = _Path(__file__).resolve().parent / "rules"
    rule_engine = RuleEngine(rules_dir=rules_dir)

    # Build engines
    engines = build_engines(cfg, db, classifier, rule_engine=rule_engine)
    dash.set_infinite_balance(cfg.trading.infinite_balance)
    # Pre-seed all engine names so the dashboard's per-engine block always
    # lists every engine, even before init_from_db has filled in real values.
    # Avoids a startup race where the render loop intermittently drops engines.
    dash.register_engines([e.name for e in engines])
    for engine in engines:
        await engine.trader.init_from_db()
        dash.set_balance(engine.name, engine.trader.balance)
        dash.set_engine_staked(engine.name, engine.trader.total_staked)

    logger.info("[main] engines: %s", [e.name for e in engines])

    # News memory (semantic deduplication)
    news_memory = NewsMemory(
        path=cfg.news_memory.path,
        embedder=embedder,
        threshold=cfg.news_memory.dedup_distance_threshold,
    )
    logger.info("[main] NewsMemory ready — %d headlines stored", news_memory.size)

    # Price-impact tracker (hypothesis testing)
    impact_tracker = PriceImpactTracker(db=db, dashboard_dir=cfg.dashboard.data_dir)
    await impact_tracker.start()
    logger.info("[main] PriceImpactTracker started")

    # Article body fetcher (fire-and-forget; first paragraph capture for analysis)
    article_fetcher = ArticleFetcher(db=db)
    logger.info("[main] ArticleFetcher ready")

    # Engine runner (fans out headlines to all engines)
    runner = EngineRunner(
        engines=engines, index=index, embedder=embedder,
        db=db, classifier=classifier, rule_engine=rule_engine,
        dashboard=dash, news_memory=news_memory,
        impact_tracker=impact_tracker,
        article_fetcher=article_fetcher,
    )

    # Watchers
    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    watchers: list = [
        SiteWatcher(
            url=site.url,
            change_wait_seconds=site.change_wait_seconds,
            reload_seconds=site.reload_seconds,
            max_failures=site.max_failures,
            db=db,
        )
        for site in cfg.watched_sites
    ]
    # Cycle-8: RSS/Atom subscribers for outlets identified as feed-capable
    # in cycle 4's preflight. Vastly cheaper per fetch than Playwright; runs
    # alongside the html watchers (the html version is dropped from
    # watched_sites for any URL we cover here, so we don't duplicate work).
    watchers += [
        FeedWatcher(
            site_url=f.site_url,
            feed_url=f.feed_url,
            interval_seconds=f.interval_seconds,
            max_failures=f.max_failures,
            db=db,
        )
        for f in cfg.feeds
    ]
    for w in watchers:
        s = w.status
        dash.set_watcher_status(WatcherStatus(
            url=s["url"], reachable=True, failures=0,
            last_headline_at=None, last_headline="", total_headlines=0,
            is_unreachable=False,
        ))

    # Shared Brave Search budget across all Twitter watchers
    brave_budget = SearchBudget(per_hour=cfg.search_budget.brave_per_hour, db=db)
    logger.info("[main] Brave Search budget: %d calls/hour", cfg.search_budget.brave_per_hour)

    # Social account watchers
    social_watchers: list = []
    for acc in cfg.social_accounts:
        if acc.platform == "twitter":
            sw = TwitterAccountWatcher(
                username=acc.username,
                interval_seconds=acc.interval_seconds,
                max_failures=acc.max_failures,
                budget=brave_budget,
            )
        elif acc.platform == "truth_social":
            sw = TruthSocialWatcher(
                username=acc.username,
                interval_seconds=acc.interval_seconds,
                max_failures=acc.max_failures,
            )
        else:
            logger.warning("[main] unknown platform %r for %s — skipping", acc.platform, acc.username)
            continue
        social_watchers.append(sw)
        logger.info("[main] social watcher: @%s (%s, interval=%ds)",
                    acc.username, acc.platform, acc.interval_seconds)

    all_watchers = watchers + social_watchers
    watcher_tasks = [
        asyncio.create_task(w.run(queue), name=f"watcher-{getattr(w, '_url', getattr(w, '_username', '?'))}")
        for w in all_watchers
    ]

    refresh_secs = cfg.markets.refresh_hours * 3600

    # Periodic matplotlib quant plots (every 10 minutes by default)
    plotter = QuantPlotter(
        db=db,
        dashboard_dir=cfg.dashboard.data_dir,
        interval_secs=cfg.plotter.interval_secs,
    )

    # Scraper health poller — surfaces a single root cause when every watcher
    # is dead at once because the shared scraper is unreachable.
    scraper_poller = ScraperHealthPoller()
    await scraper_poller.start()

    bg_tasks = [
        asyncio.create_task(dash.run(), name="dashboard"),
        asyncio.create_task(_market_refresh_loop(index, refresh_secs, dash), name="market-refresh"),
        asyncio.create_task(_position_monitor_loop(runner, db, dash), name="position-monitor"),
        asyncio.create_task(_watcher_status_loop(all_watchers, dash), name="watcher-status"),
        asyncio.create_task(
            _system_health_loop(scraper_poller, db, dash, api_key_present=bool(api_key)),
            name="system-health",
        ),
        asyncio.create_task(_headline_drain_loop(queue, runner, dash), name="drain"),
        asyncio.create_task(plotter.run(), name="quant-plotter"),
        asyncio.create_task(lessons.runner(db, interval_secs=600.0), name="lessons"),
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

    # ── Safe shutdown ──────────────────────────────────────────────────
    # Order matters:
    #   1. Stop accepting NEW headlines — close watchers so no new items hit
    #      the queue. In-flight runner.process() calls are allowed to finish
    #      so we don't lose trades that already debited balance / inserted
    #      signal rows.
    #   2. Cancel long-running background loops (dashboard, monitors, drain).
    #   3. Close HTTP clients + DB. db.close() runs a WAL checkpoint so the
    #      next start reads a clean main DB file.
    # The whole drain is bounded by a timeout — if anything refuses to die
    # we log it and force-exit rather than hang the host shutdown.
    async def _drain() -> None:
        logger.info("[main] stopping watchers …")
        for w in all_watchers:
            try:
                await asyncio.wait_for(w.close(), timeout=3.0)
            except (asyncio.TimeoutError, Exception) as exc:  # noqa: BLE001
                logger.warning("[main] watcher close timeout/err: %s", exc)
        for task in watcher_tasks:
            task.cancel()

        logger.info("[main] cancelling background loops …")
        for task in bg_tasks:
            task.cancel()
        await asyncio.gather(*all_tasks, return_exceptions=True)

        logger.info("[main] closing subsystems …")
        await classifier.close()
        await news_memory.close()
        await impact_tracker.close()
        await article_fetcher.close()
        await runner.close()
        for engine in engines:
            await engine.trader.close()
        await db.close()    # PRAGMA wal_checkpoint(TRUNCATE) runs inside

    try:
        await asyncio.wait_for(_drain(), timeout=20.0)
        logger.info("[main] Seulgi stopped cleanly")
    except asyncio.TimeoutError:
        logger.warning("[main] shutdown timed out after 20s — forcing exit")


if __name__ == "__main__":
    asyncio.run(main())

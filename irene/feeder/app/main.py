"""
Irene Feeder service.

Concurrently polls NewsAPI and RSS feeds, embeds incoming signals,
maps them to open markets, writes to PostgreSQL, and publishes
MappedSignal JSON to the Redis irene:signals channel.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import signal
from datetime import datetime, timedelta, timezone

import aiohttp

from irene_shared.config import ConfigManager
from irene_shared.db import IreneDB
from irene_shared.models import MarketEventType
from irene_shared.redis_client import IreneRedis
from irene_shared.timing import ProfileStore, Timer, jittered_sleep

from sources.newsapi import NewsAPISource
from sources.rss import RSSSource
from mapper import SignalMapper

# Import Embedder from scanner — shared within Irene via volume mount / PYTHONPATH
import sys, os as _os
_scanner_app = _os.path.join(_os.path.dirname(__file__), "..", "..", "scanner", "app")
if _scanner_app not in sys.path:
    sys.path.insert(0, _scanner_app)
from embedder import Embedder  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [feeder] %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

_CONFIG_PATH = os.getenv("IRENE_CONFIG_PATH", "/config/config.yaml")


class Feeder:
    def __init__(
        self,
        config_manager: ConfigManager,
        db: IreneDB,
        redis: IreneRedis,
    ) -> None:
        self._cfg = config_manager
        self._db = db
        self._redis = redis
        self._embedder = Embedder()
        self._mapper = SignalMapper(db)
        self._running = False
        self._newsapi_last_fetched: datetime | None = None
        self._rss_last_fetched: datetime | None = None
        self._profile = ProfileStore(window=200)
        self._rng = random.Random()

    async def start(self) -> None:
        logger.info("Feeder starting")
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._embedder.load)
        self._running = True

        async with aiohttp.ClientSession() as session:
            self._session = session
            await asyncio.gather(
                self._newsapi_loop(),
                self._rss_loop(),
            )

    async def stop(self) -> None:
        logger.info("Feeder stopping")
        self._running = False

    # ------------------------------------------------------------------
    # Polling loops
    # ------------------------------------------------------------------

    async def _newsapi_loop(self) -> None:
        # Startup jitter: spread initial fetch so scanner and feeder don't both
        # hit their first iteration at t=0.
        cfg = self._cfg.get()
        startup_delay = self._rng.uniform(0, cfg.signals.newsapi_poll_seconds * 0.5)
        logger.debug("NewsAPI loop startup delay: %.1fs", startup_delay)
        await asyncio.sleep(startup_delay)

        while self._running:
            cfg = self._cfg.get()
            if cfg.signals.newsapi_key:
                try:
                    with Timer("feeder", "newsapi_ingest", self._profile):
                        await self._ingest_newsapi()
                except Exception as exc:
                    logger.exception("NewsAPI ingestion error: %s", exc)
            interval = cfg.signals.newsapi_poll_seconds
            jitter = cfg.signals.jitter_fraction
            await jittered_sleep(interval, jitter_fraction=jitter, rng=self._rng)

    async def _rss_loop(self) -> None:
        # Stagger RSS startup: each service instance gets a different offset
        # so multiple feeders (if scaled) don't all hammer feeds simultaneously.
        cfg = self._cfg.get()
        startup_spread = cfg.signals.rss_poll_seconds * cfg.signals.rss_startup_spread_fraction
        startup_delay = self._rng.uniform(0, startup_spread)
        logger.debug("RSS loop startup delay: %.1fs", startup_delay)
        await asyncio.sleep(startup_delay)

        while self._running:
            cfg = self._cfg.get()
            try:
                with Timer("feeder", "rss_ingest", self._profile):
                    await self._ingest_rss_feeds()
            except Exception as exc:
                logger.exception("RSS ingestion error: %s", exc)
            interval = cfg.signals.rss_poll_seconds
            jitter = cfg.signals.jitter_fraction
            await jittered_sleep(interval, jitter_fraction=jitter, rng=self._rng)

    # ------------------------------------------------------------------
    # Ingestion
    # ------------------------------------------------------------------

    async def _ingest_newsapi(self) -> None:
        cfg = self._cfg.get()
        since = self._newsapi_last_fetched or (
            datetime.now(timezone.utc) - timedelta(hours=2)
        )

        source = NewsAPISource(
            api_key=cfg.signals.newsapi_key,
            session=self._session,
            page_size=cfg.signals.newsapi_page_size,
        )
        signals = await source.fetch(since=since)
        self._newsapi_last_fetched = datetime.now(timezone.utc)
        await self._process_signals(signals)

    async def _ingest_rss_feeds(self) -> None:
        cfg = self._cfg.get()
        since = self._rss_last_fetched or (
            datetime.now(timezone.utc) - timedelta(hours=2)
        )

        # Per-feed jitter: stagger concurrent fetches so feeds don't fire in
        # a single burst. Each feed gets a random delay up to half the poll interval.
        max_feed_offset = cfg.signals.rss_poll_seconds * 0.5
        async def _fetch_with_jitter(url: str) -> list:
            delay = self._rng.uniform(0, max_feed_offset)
            if delay > 0.5:
                await asyncio.sleep(delay)
            return await RSSSource(url, self._session).fetch(since=since)

        tasks = [_fetch_with_jitter(feed.url) for feed in cfg.signals.rss_feeds]
        all_results = await asyncio.gather(*tasks, return_exceptions=True)
        self._rss_last_fetched = datetime.now(timezone.utc)

        signals = []
        for result in all_results:
            if isinstance(result, Exception):
                logger.warning("RSS feed error: %s", result)
            else:
                signals.extend(result)

        await self._process_signals(signals)

    # ------------------------------------------------------------------
    # Processing
    # ------------------------------------------------------------------

    async def _process_signals(self, signals) -> None:
        if not signals:
            return

        loop = asyncio.get_event_loop()
        cfg = self._cfg.get()

        for signal in signals:
            # Insert into DB (dedup by URL)
            signal_id = await self._db.insert_signal(
                source=signal.source.value,
                source_detail=signal.source_detail,
                url=signal.url,
                title=signal.title,
                description=signal.description,
                published_at=signal.published_at,
            )
            if signal_id is None:
                continue  # duplicate URL

            # Embed the signal
            text = f"{signal.title} {signal.description or ''}".strip()
            with Timer("feeder", "signal_embed", self._profile):
                embedding = await loop.run_in_executor(None, self._embedder.embed, text)
            await self._db.update_signal_embedding(signal_id, embedding)

            # Map to markets
            with Timer("feeder", "signal_map", self._profile):
                mapped_signals = await self._mapper.map(
                    signal=signal,
                    signal_id=signal_id,
                    query_embedding=embedding,
                    cfg=cfg.signals,
                )

            if not mapped_signals:
                continue

            # Persist + publish each match
            for ms in mapped_signals:
                if ms.direction.value == "NEUTRAL":
                    logger.debug(
                        "Signal %d matched %s but direction=NEUTRAL — skipping",
                        signal_id, ms.condition_id,
                    )
                    continue

                ms_id = await self._db.insert_mapped_signal(
                    signal_id=signal_id,
                    condition_id=ms.condition_id,
                    similarity_score=ms.similarity_score,
                    direction=ms.direction.value,
                    direction_confidence=ms.direction_confidence,
                )

                await self._db.log_event(
                    condition_id=ms.condition_id,
                    event_type=MarketEventType.SIGNAL_MATCH,
                    payload={
                        "signal_id": signal_id,
                        "mapped_signal_id": ms_id,
                        "title": signal.title,
                        "source": signal.source_detail,
                        "similarity": round(ms.similarity_score, 4),
                        "direction": ms.direction.value,
                        "direction_confidence": round(ms.direction_confidence, 4),
                    },
                )

                # Attach the DB id to the model before publishing
                ms_with_id = ms.model_copy(update={"signal_id": ms_id})
                receivers = await self._redis.publish_signal(ms_with_id)

                await self._db.mark_mapped_signal_dispatched(ms_id)
                await self._db.mark_signal_mapped(signal_id)

                logger.info(
                    "Dispatched signal %d → market %s (dir=%s, sim=%.3f, receivers=%d)",
                    signal_id, ms.condition_id, ms.direction.value,
                    ms.similarity_score, receivers,
                )

        # Heartbeat after each batch
        await self._redis.publish_heartbeat("feeder")
        await self._redis.set_last_seen("feeder", datetime.now(timezone.utc).isoformat())


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


async def main() -> None:
    cfg_manager = ConfigManager(_CONFIG_PATH)
    await cfg_manager.start()

    cfg = cfg_manager.get()
    db = IreneDB(cfg.database.url, pool_size=cfg.database.pool_size)
    await db.connect()

    redis = IreneRedis(
        url=cfg.redis.url,
        signal_channel=cfg.redis.signal_channel,
        heartbeat_channel=cfg.redis.heartbeat_channel,
    )
    await redis.connect()

    feeder = Feeder(cfg_manager, db, redis)

    loop = asyncio.get_event_loop()

    def _shutdown(*_):
        logger.info("Shutdown signal received")
        loop.create_task(feeder.stop())

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        await feeder.start()
    finally:
        await redis.close()
        await db.close()
        await cfg_manager.stop()


if __name__ == "__main__":
    asyncio.run(main())

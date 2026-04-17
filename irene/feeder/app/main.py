"""
Irene Feeder service.

Concurrently polls NewsAPI, Brave Search, RSS feeds, and watched pages,
embeds incoming signals, maps them to open markets, writes to PostgreSQL,
and publishes MappedSignal JSON to the Redis irene:signals channel.

Edge detection:
  - Every incoming signal is embedded and immediately matched against the
    current market index via cosine similarity.
  - Every match (above threshold) is logged to the DB regardless of direction
    so the monitoring dashboard can show full coverage.
  - Non-NEUTRAL matches are dispatched to the trader via Redis pub/sub.
  - AI direction classification (Claude Haiku) is used when cfg.llm.api_key
    is set and cfg.signals.use_llm_direction=True; falls back to keyword
    classifier otherwise.
  - A backfill loop re-processes historical unmapped signals every 30 minutes.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import signal
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiohttp

from irene_shared.config import ConfigManager
from irene_shared.db import IreneDB
from irene_shared.models import Direction, MarketEventType, MappedSignal, RawSignal, SignalSource
from irene_shared.ratelimit import RotatingApiKeyPool
from irene_shared.redis_client import IreneRedis
from irene_shared.timing import ProfileStore, Timer, jittered_sleep

from sources.newsapi import NewsAPISource
from sources.rss import RSSSource
from sources.brave import BraveNewsSource
from sources.watched import WatchedPageSource
from mapper import SignalMapper
from direction import DirectionClassifier, DirectionResult
from ai_direction import AIDirectionClassifier

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
if _log_dir := os.getenv("IRENE_LOG_DIR"):
    os.makedirs(_log_dir, exist_ok=True)
    _fh = logging.FileHandler(os.path.join(_log_dir, "feeder.log"))
    _fh.setFormatter(logging.Formatter("%(asctime)s [feeder] %(levelname)s %(name)s: %(message)s"))
    logging.getLogger().addHandler(_fh)
logger = logging.getLogger(__name__)

_CONFIG_PATH = os.getenv("IRENE_CONFIG_PATH", "/config/config.yaml")

# Cooldown before retrying an unreachable watched page (seconds)
_UNREACHABLE_COOLDOWN = 1800  # 30 minutes


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
        self._keyword_classifier = DirectionClassifier()
        self._ai_classifier: Optional[AIDirectionClassifier] = None
        self._running = False
        self._newsapi_last_fetched: datetime | None = None
        self._rss_last_fetched: datetime | None = None
        self._profile = ProfileStore(window=200)
        self._rng = random.Random()
        self._newsapi_key_pool = RotatingApiKeyPool("newsapi")
        self._brave_key_pool = RotatingApiKeyPool("brave")
        # Persistent RSS source instances keyed by URL
        self._rss_sources: dict[str, RSSSource] = {}
        # Persistent watched-page tasks: url → (WatchedPageSource, asyncio.Task)
        self._watched_sources: dict[str, tuple[WatchedPageSource, asyncio.Task]] = {}
        # Queue drained by _watched_drain_loop
        self._watched_queue: asyncio.Queue = asyncio.Queue()
        # Unreachable URLs and their retry-after timestamp
        self._unreachable_until: dict[str, float] = {}

    def _get_ai_classifier(self) -> Optional[AIDirectionClassifier]:
        """Return AI classifier if configured, else None (use keyword fallback)."""
        cfg = self._cfg.get()
        if not cfg.signals.use_llm_direction:
            return None
        # Accept key from config or from GEMINI_API_KEY env var directly
        api_key = cfg.llm.api_key or os.getenv("GEMINI_API_KEY", "")
        if not api_key:
            logger.warning("use_llm_direction=true but no API key set — falling back to keyword classifier")
            return None
        if self._ai_classifier is None:
            self._ai_classifier = AIDirectionClassifier(
                api_key=api_key,
                model=cfg.llm.model,
            )
        return self._ai_classifier

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
                self._brave_loop(),
                self._watched_manager_loop(),
                self._watched_drain_loop(),
                self._backfill_unmapped_loop(),
                self._profile_loop(),
            )

    async def stop(self) -> None:
        logger.info("Feeder stopping")
        self._running = False

    # ------------------------------------------------------------------
    # Polling loops
    # ------------------------------------------------------------------

    async def _brave_loop(self) -> None:
        cfg = self._cfg.get()
        startup_delay = self._rng.uniform(0, cfg.signals.brave_poll_seconds * 0.5)
        await asyncio.sleep(startup_delay)

        while self._running:
            cfg = self._cfg.get()
            if cfg.signals.brave_api_keys:
                try:
                    with Timer("feeder", "brave_ingest", self._profile):
                        await self._ingest_brave()
                except Exception as exc:
                    logger.exception("Brave ingestion error: %s", exc)
            interval = cfg.signals.brave_poll_seconds
            jitter = cfg.signals.jitter_fraction
            await jittered_sleep(interval, jitter_fraction=jitter, rng=self._rng)

    async def _ingest_brave(self) -> None:
        cfg = self._cfg.get()
        api_key = await self._brave_key_pool.reserve_key(
            self._redis,
            cfg.signals.brave_api_keys,
            cfg.rate_limits.brave_max_per_day,
            cost=1,
        )
        if not api_key:
            return
        since = self._rss_last_fetched
        source = BraveNewsSource(api_key=api_key, session=self._session)
        signals = await source.fetch(since=since)
        await self._process_signals(signals)

    async def _watched_manager_loop(self) -> None:
        """
        Reconcile running WatchedPageSource tasks with configured watched_pages.

        - Starts new watchers for newly configured URLs (skipping those in cooldown).
        - Cancels watchers for removed URLs.
        - Restarts crashed tasks (unless URL is in unreachable cooldown).
        - Detects sources that self-marked unreachable and applies cooldown.
        """
        while self._running:
            cfg = self._cfg.get()
            configured_urls = {entry.url for entry in cfg.signals.watched_pages}
            now = time.monotonic()

            # Start sources for newly configured URLs (not in cooldown)
            for entry in cfg.signals.watched_pages:
                url = entry.url
                if url in self._watched_sources:
                    continue
                if url in self._unreachable_until and now < self._unreachable_until[url]:
                    remaining = self._unreachable_until[url] - now
                    logger.debug(
                        "WatchedPage(%s): unreachable cooldown — %.0fs remaining", url, remaining
                    )
                    continue

                source = WatchedPageSource(
                    url=url,
                    reload_seconds=entry.reload_seconds,
                    jitter_fraction=entry.jitter_fraction,
                    change_wait_seconds=entry.change_wait_seconds,
                    post_mode=entry.post_mode,
                    max_failures=entry.max_failures,
                    rng=self._rng,
                )
                task = asyncio.create_task(
                    source.run(self._watched_queue),
                    name=f"watched:{url}",
                )
                self._watched_sources[url] = (source, task)
                logger.info("WatchedPage: started watcher for %s", url)

            # Stop sources for removed URLs
            for url in list(self._watched_sources):
                if url not in configured_urls:
                    source, task = self._watched_sources.pop(url)
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    await source.close()
                    logger.info("WatchedPage: stopped watcher for %s", url)

            # Check for crashed or unreachable tasks
            for url, (source, task) in list(self._watched_sources.items()):
                if not task.done():
                    continue

                # Task finished — either crashed or self-terminated (unreachable)
                if source.is_unreachable:
                    logger.warning(
                        "WatchedPage(%s): marked unreachable — cooldown %ds", url, _UNREACHABLE_COOLDOWN
                    )
                    self._unreachable_until[url] = now + _UNREACHABLE_COOLDOWN
                    del self._watched_sources[url]
                    await source.close()
                    continue

                exc = task.exception() if not task.cancelled() else None
                if exc:
                    logger.warning("WatchedPage(%s): task crashed (%s), restarting", url, exc)
                await source.close()

                cfg_entry = next((e for e in cfg.signals.watched_pages if e.url == url), None)
                if cfg_entry:
                    new_source = WatchedPageSource(
                        url=url,
                        reload_seconds=cfg_entry.reload_seconds,
                        jitter_fraction=cfg_entry.jitter_fraction,
                        change_wait_seconds=cfg_entry.change_wait_seconds,
                        post_mode=cfg_entry.post_mode,
                        max_failures=cfg_entry.max_failures,
                        rng=self._rng,
                    )
                    new_task = asyncio.create_task(
                        new_source.run(self._watched_queue),
                        name=f"watched:{url}",
                    )
                    self._watched_sources[url] = (new_source, new_task)
                else:
                    del self._watched_sources[url]

            await asyncio.sleep(60)

        # Cleanup on shutdown
        for url, (source, task) in self._watched_sources.items():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            await source.close()

    async def _watched_drain_loop(self) -> None:
        """Drain signals from self._watched_queue and process them immediately."""
        while self._running:
            batch = []
            try:
                sig = await asyncio.wait_for(self._watched_queue.get(), timeout=5.0)
                batch.append(sig)
                while not self._watched_queue.empty() and len(batch) < 20:
                    batch.append(self._watched_queue.get_nowait())
            except asyncio.TimeoutError:
                continue

            if batch:
                try:
                    await self._process_signals(batch)
                except Exception as exc:
                    logger.exception("Watched drain error: %s", exc)

    async def _backfill_unmapped_loop(self) -> None:
        """
        Re-run the mapper over historical signals with embeddings but no market match.

        Runs 30s after startup (lets market cache warm up), then every 30 minutes.
        """
        await asyncio.sleep(30)

        while self._running:
            try:
                await self._run_backfill()
            except Exception as exc:
                logger.exception("Backfill error: %s", exc)
            await asyncio.sleep(1800)

    async def _run_backfill(self) -> None:
        cfg = self._cfg.get()
        rows = await self._db.get_unmapped_signals_with_embeddings(limit=500)
        if not rows:
            logger.debug("Backfill: no unmapped signals to process")
            return

        logger.info("Backfill: re-mapping %d unmapped signals against current market index", len(rows))
        dispatched_count = 0

        for row in rows:
            if not self._running:
                break
            embedding = row["embedding"]
            if embedding is None:
                continue

            try:
                source_enum = SignalSource(row["source"])
            except ValueError:
                source_enum = SignalSource.RSS

            signal_obj = RawSignal(
                source=source_enum,
                source_detail=row["source_detail"],
                url=row["url"],
                title=row["title"],
                description=row["description"],
                published_at=row["published_at"],
                ingested_at=row["ingested_at"],
            )

            with Timer("feeder", "signal_map", self._profile):
                mapped_signals = await self._mapper.map(
                    signal=signal_obj,
                    signal_id=row["id"],
                    query_embedding=embedding,
                    cfg=cfg.signals,
                )

            if not mapped_signals:
                # No market matches at all — mark mapped so we stop retrying
                await self._db.mark_signal_mapped(row["id"])
                continue

            # Re-classify direction (AI if available, else keyword)
            signal_text = f"{signal_obj.title} {signal_obj.description or ''}".strip()
            direction_map = await self._classify_directions(
                signal_text=signal_text,
                mapped_signals=mapped_signals,
                min_confidence=cfg.signals.direction_min_confidence,
            )

            signal_dispatched = False
            for ms in mapped_signals:
                dr = direction_map.get(ms.condition_id)
                direction = dr.direction if dr else ms.direction
                confidence = dr.confidence if dr else ms.direction_confidence

                # Document every edge detection regardless of direction
                ms_id = await self._db.insert_mapped_signal(
                    signal_id=row["id"],
                    condition_id=ms.condition_id,
                    similarity_score=ms.similarity_score,
                    direction=direction.value,
                    direction_confidence=confidence,
                )
                dispatched = direction != Direction.NEUTRAL
                await self._db.log_event(
                    condition_id=ms.condition_id,
                    event_type=MarketEventType.SIGNAL_MATCH,
                    payload={
                        "signal_id": row["id"],
                        "mapped_signal_id": ms_id,
                        "title": signal_obj.title,
                        "source": signal_obj.source_detail,
                        "similarity": round(ms.similarity_score, 4),
                        "direction": direction.value,
                        "direction_confidence": round(confidence, 4),
                        "dispatched": dispatched,
                        "backfill": True,
                    },
                )

                if not dispatched:
                    logger.debug(
                        "Backfill: signal %d → %s NEUTRAL (sim=%.3f) — logged, not dispatched",
                        row["id"], ms.condition_id, ms.similarity_score,
                    )
                    continue

                ms_with_id = ms.model_copy(update={"signal_id": ms_id, "direction": direction, "direction_confidence": confidence})
                await self._redis.publish_signal(ms_with_id)
                await self._db.mark_mapped_signal_dispatched(ms_id)
                dispatched_count += 1
                signal_dispatched = True
                logger.info(
                    "Backfill: dispatched signal %d → market %s (dir=%s, sim=%.3f)",
                    row["id"], ms.condition_id, direction.value, ms.similarity_score,
                )

            # Only mark signal mapped if at least one match occurred (which it did)
            await self._db.mark_signal_mapped(row["id"])

        logger.info("Backfill: dispatched %d mapped signals from %d historical signals", dispatched_count, len(rows))

    async def _profile_loop(self) -> None:
        """Publish ProfileStore summary to Redis periodically."""
        while self._running:
            cfg = self._cfg.get()
            interval = cfg.timing.publish_interval_seconds
            if interval > 0:
                await asyncio.sleep(interval)
                try:
                    profile = self._profile.summary_as_dict()
                    if self._watched_sources:
                        profile["_watched_pages"] = {
                            url: "running" if not task.done() else "stopped"
                            for url, (_, task) in self._watched_sources.items()
                        }
                    if self._unreachable_until:
                        now = time.monotonic()
                        profile["_unreachable_pages"] = {
                            url: f"retry in {int(ts - now)}s"
                            for url, ts in self._unreachable_until.items()
                            if ts > now
                        }
                    await self._redis.set_profile("feeder", profile)
                except Exception as exc:
                    logger.warning("Failed to publish profile: %s", exc)
            else:
                await asyncio.sleep(60)

    async def _newsapi_loop(self) -> None:
        cfg = self._cfg.get()
        startup_delay = self._rng.uniform(0, cfg.signals.newsapi_poll_seconds * 0.5)
        await asyncio.sleep(startup_delay)

        while self._running:
            cfg = self._cfg.get()
            if cfg.signals.newsapi_keys:
                try:
                    with Timer("feeder", "newsapi_ingest", self._profile):
                        await self._ingest_newsapi()
                except Exception as exc:
                    logger.exception("NewsAPI ingestion error: %s", exc)
            interval = cfg.signals.newsapi_poll_seconds
            jitter = cfg.signals.jitter_fraction
            await jittered_sleep(interval, jitter_fraction=jitter, rng=self._rng)

    async def _rss_loop(self) -> None:
        cfg = self._cfg.get()
        startup_spread = cfg.signals.rss_poll_seconds * cfg.signals.rss_startup_spread_fraction
        startup_delay = self._rng.uniform(0, startup_spread)
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
        api_key = await self._newsapi_key_pool.reserve_key(
            self._redis,
            cfg.signals.newsapi_keys,
            cfg.rate_limits.newsapi_max_per_day,
            cost=1,
        )
        if not api_key:
            return
        since = self._newsapi_last_fetched or (datetime.now(timezone.utc) - timedelta(hours=2))
        source = NewsAPISource(
            api_key=api_key,
            session=self._session,
            page_size=cfg.signals.newsapi_page_size,
        )
        signals = await source.fetch(since=since)
        self._newsapi_last_fetched = datetime.now(timezone.utc)
        await self._process_signals(signals)

    async def _ingest_rss_feeds(self) -> None:
        cfg = self._cfg.get()
        n_feeds = len(cfg.signals.rss_feeds)
        if n_feeds > 0 and not await self._redis.try_consume_daily_budget(
            "rss",
            cfg.rate_limits.rss_http_max_per_day,
            cost=n_feeds,
        ):
            return
        since = self._rss_last_fetched or (datetime.now(timezone.utc) - timedelta(hours=2))

        for feed in cfg.signals.rss_feeds:
            if feed.url not in self._rss_sources:
                self._rss_sources[feed.url] = RSSSource(feed.url, self._session)

        max_feed_offset = cfg.signals.rss_poll_seconds * 0.5

        async def _fetch_with_jitter(url: str) -> list:
            delay = self._rng.uniform(0, max_feed_offset)
            if delay > 0.5:
                await asyncio.sleep(delay)
            return await self._rss_sources[url].fetch(since=since)

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
    # Direction classification
    # ------------------------------------------------------------------

    async def _classify_directions(
        self,
        signal_text: str,
        mapped_signals: list[MappedSignal],
        min_confidence: float,
    ) -> dict[str, DirectionResult]:
        """
        Classify direction for each mapped signal's market.

        Uses AI classifier if available, else falls back to keyword classifier.
        Returns {condition_id: DirectionResult}.
        """
        ai = self._get_ai_classifier()
        if ai is not None:
            markets = [(ms.condition_id, ms.question) for ms in mapped_signals]
            return await ai.classify_batch(signal_text, markets, min_confidence=min_confidence)
        else:
            # Keyword classifier: same result for all markets (signal-level, not per-market)
            dr = self._keyword_classifier.classify(
                signal_text,
                min_confidence=min_confidence,
            )
            return {ms.condition_id: dr for ms in mapped_signals}

    # ------------------------------------------------------------------
    # Processing — immediate edge detection for every incoming signal
    # ------------------------------------------------------------------

    async def _process_signals(self, signals) -> None:
        """
        Embed → map → classify direction → document all detections → dispatch.

        Every signal is processed immediately as it arrives.
        Every (signal, market) pair above the similarity threshold is logged
        to the DB (SIGNAL_MATCH event) regardless of direction so the dashboard
        shows full edge-detection coverage.
        Only non-NEUTRAL pairs are dispatched to the trader via Redis.
        """
        try:
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

                # Embed the signal text (title only for cosine similarity with question embeddings)
                text = signal.title
                with Timer("feeder", "signal_embed", self._profile):
                    embedding = await loop.run_in_executor(None, self._embedder.embed, text)
                await self._db.update_signal_embedding(signal_id, embedding)

                # Map to markets via cosine similarity
                with Timer("feeder", "signal_map", self._profile):
                    mapped_signals = await self._mapper.map(
                        signal=signal,
                        signal_id=signal_id,
                        query_embedding=embedding,
                        cfg=cfg.signals,
                    )

                if not mapped_signals:
                    continue

                logger.info(
                    "Signal %d matched %d markets: %r",
                    signal_id, len(mapped_signals), signal.title[:80],
                )

                # Classify direction for all matched markets (AI or keyword)
                signal_text = f"{signal.title} {signal.description or ''}".strip()
                with Timer("feeder", "signal_classify", self._profile):
                    direction_map = await self._classify_directions(
                        signal_text=signal_text,
                        mapped_signals=mapped_signals,
                        min_confidence=cfg.signals.direction_min_confidence,
                    )

                # Document every detection; dispatch non-NEUTRAL ones
                for ms in mapped_signals:
                    dr = direction_map.get(ms.condition_id)
                    direction = dr.direction if dr else ms.direction
                    confidence = dr.confidence if dr else ms.direction_confidence
                    dispatched = direction != Direction.NEUTRAL

                    ms_id = await self._db.insert_mapped_signal(
                        signal_id=signal_id,
                        condition_id=ms.condition_id,
                        similarity_score=ms.similarity_score,
                        direction=direction.value,
                        direction_confidence=confidence,
                    )

                    # Log every detection — dashboard shows full coverage
                    await self._db.log_event(
                        condition_id=ms.condition_id,
                        event_type=MarketEventType.SIGNAL_MATCH,
                        payload={
                            "signal_id": signal_id,
                            "mapped_signal_id": ms_id,
                            "title": signal.title,
                            "source": signal.source_detail,
                            "similarity": round(ms.similarity_score, 4),
                            "direction": direction.value,
                            "direction_confidence": round(confidence, 4),
                            "dispatched": dispatched,
                        },
                    )

                    if not dispatched:
                        logger.debug(
                            "Edge detected: signal %d → %s NEUTRAL (sim=%.3f) — logged only",
                            signal_id, ms.condition_id, ms.similarity_score,
                        )
                        continue

                    ms_with_id = ms.model_copy(update={
                        "signal_id": ms_id,
                        "direction": direction,
                        "direction_confidence": confidence,
                    })
                    receivers = await self._redis.publish_signal(ms_with_id)
                    await self._db.mark_mapped_signal_dispatched(ms_id)
                    await self._db.mark_signal_mapped(signal_id)

                    logger.info(
                        "Edge dispatched: signal %d → market %s (dir=%s, sim=%.3f, conf=%.2f, receivers=%d)",
                        signal_id, ms.condition_id, direction.value,
                        ms.similarity_score, confidence, receivers,
                    )

        finally:
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

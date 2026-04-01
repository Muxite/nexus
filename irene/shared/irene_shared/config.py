"""
YAML config hot-reload for Irene.

All services call config_manager.get() at use-site — never cache the returned
IreneConfig directly, as it is replaced atomically every 20 seconds.

Environment variable overrides use double-underscore as separator:
  IRENE__TRADING__MIN_EDGE=0.06
  IRENE__POLYMARKET__PRIVATE_KEY=0xabc...
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from typing import Optional

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

logger = logging.getLogger(__name__)

_DEFAULT_RELOAD_INTERVAL = 20  # seconds


# ---------------------------------------------------------------------------
# Config sub-models
# ---------------------------------------------------------------------------


class IreneSection(BaseModel):
    mode: str = "paper"  # "paper" | "live"
    drawdown_limit: float = Field(default=0.10, gt=0, le=1.0)

    @field_validator("mode")
    @classmethod
    def valid_mode(cls, v: str) -> str:
        if v not in ("paper", "live"):
            raise ValueError(f"mode must be 'paper' or 'live', got {v!r}")
        return v


class PolymarketSection(BaseModel):
    clob_url: str = "https://clob.polymarket.com"
    gamma_url: str = "https://gamma-api.polymarket.com"
    private_key: str = ""
    api_key: str = ""
    api_secret: str = ""
    api_passphrase: str = ""
    chain_id: int = 137


class TradingSection(BaseModel):
    balance_usdc: float = Field(default=100.0, gt=0)
    max_position_fraction: float = Field(default=0.20, gt=0, le=1.0)
    kelly_fraction: float = Field(default=0.25, gt=0, le=1.0)
    min_edge: float = Field(default=0.04, gt=0)
    fee_buffer: float = Field(default=0.02, ge=0)
    max_liquidity_fraction: float = Field(default=0.15, gt=0, le=1.0)
    max_open_positions: int = Field(default=8, ge=1)
    order_type: str = "FOK"
    position_timeout_hours: float = Field(default=48.0, gt=0)
    edge_increment_yes: float = Field(default=0.12, gt=0, le=0.5)
    edge_increment_no: float = Field(default=0.12, gt=0, le=0.5)

    @field_validator("order_type")
    @classmethod
    def valid_order_type(cls, v: str) -> str:
        if v not in ("FOK", "FAK"):
            raise ValueError(f"order_type must be FOK or FAK, got {v!r}")
        return v


class MarketsSection(BaseModel):
    min_volume_usdc: float = Field(default=5000.0, ge=0)
    max_hours_to_close: float = Field(default=72.0, gt=0)
    min_hours_to_close: float = Field(default=2.0, ge=0)
    exclude_keywords: list[str] = Field(
        default=[
            "nfl", "nba", "mlb", "nhl", "premier league", "champions league",
            "laliga", "bundesliga", "world cup", "super bowl", "stanley cup",
            "march madness",
        ]
    )

    @model_validator(mode="after")
    def max_greater_than_min(self) -> "MarketsSection":
        if self.max_hours_to_close <= self.min_hours_to_close:
            raise ValueError("max_hours_to_close must be > min_hours_to_close")
        return self


class RSSFeedEntry(BaseModel):
    url: str


class SignalsSection(BaseModel):
    newsapi_key: str = ""
    newsapi_poll_seconds: int = Field(default=60, ge=5)
    newsapi_page_size: int = Field(default=100, ge=1, le=100)
    jitter_fraction: float = Field(default=0.20, ge=0.0, le=1.0)
    # Per-feed startup offset spread: each RSS feed waits a random fraction of
    # rss_poll_seconds before its first fetch, so feeds don't all fire together.
    rss_startup_spread_fraction: float = Field(default=1.0, ge=0.0, le=5.0)
    rss_feeds: list[RSSFeedEntry] = Field(
        default=[
            RSSFeedEntry(url="https://feeds.reuters.com/reuters/topNews"),
            RSSFeedEntry(url="https://feeds.bbci.co.uk/news/rss.xml"),
        ]
    )
    rss_poll_seconds: int = Field(default=45, ge=5)
    similarity_threshold: float = Field(default=0.72, gt=0, lt=1.0)
    top_k_markets: int = Field(default=3, ge=1)
    max_signal_age_seconds: int = Field(default=180, ge=10)
    direction_min_confidence: float = Field(default=0.30, ge=0, le=1.0)
    use_llm_direction: bool = False
    llm_direction_confidence_threshold: float = Field(default=0.50, ge=0, le=1.0)
    yes_keywords: list[str] = Field(default=[])
    no_keywords: list[str] = Field(default=[])


class ScannerSection(BaseModel):
    poll_interval_seconds: int = Field(default=120, ge=10)
    embedding_rebuild_interval_seconds: int = Field(default=3600, ge=60)
    gamma_page_size: int = Field(default=200, ge=1, le=500)
    jitter_fraction: float = Field(default=0.20, ge=0.0, le=1.0)


class MonitorSection(BaseModel):
    port: int = Field(default=8080, ge=1024, le=65535)
    per_market_log_days: int = Field(default=30, ge=1)


class TimingSection(BaseModel):
    # Global jitter applied to all poll intervals not overridden at section level.
    # Individual sections (scanner, signals) can set their own jitter_fraction.
    jitter_fraction: float = Field(default=0.20, ge=0.0, le=1.0)
    # How many timing samples to keep per operation in the in-memory ProfileStore.
    profile_window: int = Field(default=200, ge=10)
    # Publish timing summary to Redis every N seconds (0 = disabled).
    publish_interval_seconds: int = Field(default=60, ge=0)


class LLMSection(BaseModel):
    provider: str = "anthropic"
    api_key: str = ""
    model: str = "claude-haiku-4-5-20251001"


class DatabaseSection(BaseModel):
    url: str = "postgresql://irene:irene@postgres:5432/irene"
    pool_size: int = Field(default=5, ge=1)


class RedisSection(BaseModel):
    url: str = "redis://redis:6379/0"
    signal_channel: str = "irene:signals"
    heartbeat_channel: str = "irene:heartbeat"


# ---------------------------------------------------------------------------
# Root config
# ---------------------------------------------------------------------------


class IreneConfig(BaseModel):
    irene: IreneSection = Field(default_factory=IreneSection)
    polymarket: PolymarketSection = Field(default_factory=PolymarketSection)
    trading: TradingSection = Field(default_factory=TradingSection)
    markets: MarketsSection = Field(default_factory=MarketsSection)
    signals: SignalsSection = Field(default_factory=SignalsSection)
    scanner: ScannerSection = Field(default_factory=ScannerSection)
    monitor: MonitorSection = Field(default_factory=MonitorSection)
    timing: TimingSection = Field(default_factory=TimingSection)
    llm: LLMSection = Field(default_factory=LLMSection)
    database: DatabaseSection = Field(default_factory=DatabaseSection)
    redis: RedisSection = Field(default_factory=RedisSection)

    @model_validator(mode="after")
    def live_requires_private_key(self) -> "IreneConfig":
        if self.irene.mode == "live" and not self.polymarket.private_key:
            raise ValueError("polymarket.private_key is required when irene.mode=live")
        return self


# ---------------------------------------------------------------------------
# Environment variable overlay
# ---------------------------------------------------------------------------


def _apply_env_overrides(raw: dict) -> dict:
    """
    Apply IRENE__<SECTION>__<KEY>=value overrides from environment.
    Double-underscore is the delimiter. Booleans and numbers are coerced.
    """
    for env_key, env_val in os.environ.items():
        if not env_key.startswith("IRENE__"):
            continue
        parts = env_key.split("__")
        if len(parts) != 3:
            continue
        _, section, key = parts
        section = section.lower()
        key = key.lower()
        raw.setdefault(section, {})[key] = _coerce(env_val)
    return raw


def _coerce(value: str):
    """Coerce an env-var string to bool, int, float, or keep as str."""
    if value.lower() in ("true", "yes", "1"):
        return True
    if value.lower() in ("false", "no", "0"):
        return False
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


# ---------------------------------------------------------------------------
# ConfigManager
# ---------------------------------------------------------------------------


class ConfigManager:
    """
    Loads config.yaml and re-reads it every `reload_interval` seconds.

    Usage:
        cm = ConfigManager("/config/config.yaml")
        await cm.start()
        cfg = cm.get()           # always returns the latest validated config
        await cm.stop()
    """

    def __init__(
        self,
        path: str,
        reload_interval: int = _DEFAULT_RELOAD_INTERVAL,
    ) -> None:
        self._path = path
        self._reload_interval = reload_interval
        self._lock = threading.Lock()
        self._config: IreneConfig = self._load()
        self._task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self) -> IreneConfig:
        """Return the current (most recently loaded) config snapshot."""
        with self._lock:
            return self._config

    async def start(self) -> None:
        """Begin background reload loop."""
        self._task = asyncio.create_task(self._reload_loop())

    async def stop(self) -> None:
        """Cancel background reload loop."""
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _load(self) -> IreneConfig:
        """Read, parse, and validate the YAML file. Returns IreneConfig."""
        try:
            with open(self._path, "r") as f:
                raw = yaml.safe_load(f) or {}
        except FileNotFoundError:
            logger.warning("Config file not found at %s — using defaults", self._path)
            raw = {}
        except yaml.YAMLError as exc:
            logger.error("YAML parse error in %s: %s", self._path, exc)
            raise

        raw = _apply_env_overrides(raw)
        return IreneConfig.model_validate(raw)

    async def _reload_loop(self) -> None:
        while True:
            await asyncio.sleep(self._reload_interval)
            try:
                new_config = await asyncio.get_event_loop().run_in_executor(
                    None, self._load
                )
                with self._lock:
                    self._config = new_config
                logger.debug("Config reloaded from %s", self._path)
            except Exception as exc:
                logger.warning(
                    "Config reload failed (%s) — keeping previous config", exc
                )

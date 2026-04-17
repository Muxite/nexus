"""
Seulgi configuration loader.

Reads config.yaml and merges in env var overrides (SEULGI__SECTION__KEY format).

All config values are plain Python dataclasses — no validators, no magic.
Access any value directly: cfg.trading.balance_usdc

Example env override:
    SEULGI__TRADING__BALANCE_USDC=500  →  cfg.trading.balance_usdc = 500.0
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Optional

import yaml

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class WatchedSite:
    """One news site to watch."""
    url: str
    change_wait_seconds: int = 30   # poll interval for DOM changes
    reload_seconds: int = 300       # reload page if silent for this long
    max_failures: int = 5           # consecutive failures before giving up


@dataclass
class MarketsConfig:
    refresh_hours: float = 4.0        # re-fetch market list every N hours
    similarity_threshold: float = 0.35  # min cosine similarity to match
    top_k: int = 15                   # max markets per headline


@dataclass
class AIConfig:
    model: str = "google/gemini-2.5-flash-lite-preview-06-17"
    api_key: str = ""                  # blank = use OPENROUTER_API_KEY env var
    api_base: str = "https://openrouter.ai/api/v1"
    min_confidence: float = 0.45


@dataclass
class TradingConfig:
    balance_usdc: float = 1000.0
    max_position_usdc: float = 100.0  # max USDC per trade
    min_trade_usdc: float = 5.0       # minimum trade size — below this, skip
    min_edge: float = 0.04            # minimum computed edge to execute
    edge_increment: float = 0.12      # max edge boost at confidence=1.0
    position_timeout_hours: float = 24.0
    max_open_positions: int = 20      # hard cap on concurrent positions
    mode: str = "paper"


@dataclass
class DBConfig:
    path: str = "/data/seulgi.db"


@dataclass
class DashboardConfig:
    data_dir: str = "/data/dashboard"   # directory where all .txt files are written
    interval_secs: float = 30.0         # how often to rewrite files


@dataclass
class EngineConfig:
    """Per-engine trading rule overrides. Unset fields fall back to global trading config."""
    name: str = "default"
    balance_usdc: Optional[float] = None
    max_position_usdc: Optional[float] = None
    min_trade_usdc: Optional[float] = None
    min_edge: Optional[float] = None
    edge_increment: Optional[float] = None
    position_timeout_hours: Optional[float] = None
    max_open_positions: Optional[int] = None


@dataclass
class Config:
    watched_sites: list[WatchedSite] = field(default_factory=list)
    markets: MarketsConfig = field(default_factory=MarketsConfig)
    ai: AIConfig = field(default_factory=AIConfig)
    trading: TradingConfig = field(default_factory=TradingConfig)
    engines: list[EngineConfig] = field(default_factory=list)
    db: DBConfig = field(default_factory=DBConfig)
    dashboard: DashboardConfig = field(default_factory=DashboardConfig)
    log_level: str = "INFO"


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


def load_config(path: Optional[str] = None) -> Config:
    """
    Load config from a YAML file, then apply env var overrides.

    Path defaults to SEULGI_CONFIG env var or 'config.yaml'.
    """
    if path is None:
        path = os.getenv("SEULGI_CONFIG", "config.yaml")

    raw: dict = {}
    if os.path.exists(path):
        with open(path) as f:
            raw = yaml.safe_load(f) or {}
        logger.info("Loaded config from %s", path)
    else:
        logger.warning("Config file not found at %s — using defaults", path)

    cfg = _build_config(raw)
    _apply_env_overrides(cfg)
    return cfg


def _build_config(raw: dict) -> Config:
    sites = [
        WatchedSite(
            url=s["url"],
            change_wait_seconds=int(s.get("change_wait_seconds", 30)),
            reload_seconds=int(s.get("reload_seconds", 300)),
            max_failures=int(s.get("max_failures", 5)),
        )
        for s in raw.get("watched_sites", [])
    ]

    m = raw.get("markets", {})
    a = raw.get("ai", {})
    t = raw.get("trading", {})
    d = raw.get("db", {})
    dash = raw.get("dashboard", {})

    engine_cfgs = [
        EngineConfig(
            name=str(e.get("name", f"engine_{i}")),
            balance_usdc=_opt_float(e.get("balance_usdc")),
            max_position_usdc=_opt_float(e.get("max_position_usdc")),
            min_trade_usdc=_opt_float(e.get("min_trade_usdc")),
            min_edge=_opt_float(e.get("min_edge")),
            edge_increment=_opt_float(e.get("edge_increment")),
            position_timeout_hours=_opt_float(e.get("position_timeout_hours")),
            max_open_positions=int(e["max_open_positions"]) if "max_open_positions" in e else None,
        )
        for i, e in enumerate(raw.get("engines", []))
    ]

    return Config(
        watched_sites=sites,
        engines=engine_cfgs,
        markets=MarketsConfig(
            refresh_hours=float(m.get("refresh_hours", 4.0)),
            similarity_threshold=float(m.get("similarity_threshold", 0.35)),
            top_k=int(m.get("top_k", 15)),
        ),
        ai=AIConfig(
            model=str(a.get("model", "google/gemini-2.5-flash")),
            api_key=str(a.get("api_key", "")),
            api_base=str(a.get("api_base", "https://openrouter.ai/api/v1")),
            min_confidence=float(a.get("min_confidence", 0.45)),
        ),
        trading=TradingConfig(
            balance_usdc=float(t.get("balance_usdc", 1000.0)),
            max_position_usdc=float(t.get("max_position_usdc", 100.0)),
            min_trade_usdc=float(t.get("min_trade_usdc", 5.0)),
            min_edge=float(t.get("min_edge", 0.04)),
            edge_increment=float(t.get("edge_increment", 0.12)),
            position_timeout_hours=float(t.get("position_timeout_hours", 24.0)),
            max_open_positions=int(t.get("max_open_positions", 20)),
            mode=str(t.get("mode", "paper")),
        ),
        db=DBConfig(path=str(d.get("path", "/data/seulgi.db"))),
        dashboard=DashboardConfig(
            data_dir=str(dash.get("data_dir", "/data/dashboard")),
            interval_secs=float(dash.get("interval_secs", 30.0)),
        ),
        log_level=str(raw.get("log_level", "INFO")),
    )


def _apply_env_overrides(cfg: Config) -> None:
    """
    Override config values from env vars.

    Format: SEULGI__SECTION__KEY=value
    Examples:
        SEULGI__TRADING__BALANCE_USDC=500
        SEULGI__AI__API_KEY=mykey
        SEULGI__MARKETS__TOP_K=20
    """
    prefix = "SEULGI__"
    for key, val in os.environ.items():
        if not key.startswith(prefix):
            continue
        parts = key[len(prefix):].lower().split("__")
        if len(parts) != 2:
            continue
        section, attr = parts
        section_obj = {
            "markets": cfg.markets,
            "ai": cfg.ai,
            "trading": cfg.trading,
            "db": cfg.db,
            "dashboard": cfg.dashboard,
        }.get(section)
        if section_obj is None:
            continue
        if not hasattr(section_obj, attr):
            continue
        current = getattr(section_obj, attr)
        try:
            if isinstance(current, float):
                setattr(section_obj, attr, float(val))
            elif isinstance(current, int):
                setattr(section_obj, attr, int(val))
            else:
                setattr(section_obj, attr, val)
            logger.debug("Env override: %s.%s = %r", section, attr, val)
        except (ValueError, TypeError) as exc:
            logger.warning("Bad env override %s=%s: %s", key, val, exc)

    # OPENROUTER_API_KEY (preferred) or legacy GEMINI_API_KEY
    if not cfg.ai.api_key:
        cfg.ai.api_key = os.getenv("OPENROUTER_API_KEY") or os.getenv("GEMINI_API_KEY", "")

    # Special: LOG_LEVEL env var
    if log_level := os.getenv("LOG_LEVEL"):
        cfg.log_level = log_level


def _opt_float(v) -> Optional[float]:
    return float(v) if v is not None else None

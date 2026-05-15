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
class FeedSite:
    """An RSS/Atom feed to subscribe to instead of polling HTML.

    Cycle 8: for the 26 outlets identified in cycle 4's preflight as
    feed-capable, a feed fetch costs ~50ms vs ~3–10s for a Playwright render.
    `site_url` is what the trader records on each headline (display + source
    bucket); `feed_url` is the actual RSS endpoint.
    """
    site_url: str
    feed_url: str
    interval_seconds: int = 60
    max_failures: int = 5


@dataclass
class SocialAccount:
    """A Twitter/X or Truth Social account to monitor for market-moving posts."""
    username: str
    platform: str = "twitter"       # "twitter" or "truth_social"
    interval_seconds: int = 90      # poll interval
    max_failures: int = 5


@dataclass
class MarketsConfig:
    refresh_hours: float = 4.0          # re-fetch market list every N hours
    similarity_threshold: float = 0.35  # min cosine similarity to match
    top_k: int = 15                     # max markets per headline
    max_tracked_markets: int = 300      # cap on markets kept in index (top N by volume)


@dataclass
class AIConfig:
    model: str = "google/gemini-2.5-flash-lite"
    premium_model: str = ""            # if set, used for high-similarity batches
    premium_sim_threshold: float = 0.70  # similarity ≥ this → use premium_model
    api_key: str = ""                  # blank = use OPENROUTER_API_KEY env var
    api_base: str = "https://openrouter.ai/api/v1"
    min_confidence: float = 0.45
    # Ensemble sampling: run N parallel calls per batch at higher temperature
    # and aggregate by majority vote + averaged distribution. Costs N× tokens
    # but produces an `agreement` signal the trader can gate on, and denoises
    # the single-shot model's anti-informative high-conf calls. N=1 disables.
    ensemble_samples: int = 3
    ensemble_temperature: float = 0.5
    # Minimum ensemble agreement fraction to fire a trade. With samples=3 a
    # value of 0.66 demands at least 2-of-3 votes for the chosen direction;
    # 1.0 demands unanimity. Decoupled from `confidence` so the edge model
    # keeps using the model's native probability output.
    min_agreement: float = 0.66


@dataclass
class TradingConfig:
    balance_usdc: float = 1000.0
    max_position_usdc: float = 100.0  # max USDC per trade
    min_trade_usdc: float = 5.0       # minimum trade size — below this, skip
    min_edge: float = 0.04            # minimum computed edge to execute
    edge_increment: float = 0.12      # max edge boost at confidence=1.0
    position_timeout_hours: float = 24.0
    max_open_positions: int = 20      # hard cap on concurrent positions
    max_unique_markets: int = 15      # max distinct condition_ids with open positions
    cold_position_hours: float = 12.0 # hours since last match before a market is "cold"
    # Technical exit policy — closes positions on small price moves so capital
    # recycles fast instead of sitting in 24h timeouts. All four units are in
    # *probability points* (0.005 = half a cent on a 0..1 token). Setting any
    # of these to 0 disables that exit path. The legacy WIN/LOSS price cutoffs
    # (0.95 / 0.05) stay active as backstops.
    take_profit: float = 0.0          # close once (current - entry) * dir_sign >= take_profit
    stop_loss: float = 0.0            # close once (current - entry) * dir_sign <= -stop_loss
    trailing_stop: float = 0.0        # after the position has been in profit, close once
                                      # MTM drops by this much from the high-water mark
    min_hold_seconds: float = 0.0     # don't apply the three above before this age
    infinite_balance: bool = False    # if True, never block trades on balance — keep
                                      # buying until max_open_positions /
                                      # max_unique_markets / cold-cull / timeout / WIN /
                                      # LOSS handles exit. Position caps still apply.
    mode: str = "paper"
    # Rail-skip: refuse trades where the entry-side mid price is within
    # `rail_skip_threshold` of 0 or 1 (i.e. p ∈ [thr, 1-thr] is allowed).
    # Saturated markets can't move meaningfully — 99% no_signal at p≥0.95 in
    # the 6h run. Set to 0 to disable. Per-engine override available.
    rail_skip_threshold: float = 0.0
    # Hard rail floor that applies to EVERY engine (no opt-out via per-engine
    # override of `rail_skip_threshold` reaches here). At mid ≥ 1-thr or
    # ≤ thr the bid-ask spread alone routinely exceeds tight stop-losses
    # (e.g. aggressive's SL=0.015), so SL fires on spread movement before any
    # signal arrives. Default 0.02 = refuse anything inside [0, 0.02] ∪
    # [0.98, 1]. Set to 0 to disable. Per-engine override IS respected so
    # this is still a knob, but the default protects aggressive too.
    entry_rail_skip_threshold: float = 0.02
    # Invert direction when AI is highly confident in a non-rail market.
    # Empirically conf ≥ 0.85 is anti-informative (IC < 0). Off by default;
    # opt-in per engine as a shadow strategy.
    invert_high_conf: bool = False
    invert_conf_threshold: float = 0.85
    # Round-trip trading cost in basis points of stake (1 bp = 0.01%). Default
    # 20 bps = 0.20% of stake deducted at close. Models the bid-ask spread
    # (Polymarket has no protocol fees, but you cross the spread on entry AND
    # exit — a flat buy + immediate sell loses you ~the spread). Without this
    # a buy-then-sell at the same mid yields zero PnL, which understates real
    # costs and lets the bot accumulate tiny "edges" that don't exist net.
    # Per-engine override allowed.
    spread_cost_bps: float = 20.0


@dataclass
class NewsMemoryConfig:
    path: str = "/data/news_chroma"           # ChromaDB persistent directory
    dedup_distance_threshold: float = 0.12    # cosine distance below this = republish


@dataclass
class DBConfig:
    path: str = "/data/seulgi.db"


@dataclass
class DashboardConfig:
    data_dir: str = "/data/dashboard"   # directory where all .txt files are written
    interval_secs: float = 30.0         # how often to rewrite files


@dataclass
class PlotterConfig:
    """Periodic matplotlib renderer of quant stats."""
    interval_secs: float = 600.0   # 10 minutes


@dataclass
class SearchBudgetConfig:
    """Caps on expensive external API calls."""
    brave_per_hour: int = 20   # max Brave Search API calls per hour across all Twitter watchers


@dataclass
class VariantConfig:
    """Shadow-strategy overrides on top of a parent engine.

    Variants share the parent engine's AI classification call (one batch per
    headline serves all variants) but evaluate their own buy/sell logic and
    write trades tagged with the variant's name. Use them to A/B different
    thresholds, exits, or sizing cheaply.

    Unset fields fall back to the parent EngineConfig's value (and from there
    to the global TradingConfig).
    """
    name: str = "variant"
    max_position_usdc: Optional[float] = None
    min_trade_usdc: Optional[float] = None
    min_edge: Optional[float] = None
    edge_increment: Optional[float] = None
    position_timeout_hours: Optional[float] = None
    max_open_positions: Optional[int] = None
    min_confidence: Optional[float] = None
    take_profit: Optional[float] = None
    stop_loss: Optional[float] = None
    trailing_stop: Optional[float] = None
    min_hold_seconds: Optional[float] = None
    rail_skip_threshold: Optional[float] = None
    entry_rail_skip_threshold: Optional[float] = None
    invert_high_conf: Optional[bool] = None
    invert_conf_threshold: Optional[float] = None
    spread_cost_bps: Optional[float] = None


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
    min_confidence: Optional[float] = None   # per-engine override for ai.min_confidence
    take_profit: Optional[float] = None
    stop_loss: Optional[float] = None
    trailing_stop: Optional[float] = None
    min_hold_seconds: Optional[float] = None
    rail_skip_threshold: Optional[float] = None
    entry_rail_skip_threshold: Optional[float] = None
    invert_high_conf: Optional[bool] = None
    invert_conf_threshold: Optional[float] = None
    spread_cost_bps: Optional[float] = None
    # Optional shadow-strategy variants. When non-empty the engine spawns one
    # PaperTrader per variant; all variants share the engine's batched AI
    # call but record trades under their own variant_name. When empty the
    # engine behaves as a single variant tagged with engine.name.
    variants: list = field(default_factory=list)   # list[VariantConfig]


@dataclass
class Config:
    watched_sites: list[WatchedSite] = field(default_factory=list)
    feeds: list[FeedSite] = field(default_factory=list)
    social_accounts: list[SocialAccount] = field(default_factory=list)
    markets: MarketsConfig = field(default_factory=MarketsConfig)
    ai: AIConfig = field(default_factory=AIConfig)
    trading: TradingConfig = field(default_factory=TradingConfig)
    engines: list[EngineConfig] = field(default_factory=list)
    db: DBConfig = field(default_factory=DBConfig)
    dashboard: DashboardConfig = field(default_factory=DashboardConfig)
    news_memory: NewsMemoryConfig = field(default_factory=NewsMemoryConfig)
    search_budget: SearchBudgetConfig = field(default_factory=SearchBudgetConfig)
    plotter: PlotterConfig = field(default_factory=PlotterConfig)
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
    feeds = [
        FeedSite(
            site_url=str(f.get("site_url") or f["url"]),
            feed_url=str(f["feed_url"]),
            interval_seconds=int(f.get("interval_seconds", 60)),
            max_failures=int(f.get("max_failures", 5)),
        )
        for f in raw.get("feeds", [])
    ]

    m = raw.get("markets", {})
    a = raw.get("ai", {})
    t = raw.get("trading", {})
    d = raw.get("db", {})
    dash = raw.get("dashboard", {})
    nm = raw.get("news_memory", {})
    sb = raw.get("search_budget", {})

    social_cfgs = [
        SocialAccount(
            username=str(s.get("username", "")),
            platform=str(s.get("platform", "twitter")),
            interval_seconds=int(s.get("interval_seconds", 90)),
            max_failures=int(s.get("max_failures", 5)),
        )
        for s in raw.get("social_accounts", [])
        if s.get("username")
    ]

    def _build_variant(i: int, v: dict) -> VariantConfig:
        return VariantConfig(
            name=str(v.get("name", f"variant_{i}")),
            max_position_usdc=_opt_float(v.get("max_position_usdc")),
            min_trade_usdc=_opt_float(v.get("min_trade_usdc")),
            min_edge=_opt_float(v.get("min_edge")),
            edge_increment=_opt_float(v.get("edge_increment")),
            position_timeout_hours=_opt_float(v.get("position_timeout_hours")),
            max_open_positions=int(v["max_open_positions"]) if "max_open_positions" in v else None,
            min_confidence=_opt_float(v.get("min_confidence")),
            take_profit=_opt_float(v.get("take_profit")),
            stop_loss=_opt_float(v.get("stop_loss")),
            trailing_stop=_opt_float(v.get("trailing_stop")),
            min_hold_seconds=_opt_float(v.get("min_hold_seconds")),
            rail_skip_threshold=_opt_float(v.get("rail_skip_threshold")),
            entry_rail_skip_threshold=_opt_float(v.get("entry_rail_skip_threshold")),
            invert_high_conf=v["invert_high_conf"] if "invert_high_conf" in v else None,
            invert_conf_threshold=_opt_float(v.get("invert_conf_threshold")),
            spread_cost_bps=_opt_float(v.get("spread_cost_bps")),
        )

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
            min_confidence=_opt_float(e.get("min_confidence")),
            take_profit=_opt_float(e.get("take_profit")),
            stop_loss=_opt_float(e.get("stop_loss")),
            trailing_stop=_opt_float(e.get("trailing_stop")),
            min_hold_seconds=_opt_float(e.get("min_hold_seconds")),
            rail_skip_threshold=_opt_float(e.get("rail_skip_threshold")),
            entry_rail_skip_threshold=_opt_float(e.get("entry_rail_skip_threshold")),
            invert_high_conf=e["invert_high_conf"] if "invert_high_conf" in e else None,
            invert_conf_threshold=_opt_float(e.get("invert_conf_threshold")),
            variants=[_build_variant(j, v) for j, v in enumerate(e.get("variants", []) or [])],
        )
        for i, e in enumerate(raw.get("engines", []))
    ]

    return Config(
        watched_sites=sites,
        feeds=feeds,
        social_accounts=social_cfgs,
        engines=engine_cfgs,
        markets=MarketsConfig(
            refresh_hours=float(m.get("refresh_hours", 4.0)),
            similarity_threshold=float(m.get("similarity_threshold", 0.35)),
            top_k=int(m.get("top_k", 15)),
            max_tracked_markets=int(m.get("max_tracked_markets", 300)),
        ),
        news_memory=NewsMemoryConfig(
            path=str(nm.get("path", "/data/news_chroma")),
            dedup_distance_threshold=float(nm.get("dedup_distance_threshold", 0.12)),
        ),
        ai=AIConfig(
            model=str(a.get("model", "google/gemini-2.5-flash-lite")),
            premium_model=str(a.get("premium_model", "")),
            premium_sim_threshold=float(a.get("premium_sim_threshold", 0.70)),
            api_key=str(a.get("api_key", "")),
            api_base=str(a.get("api_base", "https://openrouter.ai/api/v1")),
            min_confidence=float(a.get("min_confidence", 0.45)),
            ensemble_samples=int(a.get("ensemble_samples", 3)),
            ensemble_temperature=float(a.get("ensemble_temperature", 0.5)),
            min_agreement=float(a.get("min_agreement", 0.66)),
        ),
        trading=TradingConfig(
            balance_usdc=float(t.get("balance_usdc", 1000.0)),
            max_position_usdc=float(t.get("max_position_usdc", 100.0)),
            min_trade_usdc=float(t.get("min_trade_usdc", 5.0)),
            min_edge=float(t.get("min_edge", 0.04)),
            edge_increment=float(t.get("edge_increment", 0.12)),
            position_timeout_hours=float(t.get("position_timeout_hours", 24.0)),
            max_open_positions=int(t.get("max_open_positions", 20)),
            max_unique_markets=int(t.get("max_unique_markets", 15)),
            cold_position_hours=float(t.get("cold_position_hours", 12.0)),
            take_profit=float(t.get("take_profit", 0.0)),
            stop_loss=float(t.get("stop_loss", 0.0)),
            trailing_stop=float(t.get("trailing_stop", 0.0)),
            min_hold_seconds=float(t.get("min_hold_seconds", 0.0)),
            infinite_balance=bool(t.get("infinite_balance", False)),
            mode=str(t.get("mode", "paper")),
            rail_skip_threshold=float(t.get("rail_skip_threshold", 0.0)),
            entry_rail_skip_threshold=float(t.get("entry_rail_skip_threshold", 0.02)),
            invert_high_conf=bool(t.get("invert_high_conf", False)),
            invert_conf_threshold=float(t.get("invert_conf_threshold", 0.85)),
        ),
        db=DBConfig(path=str(d.get("path", "/data/seulgi.db"))),
        dashboard=DashboardConfig(
            data_dir=str(dash.get("data_dir", "/data/dashboard")),
            interval_secs=float(dash.get("interval_secs", 30.0)),
        ),
        search_budget=SearchBudgetConfig(
            brave_per_hour=int(sb.get("brave_per_hour", 20)),
        ),
        plotter=PlotterConfig(
            interval_secs=float(raw.get("plotter", {}).get("interval_secs", 600.0)),
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

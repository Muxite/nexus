"""
## Specification

**Purpose.** Load `config.yaml` once, validate it with pydantic, and re-read
it every `reload_interval` seconds so live tuning doesn't need a container
restart.

**Public interface.**
- `Config` — the validated root config model.
- `ConfigManager` — `.get()` returns the latest validated `Config`; `.start()`
  / `.stop()` run the background reload loop.
- `load_config(path)` — one-shot synchronous load (used by tests + the boot
  smoke path before the asyncio loop exists).

**Invariants.**
- Every section has typed defaults; an empty / missing YAML is valid.
- `IRENE__SECTION__KEY=value` env-var overrides win over YAML.
- `Config` is immutable; reload replaces the manager's reference atomically.

**Side effects.** Reads `config.yaml`; reads `os.environ` for overrides.

**Failure modes.** A YAML parse error on RELOAD logs a warning and KEEPS the
previous config (we never let a typo crash a running pipeline). A YAML parse
error on FIRST load raises — boot is intentionally strict.

**Non-goals.** Schema migration, secret management, runtime mutation of
sections by code.

Pattern stolen from old-irene `irene_shared/config.py` at commit 122720a.
Simplified for irene v1: fewer sections, no live/paper mode flag (every
P1 trade is paper), no per-section validators beyond the pydantic-native
ones.
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

_DEFAULT_RELOAD_INTERVAL = 20  # seconds


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------


class IreneSection(BaseModel):
    """Top-level lifecycle settings."""

    log_level: str = "INFO"
    # The cycle name written into every decision/prediction/trade row. main.py
    # calls db.ensure_current_cycle(name) on boot; the value comes from this
    # field unless overridden by IRENE_CYCLE in the env.
    cycle: str = "cycle-0"


class DBSection(BaseModel):
    """SQLite-only in P1. Postgres is drafted in P4."""

    path: str = "/data/irene.db"
    # The seulgi-path runtime assertion in db.py also checks this value at
    # connect time. There is no override.
    journal_mode: str = "WAL"
    synchronous: str = "NORMAL"


class ScraperSection(BaseModel):
    """The ONLY thing shared with seulgi — an HTTP service we both poll."""

    url: str = "http://scraper:9000"
    timeout_seconds: float = Field(default=8.0, gt=0)


class EmbedderSection(BaseModel):
    """Sentence-transformers model identifier stamped on every embedding."""

    # Matching seulgi's default keeps the model cache layer shareable.
    name: str = "sentence-transformers/all-MiniLM-L6-v2"
    dim: int = Field(default=384, gt=0)


class OpenRouterSection(BaseModel):
    api_key: str = ""
    direction_model: str = "google/gemini-2.5-flash"
    timeout_seconds: float = Field(default=20.0, gt=0)


class BudgetSection(BaseModel):
    """Wired in P1 — see master-plan §"Phase 1 — Detailed scope"."""

    monthly_cap_usd: float = Field(default=50.0, gt=0)
    # Pause when remaining cap drops below this threshold.
    pause_at_remaining_usd: float = Field(default=2.0, ge=0)
    poll_seconds: int = Field(default=60, ge=10)


class IngestSection(BaseModel):
    """News ingest poller — feed + HTML site URLs and cadence.
    See stages/ingest.py.

    Cycle 4: irene migrated from scraper:9000 to jennie-scraper-v2:9001.
    The /feed endpoint is shape-compatible; new in v2 is /scrape which
    extracts headline link-sets from HTML homepages (trafilatura
    boilerplate-stripped). `html_sites` drives the new HTML watcher
    loop. Empty list → loop is a no-op (RSS-only mode).
    """

    feeds: list[str] = Field(default_factory=list)
    poll_seconds: int = Field(default=60, ge=5)
    # HTML homepages polled via /scrape mode=links.
    html_sites: list[str] = Field(default_factory=list)
    html_poll_seconds: int = Field(default=300, ge=30)
    # Cycle 5: combined RSS+HTML homepages polled via /scrape mode=combined.
    # The scraper fetches the host's pinned RSS feed (from per_host.yaml)
    # and the HTML homepage in parallel and returns the merged link-set,
    # so a single source URL produces both RSS items (with summaries)
    # and HTML breaking links not yet in the feed. Empty list → loop is
    # a no-op; existing rss_feeds/html_sites loops keep working.
    combined_sources: list[str] = Field(default_factory=list)
    combined_poll_seconds: int = Field(default=180, ge=30)
    # When > 0, /scrape mode=combined fans out mode=article fetches for
    # the top-N HTML-only links to enrich them with body-derived summaries.
    # Costs an extra HTTP/render per link, in exchange for far better
    # LLM-side signal on bare-title HTML breaking news. Default off.
    combined_enrich_html_top_n: int = Field(default=0, ge=0, le=20)


class DedupeSection(BaseModel):
    """Dedupe stage — Chroma store path + freshness threshold. See stages/dedupe.py."""

    chroma_path: str = "/data/news_chroma"
    breaking_distance: float = Field(default=0.55, gt=0.0, le=2.0)


class MatchSection(BaseModel):
    """Match stage — cosine floor + top-k cap for article → market candidates.

    Tightened 2026-05-24 after 350 decisions of soak inspection showed
    the floor=0.35 + top_k=25 combo accepted obvious noise matches like
    "Hyderabad street-racing" → "F1 Antonelli champion" at sim 0.354
    and fanned 1 article to 15-25 LLM calls. floor=0.50 + top_k=8 keeps
    signal-to-noise high and cuts LLM cost ~3-4×.
    """

    similarity_floor: float = Field(default=0.50, gt=0.0, lt=1.0)
    top_k: int = Field(default=8, ge=1, le=100)


class ExtractSection(BaseModel):
    """Extraction (P2 shadow layer) — model + token budget per article."""

    model: str = "google/gemini-2.5-flash"
    timeout_seconds: float = Field(default=20.0, gt=0)
    max_tokens: int = Field(default=1024, ge=128, le=8192)
    # Hard upper bound on the article body characters passed to the LLM
    # (irene's ingested summaries are short, but truncating defensively
    # keeps a runaway long-form article from blowing up the prompt).
    max_article_chars: int = Field(default=4000, ge=200)


class CLOBSection(BaseModel):
    """Polymarket CLOB client config — base URL + timeout."""

    base_url: str = "https://clob.polymarket.com"
    timeout_seconds: float = Field(default=5.0, gt=0)


class CloserSection(BaseModel):
    """Position-closer poll cadence + close rules.

    Rule eval order (cycle 3): edge_decay → take_profit → stop_loss →
    stagnation → timeout. First match wins. edge_decay uses the
    model's target_mid (written by trade.py at execute time); the
    others are absolute thresholds.
    """

    poll_seconds: int = Field(default=15, ge=1)
    # Catchall. Cycle 3 dropped from 24h to 1.5h — news-shock
    # arbitrage windows close in minutes; 90 min is the dead-money cutoff.
    timeout_hours: float = Field(default=1.5, ge=0.0)
    # When CLOB can't return a mid for an open trade, hold the position
    # rather than close it at the default; closer just skips this cycle.
    skip_on_missing_mid: bool = True
    # take_profit / stop_loss are fractions of the trade's `size_usdc`.
    # 0.0 disables the rule. edge_decay (target_mid hit) covers normal
    # exits; these are the safety net for blown predictions.
    take_profit_pct: float = Field(default=0.50, ge=0.0, le=10.0)
    stop_loss_pct: float = Field(default=0.30, ge=0.0, le=1.0)
    # Cycle 3: stagnation rule. If position has aged past
    # stagnation_minutes AND the mid hasn't moved by more than
    # stagnation_drift, close — the catalyst either priced in instantly
    # or wasn't real; free the cash for the next signal.
    stagnation_minutes: int = Field(default=30, ge=1)
    stagnation_drift: float = Field(default=0.02, ge=0.0, le=1.0)


class ImpactSection(BaseModel):
    """Impact-tracker poll cadence."""

    poll_seconds: int = Field(default=15, ge=5)
    # Maximum age (hours) we keep polling a price_impacts row. After this
    # we stop polling even if some buckets are NULL — the t=3600s bucket
    # is the last interesting one anyway.
    max_age_hours: float = Field(default=2.0, gt=0)


class DKGTradeSection(BaseModel):
    """DKG trade stage — fires paper trades off `market_links.final_score`.

    P3 chunk 3 adds a second decision_path ('dkg') alongside the MVP
    matcher → trade path. Sizing reuses TradeSection (Kelly, balance,
    min_position_usdc) so the two paths stay in lockstep unless someone
    explicitly drifts them apart.
    """

    enabled: bool = True
    score_floor: float = Field(default=0.8, ge=0.0, le=1.0)
    # When True, reuse TradeSection's edge/size gates verbatim. Flip to
    # False only if you want a DKG-specific gate set (none in P3 ch3).
    inherit_trade_gates: bool = True


class TradeSection(BaseModel):
    """Trade stage — gate cascade thresholds + sizing config (paper only)."""

    balance_usdc: float = Field(default=1000.0, gt=0)
    min_similarity: float = Field(default=0.35, gt=0.0, lt=1.0)
    min_confidence: float = Field(default=0.55, gt=0.0, le=1.0)
    min_edge: float = Field(default=0.04, gt=0.0, lt=1.0)
    fee_buffer: float = Field(default=0.02, ge=0.0, lt=1.0)
    # Cycle 4: reject entries when CLOB mid is in degenerate range — the
    # market is essentially resolved and any "edge" the model thinks it
    # sees is mostly fees + spread. Threshold 0.02/0.98 lets 0.946-type
    # trades through (they can still profit) but blocks 0.01/0.99 extremes.
    min_mid: float = Field(default=0.02, ge=0.0, lt=0.5)
    max_mid: float = Field(default=0.98, gt=0.5, le=1.0)
    kelly_fraction: float = Field(default=0.25, gt=0.0, le=1.0)
    max_position_fraction: float = Field(default=0.20, gt=0.0, le=1.0)
    min_position_usdc: float = Field(default=1.0, gt=0)
    # P1 stub: no CLOB integration yet. Trade.py reads markets.last_mid_price
    # if populated; otherwise falls back to this value. Replace with real
    # mid-price fetching in a follow-up.
    default_mid_price: float = Field(default=0.5, gt=0.0, lt=1.0)
    # Freshness labels that the freshness gate accepts.
    accept_freshness: list[str] = Field(default_factory=lambda: ["BREAKING", "EVOLVING"])


# ---------------------------------------------------------------------------
# Root
# ---------------------------------------------------------------------------


class Config(BaseModel):
    irene: IreneSection = Field(default_factory=IreneSection)
    db: DBSection = Field(default_factory=DBSection)
    scraper: ScraperSection = Field(default_factory=ScraperSection)
    embedder: EmbedderSection = Field(default_factory=EmbedderSection)
    openrouter: OpenRouterSection = Field(default_factory=OpenRouterSection)
    budget: BudgetSection = Field(default_factory=BudgetSection)
    ingest: IngestSection = Field(default_factory=IngestSection)
    dedupe: DedupeSection = Field(default_factory=DedupeSection)
    match: MatchSection = Field(default_factory=MatchSection)
    trade: TradeSection = Field(default_factory=TradeSection)
    dkg_trade: DKGTradeSection = Field(default_factory=DKGTradeSection)
    clob: CLOBSection = Field(default_factory=CLOBSection)
    closer: CloserSection = Field(default_factory=CloserSection)
    extract: ExtractSection = Field(default_factory=ExtractSection)
    impact: ImpactSection = Field(default_factory=ImpactSection)

    model_config = {"frozen": True}


# ---------------------------------------------------------------------------
# Env-var overlay (IRENE__SECTION__KEY=value)
# ---------------------------------------------------------------------------


def _coerce(value: str) -> object:
    lo = value.lower()
    if lo in ("true", "yes", "1"):
        return True
    if lo in ("false", "no", "0"):
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


def _apply_env_overrides(raw: dict) -> dict:
    for env_key, env_val in os.environ.items():
        if not env_key.startswith("IRENE__"):
            continue
        parts = env_key.split("__")
        if len(parts) != 3:
            continue
        _, section, key = parts
        raw.setdefault(section.lower(), {})[key.lower()] = _coerce(env_val)
    # IRENE_CYCLE is a bare env var (not nested) because cycle name changes
    # per `docker compose run`. Map it onto irene.cycle if set.
    cycle = os.environ.get("IRENE_CYCLE")
    if cycle:
        raw.setdefault("irene", {})["cycle"] = cycle
    return raw


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


def load_config(path: str | Path) -> Config:
    """Read + validate the YAML. Used by the boot path and by tests."""
    p = Path(path)
    try:
        raw = yaml.safe_load(p.read_text()) or {}
    except FileNotFoundError:
        logger.warning("Config file not found at %s — using defaults.", p)
        raw = {}
    raw = _apply_env_overrides(raw)
    return Config.model_validate(raw)


class ConfigManager:
    """Hot-reloads `config.yaml` every `reload_interval` seconds."""

    def __init__(
        self,
        path: str | Path,
        reload_interval: int = _DEFAULT_RELOAD_INTERVAL,
    ) -> None:
        self._path = Path(path)
        self._reload_interval = reload_interval
        self._lock = threading.Lock()
        self._config = load_config(self._path)
        self._task: Optional[asyncio.Task] = None

    def get(self) -> Config:
        with self._lock:
            return self._config

    async def start(self) -> None:
        self._task = asyncio.create_task(self._reload_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _reload_loop(self) -> None:
        while True:
            await asyncio.sleep(self._reload_interval)
            try:
                new_config = load_config(self._path)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Config reload failed (%s) — keeping previous config.", exc
                )
                continue
            with self._lock:
                self._config = new_config
            logger.debug("Config reloaded from %s", self._path)

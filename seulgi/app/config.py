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
    # Stage-3 (prediction) model. Default upgraded from flash-lite → flash:
    # the relevance gate now culls irrelevant markets before this call, so
    # the model can spend its reasoning budget on the survivors.
    model: str = "google/gemini-2.5-flash"
    # Cross-vendor premium fallback. Triggered when relevance scores ≥2
    # markets STRONG_YES AND freshness=BREAKING — only the strongest signals
    # justify the cost step-up.
    premium_model: str = "anthropic/claude-haiku-4-5"
    premium_sim_threshold: float = 0.70   # legacy: similarity ≥ this → premium
    premium_trigger_breaking_strong: bool = True   # new gate (see relevance.py)
    api_key: str = ""                     # blank = use OPENROUTER_API_KEY env var
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
class RelevanceConfig:
    """Stage-2 cheap LLM gate between embedding match and prediction call.

    Asks YES/NO+reason per candidate market; culls to the set of matches the
    prediction model actually has a useful opinion on. On API/parse failure
    we fail-open (keep every match) so a relevance hiccup never hides trades.
    """
    enabled: bool = True
    model: str = "google/gemini-2.5-flash-lite"
    temperature: float = 0.1
    # Verdict scores that survive the gate. Keep STRONG_YES + YES; drop
    # WEAK + NO. Tweak per A/B run to widen/narrow the funnel.
    keep_scores: tuple = ("STRONG_YES", "YES")
    # Maximum tokens reserved per market in the response. The prompt is
    # batched; total budget = max_tokens × n_markets + ~150.
    max_tokens_per_market: int = 60
    # Cycle-13 token reduction: if no candidate market has cosine similarity
    # ≥ this threshold, the runner skips Stage 2 + Stage 3 entirely and
    # writes per-variant `low_similarity:<max_sim>` skips. The embedding
    # index already gates at 0.35; this is a stricter floor so we don't
    # pay LLM tokens to be told "the headline isn't about any of these."
    # 0.0 disables (legacy behaviour).
    min_top_similarity: float = 0.45


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
    # Price-relative stop-loss floor (cycle-10). Effective SL becomes
    #   max(stop_loss, stop_loss_pct * entry_price)
    # so mid-priced (~0.50) entries get a wider absolute SL than 0.015,
    # surviving normal spread movement. Low-price entries keep the existing
    # fixed `stop_loss` floor. 0.0 disables (legacy behaviour).
    stop_loss_pct: float = 0.0
    # Asymmetric entry gate. Refuses entries when match.mid_yes < this
    # threshold, regardless of direction — gates the engine toward
    # markets that are already heavily priced (0.75+) where a NO bet is
    # the high-probability play. 0.0 disables. Per-engine override allowed.
    min_entry_yes_mid: float = 0.0
    # Volatility-aware entry gate (cycle 11). Refuses entries when the
    # pre-headline 30m stddev of mid exceeds this. Lets a variant opt
    # out of markets that bounce wider than the SL can survive.
    # 0.0 disables (default). Per-engine override allowed.
    max_market_vol_30m: float = 0.0
    # Invert direction when AI is highly confident in a non-rail market.
    # Empirically conf ≥ 0.85 is anti-informative (IC < 0). Off by default;
    # opt-in per engine as a shadow strategy.
    invert_high_conf: bool = False
    invert_conf_threshold: float = 0.85
    # M4 — variant-level pre-trade filters. Used by the evolutionary engine
    # expansion to test narrow strategy slices in isolation. All four default
    # to "no filter"; per-engine override is the intended use.
    #
    # direction_filter: only fire when result.direction matches ("YES" / "NO").
    #   Historical lifetime: YES is +EV (hit 50.7%); NO loses hard (hit 19.8%).
    # freshness_filter: only fire when headline._freshness_label matches
    #   ("EVOLVING" / "BREAKING"). Historical: EVOLVING 46.2% hit vs BREAKING
    #   13.6%.
    # mid_yes_min / mid_yes_max: variant-level tightening of the global E.3
    #   mid-band ([0.65, 0.85]). Historical: mid_yes ∈ [0.75, 0.85] hit 60%.
    direction_filter: str = ""
    freshness_filter: str = ""
    mid_yes_min: float = 0.0
    mid_yes_max: float = 1.0
    # A1.2 (2026-05-22): explicit p_alpha floor — gates the trade on the AI's
    # estimated P(this is +EV) signal, distinct from `min_confidence` (which
    # historically gated on `ai.ClassifyResult.confidence`, a field that's
    # ambiguously `p_alpha` OR `mean_p_winner` per ai.py:_aggregate_ensemble).
    # See trader.py spec block. 0.0 disables (default — backward-compat).
    min_p_alpha: float = 0.0
    # A1.2 (2026-05-22, master plan 4B.2): if > 0, additionally gate the
    # entry on the True-EV-per-$1 measure from simulate.compute_ev_{yes,no}
    # plus a fee headroom estimate. 0.0 disables (the legacy `min_edge` gate
    # remains the primary filter). Per-engine override allowed.
    min_ev_per_dollar: float = 0.0
    # A4A.2 (2026-05-23): JIT pricing tolerance band (basis points,
    # relative to snapshot mid). The trader refetches top-of-book right
    # before `open_position`; if `|live − snapshot| / snapshot >
    # jit_tolerance_bps / 10000`, the IOC "fails" and we book a
    # `ioc_fail_price_moved` skip instead of executing on a stale price.
    # See app/execution.py. Per-engine override allowed.
    jit_tolerance_bps: float = 50.0
    # Round-trip trading cost in basis points of stake (1 bp = 0.01%). Default
    # 20 bps = 0.20% of stake deducted at close. Models the bid-ask spread
    # (Polymarket has no protocol fees, but you cross the spread on entry AND
    # exit — a flat buy + immediate sell loses you ~the spread). Without this
    # a buy-then-sell at the same mid yields zero PnL, which understates real
    # costs and lets the bot accumulate tiny "edges" that don't exist net.
    # Per-engine override allowed.
    spread_cost_bps: float = 20.0
    # ── Simulate mode (§1, §7) ────────────────────────────────────────
    # When `mode == "simulate"`, Seulgi pretends it has a tiny real
    # bankroll (simulate_total_usdc), splits it evenly across active
    # engines, models Polymarket fees + slippage + share quantization,
    # and pauses LLM calls when no engine has free capital to act on.
    # See docs in plan §1 and §7.
    simulate_total_usdc: float = 50.0
    simulate_min_trade_usdc: float = 1.0
    simulate_fees: "SimulateFeesConfig" = field(default_factory=lambda: SimulateFeesConfig())
    simulate_slippage: "SimulateSlippageConfig" = field(default_factory=lambda: SimulateSlippageConfig())
    simulate_sizing: "SimulateSizingConfig" = field(default_factory=lambda: SimulateSizingConfig())

    def is_simulate(self) -> bool:
        return self.mode == "simulate"


@dataclass
class SimulateFeesConfig:
    """Polymarket dynamic taker-fee model.

    Fee = max(min_fee_usdc, shares × rate × p × (1-p))
    rate is looked up by market_type (from markets.market_type column).

    Reconciled with the Polymarket reference manual (2026-05-19) — Phase E.7.2:
    - default_rate matches "General" tier (0.05), not Politics (0.04).
    - min_fee_usdc matches the safety floor.
    - Maker rebates (20% crypto / 25% other) are informational only; we model
      takers exclusively, so we don't claim them. Conservative for paper
      trading a strategy that doesn't yet earn rebates.
    - Gas (Polymarket runs on Polygon; meta-tx subsidises most web-app trades
      but EOA / direct contract trades pay <$0.01 in POL each leg). We add
      gas_cost_per_trade_usdc as a per-leg drag so paper PnL stays honest.
    - On-ramp / off-ramp processing fees (1-4.5%) are NOT modelled — we
      assume pre-existing USDC on Polygon (the cycle 10 micro-bankroll setup).
    - 0% settlement tax: enforced via trader.py setting exit_fee=0.0 on
      RESOLVE_WIN / RESOLVE_LOSS branches.
    """
    enabled: bool = True
    default_rate: float = 0.05        # "General / Economics / Culture / Weather" tier
    category_rates: dict = field(default_factory=lambda: {
        "crypto":      0.072,
        "crypto_price": 0.072,
        "economics":   0.05,
        "weather":     0.05,
        "sports":      0.03,
        "geopolitics": 0.00,
        "political":   0.04,
        "politics":    0.04,
        "finance":     0.04,
        "tech":        0.04,
        "event":       0.04,        # default fallback — "Mentions" tier (0.04)
        "entertainment": 0.05,       # "Culture" tier
        "price_threshold": 0.04,
    })
    min_fee_usdc: float = 0.00001
    # Per-leg gas cost in USDC. Spec: "<$0.01 in POL" per on-chain interaction.
    # 0.005 = conservative midpoint; applied additively to entry_fee and
    # exit_fee. Set to 0.0 if meta-tx subsidy keeps the operator gas-free.
    gas_cost_per_trade_usdc: float = 0.005
    # Informational only — we always take. Recorded in audit reports for
    # operators considering a maker strategy.
    maker_rebate_crypto: float = 0.20
    maker_rebate_other:  float = 0.25


@dataclass
class SimulateSlippageConfig:
    """Deterministic local approximation of order-book walk."""
    enabled: bool = True
    bps_per_dollar: float = 2.0       # entry walks 2 bps for every $1 notional
    max_price_move: float = 0.02      # cap so micro-cap markets stay sane
    # A1.1 (2026-05-22): lowered 5.0 → 0.50. The old threshold disabled
    # slippage for 100/100 sampled trades (size_usdc < $5 was the norm
    # under fractional kelly + super_scout micro-stakes), so the fidelity
    # layer was a NO-OP in production. 0.50 keeps super_scout's $0.05
    # micro-stakes skipping (walk < 0.0001, below tick) but lets ≥$0.50
    # trades finally see the bps drag they were supposed to.
    disable_below_usdc: float = 0.50
    disable_for_categories: list = field(default_factory=lambda: ["geopolitics"])


@dataclass
class SimulateSizingConfig:
    """Kelly-based stake sizing for simulate mode.

    method == "fractional_kelly" → stake = bankroll × kelly_fraction × f*
    method == "fixed"            → legacy (min(max_position, balance))
    """
    method: str = "fractional_kelly"
    kelly_fraction: float = 0.25
    max_bankroll_pct: float = 0.05
    min_stake_usdc: float = 1.0
    # 4B.3: hard cap on the kelly multiplier itself (and bankroll pct).
    # Applied at call-time so an unsafe per-engine kelly_fraction knob
    # cannot blow up sizing.
    max_fractional_kelly_cap: float = 0.10


@dataclass
class MarketContextConfig:
    """Cycle-11 pre-headline market-behaviour fetch.

    When enabled, the runner fetches the YES-side mid-price history for
    every relevance-survived candidate market right before classify_batch.
    Seven derived scalars are persisted on the trade row + injected into
    the prediction prompt. See app/market_context.py.
    """
    enabled: bool = True
    window_secs: int = 1800              # 30 minutes
    cache_ttl_secs: int = 30             # share fetches across variants


@dataclass
class NewsMemoryConfig:
    path: str = "/data/news_chroma"           # ChromaDB persistent directory
    dedup_distance_threshold: float = 0.12    # cosine distance below this = republish
    # ── Freshness labelling (cycle-9): label every incoming headline as
    # BREAKING / EVOLVING / REHASH / STALE_RETURN based on nearest-neighbour
    # distance + age. See app/news_memory.py:assess() for the decision table.
    fresh_dist_threshold: float = 0.30        # > this ⇒ BREAKING regardless of age
    rehash_age_secs: float = 21600.0          # 6h — REHASH window for very close matches
    stale_age_secs: float = 86400.0           # 24h — STALE_RETURN window
    recent_context_window_secs: float = 86400.0
    recent_context_n: int = 3                 # similar headlines fed back to AI
    # Cycle-13 token reduction: freshness labels that short-circuit the
    # entire pipeline. Each label here means "write per-variant skip rows
    # and DO NOT call Stage 2 (relevance) or Stage 3 (prediction)." Default
    # was just REHASH; STALE_RETURN added after the per-label win/loss
    # report showed it costing −$138.64 over 26 closed trades.
    short_circuit_labels: list = field(default_factory=lambda: ["REHASH", "STALE_RETURN"])


@dataclass
class DBConfig:
    path: str = "/data/seulgi.db"


@dataclass
class DashboardConfig:
    data_dir: str = "/data/dashboard"   # directory where all .txt files are written
    interval_secs: float = 30.0         # how often to rewrite files


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
    stop_loss_pct: Optional[float] = None
    min_entry_yes_mid: Optional[float] = None
    max_market_vol_30m: Optional[float] = None
    # M4 variant-level filters
    direction_filter: Optional[str] = None
    freshness_filter: Optional[str] = None
    mid_yes_min: Optional[float] = None
    mid_yes_max: Optional[float] = None
    # A4A.2 JIT-pricing tolerance override (bps; relative to snapshot mid).
    jit_tolerance_bps: Optional[float] = None


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
    stop_loss_pct: Optional[float] = None
    min_entry_yes_mid: Optional[float] = None
    max_market_vol_30m: Optional[float] = None
    # A4A.2 JIT-pricing tolerance override (bps; relative to snapshot mid).
    jit_tolerance_bps: Optional[float] = None
    # Per-engine relevance gate override. When None, the global
    # `relevance.keep_scores` is used. When set, this engine evaluates
    # only markets whose Stage-2 relevance verdict is in this list.
    # Example: scout sets ["STRONG_YES", "YES", "WEAK"] to see a broader
    # corpus while production engines stay at ["STRONG_YES"] for ROI.
    # The prediction LLM runs on the union across engines so every
    # engine has classify_results for its accepted markets.
    relevance_keep_scores: Optional[list] = None  # list[str] | None
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
    relevance: RelevanceConfig = field(default_factory=RelevanceConfig)
    market_context: MarketContextConfig = field(default_factory=MarketContextConfig)
    search_budget: SearchBudgetConfig = field(default_factory=SearchBudgetConfig)
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
    rv = raw.get("relevance", {})
    mctx = raw.get("market_context", {})
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
            stop_loss_pct=_opt_float(v.get("stop_loss_pct")),
            min_entry_yes_mid=_opt_float(v.get("min_entry_yes_mid")),
            max_market_vol_30m=_opt_float(v.get("max_market_vol_30m")),
            direction_filter=v.get("direction_filter"),
            freshness_filter=v.get("freshness_filter"),
            mid_yes_min=_opt_float(v.get("mid_yes_min")),
            mid_yes_max=_opt_float(v.get("mid_yes_max")),
            jit_tolerance_bps=_opt_float(v.get("jit_tolerance_bps")),
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
            stop_loss_pct=_opt_float(e.get("stop_loss_pct")),
            min_entry_yes_mid=_opt_float(e.get("min_entry_yes_mid")),
            max_market_vol_30m=_opt_float(e.get("max_market_vol_30m")),
            jit_tolerance_bps=_opt_float(e.get("jit_tolerance_bps")),
            relevance_keep_scores=(
                list(e["relevance_keep_scores"])
                if isinstance(e.get("relevance_keep_scores"), list)
                else None
            ),
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
            fresh_dist_threshold=float(nm.get("fresh_dist_threshold", 0.30)),
            rehash_age_secs=float(nm.get("rehash_age_secs", 21600.0)),
            stale_age_secs=float(nm.get("stale_age_secs", 86400.0)),
            recent_context_window_secs=float(nm.get("recent_context_window_secs", 86400.0)),
            recent_context_n=int(nm.get("recent_context_n", 3)),
            # Distinguish "key missing" from "explicit empty list":
            # config.yaml has `short_circuit_labels: []` to disable
            # short-circuit entirely, but `[] or default` evaluates to the
            # default in Python — silently overriding the operator's intent
            # and dropping ~57% of decisions to freshness:stale_return.
            short_circuit_labels=list(
                nm["short_circuit_labels"]
                if "short_circuit_labels" in nm
                else ["REHASH", "STALE_RETURN"]
            ),
        ),
        relevance=RelevanceConfig(
            enabled=bool(rv.get("enabled", True)),
            model=str(rv.get("model", "google/gemini-2.5-flash-lite")),
            temperature=float(rv.get("temperature", 0.1)),
            keep_scores=tuple(rv.get("keep_scores") or ("STRONG_YES", "YES")),
            max_tokens_per_market=int(rv.get("max_tokens_per_market", 60)),
            min_top_similarity=float(rv.get("min_top_similarity", 0.45)),
        ),
        market_context=MarketContextConfig(
            enabled=bool(mctx.get("enabled", True)),
            window_secs=int(mctx.get("window_secs", 1800)),
            cache_ttl_secs=int(mctx.get("cache_ttl_secs", 30)),
        ),
        ai=AIConfig(
            model=str(a.get("model", "google/gemini-2.5-flash")),
            premium_model=str(a.get("premium_model", "anthropic/claude-haiku-4-5")),
            premium_sim_threshold=float(a.get("premium_sim_threshold", 0.70)),
            premium_trigger_breaking_strong=bool(a.get("premium_trigger_breaking_strong", True)),
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
            stop_loss_pct=float(t.get("stop_loss_pct", 0.0)),
            min_entry_yes_mid=float(t.get("min_entry_yes_mid", 0.0)),
            max_market_vol_30m=float(t.get("max_market_vol_30m", 0.0)),
            jit_tolerance_bps=float(t.get("jit_tolerance_bps", 50.0)),
            simulate_total_usdc=float(t.get("simulate_total_usdc", 50.0)),
            simulate_min_trade_usdc=float(t.get("simulate_min_trade_usdc", 1.0)),
            simulate_fees=_load_simulate_fees(t.get("simulate_fees") or t.get("simulate", {}).get("fee_model")),
            simulate_slippage=_load_simulate_slippage(t.get("simulate_slippage") or t.get("simulate", {}).get("slippage")),
            simulate_sizing=_load_simulate_sizing(t.get("simulate_sizing") or t.get("simulate", {}).get("sizing")),
        ),
        db=DBConfig(path=str(d.get("path", "/data/seulgi.db"))),
        dashboard=DashboardConfig(
            data_dir=str(dash.get("data_dir", "/data/dashboard")),
            interval_secs=float(dash.get("interval_secs", 30.0)),
        ),
        search_budget=SearchBudgetConfig(
            brave_per_hour=int(sb.get("brave_per_hour", 20)),
        ),
        log_level=str(raw.get("log_level", "INFO")),
    )


def _load_simulate_fees(raw: Optional[dict]) -> SimulateFeesConfig:
    if not raw:
        return SimulateFeesConfig()
    base = SimulateFeesConfig()
    rates = dict(base.category_rates)
    rates.update(raw.get("category_rates") or {})
    return SimulateFeesConfig(
        enabled=bool(raw.get("enabled", base.enabled)),
        default_rate=float(raw.get("default_rate", base.default_rate)),
        category_rates=rates,
        min_fee_usdc=float(raw.get("min_fee_usdc", base.min_fee_usdc)),
        gas_cost_per_trade_usdc=float(raw.get("gas_cost_per_trade_usdc", base.gas_cost_per_trade_usdc)),
        maker_rebate_crypto=float(raw.get("maker_rebate_crypto", base.maker_rebate_crypto)),
        maker_rebate_other=float(raw.get("maker_rebate_other", base.maker_rebate_other)),
    )


def _load_simulate_slippage(raw: Optional[dict]) -> SimulateSlippageConfig:
    if not raw:
        return SimulateSlippageConfig()
    base = SimulateSlippageConfig()
    return SimulateSlippageConfig(
        enabled=bool(raw.get("enabled", base.enabled)),
        bps_per_dollar=float(raw.get("bps_per_dollar", base.bps_per_dollar)),
        max_price_move=float(raw.get("max_price_move", base.max_price_move)),
        disable_below_usdc=float(raw.get("disable_below_usdc", base.disable_below_usdc)),
        disable_for_categories=list(raw.get("disable_for_categories", base.disable_for_categories)),
    )


def _load_simulate_sizing(raw: Optional[dict]) -> SimulateSizingConfig:
    if not raw:
        return SimulateSizingConfig()
    base = SimulateSizingConfig()
    return SimulateSizingConfig(
        method=str(raw.get("method", base.method)),
        kelly_fraction=float(raw.get("kelly_fraction", base.kelly_fraction)),
        max_bankroll_pct=float(raw.get("max_bankroll_pct", base.max_bankroll_pct)),
        min_stake_usdc=float(raw.get("min_stake_usdc", base.min_stake_usdc)),
        max_fractional_kelly_cap=float(raw.get(
            "max_fractional_kelly_cap", base.max_fractional_kelly_cap)),
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

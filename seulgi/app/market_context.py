"""Pre-headline market behaviour capture (cycle 11).

For each MarketMatch the AI is about to price, fetch a 30-minute window
of mid-price history from Polymarket's CLOB and derive 7 compact
scalars (vol/mean/min/max/current/drift/n). These get injected into
the prediction prompt AND persisted on the trade row, so we can
diagnose later whether losses concentrate in noisy markets (markets
too volatile for our SL) vs spread evenly across vol (AI direction
errors).

Token discipline: only the scalars ever leave this module — never the
raw price array. ~70 tokens per market in the prompt regardless of
window length.

Reuses `impact_tracker._fetch_price_history_window` so there's no new
HTTP code path. Fail-open: any fetch error → `_mkt_ctx = None`, the
prompt block is silently skipped for that market, the trade still
happens.
"""
from __future__ import annotations

import asyncio
import logging
import statistics
import time
from dataclasses import asdict, dataclass
from typing import Optional

import aiohttp

from impact_tracker import _fetch_price_history_window


logger = logging.getLogger(__name__)


@dataclass
class MarketContext:
    """Derived statistics for a single market's recent mid-price window.

    All seven fields are scalars. The raw price list is intentionally
    discarded after stats are computed — anything that needs the bars
    can reach for `impact_tracker._fetch_price_history_window` itself.
    """
    vol: float        # stddev of mid over the window
    mean: float       # arithmetic mean
    lo: float         # window minimum
    hi: float         # window maximum
    current: float    # most recent sample
    drift: float      # last - first (signed)
    n_samples: int    # how many minute-bars came back


def _stats(history: list[dict]) -> Optional[MarketContext]:
    """Reduce raw [{t,p}, ...] entries to a MarketContext.

    Returns None when fewer than 2 samples are available — stddev is
    undefined and the AI gains no information from a one-sample point
    estimate.
    """
    prices: list[float] = []
    for entry in history:
        p = entry.get("p")
        if p is None:
            continue
        try:
            prices.append(float(p))
        except (TypeError, ValueError):
            continue
    if len(prices) < 2:
        return None
    return MarketContext(
        vol=statistics.pstdev(prices),
        mean=sum(prices) / len(prices),
        lo=min(prices),
        hi=max(prices),
        current=prices[-1],
        drift=prices[-1] - prices[0],
        n_samples=len(prices),
    )


async def fetch_context_for_matches(
    session: aiohttp.ClientSession,
    matches: list,                 # list[MarketMatch] — avoid circular import
    window_secs: int = 1800,
    cache: Optional[dict] = None,
) -> dict:
    """Fetch context for each match's YES token in parallel.

    Stashes `_mkt_ctx` (MarketContext | None) on every match in-place
    so downstream code can read it as an attribute, matching the
    `_relevance_score` / `_freshness_label` handoff pattern.

    Returns a dict of debug info for the caller's log line:
        {"n_in": int, "n_fetched": int, "elapsed_ms": float}
    `n_in` counts matches with a token_yes; `n_fetched` counts how
    many produced a non-None MarketContext.
    """
    if not matches:
        return {"n_in": 0, "n_fetched": 0, "elapsed_ms": 0.0}

    end_ts = int(time.time())
    start_ts = end_ts - window_secs

    async def _one(m) -> Optional[MarketContext]:
        token = getattr(m, "token_yes", None)
        if not token:
            return None
        if cache is not None:
            cached = cache.get(token)
            if cached is not None:
                hit_ctx, ts = cached
                if end_ts - ts < 30:        # 30s TTL — variants share the fetch
                    return hit_ctx
        try:
            history = await _fetch_price_history_window(
                session, token, start_ts, end_ts,
            )
        except Exception as exc:            # noqa: BLE001
            logger.debug("[mkt_ctx] fetch error %s: %s", token[:12], exc)
            return None
        ctx = _stats(history)
        if cache is not None:
            cache[token] = (ctx, end_ts)
        return ctx

    t0 = time.monotonic()
    results: list[Optional[MarketContext]] = await asyncio.gather(
        *[_one(m) for m in matches], return_exceptions=False,
    )
    elapsed_ms = (time.monotonic() - t0) * 1000.0

    n_in = sum(1 for m in matches if getattr(m, "token_yes", None))
    n_fetched = 0
    for m, ctx in zip(matches, results):
        m._mkt_ctx = ctx
        if ctx is not None:
            n_fetched += 1
    return {"n_in": n_in, "n_fetched": n_fetched, "elapsed_ms": elapsed_ms}


def format_market_context_block(matches: list) -> str:
    """Render the per-market context lines for the prediction prompt.

    One line per market in the same index order as the question list,
    matching the `m0`/`m1`/... idx scheme. Markets with no context
    are silently skipped — the AI doesn't need to know which token
    failed to fetch.

    Empty string when no match has context; the caller then omits the
    block entirely so we don't waste any tokens on a header alone.
    """
    lines = []
    for i, m in enumerate(matches):
        ctx = getattr(m, "_mkt_ctx", None)
        if ctx is None:
            continue
        lines.append(
            f"  m{i:<2} vol={ctx.vol:.3f} mean={ctx.mean:.2f} "
            f"min={ctx.lo:.2f} max={ctx.hi:.2f} cur={ctx.current:.2f} "
            f"drift={ctx.drift:+.3f} n={ctx.n_samples}"
        )
    if not lines:
        return ""
    header = "RECENT MARKET BEHAVIOUR (30-min pre-headline mid):"
    return header + "\n" + "\n".join(lines) + "\n"


def context_dict(ctx: Optional[MarketContext]) -> Optional[dict]:
    """Serialize a MarketContext for prompt_meta JSON. None passes through."""
    if ctx is None:
        return None
    return asdict(ctx)

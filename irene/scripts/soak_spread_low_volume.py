#!/usr/bin/env python3
"""Spread soak — low-volume band ($10k-$50k 24h volume), top-30 by volume.

Validates the cycle-12 finding (high-volume markets at $0.001-0.02 spread)
against the RAG claim "$0.05+ wide on thin markets". Pairs with the
`thin_market` gate (`cfg.trade.min_market_volume_usdc = 10_000`) — if this
band shows $0.05+ medians, the `thin_market + wide_spread($0.10)` pair is
well-tuned; if it's still tight, we can lower `wide_spread_threshold` from
0.10.

Output: CSV at `/tmp/spread_soak_low_vol.csv`. Same row schema as
soak_spread_sampler.py. Runs 30 min.
"""
from __future__ import annotations

import asyncio
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx

GAMMA = "https://gamma-api.polymarket.com/markets"
CLOB_BOOK = "https://clob.polymarket.com/book"
OUT = Path("/tmp/spread_soak_low_vol.csv")
TOP_N = 30
MIN_VOL = 10_000.0
MAX_VOL = 50_000.0
CADENCE_S = 60
DURATION_S = 1800  # 30 min


async def fetch_low_volume_markets(http: httpx.AsyncClient) -> list[dict]:
    """Walk pages from least-volume of the active set, filter into the band."""
    out: list[dict] = []
    offset = 0
    while len(out) < TOP_N and offset < 500:
        r = await http.get(
            GAMMA,
            params={
                "limit": 100, "active": "true", "closed": "false",
                "order": "volume24hr", "ascending": "true",
                "offset": offset,
            },
        )
        r.raise_for_status()
        page = r.json()
        if not page:
            break
        for m in page:
            v = float(m.get("volume24hr") or 0.0)
            if MIN_VOL <= v <= MAX_VOL:
                out.append(m)
                if len(out) >= TOP_N:
                    break
        offset += 100
    return out


async def fetch_book(http: httpx.AsyncClient, token_id: str):
    try:
        r = await http.get(CLOB_BOOK, params={"token_id": token_id})
        if r.status_code != 200:
            return None
        b = r.json()
        bids, asks = b.get("bids", []), b.get("asks", [])
        if not isinstance(bids, list) or not isinstance(asks, list) or not bids or not asks:
            return None
        bb = max(float(x["price"]) for x in bids if "price" in x)
        ba = min(float(x["price"]) for x in asks if "price" in x)
        if bb > ba or not (0.0 <= bb <= 1.0 and 0.0 <= ba <= 1.0):
            return None
        return bb, ba
    except (httpx.HTTPError, ValueError, KeyError, TypeError):
        return None


async def main():
    http = httpx.AsyncClient(timeout=10.0)
    new = not OUT.exists()
    f = OUT.open("a", newline="")
    w = csv.writer(f)
    if new:
        w.writerow([
            "ts_utc", "conditionId", "question", "volume24hr", "token",
            "best_bid", "best_ask", "spread", "mid",
        ])
    start = asyncio.get_event_loop().time()
    cycles = 0
    try:
        while asyncio.get_event_loop().time() - start < DURATION_S:
            try:
                ms = await fetch_low_volume_markets(http)
            except Exception as exc:
                print(f"[low-vol soak] gamma error: {exc}", file=sys.stderr, flush=True)
                await asyncio.sleep(CADENCE_S)
                continue
            ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
            sampled = 0
            for m in ms:
                cid = m.get("conditionId") or ""
                q = (m.get("question") or "")[:80]
                vol = float(m.get("volume24hr") or 0.0)
                raw = m.get("clobTokenIds")
                try:
                    tokens = json.loads(raw) if isinstance(raw, str) else (raw or [])
                except (json.JSONDecodeError, ValueError):
                    tokens = []
                if not tokens or not tokens[0]:
                    continue
                tk = str(tokens[0])
                book = await fetch_book(http, tk)
                if book is None:
                    continue
                bb, ba = book
                w.writerow([
                    ts, cid, q, vol, tk, bb, ba,
                    round(ba - bb, 5), round((bb + ba) / 2, 5),
                ])
                sampled += 1
            f.flush()
            cycles += 1
            print(f"[low-vol soak] cycle {cycles}: {sampled}/{len(ms)} markets @ {ts}",
                  file=sys.stderr, flush=True)
            await asyncio.sleep(CADENCE_S)
    finally:
        await http.aclose()
        f.close()
        print(f"[low-vol soak] done. {cycles} cycles, output {OUT}",
              file=sys.stderr, flush=True)


if __name__ == "__main__":
    asyncio.run(main())

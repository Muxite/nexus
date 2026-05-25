#!/usr/bin/env python3
"""Spread soak — sub-$30k 24h-volume band.

Closes the gap left by cycles 12 (high-vol, $100k+) and 14 (low-vol v2,
$30k-$50k). The cycle-16 analysis flagged that we still don't know spreads
for the long tail below $30k — RAG claims "$0.05+ wide" might hold there
even if it doesn't above $30k.

Strategy: walk descending from $30k by setting a hard volume ceiling and
filtering as we page. Stops collecting at TOP_N=30 or once we drop below
MIN_VOL=$1k (below that, markets are inactive in practice).

Output: CSV at `/tmp/spread_soak_sub30k.csv`. 30-min run.
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
OUT = Path("/tmp/spread_soak_sub30k.csv")
TOP_N = 30
MIN_VOL = 1_000.0   # below this is functionally dead
MAX_VOL = 30_000.0  # the band cycle 14 didn't reach
CADENCE_S = 60
DURATION_S = 1800   # 30 min
GAMMA_MAX_PAGES = 100


async def fetch_sub30k_markets(http: httpx.AsyncClient) -> list[dict]:
    """Walk descending; skip markets above $30k; collect until TOP_N or
    we drop below MIN_VOL (=$1k floor — sub-$1k is functionally inactive)."""
    out: list[dict] = []
    offset = 0
    for _ in range(GAMMA_MAX_PAGES):
        r = await http.get(
            GAMMA,
            params={
                "limit": 100, "active": "true", "closed": "false",
                "order": "volume24hr", "ascending": "false",
                "offset": offset,
            },
        )
        r.raise_for_status()
        page = r.json()
        if not page:
            break
        for m in page:
            v = float(m.get("volume24hr") or 0.0)
            if v > MAX_VOL:
                continue  # haven't reached the band yet
            if v < MIN_VOL:
                # Walked past the band; nothing useful further down.
                return out
            out.append(m)
            if len(out) >= TOP_N:
                return out
        offset += len(page)
        if len(page) < 100:
            break
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
                ms = await fetch_sub30k_markets(http)
            except Exception as exc:
                print(f"[sub30k] gamma error: {exc}", file=sys.stderr, flush=True)
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
            print(f"[sub30k] cycle {cycles}: {sampled}/{len(ms)} markets @ {ts}",
                  file=sys.stderr, flush=True)
            await asyncio.sleep(CADENCE_S)
    finally:
        await http.aclose()
        f.close()
        print(f"[sub30k] done. {cycles} cycles, output {OUT}",
              file=sys.stderr, flush=True)


if __name__ == "__main__":
    asyncio.run(main())

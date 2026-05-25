#!/usr/bin/env python3
"""Spread / volume soak — top-N active markets, /book every CADENCE_S seconds.

Why this exists. The C1 plan (`irene/plans/order-execution-spreads.md`) marks
spread-by-volume as a 📊 seed — RAG priors of "$0.01-0.02 tight / $0.05+ wide"
are seeds, not authority. This script collects our own data so we can replace
those seeds with measured distributions.

Output: CSV at `/tmp/spread_soak.csv`. One row per (timestamp, market) sample.
Columns: ts_utc, conditionId, question[:80], volume24hr, token, best_bid,
best_ask, spread, mid.

Soft stop: runs for DURATION_S seconds (default 30 min). Kill it earlier with
`kill <pid>` if you want partial data.

Sample analysis (run after the soak finishes):

    python3 -c "
    import csv, statistics
    rows = list(csv.DictReader(open('/tmp/spread_soak.csv')))
    spreads = [float(r['spread']) for r in rows]
    print(f'n={len(spreads)} median={statistics.median(spreads):.4f}',
          f'p90={sorted(spreads)[int(len(spreads)*0.9)]:.4f}')
    "

The soak doubles as a live regression test for `CLOBClient.get_top_of_book`
(via the bare /book GET below) — if Polymarket changes the endpoint or
response shape, this script breaks first.
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
OUT = Path("/tmp/spread_soak.csv")
TOP_N = 30
CADENCE_S = 60
DURATION_S = 1800  # 30 min


async def fetch_top_markets(http: httpx.AsyncClient) -> list[dict]:
    r = await http.get(
        GAMMA,
        params={
            "limit": TOP_N,
            "active": "true",
            "closed": "false",
            "order": "volume24hr",
            "ascending": "false",
        },
    )
    r.raise_for_status()
    return r.json()


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
                ms = await fetch_top_markets(http)
            except Exception as exc:
                print(f"[soak] gamma error: {exc}", file=sys.stderr, flush=True)
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
                tk = str(tokens[0])  # only the YES side, halves call volume
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
            print(f"[soak] cycle {cycles}: {sampled}/{len(ms)} markets sampled @ {ts}",
                  file=sys.stderr, flush=True)
            await asyncio.sleep(CADENCE_S)
    finally:
        await http.aclose()
        f.close()
        print(f"[soak] done. {cycles} cycles, output {OUT}", file=sys.stderr, flush=True)


if __name__ == "__main__":
    asyncio.run(main())

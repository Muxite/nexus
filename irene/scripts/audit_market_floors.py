#!/usr/bin/env python3
"""One-shot survey: pull `taker_base_fee` + `minimum_order_size` for top-N
active markets by volume24hr. Computes distributions, flags markets where
our $25 USDC floor is BELOW the per-market floor (= we'd fail at exchange-
side rejection without cycle-13's gate, which we now have).

Output: stdout summary + CSV at `/tmp/market_floors.csv`. ~1-2 min for N=500.
"""
from __future__ import annotations

import asyncio
import csv
import json
import statistics
import sys
from collections import Counter
from pathlib import Path

import httpx

GAMMA = "https://gamma-api.polymarket.com/markets"
CLOB_MARKET = "https://clob.polymarket.com/markets"
OUT = Path("/tmp/market_floors.csv")
N_MARKETS = 500
OUR_USDC_FLOOR = 25.0


async def fetch_top_markets(http: httpx.AsyncClient, n: int) -> list[dict]:
    out: list[dict] = []
    offset = 0
    while len(out) < n:
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
        out.extend(page)
        offset += len(page)
        if len(page) < 100:
            break
    return out[:n]


async def fetch_market_info(http: httpx.AsyncClient, cid: str):
    try:
        r = await http.get(f"{CLOB_MARKET}/{cid}")
        if r.status_code != 200:
            return None, None
        body = r.json()
        fee_bps = body.get("taker_base_fee")
        min_shares = body.get("minimum_order_size")
        fee = float(fee_bps) / 10000.0 if fee_bps is not None else None
        mn = float(min_shares) if min_shares is not None else None
        return fee, mn
    except (httpx.HTTPError, ValueError, KeyError, TypeError):
        return None, None


async def main():
    http = httpx.AsyncClient(timeout=10.0)
    print(f"[audit] fetching top {N_MARKETS} markets…", file=sys.stderr, flush=True)
    try:
        ms = await fetch_top_markets(http, N_MARKETS)
    except Exception as exc:
        print(f"[audit] gamma error: {exc}", file=sys.stderr)
        return
    print(f"[audit] {len(ms)} markets fetched; probing CLOB for each…",
          file=sys.stderr, flush=True)

    rows = []
    for i, m in enumerate(ms):
        cid = m.get("conditionId") or ""
        if not cid:
            continue
        vol = float(m.get("volume24hr") or 0.0)
        q = (m.get("question") or "")[:80]
        # Need an executable price to convert min_shares → USDC floor.
        # Use Gamma's outcomePrices[0] as a proxy (= last trade or mid).
        outc = m.get("outcomePrices") or "[]"
        try:
            outc_list = json.loads(outc) if isinstance(outc, str) else outc
            yes_price = float(outc_list[0]) if outc_list else 0.5
        except (json.JSONDecodeError, ValueError, IndexError, TypeError):
            yes_price = 0.5
        fee, mn = await fetch_market_info(http, cid)
        usdc_floor = (mn * yes_price) if (mn is not None and yes_price > 0) else None
        rows.append({
            "conditionId": cid, "question": q, "volume24hr": vol,
            "yes_price": yes_price, "fee_rate": fee, "min_shares": mn,
            "usdc_floor": usdc_floor,
        })
        if (i + 1) % 100 == 0:
            print(f"[audit] {i+1}/{len(ms)} probed", file=sys.stderr, flush=True)

    # Write CSV.
    with OUT.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "conditionId", "question", "volume24hr", "yes_price",
            "fee_rate", "min_shares", "usdc_floor",
        ])
        w.writeheader()
        w.writerows(rows)

    # Compute distributions.
    fee_vals = [r["fee_rate"] for r in rows if r["fee_rate"] is not None]
    fee_counter = Counter(round(v, 4) for v in fee_vals)
    min_vals = [r["min_shares"] for r in rows if r["min_shares"] is not None]
    floor_vals = [r["usdc_floor"] for r in rows if r["usdc_floor"] is not None]
    above_our_floor = [r for r in rows
                       if r["usdc_floor"] is not None and r["usdc_floor"] > OUR_USDC_FLOOR]

    print()
    print(f"=== Market floor survey (n={len(rows)}) ===")
    print(f"Successfully probed: fee={len(fee_vals)}, min_shares={len(min_vals)}")
    print()
    print("fee_rate distribution (fractional):")
    for k in sorted(fee_counter):
        print(f"  {k:.4f}: {fee_counter[k]}")
    print()
    print(f"min_shares: median={statistics.median(min_vals):.0f}, "
          f"max={max(min_vals):.0f}, p99={sorted(min_vals)[int(len(min_vals)*0.99)]:.0f}")
    print()
    print(f"usdc_floor (= min_shares × yes_price): "
          f"median={statistics.median(floor_vals):.2f}, "
          f"p90={sorted(floor_vals)[int(len(floor_vals)*0.9)]:.2f}, "
          f"max={max(floor_vals):.2f}")
    print()
    print(f"Markets where per-market USDC floor > our ${OUR_USDC_FLOOR:.0f}: "
          f"{len(above_our_floor)} / {len(rows)} ({100*len(above_our_floor)/len(rows):.1f}%)")
    if above_our_floor:
        print("Top 5 outliers:")
        for r in sorted(above_our_floor, key=lambda x: -x["usdc_floor"])[:5]:
            print(f"  ${r['usdc_floor']:.2f} ({r['min_shares']} sh × ${r['yes_price']:.4f}) — "
                  f"{r['question'][:70]}")

    print(f"\n[audit] full CSV at {OUT}")
    await http.aclose()


if __name__ == "__main__":
    asyncio.run(main())

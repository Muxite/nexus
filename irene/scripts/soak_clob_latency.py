#!/usr/bin/env python3
"""CLOB latency probe — measure real GET /midpoint p50/p95/p99 from our host.

Why this exists. The latency-repricing plan (`irene/plans/latency-repricing.md`)
documents that Polymarket removed a 500ms artificial taker delay, making sub-
100ms the new floor target. But we have no in-house measurement of round-trip
latency from our actual host to clob.polymarket.com. This script collects 300
samples (5 min @ 1s) and reports p50/p95/p99.

Output: stderr summary + JSON line at end. Token defaults to the BTC $150k YES
token (high-volume, always-active).

Run:
    python3 irene/scripts/soak_clob_latency.py > /tmp/clob_latency.log 2>&1 &
"""
from __future__ import annotations

import asyncio
import json
import statistics
import sys
import time
from datetime import datetime, timezone

import httpx

CLOB_MID = "https://clob.polymarket.com/midpoint"
# BTC $150k YES token (high-volume, always-mid). Override with TOKEN=... if needed.
DEFAULT_TOKEN = (
    "13915689317269078219168496739008737517740566192006337297676041270492637394586"
)
N_SAMPLES = 300  # 5 min @ 1s
CADENCE_S = 1.0


async def probe(http: httpx.AsyncClient, token_id: str) -> float | None:
    t0 = time.perf_counter()
    try:
        r = await http.get(CLOB_MID, params={"token_id": token_id})
        if r.status_code != 200:
            return None
        try:
            r.json()
        except ValueError:
            return None
    except httpx.HTTPError:
        return None
    return (time.perf_counter() - t0) * 1000.0  # ms


async def main():
    import os
    token = os.environ.get("TOKEN", DEFAULT_TOKEN)
    http = httpx.AsyncClient(timeout=5.0)
    samples: list[float] = []
    failures = 0
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[probe] start {started} token={token[:16]}... N={N_SAMPLES}",
          file=sys.stderr, flush=True)
    try:
        for i in range(N_SAMPLES):
            ms = await probe(http, token)
            if ms is None:
                failures += 1
            else:
                samples.append(ms)
            if (i + 1) % 30 == 0:
                if samples:
                    s = sorted(samples)
                    print(
                        f"[probe] {i+1}/{N_SAMPLES} ok={len(samples)} fail={failures} "
                        f"p50={statistics.median(samples):.1f}ms p95={s[int(len(s)*0.95)]:.1f}ms",
                        file=sys.stderr, flush=True,
                    )
            await asyncio.sleep(CADENCE_S)
    finally:
        await http.aclose()

    if not samples:
        print("[probe] NO successful samples — endpoint or network is broken",
              file=sys.stderr, flush=True)
        return

    s = sorted(samples)
    summary = {
        "started_utc": started,
        "n": len(samples),
        "failures": failures,
        "min_ms": round(s[0], 2),
        "p50_ms": round(statistics.median(samples), 2),
        "p90_ms": round(s[int(len(s) * 0.90)], 2),
        "p95_ms": round(s[int(len(s) * 0.95)], 2),
        "p99_ms": round(s[int(len(s) * 0.99)], 2) if len(s) >= 100 else None,
        "max_ms": round(s[-1], 2),
        "mean_ms": round(statistics.mean(samples), 2),
        "stdev_ms": round(statistics.stdev(samples), 2) if len(samples) > 1 else 0.0,
    }
    print(f"[probe] DONE {json.dumps(summary)}", file=sys.stderr, flush=True)


if __name__ == "__main__":
    asyncio.run(main())

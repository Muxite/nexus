# Irene P1 soak report — 2026-05-23

> Phase 1 ship-gate verification. All three required gates green; pipeline is approved to roll over into P2 (extraction shadow layer).

- **Run**: `python -m scripts.soak.run --count 1000 --concurrency 50 --db /tmp/irene_soak_full.db`
- **Date**: 2026-05-23 02:39 UTC
- **Build**: HEAD on `main` at the time of this report. 187 tests green.
- **Environment**: host Python 3.12.3, no chromadb (dedupe degraded to BREAKING-everything, documented), no real OpenRouter (stub AI returns YES@0.7 to exercise every gate), fake CLOB returning a deterministic 0.55 mid so the close path closes everything.

## P1 ship-gate items

The master-plan §"Phase 1 — Detailed scope" defined three required gates. All three are now green.

| Gate | Threshold | Result | Status |
|---|---|---|---|
| **1. Container boots clean** | `python -m app` boots all 6 stages and runs cleanly under load | "irene boot OK, 6 stages wired" + live Gamma poll fires; SIGTERM exit 0 | **PASS** |
| **2. Tracer drops** | `--count 1000 --concurrency 50` → 0 trade-path drops | 1000 articles → 3000 trades → **0 trade-path drops, 0 handler exceptions** in 10.2s | **PASS** |
| **3. Replay accuracy** | every closed trade's `realized_pnl_usdc` within ±$0.01 of the reconstruction | 3000 / 3000 within tolerance; max drift **$0.0000** | **PASS** |

## Funnel (from the 1000-bullet soak)

```
articles            1000
article_clusters    1000   (chromadb missing → 1:1 with articles)
signals             3000   (1000 articles × 3 stub markets per the tracer harness)
decisions           3000   (every signal records a final decision)
trades              3000   (stub AI returns YES@0.7 + loose floors → 100% execute)
trade_closures      3000   (soak forces timeout_hours=0 → closer fires on every trade)
decision_inputs     3000   (db.capture writes inside the signal-insert batch)
```

Drop counters:

```
trade_path_drops    0
handler_exceptions  0
telemetry_drops     0   (no telemetry produced during the soak)
```

## What the soak exercises

- `app/bus.py` — bounded `asyncio.Queue`, trade-path back-pressure, telemetry drop-first. With concurrency 50 and 1000 bullets the publish path doesn't drop a single trade-path message.
- `app/db.py` — `db.batch()` serialised by `asyncio.Lock`. Without the lock, concurrent subscriber tasks (`dedupe`, `match`, `trade`) collide on `aiosqlite`'s shared connection (`cannot start a transaction within a transaction`). The lock makes this safe; 3000 trades commit cleanly.
- `app/stages/match.py` + `app/db.py` — `db.capture()` writes `decision_inputs` inside the same `db.batch()` as the signal insert. The 3000 captures match the 3000 signals 1:1; if the batch rolled back, both would.
- `app/stages/trade.py` — gate cascade. Stub AI returns YES@0.7; with `min_similarity=0.01`, `min_confidence=0.55`, `min_edge=0.01` (loose tracer thresholds), 100% of signals pass every gate and execute. The cascade itself records `gate_history` in JSON for every decision; replay accuracy proves the cascade's numeric outputs are deterministically reconstructable.
- `app/stages/closer.py` — synchronous `run_close_cycle()` call after the bus drains. With `timeout_hours=0`, every open trade qualifies; with `skip_on_missing_mid=False` and a real CLOB return value, every trade closes.
- `scripts/replay/accuracy.py` — re-derives `realized_pnl_usdc` from `(direction, size_usdc, entry_mid, exit_mid)` using the same formula as the closer, against the live DB. Zero drift is expected because the soak's exit mid is constant; a future soak with varying CLOB prices will be a stricter test.

## What the soak does NOT exercise (open work for P2+)

- **Live Polymarket CLOB pricing.** Both `trade.py` (entry mid) and `closer.py` (exit mid) use a fake CLOB returning a constant 0.55 in this soak. The real `app/clob.py` client exists, is tested, and is wired by `main.py` — but the soak isolates network I/O. P2 ships a live-CLOB soak.
- **Real LLM calls.** `_TracerAI` writes deterministic predictions without hitting OpenRouter. The real `ai.py` is tested in isolation (15 test cases), and `main.py` wires it; the soak doesn't pay LLM cost.
- **Markets refresh.** The tracer pre-seeds 3 stub markets; it does not start the Gamma poll loop. The boot-smoke test does the live-Gamma verification separately.
- **News ingest.** Synthetic articles are inserted directly into `articles`. The scraper:9000 `/feed` path is exercised only by ingest's unit tests.
- **Dedupe with chromadb.** The host doesn't have chromadb installed — the stage's documented `_enabled=False` degradation labels every article BREAKING and gives it its own cluster (so clusters=articles=1000). In production / Docker, chromadb groups near-duplicates and the cluster count is lower.
- **Sustained running.** Soak runs 10.2 seconds. A multi-hour soak is the next ramp.

## Comparison vs seulgi (first-hour shape)

A direct first-hour comparison against seulgi was not run in this soak — seulgi consumes live news, irene consumes synthetic. The functional comparison is:

| Capability | seulgi | irene (today) |
|---|---|---|
| Live news ingest | yes (scraper:9000, RSS+Twitter+HTML) | yes (RSS via scraper); HTML+Twitter deferred |
| Market refresh | yes (Gamma) | yes (Gamma — live-verified in boot smoke) |
| Dedup / freshness | yes (chromadb) | yes (chromadb in Docker; fallback in dev) |
| Match → signals | yes (single table with nullable direction) | yes (`signals` table; direction REMOVED — lives in `predictions`) |
| LLM direction | yes (one writer) | yes (`ai.py`; same isolation) |
| Gate cascade | yes (engine.py monolith) | yes (`trade.py`; gate cascade in explicit order, `decisions` row per signal) |
| Position close | yes | yes (`closer.py`; P1 timeout only) |
| Capture for replay | partial (`decision_inputs` in seulgi) | yes (`db.capture()` inside live batch) |
| Tracer stress test | no | yes (1000/50, 0 drops) |
| Replay accuracy gate | no | yes (300/300, 3000/3000 within ±$0.01) |
| DB | SQLite | SQLite (Postgres draft in P4) |

## Reproducing this report

```bash
cd irene
PYTHONPATH=. python3 -m scripts.soak.run \
    --count 1000 --concurrency 50 \
    --db /tmp/irene_soak_full.db
```

Expected output:

```
soak: elapsed=~10s  passed=True
  articles            1000
  article_clusters    1000
  signals             3000
  decisions           3000
  trades              3000
  trade_closures      3000
  decision_inputs     3000
  trade_path_drops    0
  handler_exceptions  0
  replay              3000/3000 within ±$0.01  max_drift=$0.0000
```

`scripts/soak/run.py` exit code is 0 if and only if all three numeric gates pass.

## Verdict

**P1 ship-gate is GREEN.** Irene is approved to move into P2: the extraction (shadow) layer.

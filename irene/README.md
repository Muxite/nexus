# Irene

A clean-slate, DKG-shaped paper-trading bot for Polymarket, designed to run in parallel with [seulgi](../seulgi/). Both bots share the same `scraper:9000` HTTP service; **everything else is separate** — DB, chroma store, embeddings, config, dashboard.

Irene is studied-into-existence — every phase ends with a report, every paper the user drops gets a folder under `docs/papers/`, every module gets a `## Specification` block. See `master-plan.md` for the roadmap and `architecture.md` for what currently exists.

---

## Where to find things

| You want to know… | Look here |
|---|---|
| Roadmap / phased build plan | [`master-plan.md`](master-plan.md) |
| What exists today (current state) | [`architecture.md`](architecture.md) |
| The DKG design spec (north star) | [`docs/dkg_spec.md`](docs/dkg_spec.md) |
| **Modularization principles (every module follows these)** | [`docs/modularization.md`](docs/modularization.md) |
| Decision-funnel walkthrough | `docs/decision_funnel.md` (TBD P1) |
| Pitfalls / lessons from seulgi | `docs/pitfalls.md` (TBD P1) |
| Catalogue of past errors + fixes | `docs/errors_and_fixes.md` (TBD P1) |
| Canonical SQL queries | `docs/queries.md` (TBD P1) |
| Quant background (Kelly, edge, calibration) | `docs/quant_guide.md` (TBD P2) |
| Cycle methodology | `docs/cycle_methodology.md` (TBD P1) |
| Papers + formulas | [`docs/papers/`](docs/papers/) |
| **What changed today / current state of the world** | latest file in [`handoffs/`](handoffs/) |
| Point-in-time forensic reports | [`reports/`](reports/) — dated `YYYY-MM-DD-<topic>.md` |
| Cycle-by-cycle changelog | `changelogs/cycles.md` (TBD P1) |
| Open research questions | `questions/design_questions.md` (TBD P1) |
| Operational rules / runbooks | `rules/` (TBD P1) |
| Tracer-bullet injection + check scripts | `scripts/tracer/` (TBD P1) |
| Replay / backtest scripts | `scripts/replay/` (TBD P1) |

Evergreen docs in `docs/` are edited freely; `handoffs/` are dated and immutable after the next one lands; `reports/` are immutable forever (write a new dated report to revise).

---

## Pipeline (target shape)

```
              ┌──────────────────────────┐
              │  scraper:9000  (HTTP)    │  ← shared with seulgi
              │  /fetch  /feed  /twitter │
              └────────────┬─────────────┘
                           │ poll
                           ▼
              ┌──────────────────────────┐
              │  ingest    → articles    │
              │  dedupe    → clusters    │
              │  markets   → gamma cache │
              │  match     → signals     │
              │  predict   → predictions │  (LLM, OpenRouter)
              │  trade     → trades      │  (paper only in v1)
              └──────────────────────────┘
                           │
              ┌────────────┴─────────────┐
              ▼                          ▼
      irene/data/irene.db        irene/data/news_chroma/
```

P2+ layers (`extract` → `entities`/`facts`, `eventize` → `events`, `link` → `market_links`) attach to the same bus but ship later. See `master-plan.md` for phase timing.

**Cross-cutting:**
- Every stage publishes via `bus.publish(topic, payload)`; v1 is in-process (`asyncio.Queue`), v2 swaps to Redis with no other code changes. Payloads are JSON-dicts only.
- Every gate writes a `decisions` row with `outcome` + `reason`. No silent skips.
- **Tracer bullets** (synthetic articles tagged `is_tracer=1`) travel the real pipeline for end-to-end verification and back-pressure stress testing. See `scripts/tracer/` (P1).
- **Replay/backtest** (capture tables `decision_inputs` / `llm_calls`; offline scripts under `scripts/replay/`) runs counterfactual configs without re-paying LLM tokens. See `scripts/replay/` (P1).

---

## Quick start (target — most of this lands in P1)

```bash
# Start the shared scraper from the root of the parent project:
docker compose -f ../docker-compose.yml up -d scraper

# Start irene standalone (canonical):
docker compose -f docker-compose.yml up -d

# Tail logs:
docker compose logs -f irene

# Fire 10 tracer bullets and verify every stage saw each one:
python scripts/tracer/fire.py --count 10
python scripts/tracer/check.py --recent 10

# Replay last hour with a new direction prompt:
python scripts/replay/predict.py --window 1h --prompt-version direction-v4

# Reconstruct paper-PnL accuracy from snapshots (P1 ship gate):
python scripts/replay/accuracy.py --since "1h ago"
```

Anything in the table above marked `(TBD)` does not exist yet; see `master-plan.md` for when it lands.

---

## Environment variables (target)

| Var | Required | Default | Used by |
|---|---|---|---|
| `SCRAPER_URL` | yes | `http://scraper:9000` | ingest stage |
| `OPENROUTER_API_KEY` | yes (P1+) | unset | predict stage |
| `IRENE__CYCLE_NAME` | no | `cycle_<timestamp>` | main (cycles writer) |
| `IRENE__TRADING__BALANCE_USDC` | no | `1000.0` | trade stage |
| `IRENE__BUS__BACKEND` | no | `inproc` | bus (set `redis` in P2+) |
| `IRENE__TRACER__ENABLED` | no | `1` | tracer cron + injector |
| `LOG_LEVEL` | no | `INFO` | all |

---

## Hard rules irene follows from day 1

1. **No DB sharing with seulgi.** A runtime assertion in `db.py` will refuse to connect if the resolved path matches any known seulgi DB path. The dual-container hazard memo (`~/.claude/projects/-home-muk-projects-jennie/memory/project_dual_container.md`) describes the chroma corruption that proves this matters.
2. **Standalone docker-compose only.** The root `docker-compose.yml` is **not** touched. Run irene from `irene/docker-compose.yml` directly.
3. **Single-decision-per-headline.** No multi-engine variant fan-out. The comparison axis is `trades.decision_path` (mvp vs dkg), starting P3.
4. **Tracers and live trades cannot mix.** `db.insert_trade()` will reject any row with `is_tracer=1 AND paper=0`. A tracer can never become a live trade.
5. **Capture lives inside the live batch.** `decision_inputs` and `llm_calls` writes share the same `db.batch()` as the stage's primary write. Capture cannot lag the live data.

---

## Status

**2026-05-23 — Phase 0 (scaffold) just landed.** No code, no container, no schema. See [`handoffs/2026-05-23.md`](handoffs/2026-05-23.md) for what's next.

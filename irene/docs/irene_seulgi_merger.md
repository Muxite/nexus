# Irene + seulgi: merger / sync plan

> What we'd need before the two bots could become one. Not a commitment
> to merge — a forward-look so the choices we make in irene's P3+
> phases don't paint us into a corner.

- **Status:** draft, 2026-05-23.
- **Companion doc:** `docs/philosophy.md` — both bots run the same
  speed-not-prediction thesis. The merger is about codebases, not
  business model.

## Why this doc exists

irene and seulgi are doing the same job. They share scraper inputs,
the same Polymarket targets, the same LLM provider, the same edge
hypothesis. The cleaner architecture lives in irene; the months of
real-world experience live in seulgi. The merger is a question of
**when**, not **whether** — and the cost of merging grows with every
divergence we accumulate. This doc keeps the cost down by naming the
constraints up front.

## The two snapshots, today

| Concern | seulgi | irene |
|---|---|---|
| Architecture | Monolithic `engine.py` + sibling modules | Bus + 9 stages (markets, ingest, dedupe, match, trade, closer, extract, resolve, impact_tracker) |
| DB | SQLite, one file (`/data/seulgi.db`) | SQLite, separate file (`/data/irene.db`) |
| Schema style | Wide tables, in-place mutation, ALTER-heavy migration | Narrow tables, append-only, registry-driven schema |
| Bus / topics | None; direct function calls | In-process `InProcBus`; Redis-shaped contract |
| Configuration | Dataclasses, env overrides | Pydantic, env overrides + hot reload |
| Trade lifecycle | `engine.py` evaluates + executes; `trader.py` monitors positions | `match.py` → `trade.py` → `closer.py`; `trade_closures` is its own append-only table |
| LLM client | `ai.py` (direction classifier) | `app/ai.py` (direction) + separate path in `app/stages/extract.py` |
| Observability | `dashboard.py` (1927 lines, text panels), `scripts/analysis/*` | `app/dashboard.py` (smaller, text panels), `scripts/analysis/*` (subset) |
| Capture / replay | `decision_inputs` written inline; replay scripts under `scripts/postmortem/` | `decision_inputs` written via `db.capture()`; `scripts/replay/accuracy.py` is the gate |
| Extraction / DKG | None | P2: `extract.py` + 4 tables. P3 chunk 1: `resolve.py` + overlay table. |
| Container | `seulgi/docker-compose.yml` (standalone) | `irene/docker-compose.yml` (standalone) |

## What the merger does NOT change

The **edge hypothesis** is shared:

> Read news → confirm wording resolves a market → buy before reprice.

Both bots' raison d'être is identical. Anything that changes that
thesis is a different conversation, not "merge irene and seulgi".

The **interfaces with the outside world** are also shared:

- The scraper (`scraper:9000`) — already shared.
- Polymarket Gamma API + CLOB — different clients, same upstream.
- OpenRouter — different budget tracking, same upstream.

## Migration log

- **2026-05-23 — one-time data-layer import landed.** See
  `reports/2026-05-23-irene-seulgi-migration.md`. 27,914 articles +
  10,462 markets imported via `scripts/migrate/from_seulgi.py`.
  Scope was data-layer only (no trades/engine_results/price_impacts).
  This realises the "fourth end-state" described below.

## Three plausible end-states

### A. Irene wins; seulgi retires

Irene's architecture continues, seulgi's data migrates in. **This is the
most likely outcome** given that irene is being built deliberately to
avoid seulgi's known debts (WAL deadlocks, dual-container HNSW
corruption, variant attribution collapse — see master-plan §"Context").

**What this needs:**
- A one-shot importer: `scripts/migrate/from_seulgi.py` that reads
  `seulgi.db` and writes the equivalent rows into `irene.db`. Schema
  mapping in §"Schema mapping" below.
- Operational dual-write window: both bots run for some weeks, both
  trading, irene's funnel + impact_tracker numbers compared against
  seulgi's. Cutover only when irene's numbers match seulgi's within
  noise.
- A `seulgi/MIGRATED.md` stub when the bot is retired, pointing readers
  at the irene container.

### B. Seulgi wins; irene's contributions back-port

If irene's DKG layers (extract / resolve / eventize / link) don't lift
PnL, seulgi keeps running. The DKG infrastructure is shelved or runs
as an offline-only research tool.

**What this needs:**
- Honest end-of-P3 dual-path measurement (master-plan §"Phase 2+"
  P3 risk #3: "The failure mode to measure: DKG path makes the same
  trades with extra steps").
- A `dkg/` namespace under `seulgi/scripts/` for whatever extracted
  insights are worth keeping.

### C. They coexist permanently

Both keep running, both trade, the comparison serves as a continuous
A/B. Possible if neither is strictly dominant — e.g. irene is faster
on tracked markets, seulgi has broader coverage.

**What this needs:**
- Sustained shared scraper. The `scraper:9000` SLA becomes load-bearing.
- Documented "which bot is canonical for what" — e.g. seulgi's signals
  are the historical ledger, irene's funnel is the live experiment.

### D. Irene-as-warehouse; seulgi-as-demo (chosen 2026-05-23)

Irene's DB becomes the canonical store for the migrated historical
corpus + the DKG. Seulgi keeps running against its own DB to demo its
thinking system, but irene is where analytics, DKG queries, and
future trade-decision work happens. Realised by the
2026-05-23 data-layer import (see "Migration log" above).

**What this needs:**
- One-time importer (`scripts/migrate/from_seulgi.py` — done).
- Acknowledgement that seulgi's ongoing writes do NOT propagate to
  irene; the snapshot is frozen at import time. Anyone querying for
  recent seulgi state queries seulgi directly.
- A plan for what to do with seulgi's decision/trade ledger when
  it eventually matters for irene — see follow-up #2 in the
  migration report.

## The cost of NOT planning for merger

If we let irene and seulgi diverge without a merger plan, three
specific risks compound:

1. **Schema-name collisions** in any future joint analysis. Both bots'
   `trades` table sound the same but have different columns. A single
   analyst joining the two needs to know which bot's `trades.size_usdc`
   is what. → **Adopt a `bot` column or a per-bot namespace prefix in
   any shared join tables.**

2. **Duplicate market-listening.** Both bots polling Gamma at the same
   rate is twice the load with no benefit. → **A single Gamma poller
   service feeding both, OR an explicit "which bot owns market refresh
   this hour" handshake.**

3. **Double trade actions on the same news.** When (and if) we go live,
   both bots seeing the same headline would both try to take the same
   side — at best they'd race for the same fill; at worst they'd push
   the price against each other. → **A live-cutover plan must answer:
   does one bot stand down, or do they coordinate via an explicit
   leader?**

## Schema mapping (irene's tables ← seulgi's)

A draft. The full migration script lands when migration is actually
green-lit; this is the rosetta.

> **2026-05-23 update.** This draft predates seulgi's refactor that
> removed the `headlines` and `mapped_signals` tables. The corrected
> mapping (and what was actually implemented in the 2026-05-23 import)
> is below; the original draft is preserved at the bottom of this
> section for reference.

### Corrected mapping (as-built)

| irene table | seulgi source (actual) | status |
|---|---|---|
| `articles` | `signals` LEFT JOIN `headline_articles` ON `signals.headline_id = headline_articles.headline_id` | **migrated 2026-05-23** — 27,914 rows. `title ← signals.headline`, `summary ← headline_articles.body`, `published_at ← signals.detected_at`, `url ← coalesce(article_url, 'seulgi-headline://'\|\|headline_id)`, `source='seulgi_migration'`. |
| `markets` | `markets` | **migrated 2026-05-23** — 10,462 rows. Embedding is raw `np.float32.tobytes()` in seulgi (NOT pickled — the draft was wrong about this). `embedder_id='sentence-transformers/all-MiniLM-L6-v2@384'` stamped post-hoc. |
| `cycles` | (none) | **migrated 2026-05-23** — synthetic cycle `seulgi-import-2026-05-23` (id=1). |
| `article_clusters` / `article_cluster_members` | (none) | Empty post-import. Cluster history of migrated articles starts at `BREAKING` (own cluster). |
| `price_impacts` | `price_impacts` | **NOT migrated.** irene's schema requires `trade_id NOT NULL UNIQUE REFERENCES trades(id)`; with trades out of scope, this can't land without back-filling trades or relaxing the FK. Deferred. |
| `signals` / `predictions` / `decisions` | (would be `engine_results`) | **NOT migrated.** Original filter `executed=1 OR edge IS NOT NULL` returned 100% of seulgi's 10.64M engine_results rows (every row has `edge` computed). Skipped per locked decision; revisit if irene needs the historical decision ledger. |
| `trades` / `trade_closures` / `trade_features` | (would be `trades` + `positions`) | **NOT migrated.** Out of scope. 564 seulgi trades + 44 positions stay seulgi-only. |
| `entities` + `entity_aliases` + `extraction_runs` + `facts` + `entity_resolutions` | (none) | Empty post-import. Populated by the in-flight DKG bulk-extraction thread (`scripts/migrate/dkg_bulk_sample.py` + the replay step). |

### Original draft (pre-2026-05-23, for reference)

| irene table | seulgi source | notes |
|---|---|---|
| `articles` | `headlines` + `headline_articles` | seulgi splits the metadata + body; irene keeps it in one row. `articles.published_at` ← `headlines.detected_at`. `is_tracer=0` for all migrated rows. **Stale: `headlines` no longer exists.** |
| `article_clusters` + `article_cluster_members` | (none) | Seulgi's near-dup detection is per-fetch via `news_memory.py` (chromadb) — no historical cluster table. Cluster history of migrated articles starts at `BREAKING` (own cluster). |
| `markets` | `markets` | Direct field-by-field map. `embedding` (BLOB) ← seulgi's pickle-wrapped numpy → re-pickle as np.float32.tobytes() per irene's convention. **Stale: seulgi stores raw float32 bytes, no pickle.** `embedder_id` stamped post-hoc from seulgi's known model. |
| `signals` | `engine_results` + `mapped_signals` | seulgi's `engine_results` mixes signal + decision + trade. Project to irene's `signals` (article × market match only — no direction column). One signal row per (signal_id, condition_id) tuple. **Stale: `mapped_signals` no longer exists.** |
| `predictions` | `engine_results` (direction columns) | Direction + confidence float — map straight. Cache key `prompt_sha` is unknown for legacy rows; leave NULL and they don't participate in dedup. |
| `decisions` | `engine_results` (skip_reason + executed) | If `executed=1` → outcome='execute', reason='ok'. Else outcome='reject', reason=<skip_reason>. |
| `trades` + `trade_closures` | `trades` + `positions` | seulgi's `trades.closed_at` populated → write a `trade_closures` row. `realized_pnl` field maps direct. |
| `entities` + `entity_aliases` + `extraction_runs` + `facts` | (none) | Seulgi has no extraction layer; migrated articles start with empty DKG state. P3 extraction re-runs on the imported corpus to populate. |
| `entity_resolutions` | (none) | Same. P3 resolver re-runs. |
| `price_impacts` | `price_impacts` + `price_timeseries` | Direct map for the bucket columns. Sign-convention adjustment if seulgi's stored deltas use a different convention (TBD on inspection). |
| `cycles` | (n/a — seulgi has no cycle table; uses a single `cycle` column per row) | Manufacture a `cycle-imported-<date>` row and stamp every imported row with its id. |

## Operational gates before any merger step

Before flipping a switch, all four:

1. **Schema parity.** Every irene table can be populated from seulgi's
   data with no manual editing of the migration script. Verified by a
   one-shot dry-run against a snapshot.
2. **Dual-write parity.** Both bots running for N=14 days; their
   funnels (per `scripts/analysis/funnel.py`) match within 5% on
   each gate.
3. **Impact-tracker parity.** Both bots' `price_impacts` show similar
   hit-rates at +5m / +15m / +60m on overlapping markets.
4. **The trader is calmer.** A merger during a live-trading incident
   is suicide. The merger window must be a quiet period (no open
   incidents, no recent emergency patches, no pending exchange
   changes).

## What the next few P3/P4 chunks should keep in mind

- Every new table irene adds should consider its seulgi counterpart
  (or explicitly note "no seulgi equivalent — migration starts
  empty").
- Every new ENV var should be unique enough not to collide with
  seulgi's. `IRENE__*` works; `OPENROUTER_API_KEY` is intentionally
  shared.
- Every new file path under `/data/` should be irene-prefixed
  (`/data/irene.db`, `/data/dashboard/...`) so the dual-container
  hazard memo stays accurate.
- The Postgres draft (master-plan §"P4 — Dual-write comparison +
  Postgres draft") should be the FIRST opportunity to align schemas
  — Postgres is the moment we natural-rebuild, not later.

## Open questions to resolve before any merger commit

1. **Which bot owns market refresh?** Today both poll Gamma independently.
   Doubles cost + doubles request volume against the API. Pick one or
   centralise.
2. **OpenRouter budget cap accounting.** Both bots count against the
   same monthly cap; both have their own pause-flag file. If we go
   live they race for the same dollars.
3. **What's the trigger for "we should actually merge now"?** Without
   a stated trigger, divergence wins forever. Candidate triggers:
   (a) irene out-PnLs seulgi for 14 days; (b) seulgi has an incident
   that the irene architecture would have caught; (c) the live-trade
   gate is reached and we can't run both bots simultaneously.

The honest read today is: P3 chunk 2 (eventize + linker) and chunk 3
(dual-path trader comparison) tell us most of what we need. If chunk
3's report shows DKG outperforms MVP, the merger is irene's
architecture absorbing seulgi's data. If not, we keep both running and
revisit in a quarter.

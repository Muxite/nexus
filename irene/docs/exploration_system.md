# Exploration system

## Purpose

The exploration system is a **wide, cheap data firehose**: it ingests
news, matches it against Polymarket markets via the existing DKG link
graph, runs each (article, market) candidate through a stack of cheap
gates, and (for survivors) captures pre/post-news order-book data
around the article publication time. The captured data feeds an
offline **sandbox** where prompts, strategies, and PnL models can be
iterated cheaply — without touching the hot path.

The exploration container (`irene-explorer`, `IRENE_MODE=explorer`)
runs **persistently alongside production** but writes only to its own
tables (`explorations` + `odds_observations`). Cycle-25's dual-write
hazard is honored by partitioning every explorer row under a cycle
whose name starts with `explorer-`.

## Goals

1. **Capture broadly.** Every (article, market) candidate the DKG
   surfaces becomes an `explorations` row. Survivors get odds capture
   started; rejections are persisted as audit rows so we can
   reconstruct what got filtered out and why.
2. **Gate cheaply.** A pluggable pipeline runs each candidate through
   ordered gates. The cheapest gates run first; only survivors pay for
   the more expensive ones. The bundled LLM gate amortizes its system
   prompt across ~20 candidates per call.
3. **Judge offline.** No expensive analysis happens in the hot path.
   The sandbox in `irene/sandbox/` re-judges captured rows with
   whichever strategy (single-step or multi-step chain) is being
   evaluated. Costs and prompts are operator-controlled, not pipeline-
   controlled.
4. **Backtest at the right horizon.** Capture extends 1 hour past the
   news so we can simulate "would trading at t0 have been profitable"
   using realistic post-news price action.
5. **Reproducibility.** Each backtest run is replayable from cached
   LLM responses + pinned set membership. Two runs of the same
   strategy on the same set produce byte-identical PnL numbers.

## Architecture

```
news feeds ─┐
            ├─► ingest ─► dedupe ─► match ─► DKG link scoring ─► market_links
                                                                       │
                                                                       ▼
                                                              explorer.handle()
                                                                       │
                                                  builds Candidate per link
                                                                       │
                                                                       ▼
                                                      ┌────────────────────────┐
                                                      │   GatePipeline.run()   │
                                                      │   ┌──────────────┐     │
                                                      │   │  DKG gate    │     │
                                                      │   └──────┬───────┘     │
                                                      │          ▼             │
                                                      │   ┌──────────────┐     │
                                                      │   │ LLMBatchGate │     │
                                                      │   └──────┬───────┘     │
                                                      └──────────┼─────────────┘
                                                                 ▼
                                                       ┌─────────┴──────────┐
                                                       ▼                    ▼
                                                  passed              rejected
                                                       │                    │
                                                       ▼                    ▼
                                              insert exploration   insert exploration
                                                  + start          (audit-only,
                                                  odds_capture     capture_status='complete')
                                                       │
                                                       ▼
                                              odds_observations
                                              (pre-news + post-news,
                                               15s cadence × 1h)
                                                       │
                                                       ▼
                                               irene/sandbox/  (offline)
                                                       │
                                              ┌────────┴────────┐
                                              ▼                 ▼
                                          make_set        run_strategy
                                              │            (single-step
                                              ▼             or chain)
                                          set YAML              │
                                                                ▼
                                                          runs/<id>/
                                                          report.md
```

## Gate framework (`irene/app/gates/`)

A **gate** is one filter step on a list of `Candidate` records:

```python
class Gate(ABC):
    name: str
    async def evaluate(
        self, candidates: list[Candidate]
    ) -> list[GateDecision]: ...
```

`Candidate` (Pydantic BaseModel) carries every field a gate could need:
`article_id`, `market_condition_id`, `final_score`, freshness, breaking
score, article title/body, market question/description/end_date, etc.

`GateDecision` records one gate's verdict on one candidate:
`{candidate, gate_name, outcome (passed|rejected), reason, metadata}`.

`GatePipeline.run(candidates) -> dict[str, list[GateDecision]]` walks
gates in order. Each gate sees the survivors of the previous gate;
rejected candidates accumulate into the full decision history. The
return is keyed by `"article_id:market_condition_id"` so the explorer
can quickly look up "did this pair pass" and "if not, who killed it".

### Built-in gates

| Type            | File                              | Purpose                                                          |
| --------------- | --------------------------------- | ---------------------------------------------------------------- |
| `market_open`   | `irene/app/gates/market_open.py`  | Pure-Python liveness filter: rejects candidates whose market is `active=0` or past `end_date` at t0. Runs **first** (free) so dead markets never reach the LLM gate or odds capture. No network. |
| `dkg_threshold` | `irene/app/gates/dkg.py`          | Pure-Python `final_score >= min_final_score` filter. No network. |
| `llm_batch`     | `irene/app/gates/llm_batch.py`    | One LLM call per `batch_size` candidates; returns related indices. |

> **Why `market_open` exists.** A closed/resolved market has no resting
> order book, so odds capture against it returns an empty book for the
> entire post-news window — history-only mids, no askable price. A coverage
> audit found a large cohort of "untradeable" explorations that were all
> dead-at-t0. The gate drops them up front; the resolution outcome (below)
> still grades them offline.

### Adding a new gate

1. Implement a subclass of `Gate` (in `irene/app/gates/<name>.py`).
2. Add a `<name>GateConfig` Pydantic sub-model in
   `irene/app/config.py` with a `type: Literal["<type>"]` field.
3. Append it to the `GateConfig` `Annotated[Union[...], Field(...)]`
   discriminator.
4. Add a dispatch branch in `irene/app/gates/pipeline.py::build_pipeline`.
5. Reference it in `irene/explorer.yaml` under `explorer.gates`.

## Bundled LLM binary-classifier gate

The cheapest LLM-backed gate. Designed to be the cost-saving step
between DKG and any expensive downstream analysis.

**Algorithm:**
1. Take up to `batch_size` candidates (default 20).
2. Label them with short codes: `A`, `B`, …, `Z`, `AA`, `AB`, …
3. Build one user prompt:
   ```
   [A] News: <headline>
       Market: <question>
   [B] News: ...
       Market: ...
   ...
   ```
4. Send one OpenRouter call (~30 max output tokens, temperature 0).
5. Parse `{"related": ["A", "G", "K"]}`. Candidates whose label is in
   the list pass; the rest are rejected with `reason="llm_unrelated"`.

**Cache key:** SHA1 of `system | user | model | prompt_version`.
Persisted to the shared `llm_calls` table. Because `user` includes the
ordered list of labeled pairs, **each new batch misses cache** — the
cache only helps if the exact same batch fires twice (duplicate event
delivery, idempotency retries). At steady state, expect a fresh call
per batch, ~$0.07/day on a 500-candidate/hour firehose. If you want
free-after-first-call per-pair caching, ship a per-pair LLM gate
variant — bundled trades that property for cheaper per-pair first-call
cost.

**Cost** (Gemini 2.5 Flash, OpenRouter):
- Per-pair call: ~$0.00004 each.
- Bundled (N=20): ~1520 input + ~30 output ≈ $0.00012 for 20 pairs
  = ~$0.000006 per pair (~7× cheaper than per-pair).

**Tuning:** raise `batch_size` if events typically produce more
candidates than 20; lower it if you see parse errors (longer prompts
sometimes drop labels). Bump `prompt_version` to invalidate the cache
for this gate.

## Data model

### `explorations` (irene/data/irene.db)

One row per (article, market) candidate the explorer considered.

| Column                | Type    | Notes |
| --------------------- | ------- | ----- |
| `id`                  | INTEGER | PK |
| `article_id`          | INTEGER | FK → `articles.id` |
| `event_id`            | INTEGER | source event |
| `market_condition_id` | TEXT    | FK → `markets.condition_id` |
| `t0_iso`              | TEXT    | exploration creation timestamp |
| `dkg_final_score`     | REAL    | DKG link score at t0 |
| `cosine_similarity`   | REAL    | from `signals` if any |
| `freshness`           | TEXT    | BREAKING / EVOLVING / REHASH / NULL |
| `article_tier`        | TEXT    | mainstream / etc |
| `market_volume_24h`   | REAL    | snapshot |
| `market_spread_t0`    | REAL    | snapshot, NULL on book miss |
| `market_mid_t0`       | REAL    | YES mid at t0, NULL on book miss |
| `hours_to_expiry`     | REAL    | computed from market end_date |
| `llm_related`         | INTEGER | 1 if gate pipeline passed, else 0 |
| `llm_confidence`      | REAL    | legacy single-gate field; NULL under v2 pipeline |
| `llm_reason`          | TEXT    | summary of why ("dkg:passed; llm_batch:passed" or "rejected_by:..." ) |
| `llm_prompt_hash`     | TEXT    | legacy; NULL under v2 pipeline |
| `capture_status`      | TEXT    | pending / active / complete / aborted |
| `capture_end_at`      | TEXT    | t0 + capture_post_secs |
| `cycle_id`            | INTEGER | FK → `cycles.id` |
| `gate_status`         | TEXT    | **NEW**: 'passed' or 'rejected' |
| `rejected_by_gate`    | TEXT    | **NEW**: name of the first gate that rejected; NULL on pass |
| `gate_history`        | TEXT    | **NEW**: JSON list[dict] of per-gate decisions |

### `odds_observations`

Time-series of order-book snapshots per exploration over the
`[-capture_pre_secs, +capture_post_secs]` window. Only created for
`gate_status='passed'` explorations.

Columns: `id`, `exploration_id`, `token_id`, `source` (`history` for
pre-news backfill, `live` for post-news polls), `sampled_at`,
`offset_seconds` (signed), `mid`, `best_bid`, `best_ask`, `spread`.

### Resolution grading — "did we get it right"

The sandbox loader joins `markets.last_outcome_prices` and derives
`resolved_yes_payout` per row (~1.0 = YES won, ~0.0 = NO won, `None` = not
yet resolved) via the same parse as `markets.py::_state3_exit_mid`. The
report shows a per-row **Resolution** line and, when a strategy emits a
direction, a ✓/✗ correctness mark; `summary_counts` aggregates
resolved/YES/NO/pending. This grades against ground truth even for
otherwise-untradeable rows that never had a live ask — resolution data is
already present for nearly every passed exploration and keeps updating for
free as UMA finalizes, so no schema change or backfill sweep is needed.

### Data coverage and the "tradeable" filter

Not every captured exploration is usable for backtest. A row is
**tradeable** if `odds_observations` contains at least one post-news
sample (`source='live'`) with a non-NULL `best_ask` — the entry quote
used by the YES/NO Kelly sizer in `irene/sandbox/pnl.py`.

As of the cycle-26 snapshot:

| Coverage tier | rows | notes |
|---|---:|---|
| Tradeable (≥1 non-NULL `best_ask`) | 323 / 430 (75%) | usable for any backtest |
| Of those, 41-79 non-NULL `mid` samples | 309 / 430 (72%) | "good coverage" — ~5-15 min of dense ticks at 15s cadence |
| Of those, 11-40 mid samples | 13 | partial — minute-scale gaps |
| Of those, 1-10 mid samples | 1 | barely covered, treat with suspicion |
| **Untradeable** (0 non-NULL `mid`/`best_ask`) | **107 / 430 (25%)** | book reads returned NULL the whole window. Usually very thin markets (vol $0, no orders) — Polymarket has nothing to quote. |

**Per-cycle (passed-gate rows only):** explorer-0 is 217/279 (78%)
covered, cycle-23 is 94/139 (68%) — explorer-0's markets are slightly
more liquid on average.

#### Capture schedule — single source of truth

The pre-news lookback and post-news checkpoints live in **one file**,
`irene/capture_schedule.py`, so the explorer's capture horizons and the
sandbox's backtest exit horizons can never drift:

```python
CAPTURE_PRE_SECS          = 86_400          # 24h pre-news baseline
CAPTURE_POST_OFFSETS_SECS = (300, 1_800, 3_600)   # +5m / +30m / +1h
CAPTURE_POST_SECS         = max(CAPTURE_POST_OFFSETS_SECS)  # window end
```

The app imports it bare (`from capture_schedule import …`; the container
runs with `PYTHONPATH=/app`); the offline sandbox imports it as
`from irene.capture_schedule import …`. Both resolve to the same file.
`ExplorerConfig` defaults to these values (overridable per-deployment via
`explorer.capture_*`), and `run_strategy` defaults a strategy's
`exit_horizon_seconds` to `CAPTURE_POST_SECS`.

`odds_capture` keeps its dense 15s poll (so interior checkpoints are
always covered) and additionally forces one snapshot at the window
boundary, guaranteeing a usable book at/after the terminal checkpoint —
the sandbox's `offset >= horizon` exit lookup depends on it.

#### Capture window — historical vs current

All existing data was captured with `capture_post_secs=1200` (20-min
window). The bump to `3600` (1h) only applies to **new** explorations
started after the irene container restart that picked up the v2 gate
pipeline. Backtests asking for a 1h `exit_horizon_seconds` on
historical rows silently fall back to the last available live tick
(~+1199s ≈ 20 min); for those rows the run is effectively a 20-min
backtest, not a 1h one. Mixed-vintage sets are fine, but mind which
horizon you're actually testing.

#### Default filter: `tradeable_only`

`irene/sandbox/sets.py` and `make_set.py` default to dropping
untradeable rows. The filter is a fixed EXISTS subquery defined in
`irene/sandbox/loader.py::TRADEABLE_EXISTS`:

```sql
EXISTS (
    SELECT 1 FROM odds_observations o
    WHERE o.exploration_id = e.id
      AND o.source = 'live'
      AND o.best_ask IS NOT NULL
    LIMIT 1
)
```

`make_set` AND-conjoins this onto every `--filter` and every set
composition unless you pass `--include-untradeable`. Composing
existing sets also runs the filter so cached old sets get the same
treatment.

| flag | behavior |
|---|---|
| (default) | tradeable rows only — what you want for backtests |
| `--include-untradeable` | keep everything — useful for auditing rejections, coverage gaps, or untraded markets |

The set YAML's `filter_summary` field records when the filter applied;
e.g. `e.capture_status = 'complete'  [+ tradeable_only: has non-NULL
post-news best_ask]`. If you ever want to know which rows got dropped,
re-create the set with `--include-untradeable` and diff the
`exploration_ids` lists.

## Sandbox loop (`irene/sandbox/`)

The sandbox is the offline counterpart to the explorer. Strategies
re-judge captured rows; PnL is simulated against the stored
`odds_observations`.

```
irene/sandbox/
  __init__.py            – package marker
  paths.py               – filesystem roots
  loader.py              – SandboxRow + DB → list[SandboxRow]
  sets.py                – set YAML files: hand-curated id lists
  strategies.py          – strategy YAML: prompt + parse + backtest knobs
  chain.py               – multi-step chain executor
  llm.py                 – OpenRouter caller w/ on-disk cache
  pnl.py                 – Kelly sizer + entry/exit PnL
  verdicts.py            – human-graded verdict file IO
  report.py              – markdown rendering
  strategies/            – seed strategy YAMLs (v1 … v6)
irene/scripts/sandbox/   – CLI entry points (argparse, python -m)
irene/data/sandbox/      – runtime artefacts (gitignored)
  sets/                  – hand-editable set files
  cache/llm/             – per-prompt LLM response cache
  runs/<id>/             – one dir per strategy run (manifest, decisions,
                            trades, report)
  verdicts/              – human-graded verdict files
```

### Strategy types

| Strategy            | Steps                                    | Use case |
| ------------------- | ---------------------------------------- | -------- |
| `v1-baseline`       | none (uses orig LLM verdict)             | reference floor |
| `v2-direction-with-skip` | 1 single                            | replaces production prompt + adds direction/SKIP |
| `v3-kelly-pyes`     | 1 single, emits p_yes                    | low-cost probability + Kelly sizing |
| `v4-extract-classify-pyes` | 3 single chain                    | extract → classify → p_yes |
| `v5-selfconsistency` | 1 self_consistency, N samples           | sample averaging for calibration |
| `v6-critic`         | 2 chain (base + critic)                  | reflexive review |

All v3+ strategies share Kelly backtest knobs: `bankroll_usdc=1000`,
`kelly_fraction=0.25`, `max_position_pct=0.10`, `min_edge=0.02`,
`exit_horizon_seconds=3600` (matches explorer capture window).

### Sandbox CLIs

| Command                                         | Purpose |
| ----------------------------------------------- | ------- |
| `make_set --filter "<SQL>"`                     | Create a set from a SQL WHERE fragment over the joined view. |
| `make_set --all-explorations`                   | Set of every exploration across every cycle. |
| `make_set --from-set A B --op intersect`        | Compose existing sets. |
| `show_set --set NAME`                           | Render set as markdown. |
| `run_strategy --set NAME --strategy NAME`       | Backtest. `--use-orig` for the baseline; `--dry-run` for cache-only; `--max-calls N` for cost cap. |
| `verdict_template --set NAME`                   | Emit a verdict-stub file for human grading. |
| `aggregate_verdicts --in FILE`                  | Parse a filled verdict file into counts + breakdowns. |
| `compare_runs --a ID --b ID`                    | Diff two strategy runs on the same set. |

## Reproducibility contract

A second run of the same `(set, strategy)` pair must produce
byte-identical numbers.

This holds because:
- **Set YAML pins** `exploration_ids`; the manifest stores
  `sha1(sorted(ids))` so any hand-edit shows up as a different set hash.
- **LLM responses are cached** on disk at
  `irene/data/sandbox/cache/llm/<sha>.json`. Cache key is
  `sha1(system | user | model | strategy_version | temperature |
  extra_key)`. Bumping `version:` in a strategy YAML invalidates the
  cache for that strategy only (other strategies keep their cache).
- **PnL math is deterministic** given `(p_yes, entry_price, direction,
  bankroll, kelly_fraction, max_position_pct, min_edge)`.

To reproduce a run, copy the `reproduce_cmd` field from its
`manifest.json` (also printed at run end) and execute it.

## Operator runbook

### Watching the firehose

```bash
python3 -m irene.scripts.explorer.firehose_stats
```

Reports last-1h / last-24h exploration counts, admission-cap utilization,
estimated CLOB calls/sec, and odds_observations throughput.

### Tuning knobs (`irene/explorer.yaml`)

- **`max_active_explorations`** — raise if `firehose_stats` shows the
  cap is binding AND Polymarket calls/sec stays below ~50 (12.5% of the
  400 req/s budget). Lower if odds_capture is falling behind.
- **`gates[0].min_final_score`** (DKG threshold) — raise to cut volume
  if the LLM gate accepts too many borderline candidates. Lower to
  feed more headlines through.
- **`gates[1].batch_size`** — raise if per-event link counts often
  exceed the current batch; lower if you see parse errors (longer
  prompts can drop labels).
- **`gates[1].prompt_version`** — bump after editing the bundled
  gate's system prompt to invalidate cached responses.
- **`capture_post_secs`** — 3600 (1h) is the default; longer windows
  (e.g. 7200) let backtests evaluate at 2h+ horizons but use 2× the
  CLOB call budget per exploration.

### Cost expectations

- DKG gate: free (pure Python).
- Bundled LLM gate: ~$0.0001 per batch (~$0.000006/candidate at N=20)
  on fresh calls. Cache does NOT amortize across days — each new batch
  pays full cost. At 500 candidates/hour ≈ ~25 batches ≈ **$0.0025/hour**
  ≈ **$0.06/day** in steady state.
- Sandbox `run_strategy`: operator-controlled. Default `--max-calls 50`
  caps surprise costs; bump explicitly when running a full sweep.

### Rolling back

If the v2 pipeline misbehaves, set `explorer.gates: []` in
`irene/explorer.yaml` and restart the container. Every candidate then
passes (no gates → nothing to reject) and lands as a `gate_status='passed'`
row with empty `gate_history`. Effectively reverts to the firehose
behavior shipped earlier this cycle.

## What's deferred

- **Concurrent odds_capture poll loop.** Sequential is fine up to ~300
  actives; the rewrite waits until firehose stats show we're
  approaching that.
- **Pydantic everywhere in sandbox.** `SandboxRow` etc. remain
  dataclasses for now; only the IO surface (sets, strategies, manifest)
  is a candidate for promotion.
- **Article `type` field** (opinion / breaking / news / analysis).
  Wanted; orthogonal to the gate work.
- **Per-pair LLM gate.** Bundled-only first; add the per-pair variant
  later if bundled parsing turns out to be too fragile.
- **Promoting any sandbox strategy into production trading.** Sandbox
  is research-only; production prompts live in `irene/app/ai.py` and
  are edited deliberately.

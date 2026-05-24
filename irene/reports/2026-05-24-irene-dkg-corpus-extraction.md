# Full DKG extraction over the migrated 27k corpus

> First end-to-end production run of `scripts/dkg/extract_to_irene.py`
> over the full `source='seulgi_migration'` article set. Validates the
> EntityIndex perf work at real scale, gives the eventize research
> session real data to design against, and is the load-bearing
> prerequisite for P3 chunk 2 implementation.

- **Date**: 2026-05-24
- **Build**: HEAD on `main` after `9e81212` (P3 chunk 2 groundwork).
- **Cost**: $1.32 (under the $3 cap; prompt-tuning report projected $2.70).
- **Duration**: 30 min 11s (1810s) at concurrency=15.
- **Output**: writes landed directly in `irene/data/irene.db` (production tables, not the sandbox migration DB).

## TL;DR

Clean run. **27,514 articles processed in 30 min at $1.32**, 33.7% emit facts (matches the smoke-run extrapolation). The big finding is on the perf side: **`sbert_lookup` p50 dropped from 8.2ms to 24µs — a 340× speedup** at production scale, validating the EntityIndex work in commit `3629fe9`. The LLM call now solidly dominates the critical path (42ms p50 / 200ms p95 / 335ms p99), with everything else under a millisecond.

## Run summary

```
model:        google/gemini-2.5-flash-lite
prompt:       v3 (locked, per 2026-05-23-irene-dkg-prompt-tuning.md)
concurrency:  15
sample_rate:  0.05 (5% of articles timed for perf)
duration:     1810s (30m 11s)
rate:         15.2 articles/s
total cost:   $1.325
budget cap:   $3.00
aborted:      —
```

## Status counts

| status | count | % |
|---|---|---|
| ok (≥ 1 fact) | 9,277 | 33.7% |
| empty (correct ontology decline) | 17,824 | 64.8% |
| parse_error | 413 | 1.5% |
| http_error / timeout / 402 / dup | 0 each | 0% |

The 1.5% parse-error rate is slightly higher than the 0.6% from the 1k smoke; investigated 5 raw_response samples by spot-check, they're cases where the model emitted a JSON-shaped response with a top-level array of strings (not the expected `{facts: [...]}` shape). Not worth fixing for chunk 2; flagged as a possible prompt revision for later.

## Final irene.db shape

| table | rows |
|---|---|
| articles | 27,914 |
| markets | 10,462 (unchanged from migration) |
| extraction_runs | 27,914 (1:1 with articles) |
| **entities** | **4,370** |
| **facts** | **10,458** |
| entity_aliases | 460 (SBERT-merge surface forms) |
| entity_resolutions | 4,370 (1:1 with entities post-backfill) |
| events / event_edges | 0 (schema in place; awaiting eventize chunk 2) |

The 4,370 entity count is meaningfully lower than the 12-15k pre-SBERT projection — strong evidence the cosine dedup is folding surface-form variants effectively. Entity-aliases shows 460 active SBERT-merge writes (one alias row per surface form that mapped to a differing canonical via cosine).

## Entity-type distribution

```
1301  PERSON
 878  ORG
 750  EVENT
 744  COMPANY
 283  ASSET
 265  COUNTRY
 149  ECON_INDICATOR
```

PERSON dominates as expected from the smoke. EVENT-type entities (750) are the most novel category — these are named-event entities like "Q3 earnings" or "Ebola outbreak" surfaced by the LLM, distinct from the runtime `events` rows that eventize will create.

## Relation distribution

```
3448  decides
2224  announces
1567  accuses
1404  changes
1067  launches
 ... (5 other ontology relations)
```

The `decides` plurality matches the smoke. All 10 locked relations appear at meaningful volume (no relation extinct on real data, no off-ontology relation dominant).

## Resolver backfill

After extraction, `scripts/dkg/run_resolver_backfill.py` ran over the 9,393 articles with `status='ok'`:

```
articles_seen:           9,393
articles_with_unresolved: 3,077
entities_resolved:       4,370
elapsed:                 10.6s
rate:                    290 articles/s
by kind:
  self:    4,364  (99.9% — no merge target)
  acronym:     4
  suffix:      2
```

The deterministic resolver caught only 6 merges (4 acronym + 2 suffix) on top of the 460 SBERT merges from the extraction pass. This is the right shape: SBERT does the heavy lifting at insert time; the resolver covers the explicit acronym table cases (FDA ↔ U.S. Food and Drug Administration, etc.) that SBERT would only catch if both forms appeared in the corpus. Most entities have no merge target and resolve to themselves — that's just the long tail of one-off entity mentions.

## Critical-path perf (sampled at 5%, n ≈ 1,400 per stat)

| span | p50 (ms) | p95 (ms) | p99 (ms) | count |
|---|---|---|---|---|
| `article_end_to_end` | 42.2 | 200.1 | 369.2 | 1,409 |
| `llm_call` | 42.7 | 200.4 | 335.1 | 1,343 |
| **`sbert_lookup`** | **0.024** | **8.31** | **12.24** | 910 |
| `facts_write` | 0.001 | 0.045 | 0.065 | 1,389 |

Timer overhead (calibrated): 560 ns. All recorded times are corrected.

### Compare to before the EntityIndex (commit 2b81bfd, 200-article smoke):

| span | before (ms) | after (ms) | speedup |
|---|---|---|---|
| `sbert_lookup` p50 | 8.176 | 0.024 | **340×** |
| `sbert_lookup` p95 | 10.05 | 8.31 | ~1.2× |
| `article_end_to_end` p50 | 40.3 | 42.2 | ~1× (LLM dominates) |

The p50 collapse to 24µs validates the EntityIndex: dict-lookup fast path + numpy dot product, no SQL. The p95 still hits ~8ms because that's when cosine fires against a large per-type matrix — at 4,370 entities split across 7 types, the per-type matrices are ~600 rows × 384 dim, and a single dot-product is a few ms.

The remaining bottleneck is the LLM call itself (42ms p50). Anything beyond ~15-20% additional speedup would require a within-article embed batching pass (flagged in the EntityIndex commit message) or model substitution.

## What this enables

1. **Eventize chunk 2 can land.** The research doc (`docs/papers/eventization/notes.md`) cited specific high-repetition tuples like `("Trump", "decides", "Iran")` repeated 31× — the design is grounded in this real DKG, not a toy. Implementation can now proceed confidently.

2. **Linker / market_links** has real data to score against. 4,370 entities × 10,462 markets = up to 45M potential entity-market matches, which the linker will narrow down via the four scoring components.

3. **Dual-path overlap measurement** (chunk 3) has a baseline. The MVP path will produce decisions for the same corpus; chunk 3 measures the (article, market_condition_id) pair overlap.

## Known issues from this run

1. **`facts.observed_at` is `now()`, not `article.published_at`.** Per the research doc's open issues list — every fact in the DKG has `observed_at = 2026-05-24` because the extractor stamps current time. The time-window math eventize will need depends on per-article timestamps. **Must be fixed in chunk 2's first commit.** Net diff: one JOIN + one column in `extract_to_irene.py`'s `_persist_extraction`.

2. **`events` / `event_edges` schema didn't apply on first extraction run.** Because `extract_to_irene.py` didn't `import app.stages.eventize`, the eventize DDL wasn't registered. Fixed mid-session; subsequent runs apply the eventize DDL idempotently via `schema_migrations`. The same gotcha will bite anywhere a new stage's DDL must reach a long-lived DB: the entry-point must import the stage.

3. **Parse-error rate ticked up slightly** (0.6% in 1k smoke → 1.5% at 27k). Hypothesis: model returning top-level JSON arrays of strings on some headlines. Defer to a focused parse-resilience pass; not blocking chunk 2.

## Reproducing this run

```bash
# Full extraction (idempotent; safe to resume on interrupt)
set -a; source ../seulgi/.env; set +a
PYTHONPATH=. python3 -m scripts.dkg.extract_to_irene \
    --source-filter "source='seulgi_migration'" \
    --concurrency 15 --budget-cap-usd 3.0 \
    --model google/gemini-2.5-flash-lite \
    --sample-rate 0.05

# Resolver backfill
PYTHONPATH=. python3 -m scripts.dkg.run_resolver_backfill \
    --irene-db data/irene.db

# Post-state sanity
python3 -c "import sqlite3; c=sqlite3.connect('file:data/irene.db?mode=ro',uri=True); \
  print('entities', c.execute('SELECT COUNT(*) FROM entities').fetchone()[0]); \
  print('facts', c.execute('SELECT COUNT(*) FROM facts').fetchone()[0]); \
  print('entity_resolutions', c.execute('SELECT COUNT(*) FROM entity_resolutions').fetchone()[0])"
```

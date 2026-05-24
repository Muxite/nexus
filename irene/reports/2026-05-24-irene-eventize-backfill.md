# Eventize backfill — chunk 2a complete

> First end-to-end run of `EventizeStage.handle()` over the full
> 10,458-fact DKG. Validates the hybrid clustering algorithm
> (entity-overlap + cosine ≥ 0.75) against real data; unblocks the
> linker (chunk 2b) and the dual-path trader (chunk 3) per master-plan.

- **Date**: 2026-05-24
- **Build**: HEAD on `main` after the eventize implementation commit.
- **Algorithm**: hybrid clustering per `docs/papers/eventization/notes.md`.
- **Duration**: full backfill end-to-end via `scripts/dkg/run_eventize_backfill.py`.

## TL;DR

**Eventize works.** 10,458 facts → 4,382 events (avg 2.4 facts/event), with the top clusters mapping cleanly to real news stories ("Trump decides Iran call off strike" at support=107, "Ebola outbreak" at 88, "Elon Musk decides OpenAI" at 54). 992 events graduated to `confirmed` status (support_count ≥ 3) — meaningful sustained stories. One real concern: 38% `UNCLASSIFIED` event_types — the YAML rules don't cover enough (subj, relation, obj) triples and need a follow-up expansion before the linker depends heavily on event_type for scoring.

## Final shape

| metric | value |
|---|---|
| events | 4,382 |
| event_edges (all `confirms`) | 1,582 |
| facts with event_id | 10,458 (100%) |
| `emerging` (support ≤ 2) | 3,390 |
| `confirmed` (support ≥ 3) | 992 |

## Event-type distribution

```
1657  UNCLASSIFIED          (38%)
1105  ANNOUNCEMENT          (25%)
 890  REGULATORY_DECISION   (20%)
 364  PRODUCT_LAUNCH        (8%)
 246  LAWSUIT               (6%)
  40  RESIGNATION
  28  RATE_CHANGE
  24  MERGER
  19  NOMINATION
   9  ELECTION
```

**UNCLASSIFIED at 38% is too high.** Sample of high-support UNCLASSIFIED clusters reveals the gap: `(COUNTRY, accuses, PERSON)`, `(PERSON, signs, PERSON)`, `(PERSON, accuses, COUNTRY)`, and `(PERSON, changes, ...)` are all common in the corpus but missing from `app/stages/data/event_types.yaml`. Adding ~10 more `type_rules` rows should drop UNCLASSIFIED below 15%. Flagged as the first chunk-2b follow-up.

## Support-count histogram

```
   1: 2800 ████████████████████████████████████████████████████████████
   2:  590 █████████████████████████████
   3:  274 █████████████
   4:  177 ████████
   5:  146 ███████
   6:   88 ████
   7:   86 ████
  8-12:  154
 13-20:   45
 21-50:   16
 51+:     5   (107, 110, 88, 69, 54)
```

The long tail (events with 30+ confirmations spanning multiple days) is exactly what we want — they represent sustained news stories. The singletons (2,800 events with support=1) are mostly tail headlines without follow-up confirmations.

## Top 10 largest events (hand-validated as real news)

| support | type | canonical_summary | span |
|---:|---|---|---|
| 110 | UNCLASSIFIED | US accuses Raúl Castro | 2026-05-19..22 |
| 107 | REGULATORY_DECISION | Trump decides Iran call off strike | 2026-05-18..23 |
| 88 | ANNOUNCEMENT | WHO announces health emergency Ebola | 2026-05-18..23 |
| 69 | UNCLASSIFIED | WHO decides Ebola outbreak global health emergency | 2026-05-18..23 |
| 54 | REGULATORY_DECISION | Elon Musk decides OpenAI | 2026-05-18..22 |
| 46 | REGULATORY_DECISION | Trump decides Paxton | 2026-05-19..21 |
| 36 | REGULATORY_DECISION | US decides Europe divided | 2026-05-18..20 |
| 35 | ANNOUNCEMENT | Putin signs Xi strengthen deeply unbalanced relations | 2026-05-19..22 |
| 34 | UNCLASSIFIED | Barney Frank changes status to deceased | 2026-05-20..21 |
| 33 | ANNOUNCEMENT | Arsenal announces Premier League | 2026-05-19..21 |

These are real news stories. The Ebola pair (events #48 and #51) is interesting: both refer to the same outbreak but with different relations (`announces` vs `decides`) — current eventize treats them as separate events because the templated triple differs. A future polarity-aware merge could fold them.

## Algorithm behavior verified on real data

| check | result |
|---|---|
| Entity-overlap gate prevents over-merging | ✓ — events with shared entities but different relations split correctly |
| 24h time window respected | ✓ — top clusters span multiple days but are continuations, not bleed-over |
| Tie-break by most-recent works | ✓ — incremental centroid stays anchored to the active story |
| `facts.event_id` populated for 100% of facts | ✓ |
| `confirms` self-edges written | ✓ — 1,582 edges for 992 confirmed events (some events confirmed multiple times) |
| Idempotency | ✓ — re-running the backfill is a no-op (facts already carry event_id) |

## Critical perf

The backfill ran end-to-end with the live `EventizeStage` (no parallel codepath). All operations happen inside `db.batch()` per article. Embedding is batched (`embed_batch` per article) so the embedder cost amortises. Memory: the EventIndex held ~4k events × 384-d float32 ≈ 6 MB across all types at peak.

## What's still missing for profitability

This report closes chunk 2a. The path to profitability requires:

1. **Chunk 2b — Linker.** `app/stages/link.py` scores each (event, market) candidate via the four-component formula (directness / novelty / deadline_fit / resolution_fit) from the research doc. Writes `market_links` rows. Without this, the events are unreachable from the trade path.

2. **Chunk 3 — Dual-path trader.** Introduces `decision_path='dkg'` alongside `'mvp'` in `trades`. End-of-chunk report measures MVP-vs-DKG overlap; >85% overlap with no PnL lift → kill the DKG path.

3. **P4 — Postgres + sustained comparison.**

4. **P5 — Cutover.** Live trading authorisation requires ≥5 consecutive 24h windows of DKG PnL ≥ MVP PnL.

## Follow-ups to address in chunk 2b (linker session)

1. **YAML rule expansion** — drop UNCLASSIFIED from 38% to <15% by adding the missing (subj, relation, obj) tuples. Sample at the top of the UNCLASSIFIED bucket; pick out the patterns; extend `event_types.yaml`.
2. **`event_id`-aware re-extraction strategy** — if we bump `EVENTIZE_VERSION` to v2 (algorithm change), we need a separate column or a re-pass mechanism. Currently bumping the version doesn't re-cluster prior facts. Acceptable for now; revisit when we change algorithms.
3. **Polarity-aware confirm/contradict edges** — events #48 (`WHO announces...`) and #51 (`WHO decides...`) on the same Ebola outbreak should ideally `confirm` each other. Needs the polarity/synonym detection that's currently deferred.
4. **`canonical_summary` quality** — first-fact's templated triple is cheap but ugly. Linker scoring uses this string against `market.question` — if quality drops, revisit (e.g. concatenate the first-3 evidence_spans).

## Reproducing

```bash
PYTHONPATH=. python3 -m scripts.dkg.run_eventize_backfill \
    --irene-db /home/muk/projects/jennie/irene/data/irene.db

python3 -c "import sqlite3; c=sqlite3.connect('file:data/irene.db?mode=ro',uri=True); \
  print('events:', c.execute('SELECT COUNT(*) FROM events').fetchone()[0]); \
  print('confirmed:', c.execute(\"SELECT COUNT(*) FROM events WHERE status='confirmed'\").fetchone()[0])"
```

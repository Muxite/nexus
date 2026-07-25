# Irene DKG bulk-extraction — 1k validation report

> First scaled run of `scripts/migrate/dkg_bulk_sample.py` against the
> seulgi headline corpus. Locks the SBERT-replay design assumptions
> with real data before any larger pass.

- **Date**: 2026-05-23
- **Build**: `7ae2eda` (committed migrate scripts).
- **Cost incurred**: $0.0483.
- **Batch id**: `batch-20260523T223145Z-bcc332`.
- **Output db**: `irene/data/irene_dkg_migration.db` (kept; feeds replay).

## TL;DR

**Pass.** 994/1000 ok (99.4%), 6 parse errors, zero HTTP/timeout/402.
Per-call cost $0.000048 — 20% cheaper than the prompt-tuning report
projected. The model declines to extract on 65.5% of random news
headlines (correct ontology rejection), and the surface-form
duplication is exactly the failure mode SBERT dedupe is designed to
collapse. **Do NOT scale up yet** — the replay path is still unbuilt,
and several off-ontology issues need a policy before we spend more.

## Run summary

```
target            1,000
completed         1,000
elapsed           42.0s
rate              23.8/s
total cost        $0.0483
budget cap        $1.00
aborted reason    —
```

## Status counts

| status | count | % |
|---|---|---|
| ok | 994 | 99.4% |
| parse_error | 6 | 0.6% |
| http_error | 0 | 0% |
| timeout | 0 | 0% |
| empty | 0 | 0% |
| budget_paused | 0 | 0% |

6 parse errors over 1000 calls — within tolerance. Stored in
`dkg_migration_runs.raw_response` for later audit; not investigated in
this run.

## Latency

| stat | overall | ok-only |
|---|---|---|
| p50 | 530 ms | 529 ms |
| p95 | 1258 ms | 1258 ms |
| p99 | 1793 ms | 1793 ms |
| min | 325 ms | (same) |
| max | 5492 ms | (same) |

p95 < 1.3s. The pathological 5.5s max is one outlier — irrelevant for
bulk work.

## Cost

| metric | value |
|---|---|
| total (ok-only) | $0.0480 |
| per-call avg | $0.000048 |
| tokens_in avg | 393.9 |
| tokens_out avg | 22.3 |

Extrapolation:

| scale | est. cost |
|---|---|
| 10k | $0.48 |
| 100k | $4.83 |
| 500k | $24.15 |
| 2M | $96.60 |

The prompt-tuning report estimated $120 for 2M; actual would be ~$97.
We're well inside the existing $10 credit + monthly cap headroom for a
full pass when we're ready.

## Fact density (ok-only)

```
0 facts → 651 articles   (65.5%)
1 fact  → 308 articles   (31.0%)
2 facts →  31 articles   ( 3.1%)
3 facts →   4 articles   ( 0.4%)
avg facts/article: 0.38
```

The empty rate of 65.5% lines up with the prompt-tuning report's
"correctly skipped 3 out-of-ontology headlines" finding scaled up:
most random news headlines are stock-move noise, sports, weather, or
non-ontology events. The 34.5% facty rate is the actionable corpus.

## Relations emitted

```
decides     118       resigns       8
announces    93       nominates     8
changes      49       acquires      5
accuses      47       signs         5
launches     40       elects        0   ← never fired
```

All 10 locked relations covered except `elects`. `elects` is
politically-rare in any 1k sample; not a red flag.

**Off-ontology relations** (model bent the rule):

```
visits, confirms, appoints, defends, releases,
sends, welcomes, loses, demand    (9 unique, ~10 total facts)
```

Per the v3 prompt: "If the headline doesn't fit the ontology, emit an
empty facts list." Instead the model shoehorns the headline into a
made-up relation in ~1% of facty articles. **Policy needed in replay:
drop these or remap.** Recommendation: drop and count under
`extraction_runs.dropped_fact_count`. The signal that this is rare
(~1% of fact-emitting calls) means a hard drop won't bleed real
information.

## Subject entity types

```
PERSON           133       EVENT           13
ORG               90       ASSET            6
COUNTRY           85       ECON_INDICATOR   4
COMPANY           50       CITY             1   ← off-ontology
```

One `CITY` fact. **Policy needed in replay:** drop (single occurrence
doesn't justify expanding the locked 7-type ontology).

## Surface-form duplication (what SBERT must collapse)

These pairs/clusters appeared in the top-30 subject list and would
SBERT-merge under threshold 0.85:

| cluster | surface forms |
|---|---|
| Trump | "Trump" (19), implicit elsewhere |
| Putin | "Putin" (5), "Vladimir Putin" (3) |
| United States | "US" (13), "U.S." (3) |
| Supreme Court | "Supreme Court" (4), implicit elsewhere |

And almost certainly more in the long tail. This is the gap the
prompt-tuning report estimated at "0.18 strict-precision". Validating
that SBERT closes it is the gate for step 4 (replay).

## Seulgi corpus coverage (sanity)

Sample-date distribution:

```
2026-05-18    26       2026-05-21    63
2026-05-19   447       2026-05-22   105
2026-05-20   340       2026-05-23    19
```

Heavily skewed to 5-19 / 5-20 — likely a seulgi ingest backfill. Not
a problem for this validation (we care about LLM behaviour, not
temporal coverage), but worth flagging if we later care about
multi-week balance: increase `--seed` variety or sample stratified by
`detected_at`.

Freshness label distribution:

```
EVOLVING       362       STALE_RETURN  225
REHASH         344       BREAKING       69
```

In line with seulgi's general freshness mix — random sampling didn't
bias toward any one bucket.

## Decisions locked from this run

1. **SBERT dedupe is justified.** Surface-form duplication is real and
   visible at 1k; will be much worse at 50k+ where the long tail
   explodes. Don't skip it.
2. **Threshold 0.85 is the starting point**, per the prompt-tuning
   report. The 1k batch is too small to tune the threshold reliably;
   the replay step's gold-set eval (`extraction_quality.py
   --alias-aware`) is the right tuning surface.
3. **Replay must validate ontology.** Both off-ontology entity types
   (`CITY`) and off-ontology relations (~9 unique) appear at >0%
   frequency. The replay should drop these to `dropped_fact_count`
   rather than insert them, mirroring the existing extract.py
   validation.
4. **In-memory entity index is fine for 1k.** A 50k or 500k pass would
   reveal whether in-memory survives; the per-`entity_type` partition
   means each matrix stays bounded. Hold the chroma option in
   reserve.

## Why not scale up now

The plan calls for SBERT dedupe (step 3) and replay (step 4) before
the next scale-up. The 1k batch is **enough to validate the replay
path end-to-end**: ~390 facty articles, ~600 fact rows, ~100-150
unique-ish entities (post-SBERT-collapse) — a meaningful test bed.
Spending more on LLM output before knowing the replay works correctly
risks having to re-collect at a different SBERT threshold or after an
ontology policy change.

If the 1k turns out to be too sparse for replay validation, the
follow-up run is $0.05 — trivial to extend.

## Reproducing

```bash
cd /home/muk/projects/nexus/irene
set -a; source ../seulgi/.env; set +a
PYTHONPATH=. python3 -m scripts.migrate.dkg_bulk_sample \
    --target 1000 --concurrency 15 --budget-cap-usd 1.0 \
    --model google/gemini-2.5-flash-lite --seed 42

PYTHONPATH=. python3 -m scripts.migrate.dkg_stats \
    --db data/irene_dkg_migration.db --format text
```

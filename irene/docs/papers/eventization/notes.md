# Eventization — research session

> Pre-implementation design notes for `app/stages/eventize.py`. Locks
> the clustering metric, time window, tie-break rule, and the four
> DKG scoring components before any clustering code lands.

- **Date**: 2026-05-24.
- **Mandate**: master-plan §"Phase 2+" P3 — "Three open research
  problems must be answered in a dedicated research session at the
  start of P3 before any code". This is that session.
- **Companion doc**: `docs/dkg_spec.md` is the authoritative schema /
  topic contract; this doc fills in the *algorithm* the spec
  underspecifies.
- **Real-data anchor**: 4506 / 27914 articles extracted at draft time
  (full corpus extraction in flight under
  `scripts/dkg/extract_to_irene.py`). All figures below come from that
  partial run; they will shift mildly when full extraction completes
  but the orders of magnitude are stable.

## What eventize must do (one paragraph)

Take the incoming stream of `(subj_entity, relation, obj_entity,
obj_value, evidence_span)` facts from `extract.py` and cluster them
into canonical real-world **events** — "an FDA decision on Pfizer's
drug X", "a Trump announcement about Iran sanctions on date Y". Each
event accumulates a `support_count` (how many facts confirm it), a
`canonical_summary`, a temporal envelope (first_seen / last_seen), and
edges to other events (`confirms`, `contradicts`, `escalates`,
`resolves`). The downstream **linker** (`link.py`) takes these events
and scores them against open Polymarket markets via four feature
scores: `directness`, `novelty`, `deadline_fit`, `resolution_fit`.

## Real-data evidence

From the partial extraction:

| metric | value |
|---|---|
| facts | 1,813 |
| distinct (subj, relation) pairs | ~1,200 (~67% singleton) |
| entities | 1,689 |
| entity_type | PERSON 473, ORG 343, COMPANY 320, EVENT 264, COUNTRY 150, ASSET 107, ECON_INDICATOR 41 |
| repeated (subj, relation) ≥ 3× | 200+ pairs |
| max (subj, relation, obj) repeats | 31× — `("Trump", "decides", "Iran")` |

Sample of high-repetition tuples (these are the events eventize must
cluster correctly):

```
 31x  ('Trump',     'decides',   'Iran')
  9x  ('Trump',     'announces', 'Iran')
  9x  ('Elon Musk', 'decides',   'OpenAI')
  8x  ('Trump',     'decides',   'IRS')
  8x  ('Elon Musk', 'accuses',   'OpenAI')
  8x  ('WHO',       'announces', 'Ebola outbreak')
  7x  ('Musk',      'decides',   'OpenAI')          ← SBERT merge candidate
  6x  ('Elon Musk', 'accuses',   'Sam Altman')
```

Two visible signals these inform:

1. **Surface duplication still leaks past extract.** "Elon Musk" and
   "Musk" both appear; "US" and "U.S." both appear. The resolver
   (acronym/alias rules) catches some; the SBERT-dedup in
   `find_or_create_entity` catches others; eventize must STILL use
   `entity_resolutions.resolved_to_id` for its clustering identity,
   not the raw `subj_entity_id`, to handle the residue.

2. **Same (subj, relation, obj) tuple ≠ same event.** 9× ("Elon Musk",
   "decides", "OpenAI") is almost certainly NOT one event reported
   nine times. It's likely 3-5 separate Musk-OpenAI decisions over
   weeks of news. **The time window matters as much as the entity
   tuple.**

## Problem 1 — Fact-clustering similarity metric

### Candidates

| metric | pro | con |
|---|---|---|
| Jaccard on entity-resolved subj/obj set | cheap, deterministic, captures "(FDA, decides, Pfizer) ≈ (FDA, denies, Pfizer)" | misses paraphrase events with different entity sets; same entity tuple = same cluster (over-merges) |
| Sentence cosine on `evidence_span` | captures semantic identity even across different entity surfaces | misses entity overlap; cost ~5ms per fact via the embedder |
| **Hybrid: cosine on `{subj} {relation} {obj} {obj_value}`, gated by entity overlap** | combines both signals; relation vocabulary is locked (10 verbs) so templated similarity behaves well | two embedder calls per fact (insert + lookup); needs threshold tuning |

### Recommendation — hybrid, with explicit fallback

**Lock the hybrid.** Match a new fact F to an existing event E iff:

1. **Entity-overlap gate**: F's resolved subj_entity_id is in E's
   support-set OR F's resolved obj_entity_id is in E's support-set.
   Mandatory — no cluster without a shared entity. This rules out
   the "different events with overlapping vocabulary" failure mode.

2. **Semantic similarity**: `cosine(embed(F.template), E.centroid) ≥
   0.75`, where `F.template` is the string
   `f"{F.subj_canonical} {F.relation} {F.obj_canonical or ''} {F.obj_value or ''}"`
   trimmed. The event's centroid is the running mean of its members'
   embeddings, recomputed lazily (every 10 confirmations).

3. **Time window**: F.published_at within `±EVENT_WINDOW_HOURS` of
   E.last_seen_at. Default 24h, configurable per event_type.

The 0.75 threshold is the conservative side of "same event"; tune
post-implementation by sampling clusters and hand-judging. A lower
threshold (0.65) over-merges; higher (0.85) under-merges.

### Why not Jaccard alone

The entity-overlap rule above is already a Jaccard floor. Adding
cosine on top is what discriminates "Trump decides Iran sanctions on
2026-05-19" from "Trump decides Iran-deal renegotiation on
2026-05-22" — both share entities but the templated triple + obj_value
differs. Pure Jaccard would over-merge.

### Why not pure cosine

Embedder-only clustering would happily fold ("Pfizer", "announces",
"Q2 earnings") and ("Pfizer", "announces", "Q3 earnings") into one
event because the surface text is 90%+ similar. The entity-overlap
gate alone doesn't help; obj_value differentiation is what saves us,
and that lives in the templated string. So both signals are needed —
hybrid it is.

## Problem 2 — Time window + tie-break rule

### Time window

**Default 24h, configurable per `event_type`.** Justification:

- **News-propagation pattern.** Financial news on a discrete event
  (FDA decision, rate change, earnings beat) typically peaks within a
  few hours of breaking and decays over 24-48h. Most "confirming"
  reports arrive same-day.
- **Empirical from seulgi**: the 1k validation report showed 65.5%
  of headlines emit empty (i.e. recycled / commentary). Of the facty
  ones, the dense first-day cluster dominates. A 24h window admits
  the dense cluster and clips the commentary tail.
- **Per-`event_type` override.** Long-resolving events (UK_ELECTION,
  TRIAL, REGULATORY_DECISION) need longer windows — 7-30 days. Short
  events (RATE_CHANGE) might warrant 6h. Implement as a YAML map
  alongside `acronyms.yaml`; default to 24h on unknown types.

### Tie-break rule

When a new fact F satisfies the hybrid criterion against MULTIPLE
existing events {E₁, E₂, …}, attach to the one whose `last_seen_at`
is most recent. Tie-broken further by:

1. Higher cosine score.
2. Lower event id (oldest).

This matches the news-evolution pattern: "Trump decides Iran" in a
fresh article almost always refers to the ongoing story, not a
six-month-old archived one.

### Stale events

An event is `stale` once `now() - last_seen_at > STALE_HOURS`
(default 72h, configurable per type). Stale events are EXCLUDED from
the candidate pool for new-fact attachment. They remain queryable for
historical analysis but won't accidentally re-anchor unrelated facts.

## Problem 3 — DKG scoring components (real implementations)

Per master-plan critique: **if a component is provably uniform on real
data, drop it.** Implementations below; histogram + uniformity check
is mandatory after eventize lands, before linker work begins. Drop any
component whose population std-dev is < 0.05 across real events.

### directness_score

How directly does this event resolve the market? Math:

```
directness = max(0, cosine(event.canonical_summary_embedding,
                           market.question_embedding))
             * entity_overlap_bonus
```

Where `entity_overlap_bonus` is `1.0` if the market.question mentions
at least one entity from the event's support set (via existing
entities.canonical_name match or alias), else `0.5`. The two-tier
form is more discriminative than a single boolean — markets that name
the entity get the full cosine; markets that match only semantically
get a 50% haircut.

### novelty_score

Z-score of confirm-count relative to a per-event-type 30-day baseline:

```
novelty = clamp(1.0 - sigmoid((support_count - baseline_p50) / baseline_std), 0.0, 1.0)
```

Where `baseline_p50` and `baseline_std` are recomputed nightly per
event_type. First confirmation → high novelty (near 1.0); 10th
confirmation → low novelty (near 0). The sigmoid keeps the transition
smooth.

### deadline_fit_score

Temporal distance from the event timestamp to the market's `end_date`:

```
days_to_deadline = (market.end_date - event.last_seen_at) / day
deadline_fit = clamp(1.0 - days_to_deadline / 30, 0.0, 1.0)
```

Events that resolve a market within 30 days get a meaningful score;
beyond that the market is too far out for our event to be load-bearing.
Negative days (event after deadline) → 1.0 (already resolved); irene
shouldn't trade that anyway, the market-state filter upstream catches it.

### resolution_fit_score

Polarity match: does the event's direction map to a YES on the market?
Requires a per-event-type → market-direction lookup. Initial table
(extensible in YAML):

| event_type | polarity → market_yes_means |
|---|---|
| REGULATORY_DECISION | approve → YES, deny → NO |
| ELECTION | candidate_won → YES (with binding to question subject) |
| RATE_CHANGE | direction match (cut/hike/hold) per question |
| MERGER | announced → YES, blocked/withdrawn → NO |
| LAWSUIT | filed → YES (markets resolve on filing) |

For event_type not in the table, `resolution_fit = 0.5` (neutral). The
linker logs unknown event_types so we can extend the table over time.

### Uniformity check (after eventize ships)

```sql
SELECT
  AVG(directness_score) AS d_mean, STDEV(directness_score) AS d_std,
  AVG(novelty_score)     AS n_mean, STDEV(novelty_score)    AS n_std,
  AVG(deadline_fit_score) AS f_mean, STDEV(deadline_fit_score) AS f_std,
  AVG(resolution_fit_score) AS r_mean, STDEV(resolution_fit_score) AS r_std
FROM market_links;
```

Drop any score with `std < 0.05` — it adds noise without discrimination.

## Problem 4 — Dual-path overlap measurement (gate for chunk 3)

The failure mode the master-plan calls out: "DKG path makes the same
trades the MVP path made, with extra steps". Operationalised:

```sql
-- For each (article_id, market_condition_id) pair, did MVP and DKG
-- reach the same execute/reject decision?
WITH mvp_decisions AS (
  SELECT s.article_id, s.market_condition_id,
         d.outcome AS mvp_outcome, d.reason AS mvp_reason
  FROM signals s
  JOIN decisions d ON d.signal_id = s.id
  WHERE d.decision_path = 'mvp'
),
dkg_decisions AS (
  SELECT s.article_id, s.market_condition_id,
         d.outcome AS dkg_outcome, d.reason AS dkg_reason
  FROM signals s
  JOIN decisions d ON d.signal_id = s.id
  WHERE d.decision_path = 'dkg'
)
SELECT
  COUNT(*) AS pairs,
  SUM(CASE WHEN mvp_outcome = dkg_outcome THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS agreement,
  SUM(CASE WHEN mvp_outcome != dkg_outcome AND dkg_outcome = 'execute' THEN 1 ELSE 0 END) AS dkg_extra_executes,
  SUM(CASE WHEN mvp_outcome != dkg_outcome AND mvp_outcome = 'execute' THEN 1 ELSE 0 END) AS dkg_extra_rejects
FROM mvp_decisions m
JOIN dkg_decisions d USING (article_id, market_condition_id);
```

Gate (per master-plan): if **agreement > 0.85 AND no PnL lift in
`dkg_extra_executes` trades**, kill the DKG path. The PnL comparison
requires the impact_tracker to have polled both sets — chunk 3
verification.

## Open issues from the real-data scan

1. **`facts.observed_at` is `now()`, not article.published_at.** Every
   fact in the partial DKG has `observed_at = 2026-05-24` because
   `extract_to_irene.py` stamps current time, not the source article's
   `published_at`. Eventize's time-window math will be broken until
   this is fixed. **Action:** patch
   `scripts/dkg/extract_to_irene.py` to read `published_at` from
   `articles` and stamp it onto `facts.observed_at`. Net diff: one
   JOIN + one column.

2. **Entity surface-form residue.** "Musk" vs "Elon Musk", "US" vs
   "U.S." appear despite SBERT-dedup + resolver. Two mitigations:
   - Run `scripts/dkg/run_resolver_backfill.py` over the corpus
     (covered in step 2 of the current plan).
   - When `facts.event_id` is being computed, ALWAYS look up the
     entity via `COALESCE(entity_resolutions.resolved_to_id,
     facts.subj_entity_id)`, never the raw id.

3. **EVENT entity-type as the second-largest category (264 rows) is
   confusing.** EVENT is in the ontology for things like "Ebola
   outbreak" or "Q3 2026 earnings". When EVENT appears as a fact's
   subj or obj, it's a NAMED event distinct from the runtime event
   eventize will create. Disambiguate in the schema by naming the
   eventize table `events` (which we did) and the ontology entity
   type `EVENT` (which we have). They don't conflict but the docs
   should be clear.

## Open questions explicitly deferred

These are flagged for chunk-2 implementation review, not resolved
here:

1. **What's the right `event_type` value to insert?** Initial proposal:
   derive from the dominant `(subj_entity.type, relation, obj_entity.type)`
   triple via a YAML lookup. e.g. `(ORG, decides, COMPANY) →
   REGULATORY_DECISION`; `(PERSON, elects, COUNTRY) → ELECTION`.
   Build the table empirically post-extraction.
2. **`canonical_summary` generation.** Cheap: concatenate the first 3
   evidence_spans into one string. Expensive: an LLM call to summarise.
   Start cheap; revisit if linker scores end up dominated by spurious
   centroids.
3. **Cross-cycle event merging.** Should an event from yesterday's
   cycle absorb today's matching fact, or stay sealed? Lean: absorb
   if within window. The `cycles` table is for write-attribution, not
   semantic boundaries.

## Decisions locked from this session

1. Clustering metric: **hybrid** — entity-overlap gate + 0.75 cosine on the templated triple.
2. Time window: **24h default, per-type configurable** via YAML.
3. Tie-break: **most-recent active event wins**; secondary by cosine, tertiary by event id.
4. Scoring formula: **all four components** (directness / novelty / deadline_fit / resolution_fit) implemented per the math above. Uniformity check before linker work begins; drop any with `std < 0.05`.
5. Identity for clustering: always go through `entity_resolutions.resolved_to_id` (never raw subj_entity_id).
6. `facts.observed_at` bug: **must be fixed before eventize ships** — patch the extractor.

## What chunk 2 needs to land (implementation checklist)

- [ ] Fix `scripts/dkg/extract_to_irene.py` to stamp `facts.observed_at` from `articles.published_at`.
- [ ] Implement `EventizeStage.handle()` per the hybrid algorithm above.
- [ ] Add per-event-type YAML config under `app/stages/data/event_types.yaml` (window override, polarity table).
- [ ] In-memory `EventIndex` mirroring `EntityIndex` (per-event_type matrix of centroid embeddings; same load+register pattern).
- [ ] `app/stages/link.py` consuming `TOPIC_EVENT_UPDATED`, writing `market_links` (schema not in this doc — chunk 2 design).
- [ ] Tests covering: cluster hit, cluster miss → new event, time-window split, stale-event exclusion, tie-break across multiple matches, contradicts-edge detection.
- [ ] Uniformity histogram in the end-of-chunk-2 report.

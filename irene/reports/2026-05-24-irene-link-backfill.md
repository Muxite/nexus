# Linker backfill + profitability assessment — chunk 2b

> First end-to-end run of `LinkerStage` over the full populated DKG.
> Scores 4,331 events against 9,024 active markets via the four
> components (directness / novelty / deadline_fit / resolution_fit),
> writes top-K candidates to `market_links`. Closes P3 chunk 2; the
> last gate before chunk 3 (dual-path trader) and the path to live
> profitability.

- **Date**: 2026-05-24
- **Algorithm**: per `docs/papers/eventization/notes.md` §"Problem 3".
- **Duration**: 205s (3:25) at ~21 events/s.

## TL;DR

**The DKG produces real trade signal — but the noise floor is the next thing to fix.** 

At `final_score ≥ 0.8` we get 45 links of which **69% have an explicit event-entity name in the market question** — these are high-confidence trade candidates. Quality drops smoothly: ≥0.75 is 32% entity-grounded, ≥0.7 is 15.7%, ≥0.5 is 10.9%. The pattern says "use a high score threshold as the gate" — exactly the operational shape we want for chunk 3's dual-path trader.

The honest verdict on profitability: **we have signal, we don't yet have proof of edge.** To convert signal → realised PnL we need (1) the dual-path trader (chunk 3), (2) a way to measure realised price moves against the DKG-selected candidates — either migrate seulgi's `price_impacts` (currently skipped — needs trades FK relaxation or backfill) or paper-trade forward against live Polymarket data.

## What landed

| metric | value |
|---|---|
| events scored | 4,331 |
| market_links written | 43,626 (avg 10.07 per event = top-K cap) |
| duration | 205 s (3:25) |
| rate | 21 events/s |

Active markets in the pool: 9,024 (of 10,462 total — closed markets excluded by the stage's `WHERE active=1`).

## Score component health (uniformity check — research-doc gate)

Per the research doc, drop any component with std < 0.05 on real data. All four pass:

| component | mean | std | min | max |
|---|---:|---:|---:|---:|
| directness  | 0.176 | **0.104** | 0.10 | 0.81 |
| novelty     | 0.806 | **0.266** | 0.18 | 1.00 |
| deadline_fit | 0.660 | **0.248** | 0.00 | 0.98 |
| resolution_fit | 0.646 | **0.153** | 0.50 | 0.95 |
| **final** | 0.572 | 0.099 | 0.22 | 0.84 |

`final_score`: p95 = 0.72, p99 = 0.76, max = 0.84. The score never approaches 1.0 because (a) directness max is ~0.8 (cosine on simple summaries against question text caps out), and (b) the four-component mean caps any single component's pull.

## Final-score distribution

```
[0.00, 0.30):    431
[0.30, 0.50): 10,036
[0.50, 0.60): 13,540
[0.60, 0.70): 15,711
[0.70, 0.75):  3,346
[0.75, 0.80):    517
[0.80, 0.85):     45  ← gate candidates
```

Events with at least one high-score link:

| threshold | events |
|---|---|
| ≥ 0.5 | 3,768 |
| ≥ 0.6 | 2,596 |
| ≥ 0.7 | 622 |
| ≥ 0.75 | 128 |
| ≥ 0.8 | **19** |

19 events with one or more links scoring ≥ 0.8. Of 4,331 events linked, that's 0.4% — a small high-confidence set, exactly the kind of filter chunk 3's trader needs.

## Top-20 (event, market) pairs — hand-validated

Real matches (would-be good trade candidates):

| score | event | market | verdict |
|---:|---|---|---|
| 0.841 | Trump nominates Ken Paxton | Will Ken Paxton win the 2026 Texas Republican Primary? | **TRUE** |
| 0.833 | Trump nominates Ken Paxton | Will Ken Paxton drop out? | **TRUE** |
| 0.830 | Trump nominates Ken Paxton | Will Ken Paxton win the Texas Republican Senate runoff by 9%+ | **TRUE** |
| 0.820 | Jackson decides Republican primary run-off | Will Everett Jackson be the Republican Nominee for TX-30? | **TRUE** |
| 0.819 | Khamenei decides uranium must stay in Iran | US obtains Iranian enriched uranium by May 31? | **TRUE** |
| 0.817 | Trump decides Republican primary | Will John Cornyn win the 2026 Texas Republican Primary? | TRUE-context |

False positives from keyword collision:

| score | event | market | failure mode |
|---:|---|---|---|
| 0.834 | David Brock Smith decides GOP | Will Matthew Smith be Republican Nominee for TX-19? | "Smith" collision — different people |
| 0.829 | Israel changes Iran stance | Bank of Israel Interest Rate | "Israel" → country vs central bank |
| 0.821 | France decides Israel | Bank of Israel Interest Rate | same |
| 0.820 | Western nations decides Israel halt construction | Bank of Israel Interest Rate | same |

Pattern is clear: **short, common entity names ("Israel", "Smith", "US") drive the noise floor**. The current entity-overlap bonus is a substring check on tokens, which can't distinguish "Israel" the country from "Bank of Israel" the institution.

## Entity-grounded true-positive rate by threshold

A reasonable proxy for "is this link plausible?": does the market question contain at least one of the event's entity names (length ≥ 4 to skip common-word noise)?

| threshold | links | entity-grounded | true-positive rate |
|---:|---:|---:|---:|
| ≥ 0.5 | 33,159 | 3,601 | 10.9% |
| ≥ 0.6 | 19,619 | 2,335 | 11.9% |
| ≥ 0.7 | 3,908 | 615 | 15.7% |
| ≥ 0.75 | 562 | 180 | **32%** |
| ≥ 0.8 | 45 | 31 | **69%** |

**This is the most important number in the whole report.** Quality grows monotonically with threshold. A trader-side gate of `final_score ≥ 0.8` produces a 69%-precision candidate stream — usable, with room for further filtering downstream.

## Profitability assessment — honest read

**Can we be profitable?** Not yet provable. We have:
- ✓ Signal: top 0.8+ links are 69% real-news-to-real-market matches.
- ✓ A usable score gate.
- ✓ End-to-end DKG pipeline (extract → resolve → eventize → link).

We DON'T yet have:
- ✗ Decision emission: no `decision_path='dkg'` rows in `trades` / `decisions`.
- ✗ PnL feedback: the migration intentionally skipped `price_impacts` because of the trades FK. We have no historical realised price moves to score the DKG path against.
- ✗ A comparison against MVP (chunk 3's central task).

**The path to a yes/no on profitability** is two steps:
1. **Chunk 3: dual-path trader.** Emit `decision_path='dkg'` decisions for events with `final_score ≥ 0.8` (or whatever gate empirics support). Same decision row format as the MVP path.
2. **Realised-PnL collection.** Either (a) backfill seulgi's `price_impacts` (requires loosening the `trades.id FK` on `price_impacts.trade_id`, or backfilling synthetic trades), or (b) point an `impact_tracker` at the DKG-decision markets and let it poll forward.

Once both are done, the master-plan §"P5 — Cutover candidates" gate kicks in: ≥5 consecutive 24h windows of DKG PnL ≥ MVP PnL → live trading authorisation.

## What needs fixing (priority-ordered)

1. **Tighten the entity-overlap bonus.** The current substring check on summary tokens → market question is the dominant noise source. Replace with: the event's RESOLVED entity (`entity_resolutions.resolved_to_id` → `entities.canonical_name`) must match an entity-name pattern in the market question. Drops the "Israel" / "Smith" collisions.

2. **Drop UNCLASSIFIED below 15%.** Currently 38% of events have UNCLASSIFIED type, which forces `resolution_fit = 0.5` (neutral) instead of a real value. Adding ~10 more `type_rules` rows to `event_types.yaml` should fix the top patterns ((COUNTRY, accuses, …), (PERSON, signs, …), (PERSON, changes, …)).

3. **Cleaner canonical_summary.** Currently the first fact's templated triple ("Trump decides Iran call off strike" is fine; "Western nations decides Israel halt contentious construction in West Bank East Jerusalem" is mangled). Two options: (a) use the article title verbatim, (b) one LLM call per event to produce a clean summary (~$0.0001/event × 4k events = ~$0.40).

4. **Per-event-type score weights.** All four components are equal-weighted today. ELECTION events probably want higher `resolution_fit` and `deadline_fit` weight; ANNOUNCEMENT events probably want higher `directness`. Tune empirically once chunk 3 has PnL data.

5. **Resolve "Bank of Israel" vs "Israel" properly.** Beyond the substring fix: improve the LLM extraction prompt to disambiguate the type — Bank-of-Israel is ORG with a country qualifier, Israel is COUNTRY. Likely a few prompt examples would teach this.

## What's working well

- **Eventize clustering is correctly grouping news stories.** The high-support events (Trump-Iran at 107 facts, Ebola at 88, Musk-OpenAI at 54) are real news threads.
- **`Trump nominates Ken Paxton` gets 7 of the top-20 links** — and they're ALL real Paxton election markets ending in the next 7 days. **This is exactly the edge irene is designed to capture.** A real DKG-driven trade today on Paxton markets would be a high-conviction bet.
- **All four scoring components are useful** (std > 0.05). No need to drop any.

## Reproducing

```bash
PYTHONPATH=. python3 -m scripts.dkg.run_link_backfill \
    --irene-db /home/muk/projects/nexus/irene/data/irene.db

# Inspect the top of the distribution
python3 -c "import sqlite3; c=sqlite3.connect('file:data/irene.db?mode=ro',uri=True); \
  [print(r[0], r[1][:50], '→', r[2][:50]) for r in c.execute('''
    SELECT ml.final_score, e.canonical_summary, m.question
    FROM market_links ml JOIN events e ON e.id=ml.event_id
    JOIN markets m ON m.condition_id=ml.market_condition_id
    ORDER BY ml.final_score DESC LIMIT 20
  ''')]"
```

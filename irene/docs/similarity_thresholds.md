# Similarity thresholds — the three knobs, two unit systems

> Companion to [`theory.md`](theory.md). Spawned from the
> `sizing-edge-similarity` plan (see
> [`plans/sizing-edge-similarity.md`](../plans/sizing-edge-similarity.md))
> after the 2026-05-25 unstructured RAG batch recommended a
> "0.85–0.90" similarity cutoff for autonomous news matches. That
> number is in **cosine similarity**, but our most-talked-about
> threshold (`cfg.dedupe.breaking_distance = 0.55`) is in **cosine
> distance**. The two numbers look comparable but are measuring
> different quantities on different scales. This doc nails down
> which knob means what so the next reader doesn't crank a recall
> floor up to a precision-style value.
>
> - **Created:** 2026-05-25
> - **Status:** Reference. No code change attached; thresholds in
>   `app/config.py` are unchanged.

---

## 1. Cosine similarity vs cosine distance

For two **unit-length** embedding vectors `u` and `v`:

```
cosine_similarity(u, v)  =  u · v                in [-1, +1]
cosine_distance(u, v)    =  1 - cosine_similarity(u, v)   in [0, 2]
```

In practice both irene's `MarketIndex` and Chroma return values in the
positive half (sentence embeddings essentially never land in negatives
for real-world text), so we can treat the working ranges as:

- similarity ∈ [0, 1] — **higher = more alike**.
- distance ∈ [0, 1] — **lower = more alike**.

Conversion is symmetric: `distance = 1 - similarity` and
`similarity = 1 - distance`. The danger is that "0.55" reads as
"medium-high" in similarity land and as "fairly permissive" in
distance land — opposite meanings, same digits.

## 2. The three floors in our config

| Knob                                | Default | Units                | Defined in                                                          | Read at                                                                                | Gate it drives                                                                                          |
|-------------------------------------|---------|----------------------|---------------------------------------------------------------------|----------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| `cfg.match.similarity_floor`        | 0.50    | cosine **similarity** | `app/config.py:155` (MatchSection)                                  | `app/stages/match.py:106` → passed as `floor=` to `MarketIndex.topk` (`markets.py:308`) | Candidate-pool floor: which markets even *enter* the signals table for a given article.                  |
| `cfg.trade.min_similarity`          | 0.35    | cosine **similarity** | `app/config.py:237` (TradeSection)                                  | `app/stages/trade.py:268`                                                              | Trade-stage similarity_floor gate. **Effectively dead in production** — see §2.1.                       |
| `cfg.dedupe.breaking_distance`      | 0.55    | cosine **distance**   | `app/config.py:142` (DedupeSection)                                 | `app/stages/dedupe.py:228` → consumed by `_label(...)` at `dedupe.py:353`              | Freshness label: nearest-neighbour distance ≥ this ⇒ BREAKING; below this ⇒ EVOLVING / REHASH / STALE.  |

> **Plan-doc correction.** The `sizing-edge-similarity.md` table
> referred to a knob named `cfg.match.min_similarity`. That knob does
> not exist. MatchSection only exposes `similarity_floor` (0.50) and
> `top_k` (8). The 0.35 floor lives on TradeSection as `min_similarity`
> — there are **three** floors, not four, and they split across two
> sections.

### 2.1 Why `cfg.trade.min_similarity` is effectively dead code

`MatchStage` calls `MarketIndex.topk(query_vec, k=top_k, floor=self._floor)`
where `self._floor = cfg.match.similarity_floor = 0.50`. `topk` (see
`markets.py:330-336`) breaks out of its scan the moment a candidate's
similarity drops below `floor`. So no row with `signals.similarity < 0.50`
is ever inserted, which means `trade.py:268`'s check
`float(similarity) < self._cfg.min_similarity` (`= 0.35`) cannot fire
in normal operation — the comparison is always between something ≥ 0.50
and 0.35.

The gate is still real code, has tests
(`tests/stages/test_trade.py:318` and friends), and would re-arm if
either (a) `cfg.match.similarity_floor` were lowered below 0.35 or
(b) signals were published through a non-`MatchStage` path. We are
**not deleting it** — it is cheap defence-in-depth and the test
coverage protects against the gate being silently removed before
either of those conditions changes. But anyone reading the gate
cascade should know it is currently a no-op in steady state.

## 3. The RAG number maps to distance, not similarity

The unstructured batch (`questions/unstructured answers.md`, line 77)
recommends:

- **0.85–0.90 cosine similarity** for "definite autonomous matches"
- **0.80–0.90 cosine similarity** for "human review"

Both numbers describe a **dedupe / news-coreference** decision —
"are these two articles the same news event?" — which in our code
is governed by `cfg.dedupe.breaking_distance`. But our threshold is
in **distance**, so the unit-correct mapping is:

```
RAG similarity 0.85  ≡  distance 0.15
RAG similarity 0.90  ≡  distance 0.10
Our default     ≡  distance 0.55  (≈ similarity 0.45)
```

So the RAG is recommending **0.10–0.15 distance** for autonomous
coreference; our `breaking_distance = 0.55` is roughly four times more
permissive. That gap is mostly **definition** (we cluster topic-level
coverage across outlets, not strict wire rewrites) and partly
**model scale** (MiniLM-L6-v2 sits lower on the similarity scale than
the larger models the literature typically benchmarks on — see the
inline comment at `dedupe.py:92`). The plan recommends an offline
replay before tightening; do not change the default from this doc alone.

## 4. WARNING — do not push the recall floors to the precision number

**Do NOT raise `cfg.match.similarity_floor` (0.50) or
`cfg.trade.min_similarity` (0.35) toward 0.85 because the RAG batch
said "0.85".** Those two floors are *recall* gates on the
article → market candidate pool. The RAG's 0.85 is a *precision* gate
on news-event coreference (a completely different decision). Pushing
either recall floor to 0.85 would collapse the candidate pool to near
zero and kill the trade pipeline.

The same applies in reverse for `cfg.dedupe.breaking_distance`: it is
a *precision* knob in distance units; do NOT mechanically lower it from
0.55 toward 0.10 without a `scripts/replay/`-style experiment on a
fixed article window. The plan spells out the replay design
(parallel run over {0.55, 0.40, 0.25, 0.15} with manual labels on
cluster-membership changes).

If a future cycle wants to move any of these three numbers, the
sequence is: (a) gold-set replay over recent signals/articles,
(b) precision/recall plot, (c) update the default, (d) update this
doc. Default changes also fall under the
[`architecture.md`](../architecture.md) 30-day-soak rule.

## 5. Cross-references

- [`plans/sizing-edge-similarity.md`](../plans/sizing-edge-similarity.md)
  — the parent plan; "Findings → Apparent contradictions" section
  introduced the unit-mismatch framing.
- [`questions/research_questions.md`](../questions/research_questions.md)
  question **O-F3** — open research question on the academic
  literature for news-to-event matching thresholds. The note at the
  bottom of F3 points back here for the unit clarification.
- [`questions/research_answers.md`](../questions/research_answers.md)
  Section F (when populated) — RAG validations of the sibling
  defaults `kelly_fraction` and `min_edge`; F3 is the
  similarity-floor entry that this doc unpacks.
- [`theory.md`](theory.md) §1 (edge model) — the edge gate downstream
  of these floors; the similarity floor decides *which* markets reach
  the edge gate, the edge gate decides *whether to trade them*.
- Inline anchor: `app/stages/dedupe.py:90-94` — the source-of-truth
  comment justifying the MiniLM-specific 0.55 distance default.

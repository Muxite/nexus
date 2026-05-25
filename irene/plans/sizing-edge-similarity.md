# Plan — sizing / edge / similarity config

> Source: `irene/questions/unstructured answers.md` lines 67-81, batch
> `2026-05-25-unstructured-1`. Companion to `closer-fees.md`.

## Context

This slice covers four config-adjacent topics: Kelly fraction, minimum
edge, news-similarity thresholds, and soak/evaluation methodology. Unlike
the fee slice, this batch is **mostly statistical** — empirical ranges
from RAG-summarised industry literature. It is NOT a source of hardcoded
config defaults. The useful payload is in three parts:

1. **Validation** of our existing 0.25 / 0.04 defaults as literature-consistent
   (line 71, line 74) — a small but non-trivial win because both numbers
   were chosen on first-principles grounds without prior backtest support.
2. **One apparent contradiction** between the RAG's recommended
   similarity range (0.85–0.90, line 77) and our `cfg.match.min_similarity`
   (0.35). On inspection this is a **definition mismatch**, not a real
   disagreement — we have three different similarity floors and the RAG
   range is about a *fourth* concept (dedupe-confirmation cutoff for
   "same news event"). See below.
3. **Process seeds** — soak duration, heavy-tailed-loss warning,
   sub-100ms arb closure window. None of these belong in config; all of
   them should shape how we run the cycle-8+ soak.

Trust convention from `research_answers.md`: 📊 = statistical seed,
not a config default. The entire slice is dominated by 📊 claims.

---

## Findings

### Validations of current config — literature-consistent (✅)

- `cfg.kelly_fraction = 0.25` (`app/config.py:288`) matches the
  "quarter Kelly" convention recommended for binary prediction markets
  (line 71). The cited rationale: full Kelly on binary options has a
  **33% mathematical probability of halving bankroll before doubling**
  (line 71). Quarter Kelly is described as an "optimal anchor to
  maximise logarithmic growth while heavily mitigating probability
  estimation errors and model inaccuracies" (line 71).
  **Action:** none. Note in `theory.md` that the choice has external
  backing.

- `cfg.trade.min_edge = 0.04` (`app/config.py:239`) sits inside the
  cited "3-5% required edge" band (line 74) and matches the
  "**4% optimal-target threshold**" used by Kalshalyst-style bots
  (line 74). The RAG frames 4% as a buffer against fees + slippage +
  transactional friction (line 74).
  **Action:** none on the default. Reframe the cycle-7 fee gate
  rationale in `theory.md` as "we satisfy the literature band by
  construction".

### Seeds only — measure ourselves; never hard-code (📊)

- **Win rate >85%** for "grinding" prediction-market arb (line 80).
  Useful as the empirical W2 cap from the philosophy critique
  (`p_hat_cap ≈ 0.85`, see `research_answers.md` §W2). We should *measure*
  our realised win-rate per cycle and compare against this prior, not
  encode 0.85 as a confidence cap until our own data confirms it.
- **1–3% per-trade return** for the grinding regime (line 80). Useful as
  a sanity band for the realised-PnL distribution; outliers above 3%
  should be inspected for fee/spread mis-accounting.
- **30-day live-soak minimum** for confidence in edge and risk
  management (line 80). Adoptable as a **process rule**, not a config.
- **Average latency-arb opportunity closes in ~2.7 seconds** (line 80)
  and "sub-100ms execution dictates profitability more than predictive
  accuracy" (line 80). Both are 📊 seeds for the latency-repricing /
  closer-timeout discussion — they should be quoted in
  `philosophy.md`'s budget section but not committed to config.

### Apparent contradictions — actually definition mismatches (⚠️)

The RAG recommends a similarity cutoff in **0.85–0.90** for "definite
autonomous matches" (line 77), with 0.80–0.90 flagged for "human
review" (line 77). Our config has **three** similarity-adjacent floors;
none of them is the one the RAG is describing.

| Knob                                  | Value                | Where             | Semantics                                                            |
|---------------------------------------|----------------------|-------------------|----------------------------------------------------------------------|
| `cfg.match.similarity_floor`          | 0.50                 | `match.py`        | Candidate-pool floor: which markets *could* relate to this article.  |
| `cfg.trade.min_similarity`            | 0.35                 | `trade.py:268`    | Trade-gate floor: redundant second check before sizing.              |
| `cfg.dedupe.breaking_distance`        | 0.55 *(cosine dist)* | `dedupe.py:228`   | News-coreference cutoff: "is this the same story?" (≈0.45 sim).      |
| *(RAG-described knob, line 77)*       | 0.85–0.90 *(sim)*    | *not in our code* | Autonomous coreference cutoff for dedupe of identical news events.   |

The RAG's range maps to **`cfg.dedupe.breaking_distance`** by intent
(both are "is this the same news event?"), but the units flip — our
threshold is **cosine distance** (= `1 − cosine_similarity`), so
RAG's 0.85–0.90 similarity corresponds to a distance of **0.10–0.15**.
Our current 0.55 cosine distance (≈0.45 cosine sim) is much more
permissive than the RAG range.

**Why the apparent gap is not a bug:**

1. The RAG is talking about strict autonomous coreference for news
   wires (AP rewrites of the same Reuters story). We are clustering
   *articles about a topic* (the same political event covered by AP,
   Axios, FT), where lexical/embedding overlap is naturally lower.
2. The dedupe stage does not act on a hard cutoff — it labels into
   four freshness buckets (BREAKING / EVOLVING / REHASH / STALE_RETURN,
   see `dedupe.py:34-37`), and the trade stage downstream takes the
   bucket as evidence, not as ground truth.
3. The MiniLM-L6-v2 embedding model has a different similarity scale
   than the larger embeddings the academic literature typically uses;
   0.55 cosine distance on MiniLM is comparable to ~0.15 cosine
   distance on a stronger model. (See `dedupe.py:92` for the
   "headline-rewrite vs unrelated-news divide on MiniLM-L6-v2"
   comment that pinned the 0.55 default.)

**Action:** do NOT raise 0.35 or 0.50 toward 0.85; do NOT lower
`breaking_distance` toward 0.10 without an offline replay. Instead,
clarify the four-way semantic split in a docs note so future readers
don't conflate them.

### Stale (⚠️)

- The "average arb opportunity closes in 2.7s" claim (line 80) lives
  next to the "sub-100ms execution dictates profitability" claim (line
  80). The second contradicts the first as a budget — 2.7s is well
  above 100ms. The most charitable read: the *opportunity window* is
  2.7s wide, but you need sub-100ms turnaround to capture the *first
  fill* before competition. This matters for the philosophy.md budget
  discussion, not for this plan.
- The 30-day soak claim (line 80) overlaps with `research_questions.md`
  question O-F4 ("how long a soak does the literature suggest before
  claiming profitability with confidence?"). This batch effectively
  answers O-F4 with "30 days".

---

## Proposed changes

### `irene/docs/theory.md` — annotate existing defaults

Add a short subsection ("Literature alignment, 2026-05-25") noting that
`kelly_fraction = 0.25` and `min_edge = 0.04` are now literature-backed
(per this batch). Cite the file + line numbers. Keep the framing
careful: literature backing ≠ proof of profitability for *our* setup —
the 30-day soak is still the source of truth.

### `irene/docs/` — clarify the four-way similarity-threshold semantics

Either add a short note in `theory.md` or a dedicated
`docs/similarity-thresholds.md` covering the four knobs in the table
above. The point is to prevent a future implementer reading the RAG
slice and blindly raising `cfg.match.min_similarity` from 0.35 to 0.85
— that would kill recall on the trade gate without addressing the
intended issue (news-coreference precision).

### `irene/questions/research_answers.md` — open Section F

This batch is the first RAG content for Section F (questions O-F1,
O-F2, O-F3, O-F4). Add four entries:

- **F1** (Kelly fraction): ✅ — quarter Kelly is the literature anchor.
  Plan stub: no change to default; document the validation.
- **F2** (edge floor): ✅ — 4% is at the top of the recommended 3–5%
  band. Plan stub: no change to default; document the validation.
- **F3** (similarity floor): ⚠️ — RAG range applies to a different
  knob than the one in our config (see definition-mismatch table).
  Plan stub: docs clarification; do NOT change `min_similarity` or
  `breaking_distance` without replay.
- **F4** (heavy-tailed-arb soak): 📊 — 30-day soak is the recommended
  validation horizon. Plan stub: adopt as a process rule.

### Process changes — no code

- **Soak rule for default changes.** Any future change to
  `kelly_fraction`, `min_edge`, or `min_similarity` requires either
  (a) a 30-day live soak with the new value, or (b) a replay
  experiment over an equivalent snapshot DB. Encode this in
  `architecture.md` near the "config-default change" workflow.
- **Realised-win-rate reporting.** When the soak completes, write
  `reports/YYYY-MM-DD-edge-calibration.md` comparing measured
  win-rate to the 📊 85% claim (line 80), and the realised per-trade
  return distribution to the 1–3% claim (line 80).
- **Cross-link the latency seeds.** When the latency-repricing plan
  references "2.7s opportunity window" or "sub-100ms required" (line
  80), cite this batch as the seed.

### Code changes — minimal, all gated

- **NO change** to `cfg.kelly_fraction` (literature backing for current
  value).
- **NO change** to `cfg.trade.min_edge` (literature backing for current
  value).
- **NO change** to `cfg.match.similarity_floor` or
  `cfg.trade.min_similarity` from this batch alone; the RAG range
  does not apply to either knob.
- **NO change** to `cfg.dedupe.breaking_distance` from this batch
  alone; raising it toward the RAG range would require an offline
  replay over recent BREAKING-labelled signals to confirm we don't
  collapse legitimate distinct stories into one cluster. If a
  follow-up plan decides to attempt this, it should be gated behind
  `scripts/replay/` evidence (parallel run of {0.55, 0.40, 0.25,
  0.15} on a fixed article window with manual labelling of any
  cluster-membership changes).

---

## Open questions

- Are we already logging realised per-trade win-rate per cycle?
  (i-A1, i-E1 in `research_questions.md` partially cover this, but
  neither names "win rate" as the metric.) If not, that's the
  prerequisite measurement before either the 85% or 30-day claim can
  bind to a config change.
- Where in the RAG source does the "2.7s avg arb opportunity" number
  come from (line 80)? If it's traceable to a published study, we
  should cite that study in the philosophy.md budget paragraph. If
  it's purely the RAG's synthesis, mark it 📊 priors-only.
- Does `cfg.match.similarity_floor = 0.50` and `cfg.trade.min_similarity
  = 0.35` create a redundant gate? If match.py never publishes signals
  below 0.50, the trade-gate floor at 0.35 is dead code. Worth a
  one-shot audit of `signals.similarity` distribution before next
  cycle.

---

## Verification

- Any future threshold change must replay against historical signals
  (`scripts/replay/accuracy.py`) before going live — same gate used
  for cycle 7's fee math.
- Default changes to `kelly_fraction` or `min_edge` require a 30-day
  small-position soak (📊 seed from line 80; adopt as process rule).
- Docs changes (`theory.md`, similarity-thresholds note) need only a
  read-through by the next handoff.

---

## Cross-cutting notes

- The **latency-repricing** plan (sibling) will independently reference
  the "sub-100ms" and "2.7s opportunity window" seeds (line 80).
  Duplication of citations across the two plans is fine — both should
  point back to this batch.
- The **heavy-tailed-loss warning** (line 80) compounds the W2
  weakspot from `research_answers.md` (`p_hat = 1.0 → ruin`). Both
  argue for an empirical p_hat cap in addition to fractional Kelly.
  The cap is a separate plan (W2 is its own item); this plan only
  validates the *fractional Kelly* leg of the defence.
- The **closer-fees plan** (`closer-fees.md`) and this plan both
  touch the per-trade economics. They are complementary: closer-fees
  fixes the *accounting*; this plan validates the *sizing parameters*
  the accounting feeds.

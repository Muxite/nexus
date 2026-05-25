# Plan — LLM choice, R-rate calibration, prompt cache (H1–H3)

> Source: `irene/questions/unstructured answers.md` lines 109-148, batch
> `2026-05-25-unstructured-1`. Cross-refs: `research_answers.md` §W1, W3,
> W4, W5, W7; `research_questions.md` §P2.H (O-H1, O-H2, O-H3) and §P1.H
> (i-H1, i-H2).

## Context

Three loosely-related but co-located concerns: (1) LLM model choice via
public benchmark numbers for Haiku 4.5 / Gemini 2.5 Flash / GPT-5 Mini /
DeepSeek V4 Flash (lines 115-127); (2) asymmetric-classifier calibration
for the 70-80 % reject-rate (lines 128-130); (3) prompt-cache mechanics
— exact prefix matching, ≥1024-token minimum, 128-token block increments
(lines 131-134).

Hard constraint: saved memory says *"prediction stays on 2.5-flash;
3.1-flash-lite-preview broke trade volume on 2026-05-21"*. **This plan
does NOT propose a production model swap.** It proposes an A/B probe
lane, a cache reality-check, and a calibration experiment — all of which
run alongside the live `google/gemini-2.5-flash` path (`config.yaml:22`)
without changing it.

LLM shim: `app/ai.py` — `AIClient.direction(...)` at lines 367-420,
`_SYSTEM_MESSAGE` at lines 106-117, `_LOGIT_BIAS_YNU` at line 125,
`max_tokens=32 / temperature=0` in `_call_openrouter` at lines 467-471.

## Findings

### Protocol facts (✅ act on these, with caveats on the specific numbers)

- **Prompt cache requires ≥1024 static tokens; cached chunks are
  appended in 128-token increments** (lines 132-134). Cacheable ceiling
  = `⌊x/128⌋ × 128` for `x` static tokens above the 1024 floor.
  Independently corroborated by `research_answers.md` W7 (Anthropic
  explicitly documents a 1024-token minimum).
- **Cache placement rule:** static prefix at the **absolute beginning**,
  dynamic suffix at the end (lines 132-133). Our current `_SYSTEM_MESSAGE`
  is correctly placed first, but is well under the threshold.
- **Asymmetric reject-rate is a known failure mode** when misclassification
  cost vastly outweighs abstain cost (line 128). Named calibration
  techniques: **Isotonic regression, Platt scaling, conformal prediction**
  (line 129). All three are off-the-shelf in scikit-learn / `mapie` —
  no novel research needed.
- **Claude Haiku 4.5 leads on TTFT, Gemini 2.5 Flash leads on $/token,
  DeepSeek V4 Flash leads on raw cost, GPT-5 Mini leads on JSON
  reliability** (lines 115-127). Use this table for *triage*, not as
  source-of-truth.

### Seeds only (📊 — verify locally before any production change)

- **Claude Haiku 4.5 ~597 ms TTFT** (line 116) — public benchmark
  snapshot. Real number for our prompt, region, and OpenRouter routing
  will differ.
- **Gemini 2.5 Flash ~1,238 ms total / ~223 t/s** (line 119) — same
  caveat. Our `predictions` table + dashboard already records per-call
  `latency_ms` (see `main.py:198`); the real distribution is the source
  of truth.
- **Cost table** (lines 117-127): useful for the budget projection
  spreadsheet (`budget.monthly_cap_usd = 50.0`, `config.yaml:25`) but the
  A/B probe surfaces the actual per-call cost we'd pay.

### Validates an existing distilled finding

- **W7 — "prompt cache myth"** (`research_answers.md:357-366`). The
  unstructured batch independently states the 1024-token minimum, which
  proves the `philosophy.md:133` claim *"System message is tiny and
  static (cached by the provider)"* is wrong at our prompt size. Our
  `_SYSTEM_MESSAGE` (`ai.py:106-117`) is ~120 tokens — nowhere near
  cacheable. The dashboard `cache_hit` metric in
  `app/dashboard/pages/main.py:208-217` measures our **internal** SQLite
  `llm_calls` cache (prompt_sha matching), not provider-side prefix
  cache — the two are unrelated.

### Cross-validates philosophy critiques

- **W3 (logit_bias raw chars):** our `_LOGIT_BIAS_YNU` (`ai.py:125`)
  uses raw characters, which most providers ignore — already conceded
  in the comment on line 123. Confirm it's a no-op before redesigning
  any prompt around it.
- **W1 (serial LLM calls)** and **W4 (OpenRouter routing tax)** dominate
  the latency picture far more than model choice does — 600 ms Haiku vs
  1200 ms Gemini matters little if we make 25 serial calls per article.
  The A/B probe must eventually be paired with the W1 fix; this plan
  scopes only the per-call A/B.

## Proposed changes

### 1. A/B latency + accuracy probe lane (the safe substitute for a model swap)

Scope: every `ai.direction(...)` call that succeeds gets a sampled
shadow call against one or two challengers; results are logged but
**never used for trade decisions**. Production direction call remains
unchanged.

Shape:

- New optional config block in `config.yaml`:
  ```yaml
  openrouter:
    direction_model: google/gemini-2.5-flash   # unchanged
    probe:
      enabled: false                # default off; flip in one shot
      sample_rate: 0.10             # 10 % of production calls
      challengers:
        - anthropic/claude-haiku-4.5
        - deepseek/deepseek-v4-flash    # tentative — pick the cheapest
  ```
- New table `llm_probe_calls` (mirror of `llm_calls` plus
  `production_call_id`, `production_label`, `challenger_label`,
  `agreement` boolean). DDL registered in `app/ai.py` next to `_AI_DDL`.
- New helper `AIClient._probe_shadow(prompt, prod_result)` invoked after
  the production call returns. Fire-and-forget on a bounded
  `asyncio.Semaphore(4)` so it never blocks the hot path.
- Reuse the existing `_call_openrouter` pattern — same JSON envelope,
  same `_parse_response`, same `_SYSTEM_MESSAGE`. The only difference is
  the `model` field on the outgoing payload.

Reporting (lightweight, batched):

- Add a `throughput.txt` section `LLM probe (24h)` with one row per
  challenger:
  - TTFT p50 / p95 (proxied: total latency, since OpenRouter exposes no
    TTFT directly — flag this caveat in the report header).
  - Label-agreement % vs production.
  - Per-call $/1k input + $/1k output (read from provider pricing table
    we already hold for budget math).

Decision rule (documented up-front, not coded):

- Promote a challenger to production **only** if (a) total latency p95
  is ≥ 30 % lower AND (b) label agreement on Y/N rows is ≥ 95 %. The
  R-rate gap allowance is documented separately — see §3 below.
- Until both gates are clearly met across ≥ 1 week of probe data, the
  production model stays on `google/gemini-2.5-flash`.

### 2. Prompt-cache decision (binary: drop the claim, or actually invest)

Two coherent options. Pick one — running both wastes effort.

**Option A — drop the claim, status quo on prompt size.**
- Flag `philosophy.md:133` (*"cached by the provider"*) for human review
  — philosophy.md is read-only, do not auto-edit. Add a one-line comment
  on `main.py:208-217` clarifying our `cache_hit` metric is the
  *internal* SQLite cache, not provider-side prefix cache.
- No code change; latency budget assumptions unchanged.

**Option B — actually enable prefix cache.**
- Restructure the prompt so the static prefix exceeds 1024 tokens. The
  natural extension is a static "Polymarket reference card" prepended
  to the system message: fee schedule (closer-fees plan output),
  category taxonomy, common UMA resolution conditions, abbreviation
  glossary. Aim for ~1300 static tokens (≥ 1024 floor + one 128-block
  buffer).
- Place dynamic content (Condition + News) strictly after the static
  block. Confirm with one provider that `messages[0].content` static +
  `messages[1].content` dynamic actually engages the cache; some
  providers cache only `system` role text.
- Caveats Option B must clear before merge:
  - **Input cost goes up** (we pay full price on the first call of each
    cache window, then cache discount on the rest). Cost model needs to
    project whether the per-article LLM cost rises or falls net.
  - **TTFT measurably improves** on cache-hit calls. If the probe
    (§1) shows it doesn't, Option B isn't worth the prompt complexity.
  - The static reference card has to stay genuinely static — any field
    that changes (fee param re-publication, new category) busts the
    cache. Versioning the reference card is fine, but every version bump
    pays the cold-path cost.

**Recommendation:** Option A first. The truth-in-docs fix is free and
prevents downstream confusion. Then run an Option B experiment in the
A/B probe lane (§1) before deciding to merge it — `philosophy.md` should
not gain *another* unverified caching claim.

### 3. R-rate calibration (the 70-80 % reject question)

Cycle-7 onwards we've seen ~70-80 % R / U-rate on direction calls. The
unstructured batch confirms this is *the* canonical failure mode for
cost-asymmetric classifiers (line 128). Two interventions, run in order:

**Step 3a — stratification first (free).** Run query i-E2 from
`research_questions.md`: split R-rate by `(article source, market
category, cosine bucket)`. If R-rate varies by a factor of 2 or more
across slices, the gate *before* the LLM (similarity / freshness /
entity-overlap) is the right knob and calibration is treating the
symptom. If R-rate is roughly uniform, it's a prompt or model issue and
calibration is worth running.

**Step 3b — Platt scaling on logits (if 3a says so).** Of the three
named techniques, **Platt scaling** is the cheapest to wire up:
- Pull `predictions` rows that have a hand-labeled ground-truth outcome
  (we can use `price_impacts` resolved markets as proxy ground-truth
  for the Y/N call: did the market actually resolve in our direction?).
- Fit a logistic regression on `(model_label, confidence_digit)` →
  `was_correct`.
- Apply the fitted transform **post-hoc** to predictions — adjusts the
  effective decision threshold without changing the LLM call.
- Run as an offline experiment first; if the F1 / precision improves at
  matching recall, **then** consider whether to wire it into the gate
  cascade.

**Isotonic regression / conformal prediction** are stronger but
heavier — gate them on whether Platt scaling shows a meaningful lift.

**Hard constraint:** calibration changes the *interpretation* of LLM
output, which is conceptually a `prompt_version` bump (see
`ai.py:349 PROMPT_VERSION`). Frame any production wiring as a new
prompt version so the cache invalidates cleanly and the dashboard
distinguishes pre- vs post-calibration metrics.

## Open questions

- **What is our current internal cache-hit rate per provider?** The
  cycle-7 handoff cites ~21 %. Re-check on a fresh dashboard sample —
  the `_LABEL_TO_DIRECTION` change in v6 may have shifted prompt_sha
  diversity.
- **Does OpenRouter pass `cache_control` directives through to each
  upstream provider consistently?** Anthropic, Gemini, and DeepSeek each
  document the parameter differently. W4 suggests we drop OpenRouter
  eventually (direct provider calls) — if we go Option B, that decision
  may need to come first.
- **Is the gold set large enough for Platt scaling?** We need a few
  hundred labeled rows minimum to fit a stable sigmoid. If
  `price_impacts` doesn't have enough resolved-market rows yet, defer
  Step 3b until cycle 8+ has accumulated more.
- **Does `logit_bias` actually do anything on Gemini-via-OpenRouter?**
  Verify via W3's stub before designing any prompt restructure for
  Option B.

## Verification

- **Probe lane (§1):**
  - ≥ 1 week of paired calls at 10 % sample rate (≈ thousands of pairs
    given current call volume).
  - Surface in `throughput.txt`: TTFT (proxy) p50 / p95 per challenger;
    label agreement on Y/N rows; $ projection per 1k articles.
  - No production trade decision is allowed to consult probe rows.
- **Cache rewrite (§2 Option B, if pursued):**
  - Measure provider-reported `cache_creation_input_tokens` /
    `cache_read_input_tokens` (Anthropic exposes these explicitly).
  - Latency delta on cache-hit vs cache-miss calls must be ≥ 100 ms p50
    to justify the prompt redesign.
- **Calibration (§3):**
  - Held-out gold set: precision / recall at matching F1 vs uncalibrated.
  - Required lift to justify wiring into the gate: ≥ 5 absolute points
    of precision at equal recall, or ≥ 10 % reject-rate reduction at
    equal precision.

## Cross-cutting notes

- **Read-only files touched conceptually but NOT edited:**
  `irene/docs/philosophy.md` (Option A flags lines around 133 for human
  review). Do not auto-edit; surface in the cycle handoff for the user
  to review.
- **W1 (serial LLM calls)** and **W4 (OpenRouter routing tax)** from the
  philosophy critique batch dominate the LLM latency picture — model
  swap alone won't fix the budget. Whichever plan picks up W1 should
  cross-reference this one so the parallelism work doesn't pre-empt the
  probe lane data we need.
- **W3 (logit_bias raw chars)** needs the separate verification its own
  stub describes. This plan reuses the current bias structure for the
  probe (§1) so probe results are directly comparable to production —
  but the moment W3 lands a fix, the probe should re-baseline.
- **No `config.yaml` production value changes** in this plan beyond
  adding the optional `openrouter.probe` block (default `enabled: false`).
  `direction_model` stays on `google/gemini-2.5-flash` per the saved-memory
  constraint until probe data clearly says otherwise.
- **Unblocks (from `research_questions.md`):** O-H1 (model benchmarks),
  O-H2 (asymmetric classifier calibration), O-H3 (cache ceiling), i-H1
  (cache-hit potential), i-H2 (per-model dry-run), i-E2 (R-rate
  stratification), handoff #4 (R-rate).

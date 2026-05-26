# Handoff H — LLM context richness (ease off input truncation)

**Date:** 2026-05-26
**User directive (verbatim):** "ease off on token limits for inputs to llm."
**Reference:** `irene/handoffs/2026-05-26-A-iran-funnel-audit.md`. The one Iran-related signal that reached the LLM returned `NEUTRAL`. Likely cause: the LLM saw a 1-line headline + truncated summary against a vague market question and couldn't make a confident call.
**Dependency:** none. **Most valuable AFTER handoff B** (which actually gives the LLM things to look at).
**Blocks:** the "should we lower min_confidence?" decision — better to give the model more context first, then decide if it still over-abstains.

## The problem

Today's `compose_direction_prompt` (`app/stages/trade.py:282-318`) builds a 4-field prompt:

```
NEWS HEADLINE: <article.title>
NEWS SUMMARY:  <article.summary>

MARKET QUESTION: <market.question>
MARKET DETAILS:  <market.description>

Read the news as plain language. Does the news DECISIVELY resolve this market?
  YES — …  NO — …  NEUTRAL — …
`confidence` is how CLEARLY the news resolves the market …
Output JSON ONLY.
```

`title` and `summary` come from the scraper (often a 1-sentence summary or RSS description). `description` from Polymarket Gamma. The OUTPUT is capped at `max_tokens=1` with logit-bias for Y/N/P/U/R (cycle-7 design — that single-token output is intentional and stays). But INPUT has no enforced cap — what's actually short is the upstream `summary` field and the LLM's lack of supporting context.

Result: when a "U.S. self-defense strikes" article hits a "Will the Iranian regime survive U.S. military strikes?" market with a 1-sentence summary, the LLM can't tell whether *this specific strike* resolves the question — too little context → NEUTRAL.

## What's already there (reuse)

- **scraper-v2 has `/scrape mode=article`** — returns trafilatura-cleaned full article body (no boilerplate). See `services/scraper-v2/README.md`. Already used elsewhere by irene's combined-source flow.
- **`predictions.raw_response`** already captures the full LLM response (not just the parsed label).
- **`market` dict in compose_direction_prompt** has access to `end_date`, `volume_usdc`, `category` (cycle-23 `_fetch_market` widened) — none are surfaced to the LLM.

## Scope

### Step 1 — audit current prompt sizes

Before changing anything, measure. Add a one-off `scripts/audit/llm_prompt_size_audit.py`:

```python
# For the last 100 LLM-reached decisions, pull the prompt from llm_calls,
# compute char count + estimated token count (chars/4), and print
# percentiles (p10/p50/p90/p99).
SELECT prompt FROM llm_calls ORDER BY id DESC LIMIT 100;
```

Establishes the baseline. Without it we don't know if the prompt is 200 chars or 2000.

### Step 2 — enrich the prompt with fuller article body

Two-tier strategy:

**A. Use the article body when available.** scraper-v2's combined mode often produces a `body` field (full text) for HTML-mode articles. Confirm the `articles` table schema (check `app/stages/ingest.py`) for whether `body` or similar is stored. If yes:

```python
def compose_direction_prompt(article, market):
    title = article.get("title") or ""
    summary = article.get("summary") or ""
    body = article.get("body") or ""
    # Prefer body (trafilatura-cleaned) when available; fall back to summary.
    article_text = body if body and len(body) > len(summary) * 1.5 else summary
    # Cap at ~3000 chars (~750 tokens) to leave room for prompt overhead
    # without unbounded LLM cost on a long-form article.
    if len(article_text) > 3000:
        article_text = article_text[:3000] + " […truncated]"
    ...
```

**B. If `body` is NOT stored,** add it. Modify `ingest.py` to capture the body field from scraper-v2 responses when present. Migration to add `body TEXT NULL` column. Backfill is unnecessary — new articles only.

### Step 3 — expand market context

Today: just `question` + `description`. Add:
- `end_date` — lets the LLM reason about "is this resolution imminent?"
- `category` — when populated (still NULL from Gamma per cycle-23 finding, but defensive).
- `volume_usdc` — signals market liquidity.
- The 5 most recent decided trades on this market (if any) — gives the LLM a sense of recent market sentiment.

Sketch:

```python
return (
    f"=== NEWS ARTICLE ===\n"
    f"Headline: {title}\n"
    f"Source: {article.get('source')!r} ({article.get('source_detail')!r})\n"
    f"Published: {article.get('published_at')}\n"
    f"\n"
    f"{article_text}\n"
    f"\n"
    f"=== MARKET ===\n"
    f"Question: {question}\n"
    f"Resolution deadline: {market.get('end_date') or '(unknown)'}\n"
    f"Volume (24h): {market.get('volume_usdc') or '?'} USDC\n"
    f"Category: {market.get('category') or '(uncategorized)'}\n"
    f"\n"
    f"Details: {description}\n"
    f"\n"
    f"=== INSTRUCTIONS ===\n"
    # ... existing Y/N/NEUTRAL/confidence rubric ...
)
```

### Step 4 — soften the "default to NEUTRAL" guidance

Current prompt line: *"Default to this [NEUTRAL] whenever you're unsure."* That's a strong abstain prior. The user says the bot over-abstains. Replace with:

> "NEUTRAL — the news is unrelated, ambiguous, or genuinely doesn't push the market either way. Prefer YES or NO with a low confidence (e.g. 0.4) when you can see a directional lean even if the resolution isn't certain — the gate handles confidence filtering. Reserve NEUTRAL for the truly orthogonal cases."

This is the "tune how often we say yes or no instead of neutral" knob the user asked for in handoff F — implemented at the prompt level rather than the gate level. Pairs with F's tunable `min_confidence`.

### Step 5 — provider-level input cap

Check `app/ai.py` for any explicit max-input-tokens on the OpenRouter call. If present and conservative (e.g. <2000), raise to ≥4000. Gemini-2.5-flash (current model) supports very large contexts; the bottleneck is cost, not context window.

Add `cfg.openrouter.max_input_tokens: int = 4000` so it's a knob.

### Step 6 — preserve single-token output

The `max_tokens=1` + `logit_bias` design (ai.py:103) is INTENTIONAL — gives a free Y/N/P/U/R/? classifier output without parsing JSON. Do NOT change this. Only input context expands.

### Step 7 — tests

`app/tests/stages/test_trade.py` — extend the `compose_direction_prompt` test to:
- Assert `body` is preferred over `summary` when both present.
- Assert ~3000 char truncation works (no unbounded prompts).
- Assert market `end_date` + `volume_usdc` appear in the prompt.
- Assert "Output JSON ONLY" tail unchanged (downstream parser still works).

`app/tests/test_ai.py` (or wherever `direction()` lives) — assert the LLM call accepts the longer prompt and `max_tokens=1` still binds.

## Files

| Change | File |
|---|---|
| Audit script | `scripts/audit/llm_prompt_size_audit.py` (new) |
| Prompt builder | `app/stages/trade.py:282` (`compose_direction_prompt`) |
| Article body schema | `app/stages/ingest.py` (if `body` not yet stored) |
| Migration | `scripts/migrate/2026_05_26_article_body.py` (new) — `ALTER TABLE articles ADD COLUMN body TEXT` |
| Provider cap | `app/ai.py` + `app/config.py` (`OpenRouterSection.max_input_tokens`) |
| Tests | `app/tests/stages/test_trade.py`, `app/tests/test_ai.py` |

## Verification

1. **Baseline:** run the audit script before any change. Record the p50/p90/p99 prompt sizes for current LLM calls. Expect ~500-1500 chars.
2. `pytest app/tests/stages/test_trade.py app/tests/test_ai.py` — new prompt-builder tests pass.
3. **Post-deploy audit:** run the audit script again 1h after rebuild. p50 should rise (e.g. 500 → 1800 chars) reflecting fuller context.
4. **LLM verdict shift:** dashboard `labels (1h)` line — expect Y+N share to rise vs P+U+R as the model has more context to be decisive on. Pair with handoff B so this measurement is meaningful (no point measuring LLM behavior if the LLM is never called).
5. **Cost watch:** OpenRouter budget burn rate goes up proportionally to prompt size. Monitor `dashboard.txt` "AI subsystem remaining: $X.XX" over a 6h window. If the burn crosses 2× pre-fix rate, dial back `max_input_tokens` to 3000.
6. **Tracer verification (via handoff G):** burst `iran_strike` tracers with the new prompt; audit shows Y/N share rises substantially vs the NEUTRAL-dominant baseline.

## Coordinate with…

- **Handoff B** — H is the second multiplier on top of B. B unblocks the LLM seeing things; H gives the LLM enough context to be decisive about what it sees. Both together = the bot expresses views at the rate it should.
- **Handoff F** — F's `min_confidence` tuning question becomes moot if H raises the LLM's confidence on real signals. Land H first, audit the new confidence distribution via F's panel, then decide whether to tune.
- **Handoff G** — Iran tracer bursts are the fastest way to see H's effect in action. After H lands, re-burst with templates from G and compare label distribution.

## Subagent prompt suggestion

> Read `irene/handoffs/2026-05-26-H-llm-context-richness.md` end-to-end. Run the audit script first to establish the baseline prompt size. Edit `compose_direction_prompt` per the doc — fuller article body, expanded market context, softer NEUTRAL guidance. If `articles.body` doesn't exist, add the column + migration + update ingest to capture it. Add the OpenRouter `max_input_tokens` config knob. Add tests. Rebuild + restart. Re-run the audit script and report the prompt-size delta. Do NOT touch any other handoff scope. Do NOT change `max_tokens=1` output cap — that's the cycle-7 logit-bias classifier and must stay.

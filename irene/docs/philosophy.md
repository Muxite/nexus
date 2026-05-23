# Irene's philosophy — speed, not prediction

> **The single most important framing decision.** Every other choice in
> the codebase flows from this one.

- **Locked:** 2026-05-23
- **Status:** Authoritative. If anything else in the repo contradicts
  this, this file wins. Update everything that contradicts.

## The thesis in one sentence

**Irene does not predict anything.** Irene reads news, confirms the
wording is decisive for a known market, and buys before the market
reprices.

## What that means in practice

We are not a forecasting bot. We are not modelling the probability
distribution over an event. We are not refining anyone's prior.

We are an **interpreter-and-runner**:

1. **Interpret.** A news event lands. We use an LLM (and embedding
   similarity) to confirm two things: (a) which existing Polymarket
   markets this news affects, and (b) which way it pushes them. The
   LLM's job is **natural-language understanding** — does the article
   say "the FDA approved drug X" or "the FDA rejected drug X" — not
   probability estimation.
2. **Run.** If the wording is decisive AND we get to the trade before
   the market reprices, we win. If we're slow, the price has already
   moved and the edge is gone.

The edge is **time**, not insight.

## What this changes in the codebase

### What the LLM is doing (and what it isn't)

When `app/stages/trade.py` calls `ai.direction(signal_id, prompt)`, we
are NOT asking the model "what probability do you assign to YES?". We
are asking:

> "Given THIS news, does the market RESOLVE toward YES, RESOLVE toward
>  NO, or is it UNCLEAR / ambiguous / unrelated?"

The `confidence` field is **how clearly the news resolves the market**,
not the model's subjective probability. A confidence of `0.9` means
"this article very plainly tells us YES will happen / has happened" —
not "I forecast 90% probability of YES."

### What `direction_lowconf` actually gates

Same code, different intent: the `min_confidence` gate filters out
signals where the LLM couldn't read the news as decisively resolving
the market. It's a wording-quality gate, not a confidence-of-prediction
gate.

### Why "predictions" exists as a table name

History. The table `predictions` is owned by `app/ai.py` and stores the
LLM's direction call. We don't rename it — the column shapes are right
— but mental-model-wise it's a `nl_interpretations` table. Treat it
that way when reading code.

### Why p_hat looks like a probability

Computational convenience. `p_hat = confidence if direction=='YES' else
(1-confidence)` is how `trade.py` maps the LLM's "this clearly resolves
YES at confidence 0.9" into the same edge-and-Kelly math any bot uses.
The number isn't a forecast. It's a knob the size-and-edge layer
already understands.

## What this prioritises in operations

**Latency observability is the most important diagnostic.** If irene's
median article→trade latency is 30 seconds, we have no edge. If it's 2
seconds, we have a real shot. If it's 200ms, we're competing well.

Per-stage timing matters:
- `ingest.poll` — how soon after a feed publishes do we see the article?
- `dedupe.handle` — how long does Chroma similarity take?
- `match.handle` — how long does the cosine search + embed take?
- `ai.direction` — how long does the OpenRouter call take? (this is
  usually the dominant tail.)
- `trade.handle` — gate cascade + execute (the last few ms before the
  trade fires).

These five together are the "time-to-trade" budget. Every analysis script,
dashboard, and report keeps that number central.

## What this de-emphasises

- **Calibration.** We're not predicting probabilities, so calibration of
  LLM confidence is a second-order concern. It still helps the gate (a
  miscalibrated LLM lets bad trades through) but it isn't worth a
  six-month research project.
- **Forecasting accuracy on long-horizon markets.** Markets that resolve
  in months are not our edge — the price has time to absorb news. We
  thrive on short-horizon markets where the news IS the resolution.
- **Counterfactual replay against "what if the model were better".** Our
  replay harness reconstructs PnL from the captured trade state, not
  from re-running the LLM on the article. The LLM's call is the call;
  the question is whether we were fast enough to capitalise on it.

## What this means for the impact tracker

For every executed trade, we want to know: **did the market actually
move in our direction, and when?** The `price_impacts` table answers
this. If we trade YES at mid=0.30 and the price climbs to 0.55 within
five minutes, the thesis worked. If it stays at 0.30 for an hour and
then drifts away from us, either:

1. We were wrong about the wording (the news didn't resolve the
   market), OR
2. The market never repriced because nobody else read the news either.

Both outcomes are diagnostic. Both tell us something about our
detection layer and our market choice. Neither outcome is a forecasting
failure — they're observability signals.

## What this means for irene/seulgi merger

Seulgi grew the same way, with the same edge. The "interpret and run"
framing is identical between the two. The merger plan (see
`docs/irene_seulgi_merger.md`) assumes both bots are doing the same
thing; the differences are in implementation choice, not philosophy.

## How the LLM call is shaped (v2-fast)

Because the LLM is doing natural-language interpretation and not
probability estimation, we drop nearly all the call's tokens:

- **System message is tiny and static** (cached by the provider).
  "You are a high-speed financial resolution engine. Output exactly
  ONE character: Y / N / U."
- **User message is just `Condition: … / News: … / Output:`** — no
  chain-of-thought, no explanation request, no examples.
- **`max_tokens=1, temperature=0, logit_bias={'Y':100,'N':100,'U':100}`** —
  the provider returns the first character and stops. Time-to-final-
  token is ~10x lower than the old JSON-parse path.

The parser maps:

| LLM emits | direction | confidence |
|---|---|---|
| `Y` (decisive YES) | `YES` | `1.0` |
| `N` (decisive NO) | `NO` | `1.0` |
| `U` (unclear) | `NEUTRAL` | `0.0` |

`confidence` is fixed because in the new regime it represents
decisiveness (binary: decisive vs. not), not probability. The gate
cascade still uses it as a number because the downstream math is
correct either way; the *meaning* is interpretation strength.

## Why cheaper-per-call ⇒ more-markets-per-article

The old JSON-parse call returned ~250 tokens; the new path returns 1.
That's roughly an order-of-magnitude per-call cost cut. We spent the
savings on **more candidate markets per article**:

- `MatchSection.top_k` was 5 in P1. **It's 25 by default now.**
- Each candidate becomes its own signal → its own AI call → its own
  trade decision. With 5x the candidates we get 5x the chances to find
  a market the news decisively resolves.
- The cosine floor still gates which markets are even candidates, so
  bumping top_k doesn't degrade quality — it only increases coverage.
- The total cost per article drops despite 5x candidates because each
  call is ~1/100 the tokens-out it was before.

If a future provider supports it, we'd consider running the top_k
calls in parallel (bounded `asyncio.Semaphore`) to hide the per-call
latency under network. For now they're serialised in `trade.handle`'s
subscriber task.

## TL;DR for new readers

1. We don't forecast. We **interpret news, confirm market alignment, and
   buy fast**.
2. Time is the moat. Every stage's latency is a first-class metric.
3. The LLM emits **one character** (Y/N/U) per market-vs-news pairing.
   No chain-of-thought, no JSON, no reasoning text.
4. Cheap calls ⇒ more candidate markets per article. Default top_k is
   25, was 5.
5. The impact tracker is how we know we have edge: do prices move our
   way after we trade?

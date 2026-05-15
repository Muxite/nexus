# Open Questions for a Quant

A list of unresolved methodological / statistical / experience questions in the **seulgi** paper-trader that someone with low-frequency-trading background would have a better shot at than I do. Each question is self-contained: what we do, why it's questionable, and the data we already have.

---

## 1. What seulgi is

A news-driven paper-trader for Polymarket binary markets. Pipeline:

1. ~70 news watchers + Truth Social fire headlines into a queue.
2. The headline is embedded (`all-MiniLM-L6-v2`, 384d) and cosine-matched against the top-20k Polymarket markets by volume. Top-K above a similarity floor go to the AI.
3. An LLM (Gemini 2.5 Flash Lite, ensembled N=3 at temperature 0.5) emits a distribution `(p_yes, p_no, p_neutral, p_alpha, risk)` per market in one batched call. Ensemble is collapsed by majority vote on direction; confidence reported = mean `p_alpha` across samples.
4. Three engines (`aggressive`, `balanced`, `inverted_highconf`) read the same classification and apply their own thresholds. The edge model is `edge = edge_increment · (2·conf − 1)`, `p_hat = mid + edge`, fire when `edge ≥ min_edge`.
5. Positions monitored every 60s for TP / SL / trailing-stop / timeout / cold-cull. PnL deducted spread cost `spread_cost_bps`. SQLite stores everything.
6. A `price_impacts` table records `(t-300s, t-60s, t0, t+1m, t+5m, t+15m, t+30m, t+60m)` for every above-threshold (conf, sim) pair, regardless of whether it traded.
7. A `lessons` aggregator (`app/lessons.py`) re-buckets `price_impacts` every 10 min by `(source × market_type × conf_bucket × p_alpha × risk)`, computes a signed-IC proxy + capture rate, and writes prompt-time "be more careful here" notes back into the next AI batch.

We iterate in "cycles" (`CYCLE_METHODOLOGY.md`): snapshot DB → ship one or two structural changes → run ≥6h → postmortem. We're on cycle 8.

---

## 2. Current empirical state (data the quant can react to)

From the most recent postmortem with non-empty data (`data/postmortems/2026-05-14-0134/`, **n=964 classified rows over ~24h** after applying cycle-3 rail/similarity rules):

```
Outcome buckets (5m horizon, signed by predicted direction)
  no_signal  88.6%   (854/964)
  wrong_dir   5.1%   ( 49/964)
  captured    2.9%   ( 28/964)
  too_slow    3.4%   ( 33/964)

ic_global = −0.0067   (was −0.11 in 6h pre-rules cycle 3)
ic_by_market_type:
  sports           +0.078
  event            −0.025
  political        −0.012
  price_threshold  −0.043

Capture rate by AI confidence bucket
  [0.50, 0.55)   2.5%  (n=40)
  [0.55, 0.65)   3.1%  (n=510)
  [0.65, 0.75)   3.9%  (n=282)
  [0.75, 0.85)   0.0%  (n=46)
  [0.85, 0.95)   0.0%  (n=39)
  [0.95, 1.01)   0.0%  (n=47)

Capture rate by entry-price p0
  [0.00, 0.05)   0.0%   (n=65,  100% no_signal)
  [0.05, 0.10)   0.0%   (n=30,   93% no_signal)
  [0.10, 0.30)   8.8%   (n=114,  70% no_signal, 14.9% wrong_dir)
  [0.30, 0.70)   6.2%   (n=129)
  [0.70, 0.90)   4.5%   (n=132)
  [0.90, 0.95)   2.2%   (n=89)
  [0.95, 1.01)   0.5%   (n=405,  99% no_signal)

Per-engine PnL (closed trades, after 20 bps spread cost)
  aggressive          +$277.0   (80 closed)   TP 0.5¢, SL 1.5¢
  balanced            + $96.3   (107)         TP 2¢, SL 3¢
  inverted_highconf   + $36.2   (25)          flip dir when conf ≥ 0.75
  (conservative removed in cycle 4 at −$88)

AI neutral rate (1h):  84%
Decision latency p50: 210s   p99: 760s   (yes, seconds — see Q11)
```

Read those numbers as the playing field. The questions below are about how to interpret and react to them.

---

## 3. Stats questions

### Q1 — Confidence → edge mapping

**What we do.** `app/trader.py:17,499–505`: `edge = edge_increment · (2·conf − 1)`, `p_hat = mid + edge`. So `conf=0.5 → 0 edge`, `conf=1.0 → edge_increment`. The mapping is linear, the same shape across all engines (only the slope `edge_increment` and threshold `min_edge` change).

Since cycle 7, `conf` is the ensemble-averaged `p_alpha` (model's stated P(this trade is +EV in 5–30m)). The model is asked to self-rate trade EV, not just direction.

**Why we're asking.** The linear shape and the break-even point at 0.5 were chosen because they "feel right" — no fit. Two specific worries:

- If `p_alpha` is what we say it is, then break-even should be derived from spread cost, not fixed at 0.5. We charge 20 bps round-trip; at mid=0.5 a 20 bps cost moves the break-even to ~0.51, but at mid=0.10 the same dollar spread is ~10× larger in implied-probability terms.
- The empirical capture-by-confidence curve (above) is **non-monotonic and inverted at the top** (capture rate goes to 0 above conf=0.75). A linear-in-conf edge sizing is exactly the wrong shape if that curve is real.

**Data available.** `trades.signed_edge`, `trades.confidence_raw`, `price_impacts.delta_t5m`. A 2D scatter of `(conf, signed Δ5m)` already has thousands of rows; we have everything to fit Platt / isotonic / spline.

**Question for you.** What's the principled way to turn the AI's `p_alpha` into an executable edge, given (a) the observed inverted-U capture curve, (b) explicit spread cost, (c) mid-dependent payoff asymmetry on a binary? Are we crazy to even try, or is a Platt fit on `(p_alpha, captured)` the right starting move?

---

### Q2 — Ensemble: agreement as gate or as multiplier?

**What we do.** `app/ai.py:281–318, 510–593`. We make N=3 calls at temperature 0.5, parse `(p_yes, p_no, p_neutral, p_alpha, risk)` from each, majority-vote on direction, average distributions, and report `confidence = mean(p_alpha)`. A separate hyperparameter `ai.min_agreement: 0.66` requires ≥2/3 votes (`config.yaml:452`). Earlier cycles baked agreement *into* confidence (`mean_p · agreement_fraction`); cycle 7 split them so `2·conf−1` rescale still works.

**Why we're asking.** Three things to unpick:
1. Whether the LLM's stated `p_alpha` is a probability at all (it has the right type signature but no calibration evidence yet).
2. Whether ensemble agreement at temperature 0.5 is measuring genuine uncertainty about the headline, or just decoding noise on the same posterior.
3. Whether the right composition is gate-then-quote (current), multiplicative (old), Bayesian (each sample is a noisy probe of the same latent `p_true`), or "drop the ensemble and just take a single call at temp=0".

**Data available.** Each `trades` row stores `agreement`, `confidence`, `p_alpha`, `p_yes/p_no/p_neutral`. We can ask: at fixed `mean_p_alpha`, does outcome quality vary with agreement?

**Question for you.** With N=3 LLM samples that aren't statistically independent (shared prompt, shared base model, just different decoding seeds), what should we actually be reporting? Is agreement meaningful, or are we counting beans?

---

### Q2.5 — Concentration bounds: how much of what we're saying is statistical noise?

**What we do.** We quote IC values, capture rates, and bucket statistics from `price_impacts` aggregates and treat them as point estimates. The lessons aggregator activates rules at `support_n ≥ 10, |IC_proxy| ≥ 0.05` (`app/lessons.py:153–156`). The plotter draws Wilson CIs for proportions but not for the IC-style mean statistics.

**What we can derive cheaply.** Both our main random variables are bounded — signed Δ5m ∈ roughly [−1, 1], outcome buckets are Bernoulli {0, 1} — so **Hoeffding** gives distribution-free bands without needing σ:

```
P(|X̄ − μ| ≥ t) ≤ 2·exp(−2 n t² / (b−a)²)
```

Plugging in (95%, range b−a = 2 for signed Δ5m):

| Quantity | n | 95% Hoeffding band | What it means |
|---|---|---|---|
| `ic_global` cycle 8 | 964 | ±0.087 | Reported −0.0067 — null vs. zero |
| `ic_global` cycle 3 | 662 | ±0.105 | Reported −0.11 — barely outside |
| Per-bucket IC at n=10 | 10 | ±0.85 | Threshold of 0.05 lives inside noise |
| Per-bucket IC at n=50 | 50 | ±0.38 | Still inside |

Hoeffding is conservative; the CLT band using observed σ is tighter, probably ~5–10× smaller in practice. For Bernoulli capture rates the Wilson interval is the sharp tool: at p̂=0.03, n=964, CI ≈ [2.0%, 4.3%]; at n=10 with 0 captures, CI ≈ [0%, 28%].

To detect an effect size of |IC|=0.05 with Hoeffding at 95% requires:

```
n ≥ 2 · log(2 / 0.05) / (2 · 0.05²)  ≈  740 per bucket
```

The lessons aggregator's `support_n ≥ 10` threshold is **two orders of magnitude below** what Hoeffding asks for, and we activate up to ~10 candidate rules per observation across granularities (`lessons.py:239–257`). On top of that, active rules are injected into subsequent AI prompts, so this is closed-loop multiple-testing, not the textbook iid case.

**What's broken before we even start.** LLN and CLT both assume iid observations and a stationary target. Both fail here:
- Same headline fans into ≤25 markets through one batched LLM call — those rows are strongly correlated through the shared prompt. Effective n is materially smaller than nominal n, and we don't measure the within-batch correlation.
- News regime, Polymarket liquidity, and trader composition all drift on the timescale of one cycle (~6–24h). The "true mean" is moving.

**Data available.** We have the full per-row `price_impacts` + `trades` join, including `headline_id` for cluster-bootstrap on intra-batch correlation, `detected_at` for rolling-window estimates, and `signed_edge` / `delta_t5m` per row for sample-variance estimation.

**Questions for you.**
1. What's the right confidence-band tool for this dataset — Hoeffding (free, conservative), CLT with sample σ (sharp, ignores tails), Wilson for proportions, or something Bayesian with an informative prior?
2. Given correlated rows (within-batch) and drift (across-time), how should we discount nominal n to effective n? Cluster bootstrap on `headline_id`? Heteroscedastic robust SE?
3. The lessons aggregator's `n ≥ 10, |IC| ≥ 0.05` threshold appears to fire inside the noise floor by ~50×. Is the right fix (a) raise the n requirement to ~750/cell, (b) shrink per-cell estimates toward the parent bucket (James-Stein), (c) accept that the lessons system is mostly cosmetic and treat it as a soft prior on the AI prompt rather than a discovery engine, or (d) something else?
4. Concretely: at our cycle scale (~1000 classified rows / 24h), what *is* the smallest effect we can hope to detect, and what does that imply for which questions on this doc are even worth asking right now?

---

### Q3 — Why is the model anti-informative at high confidence?

**What we do.** Cycle 3 (`data/analysis/RECOMMENDATIONS.md`) found IC=−0.11 globally and ~0% captured for conf≥0.75. Cycle 8 (above) confirms: capture rate is 0 for conf in [0.75, 1.01) across n=132 rows. We responded by adding `skip_high_confidence_no_signal` and an `inverted_highconf` engine that flips direction at conf≥0.75 (`config.yaml:527–541`).

**Why we're asking.** Three candidate root causes; we don't know which:
- **(a)** The LLM is confidently predicting market reactions to news that's already priced in (`p0≥0.95` bucket is 99% no_signal and dominated this cycle).
- **(b)** Reflexive crowd: confident-sounding headlines on Polymarket attract counter-traders. The market really does move opposite — temporarily.
- **(c)** Training-data drift: the model has memorized statistical regularities ("Fed raise → market drop") that no longer apply on this venue.

Inversion at high confidence makes ~$36 (n=25 trades) — it works, but the sample is small and we don't know if it generalizes once rails are filtered.

**Data available.** Per-row mid_price_t0, predicted direction, signed Δ5m, source, market_type, headline text. Easy to bucket by (a) "was market at rail before headline" via `delta_pre_60s`, (b) "did mid drift back after t5m" via `delta_t30m/t60m`, (c) source family.

**Question for you.** Given (a)/(b)/(c) above, what's the cheapest experiment to distinguish them? Is inversion actually a real edge, or are we trading on a small-n statistical artifact?

---

### Q4 — Are the LLM's probabilities probabilities?

**What we do.** `app/ai.py:439–471`. We ask Gemini for `(p_yes, p_no, p_neutral)` that sum to 1, plus `p_alpha`. We renormalize defensively, argmax for direction, and pass through. The system prompt at `ai.py:51–56, 78–82, 84–110` is the only calibration signal — there's no post-hoc calibration step.

**Why we're asking.** LLM-emitted probabilities are notoriously not probabilities. They're tokens that look like probabilities. The model could be (i) well-calibrated, (ii) overconfident in a known shape, (iii) using probability as a stylistic choice. Without empirical calibration, we don't know.

**Data available.** Every `trades` row has `confidence_raw`, `p_yes/p_no/p_neutral`, and we can join to `price_impacts.delta_t5m`. Reliability diagram by 5%-confidence bucket is one query.

The plotter (`app/plotter.py`) already draws calibration with Wilson 95% CIs but the null it compares against is "precision = 50% random." That's the wrong null for a continuous probability — we want to compare against the y=x line.

**Question for you.** What's the right diagnostic suite for LLM-emitted probabilities? Reliability diagram + ECE + Brier score, or do you trust something else for non-stationary regime / small-n settings?

---

### Q5 — Similarity floor: is 0.55 throwing out signal?

**What we do.** `markets.similarity_threshold: 0.55` and `impact_tracker.MIN_SIMILARITY=0.55` (`app/impact_tracker.py:66`). We raised it from 0.35 because sub-0.55 matches were dominated by spurious keyword overlaps ("Rick Rieder" → "Rick Devens"). `top_k=25` markets per headline.

**Why we're asking.** This is the single decision that determines how big the trade funnel is. If we lower the floor, we get more spurious matches and more AI cost, but we might catch markets where the embedding-question-pair has low cosine but the news is actually informative (numbers, jurisdictional terms, etc.). The current floor was chosen by eyeballing the spurious-match list; it isn't an ROC operating point.

**Data available.** `price_impacts` records sim, and we have IC-by-similarity tables. The tracker also stores rows that were classified by the AI but skipped at the trade gate — so we have non-traded labels for similarity in [0.35, 0.55) historically (in cycle ≤3 snapshots under `snapshots/cycle2_baseline/` etc.).

**Question for you.** How would you frame "where to set the similarity floor" as a constrained optimization problem when the costs (AI tokens, prompt latency) and benefits (captured alpha) are on different scales? And is cosine sim on a 384d sentence-transformer the right matcher for short headlines vs. typically-longer Polymarket question strings at all?

---

### Q6 — Multiple testing in the lessons aggregator

**What we do.** `app/lessons.py:189–342`. Every 10 minutes we re-bucket `price_impacts` by `(source × market_type × conf_bucket × p_alpha × risk)` and emit candidate rules. Lines 239–257: a single observation gets fanned into up to 10 candidate `rule_key`s (most-specific to most-general). Activation: `support_n ≥ 10` and `|IC_proxy| ≥ 0.05`, where IC proxy = mean signed delta in predicted direction (`lessons.py:274`). Thresholds: `SKIP_IC_THRESHOLD = −0.08`, `DOWNWEIGHT_IC_THRESHOLD = −0.03`.

Active lessons get injected as prompt-header bullets the next time the AI classifies a headline — so this is a closed loop.

**Why we're asking.** A few worries:
- The IC "proxy" is `mean(signed_delta)`, not Spearman correlation with the rank ordering of confidence. It's an alpha estimate, not an IC. That's fine, but it's not what the rest of the literature calls IC, and the threshold values were borrowed from quant-finance IC literature.
- With ~50 sources × 4 market types × 5 conf buckets × 5 p_alpha buckets × 3 risk buckets, the candidate-rule space is ≈ 15k cells. Even with the most-specific-only constraint, we activate dozens of rules per cycle at `|IC_proxy| ≥ 0.05`. No FDR / Bonferroni / hierarchical shrinkage.
- Because the lessons feed back into the AI prompt, an active rule changes the distribution it was estimated from — this is a partially-observable Markov dynamic, not a stationary estimation problem.

**Data available.** The full `price_impacts` table is per-row and tagged with the relevant bucket fields. We can replay aggregation offline with any decision rule.

**Question for you.** What does a sane "rule discovery from streaming buckets with feedback" pipeline look like? Hierarchical shrinkage à la James-Stein on the per-cell means? Bayesian model averaging across granularities? Or do we just need more data and patience?

---

### Q7 — Causal structure of "headline → move"

**What we do.** `app/impact_tracker.py:218–246, 549–576`. For every classified `price_impacts` row we backfill `price_pre_300s, price_pre_60s, price_t0` from Polymarket's `/prices-history` (1-min fidelity). The `classify_missed_alpha` taxonomy splits outcomes into `too_slow` (pre-move already in our direction), `captured`, `no_signal`, `wrong_dir`, using a 0.5¢ threshold (`MISSED_ALPHA_THRESH = 0.005`).

**Why we're asking.** This is correlational. If the market moved in `[t−60s, t]` *and* we trade at `t`, we don't know whether the headline caused the move, the market and the headline both responded to a wire 90 seconds earlier, or both reflect a common antecedent (the actual news event, which the headline merely covers).

The `too_slow` bucket (3.4%) is exactly the "latency-limited alpha" hypothesis. The `captured` bucket (2.9%) is the "we got there ahead of the market" hypothesis. The system treats those as cleanly separable; with 1-min price fidelity vs. our ~minutes-of-latency decision time, that separation is questionable.

**Data available.** Headline `detected_at`, `published_at` if the site exposes it (rarely), source URL. Polymarket trade-history is available at higher fidelity than midpoint-history if we want it.

**Question for you.** How do you actually identify the causal pre-move window for news → prediction market on a venue with this kind of price fidelity and trader composition? Is there a published methodology you'd point us to?

---

### Q8 — Survivorship: we only record above-threshold matches

**What we do.** `impact_tracker.py:211–214`: a `price_impacts` row is only inserted when `ai_confidence ≥ MIN_CONFIDENCE` and `similarity ≥ MIN_SIMILARITY`. Everything below is dropped — never enters the dataset.

**Why we're asking.** We can't estimate the false-negative rate (markets that would have moved but didn't pass our gates) without recording the gates' "no" decisions. The system's headline reaction rate, capture rate, etc. are conditional on the gate, not unconditional.

**Data available.** The `signals` table (raw headlines) and `engine_results` (per-engine skips) cover the early-pipeline drop. We could shadow-record below-threshold rows at a low sample rate.

**Question for you.** What's the minimum-cost way to estimate the false-negative rate without doubling our AI bill? Reservoir-sampled control arm? Periodic threshold-free shadow runs?

---

### Q9 — Regime change detection

**What we do.** Cycles are ~6–24h. Between cycles, we ship one or two structural changes and reset per-engine state but keep `markets`, `news_chroma`, and `lessons` (`CYCLE_METHODOLOGY.md`, "Reset and run"). Seeded lessons (`lessons.py:128–148`) come from cycle 3 and persist until empirical evidence overrides them. The market universe refreshes every 4h.

**Why we're asking.** Polymarket liquidity, prediction-market trader composition, news flow composition, and the LLM's output distribution can all drift on the 1-hour scale. We have no online drift detection. If a cycle's "winning rule" emerged from a transient regime (the OPEC-meeting week, the Israel-Iran flareup, a US election cycle), we'll happily bake it in.

**Data available.** Everything is timestamped, including per-engine PnL, per-bucket capture rate, AI neutral rate (was 84% this cycle, was lower before).

**Question for you.** What's a defensible regime-shift detector in this setting? Page-Hinkley on bucket capture rate? CUSUM on per-engine PnL? Something simpler we're missing?

---

### Q10 — Why is `aggressive` profitable when IC is ~0?

**What we do.** The `aggressive` engine has TP=0.5¢ and SL=1.5¢ with a 60s min-hold (`config.yaml:496–504`). It opts out of the rail-skip filter. PnL: +$277 on 80 closed trades in cycle 8 (≈+$3.5/trade gross). Cycle 6 we deliberately did *not* change its parameters because it was profitable while IC was −0.11.

**Why we're asking.** The story we tell ourselves is "aggressive harvests intraday volatility via tight TP, independent of directional signal." But:
- We've never separated TP-fill rate from directional signal cleanly. If a position closes via TP at +0.5¢ in 90 seconds, that's a directional win even if our 5-min IC is zero.
- Spread cost is 20 bps/round-trip baked in (`config.yaml`, `spread_cost_bps`). At entry mid 0.5 that's 0.1¢; at 0.10 it's 0.5¢. With TP=0.5¢, aggressive at low-mid markets is barely break-even at fill — a single tick of spread movement consumes the whole TP target.

**Question for you.** Is aggressive's PnL a real strategy or a noise-harvesting artifact that will mean-revert as the dataset grows? What's the right way to decompose "TP-driven PnL" vs. "directional signal PnL" with what we already log?

---

## 4. Dev-lifecycle / experience questions (not stats)

You've shipped a lot of research code at TS. Three things I genuinely don't know how to navigate, and we'd value your judgment way more than your math here.

### Q11 — How do you decide what to fix first in a setup like this?

**Concrete fork in the road right now.** Cycle 8 shows latency p50 = **210 seconds** (yes, seconds — driven by AI batch wait, profile path in `app/profiler.py` and bottleneck in `ai/classify_batch`). The 6h analysis says latency is *not* the primary loss vector (only 3% `too_slow`). But we also have:

- IC ≈ 0, capture rate ≈ 3% — signal is barely there.
- 84% AI neutral rate — most of the pipeline runs to no decision.
- One engine (`aggressive`) printing money on what we think is noise.
- A lessons system that's actively rewriting itself every 10 min.

**Where would you spend the next week of engineering time?** Specifically: do you (a) drive latency to <5s and assume the captured bucket grows, (b) keep iterating on rules and rule-discovery, (c) stop and rebuild the calibration story end-to-end, (d) accept aggressive's profit is the only real edge and tune around that, or (e) something I haven't listed?

Bonus: what's the trap a researcher falls into in a setup that has *good* instrumentation but a marginal signal?

---

### Q12 — When IC is negative globally but a sub-strategy is profitable

The `aggressive` engine prints money. `inverted_highconf` (flip direction when conf≥0.75) prints money on small n. Yet our directional signal is near-zero by every direct measurement.

**Question.** At what point do you stop treating the directional signal as "the thing we're trying to build" and start treating the profitable sub-strategies as the actual product? Is there a cultural / process answer at TS for "the signal you set out to build isn't the signal that's making money" beyond "follow the PnL"?

---

### Q13 — How do you budget engineer-time on a research platform?

**Setup.** Each "cycle" (1–2 structural changes + 6–24h run + postmortem) costs roughly half a day of engineer time and produces ~1000 classified rows. Lessons fire continuously between cycles, so there's a slow inner loop and a fast outer loop. Some changes (rule additions, threshold sweeps) close inside one cycle. Some (calibration, regime detection, causal structure) need many cycles of data.

**Question.** How do you decide between (a) shipping one more rule into the next cycle, (b) running another cycle of the *current* config to accumulate more data, (c) stepping back to refactor instrumentation so the next month of cycles teaches us more? Is there a heuristic you actually use, or is it just taste?

---

## Appendix — Where to look

| File | What's there |
|---|---|
| `seulgi/app/trader.py` | Edge model, position lifecycle, all skip-reason logging |
| `seulgi/app/ai.py` | LLM prompt, ensemble aggregation, distribution parser |
| `seulgi/app/lessons.py` | Bucket aggregator, seeded lessons, prompt-injection notes |
| `seulgi/app/impact_tracker.py` | Pre/post price backfill, missed-alpha taxonomy |
| `seulgi/app/markets.py` | Embedding index, similarity search |
| `seulgi/app/plotter.py` | Calibration and capture charts (Wilson CIs) |
| `seulgi/config.yaml` | All thresholds, engine defs, rail-skips |
| `seulgi/data/postmortems/2026-05-14-0134/` | Cycle 8 bucket stats and metrics used above |
| `seulgi/data/analysis/RECOMMENDATIONS.md` | Cycle 3 6h findings — IC=−0.11 era |
| `seulgi/CYCLE_METHODOLOGY.md` | How we iterate |
| `seulgi/snapshots/cycleN_end/seulgi.db` | Full DB snapshot per cycle (queryable with sqlite3) |

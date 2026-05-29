# irene trading-viability synthesis — why there's no edge yet, and the one lane left

**Date:** 2026-05-29
**Audience:** team
**Universe:** 1,465 tradeable explorer rows (`tradeable_all`, current `irene/data/irene.db`)
**Method:** completed the cycle-26 gate migration + fixed the test suite, ran `t3b-magnitude-filtered`
on the full set, decomposed the loss, then ran a market-microstructure research pass and re-tested
each hypothesis against irene's own data.
**Reproducibility:** sandbox run_id `20260529T051205Z_t3b-magnitude-filtered_tradeable_all_b99dfb`;
all `[irene data]` figures come from that run + read-only SQL over `irene.db`. Companion per-run
report: `reports/2026-05-29-sandbox-t3b-friction-floor.md`.

> **Provenance key.** `[irene data]` = computed from our DB/backtests, reproducible. `[external —
> verify]` = from a market-microstructure research pass; presented with sources but **not yet
> independently confirmed**. The Polymarket fee schedule in §4 is the one external claim that
> materially changes our PnL math — **verify against Polymarket docs before acting on it.**

## TL;DR

**On the current data and capture instrument, there is no tradeable edge — fast or slow.** The loss
is ~99% transaction-cost friction sitting on a directional signal of essentially zero. Correcting a
wrong fee assumption roughly halves the loss but does not cross into profit; it just relocates the
problem from fees to spread. Passive market-making doesn't clear either (adverse selection ≫ rewards).
The deeper issue is that our LLM gate answers "is this news related to this market's *entity*?", which
flags a large dead population, when the tradeable question is "does this news *increment a still-open
perceived probability*?" The only lane with a measurable signal is **narrative continuation on
mid-priced, already-moving markets** — and capturing it needs a re-instrumented pipeline (longer
horizon, catalyst-anchored t0, rules text), not another prompt. **For now: treat the explorer output
as a labeled research dataset, not a PnL engine.**

## 1. Engineering status (done this session)

- Test suite: **30 failing → 0 (826 passing).** `[irene data]`
- Completed the in-progress **cycle-26 gate-pipeline migration**: the explorer now runs candidates
  through a configurable pipeline (`app/gates/`: `market_open → dkg_threshold → llm_batch`) instead
  of a single inline relatedness call. Every candidate writes an audit row tagged with `gate_status`
  + `gate_history`; only survivors trigger odds capture.
- Real codebase fix (not just tests): `_TracerAI` in `scripts/tracer/fire.py` lacked
  `classify_batch_for_gate`, which the migrated explorer requires at construction — added a
  deterministic stub so the tracer harness works under the v2 pipeline.
- Test updates to the v2 contract: `test_explorer.py` (gate pipeline + audit-row semantics),
  `test_markets.py` (cycle-26 fee-rate soft-cooldown contract), `test_dedupe.py` (a wall-clock-fragile
  burst-window test, anchored to its synthetic timeline).
- Committed on branch **`cycle-26-gate-migration`** (not pushed). The sandbox toolset + reports are
  intentionally left out of that commit.

## 2. The sandbox result

`t3b-magnitude-filtered` (2-step magnitude classifier + 5-predicate pretrade gate + TP/SL/trailing
exits) on the full 1,465-row set: `[irene data]`

| metric | value |
|---|---|
| trades executed | 43 (of 1,465; 1,422 skipped) |
| total PnL | **−$124.88** |
| ROI on bankroll ($1k) | −12.49% |
| win-rate | 7.0% (3W / 40L) |
| LLM cost | 72 calls, **$0.029** (pretrade gate filtered most rows before the LLM) |

The new second-stage gate made the strategy *cheaper and lower-variance* than the 05-28 sweep
(t3 lost −$330 over 151 trades), but **per-trade economics got slightly worse** (ROI-on-deployed
−5.6% → −9.6%): it loses less by trading less, not by finding a better subset.

## 3. Why it loses — loss decomposition `[irene data]`

| component | total | mean/trade | note |
|---|---:|---:|---|
| directional drift | **−$0.66** | −$0.02 | what we'd net with zero friction |
| − entry spread | −$23.60 | −$0.55 | half-spread crossed on entry |
| − exit spread | −$27.51 | −$0.64 | half-spread crossed on exit |
| − fees | −$73.11 | −$1.70 | as-modeled flat fee |
| **= total** | **−$124.88** | −$2.90 | |

**Friction is 99% of the loss; the predictions net to break-even.** The oracle (perfect-foresight)
exit still loses **−$77.03**, so exit timing is not the lever — the friction floor is. The
highest-conviction bucket (edge ≥0.30, n=25) is the *worst* performer (−$86, 4% win-rate): the model
is most confidently wrong exactly where it sizes up, and those trades cluster in one Iran/oil news
event (low independence — same overfit risk that flipped the 05-27 "winner").

## 4. Fee correction — the one external fact that moves the numbers

`[external — verify]` Polymarket moved to a "dynamic taker fee" on 2026-03-30; **Geopolitical/World
markets remain 100% fee-free** (no taker or maker fee), while other categories carry a probability-
scaled taker fee peaking near 50¢ (Politics ~1.0%, Sports ~0.75%, Crypto ~1.8%); makers never pay.
Sources: docs.polymarket.com/trading/fees, help.polymarket.com/.../trading-fees. **Verify before
acting — it directly changes backtest PnL.**

`[irene data]` Bucketing our 43 executed trades by market-question keywords: **37 (86%) are
geopolitics** (Iran/ceasefire/strike), 5 crypto/commodity, 1 other. So our flat ~2% fee was wrong for
the large majority. Recomputing:

| fee assumption | total PnL |
|---|---:|
| as-modeled (flat) | −$124.88 |
| geopolitics fees → 0 (real schedule) | **−$61.08** |
| all fees → 0 (e.g. maker entry) | −$51.77 |

The fee fix **halves the loss but doesn't cross zero** — the residual −$52 is almost exactly the
round-trip spread cost. It converts a *fee-dominated* loss into a *spread-dominated* one.
(Caveat: our `markets.category` column is unpopulated, so this used a keyword proxy — populating
real categories + fetched `fee_rate` is a concrete data-quality fix.)

## 5. Latency blindness — we can't see the window where a taker edge would live

`[irene data]` Our capture is **15-second REST top-of-book polling**, timestamped with the local poll
clock (not exchange-native), and **t0 is stamped minutes after the wire** (it's set after
ingest → entity extraction → DKG link-scoring → LLM gate). Entry offsets look fast (median 14s *from
t0*), but t0 itself is already late.

`[external — verify]` Prediction-market news repricing completes in milliseconds-to-seconds in modern
algo-dominated books. Combined with the above, **we are structurally blind to the entire window where
a directional taker edge would exist** — which is the cleanest explanation for the ≈0 residual drift
in §3. No prompt change fixes a data-timing problem.

## 6. Passive market-making is also infeasible here `[irene data]`

If we can't take, can we *make* (rest quotes, earn spread + Liquidity Rewards)? Measured the
adverse-selection hit a too-slow maker eats = pre-news → post-news mid repricing (YES token, n=2,090
events with a real pre-news baseline):

| adverse-selection hit | value |
|---|---|
| mean | **6.1¢/share** |
| median | 0.5¢ |
| p90 / p99 | 20.9¢ / 66.5¢ |
| events moving >10¢ | 15.4% |

Against a **median quotable spread of 2.0¢** → a maker earns ~1.0¢ per quiet fill. `[external —
verify]` Liquidity-Rewards yield tops out around ~1¢/share-day, pays **$0 on quiet markets**, and
geopolitics generates **no maker rebates** (fee-free → nothing to fund them). **Reward (~1¢) <
adverse-selection drag (6.1¢ mean, 20¢+ tail).** Classic market-maker payoff: thin spread capture
most of the time, blown out by the tail — and our latency means our LLM "circuit breaker" to pull
quotes would fire *after* the bots have run us over.

## 7. News-vs-rules, and the only lane with a signal `[irene data]`

The framing error: our gate asks **"is this news related to the market entity?"** — but a prediction
market only moves when the **perceived probability** of its *specific resolution rule* changes. A
headline can be entity-related yet legally irrelevant (wrong oracle source, ambiguous wording, or
already priced in), and the book won't budge. That describes most of our dataset.

Bucketing every event by starting "room" (midband vs extreme price) × pre-news state (was the mid
already drifting in the 24h before t0):

| starting price | pre-news state | n | mean post-move | % >5¢ |
|---|---|--:|--:|--:|
| extreme (<0.25/>0.75) | quiet | 816 | 0.56¢ | 2% |
| extreme | already moving | 346 | 1.85¢ | 8% |
| midband (0.25–0.75) | quiet | 218 | 1.70¢ | 8% |
| **midband** | **already moving** | **148** | **2.91¢** | **16%** |

**Medium shifts concentrate in midband markets whose narrative was already in motion** — 8× the
move-rate of the dead majority. The signal is *continuation of a live, unresolved perception*, not a
reaction to a fresh headline (consistent with "the real news trade happened earlier on rumors"). So
the correct gate is: **"does this news materially increment a still-open, rule-relevant perceived
probability?"** — which means ingesting the market's `resolution_rules` text to *discard* the
legally-irrelevant headlines, not to validate eventual payout.

**Honest limit:** even this best bucket is thin — 2.9¢ mean move vs a 2¢ spread. It's a narrow lane,
positive-gross only at zero (geopolitics) fees, and the continuation plays out over hours, beyond our
current +1h window.

## 8. Recommendation

1. **Do not deploy any current prompt × filter × exit combo, and don't run more prompt variants** —
   they cannot beat the friction floor on this data.
2. **Do not chase the infrastructure/latency game.** `[external — verify]` bots are ~75% of volume and
   ~67% of profit accrues to ~0.1% of accounts; racing co-located market makers is the one game we
   lose by construction, and it's exactly where the LLM adds no value.
3. **The one bet worth testing is slow / narrative catalysts** (court rulings, regulatory decisions,
   long-horizon political/economic outcomes) where repricing takes hours-to-days and rewards *reading*
   over *speed* — the LLM's only durable moat. This requires **re-instrumentation, not a backtest tweak**:
   - capture horizon of **days**, not +1h;
   - **t0 = catalyst drop** (document/filing/scheduled resolution), not first matched headline;
   - **market selection toward midband / already-in-motion + document-heavy** markets, away from
     breaking-news/crypto/sports;
   - **ingest `resolution_rules`** and add a "perceived-probability increment" gate.
4. **Cheap wins meanwhile:** populate `markets.category` + use real `fee_rate` in the backtest
   (kills the §4 ambiguity); re-score historical results with **cluster-adjusted** stats (treat a news
   cluster as N≈1, not N=trades) so we stop fooling ourselves with correlated samples.

## 9. Bottom line

irene is a working, well-instrumented **data-collection** system; that part is solid and now fully
migrated/tested. What it is *not* — on current data and capture design — is a profitable trading
system, and the reasons are structural (friction + latency + an entity-match gate), not a tuning
problem. The path forward is a deliberate re-instrumentation toward slow, reasoning-heavy catalysts,
entered as a falsifiable experiment with the thin-margin caveat stated up front.

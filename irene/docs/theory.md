# Irene theory — why each piece exists, and the math behind it

> Companion to [`philosophy.md`](philosophy.md). Philosophy says **what**
> irene is (news interpreter + fast runner, not a forecaster). This doc
> says **why** every gate, every formula, and every default is shaped the
> way it is.
>
> - **Created:** 2026-05-25 (post cycles 4–7)
> - **Status:** Working doc. Numbers that need external confirmation are
>   tagged `→ research_questions.md#XN`.

---

## 1. The edge model

Every irene trade hangs on one inequality (`trade.py` gate 5):

```
edge  =  |p̂ − mid|  −  fee_buffer   ≥   min_edge   (= 0.04)
```

- `mid` is the displayed mid-price on Polymarket at decision time.
- `p̂` is the LLM's reading of which way the news resolves the market,
  mapped to a YES-probability scale: `p̂ = confidence if direction=YES
  else 1 − confidence`.
- `fee_buffer` is a hand-set scalar (currently absorbed into the cycle-7
  fee math at close time, but the gate still uses it as a pre-fee
  pessimism term).
- `min_edge = 0.04` is the floor below which the trade is not worth the
  variance.

**Why this works even though p̂ is not a probability.** Philosophy.md is
emphatic that the LLM is doing NLU, not forecasting. The math is still
correct because the *gate* doesn't need a forecast — it needs the news
to *contradict* the market clearly enough that "buying toward p̂ and
holding until repricing" has positive expected value once fees are paid.
If `p̂ = 0.9` and `mid = 0.30`, what we're saying is "the news plainly
says YES; the market is priced for NO; somebody hasn't read this yet."
The gate fires because the *information* — not the prediction — implies
the market should move.

**What `fee_buffer` is hiding.** It's a one-number pessimism term
covering:
1. The bid-ask spread we'll cross to enter (mid is mid, not best-ask).
2. Possible slippage on partial fills during a fast move.
3. Resolution uncertainty (UMA dispute risk).

The cycle-7 fee math (§2 below) made the round-trip *taker fee* explicit
and netted it into `realized_pnl_usdc`. `fee_buffer` now nominally
covers (1)+(2)+(3); it does **not** double-count taker fees. The cycle-8
question is whether `fee_buffer` should grow a per-market spread
estimate (→ `research_questions.md` i-C1 + O-C1).

---

## 2. Fee math — Polymarket's parabolic taker

Per-leg fee in USDC:

```
fee_usdc  =  shares  ×  fee_rate  ×  p  ×  (1 − p)
```

- `shares = size_usdc / position_price` (the price you're buying at).
- `p = position_price` (YES-mid for a YES position, `1 − YES-mid` for
  NO).
- `fee_rate` is category-dependent (table below).
- Both legs (entry and exit) pay this. Maker is 0; settlement is 0.

**Where it peaks.** `p × (1 − p)` is maximised at `p = 0.5`, giving
`0.25 × fee_rate` per share. At the extremes (`p → 0` or `p → 1`) it
collapses to ~0.

**Why a parabola, not a flat percent.** A parabolic fee is the natural
shape for a binary market: at `p = 0.5` the marginal information from a
trade is highest (the market is genuinely uncertain), so a flat
percentage of notional would over-tax informed trades at the extremes.
The parabola flattens fee burden where price approaches certainty.

**Category table** (cycle 7, hardcoded in `cfg.trade.polymarket_fee_rates`):

| category                       | rate  | peak fee /100 shares at p=0.5 |
| ------------------------------ | ----- | ----------------------------- |
| crypto                         | 0.072 | $1.80                         |
| economics/culture/weather      | 0.05  | $1.25                         |
| finance/politics/tech/mentions | 0.04  | $1.00                         |
| sports                         | 0.03  | $0.75                         |
| geopolitics                    | 0.00  | $0 (free)                     |

Numbers are not currently sourced to a primary doc — see
`research_questions.md` O-A1, O-A2 (online) and i-A2, i-B3 (internal).

**What the geopolitics = 0 implies for strategy.** Free trades on
geopolitics markets means any positive edge is profitable. If the
fee-table is real, then a meaningful share of irene's expected value
should come from the geopolitics tier; cycle 8+ profitability
attribution should split PnL by category to verify.

**Why both legs are paid.** A latency-arb position is opened and closed
within the same news cycle — there is no "make then close at expiry for
free" path that avoids the exit fee. Settlement fee is 0, but settlement
only happens on positions held to resolution; irene closes well before.

---

## 3. Kelly sizing

Kelly fraction on a binary contract paying $1 (cycle-3 `trade.py`):

```
kelly_full  =  (p̂  ×  b  −  (1 − p̂))  /  b
                where  b = (1 − mid) / mid
```

- `b` is the "odds" on a $1 payout — the same convention as sports
  betting.
- `kelly_full` is the fraction of *bankroll* the unconstrained-risk
  Kelly trader would put on this bet.
- We cap at `max(0, kelly_full) × cfg.kelly_fraction` with
  `kelly_fraction = 0.25` ("quarter Kelly").

**Why 0.25.** Quarter Kelly is the standard "no, really, don't blow up"
fraction in any literature that takes drawdowns seriously. Full Kelly
maximises log-bankroll growth in expectation but has 20%+ drawdown
probability even with a known edge. Quarter Kelly trades growth for
variance; for an experimental paper-trader where the *edge itself is
uncertain*, quarter Kelly is conservative but defensible.

A literature anchor for prediction markets specifically is open
(`research_questions.md` O-F1; internal sweep i-F3).

**Why the $25 minimum.** `min_position_usdc = 25` is the noise floor —
below this, fees and minimum-trade-size constraints eat the edge before
the position can move. The gate rejects clipped sizes below this in two
places: pre-cash-clip (gate 6) and post-cash-clip (gate 7).

**How cycle-3's "clip to cash" interacts with Kelly.** Before cycle 3,
sizes that exceeded available cash were rejected outright. After cycle
3, the size is clipped to available cash and the *clipped* size is
checked against `min_position_usdc`. The interpretation: Kelly tells us
what we'd *like* to size, but cash availability is a hard constraint;
as long as the constrained size still clears the noise floor, take the
trade. This subtly biases small-bankroll behaviour toward more trades
at smaller relative sizes — defensible for a $1000-bankroll paper run.

---

## 4. Freshness label causal logic

`dedupe.py` assigns each article a label based on (Chroma distance to
nearest existing cluster) and (wall-clock age of that cluster):

| label          | trigger                                            | what it means              |
| -------------- | -------------------------------------------------- | -------------------------- |
| `BREAKING`     | distance > 0.55 (no near cluster)                  | new event nobody's covered |
| `EVOLVING`     | distance ≤ 0.55 AND cluster age < 6h               | ongoing story, fresh angle |
| `REHASH`       | distance ≤ 0.55 AND 6h ≤ age < 24h                 | same story, day-old        |
| `STALE_RETURN` | distance ≤ 0.55 AND age ≥ 24h                      | recycled coverage          |

**Why BREAKING bypasses extreme-mid and far-future gates.** Cycle 7's
`market_news_imbalance` gate (§5 below) rejects far-future markets and
extreme-mid markets *unless* the news is BREAKING. The reasoning:
ordinary news doesn't move a "NATO withdraws by 2027" market priced at
0.928 — that market's tail is already priced in and only genuinely-new
information should shift it. But a *truly new* development (a treaty
signature, a high-profile resignation) can move even a deeply-priced
market because the market simply didn't know. BREAKING means the news
introduced information not previously in the cluster space.

**Why EVOLVING gets neither bypass.** EVOLVING is "more coverage of a
story we already know about." The market has had time to price the
underlying event; new EVOLVING coverage rarely contains decisive new
information; permitting EVOLVING through extreme-mid gates would let
ordinary tracking news fire trades on already-priced markets.

**Why REHASH and STALE_RETURN are auto-rejects.** Gate 2 (freshness)
rejects any signal whose label is not in `{BREAKING, EVOLVING}`. The
underlying claim: by 6h post-cluster, the market has had every
sophisticated participant's eyes; we cannot have edge on day-old
information. Even if our LLM correctly reads a REHASH article as
resolving the market, the market has already repriced.

**Open theory question.** The 0.55 distance threshold and the 6h /
24h age bands are hand-set. Empirical justification —
`research_questions.md` O-F3 (literature) + i-F1 (gold-set sweep).

---

## 5. Decision-funnel rationale (gate by gate)

`trade.py` runs every signal through this cascade. First reject wins.
Each gate writes a `decisions` row with `reason=<gate_name>`.

### Gate 1 — `similarity_floor` (`cfg.match.min_similarity = 0.35`)

**Catches:** matches where the article and market just don't talk about
the same thing.

**Why it exists:** `match.py` returns top-K candidates per article;
without a floor, every article gets compared against 25 markets,
including ones with cosine < 0.10. The LLM would then be asked "does
this Steyer article resolve the IPL cricket market?" and would correctly
say R, but the LLM call is wasted money.

**If removed:** LLM-call rate spikes, R-rate climbs, cost goes up,
nothing else changes.

### Gate 2 — `freshness` (`{BREAKING, EVOLVING}` only)

**Catches:** matches on stories the market has already priced.

**Why it exists:** see §4. Stale news has no edge.

**If removed:** trades fire on 12h-old news that the market saw at hour
0; expected PnL on those trades is zero before fees, negative after.

### Gate 2.5 — `entity_overlap`

**Catches:** matches where the cosine similarity is high but the
specific entities don't line up (e.g. "Tesla recall" article matched to
"Tesla earnings" market).

**Why it exists:** semantic similarity at the embedding level often puts
unrelated-but-topically-adjacent content close together. Entity-level
overlap is a sharper filter than cosine alone.

**If removed:** more false positives at the LLM gate; LLM does the same
filtering at higher cost.

### Gate 3 — `direction` (LLM Y/N/P/U/R label + confidence digit)

**Catches:** matches where the article exists but doesn't decisively
resolve the market. P (pass — weak evidence), U (unclear), R (mismatch)
all reject here.

**Why it exists:** the central piece of the latency-arb thesis. Trade
when the article plainly tells us the outcome.

**If removed:** there's no trade decision at all. This gate is the
strategy.

### Gate 4 — `mid_degenerate` (`0.02 < mid < 0.98`)

**Catches:** markets where the price is so close to 0 or 1 that the
edge math is dominated by floating-point noise and the position would
fight against the 99%+ certainty already priced in.

**Why it exists (cycle 4):** before this gate, fired trades on markets
at mid=0.99 produced absurd Kelly sizing and rounding-driven phantom
PnL. Degenerate-mid filter is the cheapest one-line fix.

**If removed:** PnL accounting breaks at the extremes; rare but real.

### Gate 4.b — `market_news_imbalance` (cycle 7, five sub-rules)

The biggest gate added in cycle 7. Catches trades where the news is
"ordinary" but the market is "extreme" (either in price or in time
horizon). Five sub-rules:

| sub-rule                       | rejects when                                                | rationale                                                                                                       |
| ------------------------------ | ----------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `thin_market`                  | `volume_usdc < $10k`                                        | thin books mean slippage will eat the edge; defensive — skipped when `volume_usdc` is 0/NULL (parser bug)       |
| `extreme_mid_no_breaking`      | `mid ∉ [0.15, 0.85]` AND `freshness != BREAKING`            | extreme-mid markets only move on genuinely new information; ordinary EVOLVING coverage can't shift them         |
| `far_future_no_breaking`       | `days_to_expiry > 180` AND `freshness != BREAKING`          | long-horizon markets have time to absorb news through other channels; only fresh information has edge here      |
| `near_expiry`                  | `days_to_expiry < 1`                                        | within 24h of resolution the market is doing real-time settlement, not pricing — our latency edge is gone        |
| `singleton_on_extreme_mid`     | `cluster_size = 1` AND `mid ∈ extreme zone`                 | a single article moving an extreme market is almost certainly noise, not signal                                 |

**Why this gate is conjunctive ("ordinary news" AND "extreme market"):**
A BREAKING story on an extreme market — Epstein 0.115 BREAKING in cycle
29 — is exactly the kind of trade the strategy wants: huge mispricing
that the market hasn't caught up on. Only the *combination* of
ordinary-evolving coverage AND extreme market is the smelly case.

**If removed:** the cycle-29 audit produced three "smelly" trades
(NATO 0.928 EVOLVING, Rajasthan 0.875 EVOLVING, plus thin-book Steyer)
that look much worse than typical trades. Gate 4.b makes those
non-firers in cycle 31+. The cycle-7 unit tests pin this contract.

### Gate 5 — `edge` (`edge ≥ 0.04`, see §1)

The headline gate. Without enough p̂-vs-mid divergence after fees, the
trade is not worth the variance.

### Gate 6 — `size` (Kelly + min_position)

Don't trade if Kelly-recommended-size is below the $25 noise floor.

### Gate 7 — `cash` (cycle-3 clip + min_position)

Don't trade if available cash forces the clipped size below the noise
floor.

---

## 6. MVP vs DKG — two trade paths, one bot

irene has two distinct paths to a trade decision, distinguished by
`trades.decision_path`:

- **MVP path** (`trade.py`): article → top-K cosine match → LLM
  direction → gate cascade → trade. One-hop: article directly resolves
  market.
- **DKG path** (`dkg_trade.py`): article → extracted facts → clustered
  event → linked market → LLM direction → gate cascade → trade. Two-hop:
  article describes an *event*, the event causally implies the market.

**Why both exist.** The MVP path is fast and covers the "news directly
contradicts market wording" case ("FDA approves drug X" + market
"FDA will approve drug X"). The DKG path covers the "news describes
something whose downstream causal chain affects the market" case
("Senate votes against new sanctions" + market "Russia will invade
Estonia in 2027" — semantically distant, causally connected).

**Why DKG was never expected to dominate.** A two-hop path produces a
narrower set of fires (`market_links.final_score ≥ 0.8` is a steep
threshold) but each fire is a higher-conviction directional read. DKG
was always the *quality* path; MVP is the *coverage* path.

**Where DKG lags MVP (cycle 7).** DKG doesn't have the cycle-7
`market_news_imbalance` gate yet (handoff follow-up #5). DKG already
has the cycle-4 `mid_degenerate` gate via cycle-5.1 Fix 7. The
remaining gate-parity port is low priority because DKG fires rarely
under live data.

**Where DKG is conceptually cleaner.** MVP fires on a single cosine
similarity. DKG fires on a structured event with extracted entities
and a scored market link. If we ever wanted to *explain* why a trade
fired to a third party, DKG's path produces a more legible audit
trail. For now both paths are paper.

(For the implementation detail, see [`dkg_spec.md`](dkg_spec.md).)

---

## 7. AI label theory — the asymmetric R-heavy classifier

`ai.py` returns one of `{Y, N, P, U, R}` plus a confidence digit:

- `Y` — trade YES, news strongly supports the condition being TRUE
- `N` — trade NO, news strongly supports the condition being FALSE
- `P` — pass, news is relevant but evidence is weak
- `U` — uncertain, can't tell which way this cuts
- `R` — reject, news is NOT about this market

The trade-gate (Gate 3) accepts only `Y` or `N` with confidence digit
above `min_confidence`.

**Why R-heavy is correct behaviour.** `match.py` returns the top-K
markets by cosine, regardless of whether any of them is *actually* the
right market. For a random article, the expected outcome on each of K
candidates is R — the article doesn't resolve any of them. An R-rate of
70–80% means: on most article × market pairs, we correctly recognise
the pair as a mismatch and don't burn an LLM dollar on a doomed trade
decision. The strategy's *fires* live in the 20–30% non-R tail.

**Why R-rate too low would be bad.** If the LLM said Y/N more
liberally, gate 3 would let through pairs that don't actually resolve;
edge-gate would have to do the filtering at greater cost (and may not
catch it).

**Why R-rate too high is bad.** The fundamental tension: we want the
LLM aggressive about *recognising* the right market when it does see
one, but conservative about *labelling* unrelated material. The current
prompt + Gemini 2.5 Flash combo sits at 76% R — possibly the right
operating point, possibly model under-confidence, possibly prompt
weakness. Open question: `research_questions.md` i-E2 (R-rate by slice)
+ O-H1, O-H2 (benchmarks and lit).

**P vs U vs R.** `P` is "I read the article, I see the relevance, but I
can't decide a direction" — a recoverable case (a better article might
clarify). `U` is "ambiguous between Y and N" — same outcome (reject)
but different root cause. `R` is "not about this market at all" —
matching layer error. The three labels split the failure space so
post-mortems can attribute rejects to either matching (R) or LLM
hedging (P/U).

---

## 8. Open theoretical questions

Cross-references to `research_questions.md`. Each open question lists
its **internal** angle (something we can measure in our own repo / DB)
and its **online** angle (literature or vendor docs). Reconciling both
sides is the highest-confidence path to changing a config default.

| Question                                                                | Internal angle | Online angle |
| ----------------------------------------------------------------------- | -------------- | ------------ |
| Edge floor `min_edge = 0.04`: defensible?                               | i-F2           | O-F2         |
| Kelly fraction 0.25 vs published prediction-market backtests?           | i-F3           | O-F1         |
| Cosine floor 0.35 and BREAKING distance 0.55 — empirical justification? | i-F1           | O-F3         |
| Fee formula correctness and category-rate table?                        | i-A2, i-B3     | O-A1, O-A2   |
| News→repricing half-life — how fast is the window we're racing?         | i-D2           | O-E1         |
| Real `fee_buffer` composition once spread + slippage are measured?      | i-C1, i-C2     | O-C1         |
| Is R-rate 70–80% the right operating point or hedged under-confidence?  | i-E2, i-H2     | O-H1, O-H2   |

Each of these would, if answered on both sides, change at least one
config default or unlock a gate currently defaulting to "off" / "skip".

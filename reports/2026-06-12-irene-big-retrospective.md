# irene big retrospective — what was built, what the hypotheses said, what the data answered

**Date:** 2026-06-12
**Audience:** team
**Method:** read-only pass over the full written record (22 dated reports, master-plan, philosophy,
cycle logs), git history (211 commits), and fresh read-only SQL over `irene/data/irene.db`
(2.4 GB, live container untouched). New analysis covers the **post-synthesis window
2026-05-30 → 2026-06-12** that no prior report examined, plus the two "cheap wins" the 05-29
synthesis recommended but never ran (cluster-adjusted re-scoring, fee-model audit).
**Provenance:** every number below is either `[doc]` (cited report) or `[db]` (query in §12).

## TL;DR

irene was built in ~16 days of intense development (2026-05-14 → 05-29, 26 cycles, ~65k lines,
836 tests) on the thesis **"speed, not prediction."** The thesis was tested honestly and refuted in
stages: a 54-run backtest sweep found one winner (N=9, +3.73% ROI) that collapsed on 3.4× more data
(−9.2%, the **mirage**); loss decomposition showed the residual loss is **~99% friction** on a
directional signal of ~0; and a microstructure pass showed the passive (maker) side loses to
adverse selection too.

This retrospective adds three things:

1. **The live paper wallet's −$4,526 is largely accounting fiction in both directions.** It models
   zero spread (fills at mid), its fee model overcharges by ~5× (mean 5.7% of size, max 42.6%),
   and its biggest winners are mid-price ghosts on zero-volume markets. The clean liquid subset
   (255 trades, vol ≥ $100k) shows **+$1.13/trade gross drift — real but below the ~$2–3 round-trip
   spread**. The live wallet independently reproduces the sandbox verdict.
2. **The 05-29 synthesis survived out-of-sample.** 328 closures after 05-30 behave exactly as
   predicted: thin gross signal, friction-dominated, and the one "great day" (+$1,001 on 06-11)
   was a single zero-volume market (COIN weekly-low, +$1,395) — another N=1 mirage.
3. **Cluster-adjusted, the whole history is ~52 independent bets, not 845 trades** — and 3 of 52
   clusters were profitable. Total LLM spend for everything: **$38.79.**

The standing recommendation (slow/narrative catalysts + resolution-rules gate + the explorer as a
labeled dataset) is unchanged and now better supported. The explorer has banked **81,413 gated
explorations and 2.18M odds observations** in 16 days — the dataset reframe is real.

## 1. The development arc

| period | what happened |
|---|---|
| 2026-03-14 | predecessor repo starts — Tkinter GUI + paper-trading framework, written by collaborators and **not part of nexus** (see `docs/rename-from-jennie.md`) |
| 2026-04-01 | "irene is the next big thing trust" — clean-slate bot branch |
| 2026-05-14 → 05-23 | P0 scaffold: master-plan, philosophy lock, modularization rules |
| 2026-05-23 → 05-24 | P1 MVP (article→embed→match→LLM→paper-trade), P2 extraction (shadow), P3 DKG chunks 1–3 |
| 2026-05-24 → 05-27 | Cycles 1–26: fees, UMA close machine, wallet honesty, gate pipeline, events-endpoint dual-read, WS market cache |
| 2026-05-27 → 05-29 | Pivot to empirical: sandbox sweeps ×2, friction floor, viability synthesis |
| 2026-05-30 → 06-07 | Container down 8 days (Dockerfile missing `capture_schedule.py`) |
| 2026-06-08 → now | Fixed (`5f18fae`) + budget phantom-pause fix; explorer collecting continuously |

186 of 211 commits landed in the final 27 days — roughly 7/day. The cycle methodology (numbered
iterations, dated immutable reports, tracer bullets, replay gate) held up under that pace.

## 2. The hypothesis ledger

| # | hypothesis | verdict | evidence |
|---|---|---|---|
| H1 | **Speed beats prediction** — interpret decisive news, buy before repricing | **Refuted structurally** | t0 stamped minutes after the wire (15s REST polling + pipeline); repricing completes in ms–s; median entry +14s *from an already-late t0* `[doc: synthesis §5]` |
| H2 | LLM has directional edge post-news | **Refuted** | drift −$0.66 on 43 sandbox trades `[doc]`; +$1.13/trade gross on 255 liquid live trades, sub-spread `[db]` |
| H3 | Some prompt × filter combo is profitable | **Refuted** | 54-run sweep ×2; only "winner" collapsed at 3.4× sample `[doc: 05-27, 05-28 sweeps]` |
| H4 | t3 × short_dated is the winner (N=9, +3.73%) | **Mirage** | N=42: −9.20%, win-rate 66.7%→16.7%; 6 of 9 wins were one Iran/oil cluster `[doc: 05-28]` |
| H5 | Exit timing is the lever | **Refuted** | oracle (perfect-foresight) exit still loses −$77 `[doc: friction floor]` |
| H6 | If we can't take, we can make (passive MM) | **Refuted** | adverse selection 6.1¢ mean (p99 66.5¢) vs ~1¢ reward; latency means the LLM circuit-breaker fires after the bots run us over `[doc: synthesis §6]` |
| H7 | Momentum (or fading it) in the first minute is predictive | **Refuted both ways** | t5 −14%, t7 −21% at scale `[doc: 05-28]` |
| H8 | Midband + already-moving markets carry signal | **Open — the one survivor** | 2.91¢ mean post-move, 16% >5¢ — 8× the dead majority; but vs 2¢ spread, positive-gross only at zero fees, plays out over hours `[doc: synthesis §7]` |
| H9 | The 05-29 "no edge" verdict | **Confirmed out-of-sample** | §4 below `[db]` |

## 3. Live-wallet forensics — the −$4,526 is mostly measurement, not market `[db]`

845 live closures (2026-05-24 → 06-12): −$4,525.74, 16.7% win rate, $136.9k deployed.
Decomposing every trade into mid-to-mid drift + fees + residual:

| component | value | meaning |
|---|---:|---|
| mid-to-mid drift | **+$3,736** | gross price movement in our direction |
| modeled fees | **−$8,262** | 6.03% of deployed (!) |
| residual (spread/slippage) | **$0.00 exactly** | the paper wallet fills at mid — no spread is modeled |

Three artifacts dominate:

1. **Fee model overcharges ~5×.** Mean fee/size = 5.74%; worst single trade paid 42.6% of its size
   in fees (`fee_rate=0.1` applied per side as 10%). Real schedule: geopolitics 0%, others ≤~2%
   peaking at 50¢. The 751 **stagnation** closures are price-flat (drift −$212 total) — their
   −$7,227 "loss" is almost purely modeled fees on dead round trips.
2. **The winners are mid-price ghosts.** Of the 29 target closures (+$6,496, all of the wallet's
   gross profit), the top of the list is: +$1,120 "US median home value" (volume **$0**), +$956
   AAPL weekly-low (volume $15), +$641 Miami home value ($247), 3× COIN weekly-low (+$1,395,
   volume $0, exits pinned at exactly 0.500). 179 trades on zero-volume markets net **+$1,461** —
   prices nobody could have traded at. 29 entries and 17 exits sit at exactly 0.500.
3. **The believable subset agrees with the sandbox.** Liquid markets (vol ≥ $100k, n=255):
   gross drift **+$1.13/trade** — positive, real, and smaller than the ~2¢ round-trip spread
   (~$2–3 at the $162 mean size) the wallet doesn't charge. Same verdict as the synthesis,
   reached from independent data.

Net: the live wallet over-penalizes fees by thousands, under-penalizes spread by hundreds, and
mints fictional wins on empty books. Median trade: −$3.58. PnL excluding the top-10 (artifact)
winners: −$9,355.

## 4. Out-of-sample epilogue: the synthesis predicted the future `[db]`

| window | n | PnL | win% | fees | drift |
|---|--:|--:|--:|--:|--:|
| pre-synthesis (< 05-30) | 517 | −$3,120 | 18.2% | $4,333 | — |
| post-synthesis (≥ 05-30) | 328 | −$1,405 | 14.3% | $3,929 | +$2,524 |

The post-window's positive drift is the same illusion: **June 11's +$1,001 "best day ever"
(74 trades, 27% win) was one market** — "Will COIN hit (LOW) $145 Week of June 8" contributed
+$1,395 across 3 trades on a zero-volume book; everything else that day netted negative.
The N≈1 cluster lesson from the 05-28 mirage reproduced itself in live data within two weeks.

**Cluster-adjusted re-scoring** (the synthesis's recommended fix, now run): the 845 trades map to
**52 article clusters** (largest: cluster 18, 188 trades, −$290) and 297 markets. Profitable
clusters: **3/52 (5.8%)**. Profitable markets: 34/297. The effective sample of the entire
trading history is ~52 bets, and the wallet's variance ("+$1,001 day", "−$1,511 day") is exactly
what 52 correlated bets look like.

## 5. The decision funnel and what it cost `[db]`

```
75,700 articles → 48,809 signals → 127,407 decisions → 852 trades (0.67% of decisions)
```

Top rejections: `direction_neutral` 51,689 (the 1-token wording gate does most of the work),
`market_inactive` 30,741, `market_near_expiry` 29,127, `freshness_stale` 6,060.

Total LLM bill for everything ever: **132,685 calls, 71.3M in / 7.0M out tokens, ≈ $38.79**
(gemini-2.5-flash) — ~4.6¢ per executed trade, ~$0.0005 per article evaluated. Prediction cache
hit rate is only 4.2% (2,451/58,421); cheap enough that nobody had to care.

## 6. What the explorer banked (the dataset reframe, quantified) `[db]`

Since 2026-05-27: **81,413 explorations** (4,554 passed gates; rejections audited:
`cheap_binary` 61,882, `dkg` 14,977), **2,175,744 odds observations**, 81,391 completed captures —
every row labeled with gate status, gate history, t0 market state, and LLM verdict + reason.
This is the "labeled research dataset, not a PnL engine" the synthesis asked for, and it grew
through the 8-day container outage's recovery without intervention.

## 7. Engineering retrospective — what held up

- **Tracer bullets + replay gate.** Synthetic articles traverse the real pipeline (`is_tracer`
  on every table); replay reconstructed 3,000/3,000 trades within ±$0.01. This is why the §3
  forensics were even possible.
- **Audit-everything.** No silent gate skips — 127k decision rows with reasons; explorer audit
  rows for rejections. The funnel in §5 is one GROUP BY.
- **Modularization rules + lint-enforced invariants** (trade path mechanically cannot read
  extraction tables). The P2/P3 extraction layer shipped, stayed shadow, and never contaminated
  the hot path — a hypothesis firewall.
- **Dated immutable reports.** The 05-27 report flagged its own winner as probably-correlated;
  the 05-28 report killed it. The written record made the mirage a finding instead of a deploy.
- **1-token LLM call (Y/N/U).** ~100× cost cut spent on 5× candidate breadth; total spend $39.
  The cost philosophy worked even though the trading thesis didn't.

**What didn't hold up:** the paper wallet as an instrument. Mid-fills on empty books, a 5×-wrong
fee model, and `markets.category` still unpopulated (0 rows) mean the headline wallet number was
never decision-grade. The sandbox (entry-priced from live order-book captures) was the real
instrument; the wallet was a tracer for plumbing, not economics.

## 8. What to do next (ranked, consistent with the synthesis)

1. **The one live bet: slow / narrative catalysts.** Days-horizon capture, t0 = catalyst drop
   (filing/ruling/scheduled event), midband + already-moving + document-heavy market selection,
   and ingest `resolution_rules` to gate on "does this increment a still-open, rule-relevant
   perceived probability?" — the only lane where reading beats speed (H8).
2. **Fix the wallet so it can't lie again:** model spread (fill at bid/ask, not mid), refuse
   fills on zero-volume books, use real `fee_rate` semantics, populate `markets.category`.
   Cheap, and makes any future live test interpretable.
3. **Cluster-aware everything:** one position per article cluster (cluster_id already exists),
   and report PnL per cluster, never per trade.
4. **Keep the explorer running as a dataset engine.** 2.2M odds observations in 16 days at ~$2.4/day
   LLM cost; the H8 lane needs precisely this data at longer horizons.
5. **Do not:** more prompt variants (H3), latency arms race (H1), passive MM (H6).

## 9. Theory corner — the transferable lessons

- **News doesn't move prediction markets; it increments narratives already in motion.** The 8×
  signal concentration in midband+moving markets says the repricing the headline "causes" mostly
  happened earlier, on rumor and order flow. An LLM reading headlines is structurally last in line.
- **Friction is the null hypothesis.** Every strategy sweep, the oracle exit, and the live wallet
  decomposition all reduce to: |signal| < spread + fees. Any retail-latency taker strategy must
  beat that inequality *per independent cluster*, not per trade.
- **Effective N is the master statistic.** N=9 → N=42 killed the sweep winner; 845 trades → 52
  clusters reframes the whole history; June 11 was N=1 wearing 74 trades as a costume.
- **Both sides of the book are closed at retail latency:** taking loses to staleness (the move
  already happened), making loses to adverse selection (6.1¢ mean hit vs ~1¢ reward, 15% of
  events >10¢). The only opening is where repricing is *slow* because it requires reading.
- **Zero-volume mids are sirens.** Any backtest or paper wallet that marks to mid on an empty
  book will eventually print a +$1,120 trade on a market no one has ever traded.

## 10. Reproduce `[db]`

```bash
python3 - <<'EOF'
import sqlite3
c = sqlite3.connect('file:irene/data/irene.db?mode=ro', uri=True)
# windows
print(c.execute("""SELECT tc.closed_at>='2026-05-30', COUNT(*), SUM(tc.realized_pnl_usdc)
  FROM trade_closures tc WHERE tc.is_tracer=0 GROUP BY 1""").fetchall())
# decomposition: drift vs fees vs residual (residual = 0 → mid fills, no spread)
# cluster adjustment
print(c.execute("""SELECT COUNT(DISTINCT acm.cluster_id),
  SUM(CASE WHEN x.pnl>0 THEN 1 ELSE 0 END) FROM (
    SELECT acm2.cluster_id cid, SUM(tc.realized_pnl_usdc) pnl
    FROM trade_closures tc JOIN trades t ON t.id=tc.trade_id
    JOIN signals s ON s.id=t.signal_id
    JOIN article_cluster_members acm2 ON acm2.article_id=s.article_id
    WHERE tc.is_tracer=0 GROUP BY 1) x
  JOIN article_cluster_members acm ON acm.cluster_id=x.cid""").fetchall())
# liquidity buckets, fee/size, llm spend: see queries in session 2026-06-12
EOF
```

Key docs: `irene/reports/2026-05-29-irene-trading-viability-synthesis.md`,
`2026-05-28-sandbox-larger-set-sweep.md`, `2026-05-29-sandbox-t3b-friction-floor.md`,
`irene/docs/philosophy.md`, `irene/master-plan.md`.



microstrategy conversy:
bet that microstretgy will sell bitcoins in may, and they did. last day of may, but public in june. it shouldve resolved to true but oracles said false
many angry people, zelensky suit thing.

what about how our trades impact the market itself? minting etc.

how do we simulate minting?

a lot of problems

who are you trying to present to? interviewer
athikom never interviewed.

how much history? i got like 2 weeks in active collection. wait til more data.

in real stock market:
thousands of stocks and several years looks like a lot of data. but in truth you dont have much that much data, and theres correlation
between data points and autocorellation. shit ton of variance. some things dont make sence, 
irrational longer than solvent.

when you present: present what you tried to do, limitations, what you did, and what you are aware of. know your challenges
strogly reccommend documentation. decision process. white paper.

how does productionization work?
- comapnies have the infrastructure, follow what they tell, company api.
- code follows company api.
- present to managers and convince them.
- good research reports.

the harder part is seeing what went on in your head, why these changes were made, rationale. 
how did i change in my next interation. 

not for making money, just for learning. 

main money strategy is market making. taking is easier to understand and simulate. as a market maker, you dont know your fills
if you dont do it right you will get run over. its veyr important for market makers to make markets well.
price well, pool market, two sides bid and ask
you can retreat your bids and drop your ask.

you make money when people cross the market, you make the spread. if you dont make the spread, you lose directional.
trading games jane street. take through the market, and put our your bids at the right price to clean out your position.

market making is good if youre very good.

basically, as a taker focus on market impact you make as the next step. directional bets move the market a lot. market makers focus on bills.

how do you protect against people with an edge.


completely unrelated interesting:
win big i win big i must be so good. variance vro. 100 people do stupid shit and 99 are fucked and 1 are good.

a lot of small hedge funds make 100% return and the rest who did the same and lost their money
common scam: we are gonna recommend you trade, if you lose money we dont charge, we take comm if you win
if you have 100 customers, tell 50 to go long and 50 to go short and collect the money.

t stats.
split into train and test sections.

taboo: very easy to have lookahead, and it will kill your research, 10 years of data how to do train.
split it up randomly? randomly unique markets and dates. split temporally. everything on a date.
hard part. month split and shuffle? run into 2022 and out of sample is pre 2022. great now theres lookahead.
signal is so weak but leakage is so much. only clean way is to do pre and post. hard cutoff 2020, pre and post.

the eend of your pre and the end of your response is on thee same second. there must be a small gap between predictor and response
the gap could introduce lookahead. learnt lessons Athikom. right 51% of the time is extremely high.

takes time to build intuition on scale. is the market really inefficient? if you were really trying to make money, who else is here?

heres some cool tricks:
if you do pre post you have very few data to work with
artificially increase data using 10 years, 3 years of trading split into 2 sections, do the first part and then predict the next and retrain.
more effectvie samplesize. "rolling". common practice in the business. not enough data right now.

will still get burnt. 

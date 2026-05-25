# Research questions — irene profitability

> Two parts:
>
> 1. **Internal findings** — things we can answer by reading our own code,
>    querying our DB, running scripts, or designing measurements. No
>    internet needed.
> 2. **Online research** — things whose answers live outside the repo
>    (Polymarket docs, UMA docs, Gamma reference, papers, blogs).
>
> A question that benefits from **both** angles appears in both sections
> with a `↔` cross-link, so the two answers can be reconciled.
>
> Each item has a one-line **unblocks:** footer pointing to the gate,
> table, config default, or handoff item it would inform.
>
> - **Created:** 2026-05-25 (post cycles 4–7)
> - **Use:** pick a section, batch the work. When an answer changes a
>   config default or unblocks a handoff item, link the resolving commit
>   here.

---

# Part 1 — Internal findings

Questions we can resolve by inspecting our own state — the codebase, the
SQLite DB, the live dashboard, the snapshots, or by writing a one-off
script.

## P1.A — Wallet & PnL accounting

- **i-A1.** Total fees paid across the cycle. Run the audit query from the cycle-7 handoff:
  ```sql
  SELECT close_reason, SUM(entry_fee_usdc + exit_fee_usdc) AS total_fees,
         SUM(realized_pnl_usdc) AS net_pnl
  FROM trade_closures
  WHERE cycle_id = ?
  GROUP BY close_reason;
  ```
  Compare fees-paid vs gross-PnL bucket-by-bucket. If fees > gross on a `close_reason`, that bucket is structurally negative — flag the gate that admits it.
  - **unblocks:** profitability attribution; cycle-8+ tuning prioritisation.

- **i-A2.** Predicted-vs-realised fee check. For a sample of closed trades, recompute `shares × fee_rate × p × (1−p)` by hand from `trades` + `trade_closures`, compare against `entry_fee_usdc + exit_fee_usdc`. ↔ O-A1, O-A4.
  - **unblocks:** confidence in `closer.py` fee math (or detection of a bug).

- **i-A3.** Identity-check audit over the full cycle. Dashboard reports `identity OK`/`DRIFT`. Sweep every minute of `dashboard.txt` history to confirm `DRIFT` never appears.
  - **unblocks:** confidence in cycle-3 cash-clip and cycle-7 fee math interaction.

## P1.B — Gamma & market data

- **i-B1.** Curl Gamma directly for a known-high-volume market (e.g. Rihanna), pretty-print the JSON, identify which key holds 24h dollar volume. Compare against `markets._parse_gamma_row:184`'s `float(raw.get("volume") or 0.0)`. Add a one-shot `scripts/inspect/gamma_sample.py` if convenient. ↔ O-B1.
  - **unblocks:** handoff follow-up #1 (volume parser; `thin_market` sub-rule).

- **i-B2.** Curl Gamma for a *resolved* condition_id (find one in our `markets` table where `active=0`). Inspect `outcomePrices`, `closed`, `umaResolutionStatus`. Determine the field shape we need to read in the cycle-7 auto-close path. ↔ O-B2.
  - **unblocks:** handoff follow-up #2 (proper $1/$0 settlement instead of entry_mid).

- **i-B3.** Distinct `feeType` values seen across our `markets` table. ```sql SELECT DISTINCT json_extract(raw_gamma, '$.feeType') FROM markets;``` (or query whatever field stores the raw payload). Then hand-map each to our category table; the gaps are what `derive_category_from_feeType` must handle. ↔ O-B3.
  - **unblocks:** handoff follow-up #3 (category derivation).

- **i-B4.** Gamma update latency. Run a script that polls Gamma's `lastTradePrice` once a second and CLOB's `/midpoint` once a second for a single liquid market, log both with timestamps, compute the offset.
  - **unblocks:** decision on whether to add a CLOB-only market index for hot markets.

## P1.C — CLOB execution realism

- **i-C1.** Bid–ask spread distribution across our live universe. Pull CLOB orderbooks for ~100 markets we currently track; compute (best_ask − best_bid) per market; bucket by `volume_usdc` (when fixed). Median and 90th-percentile spread by liquidity tier. ↔ O-C1.
  - **unblocks:** whether `fee_buffer` needs an explicit spread term in the edge calc.

- **i-C2.** Slippage simulation against the visible book. For each market in the sample, simulate buying $25, $50, $100, $200 against the visible asks; report the effective fill price vs mid.
  - **unblocks:** realism of `min_position_usdc = $25` and `max_position_fraction = 0.10`; calibration of cycle-8+ edge floor.

## P1.D — News→repricing latency (the philosophy.md core metric)

- **i-D1.** Article→trade latency distribution across the cycle. We already have per-stage `Timer` data and `throughput.txt`. Compute p50/p95/p99 for each of: ingest.poll → article.ingested, article.ingested → article.deduped, article.deduped → signal.created, signal.created → trade.handle, trade.handle → execute. Where's the dominant tail?
  - **unblocks:** philosophy.md's "2-second budget" claim; informs which stage to optimise next.

- **i-D2.** Repricing half-life on our own trades. For each executed trade, query CLOB `/midpoint` at +1m, +5m, +15m, +1h. Plot mid vs time per trade in `data/dashboard/plots/missed_edge/` (the directory already exists). Median time to "first 50% movement toward p̂". ↔ O-E1.
  - **unblocks:** empirical defensibility of the `closer.poll_seconds = 15` and `cfg.closer.timeout` (24h).

- **i-D3.** Counterfactual: would we have made money if we'd fired faster? Take rejected trades whose direction-gate was rejected for `min_confidence` borderline cases (digit just below floor) and ask what the mid did over the next hour. If consistently in our direction, the gate is too tight.
  - **unblocks:** `cfg.trade.min_confidence` default; LLM prompt tuning.

## P1.E — Decision-funnel diagnostics

- **i-E1.** Funnel histogram per cycle. We have `funnel.txt`. Per cycle, what fraction of articles survive each gate? Where is most volume lost? Is the relative shape stable across cycles?
  - **unblocks:** prioritisation of cycle 8+ work; finding the gate with the most upside.

- **i-E2.** R-rate stratification. Among LLM calls returning R, split by (article source, market category, cosine bucket). If R-rate varies hugely across slices, the gate before the LLM (similarity, freshness, entity-overlap) is the right knob; if R-rate is uniform, it's a prompt/model issue. ↔ O-H2.
  - **unblocks:** handoff follow-up #4 (R-rate); Phase F prompt design.

- **i-E3.** Cycle-7 `market_news_imbalance` audit. Look at every signal rejected by the new gate in cycles 7+. For each, hand-check 5 samples: does the rejection look right? Are we losing real edge? Are any sub-rules over-firing?
  - **unblocks:** sensitivity sweep on cycle-7 thresholds (15/85, 180-day, $10k).

## P1.F — Param sensitivity (codebase-side, not requiring live trades)

- **i-F1.** Gold-set sweep of `cfg.match.min_similarity`. Re-run `scripts/eval/extraction_quality.py` (or build a similar script on `signals` history) at cosine floors of 0.30, 0.32, 0.35, 0.40, 0.45. Plot precision/recall. ↔ O-F3.
  - **unblocks:** defensibility of `cfg.match.min_similarity`.

- **i-F2.** Replay-based edge-floor sweep. Use `scripts/replay/accuracy.py` against a snapshot DB, replay at `min_edge = 0.02, 0.03, 0.04, 0.06, 0.08`. Net-PnL per setting.
  - **unblocks:** defensibility of `cfg.trade.min_edge`.

- **i-F3.** Kelly-fraction sweep. Same as F2 but vary `kelly_fraction` (0.10, 0.15, 0.25, 0.50, 1.0). Equity curve max-drawdown per setting. ↔ O-F1.
  - **unblocks:** defensibility of `cfg.kelly_fraction`.

## P1.G — Source attribution

- **i-G1.** Which sources produced the trades we actually fired? Join `articles → signals → trades`; group by `articles.source`. Top-10 sources by trade count and by net PnL. Compares the value of expensive sources (NYT/FT homepage scrape) vs cheap RSS. ↔ O-G1.
  - **unblocks:** cycle 8+ scraper-v2 source-list curation.

- **i-G2.** Source latency. Per source, median age of the article (article published → ingest seen). Is there a tail of slow sources we can drop without losing trade volume?
  - **unblocks:** cycle 8+ source-list pruning.

## P1.H — LLM behavior

- **i-H1.** Cache-hit potential. Query `llm_calls` for the prompt-sha distribution. What fraction of calls share a sha with another call? If high, our cache is under-utilised. If low, prompt-templating is too article-specific. ↔ O-H3.
  - **unblocks:** LLM cost projection; possible prompt-templating refactor.

- **i-H2.** Per-model dry-run. Replay our gold set against `claude-haiku-4-5`, `gpt-5-mini`, `gemini-2.5-flash`. Compare R-rate, latency p50/p95, and per-call cost. ↔ O-H1.
  - **unblocks:** `cfg.ai.model` default; budget projection.

---

# Part 2 — Online research

Questions whose answers come from the internet — Polymarket docs, UMA
docs, Gamma reference, academic papers, market-microstructure blogs.

## P2.A — Polymarket fee model (primary sources)

- **O-A1.** Does Polymarket's published fee schedule confirm the formula `fee = shares × fee_rate × p × (1−p)`, is it per-leg or round-trip, is `p` the trade price (taker's fill) or the displayed mid, and is any fee charged at resolution when winning shares pay $1? ↔ i-A2.
  - **unblocks:** correctness of `closer.py` `_compute_round_trip_fee`.

- **O-A2.** What is the canonical `feeType` → `fee_rate` mapping on Polymarket, and which primary source documents it (our hardcoded table — crypto 0.072, politics/finance/tech 0.04, sports 0.03, geopolitics 0.00, default 0.05 — came from a single reading of `docs.polymarket.com`)? ↔ i-B3.
  - **unblocks:** handoff follow-up #3.

- **O-A3.** Does Polymarket pay a maker rebate (negative fee), zero, or something just below taker — and if a rebate exists, how big is it?
  - **unblocks:** execution-model design for the real-money path (post a limit vs always cross).

- **O-A4.** Is the Polymarket taker fee applied to size in USDC notional, or to share count (our formula assumes shares)? ↔ i-A2.
  - **unblocks:** confidence in cycle-7 fee math.

## P2.B — Gamma API schema (docs + sample payloads)

- **O-B1.** Which field on Gamma's `/markets` endpoint carries 24h dollar volume (`volume`, `volumeNum`, `volume24hr`, `volume24hrClob`), is the value in USDC notional or shares or something else, and what does a sample payload for a high-volume condition_id look like? ↔ i-B1.
  - **unblocks:** handoff follow-up #1.

- **O-B2.** What is the resolved-market payload shape on Gamma — specifically the fields `outcomePrices`, `closed`, `acceptingOrders`, `umaResolutionStatus`, `resolvedBy` — and is there a sample payload from a known-resolved market? ↔ i-B2.
  - **unblocks:** handoff follow-up #2.

- **O-B3.** Is `feeType` the canonical fee-rate driver on Gamma, or is the `tags` array authoritative — and are tags free-text or drawn from a fixed taxonomy? ↔ i-B3.
  - **unblocks:** handoff follow-up #3.

- **O-B4.** What rate limits (per minute / per IP / per API key), page size on `/markets`, pagination scheme (offset, cursor), and backoff guidance does Gamma publish?
  - **unblocks:** safety of `MarketsStage` polling cadence.

## P2.C — CLOB mechanics (docs)

- **O-C1.** What is the published or blog-aggregated bid–ask spread distribution on the Polymarket CLOB by market-liquidity tier, and how does it compare to our own measurement? ↔ i-C1.
  - **unblocks:** whether `fee_buffer` needs an explicit spread term.

- **O-C2.** What is the minimum trade size in USDC on the Polymarket CLOB, and is our `min_position_usdc = $25` above the platform floor?
  - **unblocks:** safety of cycle-3 cash-clip logic.

- **O-C3.** What order types does the Polymarket CLOB support (GTC, FOK, IOC, post-only, market-buy), and how does each behave on partial fills during a fast move?
  - **unblocks:** real-money execution wrapper design.

- **O-C4.** What is the typical fill latency for small market orders ($25–$200) on moderately liquid Polymarket markets — sub-second or multi-second? ↔ i-D1.
  - **unblocks:** real-money soak design.

## P2.D — Market resolution and UMA oracle

- **O-D1.** Who resolves Polymarket markets — is it always UMA's optimistic oracle, or are some auto-resolved on-chain — what is the dispute-window length, and what is the median wall-clock from `closed=true` to final `outcomePrices`?
  - **unblocks:** correctness of cycle-7 auto-close-on-deactivation; informs whether to wait past `closed=true`.

- **O-D2.** How does Polymarket interpret ambiguous deadlines like "by June 30" — UTC midnight on the 30th, end of the 30th, or 1 second past midnight on July 1?
  - **unblocks:** correctness of `days_to_expiry` math and the `near_expiry` sub-rule.

- **O-D3.** How often are UMA resolutions disputed and overturned, and are there published incident counts or override rates?
  - **unblocks:** prioritisation of handoff follow-up #2.

## P2.E — News→repricing empirical bounds

- **O-E1.** Are there published empirical estimates of the repricing half-life on Polymarket after a breaking-news event (paper, substack, or analysis with concrete numbers) that we can use to calibrate our `closer.timeout`, edge floor, and BREAKING-bypass logic? ↔ i-D2.
  - **unblocks:** numeric defensibility of cycle 7 and cycle 9 design.

- **O-E2.** What is the operating latency floor of high-volume Polymarket traders — multi-second polling like irene, or sub-second WebSocket with co-located bots?
  - **unblocks:** realism of philosophy.md's 2-second budget; ceiling on irene's edge window.

## P2.F — Strategy / theory literature

- **O-F1.** What Kelly fraction do published backtests recommend for prediction-market strategies, and is there a specific anchor for binary event markets (we use 0.25)? ↔ i-F3.
  - **unblocks:** defensibility of `cfg.kelly_fraction`.

- **O-F2.** What edge-above-fees threshold has been empirically profitable in published event-driven prediction-market strategies, and does it support our `min_edge = 0.04` floor?
  - **unblocks:** defensibility of `cfg.trade.min_edge`.

- **O-F3.** Is there prior academic or industry work on news-to-event matching thresholds — specifically on the right cosine floor and the right "new event" distance cutoff? ↔ i-F1.
  - **unblocks:** defensibility of `cfg.match.min_similarity` and `cfg.dedupe.breaking_distance`.

- **O-F4.** Is news-arb a low-variance grinding strategy or a power-law heavy-tailed one, and how long a soak does the literature suggest before claiming profitability with confidence?
  - **unblocks:** design of the cycle-8+ profitability test.

## P2.G — News-source landscape

- **O-G1.** Which wire sources (AP, Reuters, Bloomberg, FT, Axios) are highest-signal-per-dollar for prediction-market-tradable events, and which expose free RSS feeds we are not currently polling? ↔ i-G1.
  - **unblocks:** scraper-v2 cycle 8+ source-list curation.

- **O-G2.** What are the terms-of-service, robots.txt directives, and rate limits for scraping tier-1 news sources (AP, Reuters sitemap_news, FT, NYT, Bloomberg), and which have explicit "no scraping" policies we'd be tripping?
  - **unblocks:** legal hygiene; source blacklist risk.

- **O-G3.** Do any tier-1 news sources expose real-time push feeds (WebSocket or Server-Sent-Events) as an alternative to RSS polling?
  - **unblocks:** cycle 8+ ingest latency budget; potential replacement of RSS polling with a push path.

- **O-G4.** What is the commercial wire-service pricing for solo users on Reuters Connect, Bloomberg Terminal, AP Wire, and Dow Jones Newswires (rough $/month range)?
  - **unblocks:** strategic decision on 2026-Q3+ wire subscription.

## P2.H — LLM choice (benchmarks + lit)

- **O-H1.** Which low-latency LLM is currently best for short-form news-vs-market classification, according to public benchmarks comparing Claude Haiku 4.5, GPT-5 mini, DeepSeek, and Gemini 2.5 Flash on similar tasks? ↔ i-H2.
  - **unblocks:** handoff follow-up #4; `cfg.ai.model` defensibility.

- **O-H2.** Is a 70–80% R-rate a known failure mode for asymmetric reject-leaning classifiers, and is there prior work on calibrating this kind of classifier? ↔ i-E2.
  - **unblocks:** Phase F prompt rewrite vs model swap vs accept-and-soak.

- **O-H3.** What is the theoretical cache-hit ceiling for prediction-market prompts given the article-vs-question structure of the call? ↔ i-H1.
  - **unblocks:** cost-budget projection for 24/7 operation.

## P2.I — Legal and operational

- **O-I1.** Is Polymarket legally accessible from the user's jurisdiction, and what are the KYC requirements, geofence behaviour, VPN-friendliness, and account-freeze risk for bot traffic?
  - **unblocks:** real-money cutover path (cycle 10+).

- **O-I2.** What is the cheapest route to bridge USDC onto Polygon for Polymarket, what is the confirmation latency, and are there hidden fees?
  - **unblocks:** real-money cutover path.

- **O-I3.** What alternative prediction-market venues (Kalshi, Manifold, Smarkets, futarchy markets) exist, and how do their liquidity, fee structures, and APIs compare to Polymarket's?
  - **unblocks:** strategic backup; informs whether to keep irene Polymarket-specific.

- **O-I4.** How are prediction-market winnings taxed in the user's jurisdiction (realised per trade, net annual, or other)?
  - **unblocks:** real-money cutover path.

- **O-I5.** What are the cost-effective 24/7 hosting options (VPS, edge worker, home server) for the cycle 10+ real-money path?
  - **unblocks:** operational cost projection.

---

## How to use this list

Pick a section, batch the work, attach answers as a sibling file
(`research_answers_A_fees.md`, `internal_findings_B_gamma.md`, etc.).
When an answer changes a config default or unblocks a handoff item,
update both the relevant source file and the `unblocks:` footer here to
point to the resolving commit.

**Overlap pairs to reconcile** (when both sides are answered, compare):

| Internal | Online | What you'd learn from reconciling |
|---|---|---|
| i-A2 (predicted vs realised fee) | O-A1, O-A4 (formula correctness) | whether our formula matches reality |
| i-B1 (Gamma curl for volume field) | O-B1 (Gamma docs) | which field is canonical |
| i-B2 (Gamma curl for resolved market) | O-B2 (Gamma docs) | the resolved-market shape |
| i-B3 (distinct feeTypes in our DB) | O-B3 (Gamma docs) | category-derivation completeness |
| i-C1 (our spread measurement) | O-C1 (published norms) | whether our liquidity universe is typical |
| i-D2 (our repricing half-life) | O-E1 (literature) | whether our edge window is competitive |
| i-E2 (our R-rate by slice) | O-H2 (asymmetric-classifier lit) | model vs prompt vs gate diagnosis |
| i-F1 (gold-set cosine sweep) | O-F3 (news-matching lit) | defensibility of `min_similarity` |
| i-F3 (Kelly-fraction sweep) | O-F1 (Kelly lit) | defensibility of `kelly_fraction` |
| i-G1 (sources by realised PnL) | O-G1 (source rankings) | what to add or drop from cycle 8+ |
| i-H1 (cache-hit potential) | O-H3 (lit ceiling) | how much cost reduction is on the table |
| i-H2 (per-model dry-run) | O-H1 (benchmarks) | which model to default to |

**Open follow-ups in the cycle-7 handoff** that map cleanly to a question:

- handoff #1 (volume parser) → **i-B1** / **O-B1**
- handoff #2 (Gamma outcome lookup) → **i-B2** / **O-B2**, **O-D1**, **O-D3**
- handoff #3 (category derivation) → **i-B3** / **O-A2**, **O-B3**
- handoff #4 (R-rate) → **i-E2** / **O-H1**, **O-H2**
- handoff #5 (DKG path imbalance gate) — codebase-only, not here.
- handoff #6 (per-function timings) — codebase-only, not here.
- handoff #7 (CLT required-n) — codebase-only, not here.

# Research answers — irene

> Distilled answers from external research (RAG AI, papers, docs).
> Each entry simplifies the raw answer into **primary info** + **maps to** + **plan stub**.
> Sibling to `research_questions.md`. Sectioning mirrors that doc (A–I); a `W#` block at the end captures systemic weakspots flagged outside the question list.
>
> **Created:** 2026-05-25.
> **Convention:** ✅ verified · ⚠️ partial / needs local verification · ❌ contradicted.

---

## Status log (most-recent first)

- **2026-05-25** — batch 5: Section D (Resolution / oracle) — D1, D2 answered; D3 evidence-gap (no public base rate).
- **2026-05-25** — batch 4: Section C (CLOB mechanics) — C1 priors only (no native historical spread feed); C2, C3, C4 answered.
- **2026-05-25** — batch 3: Section B (Gamma API) — B1–B5 answered (B1, B2 partial).
- **2026-05-25** — batch 2: Section A (Polymarket fees) — A1–A4 answered.
- **2026-05-25** — batch 1: philosophy critique — 7 new weakspots W1–W7.

---

## A — Polymarket fee model

### A1 — Fee formula and resolution  ✅
**Primary info**
- `fee = shares × fee_rate × p × (1−p)` is Polymarket's official, newly corrected calculation.
- `p` = **actual taker fill price** at match time, **not** displayed mid.
- Both entry and exit pay taker fees **only if** the bot crosses the spread.
- **No fee at resolution.** Winning shares pay exactly $1.00.

**Maps to:** `closer.py:_compute_round_trip_fee`, `realized_pnl_usdc`.

**Plan stub:** verify `closer.py` is using the executed fill price for `p`, not entry_mid. If it uses mid, swap to fill price. Resolution path needs no fee adjustment.

---

### A2 — Category rates and where to fetch them  ✅
**Primary info**
- Hardcoded category table values match Polymarket's April 2026 fee parameters.
- `fee_rate` is defined as the **peak effective fee at p=0.5**, so 0.072 → 1.80% peak (0.072 × 0.5 × 0.5).
- **Rates are per-market**, not universally per-category. Don't assume a category implies a rate.
- Authoritative source: `getClobMarketInfo(conditionID)` via the CLOB API. Don't scrape market pages.

**Maps to:** handoff #3 (category derivation); `closer.py` fee_rate lookup; `markets.py` parser.

**Plan stub:** retire the hardcoded category→rate table for any market we actually hold. At market discovery, query `getClobMarketInfo(conditionID)` and persist the per-market fee_rate; refresh periodically. Keep the category table only as a fallback for un-fetched markets.

---

### A3 — Maker rebates  ✅
**Primary info**
- Makers pay **zero** trading fees.
- Makers earn **rebates of 20–25%** of the taker fee paid by their counterparty.
- Rebates are proportional to your share of executed maker liquidity.
- **Paid out daily in USDC.**

**Maps to:** cycle-9+ execution model design.

**Plan stub:** real-money execution defaults to limit-post (maker); cross the spread only when the latency window demands it. Add maker rebate as a **positive** PnL term in cycle-9+ math. New question for the execution layer: how do we decide post-vs-cross per signal given our latency budget?

---

### A4 — Shares vs notional  ✅
**Primary info**
- Fee formula's `C` is share count; output is USDC.
- `closer.py`'s shares-based math is correct.

**Maps to:** `closer.py`.

**Plan stub:** no change.

---

## B — Gamma API schema

### B1 — 24h volume field  ⚠️ (no live payload captured)
**Primary info**
- Best evidence: **`volume24hr`** is the 24-hour volume in **USDC notional**.
- `volumeNum` looks like **cumulative** market volume, not rolling 24h.
- Confirming signals: Gamma docs sort `/markets` with `order=volume24hr`; a published Python tutorial reads Gamma and displays `volume24hr` as "Volume 24h" in dollars.
- **Verification gap:** no live raw payload captured this batch — we should capture one ourselves from a known-high-volume conditionID (e.g. the Rihanna market) before committing.

**Maps to:** handoff #1 (volume parser); `markets.py:_parse_gamma_row:184`.

**Plan stub:** change parser to read `volume24hr` (USDC) for the thin-market gate; treat `volumeNum` as lifetime volume only. Run a one-shot capture against a high-volume conditionID and diff the payload before merging.

---

### B2 — Resolved-market payload  ⚠️ (no verified live sample)
**Primary info**
- Docs reference `closed`, `closedTime`, plus `outcomePrices`, `acceptingOrders`, `umaResolutionStatus`, `resolvedBy`.
- No live settled-market payload captured this batch.
- **Safe rule until verified:** close on explicit `closed=true` / resolution signal **plus** final settlement price from CLOB — not entry_mid, not a stale Gamma mid.

**Maps to:** handoff #2 (auto-close at correct $1/$0 instead of entry_mid).

**Plan stub:** rather than blocking on Gamma resolved-schema clarity, do our own capture (pick a known-expired election market). Until that capture exists, the close path should prefer the CLOB settlement read over Gamma `outcomePrices`.

---

### B3 — Category indicator  ✅
**Primary info**
- Gamma **does** have a top-level `category` field.
- Listing API supports `tag_id` filters.
- For **fee-rate lookup**: prefer the per-market CLOB fee param (see A2); use `feeType` only as fallback.
- For **market grouping/taxonomy**: use `category` first, `tag_id` second.

**Maps to:** handoff #3 (category derivation).

**Plan stub:** stop deriving category from `feeType`. Read `category` directly. Keep `feeType` only as a fee-rate fallback when CLOB lookup is unavailable.

---

### B4 — Rate limits and pagination  ✅
**Primary info**
- `/markets`: **300 req / 10s**.
- `/markets` + `/events` combined: **900 req / 10s**.
- Overall Gamma: **4000 req / 10s**.
- Pagination: **cursor-based** via `after_cursor` / `next_cursor`. Example page size: `limit=100`.
- Cloudflare also rate-limits at the edge — exponential backoff with jitter is still required regardless of nominal quota.

**Maps to:** `MarketsStage` poll cadence (`markets.py`).

**Plan stub:** measure current `/markets` request rate; confirm it's well under 300/10s. Switch to cursor pagination if not already. Wrap requests in exponential-backoff-with-jitter on 429 / Cloudflare blocks.

---

### B5 — Update latency  ✅
**Primary info**
- No official SLA from Polymarket.
- One integration changelog reports Gamma response variance **exceeding 30 seconds under load**.
- Public guidance: **Gamma for discovery/metadata only**; **CLOB / WebSocket for real-time prices**.

**Maps to:** `closer.py` mid reads, `trade.py` mid reads, any future hot-market index.

**Plan stub:** audit every price read on the hot path. `closer.py` already uses CLOB — verify nothing in `trade.py` still reads Gamma mid. If `trade.py` does, replace with CLOB.

---

### Parser policy summary (consolidated from B1–B5)
| Field intent          | Source                                                |
|-----------------------|-------------------------------------------------------|
| 24h volume            | Gamma `volume24hr` (USDC)                             |
| Lifetime volume       | Gamma `volumeNum`                                     |
| Category / taxonomy   | Gamma `category` (fallback `tag_id`)                  |
| Fee rate (per market) | CLOB `getClobMarketInfo(conditionID)` (fallback `feeType`) |
| Hot-path price        | CLOB / WebSocket — **never Gamma**                    |
| Discovery / listing   | Gamma `/markets` (cursor paginated, backoff)          |

---

## C — CLOB mechanics and execution realism

### C1 — Spread by liquidity tier  ⚠️ priors only (no native historical feed)
**Primary info**
- Polymarket's historical queries expose **midpoints, not bid/ask**, so spread-by-volume work needs live `/book` snapshots or a third-party spread-history feed — we have to collect this ourselves.
- Practical priors from explainer/integration sources:
  - **Liquid (~$1M volume): ~1–2¢ spread.**
  - **Medium liquidity: ~3–5¢ spread.**
  - **Thin (~$10k volume): ~10¢+ spread, niche markets can show 10–15%.**
- Use these as **seed calibration only**; replace with our own bucketed medians once we collect `/book` snapshots.

**Maps to:** edge-gate math (`trade.py` gate cascade), cycle-9 execution realism.

**Plan stub:** add a `spread_cost_estimate` term to the edge gate using the seed priors keyed off `volume24hr` bucket. Start collecting `/book` snapshots in a sidecar table so we can replace priors with empirical medians within a cycle or two.

---

### C2 — Minimum trade size  ✅
**Primary info**
- Polymarket exposes **`min_order_size`** as a **per-market** field on the order book — fetch at runtime, don't hardcode.
- Outside-docs reference: typical floor around **$1**.
- Resting **GTC/GTD orders also face a 5-share minimum**; FOK/FAK are governed only by per-market `min_order_size`.
- Collateral note: current USDC collateral is **migrating to pUSD backed by USDC** — watch for this in executor.

**Maps to:** `cfg.min_position_usdc` (currently $25); cycle-3 cash-clip-then-gate; future executor.

**Plan stub:** `min_position_usdc = $25` is safely above the floor — keep as-is. Live executor must read `min_order_size` per market at arm-time and reject below the per-market floor. Note pUSD migration in a comment near the collateral handling.

---

### C3 — Order types  ✅
**Primary info**
- **All Polymarket orders are limit orders.** There is no native "market order" primitive — what looks like a market order is just an aggressive limit priced to execute immediately.
- Documented types: **GTC, GTD, FOK, FAK**.
- **Post-only** orders are rejected if they would cross the spread.
- **FAK = closest to IOC**: fills what's immediately available, cancels the rest.
- **FOK = all-or-nothing.**
- **GTC/GTD** can leave residual exposure resting on the book.
- An aggressive order can walk multiple price levels up to your limit if the best quote is too small.

**Maps to:** trade.py's implicit "IOC-at-mid" assumption; real-money execution wrapper design.

**Plan stub:** replace "IOC-at-mid" with **FAK + max-slippage cap**: immediate execution, allows partials, no accidental resting exposure. Set the limit price relative to current best bid/ask at arm-time, not mid.

---

### C4 — Fill latency  ✅
**Primary info**
- No official public p50/p95 table.
- System is **off-chain matched**, sub-second matching is intended behavior.
- Public latency guides attribute:
  - ~70–85ms exchange-side processing,
  - sub-50ms market-data updates,
  - 50–100ms network from favorable locations (hundreds of ms from poor placement).
- **Engineering assumption for $25–$200 aggressive orders on moderately liquid markets:** usually sub-second, often low-hundreds of ms on decent infra, occasionally several hundred ms or worse on bad paths.

**Maps to:** cycle-8+ real-execution latency budget; `trade.py` stale-quote check.

**Plan stub:** `trade.py` decision step must **stale-check against fresh best bid/ask** (not mid) right before send. Add a freshness budget (e.g. quote ≤ 500ms old). Cycle-8+ latency budget for real execution = ~200–500ms on top of `ai.direction` cost.

---

### Implementation changes summary (from C1–C4)
- **Edge gate:** `edge_after_costs = model_edge − taker_fee − 0.5 × spread − depth_slippage`. Midpoint-based PnL is too optimistic.
- **Execution:** **FAK-first** with max-slippage cap. No "IOC-at-mid" — that mode doesn't exist natively.
- **Market admission at arm-time:** fetch `min_order_size`, `tick_size`, top-of-book depth, current best bid/ask. Re-evaluate edge against the **executable side**, not mid. Reject if edge disappears.

---

## D — Market resolution and oracle

### D1 — Resolver identity and timing  ✅ (timing) / ✅ (resolver)
**Primary info**
- Polymarket markets resolve through **UMA's optimistic oracle**.
- Some numeric markets may use auto-data feeds (e.g. Chainlink) for the factual input, but **UMA still finalizes** — don't assume a separate auto-resolve settlement path unless the specific market explicitly proves it.
- **Uncontested dispute window: 2 hours** after a proposal is submitted.
- **Disputed path:** ~24–48h debate + ~48h UMA vote → several days beyond `closed`.
- **No public median** for time from `closed` to final `outcomePrices`; depends on when the result is available, proposal timing, and whether challenged.
- **State semantics for the bot:**
  - `market_closed` → stop opening new positions.
  - `proposed_resolution` → enter post-close waiting state.
  - `finalized_resolution` / `outcomePrices` → trigger terminal settlement logic.
- **Do NOT treat `closed=true` as final settlement.**

**Maps to:** handoff #2 (auto-close at $1/$0); `closer.py` close path; B2.

**Plan stub:** add a **three-state** close state machine (closed → proposed → finalized). Wait for `finalized_resolution` (or `outcomePrices` proxy after the 2h window with no dispute signal) before booking $1/$0. Until then, keep cycle-7's pre-resolution exit at last CLOB mid as the conservative fallback.

---

### D2 — Ambiguous deadlines  ✅
**Primary info**
- Live Polymarket pages commonly write date rules as **"by June 30, 2026, 11:59 PM ET"** — **Eastern Time, not UTC**.
- Time-window markets written as **"12:00 AM ET" through "11:59 PM ET"** — named day = full ET day.
- **Frontend and API both use Eastern Time** (confirmed via March 2026 DST reporting bug).
- **Robust rule:** parse the rule text first; if title says "by June 30" and rules are unavailable, the **least-wrong fallback is end-of-day June 30 ET** (i.e. just before July 1 12:00 AM ET).

**Maps to:** `days_to_expiry`; `near_expiry` sub-rule of `market_news_imbalance`; cycle-7 fee-tax flag for terminal markets.

**Plan stub:** switch deadline parsing to **ET** by default. Parse the rule text when available; fall back to end-of-day ET on title-only deadlines. Add a unit test that "by June 30" maps to `2026-06-30 23:59:59 ET`.

---

### D3 — UMA dispute frequency  ⚠️ evidence gap (no public base rate)
**Primary info**
- Official materials describe the dispute mechanic but **publish no dispute-rate or overturn-rate statistic**.
- Confirmed outcomes of disputes: proposer loss, disputer win, "Too Early", or rare "Unknown/50-50".
- Once UMA finalizes, the result is **immutable** on Polymarket.

**Maps to:** prioritisation of handoff #2.

**Plan stub:** without a base rate, prioritize **D1/D2 fixes first** (wait for finalized resolution; ET deadlines). Keep cycle-7's pre-resolution exit as the conservative risk-control choice for ambiguous or terminal markets. Revisit D3 only if we observe disputes affecting our own positions.

---

## E–I — open

No external answers in these batches. Questions remain in `research_questions.md`.

---

## W — Systemic weakspots (philosophy critique, batch 1)

> Not direct answers to A–I; these are architectural critiques that introduce
> new weakspots. Each is the RAG AI's claim — verify against the live code
> before committing a fix.

### W1 — Serial LLM calls destroy the latency moat  ⚠️ likely accurate
**Claim:** philosophy.md targets a 200ms latency budget but acknowledges top-K=25 candidates are evaluated serially (`philosophy.md:170–173`). ~150ms TTFT × 25 ≈ 3.75s.

**Primary info:** if true, the evaluation step alone is ~20× the stated budget.

**Fix path:** `asyncio.gather` the K calls with a bounded semaphore (e.g. 8 concurrent).

**Maps to:** `app/stages/trade.py` subscriber task; philosophy.md serialisation note.

**Plan stub:** confirm current code matches philosophy.md's "serialised" caveat. Convert to bounded `asyncio.gather`. Measure before/after `ai.direction` aggregate latency on the throughput.txt run.

---

### W2 — `p_hat = 1.0` → Full Kelly → ruin  ⚠️ likely accurate, partly mitigated
**Claim:** philosophy.md maps `Y → confidence = 1.0` (line ~146). Feeding 1.0 into Kelly says bet 100% of bankroll.

**Primary info:** single false positive (LLM hallucination, news retraction, UMA pedantic resolution) → bankroll wipe. We already use `cfg.kelly_fraction = 0.25`, which clips exposure, but does not address the fact that the input itself is mis-calibrated.

**Fix path:** cap `p_hat` at an empirical historical win-rate (e.g. 0.85), then apply fractional Kelly. Both, not either.

**Maps to:** F1 (Kelly fraction); `trade.py` sizing; philosophy.md p_hat-mapping table.

**Plan stub:** verify fractional Kelly is unconditionally applied. Add explicit cap (`p_hat = min(confidence, p_hat_cap)`) with `p_hat_cap` in config, default ~0.85. Audit-log every time the cap binds.

---

### W3 — `logit_bias` with raw character keys is invalid  ⚠️ needs verification at call site
**Claim:** philosophy.md shows `logit_bias={'Y':100,'N':100,'U':100}`. LLM APIs require **token IDs (ints)**, not raw chars. One letter maps to multiple token IDs depending on leading space and case (`Y`, ` Y`, `y`).

**Primary info:** raw-char keys are silently ignored or rejected by most providers; the "one of Y/N/U guaranteed" property may not hold.

**Fix path:** either (a) tokenize via the provider's tokenizer (e.g. `tiktoken`) to get all relevant int IDs, or (b) use Structured Outputs / JSON Schema enum to enforce the choice.

**Maps to:** H1; the LLM call shim around `ai.direction`.

**Plan stub:** read the actual call site. If raw chars are being sent, switch to structured-output / enum — simpler than maintaining tokenizers across providers via OpenRouter.

---

### W4 — OpenRouter routing latency  ⚠️ likely accurate
**Claim:** aggregators add 20–100ms vs direct provider.

**Primary info:** if the whole thesis is latency, the aggregator is a structural tax.

**Fix path:** A/B probe OpenRouter vs direct (Anthropic, OpenAI, Groq) for the exact `ai.direction` payload. Move to direct for production. Consider in-house vLLM later.

**Maps to:** H1, E2; `throughput.txt` regime.

**Plan stub:** add an A/B latency probe (same prompt, OpenRouter vs Groq vs direct Anthropic) and surface the deltas in the throughput report. Decide based on numbers.

---

### W5 — Zero-shot single-char loses CoT accuracy  ⚠️ unmeasured
**Claim:** Polymarket rules carry date cutoffs and pedantic conditions ("Did FDA approve drug X for general use before July 15?"). Removing the LLM's scratchpad pushes complex conditional logic into a single forward pass and raises the false-positive rate.

**Primary info:** speed gain may not net out against accuracy loss; we currently can't quantify either side.

**Fix path:** fine-tune a small fast model on Polymarket resolution logic — keep accuracy without CoT latency. Or: targeted CoT only for high-stake markets.

**Maps to:** H1, H2; cycle-8+ accuracy work.

**Plan stub:** before any fine-tune, instrument the false-positive rate on a gold set. If acceptable, defer. If not, evaluate (a) minimal CoT gated on stake size vs (b) fine-tune.

---

### W6 — "Price didn't move" misses the priced-in case  ⚠️ likely accurate
**Claim:** philosophy.md's impact tracker interpretation (lines 110–119) lists only "wrong wording" or "no one read it". Misses **"already priced in"** — markets react to rumours, X/Twitter leaks, and insider action before traditional news.

**Primary info:** a trade at 0.30 that stays at 0.30 may be the top of a stale cycle, not a missed signal.

**Fix path:** add a "priced-in" hypothesis to the impact tracker. Detect via pre-news price drift (cumulative move in the N minutes before article timestamp).

**Maps to:** `price_impacts` table and its analysis; philosophy.md interpretation block.

**Plan stub:** add `pre_news_drift_15m` (and maybe `pre_news_drift_60m`) to `price_impacts`. Cross-tabulate with post-trade movement to classify outcomes into {decisive, late, wrong-wording, no-readers}.

---

### W7 — Prompt cache doesn't engage on tiny prompts  ✅ accurate (Anthropic), partial elsewhere
**Claim:** philosophy.md says the static system prompt is "cached by the provider". Anthropic prompt cache requires ≥1024 tokens. A 20-word system prompt won't cache.

**Primary info:** the implied latency benefit is fictional on Anthropic. Time-to-first-token is dominated by network + compute, not parsing a few tokens. Other providers have different thresholds; we may not be on Anthropic via OpenRouter anyway.

**Fix path:** documentation fix — remove or qualify the cache claim. No code change. Latency budget assumptions don't need to change.

**Maps to:** `philosophy.md` (around line 133, the "cached by the provider" line).

**Plan stub:** edit philosophy.md to drop the cache claim, or qualify it as "may cache on providers with a ≥1k-token min; doesn't apply at our prompt size".

---

## How to add the next batch

1. Append findings to the matching section header (e.g. `### C1 …`).
2. Use the same shape: **Primary info / Maps to / Plan stub**.
3. Tag with ✅ / ⚠️ / ❌.
4. Add a one-liner to the **Status log** at the top.
5. If the batch introduces a systemic weakspot not in A–I, add a new `W#` entry.

When a plan stub becomes a real cycle, replace the stub with the resolving commit hash and mark the entry ✅ DONE in the heading.

# Handoff G — Tracer-bullet "machine gun" stress harness

**Date:** 2026-05-26
**Reference:** `irene/handoffs/2026-05-26-A-iran-funnel-audit.md`; `2026-05-26-B-dedupe-freshness.md` (handoff G is most valuable AFTER B).
**Dependency:** none structurally; defer scheduling until after B.

## The intent (from the user)

> "add a series of ways to fix and test and use the tracer bullet system so we can fire loads of things about iran war, peace deal, etc (fake or real articles, either ok) as like a machine gun payload, blast them into the system to see how it responds. however, they are tagged so we can remove them once the test is done and the system can keep chugging along, basically every stage of the pipeline knows they arent real so doesnt write anything about them, but treats them like theyre real and passes them on if they meet reqs."

## What exists today

The P1 tracer-bullet system (per master plan):
- `scripts/tracer/fire.py` — embedded pipeline harness with **mocked externals** (no real scraper, no Polymarket Gamma, no OpenRouter). Used as a CI ship gate: `--count 1000 --concurrency 50` must drop zero non-telemetry bullets.
- `scripts/tracer/check.py` — verifies the post-burst DB shape.
- `scripts/tracer/templates/{economic_indicator, merger, regulatory_decision}.yaml` — 3 existing tracer scenarios.
- **Every owned-table writer stamps `is_tracer=1` + `tracer_id=<unique>`** on tracer-derived rows. The flag flows article → cluster → signal → decision → prediction → trade.
- **Hard rule #4 from master-plan.md**: "Tracers and live trades cannot mix. `is_tracer=1 AND paper=0` is a hard write rejection."

So the safety guarantee already exists. This handoff is purely additive: more templates, a live-externals mode, post-burst audit + cleanup.

## Scope

### Step 1 — new tracer templates (cast a WIDE net)

File location: `scripts/tracer/templates/<name>.yaml`. Mirror the shape of `regulatory_decision.yaml` (load it to discover the schema — variables, headline patterns, body patterns).

**User directive:** "expand bullet test cases and headline types (could legit say the market is crashing and stuff, anything crazy you want)." Don't be conservative — synthesize a wide variety of news regimes so we discover where the pipeline breaks for each shape of event.

New templates (10+ files; one per scenario class):

1. **`iran_strike.yaml`** — "U.S. strikes Iranian missile site near {{target}}", "Israel hits Hezbollah convoy in {{location}}; {{n}} reported dead", "Iran launches {{count}} missiles toward {{israeli_city}}", "U.S. Navy intercepts Iranian boat laying mines in {{strait}}". Tagged for `Will the U.S. invade Iran before 2027?`, `Iran nuclear test before 2027?`, `Will Israel strike 4/5 countries in 2026?`.

2. **`iran_peace_deal.yaml`** — "U.S. and Iran reach nuclear framework agreement", "Rubio: breakthrough on uranium stockpile", "Tehran accepts {{percent}}% reduction in enriched uranium", "Ceasefire holds for {{days}} days". Tagged for the US-Iran deal markets.

3. **`hezbollah_disarm.yaml`** — "Hezbollah disarms {{percent}}% of medium-range arsenal under Saudi-brokered deal", "Lebanese parliament passes Hezbollah weapons-handover bill {{vote}}".

4. **`market_crash.yaml`** — "S&P 500 plunges {{percent}}% on {{trigger}}", "Bitcoin crashes below ${{price}}k after {{event}}", "Nasdaq circuit breaker triggered at -{{percent}}%", "Treasury yields spike to {{rate}}% on {{cause}}", "Gold hits all-time high ${{price}}/oz on safe-haven flight". Targets any finance/crypto/equity market.

5. **`central_bank_surprise.yaml`** — "Fed hikes by {{bps}} bps in emergency meeting", "ECB cuts rates {{bps}} bps, citing {{reason}}", "BoJ ends yield-curve control after {{years}} years", "Powell signals {{n}} more hikes in 2026". Targets Fed-decision markets, recession/no-recession markets.

6. **`election_upset.yaml`** — "{{candidate}} concedes {{state}} primary to {{winner}}", "{{party}} wins {{n}}-seat majority in surprise upset", "Supreme Court rules {{precedent}} unconstitutional {{vote}}". Targets election-outcome markets.

7. **`celebrity_event.yaml`** — "{{celeb}} dies at {{age}} after long battle with {{cause}}", "{{celeb}} arrested in {{location}} on {{charge}} charges", "{{celeb}} announces retirement effective {{date}}". Targets celebrity/death/scandal markets.

8. **`tech_breakthrough.yaml`** — "OpenAI releases GPT-{{version}} with {{trait}} capability", "Apple announces {{product}} launch for {{quarter}}", "SpaceX {{stage}} explodes during {{phase}} test", "Anthropic launches Claude {{version}} matching {{competitor}}". Targets AI-progress / company-launch markets.

9. **`sports_upset.yaml`** — "{{underdog}} beats {{favorite}} {{score}} in {{tournament}} stunner", "{{star}} ruled out for season with {{injury}}", "{{team}} fires manager {{name}} after {{streak}}". Targets sports-outcome markets.

10. **`natural_disaster.yaml`** — "Magnitude {{mag}} earthquake strikes {{location}}; {{n}} dead", "Hurricane {{name}} makes landfall in {{state}} as Cat {{cat}}", "{{count}} dead in {{city}} chemical tank rupture". Targets disaster-occurrence markets if any exist.

11. **`crypto_event.yaml`** — "{{exchange}} halts withdrawals; {{coin}} crashes {{percent}}%", "SEC approves {{coin}} ETF", "{{stablecoin}} de-pegs to ${{rate}}". Targets crypto-specific markets.

12. **`absurd_events.yaml`** — deliberately strange shapes: "Bigfoot sighted in Yellowstone, NPS confirms", "Cure for cancer announced by {{lab}}", "Aliens land at White House". Tests how the matcher handles novel-but-bizarre input that *shouldn't* match anything. Acceptance: most should reject at similarity_floor or direction_neutral.

Variable lists in each template should produce ≥50 unique articles per fire. Use a YAML anchor pattern for shared variables (countries, dates, percentages, etc.).

### Step 2 — `fire.py --live-externals` mode

Today `fire.py` runs a fully-embedded pipeline with mocked externals. Add a flag that boots the pipeline against the LIVE side:

```bash
python3 -m scripts.tracer.fire \
    --template iran_strike \
    --count 100 \
    --concurrency 10 \
    --live-externals \
    --tracer-id-prefix iran-burst-2026-05-26T10:00 \
    --budget-cap-usd 5
```

`--live-externals` behavior:
- Read the **live DB** at `/data/irene.db` (mounted into the tracer process).
- Use the **real** `Embedder` (sentence-transformers), real `CLOBClient`, real `AIClient` (OpenRouter — burns real budget).
- Load real config from `/config/config.yaml`.
- Tracer-tagged articles flow through the embedded pipeline's bus → real dedupe (joins clusters in the live DB but inserts tracer-flagged rows) → real matcher (against live MarketIndex) → real LLM call → real direction gate → trade gate → BLOCKED at insert by rule #4.

`--budget-cap-usd N` (safety): before starting, snapshot OpenRouter budget; abort the burst if it would exceed N USD. Defaults to a small number (e.g. 5).

**Important constraint:** the bus is process-local. The tracer's embedded pipeline runs in the tracer process, NOT in the live `irene` container's process. So the live bot doesn't see the tracers via the bus — they only show up in the live DB after the tracer's stages write them. That's fine for the purpose: we're testing the STAGES (dedupe, matcher, LLM, direction gate), not the live bot's bus loop. The live bot continues to ingest real news from the scraper concurrently.

For cleanliness: the tracer process should use the **same models** (cluster index, market index) the live bot uses, by loading from the same DB rows + chroma. Verify this; in particular, the dedupe stage's chroma index must be the same instance (or a read-only replica) so tracers cluster against the same cluster space the live bot sees.

### Step 3 — burst-audit CLI

File: `scripts/tracer/audit_burst.py` (new). Argparse with `--tracer-id-prefix` (required).

Funnel report:

```
== Tracer burst audit — prefix: iran-burst-2026-05-26T10:00 ==
Window: <first fire> → <last fire> (Nm Xs)

Articles fired:                  N
  → Signals matched:              N₂ (k% match rate)
  → Decisions made:               N₃
       — reject  freshness_stale: K₁ (K₁% of decisions)
       — reject  direction_neutral: K₂
       — reject  direction_lowconf: K₃
       — reject  mid_unavailable: K₄
       — reject  market_*: K₅
       — execute (paper, tracer): K₆
  → LLM-reached:                  N₄ (K% of signals)
  → LLM verdict distribution:
       Y: K₇ avg_conf=0.XX
       N: K₈ avg_conf=0.XX
       NEUTRAL: K₉ avg_conf=0.XX
```

Per-pair CSV at `data/dashboard/plots/tracer_burst_<prefix>.csv`: `tracer_id, article_id, article_title, market_id, market_question, similarity, freshness, llm_label, llm_direction, llm_confidence, decision_outcome, reject_reason, gate_history_snippet`.

### Step 4 — cleanup CLI

File: `scripts/tracer/cleanup.py` (new). Argparse with `--tracer-id-prefix` (REQUIRED, no default — safety).

Deletes from every `is_tracer`-aware table where `is_tracer=1 AND tracer_id LIKE <prefix>%`:

```
articles, article_clusters, signals, predictions, decisions, trades,
trade_features, trade_closures, decision_inputs, llm_calls, price_impacts,
entity_resolutions, entity_extractions, facts, events, event_edges,
market_links
```

(Confirm the exact list by grepping `is_tracer` across `app/` — see the existing `scripts/tracer/_queries.py` for the canonical set.)

`--dry-run` flag: prints would-be delete counts per table without executing. Use for confidence before destructive runs.

Output: per-table delete counts + grand total. Idempotent (re-run = 0/0/0).

### Step 4.5 — wallet-exercise mode (NEW — per user directive "use the bullets to test the wallet if possible")

The existing tracer system tags rows with `is_tracer=1`, and rule #4 says `is_tracer=1 AND paper=0` is a hard reject — meaning tracer trades CAN reach `trade.execute` as long as they're paper. So tracer-flagged trades DO exercise the paper wallet's cash/equity math, just with isolated rows that cleanup wipes.

**Wallet-lifecycle template scenarios:** craft tracer payloads designed to PASS every gate (high similarity, BREAKING freshness via burst detection, decisive LLM verdict, fresh market, sufficient cash, normal spread, edge > min_edge). These produce executed tracer trades. Then the existing closer (`app/stages/closer.py`) processes them on its normal poll cadence: take_profit / stop_loss / stagnation / timeout. When closed, `trade_closures` rows materialise with `is_tracer=1` and the wallet math runs end-to-end.

Add a `--exercise-wallet` flag to `fire.py`:
- Forces high-quality payload variants (uses templates' "high_quality_overrides" section: similarity≥0.85, body contains decisive language, market is fresh + liquid).
- Pre-flight: snapshot live wallet equity, open count, realized.
- Post-burst wait (e.g. 60s) for the embedded pipeline to process.
- Post-flight: query tracer-only wallet view:
  ```sql
  SELECT
    COUNT(*) FILTER (WHERE is_tracer=1) AS tracer_trades,
    SUM(size_usdc) FILTER (WHERE is_tracer=1 AND ...) AS tracer_invested,
    SUM(c.realized_pnl_usdc) FILTER (WHERE c.is_tracer=1) AS tracer_realized,
    SUM(c.entry_fee_usdc + c.exit_fee_usdc) FILTER (WHERE c.is_tracer=1) AS tracer_fees
  FROM trades t LEFT JOIN trade_closures c ON c.trade_id = t.id
  WHERE t.tracer_id LIKE <prefix>%
  ```
- Print: tracer trades fired, tracer trades closed (and how — take_profit/stop_loss/etc.), tracer realized PnL, tracer fees burned, live wallet delta (should be ZERO — verifies isolation).

Acceptance: live wallet equity unchanged before/after the burst (proves rule #4 isolation works). Tracer wallet shows realistic trade lifecycle (executes → closes → realized PnL ± expected magnitude per direction × size × mid-movement).

**Important:** this is the only handoff where `fire.py --exercise-wallet` should be allowed to use `--budget-cap-usd` higher than ~$5 (because each high-quality bullet costs a real LLM call). Defaults still conservative; user opts in.

### Step 5 — live-pipeline safety pre-flight

Inside `fire.py --live-externals`, before the first article publish:
- `SELECT COUNT(*) FROM articles WHERE is_tracer=1 AND tracer_id NOT LIKE '<our prefix>%' AND ingested_at > <now-1h>` — if > 0, WARN that another tracer burst is concurrent, ask for `--force` to proceed.
- Snapshot budget. Re-check periodically. Abort if cap hit.
- Tag every published payload with `tracer_id = <prefix>-<seq>` so cleanup is precise.

### Step 6 — tests

File: `app/tests/scripts/test_tracer_burst.py` (new):
- Load each new template; assert it parses + generates N distinct articles.
- `--live-externals=mock` (a test mode that mocks just the LLM call to avoid budget burn) — fire 5 articles, assert 5 rows land in `articles` with `is_tracer=1`, assert ≥0 signals + ≥0 decisions.
- Cleanup test: fire 5 articles, run `cleanup.py --tracer-id-prefix test-`, assert all 5 articles + downstream rows gone, assert non-tracer rows untouched.

Existing tracer CI (`scripts/tracer/fire.py --count 1000 --concurrency 50`) must still pass — no regression on the embedded-mock path.

## Files

| Change | File |
|---|---|
| Templates (12 new) | `scripts/tracer/templates/{iran_strike, iran_peace_deal, hezbollah_disarm, market_crash, central_bank_surprise, election_upset, celebrity_event, tech_breakthrough, sports_upset, natural_disaster, crypto_event, absurd_events}.yaml` |
| Live mode + wallet exercise | `scripts/tracer/fire.py` — add `--live-externals`, `--budget-cap-usd`, `--exercise-wallet` flags |
| Burst audit | `scripts/tracer/audit_burst.py` (new) |
| Cleanup | `scripts/tracer/cleanup.py` (new) |
| Safety pre-flight | inside `scripts/tracer/fire.py` |
| Tests | `app/tests/scripts/test_tracer_burst.py` (new) |

## Verification

1. **Template parse:** `python3 -c "from scripts.tracer.templates import load; load('iran_strike')"` succeeds; calling it 100 times generates 100 distinct articles.
2. **Existing CI gate unchanged:** `pytest app/tests/scripts/` + `scripts/tracer/fire.py --count 1000 --concurrency 50` both pass.
3. **Synthetic verification of handoff B:**
   - Pre-B baseline: `scripts/tracer/fire.py --template iran_strike --count 100 --live-externals --tracer-id-prefix iran-burst-preB --budget-cap-usd 2`. Expected: most articles labeled STALE_RETURN (matches live bot behavior).
   - `scripts/tracer/audit_burst.py --tracer-id-prefix iran-burst-preB` — produces the funnel report.
   - `scripts/tracer/cleanup.py --tracer-id-prefix iran-burst-preB` — verify zero rows remain.
   - Post-B (after handoff B lands): repeat the burst. Expect ≥70 articles labeled BREAKING/EVOLVING.
4. **Cleanup:** `scripts/tracer/cleanup.py --dry-run --tracer-id-prefix iran-burst-` lists candidates; `--no-dry-run` deletes; assert live wallet equity unchanged before/after.

## Coordinate with…

- **Handoff B** — G is how we verify B in minutes rather than 24h. Burst → confirm freshness label distribution → confirm matcher routes correctly → confirm LLM exposure increases.
- **Handoff F** — once F's panel exists, G bursts populate the panel with synthetic near-misses for inspection.
- **Handoff C** — burst a `hezbollah_disarm` template against the still-open trade-40 market (post-D cleanup) to verify the DKG safety gates now reject closed-market trades.

## Subagent prompt suggestion

> Read `irene/handoffs/2026-05-26-G-tracer-stress-harness.md` end-to-end. Phase 1: write the 4 new templates. Phase 2: extend `fire.py` with `--live-externals` + `--budget-cap-usd` flags + safety pre-flight. Phase 3: write `audit_burst.py` and `cleanup.py`. Phase 4: write tests. Run pytest + the existing CI tracer gate. Verify with a dry-run burst against the live DB (use `--budget-cap-usd 1` to limit). Do NOT touch any other handoff scope.

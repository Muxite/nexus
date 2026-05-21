# Phase E.9 — overnight monitor report (H1 → H8)

**Window**: 2026-05-19T10:13 UTC → 2026-05-19T18:33 UTC (8h post-restart)
**Live commits at start**: `86825bc` (E.9 docs + backtest), `7fad922` (E.9b cluster_factor fix), `2f445c7` (E.9 `min_edge` 0.03 → 0.015)
**Container**: `jennie-seulgi`, restarted 10:13 UTC, Up 8h continuously
**Bankroll**: $800 baseline; drift **+$0.11 throughout**; 0 open positions

---

## TL;DR

* **E.9 mechanics validated** end-to-end at H5 — a `conf=0.60` signal cleared the new `min_edge=0.015` floor, generated `p_hat=0.825`, and reached the post-AI gate stack. It was blocked downstream by `RULE_breaking_no_anti_edge_SKIP` because the headline was an NHL sports market — the sports skip rule didn't catch it upstream. The E.9 change works; we just need the same conviction on a non-sports breaking headline.
* **Zero failures**: 8h continuous container uptime, zero `Traceback`/`ERROR`/`CRITICAL` lines, zero realized losses, drift unchanged at +$0.11. The E.9b `cluster_factor` fix held — no NameError despite admitting E.9-band signals.
* **The binding constraint is no longer min_edge — it's the LLM-emitted `p_alpha`.** Over 8 hours and ~88 non-NEUTRAL rows, only 8 (all from a single H5 sports headline) reached the E.9 admit band of `conf ≥ 0.5625`. The trader-stage gates are fine; the LLM is what's not producing conviction.

---

## Hourly table

| Hour | Time UTC | Container | non-NEUTRAL/1h | max(conf) | E.9-band (conf ≥ 0.5625) | drift | Notable |
|------|----------|-----------|---------------:|----------:|--------------------------:|------:|---------|
| H1 | 11:20 | Up 1h | 16 | 0.43 | 0 | +$0.11 | LLM signals 0.21–0.43; binding gate shifted from `edge_too_small` to `low_confidence` |
| H2 | 12:22 | Up 2h | 16 | 0.40 | 0 | +$0.11 | conf distribution shifted lower vs H1 |
| H3 | 13:24 | Up 3h | 8 | 0.47 | 0 | +$0.11 | Pre-AI funnel volume doubled (26k vs 12k) — news cycle warming |
| H4 | 14:25 | Up 4h | **0** | n/a | 0 | +$0.11 | **Hold trigger**: no signals reached AI. Pre-AI rules killed 7.5k matches (extreme_low_price 4144 + mid_below_065 3352). |
| H5 | 15:27 | Up 5h | 8 | **0.60** | **8** | +$0.11 | **First E.9-band hit.** Conf=0.60 NO signal at mid_yes=0.705, NHL Hurricanes question, killed by `RULE_breaking_no_anti_edge_SKIP` |
| H6 | 16:29 | Up 6h | 16 | 0.50 | 0 | +$0.11 | H5 confirmed as single-headline outlier; regime back to conf ≤ 0.50 |
| H7 | 17:31 | Up 7h | 16 | 0.23 | 0 | +$0.11 | Weakest hour — all 16 rows in <0.30 bucket |
| H8 | 18:33 | Up 8h | 8 | 0.43 | 0 | +$0.11 | Stabilised back to conf 0.40–0.45 band; cumulative: 88 non-NEUTRAL, 8 E.9-band (9.1%), 0 executed |

## Findings

### (a) E.9 mechanics validated

The H5 batch is the dispositive evidence:

```
2026-05-19T14:31:39  conf=0.60  mid_yes=0.705  edge=0.12*(2*0.6-1)=0.024  → passes new min_edge=0.015
                                                                          → would FAIL old min_edge=0.03
                     direction=NO  freshness=BREAKING  question=NHL/Hurricanes-elim
                     skip_reason=RULE_breaking_no_anti_edge_SKIP
```

This signal would not have reached `_evaluate_market`'s sizing block under the old min_edge — Phase E.9 admitted it. The post-AI gate stack stopped it for sport-specific (not E.9-related) reasons. The `cluster_factor` plumbing fix from E.9b also held — no NameError despite this being the first signal to clear the edge gate post-restart.

### (b) Zero crashes, zero losses

8 hours, ~80,000 decisions processed:

* No `Traceback` / `ERROR` / `CRITICAL` log lines (filtered for known-noise patterns: telemetry, "terror" string-in-headline, empty relevance errors).
* `realized_pnl` since cycle 2 start: $0.11 (from one cycle-reset rounding artefact).
* `open positions`: 0 throughout.
* Container uptime monotonic — never restarted.

### (c) The new binding constraint is upstream of any tunable gate

Of 88 non-NEUTRAL rows over 8 hours:

| conf range | count | what happens |
|---|---:|---|
| < 0.30 | ~32 | NEUTRAL or `low_confidence` skip |
| 0.30–0.45 | ~40 | `low_confidence` skip (most variants min_conf=0.40, two at 0.45) |
| 0.45–0.50 | 8 | passes min_conf, fails `edge_too_small` even at E.9's 0.015 floor (edge=0) |
| 0.50–0.5625 | 8 | passes min_conf, still fails edge_too_small (edge < 0.015) |
| 0.5625–0.625 (E.9 band) | **8** | **passes E.9 edge gate** — killed by `RULE_breaking_no_anti_edge_SKIP` (sports) |
| ≥ 0.625 | 0 | — |

The LLM is emitting `p_alpha` in a noticeably lower regime than the pre-drought 24h (where 89% of executed trades had `p_alpha ∈ [0.625, 0.75)`). Whatever happened in the late evening — E.8.1 prompt softening, model-side regime shift, or news-cycle quiet — is still in effect 17h later. **Tuning min_edge/min_confidence/anti-informative further can't recover from an LLM that won't emit ≥0.625.**

---

## Recommended next levers (ordered by ROI)

1. **Prompt iteration (E.8 continuation)** — highest ROI. The drought is fundamentally an LLM-confidence problem, not a gate problem. Specific moves to try:
   * Add a calibration block to the prompt: "Most fresh news produces a 5-15% probability shift. Default `p_alpha` to 0.55-0.70 when you have any directional view; reserve 0.4-0.5 only for genuine ambiguity."
   * A/B against current ai.py prompt by running both for 2h with engine-level routing (one variant per model).
   * Check whether OpenRouter's `gemini-2.5-flash` switched silently to a quieter version — recall E.8c bumped max_tokens because of JSON truncation. Possible the underlying model degraded.

2. **Drop `RULE_high_confidence_anti_informative_SKIP` for `inverted_highconf` only** — direct test of the inversion thesis. Currently the rule short-circuits both branches; relaxing it just for the inverted variants gives a clean A/B against the balanced variants. Per the rule's own doc the post-mortem evidence is now 6 days stale.

3. **Lower `min_confidence` 0.40 → 0.35** — admits the conf 0.35–0.40 band. Carries risk (weak signals) but the variance-control machinery (E.5b rolling hit-rate pause, E.8 cluster sizing) is built for exactly this.

4. **Clear or shorten ChromaDB freshness store** — eliminates the `REHASH`/`STALE_RETURN` skip pressure (saw 8500+ freshness skips per hour). Lower priority because the conf ceiling will still bind, but it'd at least raise the AI-reached count so we can see whether higher-conviction signals exist in the freshness-skipped pool.

**Hold**: don't change `min_edge` further. The 0.015 floor is doing its job (proven at H5); going lower admits noise without raising the conf ceiling.

---

## Critical files for the next session

* `seulgi/REPORT_PHASE_E9_2026-05-19.md` — original E.9 diagnosis writeup
* `seulgi/DECISION_FUNNEL.md` — every gate in order, evidence per threshold, drought-diagnosis playbook
* `seulgi/app/ai.py` — prompt template at line 84; `_parse_one` (p_alpha contract) at line 560
* `seulgi/app/trader.py` — edge model at line 798; `_evaluate_market` (cluster_factor now plumbed) at line 588
* `seulgi/rules/skip_high_confidence_anti_informative.md` — anti-informative rule + its 6-day-stale evidence
* `seulgi/rules/skip_breaking_no.md` — Phase E.1 rule that blocked the H5 hit
* `seulgi/scripts/analysis/backtest_min_edge.py` — replay tool for any future edge-formula tweak
* `seulgi/app/tests/test_ai.py::TestProductionConfidenceContract` — pins the `confidence = p_alpha` contract
* `seulgi/config.yaml:540-640` — engines block (8 active variants)
* `seulgi/snapshots/e9_h*` — H1-H8 dashboard snapshots for forensic replay

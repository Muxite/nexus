# Seulgi Decision Funnel

Every headline goes through this gate sequence before a trade can fire.
Each gate is listed with where it lives in code, what it's measuring,
what the current threshold is, and the **historical evidence** that set
the threshold. When the system stops trading (see Phase E.9 drought,
2026-05-19), the answer is almost always one of these gates over-firing.

Maintained: 2026-05-19. Update this doc when you change any threshold.

---

## Order of operations

```
                ┌────────────────────────┐
   headline ──► │   pre-AI stage         │ — runs once per headline
                │                        │
                │ 1. freshness label     │   ai.py / news_memory.py
                │ 2. low_similarity      │   markets.py (sim ≥ 0.55)
                │ 3. pre-AI rule gate    │   rules.py + seulgi/rules/skip_*.md
                │                        │
                │   ↓ survivors          │
                │                        │
                │ 4. relevance LLM       │   relevance.py (Gemini lite)
                │                        │
                │   ↓ survivors          │
                │                        │
                │ 5. classify_batch LLM  │   ai.py (Gemini main)
                └────────────────────────┘
                            │
                            ▼
                ┌────────────────────────┐
                │   per-engine evaluate  │ — runs N times (one per engine variant)
                │                        │
                │ 6. min_confidence      │   trader.py:_evaluate_market
                │ 7. min_agreement       │
                │ 8. min_entry_yes_mid   │
                │ 9. hard_rail           │
                │10. rail_skip           │
                │11. edge_too_small      │
                │12. RULE post-AI block  │
                │13. max_open / max_uniq │
                │14. duplicate position  │
                │15. kelly sizing        │
                │16. cluster_factor      │
                │17. min_trade_usdc      │
                │18. slippage/fee/round  │
                └────────────────────────┘
                            │
                            ▼
                       execute trade
```

---

## Gate reference

### Pre-AI (one pass per headline)

| # | Gate | Where | Current setting | Why |
|---|---|---|---|---|
| 1 | Freshness label | `news_memory.py` → injected into prompt; also drives the `freshness:*` skip family | BREAKING / EVOLVING / REHASH / STALE_RETURN buckets in ChromaDB by 24h-age + cosine-distance | Cycle 9 — binary republish hint was lossy, labelled freshness reduced false-skip on EVOLVING coverage |
| 2 | Low-similarity bypass | `markets.py` — emits `low_similarity:<score>` skips | `similarity ≥ 0.55` required (Phase E.2 floor) | E.2 evidence: <0.55 was 99% no_signal in cycle 9 |
| 3 | Pre-AI rules | `rules.py:gate_pre_ai` + markdown rules in `seulgi/rules/skip_*.md` | E.1 (BREAKING+NO), E.3 (mid ∈ [0.65, 0.85]), E.4 (_lowvol cut), E.5 (Iran/US election), `extreme_low_price`, `extreme_high_price`, `sports`, `weather` | Each rule's evidence is in its own `seulgi/rules/skip_*.md` frontmatter — read before changing |
| 4 | Relevance LLM | `relevance.py` (gemini-2.5-flash-lite) | Returns STRONG/WEAK/NOT_RELEVANT; current code accepts STRONG only | Cycle 9 — Stage-2 cut cost ~70% of headlines but raised IC. **Suspected over-restrictive in 2026-05-19 drought.** |
| 5 | classify_batch | `ai.py` (gemini-2.5-flash) | Returns `{p_yes, p_no, p_neutral, p_alpha, risk}` per market | `p_alpha` becomes trader-facing `confidence` (ai.py:602). Critical contract — see `tests/test_ai.py::TestProductionConfidenceContract` |

### Per-engine (fans out from one classify_batch to N variants)

| # | Gate | Where | Current setting | Why |
|---|---|---|---|---|
| 6 | `min_confidence` | `trader.py` checks `result.confidence` (= p_alpha) against `ai_cfg.min_confidence` | `0.40` (most variants) / `0.45` (conf45_* mutants) | Below this is treated as NEUTRAL. Cycle 7 contract. Drought-window evidence: 36/96 rows fail this gate. |
| 7 | `min_agreement` | Ensemble vote agreement | `0.66` (default `getattr(...,0.66)`) | Cycle 9 ensemble — disagree-among-samples = drop |
| 8 | `min_entry_yes_mid` | `trader.py:685` | `0.0` (disabled) on current variants | Cycle 10 idea: only enter direction=NO when YES mid is rich. Not currently used; left as opt-in. |
| 9 | `hard_rail` | `entry_rail_skip_threshold` — bilateral | `0.0` (disabled in current config) | Refuse trades inside [0, t] ∪ [1-t, 1] — spread alone exceeds tight SL |
| 10 | `rail_skip` | Softer rail | `0.0` (disabled in current config) | Saturated markets — was 0.95 originally. Aggressive opted out before retirement. |
| 11 | **`edge_too_small`** | **`trader.py:812`** | **`min_edge = 0.015` (E.9), down from 0.03** | **Binding bottleneck of the 2026-05-19 drought.** With `edge_increment=0.12`, the formula `edge = edge_increment × (2·conf − 1)` requires `conf ≥ 0.5625`. See `REPORT_PHASE_E9_2026-05-19.md`. |
| 12 | `RULE_high_confidence_anti_informative_SKIP` | `rules.py:gate_post_ai` + `seulgi/rules/skip_high_confidence_anti_informative.md` | Fires when `confidence ≥ 0.75` | 2026-05-14 bucket evidence: 100% no_signal in 0.75–1.0 buckets. **Evidence is 5 days stale as of 2026-05-19.** Blocks ~3 rows per variant per 3h during drought. |
| 13 | `max_open_positions` / `max_unique_markets` | `trader.py` | `max_open=20`, `max_unique=15` | Variance + concentration limits |
| 14 | Duplicate position | `db.has_open_position` | 1 position per (variant, market) | Avoid stacking on the same idea |
| 15 | Kelly sizing | `simulate.fractional_kelly` | Sized off `p_hat` vs `p_market`, capped at `max_position_usdc` | Plan §7.4. Returns `skip_reason='kelly_below_min'` if fractional Kelly under floor. |
| 16 | `cluster_factor` | Passed from `engine.py:660` into `trader.evaluate()` | `1.0/√N` where N = survivors-for-headline | E8 sizing — bound capital exposure per headline by √N rather than N. **Was the source of a latent NameError fixed in E.9b.** |
| 17 | `min_trade_usdc` | After all sizing transforms | `2.0` default | Don't fire on dust |
| 18 | Slippage / fee / quantization | `simulate.py:27-122` | `PRICE_MIN/MAX` clamp; fee model reconciled in E.7.2 | Plan §7.1-7.3 — paper realism so live cutover doesn't surprise us |

---

## Phase-E gates by reference

| Phase | What it added | Doc |
|---|---|---|
| E.1 | BREAKING+NO entry skip | `seulgi/rules/skip_breaking_no.md` |
| E.2 | Similarity floor 0.55, low_similarity bypass | inline in `markets.py` |
| E.3 | Mid-band requirement [0.65, 0.85] | `skip_mid_below_065.md`, `skip_mid_above_085.md` |
| E.4 | `_lowvol` cut | retired variants |
| E.5 | Iran + US election topic skips | `skip_iran_topic.md`, `skip_us_election_topic.md` |
| E.5a | TIMEOUT_BLEED short-hold exit | `trader.py` close-reason `timeout_bleed` |
| E.5b | Per-variant rolling hit-rate pause | `engine.py:_engine_should_pause` |
| E.7.1 | Dashboard active-variant leaderboard | `dashboard.py:689-720` |
| E.7.2 | Polymarket fee/gas reconciliation | `simulate.py` |
| E.7.3 | Evolutionary engine expansion (cycle 2 reset) | `config.yaml` engines block |
| E.8 | Cluster-size sizing | `engine.py:660`, `trader.py` |
| E.8.1 | LLM NEUTRAL-bias softening | `ai.py:_BATCH_PROMPT_TEMPLATE` |
| E.8b | (reverted) p_yes ≠ mid sharpening | reverted in 5991ca0 |
| E.8c | max_tokens 160·N+100 | `ai.py:384` |
| E.9 | **min_edge 0.03 → 0.015** | this doc + `REPORT_PHASE_E9_2026-05-19.md` |
| E.9b | **cluster_factor NameError fix** | this doc + `trader.py:_evaluate_market` |
| E.10 | TRAIL guard (don't trail below entry) | `trader.py:~1241` |
| E.11 | Hour 23/00/01 UTC pause | `engine.py:203-207` |

---

## Diagnosing a drought

When non-NEUTRAL count drops or executions stop, walk the funnel in
order:

1. Check `seulgi/data/dashboard/dashboard.txt` → DECISION FUNNEL (last 1h)
2. If freshness is high (>50%) and net AI-reached is tiny → **freshness gate** is over-firing. Consider clearing or shortening the ChromaDB store.
3. If pre-AI rule drops dominate → walk `seulgi/rules/*.md` against current market mid distribution. Likely candidate: E.3 mid-band.
4. If signals reach AI but argmax=NEUTRAL → **prompt issue**. See Phase E.8 series.
5. If non-NEUTRAL but all `edge_too_small` → **min_edge gate** (this drought). Backtest with `scripts/analysis/backtest_min_edge.py`.
6. If non-NEUTRAL but all `RULE_high_confidence_anti_informative_SKIP` → **post-AI anti-informative rule**. Evidence may be stale.
7. If admitted but `kelly_below_min` → bankroll too small for current edge magnitudes.

The dashboard funnel + the `decided_at`-filtered SQL queries in
`seulgi/QUERIES.md` are the right tools — do not eyeball logs.

# Phase 5 F1 retry — interim status

**Date:** 2026-05-23 ~23:58 UTC start (interim writeup at T+~5 min)
**Plan:** `/home/muk/.claude/plans/plan-continuation-of-seulgi-steady-cat.md`
**Goal:** ≥50 executed trades in 4h, 0 crashes/commit-races, telemetry queue drained.

This report covers the entire plan execution (Phases A–E) up to bot
restart. The 4-hour F1 outcome (pass/fail) is appended after the bot
runs to T+4h.

---

## What shipped (Phases A–E)

| Phase | Commit | Outcome |
|:----:|---|---|
| A — Hardening | `ac9b55a` | docker-compose dual-container retired; replay harness + suite hardened. Regression suite (smoke / load / stress, N=20 each) exits 0 from repo root. |
| B — `decision_inputs` writer | `3dd42c6` | Discovered the writer was already wired at `engine.py:447-491` (commit `1b606c3` FIX-3); test xfail in `test_capture.py` flipped to an unconditional `>= 1` assert. 3/3 tests pass. Live writes verified: 949 rows at restart, then +N per signal. |
| C — Matchers | `6e43a5f` | `cross_encoder.enabled` + `learned_mlp.enabled` flipped to true; commented-out `knowledge_graph` slot reserved for irene. 12/12 matchers tests pass. **Live verified at boot**: `CrossEncoderReranker: loaded cross-encoder/ms-marco-MiniLM-L-6-v2`; `LearnedMLPMatcher: only 0 closed-trade samples (<30) — refusing to fit; score() will return neutral 0.5` (expected — `decision_inputs` only started populating 2026-05-23 06:00 and no closed trades joined yet). |
| D — Variant sweep | _skipped_ | The backtest replays trader decisions against `engine_results`' frozen `(headline_id → condition_id)` pairings. Matcher choice only affects which candidate `find_matches` would pick at *live* time; it doesn't enter the trader replay path (`extra_score` is a re-ranking signal in `markets.py:642`, not a trader input). Per user direction, Phase D was skipped and matcher A/B moved to Phase E (forward live A/B). |
| E — Live restart | (in progress) | Bot up via `lifecycle.sh start` at 2026-05-23T23:58 UTC. 20 engines built (incl. 3 new `loose_*` balanced variants). All three matchers active (cosine baseline + cross-encoder + learned MLP). |

---

## Boot configuration

- **Matchers chain**:
  - `cosine` — always-on SBERT (`sentence-transformers/all-MiniLM-L6-v2`, 384-dim, cached in `seulgi_model_cache`).
  - `cross_encoder` — `cross-encoder/ms-marco-MiniLM-L-6-v2`, fires only on cosine `∈ [0.30, 0.55]`. Loaded successfully at boot (50 MB download).
  - `learned_mlp` — pure-numpy 768→32→16→1. **Inactive (neutral 0.5)** until ≥30 closed trades with paired `decision_inputs` rows accumulate. Refit cadence: 7d.
- **Engines built (20)**:
  - balanced/*: 13 variants (incl. new `loose_confidence_035`, `loose_mid_band_058`, `loose_rail_floor_001` at config.yaml:726-746).
  - inverted_highconf/*: 4 variants.
  - loose_relevance_yes/default.
  - scout/all_signals, super_scout/all_signals.
- **Phased rollout (A4B.4)**: production engines override `use_true_ev_formula: false`; scout/super_scout inherit `use_true_ev_formula: true` for diagnostic data.
- **Calibration**: `Calibrator(db, method="isotonic")` per `main.py:392` (4h refresh cadence).
- **Counterfactual sampler**: enabled at `rate=0.05, cap=3, filters=[duplicate_position, freshness_filter, mid_band, min_confidence, min_edge]`.
- **Snapshot pre-restart**: `seulgi/snapshots/pre_stop_20260523T063231Z` (8.7 GB; 564 trades / 45 open positions baseline).

---

## F1 exit gates (re-stated from plan)

- **PASS** — ≥50 executed trades by T+4h, 0 crashes / commit-races / DB row-ID gaps, calibrated YES/NO Brier still negative, telemetry queue drained.
- **FAIL** — <50 trades after 4h. Escalation lever (deferred from this plan): pre-AI rule relaxation — `mid_below_065` + `extreme_low_price` killed 85% of decisions in the 2026-05-23 pre-stop smoke; A0.10 audit flagged the evidence base as a 190-row cycle-9 backtest (stale).

---

## Monitoring commands (run these to assess F1 outcome at T+4h)

```bash
# Trades count + per-engine breakdown over the F1 window
python3 - << 'PY'
import sqlite3
con = sqlite3.connect('file:/home/muk/projects/jennie/seulgi/data/seulgi.db?mode=ro', uri=True)
con.row_factory = sqlite3.Row
window_start = '2026-05-23T23:58'
n = con.execute(
    "SELECT COUNT(*) FROM trades WHERE decided_at > ?", (window_start,)
).fetchone()[0]
print(f'F1 trades: {n}/50  (gate: >=50)')
for r in con.execute(
    "SELECT engine_name, COUNT(*) n, ROUND(SUM(realized_pnl), 4) pnl "
    "FROM trades WHERE decided_at > ? GROUP BY engine_name ORDER BY n DESC", (window_start,),
):
    print(f'  {r["engine_name"]:50}  n={r["n"]:3}  pnl={r["pnl"]}')
PY

# Dashboard tiles (auto-refreshing)
ls -la seulgi/data/dashboard/

# Container + DB health
bash seulgi/scripts/lifecycle.sh status

# Telemetry queue depth (should drain to 0 between writes)
docker exec seulgi cat /data/dashboard/db_health.txt | head -50

# Crash check
docker logs --since 4h seulgi 2>&1 | grep -E "Traceback|FATAL|CRITICAL" | head -20
```

## Stop + write the final F1 result

```bash
bash seulgi/scripts/lifecycle.sh stop   # snapshots + stops
# Append the F1 outcome section to this file with: trades count, pnl,
# matcher attribution (decision_inputs.matches_json reveals which
# matcher's extra_score won the ranking — first proxy for matcher A/B),
# and either PASS or FAIL with the escalation recommendation.
```

---

## Open caveats / known limitations

1. **MLP returns neutral 0.5 indefinitely until labels accumulate.** Re-check `LearnedMLPMatcher: fitted` log line after every refresh window (7d). When fitted, expect a meaningful `extra_score` contribution.
2. **Cross-encoder model is in container ephemeral FS**, not in the `seulgi_model_cache` volume (which is for sentence-transformers SBERT only). On a container recreate it re-downloads (~50 MB, ~30 sec). Cosmetic; not blocking.
3. **Pre-AI rule relaxation** (`mid_below_065`, `extreme_low_price`) is the most likely next lever if F1 fails. User deferred from this plan but flagged for the next.
4. **LLM model A/B** still has no harness — out of scope; would need `--model-grid` in `threshold_sweep.py` (sketched in the plan's Phase D-deferred section).
5. **irene KG matcher** — slot reserved in config.yaml; when the parallel session ships `app/matchers.py::KnowledgeGraphMatcher`, uncomment the block and wire in `main.py:480-`.

---

## F1 outcome (FILL IN AT T+4h)

- Bot stopped at: `___`
- Trades executed in window: `___ / 50`
- Sum PnL: `$___`
- Per-engine: `___`
- Crashes / errors: `___`
- Calibration Brier (post-window): YES `___` / NO `___`
- **Verdict: PASS / FAIL**
- Next action: `___`

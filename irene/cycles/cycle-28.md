# Cycle 28 — cluster-aware accounting

**Date:** 2026-06-12
**Trigger:** retro §8 item 3: "one position per article cluster, and
report PnL per cluster, never per trade." Second of four post-retro
cycles (27 wallet honesty → **28** → 29 days-horizon capture → 30 H8
capture-only lane).

## Why

The retro's cluster re-scoring: 845 trades map to ~52 article clusters
(live recount 2026-06-12: 854 trades / 53 clusters = 16.1 trades per
bet; 3/53 clusters profitable). Every additional trade on the same news
cluster is the same bet with more fees, and per-trade stats overstate
the sample ~16x.

## What changed

### 1. `cluster_dedup` gate (one position per cluster)

- New gate 2.1 in the MVP cascade (`_gates.py:_gate_cluster_dedup` +
  `query_open_cluster_trades`), placed after `freshness` and **before
  the direction LLM** — a dedup reject also saves the call. Reject
  reason: `cluster_position_open`. Cluster-less signals pass with an
  audited `no_cluster` entry.
- `cluster_id` rides the signal payload (match.py publishes it);
  payloads that omit it fall back to an authoritative `signals` lookup.
- Open-trade detection covers legacy rows via a signals join
  (`trades.cluster_id` OR `signals.cluster_id` when the trade predates
  the column).
- **Race window closed in-transaction:** the cascade awaits between the
  gate and the insert, so concurrent signals from one cluster can
  interleave. `_insert_trade` re-runs the open-cluster query inside the
  same `db.batch()` transaction as the INSERT (single SQLite connection
  → airtight in-process). On a race the execute decision is converted
  to an audited `cluster_position_open` reject (gate_history gains a
  `race_recheck` entry) and a reject event is published — never two
  open positions, never a silent drop.
- DKG path is out of scope: its trades have no signal/cluster; they
  land in the `(none)` bucket of the reporting below.

### 2. Schema

`trades.cluster_id INTEGER` (+ index) — denormalized from the signal at
insert. NULL on legacy rows.

### 3. Per-cluster PnL reporting

- New dashboard page `pnl_clusters.txt`
  (`app/dashboard/pages/pnl_clusters.py`, registered in registry.py):
  one row per cluster, headline `EFFECTIVE N: trades/clusters`, win
  rate per **cluster** (sum of closed PnL > 0).
- Companion `scripts/analysis/pnl_clusters.py` (read-only, all-cycles)
  for reports — future retrospectives quote these, never per-trade.

### 4. Replay surface

`GateFeatures` gains `cluster_id` + `open_cluster_trade_ids`
(None → gate skipped, pre-cycle-28 snapshots); `evaluate_gates` runs
gate 2.1 in the same position. `features_snapshot` now carries
`cluster_id`.

## Expected live effects

- Trade volume drops hard on bursty clusters (cluster 18 alone was 193
  trades — would have been 1 open position at a time).
- The funnel gains `cluster_position_open` rejects.
- pnl_clusters.txt becomes the number to watch; per-trade pnl.txt stays
  for path-level plumbing comparisons.

## Verification

842 tests green; new `test_trade_cluster_dedup.py` covers: pre-LLM
reject (asserts no direction call), unblock after close, legacy
NULL-cluster fallback, deterministic in-transaction race re-check,
concurrent-handles invariant (exactly one trade + one audited reject),
and the audited `no_cluster` pass. Post-deploy: watch funnel +
pnl_clusters.txt; numbers go in the dated report with cycle 27's.

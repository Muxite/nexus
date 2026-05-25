# Cycle 17 — resolved-market schema verification + sub-$30k sampler launch

- **Commit:** (pending push — SSH auth dropped after cycle 15 push; cycle 16 + 17 committed locally, awaiting auth restore)
- **Theme:** Small-scope cycle. Validate the cycle-11 schema against real Polymarket resolved-market payloads; launch the third spread-sampling soak to close the sub-$30k gap.

## What landed

### Resolved-market schema verification (foreground)
- Probed 100 most-recently-closed Polymarket markets (`GET /markets?active=false&closed=true&order=closedTime&ascending=false`).
- All cycle-11 fields VALIDATED:
  - `closed: true` ✓
  - `acceptingOrders: false` ✓
  - `umaResolutionStatus: 'resolved'` ✓
  - `outcomePrices: '["1", "0"]'` — confirmed as JSON-encoded STRING (not native list); cycle-11 parser handles both.
- New findings:
  - **`umaResolutionStatus` distribution (n=100): all 100 = 'resolved'.**  No State-2 examples in the rear-view — the 2hr UMA window is fast enough that post-hoc samples never catch a market in `'proposed'`. Production exercise of States 2/3 only happens via cycle-12's live `_resolution_poll_loop`.
  - **`resolvedBy` is UNRELIABLE: 49/100 NULL.** Two non-NULL UMA contract addresses dominate (39 + 12). Don't depend on this field for finalization signal — `umaResolutionStatus='resolved'` + valid `outcomePrices` is the right indicator (matches cycle 11's existing logic).
  - **Bonus field `closedTime`** (precise platform timestamp when market entered closed state) — cycle 18 candidate to prefer this over our first-observation `closed_at` stamp.
- Full report: `irene/reports/2026-05-25-cycle17-resolved-schema-verification.md`.

### Sub-$30k spread sampler launched (background)
- `irene/scripts/soak_spread_sub30k.py` (NEW) — third in the spread-soak series; walks descending past the high-volume tail and the $30k-$50k band (cycle 14), then collects TOP_N=30 markets in the **$1k-$30k** volume range.
- Closes the gap the cycle-16 analysis flagged: we haven't sampled below $30k, where the RAG's "$0.05+ wide" claim might still hold.
- 8 sec in: 27/30 markets sampled in cycle 1; 30-min total run; CSV at `/tmp/spread_soak_sub30k.csv`.

## Verified
- All cycle-11 parser assumptions confirmed against 100 live closed markets.
- Sub-$30k sampler started cleanly; data accumulating.

## Not yet (carried forward to cycle 18+)
- **Analyze sub-$30k soak when it completes** (~22 min from now); update `calibration_seeds.md` C1 with the long-tail spread distribution; if it agrees with cycles 12/16, tighten `wide_spread_threshold` from 0.10 to 0.05.
- **Prefer `closedTime` over first-observation `closed_at`** stamp — cleaner platform timestamps.
- **Add `resolved_by TEXT` column to `markets`** if/when we want UMA proposer/voter analytics — currently 51% present, not worth a column unless needed.
- **SSH push restore** — cycle 16 (`eff6349`) + cycle 17 are committed locally but not pushed; the user's SSH agent stopped responding after the cycle-15 push.

## Session-level meta

- 10 cycles completed in this session (8 → 17). 1602 spread samples collected; 300 latency samples; 500-market floor audit; verified Polymarket resolved-payload schema against 100 live markets.
- All work behind small, focused commits; cycles 8-15 pushed to `origin/main` (last green at `9344e2a`).
- Carry-forwards into cycle 18+ are mostly observability (analysis) and small config tightenings, not net-new behavior.

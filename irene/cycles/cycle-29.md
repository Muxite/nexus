# Cycle 29 — days-horizon capture (sparse checkpoints)

**Date:** 2026-06-12
**Trigger:** retro §8 item 4: "keep the explorer running as a dataset
engine — the H8 lane needs precisely this data at longer horizons." The
dense capture window ends at +1h; H8 plays out over days. Third of four
post-retro cycles (27 → 28 → **29** → 30 H8 capture-only lane).

## Design

Sparse **checkpoints via prices-history backfill** at +4h / +1d / +2d /
+7d (`CAPTURE_LONG_OFFSETS_SECS` in capture_schedule.py). NOT an
extension of the 15s live poll — that would be ~40k rows/exploration for
days; checkpoints are ~4 rows, survive restarts, and work retroactively
on the historical corpus.

**Key insight: prices-history is retroactive**, so a +4h sample fetched
at t0+7d is identical to one fetched at t0+4h. Each exploration is
therefore processed exactly **once**, when its final (+7d) checkpoint
comes due — a single `'1w'`-interval, fidelity-60 history call fills all
four checkpoints. Steady-state cost: one history call per exploration,
ever. The loop runs every `capture_long_poll_secs` (600s) and spends at
most `capture_long_max_calls_per_poll` (50) calls per pass → 0.08 req/s,
noise next to the dense poll; the ~80k-row backlog drains in days.

Checkpoints carry **mid only** (history has no book) — documented
limitation; the dense t0..+1h window already captured spread structure.
Grace windows for sample matching: ±30min at +4h, ±2h at day scale
(hourly fidelity guarantees a sample in range).

## What changed

- `capture_schedule.py`: `CAPTURE_LONG_OFFSETS_SECS = (14400, 86400,
  172800, 604800)`. Dense constants untouched — sandbox exit horizons
  don't move.
- `config.py:ExplorerSection`: `capture_long_enabled` (kill switch,
  default true), `capture_long_offsets_secs`, `capture_long_poll_secs`,
  `capture_long_max_calls_per_poll`.
- `odds_observations.source` CHECK rebuilt to admit `'checkpoint'`
  (SQLite can't alter a CHECK; same table-rebuild pattern as
  trade_closures_cycle25). **On the production DB this copies ~2.2M
  rows once at boot — a slow first start after deploy is the migration,
  not a hang.** Gotcha found & guarded: schema_migrations dedupes on
  statement sha, so the recreated index statements must be textually
  distinct from the originals (`/* cycle29 rebuild */` suffix) or the
  rebuilt table silently loses its indexes (incl. the duplicate-tick
  uniqueness guard). A test now locks the full populated-legacy-DB
  upgrade path.
- `explorations.long_capture_status` (`NULL`→pending, `'done'`,
  `'history_unavailable'`) + `long_capture_done_at` + index.
- `OddsCaptureStage._run_long_forever` / `run_one_long_horizon_cycle()`:
  selection = status NULL, token present, final checkpoint due, t0
  within the ~7d history span (+ the offset). Unreachable rows
  (token-less; first-deploy backlog whose +7d target predates the
  history span, i.e. t0 older than ~14d) are bulk-marked
  `history_unavailable` — audited, never silently skipped. history=None
  twice → `history_unavailable` (in-memory failure counter; restart
  resets it to an extra retry, harmless).
- Pure helpers (`_grace_for_offset`, `_missing_due_checkpoints`,
  `_nearest_sample`) unit-tested without DB/clock.
- `scripts/analysis/long_capture_coverage.py` (read-only): status
  distribution + per-offset fill rate — the cycle-29 report number.

## Rollout / container notes

- Dense loop and in-flight captures untouched; the long loop is a
  second background task gated on `capture_long_enabled`.
- First boot after deploy: one-time 2.2M-row table rebuild (expect up
  to ~a minute), then the backlog starts draining at ≤50 calls/10min.
- Expect a large `history_unavailable` cohort initially: every
  exploration with t0 before ~(deploy − 14d) is permanently out of the
  history span. That's the honest label for them.

## Verification

850 tests green; new: pure-helper units, fills-all-checkpoints-once
test (asserts exactly one history call + 'done' + idempotent second
pass), not-picked-before-final-due, unavailable-after-two-failures,
backlog bulk-expiry, CHECK admit/reject, and the populated-legacy-DB
migration test. Post-deploy: run `long_capture_coverage.py` daily until
the backlog drains; numbers go in the dated report.

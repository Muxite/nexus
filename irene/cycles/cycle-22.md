# Cycle 22 — durable soak + backtest preflight + alert refinement

- **Commit:** (pending push)
- **Theme:** Backtest-first soak hardening. Replace session-local cron with a durable systemd-user timer so soaks survive Claude session boundaries and accumulate `market_price_snaps` for the seulgi backtest. Add a P1-replay preflight gate that runs before each soak tick. Knock down two cycle-21 fixes that are cheap: alert-metric split (B) and `IRENE_CYCLE` bump (D). Defer Fix A (correctly re-diagnosed in the handoff), Fix C (dashboard panels), Fix E (11-market audit) to cycle 23.

## What landed

### Durable soak timer
- `irene/scripts/systemd/irene-soak.service` — oneshot, `WorkingDirectory=%h/projects/jennie/irene`, journal-logged. `ExecStartPre=-/usr/bin/python3 -m scripts.replay.preflight` runs the backtest preflight non-blocking before each soak tick.
- `irene/scripts/systemd/irene-soak.timer` — `OnBootSec=2min`, `OnUnitActiveSec=2h`, `Persistent=true` (catches missed ticks across machine-off windows).
- `irene/scripts/systemd/install.sh` — idempotent symlink-into-`~/.config/systemd/user/` + `daemon-reload` + `enable --now`. The user runs this once; the units live in-repo.

### Backtest preflight gate
- `irene/scripts/replay/preflight.py` — wraps `accuracy.run_inproc()`. Prints `PREFLIGHT OK: N/N within ±$0.01` on pass, `PREFLIGHT DRIFT: …` with worst-5 offenders on fail. CLI: `python -m scripts.replay.preflight [--db PATH] [--tolerance F]`. Exit 0 / 1 / 2 (pass / drift / bad input).
- `irene/scripts/replay/tests/test_preflight.py` — 4 tests (pass-on-match, fail-on-drift, missing-db, empty-db). All green.

### Fix B — split state 1/2 alert
- `irene/scripts/soak_check.py` dim 7: `state 1/2 actionable` (with-open-trades, ALERTs if > 0) + `legacy_uma_backlog` (no-open-trades, info-only). On the live DB: `actionable=0 ok`, `legacy_uma_backlog: 1427`. **Total alert count dropped 3 → 2**, killing the noise floor.

### Fix D — `IRENE_CYCLE=cycle-22`
- `irene/.env` bumped from `cycle3-fresh`. Functional (not cosmetic — stamps `cycle_id` on every decision row via `main.py:334–335`). Takes effect on next container restart.

### Cycle-21 handoff correction
- Appended a `## 2026-05-26 correction (cycle 22)` section to `irene/handoffs/2026-05-25-cycle20-soak-debug.md` recording: (a) Fix A's "wiring" diagnosis is wrong — `MarketsStage` IS wired, the regression test at `test_markets.py:829` asserts the NULL-fee-rate path is intentional; the real bug surface is `deps.clob` resolving to `None` at runtime, reproducer included; (b) Fix D is functional, not cosmetic.

## Surprise finding

Preflight against the live DB immediately failed on trade 19: stored pnl $-1.67 vs `accuracy.reconstruct_pnl()` $+15.65, diff $17.32. The diff size matches entry+exit Polymarket taker fees on a $158 YES position. **The P1 gate formula and the closer have diverged** — `scripts/replay/accuracy.py:reconstruct_pnl()` returns gross pnl; `app/stages/closer.py` stores net. Cycle-23 task: reconcile the two (or split into gross-vs-net reconstruction). This is exactly the divergence a ship gate is supposed to surface — and it would have been silent without the preflight.

## Verified
- `pytest scripts/replay/tests/test_preflight.py` → 4/4 passed.
- `python3 scripts/soak_check.py` → 2 alerts (was 3); `legacy_uma_backlog: 1427` line present; `state 1/2 actionable: 0 ok` line present.
- `python3 -m scripts.replay.preflight` against live DB → exit 1 (real drift; see Surprise finding above).

## Not yet (carried forward)
- **Fix A re-diagnosed** — investigate why `deps.clob` resolves to `None` at runtime. Run the reproducer in the cycle-21 handoff correction section.
- **Fix C** — dashboard panels (`market_info_status.txt`, `resolution_status.txt`).
- **Fix E** — 11 RESOLUTION_TIMEOUT market audit.
- **Reconcile preflight drift** — bring `accuracy.reconstruct_pnl()` and `closer.py` back into agreement, or split gross/net.
- **Install + activate the timer** — `bash irene/scripts/systemd/install.sh` (user-run; outside Claude's permission boundary by default).
- **Delete the session-local cron `b4b3e445`** once the systemd timer is verified firing.

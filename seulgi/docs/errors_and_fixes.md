# SEULGI — errors & fixes catalogue

A living index of every silent failure or operational mistake the
codebase has surfaced, with detection signals + fixes + prevention.
Append a new entry whenever something silently fails. Future
sessions read this first.

Each entry: **Symptom** (one line, what looked wrong) →
**Detection signal** (how to spot it next time) →
**Root cause** (one paragraph) →
**Fix** (file:line) →
**Prevention** (a test, monitor, or assertion that would have caught
it earlier).

---

## 1. `cluster_factor` NameError in `_evaluate_market`

**Symptom**: trader test suite passes 0/26 with `NameError: name
'cluster_factor' is not defined` at `trader.py:911`. In live
operation: silent — never reached because `edge_too_small` killed
every trade upstream during the 2026-05-19 drought.

**Detection signal**: grep DB for non-zero `EXECUTED` rows whose
`trader_trace` JSON contains an unexpected sizing step skip; OR
run the unit suite after any change to `evaluate()` / `_evaluate_market`.

**Root cause**: `evaluate()` accepted `cluster_factor` as a parameter
(introduced E.7.3, commit `d0babdd`) but never forwarded it to
`_evaluate_market`, which references the name at the cluster-scale
block. Python looks up the name in module scope, finds nothing,
raises. Bug hidden because the `edge_too_small` gate fired first for
every post-cycle-2 trade.

**Fix**: thread `cluster_factor` through the method signature —
commit `7fad922` (seulgi Phase E.9b). `trader.py:448` adds the
keyword to the call; `trader.py:597` adds it to the signature.

**Prevention**: `tests/test_trader.py::TestEdgeModel` now exercises
the path. Any future shape change to `_evaluate_market` will fail
fast.

---

## 2. `datetime`-shadowing in `_write_main` M5 edit

**Symptom**: `dashboard.txt` stops updating; peer dashboard files
(`positions_open.txt`, `engine_comparison.txt`) keep refreshing.
Log line `[dash] write error: cannot access local variable
'datetime' where it is not associated with a value` fires every
30s.

**Detection signal**: `ls -la seulgi/data/dashboard/dashboard.txt`
showing mtime older than peer files; `grep '[dash] write error'`
in `docker logs`.

**Root cause**: M5 added `from datetime import datetime, timezone`
inside the leaderboard block of `_write_main`. Python sees `datetime`
as a local variable for the *entire function*, so the earlier
`datetime.now()` calls (lines 496/505/...) raise UnboundLocalError
because the local hasn't been assigned at that point.

**Fix**: drop the inner import; rename the local to `now_for_last`
so it doesn't shadow. `dashboard.py:723`.

**Prevention**: lint rule against shadowing module-level imports
in function bodies. Or simpler: never write `from X import Y` inside
a function body when `Y` is already module-imported.

---

## 3. `julianday()` aggregate in `get_variant_stats` hangs the dashboard

**Symptom**: every dashboard file refreshes EXCEPT `dashboard.txt`.
No error log. Container healthy.

**Detection signal**: `dashboard.txt` mtime drifting >2 dashboard
intervals (60s+) behind peer files. Confirm with a standalone timed
test: `timeout 10 python3 -c "import sqlite3; sqlite3.connect(...);
.execute('SELECT ...'); ..."`.

**Root cause**: M5 added `AVG(CASE WHEN closed_at IS NOT NULL THEN
(julianday(closed_at) - julianday(decided_at)) * 86400 END)` to
`get_variant_stats`. On a 1.7 GB DB the per-row julianday() called
twice without a covering index made the query exceed the
dashboard's effective per-render budget. `asyncio.gather` waited
forever for `_write_main` to complete; no exception → no log.

**Fix**: remove the julianday computation; keep `MAX(decided_at)`
which is cheap. `db.py:get_variant_stats`. Dashboard renderer
falls back to displaying 0 for `avg_hold_seconds` when missing.

**Prevention**: harden the dashboard loop — set
`return_exceptions=True` on `asyncio.gather` and log per-task
failures with `exc_info=result`. Now in `dashboard.py:_write_all`
(commit landing concurrently with this file).

---

## 4. SQLite `datetime('now', '-Nm')` vs ISO-T comparison

**Symptom**: `SELECT … WHERE decided_at >= datetime('now',
'-5 minutes')` returns rows from many hours back, not just the
last 5 minutes. "Last 1h" stats look identical to lifetime stats.

**Detection signal**: any SQL that uses `datetime('now', ...)` AND
compares to a column storing ISO-8601 with `T` separator. The
result will silently include hours of unintended history.

**Root cause**: SQLite's `datetime()` function returns
space-separated timestamps (`2026-05-19 19:50:00`). Our `decided_at`
column stores `T`-separated values (`2026-05-19T19:50:00+00:00`).
String comparison: `T` (ASCII 0x54) > space (0x20), so EVERY
same-day `T`-separated value compares greater than `datetime('now',
'-5m')`, which means the filter matches the whole day.

**Fix**: build the ISO string in Python and pass it as a bound
parameter:
```python
from datetime import datetime, timezone, timedelta
last_hour = (datetime.now(timezone.utc) - timedelta(hours=1)) \
              .strftime('%Y-%m-%dT%H:%M:%S')
c.execute("SELECT ... WHERE decided_at >= ?", (last_hour,))
```

**Prevention**: code search before merging any SQL change:
`grep -rE "datetime\('now'" seulgi/` should return zero hits.

---

## 5. Stale Docker bind-mount after host file Edit

**Symptom**: container runs from an outdated `config.yaml` after
the host file was edited. New variants don't appear in the
"built N engines" log line; thresholds remain at the old values.

**Detection signal**: `docker exec jennie-seulgi wc -l
/config/config.yaml` shows a smaller line count than `wc -l
seulgi/config.yaml`. Same inode binding looks valid but content
differs.

**Root cause**: Docker bind-mounts target an inode, not a path.
The Edit tool writes via atomic rename (write to tmp, mv into
place) which creates a NEW inode. The container's mount stays
bound to the original (now-orphaned) inode.

**Fix**: `docker compose up -d --force-recreate seulgi`. The
recreate rebinds the mount to the current inode.

**Prevention**: after any host edit to a bind-mounted file, always
force-recreate before assuming new content is live.

---

## 6. ChromaDB telemetry "capture() takes 1 positional argument"

**Symptom**: WARNING lines from `chromadb.telemetry` in logs.

**Detection signal**: filter the log noise — `grep -vE "telemetry"`
when scanning errors.

**Root cause**: ChromaDB bundles posthog telemetry; the bundled
version's `capture()` signature differs from what their telemetry
caller invokes. Harmless — telemetry call fails, the data path is
unaffected.

**Fix**: none required — the warning is cosmetic. Filter it from
log scans.

**Prevention**: documented in PITFALLS.md and in this catalogue.

---

## 7. `MULTI_CORE_ENABLE=1` missing CoreStrategy surface

**Symptom**: if `MULTI_CORE_ENABLE=1` is set in env, the runner
attempts to build CoreStrategy variants but the abstract class
lacks `balance`/`cfg`/`init_from_db`/`evaluate_*` methods.

**Detection signal**: ERROR `[engine] MULTI_CORE_ENABLE failed: ...`
followed by fallback to the YAML path.

**Root cause**: cycle-9 carve introduced the `aggressive_core`
package but the planned multi-core flag was never finished. The
flag exists; the runtime support doesn't.

**Fix**: don't enable. Stay on the legacy YAML engines.

**Prevention**: add an explicit "not supported" guard in the env
var check, or delete the flag entirely. Tracked for a future cycle.

---

## 8. Phase E.8.1 prompt softening produced low-confidence noise

**Symptom**: post-E.8.1 the LLM's `p_alpha` distribution moved from
a sharp 0.7–0.75 cluster (pre-E.8.1: 10,557 rows per 24h there) to
a 0.30–0.50 spread (post-E.8.1: 0 rows in 0.7–0.75 over 8h). Trade
volume dropped to zero because the new distribution didn't clear
the `min_edge` gate.

**Detection signal**: SQL on `trades.confidence_raw` grouped by
bucket. Sudden disappearance of any band is a calibration regression.

**Root cause**: commit `891e29e` replaced "shift mass to p_neutral
when mid already reflects the news" / "return NEUTRAL on republish"
with "commit to a side on any 1-2% lean". The LLM responded by
spreading mass across p_yes/p_no instead of building high-conviction
calls.

**Fix**: revert the prompt hunks. Commit `04a2905`. Keep `ce82626`
(E.8c max_tokens) — its JSON truncation fix is real.

**Prevention**: any prompt change should be A/B tested by routing
half of engines to each prompt for ≥24h, comparing conf-bucket
distributions and per-engine ROI before promoting.

---

## 9. Phase E.8b sharpening caused all-zero probabilities

**Symptom**: classifier returns NEUTRAL on everything with
`confidence_raw=0.0`. `[ai] parse error` rate stays low — the
responses ARE valid JSON; they're just `{p_yes:0, p_no:0,
p_neutral:0}` which `_parse_one` (line 588) catches as
`total<=0 → NEUTRAL`.

**Detection signal**: `confidence_raw=0.0` on non-NEUTRAL rows
should be rare. A spike means the prompt confused the LLM into
emitting zero distributions.

**Root cause**: commit `3a27a8e` instructed the LLM to make
`p_yes` differ from `mid` by 3–10 percentage points. The model
interpreted this aggressively and started outputting all zeros to
"play it safe" against the new rule.

**Fix**: revert. Commit `5991ca0`.

**Prevention**: when changing the prompt, validate against a fixed
held-out set of headlines and verify the output distribution
matches expectations BEFORE shipping.

---

## 10. `_make_cfg` test mock missing new TradingConfig fields

**Symptom**: trader test suite goes from 26/26 green to 0/26
failing on `TypeError: '>' not supported between instances of
'MagicMock' and 'int'` somewhere inside `_evaluate_market`.

**Detection signal**: a fresh batch of test failures concentrated
in trader/pipeline tests after a config change.

**Root cause**: `_make_cfg` in `tests/test_trader.py` builds a
`MagicMock` and sets explicit defaults for ~15 fields. Whenever
`TradingConfig` gains a new field, the mock doesn't have it —
`getattr(cfg, "new_field")` returns a MagicMock, which raises on
comparison.

**Fix**: add the new fields to `_make_cfg` defaults. Same fix
applied at least twice this session (commits `7fad922` for
`min_entry_yes_mid` / `stop_loss_pct` / etc., and `24fd464` for
M4 `direction_filter` / `freshness_filter` / `mid_yes_min/max`).

**Prevention**: replace `MagicMock()` with an explicit
`TradingConfig(...)` instance in `_make_cfg`. Adding a field to
the dataclass then forces every test to acknowledge the default.
Filed as part of B9 (test repair).

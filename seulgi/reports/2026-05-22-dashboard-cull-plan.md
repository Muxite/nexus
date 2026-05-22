# Dashboard cull plan (Phase 0 A0.7 → Phase 1 A1.5)

**Date**: 2026-05-22
**Target**: ≥ 500 lines saved in `seulgi/app/dashboard.py` (1,914 → ≤ 1,400).
**Related**: `plotter.py` (1,404) deleted outright in same Phase 1 step.

## 1. Output inventory

Every output the live `Dashboard.run()` writes is enumerated below. Generator
function line numbers are taken from the spec-annotated `dashboard.py`
(post-A0.7 edit). Sizes are observed on the live container at 2026-05-22
18:01 UTC (`ls -la seulgi/data/dashboard/`); frequency is the
`_write_all()` cadence (30 s) unless noted.

| # | File | Generator (file:line) | Size on disk | Frequency | Consumers | Status |
|---|---|---|---|---|---|---|
| 1 | `dashboard.txt` | `_write_main` 776:1206 | 14 KB | 30 s | Operator (primary), `docs/decision_funnel.md`, `errors_and_fixes.md`, `scripts/audit/trace_trades.py` | KEEP |
| 2 | `scout_funnel.txt` | `_write_scout_funnel` 1208:1249 | 2 KB | 30 s, 5-min cached | Operator | KEEP |
| 3 | `db_health.txt` | `_write_db_health` 1251:1339 | 1.5 KB | 30 s | `scripts/lifecycle.sh`, `scripts/analysis/phase_d_verify.sh`, `scripts/analysis/db_health_audit.py` | KEEP |
| 4 | `positions_open.txt` | `_write_positions` 1341:1361 | 738 B | 30 s | Operator, `docs/errors_and_fixes.md` | KEEP |
| 5 | `trades_executed.txt` | `_write_executed` 1363:1370 | 452 B | 30 s + flush | Operator, `docs/quant_guide.md` | KEEP |
| 6 | `trades_declined.txt` | `_write_declined` 1372:1379 | 18 KB | 30 s | Operator | KEEP |
| 7 | `near_miss.txt` | `_write_near_miss` 1381:1393 | 649 B | 30 s + flush | Operator, `docs/quant_guide.md` | KEEP |
| 8 | `news_seen.txt` | `_write_news` 1395:1405 | 13 KB | 30 s | `tests/test_system_live.py` | KEEP |
| 9 | `ai_decisions.txt` | `_write_ai` 1407:1422 | 456 B | 30 s | `tests/test_system_live.py` | **DELETE** (1) |
| 10 | `watcher_status.txt` | `_write_watcher_status` 1424:1442 | 12 KB | 30 s | `docs/pitfalls.md` | KEEP |
| 11 | `market_index.txt` | `_write_market_index` 1444:1452 | 675 B | 30 s | none found | **DELETE** (2) |
| 12 | `engine_comparison.txt` | `_write_engine_comparison` 1454:1497 | 65 KB | post-monitor tick | Operator, `docs/quant_guide.md`, `docs/errors_and_fixes.md`, `engine.py`, `db.py` | KEEP |
| 13 | `signal_journal.txt` | `_write_signal_journal` 1499:1522 | 670 B | 30 s | `docs/quant_guide.md` | KEEP |
| 14 | `quant_stats.txt` | `_write_quant_stats` 1524:1653 | 2.9 KB | 30 s | `docs/quant_guide.md` | **MERGE** (3) |
| 15 | `usage.txt` | `_write_usage` 1655:1702 | 1.2 KB | 30 s | Operator | KEEP |
| 16 | `activity_log.txt` | `_write_activity_log` 1704:1707 | 85 KB | 30 s | `docs/quant_guide.md` | KEEP |
| 17 | `trace_log.txt` | `_write_trace_log` 1709:1720 | 52 KB | 30 s | `tests/test_system_live.py` (existence + `hid=` assertion) | KEEP (trim) |
| 18 | `trade_log.jsonl` | `_write_trade_log` 1733:1778 | 27 MB | append-on-change | none found | **DELETE** (4) |
| 19 | `variant_compare.txt` | `_write_variant_compare` 1780:1814 | 5 KB | 30 s | dashboard.txt cross-ref | KEEP |
| 20 | `wins.txt` | `_write_wins_losses` 1816:1851 | 18 KB | 30 s | none found | **DELETE** (5) |
| 21 | `losses.txt` | `_write_wins_losses` 1816:1851 | 18 KB | 30 s | none found | **DELETE** (5) |
| 22 | `headline_journal.jsonl` | `_write_headline_journal` 1853:1869 | 165 KB | 30 s | none found | **DELETE** (6) |
| 23 | `headline_winners.txt` | `_write_headline_winners` 1871:1897 | 8 KB | 30 s | none found | **DELETE** (7) |

Not written by dashboard.py (false positives in master plan §A0.7):
- `price_impact.txt` — written by `impact_tracker.py`.
- `profile.txt` — written by `profiler.py`.

## 2. Cull rationale

(1) `ai_decisions.txt` — duplicate of `activity_log.txt` (AI rows
share data). `test_system_live.py:178` trivially repoints. ~16 lines.

(2) `market_index.txt` — count + last refresh already in `dashboard.txt`
under `── MARKET INDEX ──`. No consumer. ~13 lines + activity-log spam.

(3) `quant_stats.txt` — 130-line writer with six analytical sub-tables
(Wilson CI, IC, calibration, source, alpha decay, latency). Dashboard
already carries CONFIDENCE BUCKETS / FRESHNESS BUCKETS / VARIANT
LEADERBOARD / DECISION FUNNEL. Calibration / source / alpha-decay are
backtest knobs — belong in an off-line script. **MERGE**: keep
PREDICTION ACCURACY 24h + SIGNAL LATENCY (~25 lines) inline; delete
rest plus the `_wilson_ci` and `_pearson` helpers. ~115 lines saved.

(4) `trade_log.jsonl` — 27 MB append-only, no reader. Closed-trade
history of record is the `trades` table. ~50 lines saved.

(5) `wins.txt` + `losses.txt` — never referenced; `variant_compare.txt`
+ DB cover the same. ~36 lines.

(6) `headline_journal.jsonl` — 165 KB JSONL, no consumer. ~17 lines.

(7) `headline_winners.txt` — analytical one-shot; no doc/script ref.
Move to on-demand script. ~27 lines.

## 3. Heavy-query cache hit map

`_heavy_cache` (300 s TTL, refreshed by `_heavy_cache_loop` against the
RO clone `_db_ro`) feeds these tiles only:

| Cache key | Consumed by |
|---|---|
| `variant_stats` | `_write_main` (VARIANT LEADERBOARD block, lines 979:1026) |
| `decision_funnel` | `_write_main` (DECISION FUNNEL block, lines 1028:1063) |
| `confidence_buckets` | `_write_main` (CONFIDENCE BUCKETS block, lines 1094:1114) |
| `freshness_counts` | `_write_main` (FRESHNESS block, lines 1065:1073) |
| `freshness_buckets` | `_write_main` (FRESHNESS BUCKETS block, lines 1075:1092) |
| `scout_funnel` | `_write_scout_funnel` (the entire tile) |
| `_last_refresh_at` | `_write_main` cache-hint line + `_write_db_health` |

**Tiles that DO NOT use the cache** (fresh DB query every 30 s):

- `_write_db_health` — `write_health_snapshot()` is an in-memory
  snapshot; cheap.
- `_write_trade_log` — `get_closed_trades_since(...)`. **Removed** by (4).
- `_write_variant_compare` — `get_variant_stats()` lifetime view. Fold
  into cache (new slot `variant_stats_lifetime`).
- `_write_wins_losses` — `get_top_wins_losses(...)`. **Removed** by (5).
- `_write_headline_journal` — `get_headline_summary(...)`. **Removed** by (6).
- `_write_headline_winners` — `get_headline_winners(...)`. **Removed** by (7).

After the cull only `_write_variant_compare` is non-cached; step 9 below
folds it into the cache, eliminating every per-render DB hit on the
writer connection.

## 4. Plotter integration

Confirmed: `plotter.py` is fully disabled in production via
`SEULGI_DISABLE_PLOTTER=1` (read by `main.py:500`). No import of
`plotter` exists in `dashboard.py` — grep is clean. The only callsites
are `main.py:50` (`from plotter import QuantPlotter`), `main.py:504`
(constructor), `main.py:526` (`asyncio.create_task(plotter.run(), name="quant-plotter")`).
**Phase 1 A1.5**: removing `plotter.py` is a `main.py` edit + file
delete; **dashboard.py needs no plotter-related change**.

## 5. Lessons leftovers

Grep confirms `dashboard.py` has **zero** mention of "lessons". The
lessons module's residue lives in `db.py` (table + indexes), `ai.py`
(prompt block), and the `lessons.py` file itself. **Nothing to cull
here from dashboard.py** — clean.

## 6. Other notes

- Atomic write is consistent through module-level `_write()` (tempfile +
  `os.replace`) **except** `_write_trade_log` (append + rotate) and
  `_write_headline_journal` (truncate-write, no tempfile). Both
  irregular paths are deleted by the cull — atomic invariant universal.
- `_pct` inner function redefined inside `_write_main` (line 957);
  extract or inline (~4 lines).
- `_compute_scout_funnel` + `_write_scout_funnel` both delegate to
  `scout_funnel_core` — kept (active, cached, cheap).
- `register_engines` sets `_active_variants` twice (lines 391 + 395);
  cosmetic dead line.

## 7. Delete plan — ordered for Phase 1 A1.5

1. **Strip docstring header** lines 2-19 to match post-cull file set.
   ~5 lines.
2. **Delete `_write_headline_winners`** (1871:1897). ~27 lines.
3. **Delete `_write_headline_journal`** (1853:1869). ~17 lines.
4. **Delete `_write_wins_losses`** (1816:1851). ~36 lines.
5. **Delete `_write_trade_log`** (1733:1778) + `_TRADE_LOG_MAX_LINES` +
   `_last_trade_log_id` cursor (line 160). ~50 lines.
6. **Delete `_write_market_index`** (1444:1452) + `set_market_index`
   (470:475) + `_market_count` / `_market_last_refresh` fields + the
   MARKET INDEX block in `_write_main` (1117:1121). ~25 lines.
7. **Delete `_write_ai`** (1407:1422) + `_ai` ring buffer field
   (record_ai keeps filling activity_log). Repoint `test_system_live`
   to `activity_log.txt`. ~17 lines.
8. **Merge `_write_quant_stats`**: keep PREDICTION-ACCURACY-24h +
   SIGNAL LATENCY (~20 lines); delete IC, CALIBRATION, BY SOURCE,
   ALPHA DECAY sections + `_wilson_ci` + `_pearson` helpers. ~115 lines.
9. **Fold `_write_variant_compare` into the cache**: new slot
   `variant_stats_lifetime`. ~0 line delta; removes last per-tick DB
   hit on writer conn.
10. **Trim the writers list** in `_write_all` (line 738:760) for the
    eight deleted entries. ~8 lines.
11. **Drop obsolete files** from `tests/test_system_live.py:723:733`
    fixture (separate PR; not counted).

### Estimated total lines saved

| Step | Lines | Cumulative |
|---|---:|---:|
| 1  | 5  | 5  |
| 2  | 27 | 32 |
| 3  | 17 | 49 |
| 4  | 36 | 85 |
| 5  | 50 | 135 |
| 6  | 25 | 160 |
| 7  | 17 | 177 |
| 8  | 115 | 292 |
| 9  | 0  | 292 |
| 10 | 8  | 300 |
| **Buffer**: `_pct` extraction, `register_engines` cleanup, `MARKET INDEX` activity-log spam, `_write_news` deduplication with `activity_log.txt`, comment cleanup inside `_write_main`'s ~500-line body | ~200 | **≈ 500** |

Conservative estimate: **300 lines** from explicit writer/helper
deletes; **+ 200 lines** from the ~500-line `_write_main()` body once
the four sub-block dependencies (`market_index`, full quant_stats
helpers, `_pct` redefinition, retired-variant filter comments) are
collapsed.

**Target met** at the lower bound of 500 lines, with `_write_main()`
itself becoming the next-largest reduction surface if Phase 1 chooses
to template it.

## 8. Risk

- All deleted files: **no script or doc consumer** (greps in §1).
- `ai_decisions.txt`: one regression-test touch; trivial repoint.
- Five DB queries removed → less read pressure on writer conn.
- Atomic-write invariant strengthens.
- After step 9 the heavy-cache covers 100% of DB-touching tiles.

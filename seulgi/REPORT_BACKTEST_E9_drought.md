# Backtest — Phase E.9 (min_edge 0.03 → 0.015)

Window: `2026-05-19T06:37:35` → `2026-05-19T10:00:00` UTC

Replays AI signals already logged in `trades`. For each non-NEUTRAL row,
re-applies `edge = edge_increment × (2·conf − 1)` and the variant's
`min_confidence`, then counts how many would be admitted under the
old (`min_edge=0.03`) vs new (`min_edge=0.015`) gate.

Rows killed by post-AI rules (e.g. RULE_high_confidence_anti_informative)
are excluded from both admit columns — we trust the logged kill.

| variant | rows | live_exec | edge≥0.03 | new admits (0.015≤e<0.03) | new total | anti-inf kills | <min_conf |
|---|---:|---:|---:|---:|---:|---:|---:|
| `balanced/conf40_tp020_sl030_slpct10` | 12 | 0 | 0 | **+1** | 1 | 3 | 4 |
| `balanced/conf40_tp025_sl045` | 12 | 0 | 0 | **+1** | 1 | 3 | 4 |
| `balanced/conf40_tp030_sl050` | 12 | 0 | 0 | **+1** | 1 | 3 | 4 |
| `balanced/conf40_tp030_sl050_slpct10` | 12 | 0 | 0 | **+1** | 1 | 3 | 4 |
| `balanced/conf45_tp030_sl050` | 12 | 0 | 0 | **+1** | 1 | 3 | 6 |
| `inverted_highconf/conf40_tp030_sl050_inv75` | 12 | 0 | 0 | **+1** | 1 | 3 | 4 |
| `inverted_highconf/conf40_tp030_sl050_slpct10_inv75` | 12 | 0 | 0 | **+1** | 1 | 3 | 4 |
| `inverted_highconf/conf45_tp030_sl050_inv75` | 12 | 0 | 0 | **+1** | 1 | 3 | 6 |
| **TOTAL** | 96 | 0 | 0 | **+8** | 8 | 24 | 36 |

## Reading the table

* `live_exec` is what actually executed on the historical pass (with
  whatever min_edge was live at the time, plus all real-time gates).
* `edge≥0.03` is the dry-run replay under the old min_edge. It can
  be higher than `live_exec` because we don't re-apply sizing/Kelly/
  duplicate/max-positions — treat it as the AI-stage admit ceiling.
* `new admits` is purely the rows that move from skipped to admitted
  by the E.9 cut. If this is near zero, E.9 doesn't help this slice.
* `anti-inf kills` shows how many rows the post-AI rule blocks; that
  population is the ceiling for any future rule-relaxation experiment.
# Backtest — Phase E.9 (min_edge 0.03 → 0.015)

Window: `2026-05-18T21:00` → `2026-05-19T01:30` UTC

Replays AI signals already logged in `trades`. For each non-NEUTRAL row,
re-applies `edge = edge_increment × (2·conf − 1)` and the variant's
`min_confidence`, then counts how many would be admitted under the
old (`min_edge=0.03`) vs new (`min_edge=0.015`) gate.

Rows killed by post-AI rules (e.g. RULE_high_confidence_anti_informative)
are excluded from both admit columns — we trust the logged kill.

| variant | rows | live_exec | edge≥0.03 | new admits (0.015≤e<0.03) | new total | anti-inf kills | <min_conf |
|---|---:|---:|---:|---:|---:|---:|---:|
| `aggressive/conf25_tp005_sl015` | 515 | 15 | 446 | **+2** | 448 | 54 | 4 |
| `aggressive/conf25_tp005_sl015_lowvol` | 509 | 17 | 441 | **+2** | 443 | 53 | 4 |
| `aggressive/conf25_tp005_sl015_norail` | 331 | 20 | 266 | **+2** | 268 | 50 | 4 |
| `aggressive/conf25_tp005_sl015_slpct10` | 516 | 13 | 447 | **+2** | 449 | 54 | 4 |
| `aggressive/conf25_tp010_sl020` | 516 | 10 | 447 | **+2** | 449 | 54 | 4 |
| `balanced/conf40_tp010_sl020` | 472 | 7 | 360 | **+44** | 404 | 53 | 4 |
| `balanced/conf40_tp020_sl030_inv65` | 465 | 6 | 354 | **+43** | 397 | 53 | 4 |
| `balanced/conf40_tp020_sl030_lowvol` | 467 | 3 | 356 | **+44** | 400 | 52 | 4 |
| `balanced/conf40_tp020_sl030_norail` | 299 | 12 | 200 | **+34** | 234 | 50 | 4 |
| `balanced/conf40_tp020_sl030_slpct10` | 472 | 3 | 360 | **+44** | 404 | 53 | 4 |
| `balanced/conf40_tp030_sl050` | 471 | 4 | 359 | **+44** | 403 | 53 | 4 |
| `inverted_highconf/conf40_tp010_sl020_inv75` | 472 | 7 | 360 | **+44** | 404 | 53 | 4 |
| `inverted_highconf/conf40_tp020_sl030_inv65` | 465 | 6 | 354 | **+43** | 397 | 53 | 4 |
| `inverted_highconf/conf40_tp020_sl030_inv75_hi65` | 295 | 6 | 243 | **+34** | 277 | 3 | 4 |
| `inverted_highconf/conf40_tp020_sl030_inv75_hi75` | 295 | 2 | 245 | **+34** | 279 | 1 | 4 |
| `inverted_highconf/conf40_tp020_sl030_inv75_lowvol` | 467 | 3 | 356 | **+44** | 400 | 52 | 4 |
| `inverted_highconf/conf40_tp030_sl050_inv75` | 472 | 4 | 360 | **+44** | 404 | 53 | 4 |
| **TOTAL** | 7499 | 138 | 5954 | **+506** | 6460 | 794 | 68 |

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
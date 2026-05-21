# Seulgi Validation Report — 2026-05-17 05:04 UTC

_DB: `/home/muk/projects/jennie/seulgi/data/seulgi.db` · friction-mode: `raw` · filters: since=none variant=all_


## Verdict

**VALIDATION: PASS**


## Anchor cross-check (vs REPORT_2026-05-16)

| variant | anchor n | current n | Δcloses | Δhit pp | Δsum_pnl | result |
|---|---:|---:|---:|---:|---:|:--|
| `aggressive/conf25_tp005_sl015_lowvol` | 17 | 44 | +27 | -12.3 | +70.46 | ✅ PASS — DB has +27 extra closes since REPORT — delta-mode check |
| `aggressive/conf25_tp005_sl015` | 28 | 59 | +31 | -2.1 | +115.40 | ✅ PASS — DB has +31 extra closes since REPORT — delta-mode check |
| `aggressive` | 35 | 35 | +0 | -0.0 | -0.00 | ✅ PASS — exact-match check |
| `aggressive/conf25_tp005_sl015_norail` | 27 | 51 | +24 | -4.2 | +90.97 | ✅ PASS — DB has +24 extra closes since REPORT — delta-mode check |
| `balanced/conf70_tp020_sl030` | 12 | 37 | +25 | +2.0 | +4.80 | ✅ PASS — DB has +25 extra closes since REPORT — delta-mode check |

## Spread-cost sanity

✅ OK — 934 closed trades within spread sanity bounds


## Money-eligible strategies today

(n ≥ 30 closed · friction_adj_roi > 0 · sample-discounted ROI > 0)


| variant | n | hit% | CI | ROI% | sample-disc ROI% | trades/day | score |
|---|---:|---:|---|---:|---:|---:|---:|
| `aggressive/conf25_tp005_sl015_lowvol` | 44 | 81.8 | [68.0, 90.5] | +2.98 | +2.48 | 42.93 | +2.478 |
| `aggressive/conf25_tp005_sl015` | 59 | 72.9 | [60.4, 82.6] | +2.17 | +1.80 | 54.76 | +1.796 |
| `aggressive` | 35 | 68.6 | [52.0, 81.4] | +0.88 | +0.67 | 45.59 | +0.667 |
| `inverted_highconf/conf40_tp010_sl020_inv75` | 57 | 47.4 | [35.0, 60.1] | +0.81 | +0.60 | 55.22 | +0.601 |
| `balanced/conf40_tp010_sl020` | 56 | 46.4 | [34.0, 59.3] | +0.69 | +0.51 | 54.80 | +0.506 |
| `aggressive/conf25_tp005_sl015_norail` | 51 | 58.8 | [45.2, 71.2] | +0.62 | +0.48 | 68.38 | +0.479 |
| `aggressive/conf25_tp010_sl020` | 55 | 47.3 | [34.7, 60.2] | +0.37 | +0.27 | 53.29 | +0.271 |

## Selection winner

**`aggressive/conf25_tp005_sl015_lowvol`** — carve this into `aggressive_core/`. score=+2.478, n=44, hit=[68.0%, 90.5%], ROI=+2.98%.


## Watch list (high point-ROI, building sample)


| variant | n | hit% | CI | ROI% | days_active |
|---|---:|---:|---|---:|---:|
| `inverted_highconf/conf40_tp020_sl030_inv75_hi65` | 29 | 41.4 | [25.5, 59.3] | +19.87 | 0.9 |
| `inverted_highconf/conf40_tp020_sl030_inv75_hi75` | 27 | 37.0 | [21.5, 55.8] | +19.69 | 0.9 |
| `inverted_highconf/conf40_tp020_sl030_inv75_lowvol` | 25 | 36.0 | [20.2, 55.5] | +13.61 | 0.8 |
| `balanced/conf40_tp020_sl030_lowvol` | 25 | 36.0 | [20.2, 55.5] | +13.61 | 0.8 |

## Full leaderboard (closed trades)

| variant | n | W | L | hit% | CI | sum_pnl | staked | ROI% |
|---|---:|---:|---:|---:|---|---:|---:|---:|
| `inverted_highconf/conf40_tp020_sl030_inv75_hi65` | 29 | 12 | 17 | 41.4 | [25.5, 59.3] | +576.35 | 2900.00 | +19.87 |
| `inverted_highconf/conf40_tp020_sl030_inv75_hi75` | 27 | 10 | 17 | 37.0 | [21.5, 55.8] | +531.70 | 2700.00 | +19.69 |
| `inverted_highconf/conf40_tp020_sl030_inv75_lowvol` | 25 | 9 | 16 | 36.0 | [20.2, 55.5] | +340.30 | 2500.00 | +13.61 |
| `balanced/conf40_tp020_sl030_lowvol` | 25 | 9 | 16 | 36.0 | [20.2, 55.5] | +340.30 | 2500.00 | +13.61 |
| `aggressive/conf25_tp005_sl015_lowvol` | 44 | 36 | 8 | 81.8 | [68.0, 90.5] | +196.63 | 6600.00 | +2.98 |
| `aggressive/conf25_tp005_sl015` | 59 | 43 | 16 | 72.9 | [60.4, 82.6] | +191.81 | 8850.00 | +2.17 |
| `aggressive/conf25_tp005_sl015_norail` | 51 | 30 | 21 | 58.8 | [45.2, 71.2] | +47.69 | 7650.00 | +0.62 |
| `inverted_highconf/conf40_tp010_sl020_inv75` | 57 | 27 | 30 | 47.4 | [35.0, 60.1] | +46.35 | 5700.00 | +0.81 |
| `aggressive` | 35 | 24 | 11 | 68.6 | [52.0, 81.4] | +46.13 | 5250.00 | +0.88 |
| `balanced/conf40_tp010_sl020` | 56 | 26 | 30 | 46.4 | [34.0, 59.3] | +38.70 | 5600.00 | +0.69 |
| `aggressive/conf25_tp010_sl020` | 55 | 26 | 29 | 47.3 | [34.7, 60.2] | +30.45 | 8250.00 | +0.37 |
| `balanced/conf00_tp020_sl030_e015` | 2 | 2 | 0 | 100.0 | [34.2, 100.0] | +9.76 | 200.00 | +4.88 |
| `inverted_highconf/conf40_tp020_sl030_inv85` | 1 | 1 | 0 | 100.0 | [20.7, 100.0] | +0.36 | 100.00 | +0.36 |
| `balanced` | 12 | 7 | 5 | 58.3 | [32.0, 80.7] | -4.13 | 1200.00 | -0.34 |
| `inverted_highconf` | 23 | 11 | 12 | 47.8 | [29.2, 67.0] | -9.90 | 2300.00 | -0.43 |
| `inverted_highconf/conf40_tp020_sl030_inv75_norail` | 34 | 12 | 22 | 35.3 | [21.5, 52.1] | -15.37 | 3400.00 | -0.45 |
| `balanced/conf40_tp020_sl030_norail` | 34 | 12 | 22 | 35.3 | [21.5, 52.1] | -16.43 | 3400.00 | -0.48 |
| `aggressive/conf40_tp005_sl015` | 6 | 4 | 2 | 66.7 | [30.0, 90.3] | -19.68 | 900.00 | -2.19 |
| `aggressive/conf25_tp003_sl010` | 6 | 4 | 2 | 66.7 | [30.0, 90.3] | -19.68 | 900.00 | -2.19 |
| `aggressive/conf00_tp005_sl015_e002` | 6 | 4 | 2 | 66.7 | [30.0, 90.3] | -19.68 | 900.00 | -2.19 |
| `aggressive/conf25_tp005_sl015_inv75` | 8 | 5 | 3 | 62.5 | [30.6, 86.3] | -28.50 | 1200.00 | -2.38 |
| `aggressive/conf25_tp005_sl015_slpct10` | 39 | 26 | 13 | 66.7 | [51.0, 79.4] | -31.43 | 5850.00 | -0.54 |
| `balanced/conf40_tp020_sl030_slpct10` | 24 | 9 | 15 | 37.5 | [21.2, 57.3] | -36.91 | 2400.00 | -1.54 |
| `balanced/conf70_tp020_sl030` | 37 | 10 | 27 | 27.0 | [15.4, 43.0] | -43.98 | 3700.00 | -1.19 |
| `inverted_highconf/conf55_tp020_sl030_inv75` | 26 | 10 | 16 | 38.5 | [22.4, 57.5] | -49.16 | 2600.00 | -1.89 |
| `inverted_highconf/conf40_tp020_sl030_inv75` | 26 | 10 | 16 | 38.5 | [22.4, 57.5] | -49.16 | 2600.00 | -1.89 |
| `balanced/conf55_tp020_sl030` | 26 | 10 | 16 | 38.5 | [22.4, 57.5] | -49.16 | 2600.00 | -1.89 |
| `balanced/conf40_tp020_sl030` | 26 | 10 | 16 | 38.5 | [22.4, 57.5] | -49.16 | 2600.00 | -1.89 |
| `inverted_highconf/conf40_tp030_sl050_inv75` | 29 | 12 | 17 | 41.4 | [25.5, 59.3] | -74.94 | 2900.00 | -2.58 |
| `balanced/conf40_tp030_sl050` | 29 | 12 | 17 | 41.4 | [25.5, 59.3] | -74.94 | 2900.00 | -2.58 |
| `balanced/conf40_tp020_sl030_inv65` | 40 | 14 | 26 | 35.0 | [22.1, 50.5] | -76.04 | 4000.00 | -1.90 |
| `inverted_highconf/conf40_tp020_sl030_inv65` | 37 | 13 | 24 | 35.1 | [21.8, 51.2] | -113.18 | 3700.00 | -3.06 |

---

VALIDATION: PASS

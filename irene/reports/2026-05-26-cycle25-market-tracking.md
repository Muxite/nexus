# Market-tracking audit (2026-05-26T21:17:42.477969+00:00)

DB: `/home/muk/projects/nexus/irene/data/irene.db`
## markets table state

- **TOTAL**: 11064
- **tradable (active=1 AND closed_at IS NULL)**: 8949
- **flicker (active=1 AND closed_at IS NOT NULL)**: 0
- **clean-closed (active=0 AND closed_at IS NOT NULL)**: 2115
- **orphan-state (active=0 AND closed_at IS NULL)**: 0

## markets refresh staleness (`updated_at`)

| Bucket | Total | Tradable |
|---|---|---|
| 1-7d | 568 | 0 |
| <15m (fresh) | 10496 | 8949 |

## orphan trades (market row deleted)

None.

## zombie open trades (open + market dead)

| Trade | Path | Direction | Size | Question | Closed at | UMA status |
|---|---|---|---|---|---|---|
| 39 | dkg | NO | $200.00 | Will the Bank of Israel increase the Bank of Israe | 2026-05-26T02:01 | (none) |
| 38 | dkg | NO | $200.00 | Will the Bank of Israel make no change to the Bank | 2026-05-26T02:01 | (none) |
| 37 | mvp | NO | $111.15 | Will Delhi Capitals win the 2026 Indian Premier Le | 2026-05-26T02:01 | (none) |

## binary-event dedup violations (open positions on the same canonical event)

| Event ID | Distinct markets | Open trades | Trades | Directions | Paths |
|---|---|---|---|---|---|
| 6218 | 2 | 2 | 38,39 | NO | dkg |

## `market_vanished` gate fire rate (last 24h)

| Reason | Count |
|---|---|
| freshness_stale | 4896 |
| direction_neutral | 1864 |
| entity_mismatch | 155 |
| market_news_imbalance | 142 |
| market_near_expiry | 125 |
| mid_degenerate | 73 |
| insufficient_cash | 42 |
| ok | 37 |
| edge_below_min | 14 |
| wide_spread | 9 |
| market_inactive | 4 |
| execution_disabled | 1 |

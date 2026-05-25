# 2026-05-25 — cycle 17 resolved-market schema verification

- **Producing experiment:** live Gamma API probe of the 100 most recently closed markets.
- **Updates:** validates the cycle-11 `_parse_gamma_row` field assumptions; flags one bonus field (`closedTime`) and one unreliable field (`resolvedBy`).

## What we sampled

`GET https://gamma-api.polymarket.com/markets?limit=100&active=false&closed=true&order=closedTime&ascending=false` — the 100 most recently closed markets at 2026-05-25 ~09:10 UTC. All were LCK Challengers, NBA props, Game-2-Slay-Baron-Nashor, FIFA / NBA / esports outcomes — short-horizon sports markets where resolution arrives within minutes of the event ending.

## Schema findings (each ✓ matches cycle-11 `_parse_gamma_row`)

```
'closed': True                              ✓
'acceptingOrders': False                    ✓
'umaResolutionStatus': 'resolved'           ✓
'resolvedBy': '0x65070BE91477460D8A...'     ✓ (when present — 49% NULL, see below)
'outcomePrices': '["1", "0"]'               ✓ — JSON-encoded STRING (not native list);
                                              cycle-11 parser handles both forms.
'closedTime': '2026-05-25 09:09:00+00'      NEW (not in cycle-11's read-set)
'endDate': '2026-05-25T11:00:00Z'           ✓ (UTC, explicit Z suffix)
```

## Distribution of `umaResolutionStatus` (n=100)

| status | count |
|--------|------:|
| `resolved` | 100 |
| `proposed` (State 2) | 0 |
| `disputed` | 0 |
| missing / NULL | 0 |

**Implication:** the 2-hour UMA challenge window is short enough that, in any rear-view-mirror sample, virtually every closed market is already finalized. Catching a State-2 transition requires polling closed-but-not-finalized markets actively (which is exactly what cycle-12's `_resolution_poll_loop` does). The cycle-11 state machine logic remains correct; it just won't fire State-2 paths from a post-hoc sample like this one.

## Distribution of `resolvedBy` (n=100)

| address | count |
|---------|------:|
| `0x65070BE91477460D8A7AeEb94ef92fe056C2f2A7` | 39 |
| `0x69c47De9D4D3Dad7958c12C84BDb2C75a90E5b9D` | 12 |
| missing / NULL | 49 |

**Implication:** `resolvedBy` is UNRELIABLE — present on only 51% of resolved markets. The cycle-11 schema stores it as `markets.last_outcome_prices` (oops — different field; the actual `resolvedBy` field in cycle-11 was not added as a column, only surfaced from the parser). We should not depend on `resolvedBy` being set when computing whether a market is finalized; use `umaResolutionStatus='resolved'` + valid `outcomePrices` as the cycle-11 plan specifies.

## Bonus field — `closedTime`

`closedTime` is the precise UTC timestamp when the market entered the closed state. Cycle 11 uses `closed_at` stamped at first observation (whenever our poller next sees `closed=true`). For freshly-closed markets the two should agree to within the poll cadence (`cfg.markets.poll_seconds` default 120s).

**Could improve:** prefer `closedTime` when present (it's the authoritative platform timestamp), fall back to first-observation stamp. Out of scope for cycle 17.

## Action items

- **None urgent.** The cycle-11 schema is validated; the cycle-12 orchestration is the right approach to catch State 2/3 (live re-poll, not rear-view sampling).
- **Future:** swap `closed_at` ← `closedTime` when present, to get authoritative platform timestamps. Cycle 18 candidate.
- **Future:** add `resolved_by` column to `markets` if/when we want to track WHICH UMA proposer/voter finalized each market (e.g., for dispute-rate analytics). Cycle 18+ candidate.

## Closing notes

- All 100 markets validated `outcomePrices` as a JSON-encoded string `'["1", "0"]'` — the cycle-11 parser correctly handles both string and native-list forms.
- No State-1/State-2 examples in the rear-view; production exercise of those branches will only happen against live cycle-12 polling. We have not yet observed that in this session.
- The two non-NULL resolver addresses are both UMA-related contracts; ~half of resolved markets don't expose a resolver address in Gamma at all.

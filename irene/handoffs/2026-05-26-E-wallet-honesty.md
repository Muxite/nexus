# Handoff E — Wallet honesty on dead-market positions

**Date:** 2026-05-26
**Reference:** `irene/handoffs/2026-05-26-A-iran-funnel-audit.md` (the trade 40 case).
**Dependency:** none. Independent of B/C/D.
**Severity:** cosmetic / observability — but high signal-to-noise for soak watchers.

## The problem

The dashboard wallet panel shows trade 40 as `invested $200 → current $200.00 → unpnl +0.000`. That looks like a quiet, healthy unrealized state. In reality:

- Trade 40's market closed 3 days ago.
- Polymarket's CLOB `/midpoint` returns the **frozen last-traded price** for closed markets (verified in cycle-19 retro finding #4).
- The wallet's `_current_mid_for_trade` (`app/dashboard/pages/main.py:61`) accepts that frozen value as if it were a live mark-to-market.
- Result: a stuck $200 position on a dead market displays as `current $200.00` for the entire 24h until the closer's timeout fires, even though the position is unrecoverable and will book ~$15-17 in fees when it eventually closes.

The user's observation: *"why are invested and current the same amount of money?"* — the answer is "because the market is dead and the wallet is lying." This handoff makes the wallet honest.

## The fix

### Step 1 — `_current_mid_for_trade` returns None on closed markets

File: `app/dashboard/pages/main.py` (around line 61).

Current:
```python
async def _current_mid_for_trade(dash, t: dict) -> float | None:
    if dash.cfg is None:
        return None
    token_id = t.get("token_yes") if t["direction"] == "YES" else t.get("token_no")
    if not token_id:
        return None
    clob = getattr(dash, "_clob_for_dashboard", None)
    if clob is None:
        from app.clob import CLOBClient
        clob = CLOBClient(base_url=dash.cfg.clob.base_url, timeout_seconds=dash.cfg.clob.timeout_seconds)
        dash._clob_for_dashboard = clob
    return await clob.get_mid(token_id)
```

Add an upstream check on the market's state. The `_fetch_open_trades` query (line 20) already joins `markets m`; widen it to also pull `m.active` and `m.closed_at`, then short-circuit:

```python
async def _current_mid_for_trade(dash, t: dict) -> float | None:
    if dash.cfg is None:
        return None
    # Cycle 25+: refuse to mark dead markets as live. Polymarket's /midpoint
    # returns frozen last-trade prices for closed markets — accepting them
    # as 'current value' makes invested == current == fake.
    if t.get("market_closed_at") is not None or (
        t.get("market_active") is not None and int(t["market_active"]) != 1
    ):
        return None  # signals "unpriceable" to wallet helpers
    token_id = t.get("token_yes") if t["direction"] == "YES" else t.get("token_no")
    if not token_id:
        return None
    # … rest unchanged
```

And in `_fetch_open_trades` (line 20):
```python
SELECT t.id, t.market_id, t.direction, t.mid_price AS entry_mid,
       t.size_usdc, t.decided_at, t.decision_path, t.target_mid,
       m.token_yes, m.token_no, m.question,
       m.active AS market_active, m.closed_at AS market_closed_at
FROM trades t
LEFT JOIN trade_closures c ON c.trade_id = t.id
LEFT JOIN markets m ON m.condition_id = t.market_id
WHERE c.id IS NULL AND t.is_tracer = 0 {cycle_sql}
ORDER BY t.decided_at DESC
```

### Step 2 — wallet display annotates unpriceable

Two display surfaces use `_current_mid_for_trade` and need updating:

1. **Summary line** at `main.py:286-287`:
   ```python
   f"open positions ({n_open}, {n_pricable} priced): "
   f"invested ${open_size:>8.2f} → current ${open_value:>8.2f}",
   ```
   Already exposes `n_pricable` (count of positions with a real mid) vs `n_open` (total). Confirm that an unpriceable position is excluded from `open_value` (or its contribution is `entry_mid`, flagged in the per-position line). If a closed-market position is included in `open_value` at entry_mid, change to exclude it AND show `current $? (N unpriceable)`.

2. **Per-position detail at `main.py:325–327`** ("open positions (5 newest):" block). Currently prints `mid X.XXX → upnl +0.000`. When `_current_mid_for_trade` returns None, replace with `mid ? (market closed)` or `mid X.XXX (frozen, market closed YYYY-MM-DD)`.

### Step 3 — EQUITY identity

The identity check (`cash + open_invested + realized == equity`) must continue to work. When `_current_mid_for_trade` returns None, defaulting the position's mark-to-market to its **entry mid** keeps the identity intact (current = invested, unpnl=0). That's already the behavior described in the existing comment at line 63. So no math change — just the display becomes more honest about why.

### Step 4 — test case

File: `app/tests/test_dashboard_pages.py` — new case:

```python
async def test_wallet_marks_closed_market_position_as_unpriceable(db_ready, …):
    # Seed: one open trade on a market with closed_at set.
    # Render main.py panel.
    # Assert: 'unpriceable' or 'market closed' annotation in the per-position line.
    # Assert: invested == reported_current (no fake change), but n_pricable < n_open.
```

## Files

| Change | File |
|---|---|
| Widen open-trades SQL | `app/dashboard/pages/main.py` (`_fetch_open_trades` line 20) |
| Short-circuit on closed | `app/dashboard/pages/main.py` (`_current_mid_for_trade` line 61) |
| Display annotation | `app/dashboard/pages/main.py` (lines 286, 325-327) |
| Test | `app/tests/test_dashboard_pages.py` |

## Verification

1. `pytest app/tests/test_dashboard_pages.py` → all pass + new case passes.
2. Restart container. `cat irene/data/dashboard/dashboard.txt` — trade 40's line should now show `mid 0.825 (frozen, market closed 2026-05-23)` or similar; the summary should show `open positions (1, 0 priced)` instead of `(1, 1 priced)`.
3. EQUITY identity still says `OK`.

## Coordinate with…

- **Handoff D** — after D normalizes the data so `active=0` always pairs with `closed_at IS NOT NULL`, the unpriceable check can simplify to just `t.get("market_active") != 1`. Either order is safe; D's data fix makes E's logic cleaner.
- **Handoff C** — once C lands, new positions on closed markets become impossible; E's display logic still matters for legacy positions and for the (very rare) market-closes-while-position-open transition.

## Subagent prompt suggestion

> Read `irene/handoffs/2026-05-26-E-wallet-honesty.md` end-to-end. Apply the 4 changes in `app/dashboard/pages/main.py`. Add the test case. Run `pytest app/tests/test_dashboard_pages.py`. Rebuild + restart. Verify per the doc by reading `dashboard.txt`. Do NOT touch any other handoff scope.

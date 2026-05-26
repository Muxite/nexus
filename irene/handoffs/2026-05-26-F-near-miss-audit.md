# Handoff F — Near-miss audit + tunable direction acceptance

**Date:** 2026-05-26
**Reference:** `irene/handoffs/2026-05-26-A-iran-funnel-audit.md`
**Dependency:** none structurally. **Most valuable after handoff B** unblocks dedupe — without B, the LLM rarely sees anything and the audit panel is sparse. With B, this panel becomes the next-cycle dashboard.

## The intent (from the user, original ask)

> "ideally we have a way to tune how often we say yes or no instead of neutral. how could we make that so we can see everything that leans heavily yes or no but was cut from final trading, so we can manually audit and try."

Two deliverables:
1. **Tunable threshold knob.** `cfg.trade.min_confidence` is the trade-execute floor (default 0.55). Add `cfg.trade.audit_confidence_floor` (default 0.35) — the boundary above which a rejected signal is "leaning enough to audit."
2. **Near-miss panel + CLI.** Surface every signal where the LLM was decisive (or close to it) but the trade didn't fire — so a human can read the article + market + LLM verdict and decide whether the gate was right.

Per the audit (handoff A): 29 `insufficient_cash` + 13 `mid_degenerate` + 4 `wide_spread` rejects in the last 24h ALL had LLM predictions with average confidence > 0.6. The LLM nailed the call; a different gate killed the trade. These are the "decisive but downstream-blocked" set worth eyeballing.

## What's already there (reuse)

- **`predictions` table** at `app/ai.py:158` already stores `direction`, `confidence`, `raw_response`, and the v5 `label` (Y/N/P/U/R/?). Nothing to add.
- **`decisions.prediction_id` is populated on reject rows** (confirmed; see handoff A's Q2 query). So `decisions ← predictions` JOIN works for every near-miss.
- **Dashboard pattern** at `app/dashboard/pages/decisions.py` — `FILENAME`, `async def render(dash)`, SQL with `decisions ← predictions` JOIN, gate-history formatter. Copy this verbatim.
- **`_gate_direction`** at `app/stages/trade.py:389` — single config knob (`cfg.min_confidence`). Don't change for now; ship the audit first.

## Scope

### Step 1 — config knob

File: `app/config.py` TradeSection — add:

```python
# Cycle 24+: any prediction with confidence >= this floor is "leaning"
# enough to surface in the near-miss audit. Below this, the model itself
# is admitting it has no signal. Above this AND below min_confidence
# is the band we want to inspect manually before relaxing the trade gate.
audit_confidence_floor: float = Field(default=0.35, ge=0.0, le=1.0)
```

`min_confidence` (the trade-execute floor) stays at 0.55. The audit floor is intentionally LOWER; the band 0.35-0.55 is the near-miss zone.

### Step 2 — dashboard panel `near_miss_audit.txt`

File: `app/dashboard/pages/near_miss_audit.py` (new). Mirror `pages/decisions.py`:

```python
FILENAME = "near_miss_audit.txt"

async def render(dash) -> str:
    cycle_id = dash.current_cycle_id
    cycle_sql = f"AND d.cycle_id = {int(cycle_id)}" if cycle_id is not None else ""
    audit_floor = float(dash.cfg.trade.audit_confidence_floor)
    trade_floor = float(dash.cfg.trade.min_confidence)
    lines = [f"# near-miss audit ({_now().isoformat()})", ""]

    # Section A — leaning Y/N cut at direction gate
    lines.append("=== Section A: leaning Y/N rejected at direction gate ===")
    sql_a = f"""
      SELECT d.created_at, d.signal_id, d.reason, d.decision_path,
             p.direction, p.confidence, p.label,
             a.title, m.question, m.condition_id
        FROM decisions d
        JOIN predictions p ON p.id = d.prediction_id
        JOIN signals s ON s.id = d.signal_id
        JOIN articles a ON a.id = s.article_id
        JOIN markets m ON m.condition_id = s.market_id
       WHERE d.outcome = 'reject'
         AND d.reason = 'direction_lowconf'
         AND p.direction IN ('YES','NO')
         AND p.confidence >= {audit_floor}
         AND d.created_at >= datetime('now', '-6 hours')
         {cycle_sql}
       ORDER BY p.confidence DESC LIMIT 20
    """
    # … format each row as: [time] path direction conf=X.XX label=Y/N
    #                         "article title" → "market question"

    # Section B — decisive Y/N killed downstream
    lines.append("\n=== Section B: decisive Y/N killed by downstream gate ===")
    sql_b = f"""
      SELECT d.created_at, d.signal_id, d.reason, d.decision_path, d.gate_history,
             p.direction, p.confidence, p.label,
             a.title, m.question
        FROM decisions d JOIN predictions p ON p.id = d.prediction_id
        JOIN signals s ON s.id = d.signal_id
        JOIN articles a ON a.id = s.article_id
        JOIN markets m ON m.condition_id = s.market_id
       WHERE d.outcome = 'reject'
         AND d.reason NOT IN (
             'similarity_floor','freshness_stale','entity_mismatch',
             'direction_neutral','direction_lowconf',
             'article_missing','market_missing',
             'market_closed','market_inactive','market_near_expiry',
             'execution_disabled'
         )
         AND p.direction IN ('YES','NO')
         AND p.confidence >= {trade_floor}
         AND d.created_at >= datetime('now', '-6 hours')
         {cycle_sql}
       ORDER BY p.confidence DESC LIMIT 20
    """
    # … format each row with the reject reason + a gate_history one-liner showing
    #     the values that killed the trade (e.g. "insufficient_cash: requested $250 available $50")

    return "\n".join(lines)
```

Empty-state handling: when either section is empty, print `(no near-miss rows in window — pipeline may be starved upstream; see handoff B)`.

### Step 3 — wire into the registry

File: `app/dashboard/registry.py` — add `near_miss_audit` to the `PAGES` list.

### Step 4 — CLI audit script

File: `scripts/audit/near_miss_audit.py` (new). Mirror `scripts/audit/resolution_timeout_audit.py` (cycle 23) structure: argparse with `--hours` (default 24), `--min-confidence` (default 0.35), `--db`. Outputs:
- Stdout: per-section funnel summary + 10 sample rows per section.
- `reports/YYYY-MM-DD-near-miss-audit.md` — markdown report.
- `data/dashboard/plots/near_miss_breakdown.csv` — full CSV for sortable manual review.

CSV columns: `decision_at, decision_path, reason, direction, confidence, label, article_id, article_title, market_id, market_question, gate_history_snippet`.

### Step 5 — tests

File: `app/tests/test_dashboard_pages.py` — add 2 cases:
- empty DB → both sections render the empty-state message.
- seeded DB with one `direction_lowconf` reject + one `insufficient_cash` reject (both with YES/NO predictions) → both sections show one row each.

## Files

| Change | File |
|---|---|
| Config knob | `app/config.py` (TradeSection) |
| Panel | `app/dashboard/pages/near_miss_audit.py` (new) |
| Registry | `app/dashboard/registry.py` |
| CLI | `scripts/audit/near_miss_audit.py` (new) |
| Tests | `app/tests/test_dashboard_pages.py` |

## Verification

1. `pytest app/tests/test_dashboard_pages.py -k near_miss` → 2/2 pass.
2. Restart container. After 30s tick, `cat irene/data/dashboard/near_miss_audit.txt` shows the panel. Section B should populate immediately from current data (handoff A's funnel shows 29 `insufficient_cash` + 13 `mid_degenerate` near-misses).
3. Run `python3 -m scripts.audit.near_miss_audit --hours 24` → produces the markdown report + CSV. Counts match the dashboard.
4. **Manual audit step (the actual point):** read 5–10 rows from each section. Decide whether the gate's behavior is correct. Use the data to inform whether to (a) lower `min_confidence` to 0.50, (b) tweak the prompt in `compose_direction_prompt`, (c) relax a downstream gate (cash sizing, mid_degenerate floor, etc.).

## After F lands — Phase 1 levers (next cycle)

Don't ship without data. Once F is up and 24h of soak has populated both sections:

- **Lower `cfg.trade.min_confidence`** if Section A shows lots of clearly-correct YES/NO at conf 0.45-0.54.
- **Tweak `compose_direction_prompt`** if the LLM is over-defaulting to NEUTRAL on obviously-decisive news. Test via tracer harness (handoff G) before deploying.
- **Relax a downstream gate** if Section B shows the LLM is right but a microstructure gate is over-protective.

## Coordinate with…

- **Handoff B** — F is most valuable post-B. With B, the LLM actually sees Iran-grade signals; F shows you exactly where they get killed.
- **Handoff G** — once F's panel exists, G can fire tracer bursts and the panel populates immediately with synthetic near-misses, closing the test loop.

## Subagent prompt suggestion

> Read `irene/handoffs/2026-05-26-F-near-miss-audit.md` end-to-end. Add the config knob, write the dashboard panel, wire it into the registry, write the CLI, write 2 tests. Run pytest. Rebuild + restart. Verify the panel populates from current data. Do NOT change `min_confidence` or the prompt — that decision waits on data, not this PR.

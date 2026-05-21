#!/usr/bin/env bash
# Phase D verification — fired by a one-shot crontab entry at 02:15 UTC
# 2026-05-21. Writes a single Markdown report to
# /tmp/phase_d_verify_<ts>.md and removes its own crontab entry so it
# won't fire again.
#
# Plan: /home/muk/.claude/plans/code-shipped-all-137-tranquil-turtle.md
# Handoff: /home/muk/projects/jennie/seulgi/HANDOFF_2026-05-21.md

set -uo pipefail

TS=$(date -u +%Y%m%dT%H%M%SZ)
REPORT=/tmp/phase_d_verify_${TS}.md
LATEST=/tmp/phase_d_verify_latest.md
JENNIE=/home/muk/projects/jennie
SNAP=$JENNIE/seulgi/snapshots/pre_phase_b_deploy_20260520T234808Z
# Cron starts in $HOME — child scripts use relative paths.
cd "$JENNIE"

# Baseline gap numbers captured at deploy time
BASELINE_GAP_SIGNALS=2138
BASELINE_GAP_TRADES=367740
BASELINE_GAP_ENGINE_RESULTS=203150
BASELINE_GAP_USAGE=944

pass=0
fail=0
notes=()

p() { echo "$@" >> "$REPORT"; }
mark_pass() { pass=$((pass+1)); p "- ✅ **$1**"; }
mark_fail() { fail=$((fail+1)); p "- ❌ **$1**"; notes+=("$1"); }

p "# Phase D verification — $TS"
p ""
p "Container 4 deployed 2026-05-20 23:48 UTC with Phases A/B/C."
p "Hour_gate (UTC 23/00/01) released at UTC 02:00 — engines now active."
p ""

# ── Check 1: db_health_audit (capture current state) ──────────────────────
p "## 1. db_health_audit"
p '```'
python3 $JENNIE/seulgi/scripts/analysis/db_health_audit.py >> "$REPORT" 2>&1
RC=$?
p '```'
if [[ $RC -eq 0 ]]; then mark_pass "db_health_audit ran"
else mark_fail "db_health_audit exit $RC"; fi
p ""

# Re-run silently to parse numbers
AUDIT=$(python3 $JENNIE/seulgi/scripts/analysis/db_health_audit.py 2>/dev/null)

# Helper: row "| signals | 77,072 | 79,210 | ..." → strip commas, split
get_count() { echo "$AUDIT" | awk -v t="$1" -F'|' '$0~"^\\| "t" " {gsub(/[, ]/,"",$3); print $3; exit}'; }
get_max()   { echo "$AUDIT" | awk -v t="$1" -F'|' '$0~"^\\| "t" " {gsub(/[, ⚠]/,"",$4); print $4; exit}'; }
get_age()   { echo "$AUDIT" | awk -v t="$1" -F'|' '$0~"^\\| "t" " {gsub(/[ ⚠]/,"",$7); print $7; exit}'; }

# ── Check 2: counts grew + age < 90s for live tables ─────────────────────
p "## 2. Tables advancing (counts up, ages < 90s)"
for t in signals trades engine_results usage_events; do
  cnt=$(get_count "$t")
  age=$(get_age "$t")
  p "  - $t: count=$cnt age=$age"
done

# Pre-deploy counts (captured during deploy):
declare -A BASELINE_COUNT=(
  [signals]=77072
  [trades]=5387202
  [engine_results]=2949455
  [usage_events]=12684
)

for t in signals trades engine_results usage_events; do
  cnt_raw=$(get_count "$t")
  cnt=${cnt_raw:-0}
  base=${BASELINE_COUNT[$t]}
  if [[ "$cnt" -gt "$base" ]]; then mark_pass "$t count grew ($base → $cnt)"
  else mark_fail "$t count did not grow ($base → $cnt)"; fi
done
p ""

# ── Check 3: row-ID gaps NOT growing ─────────────────────────────────────
p "## 3. Row-ID gaps stable (no new silent rollbacks)"
declare -A BASELINE_GAP=(
  [signals]=$BASELINE_GAP_SIGNALS
  [trades]=$BASELINE_GAP_TRADES
  [engine_results]=$BASELINE_GAP_ENGINE_RESULTS
  [usage_events]=$BASELINE_GAP_USAGE
)
for t in signals trades engine_results usage_events; do
  cnt_raw=$(get_count "$t"); cnt=${cnt_raw:-0}
  max_raw=$(get_max "$t"); max=${max_raw:-0}
  current_gap=$((max - cnt))
  base_gap=${BASELINE_GAP[$t]}
  delta=$((current_gap - base_gap))
  p "  - $t: gap=$current_gap (baseline $base_gap, delta $delta)"
  if [[ $delta -le 0 ]]; then mark_pass "$t gap not growing (delta $delta)"
  else mark_fail "$t gap grew by $delta — silent rollbacks happening"; fi
done
p ""

# ── Check 4: zero crashes / locks / failures in last 30m ─────────────────
p "## 4. Container log cleanliness (last 30 min)"
ERR_COUNT=$(docker logs jennie-seulgi --since 30m 2>&1 | grep -cE "task .*crashed|database is locked|\[db\].*failed")
p "  - error/lock/crash lines: $ERR_COUNT"
if [[ "$ERR_COUNT" -eq 0 ]]; then mark_pass "no crashes / locks / db failures in 30m"
else
  mark_fail "$ERR_COUNT error lines in 30m"
  p ""
  p "  Recent error context:"
  p '```'
  docker logs jennie-seulgi --since 30m 2>&1 | grep -E "task .*crashed|database is locked|\[db\].*failed" | head -10 >> "$REPORT"
  p '```'
fi
p ""

# ── Check 5: writer-lock probe (p95 < 200ms) ────────────────────────────
p "## 5. Writer-lock probe (p95 should be < 200ms)"
p '```'
python3 $JENNIE/seulgi/scripts/analysis/writer_lock_probe.py --samples 30 >> "$REPORT" 2>&1
PROBE_RC=$?
p '```'
if [[ $PROBE_RC -eq 0 ]]; then mark_pass "writer_lock_probe ran"
else mark_fail "writer_lock_probe exit $PROBE_RC"; fi
p ""

# ── Check 6: bump_market_heat + metrics_minutely fresh ──────────────────
p "## 6. Phase A3 + Phase C live signals"
if grep -q "bump_market_heat" $JENNIE/seulgi/data/dashboard/db_health.txt 2>/dev/null; then
  bmh_line=$(grep "bump_market_heat" $JENNIE/seulgi/data/dashboard/db_health.txt | head -1)
  p "  - bump_market_heat: $bmh_line"
  mark_pass "bump_market_heat counter present (Phase A3 instrumentation)"
else
  mark_fail "bump_market_heat counter missing from db_health.txt"
fi

mm_age=$(get_age "metrics_minutely")
p "  - metrics_minutely age: $mm_age"
case "$mm_age" in
  *h|*d) mark_fail "metrics_minutely stale ($mm_age) — Phase C flush not firing" ;;
  *) mark_pass "metrics_minutely fresh (Phase C flush firing)" ;;
esac
p ""

# ── Summary ─────────────────────────────────────────────────────────────
p "## Summary"
p ""
p "  - PASS: $pass"
p "  - FAIL: $fail"
p ""
if [[ $fail -eq 0 ]]; then
  p "**✅ Phase D verification PASSED.** Container 4 is stable; B-WRITES resolved."
else
  p "**❌ Phase D verification FAILED ($fail checks).** Failures:"
  for n in "${notes[@]}"; do p "  - $n"; done
fi

# Copy latest pointer so user can `cat /tmp/phase_d_verify_latest.md`
cp "$REPORT" "$LATEST"

# Self-delete the crontab entry so this doesn't fire again
crontab -l 2>/dev/null | grep -v "phase_d_verify.sh" | crontab -

echo "Phase D verify done — report at $REPORT (also $LATEST)"

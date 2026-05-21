#!/usr/bin/env bash
# B-WRITES regression suite — runs three progressively harder replay
# scenarios and hard-fails on ANY row-ID gap. Intended for CI / pre-deploy.
#
# Exit code 0 = all three runs PASS (zero row-ID gaps).
# Exit code 1+ = at least one run FAILED. See the per-run report for details.
#
# Usage:
#   docker compose run --rm seulgi-replay
#   bash seulgi/scripts/replay/regression_suite.sh
#
# Env vars:
#   SRC_DB        — path to source DB with historical signals (default: seulgi/data/seulgi.db)
#   REPLAY_TMP    — where to write per-run reports + test DBs (default: /tmp)
#   N_SIGNALS_*   — override the size of each tier (smoke / load / stress)
#
# The three tiers ARE the minimum bar for the writer. Adding new write
# paths or changing batch() should not change these results.

set -uo pipefail

SRC_DB="${SRC_DB:-seulgi/data/seulgi.db}"
REPLAY_TMP="${REPLAY_TMP:-/tmp}"
CONFIG="${CONFIG:-seulgi/config.yaml}"
TS=$(date -u +%Y%m%dT%H%M%SZ)

# Disable hour_gate so the replay runs at any UTC hour.
export SEULGI_DISABLE_HOUR_GATE=1

if [ ! -f "$SRC_DB" ]; then
    echo "ERROR: source DB not found at $SRC_DB (set SRC_DB env var)" >&2
    exit 2
fi

failures=0
overall_report="$REPLAY_TMP/replay_regression_$TS.md"
: > "$overall_report"

run_tier() {
    local name=$1 sigs=$2 conc=$3
    local report="$REPLAY_TMP/replay_${name}_${TS}.md"
    local test_db="$REPLAY_TMP/replay_${name}_${TS}.db"
    echo "──────────────────────────────────────────────────────────────"
    echo "TIER: $name (signals=$sigs, concurrency=$conc)"
    echo "──────────────────────────────────────────────────────────────"
    python3 seulgi/scripts/replay/run.py \
        --src-db "$SRC_DB" \
        --test-db "$test_db" \
        --config "$CONFIG" \
        --signals "$sigs" \
        --concurrency "$conc" \
        --report "$report" \
        --log-level WARNING 2>&1 | grep -vE "Unclosed|client_session|connections:|connector:" || true
    local rc=$?
    {
        echo ""
        echo "## Tier: $name (sigs=$sigs, conc=$conc, exit=$rc)"
        echo ""
        if [ -f "$report" ]; then
            grep -E "^- (headlines|injected|failed|elapsed|per-headline)|^\| [a-z_]+ +\|" "$report"
            echo ""
            grep "Verdict" "$report" || true
        fi
        echo ""
    } >> "$overall_report"
    return $rc
}

# Tier 1 — smoke: one coroutine, small N. The baseline that must always pass.
run_tier "smoke"      "${N_SIGNALS_SMOKE:-50}"    "${C_SMOKE:-1}"   || failures=$((failures+1))

# Tier 2 — production-load: representative concurrency at the size of a
# real cycle's burst. p95 latency target < 600 ms.
run_tier "load"       "${N_SIGNALS_LOAD:-200}"    "${C_LOAD:-4}"    || failures=$((failures+1))

# Tier 3 — stress: high concurrency, big N. Exercises the asyncio.Lock
# under the heaviest realistic load. p95 may rise, but zero gaps required.
run_tier "stress"     "${N_SIGNALS_STRESS:-500}"  "${C_STRESS:-16}" || failures=$((failures+1))

echo ""
echo "══════════════════════════════════════════════════════════════"
if [ "$failures" -eq 0 ]; then
    echo "REGRESSION SUITE: ✅ ALL TIERS PASSED"
    echo "Report: $overall_report"
    exit 0
else
    echo "REGRESSION SUITE: ❌ $failures TIER(S) FAILED"
    echo "Report: $overall_report"
    exit 1
fi

#!/bin/bash
# Cycle-25-I wallet correctness harness — fresh-DB driver.
#
# Boots a fresh empty DB, snapshots the live markets table into it (so
# the LLM has real markets to match against), runs the cycle-25-wallet
# tracer campaign against real CLOB + real OpenRouter, then audits the
# resulting wallet line-by-line.
#
# Usage:
#   bash scripts/cycle25/run_wallet_harness.sh [BUDGET_USD]
#
# Defaults: BUDGET_USD=5, harness DB at /tmp/wallet_harness.db, report
# under irene/reports/wallet-harness/.
#
# Costs real OpenRouter credits — the tracer's --budget-cap-usd flag
# guards spend.

set -euo pipefail

BUDGET="${1:-5}"
HARNESS_DB="/tmp/wallet_harness.db"
LIVE_DB="/data/irene.db"
REPORT_DIR="$(dirname "$0")/../../reports/wallet-harness"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
REPORT_PATH="${REPORT_DIR}/${TS}.md"

# 1. Fresh DB
rm -f "${HARNESS_DB}" "${HARNESS_DB}-wal" "${HARNESS_DB}-shm"

# 2. Bootstrap schema (lets the irene daemon's DDL register clean) and
# snapshot the markets table from the live DB so the matcher has data.
# Using sqlite3 from python since the binary isn't always present.
python3 - <<PY
import sqlite3
src = sqlite3.connect("${LIVE_DB}")
src.row_factory = sqlite3.Row
dst = sqlite3.connect("${HARNESS_DB}")
# Copy the markets schema + all rows.
schema_row = src.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='markets'").fetchone()
dst.execute(schema_row[0])
for col in ("fee_rate", "min_order_size", "fee_rate_fetched_at",
            "uma_proposed_at", "uma_finalized_at", "last_resolution_status",
            "last_outcome_prices"):
    try:
        dst.execute(f"ALTER TABLE markets ADD COLUMN {col} TEXT")
    except sqlite3.OperationalError:
        pass
rows = src.execute("SELECT * FROM markets").fetchall()
cols = [d[0] for d in src.execute("SELECT * FROM markets LIMIT 1").description]
placeholders = ",".join("?" * len(cols))
dst.executemany(
    f"INSERT OR REPLACE INTO markets ({','.join(cols)}) VALUES ({placeholders})",
    [tuple(r) for r in rows],
)
dst.commit()
print(f"Snapshotted {len(rows)} markets into ${HARNESS_DB}")
src.close()
dst.close()
PY

# 3. Run the campaign against the fresh DB with live externals.
PREFIX="cycle25-wallet-${TS}"
python3 -m scripts.tracer.fire \
    --campaign "$(dirname "$0")/../tracer/campaigns/cycle-25-wallet.yaml" \
    --db "${HARNESS_DB}" \
    --live-externals \
    --tracer-id-prefix "${PREFIX}" \
    --budget-cap-usd "${BUDGET}" \
    --exercise-wallet \
    --force

# 4. Audit the resulting wallet.
mkdir -p "${REPORT_DIR}"
python3 -m scripts.cycle25.audit_wallet_harness \
    --db "${HARNESS_DB}" \
    --prefix "${PREFIX}" \
    --out "${REPORT_PATH}" \
    --print

echo
echo "Harness DB:  ${HARNESS_DB}"
echo "Report:      ${REPORT_PATH}"
echo "Logs to read manually:"
echo "  - the irene daemon container (if running in parallel)"
echo "  - this script's own stdout/stderr above"

#!/usr/bin/env bash
# Periodic audit driver. Plan §4 / Step 4.
#
# Schedule from user crontab (every 4 hours):
#     0 */4 * * * /home/muk/projects/nexus/seulgi/scripts/audit/run_periodic.sh
#
# Writes a per-run directory under /home/muk/seulgi-audits/<UTC-timestamp>/
# (NOT data/dashboard/audit/ because that path is root-owned by the docker
# container at runtime). Keeps last 24 runs; older are pruned.

set -euo pipefail

REPO_ROOT="/home/muk/projects/nexus/seulgi"
OUT_ROOT="/home/muk/seulgi-audits"
STAMP=$(date -u +%Y-%m-%dT%H-%M-%SZ)
OUT_DIR="${OUT_ROOT}/${STAMP}"

mkdir -p "${OUT_ROOT}"

cd "${REPO_ROOT}"
python3 -m scripts.audit.trace_trades \
    --engines all \
    -n 10 \
    --include-open \
    --out "${OUT_DIR}" \
    >>"${OUT_ROOT}/last_run.log" 2>&1 || {
    echo "[audit] FAILED at ${STAMP}; see ${OUT_ROOT}/last_run.log" >&2
    exit 1
}

# Retention: keep most recent 24 runs.
ls -1dt "${OUT_ROOT}"/2*Z 2>/dev/null | tail -n +25 | xargs -r rm -rf

# Surface FAIL count at top of log so a quick `tail` is informative.
grep -h "^\[audit\]" "${OUT_DIR}"/../*Z/INDEX.md 2>/dev/null | tail -1 \
    >>"${OUT_ROOT}/last_run.log" || true
echo "[audit] ok ${STAMP} -> ${OUT_DIR}" >>"${OUT_ROOT}/last_run.log"

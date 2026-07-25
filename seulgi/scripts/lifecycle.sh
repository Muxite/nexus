#!/usr/bin/env bash
# seulgi container lifecycle helper — start / stop / status.
#
# Use this instead of raw `docker compose` so we standardise on:
#   - canonical container name: `seulgi`  (NOT `nexus-seulgi`)
#   - canonical compose file:   seulgi/docker-compose.yml
#   - pre-stop snapshot:        scripts.snapshot.snapshot save
#
# Reason: 2026-05-21 dual-container hazard (two containers writing the
# same data/seulgi.db + data/news_chroma corrupted the HNSW index and
# cost 30 min of debug). Documented in
# memory/project_dual_container.md.
#
# Usage:
#   bash seulgi/scripts/lifecycle.sh start
#   bash seulgi/scripts/lifecycle.sh stop
#   bash seulgi/scripts/lifecycle.sh status

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE="$REPO_ROOT/seulgi/docker-compose.yml"
SEULGI_DIR="$REPO_ROOT/seulgi"
DB="$SEULGI_DIR/data/seulgi.db"
HEALTH="$SEULGI_DIR/data/dashboard/db_health.txt"

assert_no_dual() {
    if docker ps -a --format '{{.Names}}' | grep -qE '^nexus-seulgi$'; then
        echo "ERROR: nexus-seulgi container exists. Run 'docker rm nexus-seulgi' first." >&2
        echo "Dual-container hazard: memory/project_dual_container.md" >&2
        exit 1
    fi
}

cmd_start() {
    assert_no_dual
    echo "→ building seulgi-seulgi:latest…"
    docker compose -f "$COMPOSE" build seulgi
    echo "→ starting container 'seulgi'…"
    docker compose -f "$COMPOSE" up -d --force-recreate seulgi
    echo "→ waiting for first insert_signal to land (max 3 min)…"
    deadline=$(($(date +%s) + 180))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        if [ -f "$HEALTH" ] && grep -q "insert_signal" "$HEALTH" 2>/dev/null; then
            echo "→ ready ✓"
            cmd_status
            return 0
        fi
        sleep 5
    done
    echo "WARNING: 3-minute deadline reached without seeing insert_signal in db_health.txt." >&2
    cmd_status
    return 1
}

cmd_stop() {
    if docker ps --format '{{.Names}}' | grep -qE '^seulgi$'; then
        echo "→ snapshotting before stop…"
        cd "$SEULGI_DIR" && \
            python3 -m scripts.snapshot.snapshot save \
                "pre_stop_$(date -u +%Y%m%dT%H%M%SZ)" 2>&1 | tail -5 || \
            echo "WARNING: snapshot failed (continuing with stop)"
    fi
    echo "→ stopping container 'seulgi'…"
    docker compose -f "$COMPOSE" stop seulgi
    echo "→ stopped ✓"
}

cmd_status() {
    echo "=== containers ==="
    docker ps -a --filter "name=seulgi" --format "table {{.Names}}\t{{.Status}}\t{{.Image}}"
    echo ""
    if [ -f "$HEALTH" ]; then
        echo "=== db_health.txt (last refresh) ==="
        head -5 "$HEALTH" 2>/dev/null | tail -3
        echo ""
        echo "=== write counters ==="
        grep -A 15 "Write counters" "$HEALTH" 2>/dev/null || echo "(no counters yet)"
    fi
    echo ""
    if [ -f "$DB" ] && command -v python3 >/dev/null 2>&1; then
        echo "=== row counts (last 1h) ==="
        python3 - <<PY
import sqlite3
c = sqlite3.connect("$DB")
for t, col in (("trades","decided_at"),("engine_results","decided_at"),
               ("signals","detected_at"),("usage_events","ts")):
    n = c.execute(f"SELECT COUNT(*) FROM {t} WHERE {col} >= datetime('now','-1 hours')").fetchone()[0]
    print(f"  {t}: {n}")
PY
    fi
    echo ""
    if [ -n "${OPENROUTER_API_KEY:-}" ]; then
        echo "=== OpenRouter budget ==="
        cd "$SEULGI_DIR" && \
            OPENROUTER_API_KEY="$OPENROUTER_API_KEY" \
            python3 -m scripts.analysis.openrouter_budget 2>&1 | head -8 || \
            echo "(budget query failed)"
    else
        echo "(OPENROUTER_API_KEY not set — skipping budget tile)"
    fi
}

case "${1:-status}" in
    start)  cmd_start ;;
    stop)   cmd_stop ;;
    status) cmd_status ;;
    *)
        echo "usage: $0 {start|stop|status}" >&2
        exit 1
        ;;
esac

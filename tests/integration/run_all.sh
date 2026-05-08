#!/usr/bin/env bash
# All-in-one integration test runner.
#
# Usage:
#   bash tests/integration/run_all.sh          # run all tests
#   bash tests/integration/run_all.sh offline  # run only offline tests
#   bash tests/integration/run_all.sh online   # run only online tests
#
# The script starts the docker-compose test stack, runs all tests, and
# tears everything down regardless of success or failure.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
source "$ROOT/tests/integration/lib.sh"

FILTER="${1:-all}"
FAILED=()
PASSED=()

# ═══════════════════════════════════════════════════════════════════════════
# Docker-compose lifecycle
# ═══════════════════════════════════════════════════════════════════════════
start_stack() {
    echo "══════════════════════════════════════════════════════════════"
    echo "  Starting docker-compose test stack"
    echo "══════════════════════════════════════════════════════════════"
    docker compose -f docker-compose.test.yml up -d --wait
}

stop_stack() {
    echo ""
    echo "══════════════════════════════════════════════════════════════"
    echo "  Tearing down docker-compose test stack"
    echo "══════════════════════════════════════════════════════════════"
    docker compose -f docker-compose.test.yml down -v 2>/dev/null || true
}

trap stop_stack EXIT

# ═══════════════════════════════════════════════════════════════════════════
# Cleanup any leftover replication state between tests
# ═══════════════════════════════════════════════════════════════════════════
cleanup_between_tests() {
    echo "--- cleaning up replication state between tests ---"
    psql -v ON_ERROR_STOP=0 -X -A -t \
        -h 127.0.0.1 -p 55432 -U migrator -d appdb -c "
DO \$\$
DECLARE r record;
BEGIN
    FOR r IN SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'pg_dbmigrator%' LOOP
        EXECUTE format('SELECT pg_drop_replication_slot(%L)', r.slot_name);
    END LOOP;
END
\$\$;" 2>/dev/null || true

    psql -v ON_ERROR_STOP=0 -X -A -t \
        -h 127.0.0.1 -p 55433 -U migrator -d appdb -c "
DO \$\$
DECLARE r record;
BEGIN
    FOR r IN SELECT subname FROM pg_subscription WHERE subname LIKE 'pg_dbmigrator%' LOOP
        EXECUTE format('ALTER SUBSCRIPTION %I DISABLE', r.subname);
        EXECUTE format('ALTER SUBSCRIPTION %I SET (slot_name = NONE)', r.subname);
        EXECUTE format('DROP SUBSCRIPTION %I', r.subname);
    END LOOP;
END
\$\$;" 2>/dev/null || true

    psql -v ON_ERROR_STOP=0 -X -A -t \
        -h 127.0.0.1 -p 55433 -U migrator -d appdb \
        -c "DROP SCHEMA IF EXISTS app CASCADE;" 2>/dev/null || true
}

# ═══════════════════════════════════════════════════════════════════════════
# Test runner
# ═══════════════════════════════════════════════════════════════════════════
run_test() {
    local script="$1"
    local name
    name="$(basename "$script" .sh)"

    echo ""
    echo "══════════════════════════════════════════════════════════════"
    echo "  RUNNING: $name"
    echo "══════════════════════════════════════════════════════════════"

    cleanup_between_tests

    if bash "$script"; then
        PASSED+=("$name")
        echo "  --> $name: OK"
    else
        FAILED+=("$name")
        echo "  --> $name: FAILED"
        echo ""
        echo "  Docker logs:"
        docker compose -f docker-compose.test.yml logs --tail=50 2>/dev/null || true
    fi
}

# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════
echo "══════════════════════════════════════════════════════════════"
echo "  Building workspace"
echo "══════════════════════════════════════════════════════════════"
cargo build --workspace --all-targets --quiet

start_stack

OFFLINE_TESTS=(
    tests/integration/run_offline.sh
    tests/integration/run_offline_split_sections.sh
    tests/integration/run_offline_resume.sh
    tests/integration/run_offline_sigint_cancel.sh
    tests/integration/run_offline_analyze.sh
)

ONLINE_TESTS=(
    tests/integration/run_online.sh
    tests/integration/run_online_updates.sh
    tests/integration/run_online_sustained.sh
    tests/integration/run_online_lag_cadence.sh
    tests/integration/run_online_cancel_resume.sh
    tests/integration/run_online_multi_resume_sustained.sh
    tests/integration/run_online_sequence_sync.sh
    tests/integration/run_online_auto_pub_lifecycle.sh
    tests/integration/run_online_keep_slot.sh
)

case "$FILTER" in
    offline)
        for t in "${OFFLINE_TESTS[@]}"; do run_test "$t"; done
        ;;
    online)
        for t in "${ONLINE_TESTS[@]}"; do run_test "$t"; done
        ;;
    all|*)
        for t in "${OFFLINE_TESTS[@]}"; do run_test "$t"; done
        for t in "${ONLINE_TESTS[@]}"; do run_test "$t"; done
        ;;
esac

# ═══════════════════════════════════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo "══════════════════════════════════════════════════════════════"
echo "  RESULTS"
echo "══════════════════════════════════════════════════════════════"
echo "  Passed: ${#PASSED[@]}"
for p in "${PASSED[@]}"; do echo "    + $p"; done
if (( ${#FAILED[@]} > 0 )); then
    echo "  Failed: ${#FAILED[@]}"
    for f in "${FAILED[@]}"; do echo "    - $f"; done
    echo ""
    echo "  SOME TESTS FAILED"
    exit 1
else
    echo ""
    echo "  ALL TESTS PASSED"
fi

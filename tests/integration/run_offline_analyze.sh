#!/usr/bin/env bash
# Integration test: pre-dump VACUUM ANALYZE on source + post-restore ANALYZE
# on target.
#
# Verifies:
#   1. Default (ANALYZE enabled): target has pg_statistic entries after
#      migration (proving ANALYZE ran).
#   2. Log shows "VACUUM ANALYZE" and "ANALYZE" progress events.
#   3. With PG_DBMIGRATOR_SKIP_ANALYZE=1 and PG_DBMIGRATOR_SKIP_SOURCE_VACUUM=1:
#      the log does NOT contain those events, and the target has empty
#      pg_statistic for the restored table.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
source "$ROOT/tests/integration/lib.sh"

LOG_FILE="$(mktemp -t pg_dbmigrator_analyze.XXXXXX.log)"
echo "==> log file: $LOG_FILE"

wait_for_pg "$SOURCE_URL" "source"
wait_for_pg "$TARGET_URL" "target"

# ═══════════════════════════════════════════════════════════════════════════
# Test 1: ANALYZE + VACUUM enabled (default behaviour)
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo "══════════════════════════════════════════════════════════════"
echo "  Test 1: ANALYZE + VACUUM enabled (default)"
echo "══════════════════════════════════════════════════════════════"

seed_source
reset_target_schema

PG_DBMIGRATOR_SOURCE="$SOURCE_URL" \
PG_DBMIGRATOR_TARGET="$TARGET_URL" \
NO_COLOR=1 \
RUST_LOG="info,pg_dbmigrator=info" \
    cargo run --quiet -p offline_migration_example >"$LOG_FILE" 2>&1

# Verify data landed correctly
assert_data_equal 500

# Verify log contains the VACUUM ANALYZE and ANALYZE stages
if ! grep -qi "VACUUM ANALYZE" "$LOG_FILE"; then
    echo "FAIL: log does not contain 'VACUUM ANALYZE' — source vacuum did not run" >&2
    cat "$LOG_FILE" >&2
    exit 1
fi
echo "==> OK: log contains VACUUM ANALYZE on source"

if ! grep -qi "running ANALYZE on target" "$LOG_FILE"; then
    echo "FAIL: log does not contain 'running ANALYZE on target'" >&2
    cat "$LOG_FILE" >&2
    exit 1
fi
echo "==> OK: log contains ANALYZE on target"

# Verify the target has pg_statistic entries for app.widgets
# (pg_statistic is populated by ANALYZE; if it ran, there will be rows)
stat_count=$("${PSQL_BASE[@]}" "$TARGET_URL" -c "
    SELECT count(*)
    FROM pg_statistic s
    JOIN pg_class c ON c.oid = s.starelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'app' AND c.relname = 'widgets'
" | tr -d '[:space:]')

echo "==> target pg_statistic entries for app.widgets: $stat_count"
if [[ "$stat_count" -lt 1 ]]; then
    echo "FAIL: expected pg_statistic entries for app.widgets after ANALYZE, got $stat_count" >&2
    exit 1
fi
echo "==> OK: target has pg_statistic entries (ANALYZE ran)"

echo "PASS: Test 1 — ANALYZE + VACUUM ran and target has planner statistics"

# ═══════════════════════════════════════════════════════════════════════════
# Test 2: ANALYZE + VACUUM SKIPPED
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo "══════════════════════════════════════════════════════════════"
echo "  Test 2: ANALYZE + VACUUM skipped (--skip-analyze --skip-source-vacuum)"
echo "══════════════════════════════════════════════════════════════"

LOG_FILE2="$(mktemp -t pg_dbmigrator_analyze_skip.XXXXXX.log)"
echo "==> log file: $LOG_FILE2"

seed_source
reset_target_schema

PG_DBMIGRATOR_SOURCE="$SOURCE_URL" \
PG_DBMIGRATOR_TARGET="$TARGET_URL" \
PG_DBMIGRATOR_SKIP_ANALYZE=1 \
PG_DBMIGRATOR_SKIP_SOURCE_VACUUM=1 \
NO_COLOR=1 \
RUST_LOG="info,pg_dbmigrator=info" \
    cargo run --quiet -p offline_migration_example >"$LOG_FILE2" 2>&1

# Verify data still lands correctly (skip only affects stats, not data)
assert_data_equal 500

# Verify log does NOT contain VACUUM ANALYZE or ANALYZE on target
if grep -qi "VACUUM ANALYZE" "$LOG_FILE2"; then
    echo "FAIL: log should NOT contain 'VACUUM ANALYZE' when skip flag is set" >&2
    grep -i "VACUUM" "$LOG_FILE2" >&2
    exit 1
fi
echo "==> OK: log does NOT contain VACUUM ANALYZE (skipped as expected)"

if grep -qi "running ANALYZE on target" "$LOG_FILE2"; then
    echo "FAIL: log should NOT contain 'running ANALYZE on target' when skip flag is set" >&2
    grep -i "ANALYZE" "$LOG_FILE2" >&2
    exit 1
fi
echo "==> OK: log does NOT contain ANALYZE on target (skipped as expected)"

# Verify the target has NO pg_statistic entries for app.widgets
# (since ANALYZE was skipped)
stat_count2=$("${PSQL_BASE[@]}" "$TARGET_URL" -c "
    SELECT count(*)
    FROM pg_statistic s
    JOIN pg_class c ON c.oid = s.starelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'app' AND c.relname = 'widgets'
" | tr -d '[:space:]')

echo "==> target pg_statistic entries for app.widgets (skip mode): $stat_count2"
if [[ "$stat_count2" -ne 0 ]]; then
    echo "FAIL: expected 0 pg_statistic entries when ANALYZE is skipped, got $stat_count2" >&2
    exit 1
fi
echo "==> OK: target has 0 pg_statistic entries (ANALYZE correctly skipped)"

echo ""
echo "PASS: offline analyze — both enabled/disabled paths verified"

# ═══════════════════════════════════════════════════════════════════════════
# Test 3: ANALYZE + VACUUM with schema filter
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo "══════════════════════════════════════════════════════════════"
echo "  Test 3: ANALYZE + VACUUM with schema filter (--schema app)"
echo "══════════════════════════════════════════════════════════════"

LOG_FILE3="$(mktemp -t pg_dbmigrator_analyze_schema.XXXXXX.log)"
echo "==> log file: $LOG_FILE3"

seed_source
reset_target_schema

PG_DBMIGRATOR_SOURCE="$SOURCE_URL" \
PG_DBMIGRATOR_TARGET="$TARGET_URL" \
PG_DBMIGRATOR_SCHEMAS="app" \
NO_COLOR=1 \
RUST_LOG="info,pg_dbmigrator=info" \
    cargo run --quiet -p offline_migration_example >"$LOG_FILE3" 2>&1

# Verify data landed correctly
assert_data_equal 500

# Verify log contains the VACUUM ANALYZE and ANALYZE stages
if ! grep -qi "VACUUM ANALYZE" "$LOG_FILE3"; then
    echo "FAIL: log does not contain 'VACUUM ANALYZE' with schema filter" >&2
    cat "$LOG_FILE3" >&2
    exit 1
fi
echo "==> OK: log contains VACUUM ANALYZE on source (schema-filtered)"

if ! grep -qi "running ANALYZE on target" "$LOG_FILE3"; then
    echo "FAIL: log does not contain 'running ANALYZE on target' with schema filter" >&2
    cat "$LOG_FILE3" >&2
    exit 1
fi
echo "==> OK: log contains ANALYZE on target (schema-filtered)"

echo "PASS: Test 3 — schema-filtered ANALYZE + VACUUM ran successfully"

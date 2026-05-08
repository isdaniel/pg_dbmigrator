#!/usr/bin/env bash
# End-to-end test: keep-slot flag — verify the replication slot survives
# cutover when PG_DBMIGRATOR_KEEP_SLOT=1 is set.
#
# Also verifies that when a pre-existing publication was NOT auto-created,
# it is NOT dropped after cutover (only auto-created ones are cleaned up).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
source "$ROOT/tests/integration/lib.sh"

LOG_FILE="$(mktemp -t pg_dbmigrator_keep_slot.XXXXXX.log)"
echo "==> log file: $LOG_FILE"

trap 'stop_migrator' EXIT

wait_for_pg "$SOURCE_URL" "source"
wait_for_pg "$TARGET_URL" "target"

# ── Setup: seed source (publication created by seed.sql) ────────────────
setup_online_test

# Verify publication exists (pre-created by seed.sql — not auto-created).
pub_count=$("${PSQL_BASE[@]}" "$SOURCE_URL" \
    -c "SELECT count(*) FROM pg_publication WHERE pubname = 'pg_dbmigrator_pub'" \
    | tr -d '[:space:]')
if [[ "$pub_count" != "1" ]]; then
    echo "FAIL: publication should exist from seed.sql (count=$pub_count)" >&2
    exit 1
fi

# ── Run online migration with keep-slot enabled ─────────────────────────
build_example online_migration_example
launch_online_migrator "$LOG_FILE" \
    PG_DBMIGRATOR_KEEP_SLOT=1

# ── Wait for initial catch-up ───────────────────────────────────────────
echo "==> waiting for 'ready for cutover'"
CUTOVER_LINE=$(wait_for_log_match "$LOG_FILE" "ready for cutover" 0 120)
echo "==> got 'ready for cutover' on line $CUTOVER_LINE"

# ── Cutover ─────────────────────────────────────────────────────────────
sigint_and_wait "$LOG_FILE" 120
assert_data_equal 500

# ── Post-cutover: slot should STILL exist (keep-slot=true) ──────────────
slot_count=$("${PSQL_BASE[@]}" "$SOURCE_URL" \
    -c "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'pg_dbmigrator%'" \
    | tr -d '[:space:]')
if [[ "$slot_count" == "0" ]]; then
    echo "FAIL: replication slot should have been kept (KEEP_SLOT=1) but it was dropped" >&2
    exit 1
fi
echo "==> confirmed: replication slot retained after cutover (count=$slot_count)"

# ── Post-cutover: pre-existing publication should NOT be dropped ────────
pub_count=$("${PSQL_BASE[@]}" "$SOURCE_URL" \
    -c "SELECT count(*) FROM pg_publication WHERE pubname = 'pg_dbmigrator_pub'" \
    | tr -d '[:space:]')
if [[ "$pub_count" != "1" ]]; then
    echo "FAIL: pre-existing publication should NOT be dropped (was not auto-created)" >&2
    exit 1
fi
echo "==> confirmed: pre-existing publication retained after cutover"

echo "PASS: online keep-slot — slot retained, pre-existing publication retained, data equal (500 rows)"

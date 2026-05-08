#!/usr/bin/env bash
# End-to-end test: publication/subscription lifecycle auto-management.
#
# Exercises the new auto-create + post-cutover cleanup paths that the
# standard online test does NOT cover (seed.sql pre-creates the
# publication there).
#
# Flow:
#   1. Seed source but DROP the publication so it is absent at start.
#   2. Run online migrator with auto-create enabled (default) — the
#      migrator must auto-create the publication.
#   3. Wait for initial catch-up, SIGINT to trigger cutover.
#   4. Assert data equality.
#   5. Assert the publication was dropped on source after cutover.
#   6. Assert the replication slot was dropped on source after cutover.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
source "$ROOT/tests/integration/lib.sh"

LOG_FILE="$(mktemp -t pg_dbmigrator_auto_pub.XXXXXX.log)"
echo "==> log file: $LOG_FILE"

trap 'stop_migrator' EXIT

wait_for_pg "$SOURCE_URL" "source"
wait_for_pg "$TARGET_URL" "target"

# ── Setup: seed source but remove the publication ───────────────────────
setup_online_test

echo "==> dropping publication so auto-create path is exercised"
psql_exec "$SOURCE_URL" "DROP PUBLICATION IF EXISTS pg_dbmigrator_pub;"

# Verify publication is indeed absent before we start.
pub_count=$("${PSQL_BASE[@]}" "$SOURCE_URL" \
    -c "SELECT count(*) FROM pg_publication WHERE pubname = 'pg_dbmigrator_pub'" \
    | tr -d '[:space:]')
if [[ "$pub_count" != "0" ]]; then
    echo "FAIL: publication should not exist at this point (count=$pub_count)" >&2
    exit 1
fi
echo "==> confirmed: publication absent on source"

# ── Run online migration (auto-create enabled by default) ───────────────
build_example online_migration_example
launch_online_migrator "$LOG_FILE"

# ── Wait for initial catch-up ───────────────────────────────────────────
echo "==> waiting for 'ready for cutover'"
CUTOVER_LINE=$(wait_for_log_match "$LOG_FILE" "ready for cutover" 0 120)
echo "==> got 'ready for cutover' on line $CUTOVER_LINE"

# Verify auto-create happened — the log should contain the marker.
if ! grep -q "publication created successfully" "$LOG_FILE"; then
    echo "FAIL: migrator did not auto-create the publication" >&2
    tail -n 80 "$LOG_FILE" >&2
    exit 1
fi
echo "==> confirmed: publication was auto-created by migrator"

# ── Cutover ─────────────────────────────────────────────────────────────
sigint_and_wait "$LOG_FILE" 120
assert_data_equal 500

# ── Post-cutover: verify publication was cleaned up on source ───────────
pub_count=$("${PSQL_BASE[@]}" "$SOURCE_URL" \
    -c "SELECT count(*) FROM pg_publication WHERE pubname = 'pg_dbmigrator_pub'" \
    | tr -d '[:space:]')
if [[ "$pub_count" != "0" ]]; then
    echo "FAIL: auto-created publication should have been dropped after cutover (count=$pub_count)" >&2
    exit 1
fi
echo "==> confirmed: publication dropped on source after cutover"

# ── Post-cutover: verify replication slot was cleaned up on source ──────
slot_count=$("${PSQL_BASE[@]}" "$SOURCE_URL" \
    -c "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'pg_dbmigrator%'" \
    | tr -d '[:space:]')
if [[ "$slot_count" != "0" ]]; then
    echo "FAIL: replication slot should have been dropped after cutover (count=$slot_count)" >&2
    exit 1
fi
echo "==> confirmed: replication slot dropped on source after cutover"

echo "PASS: online auto-pub lifecycle — auto-create, data equal (500 rows), publication + slot cleaned up"

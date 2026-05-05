#!/usr/bin/env bash
# Online migration test — UPDATE/DELETE/INSERT mix during dump+restore window.
#
#   1. Seed source; launch migrator.
#   2. Once pg_dump starts, kick off background mutations.
#   3. Wait for apply phase, continue mutations for 5s.
#   4. Stop mutations, wait for target == source.
#   5. SIGINT → cutover. Assert equality.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
source "$ROOT/tests/integration/lib.sh"

LOG_FILE="$(mktemp -t pg_dbmigrator_online_updates.XXXXXX.log)"
TICK_FILE="$(mktemp -t pg_dbmigrator_mut_tick.XXXXXX)"
echo "==> log file: $LOG_FILE"
echo "==> tick file: $TICK_FILE"

cleanup() {
    stop_mutations
    stop_migrator
    rm -f "$TICK_FILE"
}
trap cleanup EXIT

wait_for_pg "$SOURCE_URL" "source"
wait_for_pg "$TARGET_URL" "target"

setup_online_test
build_example online_migration_example
launch_online_migrator "$LOG_FILE"

echo "==> waiting for pg_dump to start (slot is live)"
DUMP_LINE=$(wait_for_log_match "$LOG_FILE" "starting pg_dump" 0 60)
echo "==> pg_dump started on log line $DUMP_LINE — beginning mutation loop"

start_mutations "$SOURCE_URL" "$TICK_FILE"
echo "==> mutation loop pid: $MUTATION_LOOP_PID"

echo "==> waiting for apply phase (first 'replication lag' heartbeat)"
APPLY_LINE=$(wait_for_log_match "$LOG_FILE" "replication lag" "$DUMP_LINE" 180)
echo "==> apply phase started on line $APPLY_LINE"

echo "==> continuing mutations for 5s while apply is live"
sleep 5

echo "==> stopping mutation loop"
stop_mutations
sleep 2
ticks=$(cat "$TICK_FILE" 2>/dev/null || echo 0)
echo "==> mutations executed: $ticks iterations"
src_count=$(query_count "$SOURCE_URL")
echo "==> source row count after mutations: $src_count"

echo "==> waiting for target content_hash to match source (cutover gate)"
HASH=$(wait_for_content_match "$SOURCE_URL" "$TARGET_URL" 300)
echo "==> content hashes match: $HASH"

sigint_and_wait "$LOG_FILE"
assert_data_equal

echo "PASS: online migration with UPDATE/DELETE/INSERT mix — $ticks mutations, $src_count rows, hashes equal"

#!/usr/bin/env bash
# Online migration test — 60s of sustained UPDATE/DELETE/INSERT during
# the apply phase, then strict pre-cutover equality check.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
source "$ROOT/tests/integration/lib.sh"

SUSTAIN_SECS="${SUSTAIN_SECS:-60}"

LOG_FILE="$(mktemp -t pg_dbmigrator_online_sustained.XXXXXX.log)"
TICK_FILE="$(mktemp -t pg_dbmigrator_mut_tick.XXXXXX)"
echo "==> log file: $LOG_FILE"
echo "==> tick file: $TICK_FILE"
echo "==> sustained mutation window: ${SUSTAIN_SECS}s"

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

export PG_DBMIGRATOR_MAX_RUNTIME_SECS=1200
launch_online_migrator "$LOG_FILE"

echo "==> waiting for pg_dump to start (slot is live)"
DUMP_LINE=$(wait_for_log_match "$LOG_FILE" "starting pg_dump" 0 60)
echo "==> pg_dump started on log line $DUMP_LINE — beginning mutation loop"

start_mutations "$SOURCE_URL" "$TICK_FILE"
echo "==> mutation loop pid: $MUTATION_LOOP_PID"

echo "==> waiting for apply phase (first 'replication lag' heartbeat)"
APPLY_LINE=$(wait_for_log_match "$LOG_FILE" "replication lag" "$DUMP_LINE" 180)
echo "==> apply phase started on line $APPLY_LINE — sustaining mutations for ${SUSTAIN_SECS}s"

ticks_at_start=$(cat "$TICK_FILE" 2>/dev/null || echo 0)
sleep "$SUSTAIN_SECS"
ticks_at_end=$(cat "$TICK_FILE" 2>/dev/null || echo 0)
delta=$((ticks_at_end - ticks_at_start))
echo "==> mutations during sustain window: $delta (total: $ticks_at_end)"
if (( delta < 10 )); then
    echo "FAIL: mutation loop did not make meaningful progress in ${SUSTAIN_SECS}s (delta=$delta)" >&2
    tail -n 60 "$LOG_FILE" >&2
    exit 1
fi

echo "==> stopping mutation loop"
stop_mutations
sleep 2
src_hash_pre=$(content_hash "$SOURCE_URL")
echo "==> source post-mutation hash: $src_hash_pre"

echo "==> waiting for target to fully catch up (pre-cutover equality gate)"
PRE_CUTOVER_HASH=$(wait_for_content_match "$SOURCE_URL" "$TARGET_URL" 600)
echo "==> PRE-CUTOVER content hashes match: $PRE_CUTOVER_HASH"

if [[ "$PRE_CUTOVER_HASH" != "$src_hash_pre" ]]; then
    echo "FAIL: matched hash drifted from source snapshot" >&2
    exit 1
fi

sigint_and_wait "$LOG_FILE"

src_hash=$(content_hash "$SOURCE_URL")
if [[ "$src_hash" != "$PRE_CUTOVER_HASH" ]]; then
    echo "FAIL: post-cutover hash differs from pre-cutover gate (drift after SIGINT)" >&2
    exit 1
fi
assert_data_equal

echo "PASS: online migration with ${SUSTAIN_SECS}s sustained mutations — pre-cutover equality verified, post-cutover stable ($(query_count "$SOURCE_URL") rows)"

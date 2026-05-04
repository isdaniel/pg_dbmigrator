#!/usr/bin/env bash
# Online migration test — Cancel + Resume scenario.
#
#   1. Seed source (500 rows), create publication.
#   2. Launch migrator (first run) with pinned dump path.
#   3. Wait for apply phase, start mutation loop.
#   4. Continue mutations for 5s while apply is live.
#   5. SIGINT → graceful cutover (first run exits cleanly).
#   6. Verify the resume token exists with dump+restore marked complete.
#   7. Resume: relaunch with PG_MIGRATOR_RESUME=1.
#   8. Wait for apply phase to restart.
#   9. Stop mutations, wait for target == source content.
#  10. SIGINT → graceful cutover.
#  11. Assert source == target (row count + content hash).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
source "$ROOT/tests/integration/lib.sh"

DUMP_DIR="$(mktemp -d -t pg_migrator_cancel_resume.XXXXXX)"
DUMP_PATH="$DUMP_DIR/dump"
LOG1="$(mktemp -t pg_migrator_cr_run1.XXXXXX.log)"
LOG2="$(mktemp -t pg_migrator_cr_run2.XXXXXX.log)"
TICK_FILE="$(mktemp -t pg_migrator_cr_tick.XXXXXX)"
echo "==> dump dir: $DUMP_DIR"
echo "==> log #1: $LOG1"
echo "==> log #2: $LOG2"
echo "==> tick file: $TICK_FILE"

cleanup() {
    stop_mutations
    stop_migrator
    rm -rf "$DUMP_DIR" "$TICK_FILE"
}
trap cleanup EXIT

wait_for_pg "$SOURCE_URL" "source"
wait_for_pg "$TARGET_URL" "target"

setup_online_test
build_example online_migration_example

# ═══════════════════════════════════════════════════════════════════════════
# RUN #1: initial migration — will be cancelled mid-apply
# ═══════════════════════════════════════════════════════════════════════════
echo "==> RUN #1: launching migrator (initial run, pinned dump path)"
export PG_MIGRATOR_DUMP_PATH="$DUMP_PATH"
launch_online_migrator "$LOG1"

echo "==> waiting for pg_dump to start (slot is live)"
DUMP_LINE=$(wait_for_log_match "$LOG1" "starting pg_dump" 0 60)
echo "==> pg_dump started on log line $DUMP_LINE — beginning mutation loop"

start_mutations "$SOURCE_URL" "$TICK_FILE"
echo "==> mutation loop pid: $MUTATION_LOOP_PID"

echo "==> waiting for apply phase (first 'replication lag' heartbeat)"
APPLY_LINE=$(wait_for_log_match "$LOG1" "replication lag" "$DUMP_LINE" 180)
echo "==> apply phase started on line $APPLY_LINE"

echo "==> continuing mutations for 5s while apply is live"
sleep 5

echo "==> sending SIGINT to trigger cutover (run #1)"
sigint_and_wait "$LOG1"
echo "==> RUN #1 exited"

# Verify resume token exists
RESUME_FILE="$DUMP_PATH.resume.json"
if [[ ! -f "$RESUME_FILE" ]]; then
    echo "FAIL: resume token not written at $RESUME_FILE" >&2
    ls -la "$DUMP_DIR" >&2
    exit 1
fi
echo "==> resume token found at $RESUME_FILE"

for stage in dump restore; do
    if ! grep -qi "\"$stage\"" "$RESUME_FILE"; then
        echo "FAIL: resume token missing completed stage '$stage'" >&2
        cat "$RESUME_FILE" >&2
        exit 1
    fi
done
echo "==> resume token has dump+restore marked complete"

# ═══════════════════════════════════════════════════════════════════════════
# RUN #2: resume — skip dump+restore, jump to apply
# ═══════════════════════════════════════════════════════════════════════════
echo "==> RUN #2: launching migrator with RESUME=1"
launch_online_migrator "$LOG2" PG_MIGRATOR_RESUME=1

echo "==> waiting for apply phase in run #2"
APPLY2_LINE=$(wait_for_log_match "$LOG2" "replication lag" 0 180)
echo "==> apply phase restarted on line $APPLY2_LINE"

if grep -q "starting pg_dump" "$LOG2"; then
    echo "FAIL: run #2 re-ran pg_dump despite resume token" >&2
    exit 1
fi
if grep -q "starting pg_restore" "$LOG2"; then
    echo "FAIL: run #2 re-ran pg_restore despite resume token" >&2
    exit 1
fi
echo "==> confirmed: run #2 skipped dump+restore (resume working)"

echo "==> stopping mutation loop"
stop_mutations
sleep 2

src_count=$(query_count "$SOURCE_URL")
echo "==> source row count after mutations: $src_count"

echo "==> waiting for target to match source (pre-cutover gate)"
HASH=$(wait_for_content_match "$SOURCE_URL" "$TARGET_URL" 300)
echo "==> content hashes match: $HASH"

sigint_and_wait "$LOG2"
assert_data_equal

echo "PASS: online cancel+resume — run #1 cancelled mid-apply, run #2 resumed and caught up, data equal ($src_count rows)"

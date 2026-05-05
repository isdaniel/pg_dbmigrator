#!/usr/bin/env bash
# Online migration test — Multi-Resume with 30s sustained mutations.
#
# This test verifies that the migrator can be gracefully cancelled (SIGINT)
# and resumed multiple times during the apply phase while the source is under
# continuous mutation load, and still achieve a consistent final cutover.
#
#   1. Seed source (500 rows), create publication.
#   2. RUN #1: launch migrator, wait for apply, mutate 3s, SIGINT → cutover.
#   3. RUN #2 (resume): skip dump+restore, new subscription, mutate 3s, SIGINT → cutover.
#   4. RUN #3 (resume, final): skip dump+restore, new subscription,
#      sustain mutations for 30s, stop mutations, wait for "ready for
#      cutover", verify pre-cutover equality, SIGINT → cutover.
#   5. Assert post-cutover source == target (row count + content hash).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
source "$ROOT/tests/integration/lib.sh"

SUSTAIN_SECS="${SUSTAIN_SECS:-30}"

DUMP_DIR="$(mktemp -d -t pg_dbmigrator_multi_resume.XXXXXX)"
DUMP_PATH="$DUMP_DIR/dump"
RESUME_FILE="$DUMP_PATH.resume.json"
LOG1="$(mktemp -t pg_dbmigrator_mr_run1.XXXXXX.log)"
LOG2="$(mktemp -t pg_dbmigrator_mr_run2.XXXXXX.log)"
LOG3="$(mktemp -t pg_dbmigrator_mr_run3.XXXXXX.log)"
TICK_FILE="$(mktemp -t pg_dbmigrator_mr_tick.XXXXXX)"
echo "==> dump dir: $DUMP_DIR"
echo "==> log #1: $LOG1"
echo "==> log #2: $LOG2"
echo "==> log #3: $LOG3"
echo "==> tick file: $TICK_FILE"
echo "==> sustained mutation window: ${SUSTAIN_SECS}s"

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

export PG_DBMIGRATOR_DUMP_PATH="$DUMP_PATH"
export PG_DBMIGRATOR_MAX_RUNTIME_SECS=1200

# ═══════════════════════════════════════════════════════════════════════════
# RUN #1: initial run — cancelled mid-apply via SIGINT (graceful cutover)
# ═══════════════════════════════════════════════════════════════════════════
echo "==> RUN #1: launching migrator (initial run)"
launch_online_migrator "$LOG1"

echo "==> waiting for pg_dump to start"
DUMP_LINE=$(wait_for_log_match "$LOG1" "starting pg_dump" 0 60)

start_mutations "$SOURCE_URL" "$TICK_FILE"
echo "==> mutation loop pid: $MUTATION_LOOP_PID"

echo "==> waiting for apply phase (run #1)"
APPLY_LINE=$(wait_for_log_match "$LOG1" "replication lag" "$DUMP_LINE" 180)
echo "==> apply started on line $APPLY_LINE"

echo "==> mutating for 3s while apply is active"
sleep 3

echo "==> sending SIGINT → graceful cutover (run #1)"
sigint_and_wait "$LOG1"
echo "==> RUN #1 exited"

# Verify resume token
if [[ ! -f "$RESUME_FILE" ]]; then
    echo "FAIL: resume token not written at $RESUME_FILE" >&2
    exit 1
fi
for stage in dump restore; do
    if ! grep -qi "\"$stage\"" "$RESUME_FILE"; then
        echo "FAIL: resume token missing completed stage '$stage'" >&2
        cat "$RESUME_FILE" >&2
        exit 1
    fi
done
echo "==> resume token valid with dump+restore marked complete"

# ═══════════════════════════════════════════════════════════════════════════
# RUN #2: first resume — skips dump+restore, new subscription, SIGINT cutover
# ═══════════════════════════════════════════════════════════════════════════
echo "==> RUN #2: launching migrator (resume #1)"
launch_online_migrator "$LOG2" PG_DBMIGRATOR_RESUME=1

echo "==> waiting for apply phase (run #2)"
APPLY2_LINE=$(wait_for_log_match "$LOG2" "replication lag" 0 180)
echo "==> apply restarted on line $APPLY2_LINE"

if grep -q "starting pg_dump" "$LOG2"; then
    echo "FAIL: run #2 re-ran pg_dump despite resume token" >&2
    exit 1
fi
if grep -q "starting pg_restore" "$LOG2"; then
    echo "FAIL: run #2 re-ran pg_restore despite resume token" >&2
    exit 1
fi
echo "==> confirmed: run #2 skipped dump+restore (resume working)"

echo "==> mutating for 3s while apply is active"
sleep 3

echo "==> sending SIGINT → graceful cutover (run #2)"
sigint_and_wait "$LOG2"
echo "==> RUN #2 exited"

# ═══════════════════════════════════════════════════════════════════════════
# RUN #3: final resume — sustained mutations, then graceful cutover
# ═══════════════════════════════════════════════════════════════════════════
echo "==> RUN #3: launching migrator (resume #2, final)"
launch_online_migrator "$LOG3" PG_DBMIGRATOR_RESUME=1

echo "==> waiting for apply phase (run #3)"
APPLY3_LINE=$(wait_for_log_match "$LOG3" "replication lag" 0 180)
echo "==> apply restarted on line $APPLY3_LINE"

if grep -q "starting pg_dump" "$LOG3"; then
    echo "FAIL: run #3 re-ran pg_dump despite resume token" >&2
    exit 1
fi
if grep -q "starting pg_restore" "$LOG3"; then
    echo "FAIL: run #3 re-ran pg_restore despite resume token" >&2
    exit 1
fi

echo "==> sustaining mutations for ${SUSTAIN_SECS}s"
ticks_at_start=$(cat "$TICK_FILE" 2>/dev/null || echo 0)
sleep "$SUSTAIN_SECS"
ticks_at_end=$(cat "$TICK_FILE" 2>/dev/null || echo 0)
delta=$((ticks_at_end - ticks_at_start))
echo "==> mutations during sustained window: $delta (total: $ticks_at_end)"
if (( delta < 10 )); then
    echo "FAIL: mutation loop did not make meaningful progress in ${SUSTAIN_SECS}s (delta=$delta)" >&2
    tail -n 60 "$LOG3" >&2
    exit 1
fi

echo "==> stopping mutation loop"
stop_mutations
sleep 2

src_hash_pre=$(content_hash "$SOURCE_URL")
echo "==> source post-mutation hash: $src_hash_pre"

echo "==> waiting for 'ready for cutover' in run #3"
CUTOVER_LINE=$(wait_for_log_match "$LOG3" "ready for cutover" "$APPLY3_LINE" 300)
echo "==> got 'ready for cutover' on line $CUTOVER_LINE"

echo "==> waiting for target to fully catch up (pre-cutover equality gate)"
PRE_CUTOVER_HASH=$(wait_for_content_match "$SOURCE_URL" "$TARGET_URL" 600)
echo "==> PRE-CUTOVER content hashes match: $PRE_CUTOVER_HASH"

if [[ "$PRE_CUTOVER_HASH" != "$src_hash_pre" ]]; then
    echo "FAIL: matched hash drifted from source snapshot" >&2
    exit 1
fi

echo "==> sending SIGINT to trigger cutover (run #3)"
sigint_and_wait "$LOG3"
echo "==> RUN #3 exited"

# Final assertions
src_hash=$(content_hash "$SOURCE_URL")
if [[ "$src_hash" != "$PRE_CUTOVER_HASH" ]]; then
    echo "FAIL: post-cutover hash differs from pre-cutover gate (drift after SIGINT)" >&2
    exit 1
fi
assert_data_equal

echo "PASS: online multi-resume with ${SUSTAIN_SECS}s sustained mutations — 2 cancel/resume cycles, pre-cutover equality verified, data equal ($(query_count "$SOURCE_URL") rows)"

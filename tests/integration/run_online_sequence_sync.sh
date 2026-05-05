#!/usr/bin/env bash
# Online migration test that verifies post-cutover sequence sync.
#
# Why this test exists:
#   PostgreSQL logical replication does NOT replicate `nextval()` —
#   sequence advances are not WAL-logged. Without an explicit sync at
#   cutover, the target's sequences stay at their dump-time `last_value`,
#   and the first post-cutover `INSERT … DEFAULT nextval(...)` will
#   collide with a row already replicated by the apply worker, producing
#   a duplicate-key violation. This test reproduces that flow and asserts
#   that the migrator's sequence-sync step closes the gap.
#
# Flow:
#   1. Seed source with 100 rows in app.events (BIGSERIAL PK).
#   2. Launch online migration.
#   3. Wait for first 'ready for cutover'.
#   4. INSERT 200 more rows via DEFAULT nextval — sequence advances on
#      source from 100 to 300.
#   5. Wait for second 'ready for cutover' AND target row count == 300.
#   6. SIGINT → cutover (this triggers the sequence sync step).
#   7. On the target, run `INSERT ... DEFAULT` — assert it succeeds and
#      produces id >= 301 (NO collision with replicated row 300).
#   8. Assert target's pg_sequence_last_value('app.events_id_seq') == 300.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
source "$ROOT/tests/integration/lib.sh"

INITIAL_EVENTS=100
STREAMED_EVENTS=200
EXPECTED_EVENTS=$((INITIAL_EVENTS + STREAMED_EVENTS))   # 300

LOG_FILE="$(mktemp -t pg_dbmigrator_seq_sync.XXXXXX.log)"
echo "==> log file: $LOG_FILE"

trap 'stop_migrator' EXIT

wait_for_pg "$SOURCE_URL" "source"
wait_for_pg "$TARGET_URL" "target"

setup_online_test
build_example online_migration_example
launch_online_migrator "$LOG_FILE"

# ── Phase 1: initial catch-up ────────────────────────────────────────────
echo "==> waiting for FIRST 'ready for cutover'"
FIRST_LINE=$(wait_for_log_match "$LOG_FILE" "ready for cutover" 0 120)
echo "==> got first 'ready for cutover' on line $FIRST_LINE"

# ── Phase 2: insert into app.events via DEFAULT nextval ──────────────────
echo "==> inserting $STREAMED_EVENTS more events via DEFAULT nextval on source"
psql_exec "$SOURCE_URL" "
    INSERT INTO app.events (note)
    SELECT 'streamed-' || g
    FROM generate_series(1, $STREAMED_EVENTS) g;"

# Confirm source's sequence advanced.
src_seq_before=$(query_seq_last_value "$SOURCE_URL" "app.events_id_seq")
echo "==> source sequence app.events_id_seq.last_value = $src_seq_before (expected $EXPECTED_EVENTS)"
if [[ "$src_seq_before" != "$EXPECTED_EVENTS" ]]; then
    echo "FAIL: source sequence did not advance to $EXPECTED_EVENTS" >&2
    exit 1
fi

# ── Phase 3: wait for stream to catch up & target rows to land ───────────
echo "==> waiting for SECOND 'ready for cutover'"
SECOND_LINE=$(wait_for_log_match "$LOG_FILE" "ready for cutover" "$FIRST_LINE" 600)
echo "==> got second 'ready for cutover' on line $SECOND_LINE"

echo "==> waiting for target.app.events to reach $EXPECTED_EVENTS rows"
wait_for_table_count "$TARGET_URL" "app.events" "$EXPECTED_EVENTS" 300 >/dev/null
echo "==> target reached $EXPECTED_EVENTS events"

# ── Phase 4: confirm target sequence is BEHIND before cutover ────────────
# (Sanity check: this proves we're actually testing the sync step.)
tgt_seq_before=$(query_seq_last_value "$TARGET_URL" "app.events_id_seq")
echo "==> target sequence BEFORE cutover sync = '$tgt_seq_before' (expected behind $EXPECTED_EVENTS or NULL)"
# We accept either NULL (never advanced on target since restore) or any
# value < EXPECTED_EVENTS — the point is it has NOT been kept in sync by
# logical replication.
if [[ -n "$tgt_seq_before" && "$tgt_seq_before" -ge "$EXPECTED_EVENTS" ]]; then
    echo "WARN: target sequence is already at $tgt_seq_before before cutover; sync test is degenerate"
fi

# ── Phase 5: cutover ─────────────────────────────────────────────────────
sigint_and_wait "$LOG_FILE"

# Verify the migrator logged a sequence-sync step. Don't be picky about
# the wording, just that it happened.
if ! grep -qiE "(sync(ed|ing)? .*sequence|sequence sync)" "$LOG_FILE"; then
    echo "FAIL: migrator did not log a sequence-sync step" >&2
    tail -n 80 "$LOG_FILE" >&2
    exit 1
fi
echo "==> migrator logged sequence-sync"

# ── Phase 6: assert the target sequence matches the source ──────────────
tgt_seq_after=$(query_seq_last_value "$TARGET_URL" "app.events_id_seq")
echo "==> target sequence AFTER cutover sync = $tgt_seq_after"
if [[ "$tgt_seq_after" != "$EXPECTED_EVENTS" ]]; then
    echo "FAIL: target sequence ($tgt_seq_after) != source sequence ($EXPECTED_EVENTS)" >&2
    exit 1
fi

# ── Phase 7: final smoke — application INSERT must NOT collide ──────────
psql_exec "$TARGET_URL" "INSERT INTO app.events (note) VALUES ('post-cutover')"
new_id=$("${PSQL_BASE[@]}" "$TARGET_URL" -c \
    "SELECT id FROM app.events WHERE note = 'post-cutover'" | tr -d '[:space:]')
echo "==> post-cutover INSERT got id=$new_id"
if (( new_id <= EXPECTED_EVENTS )); then
    echo "FAIL: post-cutover INSERT got id=$new_id which collides with replicated rows up to $EXPECTED_EVENTS" >&2
    exit 1
fi

echo "PASS: online sequence-sync — target sequence advanced to $EXPECTED_EVENTS, post-cutover INSERT got id=$new_id (no collision)"

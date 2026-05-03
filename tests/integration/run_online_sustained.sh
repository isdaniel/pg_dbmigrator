#!/usr/bin/env bash
# Online migration test — Scenario 2: 60s of sustained UPDATE/DELETE/INSERT
# during the apply phase, then strict pre-cutover equality check.
#
#   1. Seed source with 500 rows; create publication.
#   2. Launch examples/online_migration.
#   3. Once the slot is live (pg_dump started), kick off a background
#      mutation loop that interleaves INSERT, UPDATE, DELETE.
#   4. Wait for the FIRST "ready for cutover".
#   5. Keep mutating for SUSTAIN_SECS additional seconds (default 60).
#   6. Stop mutations on the source.
#   7. **Before cutover**, wait until target content_hash exactly matches
#      source's. This is the strict zero-data-loss gate the customer asks
#      for: source and target must be byte-for-byte identical *prior to*
#      the cutover signal.
#   8. Send SIGINT → graceful cutover.
#   9. Re-verify equality post-cutover (no drift introduced by teardown).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

export SOURCE_URL="postgres://migrator:migrator@127.0.0.1:55432/appdb"
export TARGET_URL="postgres://migrator:migrator@127.0.0.1:55433/appdb"
export SUBSCRIPTION_SOURCE_URL="postgres://migrator:migrator@pg_migrator_source:5432/appdb"

# Override for faster local iteration: SUSTAIN_SECS=10 bash run_online_sustained.sh
SUSTAIN_SECS="${SUSTAIN_SECS:-60}"

source "$ROOT/tests/integration/lib.sh"

LOG_FILE="$(mktemp -t pg_migrator_online_sustained.XXXXXX.log)"
TICK_FILE="$(mktemp -t pg_migrator_mut_tick.XXXXXX)"
echo "==> log file: $LOG_FILE"
echo "==> tick file: $TICK_FILE"
echo "==> sustained mutation window: ${SUSTAIN_SECS}s"

cleanup() {
    stop_mutations
    if [[ -n "${MIGRATOR_PID:-}" ]] && kill -0 "$MIGRATOR_PID" 2>/dev/null; then
        echo "==> cleanup: terminating migrator pid $MIGRATOR_PID"
        kill -TERM "$MIGRATOR_PID" 2>/dev/null || true
        wait "$MIGRATOR_PID" 2>/dev/null || true
    fi
    rm -f "$TICK_FILE"
}
trap cleanup EXIT

wait_for_pg "$SOURCE_URL" "source"
wait_for_pg "$TARGET_URL" "target"

echo "==> resetting source schema + creating publication"
PGPASSWORD=migrator psql -v ON_ERROR_STOP=1 -h 127.0.0.1 -p 55432 -U migrator -d appdb \
    -f "$ROOT/tests/integration/seed.sql" >/dev/null

echo "==> dropping any leftover replication slot"
psql_exec "$SOURCE_URL" "
DO \$\$
DECLARE r record;
BEGIN
    FOR r IN SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'pg_migrator%' LOOP
        EXECUTE format('SELECT pg_drop_replication_slot(%L)', r.slot_name);
    END LOOP;
END
\$\$;"

echo "==> dropping any leftover subscription on target"
psql_exec "$TARGET_URL" "
DO \$\$
DECLARE r record;
BEGIN
    FOR r IN SELECT subname FROM pg_subscription WHERE subname LIKE 'pg_migrator%' LOOP
        EXECUTE format('ALTER SUBSCRIPTION %I DISABLE', r.subname);
        EXECUTE format('ALTER SUBSCRIPTION %I SET (slot_name = NONE)', r.subname);
        EXECUTE format('DROP SUBSCRIPTION %I', r.subname);
    END LOOP;
END
\$\$;"

echo "==> resetting target schema"
psql_exec "$TARGET_URL" "DROP SCHEMA IF EXISTS app CASCADE;"

echo "==> building online_migration_example"
cargo build --quiet -p online_migration_example
BIN="$ROOT/target/debug/online_migration_example"

echo "==> launching migrator in background"
PG_MIGRATOR_SOURCE="$SOURCE_URL" \
PG_MIGRATOR_TARGET="$TARGET_URL" \
PG_MIGRATOR_SUBSCRIPTION_SOURCE="$SUBSCRIPTION_SOURCE_URL" \
PG_MIGRATOR_LAG_THRESHOLD_BYTES=64 \
PG_MIGRATOR_POLL_SECS=1 \
PG_MIGRATOR_FEEDBACK_SECS=1 \
PG_MIGRATOR_MAX_RUNTIME_SECS=1200 \
RUST_LOG="info,pg_migrator=info,pg_walstream=warn" \
    stdbuf -oL -eL "$BIN" >"$LOG_FILE" 2>&1 &
MIGRATOR_PID=$!
echo "==> migrator pid: $MIGRATOR_PID"

# ── Phase 1: wait for slot to be live, then start mutations. ─────────────
echo "==> waiting for pg_dump to start (slot is live)"
DUMP_LINE=$(wait_for_log_match "$LOG_FILE" "starting pg_dump" 0 60)
echo "==> pg_dump started on log line $DUMP_LINE — beginning mutation loop"

start_mutations "$SOURCE_URL" "$TICK_FILE"
echo "==> mutation loop pid: $MUTATION_LOOP_PID"

# ── Phase 2: wait for apply phase to start. ──────────────────────────────
# We use the first `replication lag` heartbeat (not `ready for cutover`):
# with continuous mutations the threshold-based CaughtUp event may never
# fire, but the lag heartbeat starts as soon as CREATE SUBSCRIPTION is up.
echo "==> waiting for FIRST 'replication lag' heartbeat (apply phase started)"
APPLY_LINE=$(wait_for_log_match "$LOG_FILE" "replication lag" "$DUMP_LINE" 180)
echo "==> apply phase started on line $APPLY_LINE — sustaining mutations for ${SUSTAIN_SECS}s"

# ── Phase 3: keep mutating for SUSTAIN_SECS more seconds. ────────────────
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

# ── Phase 4: stop source mutations. ──────────────────────────────────────
echo "==> stopping mutation loop"
stop_mutations
sleep 2  # let in-flight statements land
src_count_pre=$(query_count "$SOURCE_URL")
src_hash_pre=$(content_hash "$SOURCE_URL")
echo "==> source post-mutation: rows=$src_count_pre hash=$src_hash_pre"

# ── Phase 5: STRICT PRE-CUTOVER EQUALITY GATE. ───────────────────────────
# Customer requirement: "cutover 前需要確保 source & target db 資料完全一致".
# We poll content_hash on both sides until they match exactly. Only then
# do we send SIGINT.
echo "==> waiting for target to fully catch up (pre-cutover equality gate)"
PRE_CUTOVER_HASH=$(wait_for_content_match "$SOURCE_URL" "$TARGET_URL" 600)
echo "==> PRE-CUTOVER content hashes match: $PRE_CUTOVER_HASH"

if [[ "$PRE_CUTOVER_HASH" != "$src_hash_pre" ]]; then
    echo "FAIL: matched hash drifted from source snapshot (source moved? expected stopped)" >&2
    exit 1
fi

# ── Phase 6: operator-driven cutover via SIGINT. ─────────────────────────
echo "==> sending SIGINT to trigger cutover"
kill -INT "$MIGRATOR_PID"

WAIT_DEADLINE=$(( $(date +%s) + 120 ))
while kill -0 "$MIGRATOR_PID" 2>/dev/null; do
    if (( $(date +%s) > WAIT_DEADLINE )); then
        echo "FAIL: migrator did not exit within 120s after SIGINT" >&2
        tail -n 80 "$LOG_FILE" >&2
        exit 1
    fi
    sleep 0.5
done
wait "$MIGRATOR_PID" 2>/dev/null || true
unset MIGRATOR_PID

if ! grep -q "migration done" "$LOG_FILE"; then
    echo "FAIL: migrator did not log a clean 'migration done'" >&2
    tail -n 80 "$LOG_FILE" >&2
    exit 1
fi

# ── Phase 7: post-cutover re-verification. ───────────────────────────────
src_count=$(query_count "$SOURCE_URL")
tgt_count=$(query_count "$TARGET_URL")
src_hash=$(content_hash "$SOURCE_URL")
tgt_hash=$(content_hash "$TARGET_URL")
echo "==> POST-CUTOVER: src_rows=$src_count tgt_rows=$tgt_count"
echo "==> POST-CUTOVER: src_hash=$src_hash tgt_hash=$tgt_hash"

if [[ "$src_count" != "$tgt_count" ]]; then
    echo "FAIL: row counts diverged post-cutover (src=$src_count tgt=$tgt_count)" >&2
    exit 1
fi
if [[ "$src_hash" != "$tgt_hash" || -z "$src_hash" ]]; then
    echo "FAIL: content hashes differ post-cutover" >&2
    exit 1
fi
if [[ "$src_hash" != "$PRE_CUTOVER_HASH" ]]; then
    echo "FAIL: post-cutover hash differs from pre-cutover gate (drift after SIGINT)" >&2
    exit 1
fi

echo "PASS: online migration with ${SUSTAIN_SECS}s sustained mutations — pre-cutover equality verified, post-cutover stable ($src_count rows)"

#!/usr/bin/env bash
# Online migration test — Scenario 1: UPDATE/DELETE/INSERT mix during the
# dump+restore window.
#
#   1. Seed source with 500 rows; create publication.
#   2. Launch examples/online_migration.
#   3. As soon as `pg_dump` starts (i.e. the slot is alive), kick off a
#      background mutation loop that interleaves INSERT, UPDATE, DELETE.
#   4. Wait for the FIRST "ready for cutover" → apply phase reached.
#   5. Stop the mutation loop; capture the final source content_hash.
#   6. Wait until target content_hash matches source's (the strict
#      zero-data-loss precondition for cutover).
#   7. Send SIGINT → graceful cutover.
#   8. Verify source and target row counts and content_hash match.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

export SOURCE_URL="postgres://migrator:migrator@127.0.0.1:55432/appdb"
export TARGET_URL="postgres://migrator:migrator@127.0.0.1:55433/appdb"
export SUBSCRIPTION_SOURCE_URL="postgres://migrator:migrator@pg_migrator_source:5432/appdb"

source "$ROOT/tests/integration/lib.sh"

LOG_FILE="$(mktemp -t pg_migrator_online_updates.XXXXXX.log)"
TICK_FILE="$(mktemp -t pg_migrator_mut_tick.XXXXXX)"
echo "==> log file: $LOG_FILE"
echo "==> tick file: $TICK_FILE"

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
PG_MIGRATOR_MAX_RUNTIME_SECS=900 \
RUST_LOG="info,pg_migrator=info,pg_walstream=warn" \
    stdbuf -oL -eL "$BIN" >"$LOG_FILE" 2>&1 &
MIGRATOR_PID=$!
echo "==> migrator pid: $MIGRATOR_PID"

# ── Phase 1: wait for the slot to exist before starting mutations. The
# "starting pg_dump" line is logged immediately after slot creation so it's
# the safest hook for "slot is now capturing WAL".
echo "==> waiting for pg_dump to start (slot is live)"
DUMP_LINE=$(wait_for_log_match "$LOG_FILE" "starting pg_dump" 0 60)
echo "==> pg_dump started on log line $DUMP_LINE — beginning mutation loop"

start_mutations "$SOURCE_URL" "$TICK_FILE"
echo "==> mutation loop pid: $MUTATION_LOOP_PID"

# ── Phase 2: wait for apply phase to start. ──────────────────────────────
# We can't rely on "ready for cutover" here because the mutation loop keeps
# the source ahead of the threshold; instead use the first `replication
# lag` heartbeat, which fires once `CREATE SUBSCRIPTION` has been issued
# and the apply worker is polling.
echo "==> waiting for FIRST 'replication lag' heartbeat (apply phase started)"
APPLY_LINE=$(wait_for_log_match "$LOG_FILE" "replication lag" "$DUMP_LINE" 180)
echo "==> apply phase started on line $APPLY_LINE"

# Keep mutating for a short window so changes definitively land *during*
# apply (not only during dump/restore).
echo "==> continuing mutations for 5s while apply is live"
sleep 5

# ── Phase 3: stop mutations, snapshot final source state. ────────────────
echo "==> stopping mutation loop"
stop_mutations
sleep 2  # let any in-flight statement land
ticks=$(cat "$TICK_FILE" 2>/dev/null || echo 0)
echo "==> mutations executed: $ticks iterations"
src_count=$(query_count "$SOURCE_URL")
echo "==> source row count after mutations: $src_count"

# ── Phase 4: zero-data-loss gate — wait until target == source. ──────────
echo "==> waiting for target content_hash to match source (cutover gate)"
HASH=$(wait_for_content_match "$SOURCE_URL" "$TARGET_URL" 300)
echo "==> content hashes match: $HASH"

# ── Phase 5: operator-driven cutover via SIGINT. ─────────────────────────
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

# ── Phase 6: post-cutover assertions. ────────────────────────────────────
src_count=$(query_count "$SOURCE_URL")
tgt_count=$(query_count "$TARGET_URL")
echo "==> source rows: $src_count  target rows: $tgt_count"
if [[ "$src_count" != "$tgt_count" ]]; then
    echo "FAIL: row counts diverged post-cutover (src=$src_count tgt=$tgt_count)" >&2
    exit 1
fi

src_hash=$(content_hash "$SOURCE_URL")
tgt_hash=$(content_hash "$TARGET_URL")
echo "==> source content md5: $src_hash"
echo "==> target content md5: $tgt_hash"
if [[ "$src_hash" != "$tgt_hash" || -z "$src_hash" ]]; then
    echo "FAIL: source and target content hashes differ post-cutover" >&2
    exit 1
fi

echo "PASS: online migration with UPDATE/DELETE/INSERT mix — $ticks mutations, $src_count rows, hashes equal"

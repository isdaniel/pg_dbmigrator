#!/usr/bin/env bash
# Online lag-poll cadence test — verifies that once the apply loop is
# at or below the lag threshold the heartbeats fire on the FAST cadence
# (sub-second), not on the regular slow `--cutover-poll-secs`.
#
# Concretely:
#   1. Reset source + target.
#   2. Launch online_migration with:
#        - PG_MIGRATOR_POLL_SECS=10        (slow path — what we'd be
#                                            stuck with WITHOUT adaptive)
#        - PG_MIGRATOR_FAST_POLL_MS=200    (target fast path)
#        - PG_MIGRATOR_LAG_THRESHOLD_BYTES=64
#   3. No source mutations → lag should hit 0 immediately and stay
#      there, so we should see the FAST cadence basically forever.
#   4. After the first `replication lag` heartbeat lands, count the
#      heartbeats over a 3-second window. With slow=10s the count
#      would be 0–1; with fast=200ms it should be ≥ 5.
#   5. SIGINT → assert clean cutover.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

export SOURCE_URL="postgres://migrator:migrator@127.0.0.1:55432/appdb"
export TARGET_URL="postgres://migrator:migrator@127.0.0.1:55433/appdb"
export SUBSCRIPTION_SOURCE_URL="postgres://migrator:migrator@pg_migrator_source:5432/appdb"

source "$ROOT/tests/integration/lib.sh"

LOG_FILE="$(mktemp -t pg_migrator_lag_cadence.XXXXXX.log)"
echo "==> log file: $LOG_FILE"

cleanup() {
    if [[ -n "${MIGRATOR_PID:-}" ]] && kill -0 "$MIGRATOR_PID" 2>/dev/null; then
        kill -TERM "$MIGRATOR_PID" 2>/dev/null || true
        wait "$MIGRATOR_PID" 2>/dev/null || true
    fi
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

echo "==> launching migrator with slow=10s fast=200ms threshold=64"
PG_MIGRATOR_SOURCE="$SOURCE_URL" \
PG_MIGRATOR_TARGET="$TARGET_URL" \
PG_MIGRATOR_SUBSCRIPTION_SOURCE="$SUBSCRIPTION_SOURCE_URL" \
PG_MIGRATOR_LAG_THRESHOLD_BYTES=64 \
PG_MIGRATOR_POLL_SECS=10 \
PG_MIGRATOR_FAST_POLL_MS=200 \
PG_MIGRATOR_FEEDBACK_SECS=1 \
PG_MIGRATOR_MAX_RUNTIME_SECS=300 \
RUST_LOG="info,pg_migrator=info,pg_walstream=warn" \
    stdbuf -oL -eL "$BIN" >"$LOG_FILE" 2>&1 &
MIGRATOR_PID=$!
echo "==> migrator pid: $MIGRATOR_PID"

echo "==> waiting for FIRST 'replication lag' heartbeat"
FIRST=$(wait_for_log_match "$LOG_FILE" "replication lag" 0 180)
echo "==> first heartbeat at log line $FIRST"

# Snapshot heartbeat count, sleep 3s, snapshot again. Difference is the
# count over the window.
count_at_t0=$(grep -c "replication lag" "$LOG_FILE" || true)
echo "==> heartbeats at t=0: $count_at_t0"
sleep 3
count_at_t3=$(grep -c "replication lag" "$LOG_FILE" || true)
echo "==> heartbeats at t=3s: $count_at_t3"
delta=$((count_at_t3 - count_at_t0))
echo "==> heartbeats in 3s window: $delta"

# 3000 ms / 200 ms = 15 expected; allow generous slack for slot warm-up
# / fixed jitter on the very first iteration. Fail if the slow cadence
# (≤ 1 heartbeat per 3s) is observed.
if (( delta < 5 )); then
    echo "FAIL: only $delta heartbeats in 3s — adaptive fast cadence not active" >&2
    echo "---- log tail ----" >&2
    tail -n 40 "$LOG_FILE" >&2
    exit 1
fi

echo "==> sending SIGINT to trigger clean cutover"
kill -INT "$MIGRATOR_PID"

WAIT_DEADLINE=$(( $(date +%s) + 60 ))
while kill -0 "$MIGRATOR_PID" 2>/dev/null; do
    if (( $(date +%s) > WAIT_DEADLINE )); then
        echo "FAIL: migrator did not exit within 60s after SIGINT" >&2
        tail -n 60 "$LOG_FILE" >&2
        exit 1
    fi
    sleep 0.5
done
wait "$MIGRATOR_PID" 2>/dev/null || true
unset MIGRATOR_PID

if ! grep -q "migration done" "$LOG_FILE"; then
    echo "FAIL: migrator did not log 'migration done' after SIGINT" >&2
    tail -n 60 "$LOG_FILE" >&2
    exit 1
fi

echo "PASS: adaptive lag cadence — $delta heartbeats in 3s, clean cutover after SIGINT"

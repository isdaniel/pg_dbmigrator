#!/usr/bin/env bash
# Online lag-poll cadence test — verifies that once the apply loop is
# at or below the lag threshold the heartbeats fire on the FAST cadence
# (sub-second), not on the regular slow --cutover-poll-secs.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
source "$ROOT/tests/integration/lib.sh"

LOG_FILE="$(mktemp -t pg_dbmigrator_lag_cadence.XXXXXX.log)"
echo "==> log file: $LOG_FILE"

trap 'stop_migrator' EXIT

wait_for_pg "$SOURCE_URL" "source"
wait_for_pg "$TARGET_URL" "target"

setup_online_test
build_example online_migration_example

echo "==> launching migrator with slow=10s fast=200ms threshold=64"
export PG_DBMIGRATOR_POLL_SECS=10
export PG_DBMIGRATOR_FAST_POLL_MS=200
export PG_DBMIGRATOR_MAX_RUNTIME_SECS=300
launch_online_migrator "$LOG_FILE"

echo "==> waiting for FIRST 'replication lag' heartbeat"
FIRST=$(wait_for_log_match "$LOG_FILE" "replication lag" 0 180)
echo "==> first heartbeat at log line $FIRST"

count_at_t0=$(grep -c "replication lag" "$LOG_FILE" || true)
echo "==> heartbeats at t=0: $count_at_t0"
sleep 3
count_at_t3=$(grep -c "replication lag" "$LOG_FILE" || true)
echo "==> heartbeats at t=3s: $count_at_t3"
delta=$((count_at_t3 - count_at_t0))
echo "==> heartbeats in 3s window: $delta"

if (( delta < 5 )); then
    echo "FAIL: only $delta heartbeats in 3s — adaptive fast cadence not active" >&2
    tail -n 40 "$LOG_FILE" >&2
    exit 1
fi

sigint_and_wait "$LOG_FILE" 60

echo "PASS: adaptive lag cadence — $delta heartbeats in 3s, clean cutover after SIGINT"

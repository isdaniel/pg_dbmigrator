#!/usr/bin/env bash
# SIGINT mid-dump cancellation test — verifies that an in-flight pg_dump
# is killed within seconds when the migrator receives SIGINT.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
source "$ROOT/tests/integration/lib.sh"

LOG_FILE="$(mktemp -t pg_migrator_sigint.XXXXXX.log)"
DUMP_DIR="$(mktemp -d -t pg_migrator_sigint_dump.XXXXXX)"
echo "==> log file: $LOG_FILE"
echo "==> dump dir: $DUMP_DIR"

cleanup() {
    if [[ -n "${MIGRATOR_PID:-}" ]] && kill -0 "$MIGRATOR_PID" 2>/dev/null; then
        kill -KILL "$MIGRATOR_PID" 2>/dev/null || true
    fi
    rm -rf "$DUMP_DIR"
}
trap cleanup EXIT

wait_for_pg "$SOURCE_URL" "source"
wait_for_pg "$TARGET_URL" "target"

echo "==> resetting source schema and seeding ~3M rows for a slow dump"
seed_source
psql_exec "$SOURCE_URL" "
    INSERT INTO app.widgets (id, name, qty)
    SELECT (1000000 + g)::text,
           repeat('x', 200) || g::text,
           (g % 1000)::text
    FROM generate_series(1, 3000000) g;
"

reset_target_schema
build_example offline_migration_example

echo "==> launching migrator in background"
PG_MIGRATOR_SOURCE="$SOURCE_URL" \
PG_MIGRATOR_TARGET="$TARGET_URL" \
PG_MIGRATOR_DUMP_PATH="$DUMP_DIR/dump" \
RUST_LOG="info,pg_migrator=info" \
    stdbuf -oL -eL "$BIN" >"$LOG_FILE" 2>&1 &
MIGRATOR_PID=$!
echo "==> migrator pid: $MIGRATOR_PID"

echo "==> waiting for 'starting pg_dump'"
DUMP_LINE=$(wait_for_log_match "$LOG_FILE" "starting pg_dump" 0 30)
echo "==> pg_dump started on log line $DUMP_LINE"

sleep 1

echo "==> verifying pg_dump child is alive"
if ! pgrep -P "$MIGRATOR_PID" pg_dump >/dev/null; then
    echo "WARN: no pg_dump child found — perhaps the dump finished too fast" >&2
fi

echo "==> sending SIGINT to migrator pid $MIGRATOR_PID"
kill -INT "$MIGRATOR_PID"

WAIT_DEADLINE=$(( $(date +%s) + 10 ))
while kill -0 "$MIGRATOR_PID" 2>/dev/null; do
    if (( $(date +%s) > WAIT_DEADLINE )); then
        echo "FAIL: migrator did not exit within 10s after SIGINT" >&2
        tail -n 60 "$LOG_FILE" >&2
        kill -KILL "$MIGRATOR_PID" 2>/dev/null || true
        exit 1
    fi
    sleep 0.1
done
wait "$MIGRATOR_PID" 2>/dev/null && rc=0 || rc=$?
echo "==> migrator exited with rc=$rc"
unset MIGRATOR_PID

if [[ "$rc" == "0" ]]; then
    echo "FAIL: expected non-zero exit (cancellation), got $rc" >&2
    exit 1
fi

if pgrep -af "pg_dump.*$DUMP_DIR" >/dev/null; then
    echo "FAIL: pg_dump children still running after migrator exit" >&2
    pgrep -af "pg_dump.*$DUMP_DIR" >&2
    exit 1
fi

if ! grep -qE "Cancelled|cancellation requested|killing child" "$LOG_FILE"; then
    echo "FAIL: log does not show a cancellation marker" >&2
    tail -n 40 "$LOG_FILE" >&2
    exit 1
fi

echo "PASS: SIGINT killed mid-dump within 10s and pg_dump children are gone"

#!/usr/bin/env bash
# SIGINT mid-dump cancellation test — verifies that an in-flight
# `pg_dump` is killed within seconds when the migrator receives SIGINT,
# rather than blocking until pg_dump finishes naturally.
#
# Concretely:
#   1. Reset source + seed it with a few million rows so pg_dump runs
#      for several seconds (otherwise SIGINT would arrive after pg_dump
#      already exited and we'd be testing nothing).
#   2. Launch examples/offline_migration in the background.
#   3. Wait for the "starting pg_dump" log marker, then sleep briefly
#      to make sure pg_dump is genuinely mid-run.
#   4. SIGINT the migrator and assert it exits within a tight budget.
#   5. Verify no leftover pg_dump children remain.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

export SOURCE_URL="postgres://migrator:migrator@127.0.0.1:55432/appdb"
export TARGET_URL="postgres://migrator:migrator@127.0.0.1:55433/appdb"

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
PGPASSWORD=migrator psql -v ON_ERROR_STOP=1 -h 127.0.0.1 -p 55432 -U migrator -d appdb \
    -f "$ROOT/tests/integration/seed.sql" >/dev/null
psql_exec "$SOURCE_URL" "
    -- Bulk-load enough rows that pg_dump takes well over the SIGINT budget.
    INSERT INTO app.widgets (id, name, qty)
    SELECT (1000000 + g)::text,
           repeat('x', 200) || g::text,
           (g % 1000)::text
    FROM generate_series(1, 3000000) g;
"

echo "==> resetting target schema"
psql_exec "$TARGET_URL" "DROP SCHEMA IF EXISTS app CASCADE;"

echo "==> building offline_migration_example"
cargo build --quiet -p offline_migration_example
BIN="$ROOT/target/debug/offline_migration_example"

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

# Give pg_dump a moment to actually be doing real work — the test would
# be uninteresting if SIGINT arrived after pg_dump's natural exit.
sleep 1

echo "==> verifying pg_dump child is alive"
if ! pgrep -P "$MIGRATOR_PID" pg_dump >/dev/null; then
    echo "WARN: no pg_dump child found — perhaps the dump finished too fast" >&2
fi

echo "==> sending SIGINT to migrator pid $MIGRATOR_PID"
kill -INT "$MIGRATOR_PID"

# Cancellation budget: with the cancel-aware command runner, the loop
# should kill its child and exit within a couple of seconds at most.
WAIT_DEADLINE=$(( $(date +%s) + 10 ))
while kill -0 "$MIGRATOR_PID" 2>/dev/null; do
    if (( $(date +%s) > WAIT_DEADLINE )); then
        echo "FAIL: migrator did not exit within 10s after SIGINT" >&2
        echo "---- log tail ----" >&2
        tail -n 60 "$LOG_FILE" >&2
        kill -KILL "$MIGRATOR_PID" 2>/dev/null || true
        exit 1
    fi
    sleep 0.1
done
wait "$MIGRATOR_PID" 2>/dev/null && rc=0 || rc=$?
echo "==> migrator exited with rc=$rc"
unset MIGRATOR_PID

# Migrator should NOT exit 0 — pg_dump was interrupted.
if [[ "$rc" == "0" ]]; then
    echo "FAIL: expected non-zero exit (cancellation), got $rc" >&2
    exit 1
fi

# Confirm no orphan pg_dump processes survive.
if pgrep -af "pg_dump.*$DUMP_DIR" >/dev/null; then
    echo "FAIL: pg_dump children still running after migrator exit" >&2
    pgrep -af "pg_dump.*$DUMP_DIR" >&2
    exit 1
fi

# Log should show evidence of cancellation.
if ! grep -qE "Cancelled|cancellation requested|killing child" "$LOG_FILE"; then
    echo "FAIL: log does not show a cancellation marker" >&2
    tail -n 40 "$LOG_FILE" >&2
    exit 1
fi

echo "PASS: SIGINT killed mid-dump within 10s and pg_dump children are gone"

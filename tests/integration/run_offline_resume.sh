#!/usr/bin/env bash
# Offline resume token test — verifies that a second run with
# PG_MIGRATOR_RESUME=1 skips the previously-completed pg_dump and
# pg_restore stages.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
source "$ROOT/tests/integration/lib.sh"

DUMP_DIR="$(mktemp -d -t pg_migrator_resume_dump.XXXXXX)"
DUMP_PATH="$DUMP_DIR/dump"
RESUME_FILE="$DUMP_DIR/dump.resume.json"
LOG1="$(mktemp -t pg_migrator_resume_run1.XXXXXX.log)"
LOG2="$(mktemp -t pg_migrator_resume_run2.XXXXXX.log)"
echo "==> dump dir: $DUMP_DIR"
echo "==> log #1: $LOG1"
echo "==> log #2: $LOG2"

trap 'rm -rf "$DUMP_DIR"' EXIT

wait_for_pg "$SOURCE_URL" "source"
wait_for_pg "$TARGET_URL" "target"

seed_source
reset_target_schema

echo "==> RUN #1: full migration (no resume) — also writes resume token"
PG_MIGRATOR_SOURCE="$SOURCE_URL" \
PG_MIGRATOR_TARGET="$TARGET_URL" \
PG_MIGRATOR_DUMP_PATH="$DUMP_PATH" \
RUST_LOG="info,pg_migrator=info" \
    cargo run --quiet -p offline_migration_example >"$LOG1" 2>&1

if ! grep -q "starting pg_dump" "$LOG1"; then
    echo "FAIL: run #1 did not log 'starting pg_dump'" >&2
    tail -n 40 "$LOG1" >&2
    exit 1
fi
if ! grep -q "starting pg_restore" "$LOG1"; then
    echo "FAIL: run #1 did not log 'starting pg_restore'" >&2
    tail -n 40 "$LOG1" >&2
    exit 1
fi
if [[ ! -f "$RESUME_FILE" ]]; then
    echo "FAIL: resume token not written at $RESUME_FILE" >&2
    ls -la "$DUMP_DIR" >&2
    exit 1
fi

echo "==> resume token contents:"
cat "$RESUME_FILE"
echo

for stage in dump restore; do
    if ! grep -qi "\"$stage\"" "$RESUME_FILE"; then
        echo "FAIL: resume token missing completed stage '$stage'" >&2
        exit 1
    fi
done

echo "==> wiping target schema before RUN #2"
reset_target_schema

echo "==> RUN #2: with --resume, must skip both stages"
PG_MIGRATOR_SOURCE="$SOURCE_URL" \
PG_MIGRATOR_TARGET="$TARGET_URL" \
PG_MIGRATOR_DUMP_PATH="$DUMP_PATH" \
PG_MIGRATOR_RESUME=1 \
RUST_LOG="info,pg_migrator=info" \
    cargo run --quiet -p offline_migration_example >"$LOG2" 2>&1

if ! grep -q "skipped (resume): pg_dump already complete" "$LOG2"; then
    echo "FAIL: run #2 did not skip pg_dump on resume" >&2
    tail -n 40 "$LOG2" >&2
    exit 1
fi
if ! grep -q "skipped (resume): pg_restore already complete" "$LOG2"; then
    echo "FAIL: run #2 did not skip pg_restore on resume" >&2
    tail -n 40 "$LOG2" >&2
    exit 1
fi
if grep -q "starting pg_dump" "$LOG2"; then
    echo "FAIL: run #2 still ran pg_dump despite resume token" >&2
    tail -n 40 "$LOG2" >&2
    exit 1
fi

if "${PSQL_BASE[@]}" "$TARGET_URL" \
        -c "SELECT 1 FROM pg_namespace WHERE nspname='app'" | grep -q '1'; then
    echo "FAIL: target schema 'app' was reloaded — run #2 did NOT honour resume token" >&2
    exit 1
fi
echo "==> target is empty as expected; run #2 honoured the token"

echo "PASS: offline resume — second run skipped both stages cleanly"

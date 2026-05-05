#!/usr/bin/env bash
# Offline split-sections orchestration test — verifies that pre-data /
# data / post-data restore phases are issued in order and that the
# resulting target is byte-for-byte identical to the source.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
source "$ROOT/tests/integration/lib.sh"

LOG_FILE="$(mktemp -t pg_dbmigrator_offline_split.XXXXXX.log)"
DUMP_DIR="$(mktemp -d -t pg_dbmigrator_split_dump.XXXXXX)"
echo "==> log file: $LOG_FILE"
echo "==> dump dir: $DUMP_DIR"

trap 'rm -rf "$DUMP_DIR"' EXIT

wait_for_pg "$SOURCE_URL" "source"
wait_for_pg "$TARGET_URL" "target"

seed_source

echo "==> adding secondary indexes so post-data has work to do"
psql_exec "$SOURCE_URL" "
    CREATE INDEX IF NOT EXISTS widgets_name_idx ON app.widgets(name);
    CREATE INDEX IF NOT EXISTS widgets_qty_idx  ON app.widgets(qty);
"

reset_target_schema

echo "==> running offline_migration_example with split-sections"
PG_DBMIGRATOR_SOURCE="$SOURCE_URL" \
PG_DBMIGRATOR_TARGET="$TARGET_URL" \
PG_DBMIGRATOR_SPLIT_SECTIONS=1 \
PG_DBMIGRATOR_DUMP_PATH="$DUMP_DIR/dump" \
NO_COLOR=1 \
RUST_LOG="info,pg_dbmigrator=info" \
    cargo run --quiet -p offline_migration_example >"$LOG_FILE" 2>&1

echo "==> checking section ordering in log"
sections=$(sed -E 's/\x1b\[[0-9;]*m//g' "$LOG_FILE" \
    | grep -oE "running pg_restore section section=(PreData|Data|PostData)" \
    | sed -E 's/.*section=//' || true)
echo "==> section log entries:"
printf '%s\n' "$sections"
expected=$'PreData\nData\nPostData'
if [[ "$sections" != "$expected" ]]; then
    echo "FAIL: expected three section log entries in order PreData/Data/PostData; got:" >&2
    printf '%s\n' "$sections" >&2
    tail -n 60 "$LOG_FILE" >&2
    exit 1
fi

assert_data_equal 500

echo "==> verifying secondary indexes were rebuilt on target"
indexes=$("${PSQL_BASE[@]}" "$TARGET_URL" -c "
    SELECT count(*) FROM pg_indexes
    WHERE schemaname = 'app' AND tablename = 'widgets'
" | tr -d '[:space:]')
if (( indexes < 3 )); then
    echo "FAIL: expected >= 3 indexes on app.widgets, got $indexes" >&2
    exit 1
fi
echo "==> target index count: $indexes"

echo "PASS: offline split-sections — pre-data → data → post-data, hashes match, indexes rebuilt"

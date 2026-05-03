#!/usr/bin/env bash
# Offline split-sections orchestration test — verifies that pre-data /
# data / post-data restore phases are issued in order and that the
# resulting target is byte-for-byte identical to the source.
#
# Concretely:
#   1. Reset source + target.
#   2. Run examples/offline_migration with PG_MIGRATOR_SPLIT_SECTIONS=1.
#   3. Grep migrator stderr for "running pg_restore section" entries —
#      there must be exactly THREE, in pre-data → data → post-data
#      order.
#   4. Assert source/target row count and content_hash equality.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

export SOURCE_URL="postgres://migrator:migrator@127.0.0.1:55432/appdb"
export TARGET_URL="postgres://migrator:migrator@127.0.0.1:55433/appdb"

source "$ROOT/tests/integration/lib.sh"

LOG_FILE="$(mktemp -t pg_migrator_offline_split.XXXXXX.log)"
DUMP_DIR="$(mktemp -d -t pg_migrator_split_dump.XXXXXX)"
echo "==> log file: $LOG_FILE"
echo "==> dump dir: $DUMP_DIR"

cleanup() {
    rm -rf "$DUMP_DIR"
}
trap cleanup EXIT

wait_for_pg "$SOURCE_URL" "source"
wait_for_pg "$TARGET_URL" "target"

echo "==> resetting source schema"
PGPASSWORD=migrator psql -v ON_ERROR_STOP=1 -h 127.0.0.1 -p 55432 -U migrator -d appdb \
    -f "$ROOT/tests/integration/seed.sql" >/dev/null

echo "==> adding a secondary index so post-data has work to do"
psql_exec "$SOURCE_URL" "
    CREATE INDEX IF NOT EXISTS widgets_name_idx ON app.widgets(name);
    CREATE INDEX IF NOT EXISTS widgets_qty_idx  ON app.widgets(qty);
"

echo "==> resetting target schema"
psql_exec "$TARGET_URL" "DROP SCHEMA IF EXISTS app CASCADE;"

echo "==> running offline_migration_example with split-sections"
PG_MIGRATOR_SOURCE="$SOURCE_URL" \
PG_MIGRATOR_TARGET="$TARGET_URL" \
PG_MIGRATOR_SPLIT_SECTIONS=1 \
PG_MIGRATOR_DUMP_PATH="$DUMP_DIR/dump" \
NO_COLOR=1 \
RUST_LOG="info,pg_migrator=info" \
    cargo run --quiet -p offline_migration_example >"$LOG_FILE" 2>&1

echo "==> checking section ordering in log"
# tracing emits the kv field as `section=<Variant>`; strip any stray ANSI
# escape sequences in case NO_COLOR isn't honoured by the writer.
sections=$(sed -E 's/\x1b\[[0-9;]*m//g' "$LOG_FILE" \
    | grep -oE "running pg_restore section section=(PreData|Data|PostData)" \
    | sed -E 's/.*section=//' || true)
echo "==> section log entries:"
printf '%s\n' "$sections"
expected=$'PreData\nData\nPostData'
if [[ "$sections" != "$expected" ]]; then
    echo "FAIL: expected three section log entries in order PreData/Data/PostData; got:" >&2
    printf '%s\n' "$sections" >&2
    echo "---- log tail ----" >&2
    tail -n 60 "$LOG_FILE" >&2
    exit 1
fi

src_count=$(query_count "$SOURCE_URL")
tgt_count=$(query_count "$TARGET_URL")
echo "==> source rows: $src_count  target rows: $tgt_count"
if [[ "$src_count" != "$tgt_count" || "$tgt_count" != "500" ]]; then
    echo "FAIL: row counts diverged (src=$src_count tgt=$tgt_count)" >&2
    exit 1
fi

src_hash=$(content_hash "$SOURCE_URL")
tgt_hash=$(content_hash "$TARGET_URL")
echo "==> source hash: $src_hash  target hash: $tgt_hash"
if [[ "$src_hash" != "$tgt_hash" || -z "$src_hash" ]]; then
    echo "FAIL: content hashes differ" >&2
    exit 1
fi

echo "==> verifying secondary indexes were rebuilt on target"
indexes=$(PGPASSWORD=migrator "${PSQL_BASE[@]}" "$TARGET_URL" -c "
    SELECT count(*) FROM pg_indexes
    WHERE schemaname = 'app' AND tablename = 'widgets'
" | tr -d '[:space:]')
# Expect 3: PRIMARY KEY (id) + name idx + qty idx.
if (( indexes < 3 )); then
    echo "FAIL: expected ≥ 3 indexes on app.widgets, got $indexes" >&2
    exit 1
fi
echo "==> target index count: $indexes"

echo "PASS: offline split-sections — pre-data → data → post-data, hashes match, indexes rebuilt"

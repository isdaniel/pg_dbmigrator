#!/usr/bin/env bash
# End-to-end offline migration test driven by the actual example binary.
#
# Flow:
#   1. Reset source + target via psql.
#   2. Run examples/offline_migration → it does pg_dump → pg_restore.
#   3. Assert target has the same row count as source.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

export SOURCE_URL="postgres://migrator:migrator@127.0.0.1:55432/appdb"
export TARGET_URL="postgres://migrator:migrator@127.0.0.1:55433/appdb"

source "$ROOT/tests/integration/lib.sh"

wait_for_pg "$SOURCE_URL" "source"
wait_for_pg "$TARGET_URL" "target"

echo "==> resetting source schema"
PGPASSWORD=migrator psql -v ON_ERROR_STOP=1 -h 127.0.0.1 -p 55432 -U migrator -d appdb \
    -f "$ROOT/tests/integration/seed.sql" >/dev/null

echo "==> resetting target schema"
PGPASSWORD=migrator psql -v ON_ERROR_STOP=1 -h 127.0.0.1 -p 55433 -U migrator -d appdb \
    -c "DROP SCHEMA IF EXISTS app CASCADE;" >/dev/null

echo "==> running offline_migration_example"
PG_MIGRATOR_SOURCE="$SOURCE_URL" \
PG_MIGRATOR_TARGET="$TARGET_URL" \
    cargo run --quiet -p offline_migration_example

src_count=$(query_count "$SOURCE_URL")
tgt_count=$(query_count "$TARGET_URL")
echo "==> source rows: $src_count  target rows: $tgt_count"

if [[ "$src_count" != "$tgt_count" ]]; then
    echo "FAIL: target row count ($tgt_count) does not match source ($src_count)" >&2
    exit 1
fi
if [[ "$tgt_count" != "500" ]]; then
    echo "FAIL: expected 500 rows on target, got $tgt_count" >&2
    exit 1
fi

echo "PASS: offline migration replicated all 500 rows"

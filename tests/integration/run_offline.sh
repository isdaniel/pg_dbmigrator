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
source "$ROOT/tests/integration/lib.sh"

wait_for_pg "$SOURCE_URL" "source"
wait_for_pg "$TARGET_URL" "target"

seed_source

echo "==> resetting target schema"
psql -v ON_ERROR_STOP=1 -h 127.0.0.1 -p 55433 -U migrator -d appdb \
    -c "DROP SCHEMA IF EXISTS app CASCADE;" >/dev/null

echo "==> running offline_migration_example"
PG_MIGRATOR_SOURCE="$SOURCE_URL" \
PG_MIGRATOR_TARGET="$TARGET_URL" \
    cargo run --quiet -p offline_migration_example

assert_data_equal 500

echo "PASS: offline migration replicated all 500 rows"

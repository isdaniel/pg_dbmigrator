#!/usr/bin/env bash
# Offline migration auto-verify test.
#
# Flow:
#   1. Seed source; reset target.
#   2. Run the offline example (verify runs automatically unless --skip-verify).
#   3. Assert the Verify stage ran and reported a clean row-count match.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
source "$ROOT/tests/integration/lib.sh"

LOG_FILE="$(mktemp -t pg_dbmigrator_offline_verify.XXXXXX.log)"
echo "==> log file: $LOG_FILE"

wait_for_pg "$SOURCE_URL" "source"
wait_for_pg "$TARGET_URL" "target"

seed_source
reset_target_schema

echo "==> running offline_migration_example (auto-verify enabled)"
PG_DBMIGRATOR_SOURCE="$SOURCE_URL" \
PG_DBMIGRATOR_TARGET="$TARGET_URL" \
    cargo run --quiet -p offline_migration_example 2>&1 | tee "$LOG_FILE"

# Row content must actually match (independent of the log assertions below).
assert_data_equal 500

# Assert the Verify stage ran and reported a clean match. The row-count
# summary line ("verify: N table(s) matched") is emitted only by the Verify
# stage; unlike the ANSI-wrapped `stage=Verify` tracing field, the message
# text is stable to grep whether output is a tty or a file.
if ! grep -qi "verify:.*table(s) matched" "$LOG_FILE"; then
    echo "FAIL: Verify stage did not report a clean match in offline run" >&2
    tail -n 40 "$LOG_FILE" >&2
    exit 1
fi

echo "PASS: offline auto-verify reported a clean row-count match"

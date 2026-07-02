#!/usr/bin/env bash
# Online migration auto-verify test.
#
# Flow (mirrors run_online.sh's slot/publication + SIGINT cutover path):
#   1. Seed source; reset target; set up slots/subscriptions.
#   2. Launch the online example with a tiny lag threshold.
#   3. Wait for "ready for cutover", then wait for the target to hold all rows.
#   4. SIGINT -> graceful cutover.
#   5. Assert source == target, and that the post-cutover Verify stage ran and
#      reported a clean row-count match.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
source "$ROOT/tests/integration/lib.sh"

EXPECTED_TOTAL=500

LOG_FILE="$(mktemp -t pg_dbmigrator_online_verify.XXXXXX.log)"
echo "==> log file: $LOG_FILE"

trap 'stop_migrator' EXIT

wait_for_pg "$SOURCE_URL" "source"
wait_for_pg "$TARGET_URL" "target"

setup_online_test
build_example online_migration_example
launch_online_migrator "$LOG_FILE"

echo "==> waiting for 'ready for cutover'"
READY_LINE=$(wait_for_log_match "$LOG_FILE" "ready for cutover" 0 120)
echo "==> got 'ready for cutover' on line $READY_LINE"

echo "==> waiting for target row count to reach $EXPECTED_TOTAL"
wait_for_table_count "$TARGET_URL" "app.widgets" "$EXPECTED_TOTAL" 120 >/dev/null
echo "==> target holds all $EXPECTED_TOTAL rows"

# ── cutover ──────────────────────────────────────────────────────────────
sigint_and_wait "$LOG_FILE"
assert_data_equal "$EXPECTED_TOTAL"

# ── assert the post-cutover Verify stage ran and reported a clean match ──
# The "verify: N table(s) matched" summary line is emitted only by the Verify
# stage and (unlike the ANSI-wrapped `stage=Verify` field) greps cleanly from
# a redirected log file.
if ! grep -qi "verify:.*table(s) matched" "$LOG_FILE"; then
    echo "FAIL: Verify stage did not report a clean match in online run" >&2
    tail -n 40 "$LOG_FILE" >&2
    exit 1
fi

echo "PASS: online migration verified clean after cutover ($EXPECTED_TOTAL rows)"

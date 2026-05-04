#!/usr/bin/env bash
# End-to-end online migration test — strict zero-data-loss flow.
#
#   1. Seed source with 500 rows; create publication.
#   2. Launch online_migration with a tiny lag threshold.
#   3. Wait for the FIRST "ready for cutover" (initial catch-up).
#   4. Mutate the source heavily — INSERT 100k, DELETE 1k.
#   5. Wait for the SECOND "ready for cutover".
#   6. Wait until target row count matches expected.
#   7. SIGINT → graceful cutover.
#   8. Assert source == target (row count + content hash).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
source "$ROOT/tests/integration/lib.sh"

INSERT_END=100500
DELETE_END=1000
EXPECTED_TOTAL=$((500 + (INSERT_END - 500) - DELETE_END))   # = 99500

LOG_FILE="$(mktemp -t pg_migrator_online.XXXXXX.log)"
echo "==> log file: $LOG_FILE"

trap 'stop_migrator' EXIT

wait_for_pg "$SOURCE_URL" "source"
wait_for_pg "$TARGET_URL" "target"

setup_online_test
build_example online_migration_example
launch_online_migrator "$LOG_FILE"

# ── Phase 1: initial catch-up ────────────────────────────────────────────
echo "==> waiting for FIRST 'ready for cutover'"
FIRST_LINE=$(wait_for_log_match "$LOG_FILE" "ready for cutover" 0 120)
echo "==> got first 'ready for cutover' on line $FIRST_LINE"

# ── Phase 2: heavy perturbation while replication is live ────────────────
echo "==> perturbing source: +$((INSERT_END - 500)) inserts, -$DELETE_END deletes"
psql_exec "$SOURCE_URL" "
    INSERT INTO app.widgets (id, name, qty)
    SELECT g::text, 'streamed-' || g, (g * 3)::text
    FROM generate_series(501, $INSERT_END) g;"
psql_exec "$SOURCE_URL" "DELETE FROM app.widgets WHERE id::int BETWEEN 1 AND $DELETE_END;"
src_count=$(query_count "$SOURCE_URL")
echo "==> source post-perturbation row count: $src_count (expecting $EXPECTED_TOTAL)"
if [[ "$src_count" != "$EXPECTED_TOTAL" ]]; then
    echo "FAIL: perturbation did not land on source as expected" >&2
    exit 1
fi

# ── Phase 3: stream caught up ────────────────────────────────────────────
echo "==> waiting for SECOND 'ready for cutover' (must be strictly after line $FIRST_LINE)"
SECOND_LINE=$(wait_for_log_match "$LOG_FILE" "ready for cutover" "$FIRST_LINE" 600)
echo "==> got second 'ready for cutover' on line $SECOND_LINE"

# ── Phase 4: gate cutover on target holding all rows ─────────────────────
SNAP=$(lag_heartbeat_at_or_before "$LOG_FILE" "$SECOND_LINE")
TARGET_SOURCE_LSN=$(awk '{print $1}' <<<"$SNAP")
INITIAL_APPLIED_LSN=$(awk '{print $2}' <<<"$SNAP")
echo "==> snapshot at second 'ready for cutover': source_lsn=$TARGET_SOURCE_LSN applied_lsn=$INITIAL_APPLIED_LSN"

echo "==> waiting for target row count to reach $EXPECTED_TOTAL (strict zero-data-loss gate)"
GATE_DEADLINE=$(( $(date +%s) + 900 ))
while :; do
    cur=$(query_count "$TARGET_URL")
    if [[ "$cur" == "$EXPECTED_TOTAL" ]]; then
        echo "==> target reached $EXPECTED_TOTAL rows — apply is complete"
        break
    fi
    if (( $(date +%s) > GATE_DEADLINE )); then
        echo "FAIL: target did not reach $EXPECTED_TOTAL rows within 15min (last=$cur)" >&2
        tail -n 80 "$LOG_FILE" >&2
        exit 1
    fi
    sleep 2
done

# ── Phase 5: cutover ────────────────────────────────────────────────────
sigint_and_wait "$LOG_FILE"
assert_data_equal "$EXPECTED_TOTAL"

echo "PASS: online migration — two CaughtUp transitions, applied_lsn gate, content equal ($EXPECTED_TOTAL rows)"

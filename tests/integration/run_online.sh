#!/usr/bin/env bash
# End-to-end online migration test — strict zero-data-loss flow.
#
#   1. Seed source with 500 rows; create publication.
#   2. Launch examples/online_migration with a tiny lag threshold so any
#      pending WAL trips a FellBehind / JustCaughtUp transition.
#   3. Wait for the FIRST "ready for cutover" log line (initial catch-up).
#   4. Mutate the source heavily — INSERT 100 000, DELETE 1 000.
#   5. Wait for the SECOND "ready for cutover" — proves the stream has now
#      *received* all source WAL up to a captured LSN S.
#   6. Wait until a subsequent Lag heartbeat reports
#         applied_lsn >= S
#      — proves the target has *applied* everything up to S. This is the
#      strict zero-data-loss precondition for cutover.
#   7. Send SIGINT → graceful cutover.
#   8. Assert source and target row counts match the expected total AND that
#      a content hash (id|name|qty, ordered by id) is byte-for-byte equal.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

export SOURCE_URL="postgres://migrator:migrator@127.0.0.1:55432/appdb"
export TARGET_URL="postgres://migrator:migrator@127.0.0.1:55433/appdb"
# The CONNECTION clause inside CREATE SUBSCRIPTION is dialed by the target
# container's apply worker, not by us — `127.0.0.1:55432` would resolve to
# the target container's own loopback. Use the docker-compose service name.
export SUBSCRIPTION_SOURCE_URL="postgres://migrator:migrator@pg_migrator_source:5432/appdb"

source "$ROOT/tests/integration/lib.sh"

# Volume — 20× the previous run.
INSERT_END=100500       # generate_series(501, 100500)  → 100 000 inserts
DELETE_END=1000         # delete ids 1..1000           →   1 000 deletes
EXPECTED_TOTAL=$((500 + (INSERT_END - 500) - DELETE_END))   # = 99 500

LOG_FILE="$(mktemp -t pg_migrator_online.XXXXXX.log)"
echo "==> log file: $LOG_FILE"

cleanup() {
    if [[ -n "${MIGRATOR_PID:-}" ]] && kill -0 "$MIGRATOR_PID" 2>/dev/null; then
        echo "==> cleanup: terminating migrator pid $MIGRATOR_PID"
        kill -TERM "$MIGRATOR_PID" 2>/dev/null || true
        wait "$MIGRATOR_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

wait_for_pg "$SOURCE_URL" "source"
wait_for_pg "$TARGET_URL" "target"

echo "==> resetting source schema + creating publication"
PGPASSWORD=migrator psql -v ON_ERROR_STOP=1 -h 127.0.0.1 -p 55432 -U migrator -d appdb \
    -f "$ROOT/tests/integration/seed.sql" >/dev/null

echo "==> dropping any leftover replication slot"
psql_exec "$SOURCE_URL" "
DO \$\$
DECLARE r record;
BEGIN
    FOR r IN SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'pg_migrator%' LOOP
        EXECUTE format('SELECT pg_drop_replication_slot(%L)', r.slot_name);
    END LOOP;
END
\$\$;"

echo "==> resetting target schema"
psql_exec "$TARGET_URL" "DROP SCHEMA IF EXISTS app CASCADE;"

echo "==> building online_migration_example"
cargo build --quiet -p online_migration_example
BIN="$ROOT/target/debug/online_migration_example"

echo "==> launching migrator in background"
PG_MIGRATOR_SOURCE="$SOURCE_URL" \
PG_MIGRATOR_TARGET="$TARGET_URL" \
PG_MIGRATOR_SUBSCRIPTION_SOURCE="$SUBSCRIPTION_SOURCE_URL" \
PG_MIGRATOR_LAG_THRESHOLD_BYTES=64 \
PG_MIGRATOR_POLL_SECS=1 \
PG_MIGRATOR_FEEDBACK_SECS=1 \
PG_MIGRATOR_MAX_RUNTIME_SECS=900 \
RUST_LOG="info,pg_migrator=info,pg_walstream=warn" \
    stdbuf -oL -eL "$BIN" >"$LOG_FILE" 2>&1 &
MIGRATOR_PID=$!
echo "==> migrator pid: $MIGRATOR_PID"

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

# ── Phase 3: stream caught up (received_lsn >= source_lsn) ───────────────
echo "==> waiting for SECOND 'ready for cutover' (must be strictly after line $FIRST_LINE)"
SECOND_LINE=$(wait_for_log_match "$LOG_FILE" "ready for cutover" "$FIRST_LINE" 600)
echo "==> got second 'ready for cutover' on line $SECOND_LINE"

# ── Phase 4: gate cutover on target actually holding all rows ───────────
# We log the snapshotted (source_lsn, applied_lsn) at the moment of the
# second CaughtUp for forensics, but use the target row count as the
# *primary* zero-data-loss gate.
#
# Why not just wait for `applied_lsn >= source_lsn` from the heartbeat?
# pgoutput batches one transaction per Commit message: every INSERT inside
# our 100 000-row transaction streams with the *begin* LSN, so
# `stats.last_applied_lsn` only jumps to the commit LSN on the final Commit
# event — even though target rows are landing the whole time. Polling the
# target's actual row count is both cheaper and a more direct proof of
# "every change landed".
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

# ── Phase 5: operator-driven cutover via SIGINT. ─────────────────────────
echo "==> sending SIGINT to trigger cutover"
kill -INT "$MIGRATOR_PID"

echo "==> waiting for migrator to exit"
WAIT_DEADLINE=$(( $(date +%s) + 120 ))
while kill -0 "$MIGRATOR_PID" 2>/dev/null; do
    if (( $(date +%s) > WAIT_DEADLINE )); then
        echo "FAIL: migrator did not exit within 120s after SIGINT" >&2
        echo "---- log tail ----" >&2
        tail -n 80 "$LOG_FILE" >&2
        exit 1
    fi
    sleep 0.5
done
wait "$MIGRATOR_PID" 2>/dev/null || true
unset MIGRATOR_PID

if ! grep -q "migration done" "$LOG_FILE"; then
    echo "FAIL: migrator did not log a clean 'migration done'" >&2
    tail -n 80 "$LOG_FILE" >&2
    exit 1
fi

# ── Phase 6: assertions ──────────────────────────────────────────────────
src_count=$(query_count "$SOURCE_URL")
tgt_count=$(query_count "$TARGET_URL")
echo "==> source rows: $src_count  target rows: $tgt_count"

if [[ "$src_count" != "$EXPECTED_TOTAL" ]]; then
    echo "FAIL: source did not end at $EXPECTED_TOTAL rows (got $src_count)" >&2
    exit 1
fi
if [[ "$tgt_count" != "$EXPECTED_TOTAL" ]]; then
    echo "FAIL: target did not end at $EXPECTED_TOTAL rows (got $tgt_count)" >&2
    tail -n 120 "$LOG_FILE" >&2
    exit 1
fi

src_hash=$(content_hash "$SOURCE_URL")
tgt_hash=$(content_hash "$TARGET_URL")
echo "==> source content md5: $src_hash"
echo "==> target content md5: $tgt_hash"
if [[ "$src_hash" != "$tgt_hash" || -z "$src_hash" ]]; then
    echo "FAIL: source and target content hashes differ — data loss / drift" >&2
    exit 1
fi

echo "PASS: online migration — two CaughtUp transitions, applied_lsn gate, content equal ($EXPECTED_TOTAL rows)"

#!/usr/bin/env bash
# Shared helpers for the integration scripts.

# ═══════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

export SOURCE_URL="postgres://migrator:migrator@127.0.0.1:55432/appdb"
export TARGET_URL="postgres://migrator:migrator@127.0.0.1:55433/appdb"
export SUBSCRIPTION_SOURCE_URL="postgres://migrator:migrator@pg_migrator_source:5432/appdb"
export PGPASSWORD=migrator

PSQL_BASE=(psql -v ON_ERROR_STOP=1 -X -A -t)

# wait_for_pg URL name — block until the URL accepts connections.
wait_for_pg() {
    local url="$1"
    local name="$2"
    for _ in $(seq 1 60); do
        if psql "$url" -c "SELECT 1" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "timeout waiting for $name ($url)" >&2
    exit 1
}

# query_count URL — return count of rows in app.widgets.
query_count() {
    local url="$1"
    "${PSQL_BASE[@]}" "$url" -c "SELECT count(*) FROM app.widgets" \
        | tr -d '[:space:]'
}

# psql_exec URL "SQL" — execute a statement, fail on error.
psql_exec() {
    local url="$1"
    local sql="$2"
    "${PSQL_BASE[@]}" "$url" -c "$sql" >/dev/null
}

# wait_for_log_match LOGFILE PATTERN AFTER_LINE TIMEOUT_SECS
# — wait until LOGFILE has a line matching PATTERN strictly past line number
#   AFTER_LINE. Returns the new line number on success, fails on timeout.
wait_for_log_match() {
    local logfile="$1"
    local pattern="$2"
    local after_line="$3"
    local timeout_secs="$4"
    local deadline=$(( $(date +%s) + timeout_secs ))
    while (( $(date +%s) < deadline )); do
        local line_num
        # Match strictly past after_line; print the line number and exit on
        # the first hit.
        line_num=$(awk -v n="$after_line" -v p="$pattern" 'NR>n && $0 ~ p {print NR; exit}' "$logfile" 2>/dev/null || true)
        if [[ -n "$line_num" ]]; then
            echo "$line_num"
            return 0
        fi
        sleep 0.5
    done
    echo "timeout waiting for /$pattern/ in $logfile (after line $after_line)" >&2
    echo "---- log tail ----" >&2
    tail -n 60 "$logfile" >&2 || true
    return 1
}

# lag_heartbeat_at_or_before LOGFILE LINE
# — print "source_lsn applied_lsn" of the latest `replication lag` heartbeat
#   on or before LINE. Useful for snapshotting source_lsn at the moment a
#   CaughtUp transition fired.
lag_heartbeat_at_or_before() {
    local logfile="$1"
    local line="$2"
    awk -v n="$line" '
        NR <= n && /replication lag/ && /source LSN/ && /applied LSN/ {
            if (match($0, /source LSN [0-9]+/))   src=substr($0, RSTART+11, RLENGTH-11);
            if (match($0, /applied LSN [0-9]+/))  app=substr($0, RSTART+12, RLENGTH-12);
            last_src=src; last_app=app;
        }
        END { if (last_src != "") print last_src, last_app; }
    ' "$logfile" 2>/dev/null
}

# wait_for_applied_lsn LOGFILE AFTER_LINE TARGET_LSN TIMEOUT_SECS
# — block until any `replication lag` heartbeat strictly past AFTER_LINE
#   reports `applied_lsn >= TARGET_LSN`. This is the strict zero-data-loss
#   gate: it proves the target has executed every event up to the source's
#   WAL position at the moment we declared "ready for cutover", regardless
#   of any subsequent autovacuum-only WAL drift on the source.
wait_for_applied_lsn() {
    local logfile="$1"
    local after_line="$2"
    local target_lsn="$3"
    local timeout_secs="$4"
    local deadline=$(( $(date +%s) + timeout_secs ))
    while (( $(date +%s) < deadline )); do
        local result
        result=$(awk -v n="$after_line" '
            NR > n && /replication lag/ && /applied LSN/ {
                if (match($0, /applied LSN [0-9]+/)) app=substr($0, RSTART+12, RLENGTH-12);
                last_app=app; last_nr=NR;
            }
            END { if (last_nr) print last_nr, last_app; }
        ' "$logfile" 2>/dev/null || true)
        if [[ -n "$result" ]]; then
            local nr app
            read -r nr app <<<"$result"
            if [[ -n "$app" ]] && (( app >= target_lsn )); then
                echo "$nr $app"
                return 0
            fi
        fi
        sleep 0.5
    done
    echo "timeout waiting for applied_lsn >= $target_lsn in $logfile (after line $after_line)" >&2
    echo "---- log tail ----" >&2
    tail -n 60 "$logfile" >&2 || true
    return 1
}

# content_hash URL — md5 of the full ordered table contents. Used to prove
# byte-for-byte equality between source and target post-cutover.
content_hash() {
    local url="$1"
    "${PSQL_BASE[@]}" "$url" -c "
        SELECT md5(string_agg(id || '|' || name || '|' || qty, ',' ORDER BY id::int))
        FROM app.widgets" \
        | tr -d '[:space:]'
}

# query_seq_last_value URL SEQ_QUALIFIED_NAME
# — return `pg_sequence_last_value(SEQ::regclass)` as a trimmed string.
#   Returns the empty string when the sequence has never been advanced
#   (i.e. `is_called=false`), which is what PostgreSQL reports as NULL.
#   Use this anywhere we need to compare source/target sequence state
#   without duplicating an inline psql call.
query_seq_last_value() {
    local url="$1"
    local seq="$2"
    "${PSQL_BASE[@]}" "$url" \
        -c "SELECT pg_sequence_last_value('${seq}'::regclass)" \
        | tr -d '[:space:]'
}

# wait_for_table_count URL FQTABLE EXPECTED TIMEOUT_SECS
# — block until SELECT count(*) FROM FQTABLE on URL equals EXPECTED, or
#   fail after TIMEOUT_SECS. FQTABLE may be schema-qualified
#   (e.g. `app.events`). Echoes the matched count on success.
#
#   This is a generic counterpart to query_count (which is hard-coded to
#   app.widgets); use it for any new table the test cares about.
wait_for_table_count() {
    local url="$1"
    local fqtable="$2"
    local expected="$3"
    local timeout_secs="$4"
    local deadline=$(( $(date +%s) + timeout_secs ))
    local cur=""
    while (( $(date +%s) < deadline )); do
        cur=$("${PSQL_BASE[@]}" "$url" \
            -c "SELECT count(*) FROM ${fqtable}" 2>/dev/null \
            | tr -d '[:space:]')
        if [[ "$cur" == "$expected" ]]; then
            echo "$cur"
            return 0
        fi
        sleep 2
    done
    echo "timeout waiting for ${fqtable} count == ${expected} on ${url} (last=${cur})" >&2
    return 1
}

# start_mutations SOURCE_URL TICK_FILE
# — kick off a background loop that produces a mix of UPDATE / DELETE / INSERT
#   on app.widgets, exercising every replication path (not just INSERT).
#
#   * INSERT: ids 10000..   (fresh inserts, monotonic)
#   * UPDATE: rotating across the original 1..500 rows (changes name+qty)
#   * DELETE: rotating across the original 451..500 rows
#
#   The loop bumps a counter file (TICK_FILE) once per iteration so callers
#   can tell forward progress is being made. Stop the loop with
#   `stop_mutations` (matches by recorded PID).
#
# Sets the global MUTATION_LOOP_PID.
start_mutations() {
    local url="$1"
    local tick_file="$2"
    : > "$tick_file"
    (
        local i=0
        while :; do
            local ins_id=$((10000 + i))
            local upd_id=$((1 + (i % 500)))
            local del_id=$((451 + (i % 50)))
            psql -v ON_ERROR_STOP=0 -X -A -t "$url" -c "
                INSERT INTO app.widgets (id, name, qty)
                    VALUES ('$ins_id', 'ins-$ins_id', '$((i * 7))')
                    ON CONFLICT (id) DO NOTHING;
                UPDATE app.widgets SET name = 'upd-$i', qty = '$((i * 11))'
                    WHERE id = '$upd_id';
                DELETE FROM app.widgets WHERE id = '$del_id';
            " >/dev/null 2>&1 || true
            i=$((i + 1))
            echo "$i" > "$tick_file"
            sleep 0.05
        done
    ) &
    MUTATION_LOOP_PID=$!
}

# stop_mutations — terminate the background mutation loop started by
# `start_mutations`. Idempotent.
stop_mutations() {
    if [[ -n "${MUTATION_LOOP_PID:-}" ]] && kill -0 "$MUTATION_LOOP_PID" 2>/dev/null; then
        kill -TERM "$MUTATION_LOOP_PID" 2>/dev/null || true
        wait "$MUTATION_LOOP_PID" 2>/dev/null || true
    fi
    MUTATION_LOOP_PID=""
}

# wait_for_content_match SOURCE_URL TARGET_URL TIMEOUT_SECS
# — poll content_hash on both sides until they match (and are non-empty),
#   or fail after TIMEOUT_SECS. Emits the matching hash on success.
#
#   This is the strict pre-cutover gate for "source and target are
#   byte-for-byte identical right now".
wait_for_content_match() {
    local src="$1"
    local tgt="$2"
    local timeout_secs="$3"
    local deadline=$(( $(date +%s) + timeout_secs ))
    local last_src="" last_tgt=""
    while (( $(date +%s) < deadline )); do
        last_src=$(content_hash "$src")
        last_tgt=$(content_hash "$tgt")
        if [[ -n "$last_src" && "$last_src" == "$last_tgt" ]]; then
            echo "$last_src"
            return 0
        fi
        sleep 1
    done
    echo "timeout waiting for content_hash equality (source=$last_src target=$last_tgt)" >&2
    return 1
}

# ═══════════════════════════════════════════════════════════════════════════
# Setup helpers
# ═══════════════════════════════════════════════════════════════════════════

# seed_source — reset source schema via seed.sql (creates app.widgets with 500 rows).
seed_source() {
    echo "==> seeding source schema"
    psql -v ON_ERROR_STOP=1 -h 127.0.0.1 -p 55432 -U migrator -d appdb \
        -f "$ROOT/tests/integration/seed.sql" >/dev/null
}

# drop_replication_slots — drop all pg_migrator% replication slots on source.
drop_replication_slots() {
    echo "==> dropping any leftover replication slots"
    psql_exec "$SOURCE_URL" "
DO \$\$
DECLARE r record;
BEGIN
    FOR r IN SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'pg_migrator%' LOOP
        EXECUTE format('SELECT pg_drop_replication_slot(%L)', r.slot_name);
    END LOOP;
END
\$\$;"
}

# drop_subscriptions — drop all pg_migrator% subscriptions on target.
drop_subscriptions() {
    echo "==> dropping any leftover subscriptions on target"
    psql_exec "$TARGET_URL" "
DO \$\$
DECLARE r record;
BEGIN
    FOR r IN SELECT subname FROM pg_subscription WHERE subname LIKE 'pg_migrator%' LOOP
        EXECUTE format('ALTER SUBSCRIPTION %I DISABLE', r.subname);
        EXECUTE format('ALTER SUBSCRIPTION %I SET (slot_name = NONE)', r.subname);
        EXECUTE format('DROP SUBSCRIPTION %I', r.subname);
    END LOOP;
END
\$\$;"
}

# reset_target_schema — DROP SCHEMA IF EXISTS app CASCADE on target.
reset_target_schema() {
    echo "==> resetting target schema"
    psql_exec "$TARGET_URL" "DROP SCHEMA IF EXISTS app CASCADE;"
}

# setup_online_test — common preamble for online tests:
#   seed source + drop slots + drop subscriptions + reset target.
setup_online_test() {
    seed_source
    drop_replication_slots
    drop_subscriptions
    reset_target_schema
}

# build_example CRATE — build the example binary, set BIN= to its path.
build_example() {
    local crate="$1"
    echo "==> building $crate"
    cargo build --quiet -p "$crate"
    BIN="$ROOT/target/debug/$crate"
}

# launch_online_migrator LOGFILE [EXTRA_ENV...] — launch $BIN with standard
# online env vars, writing output to LOGFILE. Extra env vars can be passed
# as KEY=VALUE arguments to override defaults. Sets MIGRATOR_PID.
launch_online_migrator() {
    local logfile="$1"
    shift
    env \
        PG_MIGRATOR_SOURCE="$SOURCE_URL" \
        PG_MIGRATOR_TARGET="$TARGET_URL" \
        PG_MIGRATOR_SUBSCRIPTION_SOURCE="$SUBSCRIPTION_SOURCE_URL" \
        PG_MIGRATOR_LAG_THRESHOLD_BYTES="${PG_MIGRATOR_LAG_THRESHOLD_BYTES:-64}" \
        PG_MIGRATOR_POLL_SECS="${PG_MIGRATOR_POLL_SECS:-1}" \
        PG_MIGRATOR_FEEDBACK_SECS="${PG_MIGRATOR_FEEDBACK_SECS:-1}" \
        PG_MIGRATOR_MAX_RUNTIME_SECS="${PG_MIGRATOR_MAX_RUNTIME_SECS:-900}" \
        RUST_LOG="${RUST_LOG:-info,pg_migrator=info,pg_walstream=warn}" \
        "$@" \
        stdbuf -oL -eL "$BIN" >"$logfile" 2>&1 &
    MIGRATOR_PID=$!
    echo "==> migrator pid: $MIGRATOR_PID"
}

# wait_migrator_exit LOGFILE [TIMEOUT] — wait for MIGRATOR_PID to exit within
# TIMEOUT seconds (default 120). On timeout, print log tail and exit 1.
wait_migrator_exit() {
    local logfile="$1"
    local timeout="${2:-120}"
    local deadline=$(( $(date +%s) + timeout ))
    while kill -0 "$MIGRATOR_PID" 2>/dev/null; do
        if (( $(date +%s) > deadline )); then
            echo "FAIL: migrator did not exit within ${timeout}s" >&2
            tail -n 80 "$logfile" >&2
            exit 1
        fi
        sleep 0.5
    done
    wait "$MIGRATOR_PID" 2>/dev/null || true
    unset MIGRATOR_PID
}

# assert_migration_done LOGFILE — assert the log contains "migration done".
assert_migration_done() {
    local logfile="$1"
    if ! grep -q "migration done" "$logfile"; then
        echo "FAIL: migrator did not log 'migration done'" >&2
        tail -n 80 "$logfile" >&2
        exit 1
    fi
}

# assert_data_equal [EXPECTED_COUNT] — compare source/target row count and
# content hash. If EXPECTED_COUNT is given, also assert both sides have
# exactly that many rows. Fails with diagnostics on mismatch.
assert_data_equal() {
    local expected="${1:-}"
    local src_count tgt_count src_hash tgt_hash
    src_count=$(query_count "$SOURCE_URL")
    tgt_count=$(query_count "$TARGET_URL")
    src_hash=$(content_hash "$SOURCE_URL")
    tgt_hash=$(content_hash "$TARGET_URL")
    echo "==> POST-CUTOVER: src_rows=$src_count tgt_rows=$tgt_count"
    echo "==> POST-CUTOVER: src_hash=$src_hash tgt_hash=$tgt_hash"

    if [[ -n "$expected" && "$src_count" != "$expected" ]]; then
        echo "FAIL: source row count ($src_count) != expected ($expected)" >&2
        exit 1
    fi
    if [[ "$src_count" != "$tgt_count" ]]; then
        echo "FAIL: row counts diverged (src=$src_count tgt=$tgt_count)" >&2
        exit 1
    fi
    if [[ "$src_hash" != "$tgt_hash" || -z "$src_hash" ]]; then
        echo "FAIL: content hashes differ post-cutover" >&2
        exit 1
    fi
}

# sigint_and_wait LOGFILE [TIMEOUT] — send SIGINT to MIGRATOR_PID, wait for
# exit, and assert "migration done" in the log.
sigint_and_wait() {
    local logfile="$1"
    local timeout="${2:-120}"
    echo "==> sending SIGINT to trigger cutover"
    kill -INT "$MIGRATOR_PID"
    wait_migrator_exit "$logfile" "$timeout"
    assert_migration_done "$logfile"
}

# stop_migrator — kill MIGRATOR_PID if still running. Safe to call from traps.
stop_migrator() {
    if [[ -n "${MIGRATOR_PID:-}" ]] && kill -0 "$MIGRATOR_PID" 2>/dev/null; then
        echo "==> cleanup: terminating migrator pid $MIGRATOR_PID"
        kill -TERM "$MIGRATOR_PID" 2>/dev/null || true
        wait "$MIGRATOR_PID" 2>/dev/null || true
    fi
}

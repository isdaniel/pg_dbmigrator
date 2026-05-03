#!/usr/bin/env bash
# Shared helpers for the integration scripts.

PSQL_BASE=(psql -v ON_ERROR_STOP=1 -X -A -t)

# wait_for_pg URL name — block until the URL accepts connections.
wait_for_pg() {
    local url="$1"
    local name="$2"
    for _ in $(seq 1 60); do
        if PGPASSWORD=migrator psql "$url" -c "SELECT 1" >/dev/null 2>&1; then
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
    PGPASSWORD=migrator "${PSQL_BASE[@]}" "$url" -c "SELECT count(*) FROM app.widgets" \
        | tr -d '[:space:]'
}

# psql_exec URL "SQL" — execute a statement, fail on error.
psql_exec() {
    local url="$1"
    local sql="$2"
    PGPASSWORD=migrator "${PSQL_BASE[@]}" "$url" -c "$sql" >/dev/null
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
    PGPASSWORD=migrator "${PSQL_BASE[@]}" "$url" -c "
        SELECT md5(string_agg(id || '|' || name || '|' || qty, ',' ORDER BY id::int))
        FROM app.widgets" \
        | tr -d '[:space:]'
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
            PGPASSWORD=migrator psql -v ON_ERROR_STOP=0 -X -A -t "$url" -c "
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

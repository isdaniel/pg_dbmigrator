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

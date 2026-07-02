#!/usr/bin/env bash
# Standalone `--mode verify` test.
#
# Flow:
#   1. Migrate source -> target offline so they match (offline example).
#   2. Standalone `pg_dbmigrator --mode verify` on matching endpoints exits 0.
#   3. Introduce row-count drift on the target; `--mode verify` must exit != 0.
#
# `--mode verify` is a read-only compare provided by the pg_dbmigrator CLI
# binary (the example binaries do not expose it), so this test drives that
# binary directly for the verify steps.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
source "$ROOT/tests/integration/lib.sh"

LOG_FILE="$(mktemp -t pg_dbmigrator_verify_mode.XXXXXX.log)"
echo "==> log file: $LOG_FILE"

wait_for_pg "$SOURCE_URL" "source"
wait_for_pg "$TARGET_URL" "target"

seed_source
reset_target_schema

# ── Step 1: offline migration so source and target match ─────────────────
echo "==> running offline_migration_example to make target match source"
PG_DBMIGRATOR_SOURCE="$SOURCE_URL" \
PG_DBMIGRATOR_TARGET="$TARGET_URL" \
    cargo run --quiet -p offline_migration_example
assert_data_equal 500

# Build the CLI binary that exposes `--mode verify`.
echo "==> building pg_dbmigrator CLI binary"
cargo build --quiet -p pg_dbmigrator --bin pg_dbmigrator
BIN="$ROOT/target/debug/pg_dbmigrator"

# ── Step 2: standalone verify on matching endpoints must exit 0 ──────────
echo "==> running --mode verify on matching endpoints (expect exit 0)"
if ! PG_DBMIGRATOR_SOURCE="$SOURCE_URL" PG_DBMIGRATOR_TARGET="$TARGET_URL" \
        "$BIN" --mode verify 2>&1 | tee "$LOG_FILE"; then
    echo "FAIL: verify exited non-zero on matching endpoints" >&2
    tail -n 40 "$LOG_FILE" >&2
    exit 1
fi
# `tee` masks the exit status; re-check the pipeline result explicitly.
if [[ "${PIPESTATUS[0]}" -ne 0 ]]; then
    echo "FAIL: verify exited non-zero on matching endpoints" >&2
    tail -n 40 "$LOG_FILE" >&2
    exit 1
fi
if ! grep -qi "verify:.*table(s) matched" "$LOG_FILE"; then
    echo "FAIL: verify did not report a clean match on matching endpoints" >&2
    tail -n 40 "$LOG_FILE" >&2
    exit 1
fi
echo "PASS: verify clean exit on match"

# ── Step 3: introduce drift on the target; verify must exit non-zero ─────
echo "==> introducing row-count drift on target"
psql_exec "$TARGET_URL" \
    "INSERT INTO app.widgets (id, name, qty) VALUES ('verify-drift', 'drift', '0');"

echo "==> running --mode verify after drift (expect non-zero exit)"
if PG_DBMIGRATOR_SOURCE="$SOURCE_URL" PG_DBMIGRATOR_TARGET="$TARGET_URL" \
        "$BIN" --mode verify > "$LOG_FILE" 2>&1; then
    echo "FAIL: verify exited 0 despite row-count drift" >&2
    tail -n 40 "$LOG_FILE" >&2
    exit 1
fi
if ! grep -qi "verify:.*table(s) MISMATCHED" "$LOG_FILE"; then
    echo "FAIL: verify did not report a MISMATCH after drift" >&2
    tail -n 40 "$LOG_FILE" >&2
    exit 1
fi
echo "PASS: verify detects drift and exits non-zero"

echo "PASS: --mode verify — clean match exits 0, row-count drift exits non-zero"

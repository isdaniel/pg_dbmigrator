[![Crates.io Version](https://img.shields.io/crates/v/pg_dbmigrator)](https://crates.io/crates/pg_dbmigrator)
[![Crates.io Downloads (recent)](https://img.shields.io/crates/dr/pg_dbmigrator)](https://crates.io/crates/pg_dbmigrator)
[![Crates.io Total Downloads](https://img.shields.io/crates/d/pg_dbmigrator)](https://crates.io/crates/pg_dbmigrator)
[![docs.rs](https://img.shields.io/docsrs/pg_dbmigrator)](https://docs.rs/pg_dbmigrator)
[![codecov](https://codecov.io/gh/isdaniel/pg_dbmigrator/graph/badge.svg)](https://codecov.io/gh/isdaniel/pg_dbmigrator)

# pg_dbmigrator

A Rust library and CLI for migrating PostgreSQL databases between two
endpoints, a one-shot dump/restore for cold moves, and an online path that
keeps PostgreSQL's built-in logical replication apply worker pulling from
the source so the operator can cut over with near-zero downtime.

The online path issues `CREATE SUBSCRIPTION` on the target attached to a
slot we created with `EXPORT_SNAPSHOT` before `pg_dump` ran. 

## Modes

| Mode      | Behaviour                                                                                                                                                                                                          |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `offline` | Run `pg_dump` against the source, then `pg_restore` against the target. One-shot copy.                                                                                                                             |
| `online`  | Create a logical replication slot with `EXPORT_SNAPSHOT`, take a snapshot-consistent `pg_dump`, `pg_restore` it, then start a streaming WAL apply from the slot's start LSN until the operator triggers cutover.   |

### Online migration phases

```
Validate → PrepareSnapshot → Dump → Restore → StreamApply → (Lag heartbeat …) → CaughtUp → Cutover → Complete
```

* `PrepareSnapshot` creates the replication slot first; `START_REPLICATION`
  is deferred until **after** the dump completes, so the exported snapshot
  remains valid for the dump.
* During `StreamApply` the library polls
  `pg_current_wal_flush_lsn()` on the source every
  `--cutover-poll-secs` and emits a `Lag` progress event with
  `lag_bytes / source_lsn / received_lsn / applied_lsn`. This is the
  signal the customer watches to decide when to cut over.
* When the lag drops at or below `--lag-threshold-bytes` a one-shot
  `CaughtUp` event is emitted (“ready for cutover”).

## Install / build

```bash
# Install the CLI from source. Produces a binary called `pg_dbmigrator`.
cargo install pg_dbmigrator
pg_dbmigrator --help     
pg_dbmigrator --mode offline --source '…' --target '…' --jobs 4
```

## CLI

### Offline

```bash
cargo pg_dbmigrator \
    --mode offline \
    --source 'postgres://user:pw@src.example/db' \
    --target 'postgres://user:pw@dst.example/db' \
    --jobs 4 \
    --drop-target-first
```

### Online

On the source, before starting:

```sql
ALTER SYSTEM SET wal_level = 'logical';   -- requires restart
CREATE PUBLICATION pg_dbmigrator_pub FOR ALL TABLES;
```

```bash
cargo pg_dbmigrator \
    --mode online \
    --source 'postgres://user:pw@src/db' \
    --target 'postgres://user:pw@dst/db' \
    --slot-name pg_dbmigrator_slot \
    --publication pg_dbmigrator_pub \
    --subscription-name pg_dbmigrator_sub \
    --jobs 4 \
    --lag-threshold-bytes 8192 \
    --cutover-poll-secs 5
```

The library creates a subscription called `--subscription-name` (default
`pg_dbmigrator_sub`) on the target attached to the existing slot. On cutover
the subscription is disabled and, unless `--keep-subscription` is set,
dropped.

Before the dump runs, the migrator pre-flights the source:
`wal_level = 'logical'`, `max_replication_slots > 0`, and
`max_wal_senders > 0`. A misconfigured source fails fast with a clear
error instead of stalling later inside `CREATE_REPLICATION_SLOT`.

At cutover, the migrator runs `setval(...)` on every sequence in the
included schemas so the target picks up where the source left off —
otherwise the first `INSERT` after cutover would collide with rows the
subscription replicated. Disable with `--no-sequence-sync` if your
target role lacks privileges for `setval` on those sequences.

### Filtering

Use `--exclude-schema` and `--exclude-table` to omit large or transient
objects from the dump. Both flags accept multiple values.

```bash
cargo pg_dbmigrator --mode offline \
    --source ... --target ... \
    --exclude-schema audit \
    --exclude-table public.large_log
```

## Cutover (online mode)

Cutover is driven by `SIGINT` (Ctrl+C). The CLI prints a periodic `Lag` heartbeat after the dump completes, so the operator has a continuous bytes-behind read-out:

```
INFO stage=Lag replication lag 4096 bytes (source LSN …, received LSN …, applied LSN …)
INFO stage=Lag replication lag 1024 bytes (…)
INFO stage=CaughtUp target caught up with source (lag 512 bytes) — ready for cutover
```

When the customer is satisfied with the lag, they press **Ctrl+C** once:

* The signal handler calls `CutoverHandle::request()`.
* The streaming apply loop notices the request on its next poll, flushes
  the last LSN feedback to the source, emits a `Cutover` event, and
  returns.
* `Migrator::run` returns with `MigrationOutcome::cutover_triggered()
  == true`. The process exits cleanly. Application traffic can now be
  switched to the target.
* A second Ctrl+C is treated as an abort (escape hatch — only use it if
  the graceful path is stuck).

Cutover is always operator-driven; `--lag-threshold-bytes` is purely advisory and only controls when the one-shot `CaughtUp` "ready for cutover" event fires.

For online migrations, hold on to `migrator.cutover_handle()` and call `request()` from your own signal handler / RPC endpoint when the operator is ready to cut over. See [`examples/online_migration`](examples/online_migration) for a complete program that wires Ctrl+C to the cutover handle.

## Benchmark

See [BENCHMARK.md](BENCHMARK.md) for migration performance results across 10 GB – 200 GB datasets (PG 16 → PG 18, 8 parallel jobs, zstd compression).

## Known limitations

* The streaming apply loop binds replicated values as text and lets the
  server cast them. Custom column-level transforms are not supported.
* DDL changes are not migrated automatically — refresh the publication
  and restart the migration if the schema changes during the run.
* Extensions whose internal state cannot be re-created on the target
  (Azure-reserved extensions, pg_cron metadata, …) may cause
  `pg_restore` to exit with code 1. Pass `--allow-restore-errors` to
  treat that as a non-fatal warning when user data was restored
  successfully.
* Sequence sync at cutover requires the target role to have permission
  to call `setval()` on the destination sequences. Per-sequence
  failures are logged but do not abort cutover — inspect the warnings
  and re-`setval` manually if needed, or pre-grant `USAGE` on the
  sequences before the migration.

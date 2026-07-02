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
Validate → SourceVacuum → PrepareSnapshot → Dump → Restore → Analyze → StreamApply → (Lag heartbeat …) → CaughtUp → Cutover → SourceCleanup → Verify → Complete
```

* `Validate` pre-flights the source (`wal_level = 'logical'`,
  `max_replication_slots > 0`, `max_wal_senders > 0`) and ensures the
  required publication exists — auto-creating it if missing (see
  [Publication lifecycle](#publication--replication-resource-lifecycle)
  below).
* `SourceVacuum` runs `VACUUM ANALYZE` on the source to reclaim dead tuples
  and refresh planner statistics before the dump. Skip with `--skip-source-vacuum`.
* `PrepareSnapshot` creates the replication slot first; `START_REPLICATION`
  is deferred until **after** the dump completes, so the exported snapshot
  remains valid for the dump.
* `Analyze` runs `ANALYZE` on the target after restore so the query planner
  has fresh statistics for the first application queries. Skip with `--skip-analyze`.
* During `StreamApply` the library polls
  `pg_current_wal_flush_lsn()` on the source every
  `--cutover-poll-secs` and emits a `Lag` progress event with
  `lag_bytes / source_lsn / received_lsn / applied_lsn`. This is the
  signal the customer watches to decide when to cut over.
* When the lag drops at or below `--lag-threshold-bytes` a one-shot
  `CaughtUp` event is emitted (“ready for cutover”).
* `SourceCleanup` (after cutover) drops auto-created publications and
  replication slots on the source — see the next section.
* `Verify` compares per-table `count(*)` between source and target once the
  counts are stable. A mismatch is logged as a warning by default; pass
  `--verify-strict` to make it a hard (non-zero exit) error, or `--skip-verify`
  to skip the step.

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
pg_dbmigrator \
    --mode offline \
    --source 'postgres://user:pw@src.example/db' \
    --target 'postgres://user:pw@dst.example/db' \
    --jobs 4 \
    --drop-target-first
```

By default, `VACUUM ANALYZE` runs on the source before `pg_dump` and
`ANALYZE` runs on the target after `pg_restore`. Disable with
`--skip-source-vacuum` / `--skip-analyze` if you manage maintenance
externally.

Before the dump runs, offline migrations also pre-flight the target role's
privileges and extension availability (source extensions must be installable
on the target), so a misconfigured target fails fast with a clear error
instead of stalling inside `pg_restore`.

### Online

On the source, before starting:

```sql
ALTER SYSTEM SET wal_level = 'logical';   -- requires restart
```

The publication is auto-created by the migrator if it does not already
exist (default `FOR ALL TABLES`). If you prefer to create it manually —
e.g. to publish only specific tables — run:

```sql
CREATE PUBLICATION pg_dbmigrator_pub FOR TABLE my_schema.t1, my_schema.t2;
```

pass `--no-auto-create-publication` so the migrator uses the existing
one and does not attempt to create or drop it.

```bash
pg_dbmigrator \
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

Before the dump runs, the migrator pre-flights the source:
`wal_level = 'logical'`, `max_replication_slots > 0`,
`max_wal_senders > 0`, and extension availability (source extensions must be
installable on the target). A misconfigured source fails fast with a clear
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
pg_dbmigrator --mode offline \
    --source ... --target ... \
    --exclude-schema audit \
    --exclude-table public.large_log
```

### Verify

Compare per-table row counts between source and target. Runs automatically
after restore (offline) and after cutover (online); a mismatch is logged as a
warning by default. Use `--verify-strict` to make a mismatch a hard error
(non-zero exit) for CI, or `--skip-verify` to skip the step.

Standalone (read-only, no dump/restore) — always exits non-zero on mismatch:

```bash
pg_dbmigrator --mode verify \
    --source 'postgres://user:pw@src/db' \
    --target 'postgres://user:pw@dst/db' \
    --schema app
```

Honours `--schema` / `--table` / `--exclude-schema` / `--exclude-table` so the
verified object set matches what you migrated.

## Publication / replication resource lifecycle

The migrator fully manages the lifecycle of the replication resources it
creates, so the operator does not need to run manual cleanup SQL after a
successful cutover.

| Resource | Created by | Cleaned up at cutover | Override |
|---|---|---|---|
| Publication on source | Auto-created if missing (default) | Dropped only if it was auto-created | `--no-auto-create-publication` |
| Replication slot on source | Always created by the migrator | Dropped by default | `--keep-slot` |
| Subscription on target | Always created by the migrator | Dropped by default | `--keep-subscription` |

**Auto-create publication**: By default, the migrator checks whether the
named publication (`--publication`, default `pg_dbmigrator_pub`) exists on
the source. If it does not, the migrator creates it as `FOR ALL TABLES`
(or scoped to `--table` / `--schema` if specified). Auto-created
publications are tracked and dropped on the source after a successful
cutover. Pre-existing publications are never dropped.

**Slot cleanup**: After cutover, the replication slot on the source is no
longer needed. By default the migrator drops it. Pass `--keep-slot` if
you need to inspect the slot post-migration or if another consumer
shares it.

All cleanup steps are best-effort — failures are logged as warnings but
do not abort the migration.

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
* The migrator syncs sequences, cleans up replication resources (publication,
  slot, subscription), and returns with
  `MigrationOutcome::cutover_triggered() == true`. The process exits
  cleanly. Application traffic can now be switched to the target.
* A second Ctrl+C is treated as an abort (escape hatch — only use it if
  the graceful path is stuck).

Cutover is always operator-driven; `--lag-threshold-bytes` is purely advisory and only controls when the one-shot `CaughtUp` “ready for cutover” event fires.

For online migrations, hold on to `migrator.cutover_handle()` and call `request()` from your own signal handler / RPC endpoint when the operator is ready to cut over. See [`examples/online_migration`](examples/online_migration) for a complete program that wires Ctrl+C to the cutover handle.

## Performance defaults

The CLI ships with sensible defaults tuned for migration speed. Override
only when you have a specific reason.

| Default | Flag to override | Effect |
|---|---|---|
| Split-section restore | `--no-split-sections` | Bulk COPY without index maintenance, then rebuild indexes in parallel. 30-60% faster on index-heavy schemas. |
| `lz4:1` dump compression | `--dump-compress <spec>` | Negligible CPU, 3-5x smaller archive. Use `zstd:3` for better ratio, `none` to disable. |
| `--no-sync` on dump | `--keep-sync` | Skip fsync on transient dump files. |
| `--no-comments` | _(not exposed)_ | Omit COMMENT ON statements from dump. |
| `--no-security-labels` | _(not exposed)_ | Omit SE-Linux security labels from dump. |
| `--no-publications` | `--keep-publications` | Don't dump publication definitions to the target. |
| `--no-subscriptions` | `--keep-subscriptions` | Don't dump subscription definitions to the target. |
| Auto-detect `--jobs` | `--jobs N` | Clamps to `[1, 8]` based on host CPU count. |
| Pre-dump `VACUUM ANALYZE` | `--skip-source-vacuum` | Clean heap pages + fresh stats before dump. |
| Post-restore `ANALYZE` | `--skip-analyze` | Fresh planner stats on target immediately after restore. |
| Row-count verify | `--skip-verify` | Compare per-table `count(*)` source vs target; warn on mismatch. |
| Verify warns only | `--verify-strict` | Make a verify mismatch a hard (non-zero exit) error. |

## Benchmark

See [BENCHMARK.md](BENCHMARK.md) for migration performance results across 10 GB -- 200 GB datasets (PG 16 -> PG 18, 8 parallel jobs, zstd compression).

## Known limitations

* The streaming apply loop binds replicated values as text and lets the
  server cast them. Custom column-level transforms are not supported.
* DDL changes are not migrated automatically — refresh the publication
  and restart the migration if the schema changes during the run.
* Extensions whose internal state cannot be re-created on the target
  (Azure-reserved extensions, pg_cron metadata, ...) may cause
  `pg_restore` to exit with code 1. Pass `--allow-restore-errors` to
  treat that as a non-fatal warning when user data was restored
  successfully.
* Sequence sync at cutover requires the target role to have permission
  to call `setval()` on the destination sequences. Per-sequence
  failures are logged but do not abort cutover — inspect the warnings
  and re-`setval` manually if needed, or pre-grant `USAGE` on the
  sequences before the migration.

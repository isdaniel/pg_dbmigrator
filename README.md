# pg_migrator

A Rust library and CLI for migrating PostgreSQL databases between two
endpoints, a one-shot dump/restore for cold moves, and an online path that keeps a
logical replication stream applied to the target so the operator can cut
over with near-zero downtime.

The streaming apply phase is built on [`pg_walstream`](https://github.com/isdaniel/pg-walstream), and the overall orchestration model is inspired by

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
# Install the CLI from source. Produces a binary called `pg_migrator`.
cargo install --path crates/pg_migrator-cli

pg_migrator --mode offline --source '…' --target '…' --jobs 4
```

For development from a clone, the workspace ships a `cargo` alias so you
don't need `--bin` / `-p`:

```bash
cargo pg_migrator --help                      # equivalent to `cargo run --bin pg_migrator -- --help`
cargo pg_migrator --mode offline --source '…' --target '…'
```

## CLI

### Offline

```bash
cargo pg_migrator \
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
CREATE PUBLICATION pg_migrator_pub FOR ALL TABLES;
```

```bash
cargo pg_migrator \
    --mode online \
    --source 'postgres://user:pw@src/db' \
    --target 'postgres://user:pw@dst/db' \
    --slot-name pg_migrator_slot \
    --publication pg_migrator_pub \
    --jobs 4 \
    --lag-threshold-bytes 8192 \
    --cutover-poll-secs 5
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

## Library use

```rust
use pg_migrator::{
    EndpointConfig, MigrationConfig, MigrationMode, Migrator,
};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> pg_migrator::Result<()> {
    let cfg = MigrationConfig {
        mode: MigrationMode::Offline,
        source: EndpointConfig::parse("postgres://u:p@src/db")?,
        target: EndpointConfig::parse("postgres://u:p@dst/db")?,
        ..MigrationConfig::default()
    };

    Migrator::new(cfg)
        .run(CancellationToken::new())
        .await?;
    Ok(())
}
```

For online migrations, hold on to `migrator.cutover_handle()` and call `request()` from your own signal handler / RPC endpoint when the operator is ready to cut over. See [`examples/online_migration`](examples/online_migration) for a complete program that wires Ctrl+C to the cutover handle.

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

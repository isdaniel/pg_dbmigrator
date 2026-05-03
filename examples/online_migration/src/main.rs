//! Online migration example.
//!
//! 1. Create a logical replication slot on the source with `EXPORT_SNAPSHOT`.
//! 2. Run `pg_dump --snapshot=<id>` for a consistent baseline.
//! 3. `pg_restore` into the target.
//! 4. Issue `CREATE SUBSCRIPTION ... WITH (create_slot=false,
//!    slot_name='<existing>', enabled=true, copy_data=false)` on the target
//!    so PostgreSQL's built-in apply worker streams WAL from the slot. The
//!    library emits a periodic `Lag` heartbeat (lag_bytes, source/confirmed
//!    LSN) so the operator can decide when to cut over.
//! 5. On SIGINT (Ctrl+C) the apply loop performs a graceful cutover —
//!    `ALTER SUBSCRIPTION ... DISABLE` and (unless
//!    `PG_MIGRATOR_KEEP_SUBSCRIPTION=1`) `DROP SUBSCRIPTION`. A second
//!    Ctrl+C aborts immediately as an escape hatch.
//!
//! ## `lag_threshold_bytes` semantics
//!
//! `CutoverConfig::lag_threshold_bytes` is **advisory only**. When
//! `lag_bytes` first drops at or below it, the library emits a one-shot
//! `CaughtUp` ("ready for cutover") event so the operator knows it is the
//! cheapest moment to switch traffic. The bytes-behind `Lag` heartbeat is
//! still printed every `poll_interval` regardless of this threshold — that
//! is the continuous read-out the customer watches.
//!
//! Cutover is **always client-driven** — send SIGINT (Ctrl+C) when you are
//! ready to switch traffic. The threshold never causes the program to cut
//! over on its own.
//!
//! ## SIGINT timing note
//!
//! `CutoverHandle::request()` is only consumed once `StreamApply` is
//! running. If Ctrl+C is pressed *before* the apply loop starts (i.e. during
//! `PrepareSnapshot`, `Dump`, or `Restore`), the request flag is set but
//! has no effect on the in-flight stage; the dump/restore runs to
//! completion and the apply loop will then exit immediately on its first
//! poll. Pressing Ctrl+C twice during a pre-apply stage falls through to
//! the cancel branch below, which aborts the migration.
//!
//! Usage:
//! ```bash
//! PG_MIGRATOR_SOURCE="postgres://user:pw@src/db" \
//!     PG_MIGRATOR_TARGET="postgres://user:pw@dst/db" \
//!     cargo run -p online_migration_example
//! ```
//!
//! The source must have `wal_level=logical` and a publication that the slot
//! will use (default name: `pg_migrator_pub`). For example:
//! ```sql
//! CREATE PUBLICATION pg_migrator_pub FOR ALL TABLES;
//! ```

use std::env;
use std::time::Duration;

use anyhow::{Context, Result};
use pg_migrator::{
    config::DumpScope, CutoverConfig, EndpointConfig, MigrationConfig, MigrationMode, Migrator,
    OnlineOptions, ReplicationApplyConfig,
};
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,pg_migrator=info,pg_walstream=info")),
        )
        .with_target(false)
        .init();

    let source = env::var("PG_MIGRATOR_SOURCE").context("PG_MIGRATOR_SOURCE must be set")?;
    let target = env::var("PG_MIGRATOR_TARGET").context("PG_MIGRATOR_TARGET must be set")?;

    let online = OnlineOptions {
        slot_name: env::var("PG_MIGRATOR_SLOT_NAME").unwrap_or_else(|_| "pg_migrator_slot".into()),
        publication: env::var("PG_MIGRATOR_PUBLICATION")
            .unwrap_or_else(|_| "pg_migrator_pub".into()),
        protocol_version: 2,
        subscription_name: env::var("PG_MIGRATOR_SUBSCRIPTION_NAME")
            .unwrap_or_else(|_| "pg_migrator_sub".into()),
        // Set this when the target's apply worker reaches the source via a
        // different address than the migrator (e.g. Docker service name vs.
        // host loopback). Defaults to `PG_MIGRATOR_SOURCE`.
        subscription_source_conn: env::var("PG_MIGRATOR_SUBSCRIPTION_SOURCE").ok(),
        drop_subscription_on_cutover: !env_flag("PG_MIGRATOR_KEEP_SUBSCRIPTION"),
        force_clean: env_flag("PG_MIGRATOR_FORCE_CLEAN"),
        apply: ReplicationApplyConfig {
            feedback_interval: Duration::from_secs(env_secs("PG_MIGRATOR_FEEDBACK_SECS", 5)),
            connection_timeout: Duration::from_secs(15),
            health_check_interval: Duration::from_secs(30),
            max_runtime_seconds: env::var("PG_MIGRATOR_MAX_RUNTIME_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok()),
        },
        cutover: CutoverConfig {
            poll_interval: Duration::from_secs(env_secs("PG_MIGRATOR_POLL_SECS", 5)),
            lag_threshold_bytes: env::var("PG_MIGRATOR_LAG_THRESHOLD_BYTES")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(8 * 1024),
        },
    };

    let config = MigrationConfig {
        mode: MigrationMode::Online,
        source: EndpointConfig::parse(&source)?,
        target: EndpointConfig::parse(&target)?,
        dump_scope: DumpScope::All,
        drop_target_first: true,
        jobs: 4,
        online,
        ..MigrationConfig::default()
    };

    info!(
        source = %config.source.redacted(),
        target = %config.target.redacted(),
        "starting online migration"
    );

    let cancel = CancellationToken::new();
    let migrator = Migrator::new(config);
    let cutover = migrator.cutover_handle();

    let cancel_for_signal = cancel.clone();
    tokio::spawn(async move {
        loop {
            if tokio::signal::ctrl_c().await.is_err() {
                return;
            }
            if !cutover.is_requested() {
                info!(
                    "Ctrl+C received — requesting graceful cutover; \
                     press Ctrl+C again to abort"
                );
                cutover.request();
            } else {
                info!("Ctrl+C received again — cancelling");
                cancel_for_signal.cancel();
                return;
            }
        }
    });

    let outcome = migrator.run(cancel).await?;
    info!(
        ?outcome.stats,
        cutover_triggered = outcome.cutover_triggered(),
        dump_path = %outcome.dump_path.display(),
        "migration done"
    );
    Ok(())
}

/// Read an integer-seconds env var, falling back to `default` if absent or
/// unparseable. Lets integration tests dial poll/feedback intervals down to
/// values that produce observable lag transitions in seconds rather than
/// minutes.
fn env_secs(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

/// Parse a boolean-ish env var (`1`, `true`, `yes`).
fn env_flag(name: &str) -> bool {
    matches!(
        env::var(name).ok().as_deref().map(str::trim),
        Some("1") | Some("true") | Some("yes") | Some("on")
    )
}

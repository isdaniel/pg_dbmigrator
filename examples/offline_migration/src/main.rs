//! Offline migration example.
//!
//! Builds a [`MigrationConfig`] in offline mode and runs the migration with
//! Ctrl+C cancellation support.
//!
//! Usage:
//! ```bash
//! PG_MIGRATOR_SOURCE="postgres://user:pw@src/db" \
//!     PG_MIGRATOR_TARGET="postgres://user:pw@dst/db" \
//!     cargo run -p offline_migration_example
//! ```

use std::env;

use anyhow::{Context, Result};
use pg_migrator::{config::DumpScope, EndpointConfig, MigrationConfig, MigrationMode, Migrator};
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,pg_migrator=info")),
        )
        .with_target(false)
        .init();

    let source = env::var("PG_MIGRATOR_SOURCE").context("PG_MIGRATOR_SOURCE must be set")?;
    let target = env::var("PG_MIGRATOR_TARGET").context("PG_MIGRATOR_TARGET must be set")?;

    let config = MigrationConfig {
        mode: MigrationMode::Offline,
        source: EndpointConfig::parse(&source)?,
        target: EndpointConfig::parse(&target)?,
        dump_scope: DumpScope::All,
        drop_target_first: true,
        jobs: 4,
        ..MigrationConfig::default()
    };

    info!(
        source = %config.source.redacted(),
        target = %config.target.redacted(),
        "starting offline migration"
    );

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        cancel_clone.cancel();
    });

    let migrator = Migrator::new(config);
    let outcome = migrator.run(cancel).await?;
    info!(dump_path = %outcome.dump_path.display(), "migration done");
    Ok(())
}

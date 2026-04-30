mod args;

use anyhow::Context;
use args::Cli;
use clap::Parser;
use pg_migrator::{CutoverHandle, MigrationMode, Migrator};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let cli = Cli::parse();
    let config = cli
        .into_config()
        .context("failed to translate CLI args into MigrationConfig")?;
    let mode = config.mode;
    let migrator = Migrator::new(config);

    let cancel = CancellationToken::new();
    spawn_signal_handler(mode, migrator.cutover_handle(), cancel.clone());

    if mode == MigrationMode::Online {
        info!(
            "online mode: send SIGINT (Ctrl+C) once `CaughtUp` is logged to \
             trigger cutover and graceful shutdown"
        );
    }

    match migrator.run(cancel).await {
        Ok(out) => {
            info!(
                dump_path = %out.dump_path.display(),
                stats = ?out.stats,
                cutover_triggered = out.cutover_triggered(),
                "migration finished"
            );
            Ok(())
        }
        Err(e) => {
            error!(error = %e, "migration failed");
            Err(anyhow::anyhow!(e.to_string()))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SigintAction {
    /// Online mode, first Ctrl+C: request a graceful cutover. The apply loop
    /// flushes LSN feedback and exits with `cutover_triggered = true`.
    RequestCutover,
    /// Offline mode, or second Ctrl+C in online mode: cancel the migration.
    Cancel,
}

fn classify_sigint(mode: MigrationMode, cutover_already_requested: bool) -> SigintAction {
    match (mode, cutover_already_requested) {
        (MigrationMode::Online, false) => SigintAction::RequestCutover,
        _ => SigintAction::Cancel,
    }
}

fn spawn_signal_handler(mode: MigrationMode, handle: CutoverHandle, cancel: CancellationToken) {
    tokio::spawn(async move {
        loop {
            if let Err(e) = tokio::signal::ctrl_c().await {
                error!(error = %e, "ctrl-c handler failed");
                return;
            }
            match classify_sigint(mode, handle.is_requested()) {
                SigintAction::RequestCutover => {
                    info!(
                        "Ctrl+C received — requesting graceful cutover; \
                         press Ctrl+C again to abort"
                    );
                    handle.request();
                }
                SigintAction::Cancel => {
                    info!("Ctrl+C received — cancelling migration");
                    cancel.cancel();
                    return;
                }
            }
        }
    });
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,pg_migrator=info,pg_walstream=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offline_sigint_always_cancels() {
        assert_eq!(
            classify_sigint(MigrationMode::Offline, false),
            SigintAction::Cancel
        );
        assert_eq!(
            classify_sigint(MigrationMode::Offline, true),
            SigintAction::Cancel
        );
    }

    #[test]
    fn online_first_sigint_requests_cutover() {
        assert_eq!(
            classify_sigint(MigrationMode::Online, false),
            SigintAction::RequestCutover
        );
    }

    #[test]
    fn online_second_sigint_cancels() {
        assert_eq!(
            classify_sigint(MigrationMode::Online, true),
            SigintAction::Cancel
        );
    }
}

//! High-level migration driver.
//!
//! [`Migrator`] takes a [`MigrationConfig`] and runs the appropriate sequence
//! of dump → restore → (optional) streaming apply.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio_postgres::Client;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::{MigrationConfig, MigrationMode};
use crate::cutover::CutoverHandle;
use crate::dump::{run_pg_dump, CommandRunner, DumpFormat, DumpRequest, TokioCommandRunner};
use crate::error::{MigrationError, Result};
use crate::preflight::verify_pg_tools_installed;
use crate::progress::{MigrationStage, ProgressEvent, ProgressReporter, TracingReporter};
use crate::replicate::{
    run_streaming_apply, ApplyDeps, ApplyStats, PostgresLsnProvider, SourceLsnProvider,
};
use crate::restore::{run_pg_restore, RestoreRequest};
use crate::snapshot::prepare_replication_slot;
use crate::tls::connect_with_sslmode;

/// High-level migration driver.
#[derive(Debug)]
pub struct Migrator {
    config: MigrationConfig,
    runner: Arc<dyn CommandRunner>,
    reporter: Arc<dyn ProgressReporter>,
    /// Optional override for the dump archive path. Defaults to a `tempfile`
    /// inside `std::env::temp_dir()`.
    dump_path: Option<PathBuf>,
    /// Operator-facing handle for triggering cutover.
    cutover_handle: CutoverHandle,
}

impl Migrator {
    /// Construct a [`Migrator`] with the production defaults: the dump and
    /// restore are spawned via [`tokio::process::Command`] and progress is
    /// logged through the `tracing` subscriber.
    pub fn new(config: MigrationConfig) -> Self {
        Self {
            config,
            runner: Arc::new(TokioCommandRunner),
            reporter: Arc::new(TracingReporter),
            dump_path: None,
            cutover_handle: CutoverHandle::new(),
        }
    }

    /// Replace the [`CommandRunner`] used to invoke `pg_dump` / `pg_restore`.
    pub fn with_runner(mut self, runner: Arc<dyn CommandRunner>) -> Self {
        self.runner = runner;
        self
    }

    /// Replace the [`ProgressReporter`].
    pub fn with_reporter(mut self, reporter: Arc<dyn ProgressReporter>) -> Self {
        self.reporter = reporter;
        self
    }

    /// Pin the dump archive output path (otherwise it is generated in
    /// `std::env::temp_dir()`).
    pub fn with_dump_path(mut self, path: PathBuf) -> Self {
        self.dump_path = Some(path);
        self
    }

    /// Get a clone of the cutover handle. Hand this to a signal handler / RPC
    /// endpoint / UI so the operator can call
    /// [`CutoverHandle::request`] when ready to switch traffic to the target.
    pub fn cutover_handle(&self) -> CutoverHandle {
        self.cutover_handle.clone()
    }

    /// Get a read-only reference to the currently active configuration.
    pub fn config(&self) -> &MigrationConfig {
        &self.config
    }

    /// Run the migration pipeline.
    ///
    /// `cancel` lets the caller request a graceful shutdown — particularly
    /// important during the long-running streaming apply phase of an online
    /// migration.
    pub async fn run(&self, cancel: CancellationToken) -> Result<MigrationOutcome> {
        self.config.validate()?;
        verify_pg_tools_installed().await?;
        self.report(MigrationStage::Validate, "configuration valid")
            .await;

        match self.config.mode {
            MigrationMode::Offline => self.run_offline(cancel).await,
            MigrationMode::Online => self.run_online(cancel).await,
        }
    }

    /// Offline path: `pg_dump` → `pg_restore`.
    async fn run_offline(&self, cancel: CancellationToken) -> Result<MigrationOutcome> {
        let dump_path = self.dump_path_or_default("dump_offline");

        self.report(MigrationStage::Dump, "starting pg_dump").await;
        run_pg_dump(self.runner.as_ref(), &self.dump_request(&dump_path, None)).await?;
        if cancel.is_cancelled() {
            return Err(MigrationError::Cancelled);
        }

        self.report(MigrationStage::Restore, "starting pg_restore")
            .await;
        run_pg_restore(self.runner.as_ref(), &self.restore_request(&dump_path)).await?;

        self.report(MigrationStage::Complete, "offline migration finished")
            .await;
        Ok(MigrationOutcome {
            stats: None,
            dump_path,
        })
    }

    /// Online path: slot + snapshot → snapshot-aligned dump → restore →
    /// streaming apply.
    async fn run_online(&self, cancel: CancellationToken) -> Result<MigrationOutcome> {
        // 1. Prepare slot + snapshot (must happen *before* pg_dump runs).
        self.report(MigrationStage::PrepareSnapshot, "creating replication slot")
            .await;
        let prepared =
            prepare_replication_slot(&self.config.source.connection_string, &self.config.online)
                .await?;
        let snapshot_name = prepared.snapshot_name.clone();

        // 2. Snapshot-aligned dump.
        let dump_path = self.dump_path_or_default("dump_online");
        self.report(
            MigrationStage::Dump,
            format!(
                "starting pg_dump with snapshot {}",
                snapshot_name.as_deref().unwrap_or("<unknown>")
            ),
        )
        .await;
        run_pg_dump(
            self.runner.as_ref(),
            &self.dump_request(&dump_path, snapshot_name.clone()),
        )
        .await?;
        if cancel.is_cancelled() {
            return Err(MigrationError::Cancelled);
        }

        // 3. Restore.
        self.report(MigrationStage::Restore, "starting pg_restore")
            .await;
        run_pg_restore(self.runner.as_ref(), &self.restore_request(&dump_path)).await?;
        if cancel.is_cancelled() {
            return Err(MigrationError::Cancelled);
        }

        // 4. Streaming apply.
        let target_client = self.connect_target().await?;
        self.report(MigrationStage::StreamApply, "starting WAL streaming apply")
            .await;

        // Open a *non-replication* connection to the source for LSN polling.
        // We swallow errors here because lag detection is best-effort: the
        // apply loop is still useful (just without "caught up" reporting) if
        // the LSN poller fails to connect.
        let lsn_provider: Option<Arc<dyn SourceLsnProvider>> =
            match PostgresLsnProvider::connect(&self.config.source.connection_string).await {
                Ok(p) => Some(Arc::new(p)),
                Err(e) => {
                    warn!(error = %e, "could not open LSN polling connection — \
                          lag detection disabled");
                    None
                }
            };

        let deps = ApplyDeps {
            apply_cfg: &self.config.online.apply,
            cutover_cfg: &self.config.online.cutover,
            cutover_handle: self.cutover_handle.clone(),
            lsn_provider: lsn_provider.as_ref().map(|p| p.as_ref()),
            reporter: self.reporter.as_ref(),
        };
        let stats = run_streaming_apply(prepared.stream, &target_client, deps, cancel).await?;

        self.report(MigrationStage::Complete, "online migration finished")
            .await;
        Ok(MigrationOutcome {
            stats: Some(stats),
            dump_path,
        })
    }

    fn dump_request(&self, dump_path: &Path, snapshot: Option<String>) -> DumpRequest {
        // Custom format dump → fastest pg_restore; directory format if user
        // has asked for >1 jobs. We default to Custom to avoid surprising the
        // operator with a directory archive.
        let format = if self.config.jobs > 1 {
            DumpFormat::Directory
        } else {
            DumpFormat::Custom
        };
        DumpRequest {
            source: self.config.source.clone(),
            scope: self.config.dump_scope,
            jobs: self.config.jobs,
            snapshot,
            schemas: self.config.schemas.clone(),
            tables: self.config.tables.clone(),
            output_path: dump_path.to_path_buf(),
            format,
            no_publications: self.config.no_publications,
            no_subscriptions: self.config.no_subscriptions,
        }
    }

    fn restore_request(&self, dump_path: &Path) -> RestoreRequest {
        RestoreRequest {
            target: self.config.target.clone(),
            input_path: dump_path.to_path_buf(),
            jobs: self.config.jobs,
            clean: self.config.drop_target_first,
            no_owner: true,
            no_acl: true,
            tolerate_errors: self.config.allow_restore_errors,
        }
    }

    fn dump_path_or_default(&self, prefix: &str) -> PathBuf {
        if let Some(p) = &self.dump_path {
            return p.clone();
        }
        let mut p = std::env::temp_dir();
        p.push(format!("{prefix}-{}", std::process::id()));
        p
    }

    async fn connect_target(&self) -> Result<Client> {
        info!("connecting to target {}", self.config.target.redacted());
        connect_with_sslmode(&self.config.target.connection_string).await
    }

    async fn report(&self, stage: MigrationStage, message: impl Into<String>) {
        self.reporter
            .report(ProgressEvent::new(stage, message.into()))
            .await;
    }
}

/// Aggregate result of a single migration run.
#[derive(Debug, Clone)]
pub struct MigrationOutcome {
    /// Streaming apply statistics (only present for online migrations).
    pub stats: Option<ApplyStats>,
    /// Final dump archive path (kept on disk for inspection / re-runs).
    pub dump_path: PathBuf,
}

impl MigrationOutcome {
    /// Whether the online apply loop ended because cutover was triggered
    /// (operator-driven or auto). Always `false` for offline migrations.
    pub fn cutover_triggered(&self) -> bool {
        self.stats.map(|s| s.cutover_triggered).unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{EndpointConfig, OnlineOptions};
    use crate::dump::{CommandRunner, DumpFormat};
    use crate::progress::CollectingReporter;
    use async_trait::async_trait;
    use std::sync::Mutex;

    /// Records every command dispatched without spawning real processes.
    #[derive(Debug, Default)]
    struct RecordingRunner {
        calls: Mutex<Vec<(String, Vec<String>)>>,
    }

    impl RecordingRunner {
        fn snapshot(&self) -> Vec<(String, Vec<String>)> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl CommandRunner for RecordingRunner {
        async fn run(
            &self,
            program: &str,
            args: &[String],
            _env: &[(String, String)],
        ) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push((program.to_string(), args.to_vec()));
            Ok(())
        }
    }

    fn baseline_config() -> MigrationConfig {
        MigrationConfig {
            source: EndpointConfig::parse("postgres://u:p@src/db").unwrap(),
            target: EndpointConfig::parse("postgres://u:p@dst/db").unwrap(),
            ..MigrationConfig::default()
        }
    }

    #[tokio::test]
    async fn offline_run_invokes_dump_then_restore() {
        let runner = Arc::new(RecordingRunner::default());
        let reporter = Arc::new(CollectingReporter::new());
        let migrator = Migrator::new(baseline_config())
            .with_runner(runner.clone())
            .with_reporter(reporter.clone())
            .with_dump_path(PathBuf::from("/tmp/pg_migrator_test_dump"));

        migrator
            .run(CancellationToken::new())
            .await
            .expect("offline migration should succeed");

        let calls = runner.snapshot();
        assert_eq!(calls.len(), 2, "expected 2 calls (dump+restore)");
        assert_eq!(calls[0].0, "pg_dump");
        assert_eq!(calls[1].0, "pg_restore");

        let stages: Vec<_> = reporter
            .events()
            .await
            .into_iter()
            .map(|e| e.stage)
            .collect();
        assert!(stages.contains(&MigrationStage::Validate));
        assert!(stages.contains(&MigrationStage::Dump));
        assert!(stages.contains(&MigrationStage::Restore));
        assert!(stages.contains(&MigrationStage::Complete));
    }

    #[tokio::test]
    async fn validation_failure_short_circuits() {
        let cfg = MigrationConfig {
            jobs: 0,
            ..baseline_config()
        };
        let migrator = Migrator::new(cfg);
        let err = migrator.run(CancellationToken::new()).await.unwrap_err();
        assert!(matches!(err, MigrationError::Config(_)));
    }

    #[test]
    fn dump_request_uses_directory_format_for_parallel_jobs() {
        let cfg = MigrationConfig {
            jobs: 4,
            ..baseline_config()
        };
        let m = Migrator::new(cfg);
        let req = m.dump_request(Path::new("/tmp/dump"), None);
        assert_eq!(req.format, DumpFormat::Directory);
    }

    #[test]
    fn dump_request_uses_custom_format_for_single_job() {
        let cfg = MigrationConfig {
            jobs: 1,
            ..baseline_config()
        };
        let m = Migrator::new(cfg);
        let req = m.dump_request(Path::new("/tmp/dump"), None);
        assert_eq!(req.format, DumpFormat::Custom);
    }

    #[test]
    fn online_validation_inherits_offline_checks() {
        let cfg = MigrationConfig {
            mode: MigrationMode::Online,
            online: OnlineOptions {
                slot_name: "".into(),
                ..OnlineOptions::default()
            },
            ..baseline_config()
        };
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, MigrationError::Config(_)));
    }

    #[test]
    fn cutover_handle_is_clonable_and_stable_across_calls() {
        let m = Migrator::new(baseline_config());
        let h1 = m.cutover_handle();
        let h2 = m.cutover_handle();
        assert!(!h1.is_requested());
        h1.request();
        // Both clones share state with the migrator's internal handle.
        assert!(h2.is_requested());
    }

    #[test]
    fn migration_outcome_cutover_triggered_reflects_stats() {
        let mut out = MigrationOutcome {
            stats: None,
            dump_path: PathBuf::from("/tmp/x"),
        };
        assert!(!out.cutover_triggered()); // offline: always false

        out.stats = Some(ApplyStats {
            cutover_triggered: true,
            ..ApplyStats::default()
        });
        assert!(out.cutover_triggered());
    }
}

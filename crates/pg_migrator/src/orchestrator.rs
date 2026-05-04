//! High-level migration driver.
//!
//! [`Migrator`] takes a [`MigrationConfig`] and runs the appropriate sequence
//! of dump → restore → (optional) streaming apply.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio_postgres::Client;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::{MigrationConfig, MigrationMode};
use crate::cutover::CutoverHandle;
use crate::dump::{run_pg_dump, CommandRunner, DumpFormat, DumpRequest, TokioCommandRunner};
use crate::error::{MigrationError, Result};
use crate::native_apply::{
    cleanup_target_subscription, force_clean_stale_state, run_native_apply, ApplyStats,
    PgSubscriptionLagProvider,
};
use crate::preflight::{
    ensure_pglogical_not_interfering, ensure_target_database_exists, verify_pg_tools_installed,
    verify_publication_exists,
};
use crate::progress::{MigrationStage, ProgressEvent, ProgressReporter, TracingReporter};
use crate::restore::{run_pg_restore, run_pg_restore_in_sections, RestoreRequest};
use crate::resume::{default_resume_path, CompletedStage, ResumeToken};
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
        let mut token = self.load_or_init_resume(&dump_path).await?;

        if !token.has(CompletedStage::Dump) {
            self.report(MigrationStage::Dump, "starting pg_dump").await;
            run_pg_dump(
                self.runner.as_ref(),
                &self.dump_request(&dump_path, None),
                &cancel,
            )
            .await?;
            if cancel.is_cancelled() {
                return Err(MigrationError::Cancelled);
            }
            token.mark(CompletedStage::Dump);
            self.save_resume(&token, &dump_path).await;
        } else {
            self.report(
                MigrationStage::Dump,
                "skipped (resume): pg_dump already complete",
            )
            .await;
        }

        if !token.has(CompletedStage::Restore) {
            self.report(MigrationStage::Restore, "starting pg_restore")
                .await;
            self.restore(&dump_path, &cancel).await?;
            token.mark(CompletedStage::Restore);
            self.save_resume(&token, &dump_path).await;
        } else {
            self.report(
                MigrationStage::Restore,
                "skipped (resume): pg_restore already complete",
            )
            .await;
        }

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
        // 0. Optional best-effort cleanup of leftovers from a previous run.
        if self.config.online.force_clean {
            self.report(
                MigrationStage::Validate,
                "force-clean: dropping any stale subscription/slot",
            )
            .await;
            force_clean_stale_state(
                &self.config.source.connection_string,
                &self.config.target.connection_string,
                &self.config.online,
            )
            .await?;
        }

        // 0.5. Ensure the target database exists — pg_restore needs it.
        self.report(
            MigrationStage::Validate,
            format!(
                "ensuring target database `{}` exists",
                self.config.target.database
            ),
        )
        .await;
        ensure_target_database_exists(
            &self.config.target.connection_string,
            &self.config.target.database,
        )
        .await?;

        let dump_path = self.dump_path_or_default("dump_online");
        let mut token = self.load_or_init_resume(&dump_path).await?;

        // When resuming past Dump, the slot was created in a previous run
        // and the exported snapshot is already gone — there is no live
        // stream to keep. We only call `prepare_replication_slot` (and
        // hold a stream) when we still need to run pg_dump.
        let mut prepared_stream = None;
        let snapshot_name = if !token.has(CompletedStage::Dump) {
            // Fail fast if the publication is missing. Without this check
            // the apply worker would only error out 10+ minutes later
            // (after dump+restore) from inside `CREATE SUBSCRIPTION`.
            self.report(
                MigrationStage::Validate,
                format!(
                    "verifying publication `{}` exists on source",
                    self.config.online.publication
                ),
            )
            .await;
            verify_publication_exists(
                &self.config.source.connection_string,
                &self.config.online.publication,
            )
            .await?;

            // 1. Prepare slot + snapshot (must happen *before* pg_dump runs).
            self.report(MigrationStage::PrepareSnapshot, "creating replication slot")
                .await;
            let prepared = prepare_replication_slot(
                &self.config.source.connection_string,
                &self.config.online,
            )
            .await?;
            let snap = prepared.snapshot_name.clone();
            prepared_stream = Some(prepared.stream);
            token.mark(CompletedStage::PrepareSnapshot);
            token.snapshot_name = snap.clone();
            self.save_resume(&token, &dump_path).await;
            snap
        } else {
            self.report(
                MigrationStage::PrepareSnapshot,
                "skipped (resume): slot/snapshot already prepared in previous run",
            )
            .await;
            token.snapshot_name.clone()
        };

        // 2. Snapshot-aligned dump.
        if !token.has(CompletedStage::Dump) {
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
                &cancel,
            )
            .await?;
            if cancel.is_cancelled() {
                return Err(MigrationError::Cancelled);
            }
            token.mark(CompletedStage::Dump);
            self.save_resume(&token, &dump_path).await;
        } else {
            self.report(
                MigrationStage::Dump,
                "skipped (resume): pg_dump already complete",
            )
            .await;
        }

        // 3. Restore.
        if !token.has(CompletedStage::Restore) {
            self.report(MigrationStage::Restore, "starting pg_restore")
                .await;
            self.restore(&dump_path, &cancel).await?;
            if cancel.is_cancelled() {
                return Err(MigrationError::Cancelled);
            }
            token.mark(CompletedStage::Restore);
            self.save_resume(&token, &dump_path).await;
        } else {
            self.report(
                MigrationStage::Restore,
                "skipped (resume): pg_restore already complete",
            )
            .await;
        }

        // 4. Streaming apply via `CREATE SUBSCRIPTION` on the target. The
        // pg_walstream stream's only job was to keep the exported snapshot
        // alive across pg_dump; the slot itself persists on the source
        // independently of the stream connection, so we drop the stream
        // before handing the slot to the native apply worker.
        drop(prepared_stream);

        // When resuming into the apply phase a previous (crashed) run may
        // already have created the subscription. Drop just the
        // subscription (the slot stays — it's where we'll resume from).
        if self.config.resume {
            cleanup_target_subscription(&self.config.target.connection_string, &self.config.online)
                .await?;
        }

        // 4.5. Verify pglogical is NOT interfering with native logical replication.
        self.report(
            MigrationStage::Validate,
            "checking pglogical is not blocking native replication on target",
        )
        .await;
        ensure_pglogical_not_interfering(&self.config.target.connection_string).await?;

        let stats = self.run_native_engine(cancel).await?;
        token.last_applied_lsn = Some(stats.last_applied_lsn);
        self.save_resume(&token, &dump_path).await;

        self.report(MigrationStage::Complete, "online migration finished")
            .await;
        Ok(MigrationOutcome {
            stats: Some(stats),
            dump_path,
        })
    }

    /// Native PostgreSQL logical-replication apply path
    /// (`CREATE SUBSCRIPTION` on target).
    async fn run_native_engine(&self, cancel: CancellationToken) -> Result<ApplyStats> {
        let target_client = self.connect_target().await?;
        self.report(
            MigrationStage::StreamApply,
            "starting native logical-replication apply (CREATE SUBSCRIPTION)",
        )
        .await;

        let lag_provider = PgSubscriptionLagProvider::connect(
            &self.config.source.connection_string,
            &self.config.online.slot_name,
        )
        .await?;

        // The CONNECTION clause inside CREATE SUBSCRIPTION is dialed by the
        // target's apply worker, not by us — its network view of the source
        // may not match ours (e.g. operator on host vs. target in container).
        let subscription_source = self
            .config
            .online
            .subscription_source_conn
            .as_deref()
            .unwrap_or(&self.config.source.connection_string);

        run_native_apply(
            &target_client,
            &lag_provider,
            &self.config.online,
            subscription_source,
            self.cutover_handle.clone(),
            self.reporter.as_ref(),
            cancel,
        )
        .await
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
            compress: self.config.dump_compress.clone(),
            no_sync: self.config.no_sync,
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
            section: None,
        }
    }

    /// Issue `pg_restore` either as a single all-in-one call or, when
    /// `split_sections` is enabled, as three section-restricted calls
    /// (pre-data → data → post-data).
    async fn restore(&self, dump_path: &Path, cancel: &CancellationToken) -> Result<()> {
        let req = self.restore_request(dump_path);
        if self.config.split_sections {
            run_pg_restore_in_sections(self.runner.as_ref(), &req, cancel).await
        } else {
            run_pg_restore(self.runner.as_ref(), &req, cancel).await
        }
    }

    fn dump_path_or_default(&self, prefix: &str) -> PathBuf {
        if let Some(p) = &self.dump_path {
            return p.clone();
        }
        if let Some(p) = &self.config.dump_path {
            return p.clone();
        }
        let mut p = std::env::temp_dir();
        p.push(format!("{prefix}-{}", std::process::id()));
        p
    }

    fn resume_path(&self, dump_path: &Path) -> PathBuf {
        self.config
            .resume_file
            .clone()
            .unwrap_or_else(|| default_resume_path(dump_path))
    }

    /// Load (or freshly create) the resume token used to skip already-
    /// completed stages. When `--resume` is off, returns a brand-new
    /// in-memory token that is also persisted on every successful stage
    /// so a future run *can* resume even if the operator forgot to opt
    /// in this time. The path is honoured strictly only when
    /// `config.resume == true`.
    async fn load_or_init_resume(&self, dump_path: &Path) -> Result<ResumeToken> {
        let path = self.resume_path(dump_path);
        if self.config.resume {
            match ResumeToken::load(&path).await? {
                Some(token) => {
                    token.check_compatible(&self.config)?;
                    info!(
                        path = %path.display(),
                        completed = ?token.completed,
                        "resume token loaded — skipping completed stages"
                    );
                    Ok(token)
                }
                None => {
                    info!(
                        path = %path.display(),
                        "--resume set but no token on disk; running from scratch"
                    );
                    Ok(ResumeToken::new(&self.config, dump_path.to_path_buf()))
                }
            }
        } else {
            Ok(ResumeToken::new(&self.config, dump_path.to_path_buf()))
        }
    }

    async fn save_resume(&self, token: &ResumeToken, dump_path: &Path) {
        let path = self.resume_path(dump_path);
        if let Err(e) = token.save(&path).await {
            // Resume is a best-effort accelerator — never abort the
            // real migration because we couldn't write the token.
            tracing::warn!(error = %e, path = %path.display(), "failed to save resume token");
        }
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
            _cancel: &CancellationToken,
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
    async fn offline_run_with_split_sections_invokes_pg_restore_three_times() {
        let runner = Arc::new(RecordingRunner::default());
        let reporter = Arc::new(CollectingReporter::new());
        let cfg = MigrationConfig {
            split_sections: true,
            ..baseline_config()
        };
        let migrator = Migrator::new(cfg)
            .with_runner(runner.clone())
            .with_reporter(reporter)
            .with_dump_path(PathBuf::from("/tmp/pg_migrator_split_dump"));

        migrator
            .run(CancellationToken::new())
            .await
            .expect("split-section restore should succeed");

        let calls = runner.snapshot();
        assert_eq!(calls.len(), 4, "1 dump + 3 restore expected");
        assert_eq!(calls[0].0, "pg_dump");
        let sections: Vec<_> = calls[1..]
            .iter()
            .map(|(prog, args)| {
                assert_eq!(prog, "pg_restore");
                args.iter()
                    .find(|a| a.starts_with("--section="))
                    .cloned()
                    .unwrap_or_default()
            })
            .collect();
        assert_eq!(
            sections,
            vec![
                "--section=pre-data".to_string(),
                "--section=data".to_string(),
                "--section=post-data".to_string(),
            ]
        );
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

    #[tokio::test]
    async fn offline_run_skips_dump_when_resume_token_says_dump_complete() {
        let dir = tempfile::tempdir().unwrap();
        let dump = dir.path().join("dump");
        let resume = dir.path().join("dump.resume.json");

        let cfg = MigrationConfig {
            resume: true,
            dump_path: Some(dump.clone()),
            resume_file: Some(resume.clone()),
            ..baseline_config()
        };

        // Pre-seed the token: Dump already complete, Restore not yet.
        let mut t = crate::resume::ResumeToken::new(&cfg, dump.clone());
        t.mark(crate::resume::CompletedStage::Dump);
        t.save(&resume).await.unwrap();

        let runner = Arc::new(RecordingRunner::default());
        let migrator = Migrator::new(cfg)
            .with_runner(runner.clone())
            .with_reporter(Arc::new(CollectingReporter::new()));

        migrator.run(CancellationToken::new()).await.unwrap();

        let calls = runner.snapshot();
        assert_eq!(calls.len(), 1, "expected 1 call (restore only)");
        assert_eq!(calls[0].0, "pg_restore");
    }

    #[tokio::test]
    async fn validation_rejects_resume_without_dump_path() {
        let cfg = MigrationConfig {
            resume: true,
            dump_path: None,
            ..baseline_config()
        };
        let err = cfg.validate().unwrap_err();
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
    fn dump_request_propagates_perf_flags() {
        let cfg = MigrationConfig {
            dump_compress: Some("zstd:3".into()),
            no_sync: true,
            ..baseline_config()
        };
        let m = Migrator::new(cfg);
        let req = m.dump_request(Path::new("/tmp/dump"), None);
        assert_eq!(req.compress.as_deref(), Some("zstd:3"));
        assert!(req.no_sync);
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

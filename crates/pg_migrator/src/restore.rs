//! Wrapper around `pg_restore` (and `psql` for plain SQL dumps).

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::EndpointConfig;
use crate::dump::{is_directory_dump, pgpassword_env, CommandRunner};
use crate::error::{MigrationError, Result};

/// Restore phase, mapped to `pg_restore --section=<value>`.
///
/// The standard pgcopydb / pg_dump-best-practice ordering is
/// `PreData` (CREATE TABLE / TYPE / FUNCTION DDL with no indexes) →
/// `Data` (COPY of every table) → `PostData` (PRIMARY KEY, CHECK,
/// FOREIGN KEY, INDEX, TRIGGER). Splitting the restore lets the bulk
/// `Data` phase run without index maintenance, and lets the `PostData`
/// phase rebuild every index in parallel against fully-loaded tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RestoreSection {
    /// Schema DDL only (no indexes, no constraints, no triggers).
    PreData,
    /// `COPY` of every table's contents.
    Data,
    /// Indexes, primary / foreign keys, check constraints, triggers.
    PostData,
}

impl RestoreSection {
    /// Returns the value to pass to `--section=<value>`.
    pub fn flag(self) -> &'static str {
        match self {
            Self::PreData => "pre-data",
            Self::Data => "data",
            Self::PostData => "post-data",
        }
    }
}

/// Description of a restore invocation.
#[derive(Debug, Clone)]
pub struct RestoreRequest {
    /// Target endpoint to restore into.
    pub target: EndpointConfig,
    /// Path to the dump archive (or directory) produced by `pg_dump`.
    pub input_path: PathBuf,
    /// Number of parallel jobs to use (`--jobs`).
    pub jobs: usize,
    /// If `true`, pass `--clean --if-exists` so the target is reset first.
    pub clean: bool,
    /// If `true`, pass `--no-owner` (recommended when the target uses
    /// different role names).
    pub no_owner: bool,
    /// If `true`, pass `--no-acl` (skip GRANT/REVOKE statements). Strongly
    /// recommended for cross-server migrations where roles referenced by the
    /// source ACLs may not exist on the target.
    pub no_acl: bool,
    /// If `true`, treat a `pg_restore` exit-1 (the conventional "completed
    /// with errors" signal — see the `errors ignored on restore: N` warning)
    /// as a non-fatal warning rather than aborting the migration.
    ///
    /// Use case: cross-server migrations where the source has installed
    /// extensions whose internal state cannot be re-created on the target
    /// (e.g. Azure-reserved `azure` / `pgaadauth` extensions, `pg_cron`
    /// metadata tables that only `azure_pg_admin` can write to). The user
    /// data itself restores correctly — only extension metadata fails.
    ///
    /// Default `false` (fail-fast). Enable explicitly via the CLI's
    /// `--allow-restore-errors` flag.
    pub tolerate_errors: bool,
    /// If `Some`, restrict this restore to a single section
    /// (`--section=<flag>`). The split-section orchestration in
    /// [`run_pg_restore_in_sections`] sets this to PreData, then Data,
    /// then PostData on three consecutive calls. Leave `None` for a
    /// single all-in-one restore (the legacy behaviour).
    pub section: Option<RestoreSection>,
}

/// Build the argv vector for `pg_restore` based on the given request.
pub fn build_pg_restore_args(req: &RestoreRequest) -> Vec<String> {
    let mut args: Vec<String> = vec![
        "--no-password".into(),
        "--verbose".into(),
        "--host".into(),
        req.target.host.clone(),
        "--port".into(),
        req.target.port.to_string(),
        "--username".into(),
        req.target.user.clone(),
        "--dbname".into(),
        req.target.database.clone(),
    ];

    if req.jobs > 1 && is_directory_dump(&req.input_path) {
        args.push("--jobs".into());
        args.push(req.jobs.to_string());
    }
    // `--clean --if-exists` issues DROPs that only belong in pre-data; on
    // a section-restricted Data/PostData call they would happily drop the
    // freshly-loaded data. Limit the flag to "no section" or "pre-data".
    if req.clean && matches!(req.section, None | Some(RestoreSection::PreData)) {
        args.push("--clean".into());
        args.push("--if-exists".into());
    }
    if req.no_owner {
        args.push("--no-owner".into());
    }
    if req.no_acl {
        args.push("--no-acl".into());
    }
    if let Some(section) = req.section {
        args.push(format!("--section={}", section.flag()));
    }

    args.push(req.input_path.to_string_lossy().into_owned());
    args
}

/// Run `pg_restore` for the given request via the supplied runner.
pub async fn run_pg_restore<R: CommandRunner + ?Sized>(
    runner: &R,
    req: &RestoreRequest,
    cancel: &CancellationToken,
) -> Result<()> {
    info!(target = %req.target.redacted(), "running pg_restore");
    let args = build_pg_restore_args(req);
    let env = pgpassword_env(&req.target);
    match runner.run("pg_restore", &args, &env, cancel).await {
        Ok(()) => Ok(()),
        Err(e) if req.tolerate_errors && is_restore_partial_failure(&e) => {
            tracing::warn!(
                error = %e,
                "pg_restore exited non-zero but tolerate_errors=true; \
                 treating as warning. User-data restore may still be complete; \
                 verify by row-counts before cutover."
            );
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// Heuristic: `pg_restore` exit 1 is the conventional "completed with
/// errors" signal. We don't try to distinguish *which* errors occurred — the
/// caller has explicitly opted in via `tolerate_errors`. Any other failure
/// kind (spawn error, signal kill, etc.) still aborts.
fn is_restore_partial_failure(err: &MigrationError) -> bool {
    matches!(
        err,
        MigrationError::ExternalCommand { command, .. } if command == "pg_restore"
    )
}

/// Run `pg_restore` three times — `--section=pre-data`, then `data`, then
/// `post-data`.
///
/// Why split: the data section (`COPY` of every table) is by far the
/// hottest phase. Restoring without indexes / FK constraints lets `COPY`
/// run at raw I/O speed; the post-data section then rebuilds every index
/// in parallel (`--jobs N`) on already-warm tables. On schemas with many
/// secondary indexes this is typically 30–60 % faster than the
/// all-in-one restore.
///
/// Only the pre-data call honours `req.clean`; the Data and PostData
/// calls reuse the same archive but never re-issue DROPs.
pub async fn run_pg_restore_in_sections<R: CommandRunner + ?Sized>(
    runner: &R,
    base_req: &RestoreRequest,
    cancel: &CancellationToken,
) -> Result<()> {
    for section in [
        RestoreSection::PreData,
        RestoreSection::Data,
        RestoreSection::PostData,
    ] {
        if cancel.is_cancelled() {
            return Err(MigrationError::Cancelled);
        }
        info!(?section, "running pg_restore section");
        let mut req = base_req.clone();
        req.section = Some(section);
        run_pg_restore(runner, &req, cancel).await?;
    }
    Ok(())
}

/// Convenience helper: run a plain SQL file through `psql` (used when the
/// dump format is [`crate::dump::DumpFormat::Plain`]).
pub async fn run_psql_file<R: CommandRunner + ?Sized>(
    runner: &R,
    target: &EndpointConfig,
    sql_path: &std::path::Path,
    cancel: &CancellationToken,
) -> Result<()> {
    let args: Vec<String> = vec![
        "--no-password".into(),
        "--single-transaction".into(),
        "--set".into(),
        "ON_ERROR_STOP=1".into(),
        "--host".into(),
        target.host.clone(),
        "--port".into(),
        target.port.to_string(),
        "--username".into(),
        target.user.clone(),
        "--dbname".into(),
        target.database.clone(),
        "--file".into(),
        sql_path.to_string_lossy().into_owned(),
    ];

    runner
        .run("psql", &args, &pgpassword_env(target), cancel)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EndpointConfig;
    use async_trait::async_trait;

    fn sample_request() -> RestoreRequest {
        RestoreRequest {
            target: EndpointConfig::parse("postgresql://bob:pw@target.example:6432/app").unwrap(),
            input_path: PathBuf::from("/tmp/dump.bin"),
            jobs: 4,
            clean: true,
            no_owner: true,
            no_acl: true,
            tolerate_errors: false,
            section: None,
        }
    }

    #[test]
    fn section_flag_mapping() {
        assert_eq!(RestoreSection::PreData.flag(), "pre-data");
        assert_eq!(RestoreSection::Data.flag(), "data");
        assert_eq!(RestoreSection::PostData.flag(), "post-data");
    }

    #[test]
    fn build_args_includes_section_flag_when_set() {
        let mut req = sample_request();
        req.section = Some(RestoreSection::PostData);
        let args = build_pg_restore_args(&req);
        assert!(args.iter().any(|a| a == "--section=post-data"));
    }

    #[test]
    fn build_args_omits_clean_for_data_and_postdata_sections() {
        let mut req = sample_request(); // clean=true
        req.section = Some(RestoreSection::Data);
        let args = build_pg_restore_args(&req);
        assert!(
            !args.iter().any(|a| a == "--clean"),
            "data section must not re-issue DROPs"
        );

        req.section = Some(RestoreSection::PostData);
        let args = build_pg_restore_args(&req);
        assert!(!args.iter().any(|a| a == "--clean"));
    }

    #[test]
    fn build_args_keeps_clean_for_predata_section() {
        let mut req = sample_request();
        req.section = Some(RestoreSection::PreData);
        let args = build_pg_restore_args(&req);
        assert!(args.iter().any(|a| a == "--clean"));
        assert!(args.iter().any(|a| a == "--if-exists"));
    }

    #[test]
    fn build_args_includes_clean_and_no_owner_flags() {
        let args = build_pg_restore_args(&sample_request());
        assert!(args.iter().any(|a| a == "--clean"));
        assert!(args.iter().any(|a| a == "--if-exists"));
        assert!(args.iter().any(|a| a == "--no-owner"));
        assert!(args.iter().any(|a| a == "--no-acl"));
        assert!(args.iter().any(|a| a == "target.example"));
        assert!(args.iter().any(|a| a == "6432"));
    }

    #[test]
    fn build_args_omits_jobs_for_non_directory_dump() {
        // Path doesn't exist as a directory, so `--jobs` should not be added.
        let args = build_pg_restore_args(&sample_request());
        assert!(!args.iter().any(|a| a == "--jobs"));
    }

    #[test]
    fn build_args_skips_clean_when_not_requested() {
        let mut req = sample_request();
        req.clean = false;
        req.no_owner = false;
        req.no_acl = false;
        let args = build_pg_restore_args(&req);
        assert!(!args.iter().any(|a| a == "--clean"));
        assert!(!args.iter().any(|a| a == "--no-owner"));
        assert!(!args.iter().any(|a| a == "--no-acl"));
    }

    #[test]
    fn build_args_input_path_is_last() {
        let args = build_pg_restore_args(&sample_request());
        assert_eq!(args.last().unwrap(), "/tmp/dump.bin");
    }

    #[test]
    fn is_restore_partial_failure_matches_pg_restore_external_failure() {
        let e = MigrationError::external("pg_restore", "exited with status exit status: 1");
        assert!(is_restore_partial_failure(&e));
    }

    #[test]
    fn is_restore_partial_failure_rejects_other_commands() {
        let e = MigrationError::external("pg_dump", "exit 1");
        assert!(!is_restore_partial_failure(&e));
    }

    #[test]
    fn is_restore_partial_failure_rejects_other_error_kinds() {
        assert!(!is_restore_partial_failure(&MigrationError::config("nope")));
        assert!(!is_restore_partial_failure(&MigrationError::Cancelled));
    }

    /// Runner that always fails with the given error. Used to drive
    /// `run_pg_restore`'s `tolerate_errors` branch.
    #[derive(Debug)]
    struct FailingRunner {
        program_to_fail: String,
    }

    #[async_trait]
    impl CommandRunner for FailingRunner {
        async fn run(
            &self,
            program: &str,
            _args: &[String],
            _env: &[(String, String)],
            _cancel: &CancellationToken,
        ) -> Result<()> {
            Err(MigrationError::external(
                self.program_to_fail.clone(),
                format!("simulated failure for {program}"),
            ))
        }
    }

    #[tokio::test]
    async fn run_pg_restore_tolerates_failure_when_flag_set() {
        let runner = FailingRunner {
            program_to_fail: "pg_restore".into(),
        };
        let mut req = sample_request();
        req.tolerate_errors = true;
        run_pg_restore(&runner, &req, &CancellationToken::new())
            .await
            .expect("tolerate_errors=true should swallow pg_restore exit 1");
    }

    #[tokio::test]
    async fn run_pg_restore_propagates_failure_when_flag_unset() {
        let runner = FailingRunner {
            program_to_fail: "pg_restore".into(),
        };
        let req = sample_request(); // tolerate_errors = false
        let err = run_pg_restore(&runner, &req, &CancellationToken::new())
            .await
            .unwrap_err();
        assert!(matches!(err, MigrationError::ExternalCommand { .. }));
    }

    /// Records every spawned command and the `--section=` flag observed.
    #[derive(Debug, Default)]
    struct SectionRecordingRunner {
        sections: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait]
    impl CommandRunner for SectionRecordingRunner {
        async fn run(
            &self,
            _program: &str,
            args: &[String],
            _env: &[(String, String)],
            _cancel: &CancellationToken,
        ) -> Result<()> {
            let section = args
                .iter()
                .find(|a| a.starts_with("--section="))
                .cloned()
                .unwrap_or_else(|| "<none>".into());
            self.sections.lock().unwrap().push(section);
            Ok(())
        }
    }

    #[tokio::test]
    async fn run_pg_restore_in_sections_calls_pre_data_then_data_then_post_data() {
        let runner = SectionRecordingRunner::default();
        run_pg_restore_in_sections(&runner, &sample_request(), &CancellationToken::new())
            .await
            .expect("section restore should succeed");
        let observed = runner.sections.lock().unwrap().clone();
        assert_eq!(
            observed,
            vec![
                "--section=pre-data".to_string(),
                "--section=data".to_string(),
                "--section=post-data".to_string(),
            ]
        );
    }
}

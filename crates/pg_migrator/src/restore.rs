//! Wrapper around `pg_restore` (and `psql` for plain SQL dumps).

use std::path::PathBuf;

use tracing::info;

use crate::config::EndpointConfig;
use crate::dump::{is_directory_dump, pgpassword_env, CommandRunner};
use crate::error::{MigrationError, Result};

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
    if req.clean {
        args.push("--clean".into());
        args.push("--if-exists".into());
    }
    if req.no_owner {
        args.push("--no-owner".into());
    }
    if req.no_acl {
        args.push("--no-acl".into());
    }

    args.push(req.input_path.to_string_lossy().into_owned());
    args
}

/// Run `pg_restore` for the given request via the supplied runner.
pub async fn run_pg_restore<R: CommandRunner + ?Sized>(
    runner: &R,
    req: &RestoreRequest,
) -> Result<()> {
    info!(target = %req.target.redacted(), "running pg_restore");
    let args = build_pg_restore_args(req);
    let env = pgpassword_env(&req.target);
    match runner.run("pg_restore", &args, &env).await {
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
    match err {
        MigrationError::ExternalCommand { command, .. } if command == "pg_restore" => true,
        _ => false,
    }
}

/// Convenience helper: run a plain SQL file through `psql` (used when the
/// dump format is [`crate::dump::DumpFormat::Plain`]).
pub async fn run_psql_file<R: CommandRunner + ?Sized>(
    runner: &R,
    target: &EndpointConfig,
    sql_path: &std::path::Path,
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

    runner.run("psql", &args, &pgpassword_env(target)).await
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
        }
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
        run_pg_restore(&runner, &req)
            .await
            .expect("tolerate_errors=true should swallow pg_restore exit 1");
    }

    #[tokio::test]
    async fn run_pg_restore_propagates_failure_when_flag_unset() {
        let runner = FailingRunner {
            program_to_fail: "pg_restore".into(),
        };
        let req = sample_request(); // tolerate_errors = false
        let err = run_pg_restore(&runner, &req).await.unwrap_err();
        assert!(matches!(err, MigrationError::ExternalCommand { .. }));
    }
}

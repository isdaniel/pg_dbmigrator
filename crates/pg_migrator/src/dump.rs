//! Wrapper around the `pg_dump` external command.
//!
//! The actual process is invoked through a [`CommandRunner`] trait so that
//! unit tests can substitute a deterministic implementation without requiring
//! a real PostgreSQL installation. The default [`TokioCommandRunner`] simply
//! shells out to `pg_dump` via [`tokio::process::Command`].

use std::path::{Path, PathBuf};
use std::process::Stdio;

use async_trait::async_trait;
use tracing::{debug, info};

use crate::config::{DumpScope, EndpointConfig};
use crate::error::{MigrationError, Result};

/// Description of a `pg_dump` invocation.
#[derive(Debug, Clone)]
pub struct DumpRequest {
    /// Source endpoint to dump from.
    pub source: EndpointConfig,
    /// What to dump (schema, data, or both).
    pub scope: DumpScope,
    /// Number of parallel jobs (`--jobs`). Only honored for directory format
    /// dumps; ignored for the plain custom format.
    pub jobs: usize,
    /// Optional snapshot name to use (`--snapshot=<name>`). Required for
    /// online migrations to obtain a consistent dump aligned with the
    /// replication slot's start LSN.
    pub snapshot: Option<String>,
    /// Schemas to include (`--schema=...`).
    pub schemas: Vec<String>,
    /// Tables to include (`--table=...`).
    pub tables: Vec<String>,
    /// Where to write the dump archive.
    pub output_path: PathBuf,
    /// Output format. Defaults to [`DumpFormat::Custom`].
    pub format: DumpFormat,
    /// Pass `--no-publications` to `pg_dump`. Default `true`. The source's
    /// publications are an implementation detail of *this* migration; if they
    /// land on the target they will be recreated and emit
    /// `wal_level is insufficient to publish logical changes` warnings on a
    /// target that doesn't run logical replication. Set to `false` only when
    /// the target legitimately needs to inherit the source's publications.
    pub no_publications: bool,
    /// Pass `--no-subscriptions` to `pg_dump`. Default `true`. Same rationale
    /// as `no_publications` — a fresh target should not inherit subscription
    /// definitions that point at the *previous* upstream.
    pub no_subscriptions: bool,
}

/// `pg_dump` archive format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DumpFormat {
    /// Custom archive (`-F c`).
    Custom,
    /// Plain SQL (`-F p`).
    Plain,
    /// Directory archive (`-F d`) — required when using `--jobs > 1`.
    Directory,
}

impl DumpFormat {
    /// Returns the `-F` flag value used by `pg_dump`.
    pub fn flag(self) -> &'static str {
        match self {
            Self::Custom => "c",
            Self::Plain => "p",
            Self::Directory => "d",
        }
    }
}

/// Build the argv vector used to invoke `pg_dump` for a [`DumpRequest`].
///
/// This is split out from the actual process spawn so that tests can assert
/// on the produced command line without touching the filesystem.
pub fn build_pg_dump_args(req: &DumpRequest) -> Vec<String> {
    let mut args: Vec<String> = vec![
        "--no-password".into(),
        "--verbose".into(),
        "--format".into(),
        req.format.flag().into(),
        "--file".into(),
        req.output_path.to_string_lossy().into_owned(),
        "--host".into(),
        req.source.host.clone(),
        "--port".into(),
        req.source.port.to_string(),
        "--username".into(),
        req.source.user.clone(),
        "--dbname".into(),
        req.source.database.clone(),
    ];

    if let Some(flag) = req.scope.pg_dump_flag() {
        args.push(flag.into());
    }

    if req.format == DumpFormat::Directory && req.jobs > 1 {
        args.push("--jobs".into());
        args.push(req.jobs.to_string());
    }

    if let Some(snap) = &req.snapshot {
        args.push(format!("--snapshot={snap}"));
    }

    for s in &req.schemas {
        args.push(format!("--schema={s}"));
    }

    for t in &req.tables {
        args.push(format!("--table={t}"));
    }

    if req.no_publications {
        args.push("--no-publications".into());
    }
    if req.no_subscriptions {
        args.push("--no-subscriptions".into());
    }

    args
}

/// Trait abstracting an external command execution. The default
/// implementation is [`TokioCommandRunner`].
#[async_trait]
pub trait CommandRunner: Send + Sync + std::fmt::Debug {
    /// Run `program` with `args` and the given environment additions. Should
    /// fail if the process exits with a non-zero status.
    async fn run(&self, program: &str, args: &[String], env: &[(String, String)]) -> Result<()>;
}

/// Default [`CommandRunner`] that uses [`tokio::process::Command`].
#[derive(Debug, Default, Clone)]
pub struct TokioCommandRunner;

#[async_trait]
impl CommandRunner for TokioCommandRunner {
    async fn run(&self, program: &str, args: &[String], env: &[(String, String)]) -> Result<()> {
        debug!(program, ?args, "spawning external command");

        let mut cmd = tokio::process::Command::new(program);
        cmd.args(args);
        for (k, v) in env {
            cmd.env(k, v);
        }
        cmd.stdin(Stdio::null());
        cmd.stdout(Stdio::inherit());
        cmd.stderr(Stdio::inherit());

        let status = cmd
            .status()
            .await
            .map_err(|e| MigrationError::external(program, format!("failed to spawn: {e}")))?;

        if !status.success() {
            return Err(MigrationError::external(
                program,
                format!("exited with status {status}"),
            ));
        }

        info!(program, "external command finished successfully");
        Ok(())
    }
}

/// Run `pg_dump` according to `req` using the supplied [`CommandRunner`].
pub async fn run_pg_dump<R: CommandRunner + ?Sized>(runner: &R, req: &DumpRequest) -> Result<()> {
    let args = build_pg_dump_args(req);
    let env = pgpassword_env(&req.source);
    runner.run("pg_dump", &args, &env).await
}

/// Build the `PGPASSWORD` env override for the given endpoint, if any.
pub(crate) fn pgpassword_env(ep: &EndpointConfig) -> Vec<(String, String)> {
    if ep.password.is_empty() {
        Vec::new()
    } else {
        vec![("PGPASSWORD".into(), ep.password.clone())]
    }
}

/// Returns `true` if `path` looks like a directory-format dump (used for
/// parallel restore).
pub fn is_directory_dump(path: &Path) -> bool {
    path.is_dir()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    fn sample_endpoint() -> EndpointConfig {
        EndpointConfig::parse("postgresql://alice:s3cret@db.example:5433/app").unwrap()
    }

    fn base_request() -> DumpRequest {
        DumpRequest {
            source: sample_endpoint(),
            scope: DumpScope::All,
            jobs: 4,
            snapshot: None,
            schemas: Vec::new(),
            tables: Vec::new(),
            output_path: PathBuf::from("/tmp/dump.bin"),
            format: DumpFormat::Custom,
            no_publications: true,
            no_subscriptions: true,
        }
    }

    #[test]
    fn dump_format_flag_mapping() {
        assert_eq!(DumpFormat::Custom.flag(), "c");
        assert_eq!(DumpFormat::Plain.flag(), "p");
        assert_eq!(DumpFormat::Directory.flag(), "d");
    }

    #[test]
    fn build_args_includes_endpoint_components() {
        let args = build_pg_dump_args(&base_request());
        assert!(args.iter().any(|a| a == "--host"));
        assert!(args.iter().any(|a| a == "db.example"));
        assert!(args.iter().any(|a| a == "5433"));
        assert!(args.iter().any(|a| a == "app"));
        assert!(args.iter().any(|a| a == "alice"));
        assert!(args.iter().any(|a| a == "--format"));
        assert!(args.iter().any(|a| a == "c"));
    }

    #[test]
    fn build_args_includes_jobs_only_for_directory_format() {
        let mut req = base_request();
        req.format = DumpFormat::Custom;
        let args = build_pg_dump_args(&req);
        assert!(!args.iter().any(|a| a == "--jobs"));

        req.format = DumpFormat::Directory;
        let args = build_pg_dump_args(&req);
        assert!(args.iter().any(|a| a == "--jobs"));
        assert!(args.iter().any(|a| a == "4"));
    }

    #[test]
    fn build_args_passes_snapshot_to_pg_dump() {
        let mut req = base_request();
        req.snapshot = Some("00000003-0000000A-1".into());
        let args = build_pg_dump_args(&req);
        assert!(args.iter().any(|a| a == "--snapshot=00000003-0000000A-1"));
    }

    #[test]
    fn build_args_appends_schema_only_flag() {
        let mut req = base_request();
        req.scope = DumpScope::SchemaOnly;
        let args = build_pg_dump_args(&req);
        assert!(args.iter().any(|a| a == "--schema-only"));
    }

    #[test]
    fn build_args_appends_schemas_and_tables() {
        let mut req = base_request();
        req.schemas = vec!["public".into(), "app".into()];
        req.tables = vec!["public.users".into()];
        let args = build_pg_dump_args(&req);
        assert!(args.iter().any(|a| a == "--schema=public"));
        assert!(args.iter().any(|a| a == "--schema=app"));
        assert!(args.iter().any(|a| a == "--table=public.users"));
    }

    #[test]
    fn build_args_includes_no_publications_when_enabled() {
        let req = base_request(); // defaults to no_publications=true
        let args = build_pg_dump_args(&req);
        assert!(args.iter().any(|a| a == "--no-publications"));
        assert!(args.iter().any(|a| a == "--no-subscriptions"));
    }

    #[test]
    fn build_args_omits_no_publications_when_disabled() {
        let mut req = base_request();
        req.no_publications = false;
        req.no_subscriptions = false;
        let args = build_pg_dump_args(&req);
        assert!(!args.iter().any(|a| a == "--no-publications"));
        assert!(!args.iter().any(|a| a == "--no-subscriptions"));
    }

    /// Simple [`CommandRunner`] used in tests to assert on the command line
    /// produced by the higher-level helpers.
    type RunCall = (String, Vec<String>, Vec<(String, String)>);

    #[derive(Debug, Default, Clone)]
    struct RecordingRunner {
        calls: Arc<Mutex<Vec<RunCall>>>,
    }

    impl RecordingRunner {
        async fn calls(&self) -> Vec<RunCall> {
            self.calls.lock().await.clone()
        }
    }

    #[async_trait]
    impl CommandRunner for RecordingRunner {
        async fn run(
            &self,
            program: &str,
            args: &[String],
            env: &[(String, String)],
        ) -> Result<()> {
            self.calls
                .lock()
                .await
                .push((program.to_string(), args.to_vec(), env.to_vec()));
            Ok(())
        }
    }

    #[tokio::test]
    async fn run_pg_dump_invokes_runner_with_pgpassword() {
        let runner = RecordingRunner::default();
        run_pg_dump(&runner, &base_request()).await.unwrap();
        let calls = runner.calls().await;
        assert_eq!(calls.len(), 1);
        let (program, _args, env) = &calls[0];
        assert_eq!(program, "pg_dump");
        assert!(env.iter().any(|(k, v)| k == "PGPASSWORD" && v == "s3cret"));
    }

    #[tokio::test]
    async fn run_pg_dump_omits_pgpassword_when_no_password() {
        let runner = RecordingRunner::default();
        let mut req = base_request();
        req.source = EndpointConfig::parse("postgresql://u@h/db").unwrap();
        run_pg_dump(&runner, &req).await.unwrap();
        let calls = runner.calls().await;
        assert!(calls[0].2.is_empty());
    }
}

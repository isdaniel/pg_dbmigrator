//! Command-line interface argument definitions.

use std::path::PathBuf;

use clap::{Parser, ValueEnum};

use pg_migrator::config::DumpScope;
use pg_migrator::{
    CutoverConfig, EndpointConfig, MigrationConfig, MigrationMode, OnlineOptions,
    ReplicationApplyConfig,
};

/// pg_migrator — PostgreSQL → PostgreSQL migration tool.
#[derive(Debug, Parser)]
#[command(name = "pg_migrator", version, about)]
pub struct Cli {
    /// Migration mode.
    #[arg(long, value_enum, default_value_t = ModeArg::Offline)]
    pub mode: ModeArg,

    /// Source connection string (libpq URI).
    #[arg(long, env = "PG_MIGRATOR_SOURCE")]
    pub source: String,

    /// Target connection string (libpq URI).
    #[arg(long, env = "PG_MIGRATOR_TARGET")]
    pub target: String,

    /// What to dump (schema, data, or all).
    #[arg(long, value_enum, default_value_t = DumpScopeArg::All)]
    pub dump_scope: DumpScopeArg,

    /// Drop and recreate target schema before restoring.
    #[arg(long)]
    pub drop_target_first: bool,

    /// Number of parallel dump/restore jobs.
    #[arg(long, default_value_t = 4)]
    pub jobs: usize,

    /// Repeatable: schemas to migrate (default: all).
    #[arg(long = "schema")]
    pub schemas: Vec<String>,

    /// Repeatable: tables to migrate, formatted as `schema.table`.
    #[arg(long = "table")]
    pub tables: Vec<String>,

    /// Replication slot name (online mode only).
    #[arg(long, default_value = "pg_migrator_slot")]
    pub slot_name: String,

    /// Publication name (online mode only).
    #[arg(long, default_value = "pg_migrator_pub")]
    pub publication: String,

    /// Subscription name created on the target. Online mode only.
    #[arg(long, default_value = "pg_migrator_sub")]
    pub subscription_name: String,

    /// Override for the source URI written into
    /// `CREATE SUBSCRIPTION ... CONNECTION '<…>'`. Set this when the
    /// target's apply worker reaches the source via a different address than
    /// the migrator (e.g. Docker service name vs. host loopback). Defaults
    /// to `--source` when unset. Online mode only.
    #[arg(long)]
    pub subscription_source: Option<String>,

    /// Keep the subscription on the target after cutover (default: drop it).
    /// Online mode only.
    #[arg(long)]
    pub keep_subscription: bool,

    /// Best-effort cleanup of any leftover subscription on the target and
    /// replication slot on the source from a previous (crashed) run before
    /// starting. Use this when a previous run died after `CREATE
    /// SUBSCRIPTION` and the next run would otherwise fail with
    /// "subscription already exists" / "slot already exists". Online mode
    /// only.
    #[arg(long)]
    pub force_clean: bool,

    /// pgoutput protocol version.
    #[arg(long, default_value_t = 2)]
    pub protocol_version: u32,

    /// Stop the streaming apply phase after N seconds (online mode only).
    #[arg(long)]
    pub max_runtime_seconds: Option<u64>,

    /// Advisory threshold (WAL bytes). When `lag_bytes` first drops at or
    /// below this value the apply loop emits a one-shot `CaughtUp`
    /// ("ready for cutover") event — purely informational so the operator
    /// knows the cheapest moment to cut over. The bytes-behind `Lag`
    /// heartbeat is still printed every `--cutover-poll-secs` regardless.
    /// Cutover itself is always operator-driven via Ctrl+C. Online mode
    /// only.
    #[arg(long, default_value_t = 8 * 1024)]
    pub lag_threshold_bytes: u64,

    /// How often to poll the source's current WAL LSN, in seconds. Online
    /// mode only.
    #[arg(long, default_value_t = 5)]
    pub cutover_poll_secs: u64,

    /// Tighter poll cadence (milliseconds) used once `lag_bytes <=
    /// --lag-threshold-bytes`. Default 500 ms — keeps SIGINT response
    /// time sub-second once the operator has the green light to cut
    /// over. Online mode only.
    #[arg(long, default_value_t = 500)]
    pub cutover_fast_poll_ms: u64,

    /// Pin the dump archive output path. By default a unique path inside
    /// `$TMPDIR` is used. Required when `--resume` is set.
    #[arg(long)]
    pub dump_path: Option<PathBuf>,

    /// Resume a previous run. The orchestrator reads
    /// `<dump_path>.resume.json` (or `--resume-file`), validates the
    /// surrounding config still matches, and skips every stage already
    /// marked complete. Requires `--dump-path` so successive runs target
    /// the same archive.
    #[arg(long)]
    pub resume: bool,

    /// Override path for the resume token file. Defaults to
    /// `<dump_path>.resume.json`.
    #[arg(long)]
    pub resume_file: Option<PathBuf>,

    /// Treat `pg_restore` exit 1 (`errors ignored on restore: N`) as a
    /// non-fatal warning. Use for cross-server migrations where the source
    /// has installed extensions whose state cannot be re-created on the
    /// target (Azure-reserved extensions, pg_cron metadata tables, etc.).
    /// User data still restores; only extension internal state fails.
    #[arg(long)]
    pub allow_restore_errors: bool,

    /// Pass `--publications` (i.e. *do* dump publications) to `pg_dump`.
    /// Default behaviour is `--no-publications` since publications are
    /// migration scaffolding that produce noisy `wal_level is insufficient`
    /// warnings on the target.
    #[arg(long)]
    pub keep_publications: bool,

    /// Pass `--subscriptions` (i.e. *do* dump subscriptions) to `pg_dump`.
    /// Default behaviour is `--no-subscriptions` to avoid carrying over
    /// subscriptions that point at the previous upstream.
    #[arg(long)]
    pub keep_subscriptions: bool,

    /// Restore in three phases — `pre-data` → `data` → `post-data` —
    /// instead of one all-in-one `pg_restore` call. Skips index
    /// maintenance during the bulk `COPY` phase and rebuilds every index
    /// in parallel against fully-loaded tables. Typically 30–60 % faster
    /// on schemas with many secondary indexes; requires a directory- or
    /// custom-format dump.
    #[arg(long)]
    pub split_sections: bool,

    /// Verbose logging.
    #[arg(long)]
    pub verbose: bool,

    /// Emit machine-readable NDJSON progress events to stdout (one
    /// [`pg_migrator::ProgressEvent`] per line). Human-readable tracing
    /// logs continue to go to stderr. Pair with
    /// `RUST_LOG=warn,pg_migrator=warn` to silence stderr for clean piping.
    #[arg(long)]
    pub json: bool,
}

/// CLI-friendly mirror of [`MigrationMode`].
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ModeArg {
    Offline,
    Online,
}

impl From<ModeArg> for MigrationMode {
    fn from(value: ModeArg) -> Self {
        match value {
            ModeArg::Offline => MigrationMode::Offline,
            ModeArg::Online => MigrationMode::Online,
        }
    }
}

/// CLI-friendly mirror of [`DumpScope`].
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum DumpScopeArg {
    All,
    SchemaOnly,
    DataOnly,
}

impl From<DumpScopeArg> for DumpScope {
    fn from(value: DumpScopeArg) -> Self {
        match value {
            DumpScopeArg::All => DumpScope::All,
            DumpScopeArg::SchemaOnly => DumpScope::SchemaOnly,
            DumpScopeArg::DataOnly => DumpScope::DataOnly,
        }
    }
}

impl Cli {
    /// Convert CLI args into the library [`MigrationConfig`].
    pub fn into_config(self) -> Result<MigrationConfig, pg_migrator::MigrationError> {
        let source = EndpointConfig::parse(&self.source)?;
        let target = EndpointConfig::parse(&self.target)?;

        let apply = ReplicationApplyConfig {
            max_runtime_seconds: self.max_runtime_seconds,
            ..ReplicationApplyConfig::default()
        };

        let online = OnlineOptions {
            slot_name: self.slot_name,
            publication: self.publication,
            protocol_version: self.protocol_version,
            subscription_name: self.subscription_name,
            subscription_source_conn: self.subscription_source,
            drop_subscription_on_cutover: !self.keep_subscription,
            force_clean: self.force_clean,
            apply,
            cutover: CutoverConfig {
                poll_interval: std::time::Duration::from_secs(self.cutover_poll_secs),
                fast_poll_interval: std::time::Duration::from_millis(self.cutover_fast_poll_ms),
                lag_threshold_bytes: self.lag_threshold_bytes,
            },
        };

        Ok(MigrationConfig {
            mode: self.mode.into(),
            source,
            target,
            dump_scope: self.dump_scope.into(),
            drop_target_first: self.drop_target_first,
            jobs: self.jobs,
            schemas: self.schemas,
            tables: self.tables,
            online,
            allow_restore_errors: self.allow_restore_errors,
            no_publications: !self.keep_publications,
            no_subscriptions: !self.keep_subscriptions,
            split_sections: self.split_sections,
            resume: self.resume,
            resume_file: self.resume_file,
            dump_path: self.dump_path,
            verbose: self.verbose,
        })
    }
}

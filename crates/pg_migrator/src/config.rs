//! Configuration types used to drive a migration.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use url::Url;

use crate::error::{MigrationError, Result};

/// Top-level migration configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationConfig {
    /// Selects the migration strategy (see [`MigrationMode`]).
    pub mode: MigrationMode,
    /// Source PostgreSQL endpoint (the database to migrate _from_).
    pub source: EndpointConfig,
    /// Target PostgreSQL endpoint (the database to migrate _to_).
    pub target: EndpointConfig,
    /// What to dump (schema only, data only, or both).
    pub dump_scope: DumpScope,
    /// If `true`, drop and recreate the target schema before restore.
    pub drop_target_first: bool,
    /// Number of parallel jobs to pass to `pg_dump --jobs` / `pg_restore --jobs`.
    pub jobs: usize,
    /// Optional list of schemas to migrate. When empty, all schemas are migrated.
    pub schemas: Vec<String>,
    /// Optional list of tables (`schema.table`) to migrate.
    pub tables: Vec<String>,
    /// Online-only options — ignored in [`MigrationMode::Offline`].
    pub online: OnlineOptions,
    /// If `true`, treat `pg_restore` exit-1 (the conventional "completed
    /// with errors" signal) as a non-fatal warning rather than aborting.
    /// Required for cross-server migrations where the source has installed
    /// extensions whose internal state can't be re-created on the target
    /// (Azure-reserved extensions, extensions whose meta-tables only
    /// privileged roles can write to, etc.). Default `false`.
    #[serde(default)]
    pub allow_restore_errors: bool,
    /// Pass `--no-publications` to `pg_dump`. Default `true`. Source-side
    /// publications (e.g. our own `pg_migrator_pub`) are migration scaffolding;
    /// recreating them on the target produces noise such as
    /// `wal_level is insufficient to publish logical changes` warnings.
    #[serde(default = "default_true")]
    pub no_publications: bool,
    /// Pass `--no-subscriptions` to `pg_dump`. Default `true`. A new target
    /// should not inherit subscription definitions that point at the previous
    /// upstream.
    #[serde(default = "default_true")]
    pub no_subscriptions: bool,
    /// Verbose logging.
    pub verbose: bool,
}

fn default_true() -> bool {
    true
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            mode: MigrationMode::Offline,
            source: EndpointConfig::default(),
            target: EndpointConfig::default(),
            dump_scope: DumpScope::All,
            drop_target_first: false,
            jobs: 4,
            schemas: Vec::new(),
            tables: Vec::new(),
            online: OnlineOptions::default(),
            allow_restore_errors: false,
            no_publications: true,
            no_subscriptions: true,
            verbose: false,
        }
    }
}

impl MigrationConfig {
    /// Validate cross-field invariants. Called automatically by [`Migrator::run`].
    ///
    /// [`Migrator::run`]: crate::Migrator::run
    pub fn validate(&self) -> Result<()> {
        if self.source.connection_string.is_empty() {
            return Err(MigrationError::config("source connection string is empty"));
        }
        if self.target.connection_string.is_empty() {
            return Err(MigrationError::config("target connection string is empty"));
        }
        if self.jobs == 0 {
            return Err(MigrationError::config("jobs must be >= 1"));
        }
        if self.mode == MigrationMode::Online {
            self.online.validate()?;
        }
        Ok(())
    }
}

/// Migration strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationMode {
    /// One-shot `pg_dump` followed by `pg_restore`. Source must be quiesced
    /// (or the lost-write window accepted) for the duration of the migration.
    Offline,
    /// Snapshot-based `pg_dump` + `pg_restore` followed by ongoing logical
    /// replication apply through `pg_walstream`.
    Online,
}

/// Choose what kind of dump to produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DumpScope {
    /// Both schema and data.
    All,
    /// Schema only (`--schema-only`).
    SchemaOnly,
    /// Data only (`--data-only`).
    DataOnly,
}

impl DumpScope {
    /// Returns the matching `pg_dump` command-line flag, if any.
    pub fn pg_dump_flag(self) -> Option<&'static str> {
        match self {
            Self::All => None,
            Self::SchemaOnly => Some("--schema-only"),
            Self::DataOnly => Some("--data-only"),
        }
    }
}

/// A single PostgreSQL endpoint description.
///
/// We store the original libpq URI verbatim in `connection_string` and parse
/// it once into individual fields so that callers do not need to do that
/// themselves.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EndpointConfig {
    /// Original libpq URI (e.g. `postgresql://user:pw@host:5432/db`).
    pub connection_string: String,
    /// Hostname, parsed from `connection_string`.
    pub host: String,
    /// TCP port, parsed from `connection_string` (defaults to 5432).
    pub port: u16,
    /// Database name, parsed from `connection_string`.
    pub database: String,
    /// Username, parsed from `connection_string` (may be empty).
    pub user: String,
    /// Password, parsed from `connection_string` (may be empty).
    pub password: String,
}

impl EndpointConfig {
    /// Parse a libpq URI into an [`EndpointConfig`].
    ///
    /// Accepts the `postgresql://` and `postgres://` schemes. Query parameters
    /// (e.g. `?sslmode=require`) are preserved on `connection_string` but not
    /// extracted into individual fields.
    pub fn parse(conn: &str) -> Result<Self> {
        let url = Url::parse(conn)
            .map_err(|e| MigrationError::InvalidConnectionString(format!("{conn}: {e}")))?;

        if !matches!(url.scheme(), "postgres" | "postgresql") {
            return Err(MigrationError::InvalidConnectionString(format!(
                "unsupported scheme `{}`",
                url.scheme()
            )));
        }

        let host = url
            .host_str()
            .ok_or_else(|| {
                MigrationError::InvalidConnectionString(format!("{conn}: missing host"))
            })?
            .to_string();
        let port = url.port().unwrap_or(5432);
        let database = url
            .path()
            .trim_start_matches('/')
            .split('?')
            .next()
            .unwrap_or("")
            .to_string();
        let user = url.username().to_string();
        let password = url.password().unwrap_or("").to_string();

        Ok(Self {
            connection_string: conn.to_string(),
            host,
            port,
            database,
            user,
            password,
        })
    }

    /// Returns a redacted form suitable for logging (password is masked).
    pub fn redacted(&self) -> String {
        if self.password.is_empty() {
            self.connection_string.clone()
        } else {
            self.connection_string
                .replacen(&format!(":{}@", self.password), ":****@", 1)
        }
    }
}

/// Online-mode-specific knobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnlineOptions {
    /// Replication slot name on the source. Will be created with
    /// `EXPORT_SNAPSHOT` if it does not already exist.
    pub slot_name: String,
    /// Publication name on the source. The library does not create the
    /// publication automatically; this must be done by the operator.
    pub publication: String,
    /// pgoutput protocol version (2 enables streaming).
    pub protocol_version: u32,
    /// Subscription name created on the target. The library issues
    /// `CREATE SUBSCRIPTION ... WITH (create_slot=false, slot_name='<existing>',
    /// enabled=true, copy_data=false)` so PostgreSQL's built-in apply worker
    /// streams from the slot prepared during `PrepareSnapshot`.
    #[serde(default = "default_subscription_name")]
    pub subscription_name: String,
    /// Override for the source connection string written into
    /// `CREATE SUBSCRIPTION ... CONNECTION '<…>'`.
    ///
    /// `None` means the same URI used by the migrator (`source.connection_string`)
    /// is reused. Set this when the migrator and the target's apply worker
    /// see the source from different network locations (e.g. operator runs
    /// from outside a Docker network while the target connects via the
    /// in-network service name). Always validated by the target — the apply
    /// worker, not the migrator, dials this URI.
    #[serde(default)]
    pub subscription_source_conn: Option<String>,
    /// If `true`, drop the subscription after a successful cutover. If
    /// `false`, the subscription is left disabled but in place so the
    /// operator can inspect it. Default `true`.
    #[serde(default = "default_true")]
    pub drop_subscription_on_cutover: bool,
    /// If `true`, run a best-effort cleanup before creating the slot &
    /// subscription: any leftover subscription with the same name on the
    /// target is disabled / detached / dropped, and any leftover replication
    /// slot with the same name on the source is dropped. Use this when a
    /// previous run died after `CREATE SUBSCRIPTION` and the next run would
    /// otherwise fail with "subscription already exists" / "slot already
    /// exists". Default `false` so operators opt in explicitly. CLI:
    /// `--force-clean`.
    #[serde(default)]
    pub force_clean: bool,
    /// Configuration for the WAL apply worker.
    pub apply: ReplicationApplyConfig,
    /// Cutover knobs — when to declare the target "caught up" and how the
    /// operator triggers cutover.
    pub cutover: CutoverConfig,
}

fn default_subscription_name() -> String {
    "pg_migrator_sub".to_string()
}

impl Default for OnlineOptions {
    fn default() -> Self {
        Self {
            slot_name: "pg_migrator_slot".to_string(),
            publication: "pg_migrator_pub".to_string(),
            protocol_version: 2,
            subscription_name: default_subscription_name(),
            subscription_source_conn: None,
            drop_subscription_on_cutover: true,
            force_clean: false,
            apply: ReplicationApplyConfig::default(),
            cutover: CutoverConfig::default(),
        }
    }
}

impl OnlineOptions {
    /// Validate the online-specific fields.
    pub fn validate(&self) -> Result<()> {
        if self.slot_name.is_empty() {
            return Err(MigrationError::config("slot_name must not be empty"));
        }
        if self.publication.is_empty() {
            return Err(MigrationError::config("publication must not be empty"));
        }
        if self.protocol_version == 0 || self.protocol_version > 4 {
            return Err(MigrationError::config("protocol_version must be in 1..=4"));
        }
        if self.subscription_name.is_empty() {
            return Err(MigrationError::config(
                "subscription_name must not be empty",
            ));
        }
        self.cutover.validate()?;
        Ok(())
    }
}

/// Tunables for the streaming apply loop.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationApplyConfig {
    /// How often to send standby status updates to the source.
    #[serde(with = "humantime_serde_workaround")]
    pub feedback_interval: Duration,
    /// Connection timeout for both source and target connections.
    #[serde(with = "humantime_serde_workaround")]
    pub connection_timeout: Duration,
    /// Health-check / keep-alive interval for the source connection.
    #[serde(with = "humantime_serde_workaround")]
    pub health_check_interval: Duration,
    /// Stop the apply loop after this many seconds of catch-up. `None` means
    /// run until cancelled.
    pub max_runtime_seconds: Option<u64>,
}

impl Default for ReplicationApplyConfig {
    fn default() -> Self {
        Self {
            feedback_interval: Duration::from_secs(10),
            connection_timeout: Duration::from_secs(30),
            health_check_interval: Duration::from_secs(60),
            max_runtime_seconds: None,
        }
    }
}

/// Cutover policy.
///
/// Online migrations stream WAL until the operator decides to cut over.
/// Once the lag (in bytes between `last_applied_lsn` and the source's
/// current WAL flush position) first drops at or below
/// `lag_threshold_bytes`, the orchestrator emits an advisory
/// [`crate::progress::MigrationStage::CaughtUp`] event so the operator
/// knows it is the cheapest moment to switch. Cutover itself only happens
/// when the operator calls
/// [`crate::cutover::CutoverHandle::request`] (the CLI wires this to
/// SIGINT / Ctrl+C).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverConfig {
    /// How often to query the source's current WAL LSN.
    #[serde(with = "humantime_serde_workaround")]
    pub poll_interval: Duration,
    /// Advisory lag threshold (in WAL bytes). When the lag first drops at
    /// or below this value the orchestrator emits a one-shot `CaughtUp`
    /// ("ready for cutover") event. The threshold never triggers cutover
    /// on its own — that is always operator-driven.
    pub lag_threshold_bytes: u64,
}

impl Default for CutoverConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(5),
            // 8 KiB — one WAL page; "ready" threshold for short-lived idle gaps.
            lag_threshold_bytes: 8 * 1024,
        }
    }
}

impl CutoverConfig {
    /// Validate cutover knobs.
    pub fn validate(&self) -> Result<()> {
        if self.poll_interval.is_zero() {
            return Err(MigrationError::config("cutover.poll_interval must be > 0"));
        }
        Ok(())
    }
}

/// Tiny `serde` adapter for `std::time::Duration` ↔ seconds. Avoids pulling in
/// the `humantime` crate for what is otherwise a one-line conversion.
mod humantime_serde_workaround {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        d.as_secs().serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let secs = u64::deserialize(d)?;
        Ok(Duration::from_secs(secs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_basic_uri() {
        let ep =
            EndpointConfig::parse("postgresql://alice:s3cret@db.example:5433/app").expect("parse");
        assert_eq!(ep.host, "db.example");
        assert_eq!(ep.port, 5433);
        assert_eq!(ep.database, "app");
        assert_eq!(ep.user, "alice");
        assert_eq!(ep.password, "s3cret");
    }

    #[test]
    fn parses_default_port() {
        let ep = EndpointConfig::parse("postgres://u@h/db").unwrap();
        assert_eq!(ep.port, 5432);
        assert_eq!(ep.user, "u");
        assert!(ep.password.is_empty());
    }

    #[test]
    fn rejects_bad_scheme() {
        let err = EndpointConfig::parse("mysql://u@h/db").unwrap_err();
        assert!(matches!(err, MigrationError::InvalidConnectionString(_)));
    }

    #[test]
    fn rejects_missing_host() {
        let err = EndpointConfig::parse("postgresql:///db").unwrap_err();
        assert!(matches!(err, MigrationError::InvalidConnectionString(_)));
    }

    #[test]
    fn redacted_masks_password() {
        let ep = EndpointConfig::parse("postgresql://u:topsecret@h/db").unwrap();
        let redacted = ep.redacted();
        assert!(!redacted.contains("topsecret"));
        assert!(redacted.contains(":****@"));
    }

    #[test]
    fn redacted_passthrough_when_no_password() {
        let ep = EndpointConfig::parse("postgresql://u@h/db").unwrap();
        assert_eq!(ep.redacted(), "postgresql://u@h/db");
    }

    #[test]
    fn validate_rejects_zero_jobs() {
        let cfg = MigrationConfig {
            source: EndpointConfig::parse("postgres://u@s/db").unwrap(),
            target: EndpointConfig::parse("postgres://u@t/db").unwrap(),
            jobs: 0,
            ..MigrationConfig::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, MigrationError::Config(_)));
    }

    #[test]
    fn online_options_validate() {
        let mut opts = OnlineOptions::default();
        assert!(opts.validate().is_ok());

        opts.slot_name.clear();
        assert!(opts.validate().is_err());

        opts.slot_name = "s".into();
        opts.publication.clear();
        assert!(opts.validate().is_err());

        opts.publication = "p".into();
        opts.protocol_version = 99;
        assert!(opts.validate().is_err());
    }

    #[test]
    fn dump_scope_flag_mapping() {
        assert_eq!(DumpScope::All.pg_dump_flag(), None);
        assert_eq!(DumpScope::SchemaOnly.pg_dump_flag(), Some("--schema-only"));
        assert_eq!(DumpScope::DataOnly.pg_dump_flag(), Some("--data-only"));
    }

    #[test]
    fn cutover_config_default_is_valid() {
        let c = CutoverConfig::default();
        assert!(c.validate().is_ok());
        assert_eq!(c.lag_threshold_bytes, 8 * 1024);
    }

    #[test]
    fn cutover_config_rejects_zero_poll_interval() {
        let c = CutoverConfig {
            poll_interval: Duration::from_secs(0),
            ..CutoverConfig::default()
        };
        let err = c.validate().unwrap_err();
        assert!(matches!(err, MigrationError::Config(_)));
    }

    #[test]
    fn online_options_validate_propagates_cutover_error() {
        let opts = OnlineOptions {
            cutover: CutoverConfig {
                poll_interval: Duration::from_secs(0),
                ..CutoverConfig::default()
            },
            ..OnlineOptions::default()
        };
        assert!(opts.validate().is_err());
    }

    #[test]
    fn online_options_default_subscription_name() {
        let opts = OnlineOptions::default();
        assert_eq!(opts.subscription_name, "pg_migrator_sub");
        assert!(opts.drop_subscription_on_cutover);
    }

    #[test]
    fn online_options_reject_empty_subscription_name() {
        let opts = OnlineOptions {
            subscription_name: String::new(),
            ..OnlineOptions::default()
        };
        assert!(opts.validate().is_err());
    }
}

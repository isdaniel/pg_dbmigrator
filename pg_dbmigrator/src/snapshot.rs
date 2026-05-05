//! Snapshot / replication-slot management for online migrations.
//!
//! This module is a thin orchestration layer on top of
//! [`pg_walstream::LogicalReplicationStream`]. Its job is to:
//!
//! 1. open a replication connection to the source,
//! 2. create the logical replication slot with `EXPORT_SNAPSHOT`,
//! 3. expose the resulting snapshot id (so that `pg_dump --snapshot=...` can
//!    obtain a consistent view of the database at the slot's start LSN).
//!
//! The actual `START_REPLICATION` is deferred — the orchestrator hands
//! the slot to [`crate::native_apply`] only **after** `pg_dump` has
//! finished, because issuing `START_REPLICATION` invalidates the
//! exported snapshot.

use std::time::Duration;

use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig, StreamingMode};
use tracing::{info, warn};

use crate::config::OnlineOptions;
use crate::error::Result;

/// Result of [`prepare_replication_slot`].
///
/// Note: [`pg_walstream::LogicalReplicationStream`] does not implement
/// [`Debug`], so this struct cannot derive `Debug` either.
#[allow(missing_debug_implementations)]
pub struct PreparedSlot {
    /// The replication stream with the slot already created. The
    /// orchestrator holds it across the `pg_dump` step (so the exported
    /// snapshot stays alive) and drops it before handing the slot to
    /// [`crate::native_apply::run_native_apply`].
    pub stream: LogicalReplicationStream,
    /// The exported snapshot id, if PostgreSQL returned one. Use this with
    /// `pg_dump --snapshot=<id>` to obtain a consistent dump aligned with the
    /// slot's start LSN.
    pub snapshot_name: Option<String>,
}

/// Build a [`ReplicationStreamConfig`] for the given online options.
///
/// Exposed as `pub` so tests (and callers that want full control) can build
/// the stream themselves.
pub fn build_stream_config(opts: &OnlineOptions) -> ReplicationStreamConfig {
    ReplicationStreamConfig::new(
        opts.slot_name.clone(),
        opts.publication.clone(),
        opts.protocol_version,
        StreamingMode::On,
        opts.apply.feedback_interval,
        opts.apply.connection_timeout,
        opts.apply.health_check_interval,
        RetryConfig::default(),
    )
}

/// Open a replication connection and create the replication slot, returning
/// both the stream and the snapshot name exported by PostgreSQL.
///
/// `connection_string` must include `?replication=database` for libpq to
/// open a replication connection.
pub async fn prepare_replication_slot(
    connection_string: &str,
    opts: &OnlineOptions,
) -> Result<PreparedSlot> {
    info!(slot = %opts.slot_name, publication = %opts.publication, "preparing replication slot");
    opts.validate()?;

    let conn = ensure_replication_qs(connection_string);
    let cfg = build_stream_config(opts);
    let mut stream = LogicalReplicationStream::new(&conn, cfg).await?;
    stream.ensure_replication_slot().await?;
    let snapshot_name = stream.exported_snapshot_name().map(|s| s.to_string());
    if snapshot_name.is_none() {
        warn!("replication slot was reused — no exported snapshot is available");
    } else {
        info!(?snapshot_name, "exported snapshot ready");
    }
    Ok(PreparedSlot {
        stream,
        snapshot_name,
    })
}

/// Append `replication=database` to a libpq URI if it isn't already present.
///
/// Public so it can be unit-tested independently.
pub fn ensure_replication_qs(connection_string: &str) -> String {
    if connection_string.contains("replication=") {
        return connection_string.to_string();
    }
    if connection_string.contains('?') {
        format!("{connection_string}&replication=database")
    } else {
        format!("{connection_string}?replication=database")
    }
}

/// Bound used by the orchestrator when waiting for the slot to be ready.
/// Kept here so tests do not need to depend on real timings.
pub const DEFAULT_SLOT_TIMEOUT: Duration = Duration::from_secs(60);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_replication_qs_appends_when_missing() {
        let out = ensure_replication_qs("postgresql://u:p@h:5432/db");
        assert_eq!(out, "postgresql://u:p@h:5432/db?replication=database");
    }

    #[test]
    fn ensure_replication_qs_appends_with_existing_query() {
        let out = ensure_replication_qs("postgresql://u@h/db?sslmode=require");
        assert_eq!(
            out,
            "postgresql://u@h/db?sslmode=require&replication=database"
        );
    }

    #[test]
    fn ensure_replication_qs_keeps_existing() {
        let already = "postgresql://u@h/db?replication=database";
        assert_eq!(ensure_replication_qs(already), already);
    }

    #[test]
    fn ensure_replication_qs_keeps_other_replication_value() {
        // `replication=true` should also be detected and not re-appended.
        let s = "postgresql://u@h/db?replication=true";
        assert_eq!(ensure_replication_qs(s), s);
    }

    #[test]
    fn build_stream_config_propagates_options() {
        let opts = OnlineOptions {
            slot_name: "slot".into(),
            publication: "pub".into(),
            protocol_version: 2,
            ..OnlineOptions::default()
        };
        let cfg = build_stream_config(&opts);
        assert_eq!(cfg.slot_name, "slot");
        assert_eq!(cfg.publication_name, "pub");
        assert_eq!(cfg.protocol_version, 2);
    }

    #[test]
    fn build_stream_config_uses_apply_intervals() {
        use std::time::Duration;
        let opts = OnlineOptions {
            apply: crate::config::ReplicationApplyConfig {
                feedback_interval: Duration::from_secs(20),
                connection_timeout: Duration::from_secs(45),
                health_check_interval: Duration::from_secs(120),
                max_runtime_seconds: None,
            },
            ..OnlineOptions::default()
        };
        let cfg = build_stream_config(&opts);
        assert_eq!(cfg.feedback_interval, Duration::from_secs(20));
        assert_eq!(cfg.connection_timeout, Duration::from_secs(45));
        assert_eq!(cfg.health_check_interval, Duration::from_secs(120));
    }

    #[test]
    fn default_slot_timeout_is_60_seconds() {
        assert_eq!(DEFAULT_SLOT_TIMEOUT, std::time::Duration::from_secs(60));
    }
}

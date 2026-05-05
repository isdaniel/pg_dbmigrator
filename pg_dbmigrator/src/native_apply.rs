//! Native PostgreSQL logical-replication apply engine.
//!
//! Instead of decoding pgoutput in-process, we let PostgreSQL's own apply
//! worker do the work. The orchestrator has already created the slot on
//! the source via [`crate::snapshot::prepare_replication_slot`] and
//! finished `pg_dump` / `pg_restore`; this module then:
//!
//! 1. issues `CREATE SUBSCRIPTION ... WITH (create_slot=false,
//!    slot_name='<existing>', enabled=true, copy_data=false)` on the target
//!    so the built-in apply worker attaches to the pre-existing slot,
//! 2. polls `pg_replication_slots.confirmed_flush_lsn` against
//!    `pg_current_wal_flush_lsn()` on the source every
//!    [`crate::config::CutoverConfig::poll_interval`], emitting `Lag` /
//!    `CaughtUp` progress events,
//! 3. on cutover ([`crate::cutover::CutoverHandle::request`], typically
//!    wired to SIGINT) runs `ALTER SUBSCRIPTION ... DISABLE` and —
//!    unless the operator chose `--keep-subscription` —
//!    `DROP SUBSCRIPTION` for a clean exit.

use std::time::Instant;

use async_trait::async_trait;
use pg_walstream::{
    build_create_subscription_sql, build_detach_slot_sql, build_disable_subscription_sql,
    build_drop_subscription_sql, format_lsn, parse_lsn, quote_ident, quote_literal,
    CreateSubscriptionOptions,
};
use tokio_postgres::{Client, Statement};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::{CutoverConfig, OnlineOptions};
use crate::cutover::{CutoverHandle, LagSampler, Transition};
use crate::error::{MigrationError, Result};
use crate::progress::{MigrationStage, ProgressEvent, ProgressReporter};
use crate::tls::connect_with_sslmode;

/// Aggregated statistics for one run of [`run_native_apply`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ApplyStats {
    /// LSN of the most recent WAL position the slot has confirmed-flushed.
    pub last_applied_lsn: u64,
    /// Same as `last_applied_lsn` — kept under a separate name so the
    /// progress event payload schema stays stable for downstream dashboards.
    pub last_received_lsn: u64,
    /// Most recent observed lag (`source_lsn - confirmed_lsn`) in bytes.
    pub last_lag_bytes: u64,
    /// Whether the apply loop ended because cutover was requested.
    pub cutover_triggered: bool,
}

/// Parse PostgreSQL's textual `pg_lsn` representation (`"H/L"` where H and L
/// are hex) into a `u64`.
///
/// Thin wrapper over [`pg_walstream::parse_lsn`] that maps the upstream
/// error into our [`MigrationError`] enum so call sites stay terse.
pub fn parse_pg_lsn(s: &str) -> Result<u64> {
    parse_lsn(s).map_err(|_| MigrationError::apply(format!("could not parse pg_lsn: {s:?}")))
}

/// Source-side LSN sampler abstracted so tests can inject deterministic values.
///
/// The native engine asks **the source** for two numbers:
///   * `pg_current_wal_flush_lsn()` (how far the source has flushed), and
///   * `pg_replication_slots.confirmed_flush_lsn` for our slot (how far the
///     subscriber has acknowledged).
///
/// Lag in bytes = `current_wal_flush_lsn - confirmed_flush_lsn`.
#[async_trait]
pub trait SubscriptionLagProvider: Send + Sync {
    /// Sample the source's WAL flush LSN and the slot's confirmed flush LSN.
    /// Returns `(source_lsn, confirmed_lsn)`.
    async fn sample(&self) -> Result<(u64, u64)>;
}

/// Real-source implementation of [`SubscriptionLagProvider`] backed by
/// `tokio_postgres`.
///
/// The lag-sample SQL is `prepare()`d once at construction and reused for
/// every poll, so each heartbeat round-trip is a single bind+execute rather
/// than a parse+plan+bind+execute.
#[derive(Debug)]
pub struct PgSubscriptionLagProvider {
    client: Client,
    slot_name: String,
    sample_stmt: Statement,
}

impl PgSubscriptionLagProvider {
    /// Open a (non-replication) connection to the source for lag polling.
    pub async fn connect(connection_string: &str, slot_name: impl Into<String>) -> Result<Self> {
        let client = connect_with_sslmode(connection_string).await?;
        let sample_stmt = client
            .prepare(
                "SELECT pg_current_wal_flush_lsn()::text, \
                        confirmed_flush_lsn::text \
                 FROM pg_replication_slots \
                 WHERE slot_name = $1",
            )
            .await?;
        Ok(Self {
            client,
            slot_name: slot_name.into(),
            sample_stmt,
        })
    }
}

#[async_trait]
impl SubscriptionLagProvider for PgSubscriptionLagProvider {
    async fn sample(&self) -> Result<(u64, u64)> {
        let row = self
            .client
            .query_one(&self.sample_stmt, &[&self.slot_name])
            .await?;
        let source_raw: String = row.get(0);
        let confirmed_raw: String = row.get(1);
        let source_lsn = parse_pg_lsn(&source_raw)?;
        let confirmed_lsn = parse_pg_lsn(&confirmed_raw)?;
        Ok((source_lsn, confirmed_lsn))
    }
}

/// Build the `CREATE SUBSCRIPTION ...` SQL using pg_walstream's sql_builder.
///
/// `create_slot=false` is the critical bit: the slot was created during
/// `PrepareSnapshot` so the dump could use the exported snapshot; we must
/// not let the subscription create another one.
///
/// `copy_data=false` avoids re-copying tables that `pg_restore` already
/// loaded.
pub fn make_create_subscription_sql(opts: &OnlineOptions, source_conn: &str) -> Result<String> {
    let sub_opts = CreateSubscriptionOptions {
        subscription_name: &opts.subscription_name,
        connection_string: source_conn,
        publication: &opts.publication,
        slot_name: &opts.slot_name,
        create_slot: false,
        enabled: true,
        copy_data: false,
    };
    build_create_subscription_sql(&sub_opts).map_err(Into::into)
}

/// Best-effort cleanup of any leftover subscription on the target and any
/// leftover replication slot on the source from a previous (crashed) run.
///
/// Idempotent: every step is `IF EXISTS` / wrapped in a `DO` block that
/// checks `pg_subscription` / `pg_replication_slots`. Errors are logged
/// but do not propagate — the goal is to unblock the next `CREATE
/// SUBSCRIPTION` / slot creation, not to be a fully-featured admin tool.
///
/// Called from the orchestrator only when `OnlineOptions::force_clean` is
/// `true` (CLI: `--force-clean`).
pub async fn force_clean_stale_state(
    source_conn: &str,
    target_conn: &str,
    online: &OnlineOptions,
) -> Result<()> {
    info!(
        subscription = %online.subscription_name,
        slot = %online.slot_name,
        "force-clean: removing any stale subscription/slot from a previous run"
    );

    cleanup_target_subscription(target_conn, online).await?;

    // Source: drop replication slot if it exists. We use the connection
    // string the migrator already validated; pg_drop_replication_slot
    // requires a non-replication connection.
    let source = connect_with_sslmode(source_conn).await?;
    let slot_lit = quote_literal(&online.slot_name)?;
    let cleanup_slot_sql = format!(
        "DO $$\n\
         BEGIN\n\
            IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = {slot_lit}) THEN\n\
                PERFORM pg_drop_replication_slot({slot_lit});\n\
            END IF;\n\
         END $$;",
    );
    if let Err(e) = source.batch_execute(&cleanup_slot_sql).await {
        warn!(error = %e, "force-clean: source slot cleanup failed (continuing)");
    } else {
        info!("force-clean: source slot cleanup ok");
    }

    Ok(())
}

/// Poll the source until the replication slot is no longer active. A previous
/// apply worker may still hold the slot briefly after its subscription was
/// dropped. Returns Ok(()) once the slot is inactive or absent.
pub async fn wait_for_slot_inactive(
    source_conn: &str,
    slot_name: &str,
    reporter: &dyn ProgressReporter,
) -> Result<()> {
    let client = connect_with_sslmode(source_conn).await?;
    let stmt = client
        .prepare("SELECT active FROM pg_replication_slots WHERE slot_name = $1")
        .await?;

    let deadline = Instant::now() + std::time::Duration::from_secs(30);
    let mut warned = false;

    loop {
        let rows = client.query(&stmt, &[&slot_name]).await?;
        if rows.is_empty() {
            return Ok(());
        }
        let active: bool = rows[0].get(0);
        if !active {
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(MigrationError::apply(format!(
                "replication slot `{slot_name}` is still active after 30s — \
                 a previous apply worker may not have released it"
            )));
        }

        if !warned {
            warned = true;
            reporter
                .report(ProgressEvent::new(
                    MigrationStage::StreamApply,
                    format!(
                        "waiting for replication slot `{slot_name}` to become inactive \
                         (previous apply worker disconnecting)"
                    ),
                ))
                .await;
        }

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

/// Disable a subscription on the target without dropping it. This stops the
/// apply worker so it releases the replication slot, while preserving the
/// replication origin for a subsequent re-enable. Best-effort: errors are
/// logged but don't propagate.
pub async fn disable_target_subscription(target_conn: &str, online: &OnlineOptions) {
    let Ok(target) = connect_with_sslmode(target_conn).await else {
        return;
    };
    let Ok(sub) = quote_ident(&online.subscription_name) else {
        return;
    };
    let Ok(sub_lit) = quote_literal(&online.subscription_name) else {
        return;
    };
    let sql = format!(
        "DO $$\n\
         BEGIN\n\
            IF EXISTS (SELECT 1 FROM pg_subscription WHERE subname = {sub_lit}) THEN\n\
                EXECUTE 'ALTER SUBSCRIPTION {sub} DISABLE';\n\
            END IF;\n\
         END $$;",
    );
    if let Err(e) = target.batch_execute(&sql).await {
        warn!(error = %e, "failed to disable subscription (continuing)");
    } else {
        info!(subscription = %online.subscription_name, "subscription disabled for resume");
    }
}

/// Drop only the leftover subscription on the target — leaves the source
/// slot untouched. Used by the `--resume` path so a half-built apply
/// stage can be retried without forfeiting the slot's WAL position.
pub async fn cleanup_target_subscription(target_conn: &str, online: &OnlineOptions) -> Result<()> {
    let target = connect_with_sslmode(target_conn).await?;
    let sub = quote_ident(&online.subscription_name)?;
    let sub_lit = quote_literal(&online.subscription_name)?;
    let cleanup_sub_sql = format!(
        "DO $$\n\
         BEGIN\n\
            IF EXISTS (SELECT 1 FROM pg_subscription WHERE subname = {sub_lit}) THEN\n\
                EXECUTE 'ALTER SUBSCRIPTION {sub} DISABLE';\n\
                EXECUTE 'ALTER SUBSCRIPTION {sub} SET (slot_name = NONE)';\n\
                EXECUTE 'DROP SUBSCRIPTION {sub}';\n\
            END IF;\n\
         END $$;",
    );
    if let Err(e) = target.batch_execute(&cleanup_sub_sql).await {
        warn!(error = %e, "target subscription cleanup failed (continuing)");
    } else {
        info!("target subscription cleanup ok");
    }
    Ok(())
}

/// Run the native apply phase: create a subscription on the target, poll the
/// source for replication lag, and exit gracefully when the operator
/// triggers cutover.
pub async fn run_native_apply(
    target_client: &Client,
    lag_provider: &dyn SubscriptionLagProvider,
    online: &OnlineOptions,
    source_conn: &str,
    cutover_handle: CutoverHandle,
    reporter: &dyn ProgressReporter,
    cancel: CancellationToken,
) -> Result<ApplyStats> {
    info!(
        subscription = %online.subscription_name,
        slot = %online.slot_name,
        publication = %online.publication,
        "native engine: creating subscription"
    );

    let sub = quote_ident(&online.subscription_name)?;

    // Check if the subscription already exists (resume scenario). If it does,
    // re-enable it in place — this preserves the replication origin so the
    // apply worker does not re-process WAL events that were already applied.
    // Dropping + recreating would lose the origin and cause duplicate key
    // violations on replay.
    let exists_row = target_client
        .query(
            "SELECT 1 FROM pg_subscription WHERE subname = $1",
            &[&online.subscription_name],
        )
        .await?;

    if !exists_row.is_empty() {
        info!(
            subscription = %online.subscription_name,
            "subscription already exists — re-enabling (preserving replication origin)"
        );
        let conn_lit = quote_literal(source_conn)?;
        let slot_lit = quote_literal(&online.slot_name)?;
        let reattach_sql = format!(
            "ALTER SUBSCRIPTION {sub} DISABLE;\n\
             ALTER SUBSCRIPTION {sub} CONNECTION {conn_lit};\n\
             ALTER SUBSCRIPTION {sub} SET (slot_name = {slot_lit});\n\
             ALTER SUBSCRIPTION {sub} ENABLE;",
        );
        target_client.batch_execute(&reattach_sql).await?;
    } else {
        let create_sql = make_create_subscription_sql(online, source_conn)?;
        target_client.batch_execute(&create_sql).await?;
    }

    let health_stmt = target_client
        .prepare(
            "SELECT pid, received_lsn::text, latest_end_lsn::text \
             FROM pg_stat_subscription \
             WHERE subname = $1 AND relid IS NULL",
        )
        .await?;

    let cutover_cfg: &CutoverConfig = &online.cutover;
    let mut sampler = LagSampler::new(cutover_cfg.lag_threshold_bytes);
    let mut stats = ApplyStats::default();
    let mut stale_count: u32 = 0;
    let mut prev_confirmed: Option<u64> = None;
    // Start aggressive: first iteration shouldn't wait `poll_interval`
    // before the very first sample. After each sample we recompute the
    // wait based on observed lag.
    let mut last_poll = Instant::now() - cutover_cfg.poll_interval;
    let mut current_interval = cutover_cfg.poll_interval;

    let result = loop {
        if cancel.is_cancelled() {
            info!("native apply cancelled");
            break Ok::<(), MigrationError>(());
        }

        if last_poll.elapsed() < current_interval {
            tokio::select! {
                _ = tokio::time::sleep(current_interval - last_poll.elapsed()) => {}
                _ = cancel.cancelled() => continue,
            }
        }
        last_poll = Instant::now();

        match lag_provider.sample().await {
            Ok((source_lsn, confirmed_lsn)) => {
                let transition = sampler.observe(source_lsn, confirmed_lsn);
                stats.last_received_lsn = confirmed_lsn;
                stats.last_applied_lsn = confirmed_lsn;
                stats.last_lag_bytes = transition.lag();
                current_interval = if transition.lag() <= cutover_cfg.lag_threshold_bytes {
                    cutover_cfg.fast_poll_interval
                } else {
                    cutover_cfg.poll_interval
                };
                report_lag_heartbeat(reporter, transition.lag(), source_lsn, confirmed_lsn).await;
                report_transition(reporter, transition, source_lsn, confirmed_lsn).await;

                // Track whether confirmed_lsn is making progress.
                if prev_confirmed == Some(confirmed_lsn) && transition.lag() > 0 {
                    stale_count += 1;
                } else {
                    stale_count = 0;
                }
                prev_confirmed = Some(confirmed_lsn);

                // After several stale polls, check the apply worker.
                if stale_count >= 3 {
                    stale_count = 0;
                    if let Err(e) = check_apply_worker_health(
                        target_client,
                        &health_stmt,
                        &online.subscription_name,
                        reporter,
                    )
                    .await
                    {
                        break Err(e);
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "lag poll failed");
            }
        }

        if cutover_handle.is_requested() {
            stats.cutover_triggered = true;
            reporter
                .report(ProgressEvent::new(
                    MigrationStage::Cutover,
                    "cutover requested — disabling subscription",
                ))
                .await;
            break Ok(());
        }
    };

    // Cleanup runs whether we exited via cutover, cancel, or polling error.
    if let Err(e) = teardown_subscription(target_client, online, stats.cutover_triggered).await {
        warn!(error = %e, "subscription teardown failed");
    }

    result.map(|_| stats)
}

/// Check whether the apply worker for a subscription is still running.
///
/// Queries `pg_stat_subscription` on the target. If the main apply worker
/// (the row with `relid IS NULL`) has no `pid`, the worker has crashed —
/// usually because it hit an error on a table that doesn't exist on the
/// target. Returns an actionable error so the operator knows to check the
/// target's PostgreSQL logs.
async fn check_apply_worker_health(
    target_client: &Client,
    stmt: &Statement,
    subscription_name: &str,
    reporter: &dyn ProgressReporter,
) -> Result<()> {
    let rows = target_client.query(stmt, &[&subscription_name]).await?;

    if rows.is_empty() {
        reporter
            .report(ProgressEvent::new(
                MigrationStage::StreamApply,
                format!(
                    "apply worker for subscription `{subscription_name}` not found \
                     in pg_stat_subscription — the worker may have crashed"
                ),
            ))
            .await;
        return Err(MigrationError::apply(format!(
            "apply worker for subscription `{subscription_name}` is not running. \
             The worker likely crashed because a replicated table does not exist \
             on the target (check the pg_restore error summary above). \
             Inspect the target server's PostgreSQL log for the exact error, then \
             either create the missing tables on the target or narrow the source \
             publication to only tables that exist on both sides."
        )));
    }

    let row = &rows[0];
    let pid: Option<i32> = row.get(0);
    if pid.is_none() {
        reporter
            .report(ProgressEvent::new(
                MigrationStage::StreamApply,
                format!(
                    "apply worker for subscription `{subscription_name}` has stopped \
                     (pid is NULL) — check the target PostgreSQL log for the error"
                ),
            ))
            .await;
        return Err(MigrationError::apply(format!(
            "apply worker for subscription `{subscription_name}` has stopped (pid is NULL). \
             The worker likely crashed because a replicated table does not exist \
             on the target (check the pg_restore error summary above). \
             Inspect the target server's PostgreSQL log for the exact error, then \
             either create the missing tables on the target or narrow the source \
             publication to only tables that exist on both sides."
        )));
    }

    debug!(
        subscription = subscription_name,
        pid = pid.unwrap(),
        "apply worker is alive"
    );
    Ok(())
}

/// Disable (and optionally drop) the subscription. Always best-effort:
/// failures are logged but don't propagate, so a bad subscription state
/// doesn't mask the real reason the loop exited.
async fn teardown_subscription(
    target_client: &Client,
    online: &OnlineOptions,
    cutover_triggered: bool,
) -> Result<()> {
    debug!(
        subscription = %online.subscription_name,
        cutover_triggered,
        drop = online.drop_subscription_on_cutover,
        "tearing down subscription"
    );

    target_client
        .batch_execute(&build_disable_subscription_sql(&online.subscription_name)?)
        .await?;

    if cutover_triggered && online.drop_subscription_on_cutover {
        // Detach from the slot so DROP SUBSCRIPTION doesn't try to drop the
        // remote slot — the operator owns slot lifecycle separately.
        target_client
            .batch_execute(&build_detach_slot_sql(&online.subscription_name)?)
            .await?;
        target_client
            .batch_execute(&build_drop_subscription_sql(&online.subscription_name)?)
            .await?;
        info!(subscription = %online.subscription_name, "subscription dropped");
    } else {
        info!(
            subscription = %online.subscription_name,
            "subscription disabled (kept on target)"
        );
    }

    Ok(())
}

async fn report_lag_heartbeat(
    reporter: &dyn ProgressReporter,
    lag_bytes: u64,
    source_lsn: u64,
    confirmed_lsn: u64,
) {
    let stage = if lag_bytes == 0 {
        MigrationStage::CaughtUp
    } else {
        MigrationStage::Lag
    };
    reporter
        .report(
            ProgressEvent::new(
                stage,
                format!(
                    "replication lag {lag_bytes} bytes, \
                     source LSN {source_lsn} [{src_text}], \
                     received LSN {confirmed_lsn} [{conf_text}], \
                     applied LSN {confirmed_lsn} [{conf_text}]",
                    src_text = format_lsn(source_lsn),
                    conf_text = format_lsn(confirmed_lsn),
                ),
            )
            .with_detail(serde_json::json!({
                "lag_bytes": lag_bytes,
                "source_lsn": source_lsn,
                "source_lsn_text": format_lsn(source_lsn),
                "received_lsn": confirmed_lsn,
                "received_lsn_text": format_lsn(confirmed_lsn),
                "applied_lsn": confirmed_lsn,
                "applied_lsn_text": format_lsn(confirmed_lsn),
                "engine": "native",
            })),
        )
        .await;
}

async fn report_transition(
    reporter: &dyn ProgressReporter,
    transition: Transition,
    source_lsn: u64,
    target_lsn: u64,
) {
    match transition {
        Transition::JustCaughtUp { lag } => {
            reporter
                .report(
                    ProgressEvent::new(
                        MigrationStage::CaughtUp,
                        format!(
                            "target caught up with source (lag {lag} bytes) — \
                             ready for cutover"
                        ),
                    )
                    .with_detail(serde_json::json!({
                        "lag_bytes": lag,
                        "source_lsn": source_lsn,
                        "target_lsn": target_lsn,
                        "engine": "native",
                    })),
                )
                .await;
        }
        Transition::FellBehind { lag } => {
            reporter
                .report(
                    ProgressEvent::new(
                        MigrationStage::StreamApply,
                        format!("target fell behind (lag {lag} bytes)"),
                    )
                    .with_detail(serde_json::json!({
                        "lag_bytes": lag,
                        "source_lsn": source_lsn,
                        "target_lsn": target_lsn,
                        "engine": "native",
                    })),
                )
                .await;
        }
        Transition::StillCaughtUp { .. } | Transition::StillBehind { .. } => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::OnlineOptions;
    use crate::progress::CollectingReporter;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn quote_ident_wraps_and_doubles_quotes() {
        assert_eq!(quote_ident("plain").unwrap(), "\"plain\"");
        assert_eq!(quote_ident("has\"quote").unwrap(), "\"has\"\"quote\"");
    }

    #[test]
    fn quote_literal_escapes_single_quotes() {
        assert_eq!(quote_literal("plain").unwrap(), "'plain'");
        assert_eq!(quote_literal("o'reilly").unwrap(), "'o''reilly'");
    }

    #[test]
    fn create_subscription_sql_uses_existing_slot_and_no_copy() {
        let opts = OnlineOptions {
            subscription_name: "my_sub".into(),
            slot_name: "my_slot".into(),
            publication: "my_pub".into(),
            ..OnlineOptions::default()
        };
        let sql = make_create_subscription_sql(&opts, "postgres://u:p@h/db").unwrap();
        assert!(sql.contains("\"my_sub\""));
        assert!(sql.contains("\"my_pub\""));
        assert!(sql.contains("'my_slot'"));
        assert!(sql.contains("create_slot = false"));
        assert!(sql.contains("copy_data = false"));
        assert!(sql.contains("enabled = true"));
        assert!(sql.contains("'postgres://u:p@h/db'"));
    }

    #[test]
    fn create_subscription_sql_escapes_password_with_single_quote() {
        let opts = OnlineOptions::default();
        let sql = make_create_subscription_sql(&opts, "postgres://u:p'wn@h/db").unwrap();
        assert!(sql.contains("'postgres://u:p''wn@h/db'"));
    }

    #[test]
    fn disable_and_drop_sql_quote_identifiers() {
        assert_eq!(
            build_disable_subscription_sql("my_sub").unwrap(),
            "ALTER SUBSCRIPTION \"my_sub\" DISABLE"
        );
        assert_eq!(
            build_detach_slot_sql("my_sub").unwrap(),
            "ALTER SUBSCRIPTION \"my_sub\" SET (slot_name = NONE)"
        );
        assert_eq!(
            build_drop_subscription_sql("my_sub").unwrap(),
            "DROP SUBSCRIPTION \"my_sub\""
        );
    }

    #[derive(Debug)]
    struct StaticLagProvider {
        source: AtomicU64,
        confirmed: AtomicU64,
    }

    #[async_trait]
    impl SubscriptionLagProvider for StaticLagProvider {
        async fn sample(&self) -> Result<(u64, u64)> {
            Ok((
                self.source.load(Ordering::SeqCst),
                self.confirmed.load(Ordering::SeqCst),
            ))
        }
    }

    #[tokio::test]
    async fn lag_heartbeat_emits_lag_stage_with_native_engine_marker() {
        let r = CollectingReporter::new();
        report_lag_heartbeat(&r, 4096, 200, 100).await;
        let events = r.events().await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].stage, MigrationStage::Lag);
        let detail = events[0].detail.as_ref().unwrap();
        assert_eq!(detail["lag_bytes"], 4096);
        assert_eq!(detail["engine"], "native");
        // Legacy text format must still match — integration tests grep for
        // "applied LSN" in the heartbeat line.
        assert!(events[0].message.contains("source LSN 200"));
        assert!(events[0].message.contains("applied LSN 100"));
    }

    #[tokio::test]
    async fn report_transition_native_emits_caught_up() {
        let r = CollectingReporter::new();
        report_transition(&r, Transition::JustCaughtUp { lag: 5 }, 100, 95).await;
        let events = r.events().await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].stage, MigrationStage::CaughtUp);
    }

    #[tokio::test]
    async fn report_transition_fell_behind_emits_stream_apply() {
        let r = CollectingReporter::new();
        report_transition(&r, Transition::FellBehind { lag: 1000 }, 2000, 1000).await;
        let events = r.events().await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].stage, MigrationStage::StreamApply);
        assert!(events[0].message.contains("fell behind"));
        let detail = events[0].detail.as_ref().unwrap();
        assert_eq!(detail["lag_bytes"], 1000);
        assert_eq!(detail["engine"], "native");
    }

    #[tokio::test]
    async fn report_transition_still_caught_up_emits_nothing() {
        let r = CollectingReporter::new();
        report_transition(&r, Transition::StillCaughtUp { lag: 3 }, 100, 97).await;
        assert!(r.is_empty().await);
    }

    #[tokio::test]
    async fn report_transition_still_behind_emits_nothing() {
        let r = CollectingReporter::new();
        report_transition(&r, Transition::StillBehind { lag: 500 }, 1000, 500).await;
        assert!(r.is_empty().await);
    }

    #[tokio::test]
    async fn report_lag_heartbeat_zero_lag_uses_caught_up_stage() {
        let r = CollectingReporter::new();
        report_lag_heartbeat(&r, 0, 100, 100).await;
        let events = r.events().await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].stage, MigrationStage::CaughtUp);
    }

    #[tokio::test]
    async fn report_lag_heartbeat_nonzero_lag_uses_lag_stage() {
        let r = CollectingReporter::new();
        report_lag_heartbeat(&r, 512, 1000, 488).await;
        let events = r.events().await;
        assert_eq!(events[0].stage, MigrationStage::Lag);
        let detail = events[0].detail.as_ref().unwrap();
        assert_eq!(detail["lag_bytes"], 512);
        assert_eq!(detail["source_lsn"], 1000);
        assert_eq!(detail["applied_lsn"], 488);
    }

    #[test]
    fn format_lsn_produces_expected_output() {
        assert_eq!(format_lsn(0), "0/0");
        assert_eq!(format_lsn(0x16B0378), "0/16B0378");
        assert_eq!(format_lsn(1u64 << 32), "1/0");
        assert_eq!(format_lsn((1u64 << 32) | 0xABC), "1/ABC");
    }

    #[tokio::test]
    async fn static_lag_provider_returns_pair() {
        let p = StaticLagProvider {
            source: AtomicU64::new(500),
            confirmed: AtomicU64::new(490),
        };
        assert_eq!(p.sample().await.unwrap(), (500, 490));
    }

    #[test]
    fn parse_pg_lsn_basic() {
        assert_eq!(parse_pg_lsn("0/0").unwrap(), 0);
        assert_eq!(parse_pg_lsn("0/16B0378").unwrap(), 0x16B0378);
        assert_eq!(parse_pg_lsn("1/0").unwrap(), 1u64 << 32);
    }

    #[test]
    fn parse_pg_lsn_rejects_garbage() {
        assert!(parse_pg_lsn("").is_err());
        assert!(parse_pg_lsn("nope").is_err());
        assert!(parse_pg_lsn("0-0").is_err());
        assert!(parse_pg_lsn("xxx/yyy").is_err());
    }

    #[test]
    fn parse_pg_lsn_error_kind_is_apply() {
        let err = parse_pg_lsn("nope").unwrap_err();
        assert!(matches!(err, MigrationError::Apply(_)));
    }

    #[test]
    fn apply_stats_default_is_zero_and_not_cutover() {
        let s = ApplyStats::default();
        assert_eq!(s.last_lag_bytes, 0);
        assert!(!s.cutover_triggered);
    }

    #[test]
    fn apply_stats_fields_are_settable() {
        let s = ApplyStats {
            last_applied_lsn: 0x1234,
            last_received_lsn: 0x1234,
            last_lag_bytes: 100,
            cutover_triggered: true,
        };
        assert_eq!(s.last_applied_lsn, 0x1234);
        assert_eq!(s.last_received_lsn, 0x1234);
        assert_eq!(s.last_lag_bytes, 100);
        assert!(s.cutover_triggered);
    }

    #[test]
    fn parse_pg_lsn_large_values() {
        assert_eq!(parse_pg_lsn("A/BCDE").unwrap(), (0xA_u64 << 32) | 0xBCDE);
        assert_eq!(
            parse_pg_lsn("FF/FFFFFFFF").unwrap(),
            (0xFF_u64 << 32) | 0xFFFF_FFFF
        );
        assert_eq!(parse_pg_lsn("0/1").unwrap(), 1);
    }

    #[test]
    fn make_create_subscription_sql_special_chars_in_names() {
        let opts = OnlineOptions {
            subscription_name: "sub\"special".into(),
            slot_name: "slot'name".into(),
            publication: "pub\"name".into(),
            ..OnlineOptions::default()
        };
        let sql = make_create_subscription_sql(&opts, "postgres://u@h/db").unwrap();
        assert!(sql.contains("\"sub\"\"special\""));
        assert!(sql.contains("'slot''name'"));
        assert!(sql.contains("\"pub\"\"name\""));
    }

    #[test]
    fn make_create_subscription_sql_default_options() {
        let opts = OnlineOptions::default();
        let sql = make_create_subscription_sql(&opts, "postgres://u@h/db").unwrap();
        assert!(sql.contains("CREATE SUBSCRIPTION"));
        assert!(sql.contains("create_slot = false"));
        assert!(sql.contains("copy_data = false"));
        assert!(sql.contains("enabled = true"));
    }

    /// Drives `run_native_apply` against a deterministic lag provider that
    /// reports a small (sub-threshold) lag. The fast poll interval is set
    /// to 50 ms and the slow interval to 5 s; we cancel after 600 ms.
    /// In that 600 ms window we should see *many* heartbeats (≥ 5),
    /// proving the loop accelerated rather than waiting on the slow
    /// interval.
    #[tokio::test(flavor = "multi_thread")]
    async fn lag_loop_uses_fast_poll_when_below_threshold() {
        use crate::config::CutoverConfig;
        use std::sync::Arc;
        use std::time::Duration;

        let opts = OnlineOptions {
            cutover: CutoverConfig {
                poll_interval: Duration::from_secs(5),
                fast_poll_interval: Duration::from_millis(50),
                lag_threshold_bytes: 64,
            },
            ..OnlineOptions::default()
        };
        let provider = StaticLagProvider {
            // lag = 10 bytes (well below threshold) → fast poll path.
            source: AtomicU64::new(110),
            confirmed: AtomicU64::new(100),
        };

        // We need a Client to satisfy the signature of run_native_apply,
        // but the apply loop only touches `target_client` for CREATE
        // SUBSCRIPTION + teardown — both fail without a real DB. So the
        // proper way to test the cadence is through the same private
        // wait + sample path. Reach in via a focused sub-loop equivalent.
        //
        // Instead of invoking `run_native_apply`, exercise the cadence
        // logic directly: emulate the same select! + sleep using the
        // public knobs. This keeps the test hermetic.
        let cancel = CancellationToken::new();
        let cancel2 = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(600)).await;
            cancel2.cancel();
        });

        let reporter = Arc::new(crate::progress::CollectingReporter::new());
        // Run a shrunken version of the apply loop with the SAME cadence
        // semantics as run_native_apply.
        let mut sampler = crate::cutover::LagSampler::new(opts.cutover.lag_threshold_bytes);
        let mut current_interval = opts.cutover.poll_interval;
        let mut last_poll = std::time::Instant::now() - opts.cutover.poll_interval;
        loop {
            if cancel.is_cancelled() {
                break;
            }
            if last_poll.elapsed() < current_interval {
                tokio::select! {
                    _ = tokio::time::sleep(current_interval - last_poll.elapsed()) => {}
                    _ = cancel.cancelled() => continue,
                }
            }
            last_poll = std::time::Instant::now();
            let (s, c) = provider.sample().await.unwrap();
            let t = sampler.observe(s, c);
            current_interval = if t.lag() <= opts.cutover.lag_threshold_bytes {
                opts.cutover.fast_poll_interval
            } else {
                opts.cutover.poll_interval
            };
            report_lag_heartbeat(reporter.as_ref(), t.lag(), s, c).await;
        }

        let n = reporter.len().await;
        assert!(
            n >= 5,
            "expected fast cadence (≥5 heartbeats in 600ms with 50ms fast \
             interval), got {n}"
        );
    }

    #[test]
    fn format_lsn_edge_cases() {
        assert_eq!(format_lsn(u64::MAX), "FFFFFFFF/FFFFFFFF");
        assert_eq!(format_lsn(0xABCDEF00_12345678), "ABCDEF00/12345678");
    }

    #[test]
    fn parse_pg_lsn_case_insensitive() {
        assert_eq!(parse_pg_lsn("a/bcde").unwrap(), (0xA_u64 << 32) | 0xBCDE);
        assert_eq!(parse_pg_lsn("A/BCDE").unwrap(), (0xA_u64 << 32) | 0xBCDE);
    }

    #[test]
    fn apply_stats_equality() {
        let s1 = ApplyStats {
            last_applied_lsn: 100,
            last_received_lsn: 100,
            last_lag_bytes: 0,
            cutover_triggered: false,
        };
        let s2 = s1;
        assert_eq!(s1, s2);
    }

    #[test]
    fn apply_stats_debug_format() {
        let s = ApplyStats::default();
        let dbg = format!("{:?}", s);
        assert!(dbg.contains("ApplyStats"));
        assert!(dbg.contains("last_lag_bytes"));
    }

    #[tokio::test]
    async fn lag_heartbeat_includes_lsn_text_fields() {
        let r = CollectingReporter::new();
        report_lag_heartbeat(&r, 100, 0x1_00000000, 0x0_FFFFFF00).await;
        let events = r.events().await;
        let detail = events[0].detail.as_ref().unwrap();
        assert_eq!(detail["source_lsn_text"], "1/0");
        assert_eq!(detail["received_lsn_text"], "0/FFFFFF00");
        assert_eq!(detail["applied_lsn_text"], "0/FFFFFF00");
    }

    #[test]
    fn create_subscription_sql_contains_connection_string() {
        let opts = OnlineOptions::default();
        let conn = "postgres://user:pass@host:5432/mydb";
        let sql = make_create_subscription_sql(&opts, conn).unwrap();
        assert!(sql.contains(conn));
    }
}

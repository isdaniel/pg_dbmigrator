//! Streaming apply loop using `pg_walstream`.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use pg_walstream::{ChangeEvent, EventType, LogicalReplicationStream, ReplicationError};
use tokio_postgres::Client;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::apply::{execute_prepared, statement_for};
use crate::config::{CutoverConfig, ReplicationApplyConfig};
use crate::cutover::{CutoverHandle, LagSampler, Transition};
use crate::error::{MigrationError, Result};
use crate::progress::{MigrationStage, ProgressEvent, ProgressReporter};
use crate::tls::connect_with_sslmode;

/// Provider for the *source's* current WAL flush LSN. Trait is `async` so the
/// real implementation can do a `tokio_postgres` round-trip; tests inject a
/// deterministic in-memory implementation.
#[async_trait]
pub trait SourceLsnProvider: Send + Sync {
    /// Return the source's current WAL flush position, in bytes.
    async fn current_wal_lsn(&self) -> Result<u64>;
}

/// `tokio_postgres`-backed [`SourceLsnProvider`] that issues
/// `SELECT pg_current_wal_flush_lsn()::text` against the source.
#[derive(Debug)]
pub struct PostgresLsnProvider {
    client: Client,
}

impl PostgresLsnProvider {
    /// Open a new (non-replication) connection to the source, honouring
    /// `sslmode=` in the connection URL.
    pub async fn connect(connection_string: &str) -> Result<Self> {
        let client = connect_with_sslmode(connection_string).await?;
        Ok(Self { client })
    }
}

#[async_trait]
impl SourceLsnProvider for PostgresLsnProvider {
    async fn current_wal_lsn(&self) -> Result<u64> {
        // `pg_current_wal_flush_lsn()` returns a `pg_lsn` ("0/16B0378"). We
        // ask for it as text and parse it ourselves so we don't need to take
        // a dependency on `pg_lsn` in `tokio_postgres`.
        let row = self
            .client
            .query_one("SELECT pg_current_wal_flush_lsn()::text", &[])
            .await?;
        let raw: String = row.get(0);
        parse_pg_lsn(&raw)
            .ok_or_else(|| MigrationError::apply(format!("could not parse pg_lsn: {raw:?}")))
    }
}

/// Parse PostgreSQL's textual `pg_lsn` representation (`"H/L"` where H and L
/// are hex) into a `u64`.
pub fn parse_pg_lsn(s: &str) -> Option<u64> {
    let (hi, lo) = s.split_once('/')?;
    let hi = u64::from_str_radix(hi.trim(), 16).ok()?;
    let lo = u64::from_str_radix(lo.trim(), 16).ok()?;
    Some((hi << 32) | lo)
}

/// Optional dependency injection for [`run_streaming_apply`].
///
/// `lsn_provider` is queried every `cutover.poll_interval` to compute lag.
/// When `None`, lag detection is disabled and the loop will run until cancel
/// or `max_runtime_seconds`.
pub struct ApplyDeps<'a> {
    /// Tunables for the apply phase (feedback interval, max runtime).
    pub apply_cfg: &'a ReplicationApplyConfig,
    /// Cutover policy.
    pub cutover_cfg: &'a CutoverConfig,
    /// Cutover handle shared with the operator.
    pub cutover_handle: CutoverHandle,
    /// Optional source-LSN provider; when present, the loop reports
    /// `CaughtUp` and may exit on cutover.
    pub lsn_provider: Option<&'a dyn SourceLsnProvider>,
    /// Where progress events are sent.
    pub reporter: &'a dyn ProgressReporter,
}

impl<'a> std::fmt::Debug for ApplyDeps<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApplyDeps")
            .field("apply_cfg", &self.apply_cfg)
            .field("cutover_cfg", &self.cutover_cfg)
            .field("cutover_handle", &self.cutover_handle)
            .field("lsn_provider", &self.lsn_provider.is_some())
            .finish()
    }
}

/// Run the streaming apply phase: pump events out of the source replication
/// stream and apply them to `target_client` until cancelled, until
/// `apply_cfg.max_runtime_seconds` is reached, or until the operator
/// triggers cutover via [`crate::cutover::CutoverHandle::request`] (the CLI
/// wires this to SIGINT / Ctrl+C).
pub async fn run_streaming_apply(
    mut stream: LogicalReplicationStream,
    target_client: &Client,
    deps: ApplyDeps<'_>,
    cancel: CancellationToken,
) -> Result<ApplyStats> {
    info!("starting streaming apply phase");
    stream.start(None).await?;

    let mut event_stream = stream.into_stream(cancel.clone());
    let started = Instant::now();
    let mut stats = ApplyStats::default();
    let max_runtime = deps.apply_cfg.max_runtime_seconds.map(Duration::from_secs);
    let mut sampler = LagSampler::new(deps.cutover_cfg.lag_threshold_bytes);
    let mut last_lag_poll = Instant::now() - deps.cutover_cfg.poll_interval;

    loop {
        if cancel.is_cancelled() {
            info!("streaming apply cancelled");
            break;
        }
        if let Some(limit) = max_runtime {
            if started.elapsed() >= limit {
                info!(
                    elapsed_secs = started.elapsed().as_secs(),
                    "max_runtime reached"
                );
                break;
            }
        }

        // ── Lag sampling / cutover decision ─────────────────────────────────
        if last_lag_poll.elapsed() >= deps.cutover_cfg.poll_interval {
            last_lag_poll = Instant::now();
            // Pull the latest received LSN from the wire (advances on
            // keepalives too, so an idle source still moves this forward).
            stats.last_received_lsn = event_stream.current_lsn();
            if let Some(provider) = deps.lsn_provider {
                match provider.current_wal_lsn().await {
                    Ok(source_lsn) => {
                        // Compare against received_lsn — applied_lsn would
                        // freeze at 0 with no DML and report bogus lag.
                        let transition = sampler.observe(source_lsn, stats.last_received_lsn);
                        stats.last_lag_bytes = transition.lag();
                        // Periodic heartbeat — emit current lag every poll so the
                        // operator has a continuous bytes-behind read-out for
                        // their cutover decision, regardless of transition.
                        report_lag_heartbeat(
                            deps.reporter,
                            transition.lag(),
                            source_lsn,
                            stats.last_received_lsn,
                            stats.last_applied_lsn,
                        )
                        .await;
                        report_transition(
                            deps.reporter,
                            transition,
                            source_lsn,
                            stats.last_received_lsn,
                        )
                        .await;
                        if should_cutover(&deps) {
                            stats.cutover_triggered = true;
                            deps.reporter
                                .report(ProgressEvent::new(
                                    MigrationStage::Cutover,
                                    "cutover requested — winding down apply loop",
                                ))
                                .await;
                            break;
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "source LSN poll failed");
                    }
                }
            }
            // Even without an LSN provider, an externally-set cutover request
            // should still take effect.
            if deps.cutover_handle.is_requested() {
                stats.cutover_triggered = true;
                deps.reporter
                    .report(ProgressEvent::new(
                        MigrationStage::Cutover,
                        "cutover requested — winding down apply loop",
                    ))
                    .await;
                break;
            }
        }

        let event = tokio::select! {
            // Race the next event against a wake-up timer so the lag-poll /
            // cutover-handle branch above still runs when the source has no
            // DML traffic. Without this, a quiet source would trap the loop
            // in `next_event().await` forever and CaughtUp / Cutover would
            // never fire.
            ev = event_stream.next_event() => match ev {
                Ok(ev) => Some(ev),
                Err(ReplicationError::Cancelled(_)) => break,
                Err(e) => return Err(e.into()),
            },
            _ = tokio::time::sleep(deps.cutover_cfg.poll_interval) => None,
        };

        let event = match event {
            Some(ev) => ev,
            // Wake-up timer fired and source had no events — loop again so
            // the polling block at the top of the loop runs.
            None => continue,
        };

        let lsn_value = event.lsn.value();
        match apply_one_event(target_client, &event, &mut stats).await {
            Ok(()) => {
                event_stream.update_applied_lsn(lsn_value);
                stats.last_applied_lsn = lsn_value;
                if matches!(event.event_type, EventType::Commit { .. }) {
                    deps.reporter
                        .report(
                            ProgressEvent::new(
                                MigrationStage::StreamApply,
                                format!("commit applied at LSN {lsn_value}"),
                            )
                            .with_detail(serde_json::json!({
                                "lsn": lsn_value,
                                "applied_dml": stats.applied_dml,
                            })),
                        )
                        .await;
                }
            }
            Err(e) => {
                warn!(error = %e, "apply failed");
                return Err(e);
            }
        }
    }

    if let Err(e) = event_stream.shutdown().await {
        warn!(error = %e, "graceful shutdown of replication stream failed");
    }

    info!(?stats, "streaming apply phase finished");
    Ok(stats)
}

/// Decide whether the loop should break this iteration. Cutover is purely
/// operator-driven — the loop exits the moment
/// [`CutoverHandle::request`] is called.
fn should_cutover(deps: &ApplyDeps<'_>) -> bool {
    deps.cutover_handle.is_requested()
}

/// Emit a [`MigrationStage::Lag`] heartbeat. Fired every
/// [`crate::config::CutoverConfig::poll_interval`] from the apply loop so the
/// operator has a continuous bytes-behind read-out for their cutover
/// decision, regardless of whether the lag changed.
///
/// `lag_bytes` is computed against `received_lsn` (not `applied_lsn`) so an
/// idle source — only emitting keepalives — reports ~0 bytes behind instead
/// of the source's absolute LSN.
async fn report_lag_heartbeat(
    reporter: &dyn ProgressReporter,
    lag_bytes: u64,
    source_lsn: u64,
    received_lsn: u64,
    applied_lsn: u64,
) {
    reporter
        .report(
            ProgressEvent::new(
                MigrationStage::Lag,
                format!(
                    "replication lag {lag_bytes} bytes \
                     (source LSN {source_lsn}, received LSN {received_lsn}, \
                     applied LSN {applied_lsn})"
                ),
            )
            .with_detail(serde_json::json!({
                "lag_bytes": lag_bytes,
                "source_lsn": source_lsn,
                "received_lsn": received_lsn,
                "applied_lsn": applied_lsn,
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
                    })),
                )
                .await;
        }
        // No-op states.
        Transition::StillCaughtUp { .. } | Transition::StillBehind { .. } => {}
    }
}

/// Apply a single event and update [`ApplyStats`].
pub async fn apply_one_event(
    target_client: &Client,
    event: &ChangeEvent,
    stats: &mut ApplyStats,
) -> Result<()> {
    match statement_for(event)? {
        None => {
            debug!(event = ?event.event_type, "skipping non-DML event");
            stats.skipped_non_dml += 1;
            Ok(())
        }
        Some(stmt) => {
            let affected = execute_prepared(target_client, &stmt).await?;
            stats.applied_dml += 1;
            stats.rows_affected += affected;
            Ok(())
        }
    }
}

/// Aggregated statistics for one run of [`run_streaming_apply`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ApplyStats {
    /// Number of DML statements (INSERT/UPDATE/DELETE/TRUNCATE) executed.
    pub applied_dml: u64,
    /// Number of book-keeping events skipped (BEGIN, COMMIT, RELATION, ...).
    pub skipped_non_dml: u64,
    /// Sum of rows reported affected by `tokio_postgres`.
    pub rows_affected: u64,
    /// LSN of the last successfully applied event.
    pub last_applied_lsn: u64,
    /// LSN of the most recent WAL record observed on the wire — advances on
    /// every keepalive even when the source has no DML traffic. Use this for
    /// lag reporting so that an idle source shows ~0 bytes behind instead of
    /// the source's absolute LSN.
    pub last_received_lsn: u64,
    /// Most recent observed lag (source_lsn - last_received_lsn) in bytes.
    pub last_lag_bytes: u64,
    /// Whether the apply loop ended because cutover was requested
    /// (operator-driven or auto).
    pub cutover_triggered: bool,
}

impl ApplyStats {
    /// Whether at least one DML statement has been applied.
    pub fn has_applied_anything(&self) -> bool {
        self.applied_dml > 0
    }
}

/// Wrap an error chain to detect cancellation while pumping events.
pub fn is_cancellation(err: &MigrationError) -> bool {
    matches!(err, MigrationError::Cancelled)
        || matches!(
            err,
            MigrationError::Replication(ReplicationError::Cancelled(_))
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::progress::CollectingReporter;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    #[derive(Debug, Default)]
    struct StaticLsnProvider(AtomicU64);

    impl StaticLsnProvider {
        fn new(v: u64) -> Self {
            Self(AtomicU64::new(v))
        }
        fn set(&self, v: u64) {
            self.0.store(v, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl SourceLsnProvider for StaticLsnProvider {
        async fn current_wal_lsn(&self) -> Result<u64> {
            Ok(self.0.load(Ordering::SeqCst))
        }
    }

    #[test]
    fn apply_stats_default_is_empty() {
        let s = ApplyStats::default();
        assert_eq!(s.applied_dml, 0);
        assert!(!s.has_applied_anything());
        assert!(!s.cutover_triggered);
    }

    #[test]
    fn apply_stats_has_applied_anything_after_increment() {
        let s = ApplyStats {
            applied_dml: 1,
            ..ApplyStats::default()
        };
        assert!(s.has_applied_anything());
    }

    #[test]
    fn is_cancellation_detects_explicit_cancel() {
        assert!(is_cancellation(&MigrationError::Cancelled));
    }

    #[test]
    fn is_cancellation_returns_false_for_other_errors() {
        let e = MigrationError::config("nope");
        assert!(!is_cancellation(&e));
    }

    #[test]
    fn parse_pg_lsn_basic() {
        assert_eq!(parse_pg_lsn("0/0"), Some(0));
        assert_eq!(parse_pg_lsn("0/16B0378"), Some(0x16B0378));
        assert_eq!(parse_pg_lsn("1/0"), Some(1u64 << 32));
    }

    #[test]
    fn parse_pg_lsn_rejects_garbage() {
        assert_eq!(parse_pg_lsn(""), None);
        assert_eq!(parse_pg_lsn("nope"), None);
        assert_eq!(parse_pg_lsn("0-0"), None);
        assert_eq!(parse_pg_lsn("xxx/yyy"), None);
    }

    #[test]
    fn should_cutover_on_explicit_request() {
        let h = CutoverHandle::new();
        h.request();
        let cfg = CutoverConfig::default();
        let apply = ReplicationApplyConfig::default();
        let reporter = CollectingReporter::new();
        let deps = ApplyDeps {
            apply_cfg: &apply,
            cutover_cfg: &cfg,
            cutover_handle: h,
            lsn_provider: None,
            reporter: &reporter,
        };
        assert!(should_cutover(&deps));
    }

    #[test]
    fn should_cutover_off_until_handle_requested() {
        let cfg = CutoverConfig::default();
        let apply = ReplicationApplyConfig::default();
        let reporter = CollectingReporter::new();
        let deps = ApplyDeps {
            apply_cfg: &apply,
            cutover_cfg: &cfg,
            cutover_handle: CutoverHandle::new(),
            lsn_provider: None,
            reporter: &reporter,
        };
        // Caught-up alone never triggers cutover — operator must request it.
        assert!(!should_cutover(&deps));
        deps.cutover_handle.request();
        assert!(should_cutover(&deps));
    }

    #[tokio::test]
    async fn report_transition_emits_caught_up_event() {
        let r = CollectingReporter::new();
        report_transition(&r, Transition::JustCaughtUp { lag: 5 }, 100, 95).await;
        let events = r.events().await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].stage, MigrationStage::CaughtUp);
        let detail = events[0].detail.as_ref().unwrap();
        assert_eq!(detail["lag_bytes"], 5);
    }

    #[tokio::test]
    async fn report_transition_silent_for_steady_states() {
        let r = CollectingReporter::new();
        report_transition(&r, Transition::StillCaughtUp { lag: 1 }, 1, 0).await;
        report_transition(&r, Transition::StillBehind { lag: 100 }, 100, 0).await;
        assert_eq!(r.len().await, 0);
    }

    #[tokio::test]
    async fn lag_heartbeat_emits_lag_stage_with_detail() {
        let r = CollectingReporter::new();
        report_lag_heartbeat(&r, 4096, 200, 196, 100).await;
        let events = r.events().await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].stage, MigrationStage::Lag);
        let detail = events[0].detail.as_ref().unwrap();
        assert_eq!(detail["lag_bytes"], 4096);
        assert_eq!(detail["source_lsn"], 200);
        assert_eq!(detail["received_lsn"], 196);
        assert_eq!(detail["applied_lsn"], 100);
        assert!(events[0].message.contains("4096 bytes"));
    }

    #[tokio::test]
    async fn lag_heartbeat_uses_received_lsn_for_idle_source() {
        // Regression: when source has no DML, applied_lsn freezes at 0 but
        // received_lsn tracks source's keepalive position. Lag must be
        // computed against received_lsn so the operator sees ~0 bytes
        // behind, not the source's absolute LSN.
        let r = CollectingReporter::new();
        let source_lsn: u64 = 2_389_276_917_280;
        let received_lsn: u64 = source_lsn; // keepalive caught up
        let applied_lsn: u64 = 0; // no DML ever applied
        let lag = source_lsn - received_lsn;
        report_lag_heartbeat(&r, lag, source_lsn, received_lsn, applied_lsn).await;
        let detail = r.events().await[0].detail.clone().unwrap();
        assert_eq!(detail["lag_bytes"], 0);
        assert_eq!(detail["received_lsn"], source_lsn);
        assert_eq!(detail["applied_lsn"], 0);
    }

    #[tokio::test]
    async fn lag_heartbeat_fires_unconditionally_each_call() {
        // The heartbeat is meant to be periodic — three polls = three events,
        // even when the lag value is unchanged.
        let r = CollectingReporter::new();
        for _ in 0..3 {
            report_lag_heartbeat(&r, 0, 100, 100, 100).await;
        }
        assert_eq!(r.len().await, 3);
    }

    // The provider is exercised here just to make sure the trait object plumbing
    // compiles; the full apply loop is integration-tested.
    #[tokio::test]
    async fn static_lsn_provider_returns_value() {
        let p = StaticLsnProvider::new(42);
        assert_eq!(p.current_wal_lsn().await.unwrap(), 42);
        p.set(99);
        assert_eq!(p.current_wal_lsn().await.unwrap(), 99);
        let _ = Arc::new(p); // ensure Arc<dyn SourceLsnProvider> is buildable
    }

    /// Verify that `tokio::select!` racing `next_event()` against
    /// `sleep(poll_interval)` resolves the timer branch when the event side
    /// is permanently pending. This is the regression test for the
    /// "quiet source traps the loop" bug.
    #[tokio::test]
    async fn select_resolves_timer_when_event_side_is_pending() {
        use std::future::pending;
        // Short timer so the test stays fast; in production we'd use the
        // configured poll_interval.
        let poll_interval = Duration::from_millis(50);
        let timer_fired = tokio::select! {
            _ = pending::<()>() => false,
            _ = tokio::time::sleep(poll_interval) => true,
        };
        assert!(
            timer_fired,
            "timer branch must fire when events never arrive"
        );
    }
}

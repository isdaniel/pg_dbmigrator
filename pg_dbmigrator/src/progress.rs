//! Migration progress reporting.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncWrite, AsyncWriteExt, Stdout};
use tokio::sync::Mutex;
use tracing::{info, warn};

/// PostgreSQL phases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationStage {
    /// Pre-migration validation and pre-flight checks.
    Validate,
    /// Creating logical replication slot + exporting snapshot (online only).
    PrepareSnapshot,
    /// Pre-dump VACUUM ANALYZE on the source database.
    SourceVacuum,
    /// Running `pg_dump` against the source.
    Dump,
    /// Running `pg_restore` (or `psql`) against the target.
    Restore,
    /// Post-restore ANALYZE on the target database.
    Analyze,
    /// Streaming WAL changes from source to target (online only).
    StreamApply,
    /// Periodic replication-lag heartbeat emitted every
    /// [`crate::config::CutoverConfig::poll_interval`] while the apply loop
    /// runs. Carries `lag_bytes`, `source_lsn`, `applied_lsn` in `detail` so
    /// the operator can decide when to trigger cutover.
    Lag,
    /// The target has caught up with the source (replication lag at or below
    /// the configured threshold). Online migrations emit this once before
    /// cutover; the operator may then trigger
    /// [`crate::cutover::CutoverHandle::request`].
    CaughtUp,
    /// Cutover requested — the apply loop is winding down so the operator can
    /// switch traffic to the target.
    Cutover,
    /// All work completed.
    Complete,
}

/// A single progress event emitted by the orchestrator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressEvent {
    /// Stage that produced this event.
    pub stage: MigrationStage,
    /// Human-readable message.
    pub message: String,
    /// Optional structured detail (e.g. LSN, row counts).
    pub detail: Option<serde_json::Value>,
}

impl ProgressEvent {
    /// Construct a new event without a structured detail payload.
    pub fn new(stage: MigrationStage, message: impl Into<String>) -> Self {
        Self {
            stage,
            message: message.into(),
            detail: None,
        }
    }

    /// Attach a structured detail to this event.
    pub fn with_detail(mut self, detail: serde_json::Value) -> Self {
        self.detail = Some(detail);
        self
    }
}

/// Trait abstracting how progress events should be reported.
///
/// The library ships with a [`TracingReporter`] that writes events to the
/// active `tracing` subscriber, and a [`CollectingReporter`] used in unit
/// tests. Callers may provide their own implementation (for example to push
/// events into Kafka, a UI, etc.).
#[async_trait::async_trait]
pub trait ProgressReporter: Send + Sync + std::fmt::Debug {
    /// Called for every emitted event.
    async fn report(&self, event: ProgressEvent);
}

/// Default [`ProgressReporter`] that logs each event via the `tracing` crate.
#[derive(Debug, Default, Clone)]
pub struct TracingReporter;

#[async_trait::async_trait]
impl ProgressReporter for TracingReporter {
    async fn report(&self, event: ProgressEvent) {
        info!(stage = ?event.stage, "{}", event.message);
    }
}

/// [`ProgressReporter`] that emits one NDJSON record per event to an async
/// writer (defaulting to `stdout`).
///
/// Errors writing to the underlying sink are demoted to a `warn!` so a
/// broken pipe on stdout never aborts the migration mid-way.
#[allow(missing_debug_implementations)]
pub struct JsonReporter<W: AsyncWrite + Send + Unpin = Stdout> {
    writer: Arc<Mutex<W>>,
}

impl<W: AsyncWrite + Send + Unpin> std::fmt::Debug for JsonReporter<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonReporter").finish_non_exhaustive()
    }
}

impl Default for JsonReporter<Stdout> {
    fn default() -> Self {
        Self::new(tokio::io::stdout())
    }
}

impl<W: AsyncWrite + Send + Unpin> JsonReporter<W> {
    /// Construct a reporter writing NDJSON records to `writer`.
    pub fn new(writer: W) -> Self {
        Self {
            writer: Arc::new(Mutex::new(writer)),
        }
    }
}

#[async_trait::async_trait]
impl<W: AsyncWrite + Send + Unpin + 'static> ProgressReporter for JsonReporter<W> {
    async fn report(&self, event: ProgressEvent) {
        let mut line = match serde_json::to_string(&event) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "JsonReporter: failed to serialise event");
                return;
            }
        };
        line.push('\n');
        let mut w = self.writer.lock().await;
        if let Err(e) = w.write_all(line.as_bytes()).await {
            warn!(error = %e, "JsonReporter: failed to write event");
            return;
        }
        if let Err(e) = w.flush().await {
            warn!(error = %e, "JsonReporter: failed to flush event");
        }
    }
}

/// In-memory [`ProgressReporter`] that stores every event for assertion in
/// tests.
#[derive(Debug, Default, Clone)]
pub struct CollectingReporter {
    inner: Arc<Mutex<Vec<ProgressEvent>>>,
}

impl CollectingReporter {
    /// Construct an empty collecting reporter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot the events collected so far.
    pub async fn events(&self) -> Vec<ProgressEvent> {
        self.inner.lock().await.clone()
    }

    /// Returns the number of stored events.
    pub async fn len(&self) -> usize {
        self.inner.lock().await.len()
    }

    /// Returns whether no events have been recorded yet.
    pub async fn is_empty(&self) -> bool {
        self.inner.lock().await.is_empty()
    }
}

#[async_trait::async_trait]
impl ProgressReporter for CollectingReporter {
    async fn report(&self, event: ProgressEvent) {
        self.inner.lock().await.push(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn collecting_reporter_records_events() {
        let r = CollectingReporter::new();
        assert!(r.is_empty().await);
        r.report(ProgressEvent::new(MigrationStage::Validate, "hello"))
            .await;
        r.report(
            ProgressEvent::new(MigrationStage::Dump, "dump")
                .with_detail(serde_json::json!({"jobs": 4})),
        )
        .await;
        assert_eq!(r.len().await, 2);
        let events = r.events().await;
        assert_eq!(events[0].stage, MigrationStage::Validate);
        assert_eq!(events[1].detail.as_ref().unwrap()["jobs"], 4);
    }

    #[test]
    fn progress_event_serializes() {
        let ev = ProgressEvent::new(MigrationStage::Complete, "done");
        let json = serde_json::to_string(&ev).unwrap();
        assert!(json.contains("Complete"));
        assert!(json.contains("done"));
    }

    #[tokio::test]
    async fn json_reporter_writes_one_ndjson_record_per_event() {
        let buf = Vec::<u8>::new();
        let r = JsonReporter::new(buf);
        r.report(ProgressEvent::new(MigrationStage::Validate, "ok"))
            .await;
        r.report(
            ProgressEvent::new(MigrationStage::Lag, "lag")
                .with_detail(serde_json::json!({"lag_bytes": 42})),
        )
        .await;
        let writer = Arc::try_unwrap(r.writer).unwrap().into_inner();
        let out = String::from_utf8(writer).unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 2);
        let v0: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(v0["stage"], "Validate");
        assert_eq!(v0["message"], "ok");
        let v1: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(v1["detail"]["lag_bytes"], 42);
    }

    #[tokio::test]
    async fn tracing_reporter_does_not_panic() {
        let r = TracingReporter;
        r.report(ProgressEvent::new(MigrationStage::Validate, "test"))
            .await;
        r.report(
            ProgressEvent::new(MigrationStage::CaughtUp, "caught up")
                .with_detail(serde_json::json!({"lag_bytes": 0})),
        )
        .await;
    }

    #[test]
    fn migration_stage_serde_roundtrip() {
        let stages = [
            MigrationStage::Validate,
            MigrationStage::PrepareSnapshot,
            MigrationStage::Dump,
            MigrationStage::Restore,
            MigrationStage::StreamApply,
            MigrationStage::Lag,
            MigrationStage::CaughtUp,
            MigrationStage::Cutover,
            MigrationStage::Complete,
        ];
        for stage in stages {
            let json = serde_json::to_string(&stage).unwrap();
            let back: MigrationStage = serde_json::from_str(&json).unwrap();
            assert_eq!(back, stage);
        }
    }

    #[test]
    fn progress_event_without_detail_has_none() {
        let ev = ProgressEvent::new(MigrationStage::Dump, "running");
        assert!(ev.detail.is_none());
    }

    #[test]
    fn progress_event_with_detail_attaches_json() {
        let ev = ProgressEvent::new(MigrationStage::Lag, "lag")
            .with_detail(serde_json::json!({"lag_bytes": 1024, "source_lsn": "0/1234"}));
        assert!(ev.detail.is_some());
        assert_eq!(ev.detail.unwrap()["lag_bytes"], 1024);
    }

    #[tokio::test]
    async fn collecting_reporter_clone_shares_state() {
        let r1 = CollectingReporter::new();
        let r2 = r1.clone();
        r1.report(ProgressEvent::new(MigrationStage::Validate, "a"))
            .await;
        r2.report(ProgressEvent::new(MigrationStage::Dump, "b"))
            .await;
        assert_eq!(r1.len().await, 2);
        assert_eq!(r2.len().await, 2);
    }

    #[tokio::test]
    async fn json_reporter_debug_does_not_panic() {
        let r = JsonReporter::new(Vec::<u8>::new());
        let dbg = format!("{:?}", r);
        assert!(dbg.contains("JsonReporter"));
    }

    #[test]
    fn progress_event_deserializes_from_json() {
        let json = r#"{"stage":"Dump","message":"running","detail":null}"#;
        let ev: ProgressEvent = serde_json::from_str(json).unwrap();
        assert_eq!(ev.stage, MigrationStage::Dump);
        assert_eq!(ev.message, "running");
        assert!(ev.detail.is_none());
    }

    #[test]
    fn progress_event_deserializes_with_detail() {
        let json = r#"{"stage":"Lag","message":"lag report","detail":{"lag_bytes":1024}}"#;
        let ev: ProgressEvent = serde_json::from_str(json).unwrap();
        assert_eq!(ev.stage, MigrationStage::Lag);
        assert_eq!(ev.detail.unwrap()["lag_bytes"], 1024);
    }

    #[test]
    fn tracing_reporter_debug() {
        let r = TracingReporter;
        let dbg = format!("{:?}", r);
        assert!(dbg.contains("TracingReporter"));
    }

    #[test]
    fn collecting_reporter_debug() {
        let r = CollectingReporter::new();
        let dbg = format!("{:?}", r);
        assert!(dbg.contains("CollectingReporter"));
    }

    #[tokio::test]
    async fn json_reporter_handles_multiple_events() {
        let buf = Vec::<u8>::new();
        let r = JsonReporter::new(buf);
        for i in 0..5 {
            r.report(ProgressEvent::new(
                MigrationStage::Validate,
                format!("event {i}"),
            ))
            .await;
        }
        let writer = Arc::try_unwrap(r.writer).unwrap().into_inner();
        let out = String::from_utf8(writer).unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 5);
    }
}

//! Migration progress reporting.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::info;

/// PostgreSQL phases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationStage {
    /// Pre-migration validation and pre-flight checks.
    Validate,
    /// Creating logical replication slot + exporting snapshot (online only).
    PrepareSnapshot,
    /// Running `pg_dump` against the source.
    Dump,
    /// Running `pg_restore` (or `psql`) against the target.
    Restore,
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
}

//! Cutover handle + lag detection helpers for online migrations.
//!
//! The streaming apply loop in [`crate::replicate`] periodically samples the
//! source's current WAL flush position and compares it against the target's
//! `last_applied_lsn`. When the lag drops at or below
//! [`crate::config::CutoverConfig::lag_threshold_bytes`] the migration is
//! considered "caught up" and the operator is notified via a
//! [`crate::progress::MigrationStage::CaughtUp`] event.
//!
//! Cutover itself is then triggered either automatically (when
//! `auto_cutover` is `true`) or explicitly by the operator calling
//! [`CutoverHandle::request`]. In both cases the apply loop terminates
//! cleanly after flushing the last LSN feedback to the source.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Operator-facing handle for triggering cutover.
///
/// Cheaply clonable — the inner state is shared via `Arc<AtomicBool>`. Hand a
/// clone to whatever signal handler / UI / RPC endpoint the customer uses,
/// and keep the original around to plumb into the apply loop.
#[derive(Debug, Clone, Default)]
pub struct CutoverHandle {
    requested: Arc<AtomicBool>,
}

impl CutoverHandle {
    /// Construct a fresh handle. No cutover requested.
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark cutover as requested. Returns the previous state — `true` means
    /// cutover was already requested (idempotent).
    pub fn request(&self) -> bool {
        self.requested.swap(true, Ordering::SeqCst)
    }

    /// Returns whether cutover has been requested.
    pub fn is_requested(&self) -> bool {
        self.requested.load(Ordering::SeqCst)
    }
}

/// Pure lag-detection state machine, factored out of the streaming loop so it
/// can be unit-tested without spawning a real replication connection.
#[derive(Debug, Clone, Copy)]
pub struct LagSampler {
    threshold_bytes: u64,
    /// Sticky: once we've reported "caught up", we don't re-emit on every
    /// sample. The orchestrator may still emit a follow-up "fell behind"
    /// event when lag spikes above the threshold again.
    caught_up: bool,
}

impl LagSampler {
    /// Build a sampler with the given lag threshold (in WAL bytes).
    pub fn new(threshold_bytes: u64) -> Self {
        Self {
            threshold_bytes,
            caught_up: false,
        }
    }

    /// Whether the sampler has already reported the "caught up" state.
    pub fn is_caught_up(&self) -> bool {
        self.caught_up
    }

    /// Compute the unsigned lag between source and target WAL positions.
    /// Returns `0` if `applied_lsn >= source_lsn` (the target has applied
    /// everything we know about — possible if the source is idle).
    pub fn lag_bytes(source_lsn: u64, applied_lsn: u64) -> u64 {
        source_lsn.saturating_sub(applied_lsn)
    }

    /// Feed a fresh `(source_lsn, applied_lsn)` sample and return a
    /// transition.
    ///
    /// The caller uses [`Transition::JustCaughtUp`] to emit a `CaughtUp`
    /// progress event the *first* time the target catches up, and
    /// [`Transition::FellBehind`] when lag spikes above the threshold after
    /// having been caught up.
    pub fn observe(&mut self, source_lsn: u64, applied_lsn: u64) -> Transition {
        let lag = Self::lag_bytes(source_lsn, applied_lsn);
        let now_caught_up = lag <= self.threshold_bytes;
        let transition = match (self.caught_up, now_caught_up) {
            (false, true) => Transition::JustCaughtUp { lag },
            (true, false) => Transition::FellBehind { lag },
            (true, true) => Transition::StillCaughtUp { lag },
            (false, false) => Transition::StillBehind { lag },
        };
        self.caught_up = now_caught_up;
        transition
    }
}

/// Result of [`LagSampler::observe`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transition {
    /// Lag dropped at or below the threshold for the first time. Emit a
    /// `CaughtUp` progress event.
    JustCaughtUp { lag: u64 },
    /// Lag is still ≤ threshold. Don't re-emit anything.
    StillCaughtUp { lag: u64 },
    /// Lag spiked above the threshold after a previous catch-up. Operator
    /// may want to wait for the next `JustCaughtUp` before triggering cutover.
    FellBehind { lag: u64 },
    /// Lag is still > threshold. Initial state; no event.
    StillBehind { lag: u64 },
}

impl Transition {
    /// Returns the observed lag in bytes.
    pub fn lag(&self) -> u64 {
        match self {
            Self::JustCaughtUp { lag }
            | Self::StillCaughtUp { lag }
            | Self::FellBehind { lag }
            | Self::StillBehind { lag } => *lag,
        }
    }

    /// Whether this transition crossed into the "caught up" region.
    pub fn just_caught_up(&self) -> bool {
        matches!(self, Self::JustCaughtUp { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handle_starts_unrequested() {
        let h = CutoverHandle::new();
        assert!(!h.is_requested());
    }

    #[test]
    fn handle_request_is_idempotent() {
        let h = CutoverHandle::new();
        assert!(!h.request()); // first call returns previous state false
        assert!(h.is_requested());
        assert!(h.request()); // second call returns previous state true
    }

    #[test]
    fn handle_clones_share_state() {
        let h1 = CutoverHandle::new();
        let h2 = h1.clone();
        h1.request();
        assert!(h2.is_requested());
    }

    #[test]
    fn lag_bytes_clamps_to_zero_when_target_ahead() {
        assert_eq!(LagSampler::lag_bytes(100, 200), 0);
        assert_eq!(LagSampler::lag_bytes(200, 100), 100);
        assert_eq!(LagSampler::lag_bytes(100, 100), 0);
    }

    #[test]
    fn sampler_first_catch_up_emits_just_caught_up() {
        let mut s = LagSampler::new(8);
        // Initially behind.
        let t = s.observe(1000, 500);
        assert!(matches!(t, Transition::StillBehind { lag: 500 }));
        assert!(!s.is_caught_up());

        // Now within threshold.
        let t = s.observe(1000, 995);
        assert!(matches!(t, Transition::JustCaughtUp { lag: 5 }));
        assert!(s.is_caught_up());
    }

    #[test]
    fn sampler_does_not_re_emit_when_still_caught_up() {
        let mut s = LagSampler::new(8);
        s.observe(1000, 500);
        s.observe(1000, 1000); // JustCaughtUp
        let t = s.observe(1010, 1005);
        assert!(matches!(t, Transition::StillCaughtUp { lag: 5 }));
    }

    #[test]
    fn sampler_emits_fell_behind_on_lag_spike() {
        let mut s = LagSampler::new(8);
        s.observe(1000, 1000); // JustCaughtUp
        let t = s.observe(2000, 1000); // suddenly 1000 bytes behind
        assert!(matches!(t, Transition::FellBehind { lag: 1000 }));
        assert!(!s.is_caught_up());
    }

    #[test]
    fn transition_lag_accessor() {
        assert_eq!(Transition::JustCaughtUp { lag: 7 }.lag(), 7);
        assert_eq!(Transition::StillBehind { lag: 99 }.lag(), 99);
        assert!(Transition::JustCaughtUp { lag: 0 }.just_caught_up());
        assert!(!Transition::FellBehind { lag: 0 }.just_caught_up());
    }

    #[test]
    fn threshold_inclusive_boundary_counts_as_caught_up() {
        let mut s = LagSampler::new(8);
        let t = s.observe(108, 100);
        assert!(matches!(t, Transition::JustCaughtUp { lag: 8 }));
    }
}

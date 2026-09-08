//! Phase-level latency and queue metrics for durable replication (issue #127).
//!
//! issue #127's acceptance requires observable per-phase timings for the write
//! path and the background replication dispatcher: duplicate search, embedding,
//! local commit, outbox wait, delivery attempt, and remote acknowledgement.
//! MemPalace ships no pushed metrics sink (no Prometheus/OpenTelemetry exporter
//! in the workspace), so — per the review guidance on the narrowest observable
//! instrumentation consistent with the existing architecture — the MCP process
//! keeps an in-process aggregate of these phases and exposes it through
//! `mempalace_status` under `replication.metrics.phases`, while each sample is
//! also emitted as a structured `tracing` event (`target: "mempalace_metrics"`)
//! so it lands in whatever subscriber the operator already wires.
//!
//! # Phase names and semantics
//!
//! Each phase is recorded once per occurrence with its wall-clock duration and
//! aggregated as `count`, `last_ms`, `total_ms`, `max_ms`, and `avg_ms`:
//!
//! - `duplicate_search` — time spent in the pre-add semantic duplicate scan
//!   (`McpRuntime::find_duplicates`).
//! - `embedding` — time spent producing the embedding vector for a local drawer
//!   write (`McpRuntime::build_drawer_record`).
//! - `commit` — time spent persisting the local mutation (drawer add/delete,
//!   KG add/invalidate).
//! - `outbox_wait` — age of a replication operation when the worker claims it
//!   (queue latency from enqueue to delivery start).
//! - `delivery_attempt` — duration of one background delivery attempt (remote
//!   round trip incl. the capability handshake).
//! - `remote_acknowledge` — duration from delivery start to a successful outbox
//!   acknowledgement.
//!
//! Counts are monotonic; the aggregates are process-local and reset on restart.
//! This is deliberately *not* a wire format — it is the minimal observable
//! surface for operators to watch the async replication pipeline without a
//! third-party sink. See `docs/Federation.md` for the documented semantics.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Aggregate latency statistics for one named phase.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseStats {
    /// Number of recorded samples.
    pub count: u64,
    /// Duration of the most recent sample, in whole milliseconds.
    pub last_ms: u128,
    /// Sum of every sample, in whole milliseconds.
    pub total_ms: u128,
    /// Largest individual sample, in whole milliseconds.
    pub max_ms: u128,
}

impl PhaseStats {
    /// Arithmetic mean of all samples, in whole milliseconds.
    pub fn avg_ms(&self) -> u128 {
        if self.count == 0 { 0 } else { self.total_ms / u128::from(self.count) }
    }
}

/// Thread-safe per-process accumulator of phase timings.
///
/// Cheap to clone (shares one `Arc`); clones all write to the same aggregate, so
/// the tool path (`McpRuntime`) and the background worker can both contribute.
#[derive(Debug, Clone, Default)]
pub struct PhaseMeter {
    inner: std::sync::Arc<std::sync::Mutex<BTreeMap<&'static str, PhaseStats>>>,
}

impl PhaseMeter {
    /// Record one sample of `phase` with wall-clock duration `elapsed`.
    ///
    /// Updates the in-process aggregate and emits a structured `tracing` event:
    /// `target = "mempalace_metrics"`, fields `phase`, `elapsed_ms`, and `count`.
    pub fn record(&self, phase: &'static str, elapsed: Duration) {
        let elapsed_ms = elapsed.as_millis();
        {
            let mut phases = self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = phases.entry(phase).or_default();
            entry.count += 1;
            entry.last_ms = elapsed_ms;
            entry.total_ms += elapsed_ms;
            entry.max_ms = entry.max_ms.max(elapsed_ms);
            let count = entry.count;
            tracing::debug!(
                target: "mempalace_metrics",
                phase,
                elapsed_ms,
                count,
                "replication phase timing"
            );
        }
    }

    /// Snapshot the current aggregates keyed by phase name, in name order.
    pub fn snapshot(&self) -> BTreeMap<&'static str, PhaseStats> {
        self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregates_and_snapshots_phases() {
        let meter = PhaseMeter::default();
        meter.record("embedding", Duration::from_millis(10));
        meter.record("embedding", Duration::from_millis(30));
        meter.record("commit", Duration::from_millis(42));

        let snapshot = meter.snapshot();
        assert_eq!(snapshot.len(), 2);
        let embedding = snapshot["embedding"];
        assert_eq!(embedding.count, 2);
        assert_eq!(embedding.last_ms, 30);
        assert_eq!(embedding.total_ms, 40);
        assert_eq!(embedding.max_ms, 30);
        assert_eq!(embedding.avg_ms(), 20);
        let commit = snapshot["commit"];
        assert_eq!(commit.count, 1);
        assert_eq!(commit.total_ms, 42);
        assert_eq!(commit.avg_ms(), 42);
    }

    #[test]
    fn clones_share_aggregate() {
        let meter = PhaseMeter::default();
        let worker_meter = meter.clone();
        worker_meter.record("delivery_attempt", Duration::from_millis(7));
        let snapshot = meter.snapshot();
        assert_eq!(snapshot["delivery_attempt"].count, 1);
        assert_eq!(snapshot["delivery_attempt"].last_ms, 7);
    }
}

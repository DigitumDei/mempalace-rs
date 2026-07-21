use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};

/// Identifies a single maintenance tier operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceTier {
    /// Remove version rows older than the retention window.
    VersionRetention,
    /// Compact the tail of the drawer table when rows exceed the threshold.
    TailCompaction,
}

/// Why a maintenance tier was skipped entirely.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceSkipReason {
    /// Maintenance is globally disabled.
    Disabled,
    /// The system has not been idle long enough.
    NotIdle,
    /// No work was required (e.g. nothing to delete, no tails to compact).
    NothingToDo,
}

/// Why a maintenance tier was started but aborted before completion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceAbortReason {
    /// Another concurrent process holds the maintenance lock.
    ConcurrentRun,
    /// The system is shutting down.
    Shutdown,
    /// The operation exceeded its time budget.
    Timeout,
}

/// Outcome of a single maintenance tier operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceOutcome {
    /// Completed successfully with a count of affected items.
    Completed {
        items_affected: u64,
    },
    /// Skipped without starting.
    Skipped {
        reason: MaintenanceSkipReason,
    },
    /// Started but aborted before completion.
    Aborted {
        reason: MaintenanceAbortReason,
        items_affected: u64,
    },
    /// Failed with an error.
    Failed {
        message: String,
    },
}

/// Result of a single maintenance tier, including timing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceTierResult {
    /// Which tier this result is for.
    pub tier: MaintenanceTier,
    /// When the tier started.
    pub started_at: OffsetDateTime,
    /// Wall-clock duration of the tier operation.
    pub duration: Duration,
    /// Outcome of the tier.
    pub outcome: MaintenanceOutcome,
}

/// Overall status of a completed maintenance run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceRunStatus {
    /// All tiers completed successfully.
    Success,
    /// At least one tier was skipped (non-critical).
    Partial,
    /// At least one tier failed or was aborted.
    Failure,
}

/// Summary of a completed maintenance run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceRunSummary {
    /// Monotonically increasing run identifier.
    pub run_id: u64,
    /// When the run started.
    pub started_at: OffsetDateTime,
    /// When the run finished.
    pub finished_at: OffsetDateTime,
    /// Wall-clock duration of the entire run.
    pub duration: Duration,
    /// Overall status.
    pub status: MaintenanceRunStatus,
    /// Per-tier results.
    pub tier_results: Vec<MaintenanceTierResult>,
}

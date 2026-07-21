use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};

/// Identifies a single maintenance tier operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceTier {
    /// Remove version rows older than the retention window.
    VersionRetention,
    /// Compact LanceDB fragments to reduce storage and improve scan performance.
    FragmentCompaction,
    /// Optimize LanceDB vector indices for faster ANN search.
    VectorIndexOptimization,
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
    /// Process CPU time consumed by the tier operation.
    pub cpu_duration: Duration,
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
    /// Process CPU time consumed by the entire run.
    pub cpu_duration: Duration,
    /// Overall status.
    pub status: MaintenanceRunStatus,
    /// Per-tier results.
    pub tier_results: Vec<MaintenanceTierResult>,
}

/// Shared settings for the maintenance subsystem.
///
/// This type is the canonical settings representation used by the
/// storage engine, HTTP server, and CLI.  It mirrors the fields
/// resolved by the config crate's `MaintenanceRuntimeConfig`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceSettings {
    /// Whether the maintenance subsystem is enabled (default: true).
    pub enabled: bool,
    /// Minimum idle seconds since last write before maintenance runs (default: 300).
    pub idle_secs: u64,
    /// Maximum age in hours for retained version data (default: 24).
    pub version_retention_hours: u64,
    /// Row count threshold for triggering tail compaction (default: 1024).
    pub tail_threshold_rows: u64,
    /// Small-fragment count threshold for triggering fragment compaction (default: 10).
    pub small_fragment_threshold: u64,
}

impl Default for MaintenanceSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            idle_secs: 300,
            version_retention_hours: 24,
            tail_threshold_rows: 1024,
            small_fragment_threshold: 10,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maintenance_tier_serde_snake_case() {
        let cases = [
            (MaintenanceTier::VersionRetention, "version_retention"),
            (MaintenanceTier::FragmentCompaction, "fragment_compaction"),
            (MaintenanceTier::VectorIndexOptimization, "vector_index_optimization"),
        ];
        for (variant, expected) in &cases {
            let json = serde_json::to_string(variant).unwrap();
            assert_eq!(json, format!("\"{expected}\""));
            let deserialized: MaintenanceTier = serde_json::from_str(&json).unwrap();
            assert_eq!(&deserialized, variant);
        }
    }

    #[test]
    fn maintenance_skip_reason_serde_snake_case() {
        let cases = [
            (MaintenanceSkipReason::Disabled, "disabled"),
            (MaintenanceSkipReason::NotIdle, "not_idle"),
            (MaintenanceSkipReason::NothingToDo, "nothing_to_do"),
        ];
        for (variant, expected) in &cases {
            let json = serde_json::to_string(variant).unwrap();
            assert_eq!(json, format!("\"{expected}\""));
            let deserialized: MaintenanceSkipReason = serde_json::from_str(&json).unwrap();
            assert_eq!(&deserialized, variant);
        }
    }

    #[test]
    fn maintenance_abort_reason_serde_snake_case() {
        let cases = [
            (MaintenanceAbortReason::ConcurrentRun, "concurrent_run"),
            (MaintenanceAbortReason::Shutdown, "shutdown"),
            (MaintenanceAbortReason::Timeout, "timeout"),
        ];
        for (variant, expected) in &cases {
            let json = serde_json::to_string(variant).unwrap();
            assert_eq!(json, format!("\"{expected}\""));
            let deserialized: MaintenanceAbortReason = serde_json::from_str(&json).unwrap();
            assert_eq!(&deserialized, variant);
        }
    }

    #[test]
    fn maintenance_run_status_serde_snake_case() {
        let cases = [
            (MaintenanceRunStatus::Success, "success"),
            (MaintenanceRunStatus::Partial, "partial"),
            (MaintenanceRunStatus::Failure, "failure"),
        ];
        for (variant, expected) in &cases {
            let json = serde_json::to_string(variant).unwrap();
            assert_eq!(json, format!("\"{expected}\""));
            let deserialized: MaintenanceRunStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(&deserialized, variant);
        }
    }

    #[test]
    fn maintenance_outcome_serde_round_trip() {
        let outcomes = vec![
            MaintenanceOutcome::Completed { items_affected: 42 },
            MaintenanceOutcome::Skipped {
                reason: MaintenanceSkipReason::NotIdle,
            },
            MaintenanceOutcome::Aborted {
                reason: MaintenanceAbortReason::Timeout,
                items_affected: 7,
            },
            MaintenanceOutcome::Failed {
                message: "disk full".into(),
            },
        ];
        for outcome in outcomes {
            let json = serde_json::to_string(&outcome).unwrap();
            let deserialized: MaintenanceOutcome = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, outcome);
        }
    }

    #[test]
    fn maintenance_tier_result_round_trip_with_cpu() {
        let result = MaintenanceTierResult {
            tier: MaintenanceTier::VersionRetention,
            started_at: OffsetDateTime::UNIX_EPOCH,
            duration: Duration::seconds(10),
            cpu_duration: Duration::milliseconds(450),
            outcome: MaintenanceOutcome::Completed { items_affected: 100 },
        };
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: MaintenanceTierResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, result);
        assert!(json.contains("cpu_duration"));
    }

    #[test]
    fn maintenance_run_summary_round_trip_with_cpu() {
        let summary = MaintenanceRunSummary {
            run_id: 1,
            started_at: OffsetDateTime::UNIX_EPOCH,
            finished_at: OffsetDateTime::UNIX_EPOCH + Duration::seconds(30),
            duration: Duration::seconds(30),
            cpu_duration: Duration::milliseconds(1200),
            status: MaintenanceRunStatus::Partial,
            tier_results: vec![
                MaintenanceTierResult {
                    tier: MaintenanceTier::VersionRetention,
                    started_at: OffsetDateTime::UNIX_EPOCH,
                    duration: Duration::seconds(10),
                    cpu_duration: Duration::milliseconds(450),
                    outcome: MaintenanceOutcome::Completed { items_affected: 100 },
                },
                MaintenanceTierResult {
                    tier: MaintenanceTier::FragmentCompaction,
                    started_at: OffsetDateTime::UNIX_EPOCH,
                    duration: Duration::seconds(20),
                    cpu_duration: Duration::milliseconds(750),
                    outcome: MaintenanceOutcome::Skipped {
                        reason: MaintenanceSkipReason::NothingToDo,
                    },
                },
            ],
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: MaintenanceRunSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, summary);
        assert!(json.contains("cpu_duration"));
    }

    #[test]
    fn maintenance_run_summary_aborted_outcome() {
        let summary = MaintenanceRunSummary {
            run_id: 2,
            started_at: OffsetDateTime::UNIX_EPOCH,
            finished_at: OffsetDateTime::UNIX_EPOCH + Duration::seconds(5),
            duration: Duration::seconds(5),
            cpu_duration: Duration::ZERO,
            status: MaintenanceRunStatus::Failure,
            tier_results: vec![MaintenanceTierResult {
                tier: MaintenanceTier::FragmentCompaction,
                started_at: OffsetDateTime::UNIX_EPOCH,
                duration: Duration::seconds(5),
                cpu_duration: Duration::milliseconds(200),
                outcome: MaintenanceOutcome::Aborted {
                    reason: MaintenanceAbortReason::ConcurrentRun,
                    items_affected: 3,
                },
            }],
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: MaintenanceRunSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, summary);
        assert!(json.contains("concurrent_run"));
        assert!(json.contains("aborted"));
    }

    #[test]
    fn maintenance_settings_default() {
        let settings = MaintenanceSettings::default();
        assert!(settings.enabled);
        assert_eq!(settings.idle_secs, 300);
        assert_eq!(settings.version_retention_hours, 24);
        assert_eq!(settings.tail_threshold_rows, 1024);
        assert_eq!(settings.small_fragment_threshold, 10);
    }

    #[test]
    fn maintenance_settings_serde_round_trip() {
        let settings = MaintenanceSettings {
            enabled: false,
            idle_secs: 600,
            version_retention_hours: 48,
            tail_threshold_rows: 2048,
            small_fragment_threshold: 20,
        };
        let json = serde_json::to_string(&settings).unwrap();
        let deserialized: MaintenanceSettings = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, settings);
        assert_eq!(
            deserialized,
            MaintenanceSettings {
                enabled: false,
                idle_secs: 600,
                version_retention_hours: 48,
                tail_threshold_rows: 2048,
                small_fragment_threshold: 20,
            }
        );
    }
}
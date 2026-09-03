//! Storage layer for MemPalace Rust crates.

mod coordination;
mod delegation;
mod engine;
mod error;
mod lance;
mod maintenance;
mod skills;
mod sqlite;
mod types;

pub use coordination::{
    Artifact, CoordinationCursor, CoordinationEvent, CoordinationEventPage, CoordinationStore,
    CoordinationVisibility, INVALID_TRANSITION_PREFIX, ImportedTask, InboxPage,
    LEASE_DURATION_OUT_OF_RANGE,
    LEASE_HAS_EXPIRED, LEASE_HELD_BY_ANOTHER_WORKER, MAX_PAYLOAD_BYTES, Message, NOT_FOUND_SUFFIX,
    NewArtifact, NewMessage, NewTask, NewTaskResult, ONLY_LEASE_OWNER_MAY_RENEW,
    ONLY_OWNER_MAY_TRANSITION, ONLY_RECIPIENT_MAY_ACKNOWLEDGE, TASK_HAS_EXPIRED,
    TERMINAL_TASK_CANNOT_BE_CLAIMED, Task, TaskResult, TaskState, UNSCOPED_WING,
};
pub use delegation::{
    Checkpoint, CheckpointType, DelegationStore, NewCheckpoint, NewSpan, Span, SpanStatus,
    StopReason, Trace, TraceNode,
};
pub use engine::StorageEngine;
pub use error::{Result, StorageError};
pub use lance::{FragmentStats, LanceDrawerStore, OptimizeMetrics, PruneMetrics, VectorIndexStats};
pub use maintenance::{
    MaintenanceAbortReason, MaintenanceOutcome, MaintenanceRunStatus, MaintenanceRunSummary,
    MaintenanceSettings, MaintenanceSkipReason, MaintenanceTier, MaintenanceTierResult,
};
pub use skills::{
    NewSkill, NewSkillOutcome, Skill, SkillOutcome, SkillOutcomeResult, SkillReview, SkillScope,
    SkillStatus, SkillStore,
};
pub use sqlite::{
    ChangeCursor, ChangeEvent, ChangeLogStore, ChangePage, DiaryStore, EntityRegistryStore,
    GraphStore, IngestManifestStore, KnowledgeGraphStore, MaintenanceLeaseStore, SelfModelStore,
    SqliteOperationalStore, ToolStateStore,
};
pub use types::{
    AgentLineageRecord, ConfigEntry, DrawerFilter, DrawerMatch, DrawerStore, DuplicateStrategy,
    EntityRecord, GraphDocument, IngestCommitRequest, IngestFileRecord, IngestManifestEntry,
    IngestRun, IngestRunStatus, KnowledgeGraphFact, LineageMigrationRecord, RetryableRun,
    RevisionedWrite, SearchRequest, SelfObservationRecord, SelfObservationScope,
    SelfObservationStatus, StorageLayout, ToolStateEntry,
};

pub use mempalace_core as core;

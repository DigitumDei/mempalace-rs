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
    InboxPage, Message, NewArtifact, NewMessage, NewTask, NewTaskResult, Task, TaskResult,
    TaskState, UNSCOPED_WING,
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

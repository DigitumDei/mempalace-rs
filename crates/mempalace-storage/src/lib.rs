//! Storage layer for MemPalace Rust crates.

mod engine;
mod error;
mod lance;
mod maintenance;
mod sqlite;
mod types;

pub use engine::StorageEngine;
pub use error::{Result, StorageError};
pub use lance::LanceDrawerStore;
pub use maintenance::{
    MaintenanceAbortReason, MaintenanceOutcome, MaintenanceRunStatus, MaintenanceRunSummary,
    MaintenanceSettings, MaintenanceSkipReason, MaintenanceTier, MaintenanceTierResult,
};
pub use sqlite::{
    ChangeCursor, ChangeEvent, ChangeLogStore, ChangePage, DiaryStore, EntityRegistryStore, GraphStore,
    IngestManifestStore, KnowledgeGraphStore, MaintenanceLeaseStore, SqliteOperationalStore, ToolStateStore,
};
pub use types::{
    ConfigEntry, DrawerFilter, DrawerMatch, DrawerStore, DuplicateStrategy, EntityRecord,
    GraphDocument, IngestCommitRequest, IngestFileRecord, IngestManifestEntry, IngestRun,
    IngestRunStatus, KnowledgeGraphFact, RetryableRun, SearchRequest, StorageLayout,
    ToolStateEntry,
};

pub use mempalace_core as core;

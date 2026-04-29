//! Storage layer for MemPalace Rust crates.

mod engine;
mod error;
mod lance;
mod sqlite;
mod types;

pub use engine::StorageEngine;
pub use error::{Result, StorageError};
pub use lance::LanceDrawerStore;
pub use sqlite::{
    EntityRegistryStore, GraphStore, IngestManifestStore, KnowledgeGraphStore,
    SqliteOperationalStore, ToolStateStore,
};
pub use types::{
    ConfigEntry, DrawerFilter, DrawerMatch, DrawerStore, DuplicateStrategy, EntityRecord,
    GraphDocument, IngestCommitRequest, IngestFileRecord, IngestManifestEntry, IngestRun,
    IngestRunStatus, KnowledgeGraphFact, RetryableRun, SearchRequest, StorageLayout,
    ToolStateEntry,
};

pub use mempalace_core as core;

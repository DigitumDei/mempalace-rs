use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use time::{Date, OffsetDateTime};

use crate::error::Result;
use mempalace_core::{DrawerId, DrawerRecord, RoomId, WingId};

time::serde::format_description!(date_only, Date, "[year]-[month]-[day]");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageLayout {
    pub root: PathBuf,
    pub sqlite_path: PathBuf,
    pub lancedb_dir: PathBuf,
}

impl StorageLayout {
    pub fn new(root: impl AsRef<Path>) -> Self {
        let root = root.as_ref().to_path_buf();
        Self { sqlite_path: root.join("storage.sqlite3"), lancedb_dir: root.join("lancedb"), root }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DrawerFilter {
    pub ids: Vec<DrawerId>,
    pub wing: Option<WingId>,
    pub room: Option<RoomId>,
    pub hall: Option<String>,
    pub source_file: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchRequest {
    pub embedding: Vec<f32>,
    pub limit: usize,
    pub include_cutoff_ties: bool,
    pub filter: DrawerFilter,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DrawerMatch {
    pub record: DrawerRecord,
    pub distance: Option<f32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DuplicateStrategy {
    Error,
    Ignore,
    Overwrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngestRunStatus {
    Pending,
    Committed,
    Failed,
}

impl IngestRunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Committed => "committed",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestRun {
    pub id: i64,
    pub ingest_kind: String,
    pub source_key: String,
    pub status: IngestRunStatus,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub failed_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestManifestEntry {
    pub run_id: i64,
    pub drawer_id: DrawerId,
    pub source_file: String,
    pub content_hash: String,
    pub status: IngestRunStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestFileRecord {
    pub source_key: String,
    pub source_file: String,
    pub content_hash: String,
    pub last_ingested_at: OffsetDateTime,
    pub ingest_kind: String,
    pub drawer_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryableRun {
    pub run: IngestRun,
    pub chunk_ids: Vec<DrawerId>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IngestCommitRequest {
    pub ingest_kind: String,
    pub source_key: String,
    pub source_file: String,
    pub content_hash: String,
    pub drawers: Vec<DrawerRecord>,
    pub duplicate_strategy: DuplicateStrategy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityRecord {
    pub entity_id: String,
    pub entity_type: String,
    pub payload: serde_json::Value,
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphDocument {
    pub graph_key: String,
    pub payload: serde_json::Value,
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KnowledgeGraphFact {
    pub fact_id: String,
    pub subject_entity_id: String,
    pub predicate: String,
    pub object_entity_id: String,
    #[serde(with = "date_only::option", default)]
    pub valid_from: Option<Date>,
    #[serde(with = "date_only::option", default)]
    pub valid_to: Option<Date>,
    pub confidence: f32,
    pub source_drawer_id: Option<DrawerId>,
    pub source_file: Option<String>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigEntry {
    pub config_key: String,
    pub config_value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolStateEntry {
    pub tool_name: String,
    pub payload: String,
    pub updated_at: OffsetDateTime,
}

#[async_trait]
pub trait DrawerStore: Send + Sync {
    async fn ensure_schema(&self) -> Result<()>;
    async fn put_drawers(
        &self,
        drawers: &[DrawerRecord],
        strategy: DuplicateStrategy,
    ) -> Result<()>;
    async fn get_drawer(&self, id: &DrawerId) -> Result<Option<DrawerRecord>>;
    async fn delete_drawers(&self, ids: &[DrawerId]) -> Result<usize>;
    async fn search_drawers(&self, request: &SearchRequest) -> Result<Vec<DrawerMatch>>;
    async fn list_drawers(&self, filter: &DrawerFilter) -> Result<Vec<DrawerRecord>>;
}

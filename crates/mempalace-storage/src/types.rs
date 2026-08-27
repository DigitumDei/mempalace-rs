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
    /// Restrict results to any of these wings (an "IN" match), independent of
    /// `wing`'s single-value equality match. Empty means unconstrained by
    /// this field. Used to push a caller's visible-wing set into the storage
    /// query itself — e.g. a scoped federation token listing drawers with no
    /// `wing` filter — so authorization is enforced by the query rather than
    /// by filtering rows the query already returned. That distinction
    /// matters because storage-side filtering composes correctly with
    /// `limit`; filtering visibility out of an already-limited result page
    /// can silently strand authorized rows below the page with no cursor to
    /// reach them (see `route_drawers_list` in `mempalace-server`). An empty
    /// set from a caller whose visible-wing set is genuinely empty must be
    /// handled by the caller *not* querying at all — passing an empty `wings`
    /// here is indistinguishable from "unconstrained" and would return
    /// everything.
    pub wings: Vec<WingId>,
    pub room: Option<RoomId>,
    pub hall: Option<String>,
    pub source_file: Option<String>,
    /// Optional source-file set to match. Used for bounded branch-overlay
    /// discovery while composing semantic-search candidates.
    pub source_files: Vec<String>,
    /// Optional view/ref name to scope matched drawers. `None` and
    /// `"canonical"` exclude branch rows; a branch name includes that branch
    /// alongside the canonical and non-project rows for view composition.
    pub view: Option<String>,
    /// Include every repository view. This is for storage maintenance and the
    /// explicit `full` search view; ordinary reads remain canonical by default.
    pub include_all_views: bool,
    /// Match only rows belonging to `view`, excluding canonical and unrelated
    /// branch rows. Used to discover branch overlay keys.
    pub branch_view_only: bool,
    /// Maximum number of drawers to return.  `None` means unlimited.
    pub limit: Option<usize>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Lifecycle state for an evidence-backed observation about an agent lineage.
pub enum SelfObservationStatus {
    /// Proposed but not yet accepted into the compiled identity packet.
    Candidate,
    /// Reviewed and accepted as current self-model context.
    Promoted,
    /// Replaced by a newer promoted observation.
    Superseded,
    /// Reviewed and deliberately rejected or withdrawn.
    Retired,
}

impl SelfObservationStatus {
    /// Return the stable SQLite and wire representation.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::Promoted => "promoted",
            Self::Superseded => "superseded",
            Self::Retired => "retired",
        }
    }

    /// Parse the stable SQLite and wire representation.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "candidate" => Some(Self::Candidate),
            "promoted" => Some(Self::Promoted),
            "superseded" => Some(Self::Superseded),
            "retired" => Some(Self::Retired),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Applicability boundary for a self-observation.
pub enum SelfObservationScope {
    /// Applies to its owning lineage across model and harness changes.
    Lineage,
    /// Applies to every lineage in the local palace, with ownership retained for provenance.
    Shared,
    /// Applies only to matching model and harness runtime metadata.
    Engine,
}

impl SelfObservationScope {
    /// Return the stable SQLite and wire representation.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Lineage => "lineage",
            Self::Shared => "shared",
            Self::Engine => "engine",
        }
    }

    /// Parse the stable SQLite and wire representation.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "lineage" => Some(Self::Lineage),
            "shared" => Some(Self::Shared),
            "engine" => Some(Self::Engine),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Provider-neutral identity for a continuing agent collaborator.
pub struct AgentLineageRecord {
    /// Stable lineage identifier independent of model and harness names.
    pub lineage_id: String,
    /// Human-readable lineage name.
    pub display_name: String,
    /// Description of what remains continuous across runtimes.
    pub description: String,
    /// Optimistic-concurrency revision, starting at one.
    pub revision: i64,
    /// Whether wake-up selects this lineage when none is requested.
    pub is_default: bool,
    /// Creation time.
    pub created_at: OffsetDateTime,
    /// Last successful revision time.
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// Evidence-backed, reviewable observation about a persistent agent self.
pub struct SelfObservationRecord {
    /// Stable observation identifier.
    pub observation_id: String,
    /// Owning lineage used for identity and provenance.
    pub lineage_id: String,
    /// Current review lifecycle state.
    pub status: SelfObservationStatus,
    /// Runtime applicability boundary.
    pub scope: SelfObservationScope,
    /// Concise, falsifiable description of the observed pattern.
    pub statement: String,
    /// How behavior should change if this observation is promoted.
    pub behavioral_consequence: String,
    /// Confidence from zero to one.
    pub confidence: f32,
    /// Author who proposed the observation.
    pub author: String,
    /// Model associated with the evidence or engine constraint.
    pub model: Option<String>,
    /// Harness associated with the evidence or engine constraint.
    pub harness: Option<String>,
    /// Concrete memory or task references supporting the observation.
    pub evidence: Vec<String>,
    /// Known contradictory or limiting evidence.
    pub counterevidence: Vec<String>,
    /// Older promoted observation replaced when this one is promoted.
    pub supersedes_observation_id: Option<String>,
    /// Optimistic-concurrency revision, starting at one.
    pub revision: i64,
    /// Proposal time.
    pub created_at: OffsetDateTime,
    /// Last review or lifecycle transition time.
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Evidence-backed account of continuity and change across a runtime migration.
pub struct LineageMigrationRecord {
    /// Stable migration identifier.
    pub migration_id: String,
    /// Persistent lineage that changed runtime.
    pub lineage_id: String,
    /// Previous model, when known.
    pub from_model: Option<String>,
    /// Previous harness, when known.
    pub from_harness: Option<String>,
    /// New model.
    pub to_model: String,
    /// New harness.
    pub to_harness: String,
    /// Concise migration account.
    pub summary: String,
    /// Behaviors, commitments, and understandings that carried over.
    pub continuities: Vec<String>,
    /// Observed changes attributed to the new runtime.
    pub changes: Vec<String>,
    /// Concrete comparison or memory references supporting the account.
    pub evidence: Vec<String>,
    /// Author who recorded the migration.
    pub author: String,
    /// Recording time.
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq)]
/// Outcome of a write guarded by an expected optimistic-concurrency revision.
pub enum RevisionedWrite<T> {
    /// The write was applied and returns the current record.
    Applied(T),
    /// The write made no change because the expected revision was stale or absent.
    Conflict {
        /// Current revision, or `None` when the record does not exist.
        actual_revision: Option<i64>,
    },
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

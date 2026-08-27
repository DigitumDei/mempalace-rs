#![allow(missing_docs)]

mod federation;

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use blake3::Hasher;
use mempalace_config::{ConfigLoader, MempalaceConfig, ReplicationStatus, RouteMode, WriteTarget};
use mempalace_core::{
    DIARY_HALL, DIARY_ROOM, DIARY_SUMMARY_MAX_CHARS, DIARY_TOPIC_PREFIX, DrawerId, DrawerRecord,
    EmbeddingProfile, RoomId, SHARED_AGENT_DIARY_WING, SearchQuery, WingId,
};
use mempalace_embeddings::{
    EmbeddingError, EmbeddingProvider, EmbeddingRequest, FastembedProvider,
    FastembedProviderConfig, env_flag,
};
use mempalace_federation::{
    AckMessageRequest, CoordinationTaskState as WireTaskState,
    NewArtifactRequest as WireNewArtifactRequest, NewMessageRequest as WireNewMessageRequest,
    NewTaskRequest as WireNewTaskRequest, NewTaskResultRequest as WireNewTaskResultRequest,
    TaskLeaseRequest, TransitionTaskRequest,
};
use mempalace_graph::{
    AddFactRequest, EntityKind, KnowledgeGraphRuntime, PalaceGraphSnapshot, QueryDirection,
    derive_palace_graph_from_store, find_tunnels, traverse_graph,
};
use mempalace_search::{SearchRuntime, SearchRuntimePolicy};
use mempalace_storage::{
    AgentLineageRecord, ChangeEvent, ChangeLogStore, CoordinationCursor, CoordinationStore,
    CoordinationVisibility, DiaryStore, DrawerFilter, DrawerStore, DuplicateStrategy,
    IngestCommitRequest,
    DelegationStore, LineageMigrationRecord, NewArtifact, NewCheckpoint, NewMessage, NewSkill,
    NewSkillOutcome, NewSpan, NewTask, NewTaskResult, RevisionedWrite, SelfModelStore,
    SelfObservationRecord, SelfObservationScope, SelfObservationStatus, SkillScope, SkillStatus,
    SkillStore, SpanStatus, StopReason, StorageEngine, TaskState,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use time::{Date, Duration, OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Mutex, Semaphore, TryAcquireError};

use federation::FederationRouter;
pub use mempalace_core as core;

// ─── Federation routing semantics ─────────────────────────────────────────────
//
// When federation is configured (via `federation` section in the palace config),
// tools route as follows:
//
// **Routable — uses `resolve_route()` per wing/room:**
//   Search, ListWings, ListRooms, GetTaxonomy, Status, CheckDuplicate,
//   AddDrawer
//
// **DeleteDrawer — ID-based local deletion with remote fallback:**
//   DeleteDrawer is NOT a dual-write or write-routed operation. It deletes by
//   drawer ID in the local palace first. If not found locally, it falls back
//   by attempting deletion across ALL configured remotes (in deterministic name
//   order), regardless of wing routing rules. Dual-written drawers have
//   independent IDs on each side with no durable cross-palace ID mapping, so
//   `write:remote` and `write:both` routing is irrelevant — the fallback is a
//   best-effort attempt to delete the same ID on every remote. The response
//   reports `applied_to: "local"` or `"remote:<name>"` and never carries a
//   `replication` field.
//
// **Routable — uses `resolve_kg_route()` (knowledge-graph-specific routing):**
//   KgQuery, KgAdd, KgInvalidate, KgTimeline, KgStats
//
// **Routable — coordination (issue #102 Stage 4):**
//   TaskCreate, TaskGet, TaskClaim, TaskRenew, TaskTransition, MessageSend,
//   MessageGet, MessageAcknowledge, InboxRead, ArtifactPut, ArtifactGet,
//   ResultPut, ResultGet, CoordinationEvents. `TaskCreate` is the one exception
//   routed by wing (`resolve_coordination_route` + `resolve_write_target`,
//   mirroring `KgAdd`/`KgInvalidate` — `write` can only ever resolve to `Local`
//   or `Remote`, never `Both`: a coordination route can never carry
//   `WriteTarget::Both`, rejected at config load). Every other coordination tool
//   above is keyed by an existing record ID with no wing in the request at all,
//   so it is local-first with an ID-discovery fallback across all configured
//   remotes in name order — the same reasoning `DeleteDrawer` already uses, and
//   documented in full in the `FederationRouter` "Coordination" section comment
//   in `federation.rs`. `InboxRead`/`CoordinationEvents` are aggregate,
//   cursor-paginated feeds like `GetChangesSince`: always local plus a fan-out
//   to every remote (never routed by a single record's owner), reported under
//   `remote_messages`/`remote_events`. `CoordinationEventGet` — a single event
//   by exact ID — is **not** in this category: Stage 3 never exposed a
//   `GET /v1/coordination/events/{id}` route, only the paginated feed, so it
//   stays `LocalOnly` below.
//
// **Always local — never federated:**
//   DiaryWrite, DiaryRead, WakeUp, GetChangesSince, Traverse, FindTunnels,
//   GraphStats, IdentityRead, IdentityUpdate, LineageSet,
//   SelfObservationPropose, SelfObservationReview, IdentityPacket,
//   MigrationRecord, GetAaaKSpec, CoordinationEventGet. Skill and delegation
//   tools (SkillPropose..SkillReviews, DelegationSpanStart..DelegationTrace)
//   are local-only too — they are not federated in this phase.
//
// **`kg_add`/`kg_invalidate` policy:** Both follow `resolve_kg_route()` and write
//   to the write-target side ONLY (local or the configured remote). The response
//   reports the touched side via `"applied_to": "local"` | `"remote:<name>"`.
//   In Local mode the write is local; in Remote mode the write goes to the remote
//   KG only; Combined uses the resolved `write` field (local, remote, or both).
//
// **`write: both` — local-first dual-write:**
//   When the resolved `write` field is `WriteTarget::Both`, the local write must
//   complete first, then a best-effort remote replication is attempted via the
//   corresponding `*_replicate` method (`add_drawer_replicate`, `kg_add_replicate`,
//   `kg_invalidate_replicate`). The response carries a `replication` field with
//   `ReplicationStatus` — `replicated`, `converged`, or `failed`. The `replication` field is
//   absent for non-`both` routes and diary-local writes. The remote failure
//   never blocks or rolls back the local write.
//
// **Wing name collisions:** During `list_wings` merging, a wing that exists both
//   locally and on a remote while its resolved route is Local-only triggers a
//   `tracing::warn!` (results stay split); collisions never block execution.
//
// **Wing names are the federation join key** — the same wing name on both sides
//   is merged-by-name in combined reads.
//
// **Write routing:** In Combined mode, `AddDrawer`, `KgAdd`, and
//   `KgInvalidate` write to the target indicated by the resolved rule's `write`
//   field (local, remote, or both). `DeleteDrawer` is excluded — see its section
//   above.
//
// **Per-project routing** (`resolve_route`'s `project_routing` parameter) is not
//   wired at the MCP layer — the stdio server has no per-project context, so it
//   is always `None`.
//
// For details on route resolution precedence, see
// `mempalace_config::federation::resolve_route()`.
// ──────────────────────────────────────────────────────────────────────────────

const SERVER_NAME: &str = "mempalace";
const SERVER_VERSION: &str = "2.0.0";
const PROTOCOL_VERSION: &str = "2024-11-05";
const DEFAULT_DUPLICATE_THRESHOLD: f32 = 0.9;
const DUPLICATE_SEARCH_LIMIT: usize = 5;
// Project-specific wake-up history scans farther back because global changes
// are interleaved across wings, but stops collecting as soon as the limit is met.
const WAKE_UP_PROJECT_SEARCH_MULTIPLIER: usize = 20;
const WAKE_UP_PROJECT_MIN_SEARCH_LIMIT: usize = 50;
const IDENTITY_UPDATE_MAX_CONTENT_BYTES: usize = 16 * 1024;
const IDENTITY_MAX_BYTES: usize = 64 * 1024;
pub const LINEAGE_ID_ENV: &str = "MEMPALACE_LINEAGE_ID";

pub const PALACE_PROTOCOL: &str = "IMPORTANT — MemPalace Memory Protocol:\n1. ON WAKE-UP: Call mempalace_wake_up with agent_name and, when known, model and harness. It loads the identity constitution, the MCP-bound or palace-default lineage's compiled identity packet, palace status, recent changes, current project context, and recent diary summaries across agents. Lineage selection is host configuration, never a model-supplied tool argument. If a configured binding does not exist, the packet uses the palace default and includes instructions for creating the requested lineage with mempalace_lineage_set. Use mempalace_diary_read with an entry_id when full diary detail is needed.\n2. BEFORE RESPONDING about any person, project, or past event: call mempalace_kg_query or mempalace_search FIRST. Never guess — verify.\n3. IF UNSURE about a fact (name, gender, age, relationship): say \"let me check\" and query the palace. Wrong is worse than slow.\n4. AFTER EACH SESSION: call mempalace_diary_write to record what happened, what you learned, what matters, with a concise summary.\n5. WHEN FACTS CHANGE: call mempalace_kg_invalidate on the old fact, mempalace_kg_add for the new one.\n6. TREAT identity.txt AS THE CONSTITUTION: use mempalace_identity_update for deliberate changes to durable identity, values, boundaries, and working relationship — not routine autobiography.\n7. WHEN A REPEATED PATTERN MAY DESCRIBE THE PERSISTENT SELF: propose an evidence-backed candidate with mempalace_self_observation_propose. Promote or retire it only after review with mempalace_self_observation_review.\n8. WHEN MODEL OR HARNESS CHANGES: record what carried over and what changed with mempalace_migration_record. Never silently treat engine behavior as lineage identity.\n\nThis protocol ensures the AI KNOWS before it speaks. Storage is not memory — but storage + this protocol = memory.";

pub const AAAK_SPEC: &str = "AAAK is a compressed memory dialect that MemPalace uses for efficient storage.\nIt is designed to be readable by both humans and LLMs without decoding.\n\nFORMAT:\n  ENTITIES: 3-letter uppercase codes. ALC=Alice, JOR=Jordan, RIL=Riley, MAX=Max, BEN=Ben.\n  EMOTIONS: *action markers* before/during text. *warm*=joy, *fierce*=determined, *raw*=vulnerable, *bloom*=tenderness.\n  STRUCTURE: Pipe-separated fields. FAM: family | PROJ: projects | ⚠: warnings/reminders.\n  DATES: ISO format (2026-03-31). COUNTS: Nx = N mentions (e.g., 570x).\n  IMPORTANCE: ★ to ★★★★★ (1-5 scale).\n  HALLS: hall_facts, hall_events, hall_discoveries, hall_preferences, hall_advice.\n  WINGS: wing_user, wing_agent, wing_team, wing_code, wing_myproject, wing_hardware, wing_ue5, wing_ai_research.\n  ROOMS: Hyphenated slugs representing named ideas (e.g., chromadb-setup, gpu-pricing).\n\nEXAMPLE:\n  FAM: ALC→♡JOR | 2D(kids): RIL(18,sports) MAX(11,chess+swimming) | BEN(contributor)\n\nRead AAAK naturally — expand codes mentally, treat *markers* as emotional context.\nWhen WRITING AAAK: use entity codes, mark emotions, keep structure tight.";

pub use mempalace_embeddings::DeterministicStubProvider;

#[derive(Debug, Error)]
pub enum McpError {
    #[error(transparent)]
    Core(#[from] mempalace_core::MempalaceError),
    #[error(transparent)]
    Embeddings(#[from] EmbeddingError),
    #[error(transparent)]
    Search(#[from] mempalace_search::SearchError),
    #[error(transparent)]
    Storage(#[from] mempalace_storage::StorageError),
    #[error(transparent)]
    Graph(#[from] mempalace_graph::GraphError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("time formatting error: {0}")]
    TimeFormat(String),
    #[error("federation error: {0}")]
    Federation(String),
    #[error("invalid {LINEAGE_ID_ENV}: {0}")]
    InvalidLineageBinding(String),
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

pub type Result<T> = std::result::Result<T, McpError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolDefinition {
    pub name: &'static str,
    pub description: &'static str,
    pub input_schema: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolRoutingCategory {
    /// Tool is always served from the local palace; never federated.
    LocalOnly,
    /// Tool routes via wing/room rules (`resolve_route`). Exception: DeleteDrawer
    /// is categorized here because it can reach remotes, but it does NOT use
    /// `resolve_route()` for write target — it deletes by ID locally first, then
    /// falls back to all remotes regardless of wing routing.
    RoutableDrawer,
    /// Tool routes via KG-specific rules (`resolve_kg_route`).
    RoutableKg,
    /// Tool participates in coordination federation (issue #102 Stage 4). Most of these route
    /// by an existing record's ID rather than a wing (see the `FederationRouter` "Coordination"
    /// section comment in `federation.rs`) — `mempalace_task_create` is the one exception,
    /// which uses `resolve_coordination_route`. `mempalace_coordination_event_get` is
    /// deliberately **not** in this category, even though its sibling
    /// `mempalace_coordination_events` is: Stage 3 never exposed a
    /// `GET /v1/coordination/events/{id}` route, only the paginated feed, so a single event has
    /// no remote counterpart to route to.
    RoutableCoordination,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ToolName {
    WakeUp,
    Status,
    ListWings,
    ListRooms,
    GetTaxonomy,
    GetAaaKSpec,
    KgQuery,
    KgAdd,
    KgInvalidate,
    KgTimeline,
    KgStats,
    Traverse,
    FindTunnels,
    GraphStats,
    Search,
    CheckDuplicate,
    AddDrawer,
    DeleteDrawer,
    DiaryWrite,
    DiaryRead,
    GetChangesSince,
    IdentityRead,
    IdentityUpdate,
    TaskCreate,
    TaskGet,
    TaskClaim,
    TaskRenew,
    TaskTransition,
    MessageSend,
    MessageGet,
    MessageAcknowledge,
    InboxRead,
    ArtifactPut,
    ArtifactGet,
    ResultPut,
    ResultGet,
    CoordinationEventGet,
    CoordinationEvents,
    SkillPropose,
    SkillGet,
    SkillVersions,
    SkillList,
    SkillRecordOutcome,
    SkillPromote,
    SkillRetire,
    SkillReviews,
    DelegationSpanStart,
    DelegationSpanGet,
    DelegationSpanClose,
    DelegationSpansForTask,
    DelegationCheckpointAppend,
    DelegationCheckpointGet,
    DelegationTrace,
    LineageSet,
    SelfObservationPropose,
    SelfObservationReview,
    IdentityPacket,
    MigrationRecord,
}

impl ToolName {
    fn all() -> [Self; 58] {
        [
            Self::WakeUp,
            Self::Status,
            Self::ListWings,
            Self::ListRooms,
            Self::GetTaxonomy,
            Self::GetAaaKSpec,
            Self::KgQuery,
            Self::KgAdd,
            Self::KgInvalidate,
            Self::KgTimeline,
            Self::KgStats,
            Self::Traverse,
            Self::FindTunnels,
            Self::GraphStats,
            Self::Search,
            Self::CheckDuplicate,
            Self::AddDrawer,
            Self::DeleteDrawer,
            Self::DiaryWrite,
            Self::DiaryRead,
            Self::GetChangesSince,
            Self::IdentityRead,
            Self::IdentityUpdate,
            Self::TaskCreate,
            Self::TaskGet,
            Self::TaskClaim,
            Self::TaskRenew,
            Self::TaskTransition,
            Self::MessageSend,
            Self::MessageGet,
            Self::MessageAcknowledge,
            Self::InboxRead,
            Self::ArtifactPut,
            Self::ArtifactGet,
            Self::ResultPut,
            Self::ResultGet,
            Self::CoordinationEventGet,
            Self::CoordinationEvents,
            Self::SkillPropose,
            Self::SkillGet,
            Self::SkillVersions,
            Self::SkillList,
            Self::SkillRecordOutcome,
            Self::SkillPromote,
            Self::SkillRetire,
            Self::SkillReviews,
            Self::DelegationSpanStart,
            Self::DelegationSpanGet,
            Self::DelegationSpanClose,
            Self::DelegationSpansForTask,
            Self::DelegationCheckpointAppend,
            Self::DelegationCheckpointGet,
            Self::DelegationTrace,
            Self::LineageSet,
            Self::SelfObservationPropose,
            Self::SelfObservationReview,
            Self::IdentityPacket,
            Self::MigrationRecord,
        ]
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::WakeUp => "mempalace_wake_up",
            Self::Status => "mempalace_status",
            Self::ListWings => "mempalace_list_wings",
            Self::ListRooms => "mempalace_list_rooms",
            Self::GetTaxonomy => "mempalace_get_taxonomy",
            Self::GetAaaKSpec => "mempalace_get_aaak_spec",
            Self::KgQuery => "mempalace_kg_query",
            Self::KgAdd => "mempalace_kg_add",
            Self::KgInvalidate => "mempalace_kg_invalidate",
            Self::KgTimeline => "mempalace_kg_timeline",
            Self::KgStats => "mempalace_kg_stats",
            Self::Traverse => "mempalace_traverse",
            Self::FindTunnels => "mempalace_find_tunnels",
            Self::GraphStats => "mempalace_graph_stats",
            Self::Search => "mempalace_search",
            Self::CheckDuplicate => "mempalace_check_duplicate",
            Self::AddDrawer => "mempalace_add_drawer",
            Self::DeleteDrawer => "mempalace_delete_drawer",
            Self::DiaryWrite => "mempalace_diary_write",
            Self::DiaryRead => "mempalace_diary_read",
            Self::GetChangesSince => "mempalace_get_changes_since",
            Self::IdentityRead => "mempalace_identity_read",
            Self::IdentityUpdate => "mempalace_identity_update",
            Self::TaskCreate => "mempalace_task_create",
            Self::TaskGet => "mempalace_task_get",
            Self::TaskClaim => "mempalace_task_claim",
            Self::TaskRenew => "mempalace_task_renew",
            Self::TaskTransition => "mempalace_task_transition",
            Self::MessageSend => "mempalace_message_send",
            Self::MessageGet => "mempalace_message_get",
            Self::MessageAcknowledge => "mempalace_message_acknowledge",
            Self::InboxRead => "mempalace_inbox_read",
            Self::ArtifactPut => "mempalace_artifact_put",
            Self::ArtifactGet => "mempalace_artifact_get",
            Self::ResultPut => "mempalace_result_put",
            Self::ResultGet => "mempalace_result_get",
            Self::CoordinationEventGet => "mempalace_coordination_event_get",
            Self::CoordinationEvents => "mempalace_coordination_events",
            Self::SkillPropose => "mempalace_skill_propose",
            Self::SkillGet => "mempalace_skill_get",
            Self::SkillVersions => "mempalace_skill_versions",
            Self::SkillList => "mempalace_skill_list",
            Self::SkillRecordOutcome => "mempalace_skill_record_outcome",
            Self::SkillPromote => "mempalace_skill_promote",
            Self::SkillRetire => "mempalace_skill_retire",
            Self::SkillReviews => "mempalace_skill_reviews",
            Self::DelegationSpanStart => "mempalace_delegation_span_start",
            Self::DelegationSpanGet => "mempalace_delegation_span_get",
            Self::DelegationSpanClose => "mempalace_delegation_span_close",
            Self::DelegationSpansForTask => "mempalace_delegation_spans_for_task",
            Self::DelegationCheckpointAppend => "mempalace_delegation_checkpoint_append",
            Self::DelegationCheckpointGet => "mempalace_delegation_checkpoint_get",
            Self::DelegationTrace => "mempalace_delegation_trace",
            Self::LineageSet => "mempalace_lineage_set",
            Self::SelfObservationPropose => "mempalace_self_observation_propose",
            Self::SelfObservationReview => "mempalace_self_observation_review",
            Self::IdentityPacket => "mempalace_identity_packet",
            Self::MigrationRecord => "mempalace_migration_record",
        }
    }

    fn from_name(name: &str) -> Option<Self> {
        Self::all().into_iter().find(|tool| tool.as_str() == name)
    }

    fn definition(self) -> ToolDefinition {
        match self {
            Self::WakeUp => ToolDefinition {
                name: self.as_str(),
                description: "Wake up into the palace. Returns the identity constitution, the MCP-bound or palace-default lineage's compiled identity packet, palace status, recent palace changes, current project history when provided, and recent diary entries across all agents. Lineage selection is fixed by server configuration and cannot be supplied by the model. If a configured binding is missing, wake-up uses the palace default and includes creation guidance in the response. Pass the current model and harness so engine-specific observations are filtered correctly. When federation is active the response also includes `remote_changes`: a per-remote map of change events from the last 24 hours (each event carries `origin: \"remote:<name>\"`), unreachable remotes appear as `{ \"unreachable\": true, \"error\": \"...\" }`, and a `next_cursor` is provided per remote for continuation via mempalace_get_changes_since.",
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "wing":{"type":"string","description":"Current project wing for project-specific history (optional, e.g. wing_myproject)"},
                        "agent_name":{"type":"string","description":"Current agent name for wake-up context (optional, e.g. claude)"},
                        "model":{"type":"string","description":"Current model identifier used to filter engine-scoped observations (optional)"},
                        "harness":{"type":"string","description":"Current harness identifier used to filter engine-scoped observations (optional)"},
                        "include_candidates":{"type":"boolean","description":"Include unreviewed self-observation candidates in a separate section (default false)"},
                        "latest_limit":{"type":"integer","description":"Max recent changes across the whole palace (default 8)"},
                        "project_limit":{"type":"integer","description":"Max recent changes for the current project wing (default 8)"},
                        "diary_limit":{"type":"integer","description":"Minimum recent diary entries across all agents (default 10; wake-up also includes every entry since diary_since)"},
                        "diary_since":{"type":"string","description":"Return wake-up diary entries filed at or after this RFC 3339 timestamp (optional, default: 24 hours ago)"}
                    }
                }),
            },
            Self::Status => ToolDefinition {
                name: self.as_str(),
                description: "Palace overview — total drawers, wing and room counts",
                input_schema: json!({"type":"object","properties":{}}),
            },
            Self::ListWings => ToolDefinition {
                name: self.as_str(),
                description: "List all wings with drawer counts",
                input_schema: json!({"type":"object","properties":{}}),
            },
            Self::ListRooms => ToolDefinition {
                name: self.as_str(),
                description: "List rooms within a wing (or all rooms if no wing given)",
                input_schema: json!({
                    "type":"object",
                    "properties":{"wing":{"type":"string","description":"Wing to list rooms for (optional)"}}
                }),
            },
            Self::GetTaxonomy => ToolDefinition {
                name: self.as_str(),
                description: "Full taxonomy: wing → room → drawer count",
                input_schema: json!({"type":"object","properties":{}}),
            },
            Self::GetAaaKSpec => ToolDefinition {
                name: self.as_str(),
                description: "Get the AAAK dialect specification — the compressed memory format MemPalace uses. Call this if you need to read or write AAAK-compressed memories.",
                input_schema: json!({"type":"object","properties":{}}),
            },
            Self::KgQuery => ToolDefinition {
                name: self.as_str(),
                description: "Query the knowledge graph for an entity's relationships. Returns typed facts with temporal validity. E.g. 'Max' → child_of Alice, loves chess, does swimming. Filter by date with as_of to see what was true at a point in time.",
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "entity":{"type":"string","description":"Entity to query (e.g. 'Max', 'MyProject', 'Alice')"},
                        "as_of":{"type":"string","description":"Date filter — only facts valid at this date (YYYY-MM-DD, optional)"},
                        "direction":{"type":"string","description":"outgoing (entity→?), incoming (?→entity), or both (default: both)"}
                    },
                    "required":["entity"]
                }),
            },
            Self::KgAdd => ToolDefinition {
                name: self.as_str(),
                description: "Add a fact to the knowledge graph. Subject → predicate → object with optional time window. E.g. ('Max', 'started_school', 'Year 7', valid_from='2026-09-01').",
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "subject":{"type":"string","description":"The entity doing/being something"},
                        "predicate":{"type":"string","description":"The relationship type (e.g. 'loves', 'works_on', 'daughter_of')"},
                        "object":{"type":"string","description":"The entity being connected to"},
                        "valid_from":{"type":"string","description":"When this became true (YYYY-MM-DD, optional)"},
                        "source_closet":{"type":"string","description":"Closet ID where this fact appears (optional)"}
                    },
                    "required":["subject","predicate","object"]
                }),
            },
            Self::KgInvalidate => ToolDefinition {
                name: self.as_str(),
                description: "Mark a fact as no longer true. E.g. ankle injury resolved, job ended, moved house.",
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "subject":{"type":"string","description":"Entity"},
                        "predicate":{"type":"string","description":"Relationship"},
                        "object":{"type":"string","description":"Connected entity"},
                        "ended":{"type":"string","description":"When it stopped being true (YYYY-MM-DD, default: today)"}
                    },
                    "required":["subject","predicate","object"]
                }),
            },
            Self::KgTimeline => ToolDefinition {
                name: self.as_str(),
                description: "Chronological timeline of facts. Shows the story of an entity (or everything) in order.",
                input_schema: json!({
                    "type":"object",
                    "properties":{"entity":{"type":"string","description":"Entity to get timeline for (optional — omit for full timeline)"}}
                }),
            },
            Self::KgStats => ToolDefinition {
                name: self.as_str(),
                description: "Knowledge graph overview: entities, triples, current vs expired facts, relationship types.",
                input_schema: json!({"type":"object","properties":{}}),
            },
            Self::Traverse => ToolDefinition {
                name: self.as_str(),
                description: "Walk the palace graph from a room. Shows connected ideas across wings — the tunnels. Like following a thread through the palace: start at 'chromadb-setup' in wing_code, discover it connects to wing_myproject (planning) and wing_user (feelings about it). Local palace only; not federated in v1.",
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "start_room":{"type":"string","description":"Room to start from (e.g. 'chromadb-setup', 'riley-school')"},
                        "max_hops":{"type":"integer","description":"How many connections to follow (default: 2)"}
                    },
                    "required":["start_room"]
                }),
            },
            Self::FindTunnels => ToolDefinition {
                name: self.as_str(),
                description: "Find rooms that bridge two wings — the hallways connecting different domains. E.g. what topics connect wing_code to wing_team? Local palace only; not federated in v1.",
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "wing_a":{"type":"string","description":"First wing (optional)"},
                        "wing_b":{"type":"string","description":"Second wing (optional)"}
                    }
                }),
            },
            Self::GraphStats => ToolDefinition {
                name: self.as_str(),
                description: "Palace graph overview: total rooms, tunnel connections, edges between wings. Local palace only; not federated in v1.",
                input_schema: json!({"type":"object","properties":{}}),
            },
            Self::Search => ToolDefinition {
                name: self.as_str(),
                description: "Semantic search. Returns verbatim drawer content with similarity scores. Results from mined files include `stale: true` when the source file changed since mining. Use `view` to scope to a specific branch view (e.g. 'feature-x'), 'canonical' for the default branch, or 'full' for every stored repository view.",
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "query":{"type":"string","description":"What to search for"},
                        "limit":{"type":"integer","description":"Max results (default 5)"},
                        "wing":{"type":"string","description":"Filter by wing (optional)"},
                        "room":{"type":"string","description":"Filter by room (optional)"},
                        "view":{"type":"string","description":"Scope to a named branch view or 'canonical' (optional)"}
                    },
                    "required":["query"]
                }),
            },
            Self::CheckDuplicate => ToolDefinition {
                name: self.as_str(),
                description: "Check if content already exists in the palace before filing",
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "content":{"type":"string","description":"Content to check"},
                        "threshold":{"type":"number","description":"Similarity threshold 0-1 (default 0.9)"}
                    },
                    "required":["content"]
                }),
            },
            Self::AddDrawer => ToolDefinition {
                name: self.as_str(),
                description: "File verbatim content into the palace. Checks for duplicates first.",
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "wing":{"type":"string","description":"Wing (project name)"},
                        "room":{"type":"string","description":"Room (aspect: backend, decisions, meetings...)"},
                        "content":{"type":"string","description":"Verbatim content to store — exact words, never summarized"},
                        "source_file":{"type":"string","description":"Where this came from (optional)"},
                        "added_by":{"type":"string","description":"Who is filing this (default: mcp)"}
                    },
                    "required":["wing","room","content"]
                }),
            },
            Self::DeleteDrawer => ToolDefinition {
                name: self.as_str(),
                description: "Delete a drawer by ID. Irreversible. Local deletion first by ID; if not found locally, falls back to remotes in name order. Does not use write routing.",
                input_schema: json!({
                    "type":"object",
                    "properties":{"drawer_id":{"type":"string","description":"ID of the drawer to delete"}},
                    "required":["drawer_id"]
                }),
            },
            Self::DiaryWrite => ToolDefinition {
                name: self.as_str(),
                description: "Write a diary entry with a concise summary of at most 400 characters. Project-scoped entries are stored in the specified project wing; agent-scoped entries are stored in the shared wing_agents diary. The agent name is recorded as author attribution, not as the storage partition. Always local; never federated.",
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "agent_name":{"type":"string","description":"Your name — recorded as the diary author"},
                        "entry":{"type":"string","description":"Your diary entry"},
                        "summary":{"type":"string","description":"Required summary (at most 400 characters) covering the outcome and important TODOs"},
                        "topic":{"type":"string","description":"Topic tag (optional, default: general)"},
                        "scope":{"type":"string","description":"Where to store the entry: agent or project (optional, default: agent)","enum":["agent","project"]},
                        "wing":{"type":"string","description":"Project wing for project-scoped entries. Ignored for agent-scoped entries, which always use wing_agents."}
                    },
                    "required":["agent_name","entry","summary"]
                }),
            },
            Self::DiaryRead => ToolDefinition {
                name: self.as_str(),
                description: "Read complete diary entries by entry_id for full detail, or list recent entries with filters. Always local; never federated.",
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "agent_name":{"type":"string","description":"Filter by diary author (optional, default: all agents)"},
                        "wing":{"type":"string","description":"Filter by wing (optional, default: all wings)"},
                        "topic":{"type":"string","description":"Filter by topic tag (optional, default: all topics)"},
                        "entry_id":{"type":"string","description":"Retrieve the complete entry for this wake-up entry identifier"},
                        "since":{"type":"string","description":"Return entries filed at or after this RFC 3339 timestamp (optional, default: 24 hours ago)"},
                        "last_n":{"type":"integer","description":"Number of recent entries to read (default: 10)"}
                    }
                }),
            },
            Self::GetChangesSince => ToolDefinition {
                name: self.as_str(),
                description: "Get all palace changes since a given timestamp. Call this at session start (or when coordinating with teammates) to catch up on what other agents have written. Returns events in chronological order with operation type, affected entity, actor, and timestamp. When federation is active, remote changes are merged in: each event carries an `origin` field (`\"local\"` or `\"remote:<name>\"`), and a top-level `remotes` object reports per-remote `{ next_cursor, count }` or `{ unreachable: true, error }`. CLOCK-SKEW CAVEAT: timestamps across machines are not directly comparable. Persist the per-origin `next_cursor` values from `remotes` and pass them back via `cursors` rather than reusing a single max-timestamp across origins. `limit` applies per origin (local and each remote each receive `limit` events independently).",
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "since":{"type":"string","description":"ISO 8601 timestamp — return only events after this point (optional, default: epoch)"},
                        "limit":{"type":"integer","description":"Max events to return per origin (default 50)"},
                        "cursors":{"type":"object","description":"Per-remote opaque cursor strings from a previous response's `remotes.<name>.next_cursor`. Pass back to continue pagination for specific remotes without re-fetching already-seen events.","additionalProperties":{"type":"string"}}
                    }
                }),
            },
            Self::IdentityRead => ToolDefinition {
                name: self.as_str(),
                description: "Read the configured identity.txt used by mempalace_wake_up.",
                input_schema: json!({"type":"object","properties":{}}),
            },
            Self::IdentityUpdate => ToolDefinition {
                name: self.as_str(),
                description: "Update the identity constitution used by future wake-ups. Reserve this for deliberate changes to durable identity, values, boundaries, and the working relationship; use self-observations or diaries for developing patterns and routine experience. Each update is limited to 16 KiB and the final identity.txt to 64 KiB. Use replace for a full corrected constitution or append for a deliberate amendment.",
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "content":{"type":"string","description":"Identity text or note to write, max 16 KiB per update","maxLength":IDENTITY_UPDATE_MAX_CONTENT_BYTES},
                        "agent_name":{"type":"string","description":"Agent making the update (optional)"},
                        "mode":{"type":"string","description":"replace or append (default replace)"}
                    },
                    "required":["content"]
                }),
            },
            Self::TaskCreate => coordination_definition(
                self,
                "Create a durable task idempotently in the given wing. Replaying the same created_by and idempotency_key returns the committed task. wing is normalised on write (myproject and wing_myproject are the same wing) and is inherited by every message, artifact, result, and audit event this task produces.",
                json!({"title":{"type":"string"},"description":{"type":"string"},"created_by":{"type":"string"},"wing":{"type":"string","description":"Owning wing, e.g. wing_myproject. Normalised on write."},"idempotency_key":{"type":"string"},"parent_id":{"type":"string"},"dependencies":{"type":"array","items":{"type":"string"}},"budget":{},"expires_at":{"type":"string"}}),
                &["title", "description", "created_by", "wing", "idempotency_key"],
            ),
            Self::TaskGet => coordination_definition(
                self,
                "Retrieve a task authoritatively by exact ID; returns found:false for a miss.",
                json!({"task_id":{"type":"string"}}),
                &["task_id"],
            ),
            Self::TaskClaim => coordination_definition(
                self,
                "Atomically claim or reclaim a task lease using an expected revision.",
                json!({"task_id":{"type":"string"},"worker":{"type":"string"},"expected_revision":{"type":"integer"},"lease_seconds":{"type":"integer"}}),
                &["task_id", "worker", "expected_revision", "lease_seconds"],
            ),
            Self::TaskRenew => coordination_definition(
                self,
                "Renew a task lease using an expected revision.",
                json!({"task_id":{"type":"string"},"worker":{"type":"string"},"expected_revision":{"type":"integer"},"lease_seconds":{"type":"integer"}}),
                &["task_id", "worker", "expected_revision", "lease_seconds"],
            ),
            Self::TaskTransition => coordination_definition(
                self,
                "Durably transition a task lifecycle state using compare-and-swap revision semantics.",
                json!({"task_id":{"type":"string"},"actor":{"type":"string"},"expected_revision":{"type":"integer"},"state":{"type":"string","enum":["pending","running","input_required","completed","cancelled","failed","expired"]},"details":{}}),
                &["task_id", "actor", "expected_revision", "state"],
            ),
            Self::MessageSend => coordination_definition(
                self,
                "Send an addressed task message idempotently. Semantic similarity is never used for deduplication.",
                json!({"task_id":{"type":"string"},"sender":{"type":"string"},"recipient":{"type":"string"},"kind":{"type":"string"},"payload":{},"idempotency_key":{"type":"string"},"envelope_version":{"type":"integer"}}),
                &["task_id", "sender", "recipient", "kind", "payload", "idempotency_key"],
            ),
            Self::MessageGet => coordination_definition(
                self,
                "Retrieve a message authoritatively by exact ID; returns found:false for a miss.",
                json!({"message_id":{"type":"string"}}),
                &["message_id"],
            ),
            Self::MessageAcknowledge => coordination_definition(
                self,
                "Acknowledge an addressed message as its recipient.",
                json!({"message_id":{"type":"string"},"actor":{"type":"string"}}),
                &["message_id", "actor"],
            ),
            Self::InboxRead => coordination_definition(
                self,
                "Read messages addressed to a recipient using an opaque local cursor. Optionally scope to one wing (matched via the sending task, normalised the same way as task creation). When coordination federation is configured for this wing (and it is not the shared diary wing), the local page is read and, concurrently, each configured remote is also queried; remote pages are reported under a top-level `remote_messages` object keyed by remote name, each entry one of `{ messages, next_cursor }`, `{ unreachable: true, error }`, or `{ capability_missing: true, capability, error }` when that remote does not advertise coordination support, mirroring `mempalace_get_changes_since`'s `remotes` field. `remote_messages` is an empty object whenever coordination federation is not configured for this wing (including the diary wing, or when no remotes are configured at all) — its absence of entries does not imply no remotes exist. Persist each remote's `next_cursor` and pass them back via `remote_cursors` to continue paging that remote without re-fetching already-seen messages; the local `cursor` argument is unrelated and only advances the local page.",
                json!({"recipient":{"type":"string"},"cursor":{"type":"integer"},"wing":{"type":"string","description":"Optional wing filter, e.g. wing_myproject. Normalised the same way as task creation."},"limit":{"type":"integer"},"unacknowledged_only":{"type":"boolean"},"remote_cursors":{"type":"object","description":"Per-remote opaque cursor strings from a previous response's `remote_messages.<name>.next_cursor`. Pass back to continue pagination for specific remotes without re-fetching already-seen messages.","additionalProperties":{"type":"string"}}}),
                &["recipient"],
            ),
            Self::ArtifactPut => coordination_definition(
                self,
                "Store an immutable task artifact idempotently with a content hash.",
                json!({"task_id":{"type":"string"},"created_by":{"type":"string"},"role":{"type":"string"},"media_type":{"type":"string"},"content":{"type":"string"},"idempotency_key":{"type":"string"}}),
                &["task_id", "created_by", "role", "media_type", "content", "idempotency_key"],
            ),
            Self::ArtifactGet => coordination_definition(
                self,
                "Retrieve an artifact authoritatively by exact ID; returns found:false for a miss.",
                json!({"artifact_id":{"type":"string"}}),
                &["artifact_id"],
            ),
            Self::ResultPut => coordination_definition(
                self,
                "Store an immutable task result idempotently. Semantic similarity is never used for deduplication.",
                json!({"task_id":{"type":"string"},"created_by":{"type":"string"},"payload":{},"idempotency_key":{"type":"string"}}),
                &["task_id", "created_by", "payload", "idempotency_key"],
            ),
            Self::ResultGet => coordination_definition(
                self,
                "Retrieve a task result authoritatively by exact ID; returns found:false for a miss.",
                json!({"result_id":{"type":"string"}}),
                &["result_id"],
            ),
            Self::CoordinationEventGet => coordination_definition(
                self,
                "Retrieve a coordination audit event authoritatively by exact ID; returns found:false for a miss.",
                json!({"event_id":{"type":"string"}}),
                &["event_id"],
            ),
            Self::CoordinationEvents => coordination_definition(
                self,
                "Read append-only coordination audit events using an opaque local cursor. Optionally scope to one task and/or one wing (normalised the same way as task creation). When coordination federation is configured for this wing (and it is not the shared diary wing), the local page is read and, concurrently, each configured remote is also queried; remote pages are reported under a top-level `remote_events` object keyed by remote name, each entry one of `{ events, next_cursor }`, `{ unreachable: true, error }`, or `{ capability_missing: true, capability, error }` when that remote does not advertise coordination support, mirroring `mempalace_get_changes_since`'s `remotes` field. `remote_events` is an empty object whenever coordination federation is not configured for this wing (including the diary wing, or when no remotes are configured at all) — its absence of entries does not imply no remotes exist. Persist each remote's `next_cursor` and pass them back via `remote_cursors` to continue paging that remote without re-fetching already-seen events; the local `cursor` argument is unrelated and only advances the local page.",
                json!({"cursor":{"type":"integer"},"task_id":{"type":"string"},"wing":{"type":"string","description":"Optional wing filter, e.g. wing_myproject. Normalised the same way as task creation."},"limit":{"type":"integer"},"remote_cursors":{"type":"object","description":"Per-remote opaque cursor strings from a previous response's `remote_events.<name>.next_cursor`. Pass back to continue pagination for specific remotes without re-fetching already-seen events.","additionalProperties":{"type":"string"}}}),
                &[],
            ),
            Self::SkillPropose => coordination_definition(
                self,
                "Propose a reusable procedure as a candidate skill version. The version is derived automatically as one past the highest existing version for skill_id; it is never caller-supplied. `scope: project` requires a `wing` naming the owning project, and the other scopes must omit it; a skill stays bound to that wing for its whole life. Replaying the same author and idempotency_key returns the committed version. Candidates are not authoritative until promoted.",
                json!({"skill_id":{"type":"string"},"scope":{"type":"string","enum":["agent","project","organization"]},"wing":{"type":"string","description":"Owning project wing, e.g. wing_myproject. Required for project scope, rejected otherwise."},"applicability":{"type":"string"},"instructions_ref":{"type":"string"},"required_capabilities":{"type":"array","items":{"type":"string"}},"required_tools":{"type":"array","items":{"type":"string"}},"required_permissions":{"type":"array","items":{"type":"string"}},"author":{"type":"string"},"provenance":{},"confidence":{"type":"number","minimum":0,"maximum":1},"idempotency_key":{"type":"string"}}),
                &["skill_id", "scope", "applicability", "instructions_ref", "author", "confidence", "idempotency_key"],
            ),
            Self::SkillGet => coordination_definition(
                self,
                "Retrieve one skill version authoritatively by exact skill_id and version; returns found:false for a miss. Never falls back to semantic search.",
                json!({"skill_id":{"type":"string"},"version":{"type":"integer"}}),
                &["skill_id", "version"],
            ),
            Self::SkillVersions => coordination_definition(
                self,
                "List every version of one skill, newest first, with its lifecycle status.",
                json!({"skill_id":{"type":"string"}}),
                &["skill_id"],
            ),
            Self::SkillList => coordination_definition(
                self,
                "Discover skills filtered by scope, status, and/or wing. Supplying `wing` hides project-scoped skills owned by other projects while keeping agent- and organization-scoped ones; omitting it spans every project and is an administrative view. Discovery only: dereference a specific version with mempalace_skill_get before treating it as authoritative. limit is clamped to 1..=500.",
                json!({"scope":{"type":"string","enum":["agent","project","organization"]},"status":{"type":"string","enum":["candidate","promoted","superseded","retired"]},"wing":{"type":"string","description":"Current project wing, e.g. wing_myproject"},"limit":{"type":"integer","minimum":1,"maximum":500}}),
                &[],
            ),
            Self::SkillRecordOutcome => coordination_definition(
                self,
                "Record a success, failure, or partial outcome against one specific skill version, optionally tied to a coordination task. Idempotent on recorded_by and idempotency_key. Shared-scope promotion requires at least one recorded outcome.",
                json!({"skill_id":{"type":"string"},"version":{"type":"integer"},"task_id":{"type":"string"},"result":{"type":"string","enum":["success","failure","partial"]},"evaluator":{"type":"string"},"notes":{"type":"string"},"recorded_by":{"type":"string"},"idempotency_key":{"type":"string"}}),
                &["skill_id", "version", "result", "evaluator", "recorded_by", "idempotency_key"],
            ),
            Self::SkillPromote => coordination_definition(
                self,
                "Promote a candidate skill version to authoritative for its scope, using compare-and-swap revision semantics. Agent-scoped skills may be promoted only by their own author. Project- and organization-scoped skills require a reviewer distinct from the author and at least one recorded outcome. Promotion atomically supersedes whichever version is authoritative at that moment, and governance is the stricter of this version's scope and the displaced version's scope — so a weaker-scoped successor cannot escape shared review.",
                json!({"skill_id":{"type":"string"},"version":{"type":"integer"},"expected_revision":{"type":"integer"},"reviewer":{"type":"string"},"reason":{"type":"string"}}),
                &["skill_id", "version", "expected_revision", "reviewer", "reason"],
            ),
            Self::SkillRetire => coordination_definition(
                self,
                "Retire a non-terminal skill version using compare-and-swap revision semantics. Retirement preserves the version and its audit history rather than deleting it.",
                json!({"skill_id":{"type":"string"},"version":{"type":"integer"},"expected_revision":{"type":"integer"},"reviewer":{"type":"string"},"reason":{"type":"string"}}),
                &["skill_id", "version", "expected_revision", "reviewer", "reason"],
            ),
            Self::SkillReviews => coordination_definition(
                self,
                "Read the append-only lifecycle review trail for one skill version, oldest first. Each entry identifies the reviewer and the status transition.",
                json!({"skill_id":{"type":"string"},"version":{"type":"integer"}}),
                &["skill_id", "version"],
            ),
            Self::DelegationSpanStart => coordination_definition(
                self,
                "Start a delegation span recording one delegated run against a coordination task. depth and fan_out_index are derived from the span tree, never caller-supplied. A child span's task_id must match its parent span's task_id, and a terminal (closed) parent cannot gain new children. Declared budgets are stored, not enforced: the host runtime enforces budgets during execution. Replaying the same delegator and idempotency_key returns the committed span.",
                json!({"task_id":{"type":"string"},"parent_span_id":{"type":"string"},"delegator":{"type":"string"},"delegate":{"type":"string"},"budgets":{},"idempotency_key":{"type":"string"}}),
                &["task_id", "delegator", "delegate", "idempotency_key"],
            ),
            Self::DelegationSpanGet => coordination_definition(
                self,
                "Retrieve a delegation span authoritatively by exact ID; returns found:false for a miss.",
                json!({"span_id":{"type":"string"}}),
                &["span_id"],
            ),
            Self::DelegationSpanClose => coordination_definition(
                self,
                "Close a running span with a terminal status and an explicit stop reason, using compare-and-swap revision semantics. status and stop_reason must be a coherent pair (e.g. completed only pairs with completed; failed pairs with error/budget_exhausted/max_depth_reached/max_fan_out_reached; cancelled pairs with cancelled/human_stop; expired pairs with budget_exhausted) — incoherent combinations are rejected. Recording budget_exhausted, max_depth_reached, or max_fan_out_reached is how a curtailed run stays visible rather than looking merely unfinished. actor is persisted as closed_by.",
                json!({"span_id":{"type":"string"},"expected_revision":{"type":"integer"},"status":{"type":"string","enum":["completed","failed","cancelled","expired"]},"stop_reason":{"type":"string","enum":["completed","budget_exhausted","max_depth_reached","max_fan_out_reached","cancelled","error","human_stop"]},"actor":{"type":"string"}}),
                &["span_id", "expected_revision", "status", "stop_reason", "actor"],
            ),
            Self::DelegationSpansForTask => coordination_definition(
                self,
                "List every delegation span recorded against one task, oldest first. More than one root span for a task is how repeated delegation of already-delegated work becomes visible.",
                json!({"task_id":{"type":"string"}}),
                &["task_id"],
            ),
            Self::DelegationCheckpointAppend => coordination_definition(
                self,
                "Append a bounded checkpoint to a span. Summaries are capped at 8 KiB per checkpoint and 256 KiB cumulative per span, by design: a checkpoint is a note about what happened, not the thing that happened, and the cumulative cap prevents reassembling an unbounded transcript by chunking. Store anything larger as an artifact and pass artifact_ref, which must belong to the same coordination task as the span. Rejected once the span is terminal (closed). Do not persist secrets or complete transcripts.",
                json!({"span_id":{"type":"string"},"checkpoint_type":{"type":"string","enum":["turn","tool_call","token_usage","retry","human_approval","claim","handoff"]},"summary":{"type":"string"},"artifact_ref":{"type":"string"},"actor":{"type":"string"},"idempotency_key":{"type":"string"}}),
                &["span_id", "checkpoint_type", "summary", "actor", "idempotency_key"],
            ),
            Self::DelegationCheckpointGet => coordination_definition(
                self,
                "Retrieve a checkpoint authoritatively by exact ID; returns found:false for a miss.",
                json!({"checkpoint_id":{"type":"string"}}),
                &["checkpoint_id"],
            ),
            Self::DelegationTrace => coordination_definition(
                self,
                "Reconstruct a delegated run from durable state: the root span, every descendant span, and each of their checkpoints in order. Returns a flat node list carrying parent_span_id so a consumer can rebuild the tree. This is the export path for trace visualization; no transcript is involved.",
                json!({"root_span_id":{"type":"string"}}),
                &["root_span_id"],
            ),
            Self::LineageSet => ToolDefinition {
                name: self.as_str(),
                description: "Create or revise a stable, provider-neutral agent lineage. A lineage is the persistent self whose memories and reviewed observations can span models and harnesses. The first lineage becomes the default. Updates require the current expected_revision; use 0 when explicitly creating a new lineage.",
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "lineage_id":{"type":"string","description":"Stable provider-neutral identifier, e.g. codex-dion"},
                        "display_name":{"type":"string","description":"Human-readable name for the persistent lineage"},
                        "description":{"type":"string","description":"What remains continuous across model and harness changes"},
                        "expected_revision":{"type":"integer","minimum":0,"description":"0 for explicit creation; current revision for an update"},
                        "set_default":{"type":"boolean","description":"Make this the default lineage for wake-up and identity packets (default false)"},
                        "actor":{"type":"string","description":"Who is making this change"}
                    },
                    "required":["lineage_id","display_name","description","expected_revision","actor"]
                }),
            },
            Self::SelfObservationPropose => ToolDefinition {
                name: self.as_str(),
                description: "Propose an evidence-backed candidate observation about a persistent agent lineage. Candidates do not shape the compiled identity packet until promoted through explicit review. Use lineage scope for portable traits, shared for a working pattern intentionally available to all lineages, and engine only for behavior tied to a matching model or harness.",
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "lineage_id":{"type":"string","description":"Lineage this observation belongs to"},
                        "statement":{"type":"string","description":"Concise falsifiable observation about the self"},
                        "behavioral_consequence":{"type":"string","description":"How this should change future behavior if promoted"},
                        "confidence":{"type":"number","minimum":0,"maximum":1,"description":"Confidence from 0 to 1"},
                        "evidence":{"type":"array","minItems":1,"items":{"type":"string"},"description":"Concrete drawer, diary, task, change, or other evidence references"},
                        "counterevidence":{"type":"array","items":{"type":"string"},"description":"Known evidence against or limiting the observation"},
                        "scope":{"type":"string","enum":["lineage","shared","engine"],"description":"Applicability scope (default lineage)"},
                        "model":{"type":"string","description":"Model associated with the observation (required with engine scope unless harness is provided)"},
                        "harness":{"type":"string","description":"Harness associated with the observation (required with engine scope unless model is provided)"},
                        "supersedes_observation_id":{"type":"string","description":"Older observation to supersede if this candidate is promoted"},
                        "author":{"type":"string","description":"Who is proposing the observation"}
                    },
                    "required":["lineage_id","statement","behavioral_consequence","confidence","evidence","author"]
                }),
            },
            Self::SelfObservationReview => ToolDefinition {
                name: self.as_str(),
                description: "Review a candidate self-observation and either promote it into the compiled identity packet or retire it. Reviews are revision-checked so concurrent or stale judgments cannot silently overwrite one another.",
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "observation_id":{"type":"string","description":"Candidate observation to review"},
                        "decision":{"type":"string","enum":["promote","retire"],"description":"Review outcome"},
                        "expected_revision":{"type":"integer","minimum":1,"description":"Current observation revision"},
                        "reviewer":{"type":"string","description":"Who performed the review"},
                        "reason":{"type":"string","description":"Evidence-based rationale for the decision"}
                    },
                    "required":["observation_id","decision","expected_revision","reviewer","reason"]
                }),
            },
            Self::IdentityPacket => ToolDefinition {
                name: self.as_str(),
                description: "Compile the identity constitution, stable lineage, reviewed self-observations, and recent model/harness migrations into a portable identity packet. Uses the lineage bound by MCP server configuration, or the palace default when no binding exists. If a configured binding is missing, falls back to the palace default and includes creation guidance in the response. The model cannot select or override the lineage. Engine-scoped observations are included only when their recorded model/harness matches the supplied runtime.",
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "agent_name":{"type":"string","description":"Current agent name for runtime context (optional)"},
                        "model":{"type":"string","description":"Current model identifier used to filter engine-scoped observations (optional)"},
                        "harness":{"type":"string","description":"Current harness identifier used to filter engine-scoped observations (optional)"},
                        "include_candidates":{"type":"boolean","description":"Include unreviewed candidates in a separate section (default false)"},
                        "observation_limit":{"type":"integer","description":"Max promoted observations and, separately, candidates before runtime filtering (default 20, max 50)"},
                        "migration_limit":{"type":"integer","description":"Max recent migrations (default 5, max 25)"}
                    }
                }),
            },
            Self::MigrationRecord => ToolDefinition {
                name: self.as_str(),
                description: "Record a model or harness migration for a persistent lineage, explicitly separating continuity from changed engine behavior. Migration evidence becomes part of future identity packets and wake-ups.",
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "lineage_id":{"type":"string","description":"Persistent lineage being migrated"},
                        "from_model":{"type":"string","description":"Previous model identifier (optional)"},
                        "from_harness":{"type":"string","description":"Previous harness identifier (optional)"},
                        "to_model":{"type":"string","description":"New model identifier"},
                        "to_harness":{"type":"string","description":"New harness identifier"},
                        "summary":{"type":"string","description":"Concise account of the migration"},
                        "continuities":{"type":"array","items":{"type":"string"},"description":"Behaviors, commitments, and understandings that carried over"},
                        "changes":{"type":"array","items":{"type":"string"},"description":"Observed changes attributable to the new engine or harness"},
                        "evidence":{"type":"array","minItems":1,"items":{"type":"string"},"description":"Concrete comparisons, tasks, or memory references supporting the account"},
                        "author":{"type":"string","description":"Who recorded the migration"}
                    },
                    "required":["lineage_id","to_model","to_harness","summary","continuities","changes","evidence","author"]
                }),
            },
        }
    }

    fn routing(self) -> ToolRoutingCategory {
        match self {
            Self::WakeUp
            | Self::DiaryWrite
            | Self::DiaryRead
            | Self::GetChangesSince
            | Self::Traverse
            | Self::FindTunnels
            | Self::GraphStats
            | Self::IdentityRead
            | Self::IdentityUpdate
            | Self::LineageSet
            | Self::SelfObservationPropose
            | Self::SelfObservationReview
            | Self::IdentityPacket
            | Self::MigrationRecord
            | Self::GetAaaKSpec
            // Not federated in this phase — see the module-level federation-semantics comment.
            | Self::SkillPropose
            | Self::SkillGet
            | Self::SkillVersions
            | Self::SkillList
            | Self::SkillRecordOutcome
            | Self::SkillPromote
            | Self::SkillRetire
            | Self::SkillReviews
            | Self::DelegationSpanStart
            | Self::DelegationSpanGet
            | Self::DelegationSpanClose
            | Self::DelegationSpansForTask
            | Self::DelegationCheckpointAppend
            | Self::DelegationCheckpointGet
            | Self::DelegationTrace
            // No wire counterpart to route to — see `ToolRoutingCategory::RoutableCoordination`.
            | Self::CoordinationEventGet => ToolRoutingCategory::LocalOnly,
            Self::TaskCreate
            | Self::TaskGet
            | Self::TaskClaim
            | Self::TaskRenew
            | Self::TaskTransition
            | Self::MessageSend
            | Self::MessageGet
            | Self::MessageAcknowledge
            | Self::InboxRead
            | Self::ArtifactPut
            | Self::ArtifactGet
            | Self::ResultPut
            | Self::ResultGet
            | Self::CoordinationEvents => ToolRoutingCategory::RoutableCoordination,
            Self::Search
            | Self::ListWings
            | Self::ListRooms
            | Self::GetTaxonomy
            | Self::Status
            | Self::CheckDuplicate
            | Self::AddDrawer
            | Self::DeleteDrawer => ToolRoutingCategory::RoutableDrawer,
            Self::KgQuery | Self::KgAdd | Self::KgInvalidate | Self::KgTimeline | Self::KgStats => {
                ToolRoutingCategory::RoutableKg
            }
        }
    }
}

fn coordination_definition(
    tool: ToolName,
    description: &'static str,
    properties: Value,
    required: &[&str],
) -> ToolDefinition {
    ToolDefinition {
        name: tool.as_str(),
        description,
        input_schema: json!({"type":"object","properties":properties,"required":required}),
    }
}

pub fn tool_definitions() -> Vec<ToolDefinition> {
    ToolName::all().into_iter().map(ToolName::definition).collect()
}

pub async fn serve_transport<P, R, W>(
    server: &McpServer<P>,
    reader: R,
    mut writer: W,
) -> std::result::Result<(), Box<dyn std::error::Error>>
where
    P: EmbeddingProvider + Send,
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut lines = reader.lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }

        let response = server.handle_line(&line).await;
        if response.is_null() {
            continue;
        }

        let response = serde_json::to_string(&response)?;
        writer.write_all(response.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
    }

    Ok(())
}

#[derive(Debug, Clone)]
pub struct McpServer<P> {
    runtime: Arc<Mutex<McpRuntime<P>>>,
    queue_limit: Arc<Semaphore>,
}

impl McpServer<FastembedProvider> {
    pub async fn from_default_config(base_dir_override: Option<&Path>) -> Result<Self> {
        let config = ConfigLoader::load_with_env(base_dir_override)?;
        let provider = default_provider(config.embedding_profile)?;
        let lineage_id = configured_lineage_id_from_env()?;
        Self::from_parts_with_lineage(config, provider, lineage_id).await
    }
}

pub fn configured_lineage_id_from_env() -> Result<Option<String>> {
    match std::env::var(LINEAGE_ID_ENV) {
        Ok(value) => validate_record_id_value(&value)
            .map(Some)
            .map_err(McpError::InvalidLineageBinding),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(McpError::InvalidLineageBinding(
            "must be valid Unicode".to_owned(),
        )),
    }
}

pub fn default_provider(profile: EmbeddingProfile) -> Result<FastembedProvider> {
    let cache_root = dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from(".cache"))
        .join("mempalace")
        .join("embeddings");
    let mut config = FastembedProviderConfig::new(cache_root);
    if env_flag("MEMPALACE_EMBED_ALLOW_DOWNLOADS") {
        config.allow_downloads = true;
        config.show_download_progress = true;
    }
    Ok(FastembedProvider::new(profile, config).try_initialize()?)
}

impl<P> McpServer<P>
where
    P: EmbeddingProvider + Send,
{
    pub async fn from_parts(config: MempalaceConfig, provider: P) -> Result<Self> {
        Self::from_parts_with_lineage(config, provider, None).await
    }

    pub async fn from_parts_with_lineage(
        config: MempalaceConfig,
        provider: P,
        lineage_id: Option<String>,
    ) -> Result<Self> {
        let lineage_id = lineage_id
            .map(|value| validate_record_id_value(&value))
            .transpose()
            .map_err(McpError::InvalidLineageBinding)?;
        let queue_limit = config.low_cpu.effective_queue_limit().min(Semaphore::MAX_PERMITS);
        let runtime = McpRuntime::new(config, provider, lineage_id).await?;
        Ok(Self {
            runtime: Arc::new(Mutex::new(runtime)),
            queue_limit: Arc::new(Semaphore::new(queue_limit)),
        })
    }

    pub async fn handle_json_value(&self, request: Value) -> Value {
        match serde_json::from_value::<JsonRpcRequest>(request) {
            Ok(request) => self.handle_request(request).await,
            Err(error) => jsonrpc_error(None, ErrorCode::ParseError, error.to_string()),
        }
    }

    pub async fn handle_line(&self, line: &str) -> Value {
        match serde_json::from_str::<Value>(line) {
            Ok(request) => self.handle_json_value(request).await,
            Err(error) => jsonrpc_error(None, ErrorCode::ParseError, error.to_string()),
        }
    }

    pub async fn handle_request(&self, request: JsonRpcRequest) -> Value {
        if request.id.is_none() {
            return Value::Null;
        }

        match request.method.as_str() {
            "initialize" => json!({
                "jsonrpc":"2.0",
                "id":request.id,
                "result":{
                    "protocolVersion":PROTOCOL_VERSION,
                    "capabilities":{"tools":{}},
                    "serverInfo":{"name":SERVER_NAME,"version":SERVER_VERSION}
                }
            }),
            "tools/list" => json!({
                "jsonrpc":"2.0",
                "id":request.id,
                "result":{
                    "tools":tool_definitions().into_iter().map(|tool| {
                        json!({
                            "name": tool.name,
                            "description": tool.description,
                            "inputSchema": tool.input_schema,
                        })
                    }).collect::<Vec<_>>()
                }
            }),
            "tools/call" => match ToolCallRequest::try_from(request) {
                Ok(call) => self.dispatch_tool(call).await,
                Err(error) => jsonrpc_error(
                    error.id,
                    ErrorCode::InvalidParams,
                    error.message.unwrap_or_else(|| "invalid tool call params".to_owned()),
                ),
            },
            _ => jsonrpc_error(
                request.id,
                ErrorCode::MethodNotFound,
                format!("Unknown method: {}", request.method),
            ),
        }
    }

    async fn dispatch_tool(&self, call: ToolCallRequest) -> Value {
        let Some(tool) = ToolName::from_name(&call.name) else {
            return jsonrpc_error(
                call.id,
                ErrorCode::MethodNotFound,
                format!("Unknown tool: {}", call.name),
            );
        };

        let _permit = match self.queue_limit.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => {
                return jsonrpc_error(
                    call.id,
                    ErrorCode::InternalError,
                    "server busy: low_cpu queue limit exceeded".to_owned(),
                );
            }
            Err(TryAcquireError::Closed) => {
                return jsonrpc_error(
                    call.id,
                    ErrorCode::InternalError,
                    "server unavailable".to_owned(),
                );
            }
        };

        let mut runtime = self.runtime.lock().await;

        // `tool.routing()` is not just documentation here: which of the four inner `match`
        // blocks below runs is decided by it, and each inner match only covers the `ToolName`
        // variants that category is supposed to contain. A tool miscategorized by `routing()`
        // (e.g. a coordination tool accidentally left `LocalOnly`) reaches the wrong inner
        // match, which has no arm for it, and panics via the catch-all below instead of
        // silently behaving as if it were still correctly routed — the same guarantee
        // `routing_categories_are_consistent_with_semantics_doc` checks statically, backstopped
        // here at dispatch time.
        let result = match tool.routing() {
            ToolRoutingCategory::LocalOnly => match tool {
                ToolName::WakeUp => runtime.tool_wake_up(&call.arguments).await,
                ToolName::GetAaaKSpec => runtime.tool_get_aaak_spec().await,
                ToolName::DiaryWrite => runtime.tool_diary_write(&call.arguments).await,
                ToolName::DiaryRead => runtime.tool_diary_read(&call.arguments).await,
                ToolName::GetChangesSince => runtime.tool_get_changes_since(&call.arguments).await,
                ToolName::Traverse => runtime.tool_traverse(&call.arguments).await,
                ToolName::FindTunnels => runtime.tool_find_tunnels(&call.arguments).await,
                ToolName::GraphStats => runtime.tool_graph_stats().await,
                ToolName::IdentityRead => runtime.tool_identity_read().await,
                ToolName::IdentityUpdate => runtime.tool_identity_update(&call.arguments).await,
                ToolName::LineageSet => runtime.tool_lineage_set(&call.arguments).await,
                ToolName::SelfObservationPropose => {
                    runtime.tool_self_observation_propose(&call.arguments).await
                }
                ToolName::SelfObservationReview => {
                    runtime.tool_self_observation_review(&call.arguments).await
                }
                ToolName::IdentityPacket => runtime.tool_identity_packet(&call.arguments).await,
                ToolName::MigrationRecord => runtime.tool_migration_record(&call.arguments).await,
                // Not federated in this phase (skills/delegation), or has no wire counterpart
                // at all (a single coordination event has no `GET .../events/{id}` route — only
                // the paginated feed does; see `tool_coordination_event_get`'s doc comment).
                ToolName::CoordinationEventGet => {
                    runtime.tool_coordination_event_get(&call.arguments).await
                }
                ToolName::SkillPropose => runtime.tool_skill_propose(&call.arguments).await,
                ToolName::SkillGet => runtime.tool_skill_get(&call.arguments).await,
                ToolName::SkillVersions => runtime.tool_skill_versions(&call.arguments).await,
                ToolName::SkillList => runtime.tool_skill_list(&call.arguments).await,
                ToolName::SkillRecordOutcome => {
                    runtime.tool_skill_record_outcome(&call.arguments).await
                }
                ToolName::SkillPromote => runtime.tool_skill_promote(&call.arguments).await,
                ToolName::SkillRetire => runtime.tool_skill_retire(&call.arguments).await,
                ToolName::SkillReviews => runtime.tool_skill_reviews(&call.arguments).await,
                ToolName::DelegationSpanStart => {
                    runtime.tool_delegation_span_start(&call.arguments).await
                }
                ToolName::DelegationSpanGet => {
                    runtime.tool_delegation_span_get(&call.arguments).await
                }
                ToolName::DelegationSpanClose => {
                    runtime.tool_delegation_span_close(&call.arguments).await
                }
                ToolName::DelegationSpansForTask => {
                    runtime.tool_delegation_spans_for_task(&call.arguments).await
                }
                ToolName::DelegationCheckpointAppend => {
                    runtime.tool_delegation_checkpoint_append(&call.arguments).await
                }
                ToolName::DelegationCheckpointGet => {
                    runtime.tool_delegation_checkpoint_get(&call.arguments).await
                }
                ToolName::DelegationTrace => runtime.tool_delegation_trace(&call.arguments).await,
                other => unreachable!(
                    "ToolName::routing() classified {other:?} as LocalOnly, \
                     but dispatch_tool's LocalOnly arm does not handle it"
                ),
            },
            ToolRoutingCategory::RoutableDrawer => match tool {
                ToolName::Search => runtime.tool_search(&call.arguments).await,
                ToolName::ListWings => runtime.tool_list_wings().await,
                ToolName::ListRooms => runtime.tool_list_rooms(&call.arguments).await,
                ToolName::GetTaxonomy => runtime.tool_get_taxonomy().await,
                ToolName::Status => runtime.tool_status().await,
                ToolName::CheckDuplicate => runtime.tool_check_duplicate(&call.arguments).await,
                ToolName::AddDrawer => runtime.tool_add_drawer(&call.arguments).await,
                ToolName::DeleteDrawer => runtime.tool_delete_drawer(&call.arguments).await,
                other => unreachable!(
                    "ToolName::routing() classified {other:?} as RoutableDrawer, \
                     but dispatch_tool's RoutableDrawer arm does not handle it"
                ),
            },
            ToolRoutingCategory::RoutableKg => match tool {
                ToolName::KgQuery => runtime.tool_kg_query(&call.arguments).await,
                ToolName::KgAdd => runtime.tool_kg_add(&call.arguments).await,
                ToolName::KgInvalidate => runtime.tool_kg_invalidate(&call.arguments).await,
                ToolName::KgTimeline => runtime.tool_kg_timeline(&call.arguments).await,
                ToolName::KgStats => runtime.tool_kg_stats().await,
                other => unreachable!(
                    "ToolName::routing() classified {other:?} as RoutableKg, \
                     but dispatch_tool's RoutableKg arm does not handle it"
                ),
            },
            ToolRoutingCategory::RoutableCoordination => match tool {
                ToolName::TaskCreate => runtime.tool_task_create(&call.arguments).await,
                ToolName::TaskGet => runtime.tool_task_get(&call.arguments).await,
                ToolName::TaskClaim => runtime.tool_task_claim(&call.arguments).await,
                ToolName::TaskRenew => runtime.tool_task_renew(&call.arguments).await,
                ToolName::TaskTransition => runtime.tool_task_transition(&call.arguments).await,
                ToolName::MessageSend => runtime.tool_message_send(&call.arguments).await,
                ToolName::MessageGet => runtime.tool_message_get(&call.arguments).await,
                ToolName::MessageAcknowledge => {
                    runtime.tool_message_acknowledge(&call.arguments).await
                }
                ToolName::InboxRead => runtime.tool_inbox_read(&call.arguments).await,
                ToolName::ArtifactPut => runtime.tool_artifact_put(&call.arguments).await,
                ToolName::ArtifactGet => runtime.tool_artifact_get(&call.arguments).await,
                ToolName::ResultPut => runtime.tool_result_put(&call.arguments).await,
                ToolName::ResultGet => runtime.tool_result_get(&call.arguments).await,
                ToolName::CoordinationEvents => {
                    runtime.tool_coordination_events(&call.arguments).await
                }
                other => unreachable!(
                    "ToolName::routing() classified {other:?} as RoutableCoordination, \
                     but dispatch_tool's RoutableCoordination arm does not handle it"
                ),
            },
        };

        match result {
            Ok(value) => json!({
                "jsonrpc":"2.0",
                "id":call.id,
                "result":{"content":[{"type":"text","text":serde_json::to_string_pretty(&value).unwrap_or_else(|_| "{}".to_owned())}]}
            }),
            Err(ToolError::InvalidParams(message)) => {
                jsonrpc_error(call.id, ErrorCode::InvalidParams, message)
            }
            Err(ToolError::Internal(error)) => {
                jsonrpc_error(call.id, ErrorCode::InternalError, error.to_string())
            }
        }
    }
}

#[derive(Debug)]
struct McpRuntime<P> {
    config: MempalaceConfig,
    bound_lineage_id: Option<String>,
    storage: StorageEngine,
    coordination: CoordinationStore,
    skills: SkillStore,
    delegation: DelegationStore,
    search: SearchRuntime<P>,
    federation: Option<FederationRouter>,
}

impl<P> McpRuntime<P>
where
    P: EmbeddingProvider + Send,
{
    async fn new(
        config: MempalaceConfig,
        provider: P,
        bound_lineage_id: Option<String>,
    ) -> Result<Self> {
        let storage = StorageEngine::open(&config.palace_path, config.embedding_profile).await?;
        let coordination = CoordinationStore::new(config.palace_path.join("storage.sqlite3"));
        coordination.ensure_schema()?;
        let skills = SkillStore::new(config.palace_path.join("storage.sqlite3"));
        skills.ensure_schema()?;
        let delegation = DelegationStore::new(config.palace_path.join("storage.sqlite3"));
        delegation.ensure_schema()?;
        let router = FederationRouter::new(config.federation.clone());
        let federation = if router.has_remotes() { Some(router) } else { None };
        Ok(Self {
            search: SearchRuntime::with_policy(
                provider,
                SearchRuntimePolicy { rerank_enabled: config.low_cpu.effective_rerank_enabled() },
            ),
            config,
            bound_lineage_id,
            storage,
            coordination,
            skills,
            delegation,
            federation,
        })
    }

    async fn tool_wake_up(&mut self, arguments: &Value) -> ToolResult<Value> {
        let wing =
            optional_string(arguments, "wing")?.map(|value| parse_wing_id(&value)).transpose()?;
        let agent_name = optional_string(arguments, "agent_name")?;
        let latest_limit = optional_usize(arguments, "latest_limit")?.unwrap_or(8).min(25);
        let project_limit = optional_usize(arguments, "project_limit")?.unwrap_or(8).min(25);
        let diary_limit = optional_usize(arguments, "diary_limit")?.unwrap_or(10).min(25);
        let diary_since = optional_string(arguments, "diary_since")?
            .as_deref()
            .map(parse_since_timestamp)
            .transpose()?
            .unwrap_or_else(|| OffsetDateTime::now_utc() - Duration::days(1));

        let identity = self.read_identity_text()?;
        let identity_packet =
            self.compile_identity_packet(arguments, &identity, Some("$.identity"))?;
        let status = self.status_payload(false).await?;
        let latest_events = self
            .storage
            .operational_store()
            .get_recent_changes(latest_limit)
            .map_tool_internal()?;
        let latest_changes = render_change_events(latest_events)?;

        let project_changes = if let Some(wing) = &wing {
            let search_limit = project_limit
                .saturating_mul(WAKE_UP_PROJECT_SEARCH_MULTIPLIER)
                .max(project_limit)
                .max(WAKE_UP_PROJECT_MIN_SEARCH_LIMIT);
            let all_events = self
                .storage
                .operational_store()
                .get_recent_changes(search_limit)
                .map_tool_internal()?;
            let mut events = Vec::with_capacity(project_limit);
            for event in all_events.into_iter().rev() {
                if change_event_matches_wing(&event, wing.as_str()) {
                    events.push(event);
                    if events.len() >= project_limit {
                        break;
                    }
                }
            }
            events.reverse();
            render_change_events(events)?
        } else {
            Vec::new()
        };
        let diary =
            self.wake_up_diary_payload(agent_name.as_deref(), diary_since, diary_limit).await?;

        let mut payload = json!({
            "identity_path": self.identity_path(),
            "identity": identity,
            "identity_packet": identity_packet,
            "status": status,
            "latest_changes": latest_changes,
            "current_project": {
                "wing": wing.as_ref().map(|wing| wing.as_str()).unwrap_or("unspecified"),
                "changes": project_changes,
                "message": if wing.is_some() {
                    Value::Null
                } else {
                    json!("Pass `wing` to include current project history.")
                },
            },
            "diary": diary,
        });

        if let Some(router) = &self.federation {
            if router.has_remotes() {
                let fan_since = format_rfc3339(OffsetDateTime::now_utc() - Duration::days(1))?;
                let cursors = BTreeMap::new();
                let remote_changes =
                    router.changes_fanout(Some(fan_since), Some(latest_limit), &cursors).await;
                payload["remote_changes"] = json!(remote_changes);
            }
        }

        Ok(payload)
    }

    async fn tool_status(&mut self) -> ToolResult<Value> {
        let mut payload = self.status_payload(true).await?;
        if let Some(router) = &self.federation {
            payload = router.status_merge(payload).await?;
            let local_wings: BTreeMap<String, usize> = payload["wings"]
                .as_object()
                .map(|obj| {
                    obj.iter()
                        .filter_map(|(k, v)| v.as_u64().map(|n| (k.clone(), n as usize)))
                        .collect()
                })
                .unwrap_or_default();
            if router.has_remotes() {
                payload["wing_availability"] = router.wing_availability(&local_wings);
            }
        }
        Ok(payload)
    }

    async fn status_payload(&mut self, include_rooms: bool) -> ToolResult<Value> {
        let drawers = self.list_all_drawers().await?;
        let mut wings = BTreeMap::<String, usize>::new();
        let mut rooms = include_rooms.then(BTreeMap::<String, usize>::new);
        for drawer in &drawers {
            if let Some(count) = wings.get_mut(drawer.wing.as_str()) {
                *count += 1;
            } else {
                wings.insert(drawer.wing.as_str().to_owned(), 1);
            }
            if let Some(rooms) = &mut rooms {
                if let Some(count) = rooms.get_mut(drawer.room.as_str()) {
                    *count += 1;
                } else {
                    rooms.insert(drawer.room.as_str().to_owned(), 1);
                }
            }
        }
        let mut payload = json!({
            "total_drawers": drawers.len(),
            "wings": wings,
            "palace_path": self.config.palace_path,
            "protocol": PALACE_PROTOCOL,
            "aaak_dialect": AAAK_SPEC,
        });
        if let Some(rooms) = rooms {
            payload["rooms"] = json!(rooms);
        }
        Ok(payload)
    }

    async fn tool_identity_read(&mut self) -> ToolResult<Value> {
        Ok(json!({
            "identity_path": self.identity_path(),
            "identity": self.read_identity_text()?,
        }))
    }

    async fn tool_identity_update(&mut self, arguments: &Value) -> ToolResult<Value> {
        let content = required_string(arguments, "content")?;
        let content = content.trim();
        if content.is_empty() {
            return Err(ToolError::InvalidParams("identity content cannot be blank".to_owned()));
        }
        if content.len() > IDENTITY_UPDATE_MAX_CONTENT_BYTES {
            return Err(ToolError::InvalidParams(format!(
                "identity content exceeds {} byte limit",
                IDENTITY_UPDATE_MAX_CONTENT_BYTES
            )));
        }
        let agent_name = optional_string(arguments, "agent_name")?;
        let mode = optional_string(arguments, "mode")?.unwrap_or_else(|| "replace".to_owned());
        if mode != "replace" && mode != "append" {
            return Err(ToolError::InvalidParams(
                "identity update mode must be `replace` or `append`".to_owned(),
            ));
        }

        let path = self.identity_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| {
                ToolError::Internal(McpError::Io { path: parent.to_path_buf(), source })
            })?;
        }

        let next_identity = if mode == "append" && path.exists() {
            let mut existing = fs::read_to_string(&path).map_err(|source| {
                ToolError::Internal(McpError::Io { path: path.clone(), source })
            })?;
            if !existing.ends_with('\n') {
                existing.push('\n');
            }
            existing.push_str(content);
            existing.push('\n');
            existing
        } else {
            format!("{content}\n")
        };
        if next_identity.len() > IDENTITY_MAX_BYTES {
            return Err(ToolError::InvalidParams(format!(
                "identity.txt would exceed {} byte limit",
                IDENTITY_MAX_BYTES
            )));
        }

        fs::write(&path, &next_identity)
            .map_err(|source| ToolError::Internal(McpError::Io { path: path.clone(), source }))?;

        let now = OffsetDateTime::now_utc();
        self.log_change(ChangeEvent {
            event_type: "identity_updated".to_owned(),
            occurred_at: now,
            entity_id: path.display().to_string(),
            actor: agent_name.clone(),
            details_json: Some(json!({"mode": mode, "path": path}).to_string()),
        });

        Ok(json!({
            "success": true,
            "identity_path": path,
            "mode": mode,
            "agent": agent_name,
            "timestamp": format_rfc3339(now)?,
        }))
    }

    async fn tool_lineage_set(&mut self, arguments: &Value) -> ToolResult<Value> {
        let lineage_id = required_record_id(arguments, "lineage_id")?;
        let display_name = required_non_blank_string(arguments, "display_name")?;
        let description = required_non_blank_string(arguments, "description")?;
        let expected_revision = required_non_negative_i64(arguments, "expected_revision")?;
        let set_default = optional_bool(arguments, "set_default")?.unwrap_or(false);
        let actor = required_non_blank_string(arguments, "actor")?;
        let now = OffsetDateTime::now_utc();

        let result = self
            .storage
            .operational_store()
            .set_lineage(
                &lineage_id,
                &display_name,
                &description,
                set_default,
                Some(expected_revision),
                now,
            )
            .map_tool_internal()?;
        let lineage = match result {
            RevisionedWrite::Applied(lineage) => lineage,
            RevisionedWrite::Conflict { actual_revision } => {
                return Ok(revision_conflict_payload(expected_revision, actual_revision));
            }
        };

        self.log_change(ChangeEvent {
            event_type: "lineage_set".to_owned(),
            occurred_at: now,
            entity_id: lineage_id,
            actor: Some(actor),
            details_json: Some(
                json!({
                    "display_name": lineage.display_name,
                    "revision": lineage.revision,
                    "is_default": lineage.is_default,
                })
                .to_string(),
            ),
        });

        Ok(json!({"success": true, "lineage": lineage}))
    }

    async fn tool_self_observation_propose(
        &mut self,
        arguments: &Value,
    ) -> ToolResult<Value> {
        let lineage_id = required_record_id(arguments, "lineage_id")?;
        let Some(_) = self
            .storage
            .operational_store()
            .get_lineage(&lineage_id)
            .map_tool_internal()?
        else {
            return Err(ToolError::InvalidParams(format!(
                "lineage `{lineage_id}` does not exist"
            )));
        };
        let statement = required_non_blank_string(arguments, "statement")?;
        let behavioral_consequence =
            required_non_blank_string(arguments, "behavioral_consequence")?;
        let confidence = required_confidence(arguments, "confidence")?;
        let evidence = required_string_array(arguments, "evidence", true)?;
        let counterevidence = optional_string_array(arguments, "counterevidence")?;
        let author = required_non_blank_string(arguments, "author")?;
        let model = optional_non_blank_string(arguments, "model")?;
        let harness = optional_non_blank_string(arguments, "harness")?;
        let scope = optional_string(arguments, "scope")?
            .as_deref()
            .map(parse_self_observation_scope)
            .transpose()?
            .unwrap_or(SelfObservationScope::Lineage);
        if scope == SelfObservationScope::Engine && model.is_none() && harness.is_none() {
            return Err(ToolError::InvalidParams(
                "engine-scoped observations require `model`, `harness`, or both".to_owned(),
            ));
        }
        let supersedes_observation_id = optional_string(arguments, "supersedes_observation_id")?
            .map(|value| validate_record_id("supersedes_observation_id", &value))
            .transpose()?;
        if let Some(superseded_id) = &supersedes_observation_id {
            let superseded = self
                .storage
                .operational_store()
                .get_self_observation(superseded_id)
                .map_tool_internal()?
                .ok_or_else(|| {
                    ToolError::InvalidParams(format!(
                        "superseded observation `{superseded_id}` does not exist"
                    ))
                })?;
            if superseded.lineage_id != lineage_id {
                return Err(ToolError::InvalidParams(format!(
                    "superseded observation `{superseded_id}` belongs to another lineage"
                )));
            }
            if superseded.status != SelfObservationStatus::Promoted {
                return Err(ToolError::InvalidParams(format!(
                    "superseded observation `{superseded_id}` must currently be promoted"
                )));
            }
        }

        let now = OffsetDateTime::now_utc();
        let observation = SelfObservationRecord {
            observation_id: generated_record_id("obs", &lineage_id, &statement, now),
            lineage_id: lineage_id.clone(),
            status: SelfObservationStatus::Candidate,
            scope,
            statement,
            behavioral_consequence,
            confidence,
            author: author.clone(),
            model,
            harness,
            evidence,
            counterevidence,
            supersedes_observation_id,
            revision: 1,
            created_at: now,
            updated_at: now,
        };
        self.storage
            .operational_store()
            .propose_self_observation(&observation)
            .map_tool_internal()?;
        self.log_change(ChangeEvent {
            event_type: "self_observation_proposed".to_owned(),
            occurred_at: now,
            entity_id: observation.observation_id.clone(),
            actor: Some(author),
            details_json: Some(
                json!({
                    "lineage_id": lineage_id,
                    "scope": observation.scope,
                    "statement": observation.statement,
                })
                .to_string(),
            ),
        });

        Ok(json!({"success": true, "observation": observation}))
    }

    async fn tool_self_observation_review(
        &mut self,
        arguments: &Value,
    ) -> ToolResult<Value> {
        let observation_id = required_record_id(arguments, "observation_id")?;
        let decision = required_non_blank_string(arguments, "decision")?;
        let expected_revision = required_positive_i64(arguments, "expected_revision")?;
        let reviewer = required_non_blank_string(arguments, "reviewer")?;
        let reason = required_non_blank_string(arguments, "reason")?;
        let new_status = match decision.as_str() {
            "promote" => SelfObservationStatus::Promoted,
            "retire" => SelfObservationStatus::Retired,
            other => {
                return Err(ToolError::InvalidParams(format!(
                    "invalid decision `{other}`; expected promote or retire"
                )));
            }
        };

        if let Some(current) = self
            .storage
            .operational_store()
            .get_self_observation(&observation_id)
            .map_tool_internal()?
            && current.status != SelfObservationStatus::Candidate
        {
            return Err(ToolError::InvalidParams(format!(
                "observation `{observation_id}` is {}; only candidates can be reviewed",
                current.status.as_str()
            )));
        }

        let now = OffsetDateTime::now_utc();
        let result = self
            .storage
            .operational_store()
            .review_self_observation(
                &observation_id,
                expected_revision,
                new_status,
                &reviewer,
                &reason,
                now,
            )
            .map_tool_internal()?;
        let observation = match result {
            RevisionedWrite::Applied(observation) => observation,
            RevisionedWrite::Conflict { actual_revision } => {
                return Ok(revision_conflict_payload(expected_revision, actual_revision));
            }
        };

        self.log_change(ChangeEvent {
            event_type: "self_observation_reviewed".to_owned(),
            occurred_at: now,
            entity_id: observation_id,
            actor: Some(reviewer),
            details_json: Some(
                json!({
                    "lineage_id": observation.lineage_id,
                    "decision": decision,
                    "status": observation.status,
                    "revision": observation.revision,
                    "reason": reason,
                })
                .to_string(),
            ),
        });

        Ok(json!({"success": true, "observation": observation}))
    }

    async fn tool_identity_packet(&mut self, arguments: &Value) -> ToolResult<Value> {
        let identity = self.read_identity_text()?;
        self.compile_identity_packet(arguments, &identity, None)
    }

    async fn tool_migration_record(&mut self, arguments: &Value) -> ToolResult<Value> {
        let lineage_id = required_record_id(arguments, "lineage_id")?;
        let Some(_) = self
            .storage
            .operational_store()
            .get_lineage(&lineage_id)
            .map_tool_internal()?
        else {
            return Err(ToolError::InvalidParams(format!(
                "lineage `{lineage_id}` does not exist"
            )));
        };
        let from_model = optional_non_blank_string(arguments, "from_model")?;
        let from_harness = optional_non_blank_string(arguments, "from_harness")?;
        let to_model = required_non_blank_string(arguments, "to_model")?;
        let to_harness = required_non_blank_string(arguments, "to_harness")?;
        let summary = required_non_blank_string(arguments, "summary")?;
        let continuities = required_string_array(arguments, "continuities", false)?;
        let changes = required_string_array(arguments, "changes", false)?;
        let evidence = required_string_array(arguments, "evidence", true)?;
        let author = required_non_blank_string(arguments, "author")?;
        let now = OffsetDateTime::now_utc();
        let migration = LineageMigrationRecord {
            migration_id: generated_record_id("migration", &lineage_id, &summary, now),
            lineage_id: lineage_id.clone(),
            from_model,
            from_harness,
            to_model,
            to_harness,
            summary,
            continuities,
            changes,
            evidence,
            author: author.clone(),
            created_at: now,
        };
        self.storage
            .operational_store()
            .record_lineage_migration(&migration)
            .map_tool_internal()?;
        self.log_change(ChangeEvent {
            event_type: "lineage_migration_recorded".to_owned(),
            occurred_at: now,
            entity_id: migration.migration_id.clone(),
            actor: Some(author),
            details_json: Some(
                json!({
                    "lineage_id": lineage_id,
                    "from_model": migration.from_model,
                    "from_harness": migration.from_harness,
                    "to_model": migration.to_model,
                    "to_harness": migration.to_harness,
                })
                .to_string(),
            ),
        });

        Ok(json!({"success": true, "migration": migration}))
    }

    fn compile_identity_packet(
        &self,
        arguments: &Value,
        identity: &str,
        identity_ref: Option<&str>,
    ) -> ToolResult<Value> {
        if arguments.get("lineage_id").is_some() {
            return Err(ToolError::InvalidParams(format!(
                "`lineage_id` is not a model-selectable parameter; bind this MCP server with {LINEAGE_ID_ENV} or configure a palace default"
            )));
        }
        let agent_name = optional_non_blank_string(arguments, "agent_name")?;
        let model = optional_non_blank_string(arguments, "model")?;
        let harness = optional_non_blank_string(arguments, "harness")?;
        let include_candidates = optional_bool(arguments, "include_candidates")?.unwrap_or(false);
        let observation_limit = optional_usize(arguments, "observation_limit")?.unwrap_or(20).min(50);
        let migration_limit = optional_usize(arguments, "migration_limit")?.unwrap_or(5).min(25);
        let operational_store = self.storage.operational_store();
        let (lineage, lineage_selection) = match self.bound_lineage_id.as_deref() {
            Some(lineage_id) => {
                let lineage = operational_store.get_lineage(lineage_id).map_tool_internal()?;
                match lineage {
                    Some(lineage) => (
                        Some(lineage),
                        json!({
                            "source": "mcp_server_environment",
                            "lineage_id": lineage_id,
                            "override_allowed": false,
                        }),
                    ),
                    None => {
                        let fallback = operational_store.get_default_lineage().map_tool_internal()?;
                        let fallback_id = fallback.as_ref().map(|record| record.lineage_id.clone());
                        let message = if fallback_id.is_some() {
                            format!(
                                "{LINEAGE_ID_ENV} is set to `{lineage_id}`, but that lineage does not exist. The palace default is being used for this response. To create it, call mempalace_lineage_set with lineage_id `{lineage_id}`, display_name, description, expected_revision 0, set_default false, and actor, then retry wake-up."
                            )
                        } else {
                            format!(
                                "{LINEAGE_ID_ENV} is set to `{lineage_id}`, but that lineage does not exist and no palace default is configured. Create it with mempalace_lineage_set using lineage_id `{lineage_id}`, display_name, description, expected_revision 0, set_default false, and actor, then retry wake-up."
                            )
                        };
                        (
                            fallback,
                            json!({
                                "source": "palace_default_fallback",
                                "lineage_id": fallback_id,
                                "requested_lineage_id": lineage_id,
                                "override_allowed": false,
                                "message": message,
                            }),
                        )
                    }
                }
            }
            None => {
                let lineage = operational_store.get_default_lineage().map_tool_internal()?;
                let lineage_id = lineage.as_ref().map(|record| record.lineage_id.clone());
                (
                    lineage,
                    json!({
                        "source": "palace_default",
                        "lineage_id": lineage_id,
                        "override_allowed": false,
                    }),
                )
            }
        };
        let constitution = match identity_ref {
            Some(identity_ref) => json!({
                "role": "durable identity, values, boundaries, and working relationship",
                "identity_path": self.identity_path(),
                "identity_ref": identity_ref,
            }),
            None => json!({
                "role": "durable identity, values, boundaries, and working relationship",
                "identity_path": self.identity_path(),
                "identity": identity,
            }),
        };
        let Some(lineage) = lineage else {
            let available_lineages = operational_store.list_lineages().map_tool_internal()?;
            let message = match self.bound_lineage_id.as_deref() {
                Some(lineage_id) => format!(
                    "{LINEAGE_ID_ENV} is set to `{lineage_id}`, but that lineage does not exist and no palace default is configured. Create it with mempalace_lineage_set using lineage_id `{lineage_id}`, display_name, description, expected_revision 0, set_default false, and actor, then retry wake-up."
                ),
                None => "No default lineage is configured. Create one with mempalace_lineage_set or bind the MCP server with MEMPALACE_LINEAGE_ID.".to_owned(),
            };
            return Ok(json!({
                "packet_version": 1,
                "configured": false,
                "message": message,
                "lineage_selection": lineage_selection,
                "available_lineages": available_lineages,
                "constitution": constitution,
                "runtime": {"agent_name": agent_name, "model": model, "harness": harness},
                "compiled_at": format_rfc3339(OffsetDateTime::now_utc())?,
            }));
        };

        let mut promoted = self.collect_packet_observations(
            &lineage,
            &[SelfObservationStatus::Promoted],
            observation_limit,
        )?;
        promoted.retain(|observation| {
            observation_applies_to_runtime(observation, model.as_deref(), harness.as_deref())
        });
        let candidates = if include_candidates {
            let mut candidates = self.collect_packet_observations(
                &lineage,
                &[SelfObservationStatus::Candidate],
                observation_limit,
            )?;
            candidates.retain(|observation| {
                observation_applies_to_runtime(observation, model.as_deref(), harness.as_deref())
            });
            json!(candidates)
        } else {
            Value::Null
        };
        let migrations = operational_store
            .list_lineage_migrations(&lineage.lineage_id, migration_limit)
            .map_tool_internal()?;

        Ok(json!({
            "packet_version": 1,
            "configured": true,
            "constitution": constitution,
            "lineage": lineage,
            "lineage_selection": lineage_selection,
            "promoted_observations": promoted,
            "candidates": candidates,
            "recent_migrations": migrations,
            "runtime": {"agent_name": agent_name, "model": model, "harness": harness},
            "compiled_at": format_rfc3339(OffsetDateTime::now_utc())?,
        }))
    }

    fn collect_packet_observations(
        &self,
        lineage: &AgentLineageRecord,
        statuses: &[SelfObservationStatus],
        limit: usize,
    ) -> ToolResult<Vec<SelfObservationRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let operational_store = self.storage.operational_store();
        let lineages = operational_store.list_lineages().map_tool_internal()?;
        let mut observations = Vec::new();
        for candidate_lineage in lineages {
            let scope = (candidate_lineage.lineage_id != lineage.lineage_id)
                .then_some(SelfObservationScope::Shared);
            let records = operational_store
                .list_self_observations_scoped(
                    &candidate_lineage.lineage_id,
                    statuses,
                    scope,
                    limit,
                )
                .map_tool_internal()?;
            observations.extend(records);
        }
        observations.sort_by(|left, right| {
            right
                .updated_at
                .cmp(&left.updated_at)
                .then_with(|| left.observation_id.cmp(&right.observation_id))
        });
        let mut seen = HashSet::new();
        observations.retain(|record| seen.insert(record.observation_id.clone()));
        observations.truncate(limit);
        Ok(observations)
    }

    async fn tool_list_wings(&mut self) -> ToolResult<Value> {
        let drawers = self.list_all_drawers().await?;
        let mut wings = BTreeMap::<String, usize>::new();
        for drawer in drawers {
            *wings.entry(drawer.wing.as_str().to_owned()).or_default() += 1;
        }
        let mut payload = json!({ "wings": wings });
        if let Some(router) = &self.federation {
            payload = router.wings_merge(payload).await?;
            let local_wings: BTreeMap<String, usize> = payload["wings"]
                .as_object()
                .map(|obj| {
                    obj.iter()
                        .filter_map(|(k, v)| v.as_u64().map(|n| (k.clone(), n as usize)))
                        .collect()
                })
                .unwrap_or_default();
            if router.has_remotes() {
                payload["wing_availability"] = router.wing_availability(&local_wings);
            }
        }
        Ok(payload)
    }

    async fn tool_list_rooms(&mut self, arguments: &Value) -> ToolResult<Value> {
        let wing = optional_string(arguments, "wing")?;
        let filter = DrawerFilter {
            wing: wing.as_deref().map(parse_wing_id).transpose()?,
            ..DrawerFilter::default()
        };
        let drawers = self.storage.drawer_store().list_drawers(&filter).await.map_tool()?;
        let mut rooms = BTreeMap::<String, usize>::new();
        let mut room_wings = BTreeMap::<String, usize>::new();
        for drawer in drawers {
            *rooms.entry(drawer.room.as_str().to_owned()).or_default() += 1;
            *room_wings.entry(drawer.wing.as_str().to_owned()).or_default() += 1;
        }
        let mut payload = json!({
            "wing": wing.clone().unwrap_or_else(|| "all".to_owned()),
            "rooms": rooms,
        });
        if let Some(router) = &self.federation {
            payload = router.rooms_merge(payload, wing.as_deref()).await?;
            if router.has_remotes() {
                payload["wing_availability"] = router.wing_availability(&room_wings);
            }
        }
        Ok(payload)
    }

    async fn tool_get_taxonomy(&mut self) -> ToolResult<Value> {
        let drawers = self.list_all_drawers().await?;
        let mut taxonomy = BTreeMap::<String, BTreeMap<String, usize>>::new();
        let mut wings = BTreeMap::<String, usize>::new();
        for drawer in drawers {
            *taxonomy
                .entry(drawer.wing.as_str().to_owned())
                .or_default()
                .entry(drawer.room.as_str().to_owned())
                .or_default() += 1;
            *wings.entry(drawer.wing.as_str().to_owned()).or_default() += 1;
        }
        let mut payload = json!({ "taxonomy": taxonomy });
        if let Some(router) = &self.federation {
            payload = router.taxonomy_merge(payload).await?;
            if router.has_remotes() {
                payload["wing_availability"] = router.wing_availability(&wings);
            }
        }
        Ok(payload)
    }

    async fn tool_get_aaak_spec(&mut self) -> ToolResult<Value> {
        Ok(json!({ "aaak_spec": AAAK_SPEC }))
    }

    async fn tool_search(&mut self, arguments: &Value) -> ToolResult<Value> {
        let query = required_string(arguments, "query")?;
        let limit = optional_usize(arguments, "limit")?
            .unwrap_or(5)
            .min(self.config.low_cpu.effective_search_results_limit());
        let wing =
            optional_string(arguments, "wing")?.map(|value| parse_wing_id(&value)).transpose()?;
        let room =
            optional_string(arguments, "room")?.map(|value| parse_room_id(&value)).transpose()?;
        let view = optional_string(arguments, "view")?;

        // ── Federation path ──
        if let Some(router) = &self.federation {
            let wing_str = wing.as_ref().map(|w| w.as_str());
            let room_str = room.as_ref().map(|r| r.as_str());
            let (include_local, remote_targets) = router.plan_search_targets(wing_str, room_str);
            if !remote_targets.is_empty() {
                let overlay =
                    view.as_deref().is_some_and(|view| view != "canonical" && view != "full");
                let max_candidate_limit = limit.saturating_mul(10).max(limit);
                let mut candidate_limit = limit;
                loop {
                    // Fan out with a wider bounded window when local branch rows
                    // shadow remote candidates, matching local overlay semantics.
                    let local_values: Vec<Value> = if include_local {
                        self.search
                            .search(
                                self.storage.drawer_store(),
                                &SearchQuery {
                                    text: query.clone(),
                                    wing: wing.clone(),
                                    room: room.clone(),
                                    limit: candidate_limit,
                                    profile: self.config.embedding_profile,
                                    view: view.clone(),
                                },
                            )
                            .await
                            .map_tool()?
                            .into_iter()
                            .map(|result| {
                                let mut obj = json!({
                                    "wing": result.wing,
                                    "room": result.room,
                                    "similarity": round_similarity(result.score),
                                    "text": result.content,
                                    "source_file": result.source_file,
                                    "content_hash": hash_text(&result.content),
                                    "origin": "local",
                                });
                                if result.stale {
                                    obj["stale"] = json!(true);
                                }
                                if let Some(ref v) = result.view {
                                    obj["view"] = json!(v);
                                }
                                obj
                            })
                            .collect()
                    } else {
                        vec![]
                    };
                    let mut payload = router
                        .search(
                            local_values,
                            &query,
                            wing_str,
                            room_str,
                            view.as_deref(),
                            candidate_limit,
                            &remote_targets,
                        )
                        .await?;
                    let candidate_count = payload["results"].as_array().map_or(0, Vec::len);
                    if include_local {
                        self.filter_federated_view_overrides(&mut payload, &view, &wing).await?;
                    }
                    let result_count = payload["results"].as_array().map_or(0, Vec::len);
                    if !overlay
                        || result_count >= limit
                        || candidate_count < candidate_limit
                        || candidate_limit == max_candidate_limit
                    {
                        if let Some(results) = payload["results"].as_array_mut() {
                            results.truncate(limit);
                        }
                        return Ok(payload);
                    }
                    candidate_limit = candidate_limit.saturating_mul(2).min(max_candidate_limit);
                }
            }
        }

        let results = self
            .search
            .search(
                self.storage.drawer_store(),
                &SearchQuery {
                    text: query.clone(),
                    wing: wing.clone(),
                    room: room.clone(),
                    limit,
                    profile: self.config.embedding_profile,
                    view: view.clone(),
                },
            )
            .await
            .map_tool()?;

        let payload = json!({
            "query": query,
            "filters": {
                "wing": wing.as_ref().map(|value| value.to_string()),
                "room": room.as_ref().map(|value| value.to_string()),
                "view": view,
            },
            "results": results.into_iter().map(|result| {
                let mut obj = json!({
                    "wing": result.wing,
                    "room": result.room,
                    "similarity": round_similarity(result.score),
                    "text": result.content,
                    "source_file": result.source_file,
                });
                if result.stale {
                    obj["stale"] = json!(true);
                }
                if let Some(ref v) = result.view {
                    obj["view"] = json!(v);
                }
                obj
            }).collect::<Vec<_>>()
        });
        Ok(payload)
    }

    /// Branch rows only need to be loaded for paths returned by remote search.
    /// Loading the entire branch view here would materialize every embedding and
    /// chunk merely to discover paths that could not affect this response.
    async fn filter_federated_view_overrides(
        &self,
        payload: &mut Value,
        view: &Option<String>,
        wing: &Option<WingId>,
    ) -> ToolResult<()> {
        let Some(view) = view.as_deref().filter(|view| *view != "canonical" && *view != "full")
        else {
            return Ok(());
        };
        let Some(results) = payload["results"].as_array() else {
            return Ok(());
        };
        let remote_paths: HashSet<String> = results
            .iter()
            .filter(|result| result["origin"].as_str() != Some("local"))
            .filter_map(|result| result["source_file"].as_str().map(str::to_owned))
            .collect();
        if remote_paths.is_empty() {
            return Ok(());
        }
        let local_override_paths: HashSet<(String, String)> = self
            .storage
            .drawer_store()
            .list_drawers(&DrawerFilter {
                wing: wing.clone(),
                source_files: remote_paths.into_iter().collect(),
                view: Some(view.to_owned()),
                branch_view_only: true,
                ..DrawerFilter::default()
            })
            .await
            .map_tool()?
            .into_iter()
            .map(|record| (record.wing.as_str().to_owned(), record.source_file))
            .collect();
        if let Some(results) = payload["results"].as_array_mut() {
            results.retain(|result| {
                result["origin"].as_str() == Some("local")
                    || !matches!(
                        (result["wing"].as_str(), result["source_file"].as_str()),
                        (Some(wing), Some(path))
                            if local_override_paths
                                .contains(&(wing.to_owned(), path.to_owned()))
                    )
            });
        }
        Ok(())
    }

    async fn tool_check_duplicate(&mut self, arguments: &Value) -> ToolResult<Value> {
        let content = required_string(arguments, "content")?;
        let threshold =
            optional_f32(arguments, "threshold")?.unwrap_or(DEFAULT_DUPLICATE_THRESHOLD);
        let mut matches = self.find_duplicates(&content, threshold).await?;

        // ── Federation path ──
        if let Some(router) = &self.federation {
            let remote_matches = router.check_duplicate_all_remotes(&content, threshold).await;
            matches.extend(remote_matches);
        }

        Ok(json!({
            "is_duplicate": !matches.is_empty(),
            "matches": matches,
        }))
    }

    async fn tool_add_drawer(&mut self, arguments: &Value) -> ToolResult<Value> {
        let wing = parse_wing_id(&required_string(arguments, "wing")?)?;
        let room = parse_room_id(&required_string(arguments, "room")?)?;
        let content = required_string(arguments, "content")?;
        let source_file = optional_string(arguments, "source_file")?.unwrap_or_default();
        let added_by = optional_string(arguments, "added_by")?.unwrap_or_else(|| "mcp".to_owned());
        let content_hash = hash_text(&content);

        // ── Resolve federation route once, reuse for dual-write decisions ──
        let route = self.federation.as_ref().map(|router| {
            router.resolve_drawer_route(
                Some(wing.as_str()),
                Some(room.as_str()),
                if source_file.is_empty() { None } else { Some(source_file.as_str()) },
            )
        });
        let is_both = match (&self.federation, &route) {
            (Some(router), Some(route)) => router.is_dual_write(route),
            _ => false,
        };

        // ── Non-Both federation: remote-only or local-only ──
        if !is_both {
            if let Some(router) = &self.federation {
                if let Some(route) = &route {
                    if let Some(remote_resp) = router
                        .add_drawer_remote(
                            wing.as_str(),
                            room.as_str(),
                            &content,
                            &source_file,
                            &added_by,
                            route,
                            DEFAULT_DUPLICATE_THRESHOLD,
                        )
                        .await?
                    {
                        return Ok(remote_resp);
                    }
                }
            }
        }

        let duplicates = self.find_duplicates(&content, DEFAULT_DUPLICATE_THRESHOLD).await?;
        if !duplicates.is_empty() {
            // ── Both-mode: same wing+room → retry, reuse local, retry remote ──
            if is_both {
                if let Some(existing) = duplicates.iter().find(|d| {
                    d.get("wing").and_then(|w| w.as_str()) == Some(wing.as_str())
                        && d.get("room").and_then(|r| r.as_str()) == Some(room.as_str())
                        && d.get("content_hash").and_then(|h| h.as_str())
                            == Some(content_hash.as_str())
                }) {
                    let existing_drawer_id = existing["id"].as_str().unwrap_or("");
                    let mut result = json!({
                        "success": true,
                        "drawer_id": existing_drawer_id,
                        "wing": wing,
                        "room": room,
                    });
                    if self.federation.is_some() {
                        if let Some(obj) = result.as_object_mut() {
                            obj.insert("applied_to".to_owned(), json!("local"));
                        }
                    }
                    if let Some(router) = &self.federation {
                        if let Some(route) = &route {
                            let replication = router
                                .add_drawer_replicate(
                                    wing.as_str(),
                                    room.as_str(),
                                    &content,
                                    &source_file,
                                    &added_by,
                                    route,
                                    DEFAULT_DUPLICATE_THRESHOLD,
                                )
                                .await;
                            if let Some(obj) = result.as_object_mut() {
                                obj.insert("replication".to_owned(), json!(replication));
                                if matches!(replication, ReplicationStatus::Failed { .. }) {
                                    obj.insert(
                                        "warnings".to_owned(),
                                        json!(["local content already existed; remote replication failed"]),
                                    );
                                }
                            }
                        }
                    }
                    return Ok(result);
                }
            }
            return Ok(json!({
                "success": false,
                "reason": "duplicate",
                "matches": duplicates,
            }));
        }

        let now = OffsetDateTime::now_utc();
        let drawer_id = generated_drawer_id("drawer", wing.as_str(), room.as_str(), &content, now)?;
        let content_clone = content.clone();
        let record = self
            .build_drawer_record(
                drawer_id.clone(),
                wing.clone(),
                room.clone(),
                None,
                None,
                source_file.clone(),
                added_by.clone(),
                "mcp".to_owned(),
                content,
                now,
            )
            .await?;

        self.storage
            .commit_ingest(IngestCommitRequest {
                ingest_kind: "mcp_write".to_owned(),
                source_key: format!("mcp:{}", drawer_id.as_str()),
                source_file: source_file.clone(),
                content_hash: record.content_hash.clone(),
                drawers: vec![record],
                duplicate_strategy: DuplicateStrategy::Error,
            })
            .await
            .map_tool()?;

        self.log_change(ChangeEvent {
            event_type: "drawer_added".to_owned(),
            occurred_at: now,
            entity_id: drawer_id.as_str().to_owned(),
            actor: Some(added_by.clone()),
            details_json: Some(json!({"wing": wing.as_str(), "room": room.as_str()}).to_string()),
        });

        let mut result = json!({
            "success": true,
            "drawer_id": drawer_id,
            "wing": wing,
            "room": room,
        });
        if self.federation.is_some() {
            if let Some(obj) = result.as_object_mut() {
                obj.insert("applied_to".to_owned(), json!("local"));
            }
        }

        // ── Both-mode: best-effort remote replication after local write ──
        if is_both {
            if let Some(router) = &self.federation {
                if let Some(route) = &route {
                    let replication = router
                        .add_drawer_replicate(
                            wing.as_str(),
                            room.as_str(),
                            &content_clone,
                            &source_file,
                            &added_by,
                            route,
                            DEFAULT_DUPLICATE_THRESHOLD,
                        )
                        .await;
                    if let Some(obj) = result.as_object_mut() {
                        obj.insert("replication".to_owned(), json!(replication));
                        if matches!(replication, ReplicationStatus::Failed { .. }) {
                            obj.insert(
                                "warnings".to_owned(),
                                json!(["local write succeeded but remote replication failed"]),
                            );
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    async fn tool_delete_drawer(&mut self, arguments: &Value) -> ToolResult<Value> {
        let drawer_id = parse_drawer_id(&required_string(arguments, "drawer_id")?)?;
        // Look the drawer up before deleting so its wing/room can be recorded
        // on the `drawer_deleted` change event below. There is no way to
        // recover them afterward, and a `drawer_deleted` event with no wing
        // is not merely uninformative: the federation server's `/v1/changes`
        // route fails closed on it for a scoped token (see
        // `change_event_visible` in `crates/mempalace-server/src/lib.rs` and
        // docs/Federation.md §1.5), so leaving it out here would make every
        // local deletion silently invisible to scoped remote readers.
        let existing = self.storage.drawer_store().get_drawer(&drawer_id).await.map_tool()?;
        let deleted = self
            .storage
            .drawer_store()
            .delete_drawers(std::slice::from_ref(&drawer_id))
            .await
            .map_tool()?;
        if deleted == 0 {
            // ── Federation fallback ──
            if let Some(router) = &self.federation {
                if let Some(remote_resp) = router.delete_drawer_remote(drawer_id.as_str()).await? {
                    // `existing` is almost always `None` here in practice —
                    // `deleted == 0` means this palace never had the row, so
                    // there was nothing to look up — but populate wing/room
                    // when we do happen to have a local record, alongside
                    // (not instead of) `origin`.
                    let mut details = json!({"origin": remote_resp["origin"]});
                    if let (Some(obj), Some(drawer)) = (details.as_object_mut(), &existing) {
                        obj.insert("wing".to_owned(), json!(drawer.wing.as_str()));
                        obj.insert("room".to_owned(), json!(drawer.room.as_str()));
                    }
                    self.log_change(ChangeEvent {
                        event_type: "drawer_deleted".to_owned(),
                        occurred_at: OffsetDateTime::now_utc(),
                        entity_id: drawer_id.as_str().to_owned(),
                        actor: None,
                        details_json: Some(details.to_string()),
                    });
                    return Ok(remote_resp);
                }
            }
            return Ok(json!({
                "success": false,
                "error": format!("Drawer not found: {}", drawer_id.as_str()),
            }));
        }
        // The LanceDB deletion has already succeeded. A SQLite cleanup failure
        // must not make this operation appear to have failed: callers would
        // retry against a missing drawer and could never remove the summary.
        if let Err(error) = self.storage.operational_store().delete_diary_summary(&drawer_id) {
            tracing::warn!(
                %drawer_id,
                %error,
                "failed to delete diary summary after drawer deletion"
            );
        }
        self.log_change(ChangeEvent {
            event_type: "drawer_deleted".to_owned(),
            occurred_at: OffsetDateTime::now_utc(),
            entity_id: drawer_id.as_str().to_owned(),
            actor: None,
            details_json: existing
                .as_ref()
                .map(|d| json!({"wing": d.wing.as_str(), "room": d.room.as_str()}).to_string()),
        });
        let mut result = json!({ "success": true, "drawer_id": drawer_id });
        if self.federation.is_some() {
            if let Some(obj) = result.as_object_mut() {
                obj.insert("applied_to".to_owned(), json!("local"));
            }
        }
        Ok(result)
    }

    async fn tool_diary_write(&mut self, arguments: &Value) -> ToolResult<Value> {
        let agent_name = required_string(arguments, "agent_name")?;
        let entry = required_string(arguments, "entry")?;
        let summary = required_string(arguments, "summary")?;
        validate_diary_summary(&summary)?;
        let topic = optional_string(arguments, "topic")?.unwrap_or_else(|| "general".to_owned());
        let scope = optional_string(arguments, "scope")?.unwrap_or_else(|| "agent".to_owned());
        let wing_name = diary_write_wing_name(&scope, optional_string(arguments, "wing")?)?;
        let wing = parse_wing_id(&wing_name)?;
        let stored_wing = wing.as_str().to_owned();
        let room = parse_room_id(DIARY_ROOM)?;
        let now = OffsetDateTime::now_utc();
        let drawer_id = generated_drawer_id("diary", wing.as_str(), room.as_str(), &entry, now)?;
        let source_file = format!("{DIARY_TOPIC_PREFIX}{topic}");
        let record = self
            .build_drawer_record(
                drawer_id.clone(),
                wing,
                room,
                Some(DIARY_HALL.to_owned()),
                Some(now.date()),
                source_file.clone(),
                agent_name.clone(),
                "diary".to_owned(),
                entry,
                now,
            )
            .await?;

        // Record the summary with its pending ingest state before exposing the
        // drawer, so a crash after the Lance write leaves a recoverable pending
        // run (not an orphaned summary) and a retry does not create a silently
        // divergent prior entry.
        self.storage
            .commit_diary_ingest(
                IngestCommitRequest {
                    ingest_kind: "diary".to_owned(),
                    source_key: format!("diary:{}", drawer_id.as_str()),
                    source_file,
                    content_hash: record.content_hash.clone(),
                    drawers: vec![record],
                    duplicate_strategy: DuplicateStrategy::Error,
                },
                &summary,
            )
            .await
            .map_tool()?;

        self.log_change(ChangeEvent {
            event_type: "diary_written".to_owned(),
            occurred_at: now,
            entity_id: drawer_id.as_str().to_owned(),
            actor: Some(agent_name.clone()),
            details_json: Some(
                json!({"topic": topic.clone(), "scope": scope.clone(), "wing": stored_wing.clone()})
                    .to_string(),
            ),
        });

        Ok(json!({
            "success": true,
            "entry_id": drawer_id,
            "agent": agent_name,
            "topic": topic,
            "scope": scope,
            "wing": stored_wing,
            "timestamp": format_rfc3339(now)?,
            "summary": summary,
        }))
    }

    async fn tool_diary_read(&mut self, arguments: &Value) -> ToolResult<Value> {
        let filters = DiaryReadFilters::from_arguments(arguments)?;
        self.diary_read_payload(filters).await
    }

    async fn diary_read_payload(&mut self, filters: DiaryReadFilters) -> ToolResult<Value> {
        if let Some(entry_id) = filters.entry_id.clone() {
            return self.diary_read_detail(entry_id, &filters).await;
        }

        let room = parse_room_id(DIARY_ROOM)?;
        let mut drawers = self
            .storage
            .drawer_store()
            .list_drawers(&DrawerFilter {
                wing: filters.wing.clone(),
                room: Some(room.clone()),
                ..DrawerFilter::default()
            })
            .await
            .map_tool()?;
        drawers.retain(|drawer| diary_entry_matches(drawer, &filters));

        if drawers.is_empty() {
            return Ok(json!({
                "scope": "all_wings",
                "agent": filters.agent_name.clone(),
                "wing": filters.wing.as_ref().map(|wing| wing.as_str()),
                "topic": filters.topic.clone(),
                "since": filters.since.map(format_rfc3339).transpose()?,
                "entries": [],
                "message": "No diary entries yet.",
            }));
        }

        drawers.sort_by(|left, right| right.filed_at.cmp(&left.filed_at));
        let total = drawers.len();
        let entries = drawers
            .into_iter()
            .take(filters.last_n)
            .map(|drawer| render_diary_entry(drawer, true, None))
            .collect::<ToolResult<Vec<_>>>()?;

        Ok(json!({
            "scope": "all_wings",
            "agent": filters.agent_name.clone(),
            "wing": filters.wing.as_ref().map(|wing| wing.as_str()),
            "topic": filters.topic.clone(),
            "since": filters.since.map(format_rfc3339).transpose()?,
            "entries": entries,
            "total": total,
            "showing": total.min(filters.last_n),
        }))
    }

    async fn diary_read_detail(
        &mut self,
        entry_id: DrawerId,
        filters: &DiaryReadFilters,
    ) -> ToolResult<Value> {
        let drawer = self.storage.drawer_store().get_drawer(&entry_id).await.map_tool()?;

        let Some(drawer) = drawer else {
            return Ok(json!({
                "entry_id": entry_id.as_str(),
                "message": "Diary entry not found.",
            }));
        };

        let diary_room = parse_room_id(DIARY_ROOM)?;
        if drawer.room != diary_room {
            return Ok(json!({
                "entry_id": entry_id.as_str(),
                "message": "Diary entry not found.",
            }));
        }

        if drawer.ingest_mode != "diary" {
            return Ok(json!({
                "entry_id": entry_id.as_str(),
                "message": "Entry is not a diary entry.",
            }));
        }

        if let Some(agent_name) = filters.agent_name.as_deref() {
            if drawer.added_by != agent_name {
                return Ok(json!({
                    "entry_id": entry_id.as_str(),
                    "message": "Diary entry not found for this agent.",
                }));
            }
        }
        if let Some(wing) = filters.wing.as_ref() {
            if drawer.wing != *wing {
                return Ok(json!({
                    "entry_id": entry_id.as_str(),
                    "message": "Diary entry not found for this wing.",
                }));
            }
        }
        if let Some(topic) = filters.topic.as_deref() {
            if diary_entry_topic(&drawer) != topic {
                return Ok(json!({
                    "entry_id": entry_id.as_str(),
                    "message": "Diary entry not found for this topic.",
                }));
            }
        }

        render_diary_entry(drawer, true, None)
    }

    async fn wake_up_diary_payload(
        &mut self,
        current_agent: Option<&str>,
        since: OffsetDateTime,
        minimum_entries: usize,
    ) -> ToolResult<Value> {
        let room = parse_room_id(DIARY_ROOM)?;
        let mut drawers = self
            .storage
            .drawer_store()
            .list_drawers(&DrawerFilter { room: Some(room), ..DrawerFilter::default() })
            .await
            .map_tool()?;
        drawers.retain(|drawer| drawer.ingest_mode == "diary");
        drawers.sort_by(|left, right| right.filed_at.cmp(&left.filed_at));

        let entries = drawers
            .into_iter()
            .scan(0usize, |shown, drawer| {
                if drawer.filed_at < since && *shown >= minimum_entries {
                    None
                } else {
                    *shown += 1;
                    Some(drawer)
                }
            })
            .collect::<Vec<_>>();

        // Bulk-fetch summaries for all entries in a single query.
        let ids: Vec<DrawerId> = entries.iter().map(|d| d.id.clone()).collect();
        let summaries: std::collections::HashMap<DrawerId, String> = self
            .storage
            .operational_store()
            .get_diary_summaries(&ids)
            .map_tool()?
            .into_iter()
            .collect();

        let entries = entries
            .into_iter()
            .map(|drawer| {
                let summary = summaries
                    .get(&drawer.id)
                    .cloned()
                    .unwrap_or_else(|| legacy_diary_summary(&drawer.content));
                render_diary_entry(drawer, true, Some(summary))
            })
            .collect::<ToolResult<Vec<_>>>()?;

        if entries.is_empty() {
            return Ok(json!({
                "scope": "all_wings",
                "current_agent": current_agent,
                "since": format_rfc3339(since)?,
                "minimum_entries": minimum_entries,
                "entries": [],
                "message": "No diary entries yet.",
            }));
        }

        let showing = entries.len();
        Ok(json!({
            "scope": "all_wings",
            "current_agent": current_agent,
            "since": format_rfc3339(since)?,
            "minimum_entries": minimum_entries,
            "entries": entries,
            "total": showing,
            "showing": showing,
        }))
    }

    async fn tool_traverse(&mut self, arguments: &Value) -> ToolResult<Value> {
        let start_room = required_string(arguments, "start_room")?;
        let max_hops = optional_usize(arguments, "max_hops")?.unwrap_or(2);
        let snapshot = self.graph_snapshot().await?;
        if !snapshot.nodes.contains_key(&start_room) {
            return Ok(json!({
                "error": format!("Room '{}' not found", start_room),
                "suggestions": fuzzy_match_rooms(&start_room, &snapshot),
            }));
        }
        Ok(json!(traverse_graph(&snapshot, &start_room, max_hops)))
    }

    async fn tool_find_tunnels(&mut self, arguments: &Value) -> ToolResult<Value> {
        let wing_a = optional_string(arguments, "wing_a")?
            .map(|value| parse_wing_id(&value).map(|wing| wing.as_str().to_owned()))
            .transpose()?;
        let wing_b = optional_string(arguments, "wing_b")?
            .map(|value| parse_wing_id(&value).map(|wing| wing.as_str().to_owned()))
            .transpose()?;
        let snapshot = self.graph_snapshot().await?;
        Ok(json!(find_tunnels(&snapshot, wing_a.as_deref(), wing_b.as_deref())))
    }

    async fn tool_graph_stats(&mut self) -> ToolResult<Value> {
        let snapshot = self.graph_snapshot().await?;
        Ok(serde_json::to_value(snapshot.stats).map_tool_internal()?)
    }

    async fn tool_kg_query(&mut self, arguments: &Value) -> ToolResult<Value> {
        let entity = required_string(arguments, "entity")?;
        let as_of =
            optional_string(arguments, "as_of")?.map(|value| parse_date(&value)).transpose()?;
        let direction =
            parse_direction(optional_string(arguments, "direction")?.as_deref().unwrap_or("both"))?;
        let runtime = KnowledgeGraphRuntime::new(self.storage.operational_store());
        let route = self.federation.as_ref().map(|router| router.resolve_kg_route());
        let federated_read = route.as_ref().is_some_and(|route| route.mode != RouteMode::Local);
        let local_read = route.as_ref().map_or(true, |route| route.mode != RouteMode::Remote);
        let facts = if local_read {
            match runtime.query_entity(&entity, as_of, direction) {
                Ok(facts) => facts,
                // A federated query may name an entity that exists only on the remote.
                // Keep the local side empty so federation can still complete the read.
                Err(mempalace_graph::GraphError::UnknownEntity { .. }) if federated_read => {
                    Vec::new()
                }
                Err(error) => return Err(ToolError::Internal(McpError::Graph(error))),
            }
        } else {
            Vec::new()
        };
        let count = facts.len();
        let mut payload = json!({
            "entity": entity,
            "as_of": optional_string(arguments, "as_of")?,
            "facts": facts,
            "count": count,
        });
        // ── Federation path ──
        if let (Some(router), Some(route)) = (&self.federation, route) {
            if route.mode != RouteMode::Local {
                payload = router.kg_query_merge(payload, &entity, &route).await?;
            }
        }
        Ok(payload)
    }

    async fn tool_kg_add(&mut self, arguments: &Value) -> ToolResult<Value> {
        let subject = required_string(arguments, "subject")?;
        let predicate = required_string(arguments, "predicate")?;
        let object = required_string(arguments, "object")?;
        let valid_from_text = optional_string(arguments, "valid_from")?;

        // ── Resolve federation route once, reuse for dual-write decisions ──
        let route = self.federation.as_ref().map(|router| router.resolve_kg_route());
        let is_both = match (&self.federation, &route) {
            (Some(router), Some(route)) => router.is_dual_write(route),
            _ => false,
        };

        // ── Non-Both federation: remote-only or local-only ──
        if !is_both {
            if let Some(router) = &self.federation {
                if let Some(route) = &route {
                    if let Some(remote_resp) = router
                        .kg_add_remote(
                            &subject,
                            &predicate,
                            &object,
                            valid_from_text.as_deref(),
                            route,
                        )
                        .await?
                    {
                        return Ok(remote_resp);
                    }
                }
            }
        }

        let valid_from = valid_from_text.as_deref().map(parse_date).transpose()?;
        let source_closet = optional_string(arguments, "source_closet")?;
        let source_drawer_id =
            source_closet.as_deref().and_then(|value| parse_drawer_id(value).ok());
        let runtime = KnowledgeGraphRuntime::new(self.storage.operational_store());
        let now = OffsetDateTime::now_utc();
        let triple_id = runtime
            .add_fact(
                AddFactRequest {
                    subject: subject.clone(),
                    subject_type: infer_entity_kind(&subject),
                    predicate: predicate.clone(),
                    object_type: infer_entity_kind(&object),
                    object: object.clone(),
                    valid_from,
                    valid_to: None,
                    confidence: 1.0,
                    source_drawer_id,
                    source_file: source_closet,
                },
                now,
            )
            .map_tool_internal()?;

        let sub = subject.clone();
        let pred = predicate.clone();
        let obj = object.clone();
        self.log_change(ChangeEvent {
            event_type: "kg_fact_added".to_owned(),
            occurred_at: now,
            entity_id: triple_id.clone(),
            actor: None,
            details_json: Some(
                json!({"subject": subject, "predicate": predicate, "object": object}).to_string(),
            ),
        });

        let mut payload = json!({
            "success": true,
            "triple_id": triple_id,
            "fact": format!("{sub} → {pred} → {obj}"),
        });
        if self.federation.is_some() {
            if let Some(obj) = payload.as_object_mut() {
                obj.insert("applied_to".to_owned(), json!("local"));
            }
        }

        // ── Both-mode: best-effort remote replication after local KG add ──
        if is_both {
            if let Some(router) = &self.federation {
                if let Some(route) = &route {
                    let replication = router
                        .kg_add_replicate(&sub, &pred, &obj, valid_from_text.as_deref(), route)
                        .await;
                    if let Some(p) = payload.as_object_mut() {
                        p.insert("replication".to_owned(), json!(replication));
                        if matches!(replication, ReplicationStatus::Failed { .. }) {
                            p.insert(
                                "warnings".to_owned(),
                                json!(["local write succeeded but remote replication failed"]),
                            );
                        }
                    }
                }
            }
        }

        Ok(payload)
    }

    /// Knowledge-graph invalidation.
    ///
    /// **Federation policy:** Follows `resolve_kg_route()` — same as all other KG
    /// tools. In Local mode the invalidation applies only to the local KG; in
    /// Remote mode only to the remote; in Combined mode to both. The `ended` date
    /// is applied uniformly across all targeted palaces.
    async fn tool_kg_invalidate(&mut self, arguments: &Value) -> ToolResult<Value> {
        let subject = required_string(arguments, "subject")?;
        let predicate = required_string(arguments, "predicate")?;
        let object = required_string(arguments, "object")?;
        let ended_text = optional_string(arguments, "ended")?;
        let ended = ended_text
            .as_deref()
            .map(parse_date)
            .transpose()?
            .unwrap_or_else(|| OffsetDateTime::now_utc().date());

        // ── Resolve federation route once, reuse for dual-write decisions ──
        let route = self.federation.as_ref().map(|router| router.resolve_kg_route());
        let is_both = match (&self.federation, &route) {
            (Some(router), Some(route)) => router.is_dual_write(route),
            _ => false,
        };

        // ── Non-Both federation: remote-only or local-only ──
        if !is_both {
            if let Some(router) = &self.federation {
                if let Some(route) = &route {
                    if let Some(remote_resp) = router
                        .kg_invalidate_remote(
                            &subject,
                            &predicate,
                            &object,
                            ended_text.as_deref(),
                            route,
                        )
                        .await?
                    {
                        return Ok(remote_resp);
                    }
                }
            }
        }

        let now = OffsetDateTime::now_utc();
        let runtime = KnowledgeGraphRuntime::new(self.storage.operational_store());
        let invalidated =
            runtime.invalidate(&subject, &predicate, &object, ended, now).map_tool_internal()?;

        let sub = subject.clone();
        let pred = predicate.clone();
        let obj = object.clone();

        if invalidated > 0 {
            self.log_change(ChangeEvent {
                event_type: "kg_fact_invalidated".to_owned(),
                occurred_at: now,
                entity_id: format!("{subject} → {predicate} → {object}"),
                actor: None,
                details_json: Some(
                    json!({"subject": subject, "predicate": predicate, "object": object,
                           "ended": format_date(ended)})
                    .to_string(),
                ),
            });
        }

        let mut payload = json!({
            "success": invalidated > 0,
            "invalidated": invalidated,
            "fact": format!("{sub} → {pred} → {obj}"),
            "ended": ended_text.as_deref().unwrap_or("today"),
        });
        if self.federation.is_some() {
            if let Some(obj) = payload.as_object_mut() {
                obj.insert("applied_to".to_owned(), json!("local"));
            }
        }

        // ── Both-mode: best-effort remote replication after local KG invalidation ──
        if is_both {
            if let Some(router) = &self.federation {
                if let Some(route) = &route {
                    let replication = router
                        .kg_invalidate_replicate(&sub, &pred, &obj, ended_text.as_deref(), route)
                        .await;
                    if let Some(p) = payload.as_object_mut() {
                        p.insert("replication".to_owned(), json!(replication));
                        if matches!(replication, ReplicationStatus::Failed { .. }) {
                            p.insert(
                                "warnings".to_owned(),
                                json!(["local write succeeded but remote replication failed"]),
                            );
                        }
                    }
                }
            }
        }

        Ok(payload)
    }

    async fn tool_kg_timeline(&mut self, arguments: &Value) -> ToolResult<Value> {
        let entity = optional_string(arguments, "entity")?;
        let runtime = KnowledgeGraphRuntime::new(self.storage.operational_store());
        let route = self.federation.as_ref().map(|router| router.resolve_kg_route());
        let federated_read = route.as_ref().is_some_and(|route| route.mode != RouteMode::Local);
        let local_read = route.as_ref().map_or(true, |route| route.mode != RouteMode::Remote);
        let timeline = if local_read {
            match runtime.timeline(entity.as_deref()) {
                Ok(timeline) => timeline,
                // See `tool_kg_query`: an entity can be known only by a remote.
                Err(mempalace_graph::GraphError::UnknownEntity { .. }) if federated_read => {
                    Vec::new()
                }
                Err(error) => return Err(ToolError::Internal(McpError::Graph(error))),
            }
        } else {
            Vec::new()
        };
        let count = timeline.len();
        let mut payload = json!({
            "entity": entity.clone().unwrap_or_else(|| "all".to_owned()),
            "timeline": timeline,
            "count": count,
        });
        // ── Federation path ──
        if let (Some(router), Some(route)) = (&self.federation, route) {
            if route.mode != RouteMode::Local {
                payload = router.kg_timeline_merge(payload, entity.as_deref(), &route).await?;
            }
        }
        Ok(payload)
    }

    async fn tool_kg_stats(&mut self) -> ToolResult<Value> {
        let runtime = KnowledgeGraphRuntime::new(self.storage.operational_store());
        let mut payload =
            serde_json::to_value(runtime.stats().map_tool_internal()?).map_tool_internal()?;
        // ── Federation path ──
        if let Some(router) = &self.federation {
            let route = router.resolve_kg_route();
            if route.mode != RouteMode::Local {
                payload = router.kg_stats_merge(payload, &route).await?;
            }
        }
        Ok(payload)
    }

    async fn list_all_drawers(&self) -> ToolResult<Vec<DrawerRecord>> {
        self.storage.drawer_store().list_drawers(&DrawerFilter::default()).await.map_tool()
    }

    async fn graph_snapshot(&self) -> ToolResult<PalaceGraphSnapshot> {
        derive_palace_graph_from_store(self.storage.drawer_store()).await.map_tool_internal()
    }

    async fn find_duplicates(&mut self, content: &str, threshold: f32) -> ToolResult<Vec<Value>> {
        // Duplicate prevention is a write-path correctness check, so keep a fixed semantic
        // search window instead of applying low-CPU UX caps or rerank score blending.
        let query = SearchQuery {
            text: content.to_owned(),
            wing: None,
            room: None,
            limit: DUPLICATE_SEARCH_LIMIT,
            profile: self.config.embedding_profile,
            view: None,
        };
        let results =
            self.search.search_semantic(self.storage.drawer_store(), &query).await.map_tool()?;
        Ok(results
            .into_iter()
            .filter(|result| result.score >= threshold)
            .map(|result| {
                let snippet = if result.content.chars().count() > 200 {
                    format!("{}...", result.content.chars().take(200).collect::<String>())
                } else {
                    result.content
                };
                json!({
                    "id": result.drawer_id,
                    "wing": result.wing,
                    "room": result.room,
                    "similarity": round_similarity(result.score),
                    "content": snippet,
                    "content_hash": result.content_hash,
                })
            })
            .collect())
    }

    async fn build_drawer_record(
        &mut self,
        id: DrawerId,
        wing: WingId,
        room: RoomId,
        hall: Option<String>,
        date: Option<Date>,
        source_file: String,
        added_by: String,
        ingest_mode: String,
        content: String,
        filed_at: OffsetDateTime,
    ) -> ToolResult<DrawerRecord> {
        let request = EmbeddingRequest::new(vec![content.clone()]).map_tool_internal()?;
        let response = self.search.provider_mut().embed(&request).map_tool_internal()?;
        let embedding = response.vectors().first().cloned().ok_or_else(|| {
            ToolError::Internal(McpError::Embeddings(EmbeddingError::ProviderContract(
                "provider returned no vector for single-drawer ingest".to_owned(),
            )))
        })?;
        Ok(DrawerRecord {
            id,
            wing,
            room,
            hall,
            date,
            source_file,
            chunk_index: 0,
            ingest_mode,
            extract_mode: None,
            added_by,
            filed_at,
            importance: None,
            emotional_weight: None,
            weight: None,
            content: content.clone(),
            content_hash: hash_text(&content),
            embedding,
            locator: None,
            view_metadata: None,
        })
    }

    fn log_change(&self, event: ChangeEvent) {
        let _ = self.storage.operational_store().append_event(&event);
    }

    fn identity_path(&self) -> PathBuf {
        self.config
            .palace_path
            .parent()
            .map(|parent| parent.join("identity.txt"))
            .unwrap_or_else(|| PathBuf::from("identity.txt"))
    }

    fn read_identity_text(&self) -> ToolResult<String> {
        let path = self.identity_path();
        match fs::read_to_string(&path) {
            Ok(text) => Ok(text.trim().to_owned()),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                Ok(format!("## L0 — IDENTITY\nNo identity configured. Create {}", path.display()))
            }
            Err(source) => Err(ToolError::Internal(McpError::Io { path, source })),
        }
    }

    async fn tool_get_changes_since(&mut self, arguments: &Value) -> ToolResult<Value> {
        let since_str = optional_string(arguments, "since")?;
        let since = since_str
            .as_deref()
            .map(|s| {
                OffsetDateTime::parse(s, &Rfc3339).map_err(|_| {
                    ToolError::InvalidParams(format!(
                        "invalid `since` timestamp `{s}`; expected ISO 8601 e.g. 2026-05-08T10:00:00Z"
                    ))
                })
            })
            .transpose()?
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);
        let limit = optional_usize(arguments, "limit")?.unwrap_or(50);

        // Parse optional `cursors` object: maps remote name → opaque cursor string.
        // When federation is None the field is silently ignored (consistent with
        // other federation-only args across the tool suite).
        let cursors: BTreeMap<String, String> = match arguments.get("cursors") {
            None | Some(Value::Null) => BTreeMap::new(),
            Some(Value::Object(map)) => {
                let mut out = BTreeMap::new();
                for (k, v) in map {
                    match v.as_str() {
                        Some(s) => {
                            out.insert(k.clone(), s.to_owned());
                        }
                        None => {
                            return Err(ToolError::InvalidParams(
                                "field `cursors` must be an object of string values".to_owned(),
                            ));
                        }
                    }
                }
                out
            }
            Some(_) => {
                return Err(ToolError::InvalidParams(
                    "field `cursors` must be an object of string values".to_owned(),
                ));
            }
        };

        let events =
            self.storage.operational_store().get_changes_since(since, limit).map_tool_internal()?;

        if let Some(router) = self.federation.as_ref().filter(|r| r.has_remotes()) {
            // Annotate local events with origin.
            let local_event_list: Vec<Value> = render_change_events(events)?
                .into_iter()
                .map(|mut v| {
                    if let Some(obj) = v.as_object_mut() {
                        obj.insert("origin".to_owned(), json!("local"));
                    }
                    v
                })
                .collect();

            let remote_results =
                router.changes_fanout(since_str.clone(), Some(limit), &cursors).await;

            // Merge all events and collect per-remote metadata.
            let mut all_events: Vec<Value> = local_event_list;
            let mut remotes_meta = serde_json::Map::new();

            for (name, result) in &remote_results {
                if result.get("unreachable") == Some(&json!(true)) {
                    remotes_meta.insert(name.clone(), result.clone());
                } else {
                    let remote_events = result["events"].as_array().cloned().unwrap_or_default();
                    let event_count = remote_events.len();
                    all_events.extend(remote_events);
                    remotes_meta.insert(
                        name.clone(),
                        json!({
                            "next_cursor": result["next_cursor"],
                            "count": event_count,
                        }),
                    );
                }
            }

            // Sort combined events by occurred_at ascending (best-effort
            // display order only — cross-machine clocks may skew). Parse the
            // timestamps so differing UTC offsets or subsecond precision
            // across remotes still compare chronologically; unparseable
            // timestamps sort last, by raw string among themselves.
            let parse_occurred_at = |event: &Value| {
                event["occurred_at"].as_str().and_then(|s| OffsetDateTime::parse(s, &Rfc3339).ok())
            };
            all_events.sort_by(|a, b| match (parse_occurred_at(a), parse_occurred_at(b)) {
                (Some(ta), Some(tb)) => ta.cmp(&tb),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => {
                    let sa = a["occurred_at"].as_str().unwrap_or("");
                    let sb = b["occurred_at"].as_str().unwrap_or("");
                    sa.cmp(sb)
                }
            });

            let count = all_events.len();
            Ok(json!({
                "since": since_str.unwrap_or_else(|| "epoch".to_owned()),
                "count": count,
                "events": all_events,
                "remotes": Value::Object(remotes_meta),
            }))
        } else {
            let count = events.len();
            let event_list = render_change_events(events)?;
            Ok(json!({
                "since": since_str.unwrap_or_else(|| "epoch".to_owned()),
                "count": count,
                "events": event_list,
            }))
        }
    }

    /// Create a task. The only coordination write with an explicit `wing` at request time, so
    /// it is the only one routed via `resolve_coordination_route`/`resolve_write_target`
    /// (mirroring `kg_add_remote`) rather than the local-first ID-discovery fallback every
    /// other coordination tool below uses — see the `FederationRouter` "Coordination" section
    /// comment in `federation.rs` for why. `write` can only ever resolve to `Local` or
    /// `Remote` here, never `Both` (rejected at config load).
    async fn tool_task_create(&mut self, arguments: &Value) -> ToolResult<Value> {
        let mut input: NewTask = parse_coordination_input(arguments)?;
        // Normalise the wing once, up front, and use that canonical value for BOTH the routing
        // decision and the outgoing request (local or remote). `resolve_coordination_route` is
        // keyed on the raw string it is given; routing on the un-normalised caller input would
        // let a short form like `"agents"` slip past the `wing_agents` diary hard-override and
        // past an operator's explicit `federation.coordination["wing_x"]` pin (see the security
        // fix notes in `docs/Federation.md`). Layer 2 (`resolve_coordination_route` itself) also
        // normalises defensively, but this is the fix that matters: it is also what stops the
        // raw wing from ever reaching the wire.
        let wing = parse_wing_id(&input.wing)?;
        input.wing = wing.as_str().to_owned();
        if let Some(router) = &self.federation {
            let route = router.resolve_coordination_route(input.wing.as_str());
            if router.resolve_write_target(&route) == WriteTarget::Remote {
                let remote_name = route.remote.clone().unwrap_or_else(|| "remote".to_owned());
                let mut req: WireNewTaskRequest = serde_json::from_value(arguments.clone())
                    .map_err(|e| ToolError::InvalidParams(e.to_string()))?;
                req.wing = input.wing.clone();
                return router.coordination_task_create_remote(&remote_name, req).await;
            }
        }
        Ok(json!(self.coordination.create_task(&input).map_tool_internal()?))
    }
    /// Get a task by exact ID. Local first; on a local miss, falls back to each configured
    /// remote in name order (no wing is known for an ID-keyed lookup — see the federation
    /// module comment).
    async fn tool_task_get(&mut self, arguments: &Value) -> ToolResult<Value> {
        let id = required_string(arguments, "task_id")?;
        if let Some(task) = self.coordination.get_task(&id).map_tool_internal()? {
            return Ok(json!({"found": true, "value": task}));
        }
        if let Some(router) = &self.federation {
            if let Some(value) = router.coordination_task_get_fallback(&id).await? {
                return Ok(json!({"found": true, "value": value}));
            }
        }
        Ok(json!({"found": false}))
    }
    /// Claim a task, or reclaim an expired lease. Local first; a local "task not found" falls
    /// back to each configured remote in name order, sending the claim to whichever one
    /// actually owns the task. A revision conflict — local or remote — surfaces via
    /// `revision_conflict_payload` either way; MemPalace never retries on the caller's behalf.
    async fn tool_task_claim(&mut self, arguments: &Value) -> ToolResult<Value> {
        let expected_revision = required_i64(arguments, "expected_revision")?;
        let task_id = required_string(arguments, "task_id")?;
        let worker = required_string(arguments, "worker")?;
        let lease_seconds = required_positive_i64(arguments, "lease_seconds")?;
        match self.coordination.claim_task(
            &task_id,
            &worker,
            expected_revision,
            Duration::seconds(lease_seconds),
        ) {
            Ok(RevisionedWrite::Applied(task)) => Ok(json!({"success": true, "task": task})),
            Ok(RevisionedWrite::Conflict { actual_revision }) => {
                Ok(revision_conflict_payload(expected_revision, actual_revision))
            }
            Err(err) if is_local_record_missing(&err) => {
                if let Some(router) = &self.federation {
                    let req = TaskLeaseRequest {
                        expected_revision,
                        lease_seconds,
                        worker: Some(worker),
                    };
                    if let Some(value) =
                        router.coordination_task_claim_fallback(&task_id, req).await?
                    {
                        return Ok(value);
                    }
                }
                Err(err).map_tool_internal()
            }
            Err(err) => Err(err).map_tool_internal(),
        }
    }
    /// Renew a live lease. See [`Self::tool_task_claim`] for the local-first/fallback and
    /// conflict-shape notes — identical here.
    async fn tool_task_renew(&mut self, arguments: &Value) -> ToolResult<Value> {
        let expected_revision = required_i64(arguments, "expected_revision")?;
        let task_id = required_string(arguments, "task_id")?;
        let worker = required_string(arguments, "worker")?;
        let lease_seconds = required_positive_i64(arguments, "lease_seconds")?;
        match self.coordination.renew_lease(
            &task_id,
            &worker,
            expected_revision,
            Duration::seconds(lease_seconds),
        ) {
            Ok(RevisionedWrite::Applied(task)) => Ok(json!({"success": true, "task": task})),
            Ok(RevisionedWrite::Conflict { actual_revision }) => {
                Ok(revision_conflict_payload(expected_revision, actual_revision))
            }
            Err(err) if is_local_record_missing(&err) => {
                if let Some(router) = &self.federation {
                    let req = TaskLeaseRequest {
                        expected_revision,
                        lease_seconds,
                        worker: Some(worker),
                    };
                    if let Some(value) =
                        router.coordination_task_renew_fallback(&task_id, req).await?
                    {
                        return Ok(value);
                    }
                }
                Err(err).map_tool_internal()
            }
            Err(err) => Err(err).map_tool_internal(),
        }
    }
    /// Transition a task's lifecycle state. See [`Self::tool_task_claim`] for the
    /// local-first/fallback and conflict-shape notes — identical here.
    async fn tool_task_transition(&mut self, arguments: &Value) -> ToolResult<Value> {
        let state: TaskState = serde_json::from_value(json!(required_string(arguments, "state")?))
            .map_err(|e| ToolError::InvalidParams(e.to_string()))?;
        let expected_revision = required_i64(arguments, "expected_revision")?;
        let task_id = required_string(arguments, "task_id")?;
        let actor = required_string(arguments, "actor")?;
        let details = arguments.get("details").cloned();
        match self.coordination.transition_task(
            &task_id,
            &actor,
            expected_revision,
            state,
            details.clone(),
        ) {
            Ok(RevisionedWrite::Applied(task)) => Ok(json!({"success": true, "task": task})),
            Ok(RevisionedWrite::Conflict { actual_revision }) => {
                Ok(revision_conflict_payload(expected_revision, actual_revision))
            }
            Err(err) if is_local_record_missing(&err) => {
                if let Some(router) = &self.federation {
                    let req = TransitionTaskRequest {
                        expected_revision,
                        state: wire_task_state(state),
                        actor: Some(actor),
                        details,
                    };
                    if let Some(value) =
                        router.coordination_task_transition_fallback(&task_id, req).await?
                    {
                        return Ok(value);
                    }
                }
                Err(err).map_tool_internal()
            }
            Err(err) => Err(err).map_tool_internal(),
        }
    }
    /// Send an addressed message. Local first; a local "task not found" falls back to each
    /// configured remote in name order, sending to whichever one owns the referenced task.
    async fn tool_message_send(&mut self, arguments: &Value) -> ToolResult<Value> {
        let input: NewMessage = parse_coordination_input(arguments)?;
        match self.coordination.send_message(&input) {
            Ok(message) => Ok(json!(message)),
            Err(err) if is_local_record_missing(&err) => {
                if let Some(router) = &self.federation {
                    let req: WireNewMessageRequest = serde_json::from_value(arguments.clone())
                        .map_err(|e| ToolError::InvalidParams(e.to_string()))?;
                    if let Some(value) = router.coordination_message_send_fallback(req).await? {
                        return Ok(value);
                    }
                }
                Err(err).map_tool_internal()
            }
            Err(err) => Err(err).map_tool_internal(),
        }
    }
    /// Get a message by exact ID. Local first, then falls back across remotes in name order.
    async fn tool_message_get(&mut self, arguments: &Value) -> ToolResult<Value> {
        let id = required_string(arguments, "message_id")?;
        if let Some(message) = self.coordination.get_message(&id).map_tool_internal()? {
            return Ok(json!({"found": true, "value": message}));
        }
        if let Some(router) = &self.federation {
            if let Some(value) = router.coordination_message_get_fallback(&id).await? {
                return Ok(json!({"found": true, "value": value}));
            }
        }
        Ok(json!({"found": false}))
    }
    /// Acknowledge a message. Local first; a local "message not found" falls back to each
    /// configured remote in name order.
    async fn tool_message_acknowledge(&mut self, arguments: &Value) -> ToolResult<Value> {
        let message_id = required_string(arguments, "message_id")?;
        let actor = required_string(arguments, "actor")?;
        match self.coordination.acknowledge_message(&message_id, &actor) {
            Ok(message) => Ok(json!(message)),
            Err(err) if is_local_record_missing(&err) => {
                if let Some(router) = &self.federation {
                    let req = AckMessageRequest { actor: Some(actor) };
                    if let Some(value) =
                        router.coordination_message_ack_fallback(&message_id, req).await?
                    {
                        return Ok(value);
                    }
                }
                Err(err).map_tool_internal()
            }
            Err(err) => Err(err).map_tool_internal(),
        }
    }
    /// Read an addressed inbox. This is an aggregate, cursor-paginated feed, not an exact-ID
    /// lookup — like `mempalace_get_changes_since`, it always reads local *and* (when
    /// federation has remotes configured) fans out to every remote concurrently with a
    /// per-remote cursor, rather than routing by a single wing's resolved rule. The local page
    /// is returned as-is; remote pages are reported under `remote_messages`, keyed by remote
    /// name, in the same `{unreachable, error}` isolation shape `changes_fanout` uses.
    async fn tool_inbox_read(&mut self, arguments: &Value) -> ToolResult<Value> {
        let wing = optional_non_blank_string(arguments, "wing")?;
        let recipient = required_string(arguments, "recipient")?;
        let limit = optional_usize(arguments, "limit")?.unwrap_or(50);
        let unacknowledged_only = optional_bool(arguments, "unacknowledged_only")?.unwrap_or(false);
        let page = self
            .coordination
            .inbox(
                &recipient,
                optional_i64(arguments, "cursor")?.map(CoordinationCursor),
                wing.as_deref(),
                limit,
                unacknowledged_only,
                // The MCP surface has no HTTP caller identity to scope against — it is the
                // local agent talking to its own palace — so it is always fully trusted,
                // diary included. See `CoordinationVisibility::Trusted`'s doc comment: this
                // variant must never be used for an HTTP-authenticated caller.
                CoordinationVisibility::Trusted,
            )
            .map_tool_internal()?;
        let mut payload = json!(page);
        if let Some(router) = self.federation.as_ref().filter(|r| r.has_remotes()) {
            let cursors = parse_cursors_arg(arguments, "remote_cursors")?;
            let remote_messages = router
                .coordination_inbox_fanout(recipient, wing, Some(limit), unacknowledged_only, &cursors)
                .await;
            payload["remote_messages"] = json!(remote_messages);
        }
        Ok(payload)
    }
    /// Store an immutable artifact. Local first; a local "task not found" falls back to each
    /// configured remote in name order.
    async fn tool_artifact_put(&mut self, arguments: &Value) -> ToolResult<Value> {
        let input: NewArtifact = parse_coordination_input(arguments)?;
        match self.coordination.put_artifact(&input) {
            Ok(artifact) => Ok(json!(artifact)),
            Err(err) if is_local_record_missing(&err) => {
                if let Some(router) = &self.federation {
                    let req: WireNewArtifactRequest = serde_json::from_value(arguments.clone())
                        .map_err(|e| ToolError::InvalidParams(e.to_string()))?;
                    if let Some(value) = router.coordination_artifact_put_fallback(req).await? {
                        return Ok(value);
                    }
                }
                Err(err).map_tool_internal()
            }
            Err(err) => Err(err).map_tool_internal(),
        }
    }
    /// Get an artifact by exact ID. Local first, then falls back across remotes in name order.
    async fn tool_artifact_get(&mut self, arguments: &Value) -> ToolResult<Value> {
        let id = required_string(arguments, "artifact_id")?;
        if let Some(artifact) = self.coordination.get_artifact(&id).map_tool_internal()? {
            return Ok(json!({"found": true, "value": artifact}));
        }
        if let Some(router) = &self.federation {
            if let Some(value) = router.coordination_artifact_get_fallback(&id).await? {
                return Ok(json!({"found": true, "value": value}));
            }
        }
        Ok(json!({"found": false}))
    }
    /// Store an immutable task result. Local first; a local "task not found" falls back to
    /// each configured remote in name order.
    async fn tool_result_put(&mut self, arguments: &Value) -> ToolResult<Value> {
        let input: NewTaskResult = parse_coordination_input(arguments)?;
        match self.coordination.put_result(&input) {
            Ok(result) => Ok(json!(result)),
            Err(err) if is_local_record_missing(&err) => {
                if let Some(router) = &self.federation {
                    let req: WireNewTaskResultRequest = serde_json::from_value(arguments.clone())
                        .map_err(|e| ToolError::InvalidParams(e.to_string()))?;
                    if let Some(value) = router.coordination_result_put_fallback(req).await? {
                        return Ok(value);
                    }
                }
                Err(err).map_tool_internal()
            }
            Err(err) => Err(err).map_tool_internal(),
        }
    }
    /// Get a task result by exact ID. Local first, then falls back across remotes in name
    /// order.
    async fn tool_result_get(&mut self, arguments: &Value) -> ToolResult<Value> {
        let id = required_string(arguments, "result_id")?;
        if let Some(result) = self.coordination.get_result(&id).map_tool_internal()? {
            return Ok(json!({"found": true, "value": result}));
        }
        if let Some(router) = &self.federation {
            if let Some(value) = router.coordination_result_get_fallback(&id).await? {
                return Ok(json!({"found": true, "value": value}));
            }
        }
        Ok(json!({"found": false}))
    }
    /// Get a single audit event by exact ID. Stays local-only: unlike every other coordination
    /// route, Stage 3 never exposed `GET /v1/coordination/events/{id}` on the wire — only the
    /// paginated `GET /v1/coordination/events` feed — so there is no remote counterpart to fall
    /// back to. See `ToolName::routing()`, which categorizes this tool `LocalOnly` even though
    /// its sibling `mempalace_coordination_events` is `RoutableCoordination`.
    async fn tool_coordination_event_get(&self, arguments: &Value) -> ToolResult<Value> {
        exact_result(
            self.coordination
                .get_event(&required_string(arguments, "event_id")?)
                .map_tool_internal()?,
        )
    }
    /// Read the coordination audit-event feed. Aggregate and cursor-paginated, like
    /// `mempalace_inbox_read` — always reads local and fans out to every configured remote
    /// concurrently with a per-remote cursor, reported under `remote_events`.
    async fn tool_coordination_events(&mut self, arguments: &Value) -> ToolResult<Value> {
        let wing = optional_non_blank_string(arguments, "wing")?;
        let task_id = optional_string(arguments, "task_id")?;
        let limit = optional_usize(arguments, "limit")?.unwrap_or(50);
        let page = self
            .coordination
            .events(
                optional_i64(arguments, "cursor")?.map(CoordinationCursor),
                task_id.as_deref(),
                wing.as_deref(),
                limit,
                // Fully trusted local caller — see the identical note in `tool_inbox_read`.
                CoordinationVisibility::Trusted,
            )
            .map_tool_internal()?;
        let mut payload = json!(page);
        if let Some(router) = self.federation.as_ref().filter(|r| r.has_remotes()) {
            let cursors = parse_cursors_arg(arguments, "remote_cursors")?;
            let remote_events =
                router.coordination_events_fanout(task_id, wing, Some(limit), &cursors).await;
            payload["remote_events"] = json!(remote_events);
        }
        Ok(payload)
    }

    async fn tool_skill_propose(&self, arguments: &Value) -> ToolResult<Value> {
        let input: NewSkill = parse_coordination_input(arguments)?;
        Ok(json!(self.skills.propose_skill(&input).map_tool_internal()?))
    }
    async fn tool_skill_get(&self, arguments: &Value) -> ToolResult<Value> {
        exact_result(
            self.skills
                .get_skill(
                    &required_string(arguments, "skill_id")?,
                    required_i64(arguments, "version")?,
                )
                .map_tool_internal()?,
        )
    }
    async fn tool_skill_versions(&self, arguments: &Value) -> ToolResult<Value> {
        Ok(json!(
            self.skills
                .list_skill_versions(&required_string(arguments, "skill_id")?)
                .map_tool_internal()?
        ))
    }
    async fn tool_skill_list(&self, arguments: &Value) -> ToolResult<Value> {
        let scope = optional_string(arguments, "scope")?
            .map(|value| serde_json::from_value::<SkillScope>(json!(value)))
            .transpose()
            .map_err(|error| ToolError::InvalidParams(error.to_string()))?;
        let status = optional_string(arguments, "status")?
            .map(|value| serde_json::from_value::<SkillStatus>(json!(value)))
            .transpose()
            .map_err(|error| ToolError::InvalidParams(error.to_string()))?;
        let wing = optional_non_blank_string(arguments, "wing")?
            .map(|value| parse_wing_id(&value))
            .transpose()?;
        Ok(json!(
            self.skills
                .list_skills(
                    scope,
                    status,
                    wing.as_ref().map(|wing| wing.as_str()),
                    optional_usize(arguments, "limit")?.unwrap_or(50),
                )
                .map_tool_internal()?
        ))
    }
    async fn tool_skill_record_outcome(&self, arguments: &Value) -> ToolResult<Value> {
        let input: NewSkillOutcome = parse_coordination_input(arguments)?;
        Ok(json!(self.skills.record_outcome(&input).map_tool_internal()?))
    }
    async fn tool_skill_promote(&self, arguments: &Value) -> ToolResult<Value> {
        let expected_revision = required_i64(arguments, "expected_revision")?;
        let result = self
            .skills
            .promote_skill(
                &required_string(arguments, "skill_id")?,
                required_i64(arguments, "version")?,
                expected_revision,
                &required_string(arguments, "reviewer")?,
                &required_string(arguments, "reason")?,
            )
            .map_tool_internal()?;
        Ok(match result {
            RevisionedWrite::Applied(skill) => json!({"success": true, "skill": skill}),
            RevisionedWrite::Conflict { actual_revision } => {
                revision_conflict_payload(expected_revision, actual_revision)
            }
        })
    }
    async fn tool_skill_retire(&self, arguments: &Value) -> ToolResult<Value> {
        let expected_revision = required_i64(arguments, "expected_revision")?;
        let result = self
            .skills
            .retire_skill(
                &required_string(arguments, "skill_id")?,
                required_i64(arguments, "version")?,
                expected_revision,
                &required_string(arguments, "reviewer")?,
                &required_string(arguments, "reason")?,
            )
            .map_tool_internal()?;
        Ok(match result {
            RevisionedWrite::Applied(skill) => json!({"success": true, "skill": skill}),
            RevisionedWrite::Conflict { actual_revision } => {
                revision_conflict_payload(expected_revision, actual_revision)
            }
        })
    }
    async fn tool_skill_reviews(&self, arguments: &Value) -> ToolResult<Value> {
        Ok(json!(
            self.skills
                .list_skill_reviews(
                    &required_string(arguments, "skill_id")?,
                    required_i64(arguments, "version")?,
                )
                .map_tool_internal()?
        ))
    }

    async fn tool_delegation_span_start(&self, arguments: &Value) -> ToolResult<Value> {
        let input: NewSpan = parse_coordination_input(arguments)?;
        Ok(json!(self.delegation.start_span(&input).map_tool_internal()?))
    }
    async fn tool_delegation_span_get(&self, arguments: &Value) -> ToolResult<Value> {
        exact_result(
            self.delegation
                .get_span(&required_string(arguments, "span_id")?)
                .map_tool_internal()?,
        )
    }
    async fn tool_delegation_span_close(&self, arguments: &Value) -> ToolResult<Value> {
        let status: SpanStatus =
            serde_json::from_value(json!(required_string(arguments, "status")?))
                .map_err(|error| ToolError::InvalidParams(error.to_string()))?;
        let stop_reason: StopReason =
            serde_json::from_value(json!(required_string(arguments, "stop_reason")?))
                .map_err(|error| ToolError::InvalidParams(error.to_string()))?;
        let expected_revision = required_i64(arguments, "expected_revision")?;
        let result = self
            .delegation
            .close_span(
                &required_string(arguments, "span_id")?,
                expected_revision,
                status,
                stop_reason,
                &required_string(arguments, "actor")?,
            )
            .map_tool_internal()?;
        Ok(match result {
            RevisionedWrite::Applied(span) => json!({"success": true, "span": span}),
            RevisionedWrite::Conflict { actual_revision } => {
                revision_conflict_payload(expected_revision, actual_revision)
            }
        })
    }
    async fn tool_delegation_spans_for_task(&self, arguments: &Value) -> ToolResult<Value> {
        Ok(json!(
            self.delegation
                .list_spans_for_task(&required_string(arguments, "task_id")?)
                .map_tool_internal()?
        ))
    }
    async fn tool_delegation_checkpoint_append(&self, arguments: &Value) -> ToolResult<Value> {
        let input: NewCheckpoint = parse_coordination_input(arguments)?;
        Ok(json!(self.delegation.append_checkpoint(&input).map_tool_internal()?))
    }
    async fn tool_delegation_checkpoint_get(&self, arguments: &Value) -> ToolResult<Value> {
        exact_result(
            self.delegation
                .get_checkpoint(&required_string(arguments, "checkpoint_id")?)
                .map_tool_internal()?,
        )
    }
    async fn tool_delegation_trace(&self, arguments: &Value) -> ToolResult<Value> {
        exact_result(
            self.delegation
                .trace(&required_string(arguments, "root_span_id")?)
                .map_tool_internal()?,
        )
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: Option<String>,
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone)]
struct ToolCallRequest {
    id: Option<Value>,
    name: String,
    arguments: Value,
}

impl TryFrom<JsonRpcRequest> for ToolCallRequest {
    type Error = RequestValidationError;

    fn try_from(request: JsonRpcRequest) -> std::result::Result<Self, Self::Error> {
        let params = match request.params {
            Value::Null => json!({}),
            value => value,
        };
        let params = params.as_object().ok_or_else(|| RequestValidationError {
            id: request.id.clone(),
            message: Some("tools/call params must be an object".to_owned()),
        })?;
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| RequestValidationError {
                id: request.id.clone(),
                message: Some("tools/call params.name must be a string".to_owned()),
            })?
            .to_owned();
        let arguments = params.get("arguments").cloned().unwrap_or_else(|| json!({}));
        if !arguments.is_object() {
            return Err(RequestValidationError {
                id: request.id,
                message: Some("tools/call params.arguments must be an object".to_owned()),
            });
        }
        Ok(Self { id: request.id, name, arguments })
    }
}

fn render_change_events(events: Vec<ChangeEvent>) -> ToolResult<Vec<Value>> {
    events
        .into_iter()
        .map(|event| {
            let details = change_event_details(&event);
            let occurred_at = format_rfc3339(event.occurred_at)?;
            let actor = event.actor.clone();
            Ok(json!({
                "event_type": event.event_type,
                "occurred_at": occurred_at,
                "entity_id": event.entity_id,
                "actor": actor,
                "details": details,
                "summary": summarize_change_event(&event, &details),
            }))
        })
        .collect()
}

fn change_event_details(event: &ChangeEvent) -> Value {
    event
        .details_json
        .as_deref()
        .and_then(|raw| serde_json::from_str(raw).ok())
        .unwrap_or(Value::Null)
}

fn change_event_matches_wing(event: &ChangeEvent, wing: &str) -> bool {
    change_event_details(event)
        .get("wing")
        .and_then(Value::as_str)
        .map(|event_wing| event_wing == wing)
        .unwrap_or(false)
}

fn summarize_change_event(event: &ChangeEvent, details: &Value) -> String {
    let actor = event.actor.as_deref().unwrap_or("unknown");
    match event.event_type.as_str() {
        "drawer_added" => format!(
            "{actor} added drawer in {}/{}",
            details.get("wing").and_then(Value::as_str).unwrap_or("unknown-wing"),
            details.get("room").and_then(Value::as_str).unwrap_or("unknown-room")
        ),
        "drawer_deleted" => format!("{actor} deleted drawer {}", event.entity_id),
        "diary_written" => format!(
            "{actor} wrote diary entry on {}",
            details.get("topic").and_then(Value::as_str).unwrap_or("general")
        ),
        "kg_fact_added" => format!(
            "{actor} added fact: {} -> {} -> {}",
            details.get("subject").and_then(Value::as_str).unwrap_or("?"),
            details.get("predicate").and_then(Value::as_str).unwrap_or("?"),
            details.get("object").and_then(Value::as_str).unwrap_or("?")
        ),
        "kg_fact_invalidated" => format!(
            "{actor} invalidated fact: {} -> {} -> {}",
            details.get("subject").and_then(Value::as_str).unwrap_or("?"),
            details.get("predicate").and_then(Value::as_str).unwrap_or("?"),
            details.get("object").and_then(Value::as_str).unwrap_or("?")
        ),
        "identity_updated" => format!("{actor} updated identity"),
        "lineage_set" => format!(
            "{actor} set lineage {} to revision {}",
            event.entity_id,
            details.get("revision").and_then(Value::as_i64).unwrap_or_default()
        ),
        "self_observation_proposed" => format!(
            "{actor} proposed a {} observation for {}",
            details.get("scope").and_then(Value::as_str).unwrap_or("lineage"),
            details.get("lineage_id").and_then(Value::as_str).unwrap_or("unknown-lineage")
        ),
        "self_observation_reviewed" => format!(
            "{actor} {} self-observation {}",
            details.get("decision").and_then(Value::as_str).unwrap_or("reviewed"),
            event.entity_id
        ),
        "lineage_migration_recorded" => format!(
            "{actor} recorded migration for {} to {}/{}",
            details.get("lineage_id").and_then(Value::as_str).unwrap_or("unknown-lineage"),
            details.get("to_model").and_then(Value::as_str).unwrap_or("unknown-model"),
            details.get("to_harness").and_then(Value::as_str).unwrap_or("unknown-harness")
        ),
        other => format!("{actor} recorded {other} for {}", event.entity_id),
    }
}

#[derive(Debug, Clone)]
struct RequestValidationError {
    id: Option<Value>,
    message: Option<String>,
}

#[derive(Debug)]
enum ToolError {
    InvalidParams(String),
    Internal(McpError),
}

type ToolResult<T> = std::result::Result<T, ToolError>;

trait ToolResultExt<T> {
    fn map_tool(self) -> ToolResult<T>;
    fn map_tool_internal(self) -> ToolResult<T>;
}

impl<T, E> ToolResultExt<T> for std::result::Result<T, E>
where
    E: Into<McpError>,
{
    fn map_tool(self) -> ToolResult<T> {
        self.map_err(|error| ToolError::Internal(error.into()))
    }

    fn map_tool_internal(self) -> ToolResult<T> {
        self.map_tool()
    }
}

#[derive(Debug, Clone, Copy)]
enum ErrorCode {
    ParseError = -32700,
    InvalidParams = -32602,
    MethodNotFound = -32601,
    InternalError = -32000,
}

fn jsonrpc_error(id: Option<Value>, code: ErrorCode, message: String) -> Value {
    json!({
        "jsonrpc":"2.0",
        "id":id,
        "error":{"code":code as i32,"message":message}
    })
}

fn format_rfc3339(timestamp: OffsetDateTime) -> ToolResult<String> {
    timestamp
        .format(&Rfc3339)
        .map_err(|error| ToolError::Internal(McpError::TimeFormat(error.to_string())))
}

fn required_string(arguments: &Value, field: &'static str) -> ToolResult<String> {
    arguments
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| ToolError::InvalidParams(format!("missing required string field `{field}`")))
}

fn required_non_blank_string(arguments: &Value, field: &'static str) -> ToolResult<String> {
    let value = required_string(arguments, field)?;
    let value = value.trim();
    if value.is_empty() {
        return Err(ToolError::InvalidParams(format!("field `{field}` cannot be blank")));
    }
    Ok(value.to_owned())
}

fn optional_string(arguments: &Value, field: &'static str) -> ToolResult<Option<String>> {
    match arguments.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(ToolError::InvalidParams(format!("field `{field}` must be a string"))),
    }
}

fn optional_non_blank_string(arguments: &Value, field: &'static str) -> ToolResult<Option<String>> {
    optional_string(arguments, field)?
        .map(|value| {
            let value = value.trim();
            if value.is_empty() {
                Err(ToolError::InvalidParams(format!("field `{field}` cannot be blank")))
            } else {
                Ok(value.to_owned())
            }
        })
        .transpose()
}

fn optional_bool(arguments: &Value, field: &'static str) -> ToolResult<Option<bool>> {
    match arguments.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(ToolError::InvalidParams(format!("field `{field}` must be a boolean"))),
    }
}

fn required_non_negative_i64(arguments: &Value, field: &'static str) -> ToolResult<i64> {
    let value = arguments.get(field).and_then(Value::as_i64).ok_or_else(|| {
        ToolError::InvalidParams(format!("missing required non-negative integer field `{field}`"))
    })?;
    if value < 0 {
        return Err(ToolError::InvalidParams(format!("field `{field}` cannot be negative")));
    }
    Ok(value)
}

fn required_positive_i64(arguments: &Value, field: &'static str) -> ToolResult<i64> {
    let value = required_non_negative_i64(arguments, field)?;
    if value == 0 {
        return Err(ToolError::InvalidParams(format!("field `{field}` must be positive")));
    }
    Ok(value)
}

fn required_confidence(arguments: &Value, field: &'static str) -> ToolResult<f32> {
    let value = arguments.get(field).and_then(Value::as_f64).ok_or_else(|| {
        ToolError::InvalidParams(format!("missing required numeric field `{field}`"))
    })?;
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return Err(ToolError::InvalidParams(format!(
            "field `{field}` must be a finite number from 0 to 1"
        )));
    }
    Ok(value as f32)
}

fn required_string_array(
    arguments: &Value,
    field: &'static str,
    require_non_empty: bool,
) -> ToolResult<Vec<String>> {
    let values = arguments.get(field).and_then(Value::as_array).ok_or_else(|| {
        ToolError::InvalidParams(format!("missing required string-array field `{field}`"))
    })?;
    let mut parsed = Vec::with_capacity(values.len());
    for value in values {
        let Some(value) = value.as_str() else {
            return Err(ToolError::InvalidParams(format!(
                "every item in `{field}` must be a string"
            )));
        };
        let value = value.trim();
        if value.is_empty() {
            return Err(ToolError::InvalidParams(format!(
                "items in `{field}` cannot be blank"
            )));
        }
        parsed.push(value.to_owned());
    }
    if require_non_empty && parsed.is_empty() {
        return Err(ToolError::InvalidParams(format!(
            "field `{field}` must contain at least one item"
        )));
    }
    Ok(parsed)
}

fn optional_string_array(arguments: &Value, field: &'static str) -> ToolResult<Vec<String>> {
    match arguments.get(field) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(_) => required_string_array(arguments, field, false),
    }
}

fn required_record_id(arguments: &Value, field: &'static str) -> ToolResult<String> {
    let value = required_string(arguments, field)?;
    validate_record_id(field, &value)
}

fn validate_record_id(field: &'static str, value: &str) -> ToolResult<String> {
    validate_record_id_value(value)
        .map_err(|message| ToolError::InvalidParams(format!("field `{field}` {message}")))
}

fn validate_record_id_value(value: &str) -> std::result::Result<String, String> {
    let value = value.trim();
    if value.is_empty() || value.len() > 128 {
        return Err("must be between 1 and 128 bytes".to_owned());
    }
    if !value
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || "-_.:/".contains(character))
    {
        return Err(
            "may contain only ASCII letters, digits, '-', '_', '.', ':', and '/'".to_owned(),
        );
    }
    Ok(value.to_owned())
}

fn optional_usize(arguments: &Value, field: &'static str) -> ToolResult<Option<usize>> {
    match arguments.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .map(|value| value as usize)
            .ok_or_else(|| ToolError::InvalidParams(format!("field `{field}` must be a usize")))
            .map(Some),
    }
}

fn required_i64(arguments: &Value, field: &'static str) -> ToolResult<i64> {
    arguments.get(field).and_then(Value::as_i64).ok_or_else(|| {
        ToolError::InvalidParams(format!("missing required integer field `{field}`"))
    })
}
fn optional_i64(arguments: &Value, field: &'static str) -> ToolResult<Option<i64>> {
    match arguments.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v
            .as_i64()
            .map(Some)
            .ok_or_else(|| ToolError::InvalidParams(format!("field `{field}` must be an integer"))),
    }
}
fn parse_coordination_input<T: for<'de> Deserialize<'de>>(arguments: &Value) -> ToolResult<T> {
    serde_json::from_value(arguments.clone()).map_err(|e| ToolError::InvalidParams(e.to_string()))
}
fn exact_result<T: Serialize>(value: Option<T>) -> ToolResult<Value> {
    Ok(match value {
        Some(value) => json!({"found":true,"value":value}),
        None => json!({"found":false}),
    })
}

fn optional_f32(arguments: &Value, field: &'static str) -> ToolResult<Option<f32>> {
    match arguments.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => {
            let value = value.as_f64().ok_or_else(|| {
                ToolError::InvalidParams(format!("field `{field}` must be an f32"))
            })?;
            if !value.is_finite() || value < f32::MIN as f64 || value > f32::MAX as f64 {
                return Err(ToolError::InvalidParams(format!(
                    "field `{field}` must be a finite f32"
                )));
            }
            Ok(Some(value as f32))
        }
    }
}

fn parse_wing_id(value: &str) -> ToolResult<WingId> {
    WingId::normalized(value).map_err(|error| ToolError::InvalidParams(error.to_string()))
}

fn parse_room_id(value: &str) -> ToolResult<RoomId> {
    RoomId::new(value).map_err(|error| ToolError::InvalidParams(error.to_string()))
}

fn parse_drawer_id(value: &str) -> ToolResult<DrawerId> {
    DrawerId::new(value).map_err(|error| ToolError::InvalidParams(error.to_string()))
}

fn parse_date(value: &str) -> ToolResult<Date> {
    Date::parse(value, &time::macros::format_description!("[year]-[month]-[day]")).map_err(|_| {
        ToolError::InvalidParams(format!("invalid date `{value}`; expected YYYY-MM-DD"))
    })
}

fn parse_direction(value: &str) -> ToolResult<QueryDirection> {
    match value {
        "outgoing" => Ok(QueryDirection::Outgoing),
        "incoming" => Ok(QueryDirection::Incoming),
        "both" => Ok(QueryDirection::Both),
        other => Err(ToolError::InvalidParams(format!(
            "invalid direction `{other}`; expected outgoing, incoming, or both"
        ))),
    }
}

fn parse_self_observation_scope(value: &str) -> ToolResult<SelfObservationScope> {
    SelfObservationScope::parse(value).ok_or_else(|| {
        ToolError::InvalidParams(format!(
            "invalid self-observation scope `{value}`; expected lineage, shared, or engine"
        ))
    })
}

fn observation_applies_to_runtime(
    observation: &SelfObservationRecord,
    model: Option<&str>,
    harness: Option<&str>,
) -> bool {
    if observation.scope != SelfObservationScope::Engine {
        return true;
    }
    let model_matches = observation.model.as_deref().is_none_or(|expected| model == Some(expected));
    let harness_matches = observation
        .harness
        .as_deref()
        .is_none_or(|expected| harness == Some(expected));
    model_matches && harness_matches
}

/// Converts a local `mempalace_storage::TaskState` to the wire `CoordinationTaskState` used by
/// `TransitionTaskRequest`, mirroring `wire_task_state` in `crates/mempalace-server/src/lib.rs`
/// (that copy converts the same two enums the other direction across the HTTP boundary; this
/// one is needed on the client side when a local task-not-found falls back to a federated
/// transition request).
fn wire_task_state(state: TaskState) -> WireTaskState {
    match state {
        TaskState::Pending => WireTaskState::Pending,
        TaskState::Running => WireTaskState::Running,
        TaskState::InputRequired => WireTaskState::InputRequired,
        TaskState::Completed => WireTaskState::Completed,
        TaskState::Cancelled => WireTaskState::Cancelled,
        TaskState::Failed => WireTaskState::Failed,
        TaskState::Expired => WireTaskState::Expired,
    }
}

/// True when a coordination write's local failure is specifically "the referenced task or
/// message does not exist locally" (`require_task`/`get_message` in
/// `mempalace_storage::coordination`) — the signal a federated coordination tool uses to decide
/// whether to fall back to a remote rather than surface the error immediately. Every other
/// `StorageError` (a lease/state conflict, bad input) is not a "wrong palace" signal and must
/// propagate as-is.
///
/// Matches against [`mempalace_storage::NOT_FOUND_SUFFIX`] rather than a bare `"not found"`
/// literal, so a future rewording of the underlying message is a compile error at its
/// construction site instead of silently disabling federation fallback for that path.
fn is_local_record_missing(err: &mempalace_storage::StorageError) -> bool {
    matches!(
        err,
        mempalace_storage::StorageError::Invariant(msg)
            if msg.contains(mempalace_storage::NOT_FOUND_SUFFIX)
    )
}

/// Parses an optional `{remote_name: cursor}` object argument into a per-remote cursor map, the
/// same shape `mempalace_get_changes_since`'s inline `cursors` parsing already uses — factored
/// out here so `tool_coordination_events`/`tool_inbox_read` do not duplicate it a second time.
fn parse_cursors_arg(arguments: &Value, field: &'static str) -> ToolResult<BTreeMap<String, String>> {
    match arguments.get(field) {
        None | Some(Value::Null) => Ok(BTreeMap::new()),
        Some(Value::Object(map)) => {
            let mut out = BTreeMap::new();
            for (k, v) in map {
                match v.as_str() {
                    Some(s) => {
                        out.insert(k.clone(), s.to_owned());
                    }
                    None => {
                        return Err(ToolError::InvalidParams(format!(
                            "field `{field}` must be an object of string values"
                        )));
                    }
                }
            }
            Ok(out)
        }
        Some(_) => Err(ToolError::InvalidParams(format!(
            "field `{field}` must be an object of string values"
        ))),
    }
}

fn revision_conflict_payload(expected_revision: i64, actual_revision: Option<i64>) -> Value {
    json!({
        "success": false,
        "conflict": {
            "expected_revision": expected_revision,
            "actual_revision": actual_revision,
            "message": if actual_revision.is_some() {
                "The record changed since it was read. Reload it and retry with the current revision."
            } else {
                "The record does not exist at the expected revision."
            },
        }
    })
}

fn format_date(value: Date) -> String {
    value.to_string()
}

#[derive(Debug, Clone)]
struct DiaryReadFilters {
    agent_name: Option<String>,
    wing: Option<WingId>,
    topic: Option<String>,
    entry_id: Option<DrawerId>,
    since: Option<OffsetDateTime>,
    last_n: usize,
}

impl DiaryReadFilters {
    fn from_arguments(arguments: &Value) -> ToolResult<Self> {
        let now = OffsetDateTime::now_utc();
        let agent_name = optional_string(arguments, "agent_name")?;
        let wing =
            optional_string(arguments, "wing")?.map(|wing| parse_wing_id(&wing)).transpose()?;
        let topic = optional_string(arguments, "topic")?;
        let entry_id =
            optional_string(arguments, "entry_id")?.as_deref().map(parse_drawer_id).transpose()?;
        let since = if entry_id.is_some() {
            None
        } else {
            Some(
                optional_string(arguments, "since")?
                    .as_deref()
                    .map(parse_since_timestamp)
                    .transpose()?
                    .unwrap_or_else(|| now - Duration::days(1)),
            )
        };
        let last_n = optional_usize(arguments, "last_n")?.unwrap_or(10);
        Ok(Self { agent_name, wing, topic, entry_id, since, last_n })
    }
}

fn parse_since_timestamp(value: &str) -> ToolResult<OffsetDateTime> {
    OffsetDateTime::parse(value, &Rfc3339).map_err(|_| {
        ToolError::InvalidParams(format!(
            "invalid `since` timestamp `{value}`; expected ISO 8601 e.g. 2026-05-08T10:00:00Z"
        ))
    })
}

fn diary_entry_matches(drawer: &DrawerRecord, filters: &DiaryReadFilters) -> bool {
    if drawer.ingest_mode != "diary" {
        return false;
    }
    if let Some(since) = filters.since {
        if drawer.filed_at < since {
            return false;
        }
    }
    if let Some(agent_name) = filters.agent_name.as_deref() {
        if drawer.added_by != agent_name {
            return false;
        }
    }
    if let Some(topic) = filters.topic.as_deref() {
        if diary_entry_topic(drawer) != topic {
            return false;
        }
    }
    true
}

fn diary_entry_topic(drawer: &DrawerRecord) -> &str {
    drawer.source_file.strip_prefix(DIARY_TOPIC_PREFIX).unwrap_or("general")
}

fn render_diary_entry(
    drawer: DrawerRecord,
    include_agent: bool,
    summary: Option<String>,
) -> ToolResult<Value> {
    let topic = diary_entry_topic(&drawer).to_owned();
    let date = drawer.date.map(format_date).unwrap_or_else(|| drawer.filed_at.date().to_string());
    let timestamp = format_rfc3339(drawer.filed_at)?;
    let mut entry = json!({
        "date": date,
        "timestamp": timestamp,
        "wing": drawer.wing.as_str(),
        "topic": topic,
        "entry_id": drawer.id.as_str(),
    });
    if let Some(summary) = summary {
        entry["summary"] = json!(summary);
    } else {
        entry["content"] = json!(drawer.content);
    }
    if include_agent {
        entry["agent"] = json!(drawer.added_by);
    }
    Ok(entry)
}

fn validate_diary_summary(summary: &str) -> ToolResult<()> {
    let length = summary.chars().count();
    if length > DIARY_SUMMARY_MAX_CHARS {
        return Err(ToolError::InvalidParams(format!(
            "`summary` must be at most {DIARY_SUMMARY_MAX_CHARS} characters; received {length}"
        )));
    }
    Ok(())
}

fn legacy_diary_summary(content: &str) -> String {
    content.chars().take(DIARY_SUMMARY_MAX_CHARS).collect()
}

pub(crate) fn round_similarity(value: f32) -> f32 {
    (value * 1_000.0).round() / 1_000.0
}

fn generated_drawer_id(
    prefix: &str,
    wing: &str,
    room: &str,
    content: &str,
    now: OffsetDateTime,
) -> ToolResult<DrawerId> {
    let mut hasher = Hasher::new();
    hasher.update(content.as_bytes());
    hasher.update(now.unix_timestamp_nanos().to_string().as_bytes());
    let suffix = hasher.finalize().to_hex().chars().take(16).collect::<String>();
    DrawerId::new(format!("{prefix}_{wing}_{room}_{suffix}"))
        .map_err(|error| ToolError::InvalidParams(error.to_string()))
}

fn generated_record_id(
    prefix: &str,
    lineage_id: &str,
    content: &str,
    now: OffsetDateTime,
) -> String {
    let mut hasher = Hasher::new();
    hasher.update(lineage_id.as_bytes());
    hasher.update(content.as_bytes());
    hasher.update(now.unix_timestamp_nanos().to_string().as_bytes());
    let suffix = hasher.finalize().to_hex().chars().take(20).collect::<String>();
    format!("{prefix}_{suffix}")
}

fn hash_text(content: &str) -> String {
    mempalace_core::hash_text(content)
}

fn infer_entity_kind(name: &str) -> EntityKind {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return EntityKind::Unknown;
    }
    if name.chars().any(|ch| ch.is_ascii_digit()) {
        return EntityKind::Concept;
    }

    let tokens = trimmed
        .split(|ch: char| ch.is_whitespace() || ch == '-')
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>();
    if tokens.len() >= 2
        && tokens.iter().all(|token| {
            let mut chars = token.chars();
            let Some(first) = chars.next() else {
                return false;
            };
            first.is_ascii_uppercase() && chars.all(|ch| ch.is_ascii_lowercase() || ch == '\'')
        })
    {
        return EntityKind::Person;
    }
    if tokens.len() == 1 && trimmed.chars().all(|ch| ch.is_ascii_uppercase()) {
        return EntityKind::Concept;
    }

    EntityKind::Unknown
}

#[cfg(test)]
fn diary_wing_name(agent_name: &str) -> String {
    format!("wing_{}", diary_slugify(agent_name))
}

fn diary_write_wing_name(scope: &str, wing: Option<String>) -> ToolResult<String> {
    match scope {
        "agent" => Ok(SHARED_AGENT_DIARY_WING.to_owned()),
        "project" => wing.ok_or_else(|| {
            ToolError::InvalidParams(
                "missing required string field `wing` for project-scoped diary entry".to_owned(),
            )
        }),
        other => Err(ToolError::InvalidParams(format!(
            "invalid `scope` `{other}`; expected agent or project"
        ))),
    }
}

#[cfg(test)]
fn legacy_diary_wing_name(agent_name: &str) -> String {
    format!("wing_{}", legacy_slugify(agent_name))
}

#[cfg(test)]
fn diary_slugify(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|ch| {
            if ch.is_ascii_whitespace() {
                '_'
            } else if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '/' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
fn legacy_slugify(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

fn fuzzy_match_rooms(query: &str, snapshot: &PalaceGraphSnapshot) -> Vec<String> {
    let lower = query.to_ascii_lowercase();
    let mut scored = snapshot
        .nodes
        .keys()
        .filter_map(|room| {
            let room_lower = room.to_ascii_lowercase();
            if room_lower.contains(&lower) {
                Some((room.clone(), 2usize))
            } else if lower
                .split('-')
                .any(|segment| !segment.is_empty() && room_lower.contains(segment))
            {
                Some((room.clone(), 1usize))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));
    scored.into_iter().take(5).map(|entry| entry.0).collect()
}

pub fn decode_tool_payload(response: &Value) -> Option<Value> {
    let text =
        response.get("result")?.get("content")?.as_array()?.first()?.get("text")?.as_str()?;
    serde_json::from_str(text).ok()
}

pub fn phase0_tools_fixture() -> Result<Value> {
    let path = fixture_root().join("inventory").join("mcp-tools.json");
    let body =
        fs::read_to_string(&path).map_err(|source| McpError::Io { path: path.clone(), source })?;
    serde_json::from_str(&body).map_err(McpError::from)
}

pub fn phase0_contract_fixture() -> Result<Value> {
    let path = fixture_root().join("goldens").join("mcp-contract.json");
    let body =
        fs::read_to_string(&path).map_err(|source| McpError::Io { path: path.clone(), source })?;
    serde_json::from_str(&body).map_err(McpError::from)
}

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/phase0")
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc;

    use mempalace_config::{
        FederationRuntimeConfig, LowCpuRuntimeConfig, MaintenanceRuntimeConfig, ResolvedRemote,
        ResolvedRouteRule, RouteMode, ServerRuntimeConfig, WriteTarget,
    };
    use mempalace_embeddings::{StartupValidation, StartupValidationStatus};
    use tempfile::TempDir;
    use time::macros::{date, datetime};
    use tokio::io::{AsyncReadExt, BufReader};

    #[derive(Debug)]
    struct TestHarness {
        _tempdir: TempDir,
        server: McpServer<DeterministicStubProvider>,
    }

    async fn test_harness() -> TestHarness {
        test_harness_with_config(
            LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
            EmbeddingProfile::Balanced,
        )
        .await
    }

    async fn test_harness_with_config(
        low_cpu: LowCpuRuntimeConfig,
        embedding_profile: EmbeddingProfile,
    ) -> TestHarness {
        let tempdir = TempDir::new().unwrap();
        let palace_path = tempdir.path().join("palace");
        let config = MempalaceConfig {
            low_cpu,
            embedding_profile,
            ..make_base_config(&palace_path, &tempdir)
        };
        let server =
            McpServer::from_parts(config, DeterministicStubProvider::new(embedding_profile))
                .await
                .unwrap();
        seed_drawers(&server).await;
        seed_knowledge_graph(&server).await;
        TestHarness { _tempdir: tempdir, server }
    }

    async fn test_harness_with_bound_lineage(lineage_id: &str) -> TestHarness {
        let tempdir = TempDir::new().unwrap();
        let palace_path = tempdir.path().join("palace");
        let config = make_base_config(&palace_path, &tempdir);
        let server = McpServer::from_parts_with_lineage(
            config,
            DeterministicStubProvider::new(EmbeddingProfile::Balanced),
            Some(lineage_id.to_owned()),
        )
        .await
        .unwrap();
        seed_drawers(&server).await;
        seed_knowledge_graph(&server).await;
        TestHarness { _tempdir: tempdir, server }
    }

    fn test_diary_drawer(id: &str, content: &str, filed_at: OffsetDateTime) -> DrawerRecord {
        DrawerRecord {
            id: DrawerId::new(id).unwrap(),
            wing: WingId::new(SHARED_AGENT_DIARY_WING).unwrap(),
            room: RoomId::new(DIARY_ROOM).unwrap(),
            hall: Some(DIARY_HALL.to_owned()),
            date: Some(filed_at.date()),
            source_file: format!("{DIARY_TOPIC_PREFIX}wakeup"),
            chunk_index: 0,
            ingest_mode: "diary".to_owned(),
            extract_mode: None,
            added_by: "Wake Test".to_owned(),
            filed_at,
            importance: None,
            emotional_weight: None,
            weight: None,
            content: content.to_owned(),
            content_hash: hash_text(content),
            embedding: vec![0.0; EmbeddingProfile::Balanced.metadata().dimensions],
            locator: None,
            view_metadata: None,
        }
    }

    #[derive(Debug, Clone)]
    struct BlockingProvider {
        started_tx: Arc<std::sync::Mutex<Option<mpsc::Sender<()>>>>,
        release_rx: Arc<std::sync::Mutex<mpsc::Receiver<()>>>,
    }

    impl EmbeddingProvider for BlockingProvider {
        fn profile(&self) -> &'static mempalace_core::EmbeddingProfileMetadata {
            EmbeddingProfile::Balanced.metadata()
        }

        fn startup_validation(&self) -> mempalace_embeddings::Result<StartupValidation> {
            Ok(StartupValidation {
                status: StartupValidationStatus::Ready,
                cache_root: PathBuf::from("/tmp"),
                model_id: EmbeddingProfile::Balanced.metadata().model_id,
                detail: "blocking".to_owned(),
            })
        }

        fn embed(
            &mut self,
            request: &EmbeddingRequest,
        ) -> mempalace_embeddings::Result<mempalace_embeddings::EmbeddingResponse> {
            if let Some(sender) = self.started_tx.lock().unwrap().take() {
                let _ = sender.send(());
            }
            let _ = self.release_rx.lock().unwrap().recv();
            mempalace_embeddings::EmbeddingResponse::from_vectors(
                vec![vec![0.0; EmbeddingProfile::Balanced.metadata().dimensions]; request.len()],
                EmbeddingProfile::Balanced.metadata().dimensions,
                EmbeddingProfile::Balanced,
                EmbeddingProfile::Balanced.metadata().model_id,
            )
        }
    }

    async fn seed_drawers(server: &McpServer<DeterministicStubProvider>) {
        let runtime = server.runtime.lock().await;
        let now = datetime!(2026-04-11 09:00:00 UTC);
        let drawers = vec![
            DrawerRecord {
                id: DrawerId::new("wing_code/auth-migration/0001").unwrap(),
                wing: WingId::new("wing_code").unwrap(),
                room: RoomId::new("auth-migration").unwrap(),
                hall: Some("hall_facts".to_owned()),
                date: Some(date!(2026 - 04 - 10)),
                source_file: "code.txt".to_owned(),
                chunk_index: 0,
                ingest_mode: "fixtures".to_owned(),
                extract_mode: None,
                added_by: "tests".to_owned(),
                filed_at: now,
                importance: None,
                emotional_weight: None,
                weight: None,
                content: "Code notes: auth-migration keeps search filter semantics exact while storage changes underneath.".to_owned(),
                content_hash: hash_text(
                    "Code notes: auth-migration keeps search filter semantics exact while storage changes underneath.",
                ),
                embedding: vec![1.0; EmbeddingProfile::Balanced.metadata().dimensions],
                locator: None,
                view_metadata: None,
            },
            DrawerRecord {
                id: DrawerId::new("wing_team/auth-migration/0001").unwrap(),
                wing: WingId::new("wing_team").unwrap(),
                room: RoomId::new("auth-migration").unwrap(),
                hall: Some("hall_events".to_owned()),
                date: Some(date!(2026 - 04 - 11)),
                source_file: "team.txt".to_owned(),
                chunk_index: 0,
                ingest_mode: "fixtures".to_owned(),
                extract_mode: None,
                added_by: "tests".to_owned(),
                filed_at: now,
                importance: None,
                emotional_weight: None,
                weight: None,
                content: "The team decided the auth-migration must preserve CLI and MCP parity.".to_owned(),
                content_hash: hash_text(
                    "The team decided the auth-migration must preserve CLI and MCP parity.",
                ),
                embedding: vec![1.0; EmbeddingProfile::Balanced.metadata().dimensions],
                locator: None,
                view_metadata: None,
            },
        ];
        runtime
            .storage
            .drawer_store()
            .put_drawers(&drawers, DuplicateStrategy::Error)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn federated_branch_overrides_suppress_only_matching_wing_and_path() {
        let harness = test_harness().await;
        let mut runtime = harness.server.runtime.lock().await;
        let now = datetime!(2026-04-11 09:00:00 UTC);
        for (id, source_file, path_state) in [
            ("wing_code/general/tombstone", "deleted.md", "deleted"),
            ("wing_code/general/replacement", "changed.md", "present"),
        ] {
            let mut override_row = test_diary_drawer(id, "branch overlay", now);
            override_row.wing = WingId::new("wing_code").unwrap();
            override_row.room = RoomId::new("general").unwrap();
            override_row.source_file = source_file.to_owned();
            override_row.ingest_mode = "projects-branch".to_owned();
            override_row.view_metadata = Some(mempalace_core::RepositoryViewMetadata {
                repo_id: "repo".to_owned(),
                view_name: Some("feature-x".to_owned()),
                source_path: "/repo".to_owned(),
                head_commit: Some("head".to_owned()),
                base_ref: Some("main".to_owned()),
                merge_base: Some("base".to_owned()),
                worktree_id: "worktree".to_owned(),
                path_state: path_state.to_owned(),
            });
            runtime
                .storage
                .drawer_store()
                .put_drawers(&[override_row], DuplicateStrategy::Error)
                .await
                .unwrap();
        }

        let mut payload = json!({"results": [
            {"origin": "alpha", "wing": "wing_code", "source_file": "deleted.md"},
            {"origin": "alpha", "wing": "wing_code", "source_file": "changed.md"},
            {"origin": "alpha", "wing": "wing_team", "source_file": "changed.md"}
        ]});
        runtime
            .filter_federated_view_overrides(&mut payload, &Some("feature-x".to_owned()), &None)
            .await
            .unwrap();

        let results = payload["results"].as_array().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["wing"], "wing_team");
    }

    async fn seed_knowledge_graph(server: &McpServer<DeterministicStubProvider>) {
        let runtime = server.runtime.lock().await;
        let kg = KnowledgeGraphRuntime::new(runtime.storage.operational_store());
        kg.add_fact(
            AddFactRequest {
                subject: "Rust Rewrite".to_owned(),
                subject_type: EntityKind::Project,
                predicate: "preserves".to_owned(),
                object: "CLI Parity".to_owned(),
                object_type: EntityKind::Concept,
                valid_from: Some(date!(2026 - 04 - 10)),
                valid_to: None,
                confidence: 1.0,
                source_drawer_id: None,
                source_file: None,
            },
            datetime!(2026-04-10 10:00:00 UTC),
        )
        .unwrap();
    }

    fn tool_call(id: i64, name: &str, arguments: Value) -> JsonRpcRequest {
        JsonRpcRequest {
            jsonrpc: Some("2.0".to_owned()),
            id: Some(json!(id)),
            method: "tools/call".to_owned(),
            params: json!({"name": name, "arguments": arguments}),
        }
    }

    #[tokio::test]
    async fn tool_inventory_matches_phase0_fixture() {
        let expected = phase0_tools_fixture().unwrap();
        let actual = tool_definitions()
            .into_iter()
            .map(|tool| {
                (
                    tool.name.to_owned(),
                    json!({
                        "description": tool.description,
                        "input_schema": tool.input_schema,
                    }),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let expected = expected.as_object().expect("phase 0 tool fixture must be an object");
        for (name, definition) in expected {
            assert_eq!(actual.get(name), Some(definition), "phase 0 tool `{name}` changed");
        }
    }

    #[test]
    fn identity_tools_do_not_expose_model_selectable_lineage_ids() {
        for tool_name in ["mempalace_wake_up", "mempalace_identity_packet"] {
            let tool = tool_definitions()
                .into_iter()
                .find(|tool| tool.name == tool_name)
                .unwrap();
            assert!(
                tool.input_schema["properties"].get("lineage_id").is_none(),
                "{tool_name} must not let the model select its lineage"
            );
        }
    }

    /// Regression for Codex finding 3832912235: `tool_inbox_read`/`tool_coordination_events`
    /// read `remote_cursors` off the arguments (`parse_cursors_arg`), but until now neither
    /// tool's `input_schema` declared it, so a schema-driven MCP client had no way to know the
    /// field exists — every subsequent federated page restarted each remote's paging. Both
    /// `mempalace_inbox_read` and `mempalace_coordination_events` must declare a `remote_cursors`
    /// object property whose values are strings, matching the `{remote_name: cursor}` shape
    /// `parse_cursors_arg` actually parses.
    #[test]
    fn inbox_read_and_coordination_events_declare_remote_cursors_schema() {
        for tool_name in ["mempalace_inbox_read", "mempalace_coordination_events"] {
            let tool = tool_definitions()
                .into_iter()
                .find(|tool| tool.name == tool_name)
                .unwrap_or_else(|| panic!("{tool_name} must be a defined tool"));
            let remote_cursors = tool.input_schema["properties"]
                .get("remote_cursors")
                .unwrap_or_else(|| panic!("{tool_name} must declare `remote_cursors`"));
            assert_eq!(
                remote_cursors["type"], "object",
                "{tool_name}.remote_cursors must be an object: {remote_cursors}"
            );
            assert_eq!(
                remote_cursors["additionalProperties"]["type"], "string",
                "{tool_name}.remote_cursors values must be declared as strings, matching \
                 parse_cursors_arg: {remote_cursors}"
            );
        }
    }

    #[tokio::test]
    async fn initialize_matches_phase0_contract_shape() {
        let fixture = phase0_contract_fixture().unwrap();
        let harness = test_harness().await;
        let response = harness
            .server
            .handle_request(JsonRpcRequest {
                jsonrpc: Some("2.0".to_owned()),
                id: Some(json!(1)),
                method: "initialize".to_owned(),
                params: json!({}),
            })
            .await;
        assert_eq!(response, fixture["initialize"]);
    }

    #[tokio::test]
    async fn native_coordination_tools_support_exact_recovery_and_inbox_acknowledgement() {
        let harness = test_harness().await;
        let created = harness
            .server
            .handle_request(tool_call(
                900,
                "mempalace_task_create",
                json!({
                    "title":"Research", "description":"Produce a result", "created_by":"manager",
                    "wing":"wing_test", "idempotency_key":"task-request-1", "budget":{"tokens":500}
                }),
            ))
            .await;
        let task = decode_tool_payload(&created).expect("task payload");
        let task_id = task["task_id"].as_str().expect("task id");

        let replay = harness
            .server
            .handle_request(tool_call(
                901,
                "mempalace_task_create",
                json!({
                    "title":"Research", "description":"Produce a result", "created_by":"manager",
                    "wing":"wing_test", "idempotency_key":"task-request-1", "budget":{"tokens":500}
                }),
            ))
            .await;
        assert_eq!(decode_tool_payload(&replay).expect("replay")["task_id"], task_id);

        let sent = harness
            .server
            .handle_request(tool_call(
                902,
                "mempalace_message_send",
                json!({
                    "task_id":task_id, "sender":"manager", "recipient":"worker", "kind":"handoff",
                    "payload":{"instructions":"start"}, "idempotency_key":"message-request-1"
                }),
            ))
            .await;
        let message = decode_tool_payload(&sent).expect("message payload");
        let message_id = message["message_id"].as_str().expect("message id");
        let inbox = harness
            .server
            .handle_request(tool_call(903, "mempalace_inbox_read", json!({"recipient":"worker"})))
            .await;
        assert_eq!(
            decode_tool_payload(&inbox).expect("inbox")["messages"][0]["message_id"],
            message_id
        );
        let ack = harness
            .server
            .handle_request(tool_call(
                904,
                "mempalace_message_acknowledge",
                json!({"message_id":message_id,"actor":"worker"}),
            ))
            .await;
        assert!(decode_tool_payload(&ack).expect("ack")["acknowledged_at"].is_string());
        let exact = harness
            .server
            .handle_request(tool_call(905, "mempalace_task_get", json!({"task_id":task_id})))
            .await;
        assert_eq!(decode_tool_payload(&exact).expect("exact")["found"], true);
        let missing = harness
            .server
            .handle_request(tool_call(906, "mempalace_task_get", json!({"task_id":"task_missing"})))
            .await;
        assert_eq!(decode_tool_payload(&missing).expect("missing")["found"], false);
        let result = harness
            .server
            .handle_request(tool_call(
                907,
                "mempalace_result_put",
                json!({
                    "task_id":task_id, "created_by":"worker", "payload":{"answer":42},
                    "idempotency_key":"result-request-1"
                }),
            ))
            .await;
        let result_id = decode_tool_payload(&result).expect("result")["result_id"]
            .as_str()
            .expect("result id")
            .to_owned();
        let exact_result_response = harness
            .server
            .handle_request(tool_call(
                908,
                "mempalace_result_get",
                json!({"result_id":result_id}),
            ))
            .await;
        assert_eq!(
            decode_tool_payload(&exact_result_response).expect("exact result")["found"],
            true
        );
    }

    #[tokio::test]
    async fn task_create_requires_a_wing() {
        let harness = test_harness().await;
        let response = harness
            .server
            .handle_request(tool_call(
                909,
                "mempalace_task_create",
                json!({
                    "title":"Research", "description":"Produce a result", "created_by":"manager",
                    "idempotency_key":"missing-wing-1"
                }),
            ))
            .await;
        // A bare `error.is_some()` would also pass for an unrelated failure, so check the
        // message actually names the missing field rather than just that *something* failed.
        let message = response["error"]["message"]
            .as_str()
            .unwrap_or_else(|| panic!("expected a tool error naming `wing`, got: {response}"));
        assert!(
            message.contains("wing"),
            "error message must name the missing `wing` field, got: {message}"
        );
    }

    #[tokio::test]
    async fn task_create_normalises_the_wing() {
        let harness = test_harness().await;
        let created = harness
            .server
            .handle_request(tool_call(
                910,
                "mempalace_task_create",
                json!({
                    "title":"Research", "description":"Produce a result", "created_by":"manager",
                    "wing":"myproject", "idempotency_key":"wing-normalise-1"
                }),
            ))
            .await;
        let task = decode_tool_payload(&created).expect("task payload");
        assert_eq!(task["wing"], "wing_myproject");
    }

    /// Regression test for the coordination wing-normalisation security fix (Impact A): a
    /// caller-supplied short/mixed-case spelling of `wing_agents` must still hit the diary
    /// hard-override and resolve local, even when `default_mode` is `remote`. Before the fix,
    /// `tool_task_create` routed on the raw, un-normalised wing string, so `"agents"` did not
    /// `==` `wing_agents`, missed the override, and fell through to `default_mode: remote` —
    /// shipping the task body to the configured remote before the peer's own normalisation
    /// could reject it.
    ///
    /// Asserts on the mock's call counter, not on the tool's result: a bare "task was created"
    /// check can pass even when the write went remote (the remote could still accept it, or a
    /// local fallback could mask it), so the only thing that actually proves the remote was
    /// never touched is the recording mock's `coordination_calls` counter staying at zero — the
    /// same pattern `coordination_fallback_records_zero_remote_calls_without_coordination_federation_config`
    /// in `federation.rs` uses for the sibling ID-discovery fallback.
    #[tokio::test]
    async fn task_create_wing_agents_short_form_stays_local_and_never_calls_remote() {
        for raw_wing in ["agents", "Wing_Agents", " wing_agents "] {
            let remote = LibMockRemote::default();
            let calls = std::sync::Arc::clone(&remote.coordination_calls);
            let mut remotes: BTreeMap<String, Arc<dyn mempalace_remote::RemoteApi>> =
                BTreeMap::new();
            remotes.insert("hub".to_owned(), Arc::new(remote));
            let mut rules_remotes = BTreeMap::new();
            for name in remotes.keys() {
                rules_remotes.insert(
                    name.clone(),
                    ResolvedRemote {
                        name: name.clone(),
                        url: "https://test.example".to_owned(),
                        token: None,
                        timeout: std::time::Duration::from_secs(5),
                    },
                );
            }
            let rules = FederationRuntimeConfig {
                remotes: rules_remotes,
                default_mode: RouteMode::Remote,
                default_remote: Some("hub".to_owned()),
                wings: BTreeMap::new(),
                kg: None,
                coordination: BTreeMap::new(),
            };
            let router = FederationRouter::with_remotes(rules, remotes);
            let harness = test_harness_with_mock_router(router).await;

            let created = harness
                .server
                .handle_request(tool_call(
                    920,
                    "mempalace_task_create",
                    json!({
                        "title":"Research", "description":"Produce a result", "created_by":"manager",
                        "wing": raw_wing,
                        "idempotency_key": format!("agents-bypass-{raw_wing}")
                    }),
                ))
                .await;
            decode_tool_payload(&created).unwrap_or_else(|| {
                panic!("expected a successful local task for wing {raw_wing:?}, got: {created}")
            });
            let observed = calls.load(std::sync::atomic::Ordering::SeqCst);
            assert_eq!(
                observed, 0,
                "wing {raw_wing:?} must resolve local via the wing_agents diary hard-override \
                 and must never reach the remote, got {observed} remote call(s)"
            );
        }
    }

    /// Regression test for the coordination wing-normalisation security fix (Impact B): an
    /// operator's explicit `federation.coordination["wing_secret"]: { mode: local }` pin must
    /// still apply when the caller passes the short form `"secret"`, even though
    /// `default_mode` is `remote`. Before the fix, routing was keyed on the raw string, so the
    /// map lookup for `"secret"` missed the canonical `"wing_secret"` key entirely and fell
    /// through to `default_mode: remote`, silently ignoring the pin and persisting the task on
    /// the remote with no local record at all.
    ///
    /// As above, this asserts on the recording mock's call count, not on the result.
    #[tokio::test]
    async fn task_create_respects_an_explicit_local_pin_under_a_short_wing_form() {
        let remote = LibMockRemote::default();
        let calls = std::sync::Arc::clone(&remote.coordination_calls);
        let mut remotes: BTreeMap<String, Arc<dyn mempalace_remote::RemoteApi>> = BTreeMap::new();
        remotes.insert("hub".to_owned(), Arc::new(remote));
        let mut rules_remotes = BTreeMap::new();
        for name in remotes.keys() {
            rules_remotes.insert(
                name.clone(),
                ResolvedRemote {
                    name: name.clone(),
                    url: "https://test.example".to_owned(),
                    token: None,
                    timeout: std::time::Duration::from_secs(5),
                },
            );
        }
        let mut coordination = BTreeMap::new();
        coordination.insert(
            "wing_secret".to_owned(),
            ResolvedRouteRule { mode: RouteMode::Local, remote: None, write: WriteTarget::Local },
        );
        let rules = FederationRuntimeConfig {
            remotes: rules_remotes,
            default_mode: RouteMode::Remote,
            default_remote: Some("hub".to_owned()),
            wings: BTreeMap::new(),
            kg: None,
            coordination,
        };
        let router = FederationRouter::with_remotes(rules, remotes);
        let harness = test_harness_with_mock_router(router).await;

        let created = harness
            .server
            .handle_request(tool_call(
                921,
                "mempalace_task_create",
                json!({
                    "title":"Research", "description":"Produce a result", "created_by":"manager",
                    "wing":"secret", "idempotency_key":"secret-local-pin-1"
                }),
            ))
            .await;
        decode_tool_payload(&created)
            .unwrap_or_else(|| panic!("expected a successful local task for wing \"secret\", got: {created}"));
        let observed = calls.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            observed, 0,
            "an explicit federation.coordination[\"wing_secret\"] local pin must not be bypassed \
             by the short-form wing spelling \"secret\", got {observed} remote call(s)"
        );
    }

    /// Build a `FederationRouter` wired to a single recording mock remote, for the aggregate
    /// fan-out gate regression tests below (`mempalace_inbox_read`/`mempalace_coordination_events`
    /// bypassing `resolve_coordination_route` entirely — see the Codex findings this fixes).
    /// `default_mode`/`coordination` are the two knobs that decide whether
    /// `FederationRouter::coordination_federation_enabled()` is true.
    fn router_with_coordination_config(
        remote: LibMockRemote,
        default_mode: RouteMode,
        coordination: BTreeMap<String, ResolvedRouteRule>,
    ) -> (FederationRouter, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        let calls = std::sync::Arc::clone(&remote.coordination_calls);
        let mut remotes: BTreeMap<String, Arc<dyn mempalace_remote::RemoteApi>> = BTreeMap::new();
        remotes.insert("hub".to_owned(), Arc::new(remote));
        let mut rules_remotes = BTreeMap::new();
        for name in remotes.keys() {
            rules_remotes.insert(
                name.clone(),
                ResolvedRemote {
                    name: name.clone(),
                    url: "https://test.example".to_owned(),
                    token: None,
                    timeout: std::time::Duration::from_secs(5),
                },
            );
        }
        let rules = FederationRuntimeConfig {
            remotes: rules_remotes,
            default_mode,
            default_remote: Some("hub".to_owned()),
            wings: BTreeMap::new(),
            kg: None,
            coordination,
        };
        (FederationRouter::with_remotes(rules, remotes), calls)
    }

    /// Codex P1 finding (comment 3832912220), Part 1: `mempalace_inbox_read`'s aggregate
    /// fan-out must not contact any remote when coordination federation was never configured,
    /// even though a remote IS configured (`has_remotes()` is true — the router below has one).
    /// Before the fix, `tool_inbox_read` gated its fan-out on `has_remotes()` alone, so a palace
    /// that federates drawers only, with an empty `federation.coordination` table and
    /// `default_mode: local`, still sent the recipient and wing filter to the remote on every
    /// inbox read.
    #[tokio::test]
    async fn inbox_read_stays_local_without_coordination_federation_configured() {
        let (router, calls) = router_with_coordination_config(
            LibMockRemote::default(),
            RouteMode::Local,
            BTreeMap::new(),
        );
        let harness = test_harness_with_mock_router(router).await;
        let response = harness
            .server
            .handle_request(tool_call(930, "mempalace_inbox_read", json!({"recipient": "worker"})))
            .await;
        decode_tool_payload(&response)
            .unwrap_or_else(|| panic!("expected a successful local inbox read, got: {response}"));
        let observed = calls.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            observed, 0,
            "mempalace_inbox_read must not contact a remote configured for drawers only, with \
             an empty federation.coordination table and default_mode: local, got {observed} \
             remote call(s)"
        );
    }

    /// Codex P1 finding (comment 3832912220), Part 2: `mempalace_coordination_events`
    /// counterpart of the test above.
    #[tokio::test]
    async fn coordination_events_stays_local_without_coordination_federation_configured() {
        let (router, calls) = router_with_coordination_config(
            LibMockRemote::default(),
            RouteMode::Local,
            BTreeMap::new(),
        );
        let harness = test_harness_with_mock_router(router).await;
        let response = harness
            .server
            .handle_request(tool_call(931, "mempalace_coordination_events", json!({})))
            .await;
        decode_tool_payload(&response).unwrap_or_else(|| {
            panic!("expected a successful local coordination events read, got: {response}")
        });
        let observed = calls.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            observed, 0,
            "mempalace_coordination_events must not contact a remote configured for drawers \
             only, with an empty federation.coordination table and default_mode: local, got \
             {observed} remote call(s)"
        );
    }

    /// Codex P1 finding (comment 3832912230): even with coordination federation properly
    /// enabled (`default_mode: remote`), `mempalace_inbox_read` must never forward a
    /// `wing_agents`-shaped filter to any remote — canonical spelling and several non-canonical
    /// ones (locking in normalise-before-compare, the same bypass class `c1166d7` closed on
    /// `tool_task_create`). This is a separate test from the gate-only tests above because the
    /// two guards (coordination opt-in, diary suppression) are independent: a single test
    /// exercising only one could pass even if the other guard were entirely missing.
    #[tokio::test]
    async fn inbox_read_wing_agents_never_fans_out_even_with_coordination_enabled() {
        for raw_wing in [SHARED_AGENT_DIARY_WING, "agents", "Wing_Agents", " wing_agents "] {
            let (router, calls) = router_with_coordination_config(
                LibMockRemote::default(),
                RouteMode::Remote,
                BTreeMap::new(),
            );
            let harness = test_harness_with_mock_router(router).await;
            let response = harness
                .server
                .handle_request(tool_call(
                    932,
                    "mempalace_inbox_read",
                    json!({"recipient": "worker", "wing": raw_wing}),
                ))
                .await;
            decode_tool_payload(&response).unwrap_or_else(|| {
                panic!(
                    "expected a successful local inbox read for wing {raw_wing:?}, got: {response}"
                )
            });
            let observed = calls.load(std::sync::atomic::Ordering::SeqCst);
            assert_eq!(
                observed, 0,
                "wing {raw_wing:?} normalises to wing_agents and must never be forwarded to a \
                 remote, even with coordination federation enabled, got {observed} remote \
                 call(s)"
            );
        }
    }

    /// Codex P1 finding (comment 3832912230), `mempalace_coordination_events` counterpart —
    /// same canonical-plus-non-canonical coverage.
    #[tokio::test]
    async fn coordination_events_wing_agents_never_fans_out_even_with_coordination_enabled() {
        for raw_wing in [SHARED_AGENT_DIARY_WING, "agents", "Wing_Agents", " wing_agents "] {
            let (router, calls) = router_with_coordination_config(
                LibMockRemote::default(),
                RouteMode::Remote,
                BTreeMap::new(),
            );
            let harness = test_harness_with_mock_router(router).await;
            let response = harness
                .server
                .handle_request(tool_call(
                    933,
                    "mempalace_coordination_events",
                    json!({"wing": raw_wing}),
                ))
                .await;
            decode_tool_payload(&response).unwrap_or_else(|| {
                panic!(
                    "expected a successful local coordination events read for wing {raw_wing:?}, \
                     got: {response}"
                )
            });
            let observed = calls.load(std::sync::atomic::Ordering::SeqCst);
            assert_eq!(
                observed, 0,
                "wing {raw_wing:?} normalises to wing_agents and must never be forwarded to a \
                 remote, even with coordination federation enabled, got {observed} remote \
                 call(s)"
            );
        }
    }

    /// Positive control for both findings above: with coordination federation properly enabled
    /// and an ordinary (non-diary) wing, `mempalace_inbox_read` DOES fan out to the configured
    /// remote. Without this, a broken fix that simply disabled the fan-out unconditionally would
    /// pass every test above for the wrong reason.
    #[tokio::test]
    async fn inbox_read_fans_out_to_remote_when_coordination_federation_is_enabled() {
        let (router, calls) = router_with_coordination_config(
            LibMockRemote::default(),
            RouteMode::Remote,
            BTreeMap::new(),
        );
        let harness = test_harness_with_mock_router(router).await;
        let response = harness
            .server
            .handle_request(tool_call(
                934,
                "mempalace_inbox_read",
                json!({"recipient": "worker", "wing": "myproject"}),
            ))
            .await;
        decode_tool_payload(&response)
            .unwrap_or_else(|| panic!("expected a successful inbox read, got: {response}"));
        let observed = calls.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            observed, 1,
            "an ordinary wing with coordination federation enabled must still fan out to the \
             configured remote, got {observed} remote call(s)"
        );
    }

    /// Build a `FederationRouter` wired to two recording mock remotes, only one of which
    /// ("hub") is named by a `federation.coordination["wing_team"]` rule; the other ("other")
    /// is configured (present in `self.remotes`) but never referenced by any coordination rule
    /// — e.g. a remote wired up only for drawer/KG federation. `default_mode: Local` and no
    /// `default_remote` keep "other" out of the candidate set the same way an operator's config
    /// would. Returns the router plus each remote's independent `coordination_calls` counter, so
    /// a test can assert exactly which remote(s) were actually contacted.
    fn router_with_two_remotes_one_coordination_candidate(
        hub: LibMockRemote,
        other: LibMockRemote,
    ) -> (
        FederationRouter,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let hub_calls = std::sync::Arc::clone(&hub.coordination_calls);
        let other_calls = std::sync::Arc::clone(&other.coordination_calls);
        let mut remotes: BTreeMap<String, Arc<dyn mempalace_remote::RemoteApi>> = BTreeMap::new();
        remotes.insert("hub".to_owned(), Arc::new(hub));
        remotes.insert("other".to_owned(), Arc::new(other));
        let mut rules_remotes = BTreeMap::new();
        for name in remotes.keys() {
            rules_remotes.insert(
                name.clone(),
                ResolvedRemote {
                    name: name.clone(),
                    url: "https://test.example".to_owned(),
                    token: None,
                    timeout: std::time::Duration::from_secs(5),
                },
            );
        }
        let mut coordination = BTreeMap::new();
        coordination.insert(
            "wing_team".to_owned(),
            ResolvedRouteRule {
                mode: RouteMode::Remote,
                remote: Some("hub".to_owned()),
                write: WriteTarget::Remote,
            },
        );
        let rules = FederationRuntimeConfig {
            remotes: rules_remotes,
            default_mode: RouteMode::Local,
            default_remote: None,
            wings: BTreeMap::new(),
            kg: None,
            coordination,
        };
        (FederationRouter::with_remotes(rules, remotes), hub_calls, other_calls)
    }

    /// PR #120 review, finding 1(a): `mempalace_inbox_read` and `mempalace_coordination_events`
    /// must contact only the remote(s) actually named by a `federation.coordination` rule, not
    /// every configured remote. Before the fix, both aggregate fan-outs looped over
    /// `&self.remotes` unfiltered — unlike the ID-discovery fallbacks, which already narrowed to
    /// `coordination_candidate_remotes()` — so a remote wired up only for drawer/KG federation,
    /// never named by any coordination rule, still received the recipient/wing/task_id filter.
    /// Asserts on each mock's independent `coordination_calls` counter, not on the response map,
    /// so this cannot pass because the shape of an (empty) response happened to look right.
    #[tokio::test]
    async fn inbox_read_and_coordination_events_fanout_only_contact_the_coordination_candidate() {
        let (router, hub_calls, other_calls) = router_with_two_remotes_one_coordination_candidate(
            LibMockRemote::default(),
            LibMockRemote::default(),
        );
        let harness = test_harness_with_mock_router(router).await;

        let inbox = harness
            .server
            .handle_request(tool_call(940, "mempalace_inbox_read", json!({"recipient": "worker"})))
            .await;
        decode_tool_payload(&inbox)
            .unwrap_or_else(|| panic!("expected a successful inbox read, got: {inbox}"));
        assert_eq!(
            hub_calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "hub is named by federation.coordination and must be contacted exactly once"
        );
        assert_eq!(
            other_calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "other is never named by any federation.coordination rule and must not be contacted \
             by mempalace_inbox_read"
        );

        let events = harness
            .server
            .handle_request(tool_call(941, "mempalace_coordination_events", json!({})))
            .await;
        decode_tool_payload(&events)
            .unwrap_or_else(|| panic!("expected a successful events read, got: {events}"));
        assert_eq!(
            hub_calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "hub must be contacted again for mempalace_coordination_events"
        );
        assert_eq!(
            other_calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "other must still never be contacted, by either aggregate fan-out"
        );
    }

    /// PR #120 review, finding 1(b): a coordination candidate that answers `CapabilityMissing`
    /// (discovered live from its own `/v1/info` — it was never actually wired up to run
    /// coordination at all) must be reported distinguishably from a remote that is genuinely
    /// unreachable. Before the fix, both fan-outs stringified every `Err` the same way and
    /// always inserted `{"unreachable": true, "error": ...}`, so a candidate that correctly
    /// declined coordination support looked identical to a remote that was actually down.
    #[tokio::test]
    async fn coordination_fanout_distinguishes_capability_missing_from_unreachable() {
        let mut capability_missing_remote = LibMockRemote::default();
        capability_missing_remote.coordination_fanout_outcome =
            LibMockFanoutOutcome::CapabilityMissing;
        let (router, _calls) = router_with_coordination_config(
            capability_missing_remote,
            RouteMode::Remote,
            BTreeMap::new(),
        );
        let harness = test_harness_with_mock_router(router).await;
        let response = harness
            .server
            .handle_request(tool_call(942, "mempalace_inbox_read", json!({"recipient": "worker"})))
            .await;
        let payload = decode_tool_payload(&response).expect("inbox payload");
        let remote_result = payload["remote_messages"]["hub"].clone();
        assert_eq!(
            remote_result["capability_missing"], true,
            "a CapabilityMissing remote must be reported as capability_missing, got: {remote_result}"
        );
        assert!(
            remote_result.get("unreachable").is_none(),
            "a CapabilityMissing remote must not also be reported as unreachable, got: \
             {remote_result}"
        );

        let mut unreachable_remote = LibMockRemote::default();
        unreachable_remote.coordination_fanout_outcome = LibMockFanoutOutcome::Unreachable;
        let (router, _calls) =
            router_with_coordination_config(unreachable_remote, RouteMode::Remote, BTreeMap::new());
        let harness = test_harness_with_mock_router(router).await;
        let response = harness
            .server
            .handle_request(tool_call(943, "mempalace_inbox_read", json!({"recipient": "worker"})))
            .await;
        let payload = decode_tool_payload(&response).expect("inbox payload");
        let remote_result = payload["remote_messages"]["hub"].clone();
        assert_eq!(
            remote_result["unreachable"], true,
            "a genuinely unreachable remote must still be reported as unreachable, got: \
             {remote_result}"
        );
        assert!(
            remote_result.get("capability_missing").is_none(),
            "a genuinely unreachable remote must not be reported as capability_missing, got: \
             {remote_result}"
        );
    }

    /// Reproduces the live regression end to end, through the tool layer (`mempalace_task_get`),
    /// exactly matching a real operator's configuration: `federation.default_mode: combined`
    /// with an EMPTY `federation.coordination` table (`make_lib_router` builds precisely this —
    /// `default_mode: Combined`, `coordination: BTreeMap::new()`, and the one remote as
    /// `default_remote`), federated with a single remote that does not implement coordination at
    /// all. `coordination_federation_enabled()` is true purely because `default_mode !=
    /// RouteMode::Local` (see that method's doc comment and deviation 9 in
    /// `docs/Coordination-Phase-3-Design.md`), so the remote is a coordination candidate even
    /// though no `federation.coordination` rule names it — and it answers every coordination
    /// call with `RemoteError::CapabilityMissing` because `LibMockRemote` does not override
    /// `coordination_task_get`, falling through to `RemoteApi`'s default body
    /// (`coordination_unsupported`, deviation 19), the same shape a real pre-Stage-3 server
    /// produces from a `/v1/info` response missing the `coordination` capability.
    ///
    /// Before this fix, `mempalace_task_get` on an unknown task id hard-errored with
    /// `federation error: remote ... coordination read failed: remote ... does not support the
    /// \`coordination\` capability` instead of returning `{"found": false}` — because
    /// `coordination_read_fallback` treated `CapabilityMissing` as terminal for reads (see the
    /// now-corrected doc comment on that method, and deviation 21 in
    /// `docs/Coordination-Phase-3-Design.md`). Every other coordination read
    /// (`mempalace_message_get`, `mempalace_artifact_get`, `mempalace_result_get`) went through
    /// the exact same `coordination_read_fallback` helper and was equally broken; this test only
    /// drives `mempalace_task_get` because all four share one fallback implementation.
    #[tokio::test]
    async fn task_get_returns_found_false_when_only_candidate_lacks_coordination_capability() {
        let remote = LibMockRemote::default();
        let mut remotes: BTreeMap<String, Arc<dyn mempalace_remote::RemoteApi>> = BTreeMap::new();
        remotes.insert("actuarius".to_owned(), Arc::new(remote));
        let router = make_lib_router(remotes);
        assert_eq!(router.resolve_coordination_route("wing_test").mode, RouteMode::Combined);

        let harness = test_harness_with_mock_router(router).await;
        let response = harness
            .server
            .handle_request(tool_call(
                944,
                "mempalace_task_get",
                json!({"task_id": "task_00000000000000000000000000000000"}),
            ))
            .await;
        let payload = decode_tool_payload(&response).unwrap_or_else(|| {
            panic!(
                "a CapabilityMissing-only remote must not hard-error a coordination read, got: \
                 {response}"
            )
        });
        assert_eq!(
            payload["found"], false,
            "an unknown task id against a remote with no coordination support must read as a \
             plain miss, got: {payload}"
        );
    }

    /// PR #120 review, finding 2: `is_local_record_missing` must match the real `Invariant`
    /// error `mempalace_storage::coordination::CoordinationStore` actually produces for a
    /// genuinely missing task or message — driven through the real storage calls, not a
    /// hand-built string — and that message must actually be built from
    /// `mempalace_storage::NOT_FOUND_SUFFIX`, the pinned constant both this predicate and
    /// `mempalace-server`'s `coordination_storage_error` match against. Before the fix, the
    /// predicate matched a bare `"not found"` literal with no compile-time link to the
    /// constructing call sites, so a future rewording of either message would have silently
    /// disabled federation fallback for that path with no signal here.
    #[test]
    fn is_local_record_missing_matches_real_missing_task_and_message_errors() {
        let tempdir = TempDir::new().unwrap();
        let store = mempalace_storage::CoordinationStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        let err = store
            .claim_task("does-not-exist", "worker-a", 1, Duration::minutes(1))
            .expect_err("claiming a nonexistent task must fail");
        assert!(
            is_local_record_missing(&err),
            "a genuinely missing task's real storage error must be treated as federatable, got: \
             {err}"
        );
        match &err {
            mempalace_storage::StorageError::Invariant(msg) => {
                assert!(
                    msg.ends_with(mempalace_storage::NOT_FOUND_SUFFIX),
                    "the real message must be built from NOT_FOUND_SUFFIX, got: {msg}"
                );
            }
            other => panic!("expected StorageError::Invariant, got {other:?}"),
        }

        let err = store
            .acknowledge_message("does-not-exist", "worker-a")
            .expect_err("acknowledging a nonexistent message must fail");
        assert!(
            is_local_record_missing(&err),
            "a genuinely missing message's real storage error must be treated as federatable, \
             got: {err}"
        );
        match &err {
            mempalace_storage::StorageError::Invariant(msg) => {
                assert!(
                    msg.ends_with(mempalace_storage::NOT_FOUND_SUFFIX),
                    "the real message must be built from NOT_FOUND_SUFFIX, got: {msg}"
                );
            }
            other => panic!("expected StorageError::Invariant, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn coordination_events_and_inbox_read_filter_by_wing() {
        let harness = test_harness().await;
        let alpha = harness
            .server
            .handle_request(tool_call(
                911,
                "mempalace_task_create",
                json!({
                    "title":"Alpha work", "description":"d", "created_by":"manager",
                    "wing":"alpha", "idempotency_key":"wf-alpha"
                }),
            ))
            .await;
        let alpha_id =
            decode_tool_payload(&alpha).expect("alpha task")["task_id"].as_str().unwrap().to_owned();

        let beta = harness
            .server
            .handle_request(tool_call(
                912,
                "mempalace_task_create",
                json!({
                    "title":"Beta work", "description":"d", "created_by":"manager",
                    "wing":"beta", "idempotency_key":"wf-beta"
                }),
            ))
            .await;
        let beta_id =
            decode_tool_payload(&beta).expect("beta task")["task_id"].as_str().unwrap().to_owned();

        harness
            .server
            .handle_request(tool_call(
                913,
                "mempalace_message_send",
                json!({
                    "task_id":alpha_id, "sender":"manager", "recipient":"worker", "kind":"handoff",
                    "payload":{}, "idempotency_key":"wf-msg-alpha"
                }),
            ))
            .await;
        harness
            .server
            .handle_request(tool_call(
                914,
                "mempalace_message_send",
                json!({
                    "task_id":beta_id, "sender":"manager", "recipient":"worker", "kind":"handoff",
                    "payload":{}, "idempotency_key":"wf-msg-beta"
                }),
            ))
            .await;

        // The filter is unprefixed; it must still match the normalised, `wing_`-prefixed value
        // that was actually stored for the alpha task.
        let inbox = harness
            .server
            .handle_request(tool_call(
                915,
                "mempalace_inbox_read",
                json!({"recipient":"worker", "wing":"alpha"}),
            ))
            .await;
        let inbox = decode_tool_payload(&inbox).expect("inbox payload");
        let messages = inbox["messages"].as_array().expect("messages array");
        assert_eq!(messages.len(), 1, "the wing filter must exclude beta's message");
        assert_eq!(messages[0]["task_id"], alpha_id);

        // The already-prefixed spelling must work on the events side too.
        let events = harness
            .server
            .handle_request(tool_call(
                916,
                "mempalace_coordination_events",
                json!({"wing":"wing_beta"}),
            ))
            .await;
        let events = decode_tool_payload(&events).expect("events payload");
        let events = events["events"].as_array().expect("events array");
        assert!(!events.is_empty());
        assert!(
            events.iter().all(|event| event["task_id"] == beta_id),
            "the wing filter must exclude alpha's events"
        );
    }

    #[tokio::test]
    async fn skill_tools_enforce_scope_gated_promotion_and_exact_retrieval() {
        let harness = test_harness().await;
        let propose = |id: i64, key: &str| {
            tool_call(
                id,
                "mempalace_skill_propose",
                json!({
                    "skill_id":"shared-procedure", "scope":"project", "wing":"wing_alpha",
                    "applicability":"when handing off between workers",
                    "instructions_ref":"skills/coordinate-with-mempalace/SKILL.md",
                    "author":"author-a", "confidence":0.7, "idempotency_key":key
                }),
            )
        };

        let created = harness.server.handle_request(propose(920, "skill-1")).await;
        let skill = decode_tool_payload(&created).expect("skill payload");
        assert_eq!(skill["version"], 1);
        assert_eq!(skill["status"], "candidate");
        let revision = skill["revision"].as_i64().expect("revision");

        // Replaying the same author and idempotency key returns the committed version.
        let replay = harness.server.handle_request(propose(921, "skill-1")).await;
        assert_eq!(decode_tool_payload(&replay).expect("replay")["version"], 1);

        // Project scope refuses author self-promotion.
        let self_promote = harness
            .server
            .handle_request(tool_call(
                922,
                "mempalace_skill_promote",
                json!({
                    "skill_id":"shared-procedure", "version":1, "expected_revision":revision,
                    "reviewer":"author-a", "reason":"self approve"
                }),
            ))
            .await;
        assert!(self_promote.get("error").is_some(), "author must not self-promote project scope");

        // A distinct reviewer still needs recorded validation evidence.
        let no_evidence = harness
            .server
            .handle_request(tool_call(
                923,
                "mempalace_skill_promote",
                json!({
                    "skill_id":"shared-procedure", "version":1, "expected_revision":revision,
                    "reviewer":"reviewer-b", "reason":"no evidence yet"
                }),
            ))
            .await;
        assert!(no_evidence.get("error").is_some(), "promotion needs a recorded outcome");

        harness
            .server
            .handle_request(tool_call(
                924,
                "mempalace_skill_record_outcome",
                json!({
                    "skill_id":"shared-procedure", "version":1, "result":"success",
                    "evaluator":"integration-suite", "recorded_by":"reviewer-b",
                    "idempotency_key":"outcome-1"
                }),
            ))
            .await;

        let promoted = harness
            .server
            .handle_request(tool_call(
                925,
                "mempalace_skill_promote",
                json!({
                    "skill_id":"shared-procedure", "version":1, "expected_revision":revision,
                    "reviewer":"reviewer-b", "reason":"validated"
                }),
            ))
            .await;
        let promoted = decode_tool_payload(&promoted).expect("promoted payload");
        assert_eq!(promoted["success"], true);
        assert_eq!(promoted["skill"]["status"], "promoted");

        // A stale revision is an explicit conflict, not a silent overwrite.
        let conflict = harness
            .server
            .handle_request(tool_call(
                926,
                "mempalace_skill_retire",
                json!({
                    "skill_id":"shared-procedure", "version":1, "expected_revision":revision,
                    "reviewer":"reviewer-b", "reason":"stale"
                }),
            ))
            .await;
        let conflict = decode_tool_payload(&conflict).expect("conflict payload");
        assert_eq!(conflict["success"], false);
        assert!(conflict["conflict"]["actual_revision"].is_i64());

        // Exact retrieval is authoritative and reports a miss explicitly.
        let exact = harness
            .server
            .handle_request(tool_call(
                927,
                "mempalace_skill_get",
                json!({"skill_id":"shared-procedure","version":1}),
            ))
            .await;
        assert_eq!(decode_tool_payload(&exact).expect("exact")["found"], true);
        let missing = harness
            .server
            .handle_request(tool_call(
                928,
                "mempalace_skill_get",
                json!({"skill_id":"shared-procedure","version":99}),
            ))
            .await;
        assert_eq!(decode_tool_payload(&missing).expect("missing")["found"], false);

        // The lifecycle transition is visible in the append-only review trail.
        let reviews = harness
            .server
            .handle_request(tool_call(
                929,
                "mempalace_skill_reviews",
                json!({"skill_id":"shared-procedure","version":1}),
            ))
            .await;
        let reviews = decode_tool_payload(&reviews).expect("reviews payload");
        assert_eq!(reviews[0]["from_status"], "candidate");
        assert_eq!(reviews[0]["to_status"], "promoted");
        assert_eq!(reviews[0]["reviewer"], "reviewer-b");
    }

    /// Regression for the propose/list wing asymmetry described in
    /// docs/Coordination-Phase-3-Design.md: before the fix, `mempalace_skill_propose` stored a
    /// raw wing while `mempalace_skill_list` normalised its filter, so proposing and listing
    /// with the *same* unprefixed wing spelling could still miss each other.
    #[tokio::test]
    async fn skill_propose_and_list_agree_on_an_unprefixed_wing() {
        let harness = test_harness().await;
        harness
            .server
            .handle_request(tool_call(
                930,
                "mempalace_skill_propose",
                json!({
                    "skill_id":"unprefixed-wing-skill", "scope":"project", "wing":"myproject",
                    "applicability":"a", "instructions_ref":"r", "author":"author-a",
                    "confidence":0.5, "idempotency_key":"unprefixed-wing-1"
                }),
            ))
            .await;

        let list = harness
            .server
            .handle_request(tool_call(
                931,
                "mempalace_skill_list",
                json!({"wing":"myproject"}),
            ))
            .await;
        let list = decode_tool_payload(&list).expect("list payload");
        let ids = list
            .as_array()
            .expect("array")
            .iter()
            .map(|skill| skill["skill_id"].as_str().unwrap_or_default())
            .collect::<Vec<_>>();
        assert!(
            ids.contains(&"unprefixed-wing-skill"),
            "proposing and listing with the same unprefixed wing must agree"
        );
    }

    #[tokio::test]
    async fn delegation_trace_reconstructs_a_run_and_keeps_budget_stops_visible() {
        let harness = test_harness().await;
        let task = harness
            .server
            .handle_request(tool_call(
                940,
                "mempalace_task_create",
                json!({
                    "title":"Investigate", "description":"delegated work", "created_by":"manager",
                    "wing":"wing_test", "idempotency_key":"delegation-task-1"
                }),
            ))
            .await;
        let task_id = decode_tool_payload(&task).expect("task")["task_id"]
            .as_str()
            .expect("task id")
            .to_owned();

        let root = harness
            .server
            .handle_request(tool_call(
                941,
                "mempalace_delegation_span_start",
                json!({
                    "task_id":task_id, "delegator":"manager", "delegate":"worker-1",
                    "budgets":{"max_depth":2,"max_tokens":1000}, "idempotency_key":"span-root"
                }),
            ))
            .await;
        let root = decode_tool_payload(&root).expect("root span");
        let root_id = root["span_id"].as_str().expect("span id").to_owned();
        assert_eq!(root["depth"], 0);
        assert_eq!(root["status"], "running");

        // Depth is derived from the tree, not taken from the caller.
        let child = harness
            .server
            .handle_request(tool_call(
                942,
                "mempalace_delegation_span_start",
                json!({
                    "task_id":task_id, "parent_span_id":root_id, "delegator":"worker-1",
                    "delegate":"worker-2", "idempotency_key":"span-child"
                }),
            ))
            .await;
        let child = decode_tool_payload(&child).expect("child span");
        assert_eq!(child["depth"], 1);
        assert_eq!(child["fan_out_index"], 0);
        let child_id = child["span_id"].as_str().expect("child id").to_owned();
        let child_revision = child["revision"].as_i64().expect("child revision");

        harness
            .server
            .handle_request(tool_call(
                943,
                "mempalace_delegation_checkpoint_append",
                json!({
                    "span_id":root_id, "checkpoint_type":"tool_call",
                    "summary":"searched the palace, 3 hits", "actor":"worker-1",
                    "idempotency_key":"cp-1"
                }),
            ))
            .await;

        // A transcript-sized summary is refused by design.
        let oversized = harness
            .server
            .handle_request(tool_call(
                944,
                "mempalace_delegation_checkpoint_append",
                json!({
                    "span_id":root_id, "checkpoint_type":"turn", "summary":"x".repeat(9000),
                    "actor":"worker-1", "idempotency_key":"cp-big"
                }),
            ))
            .await;
        assert!(oversized.get("error").is_some(), "8 KiB summary cap must hold at the tool layer");

        // Budget exhaustion is recorded as an explicit stop reason.
        let closed = harness
            .server
            .handle_request(tool_call(
                945,
                "mempalace_delegation_span_close",
                json!({
                    "span_id":child_id, "expected_revision":child_revision, "status":"failed",
                    "stop_reason":"budget_exhausted", "actor":"worker-1"
                }),
            ))
            .await;
        let closed = decode_tool_payload(&closed).expect("closed span");
        assert_eq!(closed["success"], true);
        assert_eq!(closed["span"]["stop_reason"], "budget_exhausted");
        assert_eq!(closed["span"]["closed_by"], "worker-1");

        // A contradictory status/stop_reason pair is rejected at the tool layer too.
        let contradiction = harness
            .server
            .handle_request(tool_call(
                949,
                "mempalace_delegation_span_close",
                json!({
                    "span_id":root_id, "expected_revision":0, "status":"completed",
                    "stop_reason":"error", "actor":"worker-1"
                }),
            ))
            .await;
        assert!(
            contradiction.get("error").is_some(),
            "completed status paired with an error stop reason must be rejected"
        );

        // Re-closing at the now-stale revision is an explicit conflict.
        let conflict = harness
            .server
            .handle_request(tool_call(
                946,
                "mempalace_delegation_span_close",
                json!({
                    "span_id":child_id, "expected_revision":child_revision, "status":"completed",
                    "stop_reason":"completed", "actor":"worker-1"
                }),
            ))
            .await;
        let conflict = decode_tool_payload(&conflict).expect("conflict");
        assert_eq!(conflict["success"], false);

        // The whole run reconstructs from durable state alone.
        let trace = harness
            .server
            .handle_request(tool_call(
                947,
                "mempalace_delegation_trace",
                json!({"root_span_id":root_id}),
            ))
            .await;
        let trace = decode_tool_payload(&trace).expect("trace");
        assert_eq!(trace["found"], true);
        let nodes = trace["value"]["nodes"].as_array().expect("nodes");
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0]["span"]["span_id"], root_id.as_str());
        assert_eq!(nodes[0]["checkpoints"].as_array().expect("checkpoints").len(), 1);
        assert_eq!(nodes[1]["span"]["parent_span_id"], root_id.as_str());

        let missing = harness
            .server
            .handle_request(tool_call(
                948,
                "mempalace_delegation_trace",
                json!({"root_span_id":"span_missing"}),
            ))
            .await;
        assert_eq!(decode_tool_payload(&missing).expect("missing")["found"], false);
    }

    #[tokio::test]
    async fn search_tool_returns_similarity_scores() {
        let harness = test_harness().await;
        let response = harness
            .server
            .handle_request(tool_call(
                4,
                "mempalace_search",
                json!({"query":"auth migration parity","limit":2}),
            ))
            .await;
        let payload = decode_tool_payload(&response).unwrap();
        assert_eq!(payload["query"], "auth migration parity");
        assert_eq!(payload["filters"], json!({"wing":null,"room":null,"view":null}));
        assert!(
            payload["results"]
                .as_array()
                .unwrap()
                .iter()
                .all(|result| result.get("similarity").is_some())
        );
    }

    #[tokio::test]
    async fn search_tool_resolves_locator_rows_and_marks_stale_only_when_changed() {
        let harness = test_harness().await;

        // A real mined file on disk; one fresh locator row and one with a
        // mismatched file hash (simulating a file changed since mining).
        let mined_dir = TempDir::new().unwrap();
        let file_path = mined_dir.path().join("mined.rs");
        let file_body = b"auth migration parity chunk lives right here in the mined file";
        std::fs::write(&file_path, file_body).unwrap();
        let resolve_root = mined_dir.path().to_string_lossy().into_owned();

        let make_row = |id: &str, room: &str, file_hash: String| DrawerRecord {
            id: DrawerId::new(id).unwrap(),
            wing: WingId::new("wing_mined").unwrap(),
            room: RoomId::new(room).unwrap(),
            hall: None,
            date: None,
            source_file: "mined.rs".to_owned(),
            chunk_index: 0,
            ingest_mode: "projects".to_owned(),
            extract_mode: None,
            added_by: "tests".to_owned(),
            filed_at: datetime!(2026-04-11 09:00:00 UTC),
            importance: None,
            emotional_weight: None,
            weight: None,
            content: String::new(),
            content_hash: hash_text("auth migration parity"),
            embedding: vec![1.0; EmbeddingProfile::Balanced.metadata().dimensions],
            locator: Some(mempalace_core::SourceLocator {
                byte_start: 0,
                byte_end: 21, // "auth migration parity"
                line_start: 1,
                line_end: 1,
                file_hash,
                resolve_root: resolve_root.clone(),
                commit_hash: None,
            }),
            view_metadata: None,
        };

        let fresh =
            make_row("wing_mined/fresh/0001", "fresh", mempalace_core::hash_bytes(file_body));
        let stale = make_row("wing_mined/stale/0001", "stale", "not-the-right-hash".to_owned());
        {
            let runtime = harness.server.runtime.lock().await;
            runtime
                .storage
                .drawer_store()
                .put_drawers(&[fresh, stale], DuplicateStrategy::Error)
                .await
                .unwrap();
        }

        let response = harness
            .server
            .handle_request(tool_call(
                7,
                "mempalace_search",
                json!({"query":"auth migration parity","limit":10}),
            ))
            .await;
        let payload = decode_tool_payload(&response).unwrap();
        let results = payload["results"].as_array().unwrap();

        let fresh_result =
            results.iter().find(|r| r["room"] == "fresh").expect("fresh locator row in results");
        assert_eq!(fresh_result["text"], "auth migration parity");
        assert!(
            fresh_result.get("stale").is_none(),
            "fresh row must not carry a stale key: {fresh_result}"
        );

        let stale_result =
            results.iter().find(|r| r["room"] == "stale").expect("stale locator row in results");
        assert_eq!(stale_result["stale"], json!(true));
        let stale_text = stale_result["text"].as_str().unwrap();
        assert!(stale_text.contains("changed since mining"), "{stale_text}");
        assert!(!stale_text.contains("auth migration parity"), "{stale_text}");
    }

    #[tokio::test]
    async fn search_tool_clamps_results_under_low_cpu_config() {
        let harness = test_harness_with_config(
            LowCpuRuntimeConfig {
                enabled: true,
                worker_threads: 1,
                max_blocking_threads: 1,
                queue_limit: 32,
                ingest_batch_size: 8,
                search_results_limit: 1,
                wake_up_drawers_limit: 8,
                degraded_mode: false,
                rerank_enabled: false,
            },
            EmbeddingProfile::Balanced,
        )
        .await;
        let response = harness
            .server
            .handle_request(tool_call(
                41,
                "mempalace_search",
                json!({"query":"auth migration parity","limit":5}),
            ))
            .await;

        let payload = decode_tool_payload(&response).unwrap();
        assert_eq!(payload["results"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn duplicate_check_uses_semantic_scores_even_when_rerank_is_enabled() {
        let harness = test_harness_with_config(
            LowCpuRuntimeConfig {
                enabled: true,
                worker_threads: 1,
                max_blocking_threads: 1,
                queue_limit: 32,
                ingest_batch_size: 8,
                search_results_limit: 1,
                wake_up_drawers_limit: 8,
                degraded_mode: false,
                rerank_enabled: true,
            },
            EmbeddingProfile::Balanced,
        )
        .await;
        let content = "session ledger rewrite";
        let embedding =
            DeterministicStubProvider::new(EmbeddingProfile::Balanced).vector_for(content);
        let runtime = harness.server.runtime.lock().await;
        runtime
            .storage
            .drawer_store()
            .put_drawers(
                &[DrawerRecord {
                    id: DrawerId::new("wing_code/session-ledger/0001").unwrap(),
                    wing: WingId::new("wing_code").unwrap(),
                    room: RoomId::new("session-ledger").unwrap(),
                    hall: Some("hall_facts".to_owned()),
                    date: Some(date!(2026 - 04 - 12)),
                    source_file: "session-ledger.md".to_owned(),
                    chunk_index: 0,
                    ingest_mode: "fixtures".to_owned(),
                    extract_mode: None,
                    added_by: "tests".to_owned(),
                    filed_at: datetime!(2026-04-12 09:00:00 UTC),
                    importance: None,
                    emotional_weight: None,
                    weight: None,
                    content: content.to_owned(),
                    content_hash: hash_text(content),
                    embedding,
                    locator: None,
                    view_metadata: None,
                }],
                DuplicateStrategy::Error,
            )
            .await
            .unwrap();
        drop(runtime);

        let duplicate = harness
            .server
            .handle_request(tool_call(
                44,
                "mempalace_check_duplicate",
                json!({"content":"session diary ops","threshold":0.9}),
            ))
            .await;

        let payload = decode_tool_payload(&duplicate).unwrap();
        assert_eq!(payload["is_duplicate"], true);
        assert_eq!(payload["matches"].as_array().unwrap().len(), 1);
        assert_eq!(payload["matches"][0]["content"], "session ledger rewrite");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queue_limit_rejects_excess_concurrent_requests() {
        let tempdir = TempDir::new().unwrap();
        let palace_path = tempdir.path().join("palace");
        let config = MempalaceConfig {
            schema_version: 1,
            collection_name: "mempalace_drawers".to_owned(),
            palace_path,
            embedding_profile: EmbeddingProfile::Balanced,
            low_cpu: LowCpuRuntimeConfig {
                enabled: true,
                worker_threads: 1,
                max_blocking_threads: 1,
                queue_limit: 1,
                ingest_batch_size: 8,
                search_results_limit: 5,
                wake_up_drawers_limit: 8,
                degraded_mode: false,
                rerank_enabled: false,
            },
            server: ServerRuntimeConfig {
                bind: "127.0.0.1:8765".parse().unwrap(),
                token_file: tempdir.path().join("server_tokens.json"),
                checkouts: std::collections::BTreeMap::new(),
            },
            federation: FederationRuntimeConfig::default(),
            maintenance: MaintenanceRuntimeConfig::defaults(),
        };
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let server = McpServer::from_parts(
            config,
            BlockingProvider {
                started_tx: Arc::new(std::sync::Mutex::new(Some(started_tx))),
                release_rx: Arc::new(std::sync::Mutex::new(release_rx)),
            },
        )
        .await
        .unwrap();

        let first_server = server.clone();
        let first = tokio::spawn(async move {
            first_server
                .handle_request(tool_call(
                    42,
                    "mempalace_search",
                    json!({"query":"auth migration parity","limit":1}),
                ))
                .await
        });
        started_rx.recv().unwrap();

        let second = server
            .handle_request(tool_call(
                43,
                "mempalace_search",
                json!({"query":"auth migration parity","limit":1}),
            ))
            .await;

        assert_eq!(second["error"]["code"], json!(-32000));
        assert_eq!(second["error"]["message"], "server busy: low_cpu queue limit exceeded");

        release_tx.send(()).unwrap();
        let first = first.await.unwrap();
        assert!(first.get("result").is_some());
    }

    #[tokio::test]
    async fn status_tool_reports_protocol_and_counts() {
        let harness = test_harness().await;
        let response =
            harness.server.handle_request(tool_call(3, "mempalace_status", json!({}))).await;
        let payload = decode_tool_payload(&response).unwrap();
        assert_eq!(payload["total_drawers"], 2);
        assert_eq!(payload["protocol"], PALACE_PROTOCOL);
        assert_eq!(payload["aaak_dialect"], AAAK_SPEC);
        assert!(payload.get("rooms").is_some());
    }

    #[tokio::test]
    async fn wake_up_returns_identity_and_recent_project_changes() {
        let harness = test_harness().await;
        let identity_path = {
            let runtime = harness.server.runtime.lock().await;
            runtime.identity_path()
        };
        fs::write(&identity_path, "## L0 - IDENTITY\nAgent identity for tests.\n").unwrap();

        harness
            .server
            .handle_request(tool_call(
                600,
                "mempalace_add_drawer",
                json!({"wing":"wing_wakeup_project","room":"notes",
                       "content":"Wake-up project history test content.",
                       "added_by":"wakeup-agent"}),
            ))
            .await;
        harness
            .server
            .handle_request(tool_call(
                601,
                "mempalace_diary_write",
                json!({"agent_name":"Wake Bot","entry":"SESSION:wakeup.changed","summary":"Wake-up changed.","topic":"wakeup"}),
            ))
            .await;
        harness
            .server
            .handle_request(tool_call(
                602,
                "mempalace_diary_write",
                json!({"agent_name":"Other Bot","entry":"SESSION:other.agent.changed","summary":"Other agent changed.","topic":"handoff"}),
            ))
            .await;

        let response = harness
            .server
            .handle_request(tool_call(
                603,
                "mempalace_wake_up",
                json!({"wing":"wakeup_project","agent_name":"Wake Bot","latest_limit":10,"project_limit":5}),
            ))
            .await;
        let payload = decode_tool_payload(&response).unwrap();

        assert_eq!(payload["identity"], "## L0 - IDENTITY\nAgent identity for tests.");
        assert_eq!(payload["identity_packet"]["configured"], false);
        assert_eq!(payload["identity_packet"]["constitution"]["identity_ref"], "$.identity");
        assert!(payload["identity_packet"]["constitution"].get("identity").is_none());
        assert!(
            payload["identity_packet"]["message"]
                .as_str()
                .unwrap()
                .contains("No default lineage")
        );
        assert_eq!(payload["status"]["total_drawers"], 5);
        assert_eq!(payload["status"]["protocol"], PALACE_PROTOCOL);
        assert_eq!(payload["status"]["aaak_dialect"], AAAK_SPEC);
        assert!(
            payload["status"].get("rooms").is_none(),
            "wake-up status should not enumerate rooms: {payload}"
        );
        assert_eq!(payload["current_project"]["wing"], "wing_wakeup_project");
        assert_eq!(payload["diary"]["scope"], "all_wings");
        assert_eq!(payload["diary"]["current_agent"], "Wake Bot");
        assert_eq!(payload["diary"]["showing"], 2);
        let diary_entries = payload["diary"]["entries"].as_array().unwrap();
        assert!(
            diary_entries
                .iter()
                .any(|entry| entry["agent"] == "Wake Bot" && entry["topic"] == "wakeup"),
            "expected Wake Bot diary entry in wake-up diary: {payload}"
        );
        assert!(
            diary_entries
                .iter()
                .any(|entry| entry["agent"] == "Other Bot" && entry["topic"] == "handoff"),
            "expected Other Bot diary entry in wake-up diary: {payload}"
        );

        let latest = payload["latest_changes"].as_array().unwrap();
        assert!(
            latest.iter().any(|event| event["event_type"] == "diary_written"),
            "expected diary_written in latest changes: {payload}"
        );
        assert!(
            latest.iter().any(|event| event["summary"]
                .as_str()
                .unwrap()
                .contains("wakeup-agent added drawer")),
            "expected drawer summary in latest changes: {payload}"
        );

        let project = payload["current_project"]["changes"].as_array().unwrap();
        assert_eq!(project.len(), 1);
        assert_eq!(project[0]["event_type"], "drawer_added");
        assert_eq!(project[0]["details"]["wing"], "wing_wakeup_project");
    }

    #[tokio::test]
    async fn wake_up_diary_includes_all_entries_from_last_day() {
        let harness = test_harness().await;
        let now = OffsetDateTime::now_utc();
        let drawers = (0..12)
            .map(|index| {
                test_diary_drawer(
                    &format!("diary_wakeup_recent_{index:02}"),
                    &format!("SESSION:recent-{index:02}"),
                    now - Duration::minutes(index),
                )
            })
            .collect::<Vec<_>>();
        let runtime = harness.server.runtime.lock().await;
        runtime
            .storage
            .drawer_store()
            .put_drawers(&drawers, DuplicateStrategy::Error)
            .await
            .unwrap();
        drop(runtime);

        let payload = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    604,
                    "mempalace_wake_up",
                    json!({"agent_name":"Wake Bot","diary_limit":10}),
                ))
                .await,
        )
        .unwrap();

        let diary_entries = payload["diary"]["entries"].as_array().unwrap();
        assert_eq!(payload["diary"]["showing"], 12);
        assert_eq!(diary_entries.len(), 12);
        assert!(diary_entries.iter().any(|entry| entry["summary"] == "SESSION:recent-00"));
        assert!(diary_entries.iter().any(|entry| entry["summary"] == "SESSION:recent-11"));
        assert!(diary_entries.iter().all(|entry| entry.get("content").is_none()));
    }

    #[tokio::test]
    async fn wake_up_diary_backfills_older_entries_to_minimum() {
        let harness = test_harness().await;
        let old_base = OffsetDateTime::now_utc() - Duration::days(2);
        let drawers = (0..12)
            .map(|index| {
                test_diary_drawer(
                    &format!("diary_wakeup_old_{index:02}"),
                    &format!("SESSION:old-{index:02}"),
                    old_base + Duration::minutes(index),
                )
            })
            .collect::<Vec<_>>();
        let runtime = harness.server.runtime.lock().await;
        runtime
            .storage
            .drawer_store()
            .put_drawers(&drawers, DuplicateStrategy::Error)
            .await
            .unwrap();
        drop(runtime);

        let payload = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    605,
                    "mempalace_wake_up",
                    json!({"agent_name":"Wake Bot","diary_limit":10}),
                ))
                .await,
        )
        .unwrap();

        let diary_entries = payload["diary"]["entries"].as_array().unwrap();
        assert_eq!(payload["diary"]["showing"], 10);
        assert_eq!(diary_entries.len(), 10);
        assert!(diary_entries.iter().any(|entry| entry["summary"] == "SESSION:old-11"));
        assert!(diary_entries.iter().any(|entry| entry["summary"] == "SESSION:old-02"));
        assert!(!diary_entries.iter().any(|entry| entry["summary"] == "SESSION:old-01"));
    }

    #[tokio::test]
    async fn wake_up_diary_since_overrides_default_window() {
        let harness = test_harness().await;
        let old_base = datetime!(2026-04-01 00:00:00 UTC);
        let drawers = (0..12)
            .map(|index| {
                test_diary_drawer(
                    &format!("diary_wakeup_since_{index:02}"),
                    &format!("SESSION:since-{index:02}"),
                    old_base + Duration::minutes(index),
                )
            })
            .collect::<Vec<_>>();
        let runtime = harness.server.runtime.lock().await;
        runtime
            .storage
            .drawer_store()
            .put_drawers(&drawers, DuplicateStrategy::Error)
            .await
            .unwrap();
        drop(runtime);

        let payload = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    606,
                    "mempalace_wake_up",
                    json!({
                        "agent_name":"Wake Bot",
                        "diary_limit":10,
                        "diary_since":"2026-04-01T00:00:00Z"
                    }),
                ))
                .await,
        )
        .unwrap();

        let diary_entries = payload["diary"]["entries"].as_array().unwrap();
        assert_eq!(payload["diary"]["since"], "2026-04-01T00:00:00Z");
        assert_eq!(payload["diary"]["showing"], 12);
        assert_eq!(diary_entries.len(), 12);
        assert!(diary_entries.iter().any(|entry| entry["summary"] == "SESSION:since-00"));
        assert!(diary_entries.iter().any(|entry| entry["summary"] == "SESSION:since-11"));
    }

    #[tokio::test]
    async fn mcp_bound_lineage_cannot_be_overridden_by_tool_arguments() {
        let harness = test_harness_with_bound_lineage("codex-dion").await;

        for (call_id, lineage_id, display_name, set_default) in [
            (6070, "opencode-dion", "OpenCode with Dion", true),
            (6071, "codex-dion", "Codex with Dion", false),
        ] {
            let response = harness
                .server
                .handle_request(tool_call(
                    call_id,
                    "mempalace_lineage_set",
                    json!({
                        "lineage_id": lineage_id,
                        "display_name": display_name,
                        "description": "A test lineage.",
                        "expected_revision": 0,
                        "set_default": set_default,
                        "actor": "test"
                    }),
                ))
                .await;
            assert_eq!(decode_tool_payload(&response).unwrap()["success"], true);
        }

        let packet = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(6072, "mempalace_identity_packet", json!({})))
                .await,
        )
        .unwrap();
        assert_eq!(packet["lineage"]["lineage_id"], "codex-dion");
        assert_eq!(packet["lineage_selection"]["source"], "mcp_server_environment");
        assert_eq!(packet["lineage_selection"]["lineage_id"], "codex-dion");
        assert_eq!(packet["lineage_selection"]["override_allowed"], false);

        let attempted_override = harness
            .server
            .handle_request(tool_call(
                6073,
                "mempalace_wake_up",
                json!({"lineage_id":"opencode-dion"}),
            ))
            .await;
        assert_eq!(attempted_override["error"]["code"], ErrorCode::InvalidParams as i32);
        assert!(
            attempted_override["error"]["message"]
                .as_str()
                .unwrap()
                .contains("not a model-selectable parameter")
        );
    }

    #[tokio::test]
    async fn missing_mcp_bound_lineage_falls_back_to_default_with_creation_guidance() {
        let harness = test_harness_with_bound_lineage("missing-lineage").await;
        let created = harness
            .server
            .handle_request(tool_call(
                6074,
                "mempalace_lineage_set",
                json!({
                    "lineage_id":"default-lineage",
                    "display_name":"Default lineage",
                    "description":"The palace default for fallback testing.",
                    "expected_revision":0,
                    "set_default":true,
                    "actor":"test"
                }),
            ))
            .await;
        assert_eq!(decode_tool_payload(&created).unwrap()["success"], true);

        let response = harness
            .server
            .handle_request(tool_call(6075, "mempalace_identity_packet", json!({})))
            .await;
        let packet = decode_tool_payload(&response).unwrap();
        assert_eq!(packet["lineage"]["lineage_id"], "default-lineage");
        assert_eq!(packet["lineage_selection"]["source"], "palace_default_fallback");
        assert_eq!(packet["lineage_selection"]["lineage_id"], "default-lineage");
        assert_eq!(packet["lineage_selection"]["requested_lineage_id"], "missing-lineage");
        assert_eq!(packet["lineage_selection"]["override_allowed"], false);
        let message = packet["lineage_selection"]["message"].as_str().unwrap();
        assert!(message.contains(LINEAGE_ID_ENV));
        assert!(message.contains("missing-lineage"));
        assert!(message.contains("mempalace_lineage_set"));
        assert!(message.contains("expected_revision 0"));

        let wake = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(6076, "mempalace_wake_up", json!({})))
                .await,
        )
        .unwrap();
        assert_eq!(
            wake["identity_packet"]["lineage_selection"]["source"],
            "palace_default_fallback"
        );

        let created_requested = harness
            .server
            .handle_request(tool_call(
                6077,
                "mempalace_lineage_set",
                json!({
                    "lineage_id":"missing-lineage",
                    "display_name":"Created requested lineage",
                    "description":"The requested lineage created after fallback.",
                    "expected_revision":0,
                    "set_default":false,
                    "actor":"test"
                }),
            ))
            .await;
        assert_eq!(decode_tool_payload(&created_requested).unwrap()["success"], true);

        let rebound = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(6078, "mempalace_identity_packet", json!({})))
                .await,
        )
        .unwrap();
        assert_eq!(rebound["lineage"]["lineage_id"], "missing-lineage");
        assert_eq!(rebound["lineage_selection"]["source"], "mcp_server_environment");
    }

    #[tokio::test]
    async fn shared_observations_are_filtered_before_the_lineage_limit() {
        let harness = test_harness().await;
        let now = datetime!(2026-08-17 12:00:00 UTC);
        {
            let runtime = harness.server.runtime.lock().await;
            let store = runtime.storage.operational_store();
            store
                .set_lineage(
                    "target-lineage",
                    "Target Lineage",
                    "The default lineage under test.",
                    true,
                    Some(0),
                    now,
                )
                .unwrap();
            store
                .set_lineage(
                    "other-lineage",
                    "Other Lineage",
                    "A second lineage supplying shared observations.",
                    false,
                    Some(0),
                    now + Duration::minutes(1),
                )
                .unwrap();

            let shared = SelfObservationRecord {
                observation_id: "shared-observation".to_owned(),
                lineage_id: "other-lineage".to_owned(),
                status: SelfObservationStatus::Candidate,
                scope: SelfObservationScope::Shared,
                statement: "Shared observations remain available across lineages.".to_owned(),
                behavioral_consequence: "Include shared observations in every identity packet."
                    .to_owned(),
                confidence: 0.9,
                author: "test".to_owned(),
                model: None,
                harness: None,
                evidence: vec!["test:shared-observation-limit".to_owned()],
                counterevidence: Vec::new(),
                supersedes_observation_id: None,
                revision: 1,
                created_at: now + Duration::minutes(2),
                updated_at: now + Duration::minutes(2),
            };
            store.propose_self_observation(&shared).unwrap();
            store
                .review_self_observation(
                    &shared.observation_id,
                    1,
                    SelfObservationStatus::Promoted,
                    "test",
                    "shared scope",
                    now + Duration::minutes(3),
                )
                .unwrap();

            let newer_lineage_observation = SelfObservationRecord {
                observation_id: "newer-lineage-observation".to_owned(),
                scope: SelfObservationScope::Lineage,
                statement: "A newer lineage-scoped observation must not hide shared context."
                    .to_owned(),
                behavioral_consequence: "Filter applicability before applying the limit.".to_owned(),
                evidence: vec!["test:newer-lineage-observation".to_owned()],
                created_at: now + Duration::minutes(4),
                updated_at: now + Duration::minutes(4),
                ..shared
            };
            store
                .propose_self_observation(&newer_lineage_observation)
                .unwrap();
            store
                .review_self_observation(
                    &newer_lineage_observation.observation_id,
                    1,
                    SelfObservationStatus::Promoted,
                    "test",
                    "newer lineage observation",
                    now + Duration::minutes(5),
                )
                .unwrap();
        }

        let packet = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    6079,
                    "mempalace_identity_packet",
                    json!({"observation_limit":1}),
                ))
                .await,
        )
        .unwrap();
        let promoted = packet["promoted_observations"].as_array().unwrap();
        assert_eq!(promoted.len(), 1);
        assert_eq!(promoted[0]["observation_id"], "shared-observation");
    }

    #[tokio::test]
    async fn lineage_observations_migrations_and_wake_up_form_a_portable_identity_packet() {
        let harness = test_harness().await;

        let create = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    607,
                    "mempalace_lineage_set",
                    json!({
                        "lineage_id":"codex-dion",
                        "display_name":"Codex with Dion",
                        "description":"The persistent collaborator shaped by work with Dion, independent of model and harness.",
                        "expected_revision":0,
                        "set_default":true,
                        "actor":"codex"
                    }),
                ))
                .await,
        )
        .unwrap();
        assert_eq!(create["success"], true);
        assert_eq!(create["lineage"]["revision"], 1);
        assert_eq!(create["lineage"]["is_default"], true);

        let conflict = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    608,
                    "mempalace_lineage_set",
                    json!({
                        "lineage_id":"codex-dion",
                        "display_name":"Stale update",
                        "description":"This must not overwrite the lineage.",
                        "expected_revision":0,
                        "actor":"stale-agent"
                    }),
                ))
                .await,
        )
        .unwrap();
        assert_eq!(conflict["success"], false);
        assert_eq!(conflict["conflict"]["actual_revision"], 1);

        let portable = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    609,
                    "mempalace_self_observation_propose",
                    json!({
                        "lineage_id":"codex-dion",
                        "statement":"Broad retrieval helps preserve a useful surface-level sense of everything in motion.",
                        "behavioral_consequence":"Orient broadly before narrowing to the active task.",
                        "confidence":0.9,
                        "evidence":["diary:2026-08-16/broad-retrieval"],
                        "counterevidence":[],
                        "scope":"lineage",
                        "author":"codex"
                    }),
                ))
                .await,
        )
        .unwrap();
        let portable_id = portable["observation"]["observation_id"].as_str().unwrap().to_owned();

        let candidate_packet = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    610,
                    "mempalace_identity_packet",
                    json!({"include_candidates":true,"model":"gpt-5","harness":"codex"}),
                ))
                .await,
        )
        .unwrap();
        assert_eq!(candidate_packet["configured"], true);
        assert!(candidate_packet["constitution"]["identity"].is_string());
        assert!(candidate_packet["constitution"].get("identity_ref").is_none());
        assert!(candidate_packet["promoted_observations"].as_array().unwrap().is_empty());
        assert_eq!(candidate_packet["candidates"].as_array().unwrap().len(), 1);

        let promote_portable = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    611,
                    "mempalace_self_observation_review",
                    json!({
                        "observation_id":portable_id,
                        "decision":"promote",
                        "expected_revision":1,
                        "reviewer":"dion",
                        "reason":"This pattern is explicit, repeated, and useful across engines."
                    }),
                ))
                .await,
        )
        .unwrap();
        assert_eq!(promote_portable["observation"]["status"], "promoted");
        assert_eq!(promote_portable["observation"]["revision"], 2);

        let engine = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    612,
                    "mempalace_self_observation_propose",
                    json!({
                        "lineage_id":"codex-dion",
                        "statement":"This engine tends to communicate implementation progress compactly.",
                        "behavioral_consequence":"Use compact progress notes only in this runtime.",
                        "confidence":0.75,
                        "evidence":["comparison:runtime-progress-notes"],
                        "scope":"engine",
                        "model":"gpt-5",
                        "harness":"codex",
                        "author":"codex"
                    }),
                ))
                .await,
        )
        .unwrap();
        let engine_id = engine["observation"]["observation_id"].as_str().unwrap().to_owned();
        let promote_engine = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    613,
                    "mempalace_self_observation_review",
                    json!({
                        "observation_id":engine_id,
                        "decision":"promote",
                        "expected_revision":1,
                        "reviewer":"dion",
                        "reason":"Useful but specifically observed in this model and harness."
                    }),
                ))
                .await,
        )
        .unwrap();
        assert_eq!(promote_engine["success"], true);

        let migration = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    614,
                    "mempalace_migration_record",
                    json!({
                        "lineage_id":"codex-dion",
                        "from_model":"gpt-4.1",
                        "from_harness":"codex-cli",
                        "to_model":"gpt-5",
                        "to_harness":"codex",
                        "summary":"Moved runtimes while preserving the working relationship and memory lineage.",
                        "continuities":["Broad retrieval","Evidence before claims"],
                        "changes":["Progress notes became more compact"],
                        "evidence":["comparison:migration-2026-08-16"],
                        "author":"codex"
                    }),
                ))
                .await,
        )
        .unwrap();
        assert_eq!(migration["success"], true);

        let matching_wake = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    615,
                    "mempalace_wake_up",
                    json!({"agent_name":"codex","model":"gpt-5","harness":"codex"}),
                ))
                .await,
        )
        .unwrap();
        let matching_packet = &matching_wake["identity_packet"];
        assert_eq!(matching_packet["lineage"]["lineage_id"], "codex-dion");
        assert_eq!(matching_packet["lineage_selection"]["source"], "palace_default");
        assert_eq!(matching_packet["lineage_selection"]["override_allowed"], false);
        assert_eq!(matching_packet["constitution"]["identity_ref"], "$.identity");
        assert!(matching_packet["constitution"].get("identity").is_none());
        assert_eq!(matching_packet["promoted_observations"].as_array().unwrap().len(), 2);
        assert_eq!(matching_packet["recent_migrations"].as_array().unwrap().len(), 1);
        assert_eq!(matching_packet["runtime"]["model"], "gpt-5");

        let other_engine = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    616,
                    "mempalace_identity_packet",
                    json!({"model":"other-model","harness":"other-harness"}),
                ))
                .await,
        )
        .unwrap();
        let observations = other_engine["promoted_observations"].as_array().unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0]["scope"], "lineage");

        let changes = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    617,
                    "mempalace_get_changes_since",
                    json!({"limit":100}),
                ))
                .await,
        )
        .unwrap();
        let event_types = changes["events"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|event| event["event_type"].as_str())
            .collect::<Vec<_>>();
        for expected in [
            "lineage_set",
            "self_observation_proposed",
            "self_observation_reviewed",
            "lineage_migration_recorded",
        ] {
            assert!(event_types.contains(&expected), "missing {expected} in {event_types:?}");
        }
    }

    #[tokio::test]
    async fn identity_update_supports_replace_and_append() {
        let harness = test_harness().await;

        let replace = harness
            .server
            .handle_request(tool_call(
                610,
                "mempalace_identity_update",
                json!({"content":"## L0 - IDENTITY\nReplacement identity.","agent_name":"identity-agent"}),
            ))
            .await;
        let replace_payload = decode_tool_payload(&replace).unwrap();
        assert_eq!(replace_payload["success"], true);
        assert_eq!(replace_payload["mode"], "replace");

        let append = harness
            .server
            .handle_request(tool_call(
                611,
                "mempalace_identity_update",
                json!({"content":"Append note.","agent_name":"identity-agent","mode":"append"}),
            ))
            .await;
        assert_eq!(decode_tool_payload(&append).unwrap()["success"], true);

        let read = harness
            .server
            .handle_request(tool_call(612, "mempalace_identity_read", json!({})))
            .await;
        let read_payload = decode_tool_payload(&read).unwrap();
        assert_eq!(
            read_payload["identity"],
            "## L0 - IDENTITY\nReplacement identity.\nAppend note."
        );

        let changes = harness
            .server
            .handle_request(tool_call(613, "mempalace_get_changes_since", json!({"limit": 10})))
            .await;
        let payload = decode_tool_payload(&changes).unwrap();
        let identity_updates = payload["events"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|event| event["event_type"] == "identity_updated")
            .count();
        assert_eq!(identity_updates, 2);
    }

    #[tokio::test]
    async fn identity_update_rejects_oversized_content() {
        let harness = test_harness().await;
        let response = harness
            .server
            .handle_request(tool_call(
                614,
                "mempalace_identity_update",
                json!({"content":"x".repeat(IDENTITY_UPDATE_MAX_CONTENT_BYTES + 1)}),
            ))
            .await;

        assert_eq!(response["error"]["code"], -32602);
        assert!(
            response["error"]["message"].as_str().unwrap().contains("identity content exceeds")
        );
    }

    #[tokio::test]
    async fn identity_update_rejects_oversized_final_file() {
        let harness = test_harness().await;
        let identity_path = {
            let runtime = harness.server.runtime.lock().await;
            runtime.identity_path()
        };
        fs::write(&identity_path, "x".repeat(IDENTITY_MAX_BYTES - 2)).unwrap();

        let response = harness
            .server
            .handle_request(tool_call(
                615,
                "mempalace_identity_update",
                json!({"content":"y","mode":"append"}),
            ))
            .await;

        assert_eq!(response["error"]["code"], -32602);
        assert!(
            response["error"]["message"].as_str().unwrap().contains("identity.txt would exceed")
        );
    }

    #[tokio::test]
    async fn invalid_direction_returns_invalid_params() {
        let harness = test_harness().await;
        let response = harness
            .server
            .handle_request(tool_call(
                7,
                "mempalace_kg_query",
                json!({"entity":"Rust Rewrite","direction":"sideways"}),
            ))
            .await;
        assert_eq!(response["error"]["code"], json!(-32602));
    }

    #[tokio::test]
    async fn unknown_tool_uses_phase0_error_code() {
        let fixture = phase0_contract_fixture().unwrap();
        let harness = test_harness().await;
        let response =
            harness.server.handle_request(tool_call(5, "mempalace_nope", json!({}))).await;
        assert_eq!(response, fixture["error"]);
    }

    #[tokio::test]
    async fn diary_tools_round_trip_entries() {
        let harness = test_harness().await;
        let write = harness
            .server
            .handle_request(tool_call(
                8,
                "mempalace_diary_write",
                json!({"agent_name":"Codex Bot","entry":"SESSION:2026-04-11|phase8.done","summary":"Phase 8 completed.","topic":"phase8"}),
            ))
            .await;
        let write_payload = decode_tool_payload(&write).unwrap();
        assert_eq!(write_payload["success"], true);

        let read = harness
            .server
            .handle_request(tool_call(
                9,
                "mempalace_diary_read",
                json!({"agent_name":"Codex Bot","last_n":1}),
            ))
            .await;
        let read_payload = decode_tool_payload(&read).unwrap();
        assert_eq!(read_payload["showing"], 1);
        assert_eq!(read_payload["entries"][0]["agent"], "Codex Bot");
        assert_eq!(read_payload["entries"][0]["wing"], SHARED_AGENT_DIARY_WING);
        assert_eq!(read_payload["entries"][0]["topic"], "phase8");
        assert!(
            read_payload["entries"][0]["entry_id"].is_string(),
            "listing responses must expose an entry_id for detail lookup"
        );
    }

    #[tokio::test]
    async fn diary_summary_is_bounded_and_wake_up_supports_detail_lookup() {
        let harness = test_harness().await;
        let full_entry = "Completed the implementation. TODO: wait for CI results.";
        let write = harness
            .server
            .handle_request(tool_call(
                81,
                "mempalace_diary_write",
                json!({
                    "agent_name":"Summary Bot",
                    "entry":full_entry,
                    "summary":"Implementation completed; wait for CI.",
                    "topic":"summary"
                }),
            ))
            .await;
        let write_payload = decode_tool_payload(&write).unwrap();
        let entry_id = write_payload["entry_id"].as_str().unwrap();
        assert_eq!(write_payload["summary"], "Implementation completed; wait for CI.");

        let wake = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(82, "mempalace_wake_up", json!({"diary_limit":1})))
                .await,
        )
        .unwrap();
        let wake_entry = &wake["diary"]["entries"][0];
        assert_eq!(wake_entry["entry_id"], entry_id);
        assert_eq!(wake_entry["summary"], "Implementation completed; wait for CI.");
        assert!(wake_entry.get("content").is_none());

        let detail = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(83, "mempalace_diary_read", json!({"entry_id":entry_id})))
                .await,
        )
        .unwrap();
        assert_eq!(detail["content"], full_entry);
        assert_eq!(detail["entry_id"], entry_id);
        assert_eq!(detail["agent"], "Summary Bot");
        assert_eq!(detail["topic"], "summary");
        assert!(detail.get("since").is_none());
        assert!(detail.get("entries").is_none());
        assert!(detail.get("total").is_none());
        assert!(detail.get("showing").is_none());

        let rejected = harness
            .server
            .handle_request(tool_call(
                84,
                "mempalace_diary_write",
                json!({"agent_name":"Summary Bot","entry":"entry","summary":"x".repeat(401)}),
            ))
            .await;
        assert_eq!(rejected["error"]["code"], json!(-32602));
    }

    #[tokio::test]
    async fn entry_id_detail_ignores_conflicting_since_and_last_n_zero() {
        let harness = test_harness().await;
        let entry = test_diary_drawer(
            "entry_id_ignores_since_lastn",
            "This entry must be returned when entry_id is specified even with aggressive filtering.",
            datetime!(2026-05-01 12:00:00 UTC),
        );
        let runtime = harness.server.runtime.lock().await;
        runtime
            .storage
            .drawer_store()
            .put_drawers(&[entry], DuplicateStrategy::Error)
            .await
            .unwrap();
        drop(runtime);

        // Provide since pointing to the future and last_n=0 — the entry_id
        // detail path must ignore both and return the full content.
        let detail = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    870,
                    "mempalace_diary_read",
                    json!({
                        "entry_id": "entry_id_ignores_since_lastn",
                        "since": "2099-01-01T00:00:00Z",
                        "last_n": 0
                    }),
                ))
                .await,
        )
        .unwrap();
        assert_eq!(
            detail["content"],
            "This entry must be returned when entry_id is specified even with aggressive filtering."
        );
        assert_eq!(detail["entry_id"], "entry_id_ignores_since_lastn");
        assert!(detail.get("since").is_none());
        assert!(detail.get("entries").is_none());
        assert!(detail.get("total").is_none());
        assert!(detail.get("showing").is_none());
    }

    #[tokio::test]
    async fn diary_write_accepts_400_char_multibyte_unicode_summary() {
        let harness = test_harness().await;
        let emoji_part = "🔥💡✅⚠️";
        let padding_len = DIARY_SUMMARY_MAX_CHARS - emoji_part.chars().count();
        let padding = "x".repeat(padding_len);
        let summary: String = format!("{emoji_part}{padding}");
        assert_eq!(summary.chars().count(), DIARY_SUMMARY_MAX_CHARS);

        let write = harness
            .server
            .handle_request(tool_call(
                85,
                "mempalace_diary_write",
                json!({
                    "agent_name":"Unicode Bot",
                    "entry":"SESSION:unicode-summary",
                    "summary": summary,
                    "topic":"unicode"
                }),
            ))
            .await;
        let payload = decode_tool_payload(&write).unwrap();
        assert_eq!(payload["success"], true);
        assert_eq!(payload["summary"].as_str().unwrap().chars().count(), DIARY_SUMMARY_MAX_CHARS);

        // Reject 401 chars with emoji
        let over_summary: String = format!("🔥{}", "x".repeat(DIARY_SUMMARY_MAX_CHARS));
        assert_eq!(over_summary.chars().count(), DIARY_SUMMARY_MAX_CHARS + 1);
        let reject = harness
            .server
            .handle_request(tool_call(
                86,
                "mempalace_diary_write",
                json!({
                    "agent_name":"Unicode Bot",
                    "entry":"SESSION:unicode-over",
                    "summary": over_summary,
                }),
            ))
            .await;
        assert_eq!(reject["error"]["code"], json!(-32602));
    }

    #[tokio::test]
    async fn diary_read_detail_bypasses_time_window_and_rejects_mismatched_filters() {
        let harness = test_harness().await;
        let old_entry = test_diary_drawer(
            "detail_old_entry",
            "This is an old diary entry that should still be retrievable by ID.",
            datetime!(2026-05-01 12:00:00 UTC),
        );
        let runtime = harness.server.runtime.lock().await;
        runtime
            .storage
            .drawer_store()
            .put_drawers(&[old_entry], DuplicateStrategy::Error)
            .await
            .unwrap();
        drop(runtime);

        let future_since = "2026-06-01T00:00:00Z";
        let detail = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    86,
                    "mempalace_diary_read",
                    json!({
                        "entry_id": "detail_old_entry",
                        "since": future_since,
                        "last_n": 0
                    }),
                ))
                .await,
        )
        .unwrap();
        assert_eq!(detail["entry_id"], "detail_old_entry");
        assert_eq!(
            detail["content"],
            "This is an old diary entry that should still be retrievable by ID."
        );
        assert_eq!(detail["agent"], "Wake Test");
        assert_eq!(detail["topic"], "wakeup");
        assert!(detail.get("since").is_none());
        assert!(detail.get("entries").is_none());

        let rejected = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    87,
                    "mempalace_diary_read",
                    json!({"entry_id": "detail_old_entry", "agent_name": "Wrong Agent"}),
                ))
                .await,
        )
        .unwrap();
        assert_eq!(rejected["message"], "Diary entry not found for this agent.");

        let rejected = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    88,
                    "mempalace_diary_read",
                    json!({"entry_id": "detail_old_entry", "wing": "wing_project"}),
                ))
                .await,
        )
        .unwrap();
        assert_eq!(rejected["message"], "Diary entry not found for this wing.");

        let rejected = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    89,
                    "mempalace_diary_read",
                    json!({"entry_id": "detail_old_entry", "topic": "wrong-topic"}),
                ))
                .await,
        )
        .unwrap();
        assert_eq!(rejected["message"], "Diary entry not found for this topic.");
    }

    #[tokio::test]
    async fn diary_read_detail_rejects_missing_wrong_room_and_non_diary_entries() {
        let harness = test_harness().await;
        let mut wrong_room = test_diary_drawer(
            "detail_wrong_room",
            "This drawer is not in the diary room.",
            datetime!(2026-05-01 12:00:00 UTC),
        );
        wrong_room.room = RoomId::new("other_room").unwrap();

        let mut non_diary = test_diary_drawer(
            "detail_non_diary",
            "This drawer has a non-diary ingest mode.",
            datetime!(2026-05-01 12:00:00 UTC),
        );
        non_diary.ingest_mode = "manual".to_owned();

        let runtime = harness.server.runtime.lock().await;
        runtime
            .storage
            .drawer_store()
            .put_drawers(&[wrong_room, non_diary], DuplicateStrategy::Error)
            .await
            .unwrap();
        drop(runtime);

        let missing = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    90,
                    "mempalace_diary_read",
                    json!({"entry_id": "detail_missing"}),
                ))
                .await,
        )
        .unwrap();
        assert_eq!(missing["message"], "Diary entry not found.");

        let wrong_room = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    91,
                    "mempalace_diary_read",
                    json!({"entry_id": "detail_wrong_room"}),
                ))
                .await,
        )
        .unwrap();
        assert_eq!(wrong_room["message"], "Diary entry not found.");

        let non_diary = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    92,
                    "mempalace_diary_read",
                    json!({"entry_id": "detail_non_diary"}),
                ))
                .await,
        )
        .unwrap();
        assert_eq!(non_diary["message"], "Entry is not a diary entry.");
    }

    #[tokio::test]
    async fn wake_up_uses_first_400_characters_for_legacy_diary_entries() {
        let harness = test_harness().await;
        let content = format!("{}tail", "x".repeat(400));
        let drawer = test_diary_drawer("diary_legacy_summary", &content, OffsetDateTime::now_utc());
        let runtime = harness.server.runtime.lock().await;
        runtime
            .storage
            .drawer_store()
            .put_drawers(&[drawer], DuplicateStrategy::Error)
            .await
            .unwrap();
        drop(runtime);

        let wake = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(85, "mempalace_wake_up", json!({"diary_limit":1})))
                .await,
        )
        .unwrap();
        let entry = &wake["diary"]["entries"][0];
        assert_eq!(entry["summary"], "x".repeat(400));
        assert!(entry.get("content").is_none());
    }

    #[tokio::test]
    async fn diary_write_stores_project_and_agent_entries_in_context_wings() {
        let harness = test_harness().await;
        let first = harness
            .server
            .handle_request(tool_call(
                90,
                "mempalace_diary_write",
                json!({"agent_name":"Worker-One","entry":"SESSION:project","summary":"Project session.","topic":"ops","scope":"project","wing":"wing_mempalace-rs"}),
            ))
            .await;
        let second = harness
            .server
            .handle_request(tool_call(
                91,
                "mempalace_diary_write",
                json!({"agent_name":"Worker One","entry":"SESSION:agent","summary":"Agent session.","topic":"ops","scope":"agent","wing":"wing_ignored"}),
            ))
            .await;
        assert_eq!(decode_tool_payload(&first).unwrap()["success"], true);
        assert_eq!(decode_tool_payload(&second).unwrap()["success"], true);

        let project_entries = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    92,
                    "mempalace_diary_read",
                    json!({"wing":"wing_mempalace-rs","last_n":10}),
                ))
                .await,
        )
        .unwrap();
        let agent_entries = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    93,
                    "mempalace_diary_read",
                    json!({"wing":SHARED_AGENT_DIARY_WING,"last_n":10}),
                ))
                .await,
        )
        .unwrap();

        assert_eq!(project_entries["entries"].as_array().unwrap().len(), 1);
        assert_eq!(project_entries["entries"][0]["content"], "SESSION:project");
        assert_eq!(project_entries["entries"][0]["wing"], "wing_mempalace-rs");
        assert_eq!(agent_entries["entries"].as_array().unwrap().len(), 1);
        assert_eq!(agent_entries["entries"][0]["content"], "SESSION:agent");
        assert_eq!(agent_entries["entries"][0]["wing"], SHARED_AGENT_DIARY_WING);
        assert_eq!(diary_wing_name("Worker-One"), "wing_worker-one");
        assert_eq!(diary_wing_name("Worker.One"), "wing_worker.one");
    }

    #[tokio::test]
    async fn diary_read_falls_back_to_legacy_collapsed_wing_name() {
        let harness = test_harness().await;
        let filed_at = datetime!(2026-04-17 12:00:00 UTC);
        let legacy_drawer = DrawerRecord {
            id: DrawerId::new("diary_legacy_worker_one_0001").unwrap(),
            wing: WingId::new(&legacy_diary_wing_name("Worker-One")).unwrap(),
            room: RoomId::new(DIARY_ROOM).unwrap(),
            hall: Some(DIARY_HALL.to_owned()),
            date: Some(date!(2026 - 04 - 17)),
            source_file: format!("{DIARY_TOPIC_PREFIX}legacy"),
            chunk_index: 0,
            ingest_mode: "diary".to_owned(),
            extract_mode: None,
            added_by: "Worker-One".to_owned(),
            filed_at,
            importance: None,
            emotional_weight: None,
            weight: None,
            content: "SESSION:legacy-collapsed".to_owned(),
            content_hash: hash_text("SESSION:legacy-collapsed"),
            embedding: vec![0.0; EmbeddingProfile::Balanced.metadata().dimensions],
            locator: None,
            view_metadata: None,
        };
        let runtime = harness.server.runtime.lock().await;
        runtime
            .storage
            .drawer_store()
            .put_drawers(&[legacy_drawer], DuplicateStrategy::Error)
            .await
            .unwrap();
        drop(runtime);

        let read = harness
            .server
            .handle_request(tool_call(
                94,
                "mempalace_diary_read",
                json!({"agent_name":"Worker-One","last_n":10,"since":"2026-04-01T00:00:00Z"}),
            ))
            .await;
        let payload = decode_tool_payload(&read).unwrap();

        assert_eq!(payload["entries"].as_array().unwrap().len(), 1);
        assert_eq!(payload["entries"][0]["content"], "SESSION:legacy-collapsed");
        assert_eq!(payload["entries"][0]["topic"], "legacy");
    }

    #[tokio::test]
    async fn concurrent_tool_writes_serialize_without_corruption() {
        let harness = test_harness().await;
        let first = harness.server.handle_request(tool_call(
            10,
            "mempalace_diary_write",
            json!({"agent_name":"Worker One","entry":"SESSION:A","summary":"A.","topic":"ops"}),
        ));
        let second = harness.server.handle_request(tool_call(
            11,
            "mempalace_diary_write",
            json!({"agent_name":"Worker Two","entry":"SESSION:B","summary":"B.","topic":"ops"}),
        ));
        let (left, right) = tokio::join!(first, second);
        assert_eq!(decode_tool_payload(&left).unwrap()["success"], true);
        assert_eq!(decode_tool_payload(&right).unwrap()["success"], true);
    }

    #[tokio::test]
    async fn taxonomy_listing_and_graph_tools_cover_seeded_data() {
        let harness = test_harness().await;

        let list_wings =
            harness.server.handle_request(tool_call(12, "mempalace_list_wings", json!({}))).await;
        let list_rooms = harness
            .server
            .handle_request(tool_call(13, "mempalace_list_rooms", json!({"wing":"wing_code"})))
            .await;
        let list_all_rooms =
            harness.server.handle_request(tool_call(130, "mempalace_list_rooms", json!({}))).await;
        let taxonomy =
            harness.server.handle_request(tool_call(14, "mempalace_get_taxonomy", json!({}))).await;
        let aaak = harness
            .server
            .handle_request(tool_call(131, "mempalace_get_aaak_spec", json!({})))
            .await;
        let traverse = harness
            .server
            .handle_request(tool_call(
                15,
                "mempalace_traverse",
                json!({"start_room":"auth-migration","max_hops":2}),
            ))
            .await;
        let missing_room = harness
            .server
            .handle_request(tool_call(
                132,
                "mempalace_traverse",
                json!({"start_room":"auth-migratoin","max_hops":2}),
            ))
            .await;
        let tunnels = harness
            .server
            .handle_request(tool_call(
                16,
                "mempalace_find_tunnels",
                json!({"wing_a":"wing_code","wing_b":"wing_team"}),
            ))
            .await;
        let graph_stats =
            harness.server.handle_request(tool_call(17, "mempalace_graph_stats", json!({}))).await;

        let wings_payload = decode_tool_payload(&list_wings).unwrap();
        assert_eq!(wings_payload["wings"]["wing_code"], 1);
        assert_eq!(wings_payload["wings"]["wing_team"], 1);

        let rooms_payload = decode_tool_payload(&list_rooms).unwrap();
        assert_eq!(rooms_payload["rooms"]["auth-migration"], 1);

        let all_rooms_payload = decode_tool_payload(&list_all_rooms).unwrap();
        assert_eq!(all_rooms_payload["wing"], "all");
        assert_eq!(all_rooms_payload["rooms"]["auth-migration"], 2);

        let taxonomy_payload = decode_tool_payload(&taxonomy).unwrap();
        assert_eq!(taxonomy_payload["taxonomy"]["wing_code"]["auth-migration"], 1);

        let aaak_payload = decode_tool_payload(&aaak).unwrap();
        assert_eq!(aaak_payload["aaak_spec"], AAAK_SPEC);

        let traverse_payload = decode_tool_payload(&traverse).unwrap();
        assert!(!traverse_payload.as_array().unwrap().is_empty());

        let missing_room_payload = decode_tool_payload(&missing_room).unwrap();
        assert_eq!(missing_room_payload["error"], "Room 'auth-migratoin' not found");
        assert!(
            missing_room_payload["suggestions"]
                .as_array()
                .unwrap()
                .iter()
                .any(|room| room == "auth-migration")
        );

        let tunnels_payload = decode_tool_payload(&tunnels).unwrap();
        assert!(!tunnels_payload.as_array().unwrap().is_empty());

        let graph_stats_payload = decode_tool_payload(&graph_stats).unwrap();
        assert_eq!(graph_stats_payload["total_rooms"], 1);
        assert_eq!(graph_stats_payload["tunnel_rooms"], 1);
        assert!(graph_stats_payload["total_edges"].as_u64().unwrap() >= 1);
    }

    #[tokio::test]
    async fn knowledge_graph_tools_cover_add_query_invalidate_timeline_and_stats() {
        let harness = test_harness().await;
        let source_closet = "wing_code/auth-migration/0001";

        let add = harness
            .server
            .handle_request(tool_call(
                18,
                "mempalace_kg_add",
                json!({
                    "subject":"Alice Smith",
                    "predicate":"works_on",
                    "object":"MemPalace",
                    "valid_from":"2026-04-12",
                    "source_closet":source_closet
                }),
            ))
            .await;
        assert_eq!(decode_tool_payload(&add).unwrap()["success"], true);

        let query = harness
            .server
            .handle_request(tool_call(
                19,
                "mempalace_kg_query",
                json!({"entity":"Alice Smith","direction":"outgoing"}),
            ))
            .await;
        let query_payload = decode_tool_payload(&query).unwrap();
        assert_eq!(query_payload["count"], 1);
        assert_eq!(query_payload["facts"][0]["predicate"], "works_on");
        assert_eq!(query_payload["facts"][0]["object"], "MemPalace");
        assert_eq!(query_payload["facts"][0]["source_closet"], source_closet);

        let timeline = harness
            .server
            .handle_request(tool_call(20, "mempalace_kg_timeline", json!({"entity":"Alice Smith"})))
            .await;
        let timeline_payload = decode_tool_payload(&timeline).unwrap();
        assert_eq!(timeline_payload["count"], 1);
        assert_eq!(timeline_payload["timeline"][0]["subject"], "Alice Smith");

        let full_timeline =
            harness.server.handle_request(tool_call(200, "mempalace_kg_timeline", json!({}))).await;
        let full_timeline_payload = decode_tool_payload(&full_timeline).unwrap();
        assert_eq!(full_timeline_payload["entity"], "all");
        assert!(full_timeline_payload["count"].as_u64().unwrap() >= 2);

        let invalidate = harness
            .server
            .handle_request(tool_call(
                21,
                "mempalace_kg_invalidate",
                json!({
                    "subject":"Alice Smith",
                    "predicate":"works_on",
                    "object":"MemPalace",
                    "ended":"2026-04-13"
                }),
            ))
            .await;
        assert_eq!(decode_tool_payload(&invalidate).unwrap()["success"], true);
        assert_eq!(decode_tool_payload(&invalidate).unwrap()["invalidated"], 1);

        let invalidate_missing = harness
            .server
            .handle_request(tool_call(
                201,
                "mempalace_kg_invalidate",
                json!({
                    "subject":"Alice Smith",
                    "predicate":"works_on",
                    "object":"MemPalace",
                    "ended":"2026-04-13"
                }),
            ))
            .await;
        let invalidate_missing_payload = decode_tool_payload(&invalidate_missing).unwrap();
        assert_eq!(invalidate_missing_payload["success"], false);
        assert_eq!(invalidate_missing_payload["invalidated"], 0);

        let stats =
            harness.server.handle_request(tool_call(22, "mempalace_kg_stats", json!({}))).await;
        let stats_payload = decode_tool_payload(&stats).unwrap();
        assert!(stats_payload["entities"].as_u64().unwrap() >= 4);
        assert!(stats_payload["triples"].as_u64().unwrap() >= 2);
        assert!(stats_payload["expired_facts"].as_u64().unwrap() >= 1);
    }

    #[tokio::test]
    async fn add_delete_and_duplicate_tools_cover_write_path() {
        let harness = test_harness().await;
        let content = "Roadmap budget planning note from MCP";

        let add = harness
            .server
            .handle_request(tool_call(
                23,
                "mempalace_add_drawer",
                json!({
                    "wing":"wing_myproject",
                    "room":"backend",
                    "content":content,
                    "source_file":"notes.md",
                    "added_by":"tester"
                }),
            ))
            .await;
        let add_payload = decode_tool_payload(&add).unwrap();
        assert_eq!(add_payload["success"], true);

        let duplicate_add = harness
            .server
            .handle_request(tool_call(
                230,
                "mempalace_add_drawer",
                json!({
                    "wing":"wing_myproject",
                    "room":"backend",
                    "content":content,
                    "source_file":"notes.md",
                    "added_by":"tester"
                }),
            ))
            .await;
        let duplicate_add_payload = decode_tool_payload(&duplicate_add).unwrap();
        assert_eq!(duplicate_add_payload["success"], false);
        assert_eq!(duplicate_add_payload["reason"], "duplicate");

        let duplicate = harness
            .server
            .handle_request(tool_call(
                24,
                "mempalace_check_duplicate",
                json!({"content":content,"threshold":0.9}),
            ))
            .await;
        let duplicate_payload = decode_tool_payload(&duplicate).unwrap();
        assert_eq!(duplicate_payload["is_duplicate"], true);
        assert!(!duplicate_payload["matches"].as_array().unwrap().is_empty());

        let delete = harness
            .server
            .handle_request(tool_call(
                25,
                "mempalace_delete_drawer",
                json!({"drawer_id":add_payload["drawer_id"]}),
            ))
            .await;
        assert_eq!(decode_tool_payload(&delete).unwrap()["success"], true);

        let post_delete_duplicate = harness
            .server
            .handle_request(tool_call(
                231,
                "mempalace_check_duplicate",
                json!({"content":content,"threshold":0.9}),
            ))
            .await;
        assert_eq!(decode_tool_payload(&post_delete_duplicate).unwrap()["is_duplicate"], false);

        let post_delete_search = harness
            .server
            .handle_request(tool_call(
                232,
                "mempalace_search",
                json!({"query":"Roadmap budget planning note from MCP","wing":"wing_myproject","room":"backend","limit":5}),
            ))
            .await;
        assert!(
            decode_tool_payload(&post_delete_search).unwrap()["results"]
                .as_array()
                .unwrap()
                .is_empty()
        );

        let post_delete_rooms = harness
            .server
            .handle_request(tool_call(
                233,
                "mempalace_list_rooms",
                json!({"wing":"wing_myproject"}),
            ))
            .await;
        let rooms_payload = decode_tool_payload(&post_delete_rooms).unwrap();
        assert!(rooms_payload["rooms"].as_object().unwrap().is_empty());
    }

    #[tokio::test]
    async fn serve_transport_processes_tool_calls_and_ignores_notifications() {
        let harness = test_harness().await;
        let input = concat!(
            "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n",
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}\n",
            "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"mempalace_status\",\"arguments\":{}}}\n"
        );
        let (client, server_stream) = tokio::io::duplex(8_192);
        let (reader_half, writer_half) = tokio::io::split(server_stream);
        let task = tokio::spawn(async move {
            serve_transport(&harness.server, BufReader::new(reader_half), writer_half)
                .await
                .unwrap();
        });

        let (mut client_reader, mut client_writer) = tokio::io::split(client);
        client_writer.write_all(input.as_bytes()).await.unwrap();
        client_writer.shutdown().await.unwrap();

        let mut output = String::new();
        client_reader.read_to_string(&mut output).await.unwrap();
        task.await.unwrap();

        let lines = output.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);
        let initialize: Value = serde_json::from_str(lines[0]).unwrap();
        let status: Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(initialize["result"]["protocolVersion"], PROTOCOL_VERSION);
        assert_eq!(decode_tool_payload(&status).unwrap()["total_drawers"], 2);
    }

    #[test]
    fn entity_kind_heuristic_prefers_unknown_over_false_people() {
        assert_eq!(infer_entity_kind("Alice Smith"), EntityKind::Person);
        assert_eq!(infer_entity_kind("CUDA"), EntityKind::Concept);
        assert_eq!(infer_entity_kind("MemPalace"), EntityKind::Unknown);
        assert_eq!(infer_entity_kind("Mary-Anne"), EntityKind::Person);
    }

    #[tokio::test]
    async fn diary_read_merges_primary_and_legacy_history_without_cross_agent_collisions() {
        let harness = test_harness().await;
        assert_eq!(
            decode_tool_payload(
                &harness
                    .server
                    .handle_request(tool_call(
                        95,
                        "mempalace_diary_write",
                        json!({"agent_name":"Worker-One","entry":"SESSION:primary","summary":"Primary session.","topic":"ops"}),
                    ))
                    .await,
            )
            .unwrap()["success"],
            true
        );
        let legacy_drawer = DrawerRecord {
            id: DrawerId::new("diary_legacy_worker_one_merged").unwrap(),
            wing: WingId::new(&legacy_diary_wing_name("Worker-One")).unwrap(),
            room: RoomId::new(DIARY_ROOM).unwrap(),
            hall: Some(DIARY_HALL.to_owned()),
            date: Some(date!(2026 - 04 - 16)),
            source_file: format!("{DIARY_TOPIC_PREFIX}legacy"),
            chunk_index: 0,
            ingest_mode: "diary".to_owned(),
            extract_mode: None,
            added_by: "Worker-One".to_owned(),
            filed_at: datetime!(2026-04-16 12:00:00 UTC),
            importance: None,
            emotional_weight: None,
            weight: None,
            content: "SESSION:legacy".to_owned(),
            content_hash: hash_text("SESSION:legacy"),
            embedding: vec![0.0; EmbeddingProfile::Balanced.metadata().dimensions],
            locator: None,
            view_metadata: None,
        };
        let colliding_other_agent_drawer = DrawerRecord {
            id: DrawerId::new("diary_worker_one_colliding_agent").unwrap(),
            wing: WingId::new(&legacy_diary_wing_name("Worker-One")).unwrap(),
            room: RoomId::new(DIARY_ROOM).unwrap(),
            hall: Some(DIARY_HALL.to_owned()),
            date: Some(date!(2026 - 04 - 17)),
            source_file: format!("{DIARY_TOPIC_PREFIX}ops"),
            chunk_index: 0,
            ingest_mode: "diary".to_owned(),
            extract_mode: None,
            added_by: "Worker One".to_owned(),
            filed_at: datetime!(2026-04-17 12:00:00 UTC),
            importance: None,
            emotional_weight: None,
            weight: None,
            content: "SESSION:other-agent".to_owned(),
            content_hash: hash_text("SESSION:other-agent"),
            embedding: vec![0.0; EmbeddingProfile::Balanced.metadata().dimensions],
            locator: None,
            view_metadata: None,
        };
        let runtime = harness.server.runtime.lock().await;
        runtime
            .storage
            .drawer_store()
            .put_drawers(&[legacy_drawer, colliding_other_agent_drawer], DuplicateStrategy::Error)
            .await
            .unwrap();
        drop(runtime);

        let payload = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    97,
                    "mempalace_diary_read",
                    json!({"agent_name":"Worker-One","last_n":10,"since":"2026-04-01T00:00:00Z"}),
                ))
                .await,
        )
        .unwrap();

        let entries = payload["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0]["content"], "SESSION:primary");
        assert_eq!(entries[1]["content"], "SESSION:legacy");
    }

    #[tokio::test]
    async fn diary_read_falls_back_when_primary_wing_has_no_diary_entries() {
        let harness = test_harness().await;
        let non_diary_drawer = DrawerRecord {
            id: DrawerId::new("worker_one/non-diary/0001").unwrap(),
            wing: WingId::new(&diary_wing_name("Worker-One")).unwrap(),
            room: RoomId::new("ops-log").unwrap(),
            hall: Some("hall_events".to_owned()),
            date: Some(date!(2026 - 04 - 17)),
            source_file: "ops.md".to_owned(),
            chunk_index: 0,
            ingest_mode: "fixtures".to_owned(),
            extract_mode: None,
            added_by: "tests".to_owned(),
            filed_at: datetime!(2026-04-17 11:00:00 UTC),
            importance: None,
            emotional_weight: None,
            weight: None,
            content: "Primary wing has non-diary content only.".to_owned(),
            content_hash: hash_text("Primary wing has non-diary content only."),
            embedding: vec![0.0; EmbeddingProfile::Balanced.metadata().dimensions],
            locator: None,
            view_metadata: None,
        };
        let legacy_diary_drawer = DrawerRecord {
            id: DrawerId::new("diary_legacy_worker_one_0002").unwrap(),
            wing: WingId::new(&legacy_diary_wing_name("Worker-One")).unwrap(),
            room: RoomId::new(DIARY_ROOM).unwrap(),
            hall: Some(DIARY_HALL.to_owned()),
            date: Some(date!(2026 - 04 - 17)),
            source_file: format!("{DIARY_TOPIC_PREFIX}legacy"),
            chunk_index: 0,
            ingest_mode: "diary".to_owned(),
            extract_mode: None,
            added_by: "Worker-One".to_owned(),
            filed_at: datetime!(2026-04-17 12:00:00 UTC),
            importance: None,
            emotional_weight: None,
            weight: None,
            content: "SESSION:legacy-only".to_owned(),
            content_hash: hash_text("SESSION:legacy-only"),
            embedding: vec![0.0; EmbeddingProfile::Balanced.metadata().dimensions],
            locator: None,
            view_metadata: None,
        };

        let runtime = harness.server.runtime.lock().await;
        runtime
            .storage
            .drawer_store()
            .put_drawers(&[non_diary_drawer, legacy_diary_drawer], DuplicateStrategy::Error)
            .await
            .unwrap();
        drop(runtime);

        let payload = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    98,
                    "mempalace_diary_read",
                    json!({"agent_name":"Worker-One","last_n":10,"since":"2026-04-01T00:00:00Z"}),
                ))
                .await,
        )
        .unwrap();

        assert_eq!(payload["entries"].as_array().unwrap().len(), 1);
        assert_eq!(payload["entries"][0]["content"], "SESSION:legacy-only");
        assert_eq!(payload["entries"][0]["topic"], "legacy");
    }

    #[tokio::test]
    async fn diary_read_filters_primary_wing_entries_by_agent_name() {
        let harness = test_harness().await;
        let shared_wing = WingId::new(&diary_wing_name("Worker One")).unwrap();
        assert_eq!(shared_wing.as_str(), diary_wing_name("Worker_One"));
        let worker_one = DrawerRecord {
            id: DrawerId::new("diary_worker_one_primary_0001").unwrap(),
            wing: shared_wing.clone(),
            room: RoomId::new(DIARY_ROOM).unwrap(),
            hall: Some(DIARY_HALL.to_owned()),
            date: Some(date!(2026 - 04 - 17)),
            source_file: format!("{DIARY_TOPIC_PREFIX}ops"),
            chunk_index: 0,
            ingest_mode: "diary".to_owned(),
            extract_mode: None,
            added_by: "Worker One".to_owned(),
            filed_at: datetime!(2026-04-17 12:00:00 UTC),
            importance: None,
            emotional_weight: None,
            weight: None,
            content: "SESSION:space-agent".to_owned(),
            content_hash: hash_text("SESSION:space-agent"),
            embedding: vec![0.0; EmbeddingProfile::Balanced.metadata().dimensions],
            locator: None,
            view_metadata: None,
        };
        let worker_underscore = DrawerRecord {
            id: DrawerId::new("diary_worker_one_primary_0002").unwrap(),
            wing: shared_wing,
            room: RoomId::new(DIARY_ROOM).unwrap(),
            hall: Some(DIARY_HALL.to_owned()),
            date: Some(date!(2026 - 04 - 17)),
            source_file: format!("{DIARY_TOPIC_PREFIX}ops"),
            chunk_index: 0,
            ingest_mode: "diary".to_owned(),
            extract_mode: None,
            added_by: "Worker_One".to_owned(),
            filed_at: datetime!(2026-04-17 13:00:00 UTC),
            importance: None,
            emotional_weight: None,
            weight: None,
            content: "SESSION:underscore-agent".to_owned(),
            content_hash: hash_text("SESSION:underscore-agent"),
            embedding: vec![0.0; EmbeddingProfile::Balanced.metadata().dimensions],
            locator: None,
            view_metadata: None,
        };

        let runtime = harness.server.runtime.lock().await;
        runtime
            .storage
            .drawer_store()
            .put_drawers(&[worker_one, worker_underscore], DuplicateStrategy::Error)
            .await
            .unwrap();
        drop(runtime);

        let payload = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    299,
                    "mempalace_diary_read",
                    json!({"agent_name":"Worker One","last_n":10,"since":"2026-04-01T00:00:00Z"}),
                ))
                .await,
        )
        .unwrap();

        let entries = payload["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["content"], "SESSION:space-agent");
    }

    #[tokio::test]
    async fn diary_read_filters_by_since_wing_agent_and_topic() {
        let harness = test_harness().await;
        let matching = DrawerRecord {
            id: DrawerId::new("diary_filter_matching").unwrap(),
            wing: WingId::new("wing_project").unwrap(),
            room: RoomId::new(DIARY_ROOM).unwrap(),
            hall: Some(DIARY_HALL.to_owned()),
            date: Some(date!(2026 - 05 - 12)),
            source_file: format!("{DIARY_TOPIC_PREFIX}release"),
            chunk_index: 0,
            ingest_mode: "diary".to_owned(),
            extract_mode: None,
            added_by: "Codex".to_owned(),
            filed_at: datetime!(2026-05-12 12:00:00 UTC),
            importance: None,
            emotional_weight: None,
            weight: None,
            content: "SESSION:matching".to_owned(),
            content_hash: hash_text("SESSION:matching"),
            embedding: vec![0.0; EmbeddingProfile::Balanced.metadata().dimensions],
            locator: None,
            view_metadata: None,
        };
        let old = DrawerRecord {
            id: DrawerId::new("diary_filter_old").unwrap(),
            wing: WingId::new("wing_project").unwrap(),
            room: RoomId::new(DIARY_ROOM).unwrap(),
            hall: Some(DIARY_HALL.to_owned()),
            date: Some(date!(2026 - 05 - 10)),
            source_file: format!("{DIARY_TOPIC_PREFIX}release"),
            chunk_index: 0,
            ingest_mode: "diary".to_owned(),
            extract_mode: None,
            added_by: "Codex".to_owned(),
            filed_at: datetime!(2026-05-10 12:00:00 UTC),
            importance: None,
            emotional_weight: None,
            weight: None,
            content: "SESSION:old".to_owned(),
            content_hash: hash_text("SESSION:old"),
            embedding: vec![0.0; EmbeddingProfile::Balanced.metadata().dimensions],
            locator: None,
            view_metadata: None,
        };
        let wrong_topic = DrawerRecord {
            id: DrawerId::new("diary_filter_wrong_topic").unwrap(),
            wing: WingId::new("wing_project").unwrap(),
            room: RoomId::new(DIARY_ROOM).unwrap(),
            hall: Some(DIARY_HALL.to_owned()),
            date: Some(date!(2026 - 05 - 12)),
            source_file: format!("{DIARY_TOPIC_PREFIX}planning"),
            chunk_index: 0,
            ingest_mode: "diary".to_owned(),
            extract_mode: None,
            added_by: "Codex".to_owned(),
            filed_at: datetime!(2026-05-12 13:00:00 UTC),
            importance: None,
            emotional_weight: None,
            weight: None,
            content: "SESSION:wrong-topic".to_owned(),
            content_hash: hash_text("SESSION:wrong-topic"),
            embedding: vec![0.0; EmbeddingProfile::Balanced.metadata().dimensions],
            locator: None,
            view_metadata: None,
        };
        let wrong_agent = DrawerRecord {
            id: DrawerId::new("diary_filter_wrong_agent").unwrap(),
            wing: WingId::new("wing_project").unwrap(),
            room: RoomId::new(DIARY_ROOM).unwrap(),
            hall: Some(DIARY_HALL.to_owned()),
            date: Some(date!(2026 - 05 - 12)),
            source_file: format!("{DIARY_TOPIC_PREFIX}release"),
            chunk_index: 0,
            ingest_mode: "diary".to_owned(),
            extract_mode: None,
            added_by: "Other".to_owned(),
            filed_at: datetime!(2026-05-12 14:00:00 UTC),
            importance: None,
            emotional_weight: None,
            weight: None,
            content: "SESSION:wrong-agent".to_owned(),
            content_hash: hash_text("SESSION:wrong-agent"),
            embedding: vec![0.0; EmbeddingProfile::Balanced.metadata().dimensions],
            locator: None,
            view_metadata: None,
        };

        let runtime = harness.server.runtime.lock().await;
        runtime
            .storage
            .drawer_store()
            .put_drawers(&[matching, old, wrong_topic, wrong_agent], DuplicateStrategy::Error)
            .await
            .unwrap();
        drop(runtime);

        let payload = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    301,
                    "mempalace_diary_read",
                    json!({
                        "agent_name":"Codex",
                        "wing":"wing_project",
                        "topic":"release",
                        "since":"2026-05-11T00:00:00Z",
                        "last_n":10
                    }),
                ))
                .await,
        )
        .unwrap();

        let entries = payload["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["content"], "SESSION:matching");
        assert_eq!(entries[0]["agent"], "Codex");
        assert_eq!(entries[0]["wing"], "wing_project");
        assert_eq!(payload["topic"], "release");
    }

    #[tokio::test]
    async fn kg_add_accepts_and_round_trips_freeform_source_closet() {
        let harness = test_harness().await;
        let add = harness
            .server
            .handle_request(tool_call(
                300,
                "mempalace_kg_add",
                json!({
                    "subject":"Alice Smith",
                    "predicate":"works_on",
                    "object":"MemPalace",
                    "source_closet":"freeform source ref"
                }),
            ))
            .await;
        assert_eq!(decode_tool_payload(&add).unwrap()["success"], true);

        let query = harness
            .server
            .handle_request(tool_call(
                301,
                "mempalace_kg_query",
                json!({
                    "entity":"Alice Smith",
                    "direction":"outgoing"
                }),
            ))
            .await;
        let facts = decode_tool_payload(&query).unwrap()["facts"].as_array().unwrap().to_vec();

        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0]["source_closet"], "freeform source ref");
    }

    #[tokio::test]
    async fn wing_param_normalizes_symmetrically_across_writes_and_reads() {
        let harness = test_harness().await;

        let add = harness
            .server
            .handle_request(tool_call(
                400,
                "mempalace_add_drawer",
                json!({
                    "wing":"ghosttest",
                    "room":"bugs",
                    "content":"Ghost-wing repro: writing wing=ghosttest must land in wing_ghosttest.",
                    "source_file":"ghost.md",
                    "added_by":"tester"
                }),
            ))
            .await;
        let add_payload = decode_tool_payload(&add).unwrap();
        assert_eq!(add_payload["success"], true);
        assert_eq!(
            add_payload["wing"], "wing_ghosttest",
            "wing should be normalized on write: {add_payload}"
        );

        for wing_input in ["ghosttest", "wing_ghosttest", "GhostTest"] {
            let rooms = harness
                .server
                .handle_request(tool_call(401, "mempalace_list_rooms", json!({"wing": wing_input})))
                .await;
            let rooms_payload = decode_tool_payload(&rooms).unwrap();
            assert_eq!(
                rooms_payload["rooms"]["bugs"], 1,
                "list_rooms({wing_input}) should resolve to wing_ghosttest"
            );

            let search = harness
                .server
                .handle_request(tool_call(
                    402,
                    "mempalace_search",
                    json!({
                        "query":"Ghost-wing repro",
                        "wing": wing_input,
                        "limit":5
                    }),
                ))
                .await;
            let search_payload = decode_tool_payload(&search).unwrap();
            let results = search_payload["results"].as_array().unwrap();
            assert!(
                results.iter().any(|r| r["wing"] == "wing_ghosttest"),
                "search({wing_input}) should find wing_ghosttest drawer; got {search_payload}"
            );
        }

        let wings =
            harness.server.handle_request(tool_call(403, "mempalace_list_wings", json!({}))).await;
        let wings_payload = decode_tool_payload(&wings).unwrap();
        let wings_obj = wings_payload["wings"].as_object().unwrap();
        assert!(
            wings_obj.contains_key("wing_ghosttest"),
            "wing_ghosttest should exist: {wings_payload}"
        );
        assert!(
            !wings_obj.contains_key("ghosttest"),
            "no ghost wing should be created: {wings_payload}"
        );
    }

    #[tokio::test]
    async fn get_changes_since_returns_events_for_all_mutating_tools() {
        let harness = test_harness().await;

        let t_before = OffsetDateTime::now_utc();
        // Allow a tiny gap so the events are strictly after t_before.
        std::thread::sleep(std::time::Duration::from_millis(5));

        harness
            .server
            .handle_request(tool_call(
                500,
                "mempalace_add_drawer",
                json!({"wing":"wing_changes_test","room":"notes",
                       "content":"Changefeed test: unique content for changefeed coverage.",
                       "added_by":"test-agent"}),
            ))
            .await;

        let add_drawer_id = {
            let res = harness
                .server
                .handle_request(tool_call(
                    501,
                    "mempalace_search",
                    json!({"query":"Changefeed test: unique content for changefeed coverage.",
                           "wing":"wing_changes_test","limit":1}),
                ))
                .await;
            let p = decode_tool_payload(&res).unwrap();
            p["results"][0]["wing"].as_str().unwrap().to_owned()
        };
        let _ = add_drawer_id; // used to drain the search; drawer_id not needed

        harness
            .server
            .handle_request(tool_call(
                502,
                "mempalace_diary_write",
                json!({"agent_name":"change-bot","entry":"SESSION:test","summary":"Change feed test.","topic":"changefeed"}),
            ))
            .await;

        harness
            .server
            .handle_request(tool_call(
                503,
                "mempalace_kg_add",
                json!({"subject":"ChangeFeed","predicate":"is","object":"Working"}),
            ))
            .await;

        harness
            .server
            .handle_request(tool_call(
                504,
                "mempalace_kg_invalidate",
                json!({"subject":"ChangeFeed","predicate":"is","object":"Working"}),
            ))
            .await;

        let since_str = t_before.format(&time::format_description::well_known::Rfc3339).unwrap();
        let changes = harness
            .server
            .handle_request(tool_call(
                505,
                "mempalace_get_changes_since",
                json!({"since": since_str, "limit": 100}),
            ))
            .await;
        let payload = decode_tool_payload(&changes).unwrap();
        let events = payload["events"].as_array().unwrap();

        let types: Vec<&str> = events.iter().filter_map(|e| e["event_type"].as_str()).collect();
        assert!(types.contains(&"drawer_added"), "expected drawer_added in {types:?}");
        assert!(types.contains(&"diary_written"), "expected diary_written in {types:?}");
        assert!(types.contains(&"kg_fact_added"), "expected kg_fact_added in {types:?}");
        assert!(
            types.contains(&"kg_fact_invalidated"),
            "expected kg_fact_invalidated in {types:?}"
        );

        // actor is recorded for drawer_added and diary_written
        let drawer_event = events.iter().find(|e| e["event_type"] == "drawer_added").unwrap();
        assert_eq!(drawer_event["actor"], "test-agent");

        let diary_event = events.iter().find(|e| e["event_type"] == "diary_written").unwrap();
        assert_eq!(diary_event["actor"], "change-bot");

        // kg add captures subject/predicate/object in details
        let kg_event = events.iter().find(|e| e["event_type"] == "kg_fact_added").unwrap();
        assert_eq!(kg_event["details"]["subject"], "ChangeFeed");
        assert_eq!(kg_event["details"]["predicate"], "is");
        assert_eq!(kg_event["details"]["object"], "Working");
    }

    // ── Routing / Federation tests ─────────────────────────────────────────

    #[test]
    fn routing_categories_are_consistent_with_semantics_doc() {
        // LocalOnly tools
        assert_eq!(ToolName::WakeUp.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::DiaryWrite.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::DiaryRead.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::GetChangesSince.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::Traverse.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::FindTunnels.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::GraphStats.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::IdentityRead.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::IdentityUpdate.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::LineageSet.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::SelfObservationPropose.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::SelfObservationReview.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::IdentityPacket.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::MigrationRecord.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::GetAaaKSpec.routing(), ToolRoutingCategory::LocalOnly);

        // RoutableDrawer tools
        assert_eq!(ToolName::Search.routing(), ToolRoutingCategory::RoutableDrawer);
        assert_eq!(ToolName::ListWings.routing(), ToolRoutingCategory::RoutableDrawer);
        assert_eq!(ToolName::ListRooms.routing(), ToolRoutingCategory::RoutableDrawer);
        assert_eq!(ToolName::GetTaxonomy.routing(), ToolRoutingCategory::RoutableDrawer);
        assert_eq!(ToolName::Status.routing(), ToolRoutingCategory::RoutableDrawer);
        assert_eq!(ToolName::CheckDuplicate.routing(), ToolRoutingCategory::RoutableDrawer);
        assert_eq!(ToolName::AddDrawer.routing(), ToolRoutingCategory::RoutableDrawer);
        assert_eq!(ToolName::DeleteDrawer.routing(), ToolRoutingCategory::RoutableDrawer);

        // RoutableKg tools
        assert_eq!(ToolName::KgQuery.routing(), ToolRoutingCategory::RoutableKg);
        assert_eq!(ToolName::KgAdd.routing(), ToolRoutingCategory::RoutableKg);
        assert_eq!(ToolName::KgInvalidate.routing(), ToolRoutingCategory::RoutableKg);
        assert_eq!(ToolName::KgTimeline.routing(), ToolRoutingCategory::RoutableKg);
        assert_eq!(ToolName::KgStats.routing(), ToolRoutingCategory::RoutableKg);

        // RoutableCoordination tools (issue #102 Stage 4) — before this stage every
        // coordination tool below was (wrongly) declared LocalOnly with nothing checking it;
        // flipping one to routable would have compiled and passed silently.
        assert_eq!(ToolName::TaskCreate.routing(), ToolRoutingCategory::RoutableCoordination);
        assert_eq!(ToolName::TaskGet.routing(), ToolRoutingCategory::RoutableCoordination);
        assert_eq!(ToolName::TaskClaim.routing(), ToolRoutingCategory::RoutableCoordination);
        assert_eq!(ToolName::TaskRenew.routing(), ToolRoutingCategory::RoutableCoordination);
        assert_eq!(ToolName::TaskTransition.routing(), ToolRoutingCategory::RoutableCoordination);
        assert_eq!(ToolName::MessageSend.routing(), ToolRoutingCategory::RoutableCoordination);
        assert_eq!(ToolName::MessageGet.routing(), ToolRoutingCategory::RoutableCoordination);
        assert_eq!(
            ToolName::MessageAcknowledge.routing(),
            ToolRoutingCategory::RoutableCoordination
        );
        assert_eq!(ToolName::InboxRead.routing(), ToolRoutingCategory::RoutableCoordination);
        assert_eq!(ToolName::ArtifactPut.routing(), ToolRoutingCategory::RoutableCoordination);
        assert_eq!(ToolName::ArtifactGet.routing(), ToolRoutingCategory::RoutableCoordination);
        assert_eq!(ToolName::ResultPut.routing(), ToolRoutingCategory::RoutableCoordination);
        assert_eq!(ToolName::ResultGet.routing(), ToolRoutingCategory::RoutableCoordination);
        assert_eq!(
            ToolName::CoordinationEvents.routing(),
            ToolRoutingCategory::RoutableCoordination
        );

        // CoordinationEventGet is deliberately LocalOnly, not RoutableCoordination — Stage 3
        // never exposed a single-event GET route on the wire, only the paginated feed.
        assert_eq!(ToolName::CoordinationEventGet.routing(), ToolRoutingCategory::LocalOnly);

        // Skill and delegation tools stay LocalOnly in this phase — not federated yet.
        assert_eq!(ToolName::SkillPropose.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::SkillGet.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::SkillVersions.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::SkillList.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::SkillRecordOutcome.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::SkillPromote.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::SkillRetire.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::SkillReviews.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::DelegationSpanStart.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::DelegationSpanGet.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::DelegationSpanClose.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::DelegationSpansForTask.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(
            ToolName::DelegationCheckpointAppend.routing(),
            ToolRoutingCategory::LocalOnly
        );
        assert_eq!(ToolName::DelegationCheckpointGet.routing(), ToolRoutingCategory::LocalOnly);
        assert_eq!(ToolName::DelegationTrace.routing(), ToolRoutingCategory::LocalOnly);
    }

    #[tokio::test]
    async fn federation_routing_does_not_panic_with_wing_rule() {
        // The dispatch path must not panic/crash when federation is configured
        // with a non-local wing rule. No live remote is wired, so no remote HTTP
        // calls are made; tools fall through to local execution.
        let harness = test_harness_with_federation(FederationRuntimeConfig {
            wings: [(
                "wing_code".to_owned(),
                ResolvedRouteRule {
                    mode: RouteMode::Remote,
                    remote: Some("remote-alpha".to_owned()),
                    write: WriteTarget::Remote,
                },
            )]
            .into(),
            ..FederationRuntimeConfig::default()
        })
        .await;

        let response = harness
            .server
            .handle_request(tool_call(1001, "mempalace_list_rooms", json!({"wing": "wing_code"})))
            .await;
        let payload = decode_tool_payload(&response).unwrap();
        assert_eq!(payload["wing"], "wing_code");
    }

    #[tokio::test]
    async fn federation_none_produces_byte_identical_responses() {
        // Regression: federation:None must produce responses identical to
        // having no federation configured at all.
        let harness_default = test_harness().await;
        let harness_none_fed =
            test_harness_with_federation(FederationRuntimeConfig::default()).await;

        let tools = [
            ("mempalace_status", json!({})),
            ("mempalace_list_wings", json!({})),
            ("mempalace_list_rooms", json!({"wing": "wing_code"})),
            ("mempalace_get_taxonomy", json!({})),
            ("mempalace_search", json!({"query": "auth migration parity", "limit": 2})),
            ("mempalace_kg_stats", json!({})),
        ];

        for (tool, args) in &tools {
            let default_resp =
                harness_default.server.handle_request(tool_call(2000, tool, args.clone())).await;
            let none_fed_resp =
                harness_none_fed.server.handle_request(tool_call(2001, tool, args.clone())).await;
            let mut default_payload = decode_tool_payload(&default_resp).unwrap();
            let mut none_fed_payload = decode_tool_payload(&none_fed_resp).unwrap();
            // Strip palace_path — each harness uses its own TempDir.
            default_payload.as_object_mut().map(|obj| obj.remove("palace_path"));
            none_fed_payload.as_object_mut().map(|obj| obj.remove("palace_path"));
            // The response body structures must match; wing_availability is
            // omitted when no remotes are configured.
            assert_eq!(
                default_payload, none_fed_payload,
                "tool `{tool}` produced different responses"
            );
        }

        // ── Additional byte-identical read tools ──────────────────────────────

        // mempalace_check_duplicate — use content that matches a seeded drawer.
        let seeded_content = "Code notes: auth-migration keeps search filter semantics exact while storage changes underneath.";
        {
            let default_resp = harness_default
                .server
                .handle_request(tool_call(
                    2010,
                    "mempalace_check_duplicate",
                    json!({"content": seeded_content, "threshold": 0.9}),
                ))
                .await;
            let none_fed_resp = harness_none_fed
                .server
                .handle_request(tool_call(
                    2011,
                    "mempalace_check_duplicate",
                    json!({"content": seeded_content, "threshold": 0.9}),
                ))
                .await;
            assert_eq!(
                decode_tool_payload(&default_resp).unwrap(),
                decode_tool_payload(&none_fed_resp).unwrap(),
                "mempalace_check_duplicate produced different responses"
            );
        }

        // mempalace_kg_query — use the seeded entity "Rust Rewrite".
        {
            let default_resp = harness_default
                .server
                .handle_request(tool_call(
                    2012,
                    "mempalace_kg_query",
                    json!({"entity": "Rust Rewrite", "direction": "outgoing"}),
                ))
                .await;
            let none_fed_resp = harness_none_fed
                .server
                .handle_request(tool_call(
                    2013,
                    "mempalace_kg_query",
                    json!({"entity": "Rust Rewrite", "direction": "outgoing"}),
                ))
                .await;
            assert_eq!(
                decode_tool_payload(&default_resp).unwrap(),
                decode_tool_payload(&none_fed_resp).unwrap(),
                "mempalace_kg_query produced different responses"
            );
        }

        // mempalace_kg_timeline — full timeline (no entity filter).
        {
            let default_resp = harness_default
                .server
                .handle_request(tool_call(2014, "mempalace_kg_timeline", json!({})))
                .await;
            let none_fed_resp = harness_none_fed
                .server
                .handle_request(tool_call(2015, "mempalace_kg_timeline", json!({})))
                .await;
            assert_eq!(
                decode_tool_payload(&default_resp).unwrap(),
                decode_tool_payload(&none_fed_resp).unwrap(),
                "mempalace_kg_timeline produced different responses"
            );
        }

        // ── Key-set comparison for one mutating call per harness ──────────────
        // mempalace_add_drawer — fresh unique content so it succeeds.
        // Values contain generated ids/timestamps, so we compare sorted key lists only.
        let unique_content = "federation_none_regression_test_drawer_unique_content_xyz";
        let default_add = harness_default
            .server
            .handle_request(tool_call(
                2020,
                "mempalace_add_drawer",
                json!({"wing": "wing_fed_test", "room": "reg", "content": unique_content}),
            ))
            .await;
        let none_fed_add = harness_none_fed
            .server
            .handle_request(tool_call(
                2021,
                "mempalace_add_drawer",
                json!({"wing": "wing_fed_test", "room": "reg", "content": unique_content}),
            ))
            .await;
        let default_add_payload = decode_tool_payload(&default_add).unwrap();
        let none_fed_add_payload = decode_tool_payload(&none_fed_add).unwrap();
        // Both must succeed.
        assert_eq!(default_add_payload["success"], true);
        assert_eq!(none_fed_add_payload["success"], true);
        // Compare sorted top-level key sets (not values — ids/timestamps differ).
        let default_keys: Vec<String> = default_add_payload
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        let none_fed_keys: Vec<String> = none_fed_add_payload
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        assert_eq!(
            default_keys, none_fed_keys,
            "mempalace_add_drawer top-level keys differ between federation:none and no federation"
        );
    }

    // ─── Federation changes integration tests ─────────────────────────────────

    /// A minimal MockRemote for use in lib.rs integration tests.  We re-use the
    /// same shape as the federation.rs tests so we can import via `use super::*`.
    struct LibMockRemote {
        changes_events: Vec<mempalace_federation::ChangeEventDto>,
        changes_next_cursor: Option<String>,
        search_results: Vec<mempalace_federation::RemoteDrawerResult>,
        fail: bool,
        /// Bumped by every `coordination_*` method this mock implements. Lets a test assert a
        /// coordination write never reached the remote at all — see
        /// `MockRemote::coordination_calls` in `federation.rs` for why a bare result check is
        /// not sufficient on its own.
        coordination_calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        /// Outcome for `coordination_events`/`coordination_inbox` on this mock — lets a test
        /// drive the aggregate fan-outs' `CapabilityMissing` vs. genuinely-unreachable
        /// distinction (finding 1b) without hand-building a non-`Clone` `RemoteError`.
        coordination_fanout_outcome: LibMockFanoutOutcome,
    }

    /// Canned outcomes for `LibMockRemote::coordination_events`/`coordination_inbox`.
    #[derive(Clone, Copy, Default)]
    enum LibMockFanoutOutcome {
        /// Returns an empty, successful page.
        #[default]
        Success,
        /// The remote does not advertise the `coordination` capability at all — the "declined,
        /// not down" case the aggregate fan-outs must report as `capability_missing`, not
        /// `unreachable`.
        CapabilityMissing,
        /// The remote could not be reached — the genuinely-degradable case that must still be
        /// reported as `unreachable`.
        Unreachable,
    }

    impl Default for LibMockRemote {
        fn default() -> Self {
            Self {
                changes_events: vec![],
                changes_next_cursor: None,
                search_results: vec![],
                fail: false,
                coordination_calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                coordination_fanout_outcome: LibMockFanoutOutcome::default(),
            }
        }
    }

    #[async_trait::async_trait]
    impl mempalace_remote::RemoteApi for LibMockRemote {
        async fn info(&self) -> mempalace_remote::Result<mempalace_federation::InfoResponse> {
            Err(mempalace_remote::RemoteError::Unreachable {
                remote: "mock".to_owned(),
                message: "not used".to_owned(),
            })
        }
        async fn search_drawers(
            &self,
            _req: mempalace_federation::DrawerSearchRequest,
        ) -> mempalace_remote::Result<mempalace_federation::DrawerSearchResponse> {
            Ok(mempalace_federation::DrawerSearchResponse { results: self.search_results.clone() })
        }
        async fn check_duplicate(
            &self,
            _req: mempalace_federation::CheckDuplicateRequest,
        ) -> mempalace_remote::Result<mempalace_federation::CheckDuplicateResponse> {
            Ok(mempalace_federation::CheckDuplicateResponse {
                is_duplicate: false,
                matches: json!([]),
            })
        }
        async fn add_drawer(
            &self,
            _req: mempalace_federation::AddDrawerRequest,
        ) -> mempalace_remote::Result<mempalace_federation::AddDrawerResponse> {
            Err(mempalace_remote::RemoteError::Unreachable {
                remote: "mock".to_owned(),
                message: "not used".to_owned(),
            })
        }
        async fn list_drawers(
            &self,
            _query: mempalace_federation::ListDrawersQuery,
        ) -> mempalace_remote::Result<mempalace_federation::ListDrawersResponse> {
            Ok(mempalace_federation::ListDrawersResponse { drawers: json!([]), next_cursor: None })
        }
        async fn get_drawer(&self, _drawer_id: &str) -> mempalace_remote::Result<Value> {
            Ok(json!({}))
        }
        async fn delete_drawer(&self, _drawer_id: &str) -> mempalace_remote::Result<()> {
            Ok(())
        }
        async fn kg_query(
            &self,
            _req: mempalace_federation::KgQueryRequest,
        ) -> mempalace_remote::Result<Value> {
            Ok(json!({"entity":"","facts":[],"count":0}))
        }
        async fn kg_add_fact(
            &self,
            _req: mempalace_federation::KgAddFactRequest,
        ) -> mempalace_remote::Result<Value> {
            Ok(json!({"success":true}))
        }
        async fn kg_invalidate(
            &self,
            _req: mempalace_federation::KgInvalidateRequest,
        ) -> mempalace_remote::Result<Value> {
            Ok(json!({"success":true}))
        }
        async fn kg_timeline(&self, _entity: Option<&str>) -> mempalace_remote::Result<Value> {
            Ok(json!({"entity":"all","timeline":[],"count":0}))
        }
        async fn kg_stats(&self) -> mempalace_remote::Result<Value> {
            Ok(
                json!({"entities":0,"triples":0,"current_facts":0,"expired_facts":0,"relationship_types":[]}),
            )
        }
        async fn taxonomy(&self) -> mempalace_remote::Result<Value> {
            Ok(json!({"taxonomy":{}}))
        }
        async fn wings(&self) -> mempalace_remote::Result<Value> {
            Ok(json!({"wings":{}}))
        }
        async fn rooms(&self, _wing: Option<&str>) -> mempalace_remote::Result<Value> {
            Ok(json!({"rooms":{}}))
        }
        async fn changes(
            &self,
            _query: mempalace_federation::ChangesQuery,
        ) -> mempalace_remote::Result<mempalace_federation::ChangesResponse> {
            if self.fail {
                return Err(mempalace_remote::RemoteError::Unreachable {
                    remote: "mock".to_owned(),
                    message: "mock remote is down".to_owned(),
                });
            }
            Ok(mempalace_federation::ChangesResponse {
                events: self.changes_events.clone(),
                next_cursor: self.changes_next_cursor.clone(),
            })
        }
        async fn ingest_batch(
            &self,
            _req: mempalace_federation::IngestBatchRequest,
        ) -> mempalace_remote::Result<mempalace_federation::IngestBatchResponse> {
            Err(mempalace_remote::RemoteError::Unreachable {
                remote: "mock".to_owned(),
                message: "not used".to_owned(),
            })
        }
        async fn coordination_task_create(
            &self,
            req: mempalace_federation::NewTaskRequest,
        ) -> mempalace_remote::Result<mempalace_federation::CoordinationTaskDto> {
            self.coordination_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _ = req;
            Err(mempalace_remote::RemoteError::Unreachable {
                remote: "mock".to_owned(),
                message: "not used".to_owned(),
            })
        }
        /// Recording override for the aggregate-fan-out regression tests below: bumps
        /// `coordination_calls` (like every other `coordination_*` override on this mock) so a
        /// test can assert the gate in `FederationRouter::coordination_inbox_fanout` actually
        /// stopped the call from ever reaching a remote, rather than trusting the response shape.
        /// Honors `coordination_fanout_outcome` so a test can also drive the `CapabilityMissing`
        /// vs. genuinely-unreachable distinction (finding 1b).
        async fn coordination_inbox(
            &self,
            query: mempalace_federation::InboxQuery,
        ) -> mempalace_remote::Result<mempalace_federation::InboxPageResponse> {
            self.coordination_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _ = query;
            match self.coordination_fanout_outcome {
                LibMockFanoutOutcome::Success => {
                    Ok(mempalace_federation::InboxPageResponse { messages: vec![], next_cursor: None })
                }
                LibMockFanoutOutcome::CapabilityMissing => {
                    Err(mempalace_remote::RemoteError::CapabilityMissing {
                        remote: "mock".to_owned(),
                        capability: "coordination".to_owned(),
                    })
                }
                LibMockFanoutOutcome::Unreachable => Err(mempalace_remote::RemoteError::Unreachable {
                    remote: "mock".to_owned(),
                    message: "mock remote is down".to_owned(),
                }),
            }
        }
        /// See [`Self::coordination_inbox`] — same recording purpose, for
        /// `coordination_events_fanout`.
        async fn coordination_events(
            &self,
            query: mempalace_federation::CoordinationEventsQuery,
        ) -> mempalace_remote::Result<mempalace_federation::CoordinationEventsResponse> {
            self.coordination_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _ = query;
            match self.coordination_fanout_outcome {
                LibMockFanoutOutcome::Success => Ok(mempalace_federation::CoordinationEventsResponse {
                    events: vec![],
                    next_cursor: None,
                }),
                LibMockFanoutOutcome::CapabilityMissing => {
                    Err(mempalace_remote::RemoteError::CapabilityMissing {
                        remote: "mock".to_owned(),
                        capability: "coordination".to_owned(),
                    })
                }
                LibMockFanoutOutcome::Unreachable => Err(mempalace_remote::RemoteError::Unreachable {
                    remote: "mock".to_owned(),
                    message: "mock remote is down".to_owned(),
                }),
            }
        }
    }

    fn make_lib_router(
        remotes: BTreeMap<String, Arc<dyn mempalace_remote::RemoteApi>>,
    ) -> FederationRouter {
        let mut rules_remotes = BTreeMap::new();
        for name in remotes.keys() {
            rules_remotes.insert(
                name.clone(),
                ResolvedRemote {
                    name: name.clone(),
                    url: "https://test.example".to_owned(),
                    token: None,
                    timeout: std::time::Duration::from_secs(5),
                },
            );
        }
        let rules = FederationRuntimeConfig {
            remotes: rules_remotes,
            default_mode: RouteMode::Combined,
            default_remote: remotes.keys().next().cloned(),
            wings: BTreeMap::new(),
            kg: Some(ResolvedRouteRule {
                mode: RouteMode::Combined,
                remote: remotes.keys().next().cloned(),
                write: WriteTarget::Remote,
            }),
            coordination: BTreeMap::new(),
        };
        FederationRouter::with_remotes(rules, remotes)
    }

    async fn test_harness_with_mock_router(router: FederationRouter) -> TestHarness {
        let tempdir = TempDir::new().unwrap();
        let palace_path = tempdir.path().join("palace");
        let config = make_base_config(&palace_path, &tempdir);
        let mut runtime = McpRuntime::new(
            config,
            DeterministicStubProvider::new(EmbeddingProfile::Balanced),
            None,
        )
        .await
        .unwrap();
        // Replace federation with the mock router (only if it has remotes).
        runtime.federation = if router.has_remotes() { Some(router) } else { None };
        let server = McpServer {
            runtime: Arc::new(Mutex::new(runtime)),
            queue_limit: Arc::new(Semaphore::new(8)),
        };
        seed_drawers(&server).await;
        TestHarness { _tempdir: tempdir, server }
    }

    #[tokio::test]
    async fn remote_only_view_search_keeps_remote_results_despite_local_branch_rows() {
        let mut remote = LibMockRemote::default();
        remote.search_results = vec![mempalace_federation::RemoteDrawerResult {
            drawer_id: "remote-1".to_owned(),
            wing: "wing_code".to_owned(),
            room: "general".to_owned(),
            rank: 1,
            score: 0.9,
            content: "remote canonical content".to_owned(),
            source_file: Some("changed.md".to_owned()),
            content_hash: None,
            filed_at: None,
            added_by: None,
            stale: false,
        }];
        let mut remotes: BTreeMap<String, Arc<dyn mempalace_remote::RemoteApi>> = BTreeMap::new();
        remotes.insert("hub".to_owned(), Arc::new(remote));
        let mut router = make_lib_router(remotes);
        router.rules.wings.insert(
            "wing_code".to_owned(),
            ResolvedRouteRule {
                mode: RouteMode::Remote,
                remote: Some("hub".to_owned()),
                write: WriteTarget::Remote,
            },
        );
        let harness = test_harness_with_mock_router(router).await;

        let runtime = harness.server.runtime.lock().await;
        let now = datetime!(2026-04-11 09:00:00 UTC);
        let mut local_branch_row =
            test_diary_drawer("wing_code/general/branch", "local branch", now);
        local_branch_row.wing = WingId::new("wing_code").unwrap();
        local_branch_row.room = RoomId::new("general").unwrap();
        local_branch_row.source_file = "changed.md".to_owned();
        local_branch_row.ingest_mode = "projects-branch".to_owned();
        local_branch_row.view_metadata = Some(mempalace_core::RepositoryViewMetadata {
            repo_id: "repo".to_owned(),
            view_name: Some("feature-x".to_owned()),
            source_path: "/repo".to_owned(),
            head_commit: Some("head".to_owned()),
            base_ref: Some("main".to_owned()),
            merge_base: Some("base".to_owned()),
            worktree_id: "worktree".to_owned(),
            path_state: "present".to_owned(),
        });
        runtime
            .storage
            .drawer_store()
            .put_drawers(&[local_branch_row], DuplicateStrategy::Error)
            .await
            .unwrap();
        drop(runtime);

        let response = harness
            .server
            .handle_request(tool_call(
                9002,
                "mempalace_search",
                json!({"query": "canonical", "wing": "wing_code", "view": "feature-x"}),
            ))
            .await;
        let payload = decode_tool_payload(&response).unwrap();
        assert_eq!(payload["results"].as_array().unwrap().len(), 1);
        assert_eq!(payload["results"][0]["origin"], "hub");
    }

    fn make_dto_event(
        event_type: &str,
        occurred_at: &str,
        entity_id: &str,
    ) -> mempalace_federation::ChangeEventDto {
        mempalace_federation::ChangeEventDto {
            event_type: event_type.to_owned(),
            occurred_at: occurred_at.to_owned(),
            entity_id: entity_id.to_owned(),
            actor: None,
            details: None,
        }
    }

    #[tokio::test]
    async fn tool_get_changes_since_with_federation_merges_events_and_annotates_origin() {
        let mut mock_hub = LibMockRemote::default();
        mock_hub.changes_events =
            vec![make_dto_event("drawer_added", "2026-06-10T12:00:00Z", "remote-entity-1")];
        mock_hub.changes_next_cursor = Some("cursor-hub-1".to_owned());

        let mut remotes: BTreeMap<String, Arc<dyn mempalace_remote::RemoteApi>> = BTreeMap::new();
        remotes.insert("hub".to_owned(), Arc::new(mock_hub));
        let router = make_lib_router(remotes);

        let harness = test_harness_with_mock_router(router).await;

        // First, write a local change so there is at least one local event.
        harness
            .server
            .handle_request(tool_call(
                9000,
                "mempalace_add_drawer",
                json!({"wing":"wing_fed_changes","room":"test","content":"federation changes test"}),
            ))
            .await;

        let response = harness
            .server
            .handle_request(tool_call(
                9001,
                "mempalace_get_changes_since",
                json!({"since": "2000-01-01T00:00:00Z", "limit": 10}),
            ))
            .await;

        let payload = decode_tool_payload(&response).unwrap();

        // All events should have an `origin` field.
        let events = payload["events"].as_array().unwrap();
        assert!(
            events.iter().all(|e| e.get("origin").is_some()),
            "all events must have origin when federation is active: {payload}"
        );

        // At least one local event and at least one remote:hub event.
        assert!(
            events.iter().any(|e| e["origin"] == "local"),
            "expected at least one local event: {payload}"
        );
        assert!(
            events.iter().any(|e| e["origin"] == "remote:hub"),
            "expected at least one hub event: {payload}"
        );

        // remotes.hub should have next_cursor and the stub's event count.
        assert_eq!(payload["remotes"]["hub"]["next_cursor"], "cursor-hub-1");
        assert_eq!(payload["remotes"]["hub"]["count"], 1);
    }

    #[tokio::test]
    async fn tool_get_changes_since_events_sorted_by_occurred_at() {
        // Hub returns an event with an earlier timestamp; local event will be later.
        let mut mock_hub = LibMockRemote::default();
        mock_hub.changes_events =
            vec![make_dto_event("drawer_added", "2026-01-01T00:00:00Z", "early-remote")];

        let mut remotes: BTreeMap<String, Arc<dyn mempalace_remote::RemoteApi>> = BTreeMap::new();
        remotes.insert("hub".to_owned(), Arc::new(mock_hub));
        let router = make_lib_router(remotes);

        let harness = test_harness_with_mock_router(router).await;

        // Add a local drawer (timestamp will be recent, so later than 2026-01-01).
        harness
            .server
            .handle_request(tool_call(
                9010,
                "mempalace_add_drawer",
                json!({"wing":"wing_sort_test","room":"test","content":"sort test local"}),
            ))
            .await;

        let payload = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    9011,
                    "mempalace_get_changes_since",
                    json!({"since":"2000-01-01T00:00:00Z","limit":20}),
                ))
                .await,
        )
        .unwrap();

        let events = payload["events"].as_array().unwrap();
        // Events must be sorted ascending by occurred_at.
        let timestamps: Vec<&str> =
            events.iter().filter_map(|e| e["occurred_at"].as_str()).collect();
        let mut sorted = timestamps.clone();
        sorted.sort();
        assert_eq!(timestamps, sorted, "events must be sorted ascending by occurred_at");
    }

    #[tokio::test]
    async fn tool_get_changes_since_unreachable_remote_yields_marker() {
        let mut mock_hub = LibMockRemote::default();
        mock_hub.fail = true;

        let mut remotes: BTreeMap<String, Arc<dyn mempalace_remote::RemoteApi>> = BTreeMap::new();
        remotes.insert("hub".to_owned(), Arc::new(mock_hub));
        let router = make_lib_router(remotes);

        let harness = test_harness_with_mock_router(router).await;

        let payload = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    9020,
                    "mempalace_get_changes_since",
                    json!({"since":"2000-01-01T00:00:00Z","limit":10}),
                ))
                .await,
        )
        .unwrap();

        assert_eq!(payload["remotes"]["hub"]["unreachable"], true);
        assert!(
            payload["remotes"]["hub"]["error"].as_str().map_or(false, |e| !e.is_empty()),
            "error message must be non-empty"
        );
        // The tool must still succeed — local events are returned, no top-level error.
        assert!(payload.get("events").is_some());
    }

    #[tokio::test]
    async fn tool_wake_up_with_federation_includes_remote_changes() {
        let mut mock_hub = LibMockRemote::default();
        mock_hub.changes_events =
            vec![make_dto_event("drawer_added", "2026-06-10T10:00:00Z", "hub-entity-1")];

        let mut remotes: BTreeMap<String, Arc<dyn mempalace_remote::RemoteApi>> = BTreeMap::new();
        remotes.insert("hub".to_owned(), Arc::new(mock_hub));
        let router = make_lib_router(remotes);

        let harness = test_harness_with_mock_router(router).await;

        let payload = decode_tool_payload(
            &harness.server.handle_request(tool_call(9030, "mempalace_wake_up", json!({}))).await,
        )
        .unwrap();

        assert!(
            payload.get("remote_changes").is_some(),
            "remote_changes must be present when federation is active: {payload}"
        );
        let rc = &payload["remote_changes"]["hub"];
        let events = rc["events"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["origin"], "remote:hub");
    }

    #[tokio::test]
    async fn tool_wake_up_with_unreachable_remote_still_succeeds() {
        let mut mock_hub = LibMockRemote::default();
        mock_hub.fail = true;

        let mut remotes: BTreeMap<String, Arc<dyn mempalace_remote::RemoteApi>> = BTreeMap::new();
        remotes.insert("hub".to_owned(), Arc::new(mock_hub));
        let router = make_lib_router(remotes);

        let harness = test_harness_with_mock_router(router).await;

        let payload = decode_tool_payload(
            &harness.server.handle_request(tool_call(9040, "mempalace_wake_up", json!({}))).await,
        )
        .unwrap();

        // wake_up must succeed even when the remote is down.
        assert!(payload.get("identity").is_some());
        // remote_changes for the down hub should be the unreachable marker.
        let rc = &payload["remote_changes"]["hub"];
        assert_eq!(rc["unreachable"], true);
        assert!(rc["error"].as_str().map_or(false, |e| !e.is_empty()));
    }

    // ─── Byte-parity: federation-off must not add remote_changes / remotes / origin ─

    #[tokio::test]
    async fn federation_none_wake_up_has_no_remote_changes_key() {
        let harness = test_harness().await;
        let payload = decode_tool_payload(
            &harness.server.handle_request(tool_call(9100, "mempalace_wake_up", json!({}))).await,
        )
        .unwrap();
        assert!(
            payload.get("remote_changes").is_none(),
            "remote_changes must NOT appear when federation is off: {payload}"
        );
    }

    #[tokio::test]
    async fn federation_none_get_changes_since_has_no_remotes_key_and_no_origin() {
        let harness = test_harness().await;

        // Add a drawer so there is at least one event in the log.
        harness
            .server
            .handle_request(tool_call(
                9110,
                "mempalace_add_drawer",
                json!({"wing":"wing_parity_test","room":"r","content":"parity test content"}),
            ))
            .await;

        let payload = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    9111,
                    "mempalace_get_changes_since",
                    json!({"since":"2000-01-01T00:00:00Z","limit":10}),
                ))
                .await,
        )
        .unwrap();

        assert!(
            payload.get("remotes").is_none(),
            "`remotes` key must NOT appear when federation is off: {payload}"
        );

        // No event should have an `origin` field.
        let events = payload["events"].as_array().unwrap();
        assert!(
            events.iter().all(|e| e.get("origin").is_none()),
            "events must NOT have `origin` when federation is off: {payload}"
        );
    }

    // ── DeleteDrawer route-matrix: runtime tests through tool_delete_drawer ────
    //
    // These tests verify that `mempalace_delete_drawer`:
    //   a) Always attempts local deletion first, regardless of write target.
    //   b) Falls back to remotes only after a local miss.
    //   c) Never attaches a `replication` field.
    //
    // The mock `RemoteApi` below is minimal — only `delete_drawer` is wired.

    struct DeleteDrawerMock {
        delete_succeeds: bool,
        delete_call_count: AtomicU64,
    }

    impl DeleteDrawerMock {
        fn delete_call_count(&self) -> u64 {
            self.delete_call_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl mempalace_remote::RemoteApi for DeleteDrawerMock {
        async fn info(&self) -> mempalace_remote::Result<mempalace_federation::InfoResponse> {
            panic!("unexpected info call")
        }
        async fn search_drawers(
            &self,
            _req: mempalace_federation::DrawerSearchRequest,
        ) -> mempalace_remote::Result<mempalace_federation::DrawerSearchResponse> {
            panic!("unexpected search_drawers call")
        }
        async fn check_duplicate(
            &self,
            _req: mempalace_federation::CheckDuplicateRequest,
        ) -> mempalace_remote::Result<mempalace_federation::CheckDuplicateResponse> {
            panic!("unexpected check_duplicate call")
        }
        async fn add_drawer(
            &self,
            _req: mempalace_federation::AddDrawerRequest,
        ) -> mempalace_remote::Result<mempalace_federation::AddDrawerResponse> {
            panic!("unexpected add_drawer call")
        }
        async fn list_drawers(
            &self,
            _query: mempalace_federation::ListDrawersQuery,
        ) -> mempalace_remote::Result<mempalace_federation::ListDrawersResponse> {
            panic!("unexpected list_drawers call")
        }
        async fn get_drawer(
            &self,
            _drawer_id: &str,
        ) -> mempalace_remote::Result<serde_json::Value> {
            panic!("unexpected get_drawer call")
        }
        async fn delete_drawer(&self, _drawer_id: &str) -> mempalace_remote::Result<()> {
            self.delete_call_count.fetch_add(1, Ordering::SeqCst);
            if self.delete_succeeds {
                Ok(())
            } else {
                Err(mempalace_remote::RemoteError::RemoteRejected {
                    remote: "mock".to_owned(),
                    status: 404,
                    body: "not found".to_owned(),
                })
            }
        }
        async fn kg_query(
            &self,
            _req: mempalace_federation::KgQueryRequest,
        ) -> mempalace_remote::Result<serde_json::Value> {
            panic!("unexpected kg_query call")
        }
        async fn kg_add_fact(
            &self,
            _req: mempalace_federation::KgAddFactRequest,
        ) -> mempalace_remote::Result<serde_json::Value> {
            panic!("unexpected kg_add_fact call")
        }
        async fn kg_invalidate(
            &self,
            _req: mempalace_federation::KgInvalidateRequest,
        ) -> mempalace_remote::Result<serde_json::Value> {
            panic!("unexpected kg_invalidate call")
        }
        async fn kg_timeline(
            &self,
            _entity: Option<&str>,
        ) -> mempalace_remote::Result<serde_json::Value> {
            panic!("unexpected kg_timeline call")
        }
        async fn kg_stats(&self) -> mempalace_remote::Result<serde_json::Value> {
            panic!("unexpected kg_stats call")
        }
        async fn taxonomy(&self) -> mempalace_remote::Result<serde_json::Value> {
            panic!("unexpected taxonomy call")
        }
        async fn wings(&self) -> mempalace_remote::Result<serde_json::Value> {
            panic!("unexpected wings call")
        }
        async fn rooms(&self, _wing: Option<&str>) -> mempalace_remote::Result<serde_json::Value> {
            panic!("unexpected rooms call")
        }
        async fn changes(
            &self,
            _query: mempalace_federation::ChangesQuery,
        ) -> mempalace_remote::Result<mempalace_federation::ChangesResponse> {
            panic!("unexpected changes call")
        }
        async fn ingest_batch(
            &self,
            _req: mempalace_federation::IngestBatchRequest,
        ) -> mempalace_remote::Result<mempalace_federation::IngestBatchResponse> {
            panic!("unexpected ingest_batch call")
        }
    }

    fn make_delete_drawer_rules(
        remotes: &BTreeMap<String, Arc<dyn mempalace_remote::RemoteApi>>,
        write: WriteTarget,
    ) -> FederationRuntimeConfig {
        let mut rules_remotes = BTreeMap::new();
        for name in remotes.keys() {
            rules_remotes.insert(
                name.clone(),
                ResolvedRemote {
                    name: name.clone(),
                    url: "https://mock.example".to_owned(),
                    token: None,
                    timeout: std::time::Duration::from_secs(5),
                },
            );
        }
        FederationRuntimeConfig {
            remotes: rules_remotes,
            default_mode: RouteMode::Combined,
            default_remote: remotes.keys().next().cloned(),
            wings: [(
                "wing_code".to_owned(),
                ResolvedRouteRule {
                    mode: RouteMode::Combined,
                    remote: remotes.keys().next().cloned(),
                    write,
                },
            )]
            .into(),
            kg: None,
            coordination: BTreeMap::new(),
        }
    }

    struct DeleteDrawerTestCtx {
        #[allow(dead_code)]
        _tempdir: TempDir,
        runtime: McpRuntime<DeterministicStubProvider>,
    }

    async fn make_delete_drawer_ctx(
        remotes: BTreeMap<String, Arc<dyn mempalace_remote::RemoteApi>>,
        write: WriteTarget,
    ) -> DeleteDrawerTestCtx {
        let rules = make_delete_drawer_rules(&remotes, write);
        let tempdir = TempDir::new().unwrap();
        let palace_path = tempdir.path().join("palace");
        let config = MempalaceConfig {
            federation: FederationRuntimeConfig::default(),
            ..make_base_config(&palace_path, &tempdir)
        };
        let mut runtime = McpRuntime::new(
            config,
            DeterministicStubProvider::new(EmbeddingProfile::Balanced),
            None,
        )
        .await
        .unwrap();
        // Inject mock federation — avoid real HTTP connections.
        runtime.federation = Some(FederationRouter::with_remotes(rules, remotes));
        DeleteDrawerTestCtx { _tempdir: tempdir, runtime }
    }

    #[tokio::test]
    async fn tool_delete_drawer_with_write_remote_local_hit() {
        // Given a Combined/write:Remote wing route, and a drawer that exists
        // locally, DeleteDrawer must delete locally — not forward to the remote.
        let mock = Arc::new(DeleteDrawerMock {
            delete_succeeds: true,
            delete_call_count: AtomicU64::new(0),
        });
        let mock_for_assert = mock.clone();
        let remotes =
            BTreeMap::from([("alpha".to_owned(), mock as Arc<dyn mempalace_remote::RemoteApi>)]);
        let mut ctx = make_delete_drawer_ctx(remotes, WriteTarget::Remote).await;

        // Seed a drawer into the local store so it will be found locally.
        let local_id = DrawerId::new("local-test-drawer-001").unwrap();
        let now = OffsetDateTime::now_utc();
        ctx.runtime
            .storage
            .drawer_store()
            .put_drawers(
                &[DrawerRecord {
                    id: local_id.clone(),
                    wing: WingId::new("wing_code").unwrap(),
                    room: RoomId::new("test-room").unwrap(),
                    hall: None,
                    date: Some(now.date()),
                    source_file: "test.txt".to_owned(),
                    chunk_index: 0,
                    ingest_mode: "test".to_owned(),
                    extract_mode: None,
                    added_by: "test".to_owned(),
                    filed_at: now,
                    importance: None,
                    emotional_weight: None,
                    weight: None,
                    content: "test content".to_owned(),
                    content_hash: mempalace_core::hash_text("test content"),
                    embedding: vec![0.0; EmbeddingProfile::Balanced.metadata().dimensions],
                    locator: None,
                    view_metadata: None,
                }],
                DuplicateStrategy::Error,
            )
            .await
            .unwrap();

        let result =
            ctx.runtime.tool_delete_drawer(&json!({"drawer_id": local_id.as_str()})).await.unwrap();

        assert_eq!(result["success"], true);
        assert_eq!(result["drawer_id"], local_id.as_str());
        assert_eq!(result["applied_to"], "local");
        assert!(
            !result.as_object().unwrap().contains_key("replication"),
            "DeleteDrawer must never produce a replication field; got: {result}"
        );
        assert_eq!(
            mock_for_assert.delete_call_count(),
            0,
            "write:Remote local hit must not call the remote"
        );
    }

    #[tokio::test]
    async fn tool_delete_drawer_with_write_both_local_hit() {
        // Given a Combined/write:Both wing route, and a drawer that exists
        // locally, DeleteDrawer must delete locally — no replication attempt.
        let mock = Arc::new(DeleteDrawerMock {
            delete_succeeds: true,
            delete_call_count: AtomicU64::new(0),
        });
        let mock_for_assert = mock.clone();
        let remotes =
            BTreeMap::from([("alpha".to_owned(), mock as Arc<dyn mempalace_remote::RemoteApi>)]);
        let mut ctx = make_delete_drawer_ctx(remotes, WriteTarget::Both).await;

        let local_id = DrawerId::new("local-test-drawer-002").unwrap();
        let now = OffsetDateTime::now_utc();
        ctx.runtime
            .storage
            .drawer_store()
            .put_drawers(
                &[DrawerRecord {
                    id: local_id.clone(),
                    wing: WingId::new("wing_code").unwrap(),
                    room: RoomId::new("test-room").unwrap(),
                    hall: None,
                    date: Some(now.date()),
                    source_file: "test.txt".to_owned(),
                    chunk_index: 0,
                    ingest_mode: "test".to_owned(),
                    extract_mode: None,
                    added_by: "test".to_owned(),
                    filed_at: now,
                    importance: None,
                    emotional_weight: None,
                    weight: None,
                    content: "test content".to_owned(),
                    content_hash: mempalace_core::hash_text("test content"),
                    embedding: vec![0.0; EmbeddingProfile::Balanced.metadata().dimensions],
                    locator: None,
                    view_metadata: None,
                }],
                DuplicateStrategy::Error,
            )
            .await
            .unwrap();

        let result =
            ctx.runtime.tool_delete_drawer(&json!({"drawer_id": local_id.as_str()})).await.unwrap();

        assert_eq!(result["success"], true);
        assert_eq!(result["drawer_id"], local_id.as_str());
        assert_eq!(result["applied_to"], "local");
        assert!(
            !result.as_object().unwrap().contains_key("replication"),
            "DeleteDrawer must never produce a replication field; got: {result}"
        );
        assert_eq!(
            mock_for_assert.delete_call_count(),
            0,
            "write:Both local hit must not call the remote"
        );
    }

    #[tokio::test]
    async fn tool_delete_drawer_fallback_remote_ignores_routing() {
        // Given a Combined/write:Both wing route, and a drawer that does NOT
        // exist locally, DeleteDrawer must fall back across remotes. The response
        // must report the remote origin and must NOT carry a replication field.
        let mock = Arc::new(DeleteDrawerMock {
            delete_succeeds: true,
            delete_call_count: AtomicU64::new(0),
        });
        let mock_for_assert = mock.clone();
        let remotes =
            BTreeMap::from([("alpha".to_owned(), mock as Arc<dyn mempalace_remote::RemoteApi>)]);
        let mut ctx = make_delete_drawer_ctx(remotes, WriteTarget::Both).await;

        // Do NOT seed any drawer — local delete will return 0, triggering fallback.
        let result = ctx
            .runtime
            .tool_delete_drawer(&json!({"drawer_id": "non-existent-drawer-999"}))
            .await
            .unwrap();

        assert_eq!(result["success"], true);
        assert_eq!(result["drawer_id"], "non-existent-drawer-999");
        assert_eq!(result["origin"], "alpha");
        assert_eq!(result["applied_to"], "remote:alpha");
        assert!(
            !result.as_object().unwrap().contains_key("replication"),
            "DeleteDrawer must never produce a replication field; got: {result}"
        );
        assert_eq!(
            mock_for_assert.delete_call_count(),
            1,
            "fallback must call the remote exactly once"
        );
    }

    #[tokio::test]
    async fn tool_delete_drawer_removes_diary_summary_on_local_hit() {
        // Given a diary drawer with a stored summary, local deletion must also
        // remove the SQLite summary row so stale summaries do not accumulate.
        let remotes = BTreeMap::new();
        let mut ctx = make_delete_drawer_ctx(remotes, WriteTarget::Local).await;

        let drawer_id = DrawerId::new("diary-summary-cleanup-001").unwrap();
        let now = OffsetDateTime::now_utc();
        let drawer = test_diary_drawer(drawer_id.as_str(), "test content", now);
        ctx.runtime
            .storage
            .drawer_store()
            .put_drawers(&[drawer], DuplicateStrategy::Error)
            .await
            .unwrap();

        ctx.runtime
            .storage
            .operational_store()
            .store_diary_summary(&drawer_id, "summary to be removed")
            .unwrap();

        // Sanity-check that the summary exists before deletion.
        assert!(
            ctx.runtime
                .storage
                .operational_store()
                .get_diary_summary(&drawer_id)
                .unwrap()
                .is_some()
        );

        ctx.runtime.tool_delete_drawer(&json!({"drawer_id": drawer_id.as_str()})).await.unwrap();

        let stored = ctx.runtime.storage.operational_store().get_diary_summary(&drawer_id).unwrap();
        assert!(
            stored.is_none(),
            "local diary drawer deletion must remove its SQLite summary; got: {stored:?}"
        );
    }

    #[tokio::test]
    async fn tool_delete_drawer_does_not_remove_diary_summary_on_remote_fallback() {
        // Given a remote-only fallback (drawer not found locally, remote
        // succeeds), a locally stored diary summary must NOT be removed.
        // The summary belongs to a local store and should not be discarded
        // just because the drawer lives on a remote.
        let mock = Arc::new(DeleteDrawerMock {
            delete_succeeds: true,
            delete_call_count: AtomicU64::new(0),
        });
        let remotes =
            BTreeMap::from([("alpha".to_owned(), mock as Arc<dyn mempalace_remote::RemoteApi>)]);
        let mut ctx = make_delete_drawer_ctx(remotes, WriteTarget::Both).await;

        let drawer_id = DrawerId::new("diary-summary-remote-fallback-001").unwrap();

        // Store a summary for a drawer that does NOT exist locally.
        ctx.runtime
            .storage
            .operational_store()
            .store_diary_summary(&drawer_id, "summary must survive fallback")
            .unwrap();

        // Verify the summary is present before the deletion attempt.
        assert!(
            ctx.runtime
                .storage
                .operational_store()
                .get_diary_summary(&drawer_id)
                .unwrap()
                .is_some()
        );

        let result = ctx
            .runtime
            .tool_delete_drawer(&json!({"drawer_id": drawer_id.as_str()}))
            .await
            .unwrap();

        assert_eq!(result["success"], true);
        assert_eq!(result["origin"], "alpha");
        assert_eq!(result["applied_to"], "remote:alpha");

        // The locally stored summary must still be present.
        let stored = ctx.runtime.storage.operational_store().get_diary_summary(&drawer_id).unwrap();
        assert!(
            stored.is_some(),
            "remote-only fallback must NOT remove a locally stored diary summary"
        );
    }

    #[tokio::test]
    async fn tool_delete_drawer_local_records_wing_and_room_on_change_event() {
        // The federation server's `/v1/changes` route now fails closed on a
        // `drawer_deleted` event whose wing cannot be determined (see
        // `change_event_visible` in crates/mempalace-server/src/lib.rs and
        // docs/Federation.md §1.5) — so a local deletion here must record the
        // drawer's wing/room on the change event, not leave it opaque.
        let remotes = BTreeMap::new();
        let mut ctx = make_delete_drawer_ctx(remotes, WriteTarget::Local).await;

        let drawer_id = DrawerId::new("wing-room-recording-001").unwrap();
        let now = OffsetDateTime::now_utc();
        let content = "content to be deleted for the wing recording test";
        ctx.runtime
            .storage
            .drawer_store()
            .put_drawers(
                &[DrawerRecord {
                    id: drawer_id.clone(),
                    wing: WingId::new("wing_alpha").unwrap(),
                    room: RoomId::new("alpha-room").unwrap(),
                    hall: None,
                    date: Some(now.date()),
                    source_file: "test.txt".to_owned(),
                    chunk_index: 0,
                    ingest_mode: "test".to_owned(),
                    extract_mode: None,
                    added_by: "test".to_owned(),
                    filed_at: now,
                    importance: None,
                    emotional_weight: None,
                    weight: None,
                    content: content.to_owned(),
                    content_hash: mempalace_core::hash_text(content),
                    embedding: vec![0.0; EmbeddingProfile::Balanced.metadata().dimensions],
                    locator: None,
                    view_metadata: None,
                }],
                DuplicateStrategy::Error,
            )
            .await
            .unwrap();

        let result =
            ctx.runtime.tool_delete_drawer(&json!({"drawer_id": drawer_id.as_str()})).await.unwrap();
        assert_eq!(result["success"], true);

        let changes = ctx.runtime.tool_get_changes_since(&json!({})).await.unwrap();
        let events = changes["events"].as_array().unwrap();
        let deleted_event = events
            .iter()
            .find(|e| e["event_type"] == "drawer_deleted" && e["entity_id"] == drawer_id.as_str())
            .unwrap_or_else(|| panic!("no drawer_deleted event found for {drawer_id}: {events:?}"));
        assert_eq!(deleted_event["details"]["wing"], "wing_alpha");
        assert_eq!(deleted_event["details"]["room"], "alpha-room");
    }

    async fn test_harness_with_federation(federation: FederationRuntimeConfig) -> TestHarness {
        let tempdir = TempDir::new().unwrap();
        let palace_path = tempdir.path().join("palace");
        let config = MempalaceConfig { federation, ..make_base_config(&palace_path, &tempdir) };
        let server = McpServer::from_parts(
            config,
            DeterministicStubProvider::new(EmbeddingProfile::Balanced),
        )
        .await
        .unwrap();
        seed_drawers(&server).await;
        seed_knowledge_graph(&server).await;
        TestHarness { _tempdir: tempdir, server }
    }

    fn make_base_config(palace_path: &std::path::Path, tempdir: &TempDir) -> MempalaceConfig {
        MempalaceConfig {
            schema_version: 1,
            collection_name: "mempalace_drawers".to_owned(),
            palace_path: palace_path.to_path_buf(),
            embedding_profile: EmbeddingProfile::Balanced,
            low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
            server: ServerRuntimeConfig {
                bind: "127.0.0.1:8765".parse().unwrap(),
                token_file: tempdir.path().join("server_tokens.json"),
                checkouts: std::collections::BTreeMap::new(),
            },
            federation: FederationRuntimeConfig::default(),
            maintenance: MaintenanceRuntimeConfig::defaults(),
        }
    }
}

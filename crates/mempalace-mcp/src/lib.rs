#![allow(missing_docs)]

mod federation;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use blake3::Hasher;
use mempalace_config::{ConfigLoader, MempalaceConfig, ReplicationStatus, RouteMode, WriteTarget};
use mempalace_core::{
    DIARY_HALL, DIARY_ROOM, DIARY_TOPIC_PREFIX, DrawerId, DrawerRecord, EmbeddingProfile, RoomId,
    SHARED_AGENT_DIARY_WING, SearchQuery, WingId,
};
use mempalace_embeddings::{
    EmbeddingError, EmbeddingProvider, EmbeddingRequest, FastembedProvider,
    FastembedProviderConfig, env_flag,
};
use mempalace_graph::{
    AddFactRequest, EntityKind, KnowledgeGraphRuntime, PalaceGraphSnapshot, QueryDirection,
    derive_palace_graph_from_store, find_tunnels, traverse_graph,
};
use mempalace_search::{SearchRuntime, SearchRuntimePolicy};
use mempalace_storage::{
    ChangeEvent, ChangeLogStore, DrawerFilter, DrawerStore, DuplicateStrategy, IngestCommitRequest,
    StorageEngine,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use time::{Date, Duration, OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Mutex, Semaphore, TryAcquireError};

pub use mempalace_core as core;
use federation::FederationRouter;

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
// **Always local — never federated:**
//   DiaryWrite, DiaryRead, WakeUp, GetChangesSince, Traverse, FindTunnels,
//   GraphStats, IdentityRead, IdentityUpdate, GetAaaKSpec
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

pub const PALACE_PROTOCOL: &str = "IMPORTANT — MemPalace Memory Protocol:\n1. ON WAKE-UP: Call mempalace_wake_up with agent_name to load identity, palace status, recent changes, current project context, and recent diaries across agents.\n2. BEFORE RESPONDING about any person, project, or past event: call mempalace_kg_query or mempalace_search FIRST. Never guess — verify.\n3. IF UNSURE about a fact (name, gender, age, relationship): say \"let me check\" and query the palace. Wrong is worse than slow.\n4. AFTER EACH SESSION: call mempalace_diary_write to record what happened, what you learned, what matters.\n5. WHEN FACTS CHANGE: call mempalace_kg_invalidate on the old fact, mempalace_kg_add for the new one.\n6. WHEN IDENTITY CHANGES: call mempalace_identity_update so future sessions wake up with the corrected identity.\n\nThis protocol ensures the AI KNOWS before it speaks. Storage is not memory — but storage + this protocol = memory.";

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
}

impl ToolName {
    fn all() -> [Self; 23] {
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
        }
    }

    fn from_name(name: &str) -> Option<Self> {
        Self::all().into_iter().find(|tool| tool.as_str() == name)
    }

    fn definition(self) -> ToolDefinition {
        match self {
            Self::WakeUp => ToolDefinition {
                name: self.as_str(),
                description: "Wake up into the palace. Returns identity.txt, palace status, recent palace changes, current project history when provided, and recent diary entries across all agents. When federation is active the response also includes `remote_changes`: a per-remote map of change events from the last 24 hours (each event carries `origin: \"remote:<name>\"`), unreachable remotes appear as `{ \"unreachable\": true, \"error\": \"...\" }`, and a `next_cursor` is provided per remote for continuation via mempalace_get_changes_since.",
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "wing":{"type":"string","description":"Current project wing for project-specific history (optional, e.g. wing_myproject)"},
                        "agent_name":{"type":"string","description":"Current agent name for wake-up context (optional, e.g. claude)"},
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
                description: "Semantic search. Returns verbatim drawer content with similarity scores. Results from mined files include `stale: true` when the source file changed since mining.",
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "query":{"type":"string","description":"What to search for"},
                        "limit":{"type":"integer","description":"Max results (default 5)"},
                        "wing":{"type":"string","description":"Filter by wing (optional)"},
                        "room":{"type":"string","description":"Filter by room (optional)"}
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
                description: "Write a diary entry. Project-scoped entries are stored in the specified project wing; agent-scoped entries are stored in the shared wing_agents diary. The agent name is recorded as author attribution, not as the storage partition. Always local; never federated.",
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "agent_name":{"type":"string","description":"Your name — recorded as the diary author"},
                        "entry":{"type":"string","description":"Your diary entry"},
                        "topic":{"type":"string","description":"Topic tag (optional, default: general)"},
                        "scope":{"type":"string","description":"Where to store the entry: agent or project (optional, default: agent)","enum":["agent","project"]},
                        "wing":{"type":"string","description":"Project wing for project-scoped entries. Ignored for agent-scoped entries, which always use wing_agents."}
                    },
                    "required":["agent_name","entry"]
                }),
            },
            Self::DiaryRead => ToolDefinition {
                name: self.as_str(),
                description: "Read recent diary entries across all wings. Defaults to entries since the past day; optional filters narrow by wing, agent author, or topic. Always local; never federated.",
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "agent_name":{"type":"string","description":"Filter by diary author (optional, default: all agents)"},
                        "wing":{"type":"string","description":"Filter by wing (optional, default: all wings)"},
                        "topic":{"type":"string","description":"Filter by topic tag (optional, default: all topics)"},
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
                description: "Update identity.txt for future wake-ups. Use replace for a full corrected identity or append for a dated note.",
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
            | Self::GetAaaKSpec => ToolRoutingCategory::LocalOnly,
            Self::Search
            | Self::ListWings
            | Self::ListRooms
            | Self::GetTaxonomy
            | Self::Status
            | Self::CheckDuplicate
            | Self::AddDrawer
            | Self::DeleteDrawer => ToolRoutingCategory::RoutableDrawer,
            Self::KgQuery
            | Self::KgAdd
            | Self::KgInvalidate
            | Self::KgTimeline
            | Self::KgStats => ToolRoutingCategory::RoutableKg,
        }
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
        Self::from_parts(config, provider).await
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
        let queue_limit = config.low_cpu.effective_queue_limit().min(Semaphore::MAX_PERMITS);
        let runtime = McpRuntime::new(config, provider).await?;
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

        let result = match tool {
            ToolName::WakeUp => runtime.tool_wake_up(&call.arguments).await,
            ToolName::Status => runtime.tool_status().await,
            ToolName::ListWings => runtime.tool_list_wings().await,
            ToolName::ListRooms => runtime.tool_list_rooms(&call.arguments).await,
            ToolName::GetTaxonomy => runtime.tool_get_taxonomy().await,
            ToolName::GetAaaKSpec => runtime.tool_get_aaak_spec().await,
            ToolName::KgQuery => runtime.tool_kg_query(&call.arguments).await,
            ToolName::KgAdd => runtime.tool_kg_add(&call.arguments).await,
            ToolName::KgInvalidate => runtime.tool_kg_invalidate(&call.arguments).await,
            ToolName::KgTimeline => runtime.tool_kg_timeline(&call.arguments).await,
            ToolName::KgStats => runtime.tool_kg_stats().await,
            ToolName::Traverse => runtime.tool_traverse(&call.arguments).await,
            ToolName::FindTunnels => runtime.tool_find_tunnels(&call.arguments).await,
            ToolName::GraphStats => runtime.tool_graph_stats().await,
            ToolName::Search => runtime.tool_search(&call.arguments).await,
            ToolName::CheckDuplicate => runtime.tool_check_duplicate(&call.arguments).await,
            ToolName::AddDrawer => runtime.tool_add_drawer(&call.arguments).await,
            ToolName::DeleteDrawer => runtime.tool_delete_drawer(&call.arguments).await,
            ToolName::DiaryWrite => runtime.tool_diary_write(&call.arguments).await,
            ToolName::DiaryRead => runtime.tool_diary_read(&call.arguments).await,
            ToolName::GetChangesSince => runtime.tool_get_changes_since(&call.arguments).await,
            ToolName::IdentityRead => runtime.tool_identity_read().await,
            ToolName::IdentityUpdate => runtime.tool_identity_update(&call.arguments).await,
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
    storage: StorageEngine,
    search: SearchRuntime<P>,
    federation: Option<FederationRouter>,
}

impl<P> McpRuntime<P>
where
    P: EmbeddingProvider + Send,
{
    async fn new(config: MempalaceConfig, provider: P) -> Result<Self> {
        let storage = StorageEngine::open(&config.palace_path, config.embedding_profile).await?;
        let router = FederationRouter::new(config.federation.clone());
        let federation = if router.has_remotes() { Some(router) } else { None };
        Ok(Self {
            search: SearchRuntime::with_policy(
                provider,
                SearchRuntimePolicy { rerank_enabled: config.low_cpu.effective_rerank_enabled() },
            ),
            config,
            storage,
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
        let status = self.status_payload().await?;
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
                let remote_changes = router
                    .changes_fanout(Some(fan_since), Some(latest_limit), &cursors)
                    .await;
                payload["remote_changes"] = json!(remote_changes);
            }
        }

        Ok(payload)
    }

    async fn tool_status(&mut self) -> ToolResult<Value> {
        let mut payload = self.status_payload().await?;
        if let Some(router) = &self.federation {
            payload = router.status_merge(payload).await?;
            let local_wings: BTreeMap<String, usize> = payload["wings"]
                .as_object()
                .map(|obj| obj.iter().filter_map(|(k, v)| v.as_u64().map(|n| (k.clone(), n as usize))).collect())
                .unwrap_or_default();
            if router.has_remotes() {
                payload["wing_availability"] = router.wing_availability(&local_wings);
            }
        }
        Ok(payload)
    }

    async fn status_payload(&mut self) -> ToolResult<Value> {
        let drawers = self.list_all_drawers().await?;
        let mut wings = BTreeMap::<String, usize>::new();
        let mut rooms = BTreeMap::<String, usize>::new();
        for drawer in &drawers {
            *wings.entry(drawer.wing.as_str().to_owned()).or_default() += 1;
            *rooms.entry(drawer.room.as_str().to_owned()).or_default() += 1;
        }
        Ok(json!({
            "total_drawers": drawers.len(),
            "wings": wings,
            "rooms": rooms,
            "palace_path": self.config.palace_path,
            "protocol": PALACE_PROTOCOL,
            "aaak_dialect": AAAK_SPEC,
        }))
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
                .map(|obj| obj.iter().filter_map(|(k, v)| v.as_u64().map(|n| (k.clone(), n as usize))).collect())
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

        // ── Federation path ──
        if let Some(router) = &self.federation {
            let wing_str = wing.as_ref().map(|w| w.as_str());
            let room_str = room.as_ref().map(|r| r.as_str());
            let (include_local, remote_targets) =
                router.plan_search_targets(wing_str, room_str);
            if !remote_targets.is_empty() {
                // Fan out: run local search when include_local, then merge with remotes.
                let local_values: Vec<Value> = if include_local {
                    self.search
                        .search(
                            self.storage.drawer_store(),
                            &SearchQuery {
                                text: query.clone(),
                                wing: wing.clone(),
                                room: room.clone(),
                                limit,
                                profile: self.config.embedding_profile,
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
                            obj
                        })
                        .collect()
                } else {
                    vec![]
                };
                return router
                    .search(local_values, &query, wing_str, room_str, limit, &remote_targets)
                    .await;
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
                },
            )
            .await
            .map_tool()?;

        let payload = json!({
            "query": query,
            "filters": {
                "wing": wing.map(|value| value.to_string()),
                "room": room.map(|value| value.to_string()),
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
                obj
            }).collect::<Vec<_>>()
        });
        Ok(payload)
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
                        && d.get("content_hash")
                            .and_then(|h| h.as_str())
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
        let deleted = self
            .storage
            .drawer_store()
            .delete_drawers(std::slice::from_ref(&drawer_id))
            .await
            .map_tool()?;
        if deleted == 0 {
            // ── Federation fallback ──
            if let Some(router) = &self.federation {
                if let Some(remote_resp) =
                    router.delete_drawer_remote(drawer_id.as_str()).await?
                {
                    self.log_change(ChangeEvent {
                        event_type: "drawer_deleted".to_owned(),
                        occurred_at: OffsetDateTime::now_utc(),
                        entity_id: drawer_id.as_str().to_owned(),
                        actor: None,
                        details_json: Some(
                            json!({"origin": remote_resp["origin"]}).to_string(),
                        ),
                    });
                    return Ok(remote_resp);
                }
            }
            return Ok(json!({
                "success": false,
                "error": format!("Drawer not found: {}", drawer_id.as_str()),
            }));
        }
        self.log_change(ChangeEvent {
            event_type: "drawer_deleted".to_owned(),
            occurred_at: OffsetDateTime::now_utc(),
            entity_id: drawer_id.as_str().to_owned(),
            actor: None,
            details_json: None,
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

        self.storage
            .commit_ingest(IngestCommitRequest {
                ingest_kind: "diary".to_owned(),
                source_key: format!("diary:{}", drawer_id.as_str()),
                source_file,
                content_hash: record.content_hash.clone(),
                drawers: vec![record],
                duplicate_strategy: DuplicateStrategy::Error,
            })
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
        }))
    }

    async fn tool_diary_read(&mut self, arguments: &Value) -> ToolResult<Value> {
        let filters = DiaryReadFilters::from_arguments(arguments)?;
        self.diary_read_payload(filters).await
    }

    async fn diary_read_payload(&mut self, filters: DiaryReadFilters) -> ToolResult<Value> {
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
                "since": format_rfc3339(filters.since)?,
                "entries": [],
                "message": "No diary entries yet.",
            }));
        }

        drawers.sort_by(|left, right| right.filed_at.cmp(&left.filed_at));
        let total = drawers.len();
        let entries = drawers
            .into_iter()
            .take(filters.last_n)
            .map(|drawer| render_diary_entry(drawer, true))
            .collect::<ToolResult<Vec<_>>>()?;

        Ok(json!({
            "scope": "all_wings",
            "agent": filters.agent_name.clone(),
            "wing": filters.wing.as_ref().map(|wing| wing.as_str()),
            "topic": filters.topic.clone(),
            "since": format_rfc3339(filters.since)?,
            "entries": entries,
            "total": total,
            "showing": total.min(filters.last_n),
        }))
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
            .map(|drawer| render_diary_entry(drawer, true))
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
        let facts = runtime.query_entity(&entity, as_of, direction).map_tool_internal()?;
        let count = facts.len();
        let mut payload = json!({
            "entity": entity,
            "as_of": optional_string(arguments, "as_of")?,
            "facts": facts,
            "count": count,
        });
        // ── Federation path ──
        if let Some(router) = &self.federation {
            let route = router.resolve_kg_route();
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
                        .kg_add_replicate(
                            &sub, &pred, &obj,
                            valid_from_text.as_deref(),
                            route,
                        )
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
            "ended": ended_text.unwrap_or_else(|| "today".to_owned()),
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
                        .kg_invalidate_replicate(
                            &sub, &pred, &obj,
                            ended_text.as_deref(),
                            route,
                        )
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
        let timeline = runtime.timeline(entity.as_deref()).map_tool_internal()?;
        let count = timeline.len();
        let mut payload = json!({
            "entity": entity.clone().unwrap_or_else(|| "all".to_owned()),
            "timeline": timeline,
            "count": count,
        });
        // ── Federation path ──
        if let Some(router) = &self.federation {
            let route = router.resolve_kg_route();
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
                        Some(s) => { out.insert(k.clone(), s.to_owned()); }
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

            let remote_results = router
                .changes_fanout(since_str.clone(), Some(limit), &cursors)
                .await;

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
                event["occurred_at"]
                    .as_str()
                    .and_then(|s| OffsetDateTime::parse(s, &Rfc3339).ok())
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

fn optional_string(arguments: &Value, field: &'static str) -> ToolResult<Option<String>> {
    match arguments.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(ToolError::InvalidParams(format!("field `{field}` must be a string"))),
    }
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

fn format_date(value: Date) -> String {
    value.to_string()
}

#[derive(Debug, Clone)]
struct DiaryReadFilters {
    agent_name: Option<String>,
    wing: Option<WingId>,
    topic: Option<String>,
    since: OffsetDateTime,
    last_n: usize,
}

impl DiaryReadFilters {
    fn from_arguments(arguments: &Value) -> ToolResult<Self> {
        let now = OffsetDateTime::now_utc();
        let agent_name = optional_string(arguments, "agent_name")?;
        let wing =
            optional_string(arguments, "wing")?.map(|wing| parse_wing_id(&wing)).transpose()?;
        let topic = optional_string(arguments, "topic")?;
        let since = optional_string(arguments, "since")?
            .as_deref()
            .map(parse_since_timestamp)
            .transpose()?
            .unwrap_or_else(|| now - Duration::days(1));
        let last_n = optional_usize(arguments, "last_n")?.unwrap_or(10);
        Ok(Self { agent_name, wing, topic, since, last_n })
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
    if drawer.ingest_mode != "diary" || drawer.filed_at < filters.since {
        return false;
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

fn render_diary_entry(drawer: DrawerRecord, include_agent: bool) -> ToolResult<Value> {
    let topic = diary_entry_topic(&drawer).to_owned();
    let date = drawer.date.map(format_date).unwrap_or_else(|| drawer.filed_at.date().to_string());
    let timestamp = format_rfc3339(drawer.filed_at)?;
    let mut entry = json!({
        "date": date,
        "timestamp": timestamp,
        "wing": drawer.wing.as_str(),
        "topic": topic,
        "content": drawer.content,
    });
    if include_agent {
        entry["agent"] = json!(drawer.added_by);
    }
    Ok(entry)
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
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::mpsc;

    use mempalace_config::{
        FederationRuntimeConfig, LowCpuRuntimeConfig, ResolvedRemote, ResolvedRouteRule, RouteMode,
        ServerRuntimeConfig, WriteTarget,
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
            },
        ];
        runtime
            .storage
            .drawer_store()
            .put_drawers(&drawers, DuplicateStrategy::Error)
            .await
            .unwrap();
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
        assert_eq!(serde_json::to_value(actual).unwrap(), expected);
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
        assert_eq!(payload["filters"], json!({"wing":null,"room":null}));
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
                json!({"agent_name":"Wake Bot","entry":"SESSION:wakeup.changed","topic":"wakeup"}),
            ))
            .await;
        harness
            .server
            .handle_request(tool_call(
                602,
                "mempalace_diary_write",
                json!({"agent_name":"Other Bot","entry":"SESSION:other.agent.changed","topic":"handoff"}),
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
        assert_eq!(payload["status"]["total_drawers"], 5);
        assert_eq!(payload["status"]["protocol"], PALACE_PROTOCOL);
        assert_eq!(payload["status"]["aaak_dialect"], AAAK_SPEC);
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
        assert!(diary_entries.iter().any(|entry| entry["content"] == "SESSION:recent-00"));
        assert!(diary_entries.iter().any(|entry| entry["content"] == "SESSION:recent-11"));
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
        assert!(diary_entries.iter().any(|entry| entry["content"] == "SESSION:old-11"));
        assert!(diary_entries.iter().any(|entry| entry["content"] == "SESSION:old-02"));
        assert!(!diary_entries.iter().any(|entry| entry["content"] == "SESSION:old-01"));
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
        assert!(diary_entries.iter().any(|entry| entry["content"] == "SESSION:since-00"));
        assert!(diary_entries.iter().any(|entry| entry["content"] == "SESSION:since-11"));
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
                json!({"agent_name":"Codex Bot","entry":"SESSION:2026-04-11|phase8.done","topic":"phase8"}),
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
    }

    #[tokio::test]
    async fn diary_write_stores_project_and_agent_entries_in_context_wings() {
        let harness = test_harness().await;
        let first = harness
            .server
            .handle_request(tool_call(
                90,
                "mempalace_diary_write",
                json!({"agent_name":"Worker-One","entry":"SESSION:project","topic":"ops","scope":"project","wing":"wing_mempalace-rs"}),
            ))
            .await;
        let second = harness
            .server
            .handle_request(tool_call(
                91,
                "mempalace_diary_write",
                json!({"agent_name":"Worker One","entry":"SESSION:agent","topic":"ops","scope":"agent","wing":"wing_ignored"}),
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
            json!({"agent_name":"Worker One","entry":"SESSION:A","topic":"ops"}),
        ));
        let second = harness.server.handle_request(tool_call(
            11,
            "mempalace_diary_write",
            json!({"agent_name":"Worker Two","entry":"SESSION:B","topic":"ops"}),
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
                        json!({"agent_name":"Worker-One","entry":"SESSION:primary","topic":"ops"}),
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
                json!({"agent_name":"change-bot","entry":"SESSION:test","topic":"changefeed"}),
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
    }

    #[tokio::test]
    async fn federation_routing_does_not_panic_with_wing_rule() {
        // The dispatch path must not panic/crash when federation is configured
        // with a non-local wing rule. No live remote is wired, so no remote HTTP
        // calls are made; tools fall through to local execution.
        let harness = test_harness_with_federation(
            FederationRuntimeConfig {
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
            },
        )
        .await;

        let response = harness
            .server
            .handle_request(tool_call(
                1001,
                "mempalace_list_rooms",
                json!({"wing": "wing_code"}),
            ))
            .await;
        let payload = decode_tool_payload(&response).unwrap();
        assert_eq!(payload["wing"], "wing_code");
    }

    #[tokio::test]
    async fn federation_none_produces_byte_identical_responses() {
        // Regression: federation:None must produce responses identical to
        // having no federation configured at all.
        let harness_default = test_harness().await;
        let harness_none_fed = test_harness_with_federation(
            FederationRuntimeConfig::default(),
        )
        .await;

        let tools = [
            ("mempalace_status", json!({})),
            ("mempalace_list_wings", json!({})),
            ("mempalace_list_rooms", json!({"wing": "wing_code"})),
            ("mempalace_get_taxonomy", json!({})),
            ("mempalace_search", json!({"query": "auth migration parity", "limit": 2})),
            ("mempalace_kg_stats", json!({})),
        ];

        for (tool, args) in &tools {
            let default_resp = harness_default
                .server
                .handle_request(tool_call(2000, tool, args.clone()))
                .await;
            let none_fed_resp = harness_none_fed
                .server
                .handle_request(tool_call(2001, tool, args.clone()))
                .await;
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
        let seeded_content =
            "Code notes: auth-migration keeps search filter semantics exact while storage changes underneath.";
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
        fail: bool,
    }

    impl Default for LibMockRemote {
        fn default() -> Self {
            Self { changes_events: vec![], changes_next_cursor: None, fail: false }
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
            Ok(mempalace_federation::DrawerSearchResponse { results: vec![] })
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
            Ok(mempalace_federation::ListDrawersResponse {
                drawers: json!([]),
                next_cursor: None,
            })
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
        async fn kg_timeline(
            &self,
            _entity: Option<&str>,
        ) -> mempalace_remote::Result<Value> {
            Ok(json!({"entity":"all","timeline":[],"count":0}))
        }
        async fn kg_stats(&self) -> mempalace_remote::Result<Value> {
            Ok(json!({"entities":0,"triples":0,"current_facts":0,"expired_facts":0,"relationship_types":[]}))
        }
        async fn taxonomy(&self) -> mempalace_remote::Result<Value> {
            Ok(json!({"taxonomy":{}}))
        }
        async fn wings(&self) -> mempalace_remote::Result<Value> {
            Ok(json!({"wings":{}}))
        }
        async fn rooms(
            &self,
            _wing: Option<&str>,
        ) -> mempalace_remote::Result<Value> {
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
        };
        FederationRouter::with_remotes(rules, remotes)
    }

    async fn test_harness_with_mock_router(router: FederationRouter) -> TestHarness {
        let tempdir = TempDir::new().unwrap();
        let palace_path = tempdir.path().join("palace");
        let config = make_base_config(&palace_path, &tempdir);
        let mut runtime =
            McpRuntime::new(config, DeterministicStubProvider::new(EmbeddingProfile::Balanced))
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

    fn make_dto_event(event_type: &str, occurred_at: &str, entity_id: &str)
        -> mempalace_federation::ChangeEventDto
    {
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
        mock_hub.changes_events = vec![
            make_dto_event("drawer_added", "2026-06-10T12:00:00Z", "remote-entity-1"),
        ];
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
        mock_hub.changes_events = vec![
            make_dto_event("drawer_added", "2026-01-01T00:00:00Z", "early-remote"),
        ];

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
        let timestamps: Vec<&str> = events
            .iter()
            .filter_map(|e| e["occurred_at"].as_str())
            .collect();
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
        mock_hub.changes_events = vec![
            make_dto_event("drawer_added", "2026-06-10T10:00:00Z", "hub-entity-1"),
        ];

        let mut remotes: BTreeMap<String, Arc<dyn mempalace_remote::RemoteApi>> = BTreeMap::new();
        remotes.insert("hub".to_owned(), Arc::new(mock_hub));
        let router = make_lib_router(remotes);

        let harness = test_harness_with_mock_router(router).await;

        let payload = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(9030, "mempalace_wake_up", json!({})))
                .await,
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
            &harness
                .server
                .handle_request(tool_call(9040, "mempalace_wake_up", json!({})))
                .await,
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
            &harness
                .server
                .handle_request(tool_call(9100, "mempalace_wake_up", json!({})))
                .await,
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
        async fn get_drawer(&self, _drawer_id: &str) -> mempalace_remote::Result<serde_json::Value> {
            panic!("unexpected get_drawer call")
        }
        async fn delete_drawer(&self, _drawer_id: &str) -> mempalace_remote::Result<()> {
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
            rules_remotes.insert(name.clone(), ResolvedRemote {
                name: name.clone(),
                url: "https://mock.example".to_owned(),
                token: None,
                timeout: std::time::Duration::from_secs(5),
            });
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
        let mut runtime = McpRuntime::new(config, DeterministicStubProvider::new(EmbeddingProfile::Balanced))
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
        let remotes = BTreeMap::from([(
            "alpha".to_owned(),
            Arc::new(DeleteDrawerMock { delete_succeeds: true }) as Arc<dyn mempalace_remote::RemoteApi>,
        )]);
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
                }],
                DuplicateStrategy::Error,
            )
            .await
            .unwrap();

        let result = ctx
            .runtime
            .tool_delete_drawer(&json!({"drawer_id": local_id.as_str()}))
            .await
            .unwrap();

        assert_eq!(result["success"], true);
        assert_eq!(result["drawer_id"], local_id.as_str());
        assert_eq!(result["applied_to"], "local");
        assert!(
            !result.as_object().unwrap().contains_key("replication"),
            "DeleteDrawer must never produce a replication field; got: {result}"
        );
    }

    #[tokio::test]
    async fn tool_delete_drawer_with_write_both_local_hit() {
        // Given a Combined/write:Both wing route, and a drawer that exists
        // locally, DeleteDrawer must delete locally — no replication attempt.
        let remotes = BTreeMap::from([(
            "alpha".to_owned(),
            Arc::new(DeleteDrawerMock { delete_succeeds: true }) as Arc<dyn mempalace_remote::RemoteApi>,
        )]);
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
                }],
                DuplicateStrategy::Error,
            )
            .await
            .unwrap();

        let result = ctx
            .runtime
            .tool_delete_drawer(&json!({"drawer_id": local_id.as_str()}))
            .await
            .unwrap();

        assert_eq!(result["success"], true);
        assert_eq!(result["drawer_id"], local_id.as_str());
        assert_eq!(result["applied_to"], "local");
        assert!(
            !result.as_object().unwrap().contains_key("replication"),
            "DeleteDrawer must never produce a replication field; got: {result}"
        );
    }

    #[tokio::test]
    async fn tool_delete_drawer_fallback_remote_ignores_routing() {
        // Given a Combined/write:Both wing route, and a drawer that does NOT
        // exist locally, DeleteDrawer must fall back across remotes. The response
        // must report the remote origin and must NOT carry a replication field.
        let remotes = BTreeMap::from([(
            "alpha".to_owned(),
            Arc::new(DeleteDrawerMock { delete_succeeds: true }) as Arc<dyn mempalace_remote::RemoteApi>,
        )]);
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
    }

    async fn test_harness_with_federation(federation: FederationRuntimeConfig) -> TestHarness {
        let tempdir = TempDir::new().unwrap();
        let palace_path = tempdir.path().join("palace");
        let config = MempalaceConfig {
            federation,
            ..make_base_config(&palace_path, &tempdir)
        };
        let server =
            McpServer::from_parts(config, DeterministicStubProvider::new(EmbeddingProfile::Balanced))
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
        }
    }
}

#![allow(missing_docs)]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use blake3::Hasher;
use mempalace_config::{ConfigLoader, LowCpuRuntimeConfig, MempalaceConfig};
use mempalace_core::{DrawerId, DrawerRecord, EmbeddingProfile, RoomId, SearchQuery, WingId};
use mempalace_embeddings::{
    EmbeddingError, EmbeddingProvider, EmbeddingRequest, FastembedProvider,
    FastembedProviderConfig, StartupValidation, StartupValidationStatus, env_flag,
};
use mempalace_graph::{
    AddFactRequest, EntityKind, KnowledgeGraphRuntime, PalaceGraphSnapshot, QueryDirection,
    derive_palace_graph_from_store, find_tunnels, traverse_graph,
};
use mempalace_search::{SearchRuntime, SearchRuntimePolicy};
use mempalace_storage::{
    DrawerFilter, DrawerStore, DuplicateStrategy, IngestCommitRequest, StorageEngine,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use time::{Date, OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Mutex, Semaphore, TryAcquireError};

pub use mempalace_core as core;

const SERVER_NAME: &str = "mempalace";
const SERVER_VERSION: &str = "2.0.0";
const PROTOCOL_VERSION: &str = "2024-11-05";
const DEFAULT_DUPLICATE_THRESHOLD: f32 = 0.9;
const DUPLICATE_SEARCH_LIMIT: usize = 5;
const DIARY_ROOM: &str = "diary";
const DIARY_HALL: &str = "hall_diary";
const DIARY_TOPIC_PREFIX: &str = "diary:";

pub const PALACE_PROTOCOL: &str = "IMPORTANT — MemPalace Memory Protocol:\n1. ON WAKE-UP: Call mempalace_status to load palace overview + AAAK spec.\n2. BEFORE RESPONDING about any person, project, or past event: call mempalace_kg_query or mempalace_search FIRST. Never guess — verify.\n3. IF UNSURE about a fact (name, gender, age, relationship): say \"let me check\" and query the palace. Wrong is worse than slow.\n4. AFTER EACH SESSION: call mempalace_diary_write to record what happened, what you learned, what matters.\n5. WHEN FACTS CHANGE: call mempalace_kg_invalidate on the old fact, mempalace_kg_add for the new one.\n\nThis protocol ensures the AI KNOWS before it speaks. Storage is not memory — but storage + this protocol = memory.";

pub const AAAK_SPEC: &str = "AAAK is a compressed memory dialect that MemPalace uses for efficient storage.\nIt is designed to be readable by both humans and LLMs without decoding.\n\nFORMAT:\n  ENTITIES: 3-letter uppercase codes. ALC=Alice, JOR=Jordan, RIL=Riley, MAX=Max, BEN=Ben.\n  EMOTIONS: *action markers* before/during text. *warm*=joy, *fierce*=determined, *raw*=vulnerable, *bloom*=tenderness.\n  STRUCTURE: Pipe-separated fields. FAM: family | PROJ: projects | ⚠: warnings/reminders.\n  DATES: ISO format (2026-03-31). COUNTS: Nx = N mentions (e.g., 570x).\n  IMPORTANCE: ★ to ★★★★★ (1-5 scale).\n  HALLS: hall_facts, hall_events, hall_discoveries, hall_preferences, hall_advice.\n  WINGS: wing_user, wing_agent, wing_team, wing_code, wing_myproject, wing_hardware, wing_ue5, wing_ai_research.\n  ROOMS: Hyphenated slugs representing named ideas (e.g., chromadb-setup, gpu-pricing).\n\nEXAMPLE:\n  FAM: ALC→♡JOR | 2D(kids): RIL(18,sports) MAX(11,chess+swimming) | BEN(contributor)\n\nRead AAAK naturally — expand codes mentally, treat *markers* as emotional context.\nWhen WRITING AAAK: use entity codes, mark emotions, keep structure tight.";

#[derive(Debug, Clone)]
pub struct DeterministicStubProvider {
    profile: EmbeddingProfile,
}

impl DeterministicStubProvider {
    pub fn new(profile: EmbeddingProfile) -> Self {
        Self { profile }
    }

    fn vector_for(&self, text: &str) -> Vec<f32> {
        let lower = text.to_ascii_lowercase();
        let seed = if ["auth", "migration", "parity"].iter().any(|token| lower.contains(token)) {
            [1.0, 0.0, 0.0, 0.0]
        } else if ["session", "diary", "ops"].iter().any(|token| lower.contains(token)) {
            [0.0, 1.0, 0.0, 0.0]
        } else if ["rust", "cli"].iter().any(|token| lower.contains(token)) {
            [0.0, 0.0, 1.0, 0.0]
        } else {
            [0.0, 0.0, 0.0, 1.0]
        };
        let mut values = Vec::with_capacity(self.profile.metadata().dimensions);
        while values.len() < self.profile.metadata().dimensions {
            values.extend(seed);
        }
        values.truncate(self.profile.metadata().dimensions);
        values
    }
}

impl EmbeddingProvider for DeterministicStubProvider {
    fn profile(&self) -> &'static mempalace_core::EmbeddingProfileMetadata {
        self.profile.metadata()
    }

    fn startup_validation(&self) -> mempalace_embeddings::Result<StartupValidation> {
        Ok(StartupValidation {
            status: StartupValidationStatus::Ready,
            cache_root: PathBuf::from("/tmp/stub"),
            model_id: self.profile.metadata().model_id,
            detail: "stub".to_owned(),
        })
    }

    fn embed(
        &mut self,
        request: &EmbeddingRequest,
    ) -> mempalace_embeddings::Result<mempalace_embeddings::EmbeddingResponse> {
        mempalace_embeddings::EmbeddingResponse::from_vectors(
            request.texts().iter().map(|text| self.vector_for(text)).collect(),
            self.profile.metadata().dimensions,
            self.profile,
            self.profile.metadata().model_id,
        )
    }
}

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
enum ToolName {
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
}

impl ToolName {
    fn all() -> [Self; 19] {
        [
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
        ]
    }

    fn as_str(self) -> &'static str {
        match self {
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
        }
    }

    fn from_name(name: &str) -> Option<Self> {
        Self::all().into_iter().find(|tool| tool.as_str() == name)
    }

    fn definition(self) -> ToolDefinition {
        match self {
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
                description: "Walk the palace graph from a room. Shows connected ideas across wings — the tunnels. Like following a thread through the palace: start at 'chromadb-setup' in wing_code, discover it connects to wing_myproject (planning) and wing_user (feelings about it).",
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
                description: "Find rooms that bridge two wings — the hallways connecting different domains. E.g. what topics connect wing_code to wing_team?",
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
                description: "Palace graph overview: total rooms, tunnel connections, edges between wings.",
                input_schema: json!({"type":"object","properties":{}}),
            },
            Self::Search => ToolDefinition {
                name: self.as_str(),
                description: "Semantic search. Returns verbatim drawer content with similarity scores.",
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
                description: "Delete a drawer by ID. Irreversible.",
                input_schema: json!({
                    "type":"object",
                    "properties":{"drawer_id":{"type":"string","description":"ID of the drawer to delete"}},
                    "required":["drawer_id"]
                }),
            },
            Self::DiaryWrite => ToolDefinition {
                name: self.as_str(),
                description: "Write to your personal agent diary in AAAK format. Your observations, thoughts, what you worked on, what matters. Each agent has their own diary with full history. Write in AAAK for compression — e.g. 'SESSION:2026-04-04|built.palace.graph+diary.tools|ALC.req:agent.diaries.in.aaak|★★★'. Use entity codes from the AAAK spec.",
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "agent_name":{"type":"string","description":"Your name — each agent gets their own diary wing"},
                        "entry":{"type":"string","description":"Your diary entry in AAAK format — compressed, entity-coded, emotion-marked"},
                        "topic":{"type":"string","description":"Topic tag (optional, default: general)"}
                    },
                    "required":["agent_name","entry"]
                }),
            },
            Self::DiaryRead => ToolDefinition {
                name: self.as_str(),
                description: "Read your recent diary entries (in AAAK). See what past versions of yourself recorded — your journal across sessions.",
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "agent_name":{"type":"string","description":"Your name — each agent gets their own diary wing"},
                        "last_n":{"type":"integer","description":"Number of recent entries to read (default: 10)"}
                    },
                    "required":["agent_name"]
                }),
            },
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
}

impl<P> McpRuntime<P>
where
    P: EmbeddingProvider + Send,
{
    async fn new(config: MempalaceConfig, provider: P) -> Result<Self> {
        let storage = StorageEngine::open(&config.palace_path, config.embedding_profile).await?;
        Ok(Self {
            search: SearchRuntime::with_policy(
                provider,
                SearchRuntimePolicy { rerank_enabled: config.low_cpu.effective_rerank_enabled() },
            ),
            config,
            storage,
        })
    }

    async fn tool_status(&mut self) -> ToolResult<Value> {
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

    async fn tool_list_wings(&mut self) -> ToolResult<Value> {
        let drawers = self.list_all_drawers().await?;
        let mut wings = BTreeMap::<String, usize>::new();
        for drawer in drawers {
            *wings.entry(drawer.wing.as_str().to_owned()).or_default() += 1;
        }
        Ok(json!({ "wings": wings }))
    }

    async fn tool_list_rooms(&mut self, arguments: &Value) -> ToolResult<Value> {
        let wing = optional_string(arguments, "wing")?;
        let filter = DrawerFilter {
            wing: wing.as_deref().map(parse_wing_id).transpose()?,
            ..DrawerFilter::default()
        };
        let drawers = self.storage.drawer_store().list_drawers(&filter).await.map_tool()?;
        let mut rooms = BTreeMap::<String, usize>::new();
        for drawer in drawers {
            *rooms.entry(drawer.room.as_str().to_owned()).or_default() += 1;
        }
        Ok(json!({
            "wing": wing.unwrap_or_else(|| "all".to_owned()),
            "rooms": rooms,
        }))
    }

    async fn tool_get_taxonomy(&mut self) -> ToolResult<Value> {
        let drawers = self.list_all_drawers().await?;
        let mut taxonomy = BTreeMap::<String, BTreeMap<String, usize>>::new();
        for drawer in drawers {
            *taxonomy
                .entry(drawer.wing.as_str().to_owned())
                .or_default()
                .entry(drawer.room.as_str().to_owned())
                .or_default() += 1;
        }
        Ok(json!({ "taxonomy": taxonomy }))
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
            "results": results.into_iter().map(|result| json!({
                "wing": result.wing,
                "room": result.room,
                "similarity": round_similarity(result.score),
                "text": result.content,
                "source_file": result.source_file,
            })).collect::<Vec<_>>()
        });
        Ok(payload)
    }

    async fn tool_check_duplicate(&mut self, arguments: &Value) -> ToolResult<Value> {
        let content = required_string(arguments, "content")?;
        let threshold =
            optional_f32(arguments, "threshold")?.unwrap_or(DEFAULT_DUPLICATE_THRESHOLD);
        let matches = self.find_duplicates(&content, threshold).await?;
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

        let duplicates = self.find_duplicates(&content, DEFAULT_DUPLICATE_THRESHOLD).await?;
        if !duplicates.is_empty() {
            return Ok(json!({
                "success": false,
                "reason": "duplicate",
                "matches": duplicates,
            }));
        }

        let now = OffsetDateTime::now_utc();
        let drawer_id = generated_drawer_id("drawer", wing.as_str(), room.as_str(), &content, now)?;
        let record = self
            .build_drawer_record(
                drawer_id.clone(),
                wing.clone(),
                room.clone(),
                None,
                None,
                source_file.clone(),
                added_by,
                "mcp".to_owned(),
                content,
                now,
            )
            .await?;

        self.storage
            .commit_ingest(IngestCommitRequest {
                ingest_kind: "mcp_write".to_owned(),
                source_key: format!("mcp:{}", drawer_id.as_str()),
                source_file,
                content_hash: record.content_hash.clone(),
                drawers: vec![record],
                duplicate_strategy: DuplicateStrategy::Error,
            })
            .await
            .map_tool()?;

        Ok(json!({
            "success": true,
            "drawer_id": drawer_id,
            "wing": wing,
            "room": room,
        }))
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
            return Ok(json!({
                "success": false,
                "error": format!("Drawer not found: {}", drawer_id.as_str()),
            }));
        }
        Ok(json!({ "success": true, "drawer_id": drawer_id }))
    }

    async fn tool_diary_write(&mut self, arguments: &Value) -> ToolResult<Value> {
        let agent_name = required_string(arguments, "agent_name")?;
        let entry = required_string(arguments, "entry")?;
        let topic = optional_string(arguments, "topic")?.unwrap_or_else(|| "general".to_owned());
        let wing = parse_wing_id(&diary_wing_name(&agent_name))?;
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

        Ok(json!({
            "success": true,
            "entry_id": drawer_id,
            "agent": agent_name,
            "topic": topic,
            "timestamp": format_rfc3339(now)?,
        }))
    }

    async fn tool_diary_read(&mut self, arguments: &Value) -> ToolResult<Value> {
        let agent_name = required_string(arguments, "agent_name")?;
        let last_n = optional_usize(arguments, "last_n")?.unwrap_or(10);
        let room = parse_room_id(DIARY_ROOM)?;
        let primary_wing = parse_wing_id(&diary_wing_name(&agent_name))?;
        let mut drawers = self
            .storage
            .drawer_store()
            .list_drawers(&DrawerFilter {
                wing: Some(primary_wing),
                room: Some(room.clone()),
                ..DrawerFilter::default()
            })
            .await
            .map_tool()?;
        drawers.retain(|drawer| drawer.added_by == agent_name);

        let legacy_wing_name = legacy_diary_wing_name(&agent_name);
        if legacy_wing_name != diary_wing_name(&agent_name) {
            let legacy_wing = parse_wing_id(&legacy_wing_name)?;
            let mut legacy_drawers = self
                .storage
                .drawer_store()
                .list_drawers(&DrawerFilter {
                    wing: Some(legacy_wing),
                    room: Some(room),
                    ..DrawerFilter::default()
                })
                .await
                .map_tool()?;
            legacy_drawers.retain(|drawer| drawer.added_by == agent_name);
            drawers.extend(legacy_drawers);
        }

        if drawers.is_empty() {
            return Ok(json!({
                "agent": agent_name,
                "entries": [],
                "message": "No diary entries yet.",
            }));
        }

        drawers.sort_by(|left, right| right.filed_at.cmp(&left.filed_at));
        let total = drawers.len();
        let entries = drawers
            .into_iter()
            .take(last_n)
            .map(|drawer| {
                let topic = drawer
                    .source_file
                    .strip_prefix(DIARY_TOPIC_PREFIX)
                    .unwrap_or("general")
                    .to_owned();
                let date = drawer
                    .date
                    .map(format_date)
                    .unwrap_or_else(|| drawer.filed_at.date().to_string());
                let timestamp = format_rfc3339(drawer.filed_at)?;
                Ok::<Value, ToolError>(json!({
                    "date": date,
                    "timestamp": timestamp,
                    "topic": topic,
                    "content": drawer.content,
                }))
            })
            .collect::<ToolResult<Vec<_>>>()?;

        Ok(json!({
            "agent": agent_name,
            "entries": entries,
            "total": total,
            "showing": total.min(last_n),
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
        let wing_a = optional_string(arguments, "wing_a")?;
        let wing_b = optional_string(arguments, "wing_b")?;
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
        Ok(json!({
            "entity": entity,
            "as_of": optional_string(arguments, "as_of")?,
            "facts": facts,
            "count": count,
        }))
    }

    async fn tool_kg_add(&mut self, arguments: &Value) -> ToolResult<Value> {
        let subject = required_string(arguments, "subject")?;
        let predicate = required_string(arguments, "predicate")?;
        let object = required_string(arguments, "object")?;
        let valid_from_text = optional_string(arguments, "valid_from")?;
        let valid_from = valid_from_text.as_deref().map(parse_date).transpose()?;
        let source_closet = optional_string(arguments, "source_closet")?;
        let source_drawer_id =
            source_closet.as_deref().and_then(|value| parse_drawer_id(value).ok());
        let runtime = KnowledgeGraphRuntime::new(self.storage.operational_store());
        let triple_id = runtime
            .add_fact(
                AddFactRequest {
                    subject: subject.clone(),
                    subject_type: infer_entity_kind(&subject),
                    predicate: predicate.clone(),
                    object: object.clone(),
                    object_type: infer_entity_kind(&object),
                    valid_from,
                    valid_to: None,
                    confidence: 1.0,
                    source_drawer_id,
                    source_file: source_closet,
                },
                OffsetDateTime::now_utc(),
            )
            .map_tool_internal()?;
        Ok(json!({
            "success": true,
            "triple_id": triple_id,
            "fact": format!("{subject} → {predicate} → {object}"),
        }))
    }

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
        let runtime = KnowledgeGraphRuntime::new(self.storage.operational_store());
        let invalidated = runtime
            .invalidate(&subject, &predicate, &object, ended, OffsetDateTime::now_utc())
            .map_tool_internal()?;
        Ok(json!({
            "success": invalidated > 0,
            "invalidated": invalidated,
            "fact": format!("{subject} → {predicate} → {object}"),
            "ended": ended_text.unwrap_or_else(|| "today".to_owned()),
        }))
    }

    async fn tool_kg_timeline(&mut self, arguments: &Value) -> ToolResult<Value> {
        let entity = optional_string(arguments, "entity")?;
        let runtime = KnowledgeGraphRuntime::new(self.storage.operational_store());
        let timeline = runtime.timeline(entity.as_deref()).map_tool_internal()?;
        let count = timeline.len();
        Ok(json!({
            "entity": entity.clone().unwrap_or_else(|| "all".to_owned()),
            "timeline": timeline,
            "count": count,
        }))
    }

    async fn tool_kg_stats(&mut self) -> ToolResult<Value> {
        let runtime = KnowledgeGraphRuntime::new(self.storage.operational_store());
        Ok(serde_json::to_value(runtime.stats().map_tool_internal()?).map_tool_internal()?)
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
        })
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
    WingId::new(value).map_err(|error| ToolError::InvalidParams(error.to_string()))
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

fn round_similarity(value: f32) -> f32 {
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
    blake3::hash(content.as_bytes()).to_hex().to_string()
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

fn diary_wing_name(agent_name: &str) -> String {
    format!("wing_{}", diary_slugify(agent_name))
}

fn legacy_diary_wing_name(agent_name: &str) -> String {
    format!("wing_{}", legacy_slugify(agent_name))
}

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
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../tests/fixtures/phase0")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::mpsc;

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
            schema_version: 1,
            collection_name: "mempalace_drawers".to_owned(),
            palace_path,
            embedding_profile,
            low_cpu,
        };
        let server =
            McpServer::from_parts(config, DeterministicStubProvider::new(embedding_profile))
                .await
                .unwrap();
        seed_drawers(&server).await;
        seed_knowledge_graph(&server).await;
        TestHarness { _tempdir: tempdir, server }
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
        assert_eq!(read_payload["entries"][0]["topic"], "phase8");
    }

    #[tokio::test]
    async fn diary_tools_preserve_allowed_punctuation_in_wing_ids() {
        let harness = test_harness().await;
        let first = harness
            .server
            .handle_request(tool_call(
                90,
                "mempalace_diary_write",
                json!({"agent_name":"Worker-One","entry":"SESSION:dash","topic":"ops"}),
            ))
            .await;
        let second = harness
            .server
            .handle_request(tool_call(
                91,
                "mempalace_diary_write",
                json!({"agent_name":"Worker One","entry":"SESSION:space","topic":"ops"}),
            ))
            .await;
        assert_eq!(decode_tool_payload(&first).unwrap()["success"], true);
        assert_eq!(decode_tool_payload(&second).unwrap()["success"], true);

        let worker_dash = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    92,
                    "mempalace_diary_read",
                    json!({"agent_name":"Worker-One","last_n":10}),
                ))
                .await,
        )
        .unwrap();
        let worker_space = decode_tool_payload(
            &harness
                .server
                .handle_request(tool_call(
                    93,
                    "mempalace_diary_read",
                    json!({"agent_name":"Worker One","last_n":10}),
                ))
                .await,
        )
        .unwrap();

        assert_eq!(worker_dash["entries"].as_array().unwrap().len(), 1);
        assert_eq!(worker_dash["entries"][0]["content"], "SESSION:dash");
        assert_eq!(worker_space["entries"].as_array().unwrap().len(), 1);
        assert_eq!(worker_space["entries"][0]["content"], "SESSION:space");
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
                json!({"agent_name":"Worker-One","last_n":10}),
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
                    json!({"agent_name":"Worker-One","last_n":10}),
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
                    json!({"agent_name":"Worker-One","last_n":10}),
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
                    json!({"agent_name":"Worker One","last_n":10}),
                ))
                .await,
        )
        .unwrap();

        let entries = payload["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["content"], "SESSION:space-agent");
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
}

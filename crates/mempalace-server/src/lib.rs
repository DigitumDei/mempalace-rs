//! Federation HTTP server for MemPalace.
//!
//! Exposes a MemPalace palace over a REST API with bearer-token authentication.
//! All wire types are defined in `mempalace-federation`; this crate provides
//! the axum router, authentication middleware, and route handlers.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use mempalace_config::MempalaceConfig;
//! use mempalace_server::{TokenRegistry, build_router};
//! use mempalace_embeddings::DeterministicStubProvider;
//! use mempalace_core::EmbeddingProfile;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config: MempalaceConfig = todo!("load from disk");
//! let tokens = TokenRegistry::load(std::path::PathBuf::from("server_tokens.json"))?;
//! let provider = DeterministicStubProvider::new(EmbeddingProfile::Balanced);
//! let router = build_router(config, provider, tokens).await?;
//! // Bind and serve with axum::serve(listener, router).await?
//! # Ok(())
//! # }
//! ```

use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{Request, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use blake3::Hasher;
use mempalace_config::MempalaceConfig;
use mempalace_core::{
    DIARY_ROOM, DIARY_TOPIC_PREFIX, DrawerId, DrawerRecord, RoomId,
    SHARED_AGENT_DIARY_WING, SearchQuery, WingId, resolve_records,
};
use mempalace_embeddings::{EmbeddingProvider, EmbeddingRequest};
use mempalace_federation::{
    AddDrawerRequest, AddDrawerResponse, ChangesQuery, ChangesResponse, ChangeEventDto,
    CheckDuplicateRequest, CheckDuplicateResponse, DrawerSearchRequest, DrawerSearchResponse,
    ErrorBody, FEDERATION_API_VERSION, InfoResponse, KgAddFactRequest, KgInvalidateRequest,
    KgQueryRequest, ListDrawersQuery, ListDrawersResponse, RemoteDrawerResult,
};
use mempalace_graph::{AddFactRequest, EntityKind, KnowledgeGraphRuntime, QueryDirection};
use mempalace_search::{SearchRuntime, SearchRuntimePolicy};
use mempalace_storage::{
    ChangeEvent, ChangeLogStore, ChangeCursor, DrawerFilter, DrawerStore, DuplicateStrategy,
    IngestCommitRequest, StorageEngine,
};
use serde_json::{Value, json};
use subtle::ConstantTimeEq;
use thiserror::Error;
use time::{Date, OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::Mutex;
use tracing::warn;

// ─── Constants ───────────────────────────────────────────────────────────────

const DEFAULT_DUPLICATE_THRESHOLD: f32 = 0.9;
const DUPLICATE_SEARCH_LIMIT: usize = 5;
/// Default page size for list/changes endpoints.
const DEFAULT_PAGE_LIMIT: usize = 50;
/// Maximum page size clients may request.
const MAX_PAGE_LIMIT: usize = 200;

// ─── Errors ──────────────────────────────────────────────────────────────────

/// Top-level error type for the federation server.
#[derive(Debug, Error)]
pub enum ServerError {
    /// Invalid or missing request parameters.
    #[error("invalid params: {0}")]
    InvalidParams(String),
    /// Bearer token was missing, unrecognised, or disabled.
    #[error("unauthorized")]
    Unauthorized,
    /// The requested resource was not found.
    #[error("not found: {0}")]
    NotFound(String),
    /// Attempt to write a diary drawer via the federation API.
    #[error("diary drawers are not federated")]
    DiaryNotFederated,
    /// Attempted to add a drawer that is a near-duplicate of an existing one.
    #[error("duplicate detected")]
    Duplicate(Value),
    /// Propagated storage error.
    #[error(transparent)]
    Storage(#[from] mempalace_storage::StorageError),
    /// Propagated search error.
    #[error(transparent)]
    Search(#[from] mempalace_search::SearchError),
    /// Propagated graph error.
    #[error(transparent)]
    Graph(#[from] mempalace_graph::GraphError),
    /// Propagated core error.
    #[error(transparent)]
    Core(#[from] mempalace_core::MempalaceError),
    /// Propagated embedding error.
    #[error(transparent)]
    Embeddings(#[from] mempalace_embeddings::EmbeddingError),
    /// Malformed identifier (wing id, room id, drawer id).
    #[error(transparent)]
    Id(#[from] mempalace_core::IdError),
    /// JSON serialisation error.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// I/O error (e.g. reading token file).
    #[error("io error at {path}: {source}")]
    Io {
        /// Path that caused the error.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// Token file format error.
    #[error("token file error: {0}")]
    TokenFile(String),
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let (status, code, message) = match &self {
            Self::InvalidParams(msg) => {
                (StatusCode::BAD_REQUEST, "invalid_params", msg.as_str().to_owned())
            }
            Self::Id(err) => (StatusCode::BAD_REQUEST, "invalid_params", err.to_string()),
            Self::Unauthorized => {
                (StatusCode::UNAUTHORIZED, "unauthorized", "missing or invalid token".to_owned())
            }
            Self::NotFound(msg) => (StatusCode::NOT_FOUND, "not_found", msg.clone()),
            Self::DiaryNotFederated => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "diary_not_federated",
                "diary drawers cannot be read or written via the federation API".to_owned(),
            ),
            Self::Duplicate(_) => (
                StatusCode::CONFLICT,
                "duplicate",
                "near-duplicate content detected; add check_duplicate first if intentional"
                    .to_owned(),
            ),
            Self::Storage(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "storage_error",
                err.to_string(),
            ),
            Self::Search(err) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "search_error", err.to_string())
            }
            Self::Graph(err) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "graph_error", err.to_string())
            }
            Self::Core(err) => (StatusCode::INTERNAL_SERVER_ERROR, "core_error", err.to_string()),
            Self::Embeddings(err) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "embedding_error", err.to_string())
            }
            Self::Json(err) => (StatusCode::INTERNAL_SERVER_ERROR, "json_error", err.to_string()),
            Self::Io { path, source } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "io_error",
                format!("I/O error at {}: {}", path.display(), source),
            ),
            Self::TokenFile(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "token_file_error", msg.clone())
            }
        };
        let body = if let Self::Duplicate(matches) = &self {
            // 409 body includes the matches list so clients can inspect duplicates
            json!({
                "code": code,
                "message": message,
                "matches": matches,
            })
        } else {
            json!(ErrorBody { code: code.to_owned(), message })
        };
        (status, Json(body)).into_response()
    }
}

// ─── Token auth ──────────────────────────────────────────────────────────────

/// A single entry in the bearer-token file as stored on disk.
#[derive(Debug, Clone, serde::Deserialize)]
struct TokenEntry {
    /// The raw bearer token string.
    token: String,
    /// Human-readable identity name (returned as the auth principal).
    name: String,
    /// If `false`, the token is treated as non-existent during auth.
    enabled: bool,
}

/// An in-memory token entry with its bearer token pre-hashed.
///
/// Hashing once at load time keeps the raw secret out of memory and avoids
/// rehashing every stored token on each incoming request.
#[derive(Debug, Clone)]
struct TokenRegistryEntry {
    /// Human-readable identity name (returned as the auth principal).
    name: String,
    /// If `false`, the token is treated as non-existent during auth.
    enabled: bool,
    /// BLAKE3 hash of the bearer token.
    token_hash: blake3::Hash,
}

/// In-memory registry of bearer tokens loaded from a JSON file.
///
/// The file is reloaded automatically when its mtime changes.
#[derive(Debug)]
pub struct TokenRegistry {
    path: PathBuf,
    inner: RwLock<TokenRegistryInner>,
}

#[derive(Debug)]
struct TokenRegistryInner {
    entries: Vec<TokenRegistryEntry>,
    mtime: Option<SystemTime>,
}

impl TokenRegistry {
    /// Loads the token registry from `path`.
    ///
    /// The file must be a JSON array of `{"token","name","enabled"}` objects.
    /// Returns `Err` if the file exists but cannot be parsed.
    pub fn load(path: PathBuf) -> Result<Self, ServerError> {
        let inner = Self::read_file(&path)?;
        Ok(Self { path, inner: RwLock::new(inner) })
    }

    fn read_file(path: &PathBuf) -> Result<TokenRegistryInner, ServerError> {
        let raw = std::fs::read_to_string(path).map_err(|source| ServerError::Io {
            path: path.clone(),
            source,
        })?;
        let file_entries: Vec<TokenEntry> = serde_json::from_str(&raw)
            .map_err(|err| ServerError::TokenFile(err.to_string()))?;
        let entries = file_entries
            .into_iter()
            .map(|entry| TokenRegistryEntry {
                name: entry.name,
                enabled: entry.enabled,
                token_hash: blake3::hash(entry.token.as_bytes()),
            })
            .collect();
        let mtime = std::fs::metadata(path)
            .and_then(|m| m.modified())
            .ok();
        Ok(TokenRegistryInner { entries, mtime })
    }

    /// Re-reads the token file if its mtime has changed since last load.
    fn check_reload(&self) {
        let current_mtime = std::fs::metadata(&self.path).and_then(|m| m.modified()).ok();
        {
            // Fast path: read-lock to compare mtime
            let guard = match self.inner.read() {
                Ok(g) => g,
                Err(_) => return,
            };
            if guard.mtime == current_mtime {
                return;
            }
        }
        // Reload needed: acquire write lock
        let Ok(mut guard) = self.inner.write() else { return };
        // Double-check after acquiring the write lock
        if guard.mtime == current_mtime {
            return;
        }
        match Self::read_file(&self.path) {
            Ok(new_inner) => *guard = new_inner,
            Err(err) => {
                // Keep serving the last-good token set, but record the current
                // mtime so we don't retry disk I/O + write-lock on every request
                // until the file actually changes again.
                warn!("failed to reload token file: {err}");
                guard.mtime = current_mtime;
            }
        }
    }

    /// Authenticates `presented` token using constant-time comparison on BLAKE3
    /// hashes. Returns `Some(name)` for an enabled, matching token; `None` otherwise.
    pub fn authenticate(&self, presented: &str) -> Option<String> {
        self.check_reload();
        let presented_hash = blake3::hash(presented.as_bytes());
        let guard = self.inner.read().ok()?;
        for entry in &guard.entries {
            if !entry.enabled {
                continue;
            }
            let eq: bool =
                presented_hash.as_bytes().ct_eq(entry.token_hash.as_bytes()).into();
            if eq {
                return Some(entry.name.clone());
            }
        }
        None
    }
}

/// Extension type inserted into axum request extensions by the auth middleware.
#[derive(Debug, Clone)]
pub struct AuthIdentity(
    /// The authenticated identity name.
    pub String,
);

// ─── Server state ─────────────────────────────────────────────────────────────

/// Shared state for the federation server.
///
/// Wrapped in `Arc` and used as axum state.
pub struct ServerState<P> {
    /// MemPalace configuration.
    pub config: MempalaceConfig,
    /// Storage engine (drawer store + operational store).
    pub storage: StorageEngine,
    /// Search runtime. Wrapped in a `Mutex` because `SearchRuntime::search`
    /// takes `&mut self`.
    pub search: Mutex<SearchRuntime<P>>,
    /// Bearer-token registry for auth.
    pub tokens: Arc<TokenRegistry>,
}

// ─── Router builder ──────────────────────────────────────────────────────────

/// Builds the axum `Router` for the federation server.
///
/// The returned router is ready to be passed to `axum::serve`. No TCP listener
/// is created here; Stage 3 (CLI integration) wires the listener.
///
/// # Example
///
/// ```rust,no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use mempalace_config::MempalaceConfig;
/// use mempalace_server::{TokenRegistry, build_router};
/// use mempalace_embeddings::DeterministicStubProvider;
/// use mempalace_core::EmbeddingProfile;
///
/// let config: MempalaceConfig = todo!();
/// let tokens = TokenRegistry::load(std::path::PathBuf::from("server_tokens.json"))?;
/// let provider = DeterministicStubProvider::new(EmbeddingProfile::Balanced);
/// let router = build_router(config, provider, tokens).await?;
/// // let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
/// // axum::serve(listener, router).await?;
/// # Ok(())
/// # }
/// ```
pub async fn build_router<P>(
    config: MempalaceConfig,
    provider: P,
    tokens: TokenRegistry,
) -> Result<Router, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let storage = StorageEngine::open(&config.palace_path, config.embedding_profile).await?;
    let search = SearchRuntime::with_policy(
        provider,
        SearchRuntimePolicy { rerank_enabled: config.low_cpu.effective_rerank_enabled() },
    );
    let state = Arc::new(ServerState {
        config,
        storage,
        search: Mutex::new(search),
        tokens: Arc::new(tokens),
    });

    // Unauthenticated routes
    let public = Router::new().route("/v1/health", get(route_health));

    // Authenticated routes — wrapped with the auth middleware
    let protected = Router::new()
        .route("/v1/info", get(route_info::<P>))
        .route("/v1/drawers/search", post(route_drawers_search::<P>))
        .route("/v1/drawers/check_duplicate", post(route_drawers_check_duplicate::<P>))
        .route("/v1/drawers", post(route_drawers_add::<P>))
        .route("/v1/drawers", get(route_drawers_list::<P>))
        .route("/v1/drawers/{id}", get(route_drawers_get::<P>))
        .route("/v1/drawers/{id}", delete(route_drawers_delete::<P>))
        .route("/v1/kg/query", post(route_kg_query::<P>))
        .route("/v1/kg/facts", post(route_kg_add::<P>))
        .route("/v1/kg/facts/invalidate", post(route_kg_invalidate::<P>))
        .route("/v1/kg/timeline", get(route_kg_timeline::<P>))
        .route("/v1/kg/stats", get(route_kg_stats::<P>))
        .route("/v1/taxonomy", get(route_taxonomy::<P>))
        .route("/v1/wings", get(route_wings::<P>))
        .route("/v1/rooms", get(route_rooms::<P>))
        .route("/v1/changes", get(route_changes::<P>))
        .layer(middleware::from_fn_with_state(Arc::clone(&state), auth_middleware::<P>));

    Ok(public.merge(protected).with_state(state))
}

// ─── Auth middleware ──────────────────────────────────────────────────────────

async fn auth_middleware<P>(
    State(state): State<Arc<ServerState<P>>>,
    mut request: Request<Body>,
    next: Next,
) -> Response
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let token = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));

    match token.and_then(|t| state.tokens.authenticate(t)) {
        Some(identity) => {
            request.extensions_mut().insert(AuthIdentity(identity));
            next.run(request).await
        }
        None => ServerError::Unauthorized.into_response(),
    }
}

// ─── Health ──────────────────────────────────────────────────────────────────

async fn route_health() -> impl IntoResponse {
    Json(json!({"status": "ok"}))
}

// ─── Info ─────────────────────────────────────────────────────────────────────

async fn route_info<P>(
    State(state): State<Arc<ServerState<P>>>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    Ok(Json(InfoResponse {
        server_version: env!("CARGO_PKG_VERSION").to_owned(),
        federation_api_version: FEDERATION_API_VERSION,
        embedding_profile: state.config.embedding_profile.as_str().to_owned(),
        capabilities: vec![
            "drawers".to_owned(),
            "kg".to_owned(),
            "changes".to_owned(),
            "taxonomy".to_owned(),
        ],
    }))
}

// ─── Drawers: search ─────────────────────────────────────────────────────────

async fn route_drawers_search<P>(
    State(state): State<Arc<ServerState<P>>>,
    Json(body): Json<DrawerSearchRequest>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let limit = body
        .limit
        .unwrap_or(5)
        .max(1)
        .min(state.config.low_cpu.effective_search_results_limit());

    let wing = body.wing.as_deref().map(WingId::new).transpose()?;
    let room = body.room.as_deref().map(RoomId::new).transpose()?;

    let results = {
        let mut search = state.search.lock().await;
        search
            .search(
                state.storage.drawer_store(),
                &SearchQuery {
                    text: body.query,
                    wing,
                    room,
                    limit,
                    profile: state.config.embedding_profile,
                },
            )
            .await?
    };

    let results: Vec<RemoteDrawerResult> = results
        .into_iter()
        .filter(|r| !is_diary_wing_or_room(r.wing.as_str(), r.room.as_str()))
        .enumerate()
        .map(|(index, result)| RemoteDrawerResult {
            drawer_id: result
                .drawer_id
                .as_ref()
                .map(|id| id.as_str().to_owned())
                .unwrap_or_default(),
            wing: result.wing.as_str().to_owned(),
            room: result.room.as_str().to_owned(),
            rank: index + 1,
            score: result.score,
            content: result.content,
            source_file: Some(result.source_file).filter(|s| !s.is_empty()),
            content_hash: None,
            filed_at: None,
            added_by: None,
            stale: result.stale,
        })
        .collect();

    Ok(Json(DrawerSearchResponse { results }))
}

// ─── Drawers: add ────────────────────────────────────────────────────────────

async fn route_drawers_add<P>(
    State(state): State<Arc<ServerState<P>>>,
    auth: axum::extract::Extension<AuthIdentity>,
    Json(body): Json<AddDrawerRequest>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let identity = auth.0.0;

    // Reject diary-shaped writes
    if is_diary_wing_or_room(&body.wing, &body.room) {
        return Err(ServerError::DiaryNotFederated);
    }
    if let Some(sf) = &body.source_file {
        if sf.starts_with(DIARY_TOPIC_PREFIX) {
            return Err(ServerError::DiaryNotFederated);
        }
    }

    let wing = WingId::new(&body.wing)?;
    let room = RoomId::new(&body.room)?;

    // Duplicate check
    let duplicates = find_duplicates(&state, &body.content, DEFAULT_DUPLICATE_THRESHOLD).await?;
    if !duplicates.is_empty() {
        return Err(ServerError::Duplicate(
            serde_json::to_value(&duplicates).unwrap_or(Value::Array(vec![])),
        ));
    }

    // Determine added_by: identity[:claimed]
    let added_by = match &body.added_by {
        Some(claimed) if claimed != &identity => format!("{identity}:{claimed}"),
        _ => identity.clone(),
    };

    let now = OffsetDateTime::now_utc();
    let drawer_id = generated_drawer_id("drawer", wing.as_str(), room.as_str(), &body.content, now)?;
    let source_file = body.source_file.unwrap_or_default();
    let record = build_drawer_record(
        &state,
        drawer_id.clone(),
        wing.clone(),
        room.clone(),
        source_file.clone(),
        added_by.clone(),
        "federation_write".to_owned(),
        body.content,
        now,
    )
    .await?;

    state
        .storage
        .commit_ingest(IngestCommitRequest {
            ingest_kind: "federation_write".to_owned(),
            source_key: format!("federation:{}", drawer_id.as_str()),
            source_file,
            content_hash: record.content_hash.clone(),
            drawers: vec![record],
            duplicate_strategy: DuplicateStrategy::Error,
        })
        .await?;

    state.storage.operational_store().append_event(&ChangeEvent {
        event_type: "drawer_added".to_owned(),
        occurred_at: now,
        entity_id: drawer_id.as_str().to_owned(),
        actor: Some(identity),
        details_json: Some(
            json!({"wing": wing.as_str(), "room": room.as_str()}).to_string(),
        ),
    })?;

    Ok(Json(AddDrawerResponse {
        success: true,
        drawer_id: Some(drawer_id.as_str().to_owned()),
        wing: Some(wing.as_str().to_owned()),
        room: Some(room.as_str().to_owned()),
    }))
}

// ─── Drawers: check duplicate ─────────────────────────────────────────────────

async fn route_drawers_check_duplicate<P>(
    State(state): State<Arc<ServerState<P>>>,
    Json(body): Json<CheckDuplicateRequest>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let threshold = body.threshold.unwrap_or(DEFAULT_DUPLICATE_THRESHOLD);
    let matches = find_duplicates(&state, &body.content, threshold).await?;
    // Filter diary matches
    let matches: Vec<Value> = matches
        .into_iter()
        .filter(|m| {
            let wing = m.get("wing").and_then(Value::as_str).unwrap_or("");
            let room = m.get("room").and_then(Value::as_str).unwrap_or("");
            !is_diary_wing_or_room(wing, room)
        })
        .collect();
    let is_duplicate = !matches.is_empty();
    Ok(Json(CheckDuplicateResponse {
        is_duplicate,
        matches: Value::Array(matches),
    }))
}

// ─── Drawers: get by id ───────────────────────────────────────────────────────

async fn route_drawers_get<P>(
    State(state): State<Arc<ServerState<P>>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let drawer_id = DrawerId::new(&id)?;
    let mut drawer = state
        .storage
        .drawer_store()
        .get_drawer(&drawer_id)
        .await?
        .ok_or_else(|| ServerError::NotFound(format!("drawer {id} not found")))?;

    if is_diary_wing_or_room(drawer.wing.as_str(), drawer.room.as_str()) {
        return Err(ServerError::NotFound(format!("drawer {id} not found")));
    }

    // Resolve locator-backed rows before building the response.
    let stale_flags = resolve_records(std::slice::from_mut(&mut drawer));
    let stale = stale_flags.first().copied().unwrap_or(false);

    Ok(Json(drawer_record_to_json_with_stale(&drawer, stale)))
}

// ─── Drawers: delete ──────────────────────────────────────────────────────────

async fn route_drawers_delete<P>(
    State(state): State<Arc<ServerState<P>>>,
    auth: axum::extract::Extension<AuthIdentity>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let identity = auth.0.0;
    let drawer_id = DrawerId::new(&id)?;

    // Check the drawer exists and is not a diary drawer
    let drawer = state
        .storage
        .drawer_store()
        .get_drawer(&drawer_id)
        .await?
        .ok_or_else(|| ServerError::NotFound(format!("drawer {id} not found")))?;

    if is_diary_wing_or_room(drawer.wing.as_str(), drawer.room.as_str()) {
        return Err(ServerError::NotFound(format!("drawer {id} not found")));
    }

    let deleted =
        state.storage.drawer_store().delete_drawers(std::slice::from_ref(&drawer_id)).await?;

    if deleted == 0 {
        return Err(ServerError::NotFound(format!("drawer {id} not found")));
    }

    state.storage.operational_store().append_event(&ChangeEvent {
        event_type: "drawer_deleted".to_owned(),
        occurred_at: OffsetDateTime::now_utc(),
        entity_id: id,
        actor: Some(identity),
        details_json: None,
    })?;

    Ok(Json(json!({"success": true})))
}

// ─── Drawers: list ───────────────────────────────────────────────────────────

async fn route_drawers_list<P>(
    State(state): State<Arc<ServerState<P>>>,
    Query(params): Query<ListDrawersQuery>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let limit = params.limit.unwrap_or(DEFAULT_PAGE_LIMIT).max(1).min(MAX_PAGE_LIMIT);
    let wing = params.wing.as_deref().map(WingId::new).transpose()?;
    let room = params.room.as_deref().map(RoomId::new).transpose()?;

    let drawers = state
        .storage
        .drawer_store()
        .list_drawers(&DrawerFilter { wing, room, ..DrawerFilter::default() })
        .await?;

    let mut drawers_filtered: Vec<DrawerRecord> = drawers
        .into_iter()
        .filter(|d| !is_diary_wing_or_room(d.wing.as_str(), d.room.as_str()))
        .take(limit)
        .collect();

    // Resolve locator-backed rows before building the response.
    let stale_flags = resolve_records(&mut drawers_filtered);

    let items: Vec<Value> = drawers_filtered
        .iter()
        .zip(stale_flags)
        .map(|(d, stale)| drawer_record_to_json_with_stale(d, stale))
        .collect();

    // Note: the underlying store lacks cursor-based list pagination.
    // next_cursor is always None in v1 — document this limitation.
    Ok(Json(ListDrawersResponse { drawers: Value::Array(items), next_cursor: None }))
}

// ─── KG: query ───────────────────────────────────────────────────────────────

async fn route_kg_query<P>(
    State(state): State<Arc<ServerState<P>>>,
    Json(body): Json<KgQueryRequest>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let as_of = body.as_of.as_deref().map(parse_date).transpose()?;
    let direction = parse_direction(body.direction.as_deref().unwrap_or("both"))?;
    let runtime = KnowledgeGraphRuntime::new(state.storage.operational_store());
    let facts = runtime.query_entity(&body.entity, as_of, direction)?;
    let count = facts.len();
    Ok(Json(json!({
        "entity": body.entity,
        "as_of": body.as_of,
        "facts": facts,
        "count": count,
    })))
}

// ─── KG: add ─────────────────────────────────────────────────────────────────

async fn route_kg_add<P>(
    State(state): State<Arc<ServerState<P>>>,
    auth: axum::extract::Extension<AuthIdentity>,
    Json(body): Json<KgAddFactRequest>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let identity = auth.0.0;
    let valid_from = body.valid_from.as_deref().map(parse_date).transpose()?;
    let runtime = KnowledgeGraphRuntime::new(state.storage.operational_store());
    let now = OffsetDateTime::now_utc();
    let triple_id = runtime.add_fact(
        AddFactRequest {
            subject: body.subject.clone(),
            subject_type: infer_entity_kind(&body.subject),
            predicate: body.predicate.clone(),
            object: body.object.clone(),
            object_type: infer_entity_kind(&body.object),
            valid_from,
            valid_to: None,
            confidence: 1.0,
            source_drawer_id: None,
            source_file: None,
        },
        now,
    )?;

    state.storage.operational_store().append_event(&ChangeEvent {
        event_type: "kg_fact_added".to_owned(),
        occurred_at: now,
        entity_id: triple_id.clone(),
        actor: Some(identity),
        details_json: Some(
            json!({"subject": body.subject, "predicate": body.predicate, "object": body.object})
                .to_string(),
        ),
    })?;

    Ok(Json(json!({
        "success": true,
        "triple_id": triple_id,
        "fact": format!("{} → {} → {}", body.subject, body.predicate, body.object),
    })))
}

// ─── KG: invalidate ──────────────────────────────────────────────────────────

async fn route_kg_invalidate<P>(
    State(state): State<Arc<ServerState<P>>>,
    auth: axum::extract::Extension<AuthIdentity>,
    Json(body): Json<KgInvalidateRequest>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let identity = auth.0.0;
    let ended_text = body.ended.clone();
    let ended = ended_text
        .as_deref()
        .map(parse_date)
        .transpose()?
        .unwrap_or_else(|| OffsetDateTime::now_utc().date());
    let now = OffsetDateTime::now_utc();
    let runtime = KnowledgeGraphRuntime::new(state.storage.operational_store());
    let invalidated =
        runtime.invalidate(&body.subject, &body.predicate, &body.object, ended, now)?;

    if invalidated > 0 {
        state.storage.operational_store().append_event(&ChangeEvent {
            event_type: "kg_fact_invalidated".to_owned(),
            occurred_at: now,
            entity_id: format!("{} → {} → {}", body.subject, body.predicate, body.object),
            actor: Some(identity),
            details_json: Some(
                json!({"subject": body.subject, "predicate": body.predicate, "object": body.object,
                       "ended": format_date(ended)})
                .to_string(),
            ),
        })?;
    }

    Ok(Json(json!({
        "success": invalidated > 0,
        "invalidated": invalidated,
        "fact": format!("{} → {} → {}", body.subject, body.predicate, body.object),
        "ended": body.ended.unwrap_or_else(|| "today".to_owned()),
    })))
}

// ─── KG: timeline ─────────────────────────────────────────────────────────────

async fn route_kg_timeline<P>(
    State(state): State<Arc<ServerState<P>>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let entity = params.get("entity").cloned();
    let runtime = KnowledgeGraphRuntime::new(state.storage.operational_store());
    let timeline = runtime.timeline(entity.as_deref())?;
    let count = timeline.len();
    Ok(Json(json!({
        "entity": entity.clone().unwrap_or_else(|| "all".to_owned()),
        "timeline": timeline,
        "count": count,
    })))
}

// ─── KG: stats ───────────────────────────────────────────────────────────────

async fn route_kg_stats<P>(
    State(state): State<Arc<ServerState<P>>>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let runtime = KnowledgeGraphRuntime::new(state.storage.operational_store());
    let stats = serde_json::to_value(runtime.stats()?)?;
    Ok(Json(stats))
}

// ─── Taxonomy ────────────────────────────────────────────────────────────────

async fn route_taxonomy<P>(
    State(state): State<Arc<ServerState<P>>>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let drawers =
        state.storage.drawer_store().list_drawers(&DrawerFilter::default()).await?;
    let mut taxonomy = std::collections::BTreeMap::<String, std::collections::BTreeMap<String, usize>>::new();
    for drawer in &drawers {
        if is_diary_wing_or_room(drawer.wing.as_str(), drawer.room.as_str()) {
            continue;
        }
        *taxonomy
            .entry(drawer.wing.as_str().to_owned())
            .or_default()
            .entry(drawer.room.as_str().to_owned())
            .or_default() += 1;
    }
    Ok(Json(json!({"taxonomy": taxonomy})))
}

// ─── Wings ───────────────────────────────────────────────────────────────────

async fn route_wings<P>(
    State(state): State<Arc<ServerState<P>>>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let drawers =
        state.storage.drawer_store().list_drawers(&DrawerFilter::default()).await?;
    let mut wings = std::collections::BTreeMap::<String, usize>::new();
    for drawer in &drawers {
        if is_diary_wing_or_room(drawer.wing.as_str(), drawer.room.as_str()) {
            continue;
        }
        *wings.entry(drawer.wing.as_str().to_owned()).or_default() += 1;
    }
    Ok(Json(json!({"wings": wings})))
}

// ─── Rooms ───────────────────────────────────────────────────────────────────

/// Query parameters for `GET /v1/rooms`.
#[derive(Debug, serde::Deserialize)]
pub struct RoomsQuery {
    /// Optional wing filter.
    pub wing: Option<String>,
}

async fn route_rooms<P>(
    State(state): State<Arc<ServerState<P>>>,
    Query(params): Query<RoomsQuery>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let wing = params.wing.as_deref().map(WingId::new).transpose()?;
    let drawers = state
        .storage
        .drawer_store()
        .list_drawers(&DrawerFilter { wing: wing.clone(), ..DrawerFilter::default() })
        .await?;
    let mut rooms = std::collections::BTreeMap::<String, usize>::new();
    for drawer in &drawers {
        if is_diary_wing_or_room(drawer.wing.as_str(), drawer.room.as_str()) {
            continue;
        }
        *rooms.entry(drawer.room.as_str().to_owned()).or_default() += 1;
    }
    Ok(Json(json!({
        "wing": params.wing.as_deref().unwrap_or("all"),
        "rooms": rooms,
    })))
}

// ─── Changes ─────────────────────────────────────────────────────────────────

async fn route_changes<P>(
    State(state): State<Arc<ServerState<P>>>,
    Query(params): Query<ChangesQuery>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let limit = params.limit.unwrap_or(DEFAULT_PAGE_LIMIT).max(1).min(MAX_PAGE_LIMIT);

    let since = params
        .since
        .as_deref()
        .map(|s| {
            OffsetDateTime::parse(s, &Rfc3339).map_err(|_| {
                ServerError::InvalidParams(format!(
                    "invalid `since` timestamp `{s}`; expected RFC 3339 e.g. 2026-01-01T00:00:00Z"
                ))
            })
        })
        .transpose()?;

    let cursor = params.cursor.as_deref().map(decode_cursor).transpose()?;

    let page = state
        .storage
        .operational_store()
        .get_changes_page(since, cursor, limit)?;

    let next_cursor = page.next_cursor.map(|c| encode_cursor(&c));

    let events: Vec<ChangeEventDto> = page
        .events
        .into_iter()
        .filter(|ev| !is_diary_change_event(ev))
        .map(change_event_to_dto)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Json(ChangesResponse { events, next_cursor }))
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Returns `true` when the wing or room identifies diary content that must not
/// be exposed via the federation API.
fn is_diary_wing_or_room(wing: &str, room: &str) -> bool {
    wing == SHARED_AGENT_DIARY_WING || room == DIARY_ROOM
}

/// Returns `true` when the change event is diary-related and should be
/// filtered from the changes feed.
fn is_diary_change_event(event: &ChangeEvent) -> bool {
    if event.event_type == "diary_written" {
        return true;
    }
    // Check details_json for diary wing/room mentions
    if let Some(details) = &event.details_json {
        if let Ok(value) = serde_json::from_str::<Value>(details) {
            let wing = value.get("wing").and_then(Value::as_str).unwrap_or("");
            let room = value.get("room").and_then(Value::as_str).unwrap_or("");
            if is_diary_wing_or_room(wing, room) {
                return true;
            }
        }
    }
    false
}

/// Converts a storage `ChangeEvent` to the federation wire DTO.
fn change_event_to_dto(event: ChangeEvent) -> Result<ChangeEventDto, ServerError> {
    let details = event
        .details_json
        .as_deref()
        .map(serde_json::from_str::<Value>)
        .transpose()?;
    Ok(ChangeEventDto {
        event_type: event.event_type,
        occurred_at: format_rfc3339(event.occurred_at)?,
        entity_id: event.entity_id,
        actor: event.actor,
        details,
    })
}

/// Encodes a `ChangeCursor` as `"{rfc3339}|{rowid}"`.
fn encode_cursor(cursor: &ChangeCursor) -> String {
    let ts = cursor
        .occurred_at
        .format(&Rfc3339)
        .unwrap_or_else(|_| cursor.occurred_at.unix_timestamp().to_string());
    format!("{ts}|{}", cursor.rowid)
}

/// Decodes a cursor encoded by [`encode_cursor`].
fn decode_cursor(s: &str) -> Result<ChangeCursor, ServerError> {
    // Split on the LAST '|' so RFC3339 strings (which never contain '|') are safe
    let pos = s.rfind('|').ok_or_else(|| {
        ServerError::InvalidParams(format!("invalid cursor `{s}`; expected `timestamp|rowid`"))
    })?;
    let ts_part = &s[..pos];
    let rowid_part = &s[pos + 1..];
    let occurred_at = OffsetDateTime::parse(ts_part, &Rfc3339).map_err(|_| {
        ServerError::InvalidParams(format!(
            "invalid cursor timestamp `{ts_part}`; expected RFC 3339"
        ))
    })?;
    let rowid = rowid_part.parse::<i64>().map_err(|_| {
        ServerError::InvalidParams(format!("invalid cursor rowid `{rowid_part}`"))
    })?;
    Ok(ChangeCursor { occurred_at, rowid })
}

/// Serialises `DrawerRecord` to JSON, stripping the embedding vector.
fn drawer_record_to_json_no_embedding(drawer: &DrawerRecord) -> Value {
    drawer_record_to_json_with_stale(drawer, false)
}

fn drawer_record_to_json_with_stale(drawer: &DrawerRecord, stale: bool) -> Value {
    let mut v = json!({
        "id": drawer.id.as_str(),
        "wing": drawer.wing.as_str(),
        "room": drawer.room.as_str(),
        "hall": drawer.hall,
        "date": drawer.date.map(format_date),
        "source_file": drawer.source_file,
        "chunk_index": drawer.chunk_index,
        "ingest_mode": drawer.ingest_mode,
        "added_by": drawer.added_by,
        "filed_at": format_rfc3339(drawer.filed_at).ok(),
        "content": drawer.content,
        "content_hash": drawer.content_hash,
    });
    if stale {
        v["stale"] = json!(true);
    }
    v
}

/// Performs a semantic duplicate search and returns a JSON array of matches.
async fn find_duplicates<P>(
    state: &ServerState<P>,
    content: &str,
    threshold: f32,
) -> Result<Vec<Value>, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let query = SearchQuery {
        text: content.to_owned(),
        wing: None,
        room: None,
        limit: DUPLICATE_SEARCH_LIMIT,
        profile: state.config.embedding_profile,
    };
    let results = {
        let mut search = state.search.lock().await;
        search.search_semantic(state.storage.drawer_store(), &query).await?
    };
    Ok(results
        .into_iter()
        .filter(|r| r.score >= threshold)
        .map(|r| {
            let snippet = if r.content.chars().count() > 200 {
                format!("{}...", r.content.chars().take(200).collect::<String>())
            } else {
                r.content
            };
            json!({
                "id": r.drawer_id,
                "wing": r.wing,
                "room": r.room,
                "similarity": r.score,
                "content": snippet,
            })
        })
        .collect())
}

/// Builds a `DrawerRecord` by computing an embedding for `content`.
async fn build_drawer_record<P>(
    state: &ServerState<P>,
    id: DrawerId,
    wing: WingId,
    room: RoomId,
    source_file: String,
    added_by: String,
    ingest_mode: String,
    content: String,
    filed_at: OffsetDateTime,
) -> Result<DrawerRecord, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let request = EmbeddingRequest::new(vec![content.clone()])?;
    let response = {
        let mut search = state.search.lock().await;
        search.provider_mut().embed(&request)?
    };
    let embedding =
        response.vectors().first().cloned().ok_or_else(|| {
            ServerError::Embeddings(mempalace_embeddings::EmbeddingError::ProviderContract(
                "provider returned no vector for single-drawer ingest".to_owned(),
            ))
        })?;
    Ok(DrawerRecord {
        id,
        wing,
        room,
        hall: None,
        date: None,
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

/// Generates a deterministic drawer identifier from its components.
fn generated_drawer_id(
    prefix: &str,
    wing: &str,
    room: &str,
    content: &str,
    now: OffsetDateTime,
) -> Result<DrawerId, ServerError> {
    let mut hasher = Hasher::new();
    hasher.update(content.as_bytes());
    hasher.update(now.unix_timestamp_nanos().to_string().as_bytes());
    let suffix = hasher.finalize().to_hex().chars().take(16).collect::<String>();
    DrawerId::new(format!("{prefix}_{wing}_{room}_{suffix}")).map_err(ServerError::Id)
}

/// Computes the BLAKE3 hex hash of a text string.
fn hash_text(content: &str) -> String {
    mempalace_core::hash_text(content)
}

/// Formats an `OffsetDateTime` as RFC 3339.
fn format_rfc3339(dt: OffsetDateTime) -> Result<String, ServerError> {
    dt.format(&Rfc3339).map_err(|err| ServerError::InvalidParams(err.to_string()))
}

/// Formats a `Date` as `YYYY-MM-DD`.
fn format_date(date: Date) -> String {
    date.format(&time::macros::format_description!("[year]-[month]-[day]"))
        .unwrap_or_else(|_| date.to_string())
}

/// Parses a `YYYY-MM-DD` date string.
fn parse_date(s: &str) -> Result<Date, ServerError> {
    // Try RFC3339 first (take date part)
    if let Ok(dt) = OffsetDateTime::parse(s, &Rfc3339) {
        return Ok(dt.date());
    }
    Date::parse(s, &time::macros::format_description!("[year]-[month]-[day]"))
        .map_err(|_| ServerError::InvalidParams(format!("invalid date `{s}`; expected YYYY-MM-DD")))
}

/// Parses the KG traversal direction string.
fn parse_direction(s: &str) -> Result<QueryDirection, ServerError> {
    match s {
        "outgoing" => Ok(QueryDirection::Outgoing),
        "incoming" => Ok(QueryDirection::Incoming),
        "both" => Ok(QueryDirection::Both),
        other => Err(ServerError::InvalidParams(format!(
            "invalid direction `{other}`; expected outgoing, incoming, or both"
        ))),
    }
}

/// Infers the KG entity kind heuristically from a name string.
///
/// Mirrors the logic in `mempalace-mcp`.
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
            let Some(first) = chars.next() else { return false };
            first.is_ascii_uppercase()
                && chars.all(|ch| ch.is_ascii_lowercase() || ch == '\'')
        })
    {
        return EntityKind::Person;
    }
    if tokens.len() == 1 && trimmed.chars().all(|ch| ch.is_ascii_uppercase()) {
        return EntityKind::Concept;
    }
    EntityKind::Unknown
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode, header};
    use http_body_util::BodyExt;
    use mempalace_config::{FederationRuntimeConfig, LowCpuRuntimeConfig, ServerRuntimeConfig};
    use mempalace_core::EmbeddingProfile;
    use mempalace_embeddings::DeterministicStubProvider;
    use serde_json::Value;
    use tempfile::TempDir;
    use tower::ServiceExt;

    // ─── Test harness ─────────────────────────────────────────────────────────

    const ALICE_TOKEN: &str = "alice-secret-token";
    const BOB_TOKEN: &str = "bob-secret-token";
    const BAD_TOKEN: &str = "bad-token-xyz";

    struct Harness {
        router: Router,
        _tempdir: TempDir,
    }

    async fn make_harness() -> Harness {
        let tempdir = TempDir::new().unwrap();
        let palace_path = tempdir.path().join("palace");

        // Write token file
        let token_file = tempdir.path().join("tokens.json");
        std::fs::write(
            &token_file,
            serde_json::to_string(&serde_json::json!([
                {"token": ALICE_TOKEN, "name": "alice", "enabled": true},
                {"token": BOB_TOKEN, "name": "bob", "enabled": false},
            ]))
            .unwrap(),
        )
        .unwrap();

        let config = MempalaceConfig {
            schema_version: 1,
            collection_name: "mempalace_drawers".to_owned(),
            palace_path,
            embedding_profile: EmbeddingProfile::Balanced,
            low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
            server: ServerRuntimeConfig {
                bind: "127.0.0.1:8765".parse().unwrap(),
                token_file: tempdir.path().join("tokens.json"),
            },
            federation: FederationRuntimeConfig::default(),
        };
        let tokens = TokenRegistry::load(token_file).unwrap();
        let provider = DeterministicStubProvider::new(EmbeddingProfile::Balanced);
        let router = build_router(config, provider, tokens).await.unwrap();
        Harness { router, _tempdir: tempdir }
    }

    /// Helper: build a JSON request with bearer auth.
    fn authed_json_request(method: Method, uri: &str, token: &str, body: Value) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    /// Helper: build a GET request with bearer auth.
    fn authed_get(uri: &str, token: &str) -> Request<Body> {
        Request::builder()
            .method(Method::GET)
            .uri(uri)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    }

    /// Collect response body as JSON.
    async fn body_json(response: axum::response::Response) -> Value {
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    // ─── 1. Health ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn health_no_token_returns_200() {
        let harness = make_harness().await;
        let request =
            Request::builder().method(Method::GET).uri("/v1/health").body(Body::empty()).unwrap();
        let response = harness.router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["status"], "ok");
    }

    // ─── 2. Info auth ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn info_without_token_returns_401() {
        let harness = make_harness().await;
        let request =
            Request::builder().method(Method::GET).uri("/v1/info").body(Body::empty()).unwrap();
        let response = harness.router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn info_with_bad_token_returns_401() {
        let harness = make_harness().await;
        let response = harness.router.oneshot(authed_get("/v1/info", BAD_TOKEN)).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn info_with_disabled_bob_token_returns_401() {
        let harness = make_harness().await;
        let response = harness.router.oneshot(authed_get("/v1/info", BOB_TOKEN)).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn info_with_alice_token_returns_federation_api_version_1() {
        let harness = make_harness().await;
        let response = harness.router.oneshot(authed_get("/v1/info", ALICE_TOKEN)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["federation_api_version"], 1u32);
    }

    // ─── 3. Add + search + get ────────────────────────────────────────────────

    #[tokio::test]
    async fn add_drawer_and_search_finds_it() {
        let harness = make_harness().await;

        // Add a drawer
        let add_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers",
                ALICE_TOKEN,
                json!({
                    "wing": "wing_code",
                    "room": "auth-migration",
                    "content": "auth migration parity test content",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(add_resp.status(), StatusCode::OK);
        let add_body = body_json(add_resp).await;
        assert_eq!(add_body["success"], true);
        let drawer_id = add_body["drawer_id"].as_str().unwrap().to_owned();

        // Search should find it at rank 1
        let search_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers/search",
                ALICE_TOKEN,
                json!({"query": "auth migration parity", "limit": 5}),
            ))
            .await
            .unwrap();
        assert_eq!(search_resp.status(), StatusCode::OK);
        let search_body = body_json(search_resp).await;
        let results = search_body["results"].as_array().unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0]["rank"], 1);
        assert!(results[0]["content"].as_str().unwrap().contains("auth migration"));

        // GET by id should return the drawer with added_by = "alice"
        let get_resp = harness
            .router
            .clone()
            .oneshot(authed_get(&format!("/v1/drawers/{drawer_id}"), ALICE_TOKEN))
            .await
            .unwrap();
        assert_eq!(get_resp.status(), StatusCode::OK);
        let get_body = body_json(get_resp).await;
        assert_eq!(get_body["added_by"], "alice");
    }

    // ─── 4. Duplicate detection ───────────────────────────────────────────────

    #[tokio::test]
    async fn add_duplicate_returns_409() {
        let harness = make_harness().await;

        let payload = json!({
            "wing": "wing_code",
            "room": "auth-test",
            "content": "auth migration parity unique content here",
        });

        // First add succeeds
        let first = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers",
                ALICE_TOKEN,
                payload.clone(),
            ))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);

        // Second add with same content returns 409
        let second = harness
            .router
            .clone()
            .oneshot(authed_json_request(Method::POST, "/v1/drawers", ALICE_TOKEN, payload))
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::CONFLICT);
        let body = body_json(second).await;
        assert_eq!(body["code"], "duplicate");
    }

    // ─── 5. Diary rejection ───────────────────────────────────────────────────

    #[tokio::test]
    async fn diary_room_rejected_with_422() {
        let harness = make_harness().await;
        let response = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers",
                ALICE_TOKEN,
                json!({"wing": "wing_code", "room": "diary", "content": "diary content"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = body_json(response).await;
        assert_eq!(body["code"], "diary_not_federated");
    }

    #[tokio::test]
    async fn diary_wing_agents_rejected_with_422() {
        let harness = make_harness().await;
        let response = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers",
                ALICE_TOKEN,
                json!({"wing": "wing_agents", "room": "general", "content": "diary wing content"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn diary_source_file_prefix_rejected_with_422() {
        let harness = make_harness().await;
        let response = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers",
                ALICE_TOKEN,
                json!({
                    "wing": "wing_code",
                    "room": "general",
                    "content": "some content",
                    "source_file": "diary:my-topic",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    // ─── 6. KG round-trip ────────────────────────────────────────────────────

    #[tokio::test]
    async fn kg_add_query_invalidate_round_trip() {
        let harness = make_harness().await;

        // Add a fact
        let add_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/kg/facts",
                ALICE_TOKEN,
                json!({
                    "subject": "Alice",
                    "predicate": "works_on",
                    "object": "MemPalace",
                    "valid_from": "2026-01-01",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(add_resp.status(), StatusCode::OK);
        let add_body = body_json(add_resp).await;
        assert_eq!(add_body["success"], true);

        // Query the entity — should find the fact
        let query_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/kg/query",
                ALICE_TOKEN,
                json!({"entity": "Alice", "direction": "outgoing"}),
            ))
            .await
            .unwrap();
        assert_eq!(query_resp.status(), StatusCode::OK);
        let query_body = body_json(query_resp).await;
        assert!(query_body["count"].as_u64().unwrap() > 0);

        // Invalidate the fact
        let inv_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/kg/facts/invalidate",
                ALICE_TOKEN,
                json!({
                    "subject": "Alice",
                    "predicate": "works_on",
                    "object": "MemPalace",
                    "ended": "2026-06-01",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(inv_resp.status(), StatusCode::OK);
        let inv_body = body_json(inv_resp).await;
        assert!(inv_body["invalidated"].as_u64().unwrap() > 0);

        // Query with as_of AFTER invalidation date — fact should be gone
        let after_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/kg/query",
                ALICE_TOKEN,
                json!({"entity": "Alice", "as_of": "2026-07-01", "direction": "outgoing"}),
            ))
            .await
            .unwrap();
        assert_eq!(after_resp.status(), StatusCode::OK);
        let after_body = body_json(after_resp).await;
        assert_eq!(after_body["count"], 0u64);
    }

    // ─── 7. Changes pagination ────────────────────────────────────────────────

    #[tokio::test]
    async fn changes_pagination_works_and_filters_diary() {
        let harness = make_harness().await;

        // Add 3 drawers with semantically distinct content (different embedding clusters)
        // so the stub provider returns different vectors and no duplicates are detected.
        let contents = [
            "auth migration parity is important for correctness",
            "session diary ops need attention every morning",
            "rust cli tooling makes development faster",
        ];
        for (i, content) in contents.iter().enumerate() {
            let resp = harness
                .router
                .clone()
                .oneshot(authed_json_request(
                    Method::POST,
                    "/v1/drawers",
                    ALICE_TOKEN,
                    json!({
                        "wing": "wing_code",
                        "room": "pagination-test",
                        "content": content,
                    }),
                ))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK, "add drawer {i} failed");
        }

        // First page: limit=2
        let page1_resp = harness
            .router
            .clone()
            .oneshot(authed_get("/v1/changes?limit=2", ALICE_TOKEN))
            .await
            .unwrap();
        assert_eq!(page1_resp.status(), StatusCode::OK);
        let page1 = body_json(page1_resp).await;
        let page1_events = page1["events"].as_array().unwrap();
        assert_eq!(page1_events.len(), 2, "expected 2 events on page 1");
        let cursor = page1["next_cursor"].as_str().expect("expected next_cursor on page 1");

        // Second page: follow cursor
        let page2_resp = harness
            .router
            .clone()
            .oneshot(authed_get(
                &format!("/v1/changes?limit=2&cursor={}", urlencoded(cursor)),
                ALICE_TOKEN,
            ))
            .await
            .unwrap();
        assert_eq!(page2_resp.status(), StatusCode::OK);
        let page2 = body_json(page2_resp).await;
        let page2_events = page2["events"].as_array().unwrap();
        assert_eq!(page2_events.len(), 1, "expected 1 event on page 2");
        assert!(page2["next_cursor"].is_null(), "expected no cursor on last page");

        // Diary events must not appear
        let all_events: Vec<Value> =
            page1_events.iter().chain(page2_events.iter()).cloned().collect();
        for event in &all_events {
            assert_ne!(
                event["event_type"].as_str().unwrap(),
                "diary_written",
                "diary event leaked into feed"
            );
        }
    }

    // ─── 8. Delete ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn delete_drawer_success_then_404() {
        let harness = make_harness().await;

        // Add
        let add_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers",
                ALICE_TOKEN,
                json!({
                    "wing": "wing_code",
                    "room": "delete-test",
                    "content": "drawer to be deleted",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(add_resp.status(), StatusCode::OK);
        let body = body_json(add_resp).await;
        let drawer_id = body["drawer_id"].as_str().unwrap().to_owned();

        // Delete
        let del_req = Request::builder()
            .method(Method::DELETE)
            .uri(format!("/v1/drawers/{drawer_id}"))
            .header(header::AUTHORIZATION, format!("Bearer {ALICE_TOKEN}"))
            .body(Body::empty())
            .unwrap();
        let del_resp = harness.router.clone().oneshot(del_req).await.unwrap();
        assert_eq!(del_resp.status(), StatusCode::OK);
        let del_body = body_json(del_resp).await;
        assert_eq!(del_body["success"], true);

        // Delete again → 404
        let del2_req = Request::builder()
            .method(Method::DELETE)
            .uri(format!("/v1/drawers/{drawer_id}"))
            .header(header::AUTHORIZATION, format!("Bearer {ALICE_TOKEN}"))
            .body(Body::empty())
            .unwrap();
        let del2_resp = harness.router.clone().oneshot(del2_req).await.unwrap();
        assert_eq!(del2_resp.status(), StatusCode::NOT_FOUND);
    }

    /// URL-encodes the cursor string so it can be embedded in a query string.
    fn urlencoded(s: &str) -> String {
        s.chars()
            .flat_map(|ch| {
                if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                    vec![ch]
                } else {
                    let encoded = format!("%{:02X}", ch as u32);
                    encoded.chars().collect::<Vec<_>>()
                }
            })
            .collect()
    }
}

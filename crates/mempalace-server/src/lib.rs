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
//! let (router, _state) = build_router(config, provider, tokens).await?;
//! // Bind and serve with axum::serve(listener, router).await?
//! # Ok(())
//! # }
//! ```

use std::path::{Path as FsPath, PathBuf};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, SystemTime};

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{Request, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use blake3::Hasher;
use mempalace_config::MempalaceConfig;
use mempalace_core::{
    BUILD_VERSION, DIARY_ROOM, DIARY_TOPIC_PREFIX, DrawerId, DrawerRecord, RoomId,
    SHARED_AGENT_DIARY_WING, SearchQuery, SourceLocator, WING_PREFIX, WingId, hash_bytes,
    mined_drawer_id, resolve_records,
};
use mempalace_embeddings::{EmbeddingProvider, EmbeddingRequest};
use mempalace_federation::{
    AckMessageRequest, AddDrawerRequest, AddDrawerResponse, ChangeEventDto, ChangesQuery,
    ChangesResponse, CheckDuplicateRequest, CheckDuplicateResponse, CoordinationArtifactDto,
    CoordinationEventDto, CoordinationEventsQuery, CoordinationEventsResponse,
    CoordinationMessageDto, CoordinationTaskDto, CoordinationTaskResultDto, CoordinationTaskState,
    DrawerSearchRequest, DrawerSearchResponse, ErrorBody, FEDERATION_API_VERSION, InboxPageResponse,
    InboxQuery, InfoResponse, IngestBatchRequest, IngestBatchResponse, IngestFileResult,
    KgAddFactRequest, KgInvalidateRequest, KgQueryRequest, ListDrawersQuery, ListDrawersResponse,
    MaintenanceAbortReason as FedMaintenanceAbortReason,
    MaintenanceRunStatus as FedMaintenanceRunStatus,
    MaintenanceSkipReason as FedMaintenanceSkipReason, MaintenanceStatus, NewArtifactRequest,
    NewMessageRequest, NewTaskRequest, NewTaskResultRequest, RemoteDrawerResult, TaskLeaseRequest,
    TransitionTaskRequest,
};
use mempalace_graph::{AddFactRequest, EntityKind, KnowledgeGraphRuntime, QueryDirection};
use mempalace_search::{SearchRuntime, SearchRuntimePolicy};
use mempalace_storage::{
    Artifact as CoordinationArtifact, ChangeCursor, ChangeEvent, ChangeLogStore,
    CoordinationCursor, CoordinationEvent, CoordinationStore, CoordinationVisibility, DrawerFilter,
    DrawerStore,
    DuplicateStrategy, IngestCommitRequest, IngestManifestStore, MaintenanceAbortReason,
    MaintenanceOutcome, MaintenanceRunSummary, MaintenanceSettings, MaintenanceSkipReason,
    Message as CoordinationMessage, NewArtifact, NewMessage, NewTask, NewTaskResult,
    RevisionedWrite, StorageEngine, Task as CoordinationTask,
    TaskResult as CoordinationTaskResult, TaskState,
};
use serde_json::{Value, json};
use subtle::ConstantTimeEq;
use thiserror::Error;
use time::{Date, OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::Mutex;
use tracing::{info, warn};

// ─── Constants ───────────────────────────────────────────────────────────────

const DEFAULT_DUPLICATE_THRESHOLD: f32 = 0.9;
const DUPLICATE_SEARCH_LIMIT: usize = 5;
/// Default page size for list/changes endpoints.
const DEFAULT_PAGE_LIMIT: usize = 50;
/// Maximum page size clients may request.
const MAX_PAGE_LIMIT: usize = 200;
/// Maximum bytes accepted for single-drawer writes and duplicate checks.
const MAX_DRAWER_CONTENT_BYTES: usize = 256 * 1024;
/// Maximum bytes accepted for a semantic search query.
const MAX_SEARCH_QUERY_BYTES: usize = 16 * 1024;
/// Maximum number of files in one remote ingest request.
const MAX_INGEST_FILES: usize = 128;
/// Maximum chunks accepted for any single file in one remote ingest request.
const MAX_INGEST_CHUNKS_PER_FILE: usize = 512;
/// Maximum total chunk text bytes accepted in one remote ingest request.
const MAX_INGEST_TEXT_BYTES: usize = 8 * 1024 * 1024;
/// Maximum bytes accepted in free-form KG string fields.
const MAX_KG_FIELD_BYTES: usize = 4096;
/// Default timeline rows returned when no limit is requested.
const DEFAULT_KG_TIMELINE_LIMIT: usize = 100;
/// Maximum timeline rows clients may request.
const MAX_KG_TIMELINE_LIMIT: usize = 200;
/// Maximum `lease_seconds` accepted by a coordination claim/renew request — a generous 100
/// years. `mempalace-storage`'s `claim_task`/`renew_lease` reject a TTL that would overflow
/// `OffsetDateTime` arithmetic (see `LEASE_DURATION_OUT_OF_RANGE`) regardless of this bound, but
/// this route-level check turns an obviously-nonsensical value into a clean 400 before a request
/// ever reaches storage, rather than relying on that lower-level guard alone.
const MAX_LEASE_SECONDS: i64 = 100 * 365 * 24 * 60 * 60;

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
    /// Bearer token is valid but its scopes do not permit the requested
    /// operation or wing. Distinct from `Unauthorized`: this means "I know who
    /// you are, and you may not do this," not "I don't know who you are."
    #[error("forbidden")]
    Forbidden,
    /// The requested resource was not found.
    #[error("not found: {0}")]
    NotFound(String),
    /// Attempt to write a diary drawer via the federation API.
    #[error("diary drawers are not federated")]
    DiaryNotFederated,
    /// Attempted to add a drawer that is a near-duplicate of an existing one.
    #[error("duplicate detected")]
    Duplicate(Value),
    /// A coordination write was rejected because of the record's current
    /// revision or state — a concurrent writer moved it, another worker holds
    /// a live lease, the task is terminal, or the caller does not own the
    /// task/lease/message it is trying to act on. `code` distinguishes the
    /// two shapes on the wire: `"revision_conflict"` carries both revisions
    /// (reload and retry with the current one) and is built directly from the
    /// typed `RevisionedWrite::Conflict` `claim_task`/`renew_lease`/
    /// `transition_task` return (see `coordination_revision_conflict`);
    /// `"coordination_conflict"` carries neither and means the write is not
    /// permitted regardless of revision (e.g. another worker's lease has not
    /// expired) — that shape is still classified from the `pub const` message
    /// fragments `coordination.rs` exports for exactly this purpose (see
    /// `coordination_storage_error`), because those rejections have no
    /// revision pair to carry and stay text-classified.
    #[error("{message}")]
    CoordinationConflict {
        /// `"revision_conflict"` or `"coordination_conflict"`.
        code: &'static str,
        /// Human-readable detail, taken from the underlying storage error.
        message: String,
        /// The revision the caller expected, present only for `code ==
        /// "revision_conflict"`.
        expected_revision: Option<i64>,
        /// The record's actual current revision, present only for `code ==
        /// "revision_conflict"`.
        actual_revision: Option<i64>,
    },
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
        if matches!(
            &self,
            Self::Storage(_)
                | Self::Search(_)
                | Self::Graph(_)
                | Self::Core(_)
                | Self::Embeddings(_)
                | Self::Json(_)
                | Self::Io { .. }
                | Self::TokenFile(_)
        ) {
            warn!(error = %self, "federation request failed with internal error");
        }
        let (status, code, message) = match &self {
            Self::InvalidParams(msg) => {
                (StatusCode::BAD_REQUEST, "invalid_params", msg.as_str().to_owned())
            }
            Self::Id(err) => (StatusCode::BAD_REQUEST, "invalid_params", err.to_string()),
            Self::Unauthorized => {
                (StatusCode::UNAUTHORIZED, "unauthorized", "missing or invalid token".to_owned())
            }
            Self::Forbidden => (
                StatusCode::FORBIDDEN,
                "forbidden",
                "token does not have the required scope for this operation".to_owned(),
            ),
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
            Self::CoordinationConflict { code, message, .. } => {
                (StatusCode::CONFLICT, *code, message.clone())
            }
            Self::Storage(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "storage_error",
                "storage operation failed".to_owned(),
            ),
            Self::Search(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "search_error",
                "search operation failed".to_owned(),
            ),
            Self::Graph(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "graph_error",
                "knowledge graph operation failed".to_owned(),
            ),
            Self::Core(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "core_error",
                "core operation failed".to_owned(),
            ),
            Self::Embeddings(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "embedding_error",
                "embedding operation failed".to_owned(),
            ),
            Self::Json(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "json_error",
                "serialization operation failed".to_owned(),
            ),
            Self::Io { .. } => {
                (StatusCode::INTERNAL_SERVER_ERROR, "io_error", "I/O operation failed".to_owned())
            }
            Self::TokenFile(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "token_file_error",
                "token registry unavailable".to_owned(),
            ),
        };
        let body = if let Self::Duplicate(matches) = &self {
            // 409 body includes the matches list so clients can inspect duplicates
            json!({
                "code": code,
                "message": message,
                "matches": matches,
            })
        } else if let Self::CoordinationConflict { expected_revision, actual_revision, .. } = &self
        {
            // 409 body includes the revisions (when known) so a client can decide whether to
            // reload and retry, instead of parsing `message`.
            json!({
                "code": code,
                "message": message,
                "expected_revision": expected_revision,
                "actual_revision": actual_revision,
            })
        } else {
            json!(ErrorBody { code: code.to_owned(), message })
        };
        (status, Json(body)).into_response()
    }
}

// ─── Token auth ──────────────────────────────────────────────────────────────

/// A single scoped-access operation a bearer token may be granted.
///
/// Closed set: deserialization fails (and so does the whole token-file load —
/// see [`TokenRegistry::read_file`]) on any string that is not one of these
/// variants. That is deliberate — the registry already fails closed on
/// malformed reloads, and a silently-ignored typo in a token file (e.g.
/// `"reed"` instead of `"read"`) would otherwise grant less access than the
/// operator intended without any signal.
///
/// `CoordinationRead`, `CoordinationWrite` and `CoordinationClaim` have no
/// routes yet — they exist so the token file format is stable ahead of the
/// coordination REST routes (issue #102 Stage 3), which will require them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum Operation {
    Read,
    Write,
    Delete,
    Ingest,
    CoordinationRead,
    CoordinationWrite,
    CoordinationClaim,
}

/// Raw (pre-normalisation) form of a token-file scope entry, as deserialized
/// from JSON. See [`TokenScopeEntry`] for the normalised form used at
/// authorization time.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTokenScope {
    wings: Vec<String>,
    operations: Vec<Operation>,
}

/// A single scoped-access grant: a set of operations permitted on a set of
/// wings. One token may carry several of these; a request is authorized if
/// any one of them covers it.
#[derive(Debug, Clone)]
struct TokenScopeEntry {
    /// Wings this grant covers, expanded at load time by
    /// `normalize_scope_wing` into every spelling that should authorize a
    /// request. Exactly one dimension is aliased: the `WING_PREFIX` prefix.
    /// REST handlers (e.g. `route_drawers_add`) build a wing with
    /// `WingId::new` (validates, does not transform) while MCP paths use
    /// `WingId::normalized` (which, among other things, adds the prefix if
    /// absent) — so `myproject` and `wing_myproject` genuinely name the same
    /// wing by convention, and a raw entry missing the prefix expands to
    /// both spellings, prefix added verbatim (case preserved).
    ///
    /// Case is a different, **not** aliased, dimension: `WingId::new`'s
    /// `validate_id` accepts uppercase ASCII, so `wing_MyProject` and
    /// `wing_myproject` can be two distinct, independently-stored wings in
    /// the same palace, not two spellings of one. Folding case in an
    /// authorization grant would silently widen it to a wing the operator
    /// never named — a privilege escalation, not a convenience — so a raw
    /// entry that is already a valid `WingId` (prefixed or not) keeps its
    /// case exactly as written, in every alias it expands to. See
    /// `normalize_scope_wing` for the full precedence. The literal `"*"` is
    /// kept as-is and matches every wing.
    wings: Vec<String>,
    /// Operations this grant permits.
    operations: Vec<Operation>,
}

/// Expands one `wings` entry from a token file into the set of spellings
/// that should authorize a request. `"*"` expands to itself only.
///
/// Two dimensions, handled differently — aliasing one and never the other is
/// the whole point of this function:
///
/// - **Prefix is aliased.** `myproject` and `wing_myproject` name the same
///   wing by convention elsewhere in this codebase (`WingId::normalized`
///   adds `WING_PREFIX` when absent). So when the raw entry is a valid
///   [`WingId`] (passes [`WingId::new`], which validates but does not
///   transform) and lacks the prefix, it expands to two aliases: the entry
///   verbatim, and the entry with `WING_PREFIX` prepended — case untouched
///   in both. An entry that already has the prefix needs no second alias.
/// - **Case is never folded.** `validate_id` accepts uppercase ASCII, so
///   `wing_MyProject` and `wing_myproject` can be two distinct wings holding
///   different data, not two spellings of one — lowercasing here would let
///   a scope silently authorize a wing the operator never named. So the
///   verbatim alias (and its prefixed sibling, if produced) always keep the
///   exact case of the raw entry; there is no case-insensitive alias.
///
/// Only when the raw entry is **not** a valid `WingId` at all — malformed
/// input, e.g. embedded whitespace — does this fall back to
/// [`WingId::normalized`], which sanitizes and lowercases, as a single
/// alias. That lowercasing is acceptable there because sanitization is the
/// whole point of that path and there is no verbatim form to preserve.
fn normalize_scope_wing(raw: &str) -> Result<Vec<String>, ServerError> {
    let trimmed = raw.trim();
    if trimmed == "*" {
        return Ok(vec!["*".to_owned()]);
    }
    if let Ok(wing) = WingId::new(trimmed) {
        let verbatim = wing.as_str().to_owned();
        if verbatim.starts_with(WING_PREFIX) {
            return Ok(vec![verbatim]);
        }
        // Prefix dimension only: prepend `WING_PREFIX` verbatim, case
        // untouched. This is string concatenation, not `WingId::normalized`
        // — it must not sanitize or lowercase, or it would silently fold
        // the case dimension this function is required to leave alone.
        let prefixed = format!("{WING_PREFIX}{verbatim}");
        return Ok(vec![verbatim, prefixed]);
    }
    WingId::normalized(trimmed)
        .map(|wing| vec![wing.as_str().to_owned()])
        .map_err(|err| ServerError::TokenFile(format!("invalid wing `{raw}` in token scope: {err}")))
}

/// Wings visible to a token for a given operation. Returned by
/// [`AuthIdentity::visible_wings`] and used by the aggregate routes (Group C:
/// taxonomy, wings, rooms, changes) to *filter* their response to what the
/// caller can see, rather than rejecting the request outright.
enum WingVisibility {
    /// Every wing is visible (unrestricted token, or a scope entry granting
    /// the operation on `"*"`).
    All,
    /// Only these wings are visible.
    Only(std::collections::BTreeSet<String>),
}

impl WingVisibility {
    fn contains(&self, wing: &str) -> bool {
        match self {
            WingVisibility::All => true,
            WingVisibility::Only(wings) => wings.contains(wing),
        }
    }
}

/// A caller's wing restriction for `coordination_read`, resolved once from its
/// [`AuthIdentity`] and owned here so the borrowed [`CoordinationVisibility`] it produces
/// (via [`Self::visibility`]) never needs to outlive a separate `Option<Vec<String>>` the
/// call site would otherwise have to keep alive alongside it.
///
/// `route_coordination_inbox`'s cross-wing branch and `route_coordination_events` both need
/// exactly this conversion; it used to be hand-copied at both sites, which is the same trap
/// that has already produced three previous coordination fixes landing on one of these two
/// routes and not the other (see the module-level "Wing authorization" note).
struct CoordinationReadScope {
    restrict_wings: Option<Vec<String>>,
}

impl CoordinationReadScope {
    fn resolve(auth: &AuthIdentity) -> Self {
        let restrict_wings = match auth.visible_wings(Operation::CoordinationRead) {
            WingVisibility::All => None,
            WingVisibility::Only(wings) => Some(wings.into_iter().collect()),
        };
        Self { restrict_wings }
    }

    /// The `CoordinationVisibility` this scope implies. Borrows `self`, so the
    /// `CoordinationReadScope` must stay alive as long as the returned value is used.
    fn visibility(&self) -> CoordinationVisibility<'_> {
        match &self.restrict_wings {
            None => CoordinationVisibility::Federated(None),
            Some(wings) => CoordinationVisibility::Federated(Some(wings.as_slice())),
        }
    }
}

/// A single entry in the bearer-token file as stored on disk.
///
/// `deny_unknown_fields` is deliberate: an absent `scopes` field means
/// unrestricted access (see below), so a typo'd key — `"scope"`,
/// `"scopees"`, a trailing space — would otherwise silently produce an
/// unrestricted token instead of the operator's intended scoped one. That
/// fails open on exactly the kind of mistake this format needs to fail
/// closed on, so an unrecognised key is a load error instead.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenEntry {
    /// The raw bearer token string.
    token: String,
    /// Human-readable identity name (returned as the auth principal).
    name: String,
    /// If `false`, the token is treated as non-existent during auth.
    enabled: bool,
    /// Scoped-access grants. `None` (the field absent, or explicit JSON
    /// `null`) means the token predates scoping and keeps unrestricted
    /// access — this grandfathering rule is what keeps existing
    /// `server_tokens.json` files working unchanged. `Some(vec![])` — an
    /// explicit empty array — is a deliberate lockout with no access at all,
    /// and is NOT a synonym for absent.
    #[serde(default)]
    scopes: Option<Vec<RawTokenScope>>,
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
    /// Resolved scopes; see [`TokenEntry::scopes`] for the `None` vs
    /// `Some(vec![])` distinction.
    scopes: Option<Vec<TokenScopeEntry>>,
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
        Self::check_token_file_permissions(path)?;
        let raw = std::fs::read_to_string(path)
            .map_err(|source| ServerError::Io { path: path.clone(), source })?;
        let file_entries: Vec<TokenEntry> =
            serde_json::from_str(&raw).map_err(|err| ServerError::TokenFile(err.to_string()))?;
        let mut entries = Vec::with_capacity(file_entries.len());
        for entry in file_entries {
            if entry.name.trim().is_empty() {
                return Err(ServerError::TokenFile(
                    "token entry name must not be empty".to_owned(),
                ));
            }
            // A colon in a token's configured `name` is ambiguous with the
            // `{identity}:{claimed}` encoding `resolve_coordination_actor`
            // builds for a claimed actor that disagrees with the
            // authenticated identity (see `route_drawers_add`'s identical
            // rule). A token named `ci` claiming actor `worker` would
            // otherwise produce the same principal string, `ci:worker`, as a
            // distinct token whose configured name literally is
            // `ci:worker` — and coordination uses that exact string as a
            // lease-ownership and transition-authorization identity, not
            // just provenance. Reject at load time, matching how the
            // registry already fails closed on other malformed entries.
            if entry.name.contains(':') {
                return Err(ServerError::TokenFile(format!(
                    "token entry name `{}` must not contain `:`",
                    entry.name
                )));
            }
            if entry.enabled && entry.token.trim().is_empty() {
                return Err(ServerError::TokenFile(format!(
                    "enabled token `{}` must not be empty",
                    entry.name
                )));
            }
            let scopes = entry
                .scopes
                .map(|raw_scopes| {
                    raw_scopes
                        .into_iter()
                        .map(|raw| {
                            let wings = raw
                                .wings
                                .iter()
                                .map(|w| normalize_scope_wing(w))
                                .collect::<Result<Vec<Vec<_>>, _>>()?
                                .into_iter()
                                .flatten()
                                .collect();
                            Ok(TokenScopeEntry { wings, operations: raw.operations })
                        })
                        .collect::<Result<Vec<_>, ServerError>>()
                })
                .transpose()?;
            entries.push(TokenRegistryEntry {
                name: entry.name,
                enabled: entry.enabled,
                token_hash: blake3::hash(entry.token.as_bytes()),
                scopes,
            });
        }
        let mtime = std::fs::metadata(path).and_then(|m| m.modified()).ok();
        Ok(TokenRegistryInner { entries, mtime })
    }

    /// Verify the token file is not group- or world-readable.
    ///
    /// On Unix this checks `mode & 0o077 == 0`.  On Windows the check is
    /// skipped because the platform permission model is fundamentally
    /// different and the file lives in the user's home directory by default.
    #[cfg(unix)]
    fn check_token_file_permissions(path: &PathBuf) -> Result<(), ServerError> {
        use std::os::unix::fs::PermissionsExt;
        let meta = std::fs::metadata(path)
            .map_err(|source| ServerError::Io { path: path.clone(), source })?;
        let mode = meta.permissions().mode();
        if mode & 0o077 != 0 {
            return Err(ServerError::TokenFile(format!(
                "token file `{}` has permissions {:#o}; expected 0600 or more restrictive",
                path.display(),
                mode & 0o777,
            )));
        }
        Ok(())
    }

    #[cfg(not(unix))]
    fn check_token_file_permissions(_path: &PathBuf) -> Result<(), ServerError> {
        Ok(())
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
                // Fail closed on malformed reloads so emergency revocation edits
                // cannot leave the previous token set active indefinitely.
                warn!(
                    "failed to reload token file; disabled all tokens until reload succeeds: {err}"
                );
                guard.entries.clear();
                if matches!(err, ServerError::TokenFile(_)) {
                    guard.mtime = current_mtime;
                } else {
                    guard.mtime = None;
                }
            }
        }
    }

    /// Authenticates `presented` token using constant-time comparison on BLAKE3
    /// hashes. Returns the resolved [`AuthIdentity`] (name plus scopes) for an
    /// enabled, matching token; `None` otherwise.
    pub fn authenticate(&self, presented: &str) -> Option<AuthIdentity> {
        if presented.trim().is_empty() {
            return None;
        }
        self.check_reload();
        let presented_hash = blake3::hash(presented.as_bytes());
        let guard = self.inner.read().ok()?;
        for entry in &guard.entries {
            if !entry.enabled {
                continue;
            }
            let eq: bool = presented_hash.as_bytes().ct_eq(entry.token_hash.as_bytes()).into();
            if eq {
                return Some(AuthIdentity(entry.name.clone(), entry.scopes.clone()));
            }
        }
        None
    }
}

/// Extension type inserted into axum request extensions by the auth middleware.
///
/// Widened for scoped tokens (issue #102 Stage 2) to carry the token's
/// resolved scopes alongside its identity name. The identity string stays the
/// first tuple field so existing call sites (`auth.0.0`, used for `added_by`
/// and `ChangeEvent.actor` provenance) keep compiling unchanged.
#[derive(Debug, Clone)]
pub struct AuthIdentity(
    /// The authenticated identity name.
    pub String,
    /// Resolved scopes. `None` means unrestricted (grandfathered); see
    /// [`TokenEntry::scopes`] for the full `None` vs `Some(vec![])` rule.
    Option<Vec<TokenScopeEntry>>,
);

impl AuthIdentity {
    /// The authenticated identity name.
    pub fn name(&self) -> &str {
        &self.0
    }

    /// True when this identity may perform `op` at all, independent of wing.
    ///
    /// This is the check used for operations with no wing concept (Group D:
    /// the KG routes — KG facts are entity-scoped, not wing-scoped, matching
    /// `resolve_kg_route` in `mempalace-config`, which deliberately skips the
    /// wing lookup entirely) and as the coarse per-route gate applied by
    /// `operation_gate` before any wing-specific check runs: a scope entry
    /// grants the operation if it appears in its `operations` list, no matter
    /// which wings that entry covers.
    fn allows_operation(&self, op: Operation) -> bool {
        match &self.1 {
            None => true,
            Some(scopes) => scopes.iter().any(|s| scope_grants(s, op)),
        }
    }

    /// True when this identity may perform `op` specifically on `wing`.
    fn allows_wing(&self, op: Operation, wing: &str) -> bool {
        match &self.1 {
            None => true,
            Some(scopes) => scopes
                .iter()
                .any(|s| scope_grants(s, op) && s.wings.iter().any(|w| w == "*" || w == wing)),
        }
    }

    /// The set of wings visible to this identity for `op`. Used by aggregate
    /// (Group C) routes to filter their response rather than reject it.
    fn visible_wings(&self, op: Operation) -> WingVisibility {
        match &self.1 {
            None => WingVisibility::All,
            Some(scopes) => {
                let mut wings = std::collections::BTreeSet::new();
                for scope in scopes {
                    if !scope_grants(scope, op) {
                        continue;
                    }
                    if scope.wings.iter().any(|w| w == "*") {
                        return WingVisibility::All;
                    }
                    wings.extend(scope.wings.iter().cloned());
                }
                WingVisibility::Only(wings)
            }
        }
    }
}

/// True when `entry` grants `op`, either directly or via the one-way
/// implication that a `coordination_claim` grant also authorizes
/// `coordination_write`. Claiming a coordination task inherently requires
/// creating/mutating it, so a token scoped to claim tasks would otherwise be
/// unable to perform the writes claiming itself entails. The implication
/// runs claim → write only: a `coordination_write` grant does NOT imply
/// `coordination_claim`, and `coordination_read` is unaffected.
///
/// This is an authorization-time rule, deliberately not expanded when the
/// token file is parsed (`RawTokenScope`/`normalize_scope_wing`) — the
/// closed set of operations an operator wrote in their token file stays
/// exactly as written; only the check applied to it is widened.
fn scope_grants(entry: &TokenScopeEntry, op: Operation) -> bool {
    entry.operations.contains(&op)
        || (op == Operation::CoordinationWrite
            && entry.operations.contains(&Operation::CoordinationClaim))
}

// ─── Server state ─────────────────────────────────────────────────────────────

/// Shared state for the federation server.
///
/// Wrapped in `Arc` and used as axum state.
pub struct ServerState<P> {
    /// MemPalace configuration.
    pub config: MempalaceConfig,
    /// Storage engine (drawer store + operational store).
    pub storage: StorageEngine,
    /// Coordination store (tasks, messages, artifacts, results, audit events).
    /// Opens the same `storage.sqlite3` file as `storage`'s operational store,
    /// via its own connection — the same construction `McpRuntime::new` uses
    /// in `mempalace-mcp`. Coordination routes are server-only as of issue
    /// #102 Stage 3; see docs/Coordination.md.
    pub coordination: CoordinationStore,
    /// Search runtime. Wrapped in a `Mutex` because `SearchRuntime::search`
    /// takes `&mut self`.
    pub search: Mutex<SearchRuntime<P>>,
    /// Bearer-token registry for auth.
    pub tokens: Arc<TokenRegistry>,
    /// The most recent maintenance run summary, if any.
    pub last_maintenance_status: std::sync::Mutex<Option<MaintenanceRunSummary>>,
    /// Typed status of the maintenance subsystem.
    pub maintenance_status: std::sync::Mutex<MaintenanceStatus>,
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
/// let (router, _state) = build_router(config, provider, tokens).await?;
/// // let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
/// // axum::serve(listener, router).await?;
/// # Ok(())
/// # }
/// ```
pub async fn build_router<P>(
    config: MempalaceConfig,
    provider: P,
    tokens: TokenRegistry,
) -> Result<(Router, Arc<ServerState<P>>), ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let storage = StorageEngine::open(&config.palace_path, config.embedding_profile).await?;
    let coordination = CoordinationStore::new(config.palace_path.join("storage.sqlite3"));
    coordination.ensure_schema()?;
    let search = SearchRuntime::with_policy(
        provider,
        SearchRuntimePolicy { rerank_enabled: config.low_cpu.effective_rerank_enabled() },
    );

    // Extract maintenance config before moving `config` into state.
    let maintenance_settings = MaintenanceSettings {
        enabled: config.maintenance.enabled,
        idle_secs: config.maintenance.idle_secs as u64,
        version_retention_hours: config.maintenance.version_retention_hours as u64,
        tail_threshold_rows: config.maintenance.tail_threshold_rows as u64,
        small_fragment_threshold: config.maintenance.small_fragment_threshold as u64,
    };
    let background_maintenance_enabled = config.maintenance.background_enabled;

    let initial_status = if maintenance_settings.enabled {
        MaintenanceStatus::Idle
    } else {
        MaintenanceStatus::Disabled
    };

    let state = Arc::new(ServerState {
        config,
        storage,
        coordination,
        search: Mutex::new(search),
        tokens: Arc::new(tokens),
        last_maintenance_status: std::sync::Mutex::new(None),
        maintenance_status: std::sync::Mutex::new(initial_status),
    });

    // ── Background maintenance task ──────────────────────────────────────────
    //
    // Runs only in the long-lived HTTP hub.  Starts with an immediate startup
    // check, then loops with a jittered sleep so concurrent hubs do not
    // synchronise their runs. Storage write paths reset the idle timer, so
    // maintenance only fires when writes have been idle for the configured duration.
    if maintenance_settings.enabled && background_maintenance_enabled {
        let task_state = Arc::clone(&state);
        let settings = maintenance_settings;
        tokio::spawn(async move {
            // Startup maintenance check
            info!("performing startup maintenance check");
            // Both mutexes below guard a plain status value with no invariant a panic
            // could corrupt, so recovering from poison is strictly better than
            // panicking a second time on every subsequent read/write of this state.
            *task_state.maintenance_status.lock().unwrap_or_else(PoisonError::into_inner) =
                MaintenanceStatus::Running;
            match task_state.storage.run_maintenance(&settings).await {
                Ok(summary) => {
                    *task_state
                        .last_maintenance_status
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner) = Some(summary.clone());
                    *task_state.maintenance_status.lock().unwrap_or_else(PoisonError::into_inner) =
                        summary_to_status(&summary);
                }
                Err(e) => {
                    warn!(error = %e, "startup maintenance run failed");
                    *task_state.maintenance_status.lock().unwrap_or_else(PoisonError::into_inner) =
                        MaintenanceStatus::Failed { message: e.to_string() };
                }
            }

            loop {
                let base = Duration::from_secs(settings.idle_secs);
                let jitter_frac = (SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .subsec_nanos() as f64)
                    / 1_000_000_000.0;
                let jitter = Duration::from_secs_f64(jitter_frac * base.as_secs_f64() * 0.1);
                let sleep_dur = base + jitter;

                tokio::time::sleep(sleep_dur).await;
                if task_state.storage.elapsed_since_last_activity()
                    < Duration::from_secs(settings.idle_secs)
                {
                    continue;
                }

                // Clear only activity that predates the completed idle window.
                // Re-check elapsed time after taking the flag so a write racing
                // this transition still postpones the run.
                if task_state.storage.take_activity_signal()
                    && task_state.storage.elapsed_since_last_activity()
                        < Duration::from_secs(settings.idle_secs)
                {
                    continue;
                }
                info!("background maintenance check");
                *task_state.maintenance_status.lock().unwrap_or_else(PoisonError::into_inner) =
                    MaintenanceStatus::Running;
                match task_state.storage.run_maintenance(&settings).await {
                    Ok(summary) => {
                        *task_state
                            .last_maintenance_status
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner) = Some(summary.clone());
                        *task_state
                            .maintenance_status
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner) = summary_to_status(&summary);
                    }
                    Err(e) => {
                        warn!(error = %e, "background maintenance run failed");
                        *task_state
                            .maintenance_status
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner) =
                            MaintenanceStatus::Failed { message: e.to_string() };
                    }
                }
            }
        });
    }

    // Unauthenticated routes
    let public = Router::new().route("/v1/health", get(route_health));

    // Authenticated routes — wrapped with the auth middleware, and each with a
    // per-route operation gate (see "Route authorization" below) declaring the
    // operation that route requires. `/v1/info` carries no gate: any
    // authenticated token may call it.
    //
    // The ingest/batch route gets a 16 MiB body limit (vs axum's 2 MiB default)
    // and is merged in as a separate sub-router so the limit is scoped to it
    // only; all other routes keep the default.
    let ingest_route = Router::new()
        .route(
            "/v1/ingest/batch",
            post(route_ingest_batch::<P>).layer(middleware::from_fn(require_ingest)),
        )
        .layer(DefaultBodyLimit::max(16 * 1024 * 1024))
        .layer(middleware::from_fn_with_state(Arc::clone(&state), auth_middleware::<P>));

    let protected = Router::new()
        .route("/v1/info", get(route_info::<P>))
        .route(
            "/v1/drawers/search",
            post(route_drawers_search::<P>).layer(middleware::from_fn(require_read)),
        )
        .route(
            "/v1/drawers/check_duplicate",
            post(route_drawers_check_duplicate::<P>).layer(middleware::from_fn(require_read)),
        )
        .route(
            "/v1/drawers",
            post(route_drawers_add::<P>).layer(middleware::from_fn(require_write)),
        )
        .route(
            "/v1/drawers",
            get(route_drawers_list::<P>).layer(middleware::from_fn(require_read)),
        )
        .route(
            "/v1/drawers/{id}",
            get(route_drawers_get::<P>).layer(middleware::from_fn(require_read)),
        )
        .route(
            "/v1/drawers/{id}",
            delete(route_drawers_delete::<P>).layer(middleware::from_fn(require_delete)),
        )
        .route("/v1/kg/query", post(route_kg_query::<P>).layer(middleware::from_fn(require_read)))
        .route("/v1/kg/facts", post(route_kg_add::<P>).layer(middleware::from_fn(require_write)))
        .route(
            "/v1/kg/facts/invalidate",
            post(route_kg_invalidate::<P>).layer(middleware::from_fn(require_write)),
        )
        .route(
            "/v1/kg/timeline",
            get(route_kg_timeline::<P>).layer(middleware::from_fn(require_read)),
        )
        .route("/v1/kg/stats", get(route_kg_stats::<P>).layer(middleware::from_fn(require_read)))
        .route("/v1/taxonomy", get(route_taxonomy::<P>).layer(middleware::from_fn(require_read)))
        .route("/v1/wings", get(route_wings::<P>).layer(middleware::from_fn(require_read)))
        .route("/v1/rooms", get(route_rooms::<P>).layer(middleware::from_fn(require_read)))
        .route("/v1/changes", get(route_changes::<P>).layer(middleware::from_fn(require_read)))
        .route(
            "/v1/coordination/tasks",
            post(route_coordination_task_create::<P>)
                .layer(middleware::from_fn(require_coordination_write)),
        )
        .route(
            "/v1/coordination/tasks/{id}",
            get(route_coordination_task_get::<P>)
                .layer(middleware::from_fn(require_coordination_read)),
        )
        .route(
            "/v1/coordination/tasks/{id}/claim",
            post(route_coordination_task_claim::<P>)
                .layer(middleware::from_fn(require_coordination_claim)),
        )
        .route(
            "/v1/coordination/tasks/{id}/renew",
            post(route_coordination_task_renew::<P>)
                .layer(middleware::from_fn(require_coordination_claim)),
        )
        .route(
            "/v1/coordination/tasks/{id}/transition",
            post(route_coordination_task_transition::<P>)
                .layer(middleware::from_fn(require_coordination_claim)),
        )
        .route(
            "/v1/coordination/messages",
            post(route_coordination_message_send::<P>)
                .layer(middleware::from_fn(require_coordination_write)),
        )
        .route(
            "/v1/coordination/messages/{id}",
            get(route_coordination_message_get::<P>)
                .layer(middleware::from_fn(require_coordination_read)),
        )
        .route(
            "/v1/coordination/messages/{id}/ack",
            post(route_coordination_message_ack::<P>)
                .layer(middleware::from_fn(require_coordination_write)),
        )
        .route(
            "/v1/coordination/inbox",
            get(route_coordination_inbox::<P>)
                .layer(middleware::from_fn(require_coordination_read)),
        )
        .route(
            "/v1/coordination/artifacts",
            post(route_coordination_artifact_put::<P>)
                .layer(middleware::from_fn(require_coordination_write)),
        )
        .route(
            "/v1/coordination/artifacts/{id}",
            get(route_coordination_artifact_get::<P>)
                .layer(middleware::from_fn(require_coordination_read)),
        )
        .route(
            "/v1/coordination/results",
            post(route_coordination_result_put::<P>)
                .layer(middleware::from_fn(require_coordination_write)),
        )
        .route(
            "/v1/coordination/results/{id}",
            get(route_coordination_result_get::<P>)
                .layer(middleware::from_fn(require_coordination_read)),
        )
        .route(
            "/v1/coordination/events",
            get(route_coordination_events::<P>)
                .layer(middleware::from_fn(require_coordination_read)),
        )
        .layer(middleware::from_fn_with_state(Arc::clone(&state), auth_middleware::<P>));

    let router = public.merge(protected).merge(ingest_route).with_state(Arc::clone(&state));
    Ok((router, state))
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
            request.extensions_mut().insert(identity);
            next.run(request).await
        }
        None => ServerError::Unauthorized.into_response(),
    }
}

// ─── Route authorization ───────────────────────────────────────────────────────
//
// Two layers do the work, matching the route groups in
// docs/Coordination-Phase-3-Design.md (Stage 2):
//
// 1. Operation gate (this section) — attached per-route via
//    `MethodRouter::layer` above, so the required operation is declared right
//    next to the route registration rather than threaded through every
//    handler signature. It runs after `auth_middleware` (which has already
//    inserted `AuthIdentity` into the request extensions) and rejects with
//    403 before the handler runs at all if the identity is not granted the
//    operation by any scope entry. This alone is sufficient for routes with
//    no wing concept at all (Group D: the KG routes).
// 2. Wing check inside the handler — for routes where the wing is only known
//    once the body is parsed or a resource is looked up (Group A-body:
//    `POST /v1/drawers`, `POST /v1/ingest/batch`; Group B: get/delete by id),
//    or where the route aggregates across wings and must filter its response
//    rather than reject the request (Group C: taxonomy/wings/rooms/changes,
//    and — despite having no wing in its own request — `check_duplicate`,
//    because its response can still reveal which wing a match lives in).
//    These call `AuthIdentity::allows_wing` or `AuthIdentity::visible_wings`
//    directly inside the handler; see each one.

async fn require_read(request: Request<Body>, next: Next) -> Response {
    operation_gate(Operation::Read, request, next).await
}

async fn require_write(request: Request<Body>, next: Next) -> Response {
    operation_gate(Operation::Write, request, next).await
}

async fn require_delete(request: Request<Body>, next: Next) -> Response {
    operation_gate(Operation::Delete, request, next).await
}

async fn require_ingest(request: Request<Body>, next: Next) -> Response {
    operation_gate(Operation::Ingest, request, next).await
}

async fn require_coordination_read(request: Request<Body>, next: Next) -> Response {
    operation_gate(Operation::CoordinationRead, request, next).await
}

async fn require_coordination_write(request: Request<Body>, next: Next) -> Response {
    operation_gate(Operation::CoordinationWrite, request, next).await
}

async fn require_coordination_claim(request: Request<Body>, next: Next) -> Response {
    operation_gate(Operation::CoordinationClaim, request, next).await
}

/// Shared implementation for the per-route operation gates above. A missing
/// `AuthIdentity` extension falls back to 401 defensively; it should be
/// unreachable in practice because every route these gates are attached to
/// also runs behind `auth_middleware`, which always runs first (see
/// `build_router`) and rejects unauthenticated requests before routing.
async fn operation_gate(op: Operation, request: Request<Body>, next: Next) -> Response {
    match request.extensions().get::<AuthIdentity>() {
        Some(identity) if identity.allows_operation(op) => next.run(request).await,
        Some(_) => ServerError::Forbidden.into_response(),
        None => ServerError::Unauthorized.into_response(),
    }
}

// ─── Health ──────────────────────────────────────────────────────────────────

async fn route_health() -> impl IntoResponse {
    Json(json!({"status": "ok"}))
}

// ─── Info ─────────────────────────────────────────────────────────────────────

/// Converts a [`MaintenanceRunSummary`] (from the storage engine) into a
/// [`MaintenanceStatus`] (the federation DTO) by examining the tier outcomes.
fn summary_to_status(summary: &MaintenanceRunSummary) -> MaintenanceStatus {
    if summary.tier_results.is_empty() {
        return MaintenanceStatus::Disabled;
    }

    let all_skipped = summary
        .tier_results
        .iter()
        .all(|r| matches!(r.outcome, MaintenanceOutcome::Skipped { .. }));
    let any_aborted = summary
        .tier_results
        .iter()
        .any(|r| matches!(r.outcome, MaintenanceOutcome::Aborted { .. }));
    let any_failed =
        summary.tier_results.iter().any(|r| matches!(r.outcome, MaintenanceOutcome::Failed { .. }));
    let any_completed = summary
        .tier_results
        .iter()
        .any(|r| matches!(r.outcome, MaintenanceOutcome::Completed { .. }));

    if all_skipped {
        let reason = summary.tier_results.first().and_then(|r| match &r.outcome {
            MaintenanceOutcome::Skipped { reason } => Some(reason),
            _ => None,
        });
        let fed_reason = match reason {
            Some(MaintenanceSkipReason::NotIdle) => FedMaintenanceSkipReason::NotIdle,
            Some(MaintenanceSkipReason::NothingToDo) => FedMaintenanceSkipReason::NothingToDo,
            _ => FedMaintenanceSkipReason::NotIdle,
        };
        return MaintenanceStatus::Skipped { reason: fed_reason };
    }

    if any_aborted && !any_completed {
        let reason = summary.tier_results.iter().find_map(|r| match &r.outcome {
            MaintenanceOutcome::Aborted { reason, .. } => Some(reason),
            _ => None,
        });
        let fed_reason = match reason {
            Some(MaintenanceAbortReason::ConcurrentRun) => FedMaintenanceAbortReason::ConcurrentRun,
            Some(MaintenanceAbortReason::Shutdown) => FedMaintenanceAbortReason::Shutdown,
            Some(MaintenanceAbortReason::Timeout) => FedMaintenanceAbortReason::Timeout,
            _ => FedMaintenanceAbortReason::ConcurrentRun,
        };
        return MaintenanceStatus::Aborted { reason: fed_reason };
    }

    if any_failed && !any_completed {
        let msg = summary
            .tier_results
            .iter()
            .find_map(|r| match &r.outcome {
                MaintenanceOutcome::Failed { message } => Some(message.clone()),
                _ => None,
            })
            .unwrap_or_default();
        return MaintenanceStatus::Failed { message: msg };
    }

    let fed_status = match summary.status {
        mempalace_storage::MaintenanceRunStatus::Success => FedMaintenanceRunStatus::Success,
        mempalace_storage::MaintenanceRunStatus::Partial => FedMaintenanceRunStatus::Partial,
        mempalace_storage::MaintenanceRunStatus::Failure => FedMaintenanceRunStatus::Failure,
    };
    MaintenanceStatus::Completed { status: fed_status }
}

async fn route_info<P>(
    State(state): State<Arc<ServerState<P>>>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let last_run = state
        .last_maintenance_status
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .as_ref()
        .and_then(|s| serde_json::to_value(s).ok());

    let status = state.maintenance_status.lock().unwrap_or_else(PoisonError::into_inner).clone();

    Ok(Json(InfoResponse {
        server_version: BUILD_VERSION.to_owned(),
        federation_api_version: FEDERATION_API_VERSION,
        embedding_profile: state.config.embedding_profile.as_str().to_owned(),
        capabilities: vec![
            "drawers".to_owned(),
            "kg".to_owned(),
            "changes".to_owned(),
            "taxonomy".to_owned(),
            "ingest".to_owned(),
            "coordination".to_owned(),
        ],
        maintenance_enabled: state.config.maintenance.enabled,
        maintenance_background_enabled: state.config.maintenance.background_enabled,
        maintenance_idle_secs: state.config.maintenance.idle_secs as u64,
        maintenance_last_run: last_run,
        maintenance_status: status,
    }))
}

// ─── Drawers: search ─────────────────────────────────────────────────────────

async fn route_drawers_search<P>(
    State(state): State<Arc<ServerState<P>>>,
    auth: axum::extract::Extension<AuthIdentity>,
    Json(body): Json<DrawerSearchRequest>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    if body.query.len() > MAX_SEARCH_QUERY_BYTES {
        return Err(ServerError::InvalidParams(format!(
            "query must be at most {MAX_SEARCH_QUERY_BYTES} bytes"
        )));
    }

    let limit =
        body.limit.unwrap_or(5).max(1).min(state.config.low_cpu.effective_search_results_limit());

    let wing = body.wing.as_deref().map(WingId::new).transpose()?;
    let room = body.room.as_deref().map(RoomId::new).transpose()?;
    let view = body.view.clone();

    // Group A: wing is optional here. When given, it must be authorized
    // outright (403 on mismatch). When absent this is a cross-wing read, so
    // instead of rejecting we filter the results below to what the token can
    // see (Group C-style filtering), matching Group A's documented rule.
    if let Some(w) = &wing {
        if !auth.0.allows_wing(Operation::Read, w.as_str()) {
            return Err(ServerError::Forbidden);
        }
    }
    let visibility = wing.is_none().then(|| auth.0.visible_wings(Operation::Read));

    // Over-fetch from the search runtime to compensate for diary rows and
    // scope-invisible rows filtered out below — the runtime ranks and
    // truncates to the limit it is given, so asking for exactly `limit` and
    // then filtering could silently return fewer than `limit` results even
    // though enough visible candidates exist further down the ranking. This
    // route still filters visibility post-fetch (unlike `route_drawers_list`,
    // which now pushes its visible-wing set into the storage query — see the
    // comment on that route's `restrict_wings`), because search is a ranked
    // top-K with no continuation promise: a short page here is an acceptable,
    // bounded trade-off, whereas `route_drawers_list`'s `limit`/`next_cursor`
    // shape implies the caller can page through everything it can see, which
    // post-fetch filtering cannot guarantee. Same 2x heuristic as
    // `route_drawers_list`'s `storage_limit` regardless.
    let search_limit = limit.saturating_mul(2);

    let results = {
        let mut search = state.search.lock().await;
        search
            .search(
                state.storage.drawer_store(),
                &SearchQuery {
                    text: body.query,
                    wing,
                    room,
                    limit: search_limit,
                    profile: state.config.embedding_profile,
                    view,
                },
            )
            .await?
    };

    let results: Vec<RemoteDrawerResult> = results
        .into_iter()
        .filter(|r| !is_diary_wing_or_room(r.wing.as_str(), r.room.as_str()))
        .filter(|r| visibility.as_ref().map(|v| v.contains(r.wing.as_str())).unwrap_or(true))
        .take(limit)
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
    if body.content.len() > MAX_DRAWER_CONTENT_BYTES {
        return Err(ServerError::InvalidParams(format!(
            "content must be at most {MAX_DRAWER_CONTENT_BYTES} bytes"
        )));
    }

    // Reject diary-shaped writes. A content rule, not an identity rule: it
    // runs before and independent of the scope check below, and applies
    // regardless of what the token is scoped to.
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

    // Group A: wing is required and body-derived, so it can only be checked
    // here, not in the per-route operation gate.
    if !auth.0.allows_wing(Operation::Write, wing.as_str()) {
        return Err(ServerError::Forbidden);
    }

    // Duplicate check. `find_duplicates` scans every wing, but this handler
    // has only established that the caller may WRITE `wing` — not that it may
    // READ whatever wing a near-duplicate happens to live in. Filtering only
    // the returned `matches` is not enough: the 409 status itself is the
    // oracle. So, mirroring `route_drawers_check_duplicate` (identical
    // shape), filter to `visible_wings(Operation::Read)` plus the diary rule
    // BEFORE deciding whether to return 409 at all. For an unrestricted token
    // every wing is visible, so behaviour is unchanged.
    //
    // Trade-off, deliberate — see docs/Federation.md §1.5: a scoped writer
    // can now create a drawer that duplicates content in a wing it cannot
    // read, because that duplicate is invisible to this check. The
    // alternative — reporting it — would disclose that wing's content to a
    // caller not authorized to read it, which is worse.
    let visibility = auth.0.visible_wings(Operation::Read);
    let duplicates = find_duplicates(&state, &body.content, DEFAULT_DUPLICATE_THRESHOLD).await?;
    let duplicates: Vec<Value> = duplicates
        .into_iter()
        .filter(|m| {
            let dup_wing = m.get("wing").and_then(Value::as_str).unwrap_or("");
            let dup_room = m.get("room").and_then(Value::as_str).unwrap_or("");
            !is_diary_wing_or_room(dup_wing, dup_room) && visibility.contains(dup_wing)
        })
        .collect();
    if !duplicates.is_empty() {
        return Err(ServerError::Duplicate(
            serde_json::to_value(&duplicates).unwrap_or(Value::Array(vec![])),
        ));
    }
    let identity = auth.0.0;

    // Determine added_by: identity[:claimed]
    let added_by = match &body.added_by {
        Some(claimed) if claimed != &identity => format!("{identity}:{claimed}"),
        _ => identity.clone(),
    };

    let now = OffsetDateTime::now_utc();
    let drawer_id =
        generated_drawer_id("drawer", wing.as_str(), room.as_str(), &body.content, now)?;
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
        details_json: Some(json!({"wing": wing.as_str(), "room": room.as_str()}).to_string()),
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
    auth: axum::extract::Extension<AuthIdentity>,
    Json(body): Json<CheckDuplicateRequest>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    if body.content.len() > MAX_DRAWER_CONTENT_BYTES {
        return Err(ServerError::InvalidParams(format!(
            "content must be at most {MAX_DRAWER_CONTENT_BYTES} bytes"
        )));
    }

    // Group C, not operation-only: there is no wing in the request (the
    // whole point is to search across all of them), so this is authorized
    // like the aggregate routes — require `read` (enforced by the per-route
    // gate before this handler runs), then filter matches to visible wings
    // rather than reject. Without this a scoped token could post arbitrary
    // content and learn, from the returned `matches` (which carry `wing` and
    // `room`) or even just the `is_duplicate` boolean, whether near-identical
    // content exists in a wing it cannot read — so `is_duplicate` below is
    // computed strictly AFTER filtering, not before.
    let visibility = auth.0.visible_wings(Operation::Read);

    let threshold = body.threshold.unwrap_or(DEFAULT_DUPLICATE_THRESHOLD);
    let matches = find_duplicates(&state, &body.content, threshold).await?;
    // Filter diary matches and matches outside the token's visible wings.
    let matches: Vec<Value> = matches
        .into_iter()
        .filter(|m| {
            let wing = m.get("wing").and_then(Value::as_str).unwrap_or("");
            let room = m.get("room").and_then(Value::as_str).unwrap_or("");
            !is_diary_wing_or_room(wing, room) && visibility.contains(wing)
        })
        .collect();
    let is_duplicate = !matches.is_empty();
    Ok(Json(CheckDuplicateResponse { is_duplicate, matches: Value::Array(matches) }))
}

// ─── Drawers: get by id ───────────────────────────────────────────────────────

async fn route_drawers_get<P>(
    State(state): State<Arc<ServerState<P>>>,
    auth: axum::extract::Extension<AuthIdentity>,
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

    // Group B: the wing is only known after this lookup. A caller without
    // read access to it gets the same 404 as a genuinely missing drawer
    // (matching the diary masking immediately above), so the response is
    // never an existence oracle for wings the caller cannot see.
    if !auth.0.allows_wing(Operation::Read, drawer.wing.as_str()) {
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

    // Group B: the wing is only known after the lookup above. Masked as 404,
    // not 403 — see the identical rule and rationale in `route_drawers_get`.
    if !auth.0.allows_wing(Operation::Delete, drawer.wing.as_str()) {
        return Err(ServerError::NotFound(format!("drawer {id} not found")));
    }
    let identity = auth.0.0;

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
        // wing/room recorded so `/v1/changes` (Group C) can filter deletion
        // events by scope the same way it already filters `drawer_added`.
        details_json: Some(
            json!({"wing": drawer.wing.as_str(), "room": drawer.room.as_str()}).to_string(),
        ),
    })?;

    Ok(Json(json!({"success": true})))
}

// ─── Drawers: list ───────────────────────────────────────────────────────────

async fn route_drawers_list<P>(
    State(state): State<Arc<ServerState<P>>>,
    auth: axum::extract::Extension<AuthIdentity>,
    Query(params): Query<ListDrawersQuery>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let limit = params.limit.unwrap_or(DEFAULT_PAGE_LIMIT).max(1).min(MAX_PAGE_LIMIT);
    let wing = params.wing.as_deref().map(WingId::new).transpose()?;
    let room = params.room.as_deref().map(RoomId::new).transpose()?;

    // Group A: wing is optional here, from the query string (same rule as
    // search's body-derived wing). Enforce outright when given; when absent,
    // filter below instead of rejecting.
    if let Some(w) = &wing {
        if !auth.0.allows_wing(Operation::Read, w.as_str()) {
            return Err(ServerError::Forbidden);
        }
    }
    // Unlike search (a ranked top-K with no continuation promise), this route
    // has a `limit`/`next_cursor` shape that implies a caller can page
    // through everything they can see. The storage layer has no
    // cursor-based pagination (see the `next_cursor` note below), so
    // visibility MUST be enforced by the storage query itself, not by
    // filtering an already-`limit`-bounded result after the fact: filtering
    // post-fetch can leave authorized rows sitting below the fetch window
    // with `next_cursor: None` and no way to ever reach them — the same
    // failure class as an inbox that silently drops mail. Pushing
    // `wing IN (...)` into `DrawerFilter` (see `DrawerFilter::wings`)
    // eliminates that: storage never returns an invisible-wing row in the
    // first place, so no invisible-wing volume can crowd a visible page out.
    let restrict_wings: Option<Vec<WingId>> = if wing.is_none() {
        match auth.0.visible_wings(Operation::Read) {
            WingVisibility::All => None,
            WingVisibility::Only(wings) => {
                Some(wings.iter().filter_map(|w| WingId::new(w.as_str()).ok()).collect())
            }
        }
    } else {
        None
    };

    // An empty `Only` set means zero wings are visible — short-circuit
    // rather than pass an empty `wings` to `DrawerFilter`, which means
    // "unconstrained" there (see its doc comment), not "match nothing".
    if restrict_wings.as_ref().is_some_and(Vec::is_empty) {
        return Ok(Json(ListDrawersResponse { drawers: Value::Array(vec![]), next_cursor: None }));
    }

    // Over-fetch from storage to compensate for diary rows filtered out
    // below — visibility is now enforced by the storage query itself (see
    // above), so this margin only needs to cover the diary-room exclusion,
    // not scope-invisible wings. The 2x factor is a heuristic; if diary rows
    // ever exceed it the result will be shorter than `limit`, but that is
    // rare and strictly better than the unbounded-load-all-then-take
    // approach.
    let storage_limit = limit.saturating_mul(2);
    let drawers = state
        .storage
        .drawer_store()
        .list_drawers(&DrawerFilter {
            wing,
            wings: restrict_wings.unwrap_or_default(),
            room,
            limit: Some(storage_limit),
            ..DrawerFilter::default()
        })
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
    validate_kg_field("entity", &body.entity)?;
    let as_of = body.as_of.as_deref().map(parse_date).transpose()?;
    let direction = parse_direction(body.direction.as_deref().unwrap_or("both"))?;
    let runtime = KnowledgeGraphRuntime::new(state.storage.operational_store());
    // A federation peer can legitimately have no record of an entity that is
    // present elsewhere in the federation. Represent that as an empty read.
    let facts = match runtime.query_entity(&body.entity, as_of, direction) {
        Ok(facts) => facts,
        Err(mempalace_graph::GraphError::UnknownEntity { .. }) => Vec::new(),
        Err(error) => return Err(error.into()),
    };
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
    validate_kg_field("subject", &body.subject)?;
    validate_kg_field("predicate", &body.predicate)?;
    validate_kg_field("object", &body.object)?;
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
    validate_kg_field("subject", &body.subject)?;
    validate_kg_field("predicate", &body.predicate)?;
    validate_kg_field("object", &body.object)?;
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

/// Query parameters for `GET /v1/kg/timeline`.
#[derive(Debug, serde::Deserialize)]
pub struct KgTimelineQuery {
    /// Optional entity filter.
    pub entity: Option<String>,
    /// Maximum number of timeline rows to return.
    pub limit: Option<usize>,
}

async fn route_kg_timeline<P>(
    State(state): State<Arc<ServerState<P>>>,
    Query(params): Query<KgTimelineQuery>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    if let Some(entity) = &params.entity {
        validate_kg_field("entity", entity)?;
    }
    let limit = params.limit.unwrap_or(DEFAULT_KG_TIMELINE_LIMIT).max(1).min(MAX_KG_TIMELINE_LIMIT);
    let entity = params.entity;
    let runtime = KnowledgeGraphRuntime::new(state.storage.operational_store());
    let mut timeline = match runtime.timeline(entity.as_deref()) {
        Ok(timeline) => timeline,
        Err(mempalace_graph::GraphError::UnknownEntity { .. }) => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    let total_count = timeline.len();
    timeline.truncate(limit);
    let count = timeline.len();
    Ok(Json(json!({
        "entity": entity.clone().unwrap_or_else(|| "all".to_owned()),
        "timeline": timeline,
        "count": count,
        "total_count": total_count,
    })))
}

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
    auth: axum::extract::Extension<AuthIdentity>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    // Group C: require read (enforced by the per-route gate before this
    // handler runs), then filter to the wings the token can see rather than
    // rejecting outright — a token scoped to one wing must get that wing's
    // taxonomy, not a 403.
    let visibility = auth.0.visible_wings(Operation::Read);
    let drawers = state.storage.drawer_store().list_drawers(&DrawerFilter::default()).await?;
    let mut taxonomy =
        std::collections::BTreeMap::<String, std::collections::BTreeMap<String, usize>>::new();
    for drawer in &drawers {
        if is_diary_wing_or_room(drawer.wing.as_str(), drawer.room.as_str()) {
            continue;
        }
        if !visibility.contains(drawer.wing.as_str()) {
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
    auth: axum::extract::Extension<AuthIdentity>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    // Group C filtering — see the identical comment in `route_taxonomy`.
    let visibility = auth.0.visible_wings(Operation::Read);
    let drawers = state.storage.drawer_store().list_drawers(&DrawerFilter::default()).await?;
    let mut wings = std::collections::BTreeMap::<String, usize>::new();
    for drawer in &drawers {
        if is_diary_wing_or_room(drawer.wing.as_str(), drawer.room.as_str()) {
            continue;
        }
        if !visibility.contains(drawer.wing.as_str()) {
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
    auth: axum::extract::Extension<AuthIdentity>,
    Query(params): Query<RoomsQuery>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    // Group C filtering — see the identical comment in `route_taxonomy`. This
    // applies even when the caller passes an explicit `?wing=`: a mismatched
    // scope filters the result to empty rather than rejecting the request.
    let visibility = auth.0.visible_wings(Operation::Read);
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
        if !visibility.contains(drawer.wing.as_str()) {
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
    auth: axum::extract::Extension<AuthIdentity>,
    Query(params): Query<ChangesQuery>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    // Group C filtering — see the comment in `route_taxonomy`. This is the
    // federated change feed, so it matters most here: a token scoped to one
    // wing must not see other wings' events go by.
    let visibility = auth.0.visible_wings(Operation::Read);

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

    let page = state.storage.operational_store().get_changes_page(since, cursor, limit)?;

    let next_cursor = page.next_cursor.map(|c| encode_cursor(&c));

    let events: Vec<ChangeEventDto> = page
        .events
        .into_iter()
        .filter(|ev| !is_diary_change_event(ev))
        .filter(|ev| change_event_visible(ev, &visibility))
        .map(change_event_to_dto)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Json(ChangesResponse { events, next_cursor }))
}

// ─── Ingest: batch ───────────────────────────────────────────────────────────

async fn route_ingest_batch<P>(
    State(state): State<Arc<ServerState<P>>>,
    auth: axum::extract::Extension<AuthIdentity>,
    Json(body): Json<IngestBatchRequest>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    // ── Validate wing ──────────────────────────────────────────────────────────
    let wing = WingId::new(&body.wing)?;

    // ── Diary guard: wing-level ───────────────────────────────────────────────
    // A content rule, not an identity rule — runs before and independent of
    // the scope check below, regardless of what the token is scoped to.
    if is_diary_wing_or_room(wing.as_str(), "") {
        return Err(ServerError::DiaryNotFederated);
    }

    // ── Scope check ────────────────────────────────────────────────────────────
    // Group A: wing is required and body-derived, so — like `POST /v1/drawers`
    // — it can only be checked here, not in the per-route operation gate.
    if !auth.0.allows_wing(Operation::Ingest, wing.as_str()) {
        return Err(ServerError::Forbidden);
    }
    let identity = auth.0.0;

    // ── Request-level validation ──────────────────────────────────────────────
    if body.files.is_empty() {
        return Err(ServerError::InvalidParams("files must not be empty".to_owned()));
    }
    if body.repo_id.is_empty() {
        return Err(ServerError::InvalidParams("repo_id must not be empty".to_owned()));
    }
    if body.files.len() > MAX_INGEST_FILES {
        return Err(ServerError::InvalidParams(format!(
            "files must contain at most {MAX_INGEST_FILES} entries"
        )));
    }
    let mut total_ingest_text_bytes = 0usize;
    for file in &body.files {
        for chunk in &file.chunks {
            total_ingest_text_bytes = total_ingest_text_bytes
                .checked_add(chunk.text.len())
                .ok_or_else(|| ServerError::InvalidParams("ingest text is too large".to_owned()))?;
        }
    }
    if total_ingest_text_bytes > MAX_INGEST_TEXT_BYTES {
        return Err(ServerError::InvalidParams(format!(
            "ingest chunk text must total at most {MAX_INGEST_TEXT_BYTES} bytes"
        )));
    }

    // ── Diary guard: any chunk's room ─────────────────────────────────────────
    for file in &body.files {
        for chunk in &file.chunks {
            if is_diary_wing_or_room("", &chunk.room) {
                return Err(ServerError::DiaryNotFederated);
            }
        }
    }

    // ── Determine added_by ────────────────────────────────────────────────────
    let added_by = match &body.agent {
        Some(agent) if agent != &identity => format!("{identity}:{agent}"),
        _ => identity.clone(),
    };

    // ── resolve_root for this wing (may be empty) ─────────────────────────────
    let resolve_root_path = state.config.server.checkouts.get(wing.as_str()).cloned();
    let resolve_root =
        resolve_root_path.as_ref().map(|p| p.to_string_lossy().into_owned()).unwrap_or_default();

    let now = OffsetDateTime::now_utc();
    let wing_str = wing.as_str().to_owned();
    let repo_id_hash = hash_text(&body.repo_id);

    // Build repository-view metadata for federated batch.
    let view_metadata = mempalace_core::RepositoryViewMetadata {
        repo_id: body.repo_id.clone(),
        view_name: None, // federated batches are always canonical
        source_path: resolve_root.clone(),
        head_commit: body.commit_hash.clone(),
        base_ref: None,
        merge_base: None,
        worktree_id: hash_text(&resolve_root),
        path_state: "present".to_owned(),
    };

    let mut file_results: Vec<IngestFileResult> = Vec::with_capacity(body.files.len());
    let mut files_ingested: usize = 0;
    let mut files_skipped: usize = 0;
    let mut files_failed: usize = 0;
    let mut total_drawers_written: usize = 0;
    // Track whether we emitted the missing-checkout warning (at most once per request)
    let mut warned_missing_checkout = false;

    for file in &body.files {
        let source_key = format!("projects:{wing_str}:{repo_id_hash}:{}", file.relative_path);

        if file.chunks.is_empty() {
            file_results.push(IngestFileResult {
                relative_path: file.relative_path.clone(),
                status: "failed".to_owned(),
                drawers_written: 0,
                error: Some("chunks must not be empty".to_owned()),
            });
            files_failed += 1;
            continue;
        }
        if file.chunks.len() > MAX_INGEST_CHUNKS_PER_FILE {
            file_results.push(IngestFileResult {
                relative_path: file.relative_path.clone(),
                status: "failed".to_owned(),
                drawers_written: 0,
                error: Some(format!(
                    "chunks must contain at most {MAX_INGEST_CHUNKS_PER_FILE} entries"
                )),
            });
            files_failed += 1;
            continue;
        }

        // ── Per-file validation ────────────────────────────────────────────────
        // relative_path safety: locator resolution joins it onto the server's
        // checkout root, so traversal or absolute paths would let a token
        // holder read files outside the checkout.
        if let Some(err) = invalid_ingest_relative_path(&file.relative_path) {
            file_results.push(IngestFileResult {
                relative_path: file.relative_path.clone(),
                status: "failed".to_owned(),
                drawers_written: 0,
                error: Some(err),
            });
            files_failed += 1;
            continue;
        }

        // Duplicate chunk_index values would collide on the same drawer id and
        // silently overwrite each other.
        {
            let mut seen = std::collections::BTreeSet::new();
            if let Some(chunk) = file.chunks.iter().find(|c| !seen.insert(c.chunk_index)) {
                file_results.push(IngestFileResult {
                    relative_path: file.relative_path.clone(),
                    status: "failed".to_owned(),
                    drawers_written: 0,
                    error: Some(format!("duplicate chunk_index {}", chunk.chunk_index)),
                });
                files_failed += 1;
                continue;
            }
        }

        // Validate byte ranges when file_hash is Some
        if file.file_hash.is_some() {
            let mut range_error: Option<String> = None;
            for chunk in &file.chunks {
                match (chunk.byte_start, chunk.byte_end, chunk.line_start, chunk.line_end) {
                    (Some(bs), Some(be), Some(ls), Some(le)) => {
                        if be < bs {
                            range_error = Some(format!(
                                "chunk {}: byte_end ({}) < byte_start ({})",
                                chunk.chunk_index, be, bs
                            ));
                            break;
                        }
                        if le < ls {
                            range_error = Some(format!(
                                "chunk {}: line_end ({}) < line_start ({})",
                                chunk.chunk_index, le, ls
                            ));
                            break;
                        }
                    }
                    _ => {
                        range_error = Some(format!(
                            "chunk {}: file_hash is set but byte/line ranges are missing",
                            chunk.chunk_index
                        ));
                        break;
                    }
                }
            }
            if let Some(err) = range_error {
                file_results.push(IngestFileResult {
                    relative_path: file.relative_path.clone(),
                    status: "failed".to_owned(),
                    drawers_written: 0,
                    error: Some(err),
                });
                files_failed += 1;
                continue;
            }
        }

        // If a checkout is configured, locator-backed rows must match the actual
        // server-side file bytes before any locator is stored.
        if let Some(file_hash) = &file.file_hash {
            if let Some(root) = &resolve_root_path {
                match read_checkout_file_for_locator(root.as_path(), &file.relative_path) {
                    Ok(bytes) => {
                        let actual_hash = hash_bytes(&bytes);
                        if actual_hash != *file_hash {
                            file_results.push(IngestFileResult {
                                relative_path: file.relative_path.clone(),
                                status: "failed".to_owned(),
                                drawers_written: 0,
                                error: Some(
                                    "file_hash does not match server checkout file".to_owned(),
                                ),
                            });
                            files_failed += 1;
                            continue;
                        }
                    }
                    Err(err) => {
                        file_results.push(IngestFileResult {
                            relative_path: file.relative_path.clone(),
                            status: "failed".to_owned(),
                            drawers_written: 0,
                            error: Some(err),
                        });
                        files_failed += 1;
                        continue;
                    }
                }
            }
        }

        // Validate empty chunk text
        if let Some(chunk) = file.chunks.iter().find(|c| c.text.is_empty()) {
            file_results.push(IngestFileResult {
                relative_path: file.relative_path.clone(),
                status: "failed".to_owned(),
                drawers_written: 0,
                error: Some(format!("chunk {}: text must not be empty", chunk.chunk_index)),
            });
            files_failed += 1;
            continue;
        }

        // ── Skip-unchanged check ───────────────────────────────────────────────
        let existing = state.storage.operational_store().get_ingested_file(&source_key);
        match existing {
            Ok(Some(record)) if record.content_hash == file.content_hash => {
                file_results.push(IngestFileResult {
                    relative_path: file.relative_path.clone(),
                    status: "skipped_unchanged".to_owned(),
                    drawers_written: 0,
                    error: None,
                });
                files_skipped += 1;
                continue;
            }
            Err(_) => {
                file_results.push(IngestFileResult {
                    relative_path: file.relative_path.clone(),
                    status: "failed".to_owned(),
                    drawers_written: 0,
                    error: Some("ingest metadata lookup failed".to_owned()),
                });
                files_failed += 1;
                continue;
            }
            _ => {} // not found or hash mismatch — proceed to ingest
        }

        // ── Validate room ids and build text list for embedding ────────────────
        let mut chunk_rooms: Vec<RoomId> = Vec::with_capacity(file.chunks.len());
        let mut room_error: Option<String> = None;
        for chunk in &file.chunks {
            match RoomId::new(&chunk.room) {
                Ok(room) => chunk_rooms.push(room),
                Err(err) => {
                    room_error = Some(err.to_string());
                    break;
                }
            }
        }
        if let Some(err) = room_error {
            file_results.push(IngestFileResult {
                relative_path: file.relative_path.clone(),
                status: "failed".to_owned(),
                drawers_written: 0,
                error: Some(err),
            });
            files_failed += 1;
            continue;
        }

        // ── Embed chunks ──────────────────────────────────────────────────────
        let texts: Vec<String> = file.chunks.iter().map(|c| c.text.clone()).collect();

        // Determine batch size
        let batch_size = if state.config.low_cpu.enabled {
            state.config.low_cpu.effective_ingest_batch_size()
        } else {
            texts.len().max(1) // all at once
        };

        let mut all_vectors: Vec<Vec<f32>> = Vec::with_capacity(texts.len());
        let mut embed_error: Option<String> = None;

        for batch in texts.chunks(batch_size.max(1)) {
            let request = match EmbeddingRequest::new(batch.to_vec()) {
                Ok(r) => r,
                Err(_) => {
                    embed_error = Some("embedding request is invalid".to_owned());
                    break;
                }
            };
            let response = {
                let mut search = state.search.lock().await;
                search.provider_mut().embed(&request)
            };
            match response {
                Ok(resp) => {
                    all_vectors.extend_from_slice(resp.vectors());
                }
                Err(_) => {
                    embed_error = Some("embedding operation failed".to_owned());
                    break;
                }
            }
        }

        if let Some(err) = embed_error {
            file_results.push(IngestFileResult {
                relative_path: file.relative_path.clone(),
                status: "failed".to_owned(),
                drawers_written: 0,
                error: Some(err),
            });
            files_failed += 1;
            continue;
        }

        // ── Build DrawerRecords ───────────────────────────────────────────────
        let mut drawers: Vec<DrawerRecord> = Vec::with_capacity(file.chunks.len());
        let mut build_error: Option<String> = None;
        let mut built_locator_row = false;

        for (i, chunk) in file.chunks.iter().enumerate() {
            let room = chunk_rooms[i].clone();
            let drawer_id = match mined_drawer_id(&wing, &room, &source_key, chunk.chunk_index) {
                Ok(id) => id,
                Err(err) => {
                    build_error = Some(err.to_string());
                    break;
                }
            };

            let embedding = match all_vectors.get(i) {
                Some(v) => v.clone(),
                None => {
                    build_error =
                        Some(format!("chunk {}: no embedding vector returned", chunk.chunk_index));
                    break;
                }
            };

            let (locator, content) = if let Some(file_hash) = &file.file_hash {
                // Locator path — ranges were validated above to all be Some
                let bs = chunk.byte_start.unwrap_or(0);
                let be = chunk.byte_end.unwrap_or(0);
                let ls = chunk.line_start.unwrap_or(1);
                let le = chunk.line_end.unwrap_or(1);
                built_locator_row = true;
                (
                    Some(SourceLocator {
                        byte_start: bs,
                        byte_end: be,
                        line_start: ls,
                        line_end: le,
                        file_hash: file_hash.clone(),
                        resolve_root: resolve_root.clone(),
                        commit_hash: body.commit_hash.clone(),
                    }),
                    String::new(),
                )
            } else {
                // Content path — store text verbatim (non-UTF-8 / no-ranges fallback)
                (None, chunk.text.clone())
            };

            drawers.push(DrawerRecord {
                id: drawer_id,
                wing: wing.clone(),
                room,
                hall: None,
                date: None,
                source_file: file.relative_path.clone(),
                chunk_index: chunk.chunk_index,
                ingest_mode: "projects".to_owned(),
                extract_mode: None,
                added_by: added_by.clone(),
                filed_at: now,
                importance: None,
                emotional_weight: None,
                weight: None,
                content,
                content_hash: hash_text(&chunk.text),
                embedding,
                locator,
                view_metadata: Some(view_metadata.clone()),
            });
        }

        if let Some(err) = build_error {
            file_results.push(IngestFileResult {
                relative_path: file.relative_path.clone(),
                status: "failed".to_owned(),
                drawers_written: 0,
                error: Some(err),
            });
            files_failed += 1;
            continue;
        }

        // Warn once if we built locator rows but have no resolve_root
        if built_locator_row && resolve_root.is_empty() && !warned_missing_checkout {
            warned_missing_checkout = true;
        }

        // ── Commit ────────────────────────────────────────────────────────────
        let n = drawers.len();
        match state
            .storage
            .replace_source_drawers(
                "projects",
                &source_key,
                &file.relative_path,
                file.content_hash.clone(),
                drawers,
            )
            .await
        {
            Ok(()) => {
                file_results.push(IngestFileResult {
                    relative_path: file.relative_path.clone(),
                    status: "ingested".to_owned(),
                    drawers_written: n,
                    error: None,
                });
                files_ingested += 1;
                total_drawers_written += n;
            }
            Err(_) => {
                file_results.push(IngestFileResult {
                    relative_path: file.relative_path.clone(),
                    status: "failed".to_owned(),
                    drawers_written: 0,
                    error: Some("ingest storage operation failed".to_owned()),
                });
                files_failed += 1;
            }
        }
    }

    // ── Warnings ──────────────────────────────────────────────────────────────
    let mut warnings: Vec<String> = Vec::new();
    if warned_missing_checkout {
        warnings.push(format!(
            "no checkout configured for wing '{wing_str}'; locator results will resolve as \
             stale placeholders until server.checkouts is set"
        ));
    }

    // ── Change event (only when at least one file was ingested) ───────────────
    if files_ingested > 0 {
        state.storage.operational_store().append_event(&ChangeEvent {
            event_type: "mine_batch".to_owned(),
            occurred_at: now,
            entity_id: format!("mine_batch:{wing_str}"),
            actor: Some(identity),
            details_json: Some(
                json!({
                    "repo_id": body.repo_id,
                    "files_ingested": files_ingested,
                    "files_skipped": files_skipped,
                    "files_failed": files_failed,
                    "drawers_written": total_drawers_written,
                    "commit_hash": body.commit_hash,
                })
                .to_string(),
            ),
        })?;
    }

    Ok(Json(IngestBatchResponse { files: file_results, warnings }))
}

// ─── Coordination (issue #102 Stage 3) ─────────────────────────────────────────
//
// Server-side only: exposes the local `CoordinationStore` (tasks, messages,
// artifacts, results, audit events) over the existing federation HTTP surface
// and its scoped-token authorization layer. The client side (`RemoteApi`,
// `FederationRouter`, MCP routing) is Stage 4 — see
// docs/Coordination-Phase-3-Design.md.
//
// Wing authorization. A task's wing is the authorization key for every
// coordination route. Task creation gets its wing from the request body
// (Group A: enforced outright, 403 on mismatch — mirrors `route_drawers_add`).
// Every other route resolves the wing from the target record before
// authorizing: a message, artifact, or result reaches its wing through its
// `task_id`; claim/renew/transition act on the task directly. A caller who
// lacks access to that wing gets 404, not 403 (Group B — mirrors
// `route_drawers_get`), so the response can never be used as an existence
// oracle for a wing the caller cannot see; see `resolve_owning_task`. The
// event feed is an aggregate (Group C): it filters to visible wings rather
// than rejecting, exactly like `route_changes`/`route_rooms`, and — because
// `coordination_events.wing` is a mandatory column, always populated even for
// pre-Phase-3 rows (`UNSCOPED_WING`) — every event has a determinable wing, so
// "fail closed" here reduces to the ordinary `WingVisibility::contains` check
// with no separate wingless-event allowlist needed (contrast
// `change_event_visible`, which does need one for the generic changes feed).
// The inbox feed is treated the same way by extension, since it is equally
// cross-task/cross-wing in shape, even though the design note calls out only
// the event feed by name.
//
// Actor identity. Storage accepts a caller-supplied actor string, which is
// fine locally where the host runtime asserts it. Over HTTP the authenticated
// token identity is authoritative: every actor-shaped field on a coordination
// write DTO (`created_by`, `sender`, `worker`) is the caller's *claimed*
// name, and `resolve_coordination_actor` applies the same
// `{identity}:{claimed}` prefixing rule `route_drawers_add` uses for
// `added_by`. `recipient` on a message is not an actor field — it addresses a
// message to someone and does not itself assert who the caller is — so it is
// taken verbatim, matching local behaviour.
//
// The acknowledging `actor` on `POST /v1/coordination/messages/{id}/ack` is a
// special case of the same rule, not an exception to it: storage requires it
// to equal the message's `recipient` **exactly** (`ONLY_RECIPIENT_MAY_ACKNOWLEDGE`),
// and `recipient` is itself stored verbatim/unauthenticated (see above). Running
// the claimed ack actor through the identity-prefixing rule therefore made every
// federated acknowledgement fail unless the remote token's identity happened to
// equal the message's recipient, since `{identity}:{claimed}` can never equal a
// recipient stored without that prefix. `resolve_ack_actor` fixes this by using
// the claim verbatim only when it exactly matches the message's own recipient —
// proving you know the (unauthenticated) address a message was already sent to,
// which is no stronger a claim than the sender who chose that recipient string
// in the first place. Any other claimed value — one that is neither the caller's
// own identity nor the message's recipient — still goes through
// `resolve_coordination_actor`'s prefixing, so a caller can never cause an
// unrelated identity's bare name to be recorded or matched: it either gets
// prefixed (and then correctly fails the recipient-equality check), or it was
// already the recipient to begin with.
//
// Lease clocks. Expiry is evaluated entirely inside `CoordinationStore` using
// `OffsetDateTime::now_utc()` — this palace's own clock. No route here
// accepts or forwards a caller-supplied timestamp for a lease or expiry
// decision.

/// Resolves the task owning a coordination sub-resource and authorizes `op`
/// against its wing. On either a missing task or an invisible wing, returns
/// 404 built from `resource_label` (e.g. `"message {id}"`, not `"task
/// {task_id}"`), so the response can never distinguish "truly missing" from
/// "wing not visible" — see the module-level "Wing authorization" note. Used
/// both when `task_id` names the resource being requested directly (task
/// routes) and when it names the *owning* task of a message/artifact/result.
fn resolve_owning_task(
    coordination: &CoordinationStore,
    auth: &AuthIdentity,
    task_id: &str,
    op: Operation,
    resource_label: &str,
) -> Result<CoordinationTask, ServerError> {
    let mask = || ServerError::NotFound(format!("{resource_label} not found"));
    let task = coordination.get_task(task_id)?.ok_or_else(mask)?;
    // The diary hard-override applies unconditionally, exactly as it does for
    // drawer routes: `wing_agents` coordination stays local no matter what
    // the token is scoped to. A read is masked as 404 — indistinguishable
    // from "does not exist", matching every other invisible-wing case this
    // function handles. A write gets the explicit `DiaryNotFederated` 422
    // `route_drawers_add` already uses for the same content rule; a write is
    // not an existence-oracle risk the way a differently-coded read would be,
    // so there is no reason to mask it instead.
    if is_diary_wing_or_room(&task.wing, "") {
        return Err(if op == Operation::CoordinationRead { mask() } else { ServerError::DiaryNotFederated });
    }
    if !auth.allows_wing(op, &task.wing) {
        return Err(mask());
    }
    Ok(task)
}

/// Fixed, generic conflict body for an idempotent coordination write whose
/// *replayed* record fails re-authorization — see [`authorize_replay_wing`].
/// Deliberately names neither the wing nor any record content: the message
/// text is the entire disclosure surface here, so it must stay identical
/// regardless of what actually went wrong.
fn coordination_replay_conflict() -> ServerError {
    ServerError::CoordinationConflict {
        code: "idempotency_key_conflict",
        message: "idempotency key is already associated with a record this token cannot access"
            .to_owned(),
        expected_revision: None,
        actual_revision: None,
    }
}

/// Re-authorizes the *returned* record of an idempotent coordination write
/// (task create, message send, artifact put, result put) against its actual
/// owning `wing`.
///
/// Storage's `find_*_by_key` replay lookup is keyed on `(actor,
/// idempotency_key)` alone — it ignores whatever task/wing the replay
/// request named. So the record handed back can belong to a wing the caller
/// cannot access even though the pre-write check on the *requested* task
/// passed. This call closes that gap by checking the wing storage actually
/// used, after the fact.
///
/// An unauthorized wing here is reported as a 409 conflict via
/// [`coordination_replay_conflict`], never a 404: the idempotency key is
/// scoped to the caller's own identity, so acknowledging that the key
/// already exists discloses nothing about another tenant — but the record's
/// wing and content must not be disclosed, so the message stays fixed and
/// generic. This is not a 404 because, unlike a masked read, a create
/// request that reaches this point genuinely conflicts with prior state; see
/// the module-level "Wing authorization" note for why reads and writes are
/// coded differently.
fn authorize_replay_wing(auth: &AuthIdentity, wing: &str) -> Result<(), ServerError> {
    if is_diary_wing_or_room(wing, "") || !auth.allows_wing(Operation::CoordinationWrite, wing) {
        return Err(coordination_replay_conflict());
    }
    Ok(())
}

/// Looks up the wing of the task owning a just-written message, artifact, or
/// result, for [`authorize_replay_wing`]. Messages, artifacts, and results
/// carry no `wing` column of their own — see docs/Coordination.md — so this
/// re-resolves it from their mandatory `task_id`. The owning task is
/// guaranteed to exist by the foreign key `coordination.rs`'s schema
/// declares; a missing task here would mean that invariant broke, which
/// surfaces as an ordinary 500 rather than being silently swallowed.
fn owning_task_wing(coordination: &CoordinationStore, task_id: &str) -> Result<String, ServerError> {
    Ok(coordination
        .get_task(task_id)?
        .ok_or_else(|| {
            ServerError::Storage(mempalace_storage::StorageError::Invariant(format!(
                "task `{task_id}` not found"
            )))
        })?
        .wing)
}

/// Resolves the actor to record for a coordination write. `claimed` is the
/// value the caller supplied for `created_by`/`sender`/`worker`/`actor` on the
/// wire; `identity` is the authenticated token name. See the module-level
/// "Actor identity" note and `route_drawers_add`'s identical `added_by` rule.
///
/// Rejects a `claimed` value containing `:` with 400: the `{identity}:{claim}`
/// encoding below is unambiguous only when neither half can itself contain
/// the delimiter. `identity` never needs the same check — `TokenRegistry`
/// already rejects a `:` in a token's configured `name` at load time, so it
/// cannot arrive here.
fn resolve_coordination_actor(
    identity: &str,
    claimed: &Option<String>,
) -> Result<String, ServerError> {
    match claimed {
        Some(claim) if claim != identity => {
            if claim.contains(':') {
                return Err(ServerError::InvalidParams(
                    "claimed actor must not contain `:`".to_owned(),
                ));
            }
            Ok(format!("{identity}:{claim}"))
        }
        _ => Ok(identity.to_owned()),
    }
}

/// Resolves the acknowledging actor for `POST /v1/coordination/messages/{id}/ack`.
/// See the module-level "Actor identity" note for why this cannot reuse
/// [`resolve_coordination_actor`] unconditionally: storage requires the final
/// actor to equal `recipient` exactly, and `recipient` is stored verbatim, so
/// a claim that already *is* the recipient is used as-is. Any other claim —
/// including one that is simply the caller's own token identity — still goes
/// through the ordinary prefixing rule, so a claim naming an unrelated
/// identity can never be recorded bare.
fn resolve_ack_actor(
    identity: &str,
    claimed: &Option<String>,
    recipient: &str,
) -> Result<String, ServerError> {
    match claimed {
        Some(claim) if claim == recipient => Ok(claim.clone()),
        _ => resolve_coordination_actor(identity, claimed),
    }
}

/// Rejects an obviously out-of-range `lease_seconds` with a clean 400 before
/// the request ever reaches storage. `mempalace-storage`'s `claim_task` and
/// `renew_lease` already reject a TTL that would overflow `OffsetDateTime`
/// arithmetic (`LEASE_DURATION_OUT_OF_RANGE`), but that guard alone still
/// requires storage to compute the failing addition; this route-level bound
/// rejects the request outright instead of depending on that lower layer.
fn validate_lease_seconds(seconds: i64) -> Result<(), ServerError> {
    if seconds <= 0 || seconds > MAX_LEASE_SECONDS {
        return Err(ServerError::InvalidParams(format!(
            "lease_seconds must be in 1..={MAX_LEASE_SECONDS}"
        )));
    }
    Ok(())
}

/// Classifies a `StorageError` from a coordination-store write into the right
/// HTTP shape. As of Phase 3 Stage 4 a stale revision is no longer part of
/// this function's job — `claim_task`/`renew_lease`/`transition_task` report
/// it as a typed `RevisionedWrite::Conflict`, handled directly by their route
/// handlers via `coordination_revision_conflict`. Everything this function
/// still sees is a genuine `StorageError`: either a lease/state/ownership
/// conflict `coordination.rs` raises as `StorageError::Invariant(String)`
/// built from one of its `pub const` message fragments, or some other error
/// entirely. The `pub const` coupling is compile-enforced, not textual: every
/// fragment matched below is re-exported from `mempalace_storage::coordination`,
/// which builds its own error text from the same constant (see that module's
/// doc comment on them, and `error_messages_start_with_their_constants` in
/// its test module). Renaming or removing one of those constants is a
/// compile error here; rewording the text it holds moves both sides
/// together, because the text exists in exactly one place. Every branch
/// below corresponds to one specific message shape coordination.rs actually
/// emits; an `Invariant` this function does not recognise, or any
/// non-`Invariant` error, falls through to the ordinary `ServerError::Storage`
/// 500 mapping.
fn coordination_storage_error(err: mempalace_storage::StorageError) -> ServerError {
    use mempalace_storage::{
        INVALID_TRANSITION_PREFIX, LEASE_HAS_EXPIRED, LEASE_HELD_BY_ANOTHER_WORKER,
        NOT_FOUND_SUFFIX, ONLY_LEASE_OWNER_MAY_RENEW, ONLY_OWNER_MAY_TRANSITION,
        ONLY_RECIPIENT_MAY_ACKNOWLEDGE, TASK_HAS_EXPIRED, TERMINAL_TASK_CANNOT_BE_CLAIMED,
    };

    let mempalace_storage::StorageError::Invariant(msg) = &err else {
        return ServerError::Storage(err);
    };
    // State/ownership/lease rules `coordination.rs` enforces once the
    // requested revision itself matched — a real conflict with the record's
    // current state, distinct from a stale revision, so it carries no
    // revision pair on the wire.
    const CONFLICT_PREFIXES: &[&str] = &[
        LEASE_HELD_BY_ANOTHER_WORKER,
        TERMINAL_TASK_CANNOT_BE_CLAIMED,
        TASK_HAS_EXPIRED,
        INVALID_TRANSITION_PREFIX,
        ONLY_OWNER_MAY_TRANSITION,
        ONLY_LEASE_OWNER_MAY_RENEW,
        LEASE_HAS_EXPIRED,
        ONLY_RECIPIENT_MAY_ACKNOWLEDGE,
    ];
    if CONFLICT_PREFIXES.iter().any(|prefix| msg.starts_with(prefix)) {
        return ServerError::CoordinationConflict {
            code: "coordination_conflict",
            message: msg.clone(),
            expected_revision: None,
            actual_revision: None,
        };
    }
    // Defensive: every route resolves its target task (and, for a
    // message/artifact/result write, the owning task) before calling into
    // storage, so `require_task`'s "not found" should never actually surface
    // here — this only guards the residual race where the row disappears
    // between that check and the write's own transaction. Matched against the
    // pinned `NOT_FOUND_SUFFIX` constant, not a bare `"not found"` literal, for the same reason
    // the conflict prefixes above are pinned: a rewording of the underlying message must be a
    // compile error here, not a silent reclassification of a 404 into a 400.
    if msg.contains(NOT_FOUND_SUFFIX) {
        return ServerError::NotFound(msg.clone());
    }
    // Everything else `coordination.rs` raises as `Invariant` is caller input
    // validation: empty actor, out-of-range idempotency key, oversized
    // title/description/payload/artifact content, or a non-positive lease
    // ttl.
    ServerError::InvalidParams(msg.clone())
}

/// Builds the `409 revision_conflict` response for a typed
/// `RevisionedWrite::Conflict` returned by `claim_task`/`renew_lease`/
/// `transition_task`. `actual_revision` is `None` only for the residual
/// lost-CAS-race case those methods guard defensively (the row changed
/// between the revision check and the write, inside the same transaction —
/// should not happen in practice); the response still carries
/// `expected_revision` so the caller knows what it asked for.
fn coordination_revision_conflict(
    expected_revision: i64,
    actual_revision: Option<i64>,
) -> ServerError {
    let message = match actual_revision {
        Some(actual) => format!("stale revision: expected {expected_revision}, current {actual}"),
        None => format!(
            "stale revision: expected {expected_revision}, but the record changed during the write"
        ),
    };
    ServerError::CoordinationConflict {
        code: "revision_conflict",
        message,
        expected_revision: Some(expected_revision),
        actual_revision,
    }
}

/// Encodes a `CoordinationCursor` as an opaque wire string. Deliberately not
/// the `"{rfc3339}|{rowid}"` shape `encode_cursor` uses for `/v1/changes` —
/// the coordination feed has no `since` parameter, so a cursor here depends on
/// no clock at all (see the coordination-DTO docs in `mempalace-federation`).
/// Clients must treat this as opaque and pass it back verbatim.
fn encode_coordination_cursor(cursor: CoordinationCursor) -> String {
    cursor.0.to_string()
}

/// Decodes a cursor encoded by [`encode_coordination_cursor`].
fn decode_coordination_cursor(s: &str) -> Result<CoordinationCursor, ServerError> {
    s.trim()
        .parse::<i64>()
        .map(CoordinationCursor)
        .map_err(|_| ServerError::InvalidParams(format!("invalid cursor `{s}`")))
}

/// Parses a full RFC 3339 timestamp (unlike `parse_date`, which only handles
/// the date part).
fn parse_rfc3339_datetime(s: &str) -> Result<OffsetDateTime, ServerError> {
    OffsetDateTime::parse(s, &Rfc3339)
        .map_err(|_| ServerError::InvalidParams(format!("invalid RFC 3339 timestamp `{s}`")))
}

fn wire_task_state(state: TaskState) -> CoordinationTaskState {
    match state {
        TaskState::Pending => CoordinationTaskState::Pending,
        TaskState::Running => CoordinationTaskState::Running,
        TaskState::InputRequired => CoordinationTaskState::InputRequired,
        TaskState::Completed => CoordinationTaskState::Completed,
        TaskState::Cancelled => CoordinationTaskState::Cancelled,
        TaskState::Failed => CoordinationTaskState::Failed,
        TaskState::Expired => CoordinationTaskState::Expired,
    }
}

fn storage_task_state(state: CoordinationTaskState) -> TaskState {
    match state {
        CoordinationTaskState::Pending => TaskState::Pending,
        CoordinationTaskState::Running => TaskState::Running,
        CoordinationTaskState::InputRequired => TaskState::InputRequired,
        CoordinationTaskState::Completed => TaskState::Completed,
        CoordinationTaskState::Cancelled => TaskState::Cancelled,
        CoordinationTaskState::Failed => TaskState::Failed,
        CoordinationTaskState::Expired => TaskState::Expired,
    }
}

fn task_to_dto(task: CoordinationTask) -> Result<CoordinationTaskDto, ServerError> {
    Ok(CoordinationTaskDto {
        task_id: task.task_id,
        title: task.title,
        description: task.description,
        state: wire_task_state(task.state),
        revision: task.revision,
        created_by: task.created_by,
        wing: task.wing,
        owner: task.owner,
        parent_id: task.parent_id,
        dependencies: task.dependencies,
        budget: task.budget,
        lease_expires_at: task.lease_expires_at.map(format_rfc3339).transpose()?,
        expires_at: task.expires_at.map(format_rfc3339).transpose()?,
        created_at: format_rfc3339(task.created_at)?,
        updated_at: format_rfc3339(task.updated_at)?,
    })
}

fn message_to_dto(message: CoordinationMessage) -> Result<CoordinationMessageDto, ServerError> {
    Ok(CoordinationMessageDto {
        message_id: message.message_id,
        sequence: message.sequence,
        task_id: message.task_id,
        sender: message.sender,
        recipient: message.recipient,
        kind: message.kind,
        payload: message.payload,
        envelope_version: message.envelope_version,
        acknowledged_at: message.acknowledged_at.map(format_rfc3339).transpose()?,
        acknowledged_by: message.acknowledged_by,
        created_at: format_rfc3339(message.created_at)?,
    })
}

fn artifact_to_dto(
    artifact: CoordinationArtifact,
) -> Result<CoordinationArtifactDto, ServerError> {
    Ok(CoordinationArtifactDto {
        artifact_id: artifact.artifact_id,
        task_id: artifact.task_id,
        created_by: artifact.created_by,
        role: artifact.role,
        media_type: artifact.media_type,
        content: artifact.content,
        content_hash: artifact.content_hash,
        created_at: format_rfc3339(artifact.created_at)?,
    })
}

fn result_to_dto(result: CoordinationTaskResult) -> Result<CoordinationTaskResultDto, ServerError> {
    Ok(CoordinationTaskResultDto {
        result_id: result.result_id,
        task_id: result.task_id,
        created_by: result.created_by,
        payload: result.payload,
        created_at: format_rfc3339(result.created_at)?,
    })
}

fn event_to_dto(event: CoordinationEvent) -> Result<CoordinationEventDto, ServerError> {
    Ok(CoordinationEventDto {
        sequence: event.sequence,
        event_id: event.event_id,
        entity_type: event.entity_type,
        entity_id: event.entity_id,
        task_id: event.task_id,
        wing: event.wing,
        event_type: event.event_type,
        actor: event.actor,
        from_state: event.from_state.map(wire_task_state),
        to_state: event.to_state.map(wire_task_state),
        revision: event.revision,
        details: event.details,
        occurred_at: format_rfc3339(event.occurred_at)?,
    })
}

// ─── Coordination: tasks ────────────────────────────────────────────────────

async fn route_coordination_task_create<P>(
    State(state): State<Arc<ServerState<P>>>,
    auth: axum::extract::Extension<AuthIdentity>,
    Json(body): Json<NewTaskRequest>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    // Group A: wing is required and body-derived, exactly like
    // `route_drawers_add` — but normalised via `WingId::normalized` (not the
    // strict `WingId::new` drawers use), because coordination wings support
    // short forms by design (Stage 1: `myproject` and `wing_myproject` are
    // the same wing) and `CoordinationStore::create_task` normalises the same
    // way internally. Authorizing against the normalised form is what keeps
    // this check and the actually-stored wing in agreement.
    let wing = WingId::normalized(&body.wing)?;
    // Reject diary-shaped writes before anything else, exactly like
    // `route_drawers_add`'s content rule: `wing_agents` coordination stays
    // local unconditionally, regardless of what the token is scoped to.
    if is_diary_wing_or_room(wing.as_str(), "") {
        return Err(ServerError::DiaryNotFederated);
    }
    if !auth.0.allows_wing(Operation::CoordinationWrite, wing.as_str()) {
        return Err(ServerError::Forbidden);
    }
    // Authorize every referenced task *before* creation. Storage's own
    // `require_task` on each dependency/parent would otherwise let a token
    // that may only write `wing` probe for hidden ids in another wing: a
    // real id succeeds, a nonexistent one 404s, and that difference is an
    // existence oracle for wings this token cannot read. Masking an
    // unauthorized reference exactly as a missing one closes it.
    for dependency in &body.dependencies {
        resolve_owning_task(
            &state.coordination,
            &auth.0,
            dependency,
            Operation::CoordinationRead,
            &format!("task {dependency}"),
        )?;
    }
    if let Some(parent) = &body.parent_id {
        resolve_owning_task(
            &state.coordination,
            &auth.0,
            parent,
            Operation::CoordinationRead,
            &format!("task {parent}"),
        )?;
    }
    let created_by = resolve_coordination_actor(&auth.0.0, &body.created_by)?;
    let expires_at = body.expires_at.as_deref().map(parse_rfc3339_datetime).transpose()?;
    let input = NewTask {
        title: body.title,
        description: body.description,
        created_by,
        wing: wing.as_str().to_owned(),
        idempotency_key: body.idempotency_key,
        parent_id: body.parent_id,
        dependencies: body.dependencies,
        budget: body.budget,
        expires_at,
    };
    let task = state.coordination.create_task(&input).map_err(coordination_storage_error)?;
    // An idempotency-key replay returns whatever task storage originally
    // created for `(created_by, idempotency_key)`, regardless of the wing
    // this request named — re-authorize the wing storage actually used.
    authorize_replay_wing(&auth.0, &task.wing)?;
    Ok(Json(task_to_dto(task)?))
}

async fn route_coordination_task_get<P>(
    State(state): State<Arc<ServerState<P>>>,
    auth: axum::extract::Extension<AuthIdentity>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let task = resolve_owning_task(
        &state.coordination,
        &auth.0,
        &id,
        Operation::CoordinationRead,
        &format!("task {id}"),
    )?;
    Ok(Json(task_to_dto(task)?))
}

async fn route_coordination_task_claim<P>(
    State(state): State<Arc<ServerState<P>>>,
    auth: axum::extract::Extension<AuthIdentity>,
    Path(id): Path<String>,
    Json(body): Json<TaskLeaseRequest>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    resolve_owning_task(
        &state.coordination,
        &auth.0,
        &id,
        Operation::CoordinationClaim,
        &format!("task {id}"),
    )?;
    validate_lease_seconds(body.lease_seconds)?;
    let worker = resolve_coordination_actor(&auth.0.0, &body.worker)?;
    let expected_revision = body.expected_revision;
    let write = state
        .coordination
        .claim_task(&id, &worker, expected_revision, time::Duration::seconds(body.lease_seconds))
        .map_err(coordination_storage_error)?;
    match write {
        RevisionedWrite::Applied(task) => Ok(Json(task_to_dto(task)?)),
        RevisionedWrite::Conflict { actual_revision } => {
            Err(coordination_revision_conflict(expected_revision, actual_revision))
        }
    }
}

async fn route_coordination_task_renew<P>(
    State(state): State<Arc<ServerState<P>>>,
    auth: axum::extract::Extension<AuthIdentity>,
    Path(id): Path<String>,
    Json(body): Json<TaskLeaseRequest>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    resolve_owning_task(
        &state.coordination,
        &auth.0,
        &id,
        Operation::CoordinationClaim,
        &format!("task {id}"),
    )?;
    validate_lease_seconds(body.lease_seconds)?;
    let worker = resolve_coordination_actor(&auth.0.0, &body.worker)?;
    let expected_revision = body.expected_revision;
    let write = state
        .coordination
        .renew_lease(&id, &worker, expected_revision, time::Duration::seconds(body.lease_seconds))
        .map_err(coordination_storage_error)?;
    match write {
        RevisionedWrite::Applied(task) => Ok(Json(task_to_dto(task)?)),
        RevisionedWrite::Conflict { actual_revision } => {
            Err(coordination_revision_conflict(expected_revision, actual_revision))
        }
    }
}

async fn route_coordination_task_transition<P>(
    State(state): State<Arc<ServerState<P>>>,
    auth: axum::extract::Extension<AuthIdentity>,
    Path(id): Path<String>,
    Json(body): Json<TransitionTaskRequest>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    resolve_owning_task(
        &state.coordination,
        &auth.0,
        &id,
        Operation::CoordinationClaim,
        &format!("task {id}"),
    )?;
    let actor = resolve_coordination_actor(&auth.0.0, &body.actor)?;
    let expected_revision = body.expected_revision;
    let write = state
        .coordination
        .transition_task(
            &id,
            &actor,
            expected_revision,
            storage_task_state(body.state),
            body.details,
        )
        .map_err(coordination_storage_error)?;
    match write {
        RevisionedWrite::Applied(task) => Ok(Json(task_to_dto(task)?)),
        RevisionedWrite::Conflict { actual_revision } => {
            Err(coordination_revision_conflict(expected_revision, actual_revision))
        }
    }
}

// ─── Coordination: messages ─────────────────────────────────────────────────

async fn route_coordination_message_send<P>(
    State(state): State<Arc<ServerState<P>>>,
    auth: axum::extract::Extension<AuthIdentity>,
    Json(body): Json<NewMessageRequest>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    resolve_owning_task(
        &state.coordination,
        &auth.0,
        &body.task_id,
        Operation::CoordinationWrite,
        &format!("task {}", body.task_id),
    )?;
    let sender = resolve_coordination_actor(&auth.0.0, &body.sender)?;
    let input = NewMessage {
        task_id: body.task_id,
        sender,
        recipient: body.recipient,
        kind: body.kind,
        payload: body.payload,
        idempotency_key: body.idempotency_key,
        envelope_version: body.envelope_version,
    };
    let message = state.coordination.send_message(&input).map_err(coordination_storage_error)?;
    // An idempotency-key replay returns whatever message storage originally
    // created for `(sender, idempotency_key)`, on whatever task that was —
    // possibly not `body.task_id`. Re-authorize the wing storage actually
    // used.
    authorize_replay_wing(&auth.0, &owning_task_wing(&state.coordination, &message.task_id)?)?;
    Ok(Json(message_to_dto(message)?))
}

async fn route_coordination_message_get<P>(
    State(state): State<Arc<ServerState<P>>>,
    auth: axum::extract::Extension<AuthIdentity>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let message = state
        .coordination
        .get_message(&id)?
        .ok_or_else(|| ServerError::NotFound(format!("message {id} not found")))?;
    resolve_owning_task(
        &state.coordination,
        &auth.0,
        &message.task_id,
        Operation::CoordinationRead,
        &format!("message {id}"),
    )?;
    Ok(Json(message_to_dto(message)?))
}

async fn route_coordination_message_ack<P>(
    State(state): State<Arc<ServerState<P>>>,
    auth: axum::extract::Extension<AuthIdentity>,
    Path(id): Path<String>,
    Json(body): Json<AckMessageRequest>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let message = state
        .coordination
        .get_message(&id)?
        .ok_or_else(|| ServerError::NotFound(format!("message {id} not found")))?;
    resolve_owning_task(
        &state.coordination,
        &auth.0,
        &message.task_id,
        Operation::CoordinationWrite,
        &format!("message {id}"),
    )?;
    let actor = resolve_ack_actor(&auth.0.0, &body.actor, &message.recipient)?;
    let acknowledged =
        state.coordination.acknowledge_message(&id, &actor).map_err(coordination_storage_error)?;
    Ok(Json(message_to_dto(acknowledged)?))
}

async fn route_coordination_inbox<P>(
    State(state): State<Arc<ServerState<P>>>,
    auth: axum::extract::Extension<AuthIdentity>,
    Query(params): Query<InboxQuery>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let limit = params.limit.unwrap_or(DEFAULT_PAGE_LIMIT).max(1).min(MAX_PAGE_LIMIT);
    let cursor = params.cursor.as_deref().map(decode_coordination_cursor).transpose()?;

    if let Some(wing_raw) = &params.wing {
        let wing = WingId::normalized(wing_raw)?;
        // Group C, not Group A: the inbox is an aggregate feed spanning many
        // tasks/wings even with an explicit wing filter — mirrors
        // `route_rooms`'s handling of its optional `?wing=`, not
        // `route_drawers_list`/`route_drawers_search`'s. A mismatched wing
        // filters to an empty page rather than 403. All rows returned by a
        // wing-filtered storage query share that one wing, so a single
        // visibility check covers the whole page. The diary hard-override
        // (`wing_agents` never federates) is folded into the same check:
        // an explicit filter of `wing_agents` is indistinguishable from one
        // the token simply cannot see.
        if is_diary_wing_or_room(wing.as_str(), "")
            || !auth.0.allows_wing(Operation::CoordinationRead, wing.as_str())
        {
            return Ok(Json(InboxPageResponse { messages: Vec::new(), next_cursor: None }));
        }
        let page = state
            .coordination
            .inbox(
                &params.recipient,
                cursor,
                Some(wing.as_str()),
                limit,
                params.unacknowledged_only,
                // The wing above is already confirmed visible and non-diary by the check just
                // above, so no further restriction is needed here; `Federated(None)` still
                // carries the diary exclusion, which is redundant in this branch but keeps every
                // federation call site going through the same "always excludes diary" path.
                CoordinationVisibility::Federated(None),
            )
            .map_err(coordination_storage_error)?;
        let messages =
            page.messages.into_iter().map(message_to_dto).collect::<Result<Vec<_>, _>>()?;
        return Ok(Json(InboxPageResponse {
            messages,
            next_cursor: page.next_cursor.map(encode_coordination_cursor),
        }));
    }

    // No wing filter: cross-wing. Visibility (including the diary
    // hard-override) is now enforced inside the storage query itself via
    // `CoordinationVisibility`, so `next_cursor` is computed only over rows
    // this caller may see — an invisible row can no longer influence it,
    // which was the vulnerability this replaced. That also means the
    // over-fetch/post-filter loop that used to live here (tracking
    // `last_examined_sequence` to avoid resuming past an unexamined
    // visible message — see `coordination_inbox_cursor_does_not_skip_the_second_visible_message`)
    // is no longer needed: storage's own `has_more`/cursor boundary is
    // already computed over the filtered set, so "examined everything" and
    // "storage's page boundary" always agree now.
    let scope = CoordinationReadScope::resolve(&auth.0);
    let page = state
        .coordination
        .inbox(
            &params.recipient,
            cursor,
            None,
            limit,
            params.unacknowledged_only,
            scope.visibility(),
        )
        .map_err(coordination_storage_error)?;
    let messages = page.messages.into_iter().map(message_to_dto).collect::<Result<Vec<_>, _>>()?;
    let next_cursor = page.next_cursor.map(encode_coordination_cursor);
    Ok(Json(InboxPageResponse { messages, next_cursor }))
}

// ─── Coordination: artifacts ────────────────────────────────────────────────

async fn route_coordination_artifact_put<P>(
    State(state): State<Arc<ServerState<P>>>,
    auth: axum::extract::Extension<AuthIdentity>,
    Json(body): Json<NewArtifactRequest>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    resolve_owning_task(
        &state.coordination,
        &auth.0,
        &body.task_id,
        Operation::CoordinationWrite,
        &format!("task {}", body.task_id),
    )?;
    let created_by = resolve_coordination_actor(&auth.0.0, &body.created_by)?;
    let input = NewArtifact {
        task_id: body.task_id,
        created_by,
        role: body.role,
        media_type: body.media_type,
        content: body.content,
        idempotency_key: body.idempotency_key,
    };
    let artifact = state.coordination.put_artifact(&input).map_err(coordination_storage_error)?;
    // See the identical note in `route_coordination_message_send`: a replay
    // can return an artifact belonging to a different, unauthorized wing.
    authorize_replay_wing(&auth.0, &owning_task_wing(&state.coordination, &artifact.task_id)?)?;
    Ok(Json(artifact_to_dto(artifact)?))
}

async fn route_coordination_artifact_get<P>(
    State(state): State<Arc<ServerState<P>>>,
    auth: axum::extract::Extension<AuthIdentity>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let artifact = state
        .coordination
        .get_artifact(&id)?
        .ok_or_else(|| ServerError::NotFound(format!("artifact {id} not found")))?;
    resolve_owning_task(
        &state.coordination,
        &auth.0,
        &artifact.task_id,
        Operation::CoordinationRead,
        &format!("artifact {id}"),
    )?;
    Ok(Json(artifact_to_dto(artifact)?))
}

// ─── Coordination: results ──────────────────────────────────────────────────

async fn route_coordination_result_put<P>(
    State(state): State<Arc<ServerState<P>>>,
    auth: axum::extract::Extension<AuthIdentity>,
    Json(body): Json<NewTaskResultRequest>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    resolve_owning_task(
        &state.coordination,
        &auth.0,
        &body.task_id,
        Operation::CoordinationWrite,
        &format!("task {}", body.task_id),
    )?;
    let created_by = resolve_coordination_actor(&auth.0.0, &body.created_by)?;
    let input = NewTaskResult {
        task_id: body.task_id,
        created_by,
        payload: body.payload,
        idempotency_key: body.idempotency_key,
    };
    let result = state.coordination.put_result(&input).map_err(coordination_storage_error)?;
    // See the identical note in `route_coordination_message_send`: a replay
    // can return a result belonging to a different, unauthorized wing.
    authorize_replay_wing(&auth.0, &owning_task_wing(&state.coordination, &result.task_id)?)?;
    Ok(Json(result_to_dto(result)?))
}

async fn route_coordination_result_get<P>(
    State(state): State<Arc<ServerState<P>>>,
    auth: axum::extract::Extension<AuthIdentity>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let result = state
        .coordination
        .get_result(&id)?
        .ok_or_else(|| ServerError::NotFound(format!("result {id} not found")))?;
    resolve_owning_task(
        &state.coordination,
        &auth.0,
        &result.task_id,
        Operation::CoordinationRead,
        &format!("result {id}"),
    )?;
    Ok(Json(result_to_dto(result)?))
}

// ─── Coordination: events ───────────────────────────────────────────────────

async fn route_coordination_events<P>(
    State(state): State<Arc<ServerState<P>>>,
    auth: axum::extract::Extension<AuthIdentity>,
    Query(params): Query<CoordinationEventsQuery>,
) -> Result<impl IntoResponse, ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let limit = params.limit.unwrap_or(DEFAULT_PAGE_LIMIT).max(1).min(MAX_PAGE_LIMIT);
    let cursor = params.cursor.as_deref().map(decode_coordination_cursor).transpose()?;
    let wing = params.wing.as_deref().map(WingId::normalized).transpose()?;

    // Group C, matching `route_changes`/`route_rooms`: require read (enforced
    // by the per-route gate before this handler runs), then filter to the
    // wings the token can see rather than reject — including when an
    // explicit `wing` filter is given but not visible, which yields an empty
    // page, not 403. See the module-level "Wing authorization" note for why
    // this never needs a wingless-event allowlist the way `/v1/changes` does.
    //
    // The filter (including the diary hard-override) is enforced inside the
    // storage query itself via `CoordinationVisibility`, not by filtering the
    // already-`LIMIT`-bounded result afterwards: a post-fetch filter still
    // lets `next_cursor` — derived from unfiltered rows — leak the existence
    // and volume of records in wings this caller cannot see. Pushing `wing IN
    // (...)` (and the diary exclusion) into the query, mirroring
    // `route_drawers_list`'s `DrawerFilter::wings`, means an invisible row
    // never reaches the LIMIT/cursor computation in the first place. An
    // explicit `?wing=` naming an invisible or diary wing still yields an
    // empty page here, not a 403 or an error: it intersects with an empty (or
    // excluded) set and the query simply returns nothing.
    let scope = CoordinationReadScope::resolve(&auth.0);

    let page = state.coordination.events(
        cursor,
        params.task_id.as_deref(),
        wing.as_ref().map(WingId::as_str),
        limit,
        scope.visibility(),
    )?;
    let next_cursor = page.next_cursor.map(encode_coordination_cursor);
    let events = page.events.into_iter().map(event_to_dto).collect::<Result<Vec<_>, _>>()?;
    Ok(Json(CoordinationEventsResponse { events, next_cursor }))
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Returns `true` when the wing or room identifies diary content that must not
/// be exposed via the federation API.
fn is_diary_wing_or_room(wing: &str, room: &str) -> bool {
    wing == SHARED_AGENT_DIARY_WING || room == DIARY_ROOM
}

/// Reads a mapped checkout file only when the resolved path remains under the
/// mapped checkout root.
fn read_checkout_file_for_locator(root: &FsPath, relative_path: &str) -> Result<Vec<u8>, String> {
    let root = root.canonicalize().map_err(|_| "checkout root is not readable".to_owned())?;
    let path = root
        .join(relative_path)
        .canonicalize()
        .map_err(|_| "checkout file is not readable".to_owned())?;
    if !path.starts_with(&root) {
        return Err("checkout file escapes configured root".to_owned());
    }
    std::fs::read(path).map_err(|_| "checkout file is not readable".to_owned())
}

/// Returns `Some(error)` when `relative_path` is not a safe repo-relative path
/// for batch ingest. The path is later joined onto the server's checkout root
/// during locator resolution, so absolute paths, drive letters, backslashes,
/// traversal, VCS metadata, and secret env files must all be rejected.
fn invalid_ingest_relative_path(path: &str) -> Option<String> {
    if path.is_empty() {
        return Some("relative_path must not be empty".to_owned());
    }
    if path.starts_with('/') || path.contains('\\') || path.contains(':') {
        return Some(format!("relative_path `{path}` must be a forward-slash repo-relative path"));
    }
    if path.split('/').any(|segment| {
        let lower = segment.to_ascii_lowercase();
        segment.is_empty()
            || segment == "."
            || segment == ".."
            || lower == ".git"
            || lower == ".hg"
            || lower == ".svn"
            || lower == ".env"
            || lower.starts_with(".env.")
    }) {
        return Some(format!(
            "relative_path `{path}` must not contain empty, traversal, VCS, or .env segments"
        ));
    }
    None
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

/// Best-effort wing extraction for `route_changes`'s Group C scope filtering.
///
/// Only a subset of change-log event types carry wing information: drawer
/// writes/deletes record it in `details_json.wing`, and mined-batch events
/// record it as the `mine_batch:{wing}` prefix of `entity_id` (parseable
/// safely because a `WingId` never contains `:`). Event types with no wing
/// concept at all return `None` here — see [`WINGLESS_EVENT_TYPES`] and
/// [`change_event_visible`] for how `None` is handled; this function only
/// extracts, it does not decide visibility.
fn change_event_wing(event: &ChangeEvent) -> Option<String> {
    if let Some(wing) = event.entity_id.strip_prefix("mine_batch:") {
        return Some(wing.to_owned());
    }
    let details = event.details_json.as_deref()?;
    let value: Value = serde_json::from_str(details).ok()?;
    value.get("wing").and_then(Value::as_str).map(str::to_owned)
}

/// Change-log event types with no wing concept at all — palace-level or
/// agent-level, never wing-scoped anywhere else in the system. These are the
/// only event types allowed to stay visible to a scoped token when
/// [`change_event_wing`] cannot determine a wing, matching the same
/// entity-scoped-not-wing-scoped rationale the Group D KG routes use (see
/// `route_kg_query` and friends).
///
/// This list must stay in sync with every event type actually written via
/// `append_event`/`log_change` across the workspace that does not carry a
/// wing. Adding a new wingless event type elsewhere means adding it here too;
/// forgetting to would make it wrongly disappear from a scoped token's feed,
/// which is the safe direction to fail in — see `change_event_visible`.
const WINGLESS_EVENT_TYPES: &[&str] = &[
    "kg_fact_added",
    "kg_fact_invalidated",
    "identity_updated",
    "lineage_set",
    "lineage_migration_recorded",
    "self_observation_proposed",
    "self_observation_reviewed",
];

/// Whether `event` is visible to a token with the given `visibility`, for
/// `route_changes`'s Group C filtering.
///
/// Fails **closed**: when [`change_event_wing`] cannot determine a wing, the
/// event is hidden from a scoped token unless its `event_type` is on
/// [`WINGLESS_EVENT_TYPES`]. An unrestricted token (`WingVisibility::All` —
/// absent `scopes`, or a scope entry granting `read` on `"*"`) still sees
/// everything regardless, so grandfathering is unaffected.
///
/// This matters because not every writer of `drawer_deleted` events records a
/// wing (older events, and some remote-fallback paths in `mempalace-mcp`,
/// have no local record of it to record), and because `entity_id` alone can
/// leak a wing in plaintext — drawer ids built by `mined_drawer_id`
/// (`crates/mempalace-core/src/hash.rs`) embed `{wing}/{room}/...` directly.
/// Defaulting an unrecognised shape to "visible" would hand a scoped token
/// exactly the cross-wing leak scoping exists to prevent, so the default here
/// is the opposite of `change_event_wing`'s old (and wrong) fail-open
/// behaviour.
fn change_event_visible(event: &ChangeEvent, visibility: &WingVisibility) -> bool {
    match change_event_wing(event) {
        Some(wing) => visibility.contains(&wing),
        None => {
            matches!(visibility, WingVisibility::All)
                || WINGLESS_EVENT_TYPES.contains(&event.event_type.as_str())
        }
    }
}

/// Converts a storage `ChangeEvent` to the federation wire DTO.
fn change_event_to_dto(event: ChangeEvent) -> Result<ChangeEventDto, ServerError> {
    let details = event.details_json.as_deref().map(serde_json::from_str::<Value>).transpose()?;
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
    let rowid = rowid_part
        .parse::<i64>()
        .map_err(|_| ServerError::InvalidParams(format!("invalid cursor rowid `{rowid_part}`")))?;
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
        view: None,
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
                "content_hash": r.content_hash,
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
    let embedding = response.vectors().first().cloned().ok_or_else(|| {
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
        view_metadata: None,
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

/// Validates a free-form KG field before it is passed to the graph store.
fn validate_kg_field(name: &str, value: &str) -> Result<(), ServerError> {
    if value.len() > MAX_KG_FIELD_BYTES {
        return Err(ServerError::InvalidParams(format!(
            "{name} must be at most {MAX_KG_FIELD_BYTES} bytes"
        )));
    }
    Ok(())
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

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode, header};
    use http_body_util::BodyExt;
    use mempalace_config::{
        FederationRuntimeConfig, LowCpuRuntimeConfig, MaintenanceRuntimeConfig, ServerRuntimeConfig,
    };
    use mempalace_core::EmbeddingProfile;
    use mempalace_embeddings::DeterministicStubProvider;
    use serde_json::Value;
    use tempfile::TempDir;
    use tower::ServiceExt;

    // ─── Test harness ─────────────────────────────────────────────────────────

    const ALICE_TOKEN: &str = "alice-secret-token";
    const BOB_TOKEN: &str = "bob-secret-token";
    const BAD_TOKEN: &str = "bad-token-xyz";
    // Scoped-token fixtures (issue #102 Stage 2). `alice` above stays
    // unrestricted (no `scopes` field) and is the grandfathering baseline;
    // these add the scope shapes the authorization tests below need.
    /// Scoped to `wing_alpha` only, with every operation used by the routes
    /// that exist in Stage 2 (`coordination_*` excluded — no routes yet).
    const SCOPED_ALPHA_TOKEN: &str = "scoped-alpha-secret-token";
    /// Scoped to `wing_alpha` only, `read` alone — for wrong-operation checks.
    const READONLY_ALPHA_TOKEN: &str = "readonly-alpha-secret-token";
    /// Explicit `"scopes": []` — a deliberate lockout, distinct from a token
    /// with no `scopes` field at all.
    const LOCKED_TOKEN: &str = "locked-secret-token";
    /// Scoped to `"*"` (every wing), `read` only.
    const WILDCARD_TOKEN: &str = "wildcard-secret-token";
    /// Scoped to the short-form wing name `"myproject"`, which normalizes to
    /// `wing_myproject` at load time.
    const SHORT_WING_TOKEN: &str = "short-wing-secret-token";
    /// Scoped to the already-prefixed, mixed-case wing id `"wing_MyProject"`
    /// verbatim — proves `normalize_scope_wing` keeps a valid fully-qualified
    /// wing id as-is instead of lowercasing it via `WingId::normalized`.
    const UPPERCASE_WING_TOKEN: &str = "uppercase-wing-secret-token";
    /// Scoped to the unprefixed wing id `"project_gamma"` verbatim — proves
    /// `normalize_scope_wing` keeps an unprefixed-but-valid entry as a
    /// matchable alias instead of only its `wing_`-prefixed normalized form.
    const UNPREFIXED_WING_TOKEN: &str = "unprefixed-wing-secret-token";
    // Coordination-token fixtures (issue #102 Stage 3).
    /// Scoped to `wing_alpha` only, with all three coordination operations —
    /// the full-access worker token used by the lifecycle tests and as the
    /// "cannot see wing_beta" side of the 404-masking tests.
    const COORD_ALPHA_TOKEN: &str = "coord-alpha-secret-token";
    /// Scoped to `wing_alpha` only, `coordination_write` alone (no
    /// `coordination_claim`) — proves a writer can create a task but not
    /// claim it.
    const COORD_WRITE_ONLY_TOKEN: &str = "coord-write-only-secret-token";
    /// Scoped to `wing_alpha` only, `coordination_claim` alone (no
    /// `coordination_write`) — proves the one-way implication (issue #102
    /// Stage 7): a claim grant reaches every write route, but a token with
    /// only `coordination_read` alongside it still gets nowhere near a write.
    const COORD_CLAIM_ONLY_TOKEN: &str = "coord-claim-only-secret-token";
    /// Scoped to `"*"` (every wing) with all three coordination operations —
    /// used only by the idempotency-replay-narrowing test, which rewrites
    /// this token's scope on disk mid-test (mirroring
    /// `hot_reload_picks_up_scope_change`) to narrow it to `wing_alpha`
    /// after it has already created a task elsewhere.
    const COORD_WIDE_TOKEN: &str = "coord-wide-secret-token";

    fn restrict_token_file(path: &std::path::Path) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let metadata = std::fs::metadata(path).unwrap();
            let mut permissions = metadata.permissions();
            permissions.set_mode(if metadata.is_dir() { 0o700 } else { 0o600 });
            std::fs::set_permissions(path, permissions).unwrap();
        }

        #[cfg(not(unix))]
        {
            let _ = path;
        }
    }

    struct Harness {
        router: Router,
        state: Arc<ServerState<DeterministicStubProvider>>,
        _tempdir: TempDir,
    }

    async fn make_harness() -> Harness {
        make_harness_with_maintenance(true).await
    }

    async fn make_harness_with_maintenance(maintenance_enabled: bool) -> Harness {
        make_harness_with_maintenance_config(maintenance_enabled, true).await
    }

    async fn make_harness_with_maintenance_config(
        maintenance_enabled: bool,
        background_maintenance_enabled: bool,
    ) -> Harness {
        let tempdir = TempDir::new().unwrap();
        let palace_path = tempdir.path().join("palace");

        // Write token file
        let token_file = tempdir.path().join("tokens.json");
        std::fs::write(&token_file, serde_json::to_string(&default_tokens_json()).unwrap())
            .unwrap();
        restrict_token_file(&token_file);

        let config = MempalaceConfig {
            schema_version: 1,
            collection_name: "mempalace_drawers".to_owned(),
            palace_path,
            embedding_profile: EmbeddingProfile::Balanced,
            low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
            server: ServerRuntimeConfig {
                bind: "127.0.0.1:8765".parse().unwrap(),
                token_file: tempdir.path().join("tokens.json"),
                checkouts: std::collections::BTreeMap::new(),
            },
            federation: FederationRuntimeConfig::default(),
            maintenance: MaintenanceRuntimeConfig {
                enabled: maintenance_enabled,
                background_enabled: background_maintenance_enabled,
                ..MaintenanceRuntimeConfig::defaults()
            },
        };
        let tokens = TokenRegistry::load(token_file).unwrap();
        let provider = DeterministicStubProvider::new(EmbeddingProfile::Balanced);
        let (router, state) = build_router(config, provider, tokens).await.unwrap();
        Harness { router, state, _tempdir: tempdir }
    }

    /// The token-file fixture shared by every test in this module (extended
    /// in Stage 2 with scoped entries — see the constants above). Factored
    /// out so the hot-reload test can rewrite just one token's scopes on disk
    /// without hand-duplicating the rest of the fixture.
    fn default_tokens_json() -> Value {
        serde_json::json!([
            {"token": ALICE_TOKEN, "name": "alice", "enabled": true},
            {"token": BOB_TOKEN, "name": "bob", "enabled": false},
            {
                "token": SCOPED_ALPHA_TOKEN, "name": "scoped_alpha", "enabled": true,
                "scopes": [{
                    "wings": ["wing_alpha"],
                    "operations": ["read", "write", "delete", "ingest"],
                }],
            },
            {
                "token": READONLY_ALPHA_TOKEN, "name": "readonly_alpha", "enabled": true,
                "scopes": [{"wings": ["wing_alpha"], "operations": ["read"]}],
            },
            {"token": LOCKED_TOKEN, "name": "locked", "enabled": true, "scopes": []},
            {
                "token": WILDCARD_TOKEN, "name": "wildcard", "enabled": true,
                "scopes": [{"wings": ["*"], "operations": ["read"]}],
            },
            {
                "token": SHORT_WING_TOKEN, "name": "short_wing", "enabled": true,
                "scopes": [{"wings": ["myproject"], "operations": ["read", "write"]}],
            },
            {
                "token": UPPERCASE_WING_TOKEN, "name": "uppercase_wing", "enabled": true,
                "scopes": [{"wings": ["wing_MyProject"], "operations": ["read", "write"]}],
            },
            {
                "token": UNPREFIXED_WING_TOKEN, "name": "unprefixed_wing", "enabled": true,
                "scopes": [{"wings": ["project_gamma"], "operations": ["read", "write"]}],
            },
            {
                "token": COORD_ALPHA_TOKEN, "name": "coord_alpha", "enabled": true,
                "scopes": [{
                    "wings": ["wing_alpha"],
                    "operations": ["coordination_read", "coordination_write", "coordination_claim"],
                }],
            },
            {
                "token": COORD_WRITE_ONLY_TOKEN, "name": "coord_write_only", "enabled": true,
                "scopes": [{"wings": ["wing_alpha"], "operations": ["coordination_write"]}],
            },
            {
                "token": COORD_CLAIM_ONLY_TOKEN, "name": "coord_claim_only", "enabled": true,
                "scopes": [{"wings": ["wing_alpha"], "operations": ["coordination_claim"]}],
            },
            {
                "token": COORD_WIDE_TOKEN, "name": "coord_wide", "enabled": true,
                "scopes": [{
                    "wings": ["*"],
                    "operations": ["coordination_read", "coordination_write", "coordination_claim"],
                }],
            },
        ])
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

    #[test]
    fn token_registry_rejects_enabled_empty_token() {
        let tempdir = TempDir::new().unwrap();
        let token_file = tempdir.path().join("tokens.json");
        std::fs::write(
            &token_file,
            serde_json::to_string(&serde_json::json!([
                {"token": "", "name": "empty", "enabled": true},
            ]))
            .unwrap(),
        )
        .unwrap();
        restrict_token_file(&token_file);

        let err = TokenRegistry::load(token_file).unwrap_err();
        assert!(err.to_string().contains("must not be empty"), "{err}");
    }

    // Finding #3 regression: a misspelled `scopes` key must not silently
    // grant an unrestricted token. Absent `scopes` means unrestricted (see
    // `TokenEntry::scopes`), so without `deny_unknown_fields` a typo like
    // `"scope"` would be dropped by serde as an unrecognised field, leaving
    // `scopes` absent and the token unrestricted — the opposite of what an
    // operator writing a scoped-token entry intended. The error must also
    // name the offending field so an operator can actually find the typo.

    #[test]
    fn token_file_rejects_misspelled_scopes_key() {
        let tempdir = TempDir::new().unwrap();
        let token_file = tempdir.path().join("tokens.json");
        std::fs::write(
            &token_file,
            serde_json::to_string(&serde_json::json!([
                {
                    "token": "typo-secret-token", "name": "typo", "enabled": true,
                    // Misspelled: should be "scopes". Absent-`scopes` means
                    // unrestricted, so this must be a load error, not a
                    // silently-unrestricted token.
                    "scope": [{"wings": ["wing_alpha"], "operations": ["read"]}],
                },
            ]))
            .unwrap(),
        )
        .unwrap();
        restrict_token_file(&token_file);

        let err = TokenRegistry::load(token_file).expect_err(
            "a misspelled `scopes` key must fail the token file load, not silently grant an \
             unrestricted token",
        );
        assert!(
            err.to_string().contains("scope"),
            "the load error must name the offending unrecognised field: {err}"
        );
    }

    #[test]
    fn token_file_accepts_correctly_spelled_scopes_key() {
        let tempdir = TempDir::new().unwrap();
        let token_file = tempdir.path().join("tokens.json");
        std::fs::write(
            &token_file,
            serde_json::to_string(&serde_json::json!([
                {
                    "token": "spelled-right-secret-token", "name": "spelled_right", "enabled": true,
                    "scopes": [{"wings": ["wing_alpha"], "operations": ["read"]}],
                },
            ]))
            .unwrap(),
        )
        .unwrap();
        restrict_token_file(&token_file);

        let registry = TokenRegistry::load(token_file)
            .expect("a correctly-spelled `scopes` key must still load");
        let identity = registry
            .authenticate("spelled-right-secret-token")
            .expect("the token from a successfully-loaded file must authenticate");
        assert_eq!(identity.name(), "spelled_right");
        assert!(identity.allows_wing(Operation::Read, "wing_alpha"));
        assert!(!identity.allows_wing(Operation::Read, "wing_beta"), "scope must stay restrictive");
    }

    #[test]
    fn token_reload_parse_error_fails_closed() {
        let tempdir = TempDir::new().unwrap();
        let token_file = tempdir.path().join("tokens.json");
        std::fs::write(
            &token_file,
            serde_json::to_string(&serde_json::json!([
                {"token": ALICE_TOKEN, "name": "alice", "enabled": true},
            ]))
            .unwrap(),
        )
        .unwrap();
        restrict_token_file(&token_file);
        let registry = TokenRegistry::load(token_file.clone()).unwrap();
        assert_eq!(registry.authenticate(ALICE_TOKEN).as_ref().map(AuthIdentity::name), Some("alice"));
        assert!(registry.authenticate("").is_none());

        std::thread::sleep(std::time::Duration::from_millis(1100));
        std::fs::write(&token_file, "not valid json").unwrap();
        assert!(registry.authenticate(ALICE_TOKEN).is_none());
    }

    #[test]
    fn token_reload_io_error_fails_closed_but_retries() {
        let tempdir = TempDir::new().unwrap();
        let token_file = tempdir.path().join("tokens.json");
        std::fs::write(
            &token_file,
            serde_json::to_string(&serde_json::json!([
                {"token": ALICE_TOKEN, "name": "alice", "enabled": true},
            ]))
            .unwrap(),
        )
        .unwrap();
        restrict_token_file(&token_file);
        let registry = TokenRegistry::load(token_file.clone()).unwrap();
        assert_eq!(registry.authenticate(ALICE_TOKEN).as_ref().map(AuthIdentity::name), Some("alice"));

        std::thread::sleep(std::time::Duration::from_millis(1100));
        std::fs::remove_file(&token_file).unwrap();
        std::fs::create_dir(&token_file).unwrap();
        restrict_token_file(&token_file);
        assert!(registry.authenticate(ALICE_TOKEN).is_none());
        {
            let guard = registry.inner.read().unwrap();
            assert!(guard.entries.is_empty());
            assert!(guard.mtime.is_none());
        }

        std::fs::remove_dir(&token_file).unwrap();
        std::fs::write(
            &token_file,
            serde_json::to_string(&serde_json::json!([
                {"token": ALICE_TOKEN, "name": "alice", "enabled": true},
            ]))
            .unwrap(),
        )
        .unwrap();
        restrict_token_file(&token_file);
        assert_eq!(registry.authenticate(ALICE_TOKEN).as_ref().map(AuthIdentity::name), Some("alice"));
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
            .oneshot(authed_json_request(Method::POST, "/v1/drawers", ALICE_TOKEN, payload.clone()))
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

    // ─── Scoped-token authorization (issue #102 Stage 2) ─────────────────────

    /// Seeds one drawer in `wing_alpha` (room `alpha-room`) and one in
    /// `wing_beta` (room `beta-room`) via the unrestricted `alice` token, for
    /// Group C filtering tests and Group B wrong/right-wing tests. Returns
    /// the two drawer ids `(alpha_id, beta_id)`.
    async fn seed_two_wings(harness: &Harness) -> (String, String) {
        let mut ids = Vec::with_capacity(2);
        for (wing, room, content) in [
            ("wing_alpha", "alpha-room", "alpha wing content about rust programming patterns"),
            ("wing_beta", "beta-room", "beta wing content about javascript tooling ecosystems"),
        ] {
            let resp = harness
                .router
                .clone()
                .oneshot(authed_json_request(
                    Method::POST,
                    "/v1/drawers",
                    ALICE_TOKEN,
                    json!({"wing": wing, "room": room, "content": content}),
                ))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK, "seeding {wing} failed");
            let body = body_json(resp).await;
            ids.push(body["drawer_id"].as_str().unwrap().to_owned());
        }
        (ids[0].clone(), ids[1].clone())
    }

    /// Seeds `count` drawers into `wing`, each containing `"rust"` so every
    /// one lands in the same deterministic-stub embedding bucket (see
    /// `DeterministicStubProvider::vector_for`) and therefore ties for
    /// similarity against a matching query. Written via `/v1/ingest/batch`
    /// rather than `POST /v1/drawers` because the latter runs near-duplicate
    /// detection — seeding several same-bucket drawers through it would 409
    /// on everything after the first. Used by the pagination-completeness
    /// tests below, which need many tied candidates to exercise ranking and
    /// the over-fetch heuristic.
    async fn seed_rust_bucket_drawers(harness: &Harness, wing: &str, count: usize) {
        let files: Vec<Value> = (0..count)
            .map(|i| {
                json!({
                    "relative_path": format!("{wing}/pagination-{i}.txt"),
                    "content_hash": format!("ch-{wing}-{i}"),
                    "chunks": [{
                        "chunk_index": 0,
                        "room": "pagination",
                        "text": format!("rust pagination filler content number {i}"),
                    }],
                })
            })
            .collect();
        let resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/ingest/batch",
                ALICE_TOKEN,
                json!({
                    "wing": wing,
                    "repo_id": format!("github.com/acme/{wing}-pagination"),
                    "files": files,
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "seeding {wing} failed");
    }

    // Grandfathering (absent `scopes` = unrestricted), one route per group.

    #[tokio::test]
    async fn grandfathered_token_covers_group_a_write() {
        let harness = make_harness().await;
        let resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers",
                ALICE_TOKEN,
                json!({"wing": "wing_any", "room": "r", "content": "grandfathering group a write proof"}),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn grandfathered_token_covers_group_b_get() {
        let harness = make_harness().await;
        let add_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers",
                ALICE_TOKEN,
                json!({"wing": "wing_any", "room": "r", "content": "grandfathering group b get proof"}),
            ))
            .await
            .unwrap();
        assert_eq!(add_resp.status(), StatusCode::OK);
        let drawer_id = body_json(add_resp).await["drawer_id"].as_str().unwrap().to_owned();

        let get_resp = harness
            .router
            .clone()
            .oneshot(authed_get(&format!("/v1/drawers/{drawer_id}"), ALICE_TOKEN))
            .await
            .unwrap();
        assert_eq!(get_resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn grandfathered_token_covers_group_d_kg_query() {
        let harness = make_harness().await;
        let resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/kg/query",
                ALICE_TOKEN,
                json!({"entity": "GrandfatherProof", "direction": "outgoing"}),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
    // Group C's grandfathering is covered by the `alice` assertions inside
    // the `group_c_*_filters_to_visible_wings` tests below.

    // `scopes: []` is a deliberate lockout, distinct from absent `scopes`.

    #[tokio::test]
    async fn empty_scopes_denied_distinct_from_absent_scopes() {
        let harness = make_harness().await;

        // Absent scopes (alice): unrestricted.
        let alice_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/kg/query",
                ALICE_TOKEN,
                json!({"entity": "ScopeDistinctionProof", "direction": "outgoing"}),
            ))
            .await
            .unwrap();
        assert_eq!(alice_resp.status(), StatusCode::OK, "absent scopes must be unrestricted");

        // Explicit empty scopes (locked): denied, on the exact same route.
        let locked_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/kg/query",
                LOCKED_TOKEN,
                json!({"entity": "ScopeDistinctionProof", "direction": "outgoing"}),
            ))
            .await
            .unwrap();
        assert_eq!(
            locked_resp.status(),
            StatusCode::FORBIDDEN,
            "`scopes: []` must not behave like absent scopes"
        );
    }

    #[tokio::test]
    async fn locked_token_denied_on_group_a() {
        let harness = make_harness().await;
        let resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers/search",
                LOCKED_TOKEN,
                json!({"query": "anything"}),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    // Group A: wrong wing -> 403, right wing -> 200. Covers both the
    // body-derived wing routes named explicitly in the design doc
    // (`POST /v1/drawers`, `POST /v1/ingest/batch`) plus search (body,
    // optional wing) and list (query, optional wing).

    #[tokio::test]
    async fn scoped_token_group_a_search_wing_enforcement() {
        let harness = make_harness().await;

        let wrong = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers/search",
                SCOPED_ALPHA_TOKEN,
                json!({"query": "anything", "wing": "wing_beta"}),
            ))
            .await
            .unwrap();
        assert_eq!(wrong.status(), StatusCode::FORBIDDEN, "wrong wing must be 403 on Group A");

        let right = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers/search",
                SCOPED_ALPHA_TOKEN,
                json!({"query": "anything", "wing": "wing_alpha"}),
            ))
            .await
            .unwrap();
        assert_eq!(right.status(), StatusCode::OK, "right wing must be 200");
    }

    #[tokio::test]
    async fn scoped_token_group_a_add_wing_enforcement() {
        let harness = make_harness().await;

        let wrong = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers",
                SCOPED_ALPHA_TOKEN,
                json!({"wing": "wing_beta", "room": "r", "content": "wrong wing add attempt content"}),
            ))
            .await
            .unwrap();
        assert_eq!(wrong.status(), StatusCode::FORBIDDEN);

        let right = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers",
                SCOPED_ALPHA_TOKEN,
                json!({"wing": "wing_alpha", "room": "r", "content": "right wing add attempt content"}),
            ))
            .await
            .unwrap();
        assert_eq!(right.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn scoped_token_group_a_ingest_wing_enforcement() {
        let harness = make_harness().await;
        let file = |path: &str| {
            json!({
                "relative_path": path,
                "content_hash": format!("ch-{path}"),
                "chunks": [{"chunk_index": 0, "room": "backend", "text": "ingest scope enforcement content"}],
            })
        };

        let wrong = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/ingest/batch",
                SCOPED_ALPHA_TOKEN,
                json!({"wing": "wing_beta", "repo_id": "github.com/acme/x", "files": [file("a.rs")]}),
            ))
            .await
            .unwrap();
        assert_eq!(wrong.status(), StatusCode::FORBIDDEN);

        let right = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/ingest/batch",
                SCOPED_ALPHA_TOKEN,
                json!({"wing": "wing_alpha", "repo_id": "github.com/acme/x", "files": [file("b.rs")]}),
            ))
            .await
            .unwrap();
        assert_eq!(right.status(), StatusCode::OK);
        let body = body_json(right).await;
        assert_eq!(body["files"][0]["status"], "ingested");
    }

    #[tokio::test]
    async fn scoped_token_group_a_list_query_wing_enforcement() {
        let harness = make_harness().await;
        seed_two_wings(&harness).await;

        let wrong = harness
            .router
            .clone()
            .oneshot(authed_get("/v1/drawers?wing=wing_beta", SCOPED_ALPHA_TOKEN))
            .await
            .unwrap();
        assert_eq!(wrong.status(), StatusCode::FORBIDDEN);

        let right = harness
            .router
            .clone()
            .oneshot(authed_get("/v1/drawers?wing=wing_alpha", SCOPED_ALPHA_TOKEN))
            .await
            .unwrap();
        assert_eq!(right.status(), StatusCode::OK);
        let body = body_json(right).await;
        assert_eq!(body["drawers"].as_array().unwrap().len(), 1);
    }

    // Group B: wrong wing -> 404 (not 403); right wing -> 200.

    #[tokio::test]
    async fn scoped_token_group_b_get_wrong_wing_returns_404() {
        let harness = make_harness().await;
        let (_alpha_id, beta_id) = seed_two_wings(&harness).await;

        let resp = harness
            .router
            .clone()
            .oneshot(authed_get(&format!("/v1/drawers/{beta_id}"), SCOPED_ALPHA_TOKEN))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "Group B masks scope denial as 404, not 403, matching the diary guard"
        );
    }

    #[tokio::test]
    async fn scoped_token_group_b_get_right_wing_returns_200() {
        let harness = make_harness().await;
        let (alpha_id, _beta_id) = seed_two_wings(&harness).await;

        let resp = harness
            .router
            .clone()
            .oneshot(authed_get(&format!("/v1/drawers/{alpha_id}"), SCOPED_ALPHA_TOKEN))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn scoped_token_group_b_delete_wrong_wing_returns_404_and_does_not_delete() {
        let harness = make_harness().await;
        let (_alpha_id, beta_id) = seed_two_wings(&harness).await;

        let del_req = Request::builder()
            .method(Method::DELETE)
            .uri(format!("/v1/drawers/{beta_id}"))
            .header(header::AUTHORIZATION, format!("Bearer {SCOPED_ALPHA_TOKEN}"))
            .body(Body::empty())
            .unwrap();
        let del_resp = harness.router.clone().oneshot(del_req).await.unwrap();
        assert_eq!(del_resp.status(), StatusCode::NOT_FOUND);

        // Confirm the denied delete did not actually remove the drawer.
        let get_resp = harness
            .router
            .clone()
            .oneshot(authed_get(&format!("/v1/drawers/{beta_id}"), ALICE_TOKEN))
            .await
            .unwrap();
        assert_eq!(get_resp.status(), StatusCode::OK, "denied delete must not have removed the drawer");
    }

    // Wrong operation with the right wing -> 403.

    #[tokio::test]
    async fn readonly_scope_wrong_operation_returns_403() {
        let harness = make_harness().await;
        let resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers",
                READONLY_ALPHA_TOKEN,
                json!({"wing": "wing_alpha", "room": "r", "content": "readonly write attempt content"}),
            ))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "a read-only token must not be able to write, even to its own wing"
        );
    }

    // `"*"` wing works.

    #[tokio::test]
    async fn wildcard_wing_scope_allows_any_wing() {
        let harness = make_harness().await;
        seed_two_wings(&harness).await;

        for wing in ["wing_alpha", "wing_beta"] {
            let resp = harness
                .router
                .clone()
                .oneshot(authed_get(&format!("/v1/drawers?wing={wing}"), WILDCARD_TOKEN))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK, "wildcard scope must authorize {wing}");
        }
    }

    // Wing spelling normalisation: a token scoped to a short-form wing name
    // authorizes a request naming the `wing_`-prefixed form.

    #[tokio::test]
    async fn wing_scope_short_form_normalizes_to_prefixed_request_wing() {
        let harness = make_harness().await;

        let right = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers",
                SHORT_WING_TOKEN,
                json!({"wing": "wing_myproject", "room": "r", "content": "short form wing normalization proof"}),
            ))
            .await
            .unwrap();
        assert_eq!(
            right.status(),
            StatusCode::OK,
            "`myproject` in the token file must authorize `wing_myproject` in the request"
        );

        let wrong = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers",
                SHORT_WING_TOKEN,
                json!({"wing": "wing_otherproject", "room": "r", "content": "short form wing normalization negative proof"}),
            ))
            .await
            .unwrap();
        assert_eq!(wrong.status(), StatusCode::FORBIDDEN);
    }

    // `normalize_scope_wing` unit coverage: two dimensions, aliased
    // differently. The prefix is aliased (an unprefixed valid entry expands
    // to itself plus a `WING_PREFIX`-prepended sibling, case untouched);
    // case is never aliased (an already-valid entry, prefixed or not, keeps
    // exactly the case it was written with — `wing_MyProject` and
    // `wing_myproject` can be two distinct wings holding different data, and
    // folding one into the other in an authorization grant would be a
    // privilege escalation). See the doc comment on `normalize_scope_wing`
    // for the full precedence.

    #[test]
    fn normalize_scope_wing_prefixed_exact_yields_only_verbatim_case_significant() {
        // Prefixed exact: nothing to alias on the prefix dimension (already
        // has `WING_PREFIX`), and the case dimension is never aliased, so
        // `wing_MyProject` expands to itself alone — NOT also
        // `wing_myproject`, which the pre-fix version incorrectly added.
        assert_eq!(normalize_scope_wing("wing_MyProject").unwrap(), vec!["wing_MyProject"]);
    }

    #[test]
    fn normalize_scope_wing_unprefixed_exact_yields_verbatim_and_prefixed_aliases() {
        // Unprefixed exact. `WingId::new` does not require `WING_PREFIX`, so
        // `project_alpha` is a valid `WingId` on its own — REST-stored wings
        // may legitimately be spelled this way — and must be kept verbatim,
        // with the prefixed spelling added as a second alias (prefix
        // dimension aliased).
        assert_eq!(
            normalize_scope_wing("project_alpha").unwrap(),
            vec!["project_alpha", "wing_project_alpha"]
        );
    }

    #[test]
    fn normalize_scope_wing_unprefixed_mixed_case_preserves_case_in_both_aliases() {
        // Pins the split: prefix aliasing still applies to a mixed-case
        // unprefixed entry, but neither alias it produces folds case —
        // `MyProject` must not become `myproject` on either spelling.
        assert_eq!(
            normalize_scope_wing("MyProject").unwrap(),
            vec!["MyProject", "wing_MyProject"]
        );
    }

    #[test]
    fn normalize_scope_wing_short_form_prefixes_without_folding_case() {
        // Short form, already lowercase, so case-preservation is not visible
        // here — `normalize_scope_wing_unprefixed_mixed_case_preserves_case_in_both_aliases`
        // above is what actually pins that case is untouched. This test
        // pins only that the prefix dimension still aliases: `myproject` is
        // itself a valid `WingId` (verbatim) and gains the `wing_`-prefixed
        // sibling REST/MCP paths may build.
        assert_eq!(normalize_scope_wing("myproject").unwrap(), vec!["myproject", "wing_myproject"]);
    }

    #[test]
    fn normalize_scope_wing_preserves_wildcard() {
        assert_eq!(normalize_scope_wing("*").unwrap(), vec!["*"]);
    }

    // End-to-end proof that case is NOT aliased: a token scoped to an
    // already-prefixed, mixed-case wing id authorizes a request naming that
    // exact wing — and only that exact spelling. A request naming its
    // lowercased form must be rejected: `wing_MyProject` and
    // `wing_myproject` can be two distinct, independently-stored wings, and
    // folding one into the other here would silently widen the grant beyond
    // what the operator wrote — a privilege escalation, not a convenience.

    #[tokio::test]
    async fn wing_scope_exact_prefixed_form_preserved_verbatim_case_significant() {
        let harness = make_harness().await;

        let right = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers",
                UPPERCASE_WING_TOKEN,
                json!({"wing": "wing_MyProject", "room": "r", "content": "uppercase wing verbatim proof"}),
            ))
            .await
            .unwrap();
        assert_eq!(
            right.status(),
            StatusCode::OK,
            "`wing_MyProject` in the token file must authorize the identical `wing_MyProject` in the request"
        );

        let lowercased = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers",
                UPPERCASE_WING_TOKEN,
                json!({"wing": "wing_myproject", "room": "r", "content": "uppercase wing verbatim negative proof"}),
            ))
            .await
            .unwrap();
        assert_eq!(
            lowercased.status(),
            StatusCode::FORBIDDEN,
            "a verbatim-form scope must not also grant a case-folded alias — \
             `wing_MyProject` and `wing_myproject` can be two distinct wings"
        );
    }

    // End-to-end proof of the unprefixed-exact case (finding #2): a scope
    // entry with no `WING_PREFIX` authorizes a request naming that exact
    // unprefixed wing, matching how REST paths build wings with `WingId::new`
    // (verbatim, no transform) rather than `WingId::normalized`.

    #[tokio::test]
    async fn wing_scope_unprefixed_exact_authorizes_unprefixed_request_wing() {
        let harness = make_harness().await;

        let unprefixed = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers",
                UNPREFIXED_WING_TOKEN,
                json!({"wing": "project_gamma", "room": "r", "content": "unprefixed wing verbatim proof"}),
            ))
            .await
            .unwrap();
        assert_eq!(
            unprefixed.status(),
            StatusCode::OK,
            "`project_gamma` in the token file must authorize the identical unprefixed `project_gamma` in the request"
        );

        let normalized = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers",
                UNPREFIXED_WING_TOKEN,
                json!({"wing": "wing_project_gamma", "room": "r", "content": "rust cli tooling ecosystem documentation for the staging cluster"}),
            ))
            .await
            .unwrap();
        assert_eq!(
            normalized.status(),
            StatusCode::OK,
            "`project_gamma` in the token file must also authorize its normalized alias `wing_project_gamma`"
        );

        let wrong = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers",
                UNPREFIXED_WING_TOKEN,
                json!({"wing": "wing_otherproject", "room": "r", "content": "unprefixed wing negative proof"}),
            ))
            .await
            .unwrap();
        assert_eq!(wrong.status(), StatusCode::FORBIDDEN);
    }

    // Group C: filter aggregate routes to visible wings rather than reject.

    #[tokio::test]
    async fn group_c_taxonomy_filters_to_visible_wings() {
        let harness = make_harness().await;
        seed_two_wings(&harness).await;

        let scoped =
            harness.router.clone().oneshot(authed_get("/v1/taxonomy", SCOPED_ALPHA_TOKEN)).await.unwrap();
        assert_eq!(scoped.status(), StatusCode::OK);
        let scoped_body = body_json(scoped).await;
        let taxonomy = scoped_body["taxonomy"].as_object().unwrap();
        assert!(taxonomy.contains_key("wing_alpha"));
        assert!(!taxonomy.contains_key("wing_beta"), "scoped token must not see wing_beta in taxonomy");

        let alice =
            harness.router.clone().oneshot(authed_get("/v1/taxonomy", ALICE_TOKEN)).await.unwrap();
        let alice_body = body_json(alice).await;
        let alice_taxonomy = alice_body["taxonomy"].as_object().unwrap();
        assert!(alice_taxonomy.contains_key("wing_alpha"));
        assert!(
            alice_taxonomy.contains_key("wing_beta"),
            "unrestricted (grandfathered) token must see both wings"
        );
    }

    #[tokio::test]
    async fn group_c_wings_filters_to_visible_wings() {
        let harness = make_harness().await;
        seed_two_wings(&harness).await;

        let scoped =
            harness.router.clone().oneshot(authed_get("/v1/wings", SCOPED_ALPHA_TOKEN)).await.unwrap();
        let scoped_body = body_json(scoped).await;
        let wings = scoped_body["wings"].as_object().unwrap();
        assert!(wings.contains_key("wing_alpha"));
        assert!(!wings.contains_key("wing_beta"));

        let alice = harness.router.clone().oneshot(authed_get("/v1/wings", ALICE_TOKEN)).await.unwrap();
        let alice_body = body_json(alice).await;
        let alice_wings = alice_body["wings"].as_object().unwrap();
        assert!(alice_wings.contains_key("wing_alpha"));
        assert!(alice_wings.contains_key("wing_beta"));
    }

    #[tokio::test]
    async fn group_c_rooms_filters_to_visible_wings() {
        let harness = make_harness().await;
        seed_two_wings(&harness).await;

        let scoped =
            harness.router.clone().oneshot(authed_get("/v1/rooms", SCOPED_ALPHA_TOKEN)).await.unwrap();
        let scoped_body = body_json(scoped).await;
        let rooms = scoped_body["rooms"].as_object().unwrap();
        assert!(rooms.contains_key("alpha-room"));
        assert!(!rooms.contains_key("beta-room"), "scoped token must not see wing_beta's rooms");

        let alice = harness.router.clone().oneshot(authed_get("/v1/rooms", ALICE_TOKEN)).await.unwrap();
        let alice_body = body_json(alice).await;
        let alice_rooms = alice_body["rooms"].as_object().unwrap();
        assert!(alice_rooms.contains_key("alpha-room"));
        assert!(alice_rooms.contains_key("beta-room"));
    }

    #[tokio::test]
    async fn group_c_changes_filters_to_visible_wings() {
        let harness = make_harness().await;
        let (alpha_id, beta_id) = seed_two_wings(&harness).await;

        let scoped = harness
            .router
            .clone()
            .oneshot(authed_get("/v1/changes?limit=50", SCOPED_ALPHA_TOKEN))
            .await
            .unwrap();
        let scoped_body = body_json(scoped).await;
        let scoped_events = scoped_body["events"].as_array().unwrap();
        assert!(scoped_events.iter().any(|e| e["entity_id"] == alpha_id));
        assert!(
            !scoped_events.iter().any(|e| e["entity_id"] == beta_id),
            "scoped token must not see wing_beta's change events: {scoped_events:?}"
        );

        let alice =
            harness.router.clone().oneshot(authed_get("/v1/changes?limit=50", ALICE_TOKEN)).await.unwrap();
        let alice_body = body_json(alice).await;
        let alice_events = alice_body["events"].as_array().unwrap();
        assert!(alice_events.iter().any(|e| e["entity_id"] == alpha_id));
        assert!(alice_events.iter().any(|e| e["entity_id"] == beta_id));
    }

    // `/v1/changes` must fail CLOSED: an event whose wing cannot be
    // determined (e.g. an older or remote-fallback `drawer_deleted` with no
    // wing in `details_json`) must be hidden from a scoped token, not shown
    // by default. An unrestricted token must still see it.
    #[tokio::test]
    async fn changes_hides_wingless_event_from_scoped_token_but_not_unrestricted() {
        let harness = make_harness().await;

        // Inject a wing-less `drawer_deleted` event directly into the change
        // log, bypassing the server's own routes (which always record wing
        // on delete) to simulate the shape an older palace, or the
        // mempalace-mcp remote-fallback delete path, could already contain.
        harness
            .state
            .storage
            .operational_store()
            .append_event(&ChangeEvent {
                event_type: "drawer_deleted".to_owned(),
                occurred_at: OffsetDateTime::now_utc(),
                entity_id: "wingless-drawer-deletion-id".to_owned(),
                actor: None,
                details_json: None,
            })
            .unwrap();

        let scoped = harness
            .router
            .clone()
            .oneshot(authed_get("/v1/changes?limit=50", SCOPED_ALPHA_TOKEN))
            .await
            .unwrap();
        let scoped_body = body_json(scoped).await;
        let scoped_events = scoped_body["events"].as_array().unwrap();
        assert!(
            !scoped_events.iter().any(|e| e["entity_id"] == "wingless-drawer-deletion-id"),
            "a wing-less event must fail closed (hidden) for a scoped token: {scoped_events:?}"
        );

        let alice =
            harness.router.clone().oneshot(authed_get("/v1/changes?limit=50", ALICE_TOKEN)).await.unwrap();
        let alice_body = body_json(alice).await;
        let alice_events = alice_body["events"].as_array().unwrap();
        assert!(
            alice_events.iter().any(|e| e["entity_id"] == "wingless-drawer-deletion-id"),
            "an unrestricted token must still see it (grandfathering unaffected): {alice_events:?}"
        );
    }

    // `check_duplicate` is Group C, not operation-only: its response can
    // reveal cross-wing content, so matches must be filtered to visible
    // wings, and `is_duplicate` must reflect that filtering, not bypass it.
    #[tokio::test]
    async fn check_duplicate_filters_matches_to_visible_wings() {
        let harness = make_harness().await;
        seed_two_wings(&harness).await;

        // wing_beta's own content verbatim, to maximize the chance the
        // deterministic stub embedding reports it as a near-duplicate.
        let query_content = "beta wing content about javascript tooling ecosystems";

        // Sanity: an unrestricted token does see the wing_beta match.
        let alice_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers/check_duplicate",
                ALICE_TOKEN,
                json!({"content": query_content}),
            ))
            .await
            .unwrap();
        assert_eq!(alice_resp.status(), StatusCode::OK);
        let alice_body = body_json(alice_resp).await;
        let alice_matches = alice_body["matches"].as_array().unwrap();
        assert!(
            alice_matches.iter().any(|m| m["wing"] == "wing_beta"),
            "sanity check failed: wing_beta should be a duplicate candidate: {alice_matches:?}"
        );
        assert_eq!(alice_body["is_duplicate"], true);

        // The scoped token (wing_alpha only) must see neither the match nor
        // a true `is_duplicate`, even though the content duplicates a
        // wing_beta drawer almost exactly.
        let scoped_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers/check_duplicate",
                SCOPED_ALPHA_TOKEN,
                json!({"content": query_content}),
            ))
            .await
            .unwrap();
        assert_eq!(scoped_resp.status(), StatusCode::OK, "must filter, not 403 — the caller does hold read");
        let scoped_body = body_json(scoped_resp).await;
        let scoped_matches = scoped_body["matches"].as_array().unwrap();
        assert!(
            scoped_matches.iter().all(|m| m["wing"] != "wing_beta"),
            "cross-wing check_duplicate must filter out wings the token cannot see: {scoped_matches:?}"
        );
        assert_eq!(
            scoped_body["is_duplicate"], false,
            "is_duplicate must be computed after filtering, not before — otherwise the boolean \
             alone is a cross-wing content oracle: {scoped_body}"
        );
    }

    // `route_drawers_add`'s own near-duplicate check has the identical shape:
    // it scans every wing via `find_duplicates`, so its 409 response (and the
    // `matches` it carries, with `wing`/`room` per match) would otherwise be
    // a cross-wing content oracle for a scoped writer. Matches outside the
    // caller's visible wings must be filtered out before the 409 decision is
    // made, not after.
    #[tokio::test]
    async fn add_drawer_duplicate_check_filters_to_visible_wings() {
        let harness = make_harness().await;
        seed_two_wings(&harness).await;

        // wing_beta's own content verbatim, so it is a near-duplicate of the
        // existing wing_beta drawer regardless of who is asking.
        let duplicate_content = "beta wing content about javascript tooling ecosystems";

        // Sanity: an unrestricted caller writing the same content to
        // wing_alpha is still blocked by the wing_beta duplicate — proves
        // duplicate detection itself still fires when the match IS visible.
        // 409 means this does not commit anything.
        let alice_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers",
                ALICE_TOKEN,
                json!({"wing": "wing_alpha", "room": "dup-room", "content": duplicate_content}),
            ))
            .await
            .unwrap();
        assert_eq!(
            alice_resp.status(),
            StatusCode::CONFLICT,
            "sanity check failed: an unrestricted caller should see the wing_beta duplicate"
        );

        // The scoped token (wing_alpha only) writing the identical content to
        // a wing it CAN write must succeed: the wing_beta duplicate is
        // outside its visible wings, so it must never surface as a 409 —
        // that 409 (and its `matches` body) would otherwise let a scoped
        // writer learn about wing_beta's content.
        let scoped_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers",
                SCOPED_ALPHA_TOKEN,
                json!({"wing": "wing_alpha", "room": "dup-room", "content": duplicate_content}),
            ))
            .await
            .unwrap();
        assert_eq!(
            scoped_resp.status(),
            StatusCode::OK,
            "cross-wing duplicate must not block a write the token is otherwise allowed to make"
        );

        // The write really did commit — this is the documented trade-off
        // (docs/Federation.md §1.5): the store now holds duplicate content
        // across wing_alpha and wing_beta, because reporting the duplicate
        // would have disclosed wing_beta's content to a caller that cannot
        // read it.
        let drawer_id = body_json(scoped_resp).await["drawer_id"].as_str().unwrap().to_owned();
        let get_resp = harness
            .router
            .clone()
            .oneshot(authed_get(&format!("/v1/drawers/{drawer_id}"), SCOPED_ALPHA_TOKEN))
            .await
            .unwrap();
        assert_eq!(get_resp.status(), StatusCode::OK, "the duplicate-content drawer must have committed");
    }

    // An unknown operation string in the token file is a load error.

    #[test]
    fn token_registry_rejects_unknown_operation() {
        let tempdir = TempDir::new().unwrap();
        let token_file = tempdir.path().join("tokens.json");
        std::fs::write(
            &token_file,
            serde_json::to_string(&serde_json::json!([
                {
                    "token": "x", "name": "bad_op", "enabled": true,
                    "scopes": [{"wings": ["*"], "operations": ["reed"]}],
                },
            ]))
            .unwrap(),
        )
        .unwrap();
        restrict_token_file(&token_file);

        let err = TokenRegistry::load(token_file).unwrap_err();
        assert!(matches!(err, ServerError::TokenFile(_)), "{err}");
    }

    // 401 (no/invalid token) and 403 (valid token, wrong scope) stay distinct.

    #[tokio::test]
    async fn unauthorized_and_forbidden_are_distinct_status_codes() {
        let harness = make_harness().await;

        let no_token_req =
            Request::builder().method(Method::GET).uri("/v1/kg/stats").body(Body::empty()).unwrap();
        let no_token_resp = harness.router.clone().oneshot(no_token_req).await.unwrap();
        assert_eq!(no_token_resp.status(), StatusCode::UNAUTHORIZED);

        let locked_resp =
            harness.router.clone().oneshot(authed_get("/v1/kg/stats", LOCKED_TOKEN)).await.unwrap();
        assert_eq!(locked_resp.status(), StatusCode::FORBIDDEN);

        assert_ne!(no_token_resp.status(), locked_resp.status());
    }

    // Hot reload picks up a scope change (the registry already reloads on
    // mtime change; this proves it applies to the new scope logic too).

    #[tokio::test]
    async fn hot_reload_picks_up_scope_change() {
        let harness = make_harness().await;

        // Initially scoped to wing_alpha only.
        let before = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers/search",
                SCOPED_ALPHA_TOKEN,
                json!({"query": "x", "wing": "wing_beta"}),
            ))
            .await
            .unwrap();
        assert_eq!(before.status(), StatusCode::FORBIDDEN);

        // Rewrite the token file, dropping scoped_alpha's `scopes` entirely
        // (now unrestricted), and wait past the mtime granularity the
        // registry's reload check relies on.
        let token_file = harness._tempdir.path().join("tokens.json");
        std::thread::sleep(std::time::Duration::from_millis(1100));
        std::fs::write(
            &token_file,
            serde_json::to_string(&serde_json::json!([
                {"token": ALICE_TOKEN, "name": "alice", "enabled": true},
                {"token": BOB_TOKEN, "name": "bob", "enabled": false},
                {"token": SCOPED_ALPHA_TOKEN, "name": "scoped_alpha", "enabled": true},
            ]))
            .unwrap(),
        )
        .unwrap();
        restrict_token_file(&token_file);

        let after = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers/search",
                SCOPED_ALPHA_TOKEN,
                json!({"query": "x", "wing": "wing_beta"}),
            ))
            .await
            .unwrap();
        assert_eq!(after.status(), StatusCode::OK, "hot reload must pick up the widened scope");
    }

    // Bonus: a wing-absent search must filter cross-wing candidates to what
    // the token can see, not merely reject an explicit wrong wing.

    #[tokio::test]
    async fn search_without_wing_filters_cross_wing_results_to_visible_scope() {
        let harness = make_harness().await;
        let (_alpha_id, beta_id) = seed_two_wings(&harness).await;

        // Query with wing_beta's own content verbatim, maximizing the chance
        // the deterministic stub embedding surfaces it as a top candidate.
        let query = json!({
            "query": "beta wing content about javascript tooling ecosystems",
            "limit": 10,
        });

        // Sanity: an unrestricted token's unfiltered search does surface it.
        let alice_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(Method::POST, "/v1/drawers/search", ALICE_TOKEN, query.clone()))
            .await
            .unwrap();
        let alice_body = body_json(alice_resp).await;
        let alice_results = alice_body["results"].as_array().unwrap();
        assert!(
            alice_results.iter().any(|r| r["drawer_id"] == beta_id),
            "sanity check failed: wing_beta drawer should be a search candidate: {alice_results:?}"
        );

        // The scoped token, searching the same query with no wing filter,
        // must never see wing_beta's drawer even though it is a candidate.
        let scoped_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(Method::POST, "/v1/drawers/search", SCOPED_ALPHA_TOKEN, query))
            .await
            .unwrap();
        let scoped_body = body_json(scoped_resp).await;
        let scoped_results = scoped_body["results"].as_array().unwrap();
        assert!(
            scoped_results.iter().all(|r| r["drawer_id"] != beta_id),
            "cross-wing search must filter out wings the token cannot see: {scoped_results:?}"
        );
        assert!(scoped_results.iter().all(|r| r["wing"] == "wing_alpha"));
    }

    // Pagination correctness, not security: scope (and diary) filtering runs
    // AFTER the storage/search layer has already applied `limit`, so without
    // an over-fetch a token whose visible results are outranked by invisible
    // ones could get fewer than `limit` results even though enough visible
    // candidates exist further down. `route_drawers_list` already over-fetches
    // 2x and filters before truncating (see the comment on its
    // `storage_limit`); `route_drawers_search` now follows the same pattern.

    #[tokio::test]
    async fn search_pagination_reaches_full_page_when_visible_results_rank_below_invisible() {
        let harness = make_harness().await;

        // Equal-sized groups (3 + 3) in the same embedding bucket, so every
        // drawer ties for top similarity against a matching query. Tied
        // matches sort by wing id ascending (`compare_ranked_matches` in
        // mempalace-search), and "wing_0hidden" < "wing_alpha"
        // lexicographically, so all 3 invisible drawers rank strictly ahead
        // of all 3 visible ones. A raw fetch of exactly `limit` (3) would
        // therefore surface zero visible results; only a >=2x over-fetch
        // reaches the visible tier.
        const LIMIT: usize = 3;
        seed_rust_bucket_drawers(&harness, "wing_0hidden", LIMIT).await;
        seed_rust_bucket_drawers(&harness, "wing_alpha", LIMIT).await;

        let resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers/search",
                SCOPED_ALPHA_TOKEN,
                json!({"query": "rust pagination filler content", "limit": LIMIT}),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let results = body["results"].as_array().unwrap();
        assert_eq!(
            results.len(),
            LIMIT,
            "a scoped token must still get a full page when its visible results rank below \
             invisible ones: {results:?}"
        );
        assert!(results.iter().all(|r| r["wing"] == "wing_alpha"));
    }

    #[tokio::test]
    async fn list_pagination_reaches_full_page_when_visible_rows_are_outnumbered_by_invisible_ones() {
        // Maintenance disabled so no background compaction can reorder rows
        // mid-test — `list_drawers` has no ranking (a plain storage scan),
        // so this test relies on insertion order: seeding the 3 invisible
        // rows before the 3 visible ones puts every invisible row ahead of
        // every visible one in the raw, un-over-fetched order.
        let harness = make_harness_with_maintenance_config(false, false).await;

        const LIMIT: usize = 3;
        seed_rust_bucket_drawers(&harness, "wing_0hidden", LIMIT).await;
        seed_rust_bucket_drawers(&harness, "wing_alpha", LIMIT).await;

        let resp = harness
            .router
            .clone()
            .oneshot(authed_get(&format!("/v1/drawers?limit={LIMIT}"), SCOPED_ALPHA_TOKEN))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let drawers = body["drawers"].as_array().unwrap();
        assert_eq!(
            drawers.len(),
            LIMIT,
            "a scoped token must still get a full page when its visible rows are outnumbered by \
             invisible ones earlier in storage order: {drawers:?}"
        );
        assert!(drawers.iter().all(|d| d["wing"] == "wing_alpha"));
    }

    // Finding #1 regression: the test above uses equal-sized groups (3 + 3),
    // which the old `limit * 2` over-fetch-then-filter still happened to
    // cover. This test seeds far more invisible rows than any fixed
    // over-fetch multiplier would reach, proving visibility is now enforced
    // by the storage query itself (`DrawerFilter::wings`) rather than by
    // filtering an already-limited page — the old code would return zero
    // rows here, permanently, since `route_drawers_list` has no cursor to
    // continue from.

    #[tokio::test]
    async fn list_pagination_reaches_full_page_when_invisible_rows_vastly_outnumber_the_page() {
        // Maintenance disabled — see the comment on the sibling test above;
        // insertion order matters here too.
        let harness = make_harness_with_maintenance_config(false, false).await;

        const LIMIT: usize = 3;
        // 10 invisible rows is well beyond `LIMIT * 2` (6): the old
        // over-fetch-then-filter code would fetch only 6 rows from storage,
        // all invisible, filter every one of them out, and return an empty
        // page with `next_cursor: None` — the visible rows seeded below
        // would be unreachable forever.
        const HIDDEN: usize = 10;
        seed_rust_bucket_drawers(&harness, "wing_0hidden", HIDDEN).await;
        seed_rust_bucket_drawers(&harness, "wing_alpha", LIMIT).await;

        let resp = harness
            .router
            .clone()
            .oneshot(authed_get(&format!("/v1/drawers?limit={LIMIT}"), SCOPED_ALPHA_TOKEN))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let drawers = body["drawers"].as_array().unwrap();
        assert_eq!(
            drawers.len(),
            LIMIT,
            "a scoped token must still get a full page when invisible rows vastly outnumber the \
             over-fetch window, not a permanently-truncated page: {drawers:?}"
        );
        assert!(drawers.iter().all(|d| d["wing"] == "wing_alpha"));
    }

    // ─── Ingest harness variant ───────────────────────────────────────────────

    /// Builds a harness with a custom checkouts map.
    async fn make_harness_with_checkouts(
        checkouts: std::collections::BTreeMap<String, std::path::PathBuf>,
    ) -> Harness {
        let tempdir = TempDir::new().unwrap();
        let palace_path = tempdir.path().join("palace");

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
        restrict_token_file(&token_file);

        let config = MempalaceConfig {
            schema_version: 1,
            collection_name: "mempalace_drawers".to_owned(),
            palace_path,
            embedding_profile: EmbeddingProfile::Balanced,
            low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
            server: ServerRuntimeConfig {
                bind: "127.0.0.1:8765".parse().unwrap(),
                token_file: tempdir.path().join("tokens.json"),
                checkouts,
            },
            federation: FederationRuntimeConfig::default(),
            maintenance: MaintenanceRuntimeConfig::defaults(),
        };
        let tokens = TokenRegistry::load(token_file).unwrap();
        let provider = DeterministicStubProvider::new(EmbeddingProfile::Balanced);
        let (router, state) = build_router(config, provider, tokens).await.unwrap();
        Harness { router, state, _tempdir: tempdir }
    }

    // ─── 9. Ingest batch ──────────────────────────────────────────────────────

    /// Happy path: 2 files (one with file_hash+ranges, one content-row without)
    /// → both "ingested", correct drawers_written, search finds them.
    #[tokio::test]
    async fn ingest_batch_happy_path_two_files() {
        let harness = make_harness().await;

        let req = json!({
            "wing": "wing_project",
            "repo_id": "github.com/acme/myrepo",
            "commit_hash": "abc123",
            "files": [
                {
                    // file_hash present → locator rows (will be stale since no checkout)
                    "relative_path": "src/auth.rs",
                    "content_hash": "ch-auth-v1",
                    "file_hash": "fh-auth-v1",
                    "chunks": [
                        {
                            "chunk_index": 0,
                            "room": "backend",
                            "text": "authentication logic password hashing",
                            "byte_start": 0, "byte_end": 37,
                            "line_start": 1, "line_end": 2
                        }
                    ]
                },
                {
                    // no file_hash → content rows
                    "relative_path": "docs/readme.md",
                    "content_hash": "ch-readme-v1",
                    "chunks": [
                        {
                            "chunk_index": 0,
                            "room": "docs",
                            "text": "project documentation overview guide"
                        }
                    ]
                }
            ]
        });

        let resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(Method::POST, "/v1/ingest/batch", ALICE_TOKEN, req))
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;

        let files = body["files"].as_array().unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0]["status"], "ingested");
        assert_eq!(files[0]["drawers_written"], 1u64);
        assert_eq!(files[1]["status"], "ingested");
        assert_eq!(files[1]["drawers_written"], 1u64);

        // Warnings should mention missing checkout since file_hash was set
        let warnings = body["warnings"].as_array().unwrap();
        assert!(!warnings.is_empty(), "expected missing-checkout warning");
        assert!(warnings[0].as_str().unwrap().contains("no checkout configured"));

        // List drawers should show 2 rows in the wing
        let list_resp = harness
            .router
            .clone()
            .oneshot(authed_get("/v1/drawers?wing=wing_project", ALICE_TOKEN))
            .await
            .unwrap();
        assert_eq!(list_resp.status(), StatusCode::OK);
        let list_body = body_json(list_resp).await;
        let drawers = list_body["drawers"].as_array().unwrap();
        assert_eq!(drawers.len(), 2);
    }

    /// Idempotent re-push: same request twice → second round is skipped_unchanged.
    #[tokio::test]
    async fn ingest_batch_idempotent_repush() {
        let harness = make_harness().await;

        let req = json!({
            "wing": "wing_idem",
            "repo_id": "github.com/acme/repo2",
            "files": [
                {
                    "relative_path": "src/main.rs",
                    "content_hash": "stable-hash",
                    "chunks": [
                        {
                            "chunk_index": 0, "room": "backend",
                            "text": "main entry point program"
                        }
                    ]
                }
            ]
        });

        // First push
        let resp1 = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/ingest/batch",
                ALICE_TOKEN,
                req.clone(),
            ))
            .await
            .unwrap();
        assert_eq!(resp1.status(), StatusCode::OK);
        let body1 = body_json(resp1).await;
        assert_eq!(body1["files"][0]["status"], "ingested");

        // Second push — same content_hash
        let resp2 = harness
            .router
            .clone()
            .oneshot(authed_json_request(Method::POST, "/v1/ingest/batch", ALICE_TOKEN, req))
            .await
            .unwrap();
        assert_eq!(resp2.status(), StatusCode::OK);
        let body2 = body_json(resp2).await;
        assert_eq!(body2["files"][0]["status"], "skipped_unchanged");
        assert_eq!(body2["files"][0]["drawers_written"], 0u64);
    }

    /// Changed file: modified content_hash + fewer chunks → "ingested" and dropped
    /// chunk's drawer is gone.
    #[tokio::test]
    async fn ingest_batch_changed_file_drops_old_drawers() {
        let harness = make_harness().await;

        // Initial: 2 chunks
        let req_v1 = json!({
            "wing": "wing_change",
            "repo_id": "github.com/acme/change-test",
            "files": [
                {
                    "relative_path": "src/lib.rs",
                    "content_hash": "hash-v1",
                    "chunks": [
                        {"chunk_index": 0, "room": "backend",
                         "text": "library initialization setup configuration"},
                        {"chunk_index": 1, "room": "backend",
                         "text": "library helper utilities functions methods"}
                    ]
                }
            ]
        });

        let r1 = harness
            .router
            .clone()
            .oneshot(authed_json_request(Method::POST, "/v1/ingest/batch", ALICE_TOKEN, req_v1))
            .await
            .unwrap();
        assert_eq!(r1.status(), StatusCode::OK);
        let b1 = body_json(r1).await;
        assert_eq!(b1["files"][0]["drawers_written"], 2u64);

        // Modified: 1 chunk only, different content_hash
        let req_v2 = json!({
            "wing": "wing_change",
            "repo_id": "github.com/acme/change-test",
            "files": [
                {
                    "relative_path": "src/lib.rs",
                    "content_hash": "hash-v2",
                    "chunks": [
                        {"chunk_index": 0, "room": "backend",
                         "text": "library initialization setup configuration"}
                    ]
                }
            ]
        });

        let r2 = harness
            .router
            .clone()
            .oneshot(authed_json_request(Method::POST, "/v1/ingest/batch", ALICE_TOKEN, req_v2))
            .await
            .unwrap();
        assert_eq!(r2.status(), StatusCode::OK);
        let b2 = body_json(r2).await;
        assert_eq!(b2["files"][0]["status"], "ingested");
        assert_eq!(b2["files"][0]["drawers_written"], 1u64);

        // Only 1 drawer should remain
        let list_resp = harness
            .router
            .clone()
            .oneshot(authed_get("/v1/drawers?wing=wing_change", ALICE_TOKEN))
            .await
            .unwrap();
        let list_body = body_json(list_resp).await;
        let drawers = list_body["drawers"].as_array().unwrap();
        assert_eq!(drawers.len(), 1, "dropped chunk should be gone");
    }

    /// Diary wing → 422.
    #[tokio::test]
    async fn ingest_batch_diary_wing_rejected() {
        let harness = make_harness().await;
        let resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/ingest/batch",
                ALICE_TOKEN,
                json!({
                    "wing": "wing_agents",
                    "repo_id": "github.com/acme/repo",
                    "files": [
                        {
                            "relative_path": "f.rs",
                            "content_hash": "ch",
                            "chunks": [
                                {"chunk_index": 0, "room": "general", "text": "some text"}
                            ]
                        }
                    ]
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = body_json(resp).await;
        assert_eq!(body["code"], "diary_not_federated");
    }

    /// Diary chunk room → 422.
    #[tokio::test]
    async fn ingest_batch_diary_chunk_room_rejected() {
        let harness = make_harness().await;
        let resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/ingest/batch",
                ALICE_TOKEN,
                json!({
                    "wing": "wing_project",
                    "repo_id": "github.com/acme/repo",
                    "files": [
                        {
                            "relative_path": "f.rs",
                            "content_hash": "ch",
                            "chunks": [
                                {"chunk_index": 0, "room": "diary", "text": "diary text"}
                            ]
                        }
                    ]
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = body_json(resp).await;
        assert_eq!(body["code"], "diary_not_federated");
    }

    /// Empty files array → 400.
    #[tokio::test]
    async fn ingest_batch_empty_files_returns_400() {
        let harness = make_harness().await;
        let resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/ingest/batch",
                ALICE_TOKEN,
                json!({"wing": "wing_x", "repo_id": "github.com/acme/r", "files": []}),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    /// Missing token → 401.
    #[tokio::test]
    async fn ingest_batch_without_token_returns_401() {
        let harness = make_harness().await;
        let req = Request::builder()
            .method(Method::POST)
            .uri("/v1/ingest/batch")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "wing": "wing_x",
                    "repo_id": "github.com/acme/r",
                    "files": []
                }))
                .unwrap(),
            ))
            .unwrap();
        let resp = harness.router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    /// byte_end < byte_start → file result "failed" (200 with per-file error).
    #[tokio::test]
    async fn ingest_batch_invalid_byte_range_fails_file_not_request() {
        let harness = make_harness().await;
        let resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/ingest/batch",
                ALICE_TOKEN,
                json!({
                    "wing": "wing_range",
                    "repo_id": "github.com/acme/repo",
                    "files": [
                        {
                            "relative_path": "src/bad.rs",
                            "content_hash": "ch-bad",
                            "file_hash": "fh-bad",
                            "chunks": [
                                {
                                    "chunk_index": 0, "room": "backend",
                                    "text": "some code here",
                                    "byte_start": 10, "byte_end": 5,  // invalid: end < start
                                    "line_start": 1, "line_end": 1
                                }
                            ]
                        }
                    ]
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "overall request should be 200");
        let body = body_json(resp).await;
        let files = body["files"].as_array().unwrap();
        assert_eq!(files[0]["status"], "failed");
        assert!(files[0]["error"].as_str().unwrap().contains("byte_end"));
    }

    /// line_end < line_start → file result "failed" (200 with per-file error).
    #[tokio::test]
    async fn ingest_batch_invalid_line_range_fails_file() {
        let harness = make_harness().await;
        let resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/ingest/batch",
                ALICE_TOKEN,
                json!({
                    "wing": "wing_lines",
                    "repo_id": "github.com/acme/repo",
                    "files": [
                        {
                            "relative_path": "src/bad.rs",
                            "content_hash": "ch-bad",
                            "file_hash": "fh-bad",
                            "chunks": [
                                {
                                    "chunk_index": 0, "room": "backend",
                                    "text": "some code here",
                                    "byte_start": 0, "byte_end": 14,
                                    "line_start": 9, "line_end": 3  // invalid: end < start
                                }
                            ]
                        }
                    ]
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "overall request should be 200");
        let body = body_json(resp).await;
        let files = body["files"].as_array().unwrap();
        assert_eq!(files[0]["status"], "failed");
        assert!(files[0]["error"].as_str().unwrap().contains("line_end"));
    }

    /// Checkout-mapped wing: file_hash matches the file's bytes → search result is non-stale.
    #[tokio::test]
    async fn ingest_batch_checkout_mapped_resolves_non_stale() {
        use mempalace_core::hash_bytes;
        use std::collections::BTreeMap;

        // Write a real file into a tempdir
        let checkout_dir = TempDir::new().unwrap();
        let file_content = b"fn authenticate(user: &str, pass: &str) -> bool { true }";
        let rel_path = "src/auth.rs";
        std::fs::create_dir_all(checkout_dir.path().join("src")).unwrap();
        std::fs::write(checkout_dir.path().join(rel_path), file_content).unwrap();

        let file_hash = hash_bytes(file_content);
        let byte_start: u64 = 0;
        let byte_end: u64 = file_content.len() as u64;

        let wing = "wing_checkout";
        let mut checkouts = BTreeMap::new();
        checkouts.insert(wing.to_owned(), checkout_dir.path().to_path_buf());
        let harness = make_harness_with_checkouts(checkouts).await;

        let resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/ingest/batch",
                ALICE_TOKEN,
                json!({
                    "wing": wing,
                    "repo_id": "github.com/acme/checkout-test",
                    "commit_hash": "deadbeef",
                    "files": [
                        {
                            "relative_path": rel_path,
                            "content_hash": "ch-auth-checkout",
                            "file_hash": file_hash,
                            "chunks": [
                                {
                                    "chunk_index": 0,
                                    "room": "backend",
                                    "text": std::str::from_utf8(file_content).unwrap(),
                                    "byte_start": byte_start,
                                    "byte_end": byte_end,
                                    "line_start": 1,
                                    "line_end": 1
                                }
                            ]
                        }
                    ]
                }),
            ))
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["files"][0]["status"], "ingested");
        // No missing-checkout warning
        assert!(body["warnings"].as_array().unwrap().is_empty(), "should have no warnings");

        // Search should find the drawer and it should NOT be stale
        let search_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers/search",
                ALICE_TOKEN,
                json!({"query": "authenticate user password", "limit": 5}),
            ))
            .await
            .unwrap();
        assert_eq!(search_resp.status(), StatusCode::OK);
        let search_body = body_json(search_resp).await;
        let results = search_body["results"].as_array().unwrap();
        assert!(!results.is_empty(), "search should find the ingested drawer");
        // stale field absent or false means not stale
        let stale = results[0].get("stale").and_then(|v| v.as_bool()).unwrap_or(false);
        assert!(!stale, "locator should resolve non-stale with matching file hash");
    }

    #[tokio::test]
    async fn ingest_batch_checkout_mapped_rejects_file_hash_mismatch() {
        use std::collections::BTreeMap;

        let checkout_dir = TempDir::new().unwrap();
        let rel_path = "src/secret.rs";
        std::fs::create_dir_all(checkout_dir.path().join("src")).unwrap();
        std::fs::write(checkout_dir.path().join(rel_path), b"server side secret bytes").unwrap();

        let wing = "wing_checkout_mismatch";
        let mut checkouts = BTreeMap::new();
        checkouts.insert(wing.to_owned(), checkout_dir.path().to_path_buf());
        let harness = make_harness_with_checkouts(checkouts).await;

        let resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/ingest/batch",
                ALICE_TOKEN,
                json!({
                    "wing": wing,
                    "repo_id": "github.com/acme/checkout-test",
                    "files": [
                        {
                            "relative_path": rel_path,
                            "content_hash": "ch-secret-checkout",
                            "file_hash": "deadbeef",
                            "chunks": [
                                {
                                    "chunk_index": 0,
                                    "room": "backend",
                                    "text": "client supplied benign text",
                                    "byte_start": 0,
                                    "byte_end": 23,
                                    "line_start": 1,
                                    "line_end": 1
                                }
                            ]
                        }
                    ]
                }),
            ))
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["files"][0]["status"], "failed");
        assert!(body["files"][0]["error"].as_str().unwrap().contains("file_hash does not match"));
    }

    /// Unmapped wing: locator rows with empty resolve_root → search returns stale.
    #[tokio::test]
    async fn ingest_batch_unmapped_wing_produces_stale_and_warning() {
        // No checkouts configured — use default harness
        let harness = make_harness().await;

        let resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/ingest/batch",
                ALICE_TOKEN,
                json!({
                    "wing": "wing_unmapped",
                    "repo_id": "github.com/acme/unmapped",
                    "files": [
                        {
                            "relative_path": "src/stale.rs",
                            "content_hash": "ch-stale",
                            "file_hash": "some-file-hash-that-wont-match",
                            "chunks": [
                                {
                                    "chunk_index": 0,
                                    "room": "backend",
                                    "text": "stale locator row from unmapped wing",
                                    "byte_start": 0, "byte_end": 36,
                                    "line_start": 1, "line_end": 1
                                }
                            ]
                        }
                    ]
                }),
            ))
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["files"][0]["status"], "ingested");

        let warnings = body["warnings"].as_array().unwrap();
        assert!(!warnings.is_empty(), "should warn about missing checkout");
        assert!(warnings[0].as_str().unwrap().contains("wing_unmapped"));

        // Search should find it but stale=true
        let search_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/drawers/search",
                ALICE_TOKEN,
                json!({"query": "stale locator row unmapped", "limit": 5}),
            ))
            .await
            .unwrap();
        assert_eq!(search_resp.status(), StatusCode::OK);
        let search_body = body_json(search_resp).await;
        let results = search_body["results"].as_array().unwrap();
        assert!(!results.is_empty(), "search must find the drawer");
        let stale = results[0].get("stale").and_then(|v| v.as_bool()).unwrap_or(false);
        assert!(stale, "result from unmapped wing must be stale=true");
    }

    /// Path traversal / absolute / backslash relative_path → per-file "failed".
    #[tokio::test]
    async fn ingest_batch_rejects_unsafe_relative_paths() {
        let harness = make_harness().await;
        let resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/ingest/batch",
                ALICE_TOKEN,
                json!({
                    "wing": "wing_paths",
                    "repo_id": "github.com/acme/paths",
                    "files": [
                        {"relative_path": "../../etc/passwd", "content_hash": "c1",
                         "chunks": [{"chunk_index": 0, "room": "general", "text": "x y z"}]},
                        {"relative_path": "/abs/path.rs", "content_hash": "c2",
                         "chunks": [{"chunk_index": 0, "room": "general", "text": "x y z"}]},
                        {"relative_path": "src\\win.rs", "content_hash": "c3",
                         "chunks": [{"chunk_index": 0, "room": "general", "text": "x y z"}]},
                        {"relative_path": ".git/config", "content_hash": "c5",
                         "chunks": [{"chunk_index": 0, "room": "general", "text": "x y z"}]},
                        {"relative_path": ".env", "content_hash": "c6",
                         "chunks": [{"chunk_index": 0, "room": "general", "text": "x y z"}]},
                        {"relative_path": "src/ok.rs", "content_hash": "c4",
                         "chunks": [{"chunk_index": 0, "room": "general",
                                     "text": "perfectly safe path content"}]}
                    ]
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let files = body["files"].as_array().unwrap();
        assert_eq!(files[0]["status"], "failed");
        assert_eq!(files[1]["status"], "failed");
        assert_eq!(files[2]["status"], "failed");
        assert_eq!(files[3]["status"], "failed", ".git path must be rejected");
        assert_eq!(files[4]["status"], "failed", ".env path must be rejected");
        assert_eq!(files[5]["status"], "ingested", "safe path must still ingest");
    }

    /// Duplicate chunk_index within one file → per-file "failed".
    #[tokio::test]
    async fn ingest_batch_rejects_duplicate_chunk_index() {
        let harness = make_harness().await;
        let resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/ingest/batch",
                ALICE_TOKEN,
                json!({
                    "wing": "wing_dup",
                    "repo_id": "github.com/acme/dup",
                    "files": [
                        {"relative_path": "src/dup.rs", "content_hash": "c1",
                         "chunks": [
                            {"chunk_index": 0, "room": "general", "text": "first chunk text"},
                            {"chunk_index": 0, "room": "general", "text": "second chunk text"}
                         ]}
                    ]
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["files"][0]["status"], "failed");
        assert!(body["files"][0]["error"].as_str().unwrap().contains("duplicate chunk_index"));
    }

    /// info endpoint now includes "ingest" capability.
    #[tokio::test]
    async fn info_includes_ingest_capability() {
        let harness = make_harness().await;
        let resp = harness.router.oneshot(authed_get("/v1/info", ALICE_TOKEN)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let caps = body["capabilities"].as_array().unwrap();
        let cap_strings: Vec<&str> = caps.iter().filter_map(|v| v.as_str()).collect();
        assert!(cap_strings.contains(&"ingest"), "capabilities must include 'ingest'");
    }

    #[tokio::test]
    async fn info_returns_maintenance_enabled_and_null_last_run() {
        let harness = make_harness().await;
        let resp = harness.router.oneshot(authed_get("/v1/info", ALICE_TOKEN)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["maintenance_enabled"], true);
        assert_eq!(body["maintenance_background_enabled"], true);
        assert_eq!(body["maintenance_idle_secs"], 300u64);
        assert!(body["maintenance_last_run"].is_null());
        // Status should be idle or one of the post-run states (the startup
        // check may have completed by now).
        let status = &body["maintenance_status"];
        assert!(
            status.is_string() || status.is_object(),
            "maintenance_status must be a string (unit variant) or object (struct variant): {status}",
        );
    }

    #[tokio::test]
    async fn info_returns_maintenance_disabled_fields() {
        let harness = make_harness_with_maintenance(false).await;
        let resp = harness.router.oneshot(authed_get("/v1/info", ALICE_TOKEN)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["maintenance_enabled"], false);
        assert_eq!(body["maintenance_background_enabled"], true);
        assert_eq!(body["maintenance_idle_secs"], 300u64);
        assert!(body["maintenance_last_run"].is_null());
        assert_eq!(body["maintenance_status"], "disabled");
    }

    #[tokio::test]
    async fn info_reports_manual_only_maintenance() {
        let harness = make_harness_with_maintenance_config(true, false).await;
        let resp = harness.router.oneshot(authed_get("/v1/info", ALICE_TOKEN)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["maintenance_enabled"], true);
        assert_eq!(body["maintenance_background_enabled"], false);
        assert_eq!(body["maintenance_status"], "idle");
    }

    #[tokio::test]
    async fn info_returns_maintenance_status_disabled() {
        let harness = make_harness_with_maintenance(false).await;
        let resp = harness.router.oneshot(authed_get("/v1/info", ALICE_TOKEN)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["maintenance_status"], "disabled");
    }

    #[tokio::test]
    async fn info_returns_maintenance_status_completed_after_startup() {
        let harness = make_harness().await;
        // Wait for the startup check to complete (up to 5 s).
        for _ in 0..100 {
            let resp =
                harness.router.clone().oneshot(authed_get("/v1/info", ALICE_TOKEN)).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let body = body_json(resp).await;
            let status = &body["maintenance_status"];
            // startup check should transition from "idle" to some post-run state.
            if status.is_object() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!("maintenance did not transition from idle after startup check");
    }

    #[tokio::test]
    async fn info_returns_maintenance_status_skipped_not_idle_when_busy() {
        // Use a very short idle_secs so the startup check completes quickly,
        // then keep sending requests to ensure the background check is skipped.
        let short_idle =
            MaintenanceRuntimeConfig { idle_secs: 1, ..MaintenanceRuntimeConfig::defaults() };
        let harness = build_with_maintenance(short_idle).await;

        // Wait for startup check to finish.
        for _ in 0..50 {
            if harness.state.last_maintenance_status.lock().unwrap().is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // Directly set the status to simulate the "skipped (activity)" state
        // that the scheduler writes when it detects recent activity.
        *harness.state.maintenance_status.lock().unwrap() =
            MaintenanceStatus::Skipped { reason: FedMaintenanceSkipReason::NotIdle };

        let resp = harness.router.oneshot(authed_get("/v1/info", ALICE_TOKEN)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let status = &body["maintenance_status"];
        assert_eq!(status["skipped"]["reason"], "not_idle");
    }

    #[tokio::test]
    async fn info_returns_maintenance_status_aborted_concurrent() {
        let harness = make_harness().await;
        *harness.state.maintenance_status.lock().unwrap() =
            MaintenanceStatus::Aborted { reason: FedMaintenanceAbortReason::ConcurrentRun };

        let resp = harness.router.oneshot(authed_get("/v1/info", ALICE_TOKEN)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let status = &body["maintenance_status"];
        assert_eq!(status["aborted"]["reason"], "concurrent_run");
    }

    #[tokio::test]
    async fn info_returns_maintenance_status_failed() {
        let harness = make_harness().await;
        *harness.state.maintenance_status.lock().unwrap() =
            MaintenanceStatus::Failed { message: "simulated error".into() };

        let resp = harness.router.oneshot(authed_get("/v1/info", ALICE_TOKEN)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let status = &body["maintenance_status"];
        assert_eq!(status["failed"]["message"], "simulated error");
    }

    #[tokio::test]
    async fn info_returns_maintenance_status_completed_success() {
        let harness = make_harness().await;
        *harness.state.maintenance_status.lock().unwrap() =
            MaintenanceStatus::Completed { status: FedMaintenanceRunStatus::Success };

        let resp = harness.router.oneshot(authed_get("/v1/info", ALICE_TOKEN)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let status = &body["maintenance_status"];
        assert_eq!(status["completed"]["status"], "success");
    }

    #[tokio::test]
    async fn info_returns_maintenance_status_completed_partial() {
        let harness = make_harness().await;
        *harness.state.maintenance_status.lock().unwrap() =
            MaintenanceStatus::Completed { status: FedMaintenanceRunStatus::Partial };

        let resp = harness.router.oneshot(authed_get("/v1/info", ALICE_TOKEN)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let status = &body["maintenance_status"];
        assert_eq!(status["completed"]["status"], "partial");
    }

    #[tokio::test]
    async fn info_returns_maintenance_status_completed_failure() {
        let harness = make_harness().await;
        *harness.state.maintenance_status.lock().unwrap() =
            MaintenanceStatus::Completed { status: FedMaintenanceRunStatus::Failure };

        let resp = harness.router.oneshot(authed_get("/v1/info", ALICE_TOKEN)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let status = &body["maintenance_status"];
        assert_eq!(status["completed"]["status"], "failure");
    }

    #[tokio::test]
    async fn info_returns_maintenance_status_running() {
        let harness = make_harness().await;
        *harness.state.maintenance_status.lock().unwrap() = MaintenanceStatus::Running;

        let resp = harness.router.oneshot(authed_get("/v1/info", ALICE_TOKEN)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let status = &body["maintenance_status"];
        assert_eq!(status, "running");
    }

    // ─── 10. Scheduler ────────────────────────────────────────────────────────

    /// Builds a router returning the state handle for scheduler introspection.
    async fn build_with_maintenance(maintenance: MaintenanceRuntimeConfig) -> Harness {
        let tempdir = TempDir::new().unwrap();
        let palace_path = tempdir.path().join("palace");

        let token_file = tempdir.path().join("tokens.json");
        std::fs::write(
            &token_file,
            serde_json::to_string(&serde_json::json!([
                {"token": ALICE_TOKEN, "name": "alice", "enabled": true},
            ]))
            .unwrap(),
        )
        .unwrap();
        restrict_token_file(&token_file);

        let config = MempalaceConfig {
            schema_version: 1,
            collection_name: "mempalace_drawers".to_owned(),
            palace_path,
            embedding_profile: EmbeddingProfile::Balanced,
            low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
            server: ServerRuntimeConfig {
                bind: "127.0.0.1:8765".parse().unwrap(),
                token_file,
                checkouts: std::collections::BTreeMap::new(),
            },
            federation: FederationRuntimeConfig::default(),
            maintenance,
        };
        let tokens = TokenRegistry::load(config.server.token_file.clone()).unwrap();
        let provider = DeterministicStubProvider::new(EmbeddingProfile::Balanced);
        let (router, state) = build_router(config, provider, tokens).await.unwrap();
        Harness { router, state, _tempdir: tempdir }
    }

    #[tokio::test]
    async fn maintenance_startup_check_records_status() {
        let harness = build_with_maintenance(MaintenanceRuntimeConfig::defaults()).await;
        // The background task runs an immediate startup check.  Poll until
        // the status is populated (should complete within a few hundred ms).
        for _ in 0..50 {
            if harness.state.last_maintenance_status.lock().unwrap().is_some() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!("startup maintenance check did not record status within 2.5 s");
    }

    #[tokio::test]
    async fn maintenance_disabled_creates_no_scheduler() {
        let disabled =
            MaintenanceRuntimeConfig { enabled: false, ..MaintenanceRuntimeConfig::defaults() };
        let harness = build_with_maintenance(disabled).await;
        // Wait long enough that even a startup check would have run.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let status = harness.state.last_maintenance_status.lock().unwrap().take();
        assert!(status.is_none(), "disabled maintenance must not record status");
    }

    #[tokio::test]
    async fn manual_maintenance_creates_no_scheduler() {
        let manual = MaintenanceRuntimeConfig {
            background_enabled: false,
            ..MaintenanceRuntimeConfig::defaults()
        };
        let harness = build_with_maintenance(manual).await;
        // A manual-only configuration must not perform the startup check.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let status = harness.state.last_maintenance_status.lock().unwrap().take();
        assert!(status.is_none(), "manual maintenance must not record status");
    }

    #[tokio::test]
    async fn maintenance_health_check_does_not_signal_write_activity() {
        let harness = make_harness().await;
        let request =
            Request::builder().method(Method::GET).uri("/v1/health").body(Body::empty()).unwrap();
        let _ = harness.router.clone().oneshot(request).await.unwrap();
        assert!(
            !harness.state.storage.take_activity_signal(),
            "read-only health checks must not postpone maintenance",
        );
    }

    #[tokio::test]
    async fn maintenance_runs_after_one_idle_period() {
        use mempalace_storage::{MaintenanceOutcome, MaintenanceSkipReason};

        let short_idle =
            MaintenanceRuntimeConfig { idle_secs: 1, ..MaintenanceRuntimeConfig::defaults() };
        let harness = build_with_maintenance(short_idle).await;

        // Wait for the startup check to complete.
        for _ in 0..50 {
            if harness.state.last_maintenance_status.lock().unwrap().is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // Clear the startup status so we can detect the next run.
        *harness.state.last_maintenance_status.lock().unwrap() = None;

        // Simulate a completed write to establish the idle period.
        harness.state.storage.signal_activity();

        // The scheduler phase begins at startup, so a write immediately after
        // the startup pass may wait almost two full intervals. Poll rather
        // than assuming a particular phase alignment.
        let mut status = None;
        for _ in 0..70 {
            status = harness.state.last_maintenance_status.lock().unwrap().take();
            if status.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(status.is_some(), "maintenance should have run after one idle period");
        let summary = status.unwrap();
        let stale_signal_skipped = summary.tier_results.iter().any(|r| {
            matches!(
                r.outcome,
                MaintenanceOutcome::Skipped { reason: MaintenanceSkipReason::NotIdle }
            )
        });
        assert!(
            !stale_signal_skipped,
            "maintenance must not be skipped by a stale activity signal",
        );
    }

    #[tokio::test]
    async fn info_shows_maintenance_last_run_after_startup_check() {
        let harness = make_harness().await;
        // Wait for the startup check to record a last_maintenance_status.
        for _ in 0..100 {
            if harness.state.last_maintenance_status.lock().unwrap().is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        let resp = harness.router.oneshot(authed_get("/v1/info", ALICE_TOKEN)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        // After a startup check, maintenance_last_run should be a non-null object.
        assert!(
            body["maintenance_last_run"].is_object(),
            "maintenance_last_run must be an object after startup: {}",
            body,
        );
        assert!(
            body["maintenance_last_run"]["run_id"].is_number(),
            "run_id must be present in maintenance_last_run",
        );
        assert!(
            body["maintenance_last_run"]["status"].is_string(),
            "status must be present in maintenance_last_run",
        );
    }

    #[tokio::test]
    async fn info_shows_maintenance_tier_results_in_last_run() {
        let harness = make_harness().await;
        // Wait for the startup check to record a last_maintenance_status.
        for _ in 0..100 {
            if harness.state.last_maintenance_status.lock().unwrap().is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        let resp = harness.router.oneshot(authed_get("/v1/info", ALICE_TOKEN)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let last_run = &body["maintenance_last_run"];
        let tier_results = last_run["tier_results"].as_array();
        assert!(
            tier_results.is_some() && !tier_results.unwrap().is_empty(),
            "maintenance_last_run must contain tier_results after startup: {}",
            body,
        );
    }

    // ─── 11. Hub-only scheduling ─────────────────────────────────────────────

    #[tokio::test]
    async fn scheduler_task_spawned_by_build_router_not_by_engine_open() {
        // The background scheduler is only spawned by build_router (the HTTP
        // hub path).  Opening StorageEngine directly (CLI/API path) must NOT
        // start periodic maintenance.
        let tempdir = TempDir::new().unwrap();
        let engine = mempalace_storage::StorageEngine::open(
            tempdir.path().join("palace"),
            EmbeddingProfile::Balanced,
        )
        .await
        .unwrap();

        // Wait long enough for a scheduler to have run if one were spawned.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // Manual pass works correctly — no background scheduler is running
        // because engine.open() does not spawn one.
        let settings = mempalace_storage::MaintenanceSettings {
            enabled: true,
            idle_secs: 0,
            ..mempalace_storage::MaintenanceSettings::default()
        };
        let summary = engine.run_maintenance(&settings).await.unwrap();
        assert_eq!(summary.tier_results.len(), 3, "manual maintenance should run all tiers");

        // Now confirm that build_router DOES spawn a scheduler by checking
        // that the startup maintenance check populates last_maintenance_status.
        let harness = build_with_maintenance(MaintenanceRuntimeConfig::defaults()).await;
        for _ in 0..50 {
            if harness.state.last_maintenance_status.lock().unwrap().is_some() {
                return; // scheduler ran the startup check
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!("build_router should spawn a scheduler that runs the startup check");
    }

    // ─── Coordination over the wire (issue #102 Stage 3) ─────────────────────

    #[tokio::test]
    async fn info_advertises_coordination_capability() {
        let harness = make_harness().await;
        let resp = harness.router.clone().oneshot(authed_get("/v1/info", ALICE_TOKEN)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let capabilities = body["capabilities"].as_array().unwrap();
        assert!(capabilities.iter().any(|c| c == "coordination"));
    }

    /// Creates a coordination task in `wing` via `token` and returns its id.
    async fn create_task(harness: &Harness, token: &str, wing: &str, key: &str) -> String {
        let resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/tasks",
                token,
                json!({
                    "title": "t",
                    "description": "d",
                    "wing": wing,
                    "idempotency_key": key,
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "task creation should succeed");
        body_json(resp).await["task_id"].as_str().unwrap().to_owned()
    }

    /// Seeds a task in `wing_beta`, plus a message/artifact/result attached to
    /// it, via the unrestricted `alice` token — for the Group B 404-masking
    /// tests below, where `coord_alpha` (scoped to `wing_alpha` only) must
    /// never learn these exist. Returns `(task_id, message_id, artifact_id,
    /// result_id)`.
    async fn seed_coordination_wing_beta_fixtures(
        harness: &Harness,
    ) -> (String, String, String, String) {
        let task_id = create_task(harness, ALICE_TOKEN, "wing_beta", "beta-masking-task").await;

        let message_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/messages",
                ALICE_TOKEN,
                json!({
                    "task_id": task_id, "recipient": "someone", "kind": "status",
                    "payload": {}, "idempotency_key": "beta-message-1",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(message_resp.status(), StatusCode::OK);
        let message_id = body_json(message_resp).await["message_id"].as_str().unwrap().to_owned();

        let artifact_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/artifacts",
                ALICE_TOKEN,
                json!({
                    "task_id": task_id, "role": "output", "media_type": "text/plain",
                    "content": "beta artifact", "idempotency_key": "beta-artifact-1",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(artifact_resp.status(), StatusCode::OK);
        let artifact_id =
            body_json(artifact_resp).await["artifact_id"].as_str().unwrap().to_owned();

        let result_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/results",
                ALICE_TOKEN,
                json!({"task_id": task_id, "payload": {}, "idempotency_key": "beta-result-1"}),
            ))
            .await
            .unwrap();
        assert_eq!(result_resp.status(), StatusCode::OK);
        let result_id = body_json(result_resp).await["result_id"].as_str().unwrap().to_owned();

        (task_id, message_id, artifact_id, result_id)
    }

    #[tokio::test]
    async fn coordination_task_lifecycle_create_claim_renew_transition() {
        let harness = make_harness().await;

        let create_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/tasks",
                COORD_ALPHA_TOKEN,
                json!({
                    "title": "Ship the thing",
                    "description": "do it well",
                    "wing": "wing_alpha",
                    "idempotency_key": "lifecycle-create-1",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(create_resp.status(), StatusCode::OK);
        let task = body_json(create_resp).await;
        assert_eq!(task["state"], "pending");
        assert_eq!(task["revision"], 0);
        assert_eq!(task["wing"], "wing_alpha");
        assert_eq!(task["created_by"], "coord_alpha");
        let task_id = task["task_id"].as_str().unwrap().to_owned();

        let get_resp = harness
            .router
            .clone()
            .oneshot(authed_get(&format!("/v1/coordination/tasks/{task_id}"), COORD_ALPHA_TOKEN))
            .await
            .unwrap();
        assert_eq!(get_resp.status(), StatusCode::OK);
        assert_eq!(body_json(get_resp).await["task_id"], task_id);

        let claim_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                &format!("/v1/coordination/tasks/{task_id}/claim"),
                COORD_ALPHA_TOKEN,
                json!({"expected_revision": 0, "lease_seconds": 300}),
            ))
            .await
            .unwrap();
        assert_eq!(claim_resp.status(), StatusCode::OK);
        let claimed = body_json(claim_resp).await;
        assert_eq!(claimed["state"], "running");
        assert_eq!(claimed["revision"], 1);
        assert_eq!(claimed["owner"], "coord_alpha");

        let renew_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                &format!("/v1/coordination/tasks/{task_id}/renew"),
                COORD_ALPHA_TOKEN,
                json!({"expected_revision": 1, "lease_seconds": 600}),
            ))
            .await
            .unwrap();
        assert_eq!(renew_resp.status(), StatusCode::OK);
        let renewed = body_json(renew_resp).await;
        assert_eq!(renewed["revision"], 2);
        assert_eq!(renewed["state"], "running");

        let transition_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                &format!("/v1/coordination/tasks/{task_id}/transition"),
                COORD_ALPHA_TOKEN,
                json!({"expected_revision": 2, "state": "completed"}),
            ))
            .await
            .unwrap();
        assert_eq!(transition_resp.status(), StatusCode::OK);
        let completed = body_json(transition_resp).await;
        assert_eq!(completed["state"], "completed");
        assert_eq!(completed["revision"], 3);
        assert!(completed["owner"].is_null());
    }

    #[tokio::test]
    async fn coordination_message_send_get_ack_and_inbox() {
        let harness = make_harness().await;
        let task_id =
            create_task(&harness, COORD_ALPHA_TOKEN, "wing_alpha", "message-lifecycle-task").await;

        let send_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/messages",
                COORD_ALPHA_TOKEN,
                json!({
                    "task_id": task_id,
                    "recipient": "coord_alpha",
                    "kind": "status",
                    "payload": {"progress": "started"},
                    "idempotency_key": "message-1",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(send_resp.status(), StatusCode::OK);
        let message = body_json(send_resp).await;
        assert_eq!(message["sender"], "coord_alpha");
        assert_eq!(message["recipient"], "coord_alpha");
        assert!(message["acknowledged_at"].is_null());
        let message_id = message["message_id"].as_str().unwrap().to_owned();

        let get_resp = harness
            .router
            .clone()
            .oneshot(authed_get(
                &format!("/v1/coordination/messages/{message_id}"),
                COORD_ALPHA_TOKEN,
            ))
            .await
            .unwrap();
        assert_eq!(get_resp.status(), StatusCode::OK);
        assert_eq!(body_json(get_resp).await["message_id"], message_id);

        let ack_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                &format!("/v1/coordination/messages/{message_id}/ack"),
                COORD_ALPHA_TOKEN,
                json!({}),
            ))
            .await
            .unwrap();
        assert_eq!(ack_resp.status(), StatusCode::OK);
        let acked = body_json(ack_resp).await;
        assert_eq!(acked["acknowledged_by"], "coord_alpha");
        assert!(acked["acknowledged_at"].is_string());

        let inbox_resp = harness
            .router
            .clone()
            .oneshot(authed_get("/v1/coordination/inbox?recipient=coord_alpha", COORD_ALPHA_TOKEN))
            .await
            .unwrap();
        assert_eq!(inbox_resp.status(), StatusCode::OK);
        let inbox = body_json(inbox_resp).await;
        let messages = inbox["messages"].as_array().unwrap();
        assert!(messages.iter().any(|m| m["message_id"] == message_id
            && m["acknowledged_by"] == "coord_alpha"));
    }

    /// Regression for Codex finding 3832912248: a federated acknowledgement
    /// must succeed when the acknowledging token's identity differs from the
    /// message's recipient, as long as the claimed `actor` equals that
    /// recipient exactly. `COORD_WIDE_TOKEN`'s identity is `coord_wide`; the
    /// message here is addressed to `worker-b`, a name that is neither
    /// `coord_wide` nor any other configured token's identity, so this only
    /// passes if `route_coordination_message_ack` stops running the ack
    /// actor through `resolve_coordination_actor`'s identity-prefixing rule
    /// when the claim already matches the recipient. See
    /// `resolve_ack_actor`.
    #[tokio::test]
    async fn coordination_message_ack_succeeds_when_claim_matches_recipient_not_identity() {
        let harness = make_harness().await;
        let task_id =
            create_task(&harness, COORD_WIDE_TOKEN, "wing_alpha", "ack-identity-mismatch-task")
                .await;

        let send_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/messages",
                COORD_WIDE_TOKEN,
                json!({
                    "task_id": task_id,
                    "recipient": "worker-b",
                    "kind": "status",
                    "payload": {"progress": "started"},
                    "idempotency_key": "ack-identity-mismatch-message",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(send_resp.status(), StatusCode::OK);
        let message = body_json(send_resp).await;
        assert_eq!(message["recipient"], "worker-b");
        let message_id = message["message_id"].as_str().unwrap().to_owned();

        // The authenticated token identity (`coord_wide`) genuinely differs
        // from the claimed actor (`worker-b`) — the scenario the finding
        // requires. Before the fix this was mangled to
        // `coord_wide:worker-b`, which never equals the stored recipient, so
        // the ack was rejected as a coordination conflict.
        let ack_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                &format!("/v1/coordination/messages/{message_id}/ack"),
                COORD_WIDE_TOKEN,
                json!({"actor": "worker-b"}),
            ))
            .await
            .unwrap();
        assert_eq!(ack_resp.status(), StatusCode::OK);
        let acked = body_json(ack_resp).await;
        assert_eq!(acked["acknowledged_by"], "worker-b");
        assert!(acked["acknowledged_at"].is_string());
    }

    /// Companion to the regression above: a claimed ack actor that is
    /// neither the caller's own token identity nor the message's recipient
    /// must still be rejected — proving the fix narrows the bypass to an
    /// exact recipient match rather than removing `resolve_coordination_actor`'s
    /// identity-prefixing protection altogether. `coord_wide` claims to be
    /// `someone_else`, which is not `worker-b` (the actual recipient), so
    /// `resolve_ack_actor` must still route it through the ordinary
    /// prefixing rule (`coord_wide:someone_else`), which cannot equal the
    /// stored recipient either way.
    #[tokio::test]
    async fn coordination_message_ack_still_rejects_a_claim_impersonating_someone_else() {
        let harness = make_harness().await;
        let task_id =
            create_task(&harness, COORD_WIDE_TOKEN, "wing_alpha", "ack-impersonation-task").await;

        let send_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/messages",
                COORD_WIDE_TOKEN,
                json!({
                    "task_id": task_id,
                    "recipient": "worker-b",
                    "kind": "status",
                    "payload": {"progress": "started"},
                    "idempotency_key": "ack-impersonation-message",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(send_resp.status(), StatusCode::OK);
        let message_id = body_json(send_resp).await["message_id"].as_str().unwrap().to_owned();

        let ack_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                &format!("/v1/coordination/messages/{message_id}/ack"),
                COORD_WIDE_TOKEN,
                json!({"actor": "someone_else"}),
            ))
            .await
            .unwrap();
        assert_eq!(ack_resp.status(), StatusCode::CONFLICT);
        let body = body_json(ack_resp).await;
        assert_eq!(body["code"], "coordination_conflict");
    }

    #[tokio::test]
    async fn coordination_artifact_put_and_get() {
        let harness = make_harness().await;
        let task_id = create_task(&harness, COORD_ALPHA_TOKEN, "wing_alpha", "artifact-task").await;

        let put_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/artifacts",
                COORD_ALPHA_TOKEN,
                json!({
                    "task_id": task_id,
                    "role": "output",
                    "media_type": "text/plain",
                    "content": "artifact body",
                    "idempotency_key": "artifact-1",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(put_resp.status(), StatusCode::OK);
        let artifact = body_json(put_resp).await;
        assert_eq!(artifact["created_by"], "coord_alpha");
        assert_eq!(artifact["content"], "artifact body");
        let artifact_id = artifact["artifact_id"].as_str().unwrap().to_owned();

        let get_resp = harness
            .router
            .clone()
            .oneshot(authed_get(
                &format!("/v1/coordination/artifacts/{artifact_id}"),
                COORD_ALPHA_TOKEN,
            ))
            .await
            .unwrap();
        assert_eq!(get_resp.status(), StatusCode::OK);
        let fetched = body_json(get_resp).await;
        assert_eq!(fetched["artifact_id"], artifact_id);
        assert_eq!(fetched["content_hash"], artifact["content_hash"]);
    }

    #[tokio::test]
    async fn coordination_result_put_and_get() {
        let harness = make_harness().await;
        let task_id = create_task(&harness, COORD_ALPHA_TOKEN, "wing_alpha", "result-task").await;

        let put_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/results",
                COORD_ALPHA_TOKEN,
                json!({
                    "task_id": task_id,
                    "payload": {"score": 42},
                    "idempotency_key": "result-1",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(put_resp.status(), StatusCode::OK);
        let result = body_json(put_resp).await;
        assert_eq!(result["created_by"], "coord_alpha");
        assert_eq!(result["payload"]["score"], 42);
        let result_id = result["result_id"].as_str().unwrap().to_owned();

        let get_resp = harness
            .router
            .clone()
            .oneshot(authed_get(&format!("/v1/coordination/results/{result_id}"), COORD_ALPHA_TOKEN))
            .await
            .unwrap();
        assert_eq!(get_resp.status(), StatusCode::OK);
        assert_eq!(body_json(get_resp).await["result_id"], result_id);
    }

    #[tokio::test]
    async fn coordination_events_feed_pages_with_cursor() {
        let harness = make_harness().await;
        let task_id = create_task(&harness, COORD_ALPHA_TOKEN, "wing_alpha", "events-task").await;
        let claim_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                &format!("/v1/coordination/tasks/{task_id}/claim"),
                COORD_ALPHA_TOKEN,
                json!({"expected_revision": 0, "lease_seconds": 300}),
            ))
            .await
            .unwrap();
        assert_eq!(claim_resp.status(), StatusCode::OK);

        // Page through with limit=1, following next_cursor, collecting every
        // event for this task; the cursor is opaque, so it is only ever
        // round-tripped verbatim, never parsed.
        let mut seen = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let uri = match &cursor {
                Some(c) => format!(
                    "/v1/coordination/events?task_id={task_id}&limit=1&cursor={}",
                    urlencoded(c)
                ),
                None => format!("/v1/coordination/events?task_id={task_id}&limit=1"),
            };
            let resp =
                harness.router.clone().oneshot(authed_get(&uri, COORD_ALPHA_TOKEN)).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let page = body_json(resp).await;
            let events = page["events"].as_array().unwrap().clone();
            assert!(events.len() <= 1);
            seen.extend(events);
            let next = page["next_cursor"].as_str().map(str::to_owned);
            assert!(seen.len() <= 10, "paging should terminate well before this");
            match next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        assert_eq!(seen.len(), 2, "expected task_created + task_claimed");
        assert_eq!(seen[0]["event_type"], "task_created");
        assert_eq!(seen[1]["event_type"], "task_claimed");
    }

    #[tokio::test]
    async fn coordination_task_get_masks_invisible_wing_as_404() {
        let harness = make_harness().await;
        let (task_id, ..) = seed_coordination_wing_beta_fixtures(&harness).await;

        let resp = harness
            .router
            .clone()
            .oneshot(authed_get(&format!("/v1/coordination/tasks/{task_id}"), COORD_ALPHA_TOKEN))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert_eq!(body_json(resp).await["code"], "not_found");

        // A genuinely missing task gets the identical 404 shape — the caller
        // can never distinguish "does not exist" from "exists, wrong wing".
        let missing_resp = harness
            .router
            .clone()
            .oneshot(authed_get("/v1/coordination/tasks/task_missing", COORD_ALPHA_TOKEN))
            .await
            .unwrap();
        assert_eq!(missing_resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn coordination_message_get_masks_invisible_wing_as_404() {
        let harness = make_harness().await;
        let (_, message_id, ..) = seed_coordination_wing_beta_fixtures(&harness).await;

        let resp = harness
            .router
            .clone()
            .oneshot(authed_get(
                &format!("/v1/coordination/messages/{message_id}"),
                COORD_ALPHA_TOKEN,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn coordination_artifact_get_masks_invisible_wing_as_404() {
        let harness = make_harness().await;
        let (_, _, artifact_id, _) = seed_coordination_wing_beta_fixtures(&harness).await;

        let resp = harness
            .router
            .clone()
            .oneshot(authed_get(
                &format!("/v1/coordination/artifacts/{artifact_id}"),
                COORD_ALPHA_TOKEN,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn coordination_result_get_masks_invisible_wing_as_404() {
        let harness = make_harness().await;
        let (.., result_id) = seed_coordination_wing_beta_fixtures(&harness).await;

        let resp = harness
            .router
            .clone()
            .oneshot(authed_get(&format!("/v1/coordination/results/{result_id}"), COORD_ALPHA_TOKEN))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn coordination_actor_impersonation_is_prefixed_not_trusted() {
        let harness = make_harness().await;

        let resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/tasks",
                COORD_ALPHA_TOKEN,
                json!({
                    "title": "t", "description": "d", "wing": "wing_alpha",
                    "idempotency_key": "impersonation-1",
                    "created_by": "someone-else",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        // The authenticated identity wins: a claimed name that disagrees with
        // it is folded in as `{identity}:{claimed}`, never trusted verbatim —
        // a remote caller cannot impersonate a local actor.
        assert_eq!(body_json(resp).await["created_by"], "coord_alpha:someone-else");

        let resp2 = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/tasks",
                COORD_ALPHA_TOKEN,
                json!({
                    "title": "t", "description": "d", "wing": "wing_alpha",
                    "idempotency_key": "impersonation-2",
                    "created_by": "coord_alpha",
                }),
            ))
            .await
            .unwrap();
        // A claimed name equal to the identity is stored bare, not doubled up.
        assert_eq!(body_json(resp2).await["created_by"], "coord_alpha");
    }

    #[tokio::test]
    async fn coordination_stale_revision_conflict_reports_actual_revision() {
        let harness = make_harness().await;
        let task_id =
            create_task(&harness, COORD_ALPHA_TOKEN, "wing_alpha", "stale-revision-task").await;

        let first = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                &format!("/v1/coordination/tasks/{task_id}/claim"),
                COORD_ALPHA_TOKEN,
                json!({"expected_revision": 0, "lease_seconds": 300}),
            ))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(body_json(first).await["revision"], 1);

        // Renewing at the now-stale revision 0 is a revision conflict, not a
        // silent overwrite, and the body carries the real current revision
        // rather than requiring the client to re-fetch to find out.
        let stale = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                &format!("/v1/coordination/tasks/{task_id}/renew"),
                COORD_ALPHA_TOKEN,
                json!({"expected_revision": 0, "lease_seconds": 300}),
            ))
            .await
            .unwrap();
        assert_eq!(stale.status(), StatusCode::CONFLICT);
        let body = body_json(stale).await;
        assert_eq!(body["code"], "revision_conflict");
        assert_eq!(body["expected_revision"], 0);
        assert_eq!(body["actual_revision"], 1);
    }

    #[tokio::test]
    async fn coordination_claim_of_task_leased_by_another_worker_is_conflict() {
        let harness = make_harness().await;
        let task_id = create_task(&harness, COORD_ALPHA_TOKEN, "wing_alpha", "leased-task").await;

        let claim_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                &format!("/v1/coordination/tasks/{task_id}/claim"),
                COORD_ALPHA_TOKEN,
                json!({"expected_revision": 0, "lease_seconds": 300, "worker": "worker-a"}),
            ))
            .await
            .unwrap();
        assert_eq!(claim_resp.status(), StatusCode::OK);
        assert_eq!(body_json(claim_resp).await["revision"], 1);

        // A second worker claims at the now-*correct* revision — this is not
        // a stale-revision conflict, it is a live-lease conflict: worker-a's
        // 300s lease has not expired, so the wire shape carries no revision
        // pair (retrying with a fresher revision would not help).
        let second = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                &format!("/v1/coordination/tasks/{task_id}/claim"),
                COORD_ALPHA_TOKEN,
                json!({"expected_revision": 1, "lease_seconds": 300, "worker": "worker-b"}),
            ))
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::CONFLICT);
        let body = body_json(second).await;
        assert_eq!(body["code"], "coordination_conflict");
        assert!(body["expected_revision"].is_null());
        assert!(body["actual_revision"].is_null());
    }

    #[tokio::test]
    async fn coordination_events_feed_filters_by_wing_and_fails_closed() {
        let harness = make_harness().await;
        let alpha_task =
            create_task(&harness, COORD_ALPHA_TOKEN, "wing_alpha", "wing-filter-alpha").await;
        let _beta_task = create_task(&harness, ALICE_TOKEN, "wing_beta", "wing-filter-beta").await;

        let resp = harness
            .router
            .clone()
            .oneshot(authed_get("/v1/coordination/events?limit=100", COORD_ALPHA_TOKEN))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let page = body_json(resp).await;
        let events = page["events"].as_array().unwrap();
        assert!(!events.is_empty());
        // A token scoped to wing_alpha sees only wing_alpha's events, even
        // though wing_beta's task_created event exists in the same
        // underlying feed: the same fail-closed-by-default rule
        // `change_event_visible` uses for `/v1/changes`, applied here to a
        // column that (unlike the generic changes feed) is always populated,
        // so nothing is ever admitted for lack of a determinable wing.
        assert!(events.iter().all(|event| event["wing"] == "wing_alpha"));
        assert!(events.iter().any(|event| event["entity_id"] == alpha_task));

        let all_resp = harness
            .router
            .clone()
            .oneshot(authed_get("/v1/coordination/events?limit=100", ALICE_TOKEN))
            .await
            .unwrap();
        assert_eq!(all_resp.status(), StatusCode::OK);
        let all_events = body_json(all_resp).await["events"].as_array().unwrap().clone();
        assert!(all_events.iter().any(|event| event["wing"] == "wing_alpha"));
        assert!(all_events.iter().any(|event| event["wing"] == "wing_beta"));
    }

    #[tokio::test]
    async fn coordination_write_scope_without_claim_can_create_but_not_claim() {
        let harness = make_harness().await;

        let create_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/tasks",
                COORD_WRITE_ONLY_TOKEN,
                json!({
                    "title": "t", "description": "d", "wing": "wing_alpha",
                    "idempotency_key": "write-only-1",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(create_resp.status(), StatusCode::OK);
        let task_id = body_json(create_resp).await["task_id"].as_str().unwrap().to_owned();

        // The operation gate rejects before the handler (and its wing check)
        // ever run — `coord_write_only` holds no `coordination_claim` grant
        // for any wing, so this is 403 (operation not permitted at all), not
        // the 404 wing-masking the get/message/artifact/result tests exercise.
        let claim_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                &format!("/v1/coordination/tasks/{task_id}/claim"),
                COORD_WRITE_ONLY_TOKEN,
                json!({"expected_revision": 0, "lease_seconds": 300}),
            ))
            .await
            .unwrap();
        assert_eq!(claim_resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn coordination_claim_scope_implies_write_on_every_write_route() {
        // issue #102 Stage 7: a token scoped to `coordination_claim` alone
        // (no `coordination_write`) must reach every write route, because
        // claiming a task inherently requires the writes claiming itself
        // entails. `COORD_CLAIM_ONLY_TOKEN` carries no `coordination_read`
        // either, so this also proves the implication runs claim -> write
        // only, not claim -> read.
        let harness = make_harness().await;

        let create_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/tasks",
                COORD_CLAIM_ONLY_TOKEN,
                json!({
                    "title": "t", "description": "d", "wing": "wing_alpha",
                    "idempotency_key": "claim-only-create-1",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(create_resp.status(), StatusCode::OK, "claim-only token should create a task");
        let task_id = body_json(create_resp).await["task_id"].as_str().unwrap().to_owned();

        let message_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/messages",
                COORD_CLAIM_ONLY_TOKEN,
                json!({
                    "task_id": task_id, "recipient": "someone", "kind": "status",
                    "payload": {}, "idempotency_key": "claim-only-message-1",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(message_resp.status(), StatusCode::OK, "claim-only token should send a message");
        let message_id = body_json(message_resp).await["message_id"].as_str().unwrap().to_owned();

        let ack_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                &format!("/v1/coordination/messages/{message_id}/ack"),
                COORD_CLAIM_ONLY_TOKEN,
                json!({"actor": "someone"}),
            ))
            .await
            .unwrap();
        assert_eq!(ack_resp.status(), StatusCode::OK, "claim-only token should ack a message");

        let artifact_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/artifacts",
                COORD_CLAIM_ONLY_TOKEN,
                json!({
                    "task_id": task_id, "role": "output", "media_type": "text/plain",
                    "content": "claim-only artifact", "idempotency_key": "claim-only-artifact-1",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(
            artifact_resp.status(),
            StatusCode::OK,
            "claim-only token should put an artifact"
        );

        let result_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/results",
                COORD_CLAIM_ONLY_TOKEN,
                json!({
                    "task_id": task_id, "payload": {},
                    "idempotency_key": "claim-only-result-1",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(result_resp.status(), StatusCode::OK, "claim-only token should put a result");

        // The implication does not extend to reads: no `coordination_read`
        // grant means the coarse operation gate rejects before the
        // handler's wing check ever runs.
        let get_resp = harness
            .router
            .clone()
            .oneshot(authed_get(
                &format!("/v1/coordination/tasks/{task_id}"),
                COORD_CLAIM_ONLY_TOKEN,
            ))
            .await
            .unwrap();
        assert_eq!(get_resp.status(), StatusCode::FORBIDDEN);

        let inbox_resp = harness
            .router
            .clone()
            .oneshot(authed_get(
                "/v1/coordination/inbox?recipient=someone",
                COORD_CLAIM_ONLY_TOKEN,
            ))
            .await
            .unwrap();
        assert_eq!(inbox_resp.status(), StatusCode::FORBIDDEN);
    }

    // ─── Coordination review-finding regressions (2026-08-20) ───────────────

    #[tokio::test]
    async fn coordination_task_create_in_wing_agents_is_rejected_with_422() {
        let harness = make_harness().await;
        let resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/tasks",
                ALICE_TOKEN,
                json!({
                    "title": "t", "description": "d", "wing": "wing_agents",
                    "idempotency_key": "diary-create-1",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body_json(resp).await["code"], "diary_not_federated");
    }

    /// Seeds a task directly in `wing_agents` via the coordination store
    /// (bypassing the HTTP create route, which now refuses to create one at
    /// all — see `coordination_task_create_in_wing_agents_is_rejected_with_422`),
    /// plus a message on it. Proves every read and feed route still masks
    /// pre-existing `wing_agents` coordination state, and that a write
    /// against it is rejected outright, regardless of the caller's token
    /// scope.
    #[tokio::test]
    async fn coordination_wing_agents_task_is_masked_from_every_route() {
        let harness = make_harness().await;
        let task = harness
            .state
            .coordination
            .create_task(&NewTask {
                title: "diary-shaped".into(),
                description: "d".into(),
                created_by: "alice".into(),
                wing: "wing_agents".into(),
                idempotency_key: "diary-seed-1".into(),
                parent_id: None,
                dependencies: vec![],
                budget: None,
                expires_at: None,
            })
            .expect("seed wing_agents task directly through storage");
        let message = harness
            .state
            .coordination
            .send_message(&NewMessage {
                task_id: task.task_id.clone(),
                sender: "alice".into(),
                recipient: "someone".into(),
                kind: "status".into(),
                payload: serde_json::json!({}),
                idempotency_key: "diary-seed-message-1".into(),
                envelope_version: 1,
            })
            .expect("seed message on the wing_agents task");

        // GET is masked as 404, unrestricted ALICE_TOKEN included — the
        // diary override applies regardless of scope, not just to a
        // narrowly-scoped caller.
        let get_resp = harness
            .router
            .clone()
            .oneshot(authed_get(&format!("/v1/coordination/tasks/{}", task.task_id), ALICE_TOKEN))
            .await
            .unwrap();
        assert_eq!(get_resp.status(), StatusCode::NOT_FOUND);

        let message_get_resp = harness
            .router
            .clone()
            .oneshot(authed_get(
                &format!("/v1/coordination/messages/{}", message.message_id),
                ALICE_TOKEN,
            ))
            .await
            .unwrap();
        assert_eq!(message_get_resp.status(), StatusCode::NOT_FOUND);

        // A write against the task (claim) is rejected with the explicit
        // diary error, not masked as 404 — a write is not an
        // existence-oracle risk the way a read is.
        let claim_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                &format!("/v1/coordination/tasks/{}/claim", task.task_id),
                ALICE_TOKEN,
                json!({"expected_revision": 0, "lease_seconds": 300}),
            ))
            .await
            .unwrap();
        assert_eq!(claim_resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body_json(claim_resp).await["code"], "diary_not_federated");

        // The inbox never surfaces the seeded message, whether unfiltered or
        // explicitly filtered to wing_agents.
        let inbox_resp = harness
            .router
            .clone()
            .oneshot(authed_get("/v1/coordination/inbox?recipient=someone", ALICE_TOKEN))
            .await
            .unwrap();
        assert_eq!(inbox_resp.status(), StatusCode::OK);
        let inbox = body_json(inbox_resp).await;
        assert!(inbox["messages"].as_array().unwrap().is_empty());

        let inbox_wing_resp = harness
            .router
            .clone()
            .oneshot(authed_get(
                "/v1/coordination/inbox?recipient=someone&wing=wing_agents",
                ALICE_TOKEN,
            ))
            .await
            .unwrap();
        assert_eq!(inbox_wing_resp.status(), StatusCode::OK);
        assert!(body_json(inbox_wing_resp).await["messages"].as_array().unwrap().is_empty());

        // The event feed never surfaces wing_agents events either, whether
        // unfiltered or explicitly filtered.
        let events_resp = harness
            .router
            .clone()
            .oneshot(authed_get("/v1/coordination/events?limit=200", ALICE_TOKEN))
            .await
            .unwrap();
        assert_eq!(events_resp.status(), StatusCode::OK);
        let events = body_json(events_resp).await;
        assert!(
            events["events"].as_array().unwrap().iter().all(|e| e["wing"] != "wing_agents"),
            "no wing_agents event should ever be federated"
        );

        let events_wing_resp = harness
            .router
            .clone()
            .oneshot(authed_get("/v1/coordination/events?wing=wing_agents&limit=200", ALICE_TOKEN))
            .await
            .unwrap();
        assert_eq!(events_wing_resp.status(), StatusCode::OK);
        assert!(body_json(events_wing_resp).await["events"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn coordination_events_cursor_does_not_leak_invisible_wing_volume() {
        let harness = make_harness().await;
        // wing_secret is invisible to COORD_ALPHA_TOKEN (scoped to wing_alpha only). Seed two
        // events in it — a pre-fix cursor computed over the unfiltered page would report
        // `has_more: true` and hand back a real sequence number here, distinguishing "has
        // events" from "empty" in a single request even though the response's own `events`
        // list is (correctly) empty either way.
        let secret_task = create_task(&harness, ALICE_TOKEN, "wing_secret", "leak-secret-1").await;
        let claim_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                &format!("/v1/coordination/tasks/{secret_task}/claim"),
                ALICE_TOKEN,
                json!({"expected_revision": 0, "lease_seconds": 300}),
            ))
            .await
            .unwrap();
        assert_eq!(claim_resp.status(), StatusCode::OK);

        let with_events = harness
            .router
            .clone()
            .oneshot(authed_get(
                "/v1/coordination/events?wing=wing_secret&limit=1",
                COORD_ALPHA_TOKEN,
            ))
            .await
            .unwrap();
        assert_eq!(with_events.status(), StatusCode::OK);

        // A wing that genuinely has zero events must produce the identical body: same empty
        // list, same null cursor — otherwise the cursor is an existence-and-volume oracle for
        // wings this token cannot see, exactly the class of leak `4cac227` closed for
        // `parent_id`/`dependencies`.
        let without_events = harness
            .router
            .clone()
            .oneshot(authed_get("/v1/coordination/events?wing=wing_ghost&limit=1", COORD_ALPHA_TOKEN))
            .await
            .unwrap();
        assert_eq!(without_events.status(), StatusCode::OK);

        assert_eq!(body_json(with_events).await, body_json(without_events).await);
    }

    #[tokio::test]
    async fn coordination_events_cursor_does_not_leak_diary_wing_volume() {
        let harness = make_harness().await;
        // Seed two wing_agents events directly through storage — the HTTP create route refuses
        // to create a task there at all (`coordination_task_create_in_wing_agents_is_rejected_with_422`).
        let task = harness
            .state
            .coordination
            .create_task(&NewTask {
                title: "diary-shaped".into(),
                description: "d".into(),
                created_by: "alice".into(),
                wing: "wing_agents".into(),
                idempotency_key: "leak-diary-1".into(),
                parent_id: None,
                dependencies: vec![],
                budget: None,
                expires_at: None,
            })
            .expect("seed wing_agents task directly through storage");
        harness
            .state
            .coordination
            .send_message(&NewMessage {
                task_id: task.task_id.clone(),
                sender: "alice".into(),
                recipient: "someone".into(),
                kind: "status".into(),
                payload: serde_json::json!({}),
                idempotency_key: "leak-diary-message-1".into(),
                envelope_version: 1,
            })
            .expect("seed message on the wing_agents task");

        // ALICE_TOKEN is unrestricted, but the diary hard-override still applies unconditionally:
        // an explicit `?wing=wing_agents` filter must be indistinguishable from a wing with no
        // events at all, for every caller, not just a narrowly scoped one.
        let with_events = harness
            .router
            .clone()
            .oneshot(authed_get("/v1/coordination/events?wing=wing_agents&limit=1", ALICE_TOKEN))
            .await
            .unwrap();
        assert_eq!(with_events.status(), StatusCode::OK);

        let without_events = harness
            .router
            .clone()
            .oneshot(authed_get(
                "/v1/coordination/events?wing=wing_ghost_diary&limit=1",
                ALICE_TOKEN,
            ))
            .await
            .unwrap();
        assert_eq!(without_events.status(), StatusCode::OK);

        assert_eq!(body_json(with_events).await, body_json(without_events).await);
    }

    #[tokio::test]
    async fn coordination_events_pagination_of_own_wing_is_unaffected_by_interleaved_invisible_rows()
     {
        let harness = make_harness().await;
        // Interleave wing_alpha (visible to COORD_ALPHA_TOKEN) and wing_beta (invisible) task
        // creation so visible and invisible global sequence numbers alternate. This is the
        // regression that matters most: a visibility filter applied incorrectly could shift or
        // miscount the cursor boundary when invisible rows sit *between* visible ones, not just
        // at the edges of the feed.
        let alpha1 =
            create_task(&harness, COORD_ALPHA_TOKEN, "wing_alpha", "interleave-alpha-1").await;
        let _beta1 = create_task(&harness, ALICE_TOKEN, "wing_beta", "interleave-beta-1").await;
        let alpha2 =
            create_task(&harness, COORD_ALPHA_TOKEN, "wing_alpha", "interleave-alpha-2").await;
        let _beta2 = create_task(&harness, ALICE_TOKEN, "wing_beta", "interleave-beta-2").await;
        let alpha3 =
            create_task(&harness, COORD_ALPHA_TOKEN, "wing_alpha", "interleave-alpha-3").await;

        let mut seen = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let uri = match &cursor {
                Some(c) => format!("/v1/coordination/events?limit=1&cursor={}", urlencoded(c)),
                None => "/v1/coordination/events?limit=1".to_owned(),
            };
            let resp =
                harness.router.clone().oneshot(authed_get(&uri, COORD_ALPHA_TOKEN)).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let page = body_json(resp).await;
            let events = page["events"].as_array().unwrap().clone();
            assert!(events.len() <= 1);
            seen.extend(events);
            let next = page["next_cursor"].as_str().map(str::to_owned);
            assert!(seen.len() <= 20, "paging should terminate well before this");
            match next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        assert!(
            seen.iter().all(|e| e["wing"] == "wing_alpha"),
            "an invisible wing must never surface, interleaved or not"
        );
        assert_eq!(
            seen.len(),
            3,
            "task_created for each of the three alpha tasks, none skipped or duplicated"
        );
        assert_eq!(seen[0]["entity_id"], alpha1);
        assert_eq!(seen[1]["entity_id"], alpha2);
        assert_eq!(seen[2]["entity_id"], alpha3);
    }

    #[tokio::test]
    async fn coordination_inbox_cursor_does_not_leak_invisible_wing_recipient_volume() {
        let harness = make_harness().await;
        // wing_secret is invisible to COORD_ALPHA_TOKEN. Seed three messages to a recipient
        // name COORD_ALPHA_TOKEN merely guesses at — `recipient` is compared against no
        // identity, by design, so any coordination_read token may probe any recipient string.
        let secret_task =
            create_task(&harness, ALICE_TOKEN, "wing_secret", "leak-inbox-secret-1").await;
        for key in ["leak-inbox-1", "leak-inbox-2", "leak-inbox-3"] {
            let resp = harness
                .router
                .clone()
                .oneshot(authed_json_request(
                    Method::POST,
                    "/v1/coordination/messages",
                    ALICE_TOKEN,
                    json!({
                        "task_id": secret_task, "recipient": "victim_agent", "kind": "status",
                        "payload": {}, "idempotency_key": key,
                    }),
                ))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
        }

        let with_messages = harness
            .router
            .clone()
            .oneshot(authed_get(
                "/v1/coordination/inbox?recipient=victim_agent&limit=1",
                COORD_ALPHA_TOKEN,
            ))
            .await
            .unwrap();
        assert_eq!(with_messages.status(), StatusCode::OK);

        // A recipient with genuinely zero messages must produce the identical body.
        let without_messages = harness
            .router
            .clone()
            .oneshot(authed_get(
                "/v1/coordination/inbox?recipient=victim_ghost&limit=1",
                COORD_ALPHA_TOKEN,
            ))
            .await
            .unwrap();
        assert_eq!(without_messages.status(), StatusCode::OK);

        assert_eq!(body_json(with_messages).await, body_json(without_messages).await);
    }

    #[tokio::test]
    async fn coordination_idempotency_replay_across_narrowed_scope_is_conflict() {
        let harness = make_harness().await;

        // coord_wide is initially scoped to every wing; create a task in
        // wing_beta under a key it will replay below.
        let original = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/tasks",
                COORD_WIDE_TOKEN,
                json!({
                    "title": "t", "description": "d", "wing": "wing_beta",
                    "idempotency_key": "narrowed-replay-1",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(original.status(), StatusCode::OK);
        assert_eq!(body_json(original).await["wing"], "wing_beta");

        // Narrow coord_wide's scope to wing_alpha only, then wait past the
        // registry's mtime-based reload granularity (matches
        // `hot_reload_picks_up_scope_change`).
        let token_file = harness._tempdir.path().join("tokens.json");
        std::thread::sleep(std::time::Duration::from_millis(1100));
        let mut tokens = default_tokens_json();
        for entry in tokens.as_array_mut().unwrap() {
            if entry["name"] == "coord_wide" {
                entry["scopes"] = json!([{
                    "wings": ["wing_alpha"],
                    "operations": ["coordination_read", "coordination_write", "coordination_claim"],
                }]);
            }
        }
        std::fs::write(&token_file, serde_json::to_string(&tokens).unwrap()).unwrap();
        restrict_token_file(&token_file);

        // Replaying the same key, now naming an authorized wing_alpha wing,
        // must not hand back the wing_beta task: the wing the caller can now
        // see was never authorized for the record storage actually has.
        let replay = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/tasks",
                COORD_WIDE_TOKEN,
                json!({
                    "title": "t", "description": "d", "wing": "wing_alpha",
                    "idempotency_key": "narrowed-replay-1",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(replay.status(), StatusCode::CONFLICT);
        let body = body_json(replay).await;
        assert_eq!(body["code"], "idempotency_key_conflict");
        // The message must not disclose the wing or any task content.
        let message = body["message"].as_str().unwrap();
        assert!(!message.contains("wing_beta"), "{message}");
    }

    #[tokio::test]
    async fn coordination_message_replay_across_unauthorized_task_is_conflict() {
        let harness = make_harness().await;

        // coord_wide creates a task in wing_beta, then sends a message on it
        // under a key it will replay below.
        let beta_task = create_task(&harness, COORD_WIDE_TOKEN, "wing_beta", "msg-replay-beta").await;
        let original = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/messages",
                COORD_WIDE_TOKEN,
                json!({
                    "task_id": beta_task, "recipient": "someone", "kind": "status",
                    "payload": {}, "idempotency_key": "msg-replay-key-1",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(original.status(), StatusCode::OK);

        // Narrow scope to wing_alpha only.
        let token_file = harness._tempdir.path().join("tokens.json");
        std::thread::sleep(std::time::Duration::from_millis(1100));
        let mut tokens = default_tokens_json();
        for entry in tokens.as_array_mut().unwrap() {
            if entry["name"] == "coord_wide" {
                entry["scopes"] = json!([{
                    "wings": ["wing_alpha"],
                    "operations": ["coordination_read", "coordination_write", "coordination_claim"],
                }]);
            }
        }
        std::fs::write(&token_file, serde_json::to_string(&tokens).unwrap()).unwrap();
        restrict_token_file(&token_file);

        // Create a decoy task in the now-authorized wing_alpha, then replay
        // the message key against it. The replay must not return the
        // original wing_beta message.
        let alpha_task = create_task(&harness, COORD_WIDE_TOKEN, "wing_alpha", "msg-replay-alpha").await;
        let replay = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/messages",
                COORD_WIDE_TOKEN,
                json!({
                    "task_id": alpha_task, "recipient": "someone", "kind": "status",
                    "payload": {}, "idempotency_key": "msg-replay-key-1",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(replay.status(), StatusCode::CONFLICT);
        assert_eq!(body_json(replay).await["code"], "idempotency_key_conflict");
    }

    #[tokio::test]
    async fn coordination_inbox_cursor_does_not_skip_the_second_visible_message() {
        let harness = make_harness().await;
        let task_id =
            create_task(&harness, COORD_ALPHA_TOKEN, "wing_alpha", "inbox-cursor-task").await;

        for key in ["inbox-cursor-1", "inbox-cursor-2"] {
            let resp = harness
                .router
                .clone()
                .oneshot(authed_json_request(
                    Method::POST,
                    "/v1/coordination/messages",
                    COORD_ALPHA_TOKEN,
                    json!({
                        "task_id": task_id, "recipient": "coord_alpha", "kind": "status",
                        "payload": {}, "idempotency_key": key,
                    }),
                ))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
        }

        // Page through with limit=1, following next_cursor, exactly as
        // `coordination_events_feed_pages_with_cursor` does for events.
        // Before the fix, storage found no third message and reported
        // `next_cursor: None` on the very first (over-fetched) page even
        // though a second visible message was still unread, making it
        // permanently unreachable.
        let mut seen = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let uri = match &cursor {
                Some(c) => format!(
                    "/v1/coordination/inbox?recipient=coord_alpha&limit=1&cursor={}",
                    urlencoded(c)
                ),
                None => "/v1/coordination/inbox?recipient=coord_alpha&limit=1".to_owned(),
            };
            let resp =
                harness.router.clone().oneshot(authed_get(&uri, COORD_ALPHA_TOKEN)).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let page = body_json(resp).await;
            let messages = page["messages"].as_array().unwrap().clone();
            assert!(messages.len() <= 1);
            seen.extend(messages);
            let next = page["next_cursor"].as_str().map(str::to_owned);
            assert!(seen.len() <= 10, "paging should terminate well before this");
            match next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        assert_eq!(seen.len(), 2, "both visible messages must eventually be reachable");
    }

    #[test]
    fn token_registry_rejects_colon_in_token_name() {
        let tempdir = TempDir::new().unwrap();
        let token_file = tempdir.path().join("tokens.json");
        std::fs::write(
            &token_file,
            serde_json::to_string(&serde_json::json!([
                {"token": ALICE_TOKEN, "name": "ci:worker", "enabled": true},
            ]))
            .unwrap(),
        )
        .unwrap();
        restrict_token_file(&token_file);

        let err = TokenRegistry::load(token_file).unwrap_err();
        assert!(err.to_string().contains("must not contain"), "{err}");
    }

    #[tokio::test]
    async fn coordination_claimed_actor_containing_colon_is_rejected() {
        let harness = make_harness().await;
        let resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/tasks",
                COORD_ALPHA_TOKEN,
                json!({
                    "title": "t", "description": "d", "wing": "wing_alpha",
                    "idempotency_key": "colon-claim-1",
                    "created_by": "ci:worker",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn coordination_claim_with_oversized_lease_seconds_returns_400_not_a_panic() {
        let harness = make_harness().await;
        let task_id =
            create_task(&harness, COORD_ALPHA_TOKEN, "wing_alpha", "oversized-lease-route-task").await;

        let claim_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                &format!("/v1/coordination/tasks/{task_id}/claim"),
                COORD_ALPHA_TOKEN,
                json!({"expected_revision": 0, "lease_seconds": i64::MAX}),
            ))
            .await
            .unwrap();
        assert_eq!(claim_resp.status(), StatusCode::BAD_REQUEST);

        // A sane claim, then an oversized renewal, exercises the same bound
        // on the renew route.
        let ok_claim = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                &format!("/v1/coordination/tasks/{task_id}/claim"),
                COORD_ALPHA_TOKEN,
                json!({"expected_revision": 0, "lease_seconds": 300}),
            ))
            .await
            .unwrap();
        assert_eq!(ok_claim.status(), StatusCode::OK);

        let renew_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                &format!("/v1/coordination/tasks/{task_id}/renew"),
                COORD_ALPHA_TOKEN,
                json!({"expected_revision": 1, "lease_seconds": i64::MAX}),
            ))
            .await
            .unwrap();
        assert_eq!(renew_resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn coordination_task_create_masks_unauthorized_dependency_as_missing() {
        let harness = make_harness().await;
        // A real task, hidden in wing_beta, that coord_alpha (scoped to
        // wing_alpha only) cannot see.
        let hidden_dependency =
            create_task(&harness, ALICE_TOKEN, "wing_beta", "oracle-hidden-dependency").await;

        let hidden_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/tasks",
                COORD_ALPHA_TOKEN,
                json!({
                    "title": "t", "description": "d", "wing": "wing_alpha",
                    "idempotency_key": "oracle-hidden-1",
                    "dependencies": [hidden_dependency],
                }),
            ))
            .await
            .unwrap();

        let missing_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/tasks",
                COORD_ALPHA_TOKEN,
                json!({
                    "title": "t", "description": "d", "wing": "wing_alpha",
                    "idempotency_key": "oracle-missing-1",
                    "dependencies": ["task_does_not_exist_at_all"],
                }),
            ))
            .await
            .unwrap();

        // A hidden, real cross-wing id and a genuinely nonexistent id must be
        // indistinguishable: same status, same error shape — otherwise the
        // route is an existence oracle for wings this token cannot read.
        assert_eq!(hidden_resp.status(), StatusCode::NOT_FOUND);
        assert_eq!(missing_resp.status(), StatusCode::NOT_FOUND);
        assert_eq!(body_json(hidden_resp).await["code"], "not_found");
        assert_eq!(body_json(missing_resp).await["code"], "not_found");

        // Same rule for `parent_id`.
        let hidden_parent_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/tasks",
                COORD_ALPHA_TOKEN,
                json!({
                    "title": "t", "description": "d", "wing": "wing_alpha",
                    "idempotency_key": "oracle-hidden-parent-1",
                    "parent_id": hidden_dependency,
                }),
            ))
            .await
            .unwrap();
        assert_eq!(hidden_parent_resp.status(), StatusCode::NOT_FOUND);

        // A visible dependency still works normally.
        let visible_dependency =
            create_task(&harness, COORD_ALPHA_TOKEN, "wing_alpha", "oracle-visible-dependency").await;
        let visible_resp = harness
            .router
            .clone()
            .oneshot(authed_json_request(
                Method::POST,
                "/v1/coordination/tasks",
                COORD_ALPHA_TOKEN,
                json!({
                    "title": "t", "description": "d", "wing": "wing_alpha",
                    "idempotency_key": "oracle-visible-1",
                    "dependencies": [visible_dependency],
                }),
            ))
            .await
            .unwrap();
        assert_eq!(visible_resp.status(), StatusCode::OK);
    }
}

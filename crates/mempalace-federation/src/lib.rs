//! Wire-types (serde DTOs) shared between the MemPalace federation HTTP server
//! and any federation client. This crate is deliberately free of runtime logic
//! so it can be compiled into both the server and lightweight clients.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The federation HTTP API version implemented by this crate.
pub const FEDERATION_API_VERSION: u32 = 1;

// ─── Server info ──────────────────────────────────────────────────────────────

/// Response body for the `GET /info` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InfoResponse {
    /// Semver string of the running MemPalace server binary.
    #[serde(default)]
    pub server_version: String,
    /// Federation API version (matches [`FEDERATION_API_VERSION`]).
    #[serde(default)]
    pub federation_api_version: u32,
    /// Name of the embedding profile used by this server (e.g. `"balanced"`).
    #[serde(default)]
    pub embedding_profile: String,
    /// Feature flags the server supports (e.g. `["drawers", "kg", "changes"]`).
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Whether the maintenance subsystem is enabled.
    #[serde(default)]
    pub maintenance_enabled: bool,
    /// Whether the HTTP server schedules maintenance automatically.
    #[serde(default)]
    pub maintenance_background_enabled: bool,
    /// Minimum idle seconds since last write before maintenance runs.
    #[serde(default)]
    pub maintenance_idle_secs: u64,
    /// JSON-serialized [`MaintenanceRunSummary`] of the last completed
    /// maintenance run, if any.
    #[serde(default)]
    pub maintenance_last_run: Option<serde_json::Value>,
    /// Typed status of the maintenance subsystem.  Replaces the ambiguous
    /// `null` of `maintenance_last_run` with explicit states.
    #[serde(default)]
    pub maintenance_status: MaintenanceStatus,
}

// ─── Maintenance status ────────────────────────────────────────────────────────

/// Why a maintenance run was skipped (before any tier started).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceSkipReason {
    /// The system has not been idle long enough.
    NotIdle,
    /// No work was required.
    NothingToDo,
}

/// Why a maintenance run was aborted (started but terminated early).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceAbortReason {
    /// Another concurrent process holds the maintenance lock.
    ConcurrentRun,
    /// The system is shutting down.
    Shutdown,
    /// The operation exceeded its time budget.
    Timeout,
}

/// Overall run status of a completed maintenance run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceRunStatus {
    /// All tiers completed successfully.
    Success,
    /// At least one tier was skipped (non-critical).
    Partial,
    /// At least one tier failed or was aborted.
    Failure,
}

/// Typed status of the maintenance subsystem.
///
/// This replaces the ambiguous `null` / opaque-JSON dance of
/// `maintenance_last_run` with an explicit state that clients can
/// match on.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceStatus {
    /// Maintenance is disabled in configuration.
    Disabled,
    /// No maintenance run has completed yet (idle / not-yet-run).
    Idle,
    /// A maintenance run is currently in progress.
    Running,
    /// The most recent attempt was skipped before any tier started.
    Skipped { reason: MaintenanceSkipReason },
    /// The most recent attempt was aborted mid-run.
    Aborted { reason: MaintenanceAbortReason },
    /// The most recent attempt failed with an error.
    Failed { message: String },
    /// The most recent run completed (possibly with tier-level failures).
    Completed { status: MaintenanceRunStatus },
}

impl Default for MaintenanceStatus {
    fn default() -> Self {
        Self::Idle
    }
}

// ─── Drawer search ────────────────────────────────────────────────────────────

/// Request body for `POST /drawers/search`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DrawerSearchRequest {
    /// Free-text semantic query.
    pub query: String,
    /// Optional wing filter.
    #[serde(default)]
    pub wing: Option<String>,
    /// Optional room filter.
    #[serde(default)]
    pub room: Option<String>,
    /// Optional view/ref name to scope search. `"canonical"` for canonical
    /// snapshots, or a branch name for a single branch view.
    #[serde(default)]
    pub view: Option<String>,
    /// Maximum number of results to return.
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Response body for `POST /drawers/search`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DrawerSearchResponse {
    /// Ranked list of matching drawers.
    pub results: Vec<RemoteDrawerResult>,
}

/// A single drawer hit returned by the federation search endpoint.
///
/// `rank` is mandatory because similarity scores are not comparable across
/// embedding profiles; clients that merge results from multiple servers must
/// merge by rank rather than by raw score.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RemoteDrawerResult {
    /// Stable drawer identifier.
    pub drawer_id: String,
    /// Wing the drawer lives in.
    pub wing: String,
    /// Room the drawer lives in.
    pub room: String,
    /// 1-based rank within this server's result set.
    pub rank: usize,
    /// Normalised similarity score in `[0, 1]`.
    pub score: f32,
    /// Full drawer content text.
    pub content: String,
    /// Original source file path, if recorded.
    #[serde(default)]
    pub source_file: Option<String>,
    /// BLAKE3 hex hash of the content at ingest time.
    #[serde(default)]
    pub content_hash: Option<String>,
    /// RFC 3339 timestamp when the drawer was filed.
    #[serde(default)]
    pub filed_at: Option<String>,
    /// Agent name recorded at ingest time.
    #[serde(default)]
    pub added_by: Option<String>,
    /// True when the drawer's mined source file changed since mining
    /// (locator-backed rows).  Absent unless true; `serde(default)` keeps old
    /// servers/clients wire-compatible.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub stale: bool,
}

// ─── Add drawer ───────────────────────────────────────────────────────────────

/// Request body for `POST /drawers`.
///
/// The server will override or augment `added_by` from the auth token; the
/// field here carries the client's claimed agent name.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AddDrawerRequest {
    /// Target wing.
    pub wing: String,
    /// Target room.
    pub room: String,
    /// Verbatim content to store.
    pub content: String,
    /// Optional source file path.
    #[serde(default)]
    pub source_file: Option<String>,
    /// Client-declared agent name.
    #[serde(default)]
    pub added_by: Option<String>,
    /// Optional caller-supplied stable drawer id.
    ///
    /// When present, durable replication preserves the local logical drawer id
    /// on the remote instead of letting the server generate a fresh one, so
    /// dual-written (local-first, replicated) drawers converge on a single
    /// stable identity. Absent for all pre-replication callers.
    ///
    /// Omitted from the JSON wire when `None` so old servers see no new field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drawer_id: Option<String>,
    /// Optional stable operation / idempotency identity for the whole mutation.
    ///
    /// Lets a durable replication outbox retry an add safely: the receiving
    /// endpoint can dedupe a replayed mutation and a caller that got back an
    /// `UnknownOutcome` can distinguish a re-run from a fresh add. Absent for
    /// all pre-replication callers.
    ///
    /// Omitted from the JSON wire when `None` so old servers see no new field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
}

/// Response body for `POST /drawers`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AddDrawerResponse {
    /// Whether the drawer was successfully stored.
    pub success: bool,
    /// Assigned drawer identifier, present when `success` is `true`.
    #[serde(default)]
    pub drawer_id: Option<String>,
    /// Resolved wing, present when `success` is `true`.
    #[serde(default)]
    pub wing: Option<String>,
    /// Resolved room, present when `success` is `true`.
    #[serde(default)]
    pub room: Option<String>,
}

// ─── Duplicate check ──────────────────────────────────────────────────────────

/// Request body for `POST /drawers/check-duplicate`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CheckDuplicateRequest {
    /// Content to test for near-duplicates.
    pub content: String,
    /// Similarity threshold in `[0, 1]`; defaults to server's configured value.
    #[serde(default)]
    pub threshold: Option<f32>,
}

/// Response body for `POST /drawers/check-duplicate`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CheckDuplicateResponse {
    /// `true` when at least one near-duplicate was found above the threshold.
    pub is_duplicate: bool,
    /// Matching drawers as a JSON array (mirrors the MCP tool payload shape).
    pub matches: Value,
}

// ─── List drawers ─────────────────────────────────────────────────────────────

/// Query parameters for `GET /drawers`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ListDrawersQuery {
    /// Optional wing filter.
    #[serde(default)]
    pub wing: Option<String>,
    /// Optional room filter.
    #[serde(default)]
    pub room: Option<String>,
    /// Maximum number of results per page.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Opaque pagination cursor returned by a previous response.
    #[serde(default)]
    pub cursor: Option<String>,
}

/// Response body for `GET /drawers`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ListDrawersResponse {
    /// Array of drawer objects (shape mirrors the storage layer).
    pub drawers: Value,
    /// Opaque cursor to pass back for the next page; `null` when exhausted.
    #[serde(default)]
    pub next_cursor: Option<String>,
}

// ─── Delete drawer ────────────────────────────────────────────────────────────

/// Query parameters for `DELETE /drawers/{id}`.
///
/// Adds a backward-compatible way for a delete-by-ID to carry an optional
/// stable operation id, without changing the trailing-slash path that old
/// callers already use. Old callers omit `operation_id` entirely (`{}`); it
/// defaults to `None`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DeleteDrawerQuery {
    /// Optional stable operation / idempotency identity (see
    /// [`AddDrawerRequest::operation_id`]), passed as a query parameter so the
    /// receiving endpoint can dedupe a replayed delete. Absent for
    /// pre-replication callers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
}

// ─── Knowledge graph ──────────────────────────────────────────────────────────

/// Request body for `POST /kg/query`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KgQueryRequest {
    /// Entity name to query.
    pub entity: String,
    /// Optional RFC 3339 or `YYYY-MM-DD` date for point-in-time queries.
    #[serde(default)]
    pub as_of: Option<String>,
    /// Traversal direction: `"outgoing"`, `"incoming"`, or `"both"`.
    #[serde(default)]
    pub direction: Option<String>,
}

/// Request body for `POST /kg/facts`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KgAddFactRequest {
    /// Subject entity.
    pub subject: String,
    /// Predicate / relationship type.
    pub predicate: String,
    /// Object entity.
    pub object: String,
    /// Optional `YYYY-MM-DD` date when the fact became true.
    #[serde(default)]
    pub valid_from: Option<String>,
    /// Optional stable operation / idempotency identity (see
    /// [`AddDrawerRequest::operation_id`]). Absent for pre-replication callers.
    ///
    /// Omitted from the JSON wire when `None` so old servers see no new field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
}

/// Request body for `POST /kg/invalidate`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KgInvalidateRequest {
    /// Subject entity.
    pub subject: String,
    /// Predicate / relationship type.
    pub predicate: String,
    /// Object entity.
    pub object: String,
    /// Optional `YYYY-MM-DD` date when the fact stopped being true.
    #[serde(default)]
    pub ended: Option<String>,
    /// Optional stable operation / idempotency identity (see
    /// [`AddDrawerRequest::operation_id`]). Absent for pre-replication callers.
    ///
    /// Omitted from the JSON wire when `None` so old servers see no new field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
}

// KG responses are plain `serde_json::Value` pass-throughs (server mirrors MCP
// tool payloads); no response DTOs are needed.

// ─── Change log ───────────────────────────────────────────────────────────────

/// Query parameters for `GET /changes`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChangesQuery {
    /// Return only events at or after this RFC 3339 timestamp.
    #[serde(default)]
    pub since: Option<String>,
    /// Maximum number of events per page.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Opaque pagination cursor returned by a previous response.
    #[serde(default)]
    pub cursor: Option<String>,
}

/// Response body for `GET /changes`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChangesResponse {
    /// Ordered list of change events.
    pub events: Vec<ChangeEventDto>,
    /// Opaque cursor to pass back for the next page; `null` when exhausted.
    #[serde(default)]
    pub next_cursor: Option<String>,
}

/// A single change event in the federation changes feed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChangeEventDto {
    /// Operation type (e.g. `"drawer_added"`, `"kg_fact_added"`).
    pub event_type: String,
    /// RFC 3339 timestamp when the event occurred.
    pub occurred_at: String,
    /// Primary identifier of the affected entity.
    pub entity_id: String,
    /// Agent or tool that performed the write, if known.
    #[serde(default)]
    pub actor: Option<String>,
    /// Optional JSON payload with extra context.
    #[serde(default)]
    pub details: Option<Value>,
}

// ─── Bulk ingest ──────────────────────────────────────────────────────────────

/// Request body for `POST /v1/ingest/batch`.
///
/// The client sends one or more pre-chunked files; the server embeds them and
/// writes drawers on its side.  `wing` and `repo_id` together determine the
/// source-key namespace so that two clients pushing the same repository
/// converge on identical drawer ids.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestBatchRequest {
    /// Target wing name (will be normalized server-side).
    pub wing: String,
    /// Machine-independent repository identity (normalized remote-URL or fallback).
    pub repo_id: String,
    /// Client-declared agent name; server may augment from auth token.
    #[serde(default)]
    pub agent: Option<String>,
    /// Git commit hash at the time of mining, for audit / change-event details.
    #[serde(default)]
    pub commit_hash: Option<String>,
    /// Files included in this batch.
    pub files: Vec<IngestFileDto>,
}

/// A single file's chunks within an [`IngestBatchRequest`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestFileDto {
    /// Repository-root-relative path with forward slashes.
    pub relative_path: String,
    /// `project_ingest_content_hash` of this file; used for skip-unchanged detection.
    pub content_hash: String,
    /// BLAKE3 hex hash of the full file bytes.  When `Some`, all chunks carry
    /// byte ranges so the server can build locator rows.  When `None`, the
    /// file could not be read as UTF-8 and chunks are stored as legacy content
    /// rows (text persisted, no locator).
    #[serde(default)]
    pub file_hash: Option<String>,
    /// Ordered list of chunks derived from this file.
    pub chunks: Vec<IngestChunkDto>,
}

/// A single text chunk within an [`IngestFileDto`].
///
/// When `file_hash` is `Some` on the parent [`IngestFileDto`], all four byte/
/// line range fields must also be `Some` so the server can store a locator row.
/// When `file_hash` is `None`, the ranges are absent and the server stores
/// `text` directly as a legacy content row.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestChunkDto {
    /// 0-based chunk index within the file; determines the drawer-id suffix.
    pub chunk_index: u32,
    /// Room name for this chunk (derived from file path / heuristics on the client).
    pub room: String,
    /// Chunk text; the server uses this for embedding.
    pub text: String,
    /// Inclusive byte offset of the first byte of this chunk in the file.
    #[serde(default)]
    pub byte_start: Option<u64>,
    /// Byte offset one past the last byte of this chunk (exclusive).
    #[serde(default)]
    pub byte_end: Option<u64>,
    /// 1-based line number of the first line of this chunk.
    #[serde(default)]
    pub line_start: Option<u32>,
    /// 1-based line number of the last line of this chunk.
    #[serde(default)]
    pub line_end: Option<u32>,
}

/// Response body for `POST /v1/ingest/batch`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestBatchResponse {
    /// Per-file outcomes, in the same order as the request's `files` array.
    pub files: Vec<IngestFileResult>,
    /// Non-fatal warnings (e.g. missing checkout mapping → stale placeholders).
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// Outcome for a single file within an [`IngestBatchResponse`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestFileResult {
    /// Repository-root-relative path, echoed from the request.
    pub relative_path: String,
    /// `"ingested"` | `"skipped_unchanged"` | `"failed"`.
    pub status: String,
    /// Number of drawers written (0 for skipped or failed files).
    #[serde(default)]
    pub drawers_written: usize,
    /// Error message when `status == "failed"`.
    #[serde(default)]
    pub error: Option<String>,
}

// ─── Coordination (issue #102 Stage 3) ─────────────────────────────────────────
//
// Wire mirrors of the storage types in `mempalace-storage/src/coordination.rs`.
// This crate must not depend on `mempalace-storage` (see the crate-level doc
// comment), so these are independent types with the same shape;
// `mempalace-server` converts between the two. Timestamps are plain RFC 3339
// strings, matching `ChangeEventDto::occurred_at` above, rather than pulling
// in the `time` crate here.
//
// Every `created_by` / `sender` / `worker` / `actor` field on a *request* DTO
// below is the caller's *claimed* name, not an assertion of identity: the
// server derives the actual actor from the authenticated token and applies
// the same `{identity}:{claimed}` prefixing rule `AddDrawerRequest::added_by`
// already uses when the claim disagrees with the token. See the federated-
// coordination part of docs/Federation.md.
//
// Coordination cursors are opaque strings encoding only
// `coordination_events.sequence` — no timestamp, no rowid pair, and
// deliberately not the `"{rfc3339}|{rowid}"` shape `/v1/changes` uses (that
// format exists because `/v1/changes` supports `since`; the coordination feed
// does not, so a cursor here depends on no clock at all). Treat `cursor` /
// `next_cursor` fields below as opaque: never parse or do arithmetic on them.

/// Mirrors `mempalace_storage::coordination::TaskState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinationTaskState {
    /// Created, not yet claimed by a worker.
    Pending,
    /// Claimed and leased by a worker.
    Running,
    /// Running but blocked on external input.
    InputRequired,
    /// Terminal: finished successfully.
    Completed,
    /// Terminal: cancelled before completion.
    Cancelled,
    /// Terminal: finished unsuccessfully.
    Failed,
    /// Terminal: passed its `expires_at` before completion.
    Expired,
}

/// Wire mirror of `mempalace_storage::coordination::Task`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CoordinationTaskDto {
    /// Immutable task identifier.
    pub task_id: String,
    /// Short human-readable title.
    pub title: String,
    /// Full description.
    pub description: String,
    /// Current lifecycle state.
    pub state: CoordinationTaskState,
    /// Monotonically increasing revision, used for compare-and-swap writes.
    pub revision: i64,
    /// Actor recorded as having created the task (identity-derived; see
    /// module docs).
    pub created_by: String,
    /// Owning wing. Authorization key for every other coordination route.
    pub wing: String,
    /// Current lease holder, present only while `state` is `running`.
    #[serde(default)]
    pub owner: Option<String>,
    /// Parent task id, if this task was created as a subtask.
    #[serde(default)]
    pub parent_id: Option<String>,
    /// Task ids this task depends on.
    #[serde(default)]
    pub dependencies: Vec<String>,
    /// Opaque budget metadata (host-runtime defined; not interpreted here).
    #[serde(default)]
    pub budget: Option<Value>,
    /// RFC 3339 timestamp, present while a worker holds a live lease.
    #[serde(default)]
    pub lease_expires_at: Option<String>,
    /// RFC 3339 timestamp.
    #[serde(default)]
    pub expires_at: Option<String>,
    /// RFC 3339 timestamp.
    pub created_at: String,
    /// RFC 3339 timestamp.
    pub updated_at: String,
}

/// Request body for `POST /v1/coordination/tasks`. Mirrors
/// `mempalace_storage::coordination::NewTask`, except `created_by` is the
/// caller's claimed creator name rather than an authoritative identity.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NewTaskRequest {
    /// Short human-readable title.
    pub title: String,
    /// Full description.
    pub description: String,
    /// Target wing (short or fully-qualified form; normalised server-side).
    pub wing: String,
    /// Deduplication key, scoped to the resolved `created_by` actor.
    pub idempotency_key: String,
    /// Claimed creator name; see module docs.
    #[serde(default)]
    pub created_by: Option<String>,
    /// Parent task id, if this task is a subtask.
    #[serde(default)]
    pub parent_id: Option<String>,
    /// Task ids this task depends on.
    #[serde(default)]
    pub dependencies: Vec<String>,
    /// Opaque budget metadata (host-runtime defined; not interpreted here).
    #[serde(default)]
    pub budget: Option<Value>,
    /// RFC 3339 timestamp.
    #[serde(default)]
    pub expires_at: Option<String>,
}

/// Request body shared by `POST /v1/coordination/tasks/{id}/claim` and
/// `POST /v1/coordination/tasks/{id}/renew` — both take an expected revision,
/// a lease duration, and an optional claimed worker name.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskLeaseRequest {
    /// Revision the caller last observed; a compare-and-swap guard.
    pub expected_revision: i64,
    /// Requested lease duration in seconds, evaluated against the server's
    /// own clock — never a caller-supplied timestamp.
    pub lease_seconds: i64,
    /// Claimed worker name; see module docs.
    #[serde(default)]
    pub worker: Option<String>,
}

/// Request body for `POST /v1/coordination/tasks/{id}/transition`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TransitionTaskRequest {
    /// Revision the caller last observed; a compare-and-swap guard.
    pub expected_revision: i64,
    /// Target lifecycle state.
    pub state: CoordinationTaskState,
    /// Claimed actor name; see module docs.
    #[serde(default)]
    pub actor: Option<String>,
    /// Opaque transition metadata, stored on the resulting audit event.
    #[serde(default)]
    pub details: Option<Value>,
}

/// Wire mirror of `mempalace_storage::coordination::Message`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CoordinationMessageDto {
    /// Immutable message identifier.
    pub message_id: String,
    /// Local per-task-independent ordering sequence (not the opaque cursor;
    /// see the module docs).
    pub sequence: i64,
    /// Owning task, and this message's wing-authorization key.
    pub task_id: String,
    /// Actor recorded as having sent the message (identity-derived; see
    /// module docs).
    pub sender: String,
    /// Addressee. Taken verbatim — not identity-derived; see module docs.
    pub recipient: String,
    /// Application-defined message kind (e.g. `"status"`, `"result"`).
    pub kind: String,
    /// Message body.
    pub payload: Value,
    /// Envelope schema version, for forward-compatible payload evolution.
    pub envelope_version: i64,
    /// RFC 3339 timestamp, present once acknowledged.
    #[serde(default)]
    pub acknowledged_at: Option<String>,
    /// Acknowledging actor, present once acknowledged.
    #[serde(default)]
    pub acknowledged_by: Option<String>,
    /// RFC 3339 timestamp.
    pub created_at: String,
}

/// Request body for `POST /v1/coordination/messages`. Mirrors
/// `mempalace_storage::coordination::NewMessage`, except `sender` is claimed,
/// not authoritative.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NewMessageRequest {
    /// Owning task; its wing is this write's authorization key.
    pub task_id: String,
    /// Addressee. Taken verbatim — not identity-derived; see module docs.
    pub recipient: String,
    /// Application-defined message kind (e.g. `"status"`, `"result"`).
    pub kind: String,
    /// Message body.
    pub payload: Value,
    /// Deduplication key, scoped to the resolved `sender` actor.
    pub idempotency_key: String,
    /// Claimed sender name; see module docs.
    #[serde(default)]
    pub sender: Option<String>,
    /// Envelope schema version; defaults to `1`.
    #[serde(default = "default_envelope_version")]
    pub envelope_version: i64,
}

fn default_envelope_version() -> i64 {
    1
}

/// Request body for `POST /v1/coordination/messages/{id}/ack`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AckMessageRequest {
    /// Claimed acknowledging actor; see module docs. Storage additionally
    /// requires the resolved actor to equal the message's `recipient`.
    #[serde(default)]
    pub actor: Option<String>,
}

/// Query parameters for `GET /v1/coordination/inbox`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InboxQuery {
    /// Whose inbox to read. Taken verbatim, not identity-derived — matches
    /// the local `mempalace_inbox_read` tool's `recipient` argument.
    pub recipient: String,
    /// Optional wing filter (short or fully-qualified form; normalised
    /// server-side). Omitting it spans every wing visible to the token.
    #[serde(default)]
    pub wing: Option<String>,
    /// Opaque pagination cursor returned by a previous response.
    #[serde(default)]
    pub cursor: Option<String>,
    /// Maximum number of messages per page.
    #[serde(default)]
    pub limit: Option<usize>,
    /// When `true`, only unacknowledged messages are returned.
    #[serde(default)]
    pub unacknowledged_only: bool,
}

/// Response body for `GET /v1/coordination/inbox`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InboxPageResponse {
    /// Messages visible to the caller on this page, in sequence order.
    pub messages: Vec<CoordinationMessageDto>,
    /// Opaque cursor to pass back for the next page; `null` when exhausted.
    #[serde(default)]
    pub next_cursor: Option<String>,
}

/// Wire mirror of `mempalace_storage::coordination::Artifact`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CoordinationArtifactDto {
    /// Immutable artifact identifier.
    pub artifact_id: String,
    /// Owning task, and this artifact's wing-authorization key.
    pub task_id: String,
    /// Actor recorded as having created the artifact (identity-derived; see
    /// module docs).
    pub created_by: String,
    /// Application-defined role (e.g. `"output"`, `"log"`).
    pub role: String,
    /// MIME type of `content`.
    pub media_type: String,
    /// Verbatim artifact content.
    pub content: String,
    /// BLAKE3 hex hash of `content`.
    pub content_hash: String,
    /// RFC 3339 timestamp.
    pub created_at: String,
}

/// Request body for `POST /v1/coordination/artifacts`. Mirrors
/// `mempalace_storage::coordination::NewArtifact`, except `created_by` is
/// claimed, not authoritative.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NewArtifactRequest {
    /// Owning task; its wing is this write's authorization key.
    pub task_id: String,
    /// Application-defined role (e.g. `"output"`, `"log"`).
    pub role: String,
    /// MIME type of `content`.
    pub media_type: String,
    /// Verbatim artifact content.
    pub content: String,
    /// Deduplication key, scoped to the resolved `created_by` actor.
    pub idempotency_key: String,
    /// Claimed creator name; see module docs.
    #[serde(default)]
    pub created_by: Option<String>,
}

/// Wire mirror of `mempalace_storage::coordination::TaskResult`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CoordinationTaskResultDto {
    /// Immutable result identifier.
    pub result_id: String,
    /// Owning task, and this result's wing-authorization key.
    pub task_id: String,
    /// Actor recorded as having created the result (identity-derived; see
    /// module docs).
    pub created_by: String,
    /// Result payload.
    pub payload: Value,
    /// RFC 3339 timestamp.
    pub created_at: String,
}

/// Request body for `POST /v1/coordination/results`. Mirrors
/// `mempalace_storage::coordination::NewTaskResult`, except `created_by` is
/// claimed, not authoritative.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NewTaskResultRequest {
    /// Owning task; its wing is this write's authorization key.
    pub task_id: String,
    /// Result payload.
    pub payload: Value,
    /// Deduplication key, scoped to the resolved `created_by` actor.
    pub idempotency_key: String,
    /// Claimed creator name; see module docs.
    #[serde(default)]
    pub created_by: Option<String>,
}

/// Wire mirror of `mempalace_storage::coordination::CoordinationEvent`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CoordinationEventDto {
    /// Local ordering sequence (not the opaque cursor; see the module docs).
    pub sequence: i64,
    /// Immutable event identifier.
    pub event_id: String,
    /// Kind of entity this event describes (e.g. `"task"`, `"message"`).
    pub entity_type: String,
    /// Identifier of the affected entity.
    pub entity_id: String,
    /// Owning task, when the event has one.
    #[serde(default)]
    pub task_id: Option<String>,
    /// The owning task's normalised wing, materialised server-side at write
    /// time — never supplied by a caller.
    pub wing: String,
    /// Event kind (e.g. `"task_created"`, `"lease_renewed"`).
    pub event_type: String,
    /// Actor who performed the recorded mutation.
    pub actor: String,
    /// Task state before the mutation, for task-transition events.
    #[serde(default)]
    pub from_state: Option<CoordinationTaskState>,
    /// Task state after the mutation, for task-transition events.
    #[serde(default)]
    pub to_state: Option<CoordinationTaskState>,
    /// Task revision resulting from the mutation, when applicable.
    #[serde(default)]
    pub revision: Option<i64>,
    /// Opaque event-specific metadata.
    #[serde(default)]
    pub details: Option<Value>,
    /// RFC 3339 timestamp.
    pub occurred_at: String,
}

/// Query parameters for `GET /v1/coordination/events`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CoordinationEventsQuery {
    /// Opaque pagination cursor returned by a previous response.
    #[serde(default)]
    pub cursor: Option<String>,
    /// Optional filter to one task's events.
    #[serde(default)]
    pub task_id: Option<String>,
    /// Optional wing filter (short or fully-qualified form; normalised
    /// server-side). Omitting it spans every wing visible to the token —
    /// this is an aggregate feed, filtered rather than rejected; see the
    /// federated-coordination part of docs/Federation.md.
    #[serde(default)]
    pub wing: Option<String>,
    /// Maximum number of events per page.
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Response body for `GET /v1/coordination/events`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CoordinationEventsResponse {
    /// Events visible to the caller on this page, in sequence order.
    pub events: Vec<CoordinationEventDto>,
    /// Opaque cursor to pass back for the next page; `null` when exhausted.
    #[serde(default)]
    pub next_cursor: Option<String>,
}

// ─── Error ────────────────────────────────────────────────────────────────────

/// Standard error response body returned by all federation endpoints on failure.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ErrorBody {
    /// Machine-readable error code (e.g. `"not_found"`, `"invalid_params"`).
    pub code: String,
    /// Human-readable error message.
    pub message: String,
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn info_response_round_trips() {
        let original = InfoResponse {
            server_version: "2.0.0".to_owned(),
            federation_api_version: FEDERATION_API_VERSION,
            embedding_profile: "balanced".to_owned(),
            capabilities: vec!["drawers".to_owned(), "kg".to_owned()],
            maintenance_enabled: true,
            maintenance_background_enabled: true,
            maintenance_idle_secs: 300,
            maintenance_last_run: None,
            maintenance_status: MaintenanceStatus::Idle,
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: InfoResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn maintenance_status_disabled_serde() {
        let raw = r#"{"maintenance_status":"disabled"}"#;
        let info: InfoResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(info.maintenance_status, MaintenanceStatus::Disabled);
    }

    #[test]
    fn maintenance_status_idle_serde() {
        let raw = r#"{"maintenance_status":"idle"}"#;
        let info: InfoResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(info.maintenance_status, MaintenanceStatus::Idle);
    }

    #[test]
    fn maintenance_status_running_serde() {
        let raw = r#"{"maintenance_status":"running"}"#;
        let info: InfoResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(info.maintenance_status, MaintenanceStatus::Running);
    }

    #[test]
    fn maintenance_status_skipped_not_idle() {
        let raw = r#"{"maintenance_status":{"skipped":{"reason":"not_idle"}}}"#;
        let info: InfoResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(
            info.maintenance_status,
            MaintenanceStatus::Skipped { reason: MaintenanceSkipReason::NotIdle }
        );
    }

    #[test]
    fn maintenance_status_skipped_nothing_to_do() {
        let raw = r#"{"maintenance_status":{"skipped":{"reason":"nothing_to_do"}}}"#;
        let info: InfoResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(
            info.maintenance_status,
            MaintenanceStatus::Skipped { reason: MaintenanceSkipReason::NothingToDo }
        );
    }

    #[test]
    fn maintenance_status_aborted_concurrent_run() {
        let raw = r#"{"maintenance_status":{"aborted":{"reason":"concurrent_run"}}}"#;
        let info: InfoResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(
            info.maintenance_status,
            MaintenanceStatus::Aborted { reason: MaintenanceAbortReason::ConcurrentRun }
        );
    }

    #[test]
    fn maintenance_status_aborted_shutdown() {
        let raw = r#"{"maintenance_status":{"aborted":{"reason":"shutdown"}}}"#;
        let info: InfoResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(
            info.maintenance_status,
            MaintenanceStatus::Aborted { reason: MaintenanceAbortReason::Shutdown }
        );
    }

    #[test]
    fn maintenance_status_aborted_timeout() {
        let raw = r#"{"maintenance_status":{"aborted":{"reason":"timeout"}}}"#;
        let info: InfoResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(
            info.maintenance_status,
            MaintenanceStatus::Aborted { reason: MaintenanceAbortReason::Timeout }
        );
    }

    #[test]
    fn maintenance_status_failed_with_message() {
        let raw = r#"{"maintenance_status":{"failed":{"message":"disk full"}}}"#;
        let info: InfoResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(
            info.maintenance_status,
            MaintenanceStatus::Failed { message: "disk full".into() }
        );
    }

    #[test]
    fn maintenance_status_completed_success() {
        let raw = r#"{"maintenance_status":{"completed":{"status":"success"}}}"#;
        let info: InfoResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(
            info.maintenance_status,
            MaintenanceStatus::Completed { status: MaintenanceRunStatus::Success }
        );
    }

    #[test]
    fn maintenance_status_completed_partial() {
        let raw = r#"{"maintenance_status":{"completed":{"status":"partial"}}}"#;
        let info: InfoResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(
            info.maintenance_status,
            MaintenanceStatus::Completed { status: MaintenanceRunStatus::Partial }
        );
    }

    #[test]
    fn maintenance_status_completed_failure() {
        let raw = r#"{"maintenance_status":{"completed":{"status":"failure"}}}"#;
        let info: InfoResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(
            info.maintenance_status,
            MaintenanceStatus::Completed { status: MaintenanceRunStatus::Failure }
        );
    }

    #[test]
    fn maintenance_status_default_is_idle() {
        let raw = r#"{}"#;
        let info: InfoResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(info.maintenance_status, MaintenanceStatus::Idle);
    }

    #[test]
    fn drawer_search_request_sparse_json_deserialises() {
        // Only `query` is required; optional fields should default to None.
        let raw = r#"{"query":"hello"}"#;
        let req: DrawerSearchRequest = serde_json::from_str(raw).unwrap();
        assert_eq!(req.query, "hello");
        assert!(req.wing.is_none());
        assert!(req.room.is_none());
        assert!(req.limit.is_none());
    }

    #[test]
    fn drawer_search_response_round_trips() {
        let original = DrawerSearchResponse {
            results: vec![RemoteDrawerResult {
                drawer_id: "drw_1".to_owned(),
                wing: "wing_code".to_owned(),
                room: "backend".to_owned(),
                rank: 1,
                score: 0.95,
                content: "hello world".to_owned(),
                source_file: Some("src/main.rs".to_owned()),
                content_hash: None,
                filed_at: None,
                added_by: None,
                stale: false,
            }],
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: DrawerSearchResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn add_drawer_request_sparse_json_deserialises() {
        let raw = r#"{"wing":"w","room":"r","content":"c"}"#;
        let req: AddDrawerRequest = serde_json::from_str(raw).unwrap();
        assert_eq!(req.wing, "w");
        assert!(req.source_file.is_none());
        assert!(req.added_by.is_none());
        assert!(req.drawer_id.is_none());
        assert!(req.operation_id.is_none());
    }

    #[test]
    fn add_drawer_request_round_trips_with_replication_fields() {
        let original = AddDrawerRequest {
            wing: "w".to_owned(),
            room: "r".to_owned(),
            content: "c".to_owned(),
            source_file: None,
            added_by: Some("claude".to_owned()),
            drawer_id: Some("drw_stable_local_id".to_owned()),
            operation_id: Some("op-add-42".to_owned()),
        };
        let json = serde_json::to_string(&original).unwrap();
        assert!(json.contains("drw_stable_local_id"), "drawer_id must serialize: {json}");
        assert!(json.contains("op-add-42"), "operation_id must serialize: {json}");
        let decoded: AddDrawerRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn add_drawer_request_omits_new_fields_when_none() {
        // A pre-replication caller sends no operation identity at all: the new
        // fields must be absent from the wire, not `null`.
        let req = AddDrawerRequest {
            wing: "w".to_owned(),
            room: "r".to_owned(),
            content: "c".to_owned(),
            source_file: None,
            added_by: None,
            drawer_id: None,
            operation_id: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(!json.contains("drawer_id"), "None drawer_id must be omitted: {json}");
        assert!(!json.contains("operation_id"), "None operation_id must be omitted: {json}");
    }

    #[test]
    fn check_duplicate_request_sparse_json_deserialises() {
        let raw = r#"{"content":"some text"}"#;
        let req: CheckDuplicateRequest = serde_json::from_str(raw).unwrap();
        assert_eq!(req.content, "some text");
        assert!(req.threshold.is_none());
    }

    #[test]
    fn list_drawers_query_sparse_json_deserialises() {
        let raw = r#"{}"#;
        let q: ListDrawersQuery = serde_json::from_str(raw).unwrap();
        assert!(q.wing.is_none());
        assert!(q.cursor.is_none());
    }

    #[test]
    fn kg_requests_round_trip() {
        let add = KgAddFactRequest {
            subject: "Alice".to_owned(),
            predicate: "works_on".to_owned(),
            object: "MemPalace".to_owned(),
            valid_from: Some("2026-01-01".to_owned()),
            operation_id: None,
        };
        let json = serde_json::to_string(&add).unwrap();
        let decoded: KgAddFactRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(add, decoded);

        let inv = KgInvalidateRequest {
            subject: "Alice".to_owned(),
            predicate: "works_on".to_owned(),
            object: "MemPalace".to_owned(),
            ended: None,
            operation_id: None,
        };
        let json = serde_json::to_string(&inv).unwrap();
        let decoded: KgInvalidateRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(inv, decoded);
    }

    #[test]
    fn kg_mutation_requests_round_trip_with_operation_id() {
        let add = KgAddFactRequest {
            subject: "Alice".to_owned(),
            predicate: "works_on".to_owned(),
            object: "MemPalace".to_owned(),
            valid_from: None,
            operation_id: Some("rg-op-add-1".to_owned()),
        };
        let json = serde_json::to_string(&add).unwrap();
        assert!(json.contains("rg-op-add-1"), "operation_id must serialize: {json}");
        let decoded: KgAddFactRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(add, decoded);
    }

    #[test]
    fn kg_mutation_requests_sparse_json_deserialises_and_omits_when_none() {
        // Old caller JSON without the new field.
        let add: KgAddFactRequest =
            serde_json::from_str(r#"{"subject":"A","predicate":"p","object":"B"}"#).unwrap();
        assert!(add.operation_id.is_none());

        let inv: KgInvalidateRequest =
            serde_json::from_str(r#"{"subject":"A","predicate":"p","object":"B"}"#).unwrap();
        assert!(inv.operation_id.is_none());

        // Fresh struct with `None` must not put the field on the wire.
        let fresh = KgAddFactRequest {
            subject: "A".to_owned(),
            predicate: "p".to_owned(),
            object: "B".to_owned(),
            valid_from: None,
            operation_id: None,
        };
        let json = serde_json::to_string(&fresh).unwrap();
        assert!(!json.contains("operation_id"), "None must omit the field: {json}");
    }

    #[test]
    fn delete_drawer_query_round_trips() {
        let original = DeleteDrawerQuery { operation_id: Some("op-del-7".to_owned()) };
        let json = serde_json::to_string(&original).unwrap();
        assert!(json.contains("op-del-7"), "operation_id must serialize: {json}");
        let decoded: DeleteDrawerQuery = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn delete_drawer_query_sparse_json_deserialises() {
        let empty: DeleteDrawerQuery = serde_json::from_str("{}").unwrap();
        assert!(empty.operation_id.is_none());

        let fresh = DeleteDrawerQuery { operation_id: None };
        let json = serde_json::to_string(&fresh).unwrap();
        assert!(!json.contains("operation_id"), "None must omit the field: {json}");
    }

    #[test]
    fn changes_response_round_trips() {
        let original = ChangesResponse {
            events: vec![ChangeEventDto {
                event_type: "drawer_added".to_owned(),
                occurred_at: "2026-01-01T00:00:00Z".to_owned(),
                entity_id: "drw_abc".to_owned(),
                actor: Some("claude".to_owned()),
                details: Some(json!({"wing":"wing_code","room":"backend"})),
            }],
            next_cursor: Some("cursor_opaque".to_owned()),
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: ChangesResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn error_body_round_trips() {
        let err =
            ErrorBody { code: "not_found".to_owned(), message: "Drawer not found".to_owned() };
        let json = serde_json::to_string(&err).unwrap();
        let decoded: ErrorBody = serde_json::from_str(&json).unwrap();
        assert_eq!(err, decoded);
    }

    #[test]
    fn ingest_batch_request_round_trips() {
        let original = IngestBatchRequest {
            wing: "wing_myproject".to_owned(),
            repo_id: "github.com/acme/myrepo".to_owned(),
            agent: Some("claude".to_owned()),
            commit_hash: Some("abc123def456".to_owned()),
            files: vec![IngestFileDto {
                relative_path: "src/main.rs".to_owned(),
                content_hash: "contenthash1".to_owned(),
                file_hash: Some("filehash1".to_owned()),
                chunks: vec![IngestChunkDto {
                    chunk_index: 0,
                    room: "backend".to_owned(),
                    text: "fn main() {}".to_owned(),
                    byte_start: Some(0),
                    byte_end: Some(12),
                    line_start: Some(1),
                    line_end: Some(1),
                }],
            }],
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: IngestBatchRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn ingest_batch_response_round_trips() {
        let original = IngestBatchResponse {
            files: vec![
                IngestFileResult {
                    relative_path: "src/main.rs".to_owned(),
                    status: "ingested".to_owned(),
                    drawers_written: 3,
                    error: None,
                },
                IngestFileResult {
                    relative_path: "src/lib.rs".to_owned(),
                    status: "skipped_unchanged".to_owned(),
                    drawers_written: 0,
                    error: None,
                },
                IngestFileResult {
                    relative_path: "src/broken.rs".to_owned(),
                    status: "failed".to_owned(),
                    drawers_written: 0,
                    error: Some("embedding failed".to_owned()),
                },
            ],
            warnings: vec!["no checkout configured for wing 'wing_x'".to_owned()],
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: IngestBatchResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn ingest_batch_request_sparse_json_deserialises() {
        // Only required fields present; optional fields default to None / empty.
        let raw = r#"{
            "wing": "wing_proj",
            "repo_id": "github.com/org/repo",
            "files": [
                {
                    "relative_path": "README.md",
                    "content_hash": "ch1",
                    "chunks": [
                        {"chunk_index": 0, "room": "docs", "text": "hello"}
                    ]
                }
            ]
        }"#;
        let req: IngestBatchRequest = serde_json::from_str(raw).unwrap();
        assert_eq!(req.wing, "wing_proj");
        assert_eq!(req.repo_id, "github.com/org/repo");
        assert!(req.agent.is_none());
        assert!(req.commit_hash.is_none());
        assert_eq!(req.files.len(), 1);
        assert!(req.files[0].file_hash.is_none());
        let chunk = &req.files[0].chunks[0];
        assert_eq!(chunk.chunk_index, 0);
        assert!(chunk.byte_start.is_none());
        assert!(chunk.byte_end.is_none());
        assert!(chunk.line_start.is_none());
        assert!(chunk.line_end.is_none());
    }

    #[test]
    fn ingest_batch_response_sparse_json_deserialises() {
        // Only required fields; drawers_written defaults to 0, warnings to empty vec.
        let raw = r#"{
            "files": [
                {"relative_path": "src/main.rs", "status": "ingested"}
            ]
        }"#;
        let resp: IngestBatchResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(resp.files.len(), 1);
        assert_eq!(resp.files[0].drawers_written, 0);
        assert!(resp.files[0].error.is_none());
        assert!(resp.warnings.is_empty());
    }

    // ─── Coordination DTOs (issue #102 Stage 3) ──────────────────────────────

    #[test]
    fn coordination_task_state_round_trips_every_variant() {
        let variants = [
            (CoordinationTaskState::Pending, "\"pending\""),
            (CoordinationTaskState::Running, "\"running\""),
            (CoordinationTaskState::InputRequired, "\"input_required\""),
            (CoordinationTaskState::Completed, "\"completed\""),
            (CoordinationTaskState::Cancelled, "\"cancelled\""),
            (CoordinationTaskState::Failed, "\"failed\""),
            (CoordinationTaskState::Expired, "\"expired\""),
        ];
        for (state, expected_json) in variants {
            let json = serde_json::to_string(&state).unwrap();
            assert_eq!(json, expected_json);
            let decoded: CoordinationTaskState = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded, state);
        }
    }

    #[test]
    fn coordination_task_dto_round_trips() {
        let original = CoordinationTaskDto {
            task_id: "task_1".to_owned(),
            title: "Do the thing".to_owned(),
            description: "details".to_owned(),
            state: CoordinationTaskState::Running,
            revision: 2,
            created_by: "alice".to_owned(),
            wing: "wing_myproject".to_owned(),
            owner: Some("worker-1".to_owned()),
            parent_id: Some("task_0".to_owned()),
            dependencies: vec!["task_dep".to_owned()],
            budget: Some(json!({"tokens": 100})),
            lease_expires_at: Some("2026-01-01T00:05:00Z".to_owned()),
            expires_at: None,
            created_at: "2026-01-01T00:00:00Z".to_owned(),
            updated_at: "2026-01-01T00:01:00Z".to_owned(),
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: CoordinationTaskDto = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn coordination_task_dto_sparse_json_deserialises() {
        let raw = r#"{
            "task_id": "task_1", "title": "t", "description": "d", "state": "pending",
            "revision": 0, "created_by": "alice", "wing": "wing_x",
            "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z"
        }"#;
        let dto: CoordinationTaskDto = serde_json::from_str(raw).unwrap();
        assert!(dto.owner.is_none());
        assert!(dto.parent_id.is_none());
        assert!(dto.dependencies.is_empty());
        assert!(dto.budget.is_none());
        assert!(dto.lease_expires_at.is_none());
        assert!(dto.expires_at.is_none());
    }

    #[test]
    fn new_task_request_round_trips() {
        let original = NewTaskRequest {
            title: "t".to_owned(),
            description: "d".to_owned(),
            wing: "wing_x".to_owned(),
            idempotency_key: "key1".to_owned(),
            created_by: Some("alice".to_owned()),
            parent_id: Some("task_0".to_owned()),
            dependencies: vec!["task_dep".to_owned()],
            budget: Some(json!({"tokens": 10})),
            expires_at: Some("2026-01-01T00:00:00Z".to_owned()),
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: NewTaskRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn new_task_request_sparse_json_deserialises() {
        let raw = r#"{"title":"t","description":"d","wing":"wing_x","idempotency_key":"key1"}"#;
        let req: NewTaskRequest = serde_json::from_str(raw).unwrap();
        assert!(req.created_by.is_none());
        assert!(req.parent_id.is_none());
        assert!(req.dependencies.is_empty());
        assert!(req.budget.is_none());
        assert!(req.expires_at.is_none());
    }

    #[test]
    fn task_lease_request_round_trips() {
        let original = TaskLeaseRequest {
            expected_revision: 3,
            lease_seconds: 600,
            worker: Some("worker-1".to_owned()),
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: TaskLeaseRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn task_lease_request_sparse_json_deserialises() {
        let raw = r#"{"expected_revision":0,"lease_seconds":60}"#;
        let req: TaskLeaseRequest = serde_json::from_str(raw).unwrap();
        assert!(req.worker.is_none());
    }

    #[test]
    fn transition_task_request_round_trips() {
        let original = TransitionTaskRequest {
            expected_revision: 1,
            state: CoordinationTaskState::Completed,
            actor: Some("worker-1".to_owned()),
            details: Some(json!({"note": "done"})),
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: TransitionTaskRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn transition_task_request_sparse_json_deserialises() {
        let raw = r#"{"expected_revision":1,"state":"cancelled"}"#;
        let req: TransitionTaskRequest = serde_json::from_str(raw).unwrap();
        assert!(req.actor.is_none());
        assert!(req.details.is_none());
    }

    #[test]
    fn coordination_message_dto_round_trips() {
        let original = CoordinationMessageDto {
            message_id: "message_1".to_owned(),
            sequence: 5,
            task_id: "task_1".to_owned(),
            sender: "alice".to_owned(),
            recipient: "bob".to_owned(),
            kind: "status".to_owned(),
            payload: json!({"ok": true}),
            envelope_version: 1,
            acknowledged_at: Some("2026-01-01T00:00:00Z".to_owned()),
            acknowledged_by: Some("bob".to_owned()),
            created_at: "2026-01-01T00:00:00Z".to_owned(),
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: CoordinationMessageDto = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn coordination_message_dto_sparse_json_deserialises() {
        let raw = r#"{
            "message_id":"message_1","sequence":1,"task_id":"task_1","sender":"a",
            "recipient":"b","kind":"status","payload":{},"envelope_version":1,
            "created_at":"2026-01-01T00:00:00Z"
        }"#;
        let dto: CoordinationMessageDto = serde_json::from_str(raw).unwrap();
        assert!(dto.acknowledged_at.is_none());
        assert!(dto.acknowledged_by.is_none());
    }

    #[test]
    fn new_message_request_round_trips() {
        let original = NewMessageRequest {
            task_id: "task_1".to_owned(),
            recipient: "bob".to_owned(),
            kind: "status".to_owned(),
            payload: json!({"ok": true}),
            idempotency_key: "key1".to_owned(),
            sender: Some("alice".to_owned()),
            envelope_version: 2,
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: NewMessageRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn new_message_request_sparse_json_deserialises() {
        let raw = r#"{
            "task_id":"task_1","recipient":"b","kind":"status","payload":{},
            "idempotency_key":"key1"
        }"#;
        let req: NewMessageRequest = serde_json::from_str(raw).unwrap();
        assert!(req.sender.is_none());
        assert_eq!(req.envelope_version, 1);
    }

    #[test]
    fn ack_message_request_round_trips() {
        let original = AckMessageRequest { actor: Some("bob".to_owned()) };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: AckMessageRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn ack_message_request_sparse_json_deserialises() {
        let req: AckMessageRequest = serde_json::from_str("{}").unwrap();
        assert!(req.actor.is_none());
    }

    #[test]
    fn inbox_query_round_trips() {
        let original = InboxQuery {
            recipient: "bob".to_owned(),
            wing: Some("wing_x".to_owned()),
            cursor: Some("42".to_owned()),
            limit: Some(50),
            unacknowledged_only: true,
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: InboxQuery = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn inbox_query_sparse_json_deserialises() {
        let raw = r#"{"recipient":"bob"}"#;
        let query: InboxQuery = serde_json::from_str(raw).unwrap();
        assert!(query.wing.is_none());
        assert!(query.cursor.is_none());
        assert!(query.limit.is_none());
        assert!(!query.unacknowledged_only);
    }

    #[test]
    fn inbox_page_response_round_trips() {
        let original = InboxPageResponse {
            messages: vec![CoordinationMessageDto {
                message_id: "message_1".to_owned(),
                sequence: 1,
                task_id: "task_1".to_owned(),
                sender: "a".to_owned(),
                recipient: "b".to_owned(),
                kind: "status".to_owned(),
                payload: json!({}),
                envelope_version: 1,
                acknowledged_at: None,
                acknowledged_by: None,
                created_at: "2026-01-01T00:00:00Z".to_owned(),
            }],
            next_cursor: Some("7".to_owned()),
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: InboxPageResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn inbox_page_response_sparse_json_deserialises() {
        let raw = r#"{"messages":[]}"#;
        let resp: InboxPageResponse = serde_json::from_str(raw).unwrap();
        assert!(resp.messages.is_empty());
        assert!(resp.next_cursor.is_none());
    }

    #[test]
    fn coordination_artifact_dto_round_trips() {
        let original = CoordinationArtifactDto {
            artifact_id: "artifact_1".to_owned(),
            task_id: "task_1".to_owned(),
            created_by: "alice".to_owned(),
            role: "output".to_owned(),
            media_type: "text/plain".to_owned(),
            content: "hello".to_owned(),
            content_hash: "abc123".to_owned(),
            created_at: "2026-01-01T00:00:00Z".to_owned(),
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: CoordinationArtifactDto = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn new_artifact_request_round_trips() {
        let original = NewArtifactRequest {
            task_id: "task_1".to_owned(),
            role: "output".to_owned(),
            media_type: "text/plain".to_owned(),
            content: "hello".to_owned(),
            idempotency_key: "key1".to_owned(),
            created_by: Some("alice".to_owned()),
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: NewArtifactRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn new_artifact_request_sparse_json_deserialises() {
        let raw = r#"{
            "task_id":"task_1","role":"output","media_type":"text/plain",
            "content":"hello","idempotency_key":"key1"
        }"#;
        let req: NewArtifactRequest = serde_json::from_str(raw).unwrap();
        assert!(req.created_by.is_none());
    }

    #[test]
    fn coordination_task_result_dto_round_trips() {
        let original = CoordinationTaskResultDto {
            result_id: "result_1".to_owned(),
            task_id: "task_1".to_owned(),
            created_by: "alice".to_owned(),
            payload: json!({"score": 1}),
            created_at: "2026-01-01T00:00:00Z".to_owned(),
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: CoordinationTaskResultDto = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn new_task_result_request_round_trips() {
        let original = NewTaskResultRequest {
            task_id: "task_1".to_owned(),
            payload: json!({"score": 1}),
            idempotency_key: "key1".to_owned(),
            created_by: Some("alice".to_owned()),
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: NewTaskResultRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn new_task_result_request_sparse_json_deserialises() {
        let raw = r#"{"task_id":"task_1","payload":{},"idempotency_key":"key1"}"#;
        let req: NewTaskResultRequest = serde_json::from_str(raw).unwrap();
        assert!(req.created_by.is_none());
    }

    #[test]
    fn coordination_event_dto_round_trips() {
        let original = CoordinationEventDto {
            sequence: 9,
            event_id: "event_1".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: "task_1".to_owned(),
            task_id: Some("task_1".to_owned()),
            wing: "wing_x".to_owned(),
            event_type: "task_created".to_owned(),
            actor: "alice".to_owned(),
            from_state: None,
            to_state: Some(CoordinationTaskState::Pending),
            revision: Some(0),
            details: Some(json!({"note": "x"})),
            occurred_at: "2026-01-01T00:00:00Z".to_owned(),
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: CoordinationEventDto = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn coordination_event_dto_sparse_json_deserialises() {
        let raw = r#"{
            "sequence":1,"event_id":"event_1","entity_type":"task","entity_id":"task_1",
            "wing":"wing_x","event_type":"task_created","actor":"alice",
            "occurred_at":"2026-01-01T00:00:00Z"
        }"#;
        let dto: CoordinationEventDto = serde_json::from_str(raw).unwrap();
        assert!(dto.task_id.is_none());
        assert!(dto.from_state.is_none());
        assert!(dto.to_state.is_none());
        assert!(dto.revision.is_none());
        assert!(dto.details.is_none());
    }

    #[test]
    fn coordination_events_query_sparse_json_deserialises() {
        let query: CoordinationEventsQuery = serde_json::from_str("{}").unwrap();
        assert!(query.cursor.is_none());
        assert!(query.task_id.is_none());
        assert!(query.wing.is_none());
        assert!(query.limit.is_none());
    }

    #[test]
    fn coordination_events_query_round_trips() {
        let original = CoordinationEventsQuery {
            cursor: Some("3".to_owned()),
            task_id: Some("task_1".to_owned()),
            wing: Some("wing_x".to_owned()),
            limit: Some(20),
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: CoordinationEventsQuery = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn coordination_events_response_round_trips() {
        let original = CoordinationEventsResponse {
            events: vec![CoordinationEventDto {
                sequence: 1,
                event_id: "event_1".to_owned(),
                entity_type: "task".to_owned(),
                entity_id: "task_1".to_owned(),
                task_id: Some("task_1".to_owned()),
                wing: "wing_x".to_owned(),
                event_type: "task_created".to_owned(),
                actor: "alice".to_owned(),
                from_state: None,
                to_state: Some(CoordinationTaskState::Pending),
                revision: Some(0),
                details: None,
                occurred_at: "2026-01-01T00:00:00Z".to_owned(),
            }],
            next_cursor: None,
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: CoordinationEventsResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn coordination_events_response_sparse_json_deserialises() {
        let raw = r#"{"events":[]}"#;
        let resp: CoordinationEventsResponse = serde_json::from_str(raw).unwrap();
        assert!(resp.events.is_empty());
        assert!(resp.next_cursor.is_none());
    }
}

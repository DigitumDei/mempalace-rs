//! HTTP client for the MemPalace federation REST API.
//!
//! This crate provides:
//!
//! - [`RemoteEndpoint`] — connection parameters for one remote palace.
//! - [`RemoteApi`] — async trait mirroring every `/v1` endpoint of the
//!   federation REST API.
//! - [`RemoteClient`] — a [`reqwest`]-backed implementation of [`RemoteApi`].
//! - [`RemoteError`] / [`Result`] — error type and convenience alias.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use mempalace_remote::{RemoteClient, RemoteEndpoint, RemoteApi, DEFAULT_TIMEOUT};
//!
//! # async fn example() -> mempalace_remote::Result<()> {
//! let endpoint = RemoteEndpoint {
//!     name: "friend-palace".to_owned(),
//!     base_url: "https://palace.example".to_owned(),
//!     token: Some("secret-token".to_owned()),
//!     timeout: DEFAULT_TIMEOUT,
//! };
//! let client = RemoteClient::new(endpoint)?;
//! let info = client.info().await?;
//! println!("Connected to server v{}", info.server_version);
//! # Ok(())
//! # }
//! ```

mod client;
mod error;

pub use client::RemoteClient;
pub use error::{RemoteError, Result};

use mempalace_federation::{
    AckMessageRequest, AddDrawerRequest, AddDrawerResponse, ChangesQuery, ChangesResponse,
    CheckDuplicateRequest, CheckDuplicateResponse, CoordinationArtifactDto,
    CoordinationEventsQuery, CoordinationEventsResponse, CoordinationMessageDto,
    CoordinationTaskDto, CoordinationTaskResultDto, DrawerSearchRequest, DrawerSearchResponse,
    InboxPageResponse, InboxQuery, InfoResponse, IngestBatchRequest, IngestBatchResponse,
    KgAddFactRequest, KgInvalidateRequest, KgQueryRequest, ListDrawersQuery, ListDrawersResponse,
    NewArtifactRequest, NewMessageRequest, NewTaskRequest, NewTaskResultRequest, TaskLeaseRequest,
    TransitionTaskRequest,
};

/// Default per-request timeout for remote calls.
pub const DEFAULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Result of a coordination write that can be rejected by a stale `expected_revision` rather
/// than a hard failure. The remote reports its actual current revision so the caller can reload
/// and retry — mirroring `mempalace_storage::RevisionedWrite<T>` (Phase 3 Stage 4) so a
/// revision conflict has one shape whether it came from local storage or a remote peer.
///
/// This crate deliberately does not depend on `mempalace-storage` (it is meant to stay a
/// lightweight HTTP client), so this is its own type rather than a re-export; the routing layer
/// (`mempalace-mcp::federation::FederationRouter`), which already depends on both crates, is
/// where the two shapes are reconciled.
#[derive(Debug, Clone, PartialEq)]
pub enum RemoteRevisionedWrite<T> {
    /// The write was applied; carries the resulting record.
    Applied(T),
    /// The remote rejected the write because `expected_revision` no longer matched.
    Conflict {
        /// The remote's actual current revision, when it reported one.
        actual_revision: Option<i64>,
    },
}

/// Builds the [`RemoteError`] every coordination method's default trait body returns — see the
/// `RemoteApi` trait-level comment on why those methods have defaults at all. Shaped like
/// [`RemoteError::RemoteRejected`] with a synthetic HTTP 501: "this operation is not available"
/// is conceptually the same rejection a peer that has never heard of `/v1/coordination/*` would
/// give, just detected locally instead of over the wire.
fn coordination_unsupported(operation: &str) -> RemoteError {
    RemoteError::RemoteRejected {
        remote: "<unbound>".to_owned(),
        status: 501,
        body: format!("{operation} is not implemented by this RemoteApi implementation"),
    }
}

/// Connection parameters for one remote palace.
///
/// Plain values — the routing layer maps `mempalace_config::ResolvedRemote`
/// into this struct before handing it to [`RemoteClient::new`].
#[derive(Debug, Clone)]
pub struct RemoteEndpoint {
    /// Display name used in errors and logs.
    pub name: String,
    /// Base URL, e.g. `https://palace.example` (with or without trailing slash).
    pub base_url: String,
    /// Bearer token, if the remote requires authentication.
    pub token: Option<String>,
    /// Per-request timeout applied to every call made through this endpoint.
    pub timeout: std::time::Duration,
}

/// One method per `/v1` endpoint of the federation REST API.
///
/// All methods perform a version-handshake on the first call (via `GET /v1/info`)
/// and cache the result.  Subsequent calls are fast.  `/v1/health` is
/// deliberately excluded — health-checking is a deployment concern, not an API
/// concern.
#[async_trait::async_trait]
pub trait RemoteApi: Send + Sync {
    /// Return server information and capabilities (`GET /v1/info`).
    async fn info(&self) -> Result<InfoResponse>;

    /// Search drawers using semantic full-text matching (`POST /v1/drawers/search`).
    async fn search_drawers(&self, req: DrawerSearchRequest) -> Result<DrawerSearchResponse>;

    /// Check whether content is a near-duplicate of an existing drawer
    /// (`POST /v1/drawers/check_duplicate`).
    async fn check_duplicate(&self, req: CheckDuplicateRequest) -> Result<CheckDuplicateResponse>;

    /// Add a new drawer to the remote palace (`POST /v1/drawers`).
    async fn add_drawer(&self, req: AddDrawerRequest) -> Result<AddDrawerResponse>;

    /// List drawers with optional filtering and pagination (`GET /v1/drawers`).
    async fn list_drawers(&self, query: ListDrawersQuery) -> Result<ListDrawersResponse>;

    /// Retrieve a single drawer by its stable identifier (`GET /v1/drawers/{id}`).
    async fn get_drawer(&self, drawer_id: &str) -> Result<serde_json::Value>;

    /// Delete a drawer by its stable identifier (`DELETE /v1/drawers/{id}`).
    async fn delete_drawer(&self, drawer_id: &str) -> Result<()>;

    /// Query the knowledge graph for an entity (`POST /v1/kg/query`).
    async fn kg_query(&self, req: KgQueryRequest) -> Result<serde_json::Value>;

    /// Add a fact to the knowledge graph (`POST /v1/kg/facts`).
    async fn kg_add_fact(&self, req: KgAddFactRequest) -> Result<serde_json::Value>;

    /// Invalidate a knowledge-graph fact (`POST /v1/kg/facts/invalidate`).
    async fn kg_invalidate(&self, req: KgInvalidateRequest) -> Result<serde_json::Value>;

    /// Retrieve the knowledge-graph timeline, optionally filtered by entity
    /// (`GET /v1/kg/timeline?entity=`).
    ///
    /// Pass `None` to retrieve the full timeline.
    async fn kg_timeline(&self, entity: Option<&str>) -> Result<serde_json::Value>;

    /// Retrieve knowledge-graph statistics (`GET /v1/kg/stats`).
    async fn kg_stats(&self) -> Result<serde_json::Value>;

    /// Retrieve the palace taxonomy (`GET /v1/taxonomy`).
    async fn taxonomy(&self) -> Result<serde_json::Value>;

    /// List the wings in the remote palace (`GET /v1/wings`).
    async fn wings(&self) -> Result<serde_json::Value>;

    /// List rooms, optionally filtered by wing (`GET /v1/rooms?wing=`).
    ///
    /// Pass `None` to list all rooms.
    async fn rooms(&self, wing: Option<&str>) -> Result<serde_json::Value>;

    /// Retrieve paginated change events (`GET /v1/changes`).
    async fn changes(&self, query: ChangesQuery) -> Result<ChangesResponse>;

    /// Bulk-ingest pre-chunked file content into the remote palace
    /// (`POST /v1/ingest/batch`).
    ///
    /// The server embeds each chunk and writes drawers using a deterministic
    /// source-key derived from `wing`, `repo_id`, and `relative_path`, so that
    /// two clients pushing the same repository converge on identical drawer ids.
    async fn ingest_batch(&self, req: IngestBatchRequest) -> Result<IngestBatchResponse>;

    // ─── Coordination (issue #102 Stage 4) ─────────────────────────────────────
    //
    // Every method below has a default body returning `coordination_unsupported` (an
    // `Err(RemoteError::RemoteRejected)` with a synthetic 501). The trait otherwise has no
    // defaults, so adding these 14 methods without them would break every existing implementor —
    // `RemoteClient` plus the test-double mocks in `mempalace-mcp::federation` and
    // `mempalace-mcp::lib`. Defaults also let a Stage-4-aware client talk cleanly to a
    // pre-Stage-3 server, or to a test double with no reason to implement coordination.
    // `RemoteClient` overrides every one of them for real, gated on the `"coordination"`
    // capability from the cached `/v1/info` handshake — see its `ensure_coordination_capability`.

    /// Create a task (`POST /v1/coordination/tasks`).
    async fn coordination_task_create(&self, req: NewTaskRequest) -> Result<CoordinationTaskDto> {
        let _ = req;
        Err(coordination_unsupported("coordination_task_create"))
    }

    /// Get one task by exact ID (`GET /v1/coordination/tasks/{id}`).
    async fn coordination_task_get(&self, task_id: &str) -> Result<CoordinationTaskDto> {
        let _ = task_id;
        Err(coordination_unsupported("coordination_task_get"))
    }

    /// Claim a task, or reclaim an expired lease
    /// (`POST /v1/coordination/tasks/{id}/claim`).
    ///
    /// A stale `expected_revision` is reported by the remote as `409 revision_conflict`; that
    /// surfaces here as `Ok(RemoteRevisionedWrite::Conflict)` carrying the remote's actual
    /// revision, not as an `Err` — matching how `mempalace_storage::CoordinationStore` itself
    /// now reports the same conflict locally (Phase 3 Stage 4). Every other rejection (e.g. a
    /// live lease held by another worker, a terminal task — `409 coordination_conflict`) has no
    /// revision pair to report and stays an `Err`. MemPalace never retries a conflicting write
    /// on the caller's behalf; that decision belongs to the caller.
    async fn coordination_task_claim(
        &self,
        task_id: &str,
        req: TaskLeaseRequest,
    ) -> Result<RemoteRevisionedWrite<CoordinationTaskDto>> {
        let _ = (task_id, req);
        Err(coordination_unsupported("coordination_task_claim"))
    }

    /// Renew a live lease (`POST /v1/coordination/tasks/{id}/renew`). See
    /// [`Self::coordination_task_claim`] for the conflict-shape note.
    async fn coordination_task_renew(
        &self,
        task_id: &str,
        req: TaskLeaseRequest,
    ) -> Result<RemoteRevisionedWrite<CoordinationTaskDto>> {
        let _ = (task_id, req);
        Err(coordination_unsupported("coordination_task_renew"))
    }

    /// Transition a task's lifecycle state (`POST /v1/coordination/tasks/{id}/transition`). See
    /// [`Self::coordination_task_claim`] for the conflict-shape note.
    async fn coordination_task_transition(
        &self,
        task_id: &str,
        req: TransitionTaskRequest,
    ) -> Result<RemoteRevisionedWrite<CoordinationTaskDto>> {
        let _ = (task_id, req);
        Err(coordination_unsupported("coordination_task_transition"))
    }

    /// Send an addressed message (`POST /v1/coordination/messages`).
    async fn coordination_message_send(
        &self,
        req: NewMessageRequest,
    ) -> Result<CoordinationMessageDto> {
        let _ = req;
        Err(coordination_unsupported("coordination_message_send"))
    }

    /// Get one message by exact ID (`GET /v1/coordination/messages/{id}`).
    async fn coordination_message_get(&self, message_id: &str) -> Result<CoordinationMessageDto> {
        let _ = message_id;
        Err(coordination_unsupported("coordination_message_get"))
    }

    /// Acknowledge a message (`POST /v1/coordination/messages/{id}/ack`).
    async fn coordination_message_ack(
        &self,
        message_id: &str,
        req: AckMessageRequest,
    ) -> Result<CoordinationMessageDto> {
        let _ = (message_id, req);
        Err(coordination_unsupported("coordination_message_ack"))
    }

    /// Read an addressed inbox, cursor-paginated (`GET /v1/coordination/inbox`).
    async fn coordination_inbox(&self, query: InboxQuery) -> Result<InboxPageResponse> {
        let _ = query;
        Err(coordination_unsupported("coordination_inbox"))
    }

    /// Store an immutable artifact (`POST /v1/coordination/artifacts`).
    async fn coordination_artifact_put(
        &self,
        req: NewArtifactRequest,
    ) -> Result<CoordinationArtifactDto> {
        let _ = req;
        Err(coordination_unsupported("coordination_artifact_put"))
    }

    /// Get one artifact by exact ID (`GET /v1/coordination/artifacts/{id}`).
    async fn coordination_artifact_get(
        &self,
        artifact_id: &str,
    ) -> Result<CoordinationArtifactDto> {
        let _ = artifact_id;
        Err(coordination_unsupported("coordination_artifact_get"))
    }

    /// Store an immutable task result (`POST /v1/coordination/results`).
    async fn coordination_result_put(
        &self,
        req: NewTaskResultRequest,
    ) -> Result<CoordinationTaskResultDto> {
        let _ = req;
        Err(coordination_unsupported("coordination_result_put"))
    }

    /// Get one task result by exact ID (`GET /v1/coordination/results/{id}`).
    async fn coordination_result_get(&self, result_id: &str) -> Result<CoordinationTaskResultDto> {
        let _ = result_id;
        Err(coordination_unsupported("coordination_result_get"))
    }

    /// Read the coordination audit-event feed, cursor-paginated
    /// (`GET /v1/coordination/events`).
    async fn coordination_events(
        &self,
        query: CoordinationEventsQuery,
    ) -> Result<CoordinationEventsResponse> {
        let _ = query;
        Err(coordination_unsupported("coordination_events"))
    }
}

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
    AddDrawerRequest, AddDrawerResponse, ChangesQuery, ChangesResponse, CheckDuplicateRequest,
    CheckDuplicateResponse, DrawerSearchRequest, DrawerSearchResponse, InfoResponse,
    IngestBatchRequest, IngestBatchResponse, KgAddFactRequest, KgInvalidateRequest, KgQueryRequest,
    ListDrawersQuery, ListDrawersResponse,
};

/// Default per-request timeout for remote calls.
pub const DEFAULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

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
}

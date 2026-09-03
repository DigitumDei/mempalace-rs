//! [`RemoteClient`] — concrete reqwest-backed implementation of [`RemoteApi`].

use mempalace_federation::{
    AckMessageRequest, AddDrawerRequest, AddDrawerResponse, ChangesQuery, ChangesResponse,
    CheckDuplicateRequest, CheckDuplicateResponse, CoordinationArtifactDto,
    CoordinationEventsQuery, CoordinationEventsResponse, CoordinationMessageDto,
    CoordinationTaskDto, CoordinationTaskResultDto, DrawerSearchRequest, DrawerSearchResponse,
    ErrorBody, FEDERATION_API_VERSION, InboxPageResponse, InboxQuery, InfoResponse,
    IngestBatchRequest, IngestBatchResponse, KgAddFactRequest, KgInvalidateRequest, KgQueryRequest,
    ListDrawersQuery, ListDrawersResponse, NewArtifactRequest, NewMessageRequest, NewTaskRequest,
    NewTaskResultRequest, TaskLeaseRequest, TransitionTaskRequest,
};

use crate::{
    RemoteApi, RemoteEndpoint, RemoteRevisionedWrite,
    error::{RemoteError, Result},
};

/// Capability string a remote must advertise on `GET /v1/info` before this client will attempt
/// any `/v1/coordination/*` route (issue #102 Stage 3/4).
const COORDINATION_CAPABILITY: &str = "coordination";

/// Maximum body length (in bytes) included verbatim in [`RemoteError::RemoteRejected`].
///
/// Bodies larger than this are truncated to avoid flooding logs.
const MAX_ERROR_BODY: usize = 2048;

/// Maximum response body (in bytes) accepted from a remote peer on success.
///
/// Responses larger than this are rejected as [`RemoteError::InvalidResponse`]
/// to prevent memory exhaustion (peer OOM).
const MAX_RESPONSE_BYTES: usize = 10 * 1024 * 1024;

/// Whether a call mutates remote state or only reads it.
///
/// Transport failures are classified differently per call kind (issue #127,
/// slice 2): reads keep the historical degradable [`RemoteError::Unreachable`]
/// behaviour regardless of why the transport failed, whereas a mutation that
/// may have reached and committed on the server surfaces as
/// [`RemoteError::UnknownOutcome`] — never as an authoritative failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CallKind {
    /// A call that only reads remote state.
    Read,
    /// A call that mutates remote state (add/delete/ingest/KG write or any
    /// coordination write).
    Mutation,
}

/// A reqwest-backed HTTP client for one remote MemPalace federation endpoint.
///
/// Build with [`RemoteClient::new`]; then use via the [`RemoteApi`] trait.
#[derive(Debug)]
pub struct RemoteClient {
    /// Display name used in error messages and log output.
    name: String,
    /// Base URL, normalised to end with `'/'` so [`reqwest::Url::join`] works correctly.
    base_url: reqwest::Url,
    /// Optional bearer token sent on every authenticated request.
    token: Option<String>,
    /// Shared reqwest HTTP client (connection-pool aware).
    http: reqwest::Client,
    /// Cached result of the initial `GET /v1/info` handshake.
    ///
    /// [`tokio::sync::OnceCell`] is used so the handshake is attempted at most
    /// once per successful call; transient failures leave the cell empty so the
    /// next call retries.
    info: tokio::sync::OnceCell<InfoResponse>,
}

impl RemoteClient {
    /// Construct a new client from a [`RemoteEndpoint`] descriptor.
    ///
    /// Returns [`RemoteError::InvalidConfig`] when the URL is unparseable or the
    /// underlying [`reqwest::Client`] cannot be built.
    ///
    /// The client is inert until the first method call, which performs a
    /// version-handshake via `GET /v1/info`.
    pub fn new(endpoint: RemoteEndpoint) -> Result<Self> {
        let raw_url = endpoint.base_url.clone();

        let mut base_url =
            reqwest::Url::parse(&raw_url).map_err(|e| RemoteError::InvalidConfig {
                remote: endpoint.name.clone(),
                message: format!("cannot parse base URL `{raw_url}`: {e}"),
            })?;

        // Normalize: ensure the path ends with '/' so that Url::join with a
        // relative path (e.g. "v1/info") appends rather than replaces the last
        // path segment. This matters when the server sits behind a reverse proxy
        // at a sub-path such as `/palace/`.
        if !base_url.path().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }

        let http = reqwest::Client::builder()
            .use_rustls_tls()
            .timeout(endpoint.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| RemoteError::InvalidConfig {
                remote: endpoint.name.clone(),
                message: format!("failed to build HTTP client: {e}"),
            })?;

        Ok(Self {
            name: endpoint.name,
            base_url,
            token: endpoint.token,
            http,
            info: tokio::sync::OnceCell::new(),
        })
    }

    /// Build a URL by joining a relative path segment to [`Self::base_url`].
    ///
    /// `path` must NOT start with `'/'` — a leading slash would clobber the
    /// base path on reverse-proxy deployments.
    fn url(&self, path: &str) -> Result<reqwest::Url> {
        self.base_url.join(path).map_err(|e| RemoteError::InvalidConfig {
            remote: self.name.clone(),
            message: format!("cannot construct URL for path `{path}`: {e}"),
        })
    }

    /// Perform the `GET /v1/info` handshake and return the response.
    ///
    /// This method does **not** call [`Self::ensure_handshake`]; it is the
    /// handshake itself.
    async fn fetch_info(&self) -> Result<InfoResponse> {
        let url = self.url("v1/info")?;
        let rb = self.http.get(url);
        // The handshake is a read. A failed handshake therefore degrades to
        // `Unreachable` for the mutations gated behind it — "before send", never
        // `UnknownOutcome`.
        self.execute(rb, CallKind::Read).await
    }

    /// Ensure the version handshake has been performed, returning a reference
    /// to the cached [`InfoResponse`].
    ///
    /// On success the cell is populated and subsequent calls are free.
    /// On transient failure the cell is left empty, allowing a retry next call.
    /// On version mismatch, [`RemoteError::VersionSkew`] is returned on every call.
    async fn ensure_handshake(&self) -> Result<&InfoResponse> {
        let info = self.info.get_or_try_init(|| self.fetch_info()).await?;
        if info.federation_api_version != FEDERATION_API_VERSION {
            return Err(RemoteError::VersionSkew {
                remote: self.name.clone(),
                ours: FEDERATION_API_VERSION,
                theirs: info.federation_api_version,
            });
        }
        Ok(info)
    }

    /// Send a prepared [`reqwest::RequestBuilder`] with auth injected, and read the full
    /// response body with a size cap (tighter for error responses than for success, since the
    /// server controls the success schema but an error body from a misbehaving proxy could be
    /// arbitrarily large). Shared by [`Self::execute`] and [`Self::execute_revisioned`] so the
    /// two only diverge in how they classify a non-2xx status, not in how bytes are read.
    ///
    /// `kind` steers transport-failure classification: reads degrade to
    /// [`RemoteError::Unreachable`], while a mutation that may have been
    /// delivered surfaces as [`RemoteError::UnknownOutcome`].
    async fn send_and_read(
        &self,
        rb: reqwest::RequestBuilder,
        kind: CallKind,
    ) -> Result<(reqwest::StatusCode, Vec<u8>)> {
        let rb = match &self.token {
            Some(tok) => rb.bearer_auth(tok),
            None => rb,
        };

        let response = rb.send().await.map_err(|e| self.classify_send_error(kind, e))?;

        let status = response.status();

        let mut bytes = Vec::new();
        let mut response = response;
        while let Some(chunk) =
            response.chunk().await.map_err(|e| self.classify_body_error(kind, status, e))?
        {
            bytes.extend_from_slice(&chunk);
            let cap = if status.is_success() { MAX_RESPONSE_BYTES } else { MAX_ERROR_BODY };
            if bytes.len() >= cap {
                break;
            }
        }
        Ok((status, bytes))
    }

    /// Map a [`reqwest::Error`] from [`reqwest::RequestBuilder::send`] into a
    /// [`RemoteError`].
    ///
    /// For reads any transport failure stays degradable
    /// [`RemoteError::Unreachable`]. For mutations, only errors that prove the
    /// request was never delivered are [`RemoteError::Unreachable`]:
    /// [`reqwest::Error::is_builder`] (the request could not even be built) and
    /// [`reqwest::Error::is_connect`] (DNS/connect refused — nothing reached the
    /// application).
    ///
    /// Everything else is ambiguous. In particular [`reqwest::Error::is_timeout`]
    /// does **not** mean "before send": reqwest wraps a request timeout as
    /// `Kind::Request` via `error::request`, so `is_request()` is true for a
    /// response that never arrived — the mutation may have committed on the
    /// server — and must surface as [`RemoteError::UnknownOutcome`], never as
    /// an authoritative failure. A dead handshake runs as a read, so a mutation
    /// gated behind a dead handshake surfaces as [`RemoteError::Unreachable`]
    /// (before send), never as `UnknownOutcome`.
    fn classify_send_error(&self, kind: CallKind, e: reqwest::Error) -> RemoteError {
        let remote = self.name.clone();
        let message = e.to_string();
        match kind {
            CallKind::Read => RemoteError::Unreachable { remote, message },
            CallKind::Mutation => {
                if e.is_builder() || e.is_connect() {
                    RemoteError::Unreachable { remote, message }
                } else {
                    RemoteError::UnknownOutcome { remote, message }
                }
            }
        }
    }

    /// Map a [`reqwest::Error`] from reading a response body
    /// ([`reqwest::Response::chunk`]) into a [`RemoteError`].
    ///
    /// A body-read failure happens only after the status line was received. A
    /// successful mutation may already have committed and is therefore
    /// [`RemoteError::UnknownOutcome`]. A non-success status is authoritative
    /// even when its explanatory body is truncated, so it retains the status
    /// classification instead of being mislabeled as an ambiguous commit.
    /// Reads keep their historical degradable behavior.
    fn classify_body_error(
        &self,
        kind: CallKind,
        status: reqwest::StatusCode,
        e: reqwest::Error,
    ) -> RemoteError {
        let remote = self.name.clone();
        let message = e.to_string();
        match kind {
            CallKind::Read => RemoteError::Unreachable { remote, message },
            CallKind::Mutation if status == reqwest::StatusCode::UNAUTHORIZED => {
                RemoteError::Unauthorized { remote }
            }
            CallKind::Mutation if !status.is_success() => RemoteError::RemoteRejected {
                remote,
                status: status.as_u16(),
                body: format!("response body read failed: {message}"),
            },
            CallKind::Mutation => RemoteError::UnknownOutcome { remote, message },
        }
    }

    /// Map a `serde_json` failure decoding a 2xx response body into a
    /// [`RemoteError`].
    ///
    /// The status line was received, so the server processed the request; for a
    /// mutation we cannot hand back the resulting payload, and must not claim an
    /// authoritative failure, so it surfaces as [`RemoteError::UnknownOutcome`].
    /// Reads surface as [`RemoteError::InvalidResponse`] as before.
    fn decode_failure(&self, kind: CallKind, e: serde_json::Error) -> RemoteError {
        match kind {
            CallKind::Read => {
                RemoteError::InvalidResponse { remote: self.name.clone(), message: e.to_string() }
            }
            CallKind::Mutation => {
                RemoteError::UnknownOutcome { remote: self.name.clone(), message: e.to_string() }
            }
        }
    }

    /// Classifies a non-2xx, non-401 response into [`RemoteError::RemoteRejected`], decoding an
    /// [`ErrorBody`] when present so the message is `"{code}: {message}"` rather than raw JSON.
    fn remote_rejected(&self, status: reqwest::StatusCode, bytes: &[u8]) -> RemoteError {
        let raw = String::from_utf8_lossy(bytes).into_owned();
        let body = if let Ok(err_body) = serde_json::from_str::<ErrorBody>(&raw) {
            format!("{}: {}", err_body.code, err_body.message)
        } else if raw.len() > MAX_ERROR_BODY {
            let mut cut = MAX_ERROR_BODY;
            while !raw.is_char_boundary(cut) {
                cut -= 1;
            }
            format!("{}… (truncated)", &raw[..cut])
        } else {
            raw
        };
        RemoteError::RemoteRejected { remote: self.name.clone(), status: status.as_u16(), body }
    }

    /// Send a prepared [`reqwest::RequestBuilder`], inject auth, and decode the
    /// response body as `T`.
    ///
    /// Error classification (`kind` distinguishes reads from mutations; see
    /// [`CallKind`]):
    /// - Transport failure before the request was delivered → [`RemoteError::Unreachable`]
    /// - Mutation transport/decoding failure where the write may have committed
    ///   → [`RemoteError::UnknownOutcome`]
    /// - HTTP 401 → [`RemoteError::Unauthorized`]
    /// - Other non-2xx → [`RemoteError::RemoteRejected`] (body included)
    /// - 2xx with bad JSON → [`RemoteError::InvalidResponse`] (reads) or
    ///   [`RemoteError::UnknownOutcome`] (mutations)
    async fn execute<T: serde::de::DeserializeOwned>(
        &self,
        rb: reqwest::RequestBuilder,
        kind: CallKind,
    ) -> Result<T> {
        let (status, bytes) = self.send_and_read(rb, kind).await?;

        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(RemoteError::Unauthorized { remote: self.name.clone() });
        }

        if !status.is_success() {
            return Err(self.remote_rejected(status, &bytes));
        }

        serde_json::from_slice(&bytes).map_err(|e| self.decode_failure(kind, e))
    }

    /// Like [`Self::execute`], but a `409` body whose `code` is `"revision_conflict"` decodes
    /// into `Ok(RemoteRevisionedWrite::Conflict)` carrying the remote's `actual_revision`,
    /// instead of an `Err` — the wire counterpart of how
    /// `mempalace_storage::CoordinationStore::claim_task`/`renew_lease`/`transition_task` report
    /// the same conflict locally (Phase 3 Stage 4). A `409 coordination_conflict` (no revision
    /// pair — a live lease held by someone else, a terminal task, an invalid transition) has
    /// nothing this shape can carry and falls through to the ordinary `RemoteRejected`
    /// classification, same as any other non-2xx status.
    ///
    /// Every coordination write this runs is a mutation, so transport failures
    /// that may have reached the server surface as
    /// [`RemoteError::UnknownOutcome`] rather than an authoritative failure.
    async fn execute_revisioned<T: serde::de::DeserializeOwned>(
        &self,
        rb: reqwest::RequestBuilder,
    ) -> Result<RemoteRevisionedWrite<T>> {
        let (status, bytes) = self.send_and_read(rb, CallKind::Mutation).await?;

        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(RemoteError::Unauthorized { remote: self.name.clone() });
        }

        if status == reqwest::StatusCode::CONFLICT {
            #[derive(serde::Deserialize)]
            struct ConflictBody {
                code: String,
                #[serde(default)]
                actual_revision: Option<i64>,
            }
            if let Ok(conflict) = serde_json::from_slice::<ConflictBody>(&bytes)
                && conflict.code == "revision_conflict"
            {
                return Ok(RemoteRevisionedWrite::Conflict {
                    actual_revision: conflict.actual_revision,
                });
            }
        }

        if !status.is_success() {
            return Err(self.remote_rejected(status, &bytes));
        }

        serde_json::from_slice(&bytes)
            .map(RemoteRevisionedWrite::Applied)
            .map_err(|e| self.decode_failure(CallKind::Mutation, e))
    }

    /// Ensures the cached `/v1/info` handshake has run and the remote advertises
    /// `"coordination"` (issue #102 Stage 3). Every coordination method calls this before
    /// sending its request — cheap, since [`Self::ensure_handshake`] caches the result in a
    /// `OnceCell` that every other method already populates on first use, so this costs nothing
    /// beyond the handshake every call already pays for.
    async fn ensure_coordination_capability(&self) -> Result<()> {
        let info = self.ensure_handshake().await?;
        if info.capabilities.iter().any(|c| c == COORDINATION_CAPABILITY) {
            Ok(())
        } else {
            Err(RemoteError::CapabilityMissing {
                remote: self.name.clone(),
                capability: COORDINATION_CAPABILITY.to_owned(),
            })
        }
    }
}

#[async_trait::async_trait]
impl RemoteApi for RemoteClient {
    /// Return the cached server info, performing the handshake if needed.
    async fn info(&self) -> Result<InfoResponse> {
        Ok(self.ensure_handshake().await?.clone())
    }

    /// Return whether the remote handshake advertised receipt-backed mutation idempotency.
    async fn idempotent_mutations_capability(&self) -> Result<Option<bool>> {
        let info = self.ensure_handshake().await?;
        Ok(Some(info.capabilities.iter().any(|capability| capability == "idempotent_mutations")))
    }

    /// Search drawers using semantic full-text matching (`POST /v1/drawers/search`).
    async fn search_drawers(&self, req: DrawerSearchRequest) -> Result<DrawerSearchResponse> {
        self.ensure_handshake().await?;
        let url = self.url("v1/drawers/search")?;
        let rb = self.http.post(url).json(&req);
        self.execute(rb, CallKind::Read).await
    }

    /// Check whether content is a near-duplicate of an existing drawer (`POST /v1/drawers/check_duplicate`).
    async fn check_duplicate(&self, req: CheckDuplicateRequest) -> Result<CheckDuplicateResponse> {
        self.ensure_handshake().await?;
        let url = self.url("v1/drawers/check_duplicate")?;
        let rb = self.http.post(url).json(&req);
        self.execute(rb, CallKind::Read).await
    }

    /// Add a new drawer to the remote palace (`POST /v1/drawers`).
    async fn add_drawer(&self, req: AddDrawerRequest) -> Result<AddDrawerResponse> {
        self.ensure_handshake().await?;
        let url = self.url("v1/drawers")?;
        let rb = self.http.post(url).json(&req);
        self.execute(rb, CallKind::Mutation).await
    }

    /// List drawers with optional filtering and pagination (`GET /v1/drawers`).
    ///
    /// `query` is serialized as URL query parameters via `serde_urlencoded`;
    /// `None` fields are omitted automatically.
    async fn list_drawers(&self, query: ListDrawersQuery) -> Result<ListDrawersResponse> {
        self.ensure_handshake().await?;
        let url = self.url("v1/drawers")?;
        let rb = self.http.get(url).query(&query);
        self.execute(rb, CallKind::Read).await
    }

    /// Retrieve a single drawer by its stable identifier (`GET /v1/drawers/{id}`).
    async fn get_drawer(&self, drawer_id: &str) -> Result<serde_json::Value> {
        self.ensure_handshake().await?;
        let path = format!("v1/drawers/{drawer_id}");
        let url = self.url(&path)?;
        let rb = self.http.get(url);
        self.execute(rb, CallKind::Read).await
    }

    /// Delete a drawer by its stable identifier (`DELETE /v1/drawers/{id}`).
    async fn delete_drawer(&self, drawer_id: &str) -> Result<()> {
        self.delete_drawer_with_operation_id(drawer_id, None).await
    }

    /// Delete a drawer by its stable identifier, carrying an optional operation
    /// id as a query parameter (`DELETE /v1/drawers/{id}?operation_id=`).
    ///
    /// The operation id lets a durable replication outbox retry a delete
    /// safely: the receiving endpoint can dedupe a replayed mutation, and
    /// `DeleteDrawerQuery` keeps old callers (that omit it) wire-compatible.
    async fn delete_drawer_with_operation_id(
        &self,
        drawer_id: &str,
        operation_id: Option<&str>,
    ) -> Result<()> {
        self.ensure_handshake().await?;
        let path = format!("v1/drawers/{drawer_id}");
        let url = self.url(&path)?;
        let rb = self.http.delete(url);
        let rb = match operation_id {
            Some(op) => rb.query(&[("operation_id", op)]),
            None => rb,
        };
        self.execute::<serde_json::Value>(rb, CallKind::Mutation).await.map(|_| ())
    }

    /// Query the knowledge graph for an entity (`POST /v1/kg/query`).
    async fn kg_query(&self, req: KgQueryRequest) -> Result<serde_json::Value> {
        self.ensure_handshake().await?;
        let url = self.url("v1/kg/query")?;
        let rb = self.http.post(url).json(&req);
        self.execute(rb, CallKind::Read).await
    }

    /// Add a fact to the knowledge graph (`POST /v1/kg/facts`).
    async fn kg_add_fact(&self, req: KgAddFactRequest) -> Result<serde_json::Value> {
        self.ensure_handshake().await?;
        let url = self.url("v1/kg/facts")?;
        let rb = self.http.post(url).json(&req);
        self.execute(rb, CallKind::Mutation).await
    }

    /// Invalidate a knowledge-graph fact (`POST /v1/kg/facts/invalidate`).
    async fn kg_invalidate(&self, req: KgInvalidateRequest) -> Result<serde_json::Value> {
        self.ensure_handshake().await?;
        let url = self.url("v1/kg/facts/invalidate")?;
        let rb = self.http.post(url).json(&req);
        self.execute(rb, CallKind::Mutation).await
    }

    /// Retrieve the knowledge-graph timeline, optionally filtered by entity (`GET /v1/kg/timeline`).
    async fn kg_timeline(&self, entity: Option<&str>) -> Result<serde_json::Value> {
        self.ensure_handshake().await?;
        let url = self.url("v1/kg/timeline")?;
        let rb = match entity {
            Some(e) => self.http.get(url).query(&[("entity", e)]),
            None => self.http.get(url),
        };
        self.execute(rb, CallKind::Read).await
    }

    /// Retrieve knowledge-graph statistics (`GET /v1/kg/stats`).
    async fn kg_stats(&self) -> Result<serde_json::Value> {
        self.ensure_handshake().await?;
        let url = self.url("v1/kg/stats")?;
        let rb = self.http.get(url);
        self.execute(rb, CallKind::Read).await
    }

    /// Retrieve the palace taxonomy (`GET /v1/taxonomy`).
    async fn taxonomy(&self) -> Result<serde_json::Value> {
        self.ensure_handshake().await?;
        let url = self.url("v1/taxonomy")?;
        let rb = self.http.get(url);
        self.execute(rb, CallKind::Read).await
    }

    /// List the wings in the remote palace (`GET /v1/wings`).
    async fn wings(&self) -> Result<serde_json::Value> {
        self.ensure_handshake().await?;
        let url = self.url("v1/wings")?;
        let rb = self.http.get(url);
        self.execute(rb, CallKind::Read).await
    }

    /// List rooms, optionally filtered by wing (`GET /v1/rooms`).
    async fn rooms(&self, wing: Option<&str>) -> Result<serde_json::Value> {
        self.ensure_handshake().await?;
        let url = self.url("v1/rooms")?;
        let rb = match wing {
            Some(w) => self.http.get(url).query(&[("wing", w)]),
            None => self.http.get(url),
        };
        self.execute(rb, CallKind::Read).await
    }

    /// Retrieve paginated change events (`GET /v1/changes`).
    ///
    /// `query` is serialized as URL query parameters via `serde_urlencoded`;
    /// `None` fields are omitted automatically.
    async fn changes(&self, query: ChangesQuery) -> Result<ChangesResponse> {
        self.ensure_handshake().await?;
        let url = self.url("v1/changes")?;
        let rb = self.http.get(url).query(&query);
        self.execute(rb, CallKind::Read).await
    }

    /// Bulk-ingest pre-chunked file content into the remote palace
    /// (`POST /v1/ingest/batch`).
    ///
    /// A 413 response (body too large) surfaces as
    /// [`RemoteError::RemoteRejected`] with `status: 413`; no client-side
    /// splitting is attempted.
    async fn ingest_batch(&self, req: IngestBatchRequest) -> Result<IngestBatchResponse> {
        self.ensure_handshake().await?;
        let url = self.url("v1/ingest/batch")?;
        let rb = self.http.post(url).json(&req);
        self.execute(rb, CallKind::Mutation).await
    }

    /// Create a task (`POST /v1/coordination/tasks`).
    async fn coordination_task_create(&self, req: NewTaskRequest) -> Result<CoordinationTaskDto> {
        self.ensure_coordination_capability().await?;
        let url = self.url("v1/coordination/tasks")?;
        let rb = self.http.post(url).json(&req);
        self.execute(rb, CallKind::Mutation).await
    }

    /// Get one task by exact ID (`GET /v1/coordination/tasks/{id}`).
    async fn coordination_task_get(&self, task_id: &str) -> Result<CoordinationTaskDto> {
        self.ensure_coordination_capability().await?;
        let path = format!("v1/coordination/tasks/{task_id}");
        let url = self.url(&path)?;
        let rb = self.http.get(url);
        self.execute(rb, CallKind::Read).await
    }

    /// Claim a task, or reclaim an expired lease (`POST /v1/coordination/tasks/{id}/claim`).
    async fn coordination_task_claim(
        &self,
        task_id: &str,
        req: TaskLeaseRequest,
    ) -> Result<RemoteRevisionedWrite<CoordinationTaskDto>> {
        self.ensure_coordination_capability().await?;
        let path = format!("v1/coordination/tasks/{task_id}/claim");
        let url = self.url(&path)?;
        let rb = self.http.post(url).json(&req);
        self.execute_revisioned(rb).await
    }

    /// Renew a live lease (`POST /v1/coordination/tasks/{id}/renew`).
    async fn coordination_task_renew(
        &self,
        task_id: &str,
        req: TaskLeaseRequest,
    ) -> Result<RemoteRevisionedWrite<CoordinationTaskDto>> {
        self.ensure_coordination_capability().await?;
        let path = format!("v1/coordination/tasks/{task_id}/renew");
        let url = self.url(&path)?;
        let rb = self.http.post(url).json(&req);
        self.execute_revisioned(rb).await
    }

    /// Transition a task's lifecycle state (`POST /v1/coordination/tasks/{id}/transition`).
    async fn coordination_task_transition(
        &self,
        task_id: &str,
        req: TransitionTaskRequest,
    ) -> Result<RemoteRevisionedWrite<CoordinationTaskDto>> {
        self.ensure_coordination_capability().await?;
        let path = format!("v1/coordination/tasks/{task_id}/transition");
        let url = self.url(&path)?;
        let rb = self.http.post(url).json(&req);
        self.execute_revisioned(rb).await
    }

    /// Send an addressed message (`POST /v1/coordination/messages`).
    async fn coordination_message_send(
        &self,
        req: NewMessageRequest,
    ) -> Result<CoordinationMessageDto> {
        self.ensure_coordination_capability().await?;
        let url = self.url("v1/coordination/messages")?;
        let rb = self.http.post(url).json(&req);
        self.execute(rb, CallKind::Mutation).await
    }

    /// Get one message by exact ID (`GET /v1/coordination/messages/{id}`).
    async fn coordination_message_get(&self, message_id: &str) -> Result<CoordinationMessageDto> {
        self.ensure_coordination_capability().await?;
        let path = format!("v1/coordination/messages/{message_id}");
        let url = self.url(&path)?;
        let rb = self.http.get(url);
        self.execute(rb, CallKind::Read).await
    }

    /// Acknowledge a message (`POST /v1/coordination/messages/{id}/ack`).
    async fn coordination_message_ack(
        &self,
        message_id: &str,
        req: AckMessageRequest,
    ) -> Result<CoordinationMessageDto> {
        self.ensure_coordination_capability().await?;
        let path = format!("v1/coordination/messages/{message_id}/ack");
        let url = self.url(&path)?;
        let rb = self.http.post(url).json(&req);
        self.execute(rb, CallKind::Mutation).await
    }

    /// Read an addressed inbox, cursor-paginated (`GET /v1/coordination/inbox`).
    async fn coordination_inbox(&self, query: InboxQuery) -> Result<InboxPageResponse> {
        self.ensure_coordination_capability().await?;
        let url = self.url("v1/coordination/inbox")?;
        let rb = self.http.get(url).query(&query);
        self.execute(rb, CallKind::Read).await
    }

    /// Store an immutable artifact (`POST /v1/coordination/artifacts`).
    async fn coordination_artifact_put(
        &self,
        req: NewArtifactRequest,
    ) -> Result<CoordinationArtifactDto> {
        self.ensure_coordination_capability().await?;
        let url = self.url("v1/coordination/artifacts")?;
        let rb = self.http.post(url).json(&req);
        self.execute(rb, CallKind::Mutation).await
    }

    /// Get one artifact by exact ID (`GET /v1/coordination/artifacts/{id}`).
    async fn coordination_artifact_get(
        &self,
        artifact_id: &str,
    ) -> Result<CoordinationArtifactDto> {
        self.ensure_coordination_capability().await?;
        let path = format!("v1/coordination/artifacts/{artifact_id}");
        let url = self.url(&path)?;
        let rb = self.http.get(url);
        self.execute(rb, CallKind::Read).await
    }

    /// Store an immutable task result (`POST /v1/coordination/results`).
    async fn coordination_result_put(
        &self,
        req: NewTaskResultRequest,
    ) -> Result<CoordinationTaskResultDto> {
        self.ensure_coordination_capability().await?;
        let url = self.url("v1/coordination/results")?;
        let rb = self.http.post(url).json(&req);
        self.execute(rb, CallKind::Mutation).await
    }

    /// Get one task result by exact ID (`GET /v1/coordination/results/{id}`).
    async fn coordination_result_get(&self, result_id: &str) -> Result<CoordinationTaskResultDto> {
        self.ensure_coordination_capability().await?;
        let path = format!("v1/coordination/results/{result_id}");
        let url = self.url(&path)?;
        let rb = self.http.get(url);
        self.execute(rb, CallKind::Read).await
    }

    /// Read the coordination audit-event feed, cursor-paginated (`GET /v1/coordination/events`).
    async fn coordination_events(
        &self,
        query: CoordinationEventsQuery,
    ) -> Result<CoordinationEventsResponse> {
        self.ensure_coordination_capability().await?;
        let url = self.url("v1/coordination/events")?;
        let rb = self.http.get(url).query(&query);
        self.execute(rb, CallKind::Read).await
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::DEFAULT_TIMEOUT;
    use crate::error::is_transient_http_status;

    fn endpoint(base_url: &str) -> RemoteEndpoint {
        RemoteEndpoint {
            name: "test-remote".to_owned(),
            base_url: base_url.to_owned(),
            token: None,
            timeout: DEFAULT_TIMEOUT,
        }
    }

    /// Helper: build a client and return its normalised base URL string.
    fn base_url_str(raw: &str) -> String {
        let client = RemoteClient::new(endpoint(raw)).unwrap();
        client.base_url.to_string()
    }

    #[test]
    fn url_normalization_no_trailing_slash() {
        // Base without trailing slash — join must not clobber the host.
        let base = base_url_str("https://x.example");
        let url = reqwest::Url::parse(&base).unwrap().join("v1/info").unwrap();
        assert_eq!(url.as_str(), "https://x.example/v1/info");
    }

    #[test]
    fn url_normalization_with_trailing_slash() {
        // Base with trailing slash — same result.
        let base = base_url_str("https://x.example/");
        let url = reqwest::Url::parse(&base).unwrap().join("v1/info").unwrap();
        assert_eq!(url.as_str(), "https://x.example/v1/info");
    }

    #[test]
    fn url_normalization_sub_path() {
        // Reverse-proxy sub-path case — the sub-path must be preserved.
        let base = base_url_str("https://x.example/palace");
        let url = reqwest::Url::parse(&base).unwrap().join("v1/info").unwrap();
        assert_eq!(url.as_str(), "https://x.example/palace/v1/info");
    }

    #[test]
    fn invalid_url_returns_invalid_config() {
        let result = RemoteClient::new(endpoint("not a url"));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), RemoteError::InvalidConfig { .. }));
    }

    #[test]
    fn is_degradable_unreachable_only() {
        let mk_name = || "r".to_owned();

        assert!(
            RemoteError::Unreachable { remote: mk_name(), message: "x".to_owned() }.is_degradable()
        );
        assert!(!RemoteError::Unauthorized { remote: mk_name() }.is_degradable());
        assert!(
            !RemoteError::VersionSkew { remote: mk_name(), ours: 1, theirs: 2 }.is_degradable()
        );
        assert!(
            !RemoteError::RemoteRejected { remote: mk_name(), status: 404, body: String::new() }
                .is_degradable()
        );
        assert!(
            !RemoteError::InvalidResponse { remote: mk_name(), message: "x".to_owned() }
                .is_degradable()
        );
        assert!(
            !RemoteError::InvalidConfig { remote: mk_name(), message: "x".to_owned() }
                .is_degradable()
        );
        assert!(
            !RemoteError::CapabilityMissing { remote: mk_name(), capability: "x".to_owned() }
                .is_degradable()
        );
        assert!(
            !RemoteError::UnknownOutcome { remote: mk_name(), message: "x".to_owned() }
                .is_degradable()
        );
    }

    #[test]
    fn outbox_classification_helpers() {
        let mk_name = || "r".to_owned();

        // Unreachable = definitely-not-sent, degradable, retryable-without-key.
        let unreachable = RemoteError::Unreachable { remote: mk_name(), message: "x".to_owned() };
        assert!(unreachable.is_unreachable_before_send());
        assert!(!unreachable.is_unknown_outcome());
        assert!(unreachable.is_retryable());
        assert!(!unreachable.is_terminal());

        // UnknownOutcome = only retryable-with-operation-id; never authoritative.
        let unknown = RemoteError::UnknownOutcome { remote: mk_name(), message: "x".to_owned() };
        assert!(!unknown.is_unreachable_before_send());
        assert!(unknown.is_unknown_outcome());
        assert!(unknown.is_retryable());
        assert!(!unknown.is_terminal());

        // Transient rejections (408/425/429/5xx) are retryable/non-terminal.
        for status in [408, 425, 429, 500, 502, 503, 599] {
            assert!(is_transient_http_status(status), "status {status} must be transient");
            let err =
                RemoteError::RemoteRejected { remote: mk_name(), status, body: String::new() };
            assert!(err.is_retryable(), "status {status} must be retryable");
            assert!(!err.is_terminal(), "status {status} must not be terminal");
        }
        // Ordinary 4xx are terminal/non-retryable.
        for status in [400, 403, 404, 409, 422] {
            assert!(!is_transient_http_status(status), "status {status} must not be transient");
            let err =
                RemoteError::RemoteRejected { remote: mk_name(), status, body: String::new() };
            assert!(!err.is_retryable(), "status {status} must not be retryable");
            assert!(err.is_terminal(), "status {status} must be terminal");
        }

        // Authoritative / config errors are terminal and never retryable.
        for err in [
            RemoteError::Unauthorized { remote: mk_name() },
            RemoteError::VersionSkew { remote: mk_name(), ours: 1, theirs: 2 },
            RemoteError::InvalidResponse { remote: mk_name(), message: "x".to_owned() },
            RemoteError::InvalidConfig { remote: mk_name(), message: "x".to_owned() },
            RemoteError::CapabilityMissing { remote: mk_name(), capability: "c".to_owned() },
        ] {
            assert!(!err.is_retryable(), "unexpected retryable: {err:?}");
            assert!(err.is_terminal(), "unexpected non-terminal: {err:?}");
        }
    }

    #[test]
    fn client_url_helper_builds_correct_paths() {
        let client = RemoteClient::new(endpoint("https://x.example/palace")).unwrap();

        let info_url = client.url("v1/info").unwrap();
        assert_eq!(info_url.as_str(), "https://x.example/palace/v1/info");

        let search_url = client.url("v1/drawers/search").unwrap();
        assert_eq!(search_url.as_str(), "https://x.example/palace/v1/drawers/search");
    }

    #[test]
    fn default_timeout_is_five_seconds() {
        assert_eq!(DEFAULT_TIMEOUT, Duration::from_secs(5));
    }

    // ─── Mutation-outcome classification (issue #127, slice 2) ─────────────────

    fn client_for_addr(addr: std::net::SocketAddr, timeout: Duration) -> RemoteClient {
        RemoteClient::new(RemoteEndpoint {
            name: "test-remote".to_owned(),
            base_url: format!("http://{addr}"),
            token: None,
            timeout,
        })
        .unwrap()
    }

    async fn spawn_stub(app: axum::Router) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        addr
    }

    /// A stub that answers `GET /v1/info` correctly so the client handshake
    /// succeeds, and mounts `federation_api_version`-compatible info plus a
    /// caller-supplied handler on `POST /v1/drawers`.
    fn drawer_stub(drawers_post: axum::routing::MethodRouter<()>) -> axum::Router {
        axum::Router::new()
            .route(
                "/v1/info",
                axum::routing::get(|| async {
                    axum::Json(serde_json::json!({
                        "server_version": "1.0.0-stub",
                        "federation_api_version": 1u32,
                        "embedding_profile": "balanced",
                        "capabilities": ["drawers", "kg"]
                    }))
                }),
            )
            .route("/v1/drawers", drawers_post)
    }

    fn add_request() -> AddDrawerRequest {
        AddDrawerRequest {
            wing: "w".to_owned(),
            room: "r".to_owned(),
            content: "c".to_owned(),
            source_file: None,
            added_by: None,
            drawer_id: None,
            operation_id: None,
        }
    }

    #[tokio::test]
    async fn mutation_send_timeout_is_unknown_outcome() {
        // The server receives the mutation (counter increments) and then hangs
        // longer than the client's per-request timeout: the write may well have
        // committed, but the client cannot confirm it. Must be `UnknownOutcome`,
        // never an authoritative failure.
        let hits = Arc::new(AtomicUsize::new(0));
        let server_hits = Arc::clone(&hits);
        let post = axum::routing::post(move || {
            let hits = Arc::clone(&server_hits);
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(30)).await;
                axum::Json(serde_json::json!({"success": true}))
            }
        });
        let addr = spawn_stub(drawer_stub(post)).await;
        let client = client_for_addr(addr, Duration::from_millis(300));

        let result = client.add_drawer(add_request()).await;
        match result {
            Err(err @ RemoteError::UnknownOutcome { .. }) => {
                assert!(err.is_unknown_outcome());
                assert!(!err.is_unreachable_before_send());
                assert!(err.is_retryable());
                assert!(!err.is_terminal());
            }
            other => panic!("expected UnknownOutcome on mutation timeout, got: {other:?}"),
        }
        assert!(
            hits.load(Ordering::SeqCst) >= 1,
            "the server must have received the mutation — that is the ambiguity UnknownOutcome exists to report"
        );
    }

    #[tokio::test]
    async fn mutation_connect_failure_is_unreachable_before_send() {
        // Nothing is listening: even the handshake cannot run, so the mutation
        // was definitely never sent — `Unreachable` (and degradable), never
        // `UnknownOutcome`. This also pins the rule that a dead handshake in
        // front of a mutation is "before send", not "unknown outcome".
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let client = client_for_addr(addr, Duration::from_secs(5));

        let result = client.add_drawer(add_request()).await;
        match result {
            Err(err @ RemoteError::Unreachable { .. }) => {
                assert!(err.is_unreachable_before_send());
                assert!(!err.is_unknown_outcome());
                assert!(err.is_degradable());
                assert!(err.is_retryable());
            }
            other => {
                panic!("expected Unreachable (before send) on connect failure, got: {other:?}")
            }
        }
    }

    #[tokio::test]
    async fn classify_connect_error_is_unreachable_for_mutations() {
        // Exercising `classify_send_error` directly against a real connect error
        // pins the mutation mapping (connect/DNS/build => definitely-not-sent ≡
        // `Unreachable`) without the handshake in the way.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let client = client_for_addr(addr, Duration::from_secs(5));

        let reqwest_err =
            client.http.get(format!("http://{addr}/v1/drawers")).send().await.unwrap_err();
        assert!(reqwest_err.is_connect(), "expected a connect error, got: {reqwest_err:?}");

        let classified = client.classify_send_error(CallKind::Mutation, reqwest_err);
        assert!(classified.is_unreachable_before_send(), "got: {classified:?}");
        assert!(!classified.is_unknown_outcome());
    }

    #[tokio::test]
    async fn mutation_authoritative_4xx_is_remote_rejected() {
        // The server responded 422: an authoritative rejection, terminal for an
        // outbox retry, and distinct from both `Unreachable` and `UnknownOutcome`.
        let post = axum::routing::post(|| async {
            (
                axum::http::StatusCode::UNPROCESSABLE_ENTITY,
                axum::Json(serde_json::json!({
                    "code": "invalid_params",
                    "message": "drawer rejected by stub"
                })),
            )
        });
        let addr = spawn_stub(drawer_stub(post)).await;
        let client = client_for_addr(addr, Duration::from_secs(5));

        let result = client.add_drawer(add_request()).await;
        match result {
            Err(err @ RemoteError::RemoteRejected { status: 422, .. }) => {
                assert!(err.is_terminal());
                assert!(!err.is_retryable());
                assert!(!err.is_degradable());
            }
            other => panic!("expected RemoteRejected(422), got: {other:?}"),
        }
    }

    /// A stub whose `POST /v1/drawers` returns a given status code with an
    /// [`mempalace_federation::ErrorBody`]-shaped body.
    fn error_status_stub(status: axum::http::StatusCode) -> axum::Router {
        drawer_stub(axum::routing::post(move || {
            let status = status;
            async move { (status, axum::Json(serde_json::json!({"code": "x", "message": "x"}))) }
        }))
    }

    #[tokio::test]
    async fn mutation_transient_429_is_retryable() {
        // 429 Too Many Requests: the remote is reachable and understood the
        // request but is overloaded. An outbox must retry (same operation id),
        // so this must be retryable/non-terminal — not a permanent failure.
        let addr = spawn_stub(error_status_stub(axum::http::StatusCode::TOO_MANY_REQUESTS)).await;
        let client = client_for_addr(addr, Duration::from_secs(5));

        let result = client.add_drawer(add_request()).await;
        match result {
            Err(err @ RemoteError::RemoteRejected { status: 429, .. }) => {
                assert!(err.is_retryable(), "429 must be retryable");
                assert!(!err.is_terminal(), "429 must not be terminal");
            }
            other => panic!("expected RemoteRejected(429), got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn mutation_transient_503_is_retryable() {
        // 503 Service Unavailable: transient server overload/failure, retryable.
        let addr = spawn_stub(error_status_stub(axum::http::StatusCode::SERVICE_UNAVAILABLE)).await;
        let client = client_for_addr(addr, Duration::from_secs(5));

        let result = client.add_drawer(add_request()).await;
        match result {
            Err(err @ RemoteError::RemoteRejected { status: 503, .. }) => {
                assert!(err.is_retryable(), "503 must be retryable");
                assert!(!err.is_terminal(), "503 must not be terminal");
            }
            other => panic!("expected RemoteRejected(503), got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn classify_timeout_error_is_unknown_outcome_even_when_is_request() {
        // Pins the review rule: reqwest wraps a request timeout as `Kind::Request`
        // (`error::request(TimedOut)`), so `is_request()` is true for a response
        // that never arrived. For a mutation that must NOT count as pre-send
        // (`Unreachable`) — the timeout is exactly the ambiguous case and must
        // be `UnknownOutcome`.
        let hits = Arc::new(AtomicUsize::new(0));
        let server_hits = Arc::clone(&hits);
        let post = axum::routing::post(move || {
            let hits = Arc::clone(&server_hits);
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(30)).await;
                axum::Json(serde_json::json!({"success": true}))
            }
        });
        let addr = spawn_stub(drawer_stub(post)).await;
        let client = client_for_addr(addr, Duration::from_millis(300));

        let reqwest_err =
            client.http.post(format!("http://{addr}/v1/drawers")).send().await.unwrap_err();
        assert!(reqwest_err.is_timeout(), "expected a timeout error, got: {reqwest_err:?}");

        // The point of the review: `is_request()` alone is NOT proof of pre-send.
        assert!(
            reqwest_err.is_request(),
            "reqwest wraps this request timeout as Kind::Request (is_request), which is why is_request cannot prove pre-send"
        );

        let classified = client.classify_send_error(CallKind::Mutation, reqwest_err);
        assert!(
            classified.is_unknown_outcome(),
            "timeout with is_request()==true must be UnknownOutcome, got: {classified:?}"
        );
        assert!(!classified.is_unreachable_before_send());
        assert!(
            hits.load(Ordering::SeqCst) >= 1,
            "the server must have received the mutation — the ambiguity is why this is UnknownOutcome"
        );
    }

    #[tokio::test]
    async fn mutation_authoritative_401_is_unauthorized_terminal() {
        // 401 must stay its own distinct variant (never folded into
        // RemoteRejected) and be terminal — an outbox must not blind-retry a
        // rejected credential.
        let post = axum::routing::post(|| async {
            (
                axum::http::StatusCode::UNAUTHORIZED,
                axum::Json(serde_json::json!({"code": "unauthorized", "message": "nope"})),
            )
        });
        let addr = spawn_stub(drawer_stub(post)).await;
        let client = client_for_addr(addr, Duration::from_secs(5));

        let result = client.add_drawer(add_request()).await;
        match result {
            Err(err @ RemoteError::Unauthorized { .. }) => {
                assert!(err.is_terminal());
                assert!(!err.is_retryable());
            }
            other => panic!("expected Unauthorized, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn mutation_undecodable_2xx_is_unknown_outcome() {
        // A 2xx whose body we cannot decode: the server accepted the mutation,
        // but we cannot return the resulting payload. Never claim an
        // authoritative failure — surface `UnknownOutcome`.
        let post = axum::routing::post(|| async { "this is not json" });
        let addr = spawn_stub(drawer_stub(post)).await;
        let client = client_for_addr(addr, Duration::from_secs(5));

        let result = client.add_drawer(add_request()).await;
        match result {
            Err(err @ RemoteError::UnknownOutcome { .. }) => {
                assert!(err.is_unknown_outcome());
                assert!(err.is_retryable());
            }
            other => panic!("expected UnknownOutcome on undecodable 2xx mutation, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn read_undecodable_2xx_stays_invalid_response() {
        // Reads keep the historical behaviour: an undecodable 2xx is
        // `InvalidResponse`, not `UnknownOutcome`.
        let search_post = axum::routing::post(|| async { "this is not json" });
        let app = axum::Router::new()
            .route(
                "/v1/info",
                axum::routing::get(|| async {
                    axum::Json(serde_json::json!({
                        "server_version": "1.0.0-stub",
                        "federation_api_version": 1u32,
                        "embedding_profile": "balanced",
                        "capabilities": ["drawers", "kg"]
                    }))
                }),
            )
            .route("/v1/drawers/search", search_post);
        let addr = spawn_stub(app).await;
        let client = client_for_addr(addr, Duration::from_secs(5));

        let result = client
            .search_drawers(DrawerSearchRequest {
                query: "q".to_owned(),
                wing: None,
                room: None,
                view: None,
                limit: None,
            })
            .await;
        match result {
            Err(err @ RemoteError::InvalidResponse { .. }) => {
                assert!(err.is_terminal());
                assert!(!err.is_retryable());
            }
            other => panic!("expected InvalidResponse for undecodable read, got: {other:?}"),
        }
    }
}

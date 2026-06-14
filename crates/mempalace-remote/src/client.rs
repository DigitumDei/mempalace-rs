//! [`RemoteClient`] — concrete reqwest-backed implementation of [`RemoteApi`].

use mempalace_federation::{
    AddDrawerRequest, AddDrawerResponse, ChangesQuery, ChangesResponse, CheckDuplicateRequest,
    CheckDuplicateResponse, DrawerSearchRequest, DrawerSearchResponse, ErrorBody,
    FEDERATION_API_VERSION, IngestBatchRequest, IngestBatchResponse, InfoResponse,
    KgAddFactRequest, KgInvalidateRequest, KgQueryRequest, ListDrawersQuery, ListDrawersResponse,
};

use crate::{
    RemoteApi, RemoteEndpoint,
    error::{RemoteError, Result},
};

/// Maximum body length (in bytes) included verbatim in [`RemoteError::RemoteRejected`].
///
/// Bodies larger than this are truncated to avoid flooding logs.
const MAX_ERROR_BODY: usize = 2048;

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
        self.execute(rb).await
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

    /// Send a prepared [`reqwest::RequestBuilder`], inject auth, and decode the
    /// response body as `T`.
    ///
    /// Error classification:
    /// - Network/timeout failures → [`RemoteError::Unreachable`]
    /// - HTTP 401 → [`RemoteError::Unauthorized`]
    /// - Other non-2xx → [`RemoteError::RemoteRejected`] (body included)
    /// - 2xx with bad JSON → [`RemoteError::InvalidResponse`]
    async fn execute<T: serde::de::DeserializeOwned>(&self, rb: reqwest::RequestBuilder) -> Result<T> {
        let rb = match &self.token {
            Some(tok) => rb.bearer_auth(tok),
            None => rb,
        };

        let response = rb.send().await.map_err(|e| RemoteError::Unreachable {
            remote: self.name.clone(),
            message: e.to_string(),
        })?;

        let status = response.status();

        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(RemoteError::Unauthorized { remote: self.name.clone() });
        }

        if !status.is_success() {
            // Read the body in chunks, stopping at MAX_ERROR_BODY to avoid
            // memory exhaustion from large error responses (e.g. HTML pages).
            let mut response = response;
            let mut bytes = Vec::new();
            while let Ok(Some(chunk)) = response.chunk().await {
                bytes.extend_from_slice(&chunk);
                if bytes.len() >= MAX_ERROR_BODY {
                    break;
                }
            }
            let raw = String::from_utf8_lossy(&bytes).into_owned();
            let body = if let Ok(err_body) = serde_json::from_str::<ErrorBody>(&raw) {
                format!("{}: {}", err_body.code, err_body.message)
            } else if raw.len() > MAX_ERROR_BODY {
                // Truncate on a char boundary — slicing at a fixed byte offset
                // would panic mid-way through a multi-byte UTF-8 character.
                let mut cut = MAX_ERROR_BODY;
                while !raw.is_char_boundary(cut) {
                    cut -= 1;
                }
                format!("{}… (truncated)", &raw[..cut])
            } else {
                raw
            };
            return Err(RemoteError::RemoteRejected {
                remote: self.name.clone(),
                status: status.as_u16(),
                body,
            });
        }

        response.json::<T>().await.map_err(|e| RemoteError::InvalidResponse {
            remote: self.name.clone(),
            message: e.to_string(),
        })
    }
}

#[async_trait::async_trait]
impl RemoteApi for RemoteClient {
    /// Return the cached server info, performing the handshake if needed.
    async fn info(&self) -> Result<InfoResponse> {
        Ok(self.ensure_handshake().await?.clone())
    }

    /// Search drawers using semantic full-text matching (`POST /v1/drawers/search`).
    async fn search_drawers(&self, req: DrawerSearchRequest) -> Result<DrawerSearchResponse> {
        self.ensure_handshake().await?;
        let url = self.url("v1/drawers/search")?;
        let rb = self.http.post(url).json(&req);
        self.execute(rb).await
    }

    /// Check whether content is a near-duplicate of an existing drawer (`POST /v1/drawers/check_duplicate`).
    async fn check_duplicate(&self, req: CheckDuplicateRequest) -> Result<CheckDuplicateResponse> {
        self.ensure_handshake().await?;
        let url = self.url("v1/drawers/check_duplicate")?;
        let rb = self.http.post(url).json(&req);
        self.execute(rb).await
    }

    /// Add a new drawer to the remote palace (`POST /v1/drawers`).
    async fn add_drawer(&self, req: AddDrawerRequest) -> Result<AddDrawerResponse> {
        self.ensure_handshake().await?;
        let url = self.url("v1/drawers")?;
        let rb = self.http.post(url).json(&req);
        self.execute(rb).await
    }

    /// List drawers with optional filtering and pagination (`GET /v1/drawers`).
    ///
    /// `query` is serialized as URL query parameters via `serde_urlencoded`;
    /// `None` fields are omitted automatically.
    async fn list_drawers(&self, query: ListDrawersQuery) -> Result<ListDrawersResponse> {
        self.ensure_handshake().await?;
        let url = self.url("v1/drawers")?;
        let rb = self.http.get(url).query(&query);
        self.execute(rb).await
    }

    /// Retrieve a single drawer by its stable identifier (`GET /v1/drawers/{id}`).
    async fn get_drawer(&self, drawer_id: &str) -> Result<serde_json::Value> {
        self.ensure_handshake().await?;
        let path = format!("v1/drawers/{drawer_id}");
        let url = self.url(&path)?;
        let rb = self.http.get(url);
        self.execute(rb).await
    }

    /// Delete a drawer by its stable identifier (`DELETE /v1/drawers/{id}`).
    async fn delete_drawer(&self, drawer_id: &str) -> Result<()> {
        self.ensure_handshake().await?;
        let path = format!("v1/drawers/{drawer_id}");
        let url = self.url(&path)?;
        let rb = self.http.delete(url);
        self.execute::<serde_json::Value>(rb).await?;
        Ok(())
    }

    /// Query the knowledge graph for an entity (`POST /v1/kg/query`).
    async fn kg_query(&self, req: KgQueryRequest) -> Result<serde_json::Value> {
        self.ensure_handshake().await?;
        let url = self.url("v1/kg/query")?;
        let rb = self.http.post(url).json(&req);
        self.execute(rb).await
    }

    /// Add a fact to the knowledge graph (`POST /v1/kg/facts`).
    async fn kg_add_fact(&self, req: KgAddFactRequest) -> Result<serde_json::Value> {
        self.ensure_handshake().await?;
        let url = self.url("v1/kg/facts")?;
        let rb = self.http.post(url).json(&req);
        self.execute(rb).await
    }

    /// Invalidate a knowledge-graph fact (`POST /v1/kg/facts/invalidate`).
    async fn kg_invalidate(&self, req: KgInvalidateRequest) -> Result<serde_json::Value> {
        self.ensure_handshake().await?;
        let url = self.url("v1/kg/facts/invalidate")?;
        let rb = self.http.post(url).json(&req);
        self.execute(rb).await
    }

    /// Retrieve the knowledge-graph timeline, optionally filtered by entity (`GET /v1/kg/timeline`).
    async fn kg_timeline(&self, entity: Option<&str>) -> Result<serde_json::Value> {
        self.ensure_handshake().await?;
        let url = self.url("v1/kg/timeline")?;
        let rb = match entity {
            Some(e) => self.http.get(url).query(&[("entity", e)]),
            None => self.http.get(url),
        };
        self.execute(rb).await
    }

    /// Retrieve knowledge-graph statistics (`GET /v1/kg/stats`).
    async fn kg_stats(&self) -> Result<serde_json::Value> {
        self.ensure_handshake().await?;
        let url = self.url("v1/kg/stats")?;
        let rb = self.http.get(url);
        self.execute(rb).await
    }

    /// Retrieve the palace taxonomy (`GET /v1/taxonomy`).
    async fn taxonomy(&self) -> Result<serde_json::Value> {
        self.ensure_handshake().await?;
        let url = self.url("v1/taxonomy")?;
        let rb = self.http.get(url);
        self.execute(rb).await
    }

    /// List the wings in the remote palace (`GET /v1/wings`).
    async fn wings(&self) -> Result<serde_json::Value> {
        self.ensure_handshake().await?;
        let url = self.url("v1/wings")?;
        let rb = self.http.get(url);
        self.execute(rb).await
    }

    /// List rooms, optionally filtered by wing (`GET /v1/rooms`).
    async fn rooms(&self, wing: Option<&str>) -> Result<serde_json::Value> {
        self.ensure_handshake().await?;
        let url = self.url("v1/rooms")?;
        let rb = match wing {
            Some(w) => self.http.get(url).query(&[("wing", w)]),
            None => self.http.get(url),
        };
        self.execute(rb).await
    }

    /// Retrieve paginated change events (`GET /v1/changes`).
    ///
    /// `query` is serialized as URL query parameters via `serde_urlencoded`;
    /// `None` fields are omitted automatically.
    async fn changes(&self, query: ChangesQuery) -> Result<ChangesResponse> {
        self.ensure_handshake().await?;
        let url = self.url("v1/changes")?;
        let rb = self.http.get(url).query(&query);
        self.execute(rb).await
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
        self.execute(rb).await
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::DEFAULT_TIMEOUT;

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

        assert!(RemoteError::Unreachable { remote: mk_name(), message: "x".to_owned() }
            .is_degradable());
        assert!(!RemoteError::Unauthorized { remote: mk_name() }.is_degradable());
        assert!(!RemoteError::VersionSkew { remote: mk_name(), ours: 1, theirs: 2 }
            .is_degradable());
        assert!(!RemoteError::RemoteRejected {
            remote: mk_name(),
            status: 404,
            body: String::new()
        }
        .is_degradable());
        assert!(!RemoteError::InvalidResponse { remote: mk_name(), message: "x".to_owned() }
            .is_degradable());
        assert!(!RemoteError::InvalidConfig { remote: mk_name(), message: "x".to_owned() }
            .is_degradable());
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
}

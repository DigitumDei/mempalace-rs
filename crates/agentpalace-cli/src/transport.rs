//! MCP transports sharing the same dispatcher and host-configured lineage.
use std::sync::{Arc, Mutex};

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use agentpalace_config::AgentPalaceConfig;
use agentpalace_embeddings::{EmbeddingProvider, env_flag};
use agentpalace_mcp::{
    DeterministicStubProvider, McpServer, configured_lineage_id_from_env, default_provider,
    serve_transport,
};
use agentpalace_server::TokenRegistry;
use serde_json::Value;

type Error = Box<dyn std::error::Error>;

// HTTP MCP and REST share one model session instead of loading the weights twice.
pub struct SharedProvider<P> {
    inner: Arc<Mutex<P>>,
    profile: &'static agentpalace_core::EmbeddingProfileMetadata,
}

impl<P: EmbeddingProvider> SharedProvider<P> {
    pub fn new(provider: P) -> Self {
        let profile = provider.profile();
        Self { inner: Arc::new(Mutex::new(provider)), profile }
    }
}

impl<P> Clone for SharedProvider<P> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone(), profile: self.profile }
    }
}

impl<P: EmbeddingProvider> EmbeddingProvider for SharedProvider<P> {
    fn profile(&self) -> &'static agentpalace_core::EmbeddingProfileMetadata {
        self.profile
    }
    fn startup_validation(
        &self,
    ) -> agentpalace_embeddings::Result<agentpalace_embeddings::StartupValidation> {
        self.inner
            .lock()
            .map_err(|_| {
                agentpalace_embeddings::EmbeddingError::ProviderContract(
                    "embedding provider lock poisoned".into(),
                )
            })?
            .startup_validation()
    }
    fn embed(
        &mut self,
        request: &agentpalace_embeddings::EmbeddingRequest,
    ) -> agentpalace_embeddings::Result<agentpalace_embeddings::EmbeddingResponse> {
        self.inner
            .lock()
            .map_err(|_| {
                agentpalace_embeddings::EmbeddingError::ProviderContract(
                    "embedding provider lock poisoned".into(),
                )
            })?
            .embed(request)
    }
}

pub async fn stdio(config: AgentPalaceConfig) -> Result<(), Error> {
    let lineage = configured_lineage_id_from_env()?;
    if env_flag("AGENTPALACE_STUB_EMBEDDINGS") {
        let provider = DeterministicStubProvider::new(config.embedding_profile);
        let server = McpServer::from_parts_with_lineage(config, provider, lineage).await?;
        serve_transport(&server, tokio::io::BufReader::new(tokio::io::stdin()), tokio::io::stdout())
            .await
    } else {
        let provider = default_provider(config.embedding_profile)?;
        let server = McpServer::from_parts_with_lineage(config, provider, lineage).await?;
        serve_transport(&server, tokio::io::BufReader::new(tokio::io::stdin()), tokio::io::stdout())
            .await
    }
}

pub async fn http_router<P: EmbeddingProvider + Send + Sync + 'static>(
    config: AgentPalaceConfig,
    provider: P,
    tokens: Arc<TokenRegistry>,
) -> Result<Router, Error> {
    let lineage = configured_lineage_id_from_env()?;
    Ok(router(McpServer::from_parts_with_lineage(config, provider, lineage).await?, tokens))
}

struct HttpState<P> {
    server: McpServer<P>,
    tokens: Arc<TokenRegistry>,
}

fn router<P: EmbeddingProvider + Send + Sync + 'static>(
    server: McpServer<P>,
    tokens: Arc<TokenRegistry>,
) -> Router {
    Router::new()
        .route("/mcp", post(handle::<P>))
        .with_state(Arc::new(HttpState { server, tokens }))
}

// Stateless Streamable HTTP (2025-03-26): JSON responses, no session IDs or
// unsolicited events. Axum returns 405 for GET, as permitted by the transport.
async fn handle<P: EmbeddingProvider + Send + Sync + 'static>(
    State(state): State<Arc<HttpState<P>>>,
    headers: HeaderMap,
    body: String,
) -> Response {
    // Browser origins are deliberately unsupported; native MCP clients omit Origin.
    if headers.contains_key("origin") {
        return StatusCode::FORBIDDEN.into_response();
    }
    let identity = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .and_then(|v| state.tokens.authenticate(v));
    match identity {
        None => return StatusCode::UNAUTHORIZED.into_response(),
        Some(identity) if !identity.is_unrestricted() => {
            return StatusCode::FORBIDDEN.into_response();
        }
        _ => {}
    }
    if headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_none_or(|v| v.split(';').next().map(str::trim) != Some("application/json"))
    {
        return StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response();
    }
    let accept = headers.get("accept").and_then(|v| v.to_str().ok()).unwrap_or("");
    if !accept.contains("application/json") || !accept.contains("text/event-stream") {
        return StatusCode::NOT_ACCEPTABLE.into_response();
    }
    if headers.get("mcp-protocol-version").is_some_and(|v| v != "2025-03-26") {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let request: Value = match serde_json::from_str(&body) {
        Ok(value) => value,
        Err(_) => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}))).into_response(),
    };
    let requests = if let Some(batch) = request.as_array() {
        if batch.is_empty() {
            return StatusCode::BAD_REQUEST.into_response();
        }
        batch.clone()
    } else {
        vec![request.clone()]
    };
    // Reject malformed envelopes before any tool can mutate storage.
    if requests.iter().any(|request| !request.is_object() || request["jsonrpc"] != "2.0") {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let mut responses = Vec::new();
    for request in requests {
        let initialize = request["method"] == "initialize";
        let mut response = state.server.handle_json_value(request).await;
        if initialize
            && let Some(result) = response.get_mut("result").and_then(Value::as_object_mut)
        {
            result.insert("protocolVersion".into(), Value::String("2025-03-26".into()));
        }
        if !response.is_null() {
            responses.push(response);
        }
    }
    if responses.is_empty() {
        return StatusCode::ACCEPTED.into_response();
    }
    if request.is_array() {
        Json(Value::Array(responses)).into_response()
    } else {
        Json(responses.remove(0)).into_response()
    }
}

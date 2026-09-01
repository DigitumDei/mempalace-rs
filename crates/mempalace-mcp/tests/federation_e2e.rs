//! End-to-end federation integration tests for the MCP server.
//!
//! Each test spawns a REAL federation HTTP server (mempalace-server crate) in-process
//! on an ephemeral port 0, constructs an `McpServer` whose federation config points at
//! the spawned server, and exercises federated tool paths through the MCP
//! request-handling entry point.
//!
//! Spawning pattern mirrors `crates/mempalace-remote/tests/remote_client_e2e.rs`.
//! MCP call pattern mirrors `crates/mempalace-mcp/tests/stdio_harness.rs`.

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use mempalace_config::{
    FederationRuntimeConfig, LowCpuRuntimeConfig, MaintenanceRuntimeConfig, MempalaceConfig,
    ResolvedRemote, ResolvedRouteRule, RouteMode, ServerRuntimeConfig, WriteTarget,
};
use mempalace_core::EmbeddingProfile;
use mempalace_federation::{DrawerSearchRequest, KgQueryRequest};
use mempalace_mcp::{DeterministicStubProvider, JsonRpcRequest, McpServer, decode_tool_payload};
use mempalace_remote::{RemoteApi, RemoteClient, RemoteEndpoint};
use mempalace_server::{TokenRegistry, build_router};
use serde_json::{Value, json};
use tempfile::TempDir;

// ─── Test token ───────────────────────────────────────────────────────────────

const TEST_TOKEN: &str = "fed-e2e-secret-token-xyz";
fn restrict_token_file(path: &std::path::Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = std::fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o600);
        std::fs::set_permissions(path, permissions).unwrap();
    }

    #[cfg(not(unix))]
    {
        let _ = path;
    }
}

// ─── Shared spawning helpers ──────────────────────────────────────────────────

/// Write a minimal bearer-token file and return its path.
fn write_token_file(dir: &TempDir) -> PathBuf {
    let path = dir.path().join("tokens.json");
    std::fs::write(
        &path,
        serde_json::to_string(&json!([
            {"token": TEST_TOKEN, "name": "e2e-fed-user", "enabled": true}
        ]))
        .unwrap(),
    )
    .unwrap();
    restrict_token_file(&path);
    path
}

/// Build a base `MempalaceConfig` rooted at `dir` with no federation (server side).
fn server_config(dir: &TempDir) -> MempalaceConfig {
    MempalaceConfig {
        schema_version: 1,
        collection_name: "mempalace_drawers".to_owned(),
        palace_path: dir.path().join("palace"),
        embedding_profile: EmbeddingProfile::Balanced,
        low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
        server: ServerRuntimeConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            token_file: dir.path().join("tokens.json"),
            checkouts: std::collections::BTreeMap::new(),
        },
        maintenance: MaintenanceRuntimeConfig::defaults(),
        federation: FederationRuntimeConfig::default(),
    }
}

/// Spawn the real federation server on an ephemeral port.
/// Returns the bound `SocketAddr`.
async fn spawn_server(dir: &TempDir) -> SocketAddr {
    spawn_server_with_handle(dir).await.0
}

/// Spawn the real federation server and return both the address and a
/// `JoinHandle` so the caller can stop the server by aborting the handle.
async fn spawn_server_with_handle(dir: &TempDir) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let token_file = write_token_file(dir);
    let config = server_config(dir);
    let tokens = TokenRegistry::load(token_file).unwrap();
    let provider = DeterministicStubProvider::new(EmbeddingProfile::Balanced);
    let (router, _state) = build_router(config, provider, tokens).await.unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (addr, handle)
}

/// Build an `McpServer` with a federation config that registers a single remote
/// named `"hub"` at `hub_url` with `token`, and applies per-wing and KG rules
/// supplied by the caller.
async fn mcp_server_with_hub(
    local_dir: &TempDir,
    hub_url: &str,
    wing_rules: BTreeMap<String, ResolvedRouteRule>,
    default_mode: RouteMode,
    kg_rule: Option<ResolvedRouteRule>,
) -> McpServer<DeterministicStubProvider> {
    let mut remotes = BTreeMap::new();
    remotes.insert(
        "hub".to_owned(),
        ResolvedRemote {
            name: "hub".to_owned(),
            url: hub_url.to_owned(),
            token: Some(TEST_TOKEN.to_owned()),
            timeout: Duration::from_secs(5),
        },
    );

    // When default_mode is not Local, we need default_remote set.
    let default_remote = match default_mode {
        RouteMode::Local => None,
        _ => Some("hub".to_owned()),
    };

    let federation = FederationRuntimeConfig {
        remotes,
        default_mode,
        default_remote,
        wings: wing_rules,
        kg: kg_rule,
        coordination: BTreeMap::new(),
    };

    let config = MempalaceConfig {
        schema_version: 1,
        collection_name: "mempalace_drawers".to_owned(),
        palace_path: local_dir.path().join("palace"),
        embedding_profile: EmbeddingProfile::Balanced,
        low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
        server: ServerRuntimeConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            token_file: local_dir.path().join("server_tokens.json"),
            checkouts: std::collections::BTreeMap::new(),
        },
        maintenance: MaintenanceRuntimeConfig::defaults(),
        federation,
    };

    McpServer::from_parts(config, DeterministicStubProvider::new(EmbeddingProfile::Balanced))
        .await
        .unwrap()
}

/// Like [`mcp_server_with_hub`], but also sets `federation.coordination` — used by the
/// coordination-specific tests below (issue #102 Stage 4), which need a route on that separate
/// table rather than `federation.wings`.
#[allow(clippy::too_many_arguments)]
async fn mcp_server_with_hub_coordination(
    local_dir: &TempDir,
    hub_url: &str,
    wing_rules: BTreeMap<String, ResolvedRouteRule>,
    coordination_rules: BTreeMap<String, ResolvedRouteRule>,
    default_mode: RouteMode,
) -> McpServer<DeterministicStubProvider> {
    mcp_server_with_hub_multi(
        local_dir,
        &[("hub", hub_url)],
        wing_rules,
        coordination_rules,
        default_mode,
    )
    .await
}

/// Build an `McpServer` with a federation config registering one or more named remotes, plus
/// `wings` and `coordination` routing tables — the general form
/// [`mcp_server_with_hub`]/[`mcp_server_with_hub_coordination`] specialize. Used directly by
/// tests that need more than one remote (e.g. the events fan-out isolation test).
async fn mcp_server_with_hub_multi(
    local_dir: &TempDir,
    remotes: &[(&str, &str)],
    wing_rules: BTreeMap<String, ResolvedRouteRule>,
    coordination_rules: BTreeMap<String, ResolvedRouteRule>,
    default_mode: RouteMode,
) -> McpServer<DeterministicStubProvider> {
    let mut resolved_remotes = BTreeMap::new();
    for (name, url) in remotes {
        resolved_remotes.insert(
            (*name).to_owned(),
            ResolvedRemote {
                name: (*name).to_owned(),
                url: (*url).to_owned(),
                token: Some(TEST_TOKEN.to_owned()),
                timeout: Duration::from_secs(5),
            },
        );
    }
    let default_remote = match default_mode {
        RouteMode::Local => None,
        _ => remotes.first().map(|(name, _)| (*name).to_owned()),
    };

    let federation = FederationRuntimeConfig {
        remotes: resolved_remotes,
        default_mode,
        default_remote,
        wings: wing_rules,
        kg: None,
        coordination: coordination_rules,
    };

    let config = MempalaceConfig {
        schema_version: 1,
        collection_name: "mempalace_drawers".to_owned(),
        palace_path: local_dir.path().join("palace"),
        embedding_profile: EmbeddingProfile::Balanced,
        low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
        server: ServerRuntimeConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            token_file: local_dir.path().join("server_tokens.json"),
            checkouts: std::collections::BTreeMap::new(),
        },
        maintenance: MaintenanceRuntimeConfig::defaults(),
        federation,
    };

    McpServer::from_parts(config, DeterministicStubProvider::new(EmbeddingProfile::Balanced))
        .await
        .unwrap()
}

/// Build a JSON-RPC tool-call request value (for use with `handle_request`).
fn tool_call(id: u64, tool: &str, arguments: Value) -> JsonRpcRequest {
    JsonRpcRequest {
        jsonrpc: Some("2.0".to_owned()),
        id: Some(json!(id)),
        method: "tools/call".to_owned(),
        params: json!({"name": tool, "arguments": arguments}),
    }
}

/// Invoke a tool on `server` and return the decoded payload (panics on missing).
async fn call_tool(
    server: &McpServer<DeterministicStubProvider>,
    id: u64,
    tool: &str,
    arguments: Value,
) -> Value {
    let response = server.handle_request(tool_call(id, tool, arguments)).await;
    decode_tool_payload(&response)
        .unwrap_or_else(|| panic!("no payload in response for tool {tool}: {response}"))
}

/// Poll an asynchronous condition with a deadline, sleeping between attempts.
///
/// issue #127 replication is durable and background-threaded: a `write: both` tool call returns
/// `status: "queued"` with a stable `operation_id` immediately, and the in-process worker
/// delivers asynchronously. Tests must therefore wait on the *observable outcome* rather than
/// assert on the immediate response. For delivery-success this is the remote's own state (polled
/// via the hub client); for terminal outcomes it is `mempalace_status` →
/// `replication.recent_terminal_failures`; for retryable outages it is the outbox backlog.
async fn poll_until<Fut, F>(what: &str, timeout: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if check().await {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn hub_client(hub_url: &str) -> std::sync::Arc<dyn RemoteApi> {
    std::sync::Arc::new(
        RemoteClient::new(RemoteEndpoint {
            name: "hub".to_owned(),
            base_url: hub_url.to_owned(),
            token: Some(TEST_TOKEN.to_owned()),
            timeout: Duration::from_secs(5),
        })
        .unwrap(),
    )
}

/// Poll the hub directly until it holds a drawer whose content text equals `content`
/// in the given wing/room — proof the background worker delivered the queued mutation.
async fn wait_for_hub_drawer(hub_url: &str, content: &str, wing: &str, room: &str) {
    let client = std::sync::Arc::clone(&hub_client(hub_url));
    poll_until(
        &format!("drawer `{content}` to appear on the hub"),
        Duration::from_secs(20),
        move || {
            let client = std::sync::Arc::clone(&client);
            let content = content.to_owned();
            let wing = wing.to_owned();
            let room = room.to_owned();
            async move {
                let req = DrawerSearchRequest {
                    query: content.clone(),
                    wing: Some(wing.clone()),
                    room: Some(room),
                    limit: Some(20),
                    view: None,
                };
                match client.search_drawers(req).await {
                    Ok(resp) => resp.results.iter().any(|r| r.content == content),
                    Err(_) => false,
                }
            }
        },
    )
    .await;
}

/// Poll the hub's KG until the fact `predicate → object` for `entity` is (or is no longer,
/// when `present` is false) returned with the given `as_of`.
async fn wait_for_hub_kg_fact(
    hub_url: &str,
    entity: &str,
    predicate: &str,
    object: &str,
    as_of: Option<&str>,
    present: bool,
) {
    let client = std::sync::Arc::clone(&hub_client(hub_url));
    poll_until(
        &format!(
            "KG fact {entity} {predicate} {object} as_of {as_of:?} to be {} on the hub",
            if present { "present" } else { "absent" }
        ),
        Duration::from_secs(20),
        move || {
            let client = std::sync::Arc::clone(&client);
            let entity = entity.to_owned();
            let predicate = predicate.to_owned();
            let object = object.to_owned();
            let as_of = as_of.map(ToOwned::to_owned);
            async move {
                match client
                    .kg_query(KgQueryRequest {
                        entity: entity.clone(),
                        as_of: as_of.clone(),
                        direction: Some("outgoing".to_owned()),
                    })
                    .await
                {
                    Ok(payload) => {
                        let found = payload["facts"].as_array().map_or(false, |facts| {
                            facts.iter().any(|f| {
                                f["predicate"].as_str() == Some(predicate.as_str())
                                    && f["object"].as_str() == Some(object.as_str())
                            })
                        });
                        if present { found } else { !found }
                    }
                    Err(_) => false,
                }
            }
        },
    )
    .await;
}

/// Return the newest `mempalace_status` payload.
async fn status_of(server: &McpServer<DeterministicStubProvider>) -> Value {
    call_tool(server, 900, "mempalace_status", json!({})).await
}

/// Whether `replication.recent_terminal_failures` lists an entry for `operation_id`.
fn status_has_terminal_failure(status: &Value, operation_id: &str) -> bool {
    status["replication"]["recent_terminal_failures"].as_array().map_or(false, |fails| {
        fails.iter().any(|f| f["operation_id"].as_str() == Some(operation_id))
    })
}

/// Whether the status backlog reports any retryable operation.
fn status_has_retryable(status: &Value) -> bool {
    status["replication"]["backlog"]["retryable_count"].as_i64().unwrap_or(0) >= 1
}

/// Poll `mempalace_status` until `predicate` holds; panics after `timeout`.
async fn wait_for_status<P>(server: &McpServer<DeterministicStubProvider>, what: &str, predicate: P)
where
    P: Fn(&Value) -> bool,
{
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let status = status_of(server).await;
        if predicate(&status) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what}; last status: {status}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// ─── Helper: combined-mode wing rule routing to "hub" with Remote write ───────

fn combined_wing_rule_remote_write() -> ResolvedRouteRule {
    ResolvedRouteRule {
        mode: RouteMode::Combined,
        remote: Some("hub".to_owned()),
        write: WriteTarget::Remote,
    }
}

fn remote_wing_rule() -> ResolvedRouteRule {
    ResolvedRouteRule {
        mode: RouteMode::Remote,
        remote: Some("hub".to_owned()),
        write: WriteTarget::Local,
    }
}

fn combined_wing_rule_both_write() -> ResolvedRouteRule {
    ResolvedRouteRule {
        mode: RouteMode::Combined,
        remote: Some("hub".to_owned()),
        write: WriteTarget::Both,
    }
}

fn combined_kg_rule_both_write() -> ResolvedRouteRule {
    ResolvedRouteRule {
        mode: RouteMode::Combined,
        remote: Some("hub".to_owned()),
        write: WriteTarget::Both,
    }
}

#[tokio::test]
async fn federated_kg_reads_return_empty_for_unknown_entities() {
    let hub_dir = TempDir::new().unwrap();
    let local_dir = TempDir::new().unwrap();
    let addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{addr}");

    // A federation endpoint must return an empty result for an entity absent
    // from this palace, rather than making the peer appear unavailable.
    let hub_client = RemoteClient::new(RemoteEndpoint {
        name: "hub".to_owned(),
        base_url: hub_url.clone(),
        token: Some(TEST_TOKEN.to_owned()),
        timeout: Duration::from_secs(5),
    })
    .unwrap();
    let remote = hub_client
        .kg_query(KgQueryRequest {
            entity: "unknown federation entity".to_owned(),
            as_of: None,
            direction: None,
        })
        .await
        .unwrap();
    assert_eq!(remote["count"], 0);
    assert_eq!(remote["facts"], json!([]));

    let server = mcp_server_with_hub(
        &local_dir,
        &hub_url,
        BTreeMap::new(),
        RouteMode::Local,
        Some(ResolvedRouteRule {
            mode: RouteMode::Combined,
            remote: Some("hub".to_owned()),
            write: WriteTarget::Local,
        }),
    )
    .await;

    // The local palace also lacks the entity. It must still merge the remote
    // result instead of short-circuiting with an unknown-entity error.
    let response =
        call_tool(&server, 1, "mempalace_kg_query", json!({"entity": "unknown federation entity"}))
            .await;
    assert_eq!(response["count"], 0);
    assert_eq!(response["facts"], json!([]));
    assert!(response.get("warnings").is_none(), "{response}");

    let timeline = call_tool(
        &server,
        2,
        "mempalace_kg_timeline",
        json!({"entity": "unknown federation entity"}),
    )
    .await;
    assert_eq!(timeline["count"], 0);
    assert_eq!(timeline["timeline"], json!([]));
    assert!(timeline.get("warnings").is_none(), "{timeline}");
}

// ─── Test 1: add_remote_search_combined_delete_remote_roundtrip ──────────────

/// Flagship flow:
/// 1. Seed "wing_shared" with Combined/write=Remote → write goes to hub.
/// 2. Also seed "wing_local" locally (no rule → default Local).
/// 3. Search wing_shared → hub origin present.
/// 4. Search with no wing filter → both origins (local + hub).
/// 5. Delete the remote drawer → hub handles it; "origin":"hub" in response.
#[tokio::test]
async fn add_remote_search_combined_delete_remote_roundtrip() {
    let hub_dir = TempDir::new().unwrap();
    let local_dir = TempDir::new().unwrap();

    let addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{addr}");

    let mut wing_rules = BTreeMap::new();
    wing_rules.insert("wing_shared".to_owned(), combined_wing_rule_remote_write());

    let server =
        mcp_server_with_hub(&local_dir, &hub_url, wing_rules, RouteMode::Local, None).await;

    // ── 1. Add into wing_shared (Combined, write=Remote) ─────────────────────
    let add_shared = call_tool(
        &server,
        1,
        "mempalace_add_drawer",
        json!({
            "wing": "wing_shared",
            "room": "general",
            "content": "federation e2e combined write remote drawer xyzzy",
            "added_by": "e2e-test"
        }),
    )
    .await;

    assert_eq!(add_shared["success"], true, "add_shared failed: {add_shared}");
    assert_eq!(add_shared["origin"], "hub", "add_shared must report origin=hub; got: {add_shared}");
    assert_eq!(
        add_shared["applied_to"], "remote:hub",
        "add_shared must report applied_to=remote:hub; got: {add_shared}"
    );
    let remote_drawer_id =
        add_shared["drawer_id"].as_str().expect("add_shared must return drawer_id").to_owned();

    // ── 2. Add locally into wing_local (no rule → Local) ─────────────────────
    let add_local = call_tool(
        &server,
        2,
        "mempalace_add_drawer",
        json!({
            "wing": "wing_local",
            "room": "notes",
            "content": "federation e2e local drawer abcde",
            "added_by": "e2e-test"
        }),
    )
    .await;

    assert_eq!(add_local["success"], true, "add_local failed: {add_local}");
    // Local adds do not include "origin" field.
    assert!(
        add_local.get("origin").is_none() || add_local["origin"].is_null(),
        "local add must not include origin field; got: {add_local}"
    );

    // ── 3. Search wing_shared → hub origin ───────────────────────────────────
    let search_shared = call_tool(
        &server,
        3,
        "mempalace_search",
        json!({
            "query": "federation e2e combined write remote xyzzy",
            "wing": "wing_shared",
            "limit": 5
        }),
    )
    .await;

    let results = search_shared["results"].as_array().expect("results must be array");
    assert!(!results.is_empty(), "search wing_shared must return results");
    let hub_result = results.iter().find(|r| r["origin"].as_str() == Some("hub"));
    assert!(
        hub_result.is_some(),
        "search wing_shared must contain a result with origin=hub; results: {results:?}"
    );

    // ── 4. Search with no wing filter → both origins ─────────────────────────
    let search_all = call_tool(
        &server,
        4,
        "mempalace_search",
        json!({
            "query": "federation e2e",
            "limit": 10
        }),
    )
    .await;

    let all_results = search_all["results"].as_array().expect("results must be array");
    assert!(!all_results.is_empty(), "global search must return results");

    let has_local = all_results
        .iter()
        .any(|r| r["origin"].as_str() == Some("local") || r.get("origin").is_none());
    let has_hub = all_results.iter().any(|r| r["origin"].as_str() == Some("hub"));
    assert!(has_local, "global search must include local result; results: {all_results:?}");
    assert!(has_hub, "global search must include hub result; results: {all_results:?}");

    // ── 5. Delete remote drawer → local miss, remote fallback deletes ────────
    let delete_resp =
        call_tool(&server, 5, "mempalace_delete_drawer", json!({"drawer_id": remote_drawer_id}))
            .await;

    assert_eq!(
        delete_resp["success"], true,
        "delete_drawer must return success=true; got: {delete_resp}"
    );
    assert_eq!(
        delete_resp["origin"], "hub",
        "delete must fall back to the remote and report origin=hub; got: {delete_resp}"
    );
    assert_eq!(
        delete_resp["applied_to"], "remote:hub",
        "delete must report applied_to=remote:hub; got: {delete_resp}"
    );
    let echoed_id = delete_resp["drawer_id"].as_str().unwrap_or("");
    assert_eq!(
        echoed_id, remote_drawer_id,
        "delete_drawer must echo the requested drawer_id; got: {delete_resp}"
    );

    // ── 6. Search wing_shared again → the hub drawer is gone ─────────────────
    let search_after = call_tool(
        &server,
        6,
        "mempalace_search",
        json!({
            "query": "federation e2e combined write remote xyzzy",
            "wing": "wing_shared",
            "limit": 5
        }),
    )
    .await;
    let after_results = search_after["results"].as_array().expect("results must be array");
    assert!(
        !after_results.iter().any(|r| r["origin"].as_str() == Some("hub")),
        "deleted hub drawer must no longer appear in search; results: {after_results:?}"
    );
}

// ─── Test 2: duplicate_add_remote_returns_duplicate_shape ────────────────────

/// Adding the same content twice through the MCP server (Combined/write=Remote)
/// must produce `{"success": false, "reason": "duplicate"}` on the second call,
/// NOT an internal error. This validates pre-add remote duplicate check + 409 mapping.
#[tokio::test]
async fn duplicate_add_remote_returns_duplicate_shape() {
    let hub_dir = TempDir::new().unwrap();
    let local_dir = TempDir::new().unwrap();

    let addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{addr}");

    let mut wing_rules = BTreeMap::new();
    wing_rules.insert("wing_dup".to_owned(), combined_wing_rule_remote_write());

    let server =
        mcp_server_with_hub(&local_dir, &hub_url, wing_rules, RouteMode::Local, None).await;

    let content = "duplicate detection e2e remote content quux plugh";

    // First add — must succeed with origin=hub.
    let first = call_tool(
        &server,
        1,
        "mempalace_add_drawer",
        json!({"wing": "wing_dup", "room": "test", "content": content}),
    )
    .await;
    assert_eq!(first["success"], true, "first add must succeed; got: {first}");
    assert_eq!(first["origin"], "hub", "first add must report hub origin; got: {first}");
    assert_eq!(
        first["applied_to"], "remote:hub",
        "first add must report applied_to=remote:hub; got: {first}"
    );

    // Second identical add — must return success=false with reason=duplicate.
    let second = call_tool(
        &server,
        2,
        "mempalace_add_drawer",
        json!({"wing": "wing_dup", "room": "test", "content": content}),
    )
    .await;
    assert_eq!(
        second["success"], false,
        "second identical add must return success=false; got: {second}"
    );
    assert_eq!(
        second["reason"], "duplicate",
        "second identical add must have reason=duplicate; got: {second}"
    );
    // Verify there is no JSON-RPC error (i.e. it was handled gracefully, not as internal error).
    assert!(
        second.get("error").is_none(),
        "duplicate response must not be a JSON-RPC error; got: {second}"
    );
}

// ─── Test 3: different_embedding_profiles_per_side ───────────────────────────

/// Server side uses EmbeddingProfile::Balanced; MCP side uses EmbeddingProfile::LowCpu.
/// Both sides are seeded with distinct, recognisable content. A combined search must
/// return results from both origins, proving that scores are never compared cross-profile
/// (the N-way rank interleave handles them independently).
#[tokio::test]
async fn different_embedding_profiles_per_side() {
    // Hub server uses Balanced profile.
    let hub_dir = TempDir::new().unwrap();
    let token_file = write_token_file(&hub_dir);
    let hub_config = server_config(&hub_dir);
    let tokens = TokenRegistry::load(token_file).unwrap();
    let hub_provider = DeterministicStubProvider::new(EmbeddingProfile::Balanced);
    let (router, _state) = build_router(hub_config, hub_provider, tokens).await.unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let hub_url = format!("http://{addr}");

    // MCP-side local dir with LowCpu profile.
    let local_dir = TempDir::new().unwrap();

    let mut remotes = BTreeMap::new();
    remotes.insert(
        "hub".to_owned(),
        ResolvedRemote {
            name: "hub".to_owned(),
            url: hub_url.clone(),
            token: Some(TEST_TOKEN.to_owned()),
            timeout: Duration::from_secs(5),
        },
    );

    // Combined default_mode so global search hits both sides.
    let mut wing_rules = BTreeMap::new();
    wing_rules.insert(
        "wing_profiles".to_owned(),
        ResolvedRouteRule {
            mode: RouteMode::Combined,
            remote: Some("hub".to_owned()),
            write: WriteTarget::Remote,
        },
    );

    let federation = FederationRuntimeConfig {
        remotes,
        default_mode: RouteMode::Local,
        default_remote: None,
        wings: wing_rules,
        kg: None,
        coordination: BTreeMap::new(),
    };

    let local_config = MempalaceConfig {
        schema_version: 1,
        collection_name: "mempalace_drawers".to_owned(),
        palace_path: local_dir.path().join("palace"),
        // MCP side uses LowCpu — different profile than hub.
        embedding_profile: EmbeddingProfile::LowCpu,
        low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::LowCpu),
        server: ServerRuntimeConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            token_file: local_dir.path().join("server_tokens.json"),
            checkouts: std::collections::BTreeMap::new(),
        },
        maintenance: MaintenanceRuntimeConfig::defaults(),
        federation,
    };

    let local_provider = DeterministicStubProvider::new(EmbeddingProfile::LowCpu);
    let server = McpServer::from_parts(local_config, local_provider).await.unwrap();

    // Seed the hub side via the MCP server (write=Remote routes it there).
    let hub_add = call_tool(
        &server,
        1,
        "mempalace_add_drawer",
        json!({
            "wing": "wing_profiles",
            "room": "hub-side",
            "content": "embedding profile test hub side unique content theta iota kappa",
            "added_by": "profiles-test"
        }),
    )
    .await;
    assert_eq!(hub_add["success"], true, "hub add must succeed: {hub_add}");
    assert_eq!(hub_add["origin"], "hub", "hub add must go to hub: {hub_add}");
    assert_eq!(
        hub_add["applied_to"], "remote:hub",
        "hub add must report applied_to=remote:hub: {hub_add}"
    );

    // Seed local side directly (wing_local has no rule → Local).
    let local_add = call_tool(
        &server,
        2,
        "mempalace_add_drawer",
        json!({
            "wing": "wing_local_profiles",
            "room": "local-side",
            "content": "embedding profile test local side unique content alpha beta gamma",
            "added_by": "profiles-test"
        }),
    )
    .await;
    assert_eq!(local_add["success"], true, "local add must succeed: {local_add}");

    // Search wing_profiles (Combined) → must include hub result.
    let search_profiles = call_tool(
        &server,
        3,
        "mempalace_search",
        json!({"query": "embedding profile test", "wing": "wing_profiles", "limit": 10}),
    )
    .await;
    let profile_results = search_profiles["results"].as_array().expect("results array");
    assert!(
        !profile_results.is_empty(),
        "search wing_profiles must return results; got: {search_profiles}"
    );
    let hub_hit = profile_results.iter().any(|r| r["origin"].as_str() == Some("hub"));
    assert!(hub_hit, "search wing_profiles must include hub origin; results: {profile_results:?}");

    // Global search (no wing) → must include both origins.
    let search_global = call_tool(
        &server,
        4,
        "mempalace_search",
        json!({"query": "embedding profile test unique content", "limit": 10}),
    )
    .await;
    let global_results = search_global["results"].as_array().expect("results array");
    assert!(!global_results.is_empty(), "global search must return results");

    let origins: Vec<&str> = global_results.iter().filter_map(|r| r["origin"].as_str()).collect();
    let has_hub = origins.iter().any(|&o| o == "hub");
    let has_local = origins.iter().any(|&o| o == "local");
    // The key assertion: both origins are present in the combined result set,
    // even though each side ranks with a different embedding profile.
    assert!(has_hub, "global search must include hub origin; origins: {origins:?}");
    assert!(has_local, "global search must include local origin; origins: {origins:?}");
}

// ─── Test 4: remote_down_degrades_reads ──────────────────────────────────────

/// When the federation remote is unreachable (port listener dropped), Combined-mode
/// search still returns local results plus a "warnings" entry. `mempalace_status`
/// reports the remote with "reachable": false. `mempalace_kg_query` returns local
/// facts plus warnings.
#[tokio::test]
async fn remote_down_degrades_reads() {
    // Bind a port to get a free address, then drop so nothing listens.
    let dead_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_addr = dead_listener.local_addr().unwrap();
    drop(dead_listener);
    let dead_url = format!("http://{dead_addr}");

    let local_dir = TempDir::new().unwrap();

    // Short timeout so the test doesn't wait long on unreachable.
    let mut remotes = BTreeMap::new();
    remotes.insert(
        "hub".to_owned(),
        ResolvedRemote {
            name: "hub".to_owned(),
            url: dead_url.clone(),
            token: Some(TEST_TOKEN.to_owned()),
            timeout: Duration::from_millis(500),
        },
    );

    let mut wing_rules = BTreeMap::new();
    wing_rules.insert(
        "wing_degraded".to_owned(),
        ResolvedRouteRule {
            mode: RouteMode::Combined,
            remote: Some("hub".to_owned()),
            write: WriteTarget::Local, // writes go local since remote is down
        },
    );

    let kg_rule = Some(ResolvedRouteRule {
        mode: RouteMode::Combined,
        remote: Some("hub".to_owned()),
        write: WriteTarget::Local,
    });

    let federation = FederationRuntimeConfig {
        remotes,
        default_mode: RouteMode::Local,
        default_remote: None,
        wings: wing_rules,
        kg: kg_rule,
        coordination: BTreeMap::new(),
    };

    let config = MempalaceConfig {
        schema_version: 1,
        collection_name: "mempalace_drawers".to_owned(),
        palace_path: local_dir.path().join("palace"),
        embedding_profile: EmbeddingProfile::Balanced,
        low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
        server: ServerRuntimeConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            token_file: local_dir.path().join("server_tokens.json"),
            checkouts: std::collections::BTreeMap::new(),
        },
        maintenance: MaintenanceRuntimeConfig::defaults(),
        federation,
    };

    let server =
        McpServer::from_parts(config, DeterministicStubProvider::new(EmbeddingProfile::Balanced))
            .await
            .unwrap();

    // Seed local data.
    let local_add = call_tool(
        &server,
        1,
        "mempalace_add_drawer",
        json!({
            "wing": "wing_degraded",
            "room": "local",
            "content": "degraded remote test local content survives",
            "added_by": "degrade-test"
        }),
    )
    .await;
    assert_eq!(
        local_add["success"], true,
        "local add with dead remote must still succeed: {local_add}"
    );
    assert_eq!(
        local_add["applied_to"], "local",
        "local add must report applied_to=local: {local_add}"
    );

    // Seed a local KG fact.
    let kg_add = call_tool(
        &server,
        2,
        "mempalace_kg_add",
        json!({"subject": "DegradeTest", "predicate": "has_state", "object": "local"}),
    )
    .await;
    assert_eq!(kg_add["success"], true, "kg_add must succeed locally: {kg_add}");
    assert_eq!(kg_add["applied_to"], "local", "kg_add must report applied_to=local: {kg_add}");

    // Combined-mode search with dead remote → local results + warnings.
    let search = call_tool(
        &server,
        3,
        "mempalace_search",
        json!({"query": "degraded remote test local content", "wing": "wing_degraded", "limit": 5}),
    )
    .await;

    // Must still have results (from local side).
    let results = search["results"].as_array().expect("results must be array");
    assert!(
        !results.is_empty(),
        "search must return local results even with dead remote: {search}"
    );
    // Must have warnings array mentioning the remote.
    let warnings = search.get("warnings").and_then(|w| w.as_array());
    assert!(
        warnings.is_some() && !warnings.unwrap().is_empty(),
        "search with dead remote must include warnings: {search}"
    );
    // Must ALSO carry structured degradation records (issue #127) alongside the legacy
    // string warnings — machine-actionable, not bare prose.
    let degradations = search.get("degradations").and_then(|d| d.as_array());
    assert!(
        degradations.is_some() && !degradations.unwrap().is_empty(),
        "search with dead remote must include structured degradations: {search}"
    );
    let first_degradation = &degradations.unwrap()[0];
    assert_eq!(first_degradation["code"], "remote_read_degraded");
    assert_eq!(first_degradation["remote"], "hub");
    assert_eq!(first_degradation["kind"], "search");
    assert_eq!(first_degradation["classification"], "unreachable");
    assert!(!first_degradation["error"].as_str().unwrap().is_empty());

    // mempalace_status → federation.remotes[].reachable must be false.
    let status = call_tool(&server, 4, "mempalace_status", json!({})).await;
    let remotes_info =
        status["federation"]["remotes"].as_array().expect("status must include federation.remotes");
    assert!(!remotes_info.is_empty(), "status must list at least one remote: {status}");
    let hub_entry = remotes_info.iter().find(|r| r["name"].as_str() == Some("hub"));
    assert!(hub_entry.is_some(), "status must list hub remote: {remotes_info:?}");
    assert_eq!(
        hub_entry.unwrap()["reachable"],
        false,
        "hub must be reported unreachable: {remotes_info:?}"
    );

    // mempalace_kg_query with dead remote → local facts + warnings.
    let kg_query = call_tool(
        &server,
        5,
        "mempalace_kg_query",
        json!({"entity": "DegradeTest", "direction": "outgoing"}),
    )
    .await;

    // Local facts must be present.
    let count = kg_query["count"].as_u64().unwrap_or(0);
    assert!(count >= 1, "kg_query must return local facts even with dead remote: {kg_query}");

    // Warnings about the dead remote must be present.
    let kg_warnings = kg_query.get("warnings").and_then(|w| w.as_array());
    assert!(
        kg_warnings.is_some() && !kg_warnings.unwrap().is_empty(),
        "kg_query with dead remote must include warnings: {kg_query}"
    );
    // Structured degradations accompany the legacy warnings.
    let kg_degradations = kg_query.get("degradations").and_then(|d| d.as_array());
    assert!(
        kg_degradations.is_some() && !kg_degradations.unwrap().is_empty(),
        "kg_query with dead remote must include structured degradations: {kg_query}"
    );
    assert_eq!(kg_degradations.unwrap()[0]["kind"], "kg_query");
    assert_eq!(kg_degradations.unwrap()[0]["classification"], "unreachable");

    // ── Local delete_drawer must report applied_to=local ─────────────────────
    let local_delete = call_tool(
        &server,
        6,
        "mempalace_delete_drawer",
        json!({"drawer_id": local_add["drawer_id"]}),
    )
    .await;
    assert_eq!(local_delete["success"], true, "local delete must succeed: {local_delete}");
    assert_eq!(
        local_delete["applied_to"], "local",
        "local delete must report applied_to=local: {local_delete}"
    );
}

// ─── Test 5: diary_room_never_routes_remote ───────────────────────────────────

/// Even when a wing is ruled Remote, adding a drawer with room "diary" must
/// execute LOCALLY (diary hard-override). The response has success=true and does
/// NOT include an "origin" field (i.e. it matches the local success shape).
#[tokio::test]
async fn diary_room_never_routes_remote() {
    let hub_dir = TempDir::new().unwrap();
    let local_dir = TempDir::new().unwrap();

    let addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{addr}");

    // Route "wing_diary_test" Remote — but diary-room add must still go local.
    let mut wing_rules = BTreeMap::new();
    wing_rules.insert("wing_diary_test".to_owned(), remote_wing_rule());

    let server =
        mcp_server_with_hub(&local_dir, &hub_url, wing_rules, RouteMode::Local, None).await;

    // Attempt to add a drawer with room "diary" into the remoted wing.
    // The diary hard-override must redirect this to local storage.
    let add_diary = call_tool(
        &server,
        1,
        "mempalace_add_drawer",
        json!({
            "wing": "wing_diary_test",
            "room": "diary",
            "content": "diary hard override test entry content unique xyzzy plugh",
            "added_by": "diary-test"
        }),
    )
    .await;

    // The add must succeed (locally).
    assert_eq!(add_diary["success"], true, "diary add must succeed locally; got: {add_diary}");

    // Local adds do not include "origin" field — no "hub" origin.
    let origin = add_diary.get("origin");
    assert!(
        origin.is_none() || origin.unwrap().is_null() || origin.unwrap().as_str() != Some("hub"),
        "diary add must NOT route to hub; got origin: {origin:?}; full response: {add_diary}"
    );

    // ── 2. Verify the drawer exists locally via taxonomy ─────────────────────
    // `mempalace_add_drawer` with room=diary uses ingest_mode="mcp_write", not
    // "diary", so `mempalace_diary_read` won't find it. Instead we confirm the
    // draw landed in the LOCAL palace by querying `mempalace_get_taxonomy`, which
    // lists all local wings/rooms regardless of ingest_mode.
    //
    // Because wing_diary_test has a Remote rule, taxonomy_merge will also fan out
    // to the hub. The hub should NOT have the drawer (diary hard-override prevented
    // it from being sent there). The local taxonomy should show wing_diary_test.
    //
    // NOTE: The wing_diary_test route is Remote (not Combined), so the fan-out
    // in plan_search_targets would be (false, ["hub"]) and no local results are
    // included. For taxonomy, federation.taxonomy_merge fans out to ALL remotes
    // and merges. Since the hub has no drawers in wing_diary_test, the merged
    // taxonomy should still show at least the local entry.
    //
    // We use a simpler check: verify that mempalace_list_wings includes
    // wing_diary_test in the merged result (hub contributes 0, local contributes 1).
    let list_wings = call_tool(&server, 2, "mempalace_list_wings", json!({})).await;
    let wings = list_wings["wings"].as_object().expect("wings must be object");
    assert!(
        wings.contains_key("wing_diary_test"),
        "list_wings must include wing_diary_test (the locally-stored diary draw); got: {list_wings}"
    );
    let local_count = wings["wing_diary_test"].as_u64().unwrap_or(0);
    assert!(
        local_count >= 1,
        "wing_diary_test must have at least 1 drawer (local diary add); got: {list_wings}"
    );
}

// ─── Test 6: wing_availability_reflects_rules ────────────────────────────────

/// With default_mode=Local and one wing ruled Remote at "hub", `mempalace_status`
/// must report wing_availability with:
/// - the ruled wing → "remote:hub"
/// - a local-only wing → "local" or "combined"
#[tokio::test]
async fn wing_availability_reflects_rules() {
    let hub_dir = TempDir::new().unwrap();
    let local_dir = TempDir::new().unwrap();

    let addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{addr}");

    let mut wing_rules = BTreeMap::new();
    wing_rules.insert("wing_remote_ruled".to_owned(), remote_wing_rule());

    let server =
        mcp_server_with_hub(&local_dir, &hub_url, wing_rules, RouteMode::Local, None).await;

    // Add a local drawer so wing_local_only appears in the palace.
    let local_add = call_tool(
        &server,
        1,
        "mempalace_add_drawer",
        json!({
            "wing": "wing_local_only",
            "room": "notes",
            "content": "wing availability test local only wing content",
            "added_by": "avail-test"
        }),
    )
    .await;
    assert_eq!(local_add["success"], true, "local add must succeed: {local_add}");

    // mempalace_status must include wing_availability.
    let status = call_tool(&server, 2, "mempalace_status", json!({})).await;

    let wing_availability = status
        .get("wing_availability")
        .expect("status with federation must include wing_availability");

    // The ruled-Remote wing must show "remote:hub".
    let ruled_avail = wing_availability.get("wing_remote_ruled");
    assert!(
        ruled_avail.is_some(),
        "wing_availability must include wing_remote_ruled; got: {wing_availability}"
    );
    assert_eq!(
        ruled_avail.unwrap().as_str(),
        Some("remote:hub"),
        "wing_remote_ruled must show remote:hub; got: {ruled_avail:?}"
    );

    // The local wing must show "local" (no rule → default_mode Local).
    let local_avail = wing_availability.get("wing_local_only");
    assert!(
        local_avail.is_some(),
        "wing_availability must include wing_local_only; got: {wing_availability}"
    );
    assert_eq!(
        local_avail.unwrap().as_str(),
        Some("local"),
        "wing_local_only must show local; got: {local_avail:?}"
    );

    // Also verify via mempalace_list_wings (which also returns wing_availability).
    let list_wings = call_tool(&server, 3, "mempalace_list_wings", json!({})).await;
    let wings_avail = list_wings
        .get("wing_availability")
        .expect("list_wings with federation must include wing_availability");
    assert_eq!(
        wings_avail.get("wing_remote_ruled").and_then(|v| v.as_str()),
        Some("remote:hub"),
        "list_wings wing_availability must reflect remote rule; got: {wings_avail}"
    );
}

// ─── Issue #19 tests: federated wake-up and changes feed ─────────────────────

// ─── Test 7: wake_up_includes_remote_changes_from_hub ────────────────────────

/// Seed 2 drawers on the hub via routed adds, then call `mempalace_wake_up`.
/// The response must include `remote_changes.hub.events` with both seeded
/// entity ids, every event tagged `origin == "remote:hub"`, and the standard
/// local sections (identity, status, diary) still present.
#[tokio::test]
async fn wake_up_includes_remote_changes_from_hub() {
    let hub_dir = TempDir::new().unwrap();
    let local_dir = TempDir::new().unwrap();

    let addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{addr}");

    // Route "wing_wake" as Combined/write=Remote so adds land on the hub.
    let mut wing_rules = BTreeMap::new();
    wing_rules.insert("wing_wake".to_owned(), combined_wing_rule_remote_write());

    let server =
        mcp_server_with_hub(&local_dir, &hub_url, wing_rules, RouteMode::Local, None).await;

    // Seed two drawers on the hub. Use distinct embedding clusters so the stub
    // provider gives each drawer a unique vector and duplicate detection does
    // not reject the second add.
    // Cluster 1: "auth migration parity" → [1,0,0,0]
    // Cluster 2: "rust cli tooling"      → [0,0,1,0]
    let add1 = call_tool(
        &server,
        1,
        "mempalace_add_drawer",
        json!({
            "wing": "wing_wake",
            "room": "general",
            "content": "wake up test hub auth migration parity cluster one",
            "added_by": "wake-test"
        }),
    )
    .await;
    assert_eq!(add1["success"], true, "hub add 1 must succeed: {add1}");
    assert_eq!(add1["origin"], "hub", "add1 must go to hub: {add1}");
    assert_eq!(add1["applied_to"], "remote:hub", "add1 must report applied_to=remote:hub: {add1}");
    let entity_id1 = add1["drawer_id"].as_str().unwrap().to_owned();

    let add2 = call_tool(
        &server,
        2,
        "mempalace_add_drawer",
        json!({
            "wing": "wing_wake",
            "room": "general",
            "content": "wake up test hub rust cli tooling cluster two",
            "added_by": "wake-test"
        }),
    )
    .await;
    assert_eq!(add2["success"], true, "hub add 2 must succeed: {add2}");
    assert_eq!(add2["origin"], "hub", "add2 must go to hub: {add2}");
    assert_eq!(add2["applied_to"], "remote:hub", "add2 must report applied_to=remote:hub: {add2}");
    let entity_id2 = add2["drawer_id"].as_str().unwrap().to_owned();

    // Call mempalace_wake_up.
    let wake = call_tool(
        &server,
        3,
        "mempalace_wake_up",
        json!({"agent_name": "wake-test", "latest_limit": 25}),
    )
    .await;

    // Standard local sections must still be present.
    assert!(wake.get("identity").is_some(), "wake_up must include identity: {wake}");
    assert!(wake.get("status").is_some(), "wake_up must include status: {wake}");
    assert!(wake.get("diary").is_some(), "wake_up must include diary: {wake}");

    // remote_changes must exist and include a "hub" key.
    let remote_changes =
        wake.get("remote_changes").expect("wake_up with federation must include remote_changes");
    let hub_changes = remote_changes.get("hub").expect("remote_changes must include 'hub' entry");

    // Must not be an unreachable marker.
    assert!(
        hub_changes.get("unreachable").is_none() || hub_changes["unreachable"] == false,
        "hub must be reachable in wake_up: {hub_changes}"
    );

    let events =
        hub_changes["events"].as_array().expect("hub remote_changes must have events array");
    assert!(!events.is_empty(), "hub remote_changes.events must be non-empty: {hub_changes}");

    // Every event must carry origin == "remote:hub".
    for event in events {
        assert_eq!(
            event["origin"].as_str(),
            Some("remote:hub"),
            "every hub remote_change event must have origin=remote:hub; event: {event}"
        );
    }

    // Both seeded entity ids must appear.
    let entity_ids: Vec<&str> = events.iter().filter_map(|e| e["entity_id"].as_str()).collect();
    assert!(
        entity_ids.contains(&entity_id1.as_str()),
        "wake_up hub events must include entity_id {entity_id1}; ids: {entity_ids:?}"
    );
    assert!(
        entity_ids.contains(&entity_id2.as_str()),
        "wake_up hub events must include entity_id {entity_id2}; ids: {entity_ids:?}"
    );
}

// ─── Test 8: wake_up_with_down_remote_marks_unreachable_and_succeeds ─────────

/// Point the hub config at a dropped-listener address (down remote).
/// `mempalace_wake_up` must still succeed (identity present) and
/// `remote_changes.hub` must have `unreachable == true` with a non-empty error.
#[tokio::test]
async fn wake_up_with_down_remote_marks_unreachable_and_succeeds() {
    // Bind a port to get a free address, then drop so nothing listens.
    let dead_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_addr = dead_listener.local_addr().unwrap();
    drop(dead_listener);
    let dead_url = format!("http://{dead_addr}");

    let local_dir = TempDir::new().unwrap();

    let mut remotes = BTreeMap::new();
    remotes.insert(
        "hub".to_owned(),
        ResolvedRemote {
            name: "hub".to_owned(),
            url: dead_url.clone(),
            token: Some(TEST_TOKEN.to_owned()),
            // Short timeout so the test does not block.
            timeout: Duration::from_millis(500),
        },
    );

    let federation = FederationRuntimeConfig {
        remotes,
        default_mode: RouteMode::Local,
        default_remote: None,
        wings: BTreeMap::new(),
        kg: None,
        coordination: BTreeMap::new(),
    };

    let config = MempalaceConfig {
        schema_version: 1,
        collection_name: "mempalace_drawers".to_owned(),
        palace_path: local_dir.path().join("palace"),
        embedding_profile: EmbeddingProfile::Balanced,
        low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
        server: ServerRuntimeConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            token_file: local_dir.path().join("server_tokens.json"),
            checkouts: std::collections::BTreeMap::new(),
        },
        maintenance: MaintenanceRuntimeConfig::defaults(),
        federation,
    };

    let server =
        McpServer::from_parts(config, DeterministicStubProvider::new(EmbeddingProfile::Balanced))
            .await
            .unwrap();

    // wake_up must succeed despite the dead remote.
    let wake = call_tool(&server, 1, "mempalace_wake_up", json!({"agent_name": "down-test"})).await;

    // Standard sections must still be present (graceful degradation).
    assert!(
        wake.get("identity").is_some(),
        "wake_up must include identity even with dead remote: {wake}"
    );
    assert!(
        wake.get("status").is_some(),
        "wake_up must include status even with dead remote: {wake}"
    );

    // remote_changes must exist with an unreachable marker for "hub".
    let remote_changes = wake
        .get("remote_changes")
        .expect("wake_up with federation must include remote_changes even when remote is down");
    let hub_changes = remote_changes.get("hub").expect("remote_changes must include 'hub' entry");

    assert_eq!(
        hub_changes["unreachable"], true,
        "hub must be marked unreachable when down: {hub_changes}"
    );
    let error_str = hub_changes["error"].as_str().unwrap_or("");
    assert!(
        !error_str.is_empty(),
        "hub unreachable marker must include non-empty error: {hub_changes}"
    );
}

// ─── Test 9: get_changes_since_cursor_continuation_across_two_pages ──────────

/// Seed 3 drawers on the hub via routed adds. Call `mempalace_get_changes_since`
/// with `limit: 2`. Assert exactly 2 remote-origin events and a non-null
/// `remotes.hub.next_cursor`. Then call again with that cursor and assert the
/// remaining event comes back, no entity_id overlap between pages, and
/// `remotes.hub.next_cursor` is null on the final page.
/// Also verify ascending `occurred_at` order within each response.
#[tokio::test]
async fn get_changes_since_cursor_continuation_across_two_pages() {
    let hub_dir = TempDir::new().unwrap();
    let local_dir = TempDir::new().unwrap();

    let addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{addr}");

    let mut wing_rules = BTreeMap::new();
    wing_rules.insert("wing_pages".to_owned(), combined_wing_rule_remote_write());

    let server =
        mcp_server_with_hub(&local_dir, &hub_url, wing_rules, RouteMode::Local, None).await;

    // Seed 3 drawers on the hub using distinct embedding clusters so duplicate
    // detection does not reject any add.
    // Cluster 1: "auth migration parity" → [1,0,0,0]
    // Cluster 2: "rust cli tooling"      → [0,0,1,0]
    // Cluster 3: (no keyword match)      → [0,0,0,1]  — different content hash prevents dup
    let contents = [
        "cursor pagination test auth migration parity hub event one",
        "cursor pagination test rust cli tooling hub event two",
        "cursor pagination test hub event three qwerty unique zxcvb",
    ];
    let mut hub_entity_ids = Vec::new();
    for (i, content) in contents.iter().enumerate() {
        let add = call_tool(
            &server,
            i as u64 + 1,
            "mempalace_add_drawer",
            json!({
                "wing": "wing_pages",
                "room": "pagination",
                "content": content,
                "added_by": "page-test"
            }),
        )
        .await;
        assert_eq!(add["success"], true, "hub add {i} must succeed: {add}");
        assert_eq!(add["origin"], "hub", "add {i} must go to hub: {add}");
        hub_entity_ids.push(add["drawer_id"].as_str().unwrap().to_owned());
    }

    // First page: limit=2, no cursor, since=epoch.
    let page1 = call_tool(
        &server,
        10,
        "mempalace_get_changes_since",
        json!({"since": "2000-01-01T00:00:00Z", "limit": 2}),
    )
    .await;

    let page1_events = page1["events"].as_array().expect("events must be array");
    // Count only remote-origin events (local side has 0 drawers in wing_pages,
    // but may have 0 local changes since we only added via remote route).
    let page1_remote: Vec<&Value> = page1_events
        .iter()
        .filter(|e| e["origin"].as_str().map_or(false, |o| o.starts_with("remote:")))
        .collect();
    assert_eq!(
        page1_remote.len(),
        2,
        "first page must return exactly 2 remote-origin events; events: {page1_events:?}"
    );

    // Verify ascending occurred_at order on page 1.
    for pair in page1_remote.windows(2) {
        let t0 = pair[0]["occurred_at"].as_str().unwrap_or("");
        let t1 = pair[1]["occurred_at"].as_str().unwrap_or("");
        assert!(t0 <= t1, "events must be ascending by occurred_at; got {t0} then {t1}");
    }

    // remotes.hub.next_cursor must be a non-null string.
    let remotes_meta = page1.get("remotes").expect("page1 must include remotes meta");
    let hub_cursor1 = remotes_meta["hub"]["next_cursor"]
        .as_str()
        .expect("remotes.hub.next_cursor must be a string on page 1");

    let page1_entity_ids: Vec<&str> =
        page1_remote.iter().filter_map(|e| e["entity_id"].as_str()).collect();

    // Second page: pass the cursor for hub.
    let page2 = call_tool(
        &server,
        11,
        "mempalace_get_changes_since",
        json!({
            "since": "2000-01-01T00:00:00Z",
            "limit": 2,
            "cursors": {"hub": hub_cursor1}
        }),
    )
    .await;

    let page2_events = page2["events"].as_array().expect("events must be array");
    let page2_remote: Vec<&Value> = page2_events
        .iter()
        .filter(|e| e["origin"].as_str().map_or(false, |o| o.starts_with("remote:")))
        .collect();
    assert!(
        !page2_remote.is_empty(),
        "second page must return at least 1 remote-origin event; events: {page2_events:?}"
    );

    // No entity_id overlap between pages.
    let page2_entity_ids: Vec<&str> =
        page2_remote.iter().filter_map(|e| e["entity_id"].as_str()).collect();
    for id in &page2_entity_ids {
        assert!(
            !page1_entity_ids.contains(id),
            "entity_id {id} appeared on both page 1 and page 2 — overlap not allowed"
        );
    }

    // All 3 hub entity ids must appear across the two pages combined.
    let all_ids: Vec<&str> =
        page1_entity_ids.iter().chain(page2_entity_ids.iter()).copied().collect();
    for expected_id in &hub_entity_ids {
        assert!(
            all_ids.contains(&expected_id.as_str()),
            "entity_id {expected_id} must appear across the two pages; got: {all_ids:?}"
        );
    }

    // Final page cursor must be null (no more data).
    let page2_remotes = page2.get("remotes").expect("page2 must include remotes meta");
    assert!(
        page2_remotes["hub"]["next_cursor"].is_null(),
        "remotes.hub.next_cursor must be null on the last page; got: {page2_remotes}"
    );
}

// ─── Test 10: diary_events_never_appear_in_remote_changes ────────────────────

/// Get a diary event into the hub palace (via an in-process McpServer before
/// spawning the HTTP server) plus one normal drawer event on the hub.
/// Federated `mempalace_get_changes_since` must show the drawer event with
/// `origin == "remote:hub"` and NO event with `event_type == "diary_written"`
/// from a remote origin.
/// Locally-written diary entries must still appear with `origin == "local"`.
#[tokio::test]
async fn diary_events_never_appear_in_remote_changes() {
    let hub_dir = TempDir::new().unwrap();
    let local_dir = TempDir::new().unwrap();

    // ── Step 1: seed a diary entry directly into the hub palace BEFORE spawning
    // the HTTP server (to avoid concurrent engine access on the same dir).
    {
        let hub_mcp = McpServer::from_parts(
            MempalaceConfig {
                schema_version: 1,
                collection_name: "mempalace_drawers".to_owned(),
                palace_path: hub_dir.path().join("palace"),
                embedding_profile: EmbeddingProfile::Balanced,
                low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
                server: ServerRuntimeConfig {
                    bind: "127.0.0.1:0".parse().unwrap(),
                    token_file: hub_dir.path().join("server_tokens.json"),
                    checkouts: std::collections::BTreeMap::new(),
                },
                maintenance: MaintenanceRuntimeConfig::defaults(),
                federation: FederationRuntimeConfig::default(),
            },
            DeterministicStubProvider::new(EmbeddingProfile::Balanced),
        )
        .await
        .unwrap();

        let diary_resp = call_tool(
            &hub_mcp,
            1,
            "mempalace_diary_write",
            json!({
                "agent_name": "hub-agent",
                "entry": "hub diary entry — must not appear in federated changes feed",
                "summary": "Hub diary entry.",
                "topic": "diary-filter-test"
            }),
        )
        .await;
        assert_eq!(diary_resp["success"], true, "hub diary_write must succeed: {diary_resp}");
        // hub_mcp is dropped here — the palace engine is released before HTTP spawn.
    }

    // ── Step 2: spawn the HTTP server over the hub dir (now free of in-process engine).
    let addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{addr}");

    // ── Step 3: set up the federated MCP server.
    let mut wing_rules = BTreeMap::new();
    wing_rules.insert("wing_diary_filter".to_owned(), combined_wing_rule_remote_write());

    let server =
        mcp_server_with_hub(&local_dir, &hub_url, wing_rules, RouteMode::Local, None).await;

    // ── Step 4: add a normal drawer on the hub to ensure at least one drawer event.
    // Use "auth migration parity" cluster so the stub provider gives a distinct vector.
    let hub_add = call_tool(
        &server,
        2,
        "mempalace_add_drawer",
        json!({
            "wing": "wing_diary_filter",
            "room": "general",
            "content": "diary filter test hub drawer auth migration parity unique",
            "added_by": "diary-filter-test"
        }),
    )
    .await;
    assert_eq!(hub_add["success"], true, "hub drawer add must succeed: {hub_add}");
    assert_eq!(hub_add["origin"], "hub", "hub drawer must go to hub: {hub_add}");
    assert_eq!(
        hub_add["applied_to"], "remote:hub",
        "hub drawer must report applied_to=remote:hub: {hub_add}"
    );
    let hub_drawer_id = hub_add["drawer_id"].as_str().unwrap().to_owned();

    // ── Step 5: write a diary entry LOCALLY on the local MCP server.
    let local_diary = call_tool(
        &server,
        3,
        "mempalace_diary_write",
        json!({
            "agent_name": "local-agent",
            "entry": "local diary entry — must appear with origin=local",
            "summary": "Local diary entry.",
            "topic": "local-diary-test"
        }),
    )
    .await;
    assert_eq!(local_diary["success"], true, "local diary_write must succeed: {local_diary}");

    // ── Step 6: call federated get_changes_since.
    let changes = call_tool(
        &server,
        4,
        "mempalace_get_changes_since",
        json!({"since": "2000-01-01T00:00:00Z"}),
    )
    .await;

    let events = changes["events"].as_array().expect("events must be array");

    // The hub drawer event must appear with origin remote:hub.
    let hub_drawer_event = events.iter().find(|e| e["entity_id"].as_str() == Some(&hub_drawer_id));
    assert!(
        hub_drawer_event.is_some(),
        "hub drawer event must appear in changes; entity_id={hub_drawer_id}; events: {events:?}"
    );
    assert_eq!(
        hub_drawer_event.unwrap()["origin"].as_str(),
        Some("remote:hub"),
        "hub drawer event must have origin=remote:hub"
    );

    // No event with event_type == "diary_written" must appear with a remote origin.
    let remote_diary_events: Vec<&Value> = events
        .iter()
        .filter(|e| {
            e["event_type"].as_str() == Some("diary_written")
                && e["origin"].as_str().map_or(false, |o| o.starts_with("remote:"))
        })
        .collect();
    assert!(
        remote_diary_events.is_empty(),
        "no diary_written event must appear with remote origin; found: {remote_diary_events:?}"
    );

    // The local diary_written event must appear with origin == "local".
    let local_diary_event = events.iter().find(|e| {
        e["event_type"].as_str() == Some("diary_written") && e["origin"].as_str() == Some("local")
    });
    assert!(
        local_diary_event.is_some(),
        "local diary_written event must appear with origin=local; events: {events:?}"
    );
}

// ─── Test 11: get_changes_since_includes_local_and_remote_origins ─────────────

/// One local add + one routed remote add → both origins present in one merged
/// response, and `remotes.hub.count` equals the number of hub-origin events.
#[tokio::test]
async fn get_changes_since_includes_local_and_remote_origins() {
    let hub_dir = TempDir::new().unwrap();
    let local_dir = TempDir::new().unwrap();

    let addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{addr}");

    let mut wing_rules = BTreeMap::new();
    wing_rules.insert("wing_mixed".to_owned(), combined_wing_rule_remote_write());

    let server =
        mcp_server_with_hub(&local_dir, &hub_url, wing_rules, RouteMode::Local, None).await;

    // Add a drawer to the hub via routed write (wing_mixed → write=Remote).
    // "auth migration parity" cluster → [1,0,0,0] vector.
    let hub_add = call_tool(
        &server,
        1,
        "mempalace_add_drawer",
        json!({
            "wing": "wing_mixed",
            "room": "general",
            "content": "mixed origins test hub drawer auth migration parity unique",
            "added_by": "mixed-test"
        }),
    )
    .await;
    assert_eq!(hub_add["success"], true, "hub add must succeed: {hub_add}");
    assert_eq!(hub_add["origin"], "hub", "hub add must go to hub: {hub_add}");
    assert_eq!(
        hub_add["applied_to"], "remote:hub",
        "hub add must report applied_to=remote:hub: {hub_add}"
    );

    // Add a drawer locally (wing without a remote rule → default Local).
    // "rust cli tooling" cluster → [0,0,1,0] vector.
    let local_add = call_tool(
        &server,
        2,
        "mempalace_add_drawer",
        json!({
            "wing": "wing_local_only_mixed",
            "room": "notes",
            "content": "mixed origins test local drawer rust cli tooling unique",
            "added_by": "mixed-test"
        }),
    )
    .await;
    assert_eq!(local_add["success"], true, "local add must succeed: {local_add}");

    // get_changes_since should merge both origins.
    let changes = call_tool(
        &server,
        3,
        "mempalace_get_changes_since",
        json!({"since": "2000-01-01T00:00:00Z"}),
    )
    .await;

    let events = changes["events"].as_array().expect("events must be array");

    let has_local = events.iter().any(|e| e["origin"].as_str() == Some("local"));
    let has_remote_hub = events.iter().any(|e| e["origin"].as_str() == Some("remote:hub"));

    assert!(has_local, "changes must include at least one local-origin event; events: {events:?}");
    assert!(
        has_remote_hub,
        "changes must include at least one remote:hub-origin event; events: {events:?}"
    );

    // remotes meta: hub count must equal the number of hub-origin events in the list.
    let remotes_meta = changes.get("remotes").expect("federated changes must include remotes meta");
    let hub_count =
        remotes_meta["hub"]["count"].as_u64().expect("remotes.hub.count must be a number");
    let actual_hub_count =
        events.iter().filter(|e| e["origin"].as_str() == Some("remote:hub")).count() as u64;
    assert_eq!(
        hub_count, actual_hub_count,
        "remotes.hub.count must equal number of remote:hub events in events array"
    );

    // Total count field must equal total events length.
    let total_count = changes["count"].as_u64().expect("count must be a number");
    assert_eq!(total_count, events.len() as u64, "top-level count must equal events.len()");
}

// ─── Dual-write (write:both) tests ─────────────────────────────────────────

// ─── Test 12: add_drawer_both_replicates_successfully ───────────────────────

/// Combined/write:Both wing rule → local write first, then **durable queued** remote
/// replication. The immediate response must show `applied_to: "local"` and a `replication`
/// object with `status: "queued"` plus a stable `operation_id` — the tool never blocks on the
/// remote (issue #127: returns before delivery, worker delivers in the background). The test
/// then awaits the worker's async delivery by polling the hub directly until the drawer
/// appears, and asserts the operation is not recorded as a terminal failure.
#[tokio::test]
async fn add_drawer_both_replicates_successfully() {
    let hub_dir = TempDir::new().unwrap();
    let local_dir = TempDir::new().unwrap();

    let addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{addr}");

    let mut wing_rules = BTreeMap::new();
    wing_rules.insert("wing_both".to_owned(), combined_wing_rule_both_write());

    let server =
        mcp_server_with_hub(&local_dir, &hub_url, wing_rules, RouteMode::Local, None).await;

    // ── Add via Both route ──────────────────────────────────────────────────
    let add = call_tool(
        &server,
        1,
        "mempalace_add_drawer",
        json!({
            "wing": "wing_both",
            "room": "both-room",
            "content": "dual-write e2e test drawer successful replication",
            "added_by": "both-test"
        }),
    )
    .await;

    assert_eq!(add["success"], true, "both add must succeed: {add}");
    assert_eq!(
        add["applied_to"], "local",
        "both add must report applied_to=local (primary write is local); got: {add}"
    );
    // Primary write is local, so no "origin" field.
    assert!(
        add.get("origin").is_none() || add["origin"].is_null(),
        "both add must not have an origin field; got: {add}"
    );
    // Replication is durably queued, not synchronously replicated — issue #127 semantics.
    let replication = add.get("replication").expect("both add must include replication field");
    assert_eq!(
        replication["status"], "queued",
        "replication must report status=queued (async delivery); got: {replication}"
    );
    assert_eq!(
        replication["remote"], "hub",
        "replication must report remote=hub; got: {replication}"
    );
    let operation_id =
        replication["operation_id"].as_str().expect("queued must carry operation_id").to_owned();
    assert!(
        operation_id.starts_with("outbox_"),
        "queued replication must expose a stable outbox operation id; got: {replication}"
    );
    // No warnings on success.
    assert!(
        add.get("warnings").is_none(),
        "both add must not have warnings on success; got: {add}"
    );
    let _drawer_id = add["drawer_id"].as_str().unwrap().to_owned();

    // ── Verify the background worker eventually delivers to the hub ──────────
    // Combined search deduplicates the local and replicated copies and prefers
    // local, so it cannot prove the remote write occurred — poll the hub directly.
    wait_for_hub_drawer(
        &hub_url,
        "dual-write e2e test drawer successful replication",
        "wing_both",
        "both-room",
    )
    .await;

    // The delivered operation must NOT be recorded as a terminal failure.
    wait_for_status(&server, "operation to be acknowledged (not terminally failed)", |status| {
        !status_has_terminal_failure(status, &operation_id)
    })
    .await;
}

// ─── Test 22: add_drawer_both_replication_fails_with_remote_rejection ───────

/// Combined/write:Both wing rule with a reachable remote that rejects the replication attempt
/// (wrong bearer token → HTTP 401). The local write must still succeed immediately and return
/// `applied_to: "local"` with `replication.status: "queued"` — the tool never waits on the
/// remote. The background worker then delivers, observes the authoritative `Unauthorized`
/// rejection, and records the operation as a **terminal failure** observable via
/// `mempalace_status`. Both the queued immediate response and the terminal outbox record are
/// asserted.
#[tokio::test]
async fn add_drawer_both_replication_fails_with_remote_rejection() {
    let hub_dir = TempDir::new().unwrap();
    let local_dir = TempDir::new().unwrap();

    let addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{addr}");

    // Use a wrong token so the hub rejects the replication with HTTP 401.
    let bad_token = "wrong-token-xyz-does-not-match";

    let mut remotes = BTreeMap::new();
    remotes.insert(
        "hub".to_owned(),
        ResolvedRemote {
            name: "hub".to_owned(),
            url: hub_url,
            token: Some(bad_token.to_owned()),
            timeout: Duration::from_secs(5),
        },
    );

    let mut wing_rules = BTreeMap::new();
    wing_rules.insert("wing_both_reject".to_owned(), combined_wing_rule_both_write());

    let federation = FederationRuntimeConfig {
        remotes,
        default_mode: RouteMode::Local,
        default_remote: None,
        wings: wing_rules,
        kg: None,
        coordination: BTreeMap::new(),
    };

    let config = MempalaceConfig {
        schema_version: 1,
        collection_name: "mempalace_drawers".to_owned(),
        palace_path: local_dir.path().join("palace"),
        embedding_profile: EmbeddingProfile::Balanced,
        low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
        server: ServerRuntimeConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            token_file: local_dir.path().join("server_tokens.json"),
            checkouts: std::collections::BTreeMap::new(),
        },
        maintenance: MaintenanceRuntimeConfig::defaults(),
        federation,
    };

    let server =
        McpServer::from_parts(config, DeterministicStubProvider::new(EmbeddingProfile::Balanced))
            .await
            .unwrap();

    let add = call_tool(
        &server,
        1,
        "mempalace_add_drawer",
        json!({
            "wing": "wing_both_reject",
            "room": "reject-room",
            "content": "dual-write e2e remote rejection test drawer",
            "added_by": "reject-test"
        }),
    )
    .await;

    // Local write must succeed.
    assert_eq!(add["success"], true, "both add with wrong token must still succeed: {add}");
    assert_eq!(
        add["applied_to"], "local",
        "both add with wrong token must report applied_to=local: {add}"
    );

    // Replication is queued (not synchronously failed) — the tool returns before delivery.
    let replication = add.get("replication").expect("both add must include replication field");
    assert_eq!(
        replication["status"], "queued",
        "replication must report status=queued with wrong token; got: {replication}"
    );
    assert_eq!(
        replication["remote"], "hub",
        "replication must report remote=hub; got: {replication}"
    );
    let operation_id =
        replication["operation_id"].as_str().expect("queued must carry operation_id").to_owned();

    // No inline warnings — rejection surfaces asynchronously through the outbox.
    assert!(
        add.get("warnings").is_none(),
        "queued replication must not add inline warnings; got: {add}"
    );

    // The authoritative permanent rejection is recorded as a terminal failure.
    wait_for_status(&server, "operation to be recorded as a terminal failure", |status| {
        status_has_terminal_failure(status, &operation_id)
    })
    .await;
}

// ─── Test 23: add_drawer_both_duplicate_replication ─────────────────────────

/// The content already exists on the hub (seeded via a Remote route), with a **different**
/// drawer_id than the local write will commit. Issue #127 forbids treating a semantic/content
/// duplicate with a different remote drawer_id as replicated/converged success: logical
/// identity is the only replay key, and a divergent remote ID is an inspectable terminal
/// identity conflict, not convergence. The local Both write must still succeed immediately and
/// report `status: "queued"`; the background worker then delivers, the hub authoritatively
/// rejects with 409 (duplicate, different id), and the operation is recorded as a **terminal
/// failure** surfaced via `mempalace_status` — not marked converged.
#[tokio::test]
async fn add_drawer_both_duplicate_replication() {
    let hub_dir = TempDir::new().unwrap();
    let local_dir_a = TempDir::new().unwrap();
    let local_dir_b = TempDir::new().unwrap();

    let addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{addr}");

    let content = "dual-write e2e duplicate replication test content unique zyxw";

    // ── Server A: Remote write to seed content on the hub ───────────────────
    let mut wing_rules_a = BTreeMap::new();
    wing_rules_a.insert("wing_seed".to_owned(), combined_wing_rule_remote_write());

    let server_a =
        mcp_server_with_hub(&local_dir_a, &hub_url, wing_rules_a, RouteMode::Local, None).await;

    let seed = call_tool(
        &server_a,
        1,
        "mempalace_add_drawer",
        json!({
            "wing": "wing_seed",
            "room": "seed-room",
            "content": content,
            "added_by": "dup-test"
        }),
    )
    .await;
    assert_eq!(seed["success"], true, "seed add must succeed: {seed}");
    assert_eq!(seed["origin"], "hub", "seed must go to hub: {seed}");
    let hub_drawer_id = seed["drawer_id"].as_str().expect("seed must return a drawer_id");

    // ── Server B: Both write with the same content ──────────────────────────
    let mut wing_rules_b = BTreeMap::new();
    wing_rules_b.insert("wing_both_dup".to_owned(), combined_wing_rule_both_write());

    let server_b =
        mcp_server_with_hub(&local_dir_b, &hub_url, wing_rules_b, RouteMode::Local, None).await;

    let dup = call_tool(
        &server_b,
        1,
        "mempalace_add_drawer",
        json!({
            "wing": "wing_both_dup",
            "room": "dup-room",
            "content": content,
            "added_by": "dup-test"
        }),
    )
    .await;

    // Local write must succeed.
    assert_eq!(dup["success"], true, "both add with duplicate content must succeed locally: {dup}");
    assert_eq!(dup["applied_to"], "local", "both add must report applied_to=local: {dup}");

    // Replication is queued, NOT synchronously converged.
    let replication = dup.get("replication").expect("both add must include replication field");
    assert_eq!(
        replication["status"], "queued",
        "replication must report status=queued (delivery is async); got: {replication}"
    );
    assert_eq!(
        replication["remote"], "hub",
        "replication must report remote=hub; got: {replication}"
    );
    let operation_id =
        replication["operation_id"].as_str().expect("queued must carry operation_id").to_owned();

    // No warnings for a queued write (conflict surfaces asynchronously).
    assert!(
        dup.get("warnings").is_none(),
        "queued replication must not produce warnings; got: {dup}"
    );

    // ── The worker hits the authoritative 409 and records a TERMINAL CONFLICT, not convergence ──
    wait_for_status(
        &server_b,
        "duplicate-with-different-id to be recorded as a terminal failure",
        |status| status_has_terminal_failure(status, &operation_id),
    )
    .await;

    // The divergent drawer must NOT exist on the hub: the seeded id is still the only copy.
    let client = hub_client(&hub_url);
    let hub_search = client
        .search_drawers(DrawerSearchRequest {
            query: content.to_owned(),
            wing: None,
            room: None,
            limit: Some(20),
            view: None,
        })
        .await
        .expect("hub search must succeed");
    let hub_ids: Vec<&str> = hub_search
        .results
        .iter()
        .filter(|r| r.content == content)
        .map(|r| r.drawer_id.as_str())
        .collect();
    assert_eq!(
        hub_ids,
        vec![hub_drawer_id],
        "hub must contain exactly the seeded drawer for this content, not a divergent duplicate; got: {hub_ids:?}"
    );
}

// ─── Test 12b: add_drawer_both_near_duplicate_same_wing_room_rejected ──────────

/// Combined/write:Both mode. A near-duplicate (different exact content but
/// same semantic wing/room) must be rejected with `success: false,
/// reason: "duplicate"` and must NOT attempt remote replication.
///
/// Regression test: the retry-reuse predicate must check content_hash in
/// addition to wing+room, otherwise a near-duplicate is incorrectly
/// treated as an idempotent retry.
#[tokio::test]
async fn add_drawer_both_near_duplicate_same_wing_room_rejected() {
    let dead_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_addr = dead_listener.local_addr().unwrap();
    drop(dead_listener);
    let dead_url = format!("http://{dead_addr}");

    let local_dir = TempDir::new().unwrap();

    let mut remotes = BTreeMap::new();
    remotes.insert(
        "hub".to_owned(),
        ResolvedRemote {
            name: "hub".to_owned(),
            url: dead_url,
            token: Some(TEST_TOKEN.to_owned()),
            timeout: Duration::from_millis(500),
        },
    );

    let mut wing_rules = BTreeMap::new();
    wing_rules.insert("wing_both_near_dup".to_owned(), combined_wing_rule_both_write());

    let federation = FederationRuntimeConfig {
        remotes,
        default_mode: RouteMode::Local,
        default_remote: None,
        wings: wing_rules,
        kg: None,
        coordination: BTreeMap::new(),
    };

    let config = MempalaceConfig {
        schema_version: 1,
        collection_name: "mempalace_drawers".to_owned(),
        palace_path: local_dir.path().join("palace"),
        embedding_profile: EmbeddingProfile::Balanced,
        low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
        server: ServerRuntimeConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            token_file: local_dir.path().join("server_tokens.json"),
            checkouts: std::collections::BTreeMap::new(),
        },
        maintenance: MaintenanceRuntimeConfig::defaults(),
        federation,
    };

    let server =
        McpServer::from_parts(config, DeterministicStubProvider::new(EmbeddingProfile::Balanced))
            .await
            .unwrap();

    // ── First add: content A ────────────────────────────────────────────────
    let content_a = "near duplicate alpha content that is semantically similar";
    let first = call_tool(
        &server,
        1,
        "mempalace_add_drawer",
        json!({
            "wing": "wing_both_near_dup",
            "room": "near-dup-room",
            "content": content_a,
            "added_by": "near-dup-test"
        }),
    )
    .await;

    // Local write must succeed (remote is down, replication will fail — that's OK).
    assert_eq!(first["success"], true, "first add must succeed locally: {first}");

    // ── Second add: content B — near-duplicate in same wing/room ────────────
    // This content produces the same deterministic embedding vector as content A
    // (both fall into the "other" keyword category), so semantic search finds it.
    // But the exact text differs, so content_hash differs.
    let content_b = "near duplicate beta content that is semantically similar";
    let second = call_tool(
        &server,
        2,
        "mempalace_add_drawer",
        json!({
            "wing": "wing_both_near_dup",
            "room": "near-dup-room",
            "content": content_b,
            "added_by": "near-dup-test"
        }),
    )
    .await;

    // Near-duplicate must be rejected — different content_hash prevents retry reuse.
    assert_eq!(second["success"], false, "near-duplicate must be rejected: {second}");
    assert_eq!(
        second["reason"], "duplicate",
        "near-duplicate must report reason=duplicate; got: {second}"
    );

    // No replication must be attempted for a rejected duplicate.
    assert!(
        second.get("replication").is_none(),
        "rejected near-duplicate must not include a replication field; got: {second}"
    );

    // No warnings for a rejected duplicate.
    assert!(
        second.get("warnings").is_none(),
        "rejected near-duplicate must not include warnings; got: {second}"
    );
}

// ─── Test 26: add_drawer_both_retry_reuses_local_drawer_and_replicates ──────

/// Dual-write retry regression under issue #127 durable queued semantics:
///
/// 1. Start a real hub, then take it down.
/// 2. First write:both add while the hub is unavailable → local success, response shows
///    `replication.status: "queued"` (never a synchronous failure), and a stable
///    `operation_id` is captured. The in-process worker keeps the intent in the retryable
///    backlog rather than recording a terminal failure for a merely-unreachable remote.
/// 3. Restore the hub on the same endpoint.
/// 4. Retry the identical add → existing local drawer is reused (same `drawer_id`), and the
///    replication is staged with the **same stable operation_id** (logical replay identity),
///    returned again as `status: "queued"`. The worker then delivers to the restored hub and
///    the drawer appears there. No duplicate local drawer is created.
///
/// Retain coverage that same-wing/room near-duplicates with different content hashes remain
/// rejected (Test 12b above).
#[tokio::test]
async fn add_drawer_both_retry_reuses_local_drawer_and_replicates() {
    // ── Phase 0: Start hub, then take it down ───────────────────────────────
    let hub_dir = TempDir::new().unwrap();
    let (hub_addr, hub_handle) = spawn_server_with_handle(&hub_dir).await;
    let hub_url = format!("http://{hub_addr}");
    hub_handle.abort();

    let local_dir = TempDir::new().unwrap();
    let content = "dual-write retry regression test content unique xyzzy";
    let wing = "wing_retry";
    let room = "retry-room";
    let added_by = "retry-test";

    // ── Phase 1: Hub is down → the write commits locally and stays QUEUED ────
    let mut remotes_a = BTreeMap::new();
    remotes_a.insert(
        "hub".to_owned(),
        ResolvedRemote {
            name: "hub".to_owned(),
            url: hub_url.clone(),
            token: Some(TEST_TOKEN.to_owned()),
            timeout: Duration::from_millis(500),
        },
    );
    let mut wing_rules_a = BTreeMap::new();
    wing_rules_a.insert(wing.to_owned(), combined_wing_rule_both_write());
    let federation_a = FederationRuntimeConfig {
        remotes: remotes_a,
        default_mode: RouteMode::Local,
        default_remote: None,
        wings: wing_rules_a,
        kg: None,
        coordination: BTreeMap::new(),
    };
    let config_a = MempalaceConfig {
        schema_version: 1,
        collection_name: "mempalace_drawers".to_owned(),
        palace_path: local_dir.path().join("palace"),
        embedding_profile: EmbeddingProfile::Balanced,
        low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
        server: ServerRuntimeConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            token_file: local_dir.path().join("server_tokens.json"),
            checkouts: std::collections::BTreeMap::new(),
        },
        maintenance: MaintenanceRuntimeConfig::defaults(),
        federation: federation_a,
    };
    let server_a =
        McpServer::from_parts(config_a, DeterministicStubProvider::new(EmbeddingProfile::Balanced))
            .await
            .unwrap();

    let first = call_tool(
        &server_a,
        1,
        "mempalace_add_drawer",
        json!({
            "wing": wing,
            "room": room,
            "content": content,
            "added_by": added_by,
        }),
    )
    .await;

    assert_eq!(first["success"], true, "first add must succeed locally: {first}");
    assert_eq!(first["applied_to"], "local", "first add must report applied_to=local: {first}");
    let drawer_id =
        first["drawer_id"].as_str().expect("first add must return drawer_id").to_owned();

    let replication = first.get("replication").expect("both add must include replication");
    assert_eq!(
        replication["status"], "queued",
        "first add with a down remote must be queued, not terminally failed; got: {replication}"
    );
    let operation_id =
        replication["operation_id"].as_str().expect("queued must carry operation_id").to_owned();
    assert!(
        operation_id.starts_with("outbox_"),
        "stable outbox operation id expected; got: {replication}"
    );
    assert!(
        first.get("warnings").is_none(),
        "queued replication must not add inline warnings; got: {first}"
    );

    // The unreachable remote keeps the intent retryable — never a terminal failure.
    wait_for_status(
        &server_a,
        "down-remote intent to be retryable, not terminally failed",
        |status| {
            status_has_retryable(status) && !status_has_terminal_failure(status, &operation_id)
        },
    )
    .await;

    drop(server_a);

    // ── Phase 2: Restore hub on the same endpoint ────────────────────────────
    let listener = tokio::net::TcpListener::bind(hub_addr).await.unwrap();
    let config = server_config(&hub_dir);
    let tokens = TokenRegistry::load(write_token_file(&hub_dir)).unwrap();
    let provider = DeterministicStubProvider::new(EmbeddingProfile::Balanced);
    let (router, _state) = build_router(config, provider, tokens).await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    let mut remotes_b = BTreeMap::new();
    remotes_b.insert(
        "hub".to_owned(),
        ResolvedRemote {
            name: "hub".to_owned(),
            url: hub_url.clone(),
            token: Some(TEST_TOKEN.to_owned()),
            timeout: Duration::from_secs(5),
        },
    );
    let mut wing_rules_b = BTreeMap::new();
    wing_rules_b.insert(wing.to_owned(), combined_wing_rule_both_write());
    let federation_b = FederationRuntimeConfig {
        remotes: remotes_b,
        default_mode: RouteMode::Local,
        default_remote: None,
        wings: wing_rules_b,
        kg: None,
        coordination: BTreeMap::new(),
    };
    let config_b = MempalaceConfig {
        schema_version: 1,
        collection_name: "mempalace_drawers".to_owned(),
        palace_path: local_dir.path().join("palace"),
        embedding_profile: EmbeddingProfile::Balanced,
        low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
        server: ServerRuntimeConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            token_file: local_dir.path().join("server_tokens_b.json"),
            checkouts: std::collections::BTreeMap::new(),
        },
        maintenance: MaintenanceRuntimeConfig::defaults(),
        federation: federation_b,
    };
    let server_b =
        McpServer::from_parts(config_b, DeterministicStubProvider::new(EmbeddingProfile::Balanced))
            .await
            .unwrap();

    let retry = call_tool(
        &server_b,
        1,
        "mempalace_add_drawer",
        json!({
            "wing": wing,
            "room": room,
            "content": content,
            "added_by": added_by,
        }),
    )
    .await;

    assert_eq!(retry["success"], true, "retry add must succeed: {retry}");
    assert_eq!(retry["applied_to"], "local", "retry must report applied_to=local: {retry}");
    assert_eq!(
        retry["drawer_id"].as_str().unwrap_or(""),
        drawer_id,
        "retry must reuse the same drawer_id (was {drawer_id}); got: {retry}"
    );

    // Stable op identity: the retry stages the SAME durable operation, never a fresh one.
    let replication2 = retry.get("replication").expect("retry add must include replication");
    assert_eq!(
        replication2["status"], "queued",
        "retry must return status=queued for the same operation; got: {replication2}"
    );
    assert_eq!(
        replication2["operation_id"].as_str().unwrap_or(""),
        operation_id,
        "retry must reuse the same operation_id ({operation_id}); got: {replication2}"
    );
    assert_eq!(
        replication2["remote"], "hub",
        "retry replication must report remote=hub; got: {replication2}"
    );

    assert!(
        retry.get("warnings").is_none(),
        "retry must not have warnings on success; got: {retry}"
    );

    // ── The worker delivers the queued intent to the restored hub ────────────
    wait_for_hub_drawer(&hub_url, content, wing, room).await;

    // ── Verify local drawer count via a local-only server ──────────────────
    drop(server_b);
    let local_only_config = MempalaceConfig {
        schema_version: 1,
        collection_name: "mempalace_drawers".to_owned(),
        palace_path: local_dir.path().join("palace"),
        embedding_profile: EmbeddingProfile::Balanced,
        low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
        server: ServerRuntimeConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            token_file: local_dir.path().join("local_only_tokens.json"),
            checkouts: std::collections::BTreeMap::new(),
        },
        maintenance: MaintenanceRuntimeConfig::defaults(),
        federation: FederationRuntimeConfig::default(),
    };
    let local_only_server = McpServer::from_parts(
        local_only_config,
        DeterministicStubProvider::new(EmbeddingProfile::Balanced),
    )
    .await
    .unwrap();
    let local_status = call_tool(&local_only_server, 1, "mempalace_list_wings", json!({})).await;
    drop(local_only_server);
    let local_wings = local_status["wings"].as_object().expect("wings must be an object");
    let local_count = local_wings.get(wing).and_then(|v| v.as_u64()).unwrap_or(0);
    assert_eq!(
        local_count, 1,
        "must have exactly 1 local drawer in {wing} after retry; got {local_count}; wings: {local_wings:?}"
    );

    // ── Verify the drawer landed on the hub via direct hub API ─────────────
    let hub_client = RemoteClient::new(RemoteEndpoint {
        name: "hub".into(),
        base_url: hub_url,
        token: Some(TEST_TOKEN.to_owned()),
        timeout: Duration::from_secs(5),
    })
    .unwrap();
    let hub_wings = hub_client.wings().await.expect("hub wings query must succeed");
    let hub_wing_count = hub_wings["wings"]
        .as_object()
        .and_then(|w| w.get(wing))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    assert_eq!(
        hub_wing_count, 1,
        "hub must have exactly 1 drawer in {wing}; got {hub_wing_count}; wings: {hub_wings:?}"
    );
}

// ─── Test 12c: delete_both_retry_after_local_delete_replays_same_operation ──

/// A `write:both` delete retried with the same operation_id after the local drawer is already
/// gone must recover the original durable outbox intent by that key and return its queued/terminal
/// state — not fall into the synchronous all-remote fallback or a false "not found".
///
/// Phases:
/// 1. write:both add while the hub is down → local success, `replication.status: "queued"`.
/// 2. Delete with a stable caller operation_id → local commit, queued replication, and a stable
///    outbox operation_id is captured.
/// 3. Retry the delete with the SAME operation_id → the outbox row (authoritative: it names the
///    destination remote and current state that local drawer metadata can no longer provide) is
///    recovered, returning success with the SAME outbox operation_id and queued state — never a
///    `success:false` not-found and never a fresh synchronous remote delete.
#[tokio::test]
async fn delete_both_retry_after_local_delete_replays_same_operation() {
    let dead_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_addr = dead_listener.local_addr().unwrap();
    drop(dead_listener);
    let dead_url = format!("http://{dead_addr}");

    let local_dir = TempDir::new().unwrap();
    let wing = "wing_delretry";
    let room = "del-retry-room";
    let content = "dual-write delete retry regression content unique qwerty";
    let del_op = "del-replay-op-1";

    let mut remotes = BTreeMap::new();
    remotes.insert(
        "hub".to_owned(),
        ResolvedRemote {
            name: "hub".to_owned(),
            url: dead_url,
            token: Some(TEST_TOKEN.to_owned()),
            timeout: Duration::from_millis(500),
        },
    );
    let mut wing_rules = BTreeMap::new();
    wing_rules.insert(wing.to_owned(), combined_wing_rule_both_write());
    let federation = FederationRuntimeConfig {
        remotes,
        default_mode: RouteMode::Local,
        default_remote: None,
        wings: wing_rules,
        kg: None,
        coordination: BTreeMap::new(),
    };
    let config = MempalaceConfig {
        schema_version: 1,
        collection_name: "mempalace_drawers".to_owned(),
        palace_path: local_dir.path().join("palace"),
        embedding_profile: EmbeddingProfile::Balanced,
        low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
        server: ServerRuntimeConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            token_file: local_dir.path().join("server_tokens.json"),
            checkouts: std::collections::BTreeMap::new(),
        },
        maintenance: MaintenanceRuntimeConfig::defaults(),
        federation,
    };
    let server =
        McpServer::from_parts(config, DeterministicStubProvider::new(EmbeddingProfile::Balanced))
            .await
            .unwrap();

    // ── 1. write:both add while the hub is down → local + queued replication ──
    let add = call_tool(
        &server,
        1,
        "mempalace_add_drawer",
        json!({
            "wing": wing,
            "room": room,
            "content": content,
            "added_by": "del-retry-test",
        }),
    )
    .await;
    assert_eq!(add["success"], true, "add must succeed locally: {add}");
    let drawer_id = add["drawer_id"].as_str().expect("add must return drawer_id").to_owned();
    assert_eq!(
        add["replication"]["status"], "queued",
        "add replication must be queued (hub down): {add}"
    );

    // ── 2. First delete: local commit + queued durable intent ────────────────
    let del1 = call_tool(
        &server,
        2,
        "mempalace_delete_drawer",
        json!({"drawer_id": drawer_id, "operation_id": del_op}),
    )
    .await;
    assert_eq!(del1["success"], true, "first delete must succeed locally: {del1}");
    assert_eq!(del1["applied_to"], "local", "delete must report applied_to=local: {del1}");
    let repl = del1.get("replication").expect("both delete must include replication: {del1}");
    assert_eq!(repl["status"], "queued", "delete replication must be queued: {del1}");
    let operation_id =
        repl["operation_id"].as_str().expect("queued must carry operation_id").to_owned();
    assert!(operation_id.starts_with("outbox_"), "stable outbox id expected: {del1}");

    // ── 3. Retry with the SAME operation_id → replay, not fallback ───────────
    let del2 = call_tool(
        &server,
        3,
        "mempalace_delete_drawer",
        json!({"drawer_id": drawer_id, "operation_id": del_op}),
    )
    .await;
    assert_eq!(
        del2["success"], true,
        "retry must report success (local delete already committed): {del2}"
    );
    assert_eq!(del2["drawer_id"], drawer_id, "retry must echo the requested drawer_id: {del2}");
    let repl2 = del2.get("replication").expect("retry must include replication state: {del2}");
    assert_eq!(
        repl2["operation_id"].as_str().unwrap_or(""),
        operation_id,
        "retry must reuse the same outbox operation_id ({operation_id}); got: {del2}"
    );
    assert_eq!(
        repl2["status"], "queued",
        "retry must report the original queued state; got: {del2}"
    );
    assert!(del2.get("error").is_none(), "retry must not fall into false not-found; got: {del2}");
}

// ─── Test 12d: delete_both_retry_after_restart_replays_same_operation ───────

/// A `write:both` delete whose durable outbox intent survives a process restart must be recovered
/// by the caller's operation_id on retry after restart, returning the original queued/terminal
/// state. Startup reconciliation on restart settles any staged intent; the activated delete intent
/// here is already `pending`/retryable and is read back verbatim.
#[tokio::test]
async fn delete_both_retry_after_restart_replays_same_operation() {
    let dead_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_addr = dead_listener.local_addr().unwrap();
    drop(dead_listener);
    let dead_url = format!("http://{dead_addr}");

    let local_dir = TempDir::new().unwrap();
    let wing = "wing_delrestart";
    let room = "del-restart-room";
    let content = "dual-write delete restart regression content unique asdfgh";
    let del_op = "del-replay-op-2";

    let make_federation = || FederationRuntimeConfig {
        remotes: BTreeMap::from([(
            "hub".to_owned(),
            ResolvedRemote {
                name: "hub".to_owned(),
                url: dead_url.clone(),
                token: Some(TEST_TOKEN.to_owned()),
                timeout: Duration::from_millis(500),
            },
        )]),
        default_mode: RouteMode::Local,
        default_remote: None,
        wings: BTreeMap::from([(wing.to_owned(), combined_wing_rule_both_write())]),
        kg: None,
        coordination: BTreeMap::new(),
    };
    let make_config = || MempalaceConfig {
        schema_version: 1,
        collection_name: "mempalace_drawers".to_owned(),
        palace_path: local_dir.path().join("palace"),
        embedding_profile: EmbeddingProfile::Balanced,
        low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
        server: ServerRuntimeConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            token_file: local_dir.path().join("server_tokens.json"),
            checkouts: std::collections::BTreeMap::new(),
        },
        maintenance: MaintenanceRuntimeConfig::defaults(),
        federation: make_federation(),
    };

    let server_a = McpServer::from_parts(
        make_config(),
        DeterministicStubProvider::new(EmbeddingProfile::Balanced),
    )
    .await
    .unwrap();

    let add = call_tool(
        &server_a,
        1,
        "mempalace_add_drawer",
        json!({
            "wing": wing,
            "room": room,
            "content": content,
            "added_by": "del-restart-test",
        }),
    )
    .await;
    assert_eq!(add["success"], true, "add must succeed locally: {add}");
    let drawer_id = add["drawer_id"].as_str().expect("add must return drawer_id").to_owned();

    // The delete commits locally (op activated to pending) before the response returns, so the
    // intent is durable when we crash.
    let del1 = call_tool(
        &server_a,
        2,
        "mempalace_delete_drawer",
        json!({"drawer_id": drawer_id, "operation_id": del_op}),
    )
    .await;
    assert_eq!(del1["success"], true, "delete must succeed locally: {del1}");
    let operation_id = del1["replication"]["operation_id"]
        .as_str()
        .expect("queued must carry operation_id")
        .to_owned();
    assert!(operation_id.starts_with("outbox_"));

    // ── Crash (drop the runtime) and restart on the same palace dir ──────────
    drop(server_a);
    let server_b = McpServer::from_parts(
        make_config(),
        DeterministicStubProvider::new(EmbeddingProfile::Balanced),
    )
    .await
    .unwrap();

    // ── Retry the same operation_id after restart → recover the original intent ──
    let del2 = call_tool(
        &server_b,
        3,
        "mempalace_delete_drawer",
        json!({"drawer_id": drawer_id, "operation_id": del_op}),
    )
    .await;
    assert_eq!(
        del2["success"], true,
        "retry after restart must report success (local delete already committed): {del2}"
    );
    let repl2 = del2.get("replication").expect("retry must include replication state: {del2}");
    assert_eq!(
        repl2["operation_id"].as_str().unwrap_or(""),
        operation_id,
        "restart retry must reuse the same outbox operation_id ({operation_id}); got: {del2}"
    );
    assert_eq!(
        repl2["status"], "queued",
        "restart retry must report the original queued state; got: {del2}"
    );
    assert!(del2.get("error").is_none(), "restart retry must not be a false not-found: {del2}");
}

// ─── Test 13: add_drawer_both_replication_fails_with_down_remote ────────────

/// Combined/write:Both wing rule with the remote down (dead address). The local write must still
/// succeed immediately and report `applied_to: "local"` with `replication.status: "queued"` —
/// issue #127 semantics: the tool returns before delivery and never blocks on the remote. The
/// in-process worker fails to reach the remote (an unreachable-before-send error), which is a
/// **retryable** outcome: the intent stays in the backlog with exponential backoff rather than
/// recording a terminal failure. The test asserts the queued response, then that status shows a
/// retryable backlog entry and NO terminal failure for that operation.
#[tokio::test]
async fn add_drawer_both_replication_fails_with_down_remote() {
    let dead_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_addr = dead_listener.local_addr().unwrap();
    drop(dead_listener);
    let dead_url = format!("http://{dead_addr}");

    let local_dir = TempDir::new().unwrap();

    let mut remotes = BTreeMap::new();
    remotes.insert(
        "hub".to_owned(),
        ResolvedRemote {
            name: "hub".to_owned(),
            url: dead_url,
            token: Some(TEST_TOKEN.to_owned()),
            timeout: Duration::from_millis(500),
        },
    );

    let mut wing_rules = BTreeMap::new();
    wing_rules.insert("wing_both_down".to_owned(), combined_wing_rule_both_write());

    let federation = FederationRuntimeConfig {
        remotes,
        default_mode: RouteMode::Local,
        default_remote: None,
        wings: wing_rules,
        kg: None,
        coordination: BTreeMap::new(),
    };

    let config = MempalaceConfig {
        schema_version: 1,
        collection_name: "mempalace_drawers".to_owned(),
        palace_path: local_dir.path().join("palace"),
        embedding_profile: EmbeddingProfile::Balanced,
        low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
        server: ServerRuntimeConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            token_file: local_dir.path().join("server_tokens.json"),
            checkouts: std::collections::BTreeMap::new(),
        },
        maintenance: MaintenanceRuntimeConfig::defaults(),
        federation,
    };

    let server =
        McpServer::from_parts(config, DeterministicStubProvider::new(EmbeddingProfile::Balanced))
            .await
            .unwrap();

    let add = call_tool(
        &server,
        1,
        "mempalace_add_drawer",
        json!({
            "wing": "wing_both_down",
            "room": "both-down-room",
            "content": "dual-write e2e down remote replication drawer",
            "added_by": "both-down-test"
        }),
    )
    .await;

    // Local write must succeed.
    assert_eq!(add["success"], true, "both add with down remote must still succeed: {add}");
    assert_eq!(
        add["applied_to"], "local",
        "both add with down remote must report applied_to=local: {add}"
    );

    // Replication is queued (async delivery), never a synchronous failure.
    let replication = add.get("replication").expect("both add must include replication field");
    assert_eq!(
        replication["status"], "queued",
        "replication must report status=queued with down remote; got: {replication}"
    );
    assert_eq!(
        replication["remote"], "hub",
        "replication must report remote=hub; got: {replication}"
    );
    let operation_id =
        replication["operation_id"].as_str().expect("queued must carry operation_id").to_owned();

    // No inline warnings — the outage is surfaced through the retryable backlog.
    assert!(
        add.get("warnings").is_none(),
        "queued replication must not add inline warnings; got: {add}"
    );

    // The unreachable remote is retryable: the intent stays in the backlog with backoff, and is
    // NEVER recorded as a terminal failure for a merely-unreachable remote.
    wait_for_status(&server, "retryable backlog entry without a terminal failure", |status| {
        status_has_retryable(status) && !status_has_terminal_failure(status, &operation_id)
    })
    .await;
}

// ─── Test 14: kg_add_both_replicates_successfully ───────────────────────────

/// Combined KG rule with write:Both → local KG add succeeds, and remote replication is durably
/// queued: the response reports `applied_to: "local"` with `replication.status: "queued"` and a
/// stable `operation_id`. The background worker then delivers the fact to the hub; the test
/// awaits that async delivery by polling the hub's KG directly, and asserts the operation was
/// not recorded as a terminal failure.
#[tokio::test]
async fn kg_add_both_replicates_successfully() {
    let hub_dir = TempDir::new().unwrap();
    let local_dir = TempDir::new().unwrap();

    let addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{addr}");

    let server = mcp_server_with_hub(
        &local_dir,
        &hub_url,
        BTreeMap::new(),
        RouteMode::Local,
        Some(combined_kg_rule_both_write()),
    )
    .await;

    let kg_add = call_tool(
        &server,
        1,
        "mempalace_kg_add",
        json!({
            "subject": "BothTest",
            "predicate": "uses_protocol",
            "object": "dual_write_kg"
        }),
    )
    .await;

    assert_eq!(kg_add["success"], true, "kg_add both must succeed: {kg_add}");
    assert_eq!(kg_add["applied_to"], "local", "kg_add both must report applied_to=local: {kg_add}");
    let replication = kg_add.get("replication").expect("kg_add both must include replication");
    assert_eq!(
        replication["status"], "queued",
        "kg_add replication must be queued (async delivery); got: {replication}"
    );
    assert_eq!(
        replication["remote"], "hub",
        "kg_add replication must target hub; got: {replication}"
    );
    let operation_id =
        replication["operation_id"].as_str().expect("queued must carry operation_id").to_owned();
    assert!(kg_add.get("warnings").is_none(), "kg_add must not have warnings on success: {kg_add}");

    // Await the background worker's async delivery to the hub.
    wait_for_hub_kg_fact(&hub_url, "BothTest", "uses_protocol", "dual_write_kg", None, true).await;

    // Delivered, not terminally failed.
    wait_for_status(&server, "kg_add replication to be acknowledged, not failed", |status| {
        !status_has_terminal_failure(status, &operation_id)
    })
    .await;
}

// ─── Test 15: kg_add_both_replication_fails_with_down_remote ────────────────

/// Combined KG rule with write:Both and a dead remote. Local KG add succeeds and reports
/// `status: "queued"` immediately; the worker keeps the intent in the retryable backlog (an
/// unreachable-before-send error is retryable), and never records a terminal failure for a
/// merely-unreachable remote.
#[tokio::test]
async fn kg_add_both_replication_fails_with_down_remote() {
    let dead_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_addr = dead_listener.local_addr().unwrap();
    drop(dead_listener);
    let dead_url = format!("http://{dead_addr}");

    let local_dir = TempDir::new().unwrap();

    let mut remotes = BTreeMap::new();
    remotes.insert(
        "hub".to_owned(),
        ResolvedRemote {
            name: "hub".to_owned(),
            url: dead_url,
            token: Some(TEST_TOKEN.to_owned()),
            timeout: Duration::from_millis(500),
        },
    );

    let federation = FederationRuntimeConfig {
        remotes,
        default_mode: RouteMode::Local,
        default_remote: None,
        wings: BTreeMap::new(),
        kg: Some(combined_kg_rule_both_write()),
        coordination: BTreeMap::new(),
    };

    let config = MempalaceConfig {
        schema_version: 1,
        collection_name: "mempalace_drawers".to_owned(),
        palace_path: local_dir.path().join("palace"),
        embedding_profile: EmbeddingProfile::Balanced,
        low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
        server: ServerRuntimeConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            token_file: local_dir.path().join("server_tokens.json"),
            checkouts: std::collections::BTreeMap::new(),
        },
        maintenance: MaintenanceRuntimeConfig::defaults(),
        federation,
    };

    let server =
        McpServer::from_parts(config, DeterministicStubProvider::new(EmbeddingProfile::Balanced))
            .await
            .unwrap();

    let kg_add = call_tool(
        &server,
        1,
        "mempalace_kg_add",
        json!({
            "subject": "BothDownTest",
            "predicate": "replication",
            "object": "failed"
        }),
    )
    .await;

    assert_eq!(kg_add["success"], true, "kg_add with down remote must succeed locally: {kg_add}");
    assert_eq!(kg_add["applied_to"], "local", "kg_add must report applied_to=local: {kg_add}");
    let replication = kg_add.get("replication").expect("kg_add both must include replication");
    assert_eq!(
        replication["status"], "queued",
        "kg_add replication must be queued with down remote; got: {replication}"
    );
    assert_eq!(replication["remote"], "hub", "replication must target hub; got: {replication}");
    let operation_id =
        replication["operation_id"].as_str().expect("queued must carry operation_id").to_owned();

    wait_for_status(&server, "retryable backlog entry without a terminal failure", |status| {
        status_has_retryable(status) && !status_has_terminal_failure(status, &operation_id)
    })
    .await;
}

// ─── Test 16: kg_invalidate_both_replicates_successfully ────────────────────

/// Combined KG rule with write:Both → local KG invalidate succeeds, and the invalidation is
/// durably queued (`applied_to: "local"`, `replication.status: "queued"`). The background worker
/// delivers both the add and the invalidate to the hub; the test awaits each async leg by
/// polling the hub's KG directly with `as_of` dates.
///
/// Verifies invalidation on the hub by using explicit `ended` + `as_of` dates:
/// the fact has `valid_from: 2026-01-01`, is invalidated with `ended: 2026-06-01`,
/// and `mempalace_kg_query` with `as_of: "2026-07-01"` must NOT include the fact.
#[tokio::test]
async fn kg_invalidate_both_replicates_successfully() {
    let hub_dir = TempDir::new().unwrap();
    let local_dir = TempDir::new().unwrap();

    let addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{addr}");

    let server = mcp_server_with_hub(
        &local_dir,
        &hub_url,
        BTreeMap::new(),
        RouteMode::Local,
        Some(combined_kg_rule_both_write()),
    )
    .await;

    // First add a fact with a known valid_from date so we can test as_of queries.
    let add_resp = call_tool(
        &server,
        1,
        "mempalace_kg_add",
        json!({
            "subject": "InvalidateBothTest",
            "predicate": "replication",
            "object": "active",
            "valid_from": "2026-01-01"
        }),
    )
    .await;
    assert_eq!(add_resp["success"], true, "kg_add must succeed: {add_resp}");
    assert_eq!(
        add_resp["replication"]["status"], "queued",
        "kg_add must be durably queued: {add_resp}"
    );

    // Verify the fact exists on the hub before invalidation, awaiting async delivery.
    wait_for_hub_kg_fact(
        &hub_url,
        "InvalidateBothTest",
        "replication",
        "active",
        Some("2026-03-01"),
        true,
    )
    .await;

    // Now invalidate with an explicit past ended date.
    let invalidate = call_tool(
        &server,
        3,
        "mempalace_kg_invalidate",
        json!({
            "subject": "InvalidateBothTest",
            "predicate": "replication",
            "object": "active",
            "ended": "2026-06-01"
        }),
    )
    .await;

    assert_eq!(invalidate["success"], true, "kg_invalidate both must succeed: {invalidate}");
    assert_eq!(
        invalidate["applied_to"], "local",
        "kg_invalidate must report applied_to=local: {invalidate}"
    );
    let replication =
        invalidate.get("replication").expect("kg_invalidate must include replication");
    assert_eq!(
        replication["status"], "queued",
        "invalidate replication must be queued; got: {replication}"
    );
    assert_eq!(
        replication["remote"], "hub",
        "invalidate replication must target hub; got: {replication}"
    );
    let operation_id =
        replication["operation_id"].as_str().expect("queued must carry operation_id").to_owned();
    assert!(
        invalidate.get("warnings").is_none(),
        "kg_invalidate must not have warnings on success: {invalidate}"
    );

    // ── Verify the fact is invalidated on the hub using as_of after ended ───
    wait_for_hub_kg_fact(
        &hub_url,
        "InvalidateBothTest",
        "replication",
        "active",
        Some("2026-07-01"),
        false,
    )
    .await;

    // Query without as_of returns all facts including historical ones (the fact
    // should still exist as a historical record on the hub).
    let client = hub_client(&hub_url);
    let hub_all = client
        .kg_query(KgQueryRequest {
            entity: "InvalidateBothTest".to_owned(),
            as_of: None,
            direction: Some("outgoing".to_owned()),
        })
        .await
        .expect("hub KG query without as_of must succeed");
    let facts_all = hub_all["facts"].as_array().expect("hub facts must be array");
    let hub_fact_historical = facts_all.iter().find(|f| {
        f["predicate"].as_str() == Some("replication") && f["object"].as_str() == Some("active")
    });
    assert!(
        hub_fact_historical.is_some(),
        "hub KG query without as_of must show the historical fact; facts: {facts_all:?}"
    );

    // Delivered, not terminally failed.
    wait_for_status(&server, "kg_invalidate to be acknowledged, not failed", |status| {
        !status_has_terminal_failure(status, &operation_id)
    })
    .await;
}

// ─── Test 17: kg_invalidate_both_replication_fails_with_down_remote ─────────

/// Combined KG rule with write:Both and a dead remote. Local KG invalidate succeeds and
/// reports `status: "queued"`; the worker keeps the intent retryable, never termially failed.
#[tokio::test]
async fn kg_invalidate_both_replication_fails_with_down_remote() {
    let dead_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_addr = dead_listener.local_addr().unwrap();
    drop(dead_listener);
    let dead_url = format!("http://{dead_addr}");

    let local_dir = TempDir::new().unwrap();

    let mut remotes = BTreeMap::new();
    remotes.insert(
        "hub".to_owned(),
        ResolvedRemote {
            name: "hub".to_owned(),
            url: dead_url,
            token: Some(TEST_TOKEN.to_owned()),
            timeout: Duration::from_millis(500),
        },
    );

    let federation = FederationRuntimeConfig {
        remotes,
        default_mode: RouteMode::Local,
        default_remote: None,
        wings: BTreeMap::new(),
        kg: Some(combined_kg_rule_both_write()),
        coordination: BTreeMap::new(),
    };

    let config = MempalaceConfig {
        schema_version: 1,
        collection_name: "mempalace_drawers".to_owned(),
        palace_path: local_dir.path().join("palace"),
        embedding_profile: EmbeddingProfile::Balanced,
        low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
        server: ServerRuntimeConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            token_file: local_dir.path().join("server_tokens.json"),
            checkouts: std::collections::BTreeMap::new(),
        },
        maintenance: MaintenanceRuntimeConfig::defaults(),
        federation,
    };

    let server =
        McpServer::from_parts(config, DeterministicStubProvider::new(EmbeddingProfile::Balanced))
            .await
            .unwrap();

    // First add a fact locally so we can invalidate it.
    // (With dead remote, the KG add queues replication; we ignore its eventual backlog state.)
    let _add_resp = call_tool(
        &server,
        1,
        "mempalace_kg_add",
        json!({
            "subject": "InvalidateDownTest",
            "predicate": "replication",
            "object": "inactive"
        }),
    )
    .await;

    // Invalidate with dead remote.
    let invalidate = call_tool(
        &server,
        2,
        "mempalace_kg_invalidate",
        json!({
            "subject": "InvalidateDownTest",
            "predicate": "replication",
            "object": "inactive"
        }),
    )
    .await;

    assert_eq!(
        invalidate["success"], true,
        "kg_invalidate with down remote must succeed locally: {invalidate}"
    );
    assert_eq!(
        invalidate["applied_to"], "local",
        "kg_invalidate must report applied_to=local: {invalidate}"
    );
    let replication =
        invalidate.get("replication").expect("kg_invalidate must include replication");
    assert_eq!(
        replication["status"], "queued",
        "invalidate replication must be queued with down remote; got: {replication}"
    );
    assert_eq!(replication["remote"], "hub", "replication must target hub; got: {replication}");
    let operation_id =
        replication["operation_id"].as_str().expect("queued must carry operation_id").to_owned();

    wait_for_status(&server, "retryable backlog entry without a terminal failure", |status| {
        status_has_retryable(status) && !status_has_terminal_failure(status, &operation_id)
    })
    .await;
}

// ─── Test 18: legacy_local_route_has_no_replication_field ────────────────────

/// With federation active but the resolved route being Local (default_mode=Local,
/// no wing rule), tool_add_drawer must NOT include a `replication` field in the
/// response — dual-write semantics only apply to `write:both`.
#[tokio::test]
async fn legacy_local_route_has_no_replication_field() {
    let hub_dir = TempDir::new().unwrap();
    let local_dir = TempDir::new().unwrap();

    let addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{addr}");

    // No wing rules → all wings route as Local (default_mode=Local).
    let server =
        mcp_server_with_hub(&local_dir, &hub_url, BTreeMap::new(), RouteMode::Local, None).await;

    let add = call_tool(
        &server,
        1,
        "mempalace_add_drawer",
        json!({
            "wing": "wing_legacy_local",
            "room": "local",
            "content": "legacy local route — no replication field expected",
            "added_by": "legacy-test"
        }),
    )
    .await;

    assert_eq!(add["success"], true, "local add must succeed: {add}");
    assert_eq!(add["applied_to"], "local", "local add must report applied_to=local: {add}");
    // Local route must NOT have a replication field.
    assert!(
        add.get("replication").is_none(),
        "local route add must not include replication field; got: {add}"
    );
}

// ─── Test 19: legacy_remote_route_has_no_replication_field ───────────────────

/// With federation active but the resolved route being Remote (write:remote),
/// tool_add_drawer must NOT include a `replication` field — the write goes
/// directly to the remote without a local write.
#[tokio::test]
async fn legacy_remote_route_has_no_replication_field() {
    let hub_dir = TempDir::new().unwrap();
    let local_dir = TempDir::new().unwrap();

    let addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{addr}");

    let mut wing_rules = BTreeMap::new();
    wing_rules.insert("wing_legacy_remote".to_owned(), combined_wing_rule_remote_write());

    let server =
        mcp_server_with_hub(&local_dir, &hub_url, wing_rules, RouteMode::Local, None).await;

    let add = call_tool(
        &server,
        1,
        "mempalace_add_drawer",
        json!({
            "wing": "wing_legacy_remote",
            "room": "remote-room",
            "content": "legacy remote route — no replication field expected",
            "added_by": "legacy-test"
        }),
    )
    .await;

    assert_eq!(add["success"], true, "remote add must succeed: {add}");
    assert_eq!(add["origin"], "hub", "remote add must report origin=hub: {add}");
    assert_eq!(
        add["applied_to"], "remote:hub",
        "remote add must report applied_to=remote:hub: {add}"
    );
    // Remote route must NOT have a replication field.
    assert!(
        add.get("replication").is_none(),
        "remote route add must not include replication field; got: {add}"
    );
}

// ─── Test 20: add_drawer_both_diary_guard_skips_replication ──────────────────

/// Even with a Both wing rule, a diary-room drawer must stay local and NOT
/// include a `replication` field in the response — diary-local writes are
/// exclusively local and must not carry a `replication` structure at all.
#[tokio::test]
async fn add_drawer_both_diary_guard_skips_replication() {
    let hub_dir = TempDir::new().unwrap();
    let local_dir = TempDir::new().unwrap();

    let addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{addr}");

    let mut wing_rules = BTreeMap::new();
    wing_rules.insert("wing_diary_both".to_owned(), combined_wing_rule_both_write());

    let server =
        mcp_server_with_hub(&local_dir, &hub_url, wing_rules, RouteMode::Local, None).await;

    let add = call_tool(
        &server,
        1,
        "mempalace_add_drawer",
        json!({
            "wing": "wing_diary_both",
            "room": "diary",
            "content": "diary guard test with both route — replication must be absent",
            "added_by": "diary-both-test"
        }),
    )
    .await;

    assert_eq!(add["success"], true, "diary both add must succeed: {add}");
    assert_eq!(add["applied_to"], "local", "diary both add must report applied_to=local: {add}");

    // Diary-local writes must NOT include a replication field per contract.
    assert!(
        add.get("replication").is_none(),
        "diary-local add must not include a replication field; got: {add}"
    );
}

// ─── Test 21: kg_add_both_with_valid_from_succeeds ──────────────────────────

/// Combined KG rule with write:Both and a `valid_from` date. Local KG add of a dated fact
/// succeeds and reports `replication.status: "queued"`; the background worker then delivers the
/// dated fact to the hub, which the test awaits via direct hub polling.
#[tokio::test]
async fn kg_add_both_with_valid_from_succeeds() {
    let hub_dir = TempDir::new().unwrap();
    let local_dir = TempDir::new().unwrap();

    let addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{addr}");

    let server = mcp_server_with_hub(
        &local_dir,
        &hub_url,
        BTreeMap::new(),
        RouteMode::Local,
        Some(combined_kg_rule_both_write()),
    )
    .await;

    let kg_add = call_tool(
        &server,
        1,
        "mempalace_kg_add",
        json!({
            "subject": "DatedBothTest",
            "predicate": "started_on",
            "object": "project_epsilon",
            "valid_from": "2026-01-15"
        }),
    )
    .await;

    assert_eq!(kg_add["success"], true, "dated kg_add both must succeed: {kg_add}");
    assert_eq!(
        kg_add["applied_to"], "local",
        "dated kg_add must report applied_to=local: {kg_add}"
    );
    let replication = kg_add.get("replication").expect("dated kg_add must include replication");
    assert_eq!(
        replication["status"], "queued",
        "dated replication must be queued; got: {replication}"
    );
    assert_eq!(
        replication["remote"], "hub",
        "dated replication must target hub; got: {replication}"
    );
    let operation_id =
        replication["operation_id"].as_str().expect("queued must carry operation_id").to_owned();

    // Await async delivery of the dated fact to the hub.
    wait_for_hub_kg_fact(&hub_url, "DatedBothTest", "started_on", "project_epsilon", None, true)
        .await;

    wait_for_status(&server, "dated kg_add to be acknowledged, not failed", |status| {
        !status_has_terminal_failure(status, &operation_id)
    })
    .await;
}

// ─── Test 24: kg_add_both_replication_fails_with_remote_rejection ───────────

/// Combined KG rule with write:Both and a reachable remote that rejects the replication attempt
/// (wrong bearer token → HTTP 401). The local KG add succeeds immediately and reports
/// `replication.status: "queued"`; the worker then observes the authoritative `Unauthorized`
/// rejection and records the operation as a terminal failure via `mempalace_status`.
#[tokio::test]
async fn kg_add_both_replication_fails_with_remote_rejection() {
    let hub_dir = TempDir::new().unwrap();
    let local_dir = TempDir::new().unwrap();

    let addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{addr}");

    // Use a wrong token so the hub rejects the replication with HTTP 401.
    let bad_token = "wrong-token-for-kg-add-xyz";

    let mut remotes = BTreeMap::new();
    remotes.insert(
        "hub".to_owned(),
        ResolvedRemote {
            name: "hub".to_owned(),
            url: hub_url,
            token: Some(bad_token.to_owned()),
            timeout: Duration::from_secs(5),
        },
    );

    let federation = FederationRuntimeConfig {
        remotes,
        default_mode: RouteMode::Local,
        default_remote: None,
        wings: BTreeMap::new(),
        kg: Some(combined_kg_rule_both_write()),
        coordination: BTreeMap::new(),
    };

    let config = MempalaceConfig {
        schema_version: 1,
        collection_name: "mempalace_drawers".to_owned(),
        palace_path: local_dir.path().join("palace"),
        embedding_profile: EmbeddingProfile::Balanced,
        low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
        server: ServerRuntimeConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            token_file: local_dir.path().join("server_tokens.json"),
            checkouts: std::collections::BTreeMap::new(),
        },
        maintenance: MaintenanceRuntimeConfig::defaults(),
        federation,
    };

    let server =
        McpServer::from_parts(config, DeterministicStubProvider::new(EmbeddingProfile::Balanced))
            .await
            .unwrap();

    let kg_add = call_tool(
        &server,
        1,
        "mempalace_kg_add",
        json!({
            "subject": "KgRejectTest",
            "predicate": "replication",
            "object": "rejected"
        }),
    )
    .await;

    // Local write must succeed despite the remote rejection.
    assert_eq!(
        kg_add["success"], true,
        "kg_add with wrong token must still succeed locally: {kg_add}"
    );
    assert_eq!(kg_add["applied_to"], "local", "kg_add must report applied_to=local: {kg_add}");

    // Replication is queued (async delivery) — not a synchronous failure.
    let replication = kg_add.get("replication").expect("kg_add both must include replication");
    assert_eq!(
        replication["status"], "queued",
        "kg_add replication must be queued with wrong token; got: {replication}"
    );
    assert_eq!(
        replication["remote"], "hub",
        "kg_add replication must target hub; got: {replication}"
    );
    let operation_id =
        replication["operation_id"].as_str().expect("queued must carry operation_id").to_owned();

    // The authoritative permanent rejection terminates the operation.
    wait_for_status(&server, "kg_add operation to be recorded as a terminal failure", |status| {
        status_has_terminal_failure(status, &operation_id)
    })
    .await;
}

// ─── Test 25: kg_invalidate_both_replication_fails_with_remote_rejection ────

/// Combined KG rule with write:Both and a reachable remote that rejects the KG invalidate
/// replication (wrong bearer token → HTTP 401). The local KG invalidate succeeds immediately and
/// reports `replication.status: "queued"`; the worker then observes the authoritative
/// `Unauthorized` rejection and records the operation as a terminal failure.
#[tokio::test]
async fn kg_invalidate_both_replication_fails_with_remote_rejection() {
    let hub_dir = TempDir::new().unwrap();
    let local_dir = TempDir::new().unwrap();

    let addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{addr}");

    // First, seed a fact on the hub using the correct token via a separate server.
    let seed_dir = TempDir::new().unwrap();
    {
        let seed_server = mcp_server_with_hub(
            &seed_dir,
            &hub_url,
            BTreeMap::new(),
            RouteMode::Local,
            Some(combined_kg_rule_remote_write_for_test()),
        )
        .await;
        let seed = call_tool(
            &seed_server,
            1,
            "mempalace_kg_add",
            json!({
                "subject": "KgInvalidateRejectTest",
                "predicate": "replication",
                "object": "will_be_rejected"
            }),
        )
        .await;
        assert_eq!(seed["success"], true, "seed kg_add must succeed: {seed}");
    }

    // Now use a wrong token for the invalidate replication test.
    let bad_token = "wrong-token-for-kg-invalidate-xyz";

    let mut remotes = BTreeMap::new();
    remotes.insert(
        "hub".to_owned(),
        ResolvedRemote {
            name: "hub".to_owned(),
            url: hub_url,
            token: Some(bad_token.to_owned()),
            timeout: Duration::from_secs(5),
        },
    );

    let federation = FederationRuntimeConfig {
        remotes,
        default_mode: RouteMode::Local,
        default_remote: None,
        wings: BTreeMap::new(),
        kg: Some(combined_kg_rule_both_write()),
        coordination: BTreeMap::new(),
    };

    let config = MempalaceConfig {
        schema_version: 1,
        collection_name: "mempalace_drawers".to_owned(),
        palace_path: local_dir.path().join("palace"),
        embedding_profile: EmbeddingProfile::Balanced,
        low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
        server: ServerRuntimeConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            token_file: local_dir.path().join("server_tokens.json"),
            checkouts: std::collections::BTreeMap::new(),
        },
        maintenance: MaintenanceRuntimeConfig::defaults(),
        federation,
    };

    let server =
        McpServer::from_parts(config, DeterministicStubProvider::new(EmbeddingProfile::Balanced))
            .await
            .unwrap();

    // Add the fact locally so we can invalidate it.
    let local_add = call_tool(
        &server,
        1,
        "mempalace_kg_add",
        json!({
            "subject": "KgInvalidateRejectTest",
            "predicate": "replication",
            "object": "will_be_rejected"
        }),
    )
    .await;
    assert_eq!(local_add["success"], true, "local kg_add must succeed: {local_add}");

    // Invalidate with wrong token — replication must fail but local succeeds.
    let invalidate = call_tool(
        &server,
        2,
        "mempalace_kg_invalidate",
        json!({
            "subject": "KgInvalidateRejectTest",
            "predicate": "replication",
            "object": "will_be_rejected"
        }),
    )
    .await;

    assert_eq!(
        invalidate["success"], true,
        "kg_invalidate with wrong token must succeed locally: {invalidate}"
    );
    assert_eq!(
        invalidate["applied_to"], "local",
        "kg_invalidate must report applied_to=local: {invalidate}"
    );

    // Replication is queued (async delivery) — not a synchronous failure.
    let replication =
        invalidate.get("replication").expect("kg_invalidate must include replication");
    assert_eq!(
        replication["status"], "queued",
        "kg_invalidate replication must be queued with wrong token; got: {replication}"
    );
    assert_eq!(replication["remote"], "hub", "replication must target hub; got: {replication}");
    let operation_id =
        replication["operation_id"].as_str().expect("queued must carry operation_id").to_owned();

    // The authoritative permanent rejection terminates the operation.
    wait_for_status(
        &server,
        "kg_invalidate operation to be recorded as a terminal failure",
        |status| status_has_terminal_failure(status, &operation_id),
    )
    .await;
}

/// Helper: combined-mode KG rule routing to "hub" with Remote write
/// (used for seeding facts on the hub).
fn combined_kg_rule_remote_write_for_test() -> ResolvedRouteRule {
    ResolvedRouteRule {
        mode: RouteMode::Combined,
        remote: Some("hub".to_owned()),
        write: WriteTarget::Remote,
    }
}

// ─── Coordination federation tests (issue #102 Stage 4) ───────────────────────

/// `mempalace_task_create` for a wing whose `federation.coordination` rule is `Remote` must
/// land on the hub, not locally — the one coordination write routed by wing rather than by
/// ID-discovery fallback.
#[tokio::test]
async fn coordination_task_create_routes_to_remote_when_wing_configured_remote() {
    let local_dir = TempDir::new().unwrap();
    let hub_dir = TempDir::new().unwrap();
    let hub_addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{hub_addr}");

    let mut coordination_rules = BTreeMap::new();
    coordination_rules.insert("wing_coordremote".to_owned(), remote_wing_rule());

    let server = mcp_server_with_hub_coordination(
        &local_dir,
        &hub_url,
        BTreeMap::new(),
        coordination_rules,
        RouteMode::Local,
    )
    .await;

    let response = call_tool(
        &server,
        1,
        "mempalace_task_create",
        json!({
            "title": "index the repo",
            "description": "d",
            "wing": "wing_coordremote",
            "idempotency_key": "e2e-remote-task-1",
            "created_by": "alice",
        }),
    )
    .await;

    assert_eq!(
        response["applied_to"], "remote:hub",
        "task_create for a Remote-routed coordination wing must land on the hub: {response}"
    );
    assert!(response["task_id"].as_str().is_some(), "response must carry a task_id: {response}");

    // The local palace must not have created a copy.
    let local_get =
        call_tool(&server, 2, "mempalace_task_get", json!({"task_id": response["task_id"]})).await;
    assert_eq!(local_get["found"], true, "the task must still be discoverable via ID fallback");
    assert_eq!(
        local_get["value"]["origin"], "remote:hub",
        "and must be reported as coming from the hub, not local: {local_get}"
    );
}

/// A combined-mode exact-ID read: a task created directly on the hub (bypassing the local MCP
/// server entirely) is invisible locally, but `mempalace_task_get` still finds it by falling
/// back to the configured remote — and annotates `origin` so the caller can tell.
#[tokio::test]
async fn coordination_task_get_falls_back_to_remote_after_local_miss() {
    let local_dir = TempDir::new().unwrap();
    let hub_dir = TempDir::new().unwrap();
    let hub_addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{hub_addr}");

    let hub_client = RemoteClient::new(RemoteEndpoint {
        name: "hub".to_owned(),
        base_url: hub_url.clone(),
        token: Some(TEST_TOKEN.to_owned()),
        timeout: Duration::from_secs(5),
    })
    .unwrap();
    let hub_task = hub_client
        .coordination_task_create(mempalace_federation::NewTaskRequest {
            title: "hub-only task".to_owned(),
            description: "d".to_owned(),
            wing: "wing_hubonly".to_owned(),
            idempotency_key: "e2e-hubonly-1".to_owned(),
            created_by: Some("alice".to_owned()),
            parent_id: None,
            dependencies: Vec::new(),
            budget: None,
            expires_at: None,
        })
        .await
        .unwrap();

    // No coordination rule at all for `wing_hubonly` — the fallback is purely ID-discovery,
    // triggered by coordination federation being configured at all (here, via a non-Local
    // `default_mode`), not by any specific wing routing. `default_mode: Local` with an empty
    // `coordination` table means coordination federation was never configured, and the
    // fallback must not run in that case — see
    // `coordination_fallback_records_zero_remote_calls_without_coordination_federation_config`
    // in `crates/mempalace-mcp/src/federation.rs`.
    let server = mcp_server_with_hub_coordination(
        &local_dir,
        &hub_url,
        BTreeMap::new(),
        BTreeMap::new(),
        RouteMode::Combined,
    )
    .await;

    let response =
        call_tool(&server, 1, "mempalace_task_get", json!({"task_id": hub_task.task_id})).await;
    assert_eq!(response["found"], true, "must find the task via remote fallback: {response}");
    assert_eq!(response["value"]["task_id"], json!(hub_task.task_id));
    assert_eq!(response["value"]["origin"], "remote:hub");

    // A task_id that exists nowhere still comes back as a clean "not found", not an error.
    let missing =
        call_tool(&server, 2, "mempalace_task_get", json!({"task_id": "task_does_not_exist"}))
            .await;
    assert_eq!(missing["found"], false, "a genuinely missing task must report found=false");
}

/// A revision conflict on a remotely-owned task, discovered through the local-first fallback,
/// surfaces through the MCP tool exactly like a local conflict does — `success: false` with the
/// remote's actual current revision, not a JSON-RPC error the caller would have to unwrap.
#[tokio::test]
async fn coordination_claim_revision_conflict_via_remote_fallback() {
    let local_dir = TempDir::new().unwrap();
    let hub_dir = TempDir::new().unwrap();
    let hub_addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{hub_addr}");

    let hub_client = RemoteClient::new(RemoteEndpoint {
        name: "hub".to_owned(),
        base_url: hub_url.clone(),
        token: Some(TEST_TOKEN.to_owned()),
        timeout: Duration::from_secs(5),
    })
    .unwrap();
    let hub_task = hub_client
        .coordination_task_create(mempalace_federation::NewTaskRequest {
            title: "claim conflict test".to_owned(),
            description: "d".to_owned(),
            wing: "wing_conflict".to_owned(),
            idempotency_key: "e2e-conflict-1".to_owned(),
            created_by: None,
            parent_id: None,
            dependencies: Vec::new(),
            budget: None,
            expires_at: None,
        })
        .await
        .unwrap();

    // `default_mode: Combined` — coordination federation must be configured for the ID
    // fallback to run at all (see
    // `coordination_fallback_records_zero_remote_calls_without_coordination_federation_config`
    // in `crates/mempalace-mcp/src/federation.rs`).
    let server = mcp_server_with_hub_coordination(
        &local_dir,
        &hub_url,
        BTreeMap::new(),
        BTreeMap::new(),
        RouteMode::Combined,
    )
    .await;

    // First claim, at the correct revision 0, is discovered on the hub and succeeds.
    let claimed = call_tool(
        &server,
        1,
        "mempalace_task_claim",
        json!({
            "task_id": hub_task.task_id,
            "worker": "worker-1",
            "expected_revision": 0,
            "lease_seconds": 300,
        }),
    )
    .await;
    assert_eq!(
        claimed["success"], true,
        "first claim at the right revision must succeed: {claimed}"
    );
    assert_eq!(claimed["applied_to"], "remote:hub");

    // Renewing at the now-stale revision 0 must come back as a typed conflict, not an error.
    let conflict = call_tool(
        &server,
        2,
        "mempalace_task_renew",
        json!({
            "task_id": hub_task.task_id,
            "worker": "worker-1",
            "expected_revision": 0,
            "lease_seconds": 300,
        }),
    )
    .await;
    assert_eq!(conflict["success"], false, "stale revision must report success=false: {conflict}");
    assert_eq!(
        conflict["conflict"]["actual_revision"], 1,
        "conflict must carry the hub's real current revision: {conflict}"
    );
}

/// The coordination-events feed fans out to every coordination *candidate* remote
/// independently — a down candidate is reported as unreachable, while a healthy one alongside it
/// still returns its events, matching the `{unreachable, error}` isolation contract
/// `changes_fanout` already guarantees for the generic change feed.
///
/// Coordination federation must be explicitly configured for the fan-out to run at all (see
/// `FederationRouter::coordination_federation_enabled` and the aggregate-fan-out gate inside
/// `coordination_events_fanout`/`coordination_inbox_fanout` themselves) — this test used to pass
/// `RouteMode::Local` with an empty `coordination_rules` table here, which is precisely the
/// "coordination federates with zero coordination configuration" shape that gate now closes, so
/// it stopped exercising the isolation contract this test exists to check (both `hub` and `down`
/// started reporting nothing at all). Pinning `wing_fanout` to `remote` in `coordination_rules`
/// opts coordination in explicitly, the way a real operator would, while leaving the isolation
/// behaviour under test unchanged.
///
/// PR #120 review, finding 1(a): the fan-outs were later narrowed to query only
/// `coordination_candidate_remotes()`, not every configured remote — a remote never named by any
/// `federation.coordination` rule (e.g. one wired up only for drawer/KG federation) is now
/// skipped entirely rather than probed. `down` must therefore be named by a coordination rule of
/// its own (`wing_fanout_down`, unrelated to the wing actually queried below — the candidate set
/// is the union across every wing's rule, not just the requested one) for this test to still
/// exercise "a down *candidate* is isolated, not the whole call" rather than accidentally
/// degenerating into "a non-candidate is never contacted", which is
/// `inbox_read_and_coordination_events_fanout_only_contact_the_coordination_candidate`'s job in
/// `crates/mempalace-mcp/src/lib.rs`, not this test's.
#[tokio::test]
async fn coordination_events_fanout_with_one_remote_down_still_returns_the_healthy_one() {
    let local_dir = TempDir::new().unwrap();
    let hub_dir = TempDir::new().unwrap();
    let hub_addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{hub_addr}");

    // A dead address for the second remote — nothing is listening.
    let dead_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_addr = dead_listener.local_addr().unwrap();
    drop(dead_listener);
    let down_url = format!("http://{dead_addr}");

    let hub_client = RemoteClient::new(RemoteEndpoint {
        name: "hub".to_owned(),
        base_url: hub_url.clone(),
        token: Some(TEST_TOKEN.to_owned()),
        timeout: Duration::from_secs(5),
    })
    .unwrap();
    hub_client
        .coordination_task_create(mempalace_federation::NewTaskRequest {
            title: "fanout test".to_owned(),
            description: "d".to_owned(),
            wing: "wing_fanout".to_owned(),
            idempotency_key: "e2e-fanout-1".to_owned(),
            created_by: None,
            parent_id: None,
            dependencies: Vec::new(),
            budget: None,
            expires_at: None,
        })
        .await
        .unwrap();

    let mut coordination_rules = BTreeMap::new();
    coordination_rules.insert(
        "wing_fanout".to_owned(),
        ResolvedRouteRule {
            mode: RouteMode::Remote,
            remote: Some("hub".to_owned()),
            write: WriteTarget::Remote,
        },
    );
    // `down` must also be a coordination candidate (named by some wing's rule — any wing,
    // since the candidate set is the union across all of them) or the aggregate fan-outs'
    // candidate-narrowing fix (PR #120 review, finding 1a) skips it entirely, and this test
    // would stop exercising "a down candidate is isolated" at all.
    coordination_rules.insert(
        "wing_fanout_down".to_owned(),
        ResolvedRouteRule {
            mode: RouteMode::Remote,
            remote: Some("down".to_owned()),
            write: WriteTarget::Remote,
        },
    );
    let server = mcp_server_with_hub_multi(
        &local_dir,
        &[("hub", &hub_url), ("down", &down_url)],
        BTreeMap::new(),
        coordination_rules,
        RouteMode::Local,
    )
    .await;

    let response =
        call_tool(&server, 1, "mempalace_coordination_events", json!({"wing": "wing_fanout"}))
            .await;

    let remote_events = response.get("remote_events").expect("remote_events must be present");
    assert_eq!(
        remote_events["down"]["unreachable"], true,
        "the down remote must be reported unreachable, not fail the whole call: {remote_events}"
    );
    let hub_events = remote_events["hub"]["events"].as_array().expect("hub must return events");
    assert!(
        !hub_events.is_empty(),
        "the healthy remote must still return its events despite the other being down: {remote_events}"
    );
    assert!(
        hub_events.iter().all(|e| e["origin"] == "remote:hub"),
        "hub events must be annotated with origin: {hub_events:?}"
    );
}

/// Regression for Codex finding 3832912235: `mempalace_coordination_events` reads a per-remote
/// `remote_cursors` argument (`parse_cursors_arg`) to continue a federated fan-out page, but
/// until now its `input_schema` never declared the field — a schema-driven client had no way to
/// send it back, so every call restarted the hub's paging from the beginning. This proves the
/// round trip actually works end to end, not just that the schema mentions the field: a real
/// `next_cursor` taken out of page 1 is fed back as `remote_cursors.hub` and must yield a
/// disjoint page 2, not a repeat of page 1.
#[tokio::test]
async fn coordination_events_remote_cursors_round_trip_paginates_without_repeats() {
    let local_dir = TempDir::new().unwrap();
    let hub_dir = TempDir::new().unwrap();
    let hub_addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{hub_addr}");

    let hub_client = RemoteClient::new(RemoteEndpoint {
        name: "hub".to_owned(),
        base_url: hub_url.clone(),
        token: Some(TEST_TOKEN.to_owned()),
        timeout: Duration::from_secs(5),
    })
    .unwrap();
    let hub_task = hub_client
        .coordination_task_create(mempalace_federation::NewTaskRequest {
            title: "events cursor pagination".to_owned(),
            description: "d".to_owned(),
            wing: "wing_events_pages".to_owned(),
            idempotency_key: "e2e-events-pages-task".to_owned(),
            created_by: None,
            parent_id: None,
            dependencies: Vec::new(),
            budget: None,
            expires_at: None,
        })
        .await
        .unwrap();
    // Task creation already emitted one event; two more messages bring the total to 3, so a
    // limit=2 first page leaves exactly one event for the second page.
    for i in 0..2 {
        hub_client
            .coordination_message_send(mempalace_federation::NewMessageRequest {
                task_id: hub_task.task_id.clone(),
                recipient: "worker-1".to_owned(),
                kind: "status".to_owned(),
                payload: json!({"i": i}),
                idempotency_key: format!("e2e-events-pages-message-{i}"),
                sender: None,
                envelope_version: 1,
            })
            .await
            .unwrap();
    }

    let mut coordination_rules = BTreeMap::new();
    coordination_rules.insert(
        "wing_events_pages".to_owned(),
        ResolvedRouteRule {
            mode: RouteMode::Remote,
            remote: Some("hub".to_owned()),
            write: WriteTarget::Remote,
        },
    );
    let server = mcp_server_with_hub_coordination(
        &local_dir,
        &hub_url,
        BTreeMap::new(),
        coordination_rules,
        RouteMode::Local,
    )
    .await;

    let page1 = call_tool(
        &server,
        1,
        "mempalace_coordination_events",
        json!({"wing": "wing_events_pages", "limit": 2}),
    )
    .await;
    let page1_remote = page1.get("remote_events").expect("remote_events must be present on page 1");
    let page1_events =
        page1_remote["hub"]["events"].as_array().expect("hub must return events on page 1");
    assert_eq!(page1_events.len(), 2, "first page must return exactly 2 events: {page1_events:?}");
    let page1_ids: Vec<&str> = page1_events.iter().filter_map(|e| e["event_id"].as_str()).collect();
    let hub_cursor1 = page1_remote["hub"]["next_cursor"]
        .as_str()
        .expect("remote_events.hub.next_cursor must be a string when more events remain")
        .to_owned();

    let page2 = call_tool(
        &server,
        2,
        "mempalace_coordination_events",
        json!({
            "wing": "wing_events_pages",
            "limit": 2,
            "remote_cursors": {"hub": hub_cursor1},
        }),
    )
    .await;
    let page2_remote = page2.get("remote_events").expect("remote_events must be present on page 2");
    let page2_events =
        page2_remote["hub"]["events"].as_array().expect("hub must return events on page 2");
    assert_eq!(
        page2_events.len(),
        1,
        "second page must return the one remaining event: {page2_events:?}"
    );
    let page2_ids: Vec<&str> = page2_events.iter().filter_map(|e| e["event_id"].as_str()).collect();
    assert_ne!(
        page1_ids, page2_ids,
        "feeding remote_cursors back must not repeat the first page's events"
    );
    for id in &page2_ids {
        assert!(
            !page1_ids.contains(id),
            "event {id} appeared on both pages — remote_cursors round trip did not advance"
        );
    }
    assert!(
        page2_remote["hub"]["next_cursor"].is_null(),
        "no events remain after page 2: {page2_remote}"
    );
}

/// Companion to the events round trip above, for `mempalace_inbox_read`'s equally undeclared
/// `remote_cursors` field. Two messages addressed to the same recipient on the hub; a limit=1
/// first page followed by the real `next_cursor` fed back must reach the second, different
/// message rather than repeating the first.
#[tokio::test]
async fn coordination_inbox_remote_cursors_round_trip_paginates_without_repeats() {
    let local_dir = TempDir::new().unwrap();
    let hub_dir = TempDir::new().unwrap();
    let hub_addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{hub_addr}");

    let hub_client = RemoteClient::new(RemoteEndpoint {
        name: "hub".to_owned(),
        base_url: hub_url.clone(),
        token: Some(TEST_TOKEN.to_owned()),
        timeout: Duration::from_secs(5),
    })
    .unwrap();
    let hub_task = hub_client
        .coordination_task_create(mempalace_federation::NewTaskRequest {
            title: "inbox cursor pagination".to_owned(),
            description: "d".to_owned(),
            wing: "wing_inbox_pages".to_owned(),
            idempotency_key: "e2e-inbox-pages-task".to_owned(),
            created_by: None,
            parent_id: None,
            dependencies: Vec::new(),
            budget: None,
            expires_at: None,
        })
        .await
        .unwrap();
    for i in 0..2 {
        hub_client
            .coordination_message_send(mempalace_federation::NewMessageRequest {
                task_id: hub_task.task_id.clone(),
                recipient: "worker-1".to_owned(),
                kind: "status".to_owned(),
                payload: json!({"i": i}),
                idempotency_key: format!("e2e-inbox-pages-message-{i}"),
                sender: None,
                envelope_version: 1,
            })
            .await
            .unwrap();
    }

    let mut coordination_rules = BTreeMap::new();
    coordination_rules.insert(
        "wing_inbox_pages".to_owned(),
        ResolvedRouteRule {
            mode: RouteMode::Remote,
            remote: Some("hub".to_owned()),
            write: WriteTarget::Remote,
        },
    );
    let server = mcp_server_with_hub_coordination(
        &local_dir,
        &hub_url,
        BTreeMap::new(),
        coordination_rules,
        RouteMode::Local,
    )
    .await;

    let page1 = call_tool(
        &server,
        1,
        "mempalace_inbox_read",
        json!({"recipient": "worker-1", "wing": "wing_inbox_pages", "limit": 1}),
    )
    .await;
    let page1_remote =
        page1.get("remote_messages").expect("remote_messages must be present on page 1");
    let page1_messages =
        page1_remote["hub"]["messages"].as_array().expect("hub must return messages on page 1");
    assert_eq!(
        page1_messages.len(),
        1,
        "first page must return exactly 1 message: {page1_messages:?}"
    );
    let page1_ids: Vec<&str> =
        page1_messages.iter().filter_map(|m| m["message_id"].as_str()).collect();
    let hub_cursor1 = page1_remote["hub"]["next_cursor"]
        .as_str()
        .expect("remote_messages.hub.next_cursor must be a string when more messages remain")
        .to_owned();

    let page2 = call_tool(
        &server,
        2,
        "mempalace_inbox_read",
        json!({
            "recipient": "worker-1",
            "wing": "wing_inbox_pages",
            "limit": 1,
            "remote_cursors": {"hub": hub_cursor1},
        }),
    )
    .await;
    let page2_remote =
        page2.get("remote_messages").expect("remote_messages must be present on page 2");
    let page2_messages =
        page2_remote["hub"]["messages"].as_array().expect("hub must return messages on page 2");
    assert_eq!(
        page2_messages.len(),
        1,
        "second page must return the one remaining message: {page2_messages:?}"
    );
    let page2_ids: Vec<&str> =
        page2_messages.iter().filter_map(|m| m["message_id"].as_str()).collect();
    assert_ne!(
        page1_ids, page2_ids,
        "feeding remote_cursors back must not repeat the first page's message"
    );
    assert!(
        page2_remote["hub"]["next_cursor"].is_null(),
        "no messages remain after page 2: {page2_remote}"
    );
}

// ─── Six ID-discovery fallback wrappers: field-plumbing coverage (PR #120 review) ─────
//
// `coordination_task_create`/`_get`/`_claim`, `coordination_events`/`_inbox` fanouts each have
// e2e coverage above, but `mempalace_message_send`, `mempalace_message_acknowledge`,
// `mempalace_artifact_put`, `mempalace_artifact_get`, `mempalace_result_put` and
// `mempalace_result_get` do not — the unit tests in `federation.rs` exercise the *shared*
// fallback-loop logic (error classification, candidate narrowing) generically, but nothing pins
// these six thin wrappers' own field plumbing, e.g. that `coordination_message_ack_fallback`
// forwards `actor` into `AckMessageRequest` correctly, or that `artifact_put`/`result_put`
// forward their request bodies unchanged. Each test below asserts on the *values* the hub
// actually stored and returned, not merely that the call succeeded — a wiring bug that dropped
// or swapped a field would still produce `"success"`/`"found": true` in every case, so a
// call-happened assertion would not catch it.

/// `mempalace_message_send` falls back to the hub when the referenced task exists only there,
/// and must forward `recipient` (stored verbatim) and `sender` (identity-resolved) unchanged —
/// a dropped `sender` would silently resolve to the bare token identity instead of
/// `{identity}:{claim}`, which this test would catch.
#[tokio::test]
async fn coordination_message_send_fallback_forwards_recipient_and_sender_fields() {
    let local_dir = TempDir::new().unwrap();
    let hub_dir = TempDir::new().unwrap();
    let hub_addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{hub_addr}");

    let hub_client = RemoteClient::new(RemoteEndpoint {
        name: "hub".to_owned(),
        base_url: hub_url.clone(),
        token: Some(TEST_TOKEN.to_owned()),
        timeout: Duration::from_secs(5),
    })
    .unwrap();
    let hub_task = hub_client
        .coordination_task_create(mempalace_federation::NewTaskRequest {
            title: "message send fallback".to_owned(),
            description: "d".to_owned(),
            wing: "wing_msgsend".to_owned(),
            idempotency_key: "e2e-msgsend-task".to_owned(),
            created_by: None,
            parent_id: None,
            dependencies: Vec::new(),
            budget: None,
            expires_at: None,
        })
        .await
        .unwrap();

    let server = mcp_server_with_hub_coordination(
        &local_dir,
        &hub_url,
        BTreeMap::new(),
        BTreeMap::new(),
        RouteMode::Combined,
    )
    .await;

    let response = call_tool(
        &server,
        1,
        "mempalace_message_send",
        json!({
            "task_id": hub_task.task_id,
            "sender": "alice",
            "recipient": "worker-9",
            "kind": "status",
            "payload": {"note": "hello from fallback"},
            "idempotency_key": "e2e-msgsend-1",
        }),
    )
    .await;

    assert_eq!(
        response["recipient"], "worker-9",
        "recipient must be forwarded verbatim through the fallback: {response}"
    );
    assert_eq!(
        response["sender"], "e2e-fed-user:alice",
        "sender must be identity-resolved from the claimed `alice`, not dropped or defaulted \
         to the bare token identity: {response}"
    );
    assert_eq!(response["payload"]["note"], "hello from fallback");
    assert_eq!(response["kind"], "status");

    // Read it back from the hub directly to confirm the fallback actually persisted what it
    // claims to have returned, not just echoed a locally-fabricated value.
    let stored = hub_client
        .coordination_message_get(response["message_id"].as_str().unwrap())
        .await
        .unwrap();
    assert_eq!(stored.recipient, "worker-9");
    assert_eq!(stored.sender, "e2e-fed-user:alice");
}

/// `mempalace_message_acknowledge` falls back to the hub for a message that exists only there,
/// and must forward `actor` through `resolve_ack_actor` correctly: an actor claim equal to the
/// message's own `recipient` must be stored bare, not identity-prefixed — proving
/// `coordination_message_ack_fallback` actually carries the caller's `actor` argument into
/// `AckMessageRequest`, rather than e.g. leaving it `None` (which would resolve to the bare
/// token identity, not `worker-9`, and this test would catch that divergence).
#[tokio::test]
async fn coordination_message_ack_fallback_forwards_actor_field() {
    let local_dir = TempDir::new().unwrap();
    let hub_dir = TempDir::new().unwrap();
    let hub_addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{hub_addr}");

    let hub_client = RemoteClient::new(RemoteEndpoint {
        name: "hub".to_owned(),
        base_url: hub_url.clone(),
        token: Some(TEST_TOKEN.to_owned()),
        timeout: Duration::from_secs(5),
    })
    .unwrap();
    let hub_task = hub_client
        .coordination_task_create(mempalace_federation::NewTaskRequest {
            title: "message ack fallback".to_owned(),
            description: "d".to_owned(),
            wing: "wing_msgack".to_owned(),
            idempotency_key: "e2e-msgack-task".to_owned(),
            created_by: None,
            parent_id: None,
            dependencies: Vec::new(),
            budget: None,
            expires_at: None,
        })
        .await
        .unwrap();
    let hub_message = hub_client
        .coordination_message_send(mempalace_federation::NewMessageRequest {
            task_id: hub_task.task_id.clone(),
            recipient: "worker-9".to_owned(),
            kind: "status".to_owned(),
            payload: json!({}),
            idempotency_key: "e2e-msgack-msg".to_owned(),
            sender: None,
            envelope_version: 1,
        })
        .await
        .unwrap();

    let server = mcp_server_with_hub_coordination(
        &local_dir,
        &hub_url,
        BTreeMap::new(),
        BTreeMap::new(),
        RouteMode::Combined,
    )
    .await;

    let response = call_tool(
        &server,
        1,
        "mempalace_message_acknowledge",
        json!({"message_id": hub_message.message_id, "actor": "worker-9"}),
    )
    .await;

    assert_eq!(
        response["acknowledged_by"], "worker-9",
        "the acknowledging actor must be forwarded and stored bare (it equals the message's \
         own recipient), not dropped or identity-prefixed: {response}"
    );
    assert!(
        response["acknowledged_at"].is_string(),
        "acknowledgement must record a timestamp: {response}"
    );

    let stored = hub_client.coordination_message_get(&hub_message.message_id).await.unwrap();
    assert_eq!(stored.acknowledged_by.as_deref(), Some("worker-9"));
}

/// `mempalace_artifact_put` falls back to the hub when the referenced task exists only there,
/// and must forward `role`, `media_type` and `content` unchanged.
#[tokio::test]
async fn coordination_artifact_put_fallback_forwards_request_body() {
    let local_dir = TempDir::new().unwrap();
    let hub_dir = TempDir::new().unwrap();
    let hub_addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{hub_addr}");

    let hub_client = RemoteClient::new(RemoteEndpoint {
        name: "hub".to_owned(),
        base_url: hub_url.clone(),
        token: Some(TEST_TOKEN.to_owned()),
        timeout: Duration::from_secs(5),
    })
    .unwrap();
    let hub_task = hub_client
        .coordination_task_create(mempalace_federation::NewTaskRequest {
            title: "artifact put fallback".to_owned(),
            description: "d".to_owned(),
            wing: "wing_artput".to_owned(),
            idempotency_key: "e2e-artput-task".to_owned(),
            created_by: None,
            parent_id: None,
            dependencies: Vec::new(),
            budget: None,
            expires_at: None,
        })
        .await
        .unwrap();

    let server = mcp_server_with_hub_coordination(
        &local_dir,
        &hub_url,
        BTreeMap::new(),
        BTreeMap::new(),
        RouteMode::Combined,
    )
    .await;

    let response = call_tool(
        &server,
        1,
        "mempalace_artifact_put",
        json!({
            "task_id": hub_task.task_id,
            "created_by": "alice",
            "role": "output",
            "media_type": "text/plain",
            "content": "the artifact body",
            "idempotency_key": "e2e-artput-1",
        }),
    )
    .await;

    assert_eq!(response["role"], "output", "role must be forwarded unchanged: {response}");
    assert_eq!(
        response["media_type"], "text/plain",
        "media_type must be forwarded unchanged: {response}"
    );
    assert_eq!(
        response["content"], "the artifact body",
        "content must be forwarded unchanged: {response}"
    );
    assert_eq!(response["created_by"], "e2e-fed-user:alice");

    let stored = hub_client
        .coordination_artifact_get(response["artifact_id"].as_str().unwrap())
        .await
        .unwrap();
    assert_eq!(stored.content, "the artifact body");
    assert_eq!(stored.role, "output");
    assert_eq!(stored.media_type, "text/plain");
}

/// `mempalace_artifact_get` falls back to the hub for an artifact that exists only there, and
/// must return its actual field values (not just `found: true`).
#[tokio::test]
async fn coordination_artifact_get_fallback_returns_correct_fields() {
    let local_dir = TempDir::new().unwrap();
    let hub_dir = TempDir::new().unwrap();
    let hub_addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{hub_addr}");

    let hub_client = RemoteClient::new(RemoteEndpoint {
        name: "hub".to_owned(),
        base_url: hub_url.clone(),
        token: Some(TEST_TOKEN.to_owned()),
        timeout: Duration::from_secs(5),
    })
    .unwrap();
    let hub_task = hub_client
        .coordination_task_create(mempalace_federation::NewTaskRequest {
            title: "artifact get fallback".to_owned(),
            description: "d".to_owned(),
            wing: "wing_artget".to_owned(),
            idempotency_key: "e2e-artget-task".to_owned(),
            created_by: None,
            parent_id: None,
            dependencies: Vec::new(),
            budget: None,
            expires_at: None,
        })
        .await
        .unwrap();
    let hub_artifact = hub_client
        .coordination_artifact_put(mempalace_federation::NewArtifactRequest {
            task_id: hub_task.task_id.clone(),
            role: "log".to_owned(),
            media_type: "application/json".to_owned(),
            content: r#"{"k":"v"}"#.to_owned(),
            idempotency_key: "e2e-artget-artifact".to_owned(),
            created_by: None,
        })
        .await
        .unwrap();

    let server = mcp_server_with_hub_coordination(
        &local_dir,
        &hub_url,
        BTreeMap::new(),
        BTreeMap::new(),
        RouteMode::Combined,
    )
    .await;

    let response = call_tool(
        &server,
        1,
        "mempalace_artifact_get",
        json!({"artifact_id": hub_artifact.artifact_id}),
    )
    .await;

    assert_eq!(response["found"], true, "must find the artifact via remote fallback: {response}");
    assert_eq!(response["value"]["role"], "log");
    assert_eq!(response["value"]["media_type"], "application/json");
    assert_eq!(response["value"]["content"], r#"{"k":"v"}"#);
    assert_eq!(response["value"]["origin"], "remote:hub");
}

/// `mempalace_result_put` falls back to the hub when the referenced task exists only there, and
/// must forward the (nested) `payload` body unchanged.
#[tokio::test]
async fn coordination_result_put_fallback_forwards_payload() {
    let local_dir = TempDir::new().unwrap();
    let hub_dir = TempDir::new().unwrap();
    let hub_addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{hub_addr}");

    let hub_client = RemoteClient::new(RemoteEndpoint {
        name: "hub".to_owned(),
        base_url: hub_url.clone(),
        token: Some(TEST_TOKEN.to_owned()),
        timeout: Duration::from_secs(5),
    })
    .unwrap();
    let hub_task = hub_client
        .coordination_task_create(mempalace_federation::NewTaskRequest {
            title: "result put fallback".to_owned(),
            description: "d".to_owned(),
            wing: "wing_resput".to_owned(),
            idempotency_key: "e2e-resput-task".to_owned(),
            created_by: None,
            parent_id: None,
            dependencies: Vec::new(),
            budget: None,
            expires_at: None,
        })
        .await
        .unwrap();

    let server = mcp_server_with_hub_coordination(
        &local_dir,
        &hub_url,
        BTreeMap::new(),
        BTreeMap::new(),
        RouteMode::Combined,
    )
    .await;

    let response = call_tool(
        &server,
        1,
        "mempalace_result_put",
        json!({
            "task_id": hub_task.task_id,
            "created_by": "alice",
            "payload": {"status": "ok", "nested": {"count": 3}},
            "idempotency_key": "e2e-resput-1",
        }),
    )
    .await;

    assert_eq!(
        response["payload"]["status"], "ok",
        "payload must be forwarded unchanged: {response}"
    );
    assert_eq!(
        response["payload"]["nested"]["count"], 3,
        "nested payload fields must survive the fallback unchanged: {response}"
    );
    assert_eq!(response["created_by"], "e2e-fed-user:alice");

    let stored =
        hub_client.coordination_result_get(response["result_id"].as_str().unwrap()).await.unwrap();
    assert_eq!(stored.payload["status"], "ok");
    assert_eq!(stored.payload["nested"]["count"], 3);
}

/// `mempalace_result_get` falls back to the hub for a result that exists only there, and must
/// return its actual payload (not just `found: true`).
#[tokio::test]
async fn coordination_result_get_fallback_returns_correct_payload() {
    let local_dir = TempDir::new().unwrap();
    let hub_dir = TempDir::new().unwrap();
    let hub_addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{hub_addr}");

    let hub_client = RemoteClient::new(RemoteEndpoint {
        name: "hub".to_owned(),
        base_url: hub_url.clone(),
        token: Some(TEST_TOKEN.to_owned()),
        timeout: Duration::from_secs(5),
    })
    .unwrap();
    let hub_task = hub_client
        .coordination_task_create(mempalace_federation::NewTaskRequest {
            title: "result get fallback".to_owned(),
            description: "d".to_owned(),
            wing: "wing_resget".to_owned(),
            idempotency_key: "e2e-resget-task".to_owned(),
            created_by: None,
            parent_id: None,
            dependencies: Vec::new(),
            budget: None,
            expires_at: None,
        })
        .await
        .unwrap();
    let hub_result = hub_client
        .coordination_result_put(mempalace_federation::NewTaskResultRequest {
            task_id: hub_task.task_id.clone(),
            payload: json!({"status": "done", "value": 42}),
            idempotency_key: "e2e-resget-result".to_owned(),
            created_by: None,
        })
        .await
        .unwrap();

    let server = mcp_server_with_hub_coordination(
        &local_dir,
        &hub_url,
        BTreeMap::new(),
        BTreeMap::new(),
        RouteMode::Combined,
    )
    .await;

    let response =
        call_tool(&server, 1, "mempalace_result_get", json!({"result_id": hub_result.result_id}))
            .await;

    assert_eq!(response["found"], true, "must find the result via remote fallback: {response}");
    assert_eq!(response["value"]["payload"]["status"], "done");
    assert_eq!(response["value"]["payload"]["value"], 42);
    assert_eq!(response["value"]["origin"], "remote:hub");
}

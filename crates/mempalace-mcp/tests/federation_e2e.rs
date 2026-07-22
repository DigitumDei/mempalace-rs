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
use mempalace_server::{TokenRegistry, build_router};
use serde_json::{Value, json};
use mempalace_remote::{RemoteApi, RemoteClient, RemoteEndpoint};
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
    let response = call_tool(
        &server,
        1,
        "mempalace_kg_query",
        json!({"entity": "unknown federation entity"}),
    )
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

    let server = mcp_server_with_hub(
        &local_dir,
        &hub_url,
        wing_rules,
        RouteMode::Local,
        None,
    )
    .await;

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
    assert_eq!(
        add_shared["origin"], "hub",
        "add_shared must report origin=hub; got: {add_shared}"
    );
    assert_eq!(
        add_shared["applied_to"], "remote:hub",
        "add_shared must report applied_to=remote:hub; got: {add_shared}"
    );
    let remote_drawer_id = add_shared["drawer_id"]
        .as_str()
        .expect("add_shared must return drawer_id")
        .to_owned();

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

    let has_local =
        all_results.iter().any(|r| r["origin"].as_str() == Some("local") || r.get("origin").is_none());
    let has_hub = all_results.iter().any(|r| r["origin"].as_str() == Some("hub"));
    assert!(has_local, "global search must include local result; results: {all_results:?}");
    assert!(has_hub, "global search must include hub result; results: {all_results:?}");

    // ── 5. Delete remote drawer → local miss, remote fallback deletes ────────
    let delete_resp = call_tool(
        &server,
        5,
        "mempalace_delete_drawer",
        json!({"drawer_id": remote_drawer_id}),
    )
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

    let origins: Vec<&str> =
        global_results.iter().filter_map(|r| r["origin"].as_str()).collect();
    let has_hub = origins.iter().any(|&o| o == "hub");
    let has_local = origins.iter().any(|&o| o == "local");
    // The key assertion: both origins are present in the combined result set,
    // even though each side ranks with a different embedding profile.
    assert!(
        has_hub,
        "global search must include hub origin; origins: {origins:?}"
    );
    assert!(
        has_local,
        "global search must include local origin; origins: {origins:?}"
    );
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
    assert_eq!(
        kg_add["applied_to"], "local",
        "kg_add must report applied_to=local: {kg_add}"
    );

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

    // mempalace_status → federation.remotes[].reachable must be false.
    let status = call_tool(&server, 4, "mempalace_status", json!({})).await;
    let remotes_info = status["federation"]["remotes"]
        .as_array()
        .expect("status must include federation.remotes");
    assert!(
        !remotes_info.is_empty(),
        "status must list at least one remote: {status}"
    );
    let hub_entry = remotes_info.iter().find(|r| r["name"].as_str() == Some("hub"));
    assert!(hub_entry.is_some(), "status must list hub remote: {remotes_info:?}");
    assert_eq!(
        hub_entry.unwrap()["reachable"], false,
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

    // ── Local delete_drawer must report applied_to=local ─────────────────────
    let local_delete = call_tool(
        &server,
        6,
        "mempalace_delete_drawer",
        json!({"drawer_id": local_add["drawer_id"]}),
    )
    .await;
    assert_eq!(
        local_delete["success"], true,
        "local delete must succeed: {local_delete}"
    );
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

    let server = mcp_server_with_hub(
        &local_dir,
        &hub_url,
        wing_rules,
        RouteMode::Local,
        None,
    )
    .await;

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
    assert_eq!(
        add_diary["success"], true,
        "diary add must succeed locally; got: {add_diary}"
    );

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

    let server = mcp_server_with_hub(
        &local_dir,
        &hub_url,
        wing_rules,
        RouteMode::Local,
        None,
    )
    .await;

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

    let wing_availability = status.get("wing_availability").expect(
        "status with federation must include wing_availability",
    );

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
    let wings_avail = list_wings.get("wing_availability").expect(
        "list_wings with federation must include wing_availability",
    );
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

    let server = mcp_server_with_hub(
        &local_dir,
        &hub_url,
        wing_rules,
        RouteMode::Local,
        None,
    )
    .await;

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
    let remote_changes = wake
        .get("remote_changes")
        .expect("wake_up with federation must include remote_changes");
    let hub_changes = remote_changes
        .get("hub")
        .expect("remote_changes must include 'hub' entry");

    // Must not be an unreachable marker.
    assert!(
        hub_changes.get("unreachable").is_none() || hub_changes["unreachable"] == false,
        "hub must be reachable in wake_up: {hub_changes}"
    );

    let events = hub_changes["events"]
        .as_array()
        .expect("hub remote_changes must have events array");
    assert!(
        !events.is_empty(),
        "hub remote_changes.events must be non-empty: {hub_changes}"
    );

    // Every event must carry origin == "remote:hub".
    for event in events {
        assert_eq!(
            event["origin"].as_str(),
            Some("remote:hub"),
            "every hub remote_change event must have origin=remote:hub; event: {event}"
        );
    }

    // Both seeded entity ids must appear.
    let entity_ids: Vec<&str> = events
        .iter()
        .filter_map(|e| e["entity_id"].as_str())
        .collect();
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
    let wake = call_tool(
        &server,
        1,
        "mempalace_wake_up",
        json!({"agent_name": "down-test"}),
    )
    .await;

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
    let hub_changes = remote_changes
        .get("hub")
        .expect("remote_changes must include 'hub' entry");

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

    let server = mcp_server_with_hub(
        &local_dir,
        &hub_url,
        wing_rules,
        RouteMode::Local,
        None,
    )
    .await;

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
        page1_remote.len(), 2,
        "first page must return exactly 2 remote-origin events; events: {page1_events:?}"
    );

    // Verify ascending occurred_at order on page 1.
    for pair in page1_remote.windows(2) {
        let t0 = pair[0]["occurred_at"].as_str().unwrap_or("");
        let t1 = pair[1]["occurred_at"].as_str().unwrap_or("");
        assert!(
            t0 <= t1,
            "events must be ascending by occurred_at; got {t0} then {t1}"
        );
    }

    // remotes.hub.next_cursor must be a non-null string.
    let remotes_meta = page1.get("remotes").expect("page1 must include remotes meta");
    let hub_cursor1 = remotes_meta["hub"]["next_cursor"]
        .as_str()
        .expect("remotes.hub.next_cursor must be a string on page 1");

    let page1_entity_ids: Vec<&str> = page1_remote
        .iter()
        .filter_map(|e| e["entity_id"].as_str())
        .collect();

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
    let page2_entity_ids: Vec<&str> = page2_remote
        .iter()
        .filter_map(|e| e["entity_id"].as_str())
        .collect();
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
        assert_eq!(
            diary_resp["success"], true,
            "hub diary_write must succeed: {diary_resp}"
        );
        // hub_mcp is dropped here — the palace engine is released before HTTP spawn.
    }

    // ── Step 2: spawn the HTTP server over the hub dir (now free of in-process engine).
    let addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{addr}");

    // ── Step 3: set up the federated MCP server.
    let mut wing_rules = BTreeMap::new();
    wing_rules.insert("wing_diary_filter".to_owned(), combined_wing_rule_remote_write());

    let server = mcp_server_with_hub(
        &local_dir,
        &hub_url,
        wing_rules,
        RouteMode::Local,
        None,
    )
    .await;

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
    assert_eq!(hub_add["applied_to"], "remote:hub", "hub drawer must report applied_to=remote:hub: {hub_add}");
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
    assert_eq!(
        local_diary["success"], true,
        "local diary_write must succeed: {local_diary}"
    );

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
        e["event_type"].as_str() == Some("diary_written")
            && e["origin"].as_str() == Some("local")
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

    let server = mcp_server_with_hub(
        &local_dir,
        &hub_url,
        wing_rules,
        RouteMode::Local,
        None,
    )
    .await;

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
    assert_eq!(hub_add["applied_to"], "remote:hub", "hub add must report applied_to=remote:hub: {hub_add}");

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

    let has_local = events
        .iter()
        .any(|e| e["origin"].as_str() == Some("local"));
    let has_remote_hub = events
        .iter()
        .any(|e| e["origin"].as_str() == Some("remote:hub"));

    assert!(
        has_local,
        "changes must include at least one local-origin event; events: {events:?}"
    );
    assert!(
        has_remote_hub,
        "changes must include at least one remote:hub-origin event; events: {events:?}"
    );

    // remotes meta: hub count must equal the number of hub-origin events in the list.
    let remotes_meta = changes.get("remotes").expect("federated changes must include remotes meta");
    let hub_count = remotes_meta["hub"]["count"]
        .as_u64()
        .expect("remotes.hub.count must be a number");
    let actual_hub_count = events
        .iter()
        .filter(|e| e["origin"].as_str() == Some("remote:hub"))
        .count() as u64;
    assert_eq!(
        hub_count, actual_hub_count,
        "remotes.hub.count must equal number of remote:hub events in events array"
    );


    // Total count field must equal total events length.
    let total_count = changes["count"].as_u64().expect("count must be a number");
    assert_eq!(
        total_count,
        events.len() as u64,
        "top-level count must equal events.len()"
    );
}

// ─── Dual-write (write:both) tests ─────────────────────────────────────────

// ─── Test 12: add_drawer_both_replicates_successfully ───────────────────────

/// Combined/write:Both wing rule → local write first, then best-effort remote
/// replication. Response must show `applied_to: "local"`, a `replication` object
/// with `status: "replicated"` and `remote: "hub"`, and no `origin` field
/// (primary write was local). The drawer must be reachable on the hub.
#[tokio::test]
async fn add_drawer_both_replicates_successfully() {
    let hub_dir = TempDir::new().unwrap();
    let local_dir = TempDir::new().unwrap();

    let addr = spawn_server(&hub_dir).await;
    let hub_url = format!("http://{addr}");

    let mut wing_rules = BTreeMap::new();
    wing_rules.insert("wing_both".to_owned(), combined_wing_rule_both_write());

    let server = mcp_server_with_hub(
        &local_dir,
        &hub_url,
        wing_rules,
        RouteMode::Local,
        None,
    )
    .await;

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
    // replication must show success.
    let replication = add.get("replication").expect("both add must include replication field");
    assert_eq!(
        replication["status"], "replicated",
        "replication must report status=replicated; got: {replication}"
    );
    assert_eq!(
        replication["remote"], "hub",
        "replication must report remote=hub; got: {replication}"
    );
    // No warnings on success.
    assert!(
        add.get("warnings").is_none(),
        "both add must not have warnings on success; got: {add}"
    );
    let _drawer_id = add["drawer_id"].as_str().unwrap().to_owned();

    // ── Verify the drawer landed on the hub directly ───────────────────────
    // Combined search deduplicates the local and replicated copies and prefers
    // local, so it cannot prove the remote write occurred.
    let hub_client = RemoteClient::new(RemoteEndpoint {
        name: "hub".into(),
        base_url: hub_url,
        token: Some(TEST_TOKEN.to_owned()),
        timeout: Duration::from_secs(5),
    })
    .unwrap();
    let hub_search = hub_client
        .search_drawers(DrawerSearchRequest {
            query: "dual-write e2e test drawer successful replication".to_owned(),
            wing: Some("wing_both".to_owned()),
            room: Some("both-room".to_owned()),
            limit: Some(5),
        })
        .await
        .expect("hub search must succeed");
    let hub_hit = hub_search.results.iter().any(|result| {
        result.content == "dual-write e2e test drawer successful replication"
    });
    assert!(
        hub_hit,
        "hub search must include the replicated drawer; results: {hub_search:?}"
    );
}

// ─── Test 22: add_drawer_both_replication_fails_with_remote_rejection ───────

/// Combined/write:Both wing rule with a reachable remote that rejects the
/// replication attempt (wrong bearer token → HTTP 401). The local write must
/// still succeed, `applied_to` must be `"local"`, and `replication.status`
/// must be `"failed"` with a non-empty `reason` and a `warnings` array.
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

    // Replication must be failed.
    let replication = add.get("replication").expect("both add must include replication field");
    assert_eq!(
        replication["status"], "failed",
        "replication must report status=failed with wrong token; got: {replication}"
    );
    assert_eq!(
        replication["remote"], "hub",
        "replication must report remote=hub; got: {replication}"
    );
    let reason = replication["reason"].as_str().unwrap_or("");
    assert!(
        !reason.is_empty(),
        "replication failure must include a non-empty reason; got: {replication}"
    );

    // Warnings must be present.
    let warnings = add.get("warnings").and_then(|w| w.as_array());
    assert!(
        warnings.is_some() && !warnings.unwrap().is_empty(),
        "both add with failed replication must include warnings array; got: {add}"
    );
}

// ─── Test 23: add_drawer_both_duplicate_replication ─────────────────────────

/// The content already exists on the hub (seeded via a Remote route).
/// A subsequent Both write for the same exact content must succeed locally but
/// report replication as converged (exact content already on remote).
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

    let server_a = mcp_server_with_hub(
        &local_dir_a,
        &hub_url,
        wing_rules_a,
        RouteMode::Local,
        None,
    )
    .await;

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

    // ── Server B: Both write with the same content ──────────────────────────
    let mut wing_rules_b = BTreeMap::new();
    wing_rules_b.insert("wing_both_dup".to_owned(), combined_wing_rule_both_write());

    let server_b = mcp_server_with_hub(
        &local_dir_b,
        &hub_url,
        wing_rules_b,
        RouteMode::Local,
        None,
    )
    .await;

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
    assert_eq!(
        dup["applied_to"], "local",
        "both add must report applied_to=local: {dup}"
    );

    // Replication must be converged (exact same content already on hub).
    let replication = dup.get("replication").expect("both add must include replication field");
    assert_eq!(
        replication["status"], "converged",
        "replication must report status=converged for exact duplicate content; got: {replication}"
    );
    assert_eq!(
        replication["remote"], "hub",
        "replication must report remote=hub; got: {replication}"
    );

    // No warnings for converged replication.
    assert!(
        dup.get("warnings").is_none(),
        "converged replication must not produce warnings; got: {dup}"
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

/// Dual-write retry regression:
///
/// 1. Start a real hub, then take it down.
/// 2. First write:both add while the hub is unavailable → local success,
///    replication fails with `status: "failed"`.
/// 3. Restore the hub on the same endpoint.
/// 4. Retry the identical add → existing local drawer is reused (same
///    `drawer_id`), replication reaches `status: "replicated"` **or**
///    `"converged"` (409/convergence is valid when the remote already has
///    the drawer), the remote contains the drawer, and no duplicate local
///    drawer is created.
///
/// Retain coverage that same-wing/room near-duplicates with different content
/// hashes remain rejected (Test 12b above).
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

    // ── Phase 1: Hub is down → replication must fail ─────────────────────────
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
    assert_eq!(
        first["applied_to"], "local",
        "first add must report applied_to=local: {first}"
    );
    let drawer_id = first["drawer_id"]
        .as_str()
        .expect("first add must return drawer_id")
        .to_owned();

    let replication = first.get("replication").expect("both add must include replication");
    assert_eq!(
        replication["status"], "failed",
        "replication must fail with down remote; got: {replication}"
    );
    let warnings = first.get("warnings").and_then(|w| w.as_array());
    assert!(
        warnings.is_some() && !warnings.unwrap().is_empty(),
        "failed replication must include warnings; got: {first}"
    );

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
    assert_eq!(
        retry["applied_to"], "local",
        "retry must report applied_to=local: {retry}"
    );
    assert_eq!(
        retry["drawer_id"].as_str().unwrap_or(""),
        drawer_id,
        "retry must reuse the same drawer_id (was {drawer_id}); got: {retry}"
    );

    let replication2 = retry.get("replication").expect("retry add must include replication");
    let status = replication2["status"].as_str().unwrap_or("");
    assert!(
        status == "replicated" || status == "converged",
        "retry replication must be 'replicated' or 'converged'; got: {replication2}"
    );
    assert_eq!(
        replication2["remote"], "hub",
        "retry replication must report remote=hub; got: {replication2}"
    );

    assert!(
        retry.get("warnings").is_none(),
        "retry must not have warnings on success; got: {retry}"
    );

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
    let local_only_server =
        McpServer::from_parts(local_only_config, DeterministicStubProvider::new(EmbeddingProfile::Balanced))
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

// ─── Test 13: add_drawer_both_replication_fails_with_down_remote ────────────

/// Combined/write:Both wing rule with the remote down (dead address).
/// The local write must still succeed, `applied_to` must be `"local"`, and
/// the `replication` field must show `status: "failed"` with a non-empty
/// `reason` and a `warnings` array.
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

    // Replication must be failed.
    let replication = add.get("replication").expect("both add must include replication field");
    assert_eq!(
        replication["status"], "failed",
        "replication must report status=failed with down remote; got: {replication}"
    );
    assert_eq!(
        replication["remote"], "hub",
        "replication must report remote=hub; got: {replication}"
    );
    let reason = replication["reason"].as_str().unwrap_or("");
    assert!(
        !reason.is_empty(),
        "replication failure must include a non-empty reason; got: {replication}"
    );

    // Warnings must be present.
    let warnings = add.get("warnings").and_then(|w| w.as_array());
    assert!(
        warnings.is_some() && !warnings.unwrap().is_empty(),
        "both add with failed replication must include warnings array; got: {add}"
    );
}

// ─── Test 14: kg_add_both_replicates_successfully ───────────────────────────

/// Combined KG rule with write:Both → local KG add succeeds, best-effort
/// replication succeeds. Response must have `applied_to: "local"`,
/// `replication.status: "replicated"`, `replication.remote: "hub"`.
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
    assert_eq!(
        kg_add["applied_to"], "local",
        "kg_add both must report applied_to=local: {kg_add}"
    );
    let replication = kg_add.get("replication").expect("kg_add both must include replication");
    assert_eq!(
        replication["status"], "replicated",
        "kg_add replication must be replicated; got: {replication}"
    );
    assert_eq!(
        replication["remote"], "hub",
        "kg_add replication must target hub; got: {replication}"
    );
    assert!(
        kg_add.get("warnings").is_none(),
        "kg_add must not have warnings on success: {kg_add}"
    );

    // Verify the fact exists on the hub directly. Combined KG queries dedupe
    // identical facts and prefer local, so no separate hub-origin fact remains.
    let hub_client = RemoteClient::new(RemoteEndpoint {
        name: "hub".into(),
        base_url: hub_url,
        token: Some(TEST_TOKEN.to_owned()),
        timeout: Duration::from_secs(5),
    })
    .unwrap();
    let hub_query = hub_client
        .kg_query(KgQueryRequest {
            entity: "BothTest".to_owned(),
            as_of: None,
            direction: Some("outgoing".to_owned()),
        })
        .await
        .expect("hub KG query must succeed");
    let hub_fact = hub_query["facts"].as_array().and_then(|facts| {
        facts.iter().find(|f| {
            f["predicate"].as_str() == Some("uses_protocol")
                && f["object"].as_str() == Some("dual_write_kg")
        })
    });
    assert!(
        hub_fact.is_some(),
        "hub KG query must include the replicated fact; response: {hub_query}"
    );
}

// ─── Test 15: kg_add_both_replication_fails_with_down_remote ────────────────

/// Combined KG rule with write:Both and a dead remote. Local KG add succeeds,
/// replication fails with `status: "failed"` and warnings.
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
    assert_eq!(
        kg_add["applied_to"], "local",
        "kg_add must report applied_to=local: {kg_add}"
    );
    let replication = kg_add.get("replication").expect("kg_add both must include replication");
    assert_eq!(
        replication["status"], "failed",
        "replication must be failed with down remote; got: {replication}"
    );
    assert_eq!(
        replication["remote"], "hub",
        "replication must target hub; got: {replication}"
    );
    let reason = replication["reason"].as_str().unwrap_or("");
    assert!(!reason.is_empty(), "failure reason must be non-empty; got: {replication}");

    let warnings = kg_add.get("warnings").and_then(|w| w.as_array());
    assert!(
        warnings.is_some() && !warnings.unwrap().is_empty(),
        "failed replication must include warnings; got: {kg_add}"
    );
}

// ─── Test 16: kg_invalidate_both_replicates_successfully ────────────────────

/// Combined KG rule with write:Both → local KG invalidate succeeds, best-effort
/// replication succeeds. Response must have `applied_to: "local"`,
/// `replication.status: "replicated"`.
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

    // Verify the fact exists on the hub before invalidation. Combined KG queries
    // dedupe replicated facts, so query the hub directly.
    let hub_client = RemoteClient::new(RemoteEndpoint {
        name: "hub".into(),
        base_url: hub_url.clone(),
        token: Some(TEST_TOKEN.to_owned()),
        timeout: Duration::from_secs(5),
    })
    .unwrap();
    let hub_before = hub_client
        .kg_query(KgQueryRequest {
            entity: "InvalidateBothTest".to_owned(),
            as_of: Some("2026-03-01".to_owned()),
            direction: Some("outgoing".to_owned()),
        })
        .await
        .expect("hub KG query before invalidation must succeed");
    let hub_fact_before = hub_before["facts"].as_array().and_then(|facts| {
        facts.iter().find(|f| {
            f["predicate"].as_str() == Some("replication")
                && f["object"].as_str() == Some("active")
        })
    });
    assert!(
        hub_fact_before.is_some(),
        "hub KG query before invalidation must include the fact; response: {hub_before}"
    );

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

    assert_eq!(
        invalidate["success"], true,
        "kg_invalidate both must succeed: {invalidate}"
    );
    assert_eq!(
        invalidate["applied_to"], "local",
        "kg_invalidate must report applied_to=local: {invalidate}"
    );
    let replication = invalidate.get("replication").expect("kg_invalidate must include replication");
    assert_eq!(
        replication["status"], "replicated",
        "invalidate replication must be replicated; got: {replication}"
    );
    assert_eq!(
        replication["remote"], "hub",
        "invalidate replication must target hub; got: {replication}"
    );
    assert!(
        invalidate.get("warnings").is_none(),
        "kg_invalidate must not have warnings on success: {invalidate}"
    );

    // ── Verify the fact is invalidated on the hub using as_of after ended ───
    let hub_after = hub_client
        .kg_query(KgQueryRequest {
            entity: "InvalidateBothTest".to_owned(),
            as_of: Some("2026-07-01".to_owned()),
            direction: Some("outgoing".to_owned()),
        })
        .await
        .expect("hub KG query after invalidation must succeed");
    let hub_fact = hub_after["facts"].as_array().and_then(|facts| {
        facts.iter().find(|f| {
            f["predicate"].as_str() == Some("replication")
                && f["object"].as_str() == Some("active")
        })
    });
    assert!(
        hub_fact.is_none(),
        "hub KG query after invalidation must not include the fact; response: {hub_after}"
    );

    // Query without as_of returns all facts including historical ones (the fact
    // should still exist as a historical record on the hub).
    let hub_all = hub_client
        .kg_query(KgQueryRequest {
            entity: "InvalidateBothTest".to_owned(),
            as_of: None,
            direction: Some("outgoing".to_owned()),
        })
        .await
        .expect("hub KG query without as_of must succeed");
    let facts_all = hub_all["facts"].as_array().expect("hub facts must be array");
    // The fact should still appear in the unfiltered query but with current: false.
    // We just verify the hub at least has the fact in the unfiltered view.
    let hub_fact_historical = facts_all.iter().find(|f| {
        f["predicate"].as_str() == Some("replication")
            && f["object"].as_str() == Some("active")
    });
    assert!(
        hub_fact_historical.is_some(),
        "hub KG query without as_of must show the historical fact; facts: {facts_all:?}"
    );
}

// ─── Test 17: kg_invalidate_both_replication_fails_with_down_remote ─────────

/// Combined KG rule with write:Both and a dead remote. Local KG invalidate
/// succeeds, replication fails with `status: "failed"` and warnings.
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
    // (With dead remote, the KG add goes both locally and tries replicate;
    //  we ignore the replication failure for setup purposes.)
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
    let replication = invalidate.get("replication").expect("kg_invalidate must include replication");
    assert_eq!(
        replication["status"], "failed",
        "replication must be failed with down remote; got: {replication}"
    );
    assert_eq!(
        replication["remote"], "hub",
        "replication must target hub; got: {replication}"
    );
    let reason = replication["reason"].as_str().unwrap_or("");
    assert!(!reason.is_empty(), "failure reason must be non-empty; got: {replication}");

    let warnings = invalidate.get("warnings").and_then(|w| w.as_array());
    assert!(
        warnings.is_some() && !warnings.unwrap().is_empty(),
        "failed replication must include warnings; got: {invalidate}"
    );
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
    let server = mcp_server_with_hub(
        &local_dir,
        &hub_url,
        BTreeMap::new(),
        RouteMode::Local,
        None,
    )
    .await;

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

    let server = mcp_server_with_hub(
        &local_dir,
        &hub_url,
        wing_rules,
        RouteMode::Local,
        None,
    )
    .await;

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

    let server = mcp_server_with_hub(
        &local_dir,
        &hub_url,
        wing_rules,
        RouteMode::Local,
        None,
    )
    .await;

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
    assert_eq!(
        add["applied_to"], "local",
        "diary both add must report applied_to=local: {add}"
    );

    // Diary-local writes must NOT include a replication field per contract.
    assert!(
        add.get("replication").is_none(),
        "diary-local add must not include a replication field; got: {add}"
    );
}

// ─── Test 21: kg_add_both_with_valid_from_succeeds ──────────────────────────

/// Combined KG rule with write:Both and a `valid_from` date. Local KG add of a
/// dated fact + remote replication must both succeed.
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
        replication["status"], "replicated",
        "dated replication must be replicated; got: {replication}"
    );
    assert_eq!(
        replication["remote"], "hub",
        "dated replication must target hub; got: {replication}"
    );
}

// ─── Test 24: kg_add_both_replication_fails_with_remote_rejection ───────────

/// Combined KG rule with write:Both and a reachable remote that rejects the
/// replication attempt (wrong bearer token → HTTP 401). The local KG add must
/// still succeed, `applied_to` must be `"local"`, and `replication.status`
/// must be `"failed"` with a non-empty `reason` and a `warnings` array.
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

    // Local write must succeed despite replication failure.
    assert_eq!(
        kg_add["success"], true,
        "kg_add with wrong token must still succeed locally: {kg_add}"
    );
    assert_eq!(
        kg_add["applied_to"], "local",
        "kg_add must report applied_to=local: {kg_add}"
    );

    // Replication must be failed.
    let replication = kg_add.get("replication").expect("kg_add both must include replication");
    assert_eq!(
        replication["status"], "failed",
        "replication must report status=failed with wrong token; got: {replication}"
    );
    assert_eq!(
        replication["remote"], "hub",
        "replication must report remote=hub; got: {replication}"
    );
    let reason = replication["reason"].as_str().unwrap_or("");
    assert!(
        !reason.is_empty(),
        "replication failure must include a non-empty reason; got: {replication}"
    );

    // Warnings must be present.
    let warnings = kg_add.get("warnings").and_then(|w| w.as_array());
    assert!(
        warnings.is_some() && !warnings.unwrap().is_empty(),
        "kg_add with failed replication must include warnings array; got: {kg_add}"
    );
}

// ─── Test 25: kg_invalidate_both_replication_fails_with_remote_rejection ────

/// Combined KG rule with write:Both and a reachable remote that rejects the
/// KG invalidate replication (wrong bearer token → HTTP 401). The local KG
/// invalidate must still succeed, and `replication.status` must be `"failed"`.
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

    // Replication must be failed.
    let replication = invalidate.get("replication").expect("kg_invalidate must include replication");
    assert_eq!(
        replication["status"], "failed",
        "replication must be failed with wrong token; got: {replication}"
    );
    assert_eq!(
        replication["remote"], "hub",
        "replication must target hub; got: {replication}"
    );
    let reason = replication["reason"].as_str().unwrap_or("");
    assert!(!reason.is_empty(), "failure reason must be non-empty; got: {replication}");

    let warnings = invalidate.get("warnings").and_then(|w| w.as_array());
    assert!(
        warnings.is_some() && !warnings.unwrap().is_empty(),
        "failed replication must include warnings; got: {invalidate}"
    );
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

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
    FederationRuntimeConfig, LowCpuRuntimeConfig, MempalaceConfig, ResolvedRemote,
    ResolvedRouteRule, RouteMode, ServerRuntimeConfig, WriteTarget,
};
use mempalace_core::EmbeddingProfile;
use mempalace_mcp::{DeterministicStubProvider, JsonRpcRequest, McpServer, decode_tool_payload};
use mempalace_server::{TokenRegistry, build_router};
use serde_json::{Value, json};
use tempfile::TempDir;

// ─── Test token ───────────────────────────────────────────────────────────────

const TEST_TOKEN: &str = "fed-e2e-secret-token-xyz";

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
        },
        federation: FederationRuntimeConfig::default(),
    }
}

/// Spawn the real federation server on an ephemeral port.
/// Returns the bound `SocketAddr`.
async fn spawn_server(dir: &TempDir) -> SocketAddr {
    let token_file = write_token_file(dir);
    let config = server_config(dir);
    let tokens = TokenRegistry::load(token_file).unwrap();
    let provider = DeterministicStubProvider::new(EmbeddingProfile::Balanced);
    let router = build_router(config, provider, tokens).await.unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    addr
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
        },
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
    let router = build_router(hub_config, hub_provider, tokens).await.unwrap();

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
        },
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
        },
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

    // Seed a local KG fact.
    let kg_add = call_tool(
        &server,
        2,
        "mempalace_kg_add",
        json!({"subject": "DegradeTest", "predicate": "has_state", "object": "local"}),
    )
    .await;
    assert_eq!(kg_add["success"], true, "kg_add must succeed locally: {kg_add}");

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

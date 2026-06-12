//! End-to-end integration tests for [`RemoteClient`] against the real in-process
//! federation server.
//!
//! Each test spawns its own server on an ephemeral port with its own `TempDir`
//! so palace state never collides across tests.

#![allow(clippy::unwrap_used)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use mempalace_config::{FederationRuntimeConfig, LowCpuRuntimeConfig, MempalaceConfig, ServerRuntimeConfig};
use mempalace_core::EmbeddingProfile;
use mempalace_embeddings::DeterministicStubProvider;
use mempalace_federation::{
    AddDrawerRequest, ChangesQuery, KgAddFactRequest, KgInvalidateRequest, KgQueryRequest,
    ListDrawersQuery,
};
use mempalace_remote::{RemoteApi, RemoteClient, RemoteEndpoint, RemoteError};
use mempalace_server::{TokenRegistry, build_router};
use tempfile::TempDir;

const TEST_TOKEN: &str = "remote-e2e-secret-token";

// ─── Shared helpers ──────────────────────────────────────────────────────────

/// Write a minimal token file and return the path.
fn write_token_file(dir: &TempDir) -> PathBuf {
    let path = dir.path().join("tokens.json");
    std::fs::write(
        &path,
        serde_json::to_string(&serde_json::json!([
            {"token": TEST_TOKEN, "name": "e2e-test-user", "enabled": true}
        ]))
        .unwrap(),
    )
    .unwrap();
    path
}

/// Build a `MempalaceConfig` wired to `tempdir` with stub embeddings.
fn test_config(tempdir: &TempDir) -> MempalaceConfig {
    MempalaceConfig {
        schema_version: 1,
        collection_name: "mempalace_drawers".to_owned(),
        palace_path: tempdir.path().join("palace"),
        embedding_profile: EmbeddingProfile::Balanced,
        low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
        server: ServerRuntimeConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            token_file: tempdir.path().join("tokens.json"),
            checkouts: std::collections::BTreeMap::new(),
        },
        federation: FederationRuntimeConfig::default(),
    }
}

/// Spawn the server on an ephemeral port and return the bound address.
async fn spawn_server(config: MempalaceConfig, token_file: PathBuf) -> SocketAddr {
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

/// Build a `RemoteClient` pointing at the given address, with an optional token.
fn client_for(addr: SocketAddr, token: Option<&str>) -> RemoteClient {
    RemoteClient::new(RemoteEndpoint {
        name: "test".into(),
        base_url: format!("http://{addr}"),
        token: token.map(str::to_owned),
        timeout: Duration::from_secs(5),
    })
    .unwrap()
}

// ─── Test 1: round_trip_drawers_and_changes ───────────────────────────────────

#[tokio::test]
async fn round_trip_drawers_and_changes() {
    let tempdir = TempDir::new().unwrap();
    let token_file = write_token_file(&tempdir);
    let config = test_config(&tempdir);
    let addr = spawn_server(config, token_file).await;
    let client = client_for(addr, Some(TEST_TOKEN));

    // info() — federation_api_version == 1, capabilities non-empty, server_version non-empty
    let info = client.info().await.unwrap();
    assert_eq!(info.federation_api_version, 1, "federation_api_version must be 1");
    assert!(!info.capabilities.is_empty(), "capabilities must be non-empty");
    assert!(!info.server_version.is_empty(), "server_version must be non-empty");

    // add_drawer → success true, drawer_id Some
    let add_resp = client
        .add_drawer(AddDrawerRequest {
            wing: "wing_rt".to_owned(),
            room: "general".to_owned(),
            content: "round trip e2e distinctive drawer content xyzzy".to_owned(),
            source_file: None,
            added_by: Some("e2e".to_owned()),
        })
        .await
        .unwrap();
    assert!(add_resp.success, "add_drawer must return success=true");
    let drawer_id = add_resp.drawer_id.expect("add_drawer must return drawer_id");

    // search_drawers → results contain our drawer_id at rank 1
    let search_resp = client
        .search_drawers(mempalace_federation::DrawerSearchRequest {
            query: "round trip e2e distinctive xyzzy".to_owned(),
            wing: Some("wing_rt".to_owned()),
            room: None,
            limit: Some(5),
        })
        .await
        .unwrap();
    let results = &search_resp.results;
    assert!(!results.is_empty(), "search results must not be empty");
    assert_eq!(
        results[0].drawer_id, drawer_id,
        "top search result must be the drawer we just added"
    );
    assert_eq!(results[0].rank, 1, "top result must have rank 1");

    // get_drawer → content matches
    let drawer_val = client.get_drawer(&drawer_id).await.unwrap();
    let content = drawer_val["content"].as_str().unwrap();
    assert!(
        content.contains("round trip e2e distinctive"),
        "get_drawer content must match; got: {content}"
    );

    // list_drawers → contains our drawer
    let list_resp = client
        .list_drawers(ListDrawersQuery {
            wing: Some("wing_rt".to_owned()),
            room: None,
            limit: None,
            cursor: None,
        })
        .await
        .unwrap();
    let drawers_arr = list_resp.drawers.as_array().unwrap();
    let found = drawers_arr.iter().any(|d| d["id"].as_str().unwrap_or("") == drawer_id);
    assert!(found, "list_drawers must contain our drawer (id={drawer_id})");

    // taxonomy / wings / rooms smoke calls
    client.taxonomy().await.unwrap();
    client.wings().await.unwrap();
    client.rooms(None).await.unwrap();
    client.rooms(Some("wing_rt")).await.unwrap();

    // changes → events contain drawer_added with our entity_id
    let changes = client.changes(ChangesQuery { since: None, limit: None, cursor: None }).await.unwrap();
    let has_event = changes.events.iter().any(|ev| {
        ev.event_type == "drawer_added" && ev.entity_id == drawer_id
    });
    assert!(has_event, "changes feed must contain a drawer_added event for our drawer");

    // delete_drawer → Ok
    client.delete_drawer(&drawer_id).await.unwrap();

    // delete again → Err(RemoteRejected { status: 404, .. })
    let del_again = client.delete_drawer(&drawer_id).await;
    match del_again {
        Err(RemoteError::RemoteRejected { status: 404, .. }) => {}
        other => panic!("expected RemoteRejected(404) on second delete, got: {other:?}"),
    }
}

// ─── Test 2: kg_round_trip ───────────────────────────────────────────────────

#[tokio::test]
async fn kg_round_trip() {
    let tempdir = TempDir::new().unwrap();
    let token_file = write_token_file(&tempdir);
    let config = test_config(&tempdir);
    let addr = spawn_server(config, token_file).await;
    let client = client_for(addr, Some(TEST_TOKEN));

    // kg_add_fact → success true
    let add_val = client
        .kg_add_fact(KgAddFactRequest {
            subject: "alice".to_owned(),
            predicate: "works_at".to_owned(),
            object: "acme".to_owned(),
            valid_from: None,
        })
        .await
        .unwrap();
    assert_eq!(add_val["success"], true, "kg_add_fact must return success=true");

    // kg_query → facts count >= 1
    let query_val = client
        .kg_query(KgQueryRequest {
            entity: "alice".to_owned(),
            as_of: None,
            direction: None,
        })
        .await
        .unwrap();
    let count = query_val["count"].as_u64().unwrap_or(0);
    assert!(count >= 1, "kg_query must return at least 1 fact for alice");

    // kg_timeline with entity
    client.kg_timeline(Some("alice")).await.unwrap();

    // kg_timeline without entity (full timeline)
    client.kg_timeline(None).await.unwrap();

    // kg_stats
    client.kg_stats().await.unwrap();

    // kg_invalidate → success true
    let inv_val = client
        .kg_invalidate(KgInvalidateRequest {
            subject: "alice".to_owned(),
            predicate: "works_at".to_owned(),
            object: "acme".to_owned(),
            ended: None,
        })
        .await
        .unwrap();
    assert_eq!(inv_val["success"], true, "kg_invalidate must return success=true");
}

// ─── Test 3: duplicate_add_is_remote_rejected_409 ────────────────────────────

#[tokio::test]
async fn duplicate_add_is_remote_rejected_409() {
    let tempdir = TempDir::new().unwrap();
    let token_file = write_token_file(&tempdir);
    let config = test_config(&tempdir);
    let addr = spawn_server(config, token_file).await;
    let client = client_for(addr, Some(TEST_TOKEN));

    let req = AddDrawerRequest {
        wing: "wing_dup".to_owned(),
        room: "test".to_owned(),
        content: "duplicate detection e2e test content unique quux".to_owned(),
        source_file: None,
        added_by: None,
    };

    // First add — must succeed
    let first = client.add_drawer(req.clone()).await.unwrap();
    assert!(first.success, "first add must succeed");

    // Second identical add — must return RemoteRejected { status: 409, .. }
    // with body containing "duplicate"
    let second = client.add_drawer(req).await;
    match second {
        Err(RemoteError::RemoteRejected { status: 409, body, .. }) => {
            assert!(
                body.to_lowercase().contains("duplicate"),
                "409 body must mention 'duplicate'; got: {body}"
            );
        }
        other => panic!("expected RemoteRejected(409) on duplicate add, got: {other:?}"),
    }
}

// ─── Test 4: diary_write_is_remote_rejected ───────────────────────────────────

#[tokio::test]
async fn diary_write_is_remote_rejected() {
    let tempdir = TempDir::new().unwrap();
    let token_file = write_token_file(&tempdir);
    let config = test_config(&tempdir);
    let addr = spawn_server(config, token_file).await;
    let client = client_for(addr, Some(TEST_TOKEN));

    // Diary room "diary" inside wing_agents → 422 UNPROCESSABLE_ENTITY
    let result = client
        .add_drawer(AddDrawerRequest {
            wing: "wing_agents".to_owned(),
            room: "diary".to_owned(),
            content: "diary entry content via federation".to_owned(),
            source_file: None,
            added_by: None,
        })
        .await;

    match result {
        Err(RemoteError::RemoteRejected { status: 422, .. }) => {}
        other => panic!("expected RemoteRejected(422) for diary write, got: {other:?}"),
    }
}

// ─── Test 5: wrong_token_is_unauthorized ─────────────────────────────────────

#[tokio::test]
async fn wrong_token_is_unauthorized() {
    let tempdir = TempDir::new().unwrap();
    let token_file = write_token_file(&tempdir);
    let config = test_config(&tempdir);
    let addr = spawn_server(config, token_file).await;
    let client = client_for(addr, Some("wrong-token-xyz"));

    let result = client.info().await;
    match result {
        Err(RemoteError::Unauthorized { .. }) => {}
        other => panic!("expected Unauthorized for wrong token, got: {other:?}"),
    }
}

// ─── Test 6: missing_token_is_unauthorized ────────────────────────────────────

#[tokio::test]
async fn missing_token_is_unauthorized() {
    let tempdir = TempDir::new().unwrap();
    let token_file = write_token_file(&tempdir);
    let config = test_config(&tempdir);
    let addr = spawn_server(config, token_file).await;
    let client = client_for(addr, None);

    let result = client.info().await;
    match result {
        Err(RemoteError::Unauthorized { .. }) => {}
        other => panic!("expected Unauthorized for missing token, got: {other:?}"),
    }
}

// ─── Test 7: version_skew_is_hard_error ──────────────────────────────────────

#[tokio::test]
async fn version_skew_is_hard_error() {
    // Minimal stub server that reports federation_api_version=999.
    let app = axum::Router::new().route(
        "/v1/info",
        axum::routing::get(|| async {
            axum::Json(serde_json::json!({
                "server_version": "9.9.9",
                "federation_api_version": 999u32,
                "embedding_profile": "balanced",
                "capabilities": []
            }))
        }),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // No token needed — the stub doesn't check auth.
    let client = client_for(addr, None);

    // First call: version skew detected
    let first = client
        .search_drawers(mempalace_federation::DrawerSearchRequest {
            query: "anything".to_owned(),
            wing: None,
            room: None,
            limit: None,
        })
        .await;
    match first {
        Err(RemoteError::VersionSkew { ours: 1, theirs: 999, .. }) => {}
        other => panic!("expected VersionSkew(ours=1, theirs=999) on first call, got: {other:?}"),
    }

    // Second call: same error (info is cached in the OnceCell; no refetch).
    let second = client
        .search_drawers(mempalace_federation::DrawerSearchRequest {
            query: "anything".to_owned(),
            wing: None,
            room: None,
            limit: None,
        })
        .await;
    match second {
        Err(RemoteError::VersionSkew { ours: 1, theirs: 999, .. }) => {}
        other => panic!("expected VersionSkew(ours=1, theirs=999) on second call, got: {other:?}"),
    }
}

// ─── Test 8: unreachable_remote_is_degradable ────────────────────────────────

#[tokio::test]
async fn unreachable_remote_is_degradable() {
    // Bind a listener to get a free port, then drop it so nothing is listening.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let client = client_for(addr, Some(TEST_TOKEN));

    let result = client.info().await;
    match result {
        Err(ref err @ RemoteError::Unreachable { .. }) => {
            assert!(err.is_degradable(), "Unreachable must be degradable");
        }
        other => panic!("expected Unreachable for dropped listener, got: {other:?}"),
    }
}

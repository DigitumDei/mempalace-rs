//! End-to-end integration tests for [`RemoteClient`] against the real in-process
//! federation server.
//!
//! Each test spawns its own server on an ephemeral port with its own `TempDir`
//! so palace state never collides across tests.

#![allow(clippy::unwrap_used)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use mempalace_config::{
    FederationRuntimeConfig, LowCpuRuntimeConfig, MaintenanceRuntimeConfig, MempalaceConfig,
    ServerRuntimeConfig,
};
use mempalace_core::EmbeddingProfile;
use mempalace_embeddings::DeterministicStubProvider;
use mempalace_federation::{
    AddDrawerRequest, ChangesQuery, IngestBatchRequest, IngestChunkDto, IngestFileDto,
    KgAddFactRequest, KgInvalidateRequest, KgQueryRequest, ListDrawersQuery, NewArtifactRequest,
    NewMessageRequest, NewTaskRequest, NewTaskResultRequest, TaskLeaseRequest,
};
use mempalace_remote::{RemoteApi, RemoteClient, RemoteEndpoint, RemoteError, RemoteRevisionedWrite};
use mempalace_server::{TokenRegistry, build_router};
use tempfile::TempDir;

const TEST_TOKEN: &str = "remote-e2e-secret-token";
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
    restrict_token_file(&path);
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
        maintenance: MaintenanceRuntimeConfig::defaults(),
    }
}

/// Spawn the server on an ephemeral port and return the bound address.
async fn spawn_server(config: MempalaceConfig, token_file: PathBuf) -> SocketAddr {
    let tokens = TokenRegistry::load(token_file).unwrap();
    let provider = DeterministicStubProvider::new(EmbeddingProfile::Balanced);
    let (router, _state) = build_router(config, provider, tokens).await.unwrap();

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
            view: None,
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
    let changes =
        client.changes(ChangesQuery { since: None, limit: None, cursor: None }).await.unwrap();
    let has_event = changes
        .events
        .iter()
        .any(|ev| ev.event_type == "drawer_added" && ev.entity_id == drawer_id);
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
        .kg_query(KgQueryRequest { entity: "alice".to_owned(), as_of: None, direction: None })
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
            view: None,
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
            view: None,
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

// ─── Test 9: ingest_batch_round_trip ─────────────────────────────────────────

#[tokio::test]
async fn ingest_batch_round_trip() {
    let tempdir = TempDir::new().unwrap();
    let token_file = write_token_file(&tempdir);
    let config = test_config(&tempdir);
    let addr = spawn_server(config, token_file).await;
    let client = client_for(addr, Some(TEST_TOKEN));

    // Two files:
    //   file 0 — no file_hash → content row (non-UTF-8 fallback path)
    //   file 1 — file_hash present → locator row
    //
    // Use keyword clusters that are textually far apart so the
    // DeterministicStubProvider assigns distinct embeddings.
    let req = IngestBatchRequest {
        wing: "wing_ingest".to_owned(),
        repo_id: "github.com/test/myrepo".to_owned(),
        agent: Some("e2e-agent".to_owned()),
        commit_hash: Some("deadbeef1234".to_owned()),
        files: vec![
            // Content-row file (no file_hash)
            IngestFileDto {
                relative_path: "src/alpha.rs".to_owned(),
                content_hash: "contenthash_alpha_001".to_owned(),
                file_hash: None,
                chunks: vec![IngestChunkDto {
                    chunk_index: 0,
                    room: "code".to_owned(),
                    text: "alpha module implements the frombulation subsystem for quxzort"
                        .to_owned(),
                    byte_start: None,
                    byte_end: None,
                    line_start: None,
                    line_end: None,
                }],
            },
            // Locator-row file (file_hash present, ranges provided)
            IngestFileDto {
                relative_path: "src/beta.rs".to_owned(),
                content_hash: "contenthash_beta_002".to_owned(),
                file_hash: Some(
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                ),
                chunks: vec![IngestChunkDto {
                    chunk_index: 0,
                    room: "code".to_owned(),
                    text: "beta module handles the wibbleplonk dispatcher initialization"
                        .to_owned(),
                    byte_start: Some(0),
                    byte_end: Some(62),
                    line_start: Some(1),
                    line_end: Some(1),
                }],
            },
        ],
    };

    let resp = client.ingest_batch(req).await.unwrap();

    // Both files must be present in the response in request order.
    assert_eq!(resp.files.len(), 2, "response must contain one result per file");

    let alpha = &resp.files[0];
    assert_eq!(alpha.relative_path, "src/alpha.rs");
    assert_eq!(alpha.status, "ingested", "alpha file must be ingested");
    assert_eq!(alpha.drawers_written, 1, "alpha: one chunk → one drawer");
    assert!(alpha.error.is_none(), "alpha must not have an error");

    let beta = &resp.files[1];
    assert_eq!(beta.relative_path, "src/beta.rs");
    assert_eq!(beta.status, "ingested", "beta file must be ingested");
    assert_eq!(beta.drawers_written, 1, "beta: one chunk → one drawer");
    assert!(beta.error.is_none(), "beta must not have an error");

    // search_drawers must find the alpha chunk by a keyword from its text.
    let search_resp = client
        .search_drawers(mempalace_federation::DrawerSearchRequest {
            query: "frombulation subsystem quxzort".to_owned(),
            wing: Some("wing_ingest".to_owned()),
            room: None,
            limit: Some(5),
            view: None,
        })
        .await
        .unwrap();
    assert!(!search_resp.results.is_empty(), "search must find the alpha chunk after ingest_batch");
    let top = &search_resp.results[0];
    assert!(
        top.content.contains("frombulation"),
        "top search result content must mention 'frombulation'; got: {}",
        top.content
    );
}

// ─── Test 10: ingest_batch_diary_wing_rejected_422 ────────────────────────────

#[tokio::test]
async fn ingest_batch_diary_wing_rejected_422() {
    let tempdir = TempDir::new().unwrap();
    let token_file = write_token_file(&tempdir);
    let config = test_config(&tempdir);
    let addr = spawn_server(config, token_file).await;
    let client = client_for(addr, Some(TEST_TOKEN));

    // Sending to the diary room inside wing_agents must be rejected with 422.
    let req = IngestBatchRequest {
        wing: "wing_agents".to_owned(),
        repo_id: "github.com/test/repo".to_owned(),
        agent: None,
        commit_hash: None,
        files: vec![IngestFileDto {
            relative_path: "diary/entry.md".to_owned(),
            content_hash: "ch_diary_001".to_owned(),
            file_hash: None,
            chunks: vec![IngestChunkDto {
                chunk_index: 0,
                room: "diary".to_owned(),
                text: "diary entry attempt via ingest_batch".to_owned(),
                byte_start: None,
                byte_end: None,
                line_start: None,
                line_end: None,
            }],
        }],
    };

    let result = client.ingest_batch(req).await;
    match result {
        Err(RemoteError::RemoteRejected { status: 422, .. }) => {}
        other => {
            panic!("expected RemoteRejected(422) for diary ingest_batch, got: {other:?}")
        }
    }
}

// ─── Test 11: ingest_batch_route_missing_returns_404 ─────────────────────────

#[tokio::test]
async fn ingest_batch_route_missing_returns_404() {
    // Stub server: answers /v1/info correctly but 404s /v1/ingest/batch.
    // This mirrors an old server that doesn't yet implement the bulk-ingest
    // endpoint (the plan notes: "old servers 404 → RemoteRejected").
    let app = axum::Router::new().route(
        "/v1/info",
        axum::routing::get(|| async {
            axum::Json(serde_json::json!({
                "server_version": "1.0.0-stub",
                "federation_api_version": 1u32,
                "embedding_profile": "balanced",
                "capabilities": ["drawers", "kg"]
            }))
        }),
    );
    // All other routes fall through to the default 404 handler.

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // No token needed — the stub doesn't check auth.
    let client = client_for(addr, None);

    let req = IngestBatchRequest {
        wing: "wing_code".to_owned(),
        repo_id: "github.com/test/repo".to_owned(),
        agent: None,
        commit_hash: None,
        files: vec![IngestFileDto {
            relative_path: "src/main.rs".to_owned(),
            content_hash: "ch_main_001".to_owned(),
            file_hash: None,
            chunks: vec![IngestChunkDto {
                chunk_index: 0,
                room: "code".to_owned(),
                text: "fn main() {}".to_owned(),
                byte_start: None,
                byte_end: None,
                line_start: None,
                line_end: None,
            }],
        }],
    };

    let result = client.ingest_batch(req).await;
    match result {
        Err(RemoteError::RemoteRejected { status: 404, .. }) => {}
        other => {
            panic!("expected RemoteRejected(404) for missing route, got: {other:?}")
        }
    }
}

// ─── Test 12: coordination_capability_gate_rejects_unsupported_remote ────────

#[tokio::test]
async fn coordination_capability_gate_rejects_unsupported_remote() {
    // Stub server that advertises everything except "coordination" — mirrors a
    // pre-Stage-3 server the client talks to before it has been upgraded.
    let app = axum::Router::new().route(
        "/v1/info",
        axum::routing::get(|| async {
            axum::Json(serde_json::json!({
                "server_version": "1.0.0-pre-stage3",
                "federation_api_version": 1u32,
                "embedding_profile": "balanced",
                "capabilities": ["drawers", "kg", "changes", "ingest"]
            }))
        }),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = client_for(addr, None);

    let result = client
        .coordination_task_create(NewTaskRequest {
            title: "t".to_owned(),
            description: "d".to_owned(),
            wing: "wing_test".to_owned(),
            idempotency_key: "k1".to_owned(),
            created_by: None,
            parent_id: None,
            dependencies: Vec::new(),
            budget: None,
            expires_at: None,
        })
        .await;
    match result {
        Err(RemoteError::CapabilityMissing { capability, .. }) => {
            assert_eq!(capability, "coordination");
        }
        other => panic!("expected CapabilityMissing, got: {other:?}"),
    }

    // The gate must be checked before the request is ever sent — every coordination method
    // must trip it identically, not just the one exercised above (`coordination_task_get` is a
    // GET, which would otherwise 404 against this stub and could be confused for "not found").
    let get_result = client.coordination_task_get("task_missing").await;
    assert!(
        matches!(get_result, Err(RemoteError::CapabilityMissing { .. })),
        "expected CapabilityMissing, got: {get_result:?}"
    );
}

// ─── Test 13: coordination_round_trip ─────────────────────────────────────────

#[tokio::test]
async fn coordination_round_trip() {
    let tempdir = TempDir::new().unwrap();
    let token_file = write_token_file(&tempdir);
    let config = test_config(&tempdir);
    let addr = spawn_server(config, token_file).await;
    let client = client_for(addr, Some(TEST_TOKEN));

    let task = client
        .coordination_task_create(NewTaskRequest {
            title: "index the repo".to_owned(),
            description: "d".to_owned(),
            wing: "wing_rt_coord".to_owned(),
            idempotency_key: "rt-coord-task-1".to_owned(),
            created_by: Some("alice".to_owned()),
            parent_id: None,
            dependencies: Vec::new(),
            budget: None,
            expires_at: None,
        })
        .await
        .unwrap();
    assert_eq!(task.state, mempalace_federation::CoordinationTaskState::Pending);
    assert_eq!(task.revision, 0);

    let fetched = client.coordination_task_get(&task.task_id).await.unwrap();
    assert_eq!(fetched.task_id, task.task_id);

    let claimed = match client
        .coordination_task_claim(
            &task.task_id,
            TaskLeaseRequest {
                expected_revision: 0,
                lease_seconds: 300,
                worker: Some("worker-1".to_owned()),
            },
        )
        .await
        .unwrap()
    {
        RemoteRevisionedWrite::Applied(task) => task,
        other => panic!("expected Applied, got: {other:?}"),
    };
    assert_eq!(claimed.revision, 1);
    assert_eq!(claimed.owner.as_deref(), Some("e2e-test-user:worker-1"));

    let renewed = match client
        .coordination_task_renew(
            &task.task_id,
            TaskLeaseRequest {
                expected_revision: claimed.revision,
                lease_seconds: 300,
                worker: Some("worker-1".to_owned()),
            },
        )
        .await
        .unwrap()
    {
        RemoteRevisionedWrite::Applied(task) => task,
        other => panic!("expected Applied, got: {other:?}"),
    };
    assert_eq!(renewed.revision, 2);

    let message = client
        .coordination_message_send(NewMessageRequest {
            task_id: task.task_id.clone(),
            recipient: "manager".to_owned(),
            kind: "status".to_owned(),
            payload: serde_json::json!({"progress": 0.5}),
            idempotency_key: "rt-coord-msg-1".to_owned(),
            sender: Some("worker-1".to_owned()),
            envelope_version: 1,
        })
        .await
        .unwrap();
    let fetched_message = client.coordination_message_get(&message.message_id).await.unwrap();
    assert_eq!(fetched_message.message_id, message.message_id);

    let artifact = client
        .coordination_artifact_put(NewArtifactRequest {
            task_id: task.task_id.clone(),
            role: "log".to_owned(),
            media_type: "text/plain".to_owned(),
            content: "did the thing".to_owned(),
            idempotency_key: "rt-coord-artifact-1".to_owned(),
            created_by: Some("worker-1".to_owned()),
        })
        .await
        .unwrap();
    let fetched_artifact = client.coordination_artifact_get(&artifact.artifact_id).await.unwrap();
    assert_eq!(fetched_artifact.content_hash, artifact.content_hash);

    let result = client
        .coordination_result_put(NewTaskResultRequest {
            task_id: task.task_id.clone(),
            payload: serde_json::json!({"answer": 42}),
            idempotency_key: "rt-coord-result-1".to_owned(),
            created_by: Some("worker-1".to_owned()),
        })
        .await
        .unwrap();
    let fetched_result = client.coordination_result_get(&result.result_id).await.unwrap();
    assert_eq!(fetched_result.result_id, result.result_id);

    let transitioned = match client
        .coordination_task_transition(
            &task.task_id,
            mempalace_federation::TransitionTaskRequest {
                expected_revision: renewed.revision,
                state: mempalace_federation::CoordinationTaskState::Completed,
                actor: Some("worker-1".to_owned()),
                details: None,
            },
        )
        .await
        .unwrap()
    {
        RemoteRevisionedWrite::Applied(task) => task,
        other => panic!("expected Applied, got: {other:?}"),
    };
    assert_eq!(transitioned.state, mempalace_federation::CoordinationTaskState::Completed);

    let events = client
        .coordination_events(mempalace_federation::CoordinationEventsQuery {
            cursor: None,
            task_id: Some(task.task_id.clone()),
            wing: None,
            limit: None,
        })
        .await
        .unwrap();
    assert!(!events.events.is_empty(), "expected at least one coordination event for the task");

    let inbox = client
        .coordination_inbox(mempalace_federation::InboxQuery {
            recipient: "manager".to_owned(),
            wing: None,
            cursor: None,
            limit: None,
            unacknowledged_only: false,
        })
        .await
        .unwrap();
    assert!(
        inbox.messages.iter().any(|m| m.message_id == message.message_id),
        "expected the sent message in manager's inbox"
    );
}

// ─── Test 14: coordination_claim_revision_conflict_surfaces_actual_revision ──

#[tokio::test]
async fn coordination_claim_revision_conflict_surfaces_actual_revision() {
    let tempdir = TempDir::new().unwrap();
    let token_file = write_token_file(&tempdir);
    let config = test_config(&tempdir);
    let addr = spawn_server(config, token_file).await;
    let client = client_for(addr, Some(TEST_TOKEN));

    let task = client
        .coordination_task_create(NewTaskRequest {
            title: "stale revision test".to_owned(),
            description: "d".to_owned(),
            wing: "wing_rt_conflict".to_owned(),
            idempotency_key: "rt-conflict-task-1".to_owned(),
            created_by: None,
            parent_id: None,
            dependencies: Vec::new(),
            budget: None,
            expires_at: None,
        })
        .await
        .unwrap();

    // First claim at the correct (0) revision succeeds and advances the task to revision 1.
    let claimed = match client
        .coordination_task_claim(
            &task.task_id,
            TaskLeaseRequest { expected_revision: 0, lease_seconds: 300, worker: None },
        )
        .await
        .unwrap()
    {
        RemoteRevisionedWrite::Applied(task) => task,
        other => panic!("expected Applied, got: {other:?}"),
    };
    assert_eq!(claimed.revision, 1);

    // Renewing at the now-stale revision 0 is a revision conflict — reported as
    // `Ok(RemoteRevisionedWrite::Conflict)` carrying the remote's real current revision, not
    // as an `Err` a caller would have to parse a message to recover from.
    let conflict = client
        .coordination_task_renew(
            &task.task_id,
            TaskLeaseRequest { expected_revision: 0, lease_seconds: 300, worker: None },
        )
        .await
        .unwrap();
    assert_eq!(conflict, RemoteRevisionedWrite::Conflict { actual_revision: Some(1) });
}

// ─── Test 15: coordination_lease_conflict_is_a_hard_error_not_a_revisioned_conflict ──

#[tokio::test]
async fn coordination_lease_conflict_is_a_hard_error_not_a_revisioned_conflict() {
    let tempdir = TempDir::new().unwrap();
    let token_file = write_token_file(&tempdir);
    let config = test_config(&tempdir);
    let addr = spawn_server(config, token_file).await;
    let client = client_for(addr, Some(TEST_TOKEN));

    let task = client
        .coordination_task_create(NewTaskRequest {
            title: "lease conflict test".to_owned(),
            description: "d".to_owned(),
            wing: "wing_rt_lease".to_owned(),
            idempotency_key: "rt-lease-task-1".to_owned(),
            created_by: None,
            parent_id: None,
            dependencies: Vec::new(),
            budget: None,
            expires_at: None,
        })
        .await
        .unwrap();

    match client
        .coordination_task_claim(
            &task.task_id,
            TaskLeaseRequest {
                expected_revision: 0,
                lease_seconds: 300,
                worker: Some("worker-a".to_owned()),
            },
        )
        .await
        .unwrap()
    {
        RemoteRevisionedWrite::Applied(task) => assert_eq!(task.revision, 1),
        other => panic!("expected Applied, got: {other:?}"),
    }

    // A second worker claiming at the now-*correct* revision hits a live-lease conflict, not a
    // stale-revision one — this has no revision pair to report, so it must stay an `Err`
    // (`RemoteRejected`), never a `RemoteRevisionedWrite::Conflict`.
    let second = client
        .coordination_task_claim(
            &task.task_id,
            TaskLeaseRequest {
                expected_revision: 1,
                lease_seconds: 300,
                worker: Some("worker-b".to_owned()),
            },
        )
        .await;
    match second {
        Err(RemoteError::RemoteRejected { status: 409, body, .. }) => {
            assert!(
                body.contains("coordination_conflict"),
                "409 body must carry the coordination_conflict code; got: {body}"
            );
        }
        other => panic!("expected RemoteRejected(409), got: {other:?}"),
    }
}

//! End-to-end test for the federation HTTP server stack.
//!
//! Builds the axum router via `mempalace_server::build_router`, binds a real
//! ephemeral TCP port (port 0), then exercises the key routes over real HTTP
//! using `reqwest`.

#![allow(clippy::unwrap_used)]

use std::path::PathBuf;

use mempalace_config::{FederationRuntimeConfig, LowCpuRuntimeConfig, MaintenanceRuntimeConfig, MempalaceConfig, ServerRuntimeConfig};
use mempalace_core::EmbeddingProfile;
use mempalace_embeddings::DeterministicStubProvider;
use mempalace_server::{TokenRegistry, build_router};
use tempfile::TempDir;

const TEST_TOKEN: &str = "serve-e2e-secret-token";
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
async fn spawn_server(
    config: MempalaceConfig,
    token_file: PathBuf,
) -> std::net::SocketAddr {
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

#[tokio::test]
async fn serve_e2e_health_info_drawers_changes() {
    let tempdir = TempDir::new().unwrap();
    let token_file = write_token_file(&tempdir);
    let config = test_config(&tempdir);
    let addr = spawn_server(config, token_file).await;
    let base = format!("http://{addr}");

    let client = reqwest::Client::new();

    // ── 1. GET /v1/health → 200 ──────────────────────────────────────────────
    let health = client.get(format!("{base}/v1/health")).send().await.unwrap();
    assert_eq!(health.status(), 200, "health should return 200");
    let health_body: serde_json::Value = health.json().await.unwrap();
    assert_eq!(health_body["status"], "ok");

    // ── 2. GET /v1/info without token → 401 ──────────────────────────────────
    let info_no_auth = client.get(format!("{base}/v1/info")).send().await.unwrap();
    assert_eq!(info_no_auth.status(), 401, "info without token should be 401");

    // ── 3. GET /v1/info with valid token → 200, federation_api_version = 1 ──
    let info = client
        .get(format!("{base}/v1/info"))
        .bearer_auth(TEST_TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(info.status(), 200, "info with token should return 200");
    let info_body: serde_json::Value = info.json().await.unwrap();
    assert_eq!(
        info_body["federation_api_version"], 1,
        "federation_api_version must be 1"
    );

    // ── 4. POST /v1/drawers (add) ────────────────────────────────────────────
    let add_resp = client
        .post(format!("{base}/v1/drawers"))
        .bearer_auth(TEST_TOKEN)
        .json(&serde_json::json!({
            "wing": "wing_e2e",
            "room": "integration",
            "content": "serve end-to-end test drawer content",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(add_resp.status(), 200, "add drawer should return 200");
    let add_body: serde_json::Value = add_resp.json().await.unwrap();
    assert_eq!(add_body["success"], true);
    let drawer_id = add_body["drawer_id"].as_str().unwrap().to_owned();

    // ── 5. POST /v1/drawers/search → finds the added drawer ──────────────────
    let search_resp = client
        .post(format!("{base}/v1/drawers/search"))
        .bearer_auth(TEST_TOKEN)
        .json(&serde_json::json!({"query": "end-to-end test drawer", "limit": 5}))
        .send()
        .await
        .unwrap();
    assert_eq!(search_resp.status(), 200, "search should return 200");
    let search_body: serde_json::Value = search_resp.json().await.unwrap();
    let results = search_body["results"].as_array().unwrap();
    assert!(
        !results.is_empty(),
        "search results should not be empty"
    );
    let found = results.iter().any(|r| {
        r["drawer_id"].as_str().unwrap_or("") == drawer_id
    });
    assert!(found, "search should find the drawer we just added (id={drawer_id})");

    // ── 6. GET /v1/changes → contains drawer_added event ─────────────────────
    let changes_resp = client
        .get(format!("{base}/v1/changes"))
        .bearer_auth(TEST_TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(changes_resp.status(), 200, "changes should return 200");
    let changes_body: serde_json::Value = changes_resp.json().await.unwrap();
    let events = changes_body["events"].as_array().unwrap();
    assert!(
        !events.is_empty(),
        "changes feed should have at least one event"
    );
    let has_drawer_added = events.iter().any(|e| {
        e["event_type"].as_str().unwrap_or("") == "drawer_added"
            && e["entity_id"].as_str().unwrap_or("") == drawer_id
    });
    assert!(
        has_drawer_added,
        "changes feed should contain a drawer_added event for the drawer we added"
    );
}

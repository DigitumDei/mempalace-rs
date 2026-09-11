//! HTTP MCP client -> unified gateway -> separate federation server.
//! Set AGENTPALACE_TEST_BINARY and AGENTPALACE_TEST_REAL_EMBEDDINGS=1 to smoke-test a deployment.
#![allow(clippy::unwrap_used)]
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
    time::Duration,
};

struct Server {
    child: Option<Child>,
    url: String,
    _dir: tempfile::TempDir,
}

impl Server {
    fn start(config: Value) -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("config.json"), config.to_string()).unwrap();
        let tokens = dir.path().join("tokens.json");
        std::fs::write(&tokens, r#"[{"name":"test","token":"test-secret","enabled":true}]"#)
            .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&tokens, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let binary = std::env::var_os("AGENTPALACE_TEST_BINARY")
            .unwrap_or_else(|| env!("CARGO_BIN_EXE_agentpalace").into());
        let real = agentpalace_core::env_var("AGENTPALACE_TEST_REAL_EMBEDDINGS").as_deref() == Ok("1");
        let child = Command::new(binary)
            .args(["serve", "--bind", "127.0.0.1:0", "--token-file"])
            .arg(tokens)
            .env("AGENTPALACE_CONFIG_DIR", dir.path())
            .env("AGENTPALACE_PALACE_PATH", dir.path().join("palace"))
            .env("AGENTPALACE_STUB_EMBEDDINGS", if real { "0" } else { "1" })
            .env("AGENTPALACE_EMBED_ALLOW_DOWNLOADS", "0")
            .env_remove("AGENTPALACE_LINEAGE_ID")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut server = Self { child: Some(child), url: String::new(), _dir: dir };
        let stderr = server.child.as_mut().unwrap().stderr.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if let Some(url) = line.strip_prefix("Listening on ") {
                    let _ = tx.send(url.to_owned());
                }
                eprintln!("{line}");
            }
        });
        server.url = rx.recv_timeout(Duration::from_secs(60)).expect("server did not start");
        server
    }

    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    async fn rpc(&self, client: &reqwest::Client, method: &str, params: Value) -> Value {
        client
            .post(format!("{}/mcp", self.url))
            .bearer_auth("test-secret")
            .header("accept", "application/json, text/event-stream")
            .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    async fn tool(&self, client: &reqwest::Client, name: &str, arguments: Value) -> Value {
        let response =
            self.rpc(client, "tools/call", json!({"name":name,"arguments":arguments})).await;
        assert_ne!(response["result"]["isError"], true, "{response}");
        agentpalace_mcp::decode_tool_payload(&response).unwrap_or_else(|| panic!("{response}"))
    }

    async fn drawer(&self, client: &reqwest::Client, id: &str) -> reqwest::Response {
        client
            .get(format!("{}/v1/drawers/{id}", self.url))
            .bearer_auth("test-secret")
            .send()
            .await
            .unwrap()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop();
    }
}

#[tokio::test]
async fn http_mcp_routes_to_remote_and_survives_remote_shutdown() {
    let mut remote = Server::start(json!({"version":1}));
    let gateway = Server::start(json!({"version":1,"federation":{
        "remotes":[{"name":"hub","url":remote.url,"token":"test-secret"}],
        "wings":{"wing_remote":{"mode":"remote","remote":"hub"}}
    }}));
    let client = reqwest::Client::builder().timeout(Duration::from_secs(30)).build().unwrap();
    let init = gateway
        .rpc(
            &client,
            "initialize",
            json!({"protocolVersion":"2025-03-26",
        "capabilities":{},"clientInfo":{"name":"remote-e2e","version":"1"}}),
        )
        .await;
    assert_eq!(init["result"]["protocolVersion"], "2025-03-26");
    let initialized = client
        .post(format!("{}/mcp", gateway.url))
        .bearer_auth("test-secret")
        .header("accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
        .send()
        .await
        .unwrap();
    assert_eq!(initialized.status(), 202);
    let tools = gateway.rpc(&client, "tools/list", json!({})).await;
    assert_eq!(
        tools["result"]["tools"].as_array().unwrap().len(),
        agentpalace_mcp::tool_definitions().len()
    );

    let content = "Temporary remote stores the violet telescope calibration record.";
    let added = gateway
        .tool(
            &client,
            "agentpalace_add_drawer",
            json!({
                "wing":"wing_remote","room":"general","content":content,"added_by":"remote-e2e"
            }),
        )
        .await;
    assert_eq!(added["success"], true, "{added}");
    assert_eq!(added["applied_to"], "remote:hub", "{added}");
    let id = added["drawer_id"].as_str().unwrap();
    let stored: Value =
        remote.drawer(&client, id).await.error_for_status().unwrap().json().await.unwrap();
    assert_eq!(stored["content"], content);
    assert_eq!(gateway.drawer(&client, id).await.status(), 404);
    let search = gateway
        .tool(&client, "agentpalace_search", json!({"query":content,"wing":"wing_remote","limit":5}))
        .await;
    assert!(search["results"].as_array().unwrap().iter().any(|r| r["origin"] == "hub"), "{search}");
    eprintln!("PASS: HTTP MCP remote write, storage isolation, and search");

    let local = gateway.tool(&client, "agentpalace_add_drawer", json!({
        "wing":"wing_local","room":"general","content":"Local orchard watering schedule.","added_by":"remote-e2e"
    })).await;
    assert_eq!(local["success"], true, "{local}");
    let local_id = local["drawer_id"].as_str().unwrap();
    assert_eq!(gateway.drawer(&client, local_id).await.status(), 200);
    assert_eq!(remote.drawer(&client, local_id).await.status(), 404);

    let deleted = gateway.tool(&client, "agentpalace_delete_drawer", json!({"drawer_id":id})).await;
    assert_eq!(deleted["success"], true, "{deleted}");
    assert_eq!(deleted["applied_to"], "remote:hub", "{deleted}");
    assert_eq!(remote.drawer(&client, id).await.status(), 404);
    eprintln!("PASS: local routing and remote deletion");

    remote.stop();
    let wake = gateway
        .tool(&client, "agentpalace_wake_up", json!({"agent_name":"remote-e2e","latest_limit":5}))
        .await;
    assert_eq!(wake["remote_changes"]["hub"]["unreachable"], true, "{wake}");
    let local_search = gateway
        .tool(
            &client,
            "agentpalace_search",
            json!({
                "query":"Local orchard watering schedule.","wing":"wing_local","limit":5
            }),
        )
        .await;
    assert!(!local_search["results"].as_array().unwrap().is_empty(), "{local_search}");
    eprintln!("PASS: remote outage reported and local MCP search remains available");
}

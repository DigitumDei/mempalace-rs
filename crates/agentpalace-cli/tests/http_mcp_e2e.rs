//! Exercise HTTP MCP and REST through the shipped executable.
#![allow(clippy::unwrap_used)]
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
    time::Duration,
};

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn single_executable_serves_mcp_and_rest_with_authentication() {
    let temp = tempfile::tempdir().unwrap();
    let tokens = temp.path().join("tokens.json");
    std::fs::write(
        &tokens,
        json!([
            {"name":"owner","token":"owner-secret","enabled":true},
            {"name":"limited","token":"limited-secret","enabled":true,"scopes":[]}
        ])
        .to_string(),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tokens, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let mut server = Server(
        Command::new(env!("CARGO_BIN_EXE_agentpalace"))
            .args(["serve", "--bind", "127.0.0.1:0", "--token-file"])
            .arg(&tokens)
            .env("AGENTPALACE_CONFIG_DIR", temp.path())
            .env("AGENTPALACE_PALACE_PATH", temp.path().join("palace"))
            .env("AGENTPALACE_STUB_EMBEDDINGS", "1")
            .env_remove("AGENTPALACE_LINEAGE_ID")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stderr = server.0.stderr.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            if let Some(url) = line.strip_prefix("Listening on ") {
                let _ = tx.send(url.to_owned());
            }
        }
    });
    let base = rx.recv_timeout(Duration::from_secs(30)).unwrap();
    let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build().unwrap();
    assert_eq!(client.get(format!("{base}/v1/health")).send().await.unwrap().status(), 200);
    let post = |token: &str, body: Value| {
        client
            .post(format!("{base}/mcp"))
            .bearer_auth(token)
            .header("accept", "application/json, text/event-stream")
            .json(&body)
    };
    let initialize = json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1"}}});
    assert_eq!(post("wrong", initialize.clone()).send().await.unwrap().status(), 401);
    assert_eq!(post("limited-secret", initialize.clone()).send().await.unwrap().status(), 403);
    assert_eq!(
        post("owner-secret", initialize.clone())
            .header("origin", "https://evil.example")
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let response: Value =
        post("owner-secret", initialize).send().await.unwrap().json().await.unwrap();
    assert_eq!(response["result"]["protocolVersion"], "2025-03-26");
    assert_eq!(
        post("owner-secret", json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
            .send()
            .await
            .unwrap()
            .status(),
        202
    );
    let tools: Value = post("owner-secret", json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        tools["result"]["tools"].as_array().unwrap().len(),
        agentpalace_mcp::tool_definitions().len()
    );
    let status: Value = post("owner-secret", json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"agentpalace_status","arguments":{}}})).send().await.unwrap().json().await.unwrap();
    assert!(status.get("result").is_some(), "{status}");
    assert_eq!(client.get(format!("{base}/mcp")).send().await.unwrap().status(), 405);
    assert_eq!(
        post("owner-secret", json!({"jsonrpc":"2.0","id":4,"method":"ping"}))
            .header("mcp-protocol-version", "unknown")
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    // A malformed later item must reject the batch before an earlier write runs.
    for invalid in [json!(42), json!({"jsonrpc":"1.0","id":6,"method":"ping"})] {
        let batch = json!([
            {"jsonrpc":"2.0","id":5,"method":"tools/call","params":{
                "name":"agentpalace_add_drawer","arguments":{
                    "wing":"wing_batch_rejected","room":"general",
                    "content":"Rejected batch must never store this telescope record.",
                    "added_by":"batch-test"
                }
            }},
            invalid
        ]);
        assert_eq!(post("owner-secret", batch).send().await.unwrap().status(), 400);
    }
    let search: Value = post("owner-secret", json!({"jsonrpc":"2.0","id":7,"method":"tools/call","params":{
        "name":"agentpalace_search","arguments":{"query":"telescope record","wing":"wing_batch_rejected","limit":5}
    }})).send().await.unwrap().json().await.unwrap();
    let payload = agentpalace_mcp::decode_tool_payload(&search).unwrap();
    assert!(payload["results"].as_array().unwrap().is_empty(), "{search}");
}

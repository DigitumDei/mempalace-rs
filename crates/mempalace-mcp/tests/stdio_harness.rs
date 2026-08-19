use std::io::{BufRead, BufReader as StdBufReader, Write};
use std::process::{Command, Stdio};

use mempalace_config::{
    FederationRuntimeConfig, LowCpuRuntimeConfig, MaintenanceRuntimeConfig, MempalaceConfig,
    ServerRuntimeConfig,
};
use mempalace_core::EmbeddingProfile;
use mempalace_mcp::{DeterministicStubProvider, McpServer, serve_transport};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

async fn test_server(tempdir: &TempDir) -> McpServer<DeterministicStubProvider> {
    let config = MempalaceConfig {
        schema_version: 1,
        collection_name: "mempalace_drawers".to_owned(),
        palace_path: tempdir.path().join("palace"),
        embedding_profile: EmbeddingProfile::Balanced,
        low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
        server: ServerRuntimeConfig {
            bind: "127.0.0.1:8765".parse().unwrap(),
            token_file: tempdir.path().join("server_tokens.json"),
            checkouts: std::collections::BTreeMap::new(),
        },
        federation: FederationRuntimeConfig::default(),
        maintenance: MaintenanceRuntimeConfig::defaults(),
    };
    McpServer::from_parts(config, DeterministicStubProvider::new(EmbeddingProfile::Balanced))
        .await
        .unwrap()
}

#[tokio::test]
async fn server_handles_initialize_and_tools_list_over_transport() {
    let tempdir = TempDir::new().unwrap();
    let server = test_server(&tempdir).await;
    let input = concat!(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}\n",
        "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{}}\n"
    );

    let (client, server_stream) = tokio::io::duplex(8_192);
    let (reader_half, writer_half) = tokio::io::split(server_stream);
    let task = tokio::spawn(async move {
        serve_transport(&server, BufReader::new(reader_half), writer_half).await.unwrap();
    });

    let (mut client_reader, mut client_writer) = tokio::io::split(client);
    client_writer.write_all(input.as_bytes()).await.unwrap();
    client_writer.shutdown().await.unwrap();

    let mut output = String::new();
    client_reader.read_to_string(&mut output).await.unwrap();
    task.await.unwrap();

    let lines = output.lines().collect::<Vec<_>>();
    let initialize: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    let tools: serde_json::Value = serde_json::from_str(lines[1]).unwrap();

    assert_eq!(initialize["result"]["protocolVersion"], "2024-11-05");
    assert!(
        tools["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "mempalace_status")
    );
}

#[tokio::test]
async fn server_returns_parse_error_for_invalid_json() {
    let tempdir = TempDir::new().unwrap();
    let server = test_server(&tempdir).await;
    let (client, server_stream) = tokio::io::duplex(4_096);
    let (reader_half, writer_half) = tokio::io::split(server_stream);
    let task = tokio::spawn(async move {
        serve_transport(&server, BufReader::new(reader_half), writer_half).await.unwrap();
    });

    let (mut client_reader, mut client_writer) = tokio::io::split(client);
    client_writer.write_all(b"{\n").await.unwrap();
    client_writer.shutdown().await.unwrap();

    let mut output = String::new();
    client_reader.read_to_string(&mut output).await.unwrap();
    task.await.unwrap();

    let response: serde_json::Value = serde_json::from_str(output.trim()).unwrap();
    assert_eq!(response["error"]["code"], -32700);
}

#[tokio::test]
async fn server_handles_embedding_backed_tool_calls_over_transport() {
    let tempdir = TempDir::new().unwrap();
    let server = test_server(&tempdir).await;
    let input = concat!(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"mempalace_add_drawer\",\"arguments\":{\"wing\":\"wing_code\",\"room\":\"backend\",\"content\":\"Rust MCP transport coverage note\",\"source_file\":\"transport.md\",\"added_by\":\"stdio-test\"}}}\n",
        "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"mempalace_check_duplicate\",\"arguments\":{\"content\":\"Rust MCP transport coverage note\",\"threshold\":0.9}}}\n",
        "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\",\"params\":{\"name\":\"mempalace_diary_write\",\"arguments\":{\"agent_name\":\"Transport Bot\",\"entry\":\"SESSION:2026-04-17|transport.coverage\",\"summary\":\"Transport coverage complete.\",\"topic\":\"transport\"}}}\n",
        "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"tools/call\",\"params\":{\"name\":\"mempalace_diary_read\",\"arguments\":{\"agent_name\":\"Transport Bot\",\"last_n\":1}}}\n"
    );

    let (client, server_stream) = tokio::io::duplex(16_384);
    let (reader_half, writer_half) = tokio::io::split(server_stream);
    let task = tokio::spawn(async move {
        serve_transport(&server, BufReader::new(reader_half), writer_half).await.unwrap();
    });

    let (mut client_reader, mut client_writer) = tokio::io::split(client);
    client_writer.write_all(input.as_bytes()).await.unwrap();
    client_writer.shutdown().await.unwrap();

    let mut output = String::new();
    client_reader.read_to_string(&mut output).await.unwrap();
    task.await.unwrap();

    let lines = output.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 4);

    let add: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    let duplicate: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
    let diary_write: serde_json::Value = serde_json::from_str(lines[2]).unwrap();
    let diary_read: serde_json::Value = serde_json::from_str(lines[3]).unwrap();

    let add_payload = mempalace_mcp::decode_tool_payload(&add).unwrap();
    assert_eq!(add_payload["success"], true);

    let duplicate_payload = mempalace_mcp::decode_tool_payload(&duplicate).unwrap();
    assert_eq!(duplicate_payload["is_duplicate"], true);
    assert!(!duplicate_payload["matches"].as_array().unwrap().is_empty());

    let diary_write_payload = mempalace_mcp::decode_tool_payload(&diary_write).unwrap();
    assert_eq!(diary_write_payload["success"], true);

    let diary_read_payload = mempalace_mcp::decode_tool_payload(&diary_read).unwrap();
    assert_eq!(diary_read_payload["agent"], "Transport Bot");
    assert_eq!(diary_read_payload["showing"], 1);
    assert_eq!(diary_read_payload["entries"][0]["topic"], "transport");
}

#[test]
fn compiled_binary_serves_stdio_with_stub_embeddings() {
    let tempdir = TempDir::new().unwrap();
    let home_dir = tempdir.path().join("home");
    std::fs::create_dir_all(&home_dir).unwrap();
    let palace_path = tempdir.path().join("palace");
    let mut child = Command::new(env!("CARGO_BIN_EXE_mempalace-mcp"))
        .env("HOME", &home_dir)
        .env("MEMPALACE_PALACE_PATH", &palace_path)
        .env("MEMPALACE_STUB_EMBEDDINGS", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    {
        let mut stdin = child.stdin.take().unwrap();
        writeln!(
            stdin,
            "{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{{}}}}"
        )
        .unwrap();
        writeln!(
            stdin,
            "{{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{{\"name\":\"mempalace_status\",\"arguments\":{{}}}}}}"
        )
        .unwrap();
    }

    let stdout = child.stdout.take().unwrap();
    let mut reader = StdBufReader::new(stdout);
    let mut initialize = String::new();
    let mut status = String::new();
    reader.read_line(&mut initialize).unwrap();
    reader.read_line(&mut status).unwrap();

    let initialize: serde_json::Value = serde_json::from_str(initialize.trim()).unwrap();
    let status: serde_json::Value = serde_json::from_str(status.trim()).unwrap();
    assert_eq!(initialize["result"]["protocolVersion"], "2024-11-05");
    assert_eq!(mempalace_mcp::decode_tool_payload(&status).unwrap()["total_drawers"], 0);

    let exit = child.wait().unwrap();
    assert!(exit.success());
}

#[test]
fn compiled_binary_falls_back_for_missing_bound_lineage_and_explains_creation() {
    let tempdir = TempDir::new().unwrap();
    let home_dir = tempdir.path().join("home");
    std::fs::create_dir_all(&home_dir).unwrap();
    let palace_path = tempdir.path().join("palace");
    let mut child = Command::new(env!("CARGO_BIN_EXE_mempalace-mcp"))
        .env("HOME", &home_dir)
        .env("MEMPALACE_PALACE_PATH", &palace_path)
        .env("MEMPALACE_STUB_EMBEDDINGS", "1")
        .env("MEMPALACE_LINEAGE_ID", "codex-dion")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    {
        let mut stdin = child.stdin.take().unwrap();
        writeln!(
            stdin,
            "{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{{\"name\":\"mempalace_lineage_set\",\"arguments\":{{\"lineage_id\":\"default-lineage\",\"display_name\":\"Default lineage\",\"description\":\"The palace default for fallback testing.\",\"expected_revision\":0,\"set_default\":true,\"actor\":\"stdio-test\"}}}}}}"
        )
        .unwrap();
        writeln!(
            stdin,
            "{{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{{\"name\":\"mempalace_identity_packet\",\"arguments\":{{}}}}}}"
        )
        .unwrap();
    }

    let stdout = child.stdout.take().unwrap();
    let mut reader = StdBufReader::new(stdout);
    let mut created = String::new();
    let mut response = String::new();
    reader.read_line(&mut created).unwrap();
    reader.read_line(&mut response).unwrap();
    let created: serde_json::Value = serde_json::from_str(created.trim()).unwrap();
    let response: serde_json::Value = serde_json::from_str(response.trim()).unwrap();
    assert_eq!(mempalace_mcp::decode_tool_payload(&created).unwrap()["success"], true);
    let packet = mempalace_mcp::decode_tool_payload(&response).unwrap();
    assert_eq!(packet["lineage"]["lineage_id"], "default-lineage");
    assert_eq!(packet["lineage_selection"]["source"], "palace_default_fallback");
    assert_eq!(packet["lineage_selection"]["requested_lineage_id"], "codex-dion");
    let message = packet["lineage_selection"]["message"].as_str().unwrap();
    assert!(message.contains("MEMPALACE_LINEAGE_ID"));
    assert!(message.contains("codex-dion"));
    assert!(message.contains("mempalace_lineage_set"));
    assert!(message.contains("expected_revision 0"));

    let exit = child.wait().unwrap();
    assert!(exit.success());
}

#[test]
fn compiled_binary_rejects_invalid_environment_lineage_id_at_startup() {
    let tempdir = TempDir::new().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_mempalace-mcp"))
        .env("HOME", tempdir.path())
        .env("MEMPALACE_PALACE_PATH", tempdir.path().join("palace"))
        .env("MEMPALACE_STUB_EMBEDDINGS", "1")
        .env("MEMPALACE_LINEAGE_ID", "not a valid id")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("InvalidLineageBinding"));
    assert!(stderr.contains("may contain only ASCII letters"));
}

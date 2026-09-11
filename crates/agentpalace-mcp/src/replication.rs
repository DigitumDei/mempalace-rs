use std::collections::BTreeMap;
use std::sync::Arc;

use agentpalace_federation::{AddDrawerRequest, KgAddFactRequest, KgInvalidateRequest};
use agentpalace_remote::{RemoteApi, RemoteError};
use agentpalace_storage::{OutboxOperation, OutboxStore, RevisionedWrite};
use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};

use crate::metrics::PhaseMeter;

const WORKER_ID: &str = "mcp-replication-worker";
const LEASE_TTL: Duration = Duration::seconds(30);
const IDLE_POLL: std::time::Duration = std::time::Duration::from_millis(250);
const MAX_BACKOFF_SECONDS: i64 = 300;
pub(crate) const OUTBOX_ACTOR: &str = "mcp-federation";
pub(crate) const OUTBOX_MAX_ATTEMPTS: i64 = 10;

/// Durable payload stored in the replication outbox. Every mutation carries its stable
/// operation id at delivery time; the id is deliberately not duplicated inside this value.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum ReplicationMutation {
    IngestFile {
        request: agentpalace_federation::IngestBatchRequest,
    },
    DrawerAdd {
        request: AddDrawerRequest,
    },
    DrawerDelete {
        drawer_id: String,
    },
    KgAdd {
        request: KgAddFactRequest,
        /// Local source provenance used for idempotency checks. The remote wire DTO does not
        /// carry this field, but a keyed local retry must still reject a provenance mismatch.
        #[serde(default)]
        source_closet: Option<String>,
    },
    KgInvalidate {
        request: KgInvalidateRequest,
    },
}

impl ReplicationMutation {
    pub(crate) fn into_value(self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self)
    }
}

/// Run one durable replication dispatcher for the lifetime of the MCP process.
///
/// Delivery is sequential per configured remote. SQLite claim leases provide cross-process
/// exclusion, while the outbox's ordering key keeps mutations for one logical entity ordered.
pub(crate) async fn run_replication_worker(
    outbox: OutboxStore,
    remotes: BTreeMap<String, Arc<dyn RemoteApi>>,
    metrics: PhaseMeter,
) {
    loop {
        let mut did_work = false;
        for (remote_name, remote) in &remotes {
            match claim_one(&outbox, remote_name) {
                Ok(Some(operation)) => {
                    did_work = true;
                    deliver_claimed(&outbox, remote.as_ref(), operation, &metrics).await;
                }
                Ok(None) => {}
                Err(error) => tracing::warn!(
                    remote = %remote_name,
                    %error,
                    "failed to claim durable replication operation"
                ),
            }
        }
        if !did_work {
            tokio::time::sleep(IDLE_POLL).await;
        }
    }
}

fn claim_one(
    outbox: &OutboxStore,
    remote_name: &str,
) -> agentpalace_storage::Result<Option<OutboxOperation>> {
    if let Some(operation) =
        outbox.reclaim_expired_lease(Some(remote_name), WORKER_ID, LEASE_TTL)?
    {
        return Ok(Some(operation));
    }
    outbox.claim_next(remote_name, WORKER_ID, LEASE_TTL)
}

async fn deliver_claimed(
    outbox: &OutboxStore,
    remote: &dyn RemoteApi,
    operation: OutboxOperation,
    metrics: &PhaseMeter,
) {
    let started_at = std::time::Instant::now();
    let queue_age_ms = (OffsetDateTime::now_utc() - operation.created_at).whole_milliseconds();
    metrics.record("outbox_wait", std::time::Duration::from_millis(queue_age_ms.max(0) as u64));
    let result = deliver(remote, &operation).await;
    let elapsed_ms = started_at.elapsed().as_millis() as u64;
    metrics.record("delivery_attempt", std::time::Duration::from_millis(elapsed_ms));
    match result {
        Ok(()) => {
            match outbox.acknowledge(&operation.operation_id, WORKER_ID, operation.revision) {
                Ok(RevisionedWrite::Applied(_)) => {
                    metrics.record("remote_acknowledge", started_at.elapsed());
                    tracing::info!(
                        operation_id = %operation.operation_id,
                        remote = %operation.destination_remote,
                        attempt = operation.attempt_count + 1,
                        elapsed_ms,
                        queue_age_ms = (OffsetDateTime::now_utc() - operation.created_at)
                            .whole_milliseconds(),
                        "durable replication operation acknowledged"
                    );
                }
                Ok(RevisionedWrite::Conflict { actual_revision }) => {
                    tracing::warn!(
                        operation_id = %operation.operation_id,
                        ?actual_revision,
                        "remote mutation succeeded but outbox acknowledgement lost a revision race; safe replay will follow"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        operation_id = %operation.operation_id,
                        %error,
                        "remote mutation succeeded but outbox acknowledgement failed; safe replay will follow"
                    );
                }
            }
        }
        Err(error) if error.is_retryable() => {
            let retry_at = OffsetDateTime::now_utc() + retry_backoff(&operation);
            match outbox.schedule_retry(
                &operation.operation_id,
                WORKER_ID,
                operation.revision,
                &bounded_error(&error),
                retry_at,
            ) {
                Ok(RevisionedWrite::Applied(_)) => {
                    tracing::warn!(
                        operation_id = %operation.operation_id,
                        remote = %operation.destination_remote,
                        attempt = operation.attempt_count + 1,
                        retry_at = %retry_at,
                        elapsed_ms,
                        %error,
                        "durable replication attempt will retry"
                    );
                }
                Ok(RevisionedWrite::Conflict { actual_revision }) => {
                    tracing::warn!(
                        operation_id = %operation.operation_id,
                        ?actual_revision,
                        "failed to persist durable replication retry because the lease was reclaimed"
                    );
                }
                Err(store_error) => {
                    tracing::warn!(
                        operation_id = %operation.operation_id,
                        %store_error,
                        "failed to persist durable replication retry"
                    );
                }
            }
        }
        Err(error) => {
            match outbox.fail(
                &operation.operation_id,
                WORKER_ID,
                operation.revision,
                &bounded_error(&error),
            ) {
                Ok(RevisionedWrite::Applied(_)) => {
                    tracing::error!(
                        operation_id = %operation.operation_id,
                        remote = %operation.destination_remote,
                        attempt = operation.attempt_count + 1,
                        elapsed_ms,
                        %error,
                        "durable replication operation failed permanently"
                    );
                }
                Ok(RevisionedWrite::Conflict { actual_revision }) => {
                    tracing::warn!(
                        operation_id = %operation.operation_id,
                        ?actual_revision,
                        "failed to persist terminal replication failure because the lease was reclaimed"
                    );
                }
                Err(store_error) => {
                    tracing::warn!(
                        operation_id = %operation.operation_id,
                        %store_error,
                        "failed to persist terminal replication failure"
                    );
                }
            }
        }
    }
}

async fn deliver(remote: &dyn RemoteApi, operation: &OutboxOperation) -> Result<(), RemoteError> {
    let info = remote.info().await?;
    if !info.capabilities.iter().any(|value| value == "idempotent_mutations") {
        return Err(RemoteError::CapabilityMissing {
            remote: operation.destination_remote.clone(),
            capability: "idempotent_mutations".to_owned(),
        });
    }

    let mutation = serde_json::from_value::<ReplicationMutation>(operation.payload.clone())
        .map_err(|error| RemoteError::InvalidConfig {
            remote: operation.destination_remote.clone(),
            message: format!("invalid durable replication payload: {error}"),
        })?;
    match mutation {
        ReplicationMutation::IngestFile { request } => {
            if !info.capabilities.iter().any(|value| value == "resumable_ingest") {
                return Err(RemoteError::CapabilityMissing {
                    remote: operation.destination_remote.clone(),
                    capability: "resumable_ingest".to_owned(),
                });
            }
            let response = remote.ingest_batch(request.clone()).await?;
            let file = response
                .files
                .first()
                .filter(|file| {
                    response.files.len() == 1
                        && request
                            .files
                            .first()
                            .is_some_and(|input| input.relative_path == file.relative_path)
                })
                .ok_or_else(|| RemoteError::UnknownOutcome {
                    remote: operation.destination_remote.clone(),
                    message: "ingest response did not identify exactly one requested file"
                        .to_owned(),
                })?;
            match file.status.as_str() {
                "ingested" | "skipped_unchanged" | "removed" => Ok(()),
                "failed" => Err(RemoteError::InvalidConfig {
                    remote: operation.destination_remote.clone(),
                    message: file.error.clone().unwrap_or_else(|| "file rejected".to_owned()),
                }),
                _ => Err(RemoteError::UnknownOutcome {
                    remote: operation.destination_remote.clone(),
                    message: file
                        .error
                        .clone()
                        .unwrap_or_else(|| "file outcome unknown".to_owned()),
                }),
            }
        }
        ReplicationMutation::DrawerAdd { mut request } => {
            request.operation_id = Some(operation.operation_id.clone());
            let response = remote.add_drawer(request).await?;
            if response.success {
                Ok(())
            } else {
                Err(RemoteError::InvalidResponse {
                    remote: operation.destination_remote.clone(),
                    message: "drawer add returned success=false".to_owned(),
                })
            }
        }
        ReplicationMutation::DrawerDelete { drawer_id } => {
            remote.delete_drawer_with_operation_id(&drawer_id, Some(&operation.operation_id)).await
        }
        ReplicationMutation::KgAdd { mut request, .. } => {
            request.operation_id = Some(operation.operation_id.clone());
            remote.kg_add_fact(request).await.map(|_| ())
        }
        ReplicationMutation::KgInvalidate { mut request } => {
            request.operation_id = Some(operation.operation_id.clone());
            remote.kg_invalidate(request).await.map(|_| ())
        }
    }
}

fn retry_backoff(operation: &OutboxOperation) -> Duration {
    let exponent = u32::try_from(operation.attempt_count.clamp(0, 8)).unwrap_or(8);
    let base = 1_i64.checked_shl(exponent).unwrap_or(MAX_BACKOFF_SECONDS);
    let capped = base.min(MAX_BACKOFF_SECONDS);
    let hash =
        blake3::hash(format!("{}:{}", operation.operation_id, operation.attempt_count).as_bytes());
    let jitter_millis = u16::from_le_bytes([hash.as_bytes()[0], hash.as_bytes()[1]]) as i64 % 1_000;
    Duration::seconds(capped) + Duration::milliseconds(jitter_millis)
}

fn bounded_error(error: &RemoteError) -> String {
    let mut message = error.to_string();
    if message.len() > 1_024 {
        let mut boundary = 1_024;
        while boundary > 0 && !message.is_char_boundary(boundary) {
            boundary -= 1;
        }
        message.truncate(boundary);
    }
    message
}

pub(crate) fn expect_applied<T>(write: RevisionedWrite<T>, action: &str) -> Result<T, String> {
    match write {
        RevisionedWrite::Applied(value) => Ok(value),
        RevisionedWrite::Conflict { actual_revision } => Err(format!(
            "outbox {action} lost a revision race (actual revision {actual_revision:?})"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn ingest_worker_resumes_partial_batch_after_lost_ack_and_keeps_terminal_failure() {
        use agentpalace_config::{
            FederationRuntimeConfig, LowCpuRuntimeConfig, MaintenanceRuntimeConfig,
            AgentPalaceConfig, ServerRuntimeConfig,
        };
        use agentpalace_core::EmbeddingProfile;
        use agentpalace_remote::{RemoteClient, RemoteEndpoint};
        use agentpalace_storage::{NewOutboxOperation, OutboxState};
        use serde_json::json;
        let directory = tempfile::tempdir().unwrap();
        let token_path = directory.path().join("tokens.json");
        std::fs::write(&token_path, r#"[{"token":"test-token","name":"test","enabled":true}]"#)
            .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&token_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let config = AgentPalaceConfig {
            schema_version: 1,
            collection_name: "agentpalace_drawers".into(),
            palace_path: directory.path().join("remote"),
            embedding_profile: EmbeddingProfile::Balanced,
            low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
            maintenance: MaintenanceRuntimeConfig::defaults(),
            federation: FederationRuntimeConfig::default(),
            server: ServerRuntimeConfig {
                bind: "127.0.0.1:0".parse().unwrap(),
                token_file: token_path.clone(),
                checkouts: BTreeMap::new(),
            },
        };
        let (router, state) = agentpalace_server::build_router(
            config,
            agentpalace_embeddings::DeterministicStubProvider::new(EmbeddingProfile::Balanced),
            agentpalace_server::TokenRegistry::load(token_path).unwrap(),
        )
        .await
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let remote = RemoteClient::new(RemoteEndpoint {
            name: "hub".into(),
            base_url: format!("http://{address}"),
            token: Some("test-token".into()),
            timeout: std::time::Duration::from_secs(2),
        })
        .unwrap();
        let offline = RemoteClient::new(RemoteEndpoint {
            name: "hub".into(),
            base_url: "http://127.0.0.1:1".into(),
            token: Some("test-token".into()),
            timeout: std::time::Duration::from_millis(100),
        })
        .unwrap();
        let path = directory.path().join("outbox.sqlite3");
        let outbox = OutboxStore::new(&path);
        outbox.ensure_schema().unwrap();
        for (id, file) in [("first", "first.rs"), ("second", "second.rs"), ("bad", "../bad")] {
            let request: agentpalace_federation::IngestBatchRequest = serde_json::from_value(json!({
                "wing":"wing_test", "repo_id":"repo", "replication":{"batch_id":"batch1","record_id":id},
                "files":[{"relative_path":file,"content_hash":id,"chunks":[
                    {"chunk_index":0,"room":"general","text":"durable worker source content"}]}]
            })).unwrap();
            let operation = outbox
                .enqueue(&NewOutboxOperation {
                    created_by: "test".into(),
                    idempotency_key: id.into(),
                    mutation_kind: "ingest_file".into(),
                    entity_id: file.into(),
                    destination_remote: "hub".into(),
                    ordering_key: file.into(),
                    payload: json!({"kind":"ingest_file","request":request}),
                    max_attempts: 10,
                })
                .unwrap();
            outbox.activate(&operation.operation_id, operation.revision).unwrap();
        }
        let metrics = PhaseMeter::default();
        let first = claim_one(&outbox, "hub").unwrap().unwrap();
        deliver_claimed(&outbox, &remote, first.clone(), &metrics).await;
        let second = claim_one(&outbox, "hub").unwrap().unwrap();
        deliver_claimed(&outbox, &offline, second.clone(), &metrics).await;
        let bad = claim_one(&outbox, "hub").unwrap().unwrap();
        deliver_claimed(&outbox, &remote, bad.clone(), &metrics).await;
        let status = outbox.ingestion_backlog().unwrap();
        assert_eq!(status["pending_batches"], 1);
        assert_eq!(status["retryable_batches"], 1);
        assert_eq!(status["failed_batches"], 1);
        assert_eq!(status["files_by_state"]["replicated"], 1);
        assert!(status["oldest_pending_age_seconds"].as_i64().is_some());
        drop(outbox);
        // Reopen after a process restart; make the persisted retry due without a wall-clock wait.
        rusqlite::Connection::open(&path).unwrap().execute(
            "UPDATE replication_outbox SET retry_after='2000-01-01T00:00:00Z' WHERE state='retryable'", []).unwrap();
        let outbox = OutboxStore::new(&path);
        let recovered = claim_one(&outbox, "hub").unwrap().unwrap();
        assert_eq!(recovered.operation_id, second.operation_id);
        // Crash after remote application but before durable acknowledgement.
        deliver(&remote, &recovered).await.unwrap();
        rusqlite::Connection::open(&path).unwrap().execute(
            "UPDATE replication_outbox SET lease_expires_at='2000-01-01T00:00:00Z' WHERE state='leased'", []).unwrap();
        drop(outbox);
        let outbox = OutboxStore::new(&path);
        let replay = claim_one(&outbox, "hub").unwrap().unwrap();
        assert_eq!(replay.operation_id, second.operation_id);
        deliver_claimed(&outbox, &remote, replay, &metrics).await;
        assert!(claim_one(&outbox, "hub").unwrap().is_none());
        assert_eq!(outbox.get_operation(&first.operation_id).unwrap().unwrap().attempt_count, 0);
        assert_eq!(
            outbox.get_operation(&bad.operation_id).unwrap().unwrap().state,
            OutboxState::Failed
        );
        assert_eq!(outbox.ingestion_backlog().unwrap()["pending_batches"], 0);
        assert_eq!(
            state.storage.receipt_store().get_receipt("second").unwrap().unwrap().status,
            agentpalace_storage::ReceiptState::Completed
        );
        handle.abort();
    }

    #[test]
    fn retry_backoff_is_bounded_and_stable() {
        let operation = OutboxOperation {
            operation_id: "op-1".to_owned(),
            sequence: 1,
            created_by: "test".to_owned(),
            idempotency_key: "test".to_owned(),
            mutation_kind: "test".to_owned(),
            entity_id: "entity".to_owned(),
            destination_remote: "remote".to_owned(),
            ordering_key: "entity".to_owned(),
            entity_sequence: 1,
            state: agentpalace_storage::OutboxState::Leased,
            revision: 1,
            lease_owner: Some(WORKER_ID.to_owned()),
            lease_expires_at: None,
            attempt_count: 99,
            max_attempts: 10,
            retry_after: None,
            last_error: None,
            payload: serde_json::json!({}),
            created_at: OffsetDateTime::now_utc(),
            updated_at: OffsetDateTime::now_utc(),
        };
        let first = retry_backoff(&operation);
        assert_eq!(first, retry_backoff(&operation));
        assert!(first >= Duration::seconds(256));
        assert!(first < Duration::seconds(MAX_BACKOFF_SECONDS + 1));
    }

    #[test]
    fn bounded_error_truncates_on_utf8_char_boundary_without_panic() {
        // Construct a *formatted* error whose byte 1024 lands in the middle of a multibyte
        // UTF-8 sequence. `bounded_error` truncates `to_string()`, which includes this prefix.
        let prefix =
            RemoteError::Unreachable { remote: "actuarius".into(), message: String::new() }
                .to_string();
        let mut msg = "a".repeat(1023 - prefix.len());
        msg.push('\u{2014}');
        msg.push_str("extra content after the boundary");

        let err = RemoteError::Unreachable { remote: "actuarius".into(), message: msg };
        let full = err.to_string();
        assert!(full.len() > 1024);
        assert_eq!(full.as_bytes()[1023], 0xE2);
        assert!(!full.is_char_boundary(1024));
        let truncated = bounded_error(&err);
        assert!(truncated.len() <= 1024);
        assert!(truncated.is_char_boundary(truncated.len()));
    }
}

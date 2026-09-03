use std::collections::BTreeMap;
use std::sync::Arc;

use mempalace_federation::{AddDrawerRequest, KgAddFactRequest, KgInvalidateRequest};
use mempalace_remote::{RemoteApi, RemoteError};
use mempalace_storage::{OutboxOperation, OutboxStore, RevisionedWrite};
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
) -> mempalace_storage::Result<Option<OutboxOperation>> {
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
            state: mempalace_storage::OutboxState::Leased,
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

//! The envelope-as-artifact isolation mechanism.
//!
//! No A2A field may become a column in MemPalace's core schema (see the crate-level docs).
//! Fields with no internal home — and the pre-coercion original state, whenever a state mapping
//! coerces — are preserved by storing the inbound A2A envelope verbatim as an immutable
//! artifact with [`PROTOCOL_ENVELOPE_ROLE`] and [`PROTOCOL_ENVELOPE_MEDIA_TYPE`], referenced
//! from the task. This reuses the artifact mechanism that already exists in
//! `mempalace_storage::coordination`, keeps the core schema untouched, and keeps the exchange
//! auditable: the original A2A state is always recoverable from the envelope even when a
//! coercion changed what MemPalace recorded as the task's own state.

use mempalace_storage::NewArtifact;
use serde_json::Value;

use crate::error::A2aError;

/// Artifact `role` used for a stored inbound A2A envelope.
pub const PROTOCOL_ENVELOPE_ROLE: &str = "protocol_envelope";
/// Artifact `media_type` used for a stored inbound A2A envelope.
pub const PROTOCOL_ENVELOPE_MEDIA_TYPE: &str = "application/vnd.a2a+json";

/// Build a [`NewArtifact`] that stores `envelope` verbatim as the task's inbound A2A protocol
/// envelope.
///
/// `task_id` is the MemPalace task the envelope maps to; `created_by` is the actor recorded as
/// having submitted it (the adapter's caller resolves this from its own authentication, the
/// same way `mempalace_storage::coordination::NewArtifact::created_by` is resolved everywhere
/// else).
///
/// # Idempotency key derivation
///
/// `idempotency_key` is `"a2a_envelope:{task_id}:{hash}"`, where `hash` is the full BLAKE3 hex
/// digest of the envelope's canonical JSON serialization, computed the same way
/// `mempalace_storage::coordination` hashes artifact content
/// (`blake3::hash(bytes).to_hex().to_string()`). Keying on both the task and the content means:
///
/// - A byte-identical retry of the same envelope for the same task replays the same artifact
///   row (storage's own idempotency semantics), rather than creating a duplicate.
/// - A genuinely different envelope for the same task (e.g. a later A2A message that legally
///   carries a different state) gets its own artifact row instead of colliding with — and
///   silently losing — the earlier one.
pub fn envelope_artifact(
    task_id: &str,
    created_by: &str,
    envelope: &Value,
) -> Result<NewArtifact, A2aError> {
    let content = serde_json::to_string(envelope)?;
    let hash = blake3::hash(content.as_bytes()).to_hex().to_string();
    let idempotency_key = format!("a2a_envelope:{task_id}:{hash}");
    Ok(NewArtifact {
        task_id: task_id.to_owned(),
        created_by: created_by.to_owned(),
        role: PROTOCOL_ENVELOPE_ROLE.to_owned(),
        media_type: PROTOCOL_ENVELOPE_MEDIA_TYPE.to_owned(),
        content,
        idempotency_key,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn envelope_artifact_carries_role_and_media_type() {
        let envelope = json!({"state": "TASK_STATE_AUTH_REQUIRED"});
        let artifact = envelope_artifact("task_1", "alice", &envelope).unwrap();
        assert_eq!(artifact.role, PROTOCOL_ENVELOPE_ROLE);
        assert_eq!(artifact.media_type, PROTOCOL_ENVELOPE_MEDIA_TYPE);
        assert_eq!(artifact.task_id, "task_1");
        assert_eq!(artifact.created_by, "alice");
    }

    #[test]
    fn envelope_artifact_preserves_the_coerced_state_verbatim() {
        // The task-level mapping coerces TASK_STATE_AUTH_REQUIRED -> InputRequired, but the envelope
        // artifact must still carry the original A2A state exactly as received.
        let envelope = json!({"status": {"state": "TASK_STATE_AUTH_REQUIRED"}, "id": "task_1"});
        let artifact = envelope_artifact("task_1", "alice", &envelope).unwrap();
        let decoded: Value = serde_json::from_str(&artifact.content).unwrap();
        assert_eq!(decoded["status"]["state"], "TASK_STATE_AUTH_REQUIRED");
    }

    #[test]
    fn idempotency_key_is_deterministic_for_identical_content() {
        let envelope = json!({"a": 1});
        let first = envelope_artifact("task_1", "alice", &envelope).unwrap();
        let second = envelope_artifact("task_1", "alice", &envelope).unwrap();
        assert_eq!(first.idempotency_key, second.idempotency_key);
    }

    #[test]
    fn idempotency_key_differs_for_different_content_on_the_same_task() {
        let first = envelope_artifact("task_1", "alice", &json!({"state": "TASK_STATE_SUBMITTED"})).unwrap();
        let second = envelope_artifact("task_1", "alice", &json!({"state": "TASK_STATE_WORKING"})).unwrap();
        assert_ne!(first.idempotency_key, second.idempotency_key);
    }

    #[test]
    fn idempotency_key_differs_for_different_tasks_with_identical_content() {
        let envelope = json!({"state": "TASK_STATE_SUBMITTED"});
        let first = envelope_artifact("task_1", "alice", &envelope).unwrap();
        let second = envelope_artifact("task_2", "alice", &envelope).unwrap();
        assert_ne!(first.idempotency_key, second.idempotency_key);
    }
}

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

/// Artifact `role` used for a stored inbound A2A envelope.
pub const PROTOCOL_ENVELOPE_ROLE: &str = "protocol_envelope";
/// Artifact `media_type` used for a stored inbound A2A envelope.
pub const PROTOCOL_ENVELOPE_MEDIA_TYPE: &str = "application/vnd.a2a+json";

/// Build a [`NewArtifact`] that stores `envelope_json` verbatim as the task's inbound A2A
/// protocol envelope.
///
/// `envelope_json` must be the **original JSON text as received on the wire** — the exact bytes
/// of the request body (or the relevant slice of it), not a re-serialization of a parsed
/// [`serde_json::Value`]. Re-serializing a parsed value is not verbatim: it normalises
/// whitespace, reorders object keys, collapses duplicate keys that were present on the wire, and
/// respells numbers (`1.0` becomes `1`, `1e3` becomes `1000`). Any of those changes would also
/// silently change the content hash below, defeating the idempotency guarantee this function
/// exists to provide. If the caller also needs a parsed [`serde_json::Value`] for translation,
/// it should parse a *separate* copy from `envelope_json` rather than feeding this function an
/// already-parsed-then-re-serialized value.
///
/// `task_id` is the MemPalace task the envelope maps to; `created_by` is the actor recorded as
/// having submitted it (the adapter's caller resolves this from its own authentication, the
/// same way `mempalace_storage::coordination::NewArtifact::created_by` is resolved everywhere
/// else).
///
/// # Idempotency key derivation
///
/// `idempotency_key` is `"a2a_envelope:{task_id}:{hash}"`, where `hash` is the full BLAKE3 hex
/// digest of `envelope_json`'s raw bytes, computed the same way `mempalace_storage::coordination`
/// hashes artifact content (`blake3::hash(bytes).to_hex().to_string()`). Keying on both the task
/// and the exact wire bytes means:
///
/// - A byte-identical retry of the same envelope for the same task replays the same artifact
///   row (storage's own idempotency semantics), rather than creating a duplicate.
/// - A genuinely different envelope for the same task (e.g. a later A2A message that legally
///   carries a different state, or the same logical content re-sent with different key order or
///   whitespace) gets its own artifact row instead of colliding with — and silently losing — the
///   earlier one.
pub fn envelope_artifact(task_id: &str, created_by: &str, envelope_json: &str) -> NewArtifact {
    let hash = blake3::hash(envelope_json.as_bytes()).to_hex().to_string();
    let idempotency_key = format!("a2a_envelope:{task_id}:{hash}");
    NewArtifact {
        task_id: task_id.to_owned(),
        created_by: created_by.to_owned(),
        role: PROTOCOL_ENVELOPE_ROLE.to_owned(),
        media_type: PROTOCOL_ENVELOPE_MEDIA_TYPE.to_owned(),
        content: envelope_json.to_owned(),
        idempotency_key,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use serde_json::Value;

    use super::*;

    #[test]
    fn envelope_artifact_carries_role_and_media_type() {
        let envelope = r#"{"state":"TASK_STATE_AUTH_REQUIRED"}"#;
        let artifact = envelope_artifact("task_1", "alice", envelope);
        assert_eq!(artifact.role, PROTOCOL_ENVELOPE_ROLE);
        assert_eq!(artifact.media_type, PROTOCOL_ENVELOPE_MEDIA_TYPE);
        assert_eq!(artifact.task_id, "task_1");
        assert_eq!(artifact.created_by, "alice");
    }

    #[test]
    fn envelope_artifact_preserves_the_coerced_state_verbatim() {
        // The task-level mapping coerces TASK_STATE_AUTH_REQUIRED -> InputRequired, but the envelope
        // artifact must still carry the original A2A state exactly as received.
        let envelope = r#"{"status":{"state":"TASK_STATE_AUTH_REQUIRED"},"id":"task_1"}"#;
        let artifact = envelope_artifact("task_1", "alice", envelope);
        let decoded: Value = serde_json::from_str(&artifact.content).unwrap();
        assert_eq!(decoded["status"]["state"], "TASK_STATE_AUTH_REQUIRED");
    }

    #[test]
    fn envelope_artifact_stores_the_exact_bytes_it_was_given() {
        // Not merely a value that parses the same way — the literal input string, whitespace,
        // key order and all.
        let envelope = "{\n  \"id\": \"task_1\",\n  \"status\": { \"state\": \"TASK_STATE_WORKING\" }\n}";
        let artifact = envelope_artifact("task_1", "alice", envelope);
        assert_eq!(artifact.content, envelope);
    }

    #[test]
    fn idempotency_key_is_deterministic_for_identical_content() {
        let envelope = r#"{"a":1}"#;
        let first = envelope_artifact("task_1", "alice", envelope);
        let second = envelope_artifact("task_1", "alice", envelope);
        assert_eq!(first.idempotency_key, second.idempotency_key);
    }

    #[test]
    fn idempotency_key_differs_for_different_content_on_the_same_task() {
        let first = envelope_artifact("task_1", "alice", r#"{"state":"TASK_STATE_SUBMITTED"}"#);
        let second = envelope_artifact("task_1", "alice", r#"{"state":"TASK_STATE_WORKING"}"#);
        assert_ne!(first.idempotency_key, second.idempotency_key);
    }

    #[test]
    fn idempotency_key_differs_for_different_tasks_with_identical_content() {
        let envelope = r#"{"state":"TASK_STATE_SUBMITTED"}"#;
        let first = envelope_artifact("task_1", "alice", envelope);
        let second = envelope_artifact("task_2", "alice", envelope);
        assert_ne!(first.idempotency_key, second.idempotency_key);
    }

    /// Regression guard for the P2 finding that `envelope_artifact` used to accept a *parsed*
    /// `serde_json::Value` and re-serialize it, which silently normalises key order and
    /// whitespace before hashing. Two envelopes that differ only in those respects — but parse
    /// to an equal `Value` — must now be treated as distinct: both preserved verbatim, each with
    /// its own idempotency key, so neither's audit record is lost to deduplication.
    #[test]
    fn two_envelopes_differing_only_in_key_order_and_whitespace_get_different_idempotency_keys_and_are_both_preserved()
     {
        let compact = r#"{"id":"task_1","status":{"state":"TASK_STATE_WORKING"}}"#;
        let reordered_and_padded =
            r#"{ "status": { "state": "TASK_STATE_WORKING" }, "id": "task_1" }"#;

        // Sanity check: these really do parse to an equal `Value`, which is exactly the case the
        // old `&Value`-based implementation would have collapsed into one artifact.
        let compact_value: Value = serde_json::from_str(compact).unwrap();
        let reordered_value: Value = serde_json::from_str(reordered_and_padded).unwrap();
        assert_eq!(compact_value, reordered_value);

        let first = envelope_artifact("task_1", "alice", compact);
        let second = envelope_artifact("task_1", "alice", reordered_and_padded);

        assert_ne!(first.idempotency_key, second.idempotency_key);
        assert_eq!(first.content, compact);
        assert_eq!(second.content, reordered_and_padded);
    }
}

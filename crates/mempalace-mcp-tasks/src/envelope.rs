//! The envelope-as-artifact isolation mechanism.
//!
//! No MCP Tasks field may become a column in MemPalace's core schema (see the crate-level docs).
//! Fields with no internal home — and the pre-coercion original status, whenever a status mapping
//! coerces — are preserved by storing the inbound MCP Tasks envelope verbatim as an immutable
//! artifact with [`PROTOCOL_ENVELOPE_ROLE`] and [`PROTOCOL_ENVELOPE_MEDIA_TYPE`], referenced from
//! the task. This is the same mechanism `mempalace_a2a::envelope` uses for A2A — same `role`
//! string (both adapters store "the whole inbound protocol envelope for this task", so sharing
//! the role is deliberate, not an oversight), but a **distinct `media_type`**
//! (`application/vnd.mcp.tasks+json`, vs. A2A's `application/vnd.a2a+json`) so a reader can tell
//! which protocol produced a given envelope artifact without parsing its content first.
//!
//! # Verbatim means verbatim
//!
//! Exactly the same requirement `mempalace_a2a::envelope::envelope_artifact` documents, and for
//! the same reason: `envelope_json` must be the **original JSON text as received on the wire**,
//! not a re-serialization of a parsed [`serde_json::Value`]. Re-serializing a parsed value
//! normalises whitespace, reorders object keys, collapses duplicate keys, and respells numbers —
//! any of which silently changes the content hash below, defeating the idempotency guarantee this
//! function exists to provide. `mempalace-a2a`'s first implementation of this function accepted a
//! parsed `Value` and had to be fixed for exactly this reason (see
//! `docs/Coordination-Phase-3-Design.md`'s regression note for
//! `two_envelopes_differing_only_in_key_order_and_whitespace_get_different_idempotency_keys_and_are_both_preserved`);
//! this crate takes `&str` from the start rather than repeating that mistake.

use mempalace_storage::NewArtifact;

/// Artifact `role` used for a stored inbound protocol envelope. Shared with
/// `mempalace_a2a::envelope::PROTOCOL_ENVELOPE_ROLE` — see the module docs for why the role is
/// shared while the media type is not.
pub const PROTOCOL_ENVELOPE_ROLE: &str = "protocol_envelope";
/// Artifact `media_type` used for a stored inbound MCP Tasks envelope. Distinct from A2A's
/// `application/vnd.a2a+json`.
pub const PROTOCOL_ENVELOPE_MEDIA_TYPE: &str = "application/vnd.mcp.tasks+json";

/// Build a [`NewArtifact`] that stores `envelope_json` verbatim as the task's inbound MCP Tasks
/// protocol envelope.
///
/// `envelope_json` must be the **original JSON text as received on the wire** — see the module
/// docs. If the caller also needs a parsed [`serde_json::Value`] for translation, it should parse
/// a *separate* copy from `envelope_json` rather than feeding this function an
/// already-parsed-then-re-serialized value.
///
/// `task_id` is the MemPalace task the envelope maps to; `created_by` is the actor recorded as
/// having submitted it.
///
/// # Idempotency key derivation
///
/// `idempotency_key` is `"mcp_tasks_envelope:{task_id}:{hash}"`, where `hash` is the full BLAKE3
/// hex digest of `envelope_json`'s raw bytes — the same hashing scheme
/// `mempalace_a2a::envelope::envelope_artifact` uses, with a distinct key prefix so the two
/// adapters' envelope artifacts for the same task never collide on idempotency key even if their
/// wire bytes happened to be identical.
pub fn envelope_artifact(task_id: &str, created_by: &str, envelope_json: &str) -> NewArtifact {
    let hash = blake3::hash(envelope_json.as_bytes()).to_hex().to_string();
    let idempotency_key = format!("mcp_tasks_envelope:{task_id}:{hash}");
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
        let envelope = r#"{"status":"input_required"}"#;
        let artifact = envelope_artifact("task_1", "alice", envelope);
        assert_eq!(artifact.role, PROTOCOL_ENVELOPE_ROLE);
        assert_eq!(artifact.media_type, PROTOCOL_ENVELOPE_MEDIA_TYPE);
        assert_eq!(artifact.task_id, "task_1");
        assert_eq!(artifact.created_by, "alice");
    }

    #[test]
    fn envelope_artifact_stores_the_exact_bytes_it_was_given() {
        let envelope = "{\n  \"taskId\": \"task_1\",\n  \"status\": \"working\"\n}";
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
        let first = envelope_artifact("task_1", "alice", r#"{"status":"working"}"#);
        let second = envelope_artifact("task_1", "alice", r#"{"status":"completed"}"#);
        assert_ne!(first.idempotency_key, second.idempotency_key);
    }

    #[test]
    fn idempotency_key_differs_for_different_tasks_with_identical_content() {
        let envelope = r#"{"status":"working"}"#;
        let first = envelope_artifact("task_1", "alice", envelope);
        let second = envelope_artifact("task_2", "alice", envelope);
        assert_ne!(first.idempotency_key, second.idempotency_key);
    }

    #[test]
    fn idempotency_key_namespace_differs_from_the_a2a_adapter() {
        // Not a dependency on mempalace-a2a (this crate does not depend on it) -- just a pinned
        // literal check that the prefix really is distinct, per the module docs.
        let artifact = envelope_artifact("task_1", "alice", r#"{"status":"working"}"#);
        assert!(artifact.idempotency_key.starts_with("mcp_tasks_envelope:"));
        assert!(!artifact.idempotency_key.starts_with("a2a_envelope:"));
    }

    #[test]
    fn two_envelopes_differing_only_in_key_order_and_whitespace_get_different_idempotency_keys_and_are_both_preserved()
     {
        let compact = r#"{"taskId":"task_1","status":"working"}"#;
        let reordered_and_padded = r#"{ "status": "working", "taskId": "task_1" }"#;

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

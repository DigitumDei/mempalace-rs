//! A2A `Message` ↔ `coordination_messages`.
//!
//! `Message` fields verified against the A2A v1.0.1 proto (`specification/a2a.proto` in
//! `a2aproject/A2A` at tag `v1.0.1`, lines ~256-269): `message_id`, `context_id`, `task_id`,
//! `role`, `parts`, `metadata`, `extensions`, `reference_task_ids`.
//!
//! # Isolation
//!
//! `mempalace_storage::coordination::Message` already stores its body as an opaque
//! [`serde_json::Value`] payload with no per-field columns of its own — `sender`, `recipient`,
//! `kind`, `payload` are the only columns, and `payload` is caller-defined JSON. Mapping an A2A
//! `Message` into that shape therefore needs no artifact-isolation trick the way task-state
//! coercion does: the entire `A2aMessage` is serialized verbatim into `payload`, so no A2A
//! field is dropped, reinterpreted, or promoted to a column. `kind` is fixed to
//! [`A2A_MESSAGE_KIND`] so a reader can recognise the payload shape before deserializing it.

use mempalace_storage::{Message, NewMessage};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::A2aError;
use crate::part::{A2aPart, A2aRole};

/// `coordination_messages.kind` used for a stored A2A message, so a reader can recognise the
/// payload shape before attempting [`message_to_a2a_message`].
pub const A2A_MESSAGE_KIND: &str = "a2a_message";

/// Wire mirror of the A2A `Message` object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct A2aMessage {
    /// Unique identifier of the message, created by the sender.
    pub message_id: String,
    /// Context this message belongs to, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_id: Option<String>,
    /// Task this message is associated with, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// Sender role.
    pub role: A2aRole,
    /// Message content.
    pub parts: Vec<A2aPart>,
    /// Optional metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    /// URIs of extensions present on this message.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extensions: Vec<String>,
    /// Task ids this message references for additional context.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reference_task_ids: Vec<String>,
}

/// Build a [`NewMessage`] carrying `a2a` verbatim as its payload.
///
/// `task_id` is the owning MemPalace task; `sender`/`recipient`/`idempotency_key` are resolved
/// by the caller the same way every other coordination write resolves them (see
/// `mempalace_storage::coordination::NewMessage`). `envelope_version` defaults to `1`, matching
/// `NewMessage`'s own default.
pub fn a2a_message_to_new_message(
    a2a: &A2aMessage,
    task_id: &str,
    sender: &str,
    recipient: &str,
    idempotency_key: String,
) -> Result<NewMessage, A2aError> {
    let payload = serde_json::to_value(a2a)?;
    Ok(NewMessage {
        task_id: task_id.to_owned(),
        sender: sender.to_owned(),
        recipient: recipient.to_owned(),
        kind: A2A_MESSAGE_KIND.to_owned(),
        payload,
        idempotency_key,
        envelope_version: 1,
    })
}

/// Decode a stored [`Message`] back into an [`A2aMessage`].
///
/// Fails with [`A2aError::InvalidStoredShape`] if `message.kind` is not [`A2A_MESSAGE_KIND`], or
/// the payload does not decode as an `A2aMessage` — e.g. a message written by a non-A2A caller.
pub fn message_to_a2a_message(message: &Message) -> Result<A2aMessage, A2aError> {
    if message.kind != A2A_MESSAGE_KIND {
        return Err(A2aError::InvalidStoredShape {
            shape: "Message",
            detail: format!("kind `{}` is not `{A2A_MESSAGE_KIND}`", message.kind),
        });
    }
    serde_json::from_value(message.payload.clone()).map_err(|err| A2aError::InvalidStoredShape {
        shape: "Message",
        detail: err.to_string(),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn sample() -> A2aMessage {
        A2aMessage {
            message_id: "msg_1".to_owned(),
            context_id: Some("ctx_1".to_owned()),
            task_id: Some("task_1".to_owned()),
            role: A2aRole::Agent,
            parts: vec![A2aPart { text: Some("hello".to_owned()), ..Default::default() }],
            metadata: None,
            extensions: Vec::new(),
            reference_task_ids: Vec::new(),
        }
    }

    #[test]
    fn round_trips_through_new_message_and_back() {
        let original = sample();
        let new_message = a2a_message_to_new_message(
            &original,
            "task_1",
            "agent-a",
            "agent-b",
            "key_1".to_owned(),
        )
        .unwrap();
        assert_eq!(new_message.kind, A2A_MESSAGE_KIND);
        assert_eq!(new_message.task_id, "task_1");

        // Simulate what storage would hand back on read.
        let stored = Message {
            message_id: "message_stored_1".to_owned(),
            sequence: 1,
            task_id: new_message.task_id.clone(),
            sender: new_message.sender.clone(),
            recipient: new_message.recipient.clone(),
            kind: new_message.kind.clone(),
            payload: new_message.payload.clone(),
            envelope_version: new_message.envelope_version,
            acknowledged_at: None,
            acknowledged_by: None,
            created_at: time::OffsetDateTime::now_utc(),
        };
        let decoded = message_to_a2a_message(&stored).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn rejects_a_message_with_the_wrong_kind() {
        let stored = Message {
            message_id: "message_stored_1".to_owned(),
            sequence: 1,
            task_id: "task_1".to_owned(),
            sender: "a".to_owned(),
            recipient: "b".to_owned(),
            kind: "status".to_owned(),
            payload: serde_json::json!({}),
            envelope_version: 1,
            acknowledged_at: None,
            acknowledged_by: None,
            created_at: time::OffsetDateTime::now_utc(),
        };
        let err = message_to_a2a_message(&stored).expect_err("wrong kind must be rejected");
        assert!(matches!(err, A2aError::InvalidStoredShape { shape: "Message", .. }));
    }
}

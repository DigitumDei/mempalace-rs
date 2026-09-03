//! A2A `Task` ↔ `coordination_tasks`.
//!
//! `Task`/`TaskStatus` fields verified against the A2A v1.0.1 proto (`specification/a2a.proto`
//! in `a2aproject/A2A` at tag `v1.0.1`, lines ~167-220): `Task` has `id`, `context_id`, `status`,
//! `artifacts`, `history`, `metadata`; `TaskStatus` has `state`, `message`, `timestamp`.
//!
//! # The hard part: `NewTask` needs fields A2A does not carry
//!
//! `mempalace_storage::coordination::NewTask` requires `title`, `description`, `wing`,
//! `created_by`, and `idempotency_key`. None of those exist anywhere on the A2A `Task` message —
//! `Task` identifies itself (`id`, `context_id`), carries its lifecycle `status`, and carries its
//! `artifacts`/`history`/`metadata`, but it has no notion of a human-readable title, a
//! free-text description distinct from its message history, an owning wing (A2A has no wing
//! concept at all — wings are a MemPalace-specific multi-tenancy boundary), or who created it
//! (A2A's closest concept, `Message.role`, only distinguishes `ROLE_USER`/`ROLE_AGENT`, not an
//! identity).
//!
//! Rather than inventing values for these (an empty string, a literal `"unknown"`, a title
//! derived by truncating a message body — all of which would silently misrepresent the task to
//! every later reader), [`NewTaskInputs`] takes them as explicit, caller-supplied parameters,
//! the same pattern [`crate::agent_card::AgentCardInputs`] already uses for
//! `AgentCard::supported_interfaces`, which has the identical problem (no source in this
//! crate's dependencies). The caller — whatever code terminates the A2A adapter and holds the
//! actual authenticated identity and routing/config context — is the only party that can supply
//! them correctly. See deviation entry 27 in `docs/Coordination-Phase-3-Design.md`.
//!
//! # `NewTask` has no `state` field; the caller chooses the creation state
//!
//! [`NewTask`] carries no state, so [`a2a_task_to_new_task`] cannot express the inbound A2A
//! status in it. Instead it runs the status through [`crate::state::map_inbound_task_state`] and
//! returns the result as `target_state` alongside the `NewTask`, so the caller sees both the
//! target state and whether reaching it required a [`crate::state::Coercion`]. This mirrors the
//! crate-wide rule that a coercion is never silently dropped, even when the value it travels
//! with (here, a whole `NewTask`) has no slot for the state itself.
//!
//! # How the caller should apply `target_state`
//!
//! Use `CoordinationStore::import_task(&new_task, target_state.value)`, which creates the task
//! *directly* in that state. Do **not** try to reach it through the transition machine.
//! `allowed_transition` (`crates/mempalace-storage/src/coordination.rs`) permits only
//! `Pending -> Cancelled | Expired`, `Running -> InputRequired | Completed | Cancelled | Failed |
//! Expired`, and `InputRequired -> Pending | Running | Cancelled | Failed | Expired`.
//! `Pending -> Running` is not in that table: the only route into `Running` is `claim_task`,
//! which requires a worker identity and a lease. So creating with `create_task` and then
//! transitioning would mean **fabricating a claim by a worker that never existed** in order to
//! land an inbound `completed` or `failed` task — audit history asserting something that did not
//! happen.
//!
//! `import_task` exists precisely to avoid that: an import is a *creation*, not a lifecycle
//! event, so it bypasses the transition machine, records the imported state on the
//! `task_created` event with an `imported` marker, and rejects [`TaskState::Expired`] as an
//! initial state (the palace produces that itself; an importer never asserts it). A task
//! imported as `Running` has no owner or lease, and none is invented — it stays claimable.
//!
//! This crate still provides no helper that performs the storage call: it is a pure translation
//! library and must not depend on a live `CoordinationStore` (see the crate-level docs). See
//! deviation entry 30 in `docs/Coordination-Phase-3-Design.md`.
//!
//! # Fields with no home in `NewTask`
//!
//! `NewTask::parent_id`, `dependencies`, `budget`, and `expires_at` are genuinely optional in
//! MemPalace, and the A2A `Task` message carries no analogue for any of them (no parent-task
//! link, no dependency list, no budget, no expiry). [`a2a_task_to_new_task`] sets all four to
//! their "absent" value (`None`/empty) rather than guessing — this is not the same as the
//! title/description/wing/created_by/idempotency_key problem above, because `None`/empty is the
//! *correct* representation of "A2A supplied nothing here", not an invented stand-in for a
//! required value.
//!
//! # `id`/`context_id`/`artifacts`/`history`/`metadata` have no home in `NewTask` either
//!
//! Per the isolation rule (see the crate-level docs), none of these become new columns. The
//! caller is expected to store the original inbound `Task` JSON verbatim via
//! [`crate::envelope::envelope_artifact`] (as it already does for task-state coercion), which is
//! where `id`, `context_id`, and `metadata` remain recoverable. `artifacts` and `history` map to
//! MemPalace's own `coordination_artifacts`/`coordination_messages` rows via
//! [`crate::artifact::a2a_artifact_to_new_artifact`] and
//! [`crate::message::a2a_message_to_new_message`] — this module does not duplicate that mapping.

use mempalace_storage::{NewTask, Task, TaskState};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;

use crate::artifact::A2aArtifact;
use crate::error::A2aError;
use crate::message::A2aMessage;
use crate::state::{A2aTaskState, Mapped, map_inbound_task_state, map_outbound_task_state};

/// Wire mirror of the A2A `TaskStatus` object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct A2aTaskStatus {
    /// The current state of the task.
    pub state: A2aTaskState,
    /// A message associated with the status, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<A2aMessage>,
    /// When the status was recorded.
    #[serde(default, with = "time::serde::rfc3339::option", skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<OffsetDateTime>,
}

/// Wire mirror of the A2A `Task` object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct A2aTask {
    /// Unique identifier for the task, generated by the server for a new task.
    pub id: String,
    /// Identifier for the contextual collection of interactions this task belongs to, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_id: Option<String>,
    /// The current status of the task.
    pub status: A2aTaskStatus,
    /// Output artifacts for the task.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<A2aArtifact>,
    /// The history of interactions for the task.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub history: Vec<A2aMessage>,
    /// Custom metadata about the task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

impl A2aTask {
    /// Recursively validates the task's nested artifacts and messages.
    ///
    /// Fails with [`A2aError::EmptyArtifactParts`] if any artifact has an empty parts list,
    /// or [`A2aError::InvalidPart`] if any part in an artifact, history message, or status message
    /// violates the `Part.content` `oneof` invariant.
    pub fn validate(&self) -> Result<(), A2aError> {
        if let Some(msg) = &self.status.message {
            for part in &msg.parts {
                part.validate()?;
            }
        }
        for artifact in &self.artifacts {
            if artifact.parts.is_empty() {
                return Err(A2aError::EmptyArtifactParts);
            }
            for part in &artifact.parts {
                part.validate()?;
            }
        }
        for message in &self.history {
            for part in &message.parts {
                part.validate()?;
            }
        }
        Ok(())
    }
}

/// Caller-supplied fields [`NewTask`] requires that the A2A `Task` message has no source for.
///
/// See the module docs' "The hard part" section for why these cannot be defaulted.
#[derive(Debug, Clone)]
pub struct NewTaskInputs<'a> {
    /// Human-readable task title. A2A has no equivalent field.
    pub title: &'a str,
    /// Task description. A2A has no equivalent field distinct from its message history.
    pub description: &'a str,
    /// Owning wing. A2A has no multi-tenancy concept at all.
    pub wing: &'a str,
    /// Actor recorded as having created the task. A2A's `Message.role` only distinguishes
    /// `ROLE_USER`/`ROLE_AGENT`, not an identity, so it cannot supply this.
    pub created_by: &'a str,
    /// Idempotency key for the underlying `create_task` call, resolved by the caller the same
    /// way every other coordination write resolves one.
    pub idempotency_key: String,
}

/// The result of translating an inbound A2A `Task` into a MemPalace [`NewTask`].
#[derive(Debug, Clone)]
pub struct NewTaskConversion {
    /// The task to create. Always creates in [`TaskState::Pending`] — see the module docs'
    /// "`NewTask` has no `state` field" section for why `target_state` below is not baked in
    /// here.
    pub new_task: NewTask,
    /// Where the task's state should end up, per [`crate::state::map_inbound_task_state`] on the
    /// A2A task's `status.state`. `create_task` has no way to create directly into any state
    /// other than [`TaskState::Pending`], so if this is not `Pending` the caller must apply it
    /// afterward — but *how* depends on the value: `Cancelled`/`Expired` are a single
    /// `transition_task` call, while `Running`/`InputRequired`/`Completed`/`Failed` all require
    /// `claim_task` (which needs a worker identity and lease TTL this type does not carry)
    /// before `transition_task` can run, if at all. See the module docs' "Reaching
    /// `target_state` is not always one call" section for the full per-state breakdown — do not
    /// assume a single `task_transition` call always suffices. `target_state.coercion` reports
    /// whether reaching it required coercing an A2A state MemPalace has no direct counterpart
    /// for (e.g. `TASK_STATE_AUTH_REQUIRED`); this is never discarded, per the crate-wide
    /// no-silent-coercion rule.
    pub target_state: Mapped<TaskState>,
}

/// Translate an inbound A2A `Task` into a [`NewTask`], plus the state it should be transitioned
/// to after creation.
///
/// Fails with [`A2aError::UnspecifiedTaskState`] if `a2a.status.state` is
/// `TASK_STATE_UNSPECIFIED` — see [`crate::state::map_inbound_task_state`].
/// Fails with [`A2aError::EmptyArtifactParts`] or [`A2aError::InvalidPart`] if any nested
/// artifact or message is malformed (see [`A2aTask::validate`]).
///
/// `NewTask::parent_id`, `dependencies`, `budget`, and `expires_at` are set to their empty/absent
/// value: A2A carries no analogue for any of them (see the module docs).
pub fn a2a_task_to_new_task(
    a2a: &A2aTask,
    inputs: NewTaskInputs<'_>,
) -> Result<NewTaskConversion, A2aError> {
    a2a.validate()?;
    let target_state = map_inbound_task_state(a2a.status.state)?;
    let new_task = NewTask {
        title: inputs.title.to_owned(),
        description: inputs.description.to_owned(),
        created_by: inputs.created_by.to_owned(),
        wing: inputs.wing.to_owned(),
        idempotency_key: inputs.idempotency_key,
        parent_id: None,
        dependencies: Vec::new(),
        budget: None,
        expires_at: None,
    };
    Ok(NewTaskConversion { new_task, target_state })
}

/// Translate an outbound MemPalace [`Task`] into an [`A2aTask`].
///
/// `artifacts` and `history` are supplied by the caller (fetched separately from
/// `coordination_artifacts`/`coordination_messages` and mapped via
/// [`crate::artifact::artifact_to_a2a_artifact`]/[`crate::message::message_to_a2a_message`]) —
/// [`Task`] itself carries neither. `status_message` becomes `TaskStatus.message`; MemPalace's
/// `Task` has no dedicated "current status message" of its own, so the caller resolves it (e.g.
/// the most recent message in `history`) rather than this function guessing.
///
/// `context_id` and `metadata` are always `None`: MemPalace's `Task` has no A2A "context" grouping
/// concept and no generic metadata column to source either from (see the module docs).
///
/// The returned [`Mapped::coercion`] reports whether `task.state` required coercion to reach its
/// A2A counterpart (only [`TaskState::Expired`] does — see
/// [`crate::state::map_outbound_task_state`]).
pub fn task_to_a2a_task(
    task: &Task,
    artifacts: Vec<A2aArtifact>,
    history: Vec<A2aMessage>,
    status_message: Option<A2aMessage>,
) -> Mapped<A2aTask> {
    let mapped_state = map_outbound_task_state(task.state);
    let a2a = A2aTask {
        id: task.task_id.clone(),
        context_id: None,
        status: A2aTaskStatus {
            state: mapped_state.value,
            message: status_message,
            timestamp: Some(task.updated_at),
        },
        artifacts,
        history,
        metadata: None,
    };
    Mapped { value: a2a, coercion: mapped_state.coercion }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use mempalace_storage::TaskState;

    use super::*;
    use crate::part::{A2aPart, A2aRole};

    fn sample_a2a_task(state: A2aTaskState) -> A2aTask {
        A2aTask {
            id: "a2a_task_1".to_owned(),
            context_id: Some("ctx_1".to_owned()),
            status: A2aTaskStatus { state, message: None, timestamp: None },
            artifacts: Vec::new(),
            history: Vec::new(),
            metadata: None,
        }
    }

    fn sample_inputs() -> NewTaskInputs<'static> {
        NewTaskInputs {
            title: "Investigate flaky test",
            description: "The nightly suite flakes on test_foo about once a week.",
            wing: "wing_myproject",
            created_by: "alice",
            idempotency_key: "key_1".to_owned(),
        }
    }

    #[test]
    fn a2a_task_to_new_task_carries_caller_supplied_fields_through() {
        let a2a = sample_a2a_task(A2aTaskState::Submitted);
        let conversion = a2a_task_to_new_task(&a2a, sample_inputs()).unwrap();
        assert_eq!(conversion.new_task.title, "Investigate flaky test");
        assert_eq!(
            conversion.new_task.description,
            "The nightly suite flakes on test_foo about once a week."
        );
        assert_eq!(conversion.new_task.wing, "wing_myproject");
        assert_eq!(conversion.new_task.created_by, "alice");
        assert_eq!(conversion.new_task.idempotency_key, "key_1");
        assert_eq!(conversion.new_task.parent_id, None);
        assert!(conversion.new_task.dependencies.is_empty());
        assert_eq!(conversion.new_task.budget, None);
        assert_eq!(conversion.new_task.expires_at, None);
    }

    #[test]
    fn a2a_task_to_new_task_reports_the_direct_target_state_uncoerced() {
        let a2a = sample_a2a_task(A2aTaskState::Submitted);
        let conversion = a2a_task_to_new_task(&a2a, sample_inputs()).unwrap();
        assert_eq!(conversion.target_state.value, TaskState::Pending);
        assert!(conversion.target_state.coercion.is_none());
    }

    #[test]
    fn a2a_task_to_new_task_reports_a_coerced_target_state_without_discarding_it() {
        // AUTH_REQUIRED coerces to InputRequired, and create_task always starts a task at
        // Pending regardless -- the caller must transition it afterward. The point of this test
        // is that the coercion record survives the trip through NewTaskConversion.
        let a2a = sample_a2a_task(A2aTaskState::AuthRequired);
        let conversion = a2a_task_to_new_task(&a2a, sample_inputs()).unwrap();
        assert_eq!(conversion.target_state.value, TaskState::InputRequired);
        let coercion = conversion.target_state.coercion.expect("must report the coercion");
        assert_eq!(coercion.from, "TASK_STATE_AUTH_REQUIRED");
        assert_eq!(coercion.to, "input_required");
    }

    #[test]
    fn a2a_task_to_new_task_rejects_unspecified_state() {
        let a2a = sample_a2a_task(A2aTaskState::Unspecified);
        let err = a2a_task_to_new_task(&a2a, sample_inputs())
            .expect_err("UNSPECIFIED must never be coerced to a state");
        assert!(matches!(err, A2aError::UnspecifiedTaskState));
    }

    fn sample_task(state: TaskState) -> Task {
        Task {
            task_id: "task_1".to_owned(),
            title: "Investigate flaky test".to_owned(),
            description: "desc".to_owned(),
            state,
            revision: 1,
            created_by: "alice".to_owned(),
            wing: "wing_myproject".to_owned(),
            owner: None,
            parent_id: None,
            dependencies: Vec::new(),
            budget: None,
            lease_expires_at: None,
            expires_at: None,
            created_at: time::OffsetDateTime::now_utc(),
            updated_at: time::OffsetDateTime::now_utc(),
        }
    }

    #[test]
    fn task_to_a2a_task_maps_state_and_carries_supplied_artifacts_and_history() {
        let task = sample_task(TaskState::Running);
        let artifact = A2aArtifact {
            artifact_id: "artifact_1".to_owned(),
            name: None,
            description: None,
            parts: vec![A2aPart { text: Some("done".to_owned()), ..Default::default() }],
            metadata: None,
            extensions: Vec::new(),
        };
        let message = A2aMessage {
            message_id: "msg_1".to_owned(),
            context_id: None,
            task_id: Some("task_1".to_owned()),
            role: A2aRole::Agent,
            parts: vec![A2aPart { text: Some("hi".to_owned()), ..Default::default() }],
            metadata: None,
            extensions: Vec::new(),
            reference_task_ids: Vec::new(),
        };
        let mapped = task_to_a2a_task(
            &task,
            vec![artifact.clone()],
            vec![message.clone()],
            Some(message.clone()),
        );
        assert_eq!(mapped.value.id, "task_1");
        assert_eq!(mapped.value.status.state, A2aTaskState::Working);
        assert!(mapped.coercion.is_none());
        assert_eq!(mapped.value.artifacts, vec![artifact]);
        assert_eq!(mapped.value.history, vec![message.clone()]);
        assert_eq!(mapped.value.status.message, Some(message));
        assert_eq!(mapped.value.status.timestamp, Some(task.updated_at));
        assert_eq!(mapped.value.context_id, None);
        assert_eq!(mapped.value.metadata, None);
    }

    #[test]
    fn task_to_a2a_task_reports_the_expired_coercion() {
        let task = sample_task(TaskState::Expired);
        let mapped = task_to_a2a_task(&task, Vec::new(), Vec::new(), None);
        assert_eq!(mapped.value.status.state, A2aTaskState::Failed);
        let coercion = mapped.coercion.expect("Expired must be reported as a coercion");
        assert_eq!(coercion.from, "expired");
        assert_eq!(coercion.to, "TASK_STATE_FAILED");
    }

    #[test]
    fn a2a_task_round_trips_through_json() {
        let a2a = A2aTask {
            id: "task_1".to_owned(),
            context_id: Some("ctx_1".to_owned()),
            status: A2aTaskStatus {
                state: A2aTaskState::Working,
                message: None,
                timestamp: Some(time::OffsetDateTime::now_utc()),
            },
            artifacts: Vec::new(),
            history: Vec::new(),
            metadata: Some(serde_json::json!({"k": "v"})),
        };
        let json = serde_json::to_string(&a2a).unwrap();
        let decoded: A2aTask = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.id, a2a.id);
        assert_eq!(decoded.status.state, a2a.status.state);
        assert_eq!(decoded.metadata, a2a.metadata);
    }

    #[test]
    fn a2a_task_validate_rejects_empty_artifact_parts() {
        let mut a2a = sample_a2a_task(A2aTaskState::Working);
        a2a.artifacts.push(A2aArtifact {
            artifact_id: "art_empty".to_owned(),
            name: None,
            description: None,
            parts: Vec::new(),
            metadata: None,
            extensions: Vec::new(),
        });
        let err = a2a_task_to_new_task(&a2a, sample_inputs()).unwrap_err();
        assert!(matches!(err, A2aError::EmptyArtifactParts));
    }

    #[test]
    fn a2a_task_validate_rejects_invalid_part_in_nested_message() {
        let mut a2a = sample_a2a_task(A2aTaskState::Working);
        a2a.history.push(A2aMessage {
            message_id: "msg_bad".to_owned(),
            context_id: None,
            task_id: Some("task_1".to_owned()),
            role: A2aRole::User,
            parts: vec![A2aPart::default()], // 0 fields set -> invalid
            metadata: None,
            extensions: Vec::new(),
            reference_task_ids: Vec::new(),
        });
        let err = a2a_task_to_new_task(&a2a, sample_inputs()).unwrap_err();
        assert!(matches!(err, A2aError::InvalidPart { .. }));
    }
}

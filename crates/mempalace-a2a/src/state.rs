//! A2A task-state enum and its (non-bijective) mapping to/from
//! [`mempalace_storage::TaskState`].
//!
//! A2A v1.0 defines nine task states; MemPalace has seven. The mapping is documented in
//! `docs/Coordination-Phase-3-Design.md` (Stage 5):
//!
//! | A2A | MemPalace | Note |
//! |---|---|---|
//! | `TASK_STATE_SUBMITTED` | `Pending` | |
//! | `TASK_STATE_WORKING` | `Running` | |
//! | `TASK_STATE_INPUT_REQUIRED` | `InputRequired` | |
//! | `TASK_STATE_COMPLETED` | `Completed` | |
//! | `TASK_STATE_FAILED` | `Failed` | |
//! | `TASK_STATE_CANCELED` | `Cancelled` | note the spelling difference — A2A uses one `l` |
//! | `TASK_STATE_AUTH_REQUIRED` | `InputRequired` | coerced; both mean "interrupted, awaiting input" |
//! | `TASK_STATE_REJECTED` | `Failed` | coerced; terminal and unsuccessful |
//! | `TASK_STATE_UNSPECIFIED` | — | rejected as malformed; never coerced ([`A2aError::UnspecifiedTaskState`]) |
//! | — | `Expired` | outbound only, emitted as `TASK_STATE_FAILED` |
//!
//! Coercion is permitted where it is documented, deterministic, and lossless in audit — the
//! inbound envelope is always stored verbatim (see [`crate::envelope`]), so the original state
//! is always recoverable. What is forbidden is *silent* coercion: every mapping in this module
//! returns a [`Mapped`] value that reports whether a coercion happened, rather than a bare
//! `TaskState`/`A2aTaskState` that would let the caller discard that fact.
//!
//! # Wire string spelling
//!
//! The wire strings above carry A2A's `TASK_STATE_` prefix, matching the authoritative source —
//! the A2A v1.0.1 proto (`specification/a2a.proto` in `a2aproject/A2A` at tag `v1.0.1`) defines
//! `enum TaskState` with values `TASK_STATE_UNSPECIFIED`, `TASK_STATE_SUBMITTED`,
//! `TASK_STATE_WORKING`, `TASK_STATE_COMPLETED`, `TASK_STATE_FAILED`, `TASK_STATE_CANCELED`,
//! `TASK_STATE_INPUT_REQUIRED`, `TASK_STATE_REJECTED`, and `TASK_STATE_AUTH_REQUIRED` — and the
//! worked JSON examples in that release's `docs/specification.md`, which consistently render
//! `"state": "TASK_STATE_COMPLETED"` and the like. Proto3 JSON encoding (protojson) serializes
//! enums by their full value name, not a stripped suffix, so the prefixed form is what appears
//! on the wire.

use mempalace_storage::TaskState;
use serde::{Deserialize, Serialize};

use crate::error::A2aError;

/// A2A v1.0 task lifecycle state.
///
/// Nine variants, one more than the six terminal directions [`TaskState`] round-trips exactly.
/// See the module docs for the full mapping table and the note on wire-string spelling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum A2aTaskState {
    /// Acknowledged, not yet started. Maps to [`TaskState::Pending`].
    #[serde(rename = "TASK_STATE_SUBMITTED")]
    Submitted,
    /// Actively being processed. Maps to [`TaskState::Running`].
    #[serde(rename = "TASK_STATE_WORKING")]
    Working,
    /// Interrupted, awaiting external input. Maps to [`TaskState::InputRequired`].
    #[serde(rename = "TASK_STATE_INPUT_REQUIRED")]
    InputRequired,
    /// Terminal: finished successfully. Maps to [`TaskState::Completed`].
    #[serde(rename = "TASK_STATE_COMPLETED")]
    Completed,
    /// Terminal: finished with an error. Maps to [`TaskState::Failed`].
    #[serde(rename = "TASK_STATE_FAILED")]
    Failed,
    /// Terminal: canceled before completion. Note the A2A spelling (one `l`). Maps to
    /// [`TaskState::Cancelled`].
    #[serde(rename = "TASK_STATE_CANCELED")]
    Canceled,
    /// Interrupted, awaiting authentication. Coerced to [`TaskState::InputRequired`] on
    /// inbound mapping — both mean "interrupted, awaiting external input".
    #[serde(rename = "TASK_STATE_AUTH_REQUIRED")]
    AuthRequired,
    /// Terminal: the agent declined to perform the task. Coerced to [`TaskState::Failed`] on
    /// inbound mapping — terminal and unsuccessful.
    #[serde(rename = "TASK_STATE_REJECTED")]
    Rejected,
    /// Unknown or indeterminate. Never mapped — inbound mapping rejects it as malformed
    /// ([`A2aError::UnspecifiedTaskState`]).
    #[serde(rename = "TASK_STATE_UNSPECIFIED")]
    Unspecified,
}

/// A successfully mapped value, together with a record of whether the source was coerced
/// rather than mapped one-to-one.
///
/// Deliberately not a bare `From`/`Into` impl: those would let a caller take just the target
/// value and silently drop whether a coercion happened, which is exactly what
/// `docs/Coordination-Phase-3-Design.md` (Stage 5) forbids ("what remains forbidden is *silent*
/// coercion"). A caller that only cares about the mapped value can still do
/// `mapped.value`, but doing so is a visible, deliberate choice at the call site rather than
/// something a type coercion hides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mapped<T> {
    /// The mapped target value.
    pub value: T,
    /// `Some` when `value` is not a direct one-to-one counterpart of the source — i.e. the
    /// mapping in the table above is marked "coerced".
    pub coercion: Option<Coercion>,
}

/// Records that a state mapping coerced its source into a value the table above does not treat
/// as a direct one-to-one counterpart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Coercion {
    /// The original wire value before coercion (e.g. `"TASK_STATE_AUTH_REQUIRED"`, `"expired"`).
    pub from: &'static str,
    /// What it was coerced to (e.g. `"input_required"`, `"TASK_STATE_FAILED"`).
    pub to: &'static str,
    /// Why the coercion is considered acceptable, per the design's documented-deterministic-
    /// lossless-in-audit rule.
    pub reason: &'static str,
}

/// Map an inbound A2A task state to its MemPalace counterpart.
///
/// Every row of the table in the module docs is covered. `UNSPECIFIED` is rejected outright —
/// see [`A2aError::UnspecifiedTaskState`] — rather than coerced to any MemPalace state, per the
/// design's explicit "never coerced" rule for that variant.
pub fn map_inbound_task_state(state: A2aTaskState) -> Result<Mapped<TaskState>, A2aError> {
    let mapped = match state {
        A2aTaskState::Submitted => Mapped { value: TaskState::Pending, coercion: None },
        A2aTaskState::Working => Mapped { value: TaskState::Running, coercion: None },
        A2aTaskState::InputRequired => {
            Mapped { value: TaskState::InputRequired, coercion: None }
        }
        A2aTaskState::Completed => Mapped { value: TaskState::Completed, coercion: None },
        A2aTaskState::Failed => Mapped { value: TaskState::Failed, coercion: None },
        A2aTaskState::Canceled => Mapped { value: TaskState::Cancelled, coercion: None },
        A2aTaskState::AuthRequired => Mapped {
            value: TaskState::InputRequired,
            coercion: Some(Coercion {
                from: "TASK_STATE_AUTH_REQUIRED",
                to: "input_required",
                reason: "both mean \"interrupted, awaiting external input\"",
            }),
        },
        A2aTaskState::Rejected => Mapped {
            value: TaskState::Failed,
            coercion: Some(Coercion {
                from: "TASK_STATE_REJECTED",
                to: "failed",
                reason: "terminal and unsuccessful",
            }),
        },
        A2aTaskState::Unspecified => return Err(A2aError::UnspecifiedTaskState),
    };
    Ok(mapped)
}

/// Map an outbound MemPalace task state to its A2A counterpart.
///
/// Inbound is not total in the MemPalace direction ([`TaskState::Expired`] has no A2A
/// counterpart), so it is coerced to `FAILED`, per the design's outbound-only coercion rule.
pub fn map_outbound_task_state(state: TaskState) -> Mapped<A2aTaskState> {
    match state {
        TaskState::Pending => Mapped { value: A2aTaskState::Submitted, coercion: None },
        TaskState::Running => Mapped { value: A2aTaskState::Working, coercion: None },
        TaskState::InputRequired => {
            Mapped { value: A2aTaskState::InputRequired, coercion: None }
        }
        TaskState::Completed => Mapped { value: A2aTaskState::Completed, coercion: None },
        TaskState::Cancelled => Mapped { value: A2aTaskState::Canceled, coercion: None },
        TaskState::Failed => Mapped { value: A2aTaskState::Failed, coercion: None },
        TaskState::Expired => Mapped {
            value: A2aTaskState::Failed,
            coercion: Some(Coercion {
                from: "expired",
                to: "TASK_STATE_FAILED",
                reason: "A2A has no analogue of a lease/deadline expiry state",
            }),
        },
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const DIRECT_PAIRS: [(A2aTaskState, TaskState); 6] = [
        (A2aTaskState::Submitted, TaskState::Pending),
        (A2aTaskState::Working, TaskState::Running),
        (A2aTaskState::InputRequired, TaskState::InputRequired),
        (A2aTaskState::Completed, TaskState::Completed),
        (A2aTaskState::Failed, TaskState::Failed),
        (A2aTaskState::Canceled, TaskState::Cancelled),
    ];

    #[test]
    fn direct_pairs_round_trip_inbound_and_outbound_without_coercion() {
        for (a2a, mem) in DIRECT_PAIRS {
            let inbound = map_inbound_task_state(a2a).expect("direct pair must map inbound");
            assert_eq!(inbound.value, mem);
            assert!(inbound.coercion.is_none(), "direct pair must not be reported as coerced");

            let outbound = map_outbound_task_state(mem);
            assert_eq!(outbound.value, a2a);
            assert!(outbound.coercion.is_none(), "direct pair must not be reported as coerced");
        }
    }

    #[test]
    fn inbound_auth_required_is_reported_as_coerced_to_input_required() {
        let mapped = map_inbound_task_state(A2aTaskState::AuthRequired).unwrap();
        assert_eq!(mapped.value, TaskState::InputRequired);
        let coercion = mapped.coercion.expect("AUTH_REQUIRED must be reported as a coercion");
        assert_eq!(coercion.from, "TASK_STATE_AUTH_REQUIRED");
        assert_eq!(coercion.to, "input_required");
    }

    #[test]
    fn inbound_rejected_is_reported_as_coerced_to_failed() {
        let mapped = map_inbound_task_state(A2aTaskState::Rejected).unwrap();
        assert_eq!(mapped.value, TaskState::Failed);
        let coercion = mapped.coercion.expect("REJECTED must be reported as a coercion");
        assert_eq!(coercion.from, "TASK_STATE_REJECTED");
        assert_eq!(coercion.to, "failed");
    }

    #[test]
    fn inbound_unspecified_is_rejected_not_coerced() {
        let err = map_inbound_task_state(A2aTaskState::Unspecified)
            .expect_err("UNSPECIFIED must never be coerced to a state");
        assert!(matches!(err, A2aError::UnspecifiedTaskState));
    }

    #[test]
    fn outbound_expired_is_reported_as_coerced_to_failed() {
        let mapped = map_outbound_task_state(TaskState::Expired);
        assert_eq!(mapped.value, A2aTaskState::Failed);
        let coercion = mapped.coercion.expect("Expired must be reported as a coercion");
        assert_eq!(coercion.from, "expired");
        assert_eq!(coercion.to, "TASK_STATE_FAILED");
    }

    #[test]
    fn wire_strings_match_the_design_table() {
        let cases = [
            (A2aTaskState::Submitted, "\"TASK_STATE_SUBMITTED\""),
            (A2aTaskState::Working, "\"TASK_STATE_WORKING\""),
            (A2aTaskState::InputRequired, "\"TASK_STATE_INPUT_REQUIRED\""),
            (A2aTaskState::Completed, "\"TASK_STATE_COMPLETED\""),
            (A2aTaskState::Failed, "\"TASK_STATE_FAILED\""),
            (A2aTaskState::Canceled, "\"TASK_STATE_CANCELED\""),
            (A2aTaskState::AuthRequired, "\"TASK_STATE_AUTH_REQUIRED\""),
            (A2aTaskState::Rejected, "\"TASK_STATE_REJECTED\""),
            (A2aTaskState::Unspecified, "\"TASK_STATE_UNSPECIFIED\""),
        ];
        for (state, expected) in cases {
            let json = serde_json::to_string(&state).unwrap();
            assert_eq!(json, expected);
            let decoded: A2aTaskState = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded, state);
        }
    }

    /// Regression guard: a future switch back to `#[serde(rename_all = ...)]` (which would
    /// strip the `TASK_STATE_` prefix) must fail loudly here rather than silently drifting from
    /// the real A2A v1.0.1 wire format.
    #[test]
    fn wire_strings_carry_the_task_state_prefix_not_the_bare_suffix() {
        assert_eq!(
            serde_json::to_string(&A2aTaskState::AuthRequired).unwrap(),
            "\"TASK_STATE_AUTH_REQUIRED\""
        );
        assert_eq!(
            serde_json::to_string(&A2aTaskState::Unspecified).unwrap(),
            "\"TASK_STATE_UNSPECIFIED\""
        );
    }
}

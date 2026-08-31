//! MCP Tasks status enum and its (non-bijective) mapping to/from
//! [`mempalace_storage::TaskState`].
//!
//! The MCP Tasks extension (`io.modelcontextprotocol/tasks`, 2026-07-28 specification —
//! `specification/2026-07-28/tasks.md` in `modelcontextprotocol/ext-tasks`) defines five task
//! statuses; MemPalace has seven. The mapping is documented in
//! `docs/Coordination-Phase-3-Design.md` (Stage 6):
//!
//! | MCP Tasks | MemPalace | Note |
//! |---|---|---|
//! | `working` | `Running` | |
//! | `input_required` | `InputRequired` | |
//! | `completed` | `Completed` | |
//! | `failed` | `Failed` | |
//! | `cancelled` | `Cancelled` | two `l`s, matching MemPalace's own spelling |
//! | — | `Pending` | outbound only; emitted as `working`, since MCP has no queued state |
//! | — | `Expired` | outbound only; emitted as `failed` |
//!
//! Unlike A2A's `TASK_STATE_UNSPECIFIED`, MCP Tasks has no "malformed/indeterminate" status
//! value, so **inbound mapping is total**: every [`McpTaskStatus`] has exactly one MemPalace
//! counterpart and [`map_inbound_task_status`] cannot fail. Outbound mapping is not total in the
//! MemPalace-to-MCP direction ([`TaskState::Pending`] and [`TaskState::Expired`] have no MCP
//! counterpart), so both are coerced, under the same documented-deterministic-lossless-in-audit
//! rule Stage 5 established for A2A: coercion is permitted, but it is always reported via
//! [`Mapped::coercion`], never silent.
//!
//! # Wire string spelling
//!
//! The wire strings above are lowercase snake_case (`"input_required"`, not `"InputRequired"` or
//! `"INPUT_REQUIRED"`), matching the `TaskStatus` string-literal union in
//! `schema/2026-07-28/schema.ts` (`modelcontextprotocol/ext-tasks`) and the worked JSON examples
//! in `specification/2026-07-28/tasks.md`. This is unlike A2A, which prefixes every value with
//! `TASK_STATE_` — the two extensions made different choices, and mixing them up here would
//! silently produce wire payloads no MCP Tasks client or server accepts.

use mempalace_storage::TaskState;
use serde::{Deserialize, Serialize};

/// MCP Tasks extension status. Five variants, serialized exactly as the wire strings the
/// extension defines — see the module docs for the mapping table and the wire-string note.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpTaskStatus {
    /// Actively being processed. Maps to [`TaskState::Running`].
    Working,
    /// Interrupted, awaiting client-provided input. Maps to [`TaskState::InputRequired`].
    InputRequired,
    /// Terminal: finished successfully. Maps to [`TaskState::Completed`].
    Completed,
    /// Terminal: finished with a JSON-RPC error. Maps to [`TaskState::Failed`].
    Failed,
    /// Terminal: cancelled before completion. Maps to [`TaskState::Cancelled`].
    Cancelled,
}

/// A successfully mapped value, together with a record of whether the source was coerced
/// rather than mapped one-to-one.
///
/// Deliberately not a bare `From`/`Into` impl: those would let a caller take just the target
/// value and silently drop whether a coercion happened. A caller that only cares about the
/// mapped value can still do `mapped.value`, but doing so is a visible, deliberate choice at the
/// call site rather than something a type coercion hides. Mirrors `mempalace_a2a::state::Mapped`
/// exactly, but is defined independently here — this crate does not depend on `mempalace-a2a`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mapped<T> {
    /// The mapped target value.
    pub value: T,
    /// `Some` when `value` is not a direct one-to-one counterpart of the source — i.e. the
    /// mapping in the module docs' table is marked "coerced".
    pub coercion: Option<Coercion>,
}

/// Records that a state mapping coerced its source into a value the table in the module docs
/// does not treat as a direct one-to-one counterpart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Coercion {
    /// The original wire value before coercion (e.g. `"pending"`, `"expired"`).
    pub from: &'static str,
    /// What it was coerced to (e.g. `"working"`, `"failed"`).
    pub to: &'static str,
    /// Why the coercion is considered acceptable, per the documented-deterministic-lossless-in-
    /// audit rule.
    pub reason: &'static str,
}

/// Map an inbound MCP Tasks status to its MemPalace counterpart.
///
/// Total: every [`McpTaskStatus`] variant has exactly one direct MemPalace counterpart, so this
/// never coerces and never fails (unlike A2A's `UNSPECIFIED`, MCP Tasks has no malformed/
/// indeterminate status value to reject).
pub fn map_inbound_task_status(status: McpTaskStatus) -> Mapped<TaskState> {
    let value = match status {
        McpTaskStatus::Working => TaskState::Running,
        McpTaskStatus::InputRequired => TaskState::InputRequired,
        McpTaskStatus::Completed => TaskState::Completed,
        McpTaskStatus::Failed => TaskState::Failed,
        McpTaskStatus::Cancelled => TaskState::Cancelled,
    };
    Mapped { value, coercion: None }
}

/// Map an outbound MemPalace task state to its MCP Tasks counterpart.
///
/// Not total in this direction: [`TaskState::Pending`] and [`TaskState::Expired`] have no MCP
/// Tasks counterpart, since MCP Tasks has no queued state and no lease/deadline-expiry state.
/// Both are coerced — `Pending` to `working` (MCP has no way to represent "not yet started" other
/// than as already working), `Expired` to `failed` (terminal and unsuccessful) — and both
/// coercions are reported via [`Mapped::coercion`], never silently dropped.
pub fn map_outbound_task_state(state: TaskState) -> Mapped<McpTaskStatus> {
    match state {
        TaskState::Pending => Mapped {
            value: McpTaskStatus::Working,
            coercion: Some(Coercion {
                from: "pending",
                to: "working",
                reason: "MCP Tasks has no queued/not-yet-started status",
            }),
        },
        TaskState::Running => Mapped { value: McpTaskStatus::Working, coercion: None },
        TaskState::InputRequired => {
            Mapped { value: McpTaskStatus::InputRequired, coercion: None }
        }
        TaskState::Completed => Mapped { value: McpTaskStatus::Completed, coercion: None },
        TaskState::Cancelled => Mapped { value: McpTaskStatus::Cancelled, coercion: None },
        TaskState::Failed => Mapped { value: McpTaskStatus::Failed, coercion: None },
        TaskState::Expired => Mapped {
            value: McpTaskStatus::Failed,
            coercion: Some(Coercion {
                from: "expired",
                to: "failed",
                reason: "MCP Tasks has no analogue of a lease/deadline expiry status",
            }),
        },
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const DIRECT_PAIRS: [(McpTaskStatus, TaskState); 5] = [
        (McpTaskStatus::Working, TaskState::Running),
        (McpTaskStatus::InputRequired, TaskState::InputRequired),
        (McpTaskStatus::Completed, TaskState::Completed),
        (McpTaskStatus::Failed, TaskState::Failed),
        (McpTaskStatus::Cancelled, TaskState::Cancelled),
    ];

    #[test]
    fn direct_pairs_round_trip_inbound_and_outbound_without_coercion() {
        for (mcp, mem) in DIRECT_PAIRS {
            let inbound = map_inbound_task_status(mcp);
            assert_eq!(inbound.value, mem);
            assert!(inbound.coercion.is_none(), "direct pair must not be reported as coerced");

            let outbound = map_outbound_task_state(mem);
            assert_eq!(outbound.value, mcp);
            assert!(outbound.coercion.is_none(), "direct pair must not be reported as coerced");
        }
    }

    #[test]
    fn outbound_pending_is_reported_as_coerced_to_working() {
        let mapped = map_outbound_task_state(TaskState::Pending);
        assert_eq!(mapped.value, McpTaskStatus::Working);
        let coercion = mapped.coercion.expect("Pending must be reported as a coercion");
        assert_eq!(coercion.from, "pending");
        assert_eq!(coercion.to, "working");
    }

    #[test]
    fn outbound_expired_is_reported_as_coerced_to_failed() {
        let mapped = map_outbound_task_state(TaskState::Expired);
        assert_eq!(mapped.value, McpTaskStatus::Failed);
        let coercion = mapped.coercion.expect("Expired must be reported as a coercion");
        assert_eq!(coercion.from, "expired");
        assert_eq!(coercion.to, "failed");
    }

    /// Pins the exact wire strings so a future switch to a different `rename_all` (or a
    /// hand-written `Serialize` impl) fails loudly here rather than silently drifting from the
    /// real MCP Tasks wire format.
    #[test]
    fn wire_strings_match_the_extension_schema() {
        let cases = [
            (McpTaskStatus::Working, "\"working\""),
            (McpTaskStatus::InputRequired, "\"input_required\""),
            (McpTaskStatus::Completed, "\"completed\""),
            (McpTaskStatus::Failed, "\"failed\""),
            (McpTaskStatus::Cancelled, "\"cancelled\""),
        ];
        for (status, expected) in cases {
            let json = serde_json::to_string(&status).unwrap();
            assert_eq!(json, expected);
            let decoded: McpTaskStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded, status);
        }
    }
}

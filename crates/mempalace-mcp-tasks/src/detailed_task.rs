//! MCP Tasks `DetailedTask` (the `tasks/get` result shape) ↔ `coordination_tasks`, plus
//! `CreateTaskResult` (the immediate handle a server returns when it processes a request
//! asynchronously as a task).
//!
//! # `Task` is a discriminated union, not a flat struct
//!
//! `schema/2026-07-28/schema.ts` (`modelcontextprotocol/ext-tasks`) defines a base `Task`
//! (`taskId`, `status`, `statusMessage`, `createdAt`, `lastUpdatedAt`, `ttlMs`, `pollIntervalMs`)
//! and five variants discriminated on `status`, united as `DetailedTask`:
//!
//! - `WorkingTask` — `status: "working"`, no extra fields.
//! - `InputRequiredTask` — `status: "input_required"`, adds `inputRequests`.
//! - `CompletedTask` — `status: "completed"`, adds `result`.
//! - `FailedTask` — `status: "failed"`, adds `error` (a [`crate::json_rpc::JsonRpcErrorObject`]).
//! - `CancelledTask` — `status: "cancelled"`, no extra fields.
//!
//! [`DetailedTask`] models this as a Rust enum rather than a flat struct with four optional
//! fields (`inputRequests`/`result`/`error` plus the fields every variant shares). A flat struct
//! would permit illegal shapes a conforming MCP Tasks implementation must never produce or
//! accept — a `completed` task carrying `inputRequests`, a `working` task carrying `error` — and
//! the only way to keep such a struct honest is a `validate()` call the caller might forget.
//! `mempalace-a2a` had to bolt on exactly that kind of call twice after the fact (its `Part`
//! `oneof` invariant and its `Artifact.parts` non-empty invariant — deviations 29 and the
//! empty-`parts` fix in `docs/Coordination-Phase-3-Design.md`) because those invariants were
//! asserted in prose instead of the type system. [`DetailedTask`]'s five variants make the
//! illegal shapes unrepresentable instead: there is no `DetailedTask` value that combines
//! `status: Working` with a `result`, because [`DetailedTask::Working`] has no field to put one
//! in.
//!
//! The wire (de)serialization still has to reconcile a discriminated union with JSON's flat
//! object shape, so [`DetailedTask`] carries `#[serde(into = "RawDetailedTask", try_from =
//! "RawDetailedTask")]`: [`RawDetailedTask`] is the flat wire mirror (private to this module),
//! and the fallible [`TryFrom<RawDetailedTask>`] impl is where "exactly the right fields for this
//! `status`" is enforced — once, in one place, for both directions (decoding rejects a
//! non-conforming payload; encoding can never produce one, because it starts from an already-
//! valid [`DetailedTask`]).
//!
//! # `CreateTaskResult`
//!
//! `CreateTaskResult = Result & Task & { resultType: "task" }` — the *base* `Task` shape (bare
//! `status`, not the discriminated `DetailedTask`), plus the `resultType: "task"` literal that
//! tells a client "this is an async task handle, not the request's normal result". MemPalace's
//! own design doc previously omitted this type entirely; [`CreateTaskResult`] fills that gap.
//! [`TaskResultType`] makes the `"task"` discriminator itself unrepresentable as anything else,
//! the same way the `status`-keyed variants above make an illegal `DetailedTask` unrepresentable.
//!
//! # The hard part: `NewTask` needs fields MCP Tasks does not carry
//!
//! `mempalace_storage::coordination::NewTask` requires `title`, `description`, `wing`,
//! `created_by`, and `idempotency_key`. None of those exist on an MCP Tasks `Task`/`DetailedTask`
//! object — it identifies itself (`taskId`), carries its lifecycle (`status`, `statusMessage`),
//! and carries type-specific payload (`inputRequests`/`result`/`error`), but has no title, no
//! description distinct from that payload, no wing (MCP Tasks has no multi-tenancy concept at
//! all), and no notion of who created it. [`NewTaskInputs`] takes them as explicit, caller-
//! supplied parameters, the same pattern `mempalace_a2a::task::NewTaskInputs` uses for the
//! identical problem.
//!
//! # `NewTask` has no `state` field: a new task is always `Pending`
//!
//! Exactly the constraint `mempalace_a2a::task` documents: `CoordinationStore::create_task`
//! always creates a task in [`TaskState::Pending`], so an inbound MCP Tasks object whose `status`
//! is not something that maps to `Pending` (nothing does directly — see [`crate::status`], since
//! MCP Tasks has no queued state) cannot be reflected at creation time.
//! [`detailed_task_to_new_task`]/[`create_task_result_to_new_task`] still run the source status
//! through [`crate::status::map_inbound_task_status`] and return the result alongside the
//! [`NewTask`], so the caller can see what the *target* state should be.
//!
//! # Reaching `target_state` is not always one call
//!
//! Identical constraint to `mempalace_a2a::task`, because it comes from
//! `CoordinationStore::allowed_transition` (`crates/mempalace-storage/src/coordination.rs`), not
//! from anything protocol-specific: `Pending -> Cancelled | Expired`, `Running -> InputRequired |
//! Completed | Cancelled | Failed | Expired`, `InputRequired -> Pending | Running | Cancelled |
//! Failed | Expired`. `Pending -> Running` is not in that table — the only way out of `Pending`
//! into `Running` is `CoordinationStore::claim_task`, which needs a worker identity and lease
//! `ttl` this crate has no source for. So, per [`NewTaskConversion::target_state`]:
//!
//! - [`TaskState::Pending`] — never the target here: MCP Tasks has no status that maps to it.
//! - [`TaskState::Cancelled`]/[`TaskState::Expired`] — a single `transition_task` call.
//! - [`TaskState::Running`] — `claim_task`, not `transition_task`.
//! - [`TaskState::InputRequired`]/[`TaskState::Completed`]/[`TaskState::Failed`] — `claim_task`
//!   first (to reach `Running`), then `transition_task` from `Running` to the target.
//!
//! This crate does not provide a helper that performs that sequence, for the same reason
//! `mempalace_a2a::task` does not: it is a pure translation library with no live
//! `CoordinationStore` dependency, and no source for the worker identity/lease TTL `claim_task`
//! requires. Do not assume a single call always suffices.
//!
//! # `ttlMs`, and fields with no home in `NewTask`
//!
//! `ttlMs` maps onto `NewTask::expires_at` via [`crate::ttl::ttl_ms_to_expires_at`] — see that
//! module for the null/absent-means-unlimited handling. `pollIntervalMs` is adapter policy, not
//! stored state, and is dropped on the way into [`NewTask`] (there is no column for it and none
//! should be added). `NewTask::parent_id`, `dependencies`, and `budget` are set to their absent
//! value: MCP Tasks carries no analogue for any of them.

use std::collections::HashMap;

use mempalace_storage::{NewTask, Task, TaskState};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use time::OffsetDateTime;

use crate::error::McpTasksError;
use crate::json_rpc::JsonRpcErrorObject;
use crate::status::{Mapped, McpTaskStatus, map_inbound_task_status, map_outbound_task_state};
use crate::ttl::{expires_at_to_ttl_ms, ttl_ms_to_expires_at};

/// Fields every `DetailedTask` variant carries, regardless of `status`.
#[derive(Debug, Clone, PartialEq)]
pub struct DetailedTaskCommon {
    /// Unique task identifier.
    pub task_id: String,
    /// Human-readable status detail, if the server supplied one.
    pub status_message: Option<String>,
    /// When the task was created.
    pub created_at: OffsetDateTime,
    /// When the task's status was last updated.
    pub last_updated_at: OffsetDateTime,
    /// Milliseconds from `created_at` after which the server may discard the task; `None` means
    /// "no TTL" (wire `null` or an absent field — see [`crate::ttl`] for why both collapse here).
    pub ttl_ms: Option<u64>,
    /// Recommended client polling interval in milliseconds, if the server supplied one.
    pub poll_interval_ms: Option<u64>,
}

/// The `tasks/get` result shape: a `Task` discriminated on `status` into one of five variants.
/// See the module docs for why this is an enum rather than a flat struct.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(into = "RawDetailedTask", try_from = "RawDetailedTask")]
pub enum DetailedTask {
    /// `status: "working"` — actively being processed, no extra payload.
    Working(DetailedTaskCommon),
    /// `status: "input_required"` — awaiting client-provided input.
    InputRequired {
        /// Fields common to every variant.
        common: DetailedTaskCommon,
        /// Opaque server-to-client requests keyed by an implementation-defined id, per
        /// `InputRequiredTask.inputRequests`. Not modeled further: the extension leaves the
        /// request shape to the underlying MCP method, and this crate does not need to interpret
        /// it to translate the task envelope.
        input_requests: HashMap<String, Value>,
    },
    /// `status: "completed"` — finished successfully; `result` is the underlying request's
    /// result object.
    Completed {
        /// Fields common to every variant.
        common: DetailedTaskCommon,
        /// The completed result payload.
        result: Value,
    },
    /// `status: "failed"` — finished with a JSON-RPC error.
    Failed {
        /// Fields common to every variant.
        common: DetailedTaskCommon,
        /// The JSON-RPC error describing the failure.
        error: JsonRpcErrorObject,
    },
    /// `status: "cancelled"` — terminated before completion, no extra payload.
    Cancelled(DetailedTaskCommon),
}

impl DetailedTask {
    /// The fields common to every variant.
    #[must_use]
    pub fn common(&self) -> &DetailedTaskCommon {
        match self {
            Self::Working(common) | Self::Cancelled(common) => common,
            Self::InputRequired { common, .. }
            | Self::Completed { common, .. }
            | Self::Failed { common, .. } => common,
        }
    }

    /// The status this variant represents.
    #[must_use]
    pub fn status(&self) -> McpTaskStatus {
        match self {
            Self::Working(_) => McpTaskStatus::Working,
            Self::InputRequired { .. } => McpTaskStatus::InputRequired,
            Self::Completed { .. } => McpTaskStatus::Completed,
            Self::Failed { .. } => McpTaskStatus::Failed,
            Self::Cancelled(_) => McpTaskStatus::Cancelled,
        }
    }
}

/// Flat wire mirror of `DetailedTask`, used only as the target of [`DetailedTask`]'s
/// `serde(into, try_from)` conversion. Not exported: nothing outside this module should ever
/// construct one directly, since doing so bypasses the variant/field validation
/// [`TryFrom<RawDetailedTask>`] performs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawDetailedTask {
    task_id: String,
    status: McpTaskStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    status_message: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    last_updated_at: OffsetDateTime,
    ttl_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    poll_interval_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    input_requests: Option<HashMap<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcErrorObject>,
}

fn reject_present(present: bool, status: &'static str, field: &'static str) -> Result<(), McpTasksError> {
    if present { Err(McpTasksError::UnexpectedField { status, field }) } else { Ok(()) }
}

impl TryFrom<RawDetailedTask> for DetailedTask {
    type Error = McpTasksError;

    /// Enforces "exactly the right fields for this `status`", per the module docs. Every
    /// non-matching field (present when it should be absent, or absent when required) is
    /// rejected explicitly rather than silently ignored or defaulted.
    fn try_from(raw: RawDetailedTask) -> Result<Self, McpTasksError> {
        let common = DetailedTaskCommon {
            task_id: raw.task_id,
            status_message: raw.status_message,
            created_at: raw.created_at,
            last_updated_at: raw.last_updated_at,
            ttl_ms: raw.ttl_ms,
            poll_interval_ms: raw.poll_interval_ms,
        };
        match raw.status {
            McpTaskStatus::Working => {
                reject_present(raw.input_requests.is_some(), "working", "inputRequests")?;
                reject_present(raw.result.is_some(), "working", "result")?;
                reject_present(raw.error.is_some(), "working", "error")?;
                Ok(Self::Working(common))
            }
            McpTaskStatus::InputRequired => {
                let input_requests = raw.input_requests.ok_or(McpTasksError::MissingField {
                    status: "input_required",
                    field: "inputRequests",
                })?;
                reject_present(raw.result.is_some(), "input_required", "result")?;
                reject_present(raw.error.is_some(), "input_required", "error")?;
                Ok(Self::InputRequired { common, input_requests })
            }
            McpTaskStatus::Completed => {
                let result = raw
                    .result
                    .ok_or(McpTasksError::MissingField { status: "completed", field: "result" })?;
                reject_present(raw.input_requests.is_some(), "completed", "inputRequests")?;
                reject_present(raw.error.is_some(), "completed", "error")?;
                Ok(Self::Completed { common, result })
            }
            McpTaskStatus::Failed => {
                let error = raw
                    .error
                    .ok_or(McpTasksError::MissingField { status: "failed", field: "error" })?;
                reject_present(raw.input_requests.is_some(), "failed", "inputRequests")?;
                reject_present(raw.result.is_some(), "failed", "result")?;
                Ok(Self::Failed { common, error })
            }
            McpTaskStatus::Cancelled => {
                reject_present(raw.input_requests.is_some(), "cancelled", "inputRequests")?;
                reject_present(raw.result.is_some(), "cancelled", "result")?;
                reject_present(raw.error.is_some(), "cancelled", "error")?;
                Ok(Self::Cancelled(common))
            }
        }
    }
}

impl From<DetailedTask> for RawDetailedTask {
    fn from(detailed: DetailedTask) -> Self {
        fn base(status: McpTaskStatus, common: DetailedTaskCommon) -> RawDetailedTask {
            RawDetailedTask {
                task_id: common.task_id,
                status,
                status_message: common.status_message,
                created_at: common.created_at,
                last_updated_at: common.last_updated_at,
                ttl_ms: common.ttl_ms,
                poll_interval_ms: common.poll_interval_ms,
                input_requests: None,
                result: None,
                error: None,
            }
        }
        match detailed {
            DetailedTask::Working(common) => base(McpTaskStatus::Working, common),
            DetailedTask::InputRequired { common, input_requests } => {
                RawDetailedTask {
                    input_requests: Some(input_requests),
                    ..base(McpTaskStatus::InputRequired, common)
                }
            }
            DetailedTask::Completed { common, result } => {
                RawDetailedTask { result: Some(result), ..base(McpTaskStatus::Completed, common) }
            }
            DetailedTask::Failed { common, error } => {
                RawDetailedTask { error: Some(error), ..base(McpTaskStatus::Failed, common) }
            }
            DetailedTask::Cancelled(common) => base(McpTaskStatus::Cancelled, common),
        }
    }
}

/// Discriminator literal for [`CreateTaskResult`]. Always serializes as `"task"` and rejects
/// decoding anything else — the same "make the illegal value unrepresentable" approach the
/// `status`-keyed [`DetailedTask`] variants use, applied to a single literal field instead of a
/// whole discriminated union.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TaskResultType;

impl Serialize for TaskResultType {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str("task")
    }
}

impl<'de> Deserialize<'de> for TaskResultType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        if value == "task" {
            Ok(Self)
        } else {
            Err(D::Error::custom(format!("expected resultType \"task\", got \"{value}\"")))
        }
    }
}

/// `CreateTaskResult = Result & Task & { resultType: "task" }` — the handle a server returns
/// immediately when it decides to process a request as an async task. Unlike [`DetailedTask`],
/// `status` here is the bare [`McpTaskStatus`]: a `CreateTaskResult` is not discriminated into
/// `inputRequests`/`result`/`error` variants, because at creation time none of those exist yet.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateTaskResult {
    /// Unique task identifier.
    pub task_id: String,
    /// The task's status at the moment this result was returned.
    pub status: McpTaskStatus,
    /// Human-readable status detail, if the server supplied one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_message: Option<String>,
    /// When the task was created.
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    /// When the task's status was last updated.
    #[serde(with = "time::serde::rfc3339")]
    pub last_updated_at: OffsetDateTime,
    /// Milliseconds from `created_at` after which the server may discard the task; `None` means
    /// "no TTL" — see [`crate::ttl`].
    pub ttl_ms: Option<u64>,
    /// Recommended client polling interval in milliseconds, if the server supplied one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub poll_interval_ms: Option<u64>,
    /// Always `"task"` on the wire; see [`TaskResultType`].
    pub result_type: TaskResultType,
}

/// Caller-supplied fields [`NewTask`] requires that MCP Tasks has no source for.
///
/// See the module docs' "The hard part" section for why these cannot be defaulted.
#[derive(Debug, Clone)]
pub struct NewTaskInputs<'a> {
    /// Human-readable task title. MCP Tasks has no equivalent field.
    pub title: &'a str,
    /// Task description. MCP Tasks has no equivalent field.
    pub description: &'a str,
    /// Owning wing. MCP Tasks has no multi-tenancy concept at all.
    pub wing: &'a str,
    /// Actor recorded as having created the task.
    pub created_by: &'a str,
    /// Idempotency key for the underlying `create_task` call, resolved by the caller the same
    /// way every other coordination write resolves one.
    pub idempotency_key: String,
}

/// The result of translating an inbound MCP Tasks object into a MemPalace [`NewTask`].
#[derive(Debug, Clone)]
pub struct NewTaskConversion {
    /// The task to create. Always creates in [`TaskState::Pending`] — see the module docs.
    pub new_task: NewTask,
    /// Where the task's state should end up, per [`crate::status::map_inbound_task_status`].
    /// `create_task` cannot create directly into any state but `Pending`, so the caller must
    /// apply this afterward; see the module docs' "Reaching `target_state` is not always one
    /// call" section for the exact per-state sequence. This mapping never coerces (MCP Tasks'
    /// inbound mapping is total — see [`crate::status`]), but `coercion` is still carried through
    /// for symmetry with the outbound direction and so a caller inspecting it never has to guess
    /// whether the field was omitted on purpose.
    pub target_state: Mapped<TaskState>,
}

/// Translate an inbound [`DetailedTask`] into a [`NewTask`], plus the state it should be
/// transitioned to after creation.
///
/// # Errors
///
/// Returns [`McpTasksError::TtlOutOfRange`] if `common.created_at + common.ttl_ms` (see
/// [`crate::ttl::ttl_ms_to_expires_at`]) overflows the representable timestamp range.
pub fn detailed_task_to_new_task(
    detailed: &DetailedTask,
    inputs: NewTaskInputs<'_>,
) -> Result<NewTaskConversion, McpTasksError> {
    let common = detailed.common();
    let target_state = map_inbound_task_status(detailed.status());
    let expires_at = ttl_ms_to_expires_at(common.created_at, common.ttl_ms)?;
    let new_task = NewTask {
        title: inputs.title.to_owned(),
        description: inputs.description.to_owned(),
        created_by: inputs.created_by.to_owned(),
        wing: inputs.wing.to_owned(),
        idempotency_key: inputs.idempotency_key,
        parent_id: None,
        dependencies: Vec::new(),
        budget: None,
        expires_at,
    };
    Ok(NewTaskConversion { new_task, target_state })
}

/// Translate an inbound [`CreateTaskResult`] into a [`NewTask`], plus the state it should be
/// transitioned to after creation. See [`detailed_task_to_new_task`] for the equivalent
/// [`DetailedTask`] conversion and the shared module docs for why both need [`NewTaskInputs`].
///
/// # Errors
///
/// Returns [`McpTasksError::TtlOutOfRange`] under the same condition as
/// [`detailed_task_to_new_task`].
pub fn create_task_result_to_new_task(
    result: &CreateTaskResult,
    inputs: NewTaskInputs<'_>,
) -> Result<NewTaskConversion, McpTasksError> {
    let target_state = map_inbound_task_status(result.status);
    let expires_at = ttl_ms_to_expires_at(result.created_at, result.ttl_ms)?;
    let new_task = NewTask {
        title: inputs.title.to_owned(),
        description: inputs.description.to_owned(),
        created_by: inputs.created_by.to_owned(),
        wing: inputs.wing.to_owned(),
        idempotency_key: inputs.idempotency_key,
        parent_id: None,
        dependencies: Vec::new(),
        budget: None,
        expires_at,
    };
    Ok(NewTaskConversion { new_task, target_state })
}

/// Translate an outbound MemPalace [`Task`] into a [`DetailedTask`], for a `tasks/get` response.
///
/// `status_message` and `poll_interval_ms` are supplied by the caller — [`Task`] has no dedicated
/// "current status message" column, and `pollIntervalMs` is adapter policy this crate does not
/// hold an opinion on (see the module docs). `input_requests`/`result`/`error` are likewise
/// caller-supplied: MemPalace's `Task` carries none of them directly (`result` would come from a
/// separate `coordination_task_results` row, `error` and `input_requests` have no MemPalace
/// storage home at all), and passing exactly the wrong combination for the mapped status is a
/// caller error this function reports rather than silently patches over.
///
/// The returned [`Mapped::coercion`] reports whether `task.state` required coercion to reach its
/// MCP Tasks counterpart (`Pending`/`Expired` — see [`crate::status::map_outbound_task_state`]).
///
/// # Errors
///
/// Returns [`McpTasksError::MissingField`]/[`McpTasksError::UnexpectedField`] if
/// `input_requests`/`result`/`error` do not match exactly what the mapped status requires (see
/// the module docs' "`Task` is a discriminated union" section).
pub fn task_to_detailed_task(
    task: &Task,
    status_message: Option<String>,
    poll_interval_ms: Option<u64>,
    input_requests: Option<HashMap<String, Value>>,
    result: Option<Value>,
    error: Option<JsonRpcErrorObject>,
) -> Result<Mapped<DetailedTask>, McpTasksError> {
    let mapped_status = map_outbound_task_state(task.state);
    let ttl_ms = expires_at_to_ttl_ms(task.created_at, task.expires_at);
    let raw = RawDetailedTask {
        task_id: task.task_id.clone(),
        status: mapped_status.value,
        status_message,
        created_at: task.created_at,
        last_updated_at: task.updated_at,
        ttl_ms,
        poll_interval_ms,
        input_requests,
        result,
        error,
    };
    let value = DetailedTask::try_from(raw)?;
    Ok(Mapped { value, coercion: mapped_status.coercion })
}

/// Translate an outbound MemPalace [`Task`] into a [`CreateTaskResult`] — the handle a server
/// returns immediately after deciding to process a request as an async task.
///
/// `status_message` and `poll_interval_ms` are caller-supplied for the same reason as in
/// [`task_to_detailed_task`]. The returned [`Mapped::coercion`] reports the same
/// `Pending`/`Expired` coercion.
#[must_use]
pub fn task_to_create_task_result(
    task: &Task,
    status_message: Option<String>,
    poll_interval_ms: Option<u64>,
) -> Mapped<CreateTaskResult> {
    let mapped_status = map_outbound_task_state(task.state);
    let ttl_ms = expires_at_to_ttl_ms(task.created_at, task.expires_at);
    let value = CreateTaskResult {
        task_id: task.task_id.clone(),
        status: mapped_status.value,
        status_message,
        created_at: task.created_at,
        last_updated_at: task.updated_at,
        ttl_ms,
        poll_interval_ms,
        result_type: TaskResultType,
    };
    Mapped { value, coercion: mapped_status.coercion }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use mempalace_storage::TaskState;

    use super::*;

    fn common() -> DetailedTaskCommon {
        DetailedTaskCommon {
            task_id: "task_1".to_owned(),
            status_message: Some("halfway there".to_owned()),
            created_at: OffsetDateTime::now_utc(),
            last_updated_at: OffsetDateTime::now_utc(),
            ttl_ms: Some(60_000),
            poll_interval_ms: Some(2_000),
        }
    }

    #[test]
    fn each_variant_round_trips_through_json() {
        let cases = vec![
            DetailedTask::Working(common()),
            DetailedTask::InputRequired {
                common: common(),
                input_requests: HashMap::from([(
                    "req_1".to_owned(),
                    serde_json::json!({"kind": "elicit"}),
                )]),
            },
            DetailedTask::Completed { common: common(), result: serde_json::json!({"ok": true}) },
            DetailedTask::Failed {
                common: common(),
                error: JsonRpcErrorObject {
                    code: crate::json_rpc::INTERNAL_ERROR,
                    message: "boom".to_owned(),
                    data: None,
                },
            },
            DetailedTask::Cancelled(common()),
        ];
        for case in cases {
            let json = serde_json::to_string(&case).unwrap();
            let decoded: DetailedTask = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded, case);
        }
    }

    #[test]
    fn working_status_serializes_without_variant_specific_fields() {
        let json = serde_json::to_string(&DetailedTask::Working(common())).unwrap();
        assert!(!json.contains("inputRequests"));
        assert!(!json.contains("\"result\""));
        assert!(!json.contains("\"error\""));
        assert!(json.contains("\"status\":\"working\""));
    }

    #[test]
    fn ttl_ms_none_serializes_as_null_not_omitted() {
        let mut base = common();
        base.ttl_ms = None;
        let json = serde_json::to_string(&DetailedTask::Working(base)).unwrap();
        assert!(json.contains("\"ttlMs\":null"), "got {json}");
    }

    #[test]
    fn decoding_completed_without_result_is_rejected() {
        let raw = serde_json::json!({
            "taskId": "task_1",
            "status": "completed",
            "createdAt": "2026-01-01T00:00:00Z",
            "lastUpdatedAt": "2026-01-01T00:00:00Z",
            "ttlMs": null,
        });
        let err = serde_json::from_value::<DetailedTask>(raw)
            .expect_err("a completed task with no result must be rejected");
        assert!(err.to_string().contains("must carry `result`"));
    }

    #[test]
    fn decoding_working_with_a_result_is_rejected() {
        let raw = serde_json::json!({
            "taskId": "task_1",
            "status": "working",
            "createdAt": "2026-01-01T00:00:00Z",
            "lastUpdatedAt": "2026-01-01T00:00:00Z",
            "ttlMs": null,
            "result": {"ok": true},
        });
        let err = serde_json::from_value::<DetailedTask>(raw)
            .expect_err("a working task with a result must be rejected");
        assert!(err.to_string().contains("must not carry `result`"));
    }

    #[test]
    fn create_task_result_type_rejects_wrong_literal() {
        let raw = serde_json::json!({
            "taskId": "task_1",
            "status": "working",
            "createdAt": "2026-01-01T00:00:00Z",
            "lastUpdatedAt": "2026-01-01T00:00:00Z",
            "ttlMs": null,
            "resultType": "complete",
        });
        let err = serde_json::from_value::<CreateTaskResult>(raw)
            .expect_err("resultType other than \"task\" must be rejected");
        assert!(err.to_string().contains("expected resultType"));
    }

    #[test]
    fn create_task_result_round_trips_through_json() {
        let result = CreateTaskResult {
            task_id: "task_1".to_owned(),
            status: McpTaskStatus::Working,
            status_message: None,
            created_at: OffsetDateTime::now_utc(),
            last_updated_at: OffsetDateTime::now_utc(),
            ttl_ms: None,
            poll_interval_ms: Some(1_000),
            result_type: TaskResultType,
        };
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"resultType\":\"task\""));
        let decoded: CreateTaskResult = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, result);
    }

    fn sample_inputs() -> NewTaskInputs<'static> {
        NewTaskInputs {
            title: "Summarize the quarterly report",
            description: "Client requested an async summary task.",
            wing: "wing_myproject",
            created_by: "alice",
            idempotency_key: "key_1".to_owned(),
        }
    }

    #[test]
    fn detailed_task_to_new_task_carries_caller_supplied_fields_and_ttl_through() {
        let detailed = DetailedTask::Working(common());
        let conversion = detailed_task_to_new_task(&detailed, sample_inputs()).unwrap();
        assert_eq!(conversion.new_task.title, "Summarize the quarterly report");
        assert_eq!(conversion.new_task.wing, "wing_myproject");
        assert_eq!(conversion.target_state.value, TaskState::Running);
        assert!(conversion.target_state.coercion.is_none());
        let expected_expires_at =
            ttl_ms_to_expires_at(detailed.common().created_at, detailed.common().ttl_ms).unwrap();
        assert_eq!(conversion.new_task.expires_at, expected_expires_at);
    }

    #[test]
    fn create_task_result_to_new_task_maps_working_to_running_target() {
        let result = CreateTaskResult {
            task_id: "task_1".to_owned(),
            status: McpTaskStatus::Working,
            status_message: None,
            created_at: OffsetDateTime::now_utc(),
            last_updated_at: OffsetDateTime::now_utc(),
            ttl_ms: None,
            poll_interval_ms: None,
            result_type: TaskResultType,
        };
        let conversion = create_task_result_to_new_task(&result, sample_inputs()).unwrap();
        assert_eq!(conversion.target_state.value, TaskState::Running);
        assert_eq!(conversion.new_task.expires_at, None);
    }

    fn sample_task(state: TaskState) -> Task {
        Task {
            task_id: "task_1".to_owned(),
            title: "Summarize the quarterly report".to_owned(),
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
            created_at: OffsetDateTime::now_utc(),
            updated_at: OffsetDateTime::now_utc(),
        }
    }

    #[test]
    fn task_to_detailed_task_maps_state_and_requires_matching_payload() {
        let task = sample_task(TaskState::Completed);
        let mapped = task_to_detailed_task(
            &task,
            None,
            None,
            None,
            Some(serde_json::json!({"ok": true})),
            None,
        )
        .unwrap();
        assert_eq!(mapped.value.status(), McpTaskStatus::Completed);
        assert!(mapped.coercion.is_none());

        let err = task_to_detailed_task(&task, None, None, None, None, None)
            .expect_err("a completed task requires a result");
        assert!(matches!(
            err,
            McpTasksError::MissingField { status: "completed", field: "result" }
        ));
    }

    #[test]
    fn task_to_detailed_task_reports_the_pending_coercion() {
        let task = sample_task(TaskState::Pending);
        let mapped = task_to_detailed_task(&task, None, None, None, None, None).unwrap();
        assert_eq!(mapped.value.status(), McpTaskStatus::Working);
        let coercion = mapped.coercion.expect("Pending must be reported as a coercion");
        assert_eq!(coercion.from, "pending");
        assert_eq!(coercion.to, "working");
    }

    #[test]
    fn task_to_create_task_result_reports_the_expired_coercion() {
        let task = sample_task(TaskState::Expired);
        let mapped = task_to_create_task_result(&task, None, None);
        assert_eq!(mapped.value.status, McpTaskStatus::Failed);
        let coercion = mapped.coercion.expect("Expired must be reported as a coercion");
        assert_eq!(coercion.from, "expired");
        assert_eq!(coercion.to, "failed");
    }
}

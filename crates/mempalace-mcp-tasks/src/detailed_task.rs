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
//!   `result: { [key: string]: unknown }` is a JSON *object*, so [`DetailedTask::Completed`]
//!   types it as `serde_json::Map<String, Value>` rather than `Value` — `null`, a scalar, or an
//!   array is a shape a conforming client must reject, so it must not be representable here
//!   either. Same reasoning as the discriminated-union point below, applied to one field instead
//!   of the whole enum.
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
//! [`CreateTaskResult`] also carries `Result::_meta` (see [`CreateTaskResult`]'s own doc comment)
//! — the base `Result` field this type had been dropping.
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
//! `ttlMs` does **not** map onto `NewTask::expires_at`. They mean different things — MCP `ttlMs`
//! is a retention hint ("the server may discard the task"), MemPalace `expires_at` is a lifecycle
//! deadline that `claim_task` enforces and this crate's own outbound mapping reports as `failed`
//! — see [`crate::ttl`]'s module docs for the full argument and the bug this used to cause. The
//! absolute deadline `ttlMs` implies is still computed (via [`crate::ttl::ttl_ms_to_deadline`])
//! but returned to the caller as [`ImportedTaskProvenance::retention_deadline`], never written
//! into [`NewTask::expires_at`] (which this crate always leaves `None` on the inbound path).
//!
//! `pollIntervalMs` is adapter policy, not stored state, and is dropped on the way into
//! [`NewTask`] (there is no column for it and none should be added). `NewTask::parent_id`,
//! `dependencies`, and `budget` are set to their absent value: MCP Tasks carries no analogue for
//! any of them.
//!
//! # `NewTask` cannot hold the inbound `taskId`, `createdAt`, or `lastUpdatedAt` — this is a real
//! # limitation, not silently handled
//!
//! `NewTask` has no `task_id` field: `CoordinationStore::create_task` generates its own local id.
//! It has no timestamp fields either: storage stamps import time for both `created_at` and
//! `updated_at`. So an inbound object's wire identity and its source timestamps have no MemPalace
//! column to land in, and this crate — a pure translation library with no storage handle and no
//! side channel of its own — cannot make them durable on its own. The envelope artifact
//! ([`crate::envelope`]) does not help here either: artifacts are queried by the *local* task id,
//! so it gives no wire-id → local-id path for a caller that only has the wire id (e.g. a later
//! `tasks/get`/`tasks/update`/`tasks/cancel` after a restart, or after the in-memory mapping the
//! caller may have kept is gone).
//!
//! **This crate cannot fix that from inside `mempalace-storage` — the caller must.**
//! [`detailed_task_to_new_task`]/[`create_task_result_to_new_task`] surface the source `taskId`,
//! `createdAt`, and `lastUpdatedAt` (plus the retention deadline from the previous section) on
//! [`NewTaskConversion::provenance`] as an [`ImportedTaskProvenance`], instead of dropping them or
//! pretending storage can round-trip them. If the caller does not itself persist the
//! `source_task_id` -> local `task_id` association (for example as a knowledge-graph fact, or a
//! side table) before discarding the [`NewTaskConversion`], **the imported task becomes
//! unreachable by its wire id after a restart** — the caller will have only the local id
//! `create_task` returned. Likewise, if the caller does not persist `source_created_at`/
//! `source_last_updated_at` somewhere it can retrieve them from later, [`task_to_detailed_task`]
//! will report storage's import-time timestamps instead of the original ones, and even an
//! otherwise-unchanged task will not round-trip its `createdAt`/`lastUpdatedAt` byte-for-byte.
//! An honest, prominent limitation here is deliberately preferred over a workaround that looks
//! complete but silently loses the mapping.

use std::collections::HashMap;

use mempalace_storage::{NewTask, Task, TaskState};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};
use time::OffsetDateTime;

use crate::error::McpTasksError;
use crate::json_rpc::JsonRpcErrorObject;
use crate::status::{Mapped, McpTaskStatus, map_inbound_task_status, map_outbound_task_state};
use crate::ttl::{deadline_to_ttl_ms, ttl_ms_to_deadline};

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
        /// The completed result payload. Typed as a JSON object
        /// (`serde_json::Map<String, Value>`), matching the schema's `result: { [key: string]:
        /// unknown }` — see the module docs' "`CompletedTask.result` must be a JSON object"
        /// section for why this is a `Map` rather than a post-hoc-validated `Value`.
        result: Map<String, Value>,
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
    /// Typed as `Map<String, Value>`, not `Value` — a bare JSON object is the only shape the
    /// schema's `result: { [key: string]: unknown }` permits, so serde rejects `null`, a scalar,
    /// or an array here automatically, rather than needing a post-hoc `validate()` call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result: Option<Map<String, Value>>,
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
///
/// # `Result`'s base fields
///
/// The core MCP schema's `Result` interface (`schema/2026-07-28/schema.ts` in
/// `modelcontextprotocol/modelcontextprotocol`) is `{ _meta?: ResultMetaObject; resultType:
/// ResultType; [key: string]: unknown }`. `resultType` is already modeled above as
/// [`TaskResultType`]; `_meta` is carried here as `meta` (opaque — this crate has no reason to
/// interpret `io.modelcontextprotocol/serverInfo` or any other key a server puts there) so it
/// round-trips instead of being silently dropped on decode and unrepresentable on encode. The
/// `[key: string]: unknown` index signature is TypeScript's way of saying "extra keys are legal
/// JSON-RPC result fields", not a field this type needs to model — nothing else in `Result` (nor
/// in `Task`, which this type also flattens) was found to need a home here.
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
    /// `Result::_meta`, carried opaquely. This crate does not interpret its contents (e.g.
    /// `io.modelcontextprotocol/serverInfo`) — it only needs to survive a decode→encode round
    /// trip so correlation/extension metadata a client or server attached is not silently lost.
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<Value>,
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
    /// `expires_at` is always `None` here: this crate never derives it from `ttlMs` — see the
    /// module docs' "`ttlMs`, and fields with no home in `NewTask`" section and
    /// [`crate::ttl`]. A caller that wants MCP retention to also drive MemPalace lifecycle expiry
    /// may set `new_task.expires_at` from `provenance.retention_deadline` itself.
    pub new_task: NewTask,
    /// Where the task's state should end up, per [`crate::status::map_inbound_task_status`].
    /// `create_task` cannot create directly into any state but `Pending`, so the caller must
    /// apply this afterward; see the module docs' "Reaching `target_state` is not always one
    /// call" section for the exact per-state sequence. This mapping never coerces (MCP Tasks'
    /// inbound mapping is total — see [`crate::status`]), but `coercion` is still carried through
    /// for symmetry with the outbound direction and so a caller inspecting it never has to guess
    /// whether the field was omitted on purpose.
    pub target_state: Mapped<TaskState>,
    /// Source-of-truth fields `NewTask` has no column for. **The caller must persist these
    /// itself** if it needs to resolve the wire identity or round-trip the original timestamps
    /// later — see the module docs' "`NewTask` cannot hold the inbound `taskId`, `createdAt`, or
    /// `lastUpdatedAt`" section. This crate has no storage handle and cannot do it for the caller.
    pub provenance: ImportedTaskProvenance,
}

/// Source-of-truth fields from an inbound MCP Tasks object that `NewTask` has no column for, and
/// that this crate cannot make durable on the caller's behalf.
///
/// See [`NewTaskConversion::provenance`] and the module docs' "`NewTask` cannot hold the inbound
/// `taskId`, `createdAt`, or `lastUpdatedAt`" section for why each field exists and what breaks
/// if the caller drops it on the floor instead of persisting it.
#[derive(Debug, Clone, PartialEq)]
pub struct ImportedTaskProvenance {
    /// The wire `taskId` from the inbound object. `CoordinationStore::create_task` assigns its
    /// own local task id, unrelated to this one. **Without the caller persisting a
    /// `source_task_id` -> local-id association, the imported task is unreachable by this id
    /// after a restart** — a later `tasks/get`/`tasks/update`/`tasks/cancel` carrying it cannot be
    /// resolved to the local record.
    pub source_task_id: String,
    /// The wire `createdAt` from the inbound object. Storage stamps its own import-time
    /// `created_at`; without the caller persisting this value separately,
    /// [`task_to_detailed_task`]/[`task_to_create_task_result`] will later report the import time
    /// instead of the true creation time.
    pub source_created_at: OffsetDateTime,
    /// The wire `lastUpdatedAt` from the inbound object. Same caveat as `source_created_at`:
    /// storage stamps its own import-time `updated_at`, and this value is lost unless the caller
    /// persists it.
    pub source_last_updated_at: OffsetDateTime,
    /// The absolute deadline `ttlMs` implies, relative to `source_created_at` — see
    /// [`crate::ttl`] for why this is retention, not lifecycle, and therefore is not written to
    /// `new_task.expires_at`. `None` means the inbound object had no TTL (wire `null` or an
    /// absent field).
    pub retention_deadline: Option<OffsetDateTime>,
}

/// Translate an inbound [`DetailedTask`] into a [`NewTask`], plus the state it should be
/// transitioned to after creation and the [`ImportedTaskProvenance`] the caller must persist
/// itself — see the module docs' "`NewTask` cannot hold the inbound `taskId`, `createdAt`, or
/// `lastUpdatedAt`" section.
///
/// `new_task.expires_at` is always `None`: `ttlMs` is retention, not lifecycle, and is never
/// written there — see [`crate::ttl`] and the module docs' "`ttlMs`, and fields with no home in
/// `NewTask`" section. The absolute deadline it implies is returned as
/// `provenance.retention_deadline` instead.
///
/// # Errors
///
/// Returns [`McpTasksError::TtlOutOfRange`] if `common.created_at + common.ttl_ms` (see
/// [`crate::ttl::ttl_ms_to_deadline`]) overflows the representable timestamp range.
pub fn detailed_task_to_new_task(
    detailed: &DetailedTask,
    inputs: NewTaskInputs<'_>,
) -> Result<NewTaskConversion, McpTasksError> {
    let common = detailed.common();
    let target_state = map_inbound_task_status(detailed.status());
    let retention_deadline = ttl_ms_to_deadline(common.created_at, common.ttl_ms)?;
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
    let provenance = ImportedTaskProvenance {
        source_task_id: common.task_id.clone(),
        source_created_at: common.created_at,
        source_last_updated_at: common.last_updated_at,
        retention_deadline,
    };
    Ok(NewTaskConversion { new_task, target_state, provenance })
}

/// Translate an inbound [`CreateTaskResult`] into a [`NewTask`], plus the state it should be
/// transitioned to after creation and the [`ImportedTaskProvenance`] the caller must persist
/// itself. See [`detailed_task_to_new_task`] for the equivalent [`DetailedTask`] conversion, the
/// shared module docs for why both need [`NewTaskInputs`], and why `new_task.expires_at` is
/// always `None` here too.
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
    let retention_deadline = ttl_ms_to_deadline(result.created_at, result.ttl_ms)?;
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
    let provenance = ImportedTaskProvenance {
        source_task_id: result.task_id.clone(),
        source_created_at: result.created_at,
        source_last_updated_at: result.last_updated_at,
        retention_deadline,
    };
    Ok(NewTaskConversion { new_task, target_state, provenance })
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
///
/// The outbound `ttlMs` is computed from `task.expires_at` — MemPalace's lifecycle deadline, not
/// a round-tripped retention hint (this crate never wrote one there — see [`crate::ttl`]). A
/// caller that separately persisted an inbound `retention_deadline`
/// ([`ImportedTaskProvenance::retention_deadline`]) and wants to report *that* as `ttlMs` instead
/// should compute it with [`deadline_to_ttl_ms`] directly.
pub fn task_to_detailed_task(
    task: &Task,
    status_message: Option<String>,
    poll_interval_ms: Option<u64>,
    input_requests: Option<HashMap<String, Value>>,
    result: Option<Map<String, Value>>,
    error: Option<JsonRpcErrorObject>,
) -> Result<Mapped<DetailedTask>, McpTasksError> {
    let mapped_status = map_outbound_task_state(task.state);
    let ttl_ms = deadline_to_ttl_ms(task.created_at, task.expires_at);
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
/// `status_message`, `poll_interval_ms`, and `meta` are caller-supplied for the same reason as
/// `status_message`/`poll_interval_ms` in [`task_to_detailed_task`]: [`Task`] has no dedicated
/// column for any of them, and `meta` (`Result::_meta`) in particular is per-response
/// correlation/extension data this crate has no source for. The returned [`Mapped::coercion`]
/// reports the same `Pending`/`Expired` coercion.
///
/// `ttl_ms` here is computed from `task.expires_at`, which is MemPalace's *lifecycle* deadline —
/// not a retention hint round-tripped from an inbound `ttlMs` (this crate never wrote one there
/// to begin with; see [`crate::ttl`]). A caller that separately tracked an inbound
/// `retention_deadline` and wants to report *that* as outbound `ttlMs` instead should compute it
/// with [`deadline_to_ttl_ms`] directly rather than relying on this function's use of
/// `task.expires_at`.
#[must_use]
pub fn task_to_create_task_result(
    task: &Task,
    status_message: Option<String>,
    poll_interval_ms: Option<u64>,
    meta: Option<Value>,
) -> Mapped<CreateTaskResult> {
    let mapped_status = map_outbound_task_state(task.state);
    let ttl_ms = deadline_to_ttl_ms(task.created_at, task.expires_at);
    let value = CreateTaskResult {
        task_id: task.task_id.clone(),
        status: mapped_status.value,
        status_message,
        created_at: task.created_at,
        last_updated_at: task.updated_at,
        ttl_ms,
        poll_interval_ms,
        meta,
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
            DetailedTask::Completed {
                common: common(),
                result: serde_json::Map::from_iter([("ok".to_owned(), serde_json::json!(true))]),
            },
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

    /// `ttlMs` is a *required key with a nullable value* in the extension schema (`number |
    /// null`), so this crate always serializes it — including as explicit `null` — but must still
    /// tolerate a payload that omits the key entirely, since serde's derive treats a missing
    /// `Option<T>` field as `None` without needing `#[serde(default)]`. This is the regression
    /// test for that tolerance on the [`DetailedTask`] side.
    #[test]
    fn absent_ttl_ms_decodes_as_none() {
        let raw = serde_json::json!({
            "taskId": "task_1",
            "status": "working",
            "createdAt": "2026-01-01T00:00:00Z",
            "lastUpdatedAt": "2026-01-01T00:00:00Z",
        });
        let decoded: DetailedTask = serde_json::from_value(raw).unwrap();
        assert_eq!(decoded.common().ttl_ms, None);
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
            meta: None,
            result_type: TaskResultType,
        };
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"resultType\":\"task\""));
        assert!(!json.contains("_meta"), "absent meta must not be serialized, got {json}");
        let decoded: CreateTaskResult = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, result);
    }

    /// Finding B: the core MCP `Result` interface's `_meta` field must survive a decode -> encode
    /// round trip verbatim rather than being silently dropped. `meta` is kept opaque (`Value`) —
    /// this crate has no reason to interpret `io.modelcontextprotocol/serverInfo` or any other
    /// key a server attaches.
    #[test]
    fn create_task_result_meta_round_trips_through_json() {
        let meta = serde_json::json!({
            "io.modelcontextprotocol/serverInfo": {"name": "mempalace", "version": "0.1.0"},
            "custom.example/traceId": "abc123",
        });
        let raw = serde_json::json!({
            "taskId": "task_1",
            "status": "working",
            "createdAt": "2026-01-01T00:00:00Z",
            "lastUpdatedAt": "2026-01-01T00:00:00Z",
            "ttlMs": null,
            "resultType": "task",
            "_meta": meta,
        });
        let decoded: CreateTaskResult = serde_json::from_value(raw).unwrap();
        assert_eq!(decoded.meta, Some(meta.clone()));

        let encoded = serde_json::to_value(&decoded).unwrap();
        assert_eq!(encoded["_meta"], meta, "encoded payload: {encoded}");

        // A second decode -> encode cycle must reproduce the identical `_meta` value.
        let round_tripped: CreateTaskResult = serde_json::from_value(encoded.clone()).unwrap();
        assert_eq!(round_tripped.meta, decoded.meta);
    }

    #[test]
    fn create_task_result_absent_meta_is_omitted_on_encode_and_none_on_decode() {
        let raw = serde_json::json!({
            "taskId": "task_1",
            "status": "working",
            "createdAt": "2026-01-01T00:00:00Z",
            "lastUpdatedAt": "2026-01-01T00:00:00Z",
            "ttlMs": null,
            "resultType": "task",
        });
        let decoded: CreateTaskResult = serde_json::from_value(raw).unwrap();
        assert_eq!(decoded.meta, None);
        let encoded = serde_json::to_value(&decoded).unwrap();
        assert!(encoded.get("_meta").is_none(), "encoded payload: {encoded}");
    }

    /// Same tolerance as [`absent_ttl_ms_decodes_as_none`] above, but for [`CreateTaskResult`],
    /// which the PR reviewer flagged as tested less thoroughly than [`DetailedTask`] for this
    /// exact contract. `resultType` must still be present — only `ttlMs` is omitted here.
    #[test]
    fn create_task_result_absent_ttl_ms_decodes_as_none() {
        let raw = serde_json::json!({
            "taskId": "task_1",
            "status": "working",
            "createdAt": "2026-01-01T00:00:00Z",
            "lastUpdatedAt": "2026-01-01T00:00:00Z",
            "resultType": "task",
        });
        let decoded: CreateTaskResult = serde_json::from_value(raw).unwrap();
        assert_eq!(decoded.ttl_ms, None);
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
    fn detailed_task_to_new_task_carries_caller_supplied_fields_through() {
        let detailed = DetailedTask::Working(common());
        let conversion = detailed_task_to_new_task(&detailed, sample_inputs()).unwrap();
        assert_eq!(conversion.new_task.title, "Summarize the quarterly report");
        assert_eq!(conversion.new_task.wing, "wing_myproject");
        assert_eq!(conversion.target_state.value, TaskState::Running);
        assert!(conversion.target_state.coercion.is_none());
        // Finding A regression: `ttlMs` must never land in `expires_at` (it is retention, not
        // MemPalace lifecycle) -- `create_task` is always given `expires_at: None`.
        assert_eq!(conversion.new_task.expires_at, None);
    }

    /// Finding A regression: a `completed` MCP task with a TTL that has since elapsed must not
    /// come back to life as a fabricated MemPalace `expires_at`/`Expired` state. The retention
    /// deadline `ttlMs` implies is surfaced on `provenance.retention_deadline` instead of being
    /// written to `new_task.expires_at`, which stays `None` regardless of the TTL value.
    #[test]
    fn detailed_task_to_new_task_never_writes_ttl_ms_into_expires_at() {
        let mut source = common();
        source.ttl_ms = Some(3_600_000); // one hour
        let detailed = DetailedTask::Completed {
            common: source.clone(),
            result: Map::from_iter([("ok".to_owned(), serde_json::json!(true))]),
        };
        let conversion = detailed_task_to_new_task(&detailed, sample_inputs()).unwrap();
        assert_eq!(
            conversion.new_task.expires_at, None,
            "ttlMs (retention) must never populate expires_at (lifecycle)"
        );
        let expected_deadline = ttl_ms_to_deadline(source.created_at, source.ttl_ms).unwrap();
        assert_eq!(conversion.provenance.retention_deadline, expected_deadline);
    }

    /// Finding D/E regression: the caller must be able to recover the inbound wire identity and
    /// source timestamps from `NewTaskConversion`, since `NewTask` has no columns for them.
    #[test]
    fn detailed_task_to_new_task_surfaces_provenance_for_the_caller_to_persist() {
        let source = common();
        let detailed = DetailedTask::Working(source.clone());
        let conversion = detailed_task_to_new_task(&detailed, sample_inputs()).unwrap();
        assert_eq!(conversion.provenance.source_task_id, source.task_id);
        assert_eq!(conversion.provenance.source_created_at, source.created_at);
        assert_eq!(conversion.provenance.source_last_updated_at, source.last_updated_at);
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
            meta: None,
            result_type: TaskResultType,
        };
        let conversion = create_task_result_to_new_task(&result, sample_inputs()).unwrap();
        assert_eq!(conversion.target_state.value, TaskState::Running);
        assert_eq!(conversion.new_task.expires_at, None);
        assert_eq!(conversion.provenance.source_task_id, "task_1");
        assert_eq!(conversion.provenance.retention_deadline, None);
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
            Some(Map::from_iter([("ok".to_owned(), serde_json::json!(true))])),
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

    /// Finding C regression: `CompletedTask.result` is typed as a JSON object
    /// (`serde_json::Map<String, Value>`), so a non-object payload (`null`, a scalar, or an
    /// array) must be rejected on decode rather than silently accepted.
    #[test]
    fn decoding_completed_result_as_a_non_object_is_rejected() {
        for non_object in [
            serde_json::json!(null),
            serde_json::json!("a string"),
            serde_json::json!(42),
            serde_json::json!([1, 2, 3]),
        ] {
            let raw = serde_json::json!({
                "taskId": "task_1",
                "status": "completed",
                "createdAt": "2026-01-01T00:00:00Z",
                "lastUpdatedAt": "2026-01-01T00:00:00Z",
                "ttlMs": null,
                "result": non_object,
            });
            serde_json::from_value::<DetailedTask>(raw)
                .expect_err("a non-object `result` must be rejected on decode");
        }
    }

    /// Finding C regression, positive case: a genuine JSON object round-trips through
    /// `DetailedTask::Completed` unchanged.
    #[test]
    fn completed_result_object_round_trips_through_json() {
        let result = Map::from_iter([
            ("summary".to_owned(), serde_json::json!("done")),
            ("count".to_owned(), serde_json::json!(3)),
        ]);
        let detailed = DetailedTask::Completed { common: common(), result: result.clone() };
        let json = serde_json::to_string(&detailed).unwrap();
        let decoded: DetailedTask = serde_json::from_str(&json).unwrap();
        match decoded {
            DetailedTask::Completed { result: decoded_result, .. } => {
                assert_eq!(decoded_result, result);
            }
            other => panic!("expected Completed, got {other:?}"),
        }
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
        let mapped = task_to_create_task_result(&task, None, None, None);
        assert_eq!(mapped.value.status, McpTaskStatus::Failed);
        let coercion = mapped.coercion.expect("Expired must be reported as a coercion");
        assert_eq!(coercion.from, "expired");
        assert_eq!(coercion.to, "failed");
    }

    /// [`task_to_create_task_result`] threads `meta` straight through to
    /// `CreateTaskResult::meta` -- it is caller-supplied opaque data, not something this crate
    /// computes.
    #[test]
    fn task_to_create_task_result_threads_meta_through() {
        let task = sample_task(TaskState::Running);
        let meta = serde_json::json!({"custom.example/traceId": "abc123"});
        let mapped = task_to_create_task_result(&task, None, None, Some(meta.clone()));
        assert_eq!(mapped.value.meta, Some(meta));
    }
}

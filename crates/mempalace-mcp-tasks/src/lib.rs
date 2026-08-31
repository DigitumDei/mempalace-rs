//! Translation library between the
//! [MCP Tasks extension](https://github.com/modelcontextprotocol/ext-tasks)
//! (`io.modelcontextprotocol/tasks`, 2026-07-28 specification) and MemPalace's local coordination
//! storage.
//!
//! # Not `mempalace-mcp`
//!
//! This crate's name sits close to [`mempalace-mcp`](../mempalace_mcp/index.html), MemPalace's
//! own MCP **server** binary (agent-facing tools like `mempalace_task_create`). They are
//! unrelated: `mempalace-mcp-tasks` is a protocol *adapter* — a pure translation library, exactly
//! like `mempalace-a2a` is for the A2A protocol — with no server, no tool registrations, and no
//! transport of its own. It translates the wire shapes a third-party MCP client/server pair
//! speaks under the `io.modelcontextprotocol/tasks` extension into MemPalace's
//! `coordination_tasks` model, and back. Whether this adapter gets wired into an actual MCP
//! transport is a separate question this crate does not answer.
//!
//! # Isolation rule
//!
//! No MCP Tasks field may become a column in MemPalace's core schema. Fields with no internal
//! home are preserved by storing the inbound envelope verbatim — the exact JSON text as received
//! on the wire, not a re-serialization of a parsed value (see [`envelope::envelope_artifact`] for
//! why that distinction matters) — as an immutable `role = "protocol_envelope"` artifact (see
//! [`envelope`]). This is the same mechanism `mempalace-a2a` uses, with a distinct `media_type` so
//! the two adapters' envelope artifacts stay distinguishable.
//!
//! # Status mapping
//!
//! MCP Tasks defines five task statuses against MemPalace's seven, so the mapping is not
//! bijective in the MemPalace-to-MCP direction (it *is* total in the MCP-to-MemPalace direction —
//! MCP Tasks has no equivalent of A2A's `UNSPECIFIED`). See [`status`] for the mapping tables and
//! the [`Mapped`]/[`Coercion`] types that report *whether* a coercion happened, never silently.
//!
//! # `Task` is a discriminated union
//!
//! Unlike A2A's single `Task` message, the MCP Tasks extension's `Task` type is a discriminated
//! union of five variants keyed on `status`. See [`detailed_task`] for why [`DetailedTask`] is
//! modeled as a Rust enum rather than a flat struct with optional fields, and for
//! [`CreateTaskResult`], the immediate task-handle shape the extension defines separately.
//!
//! # Module map
//!
//! - [`status`] — [`McpTaskStatus`] and the inbound/outbound status mappings.
//! - [`json_rpc`] — [`JsonRpcErrorObject`] and the extension's named JSON-RPC error codes.
//! - [`ttl`] — `ttlMs` ↔ `expires_at` conversion.
//! - [`detailed_task`] — [`DetailedTask`]/[`CreateTaskResult`] ↔ `coordination_tasks`.
//! - [`envelope`] — the envelope-as-artifact isolation mechanism for the whole inbound task.

pub mod detailed_task;
pub mod envelope;
pub mod json_rpc;
pub mod status;
pub mod ttl;

mod error;

pub use detailed_task::{
    CreateTaskResult, DetailedTask, DetailedTaskCommon, NewTaskConversion, NewTaskInputs,
    TaskResultType, create_task_result_to_new_task, detailed_task_to_new_task,
    task_to_create_task_result, task_to_detailed_task,
};
pub use envelope::{PROTOCOL_ENVELOPE_MEDIA_TYPE, PROTOCOL_ENVELOPE_ROLE, envelope_artifact};
pub use error::McpTasksError;
pub use json_rpc::{INTERNAL_ERROR, INVALID_TASK_ID, JsonRpcErrorObject, MISSING_CLIENT_CAPABILITY};
pub use status::{Coercion, Mapped, McpTaskStatus, map_inbound_task_status, map_outbound_task_state};
pub use ttl::{expires_at_to_ttl_ms, ttl_ms_to_expires_at};

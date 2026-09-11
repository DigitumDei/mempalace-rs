//! JSON-RPC error shapes used by the MCP Tasks extension.
//!
//! Verified against `specification/2026-07-28/tasks.md` in `modelcontextprotocol/ext-tasks`:
//!
//! - `-32021` — "Missing Required Client Capability": returned when a client lacks the
//!   `io.modelcontextprotocol/tasks` capability but attempts a task operation or subscribes to
//!   task notifications. **Not** `-32003` — an earlier draft of
//!   `docs/Coordination-Phase-3-Design.md` (Stage 6) cited `-32003`, which appears nowhere in the
//!   published extension; see deviation entry recorded there for Stage 6.
//! - `-32602` — "Invalid params": mandatory for `tasks/get` when `taskId` is invalid or does not
//!   exist, and recommended for `tasks/update`/`tasks/cancel` for the same reason. This is the
//!   standard JSON-RPC 2.0 "Invalid params" code, not an extension-specific allocation.
//! - `-32603` — "Internal error": server-side processing failures unrelated to client capability
//!   or parameters. Also a standard JSON-RPC 2.0 code.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// JSON-RPC error code for a client that lacks the `io.modelcontextprotocol/tasks` capability.
/// See the module docs for the citation and the `-32003` correction.
pub const MISSING_CLIENT_CAPABILITY: i64 = -32021;
/// Standard JSON-RPC 2.0 "Invalid params" code. Mandatory for `tasks/get` on an invalid or
/// nonexistent `taskId`; recommended for `tasks/update`/`tasks/cancel`.
pub const INVALID_TASK_ID: i64 = -32602;
/// Standard JSON-RPC 2.0 "Internal error" code, for server-side failures unrelated to client
/// capability or parameters.
pub const INTERNAL_ERROR: i64 = -32603;

/// Wire mirror of a JSON-RPC 2.0 error object, as carried by [`crate::detailed_task::DetailedTask::Failed`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcErrorObject {
    /// The JSON-RPC error code. See [`MISSING_CLIENT_CAPABILITY`], [`INVALID_TASK_ID`],
    /// [`INTERNAL_ERROR`] for the codes this extension names explicitly; other codes are legal
    /// and are passed through unmodified.
    pub code: i64,
    /// Human-readable error message.
    pub message: String,
    /// Optional additional error data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn error_codes_match_the_spec_citation() {
        assert_eq!(MISSING_CLIENT_CAPABILITY, -32021);
        assert_eq!(INVALID_TASK_ID, -32602);
        assert_eq!(INTERNAL_ERROR, -32603);
    }

    #[test]
    fn json_rpc_error_object_round_trips_through_json() {
        let error = JsonRpcErrorObject {
            code: MISSING_CLIENT_CAPABILITY,
            message: "client does not support io.modelcontextprotocol/tasks".to_owned(),
            data: Some(serde_json::json!({"requiredCapability": "io.modelcontextprotocol/tasks"})),
        };
        let json = serde_json::to_string(&error).unwrap();
        let decoded: JsonRpcErrorObject = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, error);
    }

    #[test]
    fn json_rpc_error_object_omits_absent_data() {
        let error =
            JsonRpcErrorObject { code: INTERNAL_ERROR, message: "boom".to_owned(), data: None };
        let json = serde_json::to_string(&error).unwrap();
        assert!(!json.contains("data"));
    }
}

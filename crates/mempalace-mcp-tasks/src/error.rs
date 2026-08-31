//! Error type for MCP Tasks translation failures.

/// Errors raised while translating between MCP Tasks wire shapes and MemPalace storage types.
#[derive(Debug, thiserror::Error)]
pub enum McpTasksError {
    /// A [`crate::detailed_task::DetailedTask`] wire object was missing a field its `status`
    /// requires — e.g. `status: "completed"` with no `result`, or `status: "failed"` with no
    /// `error`.
    #[error("MCP Tasks `{status}` task must carry `{field}`, but it was absent")]
    MissingField {
        /// The status value that requires `field` (e.g. `"completed"`).
        status: &'static str,
        /// The field that `status` requires but that was absent (e.g. `"result"`).
        field: &'static str,
    },
    /// A [`crate::detailed_task::DetailedTask`] wire object carried a field that belongs to a
    /// different `status` variant — e.g. `status: "working"` with a `result` or `error` present.
    /// The extension's discriminated union forbids this exactly as much as it forbids the
    /// missing-field case above: each status has exactly one legal shape.
    #[error("MCP Tasks `{status}` task must not carry `{field}`, but it was present")]
    UnexpectedField {
        /// The status value that forbids `field` (e.g. `"working"`).
        status: &'static str,
        /// The field that is not legal for `status` but was present anyway (e.g. `"result"`).
        field: &'static str,
    },
    /// [`crate::ttl::ttl_ms_to_deadline`] computed `created_at + ttlMs`, but the result falls
    /// outside the range [`time::OffsetDateTime`] can represent. Mirrors
    /// `mempalace_storage::coordination::LEASE_DURATION_OUT_OF_RANGE`'s handling of the same
    /// class of overflow for lease durations.
    #[error("computed retention deadline from ttlMs overflows the representable timestamp range")]
    TtlOutOfRange,
}

//! Error type for A2A translation failures.

/// Errors raised while translating between A2A wire shapes and MemPalace storage types.
#[derive(Debug, thiserror::Error)]
pub enum A2aError {
    /// The A2A task carried `UNSPECIFIED`, which the design explicitly forbids coercing into
    /// any MemPalace state (see `docs/Coordination-Phase-3-Design.md`, Stage 5). This is the
    /// caller's malformed input, not a mapping the adapter can complete.
    #[error(
        "A2A task state `UNSPECIFIED` is malformed and cannot be mapped to a MemPalace task state"
    )]
    UnspecifiedTaskState,
    /// Serializing or deserializing an A2A JSON payload failed.
    #[error("A2A payload (de)serialization failed: {0}")]
    Serde(#[from] serde_json::Error),
    /// A stored payload/content was not valid JSON for the A2A shape being decoded, or was
    /// missing a field required by that shape.
    #[error("stored record is not a valid A2A {shape}: {detail}")]
    InvalidStoredShape {
        /// The A2A shape that failed to decode (e.g. `"Message"`, `"Artifact"`).
        shape: &'static str,
        /// Human-readable detail of what was wrong.
        detail: String,
    },
}

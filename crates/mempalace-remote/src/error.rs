//! Error types for remote palace operations.

/// Convenience alias for fallible remote operations.
pub type Result<T> = std::result::Result<T, RemoteError>;

/// Errors produced by a remote palace client, classified so the routing
/// layer can distinguish "degrade gracefully" from "report misconfiguration".
#[derive(Debug, thiserror::Error)]
pub enum RemoteError {
    /// The remote could not be reached (connect/DNS/timeout).
    #[error("remote `{remote}` is unreachable: {message}")]
    Unreachable {
        /// Name of the remote palace.
        remote: String,
        /// Underlying error description.
        message: String,
    },
    /// The remote rejected our bearer token (HTTP 401).
    #[error("remote `{remote}` rejected credentials (HTTP 401)")]
    Unauthorized {
        /// Name of the remote palace.
        remote: String,
    },
    /// The remote speaks an incompatible federation API version.
    #[error("remote `{remote}` speaks federation api v{theirs}, this client speaks v{ours}")]
    VersionSkew {
        /// Name of the remote palace.
        remote: String,
        /// The API version this client implements.
        ours: u32,
        /// The API version the server reported.
        theirs: u32,
    },
    /// The remote understood the request and rejected it (4xx/5xx other than 401).
    #[error("remote `{remote}` rejected the request: HTTP {status}: {body}")]
    RemoteRejected {
        /// Name of the remote palace.
        remote: String,
        /// HTTP status code.
        status: u16,
        /// Response body (possibly truncated).
        body: String,
    },
    /// The remote returned a 2xx whose body could not be decoded.
    #[error("remote `{remote}` returned a malformed response: {message}")]
    InvalidResponse {
        /// Name of the remote palace.
        remote: String,
        /// Description of the decoding failure.
        message: String,
    },
    /// The endpoint definition itself is invalid (bad URL, client build failure).
    #[error("invalid configuration for remote `{remote}`: {message}")]
    InvalidConfig {
        /// Name of the remote palace.
        remote: String,
        /// Description of the configuration problem.
        message: String,
    },
    /// The remote does not advertise a capability this call requires (e.g. `"coordination"`,
    /// from `GET /v1/info`'s `capabilities` list). Distinct from `RemoteRejected` so a caller
    /// can tell "the peer doesn't support this feature at all" apart from "the peer rejected
    /// this specific request" — and distinct from a raw HTTP 404, which would otherwise look
    /// identical to "the requested record does not exist."
    #[error("remote `{remote}` does not support the `{capability}` capability")]
    CapabilityMissing {
        /// Name of the remote palace.
        remote: String,
        /// The capability string this call required.
        capability: String,
    },
}

impl RemoteError {
    /// Returns `true` when the router should degrade gracefully (skip this
    /// remote and serve local results) rather than surface a misconfiguration.
    pub fn is_degradable(&self) -> bool {
        matches!(self, Self::Unreachable { .. })
    }
}

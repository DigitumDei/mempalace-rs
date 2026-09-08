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
    /// A mutation whose outcome the client cannot confirm.
    ///
    /// The request may have reached the remote and committed before the
    /// connection timed out, was dropped mid-response, or returned a response
    /// the client could not decode. This is **not** an authoritative failure:
    /// the caller must not surface it as "the write failed" — it must be
    /// retried (durably, carrying its stable `mempalace_federation` operation
    /// id) on the assumption the write may already have been applied.
    #[error("remote `{remote}` outcome could not be confirmed: {message}")]
    UnknownOutcome {
        /// Name of the remote palace.
        remote: String,
        /// Description of the transport/decoding failure.
        message: String,
    },
}

/// Returns `true` for HTTP status codes that mean "the remote was reachable and
/// understood the request, but is transiently unable to apply it right now" —
/// the codes an outbox worker should retry (with the same operation id) rather
/// than treat as permanent failures.
///
/// Transient: `408 Request Timeout`, `425 Too Early`, `429 Too Many Requests`,
/// and every `5xx`. Ordinary `4xx` (`400`, `403`, `404`, `409`, `422`, ...) are
/// authoritative rejections — retrying will not change them.
pub fn is_transient_http_status(status: u16) -> bool {
    matches!(status, 408 | 425 | 429 | 500..=599)
}

impl RemoteError {
    /// Returns `true` when the router should degrade gracefully (skip this
    /// remote and serve local results) rather than surface a misconfiguration.
    ///
    /// Read calls that fail before a request is delivered are degradable.
    pub fn is_degradable(&self) -> bool {
        matches!(self, Self::Unreachable { .. })
    }

    /// Returns `true` when the request was **definitely never sent** to the
    /// remote: a connect/DNS/build failure, or a handshake that failed before a
    /// mutation was sent. Retrying is safe and needs no idempotency identity.
    ///
    /// This is the complement of [`Self::is_unknown_outcome`] for mutations;
    /// for reads it is equivalent to [`Self::is_degradable`].
    pub fn is_unreachable_before_send(&self) -> bool {
        matches!(self, Self::Unreachable { .. })
    }

    /// Returns `true` when this is a mutation that may have been applied
    /// remotely but whose outcome is unconfirmed. A retry is appropriate but
    /// **must** carry a stable operation/idempotency id so a replayed mutation
    /// does not double-apply on the server.
    pub fn is_unknown_outcome(&self) -> bool {
        matches!(self, Self::UnknownOutcome { .. })
    }

    /// Returns `true` when an outbox worker should retry the operation, using
    /// the same operation id as the original attempt:
    /// - the request was definitely not sent (safe retry), or
    /// - it may have been applied but went unconfirmed, or
    /// - the remote returned a transient rejection ([`is_transient_http_status`]).
    ///
    /// `Unauthorized`, `VersionSkew`, `CapabilityMissing`, `InvalidConfig`,
    /// `InvalidResponse`, and ordinary (authoritative) `RemoteRejected` statuses
    /// are not retryable.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Unreachable { .. } | Self::UnknownOutcome { .. } => true,
            Self::RemoteRejected { status, .. } => is_transient_http_status(*status),
            _ => false,
        }
    }

    /// Returns `true` when the outcome is authoritative (the server responded)
    /// or the client/configuration is broken, so retrying as-is will not change
    /// it. The complement of [`Self::is_retryable`].
    pub fn is_terminal(&self) -> bool {
        !self.is_retryable()
    }
}

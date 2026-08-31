//! `Part` and `Role`, shared building blocks of A2A `Message` and `Artifact`.
//!
//! Verified against the A2A v1.0.1 proto (`specification/a2a.proto` in `a2aproject/A2A` at tag
//! `v1.0.1`): `Message` (lines ~256-269), `Part` (lines ~220-238), `Role` (lines ~241-248), and
//! `Artifact` (lines ~274-286). Field names are rendered camelCase here to match protojson's
//! default mapping (confirmed against the worked examples in `docs/specification.md`, e.g.
//! `"taskId"`, `"contextId"`).

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Sender role of an A2A [`crate::message::A2aMessage`].
///
/// Wire strings carry the `ROLE_` prefix, matching the A2A v1.0.1 proto's `enum Role`
/// (`ROLE_UNSPECIFIED`, `ROLE_USER`, `ROLE_AGENT` — `specification/a2a.proto` in
/// `a2aproject/A2A` at tag `v1.0.1`). Proto3 JSON encoding serializes enums by their full value
/// name, so the prefixed form is what appears on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum A2aRole {
    /// Unspecified role.
    #[serde(rename = "ROLE_UNSPECIFIED")]
    Unspecified,
    /// The message is from the client to the server.
    #[serde(rename = "ROLE_USER")]
    User,
    /// The message is from the server to the client.
    #[serde(rename = "ROLE_AGENT")]
    Agent,
}

/// One content part of an A2A `Message` or `Artifact`.
///
/// The proto models this as a `oneof` over `text` / `raw` / `url` / `data`; protojson flattens
/// a `oneof` to plain optional fields on the containing message rather than wrapping it, so
/// this struct mirrors that shape directly instead of using a Rust `enum` (which would
/// serialize as a wrapper object and not round-trip against real A2A JSON).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct A2aPart {
    /// Text content, when this part is textual.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Base64-encoded raw byte content, when this part is an inline file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw: Option<String>,
    /// A URL pointing to the file's content, when this part is a file reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Arbitrary structured data, when this part is a JSON value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    /// Optional metadata associated with this part.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    /// Optional filename, for a file part.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    /// MIME type of the part content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn role_wire_strings_match_proto_enum_names() {
        assert_eq!(serde_json::to_string(&A2aRole::Unspecified).unwrap(), "\"ROLE_UNSPECIFIED\"");
        assert_eq!(serde_json::to_string(&A2aRole::User).unwrap(), "\"ROLE_USER\"");
        assert_eq!(serde_json::to_string(&A2aRole::Agent).unwrap(), "\"ROLE_AGENT\"");
    }

    #[test]
    fn part_round_trips_text_variant() {
        let part = A2aPart { text: Some("hello".to_owned()), ..Default::default() };
        let json = serde_json::to_string(&part).unwrap();
        assert_eq!(json, r#"{"text":"hello"}"#);
        let decoded: A2aPart = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, part);
    }
}

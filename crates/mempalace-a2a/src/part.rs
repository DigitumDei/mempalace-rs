//! `Part` and `Role`, shared building blocks of A2A `Message` and `Artifact`.
//!
//! Verified against the A2A v1.0.1 proto (`specification/a2a.proto` in `a2aproject/A2A` at tag
//! `v1.0.1`): `Message` (lines ~256-269), `Part` (lines ~220-238), `Role` (lines ~241-248), and
//! `Artifact` (lines ~274-286). Field names are rendered camelCase here to match protojson's
//! default mapping (confirmed against the worked examples in `docs/specification.md`, e.g.
//! `"taskId"`, `"contextId"`).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::A2aError;

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
///
/// # The `oneof` invariant is not enforced by the type
///
/// Because the four fields are plain independent `Option`s, nothing about this struct's shape
/// stops a caller (or `serde`) from constructing or decoding one with zero fields set, or with
/// more than one set — either of which violates the proto's `oneof content { text; raw; url;
/// data; }`. [`A2aPart::validate`] checks the invariant explicitly; call it (or let it be called
/// for you, as [`crate::message::a2a_message_to_new_message`],
/// [`crate::message::message_to_a2a_message`], [`crate::artifact::a2a_artifact_to_new_artifact`],
/// and [`crate::artifact::artifact_to_a2a_artifact`] all do internally) before trusting or
/// emitting a `Part`. A struct with two fields set would serialize to JSON a conforming A2A
/// implementation is required to reject, and one with zero fields set carries no content at all.
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

impl A2aPart {
    /// Check the A2A `Part.content` `oneof` invariant: exactly one of `text`, `raw`, `url`,
    /// `data` must be set.
    ///
    /// Returns [`A2aError::InvalidPart`] if zero, or more than one, of the four are set.
    pub fn validate(&self) -> Result<(), A2aError> {
        let set_count = usize::from(self.text.is_some())
            + usize::from(self.raw.is_some())
            + usize::from(self.url.is_some())
            + usize::from(self.data.is_some());
        if set_count == 1 { Ok(()) } else { Err(A2aError::InvalidPart { set_count }) }
    }
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

    #[test]
    fn validate_accepts_exactly_one_field_set_for_each_variant() {
        assert!(A2aPart { text: Some("a".to_owned()), ..Default::default() }.validate().is_ok());
        assert!(A2aPart { raw: Some("YQ==".to_owned()), ..Default::default() }.validate().is_ok());
        assert!(
            A2aPart { url: Some("https://example.com/f".to_owned()), ..Default::default() }
                .validate()
                .is_ok()
        );
        assert!(
            A2aPart { data: Some(serde_json::json!({"k": "v"})), ..Default::default() }
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn validate_rejects_zero_fields_set() {
        let err = A2aPart::default().validate().expect_err("no content field set is invalid");
        assert!(matches!(err, A2aError::InvalidPart { set_count: 0 }));
    }

    #[test]
    fn validate_rejects_more_than_one_field_set() {
        let part = A2aPart {
            text: Some("a".to_owned()),
            raw: Some("YQ==".to_owned()),
            ..Default::default()
        };
        let err = part.validate().expect_err("two content fields set is invalid");
        assert!(matches!(err, A2aError::InvalidPart { set_count: 2 }));
    }

    #[test]
    fn validate_rejects_all_four_fields_set() {
        let part = A2aPart {
            text: Some("a".to_owned()),
            raw: Some("YQ==".to_owned()),
            url: Some("https://example.com/f".to_owned()),
            data: Some(serde_json::json!(1)),
            ..Default::default()
        };
        let err = part.validate().expect_err("all four content fields set is invalid");
        assert!(matches!(err, A2aError::InvalidPart { set_count: 4 }));
    }
}

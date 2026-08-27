use std::fmt::{Display, Formatter};
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Error returned when a MemPalace identifier is malformed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IdError {
    #[error("{kind} id cannot be empty")]
    Empty { kind: &'static str },
    #[error("{kind} id contains invalid character `{ch}`")]
    InvalidCharacter { kind: &'static str, ch: char },
}

macro_rules! define_id {
    ($name:ident, $kind:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, IdError> {
                let value = value.into();
                validate_id($kind, &value)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::new(s)
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = IdError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }
    };
}

fn validate_id(kind: &'static str, value: &str) -> Result<(), IdError> {
    if value.is_empty() {
        return Err(IdError::Empty { kind });
    }

    for ch in value.chars() {
        if !(ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '/' | '.')) {
            return Err(IdError::InvalidCharacter { kind, ch });
        }
    }

    Ok(())
}

define_id!(WingId, "wing");
define_id!(RoomId, "room");
define_id!(DrawerId, "drawer");

/// Prefix every fully-qualified wing id carries. Exposed so callers that need
/// to recognise an already-prefixed id (rather than construct one) — e.g.
/// `mempalace-server`'s `normalize_scope_wing` — check against the real
/// constant instead of hardcoding `"wing_"`.
pub const WING_PREFIX: &str = "wing_";

impl WingId {
    /// Build a `WingId` from arbitrary input by canonicalizing it.
    ///
    /// Trims, lowercases, replaces whitespace and other disallowed characters
    /// with `_`, then prepends `wing_` if not already present. This is the
    /// constructor input-side callers (MCP tool handlers, CLI `--wing`
    /// override) should use so that `"llm"` and `"wing_llm"` resolve to the
    /// same wing instead of forking into a ghost.
    pub fn normalized(value: &str) -> Result<Self, IdError> {
        let canonical: String = value
            .trim()
            .to_ascii_lowercase()
            .chars()
            .map(|ch| match ch {
                'a'..='z' | '0'..='9' | '_' | '-' | '/' | '.' => ch,
                _ => '_',
            })
            .collect();

        if canonical.is_empty() {
            return Err(IdError::Empty { kind: "wing" });
        }

        let prefixed = if canonical.starts_with(WING_PREFIX) {
            canonical
        } else {
            format!("{WING_PREFIX}{canonical}")
        };

        Self::new(prefixed)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{DrawerId, RoomId, WingId};

    #[test]
    fn ids_serialize_as_plain_strings() {
        let wing = WingId::new("project_alpha").unwrap();
        let room = RoomId::new("backend.auth").unwrap();
        let drawer = DrawerId::new("project_alpha/backend/0001").unwrap();

        assert_eq!(serde_json::to_string(&wing).unwrap(), "\"project_alpha\"");
        assert_eq!(serde_json::to_string(&room).unwrap(), "\"backend.auth\"");
        assert_eq!(serde_json::to_string(&drawer).unwrap(), "\"project_alpha/backend/0001\"");
    }

    #[test]
    fn ids_reject_invalid_characters() {
        assert!(WingId::new("bad wing").is_err());
        assert!(RoomId::new("backend:auth").is_err());
        assert!(DrawerId::new("").is_err());
    }

    #[test]
    fn wing_id_normalized_prepends_prefix() {
        assert_eq!(WingId::normalized("llm").unwrap().as_str(), "wing_llm");
    }

    #[test]
    fn wing_id_normalized_keeps_existing_prefix() {
        assert_eq!(WingId::normalized("wing_llm").unwrap().as_str(), "wing_llm");
    }

    #[test]
    fn wing_id_normalized_lowercases() {
        assert_eq!(WingId::normalized("Wing_LLM").unwrap().as_str(), "wing_llm");
        assert_eq!(WingId::normalized("LLM").unwrap().as_str(), "wing_llm");
    }

    #[test]
    fn wing_id_normalized_replaces_invalid_chars() {
        assert_eq!(WingId::normalized("my project").unwrap().as_str(), "wing_my_project");
        assert_eq!(WingId::normalized("foo:bar").unwrap().as_str(), "wing_foo_bar");
    }

    #[test]
    fn wing_id_normalized_is_idempotent() {
        let once = WingId::normalized("Some Wing").unwrap();
        let twice = WingId::normalized(once.as_str()).unwrap();
        assert_eq!(once, twice);
    }

    #[test]
    fn wing_id_normalized_rejects_empty() {
        assert!(WingId::normalized("").is_err());
        assert!(WingId::normalized("   ").is_err());
    }
}

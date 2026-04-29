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
}

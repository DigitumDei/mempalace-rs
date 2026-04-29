use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::{MempalaceError, Result};

/// Pinned embedding profile names used throughout the Rust workspace.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingProfile {
    /// Default profile aligned with Python-era retrieval expectations.
    #[default]
    Balanced,
    /// Lower-cost profile for constrained machines.
    LowCpu,
}

impl EmbeddingProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Balanced => "balanced",
            Self::LowCpu => "low_cpu",
        }
    }

    pub fn metadata(self) -> &'static EmbeddingProfileMetadata {
        match self {
            Self::Balanced => &BALANCED_PROFILE,
            Self::LowCpu => &LOW_CPU_PROFILE,
        }
    }
}

impl FromStr for EmbeddingProfile {
    type Err = MempalaceError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "balanced" => Ok(Self::Balanced),
            "low_cpu" => Ok(Self::LowCpu),
            other => Err(MempalaceError::UnknownEmbeddingProfile(other.to_owned())),
        }
    }
}

/// Metadata that downstream storage and embedding crates can depend on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddingProfileMetadata {
    pub profile: EmbeddingProfile,
    pub model_id: &'static str,
    pub dimensions: usize,
}

pub const BALANCED_PROFILE: EmbeddingProfileMetadata = EmbeddingProfileMetadata {
    profile: EmbeddingProfile::Balanced,
    model_id: "sentence-transformers/all-MiniLM-L6-v2",
    dimensions: 384,
};

pub const LOW_CPU_PROFILE: EmbeddingProfileMetadata = EmbeddingProfileMetadata {
    profile: EmbeddingProfile::LowCpu,
    model_id: "Xenova/all-MiniLM-L6-v2",
    dimensions: 384,
};

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{BALANCED_PROFILE, EmbeddingProfile, LOW_CPU_PROFILE};

    #[test]
    fn profile_metadata_is_locked() {
        assert_eq!(
            EmbeddingProfile::Balanced.metadata().model_id,
            "sentence-transformers/all-MiniLM-L6-v2"
        );
        assert_eq!(BALANCED_PROFILE.dimensions, 384);
        assert_eq!(EmbeddingProfile::LowCpu.metadata().model_id, "Xenova/all-MiniLM-L6-v2");
        assert_eq!(LOW_CPU_PROFILE.dimensions, 384);
    }

    #[test]
    fn profile_names_round_trip() {
        assert_eq!("balanced".parse::<EmbeddingProfile>().unwrap(), EmbeddingProfile::Balanced);
        assert_eq!("low_cpu".parse::<EmbeddingProfile>().unwrap(), EmbeddingProfile::LowCpu);
    }
}

use std::path::PathBuf;

use thiserror::Error;

use crate::IdError;

/// Shared result type for workspace crates.
pub type Result<T> = std::result::Result<T, MempalaceError>;

/// Root error type for early workspace crates.
#[derive(Debug, Error)]
pub enum MempalaceError {
    #[error("invalid id: {0}")]
    InvalidId(#[from] IdError),
    #[error("unsupported config schema version: {0}")]
    UnsupportedConfigVersion(u32),
    #[error("unknown embedding profile: {0}")]
    UnknownEmbeddingProfile(String),
    #[error("missing home directory for path expansion")]
    MissingHomeDirectory,
    #[error("failed to read config at {path}: {source}")]
    ConfigRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write config at {path}: {source}")]
    ConfigWrite {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config at {path}: {message}")]
    ConfigParse { path: PathBuf, message: String },
}

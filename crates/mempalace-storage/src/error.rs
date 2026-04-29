use std::path::PathBuf;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error(transparent)]
    Core(#[from] mempalace_core::MempalaceError),
    #[error("invalid id: {0}")]
    InvalidId(#[from] mempalace_core::IdError),
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("lancedb error: {0}")]
    Lance(#[from] lancedb::error::Error),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("missing record: {entity} `{id}`")]
    MissingRecord { entity: &'static str, id: String },
    #[error("duplicate drawer ids are not allowed: {0:?}")]
    DuplicateDrawers(Vec<String>),
    #[error(
        "invalid embedding dimensions for drawer `{drawer_id}`: expected {expected}, got {actual}"
    )]
    InvalidEmbeddingDimensions { drawer_id: String, expected: usize, actual: usize },
    #[error("storage invariant violated: {0}")]
    Invariant(String),
}

#![allow(missing_docs)]
//! Core domain types and shared foundations for MemPalace Rust crates.

mod diary;
mod error;
pub mod hash;
mod ids;
pub mod locator;
mod profiles;
mod search;

pub use diary::{
    DIARY_HALL, DIARY_ROOM, DIARY_SUMMARY_MAX_CHARS, DIARY_TOPIC_PREFIX, SHARED_AGENT_DIARY_WING,
};
pub use error::{MempalaceError, Result};
pub use hash::{hash_bytes, hash_text, mined_drawer_id};
pub use ids::{DrawerId, IdError, RoomId, WING_PREFIX, WingId};
pub use locator::{ResolvedSnippet, SourceLocator, resolve_locator, resolve_records};
pub use profiles::{BALANCED_PROFILE, EmbeddingProfile, EmbeddingProfileMetadata, LOW_CPU_PROFILE};
pub use search::{DrawerRecord, RepositoryViewMetadata, SearchQuery, SearchResult};

/// Version embedded in release binaries.
///
/// Normal local builds use the workspace package version. Release builds set
/// `MEMPALACE_BUILD_VERSION` from `release/version.toml` and mainline history.
pub const BUILD_VERSION: &str = match option_env!("MEMPALACE_BUILD_VERSION") {
    Some(version) => version,
    None => env!("CARGO_PKG_VERSION"),
};

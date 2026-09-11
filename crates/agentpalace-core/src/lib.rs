#![allow(missing_docs)]
//! Core domain types and shared foundations for AgentPalace Rust crates.

mod diary;
mod error;
pub mod hash;
mod ids;
pub mod locator;
mod profiles;
mod search;

pub use diary::{
    DIARY_HALL, DIARY_ROOM, DIARY_SUMMARY_MAX_CHARS, DIARY_TOPIC_PREFIX, SHARED_AGENT_DIARY_WING,
    UNSCOPED_WING,
};
pub use error::{AgentPalaceError, Result};
pub use hash::{hash_bytes, hash_text, mined_drawer_id};
pub use ids::{DrawerId, IdError, RoomId, WING_PREFIX, WingId};
pub use locator::{ResolvedSnippet, SourceLocator, resolve_locator, resolve_records};
pub use profiles::{BALANCED_PROFILE, EmbeddingProfile, EmbeddingProfileMetadata, LOW_CPU_PROFILE};
pub use search::{
    DrawerRecord, RepositoryViewMetadata, SearchQuery, SearchResult, compare_layer_drawers,
};

/// Version embedded in release binaries.
///
/// Normal local builds use the workspace package version. Release builds set
/// `AGENTPALACE_BUILD_VERSION` from `release/version.toml` and mainline history.
pub const BUILD_VERSION: &str = match option_env!("AGENTPALACE_BUILD_VERSION") {
    Some(version) => version,
    None => env!("CARGO_PKG_VERSION"),
};

/// Read an AgentPalace setting, accepting its previous name during upgrades.
/// An explicitly set new name always takes precedence, including an empty value.
pub fn env_var_os(name: &str) -> Option<std::ffi::OsString> {
    std::env::var_os(name).or_else(|| {
        name.strip_prefix("AGENTPALACE_")
            .and_then(|suffix| std::env::var_os(format!("MEMPALACE_{suffix}")))
    })
}

/// Unicode form of [`env_var_os`], preserving environment error semantics.
pub fn env_var(name: &str) -> std::result::Result<String, std::env::VarError> {
    env_var_os(name).ok_or(std::env::VarError::NotPresent)?
        .into_string().map_err(std::env::VarError::NotUnicode)
}

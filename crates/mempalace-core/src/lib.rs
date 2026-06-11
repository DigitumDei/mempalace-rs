#![allow(missing_docs)]
//! Core domain types and shared foundations for MemPalace Rust crates.

mod diary;
mod error;
mod ids;
mod profiles;
mod search;

pub use diary::{DIARY_HALL, DIARY_ROOM, DIARY_TOPIC_PREFIX, SHARED_AGENT_DIARY_WING};
pub use error::{MempalaceError, Result};
pub use ids::{DrawerId, IdError, RoomId, WingId};
pub use profiles::{BALANCED_PROFILE, EmbeddingProfile, EmbeddingProfileMetadata, LOW_CPU_PROFILE};
pub use search::{DrawerRecord, SearchQuery, SearchResult};

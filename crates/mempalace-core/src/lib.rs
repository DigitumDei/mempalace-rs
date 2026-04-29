#![allow(missing_docs)]
//! Core domain types and shared foundations for MemPalace Rust crates.

mod error;
mod ids;
mod profiles;
mod search;

pub use error::{MempalaceError, Result};
pub use ids::{DrawerId, IdError, RoomId, WingId};
pub use profiles::{BALANCED_PROFILE, EmbeddingProfile, EmbeddingProfileMetadata, LOW_CPU_PROFILE};
pub use search::{DrawerRecord, SearchQuery, SearchResult};

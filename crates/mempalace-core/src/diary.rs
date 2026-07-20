//! Constants shared by all crates that work with the diary subsystem.

/// The room name used to store diary entries.
pub const DIARY_ROOM: &str = "diary";

/// The hall name used to index diary entries.
pub const DIARY_HALL: &str = "hall_diary";

/// Prefix prepended to diary topic tags when stored as a `source_file`.
pub const DIARY_TOPIC_PREFIX: &str = "diary:";

/// The wing shared by all agent-scoped diary entries.
pub const SHARED_AGENT_DIARY_WING: &str = "wing_agents";

/// Maximum allowed length (in Unicode scalar values) for a diary summary.
pub const DIARY_SUMMARY_MAX_CHARS: usize = 400;

#![allow(missing_docs)]
//! Versioned config schema and path resolution for MemPalace.

mod config;

pub use config::{
    ConfigFileV1, ConfigLoader, DEFAULT_BASE_DIR, DEFAULT_COLLECTION_NAME, LowCpuConfigFileV1,
    LowCpuRuntimeConfig, MempalaceConfig, ProjectConfig, ProjectRoomConfig, ResolvedPaths,
    build_runtime,
};

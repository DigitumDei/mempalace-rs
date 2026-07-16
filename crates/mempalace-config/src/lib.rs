#![allow(missing_docs)]
//! Versioned config schema and path resolution for MemPalace.

pub mod federation;
mod config;

pub use config::{
    ConfigFileV1, ConfigLoader, DEFAULT_BASE_DIR, DEFAULT_COLLECTION_NAME, LowCpuConfigFileV1,
    LowCpuRuntimeConfig, MempalaceConfig, ProjectConfig, ProjectRoomConfig, ResolvedPaths,
    ServerConfigFileV1, ServerRuntimeConfig, build_runtime,
};
pub use federation::{
    FederationConfigV1, FederationRuntimeConfig, ProjectRoutingConfig, RemoteConfigV1,
    ReplicationStatus, ResolvedRemote, ResolvedRouteRule, RouteMode, RouteQuery, RouteRuleV1,
    WriteTarget, resolve_kg_route, resolve_route,
};

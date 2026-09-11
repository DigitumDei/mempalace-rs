#![allow(missing_docs)]
//! Versioned config schema and path resolution for AgentPalace.

mod config;
pub mod federation;

pub use config::{
    ConfigFileV1, ConfigLoader, CoordinationConfigV1, DEFAULT_BASE_DIR, DEFAULT_COLLECTION_NAME, LowCpuConfigFileV1,
    LowCpuRuntimeConfig, MaintenanceConfigFileV1, MaintenanceRuntimeConfig, AgentPalaceConfig,
    ProjectConfig, ProjectRegistryEntryV1, ProjectRegistryFileV1, ProjectRoomConfig, ResolvedPaths,
    ServerConfigFileV1, ServerRuntimeConfig, build_runtime,
};
pub use federation::{
    FederationConfigV1, FederationRuntimeConfig, ProjectRoutingConfig, RemoteConfigV1,
    ReplicationStatus, ResolvedRemote, ResolvedRouteRule, RouteMode, RouteQuery, RouteRuleV1,
    WriteTarget, DEFAULT_COORDINATION_WING, resolve_coordination_route, resolve_kg_route, resolve_route,
};

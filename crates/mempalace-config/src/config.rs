use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;

use mempalace_core::{EmbeddingProfile, MempalaceError, Result};
use serde::{Deserialize, Serialize};
use tokio::runtime::{Builder, Runtime};

use crate::federation::{
    FederationConfigV1, FederationRuntimeConfig, ProjectRoutingConfig, resolve_federation_config,
};

pub const DEFAULT_BASE_DIR: &str = "~/.mempalace";
pub const DEFAULT_COLLECTION_NAME: &str = "mempalace_drawers";
const CONFIG_FILE_NAME: &str = "config.json";
const PROJECT_REGISTRY_FILE_NAME: &str = "projects.json";
const PROJECT_CONFIG_FILE_NAME: &str = "mempalace.yaml";
const LEGACY_PROJECT_CONFIG_FILE_NAME: &str = "mempal.yaml";
const DEFAULT_SERVER_BIND: &str = "127.0.0.1:8765";
const DEFAULT_SERVER_TOKEN_FILE: &str = "server_tokens.json";
const DEFAULT_LOW_CPU_WORKER_THREADS: usize = 1;
const DEFAULT_LOW_CPU_MAX_BLOCKING_THREADS: usize = 1;
const DEFAULT_LOW_CPU_QUEUE_LIMIT: usize = 32;
const DEFAULT_LOW_CPU_INGEST_BATCH_SIZE: usize = 8;
const DEFAULT_LOW_CPU_SEARCH_RESULTS_LIMIT: usize = 5;
const DEFAULT_LOW_CPU_WAKE_UP_DRAWERS_LIMIT: usize = 8;
const MAX_SEARCH_RESULTS_LIMIT: usize = 200;
const DEGRADED_LOW_CPU_QUEUE_LIMIT: usize = 8;
const DEGRADED_LOW_CPU_INGEST_BATCH_SIZE: usize = 4;
const DEGRADED_LOW_CPU_SEARCH_RESULTS_LIMIT: usize = 3;
const DEGRADED_LOW_CPU_WAKE_UP_DRAWERS_LIMIT: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedPaths {
    pub base_dir: PathBuf,
    pub palace_dir: PathBuf,
    pub config_file: PathBuf,
    pub project_registry_file: PathBuf,
    pub people_map_file: PathBuf,
}

/// File-level representation of the optional `[server]` config section.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerConfigFileV1 {
    /// Bind address for the federation HTTP server (e.g. `"127.0.0.1:8765"`).
    #[serde(default)]
    pub bind: Option<String>,
    /// Path to the bearer-token JSON file.  `~/`-prefixed strings are expanded.
    #[serde(default)]
    pub token_file: Option<String>,
    /// Map of wing name → local checkout path.  The server uses this to
    /// resolve locator rows for mined drawers pushed via the batch-ingest
    /// endpoint; absent entries produce stale-placeholder locators.
    #[serde(default)]
    pub checkouts: BTreeMap<String, PathBuf>,
}

/// Resolved runtime configuration for the federation HTTP server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerRuntimeConfig {
    /// Socket address the server will bind to.
    pub bind: SocketAddr,
    /// Resolved path to the bearer-token JSON file.
    pub token_file: PathBuf,
    /// Map of wing name → local checkout path (default: empty).
    pub checkouts: BTreeMap<String, PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LowCpuConfigFileV1 {
    #[serde(default)]
    pub worker_threads: Option<usize>,
    #[serde(default)]
    pub max_blocking_threads: Option<usize>,
    #[serde(default)]
    pub queue_limit: Option<usize>,
    #[serde(default)]
    pub ingest_batch_size: Option<usize>,
    #[serde(default)]
    pub search_results_limit: Option<usize>,
    #[serde(default)]
    pub wake_up_drawers_limit: Option<usize>,
    #[serde(default)]
    pub degraded_mode: Option<bool>,
    #[serde(default)]
    pub rerank_enabled: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LowCpuRuntimeConfig {
    pub enabled: bool,
    pub worker_threads: usize,
    pub max_blocking_threads: usize,
    pub queue_limit: usize,
    pub ingest_batch_size: usize,
    pub search_results_limit: usize,
    pub wake_up_drawers_limit: usize,
    pub degraded_mode: bool,
    pub rerank_enabled: bool,
}

impl LowCpuRuntimeConfig {
    pub fn defaults_for_profile(profile: EmbeddingProfile) -> Self {
        match profile {
            EmbeddingProfile::Balanced => Self {
                enabled: false,
                worker_threads: DEFAULT_LOW_CPU_WORKER_THREADS,
                max_blocking_threads: DEFAULT_LOW_CPU_MAX_BLOCKING_THREADS,
                queue_limit: usize::MAX,
                ingest_batch_size: usize::MAX,
                search_results_limit: usize::MAX,
                wake_up_drawers_limit: usize::MAX,
                degraded_mode: false,
                rerank_enabled: false,
            },
            EmbeddingProfile::LowCpu => Self {
                enabled: true,
                worker_threads: DEFAULT_LOW_CPU_WORKER_THREADS,
                max_blocking_threads: DEFAULT_LOW_CPU_MAX_BLOCKING_THREADS,
                queue_limit: DEFAULT_LOW_CPU_QUEUE_LIMIT,
                ingest_batch_size: DEFAULT_LOW_CPU_INGEST_BATCH_SIZE,
                search_results_limit: DEFAULT_LOW_CPU_SEARCH_RESULTS_LIMIT,
                wake_up_drawers_limit: DEFAULT_LOW_CPU_WAKE_UP_DRAWERS_LIMIT,
                degraded_mode: true,
                rerank_enabled: false,
            },
        }
    }

    fn with_overrides(
        mut self,
        overrides: Option<LowCpuConfigFileV1>,
        config_path: &Path,
    ) -> Result<Self> {
        let Some(overrides) = overrides else {
            return Ok(self);
        };

        self.worker_threads = required_positive_override(
            "low_cpu.worker_threads",
            overrides.worker_threads,
            self.worker_threads,
            config_path,
        )?;
        self.max_blocking_threads = required_positive_override(
            "low_cpu.max_blocking_threads",
            overrides.max_blocking_threads,
            self.max_blocking_threads,
            config_path,
        )?;
        self.queue_limit = optional_limit_override(overrides.queue_limit, self.queue_limit);
        self.ingest_batch_size = required_positive_override(
            "low_cpu.ingest_batch_size",
            overrides.ingest_batch_size,
            self.ingest_batch_size,
            config_path,
        )?;
        self.search_results_limit =
            optional_limit_override(overrides.search_results_limit, self.search_results_limit);
        self.wake_up_drawers_limit =
            optional_limit_override(overrides.wake_up_drawers_limit, self.wake_up_drawers_limit);
        if let Some(degraded_mode) = overrides.degraded_mode {
            self.degraded_mode = degraded_mode;
        }
        if let Some(rerank_enabled) = overrides.rerank_enabled {
            self.rerank_enabled = rerank_enabled;
        }

        Ok(self)
    }

    pub fn effective_queue_limit(&self) -> usize {
        if !self.enabled {
            return usize::MAX;
        }

        if self.degraded_mode {
            self.queue_limit.min(DEGRADED_LOW_CPU_QUEUE_LIMIT)
        } else {
            self.queue_limit
        }
    }

    pub fn effective_ingest_batch_size(&self) -> usize {
        if !self.enabled {
            return usize::MAX;
        }

        if self.degraded_mode {
            self.ingest_batch_size.min(DEGRADED_LOW_CPU_INGEST_BATCH_SIZE)
        } else {
            self.ingest_batch_size
        }
    }

    pub fn effective_search_results_limit(&self) -> usize {
        if !self.enabled {
            return MAX_SEARCH_RESULTS_LIMIT;
        }

        let base = if self.degraded_mode {
            self.search_results_limit.min(DEGRADED_LOW_CPU_SEARCH_RESULTS_LIMIT)
        } else {
            self.search_results_limit
        };
        base.min(MAX_SEARCH_RESULTS_LIMIT)
    }

    pub fn effective_wake_up_drawers_limit(&self) -> usize {
        if !self.enabled {
            return usize::MAX;
        }

        if self.degraded_mode {
            self.wake_up_drawers_limit.min(DEGRADED_LOW_CPU_WAKE_UP_DRAWERS_LIMIT)
        } else {
            self.wake_up_drawers_limit
        }
    }

    pub fn effective_rerank_enabled(&self) -> bool {
        self.enabled && !self.degraded_mode && self.rerank_enabled
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigFileV1 {
    #[serde(default = "default_version")]
    pub version: u32,
    #[serde(default)]
    pub palace_path: Option<String>,
    #[serde(default = "default_collection_name")]
    pub collection_name: String,
    #[serde(default)]
    pub embedding_profile: Option<EmbeddingProfile>,
    #[serde(default)]
    pub low_cpu: Option<LowCpuConfigFileV1>,
    #[serde(default)]
    pub server: Option<ServerConfigFileV1>,
    /// Optional federation routing section.
    #[serde(default)]
    pub federation: Option<FederationConfigV1>,
}

impl Default for ConfigFileV1 {
    fn default() -> Self {
        Self {
            version: 1,
            palace_path: Some(default_palace_path()),
            collection_name: default_collection_name(),
            embedding_profile: Some(EmbeddingProfile::Balanced),
            low_cpu: None,
            server: None,
            federation: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MempalaceConfig {
    /// Config schema version.
    pub schema_version: u32,
    /// SQLite collection table name.
    pub collection_name: String,
    /// Path to the palace directory.
    pub palace_path: PathBuf,
    /// Embedding profile in use.
    pub embedding_profile: EmbeddingProfile,
    /// Low-CPU runtime constraints.
    pub low_cpu: LowCpuRuntimeConfig,
    /// Federation HTTP server settings.
    pub server: ServerRuntimeConfig,
    /// Federation routing configuration.
    pub federation: FederationRuntimeConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectConfig {
    /// Wing name for this project.
    pub wing: String,
    /// Rooms defined within this project's wing.
    #[serde(default)]
    pub rooms: Vec<ProjectRoomConfig>,
    /// Optional per-project federation routing override.
    #[serde(default)]
    pub routing: Option<ProjectRoutingConfig>,
}

/// Central project registry stored in the local MemPalace configuration
/// directory.  The registry is deliberately separate from repository files so
/// clones and worktrees can share one project declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectRegistryFileV1 {
    #[serde(default = "default_version")]
    pub version: u32,
    #[serde(default)]
    pub projects: BTreeMap<String, ProjectRegistryEntryV1>,
}

impl Default for ProjectRegistryFileV1 {
    fn default() -> Self {
        Self { version: 1, projects: BTreeMap::new() }
    }
}

/// One centrally registered project declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectRegistryEntryV1 {
    /// Wing receiving mined project drawers.
    pub wing: String,
    /// Room rules used by project mining.
    #[serde(default)]
    pub rooms: Vec<ProjectRoomConfig>,
    /// Optional project-level federation route.
    #[serde(default)]
    pub routing: Option<ProjectRoutingConfig>,
    /// Checkout paths used as local discovery aliases.
    #[serde(default)]
    pub checkouts: Vec<PathBuf>,
    /// Optional repository-relative root for monorepo declarations.
    #[serde(default)]
    pub project_root: Option<String>,
}

impl From<ProjectConfig> for ProjectRegistryEntryV1 {
    fn from(config: ProjectConfig) -> Self {
        Self {
            wing: config.wing,
            rooms: config.rooms,
            routing: config.routing,
            checkouts: Vec::new(),
            project_root: None,
        }
    }
}

impl ProjectRegistryEntryV1 {
    fn project_config(&self) -> ProjectConfig {
        ProjectConfig {
            wing: self.wing.clone(),
            rooms: self.rooms.clone(),
            routing: self.routing.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectRoomConfig {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub keywords: Vec<String>,
}

pub struct ConfigLoader;

impl ConfigLoader {
    pub fn load_with_env(base_dir_override: Option<&Path>) -> Result<MempalaceConfig> {
        Self::load_from_sources(
            base_dir_override,
            // `MEMPAL_PALACE_PATH` is the legacy Python alias; keep it for upgrade compatibility.
            env::var("MEMPALACE_PALACE_PATH").ok().or_else(|| env::var("MEMPAL_PALACE_PATH").ok()),
            env::var("MEMPALACE_EMBEDDING_PROFILE").ok(),
        )
    }

    fn load_from_sources(
        base_dir_override: Option<&Path>,
        palace_path_override: Option<String>,
        profile_override: Option<String>,
    ) -> Result<MempalaceConfig> {
        let paths = resolve_paths(base_dir_override)?;
        let file = read_config_file(&paths.config_file)?;
        let embedding_profile = resolve_profile(profile_override, file.embedding_profile)?;

        let palace_path = palace_path_override
            .or(file.palace_path)
            .unwrap_or_else(|| paths.palace_dir.display().to_string());

        let resolved_palace = expand_path(&palace_path)?;
        let server = resolve_server_config(file.server, &paths.base_dir, &paths.config_file)?;
        let federation = resolve_federation_config(
            file.federation,
            &paths.config_file,
            |name| env::var(name).ok(),
        )?;

        Ok(MempalaceConfig {
            schema_version: file.version,
            collection_name: file.collection_name,
            palace_path: resolved_palace,
            embedding_profile,
            low_cpu: LowCpuRuntimeConfig::defaults_for_profile(embedding_profile)
                .with_overrides(file.low_cpu, &paths.config_file)?,
            server,
            federation,
        })
    }

    pub fn init_default(base_dir_override: Option<&Path>) -> Result<ResolvedPaths> {
        let paths = resolve_paths(base_dir_override)?;
        fs::create_dir_all(&paths.base_dir).map_err(|source| MempalaceError::ConfigWrite {
            path: paths.base_dir.clone(),
            source,
        })?;
        fs::create_dir_all(&paths.palace_dir).map_err(|source| MempalaceError::ConfigWrite {
            path: paths.palace_dir.clone(),
            source,
        })?;

        if !paths.config_file.exists() {
            let default_file = ConfigFileV1 {
                palace_path: base_dir_override.map(|_| paths.palace_dir.display().to_string()),
                ..ConfigFileV1::default()
            };
            let body = serde_json::to_string_pretty(&default_file).map_err(|err| {
                MempalaceError::ConfigParse {
                    path: paths.config_file.clone(),
                    message: err.to_string(),
                }
            })?;
            fs::write(&paths.config_file, body).map_err(|source| MempalaceError::ConfigWrite {
                path: paths.config_file.clone(),
                source,
            })?;
        }

        Ok(paths)
    }

    pub fn load_project_config(path: &Path) -> Result<ProjectConfig> {
        let config_path = resolve_project_config_path(path);
        let body = fs::read_to_string(&config_path)
            .map_err(|source| MempalaceError::ConfigRead { path: config_path.clone(), source })?;
        serde_yaml::from_str(&body).map_err(|err| MempalaceError::ConfigParse {
            path: config_path,
            message: err.to_string(),
        })
    }

    /// Resolve a project declaration from an optional repository-local file,
    /// then the centralized registry, and finally derived defaults.
    ///
    /// `project_id` should be a stable repository identity such as a
    /// normalized Git origin.  `registry_base_dir` selects the local
    /// MemPalace installation whose `projects.json` should be consulted.
    pub fn resolve_project_config(
        path: &Path,
        registry_base_dir: Option<&Path>,
        project_id: Option<&str>,
        derived_wing: &str,
        derived_rooms: Vec<ProjectRoomConfig>,
    ) -> Result<ProjectConfig> {
        let local_config_path = resolve_project_config_path(path);
        if local_config_path.exists() {
            return Self::load_project_config(path);
        }

        let registry = Self::load_project_registry(registry_base_dir)?;
        let path_matches = registry
            .projects
            .iter()
            .filter(|(project_id, entry)| project_entry_matches_path(project_id, entry, path))
            .collect::<Vec<_>>();
        if path_matches.len() > 1 {
            let ids = path_matches
                .iter()
                .map(|(id, _)| id.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(MempalaceError::ConfigParse {
                path: resolve_paths(registry_base_dir)?.project_registry_file,
                message: format!("conflicting project declarations match `{}`: {ids}", path.display()),
            });
        }
        if let Some((_, entry)) = path_matches.first() {
            return Ok(entry.project_config());
        }
        if let Some(project_id) = project_id.filter(|id| project_id_hint_has_repository_identity(id)) {
            if let Some(entry) = registry.projects.get(project_id) {
                return Ok(entry.project_config());
            }
        }

        Ok(ProjectConfig {
            wing: derived_wing.to_owned(),
            rooms: if derived_rooms.is_empty() {
                vec![ProjectRoomConfig {
                    name: "general".to_owned(),
                    description: Some("All project files".to_owned()),
                    keywords: Vec::new(),
                }]
            } else {
                derived_rooms
            },
            routing: None,
        })
    }

    /// Read the centralized project registry.  A missing registry is treated
    /// as an empty registry so first-run mining can use derived defaults.
    pub fn load_project_registry(
        base_dir_override: Option<&Path>,
    ) -> Result<ProjectRegistryFileV1> {
        let paths = resolve_paths(base_dir_override)?;
        read_project_registry_file(&paths.project_registry_file)
    }

    /// Load one centralized project declaration by its durable project ID.
    /// This is used for explicit CLI selectors, which intentionally take
    /// precedence over repository-local compatibility files.
    pub fn load_project_by_id(
        base_dir_override: Option<&Path>,
        project_id: &str,
    ) -> Result<Option<ProjectConfig>> {
        let registry = Self::load_project_registry(base_dir_override)?;
        Ok(registry.projects.get(project_id).map(ProjectRegistryEntryV1::project_config))
    }

    /// Find the durable registry ID associated with a checkout.  The path is
    /// only used for discovery; callers should persist the returned ID rather
    /// than deriving an absolute-path identity.
    pub fn find_project_id(
        base_dir_override: Option<&Path>,
        path: &Path,
        project_id_hint: Option<&str>,
    ) -> Result<Option<String>> {
        let registry = Self::load_project_registry(base_dir_override)?;
        let matches = registry
            .projects
            .iter()
            .filter(|(project_id, entry)| project_entry_matches_path(project_id, entry, path))
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        if matches.len() > 1 {
            return Err(MempalaceError::ConfigParse {
                path: resolve_paths(base_dir_override)?.project_registry_file,
                message: format!(
                    "conflicting project declarations match `{}`: {}",
                    path.display(),
                    matches.join(", ")
                ),
            });
        }
        if let Some(id) = matches.into_iter().next() {
            return Ok(Some(id));
        }
        if let Some(id) = project_id_hint
            .filter(|id| project_id_hint_has_repository_identity(id))
            .filter(|id| registry.projects.contains_key(*id))
        {
            return Ok(Some(id.to_owned()));
        }
        Ok(None)
    }

    /// Register or replace one project declaration in the centralized
    /// registry.  The checkout path is stored as a discovery alias; the
    /// registry key remains the stable project identity.
    pub fn register_project(
        base_dir_override: Option<&Path>,
        project_id: &str,
        mut entry: ProjectRegistryEntryV1,
        checkout: Option<&Path>,
    ) -> Result<PathBuf> {
        let paths = resolve_paths(base_dir_override)?;
        fs::create_dir_all(&paths.base_dir).map_err(|source| MempalaceError::ConfigWrite {
            path: paths.base_dir.clone(),
            source,
        })?;

        if let Some(checkout) = checkout {
            let resolved = checkout.canonicalize().unwrap_or_else(|_| checkout.to_path_buf());
            if !entry.checkouts.iter().any(|existing| {
                existing.canonicalize().unwrap_or_else(|_| existing.to_path_buf()) == resolved
            }) {
                entry.checkouts.push(resolved);
            }
        }

        let mut registry = read_project_registry_file(&paths.project_registry_file)?;
        if let Some(checkout) = checkout {
            let resolved = checkout.canonicalize().unwrap_or_else(|_| checkout.to_path_buf());
            for (existing_id, existing) in &registry.projects {
                if existing_id != project_id
                    && existing.checkouts.iter().any(|alias| {
                        alias.canonicalize().unwrap_or_else(|_| alias.to_path_buf()) == resolved
                    })
                    && !shared_monorepo_checkout(existing_id, existing, project_id, &entry)
                {
                    return Err(MempalaceError::ConfigParse {
                        path: paths.project_registry_file,
                        message: format!(
                            "conflicting project declarations map checkout `{}` to `{existing_id}` and `{project_id}`",
                            resolved.display()
                        ),
                    });
                }
            }
        }

        if let Some(existing) = registry.projects.get(project_id).cloned() {
            if entry.wing.trim().is_empty() {
                entry.wing = existing.wing.clone();
            }
            if entry.rooms.is_empty() {
                entry.rooms = existing.rooms.clone();
            }
            if entry.routing.is_none() {
                entry.routing = existing.routing.clone();
            }
            for checkout in existing.checkouts {
                if !entry.checkouts.iter().any(|candidate| {
                    candidate.canonicalize().unwrap_or_else(|_| candidate.to_path_buf())
                        == checkout.canonicalize().unwrap_or_else(|_| checkout.to_path_buf())
                }) {
                    entry.checkouts.push(checkout);
                }
            }
            if entry.project_root.is_none() {
                entry.project_root = existing.project_root;
            }
        }
        registry.projects.insert(project_id.to_owned(), entry);
        write_project_registry_file(&paths.project_registry_file, &registry)?;
        Ok(paths.project_registry_file)
    }

    /// Remove a project declaration from the centralized registry.
    pub fn remove_project(
        base_dir_override: Option<&Path>,
        project_id: &str,
    ) -> Result<(PathBuf, bool)> {
        let paths = resolve_paths(base_dir_override)?;
        let mut registry = read_project_registry_file(&paths.project_registry_file)?;
        let removed = registry.projects.remove(project_id).is_some();
        if removed {
            write_project_registry_file(&paths.project_registry_file, &registry)?;
        }
        Ok((paths.project_registry_file, removed))
    }
}

fn resolve_paths(base_dir_override: Option<&Path>) -> Result<ResolvedPaths> {
    let base_dir = match base_dir_override {
        Some(path) => path.to_path_buf(),
        None => expand_path(DEFAULT_BASE_DIR)?,
    };

    Ok(ResolvedPaths {
        palace_dir: base_dir.join("palace"),
        config_file: base_dir.join(CONFIG_FILE_NAME),
        project_registry_file: base_dir.join(PROJECT_REGISTRY_FILE_NAME),
        people_map_file: base_dir.join("people_map.json"),
        base_dir,
    })
}

fn project_entry_matches_path(
    project_id: &str,
    entry: &ProjectRegistryEntryV1,
    path: &Path,
) -> bool {
    let canonical_path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    if entry.checkouts.iter().any(|checkout| {
        checkout.canonicalize().unwrap_or_else(|_| checkout.to_path_buf()) == canonical_path
    }) {
        return true;
    }

    let Some(project_root) = entry.project_root.as_deref().map(normalize_project_root) else {
        return false;
    };

    if let Some(repository_root) = git_repository_root(&canonical_path) {
        let repository_matches = git_remote_identity(&repository_root)
            .is_some_and(|identity| identity == project_repository_identity(project_id));
        if repository_matches {
            if let Ok(relative) = canonical_path.strip_prefix(&repository_root) {
                if normalize_project_root(&relative.to_string_lossy()) == project_root {
                    return true;
                }
            }
        }
    }

    entry.checkouts.iter().any(|checkout| {
        let canonical_checkout = checkout.canonicalize().unwrap_or_else(|_| checkout.to_path_buf());
        canonical_path
            .strip_prefix(&canonical_checkout)
            .ok()
            .map(|relative| normalize_project_root(&relative.to_string_lossy()) == project_root)
            .unwrap_or(false)
    })
}

fn project_repository_identity(project_id: &str) -> &str {
    project_id.split_once('#').map_or(project_id, |(identity, _)| identity)
}

fn project_id_hint_has_repository_identity(project_id: &str) -> bool {
    let identity = project_repository_identity(project_id);
    !identity.is_empty() && !identity.starts_with("wing:")
}

fn shared_monorepo_checkout(
    existing_id: &str,
    existing: &ProjectRegistryEntryV1,
    project_id: &str,
    entry: &ProjectRegistryEntryV1,
) -> bool {
    if project_repository_identity(existing_id) != project_repository_identity(project_id) {
        return false;
    }

    normalize_project_root(existing.project_root.as_deref().unwrap_or("."))
        != normalize_project_root(entry.project_root.as_deref().unwrap_or("."))
}

fn normalize_project_root(value: &str) -> String {
    let normalized = value.replace('\\', "/");
    let normalized = normalized.trim_matches('/').trim_start_matches("./");
    if normalized.is_empty() { ".".to_owned() } else { normalized.to_owned() }
}

fn git_repository_root(path: &Path) -> Option<PathBuf> {
    let output = Command::new("git")
        .args(["-C", &path.to_string_lossy(), "rev-parse", "--show-toplevel"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    PathBuf::from(value.trim()).canonicalize().ok()
}

fn git_remote_identity(repository_root: &Path) -> Option<String> {
    let output = Command::new("git")
        .args(["-C", &repository_root.to_string_lossy(), "remote", "get-url", "origin"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    normalize_git_remote_url(std::str::from_utf8(&output.stdout).ok()?.trim())
}

fn normalize_git_remote_url(url: &str) -> Option<String> {
    let url = url.trim().trim_end_matches('/');
    let url = url.strip_suffix(".git").unwrap_or(url).trim_end_matches('/');
    if url.is_empty() {
        return None;
    }

    let (host, path) = if let Some((_, remainder)) = url.split_once("://") {
        let authority = remainder.split('/').next()?;
        let host = authority.rsplit_once('@').map_or(authority, |(_, host)| host);
        let path = remainder.strip_prefix(authority)?.trim_start_matches('/');
        (host, path)
    } else if let Some((user_host, path)) = url.split_once(':') {
        let host = user_host.rsplit_once('@').map_or(user_host, |(_, host)| host);
        (host, path)
    } else {
        return None;
    };

    let host = host.split(':').next().unwrap_or(host).to_ascii_lowercase();
    let path = path.trim_matches('/');
    if host.is_empty() || path.is_empty() {
        None
    } else {
        Some(format!("{host}/{path}"))
    }
}

fn read_config_file(path: &Path) -> Result<ConfigFileV1> {
    if !path.exists() {
        return Ok(ConfigFileV1 { palace_path: None, ..ConfigFileV1::default() });
    }

    let body = fs::read_to_string(path)
        .map_err(|source| MempalaceError::ConfigRead { path: path.to_path_buf(), source })?;

    let file: ConfigFileV1 = serde_json::from_str(&body).map_err(|err| {
        MempalaceError::ConfigParse { path: path.to_path_buf(), message: err.to_string() }
    })?;

    if file.version != 1 {
        return Err(MempalaceError::UnsupportedConfigVersion(file.version));
    }

    Ok(file)
}

fn read_project_registry_file(path: &Path) -> Result<ProjectRegistryFileV1> {
    if !path.exists() {
        return Ok(ProjectRegistryFileV1::default());
    }

    let body = fs::read_to_string(path)
        .map_err(|source| MempalaceError::ConfigRead { path: path.to_path_buf(), source })?;
    let file: ProjectRegistryFileV1 = serde_json::from_str(&body).map_err(|err| {
        MempalaceError::ConfigParse { path: path.to_path_buf(), message: err.to_string() }
    })?;
    if file.version != 1 {
        return Err(MempalaceError::UnsupportedConfigVersion(file.version));
    }
    for (project_id, entry) in &file.projects {
        if let (Some((_, id_root)), Some(entry_root)) =
            (project_id.split_once('#'), entry.project_root.as_deref())
        {
            if normalize_project_root(id_root) != normalize_project_root(entry_root) {
                return Err(MempalaceError::ConfigParse {
                    path: path.to_path_buf(),
                    message: format!(
                        "conflicting project declaration `{project_id}`: ID root `{id_root}` differs from project_root `{entry_root}`"
                    ),
                });
            }
        }
    }
    Ok(file)
}

fn write_project_registry_file(
    path: &Path,
    registry: &ProjectRegistryFileV1,
) -> Result<()> {
    let body = serde_json::to_string_pretty(registry).map_err(|err| MempalaceError::ConfigParse {
        path: path.to_path_buf(),
        message: err.to_string(),
    })?;
    fs::write(path, body).map_err(|source| MempalaceError::ConfigWrite {
        path: path.to_path_buf(),
        source,
    })
}

fn resolve_profile(
    env_profile: Option<String>,
    file_profile: Option<EmbeddingProfile>,
) -> Result<EmbeddingProfile> {
    if let Some(profile) = env_profile {
        return profile.parse();
    }

    Ok(file_profile.unwrap_or_default())
}

fn required_positive_override(
    field_name: &str,
    value: Option<usize>,
    default: usize,
    config_path: &Path,
) -> Result<usize> {
    match value {
        Some(0) => Err(MempalaceError::ConfigParse {
            path: config_path.to_path_buf(),
            message: format!("{field_name} must be greater than 0"),
        }),
        Some(value) => Ok(value),
        None => Ok(default),
    }
}

fn optional_limit_override(value: Option<usize>, default: usize) -> usize {
    value.unwrap_or(default)
}

pub fn build_runtime(config: &MempalaceConfig) -> std::io::Result<Runtime> {
    if !config.low_cpu.enabled {
        return Runtime::new();
    }

    Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.low_cpu.worker_threads)
        .max_blocking_threads(config.low_cpu.max_blocking_threads)
        .build()
}

fn resolve_server_config(
    file_section: Option<ServerConfigFileV1>,
    base_dir: &Path,
    config_path: &Path,
) -> Result<ServerRuntimeConfig> {
    let default_bind: SocketAddr = DEFAULT_SERVER_BIND
        .parse()
        .expect("DEFAULT_SERVER_BIND is a valid socket address");
    let default_token_file = base_dir.join(DEFAULT_SERVER_TOKEN_FILE);

    let Some(section) = file_section else {
        return Ok(ServerRuntimeConfig {
            bind: default_bind,
            token_file: default_token_file,
            checkouts: BTreeMap::new(),
        });
    };

    let bind = match section.bind {
        None => default_bind,
        Some(ref s) => s.parse::<SocketAddr>().map_err(|_| MempalaceError::ConfigParse {
            path: config_path.to_path_buf(),
            message: format!(
                "server.bind `{s}` is not a valid socket address; expected e.g. \"127.0.0.1:8765\""
            ),
        })?,
    };

    let token_file = match section.token_file {
        None => default_token_file,
        Some(ref s) => expand_path(s)?,
    };

    Ok(ServerRuntimeConfig { bind, token_file, checkouts: section.checkouts })
}

fn default_collection_name() -> String {
    DEFAULT_COLLECTION_NAME.to_owned()
}

fn default_version() -> u32 {
    1
}

fn default_palace_path() -> String {
    format!("{DEFAULT_BASE_DIR}/palace")
}

fn resolve_project_config_path(base: &Path) -> PathBuf {
    let primary = base.join(PROJECT_CONFIG_FILE_NAME);
    if primary.exists() {
        return primary;
    }

    base.join(LEGACY_PROJECT_CONFIG_FILE_NAME)
}

fn expand_path(value: &str) -> Result<PathBuf> {
    if let Some(stripped) = value.strip_prefix("~/").or_else(|| value.strip_prefix("~\\")) {
        let home = dirs::home_dir().ok_or(MempalaceError::MissingHomeDirectory)?;
        return Ok(home.join(stripped));
    }

    if value == "~" {
        return dirs::home_dir().ok_or(MempalaceError::MissingHomeDirectory);
    }

    Ok(PathBuf::from(value))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    use mempalace_core::EmbeddingProfile;

    use super::{
        ConfigLoader, DEFAULT_COLLECTION_NAME, DEFAULT_LOW_CPU_INGEST_BATCH_SIZE,
        LowCpuConfigFileV1, LowCpuRuntimeConfig, ProjectConfig, ProjectRegistryEntryV1,
        ProjectRoomConfig,
    };

    fn temp_dir() -> PathBuf {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        std::env::temp_dir().join(format!("mempalace-rs-config-{nanos}"))
    }

    #[test]
    fn init_and_load_defaults_round_trip() {
        let base = temp_dir();
        let paths = ConfigLoader::init_default(Some(&base)).unwrap();
        let config = ConfigLoader::load_with_env(Some(&base)).unwrap();

        assert_eq!(paths.config_file, base.join("config.json"));
        assert_eq!(config.schema_version, 1);
        assert_eq!(config.collection_name, DEFAULT_COLLECTION_NAME);
        assert_eq!(config.embedding_profile, EmbeddingProfile::Balanced);
        assert_eq!(config.palace_path, base.join("palace"));
        assert!(!config.low_cpu.enabled);
        assert!(paths.palace_dir.is_dir());

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn env_overrides_palace_path_and_profile() {
        let base = temp_dir();
        ConfigLoader::init_default(Some(&base)).unwrap();

        let config = ConfigLoader::load_from_sources(
            Some(&base),
            Some("/tmp/custom-palace".to_owned()),
            Some("low_cpu".to_owned()),
        )
        .unwrap();

        assert_eq!(config.palace_path, PathBuf::from("/tmp/custom-palace"));
        assert_eq!(config.embedding_profile, EmbeddingProfile::LowCpu);
        assert!(config.low_cpu.enabled);
        assert_eq!(config.low_cpu.ingest_batch_size, DEFAULT_LOW_CPU_INGEST_BATCH_SIZE);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn low_cpu_overrides_are_loaded_from_config() {
        let base = temp_dir();
        fs::create_dir_all(&base).unwrap();
        fs::write(
            base.join("config.json"),
            r#"{
  "version": 1,
  "collection_name": "mempalace_drawers",
  "embedding_profile": "low_cpu",
  "low_cpu": {
    "worker_threads": 2,
    "max_blocking_threads": 3,
    "queue_limit": 7,
    "ingest_batch_size": 4,
    "search_results_limit": 2,
    "wake_up_drawers_limit": 1,
    "degraded_mode": false,
    "rerank_enabled": true
  }
}"#,
        )
        .unwrap();

        let config = ConfigLoader::load_with_env(Some(&base)).unwrap();

        assert!(config.low_cpu.enabled);
        assert_eq!(config.low_cpu.worker_threads, 2);
        assert_eq!(config.low_cpu.max_blocking_threads, 3);
        assert_eq!(config.low_cpu.queue_limit, 7);
        assert_eq!(config.low_cpu.ingest_batch_size, 4);
        assert_eq!(config.low_cpu.search_results_limit, 2);
        assert_eq!(config.low_cpu.wake_up_drawers_limit, 1);
        assert!(!config.low_cpu.degraded_mode);
        assert!(config.low_cpu.rerank_enabled);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn degraded_mode_tightens_effective_limits_and_disables_rerank() {
        let config = LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::LowCpu);

        assert_eq!(config.effective_queue_limit(), 8);
        assert_eq!(config.effective_ingest_batch_size(), 4);
        assert_eq!(config.effective_search_results_limit(), 3);
        assert_eq!(config.effective_wake_up_drawers_limit(), 4);
        assert!(!config.effective_rerank_enabled());
    }

    #[test]
    fn non_degraded_low_cpu_uses_explicit_overrides_for_effective_limits() {
        let config = LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::LowCpu)
            .with_overrides(
                Some(LowCpuConfigFileV1 {
                    worker_threads: None,
                    max_blocking_threads: None,
                    queue_limit: Some(11),
                    ingest_batch_size: Some(6),
                    search_results_limit: Some(9),
                    wake_up_drawers_limit: Some(7),
                    degraded_mode: Some(false),
                    rerank_enabled: Some(true),
                }),
                Path::new("config.json"),
            )
            .unwrap();

        assert_eq!(config.effective_queue_limit(), 11);
        assert_eq!(config.effective_ingest_batch_size(), 6);
        assert_eq!(config.effective_search_results_limit(), 9);
        assert_eq!(config.effective_wake_up_drawers_limit(), 7);
        assert!(config.effective_rerank_enabled());
    }

    #[test]
    fn zero_limits_are_loaded_without_falling_back() {
        let config = LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::LowCpu)
            .with_overrides(
                Some(LowCpuConfigFileV1 {
                    worker_threads: None,
                    max_blocking_threads: None,
                    queue_limit: Some(0),
                    ingest_batch_size: None,
                    search_results_limit: Some(0),
                    wake_up_drawers_limit: Some(0),
                    degraded_mode: Some(false),
                    rerank_enabled: None,
                }),
                Path::new("config.json"),
            )
            .unwrap();

        assert_eq!(config.queue_limit, 0);
        assert_eq!(config.search_results_limit, 0);
        assert_eq!(config.wake_up_drawers_limit, 0);
    }

    #[test]
    fn zero_thread_overrides_are_rejected_with_precise_errors() {
        let error = LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::LowCpu)
            .with_overrides(
                Some(LowCpuConfigFileV1 {
                    worker_threads: Some(0),
                    max_blocking_threads: None,
                    queue_limit: None,
                    ingest_batch_size: None,
                    search_results_limit: None,
                    wake_up_drawers_limit: None,
                    degraded_mode: None,
                    rerank_enabled: None,
                }),
                Path::new("config.json"),
            )
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "failed to parse config at config.json: low_cpu.worker_threads must be greater than 0"
        );
    }

    #[test]
    fn project_config_parses_yaml() {
        let base = temp_dir();
        fs::create_dir_all(&base).unwrap();
        fs::write(
            base.join("mempalace.yaml"),
            "wing: project_alpha\nrooms:\n  - name: backend\n    description: Backend code\n    keywords:\n      - auth\n",
        )
        .unwrap();

        let config = ConfigLoader::load_project_config(&base).unwrap();
        assert_eq!(config.wing, "project_alpha");
        assert_eq!(config.rooms.len(), 1);
        assert_eq!(config.rooms[0].name, "backend");
        assert_eq!(config.rooms[0].description.as_deref(), Some("Backend code"));
        assert_eq!(config.rooms[0].keywords, vec!["auth"]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn load_uses_base_dir_override_without_config_file() {
        let base = temp_dir();
        fs::create_dir_all(&base).unwrap();

        let config = ConfigLoader::load_with_env(Some(&base)).unwrap();

        assert_eq!(config.palace_path, base.join("palace"));
        assert_eq!(config.collection_name, DEFAULT_COLLECTION_NAME);
        assert_eq!(config.embedding_profile, EmbeddingProfile::Balanced);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn legacy_config_without_version_or_profile_still_loads() {
        let base = temp_dir();
        fs::create_dir_all(&base).unwrap();
        fs::write(base.join("config.json"), r#"{"collection_name":"legacy_drawers"}"#).unwrap();

        let config = ConfigLoader::load_with_env(Some(&base)).unwrap();

        assert_eq!(config.schema_version, 1);
        assert_eq!(config.collection_name, "legacy_drawers");
        assert_eq!(config.embedding_profile, EmbeddingProfile::Balanced);
        assert_eq!(config.palace_path, base.join("palace"));

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn legacy_project_config_filename_is_supported() {
        let base = temp_dir();
        fs::create_dir_all(&base).unwrap();
        fs::write(base.join("mempal.yaml"), "wing: legacy\nrooms: []\n").unwrap();

        let config = ConfigLoader::load_project_config(&base).unwrap();
        assert_eq!(config.wing, "legacy");
        assert!(config.rooms.is_empty());

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn centralized_project_registry_resolves_by_identity_and_checkout_alias() {
        let config_base = temp_dir();
        let project = temp_dir();
        fs::create_dir_all(&project).unwrap();
        let entry = ProjectRegistryEntryV1 {
            wing: "wing_registered".to_owned(),
            rooms: vec![ProjectRoomConfig {
                name: "backend".to_owned(),
                description: Some("Backend code".to_owned()),
                keywords: vec!["api".to_owned()],
            }],
            routing: None,
            checkouts: Vec::new(),
            project_root: None,
        };

        ConfigLoader::register_project(
            Some(&config_base),
            "github.com/example/repo",
            entry,
            Some(&project),
        )
        .unwrap();

        let by_identity = ConfigLoader::resolve_project_config(
            &project,
            Some(&config_base),
            Some("github.com/example/repo"),
            "wing_derived",
            Vec::new(),
        )
        .unwrap();
        assert_eq!(by_identity.wing, "wing_registered");
        assert_eq!(by_identity.rooms[0].name, "backend");

        let by_alias = ConfigLoader::resolve_project_config(
            &project,
            Some(&config_base),
            Some("github.com/other/repo"),
            "wing_derived",
            Vec::new(),
        )
        .unwrap();
        assert_eq!(by_alias.wing, "wing_registered");

        let registry = ConfigLoader::load_project_registry(Some(&config_base)).unwrap();
        assert_eq!(registry.projects.len(), 1);
        assert!(config_base.join("projects.json").is_file());

        fs::remove_dir_all(config_base).unwrap();
        fs::remove_dir_all(project).unwrap();
    }

    #[test]
    fn local_project_config_has_precedence_over_central_registry() {
        let config_base = temp_dir();
        let project = temp_dir();
        fs::create_dir_all(&project).unwrap();
        fs::write(project.join("mempalace.yaml"), "wing: local\nrooms: []\n").unwrap();
        ConfigLoader::register_project(
            Some(&config_base),
            "github.com/example/repo",
            ProjectRegistryEntryV1 {
                wing: "central".to_owned(),
                rooms: Vec::new(),
                routing: None,
                checkouts: vec![project.clone()],
                project_root: None,
            },
            None,
        )
        .unwrap();

        let config = ConfigLoader::resolve_project_config(
            &project,
            Some(&config_base),
            Some("github.com/example/repo"),
            "derived",
            Vec::new(),
        )
        .unwrap();
        assert_eq!(config, ProjectConfig { wing: "local".to_owned(), rooms: Vec::new(), routing: None });

        fs::remove_dir_all(config_base).unwrap();
        fs::remove_dir_all(project).unwrap();
    }

    #[test]
    fn monorepo_project_root_matches_subproject_checkout_and_aliases_merge() {
        let config_base = temp_dir();
        let repository = temp_dir();
        let subproject = repository.join("crates").join("api");
        fs::create_dir_all(&subproject).unwrap();
        let entry = ProjectRegistryEntryV1 {
            wing: "wing_api".to_owned(),
            rooms: vec![ProjectRoomConfig {
                name: "api".to_owned(),
                description: None,
                keywords: Vec::new(),
            }],
            routing: None,
            checkouts: Vec::new(),
            project_root: Some("crates/api".to_owned()),
        };

        ConfigLoader::register_project(
            Some(&config_base),
            "github.com/example/monorepo#crates/api",
            entry,
            Some(&repository),
        )
        .unwrap();
        let clone = temp_dir();
        fs::create_dir_all(&clone).unwrap();
        ConfigLoader::register_project(
            Some(&config_base),
            "github.com/example/monorepo#crates/api",
            ProjectRegistryEntryV1 {
                wing: "wing_api".to_owned(),
                rooms: Vec::new(),
                routing: None,
                checkouts: Vec::new(),
                project_root: Some("crates/api".to_owned()),
            },
            Some(&clone),
        )
        .unwrap();

        let resolved = ConfigLoader::resolve_project_config(
            &subproject,
            Some(&config_base),
            Some("github.com/example/monorepo"),
            "derived",
            Vec::new(),
        )
        .unwrap();
        assert_eq!(resolved.wing, "wing_api");
        let registry = ConfigLoader::load_project_registry(Some(&config_base)).unwrap();
        let entry = registry.projects.get("github.com/example/monorepo#crates/api").unwrap();
        assert_eq!(entry.checkouts.len(), 2);

        fs::remove_dir_all(config_base).unwrap();
        fs::remove_dir_all(repository).unwrap();
        fs::remove_dir_all(clone).unwrap();
    }

    #[test]
    fn monorepo_project_root_does_not_match_an_unrelated_repository() {
        let config_base = temp_dir();
        let repository = temp_dir();
        let subproject = repository.join("crates").join("api");
        fs::create_dir_all(&subproject).unwrap();
        let run = |args: &[&str]| {
            let status = Command::new("git").args(args).current_dir(&repository).status().unwrap();
            assert!(status.success(), "git {args:?} failed");
        };
        run(&["init"]);
        run(&["remote", "add", "origin", "https://github.com/example/unrelated.git"]);

        ConfigLoader::register_project(
            Some(&config_base),
            "github.com/example/monorepo#crates/api",
            ProjectRegistryEntryV1 {
                wing: "wing_api".to_owned(),
                rooms: Vec::new(),
                routing: None,
                checkouts: Vec::new(),
                project_root: Some("crates/api".to_owned()),
            },
            None,
        )
        .unwrap();

        let resolved = ConfigLoader::resolve_project_config(
            &subproject,
            Some(&config_base),
            Some("github.com/example/unrelated#crates/api"),
            "wing_derived",
            Vec::new(),
        )
        .unwrap();
        assert_eq!(resolved.wing, "wing_derived");

        fs::remove_dir_all(config_base).unwrap();
        fs::remove_dir_all(repository).unwrap();
    }

    #[test]
    fn wing_only_project_id_hints_are_not_reused_for_unmatched_checkouts() {
        let config_base = temp_dir();
        let project = temp_dir();
        fs::create_dir_all(&project).unwrap();
        ConfigLoader::register_project(
            Some(&config_base),
            "wing:wing_app",
            ProjectRegistryEntryV1 {
                wing: "wing_registered".to_owned(),
                rooms: Vec::new(),
                routing: None,
                checkouts: Vec::new(),
                project_root: None,
            },
            None,
        )
        .unwrap();

        let resolved = ConfigLoader::resolve_project_config(
            &project,
            Some(&config_base),
            Some("wing:wing_app"),
            "wing_derived",
            Vec::new(),
        )
        .unwrap();
        assert_eq!(resolved.wing, "wing_derived");

        fs::remove_dir_all(config_base).unwrap();
        fs::remove_dir_all(project).unwrap();
    }

    #[test]
    fn monorepo_project_roots_can_share_one_checkout_alias() {
        let config_base = temp_dir();
        let repository = temp_dir();
        fs::create_dir_all(&repository).unwrap();
        for (project_id, project_root, wing) in [
            ("github.com/example/monorepo#crates/api", "crates/api", "wing_api"),
            ("github.com/example/monorepo#packages/web", "packages/web", "wing_web"),
        ] {
            ConfigLoader::register_project(
                Some(&config_base),
                project_id,
                ProjectRegistryEntryV1 {
                    wing: wing.to_owned(),
                    rooms: Vec::new(),
                    routing: None,
                    checkouts: Vec::new(),
                    project_root: Some(project_root.to_owned()),
                },
                Some(&repository),
            )
            .unwrap();
        }

        let registry = ConfigLoader::load_project_registry(Some(&config_base)).unwrap();
        assert_eq!(registry.projects.len(), 2);
        let canonical_repository = repository.canonicalize().unwrap();
        assert_eq!(registry.projects["github.com/example/monorepo#crates/api"].checkouts, vec![canonical_repository.clone()]);
        assert_eq!(registry.projects["github.com/example/monorepo#packages/web"].checkouts, vec![canonical_repository]);

        fs::remove_dir_all(config_base).unwrap();
        fs::remove_dir_all(repository).unwrap();
    }

    #[test]
    fn registering_an_existing_project_preserves_central_declarations() {
        let config_base = temp_dir();
        let checkout = temp_dir();
        fs::create_dir_all(&checkout).unwrap();
        let project_id = "github.com/example/repo";
        ConfigLoader::register_project(
            Some(&config_base),
            project_id,
            ProjectRegistryEntryV1 {
                wing: "wing_curated".to_owned(),
                rooms: vec![ProjectRoomConfig {
                    name: "backend".to_owned(),
                    description: Some("Curated".to_owned()),
                    keywords: vec!["api".to_owned()],
                }],
                routing: Some(crate::federation::ProjectRoutingConfig {
                    mode: crate::federation::RouteMode::Local,
                    remote: None,
                    write: None,
                }),
                checkouts: Vec::new(),
                project_root: None,
            },
            Some(&checkout),
        )
        .unwrap();
        ConfigLoader::register_project(
            Some(&config_base),
            project_id,
            ProjectRegistryEntryV1 {
                wing: String::new(),
                rooms: Vec::new(),
                routing: None,
                checkouts: Vec::new(),
                project_root: None,
            },
            Some(&checkout),
        )
        .unwrap();

        let entry = ConfigLoader::load_project_registry(Some(&config_base))
            .unwrap()
            .projects
            .remove(project_id)
            .unwrap();
        assert_eq!(entry.wing, "wing_curated");
        assert_eq!(entry.rooms[0].name, "backend");
        assert!(entry.routing.is_some());

        fs::remove_dir_all(config_base).unwrap();
        fs::remove_dir_all(checkout).unwrap();
    }

    #[test]
    fn registering_the_same_checkout_under_two_ids_reports_conflict() {
        let config_base = temp_dir();
        let project = temp_dir();
        fs::create_dir_all(&project).unwrap();
        let entry = ProjectRegistryEntryV1 {
            wing: "wing_one".to_owned(),
            rooms: Vec::new(),
            routing: None,
            checkouts: Vec::new(),
            project_root: None,
        };
        ConfigLoader::register_project(Some(&config_base), "one", entry.clone(), Some(&project))
            .unwrap();
        let error = ConfigLoader::register_project(
            Some(&config_base),
            "two",
            ProjectRegistryEntryV1 { wing: "wing_two".to_owned(), ..entry },
            Some(&project),
        )
        .unwrap_err();
        assert!(error.to_string().contains("conflicting project declarations"));

        fs::remove_dir_all(config_base).unwrap();
        fs::remove_dir_all(project).unwrap();
    }

    #[test]
    fn legacy_palace_env_alias_is_supported() {
        let base = temp_dir();
        ConfigLoader::init_default(Some(&base)).unwrap();

        let config = ConfigLoader::load_from_sources(
            Some(&base),
            Some("/tmp/legacy-palace".to_owned()),
            None,
        )
        .unwrap();

        assert_eq!(config.palace_path, PathBuf::from("/tmp/legacy-palace"));

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn invalid_embedding_profile_is_rejected() {
        let base = temp_dir();
        ConfigLoader::init_default(Some(&base)).unwrap();

        let err = ConfigLoader::load_from_sources(
            Some(&base),
            None,
            Some("definitely_not_real".to_owned()),
        )
        .unwrap_err();

        assert!(err.to_string().contains("embedding profile"), "unexpected error: {err}");

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn rejects_unsupported_schema_version() {
        let base = temp_dir();
        fs::create_dir_all(&base).unwrap();
        fs::write(
            base.join("config.json"),
            r#"{"version":2,"collection_name":"mempalace_drawers"}"#,
        )
        .unwrap();

        let err = ConfigLoader::load_with_env(Some(&base)).unwrap_err();
        assert!(err.to_string().contains("unsupported config schema version"));

        fs::remove_dir_all(base).unwrap();
    }

    // ─── Server config tests ──────────────────────────────────────────────────

    #[test]
    fn server_checkouts_populated_from_config() {
        let base = temp_dir();
        fs::create_dir_all(&base).unwrap();
        fs::write(
            base.join("config.json"),
            r#"{
  "version": 1,
  "server": {
    "bind": "127.0.0.1:8765",
    "checkouts": {
      "wing_proj": "/repos/myproject",
      "wing_docs": "/repos/docs"
    }
  }
}"#,
        )
        .unwrap();

        let config = ConfigLoader::load_with_env(Some(&base)).unwrap();

        assert_eq!(config.server.checkouts.len(), 2);
        assert_eq!(
            config.server.checkouts.get("wing_proj"),
            Some(&PathBuf::from("/repos/myproject"))
        );
        assert_eq!(
            config.server.checkouts.get("wing_docs"),
            Some(&PathBuf::from("/repos/docs"))
        );

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn server_checkouts_defaults_to_empty_when_absent() {
        let base = temp_dir();
        fs::create_dir_all(&base).unwrap();
        // server section present but no checkouts field
        fs::write(
            base.join("config.json"),
            r#"{"version":1,"server":{"bind":"127.0.0.1:8765"}}"#,
        )
        .unwrap();

        let config = ConfigLoader::load_with_env(Some(&base)).unwrap();
        assert!(config.server.checkouts.is_empty());

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn server_checkouts_defaults_to_empty_when_section_absent() {
        let base = temp_dir();
        fs::create_dir_all(&base).unwrap();
        fs::write(base.join("config.json"), r#"{"version":1}"#).unwrap();

        let config = ConfigLoader::load_with_env(Some(&base)).unwrap();
        assert!(config.server.checkouts.is_empty());

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn server_defaults_when_section_absent() {
        let base = temp_dir();
        fs::create_dir_all(&base).unwrap();
        // No server section
        fs::write(base.join("config.json"), r#"{"version":1}"#).unwrap();

        let config = ConfigLoader::load_with_env(Some(&base)).unwrap();

        let bind: std::net::SocketAddr = "127.0.0.1:8765".parse().unwrap();
        assert_eq!(config.server.bind, bind);
        assert_eq!(config.server.token_file, base.join("server_tokens.json"));

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn server_explicit_values_are_parsed() {
        let base = temp_dir();
        fs::create_dir_all(&base).unwrap();
        fs::write(
            base.join("config.json"),
            r#"{
  "version": 1,
  "server": {
    "bind": "0.0.0.0:9999",
    "token_file": "/tmp/my_tokens.json"
  }
}"#,
        )
        .unwrap();

        let config = ConfigLoader::load_with_env(Some(&base)).unwrap();

        let expected_bind: std::net::SocketAddr = "0.0.0.0:9999".parse().unwrap();
        assert_eq!(config.server.bind, expected_bind);
        assert_eq!(config.server.token_file, PathBuf::from("/tmp/my_tokens.json"));

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn invalid_server_bind_is_rejected_with_precise_error() {
        let base = temp_dir();
        fs::create_dir_all(&base).unwrap();
        fs::write(
            base.join("config.json"),
            r#"{"version":1,"server":{"bind":"not-an-address"}}"#,
        )
        .unwrap();

        let err = ConfigLoader::load_with_env(Some(&base)).unwrap_err();
        assert!(
            err.to_string().contains("server.bind"),
            "expected server.bind error message, got: {err}"
        );
        assert!(
            err.to_string().contains("not-an-address"),
            "error should contain the bad value, got: {err}"
        );

        fs::remove_dir_all(base).unwrap();
    }
}

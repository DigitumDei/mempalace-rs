use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use mempalace_core::{EmbeddingProfile, MempalaceError, Result};
use serde::{Deserialize, Serialize};
use tokio::runtime::{Builder, Runtime};

pub const DEFAULT_BASE_DIR: &str = "~/.mempalace";
pub const DEFAULT_COLLECTION_NAME: &str = "mempalace_drawers";
const CONFIG_FILE_NAME: &str = "config.json";
const PROJECT_CONFIG_FILE_NAME: &str = "mempalace.yaml";
const LEGACY_PROJECT_CONFIG_FILE_NAME: &str = "mempal.yaml";
const DEFAULT_LOW_CPU_WORKER_THREADS: usize = 1;
const DEFAULT_LOW_CPU_MAX_BLOCKING_THREADS: usize = 1;
const DEFAULT_LOW_CPU_QUEUE_LIMIT: usize = 32;
const DEFAULT_LOW_CPU_INGEST_BATCH_SIZE: usize = 8;
const DEFAULT_LOW_CPU_SEARCH_RESULTS_LIMIT: usize = 5;
const DEFAULT_LOW_CPU_WAKE_UP_DRAWERS_LIMIT: usize = 8;
const DEGRADED_LOW_CPU_QUEUE_LIMIT: usize = 8;
const DEGRADED_LOW_CPU_INGEST_BATCH_SIZE: usize = 4;
const DEGRADED_LOW_CPU_SEARCH_RESULTS_LIMIT: usize = 3;
const DEGRADED_LOW_CPU_WAKE_UP_DRAWERS_LIMIT: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedPaths {
    pub base_dir: PathBuf,
    pub palace_dir: PathBuf,
    pub config_file: PathBuf,
    pub people_map_file: PathBuf,
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
            return usize::MAX;
        }

        if self.degraded_mode {
            self.search_results_limit.min(DEGRADED_LOW_CPU_SEARCH_RESULTS_LIMIT)
        } else {
            self.search_results_limit
        }
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
}

impl Default for ConfigFileV1 {
    fn default() -> Self {
        Self {
            version: 1,
            palace_path: Some(default_palace_path()),
            collection_name: default_collection_name(),
            embedding_profile: Some(EmbeddingProfile::Balanced),
            low_cpu: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MempalaceConfig {
    pub schema_version: u32,
    pub collection_name: String,
    pub palace_path: PathBuf,
    pub embedding_profile: EmbeddingProfile,
    pub low_cpu: LowCpuRuntimeConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectConfig {
    pub wing: String,
    #[serde(default)]
    pub rooms: Vec<ProjectRoomConfig>,
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

        Ok(MempalaceConfig {
            schema_version: file.version,
            collection_name: file.collection_name,
            palace_path: expand_path(&palace_path)?,
            embedding_profile,
            low_cpu: LowCpuRuntimeConfig::defaults_for_profile(embedding_profile)
                .with_overrides(file.low_cpu, &paths.config_file)?,
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
}

fn resolve_paths(base_dir_override: Option<&Path>) -> Result<ResolvedPaths> {
    let base_dir = match base_dir_override {
        Some(path) => path.to_path_buf(),
        None => expand_path(DEFAULT_BASE_DIR)?,
    };

    Ok(ResolvedPaths {
        palace_dir: base_dir.join("palace"),
        config_file: base_dir.join(CONFIG_FILE_NAME),
        people_map_file: base_dir.join("people_map.json"),
        base_dir,
    })
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
    use std::time::{SystemTime, UNIX_EPOCH};

    use mempalace_core::EmbeddingProfile;

    use super::{
        ConfigLoader, DEFAULT_COLLECTION_NAME, DEFAULT_LOW_CPU_INGEST_BATCH_SIZE,
        LowCpuConfigFileV1, LowCpuRuntimeConfig,
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
}

//! Embedding provider contracts and fastembed-backed runtime support.

use std::env;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
pub use mempalace_core as core;
use mempalace_core::{EmbeddingProfile, EmbeddingProfileMetadata};
use thiserror::Error;
use tracing::info;

/// Result type for the embeddings crate.
pub type Result<T> = std::result::Result<T, EmbeddingError>;

/// Input payload for a single embedding call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingRequest {
    texts: Vec<String>,
}

impl EmbeddingRequest {
    /// Builds a request from owned text items.
    pub fn new(texts: Vec<String>) -> Result<Self> {
        if texts.is_empty() {
            return Err(EmbeddingError::EmptyRequest);
        }

        if texts.iter().any(|text| text.trim().is_empty()) {
            return Err(EmbeddingError::BlankInput);
        }

        Ok(Self { texts })
    }

    /// Returns the request contents in provider order.
    pub fn texts(&self) -> &[String] {
        &self.texts
    }

    /// Returns the batch size.
    pub fn len(&self) -> usize {
        self.texts.len()
    }

    /// Returns whether the request has no inputs.
    pub fn is_empty(&self) -> bool {
        self.texts.is_empty()
    }
}

impl TryFrom<Vec<String>> for EmbeddingRequest {
    type Error = EmbeddingError;

    fn try_from(value: Vec<String>) -> Result<Self> {
        Self::new(value)
    }
}

/// Output payload for a single embedding call.
#[derive(Debug, Clone, PartialEq)]
pub struct EmbeddingResponse {
    vectors: Vec<Vec<f32>>,
    dimensions: usize,
    profile: EmbeddingProfile,
    model_id: &'static str,
}

impl EmbeddingResponse {
    /// Builds a response from provider output after validating dimensions.
    pub fn from_vectors(
        vectors: Vec<Vec<f32>>,
        dimensions: usize,
        profile: EmbeddingProfile,
        model_id: &'static str,
    ) -> Result<Self> {
        Self::new(vectors, dimensions, profile, model_id)
    }

    fn new(
        vectors: Vec<Vec<f32>>,
        dimensions: usize,
        profile: EmbeddingProfile,
        model_id: &'static str,
    ) -> Result<Self> {
        if vectors.is_empty() {
            return Err(EmbeddingError::ProviderContract(
                "provider returned no vectors for a non-empty request".to_owned(),
            ));
        }

        if vectors.iter().any(|vector| vector.len() != dimensions) {
            return Err(EmbeddingError::DimensionMismatch {
                expected: dimensions,
                actual: vectors
                    .iter()
                    .find(|vector| vector.len() != dimensions)
                    .map_or(0, Vec::len),
            });
        }

        Ok(Self { vectors, dimensions, profile, model_id })
    }

    /// Embedding vectors in the same order as the input request.
    pub fn vectors(&self) -> &[Vec<f32>] {
        &self.vectors
    }

    /// Expected vector dimensions for the response.
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    /// Resolved embedding profile used for the request.
    pub fn profile(&self) -> EmbeddingProfile {
        self.profile
    }

    /// Concrete model id used for the request.
    pub fn model_id(&self) -> &'static str {
        self.model_id
    }
}

/// Readiness status for startup validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupValidationStatus {
    /// Model assets look complete and usable.
    Ready,
    /// No local assets were found for the configured profile.
    MissingAssets,
    /// Some expected assets are present, but the cache is incomplete.
    PartialDownload,
    /// Assets are present but structurally invalid.
    CorruptedCache,
}

impl StartupValidationStatus {
    /// Returns whether the provider can proceed offline.
    pub fn is_ready(self) -> bool {
        matches!(self, Self::Ready)
    }
}

impl fmt::Display for StartupValidationStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ready => write!(f, "ready"),
            Self::MissingAssets => write!(f, "missing_assets"),
            Self::PartialDownload => write!(f, "partial_download"),
            Self::CorruptedCache => write!(f, "corrupted_cache"),
        }
    }
}

/// Startup validation report surfaced to callers and logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupValidation {
    pub status: StartupValidationStatus,
    pub cache_root: PathBuf,
    pub model_id: &'static str,
    pub detail: String,
}

impl StartupValidation {
    /// Returns whether the provider can proceed offline.
    pub fn is_ready(&self) -> bool {
        self.status.is_ready()
    }
}

/// Lightweight latency capture used by the benchmark harness.
#[derive(Debug, Clone, PartialEq)]
pub struct EmbeddingBenchmark {
    pub samples: Vec<Duration>,
}

impl EmbeddingBenchmark {
    /// Measures repeated warm-path embedding requests.
    pub fn measure<P: EmbeddingProvider>(
        provider: &mut P,
        request: &EmbeddingRequest,
        iterations: usize,
    ) -> Result<Self> {
        Self::measure_with_warmup(provider, request, 3, iterations)
    }

    /// Measures repeated embedding requests after discarding warmup iterations.
    pub fn measure_with_warmup<P: EmbeddingProvider>(
        provider: &mut P,
        request: &EmbeddingRequest,
        warmup_iterations: usize,
        iterations: usize,
    ) -> Result<Self> {
        if iterations == 0 {
            return Err(EmbeddingError::Benchmark(
                "iterations must be greater than zero".to_owned(),
            ));
        }

        for _ in 0..warmup_iterations {
            let _ = provider.embed(request)?;
        }

        let mut samples = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            let started = Instant::now();
            let _ = provider.embed(request)?;
            samples.push(started.elapsed());
        }

        Ok(Self { samples })
    }

    /// Returns the p95 latency in milliseconds.
    pub fn p95_millis(&self) -> Option<f64> {
        percentile_millis(&self.samples, 95.0)
    }
}

/// Provider contract for all embedding backends.
pub trait EmbeddingProvider {
    /// Returns the pinned profile metadata for the provider instance.
    fn profile(&self) -> &'static EmbeddingProfileMetadata;

    /// Performs startup validation without mutating the backing store.
    fn startup_validation(&self) -> Result<StartupValidation>;

    /// Embeds a batch of text inputs in request order.
    fn embed(&mut self, request: &EmbeddingRequest) -> Result<EmbeddingResponse>;
}

/// Runtime settings for the fastembed backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FastembedProviderConfig {
    pub cache_root: PathBuf,
    pub allow_downloads: bool,
    pub show_download_progress: bool,
}

impl FastembedProviderConfig {
    /// Builds a config from a caller-supplied cache path.
    pub fn new(cache_root: impl Into<PathBuf>) -> Self {
        Self {
            cache_root: cache_root.into(),
            allow_downloads: false,
            show_download_progress: false,
        }
    }
}

/// Returns true only for explicit truthy environment values.
pub fn env_flag(name: &str) -> bool {
    env::var(name).map(|v| env_flag_value(&v)).unwrap_or(false)
}

fn env_flag_value(value: &str) -> bool {
    matches!(value, "1" | "true" | "TRUE" | "yes" | "YES")
}

/// Resolved profile details used by the runtime and validation layers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedEmbeddingProfile {
    pub metadata: &'static EmbeddingProfileMetadata,
    pub warm_query_p95_budget_ms: u64,
    pub low_cpu_idle_rss_budget_mb: Option<u64>,
    pub low_cpu_ingest_rss_budget_mb: Option<u64>,
}

impl ResolvedEmbeddingProfile {
    /// Resolves pinned runtime details for a configured profile.
    pub fn from_profile(profile: EmbeddingProfile) -> Self {
        match profile {
            EmbeddingProfile::Balanced => Self {
                metadata: profile.metadata(),
                warm_query_p95_budget_ms: 750,
                low_cpu_idle_rss_budget_mb: None,
                low_cpu_ingest_rss_budget_mb: None,
            },
            EmbeddingProfile::LowCpu => Self {
                metadata: profile.metadata(),
                warm_query_p95_budget_ms: 1_500,
                low_cpu_idle_rss_budget_mb: Some(450),
                low_cpu_ingest_rss_budget_mb: Some(850),
            },
        }
    }
}

/// fastembed-backed embedding provider.
pub struct FastembedProvider {
    profile: ResolvedEmbeddingProfile,
    config: FastembedProviderConfig,
    backend: Option<TextEmbedding>,
}

impl fmt::Debug for FastembedProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FastembedProvider")
            .field("profile", &self.profile)
            .field("config", &self.config)
            .field("backend_initialized", &self.backend.is_some())
            .finish()
    }
}

impl FastembedProvider {
    /// Creates a provider without initializing model assets.
    pub fn new(profile: EmbeddingProfile, config: FastembedProviderConfig) -> Self {
        Self { profile: ResolvedEmbeddingProfile::from_profile(profile), config, backend: None }
    }

    /// Initializes the backend after startup validation passes.
    pub fn try_initialize(mut self) -> Result<Self> {
        let cache_root = effective_cache_root(&self.config);
        let validation = validate_cache(self.profile, &cache_root)?;
        if !validation.is_ready() && !self.config.allow_downloads {
            return Err(EmbeddingError::OfflineStartup {
                model_id: self.profile.metadata.model_id.to_owned(),
                detail: validation.detail,
            });
        }

        if matches!(validation.status, StartupValidationStatus::CorruptedCache)
            && self.config.allow_downloads
        {
            purge_model_cache(self.profile, &cache_root)?;
        }

        let options = build_init_options(self.profile, &self.config, &cache_root)?;
        let backend =
            TextEmbedding::try_new(options).map_err(|source| EmbeddingError::Backend {
                model_id: self.profile.metadata.model_id.to_owned(),
                message: source.to_string(),
            })?;

        self.backend = Some(backend);
        Ok(self)
    }

    /// Returns the configured cache directory root.
    pub fn cache_root(&self) -> &Path {
        &self.config.cache_root
    }
}

impl EmbeddingProvider for FastembedProvider {
    fn profile(&self) -> &'static EmbeddingProfileMetadata {
        self.profile.metadata
    }

    fn startup_validation(&self) -> Result<StartupValidation> {
        let cache_root = effective_cache_root(&self.config);
        validate_cache(self.profile, &cache_root)
    }

    fn embed(&mut self, request: &EmbeddingRequest) -> Result<EmbeddingResponse> {
        let backend = self.backend.as_mut().ok_or(EmbeddingError::NotInitialized {
            model_id: self.profile.metadata.model_id.to_owned(),
        })?;

        let documents = request.texts.iter().map(String::as_str).collect::<Vec<_>>();
        let vectors = backend.embed(documents, None).map_err(|source| EmbeddingError::Backend {
            model_id: self.profile.metadata.model_id.to_owned(),
            message: source.to_string(),
        })?;

        if vectors.len() != request.len() {
            return Err(EmbeddingError::ProviderContract(format!(
                "provider returned {} vectors for {} inputs",
                vectors.len(),
                request.len()
            )));
        }

        EmbeddingResponse::new(
            vectors,
            self.profile.metadata.dimensions,
            self.profile.metadata.profile,
            self.profile.metadata.model_id,
        )
    }
}

/// Logs the startup validation state for a configured profile.
pub fn log_startup_validation(validation: &StartupValidation) {
    info!(
        embedding_model = validation.model_id,
        cache_root = %validation.cache_root.display(),
        startup_validation = %validation.status,
        detail = validation.detail,
        "embedding startup validation completed"
    );
}

/// Validates a cache directory for the given profile.
pub fn validate_cache(
    profile: ResolvedEmbeddingProfile,
    cache_root: &Path,
) -> Result<StartupValidation> {
    let model_layout = resolve_fastembed_model_layout(profile)?;
    let discovered = discover_cache_assets(cache_root, &model_layout)?;
    let model_id = profile.metadata.model_id;

    let status = if discovered.files_found == 0 {
        StartupValidationStatus::MissingAssets
    } else if !discovered.zero_length_files.is_empty() || !discovered.invalid_json_files.is_empty()
    {
        StartupValidationStatus::CorruptedCache
    } else if discovered.model_files == 0
        || discovered.tokenizer_json_files == 0
        || discovered.config_json_files == 0
        || discovered.special_tokens_map_json_files == 0
        || discovered.tokenizer_config_json_files == 0
    {
        StartupValidationStatus::PartialDownload
    } else {
        StartupValidationStatus::Ready
    };

    let detail = match status {
        StartupValidationStatus::Ready => {
            format!("warm cache detected for {model_id}")
        }
        StartupValidationStatus::MissingAssets => {
            format!("no local cache assets found for {model_id}")
        }
        StartupValidationStatus::PartialDownload => format!(
            "cache for {model_id} is incomplete: model_file={}, tokenizer_json={}, config_json={}, special_tokens_map_json={}, tokenizer_config_json={}",
            discovered.model_files,
            discovered.tokenizer_json_files,
            discovered.config_json_files,
            discovered.special_tokens_map_json_files,
            discovered.tokenizer_config_json_files
        ),
        StartupValidationStatus::CorruptedCache => format!(
            "cache for {model_id} is corrupted: zero_length={}, invalid_json={}",
            discovered.zero_length_files.len(),
            discovered.invalid_json_files.len()
        ),
    };

    Ok(StartupValidation { status, cache_root: cache_root.to_path_buf(), model_id, detail })
}

fn build_init_options(
    profile: ResolvedEmbeddingProfile,
    config: &FastembedProviderConfig,
    cache_root: &Path,
) -> Result<InitOptions> {
    let model_name = resolve_fastembed_model_layout(profile)?.model_name;

    Ok(InitOptions::new(model_name)
        .with_show_download_progress(config.show_download_progress)
        .with_cache_dir(cache_root.to_path_buf()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FastembedModelLayout {
    model_name: EmbeddingModel,
    model_cache_dir: PathBuf,
    model_file: PathBuf,
}

fn resolve_fastembed_model_layout(
    profile: ResolvedEmbeddingProfile,
) -> Result<FastembedModelLayout> {
    let model_name = match profile.metadata.profile {
        EmbeddingProfile::Balanced => EmbeddingModel::AllMiniLML6V2,
        EmbeddingProfile::LowCpu => EmbeddingModel::AllMiniLML6V2Q,
    };
    let (model_cache_dir, model_file) = {
        let model_info = TextEmbedding::get_model_info(&model_name).map_err(|source| {
            EmbeddingError::Backend {
                model_id: profile.metadata.model_id.to_owned(),
                message: source.to_string(),
            }
        })?;
        (
            PathBuf::from(format!("models--{}", model_info.model_code.replace('/', "--"))),
            PathBuf::from(&model_info.model_file),
        )
    };

    Ok(FastembedModelLayout { model_name, model_cache_dir, model_file })
}

#[derive(Debug, Default)]
struct CacheAssets {
    files_found: usize,
    model_files: usize,
    tokenizer_json_files: usize,
    config_json_files: usize,
    special_tokens_map_json_files: usize,
    tokenizer_config_json_files: usize,
    zero_length_files: Vec<PathBuf>,
    invalid_json_files: Vec<PathBuf>,
}

fn discover_cache_assets(
    cache_root: &Path,
    model_layout: &FastembedModelLayout,
) -> Result<CacheAssets> {
    let model_root = cache_root.join(&model_layout.model_cache_dir);
    if !model_root.exists() {
        return Ok(CacheAssets::default());
    }

    let mut assets = CacheAssets::default();
    assets.files_found = count_files(&model_root)?;

    let files_root = match resolve_model_files_root(&model_root)? {
        Some(files_root) => files_root,
        None => return Ok(assets),
    };

    inspect_required_file(
        &files_root.join(&model_layout.model_file),
        &mut assets,
        RequiredFile::Model,
    )?;
    inspect_required_file(
        &files_root.join("tokenizer.json"),
        &mut assets,
        RequiredFile::TokenizerJson,
    )?;
    inspect_required_file(&files_root.join("config.json"), &mut assets, RequiredFile::ConfigJson)?;
    inspect_required_file(
        &files_root.join("special_tokens_map.json"),
        &mut assets,
        RequiredFile::SpecialTokensMapJson,
    )?;
    inspect_required_file(
        &files_root.join("tokenizer_config.json"),
        &mut assets,
        RequiredFile::TokenizerConfigJson,
    )?;

    Ok(assets)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequiredFile {
    Model,
    TokenizerJson,
    ConfigJson,
    SpecialTokensMapJson,
    TokenizerConfigJson,
}

fn resolve_model_files_root(model_root: &Path) -> Result<Option<PathBuf>> {
    let snapshots_root = model_root.join("snapshots");
    if !snapshots_root.exists() {
        return Ok(Some(model_root.to_path_buf()));
    }

    let active_ref = model_root.join("refs").join("main");
    if !active_ref.exists() {
        return Ok(None);
    }

    let revision = fs::read_to_string(&active_ref)
        .map_err(|source| EmbeddingError::CacheRead { path: active_ref.clone(), source })?;
    let snapshot_root = snapshots_root.join(revision.trim());
    if snapshot_root.is_dir() { Ok(Some(snapshot_root)) } else { Ok(None) }
}

fn count_files(root: &Path) -> Result<usize> {
    let mut pending = vec![root.to_path_buf()];
    let mut count = 0;

    while let Some(path) = pending.pop() {
        let entries = fs::read_dir(&path)
            .map_err(|source| EmbeddingError::CacheRead { path: path.clone(), source })?;

        for entry in entries {
            let entry =
                entry.map_err(|source| EmbeddingError::CacheRead { path: path.clone(), source })?;
            let entry_path = entry.path();
            let metadata = entry
                .metadata()
                .map_err(|source| EmbeddingError::CacheRead { path: entry_path.clone(), source })?;

            if metadata.is_dir() {
                pending.push(entry_path);
            } else if metadata.is_file() {
                count += 1;
            }
        }
    }

    Ok(count)
}

fn inspect_required_file(
    path: &Path,
    assets: &mut CacheAssets,
    required_file: RequiredFile,
) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }

    let metadata = fs::metadata(path)
        .map_err(|source| EmbeddingError::CacheRead { path: path.to_path_buf(), source })?;
    if !metadata.is_file() {
        return Ok(());
    }

    if metadata.len() == 0 {
        assets.zero_length_files.push(path.to_path_buf());
    }

    match required_file {
        RequiredFile::Model => assets.model_files += 1,
        RequiredFile::TokenizerJson => {
            assets.tokenizer_json_files += 1;
            if json_file_is_invalid(path)? {
                assets.invalid_json_files.push(path.to_path_buf());
            }
        }
        RequiredFile::ConfigJson => {
            assets.config_json_files += 1;
            if json_file_is_invalid(path)? {
                assets.invalid_json_files.push(path.to_path_buf());
            }
        }
        RequiredFile::SpecialTokensMapJson => {
            assets.special_tokens_map_json_files += 1;
            if json_file_is_invalid(path)? {
                assets.invalid_json_files.push(path.to_path_buf());
            }
        }
        RequiredFile::TokenizerConfigJson => {
            assets.tokenizer_config_json_files += 1;
            if json_file_is_invalid(path)? {
                assets.invalid_json_files.push(path.to_path_buf());
            }
        }
    }

    Ok(())
}

fn json_file_is_invalid(path: &Path) -> Result<bool> {
    let body = fs::read_to_string(path)
        .map_err(|source| EmbeddingError::CacheRead { path: path.to_path_buf(), source })?;

    Ok(serde_json::from_str::<serde_json::Value>(&body).is_err())
}

fn effective_cache_root(config: &FastembedProviderConfig) -> PathBuf {
    effective_cache_root_from_hf_home(config, env::var_os("HF_HOME").as_deref())
}

fn effective_cache_root_from_hf_home(
    config: &FastembedProviderConfig,
    hf_home: Option<&std::ffi::OsStr>,
) -> PathBuf {
    hf_home.map(PathBuf::from).unwrap_or_else(|| config.cache_root.clone())
}

fn purge_model_cache(profile: ResolvedEmbeddingProfile, cache_root: &Path) -> Result<()> {
    let model_root = cache_root.join(resolve_fastembed_model_layout(profile)?.model_cache_dir);
    match fs::remove_dir_all(&model_root) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(EmbeddingError::CacheRead { path: model_root, source }),
    }
}

fn percentile_millis(samples: &[Duration], percentile: f64) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }

    let mut ordered = samples.to_vec();
    ordered.sort_unstable();
    let index = (((percentile / 100.0) * ordered.len() as f64).ceil() as usize).saturating_sub(1);
    ordered.get(index).map(Duration::as_secs_f64).map(|seconds| seconds * 1_000.0)
}

/// Errors produced by the embeddings crate.
#[derive(Debug, Error)]
pub enum EmbeddingError {
    #[error("embedding request must contain at least one input")]
    EmptyRequest,
    #[error("embedding request contains a blank input")]
    BlankInput,
    #[error("embedding backend has not been initialized for model `{model_id}`")]
    NotInitialized { model_id: String },
    #[error("offline startup failed for model `{model_id}`: {detail}")]
    OfflineStartup { model_id: String, detail: String },
    #[error("embedding backend failed for `{model_id}`: {message}")]
    Backend { model_id: String, message: String },
    #[error("embedding cache read failed at {path}: {source}")]
    CacheRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("embedding dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },
    #[error("provider contract violation: {0}")]
    ProviderContract(String),
    #[error("benchmark configuration error: {0}")]
    Benchmark(String),
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        EmbeddingBenchmark, EmbeddingError, EmbeddingProvider, EmbeddingRequest, EmbeddingResponse,
        FastembedProvider, FastembedProviderConfig, ResolvedEmbeddingProfile,
        StartupValidationStatus, build_init_options, effective_cache_root_from_hf_home, env_flag,
        env_flag_value, percentile_millis, resolve_fastembed_model_layout, validate_cache,
    };
    use std::env;
    use std::fs;
    use std::path::Path;
    use std::time::Duration;

    use fastembed::EmbeddingModel;
    use mempalace_core::EmbeddingProfile;
    use tempfile::tempdir;

    struct StubProvider {
        profile: ResolvedEmbeddingProfile,
        response: Vec<Vec<f32>>,
        calls: usize,
    }

    impl EmbeddingProvider for StubProvider {
        fn profile(&self) -> &'static mempalace_core::EmbeddingProfileMetadata {
            self.profile.metadata
        }

        fn startup_validation(&self) -> super::Result<super::StartupValidation> {
            unreachable!("startup validation is not used by the benchmark tests")
        }

        fn embed(&mut self, request: &EmbeddingRequest) -> super::Result<EmbeddingResponse> {
            self.calls += 1;
            let vectors = self.response.iter().take(request.len()).cloned().collect::<Vec<_>>();
            EmbeddingResponse::new(
                vectors,
                self.profile.metadata.dimensions,
                self.profile.metadata.profile,
                self.profile.metadata.model_id,
            )
        }
    }

    #[test]
    fn request_rejects_empty_inputs() {
        let err = EmbeddingRequest::new(Vec::new()).unwrap_err();
        assert!(matches!(err, EmbeddingError::EmptyRequest));
    }

    #[test]
    fn request_rejects_blank_inputs() {
        let err = EmbeddingRequest::new(vec![" ".to_owned()]).unwrap_err();
        assert!(matches!(err, EmbeddingError::BlankInput));
    }

    #[test]
    fn balanced_profile_resolution_is_locked() {
        let resolved = ResolvedEmbeddingProfile::from_profile(EmbeddingProfile::Balanced);
        assert_eq!(resolved.metadata.model_id, "sentence-transformers/all-MiniLM-L6-v2");
        assert_eq!(resolved.metadata.dimensions, 384);
        assert_eq!(resolved.warm_query_p95_budget_ms, 750);
    }

    #[test]
    fn low_cpu_profile_resolution_is_locked() {
        let resolved = ResolvedEmbeddingProfile::from_profile(EmbeddingProfile::LowCpu);
        assert_eq!(resolved.metadata.model_id, "Xenova/all-MiniLM-L6-v2");
        assert_eq!(resolved.metadata.dimensions, 384);
        assert_eq!(resolved.warm_query_p95_budget_ms, 1_500);
        assert_eq!(resolved.low_cpu_idle_rss_budget_mb, Some(450));
        assert_eq!(resolved.low_cpu_ingest_rss_budget_mb, Some(850));
    }

    #[test]
    fn low_cpu_profile_maps_to_fastembed_quantized_minilm() {
        let options = build_init_options(
            ResolvedEmbeddingProfile::from_profile(EmbeddingProfile::LowCpu),
            &FastembedProviderConfig::new("cache"),
            Path::new("cache"),
        )
        .unwrap();

        assert_eq!(options.model_name, EmbeddingModel::AllMiniLML6V2Q);
    }

    #[test]
    fn validation_reports_warm_cache() {
        let dir = tempdir().unwrap();
        write_fastembed_snapshot(dir.path(), EmbeddingProfile::Balanced);

        let report = validate_cache(
            ResolvedEmbeddingProfile::from_profile(EmbeddingProfile::Balanced),
            dir.path(),
        )
        .unwrap();

        assert_eq!(report.status, StartupValidationStatus::Ready);
        assert!(report.is_ready());
    }

    #[test]
    fn validation_reports_missing_assets() {
        let dir = tempdir().unwrap();
        let report = validate_cache(
            ResolvedEmbeddingProfile::from_profile(EmbeddingProfile::Balanced),
            dir.path(),
        )
        .unwrap();

        assert_eq!(report.status, StartupValidationStatus::MissingAssets);
    }

    #[test]
    fn validation_reports_partial_download() {
        let dir = tempdir().unwrap();
        let snapshot_root = write_fastembed_snapshot(dir.path(), EmbeddingProfile::Balanced);
        fs::remove_file(snapshot_root.join("tokenizer_config.json")).unwrap();

        let report = validate_cache(
            ResolvedEmbeddingProfile::from_profile(EmbeddingProfile::Balanced),
            dir.path(),
        )
        .unwrap();

        assert_eq!(report.status, StartupValidationStatus::PartialDownload);
    }

    #[test]
    fn validation_reports_corrupted_cache_for_invalid_json() {
        let dir = tempdir().unwrap();
        let snapshot_root = write_fastembed_snapshot(dir.path(), EmbeddingProfile::Balanced);
        fs::write(snapshot_root.join("special_tokens_map.json"), "{not-json").unwrap();

        let report = validate_cache(
            ResolvedEmbeddingProfile::from_profile(EmbeddingProfile::Balanced),
            dir.path(),
        )
        .unwrap();

        assert_eq!(report.status, StartupValidationStatus::CorruptedCache);
    }

    #[test]
    fn validation_reports_corrupted_cache_for_zero_length_model_file() {
        let dir = tempdir().unwrap();
        let snapshot_root = write_fastembed_snapshot(dir.path(), EmbeddingProfile::LowCpu);
        let model_layout = resolve_fastembed_model_layout(ResolvedEmbeddingProfile::from_profile(
            EmbeddingProfile::LowCpu,
        ))
        .unwrap();
        fs::write(snapshot_root.join(model_layout.model_file), "").unwrap();

        let report = validate_cache(
            ResolvedEmbeddingProfile::from_profile(EmbeddingProfile::LowCpu),
            dir.path(),
        )
        .unwrap();

        assert_eq!(report.status, StartupValidationStatus::CorruptedCache);
    }

    #[test]
    fn validation_rejects_missing_main_ref_even_with_complete_snapshot() {
        let dir = tempdir().unwrap();
        let model_root = write_fastembed_snapshot(dir.path(), EmbeddingProfile::Balanced)
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        fs::remove_file(model_root.join("refs/main")).unwrap();

        let report = validate_cache(
            ResolvedEmbeddingProfile::from_profile(EmbeddingProfile::Balanced),
            dir.path(),
        )
        .unwrap();

        assert_eq!(report.status, StartupValidationStatus::PartialDownload);
    }

    #[test]
    fn validation_rejects_stale_main_ref_even_with_complete_alternate_snapshot() {
        let dir = tempdir().unwrap();
        let model_root = write_fastembed_snapshot(dir.path(), EmbeddingProfile::Balanced)
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        let alternate_snapshot = model_root.join("snapshots").join("alternate-revision");
        fs::create_dir_all(&alternate_snapshot).unwrap();
        fs::write(model_root.join("refs/main"), "missing-revision\n").unwrap();

        let report = validate_cache(
            ResolvedEmbeddingProfile::from_profile(EmbeddingProfile::Balanced),
            dir.path(),
        )
        .unwrap();

        assert_eq!(report.status, StartupValidationStatus::PartialDownload);
    }

    #[test]
    fn validation_uses_selected_model_cache_layout() {
        let dir = tempdir().unwrap();
        write_fastembed_snapshot(dir.path(), EmbeddingProfile::Balanced);

        let report = validate_cache(
            ResolvedEmbeddingProfile::from_profile(EmbeddingProfile::LowCpu),
            dir.path(),
        )
        .unwrap();

        assert_eq!(report.status, StartupValidationStatus::MissingAssets);
    }

    #[test]
    fn env_flag_accepts_only_explicit_truthy_values() {
        assert!(!env_flag_value("0"));
        assert!(!env_flag_value("false"));
        assert!(!env_flag_value("no"));
        assert!(!env_flag_value(""));
        assert!(env_flag_value("1"));
        assert!(env_flag_value("true"));
        assert!(env_flag_value("TRUE"));
        assert!(env_flag_value("yes"));
        assert!(env_flag_value("YES"));
        // Absent env var resolves to false.
        assert!(!env_flag("MEMPALACE_EMBEDDINGS_ENV_FLAG_TEST_ABSENT_XYZ"));
    }

    #[test]
    fn provider_refuses_offline_startup_without_assets() {
        let dir = tempdir().unwrap();
        let provider = FastembedProvider::new(
            EmbeddingProfile::Balanced,
            FastembedProviderConfig::new(dir.path()),
        );

        let err = provider.try_initialize().unwrap_err();
        assert!(matches!(err, EmbeddingError::OfflineStartup { .. }));
    }

    #[test]
    fn provider_refuses_offline_startup_with_mixed_cache() {
        let dir = tempdir().unwrap();
        write_fastembed_snapshot(dir.path(), EmbeddingProfile::Balanced);
        let provider = FastembedProvider::new(
            EmbeddingProfile::LowCpu,
            FastembedProviderConfig::new(dir.path()),
        );

        let err = provider.try_initialize().unwrap_err();
        assert!(matches!(err, EmbeddingError::OfflineStartup { .. }));
    }

    #[test]
    fn provider_refuses_offline_startup_with_partial_download() {
        let dir = tempdir().unwrap();
        let snapshot_root = write_fastembed_snapshot(dir.path(), EmbeddingProfile::Balanced);
        fs::remove_file(snapshot_root.join("tokenizer.json")).unwrap();

        let provider = FastembedProvider::new(
            EmbeddingProfile::Balanced,
            FastembedProviderConfig::new(dir.path()),
        );

        let err = provider.try_initialize().unwrap_err();
        assert!(matches!(err, EmbeddingError::OfflineStartup { .. }));
    }

    #[test]
    fn ready_cache_builds_offline_runtime_options() {
        let dir = tempdir().unwrap();
        write_fastembed_snapshot(dir.path(), EmbeddingProfile::Balanced);

        let provider = FastembedProvider::new(
            EmbeddingProfile::Balanced,
            FastembedProviderConfig::new(dir.path()),
        );
        let validation = provider.startup_validation().unwrap();
        let options = build_init_options(
            ResolvedEmbeddingProfile::from_profile(EmbeddingProfile::Balanced),
            &FastembedProviderConfig::new(dir.path()),
            dir.path(),
        )
        .unwrap();

        assert_eq!(validation.status, StartupValidationStatus::Ready);
        assert_eq!(options.cache_dir, dir.path());
    }

    #[test]
    fn hf_home_override_is_applied_consistently_for_validation_and_runtime() {
        let configured_root = tempdir().unwrap();
        let hf_home_root = tempdir().unwrap();
        write_fastembed_snapshot(hf_home_root.path(), EmbeddingProfile::Balanced);

        let config = FastembedProviderConfig::new(configured_root.path());
        let effective_root =
            effective_cache_root_from_hf_home(&config, Some(hf_home_root.path().as_os_str()));
        let validation = validate_cache(
            ResolvedEmbeddingProfile::from_profile(EmbeddingProfile::Balanced),
            &effective_root,
        )
        .unwrap();
        let options = build_init_options(
            ResolvedEmbeddingProfile::from_profile(EmbeddingProfile::Balanced),
            &config,
            &effective_root,
        )
        .unwrap();

        assert_eq!(validation.status, StartupValidationStatus::Ready);
        assert_eq!(validation.cache_root, hf_home_root.path());
        assert_eq!(options.cache_dir, hf_home_root.path());
    }

    #[test]
    fn provider_requires_initialization_before_embedding() {
        let dir = tempdir().unwrap();
        let mut provider = FastembedProvider::new(
            EmbeddingProfile::Balanced,
            FastembedProviderConfig::new(dir.path()),
        );
        let request = EmbeddingRequest::new(vec!["hello".to_owned()]).unwrap();

        let err = provider.embed(&request).unwrap_err();
        assert!(matches!(err, EmbeddingError::NotInitialized { .. }));
    }

    #[test]
    fn benchmark_captures_p95() {
        let request = EmbeddingRequest::new(vec!["hello".to_owned()]).unwrap();
        let mut provider = StubProvider {
            profile: ResolvedEmbeddingProfile::from_profile(EmbeddingProfile::Balanced),
            response: vec![vec![0.0; 384]],
            calls: 0,
        };

        let benchmark = EmbeddingBenchmark::measure(&mut provider, &request, 3).unwrap();
        assert_eq!(benchmark.samples.len(), 3);
        assert_eq!(provider.calls, 6);
        assert!(benchmark.p95_millis().unwrap() >= 0.0);
    }

    #[test]
    fn benchmark_excludes_warmup_requests_from_samples() {
        let request = EmbeddingRequest::new(vec!["hello".to_owned()]).unwrap();
        let mut provider = StubProvider {
            profile: ResolvedEmbeddingProfile::from_profile(EmbeddingProfile::Balanced),
            response: vec![vec![0.0; 384]],
            calls: 0,
        };

        let benchmark =
            EmbeddingBenchmark::measure_with_warmup(&mut provider, &request, 2, 4).unwrap();

        assert_eq!(provider.calls, 6);
        assert_eq!(benchmark.samples.len(), 4);
    }

    #[test]
    fn percentile_uses_upper_rank() {
        let samples = vec![
            Duration::from_millis(10),
            Duration::from_millis(20),
            Duration::from_millis(30),
            Duration::from_millis(40),
        ];

        assert_eq!(percentile_millis(&samples, 95.0), Some(40.0));
    }

    fn write_fastembed_snapshot(root: &Path, profile: EmbeddingProfile) -> std::path::PathBuf {
        let layout =
            resolve_fastembed_model_layout(ResolvedEmbeddingProfile::from_profile(profile))
                .unwrap();
        let model_root = root.join(&layout.model_cache_dir);
        let snapshot_root = model_root.join("snapshots").join("test-revision");
        let model_parent = layout.model_file.parent().unwrap_or_else(|| Path::new(""));

        fs::create_dir_all(snapshot_root.join(model_parent)).unwrap();
        fs::create_dir_all(model_root.join("refs")).unwrap();
        fs::write(model_root.join("refs/main"), "test-revision\n").unwrap();

        fs::write(snapshot_root.join(&layout.model_file), "onnx").unwrap();
        fs::write(snapshot_root.join("tokenizer.json"), "{\"version\":\"1.0\"}").unwrap();
        fs::write(snapshot_root.join("config.json"), "{\"pad_token_id\":0}").unwrap();
        fs::write(snapshot_root.join("special_tokens_map.json"), "{}").unwrap();
        fs::write(
            snapshot_root.join("tokenizer_config.json"),
            "{\"model_max_length\":512,\"pad_token\":\"[PAD]\"}",
        )
        .unwrap();

        snapshot_root
    }
}

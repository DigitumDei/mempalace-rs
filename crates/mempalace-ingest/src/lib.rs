#![allow(missing_docs)]

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Read as _;
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt as _;
use std::path::{Component, Path, PathBuf};
use std::process::Command;

use mempalace_config::{ConfigLoader, ProjectConfig, ProjectRoomConfig};
use mempalace_core::{DrawerId, DrawerRecord, RoomId, SourceLocator, WingId};
use mempalace_embeddings::{EmbeddingProvider, EmbeddingRequest};
pub use mempalace_federation;
use mempalace_federation::{IngestChunkDto, IngestFileDto};
use mempalace_storage::core::MempalaceError;
use mempalace_storage::{DrawerFilter, DrawerStore, IngestManifestStore, StorageEngine};
use serde_json::Value;
use thiserror::Error;
use time::{Date, OffsetDateTime};

pub use mempalace_config as config;
pub use mempalace_core as core;
pub use mempalace_embeddings as embeddings;
pub use mempalace_storage as storage;

const PROJECT_CHUNK_SIZE: usize = 800;
const PROJECT_CHUNK_OVERLAP: usize = 100;
const PROJECT_MIN_CHUNK_SIZE: usize = 50;
const CONVO_MIN_CHUNK_SIZE: usize = 30;
const LARGE_FILE_TRUNCATION_BYTES: usize = 200_000;

const PROJECT_READABLE_EXTENSIONS: &[&str] = &[
    // Text / markup
    ".txt", ".md", ".html", ".xml", ".csv", ".json", ".yaml", ".yml", ".toml", ".ini", ".cfg",
    ".conf", ".properties",
    // Web / frontend
    ".js", ".ts", ".jsx", ".tsx", ".css", ".vue", ".svelte", ".astro",
    // Systems languages
    ".rs", ".c", ".h", ".cc", ".cpp", ".cxx", ".hh", ".hpp", ".m", ".mm", ".zig", ".nim",
    // JVM / Android
    ".java", ".kt", ".kts", ".scala", ".sbt", ".groovy", ".gradle",
    // .NET
    ".cs", ".fs", ".fsi", ".fsx",
    // Scripting / dynamic
    ".py", ".rb", ".php", ".lua", ".pl", ".pm", ".r", ".jl", ".dart",
    // Shell
    ".sh", ".bash", ".zsh", ".fish", ".ps1", ".psm1", ".psd1", ".bat", ".cmd",
    // Functional / BEAM
    ".ex", ".exs", ".erl", ".hrl", ".clj", ".cljc", ".cljs", ".edn",
    // Mobile / other
    ".swift", ".go",
    // SQL / data
    ".sql",
    // IaC / config
    ".tf", ".tfvars", ".hcl", ".proto", ".graphql", ".gql", ".dockerfile",
];
const CONVO_EXTENSIONS: &[&str] = &[".txt", ".md", ".json", ".jsonl"];
const DEFAULT_SKIP_DIRS: &[&str] = &[
    ".git",
    "node_modules",
    "__pycache__",
    ".venv",
    "venv",
    "env",
    "dist",
    "build",
    ".next",
    "coverage",
    ".mempalace",
];
/// Exact file names (case-sensitive) that are always skipped in project discovery.
/// This is the non-secret hygiene list (lockfiles, palace config); the
/// secret-shaped path denylist (issue #95) is matched separately in
/// [`secret_path_kind`].
const PROJECT_SKIP_FILES: &[&str] = &[
    "mempalace.yaml",
    "mempalace.yml",
    "mempal.yaml",
    "mempal.yml",
    ".gitignore",
    // Lockfiles
    "package-lock.json",
    "Cargo.lock",
    "yarn.lock",
    "pnpm-lock.yaml",
    "poetry.lock",
    "composer.lock",
    "Gemfile.lock",
];

/// Extensionless file names (case-sensitive) that are always included in project discovery.
const PROJECT_READABLE_BASENAMES: &[&str] =
    &["Dockerfile", "Makefile", "Rakefile", "Gemfile", "Jenkinsfile", "Vagrantfile"];

/// Binary-sniff prefix size: if any of the first N bytes is NUL (0x00) → binary.
const BINARY_SNIFF_BYTES: usize = 8192;
/// Shebang-check prefix size for extensionless files.
const SHEBANG_READ_BYTES: usize = 256;

const TOPIC_KEYWORDS: &[(&str, &[&str])] = &[
    (
        "technical",
        &[
            "code", "python", "function", "bug", "error", "api", "database", "server", "deploy",
            "git", "test", "debug", "refactor",
        ],
    ),
    (
        "architecture",
        &[
            "architecture",
            "design",
            "pattern",
            "structure",
            "schema",
            "interface",
            "module",
            "component",
            "service",
            "layer",
        ],
    ),
    (
        "planning",
        &[
            "plan",
            "roadmap",
            "milestone",
            "deadline",
            "priority",
            "sprint",
            "backlog",
            "scope",
            "requirement",
            "spec",
        ],
    ),
    (
        "decisions",
        &[
            "decided",
            "chose",
            "picked",
            "switched",
            "migrated",
            "replaced",
            "trade-off",
            "alternative",
            "option",
            "approach",
        ],
    ),
    (
        "problems",
        &[
            "problem",
            "issue",
            "broken",
            "failed",
            "crash",
            "stuck",
            "workaround",
            "fix",
            "solved",
            "resolved",
        ],
    ),
];

const DECISION_MARKERS: &[&str] = &[
    "let's use",
    "let's go with",
    "let's try",
    "we should",
    "we decided",
    "we chose",
    "we went with",
    "instead of",
    "rather than",
    "because",
    "trade-off",
    "tradeoff",
    "pros and cons",
    "architecture",
    "approach",
    "strategy",
    "pattern",
    "stack",
    "framework",
    "configure",
    "default",
];
const PREFERENCE_MARKERS: &[&str] = &[
    "i prefer",
    "always use",
    "never use",
    "don't use",
    "i like",
    "i hate",
    "please always",
    "please never",
    "my preference is",
    "my style is",
    "we always",
    "we never",
    "snake_case",
    "camelcase",
    "tabs",
    "spaces",
];
const MILESTONE_MARKERS: &[&str] = &[
    "it works",
    "it worked",
    "got it working",
    "fixed",
    "solved",
    "breakthrough",
    "figured it out",
    "finally",
    "discovered",
    "realized",
    "turns out",
    "built",
    "created",
    "implemented",
    "shipped",
    "launched",
    "deployed",
    "released",
    "prototype",
    "proof of concept",
    "demo",
];
const PROBLEM_MARKERS: &[&str] = &[
    "bug",
    "error",
    "crash",
    "fail",
    "broke",
    "broken",
    "issue",
    "problem",
    "doesn't work",
    "not working",
    "root cause",
    "workaround",
    "the fix",
    "that's why",
    "solution",
    "patched",
];
const EMOTION_MARKERS: &[&str] = &[
    "love",
    "scared",
    "afraid",
    "proud",
    "hurt",
    "happy",
    "sad",
    "cry",
    "crying",
    "miss",
    "sorry",
    "grateful",
    "angry",
    "worried",
    "lonely",
    "beautiful",
    "amazing",
    "wonderful",
    "i feel",
    "i love you",
    "i'm sorry",
    "i wish",
    "nobody knows",
];

const POSITIVE_WORDS: &[&str] = &[
    "pride",
    "proud",
    "joy",
    "happy",
    "love",
    "beautiful",
    "amazing",
    "wonderful",
    "breakthrough",
    "success",
    "works",
    "working",
    "solved",
    "fixed",
    "grateful",
];
const NEGATIVE_WORDS: &[&str] = &[
    "bug", "error", "crash", "failed", "broken", "issue", "problem", "stuck", "blocked", "missing",
    "terrible", "panic", "disaster",
];

const TYPO_CORRECTIONS: &[(&str, &str)] = &[
    ("lsresdy", "already"),
    ("alredy", "already"),
    ("knoe", "know"),
    ("befor", "before"),
    ("befroe", "before"),
    ("meny", "many"),
    ("diferent", "different"),
    ("tesing", "testing"),
    ("pleese", "please"),
    ("chekc", "check"),
    ("realy", "really"),
    ("writte", "write"),
];

#[derive(Debug, Error)]
pub enum IngestError {
    #[error(transparent)]
    Core(#[from] MempalaceError),
    #[error(transparent)]
    Storage(#[from] mempalace_storage::StorageError),
    #[error(transparent)]
    Embeddings(#[from] mempalace_embeddings::EmbeddingError),
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid relative path for `{path}`")]
    InvalidRelativePath { path: PathBuf },
    #[error(
        "branch-delta mining requires a git repository with a detectable default branch: {reason}"
    )]
    BranchDeltaUnavailable { reason: String },
    #[error("could not enumerate the tracked index for `{path}`: {reason}")]
    GitIndexUnavailable { path: PathBuf, reason: String },
}

pub type Result<T> = std::result::Result<T, IngestError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversationExtractMode {
    Exchange,
    General,
}

impl ConversationExtractMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Exchange => "exchange",
            Self::General => "general",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IngestSummary {
    pub discovered_files: usize,
    pub ignored_files: usize,
    pub unreadable_files: usize,
    pub malformed_files: usize,
    pub skipped_unchanged: usize,
    pub ingested_files: usize,
    pub drawers_written: usize,
    pub truncated_files: usize,
    /// Number of previously-mined source keys removed during a branch cleanup
    /// pass.  Always 0 for non-branch runs.
    pub removed_sources: usize,
    /// The view name detected/used for this mine, if any.  `None` for canonical
    /// or non-Git mines.
    pub view_name: Option<String>,
    /// Secret-shaped paths withheld by the path denylist during discovery
    /// (issue #95). Carries path and reason only; never file content.
    pub secret_path_skips: Vec<ProjectSourceSkip>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectIngestRequest {
    pub project_dir: PathBuf,
    pub wing: Option<String>,
    pub agent: String,
    pub limit: Option<usize>,
    pub dry_run: bool,
    pub reindex: bool,
    pub max_embed_batch_size: Option<usize>,
    /// When `true`, only files in the git delta (changed vs merge-base with the
    /// default branch, plus untracked) are mined.  Uses the `projects-branch`
    /// source-key namespace.  Returns [`IngestError::BranchDeltaUnavailable`]
    /// when no git repo or default branch is found.
    pub branch: bool,
    /// Explicit view/ref name for this mine.  When set, overrides the branch
    /// name derived from `branch: true`.  Use `"canonical"` to force a full
    /// canonical mine even when the checkout is on a non-canonical ref.
    pub view: Option<String>,
}

impl ProjectIngestRequest {
    pub fn new(project_dir: impl AsRef<Path>) -> Self {
        Self {
            project_dir: project_dir.as_ref().to_path_buf(),
            wing: None,
            agent: "mempalace-rs".to_owned(),
            limit: None,
            dry_run: false,
            reindex: false,
            max_embed_batch_size: None,
            branch: false,
            view: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationIngestRequest {
    pub convo_dir: PathBuf,
    pub wing: Option<String>,
    pub agent: String,
    pub extract_mode: ConversationExtractMode,
    pub limit: Option<usize>,
    pub dry_run: bool,
    pub reindex: bool,
    pub max_embed_batch_size: Option<usize>,
}

impl ConversationIngestRequest {
    pub fn new(convo_dir: impl AsRef<Path>) -> Self {
        Self {
            convo_dir: convo_dir.as_ref().to_path_buf(),
            wing: None,
            agent: "mempalace-rs".to_owned(),
            extract_mode: ConversationExtractMode::Exchange,
            limit: None,
            dry_run: false,
            reindex: false,
            max_embed_batch_size: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub content: String,
    pub chunk_index: u32,
    pub room_hint: Option<String>,
    pub date_hint: Option<Date>,
    /// Byte range within the original file bytes (start inclusive, end exclusive).
    /// Only set for project chunks from valid-UTF-8 files.
    pub byte_range: Option<(u64, u64)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MessageRole {
    User,
    Assistant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Message {
    role: MessageRole,
    content: String,
    timestamp: Option<OffsetDateTime>,
    speaker_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IgnorePattern {
    /// `true` when the pattern starts with `!`: a match re-includes paths that
    /// an earlier, lower-precedence rule excluded.
    negated: bool,
    /// `true` when the pattern ends with `/` and therefore only matches
    /// directories (and, through the walk, everything beneath them).
    directory_only: bool,
    /// `true` when the pattern is anchored to the directory its ignore file
    /// lives in (the pattern contains a `/`, or begins with one). Unanchored
    /// patterns match the basename at any depth, like git.
    anchored: bool,
    /// The glob pattern split into `/`-separated components.
    parts: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IgnoreRule {
    pattern: IgnorePattern,
    /// Relative directory the rule was declared in (`""` for the root).
    scope: String,
    /// Number of path components in `scope` (0 for the root).
    scope_depth: usize,
    /// Source precedence of the rule; later (higher-precedence) sources can
    /// override earlier ones.
    tier: IgnoreTier,
    /// Load order within the same scope; later rules take precedence.
    order: usize,
}

/// Source precedence of an ignore rule, mirroring git's precedence order
/// (gitignore(5)): worktree `.gitignore`/`.mempalaceignore` files override
/// `$GIT_DIR/info/exclude`, which overrides the `core.excludesFile` global
/// file. Lower-tier rules sort first so that a matching higher-tier rule wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum IgnoreTier {
    /// `core.excludesFile` (default `$XDG_CONFIG_HOME/git/ignore`).
    GlobalExclude,
    /// `$GIT_DIR/info/exclude`.
    RepoExclude,
    /// Per-directory `.gitignore` / `.mempalaceignore`.
    Worktree,
}

/// Git-compatible ignore matcher: `.gitignore`/`.mempalaceignore` patterns
/// with nested-file scoping, negation, and glob/anchoring semantics, plus
/// `$GIT_DIR/info/exclude` (Git-backed roots only) and the
/// `core.excludesFile` global file (any walk) at the correct precedence, and
/// the built-in always-skipped directories.
#[derive(Debug, Clone)]
struct IgnoreMatcher {
    /// Absolute root all relative paths are resolved against.
    root: PathBuf,
    /// Built-in directory names that are always skipped.
    skip_dirs: BTreeSet<String>,
    /// Rules sorted by `(scope_depth, order)`; the last matching rule wins.
    rules: Vec<IgnoreRule>,
    /// Directories whose ignore files have already been parsed.
    loaded: BTreeSet<PathBuf>,
    next_order: usize,
}

/// A single eligible project source path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectSource {
    /// Absolute path to the source on disk.
    pub absolute_path: PathBuf,
    /// Repository-relative path, `/`-separated, relative to the discovery root.
    pub relative_path: String,
}

/// The source set that produced a [`ProjectSourceDiscovery`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectSourceBasis {
    /// Git-backed root: the eligible subset of tracked index paths
    /// (`git ls-files`). Untracked and ignored working-tree files are never
    /// eligible.
    GitIndex,
    /// Non-Git root: a filesystem walk honouring git-compatible ignore rules.
    Filesystem,
}

/// A candidate path withheld by the path-based secret denylist (issue #95).
///
/// Recorded so operators can see what was withheld rather than auditing after
/// the fact. Carries the path and a reason only — never any file content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectSourceSkip {
    /// Root-relative, `/`-separated path of the withheld candidate.
    pub relative_path: String,
    /// Short human-readable reason describing the denylist match.
    pub reason: String,
}

/// Categories of the path-based secret denylist (issue #95). Matches are made
/// on the file name (case-insensitive) *before* any content is read, so a
/// secret path is withheld without opening the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretPathKind {
    /// `.env` and `.env.*` process-environment files.
    DotEnv,
    /// `*.kubeconfig*` Kubernetes configuration files.
    Kubeconfig,
    /// SSH private keys: `id_rsa*`, `id_ed25519`, `id_ecdsa`, `id_dsa`.
    SshPrivateKey,
    /// Keystores and truststores: `*.pfx`, `*.p12`, `*.jks`.
    Keystore,
    /// Package/registry credential files: `.npmrc`, `.netrc`.
    Netrc,
    /// Terraform state and variable files: `*.tfstate`, `*.tfvars`.
    TerraformSecret,
    /// JSON secret bundles: `secrets*.json`.
    SecretsJson,
    /// Local override/config files that commonly hold credentials:
    /// `*.local.json`.
    LocalJson,
}

impl SecretPathKind {
    /// The denylist pattern(s) this category covers, for operator-facing skip
    /// records.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DotEnv => ".env / *.env",
            Self::Kubeconfig => "*.kubeconfig*",
            Self::SshPrivateKey => "id_rsa* / id_ed25519 / id_ecdsa / id_dsa (SSH private key)",
            Self::Keystore => "*.pfx / *.p12 / *.jks (keystore)",
            Self::Netrc => ".npmrc / .netrc",
            Self::TerraformSecret => "*.tfstate / *.tfvars",
            Self::SecretsJson => "secrets*.json",
            Self::LocalJson => "*.local.json",
        }
    }
}

/// Result of [`discover_project_sources`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectSourceDiscovery {
    /// Eligible sources, sorted by `relative_path` (deterministic order).
    pub sources: Vec<ProjectSource>,
    /// Which source set produced `sources`.
    pub basis: ProjectSourceBasis,
    /// Number of candidate paths skipped (ignored, ineligible, or missing on
    /// disk).
    pub skipped: usize,
    /// Secret-denylist exclusions, in discovery order, for operator
    /// visibility. Path and reason only; never file content.
    pub skips: Vec<ProjectSourceSkip>,
}

/// A single chunk produced by [`prepare_file_chunks`], with all byte/line
/// offsets already adjusted to be file-absolute (not trimmed-string-relative).
#[derive(Debug, Clone)]
struct PreparedChunk {
    text: String,
    chunk_index: u32,
    room: String,
    /// Present iff the file is valid UTF-8.
    byte_start: Option<u64>,
    byte_end: Option<u64>,
    line_start: Option<u32>,
    line_end: Option<u32>,
}

/// Output of the pure per-file preparation step (no embedding, no I/O beyond
/// reading the file).
#[derive(Debug, Clone)]
struct PreparedFileChunks {
    relative_path: String,
    /// `project_ingest_content_hash` of the file (document hash × routing fingerprint).
    content_hash: String,
    /// `Some(document.content_hash)` when the file is valid UTF-8 (locator basis).
    file_hash: Option<String>,
    chunks: Vec<PreparedChunk>,
    truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ConversationNormalizeError {
    Malformed,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedConversation {
    transcript: String,
    messages: Vec<Message>,
}

pub async fn ingest_project<P: EmbeddingProvider>(
    engine: &StorageEngine,
    provider: &mut P,
    request: &ProjectIngestRequest,
) -> Result<IngestSummary> {
    let root = request
        .project_dir
        .canonicalize()
        .map_err(|source| IngestError::Io { path: request.project_dir.clone(), source })?;
    let derived_wing = request
        .wing
        .clone()
        .unwrap_or_else(|| derived_project_wing(&root));
    let project_id = derive_project_id(&root, &derived_wing, None);
    let config = ConfigLoader::resolve_project_config(
        &root,
        None,
        Some(&project_id),
        &derived_wing,
        Vec::new(),
    )?;
    let repo_id = ConfigLoader::find_project_id(None, &root, Some(&project_id))?
        .unwrap_or_else(|| derive_project_id(&root, &config.wing, None));
    ingest_project_with_config(engine, provider, request, &config, Some(&repo_id)).await
}

/// Mine a project using a project declaration resolved by the caller.
///
/// The CLI uses this entry point so its centralized registry can be selected
/// from the active configuration base directory.  `project_id` should remain
/// stable across clones and worktrees when available.
pub async fn ingest_project_with_config<P: EmbeddingProvider>(
    engine: &StorageEngine,
    provider: &mut P,
    request: &ProjectIngestRequest,
    config: &ProjectConfig,
    project_id: Option<&str>,
) -> Result<IngestSummary> {
    let root = request
        .project_dir
        .canonicalize()
        .map_err(|source| IngestError::Io { path: request.project_dir.clone(), source })?;
    let wing_name = request.wing.clone().unwrap_or_else(|| config.wing.clone());
    let wing_id = wing_id(&wing_name)?;
    let repo_id = project_id
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| derive_repo_id(&root, &wing_name));
    let project_root_key = stable_project_root_key(&repo_id);
    let legacy_root_key = hash_text(&root.to_string_lossy());
    // An explicit canonical view always wins, including for direct library
    // callers that did not pass through the CLI's flag normalization.
    let branch_mode = request.branch && request.view.as_deref() != Some("canonical");
    let branch_name = match branch_mode {
        true => Some(request.view.clone().unwrap_or_else(|| {
            resolve_current_branch(&root).unwrap_or_else(|| "detached".to_owned())
        })),
        false => None,
    };
    // Branch-delta mining deliberately mines untracked (non-ignored) files, so
    // it keeps the filesystem walk; canonical mines use the safe tracked-index
    // source set.
    let discovered = if branch_mode {
        discover_project_files_with_untracked(&root)?
    } else {
        discover_project_files(&root)?
    };
    let routing_fingerprint = project_routing_fingerprint(&config.rooms);

    // Resolve the git commit hash once per mine run (None if not in a repo).
    let commit_hash = resolve_commit_hash(&root);

    // Resolve root as a string for locators (use to_string_lossy for Windows verbatim paths).
    let resolve_root = root.to_string_lossy().into_owned();

    // Build repository-view metadata for project-mined drawers.
    let default_branch = detect_default_branch(&root);
    let merge_base = default_branch
        .as_ref()
        .and_then(|b| compute_merge_base(&root, b));
    let worktree_id = hash_text(&root.to_string_lossy());
    let view_metadata = mempalace_core::RepositoryViewMetadata {
        repo_id: repo_id.clone(),
        view_name: branch_name.clone(),
        source_path: resolve_root.clone(),
        head_commit: commit_hash.clone(),
        base_ref: default_branch,
        merge_base,
        worktree_id,
        path_state: "present".to_owned(),
    };

    let mut summary = IngestSummary::default();
    summary.discovered_files = discovered.files.len();
    summary.ignored_files = discovered.ignored_files;
    summary.secret_path_skips = discovered.skips;
    summary.view_name = branch_name.clone();

    // For branch mode: compute the delta set and filter files.
    let (ingest_kind, delta_set) = if branch_mode {
        let delta = compute_branch_delta(&root)?;
        let set: BTreeSet<String> = delta.into_iter().collect();
        ("projects-branch", Some(set))
    } else {
        ("projects", None)
    };

    let discovered_files = if let Some(ref delta) = delta_set {
        discovered
            .files
            .into_iter()
            .filter(|f| delta.contains(&f.relative_path))
            .collect::<Vec<_>>()
    } else {
        discovered.files
    };

    let files: Vec<ProjectSource> = apply_limit(discovered_files, request.limit).collect();

    for file in &files {
        match prepare_file_chunks(file, &routing_fingerprint, &config.rooms) {
            Ok(prepared) => {
                let sk = branch_name.as_deref().map_or_else(
                    || project_source_key(ingest_kind, &project_root_key, &wing_name, &file.relative_path),
                    |branch| {
                        project_branch_source_key(
                            ingest_kind,
                            &project_root_key,
                            &wing_name,
                            branch,
                            &file.relative_path,
                        )
                    },
                );
                let looked_up = engine.operational_store().get_ingested_file(&sk)?;
                let current_existing = looked_up.as_ref().filter(|record| record.source_key == sk);
                let migrated_source_key = if branch_name.is_none() {
                    let legacy_sk = legacy_project_source_key(
                        ingest_kind,
                        &legacy_root_key,
                        &wing_name,
                        &file.relative_path,
                    );
                    let legacy_record = if let Some(record) =
                        looked_up.as_ref().filter(|record| record.source_key != sk)
                    {
                        Some(record.clone())
                    } else {
                        engine
                            .operational_store()
                            .get_ingested_file(&legacy_sk)?
                            .filter(|record| record.source_key != sk)
                    };
                    legacy_record.map(|record| record.source_key)
                } else {
                    None
                };
                if !request.reindex {
                    if let Some(existing) = current_existing {
                        if existing.content_hash == prepared.content_hash {
                            if !request.dry_run {
                                if let Some(old_key) = migrated_source_key.as_deref() {
                                    engine.remove_source_key(old_key).await?;
                                }
                            }
                            summary.skipped_unchanged += 1;
                            continue;
                        }
                    }
                }

                if prepared.chunks.is_empty() {
                    if !request.dry_run {
                        replace_source_drawers(
                            engine,
                            &sk,
                            &file.relative_path,
                            ingest_kind,
                            prepared.content_hash,
                            Vec::new(),
                        )
                        .await?;
                        if let Some(old_key) = migrated_source_key.as_deref() {
                            engine.remove_source_key(old_key).await?;
                        }
                    }
                    summary.ingested_files += 1;
                    summary.truncated_files += usize::from(prepared.truncated);
                    continue;
                }

                // Convert PreparedChunk → Chunk and build the locator context.
                let (chunks_with_room, maybe_ctx_storage) = prepared_chunks_to_ingest(
                    &prepared,
                    &resolve_root,
                    commit_hash.as_deref(),
                );
                let ctx_borrow =
                    maybe_ctx_storage.as_ref().map(PreparedLocatorStorage::as_ctx);

                let source_drawers = build_drawers(
                    provider,
                    &wing_id,
                    &sk,
                    &file.relative_path,
                    ingest_kind,
                    None,
                    &request.agent,
                    request.max_embed_batch_size,
                    chunks_with_room,
                    ctx_borrow.as_ref(),
                    branch_name.as_deref(),
                    Some(&view_metadata),
                )?;
                let drawer_count = source_drawers.len();

                if !request.dry_run {
                    replace_source_drawers(
                        engine,
                        &sk,
                        &file.relative_path,
                        ingest_kind,
                        prepared.content_hash,
                        source_drawers,
                    )
                    .await?;
                    if let Some(old_key) = migrated_source_key.as_deref() {
                        engine.remove_source_key(old_key).await?;
                    }
                }

                summary.ingested_files += 1;
                summary.drawers_written += drawer_count;
                summary.truncated_files += usize::from(prepared.truncated);
            }
            Err(IngestError::Io { .. }) => {
                summary.unreadable_files += 1;
            }
            Err(error) => return Err(error),
        }
    }

    let deleted_paths = if branch_mode && !request.dry_run {
        compute_deleted_branch_paths(&root)?
    } else {
        BTreeSet::new()
    };

    // Deleted paths are not discovered from the worktree, so record a durable
    // branch row for each one. These rows shadow the corresponding canonical
    // path during branch-view composition without depending on vector ranking.
    if branch_mode && !request.dry_run {
        let mut tombstone_metadata = view_metadata.clone();
        tombstone_metadata.path_state = "deleted".to_owned();
        let branch = branch_name.as_deref().expect("branch mode has a view name");
        // Fetch all existing tombstones at once. A deletion-heavy branch must
        // not turn one mine into a sequential storage query per path.
        let existing_tombstones = if deleted_paths.is_empty() {
            BTreeSet::new()
        } else {
            let mut tombstones = BTreeSet::new();
            // Bound SQL `IN` predicates so deletion-heavy branches do not exceed
            // the storage backend's query limits.
            for paths in deleted_paths.iter().collect::<Vec<_>>().chunks(500) {
                tombstones.extend(
                    engine
                        .drawer_store()
                        .list_drawers(&DrawerFilter {
                            wing: Some(wing_id.clone()),
                            source_files: paths.iter().map(|path| (*path).clone()).collect(),
                            view: Some(branch.to_owned()),
                            branch_view_only: true,
                            ..DrawerFilter::default()
                        })
                        .await?
                        .into_iter()
                        .filter_map(|drawer| {
                            drawer.view_metadata.as_ref().is_some_and(|metadata| {
                                metadata.repo_id == view_metadata.repo_id
                                    && metadata.path_state == "deleted"
                            }).then_some(drawer.source_file)
                        }),
                );
            }
            tombstones
        };
        let tombstone_embedding = if deleted_paths
            .iter()
            .any(|path| !existing_tombstones.contains(path))
        {
            embed_chunks(
                provider,
                &[Chunk {
                    content: "Deleted branch path tombstone".to_owned(),
                    date_hint: None,
                    room_hint: Some("general".to_owned()),
                    byte_range: None,
                    chunk_index: 0,
                }],
                request.max_embed_batch_size,
            )
        } else {
            Ok(Vec::new())
        }?;
        for relative_path in &deleted_paths {
            let source_key = project_branch_source_key(
                ingest_kind,
                &project_root_key,
                &wing_name,
                branch,
                relative_path,
            );
            if existing_tombstones.contains(relative_path) {
                continue;
            }
            let drawers = build_drawers_from_embeddings(
                &wing_id,
                &source_key,
                relative_path,
                ingest_kind,
                None,
                &request.agent,
                vec![Chunk {
                    content: "Deleted branch path tombstone".to_owned(),
                    date_hint: None,
                    room_hint: Some("general".to_owned()),
                    byte_range: None,
                    chunk_index: 0,
                }],
                None,
                branch_name.as_deref(),
                Some(&tombstone_metadata),
                tombstone_embedding.clone(),
            )?;
            let drawer_count = drawers.len();
            replace_source_drawers(
                engine,
                &source_key,
                &relative_path,
                ingest_kind,
                hash_text("Deleted branch path tombstone"),
                drawers,
            )
            .await?;
            summary.drawers_written += drawer_count;
        }
    }

    // Remove path-hash source rows that were not present in this mine.  This
    // catches files deleted before the stable project-id migration ran.
    if branch_name.is_none() && request.limit.is_none() && !request.dry_run {
        let current_rel_paths: BTreeSet<&str> =
            files.iter().map(|file| file.relative_path.as_str()).collect();
        let legacy_prefix = format!("{ingest_kind}:{wing_name}:{legacy_root_key}:");
        for key in engine
            .operational_store()
            .ingested_source_keys_with_prefix(&legacy_prefix)?
        {
            let rel = key.splitn(4, ':').nth(3).unwrap_or("");
            if !current_rel_paths.contains(rel) {
                engine.remove_source_key(&key).await?;
            }
        }
    }

    // Branch cleanup pass: remove source keys whose relative paths are no longer
    // in the current delta (files reverted to base or deleted from the branch).
    if branch_mode && !request.dry_run {
        let current_rel_paths: BTreeSet<&str> =
            files.iter().map(|f| f.relative_path.as_str()).collect();
        let prefix = branch_name.as_deref().map_or_else(
            || format!("{ingest_kind}:{wing_name}:{project_root_key}:"),
            |branch| format!("{ingest_kind}:{wing_name}:{project_root_key}:{branch}:"),
        );
        let stale_keys =
            engine.operational_store().ingested_source_keys_with_prefix(&prefix)?;
        for key in stale_keys {
            // Key format: projects-branch:{wing}:{root_key}:{branch}:{rel_path}
            // Split off the first 4 ':'-delimited segments to get rel_path.
            let rel = key.splitn(5, ':').nth(4).unwrap_or("");
            if !current_rel_paths.contains(rel) && !deleted_paths.contains(rel) {
                // The path no longer differs from canonical. Remove both a
                // former replacement and a former deletion tombstone.
                engine.remove_source_key(&key).await?;
                summary.removed_sources += 1;
            }
        }
    }

    Ok(summary)
}

pub async fn ingest_conversations<P: EmbeddingProvider>(
    engine: &StorageEngine,
    provider: &mut P,
    request: &ConversationIngestRequest,
) -> Result<IngestSummary> {
    let root = request
        .convo_dir
        .canonicalize()
        .map_err(|source| IngestError::Io { path: request.convo_dir.clone(), source })?;
    let wing_name = request.wing.clone().unwrap_or_else(|| {
        canonicalize_label(root.file_name().and_then(|name| name.to_str()).unwrap_or("convos"))
    });
    let wing_id = wing_id(&wing_name)?;
    let discovered = discover_conversation_files(&root)?;

    let mut summary = IngestSummary::default();
    summary.discovered_files = discovered.files.len();
    summary.ignored_files = discovered.ignored_files;
    summary.secret_path_skips = discovered.skips;
    let files = apply_limit(discovered.files, request.limit);

    for file in files {
        let bytes = match fs::read(&file.absolute_path) {
            Ok(bytes) => bytes,
            Err(source) => {
                summary.unreadable_files += 1;
                let _ = source;
                continue;
            }
        };
        let content_hash = hash_bytes(&bytes);
        let source_key = source_key(
            "convos",
            &root,
            &wing_name,
            Some(request.extract_mode.as_str()),
            &file.relative_path,
        );
        if !request.reindex {
            if let Some(existing) = engine.operational_store().get_ingested_file(&source_key)? {
                if existing.content_hash == content_hash {
                    summary.skipped_unchanged += 1;
                    continue;
                }
            }
        }

        let normalized = match normalize_conversation(&file.absolute_path, &bytes) {
            Ok(normalized) => normalized,
            Err(
                ConversationNormalizeError::Malformed | ConversationNormalizeError::Unsupported,
            ) => {
                summary.malformed_files += 1;
                continue;
            }
        };

        let chunks = match request.extract_mode {
            ConversationExtractMode::Exchange => chunk_exchanges(&normalized.transcript),
            ConversationExtractMode::General => extract_memories(&normalized.transcript),
        };

        if chunks.is_empty() {
            if !request.dry_run {
                replace_source_drawers(
                    engine,
                    &source_key,
                    &file.relative_path,
                    "convos",
                    content_hash,
                    Vec::new(),
                )
                .await?;
            }
            summary.ingested_files += 1;
            continue;
        }

        let convo_room = detect_conversation_room(&normalized.transcript);
        let drawers = build_drawers(
            provider,
            &wing_id,
            &source_key,
            &file.relative_path,
            "convos",
            Some(request.extract_mode.as_str()),
            &request.agent,
            request.max_embed_batch_size,
            chunks
                .into_iter()
                .map(|mut chunk| {
                    if chunk.room_hint.is_none() {
                        chunk.room_hint = Some(convo_room.clone());
                    }
                    chunk
                })
                .collect::<Vec<_>>(),
            None,
            None,
            None,
        )?;
        let drawer_count = drawers.len();

        if !request.dry_run {
            replace_source_drawers(
                engine,
                &source_key,
                &file.relative_path,
                "convos",
                content_hash,
                drawers,
            )
            .await?;
        }
        summary.ingested_files += 1;
        summary.drawers_written += drawer_count;
    }

    Ok(summary)
}

/// Context needed to populate `DrawerRecord.locator` for project-mined chunks.
struct ProjectLocatorContext<'a> {
    file_hash: &'a str,
    resolve_root: &'a str,
    commit_hash: Option<&'a str>,
    /// Parallel to the chunk list: (line_start, line_end).
    line_numbers: &'a [(u32, u32)],
}

fn build_drawers<P: EmbeddingProvider>(
    provider: &mut P,
    wing: &WingId,
    source_key: &str,
    source_file: &str,
    ingest_mode: &str,
    extract_mode: Option<&str>,
    agent: &str,
    max_embed_batch_size: Option<usize>,
    chunks: Vec<Chunk>,
    locator_ctx: Option<&ProjectLocatorContext<'_>>,
    view: Option<&str>,
    view_metadata: Option<&mempalace_core::RepositoryViewMetadata>,
) -> Result<Vec<DrawerRecord>> {
    if chunks.is_empty() {
        return Ok(Vec::new());
    }

    // Embed using the real chunk text.
    let embeddings = embed_chunks(provider, &chunks, max_embed_batch_size)?;

    build_drawers_from_embeddings(
        wing,
        source_key,
        source_file,
        ingest_mode,
        extract_mode,
        agent,
        chunks,
        locator_ctx,
        view,
        view_metadata,
        embeddings,
    )
}

fn build_drawers_from_embeddings(
    wing: &WingId,
    source_key: &str,
    source_file: &str,
    ingest_mode: &str,
    extract_mode: Option<&str>,
    agent: &str,
    chunks: Vec<Chunk>,
    locator_ctx: Option<&ProjectLocatorContext<'_>>,
    view: Option<&str>,
    view_metadata: Option<&mempalace_core::RepositoryViewMetadata>,
    embeddings: Vec<Vec<f32>>,
) -> Result<Vec<DrawerRecord>> {
    let mut drawers = Vec::with_capacity(chunks.len());
    for (i, (chunk, embedding)) in chunks.into_iter().zip(embeddings.into_iter()).enumerate() {
        let room_name = chunk.room_hint.unwrap_or_else(|| "general".to_owned());
        let room_id = room_id(&room_name)?;
        let drawer_id = drawer_id(wing, &room_id, source_key, chunk.chunk_index)?;
        let chunk_text = chunk.content;
        let content_hash = hash_text(&chunk_text);

        let locator = match (locator_ctx, chunk.byte_range) {
            (Some(ctx), Some((byte_start, byte_end))) => {
                let (line_start, line_end) = ctx.line_numbers.get(i).copied().unwrap_or((1, 1));
                Some(SourceLocator {
                    byte_start,
                    byte_end,
                    line_start,
                    line_end,
                    file_hash: ctx.file_hash.to_owned(),
                    resolve_root: ctx.resolve_root.to_owned(),
                    commit_hash: ctx.commit_hash.map(str::to_owned),
                })
            }
            _ => None,
        };

        // When a locator is present, store empty content (resolved lazily).
        let stored_content = if locator.is_some() { String::new() } else { chunk_text };

        // For branch views, store the view name in the hall field so searches
        // can filter by view.  Canonical views keep hall = None.
        let hall = view.map(|v| format!("view:{v}"));

        drawers.push(DrawerRecord {
            id: drawer_id,
            wing: wing.clone(),
            room: room_id,
            hall,
            date: chunk.date_hint,
            source_file: source_file.to_owned(),
            chunk_index: chunk.chunk_index,
            ingest_mode: ingest_mode.to_owned(),
            extract_mode: extract_mode.map(str::to_owned),
            added_by: agent.to_owned(),
            filed_at: OffsetDateTime::now_utc(),
            importance: None,
            emotional_weight: None,
            weight: None,
            content_hash,
            content: stored_content,
            embedding,
            locator,
            view_metadata: view_metadata.cloned(),
        });
    }

    Ok(drawers)
}

fn embed_chunks<P: EmbeddingProvider>(
    provider: &mut P,
    chunks: &[Chunk],
    max_embed_batch_size: Option<usize>,
) -> Result<Vec<Vec<f32>>> {
    let batch_size = max_embed_batch_size.unwrap_or(chunks.len()).max(1);
    let mut embeddings = Vec::with_capacity(chunks.len());

    for batch in chunks.chunks(batch_size) {
        let request = EmbeddingRequest::new(
            batch.iter().map(|chunk| chunk.content.clone()).collect::<Vec<_>>(),
        )?;
        let response = provider.embed(&request)?;
        embeddings.extend(response.vectors().iter().cloned());
    }

    Ok(embeddings)
}

async fn replace_source_drawers(
    engine: &StorageEngine,
    source_key: &str,
    source_file: &str,
    ingest_kind: &str,
    content_hash: String,
    drawers: Vec<DrawerRecord>,
) -> Result<()> {
    engine
        .replace_source_drawers(ingest_kind, source_key, source_file, content_hash, drawers)
        .await
        .map_err(IngestError::Storage)
}

// ─── Per-file chunk preparation helper ─────────────────────────────────────

/// Pure per-file preparation: read the file, chunk it, compute byte/line offsets.
/// No embedding, no storage.  Returns an error only on I/O or path problems.
///
/// Files below [`PROJECT_MIN_CHUNK_SIZE`] or that produce zero chunks after
/// chunking will have an empty `chunks` vec — the caller handles the
/// replace-with-empty semantics.
fn prepare_file_chunks(
    file: &ProjectSource,
    routing_fingerprint: &str,
    rooms: &[ProjectRoomConfig],
) -> Result<PreparedFileChunks> {
    let document = read_text_document(&file.absolute_path)?;
    let content_hash =
        project_ingest_content_hash(&document.content_hash, routing_fingerprint);

    // Below the minimum size gate → return with empty chunks.
    if document.content.trim().len() < PROJECT_MIN_CHUNK_SIZE {
        return Ok(PreparedFileChunks {
            relative_path: file.relative_path.clone(),
            content_hash,
            file_hash: None,
            chunks: Vec::new(),
            truncated: document.truncated,
        });
    }

    let room = detect_project_room(
        Path::new(&file.relative_path),
        &document.content,
        rooms,
    );

    let raw_chunks = chunk_project_text(&document.content, document.valid_utf8);

    // After chunking, if we got nothing → return with empty chunks.
    if raw_chunks.is_empty() {
        return Ok(PreparedFileChunks {
            relative_path: file.relative_path.clone(),
            content_hash,
            file_hash: None,
            chunks: Vec::new(),
            truncated: document.truncated,
        });
    }

    let (file_hash, prepared_chunks) = if document.valid_utf8 {
        // Adjust chunk byte ranges from trimmed-string offsets to file offsets.
        let file_byte_ranges: Vec<(u64, u64)> = raw_chunks
            .iter()
            .filter_map(|c| c.byte_range)
            .map(|(s, e)| {
                let off = document.trim_offset as u64;
                (s + off, e + off)
            })
            .collect();

        // Reuse the bytes read by read_text_document — both the hash and
        // the line numbers must describe the same file snapshot.
        let line_numbers = compute_line_numbers(&document.raw_bytes, &file_byte_ranges);

        let chunks: Vec<PreparedChunk> = raw_chunks
            .into_iter()
            .zip(file_byte_ranges.iter())
            .zip(line_numbers.iter())
            .map(|((c, &(bs, be)), &(ls, le))| PreparedChunk {
                text: c.content,
                chunk_index: c.chunk_index,
                room: room.clone(),
                byte_start: Some(bs),
                byte_end: Some(be),
                line_start: Some(ls),
                line_end: Some(le),
            })
            .collect();

        (Some(document.content_hash), chunks)
    } else {
        let chunks: Vec<PreparedChunk> = raw_chunks
            .into_iter()
            .map(|c| PreparedChunk {
                text: c.content,
                chunk_index: c.chunk_index,
                room: room.clone(),
                byte_start: None,
                byte_end: None,
                line_start: None,
                line_end: None,
            })
            .collect();
        (None, chunks)
    };

    Ok(PreparedFileChunks {
        relative_path: file.relative_path.clone(),
        content_hash,
        file_hash,
        chunks: prepared_chunks,
        truncated: document.truncated,
    })
}

/// Convert [`PreparedFileChunks`] into a [`Chunk`] vec plus optional owned
/// locator storage that the caller can then borrow as [`ProjectLocatorContext`].
fn prepared_chunks_to_ingest(
    prepared: &PreparedFileChunks,
    resolve_root: &str,
    commit_hash: Option<&str>,
) -> (Vec<Chunk>, Option<PreparedLocatorStorage>) {
    match prepared.file_hash {
        Some(ref fh) => {
            let line_numbers: Vec<(u32, u32)> = prepared
                .chunks
                .iter()
                .map(|c| (c.line_start.unwrap_or(1), c.line_end.unwrap_or(1)))
                .collect();

            let chunks_out: Vec<Chunk> = prepared
                .chunks
                .iter()
                .map(|c| Chunk {
                    content: c.text.clone(),
                    chunk_index: c.chunk_index,
                    room_hint: Some(c.room.clone()),
                    date_hint: None,
                    byte_range: c.byte_start.zip(c.byte_end),
                })
                .collect();
            let storage = PreparedLocatorStorage {
                file_hash: fh.clone(),
                resolve_root: resolve_root.to_owned(),
                commit_hash: commit_hash.map(str::to_owned),
                line_numbers,
            };
            (chunks_out, Some(storage))
        }
        None => {
            let chunks_out: Vec<Chunk> = prepared
                .chunks
                .iter()
                .map(|c| Chunk {
                    content: c.text.clone(),
                    chunk_index: c.chunk_index,
                    room_hint: Some(c.room.clone()),
                    date_hint: None,
                    byte_range: None,
                })
                .collect();
            (chunks_out, None)
        }
    }
}

/// Owned storage for the locator context produced by [`prepared_chunks_to_ingest`].
/// Implements `AsRef<ProjectLocatorContext<'_>>` so it can be passed to `build_drawers`.
struct PreparedLocatorStorage {
    file_hash: String,
    resolve_root: String,
    commit_hash: Option<String>,
    line_numbers: Vec<(u32, u32)>,
}

impl PreparedLocatorStorage {
    fn as_ctx(&self) -> ProjectLocatorContext<'_> {
        ProjectLocatorContext {
            file_hash: &self.file_hash,
            resolve_root: &self.resolve_root,
            commit_hash: self.commit_hash.as_deref(),
            line_numbers: &self.line_numbers,
        }
    }
}

// ─── Branch-delta helpers ────────────────────────────────────────────────────

/// Describes how a checkout relates to its repository's canonical view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckoutView {
    /// The checkout is on the canonical/default branch (main/master).
    Canonical,
    /// The checkout is on a named branch or linked worktree.
    Branch {
        /// The branch name or HEAD ref of the checkout.
        view_name: String,
        /// The base/integration ref (default branch).
        base_ref: Option<String>,
        /// The merge-base commit between the view and the base ref.
        merge_base: Option<String>,
    },
    /// The checkout is not a Git repository; treat as a full directory.
    NonGit,
}

/// Detect the checkout view type for a given source directory.
///
/// Returns [`CheckoutView::NonGit`] when the directory is not inside a Git
/// repository, [`CheckoutView::Canonical`] when the current checkout is on the
/// canonical/default branch, and [`CheckoutView::Branch`] with the view name,
/// base ref, and merge-base when on a non-canonical checkout.
///
/// The canonical branch is determined by `origin/HEAD` symbolic-ref, then
/// by literal `main` / `master` fallback.
pub fn detect_checkout_view(root: &Path) -> CheckoutView {
    let toplevel = git_repo_toplevel(root);
    let toplevel = match toplevel {
        Some(p) => p,
        None => return CheckoutView::NonGit,
    };

    let default_branch = detect_default_branch(&toplevel);
    let current_branch = resolve_current_branch(&toplevel);

    match (default_branch, current_branch) {
        (Some(ref base), Some(ref current)) if branch_name(base) == current => CheckoutView::Canonical,
        (Some(base), Some(view_name)) => {
            let merge_base = compute_merge_base(&toplevel, &base);
            CheckoutView::Branch {
                view_name,
                base_ref: Some(base),
                merge_base,
            }
        }
        (Some(base), None) => {
            // Detached HEAD: use a stable hash of the toplevel path as a view identity.
            let view_name = format!("detached-{}", &hash_text(&toplevel.to_string_lossy())[..12]);
            let merge_base = compute_merge_base(&toplevel, &base);
            CheckoutView::Branch {
                view_name,
                base_ref: Some(base),
                merge_base,
            }
        }
        // Without a known integration ref there is no safe delta baseline.
        // Preserve the pre-view behavior and mine the checkout in full.
        (None, _) => CheckoutView::Canonical,
    }
}

/// Detect the default branch ref: tries `origin/HEAD` symbolic-ref, then
/// literal `main` / `master`.  Returns `None` when neither is found.
pub fn detect_default_branch(root: &Path) -> Option<String> {
    let root_str = root.to_string_lossy();

    // Keep the symbolic remote ref (e.g. "origin/main") resolvable for
    // merge-base. `branch_name` performs the short-name comparison separately.
    let out = Command::new("git")
        .args(["-C", &root_str, "symbolic-ref", "--short", "refs/remotes/origin/HEAD"])
        .output()
        .ok()?;
    if out.status.success() {
        let s = std::str::from_utf8(&out.stdout).ok()?.trim().to_owned();
        if !s.is_empty() {
            return Some(s);
        }
    }

    // Fallback: check if main / master exist as local refs. Use `output()` (not
    // `status()`) so git's stdout (the resolved SHA) and stderr ("fatal: not a
    // git repository" for non-repos) are captured rather than leaking to the
    // CLI's own stdout/stderr.
    for candidate in &["main", "master"] {
        let ok = Command::new("git")
            .args(["-C", &root_str, "rev-parse", "--verify", "--quiet", candidate])
            .output()
            .map(|out| out.status.success())
            .unwrap_or(false);
        if ok {
            return Some((*candidate).to_owned());
        }
    }
    None
}

fn branch_name(ref_name: &str) -> &str {
    ref_name.strip_prefix("origin/").unwrap_or(ref_name)
}

/// Compute the merge-base commit between `default_ref` and HEAD.
pub fn compute_merge_base(root: &Path, default_ref: &str) -> Option<String> {
    let out = Command::new("git")
        .args(["-C", &root.to_string_lossy().as_ref(), "merge-base", default_ref, "HEAD"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = std::str::from_utf8(&out.stdout).ok()?.trim().to_owned();
    if s.is_empty() { None } else { Some(s) }
}

/// Return repo-toplevel-relative forward-slash paths of all files changed
/// between `merge_base` and the working tree (including untracked files).
fn git_delta_paths(root: &Path, merge_base: &str) -> Option<Vec<String>> {
    let root_str = root.to_string_lossy();

    // Changed/added files (working tree vs merge-base).  `-z` yields
    // NUL-separated, unquoted paths so filenames with spaces or non-ASCII
    // bytes survive regardless of the user's core.quotePath setting.
    let diff_out = Command::new("git")
        .args([
            "-C",
            &root_str,
            "diff",
            "--name-only",
            "--diff-filter=d",
            "-z",
            merge_base,
        ])
        .output()
        .ok()?;
    if !diff_out.status.success() {
        return None;
    }

    // Untracked files.
    let untracked_out = Command::new("git")
        .args(["-C", &root_str, "ls-files", "--others", "--exclude-standard", "-z"])
        .output()
        .ok()?;
    if !untracked_out.status.success() {
        return None;
    }

    let mut paths = Vec::new();
    for bytes in [diff_out.stdout.as_slice(), untracked_out.stdout.as_slice()] {
        for path_bytes in bytes.split(|&b| b == 0) {
            if path_bytes.is_empty() {
                continue;
            }
            // Non-UTF-8 paths can't match our String-based relative paths;
            // skip them rather than failing the whole delta.
            if let Ok(path) = std::str::from_utf8(path_bytes) {
                paths.push(path.to_owned());
            }
        }
    }
    Some(paths)
}

/// Return project-root-relative paths deleted from HEAD since the merge-base.
fn compute_deleted_branch_paths(root: &Path) -> Result<BTreeSet<String>> {
    let default_ref = detect_default_branch(root).ok_or_else(|| {
        IngestError::BranchDeltaUnavailable {
            reason: "not a git repository or no default branch (origin/HEAD, main, master) found"
                .to_owned(),
        }
    })?;
    let merge_base = compute_merge_base(root, &default_ref).ok_or_else(|| {
        IngestError::BranchDeltaUnavailable {
            reason: format!("could not compute merge-base between '{default_ref}' and HEAD"),
        }
    })?;
    let root_str = root.to_string_lossy();
    let output = Command::new("git")
        .args([
            "-C",
            root_str.as_ref(),
            "diff",
            "--name-only",
            "--no-renames",
            "--diff-filter=D",
            "--relative",
            "-z",
            &merge_base,
        ])
        .output()
        .map_err(|source| IngestError::Io { path: root.to_path_buf(), source })?;
    if !output.status.success() {
        return Err(IngestError::BranchDeltaUnavailable {
            reason: "git diff for deleted paths failed".to_owned(),
        });
    }
    let mut deleted = BTreeSet::new();
    for path in output.stdout.split(|&byte| byte == 0).filter(|path| !path.is_empty()) {
        let Ok(path) = std::str::from_utf8(path) else { continue };
        let relative = Path::new(path)
            .components()
            .filter_map(|component| match component {
                Component::Normal(part) => part.to_str(),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("/");
        if !relative.is_empty() {
            deleted.insert(relative);
        }
    }
    Ok(deleted)
}

/// Get the absolute repo root (via `git rev-parse --show-toplevel`).
fn git_repo_toplevel(root: &Path) -> Option<PathBuf> {
    let out = Command::new("git")
        .args(["-C", &root.to_string_lossy().as_ref(), "rev-parse", "--show-toplevel"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = std::str::from_utf8(&out.stdout).ok()?.trim();
    if s.is_empty() { None } else { Some(PathBuf::from(s)) }
}

/// Compute the branch delta: returns project-root-relative forward-slash paths
/// of files that differ from the merge-base with the default branch (including
/// untracked files).  Paths outside the project root are dropped.
fn compute_branch_delta(root: &Path) -> Result<Vec<String>> {
    let default_ref = detect_default_branch(root).ok_or_else(|| {
        IngestError::BranchDeltaUnavailable {
            reason: "not a git repository or no default branch (origin/HEAD, main, master) found"
                .to_owned(),
        }
    })?;

    let merge_base = compute_merge_base(root, &default_ref).ok_or_else(|| {
        IngestError::BranchDeltaUnavailable {
            reason: format!(
                "could not compute merge-base between '{default_ref}' and HEAD"
            ),
        }
    })?;

    let repo_paths = git_delta_paths(root, &merge_base).ok_or_else(|| {
        IngestError::BranchDeltaUnavailable {
            reason: "git diff / ls-files failed".to_owned(),
        }
    })?;

    // Re-relativize paths from repo-root to project-root.
    let repo_root = git_repo_toplevel(root).ok_or_else(|| {
        IngestError::BranchDeltaUnavailable {
            reason: "git rev-parse --show-toplevel failed".to_owned(),
        }
    })?;

    // repo_root from git uses forward slashes on all platforms for the purpose
    // of path computation below; canonicalize both for comparison.
    let repo_root_canon = repo_root
        .canonicalize()
        .unwrap_or_else(|_| repo_root.clone());
    let project_root_canon = root.to_path_buf();

    let mut result = Vec::new();
    for repo_rel in repo_paths {
        // Build absolute path from repo root + repo-relative path (forward slashes).
        let abs = repo_root_canon.join(repo_rel.replace('/', std::path::MAIN_SEPARATOR_STR));
        // Now compute relative to project root.
        match abs.strip_prefix(&project_root_canon) {
            Ok(rel) => {
                // Convert back to forward slashes.
                let fwd: String = rel
                    .components()
                    .filter_map(|c| {
                        if let Component::Normal(s) = c {
                            s.to_str().map(str::to_owned)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("/");
                if !fwd.is_empty() {
                    result.push(fwd);
                }
            }
            Err(_) => {
                // Path is outside the project root — drop it.
            }
        }
    }
    Ok(result)
}

// ─── prepare_project_batch ───────────────────────────────────────────────────

/// Summary produced by [`prepare_project_batch`].
#[derive(Debug, Clone)]
pub struct PreparedProjectMine {
    /// Resolved wing name.
    pub wing: String,
    /// Machine-independent repository identity (see [`derive_repo_id`]).
    pub repo_id: String,
    /// Git commit hash at the time of preparation, if available.
    pub commit_hash: Option<String>,
    /// Current git branch name, for non-default-branch warnings on the caller side.
    pub current_branch: Option<String>,
    /// Default branch name (as resolved from origin/HEAD or main/master).
    pub default_branch: Option<String>,
    /// Files ready to send; zero-chunk files are excluded (not representable over
    /// the wire in v1 — the replace-with-empty case is local-only).
    pub files: Vec<IngestFileDto>,
    /// Discovery/preparation counts (ingested_files = files included in `files`).
    pub summary: IngestSummary,
}

/// Prepare a project mine for federation transmission.
///
/// Performs discovery + per-file chunk preparation (including byte/line offset
/// computation) but no embedding and no storage writes.  The returned
/// [`PreparedProjectMine`] contains [`IngestFileDto`] values ready for
/// [`mempalace_federation::IngestBatchRequest`].
///
/// Files with zero chunks (below the minimum size gate or producing no chunks
/// after splitting) are **excluded** from `files` — the replace-with-empty
/// semantics are not supported over the wire in v1.  They are not counted in
/// `summary.ingested_files` (only files actually included are counted).
///
/// `request.dry_run` and `request.branch` are ignored; the caller controls
/// whether to send the batch.
pub fn prepare_project_batch(request: &ProjectIngestRequest) -> Result<PreparedProjectMine> {
    let root = request
        .project_dir
        .canonicalize()
        .map_err(|source| IngestError::Io { path: request.project_dir.clone(), source })?;
    let derived_wing = request
        .wing
        .clone()
        .unwrap_or_else(|| derived_project_wing(&root));
    let project_id = derive_project_id(&root, &derived_wing, None);
    let config = ConfigLoader::resolve_project_config(
        &root,
        None,
        Some(&project_id),
        &derived_wing,
        Vec::new(),
    )?;
    let repo_id = ConfigLoader::find_project_id(None, &root, Some(&project_id))?
        .unwrap_or_else(|| derive_project_id(&root, &config.wing, None));
    prepare_project_batch_with_config(request, &config, Some(&repo_id))
}

/// Prepare a project batch using a project declaration resolved by the caller.
pub fn prepare_project_batch_with_config(
    request: &ProjectIngestRequest,
    config: &ProjectConfig,
    project_id: Option<&str>,
) -> Result<PreparedProjectMine> {
    let root = request
        .project_dir
        .canonicalize()
        .map_err(|source| IngestError::Io { path: request.project_dir.clone(), source })?;
    let wing_name = request.wing.clone().unwrap_or_else(|| config.wing.clone());
    let discovered = discover_project_files(&root)?;
    let routing_fingerprint = project_routing_fingerprint(&config.rooms);
    let commit_hash = resolve_commit_hash(&root);

    let repo_id = project_id
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| derive_repo_id(&root, &wing_name));
    let default_branch = detect_default_branch(&root);
    let current_branch = resolve_current_branch(&root);

    let mut summary = IngestSummary::default();
    summary.discovered_files = discovered.files.len();
    summary.ignored_files = discovered.ignored_files;
    summary.secret_path_skips = discovered.skips;
    // Remote mine callers resolve the checkout view before preparing the batch.
    // Preserve it so their summaries match local mine output.
    summary.view_name = request
        .view
        .as_deref()
        .filter(|view| *view != "canonical")
        .map(str::to_owned);

    let files_to_process: Vec<ProjectSource> =
        apply_limit(discovered.files, request.limit).collect();
    let mut file_dtos: Vec<IngestFileDto> = Vec::new();

    for file in &files_to_process {
        match prepare_file_chunks(file, &routing_fingerprint, &config.rooms) {
            Ok(prepared) => {
                // Skip zero-chunk files — replace-with-empty is not supported over
                // the wire in v1.
                if prepared.chunks.is_empty() {
                    continue;
                }

                let chunks: Vec<IngestChunkDto> = prepared
                    .chunks
                    .iter()
                    .map(|c| IngestChunkDto {
                        chunk_index: c.chunk_index,
                        room: c.room.clone(),
                        text: c.text.clone(),
                        byte_start: c.byte_start,
                        byte_end: c.byte_end,
                        line_start: c.line_start,
                        line_end: c.line_end,
                    })
                    .collect();

                file_dtos.push(IngestFileDto {
                    relative_path: prepared.relative_path,
                    content_hash: prepared.content_hash,
                    file_hash: prepared.file_hash,
                    chunks,
                });

                summary.ingested_files += 1;
                summary.truncated_files += usize::from(prepared.truncated);
            }
            Err(IngestError::Io { .. }) => {
                summary.unreadable_files += 1;
            }
            Err(error) => return Err(error),
        }
    }

    Ok(PreparedProjectMine {
        wing: wing_name,
        repo_id,
        commit_hash,
        current_branch,
        default_branch,
        files: file_dtos,
        summary,
    })
}

// ─── Repo identity ───────────────────────────────────────────────────────────

fn derived_project_wing(root: &Path) -> String {
    let name = root
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("project")
        .to_ascii_lowercase()
        .replace('-', "_")
        .replace(' ', "_");
    if name.starts_with("wing_") { name } else { format!("wing_{name}") }
}

/// Derive a machine-independent repository identity string from the git remote
/// URL of `origin`.  Falls back to `format!("wing:{wing}")` when no remote is
/// configured or the directory is not a git repository.
pub fn derive_repo_id(root: &Path, wing: &str) -> String {
    let url = Command::new("git")
        .args(["-C", &root.to_string_lossy(), "remote", "get-url", "origin"])
        .output()
        .ok()
        .and_then(|out| {
            if out.status.success() {
                std::str::from_utf8(&out.stdout).ok().map(|s| s.trim().to_owned())
            } else {
                None
            }
        });

    match url.as_deref().filter(|s| !s.is_empty()) {
        Some(u) => normalize_git_remote_url(u)
            .unwrap_or_else(|| format!("wing:{wing}")),
        None => format!("wing:{wing}"),
    }
}

/// Normalize a git remote URL to the form `host/path` for use as a
/// machine-independent repository identity.
///
/// Rules:
/// - Strip one trailing `/` and one trailing `.git`.
/// - `git@host:path` (SCP) → `host/path`.
/// - `scheme://[user@]host[:port]/path` → `host/path` (port dropped).
/// - Host is lowercased; path case is preserved.
///
/// Returns `None` for unrecognisable or empty input.
pub fn normalize_git_remote_url(url: &str) -> Option<String> {
    let url = url.trim();
    if url.is_empty() {
        return None;
    }

    // Strip one trailing '/' then one trailing '.git'.
    let url = url.trim_end_matches('/');
    let url = url.strip_suffix(".git").unwrap_or(url);
    // Another trailing '/' after stripping .git (e.g. "…repo.git/").
    let url = url.trim_end_matches('/');

    if url.is_empty() {
        return None;
    }

    // SCP style: git@github.com:owner/repo
    if let Some(at_pos) = url.find('@') {
        if !url[..at_pos].contains("://") {
            // No scheme before '@' → SCP format.
            let after_at = &url[at_pos + 1..];
            if let Some(colon_pos) = after_at.find(':') {
                let host = after_at[..colon_pos].to_ascii_lowercase();
                let path = &after_at[colon_pos + 1..];
                let path = path.trim_start_matches('/');
                if path.is_empty() || host.is_empty() {
                    return None;
                }
                return Some(format!("{host}/{path}"));
            }
            return None;
        }
    }

    // URL style: scheme://[user@]host[:port]/path
    if let Some(after_scheme) = url.find("://").map(|i| &url[i + 3..]) {
        // Strip optional user@ prefix.
        let host_and_rest = if let Some(at_pos) = after_scheme.find('@') {
            &after_scheme[at_pos + 1..]
        } else {
            after_scheme
        };

        // Split host (and optional :port) from path.
        let (host_port, path) = if let Some(slash_pos) = host_and_rest.find('/') {
            (&host_and_rest[..slash_pos], &host_and_rest[slash_pos + 1..])
        } else {
            (host_and_rest, "")
        };

        // Drop the port from host:port.
        let host = if let Some(colon_pos) = host_port.find(':') {
            &host_port[..colon_pos]
        } else {
            host_port
        }
        .to_ascii_lowercase();

        let path = path.trim_start_matches('/');

        if host.is_empty() {
            return None;
        }
        if path.is_empty() {
            return Some(host);
        }
        return Some(format!("{host}/{path}"));
    }

    None
}

/// Resolve the current git branch name (`git rev-parse --abbrev-ref HEAD`).
/// Returns `None` when not in a repo or in detached HEAD state.
pub fn resolve_current_branch(root: &Path) -> Option<String> {
    let out = Command::new("git")
        .args(["-C", &root.to_string_lossy(), "rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = std::str::from_utf8(&out.stdout).ok()?.trim().to_owned();
    if s.is_empty() || s == "HEAD" { None } else { Some(s) }
}

/// Returns the secret-denylist category for `file_name` (matched
/// case-insensitively), or `None` when the name is not secret-shaped.
///
/// The match is purely path-based and runs before any content is read, so a
/// secret-shaped path is withheld without opening the file (issue #95).
fn secret_path_kind(file_name: &str) -> Option<SecretPathKind> {
    let name = file_name.to_ascii_lowercase();
    if name.starts_with(".env") || name.ends_with(".env") {
        return Some(SecretPathKind::DotEnv);
    }
    if name.contains("kubeconfig") {
        return Some(SecretPathKind::Kubeconfig);
    }
    if ["id_rsa", "id_ed25519", "id_ecdsa", "id_dsa"]
        .iter()
        .any(|prefix| name.starts_with(*prefix))
    {
        return Some(SecretPathKind::SshPrivateKey);
    }
    if [".pfx", ".p12", ".jks"].iter().any(|ext| name.ends_with(*ext)) {
        return Some(SecretPathKind::Keystore);
    }
    if name == ".npmrc" || name == ".netrc" {
        return Some(SecretPathKind::Netrc);
    }
    if name.ends_with(".tfstate") || name.ends_with(".tfvars") {
        return Some(SecretPathKind::TerraformSecret);
    }
    if name.starts_with("secrets") && name.ends_with(".json") {
        return Some(SecretPathKind::SecretsJson);
    }
    if name.ends_with(".local.json") {
        return Some(SecretPathKind::LocalJson);
    }
    None
}

/// Whether a candidate source is eligible for mining, and — when excluded by
/// the secret-shaped denylist — the category for an operator-visible skip
/// record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Eligibility {
    Eligible,
    /// Excluded for a reason that is not surfaced as a skip record
    /// (extension mismatch, binary sniff, lockfile, palace config, ...).
    Excluded,
    /// Excluded by the secret-shaped path denylist (issue #95).
    Secret(SecretPathKind),
}

/// Classify `file_name` against the built-in always-skip lists: the
/// secret-shaped path denylist (issue #95) and the general hygiene list
/// (lockfiles, palace config, `.gitignore`).
fn project_skip_by_name(file_name: &str) -> Option<Eligibility> {
    if let Some(kind) = secret_path_kind(file_name) {
        return Some(Eligibility::Secret(kind));
    }
    if PROJECT_SKIP_FILES.contains(&file_name) {
        return Some(Eligibility::Excluded);
    }
    None
}

/// Append a secret-denylist skip record for `relative_path` when
/// `eligibility` carries one. Used by both Git-index and filesystem discovery
/// so exclusions are counted and recorded consistently.
fn record_secret_skip(
    eligibility: Eligibility,
    relative_path: String,
    skips: &mut Vec<ProjectSourceSkip>,
) {
    if let Eligibility::Secret(kind) = eligibility {
        skips.push(ProjectSourceSkip {
            relative_path,
            reason: kind.as_str().to_owned(),
        });
    }
}

/// Read up to `limit` bytes from `path`.  Returns `None` on any I/O error.
fn read_prefix(path: &Path, limit: usize) -> Option<Vec<u8>> {
    let file = std::fs::File::open(path).ok()?;
    let mut buf = Vec::with_capacity(limit);
    // take + read_to_end reads reliably up to `limit` bytes; a single read()
    // call may legally return fewer bytes than the buffer holds.
    file.take(limit as u64).read_to_end(&mut buf).ok()?;
    Some(buf)
}

/// Returns `true` if the file looks like binary (NUL byte in the first `BINARY_SNIFF_BYTES`).
/// I/O error → `false` (treat as text; the subsequent read_text_document call will handle it).
fn looks_binary(path: &Path) -> bool {
    read_prefix(path, BINARY_SNIFF_BYTES)
        .map(|buf| buf.contains(&0u8))
        .unwrap_or(false)
}

/// For an extensionless file that is NOT in the basename allowlist: returns `true` if the
/// first bytes start with `#!`.  I/O error → `false`.
fn has_shebang(path: &Path) -> bool {
    read_prefix(path, SHEBANG_READ_BYTES)
        .map(|buf| buf.starts_with(b"#!"))
        .unwrap_or(false)
}

/// Discover the safe set of eligible project sources under `root`.
///
/// For Git-backed roots the eligible sources are the project-readable subset of
/// tracked index paths (`git ls-files`), so ignored and untracked working-tree
/// files are never mined; a `.gitignore` never suppresses a tracked path, and
/// `.mempalaceignore` is the explicit additional exclusion. For non-Git
/// directories a filesystem walk with git-compatible ignore handling
/// (nested `.gitignore`/`.mempalaceignore` files, `!` negation, anchoring, and
/// globs) is used, honouring the `core.excludesFile` global excludes file.
/// Git-backed filesystem walks — the branch-delta mine — also honour
/// `$GIT_DIR/info/exclude` at git's precedence. Sources are sorted by relative
/// path (deterministic order), are relative to `root`, and linked Git worktrees
/// are always excluded.
pub fn discover_project_sources(root: &Path) -> Result<ProjectSourceDiscovery> {
    let root = root
        .canonicalize()
        .map_err(|source| IngestError::Io { path: root.to_path_buf(), source })?;
    if git_is_backed(&root) {
        discover_git_index_sources(&root)
    } else {
        discover_filesystem_sources(&root)
    }
}

/// Returns `true` when `root` is inside a Git work tree whose tracked index
/// paths can be enumerated with `git ls-files`.
fn git_is_backed(root: &Path) -> bool {
    Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["rev-parse", "--is-inside-work-tree"])
        .output()
        .ok()
        .is_some_and(|output| {
            output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == "true"
        })
}

/// Absolute path to `$GIT_DIR/info/exclude` for `root`, resolved via
/// `git rev-parse --git-path` (handles `GIT_DIR`, linked worktrees, and
/// gitdir files), falling back to `<root>/.git/info/exclude`. Returns `None`
/// when git cannot report a path.
fn repo_info_exclude_path(root: &Path) -> Option<PathBuf> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["rev-parse", "--git-path", "info/exclude"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if text.is_empty() {
        return Some(root.join(".git").join("info").join("exclude"));
    }
    let path = PathBuf::from(text);
    if path.is_absolute() {
        Some(path)
    } else {
        // git prints paths relative to the directory `-C` ran in.
        Some(root.join(path))
    }
}

/// Absolute path to the global excludes file for `root`: the value of
/// `core.excludesFile` (with `~/` expanded against `HOME`), or the XDG default
/// when the option is not configured.
fn global_excludes_path(root: &Path) -> Option<PathBuf> {
    let configured = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["config", "--get", "core.excludesFile"])
        .output()
        .ok();
    if let Some(output) = configured {
        if output.status.success() {
            let text = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            if !text.is_empty() {
                let path = PathBuf::from(&text);
                if path.is_absolute() {
                    return Some(path);
                }
                if let Some(home) = std::env::var_os("HOME") {
                    if let Some(stripped) = text.strip_prefix("~/") {
                        return Some(PathBuf::from(home).join(stripped));
                    }
                }
                // git resolves a relative core.excludesFile against the
                // directory it runs in, which is the repository root here.
                return Some(root.join(path));
            }
        }
    }
    let xdg_config_home = std::env::var("XDG_CONFIG_HOME").ok();
    let home = std::env::var_os("HOME").map(PathBuf::from);
    Some(default_global_excludes_path(xdg_config_home.as_deref(), home.as_deref()))
}

/// The XDG default location for global excludes when `core.excludesFile` is
/// unset: `$XDG_CONFIG_HOME/git/ignore`, or `~/.config/git/ignore` when
/// `XDG_CONFIG_HOME` is unset.
fn default_global_excludes_path(xdg_config_home: Option<&str>, home: Option<&Path>) -> PathBuf {
    let base = match xdg_config_home {
        Some(dir) if !dir.is_empty() => PathBuf::from(dir),
        _ => home.unwrap_or(Path::new("~")).join(".config"),
    };
    base.join("git").join("ignore")
}

/// Enumerate the tracked index paths under `root` and keep the project-eligible
/// subset.
fn discover_git_index_sources(root: &Path) -> Result<ProjectSourceDiscovery> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["ls-files", "-z"])
        .output()
        .map_err(|source| IngestError::Io { path: root.to_path_buf(), source })?;
    if !output.status.success() {
        // The root was confirmed Git-backed but the index cannot be enumerated.
        // Falling back to a filesystem walk here could ingest untracked and
        // ignored working-tree files, violating the tracked-index-only source
        // guarantee — surface the failure instead.
        let reason = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        return Err(IngestError::GitIndexUnavailable {
            path: root.to_path_buf(),
            reason: if reason.is_empty() {
                "git ls-files exited unsuccessfully".to_owned()
            } else {
                reason
            },
        });
    }

    // Collect the tracked index entries first so the directories that contain
    // them are known before building the ignore matcher.
    let mut entries = Vec::new();
    let mut skipped = 0usize;
    for entry in output.stdout.split(|byte| *byte == b'\0') {
        if entry.is_empty() {
            continue;
        }
        let relative = match path_from_git_bytes(entry) {
            Some(relative) => relative,
            // Undecodable path bytes (non-UTF-8 on platforms where git emits
            // text) can't be represented in the String-based relative model.
            None => {
                skipped += 1;
                continue;
            }
        };
        let relative_str = relative.to_string_lossy().into_owned();
        entries.push((relative, relative_str));
    }

    // A `.gitignore` never suppresses a tracked index path: tracked paths stay
    // tracked even after an ignore pattern is added, and the ignore file itself
    // may be untracked. `.mempalaceignore` is the explicit additional exclusion;
    // it is read from the root and from every directory that holds a tracked
    // entry so nested files apply.
    let mut ignore_matcher = IgnoreMatcher::new(root);
    let mut tracked_dirs = BTreeSet::new();
    for (relative, _) in &entries {
        if let Some(parent) = relative.parent() {
            if !parent.as_os_str().is_empty() {
                tracked_dirs.insert(parent.to_path_buf());
            }
        }
    }
    ignore_matcher.load_directory(Path::new(""), false)?;
    for dir in tracked_dirs {
        ignore_matcher.load_directory(&dir, false)?;
    }

    let worktree_skip = linked_worktree_paths(root);
    let mut sources = Vec::new();
    let mut skips = Vec::new();

    for (relative, relative_str) in entries {
        let absolute = root.join(&relative);

        let file_type = match fs::metadata(&absolute) {
            Ok(metadata) => metadata,
            // The index can still name a file that has been deleted from the
            // working tree since it was staged.
            Err(_) => {
                skipped += 1;
                continue;
            }
        };
        if !file_type.is_file() {
            // Submodules (gitlinks) and other non-file index entries.
            skipped += 1;
            continue;
        }
        // Linked Git worktrees are separate checkouts; the main index never
        // lists their files, but keep the exclusion explicit and defensive.
        if worktree_skip.contains(&absolute) {
            skipped += 1;
            continue;
        }
        if ignore_matcher.matches_with_ancestors(&relative_str) {
            skipped += 1;
            continue;
        }
        let file_name =
            relative.file_name().and_then(|value| value.to_str()).unwrap_or_default();
        match project_eligibility(&absolute, file_name) {
            Eligibility::Eligible => sources.push(ProjectSource {
                absolute_path: absolute,
                relative_path: relative_str,
            }),
            eligibility => {
                skipped += 1;
                record_secret_skip(eligibility, relative_str, &mut skips);
            }
        }
    }

    sources.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(ProjectSourceDiscovery {
        sources,
        basis: ProjectSourceBasis::GitIndex,
        skipped,
        skips,
    })
}

/// Walk `root` with git-compatible ignore handling, keeping the project-eligible
/// subset. Used for non-Git roots; the `core.excludesFile` global excludes file
/// still applies, but `$GIT_DIR/info/exclude` does not (no git work tree).
fn discover_filesystem_sources(root: &Path) -> Result<ProjectSourceDiscovery> {
    let report = discover_files(root, true, project_eligibility)?;
    Ok(ProjectSourceDiscovery {
        sources: report.files,
        basis: ProjectSourceBasis::Filesystem,
        skipped: report.ignored_files,
        skips: report.skips,
    })
}

/// Returns the eligibility of `path` (with `file_name`) for project mining.
///
/// The secret-shaped path denylist is checked first, before any content read
/// (shebang or binary sniff), so secret paths are withheld without opening the
/// file. Non-secret rejections (extension mismatch, lockfile, palace config,
/// binary sniff) are counted but not recorded as skip records.
fn project_eligibility(path: &Path, file_name: &str) -> Eligibility {
    if let Some(eligibility) = project_skip_by_name(file_name) {
        return eligibility;
    }
    let raw_ext = path.extension().and_then(|value| value.to_str()).unwrap_or_default();
    let has_ext = !raw_ext.is_empty();
    let normalized_suffix = format!(".{}", raw_ext.to_ascii_lowercase());
    let accepted = (has_ext && PROJECT_READABLE_EXTENSIONS.contains(&normalized_suffix.as_str()))
        || (!has_ext
            && (PROJECT_READABLE_BASENAMES.contains(&file_name) || has_shebang(path)));
    // Binary sniff: exclude files with a NUL byte in the first 8 KiB even
    // when the extension claims text (misnamed binaries).
    if !accepted || looks_binary(path) {
        return Eligibility::Excluded;
    }
    Eligibility::Eligible
}

/// Internal discovery used by project mining and remote batch preparation.
/// Mirrors [`discover_project_sources`] and reports into the legacy shape.
fn discover_project_files(root: &Path) -> Result<DiscoveryReport> {
    let discovery = discover_project_sources(root)?;
    Ok(DiscoveryReport {
        files: discovery.sources,
        ignored_files: discovery.skipped,
        skips: discovery.skips,
    })
}

/// Discovery used by branch-delta mining, which deliberately mines untracked
/// (non-ignored) working-tree files in addition to changed tracked files.
/// Keeps the filesystem walk with git-compatible ignore handling (including
/// `$GIT_DIR/info/exclude` for Git-backed roots and the `core.excludesFile`
/// global file); linked Git worktrees are still skipped.
fn discover_project_files_with_untracked(root: &Path) -> Result<DiscoveryReport> {
    discover_files(root, true, project_eligibility)
}

fn discover_conversation_files(root: &Path) -> Result<DiscoveryReport> {
    let extension_set = CONVO_EXTENSIONS.iter().copied().collect::<BTreeSet<_>>();
    discover_files(root, false, move |path, _file_name| {
        let suffix = path.extension().and_then(|value| value.to_str()).unwrap_or_default();
        let normalized_suffix = format!(".{}", suffix.to_ascii_lowercase());
        if extension_set.contains(normalized_suffix.as_str()) {
            Eligibility::Eligible
        } else {
            Eligibility::Excluded
        }
    })
}

fn apply_limit(
    files: Vec<ProjectSource>,
    limit: Option<usize>,
) -> Box<dyn Iterator<Item = ProjectSource>> {
    match limit {
        Some(limit) => Box::new(files.into_iter().take(limit)),
        None => Box::new(files.into_iter()),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiscoveryReport {
    files: Vec<ProjectSource>,
    ignored_files: usize,
    skips: Vec<ProjectSourceSkip>,
}

/// Run `git worktree list --porcelain -z` from `root` and return the paths of
/// every linked worktree (the main worktree is excluded). Returns an
/// empty set when git is unavailable, the root is not inside a git repository,
/// or there are no linked worktrees.
fn linked_worktree_paths(root: &Path) -> BTreeSet<PathBuf> {
    let output = match Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["worktree", "list", "--porcelain", "-z"])
        .output()
    {
        Ok(o) if o.status.success() => o,
        _ => return BTreeSet::new(),
    };

    parse_linked_worktree_paths(&output.stdout)
        .into_iter()
        // Align Git's recorded paths with the canonical walk root, including
        // worktrees registered through a symlinked path.
        .map(|path| path.canonicalize().unwrap_or(path))
        .collect()
}

fn parse_linked_worktree_paths(output: &[u8]) -> BTreeSet<PathBuf> {
    let mut paths = BTreeSet::new();
    let mut is_first = true;
    for field in output.split(|byte| *byte == b'\0') {
        if let Some(path_bytes) = field.strip_prefix(b"worktree ") {
            if is_first {
                // The first "worktree" entry is the main worktree — keep it.
                is_first = false;
                continue;
            }
            // Git's -z output preserves filesystem path bytes, including
            // newlines and non-UTF-8 Unix filenames.
            if let Some(path) = path_from_git_bytes(path_bytes) {
                paths.insert(path);
            }
        }
    }
    paths
}

#[cfg(unix)]
fn path_from_git_bytes(bytes: &[u8]) -> Option<PathBuf> {
    Some(PathBuf::from(std::ffi::OsString::from_vec(bytes.to_vec())))
}

#[cfg(not(unix))]
fn path_from_git_bytes(bytes: &[u8]) -> Option<PathBuf> {
    String::from_utf8(bytes.to_vec()).ok().map(PathBuf::from)
}

/// Walk `root` applying ignore rules (worktree `.gitignore`/`.mempalaceignore`
/// plus `$GIT_DIR/info/exclude` for Git-backed roots and the
/// `core.excludesFile` global file for any walk), accepting files for which
/// `accept_file` returns [`Eligibility::Eligible`]. The closure receives the
/// absolute path and the (lossy) file name; everything it rejects counts toward
/// `ignored_files`, and secret-denylist rejections are also recorded as skip
/// records.
fn discover_files(
    root: &Path,
    skip_linked_worktrees: bool,
    accept_file: impl Fn(&Path, &str) -> Eligibility,
) -> Result<DiscoveryReport> {
    // Git reports worktree paths as absolute, so use an absolute root when
    // comparing them during project discovery.
    let root = if skip_linked_worktrees {
        root.canonicalize().unwrap_or_else(|_| root.to_path_buf())
    } else {
        root.to_path_buf()
    };
    let mut ignore_matcher = IgnoreMatcher::new(&root);
    let mut ignored_files = 0;
    let mut files = Vec::new();
    let mut skips = Vec::new();
    // Repository-level excludes (`$GIT_DIR/info/exclude` for Git-backed roots,
    // `core.excludesFile` for any walk) apply at every depth.
    ignore_matcher.load_repo_excludes()?;
    let mut stack = vec![root.to_path_buf()];
    // Pre-compute linked worktree paths so we can skip them during the walk.
    let worktree_skip = skip_linked_worktrees
        .then(|| linked_worktree_paths(&root))
        .unwrap_or_default();

    while let Some(dir) = stack.pop() {
        // Nested `.gitignore`/`.mempalaceignore` files apply to the entries of
        // the directory that contains them. Directories that are themselves
        // ignored are never descended into, so their ignore files never load —
        // matching git's behaviour.
        let dir_rel = relative_path(&root, &dir)?;
        ignore_matcher.load_directory(Path::new(&dir_rel), true)?;

        let read_dir =
            fs::read_dir(&dir).map_err(|source| IngestError::Io { path: dir.clone(), source })?;
        for entry in read_dir {
            let entry = entry.map_err(|source| IngestError::Io { path: dir.clone(), source })?;
            let path = entry.path();
            let file_type = entry
                .file_type()
                .map_err(|source| IngestError::Io { path: path.clone(), source })?;
            let relative = relative_path(&root, &path)?;
            if file_type.is_dir() {
                if ignore_matcher.matches(&relative, true) {
                    ignored_files += 1;
                    continue;
                }
                // Skip linked git worktrees: they are duplicate checkouts of the
                // same repository and would produce redundant drawers.
                if worktree_skip.contains(&path) {
                    ignored_files += 1;
                    continue;
                }
                stack.push(path);
                continue;
            }

            if ignore_matcher.matches(&relative, false) {
                ignored_files += 1;
                continue;
            }

            let file_name = path.file_name().and_then(|value| value.to_str()).unwrap_or_default();
            match accept_file(&path, file_name) {
                Eligibility::Eligible => {
                    files.push(ProjectSource { absolute_path: path, relative_path: relative });
                }
                eligibility => {
                    ignored_files += 1;
                    record_secret_skip(eligibility, relative, &mut skips);
                }
            }
        }
    }

    files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(DiscoveryReport { files, ignored_files, skips })
}

impl IgnoreMatcher {
    /// Create a matcher over `root` with the built-in skip directories; no
    /// ignore files are parsed until [`IgnoreMatcher::load_directory`] runs.
    fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
            skip_dirs: DEFAULT_SKIP_DIRS.iter().map(|entry| (*entry).to_owned()).collect(),
            rules: Vec::new(),
            loaded: BTreeSet::new(),
            next_order: 0,
        }
    }

    /// Parse the ignore files in `dir_rel` (relative to the root; an empty path
    /// is the root itself). When `load_gitignore` is set, `.gitignore` is read
    /// first and `.mempalaceignore` second, so mempalace-specific rules take
    /// precedence within the same directory. Idempotent per directory.
    fn load_directory(&mut self, dir_rel: &Path, load_gitignore: bool) -> Result<()> {
        if !self.loaded.insert(dir_rel.to_path_buf()) {
            return Ok(());
        }
        let scope = dir_rel.to_string_lossy().replace('\\', "/");
        let scope_depth = if scope.is_empty() { 0 } else { scope.split('/').count() };

        let mut file_names = Vec::new();
        if load_gitignore {
            file_names.push(".gitignore");
        }
        file_names.push(".mempalaceignore");

        for file_name in file_names {
            let path = self.root.join(dir_rel).join(file_name);
            let body = match fs::read_to_string(&path) {
                Ok(body) => body,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(source) => return Err(IngestError::Io { path, source }),
            };
            for line in body.lines() {
                let Some(pattern) = parse_ignore_pattern(line) else { continue };
                self.rules.push(IgnoreRule {
                    pattern,
                    scope: scope.clone(),
                    scope_depth,
                    tier: IgnoreTier::Worktree,
                    order: self.next_order,
                });
                self.next_order += 1;
            }
        }
        self.sort_rules();
        Ok(())
    }

    /// Parse the repository-level ignore sources — `$GIT_DIR/info/exclude` and
    /// the `core.excludesFile` global file — whose patterns are anchored at the
    /// repository root and have lower precedence than any `.gitignore`. These
    /// run before the walk so the rules apply at every depth. `info/exclude`
    /// is a git concept and only applies inside a Git work tree; the global
    /// excludes file is user-level configuration and applies to any walk,
    /// including the non-Git filesystem fallback.
    fn load_repo_excludes(&mut self) -> Result<()> {
        if git_is_backed(&self.root) {
            if let Some(path) = repo_info_exclude_path(&self.root) {
                self.load_pattern_file(&path, IgnoreTier::RepoExclude)?;
            }
        }
        if let Some(path) = global_excludes_path(&self.root) {
            self.load_pattern_file(&path, IgnoreTier::GlobalExclude)?;
        }
        Ok(())
    }

    /// Parse the patterns in an absolute exclude file (root-scoped) into rules
    /// at `tier`. A missing file is not an error, matching git's behaviour of
    /// treating unreadable/missing exclude files as empty.
    fn load_pattern_file(&mut self, path: &Path, tier: IgnoreTier) -> Result<()> {
        let body = match fs::read_to_string(path) {
            Ok(body) => body,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(source) => return Err(IngestError::Io { path: path.to_path_buf(), source }),
        };
        for line in body.lines() {
            let Some(pattern) = parse_ignore_pattern(line) else { continue };
            self.rules.push(IgnoreRule {
                pattern,
                scope: String::new(),
                scope_depth: 0,
                tier,
                order: self.next_order,
            });
            self.next_order += 1;
        }
        self.sort_rules();
        Ok(())
    }

    /// Sort rules so that higher-precedence sources and scopes come last: the
    /// last matching rule wins, so later elements override earlier ones.
    fn sort_rules(&mut self) {
        // Deeper scopes override shallower ones; within a scope, later rules
        // override earlier ones. Cross-source, worktree files beat
        // `info/exclude`, which beats the global excludes file.
        self.rules.sort_by(|left, right| {
            (left.tier, left.scope_depth, left.order)
                .cmp(&(right.tier, right.scope_depth, right.order))
        });
    }

    /// Returns `true` when `relative_path` (relative to the root) is ignored:
    /// either a built-in skip directory, or the highest-precedence matching
    /// ignore pattern is a non-negated one.
    fn matches(&self, relative_path: &str, is_dir: bool) -> bool {
        let normalized = relative_path.replace('\\', "/");
        if self.matches_skip_dir(&normalized, is_dir) {
            return true;
        }
        let mut ignored = false;
        let mut decided = false;
        for rule in &self.rules {
            let Some(rest) = rule.scope_relative(&normalized) else { continue };
            if !rule.pattern.matches_path(rest, is_dir) {
                continue;
            }
            ignored = !rule.pattern.negated;
            decided = true;
        }
        decided && ignored
    }

    /// Returns `true` when `relative_path` (a file) or any of its ancestor
    /// directories is ignored. Used in git-index mode where files are matched
    /// directly instead of during a walk: a directory-only rule such as
    /// `build/` must exclude the tracked files beneath `build`, and (as in git)
    /// a negated pattern cannot re-include a file under an excluded directory.
    fn matches_with_ancestors(&self, relative_path: &str) -> bool {
        let normalized = relative_path.replace('\\', "/");
        if self.matches(&normalized, false) {
            return true;
        }
        let mut parent = normalized.rsplit_once('/').map(|(parent, _)| parent);
        while let Some(dir) = parent {
            if self.matches(dir, true) {
                return true;
            }
            parent = dir.rsplit_once('/').map(|(parent, _)| parent);
        }
        false
    }

    /// Built-in skip directories match any *directory* component of the path:
    /// a directory named `node_modules` is skipped along with everything under
    /// it, but a file that merely shares the name is not.
    fn matches_skip_dir(&self, normalized: &str, is_dir: bool) -> bool {
        let parts = normalized.split('/').collect::<Vec<_>>();
        let candidates: &[&str] =
            if is_dir { &parts } else { &parts[..parts.len().saturating_sub(1)] };
        self.skip_dirs.iter().any(|name| candidates.contains(&name.as_str()))
    }
}

impl IgnoreRule {
    /// Returns the portion of `path` (root-relative) that lives under this
    /// rule's scope directory, or `None` when the rule does not apply.
    fn scope_relative<'a>(&self, path: &'a str) -> Option<&'a str> {
        if self.scope.is_empty() {
            return Some(path);
        }
        path.strip_prefix(&format!("{}/", self.scope))
    }
}

impl IgnorePattern {
    fn matches_path(&self, path: &str, is_dir: bool) -> bool {
        if self.directory_only && !is_dir {
            return false;
        }
        let components = path.split('/').collect::<Vec<_>>();
        if self.anchored {
            glob_match(&self.parts, &components)
        } else {
            let Some(basename) = self.parts.first() else { return false };
            components.iter().any(|component| glob_component_match(basename, component))
        }
    }
}

/// Parse a single ignore-file line into a pattern, or `None` for blank lines,
/// comments, and empty (after `!`/trailing-slash stripping) patterns.
fn parse_ignore_pattern(line: &str) -> Option<IgnorePattern> {
    let line = strip_unescaped_trailing_whitespace(line);
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let (negated, rest) = match line.strip_prefix('!') {
        Some(rest) => (true, rest),
        None => (false, line),
    };
    // A leading backslash escapes a literal `#`/`!` so those characters can
    // open a real pattern (gitignore(5)). For any other character the backslash
    // is part of the glob itself and must survive for the matcher: `\*` targets
    // a file literally named `*`, it must not become an ignore-everything `*`.
    let rest = if rest.starts_with("\\#") || rest.starts_with("\\!") {
        &rest[1..]
    } else {
        rest
    };
    if rest.is_empty() {
        return None;
    }
    let (directory_only, body) = match rest.strip_suffix('/') {
        Some(body) => (true, body),
        None => (false, rest),
    };
    if body.is_empty() {
        return None;
    }
    // A pattern with a slash (other than a trailing one) is anchored to the
    // ignore file's directory; a leading slash anchors it explicitly.
    let (anchored, pattern_body) = if let Some(stripped) = body.strip_prefix('/') {
        (true, stripped)
    } else if body.contains('/') {
        (true, body)
    } else {
        (false, body)
    };
    if pattern_body.is_empty() {
        return None;
    }
    let parts = pattern_body.split('/').map(str::to_owned).collect();
    Some(IgnorePattern { negated, directory_only, anchored, parts })
}

/// Strip trailing whitespace from a pattern line, but only when it is not
/// escaped: git ignores a trailing space but treats `foo\ ` as a pattern for a
/// filename literally ending in a space, so the `\ ` must survive.
fn strip_unescaped_trailing_whitespace(line: &str) -> &str {
    let mut end = line.len();
    let bytes = line.as_bytes();
    while end > 0 {
        let byte = bytes[end - 1];
        let is_whitespace = matches!(byte, b' ' | b'\t' | b'\r' | b'\x0b' | b'\x0c');
        if !is_whitespace || (end >= 2 && bytes[end - 2] == b'\\') {
            break;
        }
        end -= 1;
    }
    &line[..end]
}

/// Match a `/`-split glob pattern against a `/`-split path, where a middle
/// `**` spans zero or more path components, a *trailing* `**` spans one or
/// more (git's `abc/**` "everything inside" rule), and every other component
/// uses git glob rules.
fn glob_match(parts: &[String], path: &[&str]) -> bool {
    fn go(parts: &[String], path: &[&str]) -> bool {
        if parts.is_empty() {
            return path.is_empty();
        }
        if parts[0] == "**" {
            // A trailing `/**` matches everything inside the matched prefix
            // but not the prefix itself: `abc/**` must not match `abc`, or
            // git's `abc/**` + `!abc/keep.md` re-inclusion could never be
            // evaluated. A trailing `**` therefore consumes at least one
            // component; a middle `**` may consume none.
            let min = if parts.len() == 1 { 1 } else { 0 };
            (min..=path.len()).any(|index| go(&parts[1..], &path[index..]))
        } else if path.is_empty() {
            false
        } else {
            glob_component_match(&parts[0], path[0]) && go(&parts[1..], &path[1..])
        }
    }
    go(parts, path)
}

/// Match a single glob component against text: `*` spans any run of
/// characters, `?` matches one character, `[...]` is a character class, and
/// `\` escapes the following character.
fn glob_component_match(pattern: &str, text: &str) -> bool {
    let pattern = pattern.chars().collect::<Vec<_>>();
    let text = text.chars().collect::<Vec<_>>();

    fn go(pattern: &[char], text: &[char]) -> bool {
        if pattern.is_empty() {
            return text.is_empty();
        }
        match pattern[0] {
            '*' => (0..=text.len()).any(|index| go(&pattern[1..], &text[index..])),
            '?' => !text.is_empty() && go(&pattern[1..], &text[1..]),
            '[' => {
                if text.is_empty() {
                    return false;
                }
                match match_char_class(pattern, text[0]) {
                    Some(next) => go(&pattern[next..], &text[1..]),
                    None => false,
                }
            }
            '\\' => {
                pattern.len() >= 2
                    && !text.is_empty()
                    && pattern[1] == text[0]
                    && go(&pattern[2..], &text[1..])
            }
            literal => !text.is_empty() && literal == text[0] && go(&pattern[1..], &text[1..]),
        }
    }

    go(&pattern, &text)
}

/// Match the `[...]` character class at the start of `pattern` (which begins
/// with `[`) against `text_char`, returning the index just past the closing
/// `]` on a match, or `None` for an unterminated class. Supports negation
/// (`!`/`^`) and `a-z` ranges.
fn match_char_class(pattern: &[char], text_char: char) -> Option<usize> {
    debug_assert_eq!(pattern[0], '[');
    let mut index = 1;
    let mut negate = false;
    if index < pattern.len() && (pattern[index] == '!' || pattern[index] == '^') {
        negate = true;
        index += 1;
    }
    let mut matched = false;
    // A `]` immediately after `[` (or after `[!`) is a literal `]`, so the
    // class only closes once at least one item has been consumed.
    let mut can_close = false;
    while index < pattern.len() {
        if pattern[index] == ']' && can_close {
            return Some(index + 1).filter(|_| matched != negate);
        }
        if index + 2 < pattern.len() && pattern[index + 1] == '-' && pattern[index + 2] != ']' {
            if text_char >= pattern[index] && text_char <= pattern[index + 2] {
                matched = true;
            }
            index += 3;
        } else {
            if pattern[index] == text_char {
                matched = true;
            }
            index += 1;
        }
        can_close = true;
    }
    None
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TextDocument {
    content: String,
    content_hash: String,
    truncated: bool,
    /// Byte offset of the start of the trimmed content within the file bytes.
    /// Zero when the file starts with non-whitespace (or when `valid_utf8` is false).
    trim_offset: usize,
    /// True when the effective (possibly truncated) bytes are valid UTF-8.
    /// When false, `trim_offset` is meaningless and no locator should be produced.
    valid_utf8: bool,
    /// The raw file bytes, kept so locator line numbers can be computed without
    /// a second disk read (which could also race with concurrent file edits).
    raw_bytes: Vec<u8>,
}

fn read_text_document(path: &Path) -> Result<TextDocument> {
    let bytes =
        fs::read(path).map_err(|source| IngestError::Io { path: path.to_path_buf(), source })?;
    let content_hash = hash_bytes(&bytes);
    let truncated = bytes.len() > LARGE_FILE_TRUNCATION_BYTES;
    let effective =
        if truncated { &bytes[..LARGE_FILE_TRUNCATION_BYTES] } else { bytes.as_slice() };

    match std::str::from_utf8(effective) {
        Ok(s) => {
            // Compute how many bytes are trimmed from the start (valid char boundary).
            let trimmed = s.trim_start();
            let trim_offset = s.len() - trimmed.len();
            let content = trimmed.trim_end().to_owned();
            Ok(TextDocument {
                content,
                content_hash,
                truncated,
                trim_offset,
                valid_utf8: true,
                raw_bytes: bytes,
            })
        }
        Err(_) => {
            // Non-UTF-8: fall back to lossy conversion, no locator basis.
            let content = String::from_utf8_lossy(effective).trim().to_owned();
            Ok(TextDocument {
                content,
                content_hash,
                truncated,
                trim_offset: 0,
                valid_utf8: false,
                raw_bytes: bytes,
            })
        }
    }
}

fn detect_project_room(relative_path: &Path, content: &str, rooms: &[ProjectRoomConfig]) -> String {
    let relative = relative_path.to_string_lossy().to_ascii_lowercase();
    let filename = relative_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let content_lower = content.chars().take(2_000).collect::<String>().to_ascii_lowercase();
    let parts = relative.split('/').collect::<Vec<_>>();

    for part in parts.iter().take(parts.len().saturating_sub(1)) {
        for room in rooms {
            let room_name = canonicalize_label(&room.name);
            if labels_overlap(&room_name, part) {
                return canonicalize_label(&room.name);
            }
        }
    }

    for room in rooms {
        let room_name = canonicalize_label(&room.name);
        if labels_overlap(&room_name, &filename) {
            return canonicalize_label(&room.name);
        }
    }

    let mut best_room = None::<String>;
    let mut best_score = 0usize;
    for room in rooms {
        let mut score = count_term_matches(&content_lower, &room.name.to_ascii_lowercase());
        for keyword in &room.keywords {
            score += count_term_matches(&content_lower, &keyword.to_ascii_lowercase());
        }
        if score > best_score {
            best_score = score;
            best_room = Some(room.name.clone());
        }
    }

    best_room
        .filter(|_| best_score > 0)
        .map(|room| canonicalize_label(&room))
        .unwrap_or_else(|| "general".to_owned())
}

/// Chunk the project text and track byte ranges within `content` (the already-trimmed string).
///
/// `track_offsets` — when true, `Chunk.byte_range` is set to `(start, end)` within `content`.
/// The caller is responsible for adding `trim_offset` to map these into file bytes.
fn chunk_project_text(content: &str, track_offsets: bool) -> Vec<Chunk> {
    // content is already trimmed by the caller (read_text_document returns trimmed string).
    if content.is_empty() {
        return Vec::new();
    }

    let mut chunks = Vec::new();
    let mut start = 0usize;

    while start < content.len() {
        let mut end = (start + PROJECT_CHUNK_SIZE).min(content.len());
        end = align_to_char_boundary(content, end);
        if end < content.len() {
            if let Some(split) = find_boundary(content, start, end, "\n\n") {
                if split > start + PROJECT_CHUNK_SIZE / 2 {
                    end = split;
                }
            } else if let Some(split) = find_boundary(content, start, end, "\n") {
                if split > start + PROJECT_CHUNK_SIZE / 2 {
                    end = split;
                }
            }
        }

        let raw_slice = &content[start..end];
        // Compute trim offsets within the raw slice for accurate byte ranges.
        let leading = raw_slice.len() - raw_slice.trim_start().len();
        let chunk_str = raw_slice.trim();
        if chunk_str.len() >= PROJECT_MIN_CHUNK_SIZE {
            let byte_range = if track_offsets {
                let chunk_start = start + leading;
                let chunk_end = chunk_start + chunk_str.len();
                Some((chunk_start as u64, chunk_end as u64))
            } else {
                None
            };
            chunks.push(Chunk {
                content: chunk_str.to_owned(),
                chunk_index: u32::try_from(chunks.len()).unwrap_or(u32::MAX),
                room_hint: None,
                date_hint: None,
                byte_range,
            });
        }

        if end == content.len() {
            break;
        }
        start = align_to_char_boundary(content, end.saturating_sub(PROJECT_CHUNK_OVERLAP));
    }

    chunks
}

fn align_to_char_boundary(content: &str, index: usize) -> usize {
    let mut aligned = index.min(content.len());
    while aligned > 0 && !content.is_char_boundary(aligned) {
        aligned -= 1;
    }
    aligned
}

fn find_boundary(content: &str, start: usize, end: usize, delimiter: &str) -> Option<usize> {
    content[start..end].rfind(delimiter).map(|index| start + index)
}

/// Compute 1-based line numbers for a sorted list of `(byte_start, byte_end)` pairs
/// within `file_bytes`.  Returns a parallel `Vec<(line_start, line_end)>`.
///
/// Offsets in `chunks` must be valid byte offsets into `file_bytes` and must be
/// sorted by `byte_start` for the incremental counting to work.
fn compute_line_numbers(file_bytes: &[u8], chunks: &[(u64, u64)]) -> Vec<(u32, u32)> {
    if chunks.is_empty() {
        return Vec::new();
    }

    // Walk through the bytes once in order, counting newlines.
    let mut results = Vec::with_capacity(chunks.len());
    // current_line is 1-based; newlines_before_offset[i] gives how many \n
    // appear before offset i (0-based).
    let mut newline_count = 0u32; // newlines seen so far
    let mut byte_pos = 0usize;

    // For each chunk we need the newline count just before byte_start and just
    // before byte_end.  We collect the sorted query offsets then process them.
    let mut queries: Vec<(u64, usize, bool)> = Vec::new(); // (offset, chunk_index, is_end)
    for (i, &(start, end)) in chunks.iter().enumerate() {
        queries.push((start, i, false));
        // line_end counts up to (but not including) the last byte; use saturating_sub.
        queries.push((end.saturating_sub(1), i, true));
    }
    queries.sort_unstable_by_key(|&(off, idx, is_end)| (off, idx, !is_end));

    let mut line_starts = vec![0u32; chunks.len()];
    let mut line_ends = vec![0u32; chunks.len()];

    for (target_offset, chunk_idx, is_end) in queries {
        // try_from instead of `as`: clamp instead of silently truncating on
        // 32-bit targets (the walk below stops at file_bytes.len() anyway).
        let target = usize::try_from(target_offset).unwrap_or(usize::MAX);
        // Advance byte_pos to target, counting newlines.
        while byte_pos < target && byte_pos < file_bytes.len() {
            if file_bytes[byte_pos] == b'\n' {
                newline_count += 1;
            }
            byte_pos += 1;
        }
        let line = newline_count + 1; // 1-based
        if is_end {
            line_ends[chunk_idx] = line;
        } else {
            line_starts[chunk_idx] = line;
        }
    }

    for i in 0..chunks.len() {
        results.push((line_starts[i], line_ends[i]));
    }
    results
}

/// Run `git -C <root> rev-parse HEAD` and return the trimmed stdout, or `None`
/// if git is unavailable, the directory is not a repo, or any error occurs.
fn resolve_commit_hash(root: &Path) -> Option<String> {
    let output = Command::new("git")
        .args(["-C", &root.to_string_lossy(), "rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let s = std::str::from_utf8(&output.stdout).ok()?.trim().to_owned();
    if s.is_empty() { None } else { Some(s) }
}

fn normalize_conversation(
    path: &Path,
    bytes: &[u8],
) -> std::result::Result<NormalizedConversation, ConversationNormalizeError> {
    let text = String::from_utf8_lossy(bytes);
    if text.trim().is_empty() {
        return Ok(NormalizedConversation { transcript: String::new(), messages: Vec::new() });
    }

    let lines = text.lines().collect::<Vec<_>>();
    if lines.iter().filter(|line| line.trim_start().starts_with('>')).count() >= 3 {
        let transcript = text.into_owned();
        return Ok(NormalizedConversation {
            messages: transcript_to_messages(&transcript),
            transcript,
        });
    }

    let extension = path.extension().and_then(|value| value.to_str()).unwrap_or_default();
    let trimmed = text.trim_start();
    if matches!(extension, "json" | "jsonl") || trimmed.starts_with('{') || trimmed.starts_with('[')
    {
        if let Some(transcript) = try_claude_code_jsonl(text.as_ref()) {
            return Ok(transcript);
        }

        let value: Value = serde_json::from_str(text.as_ref())
            .map_err(|_| ConversationNormalizeError::Malformed)?;
        if let Some(transcript) = try_claude_ai_json(&value) {
            return Ok(transcript);
        }
        if let Some(transcript) = try_chatgpt_json(&value) {
            return Ok(transcript);
        }
        if let Some(transcript) = try_slack_json(&value) {
            return Ok(transcript);
        }
        return Err(ConversationNormalizeError::Unsupported);
    }

    Ok(NormalizedConversation { transcript: text.into_owned(), messages: Vec::new() })
}

fn try_claude_code_jsonl(content: &str) -> Option<NormalizedConversation> {
    let mut messages = Vec::new();
    for line in content.lines().map(str::trim).filter(|line| !line.is_empty()) {
        let entry: Value = serde_json::from_str(line).ok()?;
        let object = entry.as_object()?;
        let message_type = object.get("type")?.as_str()?;
        let message = object.get("message")?;
        let text = extract_content(message.get("content")?)?;
        let timestamp = message.get("created_at").and_then(parse_timestamp_value);
        match message_type {
            "human" => messages.push(Message {
                role: MessageRole::User,
                content: text,
                timestamp,
                speaker_id: None,
            }),
            "assistant" => messages.push(Message {
                role: MessageRole::Assistant,
                content: text,
                timestamp,
                speaker_id: None,
            }),
            _ => {}
        }
    }

    if messages.len() >= 2 {
        Some(NormalizedConversation { transcript: messages_to_transcript(&messages), messages })
    } else {
        None
    }
}

fn try_claude_ai_json(data: &Value) -> Option<NormalizedConversation> {
    let array = if let Some(object) = data.as_object() {
        object.get("messages").or_else(|| object.get("chat_messages"))?.as_array()?
    } else {
        data.as_array()?
    };
    let mut messages = Vec::new();
    for item in array {
        let object = item.as_object()?;
        let role = object.get("role")?.as_str()?;
        let content = extract_content(object.get("content")?)?;
        let timestamp = object
            .get("timestamp")
            .or_else(|| object.get("created_at"))
            .and_then(parse_timestamp_value);
        match role {
            "user" | "human" => messages.push(Message {
                role: MessageRole::User,
                content,
                timestamp,
                speaker_id: None,
            }),
            "assistant" | "ai" => messages.push(Message {
                role: MessageRole::Assistant,
                content,
                timestamp,
                speaker_id: None,
            }),
            _ => {}
        }
    }

    if messages.len() >= 2 {
        Some(NormalizedConversation { transcript: messages_to_transcript(&messages), messages })
    } else {
        None
    }
}

fn try_chatgpt_json(data: &Value) -> Option<NormalizedConversation> {
    let mapping = data.get("mapping")?.as_object()?;
    let mut root_id = None::<String>;
    let mut fallback_root = None::<String>;
    for (node_id, node) in mapping {
        let object = node.as_object()?;
        if object.get("parent").is_none() || object.get("parent").is_some_and(Value::is_null) {
            if object.get("message").is_none() || object.get("message").is_some_and(Value::is_null)
            {
                root_id = Some(node_id.clone());
                break;
            }
            if fallback_root.is_none() {
                fallback_root = Some(node_id.clone());
            }
        }
    }

    let mut current_id = root_id.or(fallback_root)?;
    let mut visited = BTreeSet::new();
    let mut messages = Vec::new();
    while visited.insert(current_id.clone()) {
        let node = mapping.get(&current_id)?.as_object()?;
        if let Some(message) = node.get("message").and_then(Value::as_object) {
            let role = message.get("author")?.get("role")?.as_str()?;
            let text = message
                .get("content")?
                .get("parts")?
                .as_array()?
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(" ")
                .trim()
                .to_owned();
            if !text.is_empty() {
                let timestamp = message
                    .get("create_time")
                    .or_else(|| message.get("update_time"))
                    .and_then(parse_timestamp_value);
                match role {
                    "user" => messages.push(Message {
                        role: MessageRole::User,
                        content: text,
                        timestamp,
                        speaker_id: None,
                    }),
                    "assistant" => messages.push(Message {
                        role: MessageRole::Assistant,
                        content: text,
                        timestamp,
                        speaker_id: None,
                    }),
                    _ => {}
                }
            }
        }

        let children = node.get("children").and_then(Value::as_array);
        let Some(next) = children.and_then(|entries| entries.first()).and_then(Value::as_str)
        else {
            break;
        };
        current_id = next.to_owned();
    }

    if messages.len() >= 2 {
        Some(NormalizedConversation { transcript: messages_to_transcript(&messages), messages })
    } else {
        None
    }
}

fn try_slack_json(data: &Value) -> Option<NormalizedConversation> {
    let entries = data.as_array()?;
    let mut messages = Vec::new();
    let mut seen_users = BTreeMap::<String, MessageRole>::new();
    let mut last_role = MessageRole::Assistant;

    for entry in entries {
        let object = entry.as_object()?;
        if object.get("type")?.as_str()? != "message" {
            continue;
        }
        let user = object
            .get("user")
            .or_else(|| object.get("username"))
            .and_then(Value::as_str)?
            .to_owned();
        let text = object.get("text")?.as_str()?.trim().to_owned();
        if text.is_empty() {
            continue;
        }
        let role = if let Some(existing) = seen_users.get(&user) {
            *existing
        } else {
            let inferred = if seen_users.is_empty() || matches!(last_role, MessageRole::Assistant) {
                MessageRole::User
            } else {
                MessageRole::Assistant
            };
            seen_users.insert(user.clone(), inferred);
            inferred
        };
        last_role = role;
        messages.push(Message {
            role,
            content: text,
            timestamp: object.get("ts").and_then(parse_timestamp_value),
            speaker_id: Some(user),
        });
    }

    if messages.len() >= 2 {
        Some(NormalizedConversation { transcript: messages_to_transcript(&messages), messages })
    } else {
        None
    }
}

fn extract_content(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.trim().to_owned()),
        Value::Array(items) => {
            let parts = items
                .iter()
                .filter_map(|item| match item {
                    Value::String(text) => Some(text.clone()),
                    Value::Object(object) => {
                        object.get("text").and_then(Value::as_str).map(str::to_owned)
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            let combined = parts.join(" ").trim().to_owned();
            if combined.is_empty() { None } else { Some(combined) }
        }
        Value::Object(object) => {
            object.get("text").and_then(Value::as_str).map(|text| text.trim().to_owned())
        }
        _ => None,
    }
}

fn parse_timestamp_value(value: &Value) -> Option<OffsetDateTime> {
    match value {
        Value::String(text) => {
            OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339).ok()
        }
        Value::Number(number) => {
            if let Some(seconds) = number.as_i64() {
                OffsetDateTime::from_unix_timestamp(seconds).ok()
            } else if let Some(seconds) = number.as_f64() {
                OffsetDateTime::from_unix_timestamp(seconds as i64).ok()
            } else {
                None
            }
        }
        _ => None,
    }
}

fn messages_to_transcript(messages: &[Message]) -> String {
    let mut lines = Vec::new();
    let mut index = 0usize;
    while index < messages.len() {
        let message = &messages[index];
        match message.role {
            MessageRole::User => {
                lines.push(format!("> {}", spellcheck_user_text(&message.content)));
                if let Some(reply) = messages.get(index + 1) {
                    if matches!(reply.role, MessageRole::Assistant) {
                        lines.push(reply.content.clone());
                        index += 1;
                    }
                }
            }
            MessageRole::Assistant => {
                lines.push(message.content.clone());
            }
        }
        lines.push(String::new());
        index += 1;
    }
    lines.join("\n")
}

fn transcript_to_messages(transcript: &str) -> Vec<Message> {
    let mut messages = Vec::new();
    let mut pending_user = None::<String>;
    let mut pending_assistant = Vec::<String>::new();

    for line in transcript.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("> ") {
            if let Some(user) = pending_user.take() {
                messages.push(Message {
                    role: MessageRole::User,
                    content: user,
                    timestamp: None,
                    speaker_id: None,
                });
                if !pending_assistant.is_empty() {
                    messages.push(Message {
                        role: MessageRole::Assistant,
                        content: pending_assistant.join("\n"),
                        timestamp: None,
                        speaker_id: None,
                    });
                    pending_assistant.clear();
                }
            }
            pending_user = Some(rest.to_owned());
        } else if !trimmed.is_empty() {
            pending_assistant.push(line.to_owned());
        }
    }

    if let Some(user) = pending_user {
        messages.push(Message {
            role: MessageRole::User,
            content: user,
            timestamp: None,
            speaker_id: None,
        });
        if !pending_assistant.is_empty() {
            messages.push(Message {
                role: MessageRole::Assistant,
                content: pending_assistant.join("\n"),
                timestamp: None,
                speaker_id: None,
            });
        }
    }

    messages
}

fn spellcheck_user_text(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut start = 0usize;

    for (index, ch) in text.char_indices() {
        if ch.is_whitespace() {
            if start < index {
                result.push_str(correct_token(&text[start..index]).as_ref());
            }
            result.push(ch);
            start = index + ch.len_utf8();
        }
    }

    if start < text.len() {
        result.push_str(correct_token(&text[start..]).as_ref());
    }

    result
}

fn correct_token(token: &str) -> Cow<'_, str> {
    let stripped = token.trim_end_matches(|ch: char| ".,!?;:'\")".contains(ch));
    let suffix = &token[stripped.len()..];
    if should_skip_spellcheck(stripped) {
        return Cow::Borrowed(token);
    }

    let lower = stripped.to_ascii_lowercase();
    let Some((_, replacement)) = TYPO_CORRECTIONS.iter().find(|(typo, _)| *typo == lower) else {
        return Cow::Borrowed(token);
    };
    Cow::Owned(format!("{replacement}{suffix}"))
}

fn should_skip_spellcheck(token: &str) -> bool {
    if token.len() < 4 {
        return true;
    }
    if token.chars().any(|ch| ch.is_ascii_digit()) {
        return true;
    }
    if token.contains('-') || token.contains('_') {
        return true;
    }
    if token.contains("://")
        || token.contains("www.")
        || token.contains("~/")
        || token.contains("/Users/")
    {
        return true;
    }
    if token.chars().next().is_some_and(char::is_uppercase) {
        return true;
    }
    token.chars().all(|ch| ch.is_ascii_uppercase() || !ch.is_ascii_alphabetic())
}

fn chunk_exchanges(content: &str) -> Vec<Chunk> {
    let lines = content.lines().collect::<Vec<_>>();
    let quote_lines = lines.iter().filter(|line| line.trim_start().starts_with('>')).count();
    if quote_lines >= 3 { chunk_by_exchange(&lines) } else { chunk_by_paragraph(content) }
}

fn chunk_by_exchange(lines: &[&str]) -> Vec<Chunk> {
    let mut chunks = Vec::new();
    let mut index = 0usize;

    while index < lines.len() {
        let line = lines[index];
        if !line.trim_start().starts_with('>') {
            index += 1;
            continue;
        }

        let user_turn = line.trim().to_owned();
        index += 1;
        let mut assistant_lines = Vec::new();
        while index < lines.len() {
            let next = lines[index].trim();
            if next.starts_with('>') || next.starts_with("---") {
                break;
            }
            if !next.is_empty() {
                assistant_lines.push(next.to_owned());
            }
            index += 1;
        }

        let assistant = assistant_lines.into_iter().take(8).collect::<Vec<_>>().join(" ");
        let content =
            if assistant.is_empty() { user_turn } else { format!("{user_turn}\n{assistant}") };
        if content.trim().len() > CONVO_MIN_CHUNK_SIZE {
            chunks.push(Chunk {
                content,
                chunk_index: u32::try_from(chunks.len()).unwrap_or(u32::MAX),
                room_hint: None,
                date_hint: None,
                byte_range: None,
            });
        }
    }

    chunks
}

fn chunk_by_paragraph(content: &str) -> Vec<Chunk> {
    let paragraphs = content
        .split("\n\n")
        .map(str::trim)
        .filter(|paragraph| !paragraph.is_empty())
        .collect::<Vec<_>>();
    if paragraphs.len() <= 1 && content.lines().count() > 20 {
        let lines = content.lines().collect::<Vec<_>>();
        return lines
            .chunks(25)
            .filter_map(|group| {
                let joined = group.join("\n");
                if joined.trim().len() > CONVO_MIN_CHUNK_SIZE {
                    Some(Chunk {
                        content: joined,
                        chunk_index: 0,
                        room_hint: None,
                        date_hint: None,
                        byte_range: None,
                    })
                } else {
                    None
                }
            })
            .enumerate()
            .map(|(index, mut chunk)| {
                chunk.chunk_index = u32::try_from(index).unwrap_or(u32::MAX);
                chunk
            })
            .collect::<Vec<_>>();
    }

    paragraphs
        .into_iter()
        .filter(|paragraph| paragraph.len() > CONVO_MIN_CHUNK_SIZE)
        .enumerate()
        .map(|(index, paragraph)| Chunk {
            content: paragraph.to_owned(),
            chunk_index: u32::try_from(index).unwrap_or(u32::MAX),
            room_hint: None,
            date_hint: None,
            byte_range: None,
        })
        .collect::<Vec<_>>()
}

fn detect_conversation_room(content: &str) -> String {
    let content_lower = content.chars().take(3_000).collect::<String>().to_ascii_lowercase();
    TOPIC_KEYWORDS
        .iter()
        .map(|(room, keywords)| {
            let score = keywords
                .iter()
                .map(|keyword| count_term_matches(&content_lower, keyword))
                .sum::<usize>();
            ((*room).to_owned(), score)
        })
        .max_by_key(|(_, score)| *score)
        .filter(|(_, score)| *score > 0)
        .map(|(room, _)| room)
        .unwrap_or_else(|| "general".to_owned())
}

fn extract_memories(text: &str) -> Vec<Chunk> {
    let segments = split_into_segments(text);
    let mut memories = Vec::new();

    for segment in segments {
        if segment.trim().len() < 20 {
            continue;
        }
        let prose = extract_prose(&segment);
        let scores = [
            ("decision", score_markers(&prose, DECISION_MARKERS)),
            ("preference", score_markers(&prose, PREFERENCE_MARKERS)),
            ("milestone", score_markers(&prose, MILESTONE_MARKERS)),
            ("problem", score_markers(&prose, PROBLEM_MARKERS)),
            ("emotional", score_markers(&prose, EMOTION_MARKERS)),
        ]
        .into_iter()
        .filter(|(_, score)| *score > 0)
        .collect::<Vec<_>>();

        if scores.is_empty() {
            continue;
        }

        let mut chosen = scores
            .iter()
            .max_by_key(|(_, score)| *score)
            .map(|(kind, _)| (*kind).to_owned())
            .unwrap_or_else(|| "general".to_owned());

        if chosen == "problem" && has_resolution(&prose) {
            chosen = match sentiment(&prose) {
                Sentiment::Positive if prose.to_ascii_lowercase().contains("love") => {
                    "emotional".to_owned()
                }
                _ => "milestone".to_owned(),
            };
        }

        memories.push(Chunk {
            content: segment.trim().to_owned(),
            chunk_index: u32::try_from(memories.len()).unwrap_or(u32::MAX),
            room_hint: Some(chosen),
            date_hint: None,
            byte_range: None,
        });
    }

    memories
}

fn split_into_segments(text: &str) -> Vec<String> {
    let lines = text.lines().collect::<Vec<_>>();
    let turn_count = lines
        .iter()
        .filter(|line| {
            let trimmed = line.trim();
            trimmed.starts_with("> ")
                || trimmed.starts_with("Human:")
                || trimmed.starts_with("User:")
                || trimmed.starts_with("Assistant:")
                || trimmed.starts_with("AI:")
                || trimmed.starts_with("Claude:")
                || trimmed.starts_with("ChatGPT:")
        })
        .count();

    if turn_count >= 3 {
        let mut segments = Vec::new();
        let mut current = Vec::new();
        for line in lines {
            let trimmed = line.trim();
            let is_turn = trimmed.starts_with("> ")
                || trimmed.starts_with("Human:")
                || trimmed.starts_with("User:")
                || trimmed.starts_with("Assistant:")
                || trimmed.starts_with("AI:")
                || trimmed.starts_with("Claude:")
                || trimmed.starts_with("ChatGPT:");
            if is_turn && !current.is_empty() {
                segments.push(current.join("\n"));
                current.clear();
            }
            current.push(line.to_owned());
        }
        if !current.is_empty() {
            segments.push(current.join("\n"));
        }
        return segments;
    }

    let paragraphs = text
        .split("\n\n")
        .map(str::trim)
        .filter(|paragraph| !paragraph.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if paragraphs.len() <= 1 && lines.len() > 20 {
        return lines
            .chunks(25)
            .map(|chunk| chunk.join("\n"))
            .filter(|segment| !segment.trim().is_empty())
            .collect::<Vec<_>>();
    }
    paragraphs
}

fn extract_prose(text: &str) -> String {
    let mut prose = Vec::new();
    let mut in_code = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("```") {
            in_code = !in_code;
            continue;
        }
        if in_code || is_code_line(trimmed) {
            continue;
        }
        prose.push(line);
    }
    let joined = prose.join("\n").trim().to_owned();
    if joined.is_empty() { text.to_owned() } else { joined }
}

fn is_code_line(line: &str) -> bool {
    if line.is_empty() {
        return false;
    }
    let shell_prefixes = [
        "$ ", "# ", "cd ", "source ", "echo ", "export ", "pip ", "npm ", "git ", "python ",
        "bash ", "curl ", "wget ", "mkdir ", "rm ", "cp ", "mv ", "ls ", "cat ", "grep ", "find ",
        "chmod ", "sudo ", "brew ", "docker ",
    ];
    if shell_prefixes.iter().any(|prefix| line.starts_with(prefix)) {
        return true;
    }
    if ["import ", "from ", "def ", "class ", "function ", "const ", "let ", "var ", "return "]
        .iter()
        .any(|prefix| line.starts_with(prefix))
    {
        return true;
    }
    if line.starts_with('|') || line.starts_with("---") || matches!(line, "{" | "}" | "[" | "]") {
        return true;
    }
    let alpha = line.chars().filter(|ch| ch.is_ascii_alphabetic()).count();
    alpha * 10 < line.len().saturating_mul(4) && line.len() > 10
}

fn score_markers(text: &str, markers: &[&str]) -> usize {
    let lower = text.to_ascii_lowercase();
    markers.iter().map(|marker| lower.matches(marker).count()).sum()
}

fn has_resolution(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    ["fixed", "solved", "resolved", "patched", "got it working", "it works", "it worked"]
        .iter()
        .any(|pattern| lower.contains(pattern))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sentiment {
    Positive,
    Negative,
    Neutral,
}

fn sentiment(text: &str) -> Sentiment {
    let lower = text.to_ascii_lowercase();
    let positive = POSITIVE_WORDS.iter().filter(|word| lower.contains(**word)).count();
    let negative = NEGATIVE_WORDS.iter().filter(|word| lower.contains(**word)).count();
    if positive > negative {
        Sentiment::Positive
    } else if negative > positive {
        Sentiment::Negative
    } else {
        Sentiment::Neutral
    }
}

fn relative_path(root: &Path, path: &Path) -> Result<String> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| IngestError::InvalidRelativePath { path: path.to_path_buf() })?;
    let mut components = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(value) => components.push(value.to_string_lossy().into_owned()),
            _ => return Err(IngestError::InvalidRelativePath { path: path.to_path_buf() }),
        }
    }
    Ok(components.join("/"))
}

fn hash_bytes(bytes: &[u8]) -> String {
    mempalace_core::hash_bytes(bytes)
}

fn hash_text(text: &str) -> String {
    mempalace_core::hash_text(text)
}

fn source_key(
    ingest_kind: &str,
    root: &Path,
    wing: &str,
    extract_mode: Option<&str>,
    relative_path: &str,
) -> String {
    let root_key = hash_text(&root.to_string_lossy());
    source_key_with_root_key(ingest_kind, &root_key, wing, extract_mode, relative_path)
}

/// Return the repository-relative project root for a checkout, when the
/// checkout is a subdirectory of a Git repository.  The root repository
/// itself returns `None`; the value uses forward slashes for registry keys.
pub fn project_root_relative(root: &Path) -> Option<String> {
    let output = Command::new("git")
        .args(["-C", &root.to_string_lossy(), "rev-parse", "--show-toplevel"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let git_root = PathBuf::from(String::from_utf8(output.stdout).ok()?.trim())
        .canonicalize()
        .ok()?;
    let canonical_root = root.canonicalize().ok()?;
    let relative = canonical_root.strip_prefix(git_root).ok()?;
    let value = relative.to_string_lossy().replace('\\', "/");
    (!value.is_empty()).then_some(value)
}

/// Derive the durable project ID used by the centralized registry and local
/// project source keys.  An explicit ID is preserved verbatim; otherwise a
/// monorepo subproject is namespaced below its normalized repository identity.
pub fn derive_project_id(root: &Path, wing: &str, explicit_id: Option<&str>) -> String {
    if let Some(explicit_id) = explicit_id.filter(|value| !value.trim().is_empty()) {
        return explicit_id.trim().to_owned();
    }
    let repo_id = derive_repo_id(root, wing);
    match project_root_relative(root) {
        Some(project_root) => format!("{repo_id}#{project_root}"),
        None => repo_id,
    }
}

fn project_source_key(
    ingest_kind: &str,
    project_root_key: &str,
    wing: &str,
    relative_path: &str,
) -> String {
    source_key_with_root_key(ingest_kind, project_root_key, wing, None, relative_path)
}

fn legacy_project_source_key(
    ingest_kind: &str,
    root_key: &str,
    wing: &str,
    relative_path: &str,
) -> String {
    source_key_with_root_key(ingest_kind, root_key, wing, None, relative_path)
}

fn project_branch_source_key(
    ingest_kind: &str,
    project_root_key: &str,
    wing: &str,
    branch: &str,
    relative_path: &str,
) -> String {
    format!("{ingest_kind}:{wing}:{project_root_key}:{branch}:{relative_path}")
}

fn stable_project_root_key(repo_id: &str) -> String {
    hash_text(&format!("project:{repo_id}"))
}

/// Ingest kind for full canonical project mines.
pub const PROJECTS_INGEST_KIND: &str = "projects";
/// Ingest kind for branch-delta project mines (`mine --branch`).
pub const PROJECTS_BRANCH_INGEST_KIND: &str = "projects-branch";

/// Source-key prefix selecting every canonical (`projects`) drawer mined for
/// `project_id` in `wing`.
///
/// `project_id` must match the identity used at mine time: an explicit
/// `--project-id`, otherwise the derived repository identity
/// ([`derive_project_id`]). This mirrors the key layout produced by
/// [`project_source_key`], so the two stay in lockstep if the format changes.
pub fn project_canonical_source_prefix(wing: &str, project_id: &str) -> String {
    format!("{PROJECTS_INGEST_KIND}:{wing}:{}:", stable_project_root_key(project_id))
}

/// Source-key prefix selecting branch-delta (`projects-branch`) drawers for
/// `project_id` in `wing`.
///
/// With `branch = Some(name)` the prefix narrows to a single branch view;
/// `branch = None` matches every branch view of the project. Mirrors
/// [`project_branch_source_key`].
pub fn project_branch_source_prefix(wing: &str, project_id: &str, branch: Option<&str>) -> String {
    let root_key = stable_project_root_key(project_id);
    match branch {
        Some(branch) => format!("{PROJECTS_BRANCH_INGEST_KIND}:{wing}:{root_key}:{branch}:"),
        None => format!("{PROJECTS_BRANCH_INGEST_KIND}:{wing}:{root_key}:"),
    }
}

/// Source-key prefix selecting every drawer of `ingest_kind` mined into `wing`,
/// regardless of project. Broader than the project-scoped prefixes; callers
/// must pair it with a narrowing scope before using it to delete.
pub fn wing_kind_source_prefix(ingest_kind: &str, wing: &str) -> String {
    format!("{ingest_kind}:{wing}:")
}

fn source_key_with_root_key(
    ingest_kind: &str,
    root_key: &str,
    wing: &str,
    extract_mode: Option<&str>,
    relative_path: &str,
) -> String {
    match extract_mode {
        Some(mode) => format!("{ingest_kind}:{wing}:{mode}:{root_key}:{relative_path}"),
        None => format!("{ingest_kind}:{wing}:{root_key}:{relative_path}"),
    }
}

fn project_routing_fingerprint(rooms: &[ProjectRoomConfig]) -> String {
    let serialized = rooms
        .iter()
        .map(|room| {
            format!(
                "{}|{}|{}",
                canonicalize_label(&room.name),
                canonicalize_optional(room.description.as_deref()),
                room.keywords
                    .iter()
                    .map(|keyword| keyword.to_ascii_lowercase())
                    .collect::<Vec<_>>()
                    .join(",")
            )
        })
        .collect::<Vec<_>>()
        .join(";");
    hash_text(&serialized)
}

fn project_ingest_content_hash(document_hash: &str, routing_fingerprint: &str) -> String {
    hash_text(&format!("{document_hash}:{routing_fingerprint}"))
}

fn canonicalize_label(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|ch| match ch {
            'a'..='z' | '0'..='9' | '-' | '_' | '.' | '/' => ch,
            _ => '_',
        })
        .collect::<String>()
}

fn canonicalize_optional(value: Option<&str>) -> String {
    value.map(canonicalize_label).unwrap_or_default()
}

fn wing_id(value: &str) -> Result<WingId> {
    WingId::normalized(value).map_err(|err| IngestError::Core(err.into()))
}

fn room_id(value: &str) -> Result<RoomId> {
    RoomId::new(canonicalize_label(value)).map_err(|err| IngestError::Core(err.into()))
}

fn drawer_id(wing: &WingId, room: &RoomId, source_key: &str, chunk_index: u32) -> Result<DrawerId> {
    mempalace_core::mined_drawer_id(wing, room, source_key, chunk_index)
        .map_err(|err| IngestError::Core(err.into()))
}

fn labels_overlap(room_name: &str, candidate: &str) -> bool {
    let candidate = canonicalize_label(candidate);
    if candidate.len() < 2 {
        return false;
    }
    room_name == candidate || room_name.split(['-', '_', '.', '/']).any(|part| part == candidate)
}

fn count_term_matches(haystack: &str, needle: &str) -> usize {
    if needle.trim().is_empty() {
        return 0;
    }

    let mut matches = 0usize;
    let mut search_start = 0usize;
    while let Some(found) = haystack[search_start..].find(needle) {
        let start = search_start + found;
        let end = start + needle.len();
        let left_ok =
            start == 0 || !haystack[..start].chars().next_back().is_some_and(is_word_char);
        let right_ok =
            end == haystack.len() || !haystack[end..].chars().next().is_some_and(is_word_char);
        if left_ok && right_ok {
            matches += 1;
        }
        search_start = end;
    }
    matches
}

fn is_word_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-')
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::cell::RefCell;
    use std::fs;
    use std::rc::Rc;

    use mempalace_core::EmbeddingProfile;
    use mempalace_embeddings::{
        EmbeddingProvider, EmbeddingRequest, EmbeddingResponse, StartupValidation,
        StartupValidationStatus,
    };
    use mempalace_storage::{
        DrawerFilter, DrawerStore, DuplicateStrategy, IngestCommitRequest,
    };
    use serde_json::json;
    use tempfile::{tempdir, Builder};

    use super::*;

    #[derive(Debug)]
    struct FakeEmbeddingProvider {
        dimensions: usize,
    }

    #[derive(Debug)]
    struct RecordingEmbeddingProvider {
        dimensions: usize,
        batches: Rc<RefCell<Vec<usize>>>,
    }

    impl FakeEmbeddingProvider {
        fn new(dimensions: usize) -> Self {
            Self { dimensions }
        }
    }

    impl EmbeddingProvider for FakeEmbeddingProvider {
        fn profile(&self) -> &'static mempalace_core::EmbeddingProfileMetadata {
            EmbeddingProfile::Balanced.metadata()
        }

        fn startup_validation(&self) -> mempalace_embeddings::Result<StartupValidation> {
            Ok(StartupValidation {
                status: StartupValidationStatus::Ready,
                cache_root: PathBuf::from("/tmp/fake"),
                model_id: self.profile().model_id,
                detail: "ready".to_owned(),
            })
        }

        fn embed(
            &mut self,
            request: &EmbeddingRequest,
        ) -> mempalace_embeddings::Result<EmbeddingResponse> {
            let vectors = request
                .texts()
                .iter()
                .map(|text| {
                    let mut vector = vec![0.0; self.dimensions];
                    if let Some(first) = vector.first_mut() {
                        *first = text.len() as f32;
                    }
                    vector
                })
                .collect::<Vec<_>>();
            EmbeddingResponse::from_vectors(
                vectors,
                self.dimensions,
                EmbeddingProfile::Balanced,
                self.profile().model_id,
            )
        }
    }

    impl EmbeddingProvider for RecordingEmbeddingProvider {
        fn profile(&self) -> &'static mempalace_core::EmbeddingProfileMetadata {
            EmbeddingProfile::Balanced.metadata()
        }

        fn startup_validation(&self) -> mempalace_embeddings::Result<StartupValidation> {
            Ok(StartupValidation {
                status: StartupValidationStatus::Ready,
                cache_root: PathBuf::from("/tmp/recording"),
                model_id: self.profile().model_id,
                detail: "ready".to_owned(),
            })
        }

        fn embed(
            &mut self,
            request: &EmbeddingRequest,
        ) -> mempalace_embeddings::Result<EmbeddingResponse> {
            self.batches.borrow_mut().push(request.len());
            let vectors = request
                .texts()
                .iter()
                .map(|text| {
                    let mut vector = vec![0.0; self.dimensions];
                    if let Some(first) = vector.first_mut() {
                        *first = text.len() as f32;
                    }
                    vector
                })
                .collect::<Vec<_>>();
            EmbeddingResponse::from_vectors(
                vectors,
                self.dimensions,
                EmbeddingProfile::Balanced,
                self.profile().model_id,
            )
        }
    }

    async fn open_engine(path: &Path) -> StorageEngine {
        StorageEngine::open(path, EmbeddingProfile::Balanced).await.unwrap()
    }

    #[test]
    fn build_drawers_batches_embedding_requests_when_capped() {
        let batches = Rc::new(RefCell::new(Vec::new()));
        let mut provider = RecordingEmbeddingProvider { dimensions: 4, batches: batches.clone() };

        let drawers = build_drawers(
            &mut provider,
            &WingId::new("wing_code").unwrap(),
            "projects::wing_code::src/lib.rs",
            "src/lib.rs",
            "projects",
            None,
            "tests",
            Some(2),
            vec![
                Chunk {
                    content: "alpha".to_owned(),
                    chunk_index: 0,
                    room_hint: Some("general".to_owned()),
                    date_hint: None,
                    byte_range: None,
                },
                Chunk {
                    content: "beta".to_owned(),
                    chunk_index: 1,
                    room_hint: Some("general".to_owned()),
                    date_hint: None,
                    byte_range: None,
                },
                Chunk {
                    content: "gamma".to_owned(),
                    chunk_index: 2,
                    room_hint: Some("general".to_owned()),
                    date_hint: None,
                    byte_range: None,
                },
                Chunk {
                    content: "delta".to_owned(),
                    chunk_index: 3,
                    room_hint: Some("general".to_owned()),
                    date_hint: None,
                    byte_range: None,
                },
                Chunk {
                    content: "epsilon".to_owned(),
                    chunk_index: 4,
                    room_hint: Some("general".to_owned()),
                    date_hint: None,
                    byte_range: None,
                },
],
            None,           // locator_ctx
            None,           // view
            None,           // view_metadata
        )
        .unwrap();

        assert_eq!(drawers.len(), 5);
        assert_eq!(*batches.borrow(), vec![2, 2, 1]);
    }

    #[test]
    fn normalizes_claude_json_and_spellchecks_user_turns() {
        let payload = json!([
            {"role": "user", "content": "lsresdy knoe the question befor"},
            {"role": "assistant", "content": "I already do."}
        ]);
        let normalized =
            normalize_conversation(Path::new("chat.json"), payload.to_string().as_bytes()).unwrap();
        assert!(normalized.transcript.contains("> already know the question before"));
        assert!(normalized.transcript.contains("I already do."));
    }

    #[test]
    fn spellcheck_preserves_user_whitespace() {
        let payload = json!([
            {"role": "user", "content": "lsresdy\tknoe\nbefor"},
            {"role": "assistant", "content": "I already do."}
        ]);
        let normalized =
            normalize_conversation(Path::new("chat.json"), payload.to_string().as_bytes()).unwrap();
        assert!(normalized.transcript.contains("> already\tknow\nbefore"));
        assert!(normalized.transcript.contains("I already do."));
    }

    #[test]
    fn parses_chatgpt_mapping_json() {
        let payload = json!({
            "mapping": {
                "root": {"id":"root","parent": null, "children": ["user"], "message": null},
                "user": {
                    "id":"user",
                    "parent":"root",
                    "children":["assistant"],
                    "message": {
                        "author": {"role":"user"},
                        "content": {"parts": ["Why does this matter?"]},
                        "create_time": 1710000000
                    }
                },
                "assistant": {
                    "id":"assistant",
                    "parent":"user",
                    "children": [],
                    "message": {
                        "author": {"role":"assistant"},
                        "content": {"parts": ["It preserves context."]},
                        "create_time": 1710000001
                    }
                }
            }
        });
        let normalized =
            normalize_conversation(Path::new("chatgpt.json"), payload.to_string().as_bytes())
                .unwrap();
        assert!(normalized.transcript.contains("> Why does this matter?"));
        assert!(normalized.transcript.contains("It preserves context."));
    }

    #[test]
    fn rejects_malformed_json_exports() {
        let result = normalize_conversation(Path::new("broken.json"), br#"{"oops": "#);
        assert_eq!(result, Err(ConversationNormalizeError::Malformed));
    }

    #[test]
    fn extracts_general_memories_with_resolution_disambiguation() {
        let memories = extract_memories(
            "We finally fixed the auth bug after finding the root cause in the token refresh path.",
        );
        assert_eq!(memories.len(), 1);
        assert_eq!(memories[0].room_hint.as_deref(), Some("milestone"));
    }

    #[test]
    fn honors_gitignore_rules_during_discovery() {
        let tempdir = tempdir().unwrap();
        fs::write(tempdir.path().join(".gitignore"), "ignored/\n*.log\n").unwrap();
        fs::create_dir_all(tempdir.path().join("ignored")).unwrap();
        fs::create_dir_all(tempdir.path().join("keep")).unwrap();
        fs::write(tempdir.path().join("ignored").join("secret.md"), "hidden").unwrap();
        fs::write(tempdir.path().join("keep").join("visible.md"), "visible").unwrap();
        fs::write(tempdir.path().join("trace.log"), "noise").unwrap();

        let discovered = discover_project_files(tempdir.path()).unwrap();
        assert_eq!(discovered.files.len(), 1);
        assert_eq!(discovered.files[0].relative_path, "keep/visible.md");
        assert!(discovered.ignored_files >= 2);
    }

    #[test]
    fn does_not_treat_file_named_like_directory_as_ignored_directory() {
        let tempdir = tempdir().unwrap();
        let matcher = IgnoreMatcher::new(tempdir.path());
        assert!(!matcher.matches("node_modules", false));
        assert!(matcher.matches("node_modules", true));
    }

    #[tokio::test]
    async fn ingests_project_fixture_and_routes_rooms() {
        let tempdir = tempdir().unwrap();
        let fixture_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/phase0/inputs/project_alpha");
        let engine = open_engine(tempdir.path()).await;
        let mut provider =
            FakeEmbeddingProvider::new(EmbeddingProfile::Balanced.metadata().dimensions);

        let summary = ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: fixture_root.clone(),
                wing: None,
                agent: "tester".to_owned(),
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
                branch: false,
        view: None,},
        )
        .await
        .unwrap();

        assert_eq!(summary.ingested_files, 2);
        let backend = engine
            .drawer_store()
            .list_drawers(&DrawerFilter {
                room: Some(RoomId::new("backend").unwrap()),
                ..DrawerFilter::default()
            })
            .await
            .unwrap();
        let planning = engine
            .drawer_store()
            .list_drawers(&DrawerFilter {
                room: Some(RoomId::new("planning").unwrap()),
                ..DrawerFilter::default()
            })
            .await
            .unwrap();
        assert!(!backend.is_empty());
        assert!(!planning.is_empty());
    }

    #[tokio::test]
    async fn ingests_conversation_fixture_in_both_modes_in_same_wing() {
        let tempdir = tempdir().unwrap();
        let fixture_root =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/phase0/inputs/convos");
        let engine = open_engine(tempdir.path()).await;
        let mut provider =
            FakeEmbeddingProvider::new(EmbeddingProfile::Balanced.metadata().dimensions);

        let summary = ingest_conversations(
            &engine,
            &mut provider,
            &ConversationIngestRequest {
                convo_dir: fixture_root.clone(),
                wing: Some("phase0_convos".to_owned()),
                agent: "tester".to_owned(),
                extract_mode: ConversationExtractMode::Exchange,
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(summary.ingested_files, 1);

        let summary_general = ingest_conversations(
            &engine,
            &mut provider,
            &ConversationIngestRequest {
                convo_dir: fixture_root,
                wing: Some("phase0_convos".to_owned()),
                agent: "tester".to_owned(),
                extract_mode: ConversationExtractMode::General,
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(summary_general.ingested_files, 1);
        let decisions = engine
            .drawer_store()
            .list_drawers(&DrawerFilter {
                wing: Some(WingId::new("wing_phase0_convos").unwrap()),
                room: Some(RoomId::new("decision").unwrap()),
                ..DrawerFilter::default()
            })
            .await
            .unwrap();
        let exchange = engine
            .drawer_store()
            .list_drawers(&DrawerFilter {
                wing: Some(WingId::new("wing_phase0_convos").unwrap()),
                source_file: Some("product_strategy.txt".to_owned()),
                ..DrawerFilter::default()
            })
            .await
            .unwrap();
        assert!(!decisions.is_empty());
        assert!(!exchange.is_empty());
    }

    #[tokio::test]
    async fn rerun_does_not_delete_same_relative_conversation_in_other_wing() {
        let tempdir = tempdir().unwrap();
        let wing_a = tempdir.path().join("wing-a");
        let wing_b = tempdir.path().join("wing-b");
        fs::create_dir_all(&wing_a).unwrap();
        fs::create_dir_all(&wing_b).unwrap();
        let transcript =
            "> Why?\nBecause context matters.\n\n> What changed?\nWe fixed ingest state.\n";
        fs::write(wing_a.join("chat.txt"), transcript).unwrap();
        fs::write(wing_b.join("chat.txt"), transcript).unwrap();

        let engine = open_engine(tempdir.path()).await;
        let mut provider =
            FakeEmbeddingProvider::new(EmbeddingProfile::Balanced.metadata().dimensions);

        ingest_conversations(
            &engine,
            &mut provider,
            &ConversationIngestRequest {
                convo_dir: wing_a.clone(),
                wing: Some("wing_a".to_owned()),
                agent: "tester".to_owned(),
                extract_mode: ConversationExtractMode::Exchange,
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
            },
        )
        .await
        .unwrap();
        ingest_conversations(
            &engine,
            &mut provider,
            &ConversationIngestRequest {
                convo_dir: wing_b.clone(),
                wing: Some("wing_b".to_owned()),
                agent: "tester".to_owned(),
                extract_mode: ConversationExtractMode::Exchange,
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
            },
        )
        .await
        .unwrap();

        fs::write(
            wing_a.join("chat.txt"),
            "> Why?\nBecause scoped keys matter.\n\n> What changed?\nWe fixed cross-wing cleanup.\n",
        )
        .unwrap();
        ingest_conversations(
            &engine,
            &mut provider,
            &ConversationIngestRequest {
                convo_dir: wing_a,
                wing: Some("wing_a".to_owned()),
                agent: "tester".to_owned(),
                extract_mode: ConversationExtractMode::Exchange,
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
            },
        )
        .await
        .unwrap();

        let wing_b_drawers = engine
            .drawer_store()
            .list_drawers(&DrawerFilter {
                wing: Some(WingId::new("wing_b").unwrap()),
                source_file: Some("chat.txt".to_owned()),
                ..DrawerFilter::default()
            })
            .await
            .unwrap();
        assert!(!wing_b_drawers.is_empty());
    }

    #[tokio::test]
    async fn reruns_are_idempotent_and_reindex_changed_files() {
        let tempdir = tempdir().unwrap();
        let project_dir = tempdir.path().join("project");
        fs::create_dir_all(project_dir.join("backend")).unwrap();
        fs::write(
            project_dir.join("mempalace.yaml"),
            "wing: sample\nrooms:\n  - name: backend\n    keywords: [auth]\n  - name: general\n",
        )
        .unwrap();
        fs::write(
            project_dir.join("backend/auth.py"),
            "def login():\n    return 'auth token'\n".repeat(40),
        )
        .unwrap();

        let engine = open_engine(&tempdir.path().join("palace")).await;
        let mut provider =
            FakeEmbeddingProvider::new(EmbeddingProfile::Balanced.metadata().dimensions);

        let first = ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: project_dir.clone(),
                wing: None,
                agent: "tester".to_owned(),
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
                branch: false,
        view: None,},
        )
        .await
        .unwrap();
        let second = ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: project_dir.clone(),
                wing: None,
                agent: "tester".to_owned(),
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
                branch: false,
        view: None,},
        )
        .await
        .unwrap();
        assert_eq!(first.drawers_written, 2);
        assert_eq!(second.skipped_unchanged, 1);

        fs::write(
            project_dir.join("backend/auth.py"),
            "def login():\n    return 'changed auth'\n".repeat(20),
        )
        .unwrap();
        let third = ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: project_dir.clone(),
                wing: None,
                agent: "tester".to_owned(),
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
                branch: false,
        view: None,},
        )
        .await
        .unwrap();
        assert_eq!(third.ingested_files, 1);
        let drawers = engine
            .drawer_store()
            .list_drawers(&DrawerFilter {
                source_file: Some("backend/auth.py".to_owned()),
                ..DrawerFilter::default()
            })
            .await
            .unwrap();
        assert_eq!(drawers.len(), 1);
        // Locator-backed rows store empty content; text is resolved lazily.
        let d = &drawers[0];
        assert!(d.locator.is_some(), "expected locator on mined project drawer");
        assert!(d.content.is_empty(), "expected empty stored content for locator-backed row");
        let snippet = mempalace_core::resolve_locator(d.locator.as_ref().unwrap(), &d.source_file);
        assert!(!snippet.stale);
        assert!(snippet.text.contains("changed auth"), "resolved text: {:?}", snippet.text);
    }

    #[tokio::test]
    async fn project_wing_override_routes_drawers_into_requested_wing() {
        let tempdir = tempdir().unwrap();
        let project_dir = tempdir.path().join("project");
        fs::create_dir_all(project_dir.join("backend")).unwrap();
        fs::write(
            project_dir.join("mempalace.yaml"),
            "wing: sample\nrooms:\n  - name: backend\n    keywords: [auth]\n  - name: general\n",
        )
        .unwrap();
        fs::write(
            project_dir.join("backend/auth.py"),
            "def login():\n    return 'auth token'\n".repeat(40),
        )
        .unwrap();

        let engine = open_engine(&tempdir.path().join("palace")).await;
        let mut provider =
            FakeEmbeddingProvider::new(EmbeddingProfile::Balanced.metadata().dimensions);

        ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: project_dir.clone(),
                wing: Some("overridewing".to_owned()),
                agent: "tester".to_owned(),
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
                branch: false,
        view: None,},
        )
        .await
        .unwrap();

        let drawers = engine
            .drawer_store()
            .list_drawers(&DrawerFilter {
                wing: Some(WingId::new("wing_overridewing").unwrap()),
                ..DrawerFilter::default()
            })
            .await
            .unwrap();
        assert!(!drawers.is_empty());
    }

    #[tokio::test]
    async fn removes_orphaned_drawers_when_project_file_becomes_too_small() {
        let tempdir = tempdir().unwrap();
        let project_dir = tempdir.path().join("project");
        fs::create_dir_all(project_dir.join("backend")).unwrap();
        fs::write(
            project_dir.join("mempalace.yaml"),
            "wing: sample\nrooms:\n  - name: backend\n    keywords: [auth]\n",
        )
        .unwrap();
        fs::write(
            project_dir.join("backend/auth.py"),
            "def login():\n    return 'auth token'\n".repeat(40),
        )
        .unwrap();

        let engine = open_engine(&tempdir.path().join("palace")).await;
        let mut provider =
            FakeEmbeddingProvider::new(EmbeddingProfile::Balanced.metadata().dimensions);

        let first = ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: project_dir.clone(),
                wing: None,
                agent: "tester".to_owned(),
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
                branch: false,
        view: None,},
        )
        .await
        .unwrap();
        assert_eq!(first.drawers_written, 2);

        fs::write(project_dir.join("backend/auth.py"), "tiny").unwrap();
        let second = ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: project_dir.clone(),
                wing: None,
                agent: "tester".to_owned(),
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
                branch: false,
        view: None,},
        )
        .await
        .unwrap();

        assert_eq!(second.ingested_files, 1);
        let drawers = engine
            .drawer_store()
            .list_drawers(&DrawerFilter {
                source_file: Some("backend/auth.py".to_owned()),
                ..DrawerFilter::default()
            })
            .await
            .unwrap();
        assert!(drawers.is_empty());
    }

    #[tokio::test]
    async fn project_config_changes_trigger_reroute_without_file_edits() {
        let tempdir = tempdir().unwrap();
        let project_dir = tempdir.path().join("project");
        fs::create_dir_all(project_dir.join("notes")).unwrap();
        fs::write(
            project_dir.join("mempalace.yaml"),
            "wing: sample\nrooms:\n  - name: backend\n    keywords: [token]\n  - name: general\n",
        )
        .unwrap();
        fs::write(
            project_dir.join("notes/plan.md"),
            "Token handling and API auth strategy.\n".repeat(20),
        )
        .unwrap();

        let engine = open_engine(&tempdir.path().join("palace")).await;
        let mut provider =
            FakeEmbeddingProvider::new(EmbeddingProfile::Balanced.metadata().dimensions);

        ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: project_dir.clone(),
                wing: None,
                agent: "tester".to_owned(),
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
                branch: false,
        view: None,},
        )
        .await
        .unwrap();

        fs::write(
            project_dir.join("mempalace.yaml"),
            "wing: sample\nrooms:\n  - name: planning\n    keywords: [token, strategy]\n  - name: general\n",
        )
        .unwrap();
        let rerun = ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: project_dir.clone(),
                wing: None,
                agent: "tester".to_owned(),
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
                branch: false,
        view: None,},
        )
        .await
        .unwrap();

        assert_eq!(rerun.ingested_files, 1);
        let planning = engine
            .drawer_store()
            .list_drawers(&DrawerFilter {
                room: Some(RoomId::new("planning").unwrap()),
                source_file: Some("notes/plan.md".to_owned()),
                ..DrawerFilter::default()
            })
            .await
            .unwrap();
        assert!(!planning.is_empty());
    }

    #[tokio::test]
    async fn malformed_conversation_exports_do_not_poison_runs() {
        let tempdir = tempdir().unwrap();
        fs::write(tempdir.path().join("broken.json"), r#"{"messages":"not-valid""#).unwrap();
        fs::write(
            tempdir.path().join("chat.txt"),
            "> What changed?\nThe storage contract now tracks file hashes.\n\n> Why?\nTo support deterministic reruns.\n",
        )
        .unwrap();
        let engine = open_engine(&tempdir.path().join("palace")).await;
        let mut provider =
            FakeEmbeddingProvider::new(EmbeddingProfile::Balanced.metadata().dimensions);

        let summary = ingest_conversations(
            &engine,
            &mut provider,
            &ConversationIngestRequest {
                convo_dir: tempdir.path().to_path_buf(),
                wing: Some("mixed".to_owned()),
                agent: "tester".to_owned(),
                extract_mode: ConversationExtractMode::Exchange,
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(summary.malformed_files, 1);
        assert_eq!(summary.ingested_files, 1);
    }

    // ── Stage-2 locator tests ──────────────────────────────────────────────

    /// Mine a temp project with a multi-chunk UTF-8 file.  For every drawer
    /// assert: locator present, content empty, hash matches resolved text.
    #[tokio::test]
    async fn locator_round_trip_for_utf8_file() {
        let tempdir = tempdir().unwrap();
        let project_dir = tempdir.path().join("project");
        fs::create_dir_all(project_dir.join("src")).unwrap();
        fs::write(
            project_dir.join("mempalace.yaml"),
            "wing: loctest\nrooms:\n  - name: general\n",
        )
        .unwrap();

        // Build a file large enough to produce at least two chunks (> 1600 chars).
        let line = "The quick brown fox jumps over the lazy dog. ";
        let body: String = line.repeat(40); // ~1800 chars, two chunks
        let file_path = project_dir.join("src/code.txt");
        fs::write(&file_path, &body).unwrap();
        let file_bytes = fs::read(&file_path).unwrap();

        let engine = open_engine(&tempdir.path().join("palace")).await;
        let mut provider =
            FakeEmbeddingProvider::new(EmbeddingProfile::Balanced.metadata().dimensions);

        let summary = ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: project_dir.clone(),
                wing: None,
                agent: "tester".to_owned(),
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
                branch: false,
        view: None,},
        )
        .await
        .unwrap();

        assert!(summary.drawers_written >= 2, "expected >=2 chunks, got {}", summary.drawers_written);

        let drawers = engine
            .drawer_store()
            .list_drawers(&DrawerFilter {
                source_file: Some("src/code.txt".to_owned()),
                ..DrawerFilter::default()
            })
            .await
            .unwrap();

        assert_eq!(drawers.len() as usize, summary.drawers_written);
        for drawer in &drawers {
            let loc = drawer.locator.as_ref().expect("locator must be set for UTF-8 project file");
            // Stored content must be empty.
            assert!(drawer.content.is_empty(), "stored content must be empty for locator row");
            // file_hash must equal hash of full file bytes.
            assert_eq!(loc.file_hash, hash_bytes(&file_bytes));
            // Line numbers must be sane.
            assert!(loc.line_start >= 1);
            assert!(loc.line_end >= loc.line_start);
            // Byte range sliced from file must equal the embedded chunk text.
            let start = loc.byte_start as usize;
            let end = loc.byte_end as usize;
            let slice = std::str::from_utf8(&file_bytes[start..end])
                .expect("locator slice must be valid UTF-8");
            // content_hash is hash of the real chunk text.
            assert_eq!(drawer.content_hash, hash_text(slice));
            // resolve_locator must return non-stale text.
            let snippet = mempalace_core::resolve_locator(loc, &drawer.source_file);
            assert!(!snippet.stale, "locator must not be stale for unmodified file");
            assert_eq!(snippet.text, slice);
            assert_eq!(hash_text(&snippet.text), drawer.content_hash);
        }
    }

    /// A file starting with leading whitespace/newlines: locator offsets must
    /// still slice correctly (regression for trim_offset arithmetic).
    #[tokio::test]
    async fn locator_leading_whitespace_file() {
        let tempdir = tempdir().unwrap();
        let project_dir = tempdir.path().join("project");
        fs::create_dir_all(&project_dir).unwrap();
        fs::write(
            project_dir.join("mempalace.yaml"),
            "wing: lwtest\nrooms:\n  - name: general\n",
        )
        .unwrap();

        let line = "Content line with enough text to fill the chunk buffer here. ";
        let body = format!("\n\n\n{}", line.repeat(20)); // leading newlines
        let file_path = project_dir.join("notes.txt");
        fs::write(&file_path, &body).unwrap();
        let file_bytes = fs::read(&file_path).unwrap();

        let engine = open_engine(&tempdir.path().join("palace")).await;
        let mut provider =
            FakeEmbeddingProvider::new(EmbeddingProfile::Balanced.metadata().dimensions);

        ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: project_dir.clone(),
                wing: None,
                agent: "tester".to_owned(),
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
                branch: false,
        view: None,},
        )
        .await
        .unwrap();

        let drawers = engine
            .drawer_store()
            .list_drawers(&DrawerFilter {
                source_file: Some("notes.txt".to_owned()),
                ..DrawerFilter::default()
            })
            .await
            .unwrap();

        assert!(!drawers.is_empty());
        for drawer in &drawers {
            let loc = drawer.locator.as_ref().expect("locator must be present");
            let start = loc.byte_start as usize;
            let end = loc.byte_end as usize;
            assert!(start >= 3, "trim_offset must skip the 3 leading newlines; start={start}");
            let slice = std::str::from_utf8(&file_bytes[start..end]).unwrap();
            assert_eq!(hash_text(slice), drawer.content_hash);
        }
    }

    /// A non-UTF-8 file should store content verbatim (lossy) with locator None.
    #[tokio::test]
    async fn non_utf8_file_stores_content_no_locator() {
        let tempdir = tempdir().unwrap();
        let project_dir = tempdir.path().join("project");
        fs::create_dir_all(&project_dir).unwrap();
        fs::write(
            project_dir.join("mempalace.yaml"),
            "wing: bintest\nrooms:\n  - name: general\n",
        )
        .unwrap();

        // Latin-1 bytes that are not valid UTF-8, long enough to meet min chunk size.
        let mut raw: Vec<u8> = b"Hello world invalid ".to_vec();
        raw.extend(b"\xFF\xFE text content that is long enough to exceed the minimum ".to_vec());
        raw.extend(b"chunk size threshold so it actually gets stored in the palace.".to_vec());
        let file_path = project_dir.join("latin1.txt");
        fs::write(&file_path, &raw).unwrap();

        let engine = open_engine(&tempdir.path().join("palace")).await;
        let mut provider =
            FakeEmbeddingProvider::new(EmbeddingProfile::Balanced.metadata().dimensions);

        ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: project_dir.clone(),
                wing: None,
                agent: "tester".to_owned(),
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
                branch: false,
        view: None,},
        )
        .await
        .unwrap();

        let drawers = engine
            .drawer_store()
            .list_drawers(&DrawerFilter {
                source_file: Some("latin1.txt".to_owned()),
                ..DrawerFilter::default()
            })
            .await
            .unwrap();

        assert!(!drawers.is_empty(), "non-UTF-8 file should still be chunked and stored");
        for drawer in &drawers {
            assert!(drawer.locator.is_none(), "non-UTF-8 file must have no locator");
            assert!(!drawer.content.is_empty(), "non-UTF-8 file must store content verbatim");
        }
    }

    /// A file larger than 200 000 bytes: chunks only cover the first 200 000 bytes;
    /// file_hash is hash of the FULL bytes.
    #[tokio::test]
    async fn truncated_file_locators_cover_only_first_200k() {
        let tempdir = tempdir().unwrap();
        let project_dir = tempdir.path().join("project");
        fs::create_dir_all(&project_dir).unwrap();
        fs::write(
            project_dir.join("mempalace.yaml"),
            "wing: trunctest\nrooms:\n  - name: general\n",
        )
        .unwrap();

        // Build a file larger than LARGE_FILE_TRUNCATION_BYTES (200_000 bytes).
        let line = "Truncation boundary test line of content. ";
        let body: String = line.repeat(6_000); // ~252 000 bytes
        assert!(body.len() > LARGE_FILE_TRUNCATION_BYTES);
        let file_path = project_dir.join("large.txt");
        fs::write(&file_path, &body).unwrap();
        let file_bytes = fs::read(&file_path).unwrap();

        let engine = open_engine(&tempdir.path().join("palace")).await;
        let mut provider =
            FakeEmbeddingProvider::new(EmbeddingProfile::Balanced.metadata().dimensions);

        ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: project_dir.clone(),
                wing: None,
                agent: "tester".to_owned(),
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
                branch: false,
        view: None,},
        )
        .await
        .unwrap();

        let drawers = engine
            .drawer_store()
            .list_drawers(&DrawerFilter {
                source_file: Some("large.txt".to_owned()),
                ..DrawerFilter::default()
            })
            .await
            .unwrap();

        assert!(!drawers.is_empty());
        let expected_file_hash = hash_bytes(&file_bytes);
        for drawer in &drawers {
            let loc = drawer.locator.as_ref().expect("truncated UTF-8 file must have locator");
            // file_hash must be hash of FULL bytes.
            assert_eq!(loc.file_hash, expected_file_hash);
            // Byte ranges must lie within the first 200 000 bytes.
            assert!(loc.byte_end <= LARGE_FILE_TRUNCATION_BYTES as u64,
                "byte_end {} exceeds truncation boundary", loc.byte_end);
            // Slice must be valid.
            let start = loc.byte_start as usize;
            let end = loc.byte_end as usize;
            let slice = std::str::from_utf8(&file_bytes[start..end]).unwrap();
            assert_eq!(hash_text(slice), drawer.content_hash);
        }
    }

    /// A non-git directory should yield commit_hash = None on the locator.
    #[tokio::test]
    async fn commit_hash_none_when_not_a_git_repo() {
        let tempdir = tempdir().unwrap();
        let project_dir = tempdir.path().join("project");
        fs::create_dir_all(&project_dir).unwrap();
        fs::write(
            project_dir.join("mempalace.yaml"),
            "wing: gitless\nrooms:\n  - name: general\n",
        )
        .unwrap();
        let line = "Some content for the locator test. ";
        fs::write(project_dir.join("notes.txt"), line.repeat(30)).unwrap();

        let engine = open_engine(&tempdir.path().join("palace")).await;
        let mut provider =
            FakeEmbeddingProvider::new(EmbeddingProfile::Balanced.metadata().dimensions);

        ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: project_dir.clone(),
                wing: None,
                agent: "tester".to_owned(),
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
                branch: false,
        view: None,},
        )
        .await
        .unwrap();

        let drawers = engine
            .drawer_store()
            .list_drawers(&DrawerFilter {
                source_file: Some("notes.txt".to_owned()),
                ..DrawerFilter::default()
            })
            .await
            .unwrap();

        assert!(!drawers.is_empty());
        for drawer in &drawers {
            let loc = drawer.locator.as_ref().expect("locator must be present");
            assert!(loc.commit_hash.is_none(), "commit_hash must be None outside a git repo");
        }
    }

    /// Mine twice unchanged → second run skipped_unchanged == discovered.
    /// Mine with reindex: true → files re-ingested (ingested_files > 0, skipped_unchanged == 0).
    #[tokio::test]
    async fn reindex_forces_reingest_even_when_unchanged() {
        let tempdir = tempdir().unwrap();
        let project_dir = tempdir.path().join("project");
        fs::create_dir_all(&project_dir).unwrap();
        fs::write(
            project_dir.join("mempalace.yaml"),
            "wing: ridx\nrooms:\n  - name: general\n",
        )
        .unwrap();
        let body = "Stable content for reindex test.\n".repeat(30);
        fs::write(project_dir.join("stable.txt"), &body).unwrap();

        let engine = open_engine(&tempdir.path().join("palace")).await;
        let mut provider =
            FakeEmbeddingProvider::new(EmbeddingProfile::Balanced.metadata().dimensions);

        // First run: ingest.
        let first = ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: project_dir.clone(),
                wing: None,
                agent: "tester".to_owned(),
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
                branch: false,
        view: None,},
        )
        .await
        .unwrap();
        assert!(first.ingested_files >= 1);

        // Second run without changes: should skip.
        let second = ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: project_dir.clone(),
                wing: None,
                agent: "tester".to_owned(),
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
                branch: false,
        view: None,},
        )
        .await
        .unwrap();
        assert_eq!(second.skipped_unchanged, second.discovered_files,
            "unchanged run: all files should be skipped");
        assert_eq!(second.ingested_files, 0);

        // Third run with reindex=true: must re-ingest all.
        let third = ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: project_dir.clone(),
                wing: None,
                agent: "tester".to_owned(),
                limit: None,
                dry_run: false,
                reindex: true,
                max_embed_batch_size: None,
                branch: false,
        view: None,},
        )
        .await
        .unwrap();
        assert_eq!(third.skipped_unchanged, 0, "reindex=true: nothing should be skipped");
        assert!(third.ingested_files >= 1, "reindex=true: files must be re-ingested");
    }

    // ── Stage-4 discovery broadening tests ────────────────────────────────────

    /// New-extension files (e.g. .cs, .vue, .tf) are discovered.
    #[test]
    fn discovers_new_extension_files() {
        let tempdir = tempdir().unwrap();
        fs::write(tempdir.path().join("main.cs"), "class Program {}").unwrap();
        fs::write(tempdir.path().join("app.vue"), "<template/>").unwrap();
        fs::write(tempdir.path().join("deploy.tf"), "resource \"aws_s3_bucket\" \"b\" {}").unwrap();
        // Also a file with no known extension — should be ignored.
        fs::write(tempdir.path().join("random.xyz"), "irrelevant").unwrap();

        let report = discover_project_files(tempdir.path()).unwrap();
        let names: Vec<_> = report.files.iter().map(|f| f.relative_path.as_str()).collect();
        assert!(names.contains(&"app.vue"), "app.vue not found: {names:?}");
        assert!(names.contains(&"deploy.tf"), "deploy.tf not found: {names:?}");
        assert!(names.contains(&"main.cs"), "main.cs not found: {names:?}");
        assert!(!names.contains(&"random.xyz"), "random.xyz should be ignored: {names:?}");
        assert!(report.ignored_files >= 1);
    }

    /// Dockerfile and Makefile (no extension) are discovered via the basename allowlist.
    #[test]
    fn discovers_extensionless_basenames() {
        let tempdir = tempdir().unwrap();
        fs::write(tempdir.path().join("Dockerfile"), "FROM ubuntu:22.04").unwrap();
        fs::write(tempdir.path().join("Makefile"), "all:\n\techo done").unwrap();
        // A random extensionless file should NOT be discovered.
        fs::write(tempdir.path().join("notes"), "just notes").unwrap();

        let report = discover_project_files(tempdir.path()).unwrap();
        let names: Vec<_> = report.files.iter().map(|f| f.relative_path.as_str()).collect();
        assert!(names.contains(&"Dockerfile"), "Dockerfile not found: {names:?}");
        assert!(names.contains(&"Makefile"), "Makefile not found: {names:?}");
        assert!(!names.contains(&"notes"), "notes should be ignored: {names:?}");
    }

    /// An extensionless file with a shebang line is discovered; one without is not.
    #[test]
    fn shebang_detection_for_extensionless_files() {
        let tempdir = tempdir().unwrap();
        fs::write(tempdir.path().join("build"), "#!/usr/bin/env bash\necho hi").unwrap();
        fs::write(tempdir.path().join("noshebang"), "just plain text with no shebang").unwrap();

        let report = discover_project_files(tempdir.path()).unwrap();
        let names: Vec<_> = report.files.iter().map(|f| f.relative_path.as_str()).collect();
        assert!(names.contains(&"build"), "shebang file 'build' not found: {names:?}");
        assert!(!names.contains(&"noshebang"), "noshebang should be ignored: {names:?}");
    }

    /// A file with an allowlisted extension that contains a NUL byte is treated as binary
    /// and excluded; the ignored_files counter is incremented.
    #[test]
    fn binary_sniff_excludes_nul_byte_files() {
        let tempdir = tempdir().unwrap();
        // Build a .h file that contains a NUL byte within the first 8 KiB.
        let mut binary_h = b"// header\n".to_vec();
        binary_h.extend(vec![0u8; 100]); // NUL bytes
        binary_h.extend(b"// rest of header\n");
        // Pad to ensure it's > 100 bytes so it's unambiguously "binary" not empty.
        binary_h.extend(vec![b'x'; 7900]);
        fs::write(tempdir.path().join("data.h"), &binary_h).unwrap();
        // Also write a clean .h for comparison.
        fs::write(tempdir.path().join("clean.h"), "// clean header\nvoid foo();\n").unwrap();

        let report = discover_project_files(tempdir.path()).unwrap();
        let names: Vec<_> = report.files.iter().map(|f| f.relative_path.as_str()).collect();
        assert!(!names.contains(&"data.h"), "binary data.h must be excluded: {names:?}");
        assert!(names.contains(&"clean.h"), "clean.h must be included: {names:?}");
        // ignored_files must include the binary file.
        assert!(report.ignored_files >= 1, "ignored_files should be >= 1, got {}", report.ignored_files);
    }

    /// .env and .env.local are never discovered — even if they start with #!.
    #[test]
    fn env_files_always_skipped() {
        let tempdir = tempdir().unwrap();
        fs::write(tempdir.path().join(".env"), "SECRET=abc\n").unwrap();
        fs::write(tempdir.path().join(".env.local"), "SECRET=local\n").unwrap();
        fs::write(tempdir.path().join(".env.production"), "#!/bin/sh\nSECRET=prod\n").unwrap();

        let report = discover_project_files(tempdir.path()).unwrap();
        let names: Vec<_> = report.files.iter().map(|f| f.relative_path.as_str()).collect();
        assert!(!names.contains(&".env"), ".env must be skipped: {names:?}");
        assert!(!names.contains(&".env.local"), ".env.local must be skipped: {names:?}");
        assert!(!names.contains(&".env.production"), ".env.production must be skipped: {names:?}");
        assert_eq!(report.files.len(), 0, "no files should be discovered: {names:?}");
    }

    /// Cargo.lock and yarn.lock are skipped.
    #[test]
    fn lockfiles_are_skipped() {
        let tempdir = tempdir().unwrap();
        fs::write(tempdir.path().join("Cargo.lock"), "[package]\nname = \"foo\"\n").unwrap();
        fs::write(tempdir.path().join("yarn.lock"), "# yarn lockfile v1\n").unwrap();
        fs::write(tempdir.path().join("pnpm-lock.yaml"), "lockfileVersion: 5\n").unwrap();
        fs::write(tempdir.path().join("poetry.lock"), "[[package]]\nname = \"foo\"\n").unwrap();
        fs::write(tempdir.path().join("composer.lock"), "{}").unwrap();
        fs::write(tempdir.path().join("Gemfile.lock"), "GEM\n").unwrap();
        // A regular file that should still be found.
        fs::write(tempdir.path().join("main.rs"), "fn main() {}\n").unwrap();

        let report = discover_project_files(tempdir.path()).unwrap();
        let names: Vec<_> = report.files.iter().map(|f| f.relative_path.as_str()).collect();
        assert!(!names.contains(&"Cargo.lock"), "Cargo.lock must be skipped");
        assert!(!names.contains(&"yarn.lock"), "yarn.lock must be skipped");
        assert!(!names.contains(&"pnpm-lock.yaml"), "pnpm-lock.yaml must be skipped");
        assert!(!names.contains(&"poetry.lock"), "poetry.lock must be skipped");
        assert!(!names.contains(&"composer.lock"), "composer.lock must be skipped");
        assert!(!names.contains(&"Gemfile.lock"), "Gemfile.lock must be skipped");
        assert!(names.contains(&"main.rs"), "main.rs must be discovered: {names:?}");
    }

    // ─── Secret-shaped path denylist tests (issue #95) ───────────────────────

    /// The classifier matches the secret-shaped names and extensions from issue
    /// #95, case-insensitively, and leaves non-secret files alone.
    #[test]
    fn secret_path_kind_matches_issue95_denylist() {
        let cases: &[(&str, Option<SecretPathKind>)] = &[
            (".env", Some(SecretPathKind::DotEnv)),
            (".env.local", Some(SecretPathKind::DotEnv)),
            (".ENV", Some(SecretPathKind::DotEnv)),
            ("prod.env", Some(SecretPathKind::DotEnv)),
            ("kubeconfig", Some(SecretPathKind::Kubeconfig)),
            ("kubeconfig.yaml", Some(SecretPathKind::Kubeconfig)),
            (".kubeconfig", Some(SecretPathKind::Kubeconfig)),
            ("id_rsa", Some(SecretPathKind::SshPrivateKey)),
            ("id_rsa.pub", Some(SecretPathKind::SshPrivateKey)),
            ("id_ed25519", Some(SecretPathKind::SshPrivateKey)),
            ("cert.pfx", Some(SecretPathKind::Keystore)),
            ("keystore.p12", Some(SecretPathKind::Keystore)),
            ("truststore.jks", Some(SecretPathKind::Keystore)),
            (".npmrc", Some(SecretPathKind::Netrc)),
            (".netrc", Some(SecretPathKind::Netrc)),
            ("terraform.tfstate", Some(SecretPathKind::TerraformSecret)),
            ("vars.tfvars", Some(SecretPathKind::TerraformSecret)),
            ("secrets.json", Some(SecretPathKind::SecretsJson)),
            ("secrets.local.json", Some(SecretPathKind::SecretsJson)),
            ("appsettings.local.json", Some(SecretPathKind::LocalJson)),
            ("config.json", None),
            ("main.rs", None),
            ("Dockerfile", None),
            (".gitignore", None),
            ("Cargo.lock", None),
        ];
        for (name, expected) in cases {
            assert_eq!(secret_path_kind(name), *expected, "secret_path_kind({name:?}) mismatch");
        }
    }

    /// The denylist applies to the filesystem walk: secret-shaped files are
    /// withheld, counted consistently, and recorded with path + reason (never
    /// content).
    #[test]
    fn secret_denylist_withheld_in_filesystem_discovery() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path();
        fs::write(root.join(".env"), "SECRET=abc\n").unwrap();
        fs::write(root.join("prod.env"), "SECRET=prod\n").unwrap();
        fs::write(root.join("kubeconfig.yaml"), "apiVersion: v1\n").unwrap();
        fs::write(root.join("id_rsa"), "PRIVATE KEY\n").unwrap();
        fs::write(root.join("secrets.local.json"), "{\"key\":\"x\"}\n").unwrap();
        fs::write(root.join("appsettings.local.json"), "{\"key\":\"x\"}\n").unwrap();
        fs::write(root.join("vars.tfvars"), "token = \"x\"\n").unwrap();
        fs::write(root.join("main.rs"), "fn main() {}\n").unwrap();
        fs::write(root.join("config.json"), "{\"ok\":true}\n").unwrap();

        let discovery = discover_project_sources(root).unwrap();
        assert_eq!(discovery.basis, ProjectSourceBasis::Filesystem);
        let names: Vec<_> = discovery.sources.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["config.json", "main.rs"]);
        // Every secret-shaped path is counted as a skip and recorded with a
        // reason.
        assert_eq!(discovery.skipped, 7, "skipped = {}", discovery.skipped);
        let recorded: Vec<(&str, &str)> = discovery
            .skips
            .iter()
            .map(|s| (s.relative_path.as_str(), s.reason.as_str()))
            .collect();
        assert_eq!(recorded.len(), 7);
        for (path, reason) in &recorded {
            assert!(!names.contains(path), "{path} must not be discovered");
            assert!(!reason.is_empty(), "skip record must carry a reason");
            assert!(
                !path.contains("SECRET") && !path.contains("PRIVATE") && !reason.contains("SECRET"),
                "skip records must never expose file content: {recorded:?}"
            );
        }
    }

    /// The denylist also protects Git-index discovery: a secret-shaped file
    /// that was committed is withheld and recorded, even though git would
    /// otherwise mine it.
    #[test]
    fn secret_denylist_withheld_in_git_index_discovery() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path().to_path_buf();
        git_init_with_commit(
            &root,
            "main",
            &[
                ("main.rs", "fn main() {}\n"),
                (".env", "SECRET=hunter2\n"),
                ("secrets.local.json", "{\"key\":\"hunter2\"}\n"),
                ("kubeconfig.yaml", "apiVersion: v1\n"),
            ],
        );

        let discovery = discover_project_sources(&root).unwrap();
        assert_eq!(discovery.basis, ProjectSourceBasis::GitIndex);
        let names: Vec<_> = discovery.sources.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["main.rs"]);
        assert_eq!(discovery.skipped, 3);
        let recorded: Vec<(&str, &str)> = discovery
            .skips
            .iter()
            .map(|s| (s.relative_path.as_str(), s.reason.as_str()))
            .collect();
        assert_eq!(
            recorded,
            vec![
                (".env", ".env / *.env"),
                ("kubeconfig.yaml", "*.kubeconfig*"),
                ("secrets.local.json", "secrets*.json"),
            ]
        );
    }

    /// Lockfiles and palace config are skipped but are not secret denylist
    /// matches, so they are counted without producing skip records.
    #[test]
    fn non_secret_skips_are_counted_but_not_recorded() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path();
        fs::write(root.join("Cargo.lock"), "[package]\nname = \"x\"\n").unwrap();
        fs::write(root.join("mempalace.yaml"), "wing: test\n").unwrap();
        fs::write(root.join(".env"), "SECRET=abc\n").unwrap();
        fs::write(root.join("main.rs"), "fn main() {}\n").unwrap();

        let discovery = discover_project_sources(root).unwrap();
        let names: Vec<_> = discovery.sources.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["main.rs"]);
        // Cargo.lock and mempalace.yaml are counted but not recorded; .env is
        // counted and recorded.
        assert_eq!(discovery.skipped, 3);
        assert_eq!(discovery.skips.len(), 1);
        assert_eq!(discovery.skips[0].relative_path, ".env");
        assert_eq!(discovery.skips[0].reason, ".env / *.env");
    }

    // ─── normalize_git_remote_url table tests ────────────────────────────────

    #[test]
    fn normalize_git_remote_url_ssh_style() {
        assert_eq!(
            normalize_git_remote_url("git@github.com:Acme/Repo.git"),
            Some("github.com/Acme/Repo".to_owned())
        );
    }

    #[test]
    fn normalize_git_remote_url_https_with_trailing_slash_and_git() {
        assert_eq!(
            normalize_git_remote_url("https://github.com/acme/repo.git/"),
            Some("github.com/acme/repo".to_owned())
        );
    }

    #[test]
    fn normalize_git_remote_url_ssh_with_port() {
        assert_eq!(
            normalize_git_remote_url("ssh://git@Host.Example:2222/team/repo"),
            Some("host.example/team/repo".to_owned())
        );
    }

    #[test]
    fn normalize_git_remote_url_https_with_user() {
        assert_eq!(
            normalize_git_remote_url("https://user@gitlab.com/a/b"),
            Some("gitlab.com/a/b".to_owned())
        );
    }

    #[test]
    fn normalize_git_remote_url_garbage_returns_none() {
        assert_eq!(normalize_git_remote_url("not-a-url"), None);
        assert_eq!(normalize_git_remote_url(""), None);
        assert_eq!(normalize_git_remote_url("   "), None);
    }

    #[test]
    fn normalize_git_remote_url_host_lowercased_path_preserved() {
        assert_eq!(
            normalize_git_remote_url("https://GITHUB.COM/Owner/MixedCase"),
            Some("github.com/Owner/MixedCase".to_owned())
        );
    }

    // ─── prepare_project_batch tests ─────────────────────────────────────────

    #[tokio::test]
    async fn prepare_batch_content_hash_matches_ingest_and_byte_ranges_correct() {
        let tempdir = tempdir().unwrap();
        let project_dir = tempdir.path().join("project");
        fs::create_dir_all(project_dir.join("src")).unwrap();
        fs::write(
            project_dir.join("mempalace.yaml"),
            "wing: testbatch\nrooms:\n  - name: general\n",
        )
        .unwrap();

        // Write a multi-chunk UTF-8 file.
        let file_content = "fn main() {\n    println!(\"hello\");\n}\n".repeat(30);
        fs::write(project_dir.join("src/main.rs"), &file_content).unwrap();

        // Write a non-UTF-8 file.
        let mut non_utf8 = vec![0xFF, 0xFE];
        non_utf8.extend_from_slice(b" binary content here  binary content here  binary content here  extra");
        fs::write(project_dir.join("src/binary.rs"), &non_utf8).unwrap();

        let request = ProjectIngestRequest::new(&project_dir);

        // Run prepare_project_batch.
        let prepared = prepare_project_batch(&request).unwrap();
        assert_eq!(prepared.wing, "testbatch");

        // Find the UTF-8 file DTO.
        let utf8_dto = prepared
            .files
            .iter()
            .find(|f| f.relative_path == "src/main.rs")
            .expect("src/main.rs must be in prepared files");

        assert!(utf8_dto.file_hash.is_some(), "UTF-8 file must have file_hash");
        assert!(!utf8_dto.chunks.is_empty(), "UTF-8 file must have chunks");

        // Verify byte ranges slice the file bytes correctly.
        let file_bytes = file_content.as_bytes();
        for chunk in &utf8_dto.chunks {
            let bs = chunk.byte_start.expect("UTF-8 chunk must have byte_start") as usize;
            let be = chunk.byte_end.expect("UTF-8 chunk must have byte_end") as usize;
            let sliced = std::str::from_utf8(&file_bytes[bs..be]).unwrap();
            assert_eq!(
                sliced, chunk.text,
                "file_bytes[byte_start..byte_end] must equal chunk text"
            );
        }

        // Now also run ingest_project into a temp engine and compare content_hash.
        let engine_dir = tempdir.path().join("engine");
        let engine = open_engine(&engine_dir).await;
        let mut provider =
            FakeEmbeddingProvider::new(EmbeddingProfile::Balanced.metadata().dimensions);
        ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: project_dir.clone(),
                wing: None,
                agent: "tester".to_owned(),
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
                branch: false,
        view: None,},
        )
        .await
        .unwrap();

        // Resolve the stored source key for src/main.rs.
        let root = project_dir.canonicalize().unwrap();
        let wing_name = "testbatch";
        let repo_id = derive_repo_id(&root, wing_name);
        let sk = project_source_key(
            "projects",
            &stable_project_root_key(&repo_id),
            wing_name,
            "src/main.rs",
        );
        let stored =
            engine.operational_store().get_ingested_file(&sk).unwrap().expect("must be stored");
        assert_eq!(
            utf8_dto.content_hash, stored.content_hash,
            "prepare_project_batch content_hash must match stored content_hash"
        );
    }

    #[tokio::test]
    async fn project_ingest_migrates_legacy_path_hashed_source_keys() {
        let tempdir = tempdir().unwrap();
        let project_dir = tempdir.path().join("project");
        fs::create_dir_all(project_dir.join("src")).unwrap();
        fs::write(
            project_dir.join("mempalace.yaml"),
            "wing: migration_test\nrooms:\n  - name: general\n",
        )
        .unwrap();
        fs::write(
            project_dir.join("src/main.rs"),
            "fn main() { println!(\"migration\"); }\n".repeat(12),
        )
        .unwrap();

        let root = project_dir.canonicalize().unwrap();
        let wing_name = "migration_test";
        let legacy_key = legacy_project_source_key(
            "projects",
            &hash_text(&root.to_string_lossy()),
            wing_name,
            "src/main.rs",
        );
        let repo_id = derive_repo_id(&root, wing_name);
        let stable_key = project_source_key(
            "projects",
            &stable_project_root_key(&repo_id),
            wing_name,
            "src/main.rs",
        );

        let engine = open_engine(&tempdir.path().join("palace")).await;
        let mut provider = FakeEmbeddingProvider::new(EmbeddingProfile::Balanced.metadata().dimensions);
        let legacy_drawers = build_drawers(
            &mut provider,
            &wing_id(wing_name).unwrap(),
            &legacy_key,
            "src/main.rs",
            "projects",
            None,
            "legacy",
            None,
            vec![Chunk {
                content: "legacy drawer content that should be migrated".to_owned(),
                chunk_index: 0,
                room_hint: Some("general".to_owned()),
                date_hint: None,
                byte_range: None,
            }],
            None,
            None,
            None,
        )
        .unwrap();
        engine
            .commit_ingest(IngestCommitRequest {
                ingest_kind: "projects".to_owned(),
                source_key: legacy_key.clone(),
                source_file: "src/main.rs".to_owned(),
                content_hash: "legacy-content-hash".to_owned(),
                drawers: legacy_drawers,
                duplicate_strategy: DuplicateStrategy::Overwrite,
            })
            .await
            .unwrap();

        ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest::new(&project_dir),
        )
        .await
        .unwrap();

        assert!(engine.operational_store().get_ingested_file(&legacy_key).unwrap().is_none());
        assert!(engine
            .operational_store()
            .committed_drawer_ids_for_source_key(&legacy_key)
            .unwrap()
            .is_empty());
        assert!(!engine
            .operational_store()
            .committed_drawer_ids_for_source_key(&stable_key)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn prepare_batch_non_utf8_file_has_no_file_hash_and_no_ranges() {
        let tempdir = tempdir().unwrap();
        let project_dir = tempdir.path().join("project");
        fs::create_dir_all(&project_dir).unwrap();
        fs::write(
            project_dir.join("mempalace.yaml"),
            "wing: testbatch\nrooms:\n  - name: general\n",
        )
        .unwrap();

        // Non-UTF-8 content long enough to produce chunks.
        let mut non_utf8 = vec![0xFF, 0xFE];
        let fill: Vec<u8> = b" non utf8 filler content here  non utf8 filler content here  ".repeat(15).to_vec();
        non_utf8.extend(fill);
        fs::write(project_dir.join("data.rs"), &non_utf8).unwrap();

        let request = ProjectIngestRequest::new(&project_dir);
        let prepared = prepare_project_batch(&request).unwrap();

        // Non-UTF-8 files that produce chunks: file_hash must be None, no ranges.
        for file_dto in &prepared.files {
            if file_dto.relative_path == "data.rs" {
                assert!(file_dto.file_hash.is_none(), "non-UTF-8 file must have no file_hash");
                for chunk in &file_dto.chunks {
                    assert!(chunk.byte_start.is_none(), "non-UTF-8 chunk must have no byte_start");
                    assert!(chunk.byte_end.is_none(), "non-UTF-8 chunk must have no byte_end");
                }
            }
        }
    }

    #[test]
    fn prepare_batch_zero_chunk_files_excluded() {
        let tempdir = tempdir().unwrap();
        let project_dir = tempdir.path().join("project");
        fs::create_dir_all(&project_dir).unwrap();
        fs::write(
            project_dir.join("mempalace.yaml"),
            "wing: testbatch\nrooms:\n  - name: general\n",
        )
        .unwrap();
        // Tiny file below the minimum chunk size gate.
        fs::write(project_dir.join("tiny.rs"), "fn x() {}").unwrap();

        let request = ProjectIngestRequest::new(&project_dir);
        let prepared = prepare_project_batch(&request).unwrap();

        // The tiny file must not appear in files (zero chunks → excluded in v1).
        let found = prepared.files.iter().any(|f| f.relative_path == "tiny.rs");
        assert!(!found, "zero-chunk file must be excluded from prepare_project_batch files");
    }

    #[test]
    fn prepare_batch_preserves_requested_view_in_summary() {
        let tempdir = tempdir().unwrap();
        let project_dir = tempdir.path().join("project");
        fs::create_dir_all(&project_dir).unwrap();
        fs::write(
            project_dir.join("mempalace.yaml"),
            "wing: testbatch\nrooms:\n  - name: general\n",
        )
        .unwrap();

        let mut request = ProjectIngestRequest::new(&project_dir);
        request.view = Some("feature-x".to_owned());

        let prepared = prepare_project_batch(&request).unwrap();
        assert_eq!(prepared.summary.view_name.as_deref(), Some("feature-x"));
    }

    // ─── Branch delta tests ───────────────────────────────────────────────────

    /// Initialize a git repo at `dir` with a single commit on branch `main`.
    fn git_init_with_commit(dir: &Path, branch: &str, files: &[(&str, &str)]) {
        let run = |args: &[&str]| {
            let status = Command::new("git")
                .args(args)
                .current_dir(dir)
                .status()
                .unwrap();
            assert!(status.success(), "git {args:?} failed");
        };
        run(&["init", "-b", branch]);
        run(&["-c", "user.email=test@test.com", "-c", "user.name=Test", "config", "user.email", "test@test.com"]);
        run(&["-c", "user.email=test@test.com", "-c", "user.name=Test", "config", "user.name", "Test"]);
        for (path, content) in files {
            let full = dir.join(path);
            if let Some(parent) = full.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(&full, content).unwrap();
            // Force-add so a developer's global gitignore (e.g. one that excludes
            // `mempalace.yaml`) can't silently block staging the fixture file.
            run(&["add", "-f", path]);
        }
        run(&[
            "-c", "user.email=test@test.com",
            "-c", "user.name=Test",
            "commit", "-m", "initial",
        ]);
    }

    #[tokio::test]
    async fn branch_mine_only_delta_files() {
        let tempdir = tempdir().unwrap();
        let repo_dir = tempdir.path().join("repo");
        fs::create_dir_all(&repo_dir).unwrap();

        let base_content = "fn base() -> i32 { 42 }\n".repeat(5);
        let stable_content = "fn stable() -> &str { \"hello\" }\n".repeat(5);

        git_init_with_commit(
            &repo_dir,
            "main",
            &[
                ("mempalace.yaml", "wing: branchtest\nrooms:\n  - name: general\n"),
                ("base.rs", &base_content),
                ("stable.rs", &stable_content),
            ],
        );
        // A host-global excludes file must not silently drop the delta files.
        pin_absent_global_excludes(&repo_dir);

        // Create and switch to feature branch.
        let run_git = |args: &[&str]| {
            Command::new("git")
                .args(args)
                .current_dir(&repo_dir)
                .status()
                .unwrap();
        };
        run_git(&["checkout", "-b", "feature"]);

        // Modify one file and add one untracked file.
        let changed_content = "fn base() -> i32 { 99 }\n".repeat(5);
        fs::write(repo_dir.join("base.rs"), &changed_content).unwrap();
        let new_content = "fn new_func() -> bool { true }\n".repeat(5);
        fs::write(repo_dir.join("new_file.rs"), &new_content).unwrap();

        let engine = open_engine(&tempdir.path().join("palace")).await;
        let mut provider =
            FakeEmbeddingProvider::new(EmbeddingProfile::Balanced.metadata().dimensions);

        let summary = ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: repo_dir.clone(),
                wing: None,
                agent: "tester".to_owned(),
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
                branch: true,
        view: None,},
        )
        .await
        .unwrap();

        // Only the 2 delta files (base.rs modified + new_file.rs untracked) are mined.
        assert_eq!(summary.ingested_files, 2, "branch mine must ingest only delta files");
        assert_eq!(summary.removed_sources, 0);

        // Source keys must use the projects-branch prefix.
        let root = repo_dir.canonicalize().unwrap();
        let wing_name = "branchtest";
        let repo_id = derive_repo_id(&root, wing_name);
        let project_root_key = stable_project_root_key(&repo_id);
        let sk_base = project_branch_source_key(
            "projects-branch",
            &project_root_key,
            wing_name,
            "feature",
            "base.rs",
        );
        let stored_base =
            engine.operational_store().get_ingested_file(&sk_base).unwrap();
        assert!(stored_base.is_some(), "base.rs must be stored under projects-branch key");

        let sk_stable = project_branch_source_key(
            "projects-branch",
            &project_root_key,
            wing_name,
            "feature",
            "stable.rs",
        );
        let stored_stable =
            engine.operational_store().get_ingested_file(&sk_stable).unwrap();
        assert!(stored_stable.is_none(), "stable.rs must NOT be stored (not in delta)");
    }

    #[tokio::test]
    async fn branch_cleanup_removes_departed_files() {
        let tempdir = tempdir().unwrap();
        let repo_dir = tempdir.path().join("repo");
        fs::create_dir_all(&repo_dir).unwrap();

        let base_content = "fn base() -> i32 { 42 }\n".repeat(5);
        let stable_content = "fn stable() -> &str { \"hello\" }\n".repeat(5);

        git_init_with_commit(
            &repo_dir,
            "main",
            &[
                ("mempalace.yaml", "wing: cleanuptest\nrooms:\n  - name: general\n"),
                ("base.rs", &base_content),
                ("stable.rs", &stable_content),
            ],
        );

        let run_git = |args: &[&str]| {
            Command::new("git")
                .args(args)
                .current_dir(&repo_dir)
                .status()
                .unwrap();
        };
        run_git(&["checkout", "-b", "feature"]);

        // First mine: modify base.rs and stable.rs + add untracked.rs.  The
        // stable.rs content deliberately aliases the legacy tombstone hash.
        let changed_content = "fn base() -> i32 { 99 }\n".repeat(5);
        fs::write(repo_dir.join("base.rs"), &changed_content).unwrap();
        fs::write(repo_dir.join("stable.rs"), "removed").unwrap();
        let new_content = "fn new_func() -> bool { true }\n".repeat(5);
        fs::write(repo_dir.join("untracked.rs"), &new_content).unwrap();

        let engine = open_engine(&tempdir.path().join("palace")).await;
        let mut provider =
            FakeEmbeddingProvider::new(EmbeddingProfile::Balanced.metadata().dimensions);

        let first = ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: repo_dir.clone(),
                wing: None,
                agent: "tester".to_owned(),
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
                branch: true,
        view: None,},
        )
        .await
        .unwrap();
        assert_eq!(first.ingested_files, 3);
        assert_eq!(first.removed_sources, 0);

        // Revert base.rs to original content and delete a canonical file.
        fs::write(repo_dir.join("base.rs"), &base_content).unwrap();
        fs::remove_file(repo_dir.join("stable.rs")).unwrap();

        let second = ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: repo_dir.clone(),
                wing: None,
                agent: "tester".to_owned(),
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
                branch: true,
        view: None,},
        )
        .await
        .unwrap();

        // base.rs is no longer in the delta (reverted), stable.rs gains a durable
        // tombstone, and untracked.rs still is part of the branch delta.
        assert_eq!(second.removed_sources, 1);

        // Reverting a branch replacement removes the branch state entirely so
        // a later deletion can create a fresh durable tombstone.
        let root = repo_dir.canonicalize().unwrap();
        let wing_name = "cleanuptest";
        let repo_id = derive_repo_id(&root, wing_name);
        let sk_base = project_branch_source_key(
            "projects-branch",
            &stable_project_root_key(&repo_id),
            wing_name,
            "feature",
            "base.rs",
        );
        assert!(engine.operational_store().get_ingested_file(&sk_base).unwrap().is_none());

        let sk_stable = project_branch_source_key(
            "projects-branch",
            &stable_project_root_key(&repo_id),
            wing_name,
            "feature",
            "stable.rs",
        );
        let tombstone = engine.operational_store().get_ingested_file(&sk_stable).unwrap();
        assert_eq!(
            tombstone.map(|record| record.content_hash),
            Some(hash_text("Deleted branch path tombstone"))
        );
        let tombstone_drawers = engine
            .drawer_store()
            .list_drawers(&DrawerFilter {
                view: Some("feature".to_owned()),
                branch_view_only: true,
                source_file: Some("stable.rs".to_owned()),
                ..DrawerFilter::default()
            })
            .await
            .unwrap();
        assert!(!tombstone_drawers.is_empty());
        assert!(tombstone_drawers.iter().all(|drawer| {
            drawer
                .view_metadata
                .as_ref()
                .is_some_and(|metadata| metadata.path_state == "deleted")
        }));

        let third = ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: repo_dir.clone(),
                wing: None,
                agent: "tester".to_owned(),
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
                branch: true,
                view: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(third.removed_sources, 0, "an unchanged tombstone remains durable");
    }

    #[tokio::test]
    async fn branch_mine_subdir_project_re_relativizes_correctly() {
        let tempdir = tempdir().unwrap();
        let repo_dir = tempdir.path().join("repo");
        let project_dir = repo_dir.join("crates").join("mylib");
        fs::create_dir_all(project_dir.join("src")).unwrap();

        // Also put a file outside the project dir.
        fs::create_dir_all(repo_dir.join("other")).unwrap();

        git_init_with_commit(
            &repo_dir,
            "main",
            &[
                ("README.md", "# repo\n"),
                ("crates/mylib/mempalace.yaml", "wing: mylib\nrooms:\n  - name: general\n"),
                ("crates/mylib/src/lib.rs", "pub fn stable() {}\n".repeat(5).as_str()),
                ("other/outside.rs", "fn outside() {}\n".repeat(5).as_str()),
            ],
        );

        let run_git = |args: &[&str]| {
            Command::new("git")
                .args(args)
                .current_dir(&repo_dir)
                .status()
                .unwrap();
        };
        run_git(&["checkout", "-b", "feature"]);

        // Modify a file inside the project subdir and a file outside it.
        let changed = "pub fn changed() {}\n".repeat(5);
        fs::write(project_dir.join("src/lib.rs"), &changed).unwrap();
        let outside_changed = "fn outside_changed() {}\n".repeat(5);
        fs::write(repo_dir.join("other/outside.rs"), &outside_changed).unwrap();

        let engine = open_engine(&tempdir.path().join("palace")).await;
        let mut provider =
            FakeEmbeddingProvider::new(EmbeddingProfile::Balanced.metadata().dimensions);

        let summary = ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: project_dir.clone(),
                wing: None,
                agent: "tester".to_owned(),
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
                branch: true,
        view: None,},
        )
        .await
        .unwrap();

        // Only src/lib.rs (inside the project) should be mined; outside.rs should be ignored.
        assert_eq!(
            summary.ingested_files, 1,
            "only the file inside the project subdir must be mined"
        );
    }

    #[tokio::test]
    async fn branch_mine_not_a_git_repo_returns_error() {
        let tempdir = tempdir().unwrap();
        let project_dir = tempdir.path().join("notgit");
        fs::create_dir_all(&project_dir).unwrap();
        fs::write(
            project_dir.join("mempalace.yaml"),
            "wing: test\nrooms:\n  - name: general\n",
        )
        .unwrap();
        fs::write(
            project_dir.join("main.rs"),
            "fn main() {}\n".repeat(10),
        )
        .unwrap();

        let engine = open_engine(tempdir.path()).await;
        let mut provider =
            FakeEmbeddingProvider::new(EmbeddingProfile::Balanced.metadata().dimensions);

        let result = ingest_project(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: project_dir.clone(),
                wing: None,
                agent: "tester".to_owned(),
                limit: None,
                dry_run: false,
                reindex: false,
                max_embed_batch_size: None,
                branch: true,
        view: None,},
        )
        .await;

        assert!(
            matches!(result, Err(IngestError::BranchDeltaUnavailable { .. })),
            "expected BranchDeltaUnavailable, got: {result:?}"
        );
    }

    // ─── Linked worktree skip tests ───────────────────────────────────────────

    #[test]
    fn linked_worktrees_are_skipped_during_discovery() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path().to_path_buf();

        // Initialise a git repo with a single commit.
        git_init_with_commit(
            &root,
            "main",
            &[("main.rs", "fn main() {}\n")],
        );

        let worktree_dir = root.join("worktree");
        // Create a linked worktree (the directory must not exist beforehand).
        let status = Command::new("git")
            .args([
                "worktree",
                "add",
                "-b",
                "linked",
                worktree_dir.to_str().unwrap(),
            ])
            .current_dir(&root)
            .status()
            .unwrap();
        assert!(status.success(), "git worktree add failed");

        // Drop a file inside the linked worktree.
        fs::write(worktree_dir.join("extra.rs"), "fn extra() {}\n").unwrap();

        // Also create a sibling directory that is NOT a worktree, and track it
        // so it is part of the safe (tracked-index) source set.
        fs::create_dir_all(root.join("sibling")).unwrap();
        fs::write(root.join("sibling").join("sibling.rs"), "fn sibling() {}\n").unwrap();
        let status = Command::new("git")
            .args(["add", "sibling/sibling.rs"])
            .current_dir(&root)
            .status()
            .unwrap();
        assert!(status.success(), "git add sibling/sibling.rs failed");

        let report = discover_project_files(&root).unwrap();
        let names: Vec<_> = report.files.iter().map(|f| f.relative_path.as_str()).collect();

        // main.rs in the main worktree must be discovered.
        assert!(
            names.contains(&"main.rs"),
            "main.rs should be discovered: {names:?}"
        );
        // sibling.rs in a normal subdirectory must be discovered.
        assert!(
            names.contains(&"sibling/sibling.rs"),
            "sibling/sibling.rs should be discovered: {names:?}"
        );
        // extra.rs is inside the linked worktree and must not be discovered.
        assert!(
            !names.contains(&"worktree/extra.rs"),
            "worktree/extra.rs must be skipped: {names:?}"
        );

        // The linked worktree is a known excluded checkout, and its untracked
        // content never enters the safe source set (nothing mined under it).
        let worktree_dir_canon =
            worktree_dir.canonicalize().unwrap_or_else(|_| worktree_dir.clone());
        assert!(
            linked_worktree_paths(&root).contains(&worktree_dir_canon),
            "linked worktree path must be excluded by discovery"
        );
        assert!(
            names.iter().all(|name| !name.starts_with("worktree/")),
            "no linked-worktree paths may be discovered: {names:?}"
        );

        // Clean up the linked worktree so tempdir removal doesn't trip over it.
        Command::new("git")
            .args(["worktree", "remove", "--force", worktree_dir.to_str().unwrap()])
            .current_dir(&root)
            .status()
            .ok();
    }

    #[test]
    fn linked_worktrees_are_skipped_from_relative_root() {
        let tempdir = Builder::new().prefix("mempalace-ingest-").tempdir_in(".").unwrap();
        let root = tempdir.path().to_path_buf();
        git_init_with_commit(&root, "main", &[("main.rs", "fn main() {}\n")]);

        let worktree_dir = root.join("worktree");
        let status = Command::new("git")
            .args([
                "worktree",
                "add",
                "-b",
                "linked",
                worktree_dir.to_str().unwrap(),
            ])
            .current_dir(&root)
            .status()
            .unwrap();
        assert!(status.success(), "git worktree add failed");
        fs::write(worktree_dir.join("extra.rs"), "fn extra() {}\n").unwrap();

        let current_dir = std::env::current_dir().unwrap();
        let relative_root = root.strip_prefix(current_dir).unwrap();
        let report = discover_project_files(relative_root).unwrap();

        assert!(
            !report.files.iter().any(|file| file.relative_path == "worktree/extra.rs"),
            "linked worktree file must be skipped: {:?}",
            report.files
        );

        Command::new("git")
            .args(["worktree", "remove", "--force", worktree_dir.to_str().unwrap()])
            .current_dir(&root)
            .status()
            .ok();
    }

    #[test]
    fn linked_worktree_paths_non_git_dir_returns_empty() {
        let tempdir = tempdir().unwrap();
        let paths = linked_worktree_paths(tempdir.path());
        assert!(paths.is_empty(), "expected empty set for non-git dir");
    }

    #[test]
    #[cfg(unix)]
    fn linked_worktree_paths_accept_non_utf8_and_newline_paths() {
        use std::os::unix::ffi::OsStrExt as _;

        let output = b"worktree /repo\0HEAD abc\0\0worktree /repo/linked\nbranch-\xff\0HEAD def\0\0";
        let paths = parse_linked_worktree_paths(output);

        assert_eq!(paths.len(), 1);
        assert_eq!(
            paths.first().unwrap().as_os_str().as_bytes(),
            b"/repo/linked\nbranch-\xff"
        );
    }

    #[test]
    #[cfg(unix)]
    fn linked_worktree_paths_normalize_symlinked_paths() {
        use std::os::unix::fs::symlink;

        let tempdir = tempdir().unwrap();
        let worktree_dir = tempdir.path().join("worktree");
        fs::create_dir(&worktree_dir).unwrap();
        let symlinked_worktree = tempdir.path().join("worktree-link");
        symlink(&worktree_dir, &symlinked_worktree).unwrap();

        let output = format!(
            "worktree /repo\0HEAD abc\0\0worktree {}\0HEAD def\0\0",
            symlinked_worktree.display()
        );
        let paths = parse_linked_worktree_paths(output.as_bytes())
            .into_iter()
            .map(|path| path.canonicalize().unwrap_or(path))
            .collect::<BTreeSet<_>>();

        assert!(paths.contains(&worktree_dir));
    }

    // ─── Centralized project-source discovery tests ──────────────────────────

    /// Git-backed roots enumerate tracked index paths only: untracked and
    /// gitignored working-tree files never enter the source set.
    #[test]
    fn git_index_discovery_uses_tracked_sources_only() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path().to_path_buf();
        git_init_with_commit(
            &root,
            "main",
            &[("tracked.md", "tracked\n"), ("main.rs", "fn main() {}\n")],
        );

        // Untracked working-tree content: an ignored secret, an ignored local
        // override (not in the built-in skip list), and a plain file.
        fs::write(root.join(".env"), "SECRET=hunter2\n").unwrap();
        fs::write(root.join("secrets.local.json"), "{\"key\":\"hunter2\"}\n").unwrap();
        fs::write(root.join(".gitignore"), ".env\nsecrets.local.json\n").unwrap();
        fs::write(root.join("scratch.rs"), "fn scratch() {}\n").unwrap();

        let discovery = discover_project_sources(&root).unwrap();
        assert_eq!(discovery.basis, ProjectSourceBasis::GitIndex);
        let names: Vec<_> = discovery.sources.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["main.rs", "tracked.md"]);
        assert!(!names.contains(&".env"));
        assert!(!names.contains(&"secrets.local.json"));
        assert!(!names.contains(&"scratch.rs"));
        // The gitignored/untracked files are simply absent from the index, not
        // counted as skipped candidates.
        assert_eq!(discovery.skipped, 0);
    }

    /// .mempalaceignore still applies to tracked files in git-index mode.
    #[test]
    fn git_index_discovery_honors_mempalace_ignore_for_tracked_files() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path().to_path_buf();
        git_init_with_commit(
            &root,
            "main",
            &[("tracked.md", "tracked\n"), ("main.rs", "fn main() {}\n")],
        );
        fs::write(root.join(".mempalaceignore"), "main.rs\n").unwrap();

        let discovery = discover_project_sources(&root).unwrap();
        assert_eq!(discovery.basis, ProjectSourceBasis::GitIndex);
        let names: Vec<_> = discovery.sources.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["tracked.md"]);
        assert_eq!(discovery.skipped, 1);
    }

    /// A tracked file deleted from the working tree is skipped, not mined.
    #[test]
    fn git_index_discovery_skips_tracked_file_missing_on_disk() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path().to_path_buf();
        git_init_with_commit(
            &root,
            "main",
            &[("tracked.md", "tracked\n"), ("gone.rs", "fn gone() {}\n")],
        );
        fs::remove_file(root.join("gone.rs")).unwrap();

        let discovery = discover_project_sources(&root).unwrap();
        assert_eq!(discovery.basis, ProjectSourceBasis::GitIndex);
        let names: Vec<_> = discovery.sources.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["tracked.md"]);
        assert_eq!(discovery.skipped, 1);
    }

    /// Git-index discovery is deterministic (sorted) and root-relative.
    #[test]
    fn git_index_discovery_is_deterministic_and_root_relative() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path().to_path_buf();
        git_init_with_commit(
            &root,
            "main",
            &[
                ("zebra.md", "z\n"),
                ("alpha/src/lib.rs", "fn lib() {}\n"),
                ("mid/dir/beta.md", "b\n"),
            ],
        );

        let discovery = discover_project_sources(&root).unwrap();
        assert_eq!(discovery.basis, ProjectSourceBasis::GitIndex);
        let names: Vec<_> = discovery.sources.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["alpha/src/lib.rs", "mid/dir/beta.md", "zebra.md"]);
        for source in &discovery.sources {
            assert!(
                source.absolute_path.starts_with(&root),
                "absolute path for {}",
                source.relative_path
            );
        }
    }

    /// A tracked file remains eligible even after `.gitignore` later names it:
    /// tracked index paths are not subject to a (possibly untracked) ignore
    /// file — git keeps tracked paths tracked.
    #[test]
    fn git_index_discovery_keeps_tracked_files_even_when_gitignored() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path().to_path_buf();
        git_init_with_commit(
            &root,
            "main",
            &[("tracked.md", "tracked\n"), ("docs/notes.md", "notes\n")],
        );
        fs::write(root.join(".gitignore"), "*.md\n").unwrap();

        let discovery = discover_project_sources(&root).unwrap();
        assert_eq!(discovery.basis, ProjectSourceBasis::GitIndex);
        let names: Vec<_> = discovery.sources.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["docs/notes.md", "tracked.md"]);
        assert_eq!(discovery.skipped, 0);
    }

    /// `.mempalaceignore` is the explicit additional exclusion in git-index
    /// mode: it suppresses tracked files even when `.gitignore` does not.
    #[test]
    fn git_index_discovery_applies_mempalace_ignore_even_over_gitignore() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path().to_path_buf();
        git_init_with_commit(
            &root,
            "main",
            &[("tracked.md", "tracked\n"), ("docs/notes.md", "notes\n")],
        );
        fs::write(root.join(".gitignore"), "*.md\n").unwrap();
        fs::write(root.join(".mempalaceignore"), "tracked.md\n").unwrap();

        let discovery = discover_project_sources(&root).unwrap();
        assert_eq!(discovery.basis, ProjectSourceBasis::GitIndex);
        let names: Vec<_> = discovery.sources.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["docs/notes.md"]);
        assert_eq!(discovery.skipped, 1);
    }

    /// Nested `.mempalaceignore` files apply to tracked files in their scope.
    #[test]
    fn git_index_discovery_honors_nested_mempalace_ignore() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path().to_path_buf();
        git_init_with_commit(
            &root,
            "main",
            &[
                ("src/lib.rs", "fn lib() {}\n"),
                ("src/skip.me.md", "skip\n"),
                ("docs/keep.md", "keep\n"),
            ],
        );
        fs::write(root.join("src").join(".mempalaceignore"), "skip.me.md\n").unwrap();

        let discovery = discover_project_sources(&root).unwrap();
        assert_eq!(discovery.basis, ProjectSourceBasis::GitIndex);
        let names: Vec<_> = discovery.sources.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["docs/keep.md", "src/lib.rs"]);
        assert_eq!(discovery.skipped, 1);
    }

    /// A directory-only `.mempalaceignore` rule (`generated/`) excludes the
    /// tracked files beneath that directory in git-index mode.
    #[test]
    fn git_index_discovery_honors_directory_only_mempalace_ignore() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path().to_path_buf();
        git_init_with_commit(
            &root,
            "main",
            &[("generated/out.rs", "fn out() {}\n"), ("src/lib.rs", "fn lib() {}\n")],
        );
        fs::write(root.join(".mempalaceignore"), "generated/\n").unwrap();

        let discovery = discover_project_sources(&root).unwrap();
        assert_eq!(discovery.basis, ProjectSourceBasis::GitIndex);
        let names: Vec<_> = discovery.sources.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["src/lib.rs"]);
        assert_eq!(discovery.skipped, 1);
    }

    /// Non-Git roots keep the filesystem walk and report the Filesystem basis.
    #[test]
    fn discover_project_sources_reports_filesystem_basis_for_non_git() {
        let tempdir = tempdir().unwrap();
        fs::write(tempdir.path().join(".gitignore"), "ignored/\n").unwrap();
        fs::create_dir_all(tempdir.path().join("ignored")).unwrap();
        fs::write(tempdir.path().join("ignored").join("secret.md"), "hidden").unwrap();
        fs::create_dir_all(tempdir.path().join("keep")).unwrap();
        fs::write(tempdir.path().join("keep").join("visible.md"), "visible").unwrap();

        let discovery = discover_project_sources(tempdir.path()).unwrap();
        assert_eq!(discovery.basis, ProjectSourceBasis::Filesystem);
        let names: Vec<_> = discovery.sources.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["keep/visible.md"]);
        assert!(discovery.skipped >= 2);
    }

    /// Nested `.gitignore` files apply relative to their own directory, so a
    /// deeper ignore file can hide files a shallower one would keep.
    #[test]
    fn filesystem_discovery_honors_nested_gitignore() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path();
        fs::write(root.join(".gitignore"), "*.log\n").unwrap();
        fs::create_dir_all(root.join("sub")).unwrap();
        fs::write(root.join("sub").join(".gitignore"), "secret.md\n").unwrap();
        fs::write(root.join("sub").join("secret.md"), "hidden").unwrap();
        fs::write(root.join("sub").join("visible.md"), "visible").unwrap();
        fs::write(root.join("trace.log"), "noise").unwrap();

        let discovery = discover_project_sources(root).unwrap();
        assert_eq!(discovery.basis, ProjectSourceBasis::Filesystem);
        let names: Vec<_> = discovery.sources.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["sub/visible.md"]);
    }

    /// `!`-negated patterns re-include paths a previous rule excluded.
    #[test]
    fn filesystem_discovery_supports_negation() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path();
        fs::write(root.join(".gitignore"), "*.md\n!important.md\n").unwrap();
        fs::write(root.join("trace.md"), "noise").unwrap();
        fs::write(root.join("important.md"), "keep").unwrap();

        let discovery = discover_project_sources(root).unwrap();
        assert_eq!(discovery.basis, ProjectSourceBasis::Filesystem);
        let names: Vec<_> = discovery.sources.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["important.md"]);
    }

    /// Slash-anchored patterns match relative to the ignore file's directory,
    /// unlike unanchored basename patterns which match at any depth.
    #[test]
    fn filesystem_discovery_honors_anchored_patterns() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path();
        fs::write(root.join(".gitignore"), "/artifacts/\n").unwrap();
        fs::create_dir_all(root.join("artifacts")).unwrap();
        fs::write(root.join("artifacts").join("artifact.md"), "a").unwrap();
        fs::create_dir_all(root.join("src").join("artifacts")).unwrap();
        fs::write(root.join("src").join("artifacts").join("artifact.md"), "b").unwrap();
        fs::write(root.join("src").join("lib.md"), "lib").unwrap();

        let discovery = discover_project_sources(root).unwrap();
        assert_eq!(discovery.basis, ProjectSourceBasis::Filesystem);
        let names: Vec<_> = discovery.sources.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["src/artifacts/artifact.md", "src/lib.md"]);
    }

    /// Git glob semantics: `*` does not cross `/`, `?` matches one character,
    /// and `**` spans directory levels.
    #[test]
    fn filesystem_discovery_supports_globs_and_doublestar() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path();
        fs::write(
            root.join(".gitignore"),
            "doc/*.md\n**/cache/tmp?.md\na/**/b/m.md\n",
        )
        .unwrap();
        fs::create_dir_all(root.join("doc").join("nested")).unwrap();
        fs::write(root.join("doc").join("readme.md"), "r").unwrap();
        fs::write(root.join("doc").join("nested").join("readme.md"), "r2").unwrap();
        fs::create_dir_all(root.join("x").join("cache")).unwrap();
        fs::write(root.join("x").join("cache").join("tmp1.md"), "t1").unwrap();
        fs::write(root.join("x").join("cache").join("tmp12.md"), "t12").unwrap();
        fs::create_dir_all(root.join("a").join("x").join("b")).unwrap();
        fs::write(root.join("a").join("x").join("b").join("m.md"), "m").unwrap();
        fs::write(root.join("a").join("b").join("n.md"), "n").unwrap();

        let discovery = discover_project_sources(root).unwrap();
        assert_eq!(discovery.basis, ProjectSourceBasis::Filesystem);
        let names: Vec<_> = discovery.sources.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["a/b/n.md", "doc/nested/readme.md", "x/cache/tmp12.md"]);
    }

    /// A trailing `/**` matches everything inside the named directory but not
    /// the directory itself, exactly as git's `abc/**` keeps `abc` traversable
    /// so a `!abc/keep.md` rule can still re-include a file beneath it.
    #[test]
    fn filesystem_discovery_trailing_doublestar_does_not_exclude_directory() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path();
        fs::write(root.join(".gitignore"), "abc/**\n!abc/keep.md\n").unwrap();
        fs::create_dir_all(root.join("abc")).unwrap();
        fs::write(root.join("abc").join("keep.md"), "kept\n").unwrap();
        fs::write(root.join("abc").join("drop.md"), "ignored\n").unwrap();

        let discovery = discover_project_sources(root).unwrap();
        assert_eq!(discovery.basis, ProjectSourceBasis::Filesystem);
        let names: Vec<_> = discovery.sources.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["abc/keep.md"]);
        assert!(discovery.skipped >= 1, "abc/drop.md should be skipped");
    }

    /// A trailing `/**` on a directory-only rule still ignores only what is
    /// inside the directory, mirroring git's `abc/**/`: the directory `abc`
    /// itself and files directly inside it stay, while subdirectories (and
    /// everything beneath them) are ignored.
    #[test]
    fn filesystem_discovery_trailing_doublestar_directory_only_keeps_dir_itself() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path();
        fs::write(root.join(".gitignore"), "abc/**/\n").unwrap();
        fs::create_dir_all(root.join("abc").join("sub")).unwrap();
        fs::write(root.join("abc").join("top.md"), "kept\n").unwrap();
        fs::write(root.join("abc").join("sub").join("drop.md"), "ignored\n").unwrap();

        let discovery = discover_project_sources(root).unwrap();
        assert_eq!(discovery.basis, ProjectSourceBasis::Filesystem);
        let names: Vec<_> = discovery.sources.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["abc/top.md"]);
        assert!(discovery.skipped >= 1, "abc/sub should be skipped");
    }

    /// An unescaped trailing space is stripped, but an escaped one is kept: a
    /// `foo\ ` pattern targets a filename literally ending in a space, as in
    /// git.
    #[test]
    fn parse_ignore_pattern_strips_unescaped_but_keeps_escaped_trailing_space() {
        let bare = parse_ignore_pattern("notes.txt ").unwrap();
        assert_eq!(bare.parts, vec!["notes.txt".to_owned()]);

        let escaped = parse_ignore_pattern("trail\\ ").unwrap();
        assert_eq!(escaped.parts, vec!["trail\\ ".to_owned()]);
        assert!(escaped.matches_path("trail ", false));
        assert!(!escaped.matches_path("trail", false));

        let tabbed = parse_ignore_pattern("notes.txt\t").unwrap();
        assert_eq!(tabbed.parts, vec!["notes.txt".to_owned()]);

        let escaped_tab = parse_ignore_pattern("tab\\\t").unwrap();
        assert!(escaped_tab.matches_path("tab\t", false));
    }

    /// A leading backslash escapes only a literal `#`/`!` (so those characters
    /// can open a real pattern); for any other character the backslash is part
    /// of the glob, so `\*` targets a file literally named `*` instead of
    /// matching everything (regression for the ignore-everything bug).
    #[test]
    fn parse_ignore_pattern_escapes_only_literal_hash_and_bang() {
        let star = parse_ignore_pattern("\\*").unwrap();
        assert_eq!(star.parts, vec!["\\*".to_owned()]);
        assert!(star.matches_path("*", false));
        assert!(!star.matches_path("anything", false));

        let star_md = parse_ignore_pattern("\\*.md").unwrap();
        assert_eq!(star_md.parts, vec!["\\*.md".to_owned()]);
        assert!(star_md.matches_path("*.md", false));
        assert!(!star_md.matches_path("notes.md", false));

        let hash = parse_ignore_pattern("\\#hash").unwrap();
        assert_eq!(hash.parts, vec!["#hash".to_owned()]);
        assert!(hash.matches_path("#hash", false));

        let bang = parse_ignore_pattern("\\!bang").unwrap();
        assert_eq!(bang.parts, vec!["!bang".to_owned()]);
        assert!(!bang.negated);
        assert!(bang.matches_path("!bang", false));
    }

    /// A `.gitignore` line `\*.md` ignores only a file literally named
    /// `*.md`, not every `.md` file (regression: the escaped backslash was
    /// stripped and the pattern became an ignore-everything `*.md`).
    #[test]
    fn filesystem_discovery_escaped_leading_glob_does_not_ignore_everything() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path();
        fs::write(root.join(".gitignore"), "\\*.md\n").unwrap();
        fs::write(root.join("*.md"), "ignored").unwrap();
        fs::write(root.join("normal.md"), "kept").unwrap();

        let discovery = discover_project_sources(root).unwrap();
        assert_eq!(discovery.basis, ProjectSourceBasis::Filesystem);
        let names: Vec<_> = discovery.sources.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["normal.md"]);
    }

    /// The `core.excludesFile` global excludes file applies to the non-Git
    /// filesystem fallback too, not just Git-backed walks. `$GIT_DIR/info/
    /// exclude` is git-specific, but the global file is user-level git
    /// configuration. The test re-runs itself in a child process with a temp
    /// `HOME` (and the system gitconfig disabled) so git resolves its global
    /// config to a hermetic `.gitconfig`.
    #[test]
    fn non_git_filesystem_walk_honors_global_excludes_file() {
        const TEST_NAME: &str = "tests::non_git_filesystem_walk_honors_global_excludes_file";
        // Child mode: the temp HOME is active, so run the real assertions.
        if std::env::var_os("MEMPALACE_TEST_TEMP_HOME").is_some() {
            let tempdir = tempdir().unwrap();
            let root = tempdir.path();
            fs::write(root.join("notes.bak.md"), "ignored\n").unwrap();
            fs::write(root.join("notes.md"), "kept\n").unwrap();

            let discovery = discover_project_sources(root).unwrap();
            assert_eq!(discovery.basis, ProjectSourceBasis::Filesystem);
            let names: Vec<_> =
                discovery.sources.iter().map(|s| s.relative_path.as_str()).collect();
            assert_eq!(names, vec!["notes.md"]);
            return;
        }

        // Parent mode: write a global excludes file and a `.gitconfig` that
        // points at it, then re-run this test under a temp HOME.
        let tempdir = tempdir().unwrap();
        let home = tempdir.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let global = tempdir.path().join("global-excludes");
        fs::write(&global, "*.bak.md\n").unwrap();
        fs::write(
            home.join(".gitconfig"),
            format!("[core]\n\texcludesFile = {}\n", global.display()),
        )
        .unwrap();

        let output = Command::new(std::env::current_exe().unwrap())
            .env("HOME", &home)
            .env("XDG_CONFIG_HOME", home.join(".config"))
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("MEMPALACE_TEST_TEMP_HOME", "1")
            .env_remove("GIT_CONFIG_GLOBAL")
            .args(["--exact", TEST_NAME])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child run of {TEST_NAME} failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    /// A trailing `/**` keeps the directory walkable so the walker can evaluate
    /// a re-inclusion rule for a file inside it — the case that broke before
    /// the trailing-`/**` fix.
    #[test]
    fn filesystem_walk_reincludes_file_under_trailing_doublestar_ignore() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path().to_path_buf();
        fs::write(root.join(".gitignore"), "abc/**\n!abc/keep.md\n").unwrap();
        fs::create_dir_all(root.join("abc")).unwrap();
        fs::write(root.join("abc").join("keep.md"), "kept\n").unwrap();
        fs::write(root.join("abc").join("drop.md"), "ignored\n").unwrap();

        let report = discover_project_files(&root).unwrap();
        let names: Vec<_> = report.files.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["abc/keep.md"]);
    }

    /// Pin this repo's `core.excludesFile` to a path that does not exist so
    /// global excludes are deterministic regardless of the host's git config.
    fn pin_absent_global_excludes(root: &Path) {
        let status = Command::new("git")
            .args(["config", "core.excludesFile", "absent-global-excludes"])
            .current_dir(root)
            .status()
            .unwrap();
        assert!(status.success(), "git config core.excludesFile failed");
    }

    /// `$GIT_DIR/info/exclude` excludes untracked files in a Git-backed
    /// filesystem walk (branch-delta mining), with patterns anchored at the
    /// repository root.
    #[test]
    fn filesystem_walk_honors_git_info_exclude() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path().to_path_buf();
        git_init_with_commit(&root, "main", &[("tracked.md", "tracked\n")]);
        pin_absent_global_excludes(&root);
        fs::create_dir_all(root.join(".git").join("info")).unwrap();
        fs::write(root.join(".git").join("info").join("exclude"), "local.md\n").unwrap();
        fs::write(root.join("local.md"), "ignored\n").unwrap();
        fs::write(root.join("keep.md"), "kept\n").unwrap();

        let report = discover_project_files_with_untracked(&root).unwrap();
        let names: Vec<_> = report.files.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["keep.md", "tracked.md"]);
    }

    /// The `core.excludesFile` global excludes file excludes untracked files
    /// in a Git-backed filesystem walk.
    #[test]
    fn filesystem_walk_honors_core_excludes_file() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path().to_path_buf();
        git_init_with_commit(&root, "main", &[("tracked.md", "tracked\n")]);
        let global = tempdir.path().join("global-excludes");
        fs::write(&global, "*.bak.md\n").unwrap();
        let status = Command::new("git")
            .args(["config", "core.excludesFile", global.to_str().unwrap()])
            .current_dir(&root)
            .status()
            .unwrap();
        assert!(status.success(), "git config core.excludesFile failed");
        fs::write(root.join("notes.bak.md"), "ignored\n").unwrap();
        fs::write(root.join("notes.md"), "kept\n").unwrap();

        let report = discover_project_files_with_untracked(&root).unwrap();
        let names: Vec<_> = report.files.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["notes.md", "tracked.md"]);
    }

    /// Worktree `.gitignore` patterns outrank `$GIT_DIR/info/exclude`: an
    /// `info/exclude` negation cannot re-include a file a `.gitignore`
    /// (higher-precedence) rule excludes.
    #[test]
    fn filesystem_walk_precedence_gitignore_beats_info_exclude() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path().to_path_buf();
        git_init_with_commit(&root, "main", &[("tracked.rs", "fn tracked() {}\n")]);
        pin_absent_global_excludes(&root);
        fs::write(root.join(".gitignore"), "*.md\n").unwrap();
        fs::create_dir_all(root.join(".git").join("info")).unwrap();
        fs::write(root.join(".git").join("info").join("exclude"), "!keep.md\n").unwrap();
        fs::write(root.join("keep.md"), "ignored\n").unwrap();

        let report = discover_project_files_with_untracked(&root).unwrap();
        let names: Vec<_> = report.files.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["tracked.rs"]);
    }

    /// `$GIT_DIR/info/exclude` outranks the global excludes file: an
    /// `info/exclude` negation re-includes a file a global rule excludes.
    #[test]
    fn filesystem_walk_precedence_info_exclude_beats_global() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path().to_path_buf();
        git_init_with_commit(&root, "main", &[("tracked.rs", "fn tracked() {}\n")]);
        let global = tempdir.path().join("global-excludes");
        fs::write(&global, "*.md\n").unwrap();
        let status = Command::new("git")
            .args(["config", "core.excludesFile", global.to_str().unwrap()])
            .current_dir(&root)
            .status()
            .unwrap();
        assert!(status.success(), "git config core.excludesFile failed");
        fs::create_dir_all(root.join(".git").join("info")).unwrap();
        fs::write(root.join(".git").join("info").join("exclude"), "!keep.md\n").unwrap();
        fs::write(root.join("keep.md"), "kept\n").unwrap();
        fs::write(root.join("skip.md"), "ignored\n").unwrap();

        let report = discover_project_files_with_untracked(&root).unwrap();
        let names: Vec<_> = report.files.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["keep.md", "tracked.rs"]);
    }

    /// Git-index discovery never consults repository-level excludes: a tracked
    /// file stays eligible even when `$GIT_DIR/info/exclude` names it, exactly
    /// as git keeps tracked paths tracked.
    #[test]
    fn git_index_discovery_ignores_repo_excludes_for_tracked_files() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path().to_path_buf();
        git_init_with_commit(
            &root,
            "main",
            &[("tracked.md", "tracked\n"), ("docs/notes.md", "notes\n")],
        );
        fs::create_dir_all(root.join(".git").join("info")).unwrap();
        fs::write(root.join(".git").join("info").join("exclude"), "tracked.md\n").unwrap();

        let discovery = discover_project_sources(&root).unwrap();
        assert_eq!(discovery.basis, ProjectSourceBasis::GitIndex);
        let names: Vec<_> = discovery.sources.iter().map(|s| s.relative_path.as_str()).collect();
        assert_eq!(names, vec!["docs/notes.md", "tracked.md"]);
        assert_eq!(discovery.skipped, 0);
    }

    /// The XDG fallback location for global excludes follows git's default
    /// (`$XDG_CONFIG_HOME/git/ignore`, else `~/.config/git/ignore`).
    #[test]
    fn default_global_excludes_path_follows_xdg_then_home() {
        assert_eq!(
            default_global_excludes_path(Some("/custom/xdg"), Some(Path::new("/home/u"))),
            PathBuf::from("/custom/xdg/git/ignore")
        );
        assert_eq!(
            default_global_excludes_path(None, Some(Path::new("/home/u"))),
            PathBuf::from("/home/u/.config/git/ignore")
        );
        assert_eq!(
            default_global_excludes_path(Some(""), Some(Path::new("/home/u"))),
            PathBuf::from("/home/u/.config/git/ignore")
        );
    }

    #[test]
    fn detect_checkout_view_returns_nongit_for_non_repo() {
        let tempdir = tempdir().unwrap();
        let non_git = tempdir.path().join("not_a_repo");
        fs::create_dir(&non_git).unwrap();

        let result = detect_checkout_view(&non_git);
        assert_eq!(result, CheckoutView::NonGit);
    }

    #[test]
    fn detect_checkout_view_detects_canonical_branch_and_unknown_default() {
        let tempdir = tempdir().unwrap();
        let canonical = tempdir.path().join("canonical");
        fs::create_dir(&canonical).unwrap();
        git_init_with_commit(&canonical, "main", &[("file.txt", "contents")]);
        assert_eq!(detect_checkout_view(&canonical), CheckoutView::Canonical);

        let unknown_default = tempdir.path().join("unknown-default");
        fs::create_dir(&unknown_default).unwrap();
        git_init_with_commit(&unknown_default, "trunk", &[("file.txt", "contents")]);
        assert_eq!(detect_checkout_view(&unknown_default), CheckoutView::Canonical);
    }

    #[test]
    fn detect_checkout_view_detects_feature_and_detached_head() {
        let tempdir = tempdir().unwrap();
        let repo = tempdir.path().join("repo");
        fs::create_dir(&repo).unwrap();
        git_init_with_commit(&repo, "main", &[("file.txt", "contents")]);

        let run = |args: &[&str]| {
            let status = Command::new("git").args(args).current_dir(&repo).status().unwrap();
            assert!(status.success(), "git {args:?} failed");
        };
        run(&["checkout", "-b", "feature"]);
        assert!(matches!(
            detect_checkout_view(&repo),
            CheckoutView::Branch {
                view_name,
                base_ref: Some(base_ref),
                merge_base: Some(_),
            } if view_name == "feature" && base_ref == "main"
        ));

        run(&["checkout", "--detach"]);
        assert!(matches!(
            detect_checkout_view(&repo),
            CheckoutView::Branch {
                view_name,
                base_ref: Some(base_ref),
                merge_base: Some(_),
            } if view_name.starts_with("detached-") && base_ref == "main"
        ));
    }

    #[test]
    fn ingest_summary_view_name_defaults_to_none() {
        let summary = IngestSummary::default();
        assert_eq!(summary.view_name, None);
        assert!(summary.secret_path_skips.is_empty());
    }
}

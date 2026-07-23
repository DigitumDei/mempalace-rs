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
use mempalace_storage::{IngestManifestStore, StorageEngine};
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
/// Also see: `.env`-prefix skip in `project_file_skip_by_name`.
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
struct IgnoreRule {
    raw: String,
    kind: IgnoreRuleKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum IgnoreRuleKind {
    Extension(String),
    Directory(String),
    RelativePrefix(String),
    Basename(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IgnoreMatcher {
    rules: Vec<IgnoreRule>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiscoveredSource {
    absolute_path: PathBuf,
    relative_path: String,
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
    let branch_name = request.branch.then(|| {
        resolve_current_branch(&root).unwrap_or_else(|| "detached".to_owned())
    });
    let discovered = discover_project_files(&root)?;
    let routing_fingerprint = project_routing_fingerprint(&config.rooms);

    // Resolve the git commit hash once per mine run (None if not in a repo).
    let commit_hash = resolve_commit_hash(&root);

    // Resolve root as a string for locators (use to_string_lossy for Windows verbatim paths).
    let resolve_root = root.to_string_lossy().into_owned();

    let mut summary = IngestSummary::default();
    summary.discovered_files = discovered.files.len();
    summary.ignored_files = discovered.ignored_files;

    // For branch mode: compute the delta set and filter files.
    let (ingest_kind, delta_set) = if request.branch {
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

    let files: Vec<DiscoveredSource> = apply_limit(discovered_files, request.limit).collect();

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
    if request.branch && !request.dry_run {
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
            if !current_rel_paths.contains(rel) {
                // Only replace if there are committed drawers for this key.
                if let Some(existing) =
                    engine.operational_store().get_ingested_file(&key)?
                {
                    if existing.content_hash != hash_text("removed") {
                        replace_source_drawers(
                            engine,
                            &key,
                            rel,
                            ingest_kind,
                            hash_text("removed"),
                            Vec::new(),
                        )
                        .await?;
                        summary.removed_sources += 1;
                    }
                }
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
) -> Result<Vec<DrawerRecord>> {
    if chunks.is_empty() {
        return Ok(Vec::new());
    }

    // Embed using the real chunk text.
    let embeddings = embed_chunks(provider, &chunks, max_embed_batch_size)?;

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

        drawers.push(DrawerRecord {
            id: drawer_id,
            wing: wing.clone(),
            room: room_id,
            hall: None,
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
    file: &DiscoveredSource,
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

/// Detect the default branch ref: tries `origin/HEAD` symbolic-ref, then
/// literal `main` / `master`.  Returns `None` when neither is found.
fn detect_default_branch(root: &Path) -> Option<String> {
    let root_str = root.to_string_lossy();

    // Try the symbolic ref (e.g. "origin/main").
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

/// Compute the merge-base commit between `default_ref` and HEAD.
fn compute_merge_base(root: &Path, default_ref: &str) -> Option<String> {
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

    let files_to_process: Vec<DiscoveredSource> =
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
fn resolve_current_branch(root: &Path) -> Option<String> {
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

/// Returns `true` if `file_name` should be skipped for secrets / lockfile hygiene.
///
/// Handles exact matches from `PROJECT_SKIP_FILES` and the `.env`-prefix rule
/// (`.env`, `.env.local`, `.env.production`, etc.).
fn project_file_skip_by_name(file_name: &str) -> bool {
    if PROJECT_SKIP_FILES.contains(&file_name) {
        return true;
    }
    // Skip any file whose name starts with ".env" (covers .env, .env.local, …)
    if file_name.starts_with(".env") {
        return true;
    }
    false
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

fn discover_project_files(root: &Path) -> Result<DiscoveryReport> {
    let extension_set = PROJECT_READABLE_EXTENSIONS.iter().copied().collect::<BTreeSet<_>>();
    discover_files(root, true, |path, file_name| {
        // Secrets / lockfile hygiene (includes the .env prefix rule).
        if project_file_skip_by_name(file_name) {
            return false;
        }

        let raw_ext = path.extension().and_then(|value| value.to_str()).unwrap_or_default();
        let has_ext = !raw_ext.is_empty();
        let normalized_suffix = format!(".{}", raw_ext.to_ascii_lowercase());

        let accepted = (has_ext && extension_set.contains(normalized_suffix.as_str()))
            || (!has_ext
                && (PROJECT_READABLE_BASENAMES.contains(&file_name) || has_shebang(path)));

        // Binary sniff: exclude files with a NUL byte in the first 8 KiB even
        // when the extension claims text (misnamed binaries).
        accepted && !looks_binary(path)
    })
}

fn discover_conversation_files(root: &Path) -> Result<DiscoveryReport> {
    let extension_set = CONVO_EXTENSIONS.iter().copied().collect::<BTreeSet<_>>();
    discover_files(root, false, move |path, _file_name| {
        let suffix = path.extension().and_then(|value| value.to_str()).unwrap_or_default();
        let normalized_suffix = format!(".{}", suffix.to_ascii_lowercase());
        extension_set.contains(normalized_suffix.as_str())
    })
}

fn apply_limit(
    files: Vec<DiscoveredSource>,
    limit: Option<usize>,
) -> Box<dyn Iterator<Item = DiscoveredSource>> {
    match limit {
        Some(limit) => Box::new(files.into_iter().take(limit)),
        None => Box::new(files.into_iter()),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiscoveryReport {
    files: Vec<DiscoveredSource>,
    ignored_files: usize,
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

/// Walk `root` applying ignore rules, accepting files for which `accept_file`
/// returns `true`. The closure receives the absolute path and the (lossy) file
/// name; everything it rejects counts toward `ignored_files`.
fn discover_files(
    root: &Path,
    skip_linked_worktrees: bool,
    accept_file: impl Fn(&Path, &str) -> bool,
) -> Result<DiscoveryReport> {
    // Git reports worktree paths as absolute, so use an absolute root when
    // comparing them during project discovery.
    let root = if skip_linked_worktrees {
        root.canonicalize().unwrap_or_else(|_| root.to_path_buf())
    } else {
        root.to_path_buf()
    };
    let ignore_matcher = IgnoreMatcher::load(&root)?;
    let mut ignored_files = 0;
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    // Pre-compute linked worktree paths so we can skip them during the walk.
    let worktree_skip = skip_linked_worktrees
        .then(|| linked_worktree_paths(&root))
        .unwrap_or_default();

    while let Some(dir) = stack.pop() {
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
            if !accept_file(&path, file_name) {
                ignored_files += 1;
                continue;
            }

            files.push(DiscoveredSource { absolute_path: path, relative_path: relative });
        }
    }

    files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(DiscoveryReport { files, ignored_files })
}

impl IgnoreMatcher {
    fn load(root: &Path) -> Result<Self> {
        let mut rules = DEFAULT_SKIP_DIRS
            .iter()
            .map(|entry| IgnoreRule {
                raw: (*entry).to_owned(),
                kind: IgnoreRuleKind::Directory((*entry).to_owned()),
            })
            .collect::<Vec<_>>();

        for file_name in [".gitignore", ".mempalaceignore"] {
            let path = root.join(file_name);
            if !path.exists() {
                continue;
            }

            let body = fs::read_to_string(&path)
                .map_err(|source| IngestError::Io { path: path.clone(), source })?;
            for line in body.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with('!') {
                    continue;
                }
                let kind = if let Some(ext) = trimmed.strip_prefix("*.") {
                    IgnoreRuleKind::Extension(ext.to_ascii_lowercase())
                } else if let Some(dir) = trimmed.strip_suffix('/') {
                    IgnoreRuleKind::Directory(dir.to_owned())
                } else if trimmed.contains('/') {
                    IgnoreRuleKind::RelativePrefix(trimmed.trim_start_matches('/').to_owned())
                } else {
                    IgnoreRuleKind::Basename(trimmed.to_owned())
                };
                rules.push(IgnoreRule { raw: trimmed.to_owned(), kind });
            }
        }

        Ok(Self { rules })
    }

    fn matches(&self, relative_path: &str, is_dir: bool) -> bool {
        let normalized = relative_path.replace('\\', "/");
        let file_name = normalized.rsplit('/').next().unwrap_or(normalized.as_str());
        let parts = normalized.split('/').collect::<Vec<_>>();
        let directory_parts =
            if is_dir { parts.as_slice() } else { &parts[..parts.len().saturating_sub(1)] };

        self.rules.iter().any(|rule| match &rule.kind {
            IgnoreRuleKind::Extension(ext) => {
                !is_dir
                    && file_name
                        .rsplit('.')
                        .next()
                        .is_some_and(|value| value.eq_ignore_ascii_case(ext))
            }
            IgnoreRuleKind::Directory(dir) => directory_parts.iter().any(|part| part == dir),
            IgnoreRuleKind::RelativePrefix(prefix) => {
                normalized == *prefix || normalized.starts_with(&format!("{prefix}/"))
            }
            IgnoreRuleKind::Basename(name) => {
                file_name == name || parts.iter().any(|part| part == name)
            }
        })
    }
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
            None,
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
        let matcher = IgnoreMatcher::load(tempdir.path()).unwrap();
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
            },
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
            },
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
            },
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
            },
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
            },
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
            },
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
            },
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
            },
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
            },
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
            },
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
            },
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
            },
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
            },
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
            },
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
            },
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
            },
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
            },
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
            },
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
            },
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

        // First mine: modify base.rs + add untracked.rs
        let changed_content = "fn base() -> i32 { 99 }\n".repeat(5);
        fs::write(repo_dir.join("base.rs"), &changed_content).unwrap();
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
            },
        )
        .await
        .unwrap();
        assert_eq!(first.ingested_files, 2);
        assert_eq!(first.removed_sources, 0);

        // Revert base.rs to original content (it is no longer in the delta).
        fs::write(repo_dir.join("base.rs"), &base_content).unwrap();

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
            },
        )
        .await
        .unwrap();

        // base.rs is no longer in the delta (reverted); untracked.rs still is.
        assert_eq!(second.removed_sources, 1, "reverted file must be counted as removed");

        // base.rs drawers must be cleared.
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
        let stored = engine.operational_store().get_ingested_file(&sk_base).unwrap();
        // After cleanup the key exists but with zero drawers (replace-with-empty).
        // The content_hash is now hash_text("removed").
        assert!(
            stored.map(|s| s.content_hash == hash_text("removed")).unwrap_or(false),
            "reverted file's stored content_hash must be hash_text(\"removed\")"
        );
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
            },
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
            },
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

        // Also create a sibling directory that is NOT a worktree.
        fs::create_dir_all(root.join("sibling")).unwrap();
        fs::write(root.join("sibling").join("sibling.rs"), "fn sibling() {}\n").unwrap();

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

        // The linked worktree directory itself counts as ignored.
        assert!(
            report.ignored_files >= 1,
            "expected at least 1 ignored file (the linked worktree), got {}",
            report.ignored_files
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
}

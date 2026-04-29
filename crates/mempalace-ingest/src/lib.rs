#![allow(missing_docs)]

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

use blake3::Hasher;
use mempalace_config::{ConfigLoader, ProjectRoomConfig};
use mempalace_core::{DrawerId, DrawerRecord, RoomId, WingId};
use mempalace_embeddings::{EmbeddingProvider, EmbeddingRequest};
use mempalace_storage::core::MempalaceError;
use mempalace_storage::{
    DrawerFilter, DrawerStore, DuplicateStrategy, IngestCommitRequest, IngestManifestStore,
    StorageEngine,
};
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
    ".txt", ".md", ".py", ".js", ".ts", ".jsx", ".tsx", ".json", ".yaml", ".yml", ".html", ".css",
    ".java", ".go", ".rs", ".rb", ".sh", ".csv", ".sql", ".toml",
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
const PROJECT_SKIP_FILES: &[&str] = &[
    "mempalace.yaml",
    "mempalace.yml",
    "mempal.yaml",
    "mempal.yml",
    ".gitignore",
    "package-lock.json",
];

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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectIngestRequest {
    pub project_dir: PathBuf,
    pub wing: Option<String>,
    pub agent: String,
    pub limit: Option<usize>,
    pub dry_run: bool,
    pub max_embed_batch_size: Option<usize>,
}

impl ProjectIngestRequest {
    pub fn new(project_dir: impl AsRef<Path>) -> Self {
        Self {
            project_dir: project_dir.as_ref().to_path_buf(),
            wing: None,
            agent: "mempalace-rs".to_owned(),
            limit: None,
            dry_run: false,
            max_embed_batch_size: None,
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
    let config = ConfigLoader::load_project_config(&root)?;
    let wing_name = request.wing.clone().unwrap_or_else(|| config.wing.clone());
    let wing_id = wing_id(&wing_name)?;
    let discovered = discover_project_files(&root)?;
    let routing_fingerprint = project_routing_fingerprint(&config.rooms);

    let mut summary = IngestSummary::default();
    summary.discovered_files = discovered.files.len();
    summary.ignored_files = discovered.ignored_files;

    let files = apply_limit(discovered.files, request.limit);

    for file in files {
        match read_text_document(&file.absolute_path) {
            Ok(document) => {
                let source_key =
                    source_key("projects", &root, &wing_name, None, &file.relative_path);
                let content_hash =
                    project_ingest_content_hash(&document.content_hash, &routing_fingerprint);
                if let Some(existing) = engine.operational_store().get_ingested_file(&source_key)? {
                    if existing.content_hash == content_hash {
                        summary.skipped_unchanged += 1;
                        continue;
                    }
                }

                if document.content.trim().len() < PROJECT_MIN_CHUNK_SIZE {
                    if !request.dry_run {
                        replace_source_drawers(
                            engine,
                            &source_key,
                            &file.relative_path,
                            "projects",
                            content_hash,
                            Vec::new(),
                        )
                        .await?;
                    }
                    summary.ingested_files += 1;
                    summary.truncated_files += usize::from(document.truncated);
                    continue;
                }

                let room = detect_project_room(
                    Path::new(&file.relative_path),
                    &document.content,
                    &config.rooms,
                );
                let chunks = chunk_project_text(&document.content);
                if chunks.is_empty() {
                    if !request.dry_run {
                        replace_source_drawers(
                            engine,
                            &source_key,
                            &file.relative_path,
                            "projects",
                            content_hash,
                            Vec::new(),
                        )
                        .await?;
                    }
                    summary.ingested_files += 1;
                    summary.truncated_files += usize::from(document.truncated);
                    continue;
                }

                let source_drawers = build_drawers(
                    provider,
                    &wing_id,
                    &source_key,
                    &file.relative_path,
                    "projects",
                    None,
                    &request.agent,
                    request.max_embed_batch_size,
                    chunks
                        .into_iter()
                        .map(|chunk| Chunk {
                            content: chunk.content,
                            chunk_index: chunk.chunk_index,
                            room_hint: Some(room.clone()),
                            date_hint: None,
                        })
                        .collect::<Vec<_>>(),
                )?;
                let drawer_count = source_drawers.len();

                if !request.dry_run {
                    replace_source_drawers(
                        engine,
                        &source_key,
                        &file.relative_path,
                        "projects",
                        content_hash,
                        source_drawers,
                    )
                    .await?;
                }

                summary.ingested_files += 1;
                summary.drawers_written += drawer_count;
                summary.truncated_files += usize::from(document.truncated);
            }
            Err(IngestError::Io { .. }) => {
                summary.unreadable_files += 1;
            }
            Err(error) => return Err(error),
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
        if let Some(existing) = engine.operational_store().get_ingested_file(&source_key)? {
            if existing.content_hash == content_hash {
                summary.skipped_unchanged += 1;
                continue;
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
) -> Result<Vec<DrawerRecord>> {
    if chunks.is_empty() {
        return Ok(Vec::new());
    }

    let embeddings = embed_chunks(provider, &chunks, max_embed_batch_size)?;

    let mut drawers = Vec::with_capacity(chunks.len());
    for (chunk, embedding) in chunks.into_iter().zip(embeddings.into_iter()) {
        let room_name = chunk.room_hint.unwrap_or_else(|| "general".to_owned());
        let room_id = room_id(&room_name)?;
        let drawer_id = drawer_id(wing, &room_id, source_key, chunk.chunk_index)?;
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
            content_hash: hash_text(&chunk.content),
            content: chunk.content,
            embedding,
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
    let existing = engine.operational_store().committed_drawer_ids_for_source_key(source_key)?;
    let new_ids = drawers.iter().map(|drawer| drawer.id.clone()).collect::<BTreeSet<_>>();

    engine
        .commit_ingest(IngestCommitRequest {
            ingest_kind: ingest_kind.to_owned(),
            source_key: source_key.to_owned(),
            source_file: source_file.to_owned(),
            content_hash,
            drawers,
            duplicate_strategy: DuplicateStrategy::Overwrite,
        })
        .await?;

    let stale = existing.into_iter().filter(|id| !new_ids.contains(id)).collect::<Vec<_>>();
    if !stale.is_empty() {
        engine.drawer_store().delete_drawers(&stale).await?;
    }
    Ok(())
}

fn discover_project_files(root: &Path) -> Result<DiscoveryReport> {
    discover_files(root, PROJECT_READABLE_EXTENSIONS, true)
}

fn discover_conversation_files(root: &Path) -> Result<DiscoveryReport> {
    discover_files(root, CONVO_EXTENSIONS, false)
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

fn discover_files(
    root: &Path,
    extensions: &[&str],
    skip_project_config: bool,
) -> Result<DiscoveryReport> {
    let ignore_matcher = IgnoreMatcher::load(root)?;
    let extension_set = extensions.iter().copied().collect::<BTreeSet<_>>();
    let mut ignored_files = 0;
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];

    while let Some(dir) = stack.pop() {
        let read_dir =
            fs::read_dir(&dir).map_err(|source| IngestError::Io { path: dir.clone(), source })?;
        for entry in read_dir {
            let entry = entry.map_err(|source| IngestError::Io { path: dir.clone(), source })?;
            let path = entry.path();
            let file_type = entry
                .file_type()
                .map_err(|source| IngestError::Io { path: path.clone(), source })?;
            let relative = relative_path(root, &path)?;
            if file_type.is_dir() {
                if ignore_matcher.matches(&relative, true) {
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
            if skip_project_config && PROJECT_SKIP_FILES.contains(&file_name) {
                ignored_files += 1;
                continue;
            }

            let suffix = path.extension().and_then(|value| value.to_str()).unwrap_or_default();
            let normalized_suffix = format!(".{}", suffix.to_ascii_lowercase());
            if !extension_set.contains(normalized_suffix.as_str()) {
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
}

fn read_text_document(path: &Path) -> Result<TextDocument> {
    let bytes =
        fs::read(path).map_err(|source| IngestError::Io { path: path.to_path_buf(), source })?;
    let content_hash = hash_bytes(&bytes);
    let truncated = bytes.len() > LARGE_FILE_TRUNCATION_BYTES;
    let effective =
        if truncated { &bytes[..LARGE_FILE_TRUNCATION_BYTES] } else { bytes.as_slice() };
    let content = String::from_utf8_lossy(effective).trim().to_owned();
    Ok(TextDocument { content, content_hash, truncated })
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

fn chunk_project_text(content: &str) -> Vec<Chunk> {
    let content = content.trim();
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

        let chunk = content[start..end].trim();
        if chunk.len() >= PROJECT_MIN_CHUNK_SIZE {
            chunks.push(Chunk {
                content: chunk.to_owned(),
                chunk_index: u32::try_from(chunks.len()).unwrap_or(u32::MAX),
                room_hint: None,
                date_hint: None,
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
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize().to_hex().to_string()
}

fn hash_text(text: &str) -> String {
    hash_bytes(text.as_bytes())
}

fn source_key(
    ingest_kind: &str,
    root: &Path,
    wing: &str,
    extract_mode: Option<&str>,
    relative_path: &str,
) -> String {
    let root_key = hash_text(&root.to_string_lossy());
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
    WingId::new(canonicalize_label(value)).map_err(|err| IngestError::Core(err.into()))
}

fn room_id(value: &str) -> Result<RoomId> {
    RoomId::new(canonicalize_label(value)).map_err(|err| IngestError::Core(err.into()))
}

fn drawer_id(wing: &WingId, room: &RoomId, source_key: &str, chunk_index: u32) -> Result<DrawerId> {
    let source_hash = &hash_text(source_key)[..12];
    DrawerId::new(format!("{}/{}/{}-{:04}", wing.as_str(), room.as_str(), source_hash, chunk_index))
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
    use serde_json::json;
    use tempfile::tempdir;

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
                },
                Chunk {
                    content: "beta".to_owned(),
                    chunk_index: 1,
                    room_hint: Some("general".to_owned()),
                    date_hint: None,
                },
                Chunk {
                    content: "gamma".to_owned(),
                    chunk_index: 2,
                    room_hint: Some("general".to_owned()),
                    date_hint: None,
                },
                Chunk {
                    content: "delta".to_owned(),
                    chunk_index: 3,
                    room_hint: Some("general".to_owned()),
                    date_hint: None,
                },
                Chunk {
                    content: "epsilon".to_owned(),
                    chunk_index: 4,
                    room_hint: Some("general".to_owned()),
                    date_hint: None,
                },
            ],
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
            .join("../../../tests/fixtures/phase0/inputs/project_alpha");
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
                max_embed_batch_size: None,
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
        let fixture_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../tests/fixtures/phase0/inputs/convos");
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
                max_embed_batch_size: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(summary_general.ingested_files, 1);
        let decisions = engine
            .drawer_store()
            .list_drawers(&DrawerFilter {
                wing: Some(WingId::new("phase0_convos").unwrap()),
                room: Some(RoomId::new("decision").unwrap()),
                ..DrawerFilter::default()
            })
            .await
            .unwrap();
        let exchange = engine
            .drawer_store()
            .list_drawers(&DrawerFilter {
                wing: Some(WingId::new("phase0_convos").unwrap()),
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
                max_embed_batch_size: None,
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
                max_embed_batch_size: None,
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
                max_embed_batch_size: None,
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
        assert!(drawers[0].content.contains("changed auth"));
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
                max_embed_batch_size: None,
            },
        )
        .await
        .unwrap();

        let drawers = engine
            .drawer_store()
            .list_drawers(&DrawerFilter {
                wing: Some(WingId::new("overridewing").unwrap()),
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
                max_embed_batch_size: None,
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
                max_embed_batch_size: None,
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
                max_embed_batch_size: None,
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
                max_embed_batch_size: None,
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
                max_embed_batch_size: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(summary.malformed_files, 1);
        assert_eq!(summary.ingested_files, 1);
    }
}

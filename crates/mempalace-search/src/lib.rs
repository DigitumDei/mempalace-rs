#![allow(missing_docs)]

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

pub use mempalace_core as core;
pub use mempalace_dialect as dialect;
pub use mempalace_embeddings as embeddings;
pub use mempalace_storage as storage;

use mempalace_core::{DrawerRecord, SearchQuery, SearchResult};
use mempalace_dialect::{Dialect, WakeUpAaaKConfig};
use mempalace_embeddings::{EmbeddingProvider, EmbeddingRequest};
use mempalace_storage::{DrawerFilter, DrawerMatch, DrawerStore, SearchRequest};
use thiserror::Error;

const DEFAULT_LAYER1_MAX_DRAWERS: usize = 15;
const DEFAULT_LAYER1_MAX_CHARS: usize = 3_200;
const LAYER1_SNIPPET_LIMIT: usize = 200;
const LAYER2_SNIPPET_LIMIT: usize = 300;

pub type Result<T> = std::result::Result<T, SearchError>;

#[derive(Debug, Error)]
pub enum SearchError {
    #[error(transparent)]
    Core(#[from] mempalace_core::MempalaceError),
    #[error(transparent)]
    Embeddings(#[from] mempalace_embeddings::EmbeddingError),
    #[error(transparent)]
    Storage(#[from] mempalace_storage::StorageError),
    #[error(
        "search query requested embedding profile `{query}`, but runtime is configured for `{provider}`"
    )]
    ProfileMismatch { query: &'static str, provider: &'static str },
    #[error("search query text cannot be blank")]
    BlankQuery,
    #[error("failed to read identity at {path}: {source}")]
    IdentityRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layer1Config {
    pub max_drawers: usize,
    pub max_chars: usize,
}

impl Default for Layer1Config {
    fn default() -> Self {
        Self { max_drawers: DEFAULT_LAYER1_MAX_DRAWERS, max_chars: DEFAULT_LAYER1_MAX_CHARS }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WakeUpRequest {
    pub wing: Option<mempalace_core::WingId>,
    pub identity: IdentitySource,
    pub layer1: Layer1Config,
    pub format: WakeUpFormat,
}

impl Default for WakeUpRequest {
    fn default() -> Self {
        Self {
            wing: None,
            identity: default_identity_path()
                .map(IdentitySource::DefaultPath)
                .unwrap_or(IdentitySource::MissingDefault),
            layer1: Layer1Config::default(),
            format: WakeUpFormat::PlainText,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WakeUpFormat {
    #[default]
    PlainText,
    AaaK,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentitySource {
    Inline(String),
    Path(PathBuf),
    DefaultPath(PathBuf),
    MissingDefault,
}

impl IdentitySource {
    pub fn render(&self) -> Result<String> {
        match self {
            Self::Inline(value) => Ok(value.trim().to_owned()),
            Self::Path(path) => fs::read_to_string(path)
                .map(|text| text.trim().to_owned())
                .map_err(|source| SearchError::IdentityRead { path: path.clone(), source }),
            Self::DefaultPath(path) => match fs::read_to_string(path) {
                Ok(text) => Ok(text.trim().to_owned()),
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                    Ok(default_identity_banner())
                }
                Err(source) => Err(SearchError::IdentityRead { path: path.clone(), source }),
            },
            Self::MissingDefault => Ok(default_identity_banner()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LayerRetrieveRequest {
    pub wing: Option<mempalace_core::WingId>,
    pub room: Option<mempalace_core::RoomId>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchRuntime<P> {
    provider: P,
    dialect: Dialect,
    policy: SearchRuntimePolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SearchRuntimePolicy {
    pub rerank_enabled: bool,
}

impl<P> SearchRuntime<P>
where
    P: EmbeddingProvider,
{
    pub fn new(provider: P) -> Self {
        Self::with_policy(provider, SearchRuntimePolicy::default())
    }

    pub fn with_policy(provider: P, policy: SearchRuntimePolicy) -> Self {
        Self::with_dialect_and_policy(provider, Dialect::new(), policy)
    }

    pub fn with_dialect(provider: P, dialect: Dialect) -> Self {
        Self::with_dialect_and_policy(provider, dialect, SearchRuntimePolicy::default())
    }

    pub fn with_dialect_and_policy(
        provider: P,
        dialect: Dialect,
        policy: SearchRuntimePolicy,
    ) -> Self {
        Self { provider, dialect, policy }
    }

    pub fn provider(&self) -> &P {
        &self.provider
    }

    pub fn provider_mut(&mut self) -> &mut P {
        &mut self.provider
    }

    pub fn dialect(&self) -> &Dialect {
        &self.dialect
    }

    pub async fn search<S>(&mut self, store: &S, query: &SearchQuery) -> Result<Vec<SearchResult>>
    where
        S: DrawerStore,
    {
        self.search_with_rerank(store, query, self.policy.rerank_enabled).await
    }

    pub async fn search_semantic<S>(
        &mut self,
        store: &S,
        query: &SearchQuery,
    ) -> Result<Vec<SearchResult>>
    where
        S: DrawerStore,
    {
        self.search_with_rerank(store, query, false).await
    }

    async fn search_with_rerank<S>(
        &mut self,
        store: &S,
        query: &SearchQuery,
        rerank_enabled: bool,
    ) -> Result<Vec<SearchResult>>
    where
        S: DrawerStore,
    {
        let provider_profile = self.provider.profile().profile;
        if provider_profile != query.profile {
            return Err(SearchError::ProfileMismatch {
                query: query.profile.as_str(),
                provider: provider_profile.as_str(),
            });
        }

        if query.text.trim().is_empty() {
            return Err(SearchError::BlankQuery);
        }

        if query.limit == 0 {
            return Ok(Vec::new());
        }

        let request = EmbeddingRequest::new(vec![query.text.clone()])?;
        let response = self.provider.embed(&request)?;
        let query_embedding = response.vectors().first().cloned().ok_or_else(|| {
            SearchError::Embeddings(mempalace_embeddings::EmbeddingError::ProviderContract(
                "provider returned no vector for a non-empty search query".to_owned(),
            ))
        })?;
        let filter = DrawerFilter {
            wing: query.wing.clone(),
            room: query.room.clone(),
            ..DrawerFilter::default()
        };
        let candidate_limit =
            if rerank_enabled { query.limit.saturating_mul(2) } else { query.limit };
        let matches = store
            .search_drawers(&SearchRequest {
                embedding: query_embedding,
                limit: candidate_limit,
                include_cutoff_ties: true,
                filter,
            })
            .await?;

        let ranked = if rerank_enabled {
            rerank_matches(&query.text, matches, query.limit)
        } else {
            rank_matches(matches, query.limit)
        };

        Ok(ranked
            .into_iter()
            .map(|entry| SearchResult {
                drawer_id: Some(entry.record.id.clone()),
                wing: entry.record.wing.clone(),
                room: entry.record.room.clone(),
                score: entry.score,
                content: entry.record.content.clone(),
                source_file: source_label(&entry.record.source_file).to_owned(),
            })
            .collect())
    }

    pub async fn search_text<S>(&mut self, store: &S, query: &SearchQuery) -> Result<String>
    where
        S: DrawerStore,
    {
        let results = self.search(store, query).await?;
        Ok(render_search_results(&query.text, &results, query.wing.as_ref(), query.room.as_ref()))
    }

    pub async fn recall<S>(&self, store: &S, request: &LayerRetrieveRequest) -> Result<String>
    where
        S: DrawerStore,
    {
        let mut drawers = store
            .list_drawers(&DrawerFilter {
                wing: request.wing.clone(),
                room: request.room.clone(),
                ..DrawerFilter::default()
            })
            .await?;

        order_layer_drawers(&mut drawers);

        if drawers.is_empty() {
            let mut label = String::new();
            if let Some(wing) = &request.wing {
                label.push_str("wing=");
                label.push_str(wing.as_str());
            }
            if let Some(room) = &request.room {
                if !label.is_empty() {
                    label.push(' ');
                }
                label.push_str("room=");
                label.push_str(room.as_str());
            }

            return Ok(if label.is_empty() {
                "No drawers found.".to_owned()
            } else {
                format!("No drawers found for {label}.")
            });
        }

        let mut lines =
            vec![format!("## L2 — ON-DEMAND ({}) drawers", drawers.len().min(request.limit))];
        for record in drawers.iter().take(request.limit) {
            let snippet = flatten_and_truncate(&record.content, LAYER2_SNIPPET_LIMIT);
            let mut entry = format!("  [{}] {}", record.room.as_str(), snippet);
            let source = source_label(&record.source_file);
            if !source.is_empty() {
                entry.push_str("  (");
                entry.push_str(&source);
                entry.push(')');
            }
            lines.push(entry);
        }

        Ok(lines.join("\n"))
    }

    pub async fn wake_up<S>(&self, store: &S, request: &WakeUpRequest) -> Result<String>
    where
        S: DrawerStore,
    {
        let identity = request.identity.render()?;
        match request.format {
            WakeUpFormat::PlainText => {
                let story =
                    generate_layer1(store, request.wing.clone(), request.layer1.clone()).await?;
                Ok(format!("{identity}\n\n{story}"))
            }
            WakeUpFormat::AaaK => {
                let drawers = list_layer_drawers(store, request.wing.clone()).await?;
                Ok(self.dialect.render_wake_up_aaak(
                    &identity,
                    &drawers,
                    &WakeUpAaaKConfig {
                        max_drawers: request.layer1.max_drawers,
                        max_chars: request.layer1.max_chars,
                    },
                ))
            }
        }
    }
}

fn default_identity_path() -> Option<PathBuf> {
    default_identity_path_from_home(env::var_os("HOME").map(PathBuf::from))
}

fn default_identity_path_from_home(home: Option<PathBuf>) -> Option<PathBuf> {
    home.map(|home| home.join(".mempalace").join("identity.txt"))
}

fn default_identity_banner() -> String {
    "## L0 — IDENTITY\nNo identity configured. Create ~/.mempalace/identity.txt".to_owned()
}

pub fn render_search_results(
    query: &str,
    results: &[SearchResult],
    wing: Option<&mempalace_core::WingId>,
    room: Option<&mempalace_core::RoomId>,
) -> String {
    if results.is_empty() {
        return format!("\n  No results found for: \"{query}\"");
    }

    let mut lines = vec![
        String::new(),
        "============================================================".to_owned(),
        format!("  Results for: \"{query}\""),
    ];

    if let Some(wing) = wing {
        lines.push(format!("  Wing: {}", wing.as_str()));
    }
    if let Some(room) = room {
        lines.push(format!("  Room: {}", room.as_str()));
    }

    lines.push("============================================================".to_owned());
    lines.push(String::new());

    for (index, result) in results.iter().enumerate() {
        lines.push(format!(
            "  [{}] {} / {}",
            index + 1,
            result.wing.as_str(),
            result.room.as_str()
        ));
        lines.push(format!("      Source: {}", result.source_file));
        lines.push(format!("      Match:  {}", trim_similarity(result.score)));
        lines.push(String::new());

        for line in result.content.trim().lines() {
            lines.push(format!("      {line}"));
        }

        lines.push(String::new());
        lines.push("  ────────────────────────────────────────────────────────".to_owned());
    }

    lines.push(String::new());
    lines.join("\n")
}

pub async fn generate_layer1<S>(
    store: &S,
    wing: Option<mempalace_core::WingId>,
    config: Layer1Config,
) -> Result<String>
where
    S: DrawerStore,
{
    let mut drawers = list_layer_drawers(store, wing).await?;

    if drawers.is_empty() {
        return Ok("## L1 — No memories yet.".to_owned());
    }

    order_layer_drawers(&mut drawers);
    let top = drawers.into_iter().take(config.max_drawers).collect::<Vec<_>>();

    let mut grouped = BTreeMap::<String, Vec<DrawerRecord>>::new();
    for record in top {
        grouped.entry(record.room.as_str().to_owned()).or_default().push(record);
    }

    let mut lines = vec!["## L1 — ESSENTIAL STORY".to_owned()];
    let mut total_chars = 0;

    for (room, records) in grouped {
        let room_line = format!("\n[{room}]");
        let room_chars = char_count(&room_line);
        let mut room_lines = Vec::new();
        let mut room_has_entries = false;

        for record in records {
            let snippet = flatten_and_truncate(&record.content, LAYER1_SNIPPET_LIMIT);
            let source = source_label(&record.source_file);

            let mut entry = format!("  - {snippet}");
            if !source.is_empty() {
                entry.push_str("  (");
                entry.push_str(&source);
                entry.push(')');
            }

            let entry_chars = char_count(&entry);
            let next_total =
                total_chars + if room_has_entries { 0 } else { room_chars } + entry_chars;
            if next_total > config.max_chars {
                lines.extend(room_lines);
                lines.push("  ... (more in L3 search)".to_owned());
                return Ok(lines.join("\n"));
            }

            if !room_has_entries {
                lines.push(room_line.clone());
                total_chars += room_chars;
                room_has_entries = true;
            }

            total_chars += entry_chars;
            room_lines.push(entry);
        }

        lines.extend(room_lines);
    }

    Ok(lines.join("\n"))
}

async fn list_layer_drawers<S>(
    store: &S,
    wing: Option<mempalace_core::WingId>,
) -> Result<Vec<DrawerRecord>>
where
    S: DrawerStore,
{
    Ok(store.list_drawers(&DrawerFilter { wing, ..DrawerFilter::default() }).await?)
}

fn rank_matches(matches: Vec<DrawerMatch>, limit: usize) -> Vec<RankedMatch> {
    let mut ranked = matches
        .into_iter()
        .map(|matched| RankedMatch {
            score: normalize_score(matched.distance),
            distance: matched.distance,
            record: matched.record,
        })
        .collect::<Vec<_>>();

    ranked.sort_by(compare_ranked_matches);
    ranked.truncate(limit);
    ranked
}

fn rerank_matches(query: &str, matches: Vec<DrawerMatch>, limit: usize) -> Vec<RankedMatch> {
    let query_terms = normalized_terms(query);
    let mut ranked = matches
        .into_iter()
        .map(|matched| {
            let base_score = normalize_score(matched.distance);
            let lexical_score = lexical_overlap_score(&query_terms, &matched.record.content);
            RankedMatch {
                score: (base_score * 0.7) + (lexical_score * 0.3),
                distance: matched.distance,
                record: matched.record,
            }
        })
        .collect::<Vec<_>>();

    ranked.sort_by(compare_ranked_matches);
    ranked.truncate(limit);
    ranked
}

fn compare_ranked_matches(left: &RankedMatch, right: &RankedMatch) -> Ordering {
    right
        .score
        .partial_cmp(&left.score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| compare_distance(left.distance, right.distance))
        .then_with(|| left.record.wing.as_str().cmp(right.record.wing.as_str()))
        .then_with(|| left.record.room.as_str().cmp(right.record.room.as_str()))
        .then_with(|| {
            source_label(&left.record.source_file).cmp(&source_label(&right.record.source_file))
        })
        .then_with(|| left.record.chunk_index.cmp(&right.record.chunk_index))
        .then_with(|| left.record.id.as_str().cmp(right.record.id.as_str()))
}

fn compare_distance(left: Option<f32>, right: Option<f32>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left.partial_cmp(&right).unwrap_or(Ordering::Equal),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn order_layer_drawers(drawers: &mut [DrawerRecord]) {
    drawers.sort_by(|left, right| {
        right
            .layer_weight()
            .partial_cmp(&left.layer_weight())
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.room.as_str().cmp(right.room.as_str()))
            .then_with(|| compare_option_dates(right.date, left.date))
            .then_with(|| right.filed_at.cmp(&left.filed_at))
            .then_with(|| source_label(&left.source_file).cmp(&source_label(&right.source_file)))
            .then_with(|| left.chunk_index.cmp(&right.chunk_index))
            .then_with(|| left.id.as_str().cmp(right.id.as_str()))
    });
}

fn compare_option_dates(left: Option<time::Date>, right: Option<time::Date>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => Ordering::Greater,
        (None, Some(_)) => Ordering::Less,
        (None, None) => Ordering::Equal,
    }
}

fn normalize_score(distance: Option<f32>) -> f32 {
    distance.map_or(0.0, |value| 1.0 - value)
}

fn trim_similarity(score: f32) -> String {
    let rounded = (score * 1_000.0).round() / 1_000.0;
    let formatted = format!("{rounded:.3}");
    let trimmed = formatted.trim_end_matches('0').trim_end_matches('.');
    if trimmed.is_empty() { "0".to_owned() } else { trimmed.to_owned() }
}

fn flatten_and_truncate(content: &str, limit: usize) -> String {
    let flattened = content.split_whitespace().collect::<Vec<_>>().join(" ");

    if flattened.chars().count() <= limit {
        flattened
    } else {
        flattened.chars().take(limit.saturating_sub(3)).collect::<String>() + "..."
    }
}

fn lexical_overlap_score(query_terms: &[String], content: &str) -> f32 {
    if query_terms.is_empty() {
        return 0.0;
    }

    let content_terms = normalized_terms(content);
    let matched = query_terms
        .iter()
        .filter(|term| content_terms.iter().any(|content_term| content_term == *term))
        .count();

    matched as f32 / query_terms.len() as f32
}

fn normalized_terms(value: &str) -> Vec<String> {
    value
        .split(|c: char| !c.is_alphanumeric())
        .filter(|term| !term.is_empty())
        .map(|term| term.chars().flat_map(char::to_lowercase).collect())
        .collect()
}

fn char_count(value: &str) -> usize {
    value.chars().count()
}

fn source_label(source_file: &str) -> &str {
    Path::new(source_file).file_name().and_then(|value| value.to_str()).unwrap_or(source_file)
}

trait LayerWeight {
    fn layer_weight(&self) -> f32;
}

impl LayerWeight for DrawerRecord {
    fn layer_weight(&self) -> f32 {
        self.importance.or(self.emotional_weight).or(self.weight).unwrap_or(3.0)
    }
}

#[derive(Debug, Clone, PartialEq)]
struct RankedMatch {
    score: f32,
    distance: Option<f32>,
    record: DrawerRecord,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        Dialect, IdentitySource, Layer1Config, LayerRetrieveRequest, SearchError, SearchRuntime,
        SearchRuntimePolicy, WakeUpFormat, WakeUpRequest, default_identity_path,
        default_identity_path_from_home, generate_layer1, lexical_overlap_score,
        normalized_terms, render_search_results, trim_similarity,
    };
    use async_trait::async_trait;
    use mempalace_core::{DrawerId, DrawerRecord, EmbeddingProfile, RoomId, SearchQuery, WingId};
    use mempalace_embeddings::{
        EmbeddingProvider, EmbeddingRequest, EmbeddingResponse, StartupValidation,
        StartupValidationStatus,
    };
    use mempalace_storage::{
        DrawerFilter, DrawerMatch, DrawerStore, DuplicateStrategy, SearchRequest, StorageError,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::{SystemTime, UNIX_EPOCH};
    use time::macros::{date, datetime};

    fn embedding(value: f32) -> Vec<f32> {
        vec![value; EmbeddingProfile::Balanced.metadata().dimensions]
    }

    fn record(
        id: &str,
        wing: &str,
        room: &str,
        source_file: &str,
        content: &str,
        score: Option<f32>,
        filed_at: time::OffsetDateTime,
    ) -> DrawerRecord {
        DrawerRecord {
            id: DrawerId::new(id).unwrap(),
            wing: WingId::new(wing).unwrap(),
            room: RoomId::new(room).unwrap(),
            hall: Some("facts".to_owned()),
            date: Some(date!(2026 - 04 - 11)),
            source_file: source_file.to_owned(),
            chunk_index: 0,
            ingest_mode: "projects".to_owned(),
            extract_mode: Some("full".to_owned()),
            added_by: "tester".to_owned(),
            filed_at,
            importance: score,
            emotional_weight: None,
            weight: None,
            content: content.to_owned(),
            content_hash: format!("hash-{id}"),
            embedding: embedding(score.unwrap_or(0.0)),
        }
    }

    #[derive(Debug, Clone)]
    struct StubProvider {
        response: Vec<Vec<f32>>,
    }

    impl EmbeddingProvider for StubProvider {
        fn profile(&self) -> &'static mempalace_core::EmbeddingProfileMetadata {
            EmbeddingProfile::Balanced.metadata()
        }

        fn startup_validation(&self) -> mempalace_embeddings::Result<StartupValidation> {
            Ok(StartupValidation {
                status: StartupValidationStatus::Ready,
                cache_root: PathBuf::from("/tmp"),
                model_id: EmbeddingProfile::Balanced.metadata().model_id,
                detail: "ok".to_owned(),
            })
        }

        fn embed(
            &mut self,
            request: &EmbeddingRequest,
        ) -> mempalace_embeddings::Result<EmbeddingResponse> {
            let vectors = self.response.iter().take(request.len()).cloned().collect::<Vec<_>>();
            EmbeddingResponse::from_vectors(
                vectors,
                EmbeddingProfile::Balanced.metadata().dimensions,
                EmbeddingProfile::Balanced,
                EmbeddingProfile::Balanced.metadata().model_id,
            )
        }
    }

    #[derive(Debug, Clone)]
    struct StubStore {
        drawers: Vec<DrawerRecord>,
    }

    #[async_trait]
    impl DrawerStore for StubStore {
        async fn ensure_schema(&self) -> Result<(), StorageError> {
            Ok(())
        }

        async fn put_drawers(
            &self,
            _drawers: &[DrawerRecord],
            _strategy: DuplicateStrategy,
        ) -> Result<(), StorageError> {
            unreachable!("not used in phase 5 tests")
        }

        async fn get_drawer(&self, _id: &DrawerId) -> Result<Option<DrawerRecord>, StorageError> {
            unreachable!("not used in phase 5 tests")
        }

        async fn delete_drawers(&self, _ids: &[DrawerId]) -> Result<usize, StorageError> {
            unreachable!("not used in phase 5 tests")
        }

        async fn search_drawers(
            &self,
            request: &SearchRequest,
        ) -> Result<Vec<DrawerMatch>, StorageError> {
            let mut filtered = self
                .drawers
                .iter()
                .filter(|drawer| filter_matches(drawer, &request.filter))
                .cloned()
                .map(|drawer| DrawerMatch {
                    distance: Some((drawer.embedding[0] - request.embedding[0]).abs()),
                    record: drawer,
                })
                .collect::<Vec<_>>();

            filtered.sort_by(|left, right| {
                left.distance.partial_cmp(&right.distance).unwrap_or(std::cmp::Ordering::Equal)
            });
            if request.limit == 0 {
                filtered.clear();
            } else if request.include_cutoff_ties && filtered.len() > request.limit {
                let cutoff = filtered[request.limit - 1].distance;
                let cutoff_len = filtered.partition_point(|entry| entry.distance <= cutoff);
                filtered.truncate(cutoff_len);
            } else {
                filtered.truncate(request.limit);
            }
            Ok(filtered)
        }

        async fn list_drawers(
            &self,
            filter: &DrawerFilter,
        ) -> Result<Vec<DrawerRecord>, StorageError> {
            Ok(self
                .drawers
                .iter()
                .filter(|drawer| filter_matches(drawer, filter))
                .cloned()
                .collect())
        }
    }

    fn filter_matches(drawer: &DrawerRecord, filter: &DrawerFilter) -> bool {
        (filter.ids.is_empty() || filter.ids.iter().any(|id| id == &drawer.id))
            && filter.wing.as_ref().is_none_or(|wing| wing == &drawer.wing)
            && filter.room.as_ref().is_none_or(|room| room == &drawer.room)
            && filter.hall.as_ref().is_none_or(|hall| drawer.hall.as_ref() == Some(hall))
            && filter.source_file.as_ref().is_none_or(|source| source == &drawer.source_file)
    }

    fn sample_store() -> StubStore {
        StubStore {
            drawers: vec![
                record(
                    "wing_team/auth-migration/0001",
                    "wing_team",
                    "auth-migration",
                    "fixtures/team.txt",
                    "The team decided the auth-migration must preserve CLI and MCP parity.",
                    Some(0.49),
                    datetime!(2026-04-11 09:00:00 UTC),
                ),
                record(
                    "wing_code/auth-migration/0001",
                    "wing_code",
                    "auth-migration",
                    "fixtures/code.txt",
                    "Code notes: auth-migration keeps search filter semantics exact while storage changes underneath.",
                    Some(0.069),
                    datetime!(2026-04-11 08:00:00 UTC),
                ),
                record(
                    "project_alpha/backend/0001",
                    "project_alpha",
                    "backend",
                    "project_alpha/backend/auth.py",
                    "def issue_session(user_id: str) -> str:\n    \"\"\"\n    We switched from opaque session blobs to signed session tokens because the\n    old format made auth debugging painful during the Rust migration work.\n    \"\"\"\n    if not user_id:\n        raise ValueError(\"user_id is required\")\n\n    token = f\"session:{user_id}:signed\"\n    return token\n\n\ndef refresh_token(token: str) -> str:\n    \"\"\"\n    The auth migration plan keeps refresh logic local-first and deterministic.\n    We chose signed tokens over a database-backed session lookup because the\n    CLI and MCP tools need predictable offline behavior.\n    \"\"\"\n    if not token.startswith(\"session:\"):\n        raise ValueError(\"invalid token format\")\n    return token + \":refreshed\"",
                    Some(-0.267),
                    datetime!(2026-04-11 07:00:00 UTC),
                ),
                record(
                    "wing_team/phase0-rollout/0001",
                    "wing_team",
                    "phase0-rollout",
                    "fixtures/rollout.txt",
                    "Phase 0 rollout stays on the team wing so graph traversal captures connected_via semantics.",
                    Some(-0.848),
                    datetime!(2026-04-10 07:00:00 UTC),
                ),
            ],
        }
    }

    #[derive(Debug, Clone)]
    struct SearchSpyStore {
        drawers: Vec<DrawerRecord>,
        list_calls: Arc<Mutex<usize>>,
        search_limits: Arc<Mutex<Vec<usize>>>,
        include_cutoff_ties: Arc<Mutex<Vec<bool>>>,
    }

    #[async_trait]
    impl DrawerStore for SearchSpyStore {
        async fn ensure_schema(&self) -> Result<(), StorageError> {
            Ok(())
        }

        async fn put_drawers(
            &self,
            _drawers: &[DrawerRecord],
            _strategy: DuplicateStrategy,
        ) -> Result<(), StorageError> {
            unreachable!("not used in phase 5 tests")
        }

        async fn get_drawer(&self, _id: &DrawerId) -> Result<Option<DrawerRecord>, StorageError> {
            unreachable!("not used in phase 5 tests")
        }

        async fn delete_drawers(&self, _ids: &[DrawerId]) -> Result<usize, StorageError> {
            unreachable!("not used in phase 5 tests")
        }

        async fn search_drawers(
            &self,
            request: &SearchRequest,
        ) -> Result<Vec<DrawerMatch>, StorageError> {
            self.search_limits.lock().unwrap().push(request.limit);
            self.include_cutoff_ties.lock().unwrap().push(request.include_cutoff_ties);

            let mut filtered = self
                .drawers
                .iter()
                .filter(|drawer| filter_matches(drawer, &request.filter))
                .cloned()
                .map(|drawer| DrawerMatch {
                    distance: Some((drawer.embedding[0] - request.embedding[0]).abs()),
                    record: drawer,
                })
                .collect::<Vec<_>>();

            filtered.sort_by(|left, right| {
                left.distance.partial_cmp(&right.distance).unwrap_or(std::cmp::Ordering::Equal)
            });
            if request.limit == 0 {
                filtered.clear();
            } else if request.include_cutoff_ties && filtered.len() > request.limit {
                let cutoff = filtered[request.limit - 1].distance;
                let cutoff_len = filtered.partition_point(|entry| entry.distance <= cutoff);
                filtered.truncate(cutoff_len);
            } else {
                filtered.truncate(request.limit);
            }
            Ok(filtered)
        }

        async fn list_drawers(
            &self,
            _filter: &DrawerFilter,
        ) -> Result<Vec<DrawerRecord>, StorageError> {
            *self.list_calls.lock().unwrap() += 1;
            Ok(self.drawers.clone())
        }
    }

    fn temp_test_dir(prefix: &str) -> PathBuf {
        let unique = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        std::env::temp_dir().join(format!("mempalace-search-{prefix}-{unique}"))
    }

    #[tokio::test]
    async fn search_applies_filters_and_normalizes_similarity() {
        let mut runtime = SearchRuntime::new(StubProvider { response: vec![embedding(0.0)] });
        let store = sample_store();

        let query = SearchQuery {
            text: "auth migration parity".to_owned(),
            wing: Some(WingId::new("wing_team").unwrap()),
            room: None,
            limit: 5,
            profile: EmbeddingProfile::Balanced,
        };

        let results = runtime.search(&store, &query).await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].wing.as_str(), "wing_team");
        assert_eq!(results[0].room.as_str(), "auth-migration");
        assert!((results[0].score - 0.51).abs() < 1e-6);
        assert_eq!(
            results[0].drawer_id.as_ref().map(|value| value.as_str()),
            Some("wing_team/auth-migration/0001")
        );
        assert_eq!(results[0].source_file, "team.txt");
        assert_eq!(results[1].room.as_str(), "phase0-rollout");
    }

    #[tokio::test]
    async fn search_rejects_blank_query() {
        let mut runtime = SearchRuntime::new(StubProvider { response: vec![embedding(0.0)] });
        let store = sample_store();

        let err = runtime
            .search(
                &store,
                &SearchQuery {
                    text: "   \n\t ".to_owned(),
                    wing: None,
                    room: None,
                    limit: 5,
                    profile: EmbeddingProfile::Balanced,
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(err, SearchError::BlankQuery));
    }

    #[tokio::test]
    async fn search_returns_empty_results_when_limit_is_zero() {
        let mut runtime = SearchRuntime::new(StubProvider { response: vec![embedding(0.0)] });
        let store = sample_store();

        let results = runtime
            .search(
                &store,
                &SearchQuery {
                    text: "auth".to_owned(),
                    wing: None,
                    room: None,
                    limit: 0,
                    profile: EmbeddingProfile::Balanced,
                },
            )
            .await
            .unwrap();

        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn search_rejects_profile_mismatch() {
        let mut runtime = SearchRuntime::new(StubProvider { response: vec![embedding(0.0)] });
        let store = sample_store();
        let err = runtime
            .search(
                &store,
                &SearchQuery {
                    text: "auth".to_owned(),
                    wing: None,
                    room: None,
                    limit: 5,
                    profile: EmbeddingProfile::LowCpu,
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(err, super::SearchError::ProfileMismatch { .. }));
    }

    #[tokio::test]
    async fn search_tie_breaking_is_deterministic() {
        let store = StubStore {
            drawers: vec![
                record(
                    "wing_b/general/0001",
                    "wing_b",
                    "general",
                    "zeta.txt",
                    "B",
                    Some(0.5),
                    datetime!(2026-04-11 09:00:00 UTC),
                ),
                record(
                    "wing_a/general/0001",
                    "wing_a",
                    "general",
                    "alpha.txt",
                    "A",
                    Some(0.5),
                    datetime!(2026-04-11 09:00:00 UTC),
                ),
            ],
        };
        let mut runtime = SearchRuntime::new(StubProvider { response: vec![embedding(0.0)] });
        let query = SearchQuery {
            text: "tie".to_owned(),
            wing: None,
            room: None,
            limit: 5,
            profile: EmbeddingProfile::Balanced,
        };

        let results = runtime.search(&store, &query).await.unwrap();
        assert_eq!(
            results.iter().map(|entry| entry.wing.as_str()).collect::<Vec<_>>(),
            vec!["wing_a", "wing_b"]
        );
    }

    #[tokio::test]
    async fn search_requests_full_cutoff_tie_group_before_truncating_top_k() {
        let store = StubStore {
            drawers: (0..40)
                .rev()
                .map(|index| {
                    record(
                        &format!("wing_{index:02}/general/0001"),
                        &format!("wing_{index:02}"),
                        "general",
                        &format!("file-{index:02}.txt"),
                        &format!("payload-{index:02}"),
                        Some(0.5),
                        datetime!(2026-04-11 09:00:00 UTC),
                    )
                })
                .collect(),
        };
        let mut runtime = SearchRuntime::new(StubProvider { response: vec![embedding(0.0)] });
        let query = SearchQuery {
            text: "tie".to_owned(),
            wing: None,
            room: None,
            limit: 3,
            profile: EmbeddingProfile::Balanced,
        };

        let results = runtime.search(&store, &query).await.unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(
            results.iter().map(|entry| entry.wing.as_str()).collect::<Vec<_>>(),
            vec!["wing_00", "wing_01", "wing_02"]
        );
    }

    #[tokio::test]
    async fn rerank_policy_requests_extra_candidates_and_prefers_lexical_overlap() {
        let store = SearchSpyStore {
            drawers: vec![
                record(
                    "project_alpha/backend/0001",
                    "project_alpha",
                    "backend",
                    "backend.md",
                    "session lease keeps auth tokens valid",
                    Some(0.02),
                    datetime!(2026-04-11 09:00:00 UTC),
                ),
                record(
                    "project_alpha/backend/0002",
                    "project_alpha",
                    "backend",
                    "backend-2.md",
                    "session refresh rotates session refresh auth token keys",
                    Some(0.04),
                    datetime!(2026-04-11 08:00:00 UTC),
                ),
            ],
            list_calls: Arc::new(Mutex::new(0)),
            search_limits: Arc::new(Mutex::new(Vec::new())),
            include_cutoff_ties: Arc::new(Mutex::new(Vec::new())),
        };
        let runtime = &mut SearchRuntime::with_policy(
            StubProvider { response: vec![embedding(0.0)] },
            SearchRuntimePolicy { rerank_enabled: true },
        );

        let results = runtime
            .search(
                &store,
                &SearchQuery {
                    text: "session refresh auth".to_owned(),
                    wing: None,
                    room: None,
                    limit: 1,
                    profile: EmbeddingProfile::Balanced,
                },
            )
            .await
            .unwrap();

        assert_eq!(store.search_limits.lock().unwrap().as_slice(), &[2]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].source_file, "backend-2.md");
    }

    #[tokio::test]
    async fn search_semantic_ignores_rerank_policy_for_threshold_sensitive_callers() {
        let store = SearchSpyStore {
            drawers: vec![
                record(
                    "project_alpha/backend/0001",
                    "project_alpha",
                    "backend",
                    "backend.md",
                    "session lease keeps auth tokens valid",
                    Some(0.02),
                    datetime!(2026-04-11 09:00:00 UTC),
                ),
                record(
                    "project_alpha/backend/0002",
                    "project_alpha",
                    "backend",
                    "backend-2.md",
                    "session refresh rotates session refresh auth token keys",
                    Some(0.04),
                    datetime!(2026-04-11 08:00:00 UTC),
                ),
            ],
            list_calls: Arc::new(Mutex::new(0)),
            search_limits: Arc::new(Mutex::new(Vec::new())),
            include_cutoff_ties: Arc::new(Mutex::new(Vec::new())),
        };
        let runtime = &mut SearchRuntime::with_policy(
            StubProvider { response: vec![embedding(0.0)] },
            SearchRuntimePolicy { rerank_enabled: true },
        );
        let query = SearchQuery {
            text: "session refresh auth".to_owned(),
            wing: None,
            room: None,
            limit: 1,
            profile: EmbeddingProfile::Balanced,
        };

        let reranked = runtime.search(&store, &query).await.unwrap();
        let semantic = runtime.search_semantic(&store, &query).await.unwrap();

        assert_eq!(reranked[0].source_file, "backend-2.md");
        assert_eq!(semantic[0].source_file, "backend.md");
    }

    #[tokio::test]
    async fn search_requests_cutoff_ties_without_listing_drawers() {
        let list_calls = Arc::new(Mutex::new(0usize));
        let search_limits = Arc::new(Mutex::new(Vec::new()));
        let include_cutoff_ties = Arc::new(Mutex::new(Vec::new()));
        let store = SearchSpyStore {
            drawers: vec![
                record(
                    "wing_b/general/0001",
                    "wing_b",
                    "general",
                    "zeta.txt",
                    "B",
                    Some(0.5),
                    datetime!(2026-04-11 09:00:00 UTC),
                ),
                record(
                    "wing_a/general/0001",
                    "wing_a",
                    "general",
                    "alpha.txt",
                    "A",
                    Some(0.5),
                    datetime!(2026-04-11 09:00:00 UTC),
                ),
            ],
            list_calls: Arc::clone(&list_calls),
            search_limits: Arc::clone(&search_limits),
            include_cutoff_ties: Arc::clone(&include_cutoff_ties),
        };
        let mut runtime = SearchRuntime::new(StubProvider { response: vec![embedding(0.0)] });

        let results = runtime
            .search(
                &store,
                &SearchQuery {
                    text: "tie".to_owned(),
                    wing: None,
                    room: None,
                    limit: 1,
                    profile: EmbeddingProfile::Balanced,
                },
            )
            .await
            .unwrap();

        assert_eq!(results[0].wing.as_str(), "wing_a");
        assert_eq!(*list_calls.lock().unwrap(), 0);
        assert_eq!(search_limits.lock().unwrap().as_slice(), &[1]);
        assert_eq!(include_cutoff_ties.lock().unwrap().as_slice(), &[true]);
    }

    #[tokio::test]
    async fn search_reports_provider_contract_when_embedding_vector_is_missing() {
        let mut runtime = SearchRuntime::new(StubProvider { response: Vec::new() });
        let store = sample_store();

        let err = runtime
            .search(
                &store,
                &SearchQuery {
                    text: "auth".to_owned(),
                    wing: None,
                    room: None,
                    limit: 5,
                    profile: EmbeddingProfile::Balanced,
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            SearchError::Embeddings(mempalace_embeddings::EmbeddingError::ProviderContract(
                ref message
            )) if message == "provider returned no vector for a non-empty search query"
        ));
    }

    #[tokio::test]
    async fn search_applies_combined_wing_and_room_filters() {
        let mut runtime = SearchRuntime::new(StubProvider { response: vec![embedding(0.0)] });
        let store = sample_store();

        let results = runtime
            .search(
                &store,
                &SearchQuery {
                    text: "auth".to_owned(),
                    wing: Some(WingId::new("wing_team").unwrap()),
                    room: Some(RoomId::new("auth-migration").unwrap()),
                    limit: 5,
                    profile: EmbeddingProfile::Balanced,
                },
            )
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].wing.as_str(), "wing_team");
        assert_eq!(results[0].room.as_str(), "auth-migration");
    }

    #[tokio::test]
    async fn search_applies_room_only_filters() {
        let mut runtime = SearchRuntime::new(StubProvider { response: vec![embedding(0.0)] });
        let store = sample_store();

        let results = runtime
            .search(
                &store,
                &SearchQuery {
                    text: "auth".to_owned(),
                    wing: None,
                    room: Some(RoomId::new("auth-migration").unwrap()),
                    limit: 5,
                    profile: EmbeddingProfile::Balanced,
                },
            )
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|result| result.room.as_str() == "auth-migration"));
    }

    #[test]
    fn render_search_results_matches_python_shape() {
        let rendered = render_search_results(
            "auth migration parity",
            &[
                mempalace_core::SearchResult {
                    drawer_id: None,
                    wing: WingId::new("wing_team").unwrap(),
                    room: RoomId::new("auth-migration").unwrap(),
                    score: 0.49,
                    content: "The team decided the auth-migration must preserve CLI and MCP parity."
                        .to_owned(),
                    source_file: "team.txt".to_owned(),
                },
                mempalace_core::SearchResult {
                    drawer_id: None,
                    wing: WingId::new("wing_code").unwrap(),
                    room: RoomId::new("auth-migration").unwrap(),
                    score: 0.069,
                    content: "Code notes: auth-migration keeps search filter semantics exact while storage changes underneath."
                        .to_owned(),
                    source_file: "code.txt".to_owned(),
                },
            ],
            None,
            None,
        );

        assert!(rendered.contains("Results for: \"auth migration parity\""));
        assert!(rendered.contains("[1] wing_team / auth-migration"));
        assert!(rendered.contains("Match:  0.49"));
        assert!(rendered.contains("Match:  0.069"));
    }

    #[test]
    fn trim_similarity_trims_trailing_zeroes_exactly() {
        assert_eq!(trim_similarity(1.0), "1");
        assert_eq!(trim_similarity(0.5), "0.5");
        assert_eq!(trim_similarity(0.49), "0.49");
        assert_eq!(trim_similarity(0.069), "0.069");
    }

    #[tokio::test]
    async fn recall_returns_stable_filtered_layer_output() {
        let runtime = SearchRuntime::new(StubProvider { response: vec![embedding(0.0)] });
        let store = sample_store();
        let rendered = runtime
            .recall(
                &store,
                &LayerRetrieveRequest {
                    wing: Some(WingId::new("wing_team").unwrap()),
                    room: None,
                    limit: 10,
                },
            )
            .await
            .unwrap();

        assert!(rendered.starts_with("## L2 — ON-DEMAND (2) drawers"));
        assert!(rendered.contains("[auth-migration] The team decided"));
        assert!(rendered.contains("[phase0-rollout] Phase 0 rollout"));
    }

    #[tokio::test]
    async fn recall_reports_empty_store() {
        let runtime = SearchRuntime::new(StubProvider { response: vec![embedding(0.0)] });
        let store = StubStore { drawers: Vec::new() };

        let rendered = runtime
            .recall(
                &store,
                &LayerRetrieveRequest {
                    wing: Some(WingId::new("wing_team").unwrap()),
                    room: Some(RoomId::new("auth-migration").unwrap()),
                    limit: 10,
                },
            )
            .await
            .unwrap();

        assert_eq!(rendered, "No drawers found for wing=wing_team room=auth-migration.");
    }

    #[tokio::test]
    async fn recall_limit_zero_returns_zero_drawers() {
        let runtime = SearchRuntime::new(StubProvider { response: vec![embedding(0.0)] });
        let store = StubStore {
            drawers: (0..12)
                .map(|index| {
                    record(
                        &format!("wing_team/general/{index:04}"),
                        "wing_team",
                        "general",
                        &format!("fixtures/{index:04}.txt"),
                        &format!("entry {index:04}"),
                        Some(10.0 - index as f32),
                        datetime!(2026-04-11 09:00:00 UTC),
                    )
                })
                .collect(),
        };

        let rendered = runtime
            .recall(
                &store,
                &LayerRetrieveRequest {
                    wing: Some(WingId::new("wing_team").unwrap()),
                    room: None,
                    limit: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(rendered, "## L2 — ON-DEMAND (0) drawers");
    }

    #[tokio::test]
    async fn wake_up_uses_identity_and_groups_rooms_in_stable_order() {
        let runtime = SearchRuntime::new(StubProvider { response: vec![embedding(0.0)] });
        let store = sample_store();
        let rendered = runtime
            .wake_up(
                &store,
                &WakeUpRequest {
                    wing: None,
                    identity: IdentitySource::Inline(
                        "## L0 — IDENTITY\nI am the MemPalace phase 0 reference capture."
                            .to_owned(),
                    ),
                    layer1: Layer1Config { max_drawers: 4, max_chars: 3_200 },
                    format: WakeUpFormat::PlainText,
                },
            )
            .await
            .unwrap();

        assert!(rendered.starts_with("## L0 — IDENTITY"));
        assert!(rendered.contains("## L1 — ESSENTIAL STORY"));
        let auth_index = rendered.find("[auth-migration]").unwrap();
        let backend_index = rendered.find("[backend]").unwrap();
        let rollout_index = rendered.find("[phase0-rollout]").unwrap();
        assert!(auth_index < backend_index);
        assert!(backend_index < rollout_index);
        assert!(rendered.contains("(team.txt)"));
        assert!(rendered.contains("(auth.py)"));
    }

    #[tokio::test]
    async fn wake_up_defaults_to_plain_text_format() {
        let runtime = SearchRuntime::new(StubProvider { response: vec![embedding(0.0)] });
        let store = sample_store();
        let identity = IdentitySource::Inline("## L0 — IDENTITY\nReady.".to_owned());

        let default_rendered = runtime
            .wake_up(
                &store,
                &WakeUpRequest { identity: identity.clone(), ..WakeUpRequest::default() },
            )
            .await
            .unwrap();
        let explicit_rendered = runtime
            .wake_up(
                &store,
                &WakeUpRequest {
                    wing: None,
                    identity,
                    layer1: Layer1Config::default(),
                    format: WakeUpFormat::PlainText,
                },
            )
            .await
            .unwrap();

        assert_eq!(default_rendered, explicit_rendered);
        assert!(default_rendered.contains("## L1 — ESSENTIAL STORY"));
    }

    #[tokio::test]
    async fn wake_up_applies_wing_filter_end_to_end() {
        let runtime = SearchRuntime::new(StubProvider { response: vec![embedding(0.0)] });
        let store = sample_store();

        let rendered = runtime
            .wake_up(
                &store,
                &WakeUpRequest {
                    wing: Some(WingId::new("wing_code").unwrap()),
                    identity: IdentitySource::Inline("## L0 — IDENTITY\nReady.".to_owned()),
                    layer1: Layer1Config::default(),
                    format: WakeUpFormat::PlainText,
                },
            )
            .await
            .unwrap();

        assert!(rendered.contains("## L0 — IDENTITY\nReady."));
        assert!(rendered.contains("## L1 — ESSENTIAL STORY"));
        assert!(rendered.contains("[auth-migration]"));
        assert!(rendered.contains("code.txt"));
        assert!(!rendered.contains("team.txt"));
        assert!(!rendered.contains("auth.py"));
    }

    #[tokio::test]
    async fn wake_up_supports_aaak_format() {
        let runtime = SearchRuntime::new(StubProvider { response: vec![embedding(0.0)] });
        let store = sample_store();

        let rendered = runtime
            .wake_up(
                &store,
                &WakeUpRequest {
                    wing: Some(WingId::new("wing_code").unwrap()),
                    identity: IdentitySource::Inline("## L0 — IDENTITY\nReady.".to_owned()),
                    layer1: Layer1Config::default(),
                    format: WakeUpFormat::AaaK,
                },
            )
            .await
            .unwrap();

        assert!(rendered.contains("## L1 — AAAK STORY"));
        assert!(rendered.contains("wing_code|auth-migration|2026-04-11|code"));
        assert!(rendered.contains(":: 0:???|"));
        assert!(!rendered.contains("wing_team|"));
    }

    #[tokio::test]
    async fn wake_up_aaak_uses_configured_runtime_dialect() {
        let runtime = SearchRuntime::with_dialect(
            StubProvider { response: vec![embedding(0.0)] },
            Dialect::with_entities([("alice", "ALC")]),
        );
        let store = StubStore {
            drawers: vec![record(
                "wing_code/notes/0001",
                "wing_code",
                "notes",
                "fixtures/alice.txt",
                "alice documented the migration contract.",
                Some(10.0),
                datetime!(2026-04-11 09:45:00 UTC),
            )],
        };

        let rendered = runtime
            .wake_up(
                &store,
                &WakeUpRequest {
                    wing: Some(WingId::new("wing_code").unwrap()),
                    identity: IdentitySource::Inline("## L0 — IDENTITY\nReady.".to_owned()),
                    layer1: Layer1Config::default(),
                    format: WakeUpFormat::AaaK,
                },
            )
            .await
            .unwrap();

        assert!(rendered.contains(":: 0:ALC|"));
    }

    #[tokio::test]
    async fn wake_up_supports_aaak_truncation_without_orphan_room_headers() {
        let runtime = SearchRuntime::new(StubProvider { response: vec![embedding(0.0)] });
        let store = StubStore {
            drawers: vec![
                record(
                    "wing_code/alpha/0001",
                    "wing_code",
                    "alpha",
                    "fixtures/alpha.txt",
                    "Tiny note.",
                    Some(10.0),
                    datetime!(2026-04-11 09:45:00 UTC),
                ),
                record(
                    "wing_code/beta/0002",
                    "wing_code",
                    "beta",
                    "fixtures/beta.txt",
                    "Second room should truncate before its first entry lands in the wake-up output.",
                    Some(9.0),
                    datetime!(2026-04-11 09:30:00 UTC),
                ),
            ],
        };

        let rendered = runtime
            .wake_up(
                &store,
                &WakeUpRequest {
                    wing: Some(WingId::new("wing_code").unwrap()),
                    identity: IdentitySource::Inline("## L0 — IDENTITY\nReady.".to_owned()),
                    layer1: Layer1Config { max_drawers: 2, max_chars: 70 },
                    format: WakeUpFormat::AaaK,
                },
            )
            .await
            .unwrap();

        assert!(rendered.contains("[alpha]"));
        assert!(rendered.contains("... (more in L3 search)"));
        assert!(!rendered.contains("[beta]"));
    }

    #[tokio::test]
    async fn wake_up_aaak_honors_full_output_budget_end_to_end() {
        let runtime = SearchRuntime::new(StubProvider { response: vec![embedding(0.0)] });
        let store = sample_store();

        let rendered = runtime
            .wake_up(
                &store,
                &WakeUpRequest {
                    wing: Some(WingId::new("wing_code").unwrap()),
                    identity: IdentitySource::Inline("## L0 — IDENTITY\nReady.".to_owned()),
                    layer1: Layer1Config {
                        max_drawers: 3,
                        max_chars: super::char_count(
                            "## L0 — IDENTITY\nReady.\n\n## L1 — AAAK STORY\n\n[auth-migration]\n  ... (more in L3 search)",
                        ),
                    },
                    format: WakeUpFormat::AaaK,
                },
            )
            .await
            .unwrap();

        assert_eq!(super::char_count(&rendered), 81);
        assert_eq!(
            rendered,
            "## L0 — IDENTITY\nReady.\n\n## L1 — AAAK STORY\n\n[auth-migration]\n  ... (more in L3 search)"
        );
    }

    #[tokio::test]
    async fn wake_up_aaak_is_deterministic_across_repeated_runs() {
        let runtime = SearchRuntime::new(StubProvider { response: vec![embedding(0.0)] });
        let store = sample_store();
        let request = WakeUpRequest {
            wing: Some(WingId::new("wing_code").unwrap()),
            identity: IdentitySource::Inline("## L0 — IDENTITY\nReady.".to_owned()),
            layer1: Layer1Config::default(),
            format: WakeUpFormat::AaaK,
        };

        let first = runtime.wake_up(&store, &request).await.unwrap();
        let second = runtime.wake_up(&store, &request).await.unwrap();
        let third = runtime.wake_up(&store, &request).await.unwrap();

        assert_eq!(first, second);
        assert_eq!(second, third);
    }

    #[tokio::test]
    async fn generate_layer1_honors_wing_filter() {
        let store = sample_store();
        let layer1 = generate_layer1(
            &store,
            Some(WingId::new("wing_code").unwrap()),
            Layer1Config::default(),
        )
        .await
        .unwrap();

        assert!(layer1.contains("[auth-migration]"));
        assert!(layer1.contains("code.txt"));
        assert!(!layer1.contains("team.txt"));
    }

    #[tokio::test]
    async fn generate_layer1_truncates_when_max_chars_is_exceeded() {
        let store = sample_store();
        let rendered =
            generate_layer1(&store, None, Layer1Config { max_drawers: 4, max_chars: 120 })
                .await
                .unwrap();

        assert!(rendered.contains("## L1 — ESSENTIAL STORY"));
        assert!(rendered.contains("... (more in L3 search)"));
    }

    #[tokio::test]
    async fn search_text_reports_empty_results() {
        let mut runtime = SearchRuntime::new(StubProvider { response: vec![embedding(100.0)] });
        let store = StubStore { drawers: Vec::new() };
        let rendered = runtime
            .search_text(
                &store,
                &SearchQuery {
                    text: "missing".to_owned(),
                    wing: None,
                    room: None,
                    limit: 5,
                    profile: EmbeddingProfile::Balanced,
                },
            )
            .await
            .unwrap();

        assert_eq!(rendered, "\n  No results found for: \"missing\"");
    }

    #[tokio::test]
    async fn search_text_renders_non_empty_results() {
        let mut runtime = SearchRuntime::new(StubProvider { response: vec![embedding(0.0)] });
        let store = sample_store();
        let rendered = runtime
            .search_text(
                &store,
                &SearchQuery {
                    text: "auth".to_owned(),
                    wing: Some(WingId::new("wing_team").unwrap()),
                    room: None,
                    limit: 2,
                    profile: EmbeddingProfile::Balanced,
                },
            )
            .await
            .unwrap();

        assert!(rendered.contains("Results for: \"auth\""));
        assert!(rendered.contains("Wing: wing_team"));
        assert!(rendered.contains("Source: team.txt"));
    }

    #[test]
    fn identity_source_can_load_inline_path_and_missing_default() {
        assert_eq!(IdentitySource::Inline(" hello \n".to_owned()).render().unwrap(), "hello");
        let dir = temp_test_dir("identity-inline");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("identity.txt");
        fs::write(&path, " from file \n").unwrap();
        assert_eq!(IdentitySource::Path(path.clone()).render().unwrap(), "from file");
        fs::remove_dir_all(&dir).unwrap();
        assert!(
            IdentitySource::MissingDefault.render().unwrap().contains("No identity configured")
        );
    }

    #[test]
    fn identity_source_path_reports_read_errors() {
        let err = IdentitySource::Path(PathBuf::from("/definitely/missing/identity.txt"))
            .render()
            .unwrap_err();

        assert!(matches!(err, SearchError::IdentityRead { .. }));
    }

    #[test]
    fn default_identity_path_joins_home_directory() {
        let dir = temp_test_dir("default-identity");
        let identity_dir = dir.join(".mempalace");
        fs::create_dir_all(&identity_dir).unwrap();
        assert_eq!(
            default_identity_path_from_home(Some(dir.clone())),
            Some(identity_dir.join("identity.txt"))
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn wake_up_request_default_path_reads_identity_file() {
        let dir = temp_test_dir("default-identity-render");
        let identity_dir = dir.join(".mempalace");
        let identity_path = identity_dir.join("identity.txt");
        fs::create_dir_all(&identity_dir).unwrap();
        fs::write(&identity_path, "## L0 — IDENTITY\nConfigured by home directory.\n").unwrap();

        let rendered = IdentitySource::DefaultPath(identity_path).render().unwrap();

        assert_eq!(rendered, "## L0 — IDENTITY\nConfigured by home directory.");
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn wake_up_request_default_path_falls_back_when_home_identity_is_missing() {
        let dir = temp_test_dir("default-missing");
        let identity_path = dir.join(".mempalace").join("identity.txt");
        fs::create_dir_all(&dir).unwrap();

        let rendered = IdentitySource::DefaultPath(identity_path).render().unwrap();

        assert_eq!(
            rendered,
            "## L0 — IDENTITY\nNo identity configured. Create ~/.mempalace/identity.txt"
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn default_identity_path_returns_none_without_home() {
        assert_eq!(default_identity_path_from_home(None), None);
        assert!(matches!(
            WakeUpRequest::default().identity,
            IdentitySource::DefaultPath(_) | IdentitySource::MissingDefault
        ));
    }

    #[test]
    fn wake_up_request_missing_default_uses_literal_tilde_path_when_home_is_unset() {
        assert_eq!(
            default_identity_path(),
            default_identity_path_from_home(std::env::var_os("HOME").map(PathBuf::from))
        );
        assert_eq!(
            IdentitySource::MissingDefault.render().unwrap(),
            "## L0 — IDENTITY\nNo identity configured. Create ~/.mempalace/identity.txt"
        );
    }

    #[tokio::test]
    async fn generate_layer1_reports_empty_store_directly() {
        let store = StubStore { drawers: Vec::new() };
        let rendered = generate_layer1(&store, None, Layer1Config::default()).await.unwrap();

        assert_eq!(rendered, "## L1 — No memories yet.");
    }

    #[tokio::test]
    async fn wake_up_reports_empty_store_story() {
        let runtime = SearchRuntime::new(StubProvider { response: vec![embedding(0.0)] });
        let store = StubStore { drawers: Vec::new() };
        let rendered = runtime
            .wake_up(
                &store,
                &WakeUpRequest {
                    wing: None,
                    identity: IdentitySource::Inline("## L0 — IDENTITY\nReady.".to_owned()),
                    layer1: Layer1Config::default(),
                    format: WakeUpFormat::PlainText,
                },
            )
            .await
            .unwrap();

        assert!(rendered.contains("## L0 — IDENTITY\nReady."));
        assert!(rendered.contains("## L1 — No memories yet."));
    }

    #[tokio::test]
    async fn generate_layer1_counts_unicode_chars_in_budget() {
        let store = StubStore {
            drawers: vec![record(
                "wing_team/cafe/0001",
                "wing_team",
                "cafe",
                "fixtures/cafe.txt",
                "éééééééééééééééééééé",
                Some(0.9),
                datetime!(2026-04-11 09:00:00 UTC),
            )],
        };
        let entry = "  - éééééééééééééééééééé  (cafe.txt)";
        let max_chars = super::char_count("\n[cafe]") + super::char_count(entry);

        let rendered = generate_layer1(&store, None, Layer1Config { max_drawers: 1, max_chars })
            .await
            .unwrap();

        assert!(rendered.contains(entry));
        assert!(!rendered.contains("... (more in L3 search)"));
    }

    #[tokio::test]
    async fn generate_layer1_does_not_count_header_against_budget() {
        let store = StubStore {
            drawers: vec![record(
                "wing_team/cafe/0001",
                "wing_team",
                "cafe",
                "fixtures/cafe.txt",
                "header budget parity",
                Some(0.9),
                datetime!(2026-04-11 09:00:00 UTC),
            )],
        };
        let entry = "  - header budget parity  (cafe.txt)";
        let max_chars = super::char_count("\n[cafe]") + super::char_count(entry);

        let rendered = generate_layer1(&store, None, Layer1Config { max_drawers: 1, max_chars })
            .await
            .unwrap();

        assert!(rendered.contains("## L1 — ESSENTIAL STORY"));
        assert!(rendered.contains(entry));
        assert!(!rendered.contains("... (more in L3 search)"));
    }

    #[tokio::test]
    async fn generate_layer1_matches_python_reference_for_header_budget_case() {
        let store = StubStore {
            drawers: vec![record(
                "wing_team/cafe/0001",
                "wing_team",
                "cafe",
                "fixtures/cafe.txt",
                "header budget parity",
                Some(0.9),
                datetime!(2026-04-11 09:00:00 UTC),
            )],
        };
        let expected = "## L1 — ESSENTIAL STORY\n\n[cafe]\n  - header budget parity  (cafe.txt)";
        let max_chars =
            "\n[cafe]".chars().count() + "  - header budget parity  (cafe.txt)".chars().count();

        let rendered = generate_layer1(&store, None, Layer1Config { max_drawers: 1, max_chars })
            .await
            .unwrap();

        assert_eq!(rendered, expected);
    }

    #[tokio::test]
    async fn generate_layer1_does_not_emit_orphan_room_headers_on_truncation() {
        let store = StubStore {
            drawers: vec![
                record(
                    "wing_team/alpha/0001",
                    "wing_team",
                    "alpha",
                    "fixtures/alpha.txt",
                    "alpha entry",
                    Some(0.9),
                    datetime!(2026-04-11 09:00:00 UTC),
                ),
                record(
                    "wing_team/beta/0001",
                    "wing_team",
                    "beta",
                    "fixtures/beta.txt",
                    "beta entry that should be truncated",
                    Some(0.8),
                    datetime!(2026-04-11 08:00:00 UTC),
                ),
            ],
        };
        let max_chars = super::char_count("\n[alpha]\n  - alpha entry  (alpha.txt)");

        let rendered = generate_layer1(&store, None, Layer1Config { max_drawers: 2, max_chars })
            .await
            .unwrap();

        assert!(rendered.contains("[alpha]"));
        assert!(!rendered.contains("[beta]"));
        assert!(rendered.contains("... (more in L3 search)"));
    }

    #[tokio::test]
    async fn generate_layer1_keeps_buffered_room_entries_when_truncating_mid_room() {
        let store = StubStore {
            drawers: vec![
                record(
                    "wing_team/alpha/0001",
                    "wing_team",
                    "alpha",
                    "fixtures/alpha-1.txt",
                    "first alpha entry",
                    Some(0.9),
                    datetime!(2026-04-11 09:00:00 UTC),
                ),
                record(
                    "wing_team/alpha/0002",
                    "wing_team",
                    "alpha",
                    "fixtures/alpha-2.txt",
                    "second alpha entry that should overflow the layer one budget",
                    Some(0.8),
                    datetime!(2026-04-11 08:00:00 UTC),
                ),
            ],
        };
        let max_chars = super::char_count("\n[alpha]")
            + super::char_count("  - first alpha entry  (alpha-1.txt)");

        let rendered = generate_layer1(&store, None, Layer1Config { max_drawers: 2, max_chars })
            .await
            .unwrap();

        assert!(rendered.contains("[alpha]"));
        assert!(rendered.contains("first alpha entry  (alpha-1.txt)"));
        assert!(!rendered.contains("second alpha entry"));
        assert!(rendered.contains("... (more in L3 search)"));
    }

    #[test]
    fn normalized_terms_preserve_unicode_words() {
        assert_eq!(
            normalized_terms("CAFÉ 日本語 Привет -- auth"),
            vec!["café", "日本語", "привет", "auth"]
        );
        assert!(
            (lexical_overlap_score(&normalized_terms("日本語 café"), "日本語 café notes") - 1.0)
                .abs()
                < 1e-6
        );
    }
}

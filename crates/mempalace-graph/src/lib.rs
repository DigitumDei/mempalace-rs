#![allow(missing_docs)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use aho_corasick::AhoCorasick;
use mempalace_core::{DrawerId, DrawerRecord};
use mempalace_storage::{
    DrawerFilter, DrawerStore, EntityRecord, EntityRegistryStore, GraphDocument, GraphStore,
    KnowledgeGraphFact, KnowledgeGraphStore, ToolStateStore, core::MempalaceError,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::{Date, OffsetDateTime};

pub use mempalace_core as core;
pub use mempalace_storage as storage;

const REGISTRY_SCHEMA_VERSION: u32 = 1;
const PALACE_GRAPH_KEY: &str = "palace_graph:v1";
const REGISTRY_MODE_CONFIG_KEY: &str = "entity_registry.mode";
const DEFAULT_ENTITY_READ_CHARS: usize = 5_000;
const PERSON_CONTEXT_PATTERNS: &[&str] = &[
    "{name} said",
    "{name} asked",
    "{name} told",
    "{name} replied",
    "{name} laughed",
    "{name} smiled",
    "{name} cried",
    "{name} felt",
    "{name} thinks",
    "{name} wants",
    "{name} loves",
    "{name} hates",
    "{name} knows",
    "{name} decided",
    "{name} pushed",
    "{name} wrote",
    "hey {name}",
    "thanks {name}",
    "hi {name}",
    "dear {name}",
];
const PROJECT_CONTEXT_PATTERNS: &[&str] = &[
    "building {name}",
    "built {name}",
    "shipping {name}",
    "shipped {name}",
    "launching {name}",
    "launched {name}",
    "deployed {name}",
    "deploying {name}",
    "the {name} architecture",
    "the {name} pipeline",
    "the {name} system",
    "the {name} repo",
    "import {name}",
    "pip install {name}",
];
const PRONOUN_PATTERNS: &[&str] =
    &[" she ", " her ", " hers ", " he ", " him ", " his ", " they ", " them ", " their "];
const COMMON_ENGLISH_WORDS: &[&str] = &[
    "ever", "grace", "will", "bill", "mark", "april", "may", "june", "joy", "hope", "faith",
    "chance", "chase", "hunter", "dash", "flash", "star", "sky", "river", "brook", "lane", "art",
    "clay", "gil", "nat", "max", "rex", "ray", "jay", "rose", "violet", "lily", "ivy", "ash",
    "reed", "sage",
];
const PERSON_DISAMBIGUATION_PATTERNS: &[&str] = &[
    "{name} said",
    "{name} told",
    "{name} asked",
    "{name} was",
    "{name} is",
    "with {name}",
    "saw {name}",
    "called {name}",
    "hey {name}",
    "thanks {name}",
    "my friend {name}",
];
const CONCEPT_DISAMBIGUATION_PATTERNS: &[&str] = &[
    "the {name} of",
    "have you {name}",
    "if you {name}",
    "{name} since",
    "{name} again",
    "not {name}",
    "{name} more",
    "would {name}",
    "could {name}",
    "will {name}",
];
const STOPWORDS: &[&str] = &[
    "the",
    "a",
    "an",
    "and",
    "or",
    "but",
    "in",
    "on",
    "at",
    "to",
    "for",
    "of",
    "with",
    "by",
    "from",
    "as",
    "is",
    "was",
    "are",
    "were",
    "be",
    "been",
    "being",
    "have",
    "has",
    "had",
    "do",
    "does",
    "did",
    "will",
    "would",
    "could",
    "should",
    "may",
    "might",
    "must",
    "shall",
    "can",
    "this",
    "that",
    "these",
    "those",
    "it",
    "its",
    "they",
    "them",
    "their",
    "we",
    "our",
    "you",
    "your",
    "i",
    "my",
    "me",
    "he",
    "she",
    "his",
    "her",
    "who",
    "what",
    "when",
    "where",
    "why",
    "how",
    "which",
    "if",
    "then",
    "so",
    "not",
    "no",
    "yes",
    "ok",
    "okay",
    "just",
    "very",
    "really",
    "also",
    "already",
    "still",
    "even",
    "only",
    "here",
    "there",
    "now",
    "too",
    "up",
    "out",
    "about",
    "like",
    "use",
    "get",
    "got",
    "make",
    "made",
    "take",
    "put",
    "come",
    "go",
    "see",
    "know",
    "think",
    "new",
    "old",
    "all",
    "any",
    "some",
    "return",
    "print",
    "def",
    "class",
    "import",
    "step",
    "usage",
    "run",
    "check",
    "find",
    "add",
    "set",
    "list",
    "args",
    "dict",
    "str",
    "int",
    "bool",
    "path",
    "file",
    "type",
    "name",
    "note",
    "example",
    "option",
    "result",
    "error",
    "warning",
    "info",
    "every",
    "each",
    "next",
    "last",
    "first",
    "second",
    "stack",
    "layer",
    "mode",
    "test",
    "stop",
    "start",
    "copy",
    "move",
    "source",
    "target",
    "output",
    "input",
    "data",
    "item",
    "key",
    "value",
    "returns",
    "raises",
    "yields",
    "self",
    "cls",
    "kwargs",
    "world",
    "well",
    "want",
    "topic",
    "choose",
    "social",
    "cars",
    "phones",
    "healthcare",
    "human",
    "humans",
    "people",
    "things",
    "something",
    "nothing",
    "everything",
    "anything",
    "someone",
    "everyone",
    "anyone",
    "way",
    "time",
    "day",
    "life",
    "place",
    "thing",
    "part",
    "kind",
    "sort",
    "case",
    "point",
    "idea",
    "fact",
    "sense",
    "question",
    "answer",
    "reason",
    "number",
    "version",
    "system",
    "hey",
    "hi",
    "hello",
    "thanks",
    "thank",
    "right",
    "let",
    "click",
    "hit",
    "press",
    "tap",
    "drag",
    "drop",
    "open",
    "close",
    "save",
    "load",
    "launch",
    "install",
    "download",
    "upload",
    "scroll",
    "select",
    "enter",
    "submit",
    "cancel",
    "confirm",
    "delete",
    "paste",
    "write",
    "read",
    "search",
    "show",
    "hide",
];

#[derive(Debug, Error)]
pub enum GraphError {
    #[error(transparent)]
    Storage(#[from] mempalace_storage::StorageError),
    #[error(transparent)]
    Core(#[from] MempalaceError),
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid registry entity payload for `{entity_id}`: {source}")]
    InvalidRegistryPayload {
        entity_id: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("unknown entity `{name}`")]
    UnknownEntity { name: String },
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, GraphError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityKind {
    Person,
    Project,
    Tool,
    Concept,
    Unknown,
    Uncertain,
}

impl EntityKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Person => "person",
            Self::Project => "project",
            Self::Tool => "tool",
            Self::Concept => "concept",
            Self::Unknown => "unknown",
            Self::Uncertain => "uncertain",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistryEntry {
    pub schema_version: u32,
    pub name: String,
    pub canonical_name: String,
    pub entity_type: EntityKind,
    pub source: String,
    pub contexts: Vec<String>,
    pub aliases: Vec<String>,
    pub relationship: Option<String>,
    pub confidence: u16,
    pub ambiguous: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityRegistry {
    pub mode: String,
    pub entries: Vec<RegistryEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedPerson {
    pub name: String,
    pub relationship: Option<String>,
    pub context: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupResult {
    pub entity_type: EntityKind,
    pub confidence: u16,
    pub source: String,
    pub canonical_name: String,
    pub needs_disambiguation: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityCandidate {
    pub name: String,
    pub entity_type: EntityKind,
    pub confidence: u16,
    pub frequency: usize,
    pub signals: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EntityDetectionReport {
    pub people: Vec<EntityCandidate>,
    pub projects: Vec<EntityCandidate>,
    pub uncertain: Vec<EntityCandidate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PalaceRoomNode {
    pub room: String,
    pub wings: Vec<String>,
    pub halls: Vec<String>,
    pub count: usize,
    pub dates: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PalaceTunnel {
    pub room: String,
    pub wings: Vec<String>,
    pub halls: Vec<String>,
    pub count: usize,
    pub recent: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PalaceTraversalStep {
    pub room: String,
    pub wings: Vec<String>,
    pub halls: Vec<String>,
    pub count: usize,
    pub hop: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connected_via: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PalaceGraphStats {
    pub total_rooms: usize,
    pub tunnel_rooms: usize,
    pub total_edges: usize,
    pub rooms_per_wing: BTreeMap<String, usize>,
    pub top_tunnels: Vec<PalaceTunnelSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PalaceTunnelSummary {
    pub room: String,
    pub wings: Vec<String>,
    pub count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PalaceGraphSnapshot {
    pub nodes: BTreeMap<String, PalaceRoomNode>,
    pub tunnels: Vec<PalaceTunnel>,
    pub stats: PalaceGraphStats,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryDirection {
    Outgoing,
    Incoming,
    Both,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KnowledgeQueryRow {
    pub direction: String,
    pub subject: String,
    pub predicate: String,
    pub object: String,
    pub valid_from: Option<String>,
    pub valid_to: Option<String>,
    pub confidence: f32,
    pub source_closet: Option<String>,
    pub current: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeTimelineRow {
    pub subject: String,
    pub predicate: String,
    pub object: String,
    pub valid_from: Option<String>,
    pub valid_to: Option<String>,
    pub current: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeGraphStats {
    pub entities: usize,
    pub triples: usize,
    pub current_facts: usize,
    pub expired_facts: usize,
    pub relationship_types: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AddFactRequest {
    pub subject: String,
    pub subject_type: EntityKind,
    pub predicate: String,
    pub object: String,
    pub object_type: EntityKind,
    pub valid_from: Option<Date>,
    pub valid_to: Option<Date>,
    pub confidence: f32,
    pub source_drawer_id: Option<DrawerId>,
    pub source_file: Option<String>,
}

impl EntityRegistry {
    pub fn empty(mode: impl Into<String>) -> Self {
        Self { mode: mode.into(), entries: Vec::new() }
    }

    pub fn seed(
        mode: impl Into<String>,
        people: &[SeedPerson],
        projects: &[String],
        aliases: &BTreeMap<String, String>,
    ) -> Self {
        let mut entries = BTreeMap::<String, RegistryEntry>::new();
        let mut reverse_aliases = BTreeMap::<String, Vec<String>>::new();
        for (alias, canonical) in aliases {
            reverse_aliases.entry(canonical.to_ascii_lowercase()).or_default().push(alias.clone());
        }

        for person in people {
            let canonical = person.name.trim().to_owned();
            if canonical.is_empty() {
                continue;
            }
            let aliases =
                reverse_aliases.get(&canonical.to_ascii_lowercase()).cloned().unwrap_or_default();
            insert_registry_entry(
                &mut entries,
                RegistryEntry {
                    schema_version: REGISTRY_SCHEMA_VERSION,
                    name: canonical.clone(),
                    canonical_name: canonical.clone(),
                    entity_type: EntityKind::Person,
                    source: "onboarding".to_owned(),
                    contexts: vec![person.context.clone()],
                    aliases: aliases.clone(),
                    relationship: person.relationship.clone(),
                    confidence: 100,
                    ambiguous: COMMON_ENGLISH_WORDS
                        .iter()
                        .any(|word| word.eq_ignore_ascii_case(&canonical)),
                },
            );
            for alias in aliases {
                insert_registry_entry(
                    &mut entries,
                    RegistryEntry {
                        schema_version: REGISTRY_SCHEMA_VERSION,
                        name: alias.clone(),
                        canonical_name: canonical.clone(),
                        entity_type: EntityKind::Person,
                        source: "onboarding".to_owned(),
                        contexts: vec![person.context.clone()],
                        aliases: vec![canonical.clone()],
                        relationship: person.relationship.clone(),
                        confidence: 100,
                        ambiguous: COMMON_ENGLISH_WORDS
                            .iter()
                            .any(|word| word.eq_ignore_ascii_case(&alias)),
                    },
                );
            }
        }

        for project in projects {
            let name = project.trim();
            if name.is_empty() {
                continue;
            }
            insert_registry_entry(
                &mut entries,
                RegistryEntry {
                    schema_version: REGISTRY_SCHEMA_VERSION,
                    name: name.to_owned(),
                    canonical_name: name.to_owned(),
                    entity_type: EntityKind::Project,
                    source: "onboarding".to_owned(),
                    contexts: vec!["work".to_owned()],
                    aliases: Vec::new(),
                    relationship: None,
                    confidence: 100,
                    ambiguous: false,
                },
            );
        }

        Self { mode: mode.into(), entries: entries.into_values().collect() }
    }

    pub fn lookup(&self, word: &str, context: &str) -> LookupResult {
        let lower = word.to_ascii_lowercase();
        if let Some(entry) = self.entries.iter().find(|entry| {
            entry.name.eq_ignore_ascii_case(word)
                || entry.aliases.iter().any(|alias| alias.eq_ignore_ascii_case(word))
        }) {
            if entry.ambiguous {
                if let Some(result) = disambiguate(word, context, entry) {
                    return result;
                }
            }
            return LookupResult {
                entity_type: entry.entity_type.clone(),
                confidence: entry.confidence,
                source: entry.source.clone(),
                canonical_name: entry.canonical_name.clone(),
                needs_disambiguation: false,
            };
        }

        if COMMON_ENGLISH_WORDS.iter().any(|candidate| *candidate == lower) {
            return LookupResult {
                entity_type: EntityKind::Unknown,
                confidence: 0,
                source: "none".to_owned(),
                canonical_name: word.to_owned(),
                needs_disambiguation: true,
            };
        }

        LookupResult {
            entity_type: EntityKind::Unknown,
            confidence: 0,
            source: "none".to_owned(),
            canonical_name: word.to_owned(),
            needs_disambiguation: false,
        }
    }

    pub fn persist<S>(&self, store: &S, updated_at: OffsetDateTime) -> Result<()>
    where
        S: EntityRegistryStore + ToolStateStore,
    {
        for entry in &self.entries {
            let entity_id = entity_id(&entry.entity_type, &entry.name);
            store.upsert_entity(&EntityRecord {
                entity_id,
                entity_type: entry.entity_type.as_str().to_owned(),
                payload: serde_json::to_value(entry)?,
                updated_at,
            })?;
        }
        store.put_config(&mempalace_storage::ConfigEntry {
            config_key: REGISTRY_MODE_CONFIG_KEY.to_owned(),
            config_value: self.mode.clone(),
        })?;
        Ok(())
    }

    pub fn load<S>(store: &S) -> Result<Self>
    where
        S: EntityRegistryStore + ToolStateStore,
    {
        let mut entries = Vec::new();
        for entity in store.list_entities()? {
            let entry =
                serde_json::from_value::<RegistryEntry>(entity.payload).map_err(|source| {
                    GraphError::InvalidRegistryPayload { entity_id: entity.entity_id, source }
                })?;
            entries.push(entry);
        }
        entries.sort_by(|left, right| {
            left.entity_type
                .as_str()
                .cmp(right.entity_type.as_str())
                .then(left.name.cmp(&right.name))
        });
        let mode = store
            .get_config(REGISTRY_MODE_CONFIG_KEY)?
            .map(|entry| entry.config_value)
            .unwrap_or_else(|| "personal".to_owned());
        Ok(Self { mode, entries })
    }
}

pub fn detect_entities_in_texts(texts: &[String]) -> EntityDetectionReport {
    let combined = texts.join("\n");
    let lines = combined.lines().map(str::to_owned).collect::<Vec<_>>();
    let lower_combined = format!(" {} ", combined.to_ascii_lowercase());
    let lower_lines = lines.iter().map(|line| line.to_ascii_lowercase()).collect::<Vec<_>>();
    let mut counts = BTreeMap::<String, usize>::new();

    for token in extract_candidates(&combined) {
        *counts.entry(token).or_default() += 1;
    }

    let mut people = Vec::new();
    let mut projects = Vec::new();
    let mut uncertain = Vec::new();

    for (name, frequency) in counts {
        if frequency < 3 {
            continue;
        }
        let candidate = classify_candidate(&name, frequency, &lower_combined, &lower_lines);
        match candidate.entity_type {
            EntityKind::Person => people.push(candidate),
            EntityKind::Project => projects.push(candidate),
            _ => uncertain.push(candidate),
        }
    }

    people.sort_by(|left, right| {
        right.confidence.cmp(&left.confidence).then(left.name.cmp(&right.name))
    });
    projects.sort_by(|left, right| {
        right.confidence.cmp(&left.confidence).then(left.name.cmp(&right.name))
    });
    uncertain.sort_by(|left, right| {
        right.frequency.cmp(&left.frequency).then(left.name.cmp(&right.name))
    });

    EntityDetectionReport { people, projects, uncertain }
}

pub fn detect_entities_in_files(
    paths: &[PathBuf],
    max_files: usize,
) -> Result<EntityDetectionReport> {
    let mut texts = Vec::new();
    for path in paths.iter().take(max_files) {
        texts.push(read_text_prefix(path)?);
    }
    Ok(detect_entities_in_texts(&texts))
}

pub fn derive_palace_graph(drawers: &[DrawerRecord]) -> PalaceGraphSnapshot {
    let mut room_data = BTreeMap::<String, RoomAccumulator>::new();
    let mut edges = BTreeSet::<(String, String, String, String)>::new();

    for drawer in drawers {
        let room = drawer.room.as_str();
        if room == "general" {
            continue;
        }
        let entry = room_data.entry(room.to_owned()).or_default();
        entry.count += 1;
        entry.wings.insert(drawer.wing.as_str().to_owned());
        if let Some(hall) = &drawer.hall {
            entry.halls.insert(hall.clone());
        }
        if let Some(date) = drawer.date {
            entry.dates.insert(format_date(date));
        }
    }

    for (room, data) in &room_data {
        let wings = data.wings.iter().cloned().collect::<Vec<_>>();
        if wings.len() < 2 {
            continue;
        }
        let halls = if data.halls.is_empty() {
            vec![String::new()]
        } else {
            data.halls.iter().cloned().collect::<Vec<_>>()
        };
        for (index, left) in wings.iter().enumerate() {
            for right in wings.iter().skip(index + 1) {
                for hall in &halls {
                    edges.insert((room.clone(), left.clone(), right.clone(), hall.clone()));
                }
            }
        }
    }

    let mut nodes = BTreeMap::new();
    let mut rooms_per_wing = BTreeMap::<String, usize>::new();
    let mut tunnels = Vec::new();

    for (room, data) in room_data {
        for wing in &data.wings {
            *rooms_per_wing.entry(wing.clone()).or_default() += 1;
        }
        let node = PalaceRoomNode {
            room: room.clone(),
            wings: data.wings.iter().cloned().collect(),
            halls: data.halls.iter().cloned().collect(),
            count: data.count,
            dates: data
                .dates
                .iter()
                .rev()
                .take(5)
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect(),
        };
        if node.wings.len() >= 2 {
            tunnels.push(PalaceTunnel {
                room: room.clone(),
                wings: node.wings.clone(),
                halls: node.halls.clone(),
                count: node.count,
                recent: node.dates.last().cloned(),
            });
        }
        nodes.insert(room, node);
    }

    tunnels.sort_by(|left, right| right.count.cmp(&left.count).then(left.room.cmp(&right.room)));
    let top_tunnels = tunnels
        .iter()
        .take(10)
        .map(|tunnel| PalaceTunnelSummary {
            room: tunnel.room.clone(),
            wings: tunnel.wings.clone(),
            count: tunnel.count,
        })
        .collect::<Vec<_>>();

    let stats = PalaceGraphStats {
        total_rooms: nodes.len(),
        tunnel_rooms: tunnels.len(),
        total_edges: edges.len(),
        rooms_per_wing,
        top_tunnels,
    };

    PalaceGraphSnapshot { nodes, tunnels, stats }
}

pub async fn derive_palace_graph_from_store<S>(store: &S) -> Result<PalaceGraphSnapshot>
where
    S: DrawerStore,
{
    let drawers = store.list_drawers(&DrawerFilter::default()).await?;
    Ok(derive_palace_graph(&drawers))
}

pub fn traverse_graph(
    snapshot: &PalaceGraphSnapshot,
    start_room: &str,
    max_hops: usize,
) -> Vec<PalaceTraversalStep> {
    let Some(start) = snapshot.nodes.get(start_room) else {
        return Vec::new();
    };

    let mut visited = BTreeSet::from([start_room.to_owned()]);
    let mut frontier = VecDeque::from([(start_room.to_owned(), 0usize)]);
    let mut steps = vec![PalaceTraversalStep {
        room: start_room.to_owned(),
        wings: start.wings.clone(),
        halls: start.halls.clone(),
        count: start.count,
        hop: 0,
        connected_via: Vec::new(),
    }];

    while let Some((current_room, depth)) = frontier.pop_front() {
        if depth >= max_hops {
            continue;
        }

        let current = snapshot.nodes.get(&current_room).expect("frontier room must exist");
        let current_wings = current.wings.iter().cloned().collect::<BTreeSet<_>>();

        for (room, node) in &snapshot.nodes {
            if visited.contains(room) {
                continue;
            }
            let shared = node
                .wings
                .iter()
                .filter(|wing| current_wings.contains(*wing))
                .cloned()
                .collect::<Vec<_>>();
            if shared.is_empty() {
                continue;
            }
            visited.insert(room.clone());
            steps.push(PalaceTraversalStep {
                room: room.clone(),
                wings: node.wings.clone(),
                halls: node.halls.clone(),
                count: node.count,
                hop: depth + 1,
                connected_via: shared,
            });
            frontier.push_back((room.clone(), depth + 1));
        }
    }

    steps.sort_by(|left, right| {
        left.hop.cmp(&right.hop).then(right.count.cmp(&left.count)).then(left.room.cmp(&right.room))
    });
    steps.truncate(50);
    steps
}

pub fn find_tunnels(
    snapshot: &PalaceGraphSnapshot,
    wing_a: Option<&str>,
    wing_b: Option<&str>,
) -> Vec<PalaceTunnel> {
    let mut tunnels = snapshot
        .tunnels
        .iter()
        .filter(|tunnel| {
            wing_a.is_none_or(|wing| tunnel.wings.iter().any(|candidate| candidate == wing))
                && wing_b.is_none_or(|wing| tunnel.wings.iter().any(|candidate| candidate == wing))
        })
        .cloned()
        .collect::<Vec<_>>();
    tunnels.sort_by(|left, right| right.count.cmp(&left.count).then(left.room.cmp(&right.room)));
    tunnels
}

pub fn persist_palace_graph<S>(
    store: &S,
    snapshot: &PalaceGraphSnapshot,
    updated_at: OffsetDateTime,
) -> Result<()>
where
    S: GraphStore,
{
    store.put_graph_document(&GraphDocument {
        graph_key: PALACE_GRAPH_KEY.to_owned(),
        payload: serde_json::to_value(snapshot)?,
        updated_at,
    })?;
    Ok(())
}

pub fn load_palace_graph<S>(store: &S) -> Result<Option<PalaceGraphSnapshot>>
where
    S: GraphStore,
{
    store
        .get_graph_document(PALACE_GRAPH_KEY)?
        .map(|document| serde_json::from_value(document.payload))
        .transpose()
        .map_err(GraphError::from)
}

pub struct KnowledgeGraphRuntime<'a, S> {
    store: &'a S,
}

impl<'a, S> KnowledgeGraphRuntime<'a, S>
where
    S: EntityRegistryStore + KnowledgeGraphStore,
{
    pub fn new(store: &'a S) -> Self {
        Self { store }
    }

    pub fn add_fact(&self, request: AddFactRequest, now: OffsetDateTime) -> Result<String> {
        let subject_id = self.ensure_entity(&request.subject, request.subject_type.clone(), now)?;
        let object_id = self.ensure_entity(&request.object, request.object_type.clone(), now)?;
        let predicate = canonicalize_label(&request.predicate);

        if let Some(existing) = self.store.find_active_fact(&subject_id, &predicate, &object_id)? {
            return Ok(existing.fact_id);
        }

        let fact_id = format!(
            "fact:{}:{}:{}:{}",
            subject_id,
            predicate,
            object_id,
            request.valid_from.map(format_date).unwrap_or_else(|| now.unix_timestamp().to_string())
        );

        self.store.upsert_fact(&KnowledgeGraphFact {
            fact_id: fact_id.clone(),
            subject_entity_id: subject_id,
            predicate,
            object_entity_id: object_id,
            valid_from: request.valid_from,
            valid_to: request.valid_to,
            confidence: request.confidence,
            source_drawer_id: request.source_drawer_id,
            source_file: request.source_file,
            created_at: now,
            updated_at: now,
        })?;

        Ok(fact_id)
    }

    pub fn invalidate(
        &self,
        subject: &str,
        predicate: &str,
        object: &str,
        ended_at: Date,
        now: OffsetDateTime,
    ) -> Result<usize> {
        let subject_id = self.resolve_entity_id(subject)?;
        let object_id = self.resolve_entity_id(object)?;
        self.store
            .invalidate_active_fact(
                &subject_id,
                &canonicalize_label(predicate),
                &object_id,
                ended_at,
                now,
            )
            .map_err(GraphError::from)
    }

    pub fn query_entity(
        &self,
        name: &str,
        as_of: Option<Date>,
        direction: QueryDirection,
    ) -> Result<Vec<KnowledgeQueryRow>> {
        let entity_id = self.resolve_entity_id(name)?;
        let facts = self.store.list_facts_for_entity(&entity_id)?;
        let entity_names = self.name_lookup_for_fact_ids(&facts, Some(entity_id.clone()))?;
        let today = OffsetDateTime::now_utc().date();
        let display_name = entity_names
            .get(&entity_id)
            .cloned()
            .unwrap_or_else(|| denormalize_entity_id(&entity_id));
        let mut rows = facts
            .into_iter()
            .filter(|fact| match as_of {
                Some(date) => is_active_on(fact, date),
                None => true,
            })
            .filter_map(|fact| {
                if fact.subject_entity_id == entity_id
                    && matches!(direction, QueryDirection::Outgoing | QueryDirection::Both)
                {
                    return Some(KnowledgeQueryRow {
                        direction: "outgoing".to_owned(),
                        subject: display_name.clone(),
                        predicate: fact.predicate.clone(),
                        object: entity_names
                            .get(&fact.object_entity_id)
                            .cloned()
                            .unwrap_or_else(|| denormalize_entity_id(&fact.object_entity_id)),
                        valid_from: fact.valid_from.map(format_date),
                        valid_to: fact.valid_to.map(format_date),
                        confidence: fact.confidence,
                        source_closet: fact
                            .source_drawer_id
                            .as_ref()
                            .map(|drawer_id| drawer_id.as_str().to_owned())
                            .or_else(|| fact.source_file.clone()),
                        current: is_active_on(&fact, today),
                    });
                }
                if fact.object_entity_id == entity_id
                    && matches!(direction, QueryDirection::Incoming | QueryDirection::Both)
                {
                    return Some(KnowledgeQueryRow {
                        direction: "incoming".to_owned(),
                        subject: entity_names
                            .get(&fact.subject_entity_id)
                            .cloned()
                            .unwrap_or_else(|| denormalize_entity_id(&fact.subject_entity_id)),
                        predicate: fact.predicate.clone(),
                        object: display_name.clone(),
                        valid_from: fact.valid_from.map(format_date),
                        valid_to: fact.valid_to.map(format_date),
                        confidence: fact.confidence,
                        source_closet: fact
                            .source_drawer_id
                            .as_ref()
                            .map(|drawer_id| drawer_id.as_str().to_owned())
                            .or_else(|| fact.source_file.clone()),
                        current: is_active_on(&fact, today),
                    });
                }
                None
            })
            .collect::<Vec<_>>();

        rows.sort_by(|left, right| {
            left.valid_from
                .cmp(&right.valid_from)
                .then(left.direction.cmp(&right.direction))
                .then(left.predicate.cmp(&right.predicate))
                .then(left.object.cmp(&right.object))
        });
        Ok(rows)
    }

    pub fn timeline(&self, entity_name: Option<&str>) -> Result<Vec<KnowledgeTimelineRow>> {
        let today = OffsetDateTime::now_utc().date();
        let anchor_entity_id = entity_name.map(|name| self.resolve_entity_id(name)).transpose()?;
        let facts = match &anchor_entity_id {
            Some(entity_id) => self.store.list_facts_for_entity(entity_id)?,
            None => self.store.list_facts_limited(100)?,
        };
        let entity_names = self.name_lookup_for_fact_ids(&facts, anchor_entity_id)?;

        let mut rows = facts
            .into_iter()
            .map(|fact| {
                let current = is_active_on(&fact, today);
                KnowledgeTimelineRow {
                    subject: entity_names
                        .get(&fact.subject_entity_id)
                        .cloned()
                        .unwrap_or_else(|| denormalize_entity_id(&fact.subject_entity_id)),
                    predicate: fact.predicate,
                    object: entity_names
                        .get(&fact.object_entity_id)
                        .cloned()
                        .unwrap_or_else(|| denormalize_entity_id(&fact.object_entity_id)),
                    valid_from: fact.valid_from.map(format_date),
                    valid_to: fact.valid_to.map(format_date),
                    current,
                }
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            left.valid_from
                .cmp(&right.valid_from)
                .then(left.predicate.cmp(&right.predicate))
                .then(left.subject.cmp(&right.subject))
                .then(left.object.cmp(&right.object))
        });
        Ok(rows)
    }

    pub fn stats(&self) -> Result<KnowledgeGraphStats> {
        let facts = self.store.list_facts()?;
        let entities = canonical_entity_count(&self.store.list_entities()?);
        let today = OffsetDateTime::now_utc().date();
        let current = facts.iter().filter(|fact| is_active_on(fact, today)).count();
        let predicates = facts
            .iter()
            .map(|fact| fact.predicate.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        Ok(KnowledgeGraphStats {
            entities,
            triples: facts.len(),
            current_facts: current,
            expired_facts: facts.len().saturating_sub(current),
            relationship_types: predicates,
        })
    }

    fn ensure_entity(
        &self,
        name: &str,
        entity_type: EntityKind,
        updated_at: OffsetDateTime,
    ) -> Result<String> {
        if let Some(existing) = self.find_registered_entity(name, Some(&entity_type))? {
            return Ok(existing);
        }

        let entity_id = entity_id(&entity_type, name);
        if self.store.get_entity(&entity_id)?.is_none() {
            let entry = RegistryEntry {
                schema_version: REGISTRY_SCHEMA_VERSION,
                name: name.to_owned(),
                canonical_name: name.to_owned(),
                entity_type: entity_type.clone(),
                source: "graph".to_owned(),
                contexts: Vec::new(),
                aliases: Vec::new(),
                relationship: None,
                confidence: 100,
                ambiguous: false,
            };
            self.store.upsert_entity(&EntityRecord {
                entity_id: entity_id.clone(),
                entity_type: entity_type.as_str().to_owned(),
                payload: serde_json::to_value(entry)?,
                updated_at,
            })?;
        }
        Ok(entity_id)
    }

    fn name_lookup_for_fact_ids(
        &self,
        facts: &[KnowledgeGraphFact],
        anchor_entity_id: Option<String>,
    ) -> Result<BTreeMap<String, String>> {
        let mut entity_ids = facts
            .iter()
            .flat_map(|fact| [fact.subject_entity_id.clone(), fact.object_entity_id.clone()])
            .collect::<BTreeSet<_>>();
        if let Some(entity_id) = anchor_entity_id {
            entity_ids.insert(entity_id);
        }

        let mut names = BTreeMap::new();
        let entity_ids = entity_ids.into_iter().collect::<Vec<_>>();
        for entity in self.store.list_entities_by_ids(&entity_ids)? {
            if let Ok(entry) = serde_json::from_value::<RegistryEntry>(entity.payload) {
                names.insert(entity.entity_id, entry.canonical_name);
            }
        }
        Ok(names)
    }

    fn resolve_entity_id(&self, name: &str) -> Result<String> {
        if let Some(entity_id) = self.find_registered_entity(name, None)? {
            return Ok(entity_id);
        }

        let canonical = canonicalize_label(name);
        let entities = self.store.list_entities()?;
        for entity in entities {
            if serde_json::from_value::<RegistryEntry>(entity.payload.clone()).is_err()
                && entity.entity_id.ends_with(&canonical)
            {
                return Ok(entity.entity_id);
            }
        }
        Err(GraphError::UnknownEntity { name: name.to_owned() })
    }

    fn find_registered_entity(
        &self,
        name: &str,
        expected_type: Option<&EntityKind>,
    ) -> Result<Option<String>> {
        for entity in self.store.list_entities()? {
            let Ok(entry) = serde_json::from_value::<RegistryEntry>(entity.payload) else {
                continue;
            };
            if expected_type.is_some_and(|kind| &entry.entity_type != kind) {
                continue;
            }
            if entry.name.eq_ignore_ascii_case(name)
                || entry.canonical_name.eq_ignore_ascii_case(name)
                || entry.aliases.iter().any(|alias| alias.eq_ignore_ascii_case(name))
            {
                return Ok(Some(entity_id(&entry.entity_type, &entry.canonical_name)));
            }
        }
        Ok(None)
    }
}

#[derive(Default)]
struct RoomAccumulator {
    wings: BTreeSet<String>,
    halls: BTreeSet<String>,
    dates: BTreeSet<String>,
    count: usize,
}

fn insert_registry_entry(map: &mut BTreeMap<String, RegistryEntry>, entry: RegistryEntry) {
    map.insert(entry.name.to_ascii_lowercase(), entry);
}

fn canonical_entity_count(entities: &[EntityRecord]) -> usize {
    entities
        .iter()
        .map(|entity| {
            serde_json::from_value::<RegistryEntry>(entity.payload.clone())
                .map(|entry| entity_id(&entry.entity_type, &entry.canonical_name))
                .unwrap_or_else(|_| entity.entity_id.clone())
        })
        .collect::<BTreeSet<_>>()
        .len()
}

fn extract_candidates(text: &str) -> Vec<String> {
    let mut names = Vec::new();
    for line in text.lines() {
        for token in line.split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '-') {
            if token.len() < 2 || !token.chars().next().is_some_and(|ch| ch.is_ascii_uppercase()) {
                continue;
            }
            if STOPWORDS.iter().any(|word| word.eq_ignore_ascii_case(token)) {
                continue;
            }
            names.push(token.trim_matches('-').to_owned());
        }

        let words = line.split_whitespace().collect::<Vec<_>>();
        for pair in words.windows(2) {
            if pair.iter().all(|word| word.chars().next().is_some_and(|ch| ch.is_ascii_uppercase()))
            {
                let candidate = format!("{} {}", sanitize_token(pair[0]), sanitize_token(pair[1]));
                if candidate
                    .split_whitespace()
                    .all(|word| !STOPWORDS.iter().any(|stop| stop.eq_ignore_ascii_case(word)))
                {
                    names.push(candidate);
                }
            }
        }
    }
    names
}

fn classify_candidate(
    name: &str,
    frequency: usize,
    lower_text: &str,
    lower_lines: &[String],
) -> EntityCandidate {
    let lower_name = name.to_ascii_lowercase();
    let (mut person_score, mut person_signals) =
        score_context_patterns(PERSON_CONTEXT_PATTERNS, &lower_name, lower_text, "person context");
    let (mut project_score, mut project_signals) = score_context_patterns(
        PROJECT_CONTEXT_PATTERNS,
        &lower_name,
        lower_text,
        "project context",
    );
    if lower_text.contains(&format!(" {}.py", lower_name))
        || lower_text.contains(&format!(" import {}", lower_name))
        || lower_text.contains(&format!(" {}-core", lower_name))
    {
        project_score += 3;
        project_signals.push("code reference".to_owned());
    }

    let mut pronoun_hits = 0usize;
    for (index, line) in lower_lines.iter().enumerate() {
        if !line.contains(&lower_name) {
            continue;
        }
        let window = lower_lines
            .iter()
            .skip(index.saturating_sub(2))
            .take(5)
            .cloned()
            .collect::<Vec<_>>()
            .join(" ");
        if PRONOUN_PATTERNS.iter().any(|pronoun| window.contains(pronoun)) {
            pronoun_hits += 1;
        }
    }
    if pronoun_hits > 0 {
        person_score += pronoun_hits * 2;
        person_signals.push(format!("pronoun nearby ({pronoun_hits}x)"));
    }

    let total = person_score + project_score;
    let (entity_type, confidence, mut signals) = if total == 0 {
        (
            EntityKind::Uncertain,
            (frequency.saturating_mul(8)).min(40) as u16,
            vec![format!("appears {frequency}x, no strong type signals")],
        )
    } else {
        let person_ratio = person_score as f32 / total as f32;
        if person_ratio >= 0.7
            && person_score >= 5
            && (signal_categories(&person_signals) >= 2 || person_signals.len() >= 2)
        {
            (EntityKind::Person, (50.0 + person_ratio * 49.0).round() as u16, person_signals)
        } else if person_ratio <= 0.3 {
            (
                EntityKind::Project,
                (50.0 + (1.0 - person_ratio) * 49.0).round() as u16,
                project_signals,
            )
        } else {
            let mut combined = person_signals;
            combined.extend(project_signals);
            combined.push("mixed signals".to_owned());
            (EntityKind::Uncertain, 50, combined)
        }
    };
    signals.truncate(3);

    EntityCandidate { name: name.to_owned(), entity_type, confidence, frequency, signals }
}

fn read_text_prefix(path: &PathBuf) -> Result<String> {
    let max_bytes = (DEFAULT_ENTITY_READ_CHARS.saturating_mul(4)) as u64;
    let mut file =
        File::open(path).map_err(|source| GraphError::Io { path: path.clone(), source })?;
    let mut bytes = Vec::new();
    file.by_ref()
        .take(max_bytes)
        .read_to_end(&mut bytes)
        .map_err(|source| GraphError::Io { path: path.clone(), source })?;

    let text = match String::from_utf8(bytes) {
        Ok(text) => text,
        Err(err) => {
            let valid_up_to = err.utf8_error().valid_up_to();
            String::from_utf8_lossy(&err.into_bytes()[..valid_up_to]).into_owned()
        }
    };

    Ok(text.chars().take(DEFAULT_ENTITY_READ_CHARS).collect())
}

fn score_context_patterns(
    patterns: &[&str],
    lower_name: &str,
    lower_text: &str,
    signal_label: &str,
) -> (usize, Vec<String>) {
    let compiled =
        patterns.iter().map(|pattern| pattern.replace("{name}", lower_name)).collect::<Vec<_>>();
    let counts = count_pattern_occurrences(&compiled, lower_text);
    let total = counts.iter().sum::<usize>() * 2;
    let signals = counts
        .into_iter()
        .filter(|count| *count > 0)
        .map(|count| format!("{signal_label} ({count}x)"))
        .collect::<Vec<_>>();
    (total, signals)
}

fn count_pattern_occurrences(patterns: &[String], haystack: &str) -> Vec<usize> {
    if patterns.is_empty() {
        return Vec::new();
    }

    let Ok(automaton) = AhoCorasick::new(patterns) else {
        return vec![0; patterns.len()];
    };

    let mut counts = vec![0; patterns.len()];
    for mat in automaton.find_iter(haystack) {
        counts[mat.pattern().as_usize()] += 1;
    }
    counts
}

fn signal_categories(signals: &[String]) -> usize {
    signals
        .iter()
        .map(|signal| {
            if signal.contains("pronoun") {
                "pronoun"
            } else if signal.contains("project") || signal.contains("code") {
                "project"
            } else {
                "person"
            }
        })
        .collect::<BTreeSet<_>>()
        .len()
}

fn disambiguate(word: &str, context: &str, entry: &RegistryEntry) -> Option<LookupResult> {
    let lower_word = word.to_ascii_lowercase();
    let lower_context = format!(" {} ", context.to_ascii_lowercase());
    let person_score = PERSON_DISAMBIGUATION_PATTERNS
        .iter()
        .filter(|pattern| lower_context.contains(&pattern.replace("{name}", &lower_word)))
        .count();
    let concept_score = CONCEPT_DISAMBIGUATION_PATTERNS
        .iter()
        .filter(|pattern| lower_context.contains(&pattern.replace("{name}", &lower_word)))
        .count();

    if person_score > concept_score {
        return Some(LookupResult {
            entity_type: entry.entity_type.clone(),
            confidence: (70 + person_score as u16 * 10).min(95),
            source: entry.source.clone(),
            canonical_name: entry.canonical_name.clone(),
            needs_disambiguation: false,
        });
    }
    if concept_score > person_score {
        return Some(LookupResult {
            entity_type: EntityKind::Concept,
            confidence: (70 + concept_score as u16 * 10).min(90),
            source: "context_disambiguated".to_owned(),
            canonical_name: word.to_owned(),
            needs_disambiguation: false,
        });
    }
    None
}

fn entity_id(entity_type: &EntityKind, name: &str) -> String {
    let kind = match entity_type {
        EntityKind::Unknown => "unknown",
        other => other.as_str(),
    };
    format!("{kind}:{}", canonicalize_label(name))
}

fn denormalize_entity_id(entity_id: &str) -> String {
    entity_id
        .split_once(':')
        .map(|(_, value)| value.replace('_', " "))
        .unwrap_or_else(|| entity_id.replace('_', " "))
}

fn canonicalize_label(value: &str) -> String {
    let mut normalized = String::new();
    let mut last_was_sep = false;
    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
            last_was_sep = false;
        } else if !last_was_sep {
            normalized.push('_');
            last_was_sep = true;
        }
    }
    normalized.trim_matches('_').to_owned()
}

fn sanitize_token(token: &str) -> String {
    token.trim_matches(|ch: char| !ch.is_ascii_alphanumeric() && ch != '-').to_owned()
}

fn format_date(date: Date) -> String {
    date.format(&time::macros::format_description!("[year]-[month]-[day]"))
        .unwrap_or_else(|_| date.to_string())
}

fn is_active_on(fact: &KnowledgeGraphFact, date: Date) -> bool {
    fact.valid_from.is_none_or(|from| from <= date) && fact.valid_to.is_none_or(|to| to >= date)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::fs;

    use async_trait::async_trait;
    use mempalace_storage::IngestManifestStore;
    use tempfile::tempdir;
    use time::macros::{date, datetime};

    use super::*;

    #[test]
    fn entity_detection_classifies_people_and_projects_without_network_access() {
        let report = detect_entities_in_texts(&[String::from(
            "Kai said we should ship MemPalace.\nKai wrote the patch.\nHi Kai.\nWe built MemPalace.\nMemPalace architecture is stable.\nInstall MemPalace with pip install MemPalace.",
        )]);

        assert_eq!(report.people[0].name, "Kai");
        assert_eq!(report.people[0].entity_type, EntityKind::Person);
        assert_eq!(report.projects[0].name, "MemPalace");
        assert_eq!(report.projects[0].entity_type, EntityKind::Project);
    }

    #[test]
    fn registry_persists_and_disambiguates_ambiguous_names() {
        let tempdir = tempdir().unwrap();
        let store =
            mempalace_storage::SqliteOperationalStore::new(tempdir.path().join("state.sqlite3"));
        store.ensure_schema().unwrap();

        let registry = EntityRegistry::seed(
            "personal",
            &[SeedPerson {
                name: "Grace".to_owned(),
                relationship: Some("friend".to_owned()),
                context: "personal".to_owned(),
            }],
            &["MemPalace".to_owned()],
            &BTreeMap::new(),
        );
        registry.persist(&store, datetime!(2026-04-12 00:00:00 UTC)).unwrap();

        let loaded = EntityRegistry::load(&store).unwrap();
        let person = loaded.lookup("Grace", "Grace said hello");
        let concept = loaded.lookup("Grace", "the grace of the design");

        assert_eq!(person.entity_type, EntityKind::Person);
        assert_eq!(concept.entity_type, EntityKind::Concept);
    }

    #[test]
    fn registry_persists_mode_and_all_aliases() {
        let tempdir = tempdir().unwrap();
        let store =
            mempalace_storage::SqliteOperationalStore::new(tempdir.path().join("state.sqlite3"));
        store.ensure_schema().unwrap();

        let registry = EntityRegistry::seed(
            "work",
            &[SeedPerson {
                name: "Katherine".to_owned(),
                relationship: Some("teammate".to_owned()),
                context: "work".to_owned(),
            }],
            &[],
            &BTreeMap::from([
                (String::from("Kat"), String::from("Katherine")),
                (String::from("Katie"), String::from("Katherine")),
            ]),
        );
        registry.persist(&store, datetime!(2026-04-12 00:00:00 UTC)).unwrap();

        let loaded = EntityRegistry::load(&store).unwrap();
        let runtime = KnowledgeGraphRuntime::new(&store);
        let stats = runtime.stats().unwrap();

        assert_eq!(loaded.mode, "work");
        assert_eq!(
            loaded.entries.iter().find(|entry| entry.name == "Katherine").unwrap().aliases,
            vec![String::from("Kat"), String::from("Katie")]
        );
        assert_eq!(runtime.resolve_entity_id("Kat").unwrap(), "person:katherine");
        assert_eq!(runtime.resolve_entity_id("Katie").unwrap(), "person:katherine");
        assert_eq!(stats.entities, 1);
    }

    #[test]
    fn derives_palace_graph_with_duplicate_safe_tunnels_and_traversal() {
        let drawers = vec![
            drawer(
                "wing_code",
                "auth-migration",
                Some("hall_discoveries"),
                Some(date!(2026 - 04 - 02)),
            ),
            drawer("wing_team", "auth-migration", Some("hall_facts"), Some(date!(2026 - 04 - 02))),
            drawer("wing_team", "auth-migration", Some("hall_facts"), Some(date!(2026 - 04 - 02))),
            drawer("wing_team", "phase0-rollout", Some("hall_events"), Some(date!(2026 - 04 - 03))),
            drawer("project_alpha", "backend", None, None),
            drawer("project_alpha", "frontend", None, None),
            drawer("strategy_convos", "launch-plan", None, None),
            drawer("wing_user", "general", None, None),
        ];

        let snapshot = derive_palace_graph(&drawers);
        let tunnels = find_tunnels(&snapshot, Some("wing_code"), Some("wing_team"));
        let traversal = traverse_graph(&snapshot, "auth-migration", 2);

        assert_eq!(snapshot.stats.total_rooms, 5);
        assert_eq!(snapshot.stats.tunnel_rooms, 1);
        assert_eq!(snapshot.stats.total_edges, 2);
        assert_eq!(tunnels[0].room, "auth-migration");
        assert_eq!(traversal[1].room, "phase0-rollout");
    }

    #[test]
    fn detects_entities_from_files() {
        let tempdir = tempdir().unwrap();
        let path = tempdir.path().join("notes.txt");
        fs::write(
            &path,
            "Kai said we should ship MemPalace.\nKai wrote the patch.\nHi Kai.\nWe built MemPalace.\nMemPalace architecture is stable.\n",
        )
        .unwrap();

        let report = detect_entities_in_files(&[path], 1).unwrap();

        assert_eq!(report.people[0].name, "Kai");
        assert_eq!(report.projects[0].name, "MemPalace");
    }

    #[test]
    fn persists_and_loads_palace_graph() {
        let tempdir = tempdir().unwrap();
        let store =
            mempalace_storage::SqliteOperationalStore::new(tempdir.path().join("state.sqlite3"));
        store.ensure_schema().unwrap();

        let snapshot = derive_palace_graph(&[
            drawer("wing_code", "auth-migration", Some("hall_a"), Some(date!(2026 - 04 - 02))),
            drawer("wing_team", "auth-migration", Some("hall_b"), Some(date!(2026 - 04 - 03))),
        ]);
        persist_palace_graph(&store, &snapshot, datetime!(2026-04-12 00:00:00 UTC)).unwrap();

        assert_eq!(load_palace_graph(&store).unwrap(), Some(snapshot));
    }

    #[test]
    fn finds_all_tunnels_when_no_filters_are_given() {
        let snapshot = derive_palace_graph(&[
            drawer("wing_code", "auth-migration", Some("hall_a"), Some(date!(2026 - 04 - 02))),
            drawer("wing_team", "auth-migration", Some("hall_b"), Some(date!(2026 - 04 - 03))),
            drawer("project_alpha", "backend", None, None),
            drawer("strategy_convos", "launch-plan", None, None),
        ]);

        let tunnels = find_tunnels(&snapshot, None, None);

        assert_eq!(tunnels.len(), snapshot.tunnels.len());
        assert_eq!(tunnels[0].room, "auth-migration");
    }

    #[test]
    fn traversal_returns_empty_for_unknown_room() {
        let snapshot = derive_palace_graph(&[drawer("wing_code", "auth-migration", None, None)]);
        assert!(traverse_graph(&snapshot, "missing-room", 2).is_empty());
    }

    #[test]
    fn persists_and_queries_temporal_knowledge_graph() {
        let tempdir = tempdir().unwrap();
        let store =
            mempalace_storage::SqliteOperationalStore::new(tempdir.path().join("state.sqlite3"));
        store.ensure_schema().unwrap();
        let runtime = KnowledgeGraphRuntime::new(&store);

        runtime
            .add_fact(
                AddFactRequest {
                    subject: "Rust Rewrite".to_owned(),
                    subject_type: EntityKind::Project,
                    predicate: "preserves".to_owned(),
                    object: "CLI Parity".to_owned(),
                    object_type: EntityKind::Concept,
                    valid_from: Some(date!(2026 - 04 - 02)),
                    valid_to: None,
                    confidence: 1.0,
                    source_drawer_id: None,
                    source_file: None,
                },
                datetime!(2026-04-02 09:00:00 UTC),
            )
            .unwrap();
        runtime
            .add_fact(
                AddFactRequest {
                    subject: "Rust Rewrite".to_owned(),
                    subject_type: EntityKind::Project,
                    predicate: "targets".to_owned(),
                    object: "Phase 1".to_owned(),
                    object_type: EntityKind::Concept,
                    valid_from: Some(date!(2026 - 04 - 03)),
                    valid_to: None,
                    confidence: 1.0,
                    source_drawer_id: None,
                    source_file: None,
                },
                datetime!(2026-04-03 09:00:00 UTC),
            )
            .unwrap();
        runtime
            .invalidate(
                "Rust Rewrite",
                "targets",
                "Phase 1",
                date!(2026 - 04 - 04),
                datetime!(2026-04-04 00:00:00 UTC),
            )
            .unwrap();

        let query = runtime.query_entity("Rust Rewrite", None, QueryDirection::Outgoing).unwrap();
        let timeline = runtime.timeline(Some("Rust Rewrite")).unwrap();
        let stats = runtime.stats().unwrap();

        assert_eq!(query.len(), 2);
        assert_eq!(query[1].valid_to.as_deref(), Some("2026-04-04"));
        assert_eq!(timeline.len(), 2);
        assert_eq!(stats.current_facts, 1);
        assert_eq!(stats.expired_facts, 1);
    }

    #[test]
    fn query_entity_preserves_string_provenance_without_drawer_id() {
        let tempdir = tempdir().unwrap();
        let store =
            mempalace_storage::SqliteOperationalStore::new(tempdir.path().join("state.sqlite3"));
        store.ensure_schema().unwrap();
        let runtime = KnowledgeGraphRuntime::new(&store);

        runtime
            .add_fact(
                AddFactRequest {
                    subject: "Rust Rewrite".to_owned(),
                    subject_type: EntityKind::Project,
                    predicate: "documents".to_owned(),
                    object: "Migration Plan".to_owned(),
                    object_type: EntityKind::Concept,
                    valid_from: Some(date!(2026 - 04 - 02)),
                    valid_to: None,
                    confidence: 1.0,
                    source_drawer_id: None,
                    source_file: Some("freeform source ref".to_owned()),
                },
                datetime!(2026-04-02 09:00:00 UTC),
            )
            .unwrap();

        let query = runtime.query_entity("Rust Rewrite", None, QueryDirection::Outgoing).unwrap();

        assert_eq!(query.len(), 1);
        assert_eq!(query[0].source_closet.as_deref(), Some("freeform source ref"));
    }

    #[test]
    fn future_dated_facts_are_not_marked_current() {
        let tempdir = tempdir().unwrap();
        let store =
            mempalace_storage::SqliteOperationalStore::new(tempdir.path().join("state.sqlite3"));
        store.ensure_schema().unwrap();
        let runtime = KnowledgeGraphRuntime::new(&store);
        let today = OffsetDateTime::now_utc().date();
        let tomorrow = today.next_day().unwrap();

        runtime
            .add_fact(
                AddFactRequest {
                    subject: "Rust Rewrite".to_owned(),
                    subject_type: EntityKind::Project,
                    predicate: "starts".to_owned(),
                    object: "Phase 7".to_owned(),
                    object_type: EntityKind::Concept,
                    valid_from: Some(tomorrow),
                    valid_to: None,
                    confidence: 1.0,
                    source_drawer_id: None,
                    source_file: None,
                },
                OffsetDateTime::now_utc(),
            )
            .unwrap();

        let query = runtime.query_entity("Rust Rewrite", None, QueryDirection::Outgoing).unwrap();
        let timeline = runtime.timeline(None).unwrap();
        let stats = runtime.stats().unwrap();

        assert_eq!(query.len(), 1);
        assert!(!query[0].current);
        assert_eq!(timeline.len(), 1);
        assert!(!timeline[0].current);
        assert_eq!(stats.current_facts, 0);
        assert_eq!(stats.expired_facts, 1);
    }

    #[test]
    fn knowledge_graph_uses_canonical_entity_ids_for_aliases() {
        let tempdir = tempdir().unwrap();
        let store =
            mempalace_storage::SqliteOperationalStore::new(tempdir.path().join("state.sqlite3"));
        store.ensure_schema().unwrap();

        let registry = EntityRegistry::seed(
            "personal",
            &[SeedPerson {
                name: "Katherine".to_owned(),
                relationship: Some("teammate".to_owned()),
                context: "work".to_owned(),
            }],
            &[],
            &BTreeMap::from([(String::from("Kat"), String::from("Katherine"))]),
        );
        registry.persist(&store, datetime!(2026-04-12 00:00:00 UTC)).unwrap();

        let runtime = KnowledgeGraphRuntime::new(&store);
        runtime
            .add_fact(
                AddFactRequest {
                    subject: "Kat".to_owned(),
                    subject_type: EntityKind::Person,
                    predicate: "supports".to_owned(),
                    object: "MemPalace".to_owned(),
                    object_type: EntityKind::Project,
                    valid_from: Some(date!(2026 - 04 - 12)),
                    valid_to: None,
                    confidence: 0.9,
                    source_drawer_id: None,
                    source_file: None,
                },
                datetime!(2026-04-12 09:00:00 UTC),
            )
            .unwrap();

        let facts = store.list_facts().unwrap();
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].subject_entity_id, "person:katherine");

        let query = runtime.query_entity("Kat", None, QueryDirection::Outgoing).unwrap();
        assert_eq!(query.len(), 1);
        assert_eq!(query[0].subject, "Katherine");
    }

    #[test]
    fn querying_unknown_entities_returns_error() {
        let tempdir = tempdir().unwrap();
        let store =
            mempalace_storage::SqliteOperationalStore::new(tempdir.path().join("state.sqlite3"));
        store.ensure_schema().unwrap();
        let runtime = KnowledgeGraphRuntime::new(&store);

        let err = runtime.query_entity("Missing", None, QueryDirection::Outgoing).unwrap_err();
        assert!(matches!(err, GraphError::UnknownEntity { .. }));
    }

    #[test]
    fn global_timeline_is_limited_to_one_hundred_rows() {
        let tempdir = tempdir().unwrap();
        let store =
            mempalace_storage::SqliteOperationalStore::new(tempdir.path().join("state.sqlite3"));
        store.ensure_schema().unwrap();
        let runtime = KnowledgeGraphRuntime::new(&store);

        for index in 0..101 {
            runtime
                .add_fact(
                    AddFactRequest {
                        subject: format!("Project {index:03}"),
                        subject_type: EntityKind::Project,
                        predicate: "references".to_owned(),
                        object: format!("Concept {index:03}"),
                        object_type: EntityKind::Concept,
                        valid_from: Some(date!(2026 - 04 - 01)),
                        valid_to: None,
                        confidence: 1.0,
                        source_drawer_id: None,
                        source_file: None,
                    },
                    datetime!(2026-04-01 09:00:00 UTC),
                )
                .unwrap();
        }

        let rows = runtime.timeline(None).unwrap();

        assert_eq!(rows.len(), 100);
        assert_eq!(rows[0].subject, "Project 000");
    }

    #[test]
    fn stats_handles_empty_store() {
        let tempdir = tempdir().unwrap();
        let store =
            mempalace_storage::SqliteOperationalStore::new(tempdir.path().join("state.sqlite3"));
        store.ensure_schema().unwrap();
        let runtime = KnowledgeGraphRuntime::new(&store);

        assert_eq!(
            runtime.stats().unwrap(),
            KnowledgeGraphStats {
                entities: 0,
                triples: 0,
                current_facts: 0,
                expired_facts: 0,
                relationship_types: Vec::new(),
            }
        );
    }

    #[tokio::test]
    async fn derives_graph_from_drawer_store() {
        struct MemoryStore(Vec<DrawerRecord>);

        #[async_trait]
        impl DrawerStore for MemoryStore {
            async fn ensure_schema(&self) -> mempalace_storage::Result<()> {
                Ok(())
            }
            async fn put_drawers(
                &self,
                _drawers: &[DrawerRecord],
                _strategy: mempalace_storage::DuplicateStrategy,
            ) -> mempalace_storage::Result<()> {
                Ok(())
            }
            async fn get_drawer(
                &self,
                _id: &DrawerId,
            ) -> mempalace_storage::Result<Option<DrawerRecord>> {
                Ok(None)
            }
            async fn delete_drawers(&self, _ids: &[DrawerId]) -> mempalace_storage::Result<usize> {
                Ok(0)
            }
            async fn search_drawers(
                &self,
                _request: &mempalace_storage::SearchRequest,
            ) -> mempalace_storage::Result<Vec<mempalace_storage::DrawerMatch>> {
                Ok(Vec::new())
            }
            async fn list_drawers(
                &self,
                _filter: &DrawerFilter,
            ) -> mempalace_storage::Result<Vec<DrawerRecord>> {
                Ok(self.0.clone())
            }
        }

        let store = MemoryStore(vec![drawer(
            "wing_code",
            "auth-migration",
            Some("hall_facts"),
            Some(date!(2026 - 04 - 02)),
        )]);
        let snapshot = derive_palace_graph_from_store(&store).await.unwrap();
        assert_eq!(snapshot.stats.total_rooms, 1);
    }

    fn drawer(wing: &str, room: &str, hall: Option<&str>, date: Option<Date>) -> DrawerRecord {
        DrawerRecord {
            id: DrawerId::new(format!("{wing}/{room}/0")).unwrap(),
            wing: wing.try_into().unwrap(),
            room: room.try_into().unwrap(),
            hall: hall.map(str::to_owned),
            date,
            source_file: "fixture.txt".to_owned(),
            chunk_index: 0,
            ingest_mode: "tests".to_owned(),
            extract_mode: None,
            added_by: "tests".to_owned(),
            filed_at: datetime!(2026-04-12 00:00:00 UTC),
            importance: None,
            emotional_weight: None,
            weight: None,
            content: "payload".to_owned(),
            content_hash: "hash".to_owned(),
            embedding: Vec::new(),
        }
    }
}

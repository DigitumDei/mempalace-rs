use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, params, params_from_iter};
use time::{Duration, OffsetDateTime};

use crate::error::{Result, StorageError};
use crate::types::{
    AgentLineageRecord, ConfigEntry, EntityRecord, GraphDocument, IngestFileRecord,
    IngestManifestEntry, IngestRun, IngestRunStatus, KnowledgeGraphFact, LineageMigrationRecord,
    RetryableRun, RevisionedWrite, SelfObservationRecord, SelfObservationScope,
    SelfObservationStatus, ToolStateEntry,
};
use mempalace_core::{DIARY_SUMMARY_MAX_CHARS, DrawerId};
use time::Date;

const MIGRATIONS: &[(&str, &str)] = &[
    (
        "0001_initial_storage",
        r#"
CREATE TABLE IF NOT EXISTS migrations (
    version TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS config (
    config_key TEXT PRIMARY KEY,
    config_value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingest_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ingest_kind TEXT NOT NULL,
    source_key TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    failed_reason TEXT,
    UNIQUE(ingest_kind, source_key, created_at)
);

CREATE TABLE IF NOT EXISTS ingest_manifests (
    run_id INTEGER NOT NULL,
    drawer_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY (run_id, drawer_id),
    FOREIGN KEY (run_id) REFERENCES ingest_runs(id)
);

CREATE TABLE IF NOT EXISTS ingest_files (
    source_key TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    last_ingested_at TEXT NOT NULL,
    ingest_kind TEXT NOT NULL,
    drawer_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS entity_registry (
    entity_id TEXT PRIMARY KEY,
    entity_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS graph_state (
    graph_key TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tool_state (
    tool_name TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
    "#,
    ),
    (
        "0002_ingest_files_source_key",
        r#"
ALTER TABLE ingest_files RENAME TO ingest_files_old;

CREATE TABLE ingest_files (
    source_key TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    last_ingested_at TEXT NOT NULL,
    ingest_kind TEXT NOT NULL,
    drawer_count INTEGER NOT NULL
);

INSERT INTO ingest_files (source_key, source_file, content_hash, last_ingested_at, ingest_kind, drawer_count)
SELECT ingest_kind || ':' || source_file, source_file, content_hash, last_ingested_at, ingest_kind, drawer_count
FROM ingest_files_old;

DROP TABLE ingest_files_old;
        "#,
    ),
    (
        "0003_knowledge_graph_facts",
        r#"
CREATE TABLE IF NOT EXISTS knowledge_graph_facts (
    fact_id TEXT PRIMARY KEY,
    subject_entity_id TEXT NOT NULL,
    predicate TEXT NOT NULL,
    object_entity_id TEXT NOT NULL,
    valid_from TEXT,
    valid_to TEXT,
    confidence REAL NOT NULL,
    source_drawer_id TEXT,
    source_file TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_kg_facts_subject ON knowledge_graph_facts(subject_entity_id);
CREATE INDEX IF NOT EXISTS idx_kg_facts_object ON knowledge_graph_facts(object_entity_id);
CREATE INDEX IF NOT EXISTS idx_kg_facts_predicate ON knowledge_graph_facts(predicate);
CREATE INDEX IF NOT EXISTS idx_kg_facts_validity ON knowledge_graph_facts(valid_from, valid_to);
        "#,
    ),
    (
        "0004_knowledge_graph_fact_lookup_index",
        r#"
CREATE INDEX IF NOT EXISTS idx_kg_facts_subject_predicate_object_active
ON knowledge_graph_facts(subject_entity_id, predicate, object_entity_id, valid_to);
        "#,
    ),
    (
        "0005_change_log",
        r#"
CREATE TABLE IF NOT EXISTS change_log (
    change_id   TEXT PRIMARY KEY,
    event_type  TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    actor       TEXT,
    details_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_change_log_occurred_at ON change_log(occurred_at ASC);
        "#,
    ),
    (
        "0006_diary_summaries",
        r#"
CREATE TABLE IF NOT EXISTS diary_summaries (
    entry_id TEXT PRIMARY KEY,
    summary TEXT NOT NULL
);
        "#,
    ),
    (
        "0007_diary_summary_length_constraint",
        r#"
CREATE TABLE IF NOT EXISTS diary_summaries_v2 (
    entry_id TEXT PRIMARY KEY,
    summary TEXT NOT NULL CHECK(length(summary) <= 400)
);

INSERT INTO diary_summaries_v2 (entry_id, summary)
SELECT entry_id, SUBSTR(summary, 1, 400) FROM diary_summaries;

        DROP TABLE diary_summaries;

ALTER TABLE diary_summaries_v2 RENAME TO diary_summaries;
        "#,
    ),
    (
        "0008_maintenance_leases",
        r#"
CREATE TABLE IF NOT EXISTS maintenance_leases (
    lease_id   TEXT PRIMARY KEY,
    holder     TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

INSERT INTO maintenance_leases (lease_id, holder, expires_at, updated_at)
VALUES ('maintenance', '', '1970-01-01T00:00:00Z', '1970-01-01T00:00:00Z');
        "#,
    ),
    (
        "0009_agent_lineages",
        r#"
CREATE TABLE IF NOT EXISTS agent_lineages (
    lineage_id   TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    description  TEXT NOT NULL,
    revision     INTEGER NOT NULL CHECK(revision >= 1),
    is_default   INTEGER NOT NULL CHECK(is_default IN (0, 1)),
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_lineages_default
ON agent_lineages(is_default) WHERE is_default = 1;

CREATE TABLE IF NOT EXISTS self_observations (
    observation_id             TEXT PRIMARY KEY,
    lineage_id                 TEXT NOT NULL,
    status                     TEXT NOT NULL CHECK(status IN ('candidate', 'promoted', 'superseded', 'retired')),
    scope                      TEXT NOT NULL CHECK(scope IN ('lineage', 'shared', 'engine')),
    statement                  TEXT NOT NULL,
    behavioral_consequence     TEXT NOT NULL,
    confidence                 REAL NOT NULL CHECK(confidence >= 0.0 AND confidence <= 1.0),
    author                     TEXT NOT NULL,
    model                      TEXT,
    harness                    TEXT,
    evidence_json              TEXT NOT NULL,
    counterevidence_json       TEXT NOT NULL,
    supersedes_observation_id  TEXT,
    revision                   INTEGER NOT NULL CHECK(revision >= 1),
    created_at                 TEXT NOT NULL,
    updated_at                 TEXT NOT NULL,
    FOREIGN KEY (lineage_id) REFERENCES agent_lineages(lineage_id),
    FOREIGN KEY (supersedes_observation_id) REFERENCES self_observations(observation_id)
);

CREATE INDEX IF NOT EXISTS idx_self_observations_lineage_status
ON self_observations(lineage_id, status, updated_at DESC);

CREATE TABLE IF NOT EXISTS self_observation_reviews (
    review_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    observation_id  TEXT NOT NULL,
    revision        INTEGER NOT NULL,
    prior_status    TEXT NOT NULL,
    new_status      TEXT NOT NULL,
    reviewer        TEXT NOT NULL,
    reason          TEXT NOT NULL,
    reviewed_at     TEXT NOT NULL,
    FOREIGN KEY (observation_id) REFERENCES self_observations(observation_id)
);

CREATE TABLE IF NOT EXISTS lineage_migrations (
    migration_id       TEXT PRIMARY KEY,
    lineage_id         TEXT NOT NULL,
    from_model         TEXT,
    from_harness       TEXT,
    to_model           TEXT NOT NULL,
    to_harness         TEXT NOT NULL,
    summary            TEXT NOT NULL,
    continuities_json  TEXT NOT NULL,
    changes_json       TEXT NOT NULL,
    evidence_json      TEXT NOT NULL,
    author             TEXT NOT NULL,
    created_at         TEXT NOT NULL,
    FOREIGN KEY (lineage_id) REFERENCES agent_lineages(lineage_id)
);

CREATE INDEX IF NOT EXISTS idx_lineage_migrations_lineage_created
ON lineage_migrations(lineage_id, created_at DESC);
        "#,
    ),
];

pub trait IngestManifestStore {
    fn ensure_schema(&self) -> Result<()>;
    fn create_pending_run(
        &self,
        ingest_kind: &str,
        source_key: &str,
        entries: &[IngestManifestEntry],
        created_at: OffsetDateTime,
    ) -> Result<IngestRun>;
    fn mark_run_committed(
        &self,
        run_id: i64,
        source_key: &str,
        source_file: &str,
        content_hash: &str,
        drawer_count: usize,
        committed_at: OffsetDateTime,
    ) -> Result<()>;
    fn stale_pending_runs(&self, older_than: OffsetDateTime) -> Result<Vec<RetryableRun>>;
    fn mark_run_failed(&self, run_id: i64, reason: &str, failed_at: OffsetDateTime) -> Result<()>;
    fn committed_drawer_ids(&self) -> Result<Vec<DrawerId>>;
    fn committed_drawer_ids_for_source_key(&self, source_key: &str) -> Result<Vec<DrawerId>>;
    /// Live (latest committed run) drawer ids for every key in `source_keys`,
    /// gathered over a single connection. Used by scoped prune to avoid a fresh
    /// connection per source.
    fn committed_drawer_ids_for_source_keys(&self, source_keys: &[String])
    -> Result<Vec<DrawerId>>;
    fn get_ingested_file(&self, source_key: &str) -> Result<Option<IngestFileRecord>>;
    fn ingested_source_keys_with_prefix(&self, prefix: &str) -> Result<Vec<String>>;
    /// Return `(source_key, drawer_count)` for every mined source whose key
    /// begins with `prefix`. Read-only; used to preview a scoped prune.
    fn ingested_source_counts_with_prefix(&self, prefix: &str) -> Result<Vec<(String, i64)>>;
    /// Remove all ingest metadata for a source key.
    fn delete_source_key(&self, source_key: &str) -> Result<()>;
    /// Remove all ingest metadata for every key in `source_keys`, in one
    /// transaction over a single connection. Used by scoped prune.
    fn delete_source_keys(&self, source_keys: &[String]) -> Result<()>;
}

pub trait EntityRegistryStore {
    fn upsert_entity(&self, entity: &EntityRecord) -> Result<()>;
    fn get_entity(&self, entity_id: &str) -> Result<Option<EntityRecord>>;
    fn list_entities(&self) -> Result<Vec<EntityRecord>>;
    fn list_entities_by_ids(&self, entity_ids: &[String]) -> Result<Vec<EntityRecord>>;
}

pub trait GraphStore {
    fn put_graph_document(&self, graph: &GraphDocument) -> Result<()>;
    fn get_graph_document(&self, graph_key: &str) -> Result<Option<GraphDocument>>;
}

pub trait KnowledgeGraphStore {
    fn upsert_fact(&self, fact: &KnowledgeGraphFact) -> Result<()>;
    fn get_fact(&self, fact_id: &str) -> Result<Option<KnowledgeGraphFact>>;
    fn list_facts(&self) -> Result<Vec<KnowledgeGraphFact>>;
    fn list_facts_limited(&self, limit: usize) -> Result<Vec<KnowledgeGraphFact>>;
    fn list_facts_for_entity(&self, entity_id: &str) -> Result<Vec<KnowledgeGraphFact>>;
    fn find_active_fact(
        &self,
        subject_entity_id: &str,
        predicate: &str,
        object_entity_id: &str,
    ) -> Result<Option<KnowledgeGraphFact>>;
    fn invalidate_active_fact(
        &self,
        subject_entity_id: &str,
        predicate: &str,
        object_entity_id: &str,
        ended_at: Date,
        updated_at: OffsetDateTime,
    ) -> Result<usize>;
}

pub trait ToolStateStore {
    fn put_tool_state(&self, state: &ToolStateEntry) -> Result<()>;
    fn get_tool_state(&self, tool_name: &str) -> Result<Option<ToolStateEntry>>;
    fn put_config(&self, entry: &ConfigEntry) -> Result<()>;
    fn get_config(&self, key: &str) -> Result<Option<ConfigEntry>>;
}

/// Local operational storage for agent lineages, reviewed self-observations, and migrations.
pub trait SelfModelStore {
    /// Create or revise a lineage using an expected optimistic-concurrency revision.
    fn set_lineage(
        &self,
        lineage_id: &str,
        display_name: &str,
        description: &str,
        set_default: bool,
        expected_revision: Option<i64>,
        now: OffsetDateTime,
    ) -> Result<RevisionedWrite<AgentLineageRecord>>;
    /// Load a lineage by stable identifier.
    fn get_lineage(&self, lineage_id: &str) -> Result<Option<AgentLineageRecord>>;
    /// Load the lineage selected for wake-up when none is explicitly requested.
    fn get_default_lineage(&self) -> Result<Option<AgentLineageRecord>>;
    /// List all lineages with the default first.
    fn list_lineages(&self) -> Result<Vec<AgentLineageRecord>>;
    /// Insert a new candidate self-observation.
    fn propose_self_observation(&self, observation: &SelfObservationRecord) -> Result<()>;
    /// Load a self-observation by identifier.
    fn get_self_observation(
        &self,
        observation_id: &str,
    ) -> Result<Option<SelfObservationRecord>>;
    /// List recent observations for a lineage, optionally filtered by lifecycle state.
    fn list_self_observations(
        &self,
        lineage_id: &str,
        statuses: &[SelfObservationStatus],
        limit: usize,
    ) -> Result<Vec<SelfObservationRecord>>;
    /// List recent observations with an applicability-scope filter applied before the limit.
    ///
    /// The default implementation preserves compatibility for other stores by filtering the
    /// unscoped result in memory. SQLite overrides it to apply the predicate in SQL.
    fn list_self_observations_scoped(
        &self,
        lineage_id: &str,
        statuses: &[SelfObservationStatus],
        scope: Option<SelfObservationScope>,
        limit: usize,
    ) -> Result<Vec<SelfObservationRecord>> {
        Ok(self
            .list_self_observations(lineage_id, statuses, usize::MAX)?
            .into_iter()
            .filter(|record| match scope {
                Some(scope) => record.scope == scope,
                None => true,
            })
            .take(limit)
            .collect())
    }
    /// Apply a revision-checked review transition and append its audit record.
    fn review_self_observation(
        &self,
        observation_id: &str,
        expected_revision: i64,
        new_status: SelfObservationStatus,
        reviewer: &str,
        reason: &str,
        now: OffsetDateTime,
    ) -> Result<RevisionedWrite<SelfObservationRecord>>;
    /// Insert an evidence-backed model or harness migration record.
    fn record_lineage_migration(&self, migration: &LineageMigrationRecord) -> Result<()>;
    /// List recent migrations for a lineage.
    fn list_lineage_migrations(
        &self,
        lineage_id: &str,
        limit: usize,
    ) -> Result<Vec<LineageMigrationRecord>>;
}

/// An opaque cursor that identifies a position in the change log for stable
/// forward pagination.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangeCursor {
    /// RFC 3339 `occurred_at` value of the last row on the previous page.
    pub occurred_at: OffsetDateTime,
    /// SQLite rowid of the last row on the previous page, used as a tiebreaker
    /// when multiple events share the same `occurred_at`.
    pub rowid: i64,
}

/// A page of change events returned by [`ChangeLogStore::get_changes_page`].
#[derive(Debug, Clone, PartialEq)]
pub struct ChangePage {
    /// Change events for this page, ordered `(occurred_at ASC, rowid ASC)`.
    pub events: Vec<ChangeEvent>,
    /// Cursor to pass on the next call to continue pagination; `None` when the
    /// last page has been reached.
    pub next_cursor: Option<ChangeCursor>,
}

/// A single write event recorded in the change log.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangeEvent {
    /// Operation type: "drawer_added", "drawer_deleted", "diary_written",
    /// "kg_fact_added", or "kg_fact_invalidated".
    pub event_type: String,
    pub occurred_at: OffsetDateTime,
    /// Primary identifier of the affected entity (drawer id, fact triple, etc.).
    pub entity_id: String,
    /// The agent or tool that performed the write, if known.
    pub actor: Option<String>,
    /// Optional JSON with extra context (wing, room, subject/predicate/object, …).
    pub details_json: Option<String>,
}

pub trait ChangeLogStore {
    fn append_event(&self, event: &ChangeEvent) -> Result<()>;
    /// Whether the change log already contains at least one event with the given
    /// `entity_id` and `event_type`. Used to make crash-window recovery idempotent: a handler
    /// restores a lost side-effect event only when none already exists.
    fn event_exists(&self, entity_id: &str, event_type: &str) -> Result<bool>;
    fn get_changes_since(&self, since: OffsetDateTime, limit: usize) -> Result<Vec<ChangeEvent>>;
    fn get_recent_changes(&self, limit: usize) -> Result<Vec<ChangeEvent>>;
    /// Cursor-paginated forward scan of the change log.
    ///
    /// - `since`: if provided, only return events with `occurred_at >= since`.
    ///   Ignored when a `cursor` is provided (the cursor already encodes the
    ///   position).
    /// - `cursor`: opaque cursor from the previous [`ChangePage::next_cursor`].
    /// - `limit`: maximum events per page (exclusive; the implementation fetches
    ///   `limit + 1` internally to detect whether another page exists).
    fn get_changes_page(
        &self,
        since: Option<OffsetDateTime>,
        cursor: Option<ChangeCursor>,
        limit: usize,
    ) -> Result<ChangePage>;
}

/// Advisory lease for the maintenance subsystem.
///
/// A single-row lease table serialises concurrent maintenance-worker
/// processes so that only one holder runs at a time.  Expired leases can
/// be reclaimed by any contender.
pub trait MaintenanceLeaseStore {
    /// Claim the maintenance lease atomically.
    ///
    /// Returns `Ok(true)` when the lease was acquired (either no prior
    /// lease existed, it had expired, or the caller already held it).
    /// Returns `Ok(false)` when another process holds a valid lease —
    /// the caller should treat this as a non-error skip.
    fn try_claim_lease(&self, holder: &str, ttl: Duration) -> Result<bool>;

    /// Extend the lease TTL.
    ///
    /// Returns `Ok(true)` if the lease was extended.  Returns `Ok(false)`
    /// if the caller no longer holds the lease.
    fn renew_lease(&self, holder: &str, ttl: Duration) -> Result<bool>;

    /// Release the lease.
    ///
    /// Returns `Ok(true)` if the lease was released.  Returns `Ok(false)`
    /// if the caller did not hold the lease.
    fn release_lease(&self, holder: &str) -> Result<bool>;

    /// Read the current lease, if one exists.
    fn lease_status(&self) -> Result<Option<(String, OffsetDateTime)>>;
}

#[derive(Debug, Clone)]
pub struct SqliteOperationalStore {
    path: PathBuf,
}

impl SqliteOperationalStore {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self { path: path.as_ref().to_path_buf() }
    }

    pub fn migration_names() -> &'static [&'static str] {
        &[
            "0001_initial_storage",
            "0002_ingest_files_source_key",
            "0003_knowledge_graph_facts",
            "0004_knowledge_graph_fact_lookup_index",
            "0005_change_log",
            "0006_diary_summaries",
            "0007_diary_summary_length_constraint",
            "0008_maintenance_leases",
            "0009_agent_lineages",
        ]
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn ensure_schema_with_migrations(&self, migrations: &[(&str, &str)]) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|source| StorageError::Io { path: parent.to_path_buf(), source })?;
        }

        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE IF NOT EXISTS migrations (
                 version TEXT PRIMARY KEY,
                 applied_at TEXT NOT NULL
             );",
        )?;

        for (version, sql) in migrations {
            let already_applied = transaction
                .query_row("SELECT 1 FROM migrations WHERE version = ?1", [version], |_| Ok(()))
                .optional()?
                .is_some();

            if already_applied {
                continue;
            }

            transaction.execute_batch(sql)?;
            transaction.execute(
                "INSERT INTO migrations (version, applied_at) VALUES (?1, ?2)",
                params![
                    version,
                    OffsetDateTime::now_utc()
                        .format(&time::format_description::well_known::Rfc3339)
                        .map_err(|err| StorageError::Invariant(err.to_string()))?
                ],
            )?;
        }

        transaction.commit()?;
        Ok(())
    }

    fn open_connection(&self) -> Result<Connection> {
        let connection = Connection::open(&self.path)?;
        connection.execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;",
        )?;
        Ok(connection)
    }

    /// Open a connection configured for case-sensitive `LIKE`.
    ///
    /// SQLite's default `LIKE` is ASCII case-insensitive, but source-key prefix
    /// matching must be literal — otherwise pruning branch view `Feature` would
    /// also match, and delete, view `feature`. The exact-key (`=`) paths already
    /// compare under the case-sensitive BINARY collation; the prefix queries
    /// used by prune must agree, so they open through here.
    fn open_prefix_connection(&self) -> Result<Connection> {
        let connection = self.open_connection()?;
        connection.execute_batch("PRAGMA case_sensitive_like = ON;")?;
        Ok(connection)
    }

    fn open_lease_connection(&self) -> Result<Connection> {
        let connection = Connection::open(&self.path)?;
        connection.execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA busy_timeout = 0;",
        )?;
        Ok(connection)
    }
}

/// Persistent summaries for diary entries. Diary content remains in the drawer
/// store; keeping summaries here avoids rewriting existing Lance rows.
pub trait DiaryStore {
    fn store_diary_summary(&self, entry_id: &DrawerId, summary: &str) -> Result<()>;
    fn get_diary_summary(&self, entry_id: &DrawerId) -> Result<Option<String>>;
    fn get_diary_summaries(&self, entry_ids: &[DrawerId]) -> Result<Vec<(DrawerId, String)>>;
    fn delete_diary_summary(&self, entry_id: &DrawerId) -> Result<()>;
}

impl DiaryStore for SqliteOperationalStore {
    fn store_diary_summary(&self, entry_id: &DrawerId, summary: &str) -> Result<()> {
        if summary.chars().count() > DIARY_SUMMARY_MAX_CHARS {
            return Err(StorageError::Invariant(format!(
                "diary summary exceeds maximum length of {DIARY_SUMMARY_MAX_CHARS} characters"
            )));
        }
        let connection = self.open_connection()?;
        connection.execute(
            "INSERT INTO diary_summaries (entry_id, summary) VALUES (?1, ?2)
             ON CONFLICT(entry_id) DO UPDATE SET summary = excluded.summary",
            params![entry_id.as_str(), summary],
        )?;
        Ok(())
    }

    fn get_diary_summary(&self, entry_id: &DrawerId) -> Result<Option<String>> {
        let connection = self.open_connection()?;
        connection
            .query_row(
                "SELECT summary FROM diary_summaries WHERE entry_id = ?1",
                [entry_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    fn get_diary_summaries(&self, entry_ids: &[DrawerId]) -> Result<Vec<(DrawerId, String)>> {
        if entry_ids.is_empty() {
            return Ok(Vec::new());
        }
        let connection = self.open_connection()?;
        let placeholders = std::iter::repeat_n("?", entry_ids.len()).collect::<Vec<_>>().join(", ");
        let sql = format!(
            "SELECT entry_id, summary FROM diary_summaries WHERE entry_id IN ({placeholders})"
        );
        let mut statement = connection.prepare(&sql)?;
        let params: Vec<&str> = entry_ids.iter().map(DrawerId::as_str).collect();
        let rows = statement
            .query_map(params_from_iter(params), |row| {
                let raw: String = row.get(0)?;
                let entry_id = DrawerId::new(raw).map_err(|err| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(err),
                    )
                })?;
                let summary: String = row.get(1)?;
                Ok((entry_id, summary))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    fn delete_diary_summary(&self, entry_id: &DrawerId) -> Result<()> {
        let connection = self.open_connection()?;
        connection
            .execute("DELETE FROM diary_summaries WHERE entry_id = ?1", [entry_id.as_str()])?;
        Ok(())
    }
}

impl SqliteOperationalStore {
    /// Create a pending ingest run and store the diary summary in a single
    /// SQLite transaction.  This ensures that a crash before the Lance drawer
    /// write still leaves a recoverable pending run alongside the summary,
    /// rather than an orphaned summary with no run at all.
    pub fn create_pending_diary_run(
        &self,
        ingest_kind: &str,
        source_key: &str,
        entries: &[IngestManifestEntry],
        drawer_id: &DrawerId,
        summary: &str,
        created_at: OffsetDateTime,
    ) -> Result<Option<IngestRun>> {
        if summary.chars().count() > DIARY_SUMMARY_MAX_CHARS {
            return Err(StorageError::Invariant(format!(
                "diary summary exceeds maximum length of {DIARY_SUMMARY_MAX_CHARS} characters"
            )));
        }
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let timestamp = encode_time(created_at);

        transaction.execute(
            "INSERT INTO ingest_runs (ingest_kind, source_key, status, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?4)",
            params![ingest_kind, source_key, IngestRunStatus::Pending.as_str(), timestamp],
        )?;
        let run_id = transaction.last_insert_rowid();

        for entry in entries {
            transaction.execute(
                "INSERT INTO ingest_manifests (run_id, drawer_id, source_file, content_hash, status)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    run_id,
                    entry.drawer_id.as_str(),
                    entry.source_file,
                    entry.content_hash,
                    IngestRunStatus::Pending.as_str()
                ],
            )?;
        }

        let summary_inserted = transaction.execute(
            "INSERT INTO diary_summaries (entry_id, summary) VALUES (?1, ?2)
             ON CONFLICT(entry_id) DO NOTHING",
            params![drawer_id.as_str(), summary],
        )?;

        // The summary's unique entry ID is the atomic duplicate gate for a
        // diary ingest.  Roll back the run and manifest rows we staged above
        // when another writer already owns this entry, rather than allowing a
        // second writer to reach LanceDB with the same drawer ID.
        if summary_inserted == 0 {
            transaction.rollback()?;
            return Ok(None);
        }

        transaction.commit()?;
        Ok(Some(IngestRun {
            id: run_id,
            ingest_kind: ingest_kind.to_owned(),
            source_key: source_key.to_owned(),
            status: IngestRunStatus::Pending,
            created_at,
            updated_at: created_at,
            failed_reason: None,
        }))
    }
}

/// Build a `LIKE` pattern that matches any string starting with `prefix`,
/// escaping the LIKE metacharacters (`%`, `_`, `\`) so the caller-supplied
/// prefix is treated literally. Pair with `ESCAPE '\'` in the query.
fn like_prefix_pattern(prefix: &str) -> String {
    let mut pattern = String::with_capacity(prefix.len() + 1);
    for ch in prefix.chars() {
        if matches!(ch, '%' | '_' | '\\') {
            pattern.push('\\');
        }
        pattern.push(ch);
    }
    pattern.push('%');
    pattern
}

impl IngestManifestStore for SqliteOperationalStore {
    fn ensure_schema(&self) -> Result<()> {
        self.ensure_schema_with_migrations(MIGRATIONS)
    }

    fn create_pending_run(
        &self,
        ingest_kind: &str,
        source_key: &str,
        entries: &[IngestManifestEntry],
        created_at: OffsetDateTime,
    ) -> Result<IngestRun> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let timestamp = encode_time(created_at);
        transaction.execute(
            "INSERT INTO ingest_runs (ingest_kind, source_key, status, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?4)",
            params![ingest_kind, source_key, IngestRunStatus::Pending.as_str(), timestamp],
        )?;
        let run_id = transaction.last_insert_rowid();

        for entry in entries {
            transaction.execute(
                "INSERT INTO ingest_manifests (run_id, drawer_id, source_file, content_hash, status)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    run_id,
                    entry.drawer_id.as_str(),
                    entry.source_file,
                    entry.content_hash,
                    IngestRunStatus::Pending.as_str()
                ],
            )?;
        }

        transaction.commit()?;
        Ok(IngestRun {
            id: run_id,
            ingest_kind: ingest_kind.to_owned(),
            source_key: source_key.to_owned(),
            status: IngestRunStatus::Pending,
            created_at,
            updated_at: created_at,
            failed_reason: None,
        })
    }

    fn mark_run_committed(
        &self,
        run_id: i64,
        source_key: &str,
        source_file: &str,
        content_hash: &str,
        drawer_count: usize,
        committed_at: OffsetDateTime,
    ) -> Result<()> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let timestamp = encode_time(committed_at);

        transaction.execute(
            "UPDATE ingest_manifests SET status = ?2 WHERE run_id = ?1",
            params![run_id, IngestRunStatus::Committed.as_str()],
        )?;
        transaction.execute(
            "UPDATE ingest_runs SET status = ?2, updated_at = ?3, failed_reason = NULL WHERE id = ?1",
            params![run_id, IngestRunStatus::Committed.as_str(), timestamp],
        )?;

        let ingest_kind: String = transaction.query_row(
            "SELECT ingest_kind FROM ingest_runs WHERE id = ?1",
            [run_id],
            |row| row.get(0),
        )?;

        transaction.execute(
            "INSERT INTO ingest_files (source_key, source_file, content_hash, last_ingested_at, ingest_kind, drawer_count)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(source_key) DO UPDATE SET
                 source_file = excluded.source_file,
                 content_hash = excluded.content_hash,
                 last_ingested_at = excluded.last_ingested_at,
                 ingest_kind = excluded.ingest_kind,
                 drawer_count = excluded.drawer_count",
            params![
                source_key,
                source_file,
                content_hash,
                timestamp,
                ingest_kind,
                drawer_count as i64
            ],
        )?;

        transaction.commit()?;
        Ok(())
    }

    fn stale_pending_runs(&self, older_than: OffsetDateTime) -> Result<Vec<RetryableRun>> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT id, ingest_kind, source_key, status, created_at, updated_at, failed_reason
             FROM ingest_runs
             WHERE status = ?1 AND updated_at < ?2
             ORDER BY id ASC",
        )?;

        let runs = statement
            .query_map(
                params![IngestRunStatus::Pending.as_str(), encode_time(older_than)],
                |row| {
                    Ok(IngestRun {
                        id: row.get(0)?,
                        ingest_kind: row.get(1)?,
                        source_key: row.get(2)?,
                        status: parse_status(row.get::<_, String>(3)?)?,
                        created_at: decode_time(row.get(4)?).map_err(|err| {
                            rusqlite::Error::FromSqlConversionFailure(
                                4,
                                rusqlite::types::Type::Text,
                                Box::new(err),
                            )
                        })?,
                        updated_at: decode_time(row.get(5)?).map_err(|err| {
                            rusqlite::Error::FromSqlConversionFailure(
                                5,
                                rusqlite::types::Type::Text,
                                Box::new(err),
                            )
                        })?,
                        failed_reason: row.get(6)?,
                    })
                },
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut retryable = Vec::with_capacity(runs.len());
        for run in runs {
            let mut manifest_statement = connection.prepare(
                "SELECT drawer_id FROM ingest_manifests WHERE run_id = ?1 ORDER BY drawer_id ASC",
            )?;
            let chunk_ids = manifest_statement
                .query_map([run.id], |row| {
                    let raw: String = row.get(0)?;
                    DrawerId::new(raw).map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Text,
                            Box::new(err),
                        )
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;

            retryable.push(RetryableRun { run, chunk_ids });
        }

        Ok(retryable)
    }

    fn mark_run_failed(&self, run_id: i64, reason: &str, failed_at: OffsetDateTime) -> Result<()> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let timestamp = encode_time(failed_at);
        transaction.execute(
            "UPDATE ingest_manifests SET status = ?2 WHERE run_id = ?1",
            params![run_id, IngestRunStatus::Failed.as_str()],
        )?;
        transaction.execute(
            "UPDATE ingest_runs
             SET status = ?2, updated_at = ?3, failed_reason = ?4
             WHERE id = ?1",
            params![run_id, IngestRunStatus::Failed.as_str(), timestamp, reason],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn committed_drawer_ids(&self) -> Result<Vec<DrawerId>> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT drawer_id
             FROM ingest_manifests
             WHERE status = ?1
             ORDER BY drawer_id ASC",
        )?;
        statement
            .query_map([IngestRunStatus::Committed.as_str()], |row| {
                let raw: String = row.get(0)?;
                DrawerId::new(raw).map_err(|err| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(err),
                    )
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    fn committed_drawer_ids_for_source_key(&self, source_key: &str) -> Result<Vec<DrawerId>> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT manifest.drawer_id
             FROM ingest_manifests AS manifest
             INNER JOIN ingest_runs AS runs ON runs.id = manifest.run_id
             WHERE runs.source_key = ?1 AND runs.status = ?2 AND manifest.status = ?2
               AND runs.id = (
                   SELECT id
                   FROM ingest_runs
                   WHERE source_key = ?1 AND status = ?2
                   ORDER BY id DESC
                   LIMIT 1
               )
             ORDER BY manifest.drawer_id ASC",
        )?;
        statement
            .query_map(params![source_key, IngestRunStatus::Committed.as_str()], |row| {
                let raw: String = row.get(0)?;
                DrawerId::new(raw).map_err(|err| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(err),
                    )
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    fn committed_drawer_ids_for_source_keys(
        &self,
        source_keys: &[String],
    ) -> Result<Vec<DrawerId>> {
        // One connection + one prepared statement reused per key, rather than a
        // fresh connection per source. Same latest-committed-run semantics as
        // the single-key variant.
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT manifest.drawer_id
             FROM ingest_manifests AS manifest
             INNER JOIN ingest_runs AS runs ON runs.id = manifest.run_id
             WHERE runs.source_key = ?1 AND runs.status = ?2 AND manifest.status = ?2
               AND runs.id = (
                   SELECT id
                   FROM ingest_runs
                   WHERE source_key = ?1 AND status = ?2
                   ORDER BY id DESC
                   LIMIT 1
               )
             ORDER BY manifest.drawer_id ASC",
        )?;
        let mut ids = Vec::new();
        for source_key in source_keys {
            let rows = statement.query_map(
                params![source_key, IngestRunStatus::Committed.as_str()],
                |row| {
                    let raw: String = row.get(0)?;
                    DrawerId::new(raw).map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Text,
                            Box::new(err),
                        )
                    })
                },
            )?;
            for id in rows {
                ids.push(id.map_err(StorageError::from)?);
            }
        }
        Ok(ids)
    }

    fn get_ingested_file(&self, source_key: &str) -> Result<Option<IngestFileRecord>> {
        let connection = self.open_connection()?;
        let exact = query_ingested_file(&connection, source_key)?;
        if exact.is_some() {
            return Ok(exact);
        }

        if let Some(legacy_key) = legacy_source_key(source_key) {
            return query_ingested_file(&connection, &legacy_key);
        }

        Ok(None)
    }

    fn ingested_source_keys_with_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let pattern = like_prefix_pattern(prefix);
        // Case-sensitive: this feeds branch-delta cleanup, where over-matching a
        // key that differs only in case (e.g. branch `Feature` vs `feature`)
        // would wipe the wrong branch's drawers.
        let connection = self.open_prefix_connection()?;
        let mut statement = connection.prepare(
            "SELECT source_key FROM ingest_files WHERE source_key LIKE ?1 ESCAPE '\\' ORDER BY source_key ASC",
        )?;
        statement
            .query_map([pattern], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    fn ingested_source_counts_with_prefix(&self, prefix: &str) -> Result<Vec<(String, i64)>> {
        let pattern = like_prefix_pattern(prefix);
        let connection = self.open_prefix_connection()?;
        let mut statement = connection.prepare(
            "SELECT source_key, drawer_count FROM ingest_files \
             WHERE source_key LIKE ?1 ESCAPE '\\' ORDER BY source_key ASC",
        )?;
        statement
            .query_map([pattern], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    fn delete_source_key(&self, source_key: &str) -> Result<()> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let run_ids = transaction
            .prepare("SELECT id FROM ingest_runs WHERE source_key = ?1")?
            .query_map([source_key], |row| row.get::<_, i64>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        for run_id in run_ids {
            transaction.execute("DELETE FROM ingest_manifests WHERE run_id = ?1", [run_id])?;
        }
        transaction.execute("DELETE FROM ingest_runs WHERE source_key = ?1", [source_key])?;
        transaction.execute("DELETE FROM ingest_files WHERE source_key = ?1", [source_key])?;
        transaction.commit()?;
        Ok(())
    }

    fn delete_source_keys(&self, source_keys: &[String]) -> Result<()> {
        // One connection + one transaction with reused prepared statements,
        // rather than a fresh connection/transaction per source.
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        {
            let mut select_runs =
                transaction.prepare("SELECT id FROM ingest_runs WHERE source_key = ?1")?;
            let mut delete_manifests =
                transaction.prepare("DELETE FROM ingest_manifests WHERE run_id = ?1")?;
            let mut delete_runs =
                transaction.prepare("DELETE FROM ingest_runs WHERE source_key = ?1")?;
            let mut delete_files =
                transaction.prepare("DELETE FROM ingest_files WHERE source_key = ?1")?;
            for source_key in source_keys {
                let run_ids = select_runs
                    .query_map([source_key], |row| row.get::<_, i64>(0))?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                for run_id in run_ids {
                    delete_manifests.execute([run_id])?;
                }
                delete_runs.execute([source_key])?;
                delete_files.execute([source_key])?;
            }
        }
        transaction.commit()?;
        Ok(())
    }
}

impl SqliteOperationalStore {
    /// Return stale (older than `older_than`) failed diary runs with their
    /// manifest drawer IDs.  Used by startup reconciliation to clean up
    /// orphaned diary summaries and drawers from previous failed writes.
    pub fn stale_failed_diary_runs(&self, older_than: OffsetDateTime) -> Result<Vec<RetryableRun>> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT id, ingest_kind, source_key, status, created_at, updated_at, failed_reason
             FROM ingest_runs
             WHERE status = ?1 AND ingest_kind = 'diary' AND updated_at < ?2
             ORDER BY id ASC",
        )?;

        let runs = statement
            .query_map(params![IngestRunStatus::Failed.as_str(), encode_time(older_than)], |row| {
                Ok(IngestRun {
                    id: row.get(0)?,
                    ingest_kind: row.get(1)?,
                    source_key: row.get(2)?,
                    status: parse_status(row.get::<_, String>(3)?)?,
                    created_at: decode_time(row.get(4)?).map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(
                            4,
                            rusqlite::types::Type::Text,
                            Box::new(err),
                        )
                    })?,
                    updated_at: decode_time(row.get(5)?).map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(
                            5,
                            rusqlite::types::Type::Text,
                            Box::new(err),
                        )
                    })?,
                    failed_reason: row.get(6)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut retryable = Vec::with_capacity(runs.len());
        for run in runs {
            let mut manifest_statement = connection.prepare(
                "SELECT drawer_id FROM ingest_manifests WHERE run_id = ?1 ORDER BY drawer_id ASC",
            )?;
            let chunk_ids = manifest_statement
                .query_map([run.id], |row| {
                    let raw: String = row.get(0)?;
                    DrawerId::new(raw).map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Text,
                            Box::new(err),
                        )
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;

            retryable.push(RetryableRun { run, chunk_ids });
        }

        Ok(retryable)
    }
}

impl EntityRegistryStore for SqliteOperationalStore {
    fn upsert_entity(&self, entity: &EntityRecord) -> Result<()> {
        let connection = self.open_connection()?;
        connection.execute(
            "INSERT INTO entity_registry (entity_id, entity_type, payload_json, updated_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(entity_id) DO UPDATE SET
                 entity_type = excluded.entity_type,
                 payload_json = excluded.payload_json,
                 updated_at = excluded.updated_at",
            params![
                entity.entity_id,
                entity.entity_type,
                serde_json::to_string(&entity.payload)?,
                encode_time(entity.updated_at)
            ],
        )?;
        Ok(())
    }

    fn get_entity(&self, entity_id: &str) -> Result<Option<EntityRecord>> {
        let connection = self.open_connection()?;
        connection
            .query_row(
                "SELECT entity_id, entity_type, payload_json, updated_at
                 FROM entity_registry WHERE entity_id = ?1",
                [entity_id],
                |row| {
                    Ok(EntityRecord {
                        entity_id: row.get(0)?,
                        entity_type: row.get(1)?,
                        payload: serde_json::from_str(&row.get::<_, String>(2)?).map_err(
                            |err| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    2,
                                    rusqlite::types::Type::Text,
                                    Box::new(err),
                                )
                            },
                        )?,
                        updated_at: decode_time(row.get(3)?).map_err(|err| {
                            rusqlite::Error::FromSqlConversionFailure(
                                3,
                                rusqlite::types::Type::Text,
                                Box::new(err),
                            )
                        })?,
                    })
                },
            )
            .optional()
            .map_err(StorageError::from)
    }

    fn list_entities(&self) -> Result<Vec<EntityRecord>> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT entity_id, entity_type, payload_json, updated_at
             FROM entity_registry
             ORDER BY entity_type ASC, entity_id ASC",
        )?;
        statement
            .query_map([], |row| {
                Ok(EntityRecord {
                    entity_id: row.get(0)?,
                    entity_type: row.get(1)?,
                    payload: serde_json::from_str(&row.get::<_, String>(2)?).map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Text,
                            Box::new(err),
                        )
                    })?,
                    updated_at: decode_time(row.get(3)?).map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(
                            3,
                            rusqlite::types::Type::Text,
                            Box::new(err),
                        )
                    })?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    fn list_entities_by_ids(&self, entity_ids: &[String]) -> Result<Vec<EntityRecord>> {
        if entity_ids.is_empty() {
            return Ok(Vec::new());
        }

        let connection = self.open_connection()?;
        let placeholders =
            std::iter::repeat_n("?", entity_ids.len()).collect::<Vec<_>>().join(", ");
        let sql = format!(
            "SELECT entity_id, entity_type, payload_json, updated_at
             FROM entity_registry
             WHERE entity_id IN ({placeholders})
             ORDER BY entity_type ASC, entity_id ASC"
        );
        let mut statement = connection.prepare(&sql)?;
        statement
            .query_map(params_from_iter(entity_ids.iter()), |row| {
                Ok(EntityRecord {
                    entity_id: row.get(0)?,
                    entity_type: row.get(1)?,
                    payload: serde_json::from_str(&row.get::<_, String>(2)?).map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Text,
                            Box::new(err),
                        )
                    })?,
                    updated_at: decode_time(row.get(3)?).map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(
                            3,
                            rusqlite::types::Type::Text,
                            Box::new(err),
                        )
                    })?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }
}

impl GraphStore for SqliteOperationalStore {
    fn put_graph_document(&self, graph: &GraphDocument) -> Result<()> {
        let connection = self.open_connection()?;
        connection.execute(
            "INSERT INTO graph_state (graph_key, payload_json, updated_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(graph_key) DO UPDATE SET
                 payload_json = excluded.payload_json,
                 updated_at = excluded.updated_at",
            params![
                graph.graph_key,
                serde_json::to_string(&graph.payload)?,
                encode_time(graph.updated_at)
            ],
        )?;
        Ok(())
    }

    fn get_graph_document(&self, graph_key: &str) -> Result<Option<GraphDocument>> {
        let connection = self.open_connection()?;
        connection
            .query_row(
                "SELECT graph_key, payload_json, updated_at
                 FROM graph_state WHERE graph_key = ?1",
                [graph_key],
                |row| {
                    Ok(GraphDocument {
                        graph_key: row.get(0)?,
                        payload: serde_json::from_str(&row.get::<_, String>(1)?).map_err(
                            |err| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    1,
                                    rusqlite::types::Type::Text,
                                    Box::new(err),
                                )
                            },
                        )?,
                        updated_at: decode_time(row.get(2)?).map_err(|err| {
                            rusqlite::Error::FromSqlConversionFailure(
                                2,
                                rusqlite::types::Type::Text,
                                Box::new(err),
                            )
                        })?,
                    })
                },
            )
            .optional()
            .map_err(StorageError::from)
    }
}

impl KnowledgeGraphStore for SqliteOperationalStore {
    fn upsert_fact(&self, fact: &KnowledgeGraphFact) -> Result<()> {
        let connection = self.open_connection()?;
        connection.execute(
            "INSERT INTO knowledge_graph_facts (
                 fact_id, subject_entity_id, predicate, object_entity_id, valid_from, valid_to,
                 confidence, source_drawer_id, source_file, created_at, updated_at
             )
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT(fact_id) DO UPDATE SET
                 subject_entity_id = excluded.subject_entity_id,
                 predicate = excluded.predicate,
                 object_entity_id = excluded.object_entity_id,
                 valid_from = excluded.valid_from,
                 valid_to = excluded.valid_to,
                 confidence = excluded.confidence,
                 source_drawer_id = excluded.source_drawer_id,
                 source_file = excluded.source_file,
                 updated_at = excluded.updated_at",
            params![
                fact.fact_id,
                fact.subject_entity_id,
                fact.predicate,
                fact.object_entity_id,
                encode_optional_date(fact.valid_from),
                encode_optional_date(fact.valid_to),
                fact.confidence,
                fact.source_drawer_id.as_ref().map(DrawerId::as_str),
                fact.source_file,
                encode_time(fact.created_at),
                encode_time(fact.updated_at),
            ],
        )?;
        Ok(())
    }

    fn get_fact(&self, fact_id: &str) -> Result<Option<KnowledgeGraphFact>> {
        let connection = self.open_connection()?;
        connection
            .query_row(
                "SELECT fact_id, subject_entity_id, predicate, object_entity_id, valid_from,
                        valid_to, confidence, source_drawer_id, source_file, created_at, updated_at
                 FROM knowledge_graph_facts
                 WHERE fact_id = ?1",
                [fact_id],
                decode_fact_row,
            )
            .optional()
            .map_err(StorageError::from)
    }

    fn list_facts(&self) -> Result<Vec<KnowledgeGraphFact>> {
        self.list_facts_matching(
            "SELECT fact_id, subject_entity_id, predicate, object_entity_id, valid_from,
                    valid_to, confidence, source_drawer_id, source_file, created_at, updated_at
             FROM knowledge_graph_facts
             ORDER BY COALESCE(valid_from, '9999-12-31') ASC, predicate ASC, fact_id ASC",
            [],
        )
    }

    fn list_facts_limited(&self, limit: usize) -> Result<Vec<KnowledgeGraphFact>> {
        self.list_facts_matching(
            "SELECT fact_id, subject_entity_id, predicate, object_entity_id, valid_from,
                    valid_to, confidence, source_drawer_id, source_file, created_at, updated_at
             FROM knowledge_graph_facts
             ORDER BY COALESCE(valid_from, '9999-12-31') ASC, predicate ASC, fact_id ASC
             LIMIT ?1",
            [limit as i64],
        )
    }

    fn list_facts_for_entity(&self, entity_id: &str) -> Result<Vec<KnowledgeGraphFact>> {
        self.list_facts_matching(
            "SELECT fact_id, subject_entity_id, predicate, object_entity_id, valid_from,
                    valid_to, confidence, source_drawer_id, source_file, created_at, updated_at
             FROM knowledge_graph_facts
             WHERE subject_entity_id = ?1 OR object_entity_id = ?1
             ORDER BY COALESCE(valid_from, '9999-12-31') ASC, predicate ASC, fact_id ASC",
            [entity_id],
        )
    }

    fn find_active_fact(
        &self,
        subject_entity_id: &str,
        predicate: &str,
        object_entity_id: &str,
    ) -> Result<Option<KnowledgeGraphFact>> {
        let connection = self.open_connection()?;
        connection
            .query_row(
                "SELECT fact_id, subject_entity_id, predicate, object_entity_id, valid_from,
                        valid_to, confidence, source_drawer_id, source_file, created_at, updated_at
                 FROM knowledge_graph_facts
                 WHERE subject_entity_id = ?1
                   AND predicate = ?2
                   AND object_entity_id = ?3
                   AND valid_to IS NULL
                 ORDER BY COALESCE(valid_from, '9999-12-31') ASC, fact_id ASC
                 LIMIT 1",
                params![subject_entity_id, predicate, object_entity_id],
                decode_fact_row,
            )
            .optional()
            .map_err(StorageError::from)
    }

    fn invalidate_active_fact(
        &self,
        subject_entity_id: &str,
        predicate: &str,
        object_entity_id: &str,
        ended_at: Date,
        updated_at: OffsetDateTime,
    ) -> Result<usize> {
        let connection = self.open_connection()?;
        let changed = connection.execute(
            "UPDATE knowledge_graph_facts
             SET valid_to = ?4, updated_at = ?5
             WHERE subject_entity_id = ?1
               AND predicate = ?2
               AND object_entity_id = ?3
               AND valid_to IS NULL
               AND (valid_from IS NULL OR valid_from <= ?4)",
            params![
                subject_entity_id,
                predicate,
                object_entity_id,
                encode_date(ended_at),
                encode_time(updated_at)
            ],
        )?;
        Ok(changed)
    }
}

impl ToolStateStore for SqliteOperationalStore {
    fn put_tool_state(&self, state: &ToolStateEntry) -> Result<()> {
        let connection = self.open_connection()?;
        connection.execute(
            "INSERT INTO tool_state (tool_name, payload_json, updated_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(tool_name) DO UPDATE SET
                 payload_json = excluded.payload_json,
                 updated_at = excluded.updated_at",
            params![state.tool_name, state.payload, encode_time(state.updated_at)],
        )?;
        Ok(())
    }

    fn get_tool_state(&self, tool_name: &str) -> Result<Option<ToolStateEntry>> {
        let connection = self.open_connection()?;
        connection
            .query_row(
                "SELECT tool_name, payload_json, updated_at
                 FROM tool_state WHERE tool_name = ?1",
                [tool_name],
                |row| {
                    Ok(ToolStateEntry {
                        tool_name: row.get(0)?,
                        payload: row.get(1)?,
                        updated_at: decode_time(row.get(2)?).map_err(|err| {
                            rusqlite::Error::FromSqlConversionFailure(
                                2,
                                rusqlite::types::Type::Text,
                                Box::new(err),
                            )
                        })?,
                    })
                },
            )
            .optional()
            .map_err(StorageError::from)
    }

    fn put_config(&self, entry: &ConfigEntry) -> Result<()> {
        let connection = self.open_connection()?;
        connection.execute(
            "INSERT INTO config (config_key, config_value)
             VALUES (?1, ?2)
             ON CONFLICT(config_key) DO UPDATE SET config_value = excluded.config_value",
            params![entry.config_key, entry.config_value],
        )?;
        Ok(())
    }

    fn get_config(&self, key: &str) -> Result<Option<ConfigEntry>> {
        let connection = self.open_connection()?;
        connection
            .query_row(
                "SELECT config_key, config_value FROM config WHERE config_key = ?1",
                [key],
                |row| Ok(ConfigEntry { config_key: row.get(0)?, config_value: row.get(1)? }),
            )
            .optional()
            .map_err(StorageError::from)
    }
}

impl SelfModelStore for SqliteOperationalStore {
    fn set_lineage(
        &self,
        lineage_id: &str,
        display_name: &str,
        description: &str,
        set_default: bool,
        expected_revision: Option<i64>,
        now: OffsetDateTime,
    ) -> Result<RevisionedWrite<AgentLineageRecord>> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let current = transaction
            .query_row(
                "SELECT lineage_id, display_name, description, revision, is_default,
                        created_at, updated_at
                 FROM agent_lineages WHERE lineage_id = ?1",
                [lineage_id],
                decode_lineage_row,
            )
            .optional()?;

        let (revision, created_at, was_default) = match current {
            Some(current) => {
                if expected_revision != Some(current.revision) {
                    return Ok(RevisionedWrite::Conflict {
                        actual_revision: Some(current.revision),
                    });
                }
                (current.revision + 1, current.created_at, current.is_default)
            }
            None => {
                if expected_revision.is_some_and(|revision| revision != 0) {
                    return Ok(RevisionedWrite::Conflict { actual_revision: None });
                }
                (1, now, false)
            }
        };

        let lineage_count: i64 =
            transaction.query_row("SELECT COUNT(*) FROM agent_lineages", [], |row| row.get(0))?;
        let is_default = set_default || was_default || lineage_count == 0;
        if is_default {
            transaction.execute(
                "UPDATE agent_lineages
                 SET is_default = 0, revision = revision + 1, updated_at = ?1
                 WHERE is_default = 1 AND lineage_id <> ?2",
                params![encode_time(now), lineage_id],
            )?;
        }

        transaction.execute(
            "INSERT INTO agent_lineages (
                 lineage_id, display_name, description, revision, is_default, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(lineage_id) DO UPDATE SET
                 display_name = excluded.display_name,
                 description = excluded.description,
                 revision = excluded.revision,
                 is_default = excluded.is_default,
                 updated_at = excluded.updated_at",
            params![
                lineage_id,
                display_name,
                description,
                revision,
                is_default,
                encode_time(created_at),
                encode_time(now),
            ],
        )?;
        transaction.commit()?;

        Ok(RevisionedWrite::Applied(AgentLineageRecord {
            lineage_id: lineage_id.to_owned(),
            display_name: display_name.to_owned(),
            description: description.to_owned(),
            revision,
            is_default,
            created_at,
            updated_at: now,
        }))
    }

    fn get_lineage(&self, lineage_id: &str) -> Result<Option<AgentLineageRecord>> {
        let connection = self.open_connection()?;
        connection
            .query_row(
                "SELECT lineage_id, display_name, description, revision, is_default,
                        created_at, updated_at
                 FROM agent_lineages WHERE lineage_id = ?1",
                [lineage_id],
                decode_lineage_row,
            )
            .optional()
            .map_err(StorageError::from)
    }

    fn get_default_lineage(&self) -> Result<Option<AgentLineageRecord>> {
        let connection = self.open_connection()?;
        connection
            .query_row(
                "SELECT lineage_id, display_name, description, revision, is_default,
                        created_at, updated_at
                 FROM agent_lineages WHERE is_default = 1 LIMIT 1",
                [],
                decode_lineage_row,
            )
            .optional()
            .map_err(StorageError::from)
    }

    fn list_lineages(&self) -> Result<Vec<AgentLineageRecord>> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT lineage_id, display_name, description, revision, is_default,
                    created_at, updated_at
             FROM agent_lineages
             ORDER BY is_default DESC, display_name ASC, lineage_id ASC",
        )?;
        statement
            .query_map([], decode_lineage_row)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    fn propose_self_observation(&self, observation: &SelfObservationRecord) -> Result<()> {
        let connection = self.open_connection()?;
        connection.execute(
            "INSERT INTO self_observations (
                 observation_id, lineage_id, status, scope, statement, behavioral_consequence,
                 confidence, author, model, harness, evidence_json, counterevidence_json,
                 supersedes_observation_id, revision, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![
                observation.observation_id,
                observation.lineage_id,
                observation.status.as_str(),
                observation.scope.as_str(),
                observation.statement,
                observation.behavioral_consequence,
                observation.confidence,
                observation.author,
                observation.model,
                observation.harness,
                serde_json::to_string(&observation.evidence)?,
                serde_json::to_string(&observation.counterevidence)?,
                observation.supersedes_observation_id,
                observation.revision,
                encode_time(observation.created_at),
                encode_time(observation.updated_at),
            ],
        )?;
        Ok(())
    }

    fn get_self_observation(
        &self,
        observation_id: &str,
    ) -> Result<Option<SelfObservationRecord>> {
        let connection = self.open_connection()?;
        connection
            .query_row(
                "SELECT observation_id, lineage_id, status, scope, statement,
                        behavioral_consequence, confidence, author, model, harness,
                        evidence_json, counterevidence_json, supersedes_observation_id,
                        revision, created_at, updated_at
                 FROM self_observations WHERE observation_id = ?1",
                [observation_id],
                decode_self_observation_row,
            )
            .optional()
            .map_err(StorageError::from)
    }

    fn list_self_observations(
        &self,
        lineage_id: &str,
        statuses: &[SelfObservationStatus],
        limit: usize,
    ) -> Result<Vec<SelfObservationRecord>> {
        self.list_self_observations_scoped(lineage_id, statuses, None, limit)
    }

    fn list_self_observations_scoped(
        &self,
        lineage_id: &str,
        statuses: &[SelfObservationStatus],
        scope: Option<SelfObservationScope>,
        limit: usize,
    ) -> Result<Vec<SelfObservationRecord>> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT observation_id, lineage_id, status, scope, statement,
                    behavioral_consequence, confidence, author, model, harness,
                     evidence_json, counterevidence_json, supersedes_observation_id,
                     revision, created_at, updated_at
             FROM self_observations
             WHERE lineage_id = ?1 AND (?2 IS NULL OR scope = ?2)
             ORDER BY updated_at DESC, observation_id ASC",
        )?;
        let records = statement
            .query_map(
                params![lineage_id, scope.map(|scope| scope.as_str())],
                decode_self_observation_row,
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(records
            .into_iter()
            .filter(|record| statuses.is_empty() || statuses.contains(&record.status))
            .take(limit)
            .collect())
    }

    fn review_self_observation(
        &self,
        observation_id: &str,
        expected_revision: i64,
        new_status: SelfObservationStatus,
        reviewer: &str,
        reason: &str,
        now: OffsetDateTime,
    ) -> Result<RevisionedWrite<SelfObservationRecord>> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let current = transaction
            .query_row(
                "SELECT observation_id, lineage_id, status, scope, statement,
                        behavioral_consequence, confidence, author, model, harness,
                        evidence_json, counterevidence_json, supersedes_observation_id,
                        revision, created_at, updated_at
                 FROM self_observations WHERE observation_id = ?1",
                [observation_id],
                decode_self_observation_row,
            )
            .optional()?;
        let Some(mut current) = current else {
            return Ok(RevisionedWrite::Conflict { actual_revision: None });
        };
        if current.revision != expected_revision {
            return Ok(RevisionedWrite::Conflict {
                actual_revision: Some(current.revision),
            });
        }

        let prior_status = current.status;
        current.status = new_status;
        current.revision += 1;
        current.updated_at = now;
        transaction.execute(
            "UPDATE self_observations
             SET status = ?2, revision = ?3, updated_at = ?4
             WHERE observation_id = ?1 AND revision = ?5",
            params![
                observation_id,
                new_status.as_str(),
                current.revision,
                encode_time(now),
                expected_revision,
            ],
        )?;
        insert_self_observation_review(
            &transaction,
            observation_id,
            current.revision,
            prior_status,
            new_status,
            reviewer,
            reason,
            now,
        )?;

        if new_status == SelfObservationStatus::Promoted
            && let Some(superseded_id) = &current.supersedes_observation_id
        {
            let superseded = transaction
                .query_row(
                    "SELECT observation_id, lineage_id, status, scope, statement,
                            behavioral_consequence, confidence, author, model, harness,
                            evidence_json, counterevidence_json, supersedes_observation_id,
                            revision, created_at, updated_at
                     FROM self_observations WHERE observation_id = ?1",
                    [superseded_id],
                    decode_self_observation_row,
                )
                .optional()?;
            let Some(superseded) = superseded else {
                return Err(StorageError::Invariant(format!(
                    "superseded observation `{superseded_id}` does not exist"
                )));
            };
            if superseded.lineage_id != current.lineage_id {
                return Err(StorageError::Invariant(format!(
                    "superseded observation `{superseded_id}` belongs to another lineage"
                )));
            }
            if superseded.status != SelfObservationStatus::Promoted {
                return Err(StorageError::Invariant(format!(
                    "superseded observation `{superseded_id}` must currently be promoted"
                )));
            }
            let next_revision = superseded.revision + 1;
            let updated = transaction.execute(
                "UPDATE self_observations
                 SET status = 'superseded', revision = ?2, updated_at = ?3
                 WHERE observation_id = ?1 AND status = 'promoted' AND revision = ?4",
                params![superseded_id, next_revision, encode_time(now), superseded.revision],
            )?;
            if updated != 1 {
                return Err(StorageError::Invariant(format!(
                    "superseded observation `{superseded_id}` changed before it could be superseded"
                )));
            }
            insert_self_observation_review(
                &transaction,
                superseded_id,
                next_revision,
                superseded.status,
                SelfObservationStatus::Superseded,
                reviewer,
                &format!("superseded by `{observation_id}`: {reason}"),
                now,
            )?;
        }

        transaction.commit()?;
        Ok(RevisionedWrite::Applied(current))
    }

    fn record_lineage_migration(&self, migration: &LineageMigrationRecord) -> Result<()> {
        let connection = self.open_connection()?;
        connection.execute(
            "INSERT INTO lineage_migrations (
                 migration_id, lineage_id, from_model, from_harness, to_model, to_harness,
                 summary, continuities_json, changes_json, evidence_json, author, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                migration.migration_id,
                migration.lineage_id,
                migration.from_model,
                migration.from_harness,
                migration.to_model,
                migration.to_harness,
                migration.summary,
                serde_json::to_string(&migration.continuities)?,
                serde_json::to_string(&migration.changes)?,
                serde_json::to_string(&migration.evidence)?,
                migration.author,
                encode_time(migration.created_at),
            ],
        )?;
        Ok(())
    }

    fn list_lineage_migrations(
        &self,
        lineage_id: &str,
        limit: usize,
    ) -> Result<Vec<LineageMigrationRecord>> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT migration_id, lineage_id, from_model, from_harness, to_model, to_harness,
                    summary, continuities_json, changes_json, evidence_json, author, created_at
             FROM lineage_migrations
             WHERE lineage_id = ?1
             ORDER BY created_at DESC, migration_id ASC
             LIMIT ?2",
        )?;
        statement
            .query_map(params![lineage_id, limit as i64], decode_lineage_migration_row)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }
}

impl ChangeLogStore for SqliteOperationalStore {
    fn append_event(&self, event: &ChangeEvent) -> Result<()> {
        let change_id = format!("{}:{}", event.occurred_at.unix_timestamp_nanos(), event.entity_id);
        let connection = self.open_connection()?;
        connection.execute(
            "INSERT OR IGNORE INTO change_log
                 (change_id, event_type, occurred_at, entity_id, actor, details_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                change_id,
                event.event_type,
                encode_time(event.occurred_at),
                event.entity_id,
                event.actor,
                event.details_json,
            ],
        )?;
        Ok(())
    }

    fn event_exists(&self, entity_id: &str, event_type: &str) -> Result<bool> {
        let connection = self.open_connection()?;
        let exists: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM change_log WHERE entity_id=?1 AND event_type=?2)",
                params![entity_id, event_type],
                |row| row.get(0),
            )
            .map_err(StorageError::from)?;
        Ok(exists)
    }

    fn get_changes_since(&self, since: OffsetDateTime, limit: usize) -> Result<Vec<ChangeEvent>> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT event_type, occurred_at, entity_id, actor, details_json
             FROM change_log
             WHERE occurred_at > ?1
             ORDER BY occurred_at ASC
             LIMIT ?2",
        )?;
        statement
            .query_map(params![encode_time(since), limit as i64], |row| {
                Ok(ChangeEvent {
                    event_type: row.get(0)?,
                    occurred_at: decode_time(row.get(1)?).map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(
                            1,
                            rusqlite::types::Type::Text,
                            Box::new(err),
                        )
                    })?,
                    entity_id: row.get(2)?,
                    actor: row.get(3)?,
                    details_json: row.get(4)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    fn get_recent_changes(&self, limit: usize) -> Result<Vec<ChangeEvent>> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT event_type, occurred_at, entity_id, actor, details_json
             FROM change_log
             ORDER BY occurred_at DESC
             LIMIT ?1",
        )?;
        let mut events = statement
            .query_map(params![limit as i64], |row| {
                Ok(ChangeEvent {
                    event_type: row.get(0)?,
                    occurred_at: decode_time(row.get(1)?).map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(
                            1,
                            rusqlite::types::Type::Text,
                            Box::new(err),
                        )
                    })?,
                    entity_id: row.get(2)?,
                    actor: row.get(3)?,
                    details_json: row.get(4)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(StorageError::from)?;
        events.reverse();
        Ok(events)
    }

    fn get_changes_page(
        &self,
        since: Option<OffsetDateTime>,
        cursor: Option<ChangeCursor>,
        limit: usize,
    ) -> Result<ChangePage> {
        let fetch_limit = limit.saturating_add(1) as i64;
        let connection = self.open_connection()?;

        // We read `rowid` as column 0 (used to build the next cursor), then the
        // normal event columns at 1..=5.
        let mut rows: Vec<(i64, ChangeEvent)> = match cursor {
            Some(ref c) => {
                // Cursor path: row-value comparison for stable forward pagination.
                // (occurred_at, rowid) > (cursor_occurred_at, cursor_rowid)
                let cursor_ts = encode_time(c.occurred_at);
                let mut stmt = connection.prepare(
                    "SELECT rowid, event_type, occurred_at, entity_id, actor, details_json
                     FROM change_log
                     WHERE (occurred_at, rowid) > (?1, ?2)
                     ORDER BY occurred_at ASC, rowid ASC
                     LIMIT ?3",
                )?;
                stmt.query_map(params![cursor_ts, c.rowid, fetch_limit], decode_page_row)?
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(StorageError::from)?
            }
            None => match since {
                Some(since_dt) => {
                    let since_ts = encode_time(since_dt);
                    let mut stmt = connection.prepare(
                        "SELECT rowid, event_type, occurred_at, entity_id, actor, details_json
                         FROM change_log
                         WHERE occurred_at >= ?1
                         ORDER BY occurred_at ASC, rowid ASC
                         LIMIT ?2",
                    )?;
                    stmt.query_map(params![since_ts, fetch_limit], decode_page_row)?
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(StorageError::from)?
                }
                None => {
                    let mut stmt = connection.prepare(
                        "SELECT rowid, event_type, occurred_at, entity_id, actor, details_json
                         FROM change_log
                         ORDER BY occurred_at ASC, rowid ASC
                         LIMIT ?1",
                    )?;
                    stmt.query_map(params![fetch_limit], decode_page_row)?
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(StorageError::from)?
                }
            },
        };

        // If we got more than `limit` rows we know there is a next page.
        let next_cursor = if rows.len() > limit {
            rows.pop(); // discard the sentinel row
            // Build the cursor from the last row that we ARE returning.
            rows.last().map(|(rowid, event)| ChangeCursor {
                occurred_at: event.occurred_at,
                rowid: *rowid,
            })
        } else {
            None
        };

        Ok(ChangePage { events: rows.into_iter().map(|(_, event)| event).collect(), next_cursor })
    }
}

impl MaintenanceLeaseStore for SqliteOperationalStore {
    fn try_claim_lease(&self, holder: &str, ttl: Duration) -> Result<bool> {
        let conn = self.open_lease_connection()?;
        let now = OffsetDateTime::now_utc();
        let expires_at = now + ttl;

        // Use BEGIN IMMEDIATE so that a contended write queue yields
        // SQLITE_BUSY immediately rather than blocking or deadlocking.
        // We translate that into a clean Ok(false) (non-error skip).
        if let Err(e) = conn.execute_batch("BEGIN IMMEDIATE") {
            return if matches!(
                e,
                rusqlite::Error::SqliteFailure(ref f, _)
                    if f.code == rusqlite::ErrorCode::DatabaseBusy
            ) {
                Ok(false)
            } else {
                Err(StorageError::from(e))
            };
        }

        let result = conn.execute(
            "UPDATE maintenance_leases
             SET holder = ?1, expires_at = ?2, updated_at = ?3
             WHERE lease_id = 'maintenance'
               AND (expires_at < ?4 OR holder = ?5)",
            params![
                holder,
                encode_lease_time(expires_at),
                encode_lease_time(now),
                encode_lease_time(now),
                holder,
            ],
        );

        match result {
            Ok(rows) => {
                conn.execute_batch("COMMIT").map_err(|e| {
                    let _ = conn.execute_batch("ROLLBACK");
                    StorageError::from(e)
                })?;
                Ok(rows > 0)
            }
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(StorageError::from(e))
            }
        }
    }

    fn renew_lease(&self, holder: &str, ttl: Duration) -> Result<bool> {
        let conn = self.open_connection()?;
        let now = OffsetDateTime::now_utc();
        let expires_at = now + ttl;

        let rows = conn.execute(
            "UPDATE maintenance_leases
              SET expires_at = ?1, updated_at = ?2
              WHERE lease_id = 'maintenance' AND holder = ?3 AND expires_at > ?4",
            params![
                encode_lease_time(expires_at),
                encode_lease_time(now),
                holder,
                encode_lease_time(now),
            ],
        )?;

        Ok(rows > 0)
    }

    fn release_lease(&self, holder: &str) -> Result<bool> {
        let conn = self.open_connection()?;

        let rows = conn.execute(
            "UPDATE maintenance_leases
              SET holder = '', expires_at = ?1, updated_at = ?1
              WHERE lease_id = 'maintenance' AND holder = ?2",
            params![encode_lease_time(OffsetDateTime::UNIX_EPOCH), holder],
        )?;

        Ok(rows > 0)
    }

    fn lease_status(&self) -> Result<Option<(String, OffsetDateTime)>> {
        let conn = self.open_connection()?;
        let row = conn
            .query_row(
                "SELECT holder, expires_at FROM maintenance_leases WHERE lease_id = 'maintenance'",
                [],
                |row| {
                    let holder: String = row.get(0)?;
                    let expires_at: String = row.get(1)?;
                    Ok((
                        holder,
                        decode_time(expires_at).map_err(|err| {
                            rusqlite::Error::FromSqlConversionFailure(
                                1,
                                rusqlite::types::Type::Text,
                                Box::new(err),
                            )
                        })?,
                    ))
                },
            )
            .optional()?;

        // Return None when the row is empty (no real holder).
        Ok(row.filter(|(h, _)| !h.is_empty()))
    }
}

fn parse_status(raw: String) -> rusqlite::Result<IngestRunStatus> {
    match raw.as_str() {
        "pending" => Ok(IngestRunStatus::Pending),
        "committed" => Ok(IngestRunStatus::Committed),
        "failed" => Ok(IngestRunStatus::Failed),
        _ => Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            Box::new(StorageError::Invariant(format!("unknown ingest status `{raw}`"))),
        )),
    }
}

fn query_ingested_file(
    connection: &Connection,
    source_key: &str,
) -> Result<Option<IngestFileRecord>> {
    connection
        .query_row(
            "SELECT source_key, source_file, content_hash, last_ingested_at, ingest_kind, drawer_count
             FROM ingest_files
             WHERE source_key = ?1",
            [source_key],
            |row| {
                let drawer_count: i64 = row.get(5)?;
                Ok(IngestFileRecord {
                    source_key: row.get(0)?,
                    source_file: row.get(1)?,
                    content_hash: row.get(2)?,
                    last_ingested_at: decode_time(row.get(3)?).map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(
                            3,
                            rusqlite::types::Type::Text,
                            Box::new(err),
                        )
                    })?,
                    ingest_kind: row.get(4)?,
                    drawer_count: usize::try_from(drawer_count).map_err(|_| {
                        rusqlite::Error::FromSqlConversionFailure(
                            5,
                            rusqlite::types::Type::Integer,
                            Box::new(StorageError::Invariant(format!(
                                "invalid drawer_count `{drawer_count}`"
                            ))),
                        )
                    })?,
                })
            },
        )
        .optional()
        .map_err(StorageError::from)
}

fn legacy_source_key(source_key: &str) -> Option<String> {
    let (ingest_kind, remainder) = source_key.split_once(':')?;
    let required_delimiters = match ingest_kind {
        "projects" => 2,
        "convos" => 3,
        _ => return None,
    };

    let mut relative_path = remainder;
    for _ in 0..required_delimiters {
        let (_, tail) = relative_path.split_once(':')?;
        relative_path = tail;
    }

    Some(format!("{ingest_kind}:{relative_path}"))
}

impl SqliteOperationalStore {
    fn list_facts_matching<P>(&self, sql: &str, params: P) -> Result<Vec<KnowledgeGraphFact>>
    where
        P: rusqlite::Params,
    {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(sql)?;
        statement
            .query_map(params, decode_fact_row)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }
}

fn decode_page_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<(i64, ChangeEvent)> {
    let rowid: i64 = row.get(0)?;
    let event = ChangeEvent {
        event_type: row.get(1)?,
        occurred_at: decode_time(row.get(2)?).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(err))
        })?,
        entity_id: row.get(3)?,
        actor: row.get(4)?,
        details_json: row.get(5)?,
    };
    Ok((rowid, event))
}

fn encode_time(value: OffsetDateTime) -> String {
    value
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| value.unix_timestamp().to_string())
}

/// Encode lease timestamps with a fixed-width fractional component so SQLite's
/// text comparison preserves their chronological order.
fn encode_lease_time(value: OffsetDateTime) -> String {
    value
        .format(&time::macros::format_description!(
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:9]Z"
        ))
        .unwrap_or_else(|_| encode_time(value))
}

fn decode_time(raw: String) -> Result<OffsetDateTime> {
    OffsetDateTime::parse(&raw, &time::format_description::well_known::Rfc3339)
        .map_err(|err| StorageError::Invariant(err.to_string()))
}

fn encode_date(value: Date) -> String {
    value
        .format(&time::macros::format_description!("[year]-[month]-[day]"))
        .unwrap_or_else(|_| value.to_string())
}

fn encode_optional_date(value: Option<Date>) -> Option<String> {
    value.map(encode_date)
}

fn decode_optional_date(raw: Option<String>) -> rusqlite::Result<Option<Date>> {
    raw.map(|value| {
        Date::parse(&value, &time::macros::format_description!("[year]-[month]-[day]")).map_err(
            |err| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(StorageError::Invariant(err.to_string())),
                )
            },
        )
    })
    .transpose()
}

fn decode_fact_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<KnowledgeGraphFact> {
    Ok(KnowledgeGraphFact {
        fact_id: row.get(0)?,
        subject_entity_id: row.get(1)?,
        predicate: row.get(2)?,
        object_entity_id: row.get(3)?,
        valid_from: decode_optional_date(row.get(4)?)?,
        valid_to: decode_optional_date(row.get(5)?)?,
        confidence: row.get(6)?,
        source_drawer_id: row
            .get::<_, Option<String>>(7)?
            .map(|raw| {
                DrawerId::new(raw).map_err(|err| {
                    rusqlite::Error::FromSqlConversionFailure(
                        7,
                        rusqlite::types::Type::Text,
                        Box::new(err),
                    )
                })
            })
            .transpose()?,
        source_file: row.get(8)?,
        created_at: decode_time(row.get(9)?).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(9, rusqlite::types::Type::Text, Box::new(err))
        })?,
        updated_at: decode_time(row.get(10)?).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                10,
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })?,
    })
}

fn decode_lineage_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AgentLineageRecord> {
    Ok(AgentLineageRecord {
        lineage_id: row.get(0)?,
        display_name: row.get(1)?,
        description: row.get(2)?,
        revision: row.get(3)?,
        is_default: row.get::<_, i64>(4)? != 0,
        created_at: decode_time(row.get(5)?).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Text, Box::new(err))
        })?,
        updated_at: decode_time(row.get(6)?).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(6, rusqlite::types::Type::Text, Box::new(err))
        })?,
    })
}

fn decode_self_observation_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<SelfObservationRecord> {
    let raw_status: String = row.get(2)?;
    let status = SelfObservationStatus::parse(&raw_status).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            2,
            rusqlite::types::Type::Text,
            Box::new(StorageError::Invariant(format!(
                "unknown self-observation status `{raw_status}`"
            ))),
        )
    })?;
    let raw_scope: String = row.get(3)?;
    let scope = SelfObservationScope::parse(&raw_scope).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            3,
            rusqlite::types::Type::Text,
            Box::new(StorageError::Invariant(format!(
                "unknown self-observation scope `{raw_scope}`"
            ))),
        )
    })?;
    Ok(SelfObservationRecord {
        observation_id: row.get(0)?,
        lineage_id: row.get(1)?,
        status,
        scope,
        statement: row.get(4)?,
        behavioral_consequence: row.get(5)?,
        confidence: row.get(6)?,
        author: row.get(7)?,
        model: row.get(8)?,
        harness: row.get(9)?,
        evidence: decode_string_array_column(row, 10)?,
        counterevidence: decode_string_array_column(row, 11)?,
        supersedes_observation_id: row.get(12)?,
        revision: row.get(13)?,
        created_at: decode_time(row.get(14)?).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                14,
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })?,
        updated_at: decode_time(row.get(15)?).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                15,
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })?,
    })
}

fn decode_lineage_migration_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<LineageMigrationRecord> {
    Ok(LineageMigrationRecord {
        migration_id: row.get(0)?,
        lineage_id: row.get(1)?,
        from_model: row.get(2)?,
        from_harness: row.get(3)?,
        to_model: row.get(4)?,
        to_harness: row.get(5)?,
        summary: row.get(6)?,
        continuities: decode_string_array_column(row, 7)?,
        changes: decode_string_array_column(row, 8)?,
        evidence: decode_string_array_column(row, 9)?,
        author: row.get(10)?,
        created_at: decode_time(row.get(11)?).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                11,
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })?,
    })
}

fn decode_string_array_column(
    row: &rusqlite::Row<'_>,
    index: usize,
) -> rusqlite::Result<Vec<String>> {
    let raw: String = row.get(index)?;
    serde_json::from_str(&raw).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            Box::new(err),
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn insert_self_observation_review(
    transaction: &rusqlite::Transaction<'_>,
    observation_id: &str,
    revision: i64,
    prior_status: SelfObservationStatus,
    new_status: SelfObservationStatus,
    reviewer: &str,
    reason: &str,
    reviewed_at: OffsetDateTime,
) -> Result<()> {
    transaction.execute(
        "INSERT INTO self_observation_reviews (
             observation_id, revision, prior_status, new_status, reviewer, reason, reviewed_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            observation_id,
            revision,
            prior_status.as_str(),
            new_status.as_str(),
            reviewer,
            reason,
            encode_time(reviewed_at),
        ],
    )?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use rusqlite::OptionalExtension;
    use tempfile::tempdir;
    use time::macros::datetime;

    use crate::error::StorageError;
    use super::{
        ChangeCursor, ChangeEvent, ChangeLogStore, ChangePage, DiaryStore, EntityRegistryStore,
        GraphStore, IngestManifestStore, KnowledgeGraphStore, MIGRATIONS, MaintenanceLeaseStore,
        SelfModelStore, SqliteOperationalStore, ToolStateStore, encode_lease_time,
    };
    use crate::types::{
        ConfigEntry, EntityRecord, GraphDocument, IngestManifestEntry, IngestRunStatus,
        KnowledgeGraphFact, LineageMigrationRecord, RevisionedWrite, SelfObservationRecord,
        SelfObservationScope, SelfObservationStatus, ToolStateEntry,
    };
    use mempalace_core::{DIARY_SUMMARY_MAX_CHARS, DrawerId};
    use serde_json::json;
    use time::macros::date;
    use time::{Duration, OffsetDateTime};

    #[test]
    fn change_log_records_and_queries_events() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        let t0 = datetime!(2026-05-08 10:00:00 UTC);
        let t1 = datetime!(2026-05-08 10:01:00 UTC);
        let t2 = datetime!(2026-05-08 10:02:00 UTC);
        let t3 = datetime!(2026-05-08 10:03:00 UTC);

        store
            .append_event(&ChangeEvent {
                event_type: "drawer_added".to_owned(),
                occurred_at: t1,
                entity_id: "drawer_wing_code_backend_abc".to_owned(),
                actor: Some("claude".to_owned()),
                details_json: Some(r#"{"wing":"wing_code","room":"backend"}"#.to_owned()),
            })
            .unwrap();
        store
            .append_event(&ChangeEvent {
                event_type: "kg_fact_added".to_owned(),
                occurred_at: t2,
                entity_id: "Alice → works_on → MemPalace".to_owned(),
                actor: None,
                details_json: None,
            })
            .unwrap();
        store
            .append_event(&ChangeEvent {
                event_type: "drawer_deleted".to_owned(),
                occurred_at: t3,
                entity_id: "drawer_wing_code_backend_abc".to_owned(),
                actor: None,
                details_json: None,
            })
            .unwrap();

        // since t0 returns all three
        let all = store.get_changes_since(t0, 50).unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].event_type, "drawer_added");
        assert_eq!(all[0].actor.as_deref(), Some("claude"));
        assert_eq!(all[1].event_type, "kg_fact_added");
        assert_eq!(all[2].event_type, "drawer_deleted");

        // since t1 returns the last two
        let after_t1 = store.get_changes_since(t1, 50).unwrap();
        assert_eq!(after_t1.len(), 2);
        assert_eq!(after_t1[0].event_type, "kg_fact_added");

        // limit is respected
        let limited = store.get_changes_since(t0, 1).unwrap();
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].event_type, "drawer_added");
    }

    #[test]
    fn change_log_cursor_pagination_walks_all_pages() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        // Five events: e1 and e2 share the same occurred_at to exercise the
        // rowid tiebreaker path.
        let t_shared = datetime!(2026-06-01 09:00:00 UTC);
        let t3 = datetime!(2026-06-01 09:01:00 UTC);
        let t4 = datetime!(2026-06-01 09:02:00 UTC);
        let t5 = datetime!(2026-06-01 09:03:00 UTC);

        for (i, ts) in [t_shared, t_shared, t3, t4, t5].iter().enumerate() {
            store
                .append_event(&ChangeEvent {
                    event_type: format!("event_{i}"),
                    occurred_at: *ts,
                    entity_id: format!("entity_{i}"),
                    actor: None,
                    details_json: None,
                })
                .unwrap();
        }

        // Walk all pages with a page size of 2 and collect every event.
        let mut all_seen: Vec<String> = Vec::new();
        let mut cursor: Option<ChangeCursor> = None;
        let page_size = 2;
        loop {
            let page = store.get_changes_page(None, cursor.clone(), page_size).unwrap();
            assert!(page.events.len() <= page_size, "page has more events than the page size");
            for event in &page.events {
                all_seen.push(event.event_type.clone());
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }

        // All five events must be present, in order, with no duplicates.
        assert_eq!(all_seen.len(), 5, "expected exactly 5 events across all pages");
        assert_eq!(all_seen, vec!["event_0", "event_1", "event_2", "event_3", "event_4"]);
    }

    #[test]
    fn change_log_cursor_pagination_final_page_has_no_next_cursor() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        let t1 = datetime!(2026-06-01 10:00:00 UTC);
        store
            .append_event(&ChangeEvent {
                event_type: "drawer_added".to_owned(),
                occurred_at: t1,
                entity_id: "e1".to_owned(),
                actor: None,
                details_json: None,
            })
            .unwrap();

        // Single event with a page size of 5 — the last page must have no cursor.
        let page = store.get_changes_page(None, None, 5).unwrap();
        assert_eq!(page.events.len(), 1);
        assert!(page.next_cursor.is_none(), "last page must not have a next cursor");
    }

    #[test]
    fn change_log_cursor_pagination_since_filter_combined() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        let t1 = datetime!(2026-06-01 08:00:00 UTC);
        let t2 = datetime!(2026-06-01 09:00:00 UTC);
        let t3 = datetime!(2026-06-01 10:00:00 UTC);
        let t4 = datetime!(2026-06-01 11:00:00 UTC);
        let t5 = datetime!(2026-06-01 12:00:00 UTC);

        for (i, ts) in [t1, t2, t3, t4, t5].iter().enumerate() {
            store
                .append_event(&ChangeEvent {
                    event_type: format!("ev_{i}"),
                    occurred_at: *ts,
                    entity_id: format!("e_{i}"),
                    actor: None,
                    details_json: None,
                })
                .unwrap();
        }

        // With since=t2 we should skip event at t1 and get ev_1..ev_4.
        let mut all_seen: Vec<String> = Vec::new();
        let mut cursor: Option<ChangeCursor> = None;
        loop {
            let page = store.get_changes_page(Some(t2), cursor.clone(), 2).unwrap();
            for event in &page.events {
                all_seen.push(event.event_type.clone());
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        // ev_0 is at t1 (before since=t2) so we should see 4 events: ev_1..ev_4.
        assert_eq!(all_seen.len(), 4, "expected 4 events after since filter");
        assert_eq!(all_seen, vec!["ev_1", "ev_2", "ev_3", "ev_4"]);
    }

    #[test]
    fn applies_all_migrations() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));

        store.ensure_schema().unwrap();

        let connection = rusqlite::Connection::open(store.path()).unwrap();
        let count: i64 =
            connection.query_row("SELECT COUNT(*) FROM migrations", [], |row| row.get(0)).unwrap();
        assert_eq!(count as usize, MIGRATIONS.len());
    }

    #[test]
    fn rolls_back_failed_migrations() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));

        let result = store.ensure_schema_with_migrations(&[
            ("0001_ok", "CREATE TABLE IF NOT EXISTS ok_table(id INTEGER PRIMARY KEY);"),
            ("0002_bad", "CREATE TABLE broken("),
        ]);
        assert!(result.is_err());

        let connection = rusqlite::Connection::open(store.path()).unwrap();
        let exists = connection
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'ok_table'",
                [],
                |_| Ok(()),
            )
            .optional()
            .unwrap()
            .is_some();
        assert!(!exists);
    }

    #[test]
    fn tracks_pending_and_stale_runs() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        let run = store
            .create_pending_run(
                "projects",
                "project_alpha",
                &[IngestManifestEntry {
                    run_id: 0,
                    drawer_id: DrawerId::new("project_alpha/backend/0001").unwrap(),
                    source_file: "auth.py".to_owned(),
                    content_hash: "hash-a".to_owned(),
                    status: IngestRunStatus::Pending,
                }],
                datetime!(2026-04-10 00:00:00 UTC),
            )
            .unwrap();

        let stale = store.stale_pending_runs(datetime!(2026-04-11 00:00:00 UTC)).unwrap();
        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0].run.id, run.id);

        store.mark_run_failed(run.id, "interrupted", datetime!(2026-04-11 00:05:00 UTC)).unwrap();
        let stale_after = store.stale_pending_runs(datetime!(2026-04-12 00:00:00 UTC)).unwrap();
        assert!(stale_after.is_empty());
    }

    #[test]
    fn stores_tool_state_and_config() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        store
            .put_tool_state(&ToolStateEntry {
                tool_name: "mcp".to_owned(),
                payload: "{\"ok\":true}".to_owned(),
                updated_at: datetime!(2026-04-11 09:00:00 UTC),
            })
            .unwrap();
        store
            .put_config(&ConfigEntry {
                config_key: "profile".to_owned(),
                config_value: "balanced".to_owned(),
            })
            .unwrap();

        assert_eq!(store.get_tool_state("mcp").unwrap().unwrap().payload, "{\"ok\":true}");
        assert_eq!(store.get_config("profile").unwrap().unwrap().config_value, "balanced");
    }

    #[test]
    fn lineage_updates_are_revisioned_and_default_is_unique() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();
        let created_at = datetime!(2026-08-16 10:00:00 UTC);

        let RevisionedWrite::Applied(created) = store
            .set_lineage(
                "dion-companion",
                "Dion Companion",
                "Provider-neutral collaborator",
                false,
                Some(0),
                created_at,
            )
            .unwrap()
        else {
            panic!("first lineage should be created");
        };
        assert_eq!(created.revision, 1);
        assert!(created.is_default);

        assert_eq!(
            store
                .set_lineage(
                    "dion-companion",
                    "Dion Companion",
                    "stale update",
                    false,
                    Some(0),
                    created_at + Duration::minutes(1),
                )
                .unwrap(),
            RevisionedWrite::Conflict { actual_revision: Some(1) }
        );

        let RevisionedWrite::Applied(updated) = store
            .set_lineage(
                "dion-companion",
                "Dion Companion",
                "Accumulated collaborator",
                false,
                Some(1),
                created_at + Duration::minutes(2),
            )
            .unwrap()
        else {
            panic!("matching revision should update");
        };
        assert_eq!(updated.revision, 2);
        assert_eq!(store.get_default_lineage().unwrap().unwrap().lineage_id, "dion-companion");

        let switched_at = created_at + Duration::minutes(3);
        let RevisionedWrite::Applied(switched) = store
            .set_lineage(
                "alternate-lineage",
                "Alternate Lineage",
                "A second provider-neutral lineage",
                true,
                Some(0),
                switched_at,
            )
            .unwrap()
        else {
            panic!("a new lineage should be created");
        };
        assert!(switched.is_default);
        let displaced = store.get_lineage("dion-companion").unwrap().unwrap();
        assert!(!displaced.is_default);
        assert_eq!(displaced.revision, 3);
        assert_eq!(displaced.updated_at, switched_at);
        assert_eq!(
            store
                .set_lineage(
                    "dion-companion",
                    "Dion Companion",
                    "stale default update",
                    false,
                    Some(2),
                    switched_at + Duration::minutes(1),
                )
                .unwrap(),
            RevisionedWrite::Conflict { actual_revision: Some(3) }
        );
    }

    #[test]
    fn self_observations_promote_supersede_and_feed_migrations() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();
        let now = datetime!(2026-08-16 11:00:00 UTC);
        assert!(matches!(
            store
                .set_lineage(
                    "dion-companion",
                    "Dion Companion",
                    "Provider-neutral collaborator",
                    true,
                    Some(0),
                    now,
                )
                .unwrap(),
            RevisionedWrite::Applied(_)
        ));

        let first = SelfObservationRecord {
            observation_id: "selfobs-first".to_owned(),
            lineage_id: "dion-companion".to_owned(),
            status: SelfObservationStatus::Candidate,
            scope: SelfObservationScope::Lineage,
            statement: "Representative tests matter more than green output.".to_owned(),
            behavioral_consequence: "Verify the failure mode can occur.".to_owned(),
            confidence: 0.8,
            author: "codex".to_owned(),
            model: Some("gpt-test".to_owned()),
            harness: Some("codex".to_owned()),
            evidence: vec!["drawer-1".to_owned()],
            counterevidence: Vec::new(),
            supersedes_observation_id: None,
            revision: 1,
            created_at: now,
            updated_at: now,
        };
        store.propose_self_observation(&first).unwrap();
        assert!(matches!(
            store
                .review_self_observation(
                    "selfobs-first",
                    1,
                    SelfObservationStatus::Promoted,
                    "dion",
                    "confirmed",
                    now + Duration::minutes(1),
                )
                .unwrap(),
            RevisionedWrite::Applied(_)
        ));

        let second = SelfObservationRecord {
            observation_id: "selfobs-second".to_owned(),
            statement: "Representative tests and dangerous-direction canaries matter.".to_owned(),
            supersedes_observation_id: Some("selfobs-first".to_owned()),
            created_at: now + Duration::minutes(2),
            updated_at: now + Duration::minutes(2),
            ..first.clone()
        };
        store.propose_self_observation(&second).unwrap();
        store
            .review_self_observation(
                "selfobs-second",
                1,
                SelfObservationStatus::Promoted,
                "dion",
                "more precise",
                now + Duration::minutes(3),
            )
            .unwrap();
        assert_eq!(
            store.get_self_observation("selfobs-first").unwrap().unwrap().status,
            SelfObservationStatus::Superseded
        );
        let promoted = store
            .list_self_observations(
                "dion-companion",
                &[SelfObservationStatus::Promoted],
                10,
            )
            .unwrap();
        assert_eq!(promoted.len(), 1);
        assert_eq!(promoted[0].observation_id, "selfobs-second");

        store
            .record_lineage_migration(&LineageMigrationRecord {
                migration_id: "migration-1".to_owned(),
                lineage_id: "dion-companion".to_owned(),
                from_model: Some("model-a".to_owned()),
                from_harness: Some("harness-a".to_owned()),
                to_model: "model-b".to_owned(),
                to_harness: "harness-b".to_owned(),
                summary: "Judgment persisted with a different voice.".to_owned(),
                continuities: vec!["representative tests".to_owned()],
                changes: vec!["shorter answers".to_owned()],
                evidence: vec!["selfobs-second".to_owned()],
                author: "codex".to_owned(),
                created_at: now + Duration::minutes(4),
            })
            .unwrap();
        let migrations = store.list_lineage_migrations("dion-companion", 5).unwrap();
        assert_eq!(migrations.len(), 1);
        assert_eq!(migrations[0].to_model, "model-b");
    }

    #[test]
    fn rejects_superseding_an_observation_that_is_no_longer_promoted() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();
        let now = datetime!(2026-08-16 12:00:00 UTC);

        store
            .set_lineage(
                "dion-companion",
                "Dion Companion",
                "Provider-neutral collaborator",
                true,
                Some(0),
                now,
            )
            .unwrap();

        let first = SelfObservationRecord {
            observation_id: "selfobs-concurrent-first".to_owned(),
            lineage_id: "dion-companion".to_owned(),
            status: SelfObservationStatus::Candidate,
            scope: SelfObservationScope::Lineage,
            statement: "The first observation is current until replaced.".to_owned(),
            behavioral_consequence: "Keep it in the packet until a reviewed replacement exists."
                .to_owned(),
            confidence: 0.8,
            author: "codex".to_owned(),
            model: None,
            harness: None,
            evidence: vec!["test:concurrent-first".to_owned()],
            counterevidence: Vec::new(),
            supersedes_observation_id: None,
            revision: 1,
            created_at: now,
            updated_at: now,
        };
        store.propose_self_observation(&first).unwrap();
        store
            .review_self_observation(
                &first.observation_id,
                1,
                SelfObservationStatus::Promoted,
                "dion",
                "confirmed",
                now + Duration::minutes(1),
            )
            .unwrap();

        for observation_id in ["selfobs-concurrent-second", "selfobs-concurrent-third"] {
            store
                .propose_self_observation(&SelfObservationRecord {
                    observation_id: observation_id.to_owned(),
                    statement: format!("Replacement candidate {observation_id}."),
                    supersedes_observation_id: Some(first.observation_id.clone()),
                    created_at: now + Duration::minutes(2),
                    updated_at: now + Duration::minutes(2),
                    ..first.clone()
                })
                .unwrap();
        }

        store
            .review_self_observation(
                "selfobs-concurrent-second",
                1,
                SelfObservationStatus::Promoted,
                "dion",
                "first replacement wins",
                now + Duration::minutes(3),
            )
            .unwrap();

        let error = store
            .review_self_observation(
                "selfobs-concurrent-third",
                1,
                SelfObservationStatus::Promoted,
                "dion",
                "stale replacement",
                now + Duration::minutes(4),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            StorageError::Invariant(message)
                if message.contains("selfobs-concurrent-first")
                    && message.contains("must currently be promoted")
        ));
        assert_eq!(
            store
                .get_self_observation("selfobs-concurrent-third")
                .unwrap()
                .unwrap()
                .status,
            SelfObservationStatus::Candidate
        );
    }

    #[test]
    fn stores_entities_and_graph_documents() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        store
            .upsert_entity(&EntityRecord {
                entity_id: "person:kai".to_owned(),
                entity_type: "person".to_owned(),
                payload: json!({ "name": "Kai" }),
                updated_at: datetime!(2026-04-11 11:00:00 UTC),
            })
            .unwrap();
        store
            .put_graph_document(&GraphDocument {
                graph_key: "palace".to_owned(),
                payload: json!({ "rooms": ["backend"] }),
                updated_at: datetime!(2026-04-11 11:05:00 UTC),
            })
            .unwrap();

        assert_eq!(
            store.get_entity("person:kai").unwrap().unwrap().payload,
            json!({ "name": "Kai" })
        );
        assert_eq!(
            store.get_graph_document("palace").unwrap().unwrap().payload,
            json!({ "rooms": ["backend"] })
        );
        assert_eq!(store.list_entities().unwrap().len(), 1);
        assert_eq!(store.list_entities_by_ids(&[String::from("person:kai")]).unwrap().len(), 1);
    }

    #[test]
    fn stores_and_invalidates_knowledge_graph_facts() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        let fact = KnowledgeGraphFact {
            fact_id: "fact-1".to_owned(),
            subject_entity_id: "project:rust_rewrite".to_owned(),
            predicate: "targets".to_owned(),
            object_entity_id: "project:phase_1".to_owned(),
            valid_from: Some(date!(2026 - 04 - 03)),
            valid_to: None,
            confidence: 1.0,
            source_drawer_id: Some(DrawerId::new("project_alpha/backend/0001").unwrap()),
            source_file: Some("docs/plan.md".to_owned()),
            created_at: datetime!(2026-04-03 09:00:00 UTC),
            updated_at: datetime!(2026-04-03 09:00:00 UTC),
        };

        store.upsert_fact(&fact).unwrap();

        let stored = store.get_fact("fact-1").unwrap().unwrap();
        assert_eq!(stored.predicate, "targets");
        assert_eq!(store.list_facts_for_entity("project:rust_rewrite").unwrap().len(), 1);
        assert_eq!(
            store
                .find_active_fact("project:rust_rewrite", "targets", "project:phase_1")
                .unwrap()
                .unwrap()
                .fact_id,
            "fact-1"
        );
        assert_eq!(store.list_facts_limited(1).unwrap().len(), 1);

        let invalidated = store
            .invalidate_active_fact(
                "project:rust_rewrite",
                "targets",
                "project:phase_1",
                date!(2026 - 04 - 04),
                datetime!(2026-04-04 00:00:00 UTC),
            )
            .unwrap();
        assert_eq!(invalidated, 1);
        assert_eq!(
            store.get_fact("fact-1").unwrap().unwrap().valid_to,
            Some(date!(2026 - 04 - 04))
        );
    }

    #[test]
    fn does_not_invalidate_future_dated_facts() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        store
            .upsert_fact(&KnowledgeGraphFact {
                fact_id: "fact-future".to_owned(),
                subject_entity_id: "project:rust_rewrite".to_owned(),
                predicate: "starts".to_owned(),
                object_entity_id: "concept:phase_7".to_owned(),
                valid_from: Some(date!(2026 - 04 - 05)),
                valid_to: None,
                confidence: 1.0,
                source_drawer_id: None,
                source_file: None,
                created_at: datetime!(2026-04-03 09:00:00 UTC),
                updated_at: datetime!(2026-04-03 09:00:00 UTC),
            })
            .unwrap();

        let invalidated = store
            .invalidate_active_fact(
                "project:rust_rewrite",
                "starts",
                "concept:phase_7",
                date!(2026 - 04 - 04),
                datetime!(2026-04-04 00:00:00 UTC),
            )
            .unwrap();

        assert_eq!(invalidated, 0);
        assert_eq!(store.get_fact("fact-future").unwrap().unwrap().valid_to, None);
    }

    #[test]
    fn creates_composite_lookup_index_for_knowledge_graph_facts() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        let connection = rusqlite::Connection::open(store.path()).unwrap();
        let exists = connection
            .query_row(
                "SELECT 1 FROM sqlite_master
                 WHERE type = 'index'
                   AND name = 'idx_kg_facts_subject_predicate_object_active'",
                [],
                |_| Ok(()),
            )
            .optional()
            .unwrap()
            .is_some();

        assert!(exists);
    }

    #[test]
    fn tracks_ingested_files_by_source_key() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        let run_a = store
            .create_pending_run(
                "convos",
                "convos:wing-a:exchange:root:file.txt",
                &[IngestManifestEntry {
                    run_id: 0,
                    drawer_id: DrawerId::new("wing-a/decision/0001").unwrap(),
                    source_file: "file.txt".to_owned(),
                    content_hash: "hash-a".to_owned(),
                    status: IngestRunStatus::Pending,
                }],
                datetime!(2026-04-11 12:00:00 UTC),
            )
            .unwrap();
        store
            .mark_run_committed(
                run_a.id,
                "convos:wing-a:exchange:root:file.txt",
                "file.txt",
                "hash-a",
                1,
                datetime!(2026-04-11 12:01:00 UTC),
            )
            .unwrap();

        let run_b = store
            .create_pending_run(
                "convos",
                "convos:wing-a:general:root:file.txt",
                &[IngestManifestEntry {
                    run_id: 0,
                    drawer_id: DrawerId::new("wing-a/milestone/0001").unwrap(),
                    source_file: "file.txt".to_owned(),
                    content_hash: "hash-b".to_owned(),
                    status: IngestRunStatus::Pending,
                }],
                datetime!(2026-04-11 12:02:00 UTC),
            )
            .unwrap();
        store
            .mark_run_committed(
                run_b.id,
                "convos:wing-a:general:root:file.txt",
                "file.txt",
                "hash-b",
                1,
                datetime!(2026-04-11 12:03:00 UTC),
            )
            .unwrap();

        let exchange =
            store.get_ingested_file("convos:wing-a:exchange:root:file.txt").unwrap().unwrap();
        let general =
            store.get_ingested_file("convos:wing-a:general:root:file.txt").unwrap().unwrap();

        assert_eq!(exchange.content_hash, "hash-a");
        assert_eq!(general.content_hash, "hash-b");
    }

    #[test]
    fn lists_committed_drawer_ids_for_exact_source_key() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        let run_a = store
            .create_pending_run(
                "convos",
                "convos:wing-a:exchange:root-a:file.txt",
                &[IngestManifestEntry {
                    run_id: 0,
                    drawer_id: DrawerId::new("wing-a/general/a-0000").unwrap(),
                    source_file: "file.txt".to_owned(),
                    content_hash: "hash-a".to_owned(),
                    status: IngestRunStatus::Pending,
                }],
                datetime!(2026-04-11 12:00:00 UTC),
            )
            .unwrap();
        store
            .mark_run_committed(
                run_a.id,
                "convos:wing-a:exchange:root-a:file.txt",
                "file.txt",
                "hash-a",
                1,
                datetime!(2026-04-11 12:01:00 UTC),
            )
            .unwrap();

        let run_b = store
            .create_pending_run(
                "convos",
                "convos:wing-a:exchange:root-b:file.txt",
                &[IngestManifestEntry {
                    run_id: 0,
                    drawer_id: DrawerId::new("wing-a/general/b-0000").unwrap(),
                    source_file: "file.txt".to_owned(),
                    content_hash: "hash-b".to_owned(),
                    status: IngestRunStatus::Pending,
                }],
                datetime!(2026-04-11 12:02:00 UTC),
            )
            .unwrap();
        store
            .mark_run_committed(
                run_b.id,
                "convos:wing-a:exchange:root-b:file.txt",
                "file.txt",
                "hash-b",
                1,
                datetime!(2026-04-11 12:03:00 UTC),
            )
            .unwrap();

        let root_a = store
            .committed_drawer_ids_for_source_key("convos:wing-a:exchange:root-a:file.txt")
            .unwrap();
        let root_b = store
            .committed_drawer_ids_for_source_key("convos:wing-a:exchange:root-b:file.txt")
            .unwrap();

        assert_eq!(root_a, vec![DrawerId::new("wing-a/general/a-0000").unwrap()]);
        assert_eq!(root_b, vec![DrawerId::new("wing-a/general/b-0000").unwrap()]);
    }

    #[test]
    fn reads_legacy_migrated_ingest_rows_via_scoped_lookup() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        let connection = rusqlite::Connection::open(store.path()).unwrap();
        connection
            .execute_batch(
                r#"
CREATE TABLE ingest_files (
    source_file TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL,
    last_ingested_at TEXT NOT NULL,
    ingest_kind TEXT NOT NULL,
    drawer_count INTEGER NOT NULL
);
INSERT INTO ingest_files (source_file, content_hash, last_ingested_at, ingest_kind, drawer_count)
VALUES ('chat/file.txt', 'legacy-hash', '2026-04-11T12:00:00Z', 'convos', 2);
                "#,
            )
            .unwrap();
        drop(connection);

        store.ensure_schema().unwrap();

        let migrated = store
            .get_ingested_file("convos:wing-a:exchange:root123:chat/file.txt")
            .unwrap()
            .unwrap();

        assert_eq!(migrated.source_key, "convos:chat/file.txt");
        assert_eq!(migrated.content_hash, "legacy-hash");
        assert_eq!(migrated.drawer_count, 2);
    }

    #[test]
    fn diary_store_rejects_overlong_summary() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();
        let did = DrawerId::new("test/overlong").unwrap();

        // Summary at exactly 400 chars is accepted.
        let ok_summary = "a".repeat(400);
        store.store_diary_summary(&did, &ok_summary).unwrap();
        assert_eq!(store.get_diary_summary(&did).unwrap(), Some(ok_summary));

        // Summary at 401 chars is rejected.
        let long_summary = "b".repeat(401);
        let err = store.store_diary_summary(&did, &long_summary).unwrap_err();
        assert!(
            err.to_string().contains("diary summary exceeds maximum length of 400"),
            "expected invariant error for overlong summary, got: {err}"
        );

        // The existing valid summary was not overwritten.
        assert_eq!(store.get_diary_summary(&did).unwrap(), Some("a".repeat(400)));
    }

    #[test]
    fn create_pending_diary_run_rejects_overlong_summary() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();
        let did = DrawerId::new("diary/overlong").unwrap();

        let result = store.create_pending_diary_run(
            "diary",
            "diary:test",
            &[IngestManifestEntry {
                run_id: 0,
                drawer_id: did.clone(),
                source_file: "diary:test".to_owned(),
                content_hash: "hash".to_owned(),
                status: IngestRunStatus::Pending,
            }],
            &did,
            &"x".repeat(401),
            datetime!(2026-07-01 00:00:00 UTC),
        );
        assert!(result.is_err(), "expected error for overlong summary in create_pending_diary_run");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("diary summary exceeds maximum length of 400"),
            "expected invariant error, got: {err}"
        );

        // No pending run was created.
        let stale = store.stale_pending_runs(datetime!(2026-07-02 00:00:00 UTC)).unwrap();
        assert!(stale.is_empty(), "no pending run should exist after rejected summary");
    }

    #[test]
    fn create_pending_diary_run_does_not_overwrite_an_existing_summary() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();
        let did = DrawerId::new("diary/atomic-summary").unwrap();
        let entry = IngestManifestEntry {
            run_id: 0,
            drawer_id: did.clone(),
            source_file: "diary:test".to_owned(),
            content_hash: "hash".to_owned(),
            status: IngestRunStatus::Pending,
        };

        let first = store
            .create_pending_diary_run(
                "diary",
                "diary:first",
                &[entry.clone()],
                &did,
                "first summary",
                datetime!(2026-07-01 00:00:00 UTC),
            )
            .unwrap();
        assert!(first.is_some());

        let second = store
            .create_pending_diary_run(
                "diary",
                "diary:second",
                &[entry],
                &did,
                "second summary",
                datetime!(2026-07-01 00:00:01 UTC),
            )
            .unwrap();
        assert!(second.is_none(), "the existing summary owns this drawer ID");
        assert_eq!(store.get_diary_summary(&did).unwrap(), Some("first summary".to_owned()));

        let pending = store.stale_pending_runs(datetime!(2026-07-02 00:00:00 UTC)).unwrap();
        assert_eq!(pending.len(), 1, "the rejected writer must not leave a pending run");
    }

    #[test]
    fn get_diary_summaries_fetches_in_bulk() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        let id_a = DrawerId::new("diary/a").unwrap();
        let id_b = DrawerId::new("diary/b").unwrap();
        let id_c = DrawerId::new("diary/c").unwrap();

        store.store_diary_summary(&id_a, "summary a").unwrap();
        store.store_diary_summary(&id_b, "summary b").unwrap();

        let results =
            store.get_diary_summaries(&[id_a.clone(), id_b.clone(), id_c.clone()]).unwrap();
        assert_eq!(results.len(), 2, "only stored entries should be returned");
        assert!(results.contains(&(id_a, "summary a".to_owned())));
        assert!(results.contains(&(id_b, "summary b".to_owned())));
    }

    #[test]
    fn get_diary_summaries_empty_input() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        let results: Vec<(DrawerId, String)> = store.get_diary_summaries(&[]).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn diary_summary_check_constraint_enforced_at_sqlite_level() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        let connection = rusqlite::Connection::open(store.path()).unwrap();
        let result = connection.execute(
            "INSERT INTO diary_summaries (entry_id, summary) VALUES (?1, ?2)",
            rusqlite::params!["diary/too-long", &"x".repeat(401)],
        );
        assert!(result.is_err(), "CHECK constraint should reject summary longer than 400 chars");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("CHECK constraint") || err_msg.contains("constraint failed"),
            "expected constraint failure, got: {err_msg}"
        );
    }

    #[test]
    fn migration_0007_adds_check_constraint_and_truncates_existing_overlong() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));

        // Create the database without the constraint (migration up to 0006).
        let connection = rusqlite::Connection::open(store.path()).unwrap();
        connection.execute_batch(
            r#"
CREATE TABLE IF NOT EXISTS migrations (
    version TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL
);
INSERT INTO migrations (version, applied_at) VALUES ('0006_diary_summaries', '2026-07-01T00:00:00Z');
CREATE TABLE IF NOT EXISTS diary_summaries (
    entry_id TEXT PRIMARY KEY,
    summary TEXT NOT NULL
);
INSERT INTO diary_summaries (entry_id, summary) VALUES ('diary/short', 'valid summary');
INSERT INTO diary_summaries (entry_id, summary) VALUES ('diary/long', 'x' || 'y');
            "#,
        )
        .unwrap();
        // Insert a 500-char summary using a separate execute.
        connection
            .execute(
                "UPDATE diary_summaries SET summary = ?1 WHERE entry_id = 'diary/long'",
                rusqlite::params![&"z".repeat(500)],
            )
            .unwrap();
        drop(connection);

        // Run the migration (schema up to 0007).
        store.ensure_schema().unwrap();

        // The long summary should have been truncated to 400 chars.
        let connection = rusqlite::Connection::open(store.path()).unwrap();
        let short: String = connection
            .query_row(
                "SELECT summary FROM diary_summaries WHERE entry_id = 'diary/short'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(short, "valid summary");

        let truncated: String = connection
            .query_row(
                "SELECT summary FROM diary_summaries WHERE entry_id = 'diary/long'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(truncated.len(), 400, "long summary should be truncated to 400 chars");
        assert_eq!(truncated, "z".repeat(400));

        // Verify the CHECK constraint is active — inserting >400 chars should fail.
        let result = connection.execute(
            "INSERT INTO diary_summaries (entry_id, summary) VALUES (?1, ?2)",
            rusqlite::params!["diary/new-long", &"x".repeat(401)],
        );
        assert!(result.is_err(), "CHECK constraint must reject overlong summaries after migration");
    }

    #[test]
    fn lease_claim_succeeds_when_no_lease_exists() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();
        assert!(store.lease_status().unwrap().is_none());

        let claimed = store.try_claim_lease("worker-a", Duration::minutes(5)).unwrap();
        assert!(claimed, "should claim lease when none exists");
    }

    #[test]
    fn lease_claim_rejects_concurrent_holder() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        assert!(store.try_claim_lease("worker-a", Duration::minutes(5)).unwrap());
        let second = store.try_claim_lease("worker-b", Duration::minutes(5)).unwrap();
        assert!(!second, "worker-b must not claim lease held by worker-a");
    }

    #[test]
    fn lease_claim_reclaims_expired_lease() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        assert!(store.try_claim_lease("worker-a", Duration::ZERO).unwrap());

        let reclaimed = store.try_claim_lease("worker-b", Duration::minutes(5)).unwrap();
        assert!(reclaimed, "worker-b must reclaim expired lease");

        let status = store.lease_status().unwrap().unwrap();
        assert_eq!(status.0, "worker-b");
    }

    #[test]
    fn lease_timestamp_comparisons_preserve_fractional_order() {
        let tempdir = tempfile::tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("palace.db"));
        store.ensure_schema().unwrap();

        let conn = store.open_connection().unwrap();
        let expires_at = OffsetDateTime::from_unix_timestamp(1_767_996_800).unwrap()
            + Duration::milliseconds(120);
        conn.execute(
            "UPDATE maintenance_leases SET holder = 'worker-a', expires_at = ?1 WHERE lease_id = 'maintenance'",
            [encode_lease_time(expires_at)],
        )
        .unwrap();

        let earlier = OffsetDateTime::from_unix_timestamp(1_767_996_800).unwrap()
            + Duration::milliseconds(100);
        let is_expired: bool = conn
            .query_row(
                "SELECT expires_at < ?1 FROM maintenance_leases WHERE lease_id = 'maintenance'",
                [encode_lease_time(earlier)],
                |row| row.get(0),
            )
            .unwrap();

        // A 100 ms timestamp must remain earlier than a 120 ms lease expiry.
        // Variable-width RFC3339 fractions compare incorrectly as SQLite TEXT.
        assert!(!is_expired);
    }

    #[test]
    fn lease_renew_extends_ttl() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        let short = Duration::seconds(10);
        assert!(store.try_claim_lease("worker-a", short).unwrap());

        let status_before = store.lease_status().unwrap().unwrap();
        let renewed = store.renew_lease("worker-a", Duration::hours(1)).unwrap();
        assert!(renewed, "should renew own lease");

        let status_after = store.lease_status().unwrap().unwrap();
        assert_eq!(status_after.0, "worker-a");
        assert!(status_after.1 > status_before.1, "expiry should be extended after renewal");
    }

    #[test]
    fn lease_renew_fails_for_wrong_holder() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        assert!(store.try_claim_lease("worker-a", Duration::minutes(5)).unwrap());
        let renewed = store.renew_lease("worker-b", Duration::hours(1)).unwrap();
        assert!(!renewed, "worker-b must not renew a lease held by worker-a");
    }

    #[test]
    fn lease_renew_fails_when_expired() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        assert!(store.try_claim_lease("worker-a", Duration::ZERO).unwrap());

        let renewed = store.renew_lease("worker-a", Duration::hours(1)).unwrap();
        assert!(!renewed, "must not renew an already-expired lease");
    }

    #[test]
    fn lease_release_clears_lease() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        assert!(store.try_claim_lease("worker-a", Duration::minutes(5)).unwrap());
        let released = store.release_lease("worker-a").unwrap();
        assert!(released, "should release own lease");
        assert!(store.lease_status().unwrap().is_none(), "lease should be empty after release");
    }

    #[test]
    fn lease_release_fails_for_wrong_holder() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        assert!(store.try_claim_lease("worker-a", Duration::minutes(5)).unwrap());
        let released = store.release_lease("worker-b").unwrap();
        assert!(!released, "worker-b must not release lease held by worker-a");

        let status = store.lease_status().unwrap().unwrap();
        assert_eq!(status.0, "worker-a", "lease should still be held by worker-a");
    }

    #[test]
    fn lease_claim_renews_when_already_held_by_same_holder() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        assert!(store.try_claim_lease("worker-a", Duration::seconds(10)).unwrap());
        let status_before = store.lease_status().unwrap().unwrap();

        let claimed_again = store.try_claim_lease("worker-a", Duration::hours(1)).unwrap();
        assert!(claimed_again, "same holder should reclaim/refresh own lease");

        let status_after = store.lease_status().unwrap().unwrap();
        assert!(status_after.1 > status_before.1, "expiry should be extended");
    }

    #[test]
    fn lease_claim_contention_returns_false_immediately() {
        let tempdir = tempdir().unwrap();
        let db_path = tempdir.path().join("storage.sqlite3");
        let store = SqliteOperationalStore::new(&db_path);
        store.ensure_schema().unwrap();

        // Prime the lease row so the table exists.
        assert!(store.try_claim_lease("worker-a", Duration::minutes(5)).unwrap());
        assert!(store.release_lease("worker-a").unwrap());

        // Open a second raw connection and hold a write transaction.
        let blocker = rusqlite::Connection::open(&db_path).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

        // The contender opens a separate connection with busy_timeout=0, so
        // BEGIN IMMEDIATE should return SQLITE_BUSY immediately.
        let result = store.try_claim_lease("contender", Duration::minutes(5));
        match &result {
            Ok(false) => {} // expected: non-error skip
            other => panic!("expected Ok(false) under contention, got: {other:?}"),
        }

        // Release the blocker so the test teardown isn't racy.
        blocker.execute_batch("ROLLBACK").unwrap();

        // After contention is resolved, the contender may claim the lease.
        assert!(
            store.try_claim_lease("contender", Duration::minutes(5)).unwrap(),
            "should claim lease after blocker releases"
        );
    }
}

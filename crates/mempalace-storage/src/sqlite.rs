use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, params, params_from_iter};
use time::OffsetDateTime;

use crate::error::{Result, StorageError};
use crate::types::{
    ConfigEntry, EntityRecord, GraphDocument, IngestFileRecord, IngestManifestEntry, IngestRun,
    IngestRunStatus, KnowledgeGraphFact, RetryableRun, ToolStateEntry,
};
use mempalace_core::DrawerId;
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
    fn get_ingested_file(&self, source_key: &str) -> Result<Option<IngestFileRecord>>;
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

fn encode_time(value: OffsetDateTime) -> String {
    value
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| value.unix_timestamp().to_string())
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

#[cfg(test)]
mod tests {
    use rusqlite::OptionalExtension;
    use tempfile::tempdir;
    use time::macros::datetime;

    use super::{
        EntityRegistryStore, GraphStore, IngestManifestStore, KnowledgeGraphStore, MIGRATIONS,
        SqliteOperationalStore, ToolStateStore,
    };
    use crate::types::{
        ConfigEntry, EntityRecord, GraphDocument, IngestManifestEntry, IngestRunStatus,
        KnowledgeGraphFact, ToolStateEntry,
    };
    use mempalace_core::DrawerId;
    use serde_json::json;
    use time::macros::date;

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
}

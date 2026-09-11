//! Recoverable local half of a replicated source replacement.

use std::fs::{File, OpenOptions};

use fs4::FileExt;
use mempalace_core::{DrawerId, DrawerRecord};
use serde::{Deserialize, Serialize};

use crate::{
    DrawerStore, IngestManifestStore, NewOutboxOperation, OutboxOperation, OutboxState,
    OutboxStore, Result, RevisionedWrite, StorageEngine, StorageError,
};

/// Exact local effect retained until a replicated file has committed locally.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicatedSource {
    /// Canonical local source identity.
    pub source_key: String,
    /// Repository-relative path.
    pub source_file: String,
    /// Hash of the prepared file and routing configuration.
    pub content_hash: String,
    /// Prepared drawers, including the embeddings needed for crash recovery.
    pub drawers: Vec<DrawerRecord>,
    /// Whether this effect removes the source and its manifest.
    pub remove: bool,
    /// Old drawer IDs captured before replacement, so interrupted cleanup is replayable.
    pub previous_ids: Vec<DrawerId>,
}

impl StorageEngine {
    /// Acquire a process-safe source lock, automatically released even after a process crash.
    /// Lock files are retained; deleting them could let two processes lock different inodes.
    pub async fn lock_ingest_source(&self, key: &str) -> Result<File> {
        let directory =
            self.layout().sqlite_path.parent().expect("database has a parent").join("ingest-locks");
        std::fs::create_dir_all(&directory)
            .map_err(|source| StorageError::Io { path: directory.clone(), source })?;
        let path = directory.join(blake3::hash(key.as_bytes()).to_hex().to_string());
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|source| StorageError::Io { path: path.clone(), source })?;
        loop {
            match FileExt::try_lock_exclusive(&file) {
                Ok(()) => return Ok(file),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
                Err(source) => return Err(StorageError::Io { path, source }),
            }
        }
    }

    /// Stage the exact local and remote effects before applying either. A failed or interrupted
    /// local write remains staged and is completed by recovery without rereading source files.
    pub async fn commit_replicated_source(
        &self,
        mut intent: NewOutboxOperation,
        mut local: ReplicatedSource,
    ) -> Result<OutboxOperation> {
        let _guard = self.lock_ingest_source(&local.source_key).await?;
        self.recover_replicated_source_locked(&local.source_key).await?;
        let outbox = OutboxStore::new(&self.layout().sqlite_path);
        outbox.ensure_schema()?;
        local.previous_ids = if let Some(existing) =
            outbox.find_by_key(&intent.created_by, &intent.idempotency_key)?
        {
            let saved: ReplicatedSource =
                serde_json::from_value(existing.payload["local"].clone())?;
            saved.previous_ids
        } else {
            self.operational_store().committed_drawer_ids_for_source_key(&local.source_key)?
        };
        intent.payload["local"] = serde_json::to_value(&local)?;
        let operation = outbox.enqueue(&intent)?;
        self.apply_replicated_source(&operation).await
    }

    /// Complete staged file effects in insertion order before newer mines can replace them.
    pub async fn recover_replicated_ingestion(&self) -> Result<()> {
        let outbox = OutboxStore::new(&self.layout().sqlite_path);
        outbox.ensure_schema()?;
        for source_key in outbox.staged_ingestion_sources()? {
            let _guard = self.lock_ingest_source(&source_key).await?;
            self.recover_replicated_source_locked(&source_key).await?;
        }
        Ok(())
    }

    async fn recover_replicated_source_locked(&self, source_key: &str) -> Result<()> {
        let outbox = OutboxStore::new(&self.layout().sqlite_path);
        outbox.ensure_schema()?;
        for operation in outbox.staged_ingestion_for_source(source_key)? {
            self.apply_replicated_source(&operation).await?;
        }
        Ok(())
    }

    async fn apply_replicated_source(
        &self,
        operation: &OutboxOperation,
    ) -> Result<OutboxOperation> {
        if operation.state != OutboxState::Staged {
            return Ok(operation.clone());
        }
        let local: ReplicatedSource = serde_json::from_value(operation.payload["local"].clone())?;
        let stale: Vec<_> = local
            .previous_ids
            .iter()
            .filter(|id| !local.drawers.iter().any(|drawer| &drawer.id == *id))
            .cloned()
            .collect();
        if local.remove {
            self.remove_source_key(&local.source_key).await?;
        } else {
            self.replace_source_drawers(
                "projects",
                &local.source_key,
                &local.source_file,
                local.content_hash,
                local.drawers,
            )
            .await?;
        }
        if !stale.is_empty() {
            self.drawer_store().delete_drawers(&stale).await?;
        }
        let outbox = OutboxStore::new(&self.layout().sqlite_path);
        match outbox.activate(&operation.operation_id, operation.revision)? {
            RevisionedWrite::Applied(operation) => Ok(operation),
            RevisionedWrite::Conflict { .. } => Err(StorageError::Invariant(
                "replicated source activation lost its source lock".to_owned(),
            )),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use mempalace_core::{EmbeddingProfile, RoomId, WingId};
    use serde_json::json;

    fn drawer(name: &str) -> DrawerRecord {
        DrawerRecord {
            id: DrawerId::new(format!("wing_test/general/{name}")).unwrap(),
            wing: WingId::new("wing_test").unwrap(),
            room: RoomId::new("general").unwrap(),
            hall: None,
            date: None,
            source_file: "a.rs".into(),
            chunk_index: 0,
            ingest_mode: "projects".into(),
            extract_mode: None,
            added_by: "test".into(),
            filed_at: time::OffsetDateTime::now_utc(),
            importance: None,
            emotional_weight: None,
            weight: None,
            content: name.into(),
            content_hash: name.into(),
            embedding: vec![0.1; 384],
            locator: None,
            view_metadata: None,
        }
    }

    fn intent(key: &str, local: &ReplicatedSource) -> NewOutboxOperation {
        NewOutboxOperation {
            created_by: "test".into(),
            idempotency_key: key.into(),
            mutation_kind: "ingest_file".into(),
            entity_id: local.source_key.clone(),
            destination_remote: "hub".into(),
            ordering_key: local.source_key.clone(),
            max_attempts: 10,
            payload: json!({"kind":"ingest_file", "local":local,
                "request":{"replication":{"batch_id":"batch-test", "record_id":key}}}),
        }
    }

    #[tokio::test]
    async fn replicated_ingest_recovers_every_local_boundary_and_removal() {
        for boundary in 0..5 {
            let directory = tempfile::tempdir().unwrap();
            let engine =
                StorageEngine::open(directory.path(), EmbeddingProfile::Balanced).await.unwrap();
            let old = drawer("old");
            let new = drawer("new");
            engine
                .replace_source_drawers(
                    "projects",
                    "source",
                    "a.rs",
                    "old".into(),
                    vec![old.clone()],
                )
                .await
                .unwrap();
            let local = ReplicatedSource {
                source_key: "source".into(),
                source_file: "a.rs".into(),
                content_hash: "new".into(),
                drawers: vec![new.clone()],
                remove: false,
                previous_ids: vec![old.id.clone()],
            };
            let outbox = OutboxStore::new(&engine.layout().sqlite_path);
            outbox.ensure_schema().unwrap();
            let operation = outbox.enqueue(&intent("first", &local)).unwrap();
            if boundary >= 1 {
                engine
                    .drawer_store()
                    .put_drawers(&[new.clone()], crate::DuplicateStrategy::Overwrite)
                    .await
                    .unwrap();
            }
            if boundary >= 2 {
                engine
                    .commit_ingest(crate::IngestCommitRequest {
                        ingest_kind: "projects".into(),
                        source_key: "source".into(),
                        source_file: "a.rs".into(),
                        content_hash: "new".into(),
                        drawers: vec![new.clone()],
                        duplicate_strategy: crate::DuplicateStrategy::Overwrite,
                    })
                    .await
                    .unwrap();
            }
            if boundary >= 3 {
                engine.drawer_store().delete_drawers(&[old.id.clone()]).await.unwrap();
            }
            if boundary >= 4 {
                outbox.activate(&operation.operation_id, operation.revision).unwrap();
            }
            drop(engine);
            let engine =
                StorageEngine::open(directory.path(), EmbeddingProfile::Balanced).await.unwrap();
            engine.recover_replicated_ingestion().await.unwrap();
            engine.recover_replicated_ingestion().await.unwrap();
            assert!(engine.drawer_store().get_drawer(&old.id).await.unwrap().is_none());
            assert!(engine.drawer_store().get_drawer(&new.id).await.unwrap().is_some());
            assert_eq!(
                outbox.get_operation(&operation.operation_id).unwrap().unwrap().state,
                OutboxState::Pending
            );
            let deletion = ReplicatedSource {
                drawers: vec![],
                remove: true,
                previous_ids: vec![new.id.clone()],
                ..local
            };
            let deletion = outbox.enqueue(&intent("remove", &deletion)).unwrap();
            // Crash after deleting the source metadata, before activation.
            engine.remove_source_key("source").await.unwrap();
            drop(engine);
            let engine =
                StorageEngine::open(directory.path(), EmbeddingProfile::Balanced).await.unwrap();
            engine.recover_replicated_ingestion().await.unwrap();
            assert!(engine.operational_store().get_ingested_file("source").unwrap().is_none());
            assert_eq!(
                outbox.get_operation(&deletion.operation_id).unwrap().unwrap().state,
                OutboxState::Pending
            );
            let backlog = outbox.ingestion_backlog().unwrap();
            assert_eq!(backlog["pending_batches"], 1);
            assert_eq!(backlog["files_by_state"]["pending"], 2);
        }
    }

    #[tokio::test]
    async fn replicated_ingest_new_write_recovers_predecessor_before_staging() {
        let directory = tempfile::tempdir().unwrap();
        let engine =
            StorageEngine::open(directory.path(), EmbeddingProfile::Balanced).await.unwrap();
        let outbox = OutboxStore::new(&engine.layout().sqlite_path);
        outbox.ensure_schema().unwrap();
        let local = ReplicatedSource {
            source_key: "source".into(),
            source_file: "a.rs".into(),
            content_hash: "one".into(),
            drawers: vec![drawer("one")],
            remove: false,
            previous_ids: vec![],
        };
        outbox.enqueue(&intent("first", &local)).unwrap();
        let second =
            ReplicatedSource { content_hash: "two".into(), drawers: vec![drawer("two")], ..local };
        let operation = engine
            .commit_replicated_source(intent("second", &second), second.clone())
            .await
            .unwrap();
        let replay =
            engine.commit_replicated_source(intent("second", &second), second).await.unwrap();
        assert_eq!(replay.operation_id, operation.operation_id);
        assert_eq!(
            engine.operational_store().get_ingested_file("source").unwrap().unwrap().content_hash,
            "two"
        );
        assert!(outbox.staged_ingestion_sources().unwrap().is_empty());
        let first =
            outbox.claim_next("hub", "worker", time::Duration::minutes(1)).unwrap().unwrap();
        assert_eq!(first.idempotency_key, "first");
        assert!(outbox.claim_next("hub", "other", time::Duration::minutes(1)).unwrap().is_none());
    }
}

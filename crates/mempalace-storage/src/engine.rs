use std::collections::HashSet;
use std::path::Path;

use tracing::warn;

use crate::error::Result;
use crate::lance::LanceDrawerStore;
use crate::sqlite::{IngestManifestStore, SqliteOperationalStore};
use crate::types::{
    DrawerFilter, DrawerStore, DuplicateStrategy, IngestCommitRequest, IngestManifestEntry,
    RetryableRun, StorageLayout,
};
use mempalace_core::{DrawerId, EmbeddingProfile};
use time::{Duration, OffsetDateTime};

#[derive(Debug, Clone)]
pub struct StorageEngine {
    layout: StorageLayout,
    drawer_store: LanceDrawerStore,
    operational_store: SqliteOperationalStore,
    stale_after: Duration,
}

impl StorageEngine {
    pub async fn open(root: impl AsRef<Path>, profile: EmbeddingProfile) -> Result<Self> {
        let layout = StorageLayout::new(root);
        let engine = Self {
            drawer_store: LanceDrawerStore::new(&layout.lancedb_dir, profile),
            operational_store: SqliteOperationalStore::new(&layout.sqlite_path),
            layout,
            stale_after: Duration::hours(1),
        };

        engine.operational_store.ensure_schema()?;
        engine.drawer_store.ensure_schema().await?;
        engine.reconcile().await?;
        Ok(engine)
    }

    pub fn layout(&self) -> &StorageLayout {
        &self.layout
    }

    pub fn drawer_store(&self) -> &LanceDrawerStore {
        &self.drawer_store
    }

    pub fn operational_store(&self) -> &SqliteOperationalStore {
        &self.operational_store
    }

    pub async fn commit_ingest(&self, request: IngestCommitRequest) -> Result<i64> {
        let now = OffsetDateTime::now_utc();
        let manifests = request
            .drawers
            .iter()
            .map(|drawer| IngestManifestEntry {
                run_id: 0,
                drawer_id: drawer.id.clone(),
                source_file: request.source_file.clone(),
                content_hash: drawer.content_hash.clone(),
                status: crate::types::IngestRunStatus::Pending,
            })
            .collect::<Vec<_>>();

        let run = self.operational_store.create_pending_run(
            &request.ingest_kind,
            &request.source_key,
            &manifests,
            now,
        )?;

        let write_result =
            self.drawer_store.put_drawers(&request.drawers, request.duplicate_strategy).await;

        match write_result {
            Ok(()) => {
                self.operational_store.mark_run_committed(
                    run.id,
                    &request.source_key,
                    &request.source_file,
                    &request.content_hash,
                    request.drawers.len(),
                    OffsetDateTime::now_utc(),
                )?;
                Ok(run.id)
            }
            Err(error) => {
                self.operational_store.mark_run_failed(
                    run.id,
                    &error.to_string(),
                    OffsetDateTime::now_utc(),
                )?;
                Err(error)
            }
        }
    }

    pub async fn reconcile(&self) -> Result<()> {
        let stale_cutoff = OffsetDateTime::now_utc() - self.stale_after;
        let stale_runs = self.operational_store.stale_pending_runs(stale_cutoff)?;
        self.prune_orphaned_rows(&stale_runs).await?;

        for retryable in stale_runs {
            warn!(run_id = retryable.run.id, "marking stale pending ingest run as failed");
            self.operational_store.mark_run_failed(
                retryable.run.id,
                "stale pending ingest run",
                OffsetDateTime::now_utc(),
            )?;
        }

        Ok(())
    }

    async fn prune_orphaned_rows(&self, stale_runs: &[RetryableRun]) -> Result<()> {
        let committed_ids =
            self.operational_store.committed_drawer_ids()?.into_iter().collect::<HashSet<_>>();
        let all_drawers = self.drawer_store.list_drawers(&DrawerFilter::default()).await?;

        let stale_ids =
            stale_runs.iter().flat_map(|run| run.chunk_ids.iter().cloned()).collect::<HashSet<_>>();

        let orphaned = all_drawers
            .into_iter()
            .map(|record| record.id)
            .filter(|id| !committed_ids.contains(id) || stale_ids.contains(id))
            .collect::<Vec<DrawerId>>();

        if orphaned.is_empty() {
            return Ok(());
        }

        self.drawer_store.delete_drawers(&orphaned).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use time::macros::{date, datetime};

    use super::StorageEngine;
    use crate::sqlite::IngestManifestStore;
    use crate::types::{DrawerFilter, DrawerStore, DuplicateStrategy, IngestCommitRequest};
    use mempalace_core::{DrawerId, DrawerRecord, EmbeddingProfile, RoomId, WingId};

    fn embedding(seed: [f32; 4]) -> Vec<f32> {
        let mut values = Vec::with_capacity(EmbeddingProfile::Balanced.metadata().dimensions);
        while values.len() < EmbeddingProfile::Balanced.metadata().dimensions {
            values.extend(seed);
        }
        values.truncate(EmbeddingProfile::Balanced.metadata().dimensions);
        values
    }

    fn record(id: &str, source_file: &str, seed: [f32; 4]) -> DrawerRecord {
        DrawerRecord {
            id: DrawerId::new(id).unwrap(),
            wing: WingId::new("project_alpha").unwrap(),
            room: RoomId::new("backend").unwrap(),
            hall: Some("facts".to_owned()),
            date: Some(date!(2026 - 04 - 11)),
            source_file: source_file.to_owned(),
            chunk_index: 0,
            ingest_mode: "projects".to_owned(),
            extract_mode: Some("full".to_owned()),
            added_by: "tester".to_owned(),
            filed_at: datetime!(2026-04-11 10:00:00 UTC),
            importance: Some(0.8),
            emotional_weight: Some(0.1),
            weight: Some(1.0),
            content: format!("payload-{id}"),
            content_hash: format!("hash-{id}"),
            embedding: embedding(seed),
        }
    }

    #[tokio::test]
    async fn commits_ingest_across_both_stores() {
        let tempdir = tempdir().unwrap();
        let engine = StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();

        let run_id = engine
            .commit_ingest(IngestCommitRequest {
                ingest_kind: "projects".to_owned(),
                source_key: "project_alpha".to_owned(),
                source_file: "auth.py".to_owned(),
                content_hash: "file-hash".to_owned(),
                drawers: vec![record(
                    "project_alpha/backend/0001",
                    "auth.py",
                    [1.0, 0.0, 0.0, 0.0],
                )],
                duplicate_strategy: DuplicateStrategy::Error,
            })
            .await
            .unwrap();

        assert!(run_id > 0);
        let drawers = engine.drawer_store().list_drawers(&DrawerFilter::default()).await.unwrap();
        assert_eq!(drawers.len(), 1);
        let committed = engine.operational_store().committed_drawer_ids().unwrap();
        assert_eq!(committed.len(), 1);
    }

    #[tokio::test]
    async fn prunes_orphaned_rows_from_stale_runs() {
        let tempdir = tempdir().unwrap();
        let engine = StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();

        let stale_drawer = record("project_alpha/backend/0001", "auth.py", [1.0, 0.0, 0.0, 0.0]);
        let created_run = engine
            .operational_store()
            .create_pending_run(
                "projects",
                "project_alpha",
                &[crate::types::IngestManifestEntry {
                    run_id: 0,
                    drawer_id: stale_drawer.id.clone(),
                    source_file: "auth.py".to_owned(),
                    content_hash: stale_drawer.content_hash.clone(),
                    status: crate::types::IngestRunStatus::Pending,
                }],
                datetime!(2026-04-01 00:00:00 UTC),
            )
            .unwrap();

        engine.drawer_store().put_drawers(&[stale_drawer], DuplicateStrategy::Error).await.unwrap();

        engine.reconcile().await.unwrap();

        let drawers = engine.drawer_store().list_drawers(&DrawerFilter::default()).await.unwrap();
        assert!(drawers.is_empty());

        let stale_runs = engine
            .operational_store()
            .stale_pending_runs(datetime!(2026-04-20 00:00:00 UTC))
            .unwrap();
        assert!(stale_runs.iter().all(|run| run.run.id != created_run.id));
    }
}

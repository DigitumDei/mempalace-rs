use std::collections::{BTreeSet, HashSet};
use std::path::Path;

use tracing::warn;

use crate::error::Result;
use crate::lance::LanceDrawerStore;
use crate::sqlite::{IngestManifestStore, SqliteOperationalStore};
use crate::types::{
    DrawerFilter, DrawerStore, DuplicateStrategy, IngestCommitRequest, IngestManifestEntry,
    RetryableRun, StorageLayout,
};
use mempalace_core::{DrawerId, DrawerRecord, EmbeddingProfile};
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

    /// Replace all drawers for a mined source key.
    ///
    /// 1. Captures the currently-committed drawer ids for `source_key`.
    /// 2. Commits the new `drawers` via [`DuplicateStrategy::Overwrite`].
    /// 3. Deletes the previously-committed ids that are NOT present in the new
    ///    set (stale rows from a previous mine of the same file).
    ///
    /// Passing an empty `drawers` vec effectively wipes all drawers for the
    /// source key (used for branch-delta cleanup when a file is no longer in
    /// the delta).
    pub async fn replace_source_drawers(
        &self,
        ingest_kind: &str,
        source_key: &str,
        source_file: &str,
        content_hash: String,
        drawers: Vec<DrawerRecord>,
    ) -> Result<()> {
        let existing =
            self.operational_store.committed_drawer_ids_for_source_key(source_key)?;
        let new_ids = drawers.iter().map(|d| d.id.clone()).collect::<BTreeSet<_>>();

        self.commit_ingest(IngestCommitRequest {
            ingest_kind: ingest_kind.to_owned(),
            source_key: source_key.to_owned(),
            source_file: source_file.to_owned(),
            content_hash,
            drawers,
            duplicate_strategy: DuplicateStrategy::Overwrite,
        })
        .await?;

        let stale = existing
            .into_iter()
            .filter(|id| !new_ids.contains(id))
            .collect::<Vec<_>>();
        if !stale.is_empty() {
            self.drawer_store.delete_drawers(&stale).await?;
        }
        Ok(())
    }

    /// Remove all committed drawers and ingest metadata for a source key.
    ///
    /// This is used when migrating source-key identities so old path-based
    /// rows cannot remain alongside their stable project-id replacements.
    pub async fn remove_source_key(&self, source_key: &str) -> Result<()> {
        let drawer_ids =
            self.operational_store.committed_drawer_ids_for_source_key(source_key)?;
        if !drawer_ids.is_empty() {
            self.drawer_store.delete_drawers(&drawer_ids).await?;
        }
        self.operational_store.delete_source_key(source_key)?;
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
#[allow(clippy::unwrap_used)]
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
            locator: None,
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

    // ─── replace_source_drawers tests ─────────────────────────────────────────

    #[tokio::test]
    async fn replace_source_drawers_removes_stale_ids_and_keeps_new() {
        let tempdir = tempdir().unwrap();
        let engine = StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();
        let source_key = "projects:wing_p:abc123def456:src/auth.rs";

        // Initial commit: 2 drawers.
        let drawer_a = record("wing_p/backend/aaa-0000", "src/auth.rs", [1.0, 0.0, 0.0, 0.0]);
        let drawer_b = record("wing_p/backend/aaa-0001", "src/auth.rs", [0.0, 1.0, 0.0, 0.0]);
        engine
            .replace_source_drawers(
                "projects",
                source_key,
                "src/auth.rs",
                "hash-v1".to_owned(),
                vec![drawer_a.clone(), drawer_b.clone()],
            )
            .await
            .unwrap();

        let all = engine.drawer_store().list_drawers(&DrawerFilter::default()).await.unwrap();
        assert_eq!(all.len(), 2, "should have 2 drawers after first commit");

        // Replace with 1 new drawer; drawer_a is gone, drawer_c is new.
        let drawer_c = record("wing_p/backend/aaa-0002", "src/auth.rs", [0.0, 0.0, 1.0, 0.0]);
        engine
            .replace_source_drawers(
                "projects",
                source_key,
                "src/auth.rs",
                "hash-v2".to_owned(),
                vec![drawer_c.clone()],
            )
            .await
            .unwrap();

        let ids_after: Vec<_> =
            engine.drawer_store().list_drawers(&DrawerFilter::default()).await.unwrap()
                .into_iter()
                .map(|d| d.id)
                .collect();
        assert_eq!(ids_after.len(), 1, "stale drawers should be deleted");
        assert_eq!(ids_after[0], drawer_c.id, "the new drawer should remain");
    }

    #[tokio::test]
    async fn replace_source_drawers_with_empty_vec_wipes_all_drawers_for_key() {
        let tempdir = tempdir().unwrap();
        let engine = StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();
        let source_key = "projects:wing_p:abc123def456:src/utils.rs";

        let drawer_a = record("wing_p/utils/bbb-0000", "src/utils.rs", [1.0, 0.0, 0.0, 0.0]);
        let drawer_b = record("wing_p/utils/bbb-0001", "src/utils.rs", [0.0, 1.0, 0.0, 0.0]);
        engine
            .replace_source_drawers(
                "projects",
                source_key,
                "src/utils.rs",
                "hash-orig".to_owned(),
                vec![drawer_a, drawer_b],
            )
            .await
            .unwrap();

        // Replace with empty vec → all drawers for this source key removed.
        engine
            .replace_source_drawers(
                "projects",
                source_key,
                "src/utils.rs",
                "hash-empty".to_owned(),
                vec![],
            )
            .await
            .unwrap();

        let all = engine.drawer_store().list_drawers(&DrawerFilter::default()).await.unwrap();
        assert!(all.is_empty(), "all drawers should be deleted after empty replace");
    }
}

// ─── ingested_source_keys_with_prefix tests (on SqliteOperationalStore) ───────

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod prefix_tests {
    use crate::sqlite::{IngestManifestStore, SqliteOperationalStore};
    use crate::types::{IngestManifestEntry, IngestRunStatus};
    use mempalace_core::DrawerId;
    use tempfile::tempdir;
    use time::macros::datetime;

    fn commit_source(store: &SqliteOperationalStore, source_key: &str, drawer_id_str: &str) {
        let run = store
            .create_pending_run(
                "projects",
                source_key,
                &[IngestManifestEntry {
                    run_id: 0,
                    drawer_id: DrawerId::new(drawer_id_str).unwrap(),
                    source_file: "f.rs".to_owned(),
                    content_hash: "ch".to_owned(),
                    status: IngestRunStatus::Pending,
                }],
                datetime!(2026-01-01 00:00:00 UTC),
            )
            .unwrap();
        store
            .mark_run_committed(
                run.id,
                source_key,
                "f.rs",
                "ch",
                1,
                datetime!(2026-01-01 00:01:00 UTC),
            )
            .unwrap();
    }

    #[test]
    fn prefix_listing_returns_only_matching_keys() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        commit_source(&store, "projects:wing_a:abc:src/main.rs", "wing_a/code/abc-0000");
        commit_source(&store, "projects:wing_a:abc:src/lib.rs", "wing_a/code/abc-0001");
        commit_source(&store, "projects:wing_b:def:README.md", "wing_b/docs/def-0000");

        let wing_a_keys = store.ingested_source_keys_with_prefix("projects:wing_a:").unwrap();
        assert_eq!(wing_a_keys.len(), 2);
        assert!(wing_a_keys.iter().all(|k| k.starts_with("projects:wing_a:")));

        let wing_b_keys = store.ingested_source_keys_with_prefix("projects:wing_b:").unwrap();
        assert_eq!(wing_b_keys.len(), 1);
        assert_eq!(wing_b_keys[0], "projects:wing_b:def:README.md");

        let all_projects = store.ingested_source_keys_with_prefix("projects:").unwrap();
        assert_eq!(all_projects.len(), 3);
    }

    #[test]
    fn prefix_with_percent_does_not_wildcard_match() {
        let tempdir = tempdir().unwrap();
        let store = SqliteOperationalStore::new(tempdir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();

        // Insert a key that would be matched by '%' if not escaped.
        commit_source(
            &store,
            "projects:wing_x:abc123:src/real.rs",
            "wing_x/code/real-0000",
        );

        // A prefix containing '%' must NOT wildcard-match; expect zero results.
        let results = store.ingested_source_keys_with_prefix("projects:wing_x%").unwrap();
        assert!(
            results.is_empty(),
            "percent in prefix must not act as a wildcard; got: {results:?}"
        );
    }
}

use std::collections::{BTreeSet, HashSet};
use std::fs::File;
use std::future::Future;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use tracing::{info, warn};

use crate::error::{Result, StorageError};
use crate::lance::LanceDrawerStore;
use crate::maintenance::{
    MaintenanceAbortReason, MaintenanceOutcome, MaintenanceRunStatus, MaintenanceRunSummary,
    MaintenanceSettings, MaintenanceSkipReason, MaintenanceTier, MaintenanceTierResult,
};
use crate::sqlite::{DiaryStore, IngestManifestStore, MaintenanceLeaseStore, SqliteOperationalStore};
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
    activity_signal: Arc<AtomicBool>,
    maintenance_holder_id: String,
    last_activity_at: Arc<Mutex<Instant>>,
}

impl StorageEngine {
    pub async fn open(root: impl AsRef<Path>, profile: EmbeddingProfile) -> Result<Self> {
        let layout = StorageLayout::new(root);
        let engine = Self {
            drawer_store: LanceDrawerStore::new(&layout.lancedb_dir, profile),
            operational_store: SqliteOperationalStore::new(&layout.sqlite_path),
            layout,
            stale_after: Duration::hours(1),
            activity_signal: Arc::new(AtomicBool::new(false)),
            maintenance_holder_id: format!("maintenance-{}", std::process::id()),
            last_activity_at: Arc::new(Mutex::new(Instant::now())),
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

    /// Signal that a write operation has occurred or is about to occur.
    ///
    /// Called automatically by all write/delete paths in `StorageEngine` before
    /// the LanceDB operation starts.  The maintenance subsystem reads and clears
    /// this flag and checks the elapsed idle duration to determine whether the
    /// system has been idle long enough for a maintenance run.
    pub fn signal_activity(&self) {
        let now = Instant::now();
        self.activity_signal.store(true, Ordering::Release);
        *self.last_activity_at.lock().unwrap() = now;
    }

    /// Atomically read and clear the activity signal.
    ///
    /// Returns `true` if there was write activity since the last time this
    /// was called (or since process start).
    pub fn take_activity_signal(&self) -> bool {
        self.activity_signal.swap(false, Ordering::Acquire)
    }

    /// Elapsed wall time since the last activity signal.
    pub fn elapsed_since_last_activity(&self) -> std::time::Duration {
        Instant::now() - *self.last_activity_at.lock().unwrap()
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

        self.signal_activity();
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

    /// Coordinate a diary-specific ingest: create the pending run and store the
    /// diary summary in a single SQLite transaction before the LanceDB write,
    /// then commit on success.  On failure, remove only the summary (which was
    /// speculatively stored) and never touch the drawer — `put_drawers` either
    /// wrote nothing (on error) or the data belongs to a valid entry that must
    /// be preserved.
    ///
    /// The SQLite summary insert is also the atomic duplicate gate: a racing
    /// writer that loses it rolls back without writing to LanceDB.
    pub async fn commit_diary_ingest(
        &self,
        request: IngestCommitRequest,
        diary_summary: &str,
    ) -> Result<i64> {
        let now = OffsetDateTime::now_utc();
        if request.drawers.len() != 1 {
            return Err(StorageError::Invariant(
                "diary ingest requires exactly one drawer".to_owned(),
            ));
        }
        let drawer_id = request.drawers[0].id.clone();
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

        // Fast-path already-persisted LanceDB duplicates. The SQLite insert
        // below remains authoritative because this read cannot exclude a
        // concurrent writer.
        let duplicate_ids = self.drawer_store.existing_ids(&[drawer_id.clone()]).await?;
        if !duplicate_ids.is_empty() {
            return Err(StorageError::DuplicateDrawers(
                duplicate_ids.into_iter().collect(),
            ));
        }

        // Create pending run AND store diary summary in a single SQLite
        // transaction before the Lance drawer write, so a crash between them
        // leaves a recoverable pending run (not an orphaned summary).
        let Some(run) = self.operational_store.create_pending_diary_run(
            &request.ingest_kind,
            &request.source_key,
            &manifests,
            &drawer_id,
            diary_summary,
            now,
        )? else {
            return Err(StorageError::DuplicateDrawers(vec![drawer_id.as_str().to_owned()]));
        };

        self.signal_activity();
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
                // We inserted this summary in the same transaction as `run`,
                // and the atomic summary gate prevents a concurrent writer
                // from using it. Remove only this failed attempt's state;
                // never delete a drawer because it may belong to another
                // writer that won a LanceDB race.
                if let Err(e) = self.operational_store.delete_diary_summary(&drawer_id) {
                    warn!(error = %e, "failed to delete diary summary on commit failure");
                }
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

        // Also collect stale failed diary runs to clean up orphaned summaries
        // and drawers from previous failed writes.
        let stale_failed_diary =
            self.operational_store.stale_failed_diary_runs(stale_cutoff)?;

        // prune_orphaned_rows removes drawers whose IDs appear in stale runs
        // but are not currently committed, covering both stale pending and
        // stale failed runs.
        let all_for_prune: Vec<_> =
            stale_runs.iter().cloned().chain(stale_failed_diary.iter().cloned()).collect();
        if all_for_prune.is_empty() {
            return Ok(());
        }
        self.prune_orphaned_rows(&all_for_prune).await?;

        // Delete diary summaries for every stale chunk_id that is NOT
        // committed, regardless of whether the drawer physically exists in
        // LanceDB.  A crash before the Lance write leaves no drawer, so
        // prune_orphaned_rows cannot see it, but the summary must still be
        // removed.
        let committed_ids =
            self.operational_store.committed_drawer_ids()?.into_iter().collect::<HashSet<_>>();
        for retryable in &all_for_prune {
            for chunk_id in &retryable.chunk_ids {
                if !committed_ids.contains(chunk_id) {
                    self.operational_store.delete_diary_summary(chunk_id)?;
                }
            }
        }

        // Mark stale pending runs as failed.
        for retryable in stale_runs {
            warn!(run_id = retryable.run.id, "marking stale pending ingest run as failed");
            self.operational_store.mark_run_failed(
                retryable.run.id,
                "stale pending ingest run",
                OffsetDateTime::now_utc(),
            )?;
        }

        // Log stale failed diary runs that were cleaned up.
        for retryable in stale_failed_diary {
            warn!(run_id = retryable.run.id, "finalizing stale failed diary ingest run");
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
            self.signal_activity();
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
            self.signal_activity();
            self.drawer_store.delete_drawers(&drawer_ids).await?;
        }
        self.operational_store.delete_source_key(source_key)?;
        Ok(())
    }

    async fn prune_orphaned_rows(&self, stale_runs: &[RetryableRun]) -> Result<Vec<DrawerId>> {
        let committed_ids =
            self.operational_store.committed_drawer_ids()?.into_iter().collect::<HashSet<_>>();
        let all_drawers = self.drawer_store.list_drawers(&DrawerFilter::default()).await?;

        let stale_ids =
            stale_runs.iter().flat_map(|run| run.chunk_ids.iter().cloned()).collect::<HashSet<_>>();

        let orphaned = all_drawers
            .into_iter()
            .map(|record| record.id)
            .filter(|id| stale_ids.contains(id) && !committed_ids.contains(id))
            .collect::<Vec<DrawerId>>();

        if orphaned.is_empty() {
            return Ok(Vec::new());
        }

        self.signal_activity();
        self.drawer_store.delete_drawers(&orphaned).await?;
        Ok(orphaned)
    }

    /// Run a tier operation with concurrent lease renewal.
    ///
    /// Periodically renews the SQLite lease while the tier future is in
    /// progress.  If renewal fails, `lease_lost` is set to `true` so the
    /// caller can abort subsequent tiers.
    async fn with_lease_renewal<Fut, T>(
        &self,
        holder_id: &str,
        f: Fut,
        lease_lost: &AtomicBool,
    ) -> T
    where
        Fut: Future<Output = T>,
    {
        let mut f = Box::pin(f);
        loop {
            tokio::select! {
                biased;
                result = &mut f => return result,
                _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    match self.operational_store.renew_lease(holder_id, Duration::minutes(5)) {
                        Ok(true) => {},
                        _ => {
                            lease_lost.store(true, Ordering::Release);
                        }
                    }
                }
            }
        }
    }

    /// Execute a single maintenance run: acquire the SQLite lease, run tiers
    /// in order (vector index optimization → fragment compaction → version
    /// pruning), check the activity signal before and between tiers, and
    /// record timing/status in a [`MaintenanceRunSummary`].
    ///
    /// Each tier runs with concurrent lease renewal so that long-running
    /// operations do not lose their lease.  If the lease is lost, subsequent
    /// tiers are skipped.
    ///
    /// This is designed to be called from a background task in the HTTP hub
    /// or from a one-shot CLI command.  Regular write paths call
    /// [`signal_activity`](Self::signal_activity) instead, which this method
    /// reads to determine idleness.
    ///
    /// When the system is not idle (recent write activity detected) or
    /// maintenance is disabled, the method returns immediately with a
    /// skipped outcome — it never blocks waiting for idleness.
    pub async fn run_maintenance(&self, settings: &MaintenanceSettings) -> Result<MaintenanceRunSummary> {
        let started_at = OffsetDateTime::now_utc();
        let wall_start = Instant::now();
        let cpu_start = process_cpu_time();
        let run_id = started_at.unix_timestamp_nanos() as u64;
        let mut tier_results: Vec<MaintenanceTierResult> = Vec::new();
        let mut final_status = MaintenanceRunStatus::Success;

        // ── 1. Check enabled ─────────────────────────────────────────────────
        if !settings.enabled {
            info!("maintenance disabled by configuration");
            return Ok(MaintenanceRunSummary {
                run_id,
                started_at,
                finished_at: OffsetDateTime::now_utc(),
                duration: Duration::ZERO,
                cpu_duration: Duration::ZERO,
                status: MaintenanceRunStatus::Partial,
                tier_results: Vec::new(),
            });
        }

        // ── 2. Check idleness ────────────────────────────────────────────────
        if self.take_activity_signal() {
            info!("maintenance skipped: write activity detected");
            tier_results.push(MaintenanceTierResult {
                tier: MaintenanceTier::VectorIndexOptimization,
                started_at,
                duration: Duration::ZERO,
                cpu_duration: Duration::ZERO,
                outcome: MaintenanceOutcome::Skipped {
                    reason: MaintenanceSkipReason::NotIdle,
                },
            });
            let elapsed = wall_start.elapsed();
            let cpu_elapsed = process_cpu_time().saturating_sub(cpu_start);
            return Ok(MaintenanceRunSummary {
                run_id,
                started_at,
                finished_at: OffsetDateTime::now_utc(),
                duration: Duration::seconds_f64(elapsed.as_secs_f64()),
                cpu_duration: cpu_elapsed,
                status: MaintenanceRunStatus::Partial,
                tier_results,
            });
        }

        // Enforce idle_secs: must have been idle for the configured duration.
        let idle_duration = std::time::Duration::from_secs(settings.idle_secs);
        let elapsed_since_activity = self.elapsed_since_last_activity();
        if elapsed_since_activity < idle_duration {
            info!(
                idle_secs = settings.idle_secs,
                elapsed_since_activity_ms = elapsed_since_activity.as_millis() as u64,
                "maintenance skipped: not idle long enough",
            );
            tier_results.push(MaintenanceTierResult {
                tier: MaintenanceTier::VectorIndexOptimization,
                started_at,
                duration: Duration::ZERO,
                cpu_duration: Duration::ZERO,
                outcome: MaintenanceOutcome::Skipped {
                    reason: MaintenanceSkipReason::NotIdle,
                },
            });
            let elapsed = wall_start.elapsed();
            let cpu_elapsed = process_cpu_time().saturating_sub(cpu_start);
            return Ok(MaintenanceRunSummary {
                run_id,
                started_at,
                finished_at: OffsetDateTime::now_utc(),
                duration: Duration::seconds_f64(elapsed.as_secs_f64()),
                cpu_duration: cpu_elapsed,
                status: MaintenanceRunStatus::Partial,
                tier_results,
            });
        }

        // ── 3. Claim the maintenance lease ───────────────────────────────────
        let holder_id = &self.maintenance_holder_id;
        match self.operational_store.try_claim_lease(holder_id, Duration::minutes(5)) {
            Ok(true) => {}
            Ok(false) => {
                info!("maintenance skipped: concurrent lease held");
                tier_results.push(MaintenanceTierResult {
                    tier: MaintenanceTier::VectorIndexOptimization,
                    started_at,
                    duration: Duration::ZERO,
                    cpu_duration: Duration::ZERO,
                    outcome: MaintenanceOutcome::Aborted {
                        reason: MaintenanceAbortReason::ConcurrentRun,
                        items_affected: 0,
                    },
                });
                let elapsed = wall_start.elapsed();
                let cpu_elapsed = process_cpu_time().saturating_sub(cpu_start);
                return Ok(MaintenanceRunSummary {
                    run_id,
                    started_at,
                    finished_at: OffsetDateTime::now_utc(),
                    duration: Duration::seconds_f64(elapsed.as_secs_f64()),
                    cpu_duration: cpu_elapsed,
                    status: MaintenanceRunStatus::Failure,
                    tier_results,
                });
            }
            Err(e) => {
                warn!(error = %e, "maintenance failed to claim lease");
                tier_results.push(MaintenanceTierResult {
                    tier: MaintenanceTier::VectorIndexOptimization,
                    started_at,
                    duration: Duration::ZERO,
                    cpu_duration: Duration::ZERO,
                    outcome: MaintenanceOutcome::Failed {
                        message: e.to_string(),
                    },
                });
                let elapsed = wall_start.elapsed();
                let cpu_elapsed = process_cpu_time().saturating_sub(cpu_start);
                return Ok(MaintenanceRunSummary {
                    run_id,
                    started_at,
                    finished_at: OffsetDateTime::now_utc(),
                    duration: Duration::seconds_f64(elapsed.as_secs_f64()),
                    cpu_duration: cpu_elapsed,
                    status: MaintenanceRunStatus::Failure,
                    tier_results,
                });
            }
        }

        // ── 4. Check activity again after lease claim, before first tier ─────
        if self.take_activity_signal() {
            info!("maintenance aborted: write activity detected after lease claim");
            tier_results.push(MaintenanceTierResult {
                tier: MaintenanceTier::VectorIndexOptimization,
                started_at,
                duration: Duration::ZERO,
                cpu_duration: Duration::ZERO,
                outcome: MaintenanceOutcome::Aborted {
                    reason: MaintenanceAbortReason::ConcurrentRun,
                    items_affected: 0,
                },
            });
            final_status = MaintenanceRunStatus::Failure;
            let _ = self.operational_store.release_lease(holder_id);
            let elapsed = wall_start.elapsed();
            let cpu_elapsed = process_cpu_time().saturating_sub(cpu_start);
            return Ok(MaintenanceRunSummary {
                run_id,
                started_at,
                finished_at: OffsetDateTime::now_utc(),
                duration: Duration::seconds_f64(elapsed.as_secs_f64()),
                cpu_duration: cpu_elapsed,
                status: final_status,
                tier_results,
            });
        }

        // Shared lease-lost flag checked after each tier.
        let lease_lost = Arc::new(AtomicBool::new(false));
        // All tiers in execution order, used by record_remaining.
        let all_tiers = [
            MaintenanceTier::VectorIndexOptimization,
            MaintenanceTier::FragmentCompaction,
            MaintenanceTier::VersionRetention,
        ];

        // Helper: check activity between tiers.  Returns true when remaining
        // tiers should be skipped (caller must also record remaining results).
        let has_activity = || -> bool {
            if self.take_activity_signal() {
                info!("maintenance inter-tier abort: write activity detected");
                true
            } else {
                false
            }
        };

        // Helper: record skipped results for tiers from start_idx onward.
        let record_remaining = |start_idx: usize, results: &mut Vec<MaintenanceTierResult>| {
            for &tier in &all_tiers[start_idx..] {
                results.push(MaintenanceTierResult {
                    tier,
                    started_at: OffsetDateTime::now_utc(),
                    duration: Duration::ZERO,
                    cpu_duration: Duration::ZERO,
                    outcome: MaintenanceOutcome::Skipped {
                        reason: MaintenanceSkipReason::NotIdle,
                    },
                });
            }
        };

        // ── 5. Tier: VectorIndexOptimization (most expensive, run first) ────
        {
            let tier_start = Instant::now();
            let tier_cpu_start = process_cpu_time();
            let tier_started_at = OffsetDateTime::now_utc();

            info!(
                tier = "vector_index_optimization",
                tail_threshold = settings.tail_threshold_rows,
                "maintenance tier starting",
            );

            let tier_future = async {
                match self.drawer_store.optimize_vector_index(settings.tail_threshold_rows).await {
                    Ok(metrics) => {
                        let tier_wall = tier_start.elapsed();
                        let tier_cpu = process_cpu_time().saturating_sub(tier_cpu_start);
                        info!(
                            tier = "vector_index_optimization",
                            tail_before = metrics.tail_rows_before,
                            tail_after = metrics.tail_rows_after,
                            indexed_before = metrics.indexed_rows_before,
                            indexed_after = metrics.indexed_rows_after,
                            bytes_before = metrics.bytes_before,
                            bytes_after = metrics.bytes_after,
                            ran = metrics.ran,
                            wall_ms = tier_wall.as_millis() as u64,
                            cpu_ms = tier_cpu.whole_milliseconds() as u64,
                            "maintenance tier completed",
                        );
                        let outcome = if metrics.ran {
                            let items = metrics.indexed_rows_after.saturating_sub(metrics.indexed_rows_before) as u64;
                            MaintenanceOutcome::Completed { items_affected: items }
                        } else {
                            MaintenanceOutcome::Skipped { reason: MaintenanceSkipReason::NothingToDo }
                        };
                        MaintenanceTierResult {
                            tier: MaintenanceTier::VectorIndexOptimization,
                            started_at: tier_started_at,
                            duration: Duration::seconds_f64(tier_wall.as_secs_f64()),
                            cpu_duration: tier_cpu,
                            outcome,
                        }
                    }
                    Err(e) => {
                        let tier_wall = tier_start.elapsed();
                        let tier_cpu = process_cpu_time().saturating_sub(tier_cpu_start);
                        info!(
                            tier = "vector_index_optimization",
                            error = %e,
                            wall_ms = tier_wall.as_millis() as u64,
                            cpu_ms = tier_cpu.whole_milliseconds() as u64,
                            "maintenance tier failed",
                        );
                        MaintenanceTierResult {
                            tier: MaintenanceTier::VectorIndexOptimization,
                            started_at: tier_started_at,
                            duration: Duration::seconds_f64(tier_wall.as_secs_f64()),
                            cpu_duration: tier_cpu,
                            outcome: MaintenanceOutcome::Failed { message: e.to_string() },
                        }
                    }
                }
            };

            let result = self.with_lease_renewal(holder_id, tier_future, &lease_lost).await;
            let is_failure = matches!(result.outcome, MaintenanceOutcome::Failed { .. });
            let is_skipped = matches!(result.outcome, MaintenanceOutcome::Skipped { .. });
            tier_results.push(result);
            if is_failure {
                final_status = MaintenanceRunStatus::Failure;
            } else if is_skipped && final_status == MaintenanceRunStatus::Success {
                final_status = MaintenanceRunStatus::Partial;
            }
        }

        // ── 6. Tier: FragmentCompaction ─────────────────────────────────────
        if !has_activity() && !lease_lost.load(Ordering::Acquire) {
            let tier_start = Instant::now();
            let tier_cpu_start = process_cpu_time();
            let tier_started_at = OffsetDateTime::now_utc();

            info!(
                tier = "fragment_compaction",
                small_fragment_threshold = settings.small_fragment_threshold,
                "maintenance tier starting",
            );

            let tier_future = async {
                match self.drawer_store.compact_fragments(settings.small_fragment_threshold as usize).await {
                    Ok(metrics) => {
                        let tier_wall = tier_start.elapsed();
                        let tier_cpu = process_cpu_time().saturating_sub(tier_cpu_start);
                        info!(
                            tier = "fragment_compaction",
                            fragments_before = metrics.tail_rows_before,
                            fragments_after = metrics.tail_rows_after,
                            bytes_before = metrics.bytes_before,
                            bytes_after = metrics.bytes_after,
                            ran = metrics.ran,
                            wall_ms = tier_wall.as_millis() as u64,
                            cpu_ms = tier_cpu.whole_milliseconds() as u64,
                            "maintenance tier completed",
                        );
                        let outcome = if metrics.ran {
                            let items = metrics.indexed_rows_before.saturating_sub(metrics.indexed_rows_after) as u64;
                            MaintenanceOutcome::Completed { items_affected: items }
                        } else {
                            MaintenanceOutcome::Skipped { reason: MaintenanceSkipReason::NothingToDo }
                        };
                        MaintenanceTierResult {
                            tier: MaintenanceTier::FragmentCompaction,
                            started_at: tier_started_at,
                            duration: Duration::seconds_f64(tier_wall.as_secs_f64()),
                            cpu_duration: tier_cpu,
                            outcome,
                        }
                    }
                    Err(e) => {
                        let tier_wall = tier_start.elapsed();
                        let tier_cpu = process_cpu_time().saturating_sub(tier_cpu_start);
                        info!(
                            tier = "fragment_compaction",
                            error = %e,
                            wall_ms = tier_wall.as_millis() as u64,
                            cpu_ms = tier_cpu.whole_milliseconds() as u64,
                            "maintenance tier failed",
                        );
                        MaintenanceTierResult {
                            tier: MaintenanceTier::FragmentCompaction,
                            started_at: tier_started_at,
                            duration: Duration::seconds_f64(tier_wall.as_secs_f64()),
                            cpu_duration: tier_cpu,
                            outcome: MaintenanceOutcome::Failed { message: e.to_string() },
                        }
                    }
                }
            };

            let result = self.with_lease_renewal(holder_id, tier_future, &lease_lost).await;
            let is_failure = matches!(result.outcome, MaintenanceOutcome::Failed { .. });
            let is_skipped = matches!(result.outcome, MaintenanceOutcome::Skipped { .. });
            tier_results.push(result);
            if is_failure {
                final_status = MaintenanceRunStatus::Failure;
            } else if is_skipped && final_status == MaintenanceRunStatus::Success {
                final_status = MaintenanceRunStatus::Partial;
            }
        } else {
            final_status = MaintenanceRunStatus::Failure;
            record_remaining(1, &mut tier_results);
        }

        // ── 7. Tier: VersionRetention (cheapest, run last) ──────────────────
        if !has_activity() && !lease_lost.load(Ordering::Acquire) {
            let tier_start = Instant::now();
            let tier_cpu_start = process_cpu_time();
            let tier_started_at = OffsetDateTime::now_utc();

            info!(
                tier = "version_retention",
                retention_hours = settings.version_retention_hours,
                "maintenance tier starting",
            );

            let tier_future = async {
                match self.drawer_store.prune_versions(settings.version_retention_hours).await {
                    Ok(metrics) => {
                        let tier_wall = tier_start.elapsed();
                        let tier_cpu = process_cpu_time().saturating_sub(tier_cpu_start);
                        info!(
                            tier = "version_retention",
                            versions_removed = metrics.versions_removed,
                            bytes_freed = metrics.bytes_freed,
                            versions_before = metrics.versions_before,
                            versions_after = metrics.versions_after,
                            total_bytes_before = metrics.total_bytes_before,
                            total_bytes_after = metrics.total_bytes_after,
                            wall_ms = tier_wall.as_millis() as u64,
                            cpu_ms = tier_cpu.whole_milliseconds() as u64,
                            "maintenance tier completed",
                        );
                        let outcome = if metrics.versions_removed > 0 {
                            MaintenanceOutcome::Completed { items_affected: metrics.versions_removed as u64 }
                        } else {
                            MaintenanceOutcome::Skipped { reason: MaintenanceSkipReason::NothingToDo }
                        };
                        MaintenanceTierResult {
                            tier: MaintenanceTier::VersionRetention,
                            started_at: tier_started_at,
                            duration: Duration::seconds_f64(tier_wall.as_secs_f64()),
                            cpu_duration: tier_cpu,
                            outcome,
                        }
                    }
                    Err(e) => {
                        let tier_wall = tier_start.elapsed();
                        let tier_cpu = process_cpu_time().saturating_sub(tier_cpu_start);
                        info!(
                            tier = "version_retention",
                            error = %e,
                            wall_ms = tier_wall.as_millis() as u64,
                            cpu_ms = tier_cpu.whole_milliseconds() as u64,
                            "maintenance tier failed",
                        );
                        MaintenanceTierResult {
                            tier: MaintenanceTier::VersionRetention,
                            started_at: tier_started_at,
                            duration: Duration::seconds_f64(tier_wall.as_secs_f64()),
                            cpu_duration: tier_cpu,
                            outcome: MaintenanceOutcome::Failed { message: e.to_string() },
                        }
                    }
                }
            };

            let result = self.with_lease_renewal(holder_id, tier_future, &lease_lost).await;
            let is_failure = matches!(result.outcome, MaintenanceOutcome::Failed { .. });
            let is_skipped = matches!(result.outcome, MaintenanceOutcome::Skipped { .. });
            tier_results.push(result);
            if is_failure {
                final_status = MaintenanceRunStatus::Failure;
            } else if is_skipped && final_status == MaintenanceRunStatus::Success {
                final_status = MaintenanceRunStatus::Partial;
            }
        } else {
            record_remaining(2, &mut tier_results);
        }

        // ── 8. Release lease ─────────────────────────────────────────────────
        let _ = self.operational_store.release_lease(holder_id);

        let elapsed = wall_start.elapsed();
        let cpu_elapsed = process_cpu_time().saturating_sub(cpu_start);
        info!(
            status = ?final_status,
            tiers = tier_results.len(),
            wall_ms = elapsed.as_millis() as u64,
            cpu_ms = cpu_elapsed.whole_milliseconds() as u64,
            "maintenance run completed",
        );
        Ok(MaintenanceRunSummary {
            run_id,
            started_at,
            finished_at: OffsetDateTime::now_utc(),
            duration: Duration::seconds_f64(elapsed.as_secs_f64()),
            cpu_duration: cpu_elapsed,
            status: final_status,
            tier_results,
        })
    }
}

/// Returns the current process CPU time (user + system) as a
/// `time::Duration`.
///
/// On Linux this reads `/proc/self/stat`; on other platforms it returns
/// [`Duration::ZERO`].
fn process_cpu_time() -> time::Duration {
    #[cfg(target_os = "linux")]
    {
        let mut buf = String::new();
        if File::open("/proc/self/stat")
            .and_then(|mut f| f.read_to_string(&mut buf))
            .is_ok()
        {
            let parts: Vec<&str> = buf.split_whitespace().collect();
            if parts.len() > 14 {
                if let (Ok(utime), Ok(stime)) =
                    (parts[13].parse::<u64>(), parts[14].parse::<u64>())
                {
                    // sysconf(_SC_CLK_TCK) is typically 100 on Linux.
                    const CLK_TCK: u64 = 100;
                    let total_ticks = utime.saturating_add(stime);
                    let total_ns = total_ticks.saturating_mul(1_000_000_000) / CLK_TCK;
                    let secs = total_ns / 1_000_000_000;
                    let nanos = (total_ns % 1_000_000_000) as i32;
                    return time::Duration::new(secs as i64, nanos);
                }
            }
        }
        time::Duration::ZERO
    }
    #[cfg(not(target_os = "linux"))]
    {
        time::Duration::ZERO
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;
    use time::macros::{date, datetime};

    use super::StorageEngine;
    use crate::error::StorageError;
    use crate::lance::LanceDrawerStore;
    use crate::maintenance::{
        MaintenanceAbortReason, MaintenanceOutcome, MaintenanceRunStatus, MaintenanceSettings,
        MaintenanceSkipReason, MaintenanceTier,
    };
    use crate::sqlite::{DiaryStore, IngestManifestStore};
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

    #[tokio::test]
    async fn reconcile_skips_lance_without_stale_runs() {
        let tempdir = tempdir().unwrap();
        let mut engine =
            StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();

        // A missing table makes any accidental Lance access fail.
        engine.drawer_store = LanceDrawerStore::new(
            tempdir.path().join("unused_lance"),
            EmbeddingProfile::Balanced,
        );

        engine.reconcile().await.unwrap();
    }

    #[tokio::test]
    async fn reconcile_cleans_up_diary_summary_for_stale_diary_run() {
        let tempdir = tempdir().unwrap();
        let engine = StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();
        let drawer_id = mempalace_core::DrawerId::new("diary_wing_test_0001").unwrap();

        // Store a diary summary and a pending diary ingest run.
        engine.operational_store().store_diary_summary(&drawer_id, "test summary").unwrap();
        let created_run = engine
            .operational_store()
            .create_pending_run(
                "diary",
                "diary:diary_wing_test_0001",
                &[crate::types::IngestManifestEntry {
                    run_id: 0,
                    drawer_id: drawer_id.clone(),
                    source_file: "diary:test".to_owned(),
                    content_hash: "hash".to_owned(),
                    status: crate::types::IngestRunStatus::Pending,
                }],
                datetime!(2026-04-01 00:00:00 UTC),
            )
            .unwrap();

        // Verify summary exists before reconcile.
        assert_eq!(
            engine.operational_store().get_diary_summary(&drawer_id).unwrap(),
            Some("test summary".to_owned())
        );

        engine.reconcile().await.unwrap();

        // Summary should be deleted after reconcile.
        assert_eq!(engine.operational_store().get_diary_summary(&drawer_id).unwrap(), None);

        let stale_runs = engine
            .operational_store()
            .stale_pending_runs(datetime!(2026-04-20 00:00:00 UTC))
            .unwrap();
        assert!(stale_runs.iter().all(|run| run.run.id != created_run.id));
    }

    #[tokio::test]
    async fn reconcile_preserves_summary_for_committed_drawer() {
        let tempdir = tempdir().unwrap();
        let engine = StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();
        let drawer_id = mempalace_core::DrawerId::new("diary_wing_test_0010").unwrap();

        // Store a diary summary.
        engine.operational_store().store_diary_summary(&drawer_id, "preserved summary").unwrap();

        // Create a stale pending diary run — but the drawer is committed
        // (indicating a later successful run).
        let run = engine
            .operational_store()
            .create_pending_run(
                "diary",
                "diary:diary_wing_test_0010",
                &[crate::types::IngestManifestEntry {
                    run_id: 0,
                    drawer_id: drawer_id.clone(),
                    source_file: "diary:test".to_owned(),
                    content_hash: "hash".to_owned(),
                    status: crate::types::IngestRunStatus::Pending,
                }],
                datetime!(2026-04-01 00:00:00 UTC),
            )
            .unwrap();
        // Mark the run committed so the drawer is tracked.
        engine.operational_store().mark_run_committed(
            run.id,
            "diary:diary_wing_test_0010",
            "diary:test",
            "hash",
            1,
            datetime!(2026-04-01 01:00:00 UTC),
        )
        .unwrap();

        // Actually put the drawer in LanceDB so it's "present" (committed).
        let mut diary_record = record("diary_wing_test_0010", "diary:test", [1.0, 0.0, 0.0, 0.0]);
        diary_record.wing = mempalace_core::WingId::new("wing_agents").unwrap();
        diary_record.room = mempalace_core::RoomId::new("diary").unwrap();
        diary_record.ingest_mode = "diary".to_owned();
        engine.drawer_store().put_drawers(&[diary_record], DuplicateStrategy::Error).await.unwrap();

        // Verify summary exists before reconcile.
        assert_eq!(
            engine.operational_store().get_diary_summary(&drawer_id).unwrap(),
            Some("preserved summary".to_owned())
        );

        engine.reconcile().await.unwrap();

        // Summary must NOT be deleted — the drawer is committed.
        assert_eq!(
            engine.operational_store().get_diary_summary(&drawer_id).unwrap(),
            Some("preserved summary".to_owned())
        );
    }

    #[tokio::test]
    async fn commit_diary_ingest_success() {
        let tempdir = tempdir().unwrap();
        let engine = StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();
        let drawer_id = mempalace_core::DrawerId::new("diary_wing_test_0002").unwrap();
        let wing = mempalace_core::WingId::new("wing_agents").unwrap();
        let room = mempalace_core::RoomId::new("diary").unwrap();

        let mut diary_record = record("diary_wing_test_0002", "diary:general", [1.0, 0.0, 0.0, 0.0]);
        diary_record.wing = wing;
        diary_record.room = room;
        diary_record.ingest_mode = "diary".to_owned();

        let run_id = engine
            .commit_diary_ingest(
                IngestCommitRequest {
                    ingest_kind: "diary".to_owned(),
                    source_key: format!("diary:{}", drawer_id.as_str()),
                    source_file: "diary:general".to_owned(),
                    content_hash: diary_record.content_hash.clone(),
                    drawers: vec![diary_record],
                    duplicate_strategy: DuplicateStrategy::Error,
                },
                "test diary summary",
            )
            .await
            .unwrap();

        assert!(run_id > 0);

        // Summary should be stored.
        assert_eq!(
            engine.operational_store().get_diary_summary(&drawer_id).unwrap(),
            Some("test diary summary".to_owned())
        );

        // Drawer should be in LanceDB.
        let drawers = engine.drawer_store().list_drawers(&DrawerFilter::default()).await.unwrap();
        assert_eq!(drawers.len(), 1);
        assert_eq!(drawers[0].id, drawer_id);

        // Run should be committed.
        let committed = engine.operational_store().committed_drawer_ids().unwrap();
        assert_eq!(committed.len(), 1);
        assert!(committed.contains(&drawer_id));
    }

    #[tokio::test]
    async fn commit_diary_ingest_cleans_up_on_failure() {
        let tempdir = tempdir().unwrap();
        let engine = StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();
        let drawer_id = mempalace_core::DrawerId::new("diary_wing_test_0003").unwrap();

        // First, write a drawer directly to LanceDB to set up a duplicate.
        let mut existing = record("diary_wing_test_0003", "diary:existing", [1.0, 0.0, 0.0, 0.0]);
        existing.wing = mempalace_core::WingId::new("wing_agents").unwrap();
        existing.room = mempalace_core::RoomId::new("diary").unwrap();
        existing.ingest_mode = "diary".to_owned();
        engine.drawer_store().put_drawers(&[existing], DuplicateStrategy::Error).await.unwrap();

        // Now try to commit_diary_ingest with the same drawer ID — should fail
        // on the pre-existing duplicate check before any SQLite state is created.
        let mut diary_record = record("diary_wing_test_0003", "diary:retry", [1.0, 0.0, 0.0, 0.0]);
        diary_record.wing = mempalace_core::WingId::new("wing_agents").unwrap();
        diary_record.room = mempalace_core::RoomId::new("diary").unwrap();
        diary_record.ingest_mode = "diary".to_owned();

        let result = engine
            .commit_diary_ingest(
                IngestCommitRequest {
                    ingest_kind: "diary".to_owned(),
                    source_key: format!("diary:{}", drawer_id.as_str()),
                    source_file: "diary:retry".to_owned(),
                    content_hash: diary_record.content_hash.clone(),
                    drawers: vec![diary_record],
                    duplicate_strategy: DuplicateStrategy::Error,
                },
                "should be cleaned up",
            )
            .await;

        assert!(
            matches!(result, Err(StorageError::DuplicateDrawers(_))),
            "commit_diary_ingest should fail with DuplicateDrawers error on duplicate"
        );

        // No summary was stored (pre-check failed before SQLite state was created).
        assert_eq!(engine.operational_store().get_diary_summary(&drawer_id).unwrap(), None);

        // The original drawer from the direct write should still be there.
        let drawers = engine.drawer_store().list_drawers(&DrawerFilter::default()).await.unwrap();
        assert_eq!(drawers.len(), 1, "the original drawer should remain");
        assert_eq!(drawers[0].id, drawer_id);

        // No failed run exists because the pre-check returned before creating any.
        let stale_failed = engine
            .operational_store()
            .stale_failed_diary_runs(datetime!(2026-04-20 00:00:00 UTC))
            .unwrap();
        assert!(stale_failed.is_empty(), "no stale failed run should exist");
    }

    #[tokio::test]
    async fn reconcile_cleans_up_stale_failed_diary_run() {
        let tempdir = tempdir().unwrap();
        let engine = StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();
        let drawer_id = mempalace_core::DrawerId::new("diary_wing_failed_0001").unwrap();

        // Manually stage a failed diary run with a summary and a drawer in
        // LanceDB, simulating a crash that left the old code path incomplete.
        let mut staged = record("diary_wing_failed_0001", "diary:stale", [1.0, 0.0, 0.0, 0.0]);
        staged.wing = mempalace_core::WingId::new("wing_agents").unwrap();
        staged.room = mempalace_core::RoomId::new("diary").unwrap();
        staged.ingest_mode = "diary".to_owned();

        engine.operational_store().store_diary_summary(&drawer_id, "stale summary").unwrap();
        let created_run = engine
            .operational_store()
            .create_pending_run(
                "diary",
                "diary:diary_wing_failed_0001",
                &[crate::types::IngestManifestEntry {
                    run_id: 0,
                    drawer_id: drawer_id.clone(),
                    source_file: "diary:stale".to_owned(),
                    content_hash: staged.content_hash.clone(),
                    status: crate::types::IngestRunStatus::Pending,
                }],
                datetime!(2026-03-01 00:00:00 UTC),
            )
            .unwrap();

        // Mark the run as failed to simulate the old code path.
        engine
            .operational_store()
            .mark_run_failed(created_run.id, "simulated failure", datetime!(2026-03-01 01:00:00 UTC))
            .unwrap();

        // Write the drawer to LanceDB (simulating incomplete old cleanup).
        engine.drawer_store().put_drawers(&[staged], DuplicateStrategy::Error).await.unwrap();

        // Verify pre-reconcile state.
        assert_eq!(
            engine.operational_store().get_diary_summary(&drawer_id).unwrap(),
            Some("stale summary".to_owned())
        );
        let drawers = engine.drawer_store().list_drawers(&DrawerFilter::default()).await.unwrap();
        assert_eq!(drawers.len(), 1);

        // Run reconciliation — it should clean up the stale failed diary run.
        engine.reconcile().await.unwrap();

        // Summary should be deleted.
        assert_eq!(engine.operational_store().get_diary_summary(&drawer_id).unwrap(), None);

        // Drawer should be pruned.
        let drawers = engine.drawer_store().list_drawers(&DrawerFilter::default()).await.unwrap();
        assert!(drawers.is_empty(), "drawer should be pruned by reconcile");
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

    // ─── run_maintenance tests ─────────────────────────────────────────────────

    #[tokio::test]
    async fn run_maintenance_disabled_returns_partial() {
        let tempdir = tempdir().unwrap();
        let engine = StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();

        let settings = MaintenanceSettings { enabled: false, ..MaintenanceSettings::default() };
        let summary = engine.run_maintenance(&settings).await.unwrap();

        assert_eq!(summary.status, MaintenanceRunStatus::Partial);
        assert!(summary.tier_results.is_empty());
    }

    #[tokio::test]
    async fn run_maintenance_skipped_when_activity_detected() {
        let tempdir = tempdir().unwrap();
        let engine = StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();

        engine.signal_activity();

        let settings = MaintenanceSettings { idle_secs: 0, ..MaintenanceSettings::default() };
        let summary = engine.run_maintenance(&settings).await.unwrap();

        assert_eq!(summary.status, MaintenanceRunStatus::Partial);
        assert_eq!(summary.tier_results.len(), 1);
        assert_eq!(summary.tier_results[0].tier, MaintenanceTier::VectorIndexOptimization);
        assert_eq!(
            summary.tier_results[0].outcome,
            MaintenanceOutcome::Skipped { reason: MaintenanceSkipReason::NotIdle },
        );
    }

    #[tokio::test]
    async fn run_maintenance_skipped_when_not_idle_long_enough() {
        let tempdir = tempdir().unwrap();
        let engine = StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();

        // A large idle_secs means the system has not been idle long enough.
        let settings = MaintenanceSettings { idle_secs: 999_999, ..MaintenanceSettings::default() };
        let summary = engine.run_maintenance(&settings).await.unwrap();

        assert_eq!(summary.status, MaintenanceRunStatus::Partial);
        assert_eq!(summary.tier_results.len(), 1);
        assert_eq!(
            summary.tier_results[0].outcome,
            MaintenanceOutcome::Skipped { reason: MaintenanceSkipReason::NotIdle },
        );
    }

    #[tokio::test]
    async fn run_maintenance_all_tiers_skipped_on_empty_palace() {
        let tempdir = tempdir().unwrap();
        let engine = StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();

        let settings = MaintenanceSettings { idle_secs: 0, ..MaintenanceSettings::default() };
        let summary = engine.run_maintenance(&settings).await.unwrap();

        assert_eq!(summary.status, MaintenanceRunStatus::Partial);
        assert_eq!(summary.tier_results.len(), 3);
        // All tiers should be skipped with NothingToDo (empty palace).
        for result in &summary.tier_results {
            assert_eq!(
                result.outcome,
                MaintenanceOutcome::Skipped { reason: MaintenanceSkipReason::NothingToDo },
                "tier {:?} should be NothingToDo",
                result.tier,
            );
        }
    }

    #[tokio::test]
    async fn run_maintenance_lease_contention_returns_concurrent_run() {
        let tempdir = tempdir().unwrap();
        let engine = StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();

        // Claim the lease from a different holder so the engine's
        // run_maintenance cannot acquire it.
        let other_holder = "other-process-42";
        let claimed =
            engine.operational_store().try_claim_lease(other_holder, time::Duration::minutes(5)).unwrap();
        assert!(claimed, "should claim lease from other holder");

        let settings = MaintenanceSettings { idle_secs: 0, ..MaintenanceSettings::default() };
        let summary = engine.run_maintenance(&settings).await.unwrap();

        assert_eq!(summary.status, MaintenanceRunStatus::Failure);
        assert_eq!(summary.tier_results.len(), 1);
        assert_eq!(
            summary.tier_results[0].outcome,
            MaintenanceOutcome::Aborted {
                reason: MaintenanceAbortReason::ConcurrentRun,
                items_affected: 0,
            },
        );

        // Clean up the lease.
        engine.operational_store().release_lease(other_holder).unwrap();
    }

    #[tokio::test]
    async fn run_maintenance_aborts_when_activity_arrives_mid_tier() {
        let tempdir = tempdir().unwrap();
        let engine = Arc::new(
            StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap(),
        );

        let settings = MaintenanceSettings { idle_secs: 0, ..MaintenanceSettings::default() };

        let engine_clone = Arc::clone(&engine);
        let handle = tokio::spawn(async move {
            engine_clone.run_maintenance(&settings).await
        });

        // Wait just enough for the run to clear the initial activity check and
        // begin the first tier (or lease claim).  Then signal activity so the
        // inter-tier check aborts subsequent tiers.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        engine.signal_activity();

        let summary = match handle.await.unwrap() {
            Ok(s) => s,
            Err(e) => panic!("run_maintenance returned error: {e}"),
        };

        // The run either completed (all three tiers) or was aborted between tiers.
        // With activity signalled mid-run, at least one tier should be a skip or
        // we should see the aborted-between-tiers pattern.
        assert!(!summary.tier_results.is_empty(), "must have at least one tier result");
        if summary.tier_results.len() < 3 {
            // Activity was detected between tiers — some tiers were skipped.
            assert_eq!(summary.status, MaintenanceRunStatus::Failure);
        }
    }

    #[tokio::test]
    async fn run_maintenance_with_data_runs_tiers() {
        let tempdir = tempdir().unwrap();
        let engine = StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();

        // Insert enough drawers to create data for maintenance to work on.
        let drawers: Vec<_> = (0..20)
            .map(|i| {
                let mut r = record(
                    &format!("wing/room/{i:04}"),
                    "test.txt",
                    [0.1, 0.2, 0.3, 0.4],
                );
                r.wing = mempalace_core::WingId::new("wing").unwrap();
                r.room = mempalace_core::RoomId::new("room").unwrap();
                r
            })
            .collect();
        engine.drawer_store().put_drawers(&drawers, DuplicateStrategy::Error).await.unwrap();

        let settings = MaintenanceSettings { idle_secs: 0, ..MaintenanceSettings::default() };
        let summary = engine.run_maintenance(&settings).await.unwrap();

        // With data present, tiers should run (some may skip due to thresholds).
        assert_eq!(summary.tier_results.len(), 3);
        assert!(
            matches!(summary.status, MaintenanceRunStatus::Success | MaintenanceRunStatus::Partial),
            "expected Success or Partial, got {:?}",
            summary.status,
        );
    }
}

// ─── ingested_source_keys_with_prefix tests (on SqliteOperationalStore) ───────

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod diary_summary_tests {
    use super::StorageEngine;
    use crate::error::StorageError;
    use crate::sqlite::{DiaryStore, IngestManifestStore};
    use crate::types::{DrawerStore, DuplicateStrategy, IngestCommitRequest};
    use mempalace_core::{
        DIARY_SUMMARY_MAX_CHARS, DrawerId, DrawerRecord, EmbeddingProfile, RoomId, WingId,
    };
    use tempfile::tempdir;
    use time::macros::{date, datetime};

    fn embedding(seed: [f32; 4]) -> Vec<f32> {
        let mut values = Vec::with_capacity(EmbeddingProfile::Balanced.metadata().dimensions);
        while values.len() < EmbeddingProfile::Balanced.metadata().dimensions {
            values.extend(seed);
        }
        values.truncate(EmbeddingProfile::Balanced.metadata().dimensions);
        values
    }

    fn diary_record(id: &str, content: &str) -> DrawerRecord {
        DrawerRecord {
            id: DrawerId::new(id).unwrap(),
            wing: WingId::new("wing_agents").unwrap(),
            room: RoomId::new("diary").unwrap(),
            hall: Some("hall_diary".to_owned()),
            date: Some(date!(2026 - 06 - 01)),
            source_file: "diary:test".to_owned(),
            chunk_index: 0,
            ingest_mode: "diary".to_owned(),
            extract_mode: None,
            added_by: "tester".to_owned(),
            filed_at: datetime!(2026-06-01 10:00:00 UTC),
            importance: None,
            emotional_weight: None,
            weight: None,
            content: content.to_owned(),
            content_hash: format!("hash-{id}"),
            embedding: embedding([0.5, 0.5, 0.5, 0.5]),
            locator: None,
        }
    }

    #[tokio::test]
    async fn store_and_retrieve_exact_400_char_summary_with_multibyte_unicode() {
        let tempdir = tempdir().unwrap();
        let engine = StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();
        let drawer_id = DrawerId::new("unicode_400_summary").unwrap();

        // Build a 400-char summary that includes multibyte Unicode characters.
        // "🔥" is 1 char (in chars().count()) but 4 bytes in UTF-8.
        let emoji_part = "🔥💡✅❌";
        assert_eq!(emoji_part.chars().count(), 4);
        let padding_len = DIARY_SUMMARY_MAX_CHARS - emoji_part.chars().count();
        let padding = "x".repeat(padding_len);
        let summary: String = format!("{emoji_part}{padding}");
        assert_eq!(summary.chars().count(), DIARY_SUMMARY_MAX_CHARS);

        let record = diary_record("unicode_400_summary", "diary entry with unicode summary");
        engine
            .commit_diary_ingest(
                IngestCommitRequest {
                    ingest_kind: "diary".to_owned(),
                    source_key: "diary:unicode_400_summary".to_owned(),
                    source_file: "diary:test".to_owned(),
                    content_hash: record.content_hash.clone(),
                    drawers: vec![record],
                    duplicate_strategy: DuplicateStrategy::Error,
                },
                &summary,
            )
            .await
            .unwrap();

        let stored = engine.operational_store().get_diary_summary(&drawer_id).unwrap();
        assert_eq!(stored, Some(summary.clone()));
        assert_eq!(stored.unwrap().chars().count(), DIARY_SUMMARY_MAX_CHARS);
    }

    #[tokio::test]
    async fn concurrent_diary_ingests_preserve_the_winning_summary() {
        let tempdir = tempdir().unwrap();
        let engine = StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();
        let drawer_id = DrawerId::new("concurrent_diary_summary").unwrap();
        let first_record = diary_record("concurrent_diary_summary", "same diary content");
        let second_record = first_record.clone();

        let first = engine.commit_diary_ingest(
            IngestCommitRequest {
                ingest_kind: "diary".to_owned(),
                source_key: "diary:concurrent_diary_summary:first".to_owned(),
                source_file: "diary:test".to_owned(),
                content_hash: first_record.content_hash.clone(),
                drawers: vec![first_record],
                duplicate_strategy: DuplicateStrategy::Error,
            },
            "first writer summary",
        );
        let second = engine.commit_diary_ingest(
            IngestCommitRequest {
                ingest_kind: "diary".to_owned(),
                source_key: "diary:concurrent_diary_summary:second".to_owned(),
                source_file: "diary:test".to_owned(),
                content_hash: second_record.content_hash.clone(),
                drawers: vec![second_record],
                duplicate_strategy: DuplicateStrategy::Error,
            },
            "second writer summary",
        );

        let (first_result, second_result) = tokio::join!(first, second);
        let successes = [&first_result, &second_result]
            .into_iter()
            .filter(|result| result.is_ok())
            .count();
        assert_eq!(successes, 1, "exactly one writer must claim the drawer ID");

        let expected_summary = if first_result.is_ok() {
            "first writer summary"
        } else {
            "second writer summary"
        };
        assert_eq!(
            engine.operational_store().get_diary_summary(&drawer_id).unwrap(),
            Some(expected_summary.to_owned()),
            "the loser must neither overwrite nor delete the winner's summary"
        );
    }

    #[tokio::test]
    async fn commit_diary_ingest_rejects_empty_drawers() {
        let tempdir = tempdir().unwrap();
        let engine = StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();

        let result = engine
            .commit_diary_ingest(
                IngestCommitRequest {
                    ingest_kind: "diary".to_owned(),
                    source_key: "diary:empty".to_owned(),
                    source_file: "diary:test".to_owned(),
                    content_hash: "empty".to_owned(),
                    drawers: Vec::new(),
                    duplicate_strategy: DuplicateStrategy::Error,
                },
                "summary",
            )
            .await;

        assert!(
            matches!(&result, Err(StorageError::Invariant(message)) if message == "diary ingest requires exactly one drawer"),
            "empty diary ingests must return an error instead of panicking: {result:?}"
        );
    }

    #[tokio::test]
    async fn reject_over_limit_summary_in_store_diary_summary() {
        let tempdir = tempdir().unwrap();
        let engine = StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();
        let drawer_id = DrawerId::new("over_limit_summary").unwrap();

        let over_limit = "x".repeat(DIARY_SUMMARY_MAX_CHARS + 1);
        let result = engine
            .operational_store()
            .store_diary_summary(&drawer_id, &over_limit);
        assert!(
            result.is_err(),
            "store_diary_summary must reject summaries exceeding {DIARY_SUMMARY_MAX_CHARS} characters"
        );
        let err = result.unwrap_err().to_string();
        assert!(err.contains("diary summary exceeds maximum length"), "error: {err}");
    }

    #[tokio::test]
    async fn reject_over_limit_summary_in_create_pending_diary_run() {
        let tempdir = tempdir().unwrap();
        let engine = StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();
        let drawer_id = DrawerId::new("pending_run_over_limit").unwrap();

        let over_limit = "z".repeat(DIARY_SUMMARY_MAX_CHARS + 1);
        let result = engine
            .operational_store()
            .create_pending_diary_run(
                "diary",
                "diary:pending_run_over_limit",
                &[],
                &drawer_id,
                &over_limit,
                datetime!(2026-06-01 10:00:00 UTC),
            );
        assert!(
            result.is_err(),
            "create_pending_diary_run must reject summaries exceeding {DIARY_SUMMARY_MAX_CHARS} characters"
        );
        let err = result.unwrap_err().to_string();
        assert!(err.contains("diary summary exceeds maximum length"), "error: {err}");
    }

    #[tokio::test]
    async fn summary_survives_across_engine_restart() {
        let tempdir = tempdir().unwrap();
        let drawer_id = DrawerId::new("survive_restart_summary").unwrap();
        let summary_text = "persistent summary across restart";

        // First engine: write diary entry with summary.
        {
            let engine =
                StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();
            let record = diary_record("survive_restart_summary", "diary content for restart test");
            engine
                .commit_diary_ingest(
                    IngestCommitRequest {
                        ingest_kind: "diary".to_owned(),
                        source_key: "diary:survive_restart_summary".to_owned(),
                        source_file: "diary:test".to_owned(),
                        content_hash: record.content_hash.clone(),
                        drawers: vec![record],
                        duplicate_strategy: DuplicateStrategy::Error,
                    },
                    summary_text,
                )
                .await
                .unwrap();
        }

        // Second engine (simulates restart): verify summary and drawer survive.
        {
            let engine =
                StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();

            let stored = engine.operational_store().get_diary_summary(&drawer_id).unwrap();
            assert_eq!(stored, Some(summary_text.to_owned()));

            let drawer =
                engine.drawer_store().get_drawer(&drawer_id).await.unwrap();
            assert!(drawer.is_some(), "drawer must survive restart");
        }
    }

    #[tokio::test]
    async fn reconcile_cleans_up_stale_pending_diary_run_with_summary() {
        let tempdir = tempdir().unwrap();
        let engine = StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();
        let drawer_id = DrawerId::new("stale_pending_diary_summary").unwrap();

        // Simulate a stale pending diary run with a stored summary — no committed
        // drawer.  This is what a crash after create_pending_diary_run but before
        // LanceDB write would leave behind.
        engine
            .operational_store()
            .store_diary_summary(&drawer_id, "stale pending summary")
            .unwrap();
        let _run = engine
            .operational_store()
            .create_pending_run(
                "diary",
                "diary:stale_pending_diary_summary",
                &[crate::types::IngestManifestEntry {
                    run_id: 0,
                    drawer_id: drawer_id.clone(),
                    source_file: "diary:test".to_owned(),
                    content_hash: "hash".to_owned(),
                    status: crate::types::IngestRunStatus::Pending,
                }],
                datetime!(2026-03-01 00:00:00 UTC),
            )
            .unwrap();

        // Summary should exist before reconcile.
        assert!(engine.operational_store().get_diary_summary(&drawer_id).unwrap().is_some());

        engine.reconcile().await.unwrap();

        // Summary should be cleaned up by reconcile.
        assert_eq!(
            engine.operational_store().get_diary_summary(&drawer_id).unwrap(),
            None,
            "stale pending diary run's summary must be removed by reconcile"
        );
    }

    #[tokio::test]
    async fn failed_diary_cleanup_removes_summary_but_keeps_existing_drawer() {
        let tempdir = tempdir().unwrap();
        let engine = StorageEngine::open(tempdir.path(), EmbeddingProfile::Balanced).await.unwrap();

        let existing_id = DrawerId::new("existing_drawer_survives").unwrap();

        // First, create a committed diary entry so an existing drawer exists.
        let existing_record = diary_record("existing_drawer_survives", "existing diary content");
        engine
            .commit_diary_ingest(
                IngestCommitRequest {
                    ingest_kind: "diary".to_owned(),
                    source_key: "diary:existing_drawer_survives".to_owned(),
                    source_file: "diary:test".to_owned(),
                    content_hash: existing_record.content_hash.clone(),
                    drawers: vec![existing_record],
                    duplicate_strategy: DuplicateStrategy::Error,
                },
                "existing summary",
            )
            .await
            .unwrap();

        // Now attempt a second commit_diary_ingest with a DIFFERENT drawer ID but
        // an invalid embedding dimension, so that put_drawers fails AFTER
        // create_pending_diary_run has already stored the summary.
        let failing_id = DrawerId::new("failing_new_drawer").unwrap();
        let mut failing_record = diary_record("failing_new_drawer", "will fail");
        failing_record.embedding = vec![1.0, 2.0, 3.0]; // wrong dimensions (3 vs 384)

        let result = engine
            .commit_diary_ingest(
                IngestCommitRequest {
                    ingest_kind: "diary".to_owned(),
                    source_key: "diary:failing_new_drawer".to_owned(),
                    source_file: "diary:test".to_owned(),
                    content_hash: "failing-hash".to_owned(),
                    drawers: vec![failing_record],
                    duplicate_strategy: DuplicateStrategy::Error,
                },
                "summary from failed write",
            )
            .await;

        // The error should be InvalidEmbeddingDimensions from LanceDB put_drawers.
        assert!(
            matches!(&result, Err(StorageError::InvalidEmbeddingDimensions { .. })),
            "commit_diary_ingest must propagate put_drawers failure; got: {result:?}",
        );

        // Summary for the failed entry must have been cleaned up.
        assert_eq!(
            engine.operational_store().get_diary_summary(&failing_id).unwrap(),
            None,
            "summary for failed write must be removed",
        );

        // The existing drawer must still be present.
        let existing = engine.drawer_store().get_drawer(&existing_id).await.unwrap();
        assert!(existing.is_some(), "existing drawer must survive summary-write failure cleanup");

        // The failing new drawer must NOT be visible in LanceDB.
        let failing = engine.drawer_store().get_drawer(&failing_id).await.unwrap();
        assert!(failing.is_none(), "failing new drawer must not appear in LanceDB");

        // A failed run should exist in the operational store.
        let stale_failed = engine
            .operational_store()
            .stale_failed_diary_runs(datetime!(2026-08-20 00:00:00 UTC))
            .unwrap();
        assert!(!stale_failed.is_empty(), "a failed diary run should exist");
    }
}

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

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use arrow_array::{
    Array, ArrayRef, Date32Array, FixedSizeListArray, Float32Array, RecordBatch,
    RecordBatchIterator, StringArray, TimestampMicrosecondArray, UInt32Array, UInt64Array,
    cast::AsArray, types::Float32Type,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use futures::TryStreamExt;
use lancedb::connection::Connection;
use lancedb::database::CreateTableMode;
use lancedb::index::Index;
use lancedb::query::{ExecutableQuery, QueryBase};
use lancedb::table::NewColumnTransform;
use serde::{Deserialize, Serialize};
use time::{Date, OffsetDateTime};

use crate::error::{Result, StorageError};
use crate::types::{DrawerFilter, DrawerMatch, DrawerStore, DuplicateStrategy, SearchRequest};
use mempalace_core::{DrawerId, DrawerRecord, EmbeddingProfile};

/// Statistics about vector-index coverage for the embedding column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorIndexStats {
    /// Number of rows covered by the vector index.
    pub indexed_rows: usize,
    /// Number of rows not covered by the vector index.
    pub unindexed_rows: usize,
    /// Alias for unindexed_rows — rows that form the "tail" not yet indexed.
    pub tail_rows: usize,
    /// Total rows in the table.
    pub total_rows: usize,
}

/// Fragment and version state for the drawers table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FragmentStats {
    /// Total number of data fragments.
    pub num_fragments: usize,
    /// Number of fragments small enough to be compaction candidates.
    pub num_small_fragments: usize,
    /// Total on-disk bytes for the table.
    pub total_bytes: usize,
    /// Number of historical versions.
    pub num_versions: usize,
    /// The current (latest) version number.
    pub current_version: u64,
}

/// Before/after metrics for a compaction or index-optimisation operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OptimizeMetrics {
    /// Tail (unindexed/small-fragment) rows before the operation.
    pub tail_rows_before: usize,
    /// Tail rows after the operation.
    pub tail_rows_after: usize,
    /// Indexed rows (or total fragments) before.
    pub indexed_rows_before: usize,
    /// Indexed rows (or total fragments) after.
    pub indexed_rows_after: usize,
    /// Total on-disk bytes before the operation.
    pub bytes_before: usize,
    /// Total on-disk bytes after the operation.
    pub bytes_after: usize,
    /// Whether the operation actually ran (vs. being skipped due to threshold).
    pub ran: bool,
}

/// Metrics for a version-prune operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PruneMetrics {
    /// Number of versions before pruning.
    pub versions_before: usize,
    /// Number of versions after pruning.
    pub versions_after: usize,
    /// Number of versions removed.
    pub versions_removed: usize,
    /// Bytes freed by pruning.
    pub bytes_freed: usize,
    /// Total bytes before pruning.
    pub total_bytes_before: usize,
    /// Total bytes after pruning.
    pub total_bytes_after: usize,
}

const DRAWERS_TABLE: &str = "drawers";
const DISTANCE_COLUMN: &str = "_distance";
const MIN_VECTOR_INDEX_ROWS: usize = 256;

#[derive(Debug, Clone)]
pub struct LanceDrawerStore {
    root: PathBuf,
    profile: EmbeddingProfile,
}

impl LanceDrawerStore {
    pub fn new(root: impl AsRef<Path>, profile: EmbeddingProfile) -> Self {
        Self { root: root.as_ref().to_path_buf(), profile }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    async fn connect(&self) -> Result<Connection> {
        if let Some(parent) = self.root.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|source| StorageError::Io { path: parent.to_path_buf(), source })?;
        }
        std::fs::create_dir_all(&self.root)
            .map_err(|source| StorageError::Io { path: self.root.clone(), source })?;
        Ok(lancedb::connect(self.root.to_string_lossy().as_ref()).execute().await?)
    }

    fn schema(&self) -> SchemaRef {
        let dimensions = self.profile.metadata().dimensions as i32;
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("wing", DataType::Utf8, false),
            Field::new("room", DataType::Utf8, false),
            Field::new("hall", DataType::Utf8, true),
            Field::new("date", DataType::Date32, true),
            Field::new("source_file", DataType::Utf8, false),
            Field::new("chunk_index", DataType::UInt32, false),
            Field::new("ingest_mode", DataType::Utf8, false),
            Field::new("extract_mode", DataType::Utf8, true),
            Field::new("added_by", DataType::Utf8, false),
            Field::new(
                "filed_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("importance", DataType::Float32, true),
            Field::new("emotional_weight", DataType::Float32, true),
            Field::new("weight", DataType::Float32, true),
            Field::new("content", DataType::Utf8, false),
            Field::new("content_hash", DataType::Utf8, false),
            // Locator columns (nullable; absent in old tables — migrated by ensure_schema).
            // UInt64 is stored as arrow UInt64 (BIGINT UNSIGNED in DataFusion SQL).
            // UInt32 is stored as arrow UInt32 (INT UNSIGNED in DataFusion SQL).
            Field::new("locator_byte_start", DataType::UInt64, true),
            Field::new("locator_byte_end", DataType::UInt64, true),
            Field::new("locator_line_start", DataType::UInt32, true),
            Field::new("locator_line_end", DataType::UInt32, true),
            Field::new("locator_file_hash", DataType::Utf8, true),
            Field::new("locator_resolve_root", DataType::Utf8, true),
            Field::new("locator_commit", DataType::Utf8, true),
            // View-metadata columns (nullable; absent in old tables — migrated by ensure_schema).
            Field::new("view_repo_id", DataType::Utf8, true),
            Field::new("view_name", DataType::Utf8, true),
            Field::new("view_source_path", DataType::Utf8, true),
            Field::new("view_head_commit", DataType::Utf8, true),
            Field::new("view_base_ref", DataType::Utf8, true),
            Field::new("view_merge_base", DataType::Utf8, true),
            Field::new("view_worktree_id", DataType::Utf8, true),
            Field::new("view_path_state", DataType::Utf8, true),
            Field::new(
                "embedding",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    dimensions,
                ),
                true,
            ),
        ]))
    }

    async fn table(&self) -> Result<lancedb::Table> {
        let connection = self.connect().await?;
        Ok(connection.open_table(DRAWERS_TABLE).execute().await?)
    }

    pub(crate) async fn existing_ids(&self, ids: &[DrawerId]) -> Result<HashSet<String>> {
        if ids.is_empty() {
            return Ok(HashSet::new());
        }
        let filter = DrawerFilter {
            ids: ids.to_vec(),
            include_all_views: true,
            ..DrawerFilter::default()
        };
        Ok(self
            .list_drawers(&filter)
            .await?
            .into_iter()
            .map(|record| record.id.as_str().to_owned())
            .collect())
    }

    async fn ensure_indices(&self, table: &lancedb::Table) -> Result<()> {
        let existing = table.list_indices().await?;
        let indexed_columns = existing
            .into_iter()
            .filter_map(|config| (config.columns.len() == 1).then(|| config.columns[0].clone()))
            .collect::<HashSet<_>>();

        if !indexed_columns.contains("id") {
            table.create_index(&["id"], Index::Auto).execute().await?;
        }
        if !indexed_columns.contains("wing") {
            table.create_index(&["wing"], Index::Auto).execute().await?;
        }
        if !indexed_columns.contains("room") {
            table.create_index(&["room"], Index::Auto).execute().await?;
        }
        if !indexed_columns.contains("source_file") {
            table.create_index(&["source_file"], Index::Auto).execute().await?;
        }

        if !indexed_columns.contains("embedding")
            && table.count_rows(None).await? >= MIN_VECTOR_INDEX_ROWS
        {
            table.create_index(&["embedding"], Index::Auto).execute().await?;
        }

        Ok(())
    }

    /// Capture vector-index coverage for the embedding column.
    pub(crate) async fn vector_index_stats(&self) -> Result<VectorIndexStats> {
        let table = self.table().await?;
        let total_rows = table.count_rows(None).await?;

        let indices = table.list_indices().await?;
        let embedding_index = indices.iter().find(|cfg| cfg.columns.contains(&"embedding".into()));
        let index_name = match embedding_index {
            Some(cfg) => &cfg.name,
            None => {
                return Ok(VectorIndexStats {
                    indexed_rows: 0,
                    unindexed_rows: total_rows,
                    tail_rows: total_rows,
                    total_rows,
                });
            }
        };

        let index_stats = table.index_stats(index_name).await?;
        match index_stats {
            Some(stats) => Ok(VectorIndexStats {
                indexed_rows: stats.num_indexed_rows,
                unindexed_rows: stats.num_unindexed_rows,
                tail_rows: stats.num_unindexed_rows,
                total_rows,
            }),
            None => Ok(VectorIndexStats {
                indexed_rows: 0,
                unindexed_rows: total_rows,
                tail_rows: total_rows,
                total_rows,
            }),
        }
    }

    /// Capture fragment and version state for the drawers table.
    pub(crate) async fn fragment_stats(&self) -> Result<FragmentStats> {
        let table = self.table().await?;
        let stats = table.stats().await?;
        let versions = table.list_versions().await?;
        let current_version = table.version().await?;
        Ok(FragmentStats {
            num_fragments: stats.fragment_stats.num_fragments,
            num_small_fragments: stats.fragment_stats.num_small_fragments,
            total_bytes: stats.total_bytes,
            num_versions: versions.len(),
            current_version,
        })
    }

    /// Incrementally optimise the vector index when the unindexed tail exceeds
    /// `tail_threshold`.  Returns before/after metrics.
    pub(crate) async fn optimize_vector_index(
        &self,
        tail_threshold: u64,
    ) -> Result<OptimizeMetrics> {
        let before = self.vector_index_stats().await?;
        if before.tail_rows <= tail_threshold as usize {
            return Ok(OptimizeMetrics {
                tail_rows_before: before.tail_rows,
                tail_rows_after: before.tail_rows,
                indexed_rows_before: before.indexed_rows,
                indexed_rows_after: before.indexed_rows,
                bytes_before: 0,
                bytes_after: 0,
                ran: false,
            });
        }
        let table = self.table().await?;
        let _optimize_stats = table
            .optimize(lancedb::table::OptimizeAction::Index(
                lancedb::table::OptimizeOptions::default(),
            ))
            .await?;
        let after = self.vector_index_stats().await?;
        Ok(OptimizeMetrics {
            tail_rows_before: before.tail_rows,
            tail_rows_after: after.tail_rows,
            indexed_rows_before: before.indexed_rows,
            indexed_rows_after: after.indexed_rows,
            bytes_before: 0,
            bytes_after: 0,
            ran: true,
        })
    }

    /// Compact fragmented data in the drawers table.
    /// Only compacts when `small_fragment_threshold` is exceeded.
    /// Returns before/after fragment metrics.
    pub(crate) async fn compact_fragments(
        &self,
        small_fragment_threshold: usize,
    ) -> Result<OptimizeMetrics> {
        let before = self.fragment_stats().await?;
        if before.num_small_fragments <= small_fragment_threshold {
            return Ok(OptimizeMetrics {
                tail_rows_before: before.num_small_fragments,
                tail_rows_after: before.num_small_fragments,
                indexed_rows_before: before.num_fragments,
                indexed_rows_after: before.num_fragments,
                bytes_before: before.total_bytes,
                bytes_after: before.total_bytes,
                ran: false,
            });
        }
        let table = self.table().await?;
        let _optimize_stats = table
            .optimize(lancedb::table::OptimizeAction::Compact {
                options: lancedb::table::CompactionOptions::default(),
                remap_options: None,
            })
            .await?;
        let after = self.fragment_stats().await?;
        Ok(OptimizeMetrics {
            tail_rows_before: before.num_small_fragments,
            tail_rows_after: after.num_small_fragments,
            indexed_rows_before: before.num_fragments,
            indexed_rows_after: after.num_fragments,
            bytes_before: before.total_bytes,
            bytes_after: after.total_bytes,
            ran: true,
        })
    }

    /// Prune historical versions older than `retention_hours`, retaining at
    /// least one version (the latest).  Never removes all recoverable history.
    /// Returns the number of versions removed and bytes freed.
    pub(crate) async fn prune_versions(&self, retention_hours: u64) -> Result<PruneMetrics> {
        let before = self.fragment_stats().await?;
        let table = self.table().await?;
        let versions = table.list_versions().await?;
        let versions_before = versions.len();

        let older_than = lancedb::table::Duration::try_hours(retention_hours as i64)
            .ok_or_else(|| {
                StorageError::Invariant(format!("invalid retention_hours: {retention_hours}"))
            })?;
        let _optimize_stats = table
            .optimize(lancedb::table::OptimizeAction::Prune {
                older_than: Some(older_than),
                delete_unverified: Some(false),
                error_if_tagged_old_versions: Some(false),
            })
            .await?;

        let after = self.fragment_stats().await?;
        let versions_removed = versions_before.saturating_sub(after.num_versions);
        let bytes_freed = before.total_bytes.saturating_sub(after.total_bytes);
        Ok(PruneMetrics {
            versions_before,
            versions_after: after.num_versions,
            versions_removed,
            bytes_freed,
            total_bytes_before: before.total_bytes,
            total_bytes_after: after.total_bytes,
        })
    }

    async fn execute_search(
        &self,
        table: &lancedb::Table,
        request: &SearchRequest,
        limit: usize,
    ) -> Result<Vec<DrawerMatch>> {
        let mut query = table.vector_search(request.embedding.clone())?.limit(limit);

        let filter = compile_filter(&request.filter);
        if !filter.is_empty() {
            query = query.only_if(filter);
        }

        let stream = query.execute().await?;
        let batches = stream.try_collect::<Vec<_>>().await?;
        matches_from_batches(&batches)
    }

    async fn search_drawers_with_cutoff_ties(
        &self,
        table: &lancedb::Table,
        request: &SearchRequest,
    ) -> Result<Vec<DrawerMatch>> {
        let mut fetch_limit = request.limit;

        loop {
            let mut matches = self.execute_search(table, request, fetch_limit).await?;
            if matches.len() < fetch_limit {
                truncate_to_cutoff_ties(&mut matches, request.limit);
                return Ok(matches);
            }

            let Some(cutoff_distance) =
                matches.get(request.limit - 1).and_then(|entry| entry.distance)
            else {
                return Ok(matches);
            };
            let Some(last_distance) = matches.last().and_then(|entry| entry.distance) else {
                return Ok(matches);
            };

            if last_distance.total_cmp(&cutoff_distance).is_gt() {
                truncate_to_cutoff_ties(&mut matches, request.limit);
                return Ok(matches);
            }

            let next_limit = fetch_limit.saturating_mul(2);
            if next_limit == fetch_limit {
                return Ok(matches);
            }
            fetch_limit = next_limit;
        }
    }
}

#[async_trait]
impl DrawerStore for LanceDrawerStore {
    async fn ensure_schema(&self) -> Result<()> {
        let connection = self.connect().await?;
        let schema = self.schema();

        // Open the existing table or create a new empty one. We open first instead of
        // creating with ExistOk directly because lancedb 0.23.1 validates schema
        // equality in ExistOk mode, which would fail for old tables that predate the
        // locator columns. On the create path ExistOk is safe: a concurrent creator
        // uses this same (new) schema, so the equality check passes.
        let table = match connection.open_table(DRAWERS_TABLE).execute().await {
            Ok(existing) => existing,
            Err(lancedb::error::Error::TableNotFound { .. }) => {
                connection
                    .create_empty_table(DRAWERS_TABLE, schema.clone())
                    .mode(CreateTableMode::exist_ok(|request| request))
                    .execute()
                    .await?
            }
            Err(err) => return Err(err.into()),
        };

        // Migrate: compute columns present in expected schema but missing from the table.
        // This is idempotent and handles old tables that predate the locator columns.
        // SQL type expressions are picked below based on the Arrow DataType.
        let existing_fields: std::collections::HashSet<String> = table
            .schema()
            .await?
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        // Lance/DataFusion SQL type names (verified against lance-datafusion-1.0.1/src/planner.rs):
        //   UInt64 → "BIGINT UNSIGNED"
        //   UInt32 → "INT UNSIGNED"
        //   Utf8   → "STRING"  (VARCHAR(None) is NOT accepted; "string" is listed as supported)
        let missing_columns: Vec<(String, String)> = schema
            .fields()
            .iter()
            .filter(|f| !existing_fields.contains(f.name()))
            .map(|f| {
                let sql_expr = match f.data_type() {
                    DataType::UInt64 => "CAST(NULL AS BIGINT UNSIGNED)".to_owned(),
                    DataType::UInt32 => "CAST(NULL AS INT UNSIGNED)".to_owned(),
                    DataType::Utf8 => "CAST(NULL AS STRING)".to_owned(),
                    _ => "NULL".to_owned(),
                };
                (f.name().clone(), sql_expr)
            })
            .collect();

        if !missing_columns.is_empty() {
            table
                .add_columns(NewColumnTransform::SqlExpressions(missing_columns), None)
                .await
                .map_err(|err| StorageError::Invariant(format!("schema migration failed: {err}")))?;
        }

        self.ensure_indices(&table).await?;
        Ok(())
    }

    async fn put_drawers(
        &self,
        drawers: &[DrawerRecord],
        strategy: DuplicateStrategy,
    ) -> Result<()> {
        if drawers.is_empty() {
            return Ok(());
        }

        let expected_dimensions = self.profile.metadata().dimensions;
        for drawer in drawers {
            if drawer.embedding.len() != expected_dimensions {
                return Err(StorageError::InvalidEmbeddingDimensions {
                    drawer_id: drawer.id.as_str().to_owned(),
                    expected: expected_dimensions,
                    actual: drawer.embedding.len(),
                });
            }
        }

        let ids = drawers.iter().map(|drawer| drawer.id.clone()).collect::<Vec<_>>();
        let existing = self.existing_ids(&ids).await?;
        match strategy {
            DuplicateStrategy::Error if !existing.is_empty() => {
                let mut duplicates = existing.into_iter().collect::<Vec<_>>();
                duplicates.sort();
                return Err(StorageError::DuplicateDrawers(duplicates));
            }
            DuplicateStrategy::Ignore => {
                let filtered = drawers
                    .iter()
                    .filter(|drawer| !existing.contains(drawer.id.as_str()))
                    .cloned()
                    .collect::<Vec<_>>();
                if filtered.is_empty() {
                    return Ok(());
                }
                let table = self.table().await?;
                table.add(drawers_to_reader(self.schema(), &filtered)?).execute().await?;
                return Ok(());
            }
            DuplicateStrategy::Overwrite => {
                if !existing.is_empty() {
                    let existing_ids = existing.into_iter().collect::<Vec<_>>();
                    let delete_ids = existing_ids
                        .iter()
                        .map(|value| DrawerId::new(value.clone()))
                        .collect::<std::result::Result<Vec<_>, _>>()?;
                    self.delete_drawers(&delete_ids).await?;
                }
            }
            DuplicateStrategy::Error => {}
        }

        let table = self.table().await?;
        table.add(drawers_to_reader(self.schema(), drawers)?).execute().await?;
        self.ensure_indices(&table).await?;
        Ok(())
    }

    async fn get_drawer(&self, id: &DrawerId) -> Result<Option<DrawerRecord>> {
        let mut filter = DrawerFilter { include_all_views: true, ..DrawerFilter::default() };
        filter.ids.push(id.clone());
        Ok(self.list_drawers(&filter).await?.into_iter().next())
    }

    async fn delete_drawers(&self, ids: &[DrawerId]) -> Result<usize> {
        if ids.is_empty() {
            return Ok(0);
        }
        let table = self.table().await?;
        let predicate = format!("id IN ({})", quote_ids(ids));
        // LanceDB's delete does not report affected rows; count matches first so
        // callers can distinguish "deleted" from "was never there".
        let existing = table.count_rows(Some(predicate.clone())).await?;
        table.delete(&predicate).await?;
        Ok(existing)
    }

    async fn search_drawers(&self, request: &SearchRequest) -> Result<Vec<DrawerMatch>> {
        let expected_dimensions = self.profile.metadata().dimensions;
        if request.embedding.len() != expected_dimensions {
            return Err(StorageError::Invariant(format!(
                "query embedding dimension mismatch: expected {expected_dimensions}, got {}",
                request.embedding.len()
            )));
        }

        if request.limit == 0 {
            return Ok(Vec::new());
        }

        let table = self.table().await?;
        if request.include_cutoff_ties {
            self.search_drawers_with_cutoff_ties(&table, request).await
        } else {
            self.execute_search(&table, request, request.limit).await
        }
    }

    async fn list_drawers(&self, filter: &DrawerFilter) -> Result<Vec<DrawerRecord>> {
        let table = self.table().await?;
        let mut query = table.query();
        let filter_sql = compile_filter(filter);
        if !filter_sql.is_empty() {
            query = query.only_if(filter_sql);
        }
        if let Some(limit) = filter.limit {
            query = query.limit(limit);
        }
        let stream = query.execute().await?;
        let batches = stream.try_collect::<Vec<_>>().await?;
        records_from_batches(&batches)
    }
}

fn drawers_to_reader(
    schema: SchemaRef,
    drawers: &[DrawerRecord],
) -> Result<
    Box<
        RecordBatchIterator<
            std::vec::IntoIter<std::result::Result<RecordBatch, arrow_schema::ArrowError>>,
        >,
    >,
> {
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                drawers.iter().map(|drawer| Some(drawer.id.as_str())).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                drawers.iter().map(|drawer| Some(drawer.wing.as_str())).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                drawers.iter().map(|drawer| Some(drawer.room.as_str())).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                drawers.iter().map(|drawer| drawer.hall.as_deref()).collect::<Vec<_>>(),
            )),
            Arc::new(Date32Array::from(
                drawers.iter().map(|drawer| drawer.date.map(date_to_days)).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                drawers.iter().map(|drawer| Some(drawer.source_file.as_str())).collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                drawers.iter().map(|drawer| drawer.chunk_index).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                drawers.iter().map(|drawer| Some(drawer.ingest_mode.as_str())).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                drawers.iter().map(|drawer| drawer.extract_mode.as_deref()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                drawers.iter().map(|drawer| Some(drawer.added_by.as_str())).collect::<Vec<_>>(),
            )),
            Arc::new(
                TimestampMicrosecondArray::from(
                    drawers
                        .iter()
                        .map(|drawer| (drawer.filed_at.unix_timestamp_nanos() / 1_000) as i64)
                        .collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(Float32Array::from(
                drawers.iter().map(|drawer| drawer.importance).collect::<Vec<_>>(),
            )),
            Arc::new(Float32Array::from(
                drawers.iter().map(|drawer| drawer.emotional_weight).collect::<Vec<_>>(),
            )),
            Arc::new(Float32Array::from(
                drawers.iter().map(|drawer| drawer.weight).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                drawers.iter().map(|drawer| Some(drawer.content.as_str())).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                drawers.iter().map(|drawer| Some(drawer.content_hash.as_str())).collect::<Vec<_>>(),
            )),
            // Locator columns — None for non-locator rows.
            Arc::new(UInt64Array::from(
                drawers
                    .iter()
                    .map(|drawer| drawer.locator.as_ref().map(|loc| loc.byte_start))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                drawers
                    .iter()
                    .map(|drawer| drawer.locator.as_ref().map(|loc| loc.byte_end))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                drawers
                    .iter()
                    .map(|drawer| drawer.locator.as_ref().map(|loc| loc.line_start))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                drawers
                    .iter()
                    .map(|drawer| drawer.locator.as_ref().map(|loc| loc.line_end))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                drawers
                    .iter()
                    .map(|drawer| drawer.locator.as_ref().map(|loc| loc.file_hash.as_str()))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                drawers
                    .iter()
                    .map(|drawer| drawer.locator.as_ref().map(|loc| loc.resolve_root.as_str()))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                drawers
                    .iter()
                    .map(|drawer| {
                        drawer
                            .locator
                            .as_ref()
                            .and_then(|loc| loc.commit_hash.as_deref())
                    })
                    .collect::<Vec<_>>(),
            )),
            // View-metadata columns.
            Arc::new(StringArray::from(
                drawers
                    .iter()
                    .map(|drawer| drawer.view_metadata.as_ref().map(|vm| vm.repo_id.as_str()))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                drawers
                    .iter()
                    .map(|drawer| drawer.view_metadata.as_ref().and_then(|vm| vm.view_name.as_deref()))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                drawers
                    .iter()
                    .map(|drawer| drawer.view_metadata.as_ref().map(|vm| vm.source_path.as_str()))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                drawers
                    .iter()
                    .map(|drawer| {
                        drawer.view_metadata.as_ref().and_then(|vm| vm.head_commit.as_deref())
                    })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                drawers
                    .iter()
                    .map(|drawer| {
                        drawer.view_metadata.as_ref().and_then(|vm| vm.base_ref.as_deref())
                    })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                drawers
                    .iter()
                    .map(|drawer| {
                        drawer.view_metadata.as_ref().and_then(|vm| vm.merge_base.as_deref())
                    })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                drawers
                    .iter()
                    .map(|drawer| drawer.view_metadata.as_ref().map(|vm| vm.worktree_id.as_str()))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                drawers
                    .iter()
                    .map(|drawer| drawer.view_metadata.as_ref().map(|vm| vm.path_state.as_str()))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                drawers.iter().map(|drawer| {
                    Some(drawer.embedding.iter().copied().map(Some).collect::<Vec<_>>())
                }),
                drawers[0].embedding.len() as i32,
            )),
        ],
    )?;

    Ok(Box::new(RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema)))
}

fn compile_filter(filter: &DrawerFilter) -> String {
    let mut parts = Vec::new();

    if !filter.ids.is_empty() {
        parts.push(format!("id IN ({})", quote_ids(&filter.ids)));
    }
    if let Some(wing) = &filter.wing {
        parts.push(format!("wing = '{}'", escape_sql(wing.as_str())));
    }
    if let Some(room) = &filter.room {
        parts.push(format!("room = '{}'", escape_sql(room.as_str())));
    }
    if let Some(hall) = &filter.hall {
        parts.push(format!("hall = '{}'", escape_sql(hall)));
    }
    if let Some(source_file) = &filter.source_file {
        parts.push(format!("source_file = '{}'", escape_sql(source_file)));
    }
    if filter.include_all_views || filter.view.as_deref() == Some("full") {
        return parts.join(" AND ");
    }
    match filter.view.as_deref() {
        None | Some("canonical") => {
            // Canonical is the default repository truth. Keep non-project
            // drawers visible while excluding all branch-view rows.
            parts.push("ingest_mode != 'projects-branch'".to_owned());
        }
        Some(view) => {
            // Match view_name directly (new columns) with a hall fallback for
            // legacy rows written before the view-metadata migration.
            let escaped = escape_sql(view);
            let branch_predicate = format!(
                "(view_name = '{escaped}' OR (view_name IS NULL AND hall = 'view:{escaped}'))"
            );
            if filter.branch_view_only {
                parts.push(branch_predicate);
            } else {
                parts.push(format!("(ingest_mode != 'projects-branch' OR {branch_predicate})"));
            }
        }
    }

    parts.join(" AND ")
}

fn quote_ids(ids: &[DrawerId]) -> String {
    ids.iter().map(|id| format!("'{}'", escape_sql(id.as_str()))).collect::<Vec<_>>().join(", ")
}

fn escape_sql(value: &str) -> String {
    value.replace('\'', "''")
}

fn records_from_batches(batches: &[RecordBatch]) -> Result<Vec<DrawerRecord>> {
    let mut records = Vec::new();
    for batch in batches {
        let id = batch
            .column_by_name("id")
            .ok_or_else(|| StorageError::Invariant("missing `id` column".to_owned()))?
            .as_string::<i32>();
        let wing = batch
            .column_by_name("wing")
            .ok_or_else(|| StorageError::Invariant("missing `wing` column".to_owned()))?
            .as_string::<i32>();
        let room = batch
            .column_by_name("room")
            .ok_or_else(|| StorageError::Invariant("missing `room` column".to_owned()))?
            .as_string::<i32>();
        let hall = batch
            .column_by_name("hall")
            .ok_or_else(|| StorageError::Invariant("missing `hall` column".to_owned()))?
            .as_string::<i32>();
        let date = batch
            .column_by_name("date")
            .ok_or_else(|| StorageError::Invariant("missing `date` column".to_owned()))?
            .as_primitive::<arrow_array::types::Date32Type>();
        let source_file = batch
            .column_by_name("source_file")
            .ok_or_else(|| StorageError::Invariant("missing `source_file` column".to_owned()))?
            .as_string::<i32>();
        let chunk_index = batch
            .column_by_name("chunk_index")
            .ok_or_else(|| StorageError::Invariant("missing `chunk_index` column".to_owned()))?
            .as_primitive::<arrow_array::types::UInt32Type>();
        let ingest_mode = batch
            .column_by_name("ingest_mode")
            .ok_or_else(|| StorageError::Invariant("missing `ingest_mode` column".to_owned()))?
            .as_string::<i32>();
        let extract_mode = batch
            .column_by_name("extract_mode")
            .ok_or_else(|| StorageError::Invariant("missing `extract_mode` column".to_owned()))?
            .as_string::<i32>();
        let added_by = batch
            .column_by_name("added_by")
            .ok_or_else(|| StorageError::Invariant("missing `added_by` column".to_owned()))?
            .as_string::<i32>();
        let filed_at = batch
            .column_by_name("filed_at")
            .ok_or_else(|| StorageError::Invariant("missing `filed_at` column".to_owned()))?
            .as_primitive::<arrow_array::types::TimestampMicrosecondType>();
        let importance = batch
            .column_by_name("importance")
            .ok_or_else(|| StorageError::Invariant("missing `importance` column".to_owned()))?
            .as_primitive::<arrow_array::types::Float32Type>();
        let emotional_weight = batch
            .column_by_name("emotional_weight")
            .ok_or_else(|| StorageError::Invariant("missing `emotional_weight` column".to_owned()))?
            .as_primitive::<arrow_array::types::Float32Type>();
        let weight = batch
            .column_by_name("weight")
            .ok_or_else(|| StorageError::Invariant("missing `weight` column".to_owned()))?
            .as_primitive::<arrow_array::types::Float32Type>();
        let content = batch
            .column_by_name("content")
            .ok_or_else(|| StorageError::Invariant("missing `content` column".to_owned()))?
            .as_string::<i32>();
        let content_hash = batch
            .column_by_name("content_hash")
            .ok_or_else(|| StorageError::Invariant("missing `content_hash` column".to_owned()))?
            .as_string::<i32>();
        // Locator columns are optional (absent from tables created before schema migration).
        // Treat the whole group as absent (locator = None) when any required column is missing.
        let locator_byte_start =
            batch.column_by_name("locator_byte_start").map(|col| {
                col.as_primitive::<arrow_array::types::UInt64Type>()
            });
        let locator_byte_end =
            batch.column_by_name("locator_byte_end").map(|col| {
                col.as_primitive::<arrow_array::types::UInt64Type>()
            });
        let locator_line_start =
            batch.column_by_name("locator_line_start").map(|col| {
                col.as_primitive::<arrow_array::types::UInt32Type>()
            });
        let locator_line_end =
            batch.column_by_name("locator_line_end").map(|col| {
                col.as_primitive::<arrow_array::types::UInt32Type>()
            });
        let locator_file_hash =
            batch.column_by_name("locator_file_hash").map(|col| col.as_string::<i32>());
        let locator_resolve_root =
            batch.column_by_name("locator_resolve_root").map(|col| col.as_string::<i32>());
        let locator_commit =
            batch.column_by_name("locator_commit").map(|col| col.as_string::<i32>());
        // View-metadata columns are optional (absent from tables created before schema migration).
        let view_repo_id =
            batch.column_by_name("view_repo_id").map(|col| col.as_string::<i32>());
        let view_name =
            batch.column_by_name("view_name").map(|col| col.as_string::<i32>());
        let view_source_path =
            batch.column_by_name("view_source_path").map(|col| col.as_string::<i32>());
        let view_head_commit =
            batch.column_by_name("view_head_commit").map(|col| col.as_string::<i32>());
        let view_base_ref =
            batch.column_by_name("view_base_ref").map(|col| col.as_string::<i32>());
        let view_merge_base =
            batch.column_by_name("view_merge_base").map(|col| col.as_string::<i32>());
        let view_worktree_id =
            batch.column_by_name("view_worktree_id").map(|col| col.as_string::<i32>());
        let view_path_state =
            batch.column_by_name("view_path_state").map(|col| col.as_string::<i32>());
        let embedding = batch
            .column_by_name("embedding")
            .ok_or_else(|| StorageError::Invariant("missing `embedding` column".to_owned()))?
            .as_fixed_size_list();

        for row in 0..batch.num_rows() {
            let values = embedding.value(row);
            let vector =
                values.as_primitive::<arrow_array::types::Float32Type>().iter().collect::<Vec<_>>();
            records.push(DrawerRecord {
                id: DrawerId::new(id.value(row))?,
                wing: mempalace_core::WingId::new(wing.value(row))?,
                room: mempalace_core::RoomId::new(room.value(row))?,
                hall: (!hall.is_null(row)).then(|| hall.value(row).to_owned()),
                date: if date.is_null(row) { None } else { Some(days_to_date(date.value(row))?) },
                source_file: source_file.value(row).to_owned(),
                chunk_index: chunk_index.value(row),
                ingest_mode: ingest_mode.value(row).to_owned(),
                extract_mode: (!extract_mode.is_null(row))
                    .then(|| extract_mode.value(row).to_owned()),
                added_by: added_by.value(row).to_owned(),
                filed_at: OffsetDateTime::from_unix_timestamp_nanos(
                    i128::from(filed_at.value(row)) * 1_000,
                )
                .map_err(|err| StorageError::Invariant(err.to_string()))?,
                importance: (!importance.is_null(row)).then(|| importance.value(row)),
                emotional_weight: (!emotional_weight.is_null(row))
                    .then(|| emotional_weight.value(row)),
                weight: (!weight.is_null(row)).then(|| weight.value(row)),
                content: content.value(row).to_owned(),
                content_hash: content_hash.value(row).to_owned(),
                embedding: vector.into_iter().flatten().collect(),
                locator: build_locator(
                    row,
                    locator_byte_start,
                    locator_byte_end,
                    locator_line_start,
                    locator_line_end,
                    locator_file_hash,
                    locator_resolve_root,
                    locator_commit,
                ),
                view_metadata: build_view_metadata(
                    row,
                    view_repo_id,
                    view_name,
                    view_source_path,
                    view_head_commit,
                    view_base_ref,
                    view_merge_base,
                    view_worktree_id,
                    view_path_state,
                ),
            });
        }
    }
    Ok(records)
}

/// Reconstruct a [`SourceLocator`] from optional column arrays at the given row index.
///
/// Returns `Some` only when all four required fields (byte_start, byte_end, line_start,
/// line_end, file_hash, resolve_root) are non-null. commit_hash is always optional.
/// When any required column is entirely absent (old table, pre-migration) the callers
/// pass `None` slices and we return `None`.
fn build_locator(
    row: usize,
    byte_start: Option<&arrow_array::PrimitiveArray<arrow_array::types::UInt64Type>>,
    byte_end: Option<&arrow_array::PrimitiveArray<arrow_array::types::UInt64Type>>,
    line_start: Option<&arrow_array::PrimitiveArray<arrow_array::types::UInt32Type>>,
    line_end: Option<&arrow_array::PrimitiveArray<arrow_array::types::UInt32Type>>,
    file_hash: Option<&arrow_array::StringArray>,
    resolve_root: Option<&arrow_array::StringArray>,
    commit: Option<&arrow_array::StringArray>,
) -> Option<mempalace_core::SourceLocator> {
    let bs = byte_start.filter(|col| !col.is_null(row)).map(|col| col.value(row))?;
    let be = byte_end.filter(|col| !col.is_null(row)).map(|col| col.value(row))?;
    let ls = line_start.filter(|col| !col.is_null(row)).map(|col| col.value(row))?;
    let le = line_end.filter(|col| !col.is_null(row)).map(|col| col.value(row))?;
    let fh = file_hash
        .filter(|col| !col.is_null(row))
        .map(|col| col.value(row).to_owned())?;
    let rr = resolve_root
        .filter(|col| !col.is_null(row))
        .map(|col| col.value(row).to_owned())?;
    let ch = commit
        .filter(|col| !col.is_null(row))
        .map(|col| col.value(row).to_owned());
    Some(mempalace_core::SourceLocator {
        byte_start: bs,
        byte_end: be,
        line_start: ls,
        line_end: le,
        file_hash: fh,
        resolve_root: rr,
        commit_hash: ch,
    })
}

/// Reconstruct [`RepositoryViewMetadata`] from optional column arrays at the
/// given row index.  Returns `None` when the view_* columns are entirely absent
/// (old table, pre-migration) or when `view_repo_id` is null.
fn build_view_metadata(
    row: usize,
    repo_id: Option<&arrow_array::StringArray>,
    view_name: Option<&arrow_array::StringArray>,
    source_path: Option<&arrow_array::StringArray>,
    head_commit: Option<&arrow_array::StringArray>,
    base_ref: Option<&arrow_array::StringArray>,
    merge_base: Option<&arrow_array::StringArray>,
    worktree_id: Option<&arrow_array::StringArray>,
    path_state: Option<&arrow_array::StringArray>,
) -> Option<mempalace_core::RepositoryViewMetadata> {
    let rid = repo_id.filter(|col| !col.is_null(row)).map(|col| col.value(row).to_owned())?;
    let sp = source_path
        .filter(|col| !col.is_null(row))
        .map(|col| col.value(row).to_owned())?;
    let wtid = worktree_id
        .filter(|col| !col.is_null(row))
        .map(|col| col.value(row).to_owned())?;
    let ps = path_state
        .filter(|col| !col.is_null(row))
        .map(|col| col.value(row).to_owned())?;
    Some(mempalace_core::RepositoryViewMetadata {
        repo_id: rid,
        view_name: view_name
            .filter(|col| !col.is_null(row))
            .map(|col| col.value(row).to_owned()),
        source_path: sp,
        head_commit: head_commit
            .filter(|col| !col.is_null(row))
            .map(|col| col.value(row).to_owned()),
        base_ref: base_ref
            .filter(|col| !col.is_null(row))
            .map(|col| col.value(row).to_owned()),
        merge_base: merge_base
            .filter(|col| !col.is_null(row))
            .map(|col| col.value(row).to_owned()),
        worktree_id: wtid,
        path_state: ps,
    })
}

fn matches_from_batches(batches: &[RecordBatch]) -> Result<Vec<DrawerMatch>> {
    let records = records_from_batches(batches)?;
    let mut distances = Vec::new();
    for batch in batches {
        if let Some(distance_column) = batch.column_by_name(DISTANCE_COLUMN) {
            let distance = distance_column.as_primitive::<arrow_array::types::Float32Type>();
            for row in 0..batch.num_rows() {
                distances.push((!distance.is_null(row)).then(|| distance.value(row)));
            }
        } else {
            distances.extend(std::iter::repeat_n(None, batch.num_rows()));
        }
    }

    Ok(records
        .into_iter()
        .zip(distances)
        .map(|(record, distance)| DrawerMatch { record, distance })
        .collect())
}

fn truncate_to_cutoff_ties(matches: &mut Vec<DrawerMatch>, limit: usize) {
    if limit == 0 || matches.len() <= limit {
        return;
    }

    let Some(cutoff_distance) = matches.get(limit - 1).and_then(|entry| entry.distance) else {
        return;
    };
    let cutoff_len = matches.partition_point(|entry| {
        entry.distance.is_some_and(|distance| distance.total_cmp(&cutoff_distance).is_le())
    });
    matches.truncate(cutoff_len);
}

fn date_to_days(date: Date) -> i32 {
    date.to_julian_day()
        - Date::from_calendar_date(1970, time::Month::January, 1).unwrap().to_julian_day()
}

fn days_to_date(days: i32) -> Result<Date> {
    Date::from_julian_day(
        days + Date::from_calendar_date(1970, time::Month::January, 1).unwrap().to_julian_day(),
    )
    .map_err(|err| StorageError::Invariant(err.to_string()))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use time::macros::{date, datetime};

    use super::LanceDrawerStore;
    use crate::types::{DrawerFilter, DrawerStore, DuplicateStrategy, SearchRequest};
    use mempalace_core::{DrawerId, DrawerRecord, EmbeddingProfile, RoomId, WingId};

    fn embedding(seed: [f32; 4]) -> Vec<f32> {
        let mut values = Vec::with_capacity(EmbeddingProfile::Balanced.metadata().dimensions);
        while values.len() < EmbeddingProfile::Balanced.metadata().dimensions {
            values.extend(seed);
        }
        values.truncate(EmbeddingProfile::Balanced.metadata().dimensions);
        values
    }

    fn record(id: &str, wing: &str, room: &str, source_file: &str, seed: [f32; 4]) -> DrawerRecord {
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
            filed_at: datetime!(2026-04-11 10:00:00 UTC),
            importance: Some(0.5),
            emotional_weight: Some(0.1),
            weight: Some(1.0),
            content: format!("payload-{id}"),
            content_hash: format!("hash-{id}"),
            embedding: embedding(seed),
            locator: None,
            view_metadata: None,
        }
    }

    #[tokio::test]
    async fn supports_crud_and_filtering() {
        let tempdir = tempdir().unwrap();
        let store = LanceDrawerStore::new(tempdir.path().join("lance"), EmbeddingProfile::Balanced);
        store.ensure_schema().await.unwrap();

        let drawers = vec![
            record(
                "project_alpha/backend/0001",
                "project_alpha",
                "backend",
                "auth.py",
                [0.0, 0.1, 0.2, 0.3],
            ),
            record(
                "project_alpha/frontend/0002",
                "project_alpha",
                "frontend",
                "ui.tsx",
                [0.2, 0.2, 0.2, 0.2],
            ),
        ];
        store.put_drawers(&drawers, DuplicateStrategy::Error).await.unwrap();

        let fetched = store
            .get_drawer(&DrawerId::new("project_alpha/backend/0001").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.source_file, "auth.py");

        let filtered = store
            .list_drawers(&DrawerFilter {
                wing: Some(WingId::new("project_alpha").unwrap()),
                room: Some(RoomId::new("backend").unwrap()),
                ..DrawerFilter::default()
            })
            .await
            .unwrap();
        assert_eq!(filtered.len(), 1);

        let deleted = store
            .delete_drawers(&[DrawerId::new("project_alpha/frontend/0002").unwrap()])
            .await
            .unwrap();
        assert_eq!(deleted, 1);
    }

    #[tokio::test]
    async fn handles_duplicate_policies() {
        let tempdir = tempdir().unwrap();
        let store = LanceDrawerStore::new(tempdir.path().join("lance"), EmbeddingProfile::Balanced);
        store.ensure_schema().await.unwrap();

        let first = record(
            "project_alpha/backend/0001",
            "project_alpha",
            "backend",
            "auth.py",
            [0.1, 0.2, 0.3, 0.4],
        );
        store.put_drawers(&[first.clone()], DuplicateStrategy::Error).await.unwrap();

        let duplicate_err = store.put_drawers(&[first.clone()], DuplicateStrategy::Error).await;
        assert!(duplicate_err.is_err());

        store.put_drawers(&[first.clone()], DuplicateStrategy::Ignore).await.unwrap();
        let ignored = store.list_drawers(&DrawerFilter::default()).await.unwrap();
        assert_eq!(ignored.len(), 1);

        let overwritten = DrawerRecord { content: "updated".to_owned(), ..first };
        store.put_drawers(&[overwritten], DuplicateStrategy::Overwrite).await.unwrap();
        let current = store.list_drawers(&DrawerFilter::default()).await.unwrap();
        assert_eq!(current[0].content, "updated");
    }

    #[tokio::test]
    async fn executes_vector_search_with_filters() {
        let tempdir = tempdir().unwrap();
        let store = LanceDrawerStore::new(tempdir.path().join("lance"), EmbeddingProfile::Balanced);
        store.ensure_schema().await.unwrap();
        store
            .put_drawers(
                &[
                    record(
                        "project_alpha/backend/0001",
                        "project_alpha",
                        "backend",
                        "auth.py",
                        [1.0, 0.0, 0.0, 0.0],
                    ),
                    record(
                        "project_alpha/frontend/0002",
                        "project_alpha",
                        "frontend",
                        "ui.tsx",
                        [0.0, 1.0, 0.0, 0.0],
                    ),
                ],
                DuplicateStrategy::Error,
            )
            .await
            .unwrap();

        let results = store
            .search_drawers(&SearchRequest {
                embedding: embedding([1.0, 0.0, 0.0, 0.0]),
                limit: 2,
                include_cutoff_ties: false,
                filter: DrawerFilter {
                    room: Some(RoomId::new("backend").unwrap()),
                    ..DrawerFilter::default()
                },
            })
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].record.room.as_str(), "backend");
    }

    #[tokio::test]
    async fn vector_search_can_include_full_cutoff_tie_group() {
        let tempdir = tempdir().unwrap();
        let store = LanceDrawerStore::new(tempdir.path().join("lance"), EmbeddingProfile::Balanced);
        store.ensure_schema().await.unwrap();
        store
            .put_drawers(
                &[
                    record(
                        "project_alpha/general/0001",
                        "project_alpha",
                        "general",
                        "alpha.txt",
                        [1.0, 0.0, 0.0, 0.0],
                    ),
                    record(
                        "project_beta/general/0001",
                        "project_beta",
                        "general",
                        "beta.txt",
                        [1.0, 0.0, 0.0, 0.0],
                    ),
                    record(
                        "project_gamma/general/0001",
                        "project_gamma",
                        "general",
                        "gamma.txt",
                        [0.0, 1.0, 0.0, 0.0],
                    ),
                ],
                DuplicateStrategy::Error,
            )
            .await
            .unwrap();

        let results = store
            .search_drawers(&SearchRequest {
                embedding: embedding([1.0, 0.0, 0.0, 0.0]),
                limit: 1,
                include_cutoff_ties: true,
                filter: DrawerFilter::default(),
            })
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|entry| entry.distance == Some(0.0)));
    }

    #[tokio::test]
    async fn list_drawers_returns_all_rows_beyond_ten_thousand() {
        let tempdir = tempdir().unwrap();
        let store = LanceDrawerStore::new(tempdir.path().join("lance"), EmbeddingProfile::Balanced);
        store.ensure_schema().await.unwrap();

        let drawers = (0..10_005)
            .map(|index| {
                record(
                    &format!("project_alpha/backend/{index:04}"),
                    "project_alpha",
                    "backend",
                    "bulk.txt",
                    [0.1, 0.2, 0.3, 0.4],
                )
            })
            .collect::<Vec<_>>();
        store.put_drawers(&drawers, DuplicateStrategy::Error).await.unwrap();

        let results = store
            .list_drawers(&DrawerFilter {
                wing: Some(WingId::new("project_alpha").unwrap()),
                room: Some(RoomId::new("backend").unwrap()),
                ..DrawerFilter::default()
            })
            .await
            .unwrap();

        assert_eq!(results.len(), 10_005);
    }

    #[tokio::test]
    async fn ensure_schema_is_reopen_safe() {
        let tempdir = tempdir().unwrap();
        let root = tempdir.path().join("lance");

        let first = LanceDrawerStore::new(&root, EmbeddingProfile::Balanced);
        first.ensure_schema().await.unwrap();

        let reopened = LanceDrawerStore::new(&root, EmbeddingProfile::Balanced);
        reopened.ensure_schema().await.unwrap();
    }

    #[tokio::test]
    async fn ensure_schema_creates_id_index() {
        let tempdir = tempdir().unwrap();
        let store = LanceDrawerStore::new(tempdir.path().join("lance"), EmbeddingProfile::Balanced);
        store.ensure_schema().await.unwrap();

        let indices = store.table().await.unwrap().list_indices().await.unwrap();
        assert!(indices.iter().any(|index| index.columns.len() == 1 && index.columns[0] == "id"));
    }

    /// Back-compat: an old table (schema without locator columns) must be migrated
    /// transparently by `ensure_schema`. Old rows read back with `locator: None`
    /// and intact content; new locator rows write and read back fully.
    #[tokio::test]
    async fn schema_migration_adds_locator_columns_to_old_table() {
        use std::sync::Arc as StdArc;

        use arrow_array::{
            Date32Array, FixedSizeListArray, Float32Array, RecordBatch, RecordBatchIterator,
            StringArray, TimestampMicrosecondArray, UInt32Array,
        };
        use arrow_schema::{DataType, Field, Schema, TimeUnit};
        use lancedb::database::CreateTableMode;
        use mempalace_core::SourceLocator;

        let tempdir = tempdir().unwrap();
        let root = tempdir.path().join("lance_legacy");
        let dim = EmbeddingProfile::Balanced.metadata().dimensions as i32;

        // Build the OLD schema (no locator columns).
        let old_schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("wing", DataType::Utf8, false),
            Field::new("room", DataType::Utf8, false),
            Field::new("hall", DataType::Utf8, true),
            Field::new("date", DataType::Date32, true),
            Field::new("source_file", DataType::Utf8, false),
            Field::new("chunk_index", DataType::UInt32, false),
            Field::new("ingest_mode", DataType::Utf8, false),
            Field::new("extract_mode", DataType::Utf8, true),
            Field::new("added_by", DataType::Utf8, false),
            Field::new(
                "filed_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("importance", DataType::Float32, true),
            Field::new("emotional_weight", DataType::Float32, true),
            Field::new("weight", DataType::Float32, true),
            Field::new("content", DataType::Utf8, false),
            Field::new("content_hash", DataType::Utf8, false),
            Field::new(
                "embedding",
                DataType::FixedSizeList(
                    StdArc::new(Field::new("item", DataType::Float32, true)),
                    dim,
                ),
                true,
            ),
        ]));

        // Create an old-format table and insert one row directly via lancedb.
        std::fs::create_dir_all(&root).unwrap();
        let conn = lancedb::connect(root.to_string_lossy().as_ref()).execute().await.unwrap();
        let old_table = conn
            .create_empty_table("drawers", old_schema.clone())
            .mode(CreateTableMode::exist_ok(|r| r))
            .execute()
            .await
            .unwrap();

        let legacy_content = "legacy chunk text";
        let filed_at_us: i64 = 1_744_365_600_000_000; // some fixed µs timestamp
        let embed_vals: Vec<Option<f32>> =
            (0..dim as usize).map(|i| Some((i as f32) * 0.001)).collect();
        let old_batch = RecordBatch::try_new(
            old_schema.clone(),
            vec![
                StdArc::new(StringArray::from(vec![Some("leg/room/0001")])),
                StdArc::new(StringArray::from(vec![Some("leg")])),
                StdArc::new(StringArray::from(vec![Some("room")])),
                StdArc::new(StringArray::from(vec![None::<&str>])),
                StdArc::new(Date32Array::from(vec![None::<i32>])),
                StdArc::new(StringArray::from(vec![Some("legacy.txt")])),
                StdArc::new(UInt32Array::from(vec![0_u32])),
                StdArc::new(StringArray::from(vec![Some("legacy_ingest")])),
                StdArc::new(StringArray::from(vec![None::<&str>])),
                StdArc::new(StringArray::from(vec![Some("tester")])),
                StdArc::new(
                    TimestampMicrosecondArray::from(vec![filed_at_us]).with_timezone("UTC"),
                ),
                StdArc::new(Float32Array::from(vec![None::<f32>])),
                StdArc::new(Float32Array::from(vec![None::<f32>])),
                StdArc::new(Float32Array::from(vec![None::<f32>])),
                StdArc::new(StringArray::from(vec![Some(legacy_content)])),
                StdArc::new(StringArray::from(vec![Some("legacy-hash")])),
                StdArc::new(FixedSizeListArray::from_iter_primitive::<
                    arrow_array::types::Float32Type,
                    _,
                    _,
                >(std::iter::once(Some(embed_vals)), dim)),
            ],
        )
        .unwrap();
        old_table
            .add(RecordBatchIterator::new(
                vec![Ok(old_batch)].into_iter(),
                old_schema,
            ))
            .execute()
            .await
            .unwrap();

        // Now open via the new store — ensure_schema must migrate the table.
        let store = LanceDrawerStore::new(&root, EmbeddingProfile::Balanced);
        store.ensure_schema().await.unwrap();

        // Old row must read back with locator: None and intact content.
        let rows = store
            .list_drawers(&DrawerFilter::default())
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].content, legacy_content);
        assert!(rows[0].locator.is_none(), "legacy row must have locator: None");

        // Write a new row with a locator (commit_hash Some).
        let locator_with_commit = SourceLocator {
            byte_start: 10,
            byte_end: 20,
            line_start: 1,
            line_end: 2,
            file_hash: "abc123".to_owned(),
            resolve_root: "/checkout".to_owned(),
            commit_hash: Some("deadbeef".to_owned()),
        };
        let new_row = DrawerRecord {
            id: DrawerId::new("loc/room/0002").unwrap(),
            wing: WingId::new("loc").unwrap(),
            room: RoomId::new("room").unwrap(),
            hall: None,
            date: None,
            source_file: "new.rs".to_owned(),
            chunk_index: 0,
            ingest_mode: "mine".to_owned(),
            extract_mode: None,
            added_by: "tester".to_owned(),
            filed_at: time::macros::datetime!(2026-06-01 00:00:00 UTC),
            importance: None,
            emotional_weight: None,
            weight: None,
            content: String::new(),
            content_hash: "newhash".to_owned(),
            embedding: embedding([0.1, 0.2, 0.3, 0.4]),
            locator: Some(locator_with_commit.clone()),
            view_metadata: None,
        };
        store.put_drawers(&[new_row], DuplicateStrategy::Error).await.unwrap();

        let fetched = store
            .get_drawer(&DrawerId::new("loc/room/0002").unwrap())
            .await
            .unwrap()
            .unwrap();
        let loc = fetched.locator.as_ref().unwrap();
        assert_eq!(loc.byte_start, 10);
        assert_eq!(loc.byte_end, 20);
        assert_eq!(loc.line_start, 1);
        assert_eq!(loc.line_end, 2);
        assert_eq!(loc.file_hash, "abc123");
        assert_eq!(loc.resolve_root, "/checkout");
        assert_eq!(loc.commit_hash.as_deref(), Some("deadbeef"));

        // Write another new row with locator but commit_hash None.
        let locator_no_commit = SourceLocator {
            byte_start: 0,
            byte_end: 5,
            line_start: 1,
            line_end: 1,
            file_hash: "fff".to_owned(),
            resolve_root: "/root2".to_owned(),
            commit_hash: None,
        };
        let row_no_commit = DrawerRecord {
            id: DrawerId::new("loc/room/0003").unwrap(),
            wing: WingId::new("loc").unwrap(),
            room: RoomId::new("room").unwrap(),
            hall: None,
            date: None,
            source_file: "other.rs".to_owned(),
            chunk_index: 0,
            ingest_mode: "mine".to_owned(),
            extract_mode: None,
            added_by: "tester".to_owned(),
            filed_at: time::macros::datetime!(2026-06-01 01:00:00 UTC),
            importance: None,
            emotional_weight: None,
            weight: None,
            content: String::new(),
            content_hash: "otherhash".to_owned(),
            embedding: embedding([0.2, 0.3, 0.4, 0.5]),
            locator: Some(locator_no_commit),
            view_metadata: None,
        };
        store.put_drawers(&[row_no_commit], DuplicateStrategy::Error).await.unwrap();

        let fetched2 = store
            .get_drawer(&DrawerId::new("loc/room/0003").unwrap())
            .await
            .unwrap()
            .unwrap();
        let loc2 = fetched2.locator.as_ref().unwrap();
        assert_eq!(loc2.commit_hash, None, "commit_hash should round-trip as None");
        assert_eq!(loc2.file_hash, "fff");
    }

    // ─── Maintenance primitives tests ───────────────────────────────────────

    #[tokio::test]
    async fn vector_index_stats_returns_zero_for_empty_table() {
        let tempdir = tempdir().unwrap();
        let store = LanceDrawerStore::new(tempdir.path().join("lance"), EmbeddingProfile::Balanced);
        store.ensure_schema().await.unwrap();

        let stats = store.vector_index_stats().await.unwrap();
        assert_eq!(stats.total_rows, 0);
        assert_eq!(stats.indexed_rows, 0);
        assert_eq!(stats.tail_rows, 0);
    }

    #[tokio::test]
    async fn vector_index_stats_reports_unindexed_rows_before_optimize() {
        let tempdir = tempdir().unwrap();
        let store = LanceDrawerStore::new(tempdir.path().join("lance"), EmbeddingProfile::Balanced);
        store.ensure_schema().await.unwrap();

        let drawers: Vec<_> = (0..10)
            .map(|i| {
                record(
                    &format!("wing/room/{i:04}"),
                    "wing",
                    "room",
                    "test.txt",
                    [0.1, 0.2, 0.3, 0.4],
                )
            })
            .collect();
        store.put_drawers(&drawers, DuplicateStrategy::Error).await.unwrap();

        let stats = store.vector_index_stats().await.unwrap();
        assert_eq!(stats.total_rows, 10);
        // All rows are unindexed (no vector index built yet).
        assert_eq!(stats.unindexed_rows, 10);
        assert_eq!(stats.tail_rows, 10);
    }

    #[tokio::test]
    async fn fragment_stats_returns_valid_state() {
        let tempdir = tempdir().unwrap();
        let store = LanceDrawerStore::new(tempdir.path().join("lance"), EmbeddingProfile::Balanced);
        store.ensure_schema().await.unwrap();

        let drawer = record("wing/room/0001", "wing", "room", "test.txt", [0.1, 0.2, 0.3, 0.4]);
        store.put_drawers(&[drawer], DuplicateStrategy::Error).await.unwrap();

        let stats = store.fragment_stats().await.unwrap();
        assert_eq!(stats.num_fragments, 1);
        assert!(stats.current_version > 0);
        assert!(stats.num_versions >= 1);
    }

    #[tokio::test]
    async fn optimize_vector_index_skips_below_threshold() {
        let tempdir = tempdir().unwrap();
        let store = LanceDrawerStore::new(tempdir.path().join("lance"), EmbeddingProfile::Balanced);
        store.ensure_schema().await.unwrap();

        let drawer = record("wing/room/0001", "wing", "room", "test.txt", [0.1, 0.2, 0.3, 0.4]);
        store.put_drawers(&[drawer], DuplicateStrategy::Error).await.unwrap();

        // Threshold above the number of unindexed rows → operation is skipped.
        let metrics = store.optimize_vector_index(10_000).await.unwrap();
        assert!(!metrics.ran, "optimize should be skipped when tail is below threshold");
    }

    #[tokio::test]
    async fn optimize_vector_index_runs_when_threshold_exceeded() {
        let tempdir = tempdir().unwrap();
        let store = LanceDrawerStore::new(tempdir.path().join("lance"), EmbeddingProfile::Balanced);
        store.ensure_schema().await.unwrap();

        let drawers: Vec<_> = (0..10)
            .map(|i| {
                record(
                    &format!("wing/room/{i:04}"),
                    "wing",
                    "room",
                    "test.txt",
                    [0.1, 0.2, 0.3, 0.4],
                )
            })
            .collect();
        store.put_drawers(&drawers, DuplicateStrategy::Error).await.unwrap();

        // Threshold of 0 means every row exceeds it → optimise runs.
        let metrics = store.optimize_vector_index(0).await.unwrap();
        assert!(metrics.ran, "optimize should run when tail exceeds threshold");
        // After optimisation the tail should be smaller.
        assert!(
            metrics.tail_rows_after <= metrics.tail_rows_before,
            "tail rows should not increase after optimisation"
        );
    }

    #[tokio::test]
    async fn compact_fragments_reduces_small_fragments() {
        let tempdir = tempdir().unwrap();
        let store = LanceDrawerStore::new(tempdir.path().join("lance"), EmbeddingProfile::Balanced);
        store.ensure_schema().await.unwrap();

        let drawer = record("wing/room/0001", "wing", "room", "test.txt", [0.1, 0.2, 0.3, 0.4]);
        store.put_drawers(&[drawer], DuplicateStrategy::Error).await.unwrap();

        let metrics = store.compact_fragments(0).await.unwrap();
        assert!(metrics.ran, "compaction should run with zero threshold");
        // Fragment count should be stable or reduced.
        assert!(
            metrics.indexed_rows_after <= metrics.indexed_rows_before,
            "fragments should not increase after compaction"
        );
    }

    #[tokio::test]
    async fn compact_fragments_skips_below_threshold() {
        let tempdir = tempdir().unwrap();
        let store = LanceDrawerStore::new(tempdir.path().join("lance"), EmbeddingProfile::Balanced);
        store.ensure_schema().await.unwrap();

        let drawer = record("wing/room/0001", "wing", "room", "test.txt", [0.1, 0.2, 0.3, 0.4]);
        store.put_drawers(&[drawer], DuplicateStrategy::Error).await.unwrap();

        let metrics = store.compact_fragments(100).await.unwrap();
        assert!(!metrics.ran, "compaction should be skipped when below threshold");
    }

    #[tokio::test]
    async fn prune_versions_removes_old_versions() {
        let tempdir = tempdir().unwrap();
        let store = LanceDrawerStore::new(tempdir.path().join("lance"), EmbeddingProfile::Balanced);
        store.ensure_schema().await.unwrap();

        let drawer = record("wing/room/0001", "wing", "room", "test.txt", [0.1, 0.2, 0.3, 0.4]);
        store.put_drawers(&[drawer], DuplicateStrategy::Error).await.unwrap();

        let before = store.fragment_stats().await.unwrap();

        // Prune with a very short retention (0 hours) to exercise the path.
        let metrics = store.prune_versions(0).await.unwrap();
        assert_eq!(
            metrics.versions_before, before.num_versions,
            "versions_before must match pre-prune count"
        );
        // At least the latest version must remain.
        assert!(metrics.versions_after >= 1, "at least one version must survive pruning");
    }

    #[tokio::test]
    async fn prune_versions_never_removes_all_versions() {
        let tempdir = tempdir().unwrap();
        let store = LanceDrawerStore::new(tempdir.path().join("lance"), EmbeddingProfile::Balanced);
        store.ensure_schema().await.unwrap();

        let drawer = record("wing/room/0001", "wing", "room", "test.txt", [0.1, 0.2, 0.3, 0.4]);
        store.put_drawers(&[drawer], DuplicateStrategy::Error).await.unwrap();

        let metrics = store.prune_versions(0).await.unwrap();
        // The latest version must always survive.
        assert!(metrics.versions_after >= 1, "prune must never remove all recoverable history");
    }

    #[tokio::test]
    async fn vector_index_stats_serde_round_trip() {
        let stats = super::VectorIndexStats {
            indexed_rows: 100,
            unindexed_rows: 5,
            tail_rows: 5,
            total_rows: 105,
        };
        let json = serde_json::to_string(&stats).unwrap();
        let deserialized: super::VectorIndexStats = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, stats);
    }

    #[tokio::test]
    async fn fragment_stats_serde_round_trip() {
        let stats = super::FragmentStats {
            num_fragments: 3,
            num_small_fragments: 1,
            total_bytes: 4096,
            num_versions: 5,
            current_version: 5,
        };
        let json = serde_json::to_string(&stats).unwrap();
        let deserialized: super::FragmentStats = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, stats);
    }

    #[tokio::test]
    async fn optimize_metrics_serde_round_trip() {
        let metrics = super::OptimizeMetrics {
            tail_rows_before: 10,
            tail_rows_after: 2,
            indexed_rows_before: 100,
            indexed_rows_after: 108,
            bytes_before: 8192,
            bytes_after: 4096,
            ran: true,
        };
        let json = serde_json::to_string(&metrics).unwrap();
        let deserialized: super::OptimizeMetrics = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, metrics);
    }

    #[tokio::test]
    async fn prune_metrics_serde_round_trip() {
        let metrics = super::PruneMetrics {
            versions_before: 10,
            versions_after: 3,
            versions_removed: 7,
            bytes_freed: 8192,
            total_bytes_before: 16384,
            total_bytes_after: 8192,
        };
        let json = serde_json::to_string(&metrics).unwrap();
        let deserialized: super::PruneMetrics = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, metrics);
    }
}

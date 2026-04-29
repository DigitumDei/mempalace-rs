use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Date32Array, FixedSizeListArray, Float32Array, RecordBatch,
    RecordBatchIterator, StringArray, TimestampMicrosecondArray, UInt32Array, cast::AsArray,
    types::Float32Type,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use futures::TryStreamExt;
use lancedb::connection::Connection;
use lancedb::database::CreateTableMode;
use lancedb::index::Index;
use lancedb::query::{ExecutableQuery, QueryBase};
use time::{Date, OffsetDateTime};

use crate::error::{Result, StorageError};
use crate::types::{DrawerFilter, DrawerMatch, DrawerStore, DuplicateStrategy, SearchRequest};
use mempalace_core::{DrawerId, DrawerRecord, EmbeddingProfile};

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

    async fn existing_ids(&self, ids: &[DrawerId]) -> Result<HashSet<String>> {
        if ids.is_empty() {
            return Ok(HashSet::new());
        }
        let filter = DrawerFilter { ids: ids.to_vec(), ..DrawerFilter::default() };
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
        let table = connection
            .create_empty_table(DRAWERS_TABLE, schema)
            .mode(CreateTableMode::exist_ok(|request| request))
            .execute()
            .await?;
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
        let mut filter = DrawerFilter::default();
        filter.ids.push(id.clone());
        Ok(self.list_drawers(&filter).await?.into_iter().next())
    }

    async fn delete_drawers(&self, ids: &[DrawerId]) -> Result<usize> {
        if ids.is_empty() {
            return Ok(0);
        }
        let table = self.table().await?;
        table.delete(&format!("id IN ({})", quote_ids(ids))).await?;
        Ok(ids.len())
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
            });
        }
    }
    Ok(records)
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
}

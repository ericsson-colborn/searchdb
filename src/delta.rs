use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::json::ArrayWriter;
use bytes::Bytes;
use object_store::path::Path as StorePath;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::error::{Result, SearchDbError};

/// Changes between two Delta table versions: added rows and removed file URIs.
///
/// Used by incremental sync to handle both inserts and deletes.
/// - `added_rows`: rows from newly added Parquet files (to upsert)
/// - `removed_ids`: `_id` values from removed Parquet files (to delete)
#[derive(Debug, Default)]
#[cfg_attr(not(test), allow(dead_code))]
pub struct DeltaChanges {
    pub added_rows: Vec<serde_json::Value>,
    pub removed_ids: Vec<String>,
}

/// Wraps a Delta table and provides load helpers for SearchDB.
///
/// All file reads go through the Delta table's object store, so local,
/// S3, Azure, and GCS URIs all work transparently.
///
/// Sync strategy:
/// - Full load: DataFusion scan of the entire table
/// - Incremental: diff file paths between last_version and HEAD,
///   read only new Parquet files via object_store, upsert their rows.
///   Detect removed files and extract `_id` values for deletion.
///
/// Limitations:
/// - File-level diffing: rewritten files (OPTIMIZE) are re-read, but
///   upsert by _id handles deduplication
/// - If removed files have been vacuumed, a warning is logged and
///   `dsrch reindex` is recommended
pub struct DeltaSync {
    source: String,
}

impl DeltaSync {
    pub fn new(source: &str) -> Self {
        Self {
            source: source.to_string(),
        }
    }

    /// Open the Delta table, optionally at a specific version.
    async fn open(&self, version: Option<i64>) -> Result<deltalake::DeltaTable> {
        let table = match version {
            Some(v) => deltalake::open_table_with_version(&self.source, v).await,
            None => deltalake::open_table(&self.source).await,
        };
        table.map_err(|e| SearchDbError::Delta(format!("failed to open Delta table: {e}")))
    }

    /// Return the current (latest) version of the Delta table.
    pub async fn current_version(&self) -> Result<i64> {
        let table = self.open(None).await?;
        Ok(table.version())
    }

    /// Read all rows from the Delta table, optionally at a specific version.
    ///
    /// Uses DataFusion to scan the table, which handles any storage backend
    /// (local, S3, Azure, GCS) transparently.
    pub async fn full_load(&self, as_of_version: Option<i64>) -> Result<Vec<serde_json::Value>> {
        let table = self.open(as_of_version).await?;
        load_table_rows(table).await
    }

    /// Return the Arrow schema of the Delta table.
    pub async fn arrow_schema(&self) -> Result<arrow::datatypes::Schema> {
        let table = self.open(None).await?;
        let delta_schema = table
            .schema()
            .ok_or_else(|| SearchDbError::Delta("Delta table has no schema".into()))?;
        let arrow_schema: arrow::datatypes::Schema =
            delta_schema
                .try_into()
                .map_err(|e: arrow::error::ArrowError| {
                    SearchDbError::Delta(format!("cannot convert Delta schema to Arrow: {e}"))
                })?;
        Ok(arrow_schema)
    }

    /// Return rows from files added between last_version and HEAD.
    ///
    /// Uses file-level diffing: new paths = current.files - prev.files.
    /// If last_version < 0 (never synced), falls back to full_load().
    pub async fn rows_added_since(&self, last_version: i64) -> Result<Vec<serde_json::Value>> {
        if last_version < 0 {
            return self.full_load(None).await;
        }

        let current = self.open(None).await?;
        let current_files: HashSet<StorePath> = current
            .get_files_iter()
            .map_err(|e| SearchDbError::Delta(format!("failed to get file paths: {e}")))?
            .collect();

        let prev = match self.open(Some(last_version)).await {
            Ok(t) => t,
            Err(_) => {
                log::warn!(
                    "Cannot open Delta v{last_version} (vacuumed?), falling back to full reload"
                );
                return self.full_load(None).await;
            }
        };
        let prev_files: HashSet<StorePath> = prev
            .get_files_iter()
            .map_err(|e| SearchDbError::Delta(format!("failed to get file paths: {e}")))?
            .collect();

        let added: Vec<&StorePath> = current_files.difference(&prev_files).collect();
        if added.is_empty() {
            return Ok(vec![]);
        }

        let store = current.object_store();
        load_parquet_via_store(store.as_ref(), &added).await
    }

    /// Return changes between last_version and HEAD: added rows AND removed IDs.
    ///
    /// Detects both new Parquet files (added rows) and removed Parquet files
    /// (deleted rows). For removed files, attempts to read them and extract
    /// `_id` values for deletion from the index. If a removed file has been
    /// vacuumed (physically deleted), logs a warning — `dsrch reindex` is the
    /// fallback for that edge case.
    ///
    /// If last_version < 0 (never synced), treats all rows as added.
    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn changes_since(&self, last_version: i64) -> Result<DeltaChanges> {
        if last_version < 0 {
            let rows = self.full_load(None).await?;
            return Ok(DeltaChanges {
                added_rows: rows,
                removed_ids: vec![],
            });
        }

        let current = self.open(None).await?;
        let current_files: HashSet<StorePath> = current
            .get_files_iter()
            .map_err(|e| SearchDbError::Delta(format!("failed to get file paths: {e}")))?
            .collect();

        let prev = match self.open(Some(last_version)).await {
            Ok(t) => t,
            Err(_) => {
                log::warn!(
                    "Cannot open Delta v{last_version} (vacuumed?), falling back to full reload"
                );
                let rows = self.full_load(None).await?;
                return Ok(DeltaChanges {
                    added_rows: rows,
                    removed_ids: vec![],
                });
            }
        };
        let prev_files: HashSet<StorePath> = prev
            .get_files_iter()
            .map_err(|e| SearchDbError::Delta(format!("failed to get file paths: {e}")))?
            .collect();

        // Added files: in current but not in prev
        let added: Vec<&StorePath> = current_files.difference(&prev_files).collect();
        let added_rows = if added.is_empty() {
            vec![]
        } else {
            let store = current.object_store();
            load_parquet_via_store(store.as_ref(), &added).await?
        };

        // Removed files: in prev but not in current
        let removed: Vec<&StorePath> = prev_files.difference(&current_files).collect();
        let removed_ids = if removed.is_empty() {
            vec![]
        } else {
            let store = prev.object_store();
            extract_ids_via_store(store.as_ref(), &removed).await
        };

        Ok(DeltaChanges {
            added_rows,
            removed_ids,
        })
    }

    /// Like `changes_since`, but streams added rows through a channel instead of
    /// materializing them all in memory. Returns removed_ids immediately.
    pub async fn changes_since_streaming(
        &self,
        last_version: i64,
        row_tx: crossbeam_channel::Sender<serde_json::Value>,
    ) -> Result<Vec<String>> {
        if last_version < 0 {
            let table = self.open(None).await?;
            let paths: Vec<StorePath> = table
                .get_files_iter()
                .map_err(|e| SearchDbError::Delta(format!("failed to get file paths: {e}")))?
                .collect();
            let store = table.object_store();
            let refs: Vec<&StorePath> = paths.iter().collect();
            stream_parquet_via_store(store.as_ref(), &refs, row_tx).await?;
            return Ok(vec![]);
        }

        let current = self.open(None).await?;
        let current_files: HashSet<StorePath> = current
            .get_files_iter()
            .map_err(|e| SearchDbError::Delta(format!("failed to get file paths: {e}")))?
            .collect();

        let prev = match self.open(Some(last_version)).await {
            Ok(t) => t,
            Err(_) => {
                log::warn!(
                    "Cannot open Delta v{last_version} (vacuumed?), falling back to full reload"
                );
                let paths: Vec<StorePath> = current_files.into_iter().collect();
                let store = current.object_store();
                let refs: Vec<&StorePath> = paths.iter().collect();
                stream_parquet_via_store(store.as_ref(), &refs, row_tx).await?;
                return Ok(vec![]);
            }
        };
        let prev_files: HashSet<StorePath> = prev
            .get_files_iter()
            .map_err(|e| SearchDbError::Delta(format!("failed to get file paths: {e}")))?
            .collect();

        let added: Vec<StorePath> = current_files.difference(&prev_files).cloned().collect();
        let removed: Vec<&StorePath> = prev_files.difference(&current_files).collect();

        // Extract IDs from removed files
        let removed_ids = if removed.is_empty() {
            vec![]
        } else {
            let store = prev.object_store();
            extract_ids_via_store(store.as_ref(), &removed).await
        };

        // Stream added rows through channel
        if !added.is_empty() {
            let store = current.object_store();
            let refs: Vec<&StorePath> = added.iter().collect();
            stream_parquet_via_store(store.as_ref(), &refs, row_tx).await?;
        }
        // row_tx dropped here, closing the channel

        Ok(removed_ids)
    }
}

/// Load all rows from a DeltaTable using DataFusion (works with any storage backend).
async fn load_table_rows(table: deltalake::DeltaTable) -> Result<Vec<serde_json::Value>> {
    use deltalake::datafusion::prelude::SessionContext;

    let ctx = SessionContext::new();
    let df = ctx
        .read_table(Arc::new(table))
        .map_err(|e| SearchDbError::Delta(format!("failed to create scan: {e}")))?;

    let batches: Vec<RecordBatch> = df
        .collect()
        .await
        .map_err(|e| SearchDbError::Delta(format!("failed to collect results: {e}")))?;

    record_batches_to_json(&batches)
}

/// Download a Parquet file from object store and return its bytes.
async fn fetch_parquet_bytes(store: &dyn ObjectStore, path: &StorePath) -> Result<Bytes> {
    let result = store
        .get(path)
        .await
        .map_err(|e| SearchDbError::Delta(format!("failed to read '{}': {e}", path)))?;
    let data = result
        .bytes()
        .await
        .map_err(|e| SearchDbError::Delta(format!("failed to read bytes for '{}': {e}", path)))?;
    Ok(data)
}

/// Read Parquet files via the table's object store and return all rows as JSON.
async fn load_parquet_via_store(
    store: &dyn ObjectStore,
    paths: &[&StorePath],
) -> Result<Vec<serde_json::Value>> {
    let mut all_rows = Vec::new();

    for path in paths {
        let data = fetch_parquet_bytes(store, path).await?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(data)
            .map_err(|e| SearchDbError::Delta(format!("invalid Parquet file '{}': {e}", path)))?
            .build()
            .map_err(|e| {
                SearchDbError::Delta(format!(
                    "failed to build Parquet reader for '{}': {e}",
                    path
                ))
            })?;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| {
                SearchDbError::Delta(format!("error reading batch from '{}': {e}", path))
            })?;
            let rows = record_batches_to_json(&[batch])?;
            all_rows.extend(rows);
        }
    }

    Ok(all_rows)
}

/// Read Parquet files via object store and send each row through the channel.
async fn stream_parquet_via_store(
    store: &dyn ObjectStore,
    paths: &[&StorePath],
    tx: crossbeam_channel::Sender<serde_json::Value>,
) -> Result<()> {
    for path in paths {
        let data = match fetch_parquet_bytes(store, path).await {
            Ok(d) => d,
            Err(e) => {
                eprintln!("[dsrch] warning: cannot read {}: {e}", path);
                continue;
            }
        };

        let reader = ParquetRecordBatchReaderBuilder::try_new(data)
            .map_err(|e| SearchDbError::Delta(format!("invalid Parquet file '{}': {e}", path)))?
            .build()
            .map_err(|e| {
                SearchDbError::Delta(format!(
                    "failed to build Parquet reader for '{}': {e}",
                    path
                ))
            })?;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| {
                SearchDbError::Delta(format!("error reading batch from '{}': {e}", path))
            })?;

            let rows = record_batches_to_json(&[batch])?;
            for row in rows {
                if tx.send(row).is_err() {
                    return Ok(()); // Receiver dropped, stop early
                }
            }
        }
    }
    Ok(())
}

/// Extract `_id` values from Parquet files via object store (for deletion).
///
/// If a file cannot be read (e.g., vacuumed), logs a warning and skips it.
async fn extract_ids_via_store(store: &dyn ObjectStore, paths: &[&StorePath]) -> Vec<String> {
    use arrow::array::{Array, AsArray};

    let mut ids = Vec::new();
    let mut unreadable = 0usize;

    for path in paths {
        let data = match fetch_parquet_bytes(store, path).await {
            Ok(d) => d,
            Err(_) => {
                unreadable += 1;
                log::warn!("Cannot read removed file '{}' (vacuumed?), skipping", path);
                continue;
            }
        };

        let builder = match ParquetRecordBatchReaderBuilder::try_new(data) {
            Ok(b) => b,
            Err(e) => {
                unreadable += 1;
                log::warn!("Invalid Parquet file '{}': {e}", path);
                continue;
            }
        };

        let reader = match builder.build() {
            Ok(r) => r,
            Err(e) => {
                unreadable += 1;
                log::warn!("Failed to build Parquet reader for '{}': {e}", path);
                continue;
            }
        };

        for batch_result in reader {
            let batch = match batch_result {
                Ok(b) => b,
                Err(e) => {
                    log::warn!("Error reading batch from '{}': {e}", path);
                    continue;
                }
            };

            let schema = batch.schema();
            let idx = match schema.index_of("_id") {
                Ok(i) => i,
                Err(_) => continue,
            };

            let col = batch.column(idx);
            if let Some(string_array) = col.as_string_opt::<i32>() {
                for i in 0..string_array.len() {
                    if !string_array.is_null(i) {
                        ids.push(string_array.value(i).to_string());
                    }
                }
            } else if let Some(string_array) = col.as_string_opt::<i64>() {
                for i in 0..string_array.len() {
                    if !string_array.is_null(i) {
                        ids.push(string_array.value(i).to_string());
                    }
                }
            }
        }
    }

    if unreadable > 0 {
        eprintln!(
            "[dsrch] WARNING: {unreadable} removed file(s) could not be read (vacuumed?). \
             Run 'dsrch reindex' for a full rebuild."
        );
    }

    ids
}

/// Convert Arrow RecordBatches to JSON values.
fn record_batches_to_json(batches: &[RecordBatch]) -> Result<Vec<serde_json::Value>> {
    let buf = Vec::new();
    let mut writer = ArrayWriter::new(buf);
    writer
        .write_batches(&batches.iter().collect::<Vec<_>>())
        .map_err(|e| SearchDbError::Delta(format!("error converting batch to JSON: {e}")))?;
    writer
        .finish()
        .map_err(|e| SearchDbError::Delta(format!("error finishing JSON writer: {e}")))?;
    let rows: Vec<serde_json::Value> = serde_json::from_slice(&writer.into_inner())?;
    Ok(rows)
}

/// Strip `file://` or `file:///` prefix from a URI to get a local path.
#[cfg(test)]
fn strip_file_uri(uri: &str) -> String {
    if let Some(rest) = uri.strip_prefix("file://") {
        rest.to_string()
    } else {
        uri.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use deltalake::operations::create::CreateBuilder;
    use deltalake::DeltaOps;

    #[test]
    fn test_strip_file_uri() {
        assert_eq!(
            strip_file_uri("file:///tmp/data/part.parquet"),
            "/tmp/data/part.parquet"
        );
        assert_eq!(
            strip_file_uri("/tmp/data/part.parquet"),
            "/tmp/data/part.parquet"
        );
    }

    fn test_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    fn make_batch(rows: &[(&str, &str, f64)]) -> RecordBatch {
        let schema = test_arrow_schema();
        let ids: Vec<&str> = rows.iter().map(|(id, _, _)| *id).collect();
        let names: Vec<&str> = rows.iter().map(|(_, name, _)| *name).collect();
        let values: Vec<f64> = rows.iter().map(|(_, _, val)| *val).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap()
    }

    async fn create_delta_table(path: &str, rows: &[(&str, &str, f64)]) {
        let schema = test_arrow_schema();
        let batch = make_batch(rows);

        let table = CreateBuilder::new()
            .with_location(path)
            .with_columns(
                deltalake::kernel::StructType::try_from(schema.as_ref())
                    .unwrap()
                    .fields()
                    .cloned(),
            )
            .await
            .unwrap();

        DeltaOps(table).write(vec![batch]).await.unwrap();
    }

    async fn append_to_delta(path: &str, rows: &[(&str, &str, f64)]) {
        let batch = make_batch(rows);
        let table = deltalake::open_table(path).await.unwrap();
        DeltaOps(table).write(vec![batch]).await.unwrap();
    }

    #[tokio::test]
    async fn test_full_load() {
        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();

        create_delta_table(delta_str, &[("d1", "glucose", 100.0), ("d2", "a1c", 5.7)]).await;

        let sync = DeltaSync::new(delta_str);
        let version = sync.current_version().await.unwrap();
        assert!(version >= 0);

        let rows = sync.full_load(None).await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn test_incremental_sync() {
        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();

        create_delta_table(delta_str, &[("d1", "glucose", 100.0)]).await;

        let sync = DeltaSync::new(delta_str);
        let v1 = sync.current_version().await.unwrap();

        append_to_delta(delta_str, &[("d2", "a1c", 5.7)]).await;

        let v2 = sync.current_version().await.unwrap();
        assert!(v2 > v1);

        let rows = sync.rows_added_since(v1).await.unwrap();
        assert!(!rows.is_empty());
        let has_d2 = rows
            .iter()
            .any(|r| r.get("_id").and_then(|v| v.as_str()) == Some("d2"));
        assert!(has_d2, "incremental load should include d2");
    }

    #[tokio::test]
    async fn test_incremental_no_changes() {
        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();

        create_delta_table(delta_str, &[("d1", "glucose", 100.0)]).await;

        let sync = DeltaSync::new(delta_str);
        let v = sync.current_version().await.unwrap();

        // No new writes — incremental should return empty
        let rows = sync.rows_added_since(v).await.unwrap();
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn test_full_load_at_version() {
        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();

        create_delta_table(delta_str, &[("d1", "glucose", 100.0)]).await;
        let sync = DeltaSync::new(delta_str);
        let v1 = sync.current_version().await.unwrap();

        append_to_delta(delta_str, &[("d2", "a1c", 5.7)]).await;

        // Full load at v1 should only have 1 row
        let rows_v1 = sync.full_load(Some(v1)).await.unwrap();
        assert_eq!(rows_v1.len(), 1);

        // Full load at HEAD should have 2
        let rows_head = sync.full_load(None).await.unwrap();
        assert_eq!(rows_head.len(), 2);
    }

    #[tokio::test]
    async fn test_changes_since_append_only() {
        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();

        create_delta_table(delta_str, &[("d1", "glucose", 100.0)]).await;

        let sync = DeltaSync::new(delta_str);
        let v1 = sync.current_version().await.unwrap();

        append_to_delta(delta_str, &[("d2", "a1c", 5.7)]).await;

        let changes = sync.changes_since(v1).await.unwrap();
        assert!(!changes.added_rows.is_empty(), "should have added rows");
        assert!(
            changes.removed_ids.is_empty(),
            "append-only should have no removed IDs"
        );

        let has_d2 = changes
            .added_rows
            .iter()
            .any(|r| r.get("_id").and_then(|v| v.as_str()) == Some("d2"));
        assert!(has_d2, "added rows should include d2");
    }

    #[tokio::test]
    async fn test_changes_since_no_changes() {
        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();

        create_delta_table(delta_str, &[("d1", "glucose", 100.0)]).await;

        let sync = DeltaSync::new(delta_str);
        let v = sync.current_version().await.unwrap();

        let changes = sync.changes_since(v).await.unwrap();
        assert!(changes.added_rows.is_empty());
        assert!(changes.removed_ids.is_empty());
    }

    #[tokio::test]
    async fn test_changes_since_never_synced() {
        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();

        create_delta_table(delta_str, &[("d1", "glucose", 100.0), ("d2", "a1c", 5.7)]).await;

        let sync = DeltaSync::new(delta_str);
        // last_version < 0 means never synced — should return all rows as added
        let changes = sync.changes_since(-1).await.unwrap();
        assert_eq!(changes.added_rows.len(), 2);
        assert!(changes.removed_ids.is_empty());
    }

    #[tokio::test]
    async fn test_changes_since_overwrite_detects_removed_ids() {
        use deltalake::protocol::SaveMode;

        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();

        // Create table with d1 and d2
        create_delta_table(delta_str, &[("d1", "glucose", 100.0), ("d2", "a1c", 5.7)]).await;

        let sync = DeltaSync::new(delta_str);
        let v1 = sync.current_version().await.unwrap();

        // Overwrite with only d2 (removes d1)
        let batch = make_batch(&[("d2", "a1c", 5.7)]);
        let table = deltalake::open_table(delta_str).await.unwrap();
        DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Overwrite)
            .await
            .unwrap();

        let changes = sync.changes_since(v1).await.unwrap();

        // The overwrite creates a new file (added) and removes the old file(s)
        // The removed file should have d1 and d2 as IDs
        let removed_has_d1 = changes.removed_ids.iter().any(|id| id == "d1");
        assert!(
            removed_has_d1,
            "removed_ids should include d1, got: {:?}",
            changes.removed_ids
        );

        // Added rows should have d2 (from the new file)
        let added_has_d2 = changes
            .added_rows
            .iter()
            .any(|r| r.get("_id").and_then(|v| v.as_str()) == Some("d2"));
        assert!(added_has_d2, "added rows should include d2");
    }

    #[tokio::test]
    async fn test_changes_since_delete_detects_removed_ids() {
        use deltalake::datafusion::prelude::{col, lit};

        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();

        // Create table with d1 and d2
        create_delta_table(delta_str, &[("d1", "glucose", 100.0), ("d2", "a1c", 5.7)]).await;

        let sync = DeltaSync::new(delta_str);
        let v1 = sync.current_version().await.unwrap();

        // Delete d1 using Delta delete operation
        let table = deltalake::open_table(delta_str).await.unwrap();
        DeltaOps(table)
            .delete()
            .with_predicate(col("_id").eq(lit("d1")))
            .await
            .unwrap();

        let changes = sync.changes_since(v1).await.unwrap();

        // After delete, the original file is removed and a new file with only d2 is added.
        // The removed file had both d1 and d2.
        let removed_has_d1 = changes.removed_ids.iter().any(|id| id == "d1");
        assert!(
            removed_has_d1,
            "removed_ids should include d1, got: {:?}",
            changes.removed_ids
        );
    }

    #[tokio::test]
    async fn test_two_tier_search_with_delta_gap() {
        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();
        let index_dir = dir.path().join("index_data");
        let index_str = index_dir.to_str().unwrap();

        // Create Delta table with initial data
        create_delta_table(delta_str, &[("d1", "glucose", 100.0)]).await;

        // Connect — full load into tantivy index
        let storage = crate::storage::Storage::new(index_str);
        crate::commands::connect_delta::run(
            &storage,
            "test",
            delta_str,
            Some(r#"{"fields":{"name":"keyword","value":"numeric"}}"#),
            false,
        )
        .await
        .unwrap();

        // Verify initial search works
        let config = storage.load_config("test").unwrap();
        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("test")).unwrap();
        let results = crate::searcher::search_with_gap(
            &index,
            &config.schema,
            "+__present__:__all__",
            10,
            0,
            None,
            false,
            &[],
            None,
        )
        .unwrap();
        assert_eq!(results.len(), 1);

        // Add more rows to Delta (creates a gap)
        append_to_delta(delta_str, &[("d2", "a1c", 5.7)]).await;

        // Read gap rows
        let sync = DeltaSync::new(delta_str);
        let index_version = config.index_version.unwrap_or(-1);
        let gap_rows = sync.rows_added_since(index_version).await.unwrap();
        assert!(!gap_rows.is_empty());

        // Two-tier search should find both docs
        let results = crate::searcher::search_with_gap(
            &index,
            &config.schema,
            "+__present__:__all__",
            10,
            0,
            None,
            false,
            &gap_rows,
            None,
        )
        .unwrap();
        assert_eq!(results.len(), 2);
    }
}

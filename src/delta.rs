use std::collections::HashSet;

use arrow::json::ArrayWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::error::{Result, SearchDbError};

/// Wraps a Delta table and provides load helpers for SearchDB.
///
/// Sync strategy:
/// - Full load: read all Parquet files from the table
/// - Incremental: diff file URIs between last_version and HEAD,
///   read only new Parquet files, upsert their rows
///
/// Limitations:
/// - No delete support — use `reindex` to rebuild when deletes matter
/// - File-level diffing: rewritten files (OPTIMIZE) are re-read, but
///   upsert by _id handles deduplication
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
    pub async fn full_load(&self, as_of_version: Option<i64>) -> Result<Vec<serde_json::Value>> {
        let table = self.open(as_of_version).await?;
        let file_uris: Vec<String> = table
            .get_file_uris()
            .map_err(|e| SearchDbError::Delta(format!("failed to get file URIs: {e}")))?
            .collect();

        read_parquet_files_to_json(&file_uris)
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
    /// Uses file-level diffing: new URIs = current.file_uris() - prev.file_uris().
    /// If last_version < 0 (never synced), falls back to full_load().
    pub async fn rows_added_since(&self, last_version: i64) -> Result<Vec<serde_json::Value>> {
        if last_version < 0 {
            return self.full_load(None).await;
        }

        let current = self.open(None).await?;
        let current_files: HashSet<String> = current
            .get_file_uris()
            .map_err(|e| SearchDbError::Delta(format!("failed to get file URIs: {e}")))?
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
        let prev_files: HashSet<String> = prev
            .get_file_uris()
            .map_err(|e| SearchDbError::Delta(format!("failed to get file URIs: {e}")))?
            .collect();

        let added: Vec<&String> = current_files.difference(&prev_files).collect();
        if added.is_empty() {
            return Ok(vec![]);
        }

        read_parquet_files_to_json(&added)
    }
}

/// Read Parquet files and convert rows to JSON values.
///
/// Handles `file://` URI prefix stripping for local paths.
fn read_parquet_files_to_json(uris: &[impl AsRef<str>]) -> Result<Vec<serde_json::Value>> {
    let mut all_rows = Vec::new();

    for uri in uris {
        let path = strip_file_uri(uri.as_ref());
        let file = std::fs::File::open(&path)
            .map_err(|e| SearchDbError::Delta(format!("cannot open Parquet file '{path}': {e}")))?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| SearchDbError::Delta(format!("invalid Parquet file '{path}': {e}")))?;
        let reader = builder.build().map_err(|e| {
            SearchDbError::Delta(format!("failed to build Parquet reader for '{path}': {e}"))
        })?;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| {
                SearchDbError::Delta(format!("error reading batch from '{path}': {e}"))
            })?;

            let buf = Vec::new();
            let mut writer = ArrayWriter::new(buf);
            writer.write_batches(&[&batch]).map_err(|e| {
                SearchDbError::Delta(format!("error converting batch to JSON: {e}"))
            })?;
            writer
                .finish()
                .map_err(|e| SearchDbError::Delta(format!("error finishing JSON writer: {e}")))?;

            let rows: Vec<serde_json::Value> = serde_json::from_slice(&writer.into_inner())?;
            all_rows.extend(rows);
        }
    }

    Ok(all_rows)
}

/// Strip `file://` or `file:///` prefix from a URI to get a local path.
fn strip_file_uri(uri: &str) -> String {
    if let Some(rest) = uri.strip_prefix("file://") {
        // On Unix, file:///foo → /foo (strip first two slashes of file://, path starts with /)
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
    use std::sync::Arc;

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
        )
        .unwrap();
        assert_eq!(results.len(), 2);
    }
}

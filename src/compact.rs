use std::time::Instant;

use tantivy::index::SegmentId;
use tantivy::merge_policy::NoMergePolicy;
use tantivy::Index;

use crate::delta::DeltaSync;
use crate::error::{Result, SearchDbError};
use crate::storage::{CompactMeta, IndexConfig, Storage};
use crate::writer;

/// Configuration for the compact worker, populated from CLI flags.
#[derive(Debug, Clone)]
pub struct CompactOptions {
    pub segment_size: usize,
    pub merge_interval_secs: u64,
    pub max_segments: usize,
    pub poll_interval_secs: u64,
    /// Not yet used — reserved for time-pressure commit feature.
    #[allow(dead_code)]
    pub max_segment_age_secs: u64,
    pub force_merge: bool,
    pub once: bool,
}

impl Default for CompactOptions {
    fn default() -> Self {
        Self {
            segment_size: 10_000,
            merge_interval_secs: 300,
            max_segments: 10,
            poll_interval_secs: 10,
            max_segment_age_secs: 60,
            force_merge: false,
            once: false,
        }
    }
}

/// The compact worker: polls Delta, creates segments (L1), merges them (L2).
pub struct CompactWorker<'a> {
    storage: &'a Storage,
    name: String,
    opts: CompactOptions,
}

impl<'a> CompactWorker<'a> {
    pub fn new(storage: &'a Storage, name: &str, opts: CompactOptions) -> Self {
        Self {
            storage,
            name: name.to_string(),
            opts,
        }
    }

    /// Run the compaction loop. Returns when:
    /// - `--once` mode: after one poll+segment+merge cycle
    /// - `--force-merge` mode: after merging all segments
    /// - Signal received (SIGINT/SIGTERM)
    pub async fn run(&self, shutdown: tokio::sync::watch::Receiver<bool>) -> Result<()> {
        let config = self.storage.load_config(&self.name)?;
        let source = config.delta_source.as_deref().ok_or_else(|| {
            SearchDbError::Delta(format!(
                "index '{}' has no Delta source — use connect-delta first",
                self.name
            ))
        })?;

        let tantivy_schema = config.schema.build_tantivy_schema();
        let index = Index::open_in_dir(self.storage.tantivy_dir(&self.name))?;
        let mut index_writer = match index.writer(50_000_000) {
            Ok(w) => w,
            Err(tantivy::TantivyError::LockFailure(_, _)) => {
                return Err(SearchDbError::WriterLocked(self.name.clone()));
            }
            Err(e) => return Err(SearchDbError::Tantivy(e)),
        };

        // Disable automatic merges — we control merges explicitly in Level 2
        index_writer.set_merge_policy(Box::new(NoMergePolicy));

        let delta = DeltaSync::new(source);
        let id_field = tantivy_schema
            .get_field("_id")
            .map_err(|_| SearchDbError::Schema("missing _id field".into()))?;

        // Handle --force-merge: merge all segments and exit
        if self.opts.force_merge {
            eprintln!("[searchdb] compact: force-merging all segments...");
            self.force_merge_all(&index, &mut index_writer).await?;
        } else {
            let mut last_merge_check = Instant::now();

            // Main loop
            loop {
                if *shutdown.borrow() {
                    eprintln!("[searchdb] compact: shutdown signal received, finishing...");
                    break;
                }

                // Level 1: Poll and segment
                let segmented = self
                    .poll_and_segment(
                        &delta,
                        &tantivy_schema,
                        &config,
                        &mut index_writer,
                        id_field,
                    )
                    .await?;

                if !segmented {
                    let current_config = self.storage.load_config(&self.name)?;
                    let index_version = current_config.index_version.unwrap_or(-1);
                    eprintln!("[searchdb] compact: up to date at Delta v{index_version}");
                }

                // Level 2: Check for merge opportunity
                let merge_elapsed = last_merge_check.elapsed().as_secs();
                let should_merge = self.opts.once || merge_elapsed >= self.opts.merge_interval_secs;

                if should_merge {
                    last_merge_check = Instant::now();
                    self.maybe_merge(&index, &mut index_writer).await?;
                }

                // Exit if --once mode
                if self.opts.once {
                    break;
                }

                // Sleep until next poll
                self.sleep_or_shutdown(&shutdown).await;
            }
        }

        // Clean shutdown — consumes the writer
        index_writer.wait_merging_threads()?;
        eprintln!("[searchdb] compact: shutdown complete");
        Ok(())
    }

    /// Level 1: Poll Delta for new rows and create segments.
    /// Returns true if any rows were indexed.
    async fn poll_and_segment(
        &self,
        delta: &DeltaSync,
        tantivy_schema: &tantivy::schema::Schema,
        initial_config: &IndexConfig,
        index_writer: &mut tantivy::IndexWriter,
        id_field: tantivy::schema::Field,
    ) -> Result<bool> {
        let mut current_config = self.storage.load_config(&self.name)?;
        let index_version = current_config.index_version.unwrap_or(-1);

        let current_version = delta.current_version().await?;

        if current_version <= index_version {
            return Ok(false);
        }

        let gap = current_version - index_version;
        eprintln!(
            "[searchdb] compact: polling Delta... HEAD={current_version}, \
             index={index_version}, gap={gap} versions"
        );

        let rows = delta.rows_added_since(index_version).await?;

        if rows.is_empty() {
            eprintln!("[searchdb] compact: no new rows to index");
            current_config.index_version = Some(current_version);
            self.save_config_with_compact(&current_config)?;
            return Ok(false);
        }

        eprintln!(
            "[searchdb] compact: read {} rows from Delta v{}..v{}",
            rows.len(),
            index_version,
            current_version
        );

        // Split into batches of segment_size
        let batches: Vec<&[serde_json::Value]> = rows.chunks(self.opts.segment_size).collect();
        let num_batches = batches.len();

        for (i, batch) in batches.into_iter().enumerate() {
            for row in batch {
                let doc_id = writer::make_doc_id(row);
                let doc =
                    writer::build_document(tantivy_schema, &initial_config.schema, row, &doc_id)?;
                writer::upsert_document(index_writer, id_field, doc, &doc_id);
            }

            index_writer.commit()?;

            eprintln!(
                "[searchdb] compact: committed segment {}/{} ({} docs)",
                i + 1,
                num_batches,
                batch.len()
            );
        }

        // Update watermark AFTER all segments committed (crash safety)
        current_config.index_version = Some(current_version);
        self.save_compact_meta(&mut current_config, true, false)?;
        self.storage.save_config(&self.name, &current_config)?;

        eprintln!("[searchdb] compact: now at Delta v{current_version}");
        Ok(true)
    }

    /// Level 2: Check segment count and merge if above threshold.
    async fn maybe_merge(
        &self,
        index: &Index,
        index_writer: &mut tantivy::IndexWriter,
    ) -> Result<()> {
        let segment_ids: Vec<SegmentId> = index
            .searchable_segment_ids()
            .map_err(SearchDbError::Tantivy)?;

        let segment_count = segment_ids.len();

        eprintln!(
            "[searchdb] compact: merge check: {segment_count} segments, \
             threshold={}",
            self.opts.max_segments
        );

        if segment_count <= self.opts.max_segments {
            return Ok(());
        }

        if segment_ids.len() <= 1 {
            return Ok(());
        }

        eprintln!(
            "[searchdb] compact: merging {} segments...",
            segment_ids.len()
        );

        let merge_future = index_writer.merge(&segment_ids);
        match merge_future.await {
            Ok(_meta) => {
                eprintln!(
                    "[searchdb] compact: merged {} segments into 1",
                    segment_ids.len()
                );
            }
            Err(e) => {
                eprintln!("[searchdb] compact: merge failed: {e}");
            }
        }

        // Garbage collect old segment files
        let _ = index_writer.garbage_collect_files().await;

        // Update compaction metadata
        let mut config = self.storage.load_config(&self.name)?;
        self.save_compact_meta(&mut config, false, true)?;
        self.storage.save_config(&self.name, &config)?;

        Ok(())
    }

    /// Force-merge all segments into one, then exit.
    async fn force_merge_all(
        &self,
        index: &Index,
        index_writer: &mut tantivy::IndexWriter,
    ) -> Result<()> {
        let segment_ids: Vec<SegmentId> = index
            .searchable_segment_ids()
            .map_err(SearchDbError::Tantivy)?;

        if segment_ids.len() <= 1 {
            eprintln!(
                "[searchdb] compact: already {} segment(s), nothing to merge",
                segment_ids.len()
            );
            return Ok(());
        }

        eprintln!(
            "[searchdb] compact: force-merging {} segments into 1...",
            segment_ids.len()
        );

        let merge_future = index_writer.merge(&segment_ids);
        match merge_future.await {
            Ok(_) => {
                eprintln!("[searchdb] compact: force-merge complete");
            }
            Err(e) => {
                return Err(SearchDbError::Tantivy(e));
            }
        }

        let _ = index_writer.garbage_collect_files().await;

        // Update metadata
        let mut config = self.storage.load_config(&self.name)?;
        self.save_compact_meta(&mut config, false, true)?;
        self.storage.save_config(&self.name, &config)?;

        Ok(())
    }

    /// Update the CompactMeta in the config with current timestamps.
    fn save_compact_meta(
        &self,
        config: &mut IndexConfig,
        did_segment: bool,
        did_merge: bool,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let meta = config.compact.get_or_insert(CompactMeta {
            segment_size: self.opts.segment_size,
            merge_interval_secs: self.opts.merge_interval_secs,
            max_segments: self.opts.max_segments,
            last_segment_at: None,
            last_merge_at: None,
        });
        meta.segment_size = self.opts.segment_size;
        meta.merge_interval_secs = self.opts.merge_interval_secs;
        meta.max_segments = self.opts.max_segments;
        if did_segment {
            meta.last_segment_at = Some(now.clone());
        }
        if did_merge {
            meta.last_merge_at = Some(now);
        }
        Ok(())
    }

    /// Helper to save config with compact metadata without segment/merge timestamps.
    fn save_config_with_compact(&self, config: &IndexConfig) -> Result<()> {
        let mut config = config.clone();
        self.save_compact_meta(&mut config, false, false)?;
        self.storage.save_config(&self.name, &config)
    }

    /// Sleep for poll_interval, returning early if shutdown is signaled.
    async fn sleep_or_shutdown(&self, shutdown: &tokio::sync::watch::Receiver<bool>) {
        let poll_duration = tokio::time::Duration::from_secs(self.opts.poll_interval_secs);
        let mut shutdown = shutdown.clone();
        tokio::select! {
            _ = tokio::time::sleep(poll_duration) => {}
            _ = shutdown.changed() => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compact_options_defaults() {
        let opts = CompactOptions::default();
        assert_eq!(opts.segment_size, 10_000);
        assert_eq!(opts.merge_interval_secs, 300);
        assert_eq!(opts.max_segments, 10);
        assert_eq!(opts.poll_interval_secs, 10);
        assert_eq!(opts.max_segment_age_secs, 60);
        assert!(!opts.force_merge);
        assert!(!opts.once);
    }

    // --- Delta integration tests below ---

    use arrow::array::{Float64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use deltalake::operations::create::CreateBuilder;
    use deltalake::DeltaOps;
    use std::sync::Arc;

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
    async fn test_compact_once_creates_segments() {
        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();
        let data_dir = dir.path().join("searchdb_data");
        let data_str = data_dir.to_str().unwrap();

        create_delta_table(delta_str, &[("d1", "glucose", 100.0), ("d2", "a1c", 5.7)]).await;

        let storage = crate::storage::Storage::new(data_str);
        crate::commands::connect_delta::run(
            &storage,
            "lab",
            delta_str,
            r#"{"fields":{"name":"keyword","value":"numeric"}}"#,
        )
        .await
        .unwrap();

        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("lab")).unwrap();
        let reader = index.reader().unwrap();
        assert_eq!(reader.searcher().num_docs(), 2);

        append_to_delta(
            delta_str,
            &[
                ("d3", "creatinine", 1.2),
                ("d4", "bun", 15.0),
                ("d5", "sodium", 140.0),
            ],
        )
        .await;

        let opts = CompactOptions {
            segment_size: 2,
            once: true,
            ..CompactOptions::default()
        };
        let worker = CompactWorker::new(&storage, "lab", opts);
        let (_tx, rx) = tokio::sync::watch::channel(false);
        worker.run(rx).await.unwrap();

        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("lab")).unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 5, "should have 5 total documents");

        let segment_count = searcher.segment_readers().len();
        assert!(
            segment_count >= 2,
            "should have at least 2 segments, got {segment_count}"
        );

        let config = storage.load_config("lab").unwrap();
        assert!(config.compact.is_some(), "compact metadata should be saved");
    }

    #[tokio::test]
    async fn test_compact_force_merge_reduces_segments() {
        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();
        let data_dir = dir.path().join("searchdb_data");
        let data_str = data_dir.to_str().unwrap();

        create_delta_table(delta_str, &[("d1", "glucose", 100.0)]).await;

        let storage = crate::storage::Storage::new(data_str);
        crate::commands::connect_delta::run(
            &storage,
            "lab",
            delta_str,
            r#"{"fields":{"name":"keyword","value":"numeric"}}"#,
        )
        .await
        .unwrap();

        for i in 2..=6 {
            append_to_delta(delta_str, &[(&format!("d{i}"), "test", i as f64)]).await;

            let opts = CompactOptions {
                segment_size: 1,
                once: true,
                ..CompactOptions::default()
            };
            let worker = CompactWorker::new(&storage, "lab", opts);
            let (_tx, rx) = tokio::sync::watch::channel(false);
            worker.run(rx).await.unwrap();
        }

        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("lab")).unwrap();
        let reader = index.reader().unwrap();
        let before_merge = reader.searcher().segment_readers().len();
        assert!(
            before_merge > 1,
            "should have multiple segments before force-merge, got {before_merge}"
        );

        let opts = CompactOptions {
            force_merge: true,
            ..CompactOptions::default()
        };
        let worker = CompactWorker::new(&storage, "lab", opts);
        let (_tx, rx) = tokio::sync::watch::channel(false);
        worker.run(rx).await.unwrap();

        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("lab")).unwrap();
        let reader = index.reader().unwrap();
        let after_merge = reader.searcher().segment_readers().len();
        assert_eq!(after_merge, 1, "should have 1 segment after force-merge");
        assert_eq!(reader.searcher().num_docs(), 6);
    }

    #[tokio::test]
    async fn test_compact_upsert_dedup() {
        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();
        let data_dir = dir.path().join("searchdb_data");
        let data_str = data_dir.to_str().unwrap();

        create_delta_table(delta_str, &[("d1", "glucose", 100.0)]).await;

        let storage = crate::storage::Storage::new(data_str);
        crate::commands::connect_delta::run(
            &storage,
            "lab",
            delta_str,
            r#"{"fields":{"name":"keyword","value":"numeric"}}"#,
        )
        .await
        .unwrap();

        append_to_delta(delta_str, &[("d1", "glucose_updated", 200.0)]).await;

        let opts = CompactOptions {
            once: true,
            ..CompactOptions::default()
        };
        let worker = CompactWorker::new(&storage, "lab", opts);
        let (_tx, rx) = tokio::sync::watch::channel(false);
        worker.run(rx).await.unwrap();

        let opts = CompactOptions {
            force_merge: true,
            ..CompactOptions::default()
        };
        let worker = CompactWorker::new(&storage, "lab", opts);
        let (_tx, rx) = tokio::sync::watch::channel(false);
        worker.run(rx).await.unwrap();

        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("lab")).unwrap();
        let reader = index.reader().unwrap();
        assert_eq!(
            reader.searcher().num_docs(),
            1,
            "upsert should keep only latest version"
        );
    }

    #[tokio::test]
    async fn test_compact_writer_lock_detection() {
        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();
        let data_dir = dir.path().join("searchdb_data");
        let data_str = data_dir.to_str().unwrap();

        create_delta_table(delta_str, &[("d1", "glucose", 100.0)]).await;

        let storage = crate::storage::Storage::new(data_str);
        crate::commands::connect_delta::run(
            &storage,
            "lab",
            delta_str,
            r#"{"fields":{"name":"keyword","value":"numeric"}}"#,
        )
        .await
        .unwrap();

        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("lab")).unwrap();
        let _writer: tantivy::IndexWriter<tantivy::TantivyDocument> =
            index.writer(50_000_000).unwrap();

        let opts = CompactOptions {
            once: true,
            ..CompactOptions::default()
        };
        let worker = CompactWorker::new(&storage, "lab", opts);
        let (_tx, rx) = tokio::sync::watch::channel(false);
        let result = worker.run(rx).await;
        assert!(result.is_err());

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Another writer"),
            "error should mention writer lock: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_compact_shutdown_signal() {
        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();
        let data_dir = dir.path().join("searchdb_data");
        let data_str = data_dir.to_str().unwrap();

        create_delta_table(delta_str, &[("d1", "glucose", 100.0)]).await;

        let storage = crate::storage::Storage::new(data_str);
        crate::commands::connect_delta::run(
            &storage,
            "lab",
            delta_str,
            r#"{"fields":{"name":"keyword","value":"numeric"}}"#,
        )
        .await
        .unwrap();

        // Run compact in continuous mode but immediately signal shutdown
        let opts = CompactOptions {
            poll_interval_secs: 1,
            ..CompactOptions::default()
        };
        let worker = CompactWorker::new(&storage, "lab", opts);
        let (tx, rx) = tokio::sync::watch::channel(false);

        // Send shutdown signal immediately
        tx.send(true).unwrap();

        // Worker should exit quickly after seeing the signal
        let result =
            tokio::time::timeout(tokio::time::Duration::from_secs(5), worker.run(rx)).await;

        assert!(
            result.is_ok(),
            "worker should exit within 5 seconds of shutdown signal"
        );
        assert!(
            result.unwrap().is_ok(),
            "worker should exit cleanly on shutdown"
        );
    }
}

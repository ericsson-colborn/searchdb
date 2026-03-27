use std::time::Instant;

use tantivy::index::SegmentId;
use tantivy::merge_policy::NoMergePolicy;
use tantivy::Index;

use crate::delta::DeltaSync;
use crate::error::{Result, SearchDbError};
use crate::merge_policy::{SegmentMeta, StableLogMergePolicy, StableLogMergePolicyConfig};
use crate::pipeline::{self, PipelineConfig};
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
            eprintln!("[dsrch] compact: force-merging all segments...");
            self.force_merge_all(&index, &mut index_writer).await?;
        } else {
            let mut last_merge_check = Instant::now();

            // Main loop
            loop {
                if *shutdown.borrow() {
                    eprintln!("[dsrch] compact: shutdown signal received, finishing...");
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
                    eprintln!("[dsrch] compact: up to date at Delta v{index_version}");
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
        eprintln!("[dsrch] compact: shutdown complete");
        Ok(())
    }

    /// Level 1: Poll Delta for new rows and create segments.
    /// Returns true if any rows were indexed or deleted.
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
            "[dsrch] compact: polling Delta... HEAD={current_version}, \
             index={index_version}, gap={gap} versions"
        );

        // Create channel for streaming rows from Delta
        let (row_tx, row_rx) = crossbeam_channel::bounded(100);

        // Get removed IDs and stream added rows into channel
        let removed_ids = delta
            .changes_since_streaming(index_version, row_tx)
            .await?;

        // Process deletions first (before upserts, so re-added rows win)
        if !removed_ids.is_empty() {
            let del_count = writer::delete_documents(index_writer, id_field, &removed_ids);
            index_writer.commit()?;
            eprintln!("[dsrch] compact: deleted {del_count} document(s) from removed Delta files");
        }

        // Stream rows through pipeline — commit per segment_size batch
        let pipeline_config = PipelineConfig::default();
        let segment_size = self.opts.segment_size;
        let mut total_docs = 0u64;
        let mut batch_num = 0usize;

        let mut batch = Vec::with_capacity(segment_size);
        for row in row_rx {
            batch.push(row);
            if batch.len() >= segment_size {
                batch_num += 1;
                let batch_len = batch.len();
                let stats = pipeline::run_pipeline(
                    std::mem::take(&mut batch).into_iter(),
                    tantivy_schema,
                    &initial_config.schema,
                    index_writer,
                    pipeline_config.clone(),
                )?;
                index_writer.commit()?;
                total_docs += stats.docs_indexed;
                eprintln!(
                    "[dsrch] compact: committed segment {batch_num} ({batch_len} docs)"
                );
                batch = Vec::with_capacity(segment_size);
            }
        }

        // Final partial batch
        if !batch.is_empty() {
            batch_num += 1;
            let batch_len = batch.len();
            let stats = pipeline::run_pipeline(
                batch.into_iter(),
                tantivy_schema,
                &initial_config.schema,
                index_writer,
                pipeline_config.clone(),
            )?;
            index_writer.commit()?;
            total_docs += stats.docs_indexed;
            eprintln!(
                "[dsrch] compact: committed segment {batch_num} ({batch_len} docs)"
            );
        }

        if total_docs == 0 && removed_ids.is_empty() {
            eprintln!("[dsrch] compact: no changes to process");
            current_config.index_version = Some(current_version);
            self.save_config_with_compact(&current_config)?;
            return Ok(false);
        }

        // Update watermark AFTER all segments committed (crash safety)
        current_config.index_version = Some(current_version);
        self.save_compact_meta(&mut current_config, true, false)?;
        self.storage.save_config(&self.name, &current_config)?;

        eprintln!(
            "[dsrch] compact: indexed {total_docs} docs in {batch_num} segment(s), now at Delta v{current_version}"
        );
        Ok(total_docs > 0 || !removed_ids.is_empty())
    }

    /// Level 2: Use StableLogMergePolicy to decide which segments to merge.
    async fn maybe_merge(
        &self,
        index: &Index,
        index_writer: &mut tantivy::IndexWriter,
    ) -> Result<()> {
        let segment_metas = self.read_segment_metas(index)?;
        let segment_count = segment_metas.len();

        eprintln!("[dsrch] compact: merge check: {segment_count} segments");

        if segment_count <= 1 {
            return Ok(());
        }

        let policy = self.build_merge_policy();
        let merge_ops = policy.operations(&segment_metas);

        if merge_ops.is_empty() {
            eprintln!("[dsrch] compact: no merge needed");
            return Ok(());
        }

        let mut did_merge = false;
        for (i, group) in merge_ops.iter().enumerate() {
            let ids = self.resolve_segment_ids(index, group)?;
            if ids.len() <= 1 {
                continue;
            }

            eprintln!(
                "[dsrch] compact: merge operation {}/{}: merging {} segments...",
                i + 1,
                merge_ops.len(),
                ids.len()
            );

            match index_writer.merge(&ids).await {
                Ok(_) => {
                    eprintln!("[dsrch] compact: merged {} segments into 1", ids.len());
                    did_merge = true;
                }
                Err(e) => {
                    eprintln!("[dsrch] compact: merge failed: {e}");
                }
            }
        }

        if did_merge {
            // Garbage collect old segment files
            let _ = index_writer.garbage_collect_files().await;

            // Update compaction metadata
            let mut config = self.storage.load_config(&self.name)?;
            self.save_compact_meta(&mut config, false, true)?;
            self.storage.save_config(&self.name, &config)?;
        }

        Ok(())
    }

    /// Build a StableLogMergePolicy from CompactOptions.
    fn build_merge_policy(&self) -> StableLogMergePolicy {
        let config = StableLogMergePolicyConfig {
            // Use max_segments as the merge factor: trigger merge when
            // a level accumulates this many segments.
            merge_factor: self.opts.max_segments,
            max_merge_factor: self.opts.max_segments + 2,
            ..StableLogMergePolicyConfig::default()
        };
        StableLogMergePolicy::new(config)
    }

    /// Read segment metadata from the tantivy index into our policy-friendly type.
    fn read_segment_metas(&self, index: &Index) -> Result<Vec<SegmentMeta>> {
        let reader_metas = index
            .searchable_segment_metas()
            .map_err(SearchDbError::Tantivy)?;
        let metas = reader_metas
            .iter()
            .map(|meta| SegmentMeta {
                segment_id: meta.id().uuid_string(),
                num_docs: meta.num_docs() as usize,
                num_merge_ops: 0,
            })
            .collect();
        Ok(metas)
    }

    /// Resolve our SegmentMeta segment_ids back to tantivy SegmentIds.
    fn resolve_segment_ids(&self, index: &Index, group: &[SegmentMeta]) -> Result<Vec<SegmentId>> {
        let all_metas = index
            .searchable_segment_metas()
            .map_err(SearchDbError::Tantivy)?;

        let target_ids: std::collections::HashSet<&str> =
            group.iter().map(|s| s.segment_id.as_str()).collect();

        let ids: Vec<SegmentId> = all_metas
            .iter()
            .filter(|meta| target_ids.contains(meta.id().uuid_string().as_str()))
            .map(|meta| meta.id())
            .collect();
        Ok(ids)
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
                "[dsrch] compact: already {} segment(s), nothing to merge",
                segment_ids.len()
            );
            return Ok(());
        }

        eprintln!(
            "[dsrch] compact: force-merging {} segments into 1...",
            segment_ids.len()
        );

        let merge_future = index_writer.merge(&segment_ids);
        match merge_future.await {
            Ok(_) => {
                eprintln!("[dsrch] compact: force-merge complete");
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
        let data_dir = dir.path().join("dsrch_data");
        let data_str = data_dir.to_str().unwrap();

        create_delta_table(delta_str, &[("d1", "glucose", 100.0), ("d2", "a1c", 5.7)]).await;

        let storage = crate::storage::Storage::new(data_str);
        crate::commands::connect_delta::run(
            &storage,
            "lab",
            delta_str,
            Some(r#"{"fields":{"name":"keyword","value":"numeric"}}"#),
            false,
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
        let data_dir = dir.path().join("dsrch_data");
        let data_str = data_dir.to_str().unwrap();

        create_delta_table(delta_str, &[("d1", "glucose", 100.0)]).await;

        let storage = crate::storage::Storage::new(data_str);
        crate::commands::connect_delta::run(
            &storage,
            "lab",
            delta_str,
            Some(r#"{"fields":{"name":"keyword","value":"numeric"}}"#),
            false,
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
        let data_dir = dir.path().join("dsrch_data");
        let data_str = data_dir.to_str().unwrap();

        create_delta_table(delta_str, &[("d1", "glucose", 100.0)]).await;

        let storage = crate::storage::Storage::new(data_str);
        crate::commands::connect_delta::run(
            &storage,
            "lab",
            delta_str,
            Some(r#"{"fields":{"name":"keyword","value":"numeric"}}"#),
            false,
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
        let data_dir = dir.path().join("dsrch_data");
        let data_str = data_dir.to_str().unwrap();

        create_delta_table(delta_str, &[("d1", "glucose", 100.0)]).await;

        let storage = crate::storage::Storage::new(data_str);
        crate::commands::connect_delta::run(
            &storage,
            "lab",
            delta_str,
            Some(r#"{"fields":{"name":"keyword","value":"numeric"}}"#),
            false,
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
        let data_dir = dir.path().join("dsrch_data");
        let data_str = data_dir.to_str().unwrap();

        create_delta_table(delta_str, &[("d1", "glucose", 100.0)]).await;

        let storage = crate::storage::Storage::new(data_str);
        crate::commands::connect_delta::run(
            &storage,
            "lab",
            delta_str,
            Some(r#"{"fields":{"name":"keyword","value":"numeric"}}"#),
            false,
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

    #[tokio::test]
    async fn test_compact_handles_delta_deletes() {
        use deltalake::datafusion::prelude::{col, lit};

        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();
        let data_dir = dir.path().join("dsrch_data");
        let data_str = data_dir.to_str().unwrap();

        // Create Delta table with 3 rows
        create_delta_table(
            delta_str,
            &[
                ("d1", "glucose", 100.0),
                ("d2", "a1c", 5.7),
                ("d3", "creatinine", 1.2),
            ],
        )
        .await;

        // Connect and initial load
        let storage = crate::storage::Storage::new(data_str);
        crate::commands::connect_delta::run(
            &storage,
            "lab",
            delta_str,
            Some(r#"{"fields":{"name":"keyword","value":"numeric"}}"#),
            false,
        )
        .await
        .unwrap();

        // Verify all 3 docs are indexed
        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("lab")).unwrap();
        let reader = index.reader().unwrap();
        assert_eq!(reader.searcher().num_docs(), 3);

        // Delete d1 from Delta
        let table = deltalake::open_table(delta_str).await.unwrap();
        DeltaOps(table)
            .delete()
            .with_predicate(col("_id").eq(lit("d1")))
            .await
            .unwrap();

        // Run compact --once to pick up the delete
        let opts = CompactOptions {
            once: true,
            ..CompactOptions::default()
        };
        let worker = CompactWorker::new(&storage, "lab", opts);
        let (_tx, rx) = tokio::sync::watch::channel(false);
        worker.run(rx).await.unwrap();

        // Force merge to consolidate segments and surface actual doc count
        let opts = CompactOptions {
            force_merge: true,
            ..CompactOptions::default()
        };
        let worker = CompactWorker::new(&storage, "lab", opts);
        let (_tx, rx) = tokio::sync::watch::channel(false);
        worker.run(rx).await.unwrap();

        // Verify d1 is gone — only d2 and d3 remain
        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("lab")).unwrap();
        let reader = index.reader().unwrap();
        assert_eq!(
            reader.searcher().num_docs(),
            2,
            "d1 should be deleted, leaving 2 docs"
        );
    }

    #[tokio::test]
    async fn test_compact_handles_overwrite_deletes() {
        use deltalake::protocol::SaveMode;

        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();
        let data_dir = dir.path().join("dsrch_data");
        let data_str = data_dir.to_str().unwrap();

        // Create Delta table with 2 rows
        create_delta_table(delta_str, &[("d1", "glucose", 100.0), ("d2", "a1c", 5.7)]).await;

        // Connect and initial load
        let storage = crate::storage::Storage::new(data_str);
        crate::commands::connect_delta::run(
            &storage,
            "lab",
            delta_str,
            Some(r#"{"fields":{"name":"keyword","value":"numeric"}}"#),
            false,
        )
        .await
        .unwrap();

        // Verify 2 docs indexed
        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("lab")).unwrap();
        let reader = index.reader().unwrap();
        assert_eq!(reader.searcher().num_docs(), 2);

        // Overwrite Delta with only d2 (d1 is removed)
        let batch = make_batch(&[("d2", "a1c", 5.7)]);
        let table = deltalake::open_table(delta_str).await.unwrap();
        DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Overwrite)
            .await
            .unwrap();

        // Run compact --once
        let opts = CompactOptions {
            once: true,
            ..CompactOptions::default()
        };
        let worker = CompactWorker::new(&storage, "lab", opts);
        let (_tx, rx) = tokio::sync::watch::channel(false);
        worker.run(rx).await.unwrap();

        // Force merge
        let opts = CompactOptions {
            force_merge: true,
            ..CompactOptions::default()
        };
        let worker = CompactWorker::new(&storage, "lab", opts);
        let (_tx, rx) = tokio::sync::watch::channel(false);
        worker.run(rx).await.unwrap();

        // Verify d1 is removed — only d2 remains
        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("lab")).unwrap();
        let reader = index.reader().unwrap();
        assert_eq!(
            reader.searcher().num_docs(),
            1,
            "d1 should be deleted via overwrite, leaving 1 doc"
        );
    }
}

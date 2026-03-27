//! Parallel indexing pipeline: bounded channels between stages.
//!
//! Architecture (adapted from tantivy-cli):
//! ```text
//! Stage 1: Row producer (caller's iterator)
//!   → bounded channel (capacity: CHANNEL_CAP)
//! Stage 2: Doc builders (N threads)
//!   → bounded channel (capacity: CHANNEL_CAP)
//! Stage 3: Writer (this thread, owns IndexWriter reference)
//! ```
//!
//! Back-pressure is automatic — when the writer can't keep up,
//! channels fill, producers block. Memory stays bounded.

use crossbeam_channel::{bounded, Receiver, Sender};
use std::thread;
use tantivy::IndexWriter;

use crate::error::{Result, SearchDbError};
use crate::schema::Schema;
use crate::writer;

// Pipeline is wired in by Task A4; suppress dead_code until then.
#[allow(dead_code)]
const CHANNEL_CAP: usize = 100;

/// Configuration for the pipeline.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PipelineConfig {
    /// Number of document-builder threads. Default: available_parallelism / 4, min 1.
    pub num_builders: usize,
    /// Channel capacity between stages.
    pub channel_cap: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        let cpus = thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4);
        Self {
            num_builders: (cpus / 4).max(1),
            channel_cap: CHANNEL_CAP,
        }
    }
}

/// Stats returned after pipeline completes.
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct PipelineStats {
    pub docs_indexed: u64,
    pub errors: u64,
}

/// A built document ready for indexing.
#[allow(dead_code)]
struct BuiltDoc {
    doc: tantivy::TantivyDocument,
    doc_id: String,
}

/// Run the parallel indexing pipeline over an iterator of JSON rows.
///
/// The caller retains ownership of the IndexWriter — the pipeline does NOT commit.
/// This allows the caller to control commit boundaries (e.g., per segment_size batch).
#[allow(dead_code)]
pub fn run_pipeline(
    rows: impl Iterator<Item = serde_json::Value> + Send + 'static,
    tv_schema: &tantivy::schema::Schema,
    app_schema: &Schema,
    index_writer: &mut IndexWriter,
    config: PipelineConfig,
) -> Result<PipelineStats> {
    let tv_schema_clone = tv_schema.clone();
    let app_schema_clone = app_schema.clone();

    let id_field = tv_schema
        .get_field("_id")
        .map_err(|_| SearchDbError::Schema("missing _id field".into()))?;

    // Stage 1 → Stage 2 channel: raw JSON rows
    let (row_tx, row_rx): (Sender<serde_json::Value>, Receiver<serde_json::Value>) =
        bounded(config.channel_cap);

    // Stage 2 → Stage 3 channel: built tantivy documents
    let (doc_tx, doc_rx): (Sender<BuiltDoc>, Receiver<BuiltDoc>) = bounded(config.channel_cap);

    // Stage 1: Producer — feeds rows into the channel on a dedicated thread.
    let producer = thread::spawn(move || {
        for row in rows {
            if row_tx.send(row).is_err() {
                break; // Receivers dropped
            }
        }
        // row_tx dropped here → channel closes → builders see Disconnected
    });

    // Stage 2: Document builders — N threads pull from row_rx, build docs, send to doc_tx.
    let mut builders = Vec::with_capacity(config.num_builders);
    for _ in 0..config.num_builders {
        let rx = row_rx.clone();
        let tx = doc_tx.clone();
        let tv = tv_schema_clone.clone();
        let app = app_schema_clone.clone();

        builders.push(thread::spawn(move || {
            let mut errs = 0u64;
            for row in rx {
                let doc_id = writer::make_doc_id(&row);
                match writer::build_document(&tv, &app, &row, &doc_id) {
                    Ok(doc) => {
                        if tx.send(BuiltDoc { doc, doc_id }).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        errs += 1;
                        eprintln!("[dsrch] pipeline: doc build error: {e}");
                    }
                }
            }
            errs
        }));
    }
    // Drop our copies so channels close when all builders finish.
    drop(row_rx);
    drop(doc_tx);

    // Stage 3: Writer — this thread. Receives built docs and upserts them.
    let mut stats = PipelineStats::default();
    for built_doc in doc_rx {
        writer::upsert_document(index_writer, id_field, built_doc.doc, &built_doc.doc_id);
        stats.docs_indexed += 1;
    }

    // Join all threads and collect error counts
    producer
        .join()
        .map_err(|_| SearchDbError::Delta("pipeline producer thread panicked".into()))?;

    for handle in builders {
        let errs = handle
            .join()
            .map_err(|_| SearchDbError::Delta("pipeline builder thread panicked".into()))?;
        stats.errors += errs;
    }

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{FieldType, Schema};
    use std::collections::BTreeMap;

    fn test_schema() -> Schema {
        Schema {
            fields: BTreeMap::from([
                ("name".into(), FieldType::Keyword),
                ("score".into(), FieldType::Numeric),
            ]),
        }
    }

    #[test]
    fn test_pipeline_indexes_rows() {
        let schema = test_schema();
        let tv_schema = schema.build_tantivy_schema();
        let dir = tantivy::directory::RamDirectory::create();
        let index = tantivy::Index::create(
            dir,
            tv_schema.clone(),
            tantivy::IndexSettings::default(),
        )
        .unwrap();
        let mut writer = index.writer(15_000_000).unwrap();

        let rows: Vec<serde_json::Value> = (0..100)
            .map(|i| {
                serde_json::json!({
                    "_id": format!("doc-{i}"),
                    "name": format!("name-{i}"),
                    "score": i
                })
            })
            .collect();

        let stats = run_pipeline(
            rows.into_iter(),
            &tv_schema,
            &schema,
            &mut writer,
            PipelineConfig::default(),
        )
        .unwrap();

        writer.commit().unwrap();

        assert_eq!(stats.docs_indexed, 100);
        assert_eq!(stats.errors, 0);

        let reader = index.reader().unwrap();
        assert_eq!(reader.searcher().num_docs(), 100);
    }

    #[test]
    fn test_pipeline_handles_empty_input() {
        let schema = test_schema();
        let tv_schema = schema.build_tantivy_schema();
        let dir = tantivy::directory::RamDirectory::create();
        let index = tantivy::Index::create(
            dir,
            tv_schema.clone(),
            tantivy::IndexSettings::default(),
        )
        .unwrap();
        let mut writer = index.writer(15_000_000).unwrap();

        let rows: Vec<serde_json::Value> = vec![];

        let stats = run_pipeline(
            rows.into_iter(),
            &tv_schema,
            &schema,
            &mut writer,
            PipelineConfig::default(),
        )
        .unwrap();

        assert_eq!(stats.docs_indexed, 0);
    }

    #[test]
    fn test_pipeline_upserts_duplicate_ids() {
        let schema = test_schema();
        let tv_schema = schema.build_tantivy_schema();
        let dir = tantivy::directory::RamDirectory::create();
        let index = tantivy::Index::create(
            dir,
            tv_schema.clone(),
            tantivy::IndexSettings::default(),
        )
        .unwrap();
        let mut writer = index.writer(15_000_000).unwrap();

        let rows = vec![
            serde_json::json!({"_id": "dup", "name": "first", "score": 1}),
            serde_json::json!({"_id": "dup", "name": "second", "score": 2}),
            serde_json::json!({"_id": "unique", "name": "third", "score": 3}),
        ];

        let stats = run_pipeline(
            rows.into_iter(),
            &tv_schema,
            &schema,
            &mut writer,
            PipelineConfig::default(),
        )
        .unwrap();

        writer.commit().unwrap();

        assert_eq!(stats.docs_indexed, 3);
        let reader = index.reader().unwrap();
        assert_eq!(reader.searcher().num_docs(), 2);
    }

    #[test]
    fn test_pipeline_with_segment_commits() {
        let schema = test_schema();
        let tv_schema = schema.build_tantivy_schema();
        let dir = tantivy::directory::RamDirectory::create();
        let index = tantivy::Index::create(
            dir,
            tv_schema.clone(),
            tantivy::IndexSettings::default(),
        )
        .unwrap();
        let mut writer = index.writer(15_000_000).unwrap();

        let segment_size = 50;
        let all_rows: Vec<serde_json::Value> = (0..120)
            .map(|i| {
                serde_json::json!({
                    "_id": format!("doc-{i}"),
                    "name": format!("n{i}"),
                    "score": i
                })
            })
            .collect();

        let mut total_indexed = 0u64;
        for chunk in all_rows.chunks(segment_size) {
            let stats = run_pipeline(
                chunk.to_vec().into_iter(),
                &tv_schema,
                &schema,
                &mut writer,
                PipelineConfig {
                    num_builders: 2,
                    channel_cap: 10,
                },
            )
            .unwrap();
            writer.commit().unwrap();
            total_indexed += stats.docs_indexed;
        }

        assert_eq!(total_indexed, 120);
        let reader = index.reader().unwrap();
        assert_eq!(reader.searcher().num_docs(), 120);
    }
}

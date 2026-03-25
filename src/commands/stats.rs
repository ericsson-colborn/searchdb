use tantivy::Index;

use crate::error::{Result, SearchDbError};
use crate::storage::Storage;
use crate::OutputFormat;

/// Print index statistics.
/// `gap_info` is `Some((delta_version, gap_versions))` when a Delta source is connected.
pub fn run(
    storage: &Storage,
    name: &str,
    fmt: OutputFormat,
    gap_info: Option<(i64, i64)>,
) -> Result<()> {
    if !storage.exists(name) {
        return Err(SearchDbError::IndexNotFound(name.to_string()));
    }

    let config = storage.load_config(name)?;
    let index = Index::open_in_dir(storage.tantivy_dir(name))?;
    let reader = index
        .reader()
        .map_err(|e| SearchDbError::Schema(format!("failed to open reader: {e}")))?;
    let searcher = reader.searcher();

    let num_docs: u64 = searcher
        .segment_readers()
        .iter()
        .map(|r| r.num_docs() as u64)
        .sum();
    let num_segments = searcher.segment_readers().len();

    match fmt {
        OutputFormat::Json => {
            let mut stats = serde_json::json!({
                "index": name,
                "num_docs": num_docs,
                "num_segments": num_segments,
                "fields": &config.schema.fields,
                "index_version": config.index_version,
            });
            if let Some((delta_ver, gap_ver)) = gap_info {
                stats["delta_version"] = serde_json::json!(delta_ver);
                stats["gap_versions"] = serde_json::json!(gap_ver);
            }
            if let Some(ref compact) = config.compact {
                stats["compaction"] = serde_json::json!({
                    "segment_size": compact.segment_size,
                    "merge_interval_secs": compact.merge_interval_secs,
                    "max_segments": compact.max_segments,
                    "last_segment_at": compact.last_segment_at,
                    "last_merge_at": compact.last_merge_at,
                });
            }
            println!("{}", serde_json::to_string(&stats)?);
        }
        OutputFormat::Text => {
            println!("Index:      {name}");
            println!("Documents:  {num_docs}");
            println!("Segments:   {num_segments}");
            if let Some(v) = config.index_version {
                if let Some((delta_ver, gap_ver)) = gap_info {
                    println!("Version:    {v} (Delta HEAD: {delta_ver}, gap: {gap_ver} versions)");
                } else {
                    println!("Version:    {v}");
                }
            }
            println!("Fields:");
            for (field_name, field_type) in &config.schema.fields {
                println!("  {field_name}: {field_type:?}");
            }
            if let Some(ref src) = config.delta_source {
                println!("Delta:      {src}");
            }
            if let Some(ref compact) = config.compact {
                println!("Compaction:");
                println!("  Segment size: {}", compact.segment_size);
                println!("  Merge interval: {}s", compact.merge_interval_secs);
                println!("  Max segments: {}", compact.max_segments);
                if let Some(ref ts) = compact.last_segment_at {
                    println!("  Last segment: {ts}");
                }
                if let Some(ref ts) = compact.last_merge_at {
                    println!("  Last merge:   {ts}");
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{index as index_cmd, new_index};
    use std::io::Write;

    #[test]
    fn test_stats_empty_index() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());
        new_index::run(
            &storage,
            "test",
            Some(r#"{"fields":{"name":"keyword"}}"#),
            false,
            None,
            false,
        )
        .unwrap();
        run(&storage, "test", OutputFormat::Json, None).unwrap();
    }

    #[test]
    fn test_stats_with_docs() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());
        new_index::run(
            &storage,
            "test",
            Some(r#"{"fields":{"name":"keyword"}}"#),
            false,
            None,
            false,
        )
        .unwrap();

        let ndjson = dir.path().join("data.ndjson");
        let mut f = std::fs::File::create(&ndjson).unwrap();
        writeln!(f, r#"{{"_id":"d1","name":"alice"}}"#).unwrap();
        writeln!(f, r#"{{"_id":"d2","name":"bob"}}"#).unwrap();
        index_cmd::run(&storage, "test", Some(ndjson.to_str().unwrap())).unwrap();

        run(&storage, "test", OutputFormat::Json, None).unwrap();

        let index = Index::open_in_dir(storage.tantivy_dir("test")).unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 2);
    }

    #[test]
    fn test_stats_with_compact_meta() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());
        new_index::run(
            &storage,
            "test",
            Some(r#"{"fields":{"name":"keyword"}}"#),
            false,
            None,
            false,
        )
        .unwrap();

        // Manually inject compact metadata
        let mut config = storage.load_config("test").unwrap();
        config.compact = Some(crate::storage::CompactMeta {
            segment_size: 50_000,
            merge_interval_secs: 600,
            max_segments: 20,
            last_segment_at: Some("2026-03-24T14:30:00Z".into()),
            last_merge_at: Some("2026-03-24T14:25:00Z".into()),
        });
        storage.save_config("test", &config).unwrap();

        // Should not crash; verifies the compact metadata is read and displayed
        run(&storage, "test", OutputFormat::Json, None).unwrap();
    }

    #[test]
    fn test_stats_nonexistent_index() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());
        let result = run(&storage, "nope", OutputFormat::Json, None);
        assert!(result.is_err());
    }
}

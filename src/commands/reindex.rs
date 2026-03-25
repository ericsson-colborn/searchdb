use tantivy::Index;

use crate::delta::DeltaSync;
use crate::error::{Result, SearchDbError};
use crate::storage::Storage;
use crate::writer;

/// Full rebuild from Delta Lake source.
///
/// Drops existing index data, reads all rows from Delta (optionally at a
/// specific version), and rebuilds the index from scratch.
pub async fn run(storage: &Storage, name: &str, as_of_version: Option<i64>) -> Result<()> {
    if !storage.exists(name) {
        return Err(SearchDbError::IndexNotFound(name.to_string()));
    }

    let mut config = storage.load_config(name)?;

    let source = config.delta_source.as_deref().ok_or_else(|| {
        SearchDbError::Delta(format!(
            "index '{name}' has no Delta source — use connect-delta first"
        ))
    })?;

    let delta = DeltaSync::new(source);
    let version = match as_of_version {
        Some(v) => v,
        None => delta.current_version().await?,
    };

    eprintln!("[dsrch] Reindexing '{name}' from Delta v{version}");

    let rows = delta.full_load(as_of_version).await?;
    eprintln!("[dsrch] Loaded {} row(s) from Delta", rows.len());

    // Destroy and recreate the tantivy index directory
    let tantivy_dir = storage.tantivy_dir(name);
    if tantivy_dir.exists() {
        std::fs::remove_dir_all(&tantivy_dir)?;
    }
    std::fs::create_dir_all(&tantivy_dir)?;

    let tantivy_schema = config.schema.build_tantivy_schema();
    let index = Index::create_in_dir(&tantivy_dir, tantivy_schema.clone())?;
    let mut index_writer = index.writer(50_000_000)?;

    let id_field = tantivy_schema
        .get_field("_id")
        .map_err(|_| SearchDbError::Schema("missing _id field".into()))?;

    let mut count = 0u64;
    for row in &rows {
        let doc_id = writer::make_doc_id(row);
        let doc = writer::build_document(&tantivy_schema, &config.schema, row, &doc_id)?;
        writer::upsert_document(&index_writer, id_field, doc, &doc_id);
        count += 1;
    }
    index_writer.commit()?;

    config.index_version = Some(version);
    storage.save_config(name, &config)?;

    eprintln!("[dsrch] Reindexed '{name}' with {count} document(s) at Delta v{version}");
    Ok(())
}

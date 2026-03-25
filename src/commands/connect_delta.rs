use tantivy::Index;

use crate::delta::DeltaSync;
use crate::error::{Result, SearchDbError};
use crate::schema::Schema;
use crate::storage::{IndexConfig, Storage};
use crate::writer;

/// Attach a Delta Lake source, create an index, and perform initial full load.
pub async fn run(storage: &Storage, name: &str, source: &str, schema_json: &str) -> Result<()> {
    if storage.exists(name) {
        return Err(SearchDbError::IndexExists(name.to_string()));
    }

    let schema = Schema::from_json(schema_json)?;
    let tantivy_schema = schema.build_tantivy_schema();

    // Open Delta table and get current version
    let delta = DeltaSync::new(source);
    let version = delta.current_version().await?;
    eprintln!("[searchdb] Delta table at version {version}");

    // Full load all rows
    let rows = delta.full_load(None).await?;
    eprintln!("[searchdb] Loaded {} row(s) from Delta", rows.len());

    // Create index on disk
    storage.create_dirs(name)?;
    let index = Index::create_in_dir(storage.tantivy_dir(name), tantivy_schema.clone())?;
    let mut index_writer = index.writer(50_000_000)?;

    let id_field = tantivy_schema
        .get_field("_id")
        .map_err(|_| SearchDbError::Schema("missing _id field".into()))?;

    // Index all rows
    let mut count = 0u64;
    for row in &rows {
        let doc_id = writer::make_doc_id(row);
        let doc = writer::build_document(&tantivy_schema, &schema, row, &doc_id)?;
        writer::upsert_document(&index_writer, id_field, doc, &doc_id);
        count += 1;
    }
    index_writer.commit()?;

    // Save config with Delta source + version watermark
    let config = IndexConfig {
        schema,
        delta_source: Some(source.to_string()),
        index_version: Some(version),
        compact: None,
    };
    storage.save_config(name, &config)?;

    eprintln!("[searchdb] Created index '{name}' with {count} document(s) from Delta v{version}");
    Ok(())
}

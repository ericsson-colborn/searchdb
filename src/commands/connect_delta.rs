use tantivy::Index;

use crate::delta::DeltaSync;
use crate::error::{Result, SearchDbError};
use crate::schema::Schema;
use crate::storage::{IndexConfig, Storage};
use crate::writer;

/// Attach a Delta Lake source, create an index, and perform initial full load.
///
/// If `schema_json` is None, infers the schema from the Delta table's Arrow schema.
/// If `dry_run` is true, prints the inferred schema as JSON and exits without creating.
pub async fn run(
    storage: &Storage,
    name: &str,
    source: &str,
    schema_json: Option<&str>,
    dry_run: bool,
) -> Result<()> {
    if !dry_run && storage.exists(name) {
        return Err(SearchDbError::IndexExists(name.to_string()));
    }

    // Open Delta table and get current version
    let delta = DeltaSync::new(source);
    let version = delta.current_version().await?;
    eprintln!("[dsrch] Delta table at version {version}");

    let (schema, inferred) = match schema_json {
        Some(json) => (Schema::from_json(json)?, false),
        None => {
            let arrow_schema = delta.arrow_schema().await?;
            let schema = crate::schema::from_arrow_schema(&arrow_schema);
            eprintln!(
                "[dsrch] Inferred schema from Arrow: {} field(s)",
                schema.fields.len()
            );
            (schema, true)
        }
    };

    if dry_run {
        let json = serde_json::to_string(&schema)?;
        println!("{json}");
        return Ok(());
    }

    let tantivy_schema = schema.build_tantivy_schema();

    // Full load all rows
    let rows = delta.full_load(None).await?;
    eprintln!("[dsrch] Loaded {} row(s) from Delta", rows.len());

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
        inferred,
        delta_source: Some(source.to_string()),
        index_version: Some(version),
        compact: None,
    };
    storage.save_config(name, &config)?;

    eprintln!("[dsrch] Created index '{name}' with {count} document(s) from Delta v{version}");
    Ok(())
}

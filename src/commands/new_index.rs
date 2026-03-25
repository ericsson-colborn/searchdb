use crate::error::{Result, SearchDbError};
use crate::schema::Schema;
use crate::storage::{IndexConfig, Storage};

/// Create a new empty index from a schema declaration.
pub fn run(storage: &Storage, name: &str, schema_json: &str, overwrite: bool) -> Result<()> {
    if storage.exists(name) {
        if overwrite {
            storage.drop(name)?;
        } else {
            return Err(SearchDbError::IndexExists(name.to_string()));
        }
    }

    let schema = Schema::from_json(schema_json)?;
    let tantivy_schema = schema.build_tantivy_schema();

    storage.create_dirs(name)?;

    // Create the tantivy index on disk
    let tantivy_dir = storage.tantivy_dir(name);
    let _index = tantivy::Index::create_in_dir(&tantivy_dir, tantivy_schema)?;

    // Save our config (schema + metadata)
    let config = IndexConfig {
        schema,
        delta_source: None,
        index_version: None,
        compact: None,
    };
    storage.save_config(name, &config)?;

    eprintln!(
        "[searchdb] Created index '{name}' with {} field(s)",
        config.schema.fields.len()
    );
    Ok(())
}

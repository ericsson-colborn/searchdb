use std::collections::BTreeMap;

use crate::error::{Result, SearchDbError};
use crate::schema::Schema;
use crate::storage::{IndexConfig, Storage};

/// Create a new empty index, optionally from a schema declaration.
///
/// If `schema_json` is None, creates an empty-schema index with `inferred: true`.
/// Schema will be populated on first `dsrch index` via schema evolution.
///
/// If `infer_from` is provided, reads the NDJSON file and infers field types.
/// If both `infer_from` and `schema_json` are provided, schema acts as override.
///
/// If `dry_run` is true, prints the inferred schema as JSON and exits without creating.
pub fn run(
    storage: &Storage,
    name: &str,
    schema_json: Option<&str>,
    overwrite: bool,
    infer_from: Option<&str>,
    dry_run: bool,
) -> Result<()> {
    // Infer-from mode: read file and infer schema
    if let Some(file_path) = infer_from {
        let docs = read_ndjson_file(file_path)?;
        let inferred = crate::schema::infer_schema(&docs);

        // If schema_json provided, use it as override (partial schema)
        let schema = match schema_json {
            Some(json) => {
                let overrides = Schema::from_json(json)?;
                crate::schema::merge_schemas(&overrides, &inferred)
            }
            None => inferred,
        };

        if dry_run {
            let json = serde_json::to_string(&schema)?;
            println!("{json}");
            return Ok(());
        }

        return create_index(storage, name, schema, true, overwrite);
    }

    // Dry-run without infer-from is a no-op (nothing to infer)
    if dry_run {
        return Err(SearchDbError::Schema(
            "--dry-run requires --infer-from".into(),
        ));
    }

    let (schema, inferred) = match schema_json {
        Some(json) => (Schema::from_json(json)?, false),
        None => (
            Schema {
                fields: BTreeMap::new(),
            },
            true,
        ),
    };

    create_index(storage, name, schema, inferred, overwrite)
}

/// Read an NDJSON file into a Vec of JSON values (for inference, not indexing).
fn read_ndjson_file(path: &str) -> Result<Vec<serde_json::Value>> {
    use std::io::BufRead;
    let f = std::fs::File::open(path)
        .map_err(|e| SearchDbError::Schema(format!("cannot open file '{path}': {e}")))?;
    let reader = std::io::BufReader::new(f);
    let mut docs = Vec::new();
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str(trimmed) {
            Ok(v) => docs.push(v),
            Err(e) => eprintln!("[dsrch] Skipping invalid JSON during inference: {e}"),
        }
    }
    Ok(docs)
}

/// Create the index on disk with the given schema.
fn create_index(
    storage: &Storage,
    name: &str,
    schema: Schema,
    inferred: bool,
    overwrite: bool,
) -> Result<()> {
    if storage.exists(name) {
        if overwrite {
            storage.drop(name)?;
        } else {
            return Err(SearchDbError::IndexExists(name.to_string()));
        }
    }

    let tantivy_schema = schema.build_tantivy_schema();
    storage.create_dirs(name)?;
    let tantivy_dir = storage.tantivy_dir(name);
    let _index = tantivy::Index::create_in_dir(&tantivy_dir, tantivy_schema)?;

    let config = IndexConfig {
        schema,
        inferred,
        delta_source: None,
        index_version: None,
        compact: None,
    };
    storage.save_config(name, &config)?;

    eprintln!(
        "[dsrch] Created index '{name}' with {} field(s){}",
        config.schema.fields.len(),
        if inferred {
            " (schema will be inferred from data)"
        } else {
            ""
        }
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::FieldType;

    #[test]
    fn test_new_index_without_schema() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());

        run(&storage, "test", None, false, None, false).unwrap();

        let config = storage.load_config("test").unwrap();
        assert!(config.schema.fields.is_empty());
        assert!(config.inferred);
    }

    #[test]
    fn test_new_index_with_schema() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());

        let schema_json = r#"{"fields":{"name":"keyword"}}"#;
        run(&storage, "test", Some(schema_json), false, None, false).unwrap();

        let config = storage.load_config("test").unwrap();
        assert_eq!(config.schema.fields.len(), 1);
        assert!(!config.inferred);
    }

    #[test]
    fn test_new_index_infer_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());

        let ndjson_path = dir.path().join("sample.ndjson");
        std::fs::write(
            &ndjson_path,
            r#"{"name":"alice","age":30,"created":"2024-01-15T10:00:00Z"}
{"name":"bob","age":25}
"#,
        )
        .unwrap();

        run(
            &storage,
            "test",
            None,
            false,
            Some(ndjson_path.to_str().unwrap()),
            false,
        )
        .unwrap();

        let config = storage.load_config("test").unwrap();
        assert_eq!(config.schema.fields["name"], FieldType::Keyword);
        assert_eq!(config.schema.fields["age"], FieldType::Numeric);
        assert_eq!(config.schema.fields["created"], FieldType::Date);
        assert!(config.inferred);

        // Index should be empty (file was not indexed, just used for inference)
        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("test")).unwrap();
        let reader = index.reader().unwrap();
        assert_eq!(reader.searcher().num_docs(), 0);
    }

    #[test]
    fn test_new_index_infer_from_with_schema_override() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());

        let ndjson_path = dir.path().join("sample.ndjson");
        std::fs::write(
            &ndjson_path,
            r#"{"name":"alice","notes":"some text","age":30}"#,
        )
        .unwrap();

        // Override "notes" to be text instead of inferred keyword
        run(
            &storage,
            "test",
            Some(r#"{"fields":{"notes":"text"}}"#),
            false,
            Some(ndjson_path.to_str().unwrap()),
            false,
        )
        .unwrap();

        let config = storage.load_config("test").unwrap();
        assert_eq!(config.schema.fields["notes"], FieldType::Text); // overridden
        assert_eq!(config.schema.fields["name"], FieldType::Keyword); // inferred
        assert_eq!(config.schema.fields["age"], FieldType::Numeric); // inferred
        assert!(config.inferred);
    }

    #[test]
    fn test_new_index_dry_run() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());

        let ndjson_path = dir.path().join("sample.ndjson");
        std::fs::write(&ndjson_path, r#"{"name":"alice","age":30}"#).unwrap();

        // Dry run should succeed but NOT create the index
        run(
            &storage,
            "test",
            None,
            false,
            Some(ndjson_path.to_str().unwrap()),
            true,
        )
        .unwrap();

        assert!(!storage.exists("test"));
    }
}

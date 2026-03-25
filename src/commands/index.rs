use std::fs::File;
use std::io::{self, BufRead, BufReader};

use tantivy::{Index, TantivyDocument};

use crate::error::{Result, SearchDbError};
use crate::schema::Schema;
use crate::storage::Storage;
use crate::writer;

/// Read all existing documents from the index via the _source field.
fn read_existing_docs(storage: &Storage, name: &str) -> Result<Vec<serde_json::Value>> {
    let index = Index::open_in_dir(storage.tantivy_dir(name))?;
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let tantivy_schema = index.schema();
    let source_field = tantivy_schema
        .get_field("_source")
        .map_err(|_| SearchDbError::Schema("missing _source field".into()))?;

    let mut docs = Vec::new();
    for segment_reader in searcher.segment_readers() {
        let store_reader = segment_reader.get_store_reader(1)?;
        for doc_id in 0..segment_reader.num_docs() {
            let doc: TantivyDocument = store_reader.get(doc_id)?;
            if let Some(source_val) = doc.get_first(source_field) {
                if let Some(source_str) = tantivy::schema::Value::as_str(&source_val) {
                    if let Ok(parsed) = serde_json::from_str(source_str) {
                        docs.push(parsed);
                    }
                }
            }
        }
    }
    Ok(docs)
}

/// Rebuild the tantivy index with a new schema, reindexing existing docs.
fn rebuild_index(storage: &Storage, name: &str, new_schema: &Schema) -> Result<()> {
    let existing_docs = read_existing_docs(storage, name)?;

    // Drop and recreate tantivy index dir
    let tantivy_dir = storage.tantivy_dir(name);
    std::fs::remove_dir_all(&tantivy_dir)?;
    std::fs::create_dir_all(&tantivy_dir)?;

    let tantivy_schema = new_schema.build_tantivy_schema();
    let index = Index::create_in_dir(&tantivy_dir, tantivy_schema.clone())?;
    let mut index_writer = index.writer(50_000_000)?;

    let id_field = tantivy_schema
        .get_field("_id")
        .map_err(|_| SearchDbError::Schema("missing _id field".into()))?;

    for doc_json in &existing_docs {
        let doc_id = writer::make_doc_id(doc_json);
        let doc = writer::build_document(&tantivy_schema, new_schema, doc_json, &doc_id)?;
        writer::upsert_document(&index_writer, id_field, doc, &doc_id);
    }
    index_writer.commit()?;

    eprintln!(
        "[dsrch] Rebuilt index '{name}' with {} existing document(s)",
        existing_docs.len()
    );
    Ok(())
}

/// Read NDJSON from file or stdin into a Vec.
fn read_ndjson(file: Option<&str>) -> Result<Vec<serde_json::Value>> {
    let reader: Box<dyn BufRead> = match file {
        Some(path) => {
            let f = File::open(path)
                .map_err(|e| SearchDbError::Schema(format!("cannot open file '{path}': {e}")))?;
            Box::new(BufReader::new(f))
        }
        None => Box::new(BufReader::new(io::stdin())),
    };

    let mut docs = Vec::new();
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str(trimmed) {
            Ok(v) => docs.push(v),
            Err(e) => eprintln!("[dsrch] Skipping invalid JSON: {e}"),
        }
    }
    Ok(docs)
}

/// Bulk index NDJSON documents into an existing index.
///
/// For inferred-schema indexes, discovers new fields and triggers schema
/// evolution (rebuild + reindex) when needed. For explicit-schema indexes,
/// unknown fields are silently ignored.
pub fn run(storage: &Storage, name: &str, file: Option<&str>) -> Result<()> {
    if !storage.exists(name) {
        return Err(SearchDbError::IndexNotFound(name.to_string()));
    }

    let mut config = storage.load_config(name)?;

    // Read all input documents
    let new_docs = read_ndjson(file)?;
    if new_docs.is_empty() {
        eprintln!("[dsrch] No documents to index");
        return Ok(());
    }

    // Schema inference + evolution (only for inferred schemas)
    if config.inferred {
        let discovered = crate::schema::infer_schema(&new_docs);
        let (merged, new_fields) =
            crate::schema::merge_schemas_with_diff(&config.schema, &discovered);

        if !new_fields.is_empty() {
            eprintln!(
                "[dsrch] Schema evolution: discovered {} new field(s): {}",
                new_fields.len(),
                new_fields.join(", ")
            );
            config.schema = merged;
            rebuild_index(storage, name, &config.schema)?;
            storage.save_config(name, &config)?;
        }
    }

    // Now index the new batch
    let tantivy_schema = config.schema.build_tantivy_schema();
    let index = Index::open_in_dir(storage.tantivy_dir(name))?;
    let mut index_writer = index.writer(50_000_000)?;
    let id_field = tantivy_schema
        .get_field("_id")
        .map_err(|_| SearchDbError::Schema("missing _id field".into()))?;

    let mut count = 0u64;
    for doc_json in &new_docs {
        let doc_id = writer::make_doc_id(doc_json);
        let doc = writer::build_document(&tantivy_schema, &config.schema, doc_json, &doc_id)?;
        writer::upsert_document(&index_writer, id_field, doc, &doc_id);
        count += 1;
    }
    index_writer.commit()?;

    eprintln!("[dsrch] Indexed {count} document(s) into '{name}'");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::new_index;
    use crate::schema::FieldType;
    use std::io::Write;

    fn setup_index(dir: &std::path::Path) -> Storage {
        let storage = Storage::new(dir.to_str().unwrap());
        let schema_json = r#"{"fields":{"name":"keyword","notes":"text","age":"numeric"}}"#;
        new_index::run(&storage, "test", Some(schema_json), false, None, false).unwrap();
        storage
    }

    #[test]
    fn test_index_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let storage = setup_index(dir.path());

        let ndjson_path = dir.path().join("data.ndjson");
        let mut f = std::fs::File::create(&ndjson_path).unwrap();
        writeln!(
            f,
            r#"{{"_id":"d1","name":"glucose","notes":"fasting sample","age":45}}"#
        )
        .unwrap();
        writeln!(
            f,
            r#"{{"_id":"d2","name":"a1c","notes":"borderline diabetic","age":62}}"#
        )
        .unwrap();

        run(&storage, "test", Some(ndjson_path.to_str().unwrap())).unwrap();

        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("test")).unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 2);
    }

    #[test]
    fn test_index_upsert_deduplicates() {
        let dir = tempfile::tempdir().unwrap();
        let storage = setup_index(dir.path());

        let ndjson1 = dir.path().join("batch1.ndjson");
        std::fs::write(&ndjson1, r#"{"_id":"d1","name":"original"}"#).unwrap();
        run(&storage, "test", Some(ndjson1.to_str().unwrap())).unwrap();

        let ndjson2 = dir.path().join("batch2.ndjson");
        std::fs::write(&ndjson2, r#"{"_id":"d1","name":"updated"}"#).unwrap();
        run(&storage, "test", Some(ndjson2.to_str().unwrap())).unwrap();

        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("test")).unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 1);
    }

    #[test]
    fn test_index_skips_invalid_json() {
        let dir = tempfile::tempdir().unwrap();
        let storage = setup_index(dir.path());

        let ndjson = dir.path().join("mixed.ndjson");
        let mut f = std::fs::File::create(&ndjson).unwrap();
        writeln!(f, r#"{{"_id":"d1","name":"good"}}"#).unwrap();
        writeln!(f, "not valid json").unwrap();
        writeln!(f, r#"{{"_id":"d2","name":"also good"}}"#).unwrap();

        run(&storage, "test", Some(ndjson.to_str().unwrap())).unwrap();

        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("test")).unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 2);
    }

    #[test]
    fn test_index_nonexistent_index_errors() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());
        let result = run(&storage, "no_such_index", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_index_infers_schema_from_data() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());

        // Create index WITHOUT schema
        new_index::run(&storage, "test", None, false, None, false).unwrap();

        let ndjson_path = dir.path().join("data.ndjson");
        std::fs::write(
            &ndjson_path,
            r#"{"_id":"d1","name":"glucose","age":45,"created":"2024-01-15T10:00:00Z"}
{"_id":"d2","name":"a1c","age":62,"created":"2024-02-20T08:30:00Z"}
"#,
        )
        .unwrap();

        run(&storage, "test", Some(ndjson_path.to_str().unwrap())).unwrap();

        let config = storage.load_config("test").unwrap();
        assert_eq!(config.schema.fields["name"], FieldType::Keyword);
        assert_eq!(config.schema.fields["age"], FieldType::Numeric);
        assert_eq!(config.schema.fields["created"], FieldType::Date);

        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("test")).unwrap();
        let reader = index.reader().unwrap();
        assert_eq!(reader.searcher().num_docs(), 2);
    }

    #[test]
    fn test_index_evolves_schema_with_new_fields() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());

        new_index::run(&storage, "test", None, false, None, false).unwrap();

        // First batch
        let batch1 = dir.path().join("batch1.ndjson");
        std::fs::write(&batch1, r#"{"_id":"d1","name":"glucose","age":45}"#).unwrap();
        run(&storage, "test", Some(batch1.to_str().unwrap())).unwrap();

        let config1 = storage.load_config("test").unwrap();
        assert_eq!(config1.schema.fields.len(), 2);

        // Second batch adds "status"
        let batch2 = dir.path().join("batch2.ndjson");
        std::fs::write(
            &batch2,
            r#"{"_id":"d2","name":"a1c","age":62,"status":"active"}"#,
        )
        .unwrap();
        run(&storage, "test", Some(batch2.to_str().unwrap())).unwrap();

        let config2 = storage.load_config("test").unwrap();
        assert_eq!(config2.schema.fields.len(), 3);
        assert_eq!(config2.schema.fields["status"], FieldType::Keyword);

        // Both documents should be present
        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("test")).unwrap();
        let reader = index.reader().unwrap();
        assert_eq!(reader.searcher().num_docs(), 2);
    }

    #[test]
    fn test_index_explicit_schema_ignores_unknown_fields() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());

        // Create index WITH explicit schema (only "name")
        let schema_json = r#"{"fields":{"name":"keyword"}}"#;
        new_index::run(&storage, "test", Some(schema_json), false, None, false).unwrap();

        // Index doc with extra "age" field
        let ndjson = dir.path().join("data.ndjson");
        std::fs::write(&ndjson, r#"{"_id":"d1","name":"glucose","age":45}"#).unwrap();
        run(&storage, "test", Some(ndjson.to_str().unwrap())).unwrap();

        // Schema should NOT have changed
        let config = storage.load_config("test").unwrap();
        assert_eq!(config.schema.fields.len(), 1);
        assert!(!config.inferred);

        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("test")).unwrap();
        let reader = index.reader().unwrap();
        assert_eq!(reader.searcher().num_docs(), 1);
    }

    #[test]
    fn test_end_to_end_inferred_schema_search() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());

        new_index::run(&storage, "test", None, false, None, false).unwrap();

        let ndjson = dir.path().join("data.ndjson");
        std::fs::write(
            &ndjson,
            r#"{"_id":"d1","name":"glucose","value":95.0}
{"_id":"d2","name":"a1c","value":6.1}
"#,
        )
        .unwrap();
        run(&storage, "test", Some(ndjson.to_str().unwrap())).unwrap();

        let config = storage.load_config("test").unwrap();
        assert_eq!(config.schema.fields.len(), 2);
        assert_eq!(config.schema.fields["name"], FieldType::Keyword);
        assert_eq!(config.schema.fields["value"], FieldType::Numeric);
        assert!(config.inferred);

        // Verify searchable
        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("test")).unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let tv_schema = index.schema();
        let name_field = tv_schema.get_field("name").unwrap();

        let query_parser = tantivy::query::QueryParser::for_index(&index, vec![name_field]);
        let query = query_parser.parse_query(r#"name:"glucose""#).unwrap();
        let results = searcher
            .search(&query, &tantivy::collector::TopDocs::with_limit(10))
            .unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_end_to_end_schema_evolution_preserves_searchability() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());

        new_index::run(&storage, "test", None, false, None, false).unwrap();

        // Batch 1
        let b1 = dir.path().join("b1.ndjson");
        std::fs::write(&b1, r#"{"_id":"d1","name":"glucose"}"#).unwrap();
        run(&storage, "test", Some(b1.to_str().unwrap())).unwrap();

        // Batch 2 — new field "value"
        let b2 = dir.path().join("b2.ndjson");
        std::fs::write(&b2, r#"{"_id":"d2","name":"a1c","value":6.1}"#).unwrap();
        run(&storage, "test", Some(b2.to_str().unwrap())).unwrap();

        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("test")).unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 2);

        let config = storage.load_config("test").unwrap();
        assert_eq!(config.schema.fields["value"], FieldType::Numeric);
    }

    #[test]
    fn test_partial_schema_override_text_field() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());

        // Create with partial schema: declare "notes" as text
        let schema_json = r#"{"fields":{"notes":"text"}}"#;
        new_index::run(&storage, "test", Some(schema_json), false, None, false).unwrap();

        // Manually set inferred=true so evolution is enabled
        let mut config = storage.load_config("test").unwrap();
        config.inferred = true;
        storage.save_config("test", &config).unwrap();

        // Index doc with "notes" (declared) + "name" (to be inferred)
        let ndjson = dir.path().join("data.ndjson");
        std::fs::write(
            &ndjson,
            r#"{"_id":"d1","name":"glucose","notes":"fasting blood sample"}"#,
        )
        .unwrap();
        run(&storage, "test", Some(ndjson.to_str().unwrap())).unwrap();

        let config = storage.load_config("test").unwrap();
        assert_eq!(config.schema.fields["notes"], FieldType::Text);
        assert_eq!(config.schema.fields["name"], FieldType::Keyword);
    }
}

use tantivy::schema::Field;
use tantivy::{DateTime as TantivyDateTime, IndexWriter, TantivyDocument, Term};
use uuid::Uuid;

use crate::error::{Result, SearchDbError};
use crate::schema::{FieldType, Schema};

/// Extract or generate a document ID.
///
/// If the JSON object has an `_id` field, use its string value.
/// Otherwise generate a UUID v4 (append-only, no upsert identity).
pub fn make_doc_id(doc: &serde_json::Value) -> String {
    doc.get("_id")
        .and_then(|v| match v {
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Number(n) => Some(n.to_string()),
            _ => None,
        })
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

/// Add a single typed value to a tantivy document field.
///
/// Dispatches on the field type: text for keyword/text, f64 for numeric, date for date.
fn add_typed_value(
    tdoc: &mut TantivyDocument,
    field: tantivy::schema::Field,
    field_name: &str,
    field_type: &FieldType,
    value: &serde_json::Value,
) -> Result<()> {
    match field_type {
        FieldType::Keyword | FieldType::Text => {
            let text = match value {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            tdoc.add_text(field, &text);
        }
        FieldType::Numeric => {
            let num = value.as_f64().ok_or_else(|| {
                SearchDbError::Schema(format!(
                    "field '{field_name}' expected numeric, got {value}"
                ))
            })?;
            tdoc.add_f64(field, num);
        }
        FieldType::Date => {
            let date_str = value.as_str().ok_or_else(|| {
                SearchDbError::Schema(format!(
                    "field '{field_name}' expected date string, got {value}"
                ))
            })?;
            let parsed = parse_date(date_str)?;
            tdoc.add_date(field, parsed);
        }
    }
    Ok(())
}

/// Resolve a dot-notation path in a JSON value.
/// Example: resolve_path(doc, "user.name") -> Some(&Value::String("alice"))
fn resolve_path<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    // Don't return nested objects — they should be traversed, not indexed
    if current.is_object() {
        return None;
    }
    Some(current)
}

/// Build a tantivy `TantivyDocument` from a JSON object.
///
/// Populates:
/// - `_id` — document identity (from doc or generated UUID)
/// - `_source` — verbatim JSON for round-trip retrieval
/// - `__present__` — tokens: `__all__` (every doc) + each non-null field name
/// - User fields — type-dispatched based on schema
///
/// Supports dot-notation field names (e.g. `user.name`) by resolving paths
/// through nested JSON objects.
pub fn build_document(
    tantivy_schema: &tantivy::schema::Schema,
    app_schema: &Schema,
    doc_json: &serde_json::Value,
    doc_id: &str,
) -> Result<TantivyDocument> {
    let mut tdoc = TantivyDocument::new();

    let id_field = tantivy_schema
        .get_field("_id")
        .map_err(|_| SearchDbError::Schema("missing _id field".into()))?;
    let source_field = tantivy_schema
        .get_field("_source")
        .map_err(|_| SearchDbError::Schema("missing _source field".into()))?;
    let present_field = tantivy_schema
        .get_field("__present__")
        .map_err(|_| SearchDbError::Schema("missing __present__ field".into()))?;

    // System fields
    tdoc.add_text(id_field, doc_id);
    tdoc.add_text(source_field, doc_json.to_string());
    tdoc.add_text(present_field, "__all__");

    // Verify input is a JSON object
    let _obj = doc_json
        .as_object()
        .ok_or_else(|| SearchDbError::Schema("document must be a JSON object".into()))?;

    // User fields — type-dispatch (supports dot-notation paths)
    for (field_name, field_type) in &app_schema.fields {
        let value = match resolve_path(doc_json, field_name) {
            Some(serde_json::Value::Null) | None => continue,
            Some(v) => v,
        };

        let field = tantivy_schema.get_field(field_name).map_err(|_| {
            SearchDbError::Schema(format!("field '{field_name}' not in tantivy schema"))
        })?;

        // Collect values: arrays produce multiple values, scalars produce one
        let values: Vec<&serde_json::Value> = match value {
            serde_json::Value::Array(arr) => arr.iter().collect(),
            scalar => vec![scalar],
        };

        for val in &values {
            if val.is_null() {
                continue;
            }
            add_typed_value(&mut tdoc, field, field_name, field_type, val)?;
        }

        // Track non-null field in __present__ (once, not per element)
        tdoc.add_text(present_field, field_name);
    }

    Ok(tdoc)
}

/// Upsert a document: delete any existing doc with the same _id, then add the new one.
pub fn upsert_document(writer: &IndexWriter, id_field: Field, doc: TantivyDocument, doc_id: &str) {
    let term = Term::from_field_text(id_field, doc_id);
    writer.delete_term(term);
    writer
        .add_document(doc)
        .expect("add_document should not fail");
}

/// Delete documents by their `_id` values.
///
/// Issues a `delete_term` for each ID. Deletions take effect on the next commit.
/// Returns the number of delete operations issued (actual doc removals depend
/// on whether matching docs exist in the index).
pub fn delete_documents(writer: &IndexWriter, id_field: Field, ids: &[String]) -> usize {
    let mut count = 0;
    for id in ids {
        let term = Term::from_field_text(id_field, id);
        writer.delete_term(term);
        count += 1;
    }
    count
}

/// Parse a date string into a tantivy DateTime.
///
/// Supports ISO 8601 (YYYY-MM-DDThh:mm:ss...) and compact YYYYMMDD format.
fn parse_date(s: &str) -> Result<TantivyDateTime> {
    // YYYYMMDD compact format
    if s.len() == 8 && s.chars().all(|c| c.is_ascii_digit()) {
        let naive = chrono::NaiveDate::parse_from_str(s, "%Y%m%d")
            .map_err(|e| SearchDbError::Schema(format!("invalid YYYYMMDD date '{s}': {e}")))?;
        let dt = naive
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| SearchDbError::Schema(format!("invalid date '{s}'")))?
            .and_utc();
        return Ok(TantivyDateTime::from_timestamp_micros(
            dt.timestamp_micros(),
        ));
    }

    // ISO 8601 — normalize "Z" suffix to "+00:00" for chrono parsing
    let normalized = s.replace('Z', "+00:00");
    let dt = chrono::DateTime::parse_from_rfc3339(&normalized)
        .map_err(|e| SearchDbError::Schema(format!("invalid date '{s}': {e}")))?;
    let utc = dt.with_timezone(&chrono::Utc);
    Ok(TantivyDateTime::from_timestamp_micros(
        utc.timestamp_micros(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Schema;
    use std::collections::BTreeMap;
    use tantivy::schema::Value;

    fn test_schema() -> Schema {
        Schema {
            fields: BTreeMap::from([
                ("name".into(), FieldType::Keyword),
                ("notes".into(), FieldType::Text),
                ("age".into(), FieldType::Numeric),
                ("born".into(), FieldType::Date),
            ]),
        }
    }

    #[test]
    fn test_make_doc_id_from_string() {
        let doc = serde_json::json!({"_id": "abc123"});
        assert_eq!(make_doc_id(&doc), "abc123");
    }

    #[test]
    fn test_make_doc_id_from_number() {
        let doc = serde_json::json!({"_id": 42});
        assert_eq!(make_doc_id(&doc), "42");
    }

    #[test]
    fn test_make_doc_id_generates_uuid() {
        let doc = serde_json::json!({"name": "test"});
        let id = make_doc_id(&doc);
        assert!(!id.is_empty());
        assert!(uuid::Uuid::parse_str(&id).is_ok());
    }

    #[test]
    fn test_build_document_all_field_types() {
        let schema = test_schema();
        let tv_schema = schema.build_tantivy_schema();

        let doc_json = serde_json::json!({
            "_id": "d1",
            "name": "glucose",
            "notes": "fasting blood sample",
            "age": 45.0,
            "born": "1980-01-15T00:00:00Z"
        });

        let doc = build_document(&tv_schema, &schema, &doc_json, "d1").unwrap();

        // Verify _id field
        let id_field = tv_schema.get_field("_id").unwrap();
        let id_values: Vec<&str> = doc.get_all(id_field).flat_map(|v| v.as_str()).collect();
        assert_eq!(id_values, vec!["d1"]);

        // Verify _source contains original JSON
        let source_field = tv_schema.get_field("_source").unwrap();
        let source_values: Vec<&str> = doc.get_all(source_field).flat_map(|v| v.as_str()).collect();
        assert_eq!(source_values.len(), 1);
        let parsed: serde_json::Value = serde_json::from_str(source_values[0]).unwrap();
        assert_eq!(parsed["name"], "glucose");

        // Verify __present__ has __all__ + 4 field names
        let present_field = tv_schema.get_field("__present__").unwrap();
        let present_values: Vec<&str> = doc
            .get_all(present_field)
            .flat_map(|v| v.as_str())
            .collect();
        assert!(present_values.contains(&"__all__"));
        assert!(present_values.contains(&"name"));
        assert!(present_values.contains(&"notes"));
        assert!(present_values.contains(&"age"));
        assert!(present_values.contains(&"born"));
    }

    #[test]
    fn test_build_document_null_fields_excluded_from_present() {
        let schema = test_schema();
        let tv_schema = schema.build_tantivy_schema();

        let doc_json = serde_json::json!({
            "_id": "d2",
            "name": "a1c"
        });

        let doc = build_document(&tv_schema, &schema, &doc_json, "d2").unwrap();

        let present_field = tv_schema.get_field("__present__").unwrap();
        let present_values: Vec<&str> = doc
            .get_all(present_field)
            .flat_map(|v| v.as_str())
            .collect();
        assert!(present_values.contains(&"__all__"));
        assert!(present_values.contains(&"name"));
        assert!(!present_values.contains(&"notes"));
        assert!(!present_values.contains(&"age"));
        assert!(!present_values.contains(&"born"));
    }

    #[test]
    fn test_upsert_replaces_document() {
        let schema = Schema {
            fields: BTreeMap::from([("name".into(), FieldType::Keyword)]),
        };
        let tv_schema = schema.build_tantivy_schema();

        let dir = tempfile::tempdir().unwrap();
        let index = tantivy::Index::create_in_dir(dir.path(), tv_schema.clone()).unwrap();
        let mut writer = index.writer(50_000_000).unwrap();
        let id_field = tv_schema.get_field("_id").unwrap();

        // Insert first version
        let doc1 = build_document(
            &tv_schema,
            &schema,
            &serde_json::json!({"_id": "d1", "name": "original"}),
            "d1",
        )
        .unwrap();
        upsert_document(&writer, id_field, doc1, "d1");
        writer.commit().unwrap();

        // Upsert with new version
        let doc2 = build_document(
            &tv_schema,
            &schema,
            &serde_json::json!({"_id": "d1", "name": "updated"}),
            "d1",
        )
        .unwrap();
        upsert_document(&writer, id_field, doc2, "d1");
        writer.commit().unwrap();

        // Verify only one doc with updated value
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 1);

        // Search for the doc to verify content
        let source_field = tv_schema.get_field("_source").unwrap();
        let collected = searcher
            .search(
                &tantivy::query::AllQuery,
                &tantivy::collector::TopDocs::with_limit(10),
            )
            .unwrap();
        assert_eq!(collected.len(), 1);
        let doc: TantivyDocument = searcher.doc(collected[0].1).unwrap();
        let source = doc.get_first(source_field).unwrap().as_str().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(source).unwrap();
        assert_eq!(parsed["name"], "updated");
    }

    #[test]
    fn test_build_document_boolean_as_keyword() {
        let schema = Schema {
            fields: BTreeMap::from([("active".into(), FieldType::Keyword)]),
        };
        let tv_schema = schema.build_tantivy_schema();

        let doc_json = serde_json::json!({"_id": "d1", "active": true});
        let doc = build_document(&tv_schema, &schema, &doc_json, "d1").unwrap();

        let field = tv_schema.get_field("active").unwrap();
        let values: Vec<&str> = doc.get_all(field).flat_map(|v| v.as_str()).collect();
        assert_eq!(values, vec!["true"]);
    }

    #[test]
    fn test_build_document_keyword_array() {
        let schema = Schema {
            fields: BTreeMap::from([("tags".into(), FieldType::Keyword)]),
        };
        let tv_schema = schema.build_tantivy_schema();

        let doc_json = serde_json::json!({"_id": "d1", "tags": ["urgent", "lab", "review"]});
        let doc = build_document(&tv_schema, &schema, &doc_json, "d1").unwrap();

        let field = tv_schema.get_field("tags").unwrap();
        let values: Vec<&str> = doc.get_all(field).flat_map(|v| v.as_str()).collect();
        assert_eq!(values, vec!["urgent", "lab", "review"]);
    }

    #[test]
    fn test_build_document_numeric_array() {
        let schema = Schema {
            fields: BTreeMap::from([("scores".into(), FieldType::Numeric)]),
        };
        let tv_schema = schema.build_tantivy_schema();

        let doc_json = serde_json::json!({"_id": "d1", "scores": [90.0, 85.5, 92.0]});
        let doc = build_document(&tv_schema, &schema, &doc_json, "d1").unwrap();

        let field = tv_schema.get_field("scores").unwrap();
        let values: Vec<f64> = doc.get_all(field).flat_map(|v| v.as_f64()).collect();
        assert_eq!(values, vec![90.0, 85.5, 92.0]);
    }

    #[test]
    fn test_build_document_date_array() {
        let schema = Schema {
            fields: BTreeMap::from([("dates".into(), FieldType::Date)]),
        };
        let tv_schema = schema.build_tantivy_schema();

        let doc_json = serde_json::json!({
            "_id": "d1",
            "dates": ["2024-01-15T10:00:00Z", "2024-02-20T14:00:00Z"]
        });
        let doc = build_document(&tv_schema, &schema, &doc_json, "d1").unwrap();

        let field = tv_schema.get_field("dates").unwrap();
        let values: Vec<_> = doc.get_all(field).collect();
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn test_build_document_yyyymmdd_date() {
        let schema = Schema {
            fields: BTreeMap::from([("born".into(), FieldType::Date)]),
        };
        let tv_schema = schema.build_tantivy_schema();

        let dir = tempfile::tempdir().unwrap();
        let index = tantivy::Index::create_in_dir(dir.path(), tv_schema.clone()).unwrap();
        let mut writer = index.writer(50_000_000).unwrap();
        let id_field = tv_schema.get_field("_id").unwrap();

        let doc_json = serde_json::json!({"_id": "d1", "born": "20260315"});
        let doc = build_document(&tv_schema, &schema, &doc_json, "d1").unwrap();
        upsert_document(&writer, id_field, doc, "d1");
        writer.commit().unwrap();

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 1);

        // Verify the date was indexed (stored in _source)
        let source_field = tv_schema.get_field("_source").unwrap();
        let collected = searcher
            .search(
                &tantivy::query::AllQuery,
                &tantivy::collector::TopDocs::with_limit(10),
            )
            .unwrap();
        assert_eq!(collected.len(), 1);
        let tdoc: TantivyDocument = searcher.doc(collected[0].1).unwrap();
        let source = tdoc.get_first(source_field).unwrap().as_str().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(source).unwrap();
        assert_eq!(parsed["born"], "20260315");

        // Verify the date field was actually populated
        let born_field = tv_schema.get_field("born").unwrap();
        let date_values: Vec<_> = tdoc.get_all(born_field).collect();
        assert_eq!(date_values.len(), 1);
    }

    #[test]
    fn test_build_document_array_with_nulls() {
        let schema = Schema {
            fields: BTreeMap::from([("tags".into(), FieldType::Keyword)]),
        };
        let tv_schema = schema.build_tantivy_schema();

        let doc_json = serde_json::json!({"_id": "d1", "tags": [null, "urgent", null, "lab"]});
        let doc = build_document(&tv_schema, &schema, &doc_json, "d1").unwrap();

        let field = tv_schema.get_field("tags").unwrap();
        let values: Vec<&str> = doc.get_all(field).flat_map(|v| v.as_str()).collect();
        assert_eq!(values, vec!["urgent", "lab"]);
    }

    #[test]
    fn test_build_document_array_present_added_once() {
        let schema = Schema {
            fields: BTreeMap::from([("tags".into(), FieldType::Keyword)]),
        };
        let tv_schema = schema.build_tantivy_schema();

        let doc_json = serde_json::json!({"_id": "d1", "tags": ["a", "b", "c"]});
        let doc = build_document(&tv_schema, &schema, &doc_json, "d1").unwrap();

        let present_field = tv_schema.get_field("__present__").unwrap();
        let present_values: Vec<&str> = doc
            .get_all(present_field)
            .flat_map(|v| v.as_str())
            .collect();
        // "tags" should appear exactly once in __present__
        let tag_count = present_values.iter().filter(|&&v| v == "tags").count();
        assert_eq!(tag_count, 1);
        assert!(present_values.contains(&"__all__"));
    }

    #[test]
    fn test_build_document_array_source_preserves_array() {
        let schema = Schema {
            fields: BTreeMap::from([("tags".into(), FieldType::Keyword)]),
        };
        let tv_schema = schema.build_tantivy_schema();

        let doc_json = serde_json::json!({"_id": "d1", "tags": ["urgent", "lab"]});
        let doc = build_document(&tv_schema, &schema, &doc_json, "d1").unwrap();

        let source_field = tv_schema.get_field("_source").unwrap();
        let source = doc.get_first(source_field).unwrap().as_str().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(source).unwrap();
        assert_eq!(parsed["tags"], serde_json::json!(["urgent", "lab"]));
    }

    #[test]
    fn test_array_field_searchable() {
        let schema = Schema {
            fields: BTreeMap::from([("tags".into(), FieldType::Keyword)]),
        };
        let tv_schema = schema.build_tantivy_schema();

        let dir = tempfile::tempdir().unwrap();
        let index = tantivy::Index::create_in_dir(dir.path(), tv_schema.clone()).unwrap();
        let mut writer = index.writer(50_000_000).unwrap();
        let id_field = tv_schema.get_field("_id").unwrap();

        let doc_json = serde_json::json!({"_id": "d1", "tags": ["urgent", "lab"]});
        let doc = build_document(&tv_schema, &schema, &doc_json, "d1").unwrap();
        upsert_document(&writer, id_field, doc, "d1");
        writer.commit().unwrap();

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        let tags_field = tv_schema.get_field("tags").unwrap();
        let parser = tantivy::query::QueryParser::for_index(&index, vec![tags_field]);

        // Search for "urgent" — should find the doc
        let query = parser.parse_query(r#"tags:"urgent""#).unwrap();
        let results = searcher
            .search(&query, &tantivy::collector::TopDocs::with_limit(10))
            .unwrap();
        assert_eq!(results.len(), 1);

        // Search for "lab" — should also find the doc
        let query = parser.parse_query(r#"tags:"lab""#).unwrap();
        let results = searcher
            .search(&query, &tantivy::collector::TopDocs::with_limit(10))
            .unwrap();
        assert_eq!(results.len(), 1);

        // Search for "nonexistent" — should not find the doc
        let query = parser.parse_query(r#"tags:"nonexistent""#).unwrap();
        let results = searcher
            .search(&query, &tantivy::collector::TopDocs::with_limit(10))
            .unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_build_document_nested_objects() {
        let schema = Schema {
            fields: BTreeMap::from([
                ("user.name".into(), FieldType::Keyword),
                ("user.age".into(), FieldType::Numeric),
                ("status".into(), FieldType::Keyword),
            ]),
        };
        let tv_schema = schema.build_tantivy_schema();
        let doc = serde_json::json!({
            "user": {"name": "alice", "age": 30},
            "status": "active"
        });
        let result = build_document(&tv_schema, &schema, &doc, "test-id");
        assert!(
            result.is_ok(),
            "build_document should handle nested objects"
        );

        let tdoc = result.unwrap();
        let name_field = tv_schema.get_field("user.name").unwrap();
        let values: Vec<&str> = tdoc.get_all(name_field).flat_map(|v| v.as_str()).collect();
        assert_eq!(values, vec!["alice"]);

        let age_field = tv_schema.get_field("user.age").unwrap();
        let values: Vec<f64> = tdoc.get_all(age_field).flat_map(|v| v.as_f64()).collect();
        assert_eq!(values, vec![30.0]);

        let status_field = tv_schema.get_field("status").unwrap();
        let values: Vec<&str> = tdoc
            .get_all(status_field)
            .flat_map(|v| v.as_str())
            .collect();
        assert_eq!(values, vec!["active"]);

        // Verify __present__ has the flattened field names
        let present_field = tv_schema.get_field("__present__").unwrap();
        let present: Vec<&str> = tdoc
            .get_all(present_field)
            .flat_map(|v| v.as_str())
            .collect();
        assert!(present.contains(&"user.name"));
        assert!(present.contains(&"user.age"));
        assert!(present.contains(&"status"));
    }

    #[test]
    fn test_build_document_deeply_nested() {
        let schema = Schema {
            fields: BTreeMap::from([("a.b.c".into(), FieldType::Keyword)]),
        };
        let tv_schema = schema.build_tantivy_schema();
        let doc = serde_json::json!({"a": {"b": {"c": "deep"}}});
        let result = build_document(&tv_schema, &schema, &doc, "test-id");
        assert!(result.is_ok());

        let tdoc = result.unwrap();
        let field = tv_schema.get_field("a.b.c").unwrap();
        let values: Vec<&str> = tdoc.get_all(field).flat_map(|v| v.as_str()).collect();
        assert_eq!(values, vec!["deep"]);
    }

    #[test]
    fn test_nested_object_search_end_to_end() {
        let schema = Schema {
            fields: BTreeMap::from([
                ("user.name".into(), FieldType::Keyword),
                ("user.age".into(), FieldType::Numeric),
                ("status".into(), FieldType::Keyword),
            ]),
        };
        let tv_schema = schema.build_tantivy_schema();

        let dir = tempfile::tempdir().unwrap();
        let index = tantivy::Index::create_in_dir(dir.path(), tv_schema.clone()).unwrap();
        let mut writer = index.writer(50_000_000).unwrap();
        let id_field = tv_schema.get_field("_id").unwrap();

        // Index nested documents
        let doc1 = serde_json::json!({
            "_id": "1",
            "user": {"name": "alice", "age": 30},
            "status": "active"
        });
        let doc2 = serde_json::json!({
            "_id": "2",
            "user": {"name": "bob", "age": 25},
            "status": "inactive"
        });
        let tdoc1 = build_document(&tv_schema, &schema, &doc1, "1").unwrap();
        upsert_document(&writer, id_field, tdoc1, "1");
        let tdoc2 = build_document(&tv_schema, &schema, &doc2, "2").unwrap();
        upsert_document(&writer, id_field, tdoc2, "2");
        writer.commit().unwrap();

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 2);

        // Search by nested field using query string
        let name_field = tv_schema.get_field("user.name").unwrap();
        let parser = tantivy::query::QueryParser::for_index(&index, vec![name_field]);
        let query = parser.parse_query(r#"user.name:"alice""#).unwrap();
        let results = searcher
            .search(&query, &tantivy::collector::TopDocs::with_limit(10))
            .unwrap();
        assert_eq!(results.len(), 1);

        // Verify the result is the correct doc
        let source_field = tv_schema.get_field("_source").unwrap();
        let doc: TantivyDocument = searcher.doc(results[0].1).unwrap();
        let source = doc.get_first(source_field).unwrap().as_str().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(source).unwrap();
        assert_eq!(parsed.get("_id").and_then(|v| v.as_str()), Some("1"));
    }
}

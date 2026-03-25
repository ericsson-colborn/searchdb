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

/// Build a tantivy `TantivyDocument` from a JSON object.
///
/// Populates:
/// - `_id` — document identity (from doc or generated UUID)
/// - `_source` — verbatim JSON for round-trip retrieval
/// - `__present__` — tokens: `__all__` (every doc) + each non-null field name
/// - User fields — type-dispatched based on schema
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

    // User fields — type-dispatch
    let obj = doc_json
        .as_object()
        .ok_or_else(|| SearchDbError::Schema("document must be a JSON object".into()))?;

    for (field_name, field_type) in &app_schema.fields {
        let value = match obj.get(field_name) {
            Some(serde_json::Value::Null) | None => continue,
            Some(v) => v,
        };

        let field = tantivy_schema.get_field(field_name).map_err(|_| {
            SearchDbError::Schema(format!("field '{field_name}' not in tantivy schema"))
        })?;

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

        // Track non-null field in __present__
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

/// Parse an ISO 8601 date string into a tantivy DateTime.
fn parse_date(s: &str) -> Result<TantivyDateTime> {
    // Normalize "Z" suffix to "+00:00" for chrono parsing
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
}

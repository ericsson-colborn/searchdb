use tantivy::collector::TopDocs;
use tantivy::directory::RamDirectory;
use tantivy::query::QueryParser;
use tantivy::schema::Value;
use tantivy::{Index, TantivyDocument};

use crate::error::{Result, SearchDbError};
use crate::schema::Schema;

/// A single search hit — parsed from _source with metadata.
#[derive(Debug)]
#[allow(dead_code)]
pub struct SearchHit {
    pub doc: serde_json::Value,
    pub score: f32,
}

/// Execute a query string search against a tantivy index.
///
/// Uses tantivy's QueryParser with all user fields + system fields as defaults.
/// Returns results from `_source` with optional field projection.
pub fn search(
    index: &Index,
    app_schema: &Schema,
    query_str: &str,
    limit: usize,
    offset: usize,
    fields: Option<&[String]>,
    include_score: bool,
) -> Result<Vec<SearchHit>> {
    let tv_schema = index.schema();
    let reader = index
        .reader()
        .map_err(|e| SearchDbError::Schema(format!("failed to open reader: {e}")))?;
    let searcher = reader.searcher();

    // Default fields for the query parser: all user fields + _id + __present__
    let mut default_fields = vec![];
    for field_name in app_schema.fields.keys() {
        if let Ok(f) = tv_schema.get_field(field_name) {
            default_fields.push(f);
        }
    }
    if let Ok(f) = tv_schema.get_field("_id") {
        default_fields.push(f);
    }
    if let Ok(f) = tv_schema.get_field("__present__") {
        default_fields.push(f);
    }

    let parser = QueryParser::for_index(index, default_fields);
    let query = parser
        .parse_query(query_str)
        .map_err(|e| SearchDbError::Schema(format!("query parse failed: {e}")))?;

    // Fetch limit+offset results, then skip the first `offset`
    let top_docs = searcher.search(&query, &TopDocs::with_limit(limit + offset))?;

    let source_field = tv_schema
        .get_field("_source")
        .map_err(|_| SearchDbError::Schema("missing _source field".into()))?;
    let id_field = tv_schema
        .get_field("_id")
        .map_err(|_| SearchDbError::Schema("missing _id field".into()))?;

    let mut results = Vec::new();
    for (score, doc_address) in top_docs.into_iter().skip(offset) {
        let doc: TantivyDocument = searcher.doc(doc_address)?;
        let hit = doc_to_hit(&doc, source_field, id_field, score, fields, include_score)?;
        results.push(hit);
    }

    Ok(results)
}

/// Look up a single document by `_id`.
///
/// Returns `None` if no document matches.
pub fn get_by_id(index: &Index, doc_id: &str) -> Result<Option<serde_json::Value>> {
    let tv_schema = index.schema();
    let reader = index
        .reader()
        .map_err(|e| SearchDbError::Schema(format!("failed to open reader: {e}")))?;
    let searcher = reader.searcher();

    let id_field = tv_schema
        .get_field("_id")
        .map_err(|_| SearchDbError::Schema("missing _id field".into()))?;
    let source_field = tv_schema
        .get_field("_source")
        .map_err(|_| SearchDbError::Schema("missing _source field".into()))?;

    // Build a term query for exact _id match
    let term = tantivy::Term::from_field_text(id_field, doc_id);
    let query = tantivy::query::TermQuery::new(term, tantivy::schema::IndexRecordOption::Basic);

    let top_docs = searcher.search(&query, &TopDocs::with_limit(1))?;

    match top_docs.first() {
        Some((_score, doc_address)) => {
            let doc: TantivyDocument = searcher.doc(*doc_address)?;
            let source_str = doc
                .get_first(source_field)
                .and_then(|v| v.as_str())
                .ok_or_else(|| SearchDbError::Schema("document missing _source".into()))?;
            let mut parsed: serde_json::Value = serde_json::from_str(source_str)?;

            // Ensure _id is present in output
            if let Some(obj) = parsed.as_object_mut() {
                let id_val = doc
                    .get_first(id_field)
                    .and_then(|v| v.as_str())
                    .unwrap_or(doc_id);
                obj.insert(
                    "_id".to_string(),
                    serde_json::Value::String(id_val.to_string()),
                );
            }

            Ok(Some(parsed))
        }
        None => Ok(None),
    }
}

/// Two-tier search: persistent index + ephemeral gap index, dedup by _id.
///
/// Gap rows win over persistent index rows for the same _id (newer data).
/// Gap hits appear first (newer), then persistent hits by score.
/// NOTE: BM25 scores are not comparable across indexes (different corpus stats),
/// so we don't merge by score across tiers.
#[allow(clippy::too_many_arguments)]
pub fn search_with_gap(
    persistent_index: &Index,
    app_schema: &Schema,
    query_str: &str,
    limit: usize,
    offset: usize,
    fields: Option<&[String]>,
    include_score: bool,
    gap_rows: &[serde_json::Value],
) -> Result<Vec<SearchHit>> {
    if gap_rows.is_empty() {
        return search(
            persistent_index,
            app_schema,
            query_str,
            limit,
            offset,
            fields,
            include_score,
        );
    }

    // 1. Build ephemeral index from gap rows
    let gap_index = build_ephemeral_index(app_schema, gap_rows)?;

    // 2. Search the gap index (collect all — gap is small by design)
    let gap_hits = search(
        &gap_index,
        app_schema,
        query_str,
        gap_rows.len(),
        0,
        fields,
        include_score,
    )?;

    // 3. Collect gap _ids for dedup
    let gap_ids: std::collections::HashSet<String> = gap_hits
        .iter()
        .filter_map(|h| {
            h.doc
                .get("_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
        .collect();

    // 4. Search persistent index, excluding gap _ids
    let persistent_hits = search(
        persistent_index,
        app_schema,
        query_str,
        limit + offset,
        0,
        fields,
        include_score,
    )?;

    let filtered_persistent: Vec<SearchHit> = persistent_hits
        .into_iter()
        .filter(|h| {
            h.doc
                .get("_id")
                .and_then(|v| v.as_str())
                .map(|id| !gap_ids.contains(id))
                .unwrap_or(true)
        })
        .collect();

    // 5. Gap hits first (newer data), then persistent hits (by score), paginate
    let mut merged = gap_hits;
    merged.extend(filtered_persistent);

    let paginated: Vec<SearchHit> = merged.into_iter().skip(offset).take(limit).collect();

    Ok(paginated)
}

/// Build a temporary in-memory tantivy index from JSON rows.
///
/// Used for searching un-indexed Delta gap rows with full query syntax
/// and proper BM25 scoring. The index lives in RamDirectory and is
/// dropped when the caller discards it.
pub fn build_ephemeral_index(app_schema: &Schema, rows: &[serde_json::Value]) -> Result<Index> {
    let tv_schema = app_schema.build_tantivy_schema();
    let dir = RamDirectory::create();
    let index = Index::create(dir, tv_schema.clone(), tantivy::IndexSettings::default())?;
    let mut writer = index.writer(15_000_000)?; // Tantivy minimum is 15MB

    let id_field = tv_schema
        .get_field("_id")
        .map_err(|_| SearchDbError::Schema("missing _id field".into()))?;

    for row in rows {
        let doc_id = crate::writer::make_doc_id(row);
        let doc = crate::writer::build_document(&tv_schema, app_schema, row, &doc_id)?;
        crate::writer::upsert_document(&writer, id_field, doc, &doc_id);
    }

    writer.commit()?;
    Ok(index)
}

/// Convert a tantivy document to a SearchHit using _source.
fn doc_to_hit(
    doc: &TantivyDocument,
    source_field: tantivy::schema::Field,
    id_field: tantivy::schema::Field,
    score: f32,
    fields: Option<&[String]>,
    include_score: bool,
) -> Result<SearchHit> {
    let source_str = doc
        .get_first(source_field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| SearchDbError::Schema("document missing _source".into()))?;

    let mut parsed: serde_json::Value = serde_json::from_str(source_str)?;

    // Apply field projection
    if let Some(field_list) = fields {
        if let Some(obj) = parsed.as_object() {
            let projected: serde_json::Map<String, serde_json::Value> = obj
                .iter()
                .filter(|(k, _)| field_list.iter().any(|f| f == *k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            parsed = serde_json::Value::Object(projected);
        }
    }

    // Always include _id in output
    if let Some(obj) = parsed.as_object_mut() {
        if !obj.contains_key("_id") {
            if let Some(id_val) = doc.get_first(id_field).and_then(|v| v.as_str()) {
                obj.insert(
                    "_id".to_string(),
                    serde_json::Value::String(id_val.to_string()),
                );
            }
        }
        if include_score {
            obj.insert(
                "_score".to_string(),
                serde_json::Value::Number(serde_json::Number::from_f64(score as f64).unwrap()),
            );
        }
    }

    Ok(SearchHit { doc: parsed, score })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{FieldType, Schema};
    use crate::writer;
    use std::collections::BTreeMap;

    fn setup_test_index(dir: &std::path::Path) -> (Index, Schema, tantivy::schema::Schema) {
        let schema = Schema {
            fields: BTreeMap::from([
                ("name".into(), FieldType::Keyword),
                ("notes".into(), FieldType::Text),
            ]),
        };
        let tv_schema = schema.build_tantivy_schema();
        let index = Index::create_in_dir(dir, tv_schema.clone()).unwrap();
        let mut w = index.writer(50_000_000).unwrap();
        let id_field = tv_schema.get_field("_id").unwrap();

        let docs = vec![
            serde_json::json!({"_id": "d1", "name": "glucose", "notes": "fasting blood sample"}),
            serde_json::json!({"_id": "d2", "name": "a1c", "notes": "borderline diabetic"}),
            serde_json::json!({"_id": "d3", "name": "glucose", "notes": "postprandial check"}),
        ];

        for doc_json in &docs {
            let doc_id = writer::make_doc_id(doc_json);
            let doc = writer::build_document(&tv_schema, &schema, doc_json, &doc_id).unwrap();
            writer::upsert_document(&w, id_field, doc, &doc_id);
        }
        w.commit().unwrap();

        (index, schema, tv_schema)
    }

    #[test]
    fn test_keyword_exact_match() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_test_index(dir.path());

        let results = search(&index, &schema, r#"+name:"glucose""#, 10, 0, None, false).unwrap();
        assert_eq!(results.len(), 2);
        for hit in &results {
            assert_eq!(hit.doc["name"], "glucose");
        }
    }

    #[test]
    fn test_text_stemmed_search() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_test_index(dir.path());

        // "diabetes" should match "diabetic" via en_stem tokenizer
        let results = search(&index, &schema, "notes:diabetes", 10, 0, None, false).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc["_id"], "d2");
    }

    #[test]
    fn test_get_by_id() {
        let dir = tempfile::tempdir().unwrap();
        let (index, _, _) = setup_test_index(dir.path());

        let doc = get_by_id(&index, "d1").unwrap();
        assert!(doc.is_some());
        let doc = doc.unwrap();
        assert_eq!(doc["_id"], "d1");
        assert_eq!(doc["name"], "glucose");
    }

    #[test]
    fn test_get_missing_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let (index, _, _) = setup_test_index(dir.path());

        let doc = get_by_id(&index, "nonexistent").unwrap();
        assert!(doc.is_none());
    }

    #[test]
    fn test_search_with_field_projection() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_test_index(dir.path());

        let fields = vec!["name".to_string()];
        let results = search(
            &index,
            &schema,
            r#"+name:"glucose""#,
            10,
            0,
            Some(&fields),
            false,
        )
        .unwrap();
        assert_eq!(results.len(), 2);
        for hit in &results {
            assert!(hit.doc.get("name").is_some());
            assert!(hit.doc.get("notes").is_none());
            // _id is always included
            assert!(hit.doc.get("_id").is_some());
        }
    }

    #[test]
    fn test_search_with_score() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_test_index(dir.path());

        let results = search(&index, &schema, "notes:blood", 10, 0, None, true).unwrap();
        assert!(!results.is_empty());
        for hit in &results {
            assert!(hit.doc.get("_score").is_some());
        }
    }

    #[test]
    fn test_search_limit_and_offset() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_test_index(dir.path());

        // All docs match __present__:__all__
        let all = search(&index, &schema, "+__present__:__all__", 10, 0, None, false).unwrap();
        assert_eq!(all.len(), 3);

        let limited = search(&index, &schema, "+__present__:__all__", 2, 0, None, false).unwrap();
        assert_eq!(limited.len(), 2);

        let offset = search(&index, &schema, "+__present__:__all__", 10, 2, None, false).unwrap();
        assert_eq!(offset.len(), 1);
    }

    #[test]
    fn test_build_ephemeral_index() {
        let schema = Schema {
            fields: BTreeMap::from([
                ("name".into(), FieldType::Keyword),
                ("notes".into(), FieldType::Text),
            ]),
        };

        let rows = vec![
            serde_json::json!({"_id": "d1", "name": "glucose", "notes": "fasting sample"}),
            serde_json::json!({"_id": "d2", "name": "a1c", "notes": "borderline diabetic"}),
        ];

        let index = build_ephemeral_index(&schema, &rows).unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 2);
    }

    #[test]
    fn test_ephemeral_index_supports_query() {
        let schema = Schema {
            fields: BTreeMap::from([
                ("name".into(), FieldType::Keyword),
                ("notes".into(), FieldType::Text),
            ]),
        };

        let rows = vec![
            serde_json::json!({"_id": "d1", "name": "glucose", "notes": "fasting sample"}),
            serde_json::json!({"_id": "d2", "name": "a1c", "notes": "borderline diabetic"}),
        ];

        let index = build_ephemeral_index(&schema, &rows).unwrap();
        let results = search(&index, &schema, "notes:diabetes", 10, 0, None, false).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc["_id"], "d2");
    }

    #[test]
    fn test_search_with_gap_dedup() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Schema {
            fields: BTreeMap::from([
                ("name".into(), FieldType::Keyword),
                ("notes".into(), FieldType::Text),
            ]),
        };
        let tv_schema = schema.build_tantivy_schema();
        let index = Index::create_in_dir(dir.path(), tv_schema.clone()).unwrap();
        let mut w = index.writer(50_000_000).unwrap();
        let id_field = tv_schema.get_field("_id").unwrap();

        for doc_json in &[
            serde_json::json!({"_id": "d1", "name": "glucose", "notes": "old version"}),
            serde_json::json!({"_id": "d2", "name": "a1c", "notes": "unchanged"}),
        ] {
            let doc_id = writer::make_doc_id(doc_json);
            let doc =
                writer::build_document(&tv_schema, &schema, doc_json, &doc_id).unwrap();
            writer::upsert_document(&w, id_field, doc, &doc_id);
        }
        w.commit().unwrap();

        // Gap has updated d1 + new d3
        let gap_rows = vec![
            serde_json::json!({"_id": "d1", "name": "glucose", "notes": "NEW version"}),
            serde_json::json!({"_id": "d3", "name": "creatinine", "notes": "kidney function"}),
        ];

        let results = search_with_gap(
            &index,
            &schema,
            "+__present__:__all__",
            10,
            0,
            None,
            false,
            &gap_rows,
        )
        .unwrap();

        // Should have 3 docs: d1 (gap version), d2 (index), d3 (gap)
        assert_eq!(results.len(), 3);

        let d1 = results.iter().find(|h| h.doc["_id"] == "d1").unwrap();
        assert_eq!(d1.doc["notes"], "NEW version"); // gap wins

        assert!(results.iter().any(|h| h.doc["_id"] == "d2"));
        assert!(results.iter().any(|h| h.doc["_id"] == "d3"));
    }

    #[test]
    fn test_search_with_gap_empty_gap() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_test_index(dir.path());

        let results = search_with_gap(
            &index,
            &schema,
            "+__present__:__all__",
            10,
            0,
            None,
            false,
            &[],
        )
        .unwrap();
        assert_eq!(results.len(), 3);
    }
}

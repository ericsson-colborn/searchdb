use tantivy::collector::TopDocs;
use tantivy::directory::RamDirectory;
use tantivy::query::QueryParser;
use tantivy::schema::Value;
use tantivy::{Index, Order, TantivyDocument};

use crate::error::{Result, SearchDbError};
use crate::es_dsl::ElasticQueryDsl;
use crate::schema::{FieldType, Schema};

/// A single search hit — parsed from _source with metadata.
#[derive(Debug)]
#[allow(dead_code)]
pub struct SearchHit {
    pub doc: serde_json::Value,
    pub score: f32,
}

/// Parsed sort specification.
struct SortSpec {
    field_name: String,
    order: Order,
}

/// Parse a sort string like "field", "field:asc", or "field:desc".
/// Default order is descending (highest first, matching score semantics).
fn parse_sort(sort_str: &str) -> SortSpec {
    if let Some((field, dir)) = sort_str.rsplit_once(':') {
        let order = if dir.eq_ignore_ascii_case("asc") {
            Order::Asc
        } else {
            Order::Desc
        };
        SortSpec {
            field_name: field.to_string(),
            order,
        }
    } else {
        SortSpec {
            field_name: sort_str.to_string(),
            order: Order::Desc,
        }
    }
}

/// Execute a pre-built tantivy query and return results.
///
/// Shared by both the query string path (`search()`) and the DSL path (`search_dsl()`).
/// When `sort` is provided, results are ordered by the named fast field instead of BM25.
#[allow(clippy::too_many_arguments)]
fn execute_query(
    index: &Index,
    query: &dyn tantivy::query::Query,
    limit: usize,
    offset: usize,
    fields: Option<&[String]>,
    include_score: bool,
    sort: Option<&str>,
    app_schema: &Schema,
) -> Result<Vec<SearchHit>> {
    let tv_schema = index.schema();
    let reader = index
        .reader()
        .map_err(|e| SearchDbError::Schema(format!("failed to open reader: {e}")))?;
    let searcher = reader.searcher();

    let source_field = tv_schema
        .get_field("_source")
        .map_err(|_| SearchDbError::Schema("missing _source field".into()))?;
    let id_field = tv_schema
        .get_field("_id")
        .map_err(|_| SearchDbError::Schema("missing _id field".into()))?;

    if let Some(sort_str) = sort {
        let spec = parse_sort(sort_str);

        let field_type = app_schema.fields.get(&spec.field_name).ok_or_else(|| {
            SearchDbError::Schema(format!("sort field '{}' not in schema", spec.field_name))
        })?;

        match field_type {
            FieldType::Numeric => {
                let collector = TopDocs::with_limit(limit + offset)
                    .order_by_fast_field::<f64>(&spec.field_name, spec.order);
                let top_docs = searcher.search(query, &collector)?;
                let mut results = Vec::new();
                for (_sort_val, doc_address) in top_docs.into_iter().skip(offset) {
                    let doc: TantivyDocument = searcher.doc(doc_address)?;
                    let hit = doc_to_hit(&doc, source_field, id_field, 0.0, fields, include_score)?;
                    results.push(hit);
                }
                Ok(results)
            }
            FieldType::Date => {
                let collector = TopDocs::with_limit(limit + offset)
                    .order_by_fast_field::<tantivy::DateTime>(&spec.field_name, spec.order);
                let top_docs = searcher.search(query, &collector)?;
                let mut results = Vec::new();
                for (_sort_val, doc_address) in top_docs.into_iter().skip(offset) {
                    let doc: TantivyDocument = searcher.doc(doc_address)?;
                    let hit = doc_to_hit(&doc, source_field, id_field, 0.0, fields, include_score)?;
                    results.push(hit);
                }
                Ok(results)
            }
            _ => Err(SearchDbError::Schema(format!(
                "sort field '{}' must be numeric or date (got {:?})",
                spec.field_name, field_type
            ))),
        }
    } else {
        let top_docs = searcher.search(query, &TopDocs::with_limit(limit + offset))?;
        let mut results = Vec::new();
        for (score, doc_address) in top_docs.into_iter().skip(offset) {
            let doc: TantivyDocument = searcher.doc(doc_address)?;
            let hit = doc_to_hit(&doc, source_field, id_field, score, fields, include_score)?;
            results.push(hit);
        }
        Ok(results)
    }
}

/// Execute a query string search against a tantivy index.
///
/// Uses tantivy's QueryParser with all user fields + system fields as defaults.
/// Returns results from `_source` with optional field projection.
#[allow(clippy::too_many_arguments)]
pub fn search(
    index: &Index,
    app_schema: &Schema,
    query_str: &str,
    limit: usize,
    offset: usize,
    fields: Option<&[String]>,
    include_score: bool,
    sort: Option<&str>,
) -> Result<Vec<SearchHit>> {
    let tv_schema = index.schema();

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

    execute_query(
        index,
        query.as_ref(),
        limit,
        offset,
        fields,
        include_score,
        sort,
        app_schema,
    )
}

/// Execute an Elasticsearch DSL query against a tantivy index.
///
/// Parses the JSON DSL, compiles it to a tantivy query, and executes.
#[allow(clippy::too_many_arguments)]
pub fn search_dsl(
    index: &Index,
    app_schema: &Schema,
    dsl_json: &str,
    limit: usize,
    offset: usize,
    fields: Option<&[String]>,
    include_score: bool,
    sort: Option<&str>,
) -> Result<Vec<SearchHit>> {
    let tv_schema = index.schema();

    // Deserialize and compile
    let es_query: ElasticQueryDsl = serde_json::from_str(dsl_json)
        .map_err(|e| SearchDbError::Schema(format!("DSL parse error: {e}")))?;
    let query = es_query.compile(&tv_schema, app_schema)?;

    execute_query(
        index,
        query.as_ref(),
        limit,
        offset,
        fields,
        include_score,
        sort,
        app_schema,
    )
}

/// Two-tier DSL search: persistent index + ephemeral gap index, dedup by _id.
#[allow(clippy::too_many_arguments)]
pub fn search_dsl_with_gap(
    persistent_index: &Index,
    app_schema: &Schema,
    dsl_json: &str,
    limit: usize,
    offset: usize,
    fields: Option<&[String]>,
    include_score: bool,
    gap_rows: &[serde_json::Value],
    sort: Option<&str>,
) -> Result<Vec<SearchHit>> {
    if gap_rows.is_empty() {
        return search_dsl(
            persistent_index,
            app_schema,
            dsl_json,
            limit,
            offset,
            fields,
            include_score,
            sort,
        );
    }

    // 1. Build ephemeral index from gap rows
    let gap_index = build_ephemeral_index(app_schema, gap_rows)?;

    // 2. Search the gap index
    let gap_hits = search_dsl(
        &gap_index,
        app_schema,
        dsl_json,
        gap_rows.len(),
        0,
        fields,
        include_score,
        sort,
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
    let persistent_hits = search_dsl(
        persistent_index,
        app_schema,
        dsl_json,
        limit + offset,
        0,
        fields,
        include_score,
        sort,
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

    // 5. Gap hits first (newer data), then persistent hits, paginate
    let mut merged = gap_hits;
    merged.extend(filtered_persistent);

    let paginated: Vec<SearchHit> = merged.into_iter().skip(offset).take(limit).collect();

    Ok(paginated)
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

/// Two-tier get: check gap rows first (by _id), fall back to persistent index.
pub fn get_with_gap(
    persistent_index: &Index,
    doc_id: &str,
    gap_rows: &[serde_json::Value],
) -> Result<Option<serde_json::Value>> {
    // Check gap first — newer data wins
    for row in gap_rows {
        if let Some(id) = row.get("_id").and_then(|v| v.as_str()) {
            if id == doc_id {
                let mut doc = row.clone();
                if let Some(obj) = doc.as_object_mut() {
                    obj.insert(
                        "_id".to_string(),
                        serde_json::Value::String(doc_id.to_string()),
                    );
                }
                return Ok(Some(doc));
            }
        }
    }

    // Fall back to persistent index
    get_by_id(persistent_index, doc_id)
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
    sort: Option<&str>,
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
            sort,
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
        sort,
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
        sort,
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

/// Execute aggregations on search results.
///
/// Parses ES-compatible aggregation JSON, runs it via tantivy's built-in
/// `AggregationCollector`, and returns ES-compatible aggregation results.
/// Fields used in aggregations must have the FAST attribute enabled.
pub fn aggregate(
    index: &Index,
    query: &dyn tantivy::query::Query,
    agg_json: &str,
) -> Result<serde_json::Value> {
    use tantivy::aggregation::agg_req::Aggregations;
    use tantivy::aggregation::AggregationCollector;

    let agg_req: Aggregations = serde_json::from_str(agg_json)
        .map_err(|e| SearchDbError::Schema(format!("aggregation parse error: {e}")))?;

    let collector = AggregationCollector::from_aggs(agg_req, Default::default());

    let reader = index
        .reader()
        .map_err(|e| SearchDbError::Schema(format!("failed to open reader: {e}")))?;
    let searcher = reader.searcher();

    let agg_results = searcher.search(query, &collector)?;

    serde_json::to_value(agg_results).map_err(SearchDbError::Json)
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

        let results = search(
            &index,
            &schema,
            r#"+name:"glucose""#,
            10,
            0,
            None,
            false,
            None,
        )
        .unwrap();
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
        let results = search(&index, &schema, "notes:diabetes", 10, 0, None, false, None).unwrap();
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
            None,
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

        let results = search(&index, &schema, "notes:blood", 10, 0, None, true, None).unwrap();
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
        let all = search(
            &index,
            &schema,
            "+__present__:__all__",
            10,
            0,
            None,
            false,
            None,
        )
        .unwrap();
        assert_eq!(all.len(), 3);

        let limited = search(
            &index,
            &schema,
            "+__present__:__all__",
            2,
            0,
            None,
            false,
            None,
        )
        .unwrap();
        assert_eq!(limited.len(), 2);

        let offset = search(
            &index,
            &schema,
            "+__present__:__all__",
            10,
            2,
            None,
            false,
            None,
        )
        .unwrap();
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
        let results = search(&index, &schema, "notes:diabetes", 10, 0, None, false, None).unwrap();
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
            let doc = writer::build_document(&tv_schema, &schema, doc_json, &doc_id).unwrap();
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
            None,
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
            None,
        )
        .unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_get_with_gap_found_in_gap() {
        let dir = tempfile::tempdir().unwrap();
        let (index, _, _) = setup_test_index(dir.path());

        let gap_rows =
            vec![serde_json::json!({"_id": "d1", "name": "UPDATED", "notes": "new version"})];

        let doc = get_with_gap(&index, "d1", &gap_rows).unwrap();
        assert!(doc.is_some());
        assert_eq!(doc.unwrap()["name"], "UPDATED");
    }

    #[test]
    fn test_get_with_gap_falls_back_to_index() {
        let dir = tempfile::tempdir().unwrap();
        let (index, _, _) = setup_test_index(dir.path());

        let gap_rows = vec![serde_json::json!({"_id": "d99", "name": "unrelated"})];

        let doc = get_with_gap(&index, "d2", &gap_rows).unwrap();
        assert!(doc.is_some());
        assert_eq!(doc.unwrap()["_id"], "d2");
    }

    #[test]
    fn test_get_with_gap_not_found_anywhere() {
        let dir = tempfile::tempdir().unwrap();
        let (index, _, _) = setup_test_index(dir.path());

        let doc = get_with_gap(&index, "nonexistent", &[]).unwrap();
        assert!(doc.is_none());
    }

    #[test]
    fn test_search_dsl_term_query() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_test_index(dir.path());

        let dsl = r#"{"term": {"name": "glucose"}}"#;
        let results = search_dsl(&index, &schema, dsl, 10, 0, None, false, None).unwrap();
        assert_eq!(results.len(), 2);
        for hit in &results {
            assert_eq!(hit.doc["name"], "glucose");
        }
    }

    #[test]
    fn test_search_dsl_match_query() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_test_index(dir.path());

        let dsl = r#"{"match": {"notes": "diabetes"}}"#;
        let results = search_dsl(&index, &schema, dsl, 10, 0, None, false, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc["_id"], "d2");
    }

    #[test]
    fn test_search_dsl_match_all() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_test_index(dir.path());

        let dsl = r#"{"match_all": {}}"#;
        let results = search_dsl(&index, &schema, dsl, 10, 0, None, false, None).unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_search_dsl_bool_query() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_test_index(dir.path());

        let dsl = r#"{
            "bool": {
                "must": [{"term": {"name": "glucose"}}],
                "must_not": [{"match": {"notes": "postprandial"}}]
            }
        }"#;
        let results = search_dsl(&index, &schema, dsl, 10, 0, None, false, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc["notes"], "fasting blood sample");
    }

    #[test]
    fn test_search_dsl_invalid_json() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_test_index(dir.path());

        let dsl = "not json";
        let result = search_dsl(&index, &schema, dsl, 10, 0, None, false, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_search_dsl_with_limit_offset() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_test_index(dir.path());

        let dsl = r#"{"match_all": {}}"#;
        let results = search_dsl(&index, &schema, dsl, 2, 0, None, false, None).unwrap();
        assert_eq!(results.len(), 2);

        let results = search_dsl(&index, &schema, dsl, 10, 2, None, false, None).unwrap();
        assert_eq!(results.len(), 1);
    }

    // --- Integration-style tests with full schema (keyword, text, numeric, date) ---

    fn setup_full_test_index(dir: &std::path::Path) -> (Index, Schema, tantivy::schema::Schema) {
        let schema = Schema {
            fields: BTreeMap::from([
                ("name".into(), FieldType::Keyword),
                ("notes".into(), FieldType::Text),
                ("age".into(), FieldType::Numeric),
                ("created_at".into(), FieldType::Date),
            ]),
        };
        let tv_schema = schema.build_tantivy_schema();
        let index = Index::create_in_dir(dir, tv_schema.clone()).unwrap();
        let mut w = index.writer(50_000_000).unwrap();
        let id_field = tv_schema.get_field("_id").unwrap();

        let docs = vec![
            serde_json::json!({
                "_id": "d1", "name": "glucose", "notes": "fasting blood sample",
                "age": 45.0, "created_at": "2024-06-15T10:00:00Z"
            }),
            serde_json::json!({
                "_id": "d2", "name": "a1c", "notes": "borderline diabetic",
                "age": 62.0, "created_at": "2024-03-20T14:30:00Z"
            }),
            serde_json::json!({
                "_id": "d3", "name": "glucose", "notes": "postprandial check",
                "age": 33.0, "created_at": "2024-09-01T08:00:00Z"
            }),
            serde_json::json!({
                "_id": "d4", "name": "creatinine", "notes": "kidney function test",
                "age": 55.0, "created_at": "2024-01-10T09:00:00Z"
            }),
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
    fn test_dsl_term_keyword_exact() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_full_test_index(dir.path());

        let dsl = r#"{"term": {"name": "glucose"}}"#;
        let results = search_dsl(&index, &schema, dsl, 10, 0, None, false, None).unwrap();
        assert_eq!(results.len(), 2);
        for hit in &results {
            assert_eq!(hit.doc["name"], "glucose");
        }
    }

    #[test]
    fn test_dsl_terms_multi_value() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_full_test_index(dir.path());

        let dsl = r#"{"terms": {"name": ["glucose", "a1c"]}}"#;
        let results = search_dsl(&index, &schema, dsl, 10, 0, None, false, None).unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_dsl_match_stemmed() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_full_test_index(dir.path());

        let dsl = r#"{"match": {"notes": "diabetes"}}"#;
        let results = search_dsl(&index, &schema, dsl, 10, 0, None, false, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc["_id"], "d2");
    }

    #[test]
    fn test_dsl_match_phrase_exact() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_full_test_index(dir.path());

        let dsl = r#"{"match_phrase": {"notes": "fasting blood"}}"#;
        let results = search_dsl(&index, &schema, dsl, 10, 0, None, false, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc["_id"], "d1");
    }

    #[test]
    fn test_dsl_range_numeric() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_full_test_index(dir.path());

        let dsl = r#"{"range": {"age": {"gte": 50}}}"#;
        let results = search_dsl(&index, &schema, dsl, 10, 0, None, false, None).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_dsl_range_date() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_full_test_index(dir.path());

        let dsl = r#"{"range": {"created_at": {"gte": "2024-06-01T00:00:00Z"}}}"#;
        let results = search_dsl(&index, &schema, dsl, 10, 0, None, false, None).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_dsl_exists_query() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_full_test_index(dir.path());

        let dsl = r#"{"exists": {"field": "name"}}"#;
        let results = search_dsl(&index, &schema, dsl, 10, 0, None, false, None).unwrap();
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn test_dsl_bool_compound() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_full_test_index(dir.path());

        let dsl = r#"{
            "bool": {
                "must": [{"term": {"name": "glucose"}}],
                "filter": [{"range": {"age": {"gte": 40}}}]
            }
        }"#;
        let results = search_dsl(&index, &schema, dsl, 10, 0, None, false, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc["_id"], "d1");
    }

    #[test]
    fn test_dsl_match_all_returns_everything() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_full_test_index(dir.path());

        let dsl = r#"{"match_all": {}}"#;
        let results = search_dsl(&index, &schema, dsl, 10, 0, None, false, None).unwrap();
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn test_dsl_match_none_returns_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_full_test_index(dir.path());

        let dsl = r#"{"match_none": {}}"#;
        let results = search_dsl(&index, &schema, dsl, 10, 0, None, false, None).unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_dsl_with_gap_rows() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_full_test_index(dir.path());

        let gap_rows = vec![
            serde_json::json!({
                "_id": "d1", "name": "glucose", "notes": "UPDATED fasting",
                "age": 46.0, "created_at": "2024-06-15T10:00:00Z"
            }),
            serde_json::json!({
                "_id": "d5", "name": "glucose", "notes": "new doc",
                "age": 28.0, "created_at": "2024-12-01T00:00:00Z"
            }),
        ];

        let dsl = r#"{"term": {"name": "glucose"}}"#;
        let results =
            search_dsl_with_gap(&index, &schema, dsl, 10, 0, None, false, &gap_rows, None).unwrap();
        // d1 (gap), d3 (index), d5 (gap) = 3 glucose docs
        assert_eq!(results.len(), 3);

        // gap d1 should have updated notes
        let d1 = results.iter().find(|h| h.doc["_id"] == "d1").unwrap();
        assert_eq!(d1.doc["notes"], "UPDATED fasting");
    }

    // --- Sort tests ---

    #[test]
    fn test_parse_sort_default_desc() {
        let spec = parse_sort("age");
        assert_eq!(spec.field_name, "age");
        assert!(matches!(spec.order, Order::Desc));
    }

    #[test]
    fn test_parse_sort_asc() {
        let spec = parse_sort("age:asc");
        assert_eq!(spec.field_name, "age");
        assert!(matches!(spec.order, Order::Asc));
    }

    #[test]
    fn test_parse_sort_desc_explicit() {
        let spec = parse_sort("created_at:desc");
        assert_eq!(spec.field_name, "created_at");
        assert!(matches!(spec.order, Order::Desc));
    }

    #[test]
    fn test_sort_by_numeric_field_desc() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_full_test_index(dir.path());

        // Sort by age descending (default)
        let results = search(
            &index,
            &schema,
            "+__present__:__all__",
            10,
            0,
            None,
            false,
            Some("age"),
        )
        .unwrap();
        assert_eq!(results.len(), 4);
        // d2 (62) should be first, then d4 (55), d1 (45), d3 (33)
        assert_eq!(results[0].doc["_id"], "d2");
        assert_eq!(results[1].doc["_id"], "d4");
        assert_eq!(results[2].doc["_id"], "d1");
        assert_eq!(results[3].doc["_id"], "d3");
    }

    #[test]
    fn test_sort_by_numeric_field_asc() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_full_test_index(dir.path());

        // Sort by age ascending
        let results = search(
            &index,
            &schema,
            "+__present__:__all__",
            10,
            0,
            None,
            false,
            Some("age:asc"),
        )
        .unwrap();
        assert_eq!(results.len(), 4);
        // d3 (33) should be first, then d1 (45), d4 (55), d2 (62)
        assert_eq!(results[0].doc["_id"], "d3");
        assert_eq!(results[1].doc["_id"], "d1");
        assert_eq!(results[2].doc["_id"], "d4");
        assert_eq!(results[3].doc["_id"], "d2");
    }

    #[test]
    fn test_sort_by_date_field() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_full_test_index(dir.path());

        // Sort by created_at descending (most recent first)
        let results = search(
            &index,
            &schema,
            "+__present__:__all__",
            10,
            0,
            None,
            false,
            Some("created_at:desc"),
        )
        .unwrap();
        assert_eq!(results.len(), 4);
        // d3 (2024-09-01) > d1 (2024-06-15) > d2 (2024-03-20) > d4 (2024-01-10)
        assert_eq!(results[0].doc["_id"], "d3");
        assert_eq!(results[1].doc["_id"], "d1");
        assert_eq!(results[2].doc["_id"], "d2");
        assert_eq!(results[3].doc["_id"], "d4");
    }

    #[test]
    fn test_sort_by_date_field_asc() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_full_test_index(dir.path());

        // Sort by created_at ascending (oldest first)
        let results = search(
            &index,
            &schema,
            "+__present__:__all__",
            10,
            0,
            None,
            false,
            Some("created_at:asc"),
        )
        .unwrap();
        assert_eq!(results.len(), 4);
        // d4 (2024-01-10) < d2 (2024-03-20) < d1 (2024-06-15) < d3 (2024-09-01)
        assert_eq!(results[0].doc["_id"], "d4");
        assert_eq!(results[1].doc["_id"], "d2");
        assert_eq!(results[2].doc["_id"], "d1");
        assert_eq!(results[3].doc["_id"], "d3");
    }

    #[test]
    fn test_sort_rejects_keyword_field() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_full_test_index(dir.path());

        let result = search(
            &index,
            &schema,
            "+__present__:__all__",
            10,
            0,
            None,
            false,
            Some("name"),
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("must be numeric or date"));
    }

    #[test]
    fn test_sort_rejects_text_field() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_full_test_index(dir.path());

        let result = search(
            &index,
            &schema,
            "+__present__:__all__",
            10,
            0,
            None,
            false,
            Some("notes"),
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("must be numeric or date"));
    }

    #[test]
    fn test_sort_rejects_unknown_field() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_full_test_index(dir.path());

        let result = search(
            &index,
            &schema,
            "+__present__:__all__",
            10,
            0,
            None,
            false,
            Some("nonexistent"),
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not in schema"));
    }

    #[test]
    fn test_sort_with_limit_and_offset() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_full_test_index(dir.path());

        // Sort by age desc, take 2
        let results = search(
            &index,
            &schema,
            "+__present__:__all__",
            2,
            0,
            None,
            false,
            Some("age"),
        )
        .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].doc["_id"], "d2"); // age 62
        assert_eq!(results[1].doc["_id"], "d4"); // age 55

        // Sort by age desc, skip 2, take 2
        let results = search(
            &index,
            &schema,
            "+__present__:__all__",
            2,
            2,
            None,
            false,
            Some("age"),
        )
        .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].doc["_id"], "d1"); // age 45
        assert_eq!(results[1].doc["_id"], "d3"); // age 33
    }

    #[test]
    fn test_sort_dsl_by_numeric() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_full_test_index(dir.path());

        let dsl = r#"{"match_all": {}}"#;
        let results =
            search_dsl(&index, &schema, dsl, 10, 0, None, false, Some("age:asc")).unwrap();
        assert_eq!(results.len(), 4);
        assert_eq!(results[0].doc["_id"], "d3"); // age 33
        assert_eq!(results[3].doc["_id"], "d2"); // age 62
    }

    #[test]
    fn test_sort_score_is_zero() {
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_full_test_index(dir.path());

        let results = search(
            &index,
            &schema,
            "+__present__:__all__",
            10,
            0,
            None,
            false,
            Some("age"),
        )
        .unwrap();
        // When sorting, BM25 score should be 0.0
        for hit in &results {
            assert_eq!(hit.score, 0.0);
        }
    }

    // --- Multi-match positional search tests ---

    #[test]
    fn test_search_multi_match_positional() {
        // Positional query without field qualifiers should multi-match across all fields
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_test_index(dir.path());

        // "blood" appears in notes of d1 ("fasting blood sample")
        // QueryParser with all default fields searches keyword + text fields
        let results = search(&index, &schema, "blood", 10, 0, None, false, None).unwrap();
        assert!(!results.is_empty());
        assert!(results.iter().any(|h| h.doc["_id"] == "d1"));
    }

    #[test]
    fn test_search_multi_match_keyword_field() {
        // Positional query matching a keyword field value
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_test_index(dir.path());

        // "glucose" is a keyword value in the name field
        let results = search(&index, &schema, "glucose", 10, 0, None, false, None).unwrap();
        assert_eq!(results.len(), 2); // d1 and d3 both have name=glucose
    }

    #[test]
    fn test_search_composed_query_and_filter() {
        // Simulate the CLI composition: positional query AND filter
        let dir = tempfile::tempdir().unwrap();
        let (index, schema, _) = setup_test_index(dir.path());

        // Compose like the CLI does: "(glucose) AND (+notes:fasting)"
        let combined = "(glucose) AND (+notes:fasting)";
        let results = search(&index, &schema, combined, 10, 0, None, false, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc["_id"], "d1");
    }

    // --- Aggregation tests ---

    fn setup_agg_test_index(dir: &std::path::Path) -> (Index, Schema) {
        let schema = Schema {
            fields: BTreeMap::from([
                ("category".into(), FieldType::Keyword),
                ("price".into(), FieldType::Numeric),
            ]),
        };
        let tv_schema = schema.build_tantivy_schema();
        let index = Index::create_in_dir(dir, tv_schema.clone()).unwrap();
        let mut w = index.writer(15_000_000).unwrap();
        let id_field = tv_schema.get_field("_id").unwrap();

        let docs = vec![
            serde_json::json!({"_id": "1", "category": "electronics", "price": 100.0}),
            serde_json::json!({"_id": "2", "category": "electronics", "price": 200.0}),
            serde_json::json!({"_id": "3", "category": "books", "price": 15.0}),
            serde_json::json!({"_id": "4", "category": "books", "price": 25.0}),
            serde_json::json!({"_id": "5", "category": "clothing", "price": 50.0}),
        ];

        for doc_json in &docs {
            let doc_id = writer::make_doc_id(doc_json);
            let doc = writer::build_document(&tv_schema, &schema, doc_json, &doc_id).unwrap();
            writer::upsert_document(&w, id_field, doc, &doc_id);
        }
        w.commit().unwrap();

        (index, schema)
    }

    #[test]
    fn test_aggregate_terms() {
        let dir = tempfile::tempdir().unwrap();
        let (index, _schema) = setup_agg_test_index(dir.path());

        let agg_json = r#"{"categories": {"terms": {"field": "category"}}}"#;
        let result = aggregate(&index, &tantivy::query::AllQuery, agg_json).unwrap();

        let buckets = result["categories"]["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 3);

        // Check that all categories are present
        let keys: Vec<&str> = buckets.iter().map(|b| b["key"].as_str().unwrap()).collect();
        assert!(keys.contains(&"electronics"));
        assert!(keys.contains(&"books"));
        assert!(keys.contains(&"clothing"));

        // Check counts
        let electronics = buckets.iter().find(|b| b["key"] == "electronics").unwrap();
        assert_eq!(electronics["doc_count"], 2);
        let clothing = buckets.iter().find(|b| b["key"] == "clothing").unwrap();
        assert_eq!(clothing["doc_count"], 1);
    }

    #[test]
    fn test_aggregate_avg() {
        let dir = tempfile::tempdir().unwrap();
        let (index, _schema) = setup_agg_test_index(dir.path());

        let agg_json = r#"{"avg_price": {"avg": {"field": "price"}}}"#;
        let result = aggregate(&index, &tantivy::query::AllQuery, agg_json).unwrap();

        // Average of 100, 200, 15, 25, 50 = 78.0
        let avg = result["avg_price"]["value"].as_f64().unwrap();
        assert!((avg - 78.0).abs() < 0.01);
    }

    #[test]
    fn test_aggregate_stats() {
        let dir = tempfile::tempdir().unwrap();
        let (index, _schema) = setup_agg_test_index(dir.path());

        let agg_json = r#"{"price_stats": {"stats": {"field": "price"}}}"#;
        let result = aggregate(&index, &tantivy::query::AllQuery, agg_json).unwrap();

        assert_eq!(result["price_stats"]["count"].as_f64().unwrap() as u64, 5);
        assert!((result["price_stats"]["min"].as_f64().unwrap() - 15.0).abs() < 0.01);
        assert!((result["price_stats"]["max"].as_f64().unwrap() - 200.0).abs() < 0.01);
        assert!((result["price_stats"]["sum"].as_f64().unwrap() - 390.0).abs() < 0.01);
        assert!((result["price_stats"]["avg"].as_f64().unwrap() - 78.0).abs() < 0.01);
    }

    #[test]
    fn test_aggregate_histogram() {
        let dir = tempfile::tempdir().unwrap();
        let (index, _schema) = setup_agg_test_index(dir.path());

        let agg_json = r#"{"price_ranges": {"histogram": {"field": "price", "interval": 100}}}"#;
        let result = aggregate(&index, &tantivy::query::AllQuery, agg_json).unwrap();

        let buckets = result["price_ranges"]["buckets"].as_array().unwrap();
        assert!(!buckets.is_empty());
    }

    #[test]
    fn test_aggregate_with_query_filter() {
        let dir = tempfile::tempdir().unwrap();
        let (index, _schema) = setup_agg_test_index(dir.path());

        // Only aggregate over electronics
        let category_field = index.schema().get_field("category").unwrap();
        let term = tantivy::Term::from_field_text(category_field, "electronics");
        let query = tantivy::query::TermQuery::new(term, tantivy::schema::IndexRecordOption::Basic);

        let agg_json = r#"{"avg_price": {"avg": {"field": "price"}}}"#;
        let result = aggregate(&index, &query, agg_json).unwrap();

        // Average of 100, 200 = 150.0
        let avg = result["avg_price"]["value"].as_f64().unwrap();
        assert!((avg - 150.0).abs() < 0.01);
    }

    #[test]
    fn test_aggregate_invalid_json() {
        let dir = tempfile::tempdir().unwrap();
        let (index, _schema) = setup_agg_test_index(dir.path());

        let result = aggregate(&index, &tantivy::query::AllQuery, "not json");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("aggregation parse error"));
    }

    #[test]
    fn test_aggregate_min_max() {
        let dir = tempfile::tempdir().unwrap();
        let (index, _schema) = setup_agg_test_index(dir.path());

        let agg_json = r#"{
            "min_price": {"min": {"field": "price"}},
            "max_price": {"max": {"field": "price"}}
        }"#;
        let result = aggregate(&index, &tantivy::query::AllQuery, agg_json).unwrap();

        assert!((result["min_price"]["value"].as_f64().unwrap() - 15.0).abs() < 0.01);
        assert!((result["max_price"]["value"].as_f64().unwrap() - 200.0).abs() < 0.01);
    }

    #[test]
    fn test_aggregate_terms_with_sub_aggregation() {
        let dir = tempfile::tempdir().unwrap();
        let (index, _schema) = setup_agg_test_index(dir.path());

        let agg_json = r#"{
            "by_category": {
                "terms": {"field": "category"},
                "aggs": {
                    "avg_price": {"avg": {"field": "price"}}
                }
            }
        }"#;
        let result = aggregate(&index, &tantivy::query::AllQuery, agg_json).unwrap();

        let buckets = result["by_category"]["buckets"].as_array().unwrap();
        let electronics = buckets.iter().find(|b| b["key"] == "electronics").unwrap();
        let avg = electronics["avg_price"]["value"].as_f64().unwrap();
        assert!((avg - 150.0).abs() < 0.01);

        let books = buckets.iter().find(|b| b["key"] == "books").unwrap();
        let avg = books["avg_price"]["value"].as_f64().unwrap();
        assert!((avg - 20.0).abs() < 0.01);
    }
}

use serde::Deserialize;
use tantivy::query::{BooleanQuery, Occur, Query};
use tantivy::schema::FieldType as TvFieldType;
use tantivy::schema::IndexRecordOption;
use tantivy::Term;

use crate::error::{Result, SearchDbError};
use crate::schema::Schema as AppSchema;

use super::match_query::tokenize;

/// Elasticsearch `multi_match` query -- search across multiple fields.
///
/// Format:
/// ```json
/// {"multi_match": {"query": "search text", "fields": ["title", "body"]}}
/// ```
///
/// Runs a `match`-style query against each field and combines them with
/// `BooleanQuery` using `Should` (OR) clauses. Each field is tokenized
/// according to its own tokenizer.
#[derive(Debug, Clone, Deserialize)]
pub struct MultiMatchQuery {
    pub query: String,
    pub fields: Vec<String>,
}

impl MultiMatchQuery {
    pub fn compile(
        &self,
        tv_schema: &tantivy::schema::Schema,
        _app_schema: &AppSchema,
    ) -> Result<Box<dyn Query>> {
        if self.fields.is_empty() {
            return Err(SearchDbError::Schema(
                "multi_match requires at least one field".into(),
            ));
        }

        let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();

        for field_name in &self.fields {
            let field = tv_schema.get_field(field_name).map_err(|_| {
                SearchDbError::Schema(format!("field \"{field_name}\" not found in schema"))
            })?;

            let sub_query = compile_match_for_field(tv_schema, field, &self.query)?;
            clauses.push((Occur::Should, sub_query));
        }

        Ok(Box::new(BooleanQuery::new(clauses)))
    }
}

/// Compile a match query for a single field. Replicates the core logic of
/// `MatchQuery::compile` without requiring the OneFieldMap wrapper.
fn compile_match_for_field(
    tv_schema: &tantivy::schema::Schema,
    field: tantivy::schema::Field,
    query_text: &str,
) -> Result<Box<dyn Query>> {
    let field_entry = tv_schema.get_field_entry(field);
    let tokenizer_name = match field_entry.field_type() {
        TvFieldType::Str(opts) => {
            let indexing = opts.get_indexing_options().ok_or_else(|| {
                SearchDbError::Schema(format!(
                    "field \"{}\" is not indexed, cannot run multi_match query",
                    field_entry.name()
                ))
            })?;
            indexing.tokenizer().to_string()
        }
        other => {
            return Err(SearchDbError::Schema(format!(
                "multi_match query requires text fields, but \"{}\" is {:?}",
                field_entry.name(),
                other
            )));
        }
    };

    let tokens = tokenize(query_text, &tokenizer_name)?;

    if tokens.is_empty() {
        return Ok(Box::new(tantivy::query::EmptyQuery));
    }

    if tokens.len() == 1 {
        let term = Term::from_field_text(field, &tokens[0]);
        return Ok(Box::new(tantivy::query::TermQuery::new(
            term,
            IndexRecordOption::WithFreqs,
        )));
    }

    let clauses: Vec<(Occur, Box<dyn Query>)> = tokens
        .into_iter()
        .map(|token| {
            let term = Term::from_field_text(field, &token);
            (
                Occur::Should,
                Box::new(tantivy::query::TermQuery::new(
                    term,
                    IndexRecordOption::WithFreqs,
                )) as Box<dyn Query>,
            )
        })
        .collect();

    Ok(Box::new(BooleanQuery::new(clauses)))
}

#[cfg(test)]
mod tests {
    use crate::es_dsl::ElasticQueryDsl;
    use crate::schema::{FieldType, Schema};
    use std::collections::BTreeMap;

    fn multi_field_schema() -> (tantivy::schema::Schema, Schema) {
        let schema = Schema {
            fields: BTreeMap::from([
                ("title".into(), FieldType::Text),
                ("body".into(), FieldType::Text),
                ("status".into(), FieldType::Keyword),
            ]),
        };
        let tv = schema.build_tantivy_schema();
        (tv, schema)
    }

    #[test]
    fn test_multi_match_deserialize() {
        let json = r#"{"multi_match": {"query": "search", "fields": ["title", "body"]}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        assert!(matches!(dsl, ElasticQueryDsl::MultiMatch(_)));
    }

    #[test]
    fn test_multi_match_compile() {
        let (tv, schema) = multi_field_schema();
        let json = r#"{"multi_match": {"query": "search text", "fields": ["title", "body"]}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok(), "compile failed: {:?}", query.err());
    }

    #[test]
    fn test_multi_match_single_field() {
        let (tv, schema) = multi_field_schema();
        let json = r#"{"multi_match": {"query": "search", "fields": ["title"]}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok(), "compile failed: {:?}", query.err());
    }

    #[test]
    fn test_multi_match_keyword_field() {
        let (tv, schema) = multi_field_schema();
        // Keyword fields use raw tokenizer -- should still work
        let json = r#"{"multi_match": {"query": "active", "fields": ["status"]}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok(), "compile failed: {:?}", query.err());
    }

    #[test]
    fn test_multi_match_mixed_fields() {
        let (tv, schema) = multi_field_schema();
        let json = r#"{"multi_match": {"query": "search", "fields": ["title", "body", "status"]}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok(), "compile failed: {:?}", query.err());
    }

    #[test]
    fn test_multi_match_empty_fields_errors() {
        let (tv, schema) = multi_field_schema();
        let json = r#"{"multi_match": {"query": "search", "fields": []}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let result = dsl.compile(&tv, &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_multi_match_unknown_field_errors() {
        let (tv, schema) = multi_field_schema();
        let json = r#"{"multi_match": {"query": "search", "fields": ["title", "missing"]}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let result = dsl.compile(&tv, &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_multi_match_empty_query_text() {
        let (tv, schema) = multi_field_schema();
        let json = r#"{"multi_match": {"query": "", "fields": ["title", "body"]}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok());
    }
}

use serde::Deserialize;
use tantivy::query::{BooleanQuery, Occur, Query};
use tantivy::schema::IndexRecordOption;
use tantivy::Term;

use super::one_field_map::OneFieldMap;
use super::term_query::parse_date;
use crate::error::{Result, SearchDbError};
use crate::schema::{FieldType, Schema as AppSchema};

/// Elasticsearch `terms` query -- multi-value OR on a single field.
///
/// Format: `{"terms": {"status": ["active", "pending"]}}`
#[derive(Debug, Clone, Deserialize)]
pub struct TermsQuery(pub OneFieldMap<Vec<serde_json::Value>>);

impl TermsQuery {
    pub fn compile(
        &self,
        tv_schema: &tantivy::schema::Schema,
        app_schema: &AppSchema,
    ) -> Result<Box<dyn Query>> {
        let field_name = &self.0.field;
        let field = tv_schema.get_field(field_name).map_err(|_| {
            SearchDbError::Schema(format!("field \"{field_name}\" not found in schema"))
        })?;

        let field_type = app_schema.fields.get(field_name).ok_or_else(|| {
            SearchDbError::Schema(format!("field \"{field_name}\" not found in schema"))
        })?;

        let values = &self.0.value;
        if values.is_empty() {
            // Empty terms list matches nothing
            return Ok(Box::new(tantivy::query::EmptyQuery));
        }

        let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::with_capacity(values.len());
        for json_val in values {
            let term = match field_type {
                FieldType::Keyword | FieldType::Text => {
                    let text = json_val.as_str().ok_or_else(|| {
                        SearchDbError::Schema(format!(
                            "terms query on field \"{field_name}\" expected string, got {json_val}"
                        ))
                    })?;
                    Term::from_field_text(field, text)
                }
                FieldType::Numeric => {
                    let num = json_val.as_f64().ok_or_else(|| {
                        SearchDbError::Schema(format!(
                            "terms query on field \"{field_name}\" expected number, got {json_val}"
                        ))
                    })?;
                    Term::from_field_f64(field, num)
                }
                FieldType::Date => {
                    let date_str = json_val.as_str().ok_or_else(|| {
                        SearchDbError::Schema(format!(
                            "terms query on field \"{field_name}\" expected date string, got {json_val}"
                        ))
                    })?;
                    let dt = parse_date(date_str)?;
                    Term::from_field_date(field, dt)
                }
            };
            clauses.push((
                Occur::Should,
                Box::new(tantivy::query::TermQuery::new(
                    term,
                    IndexRecordOption::Basic,
                )),
            ));
        }

        Ok(Box::new(BooleanQuery::new(clauses)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::es_dsl::ElasticQueryDsl;
    use crate::schema::{FieldType, Schema};
    use std::collections::BTreeMap;

    #[test]
    fn test_terms_query_deserialize() {
        let json = r#"{"terms": {"status": ["active", "pending"]}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        assert!(matches!(dsl, ElasticQueryDsl::Terms(_)));
    }

    #[test]
    fn test_terms_query_compile() {
        let schema = Schema {
            fields: BTreeMap::from([("status".into(), FieldType::Keyword)]),
        };
        let tv = schema.build_tantivy_schema();
        let json = r#"{"terms": {"status": ["active", "pending"]}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok(), "compile failed: {:?}", query.err());
    }

    #[test]
    fn test_terms_query_empty_array() {
        let schema = Schema {
            fields: BTreeMap::from([("status".into(), FieldType::Keyword)]),
        };
        let tv = schema.build_tantivy_schema();
        let json = r#"{"terms": {"status": []}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok());
    }

    #[test]
    fn test_terms_query_unknown_field() {
        let schema = Schema {
            fields: BTreeMap::from([("status".into(), FieldType::Keyword)]),
        };
        let tv = schema.build_tantivy_schema();
        let json = r#"{"terms": {"missing": ["a", "b"]}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let result = dsl.compile(&tv, &schema);
        assert!(result.is_err());
    }
}

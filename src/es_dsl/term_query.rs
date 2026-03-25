use serde::Deserialize;
use tantivy::query::Query;
use tantivy::schema::IndexRecordOption;
use tantivy::Term;

use super::one_field_map::OneFieldMap;
use crate::error::{Result, SearchDbError};
use crate::schema::{FieldType, Schema as AppSchema};

/// Elasticsearch `term` query -- exact match on a single field.
///
/// Formats:
/// - `{"term": {"status": "active"}}` (shorthand)
/// - `{"term": {"status": {"value": "active"}}}` (full)
#[derive(Debug, Clone, Deserialize)]
pub struct TermQuery(pub OneFieldMap<TermQueryParams>);

/// Value for a term query -- either a bare value or an object with `value` key.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum TermQueryParams {
    Full { value: serde_json::Value },
    Shorthand(serde_json::Value),
}

impl TermQueryParams {
    pub fn value(&self) -> &serde_json::Value {
        match self {
            TermQueryParams::Full { value } => value,
            TermQueryParams::Shorthand(v) => v,
        }
    }
}

impl TermQuery {
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

        let json_val = self.0.value.value();

        let term = match field_type {
            FieldType::Keyword | FieldType::Text => {
                let text = json_val.as_str().ok_or_else(|| {
                    SearchDbError::Schema(format!(
                        "term query on field \"{field_name}\" expected string value, got {json_val}"
                    ))
                })?;
                Term::from_field_text(field, text)
            }
            FieldType::Numeric => {
                let num = json_val.as_f64().ok_or_else(|| {
                    SearchDbError::Schema(format!(
                        "term query on field \"{field_name}\" expected numeric value, got {json_val}"
                    ))
                })?;
                Term::from_field_f64(field, num)
            }
            FieldType::Date => {
                let date_str = json_val.as_str().ok_or_else(|| {
                    SearchDbError::Schema(format!(
                        "term query on field \"{field_name}\" expected date string, got {json_val}"
                    ))
                })?;
                let dt = parse_date_to_tantivy(date_str)?;
                Term::from_field_date(field, dt)
            }
        };

        Ok(Box::new(tantivy::query::TermQuery::new(
            term,
            IndexRecordOption::Basic,
        )))
    }
}

/// Parse a date string into tantivy DateTime. Supports RFC 3339 and ISO 8601 date-only.
pub(super) fn parse_date_to_tantivy(s: &str) -> Result<tantivy::DateTime> {
    // Try RFC 3339 first (e.g. "2024-01-15T10:30:00Z")
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        let utc = dt.with_timezone(&chrono::Utc);
        return Ok(tantivy::DateTime::from_timestamp_micros(
            utc.timestamp_micros(),
        ));
    }

    // Try date-only (e.g. "2024-01-15")
    if let Ok(nd) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let dt = nd
            .and_hms_opt(0, 0, 0)
            .expect("midnight is always valid")
            .and_utc();
        return Ok(tantivy::DateTime::from_timestamp_micros(
            dt.timestamp_micros(),
        ));
    }

    Err(SearchDbError::Schema(format!(
        "could not parse date \"{s}\" -- expected RFC 3339 or YYYY-MM-DD"
    )))
}

/// Alias for use by range_query and terms_query.
pub(super) use parse_date_to_tantivy as parse_date;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::es_dsl::ElasticQueryDsl;
    use crate::schema::{FieldType, Schema};
    use std::collections::BTreeMap;

    fn keyword_schema() -> (tantivy::schema::Schema, Schema) {
        let schema = Schema {
            fields: BTreeMap::from([("status".into(), FieldType::Keyword)]),
        };
        let tv = schema.build_tantivy_schema();
        (tv, schema)
    }

    #[test]
    fn test_term_query_deserialize_shorthand() {
        let json = r#"{"term": {"status": "active"}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        assert!(matches!(dsl, ElasticQueryDsl::Term(_)));
    }

    #[test]
    fn test_term_query_deserialize_full() {
        let json = r#"{"term": {"status": {"value": "active"}}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        assert!(matches!(dsl, ElasticQueryDsl::Term(_)));
    }

    #[test]
    fn test_term_query_compile_keyword() {
        let (tv, schema) = keyword_schema();
        let json = r#"{"term": {"status": "active"}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok(), "compile failed: {:?}", query.err());
    }

    #[test]
    fn test_term_query_unknown_field_errors() {
        let (tv, schema) = keyword_schema();
        let json = r#"{"term": {"nonexistent": "value"}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let result = dsl.compile(&tv, &schema);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("not found in schema"),
            "Expected 'not found in schema', got: {err_msg}"
        );
    }

    #[test]
    fn test_term_query_compile_numeric() {
        let schema = Schema {
            fields: BTreeMap::from([("age".into(), FieldType::Numeric)]),
        };
        let tv = schema.build_tantivy_schema();
        let json = r#"{"term": {"age": 42}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok(), "compile failed: {:?}", query.err());
    }

    #[test]
    fn test_term_query_compile_date() {
        let schema = Schema {
            fields: BTreeMap::from([("created_at".into(), FieldType::Date)]),
        };
        let tv = schema.build_tantivy_schema();
        let json = r#"{"term": {"created_at": "2024-01-15T10:30:00Z"}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok(), "compile failed: {:?}", query.err());
    }
}

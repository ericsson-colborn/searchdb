use serde::Deserialize;
use tantivy::query::Query;
use tantivy::schema::IndexRecordOption;
use tantivy::Term;

use crate::error::{Result, SearchDbError};
use crate::schema::Schema as AppSchema;

/// Elasticsearch `exists` query -- checks if a field has a non-null value.
///
/// Format: `{"exists": {"field": "status"}}`
///
/// Maps to a term query on the internal `__present__` field.
/// At index time, each document gets `__present__` tokens for each non-null field.
#[derive(Debug, Clone, Deserialize)]
pub struct ExistsQuery {
    pub field: String,
}

impl ExistsQuery {
    pub fn compile(
        &self,
        tv_schema: &tantivy::schema::Schema,
        app_schema: &AppSchema,
    ) -> Result<Box<dyn Query>> {
        // Validate the field exists in the schema
        if !app_schema.fields.contains_key(&self.field) {
            return Err(SearchDbError::Schema(format!(
                "field \"{}\" not found in schema",
                self.field
            )));
        }

        let present_field = tv_schema
            .get_field("__present__")
            .map_err(|_| SearchDbError::Schema("internal field __present__ not found".into()))?;

        let term = Term::from_field_text(present_field, &self.field);
        Ok(Box::new(tantivy::query::TermQuery::new(
            term,
            IndexRecordOption::Basic,
        )))
    }
}

#[cfg(test)]
mod tests {
    use crate::es_dsl::ElasticQueryDsl;
    use crate::schema::{FieldType, Schema};
    use std::collections::BTreeMap;

    fn schema_with_status() -> (tantivy::schema::Schema, Schema) {
        let schema = Schema {
            fields: BTreeMap::from([("status".into(), FieldType::Keyword)]),
        };
        let tv = schema.build_tantivy_schema();
        (tv, schema)
    }

    #[test]
    fn test_exists_query_deserialize() {
        let json = r#"{"exists": {"field": "status"}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        assert!(matches!(dsl, ElasticQueryDsl::Exists(_)));
    }

    #[test]
    fn test_exists_query_compile() {
        let (tv, schema) = schema_with_status();
        let json = r#"{"exists": {"field": "status"}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok(), "compile failed: {:?}", query.err());
    }

    #[test]
    fn test_exists_query_unknown_field() {
        let (tv, schema) = schema_with_status();
        let json = r#"{"exists": {"field": "nonexistent"}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let result = dsl.compile(&tv, &schema);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not found in schema"),
            "Expected 'not found in schema', got: {err}"
        );
    }
}

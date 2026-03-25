use serde::Deserialize;
use tantivy::query::Query;

use super::one_field_map::OneFieldMap;
use crate::error::{Result, SearchDbError};
use crate::schema::Schema as AppSchema;

/// Elasticsearch `terms` query -- multi-value OR on a single field.
///
/// Format: `{"terms": {"status": ["active", "pending"]}}`
#[derive(Debug, Clone, Deserialize)]
pub struct TermsQuery(pub OneFieldMap<Vec<serde_json::Value>>);

impl TermsQuery {
    pub fn compile(
        &self,
        _tv_schema: &tantivy::schema::Schema,
        _app_schema: &AppSchema,
    ) -> Result<Box<dyn Query>> {
        Err(SearchDbError::Schema(
            "terms query not yet implemented".into(),
        ))
    }
}

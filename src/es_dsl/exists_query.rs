use serde::Deserialize;
use tantivy::query::Query;

use crate::error::{Result, SearchDbError};
use crate::schema::Schema as AppSchema;

/// Elasticsearch `exists` query -- checks if a field has a non-null value.
///
/// Format: `{"exists": {"field": "status"}}`
#[derive(Debug, Clone, Deserialize)]
pub struct ExistsQuery {
    pub field: String,
}

impl ExistsQuery {
    pub fn compile(
        &self,
        _tv_schema: &tantivy::schema::Schema,
        _app_schema: &AppSchema,
    ) -> Result<Box<dyn Query>> {
        Err(SearchDbError::Schema(
            "exists query not yet implemented".into(),
        ))
    }
}

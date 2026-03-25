use serde::Deserialize;
use tantivy::query::Query;

use super::one_field_map::OneFieldMap;
use crate::error::{Result, SearchDbError};
use crate::schema::Schema as AppSchema;

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
        _tv_schema: &tantivy::schema::Schema,
        _app_schema: &AppSchema,
    ) -> Result<Box<dyn Query>> {
        Err(SearchDbError::Schema(
            "term query not yet implemented".into(),
        ))
    }
}

use serde::Deserialize;
use tantivy::query::Query;

use super::one_field_map::OneFieldMap;
use crate::error::{Result, SearchDbError};
use crate::schema::Schema as AppSchema;

/// Elasticsearch `range` query -- numeric/date range filtering.
///
/// Format: `{"range": {"age": {"gte": 18, "lt": 65}}}`
#[derive(Debug, Clone, Deserialize)]
pub struct RangeQuery(pub OneFieldMap<RangeQueryParams>);

#[derive(Debug, Clone, Deserialize)]
pub struct RangeQueryParams {
    #[serde(default)]
    pub gt: Option<serde_json::Value>,
    #[serde(default)]
    pub gte: Option<serde_json::Value>,
    #[serde(default)]
    pub lt: Option<serde_json::Value>,
    #[serde(default)]
    pub lte: Option<serde_json::Value>,
}

impl RangeQuery {
    pub fn compile(
        &self,
        _tv_schema: &tantivy::schema::Schema,
        _app_schema: &AppSchema,
    ) -> Result<Box<dyn Query>> {
        Err(SearchDbError::Schema(
            "range query not yet implemented".into(),
        ))
    }
}

use serde::Deserialize;
use tantivy::query::Query;

use super::one_field_map::OneFieldMap;
use crate::error::{Result, SearchDbError};
use crate::schema::Schema as AppSchema;

/// Elasticsearch `match` query -- tokenized full-text search.
///
/// Formats:
/// - `{"match": {"notes": "blood glucose"}}` (shorthand)
/// - `{"match": {"notes": {"query": "blood glucose", "operator": "and"}}}` (full)
#[derive(Debug, Clone, Deserialize)]
pub struct MatchQuery(pub OneFieldMap<MatchQueryParams>);

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum MatchQueryParams {
    Full(MatchQueryFullParams),
    Shorthand(String),
}

#[derive(Debug, Clone, Deserialize)]
pub struct MatchQueryFullParams {
    pub query: String,
    #[serde(default)]
    pub operator: MatchOperator,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum MatchOperator {
    #[default]
    Or,
    And,
}

impl MatchQuery {
    pub fn compile(
        &self,
        _tv_schema: &tantivy::schema::Schema,
        _app_schema: &AppSchema,
    ) -> Result<Box<dyn Query>> {
        Err(SearchDbError::Schema(
            "match query not yet implemented".into(),
        ))
    }
}

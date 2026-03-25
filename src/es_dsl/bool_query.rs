use serde::Deserialize;
use serde_with::formats::PreferMany;
use serde_with::{serde_as, OneOrMany};
use tantivy::query::Query;

use super::ElasticQueryDsl;
use crate::error::{Result, SearchDbError};
use crate::schema::Schema as AppSchema;

/// Elasticsearch `bool` query -- compound query with must/should/must_not/filter.
///
/// Each clause accepts either a single query or an array of queries.
/// Example: `{"bool": {"must": [{"term": {"status": "active"}}], "must_not": {"exists": {"field": "deleted"}}}}`
#[serde_as]
#[derive(Debug, Clone, Deserialize)]
pub struct BoolQuery {
    #[serde_as(as = "Option<OneOrMany<_, PreferMany>>")]
    #[serde(default)]
    pub must: Option<Vec<ElasticQueryDsl>>,

    #[serde_as(as = "Option<OneOrMany<_, PreferMany>>")]
    #[serde(default)]
    pub should: Option<Vec<ElasticQueryDsl>>,

    #[serde_as(as = "Option<OneOrMany<_, PreferMany>>")]
    #[serde(default)]
    pub must_not: Option<Vec<ElasticQueryDsl>>,

    #[serde_as(as = "Option<OneOrMany<_, PreferMany>>")]
    #[serde(default)]
    pub filter: Option<Vec<ElasticQueryDsl>>,
}

impl BoolQuery {
    pub fn compile(
        &self,
        _tv_schema: &tantivy::schema::Schema,
        _app_schema: &AppSchema,
    ) -> Result<Box<dyn Query>> {
        Err(SearchDbError::Schema(
            "bool query not yet implemented".into(),
        ))
    }
}

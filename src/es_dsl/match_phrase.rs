use serde::Deserialize;
use tantivy::query::Query;

use super::one_field_map::OneFieldMap;
use crate::error::{Result, SearchDbError};
use crate::schema::Schema as AppSchema;

/// Elasticsearch `match_phrase` query -- exact phrase matching.
///
/// Formats:
/// - `{"match_phrase": {"notes": "blood glucose"}}` (shorthand)
/// - `{"match_phrase": {"notes": {"query": "blood glucose", "slop": 1}}}` (full)
#[derive(Debug, Clone, Deserialize)]
pub struct MatchPhraseQuery(pub OneFieldMap<MatchPhraseParams>);

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum MatchPhraseParams {
    Full(MatchPhraseFullParams),
    Shorthand(String),
}

#[derive(Debug, Clone, Deserialize)]
pub struct MatchPhraseFullParams {
    pub query: String,
    #[serde(default)]
    pub slop: u32,
}

impl MatchPhraseQuery {
    pub fn compile(
        &self,
        _tv_schema: &tantivy::schema::Schema,
        _app_schema: &AppSchema,
    ) -> Result<Box<dyn Query>> {
        Err(SearchDbError::Schema(
            "match_phrase query not yet implemented".into(),
        ))
    }
}

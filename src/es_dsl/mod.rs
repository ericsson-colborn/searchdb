//! Elasticsearch query DSL compilation to tantivy queries.
//!
//! Architecture derived from quickwit-query (https://github.com/quickwit-oss/quickwit)
//! Copyright 2021-Present Datadog, Inc. Licensed under Apache 2.0.
//! Adapted for SearchDB's schema model and field types.

mod bool_query;
mod exists_query;
mod match_phrase;
mod match_query;
mod multi_match_query;
mod one_field_map;
mod prefix_query;
mod range_query;
mod term_query;
mod terms_query;
mod wildcard_query;

use serde::Deserialize;
use tantivy::query::Query;

use crate::error::Result;
use crate::schema::Schema as AppSchema;

pub use bool_query::BoolQuery;
pub use exists_query::ExistsQuery;
pub use match_phrase::MatchPhraseQuery;
pub use match_query::MatchQuery;
pub use multi_match_query::MultiMatchQuery;
pub use prefix_query::PrefixQuery;
pub use range_query::RangeQuery;
pub use term_query::TermQuery;
pub use terms_query::TermsQuery;
pub use wildcard_query::WildcardQuery;

/// Empty struct for match_all.
#[derive(Debug, Clone, Deserialize)]
pub struct MatchAllQuery {}

/// Empty struct for match_none.
#[derive(Debug, Clone, Deserialize)]
pub struct MatchNoneQuery {}

/// Top-level Elasticsearch query DSL enum.
///
/// Serde routes JSON keys like `"term"`, `"match_phrase"` to the correct variant
/// via `rename_all = "snake_case"`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ElasticQueryDsl {
    Bool(BoolQuery),
    Term(TermQuery),
    Terms(TermsQuery),
    Match(MatchQuery),
    MatchPhrase(MatchPhraseQuery),
    MultiMatch(MultiMatchQuery),
    Range(RangeQuery),
    Exists(ExistsQuery),
    Prefix(PrefixQuery),
    Wildcard(WildcardQuery),
    MatchAll(MatchAllQuery),
    MatchNone(MatchNoneQuery),
}

impl ElasticQueryDsl {
    /// Compile the ES DSL into a tantivy query.
    pub fn compile(
        &self,
        tv_schema: &tantivy::schema::Schema,
        app_schema: &AppSchema,
    ) -> Result<Box<dyn Query>> {
        match self {
            ElasticQueryDsl::Bool(q) => q.compile(tv_schema, app_schema),
            ElasticQueryDsl::Term(q) => q.compile(tv_schema, app_schema),
            ElasticQueryDsl::Terms(q) => q.compile(tv_schema, app_schema),
            ElasticQueryDsl::Match(q) => q.compile(tv_schema, app_schema),
            ElasticQueryDsl::MatchPhrase(q) => q.compile(tv_schema, app_schema),
            ElasticQueryDsl::MultiMatch(q) => q.compile(tv_schema, app_schema),
            ElasticQueryDsl::Range(q) => q.compile(tv_schema, app_schema),
            ElasticQueryDsl::Exists(q) => q.compile(tv_schema, app_schema),
            ElasticQueryDsl::Prefix(q) => q.compile(tv_schema, app_schema),
            ElasticQueryDsl::Wildcard(q) => q.compile(tv_schema, app_schema),
            ElasticQueryDsl::MatchAll(_) => Ok(Box::new(tantivy::query::AllQuery)),
            ElasticQueryDsl::MatchNone(_) => Ok(Box::new(tantivy::query::EmptyQuery)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_match_all() {
        let json = r#"{"match_all":{}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        assert!(matches!(dsl, ElasticQueryDsl::MatchAll(_)));
    }

    #[test]
    fn test_deserialize_match_none() {
        let json = r#"{"match_none":{}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        assert!(matches!(dsl, ElasticQueryDsl::MatchNone(_)));
    }

    #[test]
    fn test_unknown_query_type_errors() {
        let json = r#"{"fuzzy":{"name":"test"}}"#;
        let result: std::result::Result<ElasticQueryDsl, _> = serde_json::from_str(json);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("unknown variant"),
            "Error should mention unknown variant, got: {err_msg}"
        );
    }

    #[test]
    fn test_compile_match_all() {
        let schema = crate::schema::Schema {
            fields: std::collections::BTreeMap::new(),
        };
        let tv_schema = schema.build_tantivy_schema();
        let dsl: ElasticQueryDsl = serde_json::from_str(r#"{"match_all":{}}"#).unwrap();
        let query = dsl.compile(&tv_schema, &schema);
        assert!(query.is_ok());
    }

    #[test]
    fn test_compile_match_none() {
        let schema = crate::schema::Schema {
            fields: std::collections::BTreeMap::new(),
        };
        let tv_schema = schema.build_tantivy_schema();
        let dsl: ElasticQueryDsl = serde_json::from_str(r#"{"match_none":{}}"#).unwrap();
        let query = dsl.compile(&tv_schema, &schema);
        assert!(query.is_ok());
    }
}

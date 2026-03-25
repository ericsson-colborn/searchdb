use serde::Deserialize;
use tantivy::query::{PhraseQuery, Query};
use tantivy::Term;

use super::match_query::{tokenize, tokenize_for_field};
use super::one_field_map::OneFieldMap;
use crate::error::Result;
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
        tv_schema: &tantivy::schema::Schema,
        _app_schema: &AppSchema,
    ) -> Result<Box<dyn Query>> {
        let field_name = &self.0.field;
        let (field, tokenizer_name) = tokenize_for_field(tv_schema, field_name)?;

        let (query_text, slop) = match &self.0.value {
            MatchPhraseParams::Full(full) => (full.query.as_str(), full.slop),
            MatchPhraseParams::Shorthand(text) => (text.as_str(), 0),
        };

        let tokens = tokenize(query_text, &tokenizer_name)?;

        if tokens.is_empty() {
            return Ok(Box::new(tantivy::query::EmptyQuery));
        }

        if tokens.len() == 1 {
            let term = Term::from_field_text(field, &tokens[0]);
            return Ok(Box::new(tantivy::query::TermQuery::new(
                term,
                tantivy::schema::IndexRecordOption::WithFreqsAndPositions,
            )));
        }

        let terms: Vec<Term> = tokens
            .iter()
            .map(|t| Term::from_field_text(field, t))
            .collect();

        if slop > 0 {
            let terms_with_offset: Vec<(usize, Term)> = terms.into_iter().enumerate().collect();
            Ok(Box::new(PhraseQuery::new_with_offset_and_slop(
                terms_with_offset,
                slop,
            )))
        } else {
            Ok(Box::new(PhraseQuery::new(terms)))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::es_dsl::ElasticQueryDsl;
    use crate::schema::{FieldType, Schema};
    use std::collections::BTreeMap;

    fn text_schema() -> (tantivy::schema::Schema, Schema) {
        let schema = Schema {
            fields: BTreeMap::from([("notes".into(), FieldType::Text)]),
        };
        let tv = schema.build_tantivy_schema();
        (tv, schema)
    }

    #[test]
    fn test_match_phrase_deserialize_shorthand() {
        let json = r#"{"match_phrase": {"notes": "blood glucose level"}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        assert!(matches!(dsl, ElasticQueryDsl::MatchPhrase(_)));
    }

    #[test]
    fn test_match_phrase_deserialize_full() {
        let json = r#"{"match_phrase": {"notes": {"query": "blood glucose", "slop": 2}}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        assert!(matches!(dsl, ElasticQueryDsl::MatchPhrase(_)));
    }

    #[test]
    fn test_match_phrase_compile() {
        let (tv, schema) = text_schema();
        let json = r#"{"match_phrase": {"notes": "blood glucose"}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok(), "compile failed: {:?}", query.err());
    }

    #[test]
    fn test_match_phrase_with_slop() {
        let (tv, schema) = text_schema();
        let json = r#"{"match_phrase": {"notes": {"query": "blood glucose", "slop": 2}}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok(), "compile failed: {:?}", query.err());
    }

    #[test]
    fn test_match_phrase_empty_text() {
        let (tv, schema) = text_schema();
        let json = r#"{"match_phrase": {"notes": ""}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok());
    }

    #[test]
    fn test_match_phrase_single_term() {
        let (tv, schema) = text_schema();
        let json = r#"{"match_phrase": {"notes": "glucose"}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok());
    }
}

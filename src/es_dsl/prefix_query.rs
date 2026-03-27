use serde::Deserialize;
use tantivy::query::Query;
use tantivy::schema::FieldType as TvFieldType;
use tantivy::Term;

use super::one_field_map::OneFieldMap;
use crate::error::{Result, SearchDbError};
use crate::schema::Schema as AppSchema;

/// Elasticsearch `prefix` query -- matches documents with a field value
/// starting with the given prefix.
///
/// Formats:
/// - `{"prefix": {"status": "act"}}` (shorthand)
/// - `{"prefix": {"status": {"value": "act"}}}` (full)
///
/// For keyword fields (raw tokenizer): uses tantivy `RegexQuery` with `^prefix.*`.
/// For text fields (tokenized): uses tantivy `PhrasePrefixQuery` on the tokenized
/// prefix, matching any term starting with the last token.
#[derive(Debug, Clone, Deserialize)]
pub struct PrefixQuery(pub OneFieldMap<PrefixValue>);

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum PrefixValue {
    Object { value: String },
    Simple(String),
}

impl PrefixQuery {
    pub fn compile(
        &self,
        tv_schema: &tantivy::schema::Schema,
        _app_schema: &AppSchema,
    ) -> Result<Box<dyn Query>> {
        let field_name = &self.0.field;
        let prefix_str = match &self.0.value {
            PrefixValue::Simple(s) => s.as_str(),
            PrefixValue::Object { value } => value.as_str(),
        };

        let field = tv_schema.get_field(field_name).map_err(|_| {
            SearchDbError::Schema(format!("field \"{field_name}\" not found in schema"))
        })?;

        let field_entry = tv_schema.get_field_entry(field);

        match field_entry.field_type() {
            TvFieldType::Str(opts) => {
                let tokenizer_name = opts
                    .get_indexing_options()
                    .map(|i| i.tokenizer())
                    .unwrap_or("raw");

                if tokenizer_name == "raw" {
                    // Keyword field: use RegexQuery with escaped prefix
                    let escaped = regex_syntax_escape(prefix_str);
                    let pattern = format!("{escaped}.*");
                    let regex_query = tantivy::query::RegexQuery::from_pattern(&pattern, field)
                        .map_err(|e| {
                            SearchDbError::Schema(format!("prefix query regex failed: {e}"))
                        })?;
                    Ok(Box::new(regex_query))
                } else {
                    // Text field: tokenize then use PhrasePrefixQuery
                    compile_text_prefix(field, prefix_str, tokenizer_name)
                }
            }
            _ => Err(SearchDbError::Schema(format!(
                "prefix query requires a string field, but \"{field_name}\" is not"
            ))),
        }
    }
}

/// Compile a prefix query for a text (tokenized) field using PhrasePrefixQuery.
fn compile_text_prefix(
    field: tantivy::schema::Field,
    prefix_str: &str,
    tokenizer_name: &str,
) -> Result<Box<dyn Query>> {
    use super::match_query::tokenize;

    let tokens = tokenize(prefix_str, tokenizer_name)?;

    if tokens.is_empty() {
        return Ok(Box::new(tantivy::query::EmptyQuery));
    }

    // Build terms for all tokens -- the last one becomes the prefix
    let terms: Vec<Term> = tokens
        .iter()
        .map(|t| Term::from_field_text(field, t))
        .collect();

    let query = tantivy::query::PhrasePrefixQuery::new(terms);
    Ok(Box::new(query))
}

/// Escape special regex characters for tantivy-fst regex.
///
/// tantivy-fst uses `regex-syntax` under the hood, so we escape
/// the standard regex metacharacters.
fn regex_syntax_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len() * 2);
    for ch in input.chars() {
        if "\\^$.|+*?()[]{}".contains(ch) {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

#[cfg(test)]
mod tests {
    use crate::es_dsl::ElasticQueryDsl;
    use crate::schema::{FieldType, Schema};
    use std::collections::BTreeMap;

    #[test]
    fn test_prefix_query_deserialize_shorthand() {
        let json = r#"{"prefix": {"status": "act"}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        assert!(matches!(dsl, ElasticQueryDsl::Prefix(_)));
    }

    #[test]
    fn test_prefix_query_deserialize_full() {
        let json = r#"{"prefix": {"status": {"value": "act"}}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        assert!(matches!(dsl, ElasticQueryDsl::Prefix(_)));
    }

    #[test]
    fn test_prefix_query_compile_keyword() {
        let schema = Schema {
            fields: BTreeMap::from([("status".into(), FieldType::Keyword)]),
        };
        let tv = schema.build_tantivy_schema();
        let json = r#"{"prefix": {"status": "act"}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok(), "compile failed: {:?}", query.err());
    }

    #[test]
    fn test_prefix_query_compile_text() {
        let schema = Schema {
            fields: BTreeMap::from([("notes".into(), FieldType::Text)]),
        };
        let tv = schema.build_tantivy_schema();
        let json = r#"{"prefix": {"notes": "glu"}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok(), "compile failed: {:?}", query.err());
    }

    #[test]
    fn test_prefix_query_unknown_field() {
        let schema = Schema {
            fields: BTreeMap::from([("status".into(), FieldType::Keyword)]),
        };
        let tv = schema.build_tantivy_schema();
        let json = r#"{"prefix": {"missing": "act"}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let result = dsl.compile(&tv, &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_prefix_query_empty_string() {
        let schema = Schema {
            fields: BTreeMap::from([("status".into(), FieldType::Keyword)]),
        };
        let tv = schema.build_tantivy_schema();
        let json = r#"{"prefix": {"status": ""}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok(), "compile failed: {:?}", query.err());
    }

    #[test]
    fn test_prefix_query_special_chars() {
        let schema = Schema {
            fields: BTreeMap::from([("path".into(), FieldType::Keyword)]),
        };
        let tv = schema.build_tantivy_schema();
        // Prefix with regex metacharacters should be escaped
        let json = r#"{"prefix": {"path": "/usr/bin/"}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok(), "compile failed: {:?}", query.err());
    }

    #[test]
    fn test_regex_syntax_escape() {
        assert_eq!(super::regex_syntax_escape("hello"), "hello");
        assert_eq!(super::regex_syntax_escape("a.b*c"), r"a\.b\*c");
        assert_eq!(super::regex_syntax_escape("(test)"), r"\(test\)");
    }
}

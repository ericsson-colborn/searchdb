use serde::Deserialize;
use tantivy::query::Query;
use tantivy::schema::FieldType as TvFieldType;

use super::one_field_map::OneFieldMap;
use crate::error::{Result, SearchDbError};
use crate::schema::Schema as AppSchema;

/// Elasticsearch `wildcard` query -- pattern matching with `*` and `?` wildcards.
///
/// Formats:
/// - `{"wildcard": {"status": {"value": "act*"}}}`  (full)
/// - `{"wildcard": {"status": "act*"}}`              (shorthand)
///
/// Wildcard syntax:
/// - `*` matches any sequence of characters (including empty)
/// - `?` matches exactly one character
///
/// Compiled to a tantivy `RegexQuery` by converting wildcard syntax to regex.
#[derive(Debug, Clone, Deserialize)]
pub struct WildcardQuery(pub OneFieldMap<WildcardValue>);

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum WildcardValue {
    Object { value: String },
    Simple(String),
}

impl WildcardQuery {
    pub fn compile(
        &self,
        tv_schema: &tantivy::schema::Schema,
        _app_schema: &AppSchema,
    ) -> Result<Box<dyn Query>> {
        let field_name = &self.0.field;
        let pattern = match &self.0.value {
            WildcardValue::Simple(s) => s.as_str(),
            WildcardValue::Object { value } => value.as_str(),
        };

        let field = tv_schema.get_field(field_name).map_err(|_| {
            SearchDbError::Schema(format!("field \"{field_name}\" not found in schema"))
        })?;

        let field_entry = tv_schema.get_field_entry(field);
        match field_entry.field_type() {
            TvFieldType::Str(_) => {}
            _ => {
                return Err(SearchDbError::Schema(format!(
                    "wildcard query requires a string field, but \"{field_name}\" is not"
                )));
            }
        }

        let regex_pattern = wildcard_to_regex(pattern);

        let regex_query = tantivy::query::RegexQuery::from_pattern(&regex_pattern, field)
            .map_err(|e| SearchDbError::Schema(format!("wildcard query failed: {e}")))?;

        Ok(Box::new(regex_query))
    }
}

/// Convert an Elasticsearch wildcard pattern to a tantivy-fst regex pattern.
///
/// - `*` becomes `.*`
/// - `?` becomes `.`
/// - All other regex metacharacters are escaped
///
/// Note: tantivy-fst regex implicitly anchors (matches full term), so no
/// explicit `^` or `$` anchors are needed.
fn wildcard_to_regex(pattern: &str) -> String {
    let mut regex = String::with_capacity(pattern.len() * 2);
    let mut chars = pattern.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\\' => {
                // Escape sequence: pass through the next character literally
                if let Some(next) = chars.next() {
                    if "\\^$.|+*?()[]{}".contains(next) {
                        regex.push('\\');
                    }
                    regex.push(next);
                } else {
                    regex.push_str("\\\\");
                }
            }
            '*' => regex.push_str(".*"),
            '?' => regex.push('.'),
            c if "\\^$.|+()[]{}".contains(c) => {
                regex.push('\\');
                regex.push(c);
            }
            c => regex.push(c),
        }
    }

    regex
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::es_dsl::ElasticQueryDsl;
    use crate::schema::{FieldType, Schema};
    use std::collections::BTreeMap;

    #[test]
    fn test_wildcard_to_regex_basic() {
        assert_eq!(wildcard_to_regex("test*"), "test.*");
        assert_eq!(wildcard_to_regex("te?t"), "te.t");
        assert_eq!(wildcard_to_regex("*test*"), ".*test.*");
        assert_eq!(wildcard_to_regex("t*e?t"), "t.*e.t");
    }

    #[test]
    fn test_wildcard_to_regex_escapes() {
        assert_eq!(wildcard_to_regex("a.b"), "a\\.b");
        assert_eq!(wildcard_to_regex("a(b)"), "a\\(b\\)");
        assert_eq!(wildcard_to_regex("a[b]"), "a\\[b\\]");
    }

    #[test]
    fn test_wildcard_to_regex_escaped_wildcards() {
        // Backslash-escaped wildcards should be literal
        assert_eq!(wildcard_to_regex(r"test\*"), "test\\*");
        assert_eq!(wildcard_to_regex(r"test\?"), "test\\?");
    }

    #[test]
    fn test_wildcard_query_deserialize_shorthand() {
        let json = r#"{"wildcard": {"status": "act*"}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        assert!(matches!(dsl, ElasticQueryDsl::Wildcard(_)));
    }

    #[test]
    fn test_wildcard_query_deserialize_full() {
        let json = r#"{"wildcard": {"status": {"value": "act*"}}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        assert!(matches!(dsl, ElasticQueryDsl::Wildcard(_)));
    }

    #[test]
    fn test_wildcard_query_compile_keyword() {
        let schema = Schema {
            fields: BTreeMap::from([("status".into(), FieldType::Keyword)]),
        };
        let tv = schema.build_tantivy_schema();
        let json = r#"{"wildcard": {"status": {"value": "act*"}}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok(), "compile failed: {:?}", query.err());
    }

    #[test]
    fn test_wildcard_query_compile_text() {
        let schema = Schema {
            fields: BTreeMap::from([("notes".into(), FieldType::Text)]),
        };
        let tv = schema.build_tantivy_schema();
        let json = r#"{"wildcard": {"notes": "te*t"}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok(), "compile failed: {:?}", query.err());
    }

    #[test]
    fn test_wildcard_query_unknown_field() {
        let schema = Schema {
            fields: BTreeMap::from([("status".into(), FieldType::Keyword)]),
        };
        let tv = schema.build_tantivy_schema();
        let json = r#"{"wildcard": {"missing": {"value": "act*"}}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let result = dsl.compile(&tv, &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_wildcard_query_numeric_field_errors() {
        let schema = Schema {
            fields: BTreeMap::from([("age".into(), FieldType::Numeric)]),
        };
        let tv = schema.build_tantivy_schema();
        let json = r#"{"wildcard": {"age": {"value": "3*"}}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let result = dsl.compile(&tv, &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_wildcard_single_question_mark() {
        let schema = Schema {
            fields: BTreeMap::from([("code".into(), FieldType::Keyword)]),
        };
        let tv = schema.build_tantivy_schema();
        let json = r#"{"wildcard": {"code": "A?C"}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok(), "compile failed: {:?}", query.err());
    }
}

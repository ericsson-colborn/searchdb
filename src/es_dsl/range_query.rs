use std::ops::Bound;

use serde::Deserialize;
use tantivy::query::{Query, RangeQuery as TvRangeQuery};
use tantivy::Term;

use super::one_field_map::OneFieldMap;
use super::term_query::parse_date;
use crate::error::{Result, SearchDbError};
use crate::schema::{FieldType, Schema as AppSchema};

/// Elasticsearch `range` query -- numeric/date range filtering.
///
/// Format: `{"range": {"age": {"gte": 18, "lt": 65}}}`
///
/// Only works on `numeric` and `date` fields (which are fast fields).
/// Range queries on `keyword` or `text` fields return an error.
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
        tv_schema: &tantivy::schema::Schema,
        app_schema: &AppSchema,
    ) -> Result<Box<dyn Query>> {
        let field_name = &self.0.field;
        let field = tv_schema.get_field(field_name).map_err(|_| {
            SearchDbError::Schema(format!("field \"{field_name}\" not found in schema"))
        })?;

        let field_type = app_schema.fields.get(field_name).ok_or_else(|| {
            SearchDbError::Schema(format!("field \"{field_name}\" not found in schema"))
        })?;

        let params = &self.0.value;

        match field_type {
            FieldType::Numeric => compile_numeric(field, params),
            FieldType::Date => compile_date(field, params),
            other => Err(SearchDbError::Schema(format!(
                "range query not supported on {other:?} field \"{field_name}\" -- use numeric or date fields"
            ))),
        }
    }
}

fn compile_numeric(
    field: tantivy::schema::Field,
    params: &RangeQueryParams,
) -> Result<Box<dyn Query>> {
    let lower = if let Some(ref v) = params.gte {
        let n = v.as_f64().ok_or_else(|| {
            SearchDbError::Schema(format!("range gte expected number, got {v}"))
        })?;
        Bound::Included(Term::from_field_f64(field, n))
    } else if let Some(ref v) = params.gt {
        let n = v.as_f64().ok_or_else(|| {
            SearchDbError::Schema(format!("range gt expected number, got {v}"))
        })?;
        Bound::Excluded(Term::from_field_f64(field, n))
    } else {
        Bound::Unbounded
    };

    let upper = if let Some(ref v) = params.lte {
        let n = v.as_f64().ok_or_else(|| {
            SearchDbError::Schema(format!("range lte expected number, got {v}"))
        })?;
        Bound::Included(Term::from_field_f64(field, n))
    } else if let Some(ref v) = params.lt {
        let n = v.as_f64().ok_or_else(|| {
            SearchDbError::Schema(format!("range lt expected number, got {v}"))
        })?;
        Bound::Excluded(Term::from_field_f64(field, n))
    } else {
        Bound::Unbounded
    };

    Ok(Box::new(TvRangeQuery::new(lower, upper)))
}

fn compile_date(
    field: tantivy::schema::Field,
    params: &RangeQueryParams,
) -> Result<Box<dyn Query>> {
    let lower = if let Some(ref v) = params.gte {
        let s = v.as_str().ok_or_else(|| {
            SearchDbError::Schema(format!("range gte expected date string, got {v}"))
        })?;
        Bound::Included(Term::from_field_date(field, parse_date(s)?))
    } else if let Some(ref v) = params.gt {
        let s = v.as_str().ok_or_else(|| {
            SearchDbError::Schema(format!("range gt expected date string, got {v}"))
        })?;
        Bound::Excluded(Term::from_field_date(field, parse_date(s)?))
    } else {
        Bound::Unbounded
    };

    let upper = if let Some(ref v) = params.lte {
        let s = v.as_str().ok_or_else(|| {
            SearchDbError::Schema(format!("range lte expected date string, got {v}"))
        })?;
        Bound::Included(Term::from_field_date(field, parse_date(s)?))
    } else if let Some(ref v) = params.lt {
        let s = v.as_str().ok_or_else(|| {
            SearchDbError::Schema(format!("range lt expected date string, got {v}"))
        })?;
        Bound::Excluded(Term::from_field_date(field, parse_date(s)?))
    } else {
        Bound::Unbounded
    };

    Ok(Box::new(TvRangeQuery::new(lower, upper)))
}

#[cfg(test)]
mod tests {
    use crate::es_dsl::ElasticQueryDsl;
    use crate::schema::{FieldType, Schema};
    use std::collections::BTreeMap;

    #[test]
    fn test_range_query_deserialize() {
        let json = r#"{"range": {"age": {"gte": 18, "lt": 65}}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        assert!(matches!(dsl, ElasticQueryDsl::Range(_)));
    }

    #[test]
    fn test_range_query_compile_numeric() {
        let schema = Schema {
            fields: BTreeMap::from([("age".into(), FieldType::Numeric)]),
        };
        let tv = schema.build_tantivy_schema();
        let json = r#"{"range": {"age": {"gte": 18, "lt": 65}}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok(), "compile failed: {:?}", query.err());
    }

    #[test]
    fn test_range_query_compile_date() {
        let schema = Schema {
            fields: BTreeMap::from([("created_at".into(), FieldType::Date)]),
        };
        let tv = schema.build_tantivy_schema();
        let json =
            r#"{"range": {"created_at": {"gte": "2024-01-01T00:00:00Z", "lt": "2025-01-01T00:00:00Z"}}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok(), "compile failed: {:?}", query.err());
    }

    #[test]
    fn test_range_query_compile_date_only() {
        let schema = Schema {
            fields: BTreeMap::from([("created_at".into(), FieldType::Date)]),
        };
        let tv = schema.build_tantivy_schema();
        let json = r#"{"range": {"created_at": {"gte": "2024-01-01"}}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok(), "compile failed: {:?}", query.err());
    }

    #[test]
    fn test_range_query_on_text_field_errors() {
        let schema = Schema {
            fields: BTreeMap::from([("notes".into(), FieldType::Text)]),
        };
        let tv = schema.build_tantivy_schema();
        let json = r#"{"range": {"notes": {"gte": "a", "lt": "z"}}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let result = dsl.compile(&tv, &schema);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not supported"),
            "Expected 'not supported', got: {err}"
        );
    }

    #[test]
    fn test_range_query_one_sided() {
        let schema = Schema {
            fields: BTreeMap::from([("age".into(), FieldType::Numeric)]),
        };
        let tv = schema.build_tantivy_schema();
        // Only lower bound
        let json = r#"{"range": {"age": {"gt": 0}}}"#;
        let dsl: ElasticQueryDsl = serde_json::from_str(json).unwrap();
        let query = dsl.compile(&tv, &schema);
        assert!(query.is_ok(), "compile failed: {:?}", query.err());
    }
}

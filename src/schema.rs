use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::LazyLock;
use tantivy::schema::{DateOptions, NumericOptions, SchemaBuilder, TextFieldIndexing, TextOptions};

static ISO_DATETIME_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}").unwrap());

/// Returns true if the string looks like an ISO 8601 datetime (YYYY-MM-DDThh:mm:ss...).
pub fn looks_like_date(s: &str) -> bool {
    ISO_DATETIME_RE.is_match(s)
}

/// SearchDB field types — maps to ES-like concepts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    /// Exact match, case-sensitive (IDs, codes, statuses)
    Keyword,
    /// Tokenized, stemmed, full-text searchable
    Text,
    /// f64 — range queries (gte, lte, gt, lt)
    Numeric,
    /// ISO 8601 datetime — range queries
    Date,
}

/// Schema declaration — maps field names to types.
/// Serialized to searchdb.json (legacy filename) alongside Delta metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub fields: BTreeMap<String, FieldType>,
}

impl Schema {
    /// Parse schema from a JSON string like `{"fields":{"name":"keyword","notes":"text"}}`.
    pub fn from_json(json: &str) -> crate::error::Result<Self> {
        serde_json::from_str(json)
            .map_err(|e| crate::error::SearchDbError::Schema(format!("Invalid schema JSON: {e}")))
    }

    /// Build the tantivy schema from this declaration.
    ///
    /// Adds three internal fields:
    /// - `_id`: raw-tokenized text, stored (document identity for upsert)
    /// - `_source`: raw-tokenized text, stored (verbatim JSON for round-trip)
    /// - `__present__`: raw-tokenized text, NOT stored (null/not-null tracking)
    pub fn build_tantivy_schema(&self) -> tantivy::schema::Schema {
        let mut builder = SchemaBuilder::new();

        // -- System fields --
        let raw_stored = TextOptions::default().set_stored().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("raw")
                .set_index_option(tantivy::schema::IndexRecordOption::Basic),
        );

        let raw_not_stored = TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("raw")
                .set_index_option(tantivy::schema::IndexRecordOption::Basic),
        );

        builder.add_text_field("_id", raw_stored.clone());
        builder.add_text_field("_source", raw_stored);
        builder.add_text_field("__present__", raw_not_stored);

        // -- User fields --
        for (name, field_type) in &self.fields {
            match field_type {
                FieldType::Keyword => {
                    let opts = TextOptions::default().set_stored().set_indexing_options(
                        TextFieldIndexing::default()
                            .set_tokenizer("raw")
                            .set_index_option(tantivy::schema::IndexRecordOption::Basic),
                    );
                    builder.add_text_field(name, opts);
                }
                FieldType::Text => {
                    let opts = TextOptions::default().set_stored().set_indexing_options(
                        TextFieldIndexing::default()
                            .set_tokenizer("en_stem")
                            .set_index_option(
                                tantivy::schema::IndexRecordOption::WithFreqsAndPositions,
                            ),
                    );
                    builder.add_text_field(name, opts);
                }
                FieldType::Numeric => {
                    let opts = NumericOptions::default()
                        .set_stored()
                        .set_indexed()
                        .set_fast();
                    builder.add_f64_field(name, opts);
                }
                FieldType::Date => {
                    let opts = DateOptions::default().set_stored().set_indexed().set_fast();
                    builder.add_date_field(name, opts);
                }
            }
        }

        builder.build()
    }
}

/// Infer a FieldType from a single JSON value.
///
/// Returns None for null, arrays, and objects (not indexable in v1).
/// Strings are checked for ISO 8601 datetime pattern before defaulting to keyword.
/// Booleans are treated as keywords ("true"/"false").
pub fn infer_field_type(value: &serde_json::Value) -> Option<FieldType> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::Bool(_) => Some(FieldType::Keyword),
        serde_json::Value::Number(_) => Some(FieldType::Numeric),
        serde_json::Value::String(s) => {
            if looks_like_date(s) {
                Some(FieldType::Date)
            } else {
                Some(FieldType::Keyword)
            }
        }
        serde_json::Value::Array(_) => None,
        serde_json::Value::Object(_) => None,
    }
}

/// Internal fields that should not be inferred from document data.
const INTERNAL_FIELDS: &[&str] = &["_id", "_source", "__present__"];

/// Infer a Schema from a batch of JSON documents.
///
/// Scans all documents and collects the first non-null type for each field.
/// Skips internal fields (_id, _source, __present__), arrays, and objects.
pub fn infer_schema(docs: &[serde_json::Value]) -> Schema {
    let mut fields = BTreeMap::new();
    for doc in docs {
        if let Some(obj) = doc.as_object() {
            for (key, value) in obj {
                if INTERNAL_FIELDS.contains(&key.as_str()) {
                    continue;
                }
                if fields.contains_key(key) {
                    continue; // first non-null wins
                }
                if let Some(ft) = infer_field_type(value) {
                    fields.insert(key.clone(), ft);
                }
            }
        }
    }
    Schema { fields }
}

/// Merge a discovered schema into an existing one.
///
/// Additive only: new fields from `discovered` are added. Existing fields
/// in `base` keep their original type (base wins on conflict).
pub fn merge_schemas(base: &Schema, discovered: &Schema) -> Schema {
    let mut fields = base.fields.clone();
    for (name, field_type) in &discovered.fields {
        fields.entry(name.clone()).or_insert(field_type.clone());
    }
    Schema { fields }
}

/// Merge schemas and return the list of newly added field names.
///
/// Same as `merge_schemas` but also reports which fields were actually new.
pub fn merge_schemas_with_diff(base: &Schema, discovered: &Schema) -> (Schema, Vec<String>) {
    let mut fields = base.fields.clone();
    let mut new_fields = Vec::new();
    for (name, field_type) in &discovered.fields {
        if !fields.contains_key(name) {
            fields.insert(name.clone(), field_type.clone());
            new_fields.push(name.clone());
        }
    }
    (Schema { fields }, new_fields)
}

/// Map an Arrow DataType to a SearchDB FieldType.
/// Returns None for complex types (List, Struct, Map, etc.).
#[cfg(feature = "delta")]
fn arrow_type_to_field_type(dt: &arrow::datatypes::DataType) -> Option<FieldType> {
    use arrow::datatypes::DataType;
    match dt {
        DataType::Utf8 | DataType::LargeUtf8 => Some(FieldType::Keyword),
        DataType::Boolean => Some(FieldType::Keyword),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64 => Some(FieldType::Numeric),
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => Some(FieldType::Date),
        _ => None,
    }
}

/// Infer a SearchDB Schema from an Arrow schema (e.g., from a Delta table).
#[cfg(feature = "delta")]
pub fn from_arrow_schema(arrow_schema: &arrow::datatypes::Schema) -> Schema {
    let mut fields = BTreeMap::new();
    for field in arrow_schema.fields() {
        let name = field.name();
        if INTERNAL_FIELDS.contains(&name.as_str()) {
            continue;
        }
        if let Some(ft) = arrow_type_to_field_type(field.data_type()) {
            fields.insert(name.clone(), ft);
        }
    }
    Schema { fields }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_looks_like_date_iso8601() {
        assert!(looks_like_date("2024-01-15T10:30:00Z"));
        assert!(looks_like_date("2024-01-15T10:30:00+05:30"));
        assert!(looks_like_date("2024-01-15T10:30:00.123Z"));
        assert!(looks_like_date("2024-01-15 10:30:00"));
    }

    #[test]
    fn test_looks_like_date_rejects_non_dates() {
        assert!(!looks_like_date("hello world"));
        assert!(!looks_like_date("2024-01-15")); // date-only, no time
        assert!(!looks_like_date("Jan 15, 2024"));
        assert!(!looks_like_date("42"));
        assert!(!looks_like_date(""));
        assert!(!looks_like_date("2024-1-15T10:30:00Z")); // single-digit month
    }

    #[test]
    fn test_infer_field_type_string() {
        let val = serde_json::json!("hello");
        assert_eq!(infer_field_type(&val), Some(FieldType::Keyword));
    }

    #[test]
    fn test_infer_field_type_number() {
        assert_eq!(
            infer_field_type(&serde_json::json!(42)),
            Some(FieldType::Numeric)
        );
        assert_eq!(
            infer_field_type(&serde_json::json!(3.14)),
            Some(FieldType::Numeric)
        );
    }

    #[test]
    fn test_infer_field_type_date_string() {
        let val = serde_json::json!("2024-01-15T10:30:00Z");
        assert_eq!(infer_field_type(&val), Some(FieldType::Date));
    }

    #[test]
    fn test_infer_field_type_boolean() {
        assert_eq!(
            infer_field_type(&serde_json::json!(true)),
            Some(FieldType::Keyword)
        );
        assert_eq!(
            infer_field_type(&serde_json::json!(false)),
            Some(FieldType::Keyword)
        );
    }

    #[test]
    fn test_infer_field_type_null() {
        assert_eq!(infer_field_type(&serde_json::Value::Null), None);
    }

    #[test]
    fn test_infer_field_type_array() {
        assert_eq!(infer_field_type(&serde_json::json!([1, 2, 3])), None);
    }

    #[test]
    fn test_infer_field_type_object() {
        assert_eq!(infer_field_type(&serde_json::json!({"nested": true})), None);
    }

    #[test]
    fn test_infer_schema_basic() {
        let docs = vec![
            serde_json::json!({"name": "alice", "age": 30, "created": "2024-01-15T10:00:00Z"}),
            serde_json::json!({"name": "bob", "age": 25, "active": true}),
        ];
        let schema = infer_schema(&docs);
        assert_eq!(schema.fields["name"], FieldType::Keyword);
        assert_eq!(schema.fields["age"], FieldType::Numeric);
        assert_eq!(schema.fields["created"], FieldType::Date);
        assert_eq!(schema.fields["active"], FieldType::Keyword);
        assert_eq!(schema.fields.len(), 4);
    }

    #[test]
    fn test_infer_schema_skips_id() {
        let docs = vec![serde_json::json!({"_id": "abc", "name": "alice"})];
        let schema = infer_schema(&docs);
        assert!(!schema.fields.contains_key("_id"));
        assert_eq!(schema.fields.len(), 1);
    }

    #[test]
    fn test_infer_schema_first_non_null_wins() {
        let docs = vec![
            serde_json::json!({"status": null}),
            serde_json::json!({"status": "active"}),
        ];
        let schema = infer_schema(&docs);
        assert_eq!(schema.fields["status"], FieldType::Keyword);
    }

    #[test]
    fn test_infer_schema_skips_nested_and_arrays() {
        let docs =
            vec![serde_json::json!({"name": "alice", "tags": ["a", "b"], "addr": {"city": "NYC"}})];
        let schema = infer_schema(&docs);
        assert_eq!(schema.fields.len(), 1);
        assert!(schema.fields.contains_key("name"));
    }

    #[test]
    fn test_infer_schema_empty_docs() {
        let docs: Vec<serde_json::Value> = vec![];
        let schema = infer_schema(&docs);
        assert!(schema.fields.is_empty());
    }

    #[test]
    fn test_merge_schemas_adds_new_fields() {
        let existing = Schema {
            fields: BTreeMap::from([("name".into(), FieldType::Keyword)]),
        };
        let discovered = Schema {
            fields: BTreeMap::from([
                ("name".into(), FieldType::Keyword),
                ("age".into(), FieldType::Numeric),
            ]),
        };
        let merged = merge_schemas(&existing, &discovered);
        assert_eq!(merged.fields.len(), 2);
        assert_eq!(merged.fields["name"], FieldType::Keyword);
        assert_eq!(merged.fields["age"], FieldType::Numeric);
    }

    #[test]
    fn test_merge_schemas_existing_type_wins() {
        let existing = Schema {
            fields: BTreeMap::from([("status".into(), FieldType::Text)]),
        };
        let discovered = Schema {
            fields: BTreeMap::from([("status".into(), FieldType::Keyword)]),
        };
        let merged = merge_schemas(&existing, &discovered);
        assert_eq!(merged.fields["status"], FieldType::Text); // existing wins
    }

    #[test]
    fn test_merge_schemas_returns_new_fields_only() {
        let existing = Schema {
            fields: BTreeMap::from([("name".into(), FieldType::Keyword)]),
        };
        let discovered = Schema {
            fields: BTreeMap::from([("name".into(), FieldType::Keyword)]),
        };
        let (_merged, new_fields) = merge_schemas_with_diff(&existing, &discovered);
        assert!(new_fields.is_empty());
    }

    #[test]
    fn test_merge_schemas_with_diff_reports_new() {
        let existing = Schema {
            fields: BTreeMap::from([("name".into(), FieldType::Keyword)]),
        };
        let discovered = Schema {
            fields: BTreeMap::from([
                ("name".into(), FieldType::Keyword),
                ("age".into(), FieldType::Numeric),
                ("born".into(), FieldType::Date),
            ]),
        };
        let (merged, new_fields) = merge_schemas_with_diff(&existing, &discovered);
        assert_eq!(merged.fields.len(), 3);
        assert_eq!(new_fields.len(), 2);
        assert!(new_fields.contains(&"age".to_string()));
        assert!(new_fields.contains(&"born".to_string()));
    }

    #[cfg(feature = "delta")]
    #[test]
    fn test_from_arrow_fields_basic() {
        use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit};

        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("name", DataType::Utf8, true),
            ArrowField::new("age", DataType::Float64, true),
            ArrowField::new(
                "created",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            ArrowField::new("active", DataType::Boolean, true),
        ]);

        let schema = from_arrow_schema(&arrow_schema);
        assert_eq!(schema.fields["name"], FieldType::Keyword);
        assert_eq!(schema.fields["age"], FieldType::Numeric);
        assert_eq!(schema.fields["created"], FieldType::Date);
        assert_eq!(schema.fields["active"], FieldType::Keyword);
    }

    #[cfg(feature = "delta")]
    #[test]
    fn test_from_arrow_fields_skips_complex_types() {
        use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
        use std::sync::Arc;

        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("name", DataType::Utf8, true),
            ArrowField::new(
                "tags",
                DataType::List(Arc::new(ArrowField::new("item", DataType::Utf8, true))),
                true,
            ),
            ArrowField::new(
                "meta",
                DataType::Struct(Vec::<ArrowField>::new().into()),
                true,
            ),
        ]);

        let schema = from_arrow_schema(&arrow_schema);
        assert_eq!(schema.fields.len(), 1);
        assert!(schema.fields.contains_key("name"));
    }

    #[test]
    fn test_schema_from_json() {
        let json = r#"{"fields":{"name":"keyword","notes":"text","age":"numeric","born":"date"}}"#;
        let schema = Schema::from_json(json).unwrap();
        assert_eq!(schema.fields.len(), 4);
        assert_eq!(schema.fields["name"], FieldType::Keyword);
        assert_eq!(schema.fields["notes"], FieldType::Text);
        assert_eq!(schema.fields["age"], FieldType::Numeric);
        assert_eq!(schema.fields["born"], FieldType::Date);
    }

    #[test]
    fn test_schema_invalid_json() {
        let result = Schema::from_json("not json");
        assert!(result.is_err());
    }

    #[test]
    fn test_build_tantivy_schema_has_system_fields() {
        let schema = Schema {
            fields: BTreeMap::from([("name".into(), FieldType::Keyword)]),
        };
        let tv_schema = schema.build_tantivy_schema();
        assert!(tv_schema.get_field("_id").is_ok());
        assert!(tv_schema.get_field("_source").is_ok());
        assert!(tv_schema.get_field("__present__").is_ok());
        assert!(tv_schema.get_field("name").is_ok());
    }
}

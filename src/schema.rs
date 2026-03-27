use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::LazyLock;
use tantivy::schema::{DateOptions, NumericOptions, SchemaBuilder, TextFieldIndexing, TextOptions};

static ISO_DATETIME_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}").unwrap());

static YYYYMMDD_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\d{8}$").unwrap());

/// Validate that an 8-digit string is a plausible date (month 01-12, day 01-31).
fn is_valid_yyyymmdd(s: &str) -> bool {
    if !YYYYMMDD_RE.is_match(s) {
        return false;
    }
    let month: u32 = s[4..6].parse().unwrap_or(0);
    let day: u32 = s[6..8].parse().unwrap_or(0);
    (1..=12).contains(&month) && (1..=31).contains(&day)
}

/// Returns true if the string looks like an ISO 8601 datetime (YYYY-MM-DDThh:mm:ss...)
/// or a compact YYYYMMDD date.
pub fn looks_like_date(s: &str) -> bool {
    ISO_DATETIME_RE.is_match(s) || is_valid_yyyymmdd(s)
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
/// Returns None for null and objects (not indexable).
/// Arrays of homogeneous primitives are supported: the element type is inferred.
/// Mixed-type arrays, nested arrays, and empty arrays return None.
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
        serde_json::Value::Array(arr) => infer_array_element_type(arr),
        serde_json::Value::Object(_) => None,
    }
}

/// Infer the element type of a JSON array.
///
/// Returns the FieldType if the array contains homogeneous primitives (skipping nulls).
/// Returns None for empty arrays, mixed-type arrays, or arrays containing nested
/// arrays/objects.
fn infer_array_element_type(arr: &[serde_json::Value]) -> Option<FieldType> {
    // Find the first non-null element to determine the base type
    let first_type = arr.iter().find_map(|v| match v {
        serde_json::Value::Null => None,
        other => Some(other),
    })?;

    let base_type = match first_type {
        serde_json::Value::Bool(_) => FieldType::Keyword,
        serde_json::Value::Number(_) => FieldType::Numeric,
        serde_json::Value::String(s) => {
            if looks_like_date(s) {
                FieldType::Date
            } else {
                FieldType::Keyword
            }
        }
        // Nested arrays and objects are not supported
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => return None,
        serde_json::Value::Null => unreachable!(),
    };

    // Verify all non-null elements are the same type
    let all_match = arr.iter().all(|v| match v {
        serde_json::Value::Null => true,
        serde_json::Value::Bool(_) => matches!(base_type, FieldType::Keyword),
        serde_json::Value::Number(_) => matches!(base_type, FieldType::Numeric),
        serde_json::Value::String(s) => {
            if looks_like_date(s) {
                matches!(base_type, FieldType::Date)
            } else {
                matches!(base_type, FieldType::Keyword)
            }
        }
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => false,
    });

    if all_match {
        Some(base_type)
    } else {
        log::warn!("mixed-type array skipped during inference");
        None
    }
}

/// Internal fields that should not be inferred from document data.
const INTERNAL_FIELDS: &[&str] = &["_id", "_source", "__present__"];

/// Recursively flatten a JSON object into dot-notation field entries.
/// Example: {"user": {"name": "alice"}} -> [("user.name", Value::String("alice"))]
fn flatten_json_object(
    prefix: &str,
    obj: &serde_json::Map<String, serde_json::Value>,
    out: &mut Vec<(String, serde_json::Value)>,
) {
    for (key, value) in obj {
        let full_key = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        match value {
            serde_json::Value::Object(nested) => {
                flatten_json_object(&full_key, nested, out);
            }
            other => {
                out.push((full_key, other.clone()));
            }
        }
    }
}

/// Infer a Schema from a batch of JSON documents.
///
/// Scans all documents and collects the first non-null type for each field.
/// Nested objects are flattened into dot-notation field names (e.g. `user.name`).
/// Arrays of homogeneous primitives are supported as multi-valued fields.
/// Skips internal fields (_id, _source, __present__), mixed arrays,
/// and empty arrays.
pub fn infer_schema(docs: &[serde_json::Value]) -> Schema {
    let mut fields = BTreeMap::new();
    for doc in docs {
        if let Some(obj) = doc.as_object() {
            let mut flat = Vec::new();
            flatten_json_object("", obj, &mut flat);
            for (key, value) in &flat {
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
/// Returns None for complex types (Struct, Map, etc.).
/// List types are supported: `List(Utf8)` → Keyword, `List(Float64)` → Numeric, etc.
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
        // List types: infer from the element type (multi-valued fields)
        DataType::List(inner) | DataType::LargeList(inner) => {
            arrow_type_to_field_type(inner.data_type())
        }
        _ => None,
    }
}

/// Recursively flatten Arrow struct fields into dot-notation.
#[cfg(feature = "delta")]
fn flatten_arrow_fields(
    prefix: &str,
    fields: &arrow::datatypes::Fields,
    out: &mut BTreeMap<String, FieldType>,
) {
    for field in fields.iter() {
        let name = field.name();
        if name.starts_with('_') {
            continue;
        }
        let full_name = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}.{name}")
        };
        match field.data_type() {
            arrow::datatypes::DataType::Struct(sub_fields) => {
                flatten_arrow_fields(&full_name, sub_fields, out);
            }
            dt => {
                if let Some(ft) = arrow_type_to_field_type(dt) {
                    out.insert(full_name, ft);
                }
            }
        }
    }
}

/// Infer a SearchDB Schema from an Arrow schema (e.g., from a Delta table).
/// Struct types are flattened into dot-notation field names (e.g. `user.name`).
#[cfg(feature = "delta")]
pub fn from_arrow_schema(arrow_schema: &arrow::datatypes::Schema) -> Schema {
    let mut fields = BTreeMap::new();
    flatten_arrow_fields("", arrow_schema.fields(), &mut fields);
    // Remove internal fields that might have been included
    for name in INTERNAL_FIELDS {
        fields.remove(*name);
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
    fn test_infer_field_type_array_of_numbers() {
        assert_eq!(
            infer_field_type(&serde_json::json!([1, 2, 3])),
            Some(FieldType::Numeric)
        );
    }

    #[test]
    fn test_infer_field_type_array_of_strings() {
        assert_eq!(
            infer_field_type(&serde_json::json!(["a", "b", "c"])),
            Some(FieldType::Keyword)
        );
    }

    #[test]
    fn test_infer_field_type_array_mixed() {
        assert_eq!(infer_field_type(&serde_json::json!([1, "two", 3])), None);
    }

    #[test]
    fn test_infer_field_type_array_empty() {
        assert_eq!(infer_field_type(&serde_json::json!([])), None);
    }

    #[test]
    fn test_infer_field_type_array_nested() {
        assert_eq!(infer_field_type(&serde_json::json!([[1, 2], [3, 4]])), None);
    }

    #[test]
    fn test_infer_field_type_array_with_nulls() {
        assert_eq!(
            infer_field_type(&serde_json::json!([null, "a", null, "b"])),
            Some(FieldType::Keyword)
        );
    }

    #[test]
    fn test_infer_field_type_array_all_nulls() {
        assert_eq!(infer_field_type(&serde_json::json!([null, null])), None);
    }

    #[test]
    fn test_infer_field_type_array_of_dates() {
        assert_eq!(
            infer_field_type(&serde_json::json!([
                "2024-01-15T10:30:00Z",
                "2024-02-20T14:00:00Z"
            ])),
            Some(FieldType::Date)
        );
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
    fn test_infer_schema_flattens_nested_and_handles_arrays() {
        let docs =
            vec![serde_json::json!({"name": "alice", "tags": ["a", "b"], "addr": {"city": "NYC"}})];
        let schema = infer_schema(&docs);
        assert_eq!(schema.fields.len(), 3);
        assert!(schema.fields.contains_key("name"));
        assert_eq!(schema.fields["tags"], FieldType::Keyword);
        // Nested objects are flattened to dot-notation
        assert!(!schema.fields.contains_key("addr"));
        assert_eq!(schema.fields["addr.city"], FieldType::Keyword);
    }

    #[test]
    fn test_infer_schema_array_of_numbers() {
        let docs = vec![serde_json::json!({"name": "alice", "scores": [90, 85, 92]})];
        let schema = infer_schema(&docs);
        assert_eq!(schema.fields["scores"], FieldType::Numeric);
    }

    #[test]
    fn test_infer_schema_skips_mixed_arrays() {
        let docs = vec![serde_json::json!({"name": "alice", "data": [1, "two", 3]})];
        let schema = infer_schema(&docs);
        assert_eq!(schema.fields.len(), 1); // only "name"
        assert!(!schema.fields.contains_key("data"));
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
    fn test_from_arrow_fields_handles_list_types() {
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
                "scores",
                DataType::List(Arc::new(ArrowField::new("item", DataType::Float64, true))),
                true,
            ),
            ArrowField::new(
                "meta",
                DataType::Struct(Vec::<ArrowField>::new().into()),
                true,
            ),
        ]);

        let schema = from_arrow_schema(&arrow_schema);
        assert_eq!(schema.fields.len(), 3);
        assert_eq!(schema.fields["name"], FieldType::Keyword);
        assert_eq!(schema.fields["tags"], FieldType::Keyword);
        assert_eq!(schema.fields["scores"], FieldType::Numeric);
        // Empty struct still produces no fields
        assert!(!schema.fields.contains_key("meta"));
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
    fn test_looks_like_date_yyyymmdd() {
        assert!(looks_like_date("20260315"));
        assert!(looks_like_date("20001231"));
        assert!(!looks_like_date("99999999")); // invalid month (99)
        assert!(!looks_like_date("20261301")); // month 13
        assert!(!looks_like_date("20260132")); // day 32
        assert!(!looks_like_date("2026031")); // too short
        assert!(!looks_like_date("202603155")); // too long
        assert!(!looks_like_date("12345678")); // month=56, invalid
    }

    #[test]
    fn test_infer_yyyymmdd_as_date() {
        let ft = infer_field_type(&serde_json::json!("20260315"));
        assert_eq!(ft, Some(FieldType::Date));
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

    #[test]
    fn test_infer_schema_flattens_nested_objects() {
        let docs = vec![serde_json::json!({
            "user": {
                "name": "alice",
                "age": 30,
                "address": {
                    "city": "NYC"
                }
            },
            "status": "active"
        })];
        let schema = infer_schema(&docs);
        assert_eq!(schema.fields.get("user.name"), Some(&FieldType::Keyword));
        assert_eq!(schema.fields.get("user.age"), Some(&FieldType::Numeric));
        assert_eq!(
            schema.fields.get("user.address.city"),
            Some(&FieldType::Keyword)
        );
        assert_eq!(schema.fields.get("status"), Some(&FieldType::Keyword));
        // Top-level "user" should NOT be a field
        assert!(!schema.fields.contains_key("user"));
    }

    #[cfg(feature = "delta")]
    #[test]
    fn test_from_arrow_schema_flattens_struct() {
        use arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema};

        let arrow_schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "user",
                DataType::Struct(Fields::from(vec![
                    Field::new("name", DataType::Utf8, true),
                    Field::new("age", DataType::Int64, true),
                ])),
                true,
            ),
        ]);

        let schema = from_arrow_schema(&arrow_schema);
        assert_eq!(schema.fields.get("id"), Some(&FieldType::Keyword));
        assert_eq!(schema.fields.get("user.name"), Some(&FieldType::Keyword));
        assert_eq!(schema.fields.get("user.age"), Some(&FieldType::Numeric));
        assert!(!schema.fields.contains_key("user"));
    }
}

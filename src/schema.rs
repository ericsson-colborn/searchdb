use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tantivy::schema::{
    DateOptions, NumericOptions, SchemaBuilder, TextFieldIndexing, TextOptions, STORED,
};
use tantivy::tokenizer::TextAnalyzer;

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
/// Serialized to searchdb.json alongside Delta metadata.
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

#[cfg(test)]
mod tests {
    use super::*;

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

use std::fmt;
use std::marker::PhantomData;

use serde::de::{self, MapAccess, Visitor};
use serde::Deserialize;

/// Deserializes a single-key JSON object `{"field_name": <value>}` into `(String, T)`.
///
/// This is the ES DSL pattern where the field name is a JSON key, e.g.:
/// `{"term": {"status": "active"}}` -- the inner `{"status": "active"}` is a OneFieldMap.
#[derive(Debug, Clone)]
pub struct OneFieldMap<T> {
    pub field: String,
    pub value: T,
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for OneFieldMap<T> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct OneFieldVisitor<T>(PhantomData<T>);

        impl<'de, T: Deserialize<'de>> Visitor<'de> for OneFieldVisitor<T> {
            type Value = OneFieldMap<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a JSON object with a single field")
            }

            fn visit_map<M>(self, mut map: M) -> std::result::Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let (field, value) = map
                    .next_entry::<String, T>()?
                    .ok_or_else(|| de::Error::custom("expected a single-field object, got empty"))?;

                if map.next_key::<String>()?.is_some() {
                    return Err(de::Error::custom(
                        "expected a single-field object, got multiple fields",
                    ));
                }

                Ok(OneFieldMap { field, value })
            }
        }

        deserializer.deserialize_map(OneFieldVisitor(PhantomData))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_one_field_map_string_value() {
        let json = r#"{"status": "active"}"#;
        let ofm: OneFieldMap<String> = serde_json::from_str(json).unwrap();
        assert_eq!(ofm.field, "status");
        assert_eq!(ofm.value, "active");
    }

    #[test]
    fn test_one_field_map_object_value() {
        #[derive(Debug, Deserialize)]
        struct Inner {
            value: String,
        }
        let json = r#"{"status": {"value": "active"}}"#;
        let ofm: OneFieldMap<Inner> = serde_json::from_str(json).unwrap();
        assert_eq!(ofm.field, "status");
        assert_eq!(ofm.value.value, "active");
    }

    #[test]
    fn test_one_field_map_array_value() {
        let json = r#"{"status": ["active", "pending"]}"#;
        let ofm: OneFieldMap<Vec<String>> = serde_json::from_str(json).unwrap();
        assert_eq!(ofm.field, "status");
        assert_eq!(ofm.value, vec!["active", "pending"]);
    }

    #[test]
    fn test_one_field_map_empty_object_fails() {
        let json = r#"{}"#;
        let result: std::result::Result<OneFieldMap<String>, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_one_field_map_multiple_fields_fails() {
        let json = r#"{"field1": "a", "field2": "b"}"#;
        let result: std::result::Result<OneFieldMap<String>, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }
}

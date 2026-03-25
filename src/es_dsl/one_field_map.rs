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

# Schema Inference Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make schema optional across all SearchDB commands. When no schema is provided, infer field types from data (JSON values or Arrow schema). Support additive schema evolution for inferred-schema indexes. Preserve full backward compatibility for explicit-schema indexes.

**Architecture:** Inference happens at two levels: (1) JSON value inspection (`infer_field_type`) for NDJSON indexing, and (2) Arrow schema mapping (`from_arrow_schema`) for Delta Lake connections. Schema evolution detects new fields, merges them into the existing schema, rebuilds the tantivy index from `_source`, and reindexes. An `inferred` flag in `searchdb.json` tracks whether evolution is enabled.

**Tech Stack:** Rust, tantivy 0.25, regex crate (ISO 8601 detection), serde/serde_json, arrow (Arrow schema types), deltalake crate

**Spec:** `docs/superpowers/specs/2026-03-24-schema-inference-design.md`

---

## File Structure

```
src/
├── main.rs              # MODIFY — make --schema optional in New and ConnectDelta
├── schema.rs            # MODIFY — add inference functions, date regex, Arrow mapping, merge
├── storage.rs           # MODIFY — add `inferred` flag to IndexConfig
├── writer.rs            # unchanged
├── searcher.rs          # unchanged
├── error.rs             # unchanged
├── commands/
│   ├── new_index.rs     # MODIFY — accept Option<&str> for schema, empty-schema path
│   ├── index.rs         # MODIFY — detect new fields, trigger evolution or skip
│   ├── connect_delta.rs # MODIFY — accept Option<&str> for schema, infer from Arrow
│   ├── mod.rs           # unchanged
│   ├── search.rs        # unchanged
│   ├── get.rs           # unchanged
│   ├── stats.rs         # unchanged
│   ├── drop.rs          # unchanged
│   └── compact.rs       # unchanged
```

---

## Task 1: Add `looks_like_date` regex helper to `schema.rs`

**Files:**
- Modify: `src/schema.rs`

This task adds the ISO 8601 datetime detection regex. It is a pure function with no dependencies on other changes.

- [ ] **Step 1: Write failing tests for date detection**

Add to the `#[cfg(test)] mod tests` block in `src/schema.rs`:

```rust
#[test]
fn test_looks_like_date_iso8601() {
    assert!(super::looks_like_date("2024-01-15T10:30:00Z"));
    assert!(super::looks_like_date("2024-01-15T10:30:00+05:30"));
    assert!(super::looks_like_date("2024-01-15T10:30:00.123Z"));
    assert!(super::looks_like_date("2024-01-15 10:30:00"));
}

#[test]
fn test_looks_like_date_rejects_non_dates() {
    assert!(!super::looks_like_date("hello world"));
    assert!(!super::looks_like_date("2024-01-15"));         // date-only, no time
    assert!(!super::looks_like_date("Jan 15, 2024"));
    assert!(!super::looks_like_date("42"));
    assert!(!super::looks_like_date(""));
    assert!(!super::looks_like_date("2024-1-15T10:30:00Z")); // single-digit month
}
```

Run: `cargo test --lib schema::tests`
Expected: **Fails** because `looks_like_date` does not exist.

- [ ] **Step 2: Implement `looks_like_date`**

Add to `src/schema.rs` (above the `impl Schema` block):

```rust
use std::sync::LazyLock;
use regex::Regex;

static ISO_DATETIME_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}").unwrap()
});

/// Returns true if the string looks like an ISO 8601 datetime (YYYY-MM-DDThh:mm:ss...).
pub fn looks_like_date(s: &str) -> bool {
    ISO_DATETIME_RE.is_match(s)
}
```

Add `regex` to `Cargo.toml` if not already present.

Run: `cargo test --lib schema::tests`
Expected: **Passes.** All date detection tests green.

---

## Task 2: Add `infer_field_type` function to `schema.rs`

**Files:**
- Modify: `src/schema.rs`

This function maps a single `serde_json::Value` to an `Option<FieldType>`. Returns `None` for null, arrays, and objects.

- [ ] **Step 1: Write failing tests for field type inference**

Add to the test module in `src/schema.rs`:

```rust
#[test]
fn test_infer_field_type_string() {
    let val = serde_json::json!("hello");
    assert_eq!(super::infer_field_type(&val), Some(FieldType::Keyword));
}

#[test]
fn test_infer_field_type_number() {
    assert_eq!(super::infer_field_type(&serde_json::json!(42)), Some(FieldType::Numeric));
    assert_eq!(super::infer_field_type(&serde_json::json!(3.14)), Some(FieldType::Numeric));
}

#[test]
fn test_infer_field_type_date_string() {
    let val = serde_json::json!("2024-01-15T10:30:00Z");
    assert_eq!(super::infer_field_type(&val), Some(FieldType::Date));
}

#[test]
fn test_infer_field_type_boolean() {
    assert_eq!(super::infer_field_type(&serde_json::json!(true)), Some(FieldType::Keyword));
    assert_eq!(super::infer_field_type(&serde_json::json!(false)), Some(FieldType::Keyword));
}

#[test]
fn test_infer_field_type_null() {
    assert_eq!(super::infer_field_type(&serde_json::Value::Null), None);
}

#[test]
fn test_infer_field_type_array() {
    assert_eq!(super::infer_field_type(&serde_json::json!([1, 2, 3])), None);
}

#[test]
fn test_infer_field_type_object() {
    assert_eq!(super::infer_field_type(&serde_json::json!({"nested": true})), None);
}
```

Run: `cargo test --lib schema::tests`
Expected: **Fails** because `infer_field_type` does not exist.

- [ ] **Step 2: Implement `infer_field_type`**

Add to `src/schema.rs`:

```rust
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
```

Run: `cargo test --lib schema::tests`
Expected: **Passes.**

---

## Task 3: Add `infer_schema` function to `schema.rs`

**Files:**
- Modify: `src/schema.rs`

Scans a batch of JSON documents and produces a `Schema` covering all discovered fields. First non-null value per field wins the type.

- [ ] **Step 1: Write failing tests for batch inference**

Add to the test module in `src/schema.rs`:

```rust
#[test]
fn test_infer_schema_basic() {
    let docs = vec![
        serde_json::json!({"name": "alice", "age": 30, "created": "2024-01-15T10:00:00Z"}),
        serde_json::json!({"name": "bob", "age": 25, "active": true}),
    ];
    let schema = super::infer_schema(&docs);
    assert_eq!(schema.fields["name"], FieldType::Keyword);
    assert_eq!(schema.fields["age"], FieldType::Numeric);
    assert_eq!(schema.fields["created"], FieldType::Date);
    assert_eq!(schema.fields["active"], FieldType::Keyword);
    assert_eq!(schema.fields.len(), 4);
}

#[test]
fn test_infer_schema_skips_id() {
    let docs = vec![serde_json::json!({"_id": "abc", "name": "alice"})];
    let schema = super::infer_schema(&docs);
    assert!(!schema.fields.contains_key("_id"));
    assert_eq!(schema.fields.len(), 1);
}

#[test]
fn test_infer_schema_first_non_null_wins() {
    let docs = vec![
        serde_json::json!({"status": null}),
        serde_json::json!({"status": "active"}),
    ];
    let schema = super::infer_schema(&docs);
    assert_eq!(schema.fields["status"], FieldType::Keyword);
}

#[test]
fn test_infer_schema_skips_nested_and_arrays() {
    let docs = vec![
        serde_json::json!({"name": "alice", "tags": ["a", "b"], "addr": {"city": "NYC"}}),
    ];
    let schema = super::infer_schema(&docs);
    assert_eq!(schema.fields.len(), 1);
    assert!(schema.fields.contains_key("name"));
}

#[test]
fn test_infer_schema_empty_docs() {
    let docs: Vec<serde_json::Value> = vec![];
    let schema = super::infer_schema(&docs);
    assert!(schema.fields.is_empty());
}
```

Run: `cargo test --lib schema::tests`
Expected: **Fails** because `infer_schema` does not exist.

- [ ] **Step 2: Implement `infer_schema`**

Add to `src/schema.rs`:

```rust
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
```

Run: `cargo test --lib schema::tests`
Expected: **Passes.**

---

## Task 4: Add `merge_schemas` function to `schema.rs`

**Files:**
- Modify: `src/schema.rs`

Merges a newly-discovered schema into an existing one. Additive only: new fields are added, existing fields keep their original type.

- [ ] **Step 1: Write failing tests for schema merging**

Add to the test module in `src/schema.rs`:

```rust
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
    let merged = super::merge_schemas(&existing, &discovered);
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
    let merged = super::merge_schemas(&existing, &discovered);
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
    let (merged, new_fields) = super::merge_schemas_with_diff(&existing, &discovered);
    assert_eq!(merged.fields.len(), 1);
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
    let (merged, new_fields) = super::merge_schemas_with_diff(&existing, &discovered);
    assert_eq!(merged.fields.len(), 3);
    assert_eq!(new_fields.len(), 2);
    assert!(new_fields.contains(&"age".to_string()));
    assert!(new_fields.contains(&"born".to_string()));
}
```

Run: `cargo test --lib schema::tests`
Expected: **Fails** because `merge_schemas` and `merge_schemas_with_diff` do not exist.

- [ ] **Step 2: Implement `merge_schemas` and `merge_schemas_with_diff`**

Add to `src/schema.rs`:

```rust
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
```

Run: `cargo test --lib schema::tests`
Expected: **Passes.**

---

## Task 5: Add `inferred` flag to `IndexConfig` in `storage.rs`

**Files:**
- Modify: `src/storage.rs`

Track whether the schema was inferred (evolution enabled) or explicit (evolution disabled).

- [ ] **Step 1: Write failing test for `inferred` flag persistence**

Add to the test module in `src/storage.rs`:

```rust
#[test]
fn test_inferred_flag_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::new(dir.path().to_str().unwrap());
    storage.create_dirs("test").unwrap();

    let schema = Schema {
        fields: BTreeMap::from([("name".into(), FieldType::Keyword)]),
    };
    let tantivy_schema = schema.build_tantivy_schema();
    tantivy::Index::create_in_dir(storage.tantivy_dir("test"), tantivy_schema).unwrap();

    let config = IndexConfig {
        schema,
        inferred: true,
        delta_source: None,
        index_version: None,
        compact: None,
    };
    storage.save_config("test", &config).unwrap();

    let loaded = storage.load_config("test").unwrap();
    assert!(loaded.inferred);
}

#[test]
fn test_inferred_flag_defaults_false_when_absent() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::new(dir.path().to_str().unwrap());
    storage.create_dirs("test").unwrap();

    let schema = Schema {
        fields: BTreeMap::from([("name".into(), FieldType::Keyword)]),
    };
    let tantivy_schema = schema.build_tantivy_schema();
    tantivy::Index::create_in_dir(storage.tantivy_dir("test"), tantivy_schema).unwrap();

    // Write JSON manually WITHOUT "inferred" field (simulates old config)
    let json = r#"{"schema":{"fields":{"name":"keyword"}}}"#;
    std::fs::write(storage.config_path("test"), json).unwrap();

    let loaded = storage.load_config("test").unwrap();
    assert!(!loaded.inferred); // defaults to false
}
```

Run: `cargo test --lib storage::tests`
Expected: **Fails** because `inferred` field does not exist on `IndexConfig`.

- [ ] **Step 2: Add `inferred` field to `IndexConfig`**

In `src/storage.rs`, add the `inferred` field to `IndexConfig`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    pub schema: Schema,
    #[serde(default)]
    pub inferred: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta_source: Option<String>,
    #[serde(alias = "last_indexed_version", skip_serializing_if = "Option::is_none")]
    pub index_version: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compact: Option<CompactMeta>,
}
```

The `#[serde(default)]` attribute ensures backward compatibility: existing `searchdb.json` files without the `inferred` field will deserialize with `inferred: false`.

Update all `IndexConfig` construction sites to include `inferred`:
- `src/commands/new_index.rs`: `inferred: false` (explicit schema path)
- `src/commands/connect_delta.rs`: `inferred: false` (explicit schema path)
- Any test code that constructs `IndexConfig`

Run: `cargo test --lib storage::tests`
Expected: **Passes.**

Run: `cargo test` (full suite)
Expected: **Passes.** All existing tests still work because `inferred: false` preserves current behavior.

---

## Task 6: Make `--schema` optional in `dsrch new`

**Files:**
- Modify: `src/main.rs` -- change `schema: String` to `schema: Option<String>` in `New` variant
- Modify: `src/commands/new_index.rs` -- accept `Option<&str>` for schema_json

- [ ] **Step 1: Write failing test for creating index without schema**

Add to the test module in `src/commands/new_index.rs` (or create one):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;

    #[test]
    fn test_new_index_without_schema() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());

        run(&storage, "test", None, false).unwrap();

        let config = storage.load_config("test").unwrap();
        assert!(config.schema.fields.is_empty());
        assert!(config.inferred);
    }

    #[test]
    fn test_new_index_with_schema() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());

        let schema_json = r#"{"fields":{"name":"keyword"}}"#;
        run(&storage, "test", Some(schema_json), false).unwrap();

        let config = storage.load_config("test").unwrap();
        assert_eq!(config.schema.fields.len(), 1);
        assert!(!config.inferred);
    }
}
```

Run: `cargo test --lib commands::new_index::tests`
Expected: **Fails** because `run` does not accept `Option<&str>`.

- [ ] **Step 2: Update `new_index::run` to accept optional schema**

In `src/commands/new_index.rs`:

```rust
pub fn run(storage: &Storage, name: &str, schema_json: Option<&str>, overwrite: bool) -> Result<()> {
    if storage.exists(name) {
        if overwrite {
            storage.drop(name)?;
        } else {
            return Err(SearchDbError::IndexExists(name.to_string()));
        }
    }

    let (schema, inferred) = match schema_json {
        Some(json) => (Schema::from_json(json)?, false),
        None => (Schema { fields: BTreeMap::new() }, true),
    };
    let tantivy_schema = schema.build_tantivy_schema();

    storage.create_dirs(name)?;
    let tantivy_dir = storage.tantivy_dir(name);
    let _index = tantivy::Index::create_in_dir(&tantivy_dir, tantivy_schema)?;

    let config = IndexConfig {
        schema,
        inferred,
        delta_source: None,
        index_version: None,
        compact: None,
    };
    storage.save_config(name, &config)?;

    eprintln!(
        "[dsrch] Created index '{name}' with {} field(s){}",
        config.schema.fields.len(),
        if inferred { " (schema will be inferred from data)" } else { "" }
    );
    Ok(())
}
```

Add `use std::collections::BTreeMap;` to imports.

- [ ] **Step 3: Update `main.rs` CLI definition**

In `src/main.rs`, change the `New` variant:

```rust
New {
    name: String,
    #[arg(long)]
    schema: Option<String>,
    #[arg(long, default_value_t = false)]
    overwrite: bool,
},
```

Update the dispatch in both `run_cli` and `run_cli_sync`:

```rust
Commands::New { name, schema, overwrite } => {
    commands::new_index::run(&storage, &name, schema.as_deref(), overwrite)
}
```

- [ ] **Step 4: Update all callers and tests**

Find all call sites of `new_index::run` in tests and update them to pass `Some(schema_json)` instead of `schema_json`. Key locations:
- `src/commands/index.rs` tests (`setup_index` helper)
- Any integration tests

Run: `cargo test`
Expected: **Passes.** All tests green.

---

## Task 7: Add schema inference to `dsrch index`

**Files:**
- Modify: `src/commands/index.rs` -- detect unknown fields, trigger evolution for inferred schemas

This is the core task. When indexing NDJSON into an inferred-schema index and new fields are discovered, the index must be rebuilt with the expanded schema.

- [ ] **Step 1: Write failing test for inference on first index**

Add to the test module in `src/commands/index.rs`:

```rust
#[test]
fn test_index_infers_schema_from_data() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::new(dir.path().to_str().unwrap());

    // Create index WITHOUT schema
    new_index::run(&storage, "test", None, false).unwrap();

    let ndjson_path = dir.path().join("data.ndjson");
    std::fs::write(
        &ndjson_path,
        r#"{"_id":"d1","name":"glucose","age":45,"created":"2024-01-15T10:00:00Z"}
{"_id":"d2","name":"a1c","age":62,"created":"2024-02-20T08:30:00Z"}
"#,
    ).unwrap();

    run(&storage, "test", Some(ndjson_path.to_str().unwrap())).unwrap();

    // Verify schema was inferred
    let config = storage.load_config("test").unwrap();
    assert_eq!(config.schema.fields["name"], FieldType::Keyword);
    assert_eq!(config.schema.fields["age"], FieldType::Numeric);
    assert_eq!(config.schema.fields["created"], FieldType::Date);

    // Verify documents were indexed
    let index = tantivy::Index::open_in_dir(storage.tantivy_dir("test")).unwrap();
    let reader = index.reader().unwrap();
    assert_eq!(reader.searcher().num_docs(), 2);
}
```

Run: `cargo test --lib commands::index::tests`
Expected: **Fails** because `index::run` does not perform inference.

- [ ] **Step 2: Write failing test for schema evolution (new fields in later batch)**

```rust
#[test]
fn test_index_evolves_schema_with_new_fields() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::new(dir.path().to_str().unwrap());

    // Create index without schema
    new_index::run(&storage, "test", None, false).unwrap();

    // First batch — establishes name + age
    let batch1 = dir.path().join("batch1.ndjson");
    std::fs::write(&batch1, r#"{"_id":"d1","name":"glucose","age":45}"#).unwrap();
    run(&storage, "test", Some(batch1.to_str().unwrap())).unwrap();

    let config1 = storage.load_config("test").unwrap();
    assert_eq!(config1.schema.fields.len(), 2);

    // Second batch — adds "status" field
    let batch2 = dir.path().join("batch2.ndjson");
    std::fs::write(&batch2, r#"{"_id":"d2","name":"a1c","age":62,"status":"active"}"#).unwrap();
    run(&storage, "test", Some(batch2.to_str().unwrap())).unwrap();

    // Schema should now have 3 fields
    let config2 = storage.load_config("test").unwrap();
    assert_eq!(config2.schema.fields.len(), 3);
    assert_eq!(config2.schema.fields["status"], FieldType::Keyword);

    // Both documents should be present
    let index = tantivy::Index::open_in_dir(storage.tantivy_dir("test")).unwrap();
    let reader = index.reader().unwrap();
    assert_eq!(reader.searcher().num_docs(), 2);
}
```

- [ ] **Step 3: Write failing test for explicit schema ignoring unknown fields**

```rust
#[test]
fn test_index_explicit_schema_ignores_unknown_fields() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::new(dir.path().to_str().unwrap());

    // Create index WITH explicit schema (only "name")
    let schema_json = r#"{"fields":{"name":"keyword"}}"#;
    new_index::run(&storage, "test", Some(schema_json), false).unwrap();

    // Index doc with extra "age" field
    let ndjson = dir.path().join("data.ndjson");
    std::fs::write(&ndjson, r#"{"_id":"d1","name":"glucose","age":45}"#).unwrap();
    run(&storage, "test", Some(ndjson.to_str().unwrap())).unwrap();

    // Schema should NOT have changed (no evolution for explicit schemas)
    let config = storage.load_config("test").unwrap();
    assert_eq!(config.schema.fields.len(), 1);
    assert!(!config.inferred);

    // Document should still be indexed (extra fields silently ignored)
    let index = tantivy::Index::open_in_dir(storage.tantivy_dir("test")).unwrap();
    let reader = index.reader().unwrap();
    assert_eq!(reader.searcher().num_docs(), 1);
}
```

Run: `cargo test --lib commands::index::tests`
Expected: **Fails** because the inference/evolution logic is not implemented.

- [ ] **Step 4: Implement schema inference in `index::run`**

Rewrite `src/commands/index.rs` `run` function. The new flow:

1. Load config.
2. Read all NDJSON lines into a `Vec<serde_json::Value>`.
3. If `config.inferred`, infer schema from the batch and merge with existing.
4. If new fields were discovered, rebuild the tantivy index:
   a. Read all existing documents from old index via `_source`.
   b. Drop and recreate tantivy index directory with new schema.
   c. Reindex old documents.
5. Index the new batch.
6. Save updated config.

Key helper needed: `read_all_sources(index, tantivy_schema) -> Vec<serde_json::Value>` — reads all docs from an existing tantivy index by extracting the `_source` field.

```rust
/// Read all existing documents from the index via the _source field.
fn read_existing_docs(storage: &Storage, name: &str) -> Result<Vec<serde_json::Value>> {
    let index = Index::open_in_dir(storage.tantivy_dir(name))?;
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let tantivy_schema = index.schema();
    let source_field = tantivy_schema.get_field("_source")
        .map_err(|_| SearchDbError::Schema("missing _source field".into()))?;

    let mut docs = Vec::new();
    for segment_reader in searcher.segment_readers() {
        let store_reader = segment_reader.get_store_reader(1)?; // cache_size=1
        for doc_id in 0..segment_reader.num_docs() {
            let doc: TantivyDocument = store_reader.get(doc_id)?;
            if let Some(source_val) = doc.get_first(source_field) {
                if let Some(source_str) = source_val.as_str() {
                    if let Ok(parsed) = serde_json::from_str(source_str) {
                        docs.push(parsed);
                    }
                }
            }
        }
    }
    Ok(docs)
}

/// Rebuild the tantivy index with a new schema, reindexing existing docs from _source.
fn rebuild_index(storage: &Storage, name: &str, new_schema: &Schema) -> Result<()> {
    // Read existing docs before destroying the index
    let existing_docs = read_existing_docs(storage, name)?;

    // Drop and recreate tantivy index dir
    let tantivy_dir = storage.tantivy_dir(name);
    std::fs::remove_dir_all(&tantivy_dir)?;
    std::fs::create_dir_all(&tantivy_dir)?;

    let tantivy_schema = new_schema.build_tantivy_schema();
    let index = Index::create_in_dir(&tantivy_dir, tantivy_schema.clone())?;
    let mut index_writer = index.writer(50_000_000)?;

    let id_field = tantivy_schema.get_field("_id")
        .map_err(|_| SearchDbError::Schema("missing _id field".into()))?;

    for doc_json in &existing_docs {
        let doc_id = writer::make_doc_id(doc_json);
        let doc = writer::build_document(&tantivy_schema, new_schema, doc_json, &doc_id)?;
        writer::upsert_document(&index_writer, id_field, doc, &doc_id);
    }
    index_writer.commit()?;

    eprintln!(
        "[dsrch] Rebuilt index '{name}' with {} existing document(s)",
        existing_docs.len()
    );
    Ok(())
}
```

Then update `run` to orchestrate:

```rust
pub fn run(storage: &Storage, name: &str, file: Option<&str>) -> Result<()> {
    if !storage.exists(name) {
        return Err(SearchDbError::IndexNotFound(name.to_string()));
    }

    let mut config = storage.load_config(name)?;

    // Read all input documents
    let new_docs = read_ndjson(file)?;
    if new_docs.is_empty() {
        eprintln!("[dsrch] No documents to index");
        return Ok(());
    }

    // Schema inference + evolution (only for inferred schemas)
    if config.inferred {
        let discovered = crate::schema::infer_schema(&new_docs);
        let (merged, new_fields) = crate::schema::merge_schemas_with_diff(&config.schema, &discovered);

        if !new_fields.is_empty() {
            eprintln!(
                "[dsrch] Schema evolution: discovered {} new field(s): {}",
                new_fields.len(),
                new_fields.join(", ")
            );
            config.schema = merged;
            rebuild_index(storage, name, &config.schema)?;
            storage.save_config(name, &config)?;
        }
    }

    // Now index the new batch
    let tantivy_schema = config.schema.build_tantivy_schema();
    let index = Index::open_in_dir(storage.tantivy_dir(name))?;
    let mut index_writer = index.writer(50_000_000)?;
    let id_field = tantivy_schema.get_field("_id")
        .map_err(|_| SearchDbError::Schema("missing _id field".into()))?;

    let mut count = 0u64;
    for doc_json in &new_docs {
        let doc_id = writer::make_doc_id(doc_json);
        let doc = writer::build_document(&tantivy_schema, &config.schema, doc_json, &doc_id)?;
        writer::upsert_document(&index_writer, id_field, doc, &doc_id);
        count += 1;
    }
    index_writer.commit()?;

    eprintln!("[dsrch] Indexed {count} document(s) into '{name}'");
    Ok(())
}

/// Read NDJSON from file or stdin into a Vec.
fn read_ndjson(file: Option<&str>) -> Result<Vec<serde_json::Value>> {
    let reader: Box<dyn BufRead> = match file {
        Some(path) => {
            let f = File::open(path)
                .map_err(|e| SearchDbError::Schema(format!("cannot open file '{path}': {e}")))?;
            Box::new(BufReader::new(f))
        }
        None => Box::new(BufReader::new(io::stdin())),
    };

    let mut docs = Vec::new();
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str(trimmed) {
            Ok(v) => docs.push(v),
            Err(e) => eprintln!("[dsrch] Skipping invalid JSON: {e}"),
        }
    }
    Ok(docs)
}
```

Note: The existing `build_document` in `writer.rs` iterates over `app_schema.fields`, so fields in the JSON that are not in the schema are already silently ignored. This means explicit-schema indexes automatically skip unknown fields with no code change needed.

Run: `cargo test --lib commands::index::tests`
Expected: **Passes.** All new tests and existing tests green.

Run: `cargo test`
Expected: **Passes.** Full test suite green.

---

## Task 8: Make `--schema` optional in `dsrch connect-delta`

**Files:**
- Modify: `src/main.rs` -- change `schema: String` to `schema: Option<String>` in `ConnectDelta` variant
- Modify: `src/commands/connect_delta.rs` -- accept `Option<&str>`, infer from Arrow schema when absent

- [ ] **Step 1: Add `from_arrow_schema` to `schema.rs`**

Write failing tests first:

```rust
#[test]
fn test_from_arrow_fields_basic() {
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};

    let arrow_schema = ArrowSchema::new(vec![
        ArrowField::new("name", DataType::Utf8, true),
        ArrowField::new("age", DataType::Float64, true),
        ArrowField::new("created", DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None), true),
        ArrowField::new("active", DataType::Boolean, true),
    ]);

    let schema = super::from_arrow_schema(&arrow_schema);
    assert_eq!(schema.fields["name"], FieldType::Keyword);
    assert_eq!(schema.fields["age"], FieldType::Numeric);
    assert_eq!(schema.fields["created"], FieldType::Date);
    assert_eq!(schema.fields["active"], FieldType::Keyword);
}

#[test]
fn test_from_arrow_fields_skips_complex_types() {
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};

    let arrow_schema = ArrowSchema::new(vec![
        ArrowField::new("name", DataType::Utf8, true),
        ArrowField::new("tags", DataType::List(Arc::new(ArrowField::new("item", DataType::Utf8, true))), true),
        ArrowField::new("meta", DataType::Struct(vec![].into()), true),
    ]);

    let schema = super::from_arrow_schema(&arrow_schema);
    assert_eq!(schema.fields.len(), 1);
    assert!(schema.fields.contains_key("name"));
}
```

Run: `cargo test --lib schema::tests`
Expected: **Fails** because `from_arrow_schema` does not exist.

- [ ] **Step 2: Implement `from_arrow_schema`**

Add to `src/schema.rs`:

```rust
/// Map an Arrow DataType to a SearchDB FieldType.
/// Returns None for complex types (List, Struct, Map, etc.).
fn arrow_type_to_field_type(dt: &arrow_schema::DataType) -> Option<FieldType> {
    use arrow_schema::DataType;
    match dt {
        DataType::Utf8 | DataType::LargeUtf8 => Some(FieldType::Keyword),
        DataType::Boolean => Some(FieldType::Keyword),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
        | DataType::Float16 | DataType::Float32 | DataType::Float64 => Some(FieldType::Numeric),
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => Some(FieldType::Date),
        _ => None,
    }
}

/// Infer a SearchDB Schema from an Arrow schema (e.g., from a Delta table).
pub fn from_arrow_schema(arrow_schema: &arrow_schema::Schema) -> Schema {
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
```

Add `arrow-schema` to `Cargo.toml` dependencies if not already present (it is likely already a transitive dep of `arrow` or `deltalake`).

Run: `cargo test --lib schema::tests`
Expected: **Passes.**

- [ ] **Step 3: Update `connect_delta::run` to accept optional schema**

In `src/commands/connect_delta.rs`:

```rust
pub async fn run(
    storage: &Storage,
    name: &str,
    source: &str,
    schema_json: Option<&str>,
) -> Result<()> {
    if storage.exists(name) {
        return Err(SearchDbError::IndexExists(name.to_string()));
    }

    let delta = DeltaSync::new(source);
    let version = delta.current_version().await?;
    eprintln!("[dsrch] Delta table at version {version}");

    let (schema, inferred) = match schema_json {
        Some(json) => (Schema::from_json(json)?, false),
        None => {
            // Infer from Arrow schema
            let arrow_schema = delta.arrow_schema().await?;
            let schema = crate::schema::from_arrow_schema(&arrow_schema);
            eprintln!(
                "[dsrch] Inferred schema from Arrow: {} field(s)",
                schema.fields.len()
            );
            (schema, true)
        }
    };

    // ... rest of function unchanged, but use `inferred` in IndexConfig ...
}
```

This requires adding an `arrow_schema()` method to `DeltaSync` that returns the Arrow schema of the Delta table. If `DeltaSync` does not have this yet, add it:

```rust
// In src/delta.rs
pub async fn arrow_schema(&self) -> crate::error::Result<arrow_schema::Schema> {
    let table = deltalake::open_table(&self.source).await
        .map_err(|e| crate::error::SearchDbError::Delta(e.to_string()))?;
    let arrow_schema = table.get_schema()
        .map_err(|e| crate::error::SearchDbError::Delta(e.to_string()))?;
    // Convert DeltaSchema to ArrowSchema
    // ... implementation depends on deltalake crate version
    todo!("Map DeltaSchema -> ArrowSchema")
}
```

The exact implementation depends on the deltalake crate version. The `deltalake` crate typically provides `table.schema()` which returns a `StructType`, and there is a conversion to Arrow's `SchemaRef`. Check the crate docs for the exact API.

- [ ] **Step 4: Update `main.rs` CLI definition for `ConnectDelta`**

```rust
ConnectDelta {
    name: String,
    #[arg(long)]
    source: String,
    #[arg(long)]
    schema: Option<String>,
},
```

Update dispatch:

```rust
Commands::ConnectDelta { name, source, schema } => {
    commands::connect_delta::run(&storage, &name, &source, schema.as_deref()).await
}
```

Run: `cargo test`
Expected: **Passes.**

---

## Task 9: Handle booleans in `writer::build_document`

**Files:**
- Modify: `src/writer.rs`

Currently `build_document` handles `FieldType::Keyword` by extracting a string or calling `to_string()`. JSON booleans `true`/`false` will be coerced via `to_string()` which produces `"true"`/`"false"`. Verify this works and add a test.

- [ ] **Step 1: Write a test for boolean-as-keyword round trip**

Add to the test module in `src/writer.rs`:

```rust
#[test]
fn test_build_document_boolean_as_keyword() {
    let schema = Schema {
        fields: BTreeMap::from([("active".into(), FieldType::Keyword)]),
    };
    let tv_schema = schema.build_tantivy_schema();

    let doc_json = serde_json::json!({"_id": "d1", "active": true});
    let doc = build_document(&tv_schema, &schema, &doc_json, "d1").unwrap();

    let field = tv_schema.get_field("active").unwrap();
    let values: Vec<&str> = doc.get_all(field).flat_map(|v| v.as_str()).collect();
    assert_eq!(values, vec!["true"]);
}
```

Run: `cargo test --lib writer::tests`
Expected: **Passes** (booleans already handled by the `other => other.to_string()` branch in the Keyword/Text match arm). If it fails, adjust the match arm.

---

## Task 10: End-to-end integration test

**Files:**
- Modify: `src/commands/index.rs` (add integration test at bottom)

- [ ] **Step 1: Write an end-to-end test: create without schema, index, search**

This test exercises the full flow: create an index without schema, index NDJSON, verify schema was inferred, then verify documents are searchable.

```rust
#[test]
fn test_end_to_end_inferred_schema_search() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::new(dir.path().to_str().unwrap());

    // Create without schema
    new_index::run(&storage, "test", None, false).unwrap();

    // Index documents
    let ndjson = dir.path().join("data.ndjson");
    std::fs::write(
        &ndjson,
        r#"{"_id":"d1","name":"glucose","value":95.0}
{"_id":"d2","name":"a1c","value":6.1}
"#,
    ).unwrap();
    run(&storage, "test", Some(ndjson.to_str().unwrap())).unwrap();

    // Verify schema
    let config = storage.load_config("test").unwrap();
    assert_eq!(config.schema.fields.len(), 2);
    assert_eq!(config.schema.fields["name"], crate::schema::FieldType::Keyword);
    assert_eq!(config.schema.fields["value"], crate::schema::FieldType::Numeric);
    assert!(config.inferred);

    // Verify searchable — open index, search for name:"glucose"
    let index = tantivy::Index::open_in_dir(storage.tantivy_dir("test")).unwrap();
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    let tv_schema = index.schema();
    let name_field = tv_schema.get_field("name").unwrap();

    let query_parser = tantivy::query::QueryParser::for_index(&index, vec![name_field]);
    let query = query_parser.parse_query(r#"name:"glucose""#).unwrap();
    let results = searcher
        .search(&query, &tantivy::collector::TopDocs::with_limit(10))
        .unwrap();
    assert_eq!(results.len(), 1);
}
```

Run: `cargo test --lib commands::index::tests::test_end_to_end_inferred_schema_search`
Expected: **Passes.**

- [ ] **Step 2: Write evolution + search test**

```rust
#[test]
fn test_end_to_end_schema_evolution_preserves_searchability() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::new(dir.path().to_str().unwrap());

    new_index::run(&storage, "test", None, false).unwrap();

    // Batch 1
    let b1 = dir.path().join("b1.ndjson");
    std::fs::write(&b1, r#"{"_id":"d1","name":"glucose"}"#).unwrap();
    run(&storage, "test", Some(b1.to_str().unwrap())).unwrap();

    // Batch 2 — new field "value"
    let b2 = dir.path().join("b2.ndjson");
    std::fs::write(&b2, r#"{"_id":"d2","name":"a1c","value":6.1}"#).unwrap();
    run(&storage, "test", Some(b2.to_str().unwrap())).unwrap();

    // Both docs should be searchable
    let index = tantivy::Index::open_in_dir(storage.tantivy_dir("test")).unwrap();
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    assert_eq!(searcher.num_docs(), 2);

    // "value" field should be in schema
    let config = storage.load_config("test").unwrap();
    assert_eq!(config.schema.fields["value"], crate::schema::FieldType::Numeric);
}
```

Run: `cargo test`
Expected: **Passes.** Full test suite green.

---

## Task 11: Partial schema override test

**Files:**
- Modify: `src/commands/index.rs` (add test)

- [ ] **Step 1: Test partial schema with text override**

```rust
#[test]
fn test_partial_schema_override_text_field() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::new(dir.path().to_str().unwrap());

    // Create with partial schema: declare "notes" as text, leave rest for inference
    let schema_json = r#"{"fields":{"notes":"text"}}"#;
    new_index::run(&storage, "test", Some(schema_json), false).unwrap();

    // Manually set inferred=true so evolution is enabled for non-declared fields
    let mut config = storage.load_config("test").unwrap();
    config.inferred = true;
    storage.save_config("test", &config).unwrap();

    // Index doc with "notes" (declared) + "name" (to be inferred)
    let ndjson = dir.path().join("data.ndjson");
    std::fs::write(
        &ndjson,
        r#"{"_id":"d1","name":"glucose","notes":"fasting blood sample"}"#,
    ).unwrap();
    run(&storage, "test", Some(ndjson.to_str().unwrap())).unwrap();

    let config = storage.load_config("test").unwrap();
    assert_eq!(config.schema.fields["notes"], crate::schema::FieldType::Text); // preserved
    assert_eq!(config.schema.fields["name"], crate::schema::FieldType::Keyword); // inferred
}
```

Run: `cargo test`
Expected: **Passes.**

---

## Summary

| Task | What | Files Modified |
|------|------|---------------|
| 1 | `looks_like_date` regex helper | `src/schema.rs` |
| 2 | `infer_field_type` for single JSON value | `src/schema.rs` |
| 3 | `infer_schema` for batch of documents | `src/schema.rs` |
| 4 | `merge_schemas` + `merge_schemas_with_diff` | `src/schema.rs` |
| 5 | `inferred` flag on `IndexConfig` | `src/storage.rs` |
| 6 | `--schema` optional in `dsrch new` | `src/main.rs`, `src/commands/new_index.rs` |
| 7 | Schema inference + evolution in `dsrch index` | `src/commands/index.rs` |
| 8 | `--schema` optional in `dsrch connect-delta` | `src/main.rs`, `src/commands/connect_delta.rs`, `src/schema.rs` |
| 9 | Boolean-as-keyword verification | `src/writer.rs` |
| 10 | End-to-end integration tests | `src/commands/index.rs` |
| 11 | Partial schema override test | `src/commands/index.rs` |

Tasks 1-4 are pure functions with no external dependencies and can be implemented in any order. Task 5 is a data model change. Tasks 6-8 are the CLI integration changes. Tasks 9-11 are verification and edge case tests.

Total estimated time: 45-60 minutes for an experienced Rust developer. Each step is 2-5 minutes.

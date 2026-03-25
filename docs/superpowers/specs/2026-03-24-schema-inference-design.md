# Schema Inference for deltasearch

**Date:** 2026-03-24
**Status:** Draft

## Problem

deltasearch currently requires an explicit `--schema` argument for `dsrch new` and `dsrch connect-delta`. Users must know their field types upfront and declare them in JSON before they can index any data. This creates unnecessary friction for the common case: "I have some JSON, I want to search it."

Elasticsearch solves this with dynamic mapping: throw documents at it and it figures out the types. deltasearch should do the same. Schema becomes optional. When omitted, field types are inferred from the data itself.

## Design Principles

1. **Schema is always optional.** Every command that currently requires `--schema` makes it optional. If omitted, types are inferred from data.
2. **Explicit schema always wins.** If the user provides a schema, it is used exactly as declared. No inference, no overrides.
3. **Inference is conservative.** Strings default to `keyword` (exact match), not `text` (full-text). This is the safe default -- keyword works for all strings, while text tokenizes and stems which changes match semantics. Users opt into `text` explicitly.
4. **Schema evolution is additive.** New fields discovered in later documents are added to the schema. Existing fields never change type. This mirrors Elasticsearch's behavior.
5. **YAGNI.** Start simple. No nested object flattening. No array handling beyond "take the first element." No polymorphic fields.

## Inference Rules

### JSON Value to FieldType Mapping

When a JSON document is encountered without an explicit schema (or with a partial schema that does not cover all fields), each value is mapped to a `FieldType` according to these rules:

| JSON Value | Inferred FieldType | Rationale |
|---|---|---|
| `"hello"` (plain string) | `Keyword` | Exact match by default. Safe for IDs, codes, names. |
| `"2024-01-15T10:30:00Z"` (ISO 8601 string) | `Date` | Detected via regex before falling back to keyword. |
| `42` or `3.14` (number) | `Numeric` | JSON numbers map to f64. |
| `true` / `false` (boolean) | `Keyword` | Stored as string "true"/"false". Searchable as keyword. |
| `null` | (skipped) | Null values do not contribute to type inference. |
| `[...]` (array) | (skipped) | Arrays are not indexed. See "Arrays and Nested Objects" below. |
| `{...}` (object) | (skipped) | Nested objects are not indexed. See "Arrays and Nested Objects" below. |

### Date Detection

A string is classified as a `Date` if and only if it matches the following regex pattern:

```
^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}
```

This matches ISO 8601 datetime strings with at least second precision:
- `2024-01-15T10:30:00Z` -- yes
- `2024-01-15T10:30:00+05:30` -- yes
- `2024-01-15 10:30:00` -- yes (space separator)
- `2024-01-15` -- no (date-only, no time component)
- `Jan 15, 2024` -- no (not ISO format)

**Why regex, not try-parse?** A try-parse approach (`chrono::DateTime::parse_from_rfc3339`) is more correct but has two problems: (1) it is slower, running a full parser on every string value during inference; (2) it accepts formats we do not want to auto-detect (e.g., bare dates without times). The regex is a fast pre-filter. The actual parsing happens later in `writer::parse_date()` when the document is indexed. If the regex matches but the parse fails, the value falls back to being stored as a keyword -- but this should be extremely rare given how specific the regex is.

**Implementation:** A single `lazy_static` or `std::sync::LazyLock` compiled regex, checked with `is_match()`. No captures needed.

```rust
use std::sync::LazyLock;
use regex::Regex;

static ISO_DATETIME_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}").unwrap()
});

pub fn looks_like_date(s: &str) -> bool {
    ISO_DATETIME_RE.is_match(s)
}
```

### Booleans

JSON booleans are stored as keyword strings (`"true"` / `"false"`). This keeps the type system simple (4 types) and allows searching with `+field:"true"`. A dedicated boolean field type is not needed -- keyword handles it.

## Inference Entry Points

Schema inference activates in four scenarios, listed in order of preference:

### 0. `dsrch new --infer-from <file>` (PRIMARY — User-Directed Inference)

The recommended path for most users. Provide a sample file and let deltasearch figure out the types:

```bash
# Infer schema from a sample NDJSON file
dsrch new labs --infer-from data/sample.ndjson

# Preview the inferred schema without creating anything
dsrch new labs --infer-from data/sample.ndjson --dry-run
# prints: {"fields":{"name":"keyword","age":"numeric","created":"date"}}

# Preview, tweak, then create with overrides
dsrch new labs --infer-from data/sample.ndjson --dry-run
# user sees "notes" was inferred as keyword, wants text
dsrch new labs --infer-from data/sample.ndjson --schema '{"fields":{"notes":"text"}}'
```

**`--infer-from <path>`**: Read the file, infer field types from all rows, create the index with the inferred schema. The file is not indexed — just used for inference. Index starts empty.

**`--dry-run`**: Print the inferred schema as JSON to stdout and exit. Do not create the index. Allows the user to review and tweak before committing.

**`--infer-from` + `--schema` (combined)**: The `--schema` acts as an override — declared fields use the explicit type, other fields are inferred from the file. This is the recommended way to mark specific fields as `text` instead of the default `keyword`.

### 1. `dsrch new` Without `--schema` and Without `--infer-from`

- If neither `--schema` nor `--infer-from` is provided: create the index with an empty schema (`{"fields": {}}`). The schema will be populated on first `dsrch index` call via schema evolution.

An empty-schema index is valid. It has the three internal fields (`_id`, `_source`, `__present__`) and no user fields. Documents indexed into it trigger schema inference and evolution.

### 2. `dsrch index` With Schema Evolution

When indexing NDJSON into an index whose schema does not cover all fields in the incoming documents:

1. Parse the first batch of documents (or all documents if small).
2. For each field not already in the schema, infer the type from the first non-null value seen.
3. Merge the inferred fields into the existing schema (additive only).
4. If new fields were discovered, rebuild the tantivy schema and reindex.

See "Schema Evolution and Tantivy Immutability" below for the rebuild strategy.

### 3. `dsrch connect-delta` Without `--schema` (Auto-Inference from Arrow)

When connecting to a Delta Lake table without `--schema`, the Delta table's Arrow schema is used directly — no sample file needed:

```bash
# Schema inferred automatically from Delta table metadata
dsrch connect-delta labs --source /data/labs

# Preview what would be inferred
dsrch connect-delta labs --source /data/labs --dry-run
# prints: {"fields":{"name":"keyword","age":"numeric","created":"date"}}
```

- **Primary strategy: Arrow schema mapping.** Delta tables have an explicit Arrow schema. Map Arrow types to deltasearch field types:

| Arrow Type | deltasearch FieldType |
|---|---|
| `Utf8`, `LargeUtf8` | `Keyword` |
| `Int8/16/32/64`, `UInt8/16/32/64`, `Float16/32/64` | `Numeric` |
| `Timestamp(*)`, `Date32`, `Date64` | `Date` |
| `Boolean` | `Keyword` |
| `Struct`, `List`, `Map`, etc. | (skipped) |

- **Fallback: sample-based inference.** If the Arrow schema is not available (unlikely with Delta, but defensive), fall back to reading the first batch of rows and inferring from JSON values as in scenario 2.

Arrow schema mapping is strongly preferred because it is deterministic, covers all fields, and does not require reading any data.

## Schema Evolution and Tantivy Immutability

### The Problem

Tantivy schemas are **immutable once the index is created.** You cannot add a field to an existing tantivy index. This is a fundamental constraint. If a new document contains a field not in the current schema, we cannot simply add the field and continue.

### The Solution: Detect, Rebuild, Reindex

When schema evolution is triggered (a document contains a field not in the current schema):

1. **Detect** the new field(s) and infer their types.
2. **Merge** the new fields into the application-level `Schema` (in `searchdb.json`).
3. **Rebuild** the tantivy schema from the updated application schema.
4. **Drop and recreate** the tantivy index directory with the new schema.
5. **Reindex** all documents. Since `_source` contains the verbatim JSON for every document, we can read all existing documents from the old index, create the new index, and re-add them along with the new documents.

### Cost Analysis

Schema evolution requires a full reindex. This is expensive for large indexes. However:

- It only happens when a **new field** appears, not on every document.
- In practice, schemas stabilize quickly. After the first few batches, all fields are known.
- The reindex reads from the local tantivy index (`_source` field), not from Delta Lake. This is fast -- it is a local disk scan, not a network operation.
- For Delta-connected indexes, the alternative is `dsrch reindex`, which re-reads from Delta. Our approach is cheaper.

### Optimization: Batch Discovery

To minimize rebuilds, schema inference should scan all documents in a batch before committing. If indexing 10,000 NDJSON lines, scan all 10,000 for new fields first, then build the schema once, then index.

```
documents = read_all_ndjson_lines()
new_fields = infer_fields_from_batch(documents, existing_schema)
if new_fields is not empty:
    merged_schema = merge(existing_schema, new_fields)
    rebuild_index(merged_schema)  // reindex existing docs from _source
merged_schema = load_current_schema()
index_documents(documents, merged_schema)
```

### When NOT to Evolve

Schema evolution only applies to the **inferred** schema path. If the user provided an explicit schema via `--schema`, unknown fields are silently ignored (not indexed). This matches Elasticsearch's `dynamic: false` behavior. The rationale: if the user explicitly declared their schema, they know what fields they want. Extra fields in the data are noise.

## Overriding Inferred Types

The inference rules are deliberately conservative (strings default to keyword). Users need a way to override:

### Option 1: Partial Schema (Recommended)

Allow `--schema` to be a partial declaration. Fields listed in the schema use the declared type. Fields not listed are inferred.

```bash
# Declare "notes" as text (full-text), let everything else be inferred
dsrch new myindex --schema '{"fields":{"notes":"text"}}'
```

When inference runs, it skips fields already declared in the schema. This is the simplest approach and requires no new syntax.

### Option 2: Post-Hoc Schema Update (Future)

A `dsrch alter-schema` command that changes a field's type. This requires a full reindex (tantivy schema is immutable). Useful but not needed for v1.

```bash
# Change "notes" from keyword to text after initial inference
dsrch alter-schema myindex --field notes --type text
```

This is deferred. Option 1 covers the common case.

## Arrays and Nested Objects

### Arrays

JSON arrays are **not indexed** in v1. If a document has `"tags": ["a", "b", "c"]`, the `tags` field is skipped during inference and indexing. The original value is preserved in `_source` for round-trip fidelity.

**Future:** Array support could be added by treating arrays of primitives as multi-valued fields (tantivy supports multiple values per field). This is a natural extension but adds complexity to type inference (what if array elements have mixed types?).

### Nested Objects

Nested JSON objects are **not indexed** in v1. If a document has `"address": {"city": "NYC", "zip": "10001"}`, the `address` field is skipped.

**Future:** Nested objects could be flattened using dot notation (`address.city`, `address.zip`), following Elasticsearch's convention. This is a natural extension but requires recursive inference and field name escaping.

## Interaction with Existing Explicit Schema Path

The explicit schema path is unchanged. A compatibility matrix:

| `--schema` provided? | Schema in `searchdb.json`? | Behavior |
|---|---|---|
| Yes | N/A (creating new index) | Use explicit schema. No inference. |
| No | N/A (creating new index) | Create index with empty schema. Infer on first index. |
| N/A | Has fields | Index using existing schema. Evolve if new fields found (inferred mode only). |
| N/A | Has fields + was explicit | Index using existing schema. Ignore unknown fields (no evolution). |

### Tracking Schema Origin

Add an `inferred: bool` flag to `IndexConfig` (in `searchdb.json`):

```json
{
  "schema": {"fields": {"name": "keyword", "age": "numeric"}},
  "inferred": true,
  "delta_source": null,
  "index_version": null
}
```

- `inferred: true` -- schema was inferred (or partially inferred). Schema evolution is enabled.
- `inferred: false` (or absent, for backward compatibility) -- schema was explicitly provided. Schema evolution is disabled; unknown fields are silently ignored.

When `--schema` is provided as a partial schema alongside inference (Option 1 above), `inferred` is set to `true` because the non-declared fields will be inferred.

## Implementation Surface

### New Code

- `src/schema.rs`: Add `infer_field_type(value: &serde_json::Value) -> Option<FieldType>` function.
- `src/schema.rs`: Add `infer_schema(docs: &[serde_json::Value]) -> Schema` function.
- `src/schema.rs`: Add `merge_schemas(existing: &Schema, discovered: &Schema) -> Schema` function.
- `src/schema.rs`: Add `looks_like_date(s: &str) -> bool` function (regex-based).
- `src/schema.rs`: Add `from_arrow_schema(arrow_schema: &ArrowSchema) -> Schema` function.

### Modified Code

- `src/storage.rs`: Add `inferred: bool` field to `IndexConfig` (with `#[serde(default)]` for backward compat).
- `src/commands/new_index.rs`: Make `schema_json` parameter `Option<&str>`. Add `infer_from: Option<&str>` and `dry_run: bool`. If `infer_from`, read file and infer. If `dry_run`, print and exit.
- `src/commands/index.rs`: Before indexing, check for unknown fields. If schema is inferred, trigger evolution. If explicit, skip unknown fields.
- `src/commands/connect_delta.rs`: Make `schema_json` parameter `Option<&str>`. Add `dry_run: bool`. If no schema, infer from Arrow. If `dry_run`, print and exit.
- `src/main.rs`: Change `--schema` from required to optional. Add `--infer-from` and `--dry-run` flags to `New` and `ConnectDelta` command variants.

### Not Modified

- `src/writer.rs`: Unchanged. `build_document` already takes a schema and dispatches on field types. It does not need to know whether the schema was inferred or explicit.
- `src/searcher.rs`: Unchanged. Query parsing uses the tantivy schema, which is built the same way regardless of origin.

## Acceptance Criteria

### Core Inference

- [ ] `infer_field_type` correctly maps: string to keyword, number to numeric, ISO datetime string to date, boolean to keyword, null/array/object to None.
- [ ] `looks_like_date` matches ISO 8601 datetime strings (with T or space separator) and rejects plain strings, date-only strings, and other formats.
- [ ] `infer_schema` scans a batch of documents and returns a schema covering all non-null, non-nested, non-array fields.
- [ ] When multiple documents have different non-null values for the same field, the first non-null value determines the type.

### Schema Evolution

- [ ] Indexing documents with unknown fields into an inferred-schema index triggers schema evolution: new fields are added, index is rebuilt.
- [ ] Indexing documents with unknown fields into an explicit-schema index silently skips those fields (no evolution, no error).
- [ ] Schema evolution preserves all existing documents (reindexed from `_source`).
- [ ] The `inferred` flag in `searchdb.json` correctly tracks schema origin.

### CLI Changes

- [ ] `dsrch new myindex` (no `--schema`) succeeds and creates an index with empty schema and `inferred: true`.
- [ ] `dsrch new myindex --schema '{...}'` works exactly as before (backward compatible).
- [ ] `dsrch new myindex --infer-from data.ndjson` reads the file, infers schema, creates index (empty, ready to index).
- [ ] `dsrch new myindex --infer-from data.ndjson --dry-run` prints inferred schema JSON to stdout and exits without creating anything.
- [ ] `dsrch new myindex --infer-from data.ndjson --schema '{"fields":{"notes":"text"}}'` uses `text` for `notes`, infers everything else.
- [ ] `dsrch connect-delta myindex --source <uri>` (no `--schema`) infers schema from Arrow schema.
- [ ] `dsrch connect-delta myindex --source <uri> --dry-run` prints inferred Arrow schema mapping and exits.
- [ ] `dsrch connect-delta myindex --source <uri> --schema '{...}'` works exactly as before.

### Arrow Schema Mapping

- [ ] `from_arrow_schema` maps Utf8 to Keyword, numeric Arrow types to Numeric, Timestamp/Date to Date, Boolean to Keyword.
- [ ] Complex Arrow types (Struct, List, Map) are skipped.

### Partial Schema Override

- [ ] A partial `--schema` that declares some fields as `text` is respected: those fields use `text`, other fields are inferred.
- [ ] The index is marked `inferred: true` when partial schema is used.

### Edge Cases

- [ ] An empty NDJSON file indexed into an empty-schema index produces no error and no schema change.
- [ ] A document with only null values produces no schema change.
- [ ] A document with only nested objects and arrays produces no schema change.
- [ ] Schema evolution does not trigger if the new document has only fields already in the schema (even if values have different JSON types -- e.g., a number where a keyword was inferred). Type coercion or error is handled by `build_document`, not by inference.

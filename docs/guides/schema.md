# Schema

deltasearch schemas map field names to types. Schemas are optional -- you can declare one explicitly, infer one from sample data, or let deltasearch figure it out automatically.

## Field types

deltasearch has four field types. If you have used Elasticsearch, these will be familiar.

### `keyword`

Exact-match string field. Stored and indexed with a `raw` tokenizer (no analysis, no stemming). Use this for IDs, status codes, categories, tags, enums -- anything where you want `status:"active"` to match exactly.

**Elasticsearch equivalent:** `keyword` type.

```bash
# Exact match
dsrch search orders -q '+status:"shipped"'

# Will NOT match "SHIPPED" -- keyword is case-sensitive
```

### `text`

Full-text searchable string field. Stored and indexed with the `en_stem` tokenizer (English stemming, lowercasing, stop words). Use this for titles, descriptions, notes, body text -- anything you want to search by meaning rather than exact value.

**Elasticsearch equivalent:** `text` type with `standard` analyzer.

```bash
# Stemmed search: "running" matches "runs", "ran", "runner"
dsrch search articles -q "running"

# Phrase search
dsrch search articles -q 'description:"machine learning"'
```

### `numeric`

64-bit floating-point field. Stored, indexed, and fast (columnar). Use this for prices, quantities, scores, measurements -- anything you want range queries on.

**Elasticsearch equivalent:** `double` or `float` type.

```bash
# Range query (inclusive)
dsrch search products -q 'price:[10 TO 50]'

# Open-ended range
dsrch search products -q 'price:{100 TO *}'
```

### `date`

ISO 8601 datetime field. Stored, indexed, and fast (columnar). Use this for timestamps, created_at, updated_at -- anything time-based.

**Elasticsearch equivalent:** `date` type.

```bash
# Date range
dsrch search events -q 'created_at:[2024-01-01T00:00:00Z TO 2024-12-31T23:59:59Z]'
```

## Declaring a schema

Pass a JSON object with a `fields` key mapping field names to types:

```bash
dsrch new customers --schema '{
  "fields": {
    "name": "text",
    "email": "keyword",
    "age": "numeric",
    "signed_up": "date",
    "status": "keyword"
  }
}'
```

## Schema inference

If you do not provide a schema, deltasearch infers types from your data. The rules are:

| JSON value | Inferred type |
|-----------|---------------|
| String | `keyword` (default) |
| String matching `YYYY-MM-DDThh:mm:ss` | `date` |
| Number (integer or float) | `numeric` |
| Boolean | `keyword` (stored as `"true"` / `"false"`) |
| Null | Skipped (until a non-null value appears) |
| Array | Skipped (not indexable in v1) |
| Object | Skipped (not indexable in v1) |

The inference uses a "first non-null wins" strategy: the first document that provides a non-null value for a field determines that field's type for all subsequent documents.

Note that inference always maps strings to `keyword`. If you want full-text search on a string field, you need to declare it as `text` explicitly in the schema.

### Infer from a sample file

Use `--infer-from` to infer a schema from a sample NDJSON file without creating the index:

```bash
# Preview the inferred schema
dsrch new products --infer-from sample.ndjson --dry-run
```

Output:

```json
{"fields":{"name":"keyword","price":"numeric","created":"date","category":"keyword"}}
```

Once you are happy with the schema (perhaps promoting `name` from `keyword` to `text`), create the index:

```bash
dsrch new products --schema '{
  "fields": {
    "name": "text",
    "price": "numeric",
    "created": "date",
    "category": "keyword"
  }
}'
```

### Infer from a Delta table

When using `connect-delta`, if you omit the `--schema` flag, deltasearch infers types from the Delta table's Arrow schema:

```bash
# Preview inferred schema from Delta
dsrch connect-delta products --source /path/to/delta_table --dry-run

# Accept the inferred schema
dsrch connect-delta products --source /path/to/delta_table
```

Arrow type mapping:

| Arrow type | deltasearch type |
|-----------|---------------|
| `Utf8`, `LargeUtf8` | `keyword` |
| `Boolean` | `keyword` |
| `Int8` through `Float64` | `numeric` |
| `Timestamp`, `Date32`, `Date64` | `date` |
| `List`, `Struct`, `Map` | Skipped |

## Dynamic schema evolution

When you index documents with fields not in the current schema, deltasearch infers types for the new fields and adds them to the schema. Existing fields keep their original types.

This means you can start with a minimal schema and let it grow as your data evolves -- similar to Elasticsearch's dynamic mapping.

## Internal fields

deltasearch adds three internal fields to every index. You do not declare these in your schema.

### `_id`

Document identity. Used for upsert deduplication. If your data includes `_id`, deltasearch uses it. Otherwise, a UUID is generated.

### `_source`

The verbatim JSON of the original document. Search results are returned from `_source`, ensuring round-trip fidelity.

### `__present__`

A hidden field that tracks which fields are non-null in each document. This powers null/not-null queries:

```bash
# Find documents where "email" field exists
dsrch search users --dsl '{"exists": {"field": "email"}}'
```

## Overwriting an index

If an index already exists, `dsrch new` will fail. Use `--overwrite` to replace it:

```bash
dsrch new products --schema '{"fields":{"name":"text"}}' --overwrite
```

This destroys the existing index and creates a fresh one.

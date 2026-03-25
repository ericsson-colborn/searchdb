# Searching

deltasearch supports two query syntaxes: a Lucene-like query string (default) and an Elasticsearch-compatible query DSL. Both produce the same results -- choose whichever fits your workflow.

## Query string syntax

The query string syntax is tantivy's native parser, which follows Lucene conventions. Use it with the `-q` flag.

### Basic search

Search across all `text` fields:

```bash
dsrch search articles -q "machine learning"
```

This finds documents where any text field contains "machine" or "learning" (OR semantics by default).

### Field-scoped search

Search a specific field:

```bash
dsrch search articles -q "title:kubernetes"
```

### Exact match on keyword fields

Keyword fields require exact matches. Quote the value:

```bash
dsrch search orders -q 'status:"shipped"'
```

### Required terms (`+`)

Prefix with `+` to require a term (AND):

```bash
# Must match "genre:sci-fi" AND "year:2024"
dsrch search movies -q '+genre:"sci-fi" +year:[2024 TO 2024]'
```

### Excluded terms (`-`)

Prefix with `-` to exclude:

```bash
# Sci-fi but NOT horror
dsrch search movies -q '+genre:"sci-fi" -genre:"horror"'
```

### Range queries

Numeric and date fields support range queries:

```bash
# Inclusive range: 10 <= price <= 100
dsrch search products -q 'price:[10 TO 100]'

# Exclusive range: price > 100
dsrch search products -q 'price:{100 TO *}'

# Date range
dsrch search events -q 'created:[2024-01-01T00:00:00Z TO 2024-06-30T23:59:59Z]'
```

Square brackets `[ ]` mean inclusive. Curly braces `{ }` mean exclusive. Use `*` for unbounded.

### Phrase search

Wrap in double quotes for phrase matching (terms must appear in order):

```bash
dsrch search articles -q 'description:"neural network"'
```

### Combining everything

```bash
dsrch search products \
  -q '+category:"electronics" +price:[50 TO 200] -brand:"acme" wireless'
```

This finds electronics between $50-$200, excluding the "acme" brand, preferring matches with "wireless" in any text field.

## Elasticsearch query DSL

Use the `--dsl` flag to pass an Elasticsearch-compatible query in JSON. This is useful for programmatic queries, complex boolean logic, or migrating from Elasticsearch.

### Passing DSL queries

Three ways to provide the DSL JSON:

```bash
# Inline JSON string
dsrch search products --dsl '{"term": {"category": "electronics"}}'

# From a file
dsrch search products --dsl @query.json

# From stdin
echo '{"match_all": {}}' | dsrch search products --dsl -
```

### Supported query types

#### `term` -- exact match

```json
{"term": {"status": "active"}}
```

Equivalent to: `+status:"active"`

#### `terms` -- match any of multiple values

```json
{"terms": {"status": ["active", "pending"]}}
```

#### `match` -- full-text search

```json
{"match": {"description": "machine learning"}}
```

Equivalent to: `description:machine description:learning`

#### `match_phrase` -- phrase search

```json
{"match_phrase": {"description": "machine learning"}}
```

Equivalent to: `description:"machine learning"`

#### `range` -- numeric and date ranges

```json
{"range": {"price": {"gte": 10, "lte": 100}}}
```

Supported operators: `gt`, `gte`, `lt`, `lte`.

```json
{"range": {"created_at": {"gte": "2024-01-01T00:00:00Z", "lt": "2025-01-01T00:00:00Z"}}}
```

Equivalent to: `price:[10 TO 100]`

#### `exists` -- field existence

```json
{"exists": {"field": "email"}}
```

Finds documents where the `email` field is non-null.

#### `bool` -- compound queries

Combine multiple queries with boolean logic:

```json
{
  "bool": {
    "must": [
      {"term": {"category": "electronics"}},
      {"range": {"price": {"lte": 100}}}
    ],
    "must_not": [
      {"term": {"brand": "acme"}}
    ],
    "should": [
      {"match": {"description": "wireless"}}
    ]
  }
}
```

- `must` -- all clauses must match (AND)
- `must_not` -- no clause may match (NOT)
- `should` -- at least one clause should match (OR, boosts score)

#### `match_all` / `match_none`

```json
{"match_all": {}}
{"match_none": {}}
```

## Pagination

Use `--limit` and `--offset` for pagination:

```bash
# First 10 results
dsrch search products -q "shoes" --limit 10

# Next 10 results
dsrch search products -q "shoes" --limit 10 --offset 10
```

Default limit is 20.

## Field projection

Return only specific fields with `--fields`:

```bash
dsrch search products -q "shoes" --fields name,price
```

This reduces output size when you do not need every field.

## Relevance scoring

Add `--score` to include the BM25 relevance score in each result:

```bash
dsrch search articles -q "kubernetes" --score
```

## Output format

deltasearch auto-detects the output mode:

- **Terminal** (interactive) -- human-readable text format
- **Piped** (non-interactive) -- NDJSON, one JSON object per line

Override with `--output`:

```bash
# Force JSON output even in a terminal
dsrch search products -q "shoes" --output json

# Force text output even when piped
dsrch search products -q "shoes" --output text | less
```

## Tips for Elasticsearch users

| Elasticsearch | deltasearch equivalent |
|--------------|---------------------|
| `GET /index/_search` with JSON body | `dsrch search index --dsl '<json>'` |
| `GET /index/_search?q=term` | `dsrch search index -q "term"` |
| `GET /index/_doc/123` | `dsrch get index 123` |
| `_source` filtering | `--fields name,price` |
| `size` / `from` | `--limit` / `--offset` |

The biggest difference: deltasearch's query string and DSL are separate flags on the same command, not separate API endpoints.

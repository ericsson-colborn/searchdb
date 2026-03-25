# Quickstart

Get from zero to search in 5 minutes. This tutorial creates an index, loads sample data, and runs queries.

## Prerequisites

- deltasearch binary installed (see [Installation](../../README.md#installation))
- A terminal

## Step 1: Create an index with a schema

```bash
dsrch new movies --schema '{
  "fields": {
    "title": "text",
    "genre": "keyword",
    "year": "numeric",
    "director": "keyword",
    "description": "text"
  }
}'
```

This creates an index called `movies` with five fields. The `text` type enables full-text search with stemming. The `keyword` type is for exact matching (filters, facets). The `numeric` type supports range queries.

You can also skip the schema entirely and let deltasearch infer types from your data. See the [Schema guide](../guides/schema.md) for details.

## Step 2: Index some documents

Create a file called `movies.ndjson` with one JSON document per line:

```bash
cat > movies.ndjson << 'EOF'
{"_id":"1","title":"The Matrix","genre":"sci-fi","year":1999,"director":"Wachowski","description":"A computer hacker learns about the true nature of reality"}
{"_id":"2","title":"Inception","genre":"sci-fi","year":2010,"director":"Nolan","description":"A thief who steals corporate secrets through dream-sharing technology"}
{"_id":"3","title":"The Shawshank Redemption","genre":"drama","year":1994,"director":"Darabont","description":"Two imprisoned men bond over a number of years finding solace and eventual redemption"}
{"_id":"4","title":"Pulp Fiction","genre":"crime","year":1994,"director":"Tarantino","description":"The lives of two mob hitmen and a pair of bandits intertwine in tales of violence and redemption"}
{"_id":"5","title":"Interstellar","genre":"sci-fi","year":2014,"director":"Nolan","description":"A team of explorers travel through a wormhole in space to ensure humanity's survival"}
EOF
```

Index it:

```bash
dsrch index movies -f movies.ndjson
```

You can also pipe data directly:

```bash
cat movies.ndjson | dsrch index movies
```

## Step 3: Search

**Full-text search** -- finds stemmed matches (e.g., "travel" matches "travels"):

```bash
dsrch search movies -q "redemption"
```

**Filter by keyword** -- exact match on the `genre` field:

```bash
dsrch search movies -q '+genre:"sci-fi"'
```

**Range query** -- movies from the 1990s:

```bash
dsrch search movies -q '+year:[1990 TO 1999]'
```

**Combine filters** -- sci-fi movies directed by Nolan:

```bash
dsrch search movies -q '+genre:"sci-fi" +director:"Nolan"'
```

**Elasticsearch DSL** -- same query using the `--dsl` flag:

```bash
dsrch search movies --dsl '{
  "bool": {
    "must": [
      {"term": {"genre": "sci-fi"}},
      {"term": {"director": "Nolan"}}
    ]
  }
}'
```

## Step 4: Retrieve a document

Fetch a single document by its `_id`:

```bash
dsrch get movies 3
```

## Step 5: Check stats

```bash
dsrch stats movies
```

This shows the document count, segment count, schema, and (if connected to Delta Lake) the sync status.

## Next steps

- [Concepts](concepts.md) -- understand how Delta Lake, tantivy, and compaction work together
- [Schema guide](../guides/schema.md) -- field types, schema inference, `--dry-run`
- [Searching guide](../guides/searching.md) -- full query syntax reference
- [Compaction guide](../guides/compaction.md) -- background worker for Delta Lake sync

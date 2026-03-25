# ES Query DSL Compilation: Design Spec

**Date:** 2026-03-24
**Status:** Draft
**Author:** SearchDB team

## Problem

SearchDB currently only accepts tantivy's native Lucene-like query syntax via `-q`. Elasticsearch users expect to query with JSON DSL bodies -- the `{"query": {"bool": {"must": [...]}}}` format. Without this, SearchDB cannot be a drop-in replacement for ES read paths.

## Goal

Accept Elasticsearch query DSL (JSON) and compile it to tantivy `Box<dyn Query>` objects. This is the first of the three planned quickwit-derived components listed in CLAUDE.md.

## Research: Quickwit's Query Compilation Pipeline

### Architecture

The `quickwit-query` crate (Apache 2.0, quickwit-oss/quickwit) implements a three-stage pipeline:

```
Stage 1: ES JSON --> ElasticQueryDsl       (serde deserialization)
Stage 2: ElasticQueryDsl --> QueryAst      (ConvertibleToQueryAst trait)
Stage 3: QueryAst --> Box<dyn TantivyQuery> (BuildTantivyAst trait)
```

**Stage 1** uses serde `#[serde(rename_all = "snake_case")]` on an enum `ElasticQueryDslInner` to route JSON like `{"term": {...}}` to the correct variant. Each variant has its own struct with `#[serde(deny_unknown_fields)]` for strict parsing.

**Stage 2** converts each ES DSL struct into a `QueryAst` enum -- an intermediate representation that is ES-agnostic. This is where ES-specific sugar (like `match` defaulting to OR, `match_phrase` implying phrase mode) gets normalized. The conversion is infallible for most types (returns `anyhow::Result`).

**Stage 3** takes a `QueryAst` node and a `BuildTantivyAstContext` (which holds `&TantivySchema` + `&TokenizerManager`) and produces a `TantivyQueryAst` -- a private enum that wraps tantivy's `BooleanQuery`, leaf queries, and const predicates (match-all/match-none). The `TantivyQueryAst` has a `.simplify()` method that flattens degenerate booleans, and `.into()` produces a final `Box<dyn TantivyQuery>`.

### Supported ES Query Types

quickwit-query supports 15 query types:

| ES Query Type | QueryAst Target | Notes |
|---|---|---|
| `bool` | `BoolQuery` | must/must_not/should/filter + minimum_should_match |
| `term` | `TermQuery` | Exact match, supports boost and case_insensitive |
| `terms` | `TermSetQuery` | Multi-value OR for a single field |
| `match` | `FullTextQuery` | Tokenized, operator AND/OR, zero_terms_query |
| `match_phrase` | `FullTextQuery` (phrase mode) | Phrase query with optional slop + analyzer |
| `match_phrase_prefix` | `PhrasePrefixQuery` | Autocomplete-style prefix on last term |
| `match_bool_prefix` | `FullTextQuery` (bool_prefix mode) | Bool query with prefix on last term |
| `multi_match` | Expands to bool/fulltext | Searches across multiple fields |
| `range` | `RangeQuery` | gt/gte/lt/lte, numeric + date + string, format param |
| `exists` | `FieldPresenceQuery` | Field existence check |
| `query_string` | `UserInputQuery` | Lucene syntax passthrough |
| `prefix` | `PhrasePrefixQuery` | Prefix matching |
| `wildcard` | `WildcardQuery` | Glob patterns (* and ?) |
| `regexp` | `RegexQuery` | Full regex matching |
| `match_all` / `match_none` | `MatchAll` / `MatchNone` | Constant predicates |

### Dependency Analysis

`quickwit-query` depends on three internal quickwit crates:

1. **`quickwit-datetime`** -- Used in `json_literal.rs` for date parsing (`parse_date_time_str`, `parse_timestamp`, `DateTimeInputFormat`) and in `elastic_query_dsl/range_query.rs` for `StrptimeParser` (Java date format conversion). Dependencies: `tantivy`, `time`, `time-fmt`, `itertools`, `serde`. Relatively lightweight.

2. **`quickwit-common`** -- Used only in `query_ast/field_presence.rs` for `PathHasher` and `FIELD_PRESENCE_FIELD_NAME`. This crate is heavy: pulls in `hyper`, `prometheus`, `governor`, `sysinfo`, `pnet`, `rayon`, `futures`, etc. The actual usage is two items.

3. **`quickwit-proto`** -- Not directly imported in query compilation code. Listed as a dependency in Cargo.toml but used only tangentially (possibly for aggregation types). This crate is extremely heavy: `tonic`, `prost`, `sqlx`, `tokio`, `opentelemetry`, etc.

External dependencies beyond what we already have: `serde_with` (for `OneOrMany` deserialization in bool queries), `once_cell`, `regex`, `time`.

## Design Decisions

### Vendor, Not Depend

**Decision: Vendor a surgical subset of quickwit-query into our codebase.**

Rationale:

1. **Dependency weight.** `quickwit-query` pulls in `quickwit-common` (hyper, prometheus, sysinfo, rayon) and `quickwit-proto` (tonic, prost, tokio, opentelemetry). These add hundreds of crates to our dependency tree for features we do not need. SearchDB's value proposition is "single binary, zero deps" -- adding a gRPC stack and Prometheus for query parsing is unacceptable.

2. **quickwit-query is not published on crates.io.** It lives in a monorepo workspace. Depending on it means either a git dependency on the entire quickwit repo or maintaining a fork. Both are maintenance liabilities.

3. **We need a subset.** We do not need aggregations, `CacheNode`, `PhrasePrefixQuery`, `WildcardQuery`, `RegexQuery`, `UserInputQuery`, the custom tokenizer manager, or field presence hashing (we have our own `__present__` field). The subset we need is roughly 1,200 lines -- well within "surgical borrow" territory.

4. **quickwit-common usage is trivial.** `PathHasher` and `FIELD_PRESENCE_FIELD_NAME` are used only in `FieldPresenceQuery`, which we will not vendor (our `exists` maps to `__present__` field queries instead).

5. **quickwit-datetime usage is replaceable.** Date parsing in `json_literal.rs` can use `chrono` (which we already depend on) instead of `quickwit-datetime`'s `time`-based parsers.

6. **License compatibility.** quickwit-query is Apache 2.0. Vendoring with attribution is explicitly permitted.

### V1 Query Types (Conservative)

We support the 8 query types that cover the vast majority of ES usage:

| Query Type | Priority | Rationale |
|---|---|---|
| `bool` | Must have | Foundation -- all compound queries use this |
| `term` | Must have | Exact match on keyword fields (most common filter) |
| `terms` | Must have | Multi-value exact match (IN queries) |
| `match` | Must have | Full-text search (the primary ES use case) |
| `match_phrase` | Must have | Phrase matching ("exact sequence of words") |
| `range` | Must have | Numeric and date filtering (gte/lte/gt/lt) |
| `match_all` / `match_none` | Must have | Trivial, needed for bool query composition |
| `exists` | Must have | Null/not-null filtering, maps to our `__present__` field |

**Deferred to v2:**

| Query Type | Reason to Defer |
|---|---|
| `multi_match` | Sugar over bool+match -- users can write the bool themselves |
| `query_string` | We already have `-q` for Lucene syntax; not needed in JSON DSL |
| `match_bool_prefix` | Autocomplete use case, niche |
| `match_phrase_prefix` | Autocomplete use case, niche |
| `prefix` | Can be expressed as `wildcard` or handled later |
| `wildcard` | Niche; tantivy supports it, easy to add later |
| `regexp` | Niche; tantivy supports it, easy to add later |

### QueryAst: Do We Need It?

**Decision: Skip the intermediate QueryAst. Compile ES DSL directly to `Box<dyn tantivy::query::Query>`.**

Rationale: quickwit uses `QueryAst` as a serializable intermediate representation that can be shipped across nodes in a distributed search cluster. SearchDB is single-binary, single-node. The extra abstraction adds complexity without benefit. Each ES DSL struct will implement a `compile` method that takes `(&TantivySchema, &Schema)` and returns `Box<dyn Query>`.

If we later need a serializable IR (e.g., for a query cache or query logging), we can introduce it then. YAGNI applies.

### Exists Query Mapping

quickwit's `ExistsQuery` maps to a `FieldPresenceQuery` that uses a dedicated `_field_presence` fast field with path hashing (from `quickwit-common::PathHasher`). This is not how SearchDB works.

SearchDB already has a `__present__` field: a raw-tokenized text field where each document gets tokens `__all__` plus one token per non-null field name. An `exists` query for field `name` compiles to:

```rust
TermQuery::new(Term::from_field_text(present_field, "name"), IndexRecordOption::Basic)
```

This is simpler and already works with our indexing pipeline.

### Range Query: Fast Fields Required

quickwit's range query compilation uses `FastFieldRangeQuery`, which requires the field to be marked as "fast" (tantivy's equivalent of doc values). Our schema already marks `numeric` and `date` fields as fast. Range queries on `keyword` or `text` fields will return an error -- this matches ES behavior where range queries on non-numeric fields are uncommon and usually a mistake.

### Date Parsing

quickwit delegates date parsing in range queries to `quickwit-datetime`, which supports RFC 3339, RFC 2822, Unix timestamps, and Java date format strings. For v1, we will support:

- RFC 3339 (`2024-01-15T10:30:00Z`) -- via `chrono`
- Unix timestamps as numbers -- via simple conversion
- ISO 8601 date-only (`2024-01-15`) -- via `chrono`

Java date format strings (the ES `format` parameter in range queries) are deferred to v2.

## CLI Interface

### Option: `--dsl` flag on search command

```bash
# Query string (existing)
searchdb search myindex -q '+name:"glucose"'

# ES DSL via --dsl flag (new)
searchdb search myindex --dsl '{"bool":{"must":[{"term":{"name":"glucose"}}]}}'

# ES DSL via stdin with - marker
echo '{"match":{"notes":"diabetes"}}' | searchdb search myindex --dsl -

# ES DSL from file
searchdb search myindex --dsl @query.json
```

**Design:**
- `-q` and `--dsl` are mutually exclusive (clap `conflicts_with`)
- `--dsl` accepts a JSON string, `-` for stdin, or `@path` for a file
- The JSON is the contents of the `"query"` field in an ES search body -- not the full `{"query": {...}}` wrapper. Users pass the query object directly. This avoids parsing `size`, `from`, `_source`, etc. which have their own CLI flags (`--limit`, `--offset`, `--fields`).
- Existing flags (`--limit`, `--offset`, `--fields`, `--score`) work with both `-q` and `--dsl`

### Why not a separate command?

A separate `searchdb dsl-search` command would fragment the CLI. The search command already has all the plumbing for output, pagination, and field projection. Adding `--dsl` as an alternative to `-q` is the minimal change.

## File Structure

```
src/
  es_dsl/
    mod.rs              # ElasticQueryDsl enum, top-level deserialize + compile
    bool_query.rs       # BoolQuery: must/must_not/should/filter
    term_query.rs       # TermQuery: exact match on a field
    terms_query.rs      # TermsQuery: multi-value exact match
    match_query.rs      # MatchQuery: tokenized full-text
    match_phrase.rs     # MatchPhraseQuery: phrase matching
    range_query.rs      # RangeQuery: gt/gte/lt/lte on fast fields
    exists_query.rs     # ExistsQuery: maps to __present__ field
    one_field_map.rs    # Helper: deserialize {"field_name": <params>} pattern
  searcher.rs           # Updated: new fn search_dsl() alongside existing search()
  commands/search.rs    # Updated: handle --dsl flag
  main.rs               # Updated: --dsl arg on Search variant
```

Estimated size: ~800-1,000 lines of new code (excluding tests), ~400 lines of tests.

## Integration with Existing Searcher

The current `searcher::search()` takes a `query_str: &str` and uses `QueryParser::parse_query()` to produce a `Box<dyn Query>`. The new DSL path produces the same type -- `Box<dyn Query>` -- but through deserialization + compilation instead of parsing.

```rust
// New function in searcher.rs
pub fn search_dsl(
    index: &Index,
    app_schema: &Schema,
    dsl_json: &str,          // raw JSON string
    limit: usize,
    offset: usize,
    fields: Option<&[String]>,
    include_score: bool,
) -> Result<Vec<SearchHit>> {
    let tv_schema = index.schema();
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Deserialize ES DSL JSON
    let es_query: ElasticQueryDsl = serde_json::from_str(dsl_json)?;

    // Compile to tantivy query
    let query = es_query.compile(&tv_schema, app_schema)?;

    // Same search execution path as search()
    let top_docs = searcher.search(&query, &TopDocs::with_limit(limit + offset))?;
    // ... rest is identical to search()
}
```

The key insight: after compilation, the execution path is identical. The `search_dsl` function produces a `Box<dyn Query>` and hands it to `searcher.search()`, just like the query parser path. We should refactor to extract the shared execution into a helper:

```rust
// Internal helper: execute a pre-built query
fn execute_query(
    index: &Index,
    query: &dyn Query,
    limit: usize,
    offset: usize,
    fields: Option<&[String]>,
    include_score: bool,
) -> Result<Vec<SearchHit>>
```

Then both `search()` (query string) and `search_dsl()` (ES DSL) call `execute_query()`.

The `search_with_gap` and `get_with_gap` functions also need DSL variants, but the structure is the same -- build ephemeral index for gap rows, search both, dedup.

## Compilation Details

### Top-Level Dispatch

```rust
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ElasticQueryDsl {
    Bool(BoolQuery),
    Term(TermQuery),
    Terms(TermsQuery),
    Match(MatchQuery),
    MatchPhrase(MatchPhraseQuery),
    Range(RangeQuery),
    Exists(ExistsQuery),
    MatchAll(MatchAllQuery),
    MatchNone(MatchNoneQuery),
}
```

Serde's `rename_all = "snake_case"` handles the ES JSON keys: `match_phrase` maps to `MatchPhrase`, `match_all` maps to `MatchAll`, etc.

### OneFieldMap Pattern

ES DSL uses a pattern where the field name is a JSON key: `{"term": {"product_id": {"value": "123"}}}`. quickwit solves this with a `OneFieldMap<T>` helper that deserializes a single-key object into `(field: String, value: T)`. We vendor this helper -- it is ~30 lines.

### Bool Query Compilation

Bool queries compile recursively. Each clause (`must`, `should`, `must_not`, `filter`) contains child queries that are themselves compiled, then assembled into a `BooleanQuery`:

- `must` clauses --> `Occur::Must`
- `should` clauses --> `Occur::Should`
- `must_not` clauses --> `Occur::MustNot`
- `filter` clauses --> `Occur::Must` with `ConstScoreQuery` wrapper (no scoring)

Edge case: if only `must_not` clauses exist, we prepend an implicit `match_all` must clause. This matches ES behavior.

### Term Query Compilation

```
{"term": {"status": "active"}}
-->
TermQuery::new(Term::from_field_text(field, "active"), IndexRecordOption::Basic)
```

For numeric fields, the string value is parsed to f64 and `Term::from_field_f64()` is used. For date fields, the string is parsed as a datetime.

### Match Query Compilation

```
{"match": {"notes": {"query": "blood glucose", "operator": "and"}}}
```

1. Look up the field in the tantivy schema to find its tokenizer
2. Tokenize "blood glucose" using that tokenizer --> ["blood", "glucos"] (stemmed)
3. Build term queries for each token
4. Combine with BooleanQuery using the operator (AND -> Occur::Must, OR -> Occur::Should)

If the field uses `en_stem` tokenizer, the text is tokenized with stemming. If the field uses `raw` tokenizer (keyword fields), the entire text becomes a single term.

### Range Query Compilation

```
{"range": {"age": {"gte": 18, "lt": 65}}}
-->
FastFieldRangeQuery::new(
    Bound::Included(Term::from_field_f64(field, 18.0)),
    Bound::Excluded(Term::from_field_f64(field, 65.0)),
)
```

Requires the field to be a fast field (our `numeric` and `date` types are).

### Exists Query Compilation

```
{"exists": {"field": "status"}}
-->
TermQuery::new(Term::from_field_text(present_field, "status"), IndexRecordOption::Basic)
```

Uses our existing `__present__` field. No need for quickwit's `PathHasher` mechanism.

## New Dependencies

| Crate | Purpose | Already in tree? |
|---|---|---|
| `serde_with` | `OneOrMany` deserialization for bool query clauses | No -- new |

That is the only new dependency. `serde`, `serde_json`, `tantivy`, `anyhow`, `thiserror`, and `chrono` are already in our Cargo.toml.

`serde_with` is a well-maintained, widely-used crate. Its purpose here: ES bool queries accept both a single object and an array for `must`/`should`/etc. (e.g., `"must": {"term": {...}}` vs `"must": [{"term": {...}}]`). `serde_with`'s `OneOrMany` handles this transparently. Alternative: write a custom deserializer (~40 lines). Either approach works; `serde_with` is cleaner.

## Attribution

All vendored code originates from `quickwit-oss/quickwit` (Apache 2.0). We will:

1. Add a `LICENSE-THIRD-PARTY` file with the quickwit Apache 2.0 notice
2. Add a comment at the top of `src/es_dsl/mod.rs`:
   ```rust
   //! Elasticsearch query DSL compilation to tantivy queries.
   //!
   //! Architecture derived from quickwit-query (https://github.com/quickwit-oss/quickwit)
   //! Copyright 2021-Present Datadog, Inc. Licensed under Apache 2.0.
   //! Adapted for SearchDB's schema model and field types.
   ```

## Acceptance Criteria

### Functional

1. `searchdb search myindex --dsl '{"term":{"status":"active"}}'` returns documents where `status` is exactly "active"
2. `searchdb search myindex --dsl '{"match":{"notes":"blood glucose"}}'` returns documents matching tokenized/stemmed text search
3. `searchdb search myindex --dsl '{"bool":{"must":[{"term":{"status":"active"}},{"range":{"age":{"gte":18}}}]}}'` returns documents matching compound boolean query
4. `searchdb search myindex --dsl '{"terms":{"status":["active","pending"]}}'` returns documents where status is "active" OR "pending"
5. `searchdb search myindex --dsl '{"match_phrase":{"notes":"blood glucose level"}}'` returns documents with exact phrase
6. `searchdb search myindex --dsl '{"range":{"created_at":{"gte":"2024-01-01T00:00:00Z","lt":"2025-01-01T00:00:00Z"}}}'` returns documents in date range
7. `searchdb search myindex --dsl '{"exists":{"field":"status"}}'` returns documents where "status" is not null
8. `searchdb search myindex --dsl '{"match_all":{}}'` returns all documents
9. Stdin input works: `echo '{"match":{"notes":"test"}}' | searchdb search myindex --dsl -`
10. File input works: `searchdb search myindex --dsl @query.json`
11. `-q` and `--dsl` are mutually exclusive -- using both is a CLI error
12. `--limit`, `--offset`, `--fields`, `--score` work with `--dsl`
13. DSL search works with gap rows (two-tier search)

### Error Handling

14. Unknown query type returns a clear error: `unsupported query type: "fuzzy"`
15. Unknown field in a query returns a clear error: `field "nonexistent" not found in schema`
16. Type mismatch (range query on text field) returns a clear error
17. Malformed JSON returns a serde parse error with location info
18. Unknown parameters within a query type are rejected (serde `deny_unknown_fields`)

### Non-Functional

19. `cargo clippy -- -D warnings` passes
20. `cargo fmt` passes
21. `cargo test` passes -- all existing tests still pass, new tests for each query type
22. No new `unwrap()` in non-test code
23. Binary size increase is under 100KB (vendored code, no heavy new deps)

## Out of Scope

- ES search body wrapper (`{"query": {...}, "size": 10, "from": 0, "_source": [...]}`) -- use CLI flags
- Aggregations -- separate feature, much larger scope
- Highlighting -- tantivy has limited support, not in v1
- Sorting beyond relevance score -- future work
- Scroll/search_after pagination -- future work
- Index-time analyzers or custom tokenizers -- future work
- The `format` parameter on range queries for Java date formats -- v2

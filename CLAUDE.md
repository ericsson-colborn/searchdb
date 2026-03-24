# SearchDB — Embedded Search for the Lakehouse

## Vision

SearchDB is what happens when Delta Lake, DuckDB, and Tantivy have a baby. It combines the philosophy of DuckDB (embedded, zero-config, single binary, just works), the architecture of Delta Lake (append-only, versioned, tiered compaction), and the power of Tantivy (full-text search with BM25 scoring).

**The pitch:** Point it at JSON or blob storage and you're searching in minutes. No cluster, no daemon, no configuration. The index is a disposable cache that builds itself lazily from Delta Lake. Zero to search in under 5 minutes.

**The story:** Elasticsearch is powerful but operationally heavy — clusters, JVM tuning, replication, split brain. For teams that already have data in a lakehouse (Delta Lake on S3/GCS/Azure), running a search cluster alongside it is redundant infrastructure. SearchDB lets you search your lake directly: the lake is the source of truth, the index is a cache, and compaction happens in the background.

## Architecture

### The Two Tiers

SearchDB uses Delta Lake as the buffer and tantivy as the index. Un-indexed data lives in Delta (Parquet files); indexed data lives in tantivy segments. Queries search both tiers and merge results.

```
Query arrives:
  1. Search tantivy index (fast, covers data up to index_version)
  2. Read Delta gap: rows_added_since(index_version) → Parquet files
  3. Build ephemeral in-memory tantivy index from gap rows
  4. Merge results, dedup by _id (gap wins — newer data)
  5. Return results
  6. If gap > threshold → fire-and-forget background sync
```

No write lock during queries. Concurrent queries never block each other. Background promotion creates new tantivy segments — never rebuilds the full index.

### Tiered Segment Compaction (target architecture)

Instead of all-or-nothing index rebuilds, leverage tantivy's native segment model:

```
Small gap arrives    →  Create a new tantivy segment (fast, lightweight)
Segments accumulate  →  Merge small segments periodically (tiered merge policy)
Never rebuild        →  Always additive, compaction is just segment merging
```

This mirrors how Delta Lake handles compaction with OPTIMIZE — small files accumulate, periodic compaction merges them, but data is always queryable at every stage.

### Quickwit-Derived Components (planned)

Surgical borrows from [quickwit](https://github.com/quickwit-oss/quickwit) (~2K lines from a 10K+ codebase):

1. **ES DSL → Tantivy query compilation** — Accept Elasticsearch-compatible query DSL, compile to tantivy queries. Enables drop-in ES replacement for read paths.
2. **Object storage directory** — Tantivy `Directory` trait over S3/GCS/Azure. Search directly from blob storage without local copies.
3. **Tiered merge policy** — Smarter segment merging than tantivy's default LogMergePolicy. Compaction workers that run on a schedule rather than inline.

### Design Principles

1. **Serverless.** No daemon. Every operation is a CLI invocation. Background promotion is a fire-and-forget child process.
2. **Index-on-the-fly.** Users don't think about indexing. Queries always return fresh results.
3. **Delta is the buffer.** No intermediate files. The gap between `index_version` and Delta HEAD is the un-indexed tier.
4. **Dumb, reliable architecture.** Dedup at read time. Duplicate work over coordination.
5. **Single binary, zero deps.** `cargo build --release` produces one binary. No Python, no pip, no runtime.

## Commands

```
searchdb new <name> --schema <json>       # Create index from schema declaration
searchdb index <name> [-f file.ndjson]    # Bulk index NDJSON (stdin or file)
searchdb search <name> -q <query>         # Search (reads Delta gap + index)
searchdb serve <name> [--port 3000]       # HTTP search server (planned)
searchdb connect-delta <name> --source <uri> --schema <json>  # Attach Delta source + initial load
searchdb sync <name>                      # Promote gap into index (exits silently if locked)
searchdb reindex <name> [--as-of-version N]  # Full rebuild from Delta
searchdb get <name> <doc_id>              # Single doc by _id (gap-first lookup)
searchdb stats <name>                     # Index stats + gap size
searchdb drop <name>                      # Delete index
```

## Schema

4 field types map to tantivy config:
- `keyword` → text field, `raw` tokenizer, stored (exact match)
- `text` → text field, `en_stem` tokenizer, stored (full-text, stemmed)
- `numeric` → f64 field, indexed + fast + stored (range queries)
- `date` → date field, indexed + fast + stored (range queries)

### Internal Fields (not user-visible in schema)

- `_id` — text/raw/stored. Document identity for upsert and dedup.
- `_source` — text/raw/stored. Verbatim JSON of original doc for round-trip.
- `__present__` — text/raw/NOT stored. Tokens: `__all__` (every doc) + field names for non-null fields. Enables null/not-null queries.

## On-Disk Layout

```
{data_dir}/
├── {index_name}/
│   ├── searchdb.json    ← Schema + Delta source + index_version watermark
│   └── index/           ← Tantivy segment files
```

## Development

```bash
cargo build                     # Debug build
cargo build --release           # Release build
cargo test                      # Run all tests (38 currently)
cargo clippy -- -D warnings     # Lint (must pass)
cargo fmt                       # Format
```

### Key Crates

- `tantivy 0.25` — search engine (IndexWriter, Searcher, Schema, RamDirectory)
- `deltalake` — Delta Lake table access (async, needs tokio)
- `clap 4` — CLI argument parsing (derive macros)
- `axum` — HTTP server (planned, for `serve` command)
- `serde/serde_json` — JSON serialization
- `anyhow/thiserror` — error handling
- `arrow/parquet` — reading Delta's Parquet files

### Async Bridging

delta-rs is async (tokio). tantivy is sync. Use `#[tokio::main]` on the binary entry point. Delta-facing code is async; search/index operations are sync. The ephemeral in-memory index (RamDirectory) is built synchronously from gap rows.

## Query Syntax

Tantivy's native query parser (Lucene-like):
- `+field:"value"` — must match (keyword exact)
- `field:term` — should match (text, stemmed)
- `-field:"value"` — must not match
- `field:[100 TO 200]` — range inclusive
- `field:{100 TO *}` — range exclusive lower, open upper
- `+__present__:__all__` — anchor for null-only queries

**Planned:** Elasticsearch query DSL support via quickwit-query compilation layer.

## Coding Standards

- `cargo fmt` before every commit
- `cargo clippy -- -D warnings` must pass
- `cargo test` must pass
- Use `anyhow::Result` for application errors, `thiserror` for library error types
- Prefer `&str` over `String` in function parameters where possible
- No `unwrap()` in non-test code — use `?` or explicit error handling
- Keep functions under 50 lines
- One module per file, one concern per module

## Current State

MVP complete with Delta-as-buffer architecture:
- 9 CLI commands implemented
- 38 passing tests
- Two-tier search (persistent index + ephemeral gap index)
- Fire-and-forget background promotion via `searchdb sync`
- Zero write contention during queries

### Specs and Plans

- `docs/superpowers/specs/2026-03-24-buffer-promote-architecture-design.md` — Delta-as-buffer architecture
- `docs/superpowers/plans/2026-03-24-delta-as-buffer.md` — Implementation plan (completed)

### Next Steps

1. **Tiered segment compaction** — Replace all-or-nothing sync with segment-per-batch. Scheduled merges instead of inline rebuilds.
2. **Quickwit ES DSL layer** — Accept Elasticsearch query DSL, compile to tantivy queries.
3. **Object storage directory** — Tantivy Directory over S3/GCS/Azure from quickwit.
4. **`searchdb serve`** — HTTP server with long-lived IndexWriter and timer-based promotion.
5. **Delta delete detection** — Warn or handle rows deleted from Delta (currently silently ignored).

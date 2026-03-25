# deltasearch — Embedded Search for the Lakehouse

## Vision

deltasearch is what happens when Delta Lake, DuckDB, and Tantivy have a baby. It combines the philosophy of DuckDB (embedded, zero-config, single binary, just works), the architecture of Delta Lake (append-only, versioned, tiered compaction), and the power of Tantivy (full-text search with BM25 scoring).

**The pitch:** Point it at JSON or blob storage and you're searching in minutes. No cluster, no daemon, no configuration. The index is a disposable cache that builds itself from Delta Lake. Zero to search in under 5 minutes.

**The story:** Elasticsearch is powerful but operationally heavy — clusters, JVM tuning, replication, split brain. For teams that already have data in a lakehouse (Delta Lake on S3/GCS/Azure), running a search cluster alongside it is redundant infrastructure. deltasearch lets you search your lake directly: the lake is the source of truth, the index is a cache, and compaction happens in the background.

## Architecture

### Client/Worker Split

Like DuckDB, there is **no HTTP server**. Clients and workers are both CLI commands that interface directly with the underlying index files and storage. This is a fundamental departure from Elasticsearch's architecture.

```
Client (read-only, no credentials needed):
  dsrch search <name> -q <query>    → reads tantivy index directly
  dsrch get <name> <doc_id>         → reads tantivy index directly
  dsrch stats <name>                → reads index metadata

Worker (writes, requires credentials):
  dsrch compact <name>              → reads Delta, creates segments, merges
```

**Clients never write.** They read the tantivy index on disk. No Delta access, no credentials.

**Workers handle all writes.** The compaction worker reads Delta, creates tantivy segments, and merges them. This retains separation of concerns: don't trust the client. Workers need credentials for blob storage access.

### Tiered Segment Compaction

Two levels of compaction, both configurable:

```
Level 1 — Segmentation (after n entries):
  New Delta rows arrive → Worker creates a new tantivy segment
  Fast, lightweight. Never rebuilds the full index.

Level 2 — Merge (after m minutes):
  Accumulated segments → Worker merges into consolidated index
  Keeps total segment count bounded. Runs on a schedule.
```

Both n and m are configurable. All compaction is **atomic** — if the worker crashes mid-compaction, the index is unchanged. This mirrors Delta Lake's OPTIMIZE and LSM-tree compaction strategies.

### Quickwit-Derived Components (planned)

Surgical borrows from [quickwit](https://github.com/quickwit-oss/quickwit) (~2K lines from a 10K+ codebase):

1. **ES DSL → Tantivy query compilation** — Accept Elasticsearch-compatible query DSL, compile to tantivy queries. Enables drop-in ES replacement for read paths.
2. **Object storage directory** — Tantivy `Directory` trait over S3/GCS/Azure. Search directly from blob storage without local copies.
3. **Tiered merge policy** — Smarter segment merging than tantivy's default LogMergePolicy.

### Design Principles

1. **Serverless.** No daemon, no HTTP server. CLI commands against shared storage.
2. **Client/worker separation.** Clients read. Workers write. Workers need credentials.
3. **Tiered compaction.** New entries → segment. Accumulated segments → merge. Never full rebuild.
4. **Schema is optional.** Infer types dynamically from data (like ES dynamic mapping). Strings → keyword, numbers → numeric, ISO timestamps → date.
5. **Atomic operations.** All compaction is crash-safe. Index is always in a valid state.
6. **Single binary, zero deps.** `cargo build --release` produces one binary.

## Commands

```
dsrch new <name> [--schema <json>]     # Create index (schema optional — infer from data)
dsrch index <name> [-f file.ndjson]    # Bulk index NDJSON (stdin or file)
dsrch search <name> -q <query>         # Search (reads index directly)
dsrch get <name> <doc_id>              # Single doc by _id
dsrch stats <name>                     # Index stats + segment count
dsrch drop <name>                      # Delete index
dsrch connect-delta <name> --source <uri> [--schema <json>]  # Attach Delta source
dsrch compact <name> [--interval <m>]  # Run compaction worker (segment creation + merge)
dsrch reindex <name> [--as-of-version N]  # Full rebuild from Delta (escape hatch)
```

## Schema

### Explicit Schema (optional)

4 field types map to tantivy config:
- `keyword` → text field, `raw` tokenizer, stored (exact match)
- `text` → text field, `en_stem` tokenizer, stored (full-text, stemmed)
- `numeric` → f64 field, indexed + fast + stored (range queries)
- `date` → date field, indexed + fast + stored (range queries)

### Dynamic Schema (default when no schema specified)

Infer types from data, like Elasticsearch's dynamic mapping:
- JSON string → `keyword` (exact match by default)
- JSON number → `numeric`
- ISO 8601 string (regex match) → `date`
- Explicit opt-in for `text` (full-text) via schema override

### Internal Fields (not user-visible in schema)

- `_id` — text/raw/stored. Document identity for upsert and dedup.
- `_source` — text/raw/stored. Verbatim JSON of original doc for round-trip.
- `__present__` — text/raw/NOT stored. Tokens: `__all__` (every doc) + field names for non-null fields. Enables null/not-null queries.

## On-Disk Layout

```
{data_dir}/
├── {index_name}/
│   ├── searchdb.json    ← Schema + Delta source + index_version watermark (legacy filename)
│   └── index/           ← Tantivy segment files
```

## Development

```bash
cargo build                     # Debug build
cargo build --release           # Release build
cargo test                      # Run all tests
cargo clippy -- -D warnings     # Lint (must pass)
cargo fmt                       # Format
```

### Key Crates

- `tantivy 0.25` — search engine (IndexWriter, Searcher, Schema, RamDirectory)
- `deltalake` — Delta Lake table access (async, needs tokio)
- `clap 4` — CLI argument parsing (derive macros)
- `serde/serde_json` — JSON serialization
- `anyhow/thiserror` — error handling
- `arrow/parquet` — reading Delta's Parquet files

### Async Bridging

delta-rs is async (tokio). tantivy is sync. Use `#[tokio::main]` on the binary entry point. Delta-facing code is async; search/index operations are sync.

## Query Syntax

### Tantivy Native (current)

Lucene-like query parser:
- `+field:"value"` — must match (keyword exact)
- `field:term` — should match (text, stemmed)
- `-field:"value"` — must not match
- `field:[100 TO 200]` — range inclusive
- `field:{100 TO *}` — range exclusive lower, open upper
- `+__present__:__all__` — anchor for null-only queries

### Elasticsearch DSL (planned)

Accept Elasticsearch query JSON body via `--dsl` flag or stdin. Compile to tantivy queries via quickwit-query.

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

MVP complete with two-tier search prototype. Transitioning to client/worker architecture.

### What's Built
- 9 CLI commands, 38 passing tests
- Two-tier search (persistent index + ephemeral gap index) — prototype, being replaced by worker model
- Delta Lake sync with file-level diffing
- Schema declaration, 4 field types, 3 internal fields
- CI/CD GitHub Actions workflows
- Fire-and-forget background sync

### Active Backlog
1. ES DSL — Elasticsearch query DSL → tantivy compilation
2. Segmentation — n entries → new segment (configurable, no full rebuild)
3. Compaction — `dsrch compact` worker with configurable interval
4. Access model — clients read-only, workers own writes
5. Schema inference — optional schema, infer types dynamically
6. Failure modes — all compaction atomic, crash-safe

### Specs and Plans
- `docs/superpowers/specs/2026-03-24-buffer-promote-architecture-design.md` — Delta-as-buffer architecture (v1, being superseded by worker model)
- `docs/superpowers/plans/2026-03-24-delta-as-buffer.md` — Implementation plan (completed)

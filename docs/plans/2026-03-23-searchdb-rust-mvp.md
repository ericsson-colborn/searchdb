# deltasearch Rust MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a single-binary Rust CLI that provides tantivy search with Delta Lake sync — "ES in your pocket."

**Architecture:** Fork tantivy-cli's patterns (not literally — rewrite from scratch using the same tantivy APIs). CLI with clap derive, modular command files, tantivy for indexing/search, delta-rs for sync.

**Tech Stack:** Rust, tantivy 0.25, deltalake crate, clap 4, axum (HTTP), serde_json, tokio (async for delta-rs)

**Reference:** Python prototype at `../searchdb/` — port the logic, not the patterns.

---

## File Structure

```
src/
├── main.rs              # CLI entry point, clap App definition
├── schema.rs            # Schema type, FieldType enum, tantivy schema builder
├── writer.rs            # Document construction, _source/_id/__present__, upsert
├── searcher.rs          # Query string building, search execution, result formatting
├── storage.rs           # On-disk layout, config persistence (searchdb.json)
├── delta.rs             # DeltaSync: version check, file diffing, Parquet reading
├── error.rs             # Error types (thiserror)
├── commands/
│   ├── mod.rs           # Module exports
│   ├── new_index.rs     # `dsrch new` — create index from schema JSON
│   ├── index.rs         # `dsrch index` — bulk NDJSON indexing
│   ├── search.rs        # `dsrch search` — query + output results
│   ├── get.rs           # `dsrch get` — single doc by _id
│   ├── connect_delta.rs # `dsrch connect-delta` — attach Delta source
│   ├── sync.rs          # `dsrch sync` — manual incremental sync
│   ├── reindex.rs       # `dsrch reindex` — full rebuild from Delta
│   ├── stats.rs         # `dsrch stats` — index statistics
│   ├── drop.rs          # `dsrch drop` — delete index
│   └── serve.rs         # `dsrch serve` — HTTP search server (Phase 2)
```

---

## Task 1: CLI Skeleton + Schema Module

**Files:**
- Create: `src/main.rs`
- Create: `src/schema.rs`
- Create: `src/error.rs`
- Create: `src/commands/mod.rs`
- Create: `src/commands/new_index.rs`

- [ ] **Step 1: Define error types in `src/error.rs`**

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SearchDbError {
    #[error("Index '{0}' not found")]
    IndexNotFound(String),
    #[error("Index '{0}' already exists")]
    IndexExists(String),
    #[error("Schema error: {0}")]
    Schema(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Tantivy error: {0}")]
    Tantivy(#[from] tantivy::TantivyError),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}
```

- [ ] **Step 2: Define Schema types in `src/schema.rs`**

```rust
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    Keyword,
    Text,
    Numeric,
    Date,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub fields: HashMap<String, FieldType>,
}

impl Schema {
    pub fn build_tantivy_schema(&self) -> tantivy::schema::Schema {
        // System fields: _id, _source, __present__
        // User fields mapped by FieldType
        // Return built schema
    }
}
```

- [ ] **Step 3: Define CLI with clap in `src/main.rs`**

```rust
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "dsrch", about = "ES in your pocket")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    /// Data directory for indexes
    #[arg(long, default_value = ".dsrch")]
    data_dir: String,
}

#[derive(Subcommand)]
enum Commands {
    New { name: String, #[arg(long)] schema: String },
    // ... other commands added in later tasks
}
```

- [ ] **Step 4: Implement `dsrch new` — creates index dir + schema**

- [ ] **Step 5: Verify it compiles and runs**

```bash
cargo build
./target/debug/dsrch new test_index --schema '{"fields":{"name":"keyword","notes":"text"}}'
```

- [ ] **Step 6: Commit**

---

## Task 2: Writer Module — Document Construction + Indexing

**Files:**
- Create: `src/writer.rs`
- Create: `src/commands/index.rs`
- Modify: `src/main.rs` — add `Index` subcommand

- [ ] **Step 1: Implement document construction in `src/writer.rs`**

Build tantivy `Document` from JSON object:
- Add `_id` (from doc or generate UUID)
- Add `_source` (json.dumps equivalent)
- Add `__present__` tokens: `__all__` + each non-null field name
- Type-dispatch: keyword→add_text(raw), text→add_text, numeric→add_f64, date→add_date

- [ ] **Step 2: Implement upsert (delete_term + add_document)**

```rust
pub fn upsert_document(writer: &IndexWriter, id_field: Field, doc: Document, doc_id: &str) {
    let term = Term::from_field_text(id_field, doc_id);
    writer.delete_term(term);
    writer.add_document(doc).unwrap();
}
```

- [ ] **Step 3: Implement `dsrch index` — reads NDJSON, indexes documents**

Read from stdin or file, parse each line as JSON, construct tantivy doc, add to writer, commit.

- [ ] **Step 4: Test — create index, pipe NDJSON, verify files exist**

```bash
echo '{"_id":"d1","name":"glucose","notes":"fasting sample"}' | ./target/debug/dsrch index test_index
```

- [ ] **Step 5: Write Rust tests**

```rust
#[cfg(test)]
mod tests {
    #[test]
    fn test_document_construction() { ... }
    #[test]
    fn test_upsert_replaces_document() { ... }
}
```

- [ ] **Step 6: Commit**

---

## Task 3: Search Module — Query Building + Execution

**Files:**
- Create: `src/searcher.rs`
- Create: `src/commands/search.rs`
- Create: `src/commands/get.rs`
- Modify: `src/main.rs` — add `Search` and `Get` subcommands

- [ ] **Step 1: Implement search execution in `src/searcher.rs`**

- Parse query string with tantivy's QueryParser
- Execute search with TopDocs collector
- Extract `_source` from results, parse back to JSON
- Apply field projection if `--fields` specified
- Include `_id` and optionally `_score` in output

- [ ] **Step 2: Implement `dsrch search` command**

```bash
dsrch search test_index -q '+name:"glucose"' --limit 20 --offset 0 --output json
```

Output: NDJSON to stdout (one JSON object per line).

- [ ] **Step 3: Implement `dsrch get` command**

```bash
dsrch get test_index d1 --output json
```

Search by `_id`, return single doc or exit code 1 if not found.

- [ ] **Step 4: Write integration tests**

```rust
#[test]
fn test_keyword_exact_match() { ... }
#[test]
fn test_text_stemmed_search() { ... }
#[test]
fn test_get_by_id() { ... }
#[test]
fn test_get_missing_returns_none() { ... }
```

- [ ] **Step 5: Commit**

---

## Task 4: Storage Module + Stats/Drop Commands

**Files:**
- Create: `src/storage.rs`
- Create: `src/commands/stats.rs`
- Create: `src/commands/drop.rs`

- [ ] **Step 1: Implement `src/storage.rs`**

Manages on-disk layout:
- `searchdb.json` — schema + Delta source + version watermark (replaces Python's separate schema.json + meta.json)
- `index/` — tantivy segment directory
- `list_indexes()`, `exists()`, `drop()`, `load_config()`, `save_config()`

- [ ] **Step 2: Implement `dsrch stats`**

```bash
dsrch stats test_index --output json
# {"index":"test_index","num_docs":42,"num_segments":3,"fields":{"name":"keyword","notes":"text"}}
```

- [ ] **Step 3: Implement `dsrch drop`**

```bash
dsrch drop test_index
# [searchdb] Dropped 'test_index'
```

- [ ] **Step 4: Tests + commit**

---

## Task 5: Delta Sync Module

**Files:**
- Create: `src/delta.rs`
- Create: `src/commands/connect_delta.rs`
- Create: `src/commands/sync.rs`
- Create: `src/commands/reindex.rs`

- [ ] **Step 1: Implement `src/delta.rs` — DeltaSync struct**

```rust
pub struct DeltaSync {
    source: String,
}

impl DeltaSync {
    pub async fn current_version(&self) -> Result<i64>;
    pub async fn full_load(&self, as_of_version: Option<i64>) -> Result<Vec<serde_json::Value>>;
    pub async fn rows_added_since(&self, last_version: i64) -> Result<Vec<serde_json::Value>>;
}
```

Port the file-level diffing algorithm from Python's `delta.py`:
- `current.file_uris() - previous.file_uris()` → read only new Parquet files
- Convert Arrow RecordBatches to JSON values

- [ ] **Step 2: Implement `dsrch connect-delta`**

```bash
dsrch connect-delta labs --source /path/to/delta --schema '{"fields":{"name":"keyword"}}'
```

Creates index, performs full load, saves Delta source + version to `searchdb.json`.

- [ ] **Step 3: Implement `dsrch sync`**

Manual trigger for incremental sync. Reads meta, diffs versions, indexes new rows.

- [ ] **Step 4: Implement `dsrch reindex`**

```bash
dsrch reindex labs [--as-of-version 5]
```

Drop + rebuild from Delta source.

- [ ] **Step 5: Add auto-sync to `dsrch search` and `dsrch get`**

Before executing search, check if Delta source is configured and if version has advanced. If so, sync inline (small delta) or warn (large delta).

- [ ] **Step 6: Integration tests with local Delta tables**

Create Delta tables with `deltalake` crate in test setup, verify connect/sync/search flow.

- [ ] **Step 7: Commit**

---

## Task 6: Agent-Friendly CLI Polish

**Files:**
- Modify: all command files for consistent output formatting

- [ ] **Step 1: `--output` flag (json | text)**

JSON mode: NDJSON to stdout, status messages to stderr.
Text mode: human-readable table format.
Default: JSON when not a TTY, text when TTY.

- [ ] **Step 2: Structured error output**

```json
{"error": "index_not_found", "index": "labs", "available": ["patients"]}
```

- [ ] **Step 3: Exit codes**

0 = success, 1 = error, 2 = usage error, 3 = not found

- [ ] **Step 4: `--fields` flag on search/get**

Limit returned fields (protects agent context window).

- [ ] **Step 5: Commit**

---

## Task 7: CI + Distribution

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `.github/workflows/release.yml`

- [ ] **Step 1: CI workflow — test + clippy + fmt on every push**
- [ ] **Step 2: Release workflow — cross-compile for 4 targets on tag push**

Targets: x86_64-linux, aarch64-linux, x86_64-macos, aarch64-macos

- [ ] **Step 3: Test release locally**

```bash
cargo build --release
ls -la target/release/dsrch
```

- [ ] **Step 4: Commit**

---

## Verification

```bash
# Build
cargo build

# Lint
cargo clippy -- -D warnings

# Format
cargo fmt --check

# Test
cargo test

# End-to-end smoke test
echo '{"_id":"1","name":"glucose","notes":"fasting sample"}
{"_id":"2","name":"a1c","notes":"borderline diabetic"}' | ./target/debug/dsrch index labs --schema '{"fields":{"name":"keyword","notes":"text"}}'

./target/debug/dsrch search labs -q 'diabetes' --output json
# Should return doc 2 (stemmed match: diabetic ↔ diabetes)

./target/debug/dsrch get labs 1 --output json
# Should return doc 1

./target/debug/dsrch stats labs --output json
# Should show num_docs: 2
```

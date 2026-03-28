# Dewey Rebrand + CLI Overhaul Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebrand from deltasearch/dsrch to Dewey, overhaul CLI ergonomics with positional args, short flags, and renamed commands.

**Architecture:** Rename the binary from `dsrch` to `dewey`, rename `compact` to `librarian`, rename `new` to `create`, rename `ingest` to `to-delta`, add positional search term with multi-match, add short flags (-l, -f, -s), separate `create` (metadata) from `librarian` (data movement). All internal module names stay the same — this is a user-facing rename only.

**Tech Stack:** clap 4 (derive), existing tantivy/deltalake stack unchanged.

---

## CLI Spec (Final)

```bash
# Index lifecycle
dewey create <name> --source <uri> [--schema JSON] [--dry-run]
dewey create <name> --schema JSON                        # local-only, no Delta
dewey drop <name>
dewey stats <name>
dewey reindex <name>

# Search
dewey search <name> [QUERY] [-f FILTER] [-l LIMIT] [-s SORT] [--dsl JSON] [--agg JSON]
dewey get <name> <id> [--fields f1,f2]

# Compaction (librarian)
dewey librarian <name> [--once] [--force-merge]
dewey librarian <name> [--poll-interval N] [--segment-size N] [--max-segments N]
dewey librarian <name> [--metrics-port N]

# Data conversion
dewey to-delta <source> <target> [--format ndjson|csv|parquet] [--mode overwrite|append]

# Global flags
--dir, -d (env: DEWEY_DATA_DIR, default: .dewey)
--output json|text
```

## Key Behavioral Changes

1. **`create` is metadata only** — saves schema + delta_source to config, creates empty tantivy index. No data loading.
2. **`librarian` does all data movement** — on first run, detects empty index (index_version is None), does full load from Delta, then starts polling.
3. **Search positional QUERY** — `dewey search labs "glucose"` runs a multi-match across all text/keyword fields. Optional — omitting it with `-f` only uses the filter.
4. **`-f` filter** — tantivy query string syntax (e.g., `-f '+test_name:glucose +facility:facility-3'`). Composes with positional QUERY (both must match).
5. **`to-delta`** — positional source and target. Format auto-detected from extension.

---

### Task 1: Rename binary and package

**Files:**
- Modify: `Cargo.toml`

- [ ] **Step 1: Update Cargo.toml**

Change the binary name and package name:

```toml
[package]
name = "dewey"

[[bin]]
name = "dewey"
path = "src/main.rs"
```

- [ ] **Step 2: Verify it builds**

Run: `cargo build`
Expected: produces `target/debug/dewey`

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "refactor: rename binary from dsrch to dewey"
```

---

### Task 2: Rename CLI strings and env var

**Files:**
- Modify: `src/main.rs`

- [ ] **Step 1: Update clap command name and about**

Change the `#[command]` attribute:

```rust
#[command(
    name = "dewey",
    about = "Dewey — search your lakehouse. Embedded search backed by tantivy + Delta Lake.",
    version
)]
```

- [ ] **Step 2: Rename data_dir default and env var**

Change:
```rust
    #[arg(long, short = 'd', global = true, default_value = ".dewey", env = "DEWEY_DATA_DIR")]
    data_dir: String,
```

- [ ] **Step 3: Update all eprintln prefixes from `[dsrch]` to `[dewey]`**

Search and replace `[dsrch]` with `[dewey]` across all source files:
- `src/main.rs`
- `src/compact.rs`
- `src/delta.rs`
- `src/commands/*.rs`
- `src/pipeline.rs`

Use `grep -rn "\[dsrch\]" src/` to find all occurrences, then replace each one.

- [ ] **Step 4: Build and test**

Run: `cargo build && cargo test`

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: rebrand user-facing strings from dsrch to dewey"
```

---

### Task 3: Rename `Compact` command to `Librarian`

**Files:**
- Modify: `src/main.rs` (Commands enum + dispatch)
- Rename: `src/commands/compact.rs` → keep file name but rename the CLI command

- [ ] **Step 1: Rename the Commands enum variant**

In `src/main.rs`, change:
```rust
    /// Run the librarian — keeps the index fresh from Delta Lake
    #[cfg(feature = "delta")]
    Librarian {
```
(was `Compact`)

Update all fields to match current `Compact` fields. Keep all the same flags except remove `--source` and `--schema` (these belong to `create` now).

- [ ] **Step 2: Update the dispatch match arm**

Change `Commands::Compact { ... }` to `Commands::Librarian { ... }` in `run_cli()`.

- [ ] **Step 3: Remove --source and --schema from Librarian**

The librarian reads `delta_source` from the index config (set by `create`). Remove these fields from the Librarian variant. Update `commands/compact.rs` `run()` to remove `source` and `schema` parameters — instead, error if the index has no `delta_source` in its config.

- [ ] **Step 4: Update compact.rs error messages**

Change any "use compact --source" messages to "use dewey create --source first".

- [ ] **Step 5: Build and test**

Run: `cargo build && cargo test`

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor: rename compact command to librarian"
```

---

### Task 4: Rename `New` command to `Create`, add --source

**Files:**
- Modify: `src/main.rs` (Commands enum)
- Modify: `src/commands/new_index.rs`

- [ ] **Step 1: Rename New to Create in Commands enum**

```rust
    /// Create a new index (schema inferred from Delta source or provided explicitly)
    Create {
        /// Index name
        name: String,
        /// Delta Lake source URI (optional — omit for local-only index)
        #[arg(long)]
        source: Option<String>,
        /// Schema JSON: {"fields":{"name":"keyword","notes":"text"}}
        #[arg(long)]
        schema: Option<String>,
        /// Overwrite if index already exists
        #[arg(long, default_value_t = false)]
        overwrite: bool,
        /// Print inferred schema as JSON and exit without creating
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
```

Remove `--infer-from` (replaced by `--source` which infers from Arrow schema).

- [ ] **Step 2: Update dispatch**

Change `Commands::New { ... }` to `Commands::Create { ... }`.

- [ ] **Step 3: Update new_index.rs to handle --source**

When `--source` is provided and `--schema` is not:
1. Open the Delta table
2. Infer schema from Arrow schema (same as current `connect_delta::run`)
3. Save `delta_source` in the config
4. Do NOT load data (that's the librarian's job)

When neither `--source` nor `--schema`:
Error: "either --source or --schema is required"

- [ ] **Step 4: Build and test**

Run: `cargo build && cargo test`

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: rename new to create, add --source for Delta schema inference"
```

---

### Task 5: Rename `Ingest` to `ToDelta` with positional args

**Files:**
- Modify: `src/main.rs` (Commands enum + dispatch)

- [ ] **Step 1: Rename Ingest to ToDelta**

```rust
    /// Convert raw files (NDJSON, JSON, CSV, Parquet) into a Delta Lake table
    #[cfg(feature = "delta")]
    ToDelta {
        /// Source file(s) — glob pattern, single path, or - for stdin
        source: String,
        /// Delta Lake table destination (path or URI)
        target: String,
        /// File format (auto-detected from extension if omitted)
        #[arg(long)]
        format: Option<String>,
        /// Write mode: overwrite (default) or append
        #[arg(long, default_value = "overwrite")]
        mode: String,
        /// Rows per read batch
        #[arg(long, default_value_t = 10_000)]
        batch_size: usize,
    },
```

Note: `source` and `target` are positional (no `#[arg(long)]`).

- [ ] **Step 2: Update dispatch**

Map `Commands::ToDelta { source, target, ... }` to `commands::ingest::run(Some(&source), &target, ...)`. Handle `source == "-"` as stdin (pass `None`).

- [ ] **Step 3: Build and test**

Run: `cargo build && cargo test`

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor: rename ingest to to-delta with positional args"
```

---

### Task 6: Search positional QUERY + short flags

**Files:**
- Modify: `src/main.rs` (Search variant)
- Modify: `src/commands/search.rs`
- Modify: `src/searcher.rs`

- [ ] **Step 1: Update Search command with positional query and short flags**

```rust
    /// Search an index
    Search {
        /// Index name
        name: String,
        /// Search query (multi-match across all fields)
        query: Option<String>,
        /// Tantivy filter syntax (e.g., '+field:value -field:value')
        #[arg(short = 'f', long = "filter")]
        filter: Option<String>,
        /// Elasticsearch query DSL (JSON string, @file, or - for stdin)
        #[arg(long)]
        dsl: Option<String>,
        /// Maximum number of results
        #[arg(short = 'l', long, default_value_t = 20)]
        limit: usize,
        /// Skip first N results
        #[arg(long, default_value_t = 0)]
        offset: usize,
        /// Comma-separated list of fields to return
        #[arg(long, value_delimiter = ',')]
        fields: Option<Vec<String>>,
        /// Sort by field (e.g., "views", "views:asc", "published:desc")
        #[arg(short = 's', long)]
        sort: Option<String>,
        /// Include relevance score in output
        #[arg(long, default_value_t = false)]
        score: bool,
        /// Aggregation request JSON (ES-compatible)
        #[arg(long)]
        agg: Option<String>,
    },
```

Note: `query` is positional (no `#[arg(long)]`), optional. The old `-q`/`--query` flag is removed.

- [ ] **Step 2: Implement multi-match for positional query**

When `query` is provided (positional search term like `"glucose"`):
- Build a multi-match query across all text and keyword fields in the schema
- Use tantivy's QueryParser with all schema fields as default fields
- This becomes the "base query"

When `filter` is provided (`-f '+test_name:glucose'`):
- Parse as a tantivy query string (existing behavior, was `-q`)
- This becomes an additional filter

When both are provided:
- Combine with BooleanQuery: MUST(multi_match) + MUST(filter)

When neither is provided and no `--dsl`:
- Error: "provide a search query, --filter, or --dsl"

- [ ] **Step 3: Update commands/search.rs**

Rename `query` parameter to handle the new semantics. The old `-q` query string path becomes the `-f` filter path. The new positional `query` uses multi-match.

- [ ] **Step 4: Add test for multi-match**

Test that `dewey search labs "glucose"` finds documents where "glucose" appears in any field.

- [ ] **Step 5: Build and test**

Run: `cargo build && cargo test`

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: positional search query with multi-match, short flags (-l, -f, -s)"
```

---

### Task 7: Update librarian to handle first-run full load

**Files:**
- Modify: `src/commands/compact.rs`

- [ ] **Step 1: Remove auto-create logic from compact run()**

Remove the block that calls `connect_delta::run()` when the index doesn't exist. Instead, error with "index not found — use dewey create first."

- [ ] **Step 2: Add first-run detection in CompactWorker**

In `compact.rs` `poll_and_segment`, when `index_version` is `None` (never synced before), the existing code calls `changes_since(-1)` which triggers a full load. This already works — the librarian's first poll naturally does a full load because `index_version` starts as `None`.

Verify this works by testing: `dewey create labs --source ...` then `dewey librarian labs --once`.

- [ ] **Step 3: Remove connect_delta command entirely**

Delete `src/commands/connect_delta.rs` and remove from `src/commands/mod.rs`. Move the schema inference logic (Arrow → Schema) into `commands/new_index.rs` (the `create` command) since that's where `--source` now lives.

- [ ] **Step 4: Build and test**

Run: `cargo build && cargo test`

Note: Tests that used `connect_delta::run()` as a helper need to be updated to use the new `create` + `librarian --once` flow, or call the internal functions directly.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: librarian handles first-run full load, remove connect-delta"
```

---

### Task 8: Update docs and README

**Files:**
- Modify: `CLAUDE.md`
- Modify: `README.md`
- Modify: `docs/product-requirements.md`
- Modify: `docs/business-model.md`

- [ ] **Step 1: Update all docs**

Replace all references to:
- `deltasearch` → `Dewey` (product name)
- `dsrch` → `dewey` (binary name)
- `compact` → `librarian` (command name)
- `DSRCH_DATA_DIR` → `DEWEY_DATA_DIR`
- `.dsrch/` → `.dewey/`
- `connect-delta` → removed (use `create --source`)
- `ingest` → `to-delta`

Update the "zero to hero" example in product-requirements.md:
```bash
dewey create labs --source az://datalake/lab-results
dewey librarian labs &
dewey search labs "glucose"
```

- [ ] **Step 2: Commit**

```bash
git add -A
git commit -m "docs: rebrand to Dewey across all documentation"
```

---

## Execution Order

Tasks 1-8 are sequential (each builds on the previous rename).

## Final Verification

```bash
cargo fmt --check
cargo clippy -- -D warnings
cargo test
dewey --help
dewey search --help
dewey librarian --help
dewey create --help
dewey to-delta --help
```

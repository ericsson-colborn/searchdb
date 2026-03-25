# Ingest Command Design: Raw Files to Delta Lake

**Date:** 2026-03-25
**Status:** Draft
**GitHub Issue:** #3

## Problem

SearchDB requires data in Delta Lake format for its core workflow (`connect-delta` -> `compact` -> `search`). Users who have raw data files (NDJSON, JSON, CSV, Parquet) on local disk or blob storage currently have no way to convert them into a Delta table without external tooling (Python `deltalake`, DLT, Spark).

This is the single biggest friction point in onboarding. The pitch is "zero to search in under 5 minutes" but the first question is always: "I have JSON files. Now what?"

## Solution

Add a `dsrch ingest` command (binary name remains `searchdb` until rename in issue #1) that converts raw data files into a Delta Lake table. This is a one-shot converter, not a pipeline.

```
dsrch ingest --source ./data/*.json --delta /tmp/labs-delta
dsrch ingest --source ./data.csv --delta s3://bucket/labs-delta
cat data.ndjson | dsrch ingest --delta /tmp/labs-delta --format ndjson
```

After ingestion, the standard SearchDB workflow takes over:

```
dsrch ingest  ->  Delta Lake  ->  dsrch connect-delta  ->  dsrch compact  ->  dsrch search
  (one-shot)     (source of        (one-time)              (worker)          (client)
                   truth)
```

## Design Principles

1. **Convention over configuration.** Auto-detect format from file extension. Infer Delta schema from the data. No explicit schema required.
2. **Arrow as the universal intermediate.** Every format reader produces Arrow `RecordBatch`es. Delta Lake writes Arrow `RecordBatch`es. No JSON intermediate.
3. **Overwrite by default, append explicitly.** First ingest creates the table. Subsequent ingests require `--mode append`.
4. **Local first, remote later.** V1 handles local file sources. Remote source files (S3/GCS/Azure) deferred to v2. Remote Delta destinations work out of the box via `deltalake` crate.

## CLI Interface

```
dsrch ingest [OPTIONS]

Options:
  --source <GLOB_OR_PATH>   Source file(s) — glob pattern or single path
  --delta <URI>             Delta Lake table destination (path or URI)
  --format <FORMAT>         File format: ndjson, json, csv, parquet (auto-detected from extension)
  --mode <MODE>             Write mode: overwrite (default) or append
  --batch-size <N>          Rows per write batch (default: 10000)
```

### Format Detection

When `--format` is not specified, detect from file extension:

| Extension(s)         | Format  |
|---------------------|---------|
| `.ndjson`, `.jsonl` | NDJSON  |
| `.json`             | JSON    |
| `.csv`              | CSV     |
| `.parquet`          | Parquet |

When reading from stdin, `--format` is required (no extension to detect from).

### Stdin Support

```bash
cat data.ndjson | dsrch ingest --delta /tmp/labs --format ndjson
```

When `--source` is omitted and stdin is not a TTY, read from stdin. Buffer into Arrow RecordBatches using the specified `--format`, then write to Delta.

### Glob Support

```bash
dsrch ingest --source "./data/*.json" --delta /tmp/labs
```

Use the `glob` crate to expand patterns. Without it, shell expansion handles simple cases but quoted patterns (which we need for cross-platform support) don't expand.

## Architecture

### Data Flow

```
Source files (NDJSON/JSON/CSV/Parquet)
    |
    v
Format reader (arrow-json / arrow-csv / parquet)
    |
    v
Vec<RecordBatch>
    |
    v
DeltaOps::write() -> Delta Lake table
```

### File Reading: Format Readers

Each format reader takes a `Read` source and produces `Vec<RecordBatch>`:

**NDJSON Reader** (`arrow-json` crate, already a transitive dep via `arrow`):
- Use `arrow::json::ReaderBuilder` with inferred schema
- Read line by line, decode into Arrow arrays
- The `arrow-json` `ReaderBuilder` handles schema inference from the first N rows

**JSON Array Reader** (`arrow-json` crate):
- Read entire file content, parse as JSON array
- Convert each element to NDJSON-style processing (each object is one row)
- Feed through the same Arrow JSON reader path

**CSV Reader** (`arrow-csv` crate, new dependency):
- Use `arrow::csv::ReaderBuilder` with inferred schema
- First row is header, remaining rows are data
- Type inference from the CSV library (strings, numbers, booleans)

**Parquet Reader** (`parquet` crate, already in deps):
- Use `ParquetRecordBatchReaderBuilder` (same as `delta.rs`)
- Direct RecordBatch passthrough -- already in Arrow format

### Schema Handling

**No explicit schema.** The Delta table schema is inferred from the Arrow schema of the first file's RecordBatches. This is the same approach as `connect-delta --dry-run` but in reverse: instead of reading a Delta table's schema, we're writing one.

For `--mode append`, the existing Delta table's schema must be compatible with the incoming data. The `deltalake` crate handles schema validation on write -- if types conflict, the write fails with a clear error.

### Delta Writing

Use `DeltaOps::write()` from the `deltalake` crate:

```rust
use deltalake::operations::create::CreateBuilder;
use deltalake::DeltaOps;
use deltalake::protocol::SaveMode;

// For new tables:
let table = CreateBuilder::new()
    .with_location(delta_uri)
    .with_columns(schema_fields)
    .await?;
DeltaOps(table)
    .write(batches)
    .with_save_mode(SaveMode::Overwrite)
    .await?;

// For append to existing:
let table = deltalake::open_table(delta_uri).await?;
DeltaOps(table)
    .write(batches)
    .with_save_mode(SaveMode::Append)
    .await?;
```

### Write Modes

| Mode | Behavior |
|------|----------|
| `overwrite` | Default. Creates new table or replaces existing data. Uses `SaveMode::Overwrite`. |
| `append` | Adds rows to existing table. Uses `SaveMode::Append`. Fails if table doesn't exist. |

### Batching

Large files are processed in batches of `--batch-size` rows (default 10,000). Each batch:
1. Reads up to N rows from the format reader
2. Produces a RecordBatch
3. Accumulates batches until all files are read
4. Writes all batches in a single `DeltaOps::write()` call

This keeps memory bounded while minimizing the number of Delta transactions (one transaction per ingest invocation).

## Module Structure

```
src/
├── ingest.rs              # NEW — core ingest logic (format readers, Delta writer)
├── commands/
│   ├── ingest.rs          # NEW — CLI entry point
│   ├── mod.rs             # MODIFY — add pub mod ingest
├── main.rs                # MODIFY — add Ingest command variant
```

### `src/ingest.rs` — Core Logic

Public API:

```rust
/// Supported input formats.
pub enum InputFormat {
    Ndjson,
    Json,
    Csv,
    Parquet,
}

/// Detect format from file extension.
pub fn detect_format(path: &str) -> Option<InputFormat>;

/// Read a single file into RecordBatches.
pub fn read_file(path: &str, format: InputFormat, batch_size: usize) -> Result<Vec<RecordBatch>>;

/// Read from a reader (for stdin support).
pub fn read_reader(reader: impl Read, format: InputFormat, batch_size: usize) -> Result<Vec<RecordBatch>>;

/// Write RecordBatches to a Delta table.
pub async fn write_delta(
    delta_uri: &str,
    batches: Vec<RecordBatch>,
    mode: SaveMode,
) -> Result<i64>;  // returns Delta version
```

### `src/commands/ingest.rs` — CLI Entry Point

```rust
pub async fn run(
    source: Option<&str>,
    delta_uri: &str,
    format: Option<&str>,
    mode: &str,
    batch_size: usize,
) -> Result<()>;
```

Orchestrates: resolve source (files or stdin) -> detect format -> read into RecordBatches -> write to Delta.

## Error Handling

| Scenario | Behavior |
|----------|----------|
| No source and stdin is TTY | Error: "No source specified. Use --source or pipe data to stdin." |
| Unknown file extension, no --format | Error: "Cannot detect format for '...'. Use --format to specify." |
| Format mismatch (e.g., CSV file with .json extension) | Arrow reader error propagated |
| Empty input (no rows) | Warning, create empty Delta table with inferred schema |
| Schema conflict on append | Delta write error propagated: "Schema mismatch: ..." |
| Delta URI not writable | Delta write error propagated |

## Dependencies

### New Dependencies

- `glob = "0.3"` — glob pattern expansion for `--source "*.json"`
- `arrow-csv` — CSV reading into Arrow RecordBatches (may need explicit dep or feature flag; check if `arrow` re-exports it)

### Existing Dependencies (no changes)

- `arrow` (v54) — `arrow::json::ReaderBuilder` for NDJSON/JSON reading
- `parquet` (v54) — Parquet reading (already used in `delta.rs`)
- `deltalake` (v0.25) — `DeltaOps::write()`, `CreateBuilder`
- `tokio` — async runtime for Delta operations

### Dependency Check

The `arrow` crate is already in the dependency tree but only as an optional dep gated on the `delta` feature. The `ingest` command requires Delta, so it will be gated on the same `delta` feature flag.

The `arrow` crate v54 re-exports `arrow-json` and `arrow-csv` when the appropriate features are enabled. We may need to add `features = ["csv", "json"]` to the arrow dependency, or add explicit deps on `arrow-csv` and `arrow-json`.

## What This Is Not

- **Not a pipeline.** No scheduling, no incremental detection, no CDC. Run it once. Run it again with `--mode append` for new data.
- **Not a schema tool.** No explicit schema definition for the Delta table. Schema is always inferred from data. If you need a specific schema, create the Delta table yourself and use `--mode append`.
- **Not a transform tool.** No column selection, no filtering, no type casting. Data goes in as-is. Use DuckDB or Spark for transforms.

## Future Work (v2)

1. **Remote source files.** `--source s3://bucket/data/*.json` — requires `object_store` crate to list and read remote files. The `deltalake` crate uses `object_store` internally; we'd need to expose it for source file listing.
2. **Schema override.** `--schema '{"id":"keyword","value":"numeric"}'` — explicit typing for specific columns.
3. **Progress reporting.** File count, row count, bytes processed. Useful for large multi-file ingests.
4. **Streaming writes.** For very large files, write batches incrementally instead of accumulating all in memory. Multiple `DeltaOps::write()` calls with `SaveMode::Append`.

## Relationship to Existing Commands

| Command | Role |
|---------|------|
| `ingest` (new) | Raw files -> Delta Lake. Optional first step. |
| `connect-delta` | Delta Lake -> SearchDB index. One-time setup. |
| `compact` | Delta Lake -> tantivy segments. Long-lived worker. |
| `index` | NDJSON -> tantivy index directly (no Delta). Legacy/simple path. |
| `search` | Read-only. Queries tantivy index. |

`ingest` and `index` serve different use cases. `index` bypasses Delta entirely for users who don't need versioned storage. `ingest` writes to Delta, which then feeds the full SearchDB pipeline.

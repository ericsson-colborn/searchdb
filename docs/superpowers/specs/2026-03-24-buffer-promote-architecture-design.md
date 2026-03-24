# Delta-as-Buffer Architecture for SearchDB

**Date:** 2026-03-24
**Status:** Draft

## Problem

SearchDB is a serverless, single-binary search tool backed by Delta Lake. The current implementation holds a tantivy `IndexWriter` lock during sync operations, blocking concurrent queries. In event-driven, high-throughput patterns — many small Delta versions with 10-50 rows each, many concurrent queries triggering auto-sync — this lock becomes the bottleneck.

## Core Insight

Delta Lake is already an append-only, versioned data store. The Parquet files that haven't been indexed yet ARE the buffer tier. There's no need to build a parallel buffer system — just read the un-indexed Parquet files directly at query time and promote them into the tantivy index in the background.

## Design Principles

1. **Serverless.** No daemon. Every operation is a CLI invocation. Background promotion is a fire-and-forget child process.
2. **Index-on-the-fly.** Users don't think about indexing. Queries always return fresh results.
3. **Delta is the buffer.** No intermediate files. The gap between `index_version` and Delta HEAD is the un-indexed tier.
4. **Dumb, reliable architecture.** Dedup at read time. Duplicate work over coordination.

## Architecture

### On-Disk Layout

```
{index_name}/
├── searchdb.json     ← schema + delta source + index_version watermark
└── index/            ← tantivy index (covers data up to index_version)
```

That's it. No buffer directory, no manifest, no lockfiles beyond tantivy's own.

### searchdb.json

```json
{
  "schema": {"fields": {"name": "keyword", "notes": "text"}},
  "delta_source": "/data/labs",
  "index_version": 103
}
```

`index_version` means: the tantivy index contains all Delta data up through this version. This is a rename of the existing `last_indexed_version` field in `IndexConfig`.

### The Two Tiers

**Index tier:** The tantivy index on disk. Covers all data up to `index_version`. Searched via the inverted index — fast, selective.

**Gap tier:** The Parquet files in Delta between `index_version` and HEAD. Read directly from the Delta table at query time using the existing `delta.rows_added_since(index_version)`. No copy, no intermediate storage.

## Query Path

```
searchdb search labs -q "diabetes"

1. Read searchdb.json → index_version = 103
2. Check Delta HEAD → version 108
3. Search tantivy index (covers v0–v103)
4. Read gap: delta.rows_added_since(103) → Parquet files for v104–v108
5. Build ephemeral in-memory tantivy index from gap rows
6. Search ephemeral index
7. Merge results from both, dedup by _id (gap wins), apply limit/offset
8. Return results
9. If gap > N versions → fire-and-forget: spawn `searchdb sync labs`
```

Every query returns immediately with fresh results. The gap is always read from Delta, so results include the latest data regardless of whether promotion has happened.

### Why an Ephemeral Index for the Gap

The gap needs to support the full tantivy query syntax — boolean operators, range queries, phrase queries, stemming. Reimplementing query matching against raw JSON would be a major effort. Instead, building a temporary in-memory tantivy index from the gap rows:

- Takes milliseconds (tantivy's `RamDirectory`, gap is small by design)
- Gives full query syntax for free (same `QueryParser`)
- Gives proper BM25 scoring (gap hits scored the same as index hits)
- Uses the same `build_document` / `upsert_document` code path
- Thrown away after the query (no persistence, no lock)

### Dedup Strategy

When the same `_id` appears in both the persistent index and the gap, the gap version wins (it's newer). Implementation: build a `HashSet<String>` of `_id`s from the gap, then exclude those from the persistent index results before merging.

### Pagination (limit/offset)

1. Search the ephemeral gap index: collect all matching hits (gap is small)
2. Search the persistent index with `limit + offset`, excluding gap `_id`s
3. Merge both result sets by score (descending)
4. Apply offset/limit to the merged set

### `searchdb get` Behavior

Simplified path for single-document lookup:

1. Read gap rows, scan for matching `_id` → if found, return it (gap wins)
2. Fall back to tantivy `TermQuery` on the persistent index
3. No ephemeral index needed — `get` is an exact `_id` lookup, not a query

## Background Promotion

Promotion moves gap data into the tantivy index so future queries don't need to read Parquet files. It's a fire-and-forget child process — the query that triggers it doesn't wait.

### Trigger

When the gap exceeds N Delta versions (configurable, default 10), the query process spawns a background promoter after returning results.

### Implementation

```rust
// Fire and forget — query has already returned
std::process::Command::new(std::env::current_exe()?)
    .args(["sync", name, "--data-dir", data_dir])
    .stdin(Stdio::null())
    .stdout(Stdio::null())
    .stderr(Stdio::null())
    .spawn()?;
```

The background process is just `searchdb sync`, which already exists:

1. Acquire tantivy `IndexWriter` lock
   - If locked: exit silently (another promoter is running)
2. `delta.rows_added_since(index_version)` → read gap Parquet files
3. Upsert all rows into tantivy index
4. Commit
5. Update `index_version` in `searchdb.json`
6. Exit

### Concurrency

**Multiple queries spawning promoters:** Only one acquires the `IndexWriter` lock. Others exit silently. No harm — the successful one does the work.

**Queries during promotion:** Unaffected. Queries read the gap from Delta directly. They don't depend on the index being up to date. The tantivy `Searcher` holds an `Arc` snapshot — it never contends with the `IndexWriter`.

**Promotion crash:** Index unchanged (commit is atomic), `index_version` unchanged. Next promoter picks up where it left off.

### `sync` Command Changes

The existing `searchdb sync` command needs one change: if the `IndexWriter` lock is held, exit with code 0 (success) instead of returning an error. This makes it safe to spawn as a fire-and-forget process.

## Command Integration

| Command | Behavior |
|---------|----------|
| `search` | Read gap from Delta + search index + ephemeral gap index; spawn background sync if gap > N |
| `get` | Scan gap for `_id`, fall back to index; spawn background sync if gap > N |
| `stats` | Report index docs, index_version, Delta HEAD version, gap size |
| `sync` | Acquire lock, index gap, update version. Exit silently if lock held. |
| `connect-delta` | Full load directly to index (bulk, lock acceptable); set `index_version` |
| `reindex` | Rebuild index from Delta; set `index_version` to HEAD |
| `drop` | Delete index directory |
| `index` (NDJSON) | Write directly to index (explicit user action, lock acceptable) |
| `new` | No change |

## Performance Characteristics

### Query Latency

| Scenario | Cost |
|----------|------|
| Gap = 0 (index is current) | Tantivy search only. Fast. |
| Gap = 5 versions, ~200 rows | Tantivy search + read 5 Parquet files + build ephemeral index. Adds ~10-50ms. |
| Gap = 50 versions, ~2,000 rows | Tantivy search + read 50 Parquet files + build ephemeral index. Adds ~50-200ms. |
| Gap = 500 versions, ~20,000 rows | Noticeable. Promotion should have fired long ago (threshold N=10). |

The gap stays small in practice because every query that sees a large gap spawns a promoter. Under sustained load, promotion happens continuously in the background.

### Write Contention

None during normal query flow. The only lock acquisition is in `searchdb sync` (background promotion), which exits silently if contended. Queries never block.

## Limitations

1. **Delta deletes are not handled by incremental sync.** If rows are deleted from the Delta table, the search index retains stale documents. Use `searchdb reindex` to rebuild. Existing limitation, unchanged.

2. **Gap read cost scales with gap size.** Reading Parquet files from Delta is slower than reading a local buffer. The fire-and-forget promotion keeps the gap bounded.

3. **Delta table must be accessible at query time.** The current implementation only accesses Delta during sync. This design reads Delta on every query that has a gap. For local Delta tables this is negligible. For remote (S3, ADLS) this adds network latency.

4. **No cross-process caching of gap data.** Each query reads the same Parquet files independently. Under high concurrency with a gap, this means redundant reads. Acceptable given the "dumb, reliable" principle — and the gap shrinks quickly once promotion fires.

## What Changed from the Previous Design

The previous spec introduced a buffer directory with NDJSON files, a promotion lockfile with PID tracking, buffer file naming conventions, and cleanup logic. This was overengineered — Delta Lake already provides everything the buffer tier needs:

| Previous (buffer tier) | Current (Delta-as-buffer) |
|----------------------|--------------------------|
| NDJSON buffer files | Parquet files in Delta |
| Buffer directory management | Delta's transaction log |
| `.promoting` lockfile with PID | Tantivy's own `IndexWriter` lock |
| Buffer cleanup after promotion | Nothing to clean up |
| File naming convention | Delta version numbers |
| Manifest / version tracking | `index_version` in searchdb.json |

The entire buffer tier collapsed into one function call: `delta.rows_added_since(index_version)`.

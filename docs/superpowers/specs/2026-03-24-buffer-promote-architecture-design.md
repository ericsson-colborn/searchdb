# Buffer + Promote Architecture for SearchDB

**Date:** 2026-03-24
**Status:** Draft

## Problem

SearchDB is a serverless, single-binary search tool backed by Delta Lake. The current implementation holds a tantivy `IndexWriter` lock during sync operations, blocking concurrent queries. In event-driven, high-throughput patterns — many small Delta versions with 10-50 rows each, many concurrent queries triggering auto-sync — this lock becomes the bottleneck.

The fundamental tension: documents are keyed by `_id`, but the inverted index is keyed by terms. Updating a document requires `delete_term` + `add_document`, which holds an exclusive writer lock on the entire index.

## Design Principles

1. **Serverless.** No daemon, no background process. Every operation is a CLI invocation.
2. **Index-on-the-fly.** Users don't think about indexing. Queries trigger sync automatically.
3. **Write-only, compact later.** Borrowed from the lakehouse movement — instead of coordinating concurrent writers, make writes append-only and defer compaction.
4. **Dumb, reliable architecture.** Prefer duplicate work over coordination. Dedup at read time rather than synchronize at write time.

## Architecture: Two-Tier (Buffer + Index)

### On-Disk Layout

```
{index_name}/
├── searchdb.json          ← schema + delta source + index_version watermark
├── index/                 ← compacted tantivy index (the indexed tier)
├── buffer/                ← append-only NDJSON files (the unindexed tier)
│   ├── v105-a1b2c3d4.ndjson
│   ├── v105-e5f6g7h8.ndjson   ← duplicate from concurrent sync (OK)
│   ├── v108-i9j0k1l2.ndjson
│   └── .promoting         ← lockfile with PID, exists only during promotion
```

### Configuration: searchdb.json

The existing `searchdb.json` gains the `index_version` field, replacing the current `last_indexed_version`:

```json
{
  "schema": {"fields": {"name": "keyword", "notes": "text"}},
  "delta_source": "/data/labs",
  "index_version": 103
}
```

`index_version` means: the tantivy index contains all Delta data up through this version. No separate `manifest.json` — one config file, one source of truth. This is a rename/reuse of the existing `last_indexed_version` field in `IndexConfig`.

### The Two Tiers

**Index tier:** A compacted tantivy index. Searched via the inverted index — fast, selective. Contains all data up through `index_version`.

**Buffer tier:** A directory of NDJSON files. Each file contains rows synced from Delta. At query time, buffer rows are loaded into a temporary in-memory tantivy index for searching (see Read Path). No persistent write lock, no coordination.

The inverted index only earns its keep at ~10K+ documents. Below that threshold, building an ephemeral in-memory index from the buffer is fast (~milliseconds for <5K rows) and gives full query syntax support with proper BM25 scoring. The buffer tier exploits this: tiny incremental syncs stay as flat files until there's enough data to justify building a persistent index.

### Cost Model

| Operation | Index tier | Buffer tier |
|-----------|-----------|-------------|
| Write | IndexWriter lock, build segments, commit | Append NDJSON file (no lock) |
| Read | Term dictionary lookup → posting list → score | Build ephemeral in-memory index, search |
| Per-file open cost | mmap + FST load (~ms) | open() + read() + JSON parse (~μs) |
| Useful when | >10K docs (high selectivity) | <10K docs (ephemeral index is fast enough) |

## Write Path: Sync to Buffer

When a query triggers auto-sync:

1. Read `searchdb.json` → `index_version` (e.g., 103)
2. Check Delta table → current version (e.g., 108)
3. If current > index_version:
   a. Diff Delta file URIs: `v108.file_uris() - v103.file_uris()`
   b. Read new Parquet files → rows as JSON
   c. Write rows to `buffer/v108-{uuid}.ndjson`

That's it. No tantivy writer, no lock, no segment creation.

### Concurrent Write Behavior

Multiple processes may sync the same Delta version range simultaneously. Each writes its own buffer file (UUID suffix ensures unique filenames). This produces duplicate rows.

**Duplicates are harmless.** Dedup by `_id` at read time ensures correctness. The cost is extra buffer bytes — under realistic concurrency (10 processes syncing 50 rows each), that's ~500 duplicate rows, maybe 100KB. Negligible.

**No check-before-write optimization.** We considered checking if a buffer file for the target version range already exists. This introduces partial-coverage detection logic and a crash-safety hole (half-written file tricks other processes into skipping their sync). The "always write, always dedup" approach is simpler, more reliable, and the I/O cost is noise.

### Partial Write Safety

If a process crashes mid-write, the buffer file may contain a truncated last line. Buffer readers skip unparseable lines (matching the existing `searchdb index` behavior). No data loss — the next sync will write a complete copy.

### File Naming Convention

```
buffer/v{to}-{uuid}.ndjson
```

- `to`: Delta version this sync targeted (the HEAD version at sync time)
- `uuid`: Random suffix for uniqueness under concurrent writes
- Format: One JSON object per line (NDJSON)

The `to` version is the only metadata needed. The `from` version is always implicitly `index_version` at the time of sync, but since `index_version` changes on promotion, encoding `from` in the filename creates stale metadata. Simpler to omit it.

## Read Path: Search Index + Search Buffer

When a query executes:

1. Read all `buffer/*.ndjson` files → collect rows as JSON
2. Build a `HashSet<String>` of all `_id`s present in the buffer
3. Build an ephemeral tantivy index in `RamDirectory` from buffer rows (using the same schema)
4. Search the persistent index, filtering out any `_id` in the buffer set (stale versions)
5. Search the ephemeral buffer index
6. Merge results from both searches, apply limit/offset, return

### Why an Ephemeral Index for the Buffer

The buffer tier needs to support the full tantivy query syntax — boolean operators, range queries, phrase queries, stemming. Reimplementing query matching against raw JSON would be a major effort and a divergent code path. Instead, building a temporary in-memory tantivy index from <5K rows:

- Takes milliseconds (tantivy's `RamDirectory` is fast)
- Gives full query syntax for free (same `QueryParser`)
- Gives proper BM25 scoring (buffer hits are scored the same as index hits)
- Uses the same `build_document` / `upsert_document` code path
- Is thrown away after the query (no persistence, no lock)

### Dedup Strategy

When the same `_id` appears in both the persistent index and the buffer, the buffer version wins (it's newer). Implementation: build the buffer `_id` set first, then exclude those `_id`s from the persistent index search results. This ensures that updated documents show their latest version without double-counting.

When the same `_id` appears in multiple buffer files, any copy is fine — they contain the same data (from the same Delta source). The ephemeral index's `upsert_document` (delete_term + add) handles this naturally.

### Pagination (limit/offset)

With two search sources, pagination requires care. The approach:

1. Search the ephemeral buffer index: collect all matching hits (buffer is small by design, <5K rows)
2. Search the persistent index with `limit + offset`, excluding buffer `_id`s
3. Merge both result sets by score (descending)
4. Apply offset (skip first N) and limit (take next M) to the merged set

This is correct because buffer hits have proper BM25 scores from the ephemeral index, so score-based merging produces the right ordering.

### `searchdb get` Behavior

The `get` command (single document by `_id`) follows a simplified path:

1. Scan buffer files for a row with matching `_id` → if found, return it (buffer wins)
2. Fall back to tantivy `TermQuery` on the persistent index
3. No ephemeral index needed — `get` is an exact `_id` lookup, not a query

### `searchdb stats` Behavior

The `stats` command reports both tiers:

```json
{
  "index": "labs",
  "index_docs": 10000,
  "buffer_files": 12,
  "buffer_approx_rows": 600,
  "index_version": 103,
  "delta_current_version": 108,
  "fields": {"name": "keyword", "notes": "text"}
}
```

`buffer_approx_rows` is estimated by counting newlines across buffer files (cheap `memchr` scan, no JSON parsing).

## Promotion: Buffer → Index

Promotion moves buffered rows into the persistent tantivy index and clears the buffer. This is the only operation that acquires the tantivy `IndexWriter` lock.

### Trigger Condition

Promote when buffer file count > 50. Checked before each query, after auto-sync.

File count is the sole trigger — simple, requires only a directory listing, no parsing. Row-based triggers require reading buffer files before every query, defeating the purpose of the buffer tier.

### Promotion Flow

```
1. Check trigger: buffer file count > 50?
   → No: skip promotion, proceed to query
   → Yes: attempt promotion

2. Create lockfile: buffer/.promoting (O_CREAT | O_EXCL)
   Write promoter's PID into the file.
   → Exists: check if PID is alive (kill(pid, 0))
     → Alive: another process is promoting → skip, proceed to query
     → Dead: stale lock from crashed promoter → remove and take over
   → Created: we are the promoter

3. Snapshot buffer file list (ls buffer/*.ndjson)

4. Acquire tantivy IndexWriter on index/
   → Fails (locked): remove .promoting, skip promotion, proceed to query
   → Success: continue

5. Read all snapshot files, upsert rows into tantivy index
   (delete_term by _id + add_document — handles dedup against existing index data)

6. Commit IndexWriter

7. Update searchdb.json: set index_version to max Delta version from buffer filenames

8. Delete snapshot files + any buffer files where version <= new index_version

9. Remove buffer/.promoting lockfile

10. Proceed to query (search index + any buffer files written during promotion)
```

### Buffer Cleanup

Step 8 includes cleanup of stale buffer files: any file whose `to` version is <= the new `index_version` is deleted, even if it wasn't in the promotion snapshot. This handles:
- Files from concurrent syncs that duplicated already-promoted data
- Files left behind by a previous crashed promotion

### Concurrency During Promotion

**New syncs during promotion:** Safe. New buffer files are written after the snapshot (step 3), so they aren't deleted (step 8). They'll be scanned by the current query and promoted next time.

**Concurrent readers during promotion:** Safe. POSIX unlink semantics — deleted files remain readable until all open handles close. A query scanning a buffer file that gets deleted in step 8 continues reading without error.

**Concurrent promotion attempts:** Safe. The `.promoting` lockfile (step 2) ensures at most one promoter. The PID check detects crashed promoters without a time-based assumption.

**Crash during promotion:**
- Before commit (step 6): Index unchanged, buffer files intact, `.promoting` is stale. Next promoter detects dead PID and takes over.
- After commit, before cleanup (steps 7-8): Index has the data AND buffer files still exist. Duplicates are handled by dedup. Next promoter cleans up.

### Why the IndexWriter Lock Is Acceptable Here

The lock is only held during promotion, not during normal sync. Promotion of ~5,000 rows takes ~100-200ms. Under realistic concurrency, the collision window is small. When a process can't acquire the lock, it simply skips promotion — the buffer continues working. The system degrades gracefully: more concurrent load means the buffer stays a little longer, not that queries fail.

## Command Integration

### Affected Commands

| Command | Change |
|---------|--------|
| `search` | Auto-sync to buffer; check promotion trigger; search both tiers |
| `get` | Auto-sync to buffer; scan buffer for `_id`, fall back to index |
| `stats` | Report both index and buffer statistics |
| `connect-delta` | Full load writes directly to index (bulk, lock acceptable); set `index_version` |
| `sync` | Write to buffer instead of directly to index |
| `reindex` | Clear buffer directory, rebuild index from Delta, set `index_version` |
| `drop` | Delete buffer directory along with index directory |
| `index` (NDJSON) | Write to buffer (unifies write path) OR direct to index for bulk loads |
| `new` | No change |

### `searchdb index` (Bulk NDJSON Import)

Two modes, chosen by input size:
- **Small input (<5K rows):** Write to buffer. Consistent with the sync path.
- **Large input (>5K rows):** Write directly to tantivy IndexWriter. The lock is acceptable for bulk loads where the user explicitly invoked the command. Avoids creating a massive buffer that would immediately trigger promotion.

Threshold detection: count rows during ingestion. Start writing to a temp buffer file. If the count exceeds the threshold before EOF, switch to direct IndexWriter mode (promote the partial buffer file and continue directly).

## Limitations

1. **Delta deletes are not handled by incremental sync.** If rows are deleted from the Delta table (via DELETE or MERGE), the search index retains the stale documents. Use `searchdb reindex` to rebuild from scratch when deletes matter. This is an existing limitation documented in CLAUDE.md, unchanged by this architecture.

2. **Buffer scan cost grows with buffer size.** The ephemeral index build is fast for <5K rows but would become noticeable at larger sizes. The promotion trigger (50 files) keeps this bounded.

3. **No cross-process query result caching.** Each CLI invocation builds its own ephemeral buffer index from scratch. A long-lived `searchdb serve` process could cache this.

## Lakehouse Analogy

| Concept | Delta Lake | LSM-tree | SearchDB |
|---------|-----------|----------|----------|
| Hot/write tier | Uncommitted writes | Memtable | Buffer (NDJSON) |
| Cold/read tier | Parquet files | SSTables | Tantivy index (inverted, immutable segments) |
| Compaction | OPTIMIZE | Level compaction | Promotion (buffer → index) |
| Write contention | Transaction log (optimistic) | WAL (sequential) | File-per-sync (parallel, dedup at read) |
| Coordination | Conflict detection on commit | Single writer | None (dedup handles races) |

This is the lakehouse pattern — write-only, compact later — applied to inverted indices. Writes never touch the index. The index is built lazily from accumulated buffer data when the buffer grows large enough to justify it.
